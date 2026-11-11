"""Fixed last-stage workflow: eligibility -> one LLM -> gate -> final text.

No tools, retries, networking, streaming or persistence are performed here.
The caller must run this BEFORE atomically persisting history/final reply.
"""
from __future__ import annotations

import math
import re
import time
from dataclasses import asdict
from .contracts import fallback_text, parse_candidate, render_candidate, validate_contract
from .types import (ContractError, Guard, RewriteConfig, RewriteModel,
                    RewriteRequest, RewriteResult, canonical)

PROMPT_VERSION = "am-realizer-r2.1"
EXACT_REQUEST = re.compile(r"原文|逐字|原样|完整正文|一字不改|不要改写|不润色|预览|引用原句")
NEGATED_COPY = re.compile(r"(?:不要|不用|不需要|无需|不必|别)(?:再)?(?:照搬|复制|粘贴|引用|输出|使用|给我|给|看|贴)?(?:台账)?(?:原文|原样(?:输出|回复|发送)?)")


def requests_verbatim(text: str) -> bool:
    # An explicit host protected flag still wins regardless of these words.
    return bool(EXACT_REQUEST.search(NEGATED_COPY.sub("", text)))


class ReplyRewriter:
    def __init__(self, model: RewriteModel, config: RewriteConfig | None = None, *, clock=time.monotonic):
        self.model, self.config, self.clock = model, config or RewriteConfig(), clock
        c = self.config
        if c.mode not in ("off", "shadow", "enforce"):
            raise ContractError("INVALID_MODE")
        for value in (c.max_seconds, c.queue_seconds, c.min_remaining_seconds, c.finish_reserve_seconds):
            if type(value) not in (int, float) or not math.isfinite(value) or value < 0:
                raise ContractError("INVALID_TIME_BUDGET")
        if not 0 < c.max_seconds <= 30 or c.queue_seconds > min(2, c.max_seconds):
            raise ContractError("INVALID_TIME_BUDGET")
        if (type(c.max_tokens) is not int or not 64 <= c.max_tokens <= 2048
                or type(c.max_input_chars) is not int or not 1000 <= c.max_input_chars <= 20000
                or type(c.max_output_chars) is not int or not 500 <= c.max_output_chars <= 10000):
            raise ContractError("INVALID_MODEL_BUDGET")

    def rewrite(self, request: RewriteRequest, *, guard: Guard) -> RewriteResult:
        if not callable(guard):
            raise ContractError("GUARD_REQUIRED")
        started = self.clock()
        # Guard exceptions are never treated as generation failures.
        guard()
        c, contract = self.config, request.contract
        fp = None
        def result(text, disposition, *reasons, attempted=False, candidate=None):
            guard()
            return RewriteResult(text=text, disposition=disposition, reasons=tuple(reasons),
                                 attempted=attempted, candidate_text=candidate,
                                 elapsed_seconds=max(0.0, self.clock() - started),
                                 contract_fingerprint=fp, prompt_version=PROMPT_VERSION)
        if c.mode == "off":
            return result(request.draft, "disabled")
        if request.protected or request.pending_actions or request.status != "answered" or requests_verbatim(request.user_text):
            return result(request.draft, "protected", "EXACT_OR_TRANSACTIONAL_REPLY")
        if contract is None:
            return result(request.draft, "not_covered", "NO_TRUSTED_CONTRACT")
        try:
            validate_contract(contract)
            fp = contract.fingerprint()
            safe = fallback_text(contract)
        except (ContractError, TypeError, ValueError, RecursionError):
            return result(request.draft, "not_covered", "INVALID_HOST_CONTRACT")
        if (request.style.approved is not True or (request.style.scope_key and request.style.scope_key != contract.scope_key)
                or len(request.style.instructions) > 12
                or any(type(v) is not str or len(v) > 500 for v in request.style.instructions)):
            return result(request.draft, "not_covered", "STYLE_NOT_APPROVED_FOR_SCOPE")
        fallback = safe if c.mode == "enforce" else request.draft
        if contract.origin == "ledger_source_block":
            # Do not spend an LLM call pretending an immutable raw block can
            # become a fluent summary. Coverage must expose this limitation.
            return result(fallback, "source_projection", "NO_EQUIVALENT_VARIANTS")
        if type(request.deadline) not in (int, float) or not math.isfinite(request.deadline):
            return result(fallback, "fallback", "INVALID_DEADLINE")
        remaining = request.deadline - self.clock() - c.finish_reserve_seconds
        if remaining < c.min_remaining_seconds:
            return result(fallback, "fallback", "INSUFFICIENT_SHARED_BUDGET")
        payload = {
            "prompt_version": PROMPT_VERSION,
            "question": request.user_text[:1500],
            "style": {"id": request.style.profile_id, "version": request.style.version,
                      "instructions": list(request.style.instructions)},
            "contract_fingerprint": fp,
            "max_groups": contract.max_groups,
            "max_chars": contract.max_chars,
            "preserve_order": contract.preserve_order,
            "require_record_attribution": contract.require_record_attribution,
            "claims": [{"claim_id": claim.claim_id, "variants": list(claim.variants),
                        "required": claim.required, "depends_on": list(claim.depends_on)} for claim in contract.claims],
        }
        # The untrusted draft, raw source metadata, history and tool catalog
        # are intentionally absent. Only the minimal authorized projection is sent.
        if len(canonical(payload)) > c.max_input_chars:
            return result(fallback, "fallback", "INPUT_BUDGET_EXCEEDED")
        timeout = min(c.max_seconds, remaining)
        guard()
        model_started = self.clock()
        raw, failure = None, None
        try:
            raw = self.model.generate(payload, timeout=timeout,
                                      queue_timeout=min(c.queue_seconds, timeout), max_tokens=c.max_tokens)
        except Exception as exc:
            known = {"MODEL_BUSY", "MODEL_PAUSED", "STOP_UNKNOWN", "MODEL_TIMEOUT", "TIMEOUT"}
            failure = str(exc) if str(exc) in known else "MODEL_ERROR"
        # Runs for success, malformed output, provider busy, and timeout alike.
        # A revoked source/cancelled request cannot fall back to old text.
        guard()
        if failure:
            return result(fallback, "fallback", failure, attempted=True)
        if self.clock() - model_started > timeout or self.clock() >= request.deadline - c.finish_reserve_seconds:
            return result(fallback, "fallback", "REWRITE_BUDGET_EXCEEDED", attempted=True)
        try:
            candidate = parse_candidate(raw, c.max_output_chars)
            text = render_candidate(contract, candidate)
        except (ContractError, TypeError, ValueError, RecursionError) as exc:
            code = str(exc) if isinstance(exc, ContractError) else "INVALID_CANDIDATE"
            return result(fallback, "rejected", code, attempted=True)
        if c.mode == "shadow":
            return result(request.draft, "shadow_accepted", attempted=True, candidate=text)
        return result(text, "accepted", attempted=True, candidate=text)
