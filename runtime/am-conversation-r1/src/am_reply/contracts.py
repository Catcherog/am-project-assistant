"""Validate complete host clauses, then realize only selections of those clauses.

We deliberately do NOT call a token/number comparison a semantic proof.
The proof boundary is an intact, host-approved variant. Correctness of that
variant against its source is the upstream adapter's responsibility.
"""
from __future__ import annotations

import json
import re
from .types import Claim, ContractError, ReplyContract

ID = re.compile(r"[a-zA-Z][a-zA-Z0-9_-]{0,63}\Z")
MENTION = re.compile(r"<\s*/?\s*at\b", re.I)
CONTROL = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f]")


def validate_contract(contract: ReplyContract) -> None:
    if not isinstance(contract, ReplyContract):
        raise ContractError("INVALID_CONTRACT")
    if not contract.contract_id or not contract.scope_key or not contract.source_version:
        raise ContractError("CONTRACT_BINDING_REQUIRED")
    if (type(contract.max_chars) is not int or not 80 <= contract.max_chars <= 6000
            or type(contract.max_groups) is not int or not 1 <= contract.max_groups <= 12
            or type(contract.preserve_order) is not bool
            or type(contract.require_record_attribution) is not bool):
        raise ContractError("INVALID_CONTRACT_LIMITS")
    if type(contract.claims) is not tuple or not 1 <= len(contract.claims) <= 24:
        raise ContractError("INVALID_CLAIMS")
    ids = [claim.claim_id for claim in contract.claims if isinstance(claim, Claim)]
    if len(ids) != len(contract.claims) or len(ids) != len(set(ids)) or any(not ID.fullmatch(v) for v in ids):
        raise ContractError("INVALID_CLAIM_ID")
    if not any(c.required for c in contract.claims):
        raise ContractError("REQUIRED_CLAIM_MISSING")
    for claim in contract.claims:
        if type(claim.required) is not bool or not claim.source_ids or not all(type(s) is str and s for s in claim.source_ids):
            raise ContractError("SOURCE_BINDING_REQUIRED")
        if (type(claim.variants) is not tuple or not 1 <= len(claim.variants) <= 8
                or len(set(claim.variants)) != len(claim.variants)):
            raise ContractError("INVALID_VARIANTS")
        for value in claim.variants:
            if type(value) is not str or not value.strip() or len(value) > 5000:
                raise ContractError("INVALID_VARIANT_TEXT")
            if MENTION.search(value) or CONTROL.search(value):
                raise ContractError("UNSAFE_MARKUP")
            if any(literal and literal in value for literal in contract.forbidden_literals):
                raise ContractError("INTERNAL_METADATA_IN_VARIANT")
        if (type(claim.depends_on) is not tuple or any(d not in ids or d == claim.claim_id for d in claim.depends_on)
                or len(set(claim.depends_on)) != len(claim.depends_on)):
            raise ContractError("INVALID_DEPENDENCY")
    # Cycles have no useful omission semantics; reject miscompiled contracts.
    by_id = {c.claim_id: c for c in contract.claims}
    active, done = set(), set()
    def visit(key):
        if key in active:
            raise ContractError("CYCLIC_DEPENDENCY")
        if key in done:
            return
        active.add(key)
        for child in by_id[key].depends_on:
            visit(child)
        active.remove(key)
        done.add(key)
    for key in by_id:
        visit(key)


def _pairs(pairs):
    value = {}
    for key, item in pairs:
        if key in value:
            raise ContractError("DUPLICATE_JSON_KEY")
        value[key] = item
    return value


def parse_candidate(value, max_chars=5000):
    if type(value) is str:
        if len(value) > max_chars:
            raise ContractError("CANDIDATE_TOO_LARGE")
        try:
            value = json.loads(value, object_pairs_hook=_pairs,
                               parse_constant=lambda _: (_ for _ in ()).throw(ContractError("INVALID_JSON_NUMBER")))
        except (ValueError, RecursionError) as exc:
            if isinstance(exc, ContractError):
                raise
            raise ContractError("INVALID_JSON") from None
    if type(value) is not dict:
        raise ContractError("INVALID_CANDIDATE")
    return value


def _end(value):
    value = value.strip()
    return value if value.endswith(("。", "！", "？", ".", "!", "?")) else value + "。"


def _render_group(texts, joiner):
    if joiner == "space":
        return " ".join(_end(t) for t in texts)
    # Only remove sentence punctuation, never qualifiers/negation/parentheses.
    return "；".join(t.strip().removesuffix("。").removesuffix("；") for t in texts) + "。"


def render_candidate(contract: ReplyContract, candidate: dict) -> str:
    validate_contract(contract)
    if set(candidate) != {"contract_fingerprint", "groups", "lead_in"}:
        raise ContractError("UNEXPECTED_CANDIDATE_FIELDS")
    if candidate["contract_fingerprint"] != contract.fingerprint():
        raise ContractError("CONTRACT_FINGERPRINT_MISMATCH")
    if candidate["lead_in"] not in ("none", "record"):
        raise ContractError("INVALID_LEAD_IN")
    if contract.require_record_attribution and candidate["lead_in"] != "record":
        raise ContractError("SOURCE_ATTRIBUTION_REQUIRED")
    groups = candidate["groups"]
    if type(groups) is not list or not 1 <= len(groups) <= contract.max_groups:
        raise ContractError("INVALID_GROUPS")
    by_id = {c.claim_id: c for c in contract.claims}
    seen, ordered, rendered = set(), [], []
    for group in groups:
        if type(group) is not dict or set(group) != {"items", "joiner"} or group["joiner"] not in ("semicolon", "space"):
            raise ContractError("INVALID_GROUP")
        if type(group["items"]) is not list or not 1 <= len(group["items"]) <= 24:
            raise ContractError("INVALID_ITEMS")
        texts = []
        for item in group["items"]:
            if type(item) is not dict or set(item) != {"claim_id", "variant"}:
                raise ContractError("INVALID_ITEM")
            key, index = item["claim_id"], item["variant"]
            if type(key) is not str or key not in by_id or key in seen:
                raise ContractError("UNKNOWN_OR_DUPLICATE_CLAIM")
            claim = by_id[key]
            if type(index) is not int or not 0 <= index < len(claim.variants):
                raise ContractError("INVALID_VARIANT_INDEX")
            seen.add(key)
            ordered.append(key)
            texts.append(claim.variants[index])
        rendered.append(_render_group(texts, group["joiner"]))
    if any(c.required and c.claim_id not in seen for c in contract.claims):
        raise ContractError("REQUIRED_CLAIM_OMITTED")
    if any(not set(by_id[key].depends_on) <= seen for key in seen):
        raise ContractError("DEPENDENCY_OMITTED")
    original_order = [c.claim_id for c in contract.claims if c.claim_id in seen]
    if contract.preserve_order and ordered != original_order:
        raise ContractError("CLAIM_ORDER_CHANGED")
    text = "\n".join(rendered)
    if candidate["lead_in"] == "record":
        text = "按现有记录，" + text
    if len(text) > contract.max_chars:
        raise ContractError("REPLY_TOO_LONG")
    if MENTION.search(text) or CONTROL.search(text):
        raise ContractError("UNSAFE_MARKUP")
    if any(v and v in text for v in contract.forbidden_literals):
        raise ContractError("INTERNAL_METADATA_IN_REPLY")
    return text


def default_plan(contract: ReplyContract) -> dict:
    validate_contract(contract)
    required = {c.claim_id for c in contract.claims if c.required}
    by_id = {c.claim_id: c for c in contract.claims}
    todo = list(required)
    while todo:
        for key in by_id[todo.pop()].depends_on:
            if key not in required:
                required.add(key)
                todo.append(key)
    return {
        "contract_fingerprint": contract.fingerprint(),
        "lead_in": "record" if contract.require_record_attribution else "none",
        "groups": [{"items": [{"claim_id": c.claim_id, "variant": 0} for c in contract.claims if c.claim_id in required],
                    "joiner": "semicolon"}],
    }


def fallback_text(contract: ReplyContract) -> str:
    return render_candidate(contract, default_plan(contract))
