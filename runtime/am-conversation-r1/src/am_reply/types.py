"""Host-owned contracts. These types never grant identity or execution rights.

Variants must be built by a trusted, evidence-aware adapter, not accepted from
the rewriting LLM. An intact variant is the minimum semantic protection unit.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Protocol
import hashlib
import json


class ContractError(ValueError):
    pass


def canonical(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)


def digest(value: Any) -> str:
    return hashlib.sha256(canonical(value).encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class Claim:
    claim_id: str
    variants: tuple[str, ...]
    source_ids: tuple[str, ...]
    required: bool = True
    depends_on: tuple[str, ...] = ()
    # Same-role names or dates are kept inside an intact clause. The model
    # cannot edit these strings or submit new variants.
    relation: str = "source_block"


@dataclass(frozen=True)
class ReplyContract:
    contract_id: str
    claims: tuple[Claim, ...]
    # Opaque binding provided by the host; not authorization by itself.
    source_version: str
    scope_key: str
    # Exactly matched internal values; never remove arbitrary hex/numbers.
    forbidden_literals: tuple[str, ...] = ()
    # Model order can be disabled for chronological/conditional narratives.
    preserve_order: bool = True
    max_chars: int = 500
    max_groups: int = 3
    origin: str = "host_structured"
    # Reading a ledger verifies access, not the underlying business fact.
    require_record_attribution: bool = False

    def fingerprint(self) -> str:
        from dataclasses import asdict
        return digest(asdict(self))


@dataclass(frozen=True)
class StyleProfile:
    profile_id: str = "owner-work-baseline"
    version: str = "1"
    instructions: tuple[str, ...] = (
        "用简体中文直接回答当前问题，结论或关键进展在前。",
        "自然、简洁、像日常同事交流；默认一到三句话。",
        "不使用审计腔、客服套话，不编造亲历、私下沟通和承诺。",
    )
    approved: bool = True
    # An empty scope means a shipped generic style, not access to personal data.
    scope_key: str = ""


@dataclass(frozen=True)
class RewriteConfig:
    mode: str = "off"  # off / shadow / enforce
    max_seconds: float = 6.0
    queue_seconds: float = 0.2
    min_remaining_seconds: float = 1.0
    finish_reserve_seconds: float = 0.3
    max_tokens: int = 512
    max_input_chars: int = 12000
    max_output_chars: int = 5000


@dataclass(frozen=True)
class RewriteRequest:
    event_id: str
    user_text: str
    draft: str
    contract: ReplyContract | None
    deadline: float
    style: StyleProfile = field(default_factory=StyleProfile)
    status: str = "answered"
    protected: bool = False
    pending_actions: tuple[str, ...] = ()


@dataclass(frozen=True)
class RewriteResult:
    text: str
    disposition: str
    reasons: tuple[str, ...] = ()
    attempted: bool = False
    candidate_text: str | None = None
    elapsed_seconds: float = 0.0
    contract_fingerprint: str | None = None
    prompt_version: str = "am-realizer-r2.1"

    def telemetry(self) -> dict:
        # No prompts, names, user text, raw clauses or candidate content.
        return {
            "disposition": self.disposition,
            "reasons": list(self.reasons),
            "attempted": self.attempted,
            "elapsed_seconds": self.elapsed_seconds,
            "contract_fingerprint": self.contract_fingerprint,
            "prompt_version": self.prompt_version,
            "output_chars": len(self.text),
        }


class RewriteModel(Protocol):
    def generate(self, payload: dict, *, timeout: float, queue_timeout: float,
                 max_tokens: int) -> str | dict: ...


Guard = Callable[[], None]
