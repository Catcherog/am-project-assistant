"""Versioned, bounded contracts. Only the trusted AM host creates InboundMessage.

No HTTP endpoint in this package accepts a caller-supplied identity/permission
claim as ingress. The external Bot returns candidates, never execution grants.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass, field
import hashlib
import json
import math
from typing import Any, Mapping, Protocol


class ContractError(ValueError):
    pass


def bounded_text(value: Any, name: str, maximum: int, *, empty: bool = False) -> str:
    if not isinstance(value, str) or (not empty and not value.strip()) or len(value) > maximum:
        raise ContractError(f"invalid {name}")
    if "\x00" in value:
        raise ContractError(f"invalid {name}")
    return value


def finite_time(value: Any, name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value < 0:
        raise ContractError(f"invalid {name}")
    return float(value)


def canonical_json(value: Any) -> str:
    try:
        return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)
    except (TypeError, ValueError) as exc:
        raise ContractError("not finite JSON") from exc


@dataclass(frozen=True)
class AuthContext:
    verified: bool
    expires_at: float
    policy_version: str
    disclosure: str = "general_only"
    allowed_source_ids: tuple[str, ...] = ()
    is_owner: bool = False

    def validate(self) -> None:
        if type(self.verified) is not bool or type(self.is_owner) is not bool:
            raise ContractError("invalid identity flags")
        finite_time(self.expires_at, "auth expiry")
        bounded_text(self.policy_version, "policy_version", 256)
        if self.disclosure not in {"general_only", "project"}:
            raise ContractError("invalid disclosure")
        if not isinstance(self.allowed_source_ids, (list, tuple)) or len(self.allowed_source_ids) > 64:
            raise ContractError("too many sources")
        for source in self.allowed_source_ids:
            bounded_text(source, "source_id", 256)


@dataclass(frozen=True)
class InboundMessage:
    tenant_id: str
    app_id: str
    event_id: str
    message_id: str
    chat_id: str
    chat_type: str
    sender_id: str
    text: str
    event_created_at: float
    received_at: float
    auth: AuthContext
    origin: str = "realtime"
    mentions_bot: bool = False
    sender_is_bot: bool = False
    context: dict[str, Any] = field(default_factory=dict)
    measurement_class: str = "ordinary"

    def validate(self) -> None:
        for name in ("tenant_id", "app_id", "event_id", "message_id", "chat_id", "sender_id"):
            bounded_text(getattr(self, name), name, 256)
        bounded_text(self.text, "text", 16000)
        finite_time(self.event_created_at, "event_created_at")
        finite_time(self.received_at, "received_at")
        if self.chat_type not in {"group", "p2p"} or self.origin not in {"realtime", "reconcile"}:
            raise ContractError("invalid ingress kind")
        if type(self.mentions_bot) is not bool or type(self.sender_is_bot) is not bool:
            raise ContractError("invalid ingress flags")
        if self.measurement_class not in {"ordinary", "fresh_lookup", "complex", "synthetic"}:
            raise ContractError("invalid measurement class")
        self.auth.validate()
        if not isinstance(self.context, dict) or len(canonical_json(self.context).encode()) > 32768:
            raise ContractError("context too large")
        sources = self.context.get("sources", [])
        if not isinstance(sources, list) or len(sources) > 64:
            raise ContractError("invalid sources")
        for source in sources:
            if not isinstance(source, dict):
                raise ContractError("invalid source")
            sid = bounded_text(source.get("source_id"), "source_id", 256)
            bounded_text(source.get("revision"), "revision", 256)
            if sid not in self.auth.allowed_source_ids:
                raise ContractError("source outside authorized scope")
        if self.auth.disclosure == "general_only" and sources:
            raise ContractError("project sources in general context")

    @property
    def dedupe_key(self) -> str:
        return hashlib.sha256(canonical_json([self.tenant_id, self.app_id, self.message_id]).encode()).hexdigest()

    @property
    def conversation_id(self) -> str:
        # Group+person isolation is retained; no backend-owned global conversation.
        return hashlib.sha256(canonical_json([self.tenant_id, self.app_id, self.chat_id, self.sender_id]).encode()).hexdigest()

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> InboundMessage:
        raw = dict(data)
        auth = dict(raw.pop("auth"))
        auth["allowed_source_ids"] = tuple(auth.get("allowed_source_ids", ()))
        value = cls(auth=AuthContext(**auth), **raw)
        value.validate()
        return value


@dataclass(frozen=True)
class BotResult:
    request_id: str
    attempt_id: str
    kind: str
    text: str = ""
    sources: tuple[dict[str, str], ...] = ()
    handoff: dict[str, Any] | None = None
    error_code: str = ""

    @classmethod
    def parse(cls, raw: Mapping[str, Any]) -> BotResult:
        if not isinstance(raw, dict) or raw.get("schema") != "am.grok.result.v1":
            raise ContractError("unsupported result schema")
        allowed = {"schema", "request_id", "attempt_id", "kind", "text", "sources", "handoff", "error_code"}
        if set(raw) - allowed:
            raise ContractError("unexpected result fields")
        request_id = bounded_text(raw.get("request_id"), "request_id", 128)
        attempt_id = bounded_text(raw.get("attempt_id"), "attempt_id", 128)
        kind = raw.get("kind")
        if kind not in {"answer", "handoff", "no_reply", "error"}:
            raise ContractError("invalid result kind")
        text = bounded_text(raw.get("text", ""), "result text", 8000, empty=kind != "answer")
        sources = raw.get("sources", [])
        if not isinstance(sources, list) or len(sources) > 64:
            raise ContractError("invalid result sources")
        for source in sources:
            if not isinstance(source, dict) or set(source) != {"source_id", "revision"}:
                raise ContractError("invalid result source")
            bounded_text(source["source_id"], "source_id", 256)
            bounded_text(source["revision"], "revision", 256)
        handoff = raw.get("handoff")
        if kind == "handoff":
            if not isinstance(handoff, dict) or set(handoff) != {"workflow", "instruction"}:
                raise ContractError("invalid handoff")
            if handoff["workflow"] not in {"followup", "document", "analysis"}:
                raise ContractError("unsupported workflow")
            bounded_text(handoff["instruction"], "handoff instruction", 8000)
        elif handoff is not None:
            raise ContractError("handoff on non-handoff result")
        error_code = bounded_text(raw.get("error_code", ""), "error_code", 128, empty=True)
        return cls(request_id, attempt_id, kind, text, tuple(sources), handoff, error_code)

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["schema"] = "am.grok.result.v1"
        data["sources"] = list(self.sources)
        return data


@dataclass(frozen=True)
class Admission:
    handled: bool
    request_id: str = ""
    status: str = ""
    reason: str = ""


@dataclass(frozen=True)
class DispatchReceipt:
    # unknown is not retried automatically, even if a webhook timed out.
    status: str  # accepted | rejected | unknown
    run_id: str = ""
    detail: str = ""


@dataclass(frozen=True)
class SendReceipt:
    status: str  # sent | failed | unknown
    message_id: str = ""
    readback_at: float | None = None
    detail: str = ""


@dataclass(frozen=True)
class GuardDecision:
    allowed: bool
    useful: bool = False
    reason: str = ""


class BotTransport(Protocol):
    def submit(self, request: dict[str, Any]) -> DispatchReceipt: ...


class ReplySender(Protocol):
    def send(self, message: InboundMessage, text: str, idempotency_key: str) -> SendReceipt: ...


class ResultGuard(Protocol):
    def authorize(self, message: InboundMessage) -> GuardDecision: ...
    def validate(self, message: InboundMessage, result: BotResult) -> GuardDecision: ...


class BusinessHandoff(Protocol):
    def submit(self, message: InboundMessage, result: BotResult, idempotency_key: str) -> SendReceipt: ...
