from __future__ import annotations

from dataclasses import asdict, dataclass, field
import hashlib
import json
import math
from typing import Any


class ContractError(ValueError):
    pass


def dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)


def key(*parts: Any) -> str:
    return hashlib.sha256(dumps(parts).encode()).hexdigest()


@dataclass(frozen=True)
class Event:
    """Trusted normalized input. Scope and mentions come from host metadata, not AI."""
    tenant_id: str
    native_id: str
    chat_id: str
    sender_id: str
    body: str
    original_time: float
    channel: str = "group"  # bot_dm | personal_dm | group | resource
    kind: str = "message"
    thread_id: str = ""
    parent_id: str = ""
    root_id: str = ""
    mentions: tuple[str, ...] = ()
    project_ids: tuple[str, ...] = ()
    attachments: tuple[dict, ...] = ()
    revision: str = ""
    version_time: float = 0.0
    observed_via: str = "event"
    is_self: bool = False
    is_deleted: bool = False
    direction: str = "inbound"
    content_status: str = "parsed"

    @property
    def item_id(self) -> str:
        return key(self.tenant_id, self.kind, self.native_id)

    @property
    def content_hash(self) -> str:
        # Delivery-specific event IDs, mention display names and resource enrichment
        # do not create another reply. Edits/retractions do create a new fact version.
        return key(self.body, self.is_deleted)

    def validate(self) -> None:
        for n in ("tenant_id", "native_id", "sender_id"):
            if not isinstance(getattr(self, n), str) or not getattr(self, n):
                raise ContractError("missing " + n)
        if not isinstance(self.body, str):
            raise ContractError("body must be text")
        if self.channel not in {"bot_dm", "personal_dm", "group", "resource"}:
            raise ContractError("invalid channel")
        if self.channel != "resource" and not self.chat_id:
            raise ContractError("missing chat_id")
        for n in ("original_time", "version_time"):
            v = getattr(self, n)
            if isinstance(v, bool) or not isinstance(v, (float, int)) or not math.isfinite(v) or v < 0:
                raise ContractError("invalid " + n)
        if self.direction not in {"inbound", "outbound"}:
            raise ContractError("invalid direction")
        dumps(asdict(self))

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, raw: dict) -> Event:
        data = dict(raw)
        for n in ("mentions", "project_ids", "attachments"):
            data[n] = tuple(data.get(n, ()))
        value = cls(**data)
        value.validate()
        return value


@dataclass(frozen=True)
class Config:
    tenant_id: str
    bot_id: str
    owner_id: str
    owner_chat_id: str
    live_cutover_at: float
    conversation_ttl_seconds: float = 86400
    recent_limit: int = 10
    context_char_budget: int = 16000
    batch_wait_seconds: float = 2.0
    batch_max_items: int = 20
    batch_max_characters: int = 8000
    snapshot_stale_seconds: float = 86400
    send_hold: bool = False
    dialogue_workers: int = 2
    business_workers: int = 2
    sender_workers: int = 1
    job_lease_seconds: float = 120
    worker_timeout_seconds: float = 90
    worker_timeouts: dict[str, float] = field(default_factory=lambda: {
        "grok_bot":30,"grok_cli":180,"qwen":45,"codex_cli":600,
    })
    bot_background_jobs: bool = False
    bot_global_periodic_scan: bool = False
    ledger_write: bool = False
    # A different group cannot read a project just because it has the same name.
    project_chats: dict[str, tuple[str, ...]] = field(default_factory=dict)
    shared_resource_chats: dict[str, tuple[str, ...]] = field(default_factory=dict)
    worker_map: dict[str, str] = field(default_factory=lambda: {
        "dialogue_reply": "grok_bot",
        "personal_context_update": "grok_cli",
        "project_state_update": "grok_cli",
        "project_analysis": "grok_cli",
        "followup_draft": "grok_cli",
        "document_content": "grok_cli",
        "tag_candidates": "qwen",
        "normalize_text": "qwen",
        "build_document": "codex_cli",
        "code_task": "codex_cli",
    })

    def validate(self) -> None:
        if not all((self.tenant_id, self.bot_id, self.owner_id, self.owner_chat_id)):
            raise ContractError("tenant/bot/owner/owner_chat bindings required")
        if not isinstance(self.live_cutover_at,(int,float)) or isinstance(self.live_cutover_at,bool) or not math.isfinite(self.live_cutover_at) or self.live_cutover_at<0:
            raise ContractError("invalid live_cutover_at")
        if self.bot_background_jobs or self.bot_global_periodic_scan:
            raise ContractError("Grok Bot is reserved for dialogue")
        if self.worker_map.get("dialogue_reply") != "grok_bot":
            raise ContractError("dialogue_reply must use grok_bot")
        if any(v == "grok_bot" and k != "dialogue_reply" for k, v in self.worker_map.items()):
            raise ContractError("background work cannot use grok_bot")
        if not 1 <= self.recent_limit <= 12 or self.context_char_budget < 1000:
            raise ContractError("invalid context budget")
        if self.job_lease_seconds <= self.worker_timeout_seconds:
            raise ContractError("job lease must exceed worker timeout")
        if any(not isinstance(v,(int,float)) or isinstance(v,bool) or not math.isfinite(v) or v<=0 for v in self.worker_timeouts.values()):
            raise ContractError("invalid worker timeouts")


@dataclass(frozen=True)
class SendReceipt:
    status: str  # sent | failed (known not sent) | unknown
    platform_id: str = ""
    detail: str = ""


@dataclass(frozen=True)
class Admission:
    item_id: str
    inserted: bool
    changed: bool
    handled: bool
