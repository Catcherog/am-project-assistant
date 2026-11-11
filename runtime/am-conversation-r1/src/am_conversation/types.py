"""Shared contracts. AccessContext is constructed by a trusted ingress adapter only."""
from __future__ import annotations

from dataclasses import dataclass, field
import hashlib
import json
from typing import Any, Protocol


class ConversationError(RuntimeError):
    """Stable machine-readable error; never include credentials in its text."""


def canonical(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)


@dataclass(frozen=True)
class AccessContext:
    tenant_id: str
    app_id: str
    actor_id: str
    chat_id: str
    thread_id: str = ""
    allowed_project_ids: frozenset[str] = field(default_factory=frozenset)
    all_projects: bool = False
    is_owner: bool = False
    content_expires_at: float | None = None
    reference_project_ids: tuple[str, ...] = ()
    source_access_version: str | None = None

    def __post_init__(self) -> None:
        if self.source_access_version is not None and (not isinstance(self.source_access_version,str) or not self.source_access_version or len(self.source_access_version)>128):
            raise ConversationError('INVALID_SOURCE_ACCESS_VERSION')
        for name in ("tenant_id", "app_id", "actor_id", "chat_id"):
            value = getattr(self, name)
            if not isinstance(value, str) or not value or len(value) > 256:
                raise ConversationError("INVALID_CONTEXT")
        if not isinstance(self.thread_id, str) or len(self.thread_id) > 256:
            raise ConversationError("INVALID_CONTEXT")
        if type(self.all_projects) is not bool or type(self.is_owner) is not bool:
            raise ConversationError("INVALID_CONTEXT")
        if isinstance(self.allowed_project_ids, str):
            raise ConversationError("INVALID_CONTEXT")
        projects = frozenset(self.allowed_project_ids)
        if any(not isinstance(p, str) or not p or len(p) > 256 for p in projects):
            raise ConversationError("INVALID_CONTEXT")
        object.__setattr__(self, "allowed_project_ids", projects)
        references=tuple(self.reference_project_ids)
        if any(not isinstance(p,str) or not self.can_access_project(p) for p in references):raise ConversationError('INVALID_CONTEXT')
        object.__setattr__(self,'reference_project_ids',references)
        if self.content_expires_at is not None:
            import math
            if type(self.content_expires_at) not in (int,float) or not math.isfinite(self.content_expires_at) or self.content_expires_at<=0:
                raise ConversationError('INVALID_CONTEXT')

    @property
    def scope(self) -> tuple[str, str, str, str, str]:
        return (self.tenant_id, self.app_id, self.actor_id, self.chat_id, self.thread_id)

    @property
    def session_id(self) -> str:
        return hashlib.sha256(canonical(self.scope).encode()).hexdigest()

    def can_access_project(self, project_id: str) -> bool:
        return bool(project_id) and (self.all_projects or project_id in self.allowed_project_ids)


@dataclass(frozen=True)
class ToolCall:
    id: str
    name: str
    arguments: dict[str, Any]


@dataclass(frozen=True)
class ModelTurn:
    text: str = ""
    tool_calls: tuple[ToolCall, ...] = ()
    response_plan: dict[str, Any] | None = None


class Model(Protocol):
    def complete(self, messages: list[dict[str, Any]], tools: list[dict[str, Any]], *, request_id: str = "") -> ModelTurn: ...


@dataclass(frozen=True)
class AgentReply:
    event_id: str
    text: str
    status: str = "answered"
    replayed: bool = False
    pending_actions: tuple[str, ...] = ()
    source_ids: tuple[str, ...] = ()
    terminal_json: str | None = None

    def to_dict(self) -> dict[str, Any]:
        value = {"event_id": self.event_id, "text": self.text, "status": self.status,
                "replayed": self.replayed, "pending_actions": list(self.pending_actions),
                "source_ids": list(self.source_ids)}
        if self.terminal_json is not None:
            value['terminal_json'] = self.terminal_json
        return value
