"""Shared contracts. Times are UTC Unix milliseconds; presentation uses Shanghai."""
from __future__ import annotations

from dataclasses import dataclass, field, asdict
from typing import Any, Mapping, Protocol, Callable
import hashlib
import json
import time


def now_ms() -> int:
    return time.time_ns() // 1_000_000


def canonical(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)


def digest(value: Any) -> str:
    return hashlib.sha256(canonical(value).encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class Principal:
    user_id: str
    tenant_id: str
    groups: tuple[str, ...] = ()
    output_chat_id: str | None = None


@dataclass(frozen=True)
class Source:
    source_id: str
    kind: str
    remote_id: str
    tenant_id: str
    owner_user_id: str
    readers: tuple[str, ...] = ()
    output_chats: tuple[str, ...] = ()
    enabled: bool = False
    start_ms: int = 0
    poll_seconds: int = 180
    reconcile_seconds: int = 3600
    overlap_seconds: int = 300
    raw_retention_seconds: int = 86400
    settings: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        for name in ("source_id", "kind", "remote_id", "tenant_id", "owner_user_id"):
            if not getattr(self, name):
                raise ValueError(f"{name} is required")
        if self.start_ms < 0 or self.poll_seconds < 1 or self.reconcile_seconds < 1:
            raise ValueError("Invalid time bounds")
        if self.overlap_seconds < 0 or self.raw_retention_seconds < 1:
            raise ValueError("Invalid retention/overlap")

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> Source:
        data = dict(value)
        data["readers"] = tuple(data.get("readers", ()))
        data["output_chats"] = tuple(data.get("output_chats", ()))
        return cls(**data)


@dataclass(frozen=True)
class Observation:
    source_id: str
    object_id: str
    version: str
    # Source revision/update time; NOT the local receive time. Creation time,
    # if distinct, belongs in payload['created_at_ms'] for scope enforcement.
    occurred_at_ms: int
    text: str = ""
    payload: dict[str, Any] = field(default_factory=dict)
    deleted: bool = False
    completeness: str = "complete"
    parent_id: str | None = None
    url: str | None = None

    def __post_init__(self) -> None:
        if not self.source_id or not self.object_id or not self.version or self.occurred_at_ms < 0:
            raise ValueError("Observation identity/time is required")
        if self.completeness not in {"complete", "partial", "unreadable"}:
            raise ValueError("Invalid completeness")
        canonical(self.payload)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> Observation:
        return cls(**dict(data))


@dataclass(frozen=True)
class Discovery:
    kind: str
    remote_id: str
    reason: str
    settings: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class Page:
    observations: tuple[Observation, ...]
    next_cursor: str | None = None
    discoveries: tuple[Discovery, ...] = ()
    # False means a snapshot/object enumeration, not a continuous time-range proof.
    time_coverage: bool = False


class Adapter(Protocol):
    def fetch_page(self, source: Source, start_ms: int, end_ms: int,
                   cursor: str | None) -> Page: ...


class SyncError(RuntimeError):
    code = "sync_error"


class PermissionDenied(SyncError):
    code = "permission_denied"


class RetryableError(SyncError):
    code = "retryable_error"

    def __init__(self, message: str = "Temporary source failure", retry_after: float | None = None):
        super().__init__(message)
        self.retry_after = retry_after


class InvalidCursor(SyncError):
    code = "invalid_cursor"


class SnapshotChanged(SyncError):
    code = "snapshot_changed"


class ScopeViolation(SyncError):
    code = "scope_violation"


class IntegrityConflict(SyncError):
    code = "integrity_conflict"


class LeaseLost(SyncError):
    code = "lease_lost"


Clock = Callable[[], int]
