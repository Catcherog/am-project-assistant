"""Explicit feature configuration. Production integration is opt-in."""
from __future__ import annotations

from dataclasses import dataclass, fields
import math
from typing import Any, Mapping


@dataclass(frozen=True)
class HarnessConfig:
    enabled: bool = False
    fastpath_enabled: bool = False
    control_enabled: bool = False
    durable_queue_enabled: bool = False
    execution_seconds: float = 120.0
    content_ttl_seconds: float = 600.0
    lease_seconds: float = 30.0
    heartbeat_seconds: float = 5.0
    max_queued: int = 32
    aging_seconds: float = 30.0

    def __post_init__(self) -> None:
        for name in ("enabled", "fastpath_enabled", "control_enabled", "durable_queue_enabled"):
            if type(getattr(self, name)) is not bool:
                raise ValueError("INVALID_HARNESS_CONFIG")
        ranges = {"execution_seconds": (1, 120), "content_ttl_seconds": (1, 86400),
                  "lease_seconds": (1, 300), "heartbeat_seconds": (.05, 60),
                  "aging_seconds": (1, 3600)}
        for name, (lo, hi) in ranges.items():
            value = getattr(self, name)
            if type(value) not in (int, float) or not math.isfinite(value) or not lo <= value <= hi:
                raise ValueError("INVALID_HARNESS_CONFIG")
        if type(self.max_queued) is not int or not 1 <= self.max_queued <= 4096:
            raise ValueError("INVALID_HARNESS_CONFIG")
        if self.heartbeat_seconds * 2 >= self.lease_seconds:
            raise ValueError("HEARTBEAT_MUST_PRECEDE_LEASE_EXPIRY")
        if not self.enabled and any((self.fastpath_enabled, self.control_enabled, self.durable_queue_enabled)):
            raise ValueError("HARNESS_FEATURE_WITHOUT_ENABLE")

    @classmethod
    def from_mapping(cls, settings: Mapping[str, Any]) -> "HarnessConfig":
        if not isinstance(settings, Mapping) or set(settings) - {f.name for f in fields(cls)}:
            raise ValueError("INVALID_HARNESS_CONFIG")
        return cls(**dict(settings))
