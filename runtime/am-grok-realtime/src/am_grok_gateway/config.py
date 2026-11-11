from __future__ import annotations

from dataclasses import dataclass
import math

from .models import ContractError


@dataclass(frozen=True)
class GatewayConfig:
    enabled: bool = False
    allow_private: bool = False
    allowed_chat_ids: tuple[str, ...] = ()
    allowed_sender_ids: tuple[str, ...] = ()
    max_pending: int = 128
    max_inflight: int = 4
    max_reconcile_inflight: int = 1
    reply_workers: int = 2
    request_ttl_seconds: float = 30.0
    max_message_age_seconds: float = 600.0
    tick_seconds: float = 0.025
    callback_url: str = ""
    # A label used for evidence; it does not assert the provider's capabilities.
    backend_profile: str = "grok-bot-interactive"

    def validate(self) -> None:
        if type(self.enabled) is not bool or type(self.allow_private) is not bool:
            raise ContractError("invalid config flags")
        for key, low, high in (
            ("max_pending", 1, 10000), ("max_inflight", 1, 32),
            ("max_reconcile_inflight", 0, 32), ("reply_workers", 1, 16),
        ):
            value = getattr(self, key)
            if type(value) is not int or not low <= value <= high:
                raise ContractError(f"invalid {key}")
        if self.max_reconcile_inflight > self.max_inflight:
            raise ContractError("reconcile concurrency exceeds capacity")
        for key, low, high in (
            ("request_ttl_seconds", 1.0, 300.0),
            ("max_message_age_seconds", 1.0, 3600.0),
            ("tick_seconds", 0.005, 0.25),
        ):
            value = getattr(self, key)
            if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or not low <= value <= high:
                raise ContractError(f"invalid {key}")
        for values in (self.allowed_chat_ids, self.allowed_sender_ids):
            if not isinstance(values, (tuple, list)) or any(not isinstance(v, str) or not v for v in values):
                raise ContractError("invalid configured scope")
        if self.enabled and not (self.allow_private or self.allowed_chat_ids):
            raise ContractError("enabled gateway has no explicit ingress scope")
