"""Reuse AM's real delivered-preview binding for one-click batch confirmation.

This is NOT a webhook signature verifier and does not send preview cards. Its
reader must use the existing AM card/outbox + readback records. It must never
construct a successful record merely because an LLM says the user saw a draft.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import hashlib
import json
from typing import Callable

from .followup import Actor, ApprovalRequest, BatchResult, FollowupCoordinator, FollowupError


@dataclass(frozen=True)
class PreviewRowBinding:
    preview_id: str
    content_hash: str
    # Secret token retained in the backend preview, not printed in logs.
    token: str


def manifest_hash(rows: tuple[PreviewRowBinding, ...]) -> str:
    pairs = sorted((r.preview_id, r.content_hash) for r in rows)
    return hashlib.sha256(json.dumps(pairs, separators=(",", ":")).encode()).hexdigest()


@dataclass(frozen=True)
class DeliveredPreview:
    tenant_id: str
    owner_id: str
    chat_id: str
    message_id: str
    view_id: str
    rows: tuple[PreviewRowBinding, ...]
    delivered_at: datetime
    valid_until: datetime
    source_record_id: str
    verified: bool


@dataclass(frozen=True)
class VerifiedCallback:
    # Construct ONLY after original SDK/event authentication and replay checks.
    tenant_id: str
    app_id: str
    actor_id: str
    chat_id: str
    source_message_id: str
    event_id: str
    occurred_at: datetime
    view_id: str
    manifest_hash: str
    selected_preview_ids: tuple[str, ...]
    action: str


class ConfirmationBridge:
    def __init__(self, coordinator: FollowupCoordinator, *, expected_app_id: str,
                 read_delivered: Callable[[str, str], DeliveredPreview],
                 clock: Callable[[], datetime] | None = None):
        if not expected_app_id:
            raise ValueError("APP_ID_REQUIRED")
        self.coordinator = coordinator
        self.app_id, self.read_delivered = expected_app_id, read_delivered
        self.clock = clock or (lambda: datetime.now(timezone.utc))

    def confirm(self, actor: Actor, callback: VerifiedCallback) -> list[BatchResult]:
        """One actual user click; rows return independent truthful outcomes."""
        now = self.clock()
        if callback.action != "confirm_send_once":
            raise FollowupError("callback_is_not_send_confirmation")
        if (callback.tenant_id, callback.actor_id, callback.app_id) != (
                actor.tenant_id, actor.user_id, self.app_id) or not callback.event_id:
            raise FollowupError("callback_actor_scope_mismatch")
        record = self.read_delivered(actor.tenant_id, callback.view_id)
        if (record.tenant_id, record.owner_id, record.chat_id, record.message_id, record.view_id) != (
                actor.tenant_id, actor.user_id, callback.chat_id, callback.source_message_id, callback.view_id):
            raise FollowupError("callback_preview_binding_mismatch")
        if (record.verified is not True or not record.source_record_id or not record.message_id or
                any(t.tzinfo is None for t in (record.delivered_at, record.valid_until, callback.occurred_at, now))):
            raise FollowupError("preview_delivery_not_verified")
        if (record.valid_until <= now or callback.occurred_at >= record.valid_until or callback.occurred_at < record.delivered_at or
                callback.occurred_at > now + timedelta(seconds=30)):
            raise FollowupError("preview_or_callback_time_invalid")
        if not record.rows or len({r.preview_id for r in record.rows}) != len(record.rows):
            raise FollowupError("preview_rows_invalid")
        if callback.manifest_hash != manifest_hash(record.rows):
            raise FollowupError("displayed_manifest_changed")
        selected = callback.selected_preview_ids
        row_map = {r.preview_id:r for r in record.rows}
        if (not selected or len(set(selected)) != len(selected) or
                any(i not in row_map for i in selected)):
            raise FollowupError("selection_not_in_displayed_preview")
        return self.coordinator.approve_batch(actor, [
            ApprovalRequest(i, row_map[i].token, row_map[i].content_hash) for i in selected
        ])

