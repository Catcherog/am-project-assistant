"""Feishu envelope and exact readback contracts; NOT a transport client.

Call from the existing BusinessPort. Preserve its SDK client, token management,
single consumer, retry classification and retained send action. No sockets,
background loops, credentials, or real messages are created by this module.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import json
from typing import Callable

from .followup import (DispatchRequest, GateResult, ReadbackEvidence, Target)
from .rendering import feishu_post


@dataclass(frozen=True)
class RecipientPin:
    tenant_id: str
    app_id: str
    kind: str
    target_id: str
    exact_chat_id: str
    directory_version: str
    valid_until: datetime
    attention_ids: tuple[str, ...]
    bot_open_id: str
    reachable: bool
    internal: bool
    members_verified: bool


class PinnedRecipientGate:
    def __init__(self, app_id: str, resolve: Callable[[str, Target, datetime], RecipientPin]):
        if not app_id:
            raise ValueError("APP_ID_REQUIRED")
        self.app_id, self.resolve = app_id, resolve

    def pin(self, tenant_id: str, target: Target, now: datetime) -> RecipientPin:
        pin = self.resolve(tenant_id, target, now)
        if (pin.tenant_id, pin.app_id, pin.kind, pin.target_id, pin.directory_version) != (
                tenant_id, self.app_id, target.kind, target.target_id, target.directory_version):
            raise ValueError("RECIPIENT_PIN_CHANGED")
        if (not pin.exact_chat_id or not pin.bot_open_id or pin.reachable is not True or pin.internal is not True or
                pin.members_verified is not True or pin.valid_until.tzinfo is None or pin.valid_until <= now):
            raise ValueError("RECIPIENT_UNVERIFIED")
        if sorted(pin.attention_ids) != sorted(target.attention_ids):
            raise ValueError("RECIPIENT_PEOPLE_CHANGED")
        if target.kind == "group" and pin.exact_chat_id != target.target_id:
            raise ValueError("GROUP_ID_CHANGED")
        return pin

    def __call__(self, tenant_id: str, target: Target, now: datetime) -> GateResult:
        try:
            self.pin(tenant_id, target, now)
            return GateResult(True)
        except Exception:
            return GateResult(False, "recipient_current_scope_unverified")


@dataclass(frozen=True)
class FeishuEnvelope:
    receive_id_type: str
    receive_id: str
    msg_type: str
    content: str
    uuid: str


def envelope(request: DispatchRequest, pin: RecipientPin) -> FeishuEnvelope:
    t = request.draft.target
    if (pin.tenant_id, pin.kind, pin.target_id, pin.directory_version) != (
            request.tenant_id, t.kind, t.target_id, t.directory_version):
        raise ValueError("PIN_REQUEST_MISMATCH")
    # The original caller must first obtain this pin via PinnedRecipientGate.
    if t.kind == "group":
        content = feishu_post(request.draft.body, t.mention_ids)
        return FeishuEnvelope("chat_id", t.target_id, "post",
                              json.dumps(content, ensure_ascii=False, separators=(",", ":")), request.request_uuid)
    if t.kind not in {"owner", "recipient_p2p"}:
        raise ValueError("UNSUPPORTED_TARGET")
    return FeishuEnvelope("open_id", t.target_id, "text",
                          json.dumps({"text":request.draft.body}, ensure_ascii=False, separators=(",", ":")), request.request_uuid)


@dataclass(frozen=True)
class SendRecordBinding:
    # These fields come from the original persistent AM attempt. A text search
    # cannot create a binding; the action must be linked to this request UUID.
    request_uuid: str
    external_action_id: str
    response_message_id: str


@dataclass(frozen=True)
class NormalizedMessage:
    # Normalize using the existing ReminderPort reader/semantic_content.
    tenant_id: str
    message_id: str
    chat_id: str
    sender_id: str
    sender_type: str
    deleted: bool
    plain_body: str
    mention_ids: tuple[str, ...]
    source_record_id: str
    observed_at: datetime


def exact_readback(request: DispatchRequest, pin: RecipientPin, binding: SendRecordBinding,
                   message: NormalizedMessage) -> ReadbackEvidence | None:
    """Never infer delivery from same text, empty search, or an arbitrary ID."""
    if (pin.tenant_id, pin.kind, pin.target_id, pin.directory_version) != (
            request.tenant_id, request.draft.target.kind, request.draft.target.target_id,
            request.draft.target.directory_version):
        return None
    if (not binding.external_action_id or not binding.response_message_id or
            binding.request_uuid != request.request_uuid or
            message.message_id != binding.response_message_id or
            message.tenant_id != request.tenant_id or
            message.chat_id != pin.exact_chat_id or message.sender_id != pin.bot_open_id or
            message.sender_type not in {"app", "bot"} or message.deleted or
            message.plain_body != request.draft.body or
            sorted(message.mention_ids) != sorted(request.draft.target.mention_ids) or
            not message.source_record_id or message.observed_at.tzinfo is None):
        return None
    return ReadbackEvidence(request.request_uuid, binding.external_action_id, message.message_id,
                            request.draft.target.kind, request.draft.target.target_id,
                            request.body_sha256, request.draft.target.mention_ids,
                            "send_response_message_id", message.source_record_id, message.observed_at)

