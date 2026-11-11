"""Complete Feishu IM v1 message snapshots, separate from weak observations.

Only an authenticated raw get/list path may call this contract. A message body
cannot opt itself in. No network connection, credential, or clock is owned here.
"""
from __future__ import annotations

import json
from urllib.parse import quote

from .models import ContractError, Event, key


REVISION_PREFIX = "feishu-message-v1:"


class SnapshotIncomplete(ContractError):
    """The payload cannot establish a complete current attachment set."""


def _required_text(value, name):
    if not isinstance(value, str) or not value:
        raise SnapshotIncomplete("snapshot missing " + name)
    return value


def _milliseconds(value, name):
    # Raw REST uses decimal strings; the official SDK uses integers. Seconds,
    # floats and local read timestamps are deliberately not inferred here.
    if isinstance(value, bool) or not isinstance(value, (str, int)):
        raise SnapshotIncomplete("invalid native " + name)
    if isinstance(value, str) and (not value or not value.isascii() or not value.isdecimal()):
        raise SnapshotIncomplete("invalid native " + name)
    number = int(value)
    if not 0 <= number <= 2**53 - 1:
        raise SnapshotIncomplete("invalid native " + name)
    return number


def _content(raw, msg_type):
    if not isinstance(raw, str):
        raise SnapshotIncomplete("raw body.content must be JSON text")
    try:
        value = json.loads(raw)
    except (ValueError, TypeError) as exc:
        raise SnapshotIncomplete("invalid body.content JSON") from exc
    if not isinstance(value, dict) or "content_v2" in value:
        raise SnapshotIncomplete("unsupported complete content schema")
    attachments = []

    def resource(part, field, kind, required=True):
        resource_key = part.get(field)
        if not resource_key and not required:
            return
        _required_text(resource_key, field)
        file_name = part.get("file_name", "")
        if not isinstance(file_name, str):
            raise SnapshotIncomplete("invalid file_name")
        ref = {"kind": kind, "resource_key": resource_key,
               "file_name": file_name, "content_status": "indexed"}
        if ref not in attachments:
            attachments.append(ref)

    def post_text(text):
        if text.strip():
            return text
        # The shared-information projection requires nonempty source text.
        # A typed marker describes a textless post without inventing its content.
        names = [ref["file_name"] for ref in attachments if ref["file_name"]]
        return "[post]" + (" " + ", ".join(dict.fromkeys(names)) if names else "")

    if msg_type == "text":
        if not isinstance(value.get("text"), str):
            raise SnapshotIncomplete("text snapshot has no text")
        return value["text"], attachments, "parsed", value
    if msg_type in {"image", "file", "audio", "media", "sticker"}:
        if msg_type == "image":
            resource(value, "image_key", "image")
        else:
            resource(value, "file_key", msg_type)
            if msg_type == "media":
                resource(value, "image_key", "image", required=False)
        text = "[" + msg_type + "]" + (" " + value["file_name"] if value.get("file_name") else "")
        return text, attachments, "indexed", value
    if msg_type != "post":
        raise SnapshotIncomplete("unsupported complete message type: " + str(msg_type))

    # Official get/mget/list responses carry the independent attachment zone
    # in TOP-LEVEL files. Send/edit requests use key; raw responses use file_key.
    # Folder refs remain visible even though the single-file downloader skips them.
    if "files" in value:
        if not isinstance(value["files"], list):
            raise SnapshotIncomplete("invalid post files attachment zone")
        for item in value["files"]:
            if not isinstance(item, dict) or type(item.get("is_folder", False)) is not bool:
                raise SnapshotIncomplete("invalid post files entry")
            resource(item, "file_key", "folder" if item.get("is_folder") else "file")

    # REST commonly returns one post. Sent-message forms can contain language
    # variants; index every variant's resources and choose text deterministically.
    if isinstance(value.get("content"), list):
        if set(value) - {"content", "title", "files"}:
            raise SnapshotIncomplete("unsupported complete post fields")
        variants = [("", value)]
    else:
        variants = sorted(((k, v) for k, v in value.items() if k != "files"),
                          key=lambda pair: (pair[0] != "zh_cn", pair[0]))
        if not variants and "files" in value:
            return post_text(""), attachments, "indexed" if attachments else "parsed", value
        if not variants or any(not isinstance(v, dict) or not isinstance(v.get("content"), list)
                               or set(v) - {"title", "content"} for _, v in variants):
            raise SnapshotIncomplete("post snapshot has no complete content rows")
    bodies = []
    for _, post in variants:
        if "content_v2" in post:
            raise SnapshotIncomplete("content_v2 snapshot needs a verified parser")
        title = post.get("title", "")
        if not isinstance(title, str):
            raise SnapshotIncomplete("invalid post title")
        lines = [title] if title else []
        for row in post["content"]:
            if not isinstance(row, list):
                raise SnapshotIncomplete("incomplete post row")
            pieces = []
            for part in row:
                if not isinstance(part, dict):
                    raise SnapshotIncomplete("incomplete post node")
                tag = part.get("tag")
                if tag in {"text", "a"}:
                    if tag == "a":
                        _required_text(part.get("href"), "post link href")
                    text = part.get("text", part.get("href") if tag == "a" else None)
                    if not isinstance(text, str):
                        raise SnapshotIncomplete("invalid post text")
                    pieces.append(text)
                elif tag == "img":
                    resource(part, "image_key", "image")
                elif tag in {"media", "file"}:
                    resource(part, "file_key", tag)
                    if tag == "media":
                        resource(part, "image_key", "image", required=False)
                elif tag in {"at", "emotion", "hr"}:
                    pass  # Known non-resource nodes; mentions come from metadata.
                else:
                    raise SnapshotIncomplete("unsupported complete post node: " + str(tag))
            lines.append("".join(pieces))
        bodies.append("\n".join(lines))
    return post_text(bodies[0]), attachments, "parsed", value


def normalize_feishu_snapshot(message, config, source, *, expected_message_id=None):
    """Normalize a raw IM v1 Message from a host-authenticated get/list request.

    ``source`` is a registered/host-approved SourceSpec, never AI/body metadata.
    Absence of attachments is authoritative only after this parser succeeds.
    """
    source.validate()
    if source.tenant_id != config.tenant_id or source.container_type not in {"chat", "thread"}:
        raise ContractError("snapshot source scope mismatch")
    if not isinstance(message, dict):
        raise SnapshotIncomplete("raw message object required")
    message_id = _required_text(message.get("message_id"), "message_id")
    if expected_message_id is not None and message_id != expected_message_id:
        raise ContractError("snapshot message mismatch")
    if message.get("chat_id") != source.effective_chat_id:
        raise ContractError("snapshot chat mismatch")
    if source.container_type == "thread" and message.get("thread_id") != source.native_id:
        raise ContractError("snapshot thread mismatch")
    sender = message.get("sender")
    if not isinstance(sender, dict) or sender.get("id_type") not in {"open_id", "app_id"}:
        raise SnapshotIncomplete("raw sender id type required")
    sender_id = _required_text(sender.get("id"), "sender.id")
    created = _milliseconds(message.get("create_time"), "create_time")
    updated = _milliseconds(message.get("update_time"), "update_time")
    if updated < created:
        raise SnapshotIncomplete("update_time precedes create_time")
    if type(message.get("deleted")) is not bool:
        raise SnapshotIncomplete("raw deleted boolean required")
    deleted = message["deleted"]
    msg_type = _required_text(message.get("msg_type"), "msg_type")
    if deleted:
        body, attachments, status, content = "", [], "withdrawn", None
    else:
        raw_body = message.get("body")
        if not isinstance(raw_body, dict):
            raise SnapshotIncomplete("raw body required")
        body, attachments, status, content = _content(raw_body.get("content"), msg_type)
    mentions = []
    raw_mentions = message.get("mentions", [])
    if not isinstance(raw_mentions, list):
        raise SnapshotIncomplete("invalid mentions")
    for mention in raw_mentions:
        if not isinstance(mention, dict) or mention.get("id_type", "open_id") != "open_id":
            raise SnapshotIncomplete("invalid mention id type")
        mid = _required_text(mention.get("id"), "mention.id")
        if mid not in mentions:
            mentions.append(mid)
    thread, parent, root = (message.get(name) or "" for name in ("thread_id", "parent_id", "root_id"))
    if any(not isinstance(v, str) for v in (thread, parent, root)):
        raise SnapshotIncomplete("invalid message relationship")
    # Bind the fields actually supplied to tasks. Service-side resource display
    # metadata outside those fields must not create spurious same-time conflicts.
    fingerprint = key(msg_type, body, attachments, deleted, sorted(mentions), thread, parent, root)
    event = Event(
        tenant_id=source.tenant_id, native_id=message_id, chat_id=source.effective_chat_id,
        sender_id=sender_id, body=body, original_time=created / 1000,
        channel=source.channel, thread_id=thread, parent_id=parent, root_id=root,
        mentions=tuple(mentions), project_ids=source.project_ids,
        attachments=tuple(attachments), revision=f"{REVISION_PREFIX}{updated}:{fingerprint}",
        version_time=updated / 1000, observed_via="feishu_snapshot",
        is_self=sender_id == config.bot_id, is_deleted=deleted,
        direction="outbound" if sender_id == config.bot_id else "inbound", content_status=status,
    )
    event.validate()
    return event


def snapshot_host_payload(message, config, source):
    """Shape raw API metadata for the existing host's scope/legacy callbacks."""
    body = message.get("body") or {}
    return {"header": {"tenant_key": config.tenant_id}, "event": {
        "sender": {"sender_id": {"open_id": message.get("sender", {}).get("id", "")}},
        "message": {**message, "message_type": message.get("msg_type", ""),
                    "content": body.get("content", "{}"),
                    "chat_type": "p2p" if source.channel in {"bot_dm", "personal_dm"} else "group"},
    }}


class FeishuMessageSnapshotReader:
    """Read one exact known message through the existing authenticated transport.

    request(url, params, identity) must return raw REST JSON, with bounded timeout
    supplied by the existing host. Calling this is explicit and read-only.
    """
    url = "https://open.feishu.cn/open-apis/im/v1/messages"

    def __init__(self, request):
        self.request = request

    def read_message(self, source, message_id):
        source.validate()
        _required_text(message_id, "message_id")
        response = self.request(self.url + "/" + quote(message_id, safe=""),
                                {"user_id_type": "open_id"}, source.identity)
        if not isinstance(response, dict) or type(response.get("code")) is not int or response["code"] != 0:
            raise SnapshotIncomplete("message get did not return success")
        data = response.get("data")
        items = data.get("items") if isinstance(data, dict) else None
        if not isinstance(items, list):
            raise SnapshotIncomplete("message get has no raw items")
        matches = [item for item in items if isinstance(item, dict) and item.get("message_id") == message_id]
        if len(matches) != 1:
            raise SnapshotIncomplete("message get must identify exactly one requested message")
        return matches[0]
