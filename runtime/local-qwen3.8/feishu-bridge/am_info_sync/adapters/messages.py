"""Feishu message history and version normalization; no sending or long connection."""
from __future__ import annotations

import base64
import json
import re
from typing import Any, Protocol
from urllib.parse import urlsplit

from ..models import Discovery, InvalidCursor, Observation, Page, ScopeViolation, Source, canonical, digest
from .http import FeishuClient, InvalidResponse, page_items, remote_id


CHAT_KINDS = {"im_chat", "feishu_chat", "chat"}
THREAD_KINDS = {"im_thread", "feishu_thread", "thread"}
_URL_RE = re.compile(r"https://[^\s<>\"']+")


class AttachmentReader(Protocol):
    """Optional future integration. Returning a reference is not reading its body."""
    def read(self, source: Source, message_id: str,
             attachment: dict[str, Any]) -> bytes: ...


def _timestamp(value: Any, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, (int, str)):
        raise InvalidResponse(f"Missing or invalid {name}")
    try:
        number = int(value)
    except (TypeError, ValueError):
        raise InvalidResponse(f"Invalid {name}") from None
    if number < 0:
        raise InvalidResponse(f"Invalid {name}")
    return number


def document_discoveries(texts: list[str]) -> tuple[Discovery, ...]:
    """Recognize direct docx links only; never follow them or auto-register access."""
    found: dict[str, Discovery] = {}
    for text in texts:
        for url in _URL_RE.findall(text):
            parsed = urlsplit(url.rstrip("。，；、）)"))
            host = parsed.hostname or ""
            if parsed.scheme != "https" or parsed.username or parsed.password:
                continue
            if not (host.endswith(".feishu.cn") or host.endswith(".larksuite.com")):
                continue
            match = re.fullmatch(r"/docx/([A-Za-z0-9_-]{1,256})/?", parsed.path)
            if match:
                token = match.group(1)
                found[token] = Discovery("docx", token, "Direct document link observed; authorization required")
    return tuple(found.values())


def _content(item: dict[str, Any]) -> dict[str, Any]:
    body = item.get("body")
    raw = body.get("content") if isinstance(body, dict) else item.get("content")
    if raw is None and item.get("deleted") is True:
        return {}
    if isinstance(raw, dict):
        return raw
    if not isinstance(raw, str):
        raise InvalidResponse("Message body is missing")
    try:
        value = json.loads(raw)
    except (ValueError, TypeError):
        raise InvalidResponse("Message content is not valid JSON") from None
    if not isinstance(value, dict):
        raise InvalidResponse("Message content must be an object")
    return value


def _post(content: dict[str, Any]) -> tuple[str, list[dict[str, Any]], list[str], list[str]]:
    # Receive APIs commonly return an unwrapped post; locale wrappers are also
    # accepted for imported messages. Never concatenate several translations.
    if "content" not in content:
        preferred = content.get("zh_cn")
        if not isinstance(preferred, dict):
            preferred = next((v for v in content.values()
                              if isinstance(v, dict) and "content" in v), None)
        if preferred is None:
            raise InvalidResponse("Post message has no content rows")
        content = preferred
    rows = content.get("content")
    if not isinstance(rows, list):
        raise InvalidResponse("Post content rows are invalid")
    lines: list[str] = []
    title = content.get("title", "")
    if not isinstance(title, str):
        raise InvalidResponse("Post title is invalid")
    if title:
        lines.append(title)
    attachments: list[dict[str, Any]] = []
    issues: list[str] = []
    links: list[str] = []
    for row in rows:
        if not isinstance(row, list):
            raise InvalidResponse("Post row is invalid")
        pieces: list[str] = []
        for node in row:
            if not isinstance(node, dict):
                raise InvalidResponse("Post node is invalid")
            tag = node.get("tag")
            if tag in {"text", "a", "code_block", "md"}:
                value = node.get("text", "")
                if not isinstance(value, str):
                    raise InvalidResponse("Post text is invalid")
                pieces.append(value)
                if tag == "a" and isinstance(node.get("href"), str):
                    links.append(node["href"])
            elif tag == "at":
                pieces.append("@" + str(node.get("user_name") or node.get("user_id") or "unknown"))
            elif tag in {"img", "media", "file", "audio"}:
                attachments.append({"kind": tag, "image_key": node.get("image_key"),
                                    "file_key": node.get("file_key"), "status": "pending_content"})
                issues.append("attachment_content_pending")
            elif tag == "hr":
                pieces.append("\n")
            else:
                # Do not silently lose an unsupported content-bearing tag.
                issues.append("unsupported_post_tag:" + str(tag))
        lines.append("".join(pieces))
    return "\n".join(lines), attachments, issues, links


def _card_text(value: Any) -> list[str]:
    result: list[str] = []
    if isinstance(value, list):
        for child in value:
            result.extend(_card_text(child))
    elif isinstance(value, dict):
        if value.get("tag") in {"plain_text", "lark_md", "markdown"}:
            text = value.get("content", value.get("text"))
            if isinstance(text, str):
                result.append(text)
        for key, child in value.items():
            if isinstance(child, (list, dict)):
                result.extend(_card_text(child))
    return result


def _identity(value: Any, declared_type: Any = None) -> dict[str, str] | None:
    if isinstance(value, dict):
        for name in ("open_id", "user_id", "union_id"):
            if isinstance(value.get(name), str) and value[name]:
                return {"id": value[name], "id_type": name}
    if isinstance(value, str) and value:
        return {"id": value, "id_type": str(declared_type or "unknown")}
    return None


def _mention_entities(item: dict[str, Any]) -> list[dict[str, Any]]:
    mentions = item.get("mentions", [])
    if mentions is None:
        mentions = []
    if not isinstance(mentions, list):
        raise InvalidResponse("Message mentions must be a list")
    result = []
    for mention in mentions:
        if not isinstance(mention, dict):
            raise InvalidResponse("Message mention is invalid")
        result.append({"key": mention.get("key"),
                       "identity": _identity(mention.get("id"), mention.get("id_type"))})
    return sorted(result, key=canonical)


def sender_in_scope(source: Source, item: dict[str, Any]) -> bool:
    allowed = source.settings.get('allowed_sender_ids')
    if source.settings.get('only_user_messages') is True and item.get('sender', {}).get('sender_type') != 'user':
        return False
    if allowed is None:
        return True
    sender = item.get('sender', {})
    if not isinstance(sender, dict) or sender.get('sender_type') != 'user':
        return False
    author = _identity(sender.get('sender_id', sender.get('id')), sender.get('id_type'))
    return bool(author and author['id_type'] == 'open_id' and author['id'] in allowed)


def normalize_message(source: Source, item: dict[str, Any]) -> tuple[Observation, tuple[Discovery, ...]]:
    if not source.enabled or source.kind not in CHAT_KINDS | THREAD_KINDS:
        raise ScopeViolation("Message source is disabled or has an unsupported kind")
    if not sender_in_scope(source, item):
        raise ScopeViolation('Message sender is outside the registered source scope')
    message_id = remote_id(item.get("message_id"))
    create_ms = _timestamp(item.get("create_time"), "create_time")
    update_ms = _timestamp(item.get("update_time", create_ms), "update_time")
    if create_ms < source.start_ms:
        raise ScopeViolation("Message predates authorized source start")
    if update_ms < create_ms:
        raise InvalidResponse("Message update_time precedes create_time")
    if source.kind in CHAT_KINDS and item.get("chat_id") != source.remote_id:
        raise ScopeViolation("Message does not belong to registered chat")
    if source.kind in THREAD_KINDS:
        if item.get("thread_id") not in (None, "", source.remote_id):
            raise ScopeViolation("Message does not belong to registered thread")
        expected_chat = source.settings.get("chat_id")
        if expected_chat is not None and item.get("chat_id") != expected_chat:
            raise ScopeViolation("Thread message chat does not match registered parent")
    deleted = item.get("deleted", False)
    if type(deleted) is not bool:
        raise InvalidResponse("Message deleted flag is invalid")
    msg_type = item.get("msg_type", item.get("message_type"))
    body = _content(item)
    issues: list[str] = []
    attachments: list[dict[str, Any]] = []
    links: list[str] = []
    text = ""
    if deleted:
        # Keep a tombstone; never let the retained old text become current evidence.
        body = {}
    elif msg_type == "text":
        text = body.get("text")
        if not isinstance(text, str):
            raise InvalidResponse("Text message has no text")
    elif msg_type == "post":
        text, attachments, issues, links = _post(body)
    elif msg_type in {"image", "file", "audio", "media", "video", "sticker"}:
        text = body.get("file_name", "")
        if not isinstance(text, str):
            raise InvalidResponse("File name is invalid")
        attachments.append({"kind": msg_type, "file_key": body.get("file_key"),
                            "image_key": body.get("image_key"), "name": text,
                            "status": "pending_content"})
        issues.append("attachment_content_pending")
    elif msg_type == "interactive":
        text = "\n".join(_card_text(body))
        issues.append("interactive_card_partial")
    else:
        issues.append("unsupported_message_type:" + str(msg_type))
    # Event and polling transports have different wrappers/optional metadata.
    # Persist a stable semantic envelope so they agree on the same version.
    # The ingress inbox, not the current-object body, retains the original event.
    mention_entities = [] if deleted else _mention_entities(item)
    normalized_remote = {
        "message_id": message_id, "chat_id": item.get("chat_id"),
        "thread_id": item.get("thread_id") or None,
        "parent_id": item.get("parent_id") or None,
        "root_id": item.get("root_id") or None,
        "create_time": str(create_ms), "update_time": str(update_ms),
        "msg_type": msg_type, "body": body, "deleted": deleted,
        "mention_entities": mention_entities,
    }
    sender = item.get("sender", {})
    if not isinstance(sender, dict):
        raise InvalidResponse("Message sender metadata is invalid")
    author = _identity(sender.get("sender_id", sender.get("id")), sender.get("id_type"))
    if author is not None:
        author["sender_type"] = str(sender.get("sender_type") or "unknown")
    transport_metadata = {"sender": sender, "author": author,
                          "retrieved_via": "event" if "message_type" in item else "history"}
    payload = {"remote": normalized_remote, "message_type": msg_type,
               "created_at_ms": create_ms, "updated_at_ms": update_ms,
               "attachments": attachments, "content_issues": sorted(set(issues)),
               "transport_metadata": transport_metadata, "trusted_instruction": False}
    if deleted:
        payload["remote"].pop("body")
    # Hash includes content and tombstone state, so same-ID edits never become
    # duplicates. Zero-padded server time remains readable for reconciliation.
    version_body = {"body": body, "deleted": deleted, "type": msg_type,
                    "mention_entities": mention_entities}
    version = f"{update_ms:020d}:{digest(version_body)}"
    completeness = "complete" if not issues else ("partial" if text else "unreadable")
    observation = Observation(source.source_id, message_id, version, update_ms,
                              text=text, payload=payload, deleted=deleted,
                              completeness=completeness,
                              parent_id=item.get("parent_id") or item.get("root_id") or None)
    discoveries = list(document_discoveries([text, *links])) if not deleted else []
    thread_id = item.get("thread_id")
    if source.kind in CHAT_KINDS and thread_id and not deleted:
        discoveries.append(Discovery("im_thread", remote_id(thread_id),
                                     "Thread root observed; separate authorization/paging required",
                                     {"chat_id": source.remote_id}))
    return observation, tuple(discoveries)


class MessageAdapter:
    def __init__(self, client: FeishuClient, *, page_size: int = 50):
        if not 1 <= page_size <= 50:
            raise ValueError("Message page_size must be 1..50")
        self.client = client
        self.page_size = page_size

    @staticmethod
    def _context(source: Source, start_ms: int, end_ms: int) -> dict[str, Any]:
        return {"v": 1, "source": source.source_id, "kind": source.kind,
                "remote_id": source.remote_id, "start_ms": start_ms, "end_ms": end_ms}

    def fetch_page(self, source: Source, start_ms: int, end_ms: int,
                   cursor: str | None = None) -> Page:
        if not source.enabled:
            raise ScopeViolation("Source is disabled")
        is_chat = source.kind in CHAT_KINDS
        if not is_chat and source.kind not in THREAD_KINDS:
            raise ScopeViolation("Unsupported message source kind")
        if not is_chat and source.settings.get("allow_full_thread_read") is not True:
            raise ScopeViolation("Thread source requires explicit allow_full_thread_read authorization")
        if start_ms < source.start_ms or end_ms < start_ms:
            raise ScopeViolation("Scan window is outside authorized bounds")
        if is_chat and source.start_ms % 1000:
            raise ScopeViolation("Message source.start_ms must use whole seconds")
        if is_chat and end_ms == start_ms:
            return Page((), time_coverage=True)
        context = self._context(source, start_ms, end_ms)
        token = None
        if cursor is not None:
            try:
                envelope = json.loads(base64.urlsafe_b64decode(cursor.encode("ascii")))
            except (TypeError, ValueError, UnicodeError):
                raise InvalidCursor("Invalid adapter cursor") from None
            if not isinstance(envelope, dict) or envelope.get("context") != context:
                raise InvalidCursor("Cursor belongs to another source or time window")
            token = envelope.get("token")
            if not isinstance(token, str) or not token:
                raise InvalidCursor("Cursor has no remote page token")
        params: dict[str, str | int] = {
            "container_id_type": "chat" if is_chat else "thread",
            "container_id": remote_id(source.remote_id),
            "sort_type": "ByCreateTimeAsc", "page_size": self.page_size,
        }
        if is_chat:
            params["start_time"] = max(source.start_ms // 1000, start_ms // 1000)
            params["end_time"] = (end_ms + 999) // 1000
        if token is not None:
            params["page_token"] = token
        data = self.client.get(source, "/open-apis/im/v1/messages", params)
        items, next_token = page_items(data, token)
        observations: list[Observation] = []
        discoveries: list[Discovery] = []
        for item in items:
            if not sender_in_scope(source, item):
                continue
            created = _timestamp(item.get("create_time"), "create_time")
            # Rounded API boundaries can overlap; keep the exact requested range.
            if is_chat and not start_ms <= created < end_ms:
                continue
            if not is_chat and created < source.start_ms:
                # A thread's API cannot filter time. Do not store unauthorized
                # earlier content; the whole-thread authorization is explicit.
                continue
            observation, discovered = normalize_message(source, item)
            observations.append(observation)
            discoveries.extend(discovered)
        next_cursor = None
        if next_token:
            next_cursor = base64.urlsafe_b64encode(canonical({"context": context,
                                                             "token": next_token}).encode()).decode()
        unique = {(d.kind, d.remote_id): d for d in discoveries}
        return Page(tuple(observations), next_cursor, tuple(unique.values()), time_coverage=is_chat)

    def fetch_message(self, source: Source, message_id: str) -> Page:
        """Re-read an already registered message for edits/retraction state.

        The caller must choose known object IDs from this source, not model text.
        A not-found response raises; it is never inferred to be a deletion.
        """
        if source.kind not in CHAT_KINDS | THREAD_KINDS:
            raise ScopeViolation("Unsupported message source kind")
        if source.kind in THREAD_KINDS and source.settings.get("allow_full_thread_read") is not True:
            raise ScopeViolation("Thread source requires explicit allow_full_thread_read authorization")
        data = self.client.get(source, f"/open-apis/im/v1/messages/{remote_id(message_id)}")
        items = data.get("items")
        if not isinstance(items, list) or len(items) != 1 or not isinstance(items[0], dict):
            raise InvalidResponse("Message re-read returned no single message")
        if items[0].get("message_id") != message_id:
            raise ScopeViolation("Re-read returned a different message")
        item, discovered = normalize_message(source, items[0])
        return Page((item,), discoveries=discovered, time_coverage=False)
