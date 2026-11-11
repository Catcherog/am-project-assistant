"""Strict, side-effect-free normalization of Feishu *received* text/post bodies.

Receive post content is a JSON string containing ``title`` and two-dimensional
``content`` paragraphs. This command ingress intentionally accepts only text
and bound at nodes. It does not render Markdown, decode HTML, choose a locale,
or discard unsupported elements to expose a command hidden behind them.

Sources: https://open.feishu.cn/document/server-docs/im-v1/message-content-description/message_content
https://open.feishu.cn/document/server-docs/im-v1/message/events/receive
https://github.com/larksuite/node-sdk/blob/b188be60134d409b61259546bcf69c731d2b0b7c/channel/normalize/converters/post.ts
https://github.com/larksuite/node-sdk/blob/b188be60134d409b61259546bcf69c731d2b0b7c/channel/normalize/utils.ts
"""
from __future__ import annotations

import json
import re
from dataclasses import dataclass


MAX_CONTENT_BYTES = 65536
MAX_PARAGRAPHS = 256
MAX_NODES = 1024
MAX_MENTIONS = 64
_KEY = re.compile(r"@_user_[0-9]{1,6}|@_all")
_TOKEN = re.compile(r"(?<![A-Za-z0-9_@])(?:@_user_[0-9]{1,6}|@_all)(?![A-Za-z0-9_])")
_OPEN_ID = re.compile(r"ou_[A-Za-z0-9_]{1,128}")
_STYLES = {"bold", "italic", "underline", "lineThrough", "strikethrough", "codeInline", "code"}


class MessageTextError(ValueError):
    """A content-free error code; safe to record without logging message data."""

    def __init__(self, code: str):
        self.code = code
        super().__init__(code)


@dataclass(frozen=True)
class NormalizedMessage:
    text: str
    bot_mentioned: bool


def _object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise MessageTextError("INVALID_JSON")
        result[key] = value
    return result


def _no_constant(_):
    raise MessageTextError("INVALID_JSON")


def _string(value):
    if not isinstance(value, str):
        raise MessageTextError("INVALID_CONTENT_STRUCTURE")
    # Preserve Unicode and line breaks; never silently remove control bytes or
    # convert lookalike characters into ASCII commands.
    if any((ord(c) < 32 and c not in "\t\r\n") or 0xD800 <= ord(c) <= 0xDFFF for c in value):
        raise MessageTextError("INVALID_TEXT")
    return value


def _style(node):
    styles = node.get("style", [])
    if not isinstance(styles, list) or len(styles) > len(_STYLES):
        raise MessageTextError("UNSUPPORTED_TEXT_STYLE")
    if any(not isinstance(style, str) or style not in _STYLES for style in styles):
        raise MessageTextError("UNSUPPORTED_TEXT_STYLE")


def _mentions(message):
    raw = message.get("mentions", [])
    if not isinstance(raw, list) or len(raw) > MAX_MENTIONS:
        raise MessageTextError("INVALID_MENTIONS")
    result = {}
    for item in raw:
        if not isinstance(item, dict):
            raise MessageTextError("INVALID_MENTIONS")
        key, identity = item.get("key"), item.get("id", {})
        if (not isinstance(key, str) or not _KEY.fullmatch(key) or key in result
                or not isinstance(identity, dict)):
            raise MessageTextError("INVALID_MENTIONS")
        open_id = identity.get("open_id", "")
        if not isinstance(open_id, str) or (open_id and not _OPEN_ID.fullmatch(open_id)):
            raise MessageTextError("INVALID_MENTIONS")
        # @all never counts as a direct mention of this bot, regardless of ID.
        result[key] = "" if key == "@_all" else open_id
    return result


def _at(node, mentions, bot_open_id):
    if set(node) - {"tag", "user_id", "user_name", "style"}:
        raise MessageTextError("INVALID_CONTENT_STRUCTURE")
    _style(node)
    if "user_name" in node:
        _string(node["user_name"])
    target = _string(node.get("user_id"))
    if target in ("all", "all_members", "@_all"):
        return "@all", False
    if target in mentions:
        open_id, literal = mentions[target], target
    elif _OPEN_ID.fullmatch(target) and target in mentions.values():
        open_id, literal = target, "@" + target
    else:
        raise MessageTextError("UNBOUND_MENTION")
    is_bot = bool(bot_open_id and open_id == bot_open_id)
    # A separating space prevents /d + @bot + oc from becoming /doc.
    return (" " if is_bot else literal), is_bot


def normalize_message(message, *, bot_open_id: str = "", max_chars: int = 2000) -> NormalizedMessage:
    """Return text plus verified bot-addressing evidence, without changing input.

    Only received ``message.content`` is read. A nested ``content_v2`` mirror is
    accepted only when it exactly matches the validated ``content`` paragraphs;
    differing representations and send-style locale wrappers are rejected.
    A post mention must be an at node
    bound to event ``mentions`` by placeholder or open_id. Plain text needs an
    exact event placeholder token. Display names and bare IDs cannot authorize
    a group message. Only bound bot mentions become spaces; others stay tokens.

    Outer whitespace is preserved for the existing callers to handle. The
    length limit applies before bot-token removal for text, and to reconstructed
    text for post. The caller still owns sender/app/tenant/chat authorization,
    message IDs, original timestamps, deduplication, and absolute expiry.
    """
    if type(max_chars) is not int or max_chars <= 0:
        raise ValueError("max_chars must be a positive integer")
    if not isinstance(bot_open_id, str) or (bot_open_id and not _OPEN_ID.fullmatch(bot_open_id)):
        raise ValueError("bot_open_id must be empty or a configured open_id")
    if not isinstance(message, dict):
        raise MessageTextError("INVALID_CONTENT_STRUCTURE")
    kind, raw = message.get("message_type"), message.get("content")
    if kind not in ("text", "post"):
        raise MessageTextError("UNSUPPORTED_MESSAGE_TYPE")
    if not isinstance(raw, str):
        raise MessageTextError("INVALID_JSON")
    if len(raw) > MAX_CONTENT_BYTES:
        raise MessageTextError("INPUT_TOO_LONG")
    try:
        if len(raw.encode("utf-8")) > MAX_CONTENT_BYTES:
            raise MessageTextError("INPUT_TOO_LONG")
        content = json.loads(raw, object_pairs_hook=_object, parse_constant=_no_constant)
    except MessageTextError:
        raise
    except (ValueError, UnicodeError, RecursionError):
        raise MessageTextError("INVALID_JSON") from None
    if not isinstance(content, dict):
        raise MessageTextError("INVALID_CONTENT_STRUCTURE")
    mentions = _mentions(message)
    mentioned = False

    if kind == "text":
        if set(content) != {"text"}:
            raise MessageTextError("INVALID_CONTENT_STRUCTURE")
        text = _string(content["text"])
        if len(text) > max_chars:
            raise MessageTextError("INPUT_TOO_LONG")

        def replace(match):
            nonlocal mentioned
            if bot_open_id and mentions.get(match[0]) == bot_open_id:
                mentioned = True
                return " "
            return match[0]

        return NormalizedMessage(_TOKEN.sub(replace, text), mentioned)

    if set(content) - {"title", "content", "content_v2"} or not isinstance(content.get("content"), list):
        raise MessageTextError("UNSUPPORTED_POST_STRUCTURE")
    title = _string(content.get("title", ""))
    paragraphs = content["content"]
    if len(paragraphs) > MAX_PARAGRAPHS:
        raise MessageTextError("STRUCTURE_TOO_LARGE")
    lines, count = [], 0
    if title:
        lines.append(title)
    for paragraph in paragraphs:
        if not isinstance(paragraph, list):
            raise MessageTextError("INVALID_CONTENT_STRUCTURE")
        count += len(paragraph)
        if count > MAX_NODES:
            raise MessageTextError("STRUCTURE_TOO_LARGE")
        fragments = []
        for node in paragraph:
            if not isinstance(node, dict):
                raise MessageTextError("INVALID_CONTENT_STRUCTURE")
            if node.get("tag") == "text":
                if set(node) - {"tag", "text", "style", "un_escape"}:
                    raise MessageTextError("INVALID_CONTENT_STRUCTURE")
                _style(node)
                if "un_escape" in node and type(node["un_escape"]) is not bool:
                    raise MessageTextError("INVALID_CONTENT_STRUCTURE")
                # style (including code/codeInline) changes display only. Keep
                # text literally; do not HTML-unescape even if un_escape is set.
                fragments.append(_string(node.get("text")))
            elif node.get("tag") == "at":
                fragment, is_bot = _at(node, mentions, bot_open_id)
                fragments.append(fragment)
                mentioned = mentioned or is_bot
            else:
                # Reject the WHOLE post: never delete an image/link/unknown node
                # and join the surrounding text into an executable command.
                raise MessageTextError("UNSUPPORTED_POST_TAG")
        lines.append("".join(fragments))
    text = "\n".join(lines)
    if len(text) > max_chars:
        raise MessageTextError("INPUT_TOO_LONG")
    # Current Feishu clients include the documented v2 representation even for
    # plain text posts. Do not ignore a richer/different representation that
    # could contain unsupported nodes or expose a different command.
    if "content_v2" in content and content["content_v2"] != paragraphs:
        raise MessageTextError("POST_REPRESENTATION_MISMATCH")
    return NormalizedMessage(text, mentioned)
