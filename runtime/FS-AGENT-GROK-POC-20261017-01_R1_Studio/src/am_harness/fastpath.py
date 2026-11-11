"""Conservative, deterministic reads through the existing AM registry.

This is deliberately a small grammar, not intent detection.  A miss returns
``None`` to the normal ConversationAgent.  Call only after its claim_event;
the original engine owns event idempotency, history and the final reply.
"""
from __future__ import annotations

from dataclasses import dataclass
from contextlib import contextmanager
import hashlib
import math
from pathlib import Path
import re
import sqlite3
import time
from typing import Callable


@dataclass(frozen=True)
class FocusSnapshot:
    scope_key: str
    project_id: str
    expires_at: float


@dataclass(frozen=True)
class PreparedRead:
    """Short-lived host-produced snapshot, never an authorization token."""

    scope_key: str
    request_hash: str
    project_id: str
    text: str
    status: str
    source_ids: tuple[str, ...]
    expires_at: float


class FocusCache:
    """A bounded-lived project reference; it never grants project access."""

    def __init__(self, path: str | Path, *, clock: Callable[[], float] = time.time,
                 ttl: float = 600):
        if not math.isfinite(ttl) or not 0 < ttl <= 600:
            raise ValueError("INVALID_FOCUS_TTL")
        self.path, self.clock, self.ttl = str(path), clock, ttl
        Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as db:
            db.execute("PRAGMA journal_mode=WAL")
            db.execute("""CREATE TABLE IF NOT EXISTS harness_focus (
                scope_key TEXT PRIMARY KEY, project_id TEXT NOT NULL,
                expires_at REAL NOT NULL)""")

    @contextmanager
    def _connect(self):
        db = sqlite3.connect(self.path, timeout=10)
        try:
            with db:
                yield db
        finally:
            db.close()

    def get(self, ctx) -> FocusSnapshot | None:
        now = self.clock()
        with self._connect() as db:
            db.execute("DELETE FROM harness_focus WHERE expires_at <= ?", (now,))
            row = db.execute("SELECT project_id,expires_at FROM harness_focus WHERE scope_key=?",
                             (ctx.session_id,)).fetchone()
        if row is None or not ctx.can_access_project(row[0]):
            return None
        if ctx.content_expires_at is not None and ctx.content_expires_at <= now:
            return None
        return FocusSnapshot(ctx.session_id, row[0], row[1])

    def remember(self, ctx, project_id: str) -> None:
        if not ctx.can_access_project(project_id):
            raise ValueError("PROJECT_ACCESS_DENIED")
        now = self.clock()
        expires = min(now + self.ttl, ctx.content_expires_at or now + self.ttl)
        if expires <= now:
            return
        with self._connect() as db:
            db.execute("DELETE FROM harness_focus WHERE expires_at <= ?", (now,))
            db.execute("""INSERT INTO harness_focus VALUES(?,?,?)
                ON CONFLICT(scope_key) DO UPDATE SET project_id=excluded.project_id,
                expires_at=excluded.expires_at""", (ctx.session_id, project_id, expires))


_PREFIX = r"(?:请)?(?:帮我)?(?:查询|查一下|查查|看看|看一下|查|看)?\s*"
_FIELDS = {
    "项目进展": ("项目进展", "当前进展", "目前进展", "进展怎么样", "进展如何", "进展", "进度"),
    "关键日期": ("关键节点日期", "关键日期", "计划日期"),
    "待办": ("还有哪些待办", "有哪些待办", "待办事项", "待办"),
    "人员与职责": ("人员与职责", "负责人员", "人员安排", "谁负责", "负责人"),
}
_TAIL = r"(?:呢|吗)?[？?。！!]*"
_PRONOUN = r"(?:那|它|这个项目|该项目|刚才那个项目)(?:的)?"


def _inert(text: str) -> str:
    # Same inert mention rendering as the original LegacyConversationBridge.
    return re.sub(r"<(?=/?at\b)", "＜", text, flags=re.IGNORECASE)


class FastPathRouter:
    """Answer only an unambiguous, authorized ledger field query.

    ``projects`` must be the existing ProjectPort, ``registry`` the original
    ToolRegistry.  No model, outbox or new source connector is created here.
    ``authorize`` is the original host authorization callback.  Optional focus
    cache stores only references for short followups, never source facts.
    """

    def __init__(self, projects, registry, *, authorize, focus: FocusCache | None = None,
                 clock: Callable[[], float] = time.time, max_reply_chars: int = 6000):
        if not callable(authorize) or not 300 <= max_reply_chars <= 8000:
            raise ValueError("INVALID_FASTPATH_CONFIG")
        self.projects, self.registry, self.authorize = projects, registry, authorize
        self.focus, self.clock, self.max_reply_chars = focus, clock, max_reply_chars

    def __call__(self, ctx, event_id: str, text: str):
        return self.try_handle(ctx, event_id, text)

    def _resolve(self, ctx, text: str):
        # A newline or an unrecognised suffix is a miss.  Never strip a second
        # instruction to make a query fit the fast path.
        if not isinstance(text, str) or not text.strip() or len(text) > 500 or "\n" in text or "\r" in text:
            return None
        query = text.strip()
        candidates = []
        for field, phrases in _FIELDS.items():
            field_pattern = "(?:" + "|".join(re.escape(p) for p in phrases) + ")"
            if not re.search(field_pattern + _TAIL + r"$", query):
                continue
            if self.focus and re.fullmatch(_PREFIX + _PRONOUN + field_pattern + _TAIL, query):
                focus = self.focus.get(ctx)
                if focus and focus.scope_key == ctx.session_id and focus.expires_at > self.clock():
                    candidates.append((focus.project_id, field))
                continue
            self.authorize(ctx, "read", {})
            rows = self.projects.list_projects(ctx)
            # Match complete literal aliases.  The same alias on two visible
            # projects is ambiguous even if one row happens to be first.
            for row in rows:
                ident = row.get("id")
                if not isinstance(ident, str) or not ctx.can_access_project(ident):
                    continue
                aliases = [row.get("title", ""), *row.get("aliases", [])]
                for alias in aliases:
                    if not isinstance(alias, str) or not alias.strip() or len(alias) > 200:
                        continue
                    pattern = _PREFIX + re.escape(alias.strip()) + r"(?:项目)?(?:的)?" + field_pattern + _TAIL
                    if re.fullmatch(pattern, query, flags=re.IGNORECASE):
                        candidates.append((ident, field))
                        break
        distinct = set(candidates)
        return next(iter(distinct)) if len(distinct) == 1 else None

    def try_handle(self, ctx, event_id: str, text: str):
        from am_conversation.types import AgentReply, ConversationError

        self.authorize(ctx, "read", {})
        if ctx.content_expires_at is not None and ctx.content_expires_at <= self.clock():
            raise ConversationError("CONTENT_EXPIRED")
        selected = self._resolve(ctx, text)
        if selected is None:
            return None
        project_id, field = selected
        args = {"project_id": project_id}
        self.authorize(ctx, "read", args)
        tool = self.registry.resolve(ctx, "read_project", args)
        if tool.effect != "read":
            raise ConversationError("FASTPATH_TOOL_EFFECT_CHANGED")
        row = tool.handler(ctx, args, event_id)
        self.authorize(ctx, "read", args)
        if ctx.content_expires_at is not None and ctx.content_expires_at <= self.clock():
            raise ConversationError("CONTENT_EXPIRED")
        rendered = self._render(row, project_id, field)
        if rendered is None:
            return None
        body, status, source_ids = rendered
        if self.focus:
            self.focus.remember(ctx, project_id)
        return AgentReply(event_id, body, status, source_ids=source_ids)

    def prepare(self, ctx, text: str) -> PreparedRead | None:
        """Read-only preflight before claiming a conversation event.

        Uses the original ProjectPort directly, deliberately avoiding the
        ``read_project`` tool wrapper's legacy focus mutation.  A miss has not
        claimed an AM event and can safely enter the normal model queue.
        """
        from am_conversation.types import ConversationError

        self.authorize(ctx, "read", {})
        now = self.clock()
        if ctx.content_expires_at is not None and ctx.content_expires_at <= now:
            raise ConversationError("CONTENT_EXPIRED")
        selected = self._resolve(ctx, text)
        if selected is None:
            return None
        project_id, field = selected
        args = {"project_id": project_id}
        self.authorize(ctx, "read", args)
        if self.registry.resolve(ctx, "read_project", args).effect != "read":
            raise ConversationError("FASTPATH_TOOL_EFFECT_CHANGED")
        row = self.projects.read_project(ctx, project_id)
        self.authorize(ctx, "read", args)
        rendered = self._render(row, project_id, field)
        if rendered is None:
            return None
        body, status, source_ids = rendered
        # Preflight can itself block on source refresh. Start its five-second
        # consumption window after the successful read, never past content TTL.
        expires = min(self.clock() + 5, ctx.content_expires_at or self.clock() + 5)
        if expires <= self.clock():
            raise ConversationError("CONTENT_EXPIRED")
        return PreparedRead(ctx.session_id, hashlib.sha256(text.strip().encode()).hexdigest(),
                            project_id, body, status, source_ids, expires)

    def consume(self, ctx, event_id: str, text: str, prepared: PreparedRead):
        from am_conversation.types import AgentReply, ConversationError

        if (not isinstance(prepared, PreparedRead) or prepared.scope_key != ctx.session_id
                or prepared.request_hash != hashlib.sha256(text.strip().encode()).hexdigest()):
            raise ConversationError("FASTPATH_PREPARATION_MISMATCH")
        if prepared.expires_at <= self.clock() or (
                ctx.content_expires_at is not None and ctx.content_expires_at <= self.clock()):
            raise ConversationError("CONTENT_EXPIRED")
        self.authorize(ctx, "read", {"project_id": prepared.project_id})
        if not ctx.can_access_project(prepared.project_id):
            raise ConversationError("PROJECT_ACCESS_DENIED")
        if self.focus:
            self.focus.remember(ctx, prepared.project_id)
        return AgentReply(event_id, prepared.text, prepared.status, source_ids=prepared.source_ids)

    def _render(self, row, project_id: str, field: str):
        from am_conversation.types import ConversationError

        # Require the real legacy projection contract, not a guessed field.
        if not isinstance(row, dict) or row.get("id") != project_id:
            raise ConversationError("PROJECT_ID_MISMATCH")
        if row.get("identity_status") != "verified" or not isinstance(row.get("fields"), dict):
            return None
        value = row["fields"].get(field)
        if value is not None and not isinstance(value, str):
            return None  # Structured fields need the regular reasoning path.
        title = row.get("title")
        if not isinstance(title, str) or not title.strip():
            return None
        if value is None or not value.strip():
            body = f"《{title}》本次读取的台账未提供“{field}”内容，暂不能据此判断实际情况。"
            status = "fastpath_missing"
        else:
            body = f"《{title}》台账中的{field}：\n{value}"
            status = "fastpath_answered"
        observed = row.get("observed_at")
        snapshot = row.get("snapshot") if isinstance(row.get("snapshot"), dict) else {}
        provenance = "资料观察时间：" + (observed if isinstance(observed, str) and observed else "未提供")
        version = snapshot.get("version")
        source_ids = ["ledger-project:" + project_id]
        if isinstance(version, str) and version:
            provenance += "；快照版本：" + version
            source_ids.append("ledger-snapshot:" + version)
        if snapshot.get("stale") is True:
            provenance += "；该快照已标记过期"
        if snapshot.get("complete") is False:
            provenance += "；该快照覆盖不完整"
        body += "\n\n" + provenance + "。此处引用台账记录，未独立核验。"
        body = _inert(body)
        if len(body) > self.max_reply_chars:
            return None  # No silent truncation, no claim the partial field is complete.
        return body, status, tuple(source_ids)
