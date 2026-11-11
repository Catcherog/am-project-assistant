"""Durable conversation state; no network calls or implicit execution retries.

AccessContext is supplied by trusted ingress. Every read is scoped, and project
access is checked again against the current context. Database transactions never
span model or business-tool calls.
"""
from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime
import hashlib
import hmac
import json
import math
from pathlib import Path
import re
import sqlite3
import time
from typing import Any, Iterator
import uuid

from .types import AccessContext, ConversationError, canonical


TASK_STATUSES = frozenset({"open", "waiting", "closed", "cancelled"})
HISTORY_RETENTION = 200


def _error(code: str) -> None:
    raise ConversationError(code)


def _text(value: Any, code: str, maximum: int, *, empty: bool = False) -> str:
    if not isinstance(value, str) or len(value) > maximum or (not empty and not value.strip()):
        _error(code)
    return value


def _json(value: Any) -> str:
    try:
        return canonical(value)
    except (ValueError, TypeError, RecursionError):
        _error("INVALID_JSON_VALUE")


def _version(value: Any) -> int:
    if type(value) is not int or value < 1:
        _error("INVALID_VERSION")
    return value


def _due(value: Any) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or len(value) > 64:
        _error("INVALID_DUE_AT")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        _error("INVALID_DUE_AT")
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        _error("DUE_AT_REQUIRES_TIMEZONE")
    return parsed.isoformat()


def _access_fingerprint(ctx: AccessContext) -> str:
    value = {"projects": sorted(ctx.allowed_project_ids), "all": ctx.all_projects,
             "owner": ctx.is_owner}
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def _derived_fingerprint(ctx: AccessContext) -> str:
    base = _access_fingerprint(ctx)
    if ctx.source_access_version is None:
        return base
    return hashlib.sha256(canonical([base,ctx.source_access_version]).encode()).hexdigest()


def _project(ctx: AccessContext, project_id: Any, *, optional: bool = False) -> str | None:
    if project_id is None and optional:
        return None
    _text(project_id, "INVALID_PROJECT_ID", 256)
    if not ctx.can_access_project(project_id):
        _error("PROJECT_FORBIDDEN")
    return project_id


def _limit(value: Any, maximum: int) -> int:
    if type(value) is not int or value < 1 or value > maximum:
        _error("INVALID_LIMIT")
    return value


class ConversationStore:
    """SQLite implementation of the conversation/task/action contract.

    File-backed stores use one short-lived connection per operation. ``:memory:``
    uses a unique shared-memory URI and an anchor connection, preserving the same
    transaction semantics. ``recover_inflight`` is an explicit maintenance action:
    call it only after taking ownership of the old worker's abandoned work.
    """

    def __init__(self, db_path: str | Path, *, clock=time.time):
        self.clock = clock
        self._closed = False
        self._keeper = None
        if str(db_path) == ":memory:":
            self._path = f"file:am_conversation_{uuid.uuid4().hex}?mode=memory&cache=shared"
            self._uri = True
            self._keeper = sqlite3.connect(self._path, uri=True, isolation_level=None,
                                           check_same_thread=False)
        else:
            path = Path(db_path).expanduser()
            path.parent.mkdir(parents=True, exist_ok=True)
            self._path = str(path)
            self._uri = False
        with self._connection() as db:
            db.executescript("""
                CREATE TABLE IF NOT EXISTS conversation_events (
                    tenant_id TEXT NOT NULL, app_id TEXT NOT NULL,
                    event_id TEXT NOT NULL, scope_key TEXT NOT NULL,
                    actor_id TEXT NOT NULL, chat_id TEXT NOT NULL, thread_id TEXT NOT NULL,
                    access_fingerprint TEXT NOT NULL, input_digest TEXT NOT NULL,
                    state TEXT NOT NULL, reply_json TEXT, error_code TEXT,
                    created_at REAL NOT NULL, updated_at REAL NOT NULL,
                    PRIMARY KEY (tenant_id, app_id, event_id)
                );
                CREATE TABLE IF NOT EXISTS conversation_messages (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    tenant_id TEXT NOT NULL, app_id TEXT NOT NULL,
                    scope_key TEXT NOT NULL, access_fingerprint TEXT NOT NULL,
                    event_id TEXT, role TEXT NOT NULL, content_json TEXT NOT NULL,
                    created_at REAL NOT NULL
                );
                CREATE INDEX IF NOT EXISTS conversation_messages_scope
                    ON conversation_messages(scope_key, id);
                CREATE TABLE IF NOT EXISTS conversation_focus (
                    scope_key TEXT PRIMARY KEY, tenant_id TEXT NOT NULL, app_id TEXT NOT NULL,
                    actor_id TEXT NOT NULL, chat_id TEXT NOT NULL, thread_id TEXT NOT NULL,
                    project_id TEXT NOT NULL, event_id TEXT, updated_at REAL NOT NULL
                );
                CREATE UNIQUE INDEX IF NOT EXISTS conversation_messages_event_role
                    ON conversation_messages(tenant_id, app_id, event_id, role)
                    WHERE event_id IS NOT NULL AND role IN ('user', 'assistant');
                CREATE TABLE IF NOT EXISTS conversation_tasks (
                    id TEXT PRIMARY KEY, tenant_id TEXT NOT NULL, app_id TEXT NOT NULL,
                    scope_key TEXT NOT NULL, actor_id TEXT NOT NULL, chat_id TEXT NOT NULL,
                    thread_id TEXT NOT NULL, project_id TEXT NOT NULL,
                    title TEXT NOT NULL, details TEXT NOT NULL, due_at TEXT,
                    status TEXT NOT NULL, version INTEGER NOT NULL,
                    event_id TEXT, dedupe_key TEXT UNIQUE,
                    created_at REAL NOT NULL, updated_at REAL NOT NULL
                );
                CREATE INDEX IF NOT EXISTS conversation_tasks_scope
                    ON conversation_tasks(scope_key, status, updated_at);
                CREATE TABLE IF NOT EXISTS conversation_task_versions (
                    task_id TEXT NOT NULL, version INTEGER NOT NULL,
                    record_json TEXT NOT NULL, created_at REAL NOT NULL,
                    PRIMARY KEY(task_id, version)
                );
                CREATE TABLE IF NOT EXISTS conversation_actions (
                    id TEXT PRIMARY KEY, tenant_id TEXT NOT NULL, app_id TEXT NOT NULL,
                    scope_key TEXT NOT NULL, actor_id TEXT NOT NULL, chat_id TEXT NOT NULL,
                    thread_id TEXT NOT NULL, project_id TEXT,
                    tool_name TEXT NOT NULL, arguments_json TEXT NOT NULL,
                    preview TEXT NOT NULL, version INTEGER NOT NULL, digest TEXT NOT NULL,
                    status TEXT NOT NULL, presented_version INTEGER,
                    presented_event_id TEXT, approved_at REAL, execution_id TEXT,
                    started_at REAL, expires_at REAL NOT NULL, result_json TEXT,
                    created_at REAL NOT NULL, updated_at REAL NOT NULL
                );
                CREATE INDEX IF NOT EXISTS conversation_actions_scope
                    ON conversation_actions(scope_key, status, updated_at);
            """)
            for table in ('conversation_events','conversation_messages'):
                columns={r[1] for r in db.execute('PRAGMA table_info('+table+')')}
                if 'content_expires' not in columns:db.execute('ALTER TABLE '+table+' ADD COLUMN content_expires REAL')
                db.execute('UPDATE '+table+' SET content_expires=created_at+86400 WHERE content_expires IS NULL')
            db.execute('CREATE INDEX IF NOT EXISTS conversation_message_expiry ON conversation_messages(content_expires)')
            for table in ('conversation_actions','conversation_tasks'):
                columns={r[1] for r in db.execute('PRAGMA table_info('+table+')')}
                if 'source_access_version' not in columns:
                    db.execute('ALTER TABLE '+table+' ADD COLUMN source_access_version TEXT')
            db.execute('CREATE TABLE IF NOT EXISTS conversation_goals_v2 (goal_id TEXT PRIMARY KEY, scope_key TEXT NOT NULL,data_json TEXT NOT NULL,state TEXT NOT NULL,expires REAL NOT NULL,updated REAL NOT NULL)')
            db.execute('''CREATE TABLE IF NOT EXISTS conversation_presentations_v2 (
                presentation_id TEXT PRIMARY KEY, scope_key TEXT NOT NULL,
                tenant_id TEXT NOT NULL, app_id TEXT NOT NULL, event_id TEXT NOT NULL,
                schema_version TEXT NOT NULL, semantic_hash TEXT NOT NULL,
                data_json TEXT NOT NULL, expires REAL NOT NULL,
                UNIQUE(tenant_id,app_id,event_id))''')
            db.execute('''CREATE TABLE IF NOT EXISTS conversation_response_interrupts_v2 (
                tenant_id TEXT NOT NULL, app_id TEXT NOT NULL, event_id TEXT NOT NULL,
                scope_key TEXT NOT NULL, data_json TEXT NOT NULL, expires REAL NOT NULL,
                PRIMARY KEY(tenant_id,app_id,event_id))''')

    @contextmanager
    def _connection(self, *, write: bool = False) -> Iterator[sqlite3.Connection]:
        if self._closed:
            _error("STORE_CLOSED")
        db = sqlite3.connect(self._path, timeout=10, uri=self._uri, isolation_level=None)
        db.row_factory = sqlite3.Row
        try:
            db.execute("PRAGMA busy_timeout=10000")
            db.execute("PRAGMA foreign_keys=ON")
            if not self._uri:
                db.execute("PRAGMA journal_mode=WAL")
            if write:
                db.execute("BEGIN IMMEDIATE")
            from contextlib import nullcontext
            guard = getattr(self, "local_write_guard", None)
            with guard(db) if write and guard is not None else nullcontext():
                yield db
                if write:
                    db.commit()
        except BaseException:
            if db.in_transaction:
                db.rollback()
            raise
        finally:
            db.close()

    def close(self) -> None:
        if self._keeper is not None:
            self._keeper.close()
            self._keeper = None
        self._closed = True

    def _now(self) -> float:
        value = float(self.clock())
        if not math.isfinite(value):
            _error("INVALID_CLOCK")
        return value

    def _event(self, db: sqlite3.Connection, ctx: AccessContext, event_id: str,
               *, access: bool = True) -> sqlite3.Row:
        _text(event_id, "INVALID_EVENT_ID", 512)
        row = db.execute("SELECT * FROM conversation_events WHERE tenant_id=? AND app_id=? AND event_id=?",
                         (ctx.tenant_id, ctx.app_id, event_id)).fetchone()
        if row is None:
            _error("EVENT_NOT_FOUND")
        if row["scope_key"] != ctx.session_id:
            _error("EVENT_SCOPE_MISMATCH")
        if access and row["access_fingerprint"] != _derived_fingerprint(ctx):
            _error("EVENT_AUTHORIZATION_CHANGED")
        if row['content_expires'] is not None and row['content_expires']<=self._now():_error('EVENT_CONTENT_EXPIRED')
        return row

    def _append(self, db: sqlite3.Connection, ctx: AccessContext, role: str,
                content: Any, event_id: str | None = None) -> None:
        if not isinstance(role, str) or role not in {"user", "assistant", "tool"}:
            _error("INVALID_MESSAGE_ROLE")
        content_json = _json(content)
        if len(content_json) > 200_000:
            _error("MESSAGE_TOO_LARGE")
        expires=min(self._now()+86400,ctx.content_expires_at or self._now()+86400)
        if event_id is not None:
            event=self._event(db, ctx, event_id)
            expires=min(expires,event['content_expires'] or event['created_at']+86400)
            if role in {"user", "assistant"}:
                old = db.execute("SELECT content_json FROM conversation_messages WHERE tenant_id=? AND app_id=? AND event_id=? AND role=?",
                                 (ctx.tenant_id, ctx.app_id, event_id, role)).fetchone()
                if old is not None:
                    if old["content_json"] != content_json:
                        _error("MESSAGE_CONTENT_MISMATCH")
                    return
        db.execute("INSERT INTO conversation_messages(tenant_id,app_id,scope_key,access_fingerprint,event_id,role,content_json,created_at,content_expires) VALUES(?,?,?,?,?,?,?,?,?)",
                   (ctx.tenant_id, ctx.app_id, ctx.session_id, _access_fingerprint(ctx) if role=='user' else _derived_fingerprint(ctx),
                    event_id, role, content_json, self._now(),expires))
        db.execute("DELETE FROM conversation_messages WHERE scope_key=? AND id NOT IN (SELECT id FROM conversation_messages WHERE scope_key=? ORDER BY id DESC LIMIT ?)",
                   (ctx.session_id, ctx.session_id, HISTORY_RETENTION))

    def claim_event(self, ctx: AccessContext, event_id: str, text: str) -> dict:
        _text(event_id, "INVALID_EVENT_ID", 512)
        _text(text, "INVALID_EVENT_TEXT", 100_000, empty=True)
        digest = hashlib.sha256(text.encode()).hexdigest()
        now = self._now()
        with self._connection(write=True) as db:
            old = db.execute("SELECT * FROM conversation_events WHERE tenant_id=? AND app_id=? AND event_id=?",
                             (ctx.tenant_id, ctx.app_id, event_id)).fetchone()
            if old is not None:
                old = self._event(db, ctx, event_id)
                if old["input_digest"] != digest:
                    _error("EVENT_CONTENT_MISMATCH")
                if old["state"] == "completed":
                    return {"state": "completed", "reply": json.loads(old["reply_json"])}
                return {"state": "in_progress" if old["state"] == "processing" else "unknown", "reply": None}
            db.execute("INSERT INTO conversation_events(tenant_id,app_id,event_id,scope_key,actor_id,chat_id,thread_id,access_fingerprint,input_digest,state,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,'processing',?,?)",
                       (ctx.tenant_id, ctx.app_id, event_id, ctx.session_id, ctx.actor_id,
                        ctx.chat_id, ctx.thread_id, _derived_fingerprint(ctx), digest, now, now))
            db.execute('UPDATE conversation_events SET content_expires=? WHERE tenant_id=? AND app_id=? AND event_id=?',(min(now+86400,ctx.content_expires_at or now+86400),ctx.tenant_id,ctx.app_id,event_id))
            self._append(db, ctx, "user", text, event_id)
            return {"state": "claimed", "reply": None}

    def finish_event(self, ctx: AccessContext, event_id: str, reply: dict, *, presentation=None) -> None:
        if not isinstance(reply, dict) or not isinstance(reply.get("text"), str):
            _error("INVALID_EVENT_REPLY")
        serialized = _json(reply)
        with self._connection(write=True) as db:
            row = self._event(db, ctx, event_id)
            if row["state"] == "completed":
                if row["reply_json"] != serialized:
                    _error("EVENT_ALREADY_COMPLETED")
                return
            if row["state"] != "processing":
                _error("EVENT_NOT_PROCESSING")
            if presentation is not None:
                from am_outcome.types import fingerprint
                from am_response.schema import VERSION
                binding = presentation['outcome']['binding']
                if (presentation.get('schema_version') != VERSION or binding['engine_event_id'] != event_id
                        or tuple(binding['scope']) != ctx.scope or presentation['semantic_hash'] != fingerprint(reply['text'])
                        or presentation['expires_at'] <= self._now()):
                    _error('PRESENTATION_BINDING_INVALID')
                db.execute('INSERT INTO conversation_presentations_v2 VALUES(?,?,?,?,?,?,?,?,?)',
                    (presentation['presentation_id'], ctx.session_id, ctx.tenant_id, ctx.app_id, event_id,
                     VERSION, presentation['semantic_hash'], _json(presentation),
                     min(presentation['expires_at'], row['content_expires'])))
            db.execute("UPDATE conversation_events SET state='completed',reply_json=?,updated_at=? WHERE tenant_id=? AND app_id=? AND event_id=?",
                       (serialized, self._now(), ctx.tenant_id, ctx.app_id, event_id))
            self._append(db, ctx, "assistant", reply["text"], event_id)

    def fail_event(self, ctx: AccessContext, event_id: str, code: str, *, response_failure=None) -> None:
        _text(code, "INVALID_ERROR_CODE", 128)
        with self._connection(write=True) as db:
            row = self._event(db, ctx, event_id)
            if row["state"] == "completed":
                return
            if response_failure is not None:
                db.execute('INSERT OR IGNORE INTO conversation_response_interrupts_v2 VALUES(?,?,?,?,?,?)',
                    (ctx.tenant_id,ctx.app_id,event_id,ctx.session_id,_json(response_failure),
                     min(row['content_expires'],response_failure['expires_at'])))
            db.execute("UPDATE conversation_events SET state='unknown',error_code=?,updated_at=? WHERE tenant_id=? AND app_id=? AND event_id=?",
                       (code, self._now(), ctx.tenant_id, ctx.app_id, event_id))

    def append_message(self, ctx: AccessContext, role: str, content: Any,
                       event_id: str | None = None) -> None:
        with self._connection(write=True) as db:
            self._append(db, ctx, role, content, event_id)

    def history(self, ctx: AccessContext, limit: int = 20) -> list[dict]:
        _limit(limit, HISTORY_RETENTION)
        with self._connection() as db:
            rows = db.execute("SELECT role,content_json FROM conversation_messages WHERE scope_key=? AND ((role='user' AND access_fingerprint=?) OR (role!='user' AND access_fingerprint=?)) AND content_expires>? ORDER BY id DESC LIMIT ?",
                              (ctx.session_id, _access_fingerprint(ctx), _derived_fingerprint(ctx), self._now(),limit)).fetchall()
        return [{"role": row["role"], "content": json.loads(row["content_json"])} for row in reversed(rows)]

    def cleanup_messages(self):
        """Raw conversation expires; explicitly created work records remain separate."""
        with self._connection(write=True) as db:
            removed=db.execute('DELETE FROM conversation_messages WHERE content_expires<=?',(self._now(),)).rowcount
            db.execute('DELETE FROM conversation_goals_v2 WHERE expires<=?', (self._now(),))
            db.execute('DELETE FROM conversation_presentations_v2 WHERE expires<=?', (self._now(),))
            db.execute('DELETE FROM conversation_response_interrupts_v2 WHERE expires<=?', (self._now(),))
            db.execute("UPDATE conversation_events SET reply_json=NULL,state='expired',error_code='EVENT_CONTENT_EXPIRED' WHERE content_expires<=? AND state!='expired'",(self._now(),))
            db.execute("UPDATE conversation_actions SET arguments_json='{}',preview='',status=CASE WHEN status='cancelled' THEN status ELSE 'expired' END WHERE approved_at IS NULL AND status IN ('draft','expired','cancelled') AND expires_at<=? AND preview!=''",(self._now(),))
        return removed

    def presentation(self, ctx, event_id):
        with self._connection() as db:
            self._event(db, ctx, event_id)
            row = db.execute('SELECT data_json FROM conversation_presentations_v2 WHERE tenant_id=? AND app_id=? AND event_id=? AND scope_key=? AND expires>?',
                             (ctx.tenant_id, ctx.app_id, event_id, ctx.session_id, self._now())).fetchone()
        return json.loads(row[0]) if row else None

    def event_text(self,ctx,event_id):
        with self._connection() as db:
            self._event(db,ctx,event_id)
            row=db.execute("SELECT content_json FROM conversation_messages WHERE scope_key=? AND event_id=? AND role='user' AND content_expires>?",(ctx.session_id,event_id,self._now())).fetchone()
            return json.loads(row[0]) if row else ''

    def set_focus(self, ctx: AccessContext, project_id: str,
                  event_id: str | None = None) -> None:
        """Remember the project just resolved by a trusted, successful read."""
        setter = getattr(self, "focus_setter", None)
        if setter is not None:
            return setter(ctx, project_id, event_id)
        _project(ctx, project_id)
        with self._connection(write=True) as db:
            if event_id is not None:
                self._event(db, ctx, event_id)
            db.execute("INSERT INTO conversation_focus(scope_key,tenant_id,app_id,actor_id,chat_id,thread_id,project_id,event_id,updated_at) VALUES(?,?,?,?,?,?,?,?,?) ON CONFLICT(scope_key) DO UPDATE SET project_id=excluded.project_id,event_id=excluded.event_id,updated_at=excluded.updated_at",
                       (ctx.session_id, ctx.tenant_id, ctx.app_id, ctx.actor_id, ctx.chat_id,
                        ctx.thread_id, project_id, event_id, self._now()))

    def get_focus(self, ctx: AccessContext) -> str | None:
        getter = getattr(self, "focus_getter", None)
        if getter is not None:
            return getter(ctx)
        with self._connection() as db:
            row = db.execute("SELECT project_id FROM conversation_focus WHERE scope_key=?",
                             (ctx.session_id,)).fetchone()
        if row is None or not ctx.can_access_project(row["project_id"]):
            return None
        return row["project_id"]

    @staticmethod
    def _task_record(row: sqlite3.Row) -> dict:
        result = dict(row)
        result.pop("dedupe_key", None)
        result["session_id"] = result.pop("scope_key")
        return result

    def _task(self, db: sqlite3.Connection, ctx: AccessContext, task_id: str) -> sqlite3.Row:
        _text(task_id, "INVALID_TASK_ID", 256)
        row = db.execute("SELECT * FROM conversation_tasks WHERE id=? AND scope_key=?",
                         (task_id, ctx.session_id)).fetchone()
        if row is None:
            _error("TASK_NOT_FOUND")
        if row['source_access_version'] != ctx.source_access_version:
            _error('SOURCE_ACCESS_CHANGED')
        _project(ctx, row["project_id"])
        return row

    def _save_task_version(self, db: sqlite3.Connection, row: sqlite3.Row) -> None:
        db.execute("INSERT INTO conversation_task_versions(task_id,version,record_json,created_at) VALUES(?,?,?,?)",
                   (row["id"], row["version"], _json(self._task_record(row)), self._now()))

    def create_task(self, ctx: AccessContext, *, project_id: str, title: str,
                    details: str = "", due_at: str | None = None, status: str = "open",
                    event_id: str | None = None) -> dict:
        _project(ctx, project_id)
        _text(title, "INVALID_TASK_TITLE", 1000)
        _text(details, "INVALID_TASK_DETAILS", 100_000, empty=True)
        due_at = _due(due_at)
        if not isinstance(status, str) or status not in TASK_STATUSES:
            _error("INVALID_TASK_STATUS")
        if event_id is not None:
            _text(event_id, "INVALID_EVENT_ID", 512)
        payload = [project_id, title, details, due_at, status]
        dedupe = hashlib.sha256(_json([ctx.session_id, event_id, payload]).encode()).hexdigest() if event_id else None
        now = self._now()
        with self._connection(write=True) as db:
            if event_id is not None:
                self._event(db, ctx, event_id)
            if dedupe:
                old = db.execute("SELECT * FROM conversation_tasks WHERE dedupe_key=?", (dedupe,)).fetchone()
                if old is not None:
                    return self._task_record(old)
            task_id = "task_" + uuid.uuid4().hex
            db.execute("INSERT INTO conversation_tasks(id,tenant_id,app_id,scope_key,actor_id,chat_id,thread_id,project_id,title,details,due_at,status,version,event_id,dedupe_key,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,1,?,?,?,?)",
                       (task_id, ctx.tenant_id, ctx.app_id, ctx.session_id, ctx.actor_id, ctx.chat_id,
                        ctx.thread_id, project_id, title, details, due_at, status, event_id, dedupe, now, now))
            db.execute('UPDATE conversation_tasks SET source_access_version=? WHERE id=?',(ctx.source_access_version,task_id))
            row = self._task(db, ctx, task_id)
            self._save_task_version(db, row)
            return self._task_record(row)

    def get_task(self, ctx: AccessContext, task_id: str) -> dict:
        with self._connection() as db:
            return self._task_record(self._task(db, ctx, task_id))

    def list_tasks(self, ctx: AccessContext, *, project_id: str | None = None,
                   include_closed: bool = False, limit: int = 30) -> list[dict]:
        _limit(limit, 200)
        if type(include_closed) is not bool:
            _error("INVALID_INCLUDE_CLOSED")
        clauses = ["scope_key=?", "source_access_version IS ?"]
        args: list[Any] = [ctx.session_id, ctx.source_access_version]
        if project_id is not None:
            _project(ctx, project_id)
            clauses.append("project_id=?")
            args.append(project_id)
        elif not ctx.all_projects:
            if not ctx.allowed_project_ids:
                return []
            clauses.append("project_id IN (" + ",".join("?" for _ in ctx.allowed_project_ids) + ")")
            args.extend(sorted(ctx.allowed_project_ids))
        if not include_closed:
            clauses.append("status IN ('open','waiting')")
        args.append(limit)
        with self._connection() as db:
            rows = db.execute("SELECT * FROM conversation_tasks WHERE " + " AND ".join(clauses) + " ORDER BY updated_at DESC,id LIMIT ?", args).fetchall()
        return [self._task_record(row) for row in rows]

    def update_task(self, ctx: AccessContext, task_id: str, *, expected_version: int,
                    changes: dict) -> dict:
        _version(expected_version)
        if not isinstance(changes, dict) or not changes or set(changes) - {"title", "details", "due_at", "status"}:
            _error("INVALID_TASK_CHANGES")
        clean = dict(changes)
        if "title" in clean:
            _text(clean["title"], "INVALID_TASK_TITLE", 1000)
        if "details" in clean:
            _text(clean["details"], "INVALID_TASK_DETAILS", 100_000, empty=True)
        if "due_at" in clean:
            clean["due_at"] = _due(clean["due_at"])
        if "status" in clean and (not isinstance(clean["status"], str) or clean["status"] not in TASK_STATUSES):
            _error("INVALID_TASK_STATUS")
        with self._connection(write=True) as db:
            row = self._task(db, ctx, task_id)
            if row["version"] != expected_version:
                _error("VERSION_CONFLICT")
            assignments = [name + "=?" for name in clean]
            args = list(clean.values()) + [self._now(), task_id, expected_version]
            db.execute("UPDATE conversation_tasks SET " + ",".join(assignments) + ",version=version+1,updated_at=? WHERE id=? AND version=?", args)
            updated = self._task(db, ctx, task_id)
            self._save_task_version(db, updated)
            return self._task_record(updated)

    @staticmethod
    def _digest(action_id: str, version: int, tool_name: str, arguments: dict,
                project_id: str | None, preview: str) -> str:
        payload = {"id": action_id, "version": version, "tool": tool_name,
                   "arguments": arguments, "project_id": project_id, "preview": preview}
        return hashlib.sha256(_json(payload).encode()).hexdigest()

    @staticmethod
    def _action_record(row: sqlite3.Row) -> dict:
        result = dict(row)
        result["session_id"] = result.pop("scope_key")
        result["arguments"] = json.loads(result.pop("arguments_json"))
        serialized_result = result.pop("result_json")
        result["result"] = json.loads(serialized_result) if serialized_result is not None else None
        return result

    def _action(self, db: sqlite3.Connection, ctx: AccessContext, action_id: str,
                *, expire: bool = True, source_access: bool = True) -> sqlite3.Row:
        _text(action_id, "INVALID_ACTION_ID", 256)
        row = db.execute("SELECT * FROM conversation_actions WHERE id=? AND scope_key=?",
                         (action_id, ctx.session_id)).fetchone()
        if row is None:
            _error("ACTION_NOT_FOUND")
        if source_access and row['source_access_version'] != ctx.source_access_version:
            _error('SOURCE_ACCESS_CHANGED')
        _project(ctx, row["project_id"], optional=True)
        if expire and row["status"] in {"draft", "approved"} and row["expires_at"] <= self._now():
            db.execute("UPDATE conversation_actions SET status='expired',updated_at=? WHERE id=?",
                       (self._now(), action_id))
            row = db.execute("SELECT * FROM conversation_actions WHERE id=?", (action_id,)).fetchone()
        return row

    def create_action(self, ctx: AccessContext, *, tool_name: str, arguments: dict,
                      project_id: str | None = None, preview: str,
                      ttl_seconds: float = 1800) -> dict:
        if not isinstance(tool_name, str) or not re.fullmatch(r"[A-Za-z][A-Za-z0-9_.:-]{0,127}", tool_name):
            _error("INVALID_TOOL_NAME")
        if not isinstance(arguments, dict):
            _error("INVALID_ARGUMENTS")
        serialized = _json(arguments)
        _project(ctx, project_id, optional=True)
        _text(preview, "INVALID_PREVIEW", 100_000)
        if isinstance(ttl_seconds, bool) or not isinstance(ttl_seconds, (int, float)) or not math.isfinite(ttl_seconds) or not 0 < ttl_seconds <= 604800:
            _error("INVALID_ACTION_TTL")
        now = self._now()
        action_id = "action_" + uuid.uuid4().hex
        digest = self._digest(action_id, 1, tool_name, arguments, project_id, preview)
        with self._connection(write=True) as db:
            db.execute("INSERT INTO conversation_actions(id,tenant_id,app_id,scope_key,actor_id,chat_id,thread_id,project_id,tool_name,arguments_json,preview,version,digest,status,expires_at,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,1,?,'draft',?,?,?)",
                       (action_id, ctx.tenant_id, ctx.app_id, ctx.session_id, ctx.actor_id,
                        ctx.chat_id, ctx.thread_id, project_id, tool_name, serialized,
                        preview, digest, min(now + ttl_seconds,ctx.content_expires_at or now+ttl_seconds), now, now))
            db.execute('UPDATE conversation_actions SET source_access_version=? WHERE id=?',(ctx.source_access_version,action_id))
            return self._action_record(self._action(db, ctx, action_id))

    def get_action(self, ctx: AccessContext, action_id: str) -> dict:
        with self._connection(write=True) as db:
            return self._action_record(self._action(db, ctx, action_id))

    def action_metadata(self,ctx,action_id):
        """No revoked payload/preview; owner can still cancel or reconcile IDs."""
        if not ctx.is_owner:_error('OWNER_REQUIRED')
        with self._connection(write=True) as db:
            row=self._action(db,ctx,action_id,source_access=False)
            result={k:row[k] for k in ('id','version','status','tool_name')}
            saved=json.loads(row['result_json'] or '{}')
            result['idempotency_key']=saved.get('idempotency_key')
            return result

    def list_actions(self, ctx: AccessContext, *, active_only: bool = True, metadata_only: bool = False) -> list[dict]:
        if type(active_only) is not bool:
            _error("INVALID_ACTIVE_ONLY")
        with self._connection(write=True) as db:
            db.execute("UPDATE conversation_actions SET status='expired',updated_at=? WHERE scope_key=? AND status IN ('draft','approved') AND expires_at<=?",
                       (self._now(), ctx.session_id, self._now()))
            rows = db.execute("SELECT * FROM conversation_actions WHERE scope_key=? ORDER BY updated_at DESC,id", (ctx.session_id,)).fetchall()
            if metadata_only and not ctx.is_owner:_error('OWNER_REQUIRED')
            return [({k:row[k] for k in ('id','version','status','tool_name')} if metadata_only else self._action_record(row)) for row in rows
                    if (metadata_only or row['source_access_version'] == ctx.source_access_version)
                    and (not active_only or row["status"] in {"draft", "approved", "executing", "unknown"})
                    and (row["project_id"] is None or ctx.can_access_project(row["project_id"]))]

    def revise_action(self, ctx: AccessContext, action_id: str, *, expected_version: int,
                      arguments: dict, preview: str) -> dict:
        _version(expected_version)
        if not isinstance(arguments, dict):
            _error("INVALID_ARGUMENTS")
        serialized = _json(arguments)
        _text(preview, "INVALID_PREVIEW", 100_000)
        with self._connection(write=True) as db:
            row = self._action(db, ctx, action_id)
            if row["version"] != expected_version:
                _error("VERSION_CONFLICT")
            if row["status"] not in {"draft", "approved"}:
                _error("ACTION_NOT_EDITABLE")
            version = expected_version + 1
            digest = self._digest(action_id, version, row["tool_name"], arguments, row["project_id"], preview)
            db.execute("UPDATE conversation_actions SET arguments_json=?,preview=?,version=?,digest=?,status='draft',presented_version=NULL,presented_event_id=NULL,approved_at=NULL,updated_at=? WHERE id=?",
                       (serialized, preview, version, digest, self._now(), action_id))
            return self._action_record(self._action(db, ctx, action_id))

    def mark_presented(self, ctx: AccessContext, action_id: str, *, expected_version: int,
                       event_id: str) -> dict:
        _version(expected_version)
        with self._connection(write=True) as db:
            self._event(db, ctx, event_id)
            row = self._action(db, ctx, action_id)
            if row["version"] != expected_version:
                _error("VERSION_CONFLICT")
            if row["status"] not in {"draft", "approved"}:
                _error("ACTION_NOT_PRESENTABLE")
            db.execute("UPDATE conversation_actions SET presented_version=?,presented_event_id=?,updated_at=? WHERE id=?",
                       (expected_version, event_id, self._now(), action_id))
            return self._action_record(self._action(db, ctx, action_id))

    def approve_action(self, ctx: AccessContext, action_id: str, *, expected_version: int,
                       digest: str) -> dict:
        if not ctx.is_owner:
            _error("OWNER_REQUIRED")
        _version(expected_version)
        with self._connection(write=True) as db:
            row = self._action(db, ctx, action_id)
            if row["version"] != expected_version:
                _error("VERSION_CONFLICT")
            if row["status"] not in {"draft", "approved"}:
                _error("ACTION_NOT_APPROVABLE")
            if row["presented_version"] != expected_version:
                _error("ACTION_NOT_PRESENTED")
            if not isinstance(digest, str) or not hmac.compare_digest(row["digest"].encode(), digest.encode()):
                _error("ACTION_DIGEST_MISMATCH")
            db.execute("UPDATE conversation_actions SET status='approved',approved_at=?,updated_at=? WHERE id=?",
                       (self._now(), self._now(), action_id))
            return self._action_record(self._action(db, ctx, action_id))

    def cancel_action(self, ctx: AccessContext, action_id: str, *, expected_version: int) -> dict:
        _version(expected_version)
        with self._connection(write=True) as db:
            row = self._action(db, ctx, action_id,source_access=False)
            if row["version"] != expected_version:
                _error("VERSION_CONFLICT")
            if row["status"] == "cancelled":
                return self._action_record(row) if row['source_access_version']==ctx.source_access_version else {'id':row['id'],'version':row['version'],'status':row['status']}
            if row["status"] not in {"draft", "approved"}:
                _error("ACTION_ALREADY_STARTED_OR_CLOSED")
            db.execute("UPDATE conversation_actions SET status='cancelled',approved_at=NULL,updated_at=? WHERE id=?",
                       (self._now(), action_id))
            return self._action_record(self._action(db,ctx,action_id)) if row['source_access_version']==ctx.source_access_version else {'id':row['id'],'version':row['version'],'status':'cancelled'}

    def begin_execution(self, ctx: AccessContext, action_id: str, *, expected_version: int) -> dict:
        if not ctx.is_owner:
            _error("OWNER_REQUIRED")
        _version(expected_version)
        with self._connection(write=True) as db:
            row = self._action(db, ctx, action_id)
            if row["version"] != expected_version:
                _error("VERSION_CONFLICT")
            if row["status"] != "approved":
                return {"claimed": False, "action": self._action_record(row)}
            if row["presented_version"] != expected_version:
                _error("ACTION_NOT_PRESENTED")
            now = self._now()
            db.execute("UPDATE conversation_actions SET status='executing',execution_id=?,started_at=?,updated_at=? WHERE id=? AND status='approved'",
                       (uuid.uuid4().hex, now, now, action_id))
            return {"claimed": True, "action": self._action_record(self._action(db, ctx, action_id))}

    def finish_execution(self, ctx: AccessContext, action_id: str, *, status: str,
                         result: Any) -> dict:
        if not ctx.is_owner:
            _error("OWNER_REQUIRED")
        if not isinstance(status, str) or status not in {"succeeded", "failed", "unknown"}:
            _error("INVALID_EXECUTION_STATUS")
        serialized = _json(result)
        with self._connection(write=True) as db:
            row = self._action(db, ctx, action_id,source_access=False)
            if row["status"] in {"succeeded", "failed"}:
                if row["status"] == status and row["result_json"] == serialized:
                    return self._action_record(row) if row['source_access_version']==ctx.source_access_version else {'id':row['id'],'version':row['version'],'status':row['status']}
                _error("EXECUTION_ALREADY_FINISHED")
            if row["status"] not in {"executing", "unknown"}:
                _error("EXECUTION_NOT_STARTED")
            db.execute("UPDATE conversation_actions SET status=?,result_json=?,updated_at=? WHERE id=?",
                       (status, serialized, self._now(), action_id))
            return self._action_record(self._action(db,ctx,action_id)) if row['source_access_version']==ctx.source_access_version else {'id':row['id'],'version':row['version'],'status':status}

    def recover_inflight(self) -> dict:
        """Mark abandoned work unknown; caller must hold exclusive recovery ownership.

        No side effects are retried. A later verified result may resolve an action
        via finish_execution; another begin_execution remains disallowed.
        """
        now = self._now()
        with self._connection(write=True) as db:
            actions = db.execute("UPDATE conversation_actions SET status='unknown',result_json=?,updated_at=? WHERE status='executing'",
                                 (_json({"code": "WORKER_RECOVERY", "requires_reconciliation": True}), now)).rowcount
            events = db.execute("UPDATE conversation_events SET state='unknown',error_code='WORKER_RECOVERY',updated_at=? WHERE state='processing'", (now,)).rowcount
        return {"actions": actions, "events": events}
