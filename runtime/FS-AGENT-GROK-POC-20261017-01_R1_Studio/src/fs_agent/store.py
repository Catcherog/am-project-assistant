from __future__ import annotations

import json
import sqlite3
import threading
import time
import uuid
from contextlib import contextmanager
from pathlib import Path

CODE_PROGRESS_MODEL = "python:synthetic-progress-v1"

SCHEMA = """
CREATE TABLE IF NOT EXISTS inbox (
 seq INTEGER PRIMARY KEY AUTOINCREMENT, app TEXT NOT NULL, tenant TEXT NOT NULL,
 event_id TEXT NOT NULL, message_id TEXT, user TEXT NOT NULL, chat TEXT NOT NULL,
 chat_type TEXT NOT NULL, kind TEXT NOT NULL, body TEXT, nonce TEXT,
 source_message TEXT, received REAL NOT NULL, expires REAL NOT NULL,
 state TEXT NOT NULL DEFAULT 'received', UNIQUE(app,tenant,event_id),
 UNIQUE(app,tenant,message_id)
);
CREATE TABLE IF NOT EXISTS selection (
 scope TEXT PRIMARY KEY, route TEXT, version INTEGER NOT NULL, expires REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS menus (
 nonce TEXT PRIMARY KEY, scope TEXT NOT NULL, version INTEGER NOT NULL,
 message_id TEXT, expires REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS tasks (
 id TEXT PRIMARY KEY, seq INTEGER UNIQUE NOT NULL REFERENCES inbox(seq),
 app TEXT NOT NULL, tenant TEXT NOT NULL, user TEXT NOT NULL, chat TEXT NOT NULL,
 chat_type TEXT NOT NULL, route TEXT NOT NULL, scope TEXT NOT NULL,
 created REAL NOT NULL, expires REAL NOT NULL, queue_deadline REAL NOT NULL,
 state TEXT NOT NULL, run_id TEXT, pgid INTEGER, process_identity TEXT,
 model TEXT, elapsed REAL, error TEXT, attempts INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS task_queue ON tasks(state,created);
CREATE TABLE IF NOT EXISTS context (
 task_id TEXT NOT NULL, seq INTEGER NOT NULL, scope TEXT NOT NULL,
 role TEXT NOT NULL, body TEXT, expires REAL NOT NULL,
 PRIMARY KEY(task_id,role)
);
CREATE TABLE IF NOT EXISTS outbox (
 id TEXT PRIMARY KEY, task_id TEXT, app TEXT NOT NULL, tenant TEXT NOT NULL,
 user TEXT NOT NULL, chat TEXT NOT NULL, chat_type TEXT NOT NULL, origin_chat_type TEXT NOT NULL,
 kind TEXT NOT NULL, body TEXT, artifact TEXT, nonce TEXT,
 created REAL NOT NULL, expires REAL NOT NULL, state TEXT NOT NULL DEFAULT 'pending',
 message_id TEXT, error TEXT
);
CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS bridge_mode (
 scope TEXT PRIMARY KEY, mode TEXT NOT NULL, expires REAL NOT NULL
);
"""


def scope_of(row, route: str | None = None) -> str:
    parts = [row[k] for k in ("tenant", "app", "chat", "user")]
    return json.dumps(parts + ([route] if route else []), separators=(",", ":"))


class StoreBusyBeforeTransaction(RuntimeError):
    """Another connection owns SQLite; this transaction performed no writes."""


class Store:
    def __init__(self, path: Path, clock=time.time):
        self.clock = clock
        self.lock = threading.RLock()
        self.outbox_wakeup = lambda: None
        self._outbox_changed = False
        path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.conn = sqlite3.connect(path, timeout=1, check_same_thread=False, isolation_level=None)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=FULL")
        self.conn.execute("PRAGMA foreign_keys=ON")
        self.conn.execute("PRAGMA secure_delete=ON")
        self.conn.executescript(SCHEMA)

    @contextmanager
    def tx(self):
        wake = False
        with self.lock:
            try:
                self.conn.execute("BEGIN IMMEDIATE")
            except sqlite3.OperationalError as exc:
                if (not self.conn.in_transaction and getattr(exc,'sqlite_errorcode',None)
                        in (sqlite3.SQLITE_BUSY,sqlite3.SQLITE_LOCKED)):
                    raise StoreBusyBeforeTransaction('STORE_BUSY_BEFORE_TRANSACTION') from exc
                raise
            self._outbox_changed = False
            try:
                guard = getattr(self, "local_write_guard", None)
                if guard is not None:
                    guard(self.conn)
                yield self.conn
                commit_guard = getattr(self, "local_commit_guard", None)
                if commit_guard is not None:
                    commit_guard(self.conn)
                self.conn.execute("COMMIT")
                wake = self._outbox_changed
            except BaseException:
                self.conn.execute("ROLLBACK")
                raise
            finally:
                self._outbox_changed = False
        if wake:
            self.outbox_wakeup()

    def rows(self, sql, args=()):
        with self.lock:
            return [dict(r) for r in self.conn.execute(sql, args).fetchall()]

    def execute(self, sql, args=()):
        with self.tx() as c:
            return c.execute(sql, args).rowcount

    def put(self, event: dict, *, on_insert=None) -> bool:
        now = self.clock()
        # First receipt is immutable. Cap by platform time so delayed old events cannot revive data.
        expires = min(now, event.get("source_time", now)) + 86400
        if expires <= now:
            return False
        keys = ("app", "tenant", "event_id", "message_id", "user", "chat", "chat_type",
                "kind", "body", "nonce", "source_message")
        with self.tx() as c:
            cursor = c.execute(
                "INSERT OR IGNORE INTO inbox (" + ",".join(keys) + ",received,expires) VALUES (" +
                ",".join("?" for _ in range(len(keys) + 2)) + ")",
                tuple(event.get(k) for k in keys) + (now, expires),
            )
            if cursor.rowcount == 1 and on_insert is not None:
                on_insert(c, event, expires)
            return cursor.rowcount == 1

    def pending_inbox(self):
        return self.rows("SELECT * FROM inbox WHERE state='received' AND expires>? ORDER BY seq LIMIT 1", (self.clock(),))

    def selection(self, row):
        found = self.rows("SELECT * FROM selection WHERE scope=? AND expires>?", (scope_of(row), self.clock()))
        return found[0] if found else {"route": None, "version": 0}

    def select(self, row, route):
        with self.tx() as c:
            c.execute("INSERT INTO selection VALUES (?,?,1,?) ON CONFLICT(scope) DO UPDATE SET "
                      "route=excluded.route,version=selection.version+1,expires=excluded.expires",
                      (scope_of(row), route, row["expires"]))
        return self.selection(row)

    def menu(self, row):
        selected = self.select(row, None)
        nonce = uuid.uuid4().hex
        self.execute("INSERT INTO menus VALUES (?,?,?,NULL,?)",
                     (nonce, scope_of(row), selected["version"], row["expires"]))
        return nonce

    def valid_card(self, row) -> bool:
        found = self.rows("SELECT * FROM menus WHERE nonce=? AND expires>?", (row["nonce"], self.clock()))
        if not found:
            return False
        m = found[0]
        return (m["scope"] == scope_of(row) and m["version"] == self.selection(row)["version"]
                and m["message_id"] is not None and m["message_id"] == row["source_message"])

    def add_outbox(self, row, *, text=None, card=None, artifact=None, task_id=None, private=False, nonce=None, conn=None):
        projection = getattr(self, 'response_projection', None)
        if projection is not None and conn is None:
            with self.tx() as transaction:
                return self.add_outbox(row, text=text, card=card, artifact=artifact, task_id=task_id,
                                       private=private, nonce=nonce, conn=transaction)
        kind = "file" if artifact else "interactive" if card else "text"
        body = json.dumps(card, ensure_ascii=False) if card else text
        values = (uuid.uuid4().hex, task_id, row["app"], row["tenant"], row["user"], row["chat"],
                  "p2p" if private else row["chat_type"], row["chat_type"], kind, body, artifact, nonce, self.clock(), row["expires"])
        sql = "INSERT INTO outbox (id,task_id,app,tenant,user,chat,chat_type,origin_chat_type,kind,body,artifact,nonce,created,expires) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
        if conn is not None:
            conn.execute(sql, values)
            if projection is not None:
                projection.bind(conn, row, values[0], kind, body, artifact)
            self._outbox_changed = True
        else:
            self.execute(sql, values)
            self.outbox_wakeup()
        return values[0]

    def enqueue(self, row, route, *, acknowledgement=True):
        task_id = uuid.uuid4().hex
        with self.tx() as c:
            old = c.execute("SELECT id FROM tasks WHERE seq=?", (row["seq"],)).fetchone()
            if old:
                c.execute("UPDATE inbox SET state='handled' WHERE seq=?", (row["seq"],))
                return old[0]
            c.execute("INSERT INTO tasks (id,seq,app,tenant,user,chat,chat_type,route,scope,created,expires,queue_deadline,state) "
                      "VALUES (?,?,?,?,?,?,?,?,?,?,?,?, 'queued')",
                      (task_id, row["seq"], row["app"], row["tenant"], row["user"], row["chat"], row["chat_type"],
                       route, scope_of(row, route), row["received"], row["expires"], row["received"] + 600))
            c.execute("UPDATE inbox SET state='handled' WHERE seq=?", (row["seq"],))
            if acknowledgement:
                self.add_outbox(row, text=f"合成任务已排队：{task_id} · /{route}", task_id=task_id, conn=c)
        return task_id

    def claim(self, limit=2):
        now = self.clock()
        with self.tx() as c:
            # Resource windows first stop new claims, then drain running work.
            # Keep queued jobs visible to the unchanged R4 business-priority check.
            if c.execute("SELECT 1 FROM meta WHERE key='resource_window_owner' AND value!=''").fetchone():
                return None
            c.execute("UPDATE tasks SET state='queue_timeout',error='QUEUE_TIMEOUT' WHERE state='queued' AND queue_deadline<=?", (now,))
            active = c.execute("SELECT tenant,app,user FROM tasks WHERE state IN ('running','stop_unknown')").fetchall()
            if len(active) >= limit:
                return None
            busy = {tuple(r) for r in active}
            for row in c.execute("SELECT * FROM tasks WHERE state='queued' AND expires>? ORDER BY created,seq", (now,)).fetchall():
                if (row["tenant"], row["app"], row["user"]) in busy:
                    continue
                run_id = uuid.uuid4().hex
                # Persist execution kind in the same transaction as claiming.
                # Legacy progress runs lack this marker and remain conservative
                # on recovery; route alone never proves no API was dispatched.
                execution_model = CODE_PROGRESS_MODEL if row["route"] == "progress" else None
                c.execute("UPDATE tasks SET state='running',run_id=?,model=?,attempts=attempts+1 WHERE id=?",
                          (run_id, execution_model, row["id"]))
                result = dict(row)
                result.update(run_id=run_id, state="running", model=execution_model)
                return result
        return None

    def history(self, task):
        return self.rows("SELECT c.role,c.body,c.expires FROM context c JOIN tasks t ON t.id=c.task_id "
                         "WHERE c.scope=? AND c.seq<? AND c.expires>? AND t.state='completed' "
                         "ORDER BY c.seq,c.role DESC", (task["scope"], task["seq"], self.clock()))

    def complete(self, task, prompt, result, *, expires, artifact=None, guard=None, label="合成任务", record_context=True, presenter=None):
        with self.tx() as c:
            row = c.execute("SELECT * FROM tasks WHERE id=? AND run_id=? AND state='running'", (task["id"], task["run_id"])).fetchone()
            if not row or expires <= self.clock() or (guard is not None and not guard()):
                return False
            c.execute("UPDATE tasks SET state='completed',model=?,elapsed=?,expires=?,pgid=NULL,process_identity=NULL WHERE id=?",
                      (result.model, result.elapsed, expires, task["id"]))
            c.execute("UPDATE outbox SET expires=MIN(expires,?) WHERE task_id=?", (expires, task["id"]))
            if record_context:
                for role, body in (("user", prompt), ("assistant", result.text)):
                    c.execute("INSERT OR REPLACE INTO context VALUES (?,?,?,?,?,?)", (task["id"], task["seq"], task["scope"], role, body, expires))
            delivery = dict(task, expires=expires)
            if presenter is None or not presenter(delivery,result,c):
                self.add_outbox(delivery, text=f"{label} {task['id']}\n{result.text}", private=True, task_id=task["id"], conn=c)
            if artifact:
                self.add_outbox(delivery, artifact=artifact, private=True, task_id=task["id"], conn=c)
            if task["chat_type"] == "group":
                # Fixed allowlisted summary. Model output is never published into a group.
                summary = "合成进度：总计 5 项，已完成 3 项，进行中 2 项。" if task["route"] == "progress" else "合成文档已生成。"
                self.add_outbox(delivery, text=f"任务 {task['id']}：{summary}详细结果正在私聊交付。", task_id=task["id"], conn=c)
        return True

    def finish(self, task, state, error):
        return self.execute("UPDATE tasks SET state=?,error=? WHERE id=? AND run_id=? AND state='running'",
                            (state, error, task["id"], task["run_id"]))

    def retry(self, row, task_id):
        with self.tx() as c:
            task = c.execute("SELECT * FROM tasks WHERE id=?", (task_id,)).fetchone()
            if not task or scope_of(task) != scope_of(row) or task["expires"] <= self.clock():
                return False
            # A user retry is explicit and retains its original body, expiry and queue deadline.
            if task["state"] not in ("failed", "interrupted", "timed_out") or task["queue_deadline"] <= self.clock():
                return False
            c.execute("UPDATE tasks SET state='queued',error=NULL,pgid=NULL,process_identity=NULL WHERE id=?", (task_id,))
            return True

    def resend(self, row, task_id):
        with self.tx() as c:
            task = c.execute("SELECT * FROM tasks WHERE id=?", (task_id,)).fetchone()
            if not task or scope_of(task) != scope_of(row) or task["expires"] <= self.clock():
                return False
            # Unknown receipts must be reconciled by message ID, not silently retried.
            pending = c.execute("SELECT COUNT(*) FROM outbox WHERE task_id=? AND state IN ('sending','unknown')", (task_id,)).fetchone()[0]
            if pending:
                return False
            c.execute("UPDATE outbox SET state='pending',error=NULL WHERE task_id=? AND state='rejected' AND expires>?",
                      (task_id, self.clock()))
            return c.execute("SELECT changes()").fetchone()[0] > 0

    def meta(self, key):
        rows = self.rows("SELECT value FROM meta WHERE key=?", (key,))
        return rows[0]["value"] if rows else None

    def set_meta(self, key, value):
        self.execute("INSERT INTO meta VALUES (?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", (key, value))

    def cleanup(self):
        now = self.clock()
        with self.tx() as c:
            projection = getattr(self, 'response_projection', None)
            if projection is not None:
                projection.cleanup(c, now)
            elif c.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='am_response_outbox_v2'").fetchone():
                c.execute('DELETE FROM am_response_outbox_v2 WHERE expires<=?', (now,))
            paths = [r[0] for r in c.execute("SELECT artifact FROM outbox WHERE expires<=? AND artifact IS NOT NULL", (now,))]
            c.execute("UPDATE inbox SET body=NULL,nonce=NULL,source_message=NULL,state='expired' WHERE expires<=?", (now,))
            c.execute("UPDATE context SET body=NULL WHERE expires<=?", (now,))
            c.execute("UPDATE outbox SET body=NULL,artifact=NULL,state='expired' WHERE expires<=?", (now,))
            c.execute("UPDATE tasks SET state='expired' WHERE expires<=? AND state NOT IN ('running','stop_unknown')", (now,))
            c.execute("DELETE FROM menus WHERE expires<=?", (now,))
            c.execute("DELETE FROM selection WHERE expires<=?", (now,))
            c.execute("DELETE FROM bridge_mode WHERE expires<=?", (now,))
        with self.lock:
            self.conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        return paths

    def close(self):
        with self.lock:
            self.conn.close()
