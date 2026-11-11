"""Durable, fenced AM turn queue. This module never calls a model or sends a message.

The queue owns a *turn* lane, not the existing ModelCoordinator's provider lock.
An expired lease is quarantined: elapsed time is not evidence that inference stopped.
Every method opens its own SQLite connection; several processes may share one file.
All tables are namespaced, so the queue may share AM's outbox database.

``guard`` is a useful checkpoint, but it cannot make a later external API call atomic.
For a local outbox reservation use ``transaction_guard``/``finish(on_commit=...)``;
the original dispatcher must recheck the fence when changing pending to sending.
Only trusted host code may receive the SQL callbacks below. Never hold their
transactions while doing network I/O, model inference, or waiting for approval.
"""
from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass, replace
import hashlib
import json
import math
import os
from pathlib import Path
import re
import sqlite3
import time
from typing import Any, Callable, Iterator, Mapping
import uuid


SCHEMA_VERSION = 1
ACTIVE_STATES = ("queued", "running")
FINISH_STATES = ("succeeded", "failed", "waiting_review")
_CODE = re.compile(r"^[A-Za-z0-9_.:-]{1,128}$")


class StoreError(RuntimeError):
    """Stable error code; exception strings never contain submitted text."""

    def __init__(self, code: str):
        self.code = code
        super().__init__(code)


def canonical(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True,
                      separators=(",", ":"), allow_nan=False)


def _digest(value: Any) -> str:
    return hashlib.sha256(canonical(value).encode("utf-8")).hexdigest()


def _identifier(value: str, *, empty: bool = False, limit: int = 256) -> str:
    if not isinstance(value, str) or len(value) > limit or (not value and not empty):
        raise StoreError("INVALID_IDENTIFIER")
    if any(ord(c) < 32 for c in value):
        raise StoreError("INVALID_IDENTIFIER")
    return value


def _number(value: float, *, positive: bool = False) -> float:
    if type(value) not in (float, int) or not math.isfinite(value):
        raise StoreError("INVALID_TIME")
    if positive and value <= 0:
        raise StoreError("INVALID_TIME")
    return float(value)


@dataclass(frozen=True)
class Scope:
    """Identity supplied by an authenticated ingress, never by a model/tool argument."""

    tenant_id: str
    app_id: str
    actor_id: str
    chat_id: str
    thread_id: str = ""

    def __post_init__(self) -> None:
        for field in ("tenant_id", "app_id", "actor_id", "chat_id"):
            _identifier(getattr(self, field))
        _identifier(self.thread_id, empty=True)

    @property
    def values(self) -> tuple[str, str, str, str, str]:
        return (self.tenant_id, self.app_id, self.actor_id, self.chat_id, self.thread_id)

    @property
    def key(self) -> str:
        # Same canonical five-tuple as AM AccessContext.session_id.
        return _digest(self.values)

    @classmethod
    def from_context(cls, ctx: Any) -> "Scope":
        return cls(ctx.tenant_id, ctx.app_id, ctx.actor_id, ctx.chat_id,
                   getattr(ctx, "thread_id", ""))


@dataclass(frozen=True)
class Fence:
    run_id: str
    scope_key: str
    revision: int
    cancel_epoch: int
    generation: int = 0


@dataclass(frozen=True)
class RunSnapshot:
    id: str
    scope: Scope
    event_id: str
    input_text: str | None
    revision: int
    cancel_epoch: int
    state: str
    priority: int
    created_at: float
    queued_at: float
    deadline: float
    expires_at: float
    result: Any = None
    error: str | None = None
    purged: bool = False
    lease_generation: int = 0

    @property
    def fence(self) -> Fence:
        return Fence(self.id, self.scope.key, self.revision, self.cancel_epoch, self.lease_generation)


@dataclass(frozen=True)
class Lease:
    token: str
    generation: int
    worker_id: str
    run: RunSnapshot
    expires_at: float

    @property
    def fence(self) -> Fence:
        return self.run.fence


ChangeCallback = Callable[[sqlite3.Connection, Fence], Any]
CommitCallback = Callable[[sqlite3.Connection, RunSnapshot], Any]
ProofVerifier = Callable[[Lease, Mapping[str, Any]], bool]


class DurableStore:
    def __init__(self, path: str | Path, *, clock: Callable[[], float] = time.time,
                 max_queued: int = 32, aging_seconds: float = 30,
                 max_input_chars: int = 6000, busy_timeout_ms: int = 10000):
        if str(path) == ":memory:":
            raise StoreError("PERSISTENT_PATH_REQUIRED")
        if type(max_queued) is not int or max_queued < 1:
            raise StoreError("INVALID_QUEUE_LIMIT")
        if type(max_input_chars) is not int or max_input_chars < 1:
            raise StoreError("INVALID_INPUT_LIMIT")
        if type(busy_timeout_ms) is not int or not 1 <= busy_timeout_ms <= 60000:
            raise StoreError("INVALID_BUSY_TIMEOUT")
        self.path = Path(path).expanduser().resolve()
        self.clock = clock
        self.max_queued = max_queued
        self.aging_seconds = _number(aging_seconds, positive=True)
        self.max_input_chars = max_input_chars
        self.busy_timeout_ms = busy_timeout_ms
        self.path.parent.mkdir(parents=True, exist_ok=True)
        try:
            fd = os.open(self.path, os.O_CREAT | os.O_EXCL | os.O_RDWR, 0o600)
        except FileExistsError:
            pass
        else:
            os.close(fd)
        conn = self._connect()
        try:
            conn.execute("PRAGMA journal_mode=WAL")
        finally:
            conn.close()
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self.path), timeout=self.busy_timeout_ms / 1000,
                               isolation_level=None)
        conn.row_factory = sqlite3.Row
        conn.execute(f"PRAGMA busy_timeout={self.busy_timeout_ms}")
        conn.execute("PRAGMA foreign_keys=ON")
        return conn

    @contextmanager
    def _tx(self) -> Iterator[sqlite3.Connection]:
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            yield conn
            conn.execute("COMMIT")
        except BaseException:
            if conn.in_transaction:
                conn.execute("ROLLBACK")
            raise
        finally:
            conn.close()

    def _initialize(self) -> None:
        statements = [
            """CREATE TABLE IF NOT EXISTS am_harness_runs (
                id TEXT PRIMARY KEY, scope_key TEXT NOT NULL, scope_json TEXT,
                event_id TEXT NOT NULL, input_text TEXT, input_hash TEXT NOT NULL,
                revision INTEGER NOT NULL, cancel_epoch INTEGER NOT NULL,
                state TEXT NOT NULL, priority INTEGER NOT NULL,
                created_at REAL NOT NULL, queued_at REAL NOT NULL, updated_at REAL NOT NULL,
                not_before REAL NOT NULL, deadline REAL NOT NULL, expires_at REAL NOT NULL,
                result_json TEXT, error TEXT, purged INTEGER NOT NULL DEFAULT 0,
                lease_generation INTEGER NOT NULL DEFAULT 0,
                UNIQUE(scope_key,event_id))""",
            """CREATE INDEX IF NOT EXISTS am_harness_runs_queue
                ON am_harness_runs(state,not_before,queued_at)""",
            """CREATE INDEX IF NOT EXISTS am_harness_runs_scope
                ON am_harness_runs(scope_key,state)""",
            """CREATE TABLE IF NOT EXISTS am_harness_events (
                scope_key TEXT NOT NULL,event_id TEXT NOT NULL,operation TEXT NOT NULL,
                request_hash TEXT NOT NULL,run_id TEXT NOT NULL,applied_revision INTEGER NOT NULL,
                applied_cancel_epoch INTEGER NOT NULL,created_at REAL NOT NULL,
                PRIMARY KEY(scope_key,event_id))""",
            """CREATE TABLE IF NOT EXISTS am_harness_lane (
                id INTEGER PRIMARY KEY CHECK(id=1),generation INTEGER NOT NULL,
                state TEXT NOT NULL,token TEXT,worker_id TEXT,run_id TEXT,revision INTEGER,
                cancel_epoch INTEGER,expires_at REAL,reason TEXT)""",
            """CREATE TABLE IF NOT EXISTS am_harness_control_noops (
                scope_key TEXT NOT NULL,event_id TEXT NOT NULL,operation TEXT NOT NULL,
                request_hash TEXT NOT NULL,reason TEXT NOT NULL,created_at REAL NOT NULL,
                PRIMARY KEY(scope_key,event_id))""",
            """CREATE TABLE IF NOT EXISTS am_harness_audit (
                seq INTEGER PRIMARY KEY AUTOINCREMENT,run_id TEXT,event TEXT NOT NULL,
                revision INTEGER,at REAL NOT NULL,details_json TEXT NOT NULL)""",
        ]
        with self._tx() as conn:
            conn.execute("CREATE TABLE IF NOT EXISTS am_harness_meta (key TEXT PRIMARY KEY,value TEXT NOT NULL)")
            row = conn.execute("SELECT value FROM am_harness_meta WHERE key='schema_version'").fetchone()
            if row is not None and row[0] != str(SCHEMA_VERSION):
                raise StoreError("UNSUPPORTED_SCHEMA_VERSION")
            for statement in statements:
                conn.execute(statement)
            conn.execute("INSERT OR IGNORE INTO am_harness_meta(key,value) VALUES('schema_version',?)",
                         (str(SCHEMA_VERSION),))
            conn.execute("INSERT OR IGNORE INTO am_harness_lane(id,generation,state) VALUES(1,0,'idle')")

    @staticmethod
    def _one(conn: sqlite3.Connection, sql: str, params: tuple = ()) -> dict[str, Any] | None:
        cur = conn.execute(sql, params)
        row = cur.fetchone()
        if row is None:
            return None
        if isinstance(row, (sqlite3.Row, dict)):
            return dict(row)
        return dict(zip((col[0] for col in cur.description), row))

    @staticmethod
    def _audit(conn: sqlite3.Connection, event: str, now: float,
               run_id: str | None = None, revision: int | None = None, **details: Any) -> None:
        # Call sites only supply enum codes, counters and proof hashes, not user text.
        conn.execute("INSERT INTO am_harness_audit(run_id,event,revision,at,details_json) VALUES(?,?,?,?,?)",
                     (run_id, event, revision, now, canonical(details)))

    def _maintain(self, conn: sqlite3.Connection, now: float) -> None:
        conn.execute("""UPDATE am_harness_runs SET state='expired',error='DEADLINE_EXCEEDED',updated_at=?
            WHERE state IN ('queued','running') AND deadline<=?""", (now, now))
        # Never release the inference lane because a request or its content expired.
        conn.execute("""UPDATE am_harness_runs SET
            input_text=NULL,result_json=NULL,scope_json=NULL,purged=1,updated_at=?,
            state=CASE WHEN state IN ('queued','running','waiting_review') THEN 'expired' ELSE state END,
            error=CASE WHEN state IN ('queued','running','waiting_review') THEN 'CONTENT_EXPIRED' ELSE error END
            WHERE expires_at<=? AND purged=0""", (now, now))

    @staticmethod
    def _snapshot(row: Mapping[str, Any], scope: Scope | None = None) -> RunSnapshot:
        if scope is None:
            if row["scope_json"] is None:
                raise StoreError("CONTENT_EXPIRED")
            scope = Scope(*json.loads(row["scope_json"]))
        if scope.key != row["scope_key"]:
            raise StoreError("RUN_NOT_FOUND")
        return RunSnapshot(
            id=row["id"], scope=scope, event_id=row["event_id"], input_text=row["input_text"],
            revision=row["revision"], cancel_epoch=row["cancel_epoch"], state=row["state"],
            priority=row["priority"], created_at=row["created_at"], queued_at=row["queued_at"],
            deadline=row["deadline"], expires_at=row["expires_at"],
            result=json.loads(row["result_json"]) if row["result_json"] is not None else None,
            error=row["error"], purged=bool(row["purged"]), lease_generation=row["lease_generation"])

    def _text(self, text: str) -> str:
        if not isinstance(text, str) or not text.strip() or len(text) > self.max_input_chars:
            raise StoreError("INVALID_INPUT")
        return text

    def _run(self, conn: sqlite3.Connection, scope: Scope, run_id: str) -> dict[str, Any]:
        row = self._one(conn, "SELECT * FROM am_harness_runs WHERE id=? AND scope_key=?",
                        (run_id, scope.key))
        if row is None:
            raise StoreError("RUN_NOT_FOUND")
        return row

    def _duplicate(self, conn: sqlite3.Connection, scope: Scope, event_id: str,
                   operation: str, request_hash: str) -> dict[str, Any] | None:
        noop = conn.execute("SELECT 1 FROM am_harness_control_noops WHERE scope_key=? AND event_id=?",
                            (scope.key, event_id)).fetchone()
        if noop is not None:
            raise StoreError("EVENT_CONFLICT")
        event = self._one(conn, "SELECT * FROM am_harness_events WHERE scope_key=? AND event_id=?",
                          (scope.key, event_id))
        if event is None:
            return None
        if event["operation"] != operation or event["request_hash"] != request_hash:
            raise StoreError("EVENT_CONFLICT")
        return self._run(conn, scope, event["run_id"])

    @staticmethod
    def _record_event(conn: sqlite3.Connection, scope: Scope, event_id: str,
                      operation: str, request_hash: str, row: Mapping[str, Any], now: float) -> None:
        conn.execute("""INSERT INTO am_harness_events
            (scope_key,event_id,operation,request_hash,run_id,applied_revision,applied_cancel_epoch,created_at)
            VALUES(?,?,?,?,?,?,?,?)""",
                     (scope.key, event_id, operation, request_hash, row["id"], row["revision"],
                      row["cancel_epoch"], now))

    def submit(self, scope: Scope, event_id: str, input_text: str, *, deadline: float,
               expires_at: float, priority: int = 0) -> RunSnapshot:
        """Duplicate messages do not extend their original deadline or retention TTL.

        The fingerprint covers message content and priority. Absolute transport timing
        is deliberately excluded: a redelivery may compute a new `now + timeout`.
        """
        if not isinstance(scope, Scope):
            raise StoreError("INVALID_SCOPE")
        _identifier(event_id)
        self._text(input_text)
        if type(priority) is not int or not -10 <= priority <= 10:
            raise StoreError("INVALID_PRIORITY")
        deadline, expires_at = _number(deadline), _number(expires_at)
        now = _number(self.clock())
        request_hash = _digest({"input_text": input_text, "priority": priority})
        with self._tx() as conn:
            self._maintain(conn, now)
            old = self._duplicate(conn, scope, event_id, "submit", request_hash)
            if old is not None:
                return self._snapshot(old, scope)
            if deadline <= now or expires_at <= now or deadline > expires_at:
                raise StoreError("INVALID_DEADLINE")
            count = conn.execute("SELECT COUNT(*) FROM am_harness_runs WHERE state IN ('queued','running')").fetchone()[0]
            if count >= self.max_queued:
                raise StoreError("QUEUE_FULL")
            run_id = "run_" + _digest([scope.key, event_id])[:40]
            conn.execute("""INSERT INTO am_harness_runs
                (id,scope_key,scope_json,event_id,input_text,input_hash,revision,cancel_epoch,state,priority,
                 created_at,queued_at,updated_at,not_before,deadline,expires_at)
                VALUES(?,?,?,?,?,?,1,0,'queued',?,?,?,?,?,?,?)""",
                         (run_id, scope.key, canonical(scope.values), event_id, input_text, _digest(input_text),
                          priority, now, now, now, now, deadline, expires_at))
            row = self._run(conn, scope, run_id)
            self._record_event(conn, scope, event_id, "submit", request_hash, row, now)
            self._audit(conn, "submitted", now, run_id, 1, priority=priority)
            return self._snapshot(row, scope)

    def get(self, scope: Scope, run_id: str) -> RunSnapshot:
        with self._tx() as conn:
            self._maintain(conn, _number(self.clock()))
            return self._snapshot(self._run(conn, scope, run_id), scope)

    def active(self, scope: Scope) -> list[RunSnapshot]:
        with self._tx() as conn:
            self._maintain(conn, _number(self.clock()))
            rows = conn.execute("""SELECT * FROM am_harness_runs WHERE scope_key=?
                AND state IN ('queued','running') ORDER BY created_at,id""", (scope.key,)).fetchall()
            return [self._snapshot(row, scope) for row in rows]

    def replay_control(self, scope: Scope, event_id: str, operation: str,
                       input_text: str = "", *, target_run_id: str | None = None) -> RunSnapshot | None:
        """Resolve a redelivered control before selecting the current active task.

        Redelivery never retargets a newer task. The host should pass an explicit
        parsed target ID, when present, so a changed ID is an event conflict.
        This returns the current snapshot of the original target, not a new action.
        """
        _identifier(event_id)
        if operation not in ("cancel", "revise"):
            raise StoreError("INVALID_CONTROL")
        if operation == "revise":
            self._text(input_text)
        with self._tx() as conn:
            self._maintain(conn, _number(self.clock()))
            event = self._one(conn, "SELECT * FROM am_harness_events WHERE scope_key=? AND event_id=?",
                              (scope.key, event_id))
            if event is None:
                return None
            if target_run_id is not None and target_run_id != event["run_id"]:
                raise StoreError("EVENT_CONFLICT")
            body = {"target": event["run_id"]}
            if operation == "revise":
                body["input_text"] = input_text
            row = self._duplicate(conn, scope, event_id, operation, _digest(body))
            assert row is not None
            return self._snapshot(row, scope)

    @staticmethod
    def _noop_hash(operation: str, input_text: str, target_run_id: str | None) -> str:
        if operation not in ("cancel", "revise") or not isinstance(input_text, str):
            raise StoreError("INVALID_CONTROL")
        if target_run_id is not None:
            _identifier(target_run_id)
        return _digest({"operation": operation, "input_text": input_text, "target": target_run_id})

    def record_control_noop(self, scope: Scope, event_id: str, operation: str, input_text: str = "", *,
                            reason: str = "no_active_run", target_run_id: str | None = None) -> str:
        """Remember an inert control, so later redelivery cannot target newer work."""
        _identifier(event_id)
        if reason not in ("no_active_run", "clarification"):
            raise StoreError("INVALID_CONTROL_REASON")
        request_hash = self._noop_hash(operation, input_text, target_run_id)
        with self._tx() as conn:
            if conn.execute("SELECT 1 FROM am_harness_events WHERE scope_key=? AND event_id=?",
                            (scope.key, event_id)).fetchone() is not None:
                raise StoreError("EVENT_CONFLICT")
            row = self._one(conn, "SELECT * FROM am_harness_control_noops WHERE scope_key=? AND event_id=?",
                            (scope.key, event_id))
            if row is not None:
                if row["operation"] != operation or row["request_hash"] != request_hash:
                    raise StoreError("EVENT_CONFLICT")
                return row["reason"]
            conn.execute("""INSERT INTO am_harness_control_noops
                (scope_key,event_id,operation,request_hash,reason,created_at) VALUES(?,?,?,?,?,?)""",
                         (scope.key, event_id, operation, request_hash, reason, _number(self.clock())))
            return reason

    def replay_control_noop(self, scope: Scope, event_id: str, operation: str, input_text: str = "", *,
                            target_run_id: str | None = None) -> str | None:
        _identifier(event_id)
        request_hash = self._noop_hash(operation, input_text, target_run_id)
        conn = self._connect()
        try:
            row = self._one(conn, "SELECT * FROM am_harness_control_noops WHERE scope_key=? AND event_id=?",
                            (scope.key, event_id))
            if row is None:
                return None
            if row["operation"] != operation or row["request_hash"] != request_hash:
                raise StoreError("EVENT_CONFLICT")
            return row["reason"]
        finally:
            conn.close()

    def cancel(self, scope: Scope, event_id: str, target_run_id: str, *,
               on_change: ChangeCallback | None = None) -> RunSnapshot:
        """Cancel future work; never claims that an already sent effect was reversed.

        A completed reply can only be withdrawn by a host `on_change` callback that
        reconciles/removes its pending outbox publication in this same transaction.
        The callback must raise if the required withdrawal cannot be established.
        """
        _identifier(event_id)
        now = _number(self.clock())
        request_hash = _digest({"target": target_run_id})
        with self._tx() as conn:
            self._maintain(conn, now)
            old = self._duplicate(conn, scope, event_id, "cancel", request_hash)
            if old is not None:
                return self._snapshot(old, scope)
            row = self._run(conn, scope, target_run_id)
            if row["state"] == "cancelled":
                self._record_event(conn, scope, event_id, "cancel", request_hash, row, now)
                return self._snapshot(row, scope)
            if row["purged"] or row["state"] in ("expired", "unknown"):
                raise StoreError("RUN_NOT_ACTIVE")
            if row["state"] not in ACTIVE_STATES and on_change is None:
                raise StoreError("RUN_NOT_ACTIVE")
            if on_change is not None:
                on_change(conn, self._snapshot(row, scope).fence)
            conn.execute("""UPDATE am_harness_runs SET state='cancelled',cancel_epoch=cancel_epoch+1,
                result_json=NULL,error='CANCELLED',updated_at=? WHERE id=?""", (now, target_run_id))
            row = self._run(conn, scope, target_run_id)
            self._record_event(conn, scope, event_id, "cancel", request_hash, row, now)
            self._audit(conn, "cancelled", now, target_run_id, row["revision"], cancel_epoch=row["cancel_epoch"])
            return self._snapshot(row, scope)

    def revise(self, scope: Scope, event_id: str, target_run_id: str, input_text: str, *,
               deadline: float | None = None, on_change: ChangeCallback | None = None) -> RunSnapshot:
        """Replace current input. Original content expiry is immutable.

        A running old revision keeps its lane until it returns or is reconciled.
        A caller supplying `deadline` may start a fresh request budget, bounded by
        the original retention expiry; duplicate correction events never extend it.
        """
        _identifier(event_id)
        self._text(input_text)
        if deadline is not None:
            deadline = _number(deadline)
        now = _number(self.clock())
        request_hash = _digest({"target": target_run_id, "input_text": input_text})
        with self._tx() as conn:
            self._maintain(conn, now)
            old = self._duplicate(conn, scope, event_id, "revise", request_hash)
            if old is not None:
                return self._snapshot(old, scope)
            row = self._run(conn, scope, target_run_id)
            if row["state"] not in ACTIVE_STATES or row["purged"]:
                raise StoreError("RUN_NOT_ACTIVE")
            new_deadline = row["deadline"] if deadline is None else deadline
            if not now < new_deadline <= row["expires_at"]:
                raise StoreError("INVALID_DEADLINE")
            if on_change is not None:
                on_change(conn, self._snapshot(row, scope).fence)
            conn.execute("""UPDATE am_harness_runs SET input_text=?,input_hash=?,revision=revision+1,
                state='queued',queued_at=?,updated_at=?,not_before=?,deadline=?,result_json=NULL,error=NULL
                WHERE id=?""", (input_text, _digest(input_text), now, now, now, new_deadline, target_run_id))
            row = self._run(conn, scope, target_run_id)
            self._record_event(conn, scope, event_id, "revise", request_hash, row, now)
            self._audit(conn, "revised", now, target_run_id, row["revision"])
            return self._snapshot(row, scope)

    def _quarantine_overdue(self, conn: sqlite3.Connection, now: float) -> dict[str, Any]:
        lane = self._one(conn, "SELECT * FROM am_harness_lane WHERE id=1")
        assert lane is not None
        if lane["state"] == "active" and lane["expires_at"] <= now:
            self._mark_quarantine(conn, lane, now, "LEASE_EXPIRED")
            lane = self._one(conn, "SELECT * FROM am_harness_lane WHERE id=1")
            assert lane is not None
        return lane

    def _mark_quarantine(self, conn: sqlite3.Connection, lane: Mapping[str, Any], now: float, reason: str) -> None:
        conn.execute("UPDATE am_harness_lane SET state='quarantined',reason=? WHERE id=1", (reason,))
        conn.execute("""UPDATE am_harness_runs SET state='unknown',error=?,updated_at=?
            WHERE id=? AND revision=? AND cancel_epoch=? AND state='running'""",
                     (reason, now, lane["run_id"], lane["revision"], lane["cancel_epoch"]))
        self._audit(conn, "lane_quarantined", now, lane["run_id"], lane["revision"],
                    reason=reason, generation=lane["generation"])

    def claim(self, worker_id: str, *, lease_seconds: float = 30) -> Lease | None:
        _identifier(worker_id, limit=128)
        lease_seconds = _number(lease_seconds, positive=True)
        now = _number(self.clock())
        with self._tx() as conn:
            # Optional original-host metadata is in this SAME SQLite database.
            # Standalone Harness databases have no meta table and keep prior behavior.
            has_host_meta = conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='meta'").fetchone()
            if has_host_meta and conn.execute("SELECT 1 FROM meta WHERE key='resource_window_owner' AND value!=''").fetchone():
                return None
            self._maintain(conn, now)
            lane = self._quarantine_overdue(conn, now)
            if lane["state"] != "idle":
                return None
            row = self._one(conn, """SELECT * FROM am_harness_runs WHERE state='queued'
                AND not_before<=? AND deadline>? AND expires_at>? AND purged=0
                ORDER BY (priority + CAST((? - queued_at) / ? AS INTEGER)) DESC,queued_at,id LIMIT 1""",
                            (now, now, now, now, self.aging_seconds))
            if row is None:
                return None
            token = uuid.uuid4().hex
            generation = lane["generation"] + 1
            expiry = now + lease_seconds
            conn.execute("UPDATE am_harness_runs SET state='running',updated_at=?,lease_generation=? WHERE id=?",
                         (now, generation, row["id"]))
            conn.execute("""UPDATE am_harness_lane SET generation=?,state='active',token=?,worker_id=?,
                run_id=?,revision=?,cancel_epoch=?,expires_at=?,reason=NULL WHERE id=1""",
                         (generation, token, worker_id, row["id"], row["revision"], row["cancel_epoch"], expiry))
            row["state"] = "running"
            row["lease_generation"] = generation
            run = self._snapshot(row)
            self._audit(conn, "claimed", now, run.id, run.revision, generation=generation)
            return Lease(token, generation, worker_id, run, expiry)

    @staticmethod
    def _matches(lane: Mapping[str, Any], lease: Lease) -> bool:
        return (lane["token"] == lease.token and lane["generation"] == lease.generation
                and lane["worker_id"] == lease.worker_id and lane["run_id"] == lease.run.id
                and lane["revision"] == lease.fence.revision and lane["cancel_epoch"] == lease.fence.cancel_epoch)

    def _owned(self, conn: sqlite3.Connection, lease: Lease, now: float, *,
               allow_quarantined: bool = False) -> dict[str, Any]:
        lane = self._quarantine_overdue(conn, now)
        if not self._matches(lane, lease):
            raise StoreError("STALE_LEASE")
        if lane["state"] == "quarantined" and not allow_quarantined:
            raise StoreError("LEASE_QUARANTINED")
        if lane["state"] not in (("active", "quarantined") if allow_quarantined else ("active",)):
            raise StoreError("STALE_LEASE")
        return lane

    def heartbeat(self, lease: Lease, *, lease_seconds: float = 30) -> Lease:
        duration = _number(lease_seconds, positive=True)
        now = _number(self.clock())
        # Commit expiry quarantine even though the caller receives an error.
        error = None
        with self._tx() as conn:
            lane = self._quarantine_overdue(conn, now)
            if not self._matches(lane, lease):
                error = StoreError("STALE_LEASE")
            elif lane["state"] == "quarantined":
                error = StoreError("LEASE_QUARANTINED")
            elif lane["state"] != "active":
                error = StoreError("STALE_LEASE")
            else:
                conn.execute("UPDATE am_harness_lane SET expires_at=? WHERE id=1", (now + duration,))
        if error:
            raise error
        return replace(lease, expires_at=now + duration)

    def check_in_transaction(self, conn: sqlite3.Connection, fence: Fence, *,
                             allowed_states: tuple[str, ...] = ("running", "succeeded", "waiting_review"),
                             enforce_execution_deadline: bool = True) -> RunSnapshot:
        """Validate inside an already open transaction against this exact database.

        It is the host's responsibility to use BEGIN IMMEDIATE before read+write;
        transaction_guard does this automatically. This helper performs no commit.
        A completed reply may outlive its execution deadline while awaiting normal
        outbox delivery. Passing enforce_execution_deadline=False permits only
        succeeded/waiting_review rows; generation, revision, cancellation and
        original content expiry are still checked. It never permits late execution.
        """
        if not conn.in_transaction:
            raise StoreError("TRANSACTION_REQUIRED")
        if type(enforce_execution_deadline) is not bool:
            raise StoreError("INVALID_DEADLINE_POLICY")
        database = conn.execute("PRAGMA database_list").fetchone()
        db_path = database[2] if not isinstance(database, dict) else database["file"]
        if Path(db_path).resolve() != self.path:
            raise StoreError("DATABASE_MISMATCH")
        now = _number(self.clock())
        row = self._one(conn, "SELECT * FROM am_harness_runs WHERE id=? AND scope_key=?",
                        (fence.run_id, fence.scope_key))
        if row is None:
            raise StoreError("RUN_NOT_FOUND")
        if row["revision"] != fence.revision or row["cancel_epoch"] != fence.cancel_epoch:
            raise StoreError("STALE_FENCE")
        if row["expires_at"] <= now or row["purged"]:
            raise StoreError("CONTENT_EXPIRED")
        if not enforce_execution_deadline and row["state"] not in ("succeeded", "waiting_review"):
            raise StoreError("RUN_NOT_ACTIVE")
        if enforce_execution_deadline and row["deadline"] <= now:
            raise StoreError("DEADLINE_EXCEEDED")
        if row["state"] not in allowed_states:
            raise StoreError("RUN_NOT_ACTIVE")
        # A completed revision still belongs to the execution generation that
        # produced it. An earlier not-dispatched attempt must never regain a
        # valid publication fence after a newer attempt finishes the same input.
        if row["lease_generation"] != fence.generation:
            raise StoreError("STALE_LEASE")
        if row["state"] == "running":
            lane = self._one(conn, "SELECT * FROM am_harness_lane WHERE id=1")
            assert lane is not None
            if (lane["state"] != "active" or lane["expires_at"] <= now
                    or lane["run_id"] != fence.run_id or lane["revision"] != fence.revision
                    or lane["cancel_epoch"] != fence.cancel_epoch
                    or lane["generation"] != fence.generation):
                raise StoreError("STALE_LEASE")
        return self._snapshot(row)

    @contextmanager
    def transaction_guard(self, fence: Fence, *,
                          allowed_states: tuple[str, ...] = ("running",),
                          enforce_execution_deadline: bool = True) -> Iterator[sqlite3.Connection]:
        with self._tx() as conn:
            self.check_in_transaction(conn, fence, allowed_states=allowed_states,
                                      enforce_execution_deadline=enforce_execution_deadline)
            yield conn
            # Local SQL should be short, but a deadline may pass even inside it.
            # A late reservation is rolled back rather than committed as current.
            self.check_in_transaction(conn, fence, allowed_states=allowed_states,
                                      enforce_execution_deadline=enforce_execution_deadline)

    def guard(self, fence: Fence) -> RunSnapshot:
        # Persist quarantine before a later guard exception rolls its own read
        # transaction back. check_in_transaction itself never commits host SQL.
        with self._tx() as conn:
            self._quarantine_overdue(conn, _number(self.clock()))
        with self._tx() as conn:
            return self.check_in_transaction(conn, fence, allowed_states=("queued", "running"))

    @staticmethod
    def _release(conn: sqlite3.Connection) -> None:
        conn.execute("""UPDATE am_harness_lane SET state='idle',token=NULL,worker_id=NULL,
            run_id=NULL,revision=NULL,cancel_epoch=NULL,expires_at=NULL,reason=NULL WHERE id=1""")

    def finish(self, lease: Lease, *, result: Any = None, state: str = "succeeded",
               error: str | None = None, on_commit: CommitCallback | None = None) -> bool:
        """Publish a current result and release a known completed turn.

        False means cancellation/correction/expiry suppressed this result. A valid
        old worker still releases its lane. A timed-out lease remains quarantined;
        it needs explicit reconciliation even if that worker later returns.
        on_commit executes only for a current result, before the terminal transition.
        """
        if state not in FINISH_STATES:
            raise StoreError("INVALID_FINISH_STATE")
        if error is not None and (not isinstance(error, str) or not _CODE.fullmatch(error)):
            raise StoreError("INVALID_ERROR_CODE")
        try:
            encoded = canonical(result) if result is not None else None
        except (TypeError, ValueError, OverflowError):
            raise StoreError("INVALID_RESULT") from None
        if encoded is not None and len(encoded.encode("utf-8")) > 65536:
            raise StoreError("RESULT_TOO_LARGE")
        now = _number(self.clock())
        lease_error = None
        applied = False
        with self._tx() as conn:
            self._maintain(conn, now)
            lane = self._quarantine_overdue(conn, now)
            if not self._matches(lane, lease):
                lease_error = StoreError("STALE_LEASE")
            elif lane["state"] == "quarantined":
                lease_error = StoreError("LEASE_QUARANTINED")
            elif lane["state"] != "active":
                lease_error = StoreError("STALE_LEASE")
            else:
                row = self._run(conn, lease.run.scope, lease.run.id)
                applied = (row["revision"] == lease.fence.revision
                           and row["cancel_epoch"] == lease.fence.cancel_epoch
                           and row["state"] == "running" and row["deadline"] > now
                           and row["expires_at"] > now and not row["purged"])
                if applied:
                    if on_commit is not None:
                        on_commit(conn, self._snapshot(row, lease.run.scope))
                    self.check_in_transaction(conn, lease.fence, allowed_states=("running",))
                    conn.execute("""UPDATE am_harness_runs SET state=?,result_json=?,error=?,updated_at=?
                        WHERE id=?""", (state, encoded, error, now, lease.run.id))
                self._release(conn)
                self._audit(conn, "finished" if applied else "stale_result_dropped", now,
                            lease.run.id, lease.fence.revision, state=state, generation=lease.generation)
        if lease_error:
            raise lease_error
        return applied

    def quarantine(self, lease: Lease, *, reason: str = "EXECUTION_UNKNOWN") -> None:
        if not isinstance(reason, str) or not _CODE.fullmatch(reason):
            raise StoreError("INVALID_ERROR_CODE")
        now = _number(self.clock())
        with self._tx() as conn:
            lane = self._one(conn, "SELECT * FROM am_harness_lane WHERE id=1")
            assert lane is not None
            if not self._matches(lane, lease) or lane["state"] not in ("active", "quarantined"):
                raise StoreError("STALE_LEASE")
            self._mark_quarantine(conn, lane, now, reason)

    @staticmethod
    def _verify_proof(lease: Lease, proof: Mapping[str, Any], verifier: ProofVerifier,
                      required_key: str, required_value: str) -> str:
        if not isinstance(proof, Mapping) or proof.get(required_key) != required_value or not callable(verifier):
            raise StoreError("PROOF_REQUIRED")
        try:
            encoded = canonical(dict(proof))
        except (ValueError, TypeError, OverflowError):
            raise StoreError("INVALID_PROOF") from None
        if len(encoded.encode("utf-8")) > 8192:
            raise StoreError("INVALID_PROOF")
        # Potential I/O runs before opening the write transaction. The token and
        # generation are checked again inside it; a newer lease cannot be cleared.
        if verifier(lease, proof) is not True:
            raise StoreError("PROOF_REJECTED")
        return hashlib.sha256(encoded.encode("utf-8")).hexdigest()

    def resolve_quarantine(self, lease: Lease, *, proof: Mapping[str, Any], verifier: ProofVerifier) -> None:
        """Release only after a trusted host proves the exact old execution stopped.

        Does not reset provider pause, repeat tools, or requeue an unknown revision.
        A separately queued correction may proceed after this lane is released.
        Only a proof hash is retained, not the possibly sensitive proof document.
        """
        proof_hash = self._verify_proof(lease, proof, verifier, "execution_state", "stopped")
        now = _number(self.clock())
        with self._tx() as conn:
            lane = self._owned(conn, lease, now, allow_quarantined=True)
            if lane["state"] != "quarantined":
                raise StoreError("LANE_NOT_QUARANTINED")
            self._release(conn)
            self._audit(conn, "lane_reconciled", now, lease.run.id, lease.fence.revision,
                        generation=lease.generation, proof_hash=proof_hash)

    def release_not_dispatched(self, lease: Lease, *, proof: Mapping[str, Any],
                               verifier: ProofVerifier, delay_seconds: float = 1) -> bool:
        """Explicit retry path when the host proves no model/tool was dispatched.

        A generic exception, timeout or MODEL_BUSY after an earlier side effect is
        insufficient. A quarantined lease must use resolve_quarantine instead.
        Returns False if a cancellation/correction already superseded this input.
        """
        proof_hash = self._verify_proof(lease, proof, verifier, "dispatch_state", "not_started")
        delay_seconds = _number(delay_seconds)
        if not 0 <= delay_seconds <= 3600:
            raise StoreError("INVALID_DELAY")
        now = _number(self.clock())
        with self._tx() as conn:
            self._owned(conn, lease, now)
            row = self._run(conn, lease.run.scope, lease.run.id)
            current = (row["state"] == "running" and row["revision"] == lease.fence.revision
                       and row["cancel_epoch"] == lease.fence.cancel_epoch
                       and row["deadline"] > now and row["expires_at"] > now)
            if current:
                conn.execute("UPDATE am_harness_runs SET state='queued',not_before=?,updated_at=? WHERE id=?",
                             (now + delay_seconds, now, lease.run.id))
            self._release(conn)
            self._audit(conn, "not_dispatched", now, lease.run.id, lease.fence.revision,
                        generation=lease.generation, proof_hash=proof_hash, requeued=current)
            return current

    def lane_status(self) -> dict[str, Any]:
        """Trusted operator metadata; no lease token or user text is exposed."""
        with self._tx() as conn:
            lane = self._quarantine_overdue(conn, _number(self.clock()))
            return {key: lane[key] for key in ("state", "generation", "worker_id", "run_id",
                                               "revision", "cancel_epoch", "expires_at", "reason")}

    def purge_expired(self) -> int:
        """Erase expired payloads, preserving event hashes to prevent duplicate effects.

        SQLite may retain deleted bytes in WAL/free pages until checkpoint/vacuum;
        use the deployment's encrypted disk and retention-maintenance procedure.
        This method is logical data expiry, not a forensic secure erase guarantee.
        """
        now = _number(self.clock())
        with self._tx() as conn:
            count = conn.execute("SELECT COUNT(*) FROM am_harness_runs WHERE expires_at<=? AND purged=0", (now,)).fetchone()[0]
            self._maintain(conn, now)
            return count

    def audit(self, *, limit: int = 100) -> list[dict[str, Any]]:
        if type(limit) is not int or not 1 <= limit <= 10000:
            raise StoreError("INVALID_LIMIT")
        conn = self._connect()
        try:
            return [dict(row) for row in conn.execute("SELECT * FROM am_harness_audit ORDER BY seq DESC LIMIT ?", (limit,))]
        finally:
            conn.close()
