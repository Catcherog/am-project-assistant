"""Durable project work shared by trusted entry points, with no external effects.

The host authenticates callers before passing ``principal_id`` or registering
an entry identity. Display names and message text never establish identity.
Follow-ups are approval proposals; this module has no network or send port.
"""
from __future__ import annotations

from contextlib import closing, contextmanager
from dataclasses import dataclass
import hashlib
import json
import math
from pathlib import Path
import sqlite3
import time
import uuid


ORIGINS = frozenset(("feishu", "gpt_dot", "grok_bot"))
TASK_STATES = frozenset(("pending", "in_progress", "waiting_feedback",
                         "awaiting_owner", "reported_complete", "completed",
                         "blocked", "cancelled"))
TERMINAL_STATES = frozenset(("completed", "cancelled"))
FACT_STATES = frozenset(("plan", "reported", "verified", "conflict"))
FACT_CATEGORIES = frozenset(("plan", "progress", "blocker", "todo", "risk"))


class WorkbookError(ValueError):
    pass


class AccessDenied(PermissionError):
    pass


class IdentityConflict(WorkbookError):
    pass


@dataclass(frozen=True)
class IdentityBinding:
    origin: str
    actor_id: str
    principal_id: str
    verification_ref: str
    active: bool = True


SCHEMA = """
CREATE TABLE IF NOT EXISTS a5_wb_meta(key TEXT PRIMARY KEY,value TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS a5_wb_identities(
 origin TEXT NOT NULL,actor_id TEXT NOT NULL,principal_id TEXT NOT NULL,
 verification_ref TEXT NOT NULL,active INTEGER NOT NULL,created_at REAL NOT NULL,
 PRIMARY KEY(origin,actor_id));
CREATE TABLE IF NOT EXISTS a5_wb_project_grants(
 principal_id TEXT NOT NULL,project_id TEXT NOT NULL,can_write INTEGER NOT NULL,
 PRIMARY KEY(principal_id,project_id));
CREATE TABLE IF NOT EXISTS a5_wb_tasks(
 task_id TEXT PRIMARY KEY,requester_id TEXT NOT NULL,title TEXT NOT NULL,
 project_id TEXT,related INTEGER NOT NULL,visibility TEXT NOT NULL,
 state TEXT NOT NULL,priority INTEGER NOT NULL,due_at REAL,
 source_ref TEXT NOT NULL,verification_ref TEXT NOT NULL DEFAULT '',
 created_at REAL NOT NULL,updated_at REAL NOT NULL);
CREATE INDEX IF NOT EXISTS a5_wb_task_project ON a5_wb_tasks(project_id,state);
CREATE TABLE IF NOT EXISTS a5_wb_events(
 event_id TEXT PRIMARY KEY,event_key TEXT NOT NULL UNIQUE,
 fingerprint TEXT NOT NULL,task_id TEXT NOT NULL,principal_id TEXT NOT NULL,
 origin TEXT NOT NULL,actor_id TEXT NOT NULL,kind TEXT NOT NULL,
 text TEXT NOT NULL,source_ref TEXT NOT NULL,at REAL NOT NULL,
 FOREIGN KEY(task_id) REFERENCES a5_wb_tasks(task_id));
CREATE INDEX IF NOT EXISTS a5_wb_event_task ON a5_wb_events(task_id,at,event_id);
CREATE TABLE IF NOT EXISTS a5_wb_steps(
 step_id TEXT PRIMARY KEY,task_id TEXT NOT NULL,step_key TEXT NOT NULL,
 title TEXT NOT NULL,state TEXT NOT NULL,assignee_id TEXT NOT NULL,
 source_ref TEXT NOT NULL,updated_at REAL NOT NULL,
 UNIQUE(task_id,step_key),FOREIGN KEY(task_id) REFERENCES a5_wb_tasks(task_id));
CREATE TABLE IF NOT EXISTS a5_wb_facts(
 fact_id TEXT PRIMARY KEY,event_key TEXT NOT NULL UNIQUE,fingerprint TEXT NOT NULL,
 project_id TEXT NOT NULL,field TEXT NOT NULL,category TEXT NOT NULL,
 value_json TEXT NOT NULL,status TEXT NOT NULL,source_ref TEXT NOT NULL,
 verification_ref TEXT NOT NULL,principal_id TEXT NOT NULL,
 observed_at REAL NOT NULL,recorded_at REAL NOT NULL,supersedes_fact_id TEXT);
CREATE INDEX IF NOT EXISTS a5_wb_fact_project ON a5_wb_facts(project_id,field,observed_at);
CREATE TABLE IF NOT EXISTS a5_wb_followups(
 followup_id TEXT PRIMARY KEY,task_id TEXT NOT NULL,followup_key TEXT NOT NULL,
 recipient_id TEXT NOT NULL,prompt TEXT NOT NULL,source_ref TEXT NOT NULL,
 due_at REAL NOT NULL,state TEXT NOT NULL,created_at REAL NOT NULL,
 resolved_by_event TEXT,UNIQUE(task_id,followup_key),
 FOREIGN KEY(task_id) REFERENCES a5_wb_tasks(task_id));
CREATE TABLE IF NOT EXISTS a5_wb_followup_proposals(
 proposal_id TEXT PRIMARY KEY,followup_id TEXT NOT NULL UNIQUE,
 state TEXT NOT NULL,approval_ref TEXT NOT NULL DEFAULT '',
 delivery_ref TEXT NOT NULL DEFAULT '',created_at REAL NOT NULL,
 FOREIGN KEY(followup_id) REFERENCES a5_wb_followups(followup_id));
"""


def _required(value, name):
    if not isinstance(value, str) or not value.strip():
        raise WorkbookError(name + " is required")
    return value


def _timestamp(value, name):
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
        raise WorkbookError(name + " must be finite")
    return float(value)


def _json(value):
    try:
        return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)
    except (TypeError, ValueError) as exc:
        raise WorkbookError("value must be finite JSON") from exc


def _digest(value):
    return hashlib.sha256(_json(value).encode()).hexdigest()


class AssistantWorkbook:
    """One owner's local work ledger, shared across authenticated ingress ports."""

    def __init__(self, sqlite_path, owner_id, *, clock=time.time):
        self.path = str(Path(sqlite_path).resolve())
        self.owner_id = _required(owner_id, "owner_id")
        self.clock = clock
        Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        with closing(self._connect()) as db:
            db.execute("PRAGMA journal_mode=WAL")
            db.executescript(SCHEMA)
        with self._transaction() as db:
            db.execute("INSERT OR IGNORE INTO a5_wb_meta VALUES('owner_id',?)", (owner_id,))
            if db.execute("SELECT value FROM a5_wb_meta WHERE key='owner_id'").fetchone()[0] != owner_id:
                raise IdentityConflict("database belongs to another owner")
            db.execute("INSERT OR IGNORE INTO a5_wb_meta VALUES('schema_version','1')")
            if db.execute("SELECT value FROM a5_wb_meta WHERE key='schema_version'").fetchone()[0] != "1":
                raise WorkbookError("unsupported workbook schema")
        Path(self.path).chmod(0o600)

    def _connect(self):
        db = sqlite3.connect(self.path, timeout=10, isolation_level=None)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA busy_timeout=10000")
        db.execute("PRAGMA foreign_keys=ON")
        return db

    @contextmanager
    def _transaction(self):
        with closing(self._connect()) as db:
            db.execute("BEGIN IMMEDIATE")
            try:
                yield db
                db.commit()
            except BaseException:
                db.rollback()
                raise

    def _owner(self, principal_id):
        if principal_id != self.owner_id:
            raise AccessDenied("owner authority required")

    def _resolve(self, db, origin, actor_id):
        if origin not in ORIGINS:
            raise WorkbookError("unknown entry origin")
        row = db.execute("SELECT * FROM a5_wb_identities WHERE origin=? AND actor_id=? AND active=1",
                         (origin, actor_id)).fetchone()
        if row is None:
            raise AccessDenied("entry identity is not verified")
        return dict(row)

    def bind_identity(self, origin, actor_id, principal_id, verification_ref, *, authorized_by):
        """Called only by the trusted host after resolving the platform identity."""
        self._owner(authorized_by)
        if origin not in ORIGINS:
            raise WorkbookError("unknown entry origin")
        for name, value in (("actor_id", actor_id), ("principal_id", principal_id),
                            ("verification_ref", verification_ref)):
            _required(value, name)
        with self._transaction() as db:
            old = db.execute("SELECT * FROM a5_wb_identities WHERE origin=? AND actor_id=?",
                             (origin, actor_id)).fetchone()
            if old and old["principal_id"] != principal_id:
                raise IdentityConflict("verified entry cannot be rebound to another principal")
            db.execute("""INSERT INTO a5_wb_identities VALUES(?,?,?,?,1,?)
                ON CONFLICT(origin,actor_id) DO UPDATE SET verification_ref=excluded.verification_ref,active=1""",
                       (origin, actor_id, principal_id, verification_ref, self.clock()))
        return IdentityBinding(origin, actor_id, principal_id, verification_ref)

    def revoke_identity(self, origin, actor_id, *, authorized_by):
        self._owner(authorized_by)
        with self._transaction() as db:
            return bool(db.execute("UPDATE a5_wb_identities SET active=0 WHERE origin=? AND actor_id=?",
                                   (origin, actor_id)).rowcount)

    def resolve_identity(self, origin, actor_id):
        with closing(self._connect()) as db:
            row = self._resolve(db, origin, actor_id)
        return IdentityBinding(row["origin"], row["actor_id"], row["principal_id"], row["verification_ref"])

    def grant_project(self, principal_id, project_id, *, authorized_by, can_write=False):
        self._owner(authorized_by)
        _required(principal_id, "principal_id")
        _required(project_id, "project_id")
        if not isinstance(can_write, bool):
            raise WorkbookError("can_write must be boolean")
        with self._transaction() as db:
            db.execute("""INSERT INTO a5_wb_project_grants VALUES(?,?,?)
                ON CONFLICT(principal_id,project_id) DO UPDATE SET can_write=excluded.can_write""",
                       (principal_id, project_id, int(can_write)))

    def revoke_project(self, principal_id, project_id, *, authorized_by):
        self._owner(authorized_by)
        with self._transaction() as db:
            db.execute("DELETE FROM a5_wb_project_grants WHERE principal_id=? AND project_id=?",
                       (principal_id, project_id))

    def sync_project_grants(self, principal_id, project_ids, *, authorized_by, can_write=False):
        """Replace one principal's grants with the host's current trusted scope.

        Removal and insertion share one transaction, so a stale membership can
        never survive merely because the current set became empty.
        """
        self._owner(authorized_by)
        _required(principal_id, "principal_id")
        if isinstance(project_ids, (str, bytes)) or not isinstance(can_write, bool):
            raise WorkbookError("current project grants must be an ID collection")
        projects = tuple(dict.fromkeys(_required(p, "project_id") for p in project_ids))
        with self._transaction() as db:
            db.execute("DELETE FROM a5_wb_project_grants WHERE principal_id=?", (principal_id,))
            db.executemany("INSERT INTO a5_wb_project_grants VALUES(?,?,?)",
                           ((principal_id, project, int(can_write)) for project in projects))

    def _project_access(self, db, project_id, principal_id, *, write=False):
        _required(principal_id, "principal_id")
        if principal_id == self.owner_id:
            return
        row = db.execute("SELECT can_write FROM a5_wb_project_grants WHERE principal_id=? AND project_id=?",
                         (principal_id, project_id)).fetchone()
        if row is None or (write and not row["can_write"]):
            raise AccessDenied("project access is not granted")

    def _task(self, db, task_id, principal_id, *, write=False):
        row = db.execute("SELECT * FROM a5_wb_tasks WHERE task_id=?", (task_id,)).fetchone()
        # Do not reveal whether an inaccessible task exists.
        if row is None:
            raise AccessDenied("task is unavailable")
        if principal_id == self.owner_id:
            return dict(row)
        if principal_id == row["requester_id"]:
            # A person's own project request is not a permanent project grant.
            # Unscoped private requests and their pending referrals stay visible.
            if row["project_id"] and row["related"]:
                self._project_access(db, row["project_id"], principal_id, write=write)
            return dict(row)
        if row["visibility"] == "project" and row["project_id"]:
            try:
                self._project_access(db, row["project_id"], principal_id, write=write)
                return dict(row)
            except AccessDenied:
                pass
        raise AccessDenied("task is unavailable")

    @staticmethod
    def _event_key(origin, actor_id, event_key):
        _required(event_key, "event_key")
        return _digest([origin, actor_id, event_key])

    def _add_event(self, db, *, event_key, fingerprint, task_id, principal_id, origin,
                   actor_id, kind, text, source_ref, at):
        old = db.execute("SELECT * FROM a5_wb_events WHERE event_key=?", (event_key,)).fetchone()
        if old:
            if old["fingerprint"] != fingerprint or old["task_id"] != task_id:
                raise IdentityConflict("event identity was reused with different content")
            return dict(old), False
        event_id = "we_" + uuid.uuid4().hex
        db.execute("INSERT INTO a5_wb_events VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                   (event_id, event_key, fingerprint, task_id, principal_id, origin,
                    actor_id, kind, text, source_ref, at))
        return dict(db.execute("SELECT * FROM a5_wb_events WHERE event_id=?", (event_id,)).fetchone()), True

    def accept_task(self, event_key, origin, actor_id, title, project_id=None, *, related=True,
                    task_id=None, source_ref="", due_at=None, visibility="project", priority=2):
        """Create from a verified caller, or attach a new entry to an existing task.

        ``related`` is supplied by the trusted policy assessment. An unrelated
        request is recorded as awaiting_owner and cannot autonomously progress.
        Task IDs, not matching titles or display names, connect entry points.
        """
        _required(title, "title")
        if project_id is not None:
            _required(project_id, "project_id")
        if not isinstance(related, bool) or visibility not in ("private", "project"):
            raise WorkbookError("invalid task scope")
        if isinstance(priority, bool) or priority not in range(4):
            raise WorkbookError("priority must be P0 through P3")
        if due_at is not None:
            due_at = _timestamp(due_at, "due_at")
        now = _timestamp(self.clock(), "clock")
        event_identity = self._event_key(origin, actor_id, event_key)
        with self._transaction() as db:
            principal_id = self._resolve(db, origin, actor_id)["principal_id"]
            semantic = [principal_id, title, project_id, related, task_id, source_ref,
                        due_at, visibility, priority]
            fingerprint = _digest(semantic)
            old = db.execute("SELECT * FROM a5_wb_events WHERE event_key=?", (event_identity,)).fetchone()
            if old:
                if old["fingerprint"] != fingerprint:
                    raise IdentityConflict("task event identity changed")
                return self._task(db, old["task_id"], principal_id)
            found = db.execute("SELECT 1 FROM a5_wb_tasks WHERE task_id=?", (task_id,)).fetchone() if task_id else None
            if found:
                existing = self._task(db, task_id, principal_id)
                if project_id is not None and existing["project_id"] != project_id:
                    raise IdentityConflict("continuation project differs from the task")
                kind = "continued"
            else:
                if related and project_id:
                    self._project_access(db, project_id, principal_id)
                task_id = _required(task_id, "task_id") if task_id else "wt_" + uuid.uuid4().hex
                state = "pending" if related else "awaiting_owner"
                db.execute("INSERT INTO a5_wb_tasks VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
                           (task_id, principal_id, title, project_id, int(related),
                            visibility if project_id else "private", state, priority, due_at,
                            source_ref, "", now, now))
                kind = "requested"
            self._add_event(db, event_key=event_identity, fingerprint=fingerprint, task_id=task_id,
                            principal_id=principal_id, origin=origin, actor_id=actor_id,
                            kind=kind, text=title, source_ref=source_ref, at=now)
            return self._task(db, task_id, principal_id)

    def get_task(self, task_id, *, principal_id, include_events=False):
        with closing(self._connect()) as db:
            result = self._task(db, task_id, principal_id)
            result["steps"] = [dict(row) for row in db.execute(
                "SELECT * FROM a5_wb_steps WHERE task_id=? ORDER BY updated_at,step_id", (task_id,))]
            if include_events:
                result["events"] = [dict(row) for row in db.execute(
                    "SELECT * FROM a5_wb_events WHERE task_id=? ORDER BY at,event_id", (task_id,))]
            return result

    def append_event(self, task_id, event_key, *, principal_id, kind, text, source_ref, at=None):
        _required(kind, "kind")
        _required(source_ref, "source_ref")
        if not isinstance(text, str):
            raise WorkbookError("text must be a string")
        at = _timestamp(self.clock() if at is None else at, "at")
        with self._transaction() as db:
            self._task(db, task_id, principal_id, write=True)
            event, fresh = self._add_event(db, event_key=self._event_key("host", principal_id, event_key),
                fingerprint=_digest([task_id, principal_id, kind, text, source_ref]), task_id=task_id,
                principal_id=principal_id, origin="host", actor_id=principal_id, kind=kind,
                text=text, source_ref=source_ref, at=at)
            if fresh:
                db.execute("UPDATE a5_wb_tasks SET updated_at=max(updated_at,?) WHERE task_id=?", (at, task_id))
            return event

    def transition(self, task_id, state, *, principal_id, source_ref, verification_ref=""):
        if state not in TASK_STATES:
            raise WorkbookError("unknown task state")
        _required(source_ref, "source_ref")
        with self._transaction() as db:
            task = self._task(db, task_id, principal_id, write=True)
            if not task["related"] and state not in ("awaiting_owner", "cancelled"):
                raise AccessDenied("unrelated request needs an explicit owner decision")
            if state == "completed":
                self._owner(principal_id)
                _required(verification_ref, "verification_ref")
            if task["state"] in TERMINAL_STATES and state != task["state"]:
                raise WorkbookError("terminal task needs an explicit owner reopening")
            now = self.clock()
            if task["state"] == state:
                return task
            db.execute("UPDATE a5_wb_tasks SET state=?,verification_ref=?,updated_at=? WHERE task_id=?",
                       (state, verification_ref if state == "completed" else "", now, task_id))
            self._add_event(db, event_key="transition_" + uuid.uuid4().hex,
                fingerprint=_digest([task_id, state, source_ref, verification_ref]), task_id=task_id,
                principal_id=principal_id, origin="host", actor_id=principal_id,
                kind="state_changed", text=state, source_ref=source_ref, at=now)
            if state in TERMINAL_STATES:
                self._cancel_followups(db, task_id)
            return self._task(db, task_id, principal_id)

    def decide_unrelated(self, task_id, *, principal_id, accept, source_ref):
        self._owner(principal_id)
        _required(source_ref, "source_ref")
        if not isinstance(accept, bool):
            raise WorkbookError("accept must be boolean")
        with self._transaction() as db:
            task = self._task(db, task_id, principal_id)
            if task["related"] or task["state"] != "awaiting_owner":
                raise WorkbookError("request is not awaiting a scope decision")
            state = "pending" if accept else "cancelled"
            db.execute("UPDATE a5_wb_tasks SET related=?,state=?,updated_at=? WHERE task_id=?",
                       (int(accept), state, self.clock(), task_id))
            self._add_event(db, event_key="scope_" + task_id,
                fingerprint=_digest([task_id, accept, source_ref]), task_id=task_id,
                principal_id=principal_id, origin="host", actor_id=principal_id,
                kind="owner_scope_decision", text=state, source_ref=source_ref, at=self.clock())
            return self._task(db, task_id, principal_id)

    def reopen(self, task_id, *, principal_id, source_ref):
        self._owner(principal_id)
        _required(source_ref, "source_ref")
        with self._transaction() as db:
            task = self._task(db, task_id, principal_id)
            if task["state"] not in TERMINAL_STATES:
                raise WorkbookError("task is not terminal")
            state = "pending" if task["related"] else "awaiting_owner"
            db.execute("UPDATE a5_wb_tasks SET state=?,verification_ref='',updated_at=? WHERE task_id=?",
                       (state, self.clock(), task_id))
            self._add_event(db, event_key="reopen_" + uuid.uuid4().hex,
                fingerprint=_digest([task_id, source_ref]), task_id=task_id, principal_id=principal_id,
                origin="host", actor_id=principal_id, kind="reopened", text=state,
                source_ref=source_ref, at=self.clock())
            return self._task(db, task_id, principal_id)

    def add_step(self, task_id, step_key, title, *, principal_id, assignee_id="", source_ref=""):
        _required(step_key, "step_key")
        _required(title, "title")
        _required(source_ref, "source_ref")
        with self._transaction() as db:
            task = self._task(db, task_id, principal_id, write=True)
            if not task["related"] or task["state"] in TERMINAL_STATES:
                raise WorkbookError("task cannot be planned in this state")
            old = db.execute("SELECT * FROM a5_wb_steps WHERE task_id=? AND step_key=?",
                             (task_id, step_key)).fetchone()
            if old:
                if (old["title"], old["assignee_id"], old["source_ref"]) != (title, assignee_id, source_ref):
                    raise IdentityConflict("step key reused with different content")
                return dict(old)
            step_id = "ws_" + uuid.uuid4().hex
            db.execute("INSERT INTO a5_wb_steps VALUES(?,?,?,?,?,?,?,?)",
                       (step_id, task_id, step_key, title, "pending", assignee_id, source_ref, self.clock()))
            return dict(db.execute("SELECT * FROM a5_wb_steps WHERE step_id=?", (step_id,)).fetchone())

    def update_step(self, step_id, state, *, principal_id, source_ref):
        if state not in ("pending", "in_progress", "completed", "blocked", "cancelled"):
            raise WorkbookError("unknown step state")
        _required(source_ref, "source_ref")
        with self._transaction() as db:
            step = db.execute("SELECT * FROM a5_wb_steps WHERE step_id=?", (step_id,)).fetchone()
            if step is None:
                raise AccessDenied("step is unavailable")
            task = self._task(db, step["task_id"], principal_id, write=True)
            if task["state"] in TERMINAL_STATES:
                raise WorkbookError("task is terminal")
            db.execute("UPDATE a5_wb_steps SET state=?,source_ref=?,updated_at=? WHERE step_id=?",
                       (state, source_ref, self.clock(), step_id))
            return dict(db.execute("SELECT * FROM a5_wb_steps WHERE step_id=?", (step_id,)).fetchone())

    def record_fact(self, project_id, field, value, *, principal_id, source_ref, event_key,
                    status="reported", category="progress", observed_at=None,
                    verification_ref="", supersedes_fact_id=None):
        _required(project_id, "project_id")
        _required(field, "field")
        _required(source_ref, "source_ref")
        _required(event_key, "event_key")
        if status not in FACT_STATES or category not in FACT_CATEGORIES:
            raise WorkbookError("unknown fact status or category")
        if status == "verified":
            _required(verification_ref, "verification_ref")
        if category == "plan" and status not in ("plan", "conflict"):
            raise WorkbookError("plans must remain plans")
        observed_at = _timestamp(self.clock() if observed_at is None else observed_at, "observed_at")
        value_json = _json(value)
        fingerprint = _digest([project_id, field, value, status, category, source_ref,
                               verification_ref, supersedes_fact_id])
        fact_key = self._event_key("fact", principal_id, event_key)
        with self._transaction() as db:
            self._project_access(db, project_id, principal_id, write=True)
            old = db.execute("SELECT * FROM a5_wb_facts WHERE event_key=?", (fact_key,)).fetchone()
            if old:
                if old["fingerprint"] != fingerprint:
                    raise IdentityConflict("fact key reused with different content")
                return self._fact(old)
            if supersedes_fact_id:
                previous = db.execute("SELECT * FROM a5_wb_facts WHERE fact_id=?", (supersedes_fact_id,)).fetchone()
                if previous is None or (previous["project_id"], previous["field"]) != (project_id, field):
                    raise WorkbookError("superseded fact is outside this field")
                if observed_at < previous["observed_at"]:
                    raise WorkbookError("older observation cannot supersede a newer fact")
                if status == "conflict" or (previous["status"] in ("plan", "verified") and status != previous["status"]):
                    raise WorkbookError("candidate feedback cannot erase a plan or verified fact")
            fact_id = "wf_" + uuid.uuid4().hex
            db.execute("INSERT INTO a5_wb_facts VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                       (fact_id, fact_key, fingerprint, project_id, field, category, value_json,
                        status, source_ref, verification_ref, principal_id, observed_at, self.clock(), supersedes_fact_id))
            return self._fact(db.execute("SELECT * FROM a5_wb_facts WHERE fact_id=?", (fact_id,)).fetchone())

    @staticmethod
    def _fact(row):
        result = dict(row)
        result["value"] = json.loads(result.pop("value_json"))
        return result

    def plan_followup(self, task_id, followup_key, recipient_id, due_at, prompt, *, principal_id, source_ref):
        for name, value in (("followup_key", followup_key), ("recipient_id", recipient_id),
                            ("prompt", prompt), ("source_ref", source_ref)):
            _required(value, name)
        due_at = _timestamp(due_at, "due_at")
        with self._transaction() as db:
            task = self._task(db, task_id, principal_id, write=True)
            if not task["related"] or task["state"] in TERMINAL_STATES:
                raise WorkbookError("task cannot be followed up in this state")
            old = db.execute("SELECT * FROM a5_wb_followups WHERE task_id=? AND followup_key=?",
                             (task_id, followup_key)).fetchone()
            if old:
                if (old["recipient_id"], old["due_at"], old["prompt"], old["source_ref"]) != (recipient_id, due_at, prompt, source_ref):
                    raise IdentityConflict("follow-up key reused with different content")
                return dict(old)
            followup_id = "wu_" + uuid.uuid4().hex
            db.execute("INSERT INTO a5_wb_followups VALUES(?,?,?,?,?,?,?,?,?,?)",
                       (followup_id, task_id, followup_key, recipient_id, prompt, source_ref,
                        due_at, "scheduled", self.clock(), None))
            return dict(db.execute("SELECT * FROM a5_wb_followups WHERE followup_id=?", (followup_id,)).fetchone())

    def due_followups(self, now=None, *, principal_id, limit=100):
        """Return stable, pending approval proposals. Never sends a message."""
        self._owner(principal_id)
        now = _timestamp(self.clock() if now is None else now, "now")
        if not isinstance(limit, int) or isinstance(limit, bool) or not 1 <= limit <= 1000:
            raise WorkbookError("invalid limit")
        with self._transaction() as db:
            rows = db.execute("""SELECT f.* FROM a5_wb_followups f JOIN a5_wb_tasks t USING(task_id)
                WHERE f.state='scheduled' AND f.due_at<=? AND t.related=1
                  AND t.state NOT IN ('completed','cancelled','awaiting_owner')
                ORDER BY f.due_at,f.followup_id LIMIT ?""", (now, limit)).fetchall()
            for row in rows:
                proposal_id = "wp_" + _digest(row["followup_id"])[:32]
                db.execute("INSERT OR IGNORE INTO a5_wb_followup_proposals VALUES(?,?,'awaiting_owner','','',?)",
                           (proposal_id, row["followup_id"], now))
                db.execute("UPDATE a5_wb_followups SET state='proposed' WHERE followup_id=?", (row["followup_id"],))
            return [dict(row) for row in db.execute("""SELECT p.*,f.task_id,f.recipient_id,f.prompt,
                f.source_ref,f.due_at FROM a5_wb_followup_proposals p
                JOIN a5_wb_followups f USING(followup_id)
                JOIN a5_wb_tasks t USING(task_id)
                WHERE p.state='awaiting_owner' AND f.state='proposed'
                  AND t.state NOT IN ('completed','cancelled','awaiting_owner')
                ORDER BY f.due_at,p.proposal_id LIMIT ?""", (limit,))]

    def record_followup_delivery(self, proposal_id, *, principal_id, approval_ref, delivery_ref):
        self._owner(principal_id)
        _required(approval_ref, "approval_ref")
        _required(delivery_ref, "delivery_ref")
        with self._transaction() as db:
            proposal = db.execute("SELECT * FROM a5_wb_followup_proposals WHERE proposal_id=?", (proposal_id,)).fetchone()
            if proposal is None:
                raise WorkbookError("unknown follow-up proposal")
            if proposal["state"] == "sent":
                if (proposal["approval_ref"], proposal["delivery_ref"]) != (approval_ref, delivery_ref):
                    raise IdentityConflict("delivery receipt differs")
                return dict(proposal)
            if proposal["state"] != "awaiting_owner":
                raise WorkbookError("follow-up proposal is no longer active")
            db.execute("UPDATE a5_wb_followup_proposals SET state='sent',approval_ref=?,delivery_ref=? WHERE proposal_id=?",
                       (approval_ref, delivery_ref, proposal_id))
            db.execute("UPDATE a5_wb_followups SET state='waiting_feedback' WHERE followup_id=?", (proposal["followup_id"],))
            return dict(db.execute("SELECT * FROM a5_wb_followup_proposals WHERE proposal_id=?", (proposal_id,)).fetchone())

    def record_reply(self, task_id, event_key, origin, actor_id, text, *, source_ref,
                     claims_complete=False, followup_id=None, at=None):
        _required(source_ref, "source_ref")
        if not isinstance(text, str) or not isinstance(claims_complete, bool):
            raise WorkbookError("invalid reply")
        at = _timestamp(self.clock() if at is None else at, "at")
        with self._transaction() as db:
            principal_id = self._resolve(db, origin, actor_id)["principal_id"]
            try:
                task = self._task(db, task_id, principal_id)
            except AccessDenied:
                # Being asked for feedback permits replying to that request, not
                # reading the task's private notes or its other conversations.
                if not db.execute("SELECT 1 FROM a5_wb_followups WHERE task_id=? AND recipient_id=?",
                                  (task_id, principal_id)).fetchone():
                    raise
                task = dict(db.execute("SELECT * FROM a5_wb_tasks WHERE task_id=?", (task_id,)).fetchone())
            if followup_id and not db.execute("""SELECT 1 FROM a5_wb_followups
                    WHERE followup_id=? AND task_id=? AND recipient_id=?""",
                    (followup_id, task_id, principal_id)).fetchone():
                raise AccessDenied("reply does not match the follow-up")
            event, fresh = self._add_event(db, event_key=self._event_key(origin, actor_id, event_key),
                fingerprint=_digest([task_id, text, source_ref, claims_complete, followup_id]),
                task_id=task_id, principal_id=principal_id, origin=origin, actor_id=actor_id,
                kind="reported_complete" if claims_complete else "feedback", text=text,
                source_ref=source_ref, at=at)
            if not fresh:
                return event
            parameters = [task_id, principal_id, at]
            filter_sql = "task_id=? AND recipient_id=? AND created_at<=? AND state IN ('scheduled','proposed','waiting_feedback')"
            if followup_id:
                filter_sql += " AND followup_id=?"
                parameters.append(followup_id)
            followups = db.execute("SELECT followup_id FROM a5_wb_followups WHERE " + filter_sql, parameters).fetchall()
            for followup in followups:
                fid = followup["followup_id"]
                db.execute("UPDATE a5_wb_followups SET state='resolved',resolved_by_event=? WHERE followup_id=?",
                           (event["event_id"], fid))
                db.execute("UPDATE a5_wb_followup_proposals SET state='resolved' WHERE followup_id=?", (fid,))
            if claims_complete and task["state"] not in TERMINAL_STATES and task["related"] and at >= task["updated_at"]:
                db.execute("UPDATE a5_wb_tasks SET state='reported_complete',updated_at=max(updated_at,?) WHERE task_id=?",
                           (at, task_id))
            else:
                db.execute("UPDATE a5_wb_tasks SET updated_at=max(updated_at,?) WHERE task_id=?", (at, task_id))
            return event

    @staticmethod
    def _cancel_followups(db, task_id):
        db.execute("""UPDATE a5_wb_followup_proposals SET state='cancelled'
            WHERE followup_id IN (SELECT followup_id FROM a5_wb_followups WHERE task_id=?)
              AND state='awaiting_owner'""", (task_id,))
        db.execute("""UPDATE a5_wb_followups SET state='cancelled'
            WHERE task_id=? AND state IN ('scheduled','proposed','waiting_feedback')""", (task_id,))

    def tasks_for_principal(self, principal_id, *, project_id=None, include_terminal=False, limit=100,
                            project_ids=None, project_only=False):
        _required(principal_id, "principal_id")
        if not isinstance(limit, int) or isinstance(limit, bool) or not 1 <= limit <= 1000:
            raise WorkbookError("invalid limit")
        if not isinstance(project_only, bool) or isinstance(project_ids, (str, bytes)):
            raise WorkbookError("invalid audience project scope")
        if project_ids is not None:
            project_ids = tuple(dict.fromkeys(_required(p, "project_id") for p in project_ids))
        with closing(self._connect()) as db:
            clauses, args = [], []
            if not include_terminal:
                clauses.append("t.state NOT IN ('completed','cancelled')")
            if project_id is not None:
                clauses.append("t.project_id=?")
                args.append(project_id)
            if project_ids is not None:
                clauses.append("t.project_id IN (" + ",".join("?" for _ in project_ids) + ")"
                               if project_ids else "0")
                args.extend(project_ids)
            if project_only:
                clauses.append("t.visibility='project' AND t.project_id IS NOT NULL")
            if principal_id != self.owner_id:
                clauses.append("""(t.requester_id=? OR (t.visibility='project' AND EXISTS
                    (SELECT 1 FROM a5_wb_project_grants g WHERE g.principal_id=? AND g.project_id=t.project_id)))""")
                args.extend((principal_id, principal_id))
                clauses.append("""(t.project_id IS NULL OR t.related=0 OR EXISTS
                    (SELECT 1 FROM a5_wb_project_grants g WHERE g.principal_id=? AND g.project_id=t.project_id))""")
                args.append(principal_id)
            where = " WHERE " + " AND ".join(clauses) if clauses else ""
            args.append(limit)
            return [dict(row) for row in db.execute("SELECT t.* FROM a5_wb_tasks t" + where +
                " ORDER BY priority,coalesce(due_at,1e99),created_at,task_id LIMIT ?", args)]

    def project_brief(self, project_id, *, principal_id, limit=20):
        """A factual view. It never claims external ledgers were written."""
        if not isinstance(limit, int) or isinstance(limit, bool) or not 1 <= limit <= 100:
            raise WorkbookError("invalid limit")
        with closing(self._connect()) as db:
            self._project_access(db, project_id, principal_id)
            rows = db.execute("""SELECT * FROM a5_wb_facts f WHERE project_id=?
                AND NOT EXISTS(SELECT 1 FROM a5_wb_facts n WHERE n.supersedes_fact_id=f.fact_id)
                ORDER BY observed_at DESC,recorded_at DESC,fact_id""", (project_id,)).fetchall()
            facts = [self._fact(row) for row in rows]
        # Latest fact per field and state: a reported update does not erase a plan
        # or previously verified fact, and explicit conflicts stay visible.
        selected, seen = [], set()
        for fact in facts:
            identity = (fact["field"], fact["status"])
            if identity not in seen or fact["status"] == "conflict":
                selected.append(fact)
                seen.add(identity)
        tasks = self.tasks_for_principal(principal_id, project_id=project_id, limit=limit)
        grouped = {category: [f for f in selected if f["category"] == category][:limit]
                   for category in FACT_CATEGORIES}
        return {"project_id": project_id, "as_of": self.clock(), "plans": grouped["plan"],
                "progress": grouped["progress"], "blockers": grouped["blocker"],
                "risks": grouped["risk"], "todos": tasks,
                "recorded_todos": grouped["todo"],
                "pending_review": [f for f in selected if f["status"] == "conflict"][:limit],
                "pending_owner_tasks": [t for t in tasks if t["state"] == "awaiting_owner"],
                "reported_complete_tasks": [t for t in tasks if t["state"] == "reported_complete"],
                "external_write_performed": False}

    def owner_brief(self, *, principal_id, limit=100):
        self._owner(principal_id)
        tasks = self.tasks_for_principal(principal_id, limit=limit)
        now = self.clock()
        with closing(self._connect()) as db:
            counts = {row["state"]: row["count"] for row in db.execute(
                "SELECT state,count(*) AS count FROM a5_wb_tasks GROUP BY state")}
        return {"as_of": now, "tasks": tasks, "counts": counts,
                "needs_owner": [t for t in tasks if t["state"] == "awaiting_owner"],
                "blocked": [t for t in tasks if t["state"] == "blocked"],
                "overdue": [t for t in tasks if t["due_at"] is not None and t["due_at"] < now],
                "reported_complete": [t for t in tasks if t["state"] == "reported_complete"],
                "external_write_performed": False}
