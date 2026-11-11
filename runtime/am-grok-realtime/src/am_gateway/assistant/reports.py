"""Durable owner reports from authenticated work conversations.

This module never sends messages, runs a model, or interprets message text as an
instruction.  The caller supplies authenticated identities, real timestamps,
source references and (optionally) independently verified completion evidence.
An ended conversation is deliberately not a completed task.
"""
from __future__ import annotations

from contextlib import closing, contextmanager
from datetime import date, datetime, time as daytime, timedelta
import hashlib
import hmac
import json
from pathlib import Path
import sqlite3
import time
from typing import Callable
import uuid
from zoneinfo import ZoneInfo

from ..models import ContractError


_SCHEMA = """
CREATE TABLE IF NOT EXISTS ar5_sessions(
 session_id TEXT PRIMARY KEY, owner_id TEXT NOT NULL, scope_key TEXT NOT NULL,
 conversation_key TEXT NOT NULL, thread_id TEXT NOT NULL, participant_scope TEXT NOT NULL,
 work_scope TEXT NOT NULL, ordinal INTEGER NOT NULL, state TEXT NOT NULL,
 started_at REAL NOT NULL, last_at REAL NOT NULL, closed_at REAL,
 close_reason TEXT NOT NULL DEFAULT '', report_id TEXT NOT NULL DEFAULT '',
 UNIQUE(owner_id,scope_key,ordinal)
);
CREATE UNIQUE INDEX IF NOT EXISTS ar5_one_open_session
 ON ar5_sessions(owner_id,scope_key) WHERE state='open';
CREATE INDEX IF NOT EXISTS ar5_idle ON ar5_sessions(owner_id,state,last_at);
CREATE TABLE IF NOT EXISTS ar5_turns(
 owner_id TEXT NOT NULL, event_id TEXT NOT NULL, session_id TEXT NOT NULL,
 payload_json TEXT NOT NULL, payload_hash TEXT NOT NULL, timestamp REAL NOT NULL,
 PRIMARY KEY(owner_id,event_id)
);
CREATE INDEX IF NOT EXISTS ar5_turn_session ON ar5_turns(session_id,timestamp);
CREATE TABLE IF NOT EXISTS ar5_outbox(
 report_id TEXT PRIMARY KEY, owner_id TEXT NOT NULL, idempotency_key TEXT NOT NULL UNIQUE,
 kind TEXT NOT NULL, payload_json TEXT NOT NULL, body TEXT NOT NULL,
 body_sha256 TEXT NOT NULL, state TEXT NOT NULL, priority TEXT NOT NULL,
 created_at REAL NOT NULL, not_before REAL NOT NULL,
 attempt_no INTEGER NOT NULL DEFAULT 0, lease_token TEXT NOT NULL DEFAULT '',
 worker_id TEXT NOT NULL DEFAULT '', lease_until REAL, sent_at REAL,
 delivery_ref TEXT NOT NULL DEFAULT '', last_error TEXT NOT NULL DEFAULT '',
 resolution_json TEXT NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS ar5_ready ON ar5_outbox(owner_id,state,not_before,created_at);
CREATE TABLE IF NOT EXISTS ar5_attempts(
 report_id TEXT NOT NULL, attempt_no INTEGER NOT NULL, lease_token TEXT NOT NULL,
 worker_id TEXT NOT NULL, started_at REAL NOT NULL, lease_until REAL NOT NULL,
 state TEXT NOT NULL, delivery_ref TEXT NOT NULL DEFAULT '',
 evidence_ref TEXT NOT NULL DEFAULT '', last_error TEXT NOT NULL DEFAULT '',
 PRIMARY KEY(report_id,attempt_no)
);
CREATE TABLE IF NOT EXISTS ar5_digest_slots(
 owner_id TEXT NOT NULL, slot_at REAL NOT NULL, report_id TEXT NOT NULL,
 generated_at REAL NOT NULL, PRIMARY KEY(owner_id,slot_at)
);
"""


def _json(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _sha(value):
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _stamp(value):
    if isinstance(value, datetime):
        if value.tzinfo is None:
            raise ContractError("report timestamps require an explicit timezone")
        value = value.timestamp()
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ContractError("a real numeric timestamp or aware datetime is required")
    result = float(value)
    if not (0 <= result < 253402300800):
        raise ContractError("invalid report timestamp")
    return result


def _unique(values):
    return list({_json(value): value for value in values if value not in (None, "", {})}.values())


class AssistantReports:
    """One owner's persistent reporting ledger and transport-independent outbox.

    ``workday_predicate`` may supply the company's real workday/holiday calendar.
    Without it, Monday--Friday are workdays.  Quiet hours are 18:00--09:00 in the
    configured timezone.  P0 reports bypass quiet hours; every other report is
    held until work hours.  All source turns require an explicit ``timestamp``.
    """

    def __init__(self, sqlite_path, owner_id, owner_chat_id, idle_seconds=600,
                 timezone="Asia/Shanghai", *, workday_predicate: Callable[[date], bool] | None = None,
                 clock=time.time):
        if not owner_id or not owner_chat_id or idle_seconds <= 0:
            raise ContractError("owner, report destination and positive idle duration are required")
        if str(sqlite_path) == ":memory:":
            raise ContractError("reports require a persistent SQLite path")
        self.path = str(Path(sqlite_path).resolve())
        self.owner_id, self.owner_chat_id = str(owner_id), str(owner_chat_id)
        self.idle_seconds, self.tz, self.clock = float(idle_seconds), ZoneInfo(timezone), clock
        self.workday = workday_predicate or (lambda day: day.weekday() < 5)
        Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        with closing(self._connect()) as db:
            db.execute("PRAGMA journal_mode=WAL")
            db.executescript(_SCHEMA)

    def _connect(self):
        db = sqlite3.connect(self.path, timeout=10, isolation_level=None)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA busy_timeout=10000")
        db.execute("PRAGMA synchronous=FULL")
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

    def _now(self, value):
        return _stamp(self.clock() if value is None else value)

    def _check_owner(self, actor_id):
        if actor_id is not None and actor_id != self.owner_id:
            raise ContractError("only the bound owner may inspect or resolve reports")

    def _next_work_time(self, timestamp):
        local = datetime.fromtimestamp(timestamp, self.tz)
        if self.workday(local.date()) and 9 <= local.hour < 18:
            return timestamp
        day = local.date() if local.hour < 9 else local.date() + timedelta(days=1)
        for _ in range(370):
            if self.workday(day):
                return datetime.combine(day, daytime(9), self.tz).timestamp()
            day += timedelta(days=1)
        raise ContractError("workday calendar has no working day in the next year")

    def _previous_slot(self, slot):
        local = datetime.fromtimestamp(slot, self.tz)
        if local.hour == 14:
            return datetime.combine(local.date(), daytime(9), self.tz).timestamp()
        day = local.date() - timedelta(days=1)
        for _ in range(370):
            if self.workday(day):
                return datetime.combine(day, daytime(14), self.tz).timestamp()
            day -= timedelta(days=1)
        raise ContractError("workday calendar has no previous working day")

    @staticmethod
    def _priority(priority):
        if priority not in {"P0", "P1", "P2", "P3"}:
            raise ContractError("report priority must be P0, P1, P2 or P3")
        return priority

    def _session(self, db, *, scope_key, conversation_key, thread_id,
                 participant_scope, work_scope, timestamp):
        row = db.execute("SELECT * FROM ar5_sessions WHERE owner_id=? AND scope_key=? AND state='open'",
                         (self.owner_id, scope_key)).fetchone()
        if row and timestamp - row["last_at"] >= self.idle_seconds:
            self._close(db, row, closed_at=row["last_at"] + self.idle_seconds,
                        generated_at=timestamp, reason="idle")
            row = None
        if row:
            return row
        ordinal = db.execute("SELECT COALESCE(MAX(ordinal),0)+1 FROM ar5_sessions WHERE owner_id=? AND scope_key=?",
                             (self.owner_id, scope_key)).fetchone()[0]
        session_id = "ars_" + _sha(_json([self.owner_id, scope_key, ordinal]))[:32]
        db.execute("""INSERT INTO ar5_sessions(session_id,owner_id,scope_key,conversation_key,
            thread_id,participant_scope,work_scope,ordinal,state,started_at,last_at)
            VALUES(?,?,?,?,?,?,?,?,'open',?,?)""",
                   (session_id, self.owner_id, scope_key, conversation_key, thread_id,
                    _json(participant_scope), work_scope, ordinal, timestamp, timestamp))
        return db.execute("SELECT * FROM ar5_sessions WHERE session_id=?", (session_id,)).fetchone()

    def record_turn(self, event_id, conversation_key, *, timestamp, actor_id, text,
                    actor_name="", speaker_role="human", actor_type="human",
                    authenticated=True, is_real=True, is_work=True, directed_to_am=True,
                    thread_id="", participant_ids=None, work_scope="", task_id="",
                    source_ref=None, reply_to_event_id="", delivery_status="",
                    delivery_ref="", reply_key="", details=None, priority="P2"):
        """Record one real work turn; duplicates are immutable and idempotent.

        Human input opens a segment only when it is authenticated, addressed to
        AM, work-related and not from the owner.  Generic bot/system history is
        ignored.  An AM turn must bind to a previously admitted human event and
        be either actually ``sent`` (with a delivery reference) or explicitly
        ``pending_send``.  Pending drafts never become recorded commitments.

        ``details`` is optional structured data from the trusted coordinator.
        ``completed`` entries require ``verified=True`` and ``evidence_ref``;
        raw claims of completion are only quoted as a colleague's statements.
        The module never extracts authority or executable actions from text.
        """
        timestamp = _stamp(timestamp)
        priority = self._priority(priority)
        if not event_id or not conversation_key or not actor_id or not isinstance(text, str):
            raise ContractError("event identity, conversation, actor and text are required")
        role = "human" if speaker_role in {"human", "user", "colleague"} else speaker_role
        if not is_real or not authenticated or not is_work or not directed_to_am:
            return {"status": "ignored", "reason": "not_authenticated_work_conversation"}
        if role == "human" and (actor_id == self.owner_id or actor_type != "human"):
            return {"status": "ignored", "reason": "owner_or_bot_turn"}
        if role not in {"human", "assistant"}:
            return {"status": "ignored", "reason": "system_or_unbound_bot_turn"}
        if role == "assistant" and (not reply_to_event_id or delivery_status not in {"sent", "pending_send"}):
            return {"status": "ignored", "reason": "unbound_or_undelivered_assistant_turn"}
        if role == "assistant" and delivery_status == "sent" and not delivery_ref:
            raise ContractError("sent AM replies require a real delivery reference")
        if details is not None and not isinstance(details, dict):
            raise ContractError("report details must be structured data")
        participants = sorted(set(str(p) for p in (participant_ids or []) if p != self.owner_id))
        if role == "human" and participants and str(actor_id) not in participants:
            raise ContractError("speaker is outside the supplied participant scope")
        if not participants and not thread_id and role == "human":
            participants = [str(actor_id)]
        payload = {"event_id": str(event_id), "conversation_key": str(conversation_key),
                   "timestamp": timestamp, "actor_id": str(actor_id), "actor_name": str(actor_name),
                   "speaker_role": role, "text": text, "thread_id": str(thread_id),
                   "participant_ids": participants, "work_scope": str(work_scope), "task_id": str(task_id),
                   "source_ref": source_ref, "reply_to_event_id": str(reply_to_event_id),
                   "delivery_status": delivery_status, "delivery_ref": str(delivery_ref),
                   "reply_key": reply_key or _sha(_json([reply_to_event_id, text])),
                   "details": details or {}, "priority": priority}
        encoded = _json(payload)
        with self._transaction() as db:
            old = db.execute("SELECT * FROM ar5_turns WHERE owner_id=? AND event_id=?",
                             (self.owner_id, str(event_id))).fetchone()
            if old:
                if old["payload_hash"] != _sha(encoded):
                    raise ContractError("report event identity already has different content")
                return {"status": "recorded", "event_id": str(event_id),
                        "session_id": old["session_id"], "duplicate": True}
            if role == "assistant":
                origin = db.execute("SELECT * FROM ar5_turns WHERE owner_id=? AND event_id=?",
                                    (self.owner_id, reply_to_event_id)).fetchone()
                if not origin:
                    return {"status": "ignored", "reason": "missing_authenticated_human_origin"}
                original = json.loads(origin["payload_json"])
                if original["speaker_role"] != "human" or original["conversation_key"] != conversation_key:
                    raise ContractError("AM reply is outside its authenticated human conversation")
                prior = db.execute("SELECT * FROM ar5_sessions WHERE session_id=?", (origin["session_id"],)).fetchone()
                if thread_id and thread_id != prior["thread_id"]:
                    raise ContractError("AM reply thread does not match its source")
                scope_key = prior["scope_key"]
                thread_id, participants, work_scope = prior["thread_id"], json.loads(prior["participant_scope"]), prior["work_scope"]
            else:
                scope_key = _sha(_json([self.owner_id, conversation_key, thread_id, participants, work_scope]))
            segment = self._session(db, scope_key=scope_key, conversation_key=conversation_key,
                                    thread_id=thread_id, participant_scope=participants,
                                    work_scope=work_scope, timestamp=timestamp)
            db.execute("INSERT INTO ar5_turns VALUES(?,?,?,?,?,?)",
                       (self.owner_id, str(event_id), segment["session_id"], encoded, _sha(encoded), timestamp))
            db.execute("UPDATE ar5_sessions SET started_at=MIN(started_at,?),last_at=MAX(last_at,?) WHERE session_id=?",
                       (timestamp, timestamp, segment["session_id"]))
            return {"status": "recorded", "event_id": str(event_id),
                    "session_id": segment["session_id"], "duplicate": False}

    def _enqueue(self, db, *, kind, key, payload, body, priority, created_at):
        identity = "am-r5-report:" + _sha(_json([self.owner_id, kind, key]))
        report_id = "arp_" + _sha(identity)[:32]
        payload = dict(payload, target={"chat_id": self.owner_chat_id, "actor_id": self.owner_id,
                                       "channel": "bot_dm"})
        db.execute("""INSERT OR IGNORE INTO ar5_outbox(report_id,owner_id,idempotency_key,kind,
            payload_json,body,body_sha256,state,priority,created_at,not_before)
            VALUES(?,?,?,?,?,?,?,'ready',?,?,?)""",
                   (report_id, self.owner_id, identity, kind, _json(payload), body, _sha(body), priority,
                    created_at, created_at if priority == "P0" else self._next_work_time(created_at)))
        return self._read_report(db, report_id)

    @staticmethod
    def _body(title, sections, refs):
        labels = (("requested", "谁提出了什么"), ("promised", "AM 已送达回复／承诺"),
                  ("completed", "已核实完成"), ("pending", "未完成／待发送"),
                  ("needs_owner", "需要示例负责人判断"))
        lines = [title]
        for key, label in labels:
            lines.extend(["", label + "："])
            lines.extend("- " + str(item) for item in sections[key])
        lines.extend(["", "来源："])
        lines.extend("- " + (ref if isinstance(ref, str) else _json(ref)) for ref in refs)
        if not refs:
            lines.append("- 本段暂无可回查来源。")
        lines.extend(["", "以上引文仅是沟通记录；本段对话结束不代表关联任务完成。"])
        return "\n".join(lines)

    def _close(self, db, session, *, closed_at, generated_at, reason):
        if session["state"] == "closed":
            return self._read_report(db, session["report_id"]) if session["report_id"] else None
        rows = db.execute("SELECT payload_json FROM ar5_turns WHERE owner_id=? AND session_id=? ORDER BY timestamp,event_id",
                          (self.owner_id, session["session_id"])).fetchall()
        turns = [json.loads(row[0]) for row in rows]
        if not turns:
            return None
        sections = {k: [] for k in ("requested", "promised", "completed", "pending", "needs_owner")}
        refs, people, task_ids = [], {}, []
        sent_reply_keys = {t["reply_key"] for t in turns if t["speaker_role"] == "assistant" and t["delivery_status"] == "sent"}
        for turn in turns:
            if turn["source_ref"]:
                refs.append(turn["source_ref"])
            if turn["task_id"]:
                task_ids.append(turn["task_id"])
            if turn["speaker_role"] == "human":
                name = turn["actor_name"] or turn["actor_id"]
                people[turn["actor_id"]] = name
                sections["requested"].append(name + "（对方原话）：" + turn["text"])
            elif turn["delivery_status"] == "sent":
                sections["promised"].append("AM 已送达原文：" + turn["text"])
                if not turn["source_ref"]:
                    refs.append({"delivery_ref": turn["delivery_ref"]})
            elif turn["reply_key"] not in sent_reply_keys:
                sections["pending"].append("AM 待发送草稿（尚未对外承诺）：" + turn["text"])
            if turn["speaker_role"] == "assistant":
                origin = db.execute("SELECT payload_json FROM ar5_turns WHERE owner_id=? AND event_id=?",
                                    (self.owner_id, turn["reply_to_event_id"])).fetchone()
                if origin:
                    human = json.loads(origin[0])
                    people[human["actor_id"]] = human["actor_name"] or human["actor_id"]
                    if human["source_ref"]:
                        refs.append(human["source_ref"])
            detail = turn["details"]
            for key in ("pending", "needs_owner"):
                if isinstance(detail.get(key), list):
                    sections[key].extend(str(item) for item in detail[key] if isinstance(item, str))
            if isinstance(detail.get("completed"), list):
                for item in detail["completed"]:
                    if isinstance(item, dict) and item.get("verified") is True and item.get("evidence_ref") and item.get("text"):
                        sections["completed"].append(str(item["text"]))
                        refs.append(item["evidence_ref"])
        defaults = {"requested": "本段为已登记工作对话的增量续报；请见原请求来源。",
                    "promised": "本段没有已送达的 AM 回复记录。",
                    "completed": "本段没有独立核实的完成证据。",
                    "pending": "未提供已核实的未完成清单；关联任务状态不因本次汇报改变。",
                    "needs_owner": "本段没有登记需要本人判断的事项。"}
        sections = {key: _unique(values) or [defaults[key]] for key, values in sections.items()}
        refs = _unique(refs)
        title = "AM 工作对话" + ("续报" if session["ordinal"] > 1 else "汇报")
        payload = {"session_id": session["session_id"], "conversation_key": session["conversation_key"],
                   "thread_id": session["thread_id"], "work_scope": session["work_scope"],
                   "ordinal": session["ordinal"], "participants": people,
                   "event_ids": [t["event_id"] for t in turns], "task_ids": _unique(task_ids),
                   "started_at": session["started_at"], "closed_at": closed_at,
                   "close_reason": reason, "task_completion_inferred": False,
                   "sections": sections, "source_refs": refs}
        report = self._enqueue(db, kind="conversation", key=session["session_id"], payload=payload,
                               body=self._body(title, sections, refs),
                               priority=min((t["priority"] for t in turns)), created_at=generated_at)
        db.execute("UPDATE ar5_sessions SET state='closed',closed_at=?,close_reason=?,report_id=? WHERE session_id=?",
                   (closed_at, reason, report["report_id"], session["session_id"]))
        return report

    def close_conversation(self, conversation_key, *, closed_at=None, thread_id=None,
                           actor_id=None, session_id=None):
        """Close matching segments independently; never merge group participants."""
        now = self._now(closed_at)
        with self._transaction() as db:
            query = "SELECT * FROM ar5_sessions WHERE owner_id=? AND conversation_key=? AND state='open'"
            args = [self.owner_id, conversation_key]
            for field, value in (("thread_id", thread_id), ("session_id", session_id)):
                if value is not None:
                    query += " AND " + field + "=?"
                    args.append(value)
            rows = db.execute(query + " ORDER BY started_at,session_id", args).fetchall()
            result = []
            for row in rows:
                if actor_id is not None:
                    actors = []
                    for saved in db.execute("SELECT payload_json FROM ar5_turns WHERE session_id=?", (row["session_id"],)):
                        turn = json.loads(saved[0])
                        if turn["speaker_role"] == "human":
                            actors.append(turn["actor_id"])
                        elif turn["reply_to_event_id"]:
                            origin = db.execute("SELECT payload_json FROM ar5_turns WHERE owner_id=? AND event_id=?",
                                                (self.owner_id, turn["reply_to_event_id"])).fetchone()
                            if origin:
                                actors.append(json.loads(origin[0])["actor_id"])
                    if actor_id not in actors and actor_id not in json.loads(row["participant_scope"]):
                        continue
                if now < row["last_at"]:
                    raise ContractError("conversation cannot close before its latest real turn")
                report = self._close(db, row, closed_at=now, generated_at=now, reason="explicit")
                if report:
                    result.append(report)
            return result

    def sweep_idle(self, *, now=None):
        now = self._now(now)
        with self._transaction() as db:
            rows = db.execute("SELECT * FROM ar5_sessions WHERE owner_id=? AND state='open' AND last_at<=? ORDER BY last_at",
                              (self.owner_id, now - self.idle_seconds)).fetchall()
            return [self._close(db, row, closed_at=row["last_at"] + self.idle_seconds,
                                generated_at=now, reason="idle") for row in rows]

    def enqueue_risk(self, risk_id, *, timestamp, text, source_ref=None, priority="P0",
                     task_id="", needs_owner="", details=None):
        """Persist one trusted coordinator risk event, once per stable risk ID."""
        timestamp, priority = _stamp(timestamp), self._priority(priority)
        if not risk_id or not isinstance(text, str) or not text.strip():
            raise ContractError("risk identity and text are required")
        payload = {"risk_id": str(risk_id), "task_ids": [str(task_id)] if task_id else [],
                   "text": text, "needs_owner": needs_owner, "details": details or {},
                   "source_refs": [source_ref] if source_ref else [], "timestamp": timestamp}
        body = "AM 风险汇报 · " + priority + "\n\n" + text
        if needs_owner:
            body += "\n\n需要本人判断：" + str(needs_owner)
        if source_ref:
            body += "\n\n来源：" + (source_ref if isinstance(source_ref, str) else _json(source_ref))
        with self._transaction() as db:
            identity = "am-r5-report:" + _sha(_json([self.owner_id, "risk", str(risk_id)]))
            existing = db.execute("SELECT * FROM ar5_outbox WHERE idempotency_key=?", (identity,)).fetchone()
            if existing:
                old = json.loads(existing["payload_json"])
                comparable = {key: old[key] for key in payload}
                if comparable != payload or existing["priority"] != priority:
                    raise ContractError("risk identity already has different content")
                return self._read_report(db, existing["report_id"])
            return self._enqueue(db, kind="risk", key=str(risk_id), payload=payload,
                                 body=body, priority=priority, created_at=timestamp)

    def enqueue_notice(self, notice_id, *, title, body, source_refs=(), task_ids=(),
                       now, priority="P1", metadata=None):
        """Queue a frozen approval/referral notice separately from conversation reports.

        ``metadata`` is stored as data for the dispatcher (for example an action
        ID, frozen-content digest, and card buttons).  No metadata is executed.
        Reusing a notice ID with changed targets or content is a contract error.
        """
        now, priority = _stamp(now), self._priority(priority)
        if not notice_id or not isinstance(title, str) or not isinstance(body, str) or not body.strip():
            raise ContractError("notice identity, title and body are required")
        if metadata is not None and not isinstance(metadata, dict):
            raise ContractError("notice metadata must be structured data")
        metadata = metadata or {}
        kind = metadata.get("kind", "notice")
        if kind not in {"approval", "referral", "notice"}:
            raise ContractError("unsupported owner notice kind")
        payload = {"notice_id": str(notice_id), "title": title, "metadata": metadata,
                   "source_refs": _unique(list(source_refs)), "task_ids": _unique(list(task_ids))}
        rendered = title + "\n\n" + body if title else body
        identity = "am-r5-report:" + _sha(_json([self.owner_id, kind, str(notice_id)]))
        with self._transaction() as db:
            old = db.execute("SELECT * FROM ar5_outbox WHERE idempotency_key=?", (identity,)).fetchone()
            if old:
                original = json.loads(old["payload_json"])
                if (any(original.get(key) != value for key, value in payload.items())
                        or old["body"] != rendered or old["priority"] != priority
                        or original["target"]["chat_id"] != self.owner_chat_id):
                    raise ContractError("notice identity already has different frozen content")
                return self._read_report(db, old["report_id"])
            return self._enqueue(db, kind=kind, key=str(notice_id), payload=payload,
                                 body=rendered, priority=priority, created_at=now)

    def enqueue_due_digests(self, *, now=None):
        """Create one digest per due workday 09:00/14:00 slot, including sent reports."""
        now = self._now(now)
        with self._transaction() as db:
            first = db.execute("SELECT MIN(created_at) FROM ar5_outbox WHERE owner_id=? AND kind IN ('conversation','risk')",
                               (self.owner_id,)).fetchone()[0]
            if first is None or first > now:
                return []
            previous = db.execute("SELECT MAX(slot_at) FROM ar5_digest_slots WHERE owner_id=?",
                                  (self.owner_id,)).fetchone()[0]
            cursor = max(first, previous) if previous is not None else first
            day, last_day = datetime.fromtimestamp(cursor, self.tz).date(), datetime.fromtimestamp(now, self.tz).date()
            result = []
            while day <= last_day:
                if self.workday(day):
                    for hour in (9, 14):
                        slot = datetime.combine(day, daytime(hour), self.tz).timestamp()
                        if slot < first or slot > now or db.execute(
                                "SELECT 1 FROM ar5_digest_slots WHERE owner_id=? AND slot_at=?",
                                (self.owner_id, slot)).fetchone():
                            continue
                        since = self._previous_slot(slot)
                        reports = db.execute("""SELECT * FROM ar5_outbox WHERE owner_id=?
                            AND kind IN ('conversation','risk') AND created_at>? AND created_at<=?
                            ORDER BY created_at,report_id""", (self.owner_id, since, slot)).fetchall()
                        report_id = ""
                        if reports:
                            title = "AM 工作沟通汇总 · " + datetime.fromtimestamp(slot, self.tz).strftime("%Y-%m-%d %H:%M")
                            items, refs, lines = [], [], [title]
                            for row in reports:
                                payload = json.loads(row["payload_json"])
                                refs.extend(payload.get("source_refs", []))
                                item = {"report_id": row["report_id"], "kind": row["kind"],
                                        "delivery_state_at_summary": row["state"],
                                        "participants": payload.get("participants", {}),
                                        "task_ids": payload.get("task_ids", []),
                                        "sections": payload.get("sections", {}), "text": payload.get("text", "")}
                                items.append(item)
                                lines.extend(["", "工作对话" if row["kind"] == "conversation" else "风险事项"])
                                if item["sections"]:
                                    for key, label in (("requested", "请求／反馈"), ("promised", "已回复"),
                                                       ("completed", "已核实完成"), ("pending", "未完成"),
                                                       ("needs_owner", "需判断")):
                                        lines.append(label + "：" + "；".join(item["sections"].get(key, [])))
                                else:
                                    lines.append(item["text"])
                                lines.append("记录：" + row["report_id"])
                            digest = self._enqueue(db, kind="digest", key=str(int(slot)),
                                payload={"slot_at": slot, "window_start": since, "window_end": slot,
                                         "items": items, "source_refs": _unique(refs)},
                                body="\n".join(lines), priority="P2", created_at=now)
                            report_id = digest["report_id"]
                            result.append(digest)
                        db.execute("INSERT INTO ar5_digest_slots VALUES(?,?,?,?)",
                                   (self.owner_id, slot, report_id, now))
                day += timedelta(days=1)
            return result

    def _read_report(self, db, report_id, *, include_lease=False):
        row = db.execute("SELECT * FROM ar5_outbox WHERE owner_id=? AND report_id=?",
                         (self.owner_id, report_id)).fetchone()
        if not row:
            raise ContractError("unknown owner report")
        result = dict(row)
        result["status"] = result["state"]
        result["payload"] = json.loads(result.pop("payload_json"))
        result["resolution"] = json.loads(result.pop("resolution_json"))
        result["target"] = result["payload"]["target"]
        if not include_lease:
            result.pop("lease_token", None)
        return result

    def get_report(self, report_id, *, actor_id=None):
        self._check_owner(actor_id)
        with closing(self._connect()) as db:
            return self._read_report(db, report_id)

    def list_reports(self, *, actor_id=None, status=None, kind=None, limit=100, offset=0):
        self._check_owner(actor_id)
        if not 1 <= limit <= 1000 or offset < 0:
            raise ContractError("invalid report pagination")
        query, args = "SELECT report_id FROM ar5_outbox WHERE owner_id=?", [self.owner_id]
        for key, value in (("state", status), ("kind", kind)):
            if value is not None:
                query += " AND " + key + "=?"
                args.append(value)
        with closing(self._connect()) as db:
            return [self._read_report(db, row[0]) for row in db.execute(
                query + " ORDER BY created_at,report_id LIMIT ? OFFSET ?", args + [limit, offset])]

    def _expire(self, db, now):
        rows = db.execute("SELECT * FROM ar5_outbox WHERE owner_id=? AND state='leased' AND lease_until<=?",
                          (self.owner_id, now)).fetchall()
        for row in rows:
            db.execute("UPDATE ar5_outbox SET state='unknown',lease_until=NULL,last_error='lease_expired' WHERE report_id=?",
                       (row["report_id"],))
            db.execute("UPDATE ar5_attempts SET state='unknown',last_error='lease_expired' WHERE report_id=? AND attempt_no=?",
                       (row["report_id"], row["attempt_no"]))
        return [row["report_id"] for row in rows]

    def expire_leases(self, *, now=None):
        with self._transaction() as db:
            return self._expire(db, self._now(now))

    def claim(self, worker_id, *, now=None, lease_seconds=60):
        now = self._now(now)
        if not worker_id or lease_seconds <= 0:
            raise ContractError("worker identity and positive lease duration are required")
        with self._transaction() as db:
            self._expire(db, now)
            row = db.execute("""SELECT * FROM ar5_outbox WHERE owner_id=? AND state='ready'
                AND not_before<=? ORDER BY priority,created_at,report_id LIMIT 1""", (self.owner_id, now)).fetchone()
            if not row:
                return None
            attempt, token = row["attempt_no"] + 1, uuid.uuid4().hex
            db.execute("""UPDATE ar5_outbox SET state='leased',attempt_no=?,lease_token=?,worker_id=?,
                lease_until=?,last_error='' WHERE report_id=?""",
                       (attempt, token, str(worker_id), now + lease_seconds, row["report_id"]))
            db.execute("""INSERT INTO ar5_attempts(report_id,attempt_no,lease_token,worker_id,
                started_at,lease_until,state) VALUES(?,?,?,?,?,?,'leased')""",
                       (row["report_id"], attempt, token, str(worker_id), now, now + lease_seconds))
            return self._read_report(db, row["report_id"], include_lease=True)

    def _owned_attempt(self, db, report_id, lease_token):
        row = db.execute("SELECT * FROM ar5_outbox WHERE owner_id=? AND report_id=?",
                         (self.owner_id, report_id)).fetchone()
        if not row or not lease_token or not hmac.compare_digest(str(lease_token), row["lease_token"]):
            raise ContractError("report claim does not own the current attempt")
        return row

    def ack(self, report_id, lease_token, *, delivery_ref, now=None):
        now = self._now(now)
        if not delivery_ref:
            raise ContractError("sent reports require a platform delivery reference")
        with self._transaction() as db:
            row = self._owned_attempt(db, report_id, lease_token)
            if row["state"] == "sent":
                if row["delivery_ref"] != str(delivery_ref):
                    raise ContractError("report delivery receipt conflicts with recorded receipt")
                return self._read_report(db, report_id)
            if row["state"] not in {"leased", "unknown"}:
                raise ContractError("report has no unresolved send attempt")
            db.execute("""UPDATE ar5_outbox SET state='sent',sent_at=?,delivery_ref=?,lease_until=NULL,
                last_error='' WHERE report_id=?""", (now, str(delivery_ref), report_id))
            db.execute("UPDATE ar5_attempts SET state='sent',delivery_ref=?,last_error='' WHERE report_id=? AND attempt_no=?",
                       (str(delivery_ref), report_id, row["attempt_no"]))
            return self._read_report(db, report_id)

    def mark_unknown(self, report_id, lease_token, *, reason="delivery_unconfirmed", now=None):
        self._now(now)
        with self._transaction() as db:
            row = self._owned_attempt(db, report_id, lease_token)
            if row["state"] in {"sent", "unknown"}:
                return self._read_report(db, report_id)
            if row["state"] != "leased":
                raise ContractError("report has no current send attempt")
            db.execute("UPDATE ar5_outbox SET state='unknown',lease_until=NULL,last_error=? WHERE report_id=?",
                       (str(reason), report_id))
            db.execute("UPDATE ar5_attempts SET state='unknown',last_error=? WHERE report_id=? AND attempt_no=?",
                       (str(reason), report_id, row["attempt_no"]))
            return self._read_report(db, report_id)

    def resolve_unknown(self, report_id, *, outcome, evidence_ref, attempt_no,
                        actor_id=None, delivery_ref="", now=None):
        """Resolve a specific uncertain attempt using lookup or owner evidence.

        ``not_sent`` / ``confirmed_unsent`` retain the same body and idempotency
        key, but the next claim receives a new token.  A stale resolution or
        receipt can never release or acknowledge a later send attempt.
        """
        self._check_owner(actor_id)
        now = self._now(now)
        if outcome not in {"sent", "not_sent", "confirmed_unsent"} or not evidence_ref:
            raise ContractError("resolution requires a known outcome and evidence")
        if outcome == "sent" and not delivery_ref:
            raise ContractError("confirmed delivery requires a platform reference")
        resolution = {"outcome": outcome, "evidence_ref": evidence_ref,
                      "attempt_no": attempt_no, "delivery_ref": str(delivery_ref)}
        with self._transaction() as db:
            row = db.execute("SELECT * FROM ar5_outbox WHERE owner_id=? AND report_id=?",
                             (self.owner_id, report_id)).fetchone()
            if not row or row["attempt_no"] != attempt_no:
                raise ContractError("resolution does not match the current report attempt")
            if row["resolution_json"] == _json(resolution) and row["state"] in {"ready", "sent"}:
                return self._read_report(db, report_id)
            if row["state"] != "unknown":
                raise ContractError("only an unknown report attempt can be resolved")
            state = "sent" if outcome == "sent" else "ready"
            db.execute("""UPDATE ar5_outbox SET state=?,sent_at=?,delivery_ref=?,lease_until=NULL,
                last_error='',resolution_json=?,not_before=? WHERE report_id=?""",
                       (state, now if state == "sent" else None, str(delivery_ref), _json(resolution),
                        now if row["priority"] == "P0" else self._next_work_time(now), report_id))
            db.execute("UPDATE ar5_attempts SET state=?,delivery_ref=?,evidence_ref=?,last_error='' WHERE report_id=? AND attempt_no=?",
                       ("sent" if state == "sent" else "confirmed_unsent", str(delivery_ref),
                        _json(evidence_ref), report_id, attempt_no))
            return self._read_report(db, report_id)

    def not_sent(self, report_id, lease_token, *, evidence_ref, now=None):
        """Record a definitive pre-send/transport rejection, then permit retry."""
        if not evidence_ref:
            raise ContractError("definitive non-delivery requires evidence")
        now = self._now(now)
        with self._transaction() as db:
            row = self._owned_attempt(db, report_id, lease_token)
            resolution = {"outcome": "not_sent", "evidence_ref": evidence_ref,
                          "attempt_no": row["attempt_no"], "delivery_ref": ""}
            if row["state"] == "ready" and row["resolution_json"] == _json(resolution):
                return self._read_report(db, report_id)
            if row["state"] not in {"leased", "unknown"}:
                raise ContractError("report has no unresolved send attempt")
            db.execute("""UPDATE ar5_outbox SET state='ready',lease_until=NULL,last_error='',
                resolution_json=?,not_before=? WHERE report_id=?""",
                       (_json(resolution), now if row["priority"] == "P0" else self._next_work_time(now), report_id))
            db.execute("UPDATE ar5_attempts SET state='confirmed_unsent',evidence_ref=?,last_error='' WHERE report_id=? AND attempt_no=?",
                       (_json(evidence_ref), report_id, row["attempt_no"]))
            return self._read_report(db, report_id)
