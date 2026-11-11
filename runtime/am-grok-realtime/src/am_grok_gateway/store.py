"""SQLite inbox + outbox in the same transactions; no cross-database truth copy."""
from __future__ import annotations

from contextlib import closing, contextmanager
import hashlib
import json
from pathlib import Path
import sqlite3
import threading
import time
import uuid
from typing import Any, Iterator

from .models import BotResult, InboundMessage, canonical_json


ACTIVE = ("queued", "dispatching", "waiting", "running", "dispatch_unknown", "ready", "sending")
CALLBACK_STATES = ("dispatching", "waiting", "running", "dispatch_unknown")


class Store:
    def __init__(self, path: str | Path, *, clock=time.time):
        self.path = str(Path(path).resolve())
        self.clock = clock
        self._lock = threading.RLock()
        Path(self.path).parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        with closing(self._connect()) as db:
            db.execute("PRAGMA journal_mode=WAL")
            db.executescript("""
            CREATE TABLE IF NOT EXISTS gateway_meta(key TEXT PRIMARY KEY,value TEXT NOT NULL);
            INSERT OR IGNORE INTO gateway_meta VALUES('schema_version','1');
            CREATE TABLE IF NOT EXISTS gateway_requests(
              request_id TEXT PRIMARY KEY, dedupe_key TEXT NOT NULL UNIQUE,
              conversation_id TEXT NOT NULL, sender_id TEXT NOT NULL,
              message_json TEXT NOT NULL, origin TEXT NOT NULL, status TEXT NOT NULL,
              attempt_id TEXT NOT NULL, deadline_at REAL NOT NULL, output_deadline_at REAL,
              event_created_at REAL NOT NULL, received_at REAL NOT NULL, created_at REAL NOT NULL,
              dispatch_claimed_at REAL, dispatch_at REAL, accepted_at REAL, started_at REAL, result_at REAL,
              send_started_at REAL, sent_at REAL, readback_at REAL, visible_at REAL,
              output_message_id TEXT NOT NULL DEFAULT '', run_id TEXT NOT NULL DEFAULT '',
              result_json TEXT, bot_result_json TEXT, result_digest TEXT, result_origin TEXT NOT NULL DEFAULT 'bot',
              useful INTEGER NOT NULL DEFAULT 0, cancel_requested INTEGER NOT NULL DEFAULT 0,
              error_code TEXT NOT NULL DEFAULT '', measurement_class TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS gateway_queue ON gateway_requests(status,origin,created_at);
            CREATE INDEX IF NOT EXISTS gateway_conversation ON gateway_requests(conversation_id,status);
            CREATE TABLE IF NOT EXISTS gateway_outbox(
              request_id TEXT PRIMARY KEY REFERENCES gateway_requests(request_id),
              kind TEXT NOT NULL, status TEXT NOT NULL, lease TEXT NOT NULL DEFAULT '',
              idempotency_key TEXT NOT NULL UNIQUE
            );
            CREATE TABLE IF NOT EXISTS gateway_events(
              sequence INTEGER PRIMARY KEY AUTOINCREMENT, request_id TEXT NOT NULL,
              time REAL NOT NULL, event TEXT NOT NULL, detail TEXT NOT NULL DEFAULT ''
            );
            CREATE TABLE IF NOT EXISTS gateway_nonces(
              nonce TEXT PRIMARY KEY, expires_at REAL NOT NULL
            );
            """)
            if db.execute("SELECT value FROM gateway_meta WHERE key='schema_version'").fetchone()[0] != "1":
                raise ValueError("unsupported gateway database version")
        # Best effort on platforms supporting POSIX permissions; data never logged.
        Path(self.path).chmod(0o600)

    def _connect(self):
        db = sqlite3.connect(self.path, timeout=0.25, isolation_level=None)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA foreign_keys=ON")
        db.execute("PRAGMA busy_timeout=250")
        db.execute("PRAGMA synchronous=FULL")
        return db

    @contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        with self._lock:
            db = self._connect()
            try:
                db.execute("BEGIN IMMEDIATE")
                yield db
                db.commit()
            except BaseException:
                db.rollback()
                raise
            finally:
                db.close()

    def _event(self, db, rid, event, detail=""):
        db.execute("INSERT INTO gateway_events(request_id,time,event,detail) VALUES(?,?,?,?)",
                   (rid, self.clock(), event, detail[:256]))

    def admit(self, message: InboundMessage, *, ttl: float, max_pending: int) -> tuple[dict, bool]:
        now = self.clock()
        with self.transaction() as db:
            old = db.execute("SELECT * FROM gateway_requests WHERE dedupe_key=?", (message.dedupe_key,)).fetchone()
            if old:
                self._event(db, old["request_id"], "duplicate_ingress", message.origin)
                return dict(old), False
            pending = db.execute("SELECT count(*) FROM gateway_requests WHERE status IN (%s)" %
                                 ",".join("?" for _ in ACTIVE), ACTIVE).fetchone()[0]
            rid, attempt = "gr_" + uuid.uuid4().hex, uuid.uuid4().hex
            overloaded = pending >= max_pending
            db.execute("""INSERT INTO gateway_requests(
              request_id,dedupe_key,conversation_id,sender_id,message_json,origin,status,
              attempt_id,deadline_at,event_created_at,received_at,created_at,measurement_class)
              VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)""",
              (rid, message.dedupe_key, message.conversation_id, message.sender_id,
               canonical_json(message.to_dict()), message.origin, "queued", attempt,
               now + ttl, message.event_created_at, message.received_at, now, message.measurement_class))
            self._event(db, rid, "admitted", message.origin)
            if overloaded:
                self._notice(db, rid, attempt, "QUEUE_FULL",
                             "这条消息暂时排不过来，尚未开始处理。请稍后重试。")
            return dict(db.execute("SELECT * FROM gateway_requests WHERE request_id=?", (rid,)).fetchone()), True

    def _notice(self, db, rid, attempt, code, text):
        result = BotResult(rid, attempt, "answer", text).to_dict()
        raw = canonical_json(result)
        db.execute("""UPDATE gateway_requests SET status='ready',result_json=?,result_digest=coalesce(result_digest,?),
          result_at=?,result_origin='gateway_notice',error_code=?,output_deadline_at=? WHERE request_id=?""",
          (raw, hashlib.sha256(raw.encode()).hexdigest(), self.clock(), code, self.clock() + 5.0, rid))
        db.execute("""INSERT INTO gateway_outbox VALUES(?, 'reply','pending','',?)
          ON CONFLICT(request_id) DO UPDATE SET kind='reply',status='pending',lease=''
          WHERE gateway_outbox.status='pending'""",
                   (rid, hashlib.sha256((rid + ":final").encode()).hexdigest()[:48]))
        self._event(db, rid, "status_notice", code)

    def claim_dispatch(self, max_inflight: int, max_reconcile: int) -> dict | None:
        with self.transaction() as db:
            states = CALLBACK_STATES
            placeholders = ",".join("?" for _ in states)
            live = db.execute("SELECT count(*) FROM gateway_requests WHERE status IN (%s)" % placeholders, states).fetchone()[0]
            if live >= max_inflight:
                return None
            background = db.execute("SELECT count(*) FROM gateway_requests WHERE origin='reconcile' AND status IN (%s)" %
                                    placeholders, states).fetchone()[0]
            row = db.execute("""
              SELECT q.* FROM gateway_requests q
              WHERE q.status='queued' AND q.deadline_at>?
              AND (q.origin='realtime' OR ? < ?)
              AND NOT EXISTS(SELECT 1 FROM gateway_requests a
                WHERE a.conversation_id=q.conversation_id AND a.request_id<>q.request_id
                AND a.status IN ('dispatching','waiting','running','dispatch_unknown','ready','sending'))
              AND NOT EXISTS(SELECT 1 FROM gateway_requests older
                WHERE older.conversation_id=q.conversation_id AND older.status='queued'
                AND (older.created_at<q.created_at OR (older.created_at=q.created_at AND older.rowid<q.rowid)))
              ORDER BY CASE q.origin WHEN 'realtime' THEN 0 ELSE 1 END, q.created_at, q.rowid
              LIMIT 1""", (self.clock(), background, max_reconcile)).fetchone()
            if row is None:
                return None
            db.execute("UPDATE gateway_requests SET status='dispatching',dispatch_claimed_at=? WHERE request_id=?",
                       (self.clock(), row["request_id"]))
            self._event(db, row["request_id"], "dispatch_claimed")
            return dict(db.execute("SELECT * FROM gateway_requests WHERE request_id=?", (row["request_id"],)).fetchone())

    def dispatch_receipt(self, rid: str, attempt: str, status: str, run_id=""):
        if status not in {"accepted", "rejected", "unknown"}:
            status = "unknown"
        with self.transaction() as db:
            row = db.execute("SELECT * FROM gateway_requests WHERE request_id=? AND attempt_id=?", (rid, attempt)).fetchone()
            if not row:
                return
            self._event(db, rid, "dispatch_" + status)
            # A callback may have arrived before the POST returned.
            if status == "accepted":
                db.execute("UPDATE gateway_requests SET accepted_at=?,run_id=? WHERE request_id=?",
                           (self.clock(), run_id[:256], rid))
            if row["status"] != "dispatching":
                return
            if status == "rejected":
                self._notice(db, rid, attempt, "DISPATCH_REJECTED",
                             "实时助手这次未能启动，尚未执行你的请求。请稍后重试。")
                return
            target = {"accepted": "waiting", "rejected": "failed", "unknown": "dispatch_unknown"}[status]
            db.execute("UPDATE gateway_requests SET status=?,error_code=? WHERE request_id=?",
                       (target, "" if status == "accepted" else "DISPATCH_" + status.upper(), rid))

    def dispatch_current(self, rid, attempt):
        row = self.get(rid)
        return bool(row and row["attempt_id"] == attempt and row["status"] == "dispatching"
                    and not row["cancel_requested"] and row["deadline_at"] > self.clock())

    def mark_submission(self, rid, attempt):
        with self.transaction() as db:
            changed = db.execute("""UPDATE gateway_requests SET dispatch_at=?
              WHERE request_id=? AND attempt_id=? AND status='dispatching'
              AND cancel_requested=0 AND deadline_at>?""",
              (self.clock(), rid, attempt, self.clock())).rowcount
            if changed:
                self._event(db, rid, "transport_submission_started")
            return bool(changed)

    def block_dispatch(self, rid, attempt, code):
        with self.transaction() as db:
            row = db.execute("SELECT status,attempt_id FROM gateway_requests WHERE request_id=?", (rid,)).fetchone()
            if row and row["attempt_id"] == attempt and row["status"] == "dispatching":
                db.execute("UPDATE gateway_requests SET status='blocked',error_code=? WHERE request_id=?",
                           (code[:128], rid))
                self._event(db, rid, "dispatch_blocked", code)

    def mark_started(self, rid: str, attempt: str) -> dict:
        with self.transaction() as db:
            row = db.execute("SELECT * FROM gateway_requests WHERE request_id=?", (rid,)).fetchone()
            if not row or row["attempt_id"] != attempt:
                return {"status": "stale"}
            if row["status"] not in CALLBACK_STATES or row["deadline_at"] <= self.clock():
                return {"status": "ignored"}
            db.execute("UPDATE gateway_requests SET status='running',started_at=coalesce(started_at,?) WHERE request_id=?",
                       (self.clock(), rid))
            self._event(db, rid, "started_callback")
            return {"status": "accepted"}

    def accept_result(self, result: BotResult) -> dict:
        raw = canonical_json(result.to_dict())
        digest = hashlib.sha256(raw.encode()).hexdigest()
        with self.transaction() as db:
            row = db.execute("SELECT * FROM gateway_requests WHERE request_id=?", (result.request_id,)).fetchone()
            if not row or row["attempt_id"] != result.attempt_id:
                return {"status": "stale"}
            if row["cancel_requested"] or row["status"] in {"cancelled", "expired", "superseded"}:
                self._event(db, result.request_id, "late_result_ignored", row["status"])
                return {"status": "ignored"}
            if row["result_digest"]:
                return {"status": "duplicate" if row["result_digest"] == digest else "conflict"}
            if row["deadline_at"] <= self.clock() or row["status"] not in CALLBACK_STATES:
                self._event(db, result.request_id, "late_result_ignored", row["status"])
                return {"status": "ignored"}
            target = {"answer": "ready", "handoff": "ready", "no_reply": "no_reply", "error": "failed"}[result.kind]
            db.execute("""UPDATE gateway_requests SET status=?,result_at=?,result_json=?,bot_result_json=?,result_digest=?,error_code=?
                          WHERE request_id=?""", (target, self.clock(), raw, raw, digest, result.error_code, result.request_id))
            if target == "ready":
                db.execute("INSERT INTO gateway_outbox VALUES(?,?,'pending','',?)",
                           (result.request_id, "handoff" if result.kind == "handoff" else "reply",
                            hashlib.sha256((result.request_id + ":final").encode()).hexdigest()[:48]))
            elif result.kind == "error":
                self._notice(db, result.request_id, result.attempt_id, result.error_code or "BOT_ERROR",
                             "这次处理没有完成，未执行后续业务操作。请稍后重试。")
            self._event(db, result.request_id, "result_" + result.kind)
            return {"status": "accepted"}

    def claim_output(self) -> dict | None:
        with self.transaction() as db:
            row = db.execute("""SELECT r.*,o.kind AS output_kind,o.idempotency_key FROM gateway_requests r
              JOIN gateway_outbox o USING(request_id)
              WHERE r.status='ready' AND o.status='pending' AND r.cancel_requested=0
              AND coalesce(r.output_deadline_at,r.deadline_at)>?
              ORDER BY r.created_at LIMIT 1""", (self.clock(),)).fetchone()
            if not row:
                return None
            lease = uuid.uuid4().hex
            db.execute("UPDATE gateway_outbox SET status='sending',lease=? WHERE request_id=?", (lease, row["request_id"]))
            db.execute("UPDATE gateway_requests SET status='sending',send_started_at=? WHERE request_id=?",
                       (self.clock(), row["request_id"]))
            self._event(db, row["request_id"], "output_claimed")
            return {**dict(row), "lease": lease}

    def output_current(self, rid: str, lease: str) -> bool:
        with self.transaction() as db:
            row = db.execute("""SELECT r.cancel_requested,coalesce(r.output_deadline_at,r.deadline_at) AS deadline_at,r.status,o.lease,o.status AS ostatus
              FROM gateway_requests r JOIN gateway_outbox o USING(request_id) WHERE request_id=?""", (rid,)).fetchone()
            return bool(row and row["status"] == "sending" and row["ostatus"] == "sending"
                        and row["lease"] == lease and not row["cancel_requested"] and row["deadline_at"] > self.clock())

    def finish_output(self, rid, lease, status, *, message_id="", useful=False, readback_at=None, error_code=""):
        if status not in {"sent", "failed", "unknown", "blocked", "handed_off", "cancelled"}:
            status = "unknown"
        if status == "sent" and not message_id:
            status = "unknown"
        target = "send_unknown" if status == "unknown" else status
        with self.transaction() as db:
            row = db.execute("SELECT lease,status FROM gateway_outbox WHERE request_id=?", (rid,)).fetchone()
            if not row or row["lease"] != lease or row["status"] != "sending":
                return False
            db.execute("UPDATE gateway_outbox SET status=? WHERE request_id=?", (status, rid))
            db.execute("""UPDATE gateway_requests SET status=?,sent_at=?,readback_at=?,output_message_id=?,
              useful=?,error_code=CASE WHEN ?<>'' THEN ? ELSE error_code END WHERE request_id=?""",
              (target, self.clock() if status == "sent" else None, readback_at, message_id[:256],
               int(bool(useful) and status == "sent"), error_code[:128], error_code[:128], rid))
            self._event(db, rid, "output_" + status)
            return True

    def cancel(self, conversation_id, sender_id, request_id=None) -> dict:
        cancelled, in_flight = 0, 0
        with self.transaction() as db:
            rows = db.execute("SELECT * FROM gateway_requests WHERE conversation_id=? AND sender_id=?",
                              (conversation_id, sender_id)).fetchall()
            for row in rows:
                if request_id and row["request_id"] != request_id:
                    continue
                if row["status"] not in ACTIVE:
                    continue
                rid = row["request_id"]
                db.execute("UPDATE gateway_requests SET cancel_requested=1 WHERE request_id=?", (rid,))
                if row["status"] == "sending":
                    in_flight += 1
                    self._event(db, rid, "cancel_requested_during_output")
                else:
                    db.execute("UPDATE gateway_requests SET status='cancelled' WHERE request_id=?", (rid,))
                    db.execute("UPDATE gateway_outbox SET status='cancelled' WHERE request_id=? AND status='pending'", (rid,))
                    self._event(db, rid, "delivery_cancelled")
                    cancelled += 1
        return {"cancelled": cancelled, "output_in_flight": in_flight, "provider_stopped": False}

    def expire(self):
        with self.transaction() as db:
            rows = db.execute("""SELECT request_id,attempt_id FROM gateway_requests WHERE deadline_at<=?
              AND result_origin='bot' AND cancel_requested=0
              AND status IN ('queued','dispatching','waiting','running','dispatch_unknown','ready')""", (self.clock(),)).fetchall()
            for row in rows:
                self._notice(db, row[0], row[1], "DEADLINE",
                             "这次没有在时限内得到有效答复，已停止等待旧结果。后续业务操作没有因此自动执行。")
                self._event(db, row[0], "expired")
            db.execute("""UPDATE gateway_requests SET status='expired' WHERE result_origin='gateway_notice'
              AND status='ready' AND output_deadline_at<=?""", (self.clock(),))
            db.execute("""UPDATE gateway_outbox SET status='expired' WHERE status='pending'
              AND request_id IN (SELECT request_id FROM gateway_requests WHERE status='expired')""")
            return len(rows)

    def recover(self):
        """Call only before starting this process's workers. Never replay unknown effects."""
        with self.transaction() as db:
            rows = db.execute("SELECT request_id,status FROM gateway_requests WHERE status IN ('dispatching','waiting','running','sending')").fetchall()
            for rid, status in rows:
                target = "send_unknown" if status == "sending" else "dispatch_unknown"
                db.execute("UPDATE gateway_requests SET status=?,error_code='PROCESS_RECOVERY' WHERE request_id=?", (target, rid))
                if status == "sending":
                    db.execute("UPDATE gateway_outbox SET status='unknown' WHERE request_id=?", (rid,))
                self._event(db, rid, "recovered_" + target)
            return len(rows)

    def claim_nonce(self, nonce: str, expires_at: float) -> bool:
        with self.transaction() as db:
            db.execute("DELETE FROM gateway_nonces WHERE expires_at<=?", (self.clock(),))
            try:
                db.execute("INSERT INTO gateway_nonces VALUES(?,?)", (nonce, expires_at))
            except sqlite3.IntegrityError:
                return False
            return True

    def get(self, request_id: str) -> dict | None:
        db = self._connect()
        try:
            row = db.execute("SELECT * FROM gateway_requests WHERE request_id=?", (request_id,)).fetchone()
            return dict(row) if row else None
        finally:
            db.close()

    def records(self) -> list[dict]:
        db = self._connect()
        try:
            return [dict(row) for row in db.execute("SELECT * FROM gateway_requests ORDER BY created_at,rowid")]
        finally:
            db.close()

    def history(self, message: InboundMessage, *, max_turns=4, max_chars=6000):
        """Small same-person/chat history; retain only the same current grant."""
        db = self._connect()
        try:
            rows = db.execute("""SELECT message_json,result_json FROM gateway_requests
              WHERE conversation_id=? AND status='sent' AND useful=1 AND result_origin='bot'
              AND created_at>? ORDER BY created_at DESC,rowid DESC LIMIT 20""",
              (message.conversation_id, self.clock()-86400)).fetchall()
        finally:
            db.close()
        turns, used = [], 0
        current_sources = {s["source_id"]: s["revision"] for s in message.context.get("sources", [])}
        for row in rows:
            old = InboundMessage.from_dict(json.loads(row["message_json"]))
            result = BotResult.parse(json.loads(row["result_json"]))
            if (old.auth.policy_version != message.auth.policy_version
                    or old.auth.disclosure != message.auth.disclosure
                    or not set(old.auth.allowed_source_ids).issubset(message.auth.allowed_source_ids)
                    or any(current_sources.get(s["source_id"]) != s["revision"] for s in result.sources)):
                continue
            size = len(old.text) + len(result.text)
            if used + size > max_chars:
                continue
            turns.append([
                {"role": "user", "text": old.text, "historical_context_only": True},
                {"role": "assistant", "text": result.text, "historical_context_only": True},
            ])
            used += size
            if len(turns) >= max_turns:
                break
        return [item for turn in reversed(turns) for item in turn]
