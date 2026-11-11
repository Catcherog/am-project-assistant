"""Bounded rereads of known messages, including edits outside creation windows.

Disabled by default. The host supplies trusted adapters; this worker never
discovers unknown message IDs, starts a long connection or restores source access.
"""
from __future__ import annotations

import json
import re
from typing import Any, Mapping

from .models import LeaseLost, Page, PermissionDenied, Source, SyncError, canonical, digest
from .store import Store


class ReviewRejected(SyncError):
    def __init__(self, code: str):
        self.code = code
        super().__init__(code)


class KnownMessageReviewer:
    def __init__(self, store: Store, adapters: Mapping[str, Any]):
        self.store, self.adapters = store, dict(adapters)
        with store.transaction() as db:
            db.execute("""CREATE TABLE IF NOT EXISTS review_state(
                source_id TEXT NOT NULL REFERENCES sources(source_id),
                object_id TEXT NOT NULL,
                last_checked_ms INTEGER, last_attempt_ms INTEGER,
                last_enqueued_ms INTEGER, next_due_ms INTEGER NOT NULL DEFAULT 0,
                job_id TEXT, last_error TEXT,
                PRIMARY KEY(source_id,object_id)
            )""")
            db.execute("CREATE INDEX IF NOT EXISTS review_due ON review_state(next_due_ms,last_checked_ms)")

    @staticmethod
    def _enabled(source: Source) -> bool:
        return (source.enabled and source.kind == "im_chat"
                and source.settings.get("review_known_messages") is True)

    @staticmethod
    def _interval(source: Source) -> int:
        seconds = source.settings.get("review_interval_seconds", 3600)
        if type(seconds) is not int or seconds < 1:
            raise ReviewRejected("review_interval_invalid")
        return seconds * 1000

    def enqueue_due(self, batch_size: int = 10) -> int:
        """Maintain at most batch_size outstanding rereads, with a hard cap of 10."""
        if type(batch_size) is not int or not 1 <= batch_size <= 10:
            raise ValueError("Review batch_size must be between 1 and 10")
        enabled: dict[str, Source] = {}
        for source in self.store.list_sources():
            if not self._enabled(source) or self.store.source_status(source.source_id)["availability"] == "denied":
                continue
            try:
                self._interval(source)
            except ReviewRejected:
                self.store.open_gap(source.source_id, "", "message_review", "review_interval_invalid")
                continue
            enabled[source.source_id] = source
        if not enabled:
            return 0
        count = 0
        with self.store.transaction() as db:
            active = db.execute(
                "SELECT COUNT(*) FROM jobs WHERE kind='review_message' AND state IN ('pending','leased')"
            ).fetchone()[0]
            remaining = max(0, batch_size - active)
            if not remaining:
                return 0
            now = self.store.clock()
            marks = ",".join("?" for _ in enabled)
            # Only identities/rotation state are selected, never historical bodies.
            rows = db.execute(f"""SELECT o.source_id,o.object_id
                FROM objects o LEFT JOIN review_state r
                  ON r.source_id=o.source_id AND r.object_id=o.object_id
                LEFT JOIN jobs j ON j.job_id=r.job_id
                WHERE o.source_id IN ({marks}) AND o.state IN ('active','conflict')
                  AND COALESCE(r.next_due_ms,0)<=?
                  AND (j.job_id IS NULL OR j.state NOT IN ('pending','leased'))
                ORDER BY COALESCE(r.last_checked_ms,0),COALESCE(r.last_enqueued_ms,0),
                         o.source_id,o.object_id LIMIT ?""",
                (*enabled, now, remaining)).fetchall()
            for row in rows:
                raw = db.execute("SELECT * FROM sources WHERE source_id=?", (row["source_id"],)).fetchone()
                source = Source.from_dict(json.loads(raw["config_json"]))
                if not self._enabled(source) or raw["availability"] == "denied":
                    continue
                interval = self._interval(source)
                config_hash = digest(source.to_dict())
                key = digest([source.source_id, row["object_id"], config_hash, now // interval])
                if db.execute("SELECT 1 FROM jobs WHERE kind='review_message' AND dedupe_key=?", (key,)).fetchone():
                    continue
                job_id = self.store._enqueue(db, "review_message", key, {
                    "source_id": source.source_id, "object_id": row["object_id"],
                    "config_hash": config_hash,
                })
                db.execute("""INSERT INTO review_state
                    (source_id,object_id,last_enqueued_ms,next_due_ms,job_id)
                    VALUES(?,?,?,?,?) ON CONFLICT(source_id,object_id) DO UPDATE SET
                    last_enqueued_ms=excluded.last_enqueued_ms,next_due_ms=excluded.next_due_ms,
                    job_id=excluded.job_id""",
                    (source.source_id, row["object_id"], now, now + interval, job_id))
                count += 1
        return count

    def _check_context(self, db: Any, args: dict[str, Any]) -> tuple[Source, tuple[Any, ...]]:
        source_id, object_id = args.get("source_id"), args.get("object_id")
        if type(source_id) is not str or type(object_id) is not str:
            raise ReviewRejected("review_identity_invalid")
        raw = db.execute("SELECT * FROM sources WHERE source_id=?", (source_id,)).fetchone()
        if raw is None:
            raise ReviewRejected("review_source_unknown")
        source = Source.from_dict(json.loads(raw["config_json"]))
        if not self._enabled(source) or raw["availability"] == "denied":
            raise ReviewRejected("review_source_unavailable")
        config_hash = digest(source.to_dict())
        if config_hash != args.get("config_hash"):
            raise ReviewRejected("review_source_configuration_changed")
        self._interval(source)
        known = db.execute("SELECT * FROM objects WHERE source_id=? AND object_id=?",
                           (source_id, object_id)).fetchone()
        if known is None or known["state"] not in {"active", "conflict"}:
            raise ReviewRejected("review_object_not_active")
        if known["occurred_at_ms"] < source.start_ms:
            raise ReviewRejected("review_object_outside_authorized_history")
        return source, (config_hash, raw["updated_at_ms"], raw["availability"])

    def _failure(self, job: dict[str, Any], args: dict[str, Any], error: Exception) -> None:
        code = getattr(error, "code", type(error).__name__)
        if type(code) is not str or not re.fullmatch(r"[a-zA-Z0-9_]{1,100}", code):
            code = "review_failed"
        denied = isinstance(error, PermissionDenied) or code in {"authentication_expired", "auth_expired"}
        source_id, object_id = args.get("source_id"), args.get("object_id")
        try:
            with self.store.transaction() as db:
                self.store._check_job(db, job["job_id"], job["lease_token"])
                source = db.execute("SELECT config_json FROM sources WHERE source_id=?", (source_id,)).fetchone()
                if source is None:
                    # A malformed host job must not create an unregistered-source gap.
                    db.execute("UPDATE jobs SET state='dead',last_error=?,lease_token=NULL,lease_until_ms=NULL WHERE job_id=?",
                               (code, job["job_id"]))
                    return
                config_matches = digest(json.loads(source[0])) == args.get("config_hash")
                self.store._gap(db, source_id, object_id or "", "message_review", code)
                db.execute("""UPDATE review_state SET last_attempt_ms=?,last_error=?
                    WHERE source_id=? AND object_id=? AND job_id=?""",
                    (self.store.clock(), code, source_id, object_id, job["job_id"]))
            if denied and config_matches:
                self.store.mark_source_error(source_id, code, permission_denied=True)
            retry_after = getattr(error, "retry_after", None)
            delay = retry_after if retry_after is not None else min(300, 2 ** min(job["attempts"], 8))
            self.store.fail_job(job["job_id"], job["lease_token"], code, delay)
        except LeaseLost:
            # A replacement worker alone may finish or reschedule this task.
            return

    def run_once(self) -> bool:
        job = self.store.claim_job("review_message", lease_seconds=150)
        if job is None:
            return False
        args: dict[str, Any] = {}
        try:
            args = json.loads(job["payload_json"])
            if type(args) is not dict:
                raise ReviewRejected("review_job_invalid")
            with self.store.transaction() as db:
                self.store._check_job(db, job["job_id"], job["lease_token"])
                source, fence = self._check_context(db, args)
            adapter = self.adapters.get(source.kind)
            fetch = getattr(adapter, "fetch_message", None)
            if not callable(fetch):
                raise ReviewRejected("review_adapter_not_configured")
            page = fetch(source, args["object_id"])
            if not isinstance(page, Page) or page.next_cursor is not None or len(page.observations) != 1:
                raise ReviewRejected("review_not_one_message")
            observation = page.observations[0]
            if observation.source_id != source.source_id or observation.object_id != args["object_id"]:
                raise ReviewRejected("review_response_identity_mismatch")
            with self.store.transaction() as db:
                self.store._check_job(db, job["job_id"], job["lease_token"])
                current, current_fence = self._check_context(db, args)
                if current_fence != fence:
                    raise ReviewRejected("review_source_changed_during_read")
                self.store._record(db, observation, authoritative=True)
                for item in page.discoveries:
                    db.execute("INSERT OR IGNORE INTO discoveries VALUES(?,?,?,?,?,?,'pending',?)",
                               (digest([source.source_id, item.kind, item.remote_id]), source.source_id,
                                item.kind, item.remote_id, item.reason[:300], canonical(item.settings), self.store.clock()))
                now = self.store.clock()
                db.execute("""UPDATE review_state SET last_checked_ms=?,last_attempt_ms=?,
                    next_due_ms=?,job_id=NULL,last_error=NULL WHERE source_id=? AND object_id=?""",
                    (now, now, now + self._interval(current), source.source_id, args["object_id"]))
                db.execute("UPDATE gaps SET state='resolved',last_seen_ms=? WHERE source_id=? AND object_id=? AND kind='message_review'",
                           (now, source.source_id, args["object_id"]))
                db.execute("UPDATE jobs SET state='done',finished_at_ms=?,lease_token=NULL,lease_until_ms=NULL WHERE job_id=?",
                           (now, job["job_id"]))
                # Deliberately do not change source availability or historical coverage.
        except Exception as error:
            self._failure(job, args if type(args) is dict else {}, error)
        return True

