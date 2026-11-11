"""Bounded polling, persistent page recovery and scheduled reconciliation."""
from __future__ import annotations

import json
from typing import Any, Mapping

from .models import (Adapter, PermissionDenied, InvalidCursor, SnapshotChanged,
                     LeaseLost, ScopeViolation, RetryableError, digest)
from .store import Store


class SyncEngine:
    def __init__(self, store: Store, adapters: Mapping[str, Adapter], lease_seconds: int = 120):
        self.store, self.adapters, self.lease_seconds = store, dict(adapters), lease_seconds

    def run_source(self, source_id: str, mode: str = "incremental", max_pages: int = 100) -> dict[str, Any]:
        if max_pages < 1:
            raise ValueError("max_pages must be positive")
        source = self.store.get_source(source_id)
        if not source.enabled:
            raise ScopeViolation("Source is disabled")
        if source.kind not in self.adapters:
            self.store.mark_source_error(source_id, "adapter_not_configured")
            raise ScopeViolation("A verified adapter must be explicitly configured for this source kind")
        run = self.store.begin_sync(source_id, mode, self.lease_seconds)
        try:
            for _ in range(max_pages):
                # Recheck authorization after each page: a disable takes effect mid-run.
                source = self.store.get_source(source_id)
                if not source.enabled:
                    raise ScopeViolation("Source was disabled during synchronization")
                page = self.adapters[source.kind].fetch_page(source, run["start_ms"], run["end_ms"], run["cursor"])
                run = self.store.save_page(run["run_id"], run["lease_token"], run["cursor"], page, self.lease_seconds)
                if run["state"] == "complete":
                    return run
            self.store.release_sync(run["run_id"], run["lease_token"])
            return {**run, "state": "resumable"}
        except Exception as exc:
            code = getattr(exc, "code", type(exc).__name__)
            self.store.release_sync(run["run_id"], run["lease_token"], code,
                                    restart=isinstance(exc, (InvalidCursor, SnapshotChanged)))
            # Lease fencing is a concurrency signal, not evidence that the source failed.
            if not isinstance(exc, LeaseLost):
                self.store.mark_source_error(source_id, code,
                    permission_denied=isinstance(exc, PermissionDenied) or code in {"auth_expired", "authentication_expired"})
            raise


class Scheduler:
    """Only enqueues work for explicitly enabled sources; no source discovery crawl."""
    def __init__(self, store: Store):
        self.store = store

    def enqueue_due(self) -> int:
        count = 0
        for source in self.store.list_sources():
            if not source.enabled or source.kind == "manual" or source.settings.get("host_managed") is True:
                continue
            now = self.store.clock()
            status = self.store.source_status(source.source_id)
            checked = status["last_checked_ms"]
            reconciled = status["last_reconciled_ms"]
            if checked is None:
                mode = "incremental"
            elif reconciled is None or now - reconciled >= source.reconcile_seconds * 1000:
                mode = "reconcile"
            elif now - checked >= source.poll_seconds * 1000:
                mode = "incremental"
            else:
                continue
            with self.store.transaction() as db:
                pending = db.execute("SELECT payload_json FROM jobs WHERE kind='sync' AND state IN ('pending','leased')").fetchall()
                if any(json.loads(r[0]).get("source_id") == source.source_id for r in pending):
                    continue
                # Dead jobs remain visible. New probes use bounded time slots so the
                # scheduler cannot hammer an unavailable source on every tick.
                key = digest([source.source_id, mode, now // (source.poll_seconds * 1000)])
                existed = db.execute("SELECT 1 FROM jobs WHERE kind='sync' AND dedupe_key=?", (key,)).fetchone()
                self.store._enqueue(db, "sync", key, {"source_id": source.source_id, "mode": mode})
                count += int(not existed)
        return count


class SyncWorker:
    def __init__(self, store: Store, engine: SyncEngine, max_pages: int = 20):
        self.store, self.engine, self.max_pages = store, engine, max_pages

    def run_once(self) -> bool:
        # A source run can take longer than an individual HTTP request. Worker lease
        # must cover this bounded page budget; source lease is renewed on each page.
        job = self.store.claim_job(("sync",), lease_seconds=max(120, self.max_pages * 35))
        if job is None:
            return False
        args = json.loads(job["payload_json"])
        try:
            result = self.engine.run_source(args["source_id"], args.get("mode", "incremental"), self.max_pages)
            if result["state"] == "resumable":
                # Page budget yield is normal; do not exhaust the failure budget.
                with self.store.transaction() as db:
                    self.store._check_job(db, job["job_id"], job["lease_token"])
                    db.execute("UPDATE jobs SET state='pending',attempts=MAX(0,attempts-1),available_at_ms=?,lease_token=NULL,lease_until_ms=NULL WHERE job_id=?",
                               (self.store.clock(), job["job_id"]))
            else:
                self.store.finish_job(job["job_id"], job["lease_token"])
        except Exception as exc:
            retry = getattr(exc, "retry_after", None)
            delay = retry if retry is not None else min(300, 2 ** min(job["attempts"], 8))
            try:
                self.store.fail_job(job["job_id"], job["lease_token"], getattr(exc, "code", type(exc).__name__), delay)
            except LeaseLost:
                pass  # A replacement worker owns the job; it alone may checkpoint.
        return True
