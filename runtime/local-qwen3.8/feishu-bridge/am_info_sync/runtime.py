"""Embedding facade and optional separate collection/analysis worker loops."""
from __future__ import annotations

import json
import logging
import threading
from typing import Any, Mapping, Callable

from .models import Adapter, Observation, Principal, Source
from .store import Store
from .policy import can_read
from .facts import FactEngine
from .extraction import ExtractionWorker
from .ingest import EventBridge, EventWorker, record_offline
from .sync import SyncEngine, SyncWorker, Scheduler
from .review import KnownMessageReviewer


class InformationService:
    def __init__(self, store: Store, adapters: Mapping[str, Adapter] | None = None,
                 extractor: Callable[[Observation], list[dict[str, Any]]] | None = None):
        self.store = store
        self.facts = FactEngine(store)
        self.event_worker = EventWorker(store)
        self.sync_engine = SyncEngine(store, adapters or {})
        self.sync_worker = SyncWorker(store, self.sync_engine)
        self.scheduler = Scheduler(store)
        self.message_reviewer = KnownMessageReviewer(store, self.sync_engine.adapters)
        self.extraction_worker = ExtractionWorker(store, extractor, fact_engine=self.facts)
        self.log = logging.getLogger("am_info_sync")
        with store.transaction() as db:
            db.execute("CREATE TABLE IF NOT EXISTS worker_health(worker TEXT PRIMARY KEY,at_ms INTEGER NOT NULL,state TEXT NOT NULL,last_error TEXT)")

    def bridge(self, app_id: str, tenant_id: str) -> EventBridge:
        return EventBridge(self.store, app_id, tenant_id)

    def query(self, subject_id: str, principal: Principal, *, connection=None) -> dict[str, Any]:
        # principal must come from the trusted AM identity resolver, not from an
        # LLM argument or a user's self-declared identity in a chat message.
        # Document snapshots use one caller-owned transaction for facts AND
        # coverage. Keep the existing standalone query contract unchanged.
        result = self.facts.query(subject_id, principal, **(
            {'connection': connection} if connection is not None else {}))
        coverage = []
        fence = (self.store.transaction() if connection is None
                 else self.store.read_in_transaction(connection))
        with fence as db:
            for row in db.execute("SELECT * FROM sources").fetchall():
                source = Source.from_dict(json.loads(row["config_json"]))
                if not can_read(source, principal) or row["availability"] == "denied":
                    continue
                fixed = source.settings.get("subject_id")
                allowed = source.settings.get("subject_ids")
                if fixed is not None and fixed != subject_id:
                    continue
                if allowed is not None and subject_id not in allowed:
                    continue
                if fixed is None and allowed is None:
                    # Unassigned sources cannot be claimed as complete coverage of
                    # this subject. Their operator gap remains in diagnostics().
                    continue
                jobs = db.execute("SELECT state,payload_json FROM jobs WHERE kind IN ('extract','event','review_message') AND state NOT IN ('done','cancelled')").fetchall()
                payloads = [json.loads(j['payload_json']) for j in jobs]
                pending = sum(p.get('source_id') == source.source_id and
                    (source.kind != 'ledger_snapshot' or not p.get('object_id') or
                     p['object_id'] == subject_id) for p in payloads)
                gaps = db.execute("SELECT object_id,kind FROM gaps WHERE source_id=? AND state='open'", (source.source_id,)).fetchall()
                if source.kind == 'ledger_snapshot':
                    gaps = [g for g in gaps if not g['object_id'] or g['object_id'] == subject_id]
                checked = row["last_checked_ms"]
                stale = (source.kind != "manual" and (row["availability"] != "ok" or checked is None
                         or self.store.clock() - checked > source.poll_seconds * 2000))
                coverage.append({"source_id": source.source_id, "kind": source.kind,
                                 "last_checked_ms": checked, "collected_through_ms": row["collected_through_ms"],
                                 "stale": stale, "unprocessed_jobs": pending,
                                 "gap_kinds": sorted({r["kind"] for r in gaps})})
        result["coverage"] = {"scope": "registered_readable_sources_with_explicit_subject_mapping",
                              "sources": coverage,
                              "processing_pending": any(r["unprocessed_jobs"] for r in coverage),
                              "needs_refresh_or_review": not coverage or any(r["stale"] or r["gap_kinds"] for r in coverage),
                              "global_completeness_claim": False}
        return result

    def offline(self, source_id: str, principal: Principal, text: str, subject_id: str,
                structured_claims: list[dict[str, Any]] | None = None,
                idempotency_key: str | None = None) -> dict[str, Any]:
        return record_offline(self.store, source_id, principal, text, subject_id,
                              structured_claims, idempotency_key)

    def pulse(self, worker: str, state: str, error: str | None = None) -> None:
        with self.store.transaction() as db:
            db.execute("""INSERT INTO worker_health VALUES(?,?,?,?) ON CONFLICT(worker)
                DO UPDATE SET at_ms=excluded.at_ms,state=excluded.state,last_error=excluded.last_error""",
                (worker, self.store.clock(), state, error))

    def collect_once(self, schedule: bool = True, run_sync: bool = True) -> dict[str, int]:
        self.pulse("collector", "working")
        scheduled = self.scheduler.enqueue_due() if schedule else 0
        reviews_scheduled = self.message_reviewer.enqueue_due() if schedule else 0
        events = 0
        for _ in range(20):
            if not self.event_worker.run_once():
                break
            events += 1
        synchronized = int(self.sync_worker.run_once()) if run_sync else 0
        reviewed = int(self.message_reviewer.run_once())
        self.pulse("collector", "idle")
        return {"scheduled": scheduled, "event_jobs": events, "sync_jobs": synchronized,
                "reviews_scheduled": reviews_scheduled, "review_jobs": reviewed}

    def analyze_once(self) -> dict[str, Any] | None:
        self.pulse("analysis", "working")
        result = self.extraction_worker.run_once()
        self.pulse("analysis", "idle")
        return result

    def maintenance(self) -> dict[str, Any]:
        raw = self.store.purge_expired_raw()
        derived = self.facts.purge_expired_quotes()
        return {"raw": raw, "derived": derived}

    def diagnostics(self) -> dict[str, Any]:
        # Operator-only. Public callers must use query(), which filters sources.
        result = self.store.health()
        with self.store._lock:
            result["workers"] = [dict(r) for r in self.store.conn.execute("SELECT * FROM worker_health")]
            result["analysis_by_source"] = [dict(r) for r in self.store.conn.execute("""SELECT source_id,
                MAX(created_at_ms) AS last_fact_commit_ms,COUNT(*) AS stored_candidate_count
                FROM fact_candidates GROUP BY source_id""")]
            analysis_jobs = self.store.conn.execute("SELECT payload_json,state,finished_at_ms,created_at_ms FROM jobs WHERE kind='extract'").fetchall()
        progress = {}
        for row in analysis_jobs:
            source_id = json.loads(row["payload_json"]).get("source_id")
            if not source_id:
                continue
            item = progress.setdefault(source_id, {"source_id": source_id, "pending": 0, "leased": 0,
                                                   "dead": 0, "done": 0, "last_completed_job_ms": None})
            item[row["state"]] += 1
            if row["finished_at_ms"] is not None:
                item["last_completed_job_ms"] = max(item["last_completed_job_ms"] or 0, row["finished_at_ms"])
        result["analysis_jobs_by_source"] = list(progress.values())
        result["external_actions"] = 0
        result["model_configured"] = self.extraction_worker.extractor is not None
        return result

    def start_workers(self, stop: threading.Event, interval_seconds: float = 1.0) -> list[threading.Thread]:
        """Start internal workers, not a Feishu listener or web server.

        The host owns lifecycle. Stop and join workers before closing the Store.
        A hard process stop leaves uncommitted work recoverable through leases.
        """
        if not 0 < interval_seconds <= 60:
            raise ValueError("Worker interval must be between 0 and 60 seconds")

        def loop(name: str, operation: Callable[[], Any]) -> None:
            last_maintenance = 0
            while not stop.is_set():
                try:
                    operation()
                    if name == "collector" and self.store.clock() - last_maintenance >= 60_000:
                        self.maintenance()
                        last_maintenance = self.store.clock()
                except Exception as exc:
                    code = getattr(exc, "code", type(exc).__name__)
                    self.pulse(name, "error", str(code)[:100])
                    self.log.error("%s worker error: %s", name, code)
                stop.wait(interval_seconds)
            self.pulse(name, "stopped")

        threads = [threading.Thread(target=loop, args=("collector", self.collect_once), daemon=True,
                                    name="am-info-collector"),
                   threading.Thread(target=loop, args=("analysis", self.analyze_once), daemon=True,
                                    name="am-info-analysis")]
        for thread in threads:
            thread.start()
        return threads
