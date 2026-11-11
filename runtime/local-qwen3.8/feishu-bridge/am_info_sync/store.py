"""Transactional inbox, immutable observations, durable work and coverage checkpoints.

This module does not contain network calls. A successful accept_event() means the
SQLite transaction committed, not that a model understood the event.
"""
from __future__ import annotations

from contextlib import contextmanager
from dataclasses import asdict
from pathlib import Path
import json
import os
import sqlite3
import threading
import uuid
from typing import Any, Iterator, Sequence

from .models import (Source, Observation, Page, Clock, canonical, digest, now_ms,
                     ScopeViolation, IntegrityConflict, LeaseLost)
from .document_policy import membership_changes


SCHEMA = """
PRAGMA foreign_keys=ON;
CREATE TABLE IF NOT EXISTS schema_meta(version INTEGER PRIMARY KEY);
INSERT OR IGNORE INTO schema_meta VALUES(1);
CREATE TABLE IF NOT EXISTS source_access_version(
 singleton INTEGER PRIMARY KEY CHECK(singleton=1), namespace TEXT NOT NULL, revision INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS sources(
 source_id TEXT PRIMARY KEY, config_json TEXT NOT NULL,
 availability TEXT NOT NULL DEFAULT 'unknown', collected_through_ms INTEGER,
 last_checked_ms INTEGER, last_reconciled_ms INTEGER, last_error TEXT,
 updated_at_ms INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS observations(
 source_id TEXT NOT NULL REFERENCES sources(source_id), object_id TEXT NOT NULL,
 version TEXT NOT NULL, occurred_at_ms INTEGER NOT NULL, text TEXT NOT NULL,
 payload_json TEXT NOT NULL, deleted INTEGER NOT NULL, completeness TEXT NOT NULL,
 parent_id TEXT, url TEXT, received_at_ms INTEGER NOT NULL,
 raw_expires_at_ms INTEGER NOT NULL, purged INTEGER NOT NULL DEFAULT 0,
 body_hash TEXT NOT NULL, PRIMARY KEY(source_id,object_id,version));
CREATE TABLE IF NOT EXISTS objects(
 source_id TEXT NOT NULL REFERENCES sources(source_id), object_id TEXT NOT NULL,
 current_version TEXT NOT NULL, state TEXT NOT NULL, occurred_at_ms INTEGER NOT NULL,
 PRIMARY KEY(source_id,object_id));
CREATE TABLE IF NOT EXISTS observation_revisions(
 source_id TEXT NOT NULL, object_id TEXT NOT NULL, version TEXT NOT NULL,
 revision_id INTEGER NOT NULL, PRIMARY KEY(source_id,object_id,version));
CREATE TABLE IF NOT EXISTS inbox(
 event_key TEXT PRIMARY KEY, source_id TEXT NOT NULL REFERENCES sources(source_id),
 event_id TEXT NOT NULL, event_type TEXT NOT NULL, payload_json TEXT NOT NULL,
 body_hash TEXT NOT NULL, received_at_ms INTEGER NOT NULL, expires_at_ms INTEGER NOT NULL,
 purged INTEGER NOT NULL DEFAULT 0);
CREATE TABLE IF NOT EXISTS jobs(
 job_id TEXT PRIMARY KEY, kind TEXT NOT NULL, dedupe_key TEXT NOT NULL,
 payload_json TEXT NOT NULL, state TEXT NOT NULL DEFAULT 'pending', attempts INTEGER NOT NULL DEFAULT 0,
 available_at_ms INTEGER NOT NULL, lease_token TEXT, lease_until_ms INTEGER,
 last_error TEXT, created_at_ms INTEGER NOT NULL, finished_at_ms INTEGER,
 UNIQUE(kind,dedupe_key));
CREATE INDEX IF NOT EXISTS jobs_ready ON jobs(state,available_at_ms,lease_until_ms);
CREATE TABLE IF NOT EXISTS sync_runs(
 run_id TEXT PRIMARY KEY, source_id TEXT NOT NULL REFERENCES sources(source_id),
 mode TEXT NOT NULL, start_ms INTEGER NOT NULL, end_ms INTEGER NOT NULL,
 cursor TEXT, page_count INTEGER NOT NULL DEFAULT 0, state TEXT NOT NULL,
 lease_token TEXT, lease_until_ms INTEGER, last_error TEXT,
 time_coverage INTEGER NOT NULL DEFAULT 1, created_at_ms INTEGER NOT NULL,
 finished_at_ms INTEGER);
CREATE UNIQUE INDEX IF NOT EXISTS one_active_sync ON sync_runs(source_id) WHERE state='running';
CREATE TABLE IF NOT EXISTS sync_pages(
 run_id TEXT NOT NULL REFERENCES sync_runs(run_id), cursor_key TEXT NOT NULL,
 next_cursor TEXT, body_hash TEXT NOT NULL, saved_at_ms INTEGER NOT NULL,
 PRIMARY KEY(run_id,cursor_key));
CREATE TABLE IF NOT EXISTS gaps(
 source_id TEXT NOT NULL REFERENCES sources(source_id), object_id TEXT NOT NULL,
 kind TEXT NOT NULL, detail TEXT NOT NULL, state TEXT NOT NULL,
 first_seen_ms INTEGER NOT NULL, last_seen_ms INTEGER NOT NULL,
 PRIMARY KEY(source_id,object_id,kind));
CREATE TABLE IF NOT EXISTS discoveries(
 discovery_id TEXT PRIMARY KEY, parent_source_id TEXT NOT NULL REFERENCES sources(source_id),
 kind TEXT NOT NULL, remote_id TEXT NOT NULL, reason TEXT NOT NULL, settings_json TEXT NOT NULL,
 state TEXT NOT NULL DEFAULT 'pending', created_at_ms INTEGER NOT NULL,
 UNIQUE(parent_source_id,kind,remote_id));
"""


DOCUMENT_ACCESS_SCHEMA = {
    'document_access_meta': '''CREATE TABLE IF NOT EXISTS document_access_meta(
        singleton INTEGER PRIMARY KEY CHECK(singleton=1), namespace TEXT NOT NULL,
        access_namespace TEXT NOT NULL, access_revision INTEGER NOT NULL,
        fallback_epoch INTEGER NOT NULL DEFAULT 0)''',
    'document_source_access_epochs': '''CREATE TABLE IF NOT EXISTS document_source_access_epochs(
        source_id TEXT PRIMARY KEY, revision INTEGER NOT NULL)''',
    'document_project_access_epochs': '''CREATE TABLE IF NOT EXISTS document_project_access_epochs(
        source_id TEXT NOT NULL, subject_id TEXT NOT NULL, revision INTEGER NOT NULL,
        PRIMARY KEY(source_id,subject_id))''',
}


def _no_credentials(value: Any) -> None:
    if isinstance(value, dict):
        for key, child in value.items():
            if key.lower() in {"token", "access_token", "refresh_token", "app_secret", "password", "authorization"}:
                raise ValueError("Credentials must be provided by the host, not persisted in source settings")
            _no_credentials(child)
    elif isinstance(value, (list, tuple)):
        for child in value:
            _no_credentials(child)


class Store:
    def __init__(self, path: str | Path, clock: Clock = now_ms):
        self.path = str(path)
        self.clock = clock
        self._lock = threading.RLock()
        if self.path != ":memory:":
            Path(self.path).parent.mkdir(parents=True, exist_ok=True)
            # Create with restrictive permissions before opening SQLite.
            fd = os.open(self.path, os.O_CREAT | os.O_RDWR, 0o600)
            os.close(fd)
        self.conn = sqlite3.connect(self.path, timeout=10, isolation_level=None, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=FULL")
        self.conn.execute("PRAGMA secure_delete=ON")
        self.conn.execute("PRAGMA busy_timeout=10000")
        self.conn.executescript(SCHEMA)
        self.conn.execute("INSERT OR IGNORE INTO source_access_version VALUES(1,?,0)", (uuid.uuid4().hex,))
        if self.conn.execute("SELECT MAX(version) FROM schema_meta").fetchone()[0] != 1:
            raise RuntimeError("Unsupported database schema; explicit migration is required")
        with self.transaction() as db:
            existing = {row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")
                        if row[0] in DOCUMENT_ACCESS_SCHEMA}
            for statement in DOCUMENT_ACCESS_SCHEMA.values():
                db.execute(statement)
            current = db.execute("SELECT namespace,revision FROM source_access_version WHERE singleton=1").fetchone()
            # A fresh document namespace makes migration (or recreation of the
            # epoch tables) unable to authenticate a pre-existing v2 binding.
            db.execute("""INSERT OR IGNORE INTO document_access_meta
                (singleton,namespace,access_namespace,access_revision,fallback_epoch)
                VALUES(1,?,?,?,0)""", (uuid.uuid4().hex, current['namespace'], current['revision']))
            if existing and existing != set(DOCUMENT_ACCESS_SCHEMA):
                # Partial migration/table recreation lost some history. Rotate
                # before committing the recreation, so no concurrent capture
                # can authenticate an old binding against empty epoch tables.
                db.execute("UPDATE document_access_meta SET namespace=? WHERE singleton=1", (uuid.uuid4().hex,))
            self._reconcile_document_access(db)

    def close(self) -> None:
        with self._lock:
            self.conn.close()

    @contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        with self._lock:
            if self.conn.in_transaction:
                raise RuntimeError("Nested transactions are not supported")
            self.conn.execute("BEGIN IMMEDIATE")
            try:
                yield self.conn
                self.conn.execute("COMMIT")
            except BaseException:
                self.conn.execute("ROLLBACK")
                raise

    @contextmanager
    def read_in_transaction(self, connection: sqlite3.Connection) -> Iterator[sqlite3.Connection]:
        """Read through a caller-owned transaction without ending its fence.

        This is deliberately explicit rather than making transaction() silently
        nest. The lock also prevents another thread from borrowing an active
        transaction: it waits for that owner to finish, then fails the check.
        No writes, BEGIN, COMMIT or network work belong in this read scope.
        """
        with self._lock:
            if connection is not self.conn or not connection.in_transaction:
                raise RuntimeError("An active transaction on this Store is required")
            yield connection

    def register_source(self, source: Source) -> None:
        _no_credentials(source.settings)
        with self.transaction() as db:
            previous = db.execute("SELECT config_json FROM sources WHERE source_id=?", (source.source_id,)).fetchone()
            if previous:
                old = Source.from_dict(json.loads(previous[0]))
                if (old.kind, old.remote_id, old.tenant_id, old.owner_user_id, old.start_ms) != (
                    source.kind, source.remote_id, source.tenant_id, source.owner_user_id, source.start_ms
                ):
                    raise ScopeViolation("Source identity/history boundary is immutable; use an explicit new source")
            db.execute("""INSERT INTO sources(source_id,config_json,updated_at_ms) VALUES(?,?,?)
                ON CONFLICT(source_id) DO UPDATE SET config_json=excluded.config_json,updated_at_ms=excluded.updated_at_ms""",
                (source.source_id, canonical(source.to_dict()), self.clock()))
            if previous and previous[0] != canonical(source.to_dict()):
                db.execute("UPDATE sync_runs SET lease_token=NULL,lease_until_ms=0 WHERE source_id=? AND state='running'", (source.source_id,))
                # Conservative shared epoch: derived conversation content is
                # invalidated on local visibility changes, without identity API calls.
                access = lambda s: (s.enabled, s.readers, s.output_chats,
                                    s.settings.get('read_identity'), s.settings.get('subject_id'),
                                    s.settings.get('subject_ids'))
                if access(old) != access(source):
                    scope_changed, subjects = membership_changes(old, source)
                    source_access = lambda s: (s.enabled, frozenset(s.readers), frozenset(s.output_chats),
                                               s.settings.get('read_identity'))
                    self._bump_source_access(db, source.source_id,
                        source_wide=scope_changed or source_access(old) != source_access(source),
                        subjects=subjects)
                    if not source.enabled or not old.enabled or old.settings.get('read_identity')!=source.settings.get('read_identity'):
                        db.execute("UPDATE sources SET availability='denied',last_error='source_access_revalidation_required' WHERE source_id=?",(source.source_id,))
                analysis = lambda s: {k:s.settings.get(k) for k in (
                    'subject_id','subject_ids','fact_mappings','mapping_version','trusted_structured','extract_text')}
                if source.kind in ('docx','file_snapshot') and analysis(old)!=analysis(source):
                    self._requeue_context(db,source,changed=True)

    def _requeue_context(self,db,source,*,changed=False):
        """Re-analyze only current evidence; fence workers holding old mappings.

        Expired raw evidence stays expired. Its durable job/gap explicitly waits
        for lawful reacquisition or a new version; a URL enrichment is not one.
        """
        status=db.execute('SELECT availability FROM sources WHERE source_id=?',(source.source_id,)).fetchone()[0]
        rows=db.execute("""SELECT r.* FROM observations r JOIN objects o
            ON o.source_id=r.source_id AND o.object_id=r.object_id AND o.current_version=r.version
            WHERE o.source_id=? AND o.state='active' AND r.deleted=0""",(source.source_id,)).fetchall()
        for raw in rows:
            oid,version=raw['object_id'],raw['version']
            if not changed and not db.execute("SELECT 1 FROM gaps WHERE source_id=? AND object_id=? AND kind='analysis_context_changed' AND state='open'",(source.source_id,oid)).fetchone():continue
            key=digest([source.source_id,oid,version])
            self._enqueue(db,'extract',key,{'source_id':source.source_id,'object_id':oid,'version':version})
            job=db.execute("SELECT state,last_error FROM jobs WHERE kind='extract' AND dedupe_key=?",(key,)).fetchone()
            if not changed and job['state']!='dead':continue
            legal=source.enabled and status!='denied' and not raw['purged'] and raw['raw_expires_at_ms']>self.clock() and raw['completeness']=='complete'
            if changed or legal:
                db.execute("UPDATE jobs SET state=?,attempts=0,available_at_ms=?,lease_token=NULL,lease_until_ms=NULL,finished_at_ms=NULL,last_error=? WHERE kind='extract' AND dedupe_key=?",
                    ('pending' if legal else 'dead',self.clock(),None if legal else 'context_waiting_for_current_evidence',key))
            self._gap(db,source.source_id,oid,'analysis_context_changed','Current source mapping requires fresh extraction' if legal else 'Waiting for readable unexpired current evidence')

    def access_version(self) -> str:
        with self._lock:
            row = self.conn.execute("SELECT namespace,revision FROM source_access_version WHERE singleton=1").fetchone()
        return row[0] + ':' + str(row[1])

    def _reconcile_document_access(self, db: sqlite3.Connection) -> None:
        """Persist a conservative fence for writes by pre-v2/rollback code.

        Older writers still advance the global revision. They cannot identify
        which source changed, so an unsynchronised revision invalidates every
        previous v2 binding, including revoke-then-restore histories. This runs
        only at initialization or a known access write, never during capture.
        """
        current = db.execute("SELECT namespace,revision FROM source_access_version WHERE singleton=1").fetchone()
        db.execute("""UPDATE document_access_meta SET fallback_epoch=fallback_epoch+1,
            access_namespace=?,access_revision=? WHERE singleton=1 AND
            (access_namespace<>? OR access_revision<>?)""",
            (current['namespace'], current['revision'], current['namespace'], current['revision']))

    def _bump_source_access(self, db: sqlite3.Connection, source_id: str, *,
                            source_wide: bool = True, subjects: Sequence[str] = ()) -> None:
        # All three existing global-epoch mutation paths call this helper in
        # their original transaction. The global behavior remains unchanged.
        self._reconcile_document_access(db)
        if source_wide:
            db.execute("""INSERT INTO document_source_access_epochs VALUES(?,1)
                ON CONFLICT(source_id) DO UPDATE SET revision=revision+1""", (source_id,))
        for subject in subjects:
            db.execute("""INSERT INTO document_project_access_epochs VALUES(?,?,1)
                ON CONFLICT(source_id,subject_id) DO UPDATE SET revision=revision+1""", (source_id, subject))
        db.execute("UPDATE source_access_version SET revision=revision+1 WHERE singleton=1")
        db.execute("""UPDATE document_access_meta SET
            access_namespace=(SELECT namespace FROM source_access_version WHERE singleton=1),
            access_revision=(SELECT revision FROM source_access_version WHERE singleton=1)
            WHERE singleton=1""")

    def document_access_state(self, connection: sqlite3.Connection,
                              source_ids: Sequence[str], subject: str) -> dict[str, Any]:
        """Read scoped history from the same transaction as the evidence.

        An untracked global write is represented in the returned fingerprint,
        without repairing metadata or otherwise writing during capture. A later
        initialization/known writer durably folds it into fallback_epoch.
        """
        with self.read_in_transaction(connection) as db:
            current = db.execute("SELECT namespace,revision FROM source_access_version WHERE singleton=1").fetchone()
            meta = db.execute("SELECT * FROM document_access_meta WHERE singleton=1").fetchone()
            if meta is None:
                raise RuntimeError('Document access history is unavailable')
            untracked = None
            if (meta['access_namespace'], meta['access_revision']) != (current['namespace'], current['revision']):
                untracked = current['namespace'] + ':' + str(current['revision'])
            epochs = []
            for source_id in sorted(source_ids):
                source = db.execute("SELECT revision FROM document_source_access_epochs WHERE source_id=?", (source_id,)).fetchone()
                project = db.execute("SELECT revision FROM document_project_access_epochs WHERE source_id=? AND subject_id=?", (source_id, subject)).fetchone()
                epochs.append({'source_id': source_id, 'source_epoch': source[0] if source else 0,
                               'project_epoch': project[0] if project else 0})
            return {'namespace': meta['namespace'], 'access_namespace': current['namespace'],
                    'fallback_epoch': meta['fallback_epoch'], 'untracked_access_version': untracked,
                    'sources': epochs}

    def get_source(self, source_id: str) -> Source:
        with self._lock:
            row = self.conn.execute("SELECT config_json FROM sources WHERE source_id=?", (source_id,)).fetchone()
        if row is None:
            raise ScopeViolation("Source is not registered")
        return Source.from_dict(json.loads(row[0]))

    def list_sources(self) -> list[Source]:
        with self._lock:
            rows = self.conn.execute("SELECT config_json FROM sources ORDER BY source_id").fetchall()
        return [Source.from_dict(json.loads(r[0])) for r in rows]

    def source_status(self, source_id: str) -> dict[str, Any]:
        with self._lock:
            row = self.conn.execute("SELECT * FROM sources WHERE source_id=?", (source_id,)).fetchone()
        if row is None:
            raise ScopeViolation("Source is not registered")
        result = dict(row)
        result.pop("config_json")
        return result

    def _require_source(self, db: sqlite3.Connection, source_id: str) -> Source:
        row = db.execute("SELECT config_json FROM sources WHERE source_id=?", (source_id,)).fetchone()
        if not row:
            raise ScopeViolation("Source is not registered")
        source = Source.from_dict(json.loads(row[0]))
        if not source.enabled:
            raise ScopeViolation("Source is disabled")
        return source

    def _enqueue(self, db: sqlite3.Connection, kind: str, key: str, payload: dict[str, Any]) -> str:
        found = db.execute("SELECT job_id FROM jobs WHERE kind=? AND dedupe_key=?", (kind, key)).fetchone()
        if found:
            return found[0]
        job_id = uuid.uuid4().hex
        timestamp = self.clock()
        db.execute("INSERT INTO jobs(job_id,kind,dedupe_key,payload_json,available_at_ms,created_at_ms) VALUES(?,?,?,?,?,?)",
                   (job_id, kind, key, canonical(payload), timestamp, timestamp))
        return job_id

    def enqueue(self, kind: str, key: str, payload: dict[str, Any]) -> str:
        with self.transaction() as db:
            return self._enqueue(db, kind, key, payload)

    def accept_event(self, source_id: str, event_id: str, event_type: str,
                     payload: dict[str, Any]) -> dict[str, Any]:
        if not event_id or not event_type:
            raise ValueError("Event identity is required")
        _no_credentials(payload)
        key = digest([source_id, event_type, event_id])
        body_hash = digest(payload)
        with self.transaction() as db:
            source = self._require_source(db, source_id)
            old = db.execute("SELECT body_hash FROM inbox WHERE event_key=?", (key,)).fetchone()
            if old:
                if old[0] != body_hash:
                    raise IntegrityConflict("Event identity reused with different content")
                return {"accepted": True, "duplicate": True, "event_key": key}
            timestamp = self.clock()
            db.execute("INSERT INTO inbox VALUES(?,?,?,?,?,?,?,?,0)",
                       (key, source_id, event_id, event_type, canonical(payload), body_hash,
                        timestamp, timestamp + source.raw_retention_seconds * 1000))
            self._enqueue(db, "event", key, {"event_key": key, "source_id": source_id})
        return {"accepted": True, "duplicate": False, "event_key": key}

    def get_event(self, event_key: str) -> dict[str, Any] | None:
        with self._lock:
            row = self.conn.execute("SELECT * FROM inbox WHERE event_key=?", (event_key,)).fetchone()
        if not row or row["purged"] or row["expires_at_ms"] <= self.clock():
            return None
        return dict(row)

    def _gap(self, db: sqlite3.Connection, source_id: str, object_id: str, kind: str, detail: str) -> None:
        timestamp = self.clock()
        db.execute("""INSERT INTO gaps VALUES(?,?,?,?, 'open',?,?) ON CONFLICT(source_id,object_id,kind)
            DO UPDATE SET detail=excluded.detail,state='open',last_seen_ms=excluded.last_seen_ms""",
            (source_id, object_id, kind, detail[:500], timestamp, timestamp))

    def open_gap(self, source_id: str, object_id: str, kind: str, detail: str) -> None:
        with self.transaction() as db:
            self._gap(db, source_id, object_id, kind, detail)

    def resolve_gap(self, source_id: str, object_id: str, kind: str) -> None:
        with self.transaction() as db:
            db.execute("UPDATE gaps SET state='resolved',last_seen_ms=? WHERE source_id=? AND object_id=? AND kind=?",
                       (self.clock(), source_id, object_id, kind))

    def _record(self, db: sqlite3.Connection, obs: Observation, authoritative: bool = False) -> str:
        source = self._require_source(db, obs.source_id)
        boundary_time = obs.payload.get("created_at_ms", obs.occurred_at_ms)
        if not isinstance(boundary_time, int) or boundary_time < source.start_ms:
            raise ScopeViolation("Observation precedes the authorized history boundary")
        content = obs.to_dict()
        # Some APIs expose a stable revision but no source update timestamp.
        # A later observation time alone is not a content mutation.
        content.pop("occurred_at_ms")
        content["payload"].pop("transport_metadata", None)
        body_hash = digest(content)
        current = db.execute("SELECT * FROM objects WHERE source_id=? AND object_id=?",
                             (obs.source_id, obs.object_id)).fetchone()
        revision = obs.payload.get("revision_id")
        new_revision = int(revision) if str(revision).isdigit() else None
        prior_revision = None
        if current:
            saved_revision = db.execute("SELECT revision_id FROM observation_revisions WHERE source_id=? AND object_id=? AND version=?",
                                       (obs.source_id, obs.object_id, current["current_version"])).fetchone()
            prior_revision = saved_revision[0] if saved_revision else None
        ordered_revision = new_revision is not None and prior_revision is not None
        lower_revision = ordered_revision and new_revision < prior_revision
        existing = db.execute("SELECT body_hash,url FROM observations WHERE source_id=? AND object_id=? AND version=?",
                              (obs.source_id, obs.object_id, obs.version)).fetchone()
        if existing:
            if existing[0] != body_hash:
                # Only an authoritative blank-to-link enrichment is permitted.
                # Restore ONLY the old URL for the full hash comparison, even
                # after raw purging. Existing links are not replaced or cleared.
                old_link_content = {**content, 'url': existing['url']}
                if (authoritative is not True or existing['url'] not in (None,'')
                        or not isinstance(obs.url,str) or not obs.url.strip()
                        or digest(old_link_content) != existing['body_hash']):
                    raise IntegrityConflict("Object version reused with different content")
                db.execute("UPDATE observations SET url=?,body_hash=? WHERE source_id=? AND object_id=? AND version=?",
                           (obs.url,body_hash,obs.source_id,obs.object_id,obs.version))
                # Metadata enrichment neither reacquires raw content nor
                # reselects a conflicted current version or changes its jobs.
                return "historical" if lower_revision else "duplicate"
            if lower_revision:
                return "historical"
            # A successful authoritative reread can resolve an equal-time ambiguity.
            if authoritative:
                saved = db.execute("SELECT * FROM observations WHERE source_id=? AND object_id=? AND version=?",
                                   (obs.source_id, obs.object_id, obs.version)).fetchone()
                key = digest([obs.source_id, obs.object_id, obs.version])
                pending = db.execute("SELECT * FROM jobs WHERE kind='extract' AND dedupe_key=? AND state!='done'", (key,)).fetchone()
                if (saved["purged"] or saved["raw_expires_at_ms"] <= self.clock()) and pending and source.settings.get("reacquire_expired_for_pending") is True:
                    db.execute("UPDATE observations SET text=?,payload_json=?,purged=0,raw_expires_at_ms=? WHERE source_id=? AND object_id=? AND version=?",
                               (obs.text, canonical(obs.payload), self.clock() + source.raw_retention_seconds * 1000,
                                obs.source_id, obs.object_id, obs.version))
                    if pending["state"] != "leased" or (pending["lease_until_ms"] or 0) <= self.clock():
                        db.execute("UPDATE jobs SET state='pending',attempts=0,available_at_ms=?,lease_token=NULL,lease_until_ms=NULL WHERE job_id=?",
                                   (self.clock(), pending["job_id"]))
                    db.execute("UPDATE gaps SET state='resolved' WHERE source_id=? AND object_id=? AND kind='raw_expired_before_analysis'",
                               (obs.source_id, obs.object_id))
                if current and current["state"] == "conflict" and obs.occurred_at_ms >= current["occurred_at_ms"]:
                    self._set_current(db, obs)
            return "duplicate"
        timestamp = self.clock()
        db.execute("INSERT INTO observations VALUES(?,?,?,?,?,?,?,?,?,?,?,?,0,?)",
                   (obs.source_id, obs.object_id, obs.version, obs.occurred_at_ms, obs.text,
                    canonical(obs.payload), int(obs.deleted), obs.completeness, obs.parent_id, obs.url,
                    timestamp, timestamp + source.raw_retention_seconds * 1000, body_hash))
        if new_revision is not None:
            db.execute("INSERT INTO observation_revisions VALUES(?,?,?,?)",
                       (obs.source_id, obs.object_id, obs.version, new_revision))
        result = "inserted"
        if current:
            if lower_revision:
                return "historical"
            if not (ordered_revision and new_revision > prior_revision) and obs.occurred_at_ms < current["occurred_at_ms"]:
                return "historical"
            if (not (ordered_revision and new_revision > prior_revision)
                    and obs.occurred_at_ms == current["occurred_at_ms"] and not authoritative):
                db.execute("UPDATE objects SET state='conflict' WHERE source_id=? AND object_id=?",
                           (obs.source_id, obs.object_id))
                self._gap(db, obs.source_id, obs.object_id, "version_conflict", "Equal-time versions need an authoritative reread")
                return "conflict"
            result = "updated"
        self._set_current(db, obs)
        return result

    def _set_current(self, db: sqlite3.Connection, obs: Observation) -> None:
        state = "deleted" if obs.deleted else "active"
        db.execute("""INSERT INTO objects VALUES(?,?,?,?,?) ON CONFLICT(source_id,object_id)
            DO UPDATE SET current_version=excluded.current_version,state=excluded.state,occurred_at_ms=excluded.occurred_at_ms""",
            (obs.source_id, obs.object_id, obs.version, state, obs.occurred_at_ms))
        db.execute("UPDATE gaps SET state='resolved',last_seen_ms=? WHERE source_id=? AND object_id=? AND kind IN ('version_conflict','content_incomplete')",
                   (self.clock(), obs.source_id, obs.object_id))
        if obs.completeness != "complete" and not obs.deleted:
            self._gap(db, obs.source_id, obs.object_id, "content_incomplete", "Content or an attachment is not fully read")
        if not obs.deleted:
            self._enqueue(db, "extract", digest([obs.source_id, obs.object_id, obs.version]),
                          {"source_id": obs.source_id, "object_id": obs.object_id, "version": obs.version})

    def record(self, observation: Observation, authoritative: bool = False) -> str:
        with self.transaction() as db:
            return self._record(db, observation, authoritative)

    def get_observation(self, source_id: str, object_id: str, version: str | None = None) -> Observation | None:
        with self._lock:
            if version is None:
                current = self.conn.execute("SELECT current_version FROM objects WHERE source_id=? AND object_id=?",
                                            (source_id, object_id)).fetchone()
                if not current:
                    return None
                version = current[0]
            row = self.conn.execute("SELECT * FROM observations WHERE source_id=? AND object_id=? AND version=?",
                                    (source_id, object_id, version)).fetchone()
        if not row or row["purged"] or row["raw_expires_at_ms"] <= self.clock():
            return None
        return Observation(source_id, object_id, version, row["occurred_at_ms"], row["text"],
                           json.loads(row["payload_json"]), bool(row["deleted"]), row["completeness"],
                           row["parent_id"], row["url"])

    def current_records(self, source_id: str | None = None) -> list[dict[str, Any]]:
        query = """SELECT o.*,v.completeness,v.received_at_ms,v.text,v.payload_json,v.raw_expires_at_ms,v.purged
            FROM objects o JOIN observations v ON o.source_id=v.source_id AND o.object_id=v.object_id AND o.current_version=v.version"""
        params: tuple[Any, ...] = ()
        if source_id is not None:
            query += " WHERE o.source_id=?"
            params = (source_id,)
        with self._lock:
            return [dict(r) for r in self.conn.execute(query, params).fetchall()]

    def claim_job(self, kinds: Sequence[str] | str, lease_seconds: int = 120,
                  prefer_source_kinds: Sequence[str] = ()) -> dict[str, Any] | None:
        if isinstance(kinds, str):
            kinds = (kinds,)
        if not kinds or lease_seconds < 1:
            raise ValueError("A job kind and positive lease are required")
        timestamp = self.clock()
        marks = ",".join("?" for _ in kinds)
        priority = ''
        if prefer_source_kinds:
            priority = 'CASE WHEN (SELECT json_extract(config_json,\'$.kind\') FROM sources WHERE source_id=json_extract(jobs.payload_json,\'$.source_id\')) IN ('+','.join('?' for _ in prefer_source_kinds)+') THEN 0 ELSE 1 END, '
        with self.transaction() as db:
            row = db.execute(f"""SELECT * FROM jobs WHERE kind IN ({marks}) AND
                ((state='pending' AND available_at_ms<=?) OR (state='leased' AND lease_until_ms<=?))
                ORDER BY {priority}created_at_ms,job_id LIMIT 1""", (*kinds, timestamp, timestamp,*prefer_source_kinds)).fetchone()
            if not row:
                return None
            token = uuid.uuid4().hex
            db.execute("UPDATE jobs SET state='leased',attempts=attempts+1,lease_token=?,lease_until_ms=? WHERE job_id=?",
                       (token, timestamp + lease_seconds * 1000, row["job_id"]))
            return dict(db.execute("SELECT * FROM jobs WHERE job_id=?", (row["job_id"],)).fetchone())

    def _check_job(self, db: sqlite3.Connection, job_id: str, token: str) -> sqlite3.Row:
        row = db.execute("SELECT * FROM jobs WHERE job_id=?", (job_id,)).fetchone()
        if not row or row["state"] != "leased" or row["lease_token"] != token or row["lease_until_ms"] <= self.clock():
            raise LeaseLost("Worker lease expired or was replaced")
        return row

    def finish_job(self, job_id: str, token: str) -> None:
        with self.transaction() as db:
            self._check_job(db, job_id, token)
            db.execute("UPDATE jobs SET state='done',finished_at_ms=?,lease_until_ms=NULL WHERE job_id=?", (self.clock(), job_id))

    def renew_job(self, job_id: str, token: str, lease_seconds: int = 120) -> None:
        with self.transaction() as db:
            self._check_job(db, job_id, token)
            db.execute("UPDATE jobs SET lease_until_ms=? WHERE job_id=?", (self.clock() + lease_seconds * 1000, job_id))

    def fail_job(self, job_id: str, token: str, code: str, retry_after_seconds: float = 5, max_attempts: int = 5) -> None:
        with self.transaction() as db:
            row = self._check_job(db, job_id, token)
            state = "dead" if row["attempts"] >= max_attempts else "pending"
            db.execute("UPDATE jobs SET state=?,available_at_ms=?,lease_until_ms=NULL,lease_token=NULL,last_error=? WHERE job_id=?",
                       (state, self.clock() + max(1, int(retry_after_seconds * 1000)), code[:100], job_id))
            if state == "dead":
                payload = json.loads(row["payload_json"])
                if payload.get("source_id"):
                    self._gap(db, payload["source_id"], payload.get("object_id", ""), "dead_job", code[:100])

    def retry_dead(self, job_id: str) -> None:
        with self.transaction() as db:
            db.execute("UPDATE jobs SET state='pending',attempts=0,available_at_ms=?,last_error=NULL WHERE job_id=? AND state='dead'",
                       (self.clock(), job_id))

    def mark_source_error(self, source_id: str, code: str, permission_denied: bool = False) -> None:
        with self.transaction() as db:
            old = db.execute("SELECT availability FROM sources WHERE source_id=?", (source_id,)).fetchone()
            denied = permission_denied or bool(old and old[0] == 'denied')
            db.execute("UPDATE sources SET availability=?,last_error=?,updated_at_ms=? WHERE source_id=?",
                       ("denied" if denied else "error", code[:100], self.clock(), source_id))
            if permission_denied and old and old[0] != 'denied':
                self._bump_source_access(db, source_id)
            self._gap(db, source_id, "", "source_read", code[:100])
            if permission_denied:
                # An in-flight response obtained before revocation cannot restore
                # readability; a newly claimed run must validate access again.
                db.execute("UPDATE sync_runs SET lease_token=NULL,lease_until_ms=0 WHERE source_id=? AND state='running'", (source_id,))

    def begin_sync(self, source_id: str, mode: str = "incremental", lease_seconds: int = 120) -> dict[str, Any]:
        if mode not in {"incremental", "reconcile"}:
            raise ValueError("Invalid synchronization mode")
        timestamp = self.clock()
        with self.transaction() as db:
            source = self._require_source(db, source_id)
            row = db.execute("SELECT * FROM sync_runs WHERE source_id=? AND state='running'", (source_id,)).fetchone()
            token = uuid.uuid4().hex
            if row:
                if row["lease_until_ms"] and row["lease_until_ms"] > timestamp:
                    raise LeaseLost("Another worker owns this source synchronization")
                db.execute("UPDATE sync_runs SET lease_token=?,lease_until_ms=? WHERE run_id=?",
                           (token, timestamp + lease_seconds * 1000, row["run_id"]))
                return dict(db.execute("SELECT * FROM sync_runs WHERE run_id=?", (row["run_id"],)).fetchone())
            status = db.execute("SELECT * FROM sources WHERE source_id=?", (source_id,)).fetchone()
            watermark = status["collected_through_ms"]
            start = source.start_ms if watermark is None else max(source.start_ms, watermark - source.overlap_seconds * 1000)
            if mode == "reconcile":
                lookback = max(source.overlap_seconds, int(source.settings.get("reconcile_lookback_seconds", 86400)))
                start = max(source.start_ms, timestamp - lookback * 1000)
            if start > timestamp:
                raise ScopeViolation("Authorized history starts in the future")
            run_id = uuid.uuid4().hex
            db.execute("""INSERT INTO sync_runs(run_id,source_id,mode,start_ms,end_ms,state,lease_token,lease_until_ms,created_at_ms)
                VALUES(?,?,?,?,?,'running',?,?,?)""", (run_id, source_id, mode, start, timestamp, token,
                                                       timestamp + lease_seconds * 1000, timestamp))
            return dict(db.execute("SELECT * FROM sync_runs WHERE run_id=?", (run_id,)).fetchone())

    def save_page(self, run_id: str, token: str, requested_cursor: str | None, page: Page,
                  lease_seconds: int = 120) -> dict[str, Any]:
        with self.transaction() as db:
            run = db.execute("SELECT * FROM sync_runs WHERE run_id=?", (run_id,)).fetchone()
            if not run or run["state"] != "running" or run["lease_token"] != token or run["lease_until_ms"] <= self.clock():
                raise LeaseLost("Synchronization lease expired")
            self._require_source(db, run["source_id"])
            if requested_cursor != run["cursor"]:
                raise IntegrityConflict("Page cursor does not match the saved checkpoint")
            cursor_key = canonical(requested_cursor)
            if page.next_cursor is not None:
                if page.next_cursor == requested_cursor or db.execute("SELECT 1 FROM sync_pages WHERE run_id=? AND cursor_key=?",
                                                                       (run_id, canonical(page.next_cursor))).fetchone():
                    raise IntegrityConflict("Repeated pagination cursor")
            for obs in page.observations:
                if obs.source_id != run["source_id"]:
                    raise ScopeViolation("Adapter returned an observation from another source")
                self._record(db, obs, authoritative=True)
            for item in page.discoveries:
                db.execute("INSERT OR IGNORE INTO discoveries VALUES(?,?,?,?,?,?, 'pending',?)",
                           (digest([run["source_id"], item.kind, item.remote_id]), run["source_id"], item.kind,
                            item.remote_id, item.reason[:300], canonical(item.settings), self.clock()))
            page_hash = digest([o.to_dict() for o in page.observations])
            db.execute("INSERT INTO sync_pages VALUES(?,?,?,?,?)", (run_id, cursor_key, page.next_cursor, page_hash, self.clock()))
            coverage = bool(run["time_coverage"]) and page.time_coverage
            done = page.next_cursor is None
            db.execute("""UPDATE sync_runs SET cursor=?,page_count=page_count+1,time_coverage=?,state=?,
                lease_until_ms=?,finished_at_ms=?,last_error=NULL WHERE run_id=?""",
                (page.next_cursor, int(coverage), "complete" if done else "running", self.clock() + lease_seconds * 1000,
                 self.clock() if done else None, run_id))
            if done:
                if db.execute("SELECT availability FROM sources WHERE source_id=?", (run['source_id'],)).fetchone()[0] == 'denied':
                    self._bump_source_access(db, run['source_id'])
                # Only incremental runs can advance contiguous historical coverage.
                watermark = run["end_ms"] if coverage and run["mode"] == "incremental" else None
                db.execute("""UPDATE sources SET availability='ok',last_checked_ms=?,last_error=NULL,
                    collected_through_ms=CASE WHEN ? IS NULL THEN collected_through_ms
                     ELSE MAX(COALESCE(collected_through_ms,0),?) END,
                    last_reconciled_ms=CASE WHEN ?='reconcile' THEN ? ELSE last_reconciled_ms END,
                    updated_at_ms=? WHERE source_id=?""", (self.clock(), watermark, watermark, run["mode"],
                                                           self.clock(), self.clock(), run["source_id"]))
                db.execute("UPDATE gaps SET state='resolved',last_seen_ms=? WHERE source_id=? AND kind='source_read'",
                           (self.clock(), run["source_id"]))
                self._requeue_context(db,self._require_source(db,run['source_id']))
            return dict(db.execute("SELECT * FROM sync_runs WHERE run_id=?", (run_id,)).fetchone())

    def release_sync(self, run_id: str, token: str, code: str | None = None, restart: bool = False) -> None:
        with self.transaction() as db:
            row = db.execute("SELECT * FROM sync_runs WHERE run_id=?", (run_id,)).fetchone()
            if not row or row["state"] != "running" or row["lease_token"] != token:
                return
            db.execute("UPDATE sync_runs SET lease_until_ms=0,lease_token=NULL,last_error=? WHERE run_id=?", (code, run_id))
            if restart:
                # Invalid/expired opaque cursor: replay the SAME fixed window safely.
                db.execute("DELETE FROM sync_pages WHERE run_id=?", (run_id,))
                db.execute("UPDATE sync_runs SET cursor=NULL,page_count=0,time_coverage=1 WHERE run_id=?", (run_id,))

    def purge_expired_raw(self) -> dict[str, int]:
        timestamp = self.clock()
        with self.transaction() as db:
            rows = db.execute("SELECT source_id,object_id,version FROM observations WHERE purged=0 AND raw_expires_at_ms<=?", (timestamp,)).fetchall()
            for row in rows:
                key = digest([row["source_id"], row["object_id"], row["version"]])
                pending = db.execute("SELECT 1 FROM jobs WHERE kind='extract' AND dedupe_key=? AND state!='done'", (key,)).fetchone()
                if pending:
                    self._gap(db, row["source_id"], row["object_id"], "raw_expired_before_analysis", "Retention expired before extraction completed")
            observations = db.execute("UPDATE observations SET text='',payload_json='{}',purged=1 WHERE purged=0 AND raw_expires_at_ms<=?", (timestamp,)).rowcount
            events = db.execute("UPDATE inbox SET payload_json='{}',purged=1 WHERE purged=0 AND expires_at_ms<=?", (timestamp,)).rowcount
        return {"observations": observations, "events": events}

    def health(self) -> dict[str, Any]:
        # Operator-only diagnostics; do not expose this unfiltered to the public bot.
        with self._lock:
            counts = {r["state"]: r["n"] for r in self.conn.execute("SELECT state,COUNT(*) n FROM jobs GROUP BY state")}
            gaps = [dict(r) for r in self.conn.execute("SELECT * FROM gaps WHERE state='open' ORDER BY first_seen_ms")]
            discoveries = self.conn.execute("SELECT COUNT(*) FROM discoveries WHERE state='pending'").fetchone()[0]
        sources = []
        for source in self.list_sources():
            status = self.source_status(source.source_id)
            last = status["last_checked_ms"]
            status.update(enabled=source.enabled, kind=source.kind,
                          stale=last is None or self.clock() - last > source.poll_seconds * 2000)
            sources.append(status)
        return {"at_ms": self.clock(), "sources": sources, "jobs": counts, "gaps": gaps,
                "pending_discoveries": discoveries, "coverage_claim": "registered_sources_only"}

    def backup(self, destination: str | Path) -> None:
        target = Path(destination)
        target.parent.mkdir(parents=True, exist_ok=True)
        fd = os.open(target, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        os.close(fd)
        with self._lock:
            other = sqlite3.connect(str(target))
            try:
                self.conn.backup(other)
            finally:
                other.close()
