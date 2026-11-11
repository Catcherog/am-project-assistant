"""Short SQLite transactions. No model, subprocess or network work runs here."""
from __future__ import annotations

from contextlib import contextmanager, closing
import json
from pathlib import Path
import sqlite3
import time
import uuid

from .models import Admission, ContractError, Event, dumps


SCHEMA = """
CREATE TABLE IF NOT EXISTS gw_meta(key TEXT PRIMARY KEY,value TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS gw_items(
 item_id TEXT PRIMARY KEY, tenant_id TEXT NOT NULL, native_id TEXT NOT NULL, kind TEXT NOT NULL,
 chat_id TEXT NOT NULL, thread_id TEXT NOT NULL, sender_id TEXT NOT NULL, channel TEXT NOT NULL,
 version INTEGER NOT NULL, content_hash TEXT NOT NULL, event_json TEXT NOT NULL,
 original_time REAL NOT NULL, version_time REAL NOT NULL, received_at REAL NOT NULL,
 observed_via TEXT NOT NULL, routed_version INTEGER NOT NULL DEFAULT 0,
 is_self INTEGER NOT NULL DEFAULT 0, is_deleted INTEGER NOT NULL DEFAULT 0,
 dialogue_claimed TEXT, recipient_actor TEXT NOT NULL DEFAULT '', request_id TEXT NOT NULL DEFAULT '',
 legacy_owned INTEGER NOT NULL DEFAULT 0,
 UNIQUE(tenant_id,kind,native_id)
);
CREATE INDEX IF NOT EXISTS gw_route ON gw_items(routed_version,version,received_at);
CREATE INDEX IF NOT EXISTS gw_history ON gw_items(tenant_id,chat_id,thread_id,original_time);
CREATE TABLE IF NOT EXISTS gw_item_versions(
 item_id TEXT NOT NULL,version INTEGER NOT NULL,event_json TEXT NOT NULL,content_hash TEXT NOT NULL,
 PRIMARY KEY(item_id,version)
);
CREATE TABLE IF NOT EXISTS gw_jobs(
 job_id TEXT PRIMARY KEY,request_id TEXT NOT NULL,parent_job_id TEXT,
 logical_task_key TEXT NOT NULL UNIQUE,task_kind TEXT NOT NULL,worker TEXT NOT NULL,
 input_refs TEXT NOT NULL,conversation_json TEXT NOT NULL,session_key TEXT NOT NULL,
 project_id TEXT NOT NULL DEFAULT '',batch_key TEXT NOT NULL DEFAULT '',
 priority INTEGER NOT NULL,state TEXT NOT NULL,output_mode TEXT NOT NULL,reply_owner TEXT NOT NULL,
 created_at REAL NOT NULL,available_at REAL NOT NULL,started_at REAL,completed_at REAL,
 lease_until REAL,attempt_id TEXT NOT NULL DEFAULT '',attempt INTEGER NOT NULL DEFAULT 0,
 provider_run_id TEXT NOT NULL DEFAULT '',context_json TEXT,result_json TEXT,
 provider_accepted_at REAL,provider_started_at REAL,
 last_error TEXT NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS gw_jobs_queue ON gw_jobs(worker,state,priority,available_at);
CREATE INDEX IF NOT EXISTS gw_jobs_request ON gw_jobs(request_id,parent_job_id,state);
CREATE TABLE IF NOT EXISTS gw_job_routes(logical_task_key TEXT PRIMARY KEY,job_id TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS gw_outbox(
 action_id TEXT PRIMARY KEY,job_id TEXT NOT NULL,request_id TEXT NOT NULL,reply_owner TEXT NOT NULL,
 purpose TEXT NOT NULL,target_json TEXT NOT NULL,action_type TEXT NOT NULL,body TEXT NOT NULL,
 payload_json TEXT NOT NULL DEFAULT '{}',authorization_ref TEXT NOT NULL DEFAULT '',
 state TEXT NOT NULL,draft_version INTEGER NOT NULL DEFAULT 1,dependencies TEXT NOT NULL DEFAULT '{}',
 reason TEXT NOT NULL DEFAULT '',platform_id TEXT NOT NULL DEFAULT '',created_at REAL NOT NULL,
 sent_at REAL,visible_at REAL,lease_until REAL,last_error TEXT NOT NULL DEFAULT '',
 UNIQUE(request_id,purpose)
);
CREATE INDEX IF NOT EXISTS gw_outbox_queue ON gw_outbox(state,created_at);
CREATE TABLE IF NOT EXISTS gw_facts(
 fact_id TEXT PRIMARY KEY,tenant_id TEXT NOT NULL,project_id TEXT NOT NULL,field TEXT NOT NULL,
 visibility TEXT NOT NULL,source_chat_id TEXT NOT NULL,value_json TEXT NOT NULL,
 source_refs TEXT NOT NULL,version INTEGER NOT NULL,status TEXT NOT NULL,
 observed_at REAL NOT NULL,updated_at REAL NOT NULL,candidate_json TEXT,
 UNIQUE(tenant_id,project_id,field,visibility,source_chat_id)
);
CREATE TABLE IF NOT EXISTS gw_fact_sources(
 fact_id TEXT NOT NULL,item_id TEXT NOT NULL,source_version INTEGER NOT NULL,
 PRIMARY KEY(fact_id,item_id,source_version)
);
CREATE INDEX IF NOT EXISTS gw_fact_source_item ON gw_fact_sources(item_id,source_version);
CREATE TABLE IF NOT EXISTS gw_audit(
 seq INTEGER PRIMARY KEY AUTOINCREMENT,at REAL NOT NULL,kind TEXT NOT NULL,
 object_id TEXT NOT NULL,detail TEXT NOT NULL DEFAULT ''
);
"""


class Store:
    def __init__(self, path: str | Path, *, clock=time.time):
        self.path = str(Path(path).resolve())
        self.clock = clock
        Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        with closing(self.connect()) as db:
            db.execute("PRAGMA journal_mode=WAL")
            db.executescript(SCHEMA)
            db.execute("INSERT OR IGNORE INTO gw_meta VALUES('schema_version','2')")
            if db.execute("SELECT value FROM gw_meta WHERE key='schema_version'").fetchone()[0] != "2":
                raise ContractError("unsupported gateway schema")
            present={r["name"] for r in db.execute("PRAGMA table_info(gw_jobs)")}
            for name in ("provider_accepted_at","provider_started_at"):
                if name not in present:
                    db.execute("ALTER TABLE gw_jobs ADD COLUMN "+name+" REAL")
        Path(self.path).chmod(0o600)

    def connect(self):
        db = sqlite3.connect(self.path, timeout=2, isolation_level=None)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA busy_timeout=2000")
        db.execute("PRAGMA synchronous=FULL")
        return db

    @contextmanager
    def transaction(self):
        with closing(self.connect()) as db:
            db.execute("BEGIN IMMEDIATE")
            try:
                yield db
                db.commit()
            except BaseException:
                db.rollback()
                raise

    def rows(self, sql, args=()):
        with closing(self.connect()) as db:
            return [dict(r) for r in db.execute(sql, args)]

    def one(self, sql, args=()):
        rows = self.rows(sql, args)
        return rows[0] if rows else None

    def audit(self, db, kind, object_id, detail=""):
        db.execute("INSERT INTO gw_audit(at,kind,object_id,detail) VALUES(?,?,?,?)",
                   (self.clock(), kind, object_id, detail))

    def ingest(self, event: Event) -> Admission:
        event.validate()
        with self.transaction() as db:
            return self.ingest_in_tx(db, event)

    def ingest_in_tx(self, db, event: Event) -> Admission:
        event.validate()
        now = self.clock()
        old = db.execute("SELECT * FROM gw_items WHERE item_id=?", (event.item_id,)).fetchone()
        raw = event.to_dict()
        if old:
            if old["chat_id"]!=event.chat_id or old["sender_id"]!=event.sender_id:
                raise ContractError("native message identity changed")
            # Events and polls can arrive out of order. Equal timestamps with a
            # changed body are kept, but no edit creates a second dialogue reply.
            prior = json.loads(old["event_json"])
            stale = event.version_time < old["version_time"]
            changed = event.content_hash != old["content_hash"] and not stale
            channels = sorted(set(json.loads(old["observed_via"])) | {event.observed_via})
            if stale:
                raw = prior
            elif not changed:
                # Enrichment must not overwrite trusted bot-DM/mention metadata
                # with the weaker shape of a historical list response.
                raw["mentions"] = sorted(set(prior["mentions"]) | set(raw["mentions"]))
                raw["project_ids"] = sorted(set(prior["project_ids"]) | set(raw["project_ids"]))
                raw["attachments"] = list({dumps(a): a for a in list(prior["attachments"]) + list(raw["attachments"])}.values())
                for field in ("thread_id", "parent_id", "root_id"):
                    raw[field] = raw[field] or prior[field]
                if prior["channel"] == "bot_dm":
                    raw["channel"] = "bot_dm"
                if prior.get("direction")=="outbound":
                    raw["direction"]="outbound"
            if prior.get("content_status")=="draft_edit":
                raw["content_status"]="draft_edit"
            raw["is_self"] = bool(old["is_self"] or event.is_self)
            metadata_changed=(raw["mentions"]!=prior["mentions"] or raw["project_ids"]!=prior["project_ids"]
                              or raw["channel"]!=prior["channel"] or raw["parent_id"]!=prior["parent_id"])
            ver = old["version"] + int(changed)
            effective = Event.from_dict(raw)
            db.execute("""UPDATE gw_items SET event_json=?,content_hash=?,version=?,version_time=?,
                observed_via=?,is_self=?,is_deleted=?,channel=?,thread_id=? WHERE item_id=?""",
                (dumps(raw), effective.content_hash, ver, max(event.version_time, old["version_time"]),
                 dumps(channels), int(effective.is_self), int(effective.is_deleted),
                 effective.channel, effective.thread_id, event.item_id))
            if changed:
                db.execute("INSERT INTO gw_item_versions VALUES(?,?,?,?)",
                           (event.item_id, ver, dumps(raw), effective.content_hash))
                db.execute("""UPDATE gw_facts SET status=?,version=version+1 WHERE fact_id IN
                    (SELECT fact_id FROM gw_fact_sources WHERE item_id=? AND source_version<>?)""",
                    ("withdrawn" if effective.is_deleted else "needs_review",event.item_id,ver))
            else:
                db.execute("UPDATE gw_item_versions SET event_json=? WHERE item_id=? AND version=?",
                           (dumps(raw), event.item_id, ver))
            if metadata_changed:
                db.execute("UPDATE gw_items SET routed_version=min(routed_version,version-1) WHERE item_id=?",
                           (event.item_id,))
            return Admission(event.item_id, False, changed, bool(old["dialogue_claimed"]))
        db.execute("""INSERT INTO gw_items(item_id,tenant_id,native_id,kind,chat_id,thread_id,sender_id,
           channel,version,content_hash,event_json,original_time,version_time,received_at,observed_via,
           is_self,is_deleted) VALUES(?,?,?,?,?,?,?,?,1,?,?,?,?,?,?,?,?)""",
           (event.item_id,event.tenant_id,event.native_id,event.kind,event.chat_id,event.thread_id,
            event.sender_id,event.channel,event.content_hash,dumps(raw),event.original_time,
            event.version_time,now,dumps([event.observed_via]),int(event.is_self),int(event.is_deleted)))
        db.execute("INSERT INTO gw_item_versions VALUES(?,1,?,?)",
                   (event.item_id,dumps(raw),event.content_hash))
        return Admission(event.item_id, True, True, False)

    def get_event(self, ref, db=None) -> Event:
        sql = "SELECT event_json FROM gw_item_versions WHERE item_id=? AND version=?"
        args = (ref["item_id"], ref["version"])
        r = db.execute(sql, args).fetchone() if db else self.one(sql, args)
        if not r:
            raise ContractError("missing input version")
        return Event.from_dict(json.loads(r["event_json"]))

    def claim_job(self, workers, lease_seconds) -> dict | None:
        if not workers:
            return None
        now = self.clock()
        with self.transaction() as db:
            placeholders = ",".join("?" for _ in workers)
            row = db.execute(f"""SELECT j.* FROM gw_jobs j WHERE state='queued'
                AND worker IN ({placeholders}) AND available_at<=?
                AND NOT EXISTS(SELECT 1 FROM gw_jobs a WHERE a.session_key=j.session_key
                AND a.worker=j.worker AND a.state IN ('running','submitted'))
                ORDER BY priority,created_at,job_id LIMIT 1""", (*workers, now)).fetchone()
            if not row:
                return None
            attempt = uuid.uuid4().hex
            db.execute("""UPDATE gw_jobs SET state='running',attempt_id=?,attempt=attempt+1,
                started_at=?,lease_until=? WHERE job_id=? AND state='queued'""",
                (attempt, now, now+lease_seconds, row["job_id"]))
            return dict(db.execute("SELECT * FROM gw_jobs WHERE job_id=?", (row["job_id"],)).fetchone())

    def expire_leases(self):
        """A crashed invocation may still be running outside this process; do not replay it."""
        with self.transaction() as db:
            now = self.clock()
            jobs = db.execute("""UPDATE gw_jobs SET state='unknown',last_error='lease_expired'
                WHERE state IN ('running','submitted') AND lease_until<?""", (now,)).rowcount
            actions = db.execute("""UPDATE gw_outbox SET state='unknown',last_error='send_lease_expired'
                WHERE state='sending' AND lease_until<?""", (now,)).rowcount
            return {"jobs_unknown": jobs, "actions_unknown": actions}

    def retry_known_failed(self, job_id):
        with self.transaction() as db:
            row = db.execute("SELECT * FROM gw_jobs WHERE job_id=?", (job_id,)).fetchone()
            if not row or row["state"] != "failed":
                raise ContractError("only a known failed job can be retried")
            if db.execute("SELECT 1 FROM gw_outbox WHERE request_id=? AND purpose='final'",
                          (row["request_id"],)).fetchone():
                raise ContractError("request already has a final outcome; start a new explicit request")
            db.execute("UPDATE gw_jobs SET state='queued',available_at=?,last_error='' WHERE job_id=?",
                       (self.clock(), job_id))
