"""Extractive shared information in the gateway database, with source ACL inheritance."""
import json

from ..models import ContractError, dumps, key
from .contracts import Policy

SCHEMA = """
CREATE TABLE IF NOT EXISTS h1_info(
 seq INTEGER PRIMARY KEY AUTOINCREMENT, info_id TEXT UNIQUE NOT NULL,
 project TEXT NOT NULL, nature TEXT NOT NULL, body TEXT NOT NULL,
 sources TEXT NOT NULL, readers TEXT NOT NULL, audiences TEXT NOT NULL,
 derives TEXT NOT NULL, state TEXT NOT NULL, created REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS h1_info_project ON h1_info(project,seq);
CREATE TABLE IF NOT EXISTS h1_views(
 project TEXT PRIMARY KEY, watermark INTEGER NOT NULL, info_ids TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS h1_sync(
 source TEXT PRIMARY KEY, cursor TEXT, state TEXT NOT NULL, detail TEXT NOT NULL,
 coverage TEXT NOT NULL, updated REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS h1_projection(item_id TEXT PRIMARY KEY, version INTEGER NOT NULL);
"""
NATURES = {"feedback", "forecast", "formal_record", "decision", "pending_requirement",
           "execution_claim", "verified_result"}


class Information:
    def __init__(self, store, config):
        self.store, self.config = store, config
        with store.transaction() as db:
            for statement in SCHEMA.split(';'):
                if statement.strip():
                    db.execute(statement)

    def project_sources(self, limit=100):
        """Background extractive projection, never on the query's critical path.

        ACL comes from the trusted admission policy. Unknown project association
        stays in source storage; no model guesses the owning project.
        """
        if not 1 <= limit <= 1000:
            raise ContractError('bounded projection required')
        count = 0
        with self.store.transaction() as db:
            rows = db.execute("""SELECT i.*,b.policy_json FROM gw_items i JOIN h1_inbox b USING(item_id)
                LEFT JOIN h1_projection p USING(item_id) WHERE p.version IS NULL OR p.version<i.version
                ORDER BY i.received_at LIMIT ?""",(limit,)).fetchall()
            for row in rows:
                event = json.loads(row['event_json'])
                if row['policy_json'] != '{}' and not row['is_deleted'] and not row['is_self']:
                    policy = Policy.parse(row['policy_json'])
                    if not policy.revoked and row['chat_id'] in policy.source_chats:
                        for project in event['project_ids']:
                            if project not in policy.projects:
                                continue
                            # Explicit supplements have already been committed synchronously.
                            supplement = db.execute("SELECT source_version FROM h1_tasks WHERE item_id=? AND intent='context.supplement' AND execution_state='succeeded'",(row['item_id'],)).fetchone()
                            if not supplement or supplement['source_version']!=row['version']:
                                self.append(project=project,nature='pending_requirement' if supplement else 'feedback',body=event['body'],
                                    sources=[{'item_id':row['item_id'],'version':row['version']}],
                                    readers=[policy.principal],audiences=[policy.reply_target],
                                    operation_id=key('extractive',row['item_id'],row['version'],project),db=db)
                                count += 1
                db.execute('INSERT OR REPLACE INTO h1_projection VALUES(?,?)',(row['item_id'],row['version']))
        return count

    def append(self, *, project, nature, body, sources, readers, audiences,
               operation_id, derives=(), db=None, verified_evidence=False):
        if nature not in NATURES or not project or not body or not sources or not readers or not audiences:
            raise ContractError("information needs project, nature, evidence and ACL")
        if nature == "verified_result" and not verified_evidence:
            raise ContractError("agent claims are not verified results")
        if len(body.encode()) > 65536:
            raise ContractError("information too large")
        if db is None:
            with self.store.transaction() as con:
                return self.append(project=project, nature=nature, body=body, sources=sources,
                                   readers=readers, audiences=audiences, operation_id=operation_id,
                                   derives=derives, db=con, verified_evidence=verified_evidence)
        readers, audiences = set(readers), set(audiences)
        refs = {dumps(r): r for r in sources}
        for parent_id in derives:
            parent = db.execute("SELECT * FROM h1_info WHERE info_id=?", (parent_id,)).fetchone()
            if not parent or parent['state'] != 'active':
                raise ContractError("derived source unavailable")
            readers &= set(json.loads(parent['readers']))
            audiences &= set(json.loads(parent['audiences']))
            refs.update({dumps(r): r for r in json.loads(parent['sources'])})
        if not readers or not audiences:
            raise ContractError("empty inherited ACL")
        for ref in refs.values():
            row = db.execute("SELECT version,is_deleted FROM gw_items WHERE item_id=?", (ref['item_id'],)).fetchone()
            if not row or row['is_deleted'] or row['version'] != ref['version']:
                raise ContractError("source revision unavailable")
        ident = 'info_' + key(operation_id)[:32]
        values = (project, nature, body, dumps(list(refs.values())), dumps(sorted(readers)),
                  dumps(sorted(audiences)), dumps(list(derives)))
        old = db.execute("SELECT * FROM h1_info WHERE info_id=?", (ident,)).fetchone()
        if old:
            if tuple(old[n] for n in ('project','nature','body','sources','readers','audiences','derives')) != values:
                raise ContractError("information operation payload conflict")
            return dict(old)
        db.execute("""INSERT INTO h1_info(info_id,project,nature,body,sources,readers,audiences,
                    derives,state,created) VALUES(?,?,?,?,?,?,?,?,?,?)""",
                   (ident, *values, 'active', self.store.clock()))
        return dict(db.execute("SELECT * FROM h1_info WHERE info_id=?", (ident,)).fetchone())

    def invalidate(self, item_id, db):
        # Sources are inherited transitively, including after a parent is withdrawn.
        for row in db.execute("SELECT info_id,sources FROM h1_info WHERE state='active'"):
            if any(r['item_id'] == item_id for r in json.loads(row['sources'])):
                db.execute("UPDATE h1_info SET state='stale' WHERE info_id=?", (row['info_id'],))

    def revoke(self, info_id):
        with self.store.transaction() as db:
            pending = [info_id]
            while pending:
                current = pending.pop()
                db.execute("UPDATE h1_info SET state='revoked' WHERE info_id=?", (current,))
                for row in db.execute("SELECT info_id,derives FROM h1_info WHERE state='active'"):
                    if current in json.loads(row['derives']):
                        pending.append(row['info_id'])

    def project_view(self, project):
        with self.store.transaction() as db:
            watermark = db.execute("SELECT coalesce(max(seq),0) FROM h1_info").fetchone()[0]
            rows = db.execute("SELECT info_id FROM h1_info WHERE project=? AND state='active' ORDER BY seq DESC LIMIT ?",
                              (project, self.config.delta_limit)).fetchall()
            db.execute("INSERT OR REPLACE INTO h1_views VALUES(?,?,?)", (project, watermark, dumps([r[0] for r in rows])))
            return watermark

    @staticmethod
    def allowed(db, row, policy):
        if row['state'] != 'active' or row['project'] not in policy.projects:
            return False
        if policy.principal not in json.loads(row['readers']) or policy.reply_target not in json.loads(row['audiences']):
            return False
        for ref in json.loads(row['sources']):
            source = db.execute("SELECT version,is_deleted,chat_id FROM gw_items WHERE item_id=?", (ref['item_id'],)).fetchone()
            if not source or source['is_deleted'] or source['version'] != ref['version'] or source['chat_id'] not in policy.source_chats:
                return False
        return True

    def snapshot(self, policy, *, minimum_visible_version=0, db=None):
        if db is None:
            with self.store.transaction() as con:
                return self.snapshot(policy, minimum_visible_version=minimum_visible_version, db=con)
        if policy.revoked:
            raise ContractError("authorization revoked")
        committed = db.execute("SELECT coalesce(max(seq),0) FROM h1_info").fetchone()[0]
        if committed < minimum_visible_version:
            raise ContractError("minimum visible version unavailable")
        candidates, watermarks, lag = {}, {}, False
        for project in policy.projects:
            view = db.execute("SELECT * FROM h1_views WHERE project=?", (project,)).fetchone()
            watermarks[project] = view['watermark'] if view else 0
            for info_id in json.loads(view['info_ids']) if view else []:
                row = db.execute("SELECT * FROM h1_info WHERE info_id=?", (info_id,)).fetchone()
                if row:
                    candidates[row['info_id']] = row
            delta = db.execute("SELECT * FROM h1_info WHERE project=? AND seq>? ORDER BY seq DESC LIMIT ?",
                               (project, watermarks[project], self.config.delta_limit + 1)).fetchall()
            lag |= len(delta) > self.config.delta_limit
            for row in delta[:self.config.delta_limit]:
                candidates[row['info_id']] = row
        items = []
        # Check ACL before the size accounting, so restricted rows never become references.
        for row in sorted(candidates.values(), key=lambda r: r['seq'], reverse=True):
            if not self.allowed(db, row, policy):
                continue
            value = {n: row[n] for n in ('info_id','seq','project','nature','body')}
            value['sources'] = json.loads(row['sources'])
            # Reserve bounded metadata overhead; never include an oversize latest item silently.
            if len(dumps(items + [value]).encode()) > self.config.context_bytes - 1024:
                lag = True
                continue
            items.append(value)
        coverage = [dict(r) for r in db.execute("SELECT * FROM h1_sync ORDER BY source")
                    if r['source'] in policy.source_chats]
        snapshot = {"snapshot_version": committed, "minimum_visible_version": minimum_visible_version,
                    "policy_version": policy.version, "source_watermarks": watermarks,
                    "evidence_items": items, "view_lag_warning": bool(lag),
                    "coverage": coverage[:8], "coverage_complete": False,
                    "read_more": "task-scoped information lookup; older relevant items may be omitted"}
        # Coverage detail is diagnostic, not an unbounded payload backdoor.
        while len(dumps(snapshot).encode()) > self.config.context_bytes and snapshot['coverage']:
            snapshot['coverage'].pop()
            snapshot['view_lag_warning'] = True
        if len(dumps(snapshot).encode()) > self.config.context_bytes:
            raise ContractError("context metadata exceeds budget")
        return snapshot

    def read(self, policy, info_id):
        with self.store.transaction() as db:
            row = db.execute("SELECT * FROM h1_info WHERE info_id=?", (info_id,)).fetchone()
            if policy.revoked or not row or not self.allowed(db, row, policy):
                raise ContractError("information outside current task scope")
            return {n: row[n] for n in ('info_id', 'project', 'nature', 'body')}

    def sync_failure(self, source, error_type):
        with self.store.transaction() as db:
            db.execute("""INSERT INTO h1_sync VALUES(?,NULL,'failed',?,'{}',?)
                ON CONFLICT(source) DO UPDATE SET state='failed',detail=excluded.detail,updated=excluded.updated""",
                       (source, error_type[:120], self.store.clock()))
