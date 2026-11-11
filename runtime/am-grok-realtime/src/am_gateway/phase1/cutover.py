"""Scope ownership is a persisted handover, never a time-based history skip."""
import json

from ..models import ContractError, dumps, key


class Cutover:
    def register_cutover(self, scope, *, evidence_ref):
        if not evidence_ref:
            raise ContractError('polling isolation or shared-claim evidence required')
        with self.store.transaction() as db:
            old = db.execute('SELECT state FROM h1_cutover WHERE scope=?', (scope,)).fetchone()
            if old and old[0] != 'harness':
                raise ContractError('existing handover must be reconciled')
            db.execute("INSERT OR REPLACE INTO h1_cutover VALUES(?,'harness',?,NULL)", (scope,evidence_ref))

    def prepare_rollback(self, scope):
        with self.store.transaction() as db:
            scope_row = db.execute('SELECT * FROM h1_cutover WHERE scope=?', (scope,)).fetchone()
            if not scope_row:
                raise ContractError('scope was not enabled')
            rows = db.execute('SELECT * FROM h1_tasks WHERE scope=? ORDER BY created', (scope,)).fetchall()
            manifest = {'scope':scope,'completed':[],'unresolved':[],'undispatched':[]}
            for row in rows:
                entry = {'task_id':row['task_id'],'item_id':row['item_id'],
                         'business_id':row['business_id'],'state':row['execution_state']}
                if row['delivery_state'] == 'delivered' and row['verification_state'] == 'sufficient' and row['execution_state'] in {'succeeded','failed','cancelled'}:
                    manifest['completed'].append(entry)
                elif row['business_id']:
                    manifest['unresolved'].append(entry)
                else:
                    manifest['undispatched'].append(entry)
            manifest['fingerprint'] = key(manifest)
            db.execute("UPDATE h1_cutover SET state='paused',manifest=? WHERE scope=?", (dumps(manifest),scope))
            return manifest

    def acknowledge_legacy_import(self, scope, *, fingerprint, excluded_items, pending_owner):
        with self.store.transaction() as db:
            row = db.execute('SELECT * FROM h1_cutover WHERE scope=?', (scope,)).fetchone()
            if not row or row['state'] != 'paused':
                raise ContractError('scope must be paused')
            manifest = json.loads(row['manifest'])
            if fingerprint != manifest['fingerprint']:
                raise ContractError('handover fingerprint mismatch')
            # Refuse stale manifests if work was admitted after the pause.
            actual = {r[0] for r in db.execute('SELECT task_id FROM h1_tasks WHERE scope=?', (scope,))}
            expected = {r['task_id'] for group in ('completed','unresolved','undispatched') for r in manifest[group]}
            if actual != expected:
                raise ContractError('new backlog needs a refreshed manifest')
            required = {r['item_id'] for group in ('completed','unresolved') for r in manifest[group]}
            if not required <= set(excluded_items):
                raise ContractError('legacy has not imported completed/uncertain exclusions')
            if set(pending_owner) != {r['task_id'] for r in manifest['undispatched']}:
                raise ContractError('every undispatched task needs an explicit owner')
            # Phase one hands the whole execution scope back to the legacy
            # consumer. Retaining a Harness owner would strand that task once
            # claim() correctly excludes the legacy-owned scope.
            if any(owner != 'legacy' for owner in pending_owner.values()):
                raise ContractError('rollback supports only legacy ownership for undispatched tasks')
            if db.execute("SELECT 1 FROM h1_sessions WHERE scope=? AND state!='free'", (scope,)).fetchone():
                raise ContractError('running or uncertain executor still owns this scope')
            # Unresolved business effects remain excluded and attached to their existing records.
            for tid in pending_owner:
                db.execute("UPDATE h1_dispatch SET state='legacy_owned' WHERE task_id=?", (tid,))
                db.execute("UPDATE h1_tasks SET dispatch_state='legacy_owned' WHERE task_id=?", (tid,))
            manifest['legacy_exclusions'] = sorted(excluded_items)
            manifest['pending_owner'] = pending_owner
            db.execute("UPDATE h1_cutover SET state='legacy',manifest=? WHERE scope=?", (dumps(manifest),scope))
            return manifest
