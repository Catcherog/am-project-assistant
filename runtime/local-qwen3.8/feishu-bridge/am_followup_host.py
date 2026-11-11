"""Follow-up admission inside the original AM service and original Scheduler.

Items, facts, model calls, callbacks and network sends retain their original owners.
The tables here contain identity, semantic evidence, disclosure and adoption only.
No worker, network client, model provider or authoritative task store is created.
"""
from dataclasses import asdict
from contextlib import contextmanager
from datetime import datetime, timezone, timedelta
import json
import re
import secrets
import uuid
import os
from pathlib import Path

from fs_agent.config import BoundaryError
from fs_agent.feishu import SendRejected, SendUnknown
from fs_agent.project_catalog import canonical, digest
from fs_agent.card_ui import card_readback_matches
from fs_agent.scheduler import TZ
from am_followup.followup import (Actor, Target, Draft, EvidenceSnapshot, SendWindow,
    FollowupCoordinator, FollowupError, DispatchBlocked, GateResult, SendOutcome,
    _draft_load)
from am_followup.confirmation import (ConfirmationBridge, DeliveredPreview,
    PreviewRowBinding, VerifiedCallback, manifest_hash)
from am_followup.rendering import PURPOSES
from am_followup_sources import FollowupSources
from am_followup_recovery import FollowupRecovery
from am_followup_state import validate_candidate,validate_identity


def utc(stamp):return datetime.fromtimestamp(stamp,timezone.utc)


class FollowupHost:
    def __init__(self, am):
        self.am,self.s,self.owner=am,am.s,am.owner
        self.tenant,self.app=am.g.config.tenant_key,am.g.config.app_id
        self.scheduler=am.scheduler;self.port=self.scheduler.port
        self.sources=FollowupSources(self)
        with self.s.lock:self.s.conn.executescript('''
        CREATE TABLE IF NOT EXISTS action_aliases(
          tenant TEXT NOT NULL,producer TEXT NOT NULL,external_id TEXT NOT NULL,item_id TEXT NOT NULL,
          canonical_id TEXT NOT NULL,decision TEXT NOT NULL,PRIMARY KEY(tenant,producer,external_id));
        CREATE TABLE IF NOT EXISTS action_semantics(
          item_id TEXT PRIMARY KEY,canonical_id TEXT UNIQUE NOT NULL,identity TEXT NOT NULL,
          version INTEGER NOT NULL,deliverable_key TEXT NOT NULL,refs TEXT NOT NULL,claims TEXT NOT NULL,
          legacy_history TEXT NOT NULL,created REAL NOT NULL);
        CREATE TABLE IF NOT EXISTS followup_candidates(
          id TEXT PRIMARY KEY,producer TEXT NOT NULL,external_id TEXT NOT NULL,payload TEXT NOT NULL,
          state TEXT NOT NULL,created REAL NOT NULL,UNIQUE(producer,external_id));
        CREATE TABLE IF NOT EXISTS followup_disclosures(
          id TEXT PRIMARY KEY,item_id TEXT NOT NULL,body_hash TEXT NOT NULL,target_hash TEXT NOT NULL,
          refs TEXT NOT NULL,revision INTEGER NOT NULL,state TEXT NOT NULL,actor TEXT NOT NULL,created REAL NOT NULL);
        CREATE TABLE IF NOT EXISTS followup_bindings(
          preview_id TEXT PRIMARY KEY,item_id TEXT NOT NULL,native_target TEXT NOT NULL,
          snapshot TEXT NOT NULL,token TEXT NOT NULL,content_hash TEXT NOT NULL,nonce TEXT,invalidated INTEGER NOT NULL DEFAULT 0);
        CREATE TABLE IF NOT EXISTS followup_adoptions(
          intent_id TEXT PRIMARY KEY,item_id TEXT NOT NULL,plan_id TEXT UNIQUE NOT NULL,
          occurrence_id TEXT UNIQUE NOT NULL,request_uuid TEXT UNIQUE NOT NULL);
        CREATE TABLE IF NOT EXISTS followup_reply_links(
          message_id TEXT PRIMARY KEY,item_id TEXT NOT NULL,occurrence_id TEXT NOT NULL,
          actor TEXT NOT NULL,kind TEXT NOT NULL,created REAL NOT NULL);
        CREATE TABLE IF NOT EXISTS action_semantic_history(
          item_id TEXT NOT NULL,version INTEGER NOT NULL,claims TEXT NOT NULL,refs TEXT NOT NULL,
          actor TEXT NOT NULL,event_id TEXT NOT NULL,PRIMARY KEY(item_id,version));
        CREATE TABLE IF NOT EXISTS followup_legacy_reminders(
          draft_id TEXT PRIMARY KEY,item_id TEXT NOT NULL,decision TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS followup_import_recovery(
          item_id TEXT PRIMARY KEY,candidate_id TEXT NOT NULL,canonical_id TEXT NOT NULL,
          state TEXT NOT NULL,paused_by_recovery INTEGER NOT NULL,item_version INTEGER NOT NULL,created REAL NOT NULL);
        CREATE TABLE IF NOT EXISTS followup_import_migration_audit(
          id TEXT PRIMARY KEY,candidate_id TEXT NOT NULL,item_id TEXT NOT NULL,kind TEXT NOT NULL,detail TEXT NOT NULL,created REAL NOT NULL);
        ''')
        with self.s.tx() as c:
            old=c.execute("SELECT value FROM meta WHERE key='followup_signing_key'").fetchone()
            key=old[0] if old else secrets.token_hex(32)
            c.execute("INSERT OR IGNORE INTO meta VALUES('followup_signing_key',?)",(key,))
            path=c.execute('PRAGMA database_list').fetchone()[2]
        self.coordinator=FollowupCoordinator(path,signing_key=bytes.fromhex(key),owner_ids={self.tenant:self.owner},
            evidence_gate=self.evidence_gate,recipient_gate=self.recipient_gate,clock=lambda:utc(self.s.clock()),
            final_native_check=self.final_native_check,final_native_guard=self.final_dependency_guard)
        self.confirmation=ConfirmationBridge(self.coordinator,expected_app_id=self.app,
            read_delivered=self.read_delivered,clock=lambda:utc(self.s.clock()))
        self.scheduler.followup=self
        self.port.followup_resolver=self.refresh_p2p
        self.am.g.reminders.followup_gate=self.legacy_reminder_allowed
        self.operator_queue=Path(path).parent/'followup-operator'
        if self.operator_queue.is_symlink():raise BoundaryError('FOLLOWUP_OPERATOR_DIRECTORY_INVALID')
        self.operator_queue.mkdir(mode=0o700,exist_ok=True)
        self.operator_queue.chmod(0o700)
        self.recover_import_orphans()
        # Only former in-flight claims, never queued work, are fenced on service start.
        for r in self.s.rows("SELECT DISTINCT worker_id FROM followup_intents WHERE state IN ('claimed','sending')"):
            self.coordinator.recover_in_flight(self.tenant,r['worker_id'])
        self.recovery=FollowupRecovery(self)
        self.recovery.recover()
        from am_followup_text import FollowupTextUI
        self.text_ui=FollowupTextUI(self)
        from am_followup_expression import NativeFollowupExpression
        self.expression=NativeFollowupExpression(self)

    def actor(self,row):
        if (row.get('app'),row.get('tenant'),row.get('user'),row.get('chat_type'))!=(self.app,self.tenant,self.owner,'p2p') or row.get('expires',0)<=self.s.clock():
            raise BoundaryError('FOLLOWUP_OWNER_SCOPE_REQUIRED')
        self.am.ledger.port.authorize_user(self.owner)
        if not self.am.flags['items'] or not self.am.flags['scheduler']:raise BoundaryError('FOLLOWUP_NOT_ENABLED')
        return Actor(self.tenant,self.owner,row['id'],utc(min(row['expires'],self.s.clock()+600)))

    def semantic(self,item_id):
        rows=self.s.rows('SELECT * FROM action_semantics WHERE item_id=?',(item_id,))
        if not rows:raise BoundaryError('FOLLOWUP_ITEM_UNBOUND')
        return rows[0]

    def incomplete_import(self,item_id,c=None):
        if item_id is None:return None
        item=self.am.items.get(item_id,c);source=json.loads(item['source'])
        if not isinstance(source,dict):return None
        if not source.get('producer') or not source.get('external_id'):return None
        args=(source['producer'],source['external_id'])
        sql="SELECT id,state FROM followup_candidates WHERE producer=? AND external_id=? AND state!='adopted'"
        rows=[dict(r) for r in c.execute(sql,args)] if c else self.s.rows(sql,args)
        return rows[0] if rows else None

    @staticmethod
    def _candidate_fields_match(item,value):
        fields=value['item']
        if not isinstance(fields,dict):return False
        return (item['project_ref']==value['identity'].get('project') and item['description']==fields.get('description')
                and item['requirements']==fields.get('requirements','')
                and set(json.loads(item['assignees']))==set(fields.get('assignees',[]))
                and item['deadline']==fields.get('deadline') and item['deadline_kind']==fields.get('deadline_kind','unspecified'))

    def _candidate_item_matches(self,item,value):
        source=json.loads(item['source'])
        return (isinstance(source,dict) and self._candidate_fields_match(item,value)
                and source.get('producer')==value['producer'] and source.get('external_id')==value['external_id'])

    @contextmanager
    def state_transaction(self):
        """Use the same cross-store fence order as final send admission.

        Source refresh, people authorization and all other platform calls must
        finish before this scope. Both transactions contain only local work.
        """
        with self.s.lock:
            if self.s.conn.in_transaction:raise BoundaryError('FOLLOWUP_NATIVE_TRANSACTION_ALREADY_ACTIVE')
            with self.sources.info.store.transaction() as information_db:
                with self.s.tx() as native_db:yield native_db,information_db

    def recover_import_orphans(self):
        """Identify only exact old import leftovers; never scan or rewrite sends.

        Startup has no information service yet, so it can link a unique original
        identity and stop it, but cannot approve its current source evidence.
        The owner's normal candidate review completes adoption of that SAME ID.
        """
        with self.s.tx() as c:
            groups={}
            for item in c.execute('SELECT * FROM action_items WHERE owner=?',(self.owner,)).fetchall():
                if c.execute('SELECT 1 FROM action_semantics WHERE item_id=?',(item['id'],)).fetchone():continue
                if c.execute('SELECT 1 FROM action_aliases WHERE item_id=?',(item['id'],)).fetchone():continue
                source=json.loads(item['source'])
                if not isinstance(source,dict):continue
                if not source.get('producer') or not source.get('external_id'):continue
                candidate=c.execute('SELECT * FROM followup_candidates WHERE producer=? AND external_id=?',
                    (source['producer'],source['external_id'])).fetchone()
                if candidate:groups.setdefault(candidate['id'],(dict(candidate),[]))[1].append(dict(item))
            for candidate,items in groups.values():
                value=json.loads(candidate['payload']);canonical_id=''
                try:
                    validate_identity(value['identity'],self.tenant)
                    canonical_id='task_'+digest(value['identity'])
                    matches=len(items)==1 and self._candidate_item_matches(items[0],value)
                    original=c.execute('SELECT item_id FROM item_operations WHERE id=?',('followup-import/'+canonical_id,)).fetchone()
                    matches=matches and original is not None and original['item_id']==items[0]['id']
                except (BoundaryError,KeyError,TypeError,ValueError):matches=False
                alias=c.execute('SELECT 1 FROM action_aliases WHERE tenant=? AND producer=? AND external_id=?',
                    (self.tenant,value.get('producer'),value.get('external_id'))).fetchone()
                matches=matches and not alias
                for item in items:
                    old=c.execute('SELECT * FROM followup_import_recovery WHERE item_id=?',(item['id'],)).fetchone()
                    state='linked' if matches else 'conflict'
                    if old:
                        if state=='conflict' and old['state']=='linked':
                            c.execute("UPDATE followup_import_recovery SET state='conflict' WHERE item_id=?",(item['id'],))
                            c.execute('INSERT OR IGNORE INTO followup_import_migration_audit VALUES(?,?,?,?,?,?)',
                                ('conflict/'+item['id'],candidate['id'],item['id'],'conflict_detected',
                                 canonical({'matching_items':len(items)}),self.s.clock()))
                        continue
                    paused=int(not item['paused'] and item['state'] not in ('closed','cancelled'))
                    if paused:
                        c.execute('UPDATE action_items SET paused=1,decision_version=version+1,version=version+1,updated=? WHERE id=?',
                            (self.s.clock(),item['id']))
                    current=self.am.items.get(item['id'],c)
                    c.execute('INSERT INTO followup_import_recovery VALUES(?,?,?,?,?,?,?)',
                        (item['id'],candidate['id'],canonical_id,state,paused,current['version'],self.s.clock()))
                    audit_id='orphan/'+item['id']
                    c.execute('INSERT OR IGNORE INTO followup_import_migration_audit VALUES(?,?,?,?,?,?)',
                        (audit_id,candidate['id'],item['id'],'linked_for_review' if matches else 'conflict_paused',
                         canonical({'canonical_id':canonical_id,'matching_items':len(items),'prior_paused':bool(item['paused'])}),self.s.clock()))
                c.execute("UPDATE followup_candidates SET state='needs_review' WHERE id=?",(candidate['id'],))

    def recovery_rows(self,candidate_id,c=None):
        sql="SELECT * FROM followup_import_recovery WHERE candidate_id=? AND state!='adopted' ORDER BY item_id"
        return [dict(r) for r in c.execute(sql,(candidate_id,))] if c else self.s.rows(sql,(candidate_id,))

    def _write_semantics(self,c,row,sem,refs,claims,history,*,force=False):
        values=(canonical(refs),canonical(claims),canonical(history))
        if not force and values==(sem['refs'],sem['claims'],sem['legacy_history']):return
        c.execute('INSERT OR IGNORE INTO action_semantic_history VALUES(?,?,?,?,?,?)',
            (sem['item_id'],sem['version'],sem['claims'],sem['refs'],row['user'],row['id']))
        changed=c.execute('UPDATE action_semantics SET refs=?,claims=?,legacy_history=?,version=version+1 WHERE item_id=? AND version=?',
            (*values,sem['item_id'],sem['version'])).rowcount
        if not changed:raise BoundaryError('FOLLOWUP_SEMANTICS_CHANGED')
        c.execute('INSERT INTO action_semantic_history VALUES(?,?,?,?,?,?)',
            (sem['item_id'],sem['version']+1,values[1],values[0],row['user'],row['id']))
        c.execute('UPDATE followup_bindings SET invalidated=1 WHERE item_id=?',(sem['item_id'],))
        # Stop only work proven not to have crossed the send boundary. Existing
        # receipts, attention identities and unknown barriers remain untouched.
        stopped=[r['intent_id'] for r in c.execute("SELECT intent_id FROM followup_intents WHERE tenant_id=? "
            "AND crossed_send_boundary=0 AND state IN ('queued','claimed') "
            "AND preview_id IN (SELECT preview_id FROM followup_bindings WHERE item_id=?)",(self.tenant,sem['item_id']))]
        c.execute("UPDATE followup_intents SET state='blocked',generation=generation+1,reason='owner_reviewed_evidence_changed',updated_at=? "
            "WHERE tenant_id=? AND crossed_send_boundary=0 AND state IN ('queued','claimed') "
            "AND preview_id IN (SELECT preview_id FROM followup_bindings WHERE item_id=?)",
            (self.s.clock(),self.tenant,sem['item_id']))
        for intent_id in stopped:
            c.execute('DELETE FROM followup_delivery_barriers WHERE tenant_id=? AND intent_id=?',(self.tenant,intent_id))
        c.execute("UPDATE reminder_occurrences SET state='cancelled',cancel_requested=1,error='FOLLOWUP_EVIDENCE_REVIEWED' "
            "WHERE state IN ('ready','sending','confirmed_not_sent') AND id IN "
            "(SELECT a.occurrence_id FROM followup_adoptions a JOIN followup_intents i ON i.intent_id=a.intent_id "
            "WHERE a.item_id=? AND i.crossed_send_boundary=0 AND i.state='blocked')",(sem['item_id'],))

    def _bind_in_transaction(self,c,row,item,identity,refs,producer,external_id,history,deliverable_key,expected_version=None,force_review=False):
        validate_identity(identity,self.tenant,project=item['project_ref'])
        item_id=item['id'];canonical_id='task_'+digest(identity)
        sem=c.execute('SELECT * FROM action_semantics WHERE item_id=?',(item_id,)).fetchone()
        if sem and (identity!=json.loads(sem['identity']) or (expected_version is not None and sem['version']!=expected_version)):
            raise BoundaryError('FOLLOWUP_IDENTITY_OR_VERSION_CHANGED')
        previous=c.execute('SELECT * FROM action_aliases WHERE tenant=? AND producer=? AND external_id=?',
            (self.tenant,producer,external_id)).fetchone()
        if previous and (previous['canonical_id']!=canonical_id or previous['item_id']!=item_id):raise BoundaryError('FOLLOWUP_ALIAS_ALREADY_BOUND')
        other=c.execute('SELECT item_id FROM action_semantics WHERE canonical_id=?',(canonical_id,)).fetchone()
        if other and other[0]!=item_id:raise BoundaryError('FOLLOWUP_CANONICAL_ALREADY_BOUND')
        if c.execute("SELECT 1 FROM followup_import_recovery WHERE canonical_id=? AND item_id!=? AND state!='adopted'",
                (canonical_id,item_id)).fetchone():raise BoundaryError('FOLLOWUP_IMPORT_RECOVERY_CONFLICT')
        if sem:
            if deliverable_key and deliverable_key!=sem['deliverable_key']:raise BoundaryError('FOLLOWUP_DELIVERABLE_REVIEW_REQUIRED')
            active=json.loads(sem['refs']) if refs is None else refs
            ids={r['fact_id'] for r in active}
            claims={k:v for k,v in json.loads(sem['claims']).items() if set(v['fact_ids'])<=ids}
            merged=json.loads(sem['legacy_history'])
            for entry in history or []:
                if entry not in merged:merged.append(entry)
            self._write_semantics(c,row,sem,active,claims,merged,force=force_review)
        else:
            if not refs:raise BoundaryError('FOLLOWUP_EVIDENCE_REQUIRED')
            c.execute('INSERT INTO action_semantics VALUES(?,?,?,1,?,?,?,?,?)',
                (item_id,canonical_id,canonical(identity),deliverable_key,canonical(refs),'{}',canonical(history or []),self.s.clock()))
            c.execute('INSERT INTO action_semantic_history VALUES(?,1,?,?,?,?)',
                (item_id,'{}',canonical(refs),row['user'],row['id']))
        c.execute('INSERT OR IGNORE INTO action_aliases VALUES(?,?,?,?,?,?)',
            (self.tenant,producer,external_id,item_id,canonical_id,row['id']))

    def bind(self,row,item_id,*,identity=None,refs=None,producer='am',external_id=None,history=None,deliverable_key=''):
        self.actor(row);item=self.am.items.get(item_id)
        if item['owner']!=self.owner:raise BoundaryError('OWNER_ONLY')
        existing=self.s.rows('SELECT * FROM action_semantics WHERE item_id=?',(item_id,))
        identity=identity or (json.loads(existing[0]['identity']) if existing else
            {'tenant':self.tenant,'project':item['project_ref'],'object':item_id,'action':'request_update','cycle':'original'})
        validate_identity(identity,self.tenant,project=item['project_ref'])
        if refs is not None or not existing:
            refs=self.sources.defaults(item['project_ref']) if refs is None else refs
            if not isinstance(refs,list) or any(not isinstance(r,dict) or not isinstance(r.get('fact_id'),str) for r in refs):
                raise BoundaryError('FOLLOWUP_EVIDENCE_REQUIRED')
            refs=self.sources.resolve(item['project_ref'],[r['fact_id'] for r in refs])
        with self.state_transaction() as (c,information_db):
            current=self.am.items.get(item_id,c)
            if current['version']!=item['version']:raise BoundaryError('FOLLOWUP_ITEM_CHANGED')
            if refs is not None and self.sources.resolve(item['project_ref'],[r['fact_id'] for r in refs],connection=information_db)!=refs:
                raise BoundaryError('FOLLOWUP_SOURCE_REVIEW_CHANGED')
            self._bind_in_transaction(c,row,current,identity,refs,producer,external_id or item_id,history,deliverable_key,
                existing[0]['version'] if existing else None)
        return self.semantic(item_id)

    def import_candidate(self,row,value):
        self.actor(row)
        validate_candidate(value,self.tenant)
        # Imports are candidates until an exact association is reviewed. History
        # omission stays explicit and cannot be interpreted as never sent.
        ident=digest([self.tenant,value['producer'],value['external_id']])
        old=self.s.rows('SELECT * FROM followup_candidates WHERE id=?',(ident,))
        if old and old[0]['payload']!=canonical(value):
            previous=json.loads(old[0]['payload'])
            if old[0]['state']!='needs_review' or previous['identity']!=value['identity']:raise BoundaryError('FOLLOWUP_IMPORT_CHANGED')
            # Owner supplied correction of a quarantined candidate. Existing
            # candidate cards bind the old payload and are rejected on confirm.
            changed=self.s.execute("UPDATE followup_candidates SET payload=? WHERE id=? AND state='needs_review' AND payload=?",
                (canonical(value),ident,old[0]['payload']))
            if not changed:raise BoundaryError('FOLLOWUP_IMPORT_CHANGED')
            return ident
        self.s.execute("INSERT OR IGNORE INTO followup_candidates VALUES(?,?,?,?,'candidate',?)",
            (ident,value['producer'],value['external_id'],canonical(value),self.s.clock()))
        return ident

    def adopt_candidate(self,row,ident,*,expected_payload_hash=None,expected_recovery_hash=None):
        self.actor(row)
        found=self.s.rows('SELECT * FROM followup_candidates WHERE id=?',(ident,))
        if not found:raise BoundaryError('FOLLOWUP_CANDIDATE_MISSING')
        value=json.loads(found[0]['payload']);validate_candidate(value,self.tenant,complete=True)
        if expected_payload_hash is not None and digest(value)!=expected_payload_hash:raise BoundaryError('FOLLOWUP_IMPORT_CHANGED')
        fields=value['item'];identity=value['identity']
        if found[0]['state']=='adopted':
            alias=self.s.rows('SELECT item_id FROM action_aliases WHERE tenant=? AND producer=? AND external_id=?',
                (self.tenant,value['producer'],value['external_id']))
            if len(alias)!=1:raise BoundaryError('FOLLOWUP_ADOPTION_INCONSISTENT')
            return self.am.items.get(alias[0]['item_id'])
        projects=self.am.projects(row)
        if not any(p['key']==identity.get('project') and p.get('identity_status')=='verified' for p in projects):raise BoundaryError('FOLLOWUP_PROJECT_UNRESOLVED')
        for user in fields['assignees']:self.am.ledger.port.authorize_user(user)
        refs=self.sources.resolve(identity['project'],value['fact_ids'])
        canonical_id='task_'+digest(identity)
        with self.state_transaction() as (c,information_db):
            candidate=c.execute('SELECT * FROM followup_candidates WHERE id=?',(ident,)).fetchone()
            if candidate['payload']!=canonical(value):raise BoundaryError('FOLLOWUP_IMPORT_CHANGED')
            if candidate['state']=='adopted':
                alias=c.execute('SELECT item_id FROM action_aliases WHERE tenant=? AND producer=? AND external_id=?',
                    (self.tenant,value['producer'],value['external_id'])).fetchone()
                if not alias:raise BoundaryError('FOLLOWUP_ADOPTION_INCONSISTENT')
                return self.am.items.get(alias['item_id'],c)
            if candidate['state'] not in ('candidate','needs_review'):raise BoundaryError('FOLLOWUP_CANDIDATE_NOT_CURRENT')
            if self.sources.resolve(identity['project'],value['fact_ids'],connection=information_db)!=refs:raise BoundaryError('FOLLOWUP_SOURCE_REVIEW_CHANGED')
            recoveries=self.recovery_rows(ident,c)
            if expected_recovery_hash is not None and digest(recoveries)!=expected_recovery_hash:raise BoundaryError('FOLLOWUP_IMPORT_RECOVERY_CHANGED')
            if recoveries and (len(recoveries)!=1 or recoveries[0]['state']!='linked' or recoveries[0]['canonical_id']!=canonical_id):
                raise BoundaryError('FOLLOWUP_IMPORT_RECOVERY_CONFLICT')
            mapped=c.execute('SELECT item_id FROM action_semantics WHERE canonical_id=?',(canonical_id,)).fetchone()
            if recoveries:
                item=self.am.items.get(recoveries[0]['item_id'],c)
                if not self._candidate_item_matches(item,value):raise BoundaryError('FOLLOWUP_IMPORT_RECOVERY_CONFLICT')
                if mapped and mapped['item_id']!=item['id']:raise BoundaryError('FOLLOWUP_IMPORT_RECOVERY_CONFLICT')
            elif mapped:
                item=self.am.items.get(mapped['item_id'],c)
                if not self._candidate_fields_match(item,value):
                    raise BoundaryError('FOLLOWUP_EXISTING_ITEM_REVIEW_REQUIRED')
            else:
                item=self.am.items.create(self.owner,'followup-import/'+canonical_id,project_ref=identity['project'],
                    description=fields['description'],requirements=fields.get('requirements',''),assignees=fields['assignees'],
                    source={'producer':value['producer'],'external_id':value['external_id'],'fact_ids':value['fact_ids']},
                    deadline=fields.get('deadline'),deadline_kind=fields.get('deadline_kind','unspecified'),conn=c)
                item=self.am.items.get(item['id'],c)
            history=json.loads(canonical(value['history']))
            for entry in history:
                native=c.execute('SELECT * FROM reminder_occurrences WHERE id=? AND item_id=?',
                    (entry.get('occurrence_id'),item['id'])).fetchone()
                if not native or native['state']!='sent' or native['readback_state']!='verified':entry['state']='legacy_unknown'
                else:entry.update(state='sent',message_id=native['message_id'])
            if not value['history_complete']:history.append({'state':'legacy_unknown','reason':'history_incomplete'})
            self._bind_in_transaction(c,row,item,identity,refs,value['producer'],value['external_id'],history,fields.get('deliverable_key',''))
            if recoveries:
                recovery=recoveries[0]
                if recovery['paused_by_recovery'] and item['paused'] and item['version']==recovery['item_version']:
                    c.execute('UPDATE action_items SET paused=0,decision_version=version+1,version=version+1,updated=? WHERE id=?',
                        (self.s.clock(),item['id']))
                c.execute("UPDATE followup_import_recovery SET state='adopted' WHERE item_id=?",(item['id'],))
                c.execute('INSERT OR IGNORE INTO followup_import_migration_audit VALUES(?,?,?,?,?,?)',
                    ('adopted/'+item['id'],ident,item['id'],'adopted_original_identity',canonical({'canonical_id':canonical_id}),self.s.clock()))
            c.execute("UPDATE followup_candidates SET state='adopted' WHERE id=?",(ident,))
            item=self.am.items.get(item['id'],c)
        return item

    def record_claim(self,row,item_id,field,value,fact_ids,*,subject_key='',reply_to=None,expected_version=None):
        self.actor(row);sem=self.semantic(item_id);item=self.am.items.get(item_id)
        enums={'execution':{'unknown','planned','in_progress','blocked','done','cancelled'},
               'response':{'pending','acknowledged','not_required'},'delivery':{'pending','delivered','unknown'},
               'acceptance':{'pending','accepted','rejected','not_required'}}
        context={'plan_delivery','sample_arrival','release_materials','planned_start','actual_start','planned_release','actual_release','approval','test_overview','external_release','deadline_text','blocker'}
        if field not in enums and field not in context:raise BoundaryError('FOLLOWUP_FIELD_INVALID')
        if field in enums and value not in enums[field]:raise BoundaryError('FOLLOWUP_VALUE_INVALID')
        if field=='delivery' and (not sem['deliverable_key'] or subject_key!=sem['deliverable_key']):raise BoundaryError('FOLLOWUP_DELIVERABLE_MISMATCH')
        claims=json.loads(sem['claims'])
        if field in claims and claims[field]['value']!=value and expected_version!=sem['version']:
            raise BoundaryError('FOLLOWUP_CONFLICT_REVIEW_REQUIRED')
        if field=='response' and value=='acknowledged' and (not reply_to or claims.get('response',{}).get('request')!=reply_to):raise BoundaryError('FOLLOWUP_RESPONSE_UNBOUND')
        if field=='acceptance' and item['verifier']!=row['user']:raise BoundaryError('VERIFICATION_EVIDENCE_REQUIRED')
        refs=self.sources.resolve(item['project_ref'],fact_ids)
        claim={'value':value,'fact_ids':fact_ids,'subject_key':subject_key,'request':reply_to,'reviewed_by':row['user'],'event':row['id']}
        # Owner-selected evidence is explicit semantic review, not automatic LLM acceptance.
        claims[field]=claim
        current={f['fact_id'] for f in self.sources.facts(item['project_ref'])['facts'] if f['evidence']['raw_available']}
        if any(r['fact_id'] not in current for r in json.loads(sem['refs'])):raise BoundaryError('FOLLOWUP_SOURCE_REVIEW_REQUIRED')
        merged={r['fact_id']:r for r in json.loads(sem['refs'])}
        merged.update({r['fact_id']:r for r in refs})
        with self.state_transaction() as (c,information_db):
            fresh=c.execute('SELECT * FROM action_semantics WHERE item_id=?',(item_id,)).fetchone()
            if fresh['version']!=sem['version']:raise BoundaryError('FOLLOWUP_SEMANTICS_CHANGED')
            if self.sources.resolve(item['project_ref'],list(merged),connection=information_db)!=list(merged.values()):
                raise BoundaryError('FOLLOWUP_SOURCE_REVIEW_CHANGED')
            self._write_semantics(c,row,fresh,list(merged.values()),claims,json.loads(sem['legacy_history']))
        # Even a reviewed done claim never bypasses ActionItems.change(close).
        return self.view(item_id)

    def source_review_card(self,row,item_id,fact_ids=None,*,_plain=False):
        self.actor(row);item=self.am.items.get(item_id);sem=self.semantic(item_id)
        if item['owner']!=self.owner:raise BoundaryError('OWNER_ONLY')
        # This card contains source values, so use the original read-delivery
        # guard as well as the owner mutation gate. Identity lookup is outside
        # database fences; delivery rechecks the captured scope and epoch.
        row=dict(row,_query_read=True,_query_grant=self.am.query_grant(row))
        old=json.loads(sem['refs'])
        if fact_ids is None:
            self.sources.refresh(old)
            facts=self.sources.facts(item['project_ref'])['facts']
            keys={(r['source_id'],r['object_id'],r['field']) for r in old}
            fact_ids=[f['fact_id'] for f in facts if (f['evidence']['source_id'],f['evidence']['object_id'],f['field']) in keys and f['evidence']['raw_available']]
        refs=self.sources.resolve(item['project_ref'],fact_ids)
        self.sources.refresh(refs)
        # Capture values, references, access epoch and retention deadline in one
        # source transaction. Outbox creation later checks this SAME epoch.
        with self.s.lock:
            with self.sources.info.store.transaction() as information_db:
                refs=self.sources.resolve(item['project_ref'],fact_ids,connection=information_db)
                by_id={f['fact_id']:f for f in self.sources.facts(item['project_ref'],connection=information_db)['facts']}
                access_version=self.sources.info.store.access_version()
                expiries=[information_db.execute('SELECT raw_expires_at_ms FROM observations WHERE source_id=? AND object_id=? AND version=?',
                    (ref['source_id'],ref['object_id'],ref['version'])).fetchone()[0]/1000 for ref in refs]
        if row.get('_source_access_version',access_version)!=access_version:raise BoundaryError('SOURCE_ACCESS_CHANGED')
        expiry=min(row['expires'],self.s.clock()+600,*expiries)
        if expiry<=self.s.clock():raise BoundaryError('FOLLOWUP_RAW_EXPIRED')
        render_row=dict(row,expires=expiry,_source_access_version=access_version)
        before={canonical(r) for r in old};after={canonical(r) for r in refs}
        removed=[r for r in old if canonical(r) not in after];added=[r for r in refs if canonical(r) not in before]
        def describe(r):return r['source_id']+' / '+r['object_id']+' / '+r['field']+' / '+r['version']
        lines=['事项：'+item['description'],'本次重审仅更新本事项的当前依据；旧批准失效，未决发送历史保留。',
            '移出当前依据：']+[describe(r) for r in removed]
        if not removed:lines.append('无')
        lines+=['加入当前依据：']+[describe(r) for r in added]
        if not added:lines.append('无')
        lines+=['重审后的完整依据：']+[describe(r)+'\n'+canonical(by_id[r['fact_id']]['value']) for r in refs]
        lines+=['旧字段若不再有当前证据，将转为待核，不会自动认定完成。重审后还需新的发送预览和确认。']
        body='\n'.join(lines)
        if len(body)>16000:raise BoundaryError('FOLLOWUP_SOURCE_REVIEW_TOO_LARGE')
        snapshot={'item_id':item_id,'item_version':item['version'],'semantic_version':sem['version'],
                  'old_refs_hash':digest(old),'refs':refs,'source_access_version':access_version}
        emit=self.text_ui.publish_action if _plain else self.am.card
        return emit(render_row,object_id=item_id,version=sem['version'],snapshot=snapshot,title='重审跟进依据',body=body,
            actions=[('review_sources','确认所示依据变更')],flow='followup_source_review',expires=expiry)

    def verify_owner_review_delivery(self,row,card):
        self.actor(row)
        if self.text_ui._record(card['nonce']):
            self.text_ui.verify_delivered(row,card)
            return
        outputs=self.s.rows("SELECT * FROM outbox WHERE id=? AND state='sent'",(card['outbox_id'],))
        if not card['message_id'] or len(outputs)!=1 or outputs[0]['message_id']!=card['message_id']:
            raise BoundaryError('FOLLOWUP_REVIEW_NOT_DELIVERED')
        actual=self.am.g.reminders.port.read_card(card['message_id'])
        if (actual['message_id']!=card['message_id'] or actual['chat_id']!=row['chat']
                or actual['sender_id']!=self.am.g.config.bot_open_id or actual['sender_type'] not in ('app','bot')
                or actual.get('deleted') or actual['msg_type']!='interactive'
                or not card_readback_matches(json.loads(actual['content']) if isinstance(actual['content'],str) else actual['content'],json.loads(outputs[0]['body']))):
            raise BoundaryError('FOLLOWUP_REVIEW_NOT_VERIFIED')

    def review_sources_action(self,row,card,action):
        if action!='review_sources':raise BoundaryError('CARD_ACTION_NOT_SUPPORTED')
        self.verify_owner_review_delivery(row,card)
        snap=json.loads(card['snapshot']);item=self.am.items.get(snap['item_id']);sem=self.semantic(item['id'])
        if self.s.rows('SELECT 1 FROM action_semantic_history WHERE item_id=? AND event_id=?',
                (item['id'],'source-review/'+card['nonce'])):return '这份依据重审已处理，没有重复更改。'
        if (item['version'],sem['version'],digest(json.loads(sem['refs'])))!=(snap['item_version'],snap['semantic_version'],snap['old_refs_hash']):
            raise BoundaryError('FOLLOWUP_REVIEW_VERSION_CHANGED')
        self.sources.refresh(snap['refs'])
        fresh=self.sources.resolve(item['project_ref'],[r['fact_id'] for r in snap['refs']])
        if fresh!=snap['refs']:raise BoundaryError('FOLLOWUP_SOURCE_REVIEW_CHANGED')
        reviewed=dict(row,id='source-review/'+card['nonce'])
        with self.state_transaction() as (c,information_db):
            if self.sources.info.store.access_version()!=snap['source_access_version']:raise BoundaryError('SOURCE_ACCESS_CHANGED')
            if c.execute('SELECT 1 FROM action_semantic_history WHERE item_id=? AND event_id=?',
                    (item['id'],reviewed['id'])).fetchone():return '这份依据重审已处理，没有重复更改。'
            current=self.am.items.get(item['id'],c)
            current_sem=c.execute('SELECT * FROM action_semantics WHERE item_id=?',(item['id'],)).fetchone()
            if (current['version'],current_sem['version'],digest(json.loads(current_sem['refs'])))!=(snap['item_version'],snap['semantic_version'],snap['old_refs_hash']):
                raise BoundaryError('FOLLOWUP_REVIEW_VERSION_CHANGED')
            if self.sources.resolve(item['project_ref'],[r['fact_id'] for r in fresh],connection=information_db)!=fresh:
                raise BoundaryError('FOLLOWUP_SOURCE_REVIEW_CHANGED')
            self._bind_in_transaction(c,reviewed,current,json.loads(current_sem['identity']),fresh,'am',item['id'],None,'',
                expected_version=snap['semantic_version'],force_review=True)
        return '已重审本事项的当前依据。旧发送预览已失效；请查看事项并生成新的完整预览。发送结果未明的历史仍需对账。'

    def view(self,item_id,*,connection=None):
        item=self.am.items.get(item_id);sem=self.semantic(item_id);claims=json.loads(sem['claims'])
        facts={f['fact_id']:f for f in self.sources.facts(item['project_ref'],connection=connection)['facts']}
        usable={k for k,f in facts.items() if f['evidence']['raw_available']}
        fields={k:v['value'] for k,v in claims.items() if set(v['fact_ids'])<=usable}
        invalid=[k for k,v in claims.items() if not set(v['fact_ids'])<=usable]
        stale=[k for k,v in claims.items() if any(facts.get(fid,{}).get('stale',True) for fid in v['fact_ids'])]
        return {'item_id':item_id,'canonical_id':sem['canonical_id'],'fields':fields,'needs_review':invalid,
                'stale_fields':stale,'source_times_ms':{fid:f['evidence']['occurred_at_ms'] for fid,f in facts.items() if fid in {r['fact_id'] for r in json.loads(sem['refs'])}},
                'business_state':item['state'],'business_completed':item['state']=='closed' and bool(item['close_evidence']),
                'source_refs':json.loads(sem['refs']),'followups':self.s.rows('SELECT o.id,o.state,o.readback_state FROM reminder_occurrences o WHERE o.item_id=?',(item_id,))}

    def profile(self,user):
        if not isinstance(user,str) or not re.fullmatch(r'ou_[A-Za-z0-9_-]+',user):raise BoundaryError('FOLLOWUP_RECIPIENT_ID')
        self.am.ledger.port.authorize_user(user)
        p=self.am.ledger.port.get('/open-apis/contact/v3/users/'+user,{'user_id_type':'open_id','department_id_type':'open_department_id'}).get('user',{})
        if p.get('open_id')!=user:raise BoundaryError('FOLLOWUP_RECIPIENT_UNVERIFIED')
        result={k:p.get(k) for k in ('open_id','name','department_ids','status')}
        if not result['name']:
            names=set()
            for group_id in self.am.g.reminders.port.policy()['outbound_group_ids']:
                self.am.g.reminders.port.group(group_id)
                names.update(m['name'] for m in self.am.g.reminders.port.members(group_id) if m['id']==user and m.get('name'))
            if len(names)!=1:raise BoundaryError('FOLLOWUP_RECIPIENT_NAME_UNVERIFIED')
            result['name']=next(iter(names))
            result['name_source']='verified_existing_group_member'
        return result

    def p2p(self,user):
        profile=self.profile(user)
        # Existing platform-authenticated inbound messages establish the bot DM.
        # No contact discovery, new conversation probe, or user-to-user DM read.
        # Native natural-language and collection routes retain the original
        # authenticated incoming message ID too. Card/menu events are excluded;
        # every selected message still needs independent exact platform proof.
        rows=self.s.rows("SELECT * FROM am_inbox WHERE app=? AND tenant=? AND user=? AND chat_type='p2p' AND expires>? "
            "AND (kind IN ('text','conversation','collection') OR kind='followup_reply' AND EXISTS(SELECT 1 FROM followup_reply_links l WHERE l.message_id=am_inbox.id)) "
            "ORDER BY created DESC LIMIT 1",(self.app,self.tenant,user,self.s.clock()))
        if not rows:
            rows=self.s.rows("SELECT * FROM inbox WHERE app=? AND tenant=? AND user=? AND chat_type='p2p' AND message_id IS NOT NULL AND expires>? ORDER BY received DESC LIMIT 1",(self.app,self.tenant,user,self.s.clock()))
        if not rows:raise BoundaryError('FOLLOWUP_BOT_PRIVATE_CHAT_UNVERIFIED')
        row=rows[0];mid=row.get('message_id') or row['id']
        m=self.am.g.reminders.port.read(mid)
        if m['message_id']!=mid or m['chat_id']!=row['chat'] or m['sender_id']!=user or m['sender_type']!='user' or m.get('deleted'):raise BoundaryError('FOLLOWUP_PRIVATE_CHAT_IDENTITY')
        return {'kind':'recipient_p2p','user':user,'chat':row['chat'],'name':profile['name'],
                'tenant':self.tenant,'app':self.app,'directory_version':digest(profile),'anchor_message':mid}

    def refresh_p2p(self,target):
        fresh=self.p2p(target['user'])
        # A newer inbound message in the same verified DM does not invalidate the target.
        fresh['anchor_message']=target['anchor_message']
        if any(fresh.get(k)!=target.get(k) for k in fresh):raise BoundaryError('FOLLOWUP_RECIPIENT_CHANGED')
        return target

    def group(self,item):
        source=json.loads(item['source']);base=source.get('target')
        if not base:raise BoundaryError('FOLLOWUP_EXACT_GROUP_REQUIRED')
        group=self.am.g.reminders.port.group(base['group']['id']);bot=self.am.g.reminders.port.bot()
        members=self.am.g.reminders.port.members(group['id']);by_id={p['id']:p for p in members}
        ids=json.loads(item['assignees'])
        if any(i not in by_id for i in ids):raise BoundaryError('FOLLOWUP_ASSIGNEE_LEFT')
        for user in ids:self.profile(user)
        # Pin the full audience, not merely the mentioned engineers.
        audience=self.full_members(group['id'])
        from fs_agent.reminder_content import normalized_payload
        p=normalized_payload(item['id'],bot,group,[by_id[i] for i in ids],'待确认正文')
        return {'kind':'group','payload':p,'conversation_text':True,'audience_version':digest(audience)}

    def full_members(self,chat):
        token=None;seen=set();members=[]
        for _ in range(30):
            q={'member_id_type':'open_id','page_size':'100'}
            if token:q['page_token']=token
            data=self.am.ledger.port.get('/open-apis/im/v1/chats/'+chat+'/members',q)
            if data.get('trigger_security_conf_limit'):raise BoundaryError('FOLLOWUP_GROUP_AUDIENCE_INCOMPLETE')
            for m in data.get('items',[]):
                if m.get('member_id_type')!='open_id' or m.get('tenant_key')!=self.tenant:raise BoundaryError('FOLLOWUP_EXTERNAL_AUDIENCE')
                members.append((m['member_id'],m.get('name','')))
            if not data.get('has_more'):return sorted(members)
            token=data.get('page_token')
            if not token or token in seen:break
            seen.add(token)
        raise BoundaryError('FOLLOWUP_GROUP_AUDIENCE_INCOMPLETE')

    def recipient_gate(self,tenant,target,now):
        try:
            if tenant!=self.tenant:raise BoundaryError('FOLLOWUP_TENANT')
            rows=self.s.rows('SELECT native_target FROM followup_bindings')
            matches=[json.loads(r['native_target']) for r in rows if digest(json.loads(r['native_target']))==target.directory_version]
            if not matches:raise BoundaryError('FOLLOWUP_TARGET_UNBOUND')
            native=matches[0]
            self.port.refresh({'target':native})
            if native['kind']=='group' and digest(self.full_members(native['payload']['group']['id']))!=native['audience_version']:raise BoundaryError('FOLLOWUP_AUDIENCE_CHANGED')
            actual=native['user'] if native['kind']=='recipient_p2p' else native['payload']['group']['id']
            ids=(native['user'],) if native['kind']=='recipient_p2p' else tuple(p['id'] for p in native['payload']['people'])
            if native['kind']!=target.kind or actual!=target.target_id or tuple(sorted(ids))!=tuple(sorted(target.attention_ids)):raise BoundaryError('FOLLOWUP_RECIPIENT_MISMATCH')
            return GateResult(True)
        except Exception as e:return GateResult(False,str(e) if isinstance(e,BoundaryError) else type(e).__name__)

    def disclosure(self,ref,target,body,*,connection=None):
        query="SELECT * FROM followup_disclosures WHERE body_hash=? AND target_hash=? AND state='current'"
        args=(digest(body),digest(target))
        rows=connection.execute(query,args).fetchall() if connection is not None else self.s.rows(query,args)
        for r in rows:
            if r['actor']==self.owner and ref in json.loads(r['refs']):return digest([r['id'],r['revision'],r['refs']])
        return None

    def evidence_gate(self,tenant,owner,draft):
        try:
            if (tenant,owner)!=(self.tenant,self.owner):raise BoundaryError('FOLLOWUP_OWNER_SCOPE_REQUIRED')
            rows=self.s.rows('SELECT * FROM followup_bindings WHERE content_hash=?',(draft.evidence.versions.get('binding'),))
            if len(rows)!=1:raise BoundaryError('FOLLOWUP_BINDING_MISSING')
            b=rows[0];snap=json.loads(b['snapshot']);sem=self.semantic(b['item_id']);item=self.am.items.get(b['item_id'])
            if getattr(self,'text_ui',None) is not None:
                textual=self.s.rows('SELECT t.superseded,t.expires,p.state FROM am_followup_text_actions t '
                    'JOIN am_cards c ON c.nonce=t.nonce JOIN followup_previews p ON p.preview_id=c.object_id '
                    'AND p.tenant_id=? WHERE c.object_id=?',(self.tenant,b['preview_id']))
                if any(t['state']=='prepared' and (t['superseded'] or t['expires']<=self.s.clock()) for t in textual):
                    raise BoundaryError('FOLLOWUP_TEXT_PREVIEW_CHANGED')
            if b['invalidated'] or item['paused'] or item['state'] in ('closed','cancelled'):raise BoundaryError('FOLLOWUP_STOPPED')
            if snap['body']!=draft.body or snap['canonical_id']!=draft.task_id:raise BoundaryError('FOLLOWUP_BODY_OR_TASK_CHANGED')
            if draft.evidence.task_version!=digest([item['version'],sem['version']]):raise BoundaryError('FOLLOWUP_TASK_VERSION_CHANGED')
            refs=json.loads(sem['refs']);target=json.loads(b['native_target'])
            if refs!=snap['refs']:raise BoundaryError('FOLLOWUP_DEPENDENCY_SET_CHANGED')
            live=self.sources.snapshot(item['project_ref'],refs,target,draft.body,live=True)
            if live!=snap['dependencies']:raise BoundaryError('FOLLOWUP_DEPENDENCY_CHANGED')
            expected=EvidenceSnapshot(dict(live['versions'],binding=b['content_hash']),dict(live['acl_versions'],binding=b['content_hash']),
                                      digest([item['version'],sem['version']]),(digest(target),))
            if draft.evidence!=expected:raise BoundaryError('FOLLOWUP_EVIDENCE_INCOMPLETE')
            if self.legacy_uncertain(item['id'],sem):raise BoundaryError('FOLLOWUP_PRIOR_DELIVERY_UNCERTAIN')
            for adopted in self.s.rows('SELECT a.occurrence_id FROM followup_adoptions a JOIN followup_intents i ON i.intent_id=a.intent_id WHERE i.preview_id=?',(b['preview_id'],)):
                o=self.scheduler.occurrence(adopted['occurrence_id']);p=self.scheduler.plan(o['plan_id'])
                if o['cancel_requested'] or p['state']!='active' or p['version']!=o['plan_version']:
                    raise BoundaryError('FOLLOWUP_NATIVE_PLAN_STOPPED')
            return GateResult(True)
        except Exception as e:return GateResult(False,str(e) if isinstance(e,BoundaryError) else type(e).__name__)

    def legacy_uncertain(self,item_id,sem=None):
        sem=sem or self.semantic(item_id)
        if any(r.get('state') in ('unknown','sending','accepted','legacy_unknown') for r in json.loads(sem['legacy_history'])):return True
        if self.s.rows("SELECT d.id FROM followup_legacy_reminders m JOIN reminder_drafts d ON d.id=m.draft_id WHERE m.item_id=? AND (d.state IN ('sending','unknown') OR d.state='sent' AND COALESCE(d.readback_state,'pending')!='verified')",(item_id,)):return True
        return bool(self.s.rows("SELECT o.id FROM reminder_occurrences o LEFT JOIN followup_adoptions a ON a.occurrence_id=o.id WHERE o.item_id=? AND a.intent_id IS NULL AND (o.state IN ('sending','unknown') OR o.state='sent' AND COALESCE(o.readback_state,'pending')!='verified')",(item_id,)))

    def prepare(self,row,item_id,channel='group',*,purpose='request_progress',recipient=None,body=None,new_round=False):
        actor=self.actor(row);item=self.am.items.get(item_id)
        if self.incomplete_import(item_id):raise BoundaryError('FOLLOWUP_IMPORT_NOT_ADOPTED')
        if not self.s.rows('SELECT 1 FROM action_semantics WHERE item_id=?',(item_id,)):self.bind(row,item_id)
        sem=self.semantic(item_id)
        if not new_round:
            known=any(h.get('state')=='sent' for h in json.loads(sem['legacy_history']))
            known=known or bool(self.s.rows("SELECT o.id FROM reminder_occurrences o LEFT JOIN followup_adoptions a ON a.occurrence_id=o.id WHERE o.item_id=? AND a.intent_id IS NULL AND o.state='sent'",(item_id,)))
            known=known or bool(self.s.rows("SELECT d.id FROM followup_legacy_reminders m JOIN reminder_drafts d ON d.id=m.draft_id WHERE m.item_id=? AND d.state='sent'",(item_id,)))
            if known:raise BoundaryError('FOLLOWUP_PRIOR_ROUND_REVIEW_REQUIRED')
        if purpose not in PURPOSES:raise BoundaryError('FOLLOWUP_PURPOSE_INVALID')
        if channel=='group':target=self.group(item)
        elif channel=='recipient_p2p':
            ids=json.loads(item['assignees'])
            recipient=recipient or (ids[0] if len(ids)==1 else None)
            if recipient not in ids:raise BoundaryError('FOLLOWUP_EXACT_ASSIGNEE_REQUIRED')
            target=self.p2p(recipient)
            original_group=json.loads(item['source']).get('target',{}).get('group',{}).get('id')
            if original_group:target['disclosure_group']=original_group
        else:raise BoundaryError('FOLLOWUP_CHANNEL_INVALID')
        body=body or '关于“'+item['description']+'”，'+PURPOSES[purpose]
        if not isinstance(body,str) or not body.strip() or len(body)>4000 or re.search(r'<\s*(?:at|mention)\b',body,re.I):raise BoundaryError('FOLLOWUP_BODY_INVALID')
        refs=json.loads(sem['refs'])
        # This is the disclosure proposal accompanying the exact owner preview;
        # activation of a send still requires the delivered-preview confirmation.
        policy=digest([item_id,refs,target,body])
        self.s.execute("INSERT OR IGNORE INTO followup_disclosures VALUES(?,?,?,?,?,1,'current',?,?)",
            (policy,item_id,digest(body),digest(target),canonical(refs),self.owner,self.s.clock()))
        dependencies=self.sources.snapshot(item['project_ref'],refs,target,body,live=True)
        snapshot={'body':body,'refs':refs,'dependencies':dependencies,'canonical_id':sem['canonical_id']}
        placeholder='capture_'+uuid.uuid4().hex;binding=digest([snapshot,target,item['version'],sem['version'],placeholder])
        self.s.execute("INSERT INTO followup_bindings VALUES(?,?,?,?,?,?,NULL,0)",
            (placeholder,item_id,canonical(target),canonical(snapshot),'',binding))
        # The placeholder is a binding capture, not an executable preview.
        target_id=target['user'] if channel=='recipient_p2p' else target['payload']['group']['id']
        attention=(target['user'],) if channel=='recipient_p2p' else tuple(p['id'] for p in target['payload']['people'])
        now=self.s.clock();until=now+600
        episode=self.coordinator.register_episode(tenant_id=self.tenant,task_id=sem['canonical_id'],purpose='request_update',
            trigger_key='owner_request:'+row['id'] if new_round else 'native_item_decision:'+str(item['decision_version']))
        replacement=None
        for old in self.s.rows("SELECT * FROM followup_intents WHERE tenant_id=? AND crossed_send_boundary=0 AND state IN ('blocked','paused','revoked','failed') ORDER BY created_at DESC",(self.tenant,)):
            prior=_draft_load(old['draft_json'])
            if prior.task_id==sem['canonical_id'] and prior.episode_id==episode and set(prior.target.attention_ids)==set(attention):
                replacement=old['intent_id'];break
        # Explicit single immediate window displayed below; no recurring/business-day claim.
        window=SendWindow(utc(now),utc(until),(datetime.fromtimestamp(now,TZ).date().isoformat(),),
                          allow_outside_hours=True,calendar_version='owner_exact_single_window')
        draft=Draft(sem['canonical_id'],'request_update',episode,body,
            Target(channel,target_id,attention,attention if channel=='group' else (),digest(target),utc(until)),
            EvidenceSnapshot(dict(dependencies['versions'],binding=binding),dict(dependencies['acl_versions'],binding=binding),
                             digest([item['version'],sem['version']]),(digest(target),)),window,supersedes_intent_id=replacement)
        try:preview=self.coordinator.create_preview(actor,draft)
        except Exception:
            self.s.execute('DELETE FROM followup_bindings WHERE preview_id=?',(placeholder,));raise
        self.s.execute('UPDATE followup_bindings SET preview_id=?,token=? WHERE preview_id=?',(preview.preview_id,preview.token,placeholder))
        return preview,target

    def preview_text(self,row,item_id,channel='group',**kwargs):
        return self.preview_card(row,item_id,channel,_plain=True,**kwargs)

    def source_review_text(self,row,item_id,fact_ids=None):
        return self.source_review_card(row,item_id,fact_ids,_plain=True)

    def candidate_text(self,row,ident):
        return self.candidate_card(row,ident,_plain=True)

    def preview_card(self,row,item_id,channel='group',*,_plain=False,**kwargs):
        preview,target=self.prepare(row,item_id,channel,**kwargs)
        who=(target['name']+'（机器人私聊）') if channel=='recipient_p2p' else target['payload']['group']['name']+'；@ '+'、'.join(p['name'] for p in target['payload']['people'])
        text='对象：'+who+'\n正文：\n'+preview.draft.body+'\n次数：本次 1 条\n时间：确认后立即发送（含当前时段），预览有效至 '+preview.expires_at.astimezone(TZ).strftime('%H:%M:%S')+'\n后续：核对发送记录，等待反馈；结果未知时不重发。\n不附文件；发送和回复均不代表业务完成。'
        if preview.draft.supersedes_intent_id:text+='\n确认本次将替代之前已停止、且尚未发送的跟进。'
        rows=(PreviewRowBinding(preview.preview_id,preview.content_hash,preview.token),)
        snapshot={'preview_id':preview.preview_id,'manifest':manifest_hash(rows),'rows':[[preview.preview_id,preview.content_hash]],'body':preview.draft.body,'target':target}
        emit=self.text_ui.publish_action if _plain else self.am.card
        nonce=emit(row,object_id=preview.preview_id,version=1,snapshot=snapshot,title='本次跟进',body=text,
            actions=[('confirm_send_once','确认并发送本次'),('view','查看依据'),('modify','修改'),('stop','暂不跟进')],
            flow='followup',expires=preview.expires_at.timestamp())
        self.s.execute('UPDATE followup_bindings SET nonce=? WHERE preview_id=?',(nonce,preview.preview_id))
        return nonce

    def read_delivered(self,tenant,view):
        cards=self.s.rows("SELECT * FROM am_cards WHERE nonce=? AND flow='followup'",(view,))
        if tenant!=self.tenant or len(cards)!=1:raise FollowupError('preview_not_delivered')
        c=cards[0];outputs=self.s.rows("SELECT * FROM outbox WHERE id=? AND state='sent'",(c['outbox_id'],))
        if not c['message_id'] or len(outputs)!=1 or outputs[0]['message_id']!=c['message_id']:raise FollowupError('preview_not_delivered')
        out=outputs[0];m=self.am.g.reminders.port.read_card(c['message_id'])
        expected=json.loads(out['body'])
        if (m['message_id']!=c['message_id'] or m['chat_id']!=c['chat'] or m['sender_id']!=self.am.g.config.bot_open_id
                or m['sender_type'] not in ('app','bot') or m['msg_type']!='interactive' or m['deleted']
                or not card_readback_matches(json.loads(m['content']) if isinstance(m['content'],str) else m['content'],expected)):
            raise FollowupError('preview_delivery_not_verified')
        b=self.s.rows('SELECT * FROM followup_bindings WHERE preview_id=?',(c['object_id'],))[0]
        if b['invalidated']:raise FollowupError('preview_invalidated')
        snap=json.loads(c['snapshot']);pairs=snap['rows']
        rows=tuple(PreviewRowBinding(p[0],p[1],b['token']) for p in pairs)
        return DeliveredPreview(self.tenant,c['actor'],c['chat'],c['message_id'],view,rows,utc(m['created_at']),utc(c['expires']),out['id'],True)

    def card_action(self,row,card,action):
        actor=self.actor(row);b=self.s.rows('SELECT * FROM followup_bindings WHERE preview_id=?',(card['object_id'],))[0]
        if action=='view':
            view=self.view(b['item_id'])
            return '事项：'+self.am.items.get(b['item_id'])['description']+'\n证据条数：'+str(len(view['source_refs']))+'\n'+('部分依据已变化，需重核。' if view['needs_review'] else '实际发送前仍会核对来源版本、对象及披露范围。')+'\n业务完成：'+('已核验关闭' if view['business_completed'] else '尚未核验完成')
        if action in ('stop','modify'):
            self.s.execute('UPDATE followup_bindings SET invalidated=1 WHERE preview_id=?',(card['object_id'],))
            intents=self.s.rows('SELECT intent_id FROM followup_intents WHERE preview_id=?',(card['object_id'],))
            for i in intents:self.coordinator.revoke(actor,i['intent_id'])
            if action=='modify':
                self.edit_form(row,b['item_id'],json.loads(b['native_target'])['kind'],json.loads(b['snapshot'])['body'])
                return None
            return '原预览已停止。请重新选择对象和正文生成预览。' if not intents else '已停止未发送部分；已进入发送过程的结果保留核对。'
        if action!='confirm_send_once':raise BoundaryError('FOLLOWUP_CONFIRM_ACTION_REQUIRED')
        body=json.loads(row['body']);snapshot=json.loads(card['snapshot'])
        callback=VerifiedCallback(self.tenant,self.app,row['user'],row['chat'],body['source_message'],row['id'],utc(row['created']),
            card['nonce'],snapshot['manifest'],(card['object_id'],),action)
        results=self.confirmation.confirm(actor,callback)
        return self.approval_result_text(results)

    def approval_result_text(self,results):
        lines=[]
        for r in results:
            if r.error:
                lines.append('未发送：'+{'prior_delivery_uncertain':'此前的发送结果尚未核实。',
                    'evidence_blocked':'依据、权限或事项状态已变化，请重新核对。','recipient_blocked':'对象或可见范围需要重新核对。',
                    'preview_expired':'这份预览已过期，请生成新预览。'}.get(r.error,'本次确认未通过，请重新查看完整预览。'));continue
            if r.result.status=='already_reserved':lines.append('本轮已有跟进记录，本次新内容未发送。');continue
            o=self.adopt(r.result.intent_id)
            lines.append('已确认本次发送，等待原 AM 执行。' if o['state']=='ready' else self.status(o))
        return '\n'.join(lines)

    def adopt(self,intent_id):
        i=self.s.rows('SELECT * FROM followup_intents WHERE intent_id=? AND tenant_id=?',(intent_id,self.tenant))[0]
        b=self.s.rows('SELECT * FROM followup_bindings WHERE preview_id=?',(i['preview_id'],))[0]
        draft=_draft_load(i['draft_json']);target=json.loads(b['native_target'])
        plan='fu_plan_'+intent_id;occ='fu_send_'+intent_id;now=self.s.clock()
        payload={'body':draft.body,'target':target,'item_id':b['item_id'],'item_version':self.am.items.get(b['item_id'])['version'],'followup_intent':intent_id}
        payload=self.port.prepare(payload,occ)
        expiry=draft.window.expires_at.timestamp();scheduled=draft.window.not_before.timestamp()
        with self.s.tx() as c:
            old=c.execute('SELECT * FROM followup_adoptions WHERE intent_id=?',(intent_id,)).fetchone()
            if old:return self.scheduler.occurrence(old['occurrence_id'],c)
            # Only the exact approved native version can be adopted. A crash here
            # rolls back plan, approval, occurrence and mapping together.
            current=self.am.items.get(b['item_id'],c);sem=self.semantic(b['item_id'])
            if digest([current['version'],sem['version']])!=draft.evidence.task_version:raise BoundaryError('FOLLOWUP_ITEM_CHANGED')
            approval='fu_approval_'+intent_id
            rule=canonical({'at':scheduled,'quiet_policy':'exact'})
            c.execute("INSERT INTO reminder_plans VALUES(?,1,?,?,'once',?,?,?,'[]','preview_each','active',?,?,600,?,?,NULL,?)",
                (plan,b['item_id'],self.owner,rule,canonical(target),draft.body,float('inf'),expiry,approval,digest(payload),now))
            c.execute('INSERT INTO action_approvals VALUES(?,?,?,?,?,?,?,?,?)',(approval,'occurrence',occ,1,self.owner,canonical(payload),digest(payload),expiry,now))
            c.execute("INSERT INTO reminder_occurrences(id,plan_id,plan_version,scheduled_at,item_id,item_version,approval_id,payload,hash,request_key,deadline,state,attempts,next_attempt) VALUES(?,?,1,?,?,?,?,?,?,?,?,'ready',0,0)",
                (occ,plan,scheduled,b['item_id'],current['version'],approval,canonical(payload),digest(payload),i['request_uuid'],expiry))
            c.execute('INSERT INTO followup_adoptions VALUES(?,?,?,?,?)',(intent_id,b['item_id'],plan,occ,i['request_uuid']))
            return self.scheduler.occurrence(occ,c)

    def recover_adoptions(self):
        changed=self.recovery.recover()
        changed=self.operator_step() or changed
        for i in self.s.rows("SELECT f.intent_id FROM followup_intents f LEFT JOIN followup_adoptions a ON a.intent_id=f.intent_id WHERE f.state='queued' AND a.intent_id IS NULL AND f.tenant_id=?",(self.tenant,)):
            try:self.adopt(i['intent_id']);changed=True
            except Exception:pass  # retained admission, never a fresh key or send
        return changed

    def operator_step(self):
        """Explicit same-account local import/inspection, in the original loop.

        No operation accepts send approval or claims to be a platform callback.
        Preview sends only to configured owner; target messages still need the
        actual delivered card callback. Results stay in the private service dir.
        """
        paths=sorted(self.operator_queue.glob('*.request.json'))[:1]
        if not paths:return False
        p=paths[0];st=p.lstat()
        if p.is_symlink() or st.st_uid!=os.getuid() or st.st_mode&0o077 or st.st_size>64000:return False
        claim=p.with_suffix('.claimed')
        try:p.rename(claim)
        except FileNotFoundError:return False
        result={'ok':False,'new_engineer_sends':0}
        try:
            request=json.loads(claim.read_text())
            if type(request) is not dict or request.get('expires_at',0)<=self.s.clock():raise BoundaryError('FOLLOWUP_OPERATOR_EXPIRED')
            origin=self.am.owner_origin()
            if origin is None:raise BoundaryError('FOLLOWUP_OWNER_CHAT_UNAVAILABLE')
            row=dict(origin,id='local-followup/'+p.stem,kind='text',app=self.app,tenant=self.tenant,user=self.owner,chat_type='p2p',created=self.s.clock())
            self.actor(row);op=request.get('operation')
            if op=='inspect':
                projects=self.am.projects(row)
                groups=[self.am.g.reminders.port.group(g) for g in self.am.g.reminders.port.policy()['outbound_group_ids']]
                result.update(projects=[{'key':x['key'],'title':x['title'],'identity_status':x['identity_status']} for x in projects],groups=groups,
                    items=self.s.rows("SELECT id,project_ref,description,assignees,state FROM action_items WHERE state NOT IN ('closed','cancelled')"))
                for group in groups:
                    group['people']=self.am.g.reminders.port.members(group['id'])
            elif op=='preview':
                nonce=self.preview_text(row,request['item_id'],request.get('channel','group'),body=request.get('body'),recipient=request.get('recipient'))
                result.update(nonce=nonce,preview_queued=True)
            elif op=='validation_preview':
                owner_test=request.get('owner_self_test',False)
                item=self.validation_item(row,request['group_id'],request['recipient'],request['channel'],owner_self_test=owner_test)
                body=('[AM联调测试] 这是一条经本人确认后发送的群内测试提醒，无需处理真实项目事项。收到后可回复“收到”。'
                      if request['channel']=='group' else '[AM联调测试] 这是一条经本人确认后发送的机器人私聊测试消息，无需处理真实项目事项。收到后可回复“收到”。')
                if owner_test:
                    body='[AM本人联调测试] 这是一条发给本人、经本人确认后发送的机器人私聊测试消息，无需处理真实项目事项。请回复本消息“收到”，再回复一条标注为联调测试的进展；反馈不代表业务验收。'
                nonce=self.preview_text(row,item['id'],request['channel'],body=body,recipient=request['recipient'])
                result.update(item_id=item['id'],nonce=nonce,preview_queued=True,synthetic_business_content=True,owner_self_test=owner_test)
            elif op=='import':
                result['candidate_id']=self.import_candidate(row,request['candidate'])
            else:raise BoundaryError('FOLLOWUP_OPERATOR_OPERATION_INVALID')
            result['ok']=True
        except Exception as exc:result['error']=str(exc) if isinstance(exc,(BoundaryError,FollowupError)) else type(exc).__name__
        result['finished_at']=self.s.clock()
        out=p.with_name(p.name.replace('.request.json','.result.json'))
        fd=os.open(out,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)
        with os.fdopen(fd,'w') as stream:json.dump(result,stream,ensure_ascii=False)
        return True

    def validation_item(self,row,group_id,recipient,channel,*,owner_self_test=False):
        """Explicit local operator request only; isolated synthetic business identity.

        Reuses ActionItems/FactEngine in the original process. Never attaches a
        synthetic result to a real project or an existing business action.
        """
        self.actor(row)
        if type(owner_self_test) is not bool or channel not in ('group','recipient_p2p'):
            raise BoundaryError('FOLLOWUP_VALIDATION_TARGET')
        if (owner_self_test and (recipient!=self.owner or channel!='recipient_p2p')) or (not owner_self_test and recipient==self.owner):
            raise BoundaryError('FOLLOWUP_VALIDATION_TARGET')
        group=self.am.g.reminders.port.group(group_id);bot=self.am.g.reminders.port.bot()
        people=[p for p in self.am.g.reminders.port.members(group_id) if p['id']==recipient]
        if len(people)!=1:raise BoundaryError('FOLLOWUP_VALIDATION_TARGET')
        self.profile(recipient)
        if channel=='recipient_p2p':self.p2p(recipient)
        from am_info_sync.models import Source,Principal
        # The explicitly requested owner test has its own identity. It must
        # never replace an engineer's item or count as independent-person UAT.
        audience=('owner:' if owner_self_test else '')+channel
        info=self.sources.info;subject='validation:AM-FOLLOWUP-20261102-01:'+audience
        source_id='followup-validation:'+audience
        source=Source(source_id,'manual',subject,self.tenant,self.owner,enabled=True,output_chats=(group_id,),
            settings={'subject_id':subject,'host_managed':True,'retain_derived':False})
        info.store.register_source(source)
        text='AM-FOLLOWUP R1 系统联调测试，不属于真实业务项目；仅核对消息渠道与回执。'
        recorded=info.service.offline(source_id,Principal(self.owner,self.tenant),text,subject,
            [{'field':'validation_scope','value':text,'quote':text,'confirmed':True}],idempotency_key='r1-validation')
        observation=info.store.get_observation(source_id,recorded['object_id'],recorded['version'])
        processed=info.service.facts.process(observation,[],extractor_version='explicit-system-validation-v1')
        item=self.am.items.create(self.owner,'AM-FOLLOWUP-R1-validation/'+audience,project_ref=subject,
            description=('AM 本人联调测试（' if owner_self_test else 'AM 联调测试（')+('群内 @' if channel=='group' else '机器人私聊')+'）',requirements='仅测试发送与回读，不代表业务完成',
            assignees=[recipient],source={'kind':'synthetic_validation','project_title':'AM 系统联调测试',
                'target':{'bot':bot,'group':group,'people':people}},deadline_kind='unspecified')
        refs=self.sources.resolve(subject,processed['fact_ids'])
        self.bind(row,item['id'],refs=refs)
        return item

    def legacy_allowed(self,c,o):
        # Once an item has joined shared admission, all its old scheduled sends
        # are fenced. Their receipts remain visible and their unknowns block us.
        return not self.incomplete_import(o['item_id'],c) and not c.execute('SELECT 1 FROM action_semantics WHERE item_id=?',(o['item_id'],)).fetchone()

    def legacy_reminder_allowed(self,draft):
        return not self.s.rows('SELECT 1 FROM followup_legacy_reminders WHERE draft_id=?',(draft['id'],))

    def bind_legacy_reminder(self,row,item_id,draft_id):
        self.actor(row);self.semantic(item_id)
        d=self.am.g.reminders._internal(draft_id);origin=json.loads(d['origin'])
        if (origin['app'],origin['tenant'],origin['user'])!=(self.app,self.tenant,self.owner):raise BoundaryError('FOLLOWUP_LEGACY_SCOPE')
        old=self.s.rows('SELECT item_id FROM followup_legacy_reminders WHERE draft_id=?',(draft_id,))
        if old and old[0]['item_id']!=item_id:raise BoundaryError('FOLLOWUP_LEGACY_ALREADY_BOUND')
        self.s.execute('INSERT OR IGNORE INTO followup_legacy_reminders VALUES(?,?,?)',(draft_id,item_id,row['id']))

    def edit_form(self,row,item_id,channel,body):
        self.actor(row)
        item=self.am.items.get(item_id)
        self.text_ui._remember(row,[item['id']])
        return self.text_ui.publish(row,'原预览已失效。直接告诉我想改哪里，我会重新展示接收人和完整正文，确认后再发。')

    def ingest_reply(self,payload):
        h=payload.get('header',{});e=payload.get('event',{});m=e.get('message',{});sender=e.get('sender',{})
        if (h.get('app_id'),h.get('tenant_key'),h.get('event_type'))!=(self.app,self.tenant,'im.message.receive_v1'):return None
        if sender.get('sender_type')!='user' or sender.get('tenant_key')!=self.tenant or not m.get('parent_id'):return None
        found=self.s.rows('SELECT o.* FROM reminder_occurrences o JOIN followup_adoptions a ON a.occurrence_id=o.id WHERE o.message_id=?',(m['parent_id'],))
        if len(found)!=1:return None
        o=found[0];p=json.loads(o['payload']);target=p['target'];user=sender.get('sender_id',{}).get('open_id')
        chat=target['chat'] if target['kind']=='recipient_p2p' else target['payload']['group']['id']
        if user not in json.loads(self.am.items.get(o['item_id'])['assignees']) or m.get('chat_id')!=chat:return None
        if m.get('message_type') not in ('text','post'):return True,False
        if m.get('chat_type')!=('p2p' if target['kind']=='recipient_p2p' else 'group'):return True,False
        from fs_agent.message_text import normalize_message
        text=normalize_message(m,bot_open_id=self.am.g.config.bot_open_id,max_chars=4000).text.strip()
        created=int(m['create_time'])/1000
        if created+86400<=self.s.clock() or created>self.s.clock()+60:return True,False
        value={'occurrence':o['id'],'source_message':m['parent_id'],'text':text}
        with self.s.tx() as c:
            inserted=c.execute("INSERT OR IGNORE INTO am_inbox VALUES(?,?,?,?,?,?,?,?,?,?,'received',NULL)",
                (m['message_id'],self.app,self.tenant,user,chat,m['chat_type'],created,min(created,self.s.clock())+86400,canonical(value),'followup_reply')).rowcount
        return True,bool(inserted)

    def reply(self,row):
        v=json.loads(row['body']);o=self.scheduler.occurrence(v['occurrence']);p=json.loads(o['payload']);target=p['target']
        chat=target['chat'] if target['kind']=='recipient_p2p' else target['payload']['group']['id']
        expected_type='p2p' if target['kind']=='recipient_p2p' else 'group'
        if ((row.get('app'),row.get('tenant'),row.get('chat'),row.get('chat_type'),row.get('kind'))
                !=(self.app,self.tenant,chat,expected_type,'followup_reply') or row.get('expires',0)<=self.s.clock()):
            raise BoundaryError('FOLLOWUP_REPLY_SCOPE_INVALID')
        if row['user'] not in json.loads(self.am.items.get(o['item_id'])['assignees']) or v['source_message']!=o['message_id']:raise BoundaryError('FOLLOWUP_REPLY_UNBOUND')
        if target['kind']=='recipient_p2p':
            if row['user']!=target['user']:raise BoundaryError('FOLLOWUP_REPLY_UNBOUND')
            self.profile(row['user'])
        else:self.am.ledger.port.guard(row['chat'],row['user'])
        # The current message proves this conversation. Never reread an expired
        # greeting or require the engineer to send another unrelated message.
        actual=self.am.g.reminders.port.read(row['id'])
        if (actual.get('message_id')!=row['id'] or actual.get('chat_id')!=chat
                or actual.get('sender_id')!=row['user'] or actual.get('sender_type')!='user'
                or actual.get('parent_id')!=o['message_id'] or actual.get('deleted')
                or actual.get('msg_type') not in ('text','post')
                or abs(actual.get('created_at',0)-row['created'])>1
                or actual.get('created_at',0)+86400<=self.s.clock()):
            raise BoundaryError('FOLLOWUP_CURRENT_REPLY_NOT_VERIFIED')
        from fs_agent.message_text import normalize_message
        mentions=[dict(m,id=m['id'] if isinstance(m.get('id'),dict) else {'open_id':m.get('id','')}) for m in actual.get('mentions',[])]
        received={'message_type':actual['msg_type'],'content':actual['content'] if isinstance(actual['content'],str) else canonical(actual['content']),
                  'mentions':mentions}
        if normalize_message(received,bot_open_id=self.am.g.config.bot_open_id,max_chars=4000).text.strip()!=v['text']:
            raise BoundaryError('FOLLOWUP_CURRENT_REPLY_CHANGED')
        kind='acknowledged' if v['text'].strip().lower() in ('收到','好的','了解','ok','谢谢') else 'reported_update'
        with self.s.tx() as c:
            if row['expires']<=self.s.clock():raise BoundaryError('FOLLOWUP_REPLY_EXPIRED')
            current=self.am.items.get(o['item_id'],c)
            if row['user'] not in json.loads(current['assignees']):raise BoundaryError('FOLLOWUP_REPLY_UNBOUND')
            old=c.execute('SELECT * FROM followup_reply_links WHERE message_id=?',(row['id'],)).fetchone()
            if old:
                if (old['item_id'],old['occurrence_id'],old['actor'],old['kind'])!=(o['item_id'],o['id'],row['user'],kind):
                    raise BoundaryError('FOLLOWUP_REPLY_BINDING_CHANGED')
                return None
            result=self.am.items.feedback(row['user'],'followup-reply/'+row['id'],o['item_id'],o['item_version'],
                'received' if kind=='acknowledged' else 'reported_update',v['text'],
                {'message_id':row['id'],'occurrence':o['id'],'raw_expires_at':row['expires'],'verification':'current_exact_reply'},
                submission_key=row['id'],conn=c)
            c.execute('INSERT INTO followup_reply_links VALUES(?,?,?,?,?,?)',
                (row['id'],o['item_id'],o['id'],row['user'],kind,self.s.clock()))
        origin=self.am.owner_origin()
        if origin:
            summary='负责人已回应。' if kind=='acknowledged' else '收到负责人自报进展，内容尚待核对；请打开反馈记录查看有效期内的原文。'
            self.am.ux_feedback_notice(origin,result['item'],summary,result['business_event_id'])
        return None

    def dispatch_occurrence(self,ident):
        mappings=self.s.rows('SELECT * FROM followup_adoptions WHERE occurrence_id=?',(ident,))
        if not mappings:return None
        mapping=mappings[0];o=self.scheduler.occurrence(ident)
        if o['state'] not in ('ready','confirmed_not_sent'):return False
        def guard():
            if not self.scheduler.enabled():return False
            with self.s.tx() as c:
                fresh=self.scheduler.occurrence(ident,c);p=self.scheduler.plan(fresh['plan_id'],c)
                return fresh['state']=='sending' and self.scheduler.current(c,fresh,p) and self.scheduler.approval_valid(c,fresh,p)
        host=self
        class Sender:
            def dispatch(self,request,before_send):
                payload=json.loads(o['payload'])
                if request.request_uuid!=o['request_key'] or request.draft.body!=payload['body'] or request.intent_id!=payload['followup_intent']:
                    raise DispatchBlocked('native_mapping_changed')
                with host.s.tx() as c:
                    fresh=host.scheduler.occurrence(ident,c);p=host.scheduler.plan(fresh['plan_id'],c)
                    if not host.scheduler.current(c,fresh,p) or not host.scheduler.approval_valid(c,fresh,p):raise DispatchBlocked('native_approval_changed')
                    if fresh['state'] not in ('ready','confirmed_not_sent'):raise DispatchBlocked('native_already_claimed')
                    c.execute("UPDATE reminder_occurrences SET state='sending',send_started=?,attempts=attempts+1 WHERE id=?",(host.s.clock(),ident))
                try:
                    receipt=host.port.send(payload,o['request_key'],guard,before_send=before_send)
                    if not receipt.get('message_id'):raise SendUnknown('RECEIPT_MISSING')
                    host.s.execute("UPDATE reminder_occurrences SET state='sent',message_id=?,receipt=?,readback_state='pending' WHERE id=?",(receipt['message_id'],canonical(receipt),ident))
                    rb=host.port.reconcile(payload,o['request_key'],host.s.clock(),receipt['message_id'])
                    if rb and rb.get('verified') is True and rb.get('message_id')==receipt['message_id']:
                        host.s.execute("UPDATE reminder_occurrences SET readback_state='verified',receipt=? WHERE id=?",(canonical(rb),ident))
                        return SendOutcome('confirmed',ident,receipt['message_id'],host.readback(ident,request))
                    return SendOutcome('accepted',ident,receipt['message_id'])
                except SendRejected:
                    host.s.execute("UPDATE reminder_occurrences SET state='cancelled',error='DEFINITELY_NOT_SENT' WHERE id=?",(ident,))
                    return SendOutcome('failed',ident,reason='native_send_rejected',definitive_no_send=True)
                except DispatchBlocked:raise
                except Exception:
                    host.s.execute("UPDATE reminder_occurrences SET state='unknown',readback_state='pending',error='SEND_UNKNOWN' WHERE id=? AND state='sending'",(ident,))
                    return SendOutcome('unknown',ident)
        intent=self.coordinator.dispatch_once(self.tenant,mapping['intent_id'],Sender(),worker_id='original_am_scheduler')
        if intent.state in ('blocked','paused','revoked','failed'):
            self.s.execute("UPDATE reminder_occurrences SET state='cancelled',error=? WHERE id=? AND state IN ('ready','sending','confirmed_not_sent')",(intent.reason,ident))
        elif intent.state=='unknown':
            self.s.execute("UPDATE reminder_occurrences SET state='unknown',readback_state='pending' WHERE id=? AND state IN ('ready','sending','confirmed_not_sent')",(ident,))
        fresh=self.scheduler.occurrence(ident)
        # Scheduler True means progress, not merely ownership of this row.
        # A queued admission behind an uncertainty barrier must yield so an
        # unrelated approved occurrence can run in this same scheduler pass.
        changed=any(fresh[k]!=o[k] for k in ('state','attempts','readback_state','message_id','error'))
        if changed:self.am.business_notification('occurrence_result',ident)
        return changed

    @contextmanager
    def final_dependency_guard(self,request):
        """Fence local evidence through the original AM sending transition.

        Lock order for every FollowupSources cross-store read is:
        AM Store mutex -> Information BEGIN IMMEDIATE -> AM BEGIN IMMEDIATE.
        The coordinator opens/commits the last transaction inside this scope.
        We reserve Information's writer slot even for these reads: a WAL read
        snapshot alone would allow a revocation to commit after our last check.
        No network call is made while either database transaction is held.
        """
        rows=self.s.rows('SELECT native_target FROM followup_bindings WHERE content_hash=?',
                         (request.draft.evidence.versions.get('binding'),))
        if len(rows)!=1:raise DispatchBlocked('local_dependencies_changed')
        target=json.loads(rows[0]['native_target'])
        # This directory operation may be slow. Any source/disclosure changes
        # made while it runs are read after we acquire both local write fences.
        try:memberships=self.sources.disclosure_memberships(target)
        except BoundaryError as exc:raise DispatchBlocked('recipient_blocked',str(exc)) from None
        with self.s.lock:
            if self.s.conn.in_transaction:raise DispatchBlocked('native_transaction_already_active')
            with self.sources.info.store.transaction() as information_db:
                yield lambda native_db:self.final_dependencies_current(
                    native_db,information_db,request,target,memberships)

    def final_dependencies_current(self,c,information_db,request,target,memberships):
        """Local-only exact dependency comparison under both database fences."""
        try:
            rows=c.execute('SELECT * FROM followup_bindings WHERE content_hash=?',
                           (request.draft.evidence.versions.get('binding'),)).fetchall()
            if len(rows)!=1:return False
            binding=rows[0]
            if binding['invalidated'] or json.loads(binding['native_target'])!=target:return False
            sem=c.execute('SELECT * FROM action_semantics WHERE item_id=?',(binding['item_id'],)).fetchone()
            item=self.am.items.get(binding['item_id'],c)
            if sem is None or item['paused'] or item['state'] in ('closed','cancelled'):return False
            snapshot=json.loads(binding['snapshot']);refs=json.loads(sem['refs'])
            if (snapshot['body']!=request.draft.body or snapshot['canonical_id']!=request.draft.task_id
                    or refs!=snapshot['refs'] or digest(target)!=request.draft.target.directory_version):return False
            current=self.sources.snapshot_locked(item['project_ref'],refs,target,request.draft.body,
                information_db,native_connection=c,memberships=memberships)
            expected=EvidenceSnapshot(dict(current['versions'],binding=binding['content_hash']),
                dict(current['acl_versions'],binding=binding['content_hash']),
                digest([item['version'],sem['version']]),(digest(target),))
            return current==snapshot['dependencies'] and request.draft.evidence==expected
        except BoundaryError:return False

    def final_native_check(self,c,request):
        if not self.scheduler.enabled():return False
        mapping=c.execute('SELECT * FROM followup_adoptions WHERE intent_id=?',(request.intent_id,)).fetchone()
        if not mapping or mapping['request_uuid']!=request.request_uuid:return False
        o=self.scheduler.occurrence(mapping['occurrence_id'],c);p=self.scheduler.plan(o['plan_id'],c)
        sem=c.execute('SELECT version FROM action_semantics WHERE item_id=?',(o['item_id'],)).fetchone()
        item=self.am.items.get(o['item_id'],c)
        return (o['state']=='sending' and sem is not None and digest([item['version'],sem['version']])==request.draft.evidence.task_version
                and self.scheduler.current(c,o,p) and self.scheduler.approval_valid(c,o,p) and not self.scheduler.blocked(c,o))

    def readback(self,ident,request=None):
        return self.recovery.readback(ident)

    def reconciled(self,ident):
        return self.recovery.settle(ident)

    def status(self,o):
        if o['state']=='sent' and o['readback_state']=='verified':
            replies=self.s.rows('SELECT kind FROM followup_reply_links WHERE occurrence_id=? ORDER BY created DESC,rowid DESC',(o['id'],))
            if any(r['kind']=='reported_update' for r in replies):return '已发出并收到对方自报进展，业务结果仍待核验。'
            if replies:return '已发出，对方已回应；实际进展或交付仍待核对。'
            return '已发出并核对消息记录，正在等对方反馈。'
        if o['state'] in ('unknown','sending','sent'):return '发送结果尚未核实，暂时不会重发。'
        if o['state']=='cancelled':return '本次未继续发送；请核对最新依据与对象后重新预览。'
        return '本次跟进已登记，等待执行。'

    def candidate_card(self,row,ident,*,_plain=False):
        self.actor(row)
        records=self.s.rows('SELECT * FROM followup_candidates WHERE id=?',(ident,))
        if not records:raise BoundaryError('FOLLOWUP_CANDIDATE_MISSING')
        value=json.loads(records[0]['payload']);recoveries=self.recovery_rows(ident)
        safe=not recoveries or len(recoveries)==1 and recoveries[0]['state']=='linked'
        text=canonical(value)+'\n确认仅采用所示事项和历史，不发送工程师消息。'
        if recoveries:
            text+=('\n将接回此前导入未完成的同一原事项；保留原发送及未知历史。'
                   if safe else '\n存在冲突的旧导入事项，已停止跟进；需先核对唯一事项，不会新建第二条。')
        snapshot={'candidate':value,'recovery_hash':digest(recoveries)}
        emit=self.text_ui.publish_action if _plain else self.am.card
        return emit(row,object_id=ident,version=1,snapshot=snapshot,title='核对候选关联',body=text,
            actions=[('adopt','确认所示事项与历史关联')] if safe else [],flow='followup_import')

    def command(self,row):
        self.actor(row);q=row['body'].strip()
        if q.startswith('/followup 导入 '):
            ident=self.import_candidate(row,json.loads(q[len('/followup 导入 '):]))
            return '已保存候选，尚未建立事项或发送消息。候选编号：'+ident
        parts=q.split('｜');head=parts[0].split()
        if len(head)>=3 and head[1] in ('预览','新一轮'):
            channel='recipient_p2p' if len(head)>3 and head[3]=='私聊' else 'group'
            self.preview_text(row,head[2],channel,body=parts[1] if len(parts)==2 else None,new_round=head[1]=='新一轮')
            return None
        if len(head)==3 and head[1]=='查看':return canonical(self.view(head[2]))
        if len(head)==3 and head[1]=='重审':
            self.source_review_text(row,head[2],fact_ids=[s.strip() for s in parts[1].split(',') if s.strip()] if len(parts)==2 else None)
            return None
        if len(head)==3 and head[1]=='候选':
            self.candidate_text(row,head[2])
            return None
        if q=='/followup':return self.dashboard(row)
        return ('直接告诉我想跟进哪件事、想了解什么；需要联系对方时，我会先给你看完整预览。\n'
                '/followup 预览 事项ID 群聊|私聊｜完整正文\n生成预览不会向工程师发送。')

    def dashboard(self,row):
        self.actor(row)
        # Resolve identity before the information fence. Projection reads take
        # the same AM mutex -> Information transaction order as final sending;
        # rendering/outbox work happens only after this scope has been released.
        render_row=dict(row,_query_read=True,_query_grant=self.am.query_grant(row))
        info=self.am.information
        with self.s.lock:
            if info is None:
                text,actions,expiry=self._dashboard_content()
            else:
                with info.store.transaction() as information_db:
                    text,actions,expiry=self._dashboard_content(information_db)
                    version=info.store.access_version()
                if row.get('_source_access_version',version)!=version:
                    raise BoundaryError('SOURCE_ACCESS_CHANGED')
                render_row['_source_access_version']=version
        if expiry is not None:
            render_row['expires']=min(render_row['expires'],expiry)
            if render_row['expires']<=self.s.clock():raise BoundaryError('FOLLOWUP_RAW_EXPIRED')
        return self.text_ui.publish(render_row,'目前的待办与进展：\n\n'+text+'\n\n需要我继续看哪件事，直接告诉我就好。')

    def _dashboard_content(self,information_db=None):
        """Pure local projection; no identity lookup, model, or external calls."""
        needs=[];following=[];context=[];actions=[]
        expiry=None
        for item in self.s.rows("SELECT * FROM action_items WHERE owner=? AND state NOT IN ('closed','cancelled') ORDER BY created DESC LIMIT 12",(self.owner,)):
            receipts=self.s.rows('SELECT * FROM reminder_occurrences WHERE item_id=? ORDER BY scheduled_at DESC LIMIT 1',(item['id'],))
            text=item['description']
            if receipts and receipts[0]['state'] in ('ready','sending','unknown','sent'):
                following.append(text+'：'+self.status(receipts[0]))
            else:needs.append(text+'：'+('已暂停' if item['paused'] else '下一步与负责人待确认预览'))
            actions.append((text[:28],{'page':'item','item':item['id']}))
            if self.s.rows('SELECT 1 FROM action_semantics WHERE item_id=?',(item['id'],)):
                if information_db is None:
                    needs.append(text+'：当前来源不可读取，补充进展待核。')
                    continue
                view=self.view(item['id'],connection=information_db)
                if view['fields']:
                    sem=self.semantic(item['id']);claims=json.loads(sem['claims'])
                    shown_facts={fact for key in view['fields'] for fact in claims[key]['fact_ids']}
                    for ref in json.loads(sem['refs']):
                        if ref['fact_id'] not in shown_facts:continue
                        raw=information_db.execute('SELECT raw_expires_at_ms FROM observations WHERE source_id=? AND object_id=? AND version=?',
                            (ref['source_id'],ref['object_id'],ref['version'])).fetchone()
                        if raw is None:raise BoundaryError('FOLLOWUP_RAW_EXPIRED')
                        expires=raw[0]/1000
                        expiry=min(expiry,expires) if expiry is not None else expires
                    names={'execution':'执行','response':'回应','delivery':'本项交付','acceptance':'验收','plan_delivery':'方案交付',
                           'sample_arrival':'样机到货','release_materials':'发布材料','planned_start':'计划开测','actual_start':'实际开测',
                           'planned_release':'计划发布','actual_release':'实际发布','approval':'审批','test_overview':'测试概述',
                           'external_release':'对外版本','deadline_text':'期限','blocker':'待解决项'}
                    labels={'unknown':'待核实','planned':'计划中','in_progress':'进行中','blocked':'受阻','done':'来源称已完成',
                            'cancelled':'已取消','pending':'待处理','acknowledged':'已回应','not_required':'不适用',
                            'delivered':'已交付','accepted':'已验收','rejected':'未通过'}
                    display=[names.get(k,'补充进展')+'：'+labels.get(str(v),str(v) if isinstance(v,(str,int,float)) else '详见依据') for k,v in view['fields'].items()]
                    context.append(text+'：'+'；'.join(display))
                if view['needs_review']:needs.append(text+'：依据已变化，相关字段待核。')
                elif view['stale_fields']:context.append(text+'：保留此前已核实内容，新增变化尚未核实。')
        candidates=self.s.rows("SELECT id FROM followup_candidates WHERE state IN ('candidate','needs_review')")
        if candidates:needs.append(str(len(candidates))+' 条导入候选待核对项目、负责人及历史。')
        text='需要你决定\n'+('\n'.join(needs) or '当前没有已登记的待决定事项。')+'\n\nAM 跟进中\n'+('\n'.join(following) or '当前没有已登记的跟进。')+'\n\n供你了解\n'+('\n'.join(context) or '尚无已核实的补充进展。')
        text+='\n\n仅展示当前已登记事项，不代表所有来源已同步。'
        return text,actions,expiry
