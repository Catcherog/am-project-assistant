"""Concrete Meegle text-field execution for frozen or explicit R5 routine grants.

The current binding accepts exact target/field/before/after tuples, not arbitrary
model-supplied fields. Each write is claimed durably before submission and is
verified by a separate CLI read. An ambiguous write is never submitted twice.
"""
import argparse
import fcntl
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import stat
import subprocess
import sys
import time

from am_gateway.models import ContractError, Event, dumps, key
from integration.approved_command import ApprovedCommandExecutor
from integration.assistant_maintenance import POLICY_AUTHORITY, _formal_field, current_r5_intake_hash


def digest(value):
    return hashlib.sha256(dumps(value).encode()).hexdigest()


class MeegleOperationBridge:
    def __init__(self, config, state_path, *, invoke=None):
        self.config=config;self.path=Path(state_path).resolve();self.invoke=invoke or self._invoke
        self.path.parent.mkdir(parents=True,exist_ok=True)
        with sqlite3.connect(self.path) as db:
            db.execute('BEGIN IMMEDIATE')
            db.execute('''CREATE TABLE IF NOT EXISTS operations(
                operation_id TEXT PRIMARY KEY,payload_hash TEXT NOT NULL,state TEXT NOT NULL,
                write_calls INTEGER NOT NULL DEFAULT 0,receipt TEXT NOT NULL DEFAULT '{}')''')
            present={row[1] for row in db.execute('PRAGMA table_info(operations)')}
            for column,default in (('grant_kind','frozen'),('scope_json','{}'),('before_json','{}'),
                                   ('after_json','{}'),('authority_json','{}')):
                if column not in present:
                    db.execute("ALTER TABLE operations ADD COLUMN "+column+" TEXT NOT NULL DEFAULT '"+default+"'")
        self.path.chmod(0o600)

    def _invoke(self,args):
        binary=Path(self.config['binary'])
        if hashlib.sha256(binary.read_bytes()).hexdigest()!=self.config['binary_sha256']:
            raise ContractError('meegle_binary_changed')
        command=[str(binary),*self.config.get('profile_args',[]),*args,'--format','json']
        result=subprocess.run(command,capture_output=True,text=True,timeout=35)
        if result.returncode or len(result.stdout.encode())>2000000:
            raise ContractError('meegle_command_failed_or_unknown')
        value=json.loads(result.stdout)
        if not isinstance(value,dict) or value.get('error'):
            raise ContractError('meegle_error_response')
        return value

    def _identity(self):
        auth=self.invoke(['auth','status'])
        if auth.get('authenticated') is not True or auth.get('host')!=self.config['host']:
            raise ContractError('meegle_identity_host_changed')
        user=self.invoke(['user','me'])
        if user.get('user_key')!=self.config['user_key']:
            raise ContractError('meegle_user_changed')

    def _grant(self,request):
        if request.get('schema')!='am.approved.operation.v1' or request.get('operation') not in {'apply','readback'}:
            raise ContractError('approved_operation_required')
        payload=request['payload'];target=payload.get('target',{})
        if payload.get('action')!='update_fields' or target.get('type')!='work_item':
            raise ContractError('text_field_update_only')
        matches=[g for g in self.config.get('grants',[]) if g['project_id']==payload.get('project_id')
            and g['target_id']==target.get('id') and g['after']==payload.get('changes')]
        if not matches:
            return self._routine_grant(request)
        if len(matches)!=1:raise ContractError('no_exact_frozen_grant')
        grant=matches[0]
        if not grant.get('authorization_ref') or time.time()>grant['expires_at']:
            raise ContractError('frozen_grant_expired')
        if set(grant['before'])!=set(grant['after']) or set(grant['after'])!=set(grant['field_types']):
            raise ContractError('frozen_field_scope_mismatch')
        if any(t not in {'text','multi-text'} for t in grant['field_types'].values()) or any(not isinstance(v,str) for v in grant['after'].values()):
            raise ContractError('only_verified_text_fields_supported')
        return grant

    def _existing_operation(self,request):
        with sqlite3.connect(self.path) as db:
            db.row_factory=sqlite3.Row
            row=db.execute('SELECT * FROM operations WHERE operation_id=?',
                           (request.get('idempotency_key'),)).fetchone()
        if row and row['payload_hash']!=digest(request['payload']):
            raise ContractError('effect_payload_changed')
        return dict(row) if row else None

    def _routine_authority(self,request,grant):
        """Read the trusted runtime claim, never an approval supplied in stdin."""
        name=self.config.get('routine_database')
        if not isinstance(name,str) or not Path(name).is_absolute():
            raise ContractError('routine_runtime_database_required')
        path=Path(name).resolve()
        try:
            info=path.stat()
            if not stat.S_ISREG(info.st_mode) or info.st_uid!=os.getuid() or info.st_mode & 0o077:
                raise ContractError('private_routine_database_required')
            with sqlite3.connect(path.as_uri()+'?mode=ro',uri=True) as db:
                db.row_factory=sqlite3.Row
                db.execute('BEGIN')
                proposal=db.execute('SELECT * FROM h4_agent_proposals WHERE proposal_id=?',
                                    (request.get('proposal_id'),)).fetchone()
                record=db.execute('SELECT * FROM r5_routine_maintenance WHERE proposal_id=?',
                                  (request.get('proposal_id'),)).fetchone()
                if not proposal or not record or proposal['kind']!='project.execute' or proposal['approved_by']!=POLICY_AUTHORITY:
                    raise ContractError('standing_routine_claim_required')
                payload=request['payload']
                if (request.get('version')!=proposal['version'] or request.get('digest')!=proposal['digest']
                        or json.loads(proposal['payload'])!=payload
                        or request['digest']!=key(proposal['kind'],payload)
                        or request['idempotency_key']!='effect_'+key(proposal['proposal_id'],proposal['version'],proposal['digest'])[:40]
                        or (record['version'],record['digest'],record['effect_id'])!=(proposal['version'],proposal['digest'],request['idempotency_key'])):
                    raise ContractError('standing_routine_frozen_identity_changed')
                source=json.loads(record['source_json'])
                actions=db.execute('SELECT * FROM a5_actions WHERE action_id=?',(record['action_id'],)).fetchall()
                if len(actions)!=1:
                    raise ContractError('standing_routine_action_required')
                action=actions[0];snapshot=json.loads(action['snapshot_json'])
                if (snapshot['source_stamp']!=source or snapshot['payload']!=payload
                        or snapshot['policy']['outcome']!='allow'
                        or snapshot['context']['action_kind'] not in {'update_progress','update_todo'}
                        or source.get('field_semantics')!={field:grant['field_semantics'][field] for field in payload['changes']}):
                    raise ContractError('standing_routine_action_scope_changed')
                existing=self._existing_operation(request)
                reconciliation=request['operation']=='readback' or bool(existing and existing['write_calls'])
                if not reconciliation:
                    if proposal['status']!='executing' or record['status']!='executing' or action['state']!='executing':
                        raise ContractError('standing_routine_execution_claim_required')
                    task=db.execute('SELECT * FROM h1_tasks WHERE task_id=?',(proposal['task_id'],)).fetchone()
                    if not task:
                        raise ContractError('standing_routine_task_missing')
                    current=db.execute('''SELECT i.*,b.accepted FROM gw_items i JOIN h1_inbox b USING(item_id)
                                          WHERE i.item_id=?''',(task['item_id'],)).fetchone()
                    version=db.execute('SELECT * FROM gw_item_versions WHERE item_id=? AND version=?',
                                       (task['item_id'],task['source_version'])).fetchone()
                    policy=json.loads(task['policy_json'])
                    if (not current or not version or current['is_deleted'] or current['is_self']
                            or current['legacy_owned'] or not current['accepted']
                            or current['version']!=task['source_version'] or current['content_hash']!=version['content_hash']
                            or (task['business_id'],task['generation'],task['source_version'])!=(
                                proposal['business_id'],proposal['generation'],proposal['source_version'])
                            or source.get('source_item_id')!=task['item_id']
                            or source.get('source_content_hash')!=current['content_hash']
                            or source.get('policy_hash')!=key(task['policy_json']) or policy.get('revoked')
                            or payload['project_id'] not in policy.get('projects',[])
                            or current['chat_id'] not in policy.get('source_chats',[])
                            or current['sender_id']!=policy.get('principal')
                            or task['last_error']=='source_changed' or task['verification_state']=='conflict'):
                        raise ContractError('standing_routine_current_source_or_authority_changed')
                    intake_hash=current_r5_intake_hash(db,Event.from_dict(json.loads(version['event_json'])))
                    if intake_hash is not None and intake_hash!=source.get('r5_intake_hash'):
                        raise ContractError('r5_intake_source_changed')
        except (OSError,sqlite3.Error,KeyError,ValueError,TypeError) as exc:
            if isinstance(exc,ContractError):raise
            raise ContractError('standing_routine_authority_unavailable') from None
        return {'authority':POLICY_AUTHORITY,'action_id':record['action_id'],
                'source_ref':source.get('source_ref',''),'source_version':source.get('source_version'),
                'proposal_id':proposal['proposal_id'],'proposal_version':proposal['version'],
                'proposal_digest':proposal['digest'],'authorization_ref':grant['authorization_ref']}

    def _routine_grant(self,request):
        payload=request['payload'];changes=payload.get('changes');target=payload['target']
        if not self.config.get('routine_grants'):
            raise ContractError('no_exact_frozen_grant')
        if self.config.get('send_live') is not True:
            raise ContractError('routine_send_live_disabled')
        if not isinstance(changes,dict) or not changes or any(not isinstance(v,str) or not v.strip() for v in changes.values()):
            raise ContractError('routine_text_required_no_blanket_clear')
        matches=[]
        for candidate in self.config['routine_grants']:
            if candidate.get('enabled') is not True or candidate.get('project_id')!=payload.get('project_id') or candidate.get('target_id')!=target.get('id'):
                continue
            semantics=candidate.get('field_semantics',{});types=candidate.get('field_types',{})
            if (not isinstance(semantics,dict) or not semantics or not isinstance(types,dict)
                    or set(semantics)!=set(types) or any(value not in {'progress','todo'} for value in semantics.values())
                    or any(not isinstance(field,str) or _formal_field(field) for field in semantics)
                    or any(value not in {'text','multi-text'} for value in types.values())):
                raise ContractError('invalid_routine_field_scope')
            if not set(changes)<=set(semantics):
                continue
            if any(not isinstance(candidate.get(field),str) or not candidate[field] for field in
                   ('project_key','target_name','work_item_type','authorization_ref')):
                raise ContractError('routine_target_identity_and_authority_required')
            if 'expires_at' in candidate and time.time()>candidate['expires_at']:
                raise ContractError('routine_grant_expired')
            matches.append(candidate)
        if len(matches)!=1:
            raise ContractError('no_exact_routine_target_and_fields_grant')
        grant=dict(matches[0]);grant['after']=dict(changes)
        grant['field_types']={field:grant['field_types'][field] for field in changes}
        grant['_routine']=True
        grant['_authority']=self._routine_authority(request,grant)
        return grant

    def _read(self,grant):
        result=self.invoke(['workitem','get','--project-key',grant['project_key'],
            '--work-item-id',grant['target_id'],'--fields',dumps(sorted(grant['after']))])
        attr=result.get('work_item_attribute',{})
        if (attr.get('work_item_id')!=grant['target_id'] or attr.get('work_item_name')!=grant['target_name']
                or attr.get('owned_project',{}).get('key')!=grant['project_key']
                or attr.get('work_item_type',{}).get('key')!=grant['work_item_type']):
            raise ContractError('remote_target_identity_changed')
        fields={x['key']:x.get('value') for x in result.get('work_item_fields',[])}
        if not set(grant['after'])<=set(fields):raise ContractError('remote_fields_incomplete')
        return {k:fields[k] for k in grant['after']},result

    def handle(self,request):
        operation=request['idempotency_key'];fingerprint=digest(request['payload'])
        if not isinstance(operation,str) or not operation.startswith('effect_'):raise ContractError('effect_identity_required')
        grant=self._grant(request)
        self._identity()
        fd=os.open(self.path.with_suffix('.lock'),os.O_CREAT|os.O_RDWR,0o600)
        with os.fdopen(fd,'r+') as lock:
            fcntl.flock(lock,fcntl.LOCK_EX)
            with sqlite3.connect(self.path) as db:
                db.row_factory=sqlite3.Row
                row=db.execute('SELECT * FROM operations WHERE operation_id=?',(operation,)).fetchone()
                if row and row['payload_hash']!=fingerprint:raise ContractError('effect_payload_changed')
                if request['operation']=='readback':
                    if not row or not row['write_calls']:raise ContractError('no_submitted_effect_to_reconcile')
                    current,raw=self._read(grant)
                    if current!=grant['after']:
                        return {'status':'unknown','idempotency_key':operation,'reason':'independent_field_readback_mismatch'}
                    receipt={'status':'applied','idempotency_key':operation,'project_id':grant['project_id'],
                             'target':request['payload']['target'],'changes':current,
                             'evidence_ref':'meegle-field-readback:sha256:'+digest({'operation':operation,'target':grant['target_id'],
                                 'values':current,'update_time':raw['work_item_attribute'].get('update_time')})}
                    db.execute("UPDATE operations SET state='applied',receipt=? WHERE operation_id=?",(dumps(receipt),operation))
                    return receipt
                if row:
                    return json.loads(row['receipt']) if row['state'] in {'applied','not_applied'} else {
                        'status':'unknown','idempotency_key':operation,'reason':'already_claimed_no_replay'}
                scope={'project_id':grant['project_id'],'target':request['payload']['target']}
                if grant.get('_routine'):
                    for unresolved in db.execute("SELECT * FROM operations WHERE state NOT IN ('applied','not_applied') AND operation_id<>?",(operation,)):
                        if (json.loads(unresolved['scope_json'])==scope
                                and set(json.loads(unresolved['after_json'])) & set(grant['after'])):
                            return {'status':'not_applied','idempotency_key':operation,'reason':'prior_target_field_effect_unresolved'}
                current,_=self._read(grant)
                if grant.get('_routine'):
                    grant['before']=current
                elif current!=grant['before']:
                    return {'status':'not_applied','idempotency_key':operation,'reason':'remote_value_changed_since_preview'}
                metadata=self.invoke(['workitem','meta-fields','--project-key',grant['project_key'],
                    '--work-item-type',grant['work_item_type'],'--field-keys',dumps(sorted(grant['after']))])
                types={x['field_key']:x['field_type'] for x in metadata.get('list',[])}
                if any(types.get(k)!=v for k,v in grant['field_types'].items()):raise ContractError('remote_field_definition_changed')
                if grant.get('_routine'):
                    for field in metadata.get('list',[]):
                        if field.get('field_key') in grant['after'] and any(
                                isinstance(field.get(name),str) and _formal_field(field[name]) for name in ('field_name','name','display_name')):
                            raise ContractError('remote_formal_field_cannot_be_routine')
                    latest,_=self._read(grant)
                    if latest!=grant['before']:
                        return {'status':'not_applied','idempotency_key':operation,'reason':'remote_value_changed_during_routine_preflight'}
                    # The file lock / CLI reads may have waited while a source
                    # or scope was withdrawn. Recheck immediately before claim.
                    grant['_authority']=self._routine_authority(request,grant)
                db.execute("""INSERT INTO operations(operation_id,payload_hash,state,write_calls,
                    grant_kind,scope_json,before_json,after_json,authority_json)
                    VALUES(?,?,'submitted',1,?,?,?,?,?)""",
                    (operation,fingerprint,'routine' if grant.get('_routine') else 'frozen',dumps(scope),
                     dumps(grant['before']),dumps(grant['after']),dumps(grant.get('_authority',{}))))
                db.commit()
                try:
                    self.invoke(['workitem','update','--project-key',grant['project_key'],'--work-item-id',grant['target_id'],
                        '--fields',dumps([{'field_key':k,'field_value':v} for k,v in grant['after'].items()])])
                    receipt={'status':'submitted','idempotency_key':operation}
                except Exception:
                    receipt={'status':'unknown','idempotency_key':operation,'reason':'write_result_unconfirmed'}
                db.execute('UPDATE operations SET receipt=? WHERE operation_id=?',(dumps(receipt),operation))
                return receipt


class RoutineAwareMeegleExecutor(ApprovedCommandExecutor):
    """The R5 live flag cannot be supplied or changed by a proposal."""
    def __init__(self, *, routine_live=False, routine_grants=(), **kwargs):
        super().__init__(**kwargs)
        self.routine_live=routine_live is True
        self.routine_targets={}
        for grant in routine_grants:
            if grant.get('enabled') is True:
                if grant['target_id'] in self.routine_targets.get(grant['project_id'],{}):
                    raise ContractError('duplicate_routine_target_grant')
                self.routine_targets.setdefault(grant['project_id'],{})[grant['target_id']]=dict(grant.get('field_semantics',{}))

    def __call__(self,proposal,*,idempotency_key):
        if proposal.get('approved_by')==POLICY_AUTHORITY and not self.routine_live:
            return {'status':'not_applied','receipt':{'status':'not_applied',
                'idempotency_key':idempotency_key,'reason':'routine_send_live_disabled'}}
        return super().__call__(proposal,idempotency_key=idempotency_key)


def build_executor(bridge,host_config,client,root):
    settings=host_config.get('phase1_gateway',{})['meegle_executor']
    cfg_path=Path(settings['config_path']).resolve();cfg=json.loads(cfg_path.read_text())
    targets={}
    for grant in [*cfg.get('grants',[]),*cfg.get('routine_grants',[])]:
        targets.setdefault(grant['project_id'],{}).setdefault('work_item',[]).append(grant['target_id'])
    argv=[sys.executable,str(Path(__file__).resolve().parents[1]/'tools/meegle_operation_bridge.py'),'--config',str(cfg_path)]
    return RoutineAwareMeegleExecutor(argv=argv,readback_argv=argv,targets=targets,actions=['update_fields'],timeout_seconds=180,
        routine_live=host_config.get('phase1_gateway',{}).get('send_live') is True and cfg.get('send_live') is True,
        routine_grants=cfg.get('routine_grants',[]))


def main():
    parser=argparse.ArgumentParser();parser.add_argument('--config',type=Path,required=True);args=parser.parse_args()
    path=args.config.resolve();info=path.lstat()
    if not stat.S_ISREG(info.st_mode) or info.st_uid!=os.getuid() or info.st_mode & 0o077:
        raise ContractError('private_executor_config_required')
    config=json.loads(path.read_text());raw=sys.stdin.buffer.read(1048577)
    if len(raw)>1048576:raise ContractError('operation_input_limit')
    output=MeegleOperationBridge(config,path.with_suffix('.sqlite')).handle(json.loads(raw))
    print(dumps(output))
