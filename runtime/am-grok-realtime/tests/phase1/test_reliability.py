from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
import json
import os
from pathlib import Path
import subprocess
import sys

import pytest

from am_gateway.models import ContractError, Event
from am_gateway.phase1 import Harness, Policy
from am_gateway.phase1.contracts import business_fingerprint
from am_gateway.phase1.routing import Classifier, TransientClassifierError
from am_gateway.store import Store
from conftest import admit,receipt


def test_committed_event_cursor_survives_real_process_exit_before_task(rig):
    h,_=rig
    script = '''import os,sys
from am_gateway.phase1 import Harness,HarnessConfig,Policy
from am_gateway.models import Event
from am_gateway.store import Store
h=Harness(Store(sys.argv[1]),HarnessConfig('tenant','bot','owner','owner-dm'))
h.ingest(Event('tenant','crash','owner-dm','owner','A 项目目前卡在哪里？',1,channel='bot_dm'),Policy('owner','auth','owner-dm',('owner-dm',)),cursor='durable-cursor')
os._exit(27)
'''
    proc=subprocess.run([sys.executable,'-c',script,h.store.path],env={**os.environ,'PYTHONPATH':str(Path(__file__).resolve().parents[2]/'src')})
    assert proc.returncode==27
    assert h.store.one('SELECT cursor FROM h1_sync')['cursor']=='"durable-cursor"'
    assert not h.store.rows('SELECT * FROM h1_tasks')
    task=h.route_one()
    assert task['intent']=='project.query'
    assert h.route_one() is None
    assert len(h.store.rows('SELECT * FROM h1_tasks'))==1


def test_duplicate_channels_keep_pending_responsibility_and_one_task(rig):
    h,_=rig
    e=Event('tenant','m','owner-dm','owner','查询 A 进展',999,channel='bot_dm')
    p=Policy('owner','auth','owner-dm',('owner-dm',))
    h.ingest(e,p)
    h.ingest(replace(e,observed_via='poll'),p)
    assert h.route_one()['intent']=='project.query'
    h.ingest(e,p)
    assert h.route_one() is None


def test_atomic_claim_before_ack_and_restart_lock(rig):
    h,clock=rig
    admit(h,'a');admit(h,'b')
    with ThreadPoolExecutor(max_workers=2) as pool:
        claims=list(pool.map(lambda _:h.claim(),range(2)))
    envelope=next(c for c in claims if c)
    assert sum(c is not None for c in claims)==1
    clock[0]+=31
    h.expire()
    restarted=Harness(Store(h.store.path,clock=lambda:clock[0]),h.config)
    assert restarted.claim() is None
    assert restarted.task(envelope['identity']['task_id'])['execution_state']=='unknown'


def test_deadline_is_persisted_and_expired_backlog_never_dispatches(rig):
    h,clock=rig
    first=admit(h)
    clock[0]+=121
    h=Harness(Store(h.store.path,clock=lambda:clock[0]),h.config)
    assert h.claim() is None
    assert h.task(first['task_id'])['dispatch_state']=='expired'
    assert h.task(first['task_id'])['deadline']==1120


def test_queue_cap_and_independent_execution_scope(rig):
    h,_=rig
    caps=dict(h.config.capabilities)
    caps['conversation.reply']=replace(caps['conversation.reply'],execution_scope='separate_verified_session')
    h=Harness(h.store,replace(h.config,capabilities=caps,queue_limit=1))
    admit(h,'a');second=admit(h,'b')
    assert second['route_state']=='queue_full'
    first=h.claim()
    receipt(h,first,'unknown',{})
    admit(h,'hello','你好')
    assert h.claim()['routing']['intent']=='conversation.reply'


def test_fingerprint_freezes_business_not_transport_policy_or_route(rig):
    h,_=rig
    task=admit(h)
    env=h.claim()
    original=env['identity']['business_fingerprint']
    h2=Harness(h.store,replace(h.config,route_version='new-version'))
    other=h2.envelope(h2.task(task['task_id']))
    assert other['identity']['business_operation_id']==env['identity']['business_operation_id']
    assert other['identity']['business_fingerprint']==original
    assert business_fingerprint(text='changed',intent='project.query',principal='owner',
        reply_target='owner-dm',attachments=(),inputs={'project_ids':('A',)})!=original


def test_edit_after_dispatch_records_revision_without_second_effect(rig):
    h,_=rig
    task=admit(h)
    env=h.claim()
    request=json.loads(task['request_json'])
    request.update(body='把 A 项目状态改成完成',version_time=1001)
    h.ingest(Event.from_dict(request),Policy.parse(task['policy_json']))
    updated=h.route_one()
    assert updated['business_id']==env['identity']['business_operation_id']
    assert updated['last_error']=='source_changed'
    assert h.claim() is None
    with pytest.raises(ContractError):
        h.resume(task['task_id'],'owner',intent='project.execute')


def test_webhook_200_is_not_ack(rig):
    h,_=rig
    admit(h);env=h.claim()
    with pytest.raises(ContractError):
        receipt(h,env,'accepted',{'http_status':200})
    assert 't_a' not in json.loads(h.task(env['identity']['task_id'])['stamps'])


def test_terminal_then_late_progress_duplicate_and_conflict(rig):
    h,_=rig
    admit(h);env=h.claim()
    done={'status':'succeeded','run_id':'run','stopped_evidence':'trusted-process-exit'}
    receipt(h,env,'ended',done)
    receipt(h,env,'running',{'run_id':'run'})
    receipt(h,env,'ended',done)
    task=h.task(env['identity']['task_id'])
    assert task['execution_state']=='succeeded'
    receipt(h,env,'ended',{**done,'status':'failed'},event_id='conflict')
    assert h.task(task['task_id'])['verification_state']=='conflict'


def test_old_epoch_receipt_never_unlocks_current_execution(rig):
    h,_=rig
    admit(h,'a');admit(h,'b');old=h.claim()
    receipt(h,old,'ended',{'status':'succeeded','run_id':'old','stopped_evidence':'exit'})
    current=h.claim()
    receipt(h,current,'ended',{'status':'succeeded','run_id':'forged','stopped_evidence':'old'},generation=0)
    assert h.store.one('SELECT * FROM h1_sessions')['task_id']==current['identity']['task_id']
    with pytest.raises(ContractError):
        h.tool_action(old['identity']['task_id'],old['execution']['generation'],'reply',target='owner-dm',body='late',sender=lambda **k:{'platform_id':'bad'})


def test_receipt_identity_conflict_does_not_overwrite(rig):
    h,_=rig
    admit(h);env=h.claim()
    receipt(h,env,'running',{'run_id':'one'},event_id='same')
    with pytest.raises(ContractError):
        receipt(h,env,'running',{'run_id':'two'},event_id='same')
    assert h.task(env['identity']['task_id'])['run_id']=='one'


def test_unknown_send_never_retries_and_delivery_not_execution_completion(rig):
    h,_=rig
    admit(h);env=h.claim();calls=[]
    def sender(**kwargs):
        calls.append(kwargs)
        raise TimeoutError()
    args=(env['identity']['task_id'],env['execution']['generation'],'reply')
    for _ in range(2):
        assert h.tool_action(*args,target='owner-dm',body='回答',sender=sender)['state']=='unknown'
    assert len(calls)==1
    assert h.task(args[0])['execution_state']!='succeeded'


def test_unrelated_task_keeps_running_when_classification_errors(rig):
    h,_=rig
    bad=admit(h,'bad','帮我处理一下')
    assert bad['route_state']=='classification_error'
    assert admit(h,'good')['route_state']=='resolved'
    assert h.claim()['identity']['task_id']!=bad['task_id']


def test_transient_retry_uses_remaining_budget_and_auth_no_retry():
    now=[0.0];budgets=[]
    def transport(data,timeout):
        budgets.append(timeout);now[0]+=1
        if len(budgets)==1:raise TransientClassifierError()
        return {'decision':'unknown','intent':'unknown','missing_fields':[],
                'secondary_intents':[],'clarification_question':None,'reason_code':'unclear'}
    classifier=Classifier(transport,clock=lambda:now[0])
    event=Event('t','m','c','u','x',0)
    assert classifier.classify(event,6)['decision']=='unknown'
    assert budgets==[6,5]
    budgets.clear()
    def authentication(data,timeout):
        budgets.append(timeout);raise PermissionError()
    with pytest.raises(PermissionError):Classifier(authentication).classify(event,6)
    assert len(budgets)==1


def test_manual_resume_same_task_wrong_actor_rejected_and_capability_not_granted(rig):
    h,_=rig
    class Unknown:
        def classify(self,event,budget):
            return {'decision':'needs_clarification','intent':'unknown','missing_fields':['action'],
                    'secondary_intents':[],'clarification_question':'需要什么产物？','reason_code':'unclear'}
    h.classifier=Unknown()
    task=admit(h,'unclear','帮我处理附件')
    with pytest.raises(ContractError):h.resume(task['task_id'],'stranger',intent='project.query')
    resumed=h.resume(task['task_id'],'owner',intent='document.process')
    assert resumed['task_id']==task['task_id']
    assert resumed['route_state']=='capability_unavailable'
    assert h.claim() is None


def test_cutover_manifest_includes_completed_unknown_and_pending(rig):
    h,_=rig
    scope='grok_bot:shared';h.register_cutover(scope,evidence_ref='test-scoped-poller-stopped')
    a=admit(h,'a');ea=h.claim()
    receipt(h,ea,'ended',{'status':'succeeded','run_id':'a','stopped_evidence':'exit'})
    # Fixture records an independently verified result, not a timestamp skip.
    with h.store.transaction() as db:
        db.execute("UPDATE h1_tasks SET delivery_state='delivered',verification_state='sufficient' WHERE task_id=?",(a['task_id'],))
    b=admit(h,'b');eb=h.claim();c=admit(h,'c')
    manifest=h.prepare_rollback(scope)
    assert [len(manifest[k]) for k in ('completed','unresolved','undispatched')]==[1,1,1]
    args=dict(fingerprint=manifest['fingerprint'],excluded_items=[a['item_id'],b['item_id']],pending_owner={c['task_id']:'legacy'})
    with pytest.raises(ContractError):h.acknowledge_legacy_import(scope,**args)
    receipt(h,eb,'ended',{'status':'failed','run_id':'b','stopped_evidence':'confirmed exit'})
    h.acknowledge_legacy_import(scope,**args)
    assert h.task(c['task_id'])['dispatch_state']=='legacy_owned'
    assert h.claim() is None
