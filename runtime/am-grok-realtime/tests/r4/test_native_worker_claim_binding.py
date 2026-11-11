"""Run the actual CLI through the real HTTP receiver across task boundaries."""
from concurrent.futures import ThreadPoolExecutor
import json
from pathlib import Path
import subprocess
import sys
from types import SimpleNamespace

import pytest

from am_gateway.phase1.execution import ReceiverHTTPServer
from am_gateway.phase1.receiver import DurableReceiver
from am_gateway.store import Store
from test_receiver import envelope

TOOL=Path(__file__).resolve().parents[2]/'tools/native_worker_bridge.py'


def answer(text='A result'):
    return {'status':'succeeded','result':{'response_kind':'sufficient','reply_text':text,
            'remaining_requirements':[],'artifacts':[]},'stopped_evidence':'synthetic worker returned'}


@pytest.fixture
def rig(tmp_path,monkeypatch):
    receiver=DurableReceiver(Store(tmp_path/'receiver.sqlite',clock=lambda:1000.))
    receiver_calls=[]
    for name in ('claim','heartbeat','complete','mark_unknown','worker_context'):
        method=getattr(receiver,name)
        def traced(*args,_method=method,_name=name,**kwargs):
            receiver_calls.append((_name,kwargs.get('operation_id')))
            return _method(*args,**kwargs)
        monkeypatch.setattr(receiver,name,traced)
    server=ReceiverHTTPServer(receiver,admin_token='synthetic-admin',worker_token='synthetic-worker')
    server.start()
    cfg=tmp_path/'worker.json'
    cfg.write_text(json.dumps({'receiver_url':server.base_url,'worker_token':'synthetic-worker',
                               'worker_id':'native-binding-test'}))
    cfg.chmod(0o600)
    def call(action,data=None,handle=None):
        argv=[sys.executable,str(TOOL),'--config',str(cfg),action]
        if handle is not None:argv+=['--claim-handle',handle]
        result=subprocess.run(argv,input='' if data is None else json.dumps(data),
                              capture_output=True,text=True,timeout=8)
        assert 'synthetic-worker' not in result.stdout and 'lease_token' not in result.stdout
        return result.returncode,json.loads(result.stdout)
    def claim(mid):
        receiver.accept(envelope(mid=mid))
        code,result=call('claim')
        assert code==0 and result['status']=='claimed'
        assert result['claim_handle'].startswith('claim_')
        return result['claim_handle']
    yield SimpleNamespace(receiver=receiver,cfg=cfg,call=call,claim=claim,calls=receiver_calls)
    server.stop()


@pytest.mark.parametrize('action,payload',[
    ('result',answer('LATE A RESULT')),
    ('result',{'status':'unknown'}),
    ('heartbeat',None),
    ('read_information',{'info_id':'information-for-B'}),
])
def test_old_handle_never_touches_next_task(rig,action,payload):
    old_handle=rig.claim('A')
    assert rig.call('result',answer(),old_handle)[1]['status']=='succeeded'
    current_handle=rig.claim('B')
    assert current_handle!=old_handle
    before=rig.receiver.query('op_B')
    rig.calls.clear()
    code,result=rig.call(action,payload,old_handle)
    assert code==1 and result['error_code']=='stale_claim_handle'
    assert rig.calls==[]  # Includes protected information access and heartbeats.
    assert rig.receiver.query('op_B')==before
    assert rig.call('result',answer('B result'),current_handle)[1]['status']=='succeeded'
    assert rig.receiver.query('op_B')['result']['reply_text']=='B result'


@pytest.mark.parametrize('action,payload',[
    ('result',answer()),('heartbeat',None),('read_information',{'info_id':'private'})
])
def test_missing_handle_is_rejected_before_any_receiver_action(rig,action,payload):
    rig.claim('A')
    rig.calls.clear()
    code,result=rig.call(action,payload)
    assert code==1 and result['error_code']=='claim_handle_required'
    assert rig.calls==[]


def test_unknown_keeps_original_handle_and_never_claims_pending_task(rig):
    handle=rig.claim('A')
    rig.receiver.accept(envelope(mid='B'))
    assert rig.call('result',{'status':'unknown'},handle)[1]['status']=='unknown'
    code,duplicate=rig.call('claim')
    assert code==0 and duplicate=={'status':'already_claimed','execution_state':'unknown','same_claim':True}
    assert rig.receiver.query('op_B')['status']=='accepted'
    assert rig.call('heartbeat',handle=handle)[1]['status']=='unknown'
    assert rig.call('result',answer(),handle)[1]['status']=='succeeded'
    code,next_claim=rig.call('claim')
    assert code==0 and next_claim['status']=='claimed' and next_claim['claim_handle']!=handle


def test_same_handle_duplicate_result_remains_idempotent(rig):
    handle=rig.claim('A')
    first=rig.call('result',answer(),handle)
    second=rig.call('result',answer(),handle)
    assert first==second and first[0]==0
    events=rig.receiver.query('op_A')['events']
    assert sum(e['kind']=='result' for e in events)==1
    assert sum(e['kind']=='ended' for e in events)==1


@pytest.mark.parametrize('execution_state',['running','unknown'])
def test_legacy_active_state_is_not_rebound_to_a_new_caller(rig,execution_state):
    handle=rig.claim('A')
    state=rig.cfg.with_suffix('.claim.json')
    saved=json.loads(state.read_text())
    del saved['claim_handle']
    state.write_text(json.dumps(saved))
    if execution_state=='unknown':rig.receiver.mark_unknown(**saved['credentials'])
    rig.receiver.accept(envelope(mid='B'))
    code,result=rig.call('claim')
    assert code==0 and result=={'status':'legacy_claim_requires_resolution',
                               'execution_state':execution_state,'same_claim':True}
    assert json.loads(state.read_text())==saved
    assert rig.receiver.query('op_B')['status']=='accepted'
    rig.calls.clear()
    code,result=rig.call('result',answer(),handle)
    assert code==1 and result['error_code']=='legacy_claim_requires_resolution'
    assert rig.calls==[]


def test_terminal_legacy_state_can_migrate_without_replaying_original_task(rig):
    rig.claim('A')
    state=rig.cfg.with_suffix('.claim.json')
    saved=json.loads(state.read_text())
    del saved['claim_handle']
    state.write_text(json.dumps(saved))
    # Host reconciles the old original identity; the new Bot never adopts it.
    rig.receiver.complete(**saved['credentials'],**answer())
    rig.receiver.accept(envelope(mid='B'))
    code,result=rig.call('claim')
    assert code==0 and result['status']=='claimed'
    new_state=json.loads(state.read_text())
    assert new_state['claim_handle']==result['claim_handle']
    assert new_state['credentials']['operation_id']=='op_B'
    assert rig.receiver.query('op_A')['result']['reply_text']=='A result'
    assert len(rig.receiver.store.rows('SELECT * FROM h4_receiver_runs'))==2


def test_parallel_claims_disclose_task_and_handle_once(rig):
    rig.receiver.accept(envelope(mid='A'))
    with ThreadPoolExecutor(max_workers=4) as pool:
        replies=list(pool.map(lambda _:rig.call('claim'),range(4)))
    assert all(code==0 for code,_ in replies)
    claimed=[r for _,r in replies if r['status']=='claimed']
    assert len(claimed)==1
    assert all('claim_handle' not in r and 'task' not in r
               for _,r in replies if r['status']=='already_claimed')
    assert len(rig.receiver.store.rows('SELECT * FROM h4_receiver_runs'))==1


def test_parallel_old_completion_and_next_claim_cannot_complete_next_task(rig):
    old=rig.claim('A')
    assert rig.call('result',answer(),old)[0]==0
    rig.receiver.accept(envelope(mid='B'))
    with ThreadPoolExecutor(max_workers=2) as pool:
        late=pool.submit(rig.call,'result',answer(),old)
        next_claim=pool.submit(rig.call,'claim')
        late_code,late_result=late.result()
        claim_code,claim_result=next_claim.result()
    assert claim_code==0 and claim_result['status']=='claimed'
    if late_code==0:
        assert late_result['operation_id']=='op_A' and late_result['status']=='succeeded'
    else:
        assert late_result['error_code']=='stale_claim_handle'
    assert rig.receiver.query('op_B')['status']=='running'
    assert rig.receiver.query('op_B')['result'] is None


def test_claim_rejects_handle_instead_of_adopting_current_work(rig):
    handle=rig.claim('A')
    rig.calls.clear()
    code,result=rig.call('claim',handle=handle)
    assert code==1 and result['error_code']=='claim_does_not_accept_handle'
    assert rig.calls==[]
