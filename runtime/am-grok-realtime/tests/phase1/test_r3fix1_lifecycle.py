"""Regression cases for delayed ACK failures and single-owner rollback."""
import json

import pytest

from am_gateway.models import ContractError
from conftest import admit, receipt


def result_payload(run_id='run'):
    return {'run_id':run_id,'response_kind':'sufficient',
            'remaining_requirements':[],'artifacts':[],'reply_text':'Known answer'}


@pytest.mark.parametrize('kind,payload,dispatch,execution,session', [
    ('accepted',{'run_id':'run','durable_record':'receiver-row'},'accepted','queued','held'),
    ('running',{'run_id':'run'},'unknown','running','held'),
    ('ended',{'run_id':'run','status':'succeeded','stopped_evidence':'trusted-exit'},
     'unknown','succeeded','free'),
    ('result',result_payload(),'unknown','unknown','held'),
])
def test_callback_before_accept_timeout_preserves_its_evidence(rig,kind,payload,dispatch,execution,session):
    h,_=rig
    h.register_cutover('grok_bot:shared',evidence_ref='isolated-regression')

    class Receiver:
        def accept(self,env):
            receipt(h,env,kind,payload)
            raise TimeoutError('ACK response lost')

    h.adapters={'grok_bot':Receiver()}
    task=admit(h)
    observed=h.dispatch_one()
    assert observed['task_id']==task['task_id']
    assert observed['dispatch_state']==dispatch
    assert observed['execution_state']==execution
    assert observed['run_id']=='run'
    assert h.store.one('SELECT state FROM h1_sessions')['state']==session
    assert h.store.one('SELECT state FROM h1_dispatch')['state']==dispatch
    assert len(h.store.rows('SELECT * FROM h1_receipts'))==1
    if kind=='result':
        assert json.loads(observed['result_json'])==payload
    # Observing a result/running/end is not a substitute for a durable ACK.
    assert ('t_a' in json.loads(observed['stamps']))==(kind=='accepted')


def test_accept_timeout_without_callback_is_unknown_and_retains_scope(rig):
    h,_=rig
    h.register_cutover('grok_bot:shared',evidence_ref='isolated-regression')

    class Receiver:
        def accept(self,env):
            raise TimeoutError('no receiver evidence')

    h.adapters={'grok_bot':Receiver()}
    task=admit(h)
    observed=h.dispatch_one()
    assert (observed['dispatch_state'],observed['execution_state'])==('unknown','unknown')
    assert observed['run_id']=='' and observed['result_json'] is None
    assert observed['last_error']=='TimeoutError'
    assert h.store.one('SELECT task_id,state FROM h1_sessions')=={'task_id':task['task_id'],'state':'held'}
    assert not h.store.rows('SELECT * FROM h1_receipts')


def test_conflicting_terminal_callbacks_survive_accept_failure(rig):
    h,_=rig
    h.register_cutover('grok_bot:shared',evidence_ref='isolated-regression')

    class Receiver:
        def accept(self,env):
            receipt(h,env,'ended',{'run_id':'run','status':'succeeded','stopped_evidence':'exit'})
            receipt(h,env,'ended',{'run_id':'run','status':'failed','stopped_evidence':'different-report'},
                    event_id='conflicting-end')
            raise TimeoutError('ACK response lost')

    h.adapters={'grok_bot':Receiver()}
    admit(h)
    observed=h.dispatch_one()
    assert observed['execution_state']=='succeeded'
    assert observed['verification_state']=='conflict'
    assert observed['last_error']=='contradictory_terminal_evidence'
    assert len(h.store.rows("SELECT * FROM h1_receipts WHERE kind='ended'"))==2


def test_result_first_binds_run_id_and_rejects_another_ack_instance(rig):
    h,_=rig
    h.register_cutover('grok_bot:shared',evidence_ref='isolated-regression')

    class Receiver:
        def accept(self,env):
            receipt(h,env,'result',result_payload('result-run'))
            return {'kind':'accepted','event_id':'wrong-instance-ack',
                    'payload':{'run_id':'other-run','durable_record':'receiver-row'}}

    h.adapters={'grok_bot':Receiver()}
    admit(h)
    observed=h.dispatch_one()
    assert observed['run_id']=='result-run'
    assert json.loads(observed['result_json'])==result_payload('result-run')
    assert (observed['dispatch_state'],observed['execution_state'])==('unknown','unknown')
    assert 't_a' not in json.loads(observed['stamps'])
    assert not h.store.rows("SELECT * FROM h1_receipts WHERE kind='accepted'")


@pytest.mark.parametrize('changed_field', ['generation','attempt','business_id'])
def test_old_dispatch_failure_cannot_write_into_a_reconciled_attempt(rig,changed_field):
    h,_=rig
    h.register_cutover('grok_bot:shared',evidence_ref='isolated-regression')

    class Receiver:
        def accept(self,env):
            # Model an already committed reconciliation while this network call
            # is still in flight. Only the stale transport response is under test.
            tid=env['identity']['task_id']
            with h.store.transaction() as db:
                if changed_field=='generation':
                    db.execute('UPDATE h1_tasks SET generation=generation+1 WHERE task_id=?',(tid,))
                elif changed_field=='attempt':
                    db.execute("UPDATE h1_tasks SET attempt='new-attempt' WHERE task_id=?",(tid,))
                else:
                    db.execute("UPDATE h1_tasks SET business_id='new-operation' WHERE task_id=?",(tid,))
            self.current=h.task(tid)
            self.dispatch=h.store.one('SELECT * FROM h1_dispatch WHERE task_id=?',(tid,))
            raise TimeoutError('old call finally returned')

    receiver=Receiver()
    h.adapters={'grok_bot':receiver}
    task=admit(h)
    observed=h.dispatch_one()
    assert observed==receiver.current
    assert h.store.one('SELECT * FROM h1_dispatch WHERE task_id=?',(task['task_id'],))==receiver.dispatch
    assert h.store.one("SELECT kind FROM gw_audit WHERE kind='stale_dispatch_failure'")


def test_old_epoch_end_receipt_is_not_current_execution_evidence(rig):
    h,_=rig
    h.register_cutover('grok_bot:shared',evidence_ref='isolated-regression')

    class Receiver:
        def accept(self,env):
            receipt(h,env,'ended',{'run_id':'old-run','status':'succeeded','stopped_evidence':'old-exit'},
                    generation=env['execution']['generation']-1)
            raise TimeoutError('no current evidence')

    h.adapters={'grok_bot':Receiver()}
    admit(h)
    observed=h.dispatch_one()
    assert (observed['dispatch_state'],observed['execution_state'])==('unknown','unknown')
    assert observed['run_id']==''
    assert h.store.one('SELECT state FROM h1_sessions')['state']=='held'


@pytest.mark.parametrize('owners', [('harness','harness'),('legacy','harness')])
def test_rollback_rejects_harness_ownership_without_changing_any_ledger(rig,owners):
    h,_=rig
    scope='grok_bot:shared'
    h.register_cutover(scope,evidence_ref='isolated-regression')
    tasks=[admit(h,'a'),admit(h,'b')]
    manifest=h.prepare_rollback(scope)
    tables=('h1_cutover','h1_tasks','h1_dispatch','h1_sessions')
    before={table:h.store.rows('SELECT * FROM '+table) for table in tables}
    with pytest.raises(ContractError,match='only legacy ownership'):
        h.acknowledge_legacy_import(scope,fingerprint=manifest['fingerprint'],excluded_items=[],
                                   pending_owner={t['task_id']:owner for t,owner in zip(tasks,owners)})
    assert {table:h.store.rows('SELECT * FROM '+table) for table in tables}==before
    assert h.store.one('SELECT state FROM h1_cutover')['state']=='paused'


def test_rollback_assigns_every_pending_task_to_the_single_legacy_consumer(rig):
    h,_=rig
    scope='grok_bot:shared'
    h.register_cutover(scope,evidence_ref='isolated-regression')
    tasks=[admit(h,'a'),admit(h,'b')]
    manifest=h.prepare_rollback(scope)
    owners={task['task_id']:'legacy' for task in tasks}
    handed=h.acknowledge_legacy_import(scope,fingerprint=manifest['fingerprint'],excluded_items=[],pending_owner=owners)
    assert handed['pending_owner']==owners
    assert h.store.one('SELECT state FROM h1_cutover')['state']=='legacy'
    assert all(h.task(task['task_id'])['dispatch_state']=='legacy_owned' for task in tasks)
    assert all(row['state']=='legacy_owned' for row in h.store.rows('SELECT * FROM h1_dispatch'))
    assert h.claim() is None
