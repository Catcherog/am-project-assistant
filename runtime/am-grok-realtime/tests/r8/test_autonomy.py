from copy import deepcopy
import fcntl
import json
from pathlib import Path
import sys
from types import SimpleNamespace as NS

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'r7'))
from test_evolution import cases, make_record
from am_gateway.assistant.autonomy import AutonomousRSI
from am_gateway.assistant.evolution import EvolutionEngine, ReplayBudget
from am_gateway.assistant.improvement import ImprovementJournal
from am_gateway.models import ContractError, dumps
from am_gateway.store import Store


class Clock:
    def __init__(self):
        self.now = 1794355200.0
    def __call__(self):
        return self.now


@pytest.fixture
def rig(tmp_path):
    clock = Clock()
    store = Store(tmp_path / 'rsi.sqlite', clock=clock)
    journal = ImprovementJournal(store, 'owner')
    suites = {'dev': cases('dev', 2, 'development'), 'held': cases('held', 20, 'holdout')}
    calls, proposals = [], []
    def propose(**kwargs):
        proposals.append(kwargs)
        return {'patch': {'reply.layout': 'compact'}, 'rationale': 'synthetic comparison'}
    def replay(**kwargs):
        calls.append(kwargs)
        clock.now += .1
        return make_record(kwargs['case'], kwargs['context'],
            seconds=4 if kwargs['context']['variant'] == 'baseline' else 2)
    engine = EvolutionEngine(store, 'owner', proposer=propose, replayer=replay,
        suite_loader=lambda ref: deepcopy(suites[ref]), enabled_scopes=('controlled',),
        allowed_config_keys={'reply.layout','reply.max_chars'}, runtime_revision='source:v1', monotonic=clock)
    engine.register_baseline('controlled', {'reply.layout': 'paragraph', 'reply.max_chars': 600})
    engine.freeze_suite('dev', suites['dev'], split='development')
    engine.freeze_suite('held', suites['held'], split='holdout', independence_ref='test-distinct-heldout-tasks')
    c = NS(store=store, owner_id='owner', improvement=journal)
    busy = {'value': False}
    params = dict(scope='controlled', development_ref='dev', holdout_ref='held',
        budget={'min_idle_seconds':0,'cooldown_seconds':0}, clock=clock, monotonic=clock,
        foreground_busy=lambda: busy['value'])
    service = AutonomousRSI(engine, c, **params)
    return NS(clock=clock, store=store, journal=journal, engine=engine, coordinator=c,
        calls=calls, proposals=proposals, busy=busy, service=service, params=params)


def feedback(rig, identity='f1', text='回复太啰嗦，请更简洁。'):
    rig.journal.record_feedback(identity, actor_id='owner', text=text, source_ref='synthetic-feedback')
    rig.journal.analyze_once()


def test_new_feedback_triggers_complete_cycle_without_chat_command(rig):
    feedback(rig)
    report = rig.service.step()
    assert report['state'] == 'completed'
    assert report['result']['activation']['state'] == 'configuration_active'
    assert len(rig.calls) == 44 and len(rig.proposals) == 1
    assert rig.engine.active_config('controlled')['version'] == 2
    assert rig.service.reports()[0]['report_delivery'] == 'local_record_only'
    assert rig.store.rows('SELECT * FROM gw_outbox') == []


def test_no_signal_no_cli_and_same_signal_not_repeated_after_restart(rig):
    assert rig.service.step()['state'] == 'idle'
    assert rig.proposals == []
    feedback(rig); rig.service.step()
    second = AutonomousRSI(rig.engine, rig.coordinator, **rig.params)
    assert second.step()['state'] == 'idle'
    assert len(rig.proposals) == 1


def test_pause_persists_across_restart_and_force_never_bypasses_it(rig):
    feedback(rig)
    rig.service.set_paused(True)
    second = AutonomousRSI(rig.engine, rig.coordinator, **rig.params)
    assert second.step(force=True)['state'] == 'paused'
    assert rig.proposals == []
    second.set_paused(False)
    assert second.step()['state'] == 'completed'


def test_foreground_work_blocks_optimization(rig):
    feedback(rig); rig.busy['value'] = True
    assert rig.service.step(force=True)['state'] == 'deferred_for_interactive_work'
    assert rig.calls == [] and rig.proposals == []


def test_idle_window_restarts_when_user_work_arrives(rig):
    feedback(rig)
    params = {**rig.params, 'budget':{'min_idle_seconds':30,'cooldown_seconds':0}}
    service = AutonomousRSI(rig.engine, rig.coordinator, **params)
    assert service.step()['state'] == 'waiting_for_idle_window'
    rig.clock.now += 29; assert service.step()['state'] == 'waiting_for_idle_window'
    rig.busy['value'] = True; service.step()
    rig.busy['value'] = False; rig.clock.now += 1
    assert service.step()['state'] == 'waiting_for_idle_window'
    rig.clock.now += 30
    assert service.step()['state'] == 'completed'


def test_process_lock_prevents_parallel_host_and_cli(rig):
    feedback(rig)
    with rig.service.lock_path.open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        assert rig.service.step()['state'] == 'busy'
    assert rig.proposals == []


def test_running_job_left_after_crash_is_not_replayed(rig):
    feedback(rig); rig.service._enqueue()
    with rig.store.transaction() as db:
        db.execute("UPDATE r8_rsi_jobs SET state='running'")
    second = AutonomousRSI(rig.engine, rig.coordinator, **rig.params)
    assert second.step()['state'] == 'needs_reconciliation'
    assert rig.calls == [] and rig.proposals == []


def test_daily_limit_counts_no_change_attempts_too(rig):
    rig.engine.proposer = lambda **kw: {'patch': {}, 'rationale':'no justified change'}
    params = {**rig.params,'budget':{'min_idle_seconds':0,'cooldown_seconds':0,'max_jobs_per_day':1}}
    service = AutonomousRSI(rig.engine, rig.coordinator, **params)
    feedback(rig, 'first'); feedback(rig,'second')
    assert service.step()['result']['state'] == 'no_change'
    assert service.step(force=True)['state'] == 'daily_job_budget_reached'
    rig.clock.now += 86400
    assert service.step()['result']['state'] == 'no_change'


def test_budget_cap_not_bypassed_by_force(rig):
    feedback(rig)
    with rig.store.transaction() as db:
        db.execute('INSERT INTO r8_rsi_usage VALUES(?,?,?,0,3600)',('owner','controlled',rig.service._day()))
    assert rig.service.step(force=True)['state'] == 'daily_time_budget_reached'
    assert rig.proposals == []


def test_early_pause_resumes_same_engine_candidate_without_second_proposal(rig):
    feedback(rig)
    original = rig.engine.replayer
    def one_then_busy(**kwargs):
        record = original(**kwargs)
        if len(rig.calls) == 1:
            rig.busy['value'] = True
        return record
    rig.engine.replayer = one_then_busy
    first = rig.service.step()
    assert first['state'] == 'deferred' and len(rig.calls) == 1
    candidate = first['result']['candidate_id']
    rig.busy['value'] = False
    second = rig.service.step()
    assert second['state'] == 'completed'
    assert second['result']['candidate_id'] == candidate
    assert len(rig.calls) == 44 and len(rig.proposals) == 1


def test_tampered_job_fails_before_cli(rig):
    feedback(rig); rig.service._enqueue()
    with rig.store.transaction() as db:
        db.execute("UPDATE r8_rsi_jobs SET payload_hash='wrong'")
    with pytest.raises(ContractError, match='integrity'):
        rig.service.step()
    assert rig.proposals == []


def test_report_content_tampering_is_not_returned_as_verified(rig):
    feedback(rig); rig.service.step()
    with rig.store.transaction() as db:
        db.execute("UPDATE r8_rsi_jobs SET result_json='{}'")
    with pytest.raises(ContractError,match='integrity'):
        rig.service.reports()


def test_code_candidate_uses_own_pipeline_and_never_activates(rig):
    seen=[]
    class CodeWorker:
        def run(self, reason, cancelled):
            seen.append(reason)
            return {'state':'ready_for_native_integration','candidate_usable':True,'automatic_promotion':False}
    service = AutonomousRSI(rig.engine, rig.coordinator, code_worker=CodeWorker(), **rig.params)
    feedback(rig, text='回答太慢，需要优化速度。')
    result=service.step()
    assert result['kind']=='code' and len(seen)==1
    assert rig.engine.active_config('controlled')['version']==1
    assert result['production_deployment_performed'] is False


def test_job_time_exhaustion_does_not_starve_later_jobs(rig):
    rig.engine.proposer = lambda **kw: {'patch': {}, 'rationale': 'nothing to change'}
    feedback(rig, 'first'); feedback(rig, 'second')
    rig.service._enqueue()
    first = rig.store.one('SELECT job_id FROM r8_rsi_jobs ORDER BY created,job_id LIMIT 1')['job_id']
    with rig.store.transaction() as db:
        db.execute("UPDATE r8_rsi_jobs SET state='deferred',active_seconds=1800 WHERE job_id=?", (first,))
    assert rig.service.step()['state'] == 'job_time_budget_reached'
    assert rig.store.one('SELECT state FROM r8_rsi_jobs WHERE job_id=?',(first,))['state']=='budget_exhausted'
    assert rig.service.step()['state']=='completed'


def test_normal_cli_preemption_resumes_in_new_proposal_attempt(rig):
    feedback(rig)
    class InterruptOnce:
        last_result=None
        calls=0
        def __call__(self, **kwargs):
            self.calls+=1
            if self.calls==1:
                self.last_result={'state':'cancelled','local_process_stopped':True}
                raise ContractError('interrupted CLI')
            self.last_result={'state':'completed','local_process_stopped':True}
            return {'patch':{'reply.layout':'compact'},'rationale':'restarted independent attempt'}
    proposer=InterruptOnce();rig.engine.proposer=proposer
    first=rig.service.step()
    assert first['state']=='deferred' and first['result']['next_attempt_uses_new_workspace']
    second=rig.service.step()
    assert second['state']=='completed' and proposer.calls==2


def test_unknown_cli_stop_never_retries(rig):
    feedback(rig)
    class Unknown:
        last_result={'state':'cancelled','local_process_stopped':False}
        calls=0
        def __call__(self,**kwargs):
            self.calls+=1
            raise ContractError('unknown local process')
    proposer=Unknown();rig.engine.proposer=proposer
    assert rig.service.step()['state']=='unknown'
    assert rig.service.step()['state']=='needs_reconciliation'
    assert proposer.calls==1


def test_active_preemption_requires_new_idle_window(rig):
    rig.service._idle_since=rig.clock.now-100
    rig.busy['value']=True
    assert rig.service.should_yield()
    assert rig.service._idle_since is None


@pytest.mark.parametrize('state',['cli_incomplete','failed','completed'])
def test_unknown_code_process_stop_persists_and_blocks_all_later_jobs(rig,state):
    seen=[]
    class CodeWorker:
        def run(self,reason,cancelled):
            seen.append(reason)
            return {'state':state,'local_process_stopped':False,'candidate_usable':False}
    service=AutonomousRSI(rig.engine,rig.coordinator,code_worker=CodeWorker(),**rig.params)
    feedback(rig,text='回答太慢，需要优化速度。')
    assert service.step()['state']=='unknown'
    assert service.step(force=True)['state']=='needs_reconciliation'
    assert len(seen)==1 and rig.proposals==[] and rig.calls==[]


def test_reported_model_execution_unknown_blocks_scheduler_after_first_call(rig):
    feedback(rig)
    original=rig.engine.replayer
    def uncertain(**kwargs):
        record=original(**kwargs)
        record['outcome']={'status':'unknown'}
        record['physical_cancellation_proven']=False
        return record
    rig.engine.replayer=uncertain
    first=rig.service.step()
    assert first['state']=='unknown' and first['result']['state']=='unknown'
    second=AutonomousRSI(rig.engine,rig.coordinator,**rig.params)
    assert second.step(force=True)['state']=='needs_reconciliation'
    assert len(rig.calls)==1 and len(rig.proposals)==1
    assert rig.engine.active_config('controlled')['version']==1
