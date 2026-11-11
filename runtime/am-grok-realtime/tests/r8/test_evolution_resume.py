"""Persistent call accounting and safe-boundary resume under failure injection."""
from copy import deepcopy
import json

import pytest

from test_evolution_policy import controlled, propose
from am_gateway.assistant.evolution import ReplayBudget, ReplayDeferred
from am_gateway.models import ContractError, dumps, key


def identities(calls):
    return [(c['run_id'], c['split'], c['variant'], c['case_id'], c['attempt_id']) for c in calls]


@pytest.mark.parametrize('variant',['baseline','candidate'])
@pytest.mark.parametrize('identity_mismatch',[False,True])
def test_reported_execution_unknown_is_persisted_and_stops_calls_and_restart(controlled,variant,identity_mismatch):
    build,worker,store,_,calls=controlled
    engine=build()
    def uncertain(**kwargs):
        record=worker(**kwargs)
        engine.monotonic.now+=.5
        if kwargs['context']['variant']==variant:
            record['outcome']={'status':'unknown'}
            record['physical_cancellation_proven']=False
            if identity_mismatch:record['source_version']='unrelated-source'
        return record
    engine.replayer=uncertain
    item=propose(engine,budget=ReplayBudget(max_calls=100,max_seconds=100,per_call_seconds=10))
    report=engine.replay(item['candidate_id'])
    expected=1 if variant=='baseline' else 2
    assert report['state']=='unknown' and report['eligible'] is False
    assert report['calls_used']==len(calls)==expected
    assert report['automatic_retry_allowed'] is False
    assert report['active_seconds_used']==expected*.5
    assert report['reserved_seconds_for_unresolved']==10
    attempt=store.one("SELECT * FROM r8_evo_attempts WHERE state='unknown'")
    assert attempt['variant']==variant
    trial=store.one('SELECT * FROM r7_evo_trials WHERE run_id=? AND variant=?',(report['run_id'],variant))
    record=json.loads(trial['record_json'])
    assert record['execution_outcome_unknown'] is True and record['reported_outcome']['status']=='unknown'
    assert attempt['record_hash']==trial['record_hash']==key(record)
    restarted=build()
    again=restarted.replay(item['candidate_id'])
    assert again['state']=='unknown' and again['run_id']==report['run_id']
    assert again['step_calls_used']==0 and len(calls)==expected
    assert restarted.active_config('controlled')['version']==1


def test_defer_before_submission_then_resume_same_run(controlled):
    build, _, store, _, calls = controlled
    engine = build()
    engine.should_defer = lambda context: True
    item = propose(engine)
    paused = engine.replay(item['candidate_id'])
    assert paused['state'] == 'deferred' and paused['calls_used'] == 0
    assert paused['call_submitted_at_checkpoint'] is False
    assert store.rows('SELECT * FROM r8_evo_attempts') == []
    engine.should_defer = lambda context: False
    complete = engine.replay(item['candidate_id'])
    assert complete['run_id'] == paused['run_id']
    assert complete['eligible'] and complete['calls_used'] == 44
    assert len(identities(calls)) == len(set(identities(calls))) == 44
    assert len(store.rows('SELECT * FROM r7_evo_runs')) == 1


def test_checkpoint_resumes_after_process_clock_restart_without_repeating_done_call(controlled):
    build, worker, store, _, calls = controlled
    engine = build()

    def first_worker(**kwargs):
        record = worker(**kwargs)
        engine.monotonic.now += 1
        return record

    engine.replayer = first_worker
    engine.should_defer = lambda context: len(calls) >= 1
    item = propose(engine)
    first = engine.replay(item['candidate_id'])
    assert first['state'] == 'deferred' and first['active_seconds_used'] == 1
    assert first['calls_used'] == first['step_calls_used'] == 1
    assert store.one('SELECT state FROM r8_evo_attempts')['state'] == 'done'
    store.clock.now += 86400  # A day asleep consumes no active replay budget.
    restarted = build()

    class RestartedMonotonic:
        now = 0.0
        def __call__(self):
            return self.now

    restarted.monotonic = RestartedMonotonic()

    def later_worker(**kwargs):
        record = worker(**kwargs)
        restarted.monotonic.now += 1
        return record

    restarted.replayer = later_worker
    final = restarted.replay(item['candidate_id'])
    assert final['run_id'] == first['run_id'] and final['eligible']
    assert final['calls_used'] == final['active_seconds_used'] == 44
    assert final['step_calls_used'] == final['active_seconds_this_step'] == 43
    assert len(calls) == len(set(identities(calls))) == 44
    assert store.one('SELECT epoch FROM r8_evo_replay_progress')['epoch'] == 2


def test_call_budget_is_conserved_across_deferrals(controlled):
    build, _, _, _, calls = controlled
    engine = build()
    engine.should_defer = lambda context: len(calls) >= 1
    item = propose(engine, budget=ReplayBudget(max_calls=2, max_seconds=100, per_call_seconds=10))
    first = engine.replay(item['candidate_id'])
    assert first['state'] == 'deferred' and first['calls_used'] == 1
    engine.should_defer = None
    final = engine.replay(item['candidate_id'])
    assert final['run_id'] == first['run_id'] and final['state'] == 'budget_exhausted'
    assert final['calls_used'] == len(calls) == 2
    assert final['step_calls_used'] == 1


def test_active_time_budget_is_cumulative_and_excludes_idle(controlled):
    build, worker, store, _, calls = controlled
    engine = build()

    def timed(**kwargs):
        record = worker(**kwargs)
        engine.monotonic.now += min(1.25, kwargs['context']['timeout_seconds'])
        return record

    engine.replayer = timed
    engine.should_defer = lambda context: len(calls) >= 1
    item = propose(engine, budget=ReplayBudget(max_calls=50, max_seconds=3, per_call_seconds=2))
    first = engine.replay(item['candidate_id'])
    assert first['active_seconds_used'] == 1.25
    store.clock.now += 10000
    engine.should_defer = None
    final = engine.replay(item['candidate_id'])
    assert final['state'] == 'budget_exhausted'
    assert final['active_seconds_used'] == 3
    assert final['active_seconds_this_step'] == 1.75
    assert final['calls_used'] == len(calls) == 3
    assert calls[-1]['timeout_seconds'] == .5


def test_host_daily_remaining_budget_caps_next_worker_timeout(controlled):
    build, _, _, _, calls = controlled
    engine = build()
    engine.host_remaining_seconds = lambda: 1
    engine.should_defer = lambda context: bool(calls)
    report = engine.replay(propose(engine)['candidate_id'])
    assert report['state'] == 'deferred' and len(calls) == 1
    assert calls[0]['timeout_seconds'] == 1


def test_empty_host_budget_defers_without_a_reserved_attempt(controlled):
    build, _, store, _, calls = controlled
    engine = build()
    engine.host_remaining_seconds = lambda: 0
    report = engine.replay(propose(engine)['candidate_id'])
    assert report['state'] == 'deferred'
    assert report['reason'] == 'host_budget_exhausted_before_submission'
    assert calls == [] and store.rows('SELECT * FROM r8_evo_attempts') == []


@pytest.mark.parametrize('crash_phase', ['inside_worker', 'before_result_commit'])
def test_process_death_never_reissues_an_unresolved_started_attempt(controlled, crash_phase):
    build, worker, store, _, calls = controlled
    engine = build()
    item = propose(engine, budget=ReplayBudget(max_calls=100, max_seconds=100, per_call_seconds=10))
    if crash_phase == 'inside_worker':
        def crash(**kwargs):
            worker(**kwargs)
            raise SystemExit('simulated process death after submission')
        engine.replayer = crash
    else:
        def lose_result(*args, **kwargs):
            raise SystemExit('simulated death between response and atomic persistence')
        engine._save_trial = lose_result
    with pytest.raises(SystemExit):
        engine.replay(item['candidate_id'])
    assert len(calls) == 1
    assert store.one('SELECT state FROM r8_evo_attempts')['state'] == 'started'
    assert store.rows('SELECT * FROM r7_evo_trials') == []
    restarted = build()
    assert restarted.replay(item['candidate_id'])['state'] == 'busy'
    store.clock.now += 111
    unknown = restarted.replay(item['candidate_id'])
    assert unknown['state'] == 'unknown' and unknown['automatic_retry_allowed'] is False
    assert unknown['calls_used'] == 1 and len(calls) == 1
    assert unknown['active_seconds_complete'] is False
    assert unknown['reserved_seconds_for_unresolved'] == 10
    assert unknown['budget_seconds_charged'] == 10
    assert store.one('SELECT state FROM r8_evo_attempts')['state'] == 'unknown'
    assert len(store.rows('SELECT * FROM r7_evo_runs')) == 1
    again = restarted.replay(item['candidate_id'])
    assert again['state'] == 'unknown' and again['step_calls_used'] == 0
    assert len(calls) == 1


def test_done_trial_and_attempt_hashes_must_agree_before_resume(controlled):
    build, _, store, _, calls = controlled
    engine = build()
    engine.should_defer = lambda context: bool(calls)
    item = propose(engine)
    first = engine.replay(item['candidate_id'])
    with store.transaction() as db:
        db.execute("UPDATE r8_evo_attempts SET record_hash='wrong' WHERE run_id=?", (first['run_id'],))
    restarted = build()
    report = restarted.replay(item['candidate_id'])
    assert report['state'] == 'failed' and not report['eligible']
    assert len(calls) == 1


def test_worker_cannot_claim_safe_deferral_after_being_submitted(controlled):
    build, _, store, _, _ = controlled
    engine = build()

    def unsafe_claim(**kwargs):
        raise ReplayDeferred('worker claims no submission')

    engine.replayer = unsafe_claim
    report = engine.replay(propose(engine)['candidate_id'])
    assert report['state'] == 'rejected' and not report['eligible']
    assert report['calls_used'] == 4
    assert all(r['state'] == 'done' for r in store.rows('SELECT state FROM r8_evo_attempts'))


def test_resume_after_development_checkpoint_does_not_repeat_development(controlled):
    build, _, _, _, calls = controlled
    engine = build()
    engine.should_defer = lambda context: context['split'] == 'holdout'
    item = propose(engine)
    first = engine.replay(item['candidate_id'])
    assert first['state'] == 'deferred' and first['calls_used'] == 4
    assert first['comparisons']['development']['candidate']['ci_pass']
    engine.should_defer = None
    final = engine.replay(item['candidate_id'])
    assert final['eligible'] and final['calls_used'] == 44
    assert final['step_calls_used'] == 40
    assert sum(c['split'] == 'development' for c in calls) == 4


def test_expensive_boundary_callback_cannot_overrun_budget_and_still_submit(controlled):
    build, _, _, _, calls = controlled
    engine = build()

    def slow_boundary(context):
        engine.monotonic.now += 2
        return False

    engine.should_defer = slow_boundary
    item = propose(engine, budget=ReplayBudget(max_calls=50, max_seconds=1, per_call_seconds=1))
    report = engine.replay(item['candidate_id'])
    assert report['state'] == 'budget_exhausted'
    assert report['calls_used'] == 0 and calls == []


def test_parallel_replay_cannot_steal_active_epoch(controlled):
    build, worker, _, _, calls = controlled
    engine = build()
    other = build()
    item = propose(engine)
    observations = []

    def reentrant(**kwargs):
        if not calls:
            observations.append(other.replay(item['candidate_id']))
        return worker(**kwargs)

    engine.replayer = reentrant
    report = engine.replay(item['candidate_id'])
    assert report['eligible'] and observations[0]['state'] == 'busy'
    assert len(calls) == len(set(identities(calls))) == 44


def test_old_epoch_cannot_commit_late_result_after_reconciliation(controlled):
    build, worker, store, suites, calls = controlled
    engine = build()
    item = propose(engine, budget=ReplayBudget(max_calls=100, max_seconds=100, per_call_seconds=10))
    last = {}

    def crash(**kwargs):
        last['record'] = worker(**kwargs)
        raise SystemExit('simulated dead worker')

    engine.replayer = crash
    with pytest.raises(SystemExit):
        engine.replay(item['candidate_id'])
    store.clock.now += 111
    build().replay(item['candidate_id'])
    call = calls[0]
    with pytest.raises(ContractError, match='epoch ownership changed'):
        engine._save_trial(call['run_id'], call['split'], call['variant'], suites['dev'][0],
            call['attempt_id'], last['record'], counter={'epoch': 1, 'previous_active_seconds': 0},
            began=engine.monotonic(), submitted=True, duration=1)
    assert store.rows('SELECT * FROM r7_evo_trials') == []
