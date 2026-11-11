"""Promotion behavior using controlled records, never real-model acceptance."""
from copy import deepcopy
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'r7'))
from test_evolution import Clock, cases, make_record

from am_gateway.assistant.evolution import EvolutionEngine
from am_gateway.models import ContractError, dumps, key
from am_gateway.store import Store


@pytest.fixture
def controlled(tmp_path):
    clock = Clock()
    store = Store(tmp_path / 'policy.sqlite', clock=clock)
    suites = {'dev': cases('development-r8', 2, 'development'),
              'held': cases('holdout-r8', 20, 'holdout')}
    for values in suites.values():
        for case in values:
            case['required_layers'] = ['real_model']
            case['oracle']['latency_targets_s'] = {'completion_s': 6}
    calls = []

    def worker(*, case, config, context):
        calls.append(deepcopy(context))
        record = make_record(case, context,
            seconds=32 if context['variant'] == 'baseline' else 22)
        record['execution_provenance'].update(actual_model_call=True,
            model_run_ref='controlled-unit-fixture:' + context['case_id'])
        return record

    def build(policy='relative_quality', scopes=('controlled',), **kwargs):
        engine = EvolutionEngine(store, 'owner', suite_loader=lambda ref: deepcopy(suites[ref]),
            replayer=worker, evaluation_layer='real_model', enabled_scopes=('controlled',),
            promotion_policy=policy, relative_quality_scopes=scopes, monotonic=clock, **kwargs)
        engine.register_baseline('controlled', {'reply.layout': 'paragraph'})
        engine.freeze_suite('dev', suites['dev'], split='development')
        engine.freeze_suite('held', suites['held'], split='holdout',
            independence_ref='unit-fixture:distinct-frozen-groups')
        return engine

    return build, worker, store, suites, calls


def propose(engine, **kwargs):
    return engine.create_candidate(scope='controlled', patch={'reply.layout': 'compact'},
        development_ref=kwargs.pop('development_ref', 'dev'),
        holdout_ref=kwargs.pop('holdout_ref', 'held'), **kwargs)


def test_relative_improvement_does_not_claim_six_second_release(controlled):
    build, _, store, _, calls = controlled
    engine = build()
    item = propose(engine)
    report = engine.replay(item['candidate_id'])
    assert report['eligible'] and report['local_improvement_eligible']
    assert report['evidence_state'] == 'verified_paired_replay'
    assert report['release_gate_pass'] is False
    assert report['product_latency_target_status'] == 'missed'
    assert report['latency']['baseline']['p95'] == 32
    assert report['latency']['candidate']['p95'] == 22
    assert len(calls) == 44
    activated = engine.activate(item['candidate_id'], expected_version=1)
    assert activated['activated'] and activated['version'] == 2
    assert activated['release_gate_pass'] is False
    assert activated['product_latency_target_status'] == 'missed'
    assert activated['production_deployment_proven'] is False
    assert store.rows('SELECT * FROM gw_outbox') == []


def test_default_absolute_policy_retains_product_target_gate(controlled):
    build, _, _, _, _ = controlled
    engine = build('absolute_release')
    report = engine.replay(propose(engine)['candidate_id'])
    assert not report['eligible']
    assert 'candidate_frozen_latency_targets_not_met' in report['reasons']
    assert report['product_latency_target_status'] == 'missed'


def test_relative_requires_explicit_controlled_scope(controlled):
    build, _, _, _, calls = controlled
    engine = build(scopes=())
    with pytest.raises(ContractError, match='scope is not explicitly authorized'):
        propose(engine)
    assert calls == []


def test_relative_does_not_accept_offline_as_model_evidence(tmp_path):
    with pytest.raises(ContractError, match='actual model replay'):
        EvolutionEngine(Store(tmp_path / 'offline.sqlite'), 'owner',
            promotion_policy='relative_quality', relative_quality_scopes=('controlled',))


@pytest.mark.parametrize('defect', ['source_review', 'source_identity', 'channel', 'model_call', 'wrong_fact'])
def test_relative_never_relaxes_candidate_truth_or_binding_checks(controlled, defect):
    build, worker, _, _, _ = controlled
    engine = build()

    def damaged(**kwargs):
        record = worker(**kwargs)
        if kwargs['context']['variant'] == 'candidate':
            if defect == 'source_review':
                record.pop('semantic_review')
            elif defect == 'source_identity':
                record['source_version'] = 'another-source'
            elif defect == 'channel':
                record['channel'] = 'private' if record['channel'] == 'group' else 'group'
            elif defect == 'model_call':
                record['execution_provenance'].pop('actual_model_call')
            else:
                record['output']['facts']['state'] = 'finished'
        return record

    engine.replayer = damaged
    report = engine.replay(propose(engine)['candidate_id'])
    assert not report['eligible'] and not report['local_improvement_eligible']
    assert report['release_gate_pass'] is False


def test_unknown_baseline_is_not_a_convenient_slow_comparison(controlled):
    build, worker, _, _, calls = controlled
    engine = build()

    def unknown(**kwargs):
        record = worker(**kwargs)
        if kwargs['context']['variant'] == 'baseline':
            record['outcome']['status'] = 'unknown'
        return record

    engine.replayer = unknown
    report = engine.replay(propose(engine)['candidate_id'])
    assert not report['eligible']
    assert report['state']=='unknown' and report['automatic_retry_allowed'] is False
    assert report['reason']=='replayer_reported_execution_unknown'
    assert len(calls)==1


def test_holdout_remains_mandatory_under_relative_policy(controlled):
    build, _, _, _, _ = controlled
    engine = build()
    report = engine.replay(propose(engine, holdout_ref=None)['candidate_id'])
    assert report['state'] == 'needs_holdout'
    assert report['evidence_state'] == 'insufficient'
    assert report['eligible'] is False


def test_relative_cannot_discard_realtime_or_phone_requirements(controlled):
    build, _, _, suites, calls = controlled
    engine = build()
    suites['system'] = deepcopy(suites['dev'])
    for case in suites['system']:
        case['required_layers'] = ['real_model', 'realtime', 'phone']
    engine.freeze_suite('system', suites['system'], split='development')
    report = engine.replay(propose(engine, development_ref='system')['candidate_id'])
    assert report['state'] == 'needs_evidence' and not report['eligible']
    assert report['calls_used'] == len(calls) == 4
    assert report['comparisons']['holdout_not_run_reason'] == 'development_required_layers_unverified'


def test_switching_policy_cannot_reuse_candidate_or_replay(controlled):
    build, _, _, _, calls = controlled
    engine = build()
    item = propose(engine)
    assert engine.replay(item['candidate_id'])['eligible']
    changed = build('absolute_release')
    with pytest.raises(ContractError, match='promotion policy changed'):
        changed.activate(item['candidate_id'], expected_version=1)
    before = len(calls)
    with pytest.raises(ContractError, match='promotion policy changed'):
        changed.replay(item['candidate_id'])
    assert len(calls) == before


def test_policy_threshold_is_bound_to_replay_evidence(controlled):
    build, _, _, _, _ = controlled
    engine = build(minimum_latency_gain=.05)
    item = propose(engine)
    assert engine.replay(item['candidate_id'])['eligible']
    changed = build(minimum_latency_gain=.01)
    with pytest.raises(ContractError, match='promotion policy changed'):
        changed.activate(item['candidate_id'], expected_version=1)


def test_revoked_relative_scope_cannot_activate_old_result(controlled):
    build, _, _, _, _ = controlled
    engine = build()
    item = propose(engine)
    assert engine.replay(item['candidate_id'])['eligible']
    revoked = build(scopes=())
    with pytest.raises(ContractError, match='not explicitly authorized'):
        revoked.activate(item['candidate_id'], expected_version=1)


def test_mismatched_report_policy_is_rejected_even_with_consistent_report_hash(controlled):
    build, _, store, _, _ = controlled
    engine = build()
    item = propose(engine)
    report = engine.replay(item['candidate_id'])
    report['promotion_policy']['mode'] = 'absolute_release'
    with store.transaction() as db:
        db.execute('UPDATE r7_evo_runs SET report_json=?,report_hash=? WHERE run_id=?',
            (dumps(report), key(report), report['run_id']))
    with pytest.raises(ContractError, match='does not match candidate'):
        engine.activate(item['candidate_id'], expected_version=1)


def test_overall_speedup_cannot_hide_group_regression(controlled):
    build, worker, _, _, _ = controlled
    engine = build()

    def uneven(**kwargs):
        record = worker(**kwargs)
        if record['channel'] == 'group':
            seconds = 10 if kwargs['context']['variant'] == 'baseline' else 12
            record['timing']['events']['completed'] = 100 + seconds
        return record

    engine.replayer = uneven
    report = engine.replay(propose(engine)['candidate_id'])
    assert report['latency']['candidate']['p95'] < report['latency']['baseline']['p95']
    assert not report['eligible']
    assert 'holdout_stratum_latency_regression:group:warm' in report['reasons']


def test_relative_rejects_new_development_style_regression(controlled):
    build, worker, _, _, _ = controlled
    engine = build()

    def worse_style(**kwargs):
        record = worker(**kwargs)
        if kwargs['context']['split'] == 'development' and kwargs['context']['variant'] == 'candidate':
            record = make_record(kwargs['case'], kwargs['context'], seconds=22,
                                 text='作为一个AI，合成任务仍待验收。')
            record['execution_provenance'].update(actual_model_call=True, model_run_ref='unit-fixture')
        return record

    engine.replayer = worse_style
    report = engine.replay(propose(engine)['candidate_id'])
    assert not report['eligible']
    assert 'development_style_rule_regression' in report['reasons']


def test_style_gain_at_same_speed_is_local_improvement_not_human_acceptance(controlled):
    build, _, _, _, _ = controlled
    engine = build()

    def clearer(*, case, config, context):
        text = ('作为一个AI，' if context['variant'] == 'baseline' else '') + '合成任务仍待验收。'
        record = make_record(case, context, seconds=32, text=text)
        record['execution_provenance'].update(actual_model_call=True, model_run_ref='unit-fixture')
        return record

    engine.replayer = clearer
    report = engine.replay(propose(engine)['candidate_id'])
    assert report['eligible'] and 'style_rule_proxy' in report['improvements']
    assert report['release_gate_pass'] is False
    assert report['style_acceptance'] == 'not_inferred_from_automatic_replay'


def test_relative_keeps_first_failed_run_and_never_relabels_retry(controlled):
    build, worker, _, _, _ = controlled
    engine = build()
    engine.replayer = lambda **kwargs: None
    item = propose(engine)
    assert not engine.replay(item['candidate_id'])['eligible']
    engine.replayer = worker
    report = engine.replay(item['candidate_id'])
    assert report['comparisons']['holdout']['candidate']['ci_pass']
    assert report['eligible'] is False and report['local_improvement_eligible'] is False
    assert 'same_candidate_has_prior_failed_attempts' in report['reasons']


def test_relative_no_gain_is_not_a_successful_optimization(controlled):
    build, worker, _, _, _ = controlled
    engine = build()

    def unchanged(**kwargs):
        record = worker(**kwargs)
        record['timing']['events']['completed'] = 132
        return record

    engine.replayer = unchanged
    report = engine.replay(propose(engine)['candidate_id'])
    assert not report['eligible']
    assert 'no_measured_improvement' in report['reasons']
