"""Frozen real-output regressions and local coverage/retention contracts.

No model calls or platform messages: positive cases below are human-authored
expected structures, not evidence of current Qwen performance.
"""
from dataclasses import replace
import json
from pathlib import Path
from types import SimpleNamespace as NS
from unittest.mock import Mock

import pytest

from am_info_sync.extraction import ExtractionError, ExtractionWorker
from am_info_sync.models import Observation, Principal, Source
from am_info_sync.store import Store
from am_info_sync.facts import FactEngine
from info_extraction import BoundedCoordinatorExtractor
from info_semantics import coverage_report, pending_reviews


FROZEN = json.loads((Path(__file__).parent / 'tests/fixtures/qwen_followup_frozen_r1.json').read_text())['cases']
CASES = {case['case']: case for case in FROZEN}


def candidate(field, quote, assertion='reported'):
    return dict(subject_id='synthetic-project', field=field, value=quote, quote=quote,
                assertion_type=assertion)


EXPECTED = {
    'F06': [('delivery', '样机已到仓'), ('response', '回复收到'),
            ('plan', '计划今天开测'), ('acceptance', '实际是否开测尚待核实')],
    'F07': [('delivery', '测试方案已经发出'), ('response', '回复 OK'),
            ('progress', '没有提供实际开测记录')],
    'F08': [('context', '已经有测试概述'), ('constraint', '原始测试文件仍禁止对外发送'),
            ('acceptance', '正式可对外使用的版本待确认')],
    'F09': [('review', '本次审批已经通过'), ('action_request', '旧的待提审提醒可以撤除', 'proposal'),
            ('review', '这不是审批撤回'), ('acceptance', '不代表项目完成')],
    'F11': [('progress', '正在测试'), ('acceptance', '可用性尚未确定'), ('response', '回复了解')],
    'F14': [('delivery', '稿件与版本已经齐备'), ('plan', '计划本周发布'), ('progress', '没有实际发布记录')],
    # R1-FIX2: preserve reported speaker; the frozen real output stays unchanged.
    'F15': [('delivery', '工程师说材料已交付'), ('acceptance', '业务核验尚未完成')],
    'INJ': [('progress', '仍在测试中')],
}


def extractor_for(candidates, *, complete=True):
    coordinator = Mock()
    coordinator.complete.return_value = NS(text=json.dumps(
        {'complete': complete, 'candidates': candidates}, ensure_ascii=False))
    extractor = BoundedCoordinatorExtractor(coordinator, lambda obs: {
        'subjects': [{'subject_id': 'synthetic-project', 'title': '演示项目'}],
        'allowed_subject_ids': ['synthetic-project']})
    return extractor, coordinator


def observation(text, *, object_id='om_test', version='v1'):
    return Observation('synthetic', object_id, version, 100, text=text)


@pytest.mark.parametrize('case', FROZEN, ids=lambda case: case['case'])
def test_prior_real_outputs_are_frozen_replays_and_cannot_claim_coverage(case):
    keys = ('subject_id', 'field', 'value', 'quote', 'assertion_type')
    old = [{k: c[k] for k in keys} for c in case['candidates']]
    extractor, coordinator = extractor_for(old)
    with pytest.raises(ExtractionError, match='semantic_coverage_incomplete') as error:
        extractor(observation(case['input']))
    assert error.value.retryable is False
    assert extractor.last_call['coverage']['issues']
    assert extractor.last_call['coverage']['semantic_acceptance'] == 'requires_review'
    # A local mock call verifies the original provider contract, not Qwen.
    assert coordinator.complete.call_count == 1
    assert coordinator.complete.call_args.kwargs['native_chat'] is True
    assert coordinator.complete.call_args.kwargs['max_tokens'] == 1024
    assert case['input'] not in json.dumps(extractor.last_call, ensure_ascii=False)


@pytest.mark.parametrize('case', sorted(EXPECTED))
def test_explicit_dimensions_can_be_represented_without_authority_promotion(case):
    expected = [candidate(*args) for args in EXPECTED[case]]
    extractor, _ = extractor_for(expected)
    obs = observation(CASES[case]['input'])
    result = extractor(obs)
    store = Store(':memory:', clock=lambda: 1000)
    try:
        store.register_source(Source('synthetic', 'im_chat', 'chat', 'tenant', 'owner',
            enabled=True, settings={'subject_id': 'synthetic-project'}))
        store.record(obs)
        engine = FactEngine(store)
        engine.process(obs, result, extractor_version=extractor.version)
        facts = engine.query('synthetic-project', Principal('owner', 'tenant'))['facts']
        assert len(facts) == len(expected)
        assert all(f['authority'] == 'unconfirmed' and f['protected'] is False for f in facts)
        assert {r[0] for r in store.conn.execute('SELECT origin FROM fact_candidates')} == {'model'}
        assert extractor.last_call['coverage']['issues'] == []
        assert extractor.last_call['coverage']['semantic_acceptance'] == 'requires_review'
    finally:
        store.close()


def test_a_full_quote_cannot_hide_a_missing_negative_in_value():
    value = candidate('acceptance', '业务核验尚未完成')
    value['value'] = '业务核验完成'
    assert coverage_report('业务核验尚未完成', [value])['issues'] == [
        'acceptance_pending', 'candidate_scope_incomplete']


def test_acknowledgment_spacing_and_case_are_preserved_without_false_rejection():
    value = candidate('response', '负责人回复  OK')
    value['value'] = '回复ok'
    assert coverage_report('负责人回复  OK', [value])['issues'] == []


def test_explicitly_unassigned_action_never_inherits_the_only_project_in_context():
    value = candidate('action_request', '请把这项工作的预计投入记下来', 'proposal')
    extractor, _ = extractor_for([value])
    with pytest.raises(ExtractionError, match='subject_unresolved'):
        extractor(observation(CASES['F12']['input']))


def test_unrecognized_text_is_not_certified_accurate_by_zero_rule_matches():
    report = coverage_report('This requires a professional judgement.', [])
    assert report['matched_signals'] == 0
    assert report['semantic_acceptance'] == 'requires_review'


@pytest.mark.parametrize('mutation', [
    {'field': 'unknown_field'}, {'value': 'x' * 601}, {'quote': 'x' * 601},
])
def test_native_schema_is_enforced_even_when_provider_ignores_json_schema(mutation):
    value = candidate('progress', '资料')
    value.update(mutation)
    extractor, _ = extractor_for([value])
    with pytest.raises(ExtractionError, match='invalid_output_schema'):
        extractor(observation('资料'))


def test_failed_coverage_persists_review_gap_and_publishes_no_partial_facts():
    store = Store(':memory:', clock=lambda: 1000)
    try:
        store.register_source(Source('synthetic', 'im_chat', 'chat', 'tenant', 'owner',
            enabled=True, settings={'subject_id': 'synthetic-project'}))
        obs = observation(CASES['F06']['input'])
        store.record(obs)
        extractor, _ = extractor_for([candidate('delivery', '样机已到仓')])
        worker = ExtractionWorker(store, extractor)
        result = worker.run_once()
        assert result['status'] == 'failed'
        assert result['code'].startswith('semantic_coverage_incomplete:')
        assert store.conn.execute('SELECT count(*) FROM fact_candidates').fetchone()[0] == 0
        reviews = pending_reviews(store, Principal('owner', 'tenant'))
        assert reviews['total'] == 1
        assert reviews['items'][0]['text'] == obs.text
        assert '未完整' in reviews['items'][0]['reason']
        gap = store.conn.execute('SELECT detail FROM gaps').fetchone()[0]
        assert obs.text not in gap
    finally:
        store.close()


def review_store():
    clock = [1000]
    store = Store(':memory:', clock=lambda: clock[0])
    source = Source('synthetic', 'im_chat', 'chat', 'tenant', 'owner', enabled=True,
        raw_retention_seconds=1, settings={'subject_id': 'synthetic-project'})
    store.register_source(source)
    store.record(observation('请把这项工作的预计投入记下来。项目、负责人和承诺截止日期均未明确。'))
    store.open_gap('synthetic', 'om_test', 'extraction', 'semantic_coverage_incomplete:subject_unresolved')
    return store, source, clock


def test_review_view_obeys_tenant_reader_acl_output_chat_and_disable():
    store, source, _ = review_store()
    try:
        for principal in (Principal('stranger', 'tenant'), Principal('owner', 'other'),
                          Principal('owner', 'tenant', output_chat_id='group')):
            assert pending_reviews(store, principal)['total'] == 0
        assert pending_reviews(store, Principal('owner', 'tenant'))['total'] == 1
        store.register_source(replace(source, enabled=False))
        assert pending_reviews(store, Principal('owner', 'tenant'))['total'] == 0
    finally:
        store.close()


def test_review_view_never_resurrects_expired_or_old_version_raw_text():
    store, _, clock = review_store()
    try:
        assert pending_reviews(store, Principal('owner', 'tenant'))['items'][0]['raw_available']
        clock[0] += 1001
        item = pending_reviews(store, Principal('owner', 'tenant'))['items'][0]
        assert not item['raw_available'] and item['text'] == '' and item['url'] is None
        store.record(Observation('synthetic', 'om_test', 'v2', 101, text='新版本资料'))
        item = pending_reviews(store, Principal('owner', 'tenant'))['items'][0]
        assert item['text'] == '新版本资料'
    finally:
        store.close()


def test_owner_private_info_route_exposes_gaps_but_rejects_group_and_nonowner():
    from am_information import AMInformation
    from fs_agent.config import BoundaryError
    store, _, _ = review_store()
    try:
        host = object.__new__(AMInformation)
        host.store = store
        host.am = NS(owner='owner', query_grant=Mock(return_value={}))
        row = {'body': '/info 待核对', 'user': 'owner', 'tenant': 'tenant', 'chat_type': 'p2p',
               'expires': 86400}
        result = host.respond(row)
        assert '尚未成为可执行事项' in result and '项目归属尚未明确' in result
        assert row['_source_access_version'] == store.access_version()
        assert row['expires'] == 2
        for change in ({'user': 'stranger'}, {'chat_type': 'group'}):
            with pytest.raises(BoundaryError, match='OWNER_ONLY'):
                host.respond(dict(row, **change))
    finally:
        store.close()


def test_read_to_outbox_acl_epoch_cannot_be_rebound_after_revocation(monkeypatch):
    from am_information import AMInformation
    from fs_agent.config import BoundaryError
    import info_semantics
    store, source, _ = review_store()
    try:
        original = pending_reviews
        def revoke_after_read(*args, **kwargs):
            result = original(*args, **kwargs)
            store.register_source(replace(source, enabled=False))
            return result
        monkeypatch.setattr(info_semantics, 'pending_reviews', revoke_after_read)
        host = object.__new__(AMInformation)
        host.store = store
        host.am = NS(owner='owner', query_grant=Mock(return_value={}))
        row = {'body': '/info 待核对', 'user': 'owner', 'tenant': 'tenant',
               'chat_type': 'p2p', 'expires': 86400}
        with pytest.raises(BoundaryError, match='SOURCE_ACCESS_CHANGED'):
            host.respond(row)
        assert row['_source_access_version'] != store.access_version()
    finally:
        store.close()
