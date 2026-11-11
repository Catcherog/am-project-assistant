"""R1-FIX2 source-scope contracts; mock replay is not fresh Qwen evidence."""
from copy import deepcopy
import json
from pathlib import Path
from types import SimpleNamespace as NS
from unittest.mock import Mock

import pytest

from am_info_sync.extraction import ExtractionError, ExtractionWorker
from am_info_sync.facts import FactEngine
from am_info_sync.models import Observation, Principal, Source
from am_info_sync.store import Store
from info_extraction import BoundedCoordinatorExtractor
from info_semantics import coverage_report, pending_reviews


FROZEN = json.loads((Path(__file__).parent / 'tests/fixtures/qwen_followup_frozen_fix1.json').read_text())
CASES = {case['case']: case for case in FROZEN['cases']}


def fact(field, text, *, subject='synthetic-project', assertion='reported', value=None):
    return {'subject_id': subject, 'field': field, 'quote': text,
            'value': text if value is None else value, 'assertion_type': assertion}


def make_extractor(output, *, subjects=None):
    subjects = subjects or [{'subject_id': 'synthetic-project', 'title': '演示项目'}]
    coordinator = Mock()
    coordinator.complete.return_value = NS(text=json.dumps(output, ensure_ascii=False))
    extractor = BoundedCoordinatorExtractor(coordinator, lambda obs: {
        'subjects': subjects, 'allowed_subject_ids': [s['subject_id'] for s in subjects]})
    return extractor, coordinator


def observe(text):
    return Observation('synthetic', 'om_test', 'v1', 100, text=text)


@pytest.mark.parametrize(('name', 'code'), [
    ('NEGATION', 'negation_or_correction_uncovered'),
    ('MULTIPROJECT', 'subject_unresolved'),
    ('F12', 'subject_unresolved'),
    ('F15', 'attribution_uncovered'),
    ('CONDITION', 'review_dimension'),
])
def test_frozen_problem_outputs_create_explicit_failure_not_silent_acceptance(name, code):
    case = CASES[name]
    extractor, coordinator = make_extractor(case['output'], subjects=case['subjects'])
    with pytest.raises(ExtractionError, match=code) as error:
        extractor(observe(case['input']))
    assert error.value.retryable is False
    assert coordinator.complete.call_count == 1
    assert coordinator.complete.call_args.kwargs['native_chat'] is True
    assert coordinator.complete.call_args.kwargs['max_tokens'] == 1024
    assert case['input'] not in json.dumps(extractor.last_call, ensure_ascii=False)
    assert extractor.last_call['coverage']['semantic_acceptance'] == 'requires_review'


@pytest.mark.parametrize('name', ['F07', 'F09'])
def test_equivalent_evidence_and_constraint_fields_pass_without_rewriting_model(name):
    case = CASES[name]
    original = deepcopy(case['output'])
    extractor, _ = make_extractor(case['output'], subjects=case['subjects'])
    result = extractor(observe(case['input']))
    assert len(result) == 3
    assert case['output'] == original
    assert [c['field'] for c in result] == [c['field'] for c in original['candidates']]
    assert extractor.last_call['coverage']['issues'] == []


def test_corrected_negation_and_attribution_can_fit_six_candidates():
    values = [
        fact('delivery', '负责人澄清：材料并未交付'),
        fact('review', '审批没有通过'),
        fact('review', '也没有撤回审批', value='没有撤回审批'),
        fact('context', '之前的“已经完成”说法不成立'),
        fact('response', '只确认收到了提醒'),
        fact('acceptance', '业务验收仍未进行'),
    ]
    extractor, _ = make_extractor({'complete': True, 'candidates': values})
    assert len(extractor(observe(CASES['NEGATION']['input']))) == 6


def test_condition_is_preserved_and_dimensions_can_be_corrected_by_model():
    output = deepcopy(CASES['CONDITION']['output'])
    output['candidates'][1]['field'] = 'review'
    output['candidates'][3]['field'] = 'progress'
    extractor, _ = make_extractor(output)
    assert len(extractor(observe(CASES['CONDITION']['input']))) == 4


@pytest.mark.parametrize(('text', 'safe', 'unsafe'), [
    ('尚未完成', fact('progress', '尚未完成'), fact('progress', '完成', value='已完成')),
    ('设备到场后才能测试', fact('constraint', '设备到场后才能测试'),
        fact('progress', '测试', value='测试已开始')),
    ('如果设备到场，明天开始测试', fact('plan', '如果设备到场，明天开始测试'),
        fact('progress', '明天开始测试')),
    ('工程师说材料已交付', fact('delivery', '工程师说材料已交付'), fact('delivery', '材料已交付')),
])
def test_one_complete_candidate_cannot_shield_a_scope_stripped_second_candidate(text, safe, unsafe):
    extractor, _ = make_extractor({'complete': True, 'candidates': [safe, unsafe]})
    with pytest.raises(ExtractionError, match='candidate_scope_incomplete'):
        extractor(observe(text))


@pytest.mark.parametrize(('text', 'field'), [
    ('只有审批通过后才允许发布', 'constraint'),
    ('之前的“审批已经通过”说法不成立', 'context'),
    ('之前的“计划本周发布”说法不成立', 'context'),
    ('设备到场后才能测试', 'constraint'),
    ('评审通过后方可发出测试报告', 'constraint'),
    ('审批通过才可以交付材料', 'constraint'),
])
def test_positive_anchors_inside_scope_do_not_demand_a_second_unconditional_claim(text, field):
    extractor, _ = make_extractor({'complete': True, 'candidates': [fact(field, text)]})
    assert len(extractor(observe(text))) == 1


@pytest.mark.parametrize('text', ['评审通过后方可发出测试报告', '审批通过才可以交付材料'])
def test_postcondition_connectors_cannot_be_dropped_from_value(text):
    extractor, _ = make_extractor({'complete': True,
        'candidates': [fact('progress', text, value='测试报告发出，材料已经交付')]})
    with pytest.raises(ExtractionError, match='condition_scope_uncovered'):
        extractor(observe(text))


@pytest.mark.parametrize('text', ['尚未通过评审', '评审不通过'])
def test_inverted_or_alternative_negative_review_needs_review_dimension(text):
    extractor, _ = make_extractor({'complete': True, 'candidates': [fact('progress', text)]})
    with pytest.raises(ExtractionError, match='review_dimension'):
        extractor(observe(text))
    corrected, _ = make_extractor({'complete': True, 'candidates': [fact('review', text)]})
    assert len(corrected(observe(text))) == 1


@pytest.mark.parametrize('quote', ['演示甲项目材料已交付', '材料已交付'])
def test_literal_project_title_cannot_be_reassigned_to_another_allowed_subject(quote):
    subjects = [{'subject_id': 'a', 'title': '演示甲项目'}, {'subject_id': 'b', 'title': '演示乙项目'}]
    extractor, _ = make_extractor({'complete': True,
        'candidates': [fact('delivery', quote, subject='b')]}, subjects=subjects)
    with pytest.raises(ExtractionError, match='subject_binding_mismatch'):
        extractor(observe('演示甲项目材料已交付'))


def test_correct_multi_project_literal_bindings_are_accepted():
    subjects = [{'subject_id': 'a', 'title': '演示甲项目'}, {'subject_id': 'b', 'title': '演示乙项目'}]
    extractor, _ = make_extractor({'complete': True, 'candidates': [
        fact('delivery', '演示甲项目材料已交付', subject='a'),
        fact('delivery', '演示乙项目样机已到仓', subject='b'),
    ]}, subjects=subjects)
    assert len(extractor(observe('演示甲项目材料已交付，演示乙项目样机已到仓'))) == 2


def test_duplicated_bare_quote_in_different_project_units_needs_review():
    subjects = [{'subject_id': 'a', 'title': '演示甲项目'}, {'subject_id': 'b', 'title': '演示乙项目'}]
    extractor, _ = make_extractor({'complete': True,
        'candidates': [fact('delivery', '材料已交付', subject='a')]}, subjects=subjects)
    with pytest.raises(ExtractionError, match='subject_binding_ambiguous'):
        extractor(observe('演示甲项目材料已交付，演示乙项目材料已交付'))


def test_attributed_delivery_remains_unconfirmed_after_fact_engine():
    text = CASES['F15']['input']
    values = [fact('delivery', '工程师说材料已交付'), fact('acceptance', '业务核验尚未完成')]
    extractor, _ = make_extractor({'complete': True, 'candidates': values})
    obs = observe(text)
    store = Store(':memory:', clock=lambda: 1000)
    try:
        store.register_source(Source('synthetic', 'im_chat', 'chat', 'tenant', 'owner',
            enabled=True, settings={'subject_id': 'synthetic-project'}))
        store.record(obs)
        engine = FactEngine(store)
        engine.process(obs, extractor(obs), extractor_version=extractor.version)
        facts = engine.query('synthetic-project', Principal('owner', 'tenant'))['facts']
        assert len(facts) == 2
        assert all(f['authority'] == 'unconfirmed' and not f['protected'] for f in facts)
        assert any('工程师说' in f['value'] for f in facts)
    finally:
        store.close()


@pytest.mark.parametrize(('text', 'candidate'), [
    ('材料并未交付', fact('delivery', '材料并未交付', value='材料已交付')),
    ('之前的“已经完成”说法不成立', fact('progress', '已经完成')),
    ('如果设备到场，明天开始测试', fact('plan', '明天开始测试')),
    ('如果设备到场，明天开始测试', fact('plan', '如果设备到场，明天开始测试', value='明天开始测试')),
    ('工程师说材料已交付', fact('delivery', '材料已交付')),
    ('负责人表示材料已交付', fact('delivery', '负责人表示材料已交付', value='材料已交付')),
    ('工程师说，材料已交付', fact('delivery', '材料已交付')),
    ('材料没交付', fact('delivery', '交付', value='材料已交付')),
    ('材料不交付', fact('delivery', '交付', value='材料已交付')),
    ('材料无交付记录', fact('delivery', '交付', value='材料已交付')),
])
def test_full_quote_or_short_positive_value_cannot_hide_lost_scope(text, candidate):
    extractor, _ = make_extractor({'complete': True, 'candidates': [candidate]})
    with pytest.raises(ExtractionError, match='semantic_coverage_incomplete'):
        extractor(observe(text))


@pytest.mark.parametrize('text', [
    '负责人说它已经完成，但没有说明指哪个项目。',
    '它明天提交，项目归属待核实。',
    '请把预计投入记下来。项目、负责人均未明确。',
])
def test_explicit_ambiguous_subject_is_visible_even_with_complete_false(text):
    extractor, _ = make_extractor({'complete': False, 'candidates': []})
    with pytest.raises(ExtractionError, match='subject_unresolved'):
        extractor(observe(text))


def test_model_incomplete_does_not_invent_budget_reason():
    extractor, _ = make_extractor({'complete': False,
        'candidates': [fact('progress', '仍在测试中')]})
    with pytest.raises(ExtractionError, match='^semantic_model_incomplete$'):
        extractor(observe('仍在测试中'))
    assert extractor.last_call['coverage']['issues'] == []


def test_empty_business_extraction_cannot_claim_complete():
    extractor, _ = make_extractor({'complete': True, 'candidates': []})
    with pytest.raises(ExtractionError, match='source_clause_uncovered'):
        extractor(observe('设计文档明天提交'))


def test_omitted_positive_business_clause_is_not_hidden_by_another_candidate():
    extractor, _ = make_extractor({'complete': True,
        'candidates': [fact('delivery', '样机已到仓')]})
    with pytest.raises(ExtractionError, match='source_clause_uncovered'):
        extractor(observe('样机已到仓；设计文档明天提交。'))


def test_unrecognized_language_remains_explicitly_unvalidated():
    report = coverage_report('This needs specialist interpretation.', [])
    assert report['matched_signals'] == report['business_units_checked'] == 0
    assert report['semantic_acceptance'] == 'requires_review'


@pytest.mark.parametrize(('text', 'value'), [
    ('测试说明已提交', '说明已提交'), ('仪器名称已提交', '名称已提交'),
])
def test_explanation_and_name_nouns_are_not_misread_as_reported_speech(text, value):
    extractor, _ = make_extractor({'complete': True,
        'candidates': [fact('delivery', text, value=value)]})
    assert len(extractor(observe(text))) == 1


@pytest.mark.parametrize('change', [
    {'field': []}, {'assertion_type': []}, {'assertion_type': 'approved'},
    {'subject_id': []}, {'subject_id': ''}, {'value': ''}, {'quote': False},
])
def test_malformed_candidates_have_safe_schema_errors(change):
    value = fact('progress', '仍在测试中')
    value.update(change)
    extractor, _ = make_extractor({'complete': True, 'candidates': [value]})
    with pytest.raises(ExtractionError, match='^invalid_output_schema$'):
        extractor(observe('仍在测试中'))


def test_context_subject_allowlist_is_checked_before_fact_creation():
    extractor, _ = make_extractor({'complete': True, 'candidates': [
        fact('progress', '仍在测试中', subject='other-project')]})
    with pytest.raises(ExtractionError, match='^subject_outside_context$'):
        extractor(observe('仍在测试中'))


def test_nonverbatim_quote_becomes_nonretryable_safe_error():
    extractor, _ = make_extractor({'complete': True,
        'candidates': [fact('progress', '已经完成')]})
    with pytest.raises(ExtractionError, match='^quote_not_verbatim$') as error:
        extractor(observe('仍在测试中'))
    assert not error.value.retryable


def test_truncated_model_json_is_not_accepted():
    extractor, coordinator = make_extractor({'complete': True, 'candidates': []})
    coordinator.complete.return_value.text = '{"complete":true,"candidates":['
    with pytest.raises(ExtractionError, match='^invalid_json_output$'):
        extractor(observe('仍在测试中'))


@pytest.mark.parametrize(('name', 'reason'), [
    ('NEGATION', '否定或更正'), ('MULTIPROJECT', '项目归属尚未明确'),
    ('F12', '项目归属尚未明确'), ('F15', '转述者与原话'),
])
def test_rejected_source_stays_visible_with_no_partial_candidates(name, reason):
    case = CASES[name]
    extractor, _ = make_extractor(case['output'], subjects=case['subjects'])
    store = Store(':memory:', clock=lambda: 1000)
    try:
        store.register_source(Source('synthetic', 'im_chat', 'chat', 'tenant', 'owner',
            enabled=True, settings={'subject_ids': [s['subject_id'] for s in case['subjects']]}))
        store.record(observe(case['input']))
        result = ExtractionWorker(store, extractor).run_once()
        assert result['status'] == 'failed'
        assert store.conn.execute('SELECT COUNT(*) FROM fact_candidates').fetchone()[0] == 0
        review = pending_reviews(store, Principal('owner', 'tenant'))
        assert review['total'] == 1
        assert reason in review['items'][0]['reason']
        assert review['items'][0]['text'] == case['input']
        assert case['input'] not in store.conn.execute('SELECT detail FROM gaps').fetchone()[0]
    finally:
        store.close()
