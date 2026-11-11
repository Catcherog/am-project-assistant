"""R9 source-scope regressions; these authored cases do not measure Qwen.

The suite exercises the production catalog/extractor/fact/gap interfaces with
one mocked model response. It performs no model calls or platform writes.
"""
import json
import unittest
from types import SimpleNamespace as NS
from unittest.mock import Mock

from am_info_sync.catalog import EntityCatalog
from am_info_sync.extraction import ExtractionError, ExtractionWorker
from am_info_sync.facts import FactEngine
from am_info_sync.models import Observation, Principal, Source
from am_info_sync.store import Store
from info_extraction import BoundedCoordinatorExtractor
from info_semantics import coverage_report, pending_reviews


SUBJECTS = [
    {'subject_id': 'a', 'title': '演示甲项目'},
    {'subject_id': 'b', 'title': '演示乙项目'},
]
MULTI_CONTEXT = {'subjects': SUBJECTS, 'allowed_subject_ids': ['a', 'b']}


def fact(field, quote, *, subject='a', value=None, assertion='reported'):
    return {'subject_id': subject, 'field': field, 'quote': quote,
            'value': quote if value is None else value,
            'assertion_type': assertion}


def observation(text, *, payload=None):
    return Observation('synthetic', 'om_r9', 'v1', 100, text=text,
                       payload={} if payload is None else payload)


def extractor_for(candidates, *, context=None, provider=None, complete=True):
    coordinator = Mock()
    coordinator.complete.return_value = NS(text=json.dumps(
        {'complete': complete, 'candidates': candidates}, ensure_ascii=False))
    if provider is None:
        supplied = {'subjects': SUBJECTS[:1], 'allowed_subject_ids': ['a']}
        if context is not None:
            supplied = context
        provider = lambda obs: supplied
    return BoundedCoordinatorExtractor(coordinator, provider), coordinator


class EvidenceAbsenceTests(unittest.TestCase):
    def test_existing_sem01_acceptance_is_preserved_not_claimed_as_new_fix(self):
        text = '没有开测记录'
        extractor, _ = extractor_for([fact('acceptance', text)])
        self.assertEqual(len(extractor(observation(text))), 1)
        self.assertEqual(extractor.last_call['coverage']['issues'], [])

    def test_qualified_missing_records_do_not_assert_execution_absence(self):
        for text in ('没有开测的相关记录', '未开始测试的有效证据',
                     '尚未找到实际发布的完整记录', '没有开测相关记录'):
            with self.subTest(text=text):
                extractor, _ = extractor_for([fact('acceptance', text)])
                self.assertEqual(len(extractor(observation(text))), 1)

    def test_evidence_gap_cannot_be_misclassified_as_review(self):
        text = '没有开测的相关记录'
        extractor, _ = extractor_for([fact('review', text)])
        with self.assertRaisesRegex(ExtractionError, 'execution_evidence_missing'):
            extractor(observation(text))

    def test_independent_execution_absence_still_needs_progress(self):
        for text in ('尚未开测', '没有开始测试',
                     '没有开测的相关记录且尚未开测'):
            with self.subTest(text=text):
                extractor, _ = extractor_for([fact('acceptance', text)])
                with self.assertRaisesRegex(ExtractionError, 'execution_dimension'):
                    extractor(observation(text))
                corrected, _ = extractor_for([fact('progress', text)])
                self.assertEqual(len(corrected(observation(text))), 1)


class AttributionScopeTests(unittest.TestCase):
    def test_later_comma_clause_cannot_drop_reporter(self):
        for first in ('工程师说，材料已交付', '工程师说材料已交付'):
            with self.subTest(first=first):
                text = first + '，审批已经通过'
                extractor, _ = extractor_for([
                    fact('delivery', first), fact('review', '审批已经通过')])
                with self.assertRaisesRegex(ExtractionError, 'attribution_uncovered'):
                    extractor(observation(text))

    def test_complete_report_can_preserve_distinct_dimensions(self):
        text = '工程师说，材料已交付，审批已经通过'
        extractor, _ = extractor_for([
            fact('delivery', text), fact('review', text)])
        self.assertEqual(len(extractor(observation(text))), 2)

    def test_one_complete_report_cannot_shield_bare_later_fact(self):
        text = '工程师说，材料已交付，审批已经通过'
        extractor, _ = extractor_for([
            fact('delivery', text), fact('review', text),
            fact('review', '审批已经通过')])
        with self.assertRaisesRegex(ExtractionError, 'candidate_scope_incomplete'):
            extractor(observation(text))

    def test_sentence_boundary_keeps_independent_fact_separate(self):
        text = '工程师说材料已交付。审批已经通过'
        extractor, _ = extractor_for([
            fact('delivery', '工程师说材料已交付'), fact('review', '审批已经通过')])
        self.assertEqual(len(extractor(observation(text))), 2)

    def test_explicit_quoted_report_ends_before_later_independent_clause(self):
        text = '工程师说：“材料已交付”，审批已经通过'
        extractor, _ = extractor_for([
            fact('delivery', '工程师说：“材料已交付”'), fact('review', '审批已经通过')])
        self.assertEqual(len(extractor(observation(text))), 2)

    def test_nonbusiness_quote_does_not_become_a_required_project_fact(self):
        text = '工程师说：“今天天气很好”，审批已经通过'
        extractor, _ = extractor_for([fact('review', '审批已经通过')])
        self.assertEqual(len(extractor(observation(text))), 1)


class SubjectBindingTests(unittest.TestCase):
    def test_unbound_clause_in_multiproject_source_stays_ambiguous(self):
        text = '演示甲项目材料已交付；演示乙项目样机已到仓；随后已开始测试'
        extractor, _ = extractor_for([
            fact('delivery', '演示甲项目材料已交付'),
            fact('delivery', '演示乙项目样机已到仓', subject='b'),
            fact('progress', '随后已开始测试', subject='b')], context=MULTI_CONTEXT)
        with self.assertRaisesRegex(ExtractionError, 'subject_binding_ambiguous'):
            extractor(observation(text))

    def test_single_literal_subject_in_same_sentence_supports_bare_quote(self):
        text = '演示甲项目，材料已交付；演示乙项目，样机已到仓'
        extractor, _ = extractor_for([
            fact('delivery', '材料已交付'),
            fact('delivery', '样机已到仓', subject='b')], context=MULTI_CONTEXT)
        self.assertEqual(len(extractor(observation(text))), 2)

    def test_sentence_title_prevents_cross_comma_reassignment(self):
        text = '演示甲项目，材料已交付；演示乙项目，样机已到仓'
        extractor, _ = extractor_for([
            fact('delivery', '材料已交付', subject='b'),
            fact('delivery', '样机已到仓', subject='b')], context=MULTI_CONTEXT)
        with self.assertRaisesRegex(ExtractionError, 'subject_binding_mismatch'):
            extractor(observation(text))

    def test_bound_occurrence_cannot_hide_an_unbound_repeated_quote(self):
        text = '演示甲项目材料已交付；材料已交付；演示乙项目样机已到仓'
        extractor, _ = extractor_for([
            fact('delivery', '材料已交付'),
            fact('delivery', '演示乙项目样机已到仓', subject='b')], context=MULTI_CONTEXT)
        with self.assertRaisesRegex(ExtractionError, 'subject_binding_ambiguous'):
            extractor(observation(text))

    def test_multiple_allowed_projects_do_not_assign_a_subjectless_claim(self):
        extractor, _ = extractor_for([fact('progress', '仍在测试中')], context=MULTI_CONTEXT)
        with self.assertRaisesRegex(ExtractionError, 'subject_binding_ambiguous'):
            extractor(observation('仍在测试中'))

    def test_valid_existing_conversation_hint_keeps_single_subject_flow(self):
        context = dict(MULTI_CONTEXT, conversation_hint={'subject_id': 'a'})
        extractor, _ = extractor_for([fact('progress', '仍在测试中')], context=context)
        self.assertEqual(len(extractor(observation('仍在测试中'))), 1)

    def test_hint_does_not_hide_other_explicit_project_from_validation(self):
        context = {'subjects': SUBJECTS, 'allowed_subject_ids': ['a', 'b'],
                   'conversation_hint': {'subject_id': 'b'}}
        extractor, coordinator = extractor_for([
            fact('delivery', '演示甲项目材料已交付', subject='b')], context=context)
        with self.assertRaisesRegex(ExtractionError, 'subject_hint_conflict'):
            extractor(observation('演示甲项目材料已交付'))
        self.assertEqual(coordinator.complete.call_count, 0)

    def test_hint_cannot_expand_allowed_subjects(self):
        context = {'subjects': SUBJECTS, 'allowed_subject_ids': ['a'],
                   'conversation_hint': {'subject_id': 'b'}}
        extractor, coordinator = extractor_for([
            fact('progress', '仍在测试中', subject='b')], context=context)
        with self.assertRaisesRegex(ExtractionError, 'subject_outside_context'):
            extractor(observation('仍在测试中'))
        self.assertEqual(coordinator.complete.call_count, 0)

    def test_unknown_hint_does_not_create_new_project(self):
        context = {'subjects': SUBJECTS, 'allowed_subject_ids': ['a', 'b'],
                   'conversation_hint': {'subject_id': 'unknown'}}
        extractor, coordinator = extractor_for([
            fact('progress', '仍在测试中', subject='unknown')], context=context)
        with self.assertRaisesRegex(ExtractionError, 'subject_outside_context'):
            extractor(observation('仍在测试中'))
        self.assertEqual(coordinator.complete.call_count, 0)


class CatalogIntegrationTests(unittest.TestCase):
    def setUp(self):
        self.store = Store(':memory:', clock=lambda: 1000)
        self.store.register_source(Source(
            'synthetic', 'im_chat', 'chat', 'tenant', 'owner', enabled=True,
            settings={'subject_ids': ['a', 'b']}))
        self.catalog = EntityCatalog(self.store)
        self.catalog.register('a', '演示甲项目', aliases=['甲平台'])
        self.catalog.register('b', '演示乙项目', aliases=['乙平台'])

    def tearDown(self):
        self.store.close()

    def test_real_catalog_name_and_registered_alias_prevent_wrong_project(self):
        for text in ('演示甲项目材料已交付', '甲平台材料已交付'):
            with self.subTest(text=text):
                extractor, _ = extractor_for([fact('delivery', text, subject='b')],
                                             provider=self.catalog.context_for)
                with self.assertRaisesRegex(ExtractionError, 'subject_binding_mismatch'):
                    extractor(observation(text))

    def test_correct_catalog_binding_stays_unconfirmed_in_fact_engine(self):
        text = '工程师说甲平台材料已交付'
        obs = observation(text)
        self.store.record(obs)
        extractor, _ = extractor_for([fact('delivery', text)],
                                     provider=self.catalog.context_for)
        values = extractor(obs)
        engine = FactEngine(self.store)
        engine.process(obs, values, extractor_version=extractor.version)
        facts = engine.query('a', Principal('owner', 'tenant'))['facts']
        self.assertEqual(len(facts), 1)
        self.assertEqual(facts[0]['authority'], 'unconfirmed')
        self.assertFalse(facts[0]['protected'])
        self.assertEqual(facts[0]['value'], text)

    def test_duplicate_registered_alias_stays_ambiguous(self):
        self.catalog.register('b', '演示乙项目', aliases=['甲平台'])
        text = '甲平台材料已交付'
        extractor, _ = extractor_for([fact('delivery', text)],
                                     provider=self.catalog.context_for)
        with self.assertRaisesRegex(ExtractionError, 'subject_binding_ambiguous'):
            extractor(observation(text))

    def test_rejected_binding_is_visible_gap_without_partial_facts(self):
        text = '演示甲项目，材料已交付；演示乙项目，样机已到仓'
        self.store.record(observation(text))
        extractor, _ = extractor_for([
            fact('delivery', '材料已交付', subject='b'),
            fact('delivery', '样机已到仓', subject='b')],
            provider=self.catalog.context_for)
        result = ExtractionWorker(self.store, extractor).run_once()
        self.assertEqual(result['status'], 'failed')
        self.assertEqual(self.store.conn.execute(
            'SELECT COUNT(*) FROM fact_candidates').fetchone()[0], 0)
        review = pending_reviews(self.store, Principal('owner', 'tenant'))
        self.assertEqual(review['total'], 1)
        self.assertEqual(review['items'][0]['text'], text)
        self.assertIn('项目归属尚未明确', review['items'][0]['reason'])
        self.assertEqual(pending_reviews(
            self.store, Principal('stranger', 'tenant'))['total'], 0)
        detail = self.store.conn.execute('SELECT detail FROM gaps').fetchone()[0]
        self.assertNotIn(text, detail)

    def test_successful_bounded_check_never_claims_semantic_acceptance(self):
        report = coverage_report('仍在测试中', [fact('progress', '仍在测试中')])
        self.assertEqual(report['issues'], [])
        self.assertEqual(report['semantic_acceptance'], 'requires_review')
        self.assertNotIn('仍在测试中', json.dumps(report, ensure_ascii=False))


if __name__ == '__main__':
    unittest.main()
