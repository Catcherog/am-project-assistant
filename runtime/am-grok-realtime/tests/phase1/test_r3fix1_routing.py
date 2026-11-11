"""Regression probes for status questions and the scope of negated actions."""
import json

import pytest

from am_gateway.phase1 import routing
from conftest import admit


@pytest.mark.parametrize('text', [
    '请问生成报告了吗？',
    '请问修改报告了吗？',
    '请问运行脚本了吗？',
    '请问报告生成了没有?',
    '报告发给客户了吗？',
    '报告生成并发给客户了吗？',
    'A 项目的计划更新了吗？',
    '功能安全报告生成了吗？',
])
@pytest.mark.parametrize('passive,intent', [(False, 'project.query'), (True, 'work_message.assist')])
def test_completed_action_questions_are_queries(text, passive, intent):
    route = routing.rules(text, passive=passive)
    assert route['intent'] == intent
    assert route['secondary_intents'] == []


@pytest.mark.parametrize('text', [
    '不要通知客户，只查一下进展',
    '不要发给客户，只查一下进展',
    '请不要发送给客户，只查一下进展',
    '不用转发给同事，只查一下进展',
    '无需抄送大家，只查一下进展',
    '不要修改台账，只查一下进展',
    '不要通知客户然后查一下进展',
    '不要发给客户而是查一下进展',
    '不要生成报告,只查一下进展',
    '查一下进展并不要通知客户',
])
def test_negated_actions_do_not_block_query_or_add_delivery(text):
    route = routing.rules(text)
    assert route['intent'] == 'project.query'
    assert route['secondary_intents'] == []


@pytest.mark.parametrize('text,intent,secondary', [
    ('能帮我生成报告吗？', 'document.process', []),
    ('可以帮我修改报告吗？', 'document.process', []),
    ('能帮我运行脚本吗？', 'code.execute', []),
    ('查一下进展，再发给客户', 'project.query', ['delivery.extra']),
    ('请问生成报告了吗？然后发给客户', 'project.query', ['delivery.extra']),
    ('不要通知客户，然后生成报告', 'document.process', []),
    ('不要生成报告，而是运行脚本', 'code.execute', []),
    ('不要生成报告然后更新台账', 'project.execute', []),
    ('不要修改台账，而是生成报告并发给客户', 'document.process', ['delivery.extra']),
    ('不要通知客户，然后查一下进展并发给同事', 'project.query', ['delivery.extra']),
    ('查询进展并更新台账', 'project.execute', []),
    ('查询进展，然后生成报告', 'document.process', []),
    ('生成报告，然后更新台账', 'document.process', ['project.execute']),
    ('生成报告，然后运行脚本', 'document.process', ['code.execute']),
    ('生成报告，再发给客户，然后更新台账', 'document.process', ['delivery.extra', 'project.execute']),
    ('修改项目文档', 'document.process', []),
    ('修改项目脚本', 'code.execute', []),
    ('修改项目文档并更新台账', 'document.process', ['project.execute']),
])
def test_affirmative_requests_and_other_clauses_are_preserved(text, intent, secondary):
    route = routing.rules(text)
    assert route['intent'] == intent
    assert route['secondary_intents'] == secondary


@pytest.mark.parametrize('text', [
    '要不要生成报告？',
    '是否需要运行脚本？',
    '如果需要生成报告，只查一下进展',
    '现在能生成报告了吗？',
    '不要这个结果，只查一下进展',
])
def test_unsupported_or_ambiguous_action_scope_uses_classifier(text):
    assert routing.rules(text) is None


@pytest.mark.parametrize('text', [
    '补充：如果交期变化，需要生成报告',
    '补充：是否需要运行脚本仍待工程师确认',
])
def test_explicit_information_supplement_keeps_its_existing_entry(text):
    assert routing.rules(text)['intent'] == 'context.supplement'


@pytest.mark.parametrize('text', [
    '请问生成报告了吗？',
    '请问运行脚本了吗？',
    '不要通知客户，只查一下进展',
    '不要发给客户，只查一下进展',
])
def test_full_harness_resolves_queries_without_calling_terra(rig, text):
    h, _ = rig

    class MustNotClassify:
        def classify(self, *args):
            raise AssertionError('an explicit query must not use Terra')

    h.classifier = MustNotClassify()
    task = admit(h, text=text)
    assert task['intent'] == 'project.query'
    assert task['route_state'] == 'resolved'
    assert task['dispatch_state'] == 'pending'
    assert json.loads(task['route_json'])['secondary_intents'] == []
    assert json.loads(task['request_json'])['body'] == text


@pytest.mark.parametrize('text,intent,secondary', [
    ('查一下进展，再发给客户', 'project.query', ['delivery.extra']),
    ('生成报告，然后更新台账', 'document.process', ['project.execute']),
])
def test_full_harness_keeps_unavailable_compound_actions_visible(rig, text, intent, secondary):
    h, _ = rig
    task = admit(h, text=text)
    assert task['intent'] == intent
    assert task['route_state'] == 'capability_unavailable'
    assert json.loads(task['route_json'])['secondary_intents'] == secondary
    assert h.claim() is None
