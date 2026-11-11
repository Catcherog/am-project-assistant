"""Synthetic source-gap regressions through the real Harness/receiver path.

These verify the source boundary, not real-model semantic quality or latency.
"""
import time
from types import SimpleNamespace as NS

import pytest

from am_gateway.phase1.routing import result as route_result
from am_gateway.phase1.service import RuntimeService
from test_interactive import result, rig


@pytest.mark.parametrize('source_state', ['unavailable', 'unbound'])
def test_passive_current_query_cannot_reuse_old_fact_when_source_unavailable(rig, source_state):
    previous = rig.submit('样例甲项目当前进展如何？', channel='personal_dm', chat='personal-chat')
    old = result(rig)
    effect = rig.h.deliver_recorded_result(previous['task_id'],
        lambda **kwargs: {'platform_id': 'synthetic-visible-old-reply'})
    rig.h.verify_delivery(previous['task_id'], {
        'platform_id': effect['platform_id'], 'target': 'owner-dm', 'body': old['reply_text'],
        'sender_id': 'bot', 'platform_time': time.time(),
    })
    assert rig.receiver.publish_delivered_memory(rig.h) == 1
    assert rig.store.one('SELECT body FROM h4_receiver_memory WHERE role=?', ('assistant',))['body'] == old['reply_text']

    if source_state == 'unavailable':
        rig.snapshots.value = None
    else:
        rig.runner.source_loader = None
    # This response would reuse a stale fact if a second model call occurred.
    rig.model.transform = lambda value, request: {
        **value, 'reply_text': old['reply_text'], 'evidence_refs': [],
    }
    rig.submit('样例甲项目最新进展如何？', channel='personal_dm', chat='personal-chat')
    assert rig.task['intent'] == 'work_message.assist'
    answer = result(rig)
    assert answer['response_kind'] == 'data_gap'
    assert answer['model_provenance']['model_run_calls'] == 0
    assert len(rig.model.requests) == 1
    assert '测试尚未启动' not in answer['reply_text']

    sent = []
    service = RuntimeService(NS(gateway=rig.h, scheduler=None),
        sender=lambda **kwargs: sent.append(kwargs) or {'platform_id': 'synthetic-source-gap-reply'})
    service.interactive_runner = rig.runner
    service.delivery_once()
    assert len(sent) == 1 and sent[0]['body'] == answer['reply_text']
    assert all('测试尚未启动' not in item['body'] for item in sent)


def test_general_knowledge_conversation_remains_available_without_project_sources(rig):
    rig.snapshots.value = None
    rig.h.classifier = NS(classify=lambda event, budget: route_result('conversation.reply'))
    rig.model.transform = lambda value, request: {
        **value, 'reply_text': '光合作用利用光能，将二氧化碳和水转化为有机物。', 'evidence_refs': [],
    }
    rig.submit('光合作用是什么？')
    assert rig.task['intent'] == 'conversation.reply'
    answer = result(rig)
    assert answer['response_kind'] == 'sufficient'
    assert answer['reply_text'].startswith('光合作用')
    assert len(rig.model.requests) == 1


def test_passive_current_query_requires_current_project_citation(rig):
    rig.model.transform = lambda value, request: {**value, 'evidence_refs': []}
    rig.submit('样例甲项目当前进展如何？', channel='personal_dm', chat='personal-chat')
    answer = result(rig)
    assert answer['response_kind'] == 'failed'
    assert answer['remaining_requirements'] == ['interactive_output:current_project_evidence_not_cited']
    assert len(rig.model.requests) == 1


def test_passive_current_query_cannot_claim_full_answer_from_partial_sources(rig):
    rig.source.max_rows = 1
    rig.submit('这些项目当前进展如何？', channel='personal_dm', chat='personal-chat')
    answer = result(rig)
    assert answer['response_kind'] == 'partial'
    assert '部分授权台账尚未完整纳入本次资料，需要继续核对。' in answer['remaining_requirements']
    assert len(rig.model.requests) == 1


def test_passive_current_query_keeps_context_budget_source_gate(rig):
    rig.snapshots.value['rows'][0]['项目进展'] = '合成超长来源，不能删限定。' * 3000
    rig.submit('样例甲项目当前进展如何？', channel='personal_dm', chat='personal-chat')
    answer = result(rig)
    assert answer['response_kind'] == 'data_gap'
    assert answer['remaining_requirements'] == ['current_project_evidence_context_budget']
    assert rig.model.requests == []
