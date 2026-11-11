from copy import deepcopy
from dataclasses import replace
import json
import sys
import threading
import time
from pathlib import Path
from types import SimpleNamespace as NS

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'tests' / 'r5'))
sys.path.insert(0, str(ROOT / 'tests' / 'r6'))
from test_assistant_coordinator import rig, message
from test_fast_path_integration import bind, snapshot
from am_gateway.assistant.intelligence import (
    ReplyIntelligence, ReplyPending, IntelligentProjectQueries, AdaptiveReplyIntelligence, default_plan,
    fact_packet, render_plan, validate_plan)
from am_gateway.models import ContractError
from integration.assistant_intelligence import bind_reply_intelligence


def example():
    return {'status': 'found', 'project_ids': ['p'], 'matches': [{'id': 'p', 'title': '样例甲'}],
        'source_ref': 'fixture:ledger', 'source_version': 'v1',
        'body': '样例甲（按台账记录）\n进展：计划月底测试，目前尚未启动。\n卡点／风险：工装未到。\n下一步：工装到后测试。\n备注：须验证通过后才能结项。'}


class Planner:
    def __init__(self, release=None, invalid=False):
        self.calls, self.requests = 0, []
        self.release, self.invalid = release, invalid
        self.started, self.finished = threading.Event(), threading.Event()

    def compose(self, request, *, timeout_seconds):
        self.calls += 1
        self.requests.append(request)
        self.started.set()
        if self.release is not None:
            assert self.release.wait(2), 'test failed to release model'
        result = default_plan(request['facts'], layout='compact')
        if self.invalid:
            result['text'] = '测试已经完成。'
        self.finished.set()
        return result


def completed(engine, event, actor, result):
    end = time.monotonic() + 2
    while True:
        try:
            return engine.prepare(event, actor, result, grant_ids=['p'])
        except ReplyPending:
            if time.monotonic() > end:
                raise AssertionError('planner failed to settle')
            threading.Event().wait(.003)


@pytest.mark.parametrize('mutation', ['omit', 'duplicate', 'invent', 'rewrite', 'stale', 'move_condition'])
def test_model_cannot_change_or_drop_facts(mutation):
    packet = fact_packet(example()); plan = default_plan(packet)
    if mutation == 'omit': plan['order'].pop()
    elif mutation == 'duplicate': plan['order'][-1] = plan['order'][0]
    elif mutation == 'invent': plan['order'][-1] = 'nonexistent'
    elif mutation == 'rewrite': plan['text'] = '现在已经测试完成'
    elif mutation == 'stale': plan['facts_digest'] = 'old'
    else: plan['order'].reverse()
    with pytest.raises(ContractError):
        render_plan(plan, packet)


def test_both_layouts_preserve_all_qualifications_and_no_truncation():
    packet = fact_packet(example())
    for layout in ['paragraph', 'compact']:
        body = render_plan(default_plan(packet, layout=layout), packet)
        for atom in packet['facts']:
            assert atom['text'] in body
        assert '已经完成' not in body


def test_cross_section_condition_cannot_move_after_its_consequence():
    source = example()
    source['body'] = '样例甲（按台账记录）\n进展：如果工装尚未到，则按下面安排。\n下一步：推迟测试。'
    packet = fact_packet(source); plan = default_plan(packet)
    plan['order'].reverse()
    with pytest.raises(ContractError, match='preserve_source_conditions'):
        render_plan(plan, packet)


def test_nonstandard_or_multiple_projects_keep_original_whole(rig):
    engine = ReplyIntelligence(rig.store, rig.cfg.owner_id)
    result = example(); result['matches'].append({'id': 'q', 'title': '样例乙'})
    event = message(rig)
    assert engine.prepare(event, rig.directory.resolve(event.sender_id), result) is result


def test_one_model_call_persists_selection_and_reuses_scoped_cache(rig):
    planner = Planner()
    engine = ReplyIntelligence(rig.store, rig.cfg.owner_id, adapter=planner)
    event = message(rig); actor = rig.directory.resolve(event.sender_id)
    first = completed(engine, event, actor, example())
    assert first['reply_intelligence']['route'] == 'model_plan'
    assert completed(engine, event, actor, example()) == first and planner.calls == 1
    second = completed(engine, replace(event, native_id='om_second'), actor, example())
    assert second['reply_intelligence']['route'] == 'model_cache' and planner.calls == 1
    assert second['reply_intelligence']['model_calls'] == 0


def test_deadline_fallback_does_not_wait_for_slow_model_or_accept_late_rewrite(rig):
    release = threading.Event(); planner = Planner(release)
    mono = [0.0]
    engine = ReplyIntelligence(rig.store, rig.cfg.owner_id, adapter=planner, clock=lambda: mono[0])
    event = message(rig); actor = rig.directory.resolve(event.sender_id)
    with pytest.raises(ReplyPending):
        engine.prepare(event, actor, example(), grant_ids=['p'])
    assert planner.started.wait(1)
    mono[0] = 2.0
    selected = engine.prepare(event, actor, example(), grant_ids=['p'])
    assert selected['reply_intelligence']['route'] == 'bounded_fallback'
    release.set(); assert planner.finished.wait(1)
    # Wait for the result to settle in the durable cache, not just the mock.
    end = time.monotonic() + 1
    while engine.health()['busy'] and time.monotonic() < end:
        threading.Event().wait(.003)
    assert engine.prepare(event, actor, example(), grant_ids=['p']) == selected
    next_event = replace(event, native_id='om_later')
    later = engine.prepare(next_event, actor, example(), grant_ids=['p'])
    assert later['reply_intelligence']['route'] == 'model_cache'


def test_busy_model_never_queues_another_interactive_inference(rig):
    release = threading.Event(); planner = Planner(release)
    engine = ReplyIntelligence(rig.store, rig.cfg.owner_id, adapter=planner)
    event = message(rig); actor = rig.directory.resolve(event.sender_id)
    try:
        with pytest.raises(ReplyPending):
            engine.prepare(event, actor, example(), grant_ids=['p'])
        other = engine.prepare(replace(event, native_id='om_other'), actor, example(), grant_ids=['p'])
        assert other['reply_intelligence']['reason'] == 'model_lane_busy'
        assert planner.started.wait(1) and planner.calls == 1
    finally:
        release.set(); planner.finished.wait(1)


def test_invalid_plan_falls_back_without_an_extra_repair_model(rig):
    planner = Planner(invalid=True)
    engine = ReplyIntelligence(rig.store, rig.cfg.owner_id, adapter=planner)
    event = message(rig)
    result = completed(engine, event, rig.directory.resolve(event.sender_id), example())
    assert result['reply_intelligence']['route'] == 'bounded_fallback'
    assert '测试已经完成' not in result['body'] and planner.calls == 1


def test_cache_does_not_cross_audience_grants_question_or_changed_facts(rig):
    planner = Planner(); engine = ReplyIntelligence(rig.store, rig.cfg.owner_id, adapter=planner)
    event = message(rig); actor = rig.directory.resolve(event.sender_id)
    completed(engine, event, actor, example())
    changed = deepcopy(example()); changed['body'] = changed['body'].replace('工装未到', '工装已到，待核验')
    completed(engine, replace(event, native_id='om_new_fact'), actor, changed)
    assert planner.calls == 2
    completed(engine, replace(event, native_id='om_group', chat_id='oc_hd', channel='group'), actor, example())
    assert planner.calls == 3
    completed(engine, replace(event, native_id='om_question', body='另问人员分工'), actor, example())
    assert planner.calls == 4


def test_restart_recovers_pending_without_relaunching_model(rig):
    release = threading.Event(); planner = Planner(release)
    engine = ReplyIntelligence(rig.store, rig.cfg.owner_id, adapter=planner)
    event = message(rig); actor = rig.directory.resolve(event.sender_id)
    try:
        with pytest.raises(ReplyPending):
            engine.prepare(event, actor, example(), grant_ids=['p'])
        assert planner.started.wait(1)
        recovered = ReplyIntelligence(rig.store, rig.cfg.owner_id, adapter=planner)
        result = recovered.prepare(event, actor, example(), grant_ids=['p'])
        assert result['reply_intelligence']['reason'] == 'recovered_incomplete_attempt'
        assert planner.calls == 1
    finally:
        release.set(); planner.finished.wait(1)


def test_actual_intake_and_delivery_preserve_composed_fact_binding(rig):
    snap, reader = bind(rig)
    planner = Planner(); engine = ReplyIntelligence(rig.store, rig.cfg.owner_id, adapter=planner)
    rig.c.project_reader = IntelligentProjectQueries(reader, engine)
    event = message(rig, '示例平台进展如何', projects=())
    rig.c.capture(event)
    first = rig.c.intake_once()
    assert first['state'] == 'source_wait'
    assert planner.finished.wait(1)
    rig.clock[0] += .3
    end = time.monotonic() + 1
    while engine.health()['busy'] and time.monotonic() < end:
        threading.Event().wait(.003)
    assert rig.c.intake_once()['state'] == 'local_ready'
    assert rig.c.local_delivery_once() == 1 and len(rig.sends) == 1
    assert '预计周一' in rig.sends[0]['payload']['body']
    assert planner.calls == 1
    assert rig.c.local_delivery_once() == 0


def test_binding_disabled_preserves_r6_and_enabled_reports_missing_provider(rig):
    _, reader = bind(rig)
    assert bind_reply_intelligence(reader, NS(), rig.store, rig.cfg.owner_id, {}) is reader
    wrapper = bind_reply_intelligence(reader, NS(), rig.store, rig.cfg.owner_id,
        {'intelligence': {'enabled': True}})
    event = message(rig, '示例平台进展', projects=())
    result = wrapper(event, rig.directory.resolve(event.sender_id), ['HD'])
    assert result['reply_intelligence']['reason'] == 'model_unbound'
    assert wrapper.health()['reply_intelligence']['model_bound'] is False


def test_configuration_activation_affects_new_request_only_even_after_restart(rig):
    active = {'version': 1, 'config': {'reply.layout': 'paragraph'}}
    base = ReplyIntelligence(rig.store, rig.cfg.owner_id)
    engine = AdaptiveReplyIntelligence(base, lambda: active)
    event = message(rig); actor = rig.directory.resolve(event.sender_id)
    old = engine.prepare(event, actor, example(), grant_ids=['p'])
    active = {'version': 2, 'config': {'reply.layout': 'compact', 'reply.max_chars': 240}}
    newer = engine.prepare(replace(event, native_id='om_new_policy'), actor, example(), grant_ids=['p'])
    assert old['body'] != newer['body']
    assert '目前的进展：' in old['body'] and '\n进展：' in newer['body']
    assert engine.prepare(event, actor, example(), grant_ids=['p']) == old
    recovered = AdaptiveReplyIntelligence(ReplyIntelligence(rig.store, rig.cfg.owner_id), lambda: active)
    assert recovered.prepare(event, actor, example(), grant_ids=['p']) == old
    for atom in fact_packet(example())['facts']:
        assert atom['text'] in newer['body']


def test_unbound_model_cannot_be_selected_by_configuration_pointer(rig):
    base = ReplyIntelligence(rig.store, rig.cfg.owner_id)
    engine = AdaptiveReplyIntelligence(base, lambda: {'config': {'reply.model_id': 'unverified_provider'}})
    event = message(rig)
    with pytest.raises(ContractError, match='model_binding'):
        engine.prepare(event, rig.directory.resolve(event.sender_id), example())


def test_configuration_change_cannot_create_a_second_model_lane(rig):
    release = threading.Event(); planner = Planner(release)
    active = {'config': {'reply.layout': 'paragraph'}}
    base = ReplyIntelligence(rig.store, rig.cfg.owner_id, adapter=planner)
    engine = AdaptiveReplyIntelligence(base, lambda: active)
    event = message(rig); actor = rig.directory.resolve(event.sender_id)
    try:
        with pytest.raises(ReplyPending):
            engine.prepare(event, actor, example())
        assert planner.started.wait(1)
        active = {'config': {'reply.layout': 'compact'}}
        result = engine.prepare(replace(event, native_id='om_new_config'), actor, example())
        assert result['reply_intelligence']['reason'] == 'model_lane_busy'
        assert planner.calls == 1
    finally:
        release.set(); planner.finished.wait(1)


def test_semantic_project_mode_does_not_add_a_layout_model_call(rig):
    _, reader = bind(rig)
    assert bind_reply_intelligence(reader, NS(), rig.store, rig.cfg.owner_id,
        {'project_answer_mode': 'semantic', 'intelligence': {'enabled': True}}) is reader
