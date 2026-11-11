"""R5 collection/legacy ownership through the real Studio assembly.

The transport returns synthetic Feishu responses to actual SDK request builders.
No model, real identity lookup, message send or network connection is exercised.
"""
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
import json
from pathlib import Path
import sys
from threading import Barrier
import time
from types import SimpleNamespace as NS
from unittest.mock import Mock

import pytest
from lark_oapi.api.contact.v3 import GetUserRequest, GetUserResponse

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT.parent / 'FS-AGENT-GROK-POC-20261017-01_R1_Studio' / 'src'))

from am_gateway.collectors import Page, SourceSpec
from am_gateway.models import Event, dumps
from am_gateway.phase1.agents import ProposalRepository
from am_gateway.phase1.service import RuntimeService
from integration.am_multi_ai_host import normalize_message
from integration.assistant_host import bind_assistant
from integration.phase1_runtime import install
from integration.studio_phase1_factory import build_phase1_bundle


OWNER, PEER, BOT = 'ou_owner', 'ou_peer', 'ou_am_bot'
DEMO = 'od_demo_test'


class SyntheticSDK:
    def __init__(self, clock):
        self.clock, self.messages, self.requests = clock, [], []
        self.contact = NS(v3=NS(user=NS(get=Mock(side_effect=self.profile))))
        self.request = Mock(side_effect=self.read)

    def profile(self, request):
        assert isinstance(request, GetUserRequest)
        assert request.user_id == PEER and request.user_id_type == 'open_id'
        assert request.department_id_type == 'open_department_id'
        return GetUserResponse({'code': 0, 'data': {'user': {
            'open_id': PEER, 'name': 'DEMO同事', 'department_ids': [DEMO],
            'status': {'is_activated': True, 'is_resigned': False,
                       'is_frozen': False, 'is_exited': False},
        }}})

    def read(self, request):
        assert request.uri == '/open-apis/im/v1/messages'
        query = dict(request.queries)
        self.requests.append(query)
        items = [m for m in self.messages if m['chat_id'] == query['container_id']]
        return NS(raw=NS(content=dumps({'code': 0, 'data': {
            'items': items, 'has_more': False,
        }}).encode()))

    def add(self, mid='om_request', text='HD 项目进度怎么样？', *, chat='oc_peer',
            when=None, mentions=()):
        created = str(int((self.clock[0] if when is None else when) * 1000))
        raw = {'message_id': mid, 'chat_id': chat, 'create_time': created,
            'update_time': created, 'deleted': False, 'msg_type': 'text',
            'thread_id': '', 'parent_id': '', 'root_id': '',
            'mentions': [{'id': value, 'id_type': 'open_id'} for value in mentions],
            'sender': {'id': PEER, 'id_type': 'open_id', 'sender_type': 'user',
                       'tenant_key': 'tenant-test'},
            'body': {'content': dumps({'text': text})}}
        self.messages.append(raw)
        return raw


def receive_payload(raw):
    return {'header': {'app_id': 'cli_synthetic', 'tenant_key': 'tenant-test',
                       'event_type': 'im.message.receive_v1'},
        'event': {'sender': {'sender_id': {'open_id': raw['sender']['id']},
                            'tenant_key': 'tenant-test', 'sender_type': 'user'},
            'message': {**raw, 'message_type': raw['msg_type'],
                        'chat_type': 'p2p' if raw['chat_id'] == 'oc_peer' else 'group',
                        'content': raw['body']['content']}}}


@pytest.fixture
def rig(tmp_path, monkeypatch):
    monkeypatch.setattr('socket.socket.connect', Mock(side_effect=AssertionError('network forbidden')))
    clock, legacy_ids = [time.time() - 1], set()
    sdk = SyntheticSDK(clock)
    bridge = NS(config={'app_id': 'cli_synthetic'},
        user_allowed=lambda uid: uid in {OWNER, PEER},
        am_self=NS(owner=OWNER, owner_origin=lambda: {'chat': 'oc_owner'},
            query_settings={}, query_identity=NS(verify=Mock(return_value=None)),
            ledger=NS(port=NS(disclosure={'oc_hd'}, guard=Mock(return_value=None)))),
        poc=NS(config=NS(tenant_key='tenant-test', bot_open_id=BOT)))
    settings = {'live_cutover_at': clock[0] - 300, 'group_chat_ids': ['oc_hd'],
        'project_chats': {'HD': ['oc_hd']}, 'send_live': False,
        'assistant': {'enabled': True, 'demo_department_ids': [DEMO], 'routine_fields': {}}}
    bundle = build_phase1_bundle(bridge, {'phase1_gateway': settings}, sdk, tmp_path,
        legacy_control_port=lambda p: p['event']['message']['message_id'] in legacy_ids)
    ingress, _ = install(bundle.ports, activate=False)
    ingress.gateway.store.clock = lambda: clock[0]
    collection = bundle.bind(ingress)
    service = RuntimeService(ingress, collector=collection, ownership=bundle.legacy_ownership)
    service.proposals = ProposalRepository(service.harness.store, OWNER)

    def bind():
        return bind_assistant(service, bundle, bridge, settings, sdk, tmp_path)

    coordinator = bind()
    # A DM source is registered only after an authenticated user event. This
    # seed is that trusted event; subsequent test messages arrive through REST.
    seed = Event('tenant-test', 'om_verified_dm_binding', 'oc_peer', PEER,
                 '身份绑定事件', clock[0], channel='bot_dm')
    bundle.authority.directory.register_dm(seed)
    coordinator.on_dm_bound(seed)
    source = SourceSpec('tenant-test', 'oc_peer', channel='bot_dm', identity='am_bot')
    return NS(c=coordinator, service=service, h=service.harness, store=service.harness.store,
        ingress=ingress, collection=collection, engine=collection.collectors,
        source=source, sdk=sdk, clock=clock, bundle=bundle, settings=settings,
        directory=bundle.authority.directory, legacy=bundle.legacy_ownership,
        legacy_ids=legacy_ids, bind=bind)


def collect(rig, lane='live', *, source=None):
    source = source or rig.source
    with rig.store.transaction() as db:
        db.execute('UPDATE gw_source_scans SET retry_at=0 WHERE source_id=? AND lane=?',
                   (source.source_id, lane))
    return rig.engine.collect_one(source.source_id, lane)


def live_event(rig, raw):
    return normalize_message(receive_payload(raw), rig.h.config)


def test_poll_intake_uses_real_identity_and_reaches_harness_only_in_background(rig):
    rig.sdk.add()
    assert rig.engine.assistant is rig.c
    assert collect(rig)['status'] == 'completed'
    assert rig.store.rows('SELECT * FROM h1_inbox') == []
    pending = rig.store.one('SELECT event_json,state FROM r5_intake')
    assert pending['state'] == 'pending'
    assert json.loads(pending['event_json'])['observed_via'] == 'poll:live'
    admitted = rig.c.intake_once()
    assert admitted['state'] == 'admitted'
    work = rig.c.book.get_task(admitted['task_id'], principal_id=OWNER)
    assert work['requester_id'] == PEER and work['project_id'] == 'HD'
    assert rig.store.one('SELECT accepted FROM h1_inbox')['accepted'] == 1
    assert rig.directory.dm_actor('oc_peer') == PEER


@pytest.mark.parametrize('decline', [False, True])
def test_live_referral_and_owner_decline_cannot_be_bypassed_by_poll(rig, decline):
    raw = rig.sdk.add(text='请帮我处理一个私人事项')
    assert rig.ingress.ingest(receive_payload(raw)).claimed
    referred = rig.c.intake_once()
    assert referred['state'] == 'referred'
    if decline:
        rig.c.handle('POST', '/v1/assistant/tasks/' + referred['task_id'] + '/decision',
                     {'accept': False, 'source_ref': 'owner:decline'}, actor=OWNER)
    assert collect(rig)['status'] == 'completed'
    assert rig.c.intake_once() is None
    assert rig.store.rows('SELECT * FROM h1_inbox') == []
    assert rig.h.route_one() is None
    assert len(rig.store.rows('SELECT task_id FROM a5_wb_tasks')) == 1
    work = rig.c.book.get_task(referred['task_id'], principal_id=OWNER)
    assert work['state'] == ('cancelled' if decline else 'awaiting_owner')


def test_pending_live_capture_is_not_routed_by_overlapping_poll(rig):
    raw = rig.sdk.add()
    assert rig.ingress.ingest(receive_payload(raw)).claimed
    assert collect(rig)['status'] == 'completed'
    assert len(rig.store.rows('SELECT * FROM r5_intake')) == 1
    assert rig.store.rows('SELECT * FROM h1_inbox') == []
    assert rig.c.intake_once()['state'] == 'admitted'
    rig.h.route_one()
    assert len(rig.store.rows('SELECT * FROM h1_tasks')) == 1


def test_restart_keeps_one_assistant_task_across_receive_and_poll(rig):
    raw = rig.sdk.add()
    rig.ingress.ingest(receive_payload(raw))
    admitted = rig.c.intake_once()
    rig.h.route_one()
    rig.c = rig.bind()
    assert collect(rig)['status'] == 'completed'
    assert rig.c.intake_once() is None
    assert len(rig.store.rows('SELECT * FROM h1_tasks')) == 1
    assert [t['task_id'] for t in rig.c.book.owner_brief(principal_id=OWNER)['tasks']] == [admitted['task_id']]


def test_history_collects_old_evidence_without_starting_assistant_work(rig):
    rig.sdk.add(mid='om_old', when=rig.settings['live_cutover_at'] - 10)
    rig.sdk.add(mid='om_newer_than_history')
    assert collect(rig, 'history')['status'] == 'completed'
    assert rig.store.rows('SELECT * FROM r5_intake') == []
    assert [r['native_id'] for r in rig.store.rows('SELECT native_id FROM gw_items')] == ['om_old']
    rig.h.route_one()
    assert rig.store.rows('SELECT * FROM h1_tasks') == []
    assert collect(rig)['status'] == 'completed'
    assert rig.c.intake_once()['state'] == 'admitted'


def test_history_window_cannot_demote_an_already_admitted_live_task(rig):
    raw = rig.sdk.add()
    rig.ingress.ingest(receive_payload(raw))
    rig.c.intake_once()
    task = rig.h.route_one()
    before = rig.h.task(task['task_id'])
    assert collect(rig, 'history')['status'] == 'completed'
    after = rig.h.task(task['task_id'])
    assert after == before
    assert rig.store.one('SELECT accepted FROM h1_inbox')['accepted'] == 1


def test_group_message_without_bot_mention_remains_evidence_only(rig):
    rig.sdk.add(chat='oc_hd')
    source = SourceSpec('tenant-test', 'oc_hd', identity='am_bot', project_ids=('HD',))
    assert collect(rig, source=source)['status'] == 'completed'
    assert rig.store.rows('SELECT * FROM r5_intake') == []
    assert rig.store.one('SELECT accepted FROM h1_inbox')['accepted'] == 0


def test_legacy_command_keeps_existing_ownership_and_is_not_assistant_work(rig):
    rig.sdk.add(mid='om_legacy')
    rig.legacy_ids.add('om_legacy')
    assert collect(rig)['status'] == 'completed'
    assert rig.store.rows('SELECT * FROM r5_intake') == []
    assert rig.store.one('SELECT legacy_owned FROM gw_items')['legacy_owned'] == 1
    assert rig.store.one('SELECT accepted FROM h1_inbox')['accepted'] == 0


def test_capture_failure_does_not_advance_cursor_and_replay_is_idempotent(rig, monkeypatch):
    rig.sdk.add(mid='om_first')
    rig.sdk.add(mid='om_second')
    original = rig.c.capture

    def fail_second(event, **kwargs):
        if event.native_id == 'om_second':
            raise RuntimeError('synthetic interruption after the first durable intake')
        return original(event, **kwargs)

    monkeypatch.setattr(rig.c, 'capture', fail_second)
    assert collect(rig)['status'] == 'error'
    assert len(rig.store.rows('SELECT * FROM r5_intake')) == 1
    assert rig.store.one("SELECT page_count FROM gw_source_scans WHERE source_id=? AND lane='live'",
                         (rig.source.source_id,))['page_count'] == 0
    assert rig.store.rows('SELECT * FROM h1_inbox') == []
    monkeypatch.setattr(rig.c, 'capture', original)
    assert collect(rig)['status'] == 'completed'
    assert len(rig.store.rows('SELECT * FROM r5_intake')) == 2
    assert rig.store.rows('SELECT * FROM h1_inbox') == []
    assert rig.c.intake_once()['state'] == rig.c.intake_once()['state'] == 'admitted'
    assert len(rig.c.book.owner_brief(principal_id=OWNER)['tasks']) == 2


def test_revocation_after_page_read_prevents_assistant_capture(rig, monkeypatch):
    raw = rig.sdk.add()
    event = live_event(rig, raw)
    rig.engine.adapters['feishu_messages'] = NS(read_page=lambda source, request: Page((event,)))
    monkeypatch.setattr(rig.engine, 'source_allowed', Mock(side_effect=[True, False]))
    assert collect(rig)['status'] == 'revoked'
    assert rig.store.rows('SELECT * FROM r5_intake') == []
    assert rig.store.rows('SELECT * FROM h1_inbox') == []


def test_only_explicit_capture_acceptance_suppresses_original_ingestion(rig):
    rig.sdk.add()
    rig.engine.assistant = NS(capture=lambda *args, **kwargs: {'captured': False, 'duplicate': True})
    assert collect(rig)['status'] == 'completed'
    assert rig.store.one('SELECT accepted FROM h1_inbox')['accepted'] == 1


@pytest.mark.parametrize('lane', ['live', 'history'])
def test_registered_dm_accepts_only_the_trusted_collection_lane_contract(rig, lane):
    event = Event('tenant-test', 'om_polled', 'oc_peer', PEER, '项目进展',
                  rig.clock[0], channel='bot_dm', observed_via='poll:' + lane)
    assert rig.directory.register_dm(event).user_id == PEER
    with pytest.raises(ValueError):
        rig.directory.register_dm(replace(event, observed_via='poll:unverified'))


@pytest.mark.parametrize('assistant_first', [True, False])
def test_legacy_and_assistant_intake_have_one_persistent_owner(rig, assistant_first):
    event = live_event(rig, rig.sdk.add())
    if assistant_first:
        assert rig.c.capture(event)['captured']
        assert rig.legacy.should_skip(event)
        assert not rig.legacy.claim(event)
        assert rig.store.rows('SELECT * FROM r4_legacy_claims') == []
    else:
        assert rig.legacy.claim(event)
        assert rig.c.capture(event) is None
        assert rig.store.rows('SELECT * FROM r5_intake') == []


def test_concurrent_legacy_and_assistant_claim_cannot_both_win(rig):
    for index in range(8):
        event = live_event(rig, rig.sdk.add(mid='om_race_' + str(index)))
        start = Barrier(2)

        def assistant_claim():
            start.wait()
            captured = rig.c.capture(event)
            return bool(captured and captured.get('captured'))

        def legacy_claim():
            start.wait()
            return rig.legacy.claim(event)

        with ThreadPoolExecutor(max_workers=2) as pool:
            first = pool.submit(assistant_claim)
            second = pool.submit(legacy_claim)
            assert sum((first.result(), second.result())) == 1
        intake = rig.store.one('SELECT 1 FROM r5_intake WHERE item_id=?', (event.item_id,))
        legacy = rig.store.one('SELECT 1 FROM r4_legacy_claims WHERE item_id=?', (event.item_id,))
        assert bool(intake) != bool(legacy)
