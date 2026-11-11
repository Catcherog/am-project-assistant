from dataclasses import replace
import json
import time
from types import SimpleNamespace as NS
from unittest.mock import Mock

import pytest

from am_gateway.collectors import Page, SourceSpec
from am_gateway.models import Config, ContractError, Event, dumps
from am_gateway.phase1 import Harness, HarnessConfig, Policy
from am_gateway.phase1.contracts import registry
from am_gateway.store import Store
from integration.phase1_collection import HarnessCollector, LegacyOwnership
from integration.phase1_runtime import install
from integration.studio_phase1_factory import build_phase1_bundle, PersonalReadTransport


def rig(tmp_path):
    clock = [1000.0]
    caps = registry()
    caps['conversation.reply'] = replace(caps['conversation.reply'], available=True,
        durable_acceptance=True, evidence_ref='synthetic-receiver')
    config = HarnessConfig('tenant', 'bot', 'owner', 'owner-dm', capabilities=caps)
    h = Harness(Store(tmp_path / 'gateway.sqlite', clock=lambda: clock[0]), config)
    cfg = Config('tenant', 'bot', 'owner', 'owner-dm', live_cutover_at=900)
    policy = Policy('owner', 'fixture', 'owner-dm', ('owner-dm',), allow_reply=True)
    return h, cfg, policy, clock


def test_page_source_inbox_and_cursor_roll_back_together(tmp_path, monkeypatch):
    h, cfg, policy, _ = rig(tmp_path)
    source = SourceSpec('tenant', 'owner-dm', channel='bot_dm')
    events = tuple(Event('tenant', mid, 'owner-dm', 'owner', '你好', 999, channel='bot_dm') for mid in ('a', 'b'))
    adapter = NS(read_page=lambda s, r: Page(events, True, 'page-2'))
    c = HarnessCollector(h, cfg, {'feishu_messages': adapter}, authorization_for=lambda e: policy)
    c.register_source(source)
    real = h.ingest

    def fail_second(event, *args, **kwargs):
        if event.native_id == 'b':
            raise RuntimeError('synthetic crash before checkpoint')
        return real(event, *args, **kwargs)

    monkeypatch.setattr(h, 'ingest', fail_second)
    assert c.collect_one(source.source_id, 'live')['status'] == 'error'
    assert not h.store.rows('SELECT * FROM gw_items')
    assert not h.store.rows('SELECT * FROM h1_inbox')
    scan = h.store.one("SELECT * FROM gw_source_scans WHERE lane='live'")
    assert scan['page_token'] == '' and scan['page_count'] == 0
    monkeypatch.setattr(h, 'ingest', real)
    with h.store.transaction() as db:
        db.execute('UPDATE gw_source_scans SET retry_at=0')
    assert c.collect_one(source.source_id, 'live')['status'] == 'page'
    assert len(h.store.rows('SELECT * FROM h1_inbox')) == 2
    assert h.store.one("SELECT page_token FROM gw_source_scans WHERE lane='live'")['page_token'] == 'page-2'
    assert h.store.one('SELECT coverage FROM h1_sync')['coverage']


def test_history_is_collected_without_replaying_conversations(tmp_path):
    h, cfg, policy, _ = rig(tmp_path)
    source = SourceSpec('tenant', 'owner-dm', channel='bot_dm')
    events = (Event('tenant', 'old', 'owner-dm', 'owner', '你好', 800, channel='bot_dm'),
              Event('tenant', 'new', 'owner-dm', 'owner', '你好', 999, channel='bot_dm'))
    c = HarnessCollector(h, cfg, {'feishu_messages': NS(read_page=lambda s, r: Page(events))},
                         authorization_for=lambda e: policy)
    c.register_source(source)
    assert c.collect_one(source.source_id, 'live')['status'] == 'completed'
    h.route_one(); h.route_one()
    tasks = h.store.rows('SELECT * FROM h1_tasks')
    assert len(tasks) == 1 and tasks[0]['item_id'] == events[1].item_id


def test_revoked_source_is_disabled_before_network_read(tmp_path):
    h, cfg, policy, _ = rig(tmp_path)
    adapter = Mock()
    c = HarnessCollector(h, cfg, {'feishu_messages': adapter}, authorization_for=lambda e: policy,
                         source_allowed=lambda source: False)
    source = SourceSpec('tenant', 'owner-dm', channel='bot_dm')
    c.register_source(source)
    assert c.collect_one(source.source_id, 'live')['status'] == 'revoked'
    adapter.read_page.assert_not_called()
    assert h.store.one('SELECT active FROM gw_sources')['active'] == 0


@pytest.mark.parametrize('harness_first', [False, True])
def test_old_poller_and_new_ingress_have_one_durable_owner(tmp_path, harness_first):
    h, _, policy, _ = rig(tmp_path)
    own = LegacyOwnership(h)
    event = Event('tenant', 'same', 'owner-dm', 'owner', '你好', 999, channel='bot_dm')
    if harness_first:
        h.ingest(event, policy)
        assert own.should_skip(event) and not own.claim(event)
        assert h.route_one()['intent'] == 'conversation.reply'
    else:
        assert own.claim(event)
        assert not h.ingest(event, policy).handled
        h.route_one()
        assert not h.store.rows('SELECT * FROM h1_tasks')
    restarted = LegacyOwnership(Harness(Store(h.store.path), h.config))
    assert restarted.should_skip(event)
    assert not restarted.claim(event)
    if not harness_first:
        restarted.finish(event.item_id, state='completed', result_ref='receipt-1')
        assert restarted.status(event.item_id)['result_ref'] == 'receipt-1'


class DisclosurePort:
    def __init__(self):
        self.disclosure = {'oc_group'}

    def guard(self, chat, actor):
        if chat not in self.disclosure:
            raise PermissionError('group revoked')


def studio(tmp_path, *, personal=False):
    port = DisclosurePort()
    owner = NS(owner='ou_owner', owner_origin=lambda: {'chat': 'oc_owner'},
               query_settings={}, query_identity=NS(verify=lambda actor: True), ledger=NS(port=port))
    bridge = NS(config={'app_id': 'cli_synthetic'},
                user_allowed=lambda actor: actor in {'ou_owner', 'ou_colleague'},
                am_self=owner, poc=NS(config=NS(tenant_key='tenant', bot_open_id='ou_bot')))
    settings = {'live_cutover_at': time.time() - 300, 'group_chat_ids': ['oc_group'],
                'project_chats': {'P': ['oc_group']}, 'send_live': False}
    transport = Mock()
    if personal:
        settings.update(personal_read_enabled=True, personal_identity='owner_cli',
                        personal_chat_ids=['oc_colleague'], personal_assistance_enabled=True)
    client = NS(request=Mock(side_effect=AssertionError('construction must not read network')))
    bundle = build_phase1_bundle(bridge, {'phase1_gateway': settings}, client, tmp_path,
                                personal_transport=transport if personal else None,
                                legacy_control_port=lambda payload: False)
    ingress, scheduler = install(bundle.ports, activate=False)
    collection = bundle.bind(ingress)
    return NS(bundle=bundle, ingress=ingress, collection=collection, bridge=bridge,
              settings=settings, client=client, transport=transport, port=port)


def raw(mid='om_message', chat='oc_group', actor='ou_colleague', *, files=(), tick=0):
    now = int(time.time() * 1000) - 10000
    return {'message_id': mid, 'chat_id': chat, 'create_time': str(now), 'update_time': str(now + tick),
            'sender': {'id': actor, 'id_type': 'open_id', 'sender_type': 'user', 'tenant_key': 'tenant'},
            'msg_type': 'post', 'deleted': False, 'mentions': [],
            'body': {'content': dumps({'zh_cn': {'content': [[{'tag': 'text', 'text': '项目交付在周五'}]]},
                                      'files': [{'file_key': f, 'file_name': f} for f in files]})}}


def test_factory_is_network_free_and_live_and_poll_share_complete_snapshots(tmp_path):
    s = studio(tmp_path)
    s.client.request.assert_not_called()
    assert not s.bundle.sender.enabled
    assert s.bundle.ports.snapshot_reader is s.bundle.readers
    source = SourceSpec('tenant', 'oc_group', identity='am_bot', project_ids=('P',))
    msg = raw(files=('old',))
    s.ingress.ingest_feishu_snapshot(msg, source=source)
    msg = {**msg, 'update_time': str(int(msg['update_time']) + 1),
           'body': raw(files=())['body']}
    s.client.request.side_effect = None
    s.client.request.return_value = NS(raw=NS(content=dumps({'code': 0, 'data': {'items': [msg], 'has_more': False}}).encode()))
    outcome = s.collection.collectors.collect_one(source.source_id, 'live')
    assert outcome['status'] == 'completed'
    event = json.loads(s.ingress.gateway.store.one('SELECT event_json FROM gw_items')['event_json'])
    assert event['attachments'] == []
    assert len(s.ingress.gateway.store.rows('SELECT * FROM gw_items')) == 1


def test_owner_can_read_project_sources_in_private_but_other_groups_cannot(tmp_path):
    s = studio(tmp_path)
    h = s.ingress.gateway
    source = SourceSpec('tenant', 'oc_group', identity='am_bot', project_ids=('P',))
    s.ingress.ingest_feishu_snapshot(raw(), source=source)
    assert s.bundle.authority.project_for_owner() == 1
    query = Event('tenant', 'om_query', 'oc_owner', 'ou_owner', '项目进展如何', time.time(), channel='bot_dm')
    policy = s.bundle.authority(query)
    snapshot = h.information.snapshot(policy)
    assert any(item['body'] == '项目交付在周五' for item in snapshot['evidence_items'])
    outsider = replace(policy, reply_target='oc_other')
    assert not h.information.snapshot(outsider)['evidence_items']
    s.port.disclosure.clear()
    fresh = s.bundle.authority(query)
    assert 'oc_group' not in fresh.source_chats
    assert not h.information.snapshot(fresh)['evidence_items']


def test_personal_identity_is_separate_and_own_sent_messages_do_not_trigger_assistance(tmp_path):
    s = studio(tmp_path, personal=True)
    source = SourceSpec('tenant', 'oc_colleague', channel='personal_dm', adapter='personal_messages', identity='owner_cli')
    s.ingress.ingest_feishu_snapshot(raw('om_my_sent', 'oc_colleague', 'ou_owner'), source=source)
    own = s.ingress.gateway.store.one('SELECT * FROM gw_items')
    assert own['is_self'] == 1
    assert not s.ingress.gateway.store.one('SELECT accepted FROM h1_inbox')['accepted']
    s.ingress.ingest_feishu_snapshot(raw('om_incoming', 'oc_colleague', 'ou_colleague'), source=source)
    row = s.ingress.gateway.store.one('SELECT policy_json FROM h1_inbox WHERE item_id=?',
        (Event('tenant', 'om_incoming', 'oc_colleague', 'ou_colleague', '', 1).item_id,))
    assert Policy.parse(row['policy_json']).reply_target == 'oc_owner'
    with pytest.raises(ContractError):
        PersonalReadTransport(s.transport, tenant_id='tenant', owner_id='ou_owner', identity='am_bot', chats=['oc_colleague'])


def test_personal_inventory_is_paginated_and_cannot_expand_approved_chats(tmp_path):
    s = studio(tmp_path, personal=True)
    from integration.studio_phase1_factory import ApprovedPersonalDiscovery
    from am_gateway.discovery import DiscoverySpec
    transport = Mock(return_value={'status': 'ok', 'has_more': True, 'page_token': 'next',
        'complete_for_scope': True, 'sources': [
            {'tenant_id': 'tenant', 'native_id': c, 'identity': 'owner_cli', 'channel': 'personal_dm'}
            for c in ('oc_colleague', 'oc_unapproved')]})
    adapter = ApprovedPersonalDiscovery(transport, ['oc_colleague'], 'owner_cli', lambda chat: ())
    page = adapter.discover_page(DiscoverySpec('tenant', 'private', 'adapter', 'owner_cli', 'personal_dm'), 'first')
    assert [source.native_id for source in page.sources] == ['oc_colleague']
    assert page.has_more and page.next_page_token == 'next'
    assert transport.call_args.args[0]['page_token'] == 'first'


def test_current_disclosure_revocation_reaches_existing_tasks_without_new_message(tmp_path):
    s = studio(tmp_path)
    h = s.ingress.gateway
    msg = raw('om_task', 'oc_group', 'ou_owner')
    msg['mentions'] = [{'id': 'ou_bot'}]
    msg['msg_type'], msg['body'] = 'text', {'content': dumps({'text': '你好'})}
    s.ingress.ingest_feishu_snapshot(msg, source=SourceSpec('tenant', 'oc_group', identity='am_bot'))
    task = h.route_one()
    s.port.disclosure.clear()
    assert s.bundle.authority.refresh() == [task['task_id']]
    assert Policy.parse(h.task(task['task_id'])['policy_json']).revoked
