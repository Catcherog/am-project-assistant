"""Native binding integration with the installed SDK and synthetic raw replies.

Uses the actual Bridge.user_allowed implementation without initializing a live
Bridge or opening its databases. SDK request I/O is replaced, not the reader,
scope verifier, normalizer, Harness or Store. No real Feishu call is made.
"""
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace as NS
from unittest.mock import Mock
import json
import sys
import time

import lark_oapi as lark
import pytest

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / 'local-qwen3.8/feishu-bridge'))
sys.path.insert(0, str(ROOT / 'FS-AGENT-GROK-POC-20261017-01_R1_Studio/src'))
from bridge import Bridge
from am_gateway.collectors import SourceSpec
from am_gateway.models import ContractError
from am_gateway.phase1 import HarnessConfig, Policy
from integration.phase1_runtime import Phase1Ports, install
from integration.studio_host import BindingError, HostScope, ScopedReadTransport
from integration.studio_phase1_snapshot import bind_snapshot_ports


@pytest.fixture
def host(tmp_path, monkeypatch):
    # This is a real SDK instance with synthetic credentials and blocked I/O.
    client = lark.Client.builder().app_id('cli_synthetic').app_secret('synthetic-only').build()
    request = Mock(side_effect=AssertionError('unexpected SDK request'))
    monkeypatch.setattr(client, 'request', request)
    monkeypatch.setattr('socket.socket.connect', Mock(side_effect=AssertionError('network forbidden')))
    bridge = object.__new__(Bridge)
    bridge.config = {'app_id': 'cli_synthetic', 'access_mode': 'allowlist',
                     'allowed_open_ids': ['ou_native_owner']}
    cfg = HarnessConfig('synthetic-tenant', 'ou_native_bot', 'ou_native_owner', 'oc_native_dm')
    scope = HostScope(bridge, cfg, ['oc_native_group'])
    policy = Mock(return_value=None)
    ports = Phase1Ports(cfg, str(tmp_path / 'isolated.sqlite'), scope.verified,
                        policy, lambda p: (), Mock(return_value=False))
    return NS(client=client, request=request, bridge=bridge, ports=ports, policy=policy,
              source=SourceSpec(cfg.tenant_id, 'oc_native_group', identity='am_bot'),
              created=int(time.time() * 1000) - 10000)


def bind(host, **kwargs):
    return bind_snapshot_ports(host.ports, bridge=host.bridge, client=host.client,
                               approved_group_chats=['oc_native_group'], **kwargs)


def item(host, tick, file_key, **changes):
    content = {'zh_cn': {'content': [[{'tag': 'text', 'text': '项目当前进展'}]]},
               'files': [{'file_key': file_key, 'file_name': file_key + '.pdf'}] if file_key else []}
    raw = {'message_id': 'om_native_known', 'chat_id': 'oc_native_group',
           'create_time': str(host.created), 'update_time': str(host.created + tick),
           'msg_type': 'post', 'deleted': False, 'body': {'content': json.dumps(content)},
           'mentions': [{'id': host.ports.config.bot_id, 'id_type': 'open_id'}],
           'sender': {'id': host.ports.config.owner_id, 'id_type': 'open_id',
                      'sender_type': 'user', 'tenant_key': host.ports.config.tenant_id}}
    raw.update(changes)
    return raw


def respond(host, raw):
    host.request.side_effect = None
    host.request.return_value = NS(raw=NS(content=json.dumps({'code': 0, 'data': {'items': [raw]}}).encode()))


def stored(ingress):
    row = ingress.gateway.store.one('SELECT * FROM gw_items WHERE native_id=?', ('om_native_known',))
    return json.loads(row['event_json'])


@pytest.mark.parametrize('authorized', [False, True])
def test_bound_sdk_refresh_replacement_clear_weak_observation_and_recovery(host, authorized):
    if authorized:
        host.policy.return_value = Policy('ou_native_owner', 'synthetic-only', 'oc_native_group',
                                          ('oc_native_group',), allow_reply=True)
    ports = bind(host)
    assert ports.authorization_for is host.policy
    assert ports.legacy_control is host.ports.legacy_control
    assert ports.verified_scope is host.ports.verified_scope
    assert host.ports.snapshot_reader is None
    assert ports.snapshot_reader.client is host.client
    ingress, scheduler = install(ports, activate=False)
    assert scheduler.thread is None and scheduler.classifier_thread is None
    host.request.assert_not_called()
    observed = []
    for tick, key in [(1000, 'old'), (2000, 'new'), (3000, None)]:
        respond(host, item(host, tick, key))
        admission = ingress.refresh_message('om_native_known', source=host.source)
        assert admission.captured and admission.claimed == authorized
        observed.append([a['resource_key'] for a in stored(ingress)['attachments']])
    assert observed == [['old'], ['new'], []]
    good = stored(ingress)
    weak = item(host, 9000, 'old')
    payload = {'header': {'app_id': host.bridge.config['app_id'],
                          'tenant_key': host.ports.config.tenant_id,
                          'event_type': 'im.message.receive_v1'},
               'event': {'sender': {'sender_id': {'open_id': host.ports.config.owner_id},
                                     'sender_type': 'user', 'tenant_key': host.ports.config.tenant_id},
                         'message': {**weak, 'chat_type': 'group', 'message_type': 'post',
                                     'content': weak['body']['content']}}}
    assert ingress.ingest(payload).captured
    assert stored(ingress)['attachments'] == []
    assert stored(ingress)['revision'] == good['revision']
    assert stored(ingress)['version_time'] == good['version_time']
    assert host.request.call_count == 3  # ordinary callback did not issue GET
    host.request.side_effect = TimeoutError('synthetic timeout')
    with pytest.raises(TimeoutError):
        ingress.refresh_message('om_native_known', source=host.source)
    state = ingress.snapshot_refresh_status('om_native_known', source=host.source)
    assert state['state'] == 'failed' and state['accepted_revision'] == good['revision']
    assert stored(ingress)['attachments'] == []
    respond(host, item(host, 4000, 'recovered'))
    ingress.refresh_message('om_native_known', source=host.source)
    state = ingress.snapshot_refresh_status('om_native_known', source=host.source)
    assert state['state'] == 'saved' and state['detail'] == ''
    assert state['accepted_revision'] != good['revision']
    assert [a['resource_key'] for a in stored(ingress)['attachments']] == ['recovered']
    assert ingress.gateway.store.rows('SELECT * FROM h1_sync') == []
    if not authorized:
        assert ingress.gateway.store.rows('SELECT * FROM h1_tasks') == []
    for call in host.request.call_args_list:
        req = call.args[0]
        assert req.http_method == lark.HttpMethod.GET
        assert req.uri == '/open-apis/im/v1/messages/om_native_known'
        assert req.token_types == {lark.AccessTokenType.TENANT}
    assert scheduler.thread is None


@pytest.mark.parametrize('changes', [
    {'native_id': 'oc_unapproved'}, {'channel': 'personal_dm'},
    {'identity': 'user'}, {'tenant_id': 'other'},
    {'container_type': 'thread', 'native_id': 'omt_unknown', 'chat_id': 'oc_native_group'},
])
def test_bound_source_rejection_precedes_sdk_io(host, changes):
    ingress, _ = install(bind(host), activate=False)
    with pytest.raises(ContractError):
        ingress.refresh_message('om_native_known', source=replace(host.source, **changes))
    host.request.assert_not_called()


def test_bound_actual_host_allowlist_rejects_returned_foreign_actor(host):
    ingress, _ = install(bind(host), activate=False)
    raw = item(host, 1000, 'file')
    raw['sender']['id'] = 'ou_not_allowed'
    respond(host, raw)
    assert not ingress.refresh_message('om_native_known', source=host.source).captured
    assert ingress.snapshot_refresh_status('om_native_known', source=host.source)['state'] == 'scope_rejected'
    assert ingress.gateway.store.rows('SELECT * FROM gw_items') == []
    host.policy.assert_not_called()


def test_reuses_registered_reader_without_expanding_chats_or_threads(host):
    reader = ScopedReadTransport(host.client, ['oc_native_dm', 'oc_native_group'],
                                  app_id='cli_synthetic', bot_id='ou_native_bot',
                                  tenant_id='synthetic-tenant')
    # Thread registration goes through the existing authenticated list adapter.
    respond(host, item(host, 1000, 'thread_file', thread_id='omt_registered'))
    reader('/open-apis/im/v1/messages', {'container_id_type': 'chat', 'container_id': 'oc_native_group'}, 'am_bot')
    assert reader.threads == {'omt_registered'}
    ports = bind(host, existing_reader=reader)
    assert ports.snapshot_reader is reader
    ingress, _ = install(ports, activate=False)
    source = replace(host.source, native_id='omt_registered', container_type='thread', chat_id='oc_native_group')
    assert ingress.refresh_message('om_native_known', source=source).captured
    reader.chats = reader.chats | {'oc_outside'}
    with pytest.raises(BindingError, match='scope_mismatch'):
        bind(host, existing_reader=reader)
