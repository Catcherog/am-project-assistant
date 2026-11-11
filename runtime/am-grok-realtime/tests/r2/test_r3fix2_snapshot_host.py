"""Raw snapshot bindings use the existing SDK client without event forgery."""
from dataclasses import replace
import json
from types import SimpleNamespace as NS
from unittest.mock import Mock

import pytest

from am_gateway.collectors import SourceSpec
from am_gateway.feishu_snapshot import SnapshotIncomplete, normalize_feishu_snapshot
from am_gateway.phase1 import Harness, HarnessConfig, Policy
from am_gateway.store import Store
from integration.studio_host import BindingError, HostScope, ScopedReadTransport
from integration.phase1_runtime import Phase1Ingress, Phase1Ports


def config():
    return HarnessConfig('tenant', 'ou_bot', 'ou_owner', 'oc_owner')


def bridge():
    return NS(config={'app_id': 'cli_app'}, user_allowed=lambda actor: actor == 'ou_owner')


def source(**changes):
    return replace(SourceSpec('tenant', 'oc_group', identity='am_bot'), **changes)


def message(**changes):
    item = {'message_id': 'om_known', 'chat_id': 'oc_group',
            'create_time': '1000000', 'update_time': '1001000',
            'deleted': False, 'msg_type': 'text', 'body': {'content': '{"text":"当前版本"}'},
            'sender': {'id': 'ou_owner', 'id_type': 'open_id', 'sender_type': 'user',
                       'tenant_key': 'tenant'}}
    item.update(changes)
    return item


def reader(raw=None):
    raw = raw if raw is not None else {'code': 0, 'data': {'items': [message()]}}
    client = NS(request=Mock(return_value=NS(raw=NS(content=json.dumps(raw).encode()))))
    read = ScopedReadTransport(client, ['oc_group', 'oc_owner'], app_id='cli_app',
                               bot_id='ou_bot', tenant_id='tenant')
    return read, client


def test_explicit_get_uses_bound_sdk_and_raw_scope_without_receive_header():
    read, client = reader()
    item = read.read_message(source(), 'om_known')
    request = client.request.call_args.args[0]
    assert request.uri == '/open-apis/im/v1/messages/om_known'
    assert request.queries == [('user_id_type', 'open_id')]
    import lark_oapi as lark
    assert request.http_method == lark.HttpMethod.GET
    assert request.token_types == {lark.AccessTokenType.TENANT}
    scope = HostScope(bridge(), config(), ['oc_group'])
    assert 'header' not in item
    assert scope.verified_snapshot(source(), item)
    assert not scope.verified(item)
    event = normalize_feishu_snapshot(item, config(), source(), expected_message_id='om_known')
    assert event.revision.startswith('feishu-message-v1:1001000:')
    assert event.version_time == 1001


@pytest.mark.parametrize('changes', [
    {'tenant_id': 'another'}, {'identity': 'personal_account'},
    {'native_id': 'oc_unregistered'}, {'channel': 'personal_dm'},
    {'adapter': 'unbound'}, {'chat_id': 'oc_owner'},
    {'container_type': 'thread', 'native_id': 'omt_unverified', 'chat_id': 'oc_group'},
])
def test_exact_read_rejects_unbound_source_before_sdk(changes):
    read, client = reader()
    with pytest.raises(BindingError, match='snapshot_source_not_allowed'):
        read.read_message(source(**changes), 'om_known')
    client.request.assert_not_called()


def test_generic_transport_does_not_gain_arbitrary_single_message_endpoint():
    read, client = reader()
    with pytest.raises(BindingError, match='read_endpoint_not_allowed'):
        read('https://open.feishu.cn/open-apis/im/v1/messages/om_known',
             {'user_id_type': 'open_id'}, 'am_bot')
    client.request.assert_not_called()


@pytest.mark.parametrize('raw,error', [
    ({'code': 99991672, 'msg': 'permission denied'}, SnapshotIncomplete),
    ({'code': 0, 'data': {'items': [message(message_id='om_other')]}}, SnapshotIncomplete),
    ({'code': 0, 'data': {'items': [message(), message()]}}, SnapshotIncomplete),
    ({'code': 0, 'data': {'items': [message(chat_id='oc_other')]}}, BindingError),
    ({'code': 0, 'data': {'items': [message(thread_id=123)]}}, BindingError),
])
def test_exact_read_never_returns_error_or_mismatched_message_as_snapshot(raw, error):
    read, client = reader(raw)
    with pytest.raises(error):
        read.read_message(source(), 'om_known')
    assert client.request.call_count == 1
    assert read.threads == set()


def test_known_child_thread_requires_same_returned_thread_and_chat():
    spec = source(container_type='thread', native_id='omt_known', chat_id='oc_group')
    read, client = reader({'code': 0, 'data': {'items': [message(thread_id='omt_known')]}})
    read.threads.add('omt_known')
    item = read.read_message(spec, 'om_known')
    assert HostScope(bridge(), config(), ['oc_group']).verified_snapshot(spec, item)
    for change, reason in [({'thread_id': 'omt_other'}, 'snapshot_thread_mismatch'),
                           ({'chat_id': 'oc_owner', 'thread_id': 'omt_known'}, 'snapshot_chat_mismatch')]:
        client.request.return_value.raw.content = json.dumps(
            {'code': 0, 'data': {'items': [message(**change)]}}).encode()
        with pytest.raises(BindingError, match=reason):
            read.read_message(spec, 'om_known')
    assert read.threads == {'omt_known'}


def test_sender_tenant_is_checked_before_self_bot_mapping_or_thread_registration():
    sender = {'id': 'cli_app', 'id_type': 'app_id', 'sender_type': 'app', 'tenant_key': 'another'}
    read, _ = reader({'code': 0, 'data': {'items': [message(sender=sender, thread_id='omt_bad')]}})
    with pytest.raises(BindingError, match='read_sender_tenant_mismatch'):
        read.read_message(source(), 'om_known')
    assert read.normalized_self_messages == 0
    assert read.threads == set()


def test_own_app_is_normalized_and_never_creates_an_inbound_task(tmp_path):
    sender = {'id': 'cli_app', 'id_type': 'app_id', 'sender_type': 'app', 'tenant_key': 'tenant'}
    read, _ = reader({'code': 0, 'data': {'items': [message(
        sender=sender, mentions=[{'id': 'ou_bot', 'id_type': 'open_id'}])]}})
    item = read.read_message(source(), 'om_known')
    assert item['sender']['id'] == 'ou_bot' and item['sender']['id_type'] == 'open_id'
    assert read.normalized_self_messages == 1
    assert HostScope(bridge(), config(), ['oc_group']).verified_snapshot(source(), item)
    event = normalize_feishu_snapshot(item, config(), source())
    assert event.is_self and event.direction == 'outbound'
    h = Harness(Store(tmp_path / 'gateway.sqlite'), config())
    # Even an over-eager synthetic port cannot turn a known self message into
    # an inbound request. This fixture does not create a real host policy.
    policy = Policy('ou_bot', 'synthetic-only', 'oc_group', ('oc_group',), allow_reply=True)
    h.ingest(event, policy)
    h.route_one()
    assert h.store.rows('SELECT * FROM h1_tasks') == []


@pytest.mark.parametrize('mutation', [
    lambda item: item.update(chat_id='oc_unregistered'),
    lambda item: item['sender'].update(id='ou_blocked'),
    lambda item: item['sender'].update(tenant_key='another'),
    lambda item: item['sender'].update(id_type='user_id'),
    lambda item: item['sender'].update(id='cli_app', id_type='app_id', sender_type='app'),
    lambda item: item['sender'].update(id='ou_otherbot', sender_type='bot'),
    lambda item: item['sender'].update(sender_type='unknown'),
])
def test_snapshot_scope_rejects_wrong_actor_tenant_and_unnormalized_apps(mutation):
    item = message()
    mutation(item)
    assert not HostScope(bridge(), config(), ['oc_group']).verified_snapshot(source(), item)


def test_snapshot_scope_uses_pinned_identity_and_owner_dm_channel():
    scope = HostScope(bridge(), config(), ['oc_group'])
    dm = source(native_id='oc_owner', channel='bot_dm')
    item = message(chat_id='oc_owner')
    assert scope.verified_snapshot(dm, item)
    assert not scope.verified_snapshot(replace(dm, channel='personal_dm'), item)
    assert not scope.verified_snapshot(replace(dm, identity='user'), item)
    assert not scope.verified_snapshot(replace(dm, tenant_id='other'), item)
    assert not scope.verified_snapshot(replace(dm, channel='group'), item)
    assert not scope.verified_snapshot(source(channel='bot_dm'), message())
    assert not scope.verified_snapshot(source(chat_id='oc_owner'), item)
    # No credential refresh, policy creation, or send is performed by verification.
    assert not hasattr(scope.bridge, 'am_self')


def test_sdk_reader_scope_ingress_and_file_zone_use_one_complete_path(tmp_path):
    read, client = reader()
    scope = HostScope(bridge(), config(), ['oc_group'])
    h = Harness(Store(tmp_path / 'gateway.sqlite'), config())
    spec = source(project_ids=('A',))
    policy = Policy('ou_owner', 'synthetic-host', 'oc_group', ('oc_group',), ('A',), allow_reply=True)
    ports = Phase1Ports(h.config, h.store.path, scope.verified, lambda event: policy,
                        lambda payload: ('A',), lambda payload: False,
                        snapshot_reader=read, verified_snapshot_scope=scope.verified_snapshot)
    ingress = Phase1Ingress(h, ports)
    for index, file_key in enumerate(('old', 'new', None), 1):
        content = {'zh_cn': {'content': [[{'tag': 'text', 'text': 'A 项目目前卡在哪里？'}]]},
                   'files': [{'file_key': file_key}] if file_key else []}
        raw = message(msg_type='post', body={'content': json.dumps(content)},
                      update_time=str(1000000 + index * 1000),
                      mentions=[{'id': 'ou_bot', 'id_type': 'open_id'}])
        client.request.return_value.raw.content = json.dumps({'code': 0, 'data': {'items': [raw]}}).encode()
        admitted = ingress.refresh_message('om_known', source=spec)
        assert admitted.captured and admitted.claimed
        h.route_one()
        row = h.store.one('SELECT event_json,version FROM gw_items WHERE native_id=?', ('om_known',))
        assert row['version'] == index
        keys = [a['resource_key'] for a in json.loads(row['event_json'])['attachments']]
        assert keys == ([file_key] if file_key else [])
        assert ingress.snapshot_refresh_status('om_known', source=spec)['state'] == 'saved'
    assert client.request.call_count == 3
    assert h.store.rows('SELECT * FROM h1_sync') == []
