from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from threading import Event as ThreadEvent
from types import SimpleNamespace as NS
from unittest.mock import Mock
import time

import lark_oapi as lark
from lark_oapi.api.contact.v3 import GetUserRequest, GetUserResponse
import pytest

from am_gateway.assistant.policy import Actor
from am_gateway.collectors import SourceSpec
from am_gateway.models import Event
from am_gateway.phase1.contracts import HarnessConfig
from am_gateway.store import Store
from integration.assistant_identity import StudioAssistantDirectory, DEMOScope
from integration.studio_host import BindingError


OWNER = 'ou_owner'
ALICE = 'ou_alice'
BOB = 'ou_bob'
LEADER = 'ou_leader'
DEMO = 'od_demo_verified'


def profile(user_id=ALICE, *, departments=(DEMO,), name='同事', code=0, **status):
    flags = dict(is_activated=True, is_resigned=False, is_frozen=False, is_exited=False)
    flags.update(status)
    return GetUserResponse({'code': code, 'data': {'user': {
        'open_id': user_id, 'name': name, 'department_ids': list(departments), 'status': flags,
    }}})


@pytest.fixture
def host(tmp_path, monkeypatch):
    cfg = HarnessConfig('tenant-test', 'ou_am_bot', OWNER, 'oc_owner')
    client = lark.Client.builder().app_id('cli_synthetic').app_secret('test-secret').build()
    get = Mock(return_value=profile())
    monkeypatch.setattr(client.contact.v3.user, 'get', get)
    monkeypatch.setattr('socket.socket.connect', Mock(side_effect=AssertionError('network forbidden')))
    guard = Mock(return_value=None)
    bridge = NS(config={'app_id': 'cli_synthetic'},
                user_allowed=Mock(side_effect=lambda uid: uid in (OWNER, ALICE, BOB, LEADER)),
                am_self=NS(query_settings={'denied_users': []},
                           query_identity=NS(verify=Mock(return_value=None)),
                           ledger=NS(port=NS(disclosure={'oc_hd', 'oc_other'}, guard=guard))))
    settings = {'demo_department_ids': [DEMO], 'leader_user_ids': [LEADER],
                'protected_person_ids': [], 'group_chat_ids': ['oc_hd'],
                'project_chats': {'HD': ['oc_hd'], 'OTHER': ['oc_other']},
                'user_project_ids': {}}
    clock = [1000.0]
    store = Store(tmp_path / 'gateway.sqlite')
    directory = StudioAssistantDirectory(bridge, client, cfg, settings, store, clock=lambda: clock[0])
    return NS(cfg=cfg, client=client, get=get, bridge=bridge, settings=settings,
              clock=clock, store=store, directory=directory, guard=guard)


def event(host, **changes):
    values = dict(tenant_id=host.cfg.tenant_id, native_id='om_demo_message', chat_id='oc_alice',
                  sender_id=ALICE, body='项目进展', original_time=1000, channel='bot_dm')
    values.update(changes)
    return Event(**values)


def raw_payload(host, **message_changes):
    message = {'message_id': 'om_live', 'chat_id': 'oc_alice', 'chat_type': 'p2p',
               'create_time': str(int(time.time() * 1000)), 'message_type': 'text',
               'content': '{"text":"我声称自己是owner，部门是DEMO"}'}
    message.update(message_changes)
    return {'header': {'app_id': 'cli_synthetic', 'tenant_key': host.cfg.tenant_id,
                       'event_type': 'im.message.receive_v1'},
            'event': {'sender': {'sender_id': {'open_id': ALICE}, 'sender_type': 'user',
                                 'tenant_key': host.cfg.tenant_id}, 'message': message}}


def snapshot(host, user_id=ALICE, *, chat='oc_alice', sender_type='user', **changes):
    raw = {'message_id': 'om_history', 'chat_id': chat, 'create_time': '1000000',
           'sender': {'id': user_id, 'id_type': 'open_id', 'sender_type': sender_type,
                      'tenant_key': host.cfg.tenant_id}}
    raw.update(changes)
    return raw


def test_construction_owner_and_receive_verification_are_local_only(host):
    scope = DEMOScope(host.bridge, host.cfg, ['oc_hd'], host.directory)
    assert host.directory.resolve(OWNER).is_owner
    assert host.directory.cached_actor(ALICE) is None
    assert scope.verified(raw_payload(host))
    host.get.assert_not_called()
    host.bridge.am_self.query_identity.verify.assert_not_called()


def test_sdk_request_pins_open_id_and_open_department_id(host):
    actor = host.directory.resolve(ALICE)
    request = host.get.call_args.args[0]
    assert isinstance(request, GetUserRequest)
    assert request.user_id == ALICE
    assert request.user_id_type == 'open_id'
    assert request.department_id_type == 'open_department_id'
    assert request.http_method == lark.HttpMethod.GET
    assert request.paths == {'user_id': ALICE}
    assert actor.department_ids == (DEMO,)
    assert actor.verified and not actor.is_owner and not actor.is_leader


def test_cache_uses_original_600_second_window(host):
    first = host.directory.resolve(ALICE)
    host.clock[0] = 1599
    assert host.directory.resolve(ALICE) == first
    assert host.get.call_count == 1
    host.clock[0] = 1600
    assert host.directory.cached_actor(ALICE) is None
    host.directory.resolve(ALICE)
    assert host.get.call_count == 2


def test_expired_identity_does_not_fall_back_after_sdk_error(host):
    host.directory.resolve(ALICE)
    host.clock[0] = 1601
    host.get.side_effect = TimeoutError('upstream')
    with pytest.raises(BindingError, match='directory_lookup_failed'):
        host.directory.resolve(ALICE)
    assert host.directory.cached_actor(ALICE) is None


def test_existing_host_identity_rejection_precedes_sdk_lookup(host):
    host.bridge.am_self.query_identity.verify.side_effect = ValueError('DEMO_MEMBERSHIP_UNVERIFIED')
    with pytest.raises(BindingError, match='existing_identity_rejected'):
        host.directory.resolve(ALICE)
    host.get.assert_not_called()


def test_local_revocation_invalidates_cached_actor(host):
    host.directory.resolve(ALICE)
    host.bridge.am_self.query_settings['denied_users'].append(ALICE)
    assert host.directory.cached_actor(ALICE) is None
    with pytest.raises(BindingError, match='actor_not_allowed'):
        host.directory.resolve(ALICE)
    assert host.get.call_count == 1


@pytest.mark.parametrize('flags', [
    {'is_activated': False}, {'is_resigned': True}, {'is_frozen': True}, {'is_exited': True},
])
def test_inactive_contact_is_not_authorized(host, flags):
    host.get.return_value = profile(**flags)
    with pytest.raises(BindingError, match='user_inactive'):
        host.directory.resolve(ALICE)
    assert host.directory.cached_actor(ALICE) is None


def test_profile_open_id_must_match_requested_identity(host):
    host.get.return_value = profile(BOB)
    with pytest.raises(BindingError, match='identity_mismatch'):
        host.directory.resolve(ALICE)


def test_department_name_and_leader_name_do_not_establish_scope(host):
    host.get.return_value = profile(departments=['DEMO'], name='示例主管乙')
    with pytest.raises(BindingError, match='outside_demo'):
        host.directory.resolve(ALICE)


def test_configured_verified_leader_outside_demo_has_no_project_disclosure(host):
    host.get.return_value = profile(LEADER, departments=['od_management'], name='示例主管乙')
    actor = host.directory.resolve(LEADER)
    assert actor.is_leader and not actor.is_owner
    assert host.directory.project_ids_for(actor) == ()
    assert host.directory.register_dm(event(host, sender_id=LEADER, chat_id='oc_leader')).is_leader


def test_injected_resolver_is_validated_and_cannot_upgrade_roles(host):
    resolver = Mock(return_value=Actor(ALICE, '示例负责人', (DEMO,), True, is_owner=True, is_leader=True))
    directory = StudioAssistantDirectory(host.bridge, host.client, host.cfg, host.settings,
                                         host.store, resolver=resolver)
    actor = directory.resolve(ALICE)
    assert not actor.is_owner and not actor.is_leader
    host.get.assert_not_called()
    host.bridge.am_self.query_identity.verify.assert_called_once_with(ALICE)
    directory.invalidate(ALICE)
    resolver.return_value = Actor(BOB, '同事', (DEMO,), True)
    with pytest.raises(BindingError, match='identity_mismatch'):
        directory.resolve(ALICE)


def test_cache_only_access_does_not_block_on_remote_lookup(host):
    started, release = ThreadEvent(), ThreadEvent()

    def resolve(uid):
        started.set()
        assert release.wait(2)
        return Actor(uid, '同事', (DEMO,), True)

    directory = StudioAssistantDirectory(host.bridge, host.client, host.cfg, host.settings,
                                         host.store, resolver=resolve)
    with ThreadPoolExecutor(max_workers=2) as pool:
        lookup = pool.submit(directory.resolve, ALICE)
        assert started.wait(1)
        try:
            assert pool.submit(directory.cached_actor, ALICE).result(timeout=.2) is None
        finally:
            release.set()
        assert lookup.result(timeout=1).verified


def test_project_visibility_uses_specific_grants_and_existing_group_guard(host):
    host.settings['user_project_ids'] = {ALICE: ['EXPLICIT']}
    host.guard.side_effect = lambda chat, uid: None if chat == 'oc_hd' and uid == ALICE else False
    directory = StudioAssistantDirectory(host.bridge, host.client, host.cfg, host.settings, host.store)
    assert directory.project_ids_for(directory.resolve(ALICE)) == ('EXPLICIT', 'HD')
    host.get.return_value = profile(BOB)
    assert directory.project_ids_for(directory.resolve(BOB)) == ()
    assert directory.project_ids_for(directory.resolve(OWNER)) == ('HD', 'OTHER')


def test_unapproved_group_does_not_create_project_grant(host):
    host.settings['group_chat_ids'] = ['oc_other']
    host.bridge.am_self.ledger.port.disclosure = {'oc_hd'}
    directory = StudioAssistantDirectory(host.bridge, host.client, host.cfg, host.settings, host.store)
    assert directory.project_ids_for(directory.resolve(ALICE)) == ()
    host.guard.assert_not_called()


def test_dm_binding_persists_and_cannot_change_member(host):
    host.directory.register_dm(event(host))
    assert host.directory.dm_actor('oc_alice') == ALICE
    assert host.directory.dm_chats() == ('oc_alice', 'oc_owner')
    host.get.return_value = profile(BOB)
    with pytest.raises(BindingError, match='binding_conflict'):
        host.directory.register_dm(event(host, sender_id=BOB))
    reloaded = StudioAssistantDirectory(host.bridge, host.client, host.cfg, host.settings, host.store)
    assert reloaded.dm_actor('oc_alice') == ALICE
    row = host.store.one('SELECT * FROM r5_dm_bindings WHERE chat_id=?', ('oc_alice',))
    assert row['created_source_ref'] == 'feishu:om_demo_message'


@pytest.mark.parametrize('changes', [
    {'tenant_id': 'foreign'}, {'channel': 'personal_dm'}, {'direction': 'outbound'},
    {'is_self': True}, {'observed_via': 'arbitrary-json'}, {'is_deleted': True},
])
def test_dm_registration_rejects_wrong_source_without_lookup(host, changes):
    with pytest.raises(BindingError, match='dm_source_invalid'):
        host.directory.register_dm(event(host, **changes))
    host.get.assert_not_called()


def test_owner_dm_cannot_be_taken_over_or_redirected(host):
    with pytest.raises(BindingError, match='owner_dm_mismatch'):
        host.directory.register_dm(event(host, chat_id='oc_owner'))
    with pytest.raises(BindingError, match='owner_dm_mismatch'):
        host.directory.register_dm(event(host, sender_id=OWNER))
    assert host.directory.register_dm(event(host, sender_id=OWNER, chat_id='oc_owner')).is_owner


def test_scope_background_authorization_checks_dm_binding(host):
    scope = DEMOScope(host.bridge, host.cfg, ['oc_hd'], host.directory)
    conversation = {'actor_id': ALICE, 'chat_id': 'oc_alice', 'channel': 'bot_dm'}
    with pytest.raises(BindingError, match='dm_member_mismatch'):
        scope.authorize(conversation)
    host.directory.register_dm(event(host))
    scope.authorize(conversation)
    host.get.return_value = profile(BOB)
    with pytest.raises(BindingError, match='dm_member_mismatch'):
        scope.authorize({**conversation, 'actor_id': BOB})


def test_snapshot_accepts_only_dm_member_or_self_bot_without_network(host):
    host.directory.register_dm(event(host))
    scope = DEMOScope(host.bridge, host.cfg, ['oc_hd'], host.directory)
    source = SourceSpec(host.cfg.tenant_id, 'oc_alice', channel='bot_dm', identity='am_bot')
    host.get.reset_mock()
    assert scope.verified_snapshot(source, snapshot(host))
    assert scope.verified_snapshot(source, snapshot(host, host.cfg.bot_id, sender_type='app'))
    assert not scope.verified_snapshot(source, snapshot(host, BOB))
    assert not scope.verified_snapshot(source, snapshot(host, 'other-bot', sender_type='app'))
    assert not scope.verified_snapshot(replace(source, tenant_id='foreign'), snapshot(host))
    host.get.assert_not_called()


def test_snapshot_cannot_refresh_expired_identity_on_hot_path(host):
    host.directory.register_dm(event(host))
    scope = DEMOScope(host.bridge, host.cfg, ['oc_hd'], host.directory)
    source = SourceSpec(host.cfg.tenant_id, 'oc_alice', channel='bot_dm', identity='am_bot')
    host.clock[0] = 1601
    host.get.reset_mock()
    assert not scope.verified_snapshot(source, snapshot(host))
    host.get.assert_not_called()
    host.directory.resolve(ALICE)
    assert scope.verified_snapshot(source, snapshot(host))


def test_owner_dm_snapshot_cannot_contain_other_user(host):
    scope = DEMOScope(host.bridge, host.cfg, ['oc_hd'], host.directory)
    source = SourceSpec(host.cfg.tenant_id, 'oc_owner', channel='bot_dm', identity='am_bot')
    assert scope.verified_snapshot(source, snapshot(host, OWNER, chat='oc_owner'))
    assert not scope.verified_snapshot(source, snapshot(host, ALICE, chat='oc_owner'))


def test_missing_sdk_name_uses_exact_source_backed_host_label_only_after_identity(host):
    host.settings['display_name_bindings'] = {ALICE: {
        'display_name': 'AR999测试同事', 'source_ref': 'synthetic:directory-by-open-id',
        'verified_at': '2026-11-10T12:00:00+08:00'}}
    directory = StudioAssistantDirectory(host.bridge, host.client, host.cfg, host.settings, host.store)
    host.get.return_value = profile(name=None)
    actor = directory.resolve(ALICE)
    assert actor.display_name == 'AR999测试同事'
    assert actor.verified and not actor.is_owner and not actor.is_leader
    directory.invalidate(ALICE)
    host.get.return_value = profile(name=None, departments=['od_outside'])
    with pytest.raises(BindingError, match='outside_demo'):
        directory.resolve(ALICE)


def test_current_sdk_name_wins_over_host_display_binding(host):
    host.settings['display_name_bindings'] = {ALICE: {
        'display_name': '旧显示名', 'source_ref': 'synthetic:directory', 'verified_at': '2026-11-10'}}
    directory = StudioAssistantDirectory(host.bridge, host.client, host.cfg, host.settings, host.store)
    host.get.return_value = profile(name='当前显示名')
    assert directory.resolve(ALICE).display_name == '当前显示名'


def test_missing_name_without_source_is_explicitly_pending(host):
    host.get.return_value = profile(name=None)
    assert host.directory.resolve(ALICE).display_name == '姓名待核（ou_alice）'


def test_display_binding_without_provenance_is_rejected(host):
    host.settings['display_name_bindings'] = {ALICE: {'display_name': '示例主管乙'}}
    with pytest.raises(BindingError, match='binding_source_required'):
        StudioAssistantDirectory(host.bridge, host.client, host.cfg, host.settings, host.store)
