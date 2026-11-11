"""Independent regressions for source revisions, narrowing authority and ownership.

All records, principals and send callbacks here are synthetic. No adapter or
external service is invoked.
"""
from dataclasses import replace
import json

import pytest

from am_gateway.models import ContractError, Event
from am_gateway.phase1 import Policy
from integration.am_multi_ai_host import normalize_message
from integration.phase1_runtime import Phase1Ports, install


def source_and_policy(h, *, body='概括附件的主要结论', attachments=()):
    event = Event('tenant', 'authority-source', 'owner-dm', 'owner', body,
                  h.store.clock() - 1, channel='bot_dm', project_ids=('A',),
                  attachments=attachments, revision='r1', version_time=h.store.clock() - 1)
    policy = Policy('owner', 'existing-host-authority', 'owner-dm', ('owner-dm',),
                    ('A',), allow_reply=True, allow_supplement=True)
    return event, policy


def current_source(h, event):
    row = h.store.one('SELECT version FROM gw_items WHERE item_id=?', (event.item_id,))
    return h.store.get_event({'item_id': event.item_id, 'version': row['version']})


def stored_policy(h, task):
    return Policy.parse(h.task(task['task_id'])['policy_json'])


def replay_old_policy(h, event, policy):
    # Rejecting and ignoring an older policy are both acceptable. Neither may
    # silently replace the current narrowed policy.
    try:
        h.ingest(event, policy)
    except ContractError:
        pass


@pytest.mark.parametrize('replacement', [({'id': 'new-file', 'name': 'new.txt'},), ()])
def test_native_revision_replaces_or_removes_attachments_without_union(rig, replacement):
    h, clock = rig
    old_attachments = ({'id': 'old-file', 'name': 'old.txt'},)
    event, policy = source_and_policy(h, attachments=old_attachments)
    h.ingest(event, policy)
    original_task = h.route_one()
    original_version = original_task['source_version']

    clock[0] += 1
    revised = replace(event, revision='r2', version_time=clock[0], attachments=replacement)
    h.ingest(revised, policy)
    updated_task = h.route_one()

    assert updated_task['task_id'] == original_task['task_id']
    assert current_source(h, event).revision == 'r2'
    assert current_source(h, event).attachments == replacement
    original = h.store.get_event({'item_id': event.item_id, 'version': original_version})
    assert original.revision == 'r1'
    assert original.attachments == old_attachments
    assert tuple(json.loads(updated_task['request_json'])['attachments']) == replacement
    envelope = h.claim()
    assert envelope is not None
    assert tuple(envelope['payload']['attachment_refs']) == replacement


def test_weak_poll_omission_preserves_latest_native_attachment_revision(rig):
    h, clock = rig
    event, policy = source_and_policy(h, attachments=({'id': 'old-file'},))
    h.ingest(event, policy)
    h.route_one()
    clock[0] += 1
    revised = replace(event, revision='r2', version_time=clock[0], attachments=({'id': 'new-file'},))
    h.ingest(revised, policy)
    h.route_one()

    # This later polling observation has no native revision and omits attachment
    # metadata. Omission is not an explicit edit/removal of the source message.
    clock[0] += 1
    weak = replace(revised, revision='', attachments=(), observed_via='poll')
    h.ingest(weak)
    h.route_one()
    effective = current_source(h, event)
    assert effective.revision == 'r2'
    assert effective.attachments == ({'id': 'new-file'},)
    assert h.store.get_event({'item_id': event.item_id, 'version': 1}).attachments == ({'id': 'old-file'},)


@pytest.mark.parametrize('already_claimed', [False, True])
def test_same_message_narrowed_policy_is_immediate_and_stale_policy_cannot_restore_reply(rig, already_claimed):
    h, _ = rig
    event, original_policy = source_and_policy(h)
    h.ingest(event, original_policy)
    task = h.route_one()
    envelope = h.claim() if already_claimed else None
    narrowed = replace(original_policy, version=2, allow_reply=False)

    h.ingest(event, narrowed)
    # The tool boundary must see the narrowing before any route worker runs.
    assert stored_policy(h, task).version == 2
    assert not stored_policy(h, task).allow_reply

    replay_old_policy(h, event, original_policy)
    h.route_one()
    effective = stored_policy(h, task)
    assert effective.version == 2
    assert not effective.allow_reply
    if envelope is None:
        envelope = h.claim()
    if envelope is not None:
        sent = []
        with pytest.raises(ContractError):
            h.tool_action(task['task_id'], envelope['execution']['generation'], 'reply',
                          target='owner-dm', body='must remain unsent', sender=lambda **kw: sent.append(kw))
        assert sent == []


def test_resume_preserves_narrowed_policy_and_cannot_recover_stale_permissions(rig):
    h, _ = rig

    class NeedsClarification:
        def classify(self, event, budget):
            return {'decision': 'needs_clarification', 'intent': 'unknown',
                    'missing_fields': ['action'], 'secondary_intents': [],
                    'clarification_question': '需要什么处理？', 'reason_code': 'synthetic_clarification'}

    h.classifier = NeedsClarification()
    event, original_policy = source_and_policy(h, body='帮我处理附件')
    h.ingest(event, original_policy)
    task = h.route_one()
    assert task['route_state'] == 'needs_clarification'
    narrowed = replace(original_policy, version=2, allow_reply=False, allow_supplement=False, projects=())
    h.ingest(event, narrowed)

    h.resume(task['task_id'], 'owner', intent='project.query', supplement='只查询现有资料')
    effective = stored_policy(h, task)
    assert effective.version == 2
    assert not effective.allow_reply and not effective.allow_supplement
    assert effective.projects == ()
    replay_old_policy(h, event, original_policy)
    h.route_one()
    assert stored_policy(h, task) == effective


@pytest.mark.parametrize('revoke_via', ['ingest', 'update_policy'])
def test_revocation_survives_old_policy_poll_and_manual_resume(rig, revoke_via):
    h, _ = rig
    event, original_policy = source_and_policy(h)
    h.ingest(event, original_policy)
    task = h.route_one()
    revoked = replace(original_policy, version=2, revoked=True)
    if revoke_via == 'ingest':
        h.ingest(event, revoked)
    else:
        h.update_policy(task['task_id'], revoked)
    assert stored_policy(h, task).revoked

    # A collection-only observation and a delayed older authorization must not
    # resurrect the authorization cached in an old inbox row.
    h.ingest(replace(event, observed_via='poll'))
    replay_old_policy(h, event, original_policy)
    h.route_one()
    try:
        h.resume(task['task_id'], 'owner', intent='project.query')
    except ContractError:
        pass
    effective = stored_policy(h, task)
    assert effective.revoked and effective.version == 2
    assert h.claim() is None


def test_policy_update_cannot_undo_revocation_under_a_new_version(rig):
    h, _ = rig
    event, policy = source_and_policy(h)
    h.ingest(event, policy)
    task = h.route_one()
    h.update_policy(task['task_id'], replace(policy, version=2, revoked=True))
    with pytest.raises(ContractError):
        h.update_policy(task['task_id'], replace(policy, version=3, revoked=False))
    assert stored_policy(h, task).revoked


@pytest.mark.parametrize('routed_before_takeover', [False, True])
def test_ingress_legacy_takeover_is_persistent_and_not_reclaimed_by_late_poll(rig, routed_before_takeover):
    h, _ = rig
    legacy = [False]
    ports = Phase1Ports(
        h.config, h.store.path, lambda payload: True,
        lambda event: Policy(event.sender_id, 'verified-host', event.chat_id, (event.chat_id,),
                             ('A',), allow_reply=True),
        lambda payload: ('A',), lambda payload: legacy[0],
    )
    ingress, scheduler = install(ports)
    assert scheduler.thread is None
    gateway = ingress.gateway
    payload = {'header': {'tenant_key': 'tenant'}, 'event': {
        'sender': {'sender_id': {'open_id': 'owner'}},
        'message': {'message_id': 'legacy-takeover', 'chat_id': 'owner-dm', 'chat_type': 'p2p',
                    'create_time': '999', 'message_type': 'text',
                    'content': json.dumps({'text': 'A 项目目前卡在哪里？'})},
    }}
    first = ingress.ingest(payload)
    assert first.captured and first.claimed
    if routed_before_takeover:
        assert gateway.route_one()['route_state'] == 'resolved'

    legacy[0] = True
    takeover = ingress.ingest(payload)
    assert takeover.captured and not takeover.claimed and not takeover.dialogue
    # Ownership must take effect before the pending route worker gets a turn.
    assert gateway.claim() is None
    gateway.route_one()
    assert gateway.claim() is None

    legacy[0] = False
    ordinary_duplicate = ingress.ingest(payload)
    assert ordinary_duplicate.captured and not ordinary_duplicate.claimed
    weak_poll = normalize_message(payload, h.config, observed_via='poll', channel='bot_dm', project_ids=('A',))
    admission = gateway.ingest(weak_poll, ports.authorization_for(weak_poll))
    assert not admission.handled
    gateway.route_one()
    assert gateway.claim() is None
