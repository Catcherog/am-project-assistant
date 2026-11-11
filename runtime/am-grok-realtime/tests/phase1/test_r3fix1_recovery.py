"""Regression cases for settled capacity and multi-channel source enrichment."""
from dataclasses import replace
import json

from am_gateway.models import Event
from am_gateway.phase1 import Harness, Policy
from conftest import admit, receipt


def finish(h, task):
    envelope = h.claim()
    assert envelope['identity']['task_id'] == task['task_id']
    receipt(h, envelope, 'accepted', {'run_id': task['task_id'], 'durable_record': 'accepted'})
    receipt(h, envelope, 'result', {'run_id': task['task_id'], 'response_kind': 'sufficient',
            'remaining_requirements': [], 'artifacts': [], 'reply_text': '已核对的回答'})
    effect = h.tool_action(task['task_id'], envelope['execution']['generation'], 'reply',
                          target='owner-dm', body='已核对的回答',
                          sender=lambda **kw: {'platform_id': task['task_id'] + '-reply'})
    h.verify_delivery(task['task_id'], {'platform_id': effect['platform_id'], 'target': 'owner-dm',
                      'body': '已核对的回答', 'sender_id': 'bot', 'platform_time': 1001})
    receipt(h, envelope, 'ended', {'status': 'succeeded', 'run_id': task['task_id'],
                                  'stopped_evidence': 'fixture exit'})


def test_completed_tasks_do_not_consume_lifetime_queue_capacity(rig):
    h, _ = rig
    h = Harness(h.store, replace(h.config, queue_limit=2))
    for n in range(7):
        task = admit(h, 'finished-' + str(n))
        assert task['route_state'] == 'resolved'
        finish(h, task)
    assert len(h.store.rows("SELECT * FROM h1_tasks WHERE dispatch_state='accepted'")) == 7
    assert admit(h, 'next')['route_state'] == 'resolved'


def test_terminal_execution_with_unknown_send_still_uses_capacity(rig):
    h, _ = rig
    h = Harness(h.store, replace(h.config, queue_limit=1))
    first = admit(h, 'first')
    envelope = h.claim()
    receipt(h, envelope, 'accepted', {'run_id': 'r', 'durable_record': 'accepted'})
    h.tool_action(first['task_id'], envelope['execution']['generation'], 'reply',
                  target='owner-dm', body='结果不明', sender=lambda **kw: {})
    receipt(h, envelope, 'ended', {'status': 'succeeded', 'run_id': 'r', 'stopped_evidence': 'exit'})
    assert admit(h, 'second')['route_state'] == 'queue_full'
    assert h.store.one('SELECT state FROM h1_sessions')['state'] == 'held'


def test_queued_source_revision_does_not_count_itself_as_another_slot(rig):
    h, _ = rig
    h = Harness(h.store, replace(h.config, queue_limit=1))
    task = admit(h)
    event = Event.from_dict(json.loads(task['request_json']))
    h.ingest(replace(event, body='查询 A 项目的风险', version_time=1001),
             Policy.parse(task['policy_json']))
    revised = h.route_one()
    assert revised['task_id'] == task['task_id']
    assert revised['route_state'] == 'resolved'
    assert h.claim()['identity']['task_id'] == task['task_id']


def group_event():
    return Event('tenant', 'enriched', 'group', 'owner', 'A 项目目前卡在哪里？', 999,
                 channel='group', observed_via='poll')


def group_policy():
    return Policy('owner', 'trusted-host', 'group', ('group',), ('A',), allow_reply=True)


def test_poll_then_mention_enrichment_creates_exactly_one_task(rig):
    h, _ = rig
    event, policy = group_event(), group_policy()
    h.ingest(event, policy)
    assert h.route_one()['disposition'] == 'collection_only'
    enriched = replace(event, mentions=('bot',), project_ids=('A',), observed_via='event')
    admission = h.ingest(enriched, policy)
    assert admission.handled and admission.changed
    task = h.route_one()
    assert task['route_state'] == 'resolved'
    assert h.store.get_event({'item_id': event.item_id, 'version': 1}).mentions == ()
    assert h.store.get_event({'item_id': event.item_id, 'version': 2}).mentions == ('bot',)
    assert h.claim()['identity']['task_id'] == task['task_id']
    for duplicate in (event, enriched, replace(enriched, observed_via='poll')):
        h.ingest(duplicate, policy)
    assert h.route_one() is None
    assert len(h.store.rows('SELECT * FROM h1_tasks')) == 1


def test_project_enrichment_reprojects_previously_unassociated_source(rig):
    h, _ = rig
    event, policy = group_event(), group_policy()
    h.ingest(event, policy)
    h.route_one()
    assert h.information.project_sources() == 0
    h.ingest(replace(event, project_ids=('A',)), policy)
    h.route_one()
    assert h.information.project_sources() == 1
    assert h.information.project_sources() == 0
    snapshot = h.information.snapshot(policy)
    assert len(snapshot['evidence_items']) == 1
    assert snapshot['evidence_items'][0]['project'] == 'A'
    assert snapshot['evidence_items'][0]['sources'][0]['version'] == 2


def test_weak_poll_cannot_erase_known_metadata_or_native_revision(rig):
    h, _ = rig
    event, policy = group_event(), group_policy()
    strong = replace(event, mentions=('bot',), project_ids=('A',), revision='source-v1')
    h.ingest(strong, policy)
    task = h.route_one()
    h.ingest(event, policy)
    assert h.route_one() is None
    stored = h.store.get_event({'item_id': event.item_id, 'version': 1})
    assert stored.mentions == ('bot',) and stored.revision == 'source-v1'
    assert stored.project_ids == ('A',)
    assert h.task(task['task_id'])['source_version'] == 1


def test_post_dispatch_enrichment_never_creates_another_operation(rig):
    h, _ = rig
    task = admit(h)
    envelope = h.claim()
    source = Event.from_dict(json.loads(task['request_json']))
    h.ingest(replace(source, attachments=({'id': 'late-attachment'},)),
             Policy.parse(task['policy_json']))
    revised = h.route_one()
    assert revised['business_id'] == envelope['identity']['business_operation_id']
    assert revised['last_error'] == 'source_changed'
    assert len(h.store.rows('SELECT * FROM h1_tasks')) == 1
    assert h.claim() is None


def test_authorized_metadata_after_collection_only_recovers_task(rig):
    h, _ = rig
    event = replace(group_event(), mentions=('bot',))
    h.ingest(event)
    assert h.route_one()['disposition'] == 'collection_only'
    h.ingest(event, group_policy())
    assert h.route_one()['route_state'] == 'resolved'
    # A later collection-only duplicate must not revoke the trusted host binding.
    h.ingest(event)
    assert h.route_one() is None
    assert h.claim() is not None


def test_ack_unknown_running_execution_is_still_observed_for_timeout(rig):
    h, clock = rig
    task = admit(h)
    envelope = h.claim()
    receipt(h, envelope, 'running', {'run_id': 'r'})
    with h.store.transaction() as db:
        db.execute("UPDATE h1_tasks SET dispatch_state='unknown' WHERE task_id=?", (task['task_id'],))
    clock[0] += 31
    h.expire()
    assert h.task(task['task_id'])['execution_state'] == 'unknown'
    assert h.store.one('SELECT state FROM h1_sessions')['state'] == 'held'


def test_stale_body_with_new_self_marker_keeps_effective_evidence_hash(rig):
    h, _ = rig
    event = replace(group_event(), body='最新正文', version_time=1000)
    h.ingest(event, group_policy())
    h.ingest(replace(event, body='过期正文', version_time=900, is_self=True))
    source = h.store.one('SELECT * FROM gw_items WHERE item_id=?', (event.item_id,))
    stored = h.store.get_event({'item_id': event.item_id, 'version': source['version']})
    evidence = h.store.one('SELECT content_hash FROM gw_item_versions WHERE item_id=? AND version=?',
                           (event.item_id, source['version']))
    assert stored.body == '最新正文' and stored.is_self
    assert evidence['content_hash'] == stored.content_hash == source['content_hash']
