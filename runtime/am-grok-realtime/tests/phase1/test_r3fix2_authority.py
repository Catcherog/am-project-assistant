"""R3-FIX2: authoritative source content survives weaker observations.

All records here are synthetic. No Feishu, Bot, or external effect is invoked.
"""
from dataclasses import replace
import json

import pytest

from am_gateway.models import ContractError, Event
from am_gateway.phase1 import Policy


def source_and_policy(*, revision='r1', attachments=({'id': 'old'},)):
    event = Event('tenant', 'fix2-source', 'owner-dm', 'owner',
                  'A 项目目前卡在哪里？', 999, channel='bot_dm',
                  project_ids=('A',), attachments=attachments,
                  revision=revision, version_time=1000)
    policy = Policy('owner', 'verified-host', 'owner-dm', ('owner-dm',),
                    ('A',), allow_reply=True, allow_supplement=True)
    return event, policy


def current(h, event):
    row = h.store.one('SELECT * FROM gw_items WHERE item_id=?', (event.item_id,))
    return row, Event.from_dict(json.loads(row['event_json']))


@pytest.mark.parametrize('replacement', [({'id': 'new'},), ()])
@pytest.mark.parametrize('weak_time', [1001, 5000])
def test_weak_old_attachment_cannot_return_after_authoritative_change(rig, replacement, weak_time):
    h, _ = rig
    event, policy = source_and_policy()
    h.ingest(event, policy)
    h.route_one()
    revised = replace(event, revision='r2', version_time=1001, attachments=replacement)
    h.ingest(revised, policy)
    task = h.route_one()

    outcome = h.ingest(replace(event, revision='', version_time=weak_time, observed_via='poll'), policy)
    row, saved = current(h, event)
    assert not outcome.changed
    assert row['version'] == 2
    assert row['version_time'] == saved.version_time == 1001
    assert saved.revision == 'r2' and saved.attachments == replacement
    assert h.route_one() is None
    assert h.task(task['task_id'])['source_version'] == 2
    assert h.store.get_event({'item_id': event.item_id, 'version': 1}).attachments == event.attachments

    # A late weak observation's collection time must not fence out a real edit.
    latest = replace(revised, revision='r3', version_time=1002, attachments=({'id': 'latest'},))
    assert h.ingest(latest, policy).changed
    h.route_one()
    row, saved = current(h, event)
    assert row['version'] == 3 and saved.attachments == latest.attachments
    assert h.claim()['payload']['attachment_refs'] == [{'id': 'latest'}]


def test_weak_changed_body_and_mentions_cannot_mutate_authoritative_source(rig):
    h, _ = rig
    event, policy = source_and_policy(attachments=())
    h.ingest(event, policy)
    task = h.route_one()
    weak = replace(event, revision='', version_time=5000, body='过期摘要已完成',
                   mentions=('other-bot',), attachments=({'id': 'removed'},), observed_via='poll')
    assert not h.ingest(weak, policy).changed
    row, saved = current(h, event)
    assert row['version'] == 1 and row['version_time'] == 1000
    assert saved.body == event.body and saved.mentions == () and saved.attachments == ()
    assert h.route_one() is None
    assert json.loads(h.task(task['task_id'])['request_json'])['body'] == event.body


def test_same_revision_duplicate_cannot_change_content_or_advance_watermark(rig):
    h, _ = rig
    event, policy = source_and_policy()
    h.ingest(event, policy)
    h.route_one()
    duplicate = replace(event, body='不同正文', attachments=({'id': 'bad'},), version_time=9000)
    assert not h.ingest(duplicate, policy).changed
    row, saved = current(h, event)
    assert row['version'] == 1 and row['version_time'] == saved.version_time == 1000
    assert saved.body == event.body and saved.attachments == event.attachments


def test_stale_authoritative_revision_cannot_restore_old_source(rig):
    h, _ = rig
    event, policy = source_and_policy()
    latest = replace(event, revision='r2', version_time=1002, body='最新正文', attachments=())
    h.ingest(latest, policy)
    h.route_one()
    assert not h.ingest(event, policy).changed
    row, saved = current(h, event)
    assert row['version'] == 1 and saved.revision == 'r2'
    assert saved.body == '最新正文' and saved.attachments == ()


def test_verified_recall_is_not_swallowed_and_weak_observation_cannot_resurrect(rig):
    h, _ = rig
    event, policy = source_and_policy()
    h.ingest(event, policy)
    task = h.route_one()
    recalled = replace(event, revision='', is_deleted=True, version_time=1001, observed_via='recall')
    assert h.ingest(recalled, policy).changed
    row, saved = current(h, event)
    assert saved.is_deleted and row['version'] == 2
    assert row['version_time'] == saved.version_time == 1001
    assert h.claim() is None
    assert h.task(task['task_id'])['last_error'] == 'source_changed'

    assert not h.ingest(replace(event, revision='', version_time=9000, observed_via='poll'), policy).changed
    row, saved = current(h, event)
    assert saved.is_deleted and row['version'] == 2 and row['version_time'] == 1001
    assert h.store.get_event({'item_id': event.item_id, 'version': 1}).is_deleted is False


def test_native_snapshot_timestamp_conflict_rolls_back_without_cursor_advance(rig):
    h, _ = rig
    event, policy = source_and_policy(revision='feishu-message-v1:1000000:old')
    h.ingest(event, policy)
    h.route_one()
    tables = ('gw_items', 'gw_item_versions', 'h1_inbox', 'h1_tasks', 'h1_sync')
    before = {table: h.store.rows('SELECT * FROM ' + table) for table in tables}
    conflicting = replace(event, revision='feishu-message-v1:1000000:other', attachments=())
    with pytest.raises(ContractError, match='conflicting native snapshot'):
        h.ingest(conflicting, policy, cursor={'must_not': 'advance'})
    after = {table: h.store.rows('SELECT * FROM ' + table) for table in tables}
    assert before == after


def test_newer_native_snapshot_remains_eligible_after_weak_poll(rig):
    h, _ = rig
    event, policy = source_and_policy(revision='feishu-message-v1:1000000:old')
    h.ingest(event, policy)
    h.ingest(replace(event, revision='', version_time=9000, observed_via='poll'), policy)
    newer = replace(event, revision='feishu-message-v1:1001000:new', version_time=1001, attachments=())
    assert h.ingest(newer, policy).changed
    row, saved = current(h, event)
    assert saved.revision == newer.revision and row['version_time'] == 1001 and saved.attachments == ()


def test_generic_revisions_keep_existing_equal_timestamp_compatibility(rig):
    h, _ = rig
    event, policy = source_and_policy()
    h.ingest(event, policy)
    assert h.ingest(replace(event, revision='r2', attachments=()), policy).changed
    assert current(h, event)[1].attachments == ()


def test_policy_narrowing_still_applies_on_weak_content_duplicate(rig):
    h, _ = rig
    event, policy = source_and_policy()
    h.ingest(event, policy)
    task = h.route_one()
    narrowed = replace(policy, version=2, allow_reply=False)
    h.ingest(replace(event, revision='', version_time=9000, body='摘要'), narrowed)
    assert Policy.parse(h.task(task['task_id'])['policy_json']).allow_reply is False
    row, saved = current(h, event)
    assert row['version_time'] == 1000 and saved.body == event.body


def test_trusted_host_metadata_can_enrich_without_changing_source_content(rig):
    h, _ = rig
    event, policy = source_and_policy()
    event = replace(event, project_ids=())
    h.ingest(event, policy)
    h.route_one()
    weak = replace(event, revision='', version_time=9000, project_ids=('A',), observed_via='poll')
    assert h.ingest(weak, policy).changed
    row, saved = current(h, event)
    assert saved.project_ids == ('A',) and saved.attachments == event.attachments
    assert row['version_time'] == saved.version_time == 1000
