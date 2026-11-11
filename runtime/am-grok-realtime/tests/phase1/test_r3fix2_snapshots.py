"""Full raw snapshots through the actual Phase1Ingress and durable task path."""
from copy import deepcopy
from dataclasses import replace
import json

import pytest

from am_gateway.collectors import SourceSpec
from am_gateway.feishu_snapshot import (
    FeishuMessageSnapshotReader, SnapshotIncomplete, normalize_feishu_snapshot,
    snapshot_host_payload,
)
from am_gateway.models import ContractError, Event
from am_gateway.phase1 import Policy
from integration.phase1_runtime import Phase1Ingress, Phase1Ports


SOURCE = SourceSpec('tenant', 'owner-dm', channel='bot_dm', identity='am_bot', project_ids=('A',))


def message(at=1000000, attachment='old', *, mid='snapshot', text='A 项目目前卡在哪里？'):
    row = [{'tag': 'text', 'text': text}]
    if attachment is not None:
        row.append({'tag': 'img', 'image_key': attachment})
    return {'message_id': mid, 'chat_id': 'owner-dm', 'msg_type': 'post',
            'create_time': '999000', 'update_time': str(at), 'deleted': False,
            'sender': {'id': 'owner', 'id_type': 'open_id', 'sender_type': 'user', 'tenant_key': 'tenant'},
            'body': {'content': json.dumps({'zh_cn': {'title': '', 'content': [row]}})},
            'mentions': []}


def ingress_for(h, read=None, *, scope=lambda source, raw: True, legacy=False, authorized=True):
    policy = Policy('owner', 'verified-host', 'owner-dm', ('owner-dm',), ('A',), allow_reply=True)
    ports = Phase1Ports(h.config, h.store.path, lambda payload: True,
                        lambda event: policy if authorized else None,
                        lambda payload: ('A',), lambda payload: legacy,
                        snapshot_reader=read, verified_snapshot_scope=scope)
    return Phase1Ingress(h, ports)


def saved(h, mid='snapshot'):
    row = h.store.one('SELECT * FROM gw_items WHERE native_id=?', (mid,))
    return row, Event.from_dict(json.loads(row['event_json']))


def test_old_new_removed_snapshots_change_persisted_request_and_claim(rig):
    h, _ = rig
    calls, current = [], [message()]
    def request(url, params, identity):
        calls.append((url, params, identity))
        # The requested item need not be the first item (e.g. merged-forward output).
        return {'code': 0, 'data': {'items': [message(mid='unrelated'), current[0]]}}
    ingress = ingress_for(h, FeishuMessageSnapshotReader(request))
    versions = []
    for time, file_key in [(1000000, 'old'), (1001000, 'new'), (1002000, None)]:
        current[0] = message(time, file_key)
        admitted = ingress.refresh_message('snapshot', source=SOURCE)
        assert admitted.captured and admitted.claimed and not admitted.duplicate
        task = h.route_one()
        row, event = saved(h)
        versions.append(row['version'])
        expected = [] if file_key is None else [file_key]
        assert [a['resource_key'] for a in event.attachments] == expected
        assert [a['resource_key'] for a in json.loads(task['request_json'])['attachments']] == expected
    assert versions == [1, 2, 3]
    assert h.claim()['payload']['attachment_refs'] == []
    assert h.store.get_event({'item_id': row['item_id'], 'version': 1}).attachments[0]['resource_key'] == 'old'
    assert h.store.get_event({'item_id': row['item_id'], 'version': 2}).attachments[0]['resource_key'] == 'new'
    assert all(c[1] == {'user_id_type': 'open_id'} and c[2] == 'am_bot' for c in calls)
    assert len(calls) == 3


def test_initial_receive_remains_fast_then_explicit_snapshot_replaces(rig):
    h, _ = rig
    calls = []
    original = message(1794218400000)
    original['create_time'] = '1794218300000'
    revised = message(1794218401000, 'new')
    revised['create_time'] = original['create_time']
    def request(*args):
        calls.append(args)
        return {'code': 0, 'data': {'items': [revised]}}
    ingress = ingress_for(h, FeishuMessageSnapshotReader(request))
    ingress.ingest(snapshot_host_payload(original, h.config, SOURCE))
    assert calls == []  # No REST read/model on normal websocket admission.
    h.route_one()
    assert saved(h)[1].revision == ''
    ingress.refresh_message('snapshot', source=SOURCE)
    assert len(calls) == 1
    assert [a['resource_key'] for a in saved(h)[1].attachments] == ['new']


def test_payload_flag_cannot_upgrade_weak_ingress_to_complete_snapshot(rig):
    h, _ = rig
    ingress = ingress_for(h)
    ingress.ingest_feishu_snapshot(message(1001000, 'new'), source=SOURCE)
    forged = snapshot_host_payload(message(1002000, 'old'), h.config, SOURCE)
    forged['source_contract'] = 'feishu.im.v1.message'
    forged['revision'] = 'feishu-message-v1:9999999999:forged'
    ingress.ingest(forged)
    row, event = saved(h)
    assert row['version'] == 1 and event.attachments[0]['resource_key'] == 'new'


def test_same_complete_snapshot_is_idempotent_and_reordered_json_has_same_revision(rig):
    h, _ = rig
    ingress = ingress_for(h)
    original = message()
    assert not ingress.ingest_feishu_snapshot(original, source=SOURCE).duplicate
    duplicate = deepcopy(original)
    duplicate['body']['content'] = json.dumps(json.loads(original['body']['content']), sort_keys=True, indent=2)
    assert ingress.ingest_feishu_snapshot(duplicate, source=SOURCE).duplicate
    assert saved(h)[0]['version'] == 1


@pytest.mark.parametrize('mutation', [
    lambda raw: raw.pop('update_time'),
    lambda raw: raw.update(update_time=1000.5),
    lambda raw: raw.update(update_time=True),
    lambda raw: raw.update(update_time='NaN'),
    lambda raw: raw.update(update_time='998000'),
    lambda raw: raw.pop('body'),
    lambda raw: raw['body'].update(content='{invalid'),
    lambda raw: raw['body'].update(content=json.dumps({'zh_cn': {'content': ['truncated']}})),
    lambda raw: raw['body'].update(content=json.dumps({'zh_cn': {'content': [[{'tag': 'unknown'}]]}})),
    lambda raw: raw['body'].update(content=json.dumps({'zh_cn': {'content': [[{'tag': 'img'}]]}})),
    lambda raw: raw['body'].update(content=json.dumps({'content': [[{'tag': 'text'}]]})),
    lambda raw: raw['body'].update(content=json.dumps({'content': [[{'tag': 'a', 'text': 'x'}]]})),
    lambda raw: raw['body'].update(content=json.dumps({'content_v2': [], 'content': []})),
    lambda raw: raw.update(msg_type='interactive'),
    lambda raw: raw.update(deleted='false'),
])
def test_incomplete_snapshot_never_erases_current_attachments(rig, mutation):
    h, _ = rig
    current = [message()]
    reader = FeishuMessageSnapshotReader(lambda *args: {'code': 0, 'data': {'items': [current[0]]}})
    ingress = ingress_for(h, reader)
    ingress.refresh_message('snapshot', source=SOURCE)
    before = h.store.rows('SELECT * FROM gw_items')
    current[0] = message(1001000, None)
    mutation(current[0])
    with pytest.raises(SnapshotIncomplete):
        ingress.refresh_message('snapshot', source=SOURCE)
    assert h.store.rows('SELECT * FROM gw_items') == before
    assert ingress.snapshot_refresh_status('snapshot', source=SOURCE)['state'] == 'failed'


@pytest.mark.parametrize('response', [
    {'code': 999, 'data': {'items': []}},
    {'code': False, 'data': {'items': []}},
    {'code': 0, 'data': {}},
    {'code': 0, 'data': {'items': []}},
    {'code': 0, 'data': {'items': [message(mid='other')]}},
    {'code': 0, 'data': {'items': [message(), message()]}},
])
def test_exact_get_rejects_failure_absence_wrong_id_and_duplicate_target(response):
    reader = FeishuMessageSnapshotReader(lambda *args: response)
    with pytest.raises(SnapshotIncomplete):
        reader.read_message(SOURCE, 'snapshot')


def test_raw_snapshot_scope_is_separate_from_receive_event_scope(rig):
    h, _ = rig
    ingress = ingress_for(h, scope=lambda source, raw: False)
    assert not ingress.ingest_feishu_snapshot(message(), source=SOURCE).captured
    assert h.store.rows('SELECT * FROM gw_items') == []
    ingress.ports.verified_snapshot_scope = None
    with pytest.raises(ContractError, match='scope verifier'):
        ingress.ingest_feishu_snapshot(message(), source=SOURCE)


@pytest.mark.parametrize('authorized,legacy', [(False, False), (True, True)])
def test_snapshot_keeps_collection_only_and_legacy_ownership(rig, authorized, legacy):
    h, _ = rig
    ingress = ingress_for(h, authorized=authorized, legacy=legacy)
    result = ingress.ingest_feishu_snapshot(message(), source=SOURCE)
    assert result.captured and not result.claimed
    assert h.route_one()['disposition'] == 'collection_only'
    assert h.claim() is None


@pytest.mark.parametrize('source,raw', [
    (replace(SOURCE, tenant_id='other'), message()),
    (replace(SOURCE, native_id='other-chat'), message()),
    (replace(SOURCE, native_id='thread', chat_id='owner-dm', container_type='thread'), message()),
])
def test_wrong_scope_cannot_enter_snapshot(rig, source, raw):
    h, _ = rig
    with pytest.raises(ContractError):
        ingress_for(h).ingest_feishu_snapshot(raw, source=source)
    assert h.store.rows('SELECT * FROM gw_items') == []


def test_native_millisecond_timestamp_and_multimedia_references_are_preserved(rig):
    h, _ = rig
    raw = message(at=1794218400123)
    raw['create_time'] = 1794218300000
    raw['body']['content'] = json.dumps({'content': [[{'tag': 'media', 'file_key': 'video',
                                                       'image_key': 'poster'}]]})
    event = normalize_feishu_snapshot(raw, h.config, SOURCE)
    assert event.original_time == 1794218300.0
    assert event.version_time == 1794218400.123
    assert {a['resource_key'] for a in event.attachments} == {'video', 'poster'}
    assert event.revision.startswith('feishu-message-v1:1794218400123:')


def test_explicit_native_deleted_marker_retracts_without_body(rig):
    h, _ = rig
    ingress = ingress_for(h)
    ingress.ingest_feishu_snapshot(message(), source=SOURCE)
    h.route_one()
    deleted = message(1001000)
    deleted.update(deleted=True)
    deleted.pop('body')
    ingress.ingest_feishu_snapshot(deleted, source=SOURCE)
    assert saved(h)[1].is_deleted and saved(h)[1].attachments == ()
    assert h.claim() is None


@pytest.mark.parametrize('wrapped', [False, True])
def test_top_level_file_zone_and_inline_resources_replace_and_clear_together(rig, wrapped):
    h, _ = rig
    ingress = ingress_for(h)
    old = message()
    content = {'content': [[{'tag': 'img', 'image_key': 'inline-old'}]]}
    if wrapped:
        content = {'zh_cn': content}
    content['files'] = [{'file_key': 'zone-old', 'file_name': 'old.pdf'},
                        {'file_key': 'folder-old', 'file_name': 'assets', 'is_folder': True}]
    old['body']['content'] = json.dumps(content)
    ingress.ingest_feishu_snapshot(old, source=SOURCE)
    row, first = saved(h)
    assert {a['resource_key'] for a in first.attachments} == {'inline-old', 'zone-old', 'folder-old'}
    assert next(a['kind'] for a in first.attachments if a['resource_key'] == 'folder-old') == 'folder'

    # Files-only is an actual raw API response format, not a partial observation.
    new = message(1001000)
    new['body']['content'] = json.dumps({'files': [{'file_key': 'zone-new'}]})
    ingress.ingest_feishu_snapshot(new, source=SOURCE)
    assert [a['resource_key'] for a in saved(h)[1].attachments] == ['zone-new']
    clear = message(1002000)
    clear['body']['content'] = '{"files":[]}'
    ingress.ingest_feishu_snapshot(clear, source=SOURCE)
    assert saved(h)[1].attachments == ()
    assert h.store.get_event({'item_id': row['item_id'], 'version': 1}).attachments == first.attachments


@pytest.mark.parametrize('files', [None, {}, [None], [{}], [{'key': 'send-format'}],
                                  [{'file_key': ''}], [{'file_key': 'x', 'is_folder': 'false'}]])
def test_partial_or_write_format_attachment_zone_cannot_become_empty_snapshot(rig, files):
    h, _ = rig
    ingress = ingress_for(h)
    ingress.ingest_feishu_snapshot(message(), source=SOURCE)
    raw = message(1001000)
    raw['body']['content'] = json.dumps({'content': [], 'files': files})
    with pytest.raises(SnapshotIncomplete):
        ingress.ingest_feishu_snapshot(raw, source=SOURCE)
    assert saved(h)[1].attachments[0]['resource_key'] == 'old'


@pytest.mark.parametrize('content', [
    {'files': [{'file_key': 'plan', 'file_name': 'plan.pdf'}]},
    {'content': [[{'tag': 'img', 'image_key': 'picture'}]]},
    {'files': []},
])
def test_textless_post_does_not_block_shared_information_projection(rig, content):
    h, _ = rig
    raw = message()
    raw['body']['content'] = json.dumps(content)
    ingress_for(h).ingest_feishu_snapshot(raw, source=SOURCE)
    assert saved(h)[1].body.startswith('[post]')
    assert h.information.project_sources() == 1
    assert h.store.rows('SELECT body FROM h1_info')[0]['body'] == saved(h)[1].body
