"""Per-message refresh diagnostics must recover without claiming chat coverage."""
import json

import pytest

from am_gateway.collectors import SourceSpec
from am_gateway.feishu_snapshot import FeishuMessageSnapshotReader, SnapshotIncomplete
from am_gateway.models import ContractError
from am_gateway.phase1 import Policy
from integration.phase1_runtime import Phase1Ingress, Phase1Ports


SOURCE = SourceSpec('tenant', 'owner-dm', channel='bot_dm', identity='am_bot', project_ids=('A',))


def raw_message(message_id='om_one', at=1794218400000, image='old'):
    return {'message_id': message_id, 'chat_id': 'owner-dm', 'msg_type': 'post',
            'create_time': '1794218300000', 'update_time': str(at), 'deleted': False,
            'sender': {'id': 'owner', 'id_type': 'open_id', 'sender_type': 'user'},
            'body': {'content': json.dumps({'content': [[
                {'tag': 'text', 'text': 'A 项目目前卡在哪里？'},
                {'tag': 'img', 'image_key': image},
            ]]})}}


def make_ingress(h, request=None, scope=lambda source, raw: True):
    policy = Policy('owner', 'verified-host', 'owner-dm', ('owner-dm',), ('A',), allow_reply=True)
    ports = Phase1Ports(h.config, h.store.path, lambda raw: True, lambda event: policy,
                        lambda raw: ('A',), lambda raw: False,
                        snapshot_reader=FeishuMessageSnapshotReader(request) if request else None,
                        verified_snapshot_scope=scope)
    return Phase1Ingress(h, ports)


def seed_chat_coverage(h):
    with h.store.transaction() as db:
        db.execute('INSERT INTO h1_sync VALUES(?,?,?,?,?,?)',
                   ('owner-dm', '{"page":"retain"}', 'scanning', '',
                    '{"history":"partial"}', h.store.clock()))
    return h.store.rows('SELECT * FROM h1_sync')


def test_failed_refresh_recovers_and_retains_last_accepted_revision(rig):
    h, clock = rig
    coverage_before = seed_chat_coverage(h)
    response = [{'code': 0, 'data': {'items': [raw_message()]}}]
    ingress = make_ingress(h, lambda *args: response[0])
    ingress.refresh_message('om_one', source=SOURCE)
    first = ingress.snapshot_refresh_status('om_one', source=SOURCE)
    assert first['state'] == 'saved' and first['detail'] == '' and first['accepted_revision']

    clock[0] += 1
    response[0] = {'code': 999}
    with pytest.raises(SnapshotIncomplete):
        ingress.refresh_message('om_one', source=SOURCE)
    failed = ingress.snapshot_refresh_status('om_one', source=SOURCE)
    assert failed['state'] == 'failed' and failed['detail'] == 'SnapshotIncomplete'
    assert failed['accepted_revision'] == first['accepted_revision']

    clock[0] += 1
    response[0] = {'code': 0, 'data': {'items': [raw_message(at=1794218401000, image='new')]}}
    ingress.refresh_message('om_one', source=SOURCE)
    recovered = ingress.snapshot_refresh_status('om_one', source=SOURCE)
    assert recovered['state'] == 'saved' and recovered['detail'] == ''
    assert recovered['accepted_revision'] != first['accepted_revision']
    assert recovered['updated'] == clock[0]
    assert h.store.rows('SELECT * FROM h1_sync') == coverage_before


def test_refresh_diagnostics_are_isolated_by_message_id(rig):
    h, _ = rig
    coverage_before = seed_chat_coverage(h)
    response = [{'code': 999}]
    ingress = make_ingress(h, lambda *args: response[0])
    with pytest.raises(SnapshotIncomplete):
        ingress.refresh_message('om_one', source=SOURCE)
    one = ingress.snapshot_refresh_status('om_one', source=SOURCE)
    response[0] = {'code': 0, 'data': {'items': [raw_message('om_two')]}}
    ingress.refresh_message('om_two', source=SOURCE)
    assert ingress.snapshot_refresh_status('om_one', source=SOURCE) == one
    two = ingress.snapshot_refresh_status('om_two', source=SOURCE)
    assert one['state'] == 'failed' and two['state'] == 'saved'
    assert one['accepted_revision'] == '' and two['accepted_revision']
    assert h.store.rows('SELECT * FROM h1_sync') == coverage_before


def test_scope_rejected_refresh_is_visible_without_mutating_source(rig):
    h, _ = rig
    allowed = [True]
    ingress = make_ingress(h, lambda *args: {'code': 0, 'data': {'items': [raw_message()]}},
                           scope=lambda source, raw: allowed[0])
    ingress.refresh_message('om_one', source=SOURCE)
    previous = ingress.snapshot_refresh_status('om_one', source=SOURCE)
    sources_before = h.store.rows('SELECT * FROM gw_items')
    allowed[0] = False
    assert not ingress.refresh_message('om_one', source=SOURCE).captured
    rejected = ingress.snapshot_refresh_status('om_one', source=SOURCE)
    assert rejected['state'] == 'scope_rejected'
    assert rejected['accepted_revision'] == previous['accepted_revision']
    assert h.store.rows('SELECT * FROM gw_items') == sources_before
    assert h.store.rows('SELECT * FROM h1_sync') == []


def test_status_lookup_and_direct_admission_do_not_start_reads_or_claim_coverage(rig):
    h, _ = rig
    calls = []
    ingress = make_ingress(h, lambda *args: calls.append(args))
    assert ingress.snapshot_refresh_status('om_one', source=SOURCE) is None
    ingress.ingest_feishu_snapshot(raw_message(), source=SOURCE)
    assert ingress.snapshot_refresh_status('om_one', source=SOURCE) is None
    assert calls == [] and h.store.rows('SELECT * FROM h1_sync') == []


def test_unbound_reader_records_failure_without_network_or_source_mutation(rig):
    h, _ = rig
    ingress = make_ingress(h)
    with pytest.raises(ContractError, match='reader is not bound'):
        ingress.refresh_message('om_one', source=SOURCE)
    state = ingress.snapshot_refresh_status('om_one', source=SOURCE)
    assert state['state'] == 'failed' and state['detail'] == 'ContractError'
    assert state['accepted_revision'] == ''
    assert h.store.rows('SELECT * FROM gw_items') == h.store.rows('SELECT * FROM h1_sync') == []


def test_stale_read_reports_current_accepted_revision(rig):
    h, _ = rig
    latest = raw_message(at=1794218401000, image='new')
    response = [{'code': 0, 'data': {'items': [latest]}}]
    ingress = make_ingress(h, lambda *args: response[0])
    ingress.refresh_message('om_one', source=SOURCE)
    first = ingress.snapshot_refresh_status('om_one', source=SOURCE)
    response[0] = {'code': 0, 'data': {'items': [raw_message()]}}
    assert ingress.refresh_message('om_one', source=SOURCE).duplicate
    state = ingress.snapshot_refresh_status('om_one', source=SOURCE)
    assert state['state'] == 'saved' and state['accepted_revision'] == first['accepted_revision']
    assert h.store.rows('SELECT * FROM h1_sync') == []
