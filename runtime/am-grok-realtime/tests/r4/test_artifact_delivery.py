from dataclasses import replace
import io
import json
from types import SimpleNamespace as NS
from unittest.mock import Mock

import pytest

from am_gateway.models import ContractError, Event, dumps
from am_gateway.phase1 import Harness, HarnessConfig, Policy
from am_gateway.phase1.contracts import registry
from am_gateway.phase1.agents import ArtifactStore
from am_gateway.store import Store
from integration.phase1_artifacts import ArtifactDelivery


def finished_task(tmp_path):
    caps = registry()
    caps['conversation.reply'] = replace(caps['conversation.reply'], available=True,
        durable_acceptance=True, evidence_ref='synthetic-receiver')
    h = Harness(Store(tmp_path / 'gateway.sqlite'), HarnessConfig('tenant', 'ou_bot', 'ou_owner', 'oc_owner', capabilities=caps))
    p = Policy('ou_owner', 'host-fixture', 'oc_owner', ('oc_owner',), allow_reply=True)
    h.ingest(Event('tenant', 'om_original', 'oc_owner', 'ou_owner', '你好', h.store.clock(), channel='bot_dm'), p)
    task = h.route_one()
    env = h.claim()
    art = ArtifactStore(tmp_path / 'artifacts')
    record = art.write(task_id=task['task_id'], business_id=env['identity']['business_operation_id'],
        name='交付.docx', data=b'actual artifact bytes', media_type='application/octet-stream')
    for kind, payload in [('accepted', {'durable_record': 'receiver-row', 'run_id': 'run1'}),
                          ('result', {'run_id': 'run1', 'response_kind': 'sufficient', 'reply_text': '文件已生成。',
                                      'artifacts': [record], 'remaining_requirements': []}),
                          ('ended', {'run_id': 'run1', 'status': 'succeeded', 'stopped_evidence': 'worker-done'})]:
        h.record_receipt(event_id=kind, task_id=task['task_id'], business_id=env['identity']['business_operation_id'],
                         generation=env['execution']['generation'], kind=kind, payload=payload)
    return h, p, task['task_id'], art, record


def output_message(**changes):
    value = {'message_id': 'om_file', 'chat_id': 'oc_owner', 'msg_type': 'file', 'deleted': False,
             'parent_id': 'om_original', 'create_time': '1000000',
             'sender': {'id': 'ou_bot', 'id_type': 'open_id', 'sender_type': 'app'},
             'body': {'content': dumps({'file_key': 'file_received', 'file_name': '交付.docx'})}}
    value.update(changes)
    return value


def setup_delivery(tmp_path, send_live=True):
    h, policy, tid, artifacts, record = finished_task(tmp_path)
    captured = {}

    def upload(request):
        captured['upload'] = request
        captured['bytes'] = request.request_body.file.read()
        return NS(success=lambda: True, data=NS(file_key='file_received'))

    file_api = NS(create=Mock(side_effect=upload))
    message_api = NS(reply=Mock(return_value=NS(success=lambda: True, data=NS(message_id='om_file'))),
                     create=Mock(return_value=NS(success=lambda: True, data=NS(message_id='om_file'))))
    resource_api = NS(get=Mock(side_effect=lambda request: NS(success=lambda: True,
        file=io.BytesIO(b'actual artifact bytes'))))
    client = NS(im=NS(v1=NS(file=file_api, message=message_api, message_resource=resource_api)))
    reader = NS(read_message=Mock(return_value=output_message()))
    authority = Mock(return_value=policy)
    service = ArtifactDelivery(h, artifacts, client, authority, reader, send_live)
    return NS(h=h, policy=policy, tid=tid, artifacts=artifacts, record=record, client=client,
              reader=reader, service=service, captured=captured, authority=authority)


def test_real_sdk_upload_file_reply_and_exact_readback_are_idempotent(tmp_path):
    s = setup_delivery(tmp_path)
    result = s.service.deliver_task(s.tid)
    assert result['complete'] and result['artifacts'][0]['state'] == 'verified'
    assert result['completed_platform_time'] == 1000
    assert s.captured['bytes'] == b'actual artifact bytes'
    assert s.captured['upload'].request_body.file_type == 'stream'
    assert s.captured['upload'].request_body.file_name == '交付.docx'
    request = s.client.im.v1.message.reply.call_args.args[0]
    assert request.message_id == 'om_original'
    assert request.request_body.msg_type == 'file'
    assert json.loads(request.request_body.content) == {'file_key': 'file_received'}
    source, mid = s.reader.read_message.call_args.args
    assert source.effective_chat_id == 'oc_owner' and mid == 'om_file'
    restarted = ArtifactDelivery(s.h, s.artifacts, s.client, s.authority, s.reader, True)
    assert restarted.deliver_task(s.tid)['complete']
    assert s.client.im.v1.file.create.call_count == 1
    assert s.client.im.v1.message.reply.call_count == 1


def test_unknown_message_outcome_does_not_retry_and_can_reconcile_exact_mapping(tmp_path):
    s = setup_delivery(tmp_path)
    s.client.im.v1.message.reply.side_effect = TimeoutError('ambiguous transport')
    first = s.service.deliver_task(s.tid)
    assert not first['complete'] and first['artifacts'][0]['state'] == 'send_unknown'
    assert not s.service.deliver_task(s.tid)['complete']
    assert s.client.im.v1.message.reply.call_count == 1
    effect_id = first['artifacts'][0]['effect_id']
    assert s.service.reconcile(effect_id, platform_id='om_file')['state'] == 'verified'
    assert s.service.deliver_task(s.tid)['complete']
    assert s.client.im.v1.message.reply.call_count == 1


@pytest.mark.parametrize('changes', [
    {'chat_id': 'oc_foreign'}, {'parent_id': 'om_wrong'}, {'deleted': True},
    {'sender': {'id': 'ou_other', 'sender_type': 'app'}},
    {'body': {'content': dumps({'file_key': 'other', 'file_name': '交付.docx'})}},
    {'body': {'content': dumps({'file_key': 'file_received', 'file_name': 'different.docx'})}},
    {'create_time': 'NaN'}, {'create_time': '0'}, {'create_time': True},
])
def test_file_delivery_requires_matching_target_author_content_and_reply(changes, tmp_path):
    s = setup_delivery(tmp_path)
    s.reader.read_message.return_value = output_message(**changes)
    result = s.service.deliver_task(s.tid)
    assert not result['complete'] and result['artifacts'][0]['state'] == 'readback_pending'
    s.reader.read_message.return_value = output_message()
    assert s.service.deliver_task(s.tid)['complete']
    assert s.client.im.v1.message.reply.call_count == 1


def test_permission_revoked_between_upload_and_send_prevents_message(tmp_path):
    s = setup_delivery(tmp_path)
    s.authority.side_effect = [s.policy, s.policy, None]
    with pytest.raises(ContractError, match='authority'):
        s.service.deliver_task(s.tid)
    s.client.im.v1.message.reply.assert_not_called()
    assert s.h.store.one('SELECT state FROM h4_artifact_effects')['state'] == 'uploaded'


def test_disabled_live_send_and_tampered_artifact_never_upload(tmp_path):
    s = setup_delivery(tmp_path, send_live=False)
    with pytest.raises(ContractError):
        s.service.deliver_task(s.tid)
    s.client.im.v1.file.create.assert_not_called()
    s.service.send_live = True
    target = s.artifacts.root / s.record['relative_path']
    target.write_bytes(b'tampered')
    with pytest.raises(ContractError, match='integrity'):
        s.service.deliver_task(s.tid)
    s.client.im.v1.file.create.assert_not_called()


def test_artifact_from_other_task_cannot_be_delivered(tmp_path):
    s = setup_delivery(tmp_path)
    other = s.artifacts.write(task_id='another-task', business_id='another-op', name='foreign.txt',
                              data=b'x', media_type='text/plain')
    with s.h.store.transaction() as db:
        result = json.loads(s.h.task(s.tid)['result_json'])
        result['artifacts'] = [other]
        db.execute('UPDATE h1_tasks SET result_json=? WHERE task_id=?', (dumps(result), s.tid))
    with pytest.raises(ContractError, match='binding'):
        s.service.deliver_task(s.tid)
    s.client.im.v1.file.create.assert_not_called()


def test_platform_rekey_is_verified_by_downloaded_bytes_without_resending(tmp_path):
    s = setup_delivery(tmp_path)
    s.reader.read_message.return_value = output_message(body={'content': dumps({
        'file_key': 'file_message_resource', 'file_name': '交付.docx'})})
    result = s.service.deliver_task(s.tid)
    row = result['artifacts'][0]
    assert result['complete'] and row['resource_file_key'] == 'file_message_resource'
    assert row['resource_sha256'] == s.record['sha256']
    request = s.client.im.v1.message_resource.get.call_args.args[0]
    assert request.message_id == 'om_file' and request.file_key == 'file_message_resource'
    assert request.type == 'file'
    assert s.h.store.one('SELECT file_key FROM h4_artifact_effects')['file_key'] == 'file_received'
    restarted = ArtifactDelivery(s.h, s.artifacts, s.client, s.authority, s.reader, True)
    assert restarted.deliver_task(s.tid)['complete']
    assert s.client.im.v1.message.reply.call_count == 1
    assert s.client.im.v1.file.create.call_count == 1


@pytest.mark.parametrize('wrong_bytes', [b'actual artifact byteS', b'actual', b'actual artifact bytes extra'])
def test_matching_file_metadata_does_not_accept_changed_truncated_or_larger_bytes(tmp_path, wrong_bytes):
    s = setup_delivery(tmp_path)
    s.client.im.v1.message_resource.get.side_effect = lambda request: NS(success=lambda: True,
        file=io.BytesIO(wrong_bytes))
    result = s.service.deliver_task(s.tid)
    assert not result['complete']
    assert result['artifacts'][0]['detail'] == 'file_resource_integrity_mismatch'
    assert s.client.im.v1.message.reply.call_count == 1


def test_failed_download_retries_only_readback_not_upload_or_send(tmp_path):
    s = setup_delivery(tmp_path)
    s.client.im.v1.message_resource.get.side_effect = TimeoutError('download unavailable')
    assert not s.service.deliver_task(s.tid)['complete']
    s.client.im.v1.message_resource.get.side_effect = lambda request: NS(success=lambda: True,
        file=io.BytesIO(b'actual artifact bytes'))
    assert s.service.deliver_task(s.tid)['complete']
    assert s.client.im.v1.file.create.call_count == s.client.im.v1.message.reply.call_count == 1


def test_recall_during_resource_download_does_not_mark_verified(tmp_path):
    s = setup_delivery(tmp_path)
    s.reader.read_message.side_effect = [output_message(), output_message(deleted=True)]
    result = s.service.deliver_task(s.tid)
    assert not result['complete']
    assert result['artifacts'][0]['detail'] == 'file_message_changed_during_readback'


def test_scope_revoked_during_download_does_not_mark_verified(tmp_path):
    s = setup_delivery(tmp_path)
    def download(request):
        s.authority.return_value = None
        return NS(success=lambda: True, file=io.BytesIO(b'actual artifact bytes'))
    s.client.im.v1.message_resource.get.side_effect = download
    result = s.service.deliver_task(s.tid)
    assert not result['complete'] and result['artifacts'][0]['state'] == 'readback_pending'


def test_old_verified_metadata_receipt_gets_content_readback_without_resending(tmp_path):
    s = setup_delivery(tmp_path)
    assert s.service.deliver_task(s.tid)['complete']
    with s.h.store.transaction() as db:
        db.execute("UPDATE h4_artifact_effects SET resource_file_key='',resource_sha256='',resource_size_bytes=NULL")
    s.client.im.v1.message_resource.get.reset_mock()
    restarted = ArtifactDelivery(s.h, s.artifacts, s.client, s.authority, s.reader, True)
    assert restarted.deliver_task(s.tid)['complete']
    assert s.client.im.v1.message_resource.get.call_count == 1
    assert s.client.im.v1.file.create.call_count == s.client.im.v1.message.reply.call_count == 1
