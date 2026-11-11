import copy
from dataclasses import replace
import io
import json
from pathlib import Path
import sys
import threading
import time
from xml.etree import ElementTree
import zipfile

import pytest

from am_gateway.models import ContractError, key
from am_gateway.phase1 import Harness, HarnessConfig, Policy
from am_gateway.phase1.agents import (
    AgentRunner, ArtifactStore, CodexDraftAdapter, ProposalRepository,
    build_agent_runners, model_from_config,
)
from am_gateway.phase1.contracts import business_fingerprint, registry
from am_gateway.store import Store


def envelope(intent='document.process', inputs=None):
    inputs = inputs or {'project_ids': ['A']}
    identity = dict(task_id='task-1', request_revision=1,
                    business_operation_id='operation-1', idempotency_key='operation-1',
                    dispatch_attempt_id='attempt-1',
                    business_fingerprint=business_fingerprint(text='请生成指定产物', intent=intent,
                        principal='owner', reply_target='owner-dm', attachments=[], inputs=inputs))
    return {'schema_version': '1.0', 'identity': identity,
            'routing': {'intent': intent, 'adapter': intent.split('.')[0] + '_agent'},
            'source': {'tenant_id': 'tenant', 'native_id': 'message-1', 'chat_id': 'owner-dm',
                       'thread_id': '', 'sender_id': 'owner'},
            'payload': {'raw_text': '请生成指定产物', 'attachment_refs': [], 'structured_inputs': inputs},
            'policy': {'principal': 'owner', 'authorization_ref': 'existing-owner-authority',
                       'reply_target': 'owner-dm', 'source_chats': ['owner-dm'], 'projects': ['A'],
                       'business_operations': [], 'allow_reply': True, 'version': 1},
            'execution': {'scope': intent, 'generation': 1,
                          'accept_deadline': time.time() + 60, 'completion_deadline': time.time() + 60},
            'context': {'evidence_items': [{'info_id': 'info-1', 'body': '原计划周五完成'}]},
            'output': {'reply_target': 'owner-dm'}, 'source_version': 3}


def project_payload():
    return {'project_id': 'A', 'target': {'type': 'work_item', 'id': 'item-1'},
            'action': 'update_fields', 'changes': {'status': 'completed'},
            'reason': '用户请求', 'approved': True, 'status': 'already_applied'}


@pytest.mark.parametrize('fmt', ['md', 'json', 'docx'])
def test_document_formats_have_actual_full_content_and_stable_hash(tmp_path, fmt):
    artifacts = ArtifactStore(tmp_path)
    runner = AgentRunner('document.process', artifacts)
    env = envelope(inputs={'document': {'title': '项目进展', 'format': fmt,
                           'content': '计划周五完成。\n客户希望提前。',
                           'sections': [{'heading': '待确认', 'body': '尚未确认新的正式日期。'}]}})
    result = runner(env, {'run_id': 'run-1'})
    assert result['response_kind'] == 'sufficient'
    assert result['run_id'] == 'run-1'
    assert result['delivery_state'] == 'not_delivered'
    artifact = result['artifacts'][0]
    data = artifacts.read(artifact['artifact_id'], task_id='task-1')
    assert artifacts.get(artifact['artifact_id']) == artifact
    assert len(data) == artifact['size_bytes']
    if fmt == 'docx':
        with zipfile.ZipFile(io.BytesIO(data)) as archive:
            xml = archive.read('word/document.xml')
            ElementTree.fromstring(xml)
            assert '尚未确认新的正式日期。' in xml.decode()
    else:
        assert '尚未确认新的正式日期。' in data.decode()
    assert runner(env, {'run_id': 'run-1'})['artifacts'] == result['artifacts']


def test_artifact_download_is_registered_scoped_and_integrity_checked(tmp_path):
    store = ArtifactStore(tmp_path)
    artifact = store.write(task_id='task', business_id='op', name='x.md', data=b'original', media_type='text/markdown')
    with pytest.raises(ContractError, match='binding'):
        store.read(artifact['artifact_id'], task_id='another-task')
    with pytest.raises(ContractError):
        store.get('../secret')
    (tmp_path / artifact['relative_path']).write_bytes(b'tampered')
    with pytest.raises(ContractError, match='integrity'):
        store.get(artifact['artifact_id'])


def test_engineering_files_are_packaged_without_executing_or_editing_checkout(tmp_path):
    artifacts = ArtifactStore(tmp_path / 'artifacts')
    checkout = tmp_path / 'app.py'
    checkout.write_text('old')
    runner = AgentRunner('code.execute', artifacts)
    env = envelope('code.execute', {'files': [
        {'path': 'src/app.py', 'content': "print('hello')\n"},
        {'path': '.gitignore', 'content': '__pycache__/\n'}],
        'patch': '--- a/app.py\n+++ b/app.py\n'})
    result = runner(env)
    assert result['response_kind'] == 'partial'
    assert checkout.read_text() == 'old'
    with zipfile.ZipFile(io.BytesIO(artifacts.read(result['artifacts'][0]['artifact_id']))) as archive:
        assert archive.namelist() == ['src/app.py', '.gitignore', 'changes.patch']
        assert archive.read('src/app.py') == b"print('hello')\n"


@pytest.mark.parametrize('path', ['../../secrets', '/tmp/evil', 'C:\\evil', '.git/config'])
def test_engineering_artifact_paths_cannot_escape_bundle(tmp_path, path):
    runner = AgentRunner('code.execute', ArtifactStore(tmp_path))
    with pytest.raises(ContractError, match='unsafe'):
        runner(envelope('code.execute', {'files': [{'path': path, 'content': 'x'}]}))


def test_project_agent_strips_model_self_approval_and_preserves_frozen_review(tmp_path):
    repository = ProposalRepository(Store(tmp_path / 'state.sqlite'), 'owner')
    runner = AgentRunner('project.execute', ArtifactStore(tmp_path / 'artifacts'), proposals=repository)
    result = runner(envelope('project.execute', {'proposal': project_payload()}))
    proposal = repository.get(result['proposal_id'])
    assert proposal['status'] == 'draft'
    assert proposal['source_version'] == 3
    assert 'approved' not in proposal['payload'] and 'status' not in proposal['payload']
    assert proposal['approved_by'] == ''
    assert result['response_kind'] == 'partial'
    assert result['remaining_requirements']
    assert repository.list('draft')[0]['proposal_id'] == result['proposal_id']


def test_authenticated_confirmation_freezes_version_and_does_not_replay(tmp_path):
    repository = ProposalRepository(Store(tmp_path / 'state.sqlite'), 'owner')
    proposal = repository.propose(envelope('project.execute'), 'project.execute', project_payload())
    calls = []
    def execute(row, *, idempotency_key):
        calls.append(idempotency_key)
        assert row['approved_by'] == 'owner' and row['status'] == 'executing'
        assert row['payload']['changes']['status'] == 'completed'
        return {'status': 'applied', 'evidence_ref': 'host-readback:one', 'receipt': {'version': 12}}
    args = dict(version=proposal['version'], digest=proposal['digest'], executor=execute)
    with pytest.raises(ContractError, match='owner'):
        repository.confirm(proposal['proposal_id'], actor='third-party', **args)
    with pytest.raises(ContractError, match='changed'):
        repository.confirm(proposal['proposal_id'], actor='owner', **{**args, 'version': 99})
    first = repository.confirm(proposal['proposal_id'], actor='owner', **args)
    again = repository.confirm(proposal['proposal_id'], actor='owner', **args)
    assert first == again
    assert first['status'] == 'applied'
    assert len(calls) == 1


def test_concurrent_confirmation_only_calls_executor_once(tmp_path):
    repository = ProposalRepository(Store(tmp_path / 'state.sqlite'), 'owner')
    proposal = repository.propose(envelope('project.execute'), 'project.execute', project_payload())
    calls, results = [], []
    started, release = threading.Event(), threading.Event()
    def execute(row, *, idempotency_key):
        calls.append(idempotency_key)
        started.set()
        assert release.wait(3)
        return {'status': 'applied', 'evidence_ref': 'readback:concurrent'}
    def confirm():
        results.append(repository.confirm(proposal['proposal_id'], version=proposal['version'],
                       digest=proposal['digest'], actor='owner', executor=execute))
    worker = threading.Thread(target=confirm)
    worker.start()
    assert started.wait(2)
    confirm()
    release.set()
    worker.join(3)
    assert len(calls) == 1
    assert {row['status'] for row in results} == {'executing', 'applied'}


def test_unknown_business_effect_is_retained_and_reconciled_without_reexecution(tmp_path):
    repository = ProposalRepository(Store(tmp_path / 'state.sqlite'), 'owner')
    proposal = repository.propose(envelope('project.execute'), 'project.execute', project_payload())
    calls = []
    def execute(row, *, idempotency_key):
        calls.append(idempotency_key)
        raise TimeoutError('sensitive raw service details must not be stored')
    args = dict(version=proposal['version'], digest=proposal['digest'], actor='owner', executor=execute)
    assert repository.confirm(proposal['proposal_id'], **args)['status'] == 'unknown'
    assert repository.confirm(proposal['proposal_id'], **args)['status'] == 'unknown'
    assert len(calls) == 1
    with pytest.raises(ContractError):
        repository.revise(proposal['proposal_id'], expected_version=1, actor='owner', payload={})
    row = repository.reconcile(proposal['proposal_id'], actor='owner',
                               receipt={'status': 'applied', 'evidence_ref': 'independent-readback'})
    assert row['status'] == 'applied'
    assert 'sensitive raw' not in json.dumps(row)


def test_edit_reject_flow_invalidates_stale_buttons_and_never_executes(tmp_path):
    repository = ProposalRepository(Store(tmp_path / 'state.sqlite'), 'owner')
    proposal = repository.propose(envelope('project.execute'), 'project.execute', project_payload())
    payload = copy.deepcopy(proposal['payload'])
    payload['changes'] = {'status': 'in_progress'}
    changed = repository.revise(proposal['proposal_id'], expected_version=1, actor='owner', payload=payload)
    assert changed['version'] == 2 and changed['digest'] != proposal['digest']
    with pytest.raises(ContractError, match='changed'):
        repository.reject(proposal['proposal_id'], version=1, digest=proposal['digest'], actor='owner')
    assert repository.reject(proposal['proposal_id'], version=2, digest=changed['digest'], actor='owner')['status'] == 'rejected'
    calls = []
    row = repository.confirm(proposal['proposal_id'], version=2, digest=changed['digest'], actor='owner',
                             executor=lambda *a, **kw: calls.append(1))
    assert row['status'] == 'rejected' and calls == []


def test_model_output_cannot_reference_unseen_evidence(tmp_path):
    class Model:
        def run(self, request):
            return dict(schema='am.gateway.result.v2', job_id=request['job_id'], attempt_id=request['attempt_id'],
                        status='completed', evidence_refs=['private-unavailable-id'],
                        document={'title': 'result', 'content': 'body'})
    runner = AgentRunner('document.process', ArtifactStore(tmp_path), model=Model())
    with pytest.raises(ContractError, match='evidence'):
        runner(envelope())


def test_real_subprocess_model_generates_document_through_existing_adapter(tmp_path):
    shim = tmp_path / 'model.py'
    shim.write_text('''import json,sys
request=json.load(sys.stdin)
assert request['task_kind']=='document.process'
assert request['output_mode']=='internal'
print(json.dumps({'schema':'am.gateway.result.v2','job_id':request['job_id'],
 'attempt_id':request['attempt_id'],'status':'completed','evidence_refs':['info-1'],
 'document':{'title':'进度记录','format':'md','content':'原计划周五完成。'}}))
''')
    runners = build_agent_runners(artifact_root=tmp_path / 'artifacts', config={
        'document_agent': {'model': {'kind': 'json_command', 'argv': [sys.executable, str(shim)]}}})
    result = runners['document_agent'](envelope(), {'run_id': 'real-local-process'})
    assert result['run_id'] == 'real-local-process'
    assert result['evidence_refs'] == ['info-1']
    assert b'# ' in runners['document_agent'].artifacts.read(result['artifacts'][0]['artifact_id'])


def test_codex_transport_uses_selected_model_and_consumes_actual_output_file(tmp_path):
    binary = tmp_path / 'codex-fixture'
    binary.write_text('''#!/usr/bin/env python3
import json,sys
from pathlib import Path
args=sys.argv
assert args[1]=='exec' and args[args.index('--model')+1]=='verified-model'
assert args[args.index('--sandbox')+1]=='read-only'
prompt=sys.stdin.read(); request=json.loads(prompt.split('Input data:\\n',1)[1])
Path(args[args.index('--output-last-message')+1]).write_text(json.dumps({
 'schema':'am.gateway.result.v2','job_id':request['job_id'],'attempt_id':request['attempt_id'],
 'status':'completed','evidence_refs':[], 'document':{'content':'从实际文件返回的正文'}}))
print(json.dumps({'item':{'type':'agent_message'}}))
''')
    binary.chmod(0o700)
    runner = AgentRunner('document.process', ArtifactStore(tmp_path / 'artifacts'),
                         model=CodexDraftAdapter(binary, 'verified-model'))
    result = runner(envelope())
    assert result['response_kind'] == 'sufficient'
    assert '实际文件' in runner.artifacts.read(result['artifacts'][0]['artifact_id']).decode()


def test_runner_result_fits_existing_lifecycle_schema(tmp_path):
    from am_gateway.models import Event
    caps = registry()
    caps['document.process'] = replace(caps['document.process'], available=True, durable_acceptance=True,
                                        evidence_ref='local-test-only')
    harness = Harness(Store(tmp_path / 'state.sqlite'), HarnessConfig('tenant','bot','owner','owner-dm',capabilities=caps))
    event = Event('tenant','m1','owner-dm','owner','请生成一个文档',time.time(),channel='bot_dm',project_ids=('A',))
    policy = Policy('owner','existing-authority','owner-dm',('owner-dm',),('A',),allow_reply=True)
    harness.ingest(event, policy)
    task = harness.route_one()
    env = harness.claim()
    assert env and task
    class Model:
        def run(self, request):
            return dict(schema='am.gateway.result.v2', job_id=request['job_id'], attempt_id=request['attempt_id'],
                        status='completed', evidence_refs=[], document={'content':'完整交付内容'})
    runner = AgentRunner('document.process', ArtifactStore(tmp_path / 'artifacts'), model=Model())
    result = runner(env, {'run_id':'run1'})
    identity = env['identity']
    harness.record_receipt(event_id='result1', task_id=identity['task_id'], business_id=identity['business_operation_id'],
                           generation=env['execution']['generation'], kind='result', payload=result)
    recorded = json.loads(harness.task(identity['task_id'])['result_json'])
    assert recorded['reply_text'] == result['reply_text']
    assert recorded['artifacts'][0]['artifact_id'] == result['artifacts'][0]['artifact_id']


def test_missing_model_is_honest_clarification_not_fake_generated_output(tmp_path):
    runner = AgentRunner('document.process', ArtifactStore(tmp_path))
    result = runner(envelope())
    assert result['response_kind'] == 'clarification'
    assert result['artifacts'] == []
    assert result['remaining_requirements']


def test_cancellation_prevents_artifact_publication(tmp_path):
    runner = AgentRunner('document.process', ArtifactStore(tmp_path))
    with pytest.raises(ContractError, match='cancelled'):
        runner(envelope(inputs={'document': {'content': 'never publish'}}), {'cancelled': lambda: True})
    assert not list(tmp_path.iterdir())
