"""Real target validation and SQLite collection; synthetic CLI responses only."""
from copy import deepcopy
from types import SimpleNamespace

import pytest

from am_info_sync.models import PermissionDenied, Principal, RetryableError, Source
from am_info_sync.runtime import InformationService
from fs_agent.config import BoundaryError
from info_readers import ExistingMeegleReader
from test_document_snapshot_r9 import Rig
from test_meegle_port import port_fixture


@pytest.fixture
def env(tmp_path):
    rig = Rig(tmp_path / 'information.sqlite')
    rig.add()
    root = tmp_path / 'synthetic-meegle'
    root.mkdir()
    (root / 'state').mkdir()
    gateway = SimpleNamespace(config=SimpleNamespace(root=root, owner='ou_a'),
        store=SimpleNamespace(clock=lambda: rig.time[0] / 1000),
        authorized=lambda principal: principal.get('user') == 'ou_a')
    # This is the production MeeglePort/_state/document_target chain; only
    # _run returns synthetic platform responses. No executable is invoked.
    port, state, settings = port_fixture(gateway)
    principal = {'user': 'ou_a', 'expires': rig.row['expires']}
    reader = ExistingMeegleReader(port, lambda: principal)
    source = Source('meegle-source', 'project_snapshot', '200', 'tenant', 'owner',
        enabled=True, settings={'target_alias': 'report', 'subject_id': 'project-a',
            'fact_mappings': {'system_record': {
                'value_path': ['record'], 'quote_path': ['status_quote']}}})
    rig.store.register_source(source)
    rig.service = InformationService(rig.store, {'project_snapshot': reader})
    rig.host.service = rig.service
    result = SimpleNamespace(rig=rig, port=port, state=state, settings=settings,
                             source=source, reader=reader, principal=principal)
    collect(result)
    try:
        yield result
    finally:
        rig.close()


def collect(env):
    rig = env.rig
    run = rig.service.sync_engine.run_source(env.source.source_id)
    assert run['state'] == 'complete'
    version = rig.store.conn.execute('SELECT current_version FROM objects WHERE source_id=?',
                                    (env.source.source_id,)).fetchone()[0]
    raw = rig.store.get_observation(env.source.source_id, env.source.remote_id, version)
    rig.service.facts.process(raw, [])  # Trusted structured mapping, no model.


def source_facts(env):
    result = env.rig.service.query('project-a', Principal('owner', 'tenant'))
    return [fact for fact in result['facts'] if fact['evidence']['source_id'] == env.source.source_id]


def assert_read_only(env):
    assert env.state['uploads'] == env.state['updates'] == 0
    assert all(command[:2] in (['user', 'me'], ['workitem', 'meta-fields'],
        ['workitem', 'get'], ['workflow', 'get-node']) for command in env.state['commands'])


def invalidate(env, case):
    state, binding = env.state, env.settings['targets']['report']
    if case == 'cli_identity':
        state['identity'] = 'another-user'
    elif case == 'project_id':
        state['project_attribute']['work_item_id'] = '101'
    elif case == 'project_type':
        state['project_attribute']['work_item_type'] = {'key': 'deliverable'}
    elif case == 'project_code':
        state['project_attribute']['work_item_name'] = 'IRAD-2099-002 其他项目'
    elif case == 'project_code_ambiguous':
        state['project_attribute']['work_item_name'] = 'IRAD-2099-001 / IRAD-2099-002'
    elif case == 'project_relation_missing':
        state['missing_fields'] = ['parent']
    elif case == 'project_relation_ambiguous':
        state['field_overrides'] = {'parent': [{'id': 100}, {'id': 101}]}
    elif case == 'wrong_project_relation':
        state['field_overrides'] = {'parent': {'id': 101}}
    elif case == 'wrong_node_relation':
        state['field_overrides'] = {'node': 'another-node'}
    elif case == 'native_parent_changed':
        state['links'] = [201]
    elif case == 'node_unverified':
        state['node_pagination'] = {'has_more': True}
    elif case == 'wrong_document_kind':
        state['kind'] = '项目章程'
    elif case == 'unknown_document_kind':
        state['kind'] = '未支持的文种'
    elif case == 'legacy_config':
        binding.pop('project_type')
    elif case == 'field_roles_conflict':
        binding['document_field'] = 'files'
    elif case == 'unsupported_field_type':
        state['metadata'][0]['field_type'] = 'file'
    elif case == 'missing_field_definition':
        state['metadata'] = [field for field in state['metadata'] if field['field_key'] != 'parent']
    elif case == 'field_candidate_mismatch':
        state['review'] = 'unknown-candidate'
    else:
        raise AssertionError(case)


@pytest.mark.parametrize('case', [
    'cli_identity', 'project_id', 'project_type', 'project_code', 'project_code_ambiguous',
    'project_relation_missing', 'project_relation_ambiguous', 'wrong_project_relation',
    'wrong_node_relation', 'native_parent_changed', 'node_unverified',
    'wrong_document_kind', 'unknown_document_kind', 'legacy_config',
    'field_roles_conflict', 'unsupported_field_type', 'missing_field_definition',
    'field_candidate_mismatch'])
def test_validated_scope_failures_revoke_old_readability_and_advance_epoch(env, case):
    assert source_facts(env)
    _, old = env.rig.capture()
    before = env.rig.store.access_version()
    observation_count = env.rig.store.conn.execute('SELECT COUNT(*) FROM observations').fetchone()[0]
    invalidate(env, case)
    with pytest.raises(PermissionDenied, match='^meegle_identity_or_scope_changed$'):
        env.rig.service.sync_engine.run_source(env.source.source_id)
    status = env.rig.store.source_status(env.source.source_id)
    assert status['availability'] == 'denied'
    assert status['last_error'] == 'permission_denied'
    assert env.rig.store.access_version() != before
    assert source_facts(env) == []
    assert env.rig.store.conn.execute('SELECT COUNT(*) FROM observations').fetchone()[0] == observation_count
    _, current = env.rig.capture()
    assert old['component_hashes']['authorization'] != current['component_hashes']['authorization']
    assert_read_only(env)


def test_successful_revalidation_restores_source_but_cannot_restore_old_document_binding(env):
    _, old = env.rig.capture()
    original = deepcopy(env.state['project_attribute'])
    invalidate(env, 'project_code')
    with pytest.raises(PermissionDenied):
        env.rig.service.sync_engine.run_source(env.source.source_id)
    denied_epoch = env.rig.store.access_version()
    env.state['project_attribute'] = original
    collect(env)
    assert env.rig.store.source_status(env.source.source_id)['availability'] == 'ok'
    assert source_facts(env) and all(not fact['stale'] for fact in source_facts(env))
    assert env.rig.store.access_version() != denied_epoch
    _, restored = env.rig.capture()
    for component in ('sources', 'objects', 'facts', 'gaps', 'quality', 'pending'):
        assert restored['component_hashes'][component] == old['component_hashes'][component]
    assert restored['component_hashes']['authorization'] != old['component_hashes']['authorization']
    with pytest.raises(BoundaryError, match='SOURCE_ACCESS_CHANGED'):
        env.rig.host.document_evidence_current(env.rig.row, old)
    assert env.rig.host.document_evidence_current(env.rig.row, restored) is True
    assert_read_only(env)


@pytest.mark.parametrize('failure', ['network', 'metadata_transport', 'metadata_incomplete',
                                    'metadata_budget', 'read_deadline', 'unknown_code'])
def test_temporary_acquisition_failures_still_retry_without_revoking(env, failure):
    before = env.rig.store.access_version()
    if failure == 'metadata_transport':
        env.state['metadata_error'] = 'BUSINESS_CALL_FAILED'
    elif failure == 'metadata_incomplete':
        env.state['metadata_response'] = {'list': [], 'pagination': {'has_more': 'unknown'}}
    elif failure == 'metadata_budget':
        env.state['metadata_response'] = {'list': [], 'pagination': {'has_more': True}}
    elif failure == 'read_deadline':
        env.principal['expires'] = env.rig.time[0] / 1000 - 1
    else:
        error = ConnectionError('synthetic transport body must not escape') if failure == 'network' else BoundaryError('BUSINESS_UNKNOWN_TRANSIENT')
        def fail(*args, **kwargs):
            raise error
        env.port._run = fail
    with pytest.raises(RetryableError, match='^meegle_read_unavailable$'):
        env.rig.service.sync_engine.run_source(env.source.source_id)
    status = env.rig.store.source_status(env.source.source_id)
    assert status['availability'] == 'error'
    assert status['last_error'] == 'retryable_error'
    assert env.rig.store.access_version() == before
    assert source_facts(env) and all(fact['stale'] for fact in source_facts(env))
    assert_read_only(env)


def test_sync_worker_preserves_retry_job_for_network_error(env):
    before = env.rig.store.access_version()
    env.state['metadata_error'] = 'BUSINESS_CALL_UNCERTAIN'
    assert env.rig.service.scheduler.enqueue_due() == 1
    assert env.rig.service.sync_worker.run_once() is True
    row = env.rig.store.conn.execute("SELECT state,last_error,available_at_ms FROM jobs WHERE kind='sync'").fetchone()
    assert row['state'] == 'pending' and row['last_error'] == 'retryable_error'
    assert row['available_at_ms'] > env.rig.time[0]
    assert env.rig.store.access_version() == before
    assert env.rig.store.source_status(env.source.source_id)['availability'] == 'error'
    assert_read_only(env)


@pytest.mark.parametrize('failure', ['wrong_node', 'network'])
def test_second_node_read_cannot_turn_scope_mismatch_into_stale_readability(env, failure):
    original = env.port._run
    count = 0
    def changed(args, **kwargs):
        nonlocal count
        if args[:2] == ['workflow', 'get-node']:
            count += 1
            if count == 2 and failure == 'network':
                raise ConnectionError('synthetic node transport body')
        result = original(args, **kwargs)
        if count == 2 and args[:2] == ['workflow', 'get-node']:
            result['list'][0]['basic']['node_key'] = 'another-node'
        return result
    env.port._run = changed
    before = env.rig.store.access_version()
    if failure == 'wrong_node':
        with pytest.raises(PermissionDenied, match='^meegle_identity_or_scope_changed$'):
            env.rig.service.sync_engine.run_source(env.source.source_id)
        assert env.rig.store.source_status(env.source.source_id)['availability'] == 'denied'
        assert env.rig.store.access_version() != before
        assert source_facts(env) == []
    else:
        with pytest.raises(RetryableError, match='^meegle_read_unavailable$'):
            env.rig.service.sync_engine.run_source(env.source.source_id)
        assert env.rig.store.source_status(env.source.source_id)['availability'] == 'error'
        assert env.rig.store.access_version() == before
        assert source_facts(env)
    assert_read_only(env)
