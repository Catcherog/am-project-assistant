"""Development fixtures: real SQLite/core classes, no Qwen or Feishu calls."""
from copy import deepcopy
from dataclasses import replace
from types import SimpleNamespace
import json
import threading
import time

import pytest

from am_info_sync.document_snapshot import (
    capture_document_snapshot, validate_binding, request_scope, DocumentSnapshotError)
from am_info_sync.models import Observation, Principal, Source, digest
from am_info_sync.runtime import InformationService
from am_info_sync.store import Store
from am_information import AMInformation
from fs_agent.config import BoundaryError
from am_query import QueryMixin, QueryIdentityCache


class Rig:
    def __init__(self, path=':memory:', clock=None):
        self.time = clock if clock is not None else [1_000_000]
        self.store = Store(path, clock=lambda: self.time[0])
        self.service = InformationService(self.store)
        self.project = {'key': 'project-a', 'title': 'IRAD-2099-001 合成项目',
                        'identity_status': 'verified', 'source_revision': '1',
                        '项目进展': '待测试', '待办': '确认条件', '关键日期': '', '人员与职责': ''}
        self.row = {'tenant': 'tenant', 'app': 'app', 'user': 'owner', 'chat': 'private',
                    'chat_type': 'p2p', 'id': 'task', 'expires': 100_000.0}
        self.source = Source('source-a', 'manual', 'local-a', 'tenant', 'owner',
                             enabled=True, settings={'subject_id': 'project-a'},
                             raw_retention_seconds=86400)
        self.store.register_source(self.source)
        self.ledger_source = Source('demo-ledger', 'ledger_snapshot', 'fixed/sheet', 'tenant', 'owner',
            enabled=True, raw_retention_seconds=86400, settings={'host_managed': True,
                'subject_ids': ['project-a', 'project-b'], 'fact_mappings': {'ledger_record': {
                    'subject_path': ['subject_id'], 'value_path': ['record'], 'quote_path': ['quote']}}})
        self.store.register_source(self.ledger_source)
        self.add_ledger()
        self.host = AMInformation.__new__(AMInformation)
        self.host.store, self.host.service = self.store, self.service
        self.auth_calls = []
        self.identity = QueryIdentityCache(lambda user: (_ for _ in ()).throw(
            AssertionError('Unexpected network authorization in fixture')), 600)
        self.identity.entries['owner'] = time.monotonic() + 600
        self.host.am = SimpleNamespace(query_grant=self.grant, owner='owner',
            query_projects=lambda row: [deepcopy(self.project)], query_group=lambda row: None,
            query_identity=self.identity, query_settings={'denied_users': []}, query_scope={},
            g=SimpleNamespace(config=SimpleNamespace(app_id='app', tenant_key='tenant')),
            s=SimpleNamespace(clock=lambda: self.time[0] / 1000),
            ledger=SimpleNamespace(port=SimpleNamespace(disclosure={},
                authorize_private_user=self.identity.check, authorize_user=self.identity.check)),
            snapshots=SimpleNamespace(lock=threading.RLock(), latest='local',
                versions={'local': {'complete': True, 'rows': [self.project]}}))

    def grant(self, row):
        self.auth_calls.append(row['user'])
        return QueryMixin.query_grant(self.host.am, row)

    def add_ledger(self, project=None):
        project = project or self.project
        record = {'title': project['title'], 'identity_status': project.get('identity_status'),
                  'fields': {k: project.get(k, '') for k in ('关键日期', '待办', '项目进展', '人员与职责')}}
        obs = Observation('demo-ledger', project['key'], 'ledger:' + str(project['source_revision']) + ':' + digest(record),
            self.time[0], text=project['title'] + '\n' + '\n'.join(k + '：' + v for k, v in record['fields'].items()),
            payload={'record': record, 'subject_id': project['key'], 'quote': project['title'],
                     'revision_id': int(project['source_revision']), 'time_basis': 'observed_at'})
        self.store.record(obs, authoritative=True)
        self.service.facts.process(self.store.get_observation(obs.source_id, obs.object_id, obs.version), [])
        self.store.conn.execute("UPDATE sources SET availability='ok',last_checked_ms=? WHERE source_id='demo-ledger'", (self.time[0],))

    def add(self, text='工程师说材料已交付，业务核验待完成', *, oid='message', version='v1', source=None):
        src = source or self.source
        obs = Observation(src.source_id, oid, version, self.time[0], text=text)
        self.store.record(obs, authoritative=True)
        saved = self.store.get_observation(src.source_id, oid, version)
        self.service.facts.process(saved, [{
            'source_id': src.source_id, 'object_id': oid, 'version': version,
            'subject_id': src.settings['subject_id'], 'field': 'progress',
            'value': text, 'quote': text, 'assertion_type': 'reported'}])
        return obs

    def capture(self):
        return capture_document_snapshot(self.store, self.service, self.project,
            principal=Principal('owner', 'tenant', ('demo-query',)), scope=request_scope(self.row),
            project_code='IRAD-2099-001', expires=self.row['expires'],
            expected_access_version=self.store.access_version())

    def close(self):
        self.identity.close()
        self.store.close()


@pytest.fixture
def rig():
    value = Rig()
    value.add()
    try:
        yield value
    finally:
        value.close()


def test_one_snapshot_transaction_and_no_database_writes(rig):
    traced = []
    rig.store.conn.set_trace_callback(traced.append)
    before = rig.store.conn.total_changes
    info, binding = rig.capture()
    assert rig.store.conn.total_changes == before
    assert sum(q.startswith('BEGIN') for q in traced) == 1
    assert sum(q == 'COMMIT' for q in traced) == 1
    fact = next(f for f in info['facts'] if f['evidence']['source_id'] == 'source-a')
    assert fact['authority'] == 'unconfirmed'
    assert fact['evidence']['raw_expires_at_ms'] == 87_400_000
    assert binding['expires'] == 87_400.0
    assert '工程师' not in json.dumps(binding, ensure_ascii=False)


def test_wall_clock_and_successful_poll_heartbeat_do_not_change_binding(rig):
    _, old = rig.capture()
    rig.time[0] += 10
    rig.store.conn.execute("UPDATE sources SET last_checked_ms=?", (rig.time[0],))
    _, new = rig.capture()
    assert old['state_hash'] == new['state_hash']


def test_unrelated_source_content_does_not_change_project_binding(rig):
    _, old = rig.capture()
    other = replace(rig.source, source_id='source-b', remote_id='local-b', settings={'subject_id': 'project-b'})
    rig.store.register_source(other)
    rig.add('另一个项目的进展', source=other)
    _, new = rig.capture()
    assert old['state_hash'] == new['state_hash']


def test_new_mapped_source_is_not_missed_by_existing_fact_id_list(rig):
    _, old = rig.capture()
    rig.store.register_source(replace(rig.source, source_id='new-source', remote_id='new-local'))
    _, new = rig.capture()
    assert old['state_hash'] != new['state_hash']


def test_new_unanalysed_object_invalidates_even_before_a_fact_exists(rig):
    _, old = rig.capture()
    rig.store.record(Observation(rig.source.source_id, 'new-message', 'v1', rig.time[0], text='待澄清的新消息'))
    _, new = rig.capture()
    assert old['state_hash'] != new['state_hash']


def test_source_version_change_invalidates(rig):
    _, old = rig.capture()
    rig.time[0] += 1
    rig.add('实际还没有交付', version='v2')
    _, new = rig.capture()
    assert old['state_hash'] != new['state_hash']


def test_gap_lifecycle_is_part_of_snapshot(rig):
    _, old = rig.capture()
    rig.store.open_gap(rig.source.source_id, 'message', 'extraction_failure', 'synthetic')
    _, new = rig.capture()
    assert old['state_hash'] != new['state_hash']


def test_same_fact_identity_with_changed_authority_is_detected(rig):
    _, old = rig.capture()
    # Fault injection into the persisted projection; no production promotion API.
    rig.store.conn.execute("UPDATE fact_candidates SET authority='owner_confirmed'")
    _, new = rig.capture()
    assert old['state_hash'] != new['state_hash']


def test_expiry_boundary_preserved_after_restart(tmp_path):
    path = tmp_path / 'information.sqlite'
    first = Rig(path)
    first.add()
    _, binding = first.capture()
    clock = first.time
    first.close()
    again = Rig(path, clock)
    try:
        _, current = again.capture()
        assert current['state_hash'] == binding['state_hash']
        clock[0] = int(binding['expires'] * 1000)
        with pytest.raises(DocumentSnapshotError, match='EXPIRED'):
            validate_binding(binding, again.row, clock[0])
    finally:
        again.close()


def test_revocation_fails_before_retrieval(rig):
    _, binding = rig.capture()
    rig.store.mark_source_error(rig.source.source_id, 'synthetic_revocation', permission_denied=True)
    with pytest.raises(BoundaryError, match='SOURCE_ACCESS_CHANGED'):
        rig.host.document_evidence_current(rig.row, binding)


def test_other_actor_cannot_replay_binding(rig):
    _, binding = rig.capture()
    with pytest.raises(BoundaryError, match='SCOPE_CHANGED'):
        rig.host.document_evidence_current(dict(rig.row, user='someone-else'), binding)


def test_an_unrelated_acl_change_keeps_original_global_guard(rig):
    other = replace(rig.source, source_id='source-b', remote_id='local-b', settings={'subject_id': 'project-b'})
    rig.store.register_source(other)
    # Legacy output keeps its original global fence; only newly captured v2
    # output may use scoped authorization. This does not reissue old bindings.
    _, binding = capture_document_snapshot(rig.store, rig.service, rig.project,
        principal=Principal('owner', 'tenant', ('demo-query',)), scope=request_scope(rig.row),
        project_code='IRAD-2099-001', expires=rig.row['expires'],
        expected_access_version=rig.store.access_version(),binding_schema='am-document-evidence-binding/1')
    rig.store.register_source(replace(other, readers=('user:new-user',)))
    with pytest.raises(BoundaryError, match='SOURCE_ACCESS_CHANGED'):
        rig.host.document_evidence_current(rig.row, binding)


def test_host_preserves_current_binding_and_rejects_later_changes(rig):
    _, bundle = rig.host.evidence_bundle(dict(rig.row), 'IRAD-2099-001')
    binding = bundle.data['document_binding']
    assert rig.host.document_evidence_current(rig.row, binding) is True
    rig.project['待办'] = '新的待办'
    with pytest.raises(BoundaryError, match='EVIDENCE_CHANGED|LEDGER_CHANGED'):
        rig.host.document_evidence_current(rig.row, binding)


def test_host_revalidates_identity_even_with_cached_scope(rig):
    _, bundle = rig.host.evidence_bundle(dict(rig.row), 'IRAD-2099-001')
    rig.host.am.query_settings['denied_users'].append('owner')
    with pytest.raises(BoundaryError, match='QUERY_IDENTITY_REVOKED'):
        rig.host.document_evidence_current(dict(rig.row, _query_grant=['*']), bundle.data['document_binding'])


@pytest.mark.parametrize('value', [None, {}, {'schema': 'wrong'}, {'schema': 'am-document-evidence-binding/1'}])
def test_legacy_or_incomplete_binding_cannot_be_silently_reissued(rig, value):
    with pytest.raises(DocumentSnapshotError):
        validate_binding(value, rig.row, rig.time[0])


def test_group_output_scope_does_not_expose_private_facts(rig):
    with pytest.raises(DocumentSnapshotError, match='LEDGER_EVIDENCE_UNAVAILABLE'):
        capture_document_snapshot(rig.store, rig.service, rig.project,
            principal=Principal('owner', 'tenant', ('demo-query',), 'unapproved-group'),
            scope=['tenant', 'app', 'owner', 'unapproved-group', ''],
            project_code='IRAD-2099-001', expires=rig.row['expires'],
            expected_access_version=rig.store.access_version())
    assert rig.service.query('project-a', Principal('owner', 'tenant', (), 'unapproved-group'))['facts'] == []


def test_wrong_transaction_connection_is_rejected(rig):
    other = Store(':memory:')
    try:
        with other.transaction() as connection:
            with pytest.raises(RuntimeError, match='active transaction'):
                rig.service.query('project-a', Principal('owner', 'tenant'), connection=connection)
    finally:
        other.close()


def test_normal_query_version_tracks_state_and_coverage_not_poll_time(rig):
    first = rig.host.query(dict(rig.row), 'project-a')
    rig.store.conn.execute('UPDATE sources SET last_checked_ms=?', (rig.time[0]+1,))
    second = rig.host.query(dict(rig.row), 'project-a')
    assert first['version'] == second['version']
    rig.store.open_gap('source-a', 'message', 'extraction_failure', 'synthetic')
    third = rig.host.query(dict(rig.row), 'project-a')
    assert first['version'] != third['version']


def test_normal_query_preserves_global_access_guard(rig):
    row = dict(rig.row)
    rig.host.query(row, 'project-a')
    rig.store.mark_source_error('source-a', 'synthetic', permission_denied=True)
    with pytest.raises(BoundaryError, match='SOURCE_ACCESS_CHANGED'):
        rig.host.query(row, 'project-a')


@pytest.mark.parametrize('purge', [False, True])
def test_expired_ledger_cannot_be_reconstructed_from_project_cache(rig, purge):
    rig.store.conn.execute("UPDATE observations SET raw_expires_at_ms=? WHERE source_id='demo-ledger'", (rig.time[0],))
    if purge:
        rig.store.purge_expired_raw()
    with pytest.raises(DocumentSnapshotError, match='LEDGER_EVIDENCE_UNAVAILABLE'):
        rig.capture()
    with pytest.raises(BoundaryError, match='LEDGER_EVIDENCE_UNAVAILABLE'):
        rig.host.evidence_bundle(dict(rig.row), 'IRAD-2099-001')


def test_ledger_expiry_shortens_entire_bundle_and_is_real_source_version(rig):
    rig.store.conn.execute("UPDATE observations SET raw_expires_at_ms=? WHERE source_id='demo-ledger'", (rig.time[0] + 1234,))
    _, bundle = rig.host.evidence_bundle(dict(rig.row), 'IRAD-2099-001')
    binding = bundle.data['document_binding']
    assert binding['expires'] == (rig.time[0] + 1234) / 1000
    assert binding['ledger_evidence']['version'].startswith('ledger:1:')


def test_no_ledger_source_does_not_create_unbound_ledger_claims(rig):
    rig.store.register_source(replace(rig.ledger_source, enabled=False))
    with pytest.raises(DocumentSnapshotError, match='LEDGER_EVIDENCE_UNAVAILABLE'):
        rig.capture()


def test_nested_fields_cannot_replace_verified_raw_ledger_fields(rig):
    rig.project['fields'] = {'项目进展': '已验收（合成的错误缓存）'}
    with pytest.raises(DocumentSnapshotError, match='LEDGER_CHANGED'):
        rig.capture()


def test_other_project_ledger_work_and_gaps_do_not_invalidate_current_project(rig):
    _, old = rig.capture()
    other = dict(rig.project, key='project-b', title='IRAD-2099-002 其他合成项目')
    rig.add_ledger(other)
    rig.store.open_gap('demo-ledger', 'project-b', 'extraction_failure', 'synthetic')
    _, new = rig.capture()
    assert old['state_hash'] == new['state_hash']
    rig.store.open_gap('demo-ledger', '', 'source_read', 'source-wide synthetic')
    _, changed = rig.capture()
    assert old['state_hash'] != changed['state_hash']


def test_current_guard_does_not_refresh_identity_or_read_sources(rig):
    _, bundle = rig.host.evidence_bundle(dict(rig.row), 'IRAD-2099-001')
    rig.host.am.query_projects = lambda row: (_ for _ in ()).throw(AssertionError('Remote refresh'))
    assert rig.host.document_evidence_current(rig.row, bundle.data['document_binding']) is True


def test_local_identity_lease_expiry_is_not_extended(rig):
    _, bundle = rig.host.evidence_bundle(dict(rig.row), 'IRAD-2099-001')
    rig.identity.entries['owner'] = time.monotonic() - 1
    before = dict(rig.identity.entries)
    with pytest.raises(BoundaryError, match='QUERY_IDENTITY_EXPIRED'):
        rig.host.document_evidence_current(rig.row, bundle.data['document_binding'])
    assert rig.identity.entries == before


def test_missing_local_snapshot_never_starts_a_remote_refresh(rig):
    _, bundle = rig.host.evidence_bundle(dict(rig.row), 'IRAD-2099-001')
    rig.host.am.snapshots.latest = None
    with pytest.raises(BoundaryError, match='LOCAL_SNAPSHOT_UNAVAILABLE'):
        rig.host.document_evidence_current(rig.row, bundle.data['document_binding'])


def test_expired_lease_can_only_refresh_at_explicit_interactive_boundary(rig):
    _, bundle = rig.host.evidence_bundle(dict(rig.row), 'IRAD-2099-001')
    rig.identity.entries['owner'] = time.monotonic() - 1
    calls = []
    def check(user):
        assert not rig.store.conn.in_transaction
        calls.append(user)
    rig.host.am.ledger.port.authorize_private_user = check
    with pytest.raises(BoundaryError, match='QUERY_IDENTITY_EXPIRED'):
        rig.host.document_evidence_current(rig.row, bundle.data['document_binding'])
    assert calls == []
    assert rig.host.authorize_document_evidence(dict(rig.row, _direct_return=True)) is True
    assert calls == ['owner'] and rig.identity.seconds == 600
    assert rig.host.document_evidence_current(rig.row, bundle.data['document_binding']) is True
    assert calls == ['owner']
