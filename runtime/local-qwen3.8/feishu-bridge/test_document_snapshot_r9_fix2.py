"""Real SQLite regressions for scoped document bindings, without remote calls."""
from copy import deepcopy
from dataclasses import replace
import json
import sqlite3
import subprocess
import sys

import pytest

from am_info_sync.document_snapshot import (
    SCHEMA, LEGACY_SCHEMA, DocumentSnapshotError, capture_document_snapshot,
    request_scope, validate_binding)
from am_info_sync.models import Observation, Page, Principal
from am_info_sync.runtime import InformationService
from am_info_sync.store import Store
from test_document_snapshot_r9 import Rig


@pytest.fixture
def rig(tmp_path):
    value = Rig(tmp_path / 'information.sqlite')
    value.add()
    try:
        yield value
    finally:
        value.close()


def capture(rig, *, schema=SCHEMA, expected=None, user='owner', expires=None):
    return capture_document_snapshot(rig.store, rig.service, rig.project,
        principal=Principal(user, 'tenant', ('demo-query',)),
        scope=request_scope({**rig.row, 'user': user}), project_code='IRAD-2099-001',
        expires=rig.row['expires'] if expires is None else expires,
        expected_access_version=rig.store.access_version() if expected is None else expected,
        binding_schema=schema)


def reopen(rig):
    path = rig.store.path
    rig.store.close()
    rig.store = Store(path, clock=lambda: rig.time[0])
    rig.service = InformationService(rig.store)


def unrelated(rig):
    source = replace(rig.source, source_id='source-b', remote_id='local-b',
                     settings={'subject_id': 'project-b'})
    rig.store.register_source(source)
    return source


def authorization(binding):
    return binding['component_hashes']['authorization']


def test_v2_defaults_and_capture_does_not_write(rig):
    traced = []
    rig.store.conn.set_trace_callback(traced.append)
    before = rig.store.conn.total_changes
    _, binding = capture(rig)
    assert binding['schema'] == SCHEMA
    validate_binding(binding, rig.row, rig.time[0])
    assert rig.store.conn.total_changes == before
    assert sum(sql.startswith('BEGIN') for sql in traced) == 1
    assert not any(sql.lstrip().split()[0].upper() in {'INSERT', 'UPDATE', 'DELETE', 'REPLACE'}
                   for sql in traced)
    assert '工程师' not in json.dumps(binding, ensure_ascii=False)


def test_unrelated_acl_changes_keep_current_document_valid(rig):
    other = unrelated(rig)
    _, old = capture(rig)
    rig.store.register_source(replace(other, readers=('another-user',)))
    _, new = capture(rig, expected=old['access_version'])
    assert old['access_version'] != new['access_version']
    assert old['state_hash'] == new['state_hash']
    assert authorization(old) == authorization(new)


def test_same_ledger_other_project_membership_changes_are_scoped(rig):
    _, old = capture(rig)
    for subjects in (['project-a'], ['project-a', 'project-c'], ['project-c', 'project-a', 'project-a']):
        rig.store.register_source(replace(rig.ledger_source,
            settings={**rig.ledger_source.settings, 'subject_ids': subjects}))
        _, current = capture(rig, expected=old['access_version'])
        assert current['state_hash'] == old['state_hash']
        assert authorization(current) == authorization(old)
    assert rig.store.access_version() != old['access_version']


def test_ledger_current_project_remove_then_restore_keeps_history(rig):
    _, old = capture(rig)
    rig.store.register_source(replace(rig.ledger_source,
        settings={**rig.ledger_source.settings, 'subject_ids': ['project-b']}))
    with pytest.raises(DocumentSnapshotError, match='DOCUMENT_LEDGER_EVIDENCE_UNAVAILABLE'):
        capture(rig)
    rig.store.register_source(rig.ledger_source)
    _, restored = capture(rig)
    assert authorization(restored) != authorization(old)
    assert restored['state_hash'] != old['state_hash']
    assert restored['component_hashes']['sources'] == old['component_hashes']['sources']
    reopen(rig)
    _, reopened = capture(rig)
    assert reopened['state_hash'] == restored['state_hash']


def test_reader_revocation_and_restore_from_another_process(rig):
    for source in (rig.source, rig.ledger_source):
        rig.store.register_source(replace(source, readers=('user:viewer',)))
    _, old = capture(rig, user='viewer')
    # A distinct interpreter/connection performs the ACL cycle. The original
    # process must observe the monotonic history even though current ACL agrees.
    program = '''
from dataclasses import replace
from am_info_sync.store import Store
import sys
store = Store(sys.argv[1])
source = store.get_source('source-a')
store.register_source(replace(source, readers=()))
store.register_source(source)
store.close()
'''
    subprocess.run([sys.executable, '-c', program, rig.store.path], check=True,
                   capture_output=True, text=True, timeout=20)
    _, restored = capture(rig, user='viewer', expected=old['access_version'])
    assert authorization(restored) != authorization(old)
    assert restored['component_hashes']['sources'] == old['component_hashes']['sources']
    assert restored['component_hashes']['facts'] == old['component_hashes']['facts']


def test_remote_permission_denial_and_successful_reacquisition_keep_history(rig):
    rig.store.conn.execute("UPDATE sources SET availability='ok',last_checked_ms=? WHERE source_id='source-a'", (rig.time[0],))
    _, old = capture(rig)
    rig.store.mark_source_error('source-a', 'permission_denied', permission_denied=True)
    run = rig.store.begin_sync('source-a')
    rig.store.save_page(run['run_id'], run['lease_token'], None, Page(()))
    _, restored = capture(rig)
    assert authorization(restored) != authorization(old)
    assert restored['component_hashes']['sources'] == old['component_hashes']['sources']
    assert restored['component_hashes']['gaps'] == old['component_hashes']['gaps']


def test_reader_and_output_chat_reorder_or_duplicates_are_semantically_stable(rig):
    source = replace(rig.source, readers=('reader-a', 'reader-b'), output_chats=('chat-a', 'chat-b'))
    rig.store.register_source(source)
    _, old = capture(rig)
    rig.store.register_source(replace(source, readers=('reader-b', 'reader-a', 'reader-a'),
                                      output_chats=('chat-b', 'chat-b', 'chat-a')))
    _, new = capture(rig)
    assert old['access_version'] != new['access_version']  # Ordinary query fence retained.
    assert old['state_hash'] == new['state_hash']
    assert authorization(old) == authorization(new)


def test_explicit_freshness_makes_poll_and_reconcile_operational(rig):
    source = replace(rig.ledger_source,
                     settings={**rig.ledger_source.settings, 'freshness_seconds': 600})
    rig.store.register_source(source)
    rig.time[0] += 500_000
    info, old = capture(rig)
    coverage = next(s for s in info['coverage']['sources'] if s['source_id'] == 'demo-ledger')
    assert coverage['stale'] is False
    assert info['coverage']['needs_refresh_or_review'] is False
    assert info['incomplete_reasons'] == []
    assert all(not f['stale'] for f in info['facts'])
    rig.store.register_source(replace(source, poll_seconds=500, reconcile_seconds=7200))
    current_info, new = capture(rig)
    assert old['state_hash'] == new['state_hash']
    assert current_info['coverage'] == info['coverage']
    assert current_info['incomplete_reasons'] == info['incomplete_reasons']


def test_default_freshness_change_is_semantic_even_before_becoming_stale(rig):
    _, old = capture(rig)
    rig.store.register_source(replace(rig.source, poll_seconds=240))
    _, new = capture(rig)
    assert authorization(old) == authorization(new)
    assert old['state_hash'] != new['state_hash']


def test_poll_change_under_same_default_freshness_floor_is_operational(rig):
    rig.store.register_source(replace(rig.source, poll_seconds=30))
    _, old = capture(rig)
    rig.store.register_source(replace(rig.source, poll_seconds=50))
    _, new = capture(rig)
    assert old['state_hash'] == new['state_hash']


def test_crossing_freshness_boundary_changes_returned_quality_and_binding(rig):
    source = replace(rig.ledger_source,
                     settings={**rig.ledger_source.settings, 'freshness_seconds': 600})
    rig.store.register_source(source)
    info, old = capture(rig)
    assert info['coverage']['needs_refresh_or_review'] is False
    rig.time[0] += 600_001
    new_info, new = capture(rig)
    assert new_info['coverage']['needs_refresh_or_review'] is True
    assert any(f['stale'] for f in new_info['facts'])
    assert old['state_hash'] != new['state_hash']


@pytest.mark.parametrize('change', ['retention', 'overlap', 'unknown_setting', 'analysis_mapping'])
def test_only_explicit_scheduling_metadata_is_excluded(rig, change):
    _, old = capture(rig)
    source = rig.source
    if change == 'retention':
        source = replace(source, raw_retention_seconds=172800)
    elif change == 'overlap':
        source = replace(source, overlap_seconds=600)
    elif change == 'unknown_setting':
        source = replace(source, settings={**source.settings, 'future_policy': {'mode': 'strict'}})
    else:
        source = replace(source, settings={**source.settings, 'mapping_version': 'new-analysis-v2'})
    rig.store.register_source(source)
    _, new = capture(rig)
    assert old['state_hash'] != new['state_hash']


@pytest.mark.parametrize('change', ['raw_revision', 'unknown_message', 'gap', 'authority', 'structured_mapping'])
def test_real_evidence_and_quality_changes_invalidate(rig, change):
    _, old = capture(rig)
    if change == 'raw_revision':
        rig.time[0] += 1
        rig.add('材料尚未交付', version='v2')
    elif change == 'unknown_message':
        rig.store.record(Observation('source-a', 'not-yet-mapped', 'v1', rig.time[0], text='新消息的项目归属待解析'))
    elif change == 'gap':
        rig.store.mark_source_error('source-a', 'upstream_read_failed')
    elif change == 'authority':
        rig.store.conn.execute("UPDATE fact_candidates SET authority='owner_confirmed' WHERE source_id='source-a'")
    else:
        settings = deepcopy(rig.ledger_source.settings)
        settings['fact_mappings']['ledger_record']['quote_path'] = ['new_quote']
        rig.store.register_source(replace(rig.ledger_source, settings=settings))
    _, new = capture(rig)
    assert old['state_hash'] != new['state_hash']


def test_retry_lease_metadata_is_stable_but_dead_state_is_not(rig):
    row = rig.store.conn.execute("SELECT job_id FROM jobs WHERE kind='extract' AND state='pending' LIMIT 1").fetchone()
    assert row is not None
    _, old = capture(rig)
    rig.store.conn.execute("""UPDATE jobs SET state='leased',attempts=3,available_at_ms=999999999,
        lease_until_ms=999999999,lease_token='local-test',last_error='retry_later' WHERE job_id=?""", (row[0],))
    _, leased = capture(rig)
    assert old['state_hash'] == leased['state_hash']
    rig.store.conn.execute("UPDATE jobs SET state='dead' WHERE job_id=?", (row[0],))
    _, dead = capture(rig)
    assert dead['state_hash'] != old['state_hash']


def test_unparsed_current_raw_deadline_limits_document_and_is_never_extended(rig):
    short = replace(rig.source, source_id='short-source', remote_id='short-local', raw_retention_seconds=60)
    rig.store.register_source(short)
    observation = Observation('short-source', 'unknown-message', 'v1', rig.time[0], text='尚未解析')
    rig.store.record(observation)
    _, old = capture(rig)
    assert old['expires'] == (rig.time[0] + 60_000) / 1000
    rig.store.register_source(replace(short, raw_retention_seconds=3600))
    rig.time[0] += 1000
    rig.store.record(observation)
    _, current = capture(rig, expires=old['expires'])
    assert current['expires'] == old['expires']
    rig.time[0] += 59_000
    with pytest.raises(DocumentSnapshotError, match='DOCUMENT_EVIDENCE_EXPIRED'):
        capture(rig, expires=old['expires'])


def test_ledger_payload_must_agree_with_resolved_project(rig):
    payload = json.loads(rig.store.conn.execute("SELECT payload_json FROM observations WHERE source_id='demo-ledger'").fetchone()[0])
    payload['record']['fields']['项目进展'] = '与已解析项目不同'
    rig.store.conn.execute("UPDATE observations SET payload_json=? WHERE source_id='demo-ledger'", (json.dumps(payload),))
    with pytest.raises(DocumentSnapshotError, match='DOCUMENT_LEDGER_CHANGED_DURING_READ'):
        capture(rig)


def test_legacy_binding_keeps_global_fence_and_full_source_policy(rig):
    other = unrelated(rig)
    _, legacy = capture(rig, schema=LEGACY_SCHEMA)
    assert legacy['schema'] == LEGACY_SCHEMA
    assert 'component_hashes' not in legacy
    validate_binding(legacy, rig.row, rig.time[0])
    rig.store.register_source(replace(rig.source, reconcile_seconds=7200))
    _, changed = capture(rig, schema=LEGACY_SCHEMA)
    assert changed['state_hash'] != legacy['state_hash']
    rig.store.register_source(replace(other, readers=('another-user',)))
    with pytest.raises(DocumentSnapshotError, match='SOURCE_ACCESS_CHANGED'):
        capture(rig, schema=LEGACY_SCHEMA, expected=legacy['access_version'])


def test_unknown_schema_and_malformed_authorization_are_rejected(rig):
    _, binding = capture(rig)
    bad = deepcopy(binding)
    bad['schema'] = 'am-document-evidence-binding/999'
    with pytest.raises(DocumentSnapshotError, match='DOCUMENT_EVIDENCE_BINDING_REQUIRED'):
        validate_binding(bad, rig.row, rig.time[0])
    with pytest.raises(DocumentSnapshotError, match='DOCUMENT_EVIDENCE_BINDING_REQUIRED'):
        capture(rig, schema=bad['schema'])
    bad = deepcopy(binding)
    bad['component_hashes'].pop('authorization')
    with pytest.raises(DocumentSnapshotError, match='DOCUMENT_EVIDENCE_BINDING_INVALID'):
        validate_binding(bad, rig.row, rig.time[0])
    bad = deepcopy(binding)
    bad['component_hashes']['facts'] = 'f' * 64
    with pytest.raises(DocumentSnapshotError, match='DOCUMENT_EVIDENCE_BINDING_INVALID'):
        validate_binding(bad, rig.row, rig.time[0])


def test_legacy_database_migration_and_reopen_keep_global_history(rig):
    _, legacy = capture(rig, schema=LEGACY_SCHEMA)
    # Exact pre-v2 database shape: the original schema/global epoch remains,
    # while none of the document epoch tables exist.
    with rig.store.transaction() as db:
        db.execute('DROP TABLE document_access_meta')
        db.execute('DROP TABLE document_source_access_epochs')
        db.execute('DROP TABLE document_project_access_epochs')
    reopen(rig)
    _, migrated_legacy = capture(rig, schema=LEGACY_SCHEMA, expected=legacy['access_version'])
    assert migrated_legacy['state_hash'] == legacy['state_hash']
    assert migrated_legacy['access_version'] == legacy['access_version']
    _, migrated = capture(rig)
    reopen(rig)
    _, reopened = capture(rig)
    assert migrated['state_hash'] == reopened['state_hash']


def legacy_acl_cycle(path):
    # Simulate the original writer: config+global epoch, no v2 metadata.
    with sqlite3.connect(path, isolation_level=None) as db:
        db.execute('BEGIN IMMEDIATE')
        original = db.execute("SELECT config_json FROM sources WHERE source_id='source-a'").fetchone()[0]
        revoked = json.loads(original)
        revoked['readers'] = ['legacy-other-user']
        db.execute("UPDATE sources SET config_json=? WHERE source_id='source-a'", (json.dumps(revoked),))
        db.execute('UPDATE source_access_version SET revision=revision+1 WHERE singleton=1')
        db.execute("UPDATE sources SET config_json=? WHERE source_id='source-a'", (original,))
        db.execute('UPDATE source_access_version SET revision=revision+1 WHERE singleton=1')
        db.execute('COMMIT')


@pytest.mark.parametrize('repair', ['capture_only', 'reopen', 'known_unrelated_writer'])
def test_legacy_writer_history_cannot_be_laundered(rig, repair):
    other = unrelated(rig)
    _, old = capture(rig)
    legacy_acl_cycle(rig.store.path)
    if repair == 'reopen':
        reopen(rig)
    elif repair == 'known_unrelated_writer':
        rig.store.register_source(replace(other, readers=('another-user',)))
    changes = rig.store.conn.total_changes
    _, current = capture(rig)
    assert rig.store.conn.total_changes == changes
    assert authorization(current) != authorization(old)
    assert current['component_hashes']['sources'] == old['component_hashes']['sources']
    _, repeated = capture(rig)
    assert repeated['state_hash'] == current['state_hash']
    if repair != 'capture_only':
        assert rig.store.conn.execute('SELECT fallback_epoch FROM document_access_meta').fetchone()[0] == 1


def test_database_namespace_replacement_invalidates_even_with_same_data(rig):
    _, old = capture(rig)
    # Replacement namespace models restore/reinitialization of the store's
    # access history while business records and source policy remain identical.
    rig.store.conn.execute("UPDATE source_access_version SET namespace='replacement-database',revision=0")
    _, current = capture(rig)
    assert authorization(old) != authorization(current)
    assert old['state_hash'] != current['state_hash']


@pytest.mark.parametrize('missing', ['document_access_meta', 'document_source_access_epochs',
                                    'document_project_access_epochs'])
def test_partial_epoch_table_recreation_rotates_document_namespace(rig, missing):
    _, old = capture(rig)
    rig.store.conn.execute('DROP TABLE ' + missing)
    reopen(rig)
    _, recreated = capture(rig)
    assert authorization(recreated) != authorization(old)
    assert recreated['component_hashes']['sources'] == old['component_hashes']['sources']
    reopen(rig)
    _, stable = capture(rig)
    assert stable['state_hash'] == recreated['state_hash']
