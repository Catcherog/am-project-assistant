"""Read-only document dependencies over the existing information database.

No second fact store, remote call, permission grant or business-state promotion.
The host resolves identity/project scope before entering this local transaction.
Unknown subject membership in a mapped message source is deliberately covered
at source granularity; an unanalysed new message can invalidate the old draft.
Legacy bindings keep the global source-access fence. Version 2 uses durable
source/project revocation history and a conservative legacy-writer fallback.
"""
from __future__ import annotations

import math
import re
import json
from typing import Any

from .models import Principal, Source, digest
from .policy import can_read
from .document_policy import (authorization_policy, document_policy,
                              effective_freshness_seconds)


LEGACY_SCHEMA = 'am-document-evidence-binding/1'
SCHEMA = 'am-document-evidence-binding/2'
COMPONENT_NAMES = frozenset({'authorization', 'project', 'sources', 'objects',
                             'facts', 'quality', 'gaps', 'pending'})


class DocumentSnapshotError(ValueError):
    """A stable, body-free error code for the original host BoundaryError."""


def request_scope(row: dict[str, Any]) -> list[str]:
    values = [row.get(k) for k in ('tenant', 'app', 'user', 'chat')]
    if any(not isinstance(v, str) or not v for v in values):
        raise DocumentSnapshotError('DOCUMENT_EVIDENCE_SCOPE_INVALID')
    return values + ['']


def _deadline(value: Any) -> float:
    if type(value) not in (int, float) or not math.isfinite(value) or value <= 0:
        raise DocumentSnapshotError('DOCUMENT_EVIDENCE_EXPIRY_INVALID')
    return float(value)


def validate_binding(binding: Any, row: dict[str, Any], now_ms: int) -> None:
    if not isinstance(binding, dict) or binding.get('schema') not in (LEGACY_SCHEMA, SCHEMA):
        raise DocumentSnapshotError('DOCUMENT_EVIDENCE_BINDING_REQUIRED')
    if binding.get('scope') != request_scope(row):
        raise DocumentSnapshotError('DOCUMENT_EVIDENCE_SCOPE_CHANGED')
    if not all(isinstance(binding.get(k), str) and binding[k]
               for k in ('project_id', 'project_code', 'access_version')):
        raise DocumentSnapshotError('DOCUMENT_EVIDENCE_BINDING_INVALID')
    if not isinstance(binding.get('state_hash'), str) or not re.fullmatch(
            r'[0-9a-f]{64}', binding['state_hash']):
        raise DocumentSnapshotError('DOCUMENT_EVIDENCE_BINDING_INVALID')
    if binding['schema'] == SCHEMA:
        components = binding.get('component_hashes')
        if (not isinstance(components, dict) or set(components) != COMPONENT_NAMES
                or any(not isinstance(v, str) or not re.fullmatch(r'[0-9a-f]{64}', v)
                       for v in components.values())):
            raise DocumentSnapshotError('DOCUMENT_EVIDENCE_BINDING_INVALID')
        if binding['state_hash'] != digest({'schema': SCHEMA, 'components': components}):
            raise DocumentSnapshotError('DOCUMENT_EVIDENCE_BINDING_INVALID')
    if min(_deadline(binding.get('expires')), _deadline(row.get('expires'))) * 1000 <= now_ms:
        raise DocumentSnapshotError('DOCUMENT_EVIDENCE_EXPIRED')


def _mapped(source: Source, subject: str) -> bool:
    fixed = source.settings.get('subject_id')
    allowed = source.settings.get('subject_ids')
    if fixed is not None and fixed != subject:
        return False
    if allowed is not None and subject not in allowed:
        return False
    return fixed is not None or allowed is not None


def _project_state(project: dict[str, Any]) -> dict[str, Any]:
    # Poll timestamps and snapshot-cache IDs are not business changes. Identity,
    # source revision and actual field values are, and remain in the binding.
    return {k: project.get(k) for k in (
        'key', 'title', 'business_id', 'identity_status', 'source_revision',
        'content_fingerprint', 'fields', '关键日期', '待办', '项目进展', '人员与职责')}


def projection_version(info: dict[str, Any]) -> str:
    """Version changes in facts, authority, conflicts or coverage, not poll time."""
    coverage = info.get('coverage', {})
    return digest({
        'facts': sorted(info.get('facts', []), key=lambda f: f['fact_id']),
        'effective': info.get('effective', []), 'conflicts': info.get('conflicts', []),
        'pending_confirmation': sorted(info.get('pending_confirmation', [])),
        'incomplete_reasons': info.get('incomplete_reasons', []),
        'coverage': {
            'sources': sorted(({k: s.get(k) for k in
                ('source_id', 'stale', 'unprocessed_jobs', 'gap_kinds')}
                for s in coverage.get('sources', [])), key=lambda s: s['source_id']),
            **{k: coverage.get(k) for k in ('scope', 'processing_pending',
                'needs_refresh_or_review', 'global_completeness_claim')}}})


def capture_document_snapshot(store, service, project: dict[str, Any], *,
                              principal: Principal, scope: list[str],
                              project_code: str, expires: float,
                              expected_access_version: str | None = None,
                              binding_schema: str = SCHEMA) -> tuple[dict, dict]:
    """Return information plus a body-free binding from one local SQLite view.

    This is read-only despite using Store.transaction's existing BEGIN
    IMMEDIATE fence. It never holds a database lock across identity, model or
    platform calls. Callers must not already hold an Information transaction.
    """
    if binding_schema not in (LEGACY_SCHEMA, SCHEMA):
        raise DocumentSnapshotError('DOCUMENT_EVIDENCE_BINDING_REQUIRED')
    legacy = binding_schema == LEGACY_SCHEMA
    subject = project.get('key')
    if not isinstance(subject, str) or not subject or not project_code:
        raise DocumentSnapshotError('DOCUMENT_PROJECT_AMBIGUOUS')
    if len(scope) != 5 or scope[0] != principal.tenant_id or scope[2] != principal.user_id:
        raise DocumentSnapshotError('DOCUMENT_EVIDENCE_SCOPE_INVALID')
    expiry = _deadline(expires)
    with store.transaction() as db:
        now = store.clock()
        if expiry * 1000 <= now:
            raise DocumentSnapshotError('DOCUMENT_EVIDENCE_EXPIRED')
        observed_access_version = store.access_version()
        if legacy and observed_access_version != expected_access_version:
            raise DocumentSnapshotError('SOURCE_ACCESS_CHANGED')
        info = service.query(subject, principal, connection=db)
        fact_sources = {f['evidence']['source_id'] for f in info['facts']}
        sources = []
        objects = []
        dependencies = []
        selected = {}
        selected_status = {}
        ledger_evidence = None
        for row in db.execute('SELECT * FROM sources ORDER BY source_id').fetchall():
            source = Source.from_dict(json.loads(row['config_json']))
            # Denied/unreadable sources do not leak through coverage counts or
            # diagnostics. Durable epochs also catch revocation then restoration.
            if not can_read(source, principal) or row['availability'] == 'denied':
                continue
            if not (_mapped(source, subject) or source.source_id in fact_sources):
                continue
            selected[source.source_id] = source
            selected_status[source.source_id] = row
            sources.append({'source_id': source.source_id,
                            'policy_hash': digest(source.to_dict() if legacy else
                                                  document_policy(source, subject)),
                            'availability': row['availability']})
            records = db.execute('''SELECT o.object_id,o.current_version,o.state,
                r.body_hash,r.deleted,r.completeness,r.purged,r.raw_expires_at_ms,
                r.payload_json FROM objects o JOIN observations r
                ON r.source_id=o.source_id AND r.object_id=o.object_id
                AND r.version=o.current_version WHERE o.source_id=?
                ORDER BY o.object_id''', (source.source_id,)).fetchall()
            for record in records:
                # The fixed ledger has authoritative per-project object IDs.
                # Message/doc sources can have unresolved project membership;
                # excluding those objects would miss newly arrived evidence.
                if source.kind == 'ledger_snapshot' and record['object_id'] != subject:
                    continue
                if source.source_id == 'demo-ledger' and record['object_id'] == subject:
                    if (source.kind != 'ledger_snapshot' or record['state'] != 'active'
                            or record['deleted'] or record['completeness'] != 'complete'
                            or record['purged'] or record['raw_expires_at_ms'] <= now):
                        raise DocumentSnapshotError('DOCUMENT_LEDGER_EVIDENCE_UNAVAILABLE')
                    payload = json.loads(record['payload_json'])
                    ledger_record = payload.get('record', {})
                    if ledger_record.get('title') != project.get('title') or any(
                            ledger_record.get('fields', {}).get(k, '') != project.get(k, '')
                            for k in ('关键日期', '待办', '项目进展', '人员与职责')):
                        raise DocumentSnapshotError('DOCUMENT_LEDGER_CHANGED_DURING_READ')
                    if project.get('fields') and project['fields'] != ledger_record.get('fields'):
                        raise DocumentSnapshotError('DOCUMENT_LEDGER_CHANGED_DURING_READ')
                    if str(payload.get('revision_id')) != str(project.get('source_revision')):
                        raise DocumentSnapshotError('DOCUMENT_LEDGER_CHANGED_DURING_READ')
                    if ledger_evidence is not None:
                        raise DocumentSnapshotError('DOCUMENT_LEDGER_EVIDENCE_AMBIGUOUS')
                    expiry = min(expiry, record['raw_expires_at_ms'] / 1000)
                    ledger_evidence = {'source_id': source.source_id, 'object_id': subject,
                        'version': record['current_version'], 'raw_available': True,
                        'raw_expires_at_ms': record['raw_expires_at_ms']}
                item = {'source_id': source.source_id, 'object_id': record['object_id'],
                        'version': record['current_version'], 'state': record['state'],
                        'body_hash': record['body_hash'], 'deleted': bool(record['deleted']),
                        'completeness': record['completeness'],
                        'raw_available': not record['purged'] and record['raw_expires_at_ms'] > now,
                        'raw_expires_at_ms': record['raw_expires_at_ms']}
                objects.append(item)
                if not legacy and item['raw_available']:
                    # Unanalysed current objects are dependencies as well. Their
                    # remaining retention may shorten, never renew, the draft.
                    expiry = min(expiry, item['raw_expires_at_ms'] / 1000)
                dependencies.append({k: item[k] for k in
                                     ('source_id', 'object_id', 'version', 'raw_expires_at_ms')})
        # Project fields have their own raw source, separate from derived facts.
        # Never rebuild expired or unavailable ledger text from a project cache.
        if ledger_evidence is None:
            raise DocumentSnapshotError('DOCUMENT_LEDGER_EVIDENCE_UNAVAILABLE')
        # Stable gap and pending-job sets catch work not yet represented by a
        # fact, not just changes to the list of already extracted fact IDs.
        gaps = [dict(r) for r in db.execute('''SELECT source_id,object_id,kind
            FROM gaps WHERE state='open' ORDER BY source_id,object_id,kind''')
                if r['source_id'] in selected and
                (selected[r['source_id']].kind != 'ledger_snapshot' or
                 not r['object_id'] or r['object_id'] == subject)]
        pending = []
        for row in db.execute('''SELECT job_id,kind,state,payload_json FROM jobs
                WHERE kind IN ('extract','event','review_message')
                AND state NOT IN ('done','cancelled') ORDER BY job_id'''):
            payload = json.loads(row['payload_json'])
            source = selected.get(payload.get('source_id'))
            if source is not None and (source.kind != 'ledger_snapshot' or
                    not payload.get('object_id') or payload['object_id'] == subject):
                pending.append({'job_id': row['job_id'], 'kind': row['kind'],
                                'state': 'dead' if row['state'] == 'dead' else 'pending',
                                'payload_hash': digest(payload)})
        facts = []
        for fact in sorted(info['facts'], key=lambda f: f['fact_id']):
            evidence = fact['evidence']
            if evidence.get('raw_available'):
                limit = evidence.get('raw_expires_at_ms')
                if type(limit) is not int or limit <= now:
                    raise DocumentSnapshotError('DOCUMENT_EVIDENCE_EXPIRY_UNVERIFIED')
                expiry = min(expiry, limit / 1000)
            facts.append({'fact_id': fact['fact_id'], 'hash': digest(fact)})
        coverage = info.get('coverage', {})
        if not legacy:
            # Ordinary coverage currently derives freshness directly from the
            # scheduler, while facts already honor effective freshness. Make
            # this returned document view agree with the facts and its binding;
            # preserve the legacy/ordinary query behavior outside this view.
            for item in coverage.get('sources', []):
                source = selected[item['source_id']]
                status = selected_status[source.source_id]
                checked = status['last_checked_ms']
                item['stale'] = (source.kind != 'manual' and (
                    status['availability'] != 'ok' or checked is None or
                    now - checked > effective_freshness_seconds(source) * 1000))
            coverage['needs_refresh_or_review'] = not coverage.get('sources') or any(
                item['stale'] or item['gap_kinds'] for item in coverage.get('sources', []))
        quality = {
            'sources': sorted(({'source_id': s['source_id'], 'stale': s.get('stale'),
                                'unprocessed_jobs': s.get('unprocessed_jobs'),
                                'gap_kinds': s.get('gap_kinds', [])}
                               for s in coverage.get('sources', [])), key=lambda s: s['source_id']),
            'processing_pending': coverage.get('processing_pending'),
            'needs_refresh_or_review': coverage.get('needs_refresh_or_review'),
            'incomplete_reasons': info.get('incomplete_reasons', []),
            'pending_confirmation': sorted(info.get('pending_confirmation', [])),
            'conflicts': info.get('conflicts', []), 'effective': info.get('effective', [])}
        state = {'project': _project_state(project), 'sources': sources,
                 'objects': objects, 'facts': facts, 'quality': quality,
                 'gaps': gaps, 'pending': pending}
        components = None
        if not legacy:
            authorization = {
                'history': store.document_access_state(db, list(selected), subject),
                'policies': [authorization_policy(selected[sid], subject) for sid in sorted(selected)]}
            # Component hashes expose no raw content and allow the host to
            # distinguish authorization invalidation from evidence changes.
            components = {'authorization': digest(authorization),
                          **{name: digest(value) for name, value in state.items()}}
            # The database namespace, relevant revocation epochs and all
            # semantic components participate; the observed global epoch does
            # not. V1 retains its exact original state/hash algorithm above.
            state = {'schema': SCHEMA, 'components': components}
        binding = {'schema': binding_schema, 'project_id': subject, 'project_code': project_code,
                   'scope': list(scope), 'expires': expiry,
                   'access_version': observed_access_version,
                   'state_hash': digest(state), 'dependencies': dependencies,
                   'ledger_evidence': ledger_evidence}
        if components is not None:
            binding['component_hashes'] = components
        return info, binding
