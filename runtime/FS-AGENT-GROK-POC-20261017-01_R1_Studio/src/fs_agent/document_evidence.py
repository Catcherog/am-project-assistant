"""Validate the existing information service's document binding and quotations.

This module has no database and performs no retrieval. The host supplies both
the authenticated bundle and its current-version verifier.
"""
from __future__ import annotations

import math
import re
from datetime import datetime, timezone
from urllib.parse import quote

from am_doc_engine.models import canonical, fingerprint
from .config import BoundaryError


BINDING_SCHEMA = 'am-document-evidence-binding/2'
LEGACY_BINDING_SCHEMA = 'am-document-evidence-binding/1'
BINDING_SCHEMAS = frozenset((LEGACY_BINDING_SCHEMA, BINDING_SCHEMA))
COMPONENT_NAMES = frozenset(('authorization','project','sources','objects','facts','quality','gaps','pending'))
CONTRACT = 'am-document-evidence/1'
QUALIFIERS = ('modality', 'authority', 'business_state', 'polarity', 'time',
              'time_scope', 'scope', 'condition', 'attribution', 'conflict_group', 'stale')


def validate_binding(row, binding, *, now, project_code=None, project_id=None):
    if not isinstance(binding, dict) or binding.get('schema') not in BINDING_SCHEMAS:
        raise BoundaryError('DOCUMENT_SHARED_EVIDENCE_REGENERATE_REQUIRED')
    expected = [row['tenant'], row['app'], row['user'], row['chat'], '']
    if binding.get('scope') != expected:
        raise BoundaryError('DOCUMENT_SHARED_EVIDENCE_SCOPE_CHANGED')
    if (not binding.get('project_id') or not binding.get('project_code')
            or project_id is not None and binding['project_id'] != project_id
            or project_code is not None and binding['project_code'] != project_code):
        raise BoundaryError('DOCUMENT_SHARED_EVIDENCE_PROJECT_CHANGED')
    expires = binding.get('expires')
    if type(expires) not in (int, float) or not math.isfinite(expires) or expires <= now:
        raise BoundaryError('DOCUMENT_SHARED_EVIDENCE_EXPIRED')
    if not isinstance(binding.get('state_hash'), str) or not binding['state_hash'] or 'access_version' not in binding:
        raise BoundaryError('DOCUMENT_SHARED_EVIDENCE_BINDING_INVALID')
    if binding['schema']==BINDING_SCHEMA:
        components=binding.get('component_hashes')
        if (not isinstance(binding.get('access_version'),str) or not binding['access_version']
                or not isinstance(components,dict) or set(components)!=COMPONENT_NAMES
                or any(not isinstance(v,str) or not re.fullmatch(r'[0-9a-f]{64}',v) for v in components.values())
                or binding['state_hash']!=fingerprint({'schema':BINDING_SCHEMA,'components':components})):
            raise BoundaryError('DOCUMENT_SHARED_EVIDENCE_BINDING_INVALID')
    return binding


def require_current(row, binding, verifier, *, now, project_code=None, project_id=None):
    validate_binding(row, binding, now=now, project_code=project_code, project_id=project_id)
    if not callable(verifier):
        raise BoundaryError('DOCUMENT_SHARED_EVIDENCE_VERIFIER_REQUIRED')
    if verifier(row, binding) is not True:
        raise BoundaryError('DOCUMENT_SHARED_EVIDENCE_CHANGED')
    return True


def shared_fragment(source, segment, *, project_id, fallback_observed_at):
    """Keep the original source identity and quote; never trust metadata on uploads."""
    if source.get('evidence_contract') != CONTRACT or source.get('provenance') != 'current_project':
        raise BoundaryError('DOCUMENT_SHARED_EVIDENCE_CONTRACT_INVALID')
    if segment.get('subject_id') != project_id:
        raise BoundaryError('DOCUMENT_FACT_PROJECT_DENIED')
    origin = segment.get('source')
    if not isinstance(origin, dict) or origin.get('raw_available') is not True:
        raise BoundaryError('DOCUMENT_FACT_RAW_EVIDENCE_UNAVAILABLE')
    if not all(isinstance(origin.get(k), str) and origin[k] for k in ('source_id', 'object_id', 'version')):
        raise BoundaryError('DOCUMENT_FACT_SOURCE_IDENTITY_INVALID')
    original = segment.get('original')
    if not isinstance(original, str) or not original.strip() or original != origin.get('quote'):
        raise BoundaryError('DOCUMENT_FACT_QUOTE_INVALID')
    uri = origin.get('url') or origin.get('uri')
    if not uri:
        uri = 'am://information/' + quote(origin['source_id'], safe='') + '/' + quote(origin['object_id'], safe='')
    # A confirmed source record is not an approved project decision. Business
    # confirmation must be explicit as well as supplied by a trusted authority.
    authority = segment.get('authority', 'unconfirmed')
    business_state = segment.get('business_state', 'unconfirmed')
    confirmed = (authority in ('source_confirmed', 'owner_confirmed')
                 and business_state in ('confirmed', 'approved', 'accepted', 'verified'))
    modality = segment.get('modality')
    state = 'confirmed' if confirmed else ('proposed' if modality == 'proposal' else 'reported')
    semantic = {'forecast': 'forecast', 'proposal': 'proposal', 'baseline': 'baseline',
                'plan': 'baseline', 'decision': 'decision', 'actual': 'actual'}.get(modality)
    qualifiers = {k: segment[k] for k in QUALIFIERS if k in segment}
    qualifiers.update(claim_id=segment['id'], subject_id=project_id,
                      source_object_id=origin['object_id'], source_authority=authority)
    canonical(qualifiers)  # Reject unserializable or non-finite metadata.
    observed = segment.get('observed_at')
    if type(observed) in (int, float):
        observed = datetime.fromtimestamp(observed, timezone.utc).isoformat()
    if observed is None and type(origin.get('received_at_ms')) in (int, float):
        observed = datetime.fromtimestamp(origin['received_at_ms'] / 1000, timezone.utc).isoformat()
    return {'evidence_source_id': origin['source_id'], 'revision': origin['version'],
            'uri': uri, 'evidence_locator': str(origin.get('locator') or origin['object_id']) + '#claim=' + segment['id'],
            'state': state, 'semantic': semantic, 'qualifiers': qualifiers,
            'observed_at': observed or fallback_observed_at,
            'read_state': origin.get('read_state', 'complete')}


def coverage_issues(source):
    """Return a visible, conservative limitation without inventing missing facts."""
    from am_doc_engine.models import Issue
    if source.get('evidence_contract') != CONTRACT:
        return ()
    coverage = source.get('coverage') or {}
    if (source.get('gaps') or source.get('read_status') != 'complete'
            or coverage.get('processing_pending') or coverage.get('needs_refresh_or_review')):
        # The wrapper forces draft_with_gaps. Keep draft saving available under
        # the original workflow; incomplete coverage is not business readiness.
        return (Issue('shared_evidence_incomplete', 'warning',
                      '项目资料仍有未读全、待处理或待核对内容；本稿仅使用已核验可读的原文，相关缺口尚未关闭。'),)
    return ()
