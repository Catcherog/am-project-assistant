"""Strict shape validation for owner-reviewed follow-up imports.

Missing business fields may remain candidates. Invalid nested types, extra
executable fields and unverifiable history never create a native action item.
"""
import math
import re

from fs_agent.config import BoundaryError


IDENTITY_KEYS = {'tenant', 'project', 'object', 'action', 'cycle'}
ITEM_KEYS = {'description', 'requirements', 'assignees', 'deadline', 'deadline_kind', 'deliverable_key'}
HISTORY_KEYS = {'state', 'occurrence_id', 'message_id', 'reason', 'channel', 'target',
                'recipient', 'producer', 'external_id', 'action', 'timestamp', 'sent_at', 'request_uuid'}
HISTORY_STATES = {'sent', 'sending', 'accepted', 'unknown', 'legacy_unknown', 'failed', 'confirmed_not_sent'}


def _text(value, *, maximum=4000, empty=False):
    return type(value) is str and len(value) <= maximum and (empty or bool(value.strip()))


def validate_identity(identity, tenant, *, complete=True, project=None):
    if type(identity) is not dict or set(identity) - IDENTITY_KEYS:
        raise BoundaryError('FOLLOWUP_IDENTITY_INVALID')
    if any(not _text(v, maximum=512) for v in identity.values()):
        raise BoundaryError('FOLLOWUP_IDENTITY_INVALID')
    if identity.get('tenant', tenant) != tenant or (project is not None and identity.get('project') != project):
        raise BoundaryError('FOLLOWUP_IDENTITY_INVALID')
    if complete and set(identity) != IDENTITY_KEYS:
        raise BoundaryError('FOLLOWUP_IDENTITY_UNRESOLVED')


def validate_candidate(value, tenant, *, complete=False):
    required = {'producer', 'external_id', 'identity', 'item', 'fact_ids', 'history', 'history_complete'}
    if type(value) is not dict or set(value) != required or type(value['history_complete']) is not bool:
        raise BoundaryError('FOLLOWUP_IMPORT_SCHEMA')
    if any(not _text(value[k], maximum=200) for k in ('producer', 'external_id')):
        raise BoundaryError('FOLLOWUP_IMPORT_IDENTITY')
    validate_identity(value['identity'], tenant, complete=complete)
    fields = value['item']
    if type(fields) is not dict or set(fields) - ITEM_KEYS:
        raise BoundaryError('FOLLOWUP_IMPORT_ITEM_SCHEMA')
    for key in ('description', 'requirements', 'deliverable_key'):
        if key in fields and not _text(fields[key], maximum=4000 if key != 'deliverable_key' else 512, empty=True):
            raise BoundaryError('FOLLOWUP_IMPORT_ITEM_SCHEMA')
    if 'deadline' in fields and fields['deadline'] is not None and not _text(fields['deadline'], maximum=100):
        raise BoundaryError('FOLLOWUP_IMPORT_ITEM_SCHEMA')
    deadline_kind=fields.get('deadline_kind', 'unspecified')
    if type(deadline_kind) is not str or deadline_kind not in {'unspecified', 'estimate', 'commitment', 'requirement', 'check'}:
        raise BoundaryError('FOLLOWUP_IMPORT_ITEM_SCHEMA')
    people = fields.get('assignees', [])
    if (type(people) is not list or len(people) > 100
            or any(type(p) is not str or not re.fullmatch(r'ou_[A-Za-z0-9_-]{1,200}', p) for p in people)
            or len(set(people)) != len(people)):
        raise BoundaryError('FOLLOWUP_IMPORT_ASSIGNEES_SCHEMA')
    fact_ids = value['fact_ids']
    if (type(fact_ids) is not list or len(fact_ids) > 100
            or any(not _text(x, maximum=256) for x in fact_ids) or len(set(fact_ids)) != len(fact_ids)):
        raise BoundaryError('FOLLOWUP_IMPORT_FACTS_SCHEMA')
    history = value['history']
    if type(history) is not list or len(history) > 500:
        raise BoundaryError('FOLLOWUP_HISTORY_INVALID')
    for entry in history:
        if type(entry) is not dict or not entry or set(entry) - HISTORY_KEYS:
            raise BoundaryError('FOLLOWUP_HISTORY_INVALID')
        if 'state' in entry and (type(entry['state']) is not str or entry['state'] not in HISTORY_STATES):
            raise BoundaryError('FOLLOWUP_HISTORY_INVALID')
        for key, data in entry.items():
            if key in ('timestamp', 'sent_at') and type(data) in (int, float) and math.isfinite(data):continue
            if not _text(data, maximum=1000):raise BoundaryError('FOLLOWUP_HISTORY_INVALID')
    if complete:
        if not _text(fields.get('description')):raise BoundaryError('FOLLOWUP_DESCRIPTION_UNRESOLVED')
        if not people:raise BoundaryError('FOLLOWUP_ASSIGNEES_UNRESOLVED')
        if not fact_ids:raise BoundaryError('FOLLOWUP_EVIDENCE_REQUIRED')
    return value
