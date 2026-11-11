"""Document-only policy projection; ordinary query/access fences stay unchanged.

Only the two documented scheduling fields are omitted. In particular overlap,
raw retention, analysis settings and unknown future settings remain semantic.
Membership lists are projected onto the requested project; their history is
separately fenced by durable Store epochs.
"""
from __future__ import annotations

from .models import Source


def effective_freshness_seconds(source: Source) -> int | float:
    # Keep the same fallback rule as Facts.query, including invalid overrides.
    value = source.settings.get('freshness_seconds', max(120, source.poll_seconds * 2))
    if type(value) not in (int, float) or value < 1:
        value = max(120, source.poll_seconds * 2)
    return value


def project_membership(source: Source, subject: str) -> dict:
    fixed = source.settings.get('subject_id')
    allowed = source.settings.get('subject_ids')
    return {'fixed': None if fixed is None else fixed == subject,
            'allowed': None if allowed is None else subject in allowed}


def membership_changes(old: Source, new: Source) -> tuple[bool, list[str]]:
    """Find changed project scopes, conservatively fencing unrestricted changes.

    A restriction being added/removed can affect subjects not yet in either
    explicit list, so that case advances the whole source epoch. Ordinary list
    changes advance only the affected projects (not every shared-ledger row).
    """
    old_fixed, new_fixed = old.settings.get('subject_id'), new.settings.get('subject_id')
    old_allowed, new_allowed = old.settings.get('subject_ids'), new.settings.get('subject_ids')
    source_wide = ((old_fixed is None) != (new_fixed is None) or
                   (old_allowed is None) != (new_allowed is None))
    subjects = {v for v in (old_fixed, new_fixed) if isinstance(v, str)}
    for allowed in (old_allowed, new_allowed):
        if allowed is None:
            continue
        if not isinstance(allowed, (list, tuple)) or any(not isinstance(v, str) for v in allowed):
            # Legacy configurations with non-list membership cannot be safely
            # enumerated. Preserve a source-wide fence for those writes.
            source_wide = True
        else:
            subjects.update(allowed)
    changed = sorted(s for s in subjects if project_membership(old, s) != project_membership(new, s))
    return source_wide, changed


def authorization_policy(source: Source, subject: str) -> dict:
    return {**{k: getattr(source, k) for k in (
        'source_id', 'kind', 'remote_id', 'tenant_id', 'owner_user_id',
        'enabled')},
        'readers': sorted(set(source.readers)),
        'output_chats': sorted(set(source.output_chats)),
        'read_identity': source.settings.get('read_identity'),
        'membership': project_membership(source, subject)}


def document_policy(source: Source, subject: str) -> dict:
    policy = source.to_dict()
    policy.pop('poll_seconds')
    policy.pop('reconcile_seconds')
    policy['readers'] = sorted(set(source.readers))
    policy['output_chats'] = sorted(set(source.output_chats))
    settings = policy['settings']
    settings.pop('subject_id', None)
    settings.pop('subject_ids', None)
    policy['project_membership'] = project_membership(source, subject)
    policy['effective_freshness_seconds'] = effective_freshness_seconds(source)
    return policy
