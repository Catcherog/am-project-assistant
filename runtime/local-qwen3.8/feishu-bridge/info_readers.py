"""Read-only adapters bound to AM's existing SDK and pinned business reader.

No credentials, model, event listener, sending or business write entry points.
"""
from __future__ import annotations

import json
import re
from typing import Any

from am_info_sync.adapters.http import (AuthenticationExpired, InvalidResponse,
    RemoteAPIError, _AUTH_CODES, _PERMISSION_CODES, _RATE_CODES, remote_id)
from am_info_sync.models import (Observation, Page, PermissionDenied, RetryableError,
    ScopeViolation, Source, canonical, digest)


_MEEGLE_SCOPE_FAILURES = frozenset({
    'BUSINESS_SCOPE_DENIED', 'BUSINESS_IDENTITY_CHANGED',
    'BUSINESS_TARGET_CHANGED', 'BUSINESS_PARENT_CHANGED',
    'BUSINESS_PROJECT_IDENTITY_CHANGED', 'BUSINESS_PROJECT_CODE_MISMATCH',
    'BUSINESS_PROJECT_CODE_UNVERIFIED', 'BUSINESS_PROJECT_RELATION_MISSING',
    'BUSINESS_PROJECT_RELATION_UNVERIFIED', 'BUSINESS_NODE_UNVERIFIED',
    'BUSINESS_DOCUMENT_KIND_MISMATCH', 'BUSINESS_DOCUMENT_IDENTITY_UNVERIFIED',
    'BUSINESS_TARGET_BINDING_CHANGED',
    # A successfully read configuration/schema/value that cannot prove the
    # registered binding is not a still-readable stale source. These exact
    # validator failures require correction and successful revalidation.
    'BUSINESS_TARGET_CONFIG_INVALID', 'BUSINESS_TARGET_CONFIG_INCOMPLETE',
    'BUSINESS_TARGET_FIELD_CONFLICT', 'BUSINESS_FIELD_METADATA_AMBIGUOUS',
    'BUSINESS_FIELD_METADATA_MISSING', 'BUSINESS_FIELD_TYPE_UNSUPPORTED',
    'BUSINESS_FIELD_CANDIDATES_UNVERIFIED', 'BUSINESS_TARGET_FIELD_MISSING',
    'BUSINESS_FIELD_VALUE_UNVERIFIED', 'BUSINESS_FIELD_CANDIDATE_MISMATCH',
})
# Transport failures, response/read budgets and metadata acquisition failures
# (BUSINESS_METADATA_UNAVAILABLE/LIMIT) are deliberately absent. An exception
# class/prefix alone cannot prove a revocation; unknown failures still retry.


class ExistingSDKReader:
    """Use the already constructed original bot client and its token cache."""
    def __init__(self, client, *, app_id, tenant_id):
        self.client, self.app_id, self.tenant_id = client, app_id, tenant_id

    def get(self, source: Source, path: str, params=None):
        if not source.enabled or source.tenant_id != self.tenant_id:
            raise ScopeViolation('reader_identity_mismatch')
        if source.settings.get('read_identity', 'original_bot') != 'original_bot':
            raise ScopeViolation('user_identity_reader_not_bound')
        # Bind each route to the registered object, including single-message
        # lookups; MessageAdapter independently verifies returned chat identity.
        rid = remote_id(source.remote_id)
        valid = (
            source.kind in ('im_chat', 'im_thread') and
            (path == '/open-apis/im/v1/messages' or
             re.fullmatch(r'/open-apis/im/v1/messages/[A-Za-z0-9_-]{1,256}', path))
            or source.kind == 'docx' and path in
            (f'/open-apis/docx/v1/documents/{rid}', f'/open-apis/docx/v1/documents/{rid}/blocks')
            or source.kind == 'task_snapshot' and path == f'/open-apis/task/v2/tasks/{rid}'
        )
        if not valid:
            raise ScopeViolation('read_endpoint_outside_registered_source')
        import lark_oapi as lark
        request = (lark.BaseRequest.builder().http_method(lark.HttpMethod.GET).uri(path)
                   .token_types({lark.AccessTokenType.TENANT})
                   .queries(list((params or {}).items())).build())
        try:
            response = self.client.request(request)
            if len(response.raw.content) > 16 * 1024 * 1024:
                raise InvalidResponse('read_response_too_large')
            value = json.loads(response.raw.content)
        except (InvalidResponse, ScopeViolation):
            raise
        except Exception:
            raise RetryableError('read_transport_unavailable') from None
        code = value.get('code') if isinstance(value, dict) else None
        if type(code) is not int:
            raise InvalidResponse('read_response_has_no_code')
        if code in _AUTH_CODES:
            raise AuthenticationExpired('read_identity_expired')
        if code in _PERMISSION_CODES:
            raise PermissionDenied('source_permission_denied')
        if code in _RATE_CODES:
            raise RetryableError('read_rate_limited', retry_after=60)
        if code:
            raise RemoteAPIError(code)
        if not isinstance(value.get('data'), dict):
            raise InvalidResponse('read_data_missing')
        return value['data']


class ExistingMeegleReader:
    """Read only aliases already present in document-business.json.

    The original _state method independently verifies the exact parent, node,
    deliverable, signed-in CLI identity, membership and field binding.
    """
    def __init__(self, port, owner_origin):
        self.port, self.owner_origin = port, owner_origin

    def fetch_page(self, source, start_ms, end_ms, cursor=None):
        alias = source.settings.get('target_alias')
        if (not source.enabled or cursor is not None or source.kind != 'project_snapshot'
                or alias not in self.port.settings['targets']
                or source.remote_id != self.port.settings['targets'][alias]['deliverable']):
            raise ScopeViolation('meegle_target_not_registered')
        principal = self.owner_origin()
        if principal is None:
            raise ScopeViolation('owner_origin_unavailable')
        try:
            binding, target, fields, files = self.port._state(principal, alias)
            # Never equate reviewed delivery, work-item status and node completion.
            node_data = self.port._run(['workflow','get-node','--project-key',binding['space'],
                '--work-item-id',binding['project'],'--node-id-list',binding['node'],'--need-sub-task'])
        except Exception as exc:
            if str(exc) in _MEEGLE_SCOPE_FAILURES:
                raise PermissionDenied('meegle_identity_or_scope_changed') from None
            raise RetryableError('meegle_read_unavailable') from None
        if not isinstance(node_data, dict) or not isinstance(node_data.get('list'), list):
            raise RetryableError('meegle_read_unavailable')
        nodes = node_data.get('list', [])
        pagination = node_data.get('pagination', {})
        if not isinstance(pagination, dict):
            raise RetryableError('meegle_read_unavailable')
        if (len(nodes) != 1 or not isinstance(nodes[0], dict)
                or not isinstance(nodes[0].get('basic'), dict)
                or nodes[0]['basic'].get('node_key') != binding['node']
                or pagination.get('has_more')):
            raise PermissionDenied('meegle_identity_or_scope_changed')
        node = nodes[0]
        schedule = node.get('schedule') or {}
        plan = {k:schedule.get(k) for k in ('estimate_start_time','estimate_finish_time')}
        actual = {k:schedule.get(k) for k in ('actual_begin_time','actual_finish_time')}
        record = {'deliverable_status': target['business_status'],
                  'professional_review': target['professional_review'],
                  'project_id': target['project_id'], 'node_id': target['node_id'],
                  'node_status':node['basic'].get('status'), 'node_plan':plan, 'node_actual':actual,
                  'attachment_count': len(files) if files is not None else None,
                  'attachment_content': 'pending' if files else 'unknown' if files is None else 'none'}
        text = canonical(record)
        payload = {'record': record, 'review_quote': canonical(target['professional_review']),
                   'status_quote': canonical(target['business_status']),
                   'node_quote':canonical(record['node_status']), 'plan_quote':canonical(plan),
                   'scope': 'registered_deliverable_fields', 'time_basis': 'observed_at',
                   'pending_resources': [{'kind': 'attachment', 'status': 'pending_content'} for _ in files or []]}
        observation = Observation(source.source_id, source.remote_id,
            digest({'deliverable_revision':target['revision'],'node_fields':record}), end_ms,
            text=text, payload=payload, url=target.get('url'))
        return Page((observation,), time_coverage=False)


class ExistingTasksReader:
    """An explicitly registered Tasks GUID, separate from Meegle work items."""
    def __init__(self, reader):
        self.reader = reader

    def fetch_page(self, source, start_ms, end_ms, cursor=None):
        if (source.kind != 'task_snapshot' or not source.enabled or cursor is not None
                or source.remote_id not in source.settings.get('allowed_object_ids', ())):
            raise ScopeViolation('task_not_registered')
        data = self.reader.get(source, '/open-apis/task/v2/tasks/' + remote_id(source.remote_id),
                               {'user_id_type': 'open_id'})
        task = data.get('task')
        if not isinstance(task, dict) or task.get('guid') != source.remote_id:
            raise InvalidResponse('task_identity_mismatch')
        required = ('summary', 'completed_at', 'updated_at')
        if any(key not in task for key in required):
            raise InvalidResponse('task_fields_missing')
        try:
            updated = int(task['updated_at'])
            completed = int(task['completed_at'])
        except (TypeError, ValueError):
            raise InvalidResponse('task_time_invalid') from None
        record = {key: task.get(key) for key in ('summary', 'description', 'due', 'start', 'members')}
        record['completed_at'] = completed
        record['status'] = 'completed' if completed else 'open'
        text = canonical(record)
        payload = {'record': record, 'quote': text, 'scope': 'task_fields_only',
                   'comments': 'not_read', 'attachments': 'pending_content',
                   'source_updated_at_ms': updated, 'time_basis': 'observed_at'}
        # Task creation/update may predate registration. This is the current
        # snapshot, not permission to fetch all previous versions or comments.
        return Page((Observation(source.source_id, source.remote_id,
                    str(updated) + ':' + digest(record), end_ms, text=text, payload=payload),),
                    time_coverage=False)
