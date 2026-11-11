"""Bounded, source-bound continuation of one authorized work task.

This is task data, never global model memory or evidence that a draft was sent.
Every frozen source is checked again by the existing Harness tool boundary.
"""
from __future__ import annotations

import json

from ..models import ContractError, dumps, key


_SOURCE_FIELDS = ('body', 'is_deleted', 'sender_id', 'chat_id', 'thread_id',
                  'parent_id', 'root_id', 'mentions', 'attachments', 'revision', 'version_time')
_ACTIVE_INPUTS = frozenset({'admitted', 'feedback_recorded'})


def _exists(db, name):
    return db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)).fetchone() is not None


def owner_cli_sql(alias='t'):
    """Three bound parameters: owner ID, owner chat, owner principal."""
    return (f"(json_extract({alias}.request_json,'$.observed_via') IN ('gpt_dot','grok_bot') "
            f"AND json_extract({alias}.request_json,'$.channel')='bot_dm' "
            f"AND json_extract({alias}.request_json,'$.sender_id')=? "
            f"AND json_extract({alias}.request_json,'$.chat_id')=? "
            f"AND json_extract({alias}.policy_json,'$.principal')=?)")


def mark_owner_results_available(harness, db):
    """Persist a non-Feishu result channel; do not clear uncertain real effects."""
    cfg = harness.config
    if not _exists(db, 'r5_links'):
        return
    rows = db.execute(f"""SELECT t.* FROM h1_tasks t JOIN r5_links l USING(item_id)
        WHERE {owner_cli_sql()} AND l.origin IN ('gpt_dot','grok_bot')
        AND l.origin=json_extract(t.request_json,'$.observed_via')
        AND t.execution_state IN ('succeeded','failed') AND t.result_json IS NOT NULL
        AND t.delivery_state='not_delivered' AND t.verification_state!='conflict'
        AND t.last_error!='source_changed'
        AND NOT EXISTS(SELECT 1 FROM h1_effects e WHERE e.task_id=t.task_id)
        AND NOT EXISTS(SELECT 1 FROM h1_sessions s WHERE s.task_id=t.task_id AND s.state!='free')
        AND EXISTS(SELECT 1 FROM r5_intake i WHERE i.item_id=t.item_id AND i.state='admitted')""",
        (cfg.owner_id, cfg.owner_chat_id, cfg.owner_id)).fetchall()
    for row in rows:
        stamps = json.loads(row['stamps'])
        stamps.setdefault('owner_result_available_at', harness.store.clock())
        db.execute("UPDATE h1_tasks SET delivery_state='available_to_owner',stamps=? WHERE task_id=?",
                   (dumps(stamps), row['task_id']))


def _owner_private(harness, event, policy):
    return (policy.principal == harness.config.owner_id and event['channel'] == 'bot_dm'
            and event['chat_id'] == harness.config.owner_chat_id
            and policy.reply_target == harness.config.owner_chat_id)


def _work_access(harness, db, work, event, policy):
    if policy.revoked or event['chat_id'] != policy.reply_target:
        return False
    project = work['project_id']
    if project and project not in policy.projects:
        return False
    if work['visibility'] == 'private' and event['channel'] != 'bot_dm':
        return False
    if _owner_private(harness, event, policy):
        return True
    if project and not db.execute('SELECT 1 FROM a5_wb_project_grants WHERE principal_id=? AND project_id=?',
                                  (policy.principal, project)).fetchone():
        return False
    return (work['requester_id'] == policy.principal
            or work['visibility'] == 'project' and bool(project))


def _visible_input(harness, current, previous, policy):
    if (previous.get('is_deleted') or previous['chat_id'] not in policy.source_chats
            or not set(previous.get('project_ids', ())) <= set(policy.projects)):
        return False
    if _owner_private(harness, current, policy):
        return True
    if current['channel'] == 'group':
        # A task can acquire private additions after creation in a group.
        return previous['channel'] == 'group' and previous['chat_id'] == current['chat_id']
    return (previous['channel'] == 'group' or
            previous['channel'] == 'bot_dm' and previous['chat_id'] == current['chat_id']
            and previous['sender_id'] == policy.principal)


def _input_binding(db, item_id):
    from ..phase1.contracts import Policy
    row = db.execute('SELECT * FROM r5_intake WHERE item_id=?', (item_id,)).fetchone()
    if not row or row['state'] not in _ACTIVE_INPUTS:
        return None
    event = json.loads(row['event_json'])
    if event.get('is_deleted'):
        return None
    task = db.execute('SELECT * FROM h1_tasks WHERE item_id=?', (item_id,)).fetchone()
    if task:
        old = json.loads(task['request_json'])
        previous_policy = Policy.parse(task['policy_json'])
        source = db.execute('SELECT i.version,i.is_deleted,i.legacy_owned,b.accepted FROM gw_items i JOIN h1_inbox b USING(item_id) WHERE i.item_id=?', (item_id,)).fetchone()
        if (previous_policy.revoked or event['chat_id'] not in previous_policy.source_chats
                or not source or source['is_deleted'] or source['legacy_owned'] or not source['accepted']
                or source['version'] != task['source_version']
                or any(event.get(field) != old.get(field) for field in _SOURCE_FIELDS)):
            return None
    return {'item_id': item_id, 'source_hash': row['source_hash']}, event, task


def _source_ref(event):
    return ('entry:' + event['observed_via'] + ':' + event['native_id']
            if event['observed_via'] in {'gpt_dot', 'grok_bot'}
            else 'feishu:message:' + event['native_id'])


def _validate_bindings(harness, db, snapshot, current, policy):
    from ..phase1.contracts import Policy
    for binding in snapshot.get('sources', []):
        value = _input_binding(db, binding['item_id'])
        if (not value or value[0] != binding
                or not _visible_input(harness, current, value[1], policy)):
            raise ContractError('work continuation source changed or outside current audience')
    for binding in snapshot.get('results', []):
        row = db.execute('SELECT * FROM h1_tasks WHERE task_id=?', (binding['task_id'],)).fetchone()
        if (not row or key(row['result_json']) != binding['result_hash']
                or row['generation'] != binding['generation'] or row['source_version'] != binding['source_version']
                or row['policy_json'] != binding['policy_json'] or row['verification_state'] == 'conflict'
                or row['last_error'] == 'source_changed'):
            raise ContractError('work continuation result changed')
        previous = json.loads(row['request_json'])
        previous_policy = Policy.parse(row['policy_json'])
        if (previous_policy.revoked or not set(previous_policy.source_chats) <= set(policy.source_chats)
                or not set(previous_policy.projects) <= set(policy.projects)
                or not _visible_input(harness, current, previous, policy)):
            raise ContractError('work continuation result authority changed')
        if current['channel'] == 'group':
            effect = db.execute('SELECT * FROM h1_effects WHERE task_id=?', (row['task_id'],)).fetchone()
            if (row['delivery_state'] != 'delivered' or row['verification_state'] != 'sufficient'
                    or not effect or not effect['platform_id'] or effect['target'] != current['chat_id']):
                raise ContractError('private or undelivered result cannot enter group continuation')
        accepted = json.loads(row['context_json'] or '{}')
        for item in accepted.get('evidence_items', []):
            info = db.execute('SELECT * FROM h1_info WHERE info_id=?', (item['info_id'],)).fetchone()
            if not info or not harness.information.allowed(db, info, policy):
                raise ContractError('work continuation supporting evidence changed')
    for binding in snapshot.get('steps', []):
        row = db.execute('SELECT * FROM a5_wb_steps WHERE step_id=?', (binding['step_id'],)).fetchone()
        if not row or key(dict(row)) != binding['digest']:
            raise ContractError('work continuation step changed')


def validate_work_context(harness, db, task, policy):
    snapshot = json.loads(task['context_json'] or '{}').get('work_task')
    if not snapshot:
        return
    work = db.execute('SELECT * FROM a5_wb_tasks WHERE task_id=?', (snapshot['task_id'],)).fetchone()
    event = json.loads(task['request_json'])
    if not work or not _work_access(harness, db, work, event, policy):
        raise ContractError('work continuation access changed')
    _validate_bindings(harness, db, snapshot['bindings'], event, policy)


def add_work_context(harness, db, task, policy, context):
    """Prefer the latest same-task results within the existing total byte budget."""
    if not _exists(db, 'r5_links'):
        return context
    link = db.execute('SELECT work_task_id FROM r5_links WHERE item_id=?', (task['item_id'],)).fetchone()
    if not link:
        return context
    work = db.execute('SELECT * FROM a5_wb_tasks WHERE task_id=?', (link[0],)).fetchone()
    current = json.loads(task['request_json'])
    if not work or not _work_access(harness, db, work, current, policy):
        return context
    snapshot = {'schema': 'am.work-context.v1', 'task_id': work['task_id'], 'state': work['state'],
                'entries': [], 'bindings': {'sources': [], 'results': [], 'steps': []},
                'truncated': False, 'omitted_entries': 0,
                'status_rule': 'Generated results are drafts, not sent messages or verified business completion.'}
    output = {**context, 'work_task': snapshot}
    candidates = []
    rows = db.execute('''SELECT i.*,l.work_task_id FROM r5_intake i JOIN r5_links l USING(item_id)
        WHERE l.work_task_id=? AND i.item_id!=? AND i.created<=? ORDER BY i.created DESC,i.item_id DESC LIMIT 41''',
        (work['task_id'], task['item_id'], task['created'])).fetchall()
    if len(rows) > 40:
        snapshot['truncated'] = True
        rows = rows[:40]
    for row in rows:
        value = _input_binding(db, row['item_id'])
        if not value or not _visible_input(harness, current, value[1], policy):
            continue
        binding, event, previous = value
        source = _source_ref(event)
        request = {'kind': 'request' if row['state'] == 'admitted' else 'feedback',
                   'source_ref': source, 'origin': row['origin'], 'actor_id': event['sender_id'],
                   'text': event['body'], 'at': event['original_time']}
        if previous and previous['result_json'] and previous['execution_state'] in {'succeeded', 'failed'}:
            old_context = json.loads(previous['context_json'] or '{}').get('work_task', {})
            prior_bindings = old_context.get('bindings', {'sources': [], 'results': [], 'steps': []})
            result_binding = {'task_id': previous['task_id'], 'generation': previous['generation'],
                'source_version': previous['source_version'], 'result_hash': key(previous['result_json']),
                'policy_json': previous['policy_json']}
            dependencies = {name: list(prior_bindings.get(name, [])) for name in ('sources', 'results', 'steps')}
            dependencies['sources'].append(binding)
            dependencies['results'].append(result_binding)
            try:
                _validate_bindings(harness, db, dependencies, current, policy)
            except ContractError:
                pass
            else:
                result = json.loads(previous['result_json'])
                state = ('delivered' if previous['delivery_state'] == 'delivered' and previous['verification_state'] == 'sufficient'
                         else 'available_to_owner' if event['observed_via'] in {'gpt_dot', 'grok_bot'} else 'generated_not_sent')
                candidates.append(({'kind': 'result', 'source_ref': 'harness:result:' + previous['task_id'],
                    'request_source_ref': source, 'availability': state,
                    'execution_state': previous['execution_state'], 'business_completion_verified': False,
                    'text': result.get('reply_text', ''), 'remaining_requirements': result.get('remaining_requirements', []),
                    'artifacts': result.get('artifacts', [])}, dependencies))
        candidates.append((request, {'sources': [binding], 'results': [], 'steps': []}))
    # Steps with arbitrary host references are private owner records. Other
    # audiences receive only steps explicitly sourced to an included message.
    source_bindings = {entry['source_ref']: dependencies['sources']
                       for entry, dependencies in candidates if entry['kind'] != 'result'}
    allowed_refs = set(source_bindings)
    for row in db.execute('SELECT * FROM a5_wb_steps WHERE task_id=? ORDER BY updated_at DESC LIMIT 20', (work['task_id'],)):
        if not _owner_private(harness, current, policy) and row['source_ref'] not in allowed_refs:
            continue
        if row['source_ref'].startswith(('feishu:message:', 'entry:')) and row['source_ref'] not in allowed_refs:
            continue
        candidates.append(({'kind': 'step', **{name: row[name] for name in ('step_id', 'title', 'state', 'assignee_id', 'source_ref')}},
                           {'sources': source_bindings.get(row['source_ref'], []), 'results': [],
                            'steps': [{'step_id': row['step_id'], 'digest': key(dict(row))}]}))
    budget = harness.config.context_bytes
    for entry, dependencies in candidates:
        trial = json.loads(dumps(snapshot))
        trial['entries'].append(entry)
        for name in ('sources', 'results', 'steps'):
            trial['bindings'][name] = list({dumps(v): v for v in [*trial['bindings'][name], *dependencies[name]]}.values())
        proposed = {**output, 'work_task': trial}
        # Same-task continuity takes precedence over unrelated older project
        # evidence; both remain within one existing context_bytes limit.
        while len(dumps(proposed).encode()) > budget and proposed.get('evidence_items'):
            proposed['evidence_items'] = proposed['evidence_items'][:-1]
            proposed['view_lag_warning'] = True
        if len(dumps(proposed).encode()) > budget:
            snapshot['truncated'] = True
            snapshot['omitted_entries'] += 1
            continue
        output, snapshot = proposed, trial
    output['work_task'] = snapshot
    if len(dumps(output).encode()) > budget:
        # Minimum configurations may not fit even metadata. Explicitly retain
        # the bounded existing context instead of overflowing the envelope.
        return {**context, 'view_lag_warning': True}
    return output
