"""Adapters select documented result fields; they never infer success from prose."""
from .types import fingerprint, encode

TOOL_FAMILIES = {
    'list_projects': 'query', 'read_project': 'query', 'read_information': 'analysis',
    'list_followups': 'followup', 'create_followup': 'followup', 'change_followup': 'followup',
    'close_followup': 'followup', 'submit_followup_feedback': 'followup',
    'list_work_notes': 'worknote', 'save_work_note': 'worknote', 'update_work_note': 'worknote',
    'search_knowledge': 'knowledge', 'propose_knowledge': 'knowledge', 'revise_knowledge': 'knowledge',
    'approve_knowledge': 'knowledge', 'list_knowledge_candidates': 'knowledge', 'record_report': 'knowledge',
    'read_documents': 'document', 'edit_document': 'document', 'publish_document': 'document',
    'send_message': 'sending', 'reconcile_send': 'sending', 'list_targets': 'sending',
    'schedule_reminder': 'schedule', 'control_reminder': 'schedule', 'list_reminders': 'schedule',
    'revise_reminder': 'schedule', 'subscribe_briefing': 'schedule', 'read_briefing': 'analysis',
    'submit_project_update': 'report', 'list_project_updates': 'report', 'adopt_project_update': 'report',
    'list_pending_actions': 'approval', 'revise_pending_action': 'approval',
    'cancel_pending_action': 'control', 'discover_capabilities': 'capability',
}
LABELS = {'followup': '跟进事项', 'worknote': '工作笔记', 'knowledge': '知识记录',
          'document': '文档', 'sending': '发送', 'schedule': '提醒计划', 'report': '项目反馈',
          'query': '查询', 'analysis': '资料分析', 'approval': '待确认动作', 'control': '控制操作',
          'capability': '能力查询', 'unclassified': '这项操作'}


def family_for(name):
    return TOOL_FAMILIES.get(name, 'unclassified')


def adapt(name, arguments, result, effect, *, key):
    family = family_for(name)
    raw = result if isinstance(result, dict) else {'value': result}
    status = raw.get('status', raw.get('state', 'observed'))
    obj_type, obj_id, version, receipt = family, None, None, None
    verified = raw.get('verified') is True
    completion = 'blocked'
    gaps = []
    coverage = {'complete': False, 'requested': arguments, 'has_more': raw.get('has_more'),
                'source_coverage': raw.get('coverage', {})}
    if status in ('error', 'failed', 'result_too_large') or raw.get('available') is False:
        status = 'failed'
    elif status in ('unknown', 'executing'):
        status = 'unknown'
    elif status == 'draft' and raw.get('action_id'):
        obj_id, version = raw['action_id'], raw.get('version')
        status, completion = 'awaiting_approval', 'awaiting_approval'
    elif name == 'reconcile_send':
        obj_id = raw.get('object_id') or arguments.get('action_id')
        receipt = raw.get('receipt_id')
        coverage['complete'] = status in ('succeeded', 'failed')
        completion = 'fulfilled' if status == 'succeeded' and verified and receipt else 'blocked'
    elif effect == 'read':
        # Absence of a paging flag in an unknown adapter never proves coverage.
        complete = raw.get('complete_response') is True or raw.get('has_more') is False
        if name in ('read_project', 'discover_capabilities', 'list_targets'):
            complete = True
        if name == 'read_project' and isinstance(raw.get('information'), dict):
            info = raw['information']
            complete = complete and info.get('complete_response') is True
        source_coverage = raw.get('coverage', {})
        if isinstance(source_coverage, dict) and any(source_coverage.get(k) for k in (
                'processing_pending', 'needs_refresh_or_review', 'source_details_omitted', 'stale')):
            complete = False
        coverage['complete'] = complete
        status, completion = 'observed', ('fulfilled' if complete else 'partial')
    elif status == 'succeeded' and verified and raw.get('receipt_id'):
        receipt = raw['receipt_id']
        obj_id = raw.get('object_id', receipt)
        completion = 'fulfilled'
    elif name in ('create_followup', 'change_followup') and isinstance(raw.get('item'), dict):
        obj = raw['item']
        obj_id, version, status = obj.get('id'), obj.get('version'), obj.get('state', 'unknown')
        completion = 'fulfilled' if obj_id and type(version) is int else 'blocked'
    elif name in ('save_work_note', 'update_work_note') and isinstance(raw.get('task'), dict):
        obj = raw['task']; obj_id, version, status = obj.get('id'), obj.get('version'), obj.get('status')
        completion = 'fulfilled' if obj_id and type(version) is int else 'blocked'
    elif raw.get('record_id'):
        obj_id, version = raw['record_id'], raw.get('version')
        status = raw.get('status', 'candidate')
        completion = 'fulfilled' if name in ('propose_knowledge', 'revise_knowledge', 'record_report') else 'blocked'
    elif raw.get('draft_id'):
        obj_id, version = raw['draft_id'], raw.get('version')
        # Queue acceptance is never generation/adoption/mount completion.
        operation = arguments.get('operation')
        if operation == 'generate' or status == 'queued':
            completion = 'active'
        elif operation == 'adopt':
            completion = ('fulfilled' if status == 'ready' and obj_id == arguments.get('draft_id')
                          and version == arguments.get('version') and bool(raw.get('content_hash'))
                          and raw.get('accepted_hash') == raw.get('content_hash') == arguments.get('content_hash')
                          else 'blocked')
        else:
            completion = 'fulfilled' if obj_id and version and operation in ('create', 'add_text', 'requirements', 'target', 'cancel', 'precheck') else 'blocked'
    elif raw.get('report_id'):
        obj_id, version = raw['report_id'], raw.get('version')
        completion = 'fulfilled' if version else 'blocked'
    elif name == 'submit_followup_feedback' and raw.get('applied') is True:
        obj_id, version = arguments.get('item_id'), arguments.get('version')
        status, completion = raw.get('state', 'submitted'), 'fulfilled'
    elif name == 'control_reminder' and raw.get('plan_id'):
        obj_id, version, status = raw['plan_id'], raw.get('version'), raw.get('state')
        completion = 'fulfilled' if obj_id and version else 'blocked'
    elif name in ('approve_knowledge', 'cancel_pending_action') and raw.get('id'):
        obj_id, version, status = raw['id'], raw.get('version'), raw.get('status')
        completion = 'fulfilled' if version else 'blocked'
    if effect != 'read' and completion == 'fulfilled' and receipt is None:
        # This is a local commit observation, not an external delivery receipt.
        receipt = {'kind': 'local_result', 'object_id': obj_id, 'version': version,
                   'result_hash': fingerprint(raw), 'independent_readback': verified}
    if effect == 'read' and name != 'reconcile_send' and not coverage['complete']:
        gaps.append('本次读取范围尚未全部核实；已读取的内容不代表全部资料。')
    if status == 'unknown':
        gaps.append('执行结果仍待核对；核清前不自动重复办理。')
    if completion == 'blocked' and not gaps:
        gaps.append('这项要求尚无足够的完成凭据。')
    label = LABELS[family]
    state_text = {'fulfilled': '已记录对应结果', 'partial': '只取得部分结果', 'active': '已受理，仍在处理',
                  'awaiting_approval': '尚待确认，未执行', 'blocked': '尚未确认完成'}.get(completion, '仍待核对')
    status_text = label + '：' + state_text + '。'
    # Trusted adapter prose can retain exact local details, but cannot set status.
    if raw.get('message') and type(raw['message']) is str and status not in ('error', 'unknown', 'failed') and completion != 'blocked':
        status_text = raw['message']
    if gaps: status_text += '\n' + '\n'.join(gaps)
    return {'id': key, 'operation': name, 'target': arguments, 'object_type': obj_type,
            'object_id': obj_id, 'version': version, 'family': family, 'effect': effect,
            'status': status, 'completion': completion, 'receipt': receipt, 'verified': verified,
            'coverage': coverage, 'gaps': gaps, 'block_id': 'unit:' + key,
            'status_text': status_text, 'result': raw}


def evidence(result, prefix):
    """Keep raw blocks and typed fields together, without rejecting mixed sources."""
    if not isinstance(result, dict): return []
    if isinstance(result.get('evidence_bundle'),dict):
        return result['evidence_bundle']['claims']
    claims = []
    if isinstance(result.get('fields'), dict):
        for field, value in result['fields'].items():
            claims.append({'id': prefix + ':' + field, 'subject': result.get('id'), 'predicate': field,
                           'subject_label':result.get('title'),
                           'value': value, 'modality': 'recorded', 'expression': 'quote',
                           'source': result.get('snapshot', {}), 'observed_at': result.get('observed_at')})
    for fact in result.get('facts', []):
        if not isinstance(fact, dict): continue
        claims.append({'id': prefix + ':' + str(fact.get('fact_id', len(claims))),
                       'subject': fact.get('subject_id'), 'predicate': fact.get('field'),
                       'subject_label':fact.get('subject_label'),
                       'value': fact.get('value'), 'modality': fact.get('assertion_type', 'unknown'),
                       'business_state': fact.get('business_state'), 'expression': 'quote',
                       'source': fact.get('evidence', {}),
                       **{k:fact[k] for k in ('polarity','time','time_scope','scope','conflict_group') if k in fact}})
    if isinstance(result.get('information'), dict):
        claims += evidence(result['information'], prefix + ':information')
    if not claims and 'message' not in result:
        claims.append({'id': prefix + ':raw', 'subject': result.get('id'), 'predicate': 'source_result',
                       'value': result, 'modality': 'recorded', 'expression': 'quote', 'source': {}})
    return claims
