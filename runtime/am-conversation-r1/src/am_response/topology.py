"""Code-owned responsibility registry; not another graph execution engine."""
NODES = (
    ('N01', 'admit', 'HarnessHost._admit_source', 'event', 'original_admission'),
    ('N02', 'resolve_intent', 'ResponsePipeline.begin', 'pure', 'rebuild_current'),
    ('N03', 'plan_steps', 'completion.resolve_goals', 'pure', 'rebuild_current'),
    ('N04', 'collect_evidence', 'ResponsePipeline.record', 'read', 'fresh_scope_only'),
    ('N05', 'execute_capability', 'ConversationAgent._run', 'original_tool', 'receipt_only'),
    ('N06', 'check_completion', 'completion.check_goals', 'pure', 'receipt_only'),
    ('N07', 'compile_outcome', 'ResponsePipeline.envelope', 'pure', 'current_dependencies'),
    ('N08', 'select_response_policy', 'ResponsePipeline.finish', 'pure', 'frozen_mode'),
    ('N09', 'realize_response', 'CoordinatorModel.complete', 'original_model', 'no_effect_replay'),
    ('N10', 'validate_response', 'ResponsePipeline.validate', 'pure', 'current_scope'),
    ('N11', 'freeze_presentation', 'ConversationStore.finish_event', 'original_sql', 'stored_event'),
    ('N12', 'publish_and_verify', 'GatewayProjection.bind/receipt', 'original_outbox', 'receipt_only'),
    ('N13', 'continue_or_close', 'host_context.build', 'read', 'delivered_history_only'),
)
EDGES = ((1,2),(2,3),(3,4),(4,5),(5,4),(5,6),(4,6),(6,7),(7,8),(8,9),(9,5),
         (9,10),(8,10),(10,11),(10,8),(11,12),(12,13),(13,2))
RECOVERY = {'read': 'revalidate_scope_and_source', 'local_committed': 'read_original_receipt',
            'local_unknown': 'original_reservation_reconciliation', 'external_unknown': 'original_message_or_action_reconciliation',
            'provider_stop_unknown': 'original_lane_proof', 'terminal_notice': 'original_idempotent_notice'}


def notice_family(kind):
    if kind in ('conversation', 'query', 'group_query'): return 'query'
    if kind.startswith('document'): return 'document'
    if kind.startswith('feedback'): return 'followup'
    if kind in ('harness_control',): return 'control'
    if kind in ('harness_error', 'conversation_error'): return 'failure'
    if kind in ('reminder', 'occurrence', 'briefing'): return 'schedule'
    if kind == 'card': return 'approval'
    return 'legacy'
