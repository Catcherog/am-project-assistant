"""Narrow integration seam for the existing AM engine; not an installed patch.

Call before store.finish_event in the normal _finish path. The original host
still owns facts, authorization, deadlines, cancellation, outbox and receipts.
"""
from dataclasses import replace

from .contracts import ContractError, digest
from .runtime import recheck_before_send


def prepare_terminal_reply(*, presenter, agent, ctx, reply, envelope,
                           frame_projector, task_deadline_monotonic,
                           outcome_getter, require_current,
                           fastpath=False):
    """frame_projector is host-authored and must consume all semantic qualifiers.

    Unsupported raw claims must fail projection; do not use a model to establish
    tool success, independent native receipt identity or current access rights.
    Return (AgentReply, PreparedResponse|None, SpeechFrame|None).
    """
    if not ctx.is_owner:
        # Preserve the existing R2 experiment scope.
        return reply, None, None
    if presenter.mode == "off":
        return reply, None, None
    if not callable(outcome_getter) or not callable(require_current):
        raise ContractError("explicit_outcome_collector_callbacks_required")
    frame = frame_projector(envelope)
    original = envelope.data
    bound = original["binding"]
    if frame.data["binding"]["outcome_hash"] != envelope.digest:
        raise ContractError("projector_changed_outcome_identity")
    if frame.data["binding"]["turn_id"] != bound["engine_event_id"]:
        raise ContractError("projector_changed_event_identity")
    fb = frame.data["binding"]
    if fb["scope_id"] != digest(bound["scope"]):
        raise ContractError("projector_changed_scope")
    if fb["source_revision"] != str(bound["source_access_version"]):
        raise ContractError("projector_changed_source_revision")
    for key in ("revision", "cancel_epoch"):
        if fb[key] != bound.get(key, 0):
            raise ContractError("projector_changed_fence:" + key)
    if frame.data["expires_at"] > original["expires_at"]:
        raise ContractError("projector_extended_source_lifetime")

    def guard(current):
        agent._guard(ctx, "read")
        agent._checkpoint(ctx, "before_finish")
        # Explicit callbacks let a collect-only ContextVar work while the old
        # response mode stays legacy. Do not assume that pipeline.begin ran.
        require_current()
        current_envelope = outcome_getter()
        if current_envelope is None or current_envelope.digest != current.data["binding"]["outcome_hash"]:
            from .runtime import StaleFrame
            raise StaleFrame("outcome_changed_during_expression")

    prepared = presenter.prepare(frame, legacy_text=reply.text, guard=guard,
                                 task_deadline_monotonic=task_deadline_monotonic,
                                 fastpath=fastpath)
    return replace(reply, text=prepared.text), prepared, frame


def gateway_text(prepared, current_frame, original_gateway_guard):
    """Use at original enqueue/commit boundary; never publish directly here."""
    return recheck_before_send(prepared, current_frame, original_gateway_guard)
