"""Optional binding to AM's existing ConversationAgent, not a new provider.

The host supplies fresh identity and grants via ``context_resolver``.  A
LegacyConversationBridge resolver must also enter its request ContextVar.
Queue snapshots alone are never authorization credentials.
"""
from __future__ import annotations

from contextlib import contextmanager, nullcontext
from contextvars import ContextVar
from dataclasses import dataclass
import hashlib
import math
import time
from typing import Callable
import uuid


class BindingError(RuntimeError):
    def __init__(self, code: str):
        self.code = code
        super().__init__(code)


@dataclass(frozen=True)
class _ReadOnlyBinding:
    context: object
    expires_at: float
    trace_id: str


_READONLY_PHASES = frozenset({"before_handle_readonly", "after_handle_readonly",
    "before_fastpath_prepare", "after_fastpath_prepare", "before_fastpath_consume",
    "after_fastpath_consume", "before_finish", "after_finish"})


class LifecycleBinding:
    """Bind one durable run fence to this execution context.

    The patched AM engine calls this object outside tool error swallowing.
    The callback must raise when a fence is stale, cancelled or expired.
    Checkpoints cooperate with cancellation; this is not a network abort or
    an atomic external-effect commit.  Final delivery needs the host fence.
    """

    def __init__(self, guard: Callable, *, strict_bound: bool = True, observer=None, clock=time.time):
        if not callable(guard) or type(strict_bound) is not bool:
            raise ValueError("INVALID_LIFECYCLE_CONFIG")
        self.guard, self.strict_bound, self.observer = guard, strict_bound, observer
        self.clock = clock
        self._current = ContextVar("am_harness_bound_fence", default=None)

    @contextmanager
    def bind(self, fence):
        token = self._current.set(fence)
        try:
            yield self
        finally:
            self._current.reset(token)

    @contextmanager
    def bind_readonly(self, ctx, *, expires_at: float):
        """A <=5s ingress read; cannot enter model/control/tool/effect phases."""
        now = self.clock()
        if type(expires_at) not in (int, float) or not math.isfinite(expires_at) or expires_at <= now:
            raise BindingError("HARNESS_READONLY_EXPIRED")
        end = min(expires_at, now + 5, ctx.content_expires_at or now + 5)
        if end <= now:
            raise BindingError("HARNESS_READONLY_EXPIRED")
        token = self._current.set(_ReadOnlyBinding(ctx, end, "read-" + uuid.uuid4().hex))
        try:
            yield self
        finally:
            self._current.reset(token)

    @property
    def fence(self):
        value = self._current.get()
        return None if isinstance(value, _ReadOnlyBinding) else value

    def __call__(self, ctx, phase: str, args: dict) -> None:
        fence = self._current.get()
        if fence is None:
            if self.strict_bound:
                raise BindingError("HARNESS_UNBOUND_RUN")
            return
        if isinstance(fence, _ReadOnlyBinding):
            if ctx != fence.context:
                raise BindingError("HARNESS_SCOPE_MISMATCH")
            if phase not in _READONLY_PHASES:
                raise BindingError("HARNESS_READONLY_PHASE_DENIED")
            if self.clock() >= fence.expires_at:
                raise BindingError("HARNESS_READONLY_EXPIRED")
            if self.observer:
                try:
                    self.observer({"run_id": fence.trace_id, "phase": phase, "mode": "readonly"})
                except Exception:
                    pass
            return
        if getattr(fence, "scope_key", None) != ctx.session_id:
            raise BindingError("HARNESS_SCOPE_MISMATCH")
        if ctx.content_expires_at is not None and ctx.content_expires_at <= self.clock():
            raise BindingError("HARNESS_CONTEXT_EXPIRED")
        self.guard(fence)
        if self.observer:
            # Metadata only; never send user input or arbitrary tool arguments
            # to telemetry.  Observability failure must not retry a side effect.
            try:
                self.observer({"run_id": fence.run_id, "revision": fence.revision,
                               "cancel_epoch": fence.cancel_epoch, "phase": phase})
            except Exception:
                pass


class OneShotReadRunner:
    """Short read at ingress, before durable queue submission.

    The caller has already authenticated the original AM inbox row and entered
    LegacyConversationBridge._active. The original ConversationStore commits
    dedupe/history; the caller renders/enqueues/delivers through the existing
    outbox. ``None`` means no event was claimed and the model path may start.
    """

    def __init__(self, agent, hooks: LifecycleBinding, *, clock=time.time):
        if getattr(agent, "lifecycle", None) is not hooks or not callable(getattr(agent, "handle_fastpath", None)):
            raise BindingError("HARNESS_FASTPATH_ENTRY_NOT_INSTALLED")
        self.agent, self.hooks, self.clock = agent, hooks, clock

    def try_handle(self, ctx, event_id: str, text: str):
        with self.hooks.bind_readonly(ctx, expires_at=self.clock() + 5):
            self.hooks(ctx, "before_handle_readonly", {})
            reply = self.agent.handle_fastpath(ctx, event_id, text)
            self.hooks(ctx, "after_handle_readonly", {})
            return reply


def engine_event_id(snapshot) -> str:
    """Stable revision-specific ID prevents an old AM event poisoning a steer."""
    run_id = str(snapshot.id)
    if len(run_id) > 180:
        run_id = hashlib.sha256(run_id.encode()).hexdigest()
    return f"harness:{run_id}:v{snapshot.revision}"


class AmAgentRunner:
    """One durable turn through the original patched ConversationAgent.

    Returned ``reply.event_id`` remains the internal revision event ID.  The
    host may join it to origin_event_id in the existing outbox; do not pass it
    to an unmodified LegacyConversationBridge.process(), which expects the
    exact incoming event ID.  See the explicit integration checklist.
    """

    def __init__(self, agent, context_resolver, hooks: LifecycleBinding, *, clock=time.time):
        if getattr(agent, "lifecycle", None) is not hooks:
            raise BindingError("HARNESS_ENGINE_HOOKS_NOT_INSTALLED")
        if not callable(context_resolver):
            raise ValueError("INVALID_CONTEXT_RESOLVER")
        self.agent, self.context_resolver, self.hooks, self.clock = agent, context_resolver, hooks, clock

    def run(self, snapshot) -> dict:
        return self._execute(snapshot, fastpath_only=False)

    def try_fastpath(self, snapshot) -> dict | None:
        """Try an independent read without entering the model turn lane.

        On a grammar/shape miss no AM event was claimed; the same durable
        snapshot may be queued normally. The host must not start a normal
        worker for this same run until this attempt returns.
        """
        if not callable(getattr(self.agent, "handle_fastpath", None)):
            raise BindingError("HARNESS_FASTPATH_ENTRY_NOT_INSTALLED")
        return self._execute(snapshot, fastpath_only=True)

    def _execute(self, snapshot, *, fastpath_only: bool) -> dict | None:
        remaining = min(snapshot.deadline, snapshot.expires_at) - self.clock()
        if remaining <= 0:
            raise BindingError("HARNESS_DEADLINE_EXPIRED")
        resolved = self.context_resolver(snapshot)
        manager = resolved if hasattr(resolved, "__enter__") else nullcontext(resolved)
        with manager as ctx, self.hooks.bind(snapshot.fence):
            if ctx.session_id != snapshot.scope.key:
                raise BindingError("HARNESS_SCOPE_MISMATCH")
            self.hooks(ctx, "before_handle", {})
            remaining = min(snapshot.deadline, snapshot.expires_at,
                            ctx.content_expires_at or snapshot.expires_at) - self.clock()
            if remaining <= 0:
                raise BindingError("HARNESS_DEADLINE_EXPIRED")
            event_id = engine_event_id(snapshot)
            if fastpath_only:
                reply = self.agent.handle_fastpath(ctx, event_id, snapshot.input_text)
            else:
                reply = self.agent.handle(ctx, event_id, snapshot.input_text, timeout=remaining)
            self.hooks(ctx, "after_handle", {})
            if reply is None:
                return None
            return {"run_id": snapshot.id, "revision": snapshot.revision,
                    "cancel_epoch": snapshot.cancel_epoch,
                    "origin_event_id": snapshot.event_id, "engine_event_id": event_id,
                    "reply": reply.to_dict()}
