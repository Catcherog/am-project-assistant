"""Small host adapter for the original AM ConversationAgent.

This module does not open a socket, create a coordinator, send a message, or
persist a second copy of a conversation. Install it beside the original AM
bridge, and inject the already-running gateway.provider coordinator.
"""
from __future__ import annotations

from dataclasses import fields
import math
import time
from typing import Callable

from am_reply.compiler import build_legacy_contract
from am_reply.model import CoordinatorRewriteModel
from am_reply.pipeline import ReplyRewriter
from am_reply.types import ReplyContract, RewriteConfig, RewriteRequest, StyleProfile


class AmOutputFinalizer:
    """Callable attached only to the ordinary read-only answer branch.

    ``guard`` must revalidate the *current* host identity, expiry and source
    access version; exceptions from that guard always escape. ``style_provider``
    and ``contract_builder`` are trusted host dependencies, never model tools.
    A custom contract builder must use only the supplied authorized evidence.
    """

    def __init__(
        self, coordinator, *, settings: dict | None = None,
        guard: Callable, metrics: Callable | None = None,
        contract_builder: Callable = build_legacy_contract,
        style_provider: Callable | None = None,
        observer: Callable | None = None,
        clock: Callable[[], float] = time.monotonic,
    ):
        if not callable(guard):
            raise ValueError("OUTPUT_REWRITE_GUARD_REQUIRED")
        if settings is None:
            settings = {}
        if type(settings) is not dict or set(settings) - {f.name for f in fields(RewriteConfig)}:
            raise ValueError("OUTPUT_REWRITE_SETTINGS_INVALID")
        if not callable(contract_builder) or (style_provider is not None and not callable(style_provider)):
            raise ValueError("OUTPUT_REWRITE_DEPENDENCY_INVALID")
        self.config = RewriteConfig(**settings)
        self.clock, self.guard, self.metrics = clock, guard, metrics
        self.contract_builder, self.style_provider = contract_builder, style_provider
        self.observer = observer
        self.rewriter = ReplyRewriter(CoordinatorRewriteModel(coordinator), self.config, clock=clock)

    def _emit(self, event_id: str, **observation) -> None:
        if self.metrics is None:
            return
        metric = {
            "event_id": event_id,
            "span": "output_rewrite",
            "seconds": observation.pop("elapsed_seconds", 0.0),
            "mode": self.config.mode,
            **observation,
        }
        try:
            self.metrics(metric)
        except Exception:
            pass  # A diagnostic sink must never retry or lose a completed answer.

    def _skip(self, ctx, event_id: str, draft: str, reason: str) -> str:
        self.guard(ctx)
        self._emit(event_id, disposition="skipped", attempted=False, reasons=[reason], output_chars=len(draft))
        return draft

    def __call__(
        self, *, ctx, event_id: str, user_text: str, draft: str,
        read_evidence, deadline: float | None,
        status: str = "answered", protected: bool = False,
        pending_actions: tuple[str, ...] = (), has_local_effect: bool = False,
    ) -> str:
        self.guard(ctx)
        # Host metadata is decisive. A model cannot label a preview "ordinary"
        # to cause it to be rewritten. The engine patch also excludes these paths.
        if self.config.mode == "off":
            return self._skip(ctx, event_id, draft, "disabled")
        if status != "answered" or protected or pending_actions or has_local_effect:
            return self._skip(ctx, event_id, draft, "protected_host_reply")
        if type(deadline) not in (int, float) or not math.isfinite(deadline):
            return self._skip(ctx, event_id, draft, "shared_deadline_missing")
        if deadline <= self.clock():
            return self._skip(ctx, event_id, draft, "shared_deadline_exhausted")

        try:
            contract = self.contract_builder(ctx, event_id, user_text, draft, tuple(read_evidence))
            style = self.style_provider(ctx) if self.style_provider is not None else StyleProfile()
        except Exception:
            # No raw exception content (possibly including source data) is logged.
            # The guard is outside this try: authorization exceptions are not swallowed.
            return self._skip(ctx, event_id, draft, "host_projection_unavailable")
        self.guard(ctx)

        scope = getattr(ctx, "session_id", None)
        source_version = str(getattr(ctx, "source_access_version", None) or "unversioned")
        if (contract is not None and (
            not isinstance(contract, ReplyContract)
            or not isinstance(scope, str) or contract.scope_key != scope
            or contract.source_version != source_version
        )):
            return self._skip(ctx, event_id, draft, "contract_binding_mismatch")
        if (not isinstance(style, StyleProfile) or style.approved is not True
                or style.scope_key not in ("", scope)):
            return self._skip(ctx, event_id, draft, "style_binding_mismatch")

        request = RewriteRequest(
            event_id=event_id, user_text=user_text, draft=draft,
            contract=contract, deadline=deadline, style=style,
            status=status, protected=protected, pending_actions=tuple(pending_actions),
        )
        result = self.rewriter.rewrite(request, guard=lambda: self.guard(ctx))
        self.guard(ctx)
        self._emit(event_id, **result.telemetry())
        if self.observer is not None:
            try:
                self.observer(request, result)
            except Exception:
                pass  # Optional bounded evaluation cannot change a business result.
        return result.text
