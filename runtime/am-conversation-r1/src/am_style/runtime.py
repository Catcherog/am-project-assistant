"""Prepare once, then return to the original gateway. No send or business API."""
import time

from .contracts import (StylePolicy, ModelRequest, PreparedResponse, ContractError,
                        canonical, digest)
from .prompt import build_messages
from .render import render_canonical
from .verify import CompositeVerifier, parse_candidate


class StaleFrame(RuntimeError):
    pass


class ModelUnavailable(RuntimeError):
    pass


class StopUnconfirmed(RuntimeError):
    """A timeout is not evidence that the provider has stopped. Host retains its lease."""


class TerminalPresenter:
    def __init__(self, provider=None, *, policy=None, verifier=None, mode="off",
                 generation_budget_s=8.0, send_reserve_s=0.25,
                 monotonic=time.monotonic, wall_clock=time.time):
        if mode not in ("off", "shadow", "enforce"):
            raise ValueError("invalid_mode")
        self.provider = provider
        self.policy = policy or StylePolicy()
        self.verifier = verifier or CompositeVerifier()
        self.mode = mode
        self.generation_budget_s = generation_budget_s
        self.send_reserve_s = send_reserve_s
        self.monotonic, self.wall_clock = monotonic, wall_clock

    def _current(self, frame, guard):
        # Guard failures propagate: do not send stale legacy or canonical text either.
        if frame.data["expires_at"] <= self.wall_clock():
            raise StaleFrame("source_expired")
        guard(frame)

    def prepare(self, frame, *, legacy_text, guard, task_deadline_monotonic,
                fastpath=False):
        started = self.monotonic()
        self._current(frame, guard)
        metrics = {"mode": self.mode, "attempted": False, "schema_pass": False,
                   "semantic_pass": False, "presentation_calls": 0, "adopted": False,
                   "naturalness": "not_evaluated", "delivery": "prepared",
                   "provider": None, "model": None, "model_ms": 0.0,
                   "frame_sha256": frame.fingerprint,
                   "policy_sha256": digest(self.policy.as_dict())}

        def result(text, selected, reason, candidate=None):
            self._current(frame, guard)
            metrics.update(total_ms=(self.monotonic()-started)*1000, selected=selected, reason=reason)
            return PreparedResponse(text, frame.fingerprint, selected, reason, candidate, canonical(metrics))

        if self.mode == "off":
            return result(legacy_text, "legacy", "mode_off")
        fallback = render_canonical(frame)
        if len(fallback) > self.policy.max_chars:
            # Never truncate receipt/preview/required facts. Host must paginate.
            raise ContractError("canonical_requires_host_pagination")
        base = legacy_text if self.mode == "shadow" else fallback
        selected = "legacy" if self.mode == "shadow" else "canonical"
        if fastpath:
            return result(base, selected, "fastpath_no_model")
        if task_deadline_monotonic is None:
            return result(base, selected, "missing_shared_deadline")
        remaining = task_deadline_monotonic - self.monotonic() - self.send_reserve_s
        budget = min(self.generation_budget_s, remaining)
        if budget <= 0:
            return result(base, selected, "shared_budget_exhausted")
        if self.provider is None:
            return result(base, selected, "provider_unconfigured")
        self._current(frame, guard)
        metrics.update(attempted=True, presentation_calls=1)
        request = ModelRequest(canonical(build_messages(frame, self.policy)),
                               frame.fingerprint, digest(self.policy.as_dict()), budget)
        expression_deadline = self.monotonic() + budget
        try:
            reply = self.provider.generate(request)
        except StopUnconfirmed:
            raise  # Original coordinator must reconcile stop before releasing its lane.
        except ModelUnavailable:
            return result(base, selected, "model_unavailable")
        # Never swallow stale/cancel, authorization, or unexpected implementation failures.
        self._current(frame, guard)
        metrics.update(provider=reply.provider, model=reply.model, model_ms=reply.duration_ms)
        candidate = parse_candidate(reply.text)
        if self.monotonic() > min(expression_deadline, task_deadline_monotonic - self.send_reserve_s):
            return result(base, selected, "late_result_discarded", candidate)
        if candidate is None:
            return result(base, selected, "invalid_output_schema")
        metrics["schema_pass"] = True
        verdict = self.verifier.verify(frame, candidate, self.policy)
        metrics.update(semantic_pass=verdict.decision == "pass", verifier=verdict.verifier,
                       semantic_reason=verdict.reason)
        if verdict.decision != "pass":
            return result(base, selected, verdict.reason, candidate)
        if self.mode == "shadow":
            return result(legacy_text, "legacy", "shadow_candidate_verified", candidate)
        metrics["adopted"] = True
        return result(candidate, "candidate", "verified", candidate)


def recheck_before_send(prepared, current_frame, guard, *, wall_clock=time.time):
    """Call immediately before the original outbox enqueue/commit, not just after LLM."""
    if prepared.frame_fingerprint != current_frame.fingerprint:
        raise StaleFrame("frame_changed_before_send")
    if current_frame.data["expires_at"] <= wall_clock():
        raise StaleFrame("source_expired_before_send")
    guard(current_frame)
    return prepared.text
