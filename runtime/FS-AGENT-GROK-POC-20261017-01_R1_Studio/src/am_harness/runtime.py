"""Durable turn scheduling around the existing engine, with no new provider.

The lane serializes model-bearing AM turns managed by THIS harness. The existing
ModelCoordinator remains the final arbiter for all Qwen callers, including
background jobs and other processes. A lease is not proof that a model stopped.
"""
from __future__ import annotations

from dataclasses import dataclass
import json
import re
import threading
from contextvars import ContextVar
from typing import Any, Callable, Protocol

from .config import HarnessConfig
from .store import DurableStore, Scope, RunSnapshot, Lease, StoreError


_execution = ContextVar('am_harness_execution_stage', default=None)


class LocalPreparationRefusal(RuntimeError):
    def __init__(self, code, proof):
        self.code, self.proof = code, proof
        super().__init__(code)


def mark_dispatch(kind):
    state = _execution.get()
    if state is not None:
        state['dispatches'].append(kind)


def preparation_refusal(error):
    from .context import ContextError
    state = _execution.get()
    if isinstance(error, ContextError) and state is not None and not state['dispatches']:
        return LocalPreparationRefusal(str(error), state)
    return error


class Runner(Protocol):
    def run(self, snapshot: RunSnapshot) -> dict[str, Any]: ...


@dataclass(frozen=True)
class WorkResult:
    run_id: str | None
    status: str
    published: bool = False
    error: str | None = None


def _error_code(exc: BaseException) -> str:
    candidate = getattr(exc, "code", str(exc))
    return candidate if isinstance(candidate, str) and re.fullmatch(r"[A-Z][A-Z0-9_]{0,79}", candidate) else "RUNNER_FAILED"


class TaskRuntime:
    """Host-owned worker. No thread starts until run_once() is called.

    publish(connection, run, result) must use the SAME SQLite database and
    transaction to enqueue in the original outbox. It must do no network I/O.
    The worker does not invoke arbitrary model-selected external writes.
    """
    def __init__(self, store: DurableStore, runner: Runner, *,
                 authorize: Callable[[Scope, str], None],
                 publish: Callable[[Any, RunSnapshot, dict], None] | None = None,
                 config: HarnessConfig | None = None):
        self.store, self.runner = store, runner
        self.authorize, self.publish = authorize, publish
        self.config = config or HarnessConfig()
        if not callable(authorize):
            raise ValueError("RUNTIME_AUTHORIZATION_REQUIRED")

    def submit(self, scope: Scope, event_id: str, text: str, *,
               source_expires_at: float, priority: int = 0) -> RunSnapshot:
        if not self.config.enabled or not self.config.durable_queue_enabled:
            raise RuntimeError("HARNESS_QUEUE_DISABLED")
        self.authorize(scope, "submit")
        now = self.store.clock()
        expires = min(source_expires_at, now + self.config.content_ttl_seconds)
        return self.store.submit(scope, event_id, text,
                                 deadline=min(expires, now + self.config.execution_seconds),
                                 expires_at=expires, priority=priority)

    def run_once(self, worker_id: str) -> WorkResult:
        if not self.config.enabled or not self.config.durable_queue_enabled:
            raise RuntimeError("HARNESS_QUEUE_DISABLED")
        lease = self.store.claim(worker_id, lease_seconds=self.config.lease_seconds)
        if lease is None:
            return WorkResult(None, "idle")
        stopped, heartbeat_failed = threading.Event(), threading.Event()
        # Renew only while this in-process runner is alive. Failed renewal is
        # never converted into permission to steal another worker's lease.
        def renew() -> None:
            while not stopped.wait(self.config.heartbeat_seconds):
                try:
                    self.store.heartbeat(lease, lease_seconds=self.config.lease_seconds)
                except Exception:
                    heartbeat_failed.set()
                    return

        heartbeat = threading.Thread(target=renew, name="am-harness-heartbeat", daemon=True)
        heartbeat.start()
        runner_started = runner_returned = False
        execution = {"dispatches": []}
        execution_token = _execution.set(execution)
        try:
            self.authorize(lease.run.scope, "execute")
            self.store.guard(lease.fence)
            runner_started = True
            outcome = self.runner.run(lease.run)
            runner_returned = True
            if not isinstance(outcome, dict):
                raise RuntimeError("RUNNER_RESULT_INVALID")
            # Validate serializability before entering the commit callback.
            json.dumps(outcome, ensure_ascii=False, allow_nan=False)
            if heartbeat_failed.is_set():
                raise RuntimeError("HEARTBEAT_LOST")
            self.authorize(lease.run.scope, "publish")
            def commit(connection, current):
                if self.publish:
                    self.publish(connection, current, outcome)
            valid = self.store.finish(lease, result=outcome, on_commit=commit)
            return WorkResult(lease.run.id, "completed" if valid else "stale", bool(valid and self.publish))
        except BaseException as exc:
            code = _error_code(exc)
            # Known pre-dispatch refusals do not claim inference is running.
            # An arbitrary provider/worker exception is ambiguous and remains
            # quarantined for explicit reconciliation, never auto-replayed.
            safe_refusals = {"MODEL_BUSY", "AUTHORIZATION_DENIED", "ACCESS_DENIED",
                             "PROJECT_ACCESS_DENIED", "CONVERSATION_SCOPE_CHANGED"}
            fence_failures = {"STALE_FENCE", "RUN_CANCELLED", "RUN_EXPIRED", "RUN_DEADLINE_EXCEEDED",
                              "RUN_NOT_ACTIVE", "FENCE_STALE", "DEADLINE_EXCEEDED", "CONTENT_EXPIRED",
                              "HARNESS_DEADLINE_EXPIRED", "SOURCE_ACCESS_CHANGED"}
            if code == "STALE_LEASE":
                result = WorkResult(lease.run.id, "stale", error=code)
            elif code == "LEASE_QUARANTINED":
                result = WorkResult(lease.run.id, "unknown", error=code)
            elif (not runner_started or runner_returned or code in safe_refusals or code in fence_failures
                  or isinstance(exc, LocalPreparationRefusal) and exc.proof is execution and not execution["dispatches"]):
                try:
                    valid = self.store.finish(lease, state="failed", error=code)
                    result = WorkResult(lease.run.id, "failed" if valid else "stale", error=code)
                except StoreError as finish_error:
                    result = WorkResult(lease.run.id, "unknown" if finish_error.code == "LEASE_QUARANTINED" else "stale", error=code)
            else:
                try:
                    self.store.quarantine(lease, reason=code)
                    result = WorkResult(lease.run.id, "unknown", error=code)
                except StoreError as quarantine_error:
                    if quarantine_error.code != "STALE_LEASE":
                        raise
                    result = WorkResult(lease.run.id, "stale", error=code)
            if isinstance(exc, (KeyboardInterrupt, SystemExit)):
                raise
            return result
        finally:
            _execution.reset(execution_token)
            stopped.set()
            heartbeat.join(timeout=self.config.heartbeat_seconds + .5)
