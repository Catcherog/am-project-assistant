"""Small execution policy for optional read parallelism and typed evidence.

This is not a second agent loop. The existing AM registry remains responsible for
schema validation and scope construction. External effects are deliberately not
executed here: they must use AM's versioned preview/approval/outbox transaction.
"""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, wait
from contextvars import copy_context
from dataclasses import asdict, dataclass, field, replace
import json
import math
import re
import time
from typing import Any, Callable, Mapping, Sequence


class ToolPolicyError(ValueError):
    pass


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)


@dataclass(frozen=True)
class SourceEvidence:
    source_id: str
    project_id: str | None
    version: str
    observed_at: float
    expires_at: float
    evidence_state: str
    source_uri: str = ""

    def __post_init__(self) -> None:
        if not self.source_id or not self.version or self.evidence_state not in {"confirmed", "reported", "missing", "unknown"}:
            raise ToolPolicyError("INVALID_EVIDENCE")
        if any(type(t) not in (int, float) or not math.isfinite(t) or t <= 0 for t in (self.observed_at, self.expires_at)) or self.expires_at < self.observed_at:
            raise ToolPolicyError("INVALID_EVIDENCE_TIME")


@dataclass(frozen=True)
class ToolInvocation:
    call_id: str
    name: str
    arguments: Mapping[str, Any]
    depends_on: tuple[str, ...] = ()


@dataclass(frozen=True)
class ToolResult:
    call_id: str
    name: str
    status: str
    data: Any = None
    sources: tuple[SourceEvidence, ...] = ()
    error_code: str | None = None
    execution_state: str = "not_started"
    verified: bool = False

    def __post_init__(self) -> None:
        if self.status not in {"succeeded", "failed", "unknown", "requires_approval", "not_run", "stale", "result_too_large"}:
            raise ToolPolicyError("INVALID_TOOL_STATUS")
        if self.execution_state not in {"not_started", "completed", "may_continue"}:
            raise ToolPolicyError("INVALID_EXECUTION_STATE")
        if type(self.verified) is not bool:
            raise ToolPolicyError("INVALID_VERIFIED_FLAG")

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ToolSpec:
    name: str
    effect: str
    handler: Callable[[ToolInvocation], ToolResult | Any]
    parallel_safe: bool = False
    independent: bool = False
    max_result_bytes: int = 16384

    def __post_init__(self) -> None:
        if not re.fullmatch(r"[a-z][a-z0-9_]{0,63}", self.name):
            raise ToolPolicyError("INVALID_TOOL_NAME")
        if self.effect not in {"read", "local", "external"}:
            raise ToolPolicyError("INVALID_EFFECT")
        if (self.parallel_safe or self.independent) and self.effect != "read":
            raise ToolPolicyError("ONLY_READS_MAY_BE_PARALLEL")
        if type(self.parallel_safe) is not bool or type(self.independent) is not bool or type(self.max_result_bytes) is not int or self.max_result_bytes < 256:
            raise ToolPolicyError("INVALID_TOOL_CONFIGURATION")


class ToolExecutor:
    def __init__(self, *, max_workers: int = 3, max_calls: int = 16,
                 max_arguments_bytes: int = 32768, clock: Callable[[], float] = time.monotonic) -> None:
        if type(max_workers) is not int or not 1 <= max_workers <= 16 or type(max_calls) is not int or not 1 <= max_calls <= 64:
            raise ToolPolicyError("INVALID_EXECUTOR_CONFIGURATION")
        self.max_workers, self.max_calls = max_workers, max_calls
        self.max_arguments_bytes, self.clock = max_arguments_bytes, clock

    @staticmethod
    def _failure(call: ToolInvocation, code: str, *, status: str = "failed", execution_state: str = "not_started") -> ToolResult:
        return ToolResult(call.call_id, call.name, status, error_code=code, execution_state=execution_state)

    def _one(self, call: ToolInvocation, spec: ToolSpec, *, authorize: Callable[[ToolSpec, ToolInvocation], bool],
             guard: Callable[[], None], deadline: float | None) -> ToolResult:
        if deadline is not None and self.clock() >= deadline:
            return self._failure(call, "DEADLINE_BEFORE_DISPATCH", status="not_run")
        try:
            guard()
        except Exception:
            return self._failure(call, "FENCE_REJECTED", status="stale")
        try:
            allowed = authorize(spec, call)
        except Exception:
            return self._failure(call, "AUTHORIZATION_FAILED")
        if allowed is not True:
            return self._failure(call, "ACCESS_DENIED")
        if spec.effect == "external":
            return ToolResult(call.call_id, call.name, "requires_approval", data={"preview_required": True})
        try:
            raw = spec.handler(call)
        except Exception:
            return self._failure(call, "TOOL_HANDLER_ERROR", status="unknown" if spec.effect == "local" else "failed", execution_state="completed")
        try:
            guard()
            if authorize(spec, call) is not True:
                raise ToolPolicyError("ACCESS_REVOKED")
        except Exception:
            return self._failure(call, "FENCE_CHANGED_AFTER_LOCAL_EFFECT" if spec.effect == "local" else "RESULT_VISIBILITY_REVOKED",
                                 status="unknown" if spec.effect == "local" else "stale", execution_state="completed")
        if deadline is not None and self.clock() >= deadline:
            return self._failure(call, "DEADLINE_AFTER_EXECUTION", status="unknown" if spec.effect == "local" else "stale", execution_state="completed")
        if isinstance(raw, ToolResult):
            if raw.call_id != call.call_id or raw.name != call.name:
                return self._failure(call, "TOOL_RESULT_ID_MISMATCH", status="unknown" if spec.effect == "local" else "failed", execution_state="completed")
            # A returned handler may itself have timed out an upstream request.
            # Its explicit may_continue/not_started evidence must not be erased
            # merely because this Python wrapper returned synchronously.
            result = raw
        else:
            # Success means the read/function returned. Its business facts are
            # not automatically verified or promoted to confirmed evidence.
            result = ToolResult(call.call_id, call.name, "succeeded", data=raw, execution_state="completed", verified=False)
        try:
            payload = _json(result.to_dict())
        except (TypeError, ValueError):
            return self._failure(call, "TOOL_RESULT_NOT_JSON", status="unknown" if spec.effect == "local" else "failed", execution_state="completed")
        if len(payload.encode("utf-8")) > spec.max_result_bytes:
            return self._failure(call, "RESULT_TOO_LARGE_USE_PAGING", status="result_too_large", execution_state="completed")
        # Detach mutable handler output so later writes cannot silently change a
        # completed result after validation. SourceEvidence is frozen.
        return replace(result, data=json.loads(_json(result.data)))

    def execute(self, calls: Sequence[ToolInvocation], *,
                resolve: Callable[[str, Mapping[str, Any]], ToolSpec],
                authorize: Callable[[ToolSpec, ToolInvocation], bool],
                guard: Callable[[], None], deadline: float | None = None) -> tuple[ToolResult, ...]:
        """Run a bounded batch, returning results in input order.

        ``resolve`` must use the existing AM ToolRegistry.resolve so schema and
        project checks stay authoritative. Dependencies must precede consumers.
        Only consecutive, explicitly independent reads run in parallel. A timeout
        discards a read's eventual output; it does not terminate its Python thread.
        The caller must pass deadlines to its actual I/O ports as well.
        """
        if len(calls) > self.max_calls:
            raise ToolPolicyError("TOO_MANY_CALLS")
        if deadline is not None and (type(deadline) not in (int, float) or not math.isfinite(deadline)):
            raise ToolPolicyError("INVALID_DEADLINE")
        normalized: list[ToolInvocation] = []
        seen: set[str] = set()
        for call in calls:
            if not isinstance(call.call_id, str) or not call.call_id or len(call.call_id) > 256 or call.call_id in seen:
                raise ToolPolicyError("INVALID_OR_DUPLICATE_CALL_ID")
            if not isinstance(call.name, str) or not re.fullmatch(r"[a-z][a-z0-9_]{0,63}", call.name):
                raise ToolPolicyError("INVALID_TOOL_NAME")
            if any(dep not in seen for dep in call.depends_on) or len(set(call.depends_on)) != len(call.depends_on):
                raise ToolPolicyError("INVALID_CALL_GRAPH")
            if not isinstance(call.arguments, Mapping):
                raise ToolPolicyError("ARGUMENTS_MUST_BE_OBJECT")
            try:
                args_json = _json(dict(call.arguments))
            except (TypeError, ValueError) as exc:
                raise ToolPolicyError("ARGUMENTS_MUST_BE_JSON") from exc
            if len(args_json.encode("utf-8")) > self.max_arguments_bytes:
                raise ToolPolicyError("ARGUMENTS_TOO_LARGE")
            normalized.append(ToolInvocation(call.call_id, call.name, json.loads(args_json), tuple(call.depends_on)))
            seen.add(call.call_id)

        # Schema resolution is a read-only registry operation, never a handler.
        specs: dict[str, ToolSpec] = {}
        results: dict[str, ToolResult] = {}
        for call in normalized:
            try:
                spec = resolve(call.name, call.arguments)
                if not isinstance(spec, ToolSpec) or spec.name != call.name:
                    raise ToolPolicyError("REGISTRY_SPEC_MISMATCH")
                specs[call.call_id] = spec
            except Exception:
                results[call.call_id] = self._failure(call, "TOOL_RESOLUTION_REJECTED")

        def eligible(call: ToolInvocation) -> bool:
            spec = specs.get(call.call_id)
            return bool(spec and spec.effect == "read" and spec.parallel_safe and spec.independent and not call.depends_on)

        i = 0
        while i < len(normalized):
            call = normalized[i]
            if call.call_id in results:
                i += 1
                continue
            if any(results[dep].status != "succeeded" for dep in call.depends_on):
                results[call.call_id] = self._failure(call, "DEPENDENCY_UNSATISFIED", status="not_run")
                i += 1
                continue
            if eligible(call) and self.max_workers > 1:
                group: list[ToolInvocation] = []
                while i < len(normalized) and eligible(normalized[i]):
                    group.append(normalized[i])
                    i += 1
                executor = ThreadPoolExecutor(max_workers=min(self.max_workers, len(group)), thread_name_prefix="am-read")
                futures = {executor.submit(copy_context().run, self._one, c, specs[c.call_id], authorize=authorize, guard=guard, deadline=deadline): c for c in group}
                remaining = None if deadline is None else max(0.0, deadline - self.clock())
                done, pending = wait(futures, timeout=remaining)
                for future in done:
                    c = futures[future]
                    try:
                        results[c.call_id] = future.result()
                    except Exception:
                        results[c.call_id] = self._failure(c, "EXECUTOR_ERROR", execution_state="completed")
                for future in pending:
                    c = futures[future]
                    cancelled = future.cancel()
                    results[c.call_id] = self._failure(c, "TOOL_TIMEOUT_EXECUTION_MAY_CONTINUE" if not cancelled else "DEADLINE_BEFORE_DISPATCH",
                                                      status="unknown" if not cancelled else "not_run", execution_state="may_continue" if not cancelled else "not_started")
                executor.shutdown(wait=False, cancel_futures=True)
            else:
                results[call.call_id] = self._one(call, specs[call.call_id], authorize=authorize, guard=guard, deadline=deadline)
                i += 1
        # One read may finish while another is still waiting. Revalidate at the
        # batch boundary so an earlier result cannot leak after cancellation or
        # a permission revocation during that wait. Atomic publication still
        # belongs to the host's fenced outbox transaction.
        for call in normalized:
            result = results[call.call_id]
            if result.status not in {"succeeded", "unknown", "result_too_large", "requires_approval"}:
                continue
            spec = specs[call.call_id]
            try:
                guard()
                if authorize(spec, call) is not True:
                    raise ToolPolicyError("ACCESS_REVOKED")
            except Exception:
                changed_local = spec.effect == "local" and result.execution_state != "not_started"
                results[call.call_id] = self._failure(
                    call, "FENCE_CHANGED_AFTER_LOCAL_EFFECT" if changed_local else "BATCH_RESULT_VISIBILITY_REVOKED",
                    status="unknown" if changed_local else "stale", execution_state=result.execution_state)
        return tuple(results[c.call_id] for c in normalized)
