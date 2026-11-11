"""Bounded metadata-only telemetry with no prompt, content, or identity fields."""
from __future__ import annotations

from collections import OrderedDict, deque
from contextlib import contextmanager
import hashlib
import json
import math
import os
from pathlib import Path
import re
import threading
import time
from typing import Any, Callable, Iterator


class TraceError(ValueError):
    pass


_EVENTS = frozenset({"run_start", "queued", "lease_acquired", "control", "span_start", "span_end",
                     "first_useful", "final", "run_state", "checkpoint_hit", "checkpoint_miss", "context_selection"})
_PHASES = frozenset({"queue", "model", "tool", "context", "validation", "delivery", "work", "recovery"})
_STATUSES = frozenset({"queued", "running", "succeeded", "failed", "unknown", "cancelled", "stale", "expired",
                       "waiting_input", "waiting_review", "ok", "error", "blocked", "rejected", "not_run"})
_NUMERIC_FIELDS = frozenset({"elapsed_ms", "queue_ms", "model_ms", "tool_ms", "delivery_ms", "input_units", "output_units",
                            "input_tokens", "output_tokens", "queue_depth", "revision", "cancel_epoch", "priority",
                            "call_count", "source_count", "retry_count", "result_bytes", "selected_notes", "omitted_budget_notes", "omitted_invalid_notes"})
_BOOL_FIELDS = frozenset({"exact_token_count", "replayed", "verified", "still_running"})


class MetadataTrace:
    """A local diagnostic sink, not the durable task journal or billing source.

    Run identifiers are hashed before emission. Numeric fields/constant labels
    are whitelisted. Unknown fields fail validation instead of accidentally
    logging user input. Sink I/O failure increments ``sink_errors`` and does not
    interrupt the business action. JSONL uses append + an advisory process lock.
    """
    def __init__(self, path: str | Path | None = None, *, max_records: int = 2048,
                 max_active_runs: int = 1024, clock: Callable[[], float] = time.monotonic,
                 wall_clock: Callable[[], float] = time.time, fsync: bool = False) -> None:
        if type(max_records) is not int or max_records < 1 or type(max_active_runs) is not int or max_active_runs < 1:
            raise TraceError("INVALID_TRACE_LIMIT")
        self.path = Path(path) if path is not None else None
        if self.path is not None:
            self.path.parent.mkdir(parents=True, exist_ok=True)
        self.clock, self.wall_clock, self.fsync = clock, wall_clock, fsync
        self._records: deque[dict[str, Any]] = deque(maxlen=max_records)
        self._runs: OrderedDict[str, dict[str, Any]] = OrderedDict()
        self._max_active_runs = max_active_runs
        self._lock = threading.RLock()
        self.sink_errors = 0

    @staticmethod
    def run_ref(run_id: str) -> str:
        if not isinstance(run_id, str) or not run_id or len(run_id) > 256:
            raise TraceError("INVALID_RUN_ID")
        return hashlib.sha256(run_id.encode("utf-8")).hexdigest()[:24]

    @staticmethod
    def _validate(fields: dict[str, Any]) -> None:
        allowed = _NUMERIC_FIELDS | _BOOL_FIELDS | {"phase", "status", "tool_name"}
        if set(fields) - allowed:
            raise TraceError("TRACE_FIELD_NOT_ALLOWED")
        for name, value in fields.items():
            if name in _NUMERIC_FIELDS and (type(value) not in (int, float) or not math.isfinite(value) or value < 0):
                raise TraceError("INVALID_TRACE_NUMBER")
            if name in _BOOL_FIELDS and type(value) is not bool:
                raise TraceError("INVALID_TRACE_BOOLEAN")
            if name == "phase" and value not in _PHASES:
                raise TraceError("INVALID_TRACE_PHASE")
            if name == "status" and value not in _STATUSES:
                raise TraceError("INVALID_TRACE_STATUS")
            if name == "tool_name" and (not isinstance(value, str) or not re.fullmatch(r"[a-z][a-z0-9_]{0,63}", value)):
                raise TraceError("INVALID_TOOL_LABEL")

    @property
    def records(self) -> tuple[dict[str, Any], ...]:
        with self._lock:
            return tuple(dict(row) for row in self._records)

    def emit(self, run_id: str, event: str, **fields: Any) -> dict[str, Any]:
        if event not in _EVENTS:
            raise TraceError("INVALID_TRACE_EVENT")
        self._validate(fields)
        record = {"schema": 1, "time": self.wall_clock(), "run_ref": self.run_ref(run_id), "event": event, **fields}
        encoded = (json.dumps(record, ensure_ascii=True, sort_keys=True, separators=(",", ":"), allow_nan=False) + "\n").encode("utf-8")
        with self._lock:
            self._records.append(record)
            if self.path is not None:
                try:
                    self._append(encoded)
                except OSError:
                    self.sink_errors += 1
        return dict(record)

    def _append(self, encoded: bytes) -> None:
        # AM's supported Studio/Linux environments provide fcntl. There is no
        # silent fallback to a potentially corrupt shared JSONL file on Windows.
        import fcntl
        fd = os.open(self.path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX)
            remaining = memoryview(encoded)
            while remaining:
                written = os.write(fd, remaining)
                if written <= 0:
                    raise OSError("trace write failed")
                remaining = remaining[written:]
            if self.fsync:
                os.fsync(fd)
        finally:
            os.close(fd)

    def start_run(self, run_id: str) -> None:
        ref = self.run_ref(run_id)
        with self._lock:
            if ref in self._runs:
                return
            while len(self._runs) >= self._max_active_runs:
                self._runs.popitem(last=False)
            self._runs[ref] = {"started": self.clock(), "first_useful": False, "final": False}
            self.emit(run_id, "run_start")

    @contextmanager
    def span(self, run_id: str, phase: str, **fields: Any) -> Iterator[None]:
        if set(fields).intersection({"elapsed_ms", "status"}):
            raise TraceError("SPAN_FIELD_RESERVED")
        self.emit(run_id, "span_start", phase=phase, **fields)
        started = self.clock()
        try:
            yield
        except BaseException:
            self.emit(run_id, "span_end", phase=phase, elapsed_ms=max(0.0, (self.clock() - started) * 1000), status="error", **fields)
            raise
        else:
            self.emit(run_id, "span_end", phase=phase, elapsed_ms=max(0.0, (self.clock() - started) * 1000), status="ok", **fields)

    def _milestone(self, run_id: str, event: str, **fields: Any) -> bool:
        if "elapsed_ms" in fields:
            raise TraceError("MILESTONE_FIELD_RESERVED")
        ref = self.run_ref(run_id)
        with self._lock:
            state = self._runs.get(ref)
            if state is None:
                self.start_run(run_id)
                state = self._runs[ref]
            if state[event]:
                return False
            state[event] = True
            self.emit(run_id, event, elapsed_ms=max(0.0, (self.clock() - state["started"]) * 1000), **fields)
            return True

    def mark_first_useful(self, run_id: str, **fields: Any) -> bool:
        """Call only after content is available to/delivered for the user.

        Queue acknowledgments and typing indicators do not count as useful
        content. The host decides the true delivery boundary.
        """
        return self._milestone(run_id, "first_useful", **fields)

    def mark_final(self, run_id: str, *, status: str, **fields: Any) -> bool:
        return self._milestone(run_id, "final", status=status, **fields)


def summarize(records: tuple[dict[str, Any], ...] | list[dict[str, Any]]) -> dict[str, Any]:
    """Descriptive local timings; no inferred accuracy or live performance gain."""
    def stats(values: list[float]) -> dict[str, float | int | None]:
        if not values:
            return {"n": 0, "mean_ms": None, "max_ms": None}
        return {"n": len(values), "mean_ms": sum(values) / len(values), "max_ms": max(values)}
    result = {}
    for event in ("first_useful", "final"):
        result[event] = stats([r["elapsed_ms"] for r in records if r.get("event") == event and "elapsed_ms" in r])
    result["phases"] = {phase: stats([r["elapsed_ms"] for r in records if r.get("event") == "span_end" and r.get("phase") == phase]) for phase in sorted(_PHASES)}
    return result
