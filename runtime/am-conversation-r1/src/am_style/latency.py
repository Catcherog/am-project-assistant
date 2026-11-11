"""Latency accounting with explicit populations and missing stage measurements."""
from __future__ import annotations

import math
import time

STAGES = {
    "queue_wait": ("enqueued", "claimed"),
    "model_load": ("load_started", "load_finished"),
    "inference": ("inference_started", "inference_finished"),
    "verification": ("verification_started", "verification_finished"),
    "render": ("render_started", "render_finished"),
    "outbox": ("outbox_started", "outbox_committed"),
    "end_to_end": ("received", "completed"),
}


class LatencyTrace:
    def __init__(self, *, clock_id, clock=time.monotonic_ns):
        if not isinstance(clock_id, str) or not clock_id:
            raise ValueError("one explicit host/boot clock identity is required")
        self.clock_id, self.clock, self.events = clock_id, clock, {}

    def mark(self, event):
        if event in self.events:
            raise ValueError("timing event already recorded")
        self.events[event] = self.clock()

    def report(self):
        values = {}
        for stage, (start, end) in STAGES.items():
            a, b = self.events.get(start), self.events.get(end)
            if a is None or b is None:
                values[stage + "_ms"] = None
            elif type(a) is not int or type(b) is not int or b < a:
                raise ValueError("invalid monotonic span")
            else:
                values[stage + "_ms"] = (b - a) / 1_000_000
        return {"schema": "am-latency/4", "clock_id": self.clock_id,
                "events_ns": dict(self.events), **values,
                "missing_stages": [k for k, v in values.items() if v is None]}


def quantile(values, fraction):
    if not values:
        return None
    values = sorted(values)
    position = (len(values) - 1) * fraction
    low, high = math.floor(position), math.ceil(position)
    return values[low] + (values[high] - values[low]) * (position - low)


def summarize_latency(rows):
    """No slow-case removal; cold/warm only when explicitly observed."""
    result = {}
    for arm in sorted({row["arm"] for row in rows}):
        arm_rows = [row for row in rows if row["arm"] == arm]
        attempted = [row for row in arm_rows if row.get("attempted") is True]
        values, missing, invalid = [], 0, 0
        for row in attempted:
            value = row.get("duration_ms")
            if value is None:
                missing += 1
            elif isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value < 0:
                invalid += 1
            else:
                values.append(float(value))
        stages = {}
        for stage in STAGES:
            observed = []
            for row in attempted:
                value = row.get("timing", {}).get(stage + "_ms")
                if not isinstance(value, bool) and isinstance(value, (int, float)) and math.isfinite(value) and value >= 0:
                    observed.append(float(value))
            stages[stage] = {"measured": len(observed), "p50_ms": quantile(observed, .5),
                             "p95_ms": quantile(observed, .95)}
        result[arm] = {"eligible_rows": len(arm_rows), "attempted": len(attempted),
                       "latency_n": len(values), "missing_duration": missing, "invalid_duration": invalid,
                       "p50_ms": quantile(values, .5), "p95_ms": quantile(values, .95),
                       "max_ms": max(values) if values else None,
                       "cold_observed": sum(row.get("load_state") == "cold" for row in attempted),
                       "warm_observed": sum(row.get("load_state") == "warm" for row in attempted),
                       "load_state_unknown": sum(row.get("load_state") not in {"cold", "warm"} for row in attempted),
                       "stages": stages, "quantile_method": "linear interpolation over all measured attempts"}
    return result
