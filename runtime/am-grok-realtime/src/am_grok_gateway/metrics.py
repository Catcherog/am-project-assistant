"""Endpoint-specific latency with full denominators; acknowledgements do not pass.

API send acknowledgement, platform readback and phone-visible timestamps are
different evidence. This module never substitutes one endpoint for another.
"""
from __future__ import annotations

import json
import math
import sqlite3
from collections import Counter
from pathlib import Path
from typing import Any

from .models import ContractError, finite_time


def read_records(path: str | Path) -> list[dict]:
    p = Path(path).resolve()
    if not p.is_file():
        raise FileNotFoundError("gateway database not found")
    db = sqlite3.connect(p.as_uri() + "?mode=ro", uri=True)
    db.row_factory = sqlite3.Row
    try:
        return [dict(row) for row in db.execute("SELECT * FROM gateway_requests ORDER BY created_at,rowid")]
    finally:
        db.close()


def percentile(values: list[float], probability: float) -> float | None:
    if not values:
        return None
    value = sorted(values)[max(0, math.ceil(len(values) * probability) - 1)]
    return round(value, 3) if math.isfinite(value) else None


def summarize(records: list[dict], *, visibility: list[dict] | None = None, minimum_samples: int = 100) -> dict[str, Any]:
    if type(minimum_samples) is not int or minimum_samples < 1:
        raise ContractError("invalid minimum sample size")
    observations = {}
    for item in visibility or []:
        if not isinstance(item, dict) or set(item) != {
            "request_id", "output_message_id", "visible_at", "useful_review", "evidence_ref",
        }:
            raise ContractError("invalid visibility observation")
        if type(item["useful_review"]) is not bool or not isinstance(item["evidence_ref"], str) or not item["evidence_ref"]:
            raise ContractError("visibility requires actual usefulness review and evidence reference")
        finite_time(item["visible_at"], "visible_at")
        if item["request_id"] in observations:
            raise ContractError("duplicate visibility observation")
        observations[item["request_id"]] = item
    known = {r["request_id"] for r in records}
    if set(observations) - known:
        raise ContractError("visibility references unknown request")
    groups = {}
    for label in sorted({r["measurement_class"] for r in records}):
        rows = [r for r in records if r["measurement_class"] == label]
        dispatch, ack, visible = [], [], []
        useful_sent, visible_count, ack_only, failed, samples = 0, 0, 0, 0, []
        for row in rows:
            useful = (row["status"] == "sent" and bool(row["useful"])
                      and row["result_origin"] == "bot" and not row["error_code"])
            d = (row["dispatch_at"] - row.get("received_at", row["created_at"])) * 1000 if row["dispatch_at"] is not None else math.inf
            a = (row["sent_at"] - row["event_created_at"]) * 1000 if useful and row["sent_at"] is not None else math.inf
            if d < 0 or a < 0:
                raise ContractError("inconsistent event timestamps")
            v = math.inf
            observation = observations.get(row["request_id"])
            if observation:
                if (not row["output_message_id"] or observation["output_message_id"] != row["output_message_id"]
                        or row["sent_at"] is None
                        or observation["visible_at"] < row.get("send_started_at", row["sent_at"])
                        or observation["visible_at"] < row["event_created_at"]):
                    raise ContractError("visibility evidence does not bind the actual output")
                if useful and observation["useful_review"]:
                    visible_count += 1
                    v = (observation["visible_at"] - row["event_created_at"]) * 1000
            dispatch.append(d)
            ack.append(a)
            visible.append(v)
            useful_sent += int(useful)
            ack_only += int(row["status"] == "sent" and not useful)
            failed += int(not useful)
            samples.append({
                "request_id": row["request_id"], "origin": row["origin"], "status": row["status"],
                "error_code": row["error_code"], "useful_sent": useful,
                "ingress_to_dispatch_ms": round(d, 3) if math.isfinite(d) else None,
                "event_to_send_ack_ms": round(a, 3) if math.isfinite(a) else None,
                "event_to_visible_ms": round(v, 3) if math.isfinite(v) else None,
            })
        enough = len(rows) >= minimum_samples
        p50, p95 = percentile(visible, .5), percentile(visible, .95)
        gateway_p95 = percentile(dispatch, .95)
        if label == "synthetic":
            verdict = "SIMULATION_ONLY"
        elif label != "ordinary":
            verdict = "SEPARATE_TASK_CLASS"
        elif not enough:
            verdict = "INSUFFICIENT_SAMPLES"
        elif visible_count < len(rows):
            verdict = "INCOMPLETE_OR_FAILED_VISIBLE_RESULTS"
        elif (p50 is not None and p95 is not None and p50 <= 5000 and p95 <= 10000
              and gateway_p95 is not None and gateway_p95 <= 1000):
            verdict = "OBSERVED_TARGET_MET_REQUIRES_NATIVE_REVIEW"
        else:
            verdict = "TARGET_NOT_MET"
        groups[label] = {
            "accepted_unique_requests": len(rows),
            "useful_sent": useful_sent, "non_answer_notices": ack_only,
            "failed_pending_unknown_or_not_useful": failed,
            "verified_visible_answers": visible_count,
            "states": dict(Counter(r["status"] for r in rows)),
            "ingress_to_dispatch_p95_ms": gateway_p95,
            "gateway_target_met": bool(enough and gateway_p95 is not None and gateway_p95 <= 1000),
            "event_to_send_ack_p50_ms": percentile(ack, .5),
            "event_to_send_ack_p95_ms": percentile(ack, .95),
            "event_to_visible_p50_ms": p50, "event_to_visible_p95_ms": p95,
            "latency_verdict": verdict, "samples": samples,
        }
    return {
        "schema": "am.grok.latency-report.v1",
        "production_acceptance": "NOT_INFERRED_FROM_CODE_OR_SIMULATION",
        "minimum_samples": minimum_samples,
        "targets": {"ordinary_visible_p50_ms": 5000, "ordinary_visible_p95_ms": 10000,
                    "ingress_to_dispatch_p95_ms": 1000},
        "percentile_rule": "nearest-rank; failed, missing and non-useful outcomes are infinity; null means unbounded or no samples",
        "visibility_rule": "actual output_message_id + user-visible time + usefulness review + evidence_ref; API acknowledgement is not visibility",
        "groups": groups,
    }
