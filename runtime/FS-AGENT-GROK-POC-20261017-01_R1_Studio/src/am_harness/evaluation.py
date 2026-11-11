"""Compare normalized evidence, never manufacture live benchmark measurements."""
from __future__ import annotations

from collections import defaultdict
import math
from typing import Any, Iterable


class EvaluationError(ValueError):
    pass


def _percentile(values: list[float], percentile: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    return ordered[max(0, math.ceil(percentile * len(ordered))-1)]


def analyze(records: Iterable[dict[str, Any]], *, minimum_pairs: int = 20) -> dict[str, Any]:
    """Each pair uses the same case, repetition and environment fingerprint.

    environment_id must cover model build/quantization, hardware, source version,
    cache condition and runtime configuration. The host is responsible for that
    fingerprint. Lower latency is not reported as a win if correctness regresses.
    first_useful_ms must be actual useful-content visibility/delivery latency,
    not a typing indicator or a local callback return.
    """
    if type(minimum_pairs) is not int or minimum_pairs < 1:
        raise EvaluationError("INVALID_MINIMUM_PAIRS")
    indexed = {}; synthetic = 0
    counters = ("unauthorized_reads", "duplicate_effects", "stale_publications")
    for record in records:
        required = {"case_id", "trial_id", "environment_id", "arm", "live_model", "live_transport",
                    "success", "first_useful_ms", "final_ms", "model_calls", *counters}
        if not isinstance(record, dict) or not required.issubset(record):
            raise EvaluationError("EVALUATION_RECORD_INCOMPLETE")
        for field in ("case_id", "trial_id", "environment_id"):
            if not isinstance(record[field], str) or not record[field]:
                raise EvaluationError("INVALID_EVIDENCE_ID")
        if record["arm"] not in ("baseline", "candidate"):
            raise EvaluationError("INVALID_EVALUATION_ARM")
        if any(type(record[f]) is not bool for f in ("live_model", "live_transport")):
            raise EvaluationError("INVALID_LIVE_FLAG")
        if record["success"] is not None and type(record["success"]) is not bool:
            raise EvaluationError("INVALID_SUCCESS_VALUE")
        for field in ("first_useful_ms", "final_ms"):
            value = record[field]
            if value is not None and (type(value) not in (int, float) or not math.isfinite(value) or value < 0):
                raise EvaluationError("INVALID_LATENCY")
        for field in (*counters, "model_calls"):
            if type(record[field]) is not int or record[field] < 0:
                raise EvaluationError("INVALID_COUNTER")
        key = (record["case_id"], record["trial_id"], record["environment_id"], record["arm"])
        if key in indexed:
            raise EvaluationError("DUPLICATE_EVALUATION_RECORD")
        indexed[key] = record
        if not (record["live_model"] and record["live_transport"]):
            synthetic += 1
    pairs=[]
    for key, baseline in indexed.items():
        if key[-1] != "baseline":
            continue
        candidate=indexed.get((*key[:-1],"candidate"))
        if candidate is not None and all(r["live_model"] and r["live_transport"] for r in (baseline,candidate)):
            pairs.append((baseline,candidate))
    all_candidate=[r for r in indexed.values() if r["arm"]=="candidate" and r["live_model"] and r["live_transport"]]
    violations={k:sum(r[k] for r in all_candidate) for k in counters}
    correctness_regressions=sum(a["success"] is True and b["success"] is not True for a,b in pairs)
    complete_pairs=[(a,b) for a,b in pairs if all(r["success"] is not None and r["first_useful_ms"] is not None and r["final_ms"] is not None for r in (a,b))]
    successful_pairs=[(a,b) for a,b in complete_pairs if a["success"] is True and b["success"] is True]
    summary={"paired_live_trials":len(pairs),"complete_live_pairs":len(complete_pairs),
             "successful_live_pairs":len(successful_pairs),
             "synthetic_or_nontransport_records_excluded":synthetic,"minimum_pairs":minimum_pairs,
             "violations":violations,"correctness_regressions":correctness_regressions,
             "status":"insufficient_evidence","latency":{},"performance_claim_allowed":False}
    if any(violations.values()) or correctness_regressions:
        summary["status"]="failed_invariants"
    elif len(successful_pairs)>=minimum_pairs:
        summary["status"]="comparable_live_evidence"
        summary["performance_claim_allowed"]=True
    for metric in ("first_useful_ms","final_ms"):
        arms={arm:[p[i][metric] for p in successful_pairs] for i,arm in enumerate(("baseline","candidate"))}
        stats={arm:{"n":len(values),"p50":_percentile(values,.5),"p95":_percentile(values,.95)} for arm,values in arms.items()}
        if summary["performance_claim_allowed"]:
            before=stats["baseline"]["p50"]; after=stats["candidate"]["p50"]
            stats["p50_change_percent"]=(after-before)/before*100 if before else None
        else:
            stats["p50_change_percent"]=None
        summary["latency"][metric]=stats
    return summary
