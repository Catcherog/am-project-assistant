"""Frozen denominators. Candidate generation, verified adoption and delivery differ."""
import math
from collections import defaultdict


def percentile(values, fraction):
    xs = sorted(values)
    if not xs:
        return None
    position = (len(xs)-1)*fraction
    a, b = math.floor(position), math.ceil(position)
    return xs[a] + (xs[b]-xs[a])*(position-a)


def summarize(rows, *, expected_cases, arms):
    expected = {(case, arm) for case in expected_cases for arm in arms}
    indexed = {}
    for row in rows:
        key = (row["case_id"], row["arm"])
        if key not in expected or key in indexed:
            raise ValueError("unexpected_or_duplicate_observation")
        indexed[key] = row
    if set(indexed) != expected:
        raise ValueError("missing_observation_must_be_explicit")
    result = {"expected_cases_per_arm": len(expected_cases), "arms": {}}
    for arm in arms:
        arm_rows = [indexed[(case, arm)] for case in expected_cases]
        counts = {field: sum(r.get(field) is True for r in arm_rows)
                  for field in ("eligible", "attempted", "schema_pass", "semantic_pass", "adopted")}
        counts["available_candidates"] = sum(bool(r.get("candidate_text")) for r in arm_rows)
        counts["delivered_with_receipt"] = sum(
            r.get("delivered") is True and bool(r.get("delivery_receipt")) for r in arm_rows)
        reviews = [r["human_review"] for r in arm_rows if isinstance(r.get("human_review"), dict)]
        human = [r for r in reviews if r.get("source") == "human" and r.get("reviewer")]
        counts["human_reviewed"] = len(human)
        counts["human_acceptable"] = sum(r.get("acceptable") is True for r in human)
        counts["critical_errors"] = sum(r.get("critical_error") is True for r in human)
        counts["human_acceptance_rate"] = (counts["human_acceptable"]/len(human)) if human else None
        duration = [r["duration_ms"] for r in arm_rows if r.get("attempted") is True
                    and isinstance(r.get("duration_ms"), (float, int))]
        counts["latency_n"] = len(duration)
        counts["p50_ms"] = percentile(duration, 0.5)
        counts["p95_ms"] = percentile(duration, 0.95)
        counts["adoption_rate_all"] = counts["adopted"]/len(expected_cases) if expected_cases else None
        counts["adoption_rate_eligible"] = counts["adopted"]/counts["eligible"] if counts["eligible"] else None
        result["arms"][arm] = counts
    return result


def paired_preferences(pairs):
    """No model ranking when one side was missing or its identity wasn't joined."""
    result = {"eligible_pairs": 0, "left_wins": 0, "right_wins": 0,
              "ties": 0, "both_bad": 0, "excluded": 0}
    for row in pairs:
        if not (row.get("left_available") is True and row.get("right_available") is True
                and row.get("human") is True and row.get("identity_join_verified") is True):
            result["excluded"] += 1
            continue
        choice = row.get("choice")
        key = {"left": "left_wins", "right": "right_wins", "tie": "ties", "both_bad": "both_bad"}.get(choice)
        if key is None:
            result["excluded"] += 1
            continue
        result["eligible_pairs"] += 1
        result[key] += 1
    n = result["left_wins"] + result["right_wins"]
    result["decisive_pairs"] = n
    result["left_win_rate_decisive"] = result["left_wins"]/n if n else None
    return result
