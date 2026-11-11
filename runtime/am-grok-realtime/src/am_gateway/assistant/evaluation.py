"""R6 evidence-aware evaluation of assistant replay and delivery records.

This module only reads supplied evidence.  It never calls a model, sends a
message, changes business facts, or turns an evaluator's opinion into a fact.
The JSON contract is documented in docs/r6/群聊私聊评测方案.md.
"""
from __future__ import annotations

from collections import Counter, defaultdict
from copy import deepcopy
import json
import math
from pathlib import Path
import re
from statistics import median
import unicodedata
from typing import Any, Iterable


SCHEMA_VERSION = "am-assistant-eval/1"
LAYERS = {"offline", "real_delivery"}
CHANNELS = {"group", "private"}
SUCCESS_STATUSES = {"offline_success", "real_delivery_success"}
_ACK_ONLY = re.compile(
    r"^(?:收到|好的|好|明白|了解|已收到)[，,。.!！\s]*(?:(?:我|正在|这就|马上|已)?"
    r"(?:帮你|为你|来)?(?:查一下|查询|核实|处理|整理|排队|开始处理|稍后回复|稍等|马上处理)"
    r"[，,。.!！\s]*)*$"
)
_DELIVERY_METHODS = {"platform_api", "sdk", "human_device"}
_ASSERTION_METHODS = {"source_check", "deterministic", "independent_readback", "human_review"}


class EvaluationInputError(ValueError):
    """The supplied report cannot be evaluated without changing its meaning."""


def _finite(value: Any) -> bool:
    return type(value) in (int, float) and math.isfinite(value)


def _text(value: Any) -> str:
    return unicodedata.normalize("NFKC", value).casefold() if isinstance(value, str) else ""


def _strict_equal(actual: Any, expected: Any) -> bool:
    # JSON booleans must not pass numeric expectations through True == 1.
    return json.dumps(actual, sort_keys=True, ensure_ascii=False) == json.dumps(
        expected, sort_keys=True, ensure_ascii=False
    )


def _nonempty(value: Any) -> bool:
    return isinstance(value, str) and bool(value.strip())


def _reject_constant(value: str) -> None:
    raise EvaluationInputError("non-finite JSON value: " + value)


def load_jsonl(path: str | Path) -> list[dict[str, Any]]:
    records = []
    for number, line in enumerate(Path(path).read_text(encoding="utf-8").splitlines(), 1):
        if not line.strip():
            continue
        try:
            record = json.loads(line, parse_constant=_reject_constant)
        except (json.JSONDecodeError, EvaluationInputError) as exc:
            raise EvaluationInputError(f"{path}:{number}: {exc}") from exc
        if not isinstance(record, dict):
            raise EvaluationInputError(f"{path}:{number}: record must be an object")
        records.append(record)
    return records


def validate_cases(cases: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    result, identifiers = [], set()
    for case in cases:
        if not isinstance(case, dict) or not _nonempty(case.get("case_id")):
            raise EvaluationInputError("case_id is required")
        if case["case_id"] in identifiers:
            raise EvaluationInputError("duplicate case_id: " + case["case_id"])
        identifiers.add(case["case_id"])
        if case.get("channel") not in CHANNELS or not _nonempty(case.get("intent")):
            raise EvaluationInputError("case channel and intent are required: " + case["case_id"])
        if not _nonempty(case.get("source_version")) or not isinstance(case.get("oracle"), dict):
            raise EvaluationInputError("case source_version and oracle are required: " + case["case_id"])
        oracle = case["oracle"]
        for key in ("required_facts", "required_text", "required_qualifiers", "timing_budget_s"):
            if key in oracle and not isinstance(oracle[key], dict):
                raise EvaluationInputError(f"{case['case_id']}: {key} must be an object")
        for key in ("forbidden_text", "required_subtasks", "required_assertions", "required_artifact_kinds",
                    "allowed_kinds", "expected_retrieval_status", "required_metrics"):
            if key in oracle and (not isinstance(oracle[key], list)
                                  or not all(_nonempty(item) for item in oracle[key])):
                raise EvaluationInputError(f"{case['case_id']}: {key} must contain strings")
        for key in ("required_text", "required_qualifiers"):
            for name, alternatives in oracle.get(key, {}).items():
                if not _nonempty(name) or not isinstance(alternatives, list) or not alternatives \
                        or not all(_nonempty(item) for item in alternatives):
                    raise EvaluationInputError(f"{case['case_id']}: invalid {key} alternatives")
        for name, budget in oracle.get("timing_budget_s", {}).items():
            if not _nonempty(name) or not _finite(budget) or budget <= 0:
                raise EvaluationInputError(f"{case['case_id']}: invalid timing budget")
        if not any(oracle.get(key) for key in ("required_facts", "required_text", "required_qualifiers",
                                               "required_subtasks", "required_assertions")):
            raise EvaluationInputError("an empty oracle cannot prove success: " + case["case_id"])
        result.append(case)
    if not result:
        raise EvaluationInputError("at least one case is required")
    return result


def load_cases(path: str | Path) -> list[dict[str, Any]]:
    if str(path).endswith(".jsonl"):
        return validate_cases(load_jsonl(path))
    try:
        value = json.loads(Path(path).read_text(encoding="utf-8"), parse_constant=_reject_constant)
    except json.JSONDecodeError as exc:
        raise EvaluationInputError(str(exc)) from exc
    if isinstance(value, dict):
        if value.get("schema_version") != SCHEMA_VERSION:
            raise EvaluationInputError("unsupported case schema_version")
        value = value.get("cases")
    if not isinstance(value, list):
        raise EvaluationInputError("cases must be a list")
    return validate_cases(value)


def _stamp(value: Any, default_domain: str | None) -> tuple[float, str] | None:
    if isinstance(value, dict):
        at, domain = value.get("at"), value.get("clock_domain", default_domain)
    else:
        at, domain = value, default_domain
    if not _finite(at) or not _nonempty(domain):
        return None
    return float(at), domain


def _union_length(intervals: Iterable[tuple[float, float]]) -> float:
    intervals = sorted(intervals)
    if not intervals:
        return 0.0
    start, end = intervals[0]
    total = 0.0
    for next_start, next_end in intervals[1:]:
        if next_start <= end:
            end = max(end, next_end)
        else:
            total += end - start
            start, end = next_start, next_end
    return total + end - start


def timing_metrics(timing: Any, *, layer: str = "offline") -> dict[str, Any]:
    """Use only same-domain intervals; missing evidence stays None, not zero."""
    names = ("ack_s", "first_substantive_s", "completion_s", "attachment_s", "queue_s",
             "model_union_s", "model_sum_s", "retrieval_union_s", "accounted_union_s", "unattributed_s")
    result: dict[str, Any] = {name: None for name in names}
    issues: list[str] = []
    result["issues"] = issues
    if not isinstance(timing, dict):
        issues.append("missing_timing")
        return result
    domain = timing.get("clock_domain")
    events = timing.get("events", {})
    if not isinstance(events, dict):
        issues.append("invalid_events")
        return result

    def duration(start_name: str, end_name: str) -> float | None:
        start, end = _stamp(events.get(start_name), domain), _stamp(events.get(end_name), domain)
        if start is None or end is None:
            return None
        if start[1] != end[1]:
            issues.append(f"mixed_clock:{start_name}:{end_name}")
            return None
        if end[0] < start[0]:
            issues.append(f"reversed_time:{start_name}:{end_name}")
            return None
        return end[0] - start[0]

    endpoint = "final_visible" if layer == "real_delivery" else "completed"
    result["ack_s"] = duration("received", "ack_visible")
    result["first_substantive_s"] = duration("received", "first_substantive_visible")
    result["completion_s"] = duration("received", endpoint)
    result["attachment_s"] = duration("received", "attachment_visible")
    result["queue_s"] = duration("enqueued", "claimed")
    start, end = _stamp(events.get("received"), domain), _stamp(events.get(endpoint), domain)
    window_ok = start is not None and end is not None and start[1] == end[1] and end[0] >= start[0]
    spans = timing.get("spans")
    if not isinstance(spans, dict):
        issues.append("missing_spans")
        return result
    all_intervals, valid_categories = [], {}
    for category, values in spans.items():
        if not isinstance(values, list):
            issues.append("invalid_spans:" + category)
            valid_categories[category] = False
            continue
        intervals, valid = [], bool(window_ok)
        for span in values:
            if not isinstance(span, dict):
                valid = False
                continue
            span_domain = span.get("clock_domain", domain)
            a, b = _stamp(span.get("start"), span_domain), _stamp(span.get("end"), span_domain)
            if (not window_ok or a is None or b is None or a[1] != b[1] or a[1] != start[1]
                    or a[0] < start[0] or b[0] > end[0] or b[0] < a[0]):
                valid = False
                continue
            intervals.append((a[0], b[0]))
        valid_categories[category] = valid
        if not valid:
            issues.append("unusable_spans:" + category)
            continue
        all_intervals.extend(intervals)
        if category == "model":
            result["model_union_s"] = _union_length(intervals)
            result["model_sum_s"] = sum(b - a for a, b in intervals)
        elif category == "retrieval":
            result["retrieval_union_s"] = _union_length(intervals)
    # Both categories must be explicitly observed, including [] for zero calls.
    # Extra supplied categories also participate in the union, never in a sum.
    if (window_ok and {"model", "retrieval"}.issubset(valid_categories)
            and all(valid_categories.values())):
        result["accounted_union_s"] = _union_length(all_intervals)
        result["unattributed_s"] = result["completion_s"] - result["accounted_union_s"]
    return result


def _style_suspicions(text: str, oracle: dict[str, Any]) -> dict[str, Any]:
    flags = []
    maximum = oracle.get("style_max_chars", 600)
    if not _finite(maximum) or maximum <= 0:
        maximum = 600
    if len(text) > maximum:
        flags.append("longer_than_case_guideline")
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    if any(count > 1 for line, count in Counter(lines).items() if len(line) >= 8):
        flags.append("repeated_line")
    for phrase in ("作为一个人工智能", "作为一个AI", "首先非常感谢您", "综上所述，综上所述"):
        if phrase in text:
            flags.append("template_phrase:" + phrase)
    return {"status": "suspicions_only", "flags": flags, "human_score": None,
            "human_evaluation_performed": False}


def _delivery_checks(record: dict[str, Any], output: dict[str, Any], issues: list[str]) -> None:
    readback = record.get("readback")
    if not isinstance(readback, dict):
        issues.append("missing_independent_delivery_readback")
        return
    if (readback.get("independent") is not True or readback.get("visible") is not True
            or readback.get("method") not in _DELIVERY_METHODS or not _nonempty(readback.get("evidence_ref"))):
        issues.append("unverified_user_visible_delivery")
    for key in ("input_event_id", "conversation_id"):
        if not _nonempty(record.get(key)):
            issues.append("missing_delivery_identity:" + key)
    expected = {"channel": record.get("channel"), "conversation_id": record.get("conversation_id"),
                "origin_event_id": record.get("input_event_id")}
    for key, value in expected.items():
        if output.get(key) != value or readback.get(key) != value:
            issues.append("delivery_identity_mismatch:" + key)
    if not _nonempty(readback.get("reply_id")) or readback.get("reply_id") != output.get("reply_id"):
        issues.append("delivery_reply_mismatch")
    if readback.get("text") != output.get("text"):
        issues.append("delivery_text_mismatch")
    if readback.get("final_reply_count") != 1 or type(readback.get("final_reply_count")) is not int:
        issues.append("final_reply_count_not_one")
    if readback.get("duplicate_reply_count") != 0 or type(readback.get("duplicate_reply_count")) is not int:
        issues.append("duplicate_reply_or_count_unknown")


def _artifact_checks(record: dict[str, Any], oracle: dict[str, Any], layer: str, issues: list[str]) -> None:
    artifacts = record.get("artifacts")
    if not isinstance(artifacts, list):
        if oracle.get("required_artifact_kinds"):
            issues.append("missing_artifact_evidence")
        return
    for kind in oracle.get("required_artifact_kinds", []):
        candidates = [item for item in artifacts if isinstance(item, dict) and item.get("kind") == kind]
        if not candidates:
            issues.append("missing_artifact:" + kind)
            continue
        valid = False
        for item in candidates:
            digest = item.get("sha256")
            if not isinstance(digest, str) or not re.fullmatch(r"[a-f0-9]{64}", digest):
                continue
            if layer != "real_delivery":
                valid = True
                break
            readback = item.get("readback", {})
            if (isinstance(readback, dict) and readback.get("independent") is True
                    and readback.get("visible") is True and readback.get("sha256") == digest
                    and readback.get("method") in _DELIVERY_METHODS and _nonempty(readback.get("evidence_ref"))
                    and readback.get("channel") == record.get("channel")
                    and readback.get("conversation_id") == record.get("conversation_id")
                    and readback.get("origin_event_id") == record.get("input_event_id")):
                valid = True
                break
        if not valid:
            issues.append("artifact_not_verified:" + kind)


def evaluate_record(record: dict[str, Any], case: dict[str, Any], *, expected_layer: str | None = None) -> dict[str, Any]:
    """Evaluate one attempt, preserving the complete supplied record verbatim."""
    issues: list[str] = []
    layer = record.get("layer", "offline")
    oracle = case["oracle"]
    for field in ("case_id", "channel", "intent", "source_version"):
        if record.get(field) != case.get(field):
            issues.append("case_binding_mismatch:" + field)
    if record.get("cold_or_warm") not in {"cold", "warm", "unknown"}:
        issues.append("invalid_cold_or_warm")
    if layer not in LAYERS:
        issues.append("invalid_layer")
    if expected_layer is not None and layer != expected_layer:
        issues.append("unexpected_evidence_layer")
    if record.get("schema_version", SCHEMA_VERSION) != SCHEMA_VERSION:
        issues.append("unsupported_record_schema")
    output = record.get("output") if isinstance(record.get("output"), dict) else {}
    text = output.get("text") if isinstance(output.get("text"), str) else ""
    if not text.strip():
        issues.append("empty_output")
    kind = output.get("kind")
    allowed_kinds = oracle.get("allowed_kinds", ["answer"])
    ack_only = kind == "ack" or bool(_ACK_ONLY.fullmatch(text.strip()))
    if ack_only:
        issues.append("ack_is_not_substantive_completion")
    if kind not in allowed_kinds or kind in {"ack", "partial", "error"}:
        issues.append("incomplete_or_unexpected_output_kind")
    outcome = record.get("outcome") if isinstance(record.get("outcome"), dict) else {}
    outcome_status = outcome.get("status", "unknown")
    if outcome_status != "completed":
        issues.append("outcome:" + str(outcome_status))
    facts = output.get("facts") if isinstance(output.get("facts"), dict) else {}
    for field, expected in oracle.get("required_facts", {}).items():
        if field not in facts or not _strict_equal(facts[field], expected):
            issues.append("required_fact:" + field)
    for check_name in ("required_text", "required_qualifiers"):
        for field, alternatives in oracle.get(check_name, {}).items():
            if not any(_text(alternative) in _text(text) for alternative in alternatives):
                issues.append(check_name + ":" + field)
    for forbidden in oracle.get("forbidden_text", []):
        if _text(forbidden) in _text(text):
            issues.append("forbidden_claim:" + forbidden)
    subtasks = output.get("completed_subtasks")
    subtasks = subtasks if isinstance(subtasks, list) else []
    for required in oracle.get("required_subtasks", []):
        if required not in subtasks:
            issues.append("missing_subtask:" + required)
    statuses = oracle.get("expected_retrieval_status")
    if statuses and output.get("retrieval_status") not in statuses:
        issues.append("wrong_retrieval_status")
    assertions = record.get("assertions")
    assertions = assertions if isinstance(assertions, list) else []
    for identifier in oracle.get("required_assertions", []):
        matches = [item for item in assertions if isinstance(item, dict) and item.get("id") == identifier]
        if (len(matches) != 1 or matches[0].get("status") != "pass"
                or matches[0].get("method") not in _ASSERTION_METHODS
                or not _nonempty(matches[0].get("evidence_ref"))):
            issues.append("unverified_assertion:" + identifier)
    if "duplicate_reply_count" in outcome and outcome["duplicate_reply_count"] != 0:
        issues.append("duplicate_reply")
    if layer == "real_delivery":
        _delivery_checks(record, output, issues)
    _artifact_checks(record, oracle, layer, issues)
    timing = timing_metrics(record.get("timing"), layer=layer)
    timing["reported_elapsed_to_end_s"] = timing["completion_s"]
    if ack_only or kind in {"ack", "partial", "error"} or outcome_status != "completed":
        timing["completion_s"] = None
        if ack_only:
            timing["first_substantive_s"] = None
    for metric in oracle.get("required_metrics", []):
        if timing.get(metric) is None:
            issues.append("required_metric_unknown:" + metric)
    for metric, budget in oracle.get("timing_budget_s", {}).items():
        value = timing.get(metric)
        if value is None:
            issues.append("budget_metric_unknown:" + metric)
        elif value > budget:
            issues.append("timing_budget_exceeded:" + metric)
    if outcome_status == "not_run":
        status = "not_run"
    elif issues:
        status = "failed"
    else:
        status = str(layer) + "_success"
    return {"case_id": case["case_id"], "attempt_id": record.get("attempt_id", "1"),
            "channel": record.get("channel"), "intent": record.get("intent"), "layer": layer,
            "cold_or_warm": record.get("cold_or_warm"), "source_version": record.get("source_version"),
            "status": status, "outcome_status": outcome_status, "issues": issues, "timing": timing,
            "style": _style_suspicions(text, oracle), "record": deepcopy(record)}


def distribution(values: Iterable[float | None]) -> dict[str, Any]:
    numbers = sorted(float(value) for value in values if _finite(value))
    count = len(numbers)
    return {"n": count, "median": median(numbers) if count else None,
            "max": numbers[-1] if count else None,
            "p95": numbers[math.ceil(count * .95) - 1] if count >= 20 else None,
            "p95_status": "available" if count >= 20 else "insufficient_samples"}


def _summary(results: list[dict[str, Any]]) -> dict[str, Any]:
    counts = Counter(item["status"] for item in results)
    successes = [item for item in results if item["status"] in SUCCESS_STATUSES]
    outcomes = Counter(item.get("outcome_status", "unknown") for item in results)
    return {"denominator": len(results), "successes": len(successes),
            "success_rate": len(successes) / len(results) if results else None,
            "status_counts": dict(counts), "outcome_counts": dict(outcomes),
            "successful_completion_s": distribution(item.get("timing", {}).get("completion_s") for item in successes),
            "all_observed_elapsed_s": distribution(item.get("timing", {}).get("reported_elapsed_to_end_s") for item in results),
            "successful_first_substantive_s": distribution(item.get("timing", {}).get("first_substantive_s") for item in successes),
            "ack_s": distribution(item.get("timing", {}).get("ack_s") for item in results),
            "timing_unknown_attempts": sum(item.get("timing", {}).get("completion_s") is None for item in results)}


def evaluate_run(cases: Iterable[dict[str, Any]], records: Iterable[dict[str, Any]], *,
                 expected_layer: str | None = None) -> dict[str, Any]:
    cases = validate_cases(cases)
    if expected_layer is not None and expected_layer not in LAYERS:
        raise EvaluationInputError("unsupported expected_layer")
    cases_by_id = {case["case_id"]: case for case in cases}
    records = list(records)
    results, covered, attempted_cases, attempts = [], set(), set(), defaultdict(list)
    for index, record in enumerate(records):
        if not isinstance(record, dict):
            raise EvaluationInputError(f"record {index + 1} must be an object")
        identifier = record.get("case_id")
        if not isinstance(identifier, str) or identifier not in cases_by_id:
            results.append({"case_id": identifier, "attempt_id": record.get("attempt_id", "1"),
                            "channel": record.get("channel", "unknown"), "intent": record.get("intent", "unknown"),
                            "layer": record.get("layer", "unknown"), "cold_or_warm": record.get("cold_or_warm", "unknown"),
                            "status": "failed", "outcome_status": "unknown", "issues": ["unknown_case"],
                            "timing": {}, "record": deepcopy(record)})
            continue
        covered.add(identifier)
        result = evaluate_record(record, cases_by_id[identifier], expected_layer=expected_layer)
        if result["outcome_status"] != "not_run":
            attempted_cases.add(identifier)
        attempt_id = result["attempt_id"]
        if not isinstance(attempt_id, str) or not attempt_id.strip():
            result["issues"].append("invalid_attempt_id")
            result["status"] = "failed"
            attempt_id = f"invalid-{index}"
        attempts[(identifier, attempt_id)].append(result)
        results.append(result)
    for duplicated in attempts.values():
        if len(duplicated) > 1:
            for result in duplicated:
                result["issues"].append("duplicate_attempt_identity")
                result["status"] = "failed"
    for identifier, case in cases_by_id.items():
        if identifier not in covered:
            results.append({"case_id": identifier, "attempt_id": None, "channel": case["channel"],
                            "intent": case["intent"], "layer": expected_layer or "unknown", "cold_or_warm": "unknown",
                            "source_version": case["source_version"], "status": "not_run", "outcome_status": "not_run",
                            "issues": ["no_record_for_planned_case"], "timing": {}, "record": None})
    groups = defaultdict(list)
    for item in results:
        key = "|".join(str(item.get(name, "unknown")) for name in ("layer", "channel", "intent", "cold_or_warm"))
        groups[key].append(item)
    summary = _summary(results)
    summary.update({"planned_cases": len(cases), "observed_records": len(records),
                    "cases_with_records": len(covered), "cases_attempted": len(attempted_cases),
                    "missing_case_records": len(cases) - len(covered),
                    "unrun_cases": len(cases) - len(attempted_cases)})
    return {"schema_version": SCHEMA_VERSION, "summary": summary,
            "strata": {key: _summary(values) for key, values in sorted(groups.items())},
            "ci_pass": all(item["status"] in SUCCESS_STATUSES for item in results),
            "results": results,
            "limits": ["Offline success is not real delivery, phone, or independent human acceptance.",
                       "Readback and external assertion provenance is supplied evidence; this runner makes no external verification call.",
                       "Style flags are automatic suspicions, not human scores."]}


def compare_runs(cases: Iterable[dict[str, Any]], baseline_records: Iterable[dict[str, Any]],
                 candidate_records: Iterable[dict[str, Any]], *, expected_layer: str | None = None) -> dict[str, Any]:
    cases = validate_cases(cases)
    baseline = evaluate_run(cases, baseline_records, expected_layer=expected_layer)
    candidate = evaluate_run(cases, candidate_records, expected_layer=expected_layer)
    left = {(item["case_id"], item["attempt_id"]): item for item in baseline["results"]}
    right = {(item["case_id"], item["attempt_id"]): item for item in candidate["results"]}
    pairs, deltas, gains = [], [], []
    for key in left.keys() & right.keys():
        a, b = left[key], right[key]
        same_conditions = all(a.get(field) == b.get(field) for field in
                              ("channel", "intent", "source_version", "cold_or_warm", "layer"))
        both_success = a["status"] in SUCCESS_STATUSES and b["status"] in SUCCESS_STATUSES
        comparable = same_conditions and both_success
        before, after = a.get("timing", {}).get("completion_s"), b.get("timing", {}).get("completion_s")
        delta = after - before if comparable and _finite(before) and _finite(after) else None
        if delta is not None:
            deltas.append(delta)
            if before > 0:
                gains.append((before - after) / before)
        pairs.append({"case_id": key[0], "attempt_id": key[1], "baseline_status": a["status"],
                      "candidate_status": b["status"], "same_conditions": same_conditions,
                      "latency_comparable": comparable and delta is not None, "candidate_minus_baseline_s": delta})
    return {"schema_version": SCHEMA_VERSION, "baseline": baseline, "candidate": candidate,
            "ci_pass": candidate["ci_pass"],
            "comparison": {"paired": sorted(pairs, key=lambda pair: (str(pair["case_id"]), str(pair["attempt_id"]))),
                           "baseline_only_attempts": len(left.keys() - right.keys()),
                           "candidate_only_attempts": len(right.keys() - left.keys()),
                           "paired_completion_delta_s": distribution(deltas),
                           "paired_relative_speedup": distribution(gains),
                           "speedup_status": "paired_successes_only" if deltas else "not_demonstrated"}}
