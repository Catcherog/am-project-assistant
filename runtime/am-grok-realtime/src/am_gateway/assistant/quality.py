"""Versioned, evidence-bound quality evaluation; never a sender or deployer.

Concept matching is a diagnostic check, not a general semantic oracle. Open
factual answers need a source-grounded review bound to the exact output bytes.
Model grading stays advisory. Missing layers and failed attempts remain visible.
"""
from __future__ import annotations

from collections import Counter, defaultdict
from copy import deepcopy
import hashlib
import json
import math
from pathlib import Path
import re
from statistics import median
import unicodedata

SCHEMA_VERSION = "am-assistant-quality/2"
LAYERS = ("offline", "real_model", "realtime", "phone", "human")
ASSERTION_METHODS = {"source_check", "deterministic", "independent_readback", "human_review"}
_ACK = re.compile(r"^(?:收到|好的|好|明白|了解|已收到)[，,。.!！\s]*(?:(?:我|正在|这就|马上|已)?(?:帮你|为你|来)?(?:查一下|查询|核实|处理|整理|排队|开始处理|稍后回复|稍等|马上处理)[，,。.!！\s]*)*$")


class QualityInputError(ValueError):
    pass


def _nonempty(value):
    return isinstance(value, str) and bool(value.strip())


def _finite(value):
    return type(value) in (int, float) and math.isfinite(value)


def normalize(value):
    value = unicodedata.normalize("NFKC", value).casefold() if isinstance(value, str) else ""
    return "".join(c for c in value if not (c.isspace() or unicodedata.category(c).startswith("P")))


def output_hash(text):
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def concept_matches(text, concept):
    """Any accepted wording, or all terms in an alternative, after normalization."""
    normalized = normalize(text)
    for alternative in concept.get("any_of", []):
        terms = [alternative] if isinstance(alternative, str) else alternative.get("all_of", [])
        if terms and all(normalize(term) in normalized for term in terms):
            return True
    return False


def validate_cases(cases):
    result, seen = [], set()
    for case in cases:
        if not isinstance(case, dict) or not _nonempty(case.get("case_id")):
            raise QualityInputError("case_id is required")
        if case["case_id"] in seen:
            raise QualityInputError("duplicate case_id: " + case["case_id"])
        seen.add(case["case_id"])
        for field in ("channel", "intent", "source_version"):
            if not _nonempty(case.get(field)):
                raise QualityInputError("missing case field: " + field)
        if case["channel"] not in {"group", "private"}:
            raise QualityInputError("unsupported channel")
        oracle = case.get("oracle")
        if not isinstance(oracle, dict) or not any(oracle.get(k) for k in (
                "concepts", "required_facts", "required_assertions", "required_subtasks")):
            raise QualityInputError("a nonempty oracle is required")
        for name, concept in oracle.get("concepts", {}).items():
            alternatives = concept.get("any_of") if isinstance(concept, dict) else None
            if not _nonempty(name) or not isinstance(alternatives, list) or not alternatives:
                raise QualityInputError("concept requires nonempty any_of")
            for alt in alternatives:
                terms = [alt] if isinstance(alt, str) else alt.get("all_of") if isinstance(alt, dict) else None
                if not isinstance(terms, list) or not terms or not all(_nonempty(x) for x in terms):
                    raise QualityInputError("invalid concept alternative")
        for pattern in oracle.get("forbidden_assertions", []):
            try:
                re.compile(pattern)
            except (re.error, TypeError) as exc:
                raise QualityInputError("invalid forbidden assertion regex") from exc
        for value in oracle.get("latency_targets_s", {}).values():
            if not _finite(value) or value <= 0:
                raise QualityInputError("latency targets must be finite and positive")
        result.append(case)
    if not result:
        raise QualityInputError("at least one planned case is required")
    return result


def _reject_constant(value):
    raise QualityInputError("non-finite JSON: " + value)


def load_suite(path):
    value = json.loads(Path(path).read_text(encoding="utf-8"), parse_constant=_reject_constant)
    if not isinstance(value, dict) or value.get("schema_version") != SCHEMA_VERSION:
        raise QualityInputError("unsupported suite schema")
    validate_cases(value.get("cases", []))
    return value


def load_records(path):
    records = []
    for line in Path(path).read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        value = json.loads(line, parse_constant=_reject_constant)
        if not isinstance(value, dict):
            raise QualityInputError("records must be objects")
        records.append(value)
    return records


def distribution(values):
    values = sorted(float(v) for v in values if _finite(v) and v >= 0)
    n = len(values)
    return {"n": n, "p50": median(values) if n else None,
            "p95": values[math.ceil(n * .95) - 1] if n >= 20 else None,
            "p99": values[math.ceil(n * .99) - 1] if n >= 100 else None,
            "max": values[-1] if n else None,
            "tail_status": "p99_available" if n >= 100 else "p95_only" if n >= 20 else "insufficient_tail_samples",
            "method": "nearest_rank; p95 needs n>=20, p99 needs n>=100"}


def _timing(record, output, substantive):
    raw = record.get("timing", {})
    events = raw.get("events", {}) if isinstance(raw, dict) else {}
    domain = raw.get("clock_domain") if isinstance(raw, dict) else None
    issues = []
    def stamp(name):
        value = events.get(name)
        at, clock = (value.get("at"), value.get("clock_domain", domain)) if isinstance(value, dict) else (value, domain)
        return (float(at), clock) if _finite(at) and _nonempty(clock) else None
    def elapsed(end):
        a, b = stamp("received"), stamp(end)
        if a is None or b is None:
            return None
        if a[1] != b[1] or b[0] < a[0]:
            issues.append("invalid_clock_interval:" + end)
            return None
        return b[0] - a[0]
    endpoint = ("phone_visible" if record.get("layer") in {"phone", "human"}
                else "final_visible" if record.get("layer") == "realtime" else "completed")
    completed = elapsed(endpoint)
    first = elapsed("phone_first_substantive_visible" if record.get("layer") in {"phone", "human"}
                    else "first_substantive_visible")
    # A timestamp alone does not establish that its content was substantive.
    first_text = output.get("first_substantive_text", "")
    if not _nonempty(first_text) or _ACK.fullmatch(first_text.strip()):
        first = None
    elif first_text != output.get("text"):
        review = record.get("first_substantive_review", {})
        if (not isinstance(review, dict) or not _assertion_valid(review, first_text, record.get("source_version"))
                or review.get("independent") is not True or review.get("method") not in {"source_check", "human_review"}):
            first = None
            issues.append("first_substantive_content_unverified")
    if first is not None and completed is not None and first > completed:
        issues.append("first_substantive_after_completion")
        first = None
    return {"ack_s": elapsed("ack_visible"), "processing_s": elapsed("completed"),
            "first_substantive_s": first if substantive else None,
            "completion_s": completed if substantive and isinstance(record.get("outcome"), dict)
                            and record["outcome"].get("status") == "completed" else None,
            "observed_elapsed_s": completed, "clock_domain": domain, "endpoint": endpoint, "issues": issues}


def _assertion_valid(item, text, source_version):
    return (item.get("status") == "pass" and item.get("method") in ASSERTION_METHODS
            and _nonempty(item.get("evidence_ref"))
            and item.get("output_sha256") == output_hash(text)
            and item.get("source_version") == source_version)


def _human_valid(review):
    return (isinstance(review, dict) and review.get("independent") is True
            and _nonempty(review.get("reviewer_id")) and _nonempty(review.get("identity_source"))
            and _nonempty(review.get("evidence_ref")) and _nonempty(review.get("feedback")))


def _delivery_issues(record, output):
    issues, readback = [], record.get("readback", {})
    if not isinstance(readback, dict):
        readback = {}
    if (readback.get("independent") is not True or readback.get("visible") is not True
            or readback.get("method") not in {"sdk", "platform_api", "human_device"}
            or not _nonempty(readback.get("evidence_ref"))):
        issues.append("delivery_readback_missing")
    for field, expected in (("channel", record.get("channel")), ("conversation_id", record.get("conversation_id")),
                            ("origin_event_id", record.get("input_event_id"))):
        if not _nonempty(expected) or output.get(field) != expected or readback.get(field) != expected:
            issues.append("delivery_binding:" + field)
    if not _nonempty(output.get("reply_id")) or output.get("reply_id") != readback.get("reply_id"):
        issues.append("delivery_reply_binding")
    if output.get("text") != readback.get("text"):
        issues.append("delivery_body_binding")
    for field, expected in (("final_reply_count", 1), ("duplicate_reply_count", 0)):
        if type(readback.get(field)) is not int or readback[field] != expected:
            issues.append(field)
    return issues


def evaluate_attempt(case, record):
    oracle, issues, review_issues = case["oracle"], [], []
    output = record.get("output", {})
    output = output if isinstance(output, dict) else {}
    text = output.get("text", "")
    text = text if isinstance(text, str) else ""
    raw_outcome = record.get("outcome", {})
    outcome = raw_outcome.get("status", "unknown") if isinstance(raw_outcome, dict) else "unknown"
    layer = record.get("layer")
    substantive = bool(text.strip()) and output.get("kind") != "ack" and not _ACK.fullmatch(text.strip())
    expected_silence = output.get("kind") == "silent" and "silent" in oracle.get("allowed_kinds", [])
    result = {"case_id": case["case_id"], "attempt_id": record.get("attempt_id"), "channel": case["channel"],
              "intent": case["intent"], "family": case.get("family", case["intent"]), "layer": layer,
              "source_version": case["source_version"], "cold_or_warm": record.get("cold_or_warm", "unknown"),
              "record": deepcopy(record), "outcome_status": outcome}
    for field in ("case_id", "channel", "intent", "source_version"):
        if record.get(field) != case.get(field):
            issues.append("case_binding:" + field)
    if record.get("schema_version") != SCHEMA_VERSION:
        issues.append("record_schema")
    if not _nonempty(record.get("attempt_id")):
        issues.append("attempt_id_missing")
    if layer not in LAYERS:
        issues.append("invalid_layer")
    if outcome != "not_run":
        if not substantive and not expected_silence:
            issues.append("ack_or_empty_is_not_completion")
        if expected_silence and text.strip():
            issues.append("silent_result_contains_reply")
        if output.get("kind") not in oracle.get("allowed_kinds", ["answer"]):
            issues.append("unexpected_output_kind")
        for name, concept in oracle.get("concepts", {}).items():
            if not concept_matches(text, concept):
                issues.append("missing_concept:" + name)
        for pattern in oracle.get("forbidden_assertions", []):
            for match in re.finditer(pattern, text, re.I):
                prefix = text[max(0, match.start() - 16):match.start()]
                # A quoted/negated forbidden assertion needs source review; it
                # is not silently accepted based on an isolated positive token.
                if re.search(r"(?:不是|并非|不能说|不代表|不等于|不能认为|不要说|不得声称)[\s“‘\"']*$", prefix):
                    review_issues.append("negated_claim_requires_review:" + pattern)
                else:
                    issues.append("forbidden_assertion:" + pattern)
                    break
        facts = output.get("facts", {})
        facts = facts if isinstance(facts, dict) else {}
        for key, value in oracle.get("required_facts", {}).items():
            if key not in facts or json.dumps(facts[key], sort_keys=True) != json.dumps(value, sort_keys=True):
                issues.append("required_fact:" + key)
        subtasks = output.get("completed_subtasks", [])
        subtasks = subtasks if isinstance(subtasks, list) else []
        for task in oracle.get("required_subtasks", []):
            if task not in subtasks:
                issues.append("missing_subtask:" + task)
        assertions = record.get("assertions", [])
        assertions = assertions if isinstance(assertions, list) else []
        for identifier in oracle.get("required_assertions", []):
            matches = [a for a in assertions if isinstance(a, dict) and a.get("id") == identifier]
            if len(matches) != 1 or not _assertion_valid(matches[0], text, case["source_version"]):
                issues.append("unverified_assertion:" + identifier)
        semantic = record.get("semantic_review", {})
        semantic = semantic if isinstance(semantic, dict) else {}
        semantic_ok = (_assertion_valid(semantic, text, case["source_version"])
                       and semantic.get("method") in {"source_check", "human_review"}
                       and semantic.get("independent") is True)
        if semantic.get("method") == "human_review":
            semantic_ok = semantic_ok and _human_valid(semantic)
        if oracle.get("semantic_review_required", True) and not semantic_ok:
            review_issues.append("source_grounded_semantic_review_missing")
        if semantic_ok:
            review_issues = []
        provenance = record.get("execution_provenance", {})
        provenance = provenance if isinstance(provenance, dict) else {}
        component_evidence = (layer == "offline" and provenance.get("actual_component_run") is True
                              and _nonempty(provenance.get("component_scope"))
                              and _nonempty(provenance.get("execution_ref")))
        if not (provenance.get("actual_assistant_intake") is True or provenance.get("actual_assistant_run") is True
                or component_evidence):
            issues.append("actual_assistant_execution_unproven")
        if record.get("example_only") is True or provenance.get("illustrative_values_not_measured") is True:
            issues.append("illustration_is_not_execution")
        if layer == "real_model" and (provenance.get("actual_model_call") is not True
                                       or not _nonempty(provenance.get("model_run_ref"))):
            issues.append("actual_model_execution_unproven")
        if layer in {"realtime", "phone", "human"}:
            if provenance.get("actual_realtime_inbound") is not True:
                issues.append("realtime_entry_unproven")
            if provenance.get("actual_feishu_delivery") is not True:
                issues.append("actual_delivery_unproven")
            issues.extend(_delivery_issues(record, output))
        if layer in {"phone", "human"}:
            device = record.get("device_review", {})
            if (not isinstance(device, dict) or device.get("actual_device") is not True
                    or not _nonempty(device.get("observer_id")) or not _nonempty(device.get("evidence_ref"))
                    or device.get("output_sha256") != output_hash(text)):
                issues.append("phone_observation_unproven")
        if layer == "human" and not _human_valid(record.get("human_review")):
            issues.append("independent_human_review_unproven")
        supplied_artifacts = record.get("artifacts", [])
        supplied_artifacts = supplied_artifacts if isinstance(supplied_artifacts, list) else []
        for kind in oracle.get("required_artifact_kinds", []):
            artifacts = [a for a in supplied_artifacts if isinstance(a, dict) and a.get("kind") == kind
                         and re.fullmatch(r"[a-f0-9]{64}", str(a.get("sha256", "")))]
            if layer in {"realtime", "phone", "human"}:
                artifacts = [a for a in artifacts if isinstance(a.get("readback"), dict)
                             and a["readback"].get("sha256") == a["sha256"]
                             and a["readback"].get("independent") is True and a["readback"].get("visible") is True
                             and _nonempty(a["readback"].get("evidence_ref"))
                             and a["readback"].get("conversation_id") == record.get("conversation_id")
                             and a["readback"].get("origin_event_id") == record.get("input_event_id")]
            if not artifacts:
                issues.append("artifact_not_verified:" + kind)
    terminal = {"not_run", "timeout", "cancelled", "partial", "error"}
    if outcome in terminal:
        status = outcome
    elif outcome != "completed" or issues:
        status = "failed"
    elif review_issues:
        status = "needs_review"
    else:
        status = "passed"
    timing = _timing(record, output, substantive)
    targets = oracle.get("latency_targets_s", {})
    timing["target_results"] = {k: "unknown" if timing.get(k) is None else "met" if timing[k] <= v else "missed"
                                for k, v in targets.items()}
    timing["target_status"] = ("unknown" if not targets or "unknown" in timing["target_results"].values()
                               else "missed" if "missed" in timing["target_results"].values() else "met")
    flags = []
    if len(text) > oracle.get("style_max_chars", 400 if case["channel"] == "group" else 600):
        flags.append("longer_than_case_guideline")
    lines = [line.strip() for line in text.splitlines() if len(line.strip()) > 6]
    if len(lines) != len(set(lines)):
        flags.append("repetition")
    if re.search(r"作为(?:一个)?AI|作为(?:一个)?人工智能|claim_handle|trace_id|校验标记", text, re.I):
        flags.append("template_or_internal_marker")
    review = record.get("style_review", {})
    human_score = (review.get("score") if _human_valid(review) and _finite(review.get("score"))
                   and 1 <= review["score"] <= 5 and review.get("output_sha256") == output_hash(text)
                   and review.get("source_version") == case["source_version"] else None)
    result.update({"status": status,
                   "correctness": {"status": "not_evaluated" if outcome == "not_run" else "fail" if issues or outcome != "completed"
                                   else "needs_review" if review_issues else "pass", "issues": issues + review_issues},
                   "timing": timing,
                   "style": {"flags": flags, "human_score": human_score,
                             "human_evaluation_performed": human_score is not None,
                             "model_advisory": deepcopy(record.get("model_grade")),
                             "automatic_style_acceptance": False},
                   "release_gate_pass": status == "passed" and timing["target_status"] == "met"})
    result["evidence_scope"] = record.get("execution_provenance", {}).get("component_scope", "assistant") if isinstance(record.get("execution_provenance"), dict) else "unknown"
    return result


def _summary(results, planned):
    by_case = defaultdict(list)
    for row in results:
        if row["case_id"] in planned:
            by_case[row["case_id"]].append(row)
    correct = [r for r in results if r["status"] == "passed"]
    reliable = sum(bool(by_case[c]) and all(r["status"] == "passed" for r in by_case[c]) for c in planned)
    first_pass = sum(bool(by_case[c]) and by_case[c][0]["status"] == "passed" for c in planned)
    return {"planned_cases": len(planned), "attempt_count": sum(r["status"] != "not_run" for r in results),
            "denominator_including_not_run": len(results), "status_counts": dict(Counter(r["status"] for r in results)),
            "case_pass_count": reliable, "case_pass_rate": reliable / len(planned) if planned else None,
            "first_attempt_pass_count": first_pass, "first_attempt_pass_rate": first_pass / len(planned) if planned else None,
            "case_has_pass_count": sum(any(r["status"] == "passed" for r in by_case[c]) for c in planned),
            "correct_attempt_count": len(correct),
            "correct_completion_s": distribution(r["timing"].get("completion_s") for r in correct),
            "correct_first_substantive_s": distribution(r["timing"].get("first_substantive_s") for r in correct),
            "all_observed_elapsed_s": distribution(r.get("timing", {}).get("observed_elapsed_s") for r in results),
            "correct_but_timing_unknown": sum(r["timing"].get("completion_s") is None for r in correct),
            "release_gate_pass_attempts": sum(r.get("release_gate_pass") is True for r in results),
            "execution_scope_counts": dict(Counter(r.get("evidence_scope", "unknown") for r in results if r["status"] != "not_run")),
            "pass_definition": "correctness and layer evidence; style and latency are separate; all trials must pass for case_pass"}


def evaluate_quality(suite_or_cases, records, *, layers=LAYERS):
    cases = validate_cases(suite_or_cases["cases"] if isinstance(suite_or_cases, dict) else suite_or_cases)
    if not layers or len(set(layers)) != len(layers) or any(l not in LAYERS for l in layers):
        raise QualityInputError("select unique supported layers")
    lookup = {c["case_id"]: c for c in cases}
    results, invalid, observed, identities = [], [], set(), defaultdict(list)
    for record in records:
        if not isinstance(record, dict) or record.get("case_id") not in lookup or record.get("layer") not in LAYERS:
            invalid.append({"status": "failed", "reason": "unknown_case_or_layer", "record": deepcopy(record)})
            continue
        if record["layer"] not in layers:
            invalid.append({"status": "failed", "reason": "layer_not_selected", "record": deepcopy(record)})
            continue
        row = evaluate_attempt(lookup[record["case_id"]], record)
        results.append(row)
        observed.add((record["case_id"], record["layer"]))
        identities[(record["case_id"], record["layer"], record.get("attempt_id"))].append(row)
    for duplicate in identities.values():
        if len(duplicate) > 1:
            for row in duplicate:
                row["status"] = "failed"; row["correctness"]["status"] = "fail"
                row["correctness"]["issues"].append("duplicate_attempt_identity")
                row["release_gate_pass"] = False
    for layer in layers:
        for case in cases:
            if (case["case_id"], layer) not in observed:
                results.append(evaluate_attempt(case, {"schema_version": SCHEMA_VERSION, "case_id": case["case_id"],
                    "attempt_id": "not-run", "channel": case["channel"], "intent": case["intent"],
                    "source_version": case["source_version"], "layer": layer, "outcome": {"status": "not_run"},
                    "reason": "no_record_for_planned_case"}))
    layer_reports = {layer: _summary([r for r in results if r["layer"] == layer], lookup) for layer in layers}
    for layer in layers:
        required = {c["case_id"] for c in cases if layer in c.get("required_layers", layers)}
        required_summary = _summary([r for r in results if r["layer"] == layer and r["case_id"] in required], required)
        layer_reports[layer].update(required_cases=len(required), not_required_cases=len(cases)-len(required),
            required_case_pass_count=required_summary["case_pass_count"],
            required_case_pass_rate=required_summary["case_pass_rate"],
            required_status_counts=required_summary["status_counts"])
    strata = {}
    for layer in layers:
        for channel in ("group", "private"):
            planned = {c["case_id"] for c in cases if c["channel"] == channel}
            for thermal in ("cold", "warm", "unknown"):
                group = [r for r in results if r["layer"] == layer and r["channel"] == channel and r["cold_or_warm"] == thermal]
                strata["|".join((layer, channel, thermal))] = _summary(group, planned)
    required_results = [r for r in results if r["layer"] in lookup[r["case_id"]].get("required_layers", layers)
                        or r["outcome_status"] != "not_run"]
    covers_required_layers = all(set(c.get("required_layers", layers)).issubset(layers) for c in cases)
    return {"schema_version": SCHEMA_VERSION, "layers": layer_reports, "strata": strata,
            "results": results, "invalid_records": invalid,
            "ci_pass": bool(required_results) and not invalid and all(r["status"] == "passed" for r in required_results),
            "covers_all_required_layers": covers_required_layers,
            "release_gate_pass": bool(required_results) and covers_required_layers and not invalid
                                 and all(r["release_gate_pass"] for r in required_results),
            "limits": ["Finite cases cover named domains, not all possible natural-language requests.",
                       "Concept and contradiction patterns are diagnostics; open factual answers need output-bound source review.",
                       "Model grading is advisory and does not replace independent source or human review.",
                       "External evidence references and identities must be independently read during audit.",
                       "Offline component tests are scoped to their named modules, not the full assistant intake.",
                       "Offline, real model, realtime, phone and human evidence are never promoted into another layer.",
                       "Phone/human layers use phone_visible, never SDK final_visible, for user display latency."]}


def compare_quality(suite_or_cases, baseline_records, candidate_records, *, layers=LAYERS):
    baseline = evaluate_quality(suite_or_cases, baseline_records, layers=layers)
    candidate = evaluate_quality(suite_or_cases, candidate_records, layers=layers)
    def index(report):
        return {(r["case_id"], r["layer"], r["attempt_id"]): r for r in report["results"]}
    left, right = index(baseline), index(candidate)
    pairs, deltas = [], []
    for key in sorted(left.keys() & right.keys()):
        a, b = left[key], right[key]
        same = all(a.get(k) == b.get(k) for k in ("source_version", "channel", "intent", "cold_or_warm"))
        valid = same and a["status"] == b["status"] == "passed"
        ta, tb = a["timing"].get("completion_s"), b["timing"].get("completion_s")
        delta = tb - ta if valid and _finite(ta) and _finite(tb) else None
        if delta is not None:
            deltas.append(delta)
        pairs.append({"case_id": key[0], "layer": key[1], "attempt_id": key[2], "baseline_status": a["status"],
                      "candidate_status": b["status"], "same_conditions": same,
                      "candidate_minus_baseline_s": delta})
    # Signed deltas are not duration distributions.
    return {"schema_version": SCHEMA_VERSION, "baseline": baseline, "candidate": candidate,
            "paired": pairs, "paired_completion_delta_s": {"n": len(deltas), "p50": median(deltas) if deltas else None},
            "speedup_demonstrated": bool(deltas) and median(deltas) < 0,
            "comparison_limit": "Paired successful observations only; a median change is not a statistical significance claim."}


def regrade_r6(cases, records):
    """Preserve the v1 result; version only the two known lexical concepts.

    This does not create R7 acceptance or claim an independent semantic review.
    It answers whether the old three lexical failures survive a declared oracle
    revision. Every original record and case remains unchanged.
    """
    from .evaluation import evaluate_run
    old_cases, old_records = deepcopy(cases), deepcopy(records)
    revised = deepcopy(cases)
    changes = []
    for case in revised:
        if case["case_id"] in {"R6-05-group", "R6-05-private"}:
            mapping = case["oracle"].get("required_text", {})
            for key in mapping:
                if key in {"bounded_scope", "bounded_no_match", "scope"} or any("当前" in v or "范围" in v for v in mapping[key]):
                    mapping[key] = list(dict.fromkeys(mapping[key] + ["当前可读的台账记录", "当前可读台账", "当前台账未找到匹配记录"]))
                    changes.append({"case_id": case["case_id"], "field": "required_text." + key})
                elif key == "no_match":
                    mapping[key] = list(dict.fromkeys(mapping[key] + ["暂未匹配", "未找到匹配记录", "没有找到对应记录"]))
                    changes.append({"case_id": case["case_id"], "field": "required_text." + key})
        if case["case_id"] == "R6-B02-private":
            mapping = case["oracle"].get("required_text", {})
            for key in mapping:
                if any("读" in v or "来源" in v for v in mapping[key]):
                    mapping[key] = list(dict.fromkeys(mapping[key] + ["无法可靠读取", "当前无法读取", "来源暂时不可用"]))
                    changes.append({"case_id": case["case_id"], "field": "required_text." + key})
    return {"schema_version": SCHEMA_VERSION, "oracle_revision": "r6-lexical-concepts/2",
            "original": evaluate_run(old_cases, old_records, expected_layer="offline"),
            "revised_lexical_diagnostic": evaluate_run(revised, deepcopy(records), expected_layer="offline"),
            "changes": changes, "r7_semantic_acceptance": False,
            "note": "Revised text matching is diagnostic only; original failures and evidence remain intact."}
