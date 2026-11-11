"""Read-only, standard-library diagnostics for AM expression comparisons.

This module consumes evidence; it neither runs a model nor changes an acceptance
decision, production switch, review, training request, or original artifact.
Only explicit, equal execution bindings make a base/SFT pair comparable.
"""

from __future__ import annotations

import argparse
from collections import Counter
import hashlib
import json
import math
import os
from pathlib import Path
import re
import sys
from typing import Any, Mapping, Sequence


SCHEMA = "am.expression-diagnostics.v1"
REPORT_SCHEMA = "am.expression-diagnostics-report.v1"
ARMS = ("production_q8", "mlx_base", "mlx_sft")
PAIR_FIELDS = (
    "input_sha256", "base_revision", "tokenizer_sha256",
    "generation_settings", "runner_source_sha256", "model_manifest_sha256",
)
_HASH = re.compile(r"^[0-9a-f]{64}$")
_STATUS = re.compile(r"^[a-z][a-z0-9_]{0,63}$")


class DiagnosticError(ValueError):
    """The supplied evidence is structurally invalid or internally inconsistent."""


def _mapping(value: Any, label: str) -> Mapping[str, Any]:
    if not isinstance(value, dict):
        raise DiagnosticError(f"{label}: expected an object")
    return value


def _sequence(value: Any, label: str) -> list[Any]:
    if not isinstance(value, list):
        raise DiagnosticError(f"{label}: expected an array")
    return value


def _string(value: Any, label: str) -> str:
    if not isinstance(value, str) or not value or len(value) > 512:
        raise DiagnosticError(f"{label}: expected a nonempty string, at most 512 characters")
    return value


def _int(value: Any, label: str, minimum: int = 0) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise DiagnosticError(f"{label}: expected an integer >= {minimum}")
    return value


def _bool(value: Any, label: str) -> bool:
    if not isinstance(value, bool):
        raise DiagnosticError(f"{label}: expected a boolean")
    return value


def _digest(value: Any, label: str, *, optional: bool = False) -> str | None:
    if value is None and optional:
        return None
    if not isinstance(value, str) or not _HASH.fullmatch(value):
        raise DiagnosticError(f"{label}: expected a lowercase SHA-256 digest")
    return value


def _finite(value: Any, label: str, minimum: float = 0.0) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise DiagnosticError(f"{label}: expected a finite number")
    number = float(value)
    if not math.isfinite(number) or number < minimum:
        raise DiagnosticError(f"{label}: expected a finite number >= {minimum}")
    return number


def _canonical(value: Any) -> str:
    try:
        return json.dumps(value, sort_keys=True, separators=(",", ":"),
                          ensure_ascii=False, allow_nan=False)
    except (TypeError, ValueError) as exc:
        raise DiagnosticError("evidence contains a non-JSON or non-finite value") from exc


def _id(value: Any) -> str:
    if isinstance(value, int) and not isinstance(value, bool) and value > 0:
        return str(value)
    return _string(value, "case_id")


def _record(raw: Mapping[str, Any]) -> dict[str, Any]:
    record = dict(raw)
    record["case_id"] = _id(record.get("case_id"))
    record["arm"] = _string(record.get("arm"), "arm")
    status = record.get("status")
    if not isinstance(status, str) or not _STATUS.fullmatch(status):
        raise DiagnosticError("record.status: expected a short lowercase status label")
    for key in ("schema_pass", "artifact_bindings_verified", "fallback_used", "truncated", "record_present"):
        if key in record:
            _bool(record[key], f"record.{key}")
    output = record.get("output_text")
    supplied_hash = _digest(record.get("output_sha256"), "record.output_sha256", optional=True)
    if output is not None:
        if not isinstance(output, str):
            raise DiagnosticError("record.output_text: expected a string")
        actual_hash = hashlib.sha256(output.encode("utf-8")).hexdigest()
        if supplied_hash is not None and supplied_hash != actual_hash:
            raise DiagnosticError("record.output_sha256 does not match exact UTF-8 output")
        supplied_hash = actual_hash
    record["output_sha256"] = supplied_hash
    if record.get("record_present") is False and (supplied_hash is not None or record.get("record_sha256") is not None):
        raise DiagnosticError("an absent original record cannot contain output or a record digest")
    for key in ("input_sha256", "tokenizer_sha256", "runner_source_sha256",
                "model_manifest_sha256", "adapter_sha256", "record_sha256"):
        if record.get(key) is not None:
            _digest(record[key], f"record.{key}")
    if record.get("base_revision") is not None:
        revision = _string(record["base_revision"], "record.base_revision")
        if re.fullmatch(r"[a-f0-9]{40}|[a-f0-9]{64}", revision) is None:
            raise DiagnosticError("record.base_revision must be an immutable commit or content digest")
    if record.get("generation_settings") is not None:
        _mapping(record["generation_settings"], "record.generation_settings")
        _canonical(record["generation_settings"])
    return record


def _has_output(record: Mapping[str, Any]) -> bool:
    return (record.get("record_present", True) is True and record.get("status") == "completed"
            and record.get("output_sha256") is not None
            and not record.get("fallback_used", False)
            and not record.get("truncated", False))


def _guard(record: Mapping[str, Any]) -> str:
    if not _has_output(record):
        return "not_run"
    safety = record.get("safety", {})
    if not isinstance(safety, dict):
        raise DiagnosticError("record.safety: expected an object")
    decision = safety.get("decision", "indeterminate")
    if decision not in {"pass", "fail", "reject", "indeterminate", "not_run"}:
        raise DiagnosticError("record.safety.decision: unsupported decision")
    return decision


def _pair(base: Mapping[str, Any], sft: Mapping[str, Any],
          expected_adapter: str | None, identities: Mapping[str, Any]) -> dict[str, Any]:
    reasons: list[str] = []
    for arm, record in (("mlx_base", base), ("mlx_sft", sft)):
        if not _has_output(record):
            reasons.append(f"{arm}:completed_output_unavailable")
        if record.get("schema_pass") is not True:
            reasons.append(f"{arm}:schema_not_validated")
        if record.get("artifact_bindings_verified") is not True:
            reasons.append(f"{arm}:artifact_bindings_unverified")
        for key in ("base_revision", "tokenizer_sha256", "generation_settings", "model_manifest_sha256"):
            expected = identities[arm].get(key)
            if expected is None:
                reasons.append(f"{arm}:plan_{key}_missing")
            elif record.get(key) is not None and _canonical(record[key]) != _canonical(expected):
                reasons.append(f"{arm}:plan_{key}_mismatch")
    for key in PAIR_FIELDS:
        left, right = base.get(key), sft.get(key)
        if left is None or right is None or (key == "generation_settings" and (not left or not right)):
            reasons.append(f"{key}:missing")
        elif _canonical(left) != _canonical(right):
            reasons.append(f"{key}:mismatch")
    if base.get("adapter_sha256") is not None:
        reasons.append("mlx_base:unexpected_adapter")
    if expected_adapter is None or sft.get("adapter_sha256") is None:
        reasons.append("mlx_sft:adapter_binding_missing")
    elif sft["adapter_sha256"] != expected_adapter:
        reasons.append("mlx_sft:adapter_binding_mismatch")
    both_output = _has_output(base) and _has_output(sft)
    equal = (base["output_sha256"] == sft["output_sha256"]) if both_output else None
    return {
        "case_id": base["case_id"], "comparable": not reasons,
        "reasons": reasons, "output_hashes_equal": equal,
        "text_comparison_basis": "exact_output_sha256" if both_output else "unavailable",
        "base_output_sha256": base.get("output_sha256"),
        "sft_output_sha256": sft.get("output_sha256"),
    }


def _training(pilot: Mapping[str, Any], expected_adapter: str | None) -> dict[str, Any]:
    if not pilot:
        return {"status": "unobserved", "parameter_update": "unobserved",
                "save_roundtrip": "unobserved", "fresh_process_reload": "unobserved",
                "quality_inference": "not_established_by_training"}
    steps = _int(pilot.get("optimizer_steps", 0), "pilot.optimizer_steps")
    samples = _int(pilot.get("actual_train_samples", 0), "pilot.actual_train_samples")
    consumption = "unobserved"
    if "step_records" in pilot:
        trace = _sequence(pilot["step_records"], "pilot.step_records")
        consumed: list[str] = []
        step_numbers: list[int] = []
        for raw_step in trace:
            step = _mapping(raw_step, "pilot.step_record")
            step_numbers.append(_int(step.get("optimizer_step"), "pilot.optimizer_step", 1))
            consumed.extend(_string(i, "pilot.sample_id") for i in _sequence(step.get("ids"), "pilot.sample_ids"))
        consistent = (step_numbers == list(range(1, steps + 1)) and len(consumed) == samples)
        if pilot.get("single_pass") is True:
            consistent = consistent and len(consumed) == len(set(consumed))
        consumption = "consistent" if consistent else "inconsistent"
    adapter = _digest(pilot.get("adapter_sha256"), "pilot.adapter_sha256", optional=True)
    delta = _mapping(pilot.get("final_parameter_delta", {}), "pilot.final_parameter_delta")
    updated = (delta.get("all_finite") is True
               and _int(delta.get("changed_tensors", 0), "pilot.changed_tensors") > 0
               and _finite(delta.get("l2_norm", 0.0), "pilot.parameter_delta_l2") > 0)
    phases: dict[str, Mapping[str, Any]] = {}
    for raw in _sequence(pilot.get("phases", []), "pilot.phases"):
        phase = _mapping(raw, "pilot.phase")
        name = _string(phase.get("phase"), "pilot.phase.name")
        if name in phases:
            raise DiagnosticError("pilot contains duplicate phase evidence")
        phases[name] = phase
    def closed(name: str) -> bool:
        p = phases.get(name, {})
        return (p.get("attempted") is True and p.get("status") == "completed"
                and type(p.get("exit_code")) is int and p["exit_code"] == 0
                and p.get("timed_out") is False and p.get("stop_confirmed") is True)
    fit_pid, reload_pid = phases.get("fit", {}).get("pid"), phases.get("reload", {}).get("pid")
    independent = (type(fit_pid) is int and fit_pid > 0 and type(reload_pid) is int
                   and reload_pid > 0 and fit_pid != reload_pid)
    identity = adapter is not None and adapter == expected_adapter
    saved = (identity and pilot.get("adapter_tensor_roundtrip_verified") is True
             and pilot.get("adapter_complete_keyset_verified") is True)
    reloaded = (saved and independent and closed("reload")
                and pilot.get("fresh_load_verified") is True
                and pilot.get("adapter_tensor_equality_verified") is True)
    mechanics = (closed("fit") and updated and reloaded and steps > 0 and samples > 0
                 and consumption != "inconsistent"
                 and pilot.get("result_status") == "completed")
    reload_difference = pilot.get("reload_diagnostics_max_abs_delta")
    if reload_difference is not None:
        reload_difference = _finite(reload_difference, "pilot.reload_diagnostics_max_abs_delta")
    return {
        "status": "evidence_consistent" if mechanics else "evidence_incomplete_or_inconsistent",
        "optimizer_steps": steps, "actual_train_samples": samples,
        "sample_consumption_trace": consumption,
        "parameter_update": "observed" if updated else "not_verified",
        "save_roundtrip": "verified_in_supplied_evidence" if saved else "not_verified",
        "fresh_process_reload": "verified_in_supplied_evidence" if reloaded else "not_verified",
        "adapter_binding_matches_evaluation": identity,
        "fit_and_reload_have_distinct_process_ids": independent,
        "reload_diagnostic": {"max_abs_delta": reload_difference,
                              "comparison": "saved_adapter_vs_reloaded_adapter",
                              "is_base_vs_sft_distribution_probe": False},
        "quality_inference": "not_established_by_training",
    }


def _reviews(raw_reviews: Any, positions: Mapping[tuple[str, str], Mapping[str, Any]]) -> dict[str, Any]:
    rows = _sequence(raw_reviews, "reviews")
    ids: set[str] = set()
    reviewers: set[tuple[str, str, str, str]] = set()
    result: dict[str, Any] = {}
    for kind in ("human", "gpt_proxy", "machine"):
        result[kind] = {"rating_count": 0, "reviewed_positions": set(), "reviewers": set(),
                        "output_hashes": set(), "assessments": Counter(), "naturalness_scores": []}
    unscored = 0
    for raw in rows:
        review = _mapping(raw, "review")
        review_id = _string(review.get("review_id"), "review.review_id")
        if review_id in ids:
            raise DiagnosticError("duplicate review_id")
        ids.add(review_id)
        key = (_id(review.get("case_id")), _string(review.get("arm"), "review.arm"))
        if key not in positions:
            raise DiagnosticError("review refers to an unplanned position")
        kind = _string(review.get("kind"), "review.kind")
        if kind not in result:
            raise DiagnosticError("review.kind must be human, gpt_proxy, or machine")
        authored = _bool(review.get("human_authored"), "review.human_authored")
        if authored != (kind == "human"):
            raise DiagnosticError("review author type and human_authored disagree")
        reviewer = _string(review.get("reviewer_id"), "review.reviewer_id")
        if kind == "gpt_proxy":
            _string(review.get("authorization_ref"), "review.authorization_ref")
        assessment = review.get("assessment", "unscored")
        if assessment not in {"adopt", "revise", "clarify", "reject", "unscored"}:
            raise DiagnosticError("review.assessment: unsupported assessment")
        scores = [review.get(k) for k in ("usable", "factual_pass", "naturalness")]
        for k in ("usable", "factual_pass"):
            if review.get(k) is not None:
                _bool(review[k], f"review.{k}")
        if review.get("naturalness") is not None:
            value = _finite(review["naturalness"], "review.naturalness", 1.0)
            if value > 5.0:
                raise DiagnosticError("review.naturalness must use the declared 1–5 scale")
        has_rating = assessment != "unscored" or any(s is not None for s in scores)
        if not has_rating:
            unscored += 1
            continue
        position = positions[key]
        if not _has_output(position):
            raise DiagnosticError("a position without completed output cannot be rated")
        bound_hash = _digest(review.get("candidate_output_sha256"), "review.candidate_output_sha256")
        if bound_hash != position["output_sha256"]:
            raise DiagnosticError("review is not bound to the current exact candidate output")
        identity = (*key, kind, reviewer)
        if identity in reviewers:
            raise DiagnosticError("duplicate reviewer rating for the same candidate")
        reviewers.add(identity)
        target = result[kind]
        target["rating_count"] += 1
        target["reviewed_positions"].add(key)
        target["reviewers"].add(reviewer)
        target["output_hashes"].add(bound_hash)
        if assessment != "unscored":
            target["assessments"][assessment] += 1
        if review.get("naturalness") is not None:
            target["naturalness_scores"].append(review["naturalness"])
    for target in result.values():
        target["reviewed_position_count"] = len(target.pop("reviewed_positions"))
        target["reviewer_count"] = len(target.pop("reviewers"))
        target["distinct_reviewed_texts"] = len(target.pop("output_hashes"))
        target["assessments"] = dict(target["assessments"])
    result["unscored_markers"] = unscored
    result["machine_guard_overridden_by_review"] = False
    return result


def _distribution_probe(raw: Any, pairs: Sequence[Mapping[str, Any]],
                        positions: Mapping[tuple[str, str], Mapping[str, Any]],
                        adapter: str | None) -> dict[str, Any]:
    if raw is None:
        return {"status": "unobserved", "cause_of_identical_text": "unknown",
                "adapter_not_loaded_inference": "not_supported"}
    probe = _mapping(raw, "distribution_probe")
    case = _id(probe.get("case_id"))
    matching = next((p for p in pairs if p["case_id"] == case and p["comparable"]), None)
    if matching is None:
        raise DiagnosticError("distribution probe requires a comparable evaluated pair")
    base = positions[(case, "mlx_base")]
    for key in PAIR_FIELDS:
        if probe.get(key) is None or _canonical(probe[key]) != _canonical(base.get(key)):
            raise DiagnosticError(f"distribution_probe.{key}: missing or mismatched execution binding")
    if probe.get("adapter_sha256") != adapter or probe.get("same_prefix_verified") is not True:
        raise DiagnosticError("distribution probe adapter/prefix binding is not verified")
    count = _int(probe.get("compared_positions"), "distribution_probe.compared_positions", 1)
    changed = _int(probe.get("top1_changed_positions"), "distribution_probe.top1_changed_positions")
    if changed > count:
        raise DiagnosticError("changed top-1 positions exceed compared positions")
    delta = _finite(probe.get("max_abs_logit_delta"), "distribution_probe.max_abs_logit_delta")
    if delta == 0 and changed:
        raise DiagnosticError("zero logit delta is inconsistent with changed top-1 positions")
    return {"status": "observed_in_supplied_evidence", "case_id": case,
            "compared_positions": count, "top1_changed_positions": changed,
            "max_abs_logit_delta": delta,
            "observation": "logits_differ" if delta else "no_logit_difference_in_observed_probe",
            "cause_of_identical_text": "not_determined_by_probe_alone",
            "adapter_not_loaded_inference": "not_supported"}


def diagnose(bundle: Mapping[str, Any]) -> dict[str, Any]:
    """Build a body-free report without changing the supplied evidence.

    Metadata equality is an evidence consistency check, not independent proof of
    the execution that produced that metadata. The returned report is not a
    deployment, training, factual-pass, or business-acceptance authorization.
    """
    bundle = _mapping(bundle, "bundle")
    if bundle.get("schema") != SCHEMA:
        raise DiagnosticError(f"bundle.schema must be {SCHEMA}")
    plan = _mapping(bundle.get("plan"), "plan")
    cases = [_id(c) for c in _sequence(plan.get("case_ids"), "plan.case_ids")]
    if not cases or len(cases) > 10000 or len(cases) != len(set(cases)):
        raise DiagnosticError("plan.case_ids must contain 1–10000 unique identifiers")
    arms = _mapping(plan.get("arms"), "plan.arms")
    if not {"mlx_base", "mlx_sft"}.issubset(arms) or not set(arms).issubset(ARMS):
        raise DiagnosticError("plan.arms requires mlx_base/mlx_sft and optional production_q8")
    for name, metadata in arms.items():
        _mapping(metadata, f"plan.arms.{name}")
    _digest(plan.get("plan_sha256"), "plan.plan_sha256")
    _digest(plan.get("case_manifest_sha256"), "plan.case_manifest_sha256")
    expected_adapter = _digest(arms["mlx_sft"].get("adapter_sha256"),
                               "plan.arms.mlx_sft.adapter_sha256", optional=True)
    positions: dict[tuple[str, str], dict[str, Any]] = {
        (case, arm): {"case_id": case, "arm": arm, "status": "missing_record",
                      "output_sha256": None}
        for case in cases for arm in arms
    }
    supplied: set[tuple[str, str]] = set()
    seen: set[tuple[str, str]] = set()
    records = _sequence(bundle.get("records"), "records")
    if len(records) > len(positions):
        raise DiagnosticError("more records than planned positions")
    for raw in records:
        record = _record(_mapping(raw, "record"))
        key = (record["case_id"], record["arm"])
        if key not in positions:
            raise DiagnosticError("record refers to an unplanned position")
        if key in seen:
            raise DiagnosticError("duplicate record for a planned position")
        seen.add(key)
        if record.get("record_present", True):
            supplied.add(key)
        positions[key] = record
    pairs = [_pair(positions[(c, "mlx_base")], positions[(c, "mlx_sft")], expected_adapter, arms)
             for c in cases]
    arm_results: dict[str, Any] = {}
    for arm in arms:
        rows = [positions[(c, arm)] for c in cases]
        outputs = [r for r in rows if _has_output(r)]
        arm_results[arm] = {
            "comparison_role": "external_reference" if arm == "production_q8" else "paired_mlx",
            "planned_positions": len(rows), "provided_records": sum((c, arm) in supplied for c in cases),
            "raw_statuses": dict(Counter(r["status"] for r in rows)),
            "completed_outputs": len(outputs),
            "schema_valid_completed_outputs": sum(r.get("schema_pass") is True for r in outputs),
            "fact_guard_states": dict(Counter(_guard(r) for r in rows)),
            "unscored_no_output_positions": len(rows) - len(outputs),
        }
    verified = [p for p in pairs if p["comparable"]]
    same = sum(p["output_hashes_equal"] is True for p in verified)
    changed = sum(p["output_hashes_equal"] is False for p in verified)
    text_conclusion = ("comparison_unavailable" if not verified else
                       "text_change_observed_quality_unestablished" if changed else
                       "no_text_change_observed_in_verified_pairs")
    review_results = _reviews(bundle.get("reviews", []), positions)
    completed = [r for r in positions.values() if _has_output(r)]
    return {
        "schema": REPORT_SCHEMA,
        "evidence_scope": bundle.get("evidence_scope", "supplied_execution_metadata"),
        "input_bundle_sha256": hashlib.sha256(_canonical(bundle).encode("utf-8")).hexdigest(),
        "plan_sha256": plan["plan_sha256"], "case_manifest_sha256": plan["case_manifest_sha256"],
        "counts": {"planned_cases": len(cases), "planned_positions": len(positions),
                   "provided_records": len(supplied), "missing_records": len(positions) - len(supplied),
                   "completed_outputs": len(completed),
                   "distinct_completed_texts": len({r["output_sha256"] for r in completed}),
                   "unscored_no_output_positions": len(positions) - len(completed)},
        "arms": arm_results, "mlx_pairs": pairs,
        "comparison": {"planned_pairs": len(cases), "verified_comparable_pairs": len(verified),
                       "equal_output_hash_pairs_with_or_without_complete_bindings":
                           sum(p["output_hashes_equal"] is True for p in pairs),
                       "identical_verified_pairs": same, "changed_verified_pairs": changed,
                       "text_conclusion": text_conclusion,
                       "naturalness_improvement": "not_established_by_this_diagnostic",
                       "q8_used_as_causal_training_control": False},
        "training": _training(_mapping(bundle.get("pilot", {}), "pilot"), expected_adapter),
        "reviews": review_results,
        "distribution_probe": _distribution_probe(bundle.get("distribution_probe"), pairs,
                                                     positions, expected_adapter),
        "authority": {"changes_production": False, "starts_training": False,
                      "rewrites_original_evidence": False, "grants_business_acceptance": False},
        "limitations": [
            "Supplied hashes and verification flags are checked for consistency; original execution is not rerun.",
            "Equal generated text does not prove that the adapter was absent or that token distributions were equal.",
            "Changed text, loss, schema checks, and GPT proxy reviews do not establish naturalness improvement.",
            "Missing outputs stay in the planned denominator and receive no quality score.",
        ],
    }


def adapt_r8_public(plan_binding: Mapping[str, Any], imported: Mapping[str, Any],
                    pilot: Mapping[str, Any], *, input_bindings: Mapping[str, Any] | None = None,
                    reviews: Sequence[Mapping[str, Any]] = ()) -> dict[str, Any]:
    """Adapt R8 public plan-binding/import/pilot evidence without reading bodies.

    R8 public rows omit per-case input hashes. They remain absent unless an
    independently produced input-bindings sidecar supplies exact record-bound
    execution metadata. The sidecar never repairs or overwrites an original.
    """
    plan_binding = _mapping(plan_binding, "R8 plan-binding")
    imported = _mapping(imported, "R8 evaluation-import")
    count = _int(plan_binding.get("planned_cases"), "R8 planned_cases", 1)
    arms = _mapping(plan_binding.get("arms"), "R8 arms")
    if count > 10000:
        raise DiagnosticError("R8 planned_cases is too large")
    expected = count * len(arms)
    if _int(plan_binding.get("planned_positions"), "R8 planned_positions") != expected:
        raise DiagnosticError("R8 plan denominator does not match cases × arms")
    if _int(plan_binding.get("planned_arms"), "R8 planned_arms") != len(arms):
        raise DiagnosticError("R8 planned_arms disagrees with arm definitions")
    if _int(imported.get("planned"), "R8 import planned") != expected:
        raise DiagnosticError("R8 import planned denominator disagrees with plan")
    raw_rows = _sequence(imported.get("rows"), "R8 rows")
    if (_int(imported.get("recorded"), "R8 import recorded") != len(raw_rows)
            or _int(imported.get("missing"), "R8 import missing") != expected - len(raw_rows)):
        raise DiagnosticError("R8 import record counts disagree with supplied rows")
    sidecars: dict[tuple[str, str], Mapping[str, Any]] = {}
    if input_bindings is not None:
        input_bindings = _mapping(input_bindings, "input-bindings")
        if input_bindings.get("schema") != "am.expression-input-bindings.v1":
            raise DiagnosticError("unsupported input-bindings schema")
        for key in ("plan_seal_sha256", "case_manifest_sha256"):
            if input_bindings.get(key) != plan_binding.get(key):
                raise DiagnosticError(f"input-bindings.{key} differs from R8 plan")
        for raw in _sequence(input_bindings.get("rows"), "input-bindings.rows"):
            side = _mapping(raw, "input-bindings.row")
            key = (_id(side.get("scene")), _string(side.get("arm"), "input-bindings.arm"))
            if key in sidecars:
                raise DiagnosticError("duplicate input-binding position")
            sidecars[key] = side
    records = []
    used_sidecars: set[tuple[str, str]] = set()
    for raw in raw_rows:
        raw = _mapping(raw, "R8 row")
        record = dict(raw)
        record["case_id"] = _id(raw.get("scene"))
        arm = _string(raw.get("arm"), "R8 row.arm")
        if arm not in arms:
            raise DiagnosticError("R8 row has an unplanned arm")
        arm_metadata = _mapping(arms[arm], "R8 arm metadata")
        # These values are attested by existing artifact_bindings_verified, not
        # independently recovered from model requests by this tool.
        for field in (*PAIR_FIELDS[1:4], "model_manifest_sha256", "adapter_sha256"):
            if field in arm_metadata:
                if field in record and _canonical(record[field]) != _canonical(arm_metadata[field]):
                    raise DiagnosticError(f"R8 row conflicts with plan {field}")
                record[field] = arm_metadata[field]
        key = (record["case_id"], arm)
        side = sidecars.get(key)
        if side is not None:
            used_sidecars.add(key)
            if side.get("record_sha256") != raw.get("record_sha256"):
                raise DiagnosticError("input binding is not bound to the exact original record")
            if side.get("artifact_bindings_verified") is not True:
                raise DiagnosticError("input sidecar is not marked verified by its collector")
            for field in PAIR_FIELDS:
                if side.get(field) is None:
                    raise DiagnosticError(f"input sidecar omits {field}")
                if record.get(field) is not None and _canonical(record[field]) != _canonical(side[field]):
                    raise DiagnosticError(f"input sidecar conflicts with original {field}")
                record[field] = side[field]
        records.append(record)
    if used_sidecars != set(sidecars):
        raise DiagnosticError("input-bindings contain a position without an original record")
    return {
        "schema": SCHEMA, "evidence_scope": "R8_public_metadata_with_explicit_missing_bindings",
        "plan": {"case_ids": [str(i) for i in range(1, count + 1)], "arms": dict(arms),
                 "plan_sha256": plan_binding.get("plan_seal_sha256"),
                 "case_manifest_sha256": plan_binding.get("case_manifest_sha256")},
        "records": records, "pilot": dict(pilot), "reviews": list(reviews),
    }


def adapt_native_r5(plan: Mapping[str, Any], imported: Mapping[str, Any],
                    pilot: Mapping[str, Any], *, reviews: Sequence[Mapping[str, Any]] = ()) -> dict[str, Any]:
    """Read existing R5/R8 private artifacts through their original validator.

    Called only by the operator on the original host. The original validation
    functions deepcopy and return data; they do not persist files or invoke a
    provider. Real-mode plan validation may read the original exclusion snapshot.
    No real artifacts are needed for this module's synthetic unit tests.

    The temporary verifier deliberately does no semantic work. Its output is
    discarded; the original imported safety decisions are retained verbatim.
    The collector emits only hashes/metadata and numbered scene identifiers.
    """
    try:
        from am_style_r5.evaluation import _check_results, import_results
    except ImportError as exc:
        raise DiagnosticError("native-r5 requires the existing am_style_r5 and am_style packages") from exc
    plan = _mapping(plan, "native plan")
    imported = _mapping(imported, "native imported results")
    try:
        _check_results(plan, imported)
        original_rows = imported["rows"]
        checked = import_results(
            plan, [r["record"] for r in original_rows if r["record"] is not None],
            verifier=lambda _case, _text: {
                "decision": "indeterminate", "reason": "diagnostic_binding_check_only", "verifier": "not_run"},
        )
    except (ValueError, TypeError, KeyError, OSError) as exc:
        raise DiagnosticError("native evidence failed original R5 plan/artifact binding validation") from exc
    case_ids = [str(i) for i in range(1, len(plan["cases"]) + 1)]
    scene_map = {case["case_id"]: scene for case, scene in zip(plan["cases"], case_ids)}
    checked_index = {(r["case_id"], r["arm"]): r for r in checked["rows"]}
    records = []
    for original in original_rows:
        key = (original["case_id"], original["arm"])
        verified = checked_index[key]
        for field in ("status", "record_sha256", "schema_pass", "artifact_bindings_verified"):
            if original.get(field) != verified.get(field):
                raise DiagnosticError("native imported metadata conflicts with revalidated artifacts")
        raw = original["record"]
        # Keep the importer's exact blocked/unavailable/missing status even when
        # no original request/result record exists for this planned position.
        if raw is None:
            records.append({"case_id": scene_map[original["case_id"]], "arm": original["arm"],
                            "status": original["status"], "record_present": False,
                            "output_sha256": None, "schema_pass": False,
                            "artifact_bindings_verified": False, "safety": original["safety"]})
            continue
        identity = plan["arms"][original["arm"]]
        record = {"case_id": scene_map[original["case_id"]], "arm": original["arm"],
                  "status": original["status"], "record_sha256": original["record_sha256"],
                  "schema_pass": original["schema_pass"], "safety": original["safety"],
                  "artifact_bindings_verified": verified["artifact_bindings_verified"],
                  "input_sha256": raw["input_sha256"], "output_sha256": raw["output_sha256"],
                  "runner_source_sha256": raw["runner_source_sha256"]}
        for field in ("base_revision", "tokenizer_sha256", "generation_settings",
                      "model_manifest_sha256", "adapter_sha256"):
            if field in identity:
                record[field] = identity[field]
        records.append(record)
    adapted_reviews = []
    for raw in reviews:
        review = dict(_mapping(raw, "native review"))
        native_id = review.get("case_id")
        if native_id not in scene_map:
            raise DiagnosticError("native review refers to an unknown case")
        review["case_id"] = scene_map[native_id]
        adapted_reviews.append(review)
    return {
        "schema": SCHEMA,
        "evidence_scope": "original_R5_plan_and_artifacts_revalidated_locally_read_only",
        "plan": {"case_ids": case_ids, "arms": dict(plan["arms"]),
                 "plan_sha256": plan["sha256"], "case_manifest_sha256": plan["case_manifest_sha256"]},
        "records": records, "pilot": dict(pilot), "reviews": adapted_reviews,
    }


def render_markdown(report: Mapping[str, Any]) -> str:
    counts, comparison, reviews = report["counts"], report["comparison"], report["reviews"]
    lines = ["# AM 表达评测诊断", "",
             f"计划 {counts['planned_positions']} 个位置；完成输出 {counts['completed_outputs']} 个；"
             f"不同正文哈希 {counts['distinct_completed_texts']} 个；"
             f"无输出、保持未评分 {counts['unscored_no_output_positions']} 个。", "",
             "| 组别 | 计划 | 完成输出 | 无输出未评分 |", "| --- | ---: | ---: | ---: |"]
    for arm, values in report["arms"].items():
        lines.append(f"| {arm} | {values['planned_positions']} | {values['completed_outputs']} | "
                     f"{values['unscored_no_output_positions']} |")
    lines += ["", "## 配对与评价", "",
              f"严格可比 MLX 配对：{comparison['verified_comparable_pairs']}/"
              f"{comparison['planned_pairs']}；其中逐字哈希相同 {comparison['identical_verified_pairs']} 对。",
              f"文本结论：{comparison['text_conclusion']}。自然度改善未由本诊断工具判定。",
              f"真人评分 {reviews['human']['rating_count']} 条；"
              f"授权 GPT 代评 {reviews['gpt_proxy']['rating_count']} 条；两者分别统计。",
              "Q8 仅作为外部体验参照；缺失绑定不会由场景编号或公开摘要补齐。", "",
              "## 训练机制与限制", "",
              f"训练机制证据：{report['training']['status']}。",
              f"Token 分布探针：{report['distribution_probe']['status']}。",
              "保存后与重载后的诊断差为零，不代表基座与 SFT 的 token 分布相同。",
              "本报告不会启动训练、启用生产表达、改写原始结果或授予业务验收。", ""]
    blocked = [p for p in report["mlx_pairs"] if not p["comparable"]]
    if blocked:
        lines += ["## 配对证据缺口", ""]
        for pair in blocked:
            # Case identifiers are data; prevent Markdown line/control injection.
            case = json.dumps(pair["case_id"], ensure_ascii=False)
            lines.append(f"- 场景 {case}：{', '.join(pair['reasons'])}。")
        lines.append("")
    return "\n".join(lines)


def _reject_constant(value: str) -> None:
    raise DiagnosticError("JSON contains a non-finite numeric constant")


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise DiagnosticError("JSON contains duplicate object keys")
        result[key] = value
    return result


def _load(path: str) -> Any:
    payload = Path(path).read_bytes()
    if len(payload) > 8 * 1024 * 1024:
        raise DiagnosticError("input exceeds the 8 MiB metadata limit")
    return json.loads(payload.decode("utf-8"), object_pairs_hook=_unique_object,
                      parse_constant=_reject_constant)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    normalized = sub.add_parser("diagnose", help="read a normalized metadata bundle")
    normalized.add_argument("--bundle", required=True)
    public = sub.add_parser("r8-public", help="adapt the existing public R8 evidence files")
    public.add_argument("--plan-binding", required=True)
    public.add_argument("--evaluation-import", required=True)
    public.add_argument("--pilot", required=True)
    public.add_argument("--input-bindings")
    public.add_argument("--reviews", help="JSON array; proxy/human identity must be explicit")
    native = sub.add_parser("native-r5", help="read original private R5/R8 plan and imported results locally")
    native.add_argument("--plan", required=True)
    native.add_argument("--results", required=True)
    native.add_argument("--pilot", required=True)
    native.add_argument("--reviews", help="normalized JSON array using original case_ids")
    for command in (normalized, public, native):
        command.add_argument("--format", choices=("json", "markdown"), default="json")
        command.add_argument("--output", help="new report only; existing files are never overwritten")
    args = parser.parse_args(argv)
    try:
        if args.command == "diagnose":
            bundle = _load(args.bundle)
        elif args.command == "native-r5":
            bundle = adapt_native_r5(_load(args.plan), _load(args.results), _load(args.pilot),
                                     reviews=_load(args.reviews) if args.reviews else [])
        else:
            bundle = adapt_r8_public(_load(args.plan_binding), _load(args.evaluation_import),
                                     _load(args.pilot),
                                     input_bindings=_load(args.input_bindings) if args.input_bindings else None,
                                     reviews=_load(args.reviews) if args.reviews else [])
        report = diagnose(bundle)
        output = (render_markdown(report) if args.format == "markdown" else
                  json.dumps(report, ensure_ascii=False, indent=2, allow_nan=False) + "\n")
        if args.output:
            fd = os.open(args.output, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            with os.fdopen(fd, "w", encoding="utf-8") as stream:
                stream.write(output)
        else:
            sys.stdout.write(output)
    except (DiagnosticError, OSError, UnicodeError, json.JSONDecodeError) as exc:
        print(f"diagnostics error: {exc}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
