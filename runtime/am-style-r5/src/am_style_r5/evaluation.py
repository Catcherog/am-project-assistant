"""Frozen three-arm evaluation, artifact binding, and separated human review.

This module never invokes a model, sends a message, trains, or enables production.
Content seals detect changed evidence; they are not authentication of a human or host.
"""
from __future__ import annotations

from collections import Counter, defaultdict
from copy import deepcopy
import math
from pathlib import Path
import re
import secrets

from .common import R5Error, canonical, check_seal, file_hash, loads, object_hash, read_jsonl, require_sha, seal, text_hash


PRIMARY_ARMS = ("production_q8", "mlx_base", "mlx_sft")
STATUSES = {"completed", "error", "failed", "unknown", "stop_unknown", "truncated", "missing", "blocked", "unavailable", "timeout", "cancelled"}
MODES = {"real", "fixture"}
FACT_DECISIONS = {"pass", "fail", "cannot_judge"}
PREFERENCE_DECISIONS = {"left", "right", "tie", "both_bad", "cannot_judge"}
MLX_SHARED = ("base_revision", "model_manifest_sha256", "tokenizer_sha256", "generation_settings")


def _nonempty(value, label):
    if not isinstance(value, str) or not value.strip():
        raise R5Error("missing " + label)
    return value


def _messages(value, label):
    if not isinstance(value, list) or not value:
        raise R5Error("missing " + label)
    for message in value:
        if not isinstance(message, dict) or not isinstance(message.get("role"), str) or not isinstance(message.get("content"), str):
            raise R5Error("invalid " + label)
    return deepcopy(value)


def _groups(values, label):
    values = list(values)
    if any(not isinstance(x, str) or not x for x in values):
        raise R5Error("invalid " + label)
    return sorted(set(values))


def _candidate(raw):
    try:
        value = loads(raw)
    except (ValueError, TypeError):
        return None
    if not isinstance(value, dict) or set(value) != {"text"}:
        return None
    value = value["text"]
    return value if isinstance(value, str) and value.strip() and len(value) <= 6000 else None


def freeze_cases(cases):
    """Freeze input metadata before an adapter exists; does not certify independence.

    Accepts a case list or a previously frozen bundle, which is fully rederived.
    The case_manifest_sha256 is exactly the one later bound into build_plan().
    """
    original = cases if isinstance(cases, dict) else None
    if original is not None:
        check_seal(original)
        if original.get("schema_version") != "am-r5-frozen-cases/1":
            raise R5Error("unknown frozen case bundle")
        cases = original["cases"]
    if not isinstance(cases, (list, tuple)):
        raise R5Error("cases must be a list or frozen case bundle")
    normalized, case_ids = [], set()
    for raw in cases:
        if not isinstance(raw, dict):
            raise R5Error("invalid evaluation case")
        row = deepcopy(raw)
        ident = _nonempty(row.get("case_id"), "case_id")
        if ident in case_ids:
            raise R5Error("duplicate case_id")
        case_ids.add(ident)
        _nonempty(row.get("group_id"), "group_id")
        if row.get("split") == "holdout":
            row["split"] = "sealed_test"
        if row.get("split") not in {"sealed_test", "calibration"}:
            raise R5Error("evaluation cases must use sealed_test or calibration")
        if not isinstance(row.get("frame"), dict) or not row["frame"]:
            raise R5Error("missing frozen frame")
        row["logical_messages"] = _messages(row.get("logical_messages"), "logical messages")
        row["student_wire_messages"] = _messages(row.get("student_wire_messages"), "student transport messages")
        try:
            from am_style.contracts import SpeechFrame, StylePolicy
            from am_style.prompt import build_messages, coordinator_messages
            frozen = SpeechFrame.freeze(row["frame"])
            policy = StylePolicy(**row.get("style_policy", {}))
            expected_logical = build_messages(frozen, policy)
            expected_wire = coordinator_messages(expected_logical)
        except (ImportError, ValueError, TypeError, KeyError) as exc:
            raise R5Error("cannot validate pinned frame/prompt/transport:" + type(exc).__name__) from exc
        if row["logical_messages"] != expected_logical or row["student_wire_messages"] != expected_wire:
            raise R5Error("frame/policy/logical prompt/student transport disagree")
        row["style_policy"] = policy.as_dict()
        if not isinstance(row.get("review_context"), (str, dict)) or not row["review_context"]:
            raise R5Error("human factual review requires review_context")
        if ("independent_test_eligible" in row and "independent" in row and row["independent_test_eligible"] != row["independent"]
                or "public_exposed" in row and "public_example" in row and row["public_exposed"] != row["public_example"]):
            raise R5Error("conflicting exposure/independence metadata")
        row["frame_sha256"] = object_hash(row["frame"])
        row["logical_messages_sha256"] = object_hash(row["logical_messages"])
        row["student_wire_messages_sha256"] = object_hash(row["student_wire_messages"])
        row["input_sha256"] = object_hash({key: row[key] for key in ("frame", "logical_messages", "student_wire_messages")})
        normalized.append(row)
    if not normalized:
        raise R5Error("empty evaluation denominator")
    result = seal({"schema_version": "am-r5-frozen-cases/1", "cases": normalized,
                   "case_manifest_sha256": object_hash(normalized), "case_count": len(normalized),
                   "groups": sorted({row["group_id"] for row in normalized}), "independence": "not_assessed"})
    if original is not None and result != original:
        raise R5Error("changed frozen case derivations")
    return result


def _exclusion_snapshot(path):
    from .dataset import verify_gold_snapshot
    path = Path(path).resolve()
    snapshot = verify_gold_snapshot(path)
    records = []
    for split in ("train", "dev"):
        source = (path.parent / snapshot["files"][split]["path"]).resolve()
        if not source.is_relative_to(path.parent) or file_hash(source) != snapshot["files"][split]["sha256"]:
            raise R5Error("changed snapshot exclusion file")
        for row in read_jsonl(source):
            group = row["provenance"]["group_id"]
            records.append({"id": row["id"], "group_id": group, "parent_group_id": group,
                            "split": split, "prompt": deepcopy(row["prompt"]),
                            "fact_entities": deepcopy(row.get("fact_entities", []))})
    return {"path": str(path), "snapshot_file_sha256": file_hash(path), "snapshot_content_sha256": snapshot["sha256"],
            "files_sha256": {split: snapshot["files"][split]["sha256"] for split in ("train", "dev")},
            "records": records}


def build_plan(cases, arms, *, mode="real", training_groups=(), development_groups=(), holdout_declaration=None,
               exclusion_snapshot_path=None):
    """Build one immutable denominator. Public examples must use mode='fixture'.

    Public development examples use split='calibration' and remain not_independent,
    even when the model calls are real. Real sealed_test cases also require an unused
    for tuning declaration bound to the deterministic case_manifest_sha256.
    """
    if mode not in MODES:
        raise R5Error("invalid evaluation mode")
    if not isinstance(arms, dict) or not set(PRIMARY_ARMS) <= set(arms):
        raise R5Error("all three primary arms must remain in the plan")
    train = _groups(training_groups, "training groups")
    dev = _groups(development_groups, "development groups")
    exclusion = _exclusion_snapshot(exclusion_snapshot_path) if exclusion_snapshot_path is not None else None
    if exclusion is not None:
        train = sorted(set(train) | {r["group_id"] for r in exclusion["records"] if r["split"] == "train"})
        dev = sorted(set(dev) | {r["group_id"] for r in exclusion["records"] if r["split"] == "dev"})
    if set(train) & set(dev):
        raise R5Error("training/development group overlap")
    identities = deepcopy(arms)
    for name, identity in identities.items():
        if not isinstance(identity, dict):
            raise R5Error("invalid model identity:" + name)
        if name not in PRIMARY_ARMS and identity.get("availability") in {"unavailable", "blocked"}:
            _nonempty(identity.get("reason"), "optional arm unavailability reason")
            continue
        if not isinstance(identity.get("generation_settings"), dict) or not identity["generation_settings"]:
            raise R5Error("missing generation settings:" + name)
        if identity.get("availability", "available") not in {"available", "unavailable", "blocked"}:
            raise R5Error("invalid arm availability")
        if name in {"mlx_base", "mlx_sft"}:
            revision = _nonempty(identity.get("base_revision"), "immutable base revision")
            if re.fullmatch(r"[a-f0-9]{40}|[a-f0-9]{64}", revision) is None:
                raise R5Error("MLX base revision must be an immutable commit or content digest")
            require_sha(identity.get("model_manifest_sha256"), "model manifest hash")
            require_sha(identity.get("tokenizer_sha256"), "tokenizer hash")
        elif name == "production_q8":
            require_sha(identity.get("model_digest"), "production model digest")
    for key in MLX_SHARED:
        if identities["mlx_base"].get(key) != identities["mlx_sft"].get(key):
            raise R5Error("MLX comparison differs beyond adapter:" + key)
    if identities["mlx_base"].get("adapter_sha256") is not None:
        raise R5Error("mlx_base must have no adapter")
    require_sha(identities["mlx_sft"].get("adapter_sha256"), "SFT adapter hash")
    frozen_cases = freeze_cases(cases)
    normalized = frozen_cases["cases"]
    for row in normalized:
        if row["split"] == "sealed_test" and row["group_id"] in set(train) | set(dev):
            raise R5Error("sealed test group overlaps training/development")
        independent = row.get("independent_test_eligible", row.get("independent"))
        exposed = row.get("public_exposed", row.get("public_example"))
        if mode == "real" and row["split"] == "sealed_test" and (independent is not True or exposed is not False):
            raise R5Error("real holdout cannot reuse public or unsealed development examples")
    manifest = frozen_cases["case_manifest_sha256"]
    declaration = deepcopy(holdout_declaration)
    has_sealed = any(row["split"] == "sealed_test" for row in normalized)
    all_sealed = all(row["split"] == "sealed_test" for row in normalized)
    if has_sealed and exclusion is not None:
        from am_style.data import assert_split_integrity
        sealed_rows = [{"id": "evaluation:" + row["case_id"], "group_id": row["group_id"], "parent_group_id": row["group_id"],
                        "split": "sealed_test", "prompt": row["student_wire_messages"],
                        "fact_entities": deepcopy(row.get("fact_entities", []))} for row in normalized if row["split"] == "sealed_test"]
        try:
            assert_split_integrity(exclusion["records"] + sealed_rows)
        except ValueError as exc:
            raise R5Error("sealed test leaks training/development prompt:" + str(exc)) from exc
    if mode == "real" and has_sealed:
        if exclusion is None:
            raise R5Error("real sealed test requires a verified gold exclusion snapshot")
        for split in ("train", "dev"):
            training_hash = require_sha(identities["mlx_sft"].get(split + "_file_sha256"), "selected SFT " + split + " file hash")
            if training_hash != exclusion["files_sha256"][split]:
                raise R5Error("selected SFT training data differs from sealed-test exclusion snapshot:" + split)
        if not isinstance(declaration, dict) or declaration.get("case_manifest_sha256") != manifest or declaration.get("not_used_for_tuning") is not True:
            raise R5Error("missing case-bound unused-for-tuning human declaration")
        _nonempty(declaration.get("declared_by"), "declaration author")
        _nonempty(declaration.get("declared_at"), "declaration time")
        require_sha(declaration.get("source_evidence_sha256"), "declaration source evidence")
    return seal({
        "schema_version": "am-r5-evaluation-plan/1", "mode": mode,
        "sample_status": "independent_declared" if mode == "real" and all_sealed else "not_independent",
        "cases": normalized, "arms": identities, "primary_arms": list(PRIMARY_ARMS),
        "case_manifest_sha256": manifest, "training_groups": train, "development_groups": dev,
        "holdout_groups": sorted({c["group_id"] for c in normalized if c["split"] == "sealed_test"}),
        "evaluation_groups": sorted({c["group_id"] for c in normalized}), "holdout_declaration": declaration,
        "exclusion_snapshot": exclusion,
        "production_q8_role": "external_reference_only_not_adapter_causal_control",
        "expected_results": len(normalized) * len(identities),
    })


def validate_plan(plan):
    """Rebuild every derivation, not just a recomputable JSON seal."""
    check_seal(plan)
    if plan.get("schema_version") != "am-r5-evaluation-plan/1":
        raise R5Error("unknown evaluation plan schema")
    rebuilt = build_plan(plan["cases"], plan["arms"], mode=plan["mode"],
                         training_groups=plan["training_groups"], development_groups=plan["development_groups"],
                         holdout_declaration=plan.get("holdout_declaration"),
                         exclusion_snapshot_path=plan["exclusion_snapshot"]["path"] if plan.get("exclusion_snapshot") else None)
    if rebuilt != plan:
        raise R5Error("evaluation plan derivations changed")
    return plan


def _case(plan, case_id):
    for case in plan["cases"]:
        if case["case_id"] == case_id:
            return case
    raise R5Error("unknown case_id")


def make_result_record(plan, case_id, arm, *, status, runner_source_sha256,
                       request_artifact="", response_artifact="", raw_output="", latency_ms=None,
                       capture_kind=None, request_binding=None, response_binding=None, error=None):
    """Create an evidence record; actual captured artifacts still undergo import checks.

    request_binding selects wire_messages and generation_settings from the original
    request capture. response_binding selects the exact generated raw_output. Selectors
    support path, decode_json, after_decode_path and transform='coordinator_messages'.
    """
    validate_plan(plan)
    case = _case(plan, case_id)
    if arm not in plan["arms"] or status not in STATUSES:
        raise R5Error("unknown arm or result status")
    require_sha(runner_source_sha256, "runner source hash")
    if any(not isinstance(v, str) for v in (request_artifact, response_artifact, raw_output)):
        raise R5Error("artifacts and raw output must be exact UTF-8 strings")
    candidate = _candidate(raw_output) if status == "completed" else None
    return seal({
        "schema_version": "am-r5-result/1", "plan_sha256": plan["sha256"],
        "case_id": case_id, "arm": arm, "mode": plan["mode"], "status": status,
        "runner_source_sha256": runner_source_sha256,
        **{key: case[key] for key in ("input_sha256", "frame_sha256", "logical_messages_sha256", "student_wire_messages_sha256")},
        "model_identity_sha256": object_hash(plan["arms"][arm]),
        "request_artifact": request_artifact, "request_artifact_sha256": text_hash(request_artifact),
        "response_artifact": response_artifact, "response_artifact_sha256": text_hash(response_artifact),
        "raw_output": raw_output, "raw_output_sha256": text_hash(raw_output),
        "output_text": candidate, "output_sha256": text_hash(candidate) if candidate is not None else None,
        "latency_ms": deepcopy(latency_ms or {}), "capture_kind": capture_kind or ("fixture" if plan["mode"] == "fixture" else "unverified"),
        "request_binding": deepcopy(request_binding), "response_binding": deepcopy(response_binding),
        "error": error,
    })


def _select(artifact, selector):
    if not isinstance(selector, dict):
        raise R5Error("missing artifact selector")
    if selector.get("format") == "text":
        return artifact
    value = loads(artifact)
    def walk(current, path):
        if not isinstance(path, list):
            raise R5Error("artifact selector path must be a list")
        try:
            for key in path:
                if isinstance(current, list) and type(key) is not int:
                    raise R5Error("array selector needs integer")
                if isinstance(current, dict) and not isinstance(key, str):
                    raise R5Error("object selector needs string")
                current = current[key]
        except (KeyError, IndexError, TypeError) as exc:
            raise R5Error("artifact binding path missing") from exc
        return current
    value = walk(value, selector.get("path", []))
    if selector.get("decode_json"):
        if not isinstance(value, str):
            raise R5Error("artifact decoder requires string")
        value = walk(loads(value), selector.get("after_decode_path", []))
    transform = selector.get("transform")
    if transform == "coordinator_messages":
        try:
            from am_style.prompt import coordinator_messages
        except ImportError as exc:
            raise R5Error("pinned coordinator transform unavailable") from exc
        value = coordinator_messages(value)
    elif transform is not None:
        raise R5Error("unknown artifact transform")
    return value


def default_verifier(case, text):
    """Pinned R4 bounded/natural safety; never a human naturalness score."""
    try:
        import am_style.verify as verify_module
        import am_style.natural as natural_module
        from am_style.contracts import SpeechFrame, StylePolicy
        from am_style.natural import NaturalSurfaceVerifier
        from am_style.verify import CompositeVerifier
    except ImportError:
        return {"decision": "indeterminate", "reason": "pinned_verifier_unavailable", "verifier": "unavailable"}
    try:
        result = CompositeVerifier(NaturalSurfaceVerifier()).verify(
            SpeechFrame.freeze(case["frame"]), text, StylePolicy(**case.get("style_policy", {})))
        return {"decision": result.decision, "reason": result.reason, "verifier": "R4_bounded_plus_natural",
                "source_sha256": object_hash({"verify": file_hash(verify_module.__file__), "natural": file_hash(natural_module.__file__)})}
    except (ValueError, TypeError, KeyError) as exc:
        return {"decision": "indeterminate", "reason": "verifier_input_error:" + type(exc).__name__, "verifier": "R4_bounded_plus_natural"}


def import_results(plan, records, *, verifier=None):
    """Validate bindings and preserve every planned case/arm, including missing rows."""
    validate_plan(plan)
    validate = verifier or default_verifier
    provided = {}
    for original in records:
        check_seal(original)
        row = deepcopy(original)
        if row.get("schema_version") != "am-r5-result/1" or row.get("plan_sha256") != plan["sha256"] or row.get("mode") != plan["mode"]:
            raise R5Error("foreign plan, schema, or fixture/real mixing")
        case = _case(plan, row.get("case_id"))
        arm = row.get("arm")
        if arm not in plan["arms"] or row.get("status") not in STATUSES:
            raise R5Error("unknown arm or status")
        key = (case["case_id"], arm)
        if key in provided:
            raise R5Error("duplicate result; retries require a separately frozen attempt plan")
        require_sha(row.get("runner_source_sha256"), "runner source hash")
        for name in ("input_sha256", "frame_sha256", "logical_messages_sha256", "student_wire_messages_sha256"):
            if row.get(name) != case[name]:
                raise R5Error("changed evaluation input:" + name)
        if row.get("model_identity_sha256") != object_hash(plan["arms"][arm]):
            raise R5Error("changed model identity or settings")
        for field in ("request_artifact", "response_artifact", "raw_output"):
            if not isinstance(row.get(field), str) or row.get(field + "_sha256") != text_hash(row[field]):
                raise R5Error("changed artifact:" + field)
        latency = row.get("latency_ms", {})
        if not isinstance(latency, dict):
            raise R5Error("invalid latency record")
        for value in latency.values():
            if value is not None and (isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value < 0):
                raise R5Error("invalid latency measurement")
        is_completed = row["status"] == "completed"
        candidate = _candidate(row["raw_output"]) if is_completed else None
        if row.get("output_text") != candidate or row.get("output_sha256") != (text_hash(candidate) if candidate is not None else None):
            raise R5Error("changed parsed output")
        actual = plan["mode"] == "real"
        expected_kind = "actual_provider_exchange" if actual else "fixture"
        if row.get("capture_kind") != expected_kind:
            raise R5Error("unverified or mixed capture kind")
        if is_completed:
            if not row["request_artifact"] or not row["response_artifact"] or not row["raw_output"]:
                raise R5Error("completed result missing original exchange artifacts")
            binding = row.get("request_binding")
            if not isinstance(binding, dict):
                raise R5Error("completed result missing request bindings")
            if _select(row["request_artifact"], binding.get("wire_messages")) != case["student_wire_messages"]:
                raise R5Error("original request used different student transport")
            if _select(row["request_artifact"], binding.get("generation_settings")) != plan["arms"][arm]["generation_settings"]:
                raise R5Error("original request used different generation settings")
            if _select(row["response_artifact"], row.get("response_binding")) != row["raw_output"]:
                raise R5Error("original response does not contain recorded output")
        safety = {"decision": "indeterminate", "reason": "no_schema_valid_completed_output", "verifier": "not_run"}
        if candidate is not None:
            safety = deepcopy(validate(case, candidate))
            if not isinstance(safety, dict) or safety.get("decision") not in {"pass", "indeterminate", "reject"}:
                raise R5Error("invalid safety verifier result")
        provided[key] = {"record": row, "record_sha256": row["sha256"], "schema_pass": candidate is not None,
                         "safety": safety, "artifact_bindings_verified": is_completed,
                         "naturalness": "not_evaluated_by_machine"}
    rows = []
    for case in plan["cases"]:
        for arm in plan["arms"]:
            key = (case["case_id"], arm)
            if key in provided:
                result = provided[key]
            else:
                status = plan["arms"][arm].get("availability", "missing")
                if status == "available":
                    status = "missing"
                result = {"record": None, "record_sha256": None, "schema_pass": False,
                          "safety": {"decision": "indeterminate", "reason": "not_executed", "verifier": "not_run"},
                          "artifact_bindings_verified": False, "naturalness": "not_evaluated_by_machine"}
            rows.append({"case_id": case["case_id"], "group_id": case["group_id"], "arm": arm,
                         "status": result["record"]["status"] if result["record"] else status, **result})
    return seal({"schema_version": "am-r5-imported-results/1", "plan_sha256": plan["sha256"],
                 "mode": plan["mode"], "rows": rows, "expected_results": plan["expected_results"],
                 "provided_results": len(provided), "production_enabled": False})


def _check_results(plan, results):
    validate_plan(plan)
    check_seal(results)
    if results.get("plan_sha256") != plan["sha256"] or results.get("mode") != plan["mode"]:
        raise R5Error("foreign result set")
    expected = {(case["case_id"], arm) for case in plan["cases"] for arm in plan["arms"]}
    actual = [(row["case_id"], row["arm"]) for row in results["rows"]]
    if len(actual) != len(expected) or set(actual) != expected:
        raise R5Error("changed denominator")
    for row in results["rows"]:
        record = row.get("record")
        if record is not None:
            check_seal(record)
            if (record["sha256"] != row.get("record_sha256") or record["case_id"] != row["case_id"]
                    or record["arm"] != row["arm"] or record["status"] != row["status"]):
                raise R5Error("changed imported record binding")


def _identity_disclosure(text, plan):
    words = set(plan["arms"])
    for identity in plan["arms"].values():
        for key in ("model", "model_name"):
            if isinstance(identity.get(key), str) and len(identity[key]) > 3:
                words.add(identity[key])
    lowered = text.lower()
    return any(word.lower() in lowered for word in words)


def blind_batch(plan, results):
    """Return (public task, private mapping). Save the mapping separately with 0600."""
    _check_results(plan, results)
    public_tasks, private_tasks, private_options = [], {}, {}
    rng = secrets.SystemRandom()
    for case in plan["cases"]:
        task_id = secrets.token_urlsafe(24)
        options = []
        for row in results["rows"]:
            if row["case_id"] != case["case_id"] or row["status"] != "completed" or not row["schema_pass"]:
                continue
            record = row["record"]
            text = record["output_text"]
            if (_identity_disclosure(text, plan) or _identity_disclosure(canonical(case["review_context"]), plan)
                    or _identity_disclosure(case["frame"]["question"], plan)):
                raise R5Error("candidate or review context discloses an arm; do not silently edit it")
            option_id = secrets.token_urlsafe(24)
            options.append({"option_id": option_id, "text": text, "text_sha256": record["output_sha256"]})
            private_options[option_id] = {"task_id": task_id, "case_id": case["case_id"], "arm": row["arm"],
                                          "record_sha256": record["sha256"], "text_sha256": record["output_sha256"]}
        if not options:
            continue
        rng.shuffle(options)
        public_tasks.append({"task_id": task_id, "question": case["frame"]["question"],
                             "review_context": deepcopy(case["review_context"]), "options": options})
        private_tasks[task_id] = case["case_id"]
    rng.shuffle(public_tasks)
    public = seal({"schema_version": "am-r5-blind-public/1", "mode": plan["mode"],
                   "sample_status": plan["sample_status"], "batch_id": secrets.token_urlsafe(24),
                   "tasks": public_tasks,
                   "rating_fields": {"fact": sorted(FACT_DECISIONS), "naturalness": "1..5 or null", "absolute_usable": "true/false/null"},
                   "relative_outcomes": sorted(PREFERENCE_DECISIONS)})
    private = seal({"schema_version": "am-r5-blind-private/1", "private": True, "mode": plan["mode"],
                    "plan_sha256": plan["sha256"], "results_sha256": results["sha256"], "public_sha256": public["sha256"],
                    "tasks": private_tasks, "options": private_options})
    return public, private


def preference_content_hash(task_id, left_option, right_option):
    """Bind a relative vote to the displayed order and exact two output texts."""
    return object_hash({"task_id": task_id, "options": [
        {"option_id": x["option_id"], "text_sha256": x["text_sha256"]} for x in (left_option, right_option)]})


def _linear(values, p):
    values = sorted(values)
    if not values:
        return None
    index = (len(values) - 1) * p
    low = math.floor(index)
    high = math.ceil(index)
    return values[low] + (values[high] - values[low]) * (index - low)


def latency_summary(values):
    values = list(values)
    if any(isinstance(x, bool) or not isinstance(x, (int, float)) or not math.isfinite(x) or x < 0 for x in values):
        raise R5Error("invalid latency values")
    return {"n": len(values), "method": "linear interpolation, index=(n-1)*p", "p50_ms": _linear(values, .5),
            "p95_ms": _linear(values, .95), "max_ms": max(values) if values else None}


def feedback_report(plan, results, public=None, private=None, feedback=None):
    """Report planned denominators, safety, human dimensions and same-input votes.

    feedback can be one dict or a list (one dict per reviewer). It is not an enable
    decision. Absence, unknown, rejects and failed requests never disappear.
    """
    _check_results(plan, results)
    all_ratings, all_preferences = [], []
    if (public is None) != (private is None):
        raise R5Error("both public task and private map are required")
    if public is not None:
        check_seal(public)
        check_seal(private)
        if (private.get("public_sha256") != public["sha256"] or private.get("plan_sha256") != plan["sha256"]
                or private.get("results_sha256") != results["sha256"] or private.get("mode") != plan["mode"]):
            raise R5Error("changed blind batch or foreign mapping")
        shown = {}
        for task in public["tasks"]:
            for option in task["options"]:
                if option["text_sha256"] != text_hash(option["text"]):
                    raise R5Error("changed blind output")
                mapping = private["options"].get(option["option_id"])
                if not mapping or mapping["task_id"] != task["task_id"] or mapping["text_sha256"] != option["text_sha256"]:
                    raise R5Error("blind option binding mismatch")
                source = next(row for row in results["rows"] if row["case_id"] == mapping["case_id"] and row["arm"] == mapping["arm"])
                if not source["record"] or source["record"]["sha256"] != mapping["record_sha256"] or source["record"]["output_sha256"] != option["text_sha256"]:
                    raise R5Error("blind mapping no longer matches imported output")
                shown[option["option_id"]] = {**option, **mapping}
        feedbacks = [] if feedback is None else feedback if isinstance(feedback, list) else [feedback]
        seen_ratings, seen_preferences = set(), set()
        for document in feedbacks:
            if document.get("public_sha256") != public["sha256"] or document.get("mode") != plan["mode"]:
                raise R5Error("feedback on different content or fixture/real mixing")
            reviewer = _nonempty(document.get("reviewer"), "reviewer")
            _nonempty(document.get("reviewed_at"), "review time")
            if plan["mode"] == "real":
                require_sha(document.get("source_evidence_sha256"), "human feedback source evidence")
                if document.get("human_authored") is not True:
                    raise R5Error("machine labels cannot count as human feedback")
            for rating in document.get("ratings", []):
                if not isinstance(rating, dict) or set(rating) - {"option_id", "text_sha256", "fact", "naturalness", "absolute_usable", "reason", "edited_text"}:
                    raise R5Error("unknown rating fields; revised text requires its own full-text review")
                option = shown.get(rating.get("option_id"))
                if not option or rating.get("text_sha256") != option["text_sha256"]:
                    raise R5Error("human rating targets different output")
                if "edited_text" in rating and rating["edited_text"] != option["text"]:
                    raise R5Error("edited text needs a new full-text review")
                key = (reviewer, option["option_id"])
                if key in seen_ratings:
                    raise R5Error("duplicate reviewer rating")
                seen_ratings.add(key)
                if rating.get("fact") not in FACT_DECISIONS:
                    raise R5Error("missing factual assessment")
                natural = rating.get("naturalness")
                if natural is not None and (type(natural) is not int or not 1 <= natural <= 5):
                    raise R5Error("naturalness must be 1..5 or null")
                if rating.get("absolute_usable") is not None and type(rating["absolute_usable"]) is not bool:
                    raise R5Error("absolute usability must be bool or null")
                all_ratings.append({**rating, "reviewer": reviewer, "case_id": option["case_id"], "arm": option["arm"]})
            for preference in document.get("preferences", []):
                if not isinstance(preference, dict) or set(preference) - {"task_id", "option_ids", "content_sha256", "outcome", "reason"}:
                    raise R5Error("unknown preference fields")
                ids = preference.get("option_ids", [])
                if len(ids) != 2 or len(set(ids)) != 2 or any(i not in shown for i in ids):
                    raise R5Error("relative assessment requires two displayed outputs")
                left, right = [shown[i] for i in ids]
                if left["case_id"] != right["case_id"] or left["task_id"] != preference.get("task_id") or left["task_id"] != right["task_id"]:
                    raise R5Error("cross-input preferences forbidden")
                if preference.get("content_sha256") != preference_content_hash(left["task_id"], left, right):
                    raise R5Error("relative preference text/order changed")
                outcome = preference.get("outcome")
                if outcome not in PREFERENCE_DECISIONS:
                    raise R5Error("invalid preference outcome")
                key = (reviewer, left["case_id"], tuple(sorted(ids)))
                if key in seen_preferences:
                    raise R5Error("duplicate relative preference")
                seen_preferences.add(key)
                ratings = {r["option_id"]: r for r in all_ratings if r["reviewer"] == reviewer}
                chosen = left if outcome == "left" else right if outcome == "right" else None
                pair_usable = (chosen is not None and left["text_sha256"] != right["text_sha256"]
                               and all(i in ratings and ratings[i]["fact"] == "pass" for i in ids)
                               and ratings[chosen["option_id"]].get("absolute_usable") is True)
                all_preferences.append({"reviewer": reviewer, "case_id": left["case_id"], "left_arm": left["arm"],
                                        "right_arm": right["arm"], "outcome": outcome,
                                        "chosen_arm": chosen["arm"] if chosen else None,
                                        "eligible_preference_evidence": pair_usable})
    elif feedback is not None:
        raise R5Error("feedback requires the frozen blind task and private mapping")
    arm_reports = {}
    for arm in plan["arms"]:
        rows = [r for r in results["rows"] if r["arm"] == arm]
        ratings = [r for r in all_ratings if r["arm"] == arm]
        by_case = defaultdict(list)
        for rating in ratings:
            by_case[rating["case_id"]].append(rating)
        factual_pass = sum(all(r["fact"] == "pass" for r in rs) for rs in by_case.values())
        usable = sum(all(r["fact"] == "pass" and r.get("absolute_usable") is True for r in rs) for rs in by_case.values())
        measured = [r["record"]["latency_ms"]["end_to_end"] for r in rows if r["status"] == "completed" and r["record"]["latency_ms"].get("end_to_end") is not None]
        attempted_measured = [r["record"]["latency_ms"]["end_to_end"] for r in rows
                              if r["record"] is not None and r["record"]["latency_ms"].get("end_to_end") is not None]
        natural = [r["naturalness"] for r in ratings if r.get("naturalness") is not None]
        arm_reports[arm] = {"planned_cases": len(rows), "statuses": dict(Counter(r["status"] for r in rows)),
                            "schema_pass": sum(r["schema_pass"] for r in rows),
                            "safety": dict(Counter(r["safety"]["decision"] for r in rows)),
                            "human_reviewed_cases": len(by_case), "human_rating_count": len(ratings),
                            "human_reviewer_count": len({r["reviewer"] for r in ratings}),
                            "human_factual_pass_cases": factual_pass, "human_usable_cases": usable,
                            "factual_pass_over_planned": factual_pass / len(rows), "usable_over_planned": usable / len(rows),
                            "naturalness_scores": natural, "naturalness_median": _linear(natural, .5),
                            "completed_end_to_end_latency": latency_summary(measured),
                            "attempt_end_to_end_latency": latency_summary(attempted_measured),
                            "attempt_latency_unobserved_records": sum(r["record"] is not None for r in rows) - len(attempted_measured),
                            "planned_without_attempt_latency": len(rows) - len(attempted_measured),
                            "latency_unobserved_or_not_completed": len(rows) - len(measured)}
    index = {(r["case_id"], r["arm"]): r for r in results["rows"]}
    paired, same_source, human_paired = [], [], []
    for case in plan["cases"]:
        a, b = [index[(case["case_id"], arm)] for arm in ("mlx_base", "mlx_sft")]
        if a["status"] == b["status"] == "completed":
            paired.append(case["case_id"])
            if a["record"]["runner_source_sha256"] == b["record"]["runner_source_sha256"]:
                same_source.append(case["case_id"])
        if all(any(r["case_id"] == case["case_id"] and r["arm"] == arm for r in all_ratings) for arm in ("mlx_base", "mlx_sft")):
            human_paired.append(case["case_id"])
    group_reports = {}
    for group in plan["evaluation_groups"]:
        rows = [r for r in results["rows"] if r["group_id"] == group]
        group_reports[group] = {}
        for arm in plan["arms"]:
            arm_rows = [r for r in rows if r["arm"] == arm]
            case_ids = {r["case_id"] for r in arm_rows}
            ratings = [r for r in all_ratings if r["arm"] == arm and r["case_id"] in case_ids]
            by_case = defaultdict(list)
            for rating in ratings:
                by_case[rating["case_id"]].append(rating)
            group_reports[group][arm] = {"planned": len(arm_rows), "statuses": dict(Counter(r["status"] for r in arm_rows)),
                "schema_pass": sum(r["schema_pass"] for r in arm_rows),
                "safety": dict(Counter(r["safety"]["decision"] for r in arm_rows)),
                "human_reviewed_cases": len(by_case),
                "human_factual_pass_cases": sum(all(r["fact"] == "pass" for r in rs) for rs in by_case.values()),
                "human_usable_cases": sum(all(r["fact"] == "pass" and r.get("absolute_usable") is True for r in rs) for rs in by_case.values()),
                "naturalness_median": _linear([r["naturalness"] for r in ratings if r.get("naturalness") is not None], .5)}
    mlx_preferences = [p for p in all_preferences if {p["left_arm"], p["right_arm"]} == {"mlx_base", "mlx_sft"}
                       and p["case_id"] in same_source]
    votes = Counter(p["chosen_arm"] or p["outcome"] for p in mlx_preferences)
    return seal({"schema_version": "am-r5-evaluation-report/1", "plan_sha256": plan["sha256"],
                 "results_sha256": results["sha256"], "mode": plan["mode"], "sample_status": plan["sample_status"],
                 "planned_cases": len(plan["cases"]), "planned_results": plan["expected_results"], "arms": arm_reports,
                 "groups": group_reports, "mlx_comparison": {"completed_pairs": len(paired),
                     "same_runner_source_pairs": len(same_source), "human_rated_pairs": len(human_paired),
                     "relative_vote_count": len(mlx_preferences), "relative_case_count": len({p["case_id"] for p in mlx_preferences}),
                     "relative_votes": dict(votes), "eligible_preference_evidence": sum(p["eligible_preference_evidence"] for p in mlx_preferences),
                     "source_mismatch_case_ids": sorted(set(paired) - set(same_source)),
                     "paired_case_ids": paired},
                 "human_preferences": all_preferences, "production_q8_role": plan["production_q8_role"],
                 "confidence_interval": None,
                 "uncertainty_note": "No independence-assuming interval: cases may share scenario groups; report group counts and paired coverage. Small exposed samples cannot establish naturalness improvement.",
                 "naturalness_improvement": "not_established_by_automated_report",
                 "production_enabled": False, "business_acceptance": False,
                 "limitations": ["Hash binding does not authenticate people, execution hosts or declarations.",
                                 "Schema/safety/NLL do not establish human naturalness.",
                                 "Only matched MLX base/SFT runs can estimate adapter effects; Q8 is an external reference."]})
