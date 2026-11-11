"""Bind a private, declared holdout before a pilot can produce executable argv.

This gate uses the actual gold snapshot and frozen cases, without inventing an
SFT adapter or an evaluation model arm.  It checks retained evidence bytes and
the operator's declaration; neither a hash nor an ``origin`` field authenticates
a person or proves that undisclosed tuning did not occur.
"""
from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
import hashlib
from pathlib import Path

from am_style.data import assert_split_integrity

from .common import R5Error, check_seal, loads, object_hash, require_sha, seal
from .evaluation import _exclusion_snapshot, freeze_cases


GATE_SCHEMA = "am-style-r5-pilot-holdout-gate/1"
PLAN_SCHEMA = "am-style-r5-pilot-plan/1"
_INPUT_ERRORS = (OSError, ValueError, KeyError, TypeError, AttributeError, OverflowError)


def _nonempty(value, label):
    if not isinstance(value, str) or not value.strip():
        raise R5Error("missing " + label)
    return value


def _timestamp(value, label):
    value = _nonempty(value, label)
    stamp = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if stamp.tzinfo is None or stamp.utcoffset() is None:
        raise R5Error(label + " must include a timezone")
    return stamp.astimezone(timezone.utc)


def _read_file(path, label):
    path = Path(path).expanduser()
    if not path.is_absolute():
        raise R5Error(label + " must be an absolute path")
    path = path.resolve()
    if not path.is_file():
        raise R5Error("missing " + label)
    raw = path.read_bytes()
    if not raw.strip():
        raise R5Error("empty " + label)
    # Parse and hash the same read; do not bind a pre-read hash to later bytes.
    return raw, {"path": str(path), "sha256": hashlib.sha256(raw).hexdigest()}


def _json_evidence(raw, label):
    try:
        return loads(raw.decode("utf-8"))
    except (ValueError, TypeError) as exc:
        # Duplicate keys or malformed JSON may themselves contain private text.
        raise R5Error("invalid " + label + " JSON") from exc


def _plan_context(plan):
    if not isinstance(plan, dict) or plan.get("schema") != PLAN_SCHEMA:
        raise R5Error("invalid pilot plan for holdout binding")
    return object_hash({key: value for key, value in plan.items()
                        if key not in {"sha256", "holdout_gate"}})


def _build_binding(plan, cases_path, declaration_path, original_path, bound_at):
    context_sha = _plan_context(plan)
    binding_time = _timestamp(bound_at, "holdout binding time")
    if binding_time > datetime.now(timezone.utc):
        raise R5Error("holdout binding time is in the future")

    snapshot_ref = plan["snapshot"]
    snapshot_path = Path(snapshot_ref["path"])
    if not snapshot_path.is_absolute():
        raise R5Error("snapshot must be an absolute path")
    exclusion = _exclusion_snapshot(snapshot_path)
    if (exclusion["snapshot_file_sha256"] != require_sha(snapshot_ref.get("sha256"), "snapshot file SHA")
            or exclusion["snapshot_content_sha256"] != require_sha(plan.get("snapshot_content_sha256"), "snapshot content SHA")):
        raise R5Error("holdout exclusion differs from the actual pilot gold snapshot")

    cases_raw, cases_ref = _read_file(cases_path, "frozen independent holdout")
    bundle = _json_evidence(cases_raw, "frozen holdout")
    if not isinstance(bundle, dict) or bundle.get("schema_version") != "am-r5-frozen-cases/1":
        raise R5Error("holdout must be a frozen case bundle produced by eval-freeze")
    frozen = freeze_cases(bundle)
    train_groups = sorted({row["group_id"] for row in exclusion["records"] if row["split"] == "train"})
    dev_groups = sorted({row["group_id"] for row in exclusion["records"] if row["split"] == "dev"})
    excluded_groups = set(train_groups) | set(dev_groups)
    held_rows = []
    for case in frozen["cases"]:
        if (case["split"] != "sealed_test"
                or case.get("independent_test_eligible", case.get("independent")) is not True
                or case.get("public_exposed", case.get("public_example")) is not False):
            raise R5Error("pilot holdout must contain only private independent sealed_test cases")
        if case["group_id"] in excluded_groups:
            raise R5Error("holdout group overlaps the actual pilot train/dev snapshot")
        held_rows.append({"id": "holdout:" + case["case_id"], "group_id": case["group_id"],
                          "parent_group_id": case.get("parent_group_id", case["group_id"]),
                          "split": "sealed_test", "prompt": case["student_wire_messages"],
                          "fact_entities": deepcopy(case.get("fact_entities", []))})
    try:
        assert_split_integrity(exclusion["records"] + held_rows)
    except ValueError as exc:
        raise R5Error("holdout leaks the actual pilot train/dev prompts or groups") from exc

    declaration_raw, declaration_ref = _read_file(declaration_path, "holdout declaration")
    declaration = _json_evidence(declaration_raw, "holdout declaration")
    if (not isinstance(declaration, dict) or declaration.get("origin", "human") != "human"
            or declaration.get("not_used_for_tuning") is not True
            or declaration.get("case_manifest_sha256") != frozen["case_manifest_sha256"]):
        raise R5Error("a case-bound human unused-for-tuning declaration is required")
    _nonempty(declaration.get("declared_by"), "holdout declaration author")
    declared_at = _timestamp(declaration.get("declared_at"), "holdout declaration time")
    if declared_at > binding_time:
        raise R5Error("holdout declaration must not be later than its binding")
    _, original_ref = _read_file(original_path, "holdout declaration original")
    if require_sha(declaration.get("source_evidence_sha256"), "declaration original SHA") != original_ref["sha256"]:
        raise R5Error("holdout declaration original SHA does not match the retained file")

    return seal({"schema": GATE_SCHEMA, "bound_at_utc": bound_at,
        "plan_context_sha256": context_sha,
        "snapshot": {"path": exclusion["path"], "sha256": exclusion["snapshot_file_sha256"]},
        "snapshot_content_sha256": exclusion["snapshot_content_sha256"],
        "training_files_sha256": exclusion["files_sha256"],
        "cases": cases_ref, "case_bundle_sha256": frozen["sha256"],
        "case_manifest_sha256": frozen["case_manifest_sha256"],
        "case_count": frozen["case_count"], "holdout_group_count": len(frozen["groups"]),
        "training_group_count": len(train_groups), "development_group_count": len(dev_groups),
        "group_partition_sha256": object_hash({"holdout": frozen["groups"], "train": train_groups, "dev": dev_groups}),
        "declaration": declaration_ref, "declaration_content_sha256": object_hash(declaration),
        "declaration_original": original_ref, "not_used_for_tuning": True,
        "sample_status": "independent_declared",
        "identity_limit": "retained_bytes_and_declared_provenance_are_checked; hashes_do_not_authenticate_people_or_independence"})


def bind_holdout(plan, *, cases_path=None, declaration_path=None, original_path=None):
    """Return an immutable input binding, or None for metadata-only preparation.

    The three evidence paths must be supplied together. No model, subprocess,
    service mutation, training artifact or human approval is created here.
    """
    supplied = [value is not None for value in (cases_path, declaration_path, original_path)]
    if not any(supplied):
        return None
    if not all(supplied):
        raise R5Error("supply holdout cases, declaration and declaration original together")
    try:
        return _build_binding(plan, cases_path, declaration_path, original_path,
                              datetime.now(timezone.utc).isoformat())
    except R5Error:
        raise
    except _INPUT_ERRORS as exc:
        raise R5Error("invalid holdout input evidence (" + type(exc).__name__ + ")") from exc


def verify_holdout(plan):
    """Freshly rederive the gate from actual files; never trust a cached green flag."""
    try:
        gate = plan.get("holdout_gate") if isinstance(plan, dict) else None
        if not isinstance(gate, dict):
            raise R5Error("pilot execution requires frozen independent holdout, human declaration and retained original")
        check_seal(gate)
        if gate.get("schema") != GATE_SCHEMA:
            raise R5Error("unsupported pilot holdout gate")
        if gate.get("plan_context_sha256") != _plan_context(plan):
            raise R5Error("pilot plan changed after the holdout binding")
        rebuilt = _build_binding(plan, gate["cases"]["path"], gate["declaration"]["path"],
                                 gate["declaration_original"]["path"], gate["bound_at_utc"])
        if rebuilt != gate:
            raise R5Error("holdout cases, declaration, original or gold binding changed")
        return gate
    except R5Error:
        raise
    except _INPUT_ERRORS as exc:
        raise R5Error("invalid or missing pilot holdout evidence (" + type(exc).__name__ + ")") from exc
