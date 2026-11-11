"""Frozen scenario groups and explicit human-review imports for the R5 pilot.

No model is invoked and no proposal receives a label here. Content hashes bind
files and review context; they cannot authenticate a human or a model invocation.
The fixed R4 human gold remains byte-for-byte at the beginning of every snapshot.
"""
from __future__ import annotations

from copy import deepcopy
from datetime import datetime
from pathlib import Path
import re

from am_style.contracts import SpeechFrame, StylePolicy
from am_style.data import (DataError, assert_split_integrity, normalized_prompt,
                           split_for_group, validate_messages)
from am_style.review import (BATCH_SCHEMA, _context as r4_context,
                            feedback_template as r4_feedback_template,
                            import_feedback as r4_import_feedback,
                            validate_review_batch as r4_validate_review_batch)
from am_style.transport import (COORDINATOR_NATIVE, runtime_binding,
                                student_contract_version, student_prompt)

from .common import (R5Error, canonical, check_seal, file_hash, loads, object_hash,
                     read_json, read_jsonl, require_sha, seal, text_hash,
                     write_json)


SOURCE_REF = "0641a282584a68f60ddaf198326fc431ba67ee13"
GROUP_SCHEMA = "am-style-r5-groups/1"
IMPORT_SCHEMA = "am-style-r5-feedback-import/1"
SNAPSHOT_SCHEMA = "am-style-r5-gold-snapshot/1"
BASELINE_HASHES = {
    "train": "4392819139bddfa3cae18ec8f0f0a3ac82f4d27a8194050ed3e80e82137b0398",
    "dev": "151a8f426722d0298dcf785c1fac45fe287d3717be4d7ca0cdd21d27226a3009",
}
BASELINE_COUNTS = {"train": 9, "dev": 1}
LEGACY_FROZEN_SHA256 = "ad0b1bfb7501d11c34d5350e07cf9633663affceda220c099054e0ed498b4916"
SPLITS = {"train", "dev", "sealed_test"}
PACKAGE_ROOT = Path(__file__).resolve().parents[2]


def _text(value, label):
    if not isinstance(value, str) or not value.strip():
        raise R5Error("missing nonempty " + label)
    return value


def _timestamp(value):
    try:
        stamp = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (AttributeError, TypeError, ValueError) as exc:
        raise R5Error("review timestamp with timezone is required") from exc
    if stamp.tzinfo is None:
        raise R5Error("review timestamp with timezone is required")


def _row_for_integrity(row):
    provenance = row["provenance"]
    return {"id": row["id"], "group_id": provenance["group_id"],
            "parent_group_id": provenance["group_id"], "split": provenance["split"],
            "prompt": row["prompt"]}


def _validate_gold_rows(rows, split):
    for row in rows:
        _text(row.get("id"), "gold row ID")
        validate_messages(row.get("prompt"))
        validate_messages(row.get("completion"), completion=True)
        completion = loads(row["completion"][0]["content"])
        if not isinstance(completion, dict) or set(completion) != {"text"}:
            raise R5Error("gold completion must preserve the runtime JSON text response")
        _text(completion["text"], "gold completion text")
        p = row.get("provenance", {})
        if (p.get("split") != split or p.get("schema") != "am-style-data/1"
                or p.get("runtime_alignment") != "aligned"
                or not all(p.get(k) is True for k in
                           ("human_approved", "facts_pass", "style_pass", "acceptable"))):
            raise R5Error("gold row lacks explicit human acceptance/aligned split")
        for key in ("group_id", "reviewer", "style_policy_version", "host_contract_version"):
            _text(p.get(key), key)
        _timestamp(p.get("reviewed_at"))
        require_sha(p.get("source_sha256"), "source SHA")
        if (p.get("prompt_sha256") != object_hash(row["prompt"])
                or p.get("candidate_sha256") != object_hash(row["completion"])
                or p.get("surface_text_sha256") != text_hash(completion["text"])):
            raise R5Error("gold prompt/completion bytes changed")


def _baselines(train_path, dev_path):
    payloads, rows, metadata = {}, {}, {}
    for split, path in (("train", train_path), ("dev", dev_path)):
        raw = Path(path).read_bytes()
        if file_hash(path) != BASELINE_HASHES[split]:
            raise R5Error("baseline is not the fixed R4 combined " + split + " file")
        if not raw.endswith(b"\n"):
            raise R5Error("fixed baseline is missing its original final newline")
        parsed = read_jsonl(path)
        if len(parsed) != BASELINE_COUNTS[split]:
            raise R5Error("fixed baseline row count changed")
        _validate_gold_rows(parsed, split)
        payloads[split], rows[split] = raw, parsed
        metadata[split] = {"sha256": BASELINE_HASHES[split], "rows": len(parsed),
                           "byte_length": len(raw),
                           "ids": [r["id"] for r in parsed],
                           "groups": sorted({r["provenance"]["group_id"] for r in parsed})}
    assert_split_integrity([_row_for_integrity(r) for split in ("train", "dev") for r in rows[split]])
    return payloads, rows, metadata


def _legacy_scenarios():
    """All old R3 scenario groups, including exposed dev and held-out groups."""
    path = PACKAGE_ROOT / "baseline/data/r3-frozen-comparison.json"
    if file_hash(path) != LEGACY_FROZEN_SHA256:
        raise R5Error("the pinned R3 scenario source changed")
    frozen = read_json(path)
    groups, rows = {}, []
    policy = StylePolicy(**frozen["policy"])
    for case in frozen["cases"]:
        match = re.fullmatch(r"R3-SEED-(.+)-([12])", case["case_id"])
        if not match:
            raise R5Error("unknown pinned R3 scenario identity")
        group = match.group(1)
        split = split_for_group(group)
        if case.get("split", split) != split:
            raise R5Error("pinned R3 scenario split changed")
        groups[group] = split
        frame = SpeechFrame.freeze(case["frame"])
        target = runtime_binding(frame, policy, profile=COORDINATOR_NATIVE)
        rows.append({"id": "legacy:" + case["case_id"], "group_id": group,
                     "parent_group_id": group, "split": split,
                     "prompt": student_prompt(target)})
    return groups, rows, file_hash(path)


def _scenario(spec):
    if not isinstance(spec, dict) or spec.get("synthetic") is not True:
        raise R5Error("R5 authored scenario specs must explicitly be synthetic")
    if (any(key in spec for key in ("reviews", "selection"))
            or any(spec.get(k) is not None and spec.get(k) is not False
                   for k in ("training_approved", "human_approved"))):
        raise R5Error("scenario specifications cannot embed approval labels")
    ident, group, split = (_text(spec.get("id"), "scenario ID"),
                           _text(spec.get("group_id"), "scenario group"), spec.get("split"))
    if split not in SPLITS or spec.get("parent_group_id", group) != group:
        raise R5Error("scenario split/parent group is invalid")
    frame = SpeechFrame.freeze(spec["frame"])
    policy = StylePolicy(**spec["style_policy"])
    target = runtime_binding(frame, policy, profile=COORDINATOR_NATIVE)
    prompt = student_prompt(target)
    return {"id": ident, "group_id": group, "parent_group_id": group, "split": split,
            "frame": frame.data, "style_policy": policy.as_dict(), "target_binding": target,
            "prompt": prompt, "fact_entities": list(spec.get("fact_entities", []))}


def freeze_group_registry(specs, *, baseline_train, baseline_dev, legacy_groups=None):
    """Freeze group ownership before review/model collection; no random split moves.

    New groups use the explicit design-time split. Old groups retain the fixed
    R3 assignment, and all ten existing gold rows retain their exact old files.
    """
    _, baseline_rows, baseline = _baselines(baseline_train, baseline_dev)
    groups, legacy_rows, legacy_sha = _legacy_scenarios()
    if legacy_groups is not None:
        for group, split in legacy_groups.items():
            if group not in groups or groups[group] != split:
                raise R5Error("supplied legacy groups differ from the pinned R3 groups")
    legacy = dict(groups)
    scenarios, integrity = {}, []
    previous_signatures = {normalized_prompt(row) for row in legacy_rows}
    for spec in specs:
        record = _scenario(spec)
        ident, group, split = record["id"], record["group_id"], record["split"]
        if ident in scenarios:
            raise R5Error("duplicate scenario ID")
        if group in groups and groups[group] != split:
            raise R5Error("scenario group cannot move between splits: " + group)
        groups[group] = split
        signature = normalized_prompt(record)
        if signature in previous_signatures:
            raise R5Error("copied prompt cannot increase the sample count: " + ident)
        previous_signatures.add(signature)
        scenarios[ident] = {"group_id": group, "split": split,
                           "frame_sha256": object_hash(record["frame"]),
                           "policy_sha256": object_hash(record["style_policy"]),
                           "prompt_sha256": object_hash(record["prompt"])}
        integrity.append(record)
    if not scenarios:
        raise R5Error("cannot freeze an empty new scenario registry")
    assert_split_integrity(legacy_rows + integrity)
    for split in ("train", "dev"):
        for row in baseline_rows[split]:
            if groups.get(row["provenance"]["group_id"]) != split:
                raise R5Error("old gold group is not preserved")
    return seal({"schema": GROUP_SCHEMA, "source_ref": SOURCE_REF,
                 "baseline": baseline, "legacy_groups": legacy,
                 "legacy_frozen_file_sha256": legacy_sha, "groups": groups,
                 "scenarios": scenarios,
                 "new_group_policy": "explicit_design_time_assignment_frozen_before_review",
                 "sealed_test_policy": "excluded_from_review_and_training_exports"})


def _validate_registry(registry):
    check_seal(registry)
    if registry.get("schema") != GROUP_SCHEMA or registry.get("source_ref") != SOURCE_REF:
        raise R5Error("unsupported scenario registry")
    for split in ("train", "dev"):
        base = registry.get("baseline", {}).get(split, {})
        if base.get("sha256") != BASELINE_HASHES[split] or base.get("rows") != BASELINE_COUNTS[split]:
            raise R5Error("registry replaced the fixed 9/1 baseline")
    legacy_groups, _, _ = _legacy_scenarios()
    if (registry.get("legacy_groups") != legacy_groups
            or registry.get("legacy_frozen_file_sha256") != LEGACY_FROZEN_SHA256):
        raise R5Error("registry changed the fixed historical scenario split")
    for group, split in registry["legacy_groups"].items():
        if registry["groups"].get(group) != split:
            raise R5Error("registry moved a legacy group")
    if any(split not in SPLITS for split in registry["groups"].values()):
        raise R5Error("registry contains an unknown split")
    for scenario in registry["scenarios"].values():
        if registry["groups"].get(scenario["group_id"]) != scenario["split"]:
            raise R5Error("registry scenario/group assignment is inconsistent")
        for key in ("frame_sha256", "policy_sha256", "prompt_sha256"):
            require_sha(scenario.get(key), key)
    return registry


def _record_matches_registry(record, registry):
    expected = registry["scenarios"].get(record["id"])
    actual = {"group_id": record["group_id"], "split": record["split"],
              "frame_sha256": object_hash(record["frame"]),
              "policy_sha256": object_hash(record["style_policy"]),
              "prompt_sha256": object_hash(record["prompt"])}
    if expected != actual or registry["groups"].get(record["group_id"]) != record["split"]:
        raise R5Error("scenario/frame/prompt differs from the frozen registry")


def _model_candidates(refs, record):
    candidates = {}
    for index, ref in enumerate(refs):
        path = Path(ref["path"])
        if file_hash(path) != require_sha(ref.get("sha256"), "model record SHA"):
            raise R5Error("model observation file changed")
        observation = read_json(path)
        if (observation.get("schema") != "am-style-r5-model-observation/1"
                or observation.get("status") != "completed"
                or observation.get("actual_model_call") is not True):
            raise R5Error("model observation is not an explicit completed invocation record")
        for key in ("provider", "model", "run_id"):
            _text(observation.get(key), "model " + key)
        for key, value in (("frame_sha256", object_hash(record["frame"])),
                           ("policy_sha256", object_hash(record["style_policy"])),
                           ("prompt_sha256", object_hash(record["prompt"]))):
            if observation.get(key) != value:
                raise R5Error("model observation belongs to another frozen request")
        response = _text(observation.get("raw_response"), "raw model response")
        decoded = loads(response)
        if (not isinstance(decoded, dict) or set(decoded) != {"text"}
                or decoded["text"] != observation.get("candidate_text")
                or observation.get("raw_response_sha256") != text_hash(response)):
            raise R5Error("model response/content/hash disagree")
        text = _text(decoded["text"], "model candidate text")
        candidates["model_" + str(index + 1)] = {
            "available": True, "text": text, "sha256": text_hash(text), "origin": "model_call",
            "provider": observation["provider"], "model": observation["model"],
            "run_id": observation["run_id"], "raw_response": response,
            "raw_response_sha256": text_hash(response), "source_record_sha256": ref["sha256"],
            "human_approved": False,
            "origin_limit": "executor_observation_claim; hashes_do_not_authenticate_remote_invocation"}
    return candidates


def build_review_batch(specs, registry, *, version, source_files=None):
    """Make a generic R4-compatible pending batch; sealed-test specs stay out."""
    _validate_registry(registry)
    cases, excluded = [], []
    seen = set()
    for spec in specs:
        record = _scenario(spec)
        if record["id"] in seen:
            raise R5Error("duplicate scenario in review batch")
        seen.add(record["id"])
        _record_matches_registry(record, registry)
        if record["split"] == "sealed_test":
            excluded.append(record["id"])
            continue
        proposal = _text(spec.get("proposal"), "assistant proposal")
        facts = _text(spec.get("facts_for_review"), "review facts")
        record.update(source_case_id=record["id"], synthetic=True,
                      style_policy_version=record["style_policy"]["version"],
                      prompt_sha256=object_hash(record["prompt"]),
                      host_contract_version=student_contract_version(record["target_binding"]),
                      runtime_prompt_alignment="aligned", facts_for_review=facts,
                      reviews={}, selection={}, status="pending", training_approved=False)
        record["candidates"] = {"proposed_revision": {
            "available": True, "text": proposal, "sha256": text_hash(proposal),
            "origin": "assistant_authored_proposal", "human_approved": False, "model_run": False}}
        record["candidates"].update(_model_candidates(spec.get("model_records", []), record))
        record["source_sha256"] = object_hash({"scenario": registry["scenarios"][record["id"]],
                                                "facts_for_review": facts, "candidates": record["candidates"]})
        record["review_context_sha256"] = object_hash(r4_context(record))
        cases.append(record)
    if not cases:
        raise R5Error("review batch has no train/dev scenarios")
    assert_split_integrity(cases)
    batch = {"schema": BATCH_SCHEMA, "version": _text(version, "batch version"),
             "source_ref": SOURCE_REF, "source_files": deepcopy(source_files or {}),
             "group_registry": deepcopy(registry), "group_registry_sha256": registry["sha256"],
             "review_type": "development_calibration_not_independent_test",
             "counts": {"pending_cases": len(cases), "new_human_approvals": 0, "new_trainable_rows": 0},
             "already_reviewed": [], "host_repair": [], "unselected_case_ids": sorted(excluded),
             "cases": cases}
    batch["batch_sha256"] = object_hash(batch)
    validate_review_batch(batch)
    return batch


def validate_review_batch(batch):
    r4_validate_review_batch(batch)
    registry = _validate_registry(batch["group_registry"])
    if batch.get("group_registry_sha256") != registry["sha256"]:
        raise R5Error("review batch has another group registry")
    if batch.get("counts") != {"pending_cases": len(batch["cases"]),
                               "new_human_approvals": 0, "new_trainable_rows": 0}:
        raise R5Error("pending batch cannot claim human approvals or trainable rows")
    for record in batch["cases"]:
        _record_matches_registry(record, registry)
        if record["split"] not in {"train", "dev"}:
            raise R5Error("sealed-test cases cannot enter the calibration review")
        for candidate in record["candidates"].values():
            if candidate.get("human_approved") is not False:
                raise R5Error("pending candidates cannot carry human approval")
            if candidate.get("origin") not in {"assistant_authored_proposal", "model_call"}:
                raise R5Error("candidate origin is not explicit")
    assert_split_integrity(batch["cases"])
    return batch


def feedback_template(batch):
    validate_review_batch(batch)
    return r4_feedback_template(batch)


def revise_review_batch(batch, replacements, *, version):
    """Candidate edits create a new immutable batch; old feedback never transfers."""
    validate_review_batch(batch)
    if not replacements or version == batch["version"]:
        raise R5Error("revision requires a changed proposal and a new batch version")
    revised = deepcopy(batch)
    by_id = {record["id"]: record for record in revised["cases"]}
    if set(replacements) - set(by_id):
        raise R5Error("revision names a case outside this batch")
    changed = False
    for ident, text in replacements.items():
        text = _text(text, "revised complete proposal")
        record = by_id[ident]
        old = record["candidates"]["proposed_revision"]
        changed = changed or text != old["text"]
        record["candidates"]["proposed_revision"] = {
            "available": True, "text": text, "sha256": text_hash(text),
            "origin": "assistant_authored_proposal", "human_approved": False,
            "model_run": False, "supersedes_candidate_sha256": old["sha256"]}
        record["source_sha256"] = object_hash({"previous_source_sha256": record["source_sha256"],
                                                "candidate": record["candidates"]["proposed_revision"]})
        record["review_context_sha256"] = object_hash(r4_context(record))
    if not changed:
        raise R5Error("revision contains no text change")
    revised.update(version=_text(version, "new batch version"), parent_batch_sha256=batch["batch_sha256"],
                   previous_feedback_status="invalid_for_new_batch")
    revised.pop("batch_sha256")
    revised["batch_sha256"] = object_hash(revised)
    validate_review_batch(revised)
    return revised


def import_feedback_file(batch, feedback_path, *, raw_feedback_path=None):
    """Import an explicit human artifact through the unchanged R4 label rules.

    For a native JSON form, feedback_path is itself the original artifact. When
    an executor transcribes chat, pass the retained original to raw_feedback_path.
    Neither a human-origin field nor a file hash authenticates the named person.
    """
    validate_review_batch(batch)
    feedback_path = Path(feedback_path).resolve()
    raw_path = Path(raw_feedback_path).resolve() if raw_feedback_path else feedback_path
    feedback = read_json(feedback_path)
    imported = r4_import_feedback(batch, feedback)
    return seal({"schema": IMPORT_SCHEMA, "batch": deepcopy(batch), "feedback": feedback,
                 "group_registry_sha256": batch["group_registry_sha256"],
                 "feedback_file": {"path": str(feedback_path), "sha256": file_hash(feedback_path)},
                 "raw_feedback_file": {"path": str(raw_path), "sha256": file_hash(raw_path)},
                 "r4_import": imported,
                 "identity_limit": "explicit_human_provenance_is_required; content_hashes_are_not_authentication"})


def _validate_import(imported, *, feedback_path=None, raw_feedback_path=None):
    check_seal(imported)
    if imported.get("schema") != IMPORT_SCHEMA:
        raise R5Error("unsupported feedback import")
    validate_review_batch(imported["batch"])
    for key, override in (("feedback_file", feedback_path), ("raw_feedback_file", raw_feedback_path)):
        path = Path(override or imported[key]["path"])
        if file_hash(path) != imported[key]["sha256"]:
            raise R5Error("original human feedback file changed")
    feedback = read_json(feedback_path or imported["feedback_file"]["path"])
    if feedback != imported["feedback"]:
        raise R5Error("feedback file no longer matches the imported object")
    expected = r4_import_feedback(imported["batch"], feedback)
    if expected != imported["r4_import"]:
        raise R5Error("feedback export cannot be edited independently of human review")
    if imported.get("group_registry_sha256") != imported["batch"]["group_registry_sha256"]:
        raise R5Error("feedback import changed registry")
    return expected


def _gold_additions(imports, registry, *, join_reviewed_registries=False):
    additions, seen_imports = {"train": [], "dev": []}, set()
    for imported in imports:
        if imported["sha256"] in seen_imports:
            raise R5Error("duplicate feedback import")
        seen_imports.add(imported["sha256"])
        different_registry = imported["group_registry_sha256"] != registry["sha256"]
        if different_registry and not join_reviewed_registries:
            raise R5Error("cannot mix different frozen registries")
        # Keep the original import/batch/feedback intact. Joining is allowed only
        # for each actually accepted record whose complete frame, prompt, policy,
        # case/group identity and split remain identical in the destination.
        # This never rebinds an old approval to a revised answer or context.
        records = {r["id"]: r for r in imported["r4_import"]["records"]}
        for split in ("train", "dev"):
            for original in imported["r4_import"]["exports"]["outputs"]["sft." + split]:
                if join_reviewed_registries:
                    _record_matches_registry(records[original["id"]], registry)
                row = deepcopy(original)
                row["provenance"].update(r5_import_sha256=imported["sha256"],
                    raw_feedback_file_sha256=imported["raw_feedback_file"]["sha256"],
                    group_registry_sha256=registry["sha256"])
                if different_registry:
                    row["provenance"]["review_group_registry_sha256"] = imported["group_registry_sha256"]
                additions[split].append(row)
    return additions


def _integrity_and_counts(rows, registry):
    integrity = [_row_for_integrity(r) for split in ("train", "dev") for r in rows[split]]
    assert_split_integrity(integrity)
    signatures = set()
    for record in integrity:
        if registry["groups"].get(record["group_id"]) != record["split"]:
            raise R5Error("gold row has moved scenario split")
        signature = normalized_prompt(record)
        if signature in signatures:
            raise R5Error("duplicate logical prompt cannot increase the gold count")
        signatures.add(signature)
    return {"train": len(rows["train"]), "dev": len(rows["dev"]),
            "train_groups": len({r["provenance"]["group_id"] for r in rows["train"]}),
            "dev_groups": len({r["provenance"]["group_id"] for r in rows["dev"]})}


def _write_bytes(path, payload):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("xb") as output:
        output.write(payload)
    return file_hash(path)


def export_gold_snapshot(registry, imports, *, baseline_train, baseline_dev, output_dir,
                         join_reviewed_registries=False):
    """Write one new snapshot only after validating files, labels and leakage."""
    _validate_registry(registry)
    payloads, rows, baseline = _baselines(baseline_train, baseline_dev)
    if baseline != registry["baseline"]:
        raise R5Error("registry baseline metadata changed")
    imports = list(imports)
    for imported in imports:
        _validate_import(imported)
    if type(join_reviewed_registries) is not bool:
        raise R5Error("registry join must be an explicit boolean")
    additions = _gold_additions(imports, registry, join_reviewed_registries=join_reviewed_registries)
    for split in ("train", "dev"):
        rows[split] += additions[split]
        _validate_gold_rows(rows[split], split)
    counts = _integrity_and_counts(rows, registry)
    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=False)
    registry_hash = write_json(output / "group-registry.json", registry)
    files, references = {}, []
    for split in ("train", "dev"):
        name = "sft." + split + ".jsonl"
        data = payloads[split] + "".join(canonical(r) + "\n" for r in additions[split]).encode("utf-8")
        sha = _write_bytes(output / name, data)
        files[split] = {"path": name, "sha256": sha, "rows": counts[split],
                        "groups": counts[split + "_groups"]}
    for index, imported in enumerate(imports, 1):
        name = f"imports/{index:03}.json"
        item = {"path": name, "sha256": write_json(output / name, imported),
                "import_sha256": imported["sha256"]}
        for key in ("feedback_file", "raw_feedback_file"):
            rel = f"feedback/{index:03}-{key}.json" if key == "feedback_file" else f"feedback/{index:03}-original.txt"
            item[key] = {"path": rel, "sha256": _write_bytes(output / rel, Path(imported[key]["path"]).read_bytes())}
        references.append(item)
    snapshot_body = {"schema": SNAPSHOT_SCHEMA, "source_ref": SOURCE_REF,
        "files": files, "counts": counts, "baseline": baseline, "baseline_preserved": True,
        "group_registry_sha256": registry["sha256"],
        "registry": {"path": "group-registry.json", "sha256": registry_hash}, "imports": references,
        "sealed_test": {"exported_rows": 0,
                        "groups": sum(s == "sealed_test" for s in registry["groups"].values())},
        "sft_only": True, "training_executed": False, "naturalness_improvement_established": False}
    if join_reviewed_registries:
        snapshot_body["review_registry_join"] = "exact_accepted_context/1"
    snapshot = seal(snapshot_body)
    write_json(output / "snapshot.json", snapshot)
    return verify_gold_snapshot(output / "snapshot.json")


def _contained_file(root, reference):
    rel = Path(reference["path"])
    if rel.is_absolute() or ".." in rel.parts:
        raise R5Error("snapshot reference must be a local relative file")
    path = root / rel
    if path.is_symlink() or not path.resolve().is_relative_to(root.resolve()):
        raise R5Error("snapshot reference escapes its directory")
    if file_hash(path) != reference["sha256"]:
        raise R5Error("snapshot file bytes changed: " + str(rel))
    return path


def verify_gold_snapshot(snapshot_path):
    """Read-back API for pilot.py; rederive all new gold through R4 human gates."""
    snapshot_path = Path(snapshot_path)
    root = snapshot_path.parent
    snapshot = check_seal(read_json(snapshot_path))
    if (snapshot.get("schema") != SNAPSHOT_SCHEMA or snapshot.get("source_ref") != SOURCE_REF
            or snapshot.get("baseline_preserved") is not True or snapshot.get("sft_only") is not True):
        raise R5Error("unsupported gold snapshot")
    registry = _validate_registry(read_json(_contained_file(root, snapshot["registry"])))
    if snapshot.get("group_registry_sha256") != registry["sha256"] or snapshot.get("baseline") != registry["baseline"]:
        raise R5Error("snapshot registry/baseline binding changed")
    imports = []
    for reference in snapshot["imports"]:
        imported = read_json(_contained_file(root, reference))
        if imported.get("sha256") != reference["import_sha256"]:
            raise R5Error("snapshot feedback import identity changed")
        _validate_import(imported, feedback_path=_contained_file(root, reference["feedback_file"]),
                         raw_feedback_path=_contained_file(root, reference["raw_feedback_file"]))
        imports.append(imported)
    join_mode = snapshot.get("review_registry_join")
    if join_mode not in (None, "exact_accepted_context/1"):
        raise R5Error("unsupported reviewed-registry join")
    additions = _gold_additions(imports, registry,
                               join_reviewed_registries=join_mode == "exact_accepted_context/1")
    rows = {}
    for split in ("train", "dev"):
        path = _contained_file(root, snapshot["files"][split])
        raw = path.read_bytes()
        base = snapshot["baseline"][split]
        baseline_bytes = raw[:base["byte_length"]]
        import hashlib
        if (base["sha256"] != BASELINE_HASHES[split] or base["rows"] != BASELINE_COUNTS[split]
                or hashlib.sha256(baseline_bytes).hexdigest() != BASELINE_HASHES[split]):
            raise R5Error("snapshot no longer preserves the original 9/1 byte prefixes")
        suffix = "".join(canonical(row) + "\n" for row in additions[split]).encode("utf-8")
        if raw != baseline_bytes + suffix:
            raise R5Error("snapshot contains labels not derived from the retained human feedback")
        rows[split] = read_jsonl(path)
        _validate_gold_rows(rows[split], split)
        old_rows = rows[split][:BASELINE_COUNTS[split]]
        if (base["ids"] != [row["id"] for row in old_rows]
                or base["groups"] != sorted({row["provenance"]["group_id"] for row in old_rows})):
            raise R5Error("snapshot old-gold identity metadata changed")
    counts = _integrity_and_counts(rows, registry)
    if snapshot["counts"] != counts:
        raise R5Error("snapshot row/group counts differ from its files")
    for split in ("train", "dev"):
        if (snapshot["files"][split]["rows"] != counts[split]
                or snapshot["files"][split]["groups"] != counts[split + "_groups"]):
            raise R5Error("snapshot file count metadata changed")
    if snapshot.get("sealed_test") != {"exported_rows": 0,
                                      "groups": sum(s == "sealed_test" for s in registry["groups"].values())}:
        raise R5Error("sealed-test export is prohibited")
    return snapshot


def markdown_review(batch):
    """Readable generic review with full facts and candidate text, without labels."""
    validate_review_batch(batch)
    lines = ["# R5 表达校准待评", "", "全部场景为虚构。以下答案尚未获得人工认可，不会自动进入训练。",
             "请对完整答复分别判断：事实是否准确、语气是否合适、能否直接采用。修改后须对新版本完整文本重新确认。", ""]
    for index, record in enumerate(batch["cases"], 1):
        lines += [f"## {index}. {record['id']}", "", f"**问题：**{record['frame']['question']}", "",
                  f"**已知事实：**{record['facts_for_review']}", ""]
        for key, candidate in record["candidates"].items():
            label = "助手待审草案" if candidate["origin"] == "assistant_authored_proposal" else "模型观测答复（来源记录可核对）"
            lines += [f"**{key} · {label}：**{candidate['text']}", ""]
        lines += ["**待填写：**事实、语气、可直接采用／需要修改／无法判断。", ""]
    return "\n".join(lines)
