"""Prepare a gold pilot and recheck it without loading a model.

The only executable returned by this module is the unchanged R4 trainer.  A
metadata preflight is never labelled as training, resource-lease acquisition,
kernel execution, a successful reload, or a quality improvement.
"""
from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
import os
from pathlib import Path
import subprocess
import sys

from .common import (R5Error, check_seal, file_hash, object_hash, read_json,
                     require_sha, seal, write_json)


PACKAGE_ROOT = Path(__file__).resolve().parents[2]
BASELINE_ROOT = PACKAGE_ROOT / "baseline"
PLAN_SCHEMA = "am-style-r5-pilot-plan/1"
# Read from R4 EVIDENCE_SHA256.json: original_sha256 AND audit_sha256 for
# evidence/mlx/mlx-mechanics-002/result.json are this same value.  The request
# file is NOT interchangeable: its public copy was redacted.
AUDITED_MECHANICS_RESULT_SHA = "bf4ac5775e61e9626fb92460a49b197045a4b2d310090e70a39257e9dda6d7b6"
AUDITED_R4 = "0641a282584a68f60ddaf198326fc431ba67ee13"
RUNTIME_FILES = (
    "training/__init__.py", "training/common.py", "training/mlx_r4.py",
    "training/mlx_r4_contracts.py", "training/mlx_r4_runtime.py",
    "training/mlx_r4_resources.py", "training/mlx_r4_kernels/__init__.py",
    "training/mlx_r4_kernels/chunk_gated_delta.py",
    "am_style/__init__.py", "am_style/contracts.py", "am_style/prompt.py",
)
MECHANICS_FILES = (
    "request.json", "fit.json", "reload.json", "kernel-validation.json",
    "steps.json", "adapters.safetensors", "adapter_config.json",
)


def _absolute(path, label):
    p = Path(path).expanduser()
    if not p.is_absolute():
        raise R5Error(label + " must be an absolute path")
    return p


def _file_record(path):
    p = _absolute(path, "input file").resolve()
    if not p.is_file():
        raise R5Error("missing input file: " + str(p))
    return {"path": str(p), "sha256": file_hash(p)}


def _verify_record(record, label):
    if not isinstance(record, dict):
        raise R5Error("missing " + label)
    p = _absolute(record.get("path", ""), label).resolve()
    expected = require_sha(record.get("sha256"), label)
    if not p.is_file() or file_hash(p) != expected:
        raise R5Error("changed or missing " + label)
    return p


def _snapshot(path):
    # The data module checks the complete approval chain, the immutable R4
    # 9/1 byte prefixes, registry ownership, and every exported file hash.
    from .dataset import verify_gold_snapshot
    path = _absolute(path, "snapshot").resolve()
    snapshot = verify_gold_snapshot(path)
    counts = snapshot["counts"]
    if not (24 <= counts["train"] <= 48 and 8 <= counts["dev"] <= 16
            and counts["train_groups"] >= 8 and counts["dev_groups"] >= 4):
        raise R5Error("Pilot requires 24–48 train / 8–16 dev gold rows across >=8/4 disjoint groups")
    return path, snapshot


def _original_mechanics(result_path, manifest_path):
    """Reject a demo success flag and the redacted public request before import."""
    result_path = _absolute(result_path, "mechanics result").resolve()
    if not result_path.is_file() or file_hash(result_path) != AUDITED_MECHANICS_RESULT_SHA:
        raise R5Error("Use the audited R4 mechanics-002 ORIGINAL result; a demo or new result needs a new reviewed binding")
    result = read_json(result_path)
    hashes = result.get("files_sha256", {})
    for name in MECHANICS_FILES:
        p = (result_path.parent / name).resolve()
        if not p.is_relative_to(result_path.parent) or not p.is_file():
            raise R5Error("missing original mechanics artifact: " + name)
        if file_hash(p) != require_sha(hashes.get(name), name):
            raise R5Error("original mechanics artifact changed or redacted: " + name)
    request = read_json(result_path.parent / "request.json")
    if object_hash(request) != result.get("request_sha256"):
        raise R5Error("mechanics request object does not match the completed result")
    manifest_path = _absolute(manifest_path, "manifest").resolve()
    manifest = read_json(manifest_path)
    if manifest != request.get("manifest"):
        raise R5Error("manifest must equal the actual mechanics manifest; do not substitute public paths or a different base")
    if request.get("config", {}).get("stage") != "mechanics":
        raise R5Error("pilot must bind the completed mechanics stage")
    return result_path, manifest_path, request, manifest


def _source_binding(roots=None):
    if roots is None:
        roots = {"training": str(BASELINE_ROOT / "am-style-r3"),
                 "am_style": str(BASELINE_ROOT / "am-conversation-r1" / "src")}
        mode = "bundled_r4_optional_isolated_runtime"
    else:
        if not isinstance(roots, dict) or set(roots) != {"training", "am_style"}:
            raise R5Error("runtime_source_roots requires training and am_style parent directories")
        mode = "explicit_host_runtime"
    roots = {key: str(_absolute(value, key + " source root").resolve()) for key, value in roots.items()}
    provenance = read_json(BASELINE_ROOT / "PROVENANCE.json")
    if provenance.get("audit_r4") != AUDITED_R4:
        raise R5Error("baseline provenance belongs to another audit")
    expected = {}
    for item in provenance["files"]:
        local = item["local"]
        if local.startswith("am-style-r3/"):
            expected[local[len("am-style-r3/"):]] = item["sha256"]
        elif local.startswith("am-conversation-r1/src/"):
            expected[local[len("am-conversation-r1/src/"):]] = item["sha256"]
    checked = {}
    for name in RUNTIME_FILES:
        root = Path(roots[name.split("/", 1)[0]])
        p = (root / name).resolve()
        if not p.is_relative_to(root) or not p.is_file() or file_hash(p) != expected.get(name):
            raise R5Error("runtime source differs from audited R4: " + name
                          + "; preserve newer host files and review the difference, never overwrite or change the expected hash")
        checked[name] = expected[name]
    return {"mode": mode, "roots": roots, "files_sha256": checked,
            "baseline_provenance_sha256": file_hash(BASELINE_ROOT / "PROVENANCE.json")}


def _config(snapshot_path, snapshot, request, mechanics_path):
    config = deepcopy(request["config"])
    config.update(stage="pilot", gradient_accumulation=4,
                  mechanics_result={"path": str(mechanics_path), "sha256": AUDITED_MECHANICS_RESULT_SHA})
    for split in ("train", "dev"):
        entry = snapshot["files"][split]
        p = (snapshot_path.parent / entry["path"]).resolve()
        if not p.is_relative_to(snapshot_path.parent) or not p.is_file():
            raise R5Error("dataset is outside its frozen snapshot")
        config[split + "_file"] = str(p)
        config[split + "_sha256"] = require_sha(entry["sha256"], split)
    return config


def build_pilot_config(snapshot_path, mechanics_result_path, manifest_path):
    """Build only from verified local inputs; never fabricate missing evidence."""
    snapshot_path, snapshot = _snapshot(snapshot_path)
    result_path, _, request, _ = _original_mechanics(mechanics_result_path, manifest_path)
    return _config(snapshot_path, snapshot, request, result_path)


def _interpreter_record(executable=None):
    # Preserve a virtualenv symlink in argv: resolving it can select base Python
    # instead of the operator's existing MLX environment.
    p = _absolute(executable or sys.executable, "Python interpreter")
    if not p.is_file() or not os.access(p, os.X_OK):
        raise R5Error("select an existing executable Python; no installation is performed")
    return {"path": str(p), "sha256": file_hash(p), "resolved_path": str(p.resolve())}


def _check_output(path, manifest, *, evidence_roots=(), evidence_files=()):
    path = _absolute(path, "new output directory")
    if path.exists() or path.is_symlink():
        raise R5Error("output directory already exists; use a new run identity")
    path = path.resolve()
    protected = [Path(manifest["local_model_path"]).resolve(),
                 *map(Path, evidence_roots), *map(Path, evidence_files)]
    service = manifest["service"]
    protected.extend(Path(service[key]).resolve().parent for key in ("lease_file", "gateway_db", "chat_db"))
    protected.extend(Path(name).resolve().parent for name in service["protected_config_files"])
    if any(path.is_relative_to(p.resolve()) for p in protected):
        raise R5Error("output would enter a protected model, service, or input-evidence directory")
    return path


def _environment(binding):
    roots = binding["roots"]
    return {"PYTHONPATH": os.pathsep.join((roots["training"], roots["am_style"])),
            "HF_HUB_OFFLINE": "1",
            "TRANSFORMERS_OFFLINE": "1", "TOKENIZERS_PARALLELISM": "false"}


# This child performs ONLY the original metadata contracts. It never invokes
# the trainer's --execute path, acquires its lease, or imports MLX/model code.
_PROBE = r'''
import json, sys
from pathlib import Path
from training.common import read_json
from training.mlx_r4 import preflight
from training.mlx_r4_contracts import validate_plan, validate_mechanics_gate
from training.mlx_r4_runtime import code_hashes
manifest, config = read_json(sys.argv[1]), read_json(sys.argv[2])
checks, blockers = {}, []
for name, call in (("validate_mechanics_gate", lambda: validate_mechanics_gate(config)),
                   ("validate_plan", lambda: validate_plan(config))):
    try:
        value = call()
        checks[name] = {"status":"passed"}
        if name == "validate_plan":
            checks[name].update(train=len(value[0]), dev=len(value[1]))
    except Exception as exc:
        checks[name] = {"status":"blocked"}
        blockers.append({"check":name,"reason":str(exc)})
report = preflight(manifest, config)
blockers.extend(report["blockers"])
loaded = any(name == "mlx" or name.startswith("mlx.") for name in sys.modules)
if loaded:
    blockers.append({"check":"metadata_only","reason":"MLX was unexpectedly imported"})
print(json.dumps({"schema":"am-style-r5-pilot-probe/1","checks":checks,
    "r4_preflight":report,"blockers":blockers,"metadata_ready":not blockers,
    "ready_for_launch":False,"readiness_scope":"metadata_only_resources_not_checked",
    "code_sha256":code_hashes(),"model_loaded":False,"mlx_imported":loaded,
    "training_executed":False,"interpreter":sys.executable,
    "contracts_path":str(Path(sys.modules["training.mlx_r4_contracts"].__file__).resolve()),
    "prompt_path":str(Path(sys.modules["am_style.prompt"].__file__).resolve())}, ensure_ascii=False))
'''


def _run_probe(manifest_path, config_path, binding, interpreter, expected_code):
    command = [interpreter["path"], "-c", _PROBE, str(manifest_path), str(config_path)]
    env = os.environ.copy()
    env.update(_environment(binding))
    completed = subprocess.run(command, cwd=binding["roots"]["training"], env=env,
                               stdin=subprocess.DEVNULL, capture_output=True,
                               text=True, timeout=45, check=False)
    if completed.returncode:
        raise R5Error("metadata probe failed (exit " + str(completed.returncode)
                      + "): " + completed.stderr[-1500:])
    from .common import loads
    report = loads(completed.stdout)
    roots = binding["roots"]
    if (report.get("schema") != "am-style-r5-pilot-probe/1"
            or report.get("code_sha256") != expected_code
            or report.get("contracts_path") != str((Path(roots["training"]) / "training/mlx_r4_contracts.py").resolve())
            or report.get("prompt_path") != str((Path(roots["am_style"]) / "am_style/prompt.py").resolve())
            or report.get("mlx_imported") is not False
            or report.get("training_executed") is not False):
        raise R5Error("metadata probe did not use the exact selected, original R4 contracts")
    return report


def prepare_pilot(snapshot_path, mechanics_result_path, manifest_path, output_dir,
                  *, python_executable=None, run_dir=None, runtime_source_roots=None,
                  holdout_cases_path=None, holdout_declaration_path=None,
                  holdout_original_path=None):
    """Write a sealed plan; resource/model/training work remains unexecuted.

Use explicit host source roots first on the real machine. Defaults select the
bundled R4 snapshot for offline reproduction or an explicitly chosen isolated
run, never silently replacing newer host code.  The optional holdout paths must
be supplied together; omitting all three permits metadata preparation only.
"""
    holdout_paths = (holdout_cases_path, holdout_declaration_path, holdout_original_path)
    if any(value is not None for value in holdout_paths) and not all(value is not None for value in holdout_paths):
        raise R5Error("supply holdout cases, declaration and declaration original together")
    snapshot_path, snapshot = _snapshot(snapshot_path)
    result_path, manifest_path, request, manifest = _original_mechanics(mechanics_result_path, manifest_path)
    binding = _source_binding(runtime_source_roots)
    interpreter = _interpreter_record(python_executable)
    evidence_roots = (snapshot_path.parent, result_path.parent)
    # Protect each additional evidence file without reserving its whole parent:
    # existing layouts keep holdout files and sibling plan directories together.
    evidence_files = tuple(_absolute(path, "holdout evidence").resolve()
                           for path in holdout_paths if path is not None)
    output = _check_output(output_dir, manifest, evidence_roots=evidence_roots, evidence_files=evidence_files)
    run = _check_output(run_dir or output / "run", manifest, evidence_roots=evidence_roots, evidence_files=evidence_files)
    if run == output or output.is_relative_to(run):
        raise R5Error("run output must not contain the plan directory")
    config = _config(snapshot_path, snapshot, request, result_path)
    output.mkdir(parents=True, exist_ok=False)
    config_path = output / "pilot.config.json"
    write_json(config_path, config)
    plan = {"schema": PLAN_SCHEMA, "snapshot": _file_record(snapshot_path),
        "snapshot_content_sha256": snapshot["sha256"], "manifest": _file_record(manifest_path),
        "mechanics_result": _file_record(result_path), "config": _file_record(config_path),
        "runtime_source_binding": binding, "python_interpreter": interpreter,
        "run_directory": str(run), "training_executed": False,
        "production_changed": False, "quality_conclusion": "not_established"}
    from .holdout_gate import bind_holdout
    plan["holdout_gate"] = bind_holdout(plan, cases_path=holdout_cases_path,
        declaration_path=holdout_declaration_path, original_path=holdout_original_path)
    plan = seal(plan)
    write_json(output / "plan.json", plan)
    report = preflight_pilot(output / "plan.json")
    write_json(output / "preflight.json", report)
    return {"plan": plan, "plan_path": str(output / "plan.json"), "preflight": report}


def _checked_plan(plan_path):
    plan = check_seal(read_json(_absolute(plan_path, "plan")))
    if plan.get("schema") != PLAN_SCHEMA or plan.get("training_executed") is not False:
        raise R5Error("invalid or non-preparation pilot plan")
    snapshot_path = _verify_record(plan["snapshot"], "snapshot")
    snapshot_path, snapshot = _snapshot(snapshot_path)
    if snapshot["sha256"] != plan["snapshot_content_sha256"]:
        raise R5Error("gold snapshot content seal changed")
    manifest_path = _verify_record(plan["manifest"], "manifest")
    result_path = _verify_record(plan["mechanics_result"], "mechanics result")
    result_path, _, request, manifest = _original_mechanics(result_path, manifest_path)
    config_path = _verify_record(plan["config"], "pilot config")
    if read_json(config_path) != _config(snapshot_path, snapshot, request, result_path):
        raise R5Error("pilot config differs from its frozen gold/mechanics inputs")
    old_binding = plan["runtime_source_binding"]
    binding = _source_binding(old_binding["roots"])
    # Mode is descriptive; the exact roots and file bytes are authoritative.
    binding["mode"] = old_binding["mode"]
    if binding != old_binding:
        raise R5Error("runtime source binding changed since preparation")
    interpreter = _interpreter_record(plan["python_interpreter"]["path"])
    if interpreter != plan["python_interpreter"]:
        raise R5Error("Python interpreter changed since preparation")
    gate = plan.get("holdout_gate")
    holdout_files = () if gate is None else tuple(
        _absolute(gate[name]["path"], "holdout evidence").resolve()
        for name in ("cases", "declaration", "declaration_original"))
    _check_output(plan["run_directory"], manifest,
                  evidence_roots=(snapshot_path.parent, result_path.parent), evidence_files=holdout_files)
    return plan, manifest_path, config_path, request


def preflight_pilot(plan_path):
    """Check metadata and holdout evidence separately; never claim resource readiness.

    An old or metadata-only plan may pass metadata checks while its execution
    gate remains blocked.  Actual resource locking and the unchanged 24 GiB
    requirement still belong to the original R4 worker.
    """
    metadata_blockers, probe, plan = [], None, None
    try:
        plan, manifest, config, request = _checked_plan(plan_path)
        probe = _run_probe(manifest, config, plan["runtime_source_binding"],
                           plan["python_interpreter"], request["code_sha256"])
        metadata_blockers.extend(probe["blockers"])
    except (R5Error, OSError, ValueError, KeyError, TypeError, AttributeError, subprocess.TimeoutExpired) as exc:
        metadata_blockers.append({"check": "immutable_inputs_or_original_probe", "reason": str(exc)})
    gate_blockers, gate = [], None
    try:
        from .holdout_gate import verify_holdout
        if plan is None:
            raise R5Error("valid immutable pilot inputs are required before checking holdout evidence")
        gate = verify_holdout(plan)
    except (R5Error, OSError, ValueError, KeyError, TypeError, AttributeError) as exc:
        gate_blockers.append({"check": "independent_holdout", "reason": str(exc)})
    return seal({"schema": "am-style-r5-pilot-preflight/1",
        "checked_at_utc": datetime.now(timezone.utc).isoformat(),
        "metadata_ready": not metadata_blockers, "metadata_blockers": metadata_blockers,
        "execution_gate": {"status": "passed" if gate is not None else "blocked",
            "scope": "independent_holdout_inputs_only", "blockers": gate_blockers,
            "binding_sha256": gate["sha256"] if gate else None,
            "case_count": gate["case_count"] if gate else None},
        "ready_for_launch": False,
        "readiness_scope": "metadata_and_holdout_inputs_only; R4_resources_not_checked",
        "resource_gate": {"status": "not_checked", "owner": "original_R4_execution"},
        "blockers": metadata_blockers + gate_blockers, "original_probe": probe,
        "training_executed": False, "model_loaded": False,
        "resource_lease_acquired": False, "production_changed": False,
        "quality_conclusion": "not_established"})


def _execution_inputs(plan_path):
    """Fail before any subprocess when a plan has no valid independent holdout."""
    try:
        from .holdout_gate import verify_holdout
        checked = _checked_plan(plan_path)
        gate = verify_holdout(checked[0])
        return checked, gate
    except (R5Error, OSError, ValueError, KeyError, TypeError, AttributeError) as exc:
        raise R5Error("pilot is blocked: " + str(exc)) from exc


def execution_argv(plan_path):
    """Return a structured command after a NEW check; never run it here."""
    # This first, entirely local pass prevents old plans and cached green flags
    # from reaching even the metadata subprocess without the required evidence.
    initial_checked, initial_gate = _execution_inputs(plan_path)
    report = preflight_pilot(plan_path)
    if report.get("metadata_ready") is not True or report.get("execution_gate", {}).get("status") != "passed":
        raise R5Error("pilot is blocked: " + "; ".join(x["reason"] for x in report["blockers"]))
    # Re-read after the probe too; a file changed during a metadata check must
    # not be grandfathered by its earlier success report.
    (plan, manifest, config, _), gate = _execution_inputs(plan_path)
    if (plan != initial_checked[0] or gate != initial_gate
            or report["execution_gate"].get("binding_sha256") != gate["sha256"]):
        raise R5Error("pilot is blocked: plan or holdout binding changed during the metadata probe")
    return {"argv": [plan["python_interpreter"]["path"], "-m", "training.mlx_r4",
                     "--manifest", str(manifest), "--config", str(config),
                     "--output", plan["run_directory"], "--execute"],
            "cwd": plan["runtime_source_binding"]["roots"]["training"],
            "env": _environment(plan["runtime_source_binding"]),
            "inherit_environment": True, "shell": False,
            "preflight_sha256": report["sha256"], "training_executed": False,
            "holdout_gate_sha256": gate["sha256"], "resource_status": "not_checked",
            "note": "Start explicitly in the real environment; the unchanged R4 worker rechecks its data, code, lease, resources and original model identity."}
