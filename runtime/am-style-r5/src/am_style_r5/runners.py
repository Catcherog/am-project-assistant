"""Explicit original-queue and isolated MLX generation adapters.

Importing this module performs no model call. Only explicit execution commands
write the existing synthetic probe queue or launch an isolated MLX worker.
No production dispatcher, weight installation, merge, or deployment is provided.
"""
from __future__ import annotations

import argparse
import json
import math
import os
from pathlib import Path
import signal
import subprocess
import sys
import time
import uuid

from .common import (R5Error, canonical, object_hash, file_hash, read_json, read_jsonl, require_sha,
                     seal, check_seal, write_json, write_jsonl)


QWEN_SETTINGS = {"max_tokens": 650, "timeout_seconds": 90, "temperature": None}
MLX_SETTINGS = {"temperature": 0.0, "seed": 221, "max_tokens": 192,
                "max_prompt_tokens": 1024, "enable_thinking": False}
_UNSPECIFIED = object()


def runner_source_hash():
    """Digest of the actual R4/R5 execution bundle, including dependency identity."""
    return object_hash(_execution_sources())


def _generation(arm):
    return arm["generation_settings"]


def _identity(arm):
    return arm.get("identity", arm)


def _check_real_plan(plan, arm):
    from .evaluation import validate_plan
    validate_plan(plan)
    if plan.get("mode") != "real" or arm not in plan["arms"]:
        raise R5Error("Execution requires an explicit real plan and selected arm")


def _case_ids(plan, selected=None):
    values = [c["case_id"] for c in plan["cases"]] if selected is None else list(selected)
    if len(values) != len(set(values)):
        raise R5Error("Duplicate case selection")
    selected = set(values)
    known = {c["case_id"] for c in plan["cases"]}
    if not selected or not selected <= known:
        raise R5Error("Unknown or empty case selection")
    return [c for c in plan["cases"] if c["case_id"] in selected]


def _execution_sources():
    """Bind the code actually imported by this interpreter, not only the facade."""
    from am_style import prompt
    from training import mlx_r4_runtime
    from .pilot import _source_binding
    binding = _source_binding({"training": str(Path(mlx_r4_runtime.__file__).resolve().parents[1]),
                               "am_style": str(Path(prompt.__file__).resolve().parents[1])})
    return {"r4": binding, "r4_code_sha256": mlx_r4_runtime.code_hashes(),
            "r5": {name: file_hash(Path(__file__).parent / name)
                   for name in ("runners.py", "common.py", "evaluation.py", "pilot.py")}}


def _artifact(root, name, expected=_UNSPECIFIED):
    root = Path(root).resolve()
    raw = root / name
    path = raw.resolve()
    if raw.is_symlink() or not path.is_relative_to(root) or not path.is_file():
        raise R5Error("Missing or out-of-root artifact: " + name)
    if expected is not _UNSPECIFIED:
        require_sha(expected, name)
        if file_hash(path) != expected:
            raise R5Error("Artifact changed: " + name)
    return path


def _protected_roots(manifest):
    roots = [Path(manifest["local_model_path"]).resolve()]
    service = manifest["service"]
    roots.extend(Path(p).resolve().parent for p in service["protected_config_files"])
    roots.extend(Path(service[k]).resolve().parent for k in ("lease_file", "gateway_db", "chat_db"))
    return roots


def _fresh_output(output_dir, manifest, *, additional_roots=()):
    original = Path(output_dir)
    out = original.resolve()
    if (original.exists() or original.is_symlink()
            or any(out.is_relative_to(Path(root).resolve())
                   for root in (*_protected_roots(manifest), *additional_roots))):
        raise R5Error("Use a fresh experiment directory outside model/service/input roots")
    return out


def validate_queue_config(queue_config):
    """Read the original protected config chain and the current consumer PID.

    This is metadata binding, not authentication of a process or a remote model.
    Each received result must also match the label, process and model identity.
    """
    from training.mlx_r4_resources import validate_service_paths
    manifest_path = Path(queue_config["service_manifest_path"])
    if not manifest_path.is_absolute() or not manifest_path.is_file() or file_hash(manifest_path) != queue_config.get("service_manifest_sha256"):
        raise R5Error("Original service manifest is missing or changed")
    manifest = read_json(manifest_path)
    validate_service_paths(manifest)
    protected = {Path(name).resolve(): sha for name, sha in manifest["service"]["protected_config_files"].items()}
    host = Path(queue_config["host_config_path"])
    if not host.is_absolute() or host.resolve() not in protected or file_hash(host) != protected[host.resolve()]:
        raise R5Error("Host config must be one of the manifest's original protected files")
    cfg = read_json(host)
    poc = Path(cfg["poc_config"])
    if not poc.is_absolute() or poc.resolve() not in protected or file_hash(poc) != protected[poc.resolve()]:
        raise R5Error("poc_config must be the protected original configuration")
    service_root = Path(read_json(poc)["service"]["root_directory"])
    root = Path(queue_config["queue_root"])
    if (not service_root.is_absolute() or not root.is_absolute()
            or root.resolve() != (service_root / "state/am-conversation-r2").resolve()):
        raise R5Error("Queue path differs from the original host's protected configuration")
    root = root.resolve()
    for name in ("requests", "results"):
        child = root / name
        if not child.is_dir() or not child.resolve().is_relative_to(root):
            raise R5Error("Use the existing original probe queue without escaping links")
    pid = queue_config.get("expected_consumer_pid")
    if type(pid) is not int or pid <= 0:
        raise R5Error("Record the current original consumer PID before probing")
    try:
        os.kill(pid, 0)
    except (ProcessLookupError, PermissionError) as exc:
        raise R5Error("Original consumer PID is not observable as alive; reconcile before submitting") from exc
    return {"manifest": manifest, "root": root, "pid": pid,
            "manifest_file_sha256": file_hash(manifest_path),
            "host_config_sha256": file_hash(host), "poc_config_sha256": file_hash(poc),
            "identity_limit": "protected_config_and_observed_PID_are_not_process_authentication"}


def run_original_qwen(plan, queue_config, output_dir, *, selected=None, execute=False):
    """Sequential probes through the protected original queue; never a consumer.

    Every submission retains its request and result/error. An unverified result
    stops the batch; absence of a result does not prove that inference stopped.
    """
    if execute is not True:
        raise R5Error("Original queue execution requires execute=True")
    from am_style.prompt import RESPONSE_SCHEMA
    from training.mlx_r4_resources import PRODUCTION_MODEL, PRODUCTION_DIGEST
    from .evaluation import make_result_record
    _check_real_plan(plan, "production_q8")
    cases = _case_ids(plan, selected)
    arm, sources = plan["arms"]["production_q8"], _execution_sources()
    identity = _identity(arm)
    if _generation(arm) != QWEN_SETTINGS:
        raise R5Error("Unsupported Qwen probe settings; preserve recorded provider defaults")
    if identity.get("model") != PRODUCTION_MODEL or identity.get("model_digest") != PRODUCTION_DIGEST:
        raise R5Error("Qwen plan identity differs from the original production model")
    binding = validate_queue_config(queue_config)
    manifest, root, pid = binding["manifest"], binding["root"], binding["pid"]
    requests, results = root / "requests", root / "results"
    out = _fresh_output(output_dir, manifest, additional_roots=(root,))
    out.mkdir(parents=True, exist_ok=False)
    records, stopped, submitted, observed_calls, calls_unknown = [], False, 0, 0, False
    for number, case in enumerate(cases, 1):
        label = "r5-" + uuid.uuid4().hex
        wire = case["student_wire_messages"]
        if len(wire) != 1 or wire[0]["role"] != "user":
            raise R5Error("Original Qwen requires the reviewed single-user native envelope")
        job = {"operation": "task_model_probe", "case": "SYN-QRY-01", "expires_at": time.time() + 150,
               "model_request": {"prompt": wire[0]["content"], "json_schema": RESPONSE_SCHEMA,
                                 "max_tokens": QWEN_SETTINGS["max_tokens"], "timeout": QWEN_SETTINGS["timeout_seconds"]}}
        capture = {"provider_request": job, "actual_messages": wire,
                   "generation_settings": QWEN_SETTINGS, "queue_label": label,
                   "queue_root": str(root), "expected_consumer_pid": pid,
                   "service_manifest_sha256": binding["manifest_file_sha256"],
                   "execution_sources": sources,
                   "capture_scope": "original_coordinator_queue_exchange_not_HTTP_payload"}
        request_text = canonical(capture)
        write_json(out / f"{number:03d}-request.json", capture)
        temporary, target = requests / (label + ".tmp"), requests / (label + ".json")
        started, published, confirmed = time.monotonic(), False, False
        status, raw, response_text, error = "failed", "", "", None
        latency = {}
        try:
            current = validate_queue_config(queue_config)
            if current != binding or _execution_sources() != sources:
                raise R5Error("Original queue configuration or runner code changed")
            with temporary.open("x", encoding="utf-8") as stream:
                stream.write(canonical(job)); stream.flush(); os.fsync(stream.fileno())
            os.link(temporary, target)  # An existing request can never be overwritten.
            published, submitted = True, submitted + 1
            temporary.unlink()
            result_path = results / (label + ".json")
            while not result_path.exists() and time.monotonic() - started < 140:
                time.sleep(.2)
            if not result_path.exists():
                raise R5Error("original_result_missing; stop is not confirmed")
            _artifact(results, label + ".json")
            response_text = result_path.read_text(encoding="utf-8")
            (out / f"{number:03d}-response.json").write_text(response_text, encoding="utf-8")
            result = read_json(result_path)
            checks = {"label": label, "operation": "task_model_probe", "model": PRODUCTION_MODEL,
                      "model_digest": PRODUCTION_DIGEST, "original_host_pid": pid,
                      "original_coordinator": True, "synthetic_only": True, "external_business_calls": 0}
            if any(result.get(k) != v for k, v in checks.items()):
                raise R5Error("Original coordinator result identity/scope differs; reconcile before any further call")
            calls = result.get("model_calls", {}).get("calls")
            if isinstance(calls, list):
                observed_calls += len(calls)
                if len(calls) == 1:
                    seconds = calls[0].get("provider_seconds")
                    if type(seconds) in (float, int) and math.isfinite(seconds) and seconds >= 0:
                        latency["provider"] = seconds * 1000
            else:
                calls_unknown = True
            if result.get("ok") is True:
                raw = result.get("text")
                if not isinstance(raw, str) or not raw.strip():
                    raise R5Error("Original coordinator completed without a text response")
                status = "truncated" if result.get("finish_reason") == "length" else "completed"
                confirmed = True
                error = "token_limit" if status == "truncated" else None
            else:
                error = str(result.get("error", "unknown_original_error"))
                confirmed = result.get("stop_confirmed") is True
                status = "failed" if confirmed else "stop_unknown"
        except Exception as exc:
            error = type(exc).__name__ + ": " + str(exc)
            status = "stop_unknown" if published else "failed"
            confirmed = not published
            if published:
                calls_unknown = True
        finally:
            if temporary.exists():
                temporary.unlink()  # Only our unique unpublished temporary file.
        latency["end_to_end"] = (time.monotonic() - started) * 1000
        stopped = status == "stop_unknown"
        write_json(out / f"{number:03d}-attempt.json", {"queue_label": label, "submitted": published,
                   "status": status, "error": error, "stop_confirmed": confirmed,
                   "source_scope": "original_queue_exchange", "production_changed": False})
        record = make_result_record(plan, case["case_id"], "production_q8", status=status,
            runner_source_sha256=runner_source_hash(), request_artifact=request_text,
            response_artifact=response_text, raw_output=raw if isinstance(raw, str) else "", latency_ms=latency,
            capture_kind="actual_provider_exchange", error=error,
            request_binding={"wire_messages": {"path": ["actual_messages"]},
                             "generation_settings": {"path": ["generation_settings"]}},
            response_binding={"path": ["text"]})
        records.append(record)
        write_json(out / f"{number:03d}-record.json", record)
        # A pre-submission failure also requires reconciliation, not another probe.
        if stopped or status == "failed":
            break
    write_jsonl(out / "results.jsonl", records)
    run_status = ("stop_unknown" if stopped else "completed"
                  if len(records) == len(cases) and all(r["status"] in {"completed", "truncated"} for r in records)
                  else "failed")
    report = {"schema": "am-r5-original-queue-run/1", "plan_sha256": plan["sha256"],
              "status": run_status,
              "actual_calls": None if calls_unknown else observed_calls,
              "submitted_probes": submitted, "recorded_attempts": len(records),
              "stop_unknown": stopped, "resume_requires_reconciliation": stopped,
              "remaining_cases": len(cases) - len(records), "created_consumers": 0,
              "business_delivery": False, "production_changed": False}
    write_json(out / "run.json", report)
    return report


def validate_sft_run(run_directory, manifest):
    """Verify the selected pilot, consumed data, finite updates and exact adapter.

    Hashes bind evidence; they cannot authenticate an arbitrary executor. The
    original R4 validators and source binding are reused rather than trusting a
    self-declared mechanics_closed flag.
    """
    if not run_directory:
        raise R5Error("Select an existing completed SFT pilot directory")
    from training.mlx_r4_contracts import validate_plan, require_finite_update
    sources = _execution_sources()
    root = Path(run_directory).resolve()
    result = read_json(_artifact(root, "result.json"))
    if result.get("schema") != "am-style-mlx-r4-result/1" or result.get("mechanics_closed") is not True:
        raise R5Error("Selected SFT run did not complete the original fit/reload contract")
    hashes = result.get("files_sha256", {})
    for name in ("request.json", "fit.json", "reload.json", "kernel-validation.json", "steps.json", "adapters.safetensors", "adapter_config.json"):
        if name not in hashes:
            raise R5Error("Selected SFT graph is incomplete: " + name)
        _artifact(root, name, hashes[name])
    request, fit, reload = (read_json(root / name) for name in ("request.json", "fit.json", "reload.json"))
    if (request.get("manifest") != manifest or request.get("config", {}).get("stage") != "pilot"
            or request.get("code_sha256") != sources["r4_code_sha256"]
            or result.get("request_sha256") != object_hash(request)
            or fit.get("request_sha256") != object_hash(request)
            or reload.get("request_sha256") != object_hash(request)
            or fit.get("stage") != "pilot" or fit.get("training_completed") is not True
            or fit.get("adapter_tensor_roundtrip_verified") is not True
            or reload.get("fresh_load_verified") is not True
            or reload.get("adapter_complete_keyset_verified") is not True
            or reload.get("adapter_tensor_equality_verified") is not True
            or reload.get("fit_record_sha256") != hashes["fit.json"]):
        raise R5Error("Selected SFT is not the completed same-base/source pilot")
    train, dev = validate_plan(request["config"])
    expected_ids = [row["id"] for row in train]
    consumed = fit.get("consumed_train_ids", [])
    if (fit.get("train_examples") != len(train) or fit.get("dev_examples") != len(dev)
            or fit.get("epochs") != 1 or len(consumed) != len(set(consumed))
            or set(consumed) != set(expected_ids)):
        raise R5Error("Selected pilot consumed different or duplicated training examples")
    steps = read_json(root / "steps.json")
    # steps.json is a JSON list; the public strict reader accepts either shape.
    wanted_steps = math.ceil(len(train) / request["config"]["gradient_accumulation"])
    if not isinstance(steps, list) or len(steps) != wanted_steps or fit.get("optimizer_steps") != wanted_steps:
        raise R5Error("Selected pilot step count does not match one pass over its train data")
    step_ids = []
    for number, step in enumerate(steps, 1):
        ids = step.get("ids", [])
        remaining = len(train) - len(step_ids)
        if (step.get("optimizer_step") != number or len(ids) != min(4, remaining)
                or len(ids) != len(set(ids))):
            raise R5Error("Selected pilot gradient accumulation or example order is inconsistent")
        require_finite_update(step.get("gradient", {}), step.get("parameter_delta", {}))
        step_ids.extend(ids)
    if step_ids != consumed:
        raise R5Error("Step records and consumed IDs differ")
    require_finite_update(steps[-1]["gradient"], fit.get("parameter_delta", {}))
    adapter = hashes["adapters.safetensors"]
    if (adapter != fit.get("adapter_sha256") or adapter != reload.get("adapter_sha256")
            or hashes["adapter_config.json"] != fit.get("adapter_config_sha256")):
        raise R5Error("Selected adapter identity/config differs between fit and reload")
    for name, key in (("tokenized.json", "tokenized_sha256"), ("teacher-sft.json", "teacher_sft_sha256")):
        _artifact(root, name, fit.get(key))
    kernel = read_json(root / "kernel-validation.json")
    if (kernel.get("status") != "passed_on_actual_mlx" or kernel.get("numerical_validation_executed") is not True
            or kernel.get("code_sha256") != request["code_sha256"]
            or fit.get("kernel_validation_sha256") != hashes["kernel-validation.json"]
            or reload.get("teacher_reload_comparison", {}).get("reload_within_tolerance") is not True):
        raise R5Error("Selected pilot kernel or fresh-load diagnostics are incomplete")
    phases = result.get("phases", [])
    if (len(phases) != 2 or [p.get("phase") for p in phases] != ["fit", "reload"]
            or any(p.get("status") != "completed" or p.get("stop_confirmed") is not True
                   or p.get("exit_code") != 0 or p.get("timed_out") is not False
                   or p.get("attempted") is not True or type(p.get("pid")) is not int or p["pid"] <= 0 for p in phases)
            or phases[0]["pid"] == phases[1]["pid"]
            or reload.get("fresh_interpreter_pid") != phases[1]["pid"]):
        raise R5Error("Selected pilot requires two distinct completed/stopped fit and reload workers")
    return {"root": str(root), "adapter_sha256": adapter, "request_sha256": object_hash(request),
            "result_file_sha256": file_hash(root / "result.json"),
            "adapter_config_sha256": hashes["adapter_config.json"],
            "train_sha256": request["config"]["train_sha256"], "dev_sha256": request["config"]["dev_sha256"]}


def _mlx_identity(plan, arm_name, manifest, selected_run):
    identity = _identity(plan["arms"][arm_name])
    if (identity.get("base_revision") != manifest["revision"]
            or identity.get("model_manifest_sha256") != object_hash(manifest)
            or identity.get("tokenizer_sha256") != manifest["files"]["tokenizer.json"]["sha256"]):
        raise R5Error("Evaluation plan does not identify the sealed local MLX base")
    if arm_name == "mlx_sft":
        if (not selected_run or identity.get("adapter_sha256") != selected_run["adapter_sha256"]
                or identity.get("pilot_request_sha256") != selected_run["request_sha256"]
                or identity.get("pilot_result_file_sha256") != selected_run["result_file_sha256"]
                or identity.get("train_file_sha256") != selected_run["train_sha256"]
                or identity.get("dev_file_sha256") != selected_run["dev_sha256"]):
            raise R5Error("Plan and selected SFT checkpoint/run differ")
    elif arm_name != "mlx_base" or selected_run is not None or identity.get("adapter_sha256") is not None:
        raise R5Error("Base arm must not contain a selected SFT run or adapter")
    return identity


def _group_gone(pid):
    try:
        os.killpg(pid, 0)
        return False
    except ProcessLookupError:
        return True
    except PermissionError:
        return False


def _stop_group(process):
    """Try both signals even if the group leader exits before its descendants."""
    code = process.poll()
    for sig in (signal.SIGTERM, signal.SIGKILL):
        if _group_gone(process.pid):
            break
        try:
            os.killpg(process.pid, sig)
        except ProcessLookupError:
            pass
        try:
            code = process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            pass
    until = time.monotonic() + 1
    while not _group_gone(process.pid) and time.monotonic() < until:
        time.sleep(.05)
    return code, code is not None and _group_gone(process.pid)


def _validate_worker_completion(out, request):
    from .evaluation import import_results
    result = check_seal(read_json(_artifact(out, "worker-result.json")))
    records_path = _artifact(out, "results.jsonl", result.get("results_sha256"))
    records = read_jsonl(records_path)
    if (result.get("status") != "completed" or result.get("request_sha256") != request["sha256"]
            or result.get("plan_sha256") != request["plan"]["sha256"] or result.get("arm") != request["arm"]
            or result.get("rows") != len(request["case_ids"]) or len(records) != len(request["case_ids"])
            or [r.get("case_id") for r in records] != request["case_ids"]
            or any(r.get("arm") != request["arm"] or r.get("runner_source_sha256") != request["runner_source_sha256"] for r in records)
            or result.get("record_sha256") != [r.get("sha256") for r in records]):
        raise R5Error("Worker exit status does not match a complete bound result set")
    import_results(request["plan"], records,
                   verifier=lambda *_: {"decision": "indeterminate", "reason": "parent_integrity_only", "verifier": "not_run"})
    return records


def _parent_interruption(out, request, status, error, *, stopped):
    """Retain the first unfinished attempt without changing surviving worker files."""
    from .evaluation import import_results, make_result_record
    records = [read_json(_artifact(out, p.name)) for p in sorted(out.glob("[0-9][0-9][0-9]-record.json"))]
    if any(r.get("case_id") not in request["case_ids"] or r.get("arm") != request["arm"] for r in records):
        raise R5Error("Partial worker evidence contains an unselected case or arm")
    import_results(request["plan"], records,
                   verifier=lambda *_: {"decision": "indeterminate", "reason": "parent_integrity_only", "verifier": "not_run"})
    completed = {record["case_id"] for record in records}
    missing = [case for case in _case_ids(request["plan"], request["case_ids"]) if case["case_id"] not in completed]
    if missing:
        case = missing[0]
        number = request["case_ids"].index(case["case_id"]) + 1
        capture_path = out / f"{number:03d}-request-capture.json"
        # A worker that already recorded its failed first case did not attempt
        # the remaining cases. Keep those missing instead of inventing failures.
        if capture_path.is_file() or (not records and not (out / "worker-result.json").exists()):
            capture = capture_path.read_text(encoding="utf-8") if capture_path.is_file() else canonical({
                "planned_messages": case["student_wire_messages"], "provider_request_observed": False})
            response = canonical({"parent_observation": status, "stop_confirmed": stopped, "error": error})
            record = make_result_record(request["plan"], case["case_id"], request["arm"], status=status,
                runner_source_sha256=request["runner_source_sha256"], request_artifact=capture,
                response_artifact=response, capture_kind="actual_provider_exchange", error=error)
            write_json(out / "parent-interruption-record.json", record)
            records.append(record)
    # An unconfirmed live worker may still write its own results. Keep these
    # parent observations separate and require reconciliation before collecting.
    write_jsonl(out / "parent-results.jsonl", records)
    return str(out / "parent-results.jsonl")


def run_mlx_arm(plan, arm_name, manifest_path, output_dir, *, sft_run=None,
                selected=None, execute=False, worker_timeout_seconds=300):
    """One isolated process for one arm; verify results AND group termination."""
    if execute is not True:
        raise R5Error("MLX generation requires execute=True")
    _check_real_plan(plan, arm_name)
    if arm_name not in {"mlx_base", "mlx_sft"}:
        raise R5Error("Select mlx_base or mlx_sft")
    if type(worker_timeout_seconds) is not int or not 30 <= worker_timeout_seconds <= 300:
        raise R5Error("Keep the existing 30–300 second worker resource window")
    cases = _case_ids(plan, selected)
    if _generation(plan["arms"][arm_name]) != MLX_SETTINGS:
        raise R5Error("Use the first reviewed deterministic MLX generation settings")
    from training.mlx_r4_contracts import validate_base, validate_environment
    from training.mlx_r4_resources import validate_service_paths
    sources = _execution_sources()
    manifest = read_json(manifest_path)
    validate_base(manifest); validate_environment(manifest); validate_service_paths(manifest)
    if arm_name == "mlx_base" and sft_run is not None:
        raise R5Error("Base evaluation must not receive an SFT run")
    selected_run = validate_sft_run(sft_run, manifest) if arm_name == "mlx_sft" else None
    _mlx_identity(plan, arm_name, manifest, selected_run)
    out = _fresh_output(output_dir, manifest,
                        additional_roots=(selected_run["root"],) if selected_run else ())
    out.mkdir(parents=True, exist_ok=False)
    request = seal({"schema": "am-r5-mlx-generation-request/1", "plan": plan, "arm": arm_name,
        "manifest": manifest, "selected_run": selected_run, "execution_sources": sources,
        "case_ids": [c["case_id"] for c in cases], "timeout_seconds": worker_timeout_seconds,
        "runner_source_sha256": runner_source_hash()})
    path = out / "request.json"
    write_json(path, request)
    env = dict(os.environ, HF_HUB_OFFLINE="1", TRANSFORMERS_OFFLINE="1", TOKENIZERS_PARALLELISM="false")
    roots = sources["r4"]["roots"]
    env["PYTHONPATH"] = os.pathsep.join((str(Path(__file__).resolve().parents[1]), roots["training"], roots["am_style"]))
    argv = [sys.executable, "-m", "am_style_r5.runners", "--mlx-worker", str(path), "--execute"]
    process, timed_out, code, stopped, error = None, False, None, True, None
    try:
        with (out / "worker.log").open("xb") as log:
            process = subprocess.Popen(argv, env=env, stdin=subprocess.DEVNULL, stdout=log,
                                       stderr=subprocess.STDOUT, start_new_session=True,
                                       cwd=str(Path(__file__).resolve().parents[2]))
            try:
                code = process.wait(timeout=worker_timeout_seconds)
                stopped = _group_gone(process.pid)
                if not stopped:
                    error = "worker exited with live descendants"
                    code, stopped = _stop_group(process)
            except subprocess.TimeoutExpired:
                timed_out, error = True, "worker_timeout"
                code, stopped = _stop_group(process)
    except BaseException as exc:
        error = type(exc).__name__ + ": " + str(exc)
        if process is not None:
            code, stopped = _stop_group(process)
        # Preserve the cleanup result before allowing KeyboardInterrupt/SystemExit
        # to propagate and prevent any caller from automatically continuing.
        if not isinstance(exc, Exception):
            write_json(out / "parent-result.json", {"schema": "am-r5-mlx-generation-parent/1",
                "status": "failed" if stopped else "stop_unknown",
                "error": error, "stop_confirmed": stopped, "plan_sha256": plan["sha256"],
                "request_sha256": request["sha256"], "arm": arm_name,
                "resume_requires_reconciliation": not stopped, "production_changed": False})
            raise
    status = "completed" if stopped and code == 0 and not timed_out and error is None else "failed" if stopped else "stop_unknown"
    verified_rows = None
    if status == "completed":
        try:
            verified_rows = len(_validate_worker_completion(out, request))
        except Exception as exc:
            status, error = "failed", "worker_result_invalid: " + str(exc)
    record_file = str(out / "results.jsonl") if status == "completed" else None
    if status != "completed":
        try:
            record_file = _parent_interruption(out, request, "stop_unknown" if not stopped else "timeout" if timed_out else "failed",
                                                error or "worker_failed", stopped=stopped)
        except Exception as exc:
            error = (error or "worker_failed") + "; partial_evidence_requires_reconciliation: " + str(exc)
    report = {"schema": "am-r5-mlx-generation-parent/1", "pid": process.pid if process else None, "exit_code": code,
              "timed_out": timed_out, "stop_confirmed": stopped, "status": status, "error": error,
              "verified_rows": verified_rows, "records_path": record_file,
              "resume_requires_reconciliation": not stopped,
              "arm": arm_name, "plan_sha256": plan["sha256"], "request_sha256": request["sha256"],
              "production_changed": False}
    write_json(out / "parent-result.json", report)
    return report


def _mlx_worker(path):
    from .evaluation import make_result_record
    from training.mlx_r4_contracts import validate_base, validate_environment
    from training.mlx_r4_resources import OriginalLease
    from training.mlx_r4_runtime import _assert_adapter_equal
    from .common import text_hash
    request, out = check_seal(read_json(path)), Path(path).resolve().parent
    if (request.get("schema") != "am-r5-mlx-generation-request/1"
            or request.get("runner_source_sha256") != runner_source_hash()
            or request.get("execution_sources") != _execution_sources()):
        raise R5Error("Generation worker source/schema binding changed")
    plan, arm_name, manifest = request["plan"], request["arm"], request["manifest"]
    _check_real_plan(plan, arm_name)
    if arm_name not in {"mlx_base", "mlx_sft"}:
        raise R5Error("Internal worker accepts only an MLX arm")
    if type(request.get("timeout_seconds")) is not int or not 30 <= request["timeout_seconds"] <= 300:
        raise R5Error("Internal worker resource window is invalid")
    cases = _case_ids(plan, request["case_ids"])
    run = validate_sft_run(request["selected_run"]["root"], manifest) if arm_name == "mlx_sft" else None
    if run != request["selected_run"]:
        raise R5Error("Selected SFT graph changed after generation request was sealed")
    identity = _mlx_identity(plan, arm_name, manifest, run)
    settings = _generation(plan["arms"][arm_name])
    if settings != MLX_SETTINGS:
        raise R5Error("Generation settings changed")
    # Also defend direct internal-worker invocation; the parent is not its gate.
    if any(out.is_relative_to(root) for root in _protected_roots(manifest)) or (run and out.is_relative_to(Path(run["root"]))):
        raise R5Error("Worker output overlaps protected model/service/SFT inputs")
    validate_environment(manifest)
    validate_base(manifest, hash_weights=True)
    records, load_ms, resource_history = [], None, []
    current, number, text, capture, last = cases[0], 1, "", None, None
    status, error, load_attempted, generation_attempted = "completed", None, False, False
    try:
        with OriginalLease(manifest, request["timeout_seconds"]) as lease:
            resource_history = lease.history
            import mlx.core as mx
            from mlx_lm import load, stream_generate
            mx.set_memory_limit(22 * 2**30); mx.set_cache_limit(128 * 2**20)
            load_started, load_attempted = time.monotonic(), True
            model, tokenizer = load(manifest["local_model_path"], tokenizer_config={"trust_remote_code": False})
            if model.model_type != "qwen3_5" or len(model.layers) != 64:
                raise R5Error("Loaded model is not the sealed 64-layer Qwen text branch")
            template = getattr(tokenizer, "chat_template", None)
            if not isinstance(template, str) or text_hash(template) != manifest["files"]["chat_template.jinja"]["sha256"]:
                raise R5Error("Loaded chat template differs from the sealed snapshot")
            if arm_name == "mlx_sft":
                from mlx_lm.tuner.utils import load_adapters
                from mlx.utils import tree_flatten
                model.freeze()
                model = load_adapters(model, run["root"])
                expected = mx.load(str(_artifact(run["root"], "adapters.safetensors", run["adapter_sha256"])))
                actual = {k: v for k, v in tree_flatten(model.parameters()) if k.endswith(("lora_a", "lora_b"))}
                _assert_adapter_equal(expected, actual, mx)
            model.eval()
            load_ms = (time.monotonic() - load_started) * 1000
            for number, current in enumerate(cases, 1):
                text, capture, last, generation_attempted = "", None, None, False
                if _execution_sources() != request["execution_sources"]:
                    raise R5Error("Generation source changed before the next case")
                lease.check()
                mx.random.seed(settings["seed"])
                wire = current["student_wire_messages"]
                tokens = tokenizer.apply_chat_template(wire, tokenize=True, add_generation_prompt=True,
                    enable_thinking=False, preserve_thinking=False, return_dict=False)
                if (not isinstance(tokens, list) or not tokens or any(type(t) is not int for t in tokens)
                        or len(tokens) > settings["max_prompt_tokens"]):
                    raise R5Error("Prompt token budget/type invalid; never truncate the frozen task")
                capture = {"actual_messages": wire, "generation_settings": settings,
                    "execution_sources": request["execution_sources"],
                    "provider_request": {"token_ids": tokens, "model_identity": identity,
                        "sampler": "pinned_upstream_default_greedy", "max_tokens": settings["max_tokens"],
                        "template_options": {"enable_thinking": False, "preserve_thinking": False, "return_dict": False}}}
                write_json(out / f"{number:03d}-request-capture.json", capture)
                started, generation_attempted = time.monotonic(), True
                for item in stream_generate(model, tokenizer, tokens, max_tokens=settings["max_tokens"],
                                            prompt_progress_callback=lambda *_: lease.check()):
                    lease.check()
                    text += item.text
                    last = item
                if last is None or last.finish_reason not in {"stop", "length"}:
                    raise R5Error("MLX returned no recognized final generation record")
                response = {"text": text, "finish_reason": last.finish_reason,
                    "prompt_tokens": last.prompt_tokens, "generation_tokens": last.generation_tokens,
                    "prompt_tps": last.prompt_tps, "generation_tps": last.generation_tps,
                    "peak_memory_GB": last.peak_memory, "model_identity": identity,
                    "latency_scope": "generation_only_model_load_reported_separately"}
                write_json(out / f"{number:03d}-response.json", response)
                row_status = "completed" if last.finish_reason == "stop" else "truncated"
                record = make_result_record(plan, current["case_id"], arm_name, status=row_status,
                    runner_source_sha256=request["runner_source_sha256"], request_artifact=canonical(capture),
                    response_artifact=canonical(response), raw_output=text,
                    latency_ms={"end_to_end": (time.monotonic() - started) * 1000,
                                "model_load": load_ms if number == 1 else 0.0},
                    capture_kind="actual_provider_exchange", error=None if row_status == "completed" else "token_limit",
                    request_binding={"wire_messages": {"path": ["actual_messages"]},
                                     "generation_settings": {"path": ["generation_settings"]}},
                    response_binding={"path": ["text"]})
                records.append(record)
                write_json(out / f"{number:03d}-record.json", record)
    except Exception as exc:
        status, error = "failed", type(exc).__name__ + ": " + str(exc)
        if current["case_id"] not in {r["case_id"] for r in records}:
            response = {"text": text, "error": error, "finish_reason": "failed",
                        "model_load_attempted": load_attempted, "generation_attempted": generation_attempted}
            write_json(out / f"{number:03d}-failure.json", response)
            failed_capture = capture or {"planned_messages": current["student_wire_messages"], "provider_request_observed": False}
            record = make_result_record(plan, current["case_id"], arm_name, status="failed",
                runner_source_sha256=request["runner_source_sha256"], request_artifact=canonical(failed_capture),
                response_artifact=canonical(response), raw_output=text,
                capture_kind="actual_provider_exchange", error=error)
            records.append(record)
            write_json(out / f"{number:03d}-record.json", record)
    finally:
        result_sha = write_jsonl(out / "results.jsonl", records)
        write_json(out / "worker-result.json", seal({"status": status, "error": error, "rows": len(records),
            "record_sha256": [r["sha256"] for r in records], "results_sha256": result_sha,
            "request_sha256": request["sha256"], "arm": arm_name,
            "resource_checks": resource_history, "model_load_ms": load_ms,
            "model_load_attempted": load_attempted,
            "plan_sha256": plan["sha256"], "production_changed": False}))
    return 0 if status == "completed" else 2


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--mlx-worker", required=True)
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args(argv)
    if not args.execute:
        raise R5Error("Internal worker requires explicit execution")
    return _mlx_worker(args.mlx_worker)


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"R5 generation blocked: {exc}", file=sys.stderr)
        raise SystemExit(2)
