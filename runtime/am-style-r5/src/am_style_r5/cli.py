"""Preparation is the default. Actual model execution has explicit commands."""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import subprocess
import sys

from .common import (R5Error, canonical, object_hash, text_hash, read_json, read_jsonl,
                     write_json, write_jsonl, file_hash)


ROOT = Path(__file__).resolve().parents[2]
BASELINE_TRAIN = ROOT / "baseline/data/r4-gold/sft.train.jsonl"
BASELINE_DEV = ROOT / "baseline/data/r4-gold/sft.dev.jsonl"


def _fresh(path):
    out = Path(path).resolve()
    out.mkdir(parents=True, exist_ok=False)
    return out


def _text(path, value):
    with Path(path).open("x", encoding="utf-8") as stream:
        stream.write(value.rstrip() + "\n")


def prepare(output):
    from .dataset import (freeze_group_registry, build_review_batch,
                          feedback_template, markdown_review, export_gold_snapshot)
    from .scenarios import calibration_specs, evaluation_cases
    specs = calibration_specs()
    registry = freeze_group_registry(specs, baseline_train=BASELINE_TRAIN, baseline_dev=BASELINE_DEV)
    batch = build_review_batch(specs, registry, version="R5-calibration-01")
    out = _fresh(output)
    write_json(out / "scenarios.json", specs)
    write_json(out / "group-registry.json", registry)
    write_json(out / "review-batch.json", batch)
    write_json(out / "feedback.pending.json", feedback_template(batch))
    write_json(out / "evaluation.calibration.json", evaluation_cases(specs))
    _text(out / "review.md", markdown_review(batch))
    snapshot = export_gold_snapshot(registry, [], baseline_train=BASELINE_TRAIN,
                                   baseline_dev=BASELINE_DEV, output_dir=out / "current-gold")
    summary = {"new_proposals": len(specs), "new_train_proposals": sum(s["split"] == "train" for s in specs),
        "new_dev_proposals": sum(s["split"] == "dev" for s in specs), "new_human_approvals": 0,
        "current_gold": snapshot["counts"], "actual_model_calls": 0, "training_executed": False,
        "production_changed": False, "pilot_ready": False}
    write_json(out / "preparation.json", summary)
    return summary


def _fixture_arms():
    from .runners import MLX_SETTINGS, QWEN_SETTINGS
    shared = {"base_revision": "1" * 40, "model_manifest_sha256": "2" * 64,
              "tokenizer_sha256": "3" * 64, "generation_settings": MLX_SETTINGS}
    return {"production_q8": {"model": "fixture-production", "model_digest": "4" * 64,
                              "generation_settings": QWEN_SETTINGS},
            "mlx_base": {**shared, "adapter_sha256": None},
            "mlx_sft": {**shared, "adapter_sha256": "5" * 64}}


def _blind_files(out, public, private):
    from itertools import combinations
    from .evaluation import preference_content_hash
    write_json(out / "blind-public.json", public)
    private_path = out / "blind-private.json"
    descriptor = os.open(private_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
        stream.write(json.dumps(private, ensure_ascii=False, indent=2) + "\n")
    pending = {"schema_version": "am-r5-human-feedback/1", "public_sha256": public["sha256"],
        "mode": public["mode"], "reviewer": None, "reviewed_at": None, "human_authored": False,
        "source_evidence_sha256": None, "ratings": [], "preferences": [],
        "instructions": "Copy only completed ratings into ratings; nulls and empty lists are pending, never approval."}
    for task in public["tasks"]:
        for option in task["options"]:
            pending.setdefault("pending_rating_items", []).append({"option_id": option["option_id"],
                "text_sha256": option["text_sha256"], "fact": None,
                "naturalness": None, "absolute_usable": None})
        for left, right in combinations(task["options"], 2):
            pending.setdefault("pending_preference_items", []).append({"task_id": task["task_id"],
                "option_ids": [left["option_id"], right["option_id"]],
                "content_sha256": preference_content_hash(task["task_id"], left, right), "outcome": None})
    write_json(out / "blind-feedback.pending.json", pending)
    lines = ["# 表达效果盲评", "", f"模式：`{public['mode']}`；材料性质：`{public['sample_status']}`。",
        "fixture 为流程演示；公开校准材料不计独立效果验收。" if public["sample_status"] == "not_independent" else "此批独立性声明仍需与原始工件一起核验。",
        "", "请先核对事实与回答是否完整，再评自然度（1–5）及能否直接采用。",
        "未评、无法判断、两者都差、同样好，分别保留；修改后的句子须另行评审。", ""]
    for i, task in enumerate(public["tasks"], 1):
        lines += [f"## {i}", "", "**问题：**" + str(task.get("question", "见事实说明")), "",
                  "**事实：**" + str(task["review_context"]), ""]
        for j, option in enumerate(task["options"], 1):
            lines += [f"**候选 {j}：**{option['text']}", "", f"标识：`{option['option_id']}`", ""]
    _text(out / "blind-review.md", "\n".join(lines))


def report_markdown(report):
    lines = ["# R5 表达效果报告", "", f"模式：`{report['mode']}`；材料性质：`{report['sample_status']}`。",
        "", "| 组别 | 计划场景 | JSON通过 | 人工已评场景 | 人工可直接采用 | 自然度中位数 |",
        "| --- | ---: | ---: | ---: | ---: | ---: |"]
    for arm, values in report["arms"].items():
        lines.append(f"| {arm} | {values['planned_cases']} | {values['schema_pass']} | {values['human_reviewed_cases']} | {values['human_usable_cases']} | {values['naturalness_median']} |")
    lines += ["", "## 全部状态与延迟", "",
              "未执行、失败、超时和未知均保留在计划分母。所有有观测的尝试时长包含失败等待；成功请求另列。延迟使用线性插值，单位毫秒。",
              "真实运行中，production_q8 计入原队列等待至响应回读；MLX 的 end_to_end 只计单条生成，一次模型加载另列 model_load。它们都不等于完整业务任务耗时，也不直接跨运行时判断快慢。", "",
              "| 组别 | 状态计数 | 尝试 n | 尝试 p50 / p95 / max | 完成 n | 完成 p50 / p95 / max | 无尝试时长的计划项 |",
              "| --- | --- | ---: | --- | ---: | --- | ---: |"]
    for arm, values in report["arms"].items():
        attempts, completed = values["attempt_end_to_end_latency"], values["completed_end_to_end_latency"]
        def timing(metric):
            return " / ".join("—" if metric[key] is None else f"{metric[key]:.2f}" for key in ("p50_ms", "p95_ms", "max_ms"))
        statuses = ", ".join(f"{key}: {value}" for key, value in sorted(values["statuses"].items()))
        lines.append(f"| {arm} | {statuses} | {attempts['n']} | {timing(attempts)} | {completed['n']} | {timing(completed)} | {values['planned_without_attempt_latency']} |")
    lines += ["", "## 同基座比较", "", "```json", json.dumps(report["mlx_comparison"], ensure_ascii=False, indent=2), "```",
              "", "## 结论边界", "", "自动报告不批准生产启用，不将JSON、有限语义校验或NLL当作自然度改善。",
              "生产Q8单独作为参照；MLX base与SFT的同输入配对结果用于评估adapter效果。",
              "", report["uncertainty_note"]]
    return "\n".join(lines)


def demo(output):
    """Full file pipeline with explicit fixture outputs and ZERO human labels."""
    from .evaluation import build_plan, make_result_record, import_results, blind_batch, feedback_report
    from .scenarios import calibration_specs, evaluation_cases
    from am_style.render import render_canonical
    from am_style.contracts import SpeechFrame
    out = _fresh(output)
    specs = calibration_specs()[:3]
    plan = build_plan(evaluation_cases(specs), _fixture_arms(), mode="fixture")
    records = []
    for i, case in enumerate(plan["cases"]):
        for arm in plan["arms"]:
            # One missing result exercises the denominator and unavailable arm path.
            if i == 2 and arm == "production_q8":
                continue
            text = render_canonical(SpeechFrame.freeze(case["frame"]))
            raw = canonical({"text": text})
            request_capture = {"fixture": True, "actual_messages": case["student_wire_messages"],
                               "generation_settings": plan["arms"][arm]["generation_settings"]}
            records.append(make_result_record(plan, case["case_id"], arm,
                status="completed", runner_source_sha256=file_hash(__file__),
                raw_output=raw, capture_kind="fixture",
                request_artifact=canonical(request_capture), response_artifact=canonical({"text": raw}),
                latency_ms={"end_to_end": float(100 + i)},
                request_binding={"wire_messages": {"path": ["actual_messages"]},
                                 "generation_settings": {"path": ["generation_settings"]}},
                response_binding={"path": ["text"]}))
    imported = import_results(plan, records)
    public, private = blind_batch(plan, imported)
    report = feedback_report(plan, imported)
    write_json(out / "plan.json", plan)
    write_jsonl(out / "fixture-records.jsonl", records)
    write_json(out / "results.json", imported)
    _blind_files(out, public, private)
    write_json(out / "report.json", report)
    _text(out / "report.md", report_markdown(report))
    return {"mode": "fixture", "planned_results": plan["expected_results"],
            "recorded_results": len(records), "missing_results": 1,
            "actual_model_calls": 0, "human_approvals": 0, "training_executed": False,
            "report": str(out / "report.json")}


def _parser():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    for command in ("prepare", "demo"):
        p = sub.add_parser(command); p.add_argument("--output", required=True)
    p = sub.add_parser("import-feedback")
    p.add_argument("--batch", required=True); p.add_argument("--feedback", required=True)
    p.add_argument("--raw-feedback"); p.add_argument("--output", required=True)
    p = sub.add_parser("revise")
    p.add_argument("--batch", required=True); p.add_argument("--replacements", required=True)
    p.add_argument("--version", required=True); p.add_argument("--output", required=True)
    p = sub.add_parser("snapshot")
    p.add_argument("--registry", required=True); p.add_argument("--imports", nargs="*", default=[])
    p.add_argument("--output", required=True)
    p.add_argument("--join-reviewed-registries", action="store_true",
                   help="Join already reviewed records only when their complete contexts match this registry")
    p = sub.add_parser("verify-snapshot"); p.add_argument("--snapshot", required=True)
    p = sub.add_parser("pilot-plan")
    for name in ("snapshot", "mechanics", "manifest", "output"):
        p.add_argument("--" + name, required=True)
    p.add_argument("--python"); p.add_argument("--run-dir")
    p.add_argument("--host-src"); p.add_argument("--training-root")
    p.add_argument("--holdout-cases", help="Private eval-freeze case bundle; supply all three holdout paths together")
    p.add_argument("--holdout-declaration", help="Human unused-for-tuning declaration bound to this case manifest")
    p.add_argument("--holdout-original", help="Retained original whose bytes match source_evidence_sha256")
    for command in ("pilot-preflight", "pilot-command", "pilot-execute"):
        p = sub.add_parser(command); p.add_argument("--plan", required=True)
        if command == "pilot-execute": p.add_argument("--execute", action="store_true")
    p = sub.add_parser("eval-freeze")
    p.add_argument("--cases", required=True); p.add_argument("--output", required=True)
    p = sub.add_parser("eval-arms")
    p.add_argument("--manifest", required=True); p.add_argument("--sft-run", required=True)
    p.add_argument("--output", required=True)
    p = sub.add_parser("queue-config")
    for name in ("manifest", "host-config", "queue-root", "output"):
        p.add_argument("--" + name, required=True)
    p.add_argument("--consumer-pid", type=int, required=True)
    p = sub.add_parser("eval-plan")
    p.add_argument("--cases", required=True); p.add_argument("--arms", required=True)
    p.add_argument("--mode", choices=("fixture", "real"), required=True)
    p.add_argument("--declaration"); p.add_argument("--snapshot"); p.add_argument("--output", required=True)
    for command in ("eval-qwen", "eval-mlx"):
        p = sub.add_parser(command); p.add_argument("--plan", required=True)
        p.add_argument("--output", required=True); p.add_argument("--case-ids", nargs="*")
        p.add_argument("--execute", action="store_true")
        if command == "eval-qwen": p.add_argument("--queue-config", required=True)
        else:
            p.add_argument("--manifest", required=True); p.add_argument("--arm", choices=("mlx_base", "mlx_sft"), required=True)
            p.add_argument("--sft-run"); p.add_argument("--worker-timeout", type=int, default=300)
    p = sub.add_parser("collect")
    p.add_argument("--plan", required=True); p.add_argument("--runs", nargs="+", required=True)
    p.add_argument("--output", required=True)
    p = sub.add_parser("eval-import")
    p.add_argument("--plan", required=True); p.add_argument("--records", nargs="*", default=[])
    p.add_argument("--output", required=True)
    p = sub.add_parser("blind")
    p.add_argument("--plan", required=True); p.add_argument("--results", required=True); p.add_argument("--output", required=True)
    p = sub.add_parser("report")
    p.add_argument("--plan", required=True); p.add_argument("--results", required=True); p.add_argument("--output", required=True)
    p.add_argument("--public"); p.add_argument("--private"); p.add_argument("--feedback", nargs="*")
    return parser


def main(argv=None):
    args = _parser().parse_args(argv)
    try:
        command = args.command
        if command == "prepare": result = prepare(args.output)
        elif command == "demo": result = demo(args.output)
        elif command == "import-feedback":
            from .dataset import import_feedback_file
            result = import_feedback_file(read_json(args.batch), args.feedback, raw_feedback_path=args.raw_feedback)
            write_json(args.output, result)
        elif command == "revise":
            from .dataset import revise_review_batch, feedback_template, markdown_review
            replacements = read_json(args.replacements)
            if (not isinstance(replacements, dict)
                    or any(not isinstance(key, str) or not isinstance(value, str)
                           for key, value in replacements.items())):
                raise R5Error("--replacements must be a JSON object mapping case IDs to complete replacement text")
            batch = revise_review_batch(read_json(args.batch), replacements, version=args.version)
            out = _fresh(args.output); write_json(out / "review-batch.json", batch)
            write_json(out / "feedback.pending.json", feedback_template(batch))
            _text(out / "review.md", markdown_review(batch)); result = {"batch_sha256": batch["batch_sha256"]}
        elif command == "snapshot":
            from .dataset import export_gold_snapshot
            result = export_gold_snapshot(read_json(args.registry), [read_json(p) for p in args.imports],
                baseline_train=BASELINE_TRAIN, baseline_dev=BASELINE_DEV, output_dir=args.output,
                join_reviewed_registries=args.join_reviewed_registries)
        elif command == "verify-snapshot":
            from .dataset import verify_gold_snapshot
            result = verify_gold_snapshot(args.snapshot)
        elif command == "pilot-plan":
            from .pilot import prepare_pilot
            if bool(args.host_src) != bool(args.training_root): raise R5Error("Supply both host source roots")
            roots = {"am_style": args.host_src, "training": args.training_root} if args.host_src else None
            result = prepare_pilot(args.snapshot, args.mechanics, args.manifest, args.output,
                python_executable=args.python, run_dir=args.run_dir, runtime_source_roots=roots,
                holdout_cases_path=args.holdout_cases, holdout_declaration_path=args.holdout_declaration,
                holdout_original_path=args.holdout_original)
        elif command == "pilot-preflight":
            from .pilot import preflight_pilot
            result = preflight_pilot(args.plan)
        elif command in {"pilot-command", "pilot-execute"}:
            from .pilot import execution_argv
            if command == "pilot-execute" and args.execute is not True: raise R5Error("Use --execute to start actual training")
            result = execution_argv(args.plan)
            if command == "pilot-execute":
                env = os.environ.copy(); env.update(result["env"])
                completed = subprocess.run(result["argv"], cwd=result["cwd"], env=env, shell=False, check=False)
                return completed.returncode
        elif command == "eval-freeze":
            from .evaluation import freeze_cases
            result = freeze_cases(read_json(args.cases))
            write_json(args.output, result)
        elif command == "eval-arms":
            from .arms import build_runtime_arms
            result = build_runtime_arms(args.manifest, args.sft_run)
            write_json(args.output, result)
        elif command == "queue-config":
            from .arms import build_queue_config
            result = build_queue_config(args.manifest, args.host_config, args.queue_root, args.consumer_pid)
            write_json(args.output, result)
        elif command == "eval-plan":
            from .evaluation import build_plan
            train_groups, dev_groups = [], []
            if args.snapshot:
                from .dataset import verify_gold_snapshot
                snapshot = verify_gold_snapshot(args.snapshot)
                parent = Path(args.snapshot).resolve().parent
                train_groups = sorted({r["provenance"]["group_id"] for r in read_jsonl(parent / snapshot["files"]["train"]["path"])})
                dev_groups = sorted({r["provenance"]["group_id"] for r in read_jsonl(parent / snapshot["files"]["dev"]["path"])})
            kwargs = {"mode": args.mode, "training_groups": train_groups, "development_groups": dev_groups,
                      "holdout_declaration": read_json(args.declaration) if args.declaration else None}
            if args.snapshot: kwargs["exclusion_snapshot_path"] = args.snapshot
            result = build_plan(read_json(args.cases), read_json(args.arms), **kwargs)
            write_json(args.output, result)
        elif command == "eval-qwen":
            from .runners import run_original_qwen
            result = run_original_qwen(read_json(args.plan), read_json(args.queue_config), args.output,
                                       selected=args.case_ids, execute=args.execute)
        elif command == "eval-mlx":
            from .runners import run_mlx_arm
            result = run_mlx_arm(read_json(args.plan), args.arm, args.manifest, args.output,
                sft_run=args.sft_run, selected=args.case_ids, execute=args.execute, worker_timeout_seconds=args.worker_timeout)
        elif command == "collect":
            from .collection import collect_run_records
            result = collect_run_records(read_json(args.plan), args.runs, args.output)
        elif command == "eval-import":
            from .evaluation import import_results
            result = import_results(read_json(args.plan), [r for path in args.records for r in read_jsonl(path)])
            write_json(args.output, result)
        elif command == "blind":
            from .evaluation import blind_batch
            public, private = blind_batch(read_json(args.plan), read_json(args.results))
            _blind_files(_fresh(args.output), public, private)
            result = {"public_sha256": public["sha256"], "tasks": len(public["tasks"])}
        elif command == "report":
            from .evaluation import feedback_report
            result = feedback_report(read_json(args.plan), read_json(args.results),
                public=read_json(args.public) if args.public else None,
                private=read_json(args.private) if args.private else None,
                feedback=[read_json(p) for p in args.feedback] if args.feedback else None)
            out = _fresh(args.output); write_json(out / "report.json", result)
            _text(out / "report.md", report_markdown(result))
        else: raise R5Error("Unknown command")
        # Keep terminal summaries concise; files hold the full sealed deliverable.
        if isinstance(result, dict) and args.command not in {"pilot-command", "pilot-preflight"}:
            summary = {k: v for k, v in result.items() if k not in {"cases", "records", "exports", "groups", "ratings", "rows", "arms", "plan", "preflight", "files"}}
            if len(canonical(summary)) > 3000: summary = {"status": "written", "schema": result.get("schema", result.get("schema_version")), "sha256": result.get("sha256")}
        else: summary = result
        print(json.dumps(summary, ensure_ascii=False, indent=2))
        # A saved failure report is useful evidence, but not a successful model
        # command. Callers must not continue an execution chain on exit zero.
        if command in {"eval-qwen", "eval-mlx"}:
            return 0 if result.get("status") == "completed" else 2
        if command == "pilot-preflight":
            # Metadata preparation remains useful without holdout or a resource
            # lease. Execution commands enforce their separate, fresh gate.
            return 0 if result.get("metadata_ready") is True else 2
        return 0
    except (R5Error, ValueError, KeyError, TypeError, OSError) as exc:
        print("R5 blocked: " + str(exc), file=sys.stderr)
        return 2
