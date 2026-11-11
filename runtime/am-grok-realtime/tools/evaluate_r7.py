#!/usr/bin/env python3
"""Evaluate captured trials or call an explicitly configured native adapter.

No adapter means not_run. It never substitutes expected answers for execution.
The adapter receives input/setup/source fixtures, never the grading oracle.
"""
from __future__ import annotations

import argparse
from copy import deepcopy
import importlib
import json
import math
import os
from pathlib import Path
import sys
import time
import uuid

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from am_gateway.assistant.quality import (LAYERS, SCHEMA_VERSION, QualityInputError,
    compare_quality, evaluate_quality, load_records, load_suite, regrade_r6)


def append_record(path, record):
    """One durable append per attempt, before the next trial starts."""
    path = Path(path); path.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
    with os.fdopen(fd, "a", encoding="utf-8") as handle:
        handle.write(json.dumps(record, ensure_ascii=False, allow_nan=False) + "\n")
        handle.flush(); os.fsync(handle.fileno())


def run_adapter(suite, callback, layer, *, repetitions=3, record_output=None, request_budget_s=None):
    records = []
    run_id = uuid.uuid4().hex
    for case in suite["cases"]:
      for repetition in range(1, repetitions + 1):
        # Control setup is required to reproduce faults/identity. Expectations
        # remain solely at the evaluator side; neither worker nor proposer sees
        # required text, expected facts, or semantic reviewer instructions.
        execution_case = {k: deepcopy(v) for k, v in case.items()
                          if k not in {"oracle", "expected", "expected_answer"}}
        attempt_id = f"{run_id}-{repetition:03d}"
        execution_case["evaluation_attempt_id"] = attempt_id
        execution_case["evaluation_run_id"] = run_id
        execution_case["request_budget_s"] = request_budget_s
        started = time.monotonic()
        try:
            result = callback(execution_case, deepcopy(suite.get("source_fixture", {})), layer=layer)
            if not isinstance(result, dict):
                raise QualityInputError("native adapter must return an observed record")
            result.setdefault("attempt_id", attempt_id)
        except Exception as exc:
            # Native errors can contain a credential or private URL. Retain
            # only their type; detailed private traces stay at the native port.
            result = {"schema_version": SCHEMA_VERSION, "case_id": case["case_id"],
                "attempt_id": attempt_id, "channel": case["channel"], "intent": case["intent"],
                "source_version": case["source_version"], "layer": layer,
                "outcome": {"status": "timeout" if isinstance(exc, TimeoutError) else "error", "reason": type(exc).__name__},
                "execution_provenance": {"adapter_invoked": True}}
        result["runner_observation"] = {"run_id": run_id, "repetition": repetition,
            "adapter_elapsed_s": time.monotonic() - started, "requested_budget_s": request_budget_s,
            "elapsed_is_not_user_visible_latency": True}
        if record_output is not None:
            append_record(record_output, result)
        records.append(result)
    return records


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cases", default="evaluation/r7_cases.json")
    parser.add_argument("--records", action="append", default=[], help="Captured JSONL; repeat to retain all attempts")
    parser.add_argument("--baseline", help="Captured baseline JSONL for same-condition comparison")
    parser.add_argument("--layer", action="append", choices=LAYERS, help="Evidence layer; default all five")
    parser.add_argument("--adapter", help="Explicit native Python module:callable; requires one --layer")
    parser.add_argument("--repetitions", type=int, default=3, help="Trials per case for --adapter (default 3)")
    parser.add_argument("--record-output", help="Append/fsync raw attempt JSONL; defaults beside --output for adapter runs")
    parser.add_argument("--case-id", "--case", action="append", default=[], help="Select exact case IDs; repeat as needed")
    parser.add_argument("--family", action="append", default=[], help="Select scenario families; repeat as needed")
    parser.add_argument("--request-budget-s", type=float, help="Pass request budget to the native adapter; it owns timeout/cancellation")
    parser.add_argument("--r6-cases", help="Optional immutable R6 suite for versioned lexical diagnostic")
    parser.add_argument("--r6-records", help="Immutable R6 JSONL for the diagnostic")
    parser.add_argument("--output", required=True)
    args = parser.parse_args(argv)
    try:
        suite = load_suite(args.cases)
        original_case_count = len(suite["cases"])
        if args.case_id or args.family:
            suite["cases"] = [c for c in suite["cases"] if (not args.case_id or c["case_id"] in args.case_id)
                              and (not args.family or c["family"] in args.family)]
            if not suite["cases"]:
                raise QualityInputError("case/family filter matched no planned cases")
        if args.repetitions < 1:
            raise QualityInputError("repetitions must be positive")
        if args.request_budget_s is not None and (not math.isfinite(args.request_budget_s) or args.request_budget_s <= 0):
            raise QualityInputError("request budget must be positive and finite")
        layers = tuple(args.layer or LAYERS)
        records = [record for path in args.records for record in load_records(path)]
        if args.adapter:
            if len(layers) != 1 or ":" not in args.adapter:
                raise QualityInputError("--adapter requires module:callable and exactly one --layer")
            module, name = args.adapter.split(":", 1)
            callback = getattr(importlib.import_module(module), name)
            raw_path = args.record_output or str(Path(args.output).with_suffix(".records.jsonl"))
            records.extend(run_adapter(suite, callback, layers[0], repetitions=args.repetitions,
                                       record_output=raw_path, request_budget_s=args.request_budget_s))
        report = (compare_quality(suite, load_records(args.baseline), records, layers=layers) if args.baseline
                  else evaluate_quality(suite, records, layers=layers))
        if bool(args.r6_cases) != bool(args.r6_records):
            raise QualityInputError("both --r6-cases and --r6-records are required")
        if args.r6_cases:
            from am_gateway.assistant.evaluation import load_cases
            report["r6_versioned_lexical_diagnostic"] = regrade_r6(load_cases(args.r6_cases), load_records(args.r6_records))
        report["selection"] = {"original_case_count": original_case_count, "selected_case_count": len(suite["cases"]),
                               "case_ids": args.case_id, "families": args.family}
        output = Path(args.output); output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(report, ensure_ascii=False, indent=2, allow_nan=False) + "\n", encoding="utf-8")
        evaluated = report.get("candidate", report)
        print(json.dumps({"output": str(output), "ci_pass": evaluated["ci_pass"],
                          "layers": evaluated["layers"]}, ensure_ascii=False))
        return 0 if evaluated["ci_pass"] else 1
    except (OSError, ValueError, ImportError, AttributeError, TypeError) as exc:
        print("Evaluation input or adapter configuration failed: " + type(exc).__name__, file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
