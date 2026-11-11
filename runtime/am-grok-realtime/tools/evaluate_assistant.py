#!/usr/bin/env python3
"""Evaluate R6 JSONL evidence; no network, model calls, or business writes."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from am_gateway.assistant.evaluation import (  # noqa: E402
    EvaluationInputError, LAYERS, compare_runs, evaluate_run, load_cases, load_jsonl,
)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cases", required=True, help="Versioned JSON or JSONL cases with oracle")
    parser.add_argument("--candidate", required=True, help="Candidate JSONL evidence")
    parser.add_argument("--baseline", help="Optional baseline JSONL evidence for paired comparison")
    parser.add_argument("--expected-layer", choices=sorted(LAYERS), help="Fail records from a different evidence layer")
    parser.add_argument("--output", help="Write complete JSON report here instead of stdout")
    args = parser.parse_args(argv)
    try:
        cases = load_cases(args.cases)
        candidate = load_jsonl(args.candidate)
        if args.baseline:
            result = compare_runs(cases, load_jsonl(args.baseline), candidate, expected_layer=args.expected_layer)
        else:
            result = evaluate_run(cases, candidate, expected_layer=args.expected_layer)
        rendered = json.dumps(result, ensure_ascii=False, indent=2, allow_nan=False) + "\n"
        if args.output:
            Path(args.output).write_text(rendered, encoding="utf-8")
        else:
            sys.stdout.write(rendered)
        return 0 if result["ci_pass"] else 1
    except (OSError, UnicodeError, EvaluationInputError, TypeError, ValueError) as exc:
        sys.stderr.write("evaluation input error: " + str(exc) + "\n")
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
