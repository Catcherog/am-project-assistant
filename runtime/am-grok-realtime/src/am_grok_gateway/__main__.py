from __future__ import annotations

import argparse
import json
from pathlib import Path

from .metrics import read_records, summarize


def main():
    parser = argparse.ArgumentParser(description="AM Grok local, read-only latency report")
    sub = parser.add_subparsers(dest="command", required=True)
    report = sub.add_parser("report")
    report.add_argument("--db", required=True)
    report.add_argument("--visibility")
    report.add_argument("--minimum-samples", type=int, default=100)
    report.add_argument("--output")
    args = parser.parse_args()
    observations = json.loads(Path(args.visibility).read_text()) if args.visibility else []
    result = summarize(read_records(args.db), visibility=observations, minimum_samples=args.minimum_samples)
    text = json.dumps(result, ensure_ascii=False, indent=2, allow_nan=False)
    if args.output:
        path = Path(args.output)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text + "\n")
    else:
        print(text)


if __name__ == "__main__":
    main()
