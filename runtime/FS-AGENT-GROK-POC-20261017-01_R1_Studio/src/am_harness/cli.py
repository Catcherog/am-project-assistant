"""Local diagnostics and evidence analysis; no AM consumer or sender is started."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sqlite3


def main(argv=None) -> int:
    parser=argparse.ArgumentParser(description=__doc__)
    sub=parser.add_subparsers(dest="command",required=True)
    sub.add_parser("demo",help="Run synthetic correction/cancel/outbox example")
    status=sub.add_parser("status",help="Read task/lane metadata only")
    status.add_argument("--database",type=Path,required=True)
    evaluate=sub.add_parser("evaluate",help="Analyze normalized JSONL benchmark records")
    evaluate.add_argument("records",type=Path)
    evaluate.add_argument("--minimum-pairs",type=int,default=20)
    args=parser.parse_args(argv)
    if args.command=="demo":
        from .demo import run_demo
        result=run_demo()
    elif args.command=="status":
        path=args.database.resolve()
        if not path.is_file(): raise SystemExit("DATABASE_NOT_FOUND")
        # Existing-file URI, read-only; inspecting status cannot create schema,
        # clear a pause, reconcile a lease, or expose task text.
        with sqlite3.connect(path.as_uri()+"?mode=ro",uri=True) as conn:
            conn.row_factory=sqlite3.Row
            lane=conn.execute("SELECT state,generation,expires_at,reason FROM am_harness_lane WHERE id=1").fetchone()
            states={r[0]:r[1] for r in conn.execute("SELECT state,COUNT(*) FROM am_harness_runs GROUP BY state")}
            result={"lane":dict(lane) if lane else None,"runs_by_state":states}
    else:
        from .evaluation import analyze
        records=[json.loads(line) for line in args.records.read_text(encoding="utf-8").splitlines() if line.strip()]
        result=analyze(records,minimum_pairs=args.minimum_pairs)
    print(json.dumps(result,ensure_ascii=False,indent=2))
    return 0
