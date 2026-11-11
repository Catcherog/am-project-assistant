"""Offline CLI. Reads trusted exported DTOs, creates local previews, never uses credentials."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from .catalog import TemplateCatalog
from .engine import DocumentEngine
from .evidence import scoped_snapshot
from .models import (AccessContext, DraftRequest, canonical, profile_from_dict,
                     snapshot_from_dict, strict_json, to_dict)
from .rendering import render_html, render_markdown, build_report
from .storage import SQLiteRunStore


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="AM 文档生成离线演示/受控JSON导入")
    sub = parser.add_subparsers(dest="command", required=True)
    generate = sub.add_parser("generate", help="从导出的模板与事实包生成本地草稿")
    generate.add_argument("--bundle", required=True, type=Path)
    generate.add_argument("--out", required=True, type=Path)
    generate.add_argument("--db", type=Path, help="持久化幂等记录，默认仅本次内存")
    generate.add_argument("--type", choices=["charter", "change", "closure"], help="可选：仅运行一个演示文档类型")
    args = parser.parse_args(argv)
    raw = strict_json(args.bundle.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError("bundle必须为JSON对象")
    profiles = [profile_from_dict(p) for p in raw["profiles"]]
    access_data = dict(raw["access"])
    access_data["scopes"] = access_data.get("scopes", ["document:draft"])
    access = AccessContext(**access_data)
    snapshot = snapshot_from_dict(raw["snapshot"])
    catalog = TemplateCatalog(profiles)
    args.out.mkdir(parents=True, exist_ok=True)
    if args.db:
        args.db.parent.mkdir(parents=True, exist_ok=True)
    summary = []
    with SQLiteRunStore(args.db or ":memory:") as store:
        engine = DocumentEngine(catalog, store)
        for item in raw["requests"]:
            request = DraftRequest(**item)
            if args.type and request.doc_type != args.type:
                continue
            doc = engine.generate(request, access, snapshot)
            profile = next(p for p in profiles if p.content_hash == doc.template_hash)
            # Stable hashes form filesystem names; incoming project/request identifiers never become paths.
            stem = doc.document_id
            (args.out / f"{stem}.json").write_text(json.dumps(to_dict(doc), ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
            (args.out / f"{stem}.md").write_text(render_markdown(doc, profile), encoding="utf-8")
            (args.out / f"{stem}.html").write_text(render_html(doc, profile), encoding="utf-8")
            (args.out / f"{stem}.report.json").write_text(json.dumps(build_report(doc, profile, scoped_snapshot(snapshot, request, access)), ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
            summary.append({"doc_type": doc.doc_type, "document_id": doc.document_id, "state": doc.state,
                            "errors": sum(i.severity == "error" for i in doc.issues), "questions": list(doc.questions)})
    (args.out / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (ValueError, PermissionError, RuntimeError) as exc:
        print(f"未生成可用结果: {type(exc).__name__}: {exc}", file=sys.stderr)
        raise SystemExit(2)
