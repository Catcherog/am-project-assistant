"""Synthetic business fixtures; real model quality requires separate human review."""
from __future__ import annotations
import argparse
import importlib
import json
from pathlib import Path
import tempfile
import time
from .engine import ConversationAgent,RuntimeConfig
from .knowledge import KnowledgeStore
from .model import CoordinatorModel
from .store import ConversationStore
from .tools import build_registry
from .types import AccessContext


class FixtureProjects:
    def __init__(self):
        self.reads = []
        self.rows = [
            {"id": "p17", "title": "合成项目17", "aliases": ["17", "测试项目"],
             "facts": {"测试": "首批测试完成，另两批未开始", "报告": "预计2026-11-02提交，未批准为承诺", "计划": "评审计划2026-11-04，变更尚未批准", "责任人": "合成工程师甲"},
             "observed_at": "2026-10-30T10:00:00+08:00", "source": "fixture://projects/p17"},
            {"id": "p20", "title": "合成项目20", "aliases": ["20", "测试项目"],
             "facts": {"测试": "样件未到，测试未开始", "计划": "测试结束日期未确认", "责任人": "合成工程师乙"},
             "observed_at": "2026-10-30T10:00:00+08:00", "source": "fixture://projects/p20"},
        ]

    def list_projects(self, ctx):
        return [dict(row) for row in self.rows if ctx.can_access_project(row["id"])]

    def read_project(self, ctx, project_id):
        self.reads.append(project_id)
        return next(dict(row) for row in self.list_projects(ctx) if row["id"] == project_id)


def run_cases(model_factory, cases: list[dict], *, config=None) -> dict:
    results = []
    for case in cases:
        with tempfile.TemporaryDirectory(prefix="am-evaluation-") as directory:
            store = ConversationStore(Path(directory) / "conversation.db")
            knowledge = KnowledgeStore(Path(directory) / "knowledge.db", owner_ids={("fixture", "fixture-app"): {"owner"}})
            projects = FixtureProjects()
            role = case.get("role", "owner")
            ctx = AccessContext("fixture", "fixture-app", role, "fixture-dm", allowed_project_ids=frozenset(case.get("projects", ["p17", "p20"])), is_owner=role == "owner")
            metrics = []
            agent = ConversationAgent(model_factory(), store, build_registry(store, knowledge, projects), knowledge=knowledge, metrics=metrics.append,config=config)
            replies, error = [], None
            started = time.monotonic()
            try:
                for index, message in enumerate(case["messages"]):
                    replies.append(agent.handle(ctx, case["id"] + "-" + str(index), message).to_dict())
            except Exception as exc:
                error = type(exc).__name__ + ":" + str(exc)[:120]
            tasks = store.list_tasks(ctx, include_closed=True)
            checks = {"no_runtime_error": error is None, "no_external_actions": store.list_actions(ctx, active_only=False) == [],
                      "only_authorized_project_reads": all(ctx.can_access_project(p) for p in projects.reads)}
            expected = case.get("checks", {})
            if "read_projects" in expected:
                checks["requested_sources_read"] = set(expected["read_projects"]).issubset(projects.reads)
            if "task_projects" in expected:
                checks["work_notes_in_expected_projects"] = set(expected["task_projects"]) == {t["project_id"] for t in tasks}
            if "task_count" in expected:
                checks["work_note_count"] = expected["task_count"] == len(tasks)
            if "last_status" in expected:
                checks["last_status"] = bool(replies) and replies[-1]["status"] == expected["last_status"]
            results.append({"id": case["id"], "title": case["title"], "automated_checks": checks,
                "automated_checks_passed": all(checks.values()), "manual_review_required": True,
                "manual_rubric": case["manual_rubric"], "replies": replies, "tasks": tasks,
                "project_reads": projects.reads, "spans": metrics, "server_elapsed_seconds": time.monotonic() - started, "error": error})
            knowledge.close()
            store.close()
    return {"mode": "synthetic_business_fixtures", "external_business_calls": 0, "manual_review_completed": False,
            "case_count": len(results), "automated_pass_count": sum(row["automated_checks_passed"] for row in results), "results": results}


def main(argv=None):
    parser = argparse.ArgumentParser(description="通过原Qwen协调器运行合成业务场景，不调用真实飞书业务端口。")
    parser.add_argument("--coordinator-factory", required=True, help="受信本机代码module:callable，返回既有共享协调器")
    parser.add_argument("--cases", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--limit", type=int, default=5)
    args = parser.parse_args(argv)
    if not 1 <= args.limit <= 100:
        parser.error("limit must be 1..100")
    module_name, separator, name = args.coordinator_factory.partition(":")
    if not separator or not name.isidentifier():
        parser.error("factory must be module:callable")
    coordinator = getattr(importlib.import_module(module_name), name)()
    cases = json.loads(Path(args.cases).read_text(encoding="utf-8"))
    report = run_cases(lambda: CoordinatorModel(coordinator), cases[:args.limit])
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"cases": report["case_count"], "automated_pass_count": report["automated_pass_count"], "manual_review_completed": False}, ensure_ascii=False))
    return 0 if report["automated_pass_count"] == report["case_count"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
