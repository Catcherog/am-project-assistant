"""Offline demo and administrator-controlled local knowledge import commands."""
from __future__ import annotations

import argparse
from copy import deepcopy
from datetime import datetime
import json
from pathlib import Path
import re
import sqlite3
import tempfile
from typing import Any

from .engine import ConversationAgent, RuntimeConfig
from .knowledge import KnowledgeStore
from .learning import KnowledgeImporter
from .model import ScriptedModel
from .store import ConversationStore
from .tools import Tool, build_registry, obj, string
from .types import AccessContext, ConversationError, ModelTurn, ToolCall


DEMO_NOTICE = "合成协议演示，未调用Qwen或飞书"
_CONFIG_REQUIRED = frozenset({"tenant_id", "app_id", "actor_id", "chat_id", "owner_ids",
                              "allowed_project_ids", "all_projects", "database"})
_CONFIG_ALLOWED = _CONFIG_REQUIRED | {"thread_id"}
_PLACEHOLDER = re.compile(
    r"(?i)(?:^|[^a-z0-9])(?:your|replace|placeholder|change[_ -]?me|todo|tbd)(?:[^a-z0-9]|$)"
    r"|<[^>]+>|\$\{[^}]+\}|请.{0,12}(?:填|替换)|待(?:填|配)|你的.{0,12}(?:id|路径)"
)


class _SyntheticProjects:
    def __init__(self):
        self.rows = {
            "synthetic-alpha": {"id": "synthetic-alpha", "title": "合成项目 Alpha",
                                "aliases": ["Alpha"], "status": "准备验证",
                                "observed_at": "2026-10-30T09:00:00+08:00"},
            "synthetic-beta": {"id": "synthetic-beta", "title": "合成项目 Beta",
                               "aliases": ["Beta"], "status": "等待材料",
                               "observed_at": "2026-10-30T09:00:00+08:00"},
        }
        self.reads: list[dict[str, Any]] = []
        self.list_calls = 0

    def list_projects(self, ctx):
        self.list_calls += 1
        return [deepcopy(row) for key, row in self.rows.items() if ctx.can_access_project(key)]

    def read_project(self, ctx, project_id):
        if not ctx.can_access_project(project_id) or project_id not in self.rows:
            raise ConversationError("PROJECT_FORBIDDEN")
        result = deepcopy(self.rows[project_id])
        self.reads.append(result)
        return result


def _calls(*requests) -> ModelTurn:
    return ModelTurn(tool_calls=tuple(ToolCall(f"synthetic-call-{index}", name, args)
                                     for index, (name, args) in enumerate(requests)))


def run_demo() -> dict[str, Any]:
    """Exercise real engine/store paths with explicitly synthetic input ports.

    Model utterances are scripted protocol fixtures, not evidence that Qwen can
    produce them.  Checks below are derived from actual port calls and persisted
    records.  Presentation acknowledgements represent a simulated host delivery.
    """
    ctx = AccessContext("synthetic-tenant", "synthetic-app", "synthetic-owner",
                        "synthetic-private-chat", "synthetic-thread",
                        frozenset({"synthetic-alpha", "synthetic-beta"}), is_owner=True)
    owners = {(ctx.tenant_id, ctx.app_id): {ctx.actor_id}}
    timestamp = datetime.fromisoformat("2026-10-30T09:00:00+08:00").timestamp()
    clock = lambda: timestamp
    projects = _SyntheticProjects()
    executions: list[dict[str, Any]] = []
    steps: list[dict[str, Any]] = []
    model_calls = 0

    def attach_external(registry):
        def preview(_ctx, args):
            return ("【合成端口预览】\n项目：" + args["project_id"] + "\n接收方：" + args["recipient"] +
                    "\n内容：" + args["text"] + "\n仅写入演示内存列表，不发送飞书消息。")

        def execute(_ctx, args, request_id):
            receipt = {"action_id": request_id, "arguments": deepcopy(args),
                       "receipt_id": "synthetic-receipt-" + request_id}
            executions.append(receipt)
            return {"status": "succeeded", "verified": executions[-1] == receipt,
                    "receipt_id": receipt["receipt_id"],
                    "message": "合成端口记录了一次动作，未发送任何飞书消息。"}

        registry.register(Tool(
            "synthetic_notify", "演示专用通知端口，只向内存列表写入合成回执。",
            obj({"project_id": string(256), "recipient": string(100), "text": string(1000)},
                ("project_id", "recipient", "text")), execute, "external", True, preview,
        ))

    with tempfile.TemporaryDirectory(prefix="am-conversation-demo-") as directory:
        conversation_path = Path(directory) / "conversation.sqlite3"
        knowledge_path = Path(directory) / "knowledge.sqlite3"
        store = ConversationStore(conversation_path, clock=clock)
        knowledge = KnowledgeStore(knowledge_path, owner_ids=owners, clock=clock)
        try:
            registry = build_registry(store, knowledge, projects)
            attach_external(registry)

            def step(name, user_text, turns, *, presentation=False, event_id=None):
                nonlocal model_calls
                model = ScriptedModel(turns)
                agent = ConversationAgent(model, store, registry, knowledge=knowledge,
                                          config=RuntimeConfig(allow_external_actions=True), clock=clock)
                event_id = event_id or "synthetic-event-" + str(len(steps) + 1)
                reply = agent.handle(ctx, event_id, user_text)
                entry = {"name": name, "user": user_text, "reply": reply.to_dict(),
                         "model_calls": len(model.calls)}
                steps.append(entry)
                model_calls += len(model.calls)
                if presentation:
                    # The demo's recorded transcript is the simulated delivery
                    # sink.  Production must wait for the real Feishu receipt.
                    agent.acknowledge_presentation(ctx, reply)
                    entry["presentation"] = "synthetic_delivery_acknowledged"
                return reply

            step("项目查询", "看一下 Alpha 的当前进度", [
                _calls(("list_projects", {"query": "Alpha"})),
                _calls(("read_project", {"project_id": "synthetic-alpha"})),
                ModelTurn(text="合成项目 Alpha 当前处于准备验证阶段，资料时间为 2026-10-30 09:00。"),
            ])
            step("两个项目分别记工作笔记", "Alpha 周一上午核查测试证据，Beta 周二下午确认到料。", [
                _calls(
                    ("save_work_note", {"project_id": "synthetic-alpha", "title": "核查合成测试证据",
                                        "due_at": "2026-11-02T10:00:00+08:00"}),
                    ("save_work_note", {"project_id": "synthetic-beta", "title": "确认合成材料到料",
                                        "due_at": "2026-11-03T14:00:00+08:00"}),
                ),
                ModelTurn(text="两项工作笔记分别保存在 Alpha 和 Beta 下，正式项目计划未变动。"),
            ])
            notes_before = store.list_tasks(ctx, include_closed=True)
            store.close()
            knowledge.close()
            store = ConversationStore(conversation_path, clock=clock)
            knowledge = KnowledgeStore(knowledge_path, owner_ids=owners, clock=clock)
            registry = build_registry(store, knowledge, projects)
            attach_external(registry)
            step("关闭并重建存储与Agent后恢复", "刚才记下的两件事还在吗？", [
                _calls(("list_work_notes", {})),
                ModelTurn(text="工作笔记仍保留 Alpha 的证据核查和 Beta 的到料确认，两项日期分别保存。"),
            ])
            notes_after = store.list_tasks(ctx, include_closed=True)

            original_args = {"project_id": "synthetic-alpha", "recipient": "合成接收方甲",
                             "text": "请周一上午补充合成测试记录。"}
            draft = step("合成外部动作预览", "准备向合成接收方甲发一条提醒。", [
                _calls(("synthetic_notify", original_args)),
            ], presentation=True)
            if len(draft.pending_actions) != 1:
                raise ConversationError("DEMO_DRAFT_NOT_CREATED")
            action_id = draft.pending_actions[0]
            original_action = store.get_action(ctx, action_id)
            count_before_confirmation = len(executions)
            revised_args = {**original_args, "text": "请周二下午补充合成测试记录。"}
            step("修改并重新预览", "把时间改成周二下午。", [
                _calls(("revise_pending_action", {"action_id": action_id,
                                                  "expected_version": original_action["version"],
                                                  "arguments": revised_args})),
            ], presentation=True)
            revised_action = store.get_action(ctx, action_id)
            confirmed = step("确认执行合成端口", "确认执行", [], event_id="synthetic-confirm")
            replayed = step("同一事件重放", "确认执行", [], event_id="synthetic-confirm")
            executed_action = store.get_action(ctx, action_id)

            cancelled_draft = step("另一动作预览", "再准备一条 Beta 的合成提醒。", [
                _calls(("synthetic_notify", {"project_id": "synthetic-beta", "recipient": "合成接收方乙",
                                             "text": "请确认合成材料状态。"})),
            ], presentation=True)
            if len(cancelled_draft.pending_actions) != 1:
                raise ConversationError("DEMO_CANCEL_DRAFT_NOT_CREATED")
            cancelled_id = cancelled_draft.pending_actions[0]
            step("取消未执行动作", "取消", [])
            cancelled_action = store.get_action(ctx, cancelled_id)

            notes_by_project = {row["project_id"]: row for row in notes_after}
            checks = {
                "project_query_used_port": projects.list_calls == 1 and len(projects.reads) == 1
                                            and projects.reads[0]["id"] == "synthetic-alpha",
                "two_project_notes_persisted": len(notes_after) == 2 and
                                               {r["id"]: r for r in notes_before} == {r["id"]: r for r in notes_after},
                "project_dates_kept_separate": notes_by_project.get("synthetic-alpha", {}).get("due_at") == "2026-11-02T10:00:00+08:00"
                                               and notes_by_project.get("synthetic-beta", {}).get("due_at") == "2026-11-03T14:00:00+08:00",
                "preview_did_not_execute": count_before_confirmation == 0,
                "revised_version_presented": revised_action["version"] == original_action["version"] + 1
                                              and revised_action["presented_version"] == revised_action["version"],
                "confirmed_exact_revised_arguments": confirmed.status == "execution_succeeded" and
                                                     executed_action["status"] == "succeeded" and len(executions) == 1
                                                     and executions[0]["arguments"] == revised_args,
                "replay_did_not_execute_again": replayed.replayed and len(executions) == 1,
                "cancelled_action_not_executed": cancelled_action["status"] == "cancelled" and
                                                  all(row["action_id"] != cancelled_id for row in executions),
            }
            return {"ok": all(checks.values()), "notice": DEMO_NOTICE,
                    "model": "ScriptedModel", "scope": "temporary_synthetic_data",
                    "checks": checks, "model_calls": model_calls, "steps": steps,
                    "observed": {"project_reads": projects.reads, "notes_before_restart": notes_before,
                                 "notes_after_restart": notes_after, "executions": executions,
                                 "executed_action": executed_action, "cancelled_action": cancelled_action}}
        finally:
            store.close()
            knowledge.close()


def _config_pairs(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ConversationError("CONFIG_DUPLICATE_FIELD")
        result[key] = value
    return result


def _config_text(value, *, maximum=256, empty=False):
    if (not isinstance(value, str) or (not value and not empty) or value != value.strip() or
            len(value) > maximum or any(ord(c) < 32 for c in value)):
        raise ConversationError("CONFIG_INVALID_VALUE")
    if _PLACEHOLDER.search(value) or value.casefold() in {"owner_id", "tenant_id", "app_id", "actor_id", "chat_id", "project_id"}:
        raise ConversationError("CONFIG_PLACEHOLDER_VALUE")
    try:
        value.encode("utf-8")
    except UnicodeEncodeError as exc:
        raise ConversationError("CONFIG_INVALID_VALUE") from exc
    return value


def load_import_config(path: str | Path):
    """Read a local administrator config; never merge identity from JSONL data."""
    path = Path(path).expanduser().resolve()
    try:
        with path.open("rb") as stream:
            raw = stream.read(65537)
        if len(raw) > 65536:
            raise ConversationError("CONFIG_TOO_LARGE")
        data = json.loads(raw.decode("utf-8-sig"), object_pairs_hook=_config_pairs,
                          parse_constant=lambda _value: (_ for _ in ()).throw(ConversationError("CONFIG_INVALID_JSON")))
    except OSError as exc:
        raise ConversationError("CONFIG_UNAVAILABLE") from exc
    except (ValueError, UnicodeDecodeError, RecursionError) as exc:
        raise ConversationError("CONFIG_INVALID_JSON") from exc
    if not isinstance(data, dict) or not _CONFIG_REQUIRED.issubset(data) or set(data) - _CONFIG_ALLOWED:
        raise ConversationError("CONFIG_INVALID_FIELDS")
    identities = {field: _config_text(data[field]) for field in ("tenant_id", "app_id", "actor_id", "chat_id")}
    if type(data["all_projects"]) is not bool:
        raise ConversationError("CONFIG_INVALID_VALUE")
    for name in ("owner_ids", "allowed_project_ids"):
        if not isinstance(data[name], list) or len(data[name]) > 1000:
            raise ConversationError("CONFIG_INVALID_VALUE")
        for value in data[name]:
            _config_text(value)
    owners = frozenset(data["owner_ids"])
    database = _config_text(data["database"], maximum=4096)
    if database == ":memory:":
        raise ConversationError("CONFIG_DATABASE_MUST_BE_FILE")
    database_path = Path(database).expanduser()
    if not database_path.is_absolute():
        database_path = path.parent / database_path
    database_path = database_path.resolve()
    ctx = AccessContext(**identities, thread_id=_config_text(data.get("thread_id", ""), empty=True),
                        allowed_project_ids=frozenset(data["allowed_project_ids"]),
                        all_projects=data["all_projects"], is_owner=identities["actor_id"] in owners)
    return ctx, {(ctx.tenant_id, ctx.app_id): owners}, database_path


def import_knowledge(config_path, file_path, *, namespace="default", approve=False, dry_run=False):
    ctx, owners, database = load_import_config(config_path)
    store = None
    try:
        if dry_run:
            store = KnowledgeStore(":memory:", owner_ids=owners)
            if database.exists():
                if not database.is_file():
                    raise ConversationError("IMPORT_DATABASE_UNAVAILABLE")
                # Never open the source through KnowledgeStore: its constructor
                # performs schema initialization.  This connection is read-only.
                source = sqlite3.connect(database.as_uri() + "?mode=ro", uri=True, timeout=5.0)
                try:
                    present = source.execute(
                        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='knowledge_records'"
                    ).fetchone()
                    if present is None:
                        raise ConversationError("IMPORT_DATABASE_SCHEMA_MISSING")
                    source.backup(store._db)
                finally:
                    source.close()
        else:
            store = KnowledgeStore(database, owner_ids=owners)
        report = KnowledgeImporter(store).import_file(ctx, file_path, namespace=namespace,
                                                     approve=approve, dry_run=dry_run)
        report["command"] = "import-knowledge"
        report["database_mode"] = "memory_dry_run" if dry_run else "persistent"
        return report
    except sqlite3.Error as exc:
        raise ConversationError("IMPORT_DATABASE_UNAVAILABLE") from exc
    finally:
        if store is not None:
            store.close()


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(prog="python -m am_conversation",
                                     description="AM 本地合成演示与知识导入；不连接 Qwen 或飞书。")
    commands = parser.add_subparsers(dest="command", required=True)
    demo = commands.add_parser("demo", help="运行真实引擎与存储的合成协议演示")
    demo.add_argument("--json", action="store_true", help="输出结构化演示结果")
    importer = commands.add_parser("import-knowledge", help="从本地 JSONL 导入知识候选")
    importer.add_argument("--config", required=True, help="本机管理员提供的身份与数据库配置")
    importer.add_argument("--file", required=True, help="UTF-8 JSONL 知识文件")
    importer.add_argument("--namespace", default="default")
    importer.add_argument("--approve", action="store_true", help="由配置内 owner 明确批准本次内容")
    importer.add_argument("--dry-run", action="store_true", help="只读原库，使用内存副本模拟导入")
    args = parser.parse_args(argv)
    try:
        if args.command == "demo":
            report = run_demo()
            if args.json:
                print(json.dumps(report, ensure_ascii=False, indent=2))
            else:
                print(DEMO_NOTICE)
                print("以下对话由 ScriptedModel 驱动，验证的是程序协议和持久化行为。\n")
                for step in report["steps"]:
                    print(f"[{step['name']}]\n用户：{step['user']}\n助理：{step['reply']['text']}\n")
                print("存储与端口实测检查：")
                for name, passed in report["checks"].items():
                    print(f"- {name}: {'通过' if passed else '未通过'}")
        else:
            report = import_knowledge(args.config, args.file, namespace=args.namespace,
                                      approve=args.approve, dry_run=args.dry_run)
            print(json.dumps(report, ensure_ascii=False, indent=2))
        return 0 if report["ok"] else 1
    except ConversationError as exc:
        print(json.dumps({"ok": False, "command": args.command, "error": str(exc)}, ensure_ascii=False))
        return 2
    except (OSError, ValueError, TypeError):
        print(json.dumps({"ok": False, "command": args.command, "error": "LOCAL_COMMAND_FAILED"}, ensure_ascii=False))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
