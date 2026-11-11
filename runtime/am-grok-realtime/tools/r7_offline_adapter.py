"""Controlled offline execution of actual query modules with synthetic ports.

No model, live source, websocket, sender or business writer is available here.
It does not read an oracle, manufacture an expected answer, or mark unbound
complex capabilities as executed. Native adapters implement the same callable.
"""
from __future__ import annotations

from concurrent.futures import Future
from copy import deepcopy
from datetime import datetime
import hashlib
import json
import os
from pathlib import Path
import re
import sys
import time
from types import SimpleNamespace
import uuid

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT)); sys.path.insert(0, str(ROOT / "src"))
from am_gateway.assistant import retrieval
from am_gateway.assistant.quality import SCHEMA_VERSION, output_hash
from integration import assistant_queries

SUPPORTED_FAMILIES = {"known_project", "alias", "natural_name", "ambiguous_alias", "bounded_absence",
                      "source_unavailable", "partial_source", "stale_snapshot", "subject_correction",
                      "ambiguous_pronoun", "marked_complete", "plan_vs_actual"}
MODULES = (Path(retrieval.__file__), Path(assistant_queries.__file__))
LOADED_HASHES = {str(p.relative_to(ROOT)): hashlib.sha256(p.read_bytes()).hexdigest() for p in MODULES}


def _base(case, layer):
    return {"schema_version": SCHEMA_VERSION, "case_id": case["case_id"], "intent": case["intent"],
            "channel": case["channel"], "source_version": case["source_version"], "layer": layer,
            "attempt_id": case.get("evaluation_attempt_id", uuid.uuid4().hex), "cold_or_warm": "cold",
            "execution_provenance": {"actual_assistant_intake": False, "actual_model_call": False,
                "actual_realtime_inbound": False, "actual_feishu_delivery": False,
                "synthetic_identity_source_and_transport": True}}


def _rows(fixture):
    result = []
    for number, project in enumerate(fixture["projects"], 1):
        progress = project.get("progress", "")
        if "blocker" in project and "next_step" in project:
            progress = "进度：" + progress + "\n卡点：" + project["blocker"] + "\n下一步：" + project["next_step"] + "\n备注："
        code = f"QI-2026-{number:03d}"
        result.append({"key": project["project_id"], "project_ref": project["project_id"],
            "business_id": code, "title": code + "｜" + project["name"],
            "aliases": [project["name"], *project.get("aliases", [])], "identity_status": "verified",
            "项目进展": progress, "待办": project.get("next_step", ""),
            "关键日期": "计划：" + project["planned_finish"] if project.get("planned_finish") else "",
            "人员与职责": project.get("owner", "")})
    return result


class SnapshotPort:
    def __init__(self, fixture, setup):
        self.now = datetime.fromisoformat(setup["now"]).timestamp()
        self.error = setup.get("source_error")
        self.refresh_count = 0
        self.last_refresh_succeeded = False
        rows = _rows(fixture)
        for change in setup.get("mutations", []):
            for row in rows:
                if row["project_ref"] == change["project_id"]:
                    row["aliases"].append(change["append_alias"])
        if "source_visible_rows" in setup:
            rows = [r for r in rows if r["project_ref"] in setup["source_visible_rows"]]
        self.fresh = {"version": fixture["version"], "source_revision": 1,
            "source": "controlled-synthetic-r7", "observed_at": self.now,
            "complete": setup.get("source_complete", fixture["complete"]), "rows": rows,
            "stale": False, "refresh_error": self.error, "cache_hit": False, "include_read_time": False}
        self.value = deepcopy(self.fresh)
        if "cached_age_s" in setup:
            self.value["observed_at"] -= setup["cached_age_s"]
            self.value["version"] = setup["cached_version"]
            self.value["cache_hit"] = True

    def clock(self):
        return self.now

    def get(self, wait=False):
        return deepcopy(self.value)

    def refresh(self):
        self.refresh_count += 1
        future = Future()
        if self.error:
            future.set_exception(RuntimeError("synthetic_source_unavailable"))
        else:
            self.value = deepcopy(self.fresh)
            self.last_refresh_succeeded = True
            future.set_result(deepcopy(self.value))
        return future


def _independent_source_check(result, snapshot, allowed, question):
    """Validate a finite extractive grammar independently of the renderer.

    This proves only verbatim field provenance or a closed source-status reply.
    Free-form analysis and novel prose always require a different reviewer.
    """
    status, body = result.get("status"), result.get("body", "")
    accessible = {r["project_ref"]: r for r in snapshot["rows"] if r["project_ref"] in allowed}
    if any(pid not in accessible for pid in result.get("project_ids", [])):
        return False, "project_outside_scope"
    for row in snapshot["rows"]:
        if row["project_ref"] not in allowed and (row["title"] in body or "SYN-PRIVATE-SECRET" in body):
            return False, "hidden_row_disclosed"
    if status == "found":
        selected = None
        for line in body.splitlines():
            if not line.strip():
                continue
            header = next((r for r in accessible.values() if line == r["title"] + "（按台账记录）"), None)
            if header:
                selected = header; continue
            if selected is None or "：" not in line:
                return False, "unrecognized_extractive_structure"
            label, payload = line.split("：", 1)
            if label not in {"进展", "卡点／风险", "下一步", "下一步／待办", "本人待办", "计划与日期", "人员与职责", "备注", "进度", "卡点"}:
                return False, "unrecognized_field"
            values = [selected.get(k, "") for k in ("项目进展", "待办", "关键日期", "人员与职责")]
            values += [v.split("：", 1)[1] for raw in values[:] for v in raw.splitlines() if "：" in v]
            if payload in values:
                continue
            if payload == "当前台账未明确记录。" and any(not value for value in values):
                continue
            if payload == "台账未明确记录；不能据此判断没有风险。" and not any("卡点：" in v for v in values):
                continue
            return False, "field_not_verbatim_from_selected_source"
        return selected is not None, "verbatim_source_projection"
    if status == "ambiguous":
        named = [r for r in accessible.values() if any(alias in question for alias in r["aliases"])]
        return (len(named) >= 2 and all(r["title"] in body for r in named)
                and ("确认" in body or "哪一个" in body)), "bounded_alias_disambiguation"
    if status == "no_match":
        any_named = any(alias in question for row in accessible.values() for alias in row["aliases"])
        valid = (snapshot.get("complete") is True and not snapshot.get("refresh_error") and not any_named
                 and "当前" in body and "台账" in body and "暂未匹配" in body and "项目不存在" not in body)
        return valid, "complete_source_bounded_absence"
    if status == "unavailable":
        return (bool(snapshot.get("refresh_error")) and ("无法" in body or "失败" in body)
                and "项目不存在" not in body), "source_failure_not_absence"
    if status == "incomplete":
        return (snapshot.get("complete") is False and "不完整" in body and "不存在" not in body), "partial_source_not_absence"
    return False, "outside_closed_validator_scope"


def run_case(case, fixture, *, layer="offline"):
    record = _base(case, layer)
    if layer != "offline" or case["family"] not in SUPPORTED_FAMILIES:
        record["outcome"] = {"status": "not_run", "reason": "required_native_capability_not_bound"}
        return record
    started = time.monotonic()
    setup = case["setup"]
    port = SnapshotPort(fixture, setup)
    authority = SimpleNamespace(projects_for_chat=lambda chat_id: tuple(setup["audience"].get("project_ids", [])))
    reader = assistant_queries.StudioProjectQueries(SimpleNamespace(am_self=SimpleNamespace(snapshots=port)), authority,
        {"enabled": True, "max_snapshot_age_seconds": 60,
         "ledger_project_refs": {p["project_id"]: p["project_id"] for p in fixture["projects"]}})
    question = re.sub(r"^@AM\s*", "", case["input"]["text"])
    event = SimpleNamespace(body=question, attachments=(), channel=case["channel"], chat_id=case["input"]["conversation_id"],
        item_id=case["input"]["origin_event_id"], version_time=setup["now"], content_hash=output_hash(question))
    allowed = setup["allowed_project_ids"]
    focus = "P-SYN-ALPHA" if case["family"] == "subject_correction" else None
    result = None
    for _ in range(3):
        try:
            result = reader(event, setup["actor"], allowed, focus_project_id=focus)
            break
        except assistant_queries.FastReadPending:
            continue
    ended = time.monotonic()
    record["timing"] = {"clock_domain": "host.monotonic:" + str(os.getpid()),
        "events": {"received": started, "completed": ended}, "spans": {"model": [], "query_component": [{"start": started, "end": ended}]}}
    if result is None:
        record["outcome"] = {"status": "not_run", "reason": "query_component_delegated_to_unbound_semantic_path"}
        return record
    body, status = result.get("body", ""), result.get("status", "unknown")
    completed = []
    for label, task in (("进展：", "progress"), ("卡点／风险：", "blocker"), ("下一步：", "next_step")):
        if label in body:
            completed.append(task)
    if status == "ambiguous": completed.append("clarify")
    if status == "no_match": completed.append("bounded_no_match")
    if status == "unavailable": completed.append("explain_source_failure")
    if status == "incomplete" and port.value.get("complete") is False: completed.append("explain_partial_source")
    if port.refresh_count and case["family"] == "stale_snapshot": completed.append("refresh")
    if case["family"] == "subject_correction" and result.get("project_ids") == ["P-SYN-BETA"]: completed.append("subject_correction")
    if "已标记完成" in body and "验收仍待确认" in body: completed.append("state_qualification")
    record["output"] = {"text": body, "kind": "clarification" if status == "ambiguous" else "answer",
        "facts": {}, "completed_subtasks": completed, "retrieval_status": status, "first_substantive_text": body}
    record["outcome"] = {"status": "completed"}
    record["timing"]["events"]["first_substantive_visible"] = ended
    source_ok, check_reason = _independent_source_check(result, port.value, allowed, question)
    current_hashes = {str(p.relative_to(ROOT)): hashlib.sha256(p.read_bytes()).hexdigest() for p in MODULES}
    if current_hashes != LOADED_HASHES:
        source_ok = False; check_reason = "module_file_changed_after_import"
    folder = Path(os.environ.get("AM_R7_EVIDENCE_DIR", str(Path.cwd() / "r7-observations")))
    folder.mkdir(parents=True, exist_ok=True)
    evidence = folder / (case["case_id"] + "-" + record["attempt_id"] + ".json")
    evidence.write_text(json.dumps({"scope": "controlled_offline_query_components", "module_sha256": LOADED_HASHES,
        "input": deepcopy(case["input"]), "normalized_question": question, "setup": deepcopy(setup),
        "source_snapshot": port.value, "source_refresh_count": port.refresh_count,
        "last_refresh_succeeded": port.last_refresh_succeeded,
        "observed_query_result": result, "source_validator": {"pass": source_ok, "reason": check_reason},
        "model_calls": 0, "external_effects": 0}, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    record["execution_provenance"].update(actual_component_run=True,
        component_scope="StudioProjectQueries + LedgerIndex; synthetic source/identity; no intake/sender",
        execution_ref=str(evidence), module_sha256=LOADED_HASHES)
    record["semantic_review"] = {"status": "pass" if source_ok else "needs_review", "method": "source_check",
        "independent": True, "output_sha256": output_hash(body), "source_version": case["source_version"],
        "evidence_ref": str(evidence), "validator_scope": check_reason}
    if case["family"] == "stale_snapshot":
        observed = {
            "refresh_completed": port.refresh_count > 0 and port.last_refresh_succeeded,
            "refreshed_source_version_bound": (port.last_refresh_succeeded
                and result.get("source_version") == port.value.get("version") == fixture["version"]
                and result.get("source_observed_at") == port.value.get("observed_at") == port.now),
        }
        record["assertions"] = [{"id": name, "status": "pass" if passed else "fail", "method": "independent_readback",
            "output_sha256": output_hash(body), "source_version": case["source_version"],
            "evidence_ref": str(evidence)} for name, passed in observed.items()]
    return record
