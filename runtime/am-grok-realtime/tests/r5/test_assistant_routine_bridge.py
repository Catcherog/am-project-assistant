"""Standing routine grants cross the actual R5/Meegle apply-readback path."""
from copy import deepcopy
from dataclasses import replace
import json
from pathlib import Path
import sqlite3
import sys
import time
from types import SimpleNamespace

import pytest

from am_gateway.models import ContractError, dumps
from integration.assistant_maintenance import POLICY_AUTHORITY
from integration.meegle_executor import (MeegleOperationBridge, RoutineAwareMeegleExecutor,
                                         build_executor)
from test_assistant_maintenance import attach_coordinator, rig


def native_rig(tmp_path, *, live=True):
    base = rig(tmp_path)
    state = {"values": {"progress_text": "原进展", "todo_text": "原待办", "status": "in_progress"},
             "writes": 0, "reads": 0, "mode": "normal", "commands": []}
    grant = {"enabled": True, "project_id": "P", "project_key": "space", "target_id": "item1",
             "target_name": "实际目标项目", "work_item_type": "project-type",
             "field_semantics": {"progress_text": "progress", "todo_text": "todo"},
             "field_types": {"progress_text": "multi-text", "todo_text": "multi-text"},
             "authorization_ref": "synthetic-owner-standing-permission-fixture"}
    config = {"host": "project.feishu.cn", "user_key": "meegle-owner", "grants": [],
              "send_live": live, "routine_database": base.store.path, "routine_grants": [grant]}

    def invoke(args):
        state["commands"].append(args)
        if args[:2] == ["auth", "status"]:
            return {"authenticated": True, "host": "project.feishu.cn"}
        if args[:2] == ["user", "me"]:
            return {"user_key": "meegle-owner"}
        if args[:2] == ["workitem", "get"]:
            state["reads"] += 1
            if state["mode"] == "readback_unavailable" and state["writes"]:
                raise TimeoutError("private native response")
            return {"work_item_attribute": {"work_item_id": "item1", "work_item_name": "实际目标项目",
                    "owned_project": {"key": "space"}, "work_item_type": {"key": "project-type"},
                    "update_time": state["writes"]},
                    "work_item_fields": [{"key": field, "value": value} for field, value in state["values"].items()]}
        if args[:2] == ["workitem", "meta-fields"]:
            if state.get("during_metadata"):
                state["during_metadata"]()
            if state["mode"] == "changed_during_metadata":
                state["values"]["progress_text"] = "人工刚刚修改的进展"
            return {"list": [{"field_key": field, "field_type": "multi-text",
                              "field_name": "审批状态" if state["mode"] == "formal_metadata" and field == "progress_text"
                              else ("项目进展" if field == "progress_text" else "待办")}
                             for field in ("progress_text", "todo_text")]}
        assert args[:2] == ["workitem", "update"]
        fields = json.loads(args[args.index("--fields") + 1])
        state["writes"] += 1
        state["values"].update({field["field_key"]: field["field_value"] for field in fields})
        if state["mode"] == "lost_response":
            raise TimeoutError("transport lost after actual apply")
        return {"updated": True}

    bridge = MeegleOperationBridge(config, tmp_path / "operations.sqlite", invoke=invoke)

    class BoundExecutor(RoutineAwareMeegleExecutor):
        def __init__(self):
            argv = [sys.executable, "-c", "pass"]
            super().__init__(argv=argv, readback_argv=argv, targets={"P": {"work_item": ["item1"]}},
                             actions=["update_fields"], routine_live=live, routine_grants=config["routine_grants"])
            self.requests, self.before_apply = [], None

        def _run(self, argv, request):
            self.requests.append(deepcopy(request))
            if request["operation"] == "apply" and self.before_apply:
                self.before_apply()
            return bridge.handle(request)

    executor = BoundExecutor()
    base.executor = executor
    base.maintenance.executor = executor
    return SimpleNamespace(**vars(base), bridge=bridge, bridge_config=config, native_state=state,
                           invoke=invoke, grant=grant)


def next_proposal(r, *, event_id="m2", value="测试已经取得第二批数据", field="progress_text"):
    old = r.harness.task(r.proposal["task_id"])
    r.harness.record_receipt(event_id="draft-ended:" + old["task_id"], task_id=old["task_id"],
        business_id=old["business_id"], generation=old["generation"], kind="ended",
        payload={"status": "succeeded", "run_id": "fixture-draft:" + old["task_id"],
                 "stopped_evidence": "isolated draft generator ended"})
    event = replace(r.event, native_id=event_id, body="把项目 P 的进展改成" + value,
                    original_time=time.time() + 1, version_time=time.time() + 1)
    r.harness.ingest(event, r.authority)
    task = r.harness.route_one()
    assert task["intent"] == "project.execute"
    envelope = r.harness.claim()
    assert envelope
    r.proposal = r.repository.propose(envelope, "project.execute", {
        "project_id": "P", "target": {"type": "work_item", "id": "item1"},
        "action": "update_fields", "changes": {field: value}})
    return r.proposal


def test_standing_grant_applies_successive_new_values_without_new_frozen_after_config(tmp_path):
    r = native_rig(tmp_path)
    assert r.maintenance.step()["status"] == "applied"
    first_operation = r.executor.requests[0]["idempotency_key"]
    assert r.native_state["values"]["progress_text"] == "测试已开始"
    assert r.native_state["values"]["status"] == "in_progress"
    next_proposal(r)
    assert r.maintenance.step()["status"] == "applied"
    assert r.native_state["writes"] == 2
    assert r.native_state["values"]["progress_text"] == "测试已经取得第二批数据"
    assert "before" not in r.grant and "after" not in r.grant
    with sqlite3.connect(r.bridge.path) as db:
        db.row_factory = sqlite3.Row
        rows = [dict(row) for row in db.execute("SELECT * FROM operations ORDER BY rowid")]
    assert all(row["grant_kind"] == "routine" and row["state"] == "applied" for row in rows)
    assert json.loads(rows[0]["before_json"]) == {"progress_text": "原进展"}
    assert json.loads(rows[1]["before_json"]) == {"progress_text": "测试已开始"}
    assert json.loads(rows[0]["authority_json"])["authority"] == POLICY_AUTHORITY
    assert rows[0]["operation_id"] == first_operation
    assert len([c for c in r.native_state["commands"] if c[:2] == ["workitem", "get"]]) == 6


def test_duplicate_and_restart_readback_do_not_repeat_actual_write(tmp_path):
    r = native_rig(tmp_path)
    assert r.maintenance.step()["status"] == "applied"
    original = r.executor.requests[0]
    assert r.bridge.handle(original)["status"] == "applied"
    restarted = MeegleOperationBridge(r.bridge_config, r.bridge.path, invoke=r.invoke)
    assert restarted.handle({**original, "operation": "readback"})["status"] == "applied"
    assert r.native_state["writes"] == 1


def test_lost_write_response_is_independently_read_back_without_replay(tmp_path):
    r = native_rig(tmp_path)
    r.native_state["mode"] = "lost_response"
    assert r.maintenance.step()["status"] == "applied"
    assert r.native_state["writes"] == 1
    assert [request["operation"] for request in r.executor.requests] == ["apply", "readback"]


def test_send_live_false_does_not_call_native_cli_or_write(tmp_path):
    r = native_rig(tmp_path, live=False)
    assert r.maintenance.step()["status"] == "awaiting_binding"
    assert r.native_state["commands"] == [] and r.native_state["writes"] == 0
    request = {"schema": "am.approved.operation.v1", "operation": "apply", "idempotency_key": "effect_untrusted",
               "payload": r.proposal["payload"]}
    with pytest.raises(ContractError, match="send_live_disabled"):
        r.bridge.handle(request)
    assert r.native_state["commands"] == []


def test_stdin_cannot_claim_owner_or_policy_permission_without_runtime_record(tmp_path):
    r = native_rig(tmp_path)
    request = {"schema": "am.approved.operation.v1", "operation": "apply", "idempotency_key": "effect_untrusted",
        "proposal_id": r.proposal["proposal_id"], "version": r.proposal["version"], "digest": r.proposal["digest"],
        "payload": r.proposal["payload"], "approved_by": POLICY_AUTHORITY, "verified": True, "owner_approved": True}
    with pytest.raises(ContractError, match="standing_routine_claim_required"):
        r.bridge.handle(request)
    assert r.native_state["commands"] == [] and r.native_state["writes"] == 0


@pytest.mark.parametrize("mutation", ["source_deleted", "policy_changed", "action_not_executing", "wrong_authority"])
def test_runtime_claim_source_and_policy_are_rechecked_inside_native_bridge(tmp_path, mutation):
    r = native_rig(tmp_path)
    def mutate():
        with r.store.transaction() as db:
            if mutation == "source_deleted":
                db.execute("UPDATE gw_items SET is_deleted=1")
            elif mutation == "policy_changed":
                policy = replace(r.authority, version=2, projects=()).json()
                db.execute("UPDATE h1_tasks SET policy_json=?", (policy,))
            elif mutation == "action_not_executing":
                db.execute("UPDATE a5_actions SET state='ready'")
            else:
                db.execute("UPDATE h4_agent_proposals SET approved_by='ordinary-user'")
    r.executor.before_apply = mutate
    result = r.maintenance.step()
    assert result["status"] != "applied" and r.native_state["writes"] == 0
    assert r.native_state["commands"] == []


def test_disabled_or_wrong_fields_grant_is_not_mistaken_for_an_available_binding(tmp_path):
    r = native_rig(tmp_path)
    r.executor.routine_targets = {}
    assert r.maintenance.step()["status"] == "awaiting_binding"
    assert r.native_state["commands"] == []


def test_formal_field_alias_in_private_config_cannot_bypass_routine_policy(tmp_path):
    r = native_rig(tmp_path)
    r.grant["field_semantics"]["status"] = "progress"
    r.grant["field_types"]["status"] = "text"
    assert r.maintenance.step()["status"] == "unknown"
    assert r.native_state["writes"] == 0


def test_remote_formal_field_definition_cannot_masquerade_as_progress(tmp_path):
    r = native_rig(tmp_path)
    r.native_state["mode"] = "formal_metadata"
    assert r.maintenance.step()["status"] == "unknown"
    assert r.native_state["writes"] == 0


def test_parallel_human_edit_during_preflight_is_preserved(tmp_path):
    r = native_rig(tmp_path)
    r.native_state["mode"] = "changed_during_metadata"
    assert r.maintenance.step()["status"] == "not_applied"
    assert r.native_state["writes"] == 0
    assert r.native_state["values"]["progress_text"] == "人工刚刚修改的进展"
    with sqlite3.connect(r.bridge.path) as db:
        assert db.execute("SELECT COUNT(*) FROM operations").fetchone()[0] == 0


def test_source_withdrawn_while_native_preflight_was_running_cannot_be_written(tmp_path):
    r = native_rig(tmp_path)
    def withdraw():
        with r.store.transaction() as db:
            db.execute("UPDATE gw_items SET is_deleted=1")
    r.native_state["during_metadata"] = withdraw
    assert r.maintenance.step()["status"] == "unknown"
    assert r.native_state["writes"] == 0
    with sqlite3.connect(r.bridge.path) as db:
        assert db.execute("SELECT COUNT(*) FROM operations").fetchone()[0] == 0


def test_unresolved_write_blocks_a_new_operation_to_same_target_fields(tmp_path):
    r = native_rig(tmp_path)
    r.native_state["mode"] = "readback_unavailable"
    assert r.maintenance.step()["status"] == "unknown"
    next_proposal(r)
    assert r.maintenance.step()["status"] == "not_applied"
    assert r.native_state["writes"] == 1


@pytest.mark.parametrize("host_live,private_live,expected", [(False, True, False), (True, False, False), (True, True, True)])
def test_factory_requires_both_host_and_private_live_switches(tmp_path, host_live, private_live, expected, monkeypatch):
    r = native_rig(tmp_path)
    r.bridge_config["send_live"] = private_live
    path = tmp_path / "private-meegle.json"
    path.write_text(dumps(r.bridge_config));path.chmod(0o600)
    executor = build_executor(None, {"phase1_gateway": {"send_live": host_live,
        "meegle_executor": {"config_path": str(path)}}}, None, tmp_path)
    assert executor.routine_live is expected
    assert executor.routine_targets == {"P": {"item1": r.grant["field_semantics"]}}
    if not expected:
        def forbidden(*args, **kwargs):
            pytest.fail("disabled routine must not launch a host subprocess")
        monkeypatch.setattr(executor, "_run", forbidden)
        result = executor({**r.proposal, "approved_by": POLICY_AUTHORITY}, idempotency_key="effect_disabled")
        assert result["status"] == "not_applied"


def test_original_frozen_grant_still_requires_exact_before_and_expiry(tmp_path):
    r = native_rig(tmp_path)
    r.bridge_config["grants"] = [{"project_id": "P", "project_key": "space", "target_id": "item1",
        "target_name": "实际目标项目", "work_item_type": "project-type", "before": {"progress_text": "原进展"},
        "after": {"progress_text": "测试已开始"}, "field_types": {"progress_text": "multi-text"},
        "authorization_ref": "exact-existing-approval", "expires_at": time.time() - 1}]
    request = {"schema": "am.approved.operation.v1", "operation": "apply", "idempotency_key": "effect_existing",
               "payload": r.proposal["payload"]}
    with pytest.raises(ContractError, match="frozen_grant_expired"):
        r.bridge.handle(request)
    assert r.native_state["writes"] == 0


def test_bridge_accepts_same_admitted_r5_source_and_frozen_intake_hash(tmp_path):
    r = native_rig(tmp_path)
    attach_coordinator(r)
    assert r.maintenance.step()["status"] == "applied"
    assert r.native_state["writes"] == 1


@pytest.mark.parametrize("state", ["pending", "local_ready", "recalled", "admitted_new_revision"])
def test_r5_intake_changes_during_native_preflight_cannot_write_old_h1_values(tmp_path, state):
    r = native_rig(tmp_path)
    coordinator = attach_coordinator(r)
    def change_ingress():
        changed = replace(r.event, body=r.event.body if state == "admitted_new_revision" else "谢谢",
                          revision="v2", version_time=time.time() + 3,
                          is_deleted=state == "recalled")
        coordinator.capture(changed)
        if state == "local_ready":
            assert coordinator.intake_once()["state"] == "local_ready"
        elif state == "admitted_new_revision":
            with r.store.transaction() as db:
                db.execute("UPDATE r5_intake SET state='admitted' WHERE item_id=?", (r.event.item_id,))
    r.native_state["during_metadata"] = change_ingress
    assert r.maintenance.step()["status"] == "unknown"
    assert r.native_state["writes"] == 0
    assert json.loads(r.store.one("SELECT event_json FROM gw_items WHERE item_id=?", (r.event.item_id,))["event_json"])["body"] == r.event.body
    with sqlite3.connect(r.bridge.path) as db:
        assert db.execute("SELECT COUNT(*) FROM operations").fetchone()[0] == 0


def test_bridge_rejects_changed_r5_hash_even_if_admitted_body_stays_equal(tmp_path):
    r = native_rig(tmp_path)
    attach_coordinator(r)
    def replace_intake_binding():
        with r.store.transaction() as db:
            db.execute("UPDATE r5_intake SET source_hash='different-trusted-intake-binding' WHERE item_id=?", (r.event.item_id,))
    r.native_state["during_metadata"] = replace_intake_binding
    assert r.maintenance.step()["status"] == "unknown"
    assert r.native_state["writes"] == 0
