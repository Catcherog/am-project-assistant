"""Routine remote writes reuse the real proposal/executor/authority contracts."""
from dataclasses import replace
import json
import sys
import threading
import time
from types import SimpleNamespace

import pytest

from am_gateway.assistant.actions import AssistantActions
from am_gateway.assistant.coordinator import AssistantCoordinator
from am_gateway.assistant.policy import Actor, AssistantPolicy, PolicyConfig
from am_gateway.models import ContractError, Event, dumps
from am_gateway.phase1 import Harness, HarnessConfig, Policy
from am_gateway.phase1.agents import ProposalRepository
from am_gateway.phase1.contracts import registry
from am_gateway.store import Store
from integration.approved_command import ApprovedCommandExecutor
from integration.assistant_maintenance import POLICY_AUTHORITY, RoutineMaintenance


class HostFixture(ApprovedCommandExecutor):
    """Use the production apply/readback wrapper with an isolated host transport."""

    def __init__(self):
        argv = [sys.executable, "-c", "pass"]
        super().__init__(argv=argv, readback_argv=argv,
                         targets={"P": {"work_item": ["item1"]}}, actions=["update_fields"])
        self.values = {"progress_text": "原进展", "todo_text": "原待办", "status": "in_progress"}
        self.calls, self.mode = [], "normal"
        self.entered = self.release = None

    def _run(self, argv, request):
        self.calls.append(request)
        if request["operation"] == "apply":
            if self.entered is not None:
                self.entered.set()
                assert self.release.wait(5)
            if self.mode == "exception":
                raise TimeoutError("secret-token-and-private-exception-body")
            self.values.update(request["payload"]["changes"])
            return {"status": "submitted", "idempotency_key": request["idempotency_key"]}
        changes = {field: self.values[field] for field in request["payload"]["changes"]}
        if self.mode == "mismatch":
            changes[next(iter(changes))] = "不是所请求的值"
        return {"status": "applied", "idempotency_key": request["idempotency_key"],
                "project_id": request["payload"]["project_id"], "target": request["payload"]["target"],
                "changes": changes, "evidence_ref": "host-independent-readback:" + request["idempotency_key"]}


def rig(tmp_path, *, fields=None, changes=None, source_text=None, actor_id="alice", mentions=()):
    store = Store(tmp_path / "routine.sqlite")
    capabilities = registry()
    capabilities["project.execute"] = replace(capabilities["project.execute"],
        available=True, durable_acceptance=True, evidence_ref="isolated-maintenance-fixture")
    harness = Harness(store, HarnessConfig("tenant", "bot", "owner", "owner-dm", capabilities=capabilities))
    authority = Policy(actor_id, "host-authenticated-source", "source-dm", ("source-dm",), ("P",), allow_reply=True)
    event = Event("tenant", "m1", "source-dm", actor_id,
        source_text or "把项目 P 的进展改成测试已开始", time.time(), channel="bot_dm",
        project_ids=("P",), mentions=mentions, revision="v1")
    harness.ingest(event, authority)
    task = harness.route_one()
    if task["route_state"] != "resolved" or task["intent"] != "project.execute":
        # The source is already authenticated; choose a task type via the same
        # trusted review boundary used when free-form classification needs help.
        harness.review(task["task_id"], action="resume", actor=actor_id, intent="project.execute")
    envelope = harness.claim()
    assert envelope
    repository = ProposalRepository(store, "owner")
    proposal = repository.propose(envelope, "project.execute", {
        "project_id": "P", "target": {"type": "work_item", "id": "item1"},
        "action": "update_fields", "changes": changes or {"progress_text": "测试已开始"}})
    policy = AssistantPolicy(PolicyConfig(owner_user_ids=frozenset({"owner"}),
        demo_department_ids=frozenset({"DEMO"}), leader_user_ids=frozenset({"leader"}),
        protected_person_ids=frozenset({"jia", "chen"})))
    actors = {"owner": Actor("owner", "示例负责人", (), True, is_owner=True),
              "alice": Actor("alice", "张工", ("DEMO",), True),
              "leader": Actor("leader", "负责人", ("DEMO",), True, is_leader=True),
              "jia": Actor("jia", "示例主管甲", ("DEMO",), True),
              "chen": Actor("chen", "示例主管乙", ("DEMO",), True)}
    directory = SimpleNamespace(resolve=lambda identity: actors[identity])
    actions, executor = AssistantActions(store, "owner", policy), HostFixture()
    mapping = {"P": {"progress_text": "progress", "todo_text": "todo"}} if fields is None else fields
    maintenance = RoutineMaintenance(harness, repository, directory, policy, actions, executor, mapping)
    return SimpleNamespace(store=store, harness=harness, authority=authority, event=event, repository=repository,
        proposal=proposal, policy=policy, actors=actors, directory=directory, actions=actions,
        executor=executor, fields=mapping, maintenance=maintenance)


def new_maintenance(r):
    return RoutineMaintenance(r.harness, r.repository, r.directory, r.policy, r.actions, r.executor, r.fields)


def attach_coordinator(r):
    """Capture/admit through the real R5 ingress while reusing the fixed H1 task."""
    r.directory.project_ids_for = lambda actor: ("P",)
    r.directory.register_dm = lambda event: r.directory.resolve(event.sender_id)
    class Authority:
        collection_config = SimpleNamespace(live_cutover_at=0)
        def __call__(self, event):
            return r.authority
    coordinator = AssistantCoordinator(r.harness, directory=r.directory, policy=r.policy,
                                       authority=Authority())
    assert coordinator.capture(r.event)
    assert coordinator.intake_once()["state"] == "admitted"
    return coordinator


def test_sourced_progress_and_todo_apply_only_configured_fields_with_independent_readback(tmp_path):
    r = rig(tmp_path, changes={"progress_text": "测试已开始", "todo_text": "继续收集测试数据"})
    result = r.maintenance.step()
    assert result["status"] == "applied"
    proposal = r.repository.get(r.proposal["proposal_id"])
    assert proposal["approved_by"] == POLICY_AUTHORITY and proposal["status"] == "applied"
    assert r.executor.values == {"progress_text": "测试已开始", "todo_text": "继续收集测试数据", "status": "in_progress"}
    assert [call["operation"] for call in r.executor.calls] == ["apply", "readback"]
    action = r.actions.get(result["results"][0]["action_id"])
    assert action["state"] == "applied"
    assert action["evidence_ref"].startswith("host-write-receipt:sha256:")
    assert action["readback_evidence_ref"].startswith("host-independent-readback:")
    assert action["evidence_ref"] != action["readback_evidence_ref"]
    assert action["context"]["source_evidence"][0]["verified"] is True
    assert action["source_stamp"]["source_version"] == proposal["source_version"]
    assert r.maintenance.step()["status"] == "idle" and len(r.executor.calls) == 2
    assert new_maintenance(r).step()["status"] == "idle"


@pytest.mark.parametrize("field", ["status", "due_date", "milestone", "owner", "approval", "acceptance", "责任人", "验收"])
def test_formal_or_unmapped_changes_remain_in_original_review_queue(tmp_path, field):
    r = rig(tmp_path, changes={"progress_text": "测试已开始", field: "new"})
    result = r.maintenance.step()
    assert result["status"] == "awaiting_approval"
    assert r.repository.get(r.proposal["proposal_id"])["status"] == "draft"
    assert r.executor.calls == [] and r.actions.list() == []


@pytest.mark.parametrize("mapping,reason", [({}, "routine_fields_unconfigured"),
    ({"P": {"status": "progress"}}, "formal_field_cannot_be_routine"),
    ({"P": {"progress_text": "approval"}}, "routine_field_mapping_invalid")])
def test_empty_or_invalid_mapping_reports_awaiting_binding(tmp_path, mapping, reason):
    r = rig(tmp_path, fields=mapping)
    result = r.maintenance.step()
    assert result == {"status": "awaiting_binding", "reason": reason, "results": []}
    assert r.executor.calls == [] and r.repository.get(r.proposal["proposal_id"])["status"] == "draft"
    assert r.maintenance.status()["status"] == "awaiting_binding"


def test_unbound_executor_target_cannot_be_enabled_by_field_mapping(tmp_path):
    r = rig(tmp_path)
    r.executor.targets = {"P": {"work_item": ["different-item"]}}
    result = r.maintenance.step()
    assert result["status"] == "awaiting_binding"
    assert result["results"][0]["reason"] == "executor_target_not_bound"
    assert r.executor.calls == []
    r.maintenance.executor = None
    assert r.maintenance.step()["reason"] == "business_executor_unbound"


@pytest.mark.parametrize("change", ["source_deleted", "source_edited", "source_version", "business_id", "generation", "policy_revoked", "project_scope", "missing_version"])
def test_current_source_and_harness_authority_are_required_even_if_model_claims_verified(tmp_path, change):
    r = rig(tmp_path)
    changed_payload = r.proposal["payload"] | {"verified": True, "approved": True, "source_verified": True}
    r.repository.revise(r.proposal["proposal_id"], expected_version=1, actor="owner", payload=changed_payload)
    if change in {"source_deleted", "source_edited"}:
        r.harness.ingest(replace(r.event, is_deleted=change == "source_deleted", body="新版本",
            revision="v2", version_time=time.time() + 2), r.authority)
    elif change in {"source_version", "generation"}:
        with r.store.transaction() as db:
            db.execute("UPDATE h4_agent_proposals SET " + change + "=" + change + "+1")
    elif change == "business_id":
        with r.store.transaction() as db:
            db.execute("UPDATE h4_agent_proposals SET business_id='wrong-operation'")
    elif change == "missing_version":
        with r.store.transaction() as db:
            db.execute("DELETE FROM gw_item_versions")
    else:
        new_policy = replace(r.authority, version=2, revoked=change == "policy_revoked",
                             projects=() if change == "project_scope" else ("P",))
        r.harness.update_policy(r.proposal["task_id"], new_policy)
    result = r.maintenance.step()
    assert result["status"] in {"awaiting_review", "awaiting_approval"}
    assert r.executor.calls == [] and r.repository.get(r.proposal["proposal_id"])["status"] == "draft"


@pytest.mark.parametrize("actor,text,mentions", [
    ("leader", "把项目 P 的进展改成测试已开始", ()),
    ("alice", "把项目 P 的进展改成示例主管甲已经知悉", ()),
    ("alice", "把项目 P 的进展改成示例主管乙正在协调", ()),
    ("alice", "把项目 P 的进展改成测试已开始", ("leader",)),
    ("alice", "把项目 P 的进展改成测试已开始", ("jia",)),
    ("owner", "把项目 P 的进展改成验收已经通过", ()),
])
def test_sensitive_people_or_formal_business_content_still_requires_owner(tmp_path, actor, text, mentions):
    r = rig(tmp_path, actor_id=actor, source_text=text, mentions=mentions)
    assert r.maintenance.step()["status"] == "awaiting_approval"
    assert r.executor.calls == [] and r.repository.get(r.proposal["proposal_id"])["approved_by"] == ""


def test_directory_mismatch_cannot_turn_raw_message_into_verified_source(tmp_path):
    r = rig(tmp_path)
    r.directory.resolve = lambda _: r.actors["owner"]
    assert r.maintenance.step()["status"] == "awaiting_review"
    assert r.executor.calls == []


def test_blank_routine_value_cannot_erase_all_existing_todos(tmp_path):
    r = rig(tmp_path, changes={"todo_text": ""})
    assert r.maintenance.step()["results"][0]["reason"] == "routine_text_required_no_blanket_clear"
    assert r.executor.calls == []


def test_frozen_payload_digest_is_checked(tmp_path):
    r = rig(tmp_path)
    with r.store.transaction() as db:
        db.execute("UPDATE h4_agent_proposals SET payload=?", (dumps(r.proposal["payload"] | {"changes": {"progress_text": "偷改"}}),))
    assert r.maintenance.step()["results"][0]["reason"] == "proposal_frozen_digest_mismatch"
    assert r.executor.calls == []


def test_unknown_executor_result_never_replays_or_stores_private_exception(tmp_path):
    r = rig(tmp_path)
    r.executor.mode = "exception"
    assert r.maintenance.step()["status"] == "unknown"
    assert new_maintenance(r).step()["status"] == "idle"
    assert len(r.executor.calls) == 1
    audit = r.store.rows("SELECT detail FROM gw_audit")
    proposal = r.repository.get(r.proposal["proposal_id"])
    assert "secret-token" not in json.dumps([audit, proposal])
    assert r.maintenance.status()["unresolved"][0]["status"] == "unknown"


def test_mismatched_independent_readback_does_not_pass_as_applied(tmp_path):
    r = rig(tmp_path)
    r.executor.mode = "mismatch"
    assert r.maintenance.step()["status"] == "unknown"
    assert r.repository.get(r.proposal["proposal_id"])["status"] == "unknown"
    assert new_maintenance(r).step()["status"] == "idle" and len(r.executor.calls) == 2


def test_self_reported_applied_without_independent_readback_stays_unverified(tmp_path):
    r = rig(tmp_path)
    class Unverified:
        targets, actions = r.executor.targets, r.executor.actions
        def __call__(self, proposal, *, idempotency_key):
            return {"status": "applied", "verified": True, "evidence_ref": "self-assertion",
                    "receipt": {"status": "submitted", "idempotency_key": idempotency_key}}
    r.maintenance.executor = Unverified()
    assert r.maintenance.step()["status"] == "applied_unverified"
    assert r.repository.get(r.proposal["proposal_id"])["status"] == "applied_unverified"


def test_manual_confirmation_racing_auto_claim_cannot_double_execute(tmp_path):
    r = rig(tmp_path)
    r.executor.entered, r.executor.release = threading.Event(), threading.Event()
    results = []
    worker = threading.Thread(target=lambda: results.append(r.maintenance.step()))
    worker.start()
    try:
        assert r.executor.entered.wait(5)
        manual = r.repository.confirm(r.proposal["proposal_id"], version=r.proposal["version"],
            digest=r.proposal["digest"], actor="owner", executor=r.executor, harness=r.harness)
        assert manual["status"] == "executing"
        # A separate writer succeeds while the host operation is outstanding.
        with Store(r.store.path).transaction() as db:
            db.execute("INSERT INTO gw_audit(at,kind,object_id,detail) VALUES(0,'test','outside-lock','ok')")
    finally:
        r.executor.release.set()
        worker.join(5)
    assert not worker.is_alive() and results[0]["status"] == "applied"
    assert [call["operation"] for call in r.executor.calls] == ["apply", "readback"]


def test_two_maintenance_instances_share_the_h4_atomic_claim(tmp_path):
    r = rig(tmp_path)
    r.executor.entered, r.executor.release = threading.Event(), threading.Event()
    results = []
    worker = threading.Thread(target=lambda: results.append(r.maintenance.step()))
    worker.start()
    try:
        assert r.executor.entered.wait(5)
        assert new_maintenance(r).step()["status"] == "idle"
    finally:
        r.executor.release.set()
        worker.join(5)
    assert results[0]["status"] == "applied" and len(r.executor.calls) == 2


def test_crash_between_h4_claim_and_a5_prepare_is_parked_without_replay(tmp_path, monkeypatch):
    r = rig(tmp_path)
    def crash(*args, **kwargs):
        raise KeyboardInterrupt("simulated process loss")
    monkeypatch.setattr(r.actions, "prepare", crash)
    with pytest.raises(KeyboardInterrupt):
        r.maintenance.step()
    assert r.repository.get(r.proposal["proposal_id"])["status"] == "executing"
    assert new_maintenance(r).step()["status"] == "idle"
    assert r.executor.calls == []
    assert new_maintenance(r).status()["unresolved"][0]["status"] == "executing"


def test_crash_after_a5_effect_before_h4_completion_cannot_repeat_remote_write(tmp_path, monkeypatch):
    r = rig(tmp_path)
    original = r.actions.execute
    def crash_after_effect(*args, **kwargs):
        original(*args, **kwargs)
        raise KeyboardInterrupt("simulated process loss after effect")
    monkeypatch.setattr(r.actions, "execute", crash_after_effect)
    with pytest.raises(KeyboardInterrupt):
        r.maintenance.step()
    assert r.repository.get(r.proposal["proposal_id"])["status"] == "executing"
    assert r.actions.list()[0]["state"] == "applied"
    assert new_maintenance(r).step()["status"] == "idle" and len(r.executor.calls) == 2


def test_source_change_after_initial_claim_is_rechecked_before_host_write(tmp_path, monkeypatch):
    r = rig(tmp_path)
    original = r.actions.prepare
    def change_then_prepare(*args, **kwargs):
        r.harness.ingest(replace(r.event, body="撤销之前的内容", revision="v2", version_time=time.time() + 1), r.authority)
        return original(*args, **kwargs)
    monkeypatch.setattr(r.actions, "prepare", change_then_prepare)
    result = r.maintenance.step()
    assert result["status"] == "not_applied" and r.executor.calls == []
    assert r.repository.get(r.proposal["proposal_id"])["receipt"]["reason"] == "current_source_or_authority_unusable"


def test_unrelated_store_cannot_supply_actions_or_proposal_authority(tmp_path):
    r = rig(tmp_path)
    other = ProposalRepository(Store(tmp_path / "other.sqlite"), "owner")
    with pytest.raises(ContractError, match="same owner, store"):
        RoutineMaintenance(r.harness, other, r.directory, r.policy, r.actions, r.executor, r.fields)


def test_current_admitted_r5_source_is_frozen_in_maintenance_action(tmp_path):
    r = rig(tmp_path)
    attach_coordinator(r)
    result = r.maintenance.step()
    assert result["status"] == "applied"
    actual = r.store.one("SELECT source_hash FROM r5_intake WHERE item_id=?", (r.event.item_id,))["source_hash"]
    action = r.actions.get(result["results"][0]["action_id"])
    assert action["source_stamp"]["r5_intake_hash"] == actual


@pytest.mark.parametrize("state", ["pending", "local_ready", "recalled", "processing", "admitted_new_revision"])
def test_r5_edit_or_recall_cannot_apply_old_proposal_before_h1_catches_up(tmp_path, state):
    r = rig(tmp_path)
    coordinator = attach_coordinator(r)
    if state == "processing":
        with r.store.transaction() as db:
            db.execute("UPDATE r5_intake SET state='processing' WHERE item_id=?", (r.event.item_id,))
    else:
        changed = replace(r.event, body=r.event.body if state == "admitted_new_revision" else "谢谢",
                          revision="v2", version_time=time.time() + 2,
                          is_deleted=state == "recalled")
        coordinator.capture(changed)
        if state == "local_ready":
            assert coordinator.intake_once()["state"] == "local_ready"
        elif state == "admitted_new_revision":
            with r.store.transaction() as db:
                db.execute("UPDATE r5_intake SET state='admitted' WHERE item_id=?", (r.event.item_id,))
    # All cases deliberately retain the old Harness message and proposal.
    old = r.store.one("SELECT event_json FROM gw_items WHERE item_id=?", (r.event.item_id,))
    assert json.loads(old["event_json"])["body"] == r.event.body
    result = r.maintenance.step()
    assert result["status"] == "awaiting_review"
    assert r.repository.get(r.proposal["proposal_id"])["status"] == "draft"
    assert r.executor.calls == []


def test_r5_table_without_this_item_does_not_disable_an_existing_r4_source(tmp_path):
    r = rig(tmp_path)
    with r.store.transaction() as db:
        db.execute("CREATE TABLE r5_intake(item_id TEXT PRIMARY KEY,state TEXT,event_json TEXT,source_hash TEXT)")
    assert r.maintenance.step()["status"] == "applied"
