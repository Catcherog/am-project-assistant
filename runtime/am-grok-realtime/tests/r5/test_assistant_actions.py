"""Exact consent, source revisions and concurrent effect contracts."""

from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
import threading

import pytest

from am_gateway.assistant.actions import AssistantActions
from am_gateway.assistant.policy import ActionContext, Actor, AssistantPolicy, PolicyConfig
from am_gateway.models import ContractError
from am_gateway.store import Store


OWNER = Actor("owner-test", "示例负责人", (), True, is_owner=True)
PEER = Actor("peer-test", "同事", ("demo-test",), True)
OTHER = Actor("other-test", "其他同事", ("demo-test",), True)
OUTSIDER = Actor("outside-test", "其他部门", ("elsewhere-test",), True)
BODY = "请反馈测试报告的当前进展。"
STAMP = {"task_id": "task-test", "task_version": 1, "source_versions": {"message-test": 1}}
PAYLOAD = {"body": BODY, "target": {"chat_id": "chat-test"}, "attachments": []}


def context(kind="proactive_followup", **changes):
    values = dict(action_id="action-test", action_kind=kind, initiator=OWNER,
                  recipients=(PEER,), project_scope="demo", content_text=BODY)
    values.update(changes)
    return ActionContext(**values)


@pytest.fixture
def rig(tmp_path):
    store = Store(tmp_path / "gateway.sqlite", clock=lambda: 1000.0)
    policy = AssistantPolicy(PolicyConfig(owner_user_ids={OWNER.user_id},
                                          demo_department_ids={"demo-test"}))
    return AssistantActions(store, OWNER.user_id, policy), store, policy


def prepare(actions, ctx=None, payload=None, stamp=None):
    return actions.prepare(ctx or context(), PAYLOAD if payload is None else payload,
                           STAMP if stamp is None else stamp)


def approve(actions, prepared):
    return actions.decide(prepared["action_id"], prepared["version"], prepared["digest"], OWNER)


def applied(_):
    return {"status": "applied", "evidence_ref": "receipt-test", "readback_evidence_ref": "readback-test"}


def test_prepare_is_exactly_idempotent_and_freezes_nested_data(rig):
    actions, _, _ = rig
    payload = {"body": BODY, "target": {"chat_id": "chat-test"},
               "attachments": [{"file_id": "file-test", "sha256": "hash-test"}]}
    original = prepare(actions, payload=payload)
    assert prepare(actions, payload=payload) == original
    payload["attachments"][0]["file_id"] = "changed-test"
    frozen = actions.snapshot("action-test")
    assert frozen["payload"]["attachments"][0]["file_id"] == "file-test"
    frozen["payload"]["body"] = "changed after snapshot"
    assert actions.get("action-test")["payload"]["body"] == BODY


@pytest.mark.parametrize("change", ["body", "target", "attachment", "source", "task", "recipient", "risk"])
def test_every_material_change_invalidates_the_previous_exact_approval(rig, change):
    actions, _, _ = rig
    first = prepare(actions)
    approve(actions, first)
    ctx, payload, stamp = context(), {**PAYLOAD}, {**STAMP}
    if change == "body":
        ctx = replace(ctx, content_text="请补充原始测试记录。")
        payload["body"] = ctx.content_text
    elif change == "target":
        payload["target"] = {"chat_id": "other-chat-test"}
    elif change == "attachment":
        payload["attachments"] = [{"file_id": "new-file-test", "version": 2}]
    elif change == "source":
        stamp["source_versions"] = {"message-test": 2}
    elif change == "task":
        stamp["task_version"] = 2
    elif change == "recipient":
        ctx = replace(ctx, recipients=(OTHER,))
    else:
        ctx = replace(ctx, sensitivity={"formal_commitment"})
    second = prepare(actions, ctx, payload, stamp)
    assert second["version"] == 2 and second["digest"] != first["digest"]
    assert second["state"] == "awaiting_approval"
    with pytest.raises(ContractError):
        actions.decide("action-test", 1, first["digest"], OWNER)
    assert not actions.can_execute("action-test", second["digest"], stamp, ctx)["allowed"]
    assert not actions.can_execute("action-test", first["digest"], stamp, ctx)["allowed"]


def test_restoring_an_old_draft_does_not_restore_its_old_approval(rig):
    actions, _, _ = rig
    first = prepare(actions)
    approve(actions, first)
    prepare(actions, payload={**PAYLOAD, "attachments": ["new-file-test"]})
    restored = prepare(actions)
    assert restored["version"] == 3 and restored["digest"] != first["digest"]
    assert not actions.can_execute("action-test", restored["digest"], STAMP, context())["allowed"]


@pytest.mark.parametrize("actor", [PEER, replace(OWNER, verified=False),
                                   replace(PEER, display_name="示例负责人", is_owner=True)])
def test_only_verified_owner_can_approve_or_reject(rig, actor):
    actions, _, _ = rig
    item = prepare(actions)
    for approval in (True, False):
        with pytest.raises(ContractError):
            actions.decide("action-test", item["version"], item["digest"], actor, approve=approval)
    assert actions.get("action-test")["state"] == "awaiting_approval"


def test_owner_rejection_is_respected_even_for_an_otherwise_routine_action(rig):
    actions, _, _ = rig
    ctx = context("reply", initiator=PEER, recipients=(PEER,))
    item = prepare(actions, ctx)
    assert item["state"] == "ready"
    actions.decide("action-test", 1, item["digest"], OWNER, approve=False)
    assert not actions.can_execute("action-test", item["digest"], STAMP, ctx)["allowed"]


def test_policy_deny_cannot_be_overridden_by_owner_approval(rig):
    actions, _, _ = rig
    ctx = context("change_policy")
    item = prepare(actions, ctx)
    assert item["decision"] == "deny" and item["state"] == "denied"
    with pytest.raises(ContractError, match="denied"):
        approve(actions, item)
    calls = []
    outcome = actions.execute("action-test", item["digest"], STAMP, ctx, lambda data: calls.append(data))
    assert outcome["executed"] is False and not calls


def test_owner_can_approve_one_referred_action_without_changing_global_permissions(rig):
    actions, _, policy = rig
    ctx = context("reply", initiator=OUTSIDER, recipients=(OUTSIDER,), project_scope="unrelated")
    item = prepare(actions, ctx)
    assert item["state"] == "referred"
    approve(actions, item)
    assert actions.can_execute("action-test", item["digest"], STAMP, ctx)["allowed"]
    other = replace(ctx, action_id="other-action-test")
    assert prepare(actions, other)["state"] == "referred"
    assert policy.evaluate(ctx).outcome == "record_and_refer"


@pytest.mark.parametrize("change", ["source", "task", "actor", "recipient", "content", "scope"])
def test_send_rechecks_current_identity_content_and_source_bindings(rig, change):
    actions, _, _ = rig
    ctx = context()
    item = prepare(actions, ctx)
    approve(actions, item)
    stamp = {**STAMP}
    if change == "source":
        stamp["source_versions"] = {"message-test": 2}
    elif change == "task":
        stamp["task_version"] = 2
    elif change == "actor":
        ctx = replace(ctx, initiator=replace(OWNER, verified=False))
    elif change == "recipient":
        ctx = replace(ctx, recipients=(OTHER,))
    elif change == "content":
        ctx = replace(ctx, content_text="不同的发送正文")
    else:
        ctx = replace(ctx, project_scope="unrelated")
    gate = actions.can_execute("action-test", item["digest"], stamp, ctx)
    assert not gate["allowed"]
    assert "source_stamp_changed" in gate["gate_reasons"] or "action_context_changed" in gate["gate_reasons"]


def test_source_version_true_is_not_the_integer_version_one(rig):
    actions, _, _ = rig
    item = prepare(actions)
    approve(actions, item)
    gate = actions.can_execute("action-test", item["digest"], {**STAMP, "task_version": True}, context())
    assert not gate["allowed"] and "source_stamp_changed" in gate["gate_reasons"]


def test_can_execute_is_read_only(rig):
    actions, store, _ = rig
    item = prepare(actions)
    approve(actions, item)
    before = store.one("SELECT * FROM a5_actions")
    assert actions.can_execute("action-test", item["digest"], STAMP, context())["allowed"]
    assert store.one("SELECT * FROM a5_actions") == before


def test_changed_policy_requires_a_new_action_review(rig):
    actions, _, _ = rig
    ctx = context("reply", initiator=PEER, recipients=(PEER,))
    old = prepare(actions, ctx)
    assert old["state"] == "ready"
    actions.policy = AssistantPolicy(PolicyConfig(owner_user_ids={OWNER.user_id},
                                                 demo_department_ids={"demo-test"},
                                                 leader_user_ids={PEER.user_id}))
    gate = actions.can_execute("action-test", old["digest"], STAMP, ctx)
    assert not gate["allowed"] and "policy_changed" in gate["gate_reasons"]
    with pytest.raises(ContractError, match="policy changed"):
        approve(actions, old)
    revised = prepare(actions, ctx)
    assert revised["version"] == 2 and revised["state"] == "awaiting_approval"


def test_only_one_concurrent_executor_claims_the_action_and_io_is_outside_the_lock(rig):
    actions, store, policy = rig
    item = prepare(actions)
    approve(actions, item)
    other_instance = AssistantActions(Store(store.path), OWNER.user_id, policy)
    entered, release = threading.Event(), threading.Event()
    calls = []

    def executor(frozen):
        with store.transaction() as db:
            store.audit(db, "test.independent_transaction", "synthetic")
        calls.append(frozen["payload"])
        entered.set()
        assert release.wait(3)
        return applied(frozen)

    with ThreadPoolExecutor(max_workers=2) as pool:
        first = pool.submit(actions.execute, "action-test", item["digest"], STAMP, context(), executor)
        try:
            assert entered.wait(3)
            second = other_instance.execute("action-test", item["digest"], STAMP, context(), executor)
            assert second["executed"] is False and second["state"] == "executing"
            with pytest.raises(ContractError):
                prepare(actions, payload={**PAYLOAD, "target": "new-chat"})
        finally:
            release.set()
        assert first.result(timeout=3)["state"] == "applied"
    assert len(calls) == 1


def test_executor_receives_a_detached_frozen_payload(rig):
    actions, _, _ = rig
    item = prepare(actions)
    approve(actions, item)

    def executor(frozen):
        assert frozen["payload"] == PAYLOAD
        assert frozen["source_stamp"] == STAMP
        frozen["payload"]["body"] = "executor-local mutation"
        return applied(frozen)
    actions.execute("action-test", item["digest"], STAMP, context(), executor)
    assert actions.get("action-test")["payload"]["body"] == BODY


@pytest.mark.parametrize("result,expected", [
    ({"status": "applied", "evidence_ref": "receipt"}, "applied_unverified"),
    ({"status": "applied", "evidence_ref": "receipt", "readback_evidence_ref": "readback"}, "applied"),
    ({"status": "applied", "evidence_ref": "same", "readback_evidence_ref": "same"}, "applied_unverified"),
    ({"status": "applied"}, "unknown"),
    ({"status": "not_applied"}, "unknown"),
    ({"status": "unknown", "evidence_ref": "pending-check"}, "unknown"),
    (None, "unknown"),
])
def test_receipt_and_readback_have_separate_truth_and_never_auto_replay(rig, result, expected):
    actions, _, _ = rig
    item = prepare(actions)
    approve(actions, item)
    calls = []

    def executor(frozen):
        calls.append(frozen["digest"])
        return result
    first = actions.execute("action-test", item["digest"], STAMP, context(), executor)
    assert first["state"] == expected
    second = actions.execute("action-test", item["digest"], STAMP, context(), executor)
    assert second["executed"] is False and len(calls) == 1
    assert prepare(actions)["state"] == expected
    with pytest.raises(ContractError):
        prepare(actions, payload={**PAYLOAD, "attachments": ["replacement-file"]})


def test_executor_exception_is_unknown_without_leaking_exception_text_or_body(rig):
    actions, store, _ = rig
    item = prepare(actions)
    approve(actions, item)

    def fail(_):
        raise RuntimeError("SECRET-BODY-CONTENTS: " + BODY)
    result = actions.execute("action-test", item["digest"], STAMP, context(), fail)
    assert result["state"] == "unknown" and result["last_error"] == "executor_exception"
    audits = store.rows("SELECT detail FROM gw_audit")
    assert "SECRET-BODY-CONTENTS" not in str(result) + str(audits)
    assert BODY not in str(result) + str(audits)
    with pytest.raises(ContractError):
        approve(actions, item)


def test_definitely_not_applied_can_retry_without_duplicating_a_known_effect(rig):
    actions, _, _ = rig
    item = prepare(actions)
    approve(actions, item)
    first = actions.execute("action-test", item["digest"], STAMP, context(),
                            lambda _: {"status": "not_applied", "evidence_ref": "remote-definitely-absent"})
    assert first["state"] == "not_applied"
    second = actions.execute("action-test", item["digest"], STAMP, context(), applied)
    assert second["state"] == "applied" and second["attempt_count"] == 2


def test_harness_can_gate_and_record_an_idempotent_verified_effect_without_execute(rig):
    actions, _, _ = rig
    ctx = context("reply", initiator=PEER, recipients=(PEER,))
    item = prepare(actions, ctx)
    assert actions.can_execute("action-test", item["digest"], STAMP, ctx)["allowed"]
    recorded = actions.record_effect("action-test", 1, item["digest"], evidence_ref="receipt",
                                     readback_evidence_ref="readback")
    assert recorded["state"] == "applied"
    assert actions.record_effect("action-test", 1, item["digest"], evidence_ref="receipt",
                                 readback_evidence_ref="readback") == recorded
    with pytest.raises(ContractError):
        actions.record_effect("action-test", 1, item["digest"], status="unknown")


def test_harness_readback_can_upgrade_the_same_receipt_without_a_new_send(rig):
    actions, _, _ = rig
    ctx = context("reply", initiator=PEER, recipients=(PEER,))
    item = prepare(actions, ctx)
    actions.record_effect("action-test", 1, item["digest"], evidence_ref="receipt")
    verified = actions.record_effect("action-test", 1, item["digest"], evidence_ref="receipt",
                                     readback_evidence_ref="readback")
    assert verified["state"] == "applied" and verified["attempt_count"] == 1


def test_record_effect_cannot_bypass_missing_approval_or_resolve_unknown(rig):
    actions, _, _ = rig
    item = prepare(actions)
    with pytest.raises(ContractError):
        actions.record_effect("action-test", 1, item["digest"], evidence_ref="receipt")
    approve(actions, item)
    actions.record_effect("action-test", 1, item["digest"], status="unknown", evidence_ref="ambiguous")
    with pytest.raises(ContractError):
        actions.record_effect("action-test", 1, item["digest"], evidence_ref="receipt", readback_evidence_ref="readback")


def test_unknown_requires_verified_owner_and_specific_exact_reconciliation_evidence(rig):
    actions, _, _ = rig
    item = prepare(actions)
    approve(actions, item)
    actions.execute("action-test", item["digest"], STAMP, context(), lambda _: {"status": "unknown"})
    for actor, version, digest, evidence in [
        (PEER, 1, item["digest"], "checked-absence"),
        (replace(OWNER, verified=False), 1, item["digest"], "checked-absence"),
        (OWNER, 2, item["digest"], "checked-absence"),
        (OWNER, 1, "wrong-digest", "checked-absence"),
        (OWNER, 1, item["digest"], ""),
    ]:
        with pytest.raises(ContractError):
            actions.reconcile("action-test", version, digest, actor, "not_applied", evidence)
    fixed = actions.reconcile("action-test", 1, item["digest"], OWNER, "not_applied", "checked-absence")
    assert fixed["state"] == "not_applied"
    assert actions.execute("action-test", item["digest"], STAMP, context(), applied)["state"] == "applied"


def test_reconciliation_cannot_roll_back_a_verified_applied_effect(rig):
    actions, _, _ = rig
    item = prepare(actions)
    approve(actions, item)
    actions.execute("action-test", item["digest"], STAMP, context(), applied)
    with pytest.raises(ContractError):
        actions.reconcile("action-test", 1, item["digest"], OWNER, "not_applied", "unrelated-new-evidence")


def test_late_executor_result_cannot_overwrite_owner_reconciliation(rig):
    actions, _, _ = rig
    item = prepare(actions)
    approve(actions, item)
    entered, release = threading.Event(), threading.Event()

    def executor(frozen):
        entered.set()
        assert release.wait(3)
        return applied(frozen)
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(actions.execute, "action-test", item["digest"], STAMP, context(), executor)
        try:
            assert entered.wait(3)
            actions.reconcile("action-test", 1, item["digest"], OWNER, "unknown", "owner-investigation-pending")
        finally:
            release.set()
        result = future.result(timeout=3)
    assert result["late_result_ignored"] is True
    assert result["state"] == "unknown" and result["evidence_ref"] == "owner-investigation-pending"


def test_action_approval_and_versions_survive_reopening_store(rig):
    actions, store, policy = rig
    item = prepare(actions)
    approve(actions, item)
    reopened = AssistantActions(Store(store.path), OWNER.user_id, policy)
    assert reopened.can_execute("action-test", item["digest"], STAMP, context())["allowed"]
    assert reopened.get("action-test")["payload"] == PAYLOAD
    assert store.one("SELECT count(*) AS n FROM a5_action_versions")["n"] == 1


def test_get_and_list_are_owner_scoped_and_have_host_metadata(rig):
    actions, store, _ = rig
    item = prepare(actions)
    foreign_owner = Actor("foreign-owner-test", "另一个用户", (), True)
    foreign_policy = AssistantPolicy(PolicyConfig(owner_user_ids={foreign_owner.user_id},
                                                 demo_department_ids={"demo-test"}))
    foreign = AssistantActions(store, foreign_owner.user_id, foreign_policy)
    foreign.prepare(context(initiator=foreign_owner), PAYLOAD, STAMP)
    rows = actions.list(state="awaiting_approval")
    assert len(rows) == 1 and rows[0]["context"]["initiator"]["user_id"] == OWNER.user_id
    assert rows[0]["decision"] == "approval_required"
    assert rows[0]["reason_codes"] == item["reasons"]
    assert not actions.list(state="applied")


@pytest.mark.parametrize("payload", [
    {**PAYLOAD, "body": "body not reviewed by policy"},
    {**PAYLOAD, "target": ""},
    {**PAYLOAD, "target": True},
    {**PAYLOAD, "attachments": {"invalid-value": object()}},
    {**PAYLOAD, "nan": float("nan")},
])
def test_invalid_or_unreviewed_payload_is_not_frozen(rig, payload):
    actions, _, _ = rig
    with pytest.raises(ContractError):
        prepare(actions, payload=payload)
    assert actions.list() == []


def test_observation_does_not_expire_into_autonomous_permission(rig):
    actions, store, _ = rig
    item = prepare(actions)
    store.clock = lambda: 1000.0 + 365 * 86400
    assert not actions.can_execute("action-test", item["digest"], STAMP, context())["allowed"]
    assert prepare(actions)["state"] == "awaiting_approval"
