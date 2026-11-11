from concurrent.futures import ThreadPoolExecutor

import pytest

from am_gateway.assistant.workbook import (
    AccessDenied, AssistantWorkbook, IdentityConflict, WorkbookError,
)


OWNER = "owner-feishu-id"
ALICE = "alice-demo-id"
BOB = "bob-demo-id"


@pytest.fixture
def workbook(tmp_path):
    clock = [1000.0]
    book = AssistantWorkbook(tmp_path / "assistant.sqlite", OWNER, clock=lambda: clock[0])
    for origin, actor, principal in (
        ("feishu", OWNER, OWNER), ("gpt_dot", "owner-dot", OWNER),
        ("grok_bot", "owner-grok", OWNER), ("feishu", ALICE, ALICE),
        ("feishu", BOB, BOB),
    ):
        book.bind_identity(origin, actor, principal, "host-verified:" + actor, authorized_by=OWNER)
    book.grant_project(ALICE, "HD", authorized_by=OWNER, can_write=True)
    book.grant_project(BOB, "HD", authorized_by=OWNER, can_write=False)
    book.clock_values = clock
    return book


def make_task(book, **kwargs):
    params = dict(event_key="source-message-1", origin="feishu", actor_id=ALICE,
                  title="核对测试进展", project_id="HD", source_ref="feishu:message-1")
    params.update(kwargs)
    return book.accept_task(**params)


def plan(book, task_id, **kwargs):
    params = dict(followup_key="report:2026-11-10", recipient_id=ALICE, due_at=1100,
                  prompt="请补充报告进展", principal_id=OWNER, source_ref="plan:HD")
    params.update(kwargs)
    return book.plan_followup(task_id, **params)


def test_bound_owner_continues_same_task_across_three_entries(workbook):
    task = make_task(workbook, origin="gpt_dot", actor_id="owner-dot")
    second = make_task(workbook, event_key="f2", actor_id=OWNER, task_id=task["task_id"], title="刚才那件事补一句")
    third = make_task(workbook, event_key="g3", origin="grok_bot", actor_id="owner-grok",
                      task_id=task["task_id"], title="进展怎么样了")
    assert task["task_id"] == second["task_id"] == third["task_id"]
    assert len(workbook.owner_brief(principal_id=OWNER)["tasks"]) == 1
    assert len(workbook.get_task(task["task_id"], principal_id=OWNER, include_events=True)["events"]) == 3


def test_unbound_identity_cannot_impersonate_owner_by_actor_name(workbook):
    with pytest.raises(AccessDenied):
        make_task(workbook, origin="gpt_dot", actor_id=OWNER)
    with pytest.raises(AccessDenied):
        workbook.bind_identity("gpt_dot", "impostor", OWNER, "self-claim", authorized_by=ALICE)


def test_identity_cannot_be_rebound_to_another_principal(workbook):
    with pytest.raises(IdentityConflict):
        workbook.bind_identity("gpt_dot", "owner-dot", ALICE, "new-claim", authorized_by=OWNER)
    workbook.revoke_identity("gpt_dot", "owner-dot", authorized_by=OWNER)
    with pytest.raises(AccessDenied):
        make_task(workbook, origin="gpt_dot", actor_id="owner-dot")


def test_same_title_does_not_merge_independent_tasks(workbook):
    first = make_task(workbook)
    second = make_task(workbook, event_key="source-message-2")
    assert first["task_id"] != second["task_id"]


def test_accept_task_replay_and_concurrent_ingress_are_idempotent(workbook):
    def accept(_):
        return make_task(workbook)["task_id"]
    with ThreadPoolExecutor(max_workers=8) as pool:
        ids = list(pool.map(accept, range(16)))
    assert len(set(ids)) == 1
    assert len(workbook.get_task(ids[0], principal_id=OWNER, include_events=True)["events"]) == 1


def test_event_identity_cannot_change_payload(workbook):
    make_task(workbook)
    with pytest.raises(IdentityConflict):
        make_task(workbook, title="不同的要求")


def test_verified_bindings_and_work_survive_restart(workbook):
    task = make_task(workbook)
    reopened = AssistantWorkbook(workbook.path, OWNER)
    assert reopened.resolve_identity("gpt_dot", "owner-dot").principal_id == OWNER
    assert reopened.get_task(task["task_id"], principal_id=ALICE)["title"] == task["title"]
    with pytest.raises(IdentityConflict):
        AssistantWorkbook(workbook.path, "different-owner")


def test_task_access_does_not_cross_projects_or_private_visibility(workbook):
    private = make_task(workbook, actor_id=OWNER, visibility="private")
    with pytest.raises(AccessDenied):
        workbook.get_task(private["task_id"], principal_id=BOB)
    with pytest.raises(AccessDenied):
        make_task(workbook, event_key="new", actor_id=BOB, project_id="SECRET")
    with pytest.raises(AccessDenied):
        workbook.project_brief("SECRET", principal_id=ALICE)
    assert workbook.tasks_for_principal(BOB) == []


def test_project_read_grant_does_not_grant_write_permission(workbook):
    task = make_task(workbook)
    assert workbook.get_task(task["task_id"], principal_id=BOB)["task_id"] == task["task_id"]
    with pytest.raises(AccessDenied):
        workbook.transition(task["task_id"], "blocked", principal_id=BOB, source_ref="msg:bob")
    with pytest.raises(AccessDenied):
        workbook.record_fact("HD", "status", "changed", principal_id=BOB,
                             source_ref="msg:bob", event_key="f-bob")
    workbook.revoke_project(BOB, "HD", authorized_by=OWNER)
    with pytest.raises(AccessDenied):
        workbook.get_task(task["task_id"], principal_id=BOB)


def test_unrelated_request_is_recorded_for_owner_and_not_executed(workbook):
    task = make_task(workbook, related=False, project_id="OTHER")
    assert task["state"] == "awaiting_owner"
    assert workbook.owner_brief(principal_id=OWNER)["needs_owner"][0]["task_id"] == task["task_id"]
    with pytest.raises(AccessDenied):
        workbook.transition(task["task_id"], "in_progress", principal_id=ALICE, source_ref="msg:start")
    with pytest.raises(WorkbookError):
        plan(workbook, task["task_id"])
    with pytest.raises(AccessDenied):
        workbook.decide_unrelated(task["task_id"], principal_id=ALICE, accept=True, source_ref="own-decision")
    accepted = workbook.decide_unrelated(task["task_id"], principal_id=OWNER, accept=True, source_ref="owner:approval")
    assert accepted["state"] == "pending"


def test_owner_can_decline_unrelated_request_without_losing_record(workbook):
    task = make_task(workbook, related=False, project_id=None)
    denied = workbook.decide_unrelated(task["task_id"], principal_id=OWNER, accept=False, source_ref="owner:declined")
    assert denied["state"] == "cancelled"
    assert workbook.tasks_for_principal(OWNER) == []
    assert workbook.tasks_for_principal(OWNER, include_terminal=True)[0]["task_id"] == task["task_id"]


def test_feedback_complete_is_not_verified_completion(workbook):
    task = make_task(workbook)
    workbook.record_reply(task["task_id"], "reply1", "feishu", ALICE, "测试完成了",
                          source_ref="feishu:reply1", claims_complete=True)
    recorded = workbook.get_task(task["task_id"], principal_id=OWNER)
    assert recorded["state"] == "reported_complete"
    assert recorded["verification_ref"] == ""
    with pytest.raises(WorkbookError):
        workbook.transition(task["task_id"], "completed", principal_id=OWNER, source_ref="feedback-only")
    with pytest.raises(AccessDenied):
        workbook.transition(task["task_id"], "completed", principal_id=ALICE,
                            source_ref="msg:self", verification_ref="self-claim")
    verified = workbook.transition(task["task_id"], "completed", principal_id=OWNER,
                                   source_ref="readback:report", verification_ref="report-hash:verified")
    assert verified["state"] == "completed"


def test_terminal_task_requires_explicit_owner_reopening(workbook):
    task = make_task(workbook)
    workbook.transition(task["task_id"], "cancelled", principal_id=OWNER, source_ref="owner:cancel")
    with pytest.raises(WorkbookError):
        workbook.transition(task["task_id"], "pending", principal_id=OWNER, source_ref="retry")
    with pytest.raises(AccessDenied):
        workbook.reopen(task["task_id"], principal_id=ALICE, source_ref="self")
    assert workbook.reopen(task["task_id"], principal_id=OWNER, source_ref="owner:resume")["state"] == "pending"


def test_steps_and_failure_events_retain_sources(workbook):
    task = make_task(workbook)
    step = workbook.add_step(task["task_id"], "collect-report", "收集测试报告",
                             principal_id=OWNER, assignee_id=ALICE, source_ref="plan:1")
    repeated = workbook.add_step(task["task_id"], "collect-report", "收集测试报告",
                                 principal_id=OWNER, assignee_id=ALICE, source_ref="plan:1")
    assert step["step_id"] == repeated["step_id"]
    workbook.update_step(step["step_id"], "blocked", principal_id=OWNER, source_ref="reply:missing-data")
    event = workbook.append_event(task["task_id"], "failed-attempt-1", principal_id=OWNER,
                                  kind="failed", text="报告链接暂时不可访问", source_ref="attempt:1")
    replay = workbook.append_event(task["task_id"], "failed-attempt-1", principal_id=OWNER,
                                   kind="failed", text="报告链接暂时不可访问", source_ref="attempt:1")
    assert event["event_id"] == replay["event_id"]
    assert workbook.get_task(task["task_id"], principal_id=OWNER)["steps"][0]["state"] == "blocked"


def test_plan_report_and_verified_facts_remain_distinct(workbook):
    workbook.record_fact("HD", "completion", "11月20日", principal_id=OWNER,
                         source_ref="approved-plan", event_key="plan", status="plan", category="plan")
    workbook.record_fact("HD", "completion", "同事说完成", principal_id=ALICE,
                         source_ref="message:done", event_key="reported")
    with pytest.raises(WorkbookError):
        workbook.record_fact("HD", "completion", "已验收", principal_id=OWNER,
                             source_ref="message:done", event_key="not-verified", status="verified")
    workbook.record_fact("HD", "completion", "报告已核对", principal_id=OWNER,
                         source_ref="report:hash", event_key="verified", status="verified", verification_ref="independent-readback")
    brief = workbook.project_brief("HD", principal_id=OWNER)
    assert brief["plans"][0]["value"] == "11月20日"
    assert {fact["status"] for fact in brief["progress"]} == {"reported", "verified"}
    assert brief["external_write_performed"] is False


def test_fact_replay_preserves_history_and_rejects_changed_content(workbook):
    args = dict(principal_id=OWNER, source_ref="msg:one", event_key="fact-one", observed_at=900)
    original = workbook.record_fact("HD", "status", "等待反馈", **args)
    assert workbook.record_fact("HD", "status", "等待反馈", **args)["fact_id"] == original["fact_id"]
    with pytest.raises(IdentityConflict):
        workbook.record_fact("HD", "status", "已经完成", **args)
    new = workbook.record_fact("HD", "status", "开始测试", principal_id=OWNER,
                               source_ref="msg:two", event_key="fact-two", observed_at=1000,
                               supersedes_fact_id=original["fact_id"])
    assert workbook.project_brief("HD", principal_id=OWNER)["progress"][0]["fact_id"] == new["fact_id"]
    with pytest.raises(WorkbookError):
        workbook.record_fact("HD", "status", "旧消息", principal_id=OWNER,
                             source_ref="msg:old", event_key="old", observed_at=800, supersedes_fact_id=new["fact_id"])


def test_project_brief_includes_blockers_risks_and_review_sources(workbook):
    task = make_task(workbook, related=False)
    for category, status, value in (("blocker", "reported", "缺样件"),
                                    ("risk", "reported", "可能影响节点"),
                                    ("progress", "conflict", "两个来源日期不同")):
        workbook.record_fact("HD", category, value, principal_id=OWNER, source_ref="source:" + category,
                             event_key=category, category=category, status=status)
    brief = workbook.project_brief("HD", principal_id=ALICE)
    assert brief["blockers"][0]["source_ref"] == "source:blocker"
    assert brief["risks"][0]["value"] == "可能影响节点"
    assert brief["pending_review"][0]["status"] == "conflict"
    assert brief["pending_owner_tasks"][0]["task_id"] == task["task_id"]


def test_candidate_feedback_cannot_supersede_verified_fact(workbook):
    verified = workbook.record_fact("HD", "status", "已核对报告", principal_id=OWNER,
        source_ref="report:1", event_key="confirmed", status="verified", verification_ref="readback:1")
    with pytest.raises(WorkbookError):
        workbook.record_fact("HD", "status", "同事说取消了", principal_id=ALICE,
            source_ref="message:2", event_key="candidate", status="reported",
            supersedes_fact_id=verified["fact_id"])
    assert workbook.project_brief("HD", principal_id=OWNER)["progress"][0]["fact_id"] == verified["fact_id"]


def test_followup_due_produces_only_stable_approval_proposal(workbook):
    task = make_task(workbook)
    plan(workbook, task["task_id"])
    assert workbook.due_followups(1099, principal_id=OWNER) == []
    proposals = workbook.due_followups(1100, principal_id=OWNER)
    assert len(proposals) == 1
    assert proposals[0]["state"] == "awaiting_owner"
    assert proposals[0]["delivery_ref"] == ""
    assert workbook.due_followups(99999999, principal_id=OWNER)[0]["proposal_id"] == proposals[0]["proposal_id"]
    # Observation time passing never upgrades authority or marks a send.
    assert workbook.get_task(task["task_id"], principal_id=OWNER)["state"] == "pending"


def test_followup_proposal_is_unique_across_concurrent_instances(workbook):
    task = make_task(workbook)
    plan(workbook, task["task_id"])
    other = AssistantWorkbook(workbook.path, OWNER)
    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(lambda i: (workbook if i % 2 else other).due_followups(1200, principal_id=OWNER), range(16)))
    assert len({result[0]["proposal_id"] for result in results}) == 1


def test_followup_delivery_requires_owner_approval_and_receipt(workbook):
    task = make_task(workbook)
    plan(workbook, task["task_id"])
    proposal = workbook.due_followups(1100, principal_id=OWNER)[0]
    with pytest.raises(AccessDenied):
        workbook.record_followup_delivery(proposal["proposal_id"], principal_id=ALICE,
                                           approval_ref="pretend", delivery_ref="msg:1")
    with pytest.raises(WorkbookError):
        workbook.record_followup_delivery(proposal["proposal_id"], principal_id=OWNER,
                                           approval_ref="", delivery_ref="msg:1")
    sent = workbook.record_followup_delivery(proposal["proposal_id"], principal_id=OWNER,
                                             approval_ref="owner-approved-action-hash", delivery_ref="feishu:sent-1")
    assert sent["state"] == "sent"
    assert workbook.due_followups(999999, principal_id=OWNER) == []
    with pytest.raises(IdentityConflict):
        workbook.record_followup_delivery(proposal["proposal_id"], principal_id=OWNER,
                                           approval_ref="owner-approved-action-hash", delivery_ref="different-send")


def test_valid_reply_resolves_only_that_person_and_task_followups(workbook):
    task = make_task(workbook)
    first = plan(workbook, task["task_id"])
    plan(workbook, task["task_id"], followup_key="bob-report", recipient_id=BOB)
    workbook.due_followups(1100, principal_id=OWNER)
    workbook.record_reply(task["task_id"], "reply", "feishu", ALICE, "报告明天补齐",
                          source_ref="feishu:reply", followup_id=first["followup_id"], at=1200)
    pending = workbook.due_followups(1300, principal_id=OWNER)
    assert len(pending) == 1 and pending[0]["recipient_id"] == BOB
    assert workbook.get_task(task["task_id"], principal_id=OWNER)["state"] == "pending"


def test_reply_replay_is_idempotent_and_does_not_silence_new_cycle(workbook):
    task = make_task(workbook)
    plan(workbook, task["task_id"])
    event = workbook.record_reply(task["task_id"], "reply1", "feishu", ALICE, "处理中", source_ref="reply:1", at=1200)
    workbook.clock_values[0] = 1300
    plan(workbook, task["task_id"], followup_key="next-cycle", due_at=1400)
    replay = workbook.record_reply(task["task_id"], "reply1", "feishu", ALICE, "处理中", source_ref="reply:1", at=1200)
    assert replay["event_id"] == event["event_id"]
    assert len(workbook.due_followups(1500, principal_id=OWNER)) == 1


def test_stale_reply_does_not_override_new_work_or_silence_new_followup(workbook):
    task = make_task(workbook)
    workbook.clock_values[0] = 1400
    workbook.transition(task["task_id"], "in_progress", principal_id=OWNER, source_ref="new-work")
    plan(workbook, task["task_id"], due_at=1500)
    workbook.record_reply(task["task_id"], "late-reply", "feishu", ALICE, "之前那版完成了",
                          source_ref="old:message", claims_complete=True, at=1200)
    assert workbook.get_task(task["task_id"], principal_id=OWNER)["state"] == "in_progress"
    assert len(workbook.due_followups(1600, principal_id=OWNER)) == 1


def test_followup_recipient_can_reply_without_reading_private_task(workbook):
    task = make_task(workbook, actor_id=OWNER, visibility="private")
    followup = plan(workbook, task["task_id"], recipient_id=BOB)
    with pytest.raises(AccessDenied):
        workbook.get_task(task["task_id"], principal_id=BOB, include_events=True)
    reply = workbook.record_reply(task["task_id"], "bob-reply", "feishu", BOB,
                                  "报告已补充", source_ref="msg:bob", followup_id=followup["followup_id"], at=1200)
    assert reply["principal_id"] == BOB
    assert workbook.due_followups(1300, principal_id=OWNER) == []


def test_closing_task_cancels_pending_followup_without_implying_send(workbook):
    task = make_task(workbook)
    plan(workbook, task["task_id"])
    proposal = workbook.due_followups(1100, principal_id=OWNER)[0]
    workbook.transition(task["task_id"], "cancelled", principal_id=OWNER, source_ref="owner:cancel")
    assert workbook.due_followups(1200, principal_id=OWNER) == []
    with pytest.raises(WorkbookError):
        workbook.record_followup_delivery(proposal["proposal_id"], principal_id=OWNER,
                                           approval_ref="old-approval", delivery_ref="late-send")


def test_owner_brief_orders_priorities_and_keeps_overdue_as_fact(workbook):
    low = make_task(workbook, priority=3, due_at=900)
    high = make_task(workbook, event_key="urgent", priority=0, due_at=1100)
    brief = workbook.owner_brief(principal_id=OWNER)
    assert [t["task_id"] for t in brief["tasks"]] == [high["task_id"], low["task_id"]]
    assert [t["task_id"] for t in brief["overdue"]] == [low["task_id"]]
    assert brief["tasks"][1]["state"] == "pending"
    with pytest.raises(AccessDenied):
        workbook.owner_brief(principal_id=ALICE)
