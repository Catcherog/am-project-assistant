"""Independent state/provenance regression checks using synthetic local events.

These are engineering checks, not live Grok or Feishu acceptance.
"""
from dataclasses import replace
import json
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from am_gateway.core import Gateway
from am_gateway.models import Config, ContractError, Event, SendReceipt, dumps
from am_gateway.store import Store


class Clock:
    value = 1_800_000_000.0

    def __call__(self):
        return self.value

    def advance(self, seconds=3):
        self.value += seconds


def make_env(tmp_path):
    clock = Clock()
    config = Config("t", "bot", "owner", "owner-dm", 1,
                    project_chats={"p": ("group",)}, batch_wait_seconds=0)
    gateway = Gateway(Store(tmp_path / "gateway.sqlite", clock=clock), config)
    return gateway, clock


def event(clock, native_id, body="问题", **kwargs):
    base = Event("t", native_id, "group", "user", body, clock(),
                 version_time=clock(), mentions=("bot",))
    return replace(base, **kwargs)


def add(gateway, item):
    gateway.ingest(item)
    gateway.route()
    return item


def claim(gateway, worker="grok_bot"):
    job = gateway.store.claim_job({worker}, gateway.config.job_lease_seconds)
    assert job is not None
    gateway.request(job)
    return gateway.store.one("SELECT * FROM gw_jobs WHERE job_id=?", (job["job_id"],))


def result(job, **fields):
    return {"schema": "am.gateway.result.v2", "job_id": job["job_id"],
            "attempt_id": job["attempt_id"], "status": "completed", **fields}


def finish(gateway, job, **fields):
    return gateway.complete(job["job_id"], job["attempt_id"], result(job, **fields))


def publish_fact(gateway, clock, native_id, value, *, field="delivery", **extra):
    clock.advance()
    source = add(gateway, event(clock, native_id, str(value), mentions=(), project_ids=("p",)))
    job = claim(gateway, "grok_cli")
    update = {"project_id": "p", "field": field, "value": value,
              "source_item_ids": [source.item_id], **extra}
    finish(gateway, job, fact_updates=[update])
    row = gateway.store.one("SELECT * FROM gw_facts WHERE field=?", (field,))
    return source, row


def start_bundle(gateway, clock):
    add(gateway, event(clock, "bundle-request", "分析并制作文档"))
    bot = claim(gateway)
    finish(gateway, bot, decision_mode="submit_task", task_requests=[
        {"task_kind": "project_analysis", "instruction": "分析"},
        {"task_kind": "document_content", "instruction": "整理文档内容"},
    ])
    return bot


def _check_poll_then_mention_enrichment_routes_one_dialogue(env):
    gateway, clock = env
    weak = event(clock, "race", mentions=(), observed_via="poll")
    add(gateway, weak)
    assert not gateway.store.rows("SELECT * FROM gw_jobs WHERE task_kind='dialogue_reply'")
    admission = gateway.ingest(replace(weak, mentions=("bot",), observed_via="event"))
    assert admission.handled
    gateway.route()
    gateway.ingest(replace(weak, mentions=("bot",), observed_via="event"))
    gateway.route()
    replies = gateway.store.rows("SELECT * FROM gw_jobs WHERE task_kind='dialogue_reply'")
    assert len(replies) == 1


def _check_unknown_dialogue_late_result_keeps_real_final(env):
    gateway, clock = env
    add(gateway, event(clock, "late-answer"))
    job = claim(gateway)
    gateway.fail(job["job_id"], job["attempt_id"], "remote_timeout", unknown=True)
    finish(gateway, job, decision_mode="direct_reply", reply_text="已经核实的正式答案。")
    final = gateway.store.one("SELECT * FROM gw_outbox WHERE request_id=? AND purpose='final'",
                             (job["request_id"],))
    assert final["body"] == "已经核实的正式答案。"


def _check_unknown_child_does_not_consume_bundle_final(env):
    gateway, clock = env
    bot = start_bundle(gateway, clock)
    first = claim(gateway, "grok_cli")
    gateway.fail(first["job_id"], first["attempt_id"], "remote_timeout", unknown=True)
    second = claim(gateway, "grok_cli")
    finish(gateway, second, display_text="第二项完成。")
    assert not gateway.store.rows("SELECT * FROM gw_outbox WHERE request_id=? AND purpose='final'",
                                  (bot["request_id"],))
    finish(gateway, first, display_text="第一项晚回完成。")
    final = gateway.store.one("SELECT * FROM gw_outbox WHERE request_id=? AND purpose='final'",
                             (bot["request_id"],))
    assert "第一项晚回完成" in final["body"] and "第二项完成" in final["body"]


def _check_invalid_early_child_is_rejected_before_persisting(env):
    gateway, clock = env
    start_bundle(gateway, clock)
    first = claim(gateway, "grok_cli")
    with unittest.TestCase().assertRaises(ContractError):
        finish(gateway, first, display_text=123)
    persisted = gateway.store.one("SELECT * FROM gw_jobs WHERE job_id=?", (first["job_id"],))
    assert persisted["result_json"] is None
    gateway.fail(first["job_id"], first["attempt_id"], "invalid_display")
    second = claim(gateway, "grok_cli")
    finish(gateway, second, display_text="可交付的第二项。")
    final = gateway.store.one("SELECT * FROM gw_outbox WHERE purpose='final'")
    assert "可交付的第二项" in final["body"]


def _check_twenty_item_context_obeys_serialized_budget(env):
    gateway, clock = env
    config = replace(gateway.config, batch_wait_seconds=2, context_char_budget=16000)
    gateway = Gateway(gateway.store, config)
    for number in range(20):
        gateway.ingest(event(clock, f"batch-{number}", "进展" * 200,
                             mentions=(), project_ids=("p",)))
    gateway.route()
    job = gateway.store.claim_job({"grok_cli"}, config.job_lease_seconds)
    assert job is not None
    context = gateway.context.build(job)
    assert len(dumps(context)) <= config.context_char_budget


def _check_changed_evidence_preview_contains_current_value(env):
    gateway, clock = env
    _, fact = publish_fact(gateway, clock, "first-date", "第一版日期")
    add(gateway, event(clock, "sensitive-query", "请确认交付日期"))
    bot = claim(gateway)
    finish(gateway, bot, decision_mode="require_approval", reply_text="拟按第一版日期交付。",
           cited_fact_ids=[fact["fact_id"]])
    action = gateway.store.one("SELECT * FROM gw_outbox WHERE purpose='final'")
    publish_fact(gateway, clock, "changed-date", "更新后的日期",
                 expected_version=fact["version"], supersedes=True)
    revised = gateway.outbox.review(action["action_id"], actor_id="owner",
                                   draft_version=action["draft_version"], decision="approve")
    assert revised["state"] == "pending_approval"
    card = gateway.store.one("SELECT * FROM gw_outbox WHERE purpose=?",
                             (f"approval_notice:{action['action_id']}:{revised['draft_version']}",))
    assert "更新后的日期" in card["payload_json"]


def _check_new_equal_value_replaces_withdrawn_evidence(env):
    gateway, clock = env
    source, fact = publish_fact(gateway, clock, "old-confirmation", "DVT 完成")
    clock.advance()
    add(gateway, replace(source, is_deleted=True, version_time=clock()))
    assert gateway.store.one("SELECT status FROM gw_facts WHERE fact_id=?", (fact["fact_id"],))["status"] == "withdrawn"
    new_source, current = publish_fact(gateway, clock, "new-confirmation", "DVT 完成")
    assert current["status"] == "known"
    assert new_source.item_id in {ref["item_id"] for ref in json.loads(current["source_refs"])}


def _check_retracted_candidate_cannot_be_promoted_to_known(env):
    gateway, clock = env
    _, old = publish_fact(gateway, clock, "old-value", "日期 A")
    candidate_source, conflicted = publish_fact(gateway, clock, "candidate-value", "日期 B")
    assert conflicted["status"] == "conflicted"
    clock.advance()
    add(gateway, replace(candidate_source, is_deleted=True, version_time=clock()))
    current = gateway.store.one("SELECT * FROM gw_facts WHERE fact_id=?", (old["fact_id"],))
    try:
        gateway.facts.resolve(old["fact_id"], actor_id="owner",
                              version=current["version"], choose_candidate=True)
    except ContractError:
        pass
    resolved = gateway.store.one("SELECT * FROM gw_facts WHERE fact_id=?", (old["fact_id"],))
    assert not (resolved["status"] == "known" and json.loads(resolved["value_json"]) == "日期 B")


def _check_new_conflict_candidate_invalidates_only_related_preview(env):
    gateway, clock = env
    _, first = publish_fact(gateway, clock, "known", "日期 A")
    _, conflicted = publish_fact(gateway, clock, "candidate-one", "日期 B")
    with gateway.store.transaction() as db:
        conv = {"channel": "group", "chat_id": "group", "actor_id": "user"}
        visible = gateway.facts.visible(db, conv)
        deps = {f["fact_id"]: f["fingerprint"] for f in visible}
    publish_fact(gateway, clock, "unrelated", "负责人甲", field="owner")
    with gateway.store.transaction() as db:
        assert not gateway.facts.changed_dependencies(db, deps)
    publish_fact(gateway, clock, "candidate-two", "日期 C")
    with gateway.store.transaction() as db:
        assert first["fact_id"] in gateway.facts.changed_dependencies(db, deps)


def _check_retracting_one_queued_batch_member_preserves_other_inputs(env):
    gateway, clock = env
    config = replace(gateway.config, batch_wait_seconds=2)
    gateway = Gateway(gateway.store, config)
    first = event(clock, "batch-one", "进展 A", mentions=(), project_ids=("p",))
    second = event(clock, "batch-two", "进展 B", mentions=(), project_ids=("p",))
    gateway.ingest(first)
    gateway.ingest(second)
    gateway.route()
    assert len(gateway.store.rows("SELECT * FROM gw_jobs WHERE task_kind='project_state_update'")) == 1
    clock.advance(1)
    add(gateway, replace(first, is_deleted=True, version_time=clock()))
    runnable = gateway.store.rows("SELECT input_refs FROM gw_jobs WHERE state='queued' AND task_kind='project_state_update'")
    remaining_ids = {ref["item_id"] for row in runnable for ref in json.loads(row["input_refs"])}
    assert second.item_id in remaining_ids
    assert first.item_id not in remaining_ids


def _check_referenced_source_edit_invalidates_dependency_before_recompute(env):
    gateway, clock = env
    source, fact = publish_fact(gateway, clock, "editable-source", "日期 A")
    with gateway.store.transaction() as db:
        visible = gateway.facts.visible(db, {"channel": "group", "chat_id": "group", "actor_id": "user"})
        deps = {f["fact_id"]: f["fingerprint"] for f in visible}
    clock.advance()
    # Background re-extraction has not yet run. The original evidence itself
    # already changed, so the old preview cannot still claim unchanged support.
    gateway.ingest(replace(source, body="更正为日期 B", version_time=clock()))
    with gateway.store.transaction() as db:
        assert fact["fact_id"] in gateway.facts.changed_dependencies(db, deps)


def _check_ingested_recall_blocks_send_before_router_tick(env):
    gateway, clock = env
    source = add(gateway, event(clock, "ready-then-recall"))
    bot = claim(gateway)
    finish(gateway, bot, decision_mode="direct_reply", reply_text="即将发送的答案。")
    clock.advance()
    gateway.ingest(replace(source, is_deleted=True, version_time=clock()))

    class Sender:
        calls = 0

        def send(self, action):
            self.calls += 1
            return SendReceipt("sent", "should-not-send")

    sender = Sender()
    gateway.outbox.send_one(sender)
    assert sender.calls == 0
    final = gateway.store.one("SELECT * FROM gw_outbox WHERE purpose='final'")
    assert final["state"] != "sent"


def _check_explicit_other_actor_followup_includes_sent_parent(env):
    gateway, clock = env
    add(gateway, event(clock, "actor-a-question", sender_id="actor-a"))
    bot = claim(gateway)
    finish(gateway, bot, decision_mode="direct_reply", reply_text="已发送的明确答复：节点完成。")

    class Sender:
        def send(self, action):
            return SendReceipt("sent", "sent-parent")

    gateway.outbox.send_one(Sender())
    clock.advance()
    add(gateway, event(clock, "actor-b-followup", "那下一步呢？", sender_id="actor-b",
                       mentions=(), parent_id="sent-parent"))
    followup = claim(gateway)
    context = json.loads(followup["context_json"])
    assert "已发送的明确答复：节点完成。" in dumps(context)


class AdversarialReview(unittest.TestCase):
    """Each check gets a separate durable database and deterministic clock."""

    def setUp(self):
        self.directory = TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.env = make_env(Path(self.directory.name))


def _method(check):
    def run(self):
        check(self.env)
    run.__doc__ = check.__name__.removeprefix("_check_")
    return run


for _name, _check in list(globals().items()):
    if _name.startswith("_check_"):
        setattr(AdversarialReview, _name.replace("_check_", "test_", 1), _method(_check))


if __name__ == "__main__":
    unittest.main()
