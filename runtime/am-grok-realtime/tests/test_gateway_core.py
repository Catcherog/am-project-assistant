"""Fault and concurrency tests for the real SQLite inbox/outbox.

These tests use synthetic message contents and fake provider/platform ports.
They are not Grok, Feishu, phone, model-quality, or native latency acceptance.
"""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
import json
import threading

import pytest

from am_grok_gateway.config import GatewayConfig
from am_grok_gateway.core import Gateway, ScopeGuard
from am_grok_gateway.models import (
    AuthContext, BotResult, ContractError, DispatchReceipt, GuardDecision,
    InboundMessage, SendReceipt,
)
from am_grok_gateway.metrics import summarize
from am_grok_gateway.store import Store


ANSWER = "DVT 是设计验证阶段，用试验核对设计是否满足预定要求。"


class Clock:
    def __init__(self):
        self.value = 1_800_000_000.0
        self.lock = threading.Lock()

    def __call__(self):
        with self.lock:
            return self.value

    def advance(self, seconds):
        with self.lock:
            self.value += seconds


class FakeTransport:
    def __init__(self):
        self.calls = []
        self.receipt = DispatchReceipt("accepted", "synthetic-run")
        self.on_submit = None
        self.lock = threading.Lock()

    def submit(self, request):
        with self.lock:
            self.calls.append(json.loads(json.dumps(request)))
        if self.on_submit:
            self.on_submit(request)
        return self.receipt


class FakeSender:
    def __init__(self):
        self.calls = []
        self.status = "sent"
        self.on_send = None
        self.lock = threading.Lock()

    def send(self, message, text, idempotency_key):
        with self.lock:
            self.calls.append((message, text, idempotency_key))
            number = len(self.calls)
        if self.on_send:
            self.on_send(message, text, idempotency_key)
        return SendReceipt(self.status, f"om_synthetic_reply_{number}" if self.status == "sent" else "")


class HostCheck:
    def __init__(self):
        self.allowed = True
        self.useful = True
        self.calls = []
        self.on_check = None

    def __call__(self, message, result):
        self.calls.append((message, result))
        if self.on_check:
            self.on_check(message, result)
        return GuardDecision(self.allowed, self.useful, "CURRENT_SCOPE_REVOKED" if not self.allowed else "")


class FakeHandoff:
    def __init__(self):
        self.calls = []

    def submit(self, message, result, idempotency_key):
        self.calls.append((message, result, idempotency_key))
        return SendReceipt("sent", "am_synthetic_registration")


class Harness:
    def __init__(self, path, *, clock=None, config=None, handoff=None):
        self.clock = clock or Clock()
        self.config = config or GatewayConfig(
            enabled=True, allow_private=True, allowed_chat_ids=("group-a",),
            callback_url="https://callback.invalid/v1/results",
        )
        self.store = Store(path, clock=self.clock)
        self.transport = FakeTransport()
        self.sender = FakeSender()
        self.check = HostCheck()
        self.authorize_allowed = True
        self.on_authorize = None
        self.guard = ScopeGuard(
            self.check, current_authorize=self.authorize, clock=self.clock,
        )
        self.gateway = Gateway(
            self.config, self.store, self.transport, self.sender, self.guard,
            handoff=handoff, clock=self.clock,
        )

    def authorize(self, message):
        if self.on_authorize:
            self.on_authorize(message)
        return GuardDecision(
            self.authorize_allowed,
            reason="" if self.authorize_allowed else "CURRENT_SCOPE_REVOKED",
        )

    def message(self, mid="om_a", **changes):
        value = InboundMessage(
            tenant_id="tenant-test", app_id="app-test", event_id="event-" + mid,
            message_id=mid, chat_id="private-a", chat_type="p2p",
            sender_id="owner-test", text="解释一下 DVT",
            event_created_at=self.clock(), received_at=self.clock(),
            auth=AuthContext(True, self.clock() + 600, "scope-v1", is_owner=True),
        )
        return replace(value, **changes)

    def admit(self, message=None):
        result = self.gateway.admit(message or self.message())
        assert result.handled and result.request_id
        return result.request_id

    def result(self, rid, *, text=ANSWER, kind="answer", **fields):
        row = self.store.get(rid)
        return BotResult(rid, row["attempt_id"], kind, text, **fields).to_dict()

    def ready(self, message=None, **result_fields):
        rid = self.admit(message)
        assert self.gateway.dispatch_once()
        assert self.gateway.accept_result(self.result(rid, **result_fields))["status"] == "accepted"
        return rid


@pytest.fixture
def h(tmp_path):
    return Harness(tmp_path / "gateway.sqlite")


def test_disabled_feature_keeps_existing_host_path_and_creates_no_request(tmp_path):
    h = Harness(tmp_path / "disabled.sqlite", config=GatewayConfig())
    assert not h.gateway.admit(h.message()).handled
    assert not h.gateway.dispatch_once()
    assert not h.gateway.send_once()
    assert h.store.records() == []


@pytest.mark.parametrize("change", [
    {"sender_is_bot": True},
    {"chat_type": "group", "chat_id": "group-a", "mentions_bot": False},
    {"chat_type": "group", "chat_id": "outside-group", "mentions_bot": True},
    {"text": "/doc"},
    {"context": {"route_hint": "am_business"}},
])
def test_noneligible_messages_never_enter_grok(h, change):
    assert not h.gateway.admit(h.message(**change)).handled
    assert h.store.records() == []
    assert h.transport.calls == []


def test_expired_identity_and_old_or_future_events_never_enter_grok(h):
    expired = replace(h.message().auth, expires_at=h.clock())
    unverified = replace(h.message().auth, verified=False)
    for message in (
        h.message(auth=expired), h.message(auth=unverified),
        h.message(event_created_at=h.clock() - 601),
        h.message(event_created_at=h.clock() + 6),
    ):
        assert not h.gateway.admit(message).handled
    assert h.store.records() == []


def test_push_and_poll_race_claim_one_message_across_two_sqlite_connections(h):
    # Separate Store locks ensure this exercises SQLite uniqueness/transactions.
    peer = Harness(h.store.path, clock=h.clock, config=h.config)
    barrier = threading.Barrier(8)

    def admit(index):
        gateway = h.gateway if index % 2 else peer.gateway
        message = h.message(origin="realtime" if index % 2 else "reconcile")
        barrier.wait(timeout=3)
        return gateway.admit(message)

    with ThreadPoolExecutor(max_workers=8) as pool:
        admissions = list(pool.map(admit, range(8)))
    assert all(a.handled for a in admissions)
    assert len({a.request_id for a in admissions}) == 1
    assert len(h.store.records()) == 1
    assert h.gateway.dispatch_once()
    assert not peer.gateway.dispatch_once()
    assert len(h.transport.calls) == 1


def test_concurrent_duplicate_results_and_senders_create_one_effect(h):
    rid = h.admit()
    assert h.gateway.dispatch_once()
    result = h.result(rid)
    barrier = threading.Barrier(8)

    def callback(_):
        barrier.wait(timeout=3)
        return h.gateway.accept_result(result)["status"]

    with ThreadPoolExecutor(max_workers=8) as pool:
        decisions = list(pool.map(callback, range(8)))
    assert decisions.count("accepted") == 1
    assert decisions.count("duplicate") == 7
    with ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(lambda _: h.gateway.send_once(), range(8)))
    assert len(h.sender.calls) == 1
    assert h.sender.calls[0][0].message_id == "om_a"
    assert h.store.get(rid)["status"] == "sent"


def test_conflicting_result_cannot_replace_the_already_accepted_answer(h):
    rid = h.ready()
    assert h.gateway.accept_result(h.result(rid, text="另一份未核对内容"))["status"] == "conflict"
    assert h.gateway.send_once()
    assert [call[1] for call in h.sender.calls] == [ANSWER]


def test_callback_before_webhook_acceptance_does_not_revert_ready_state(h):
    rid = h.admit()

    def respond(request):
        assert h.gateway.mark_started({
            "request_id": request["request_id"], "attempt_id": request["attempt_id"],
        })["status"] == "accepted"
        assert h.gateway.accept_result(h.result(rid))["status"] == "accepted"

    h.transport.on_submit = respond
    assert h.gateway.dispatch_once()
    row = h.store.get(rid)
    assert row["status"] == "ready"
    assert row["started_at"] is not None and row["accepted_at"] is not None
    assert h.gateway.send_once()
    assert len(h.sender.calls) == 1


def test_wrong_attempt_and_unsolicited_request_cannot_create_an_outbox(h):
    rid = h.admit()
    assert h.gateway.dispatch_once()
    wrong = h.result(rid)
    wrong["attempt_id"] = "not-the-issued-attempt"
    assert h.gateway.accept_result(wrong)["status"] == "stale"
    wrong["request_id"] = "never-issued-request"
    assert h.gateway.accept_result(wrong)["status"] == "stale"
    assert not h.gateway.send_once()
    assert h.sender.calls == []
    assert h.gateway.accept_result(h.result(rid))["status"] == "accepted"


def test_provider_cannot_supply_a_target_or_claim_approval(h):
    rid = h.admit()
    assert h.gateway.dispatch_once()
    for field, value in (("recipient_id", "different-person"), ("approved", True), ("need_preview", False)):
        result = h.result(rid)
        result[field] = value
        with pytest.raises(ContractError):
            h.gateway.accept_result(result)
    assert not h.gateway.send_once()
    assert h.sender.calls == []


def test_cancelled_waiting_request_ignores_late_success_and_preserves_stop_truth(h):
    message = h.message()
    rid = h.admit(message)
    assert h.gateway.dispatch_once()
    cancellation = h.gateway.cancel(message.conversation_id, message.sender_id, rid)
    assert cancellation == {"cancelled": 1, "output_in_flight": 0, "provider_stopped": False}
    assert h.gateway.accept_result(h.result(rid))["status"] == "ignored"
    assert not h.gateway.send_once()
    assert h.store.get(rid)["status"] == "cancelled"
    assert h.sender.calls == []


def test_other_sender_cannot_cancel_owner_task(h):
    message = h.message()
    rid = h.admit(message)
    cancellation = h.gateway.cancel(message.conversation_id, "different-person", rid)
    assert cancellation["cancelled"] == 0
    assert h.store.get(rid)["status"] == "queued"


def test_cancel_during_final_guard_prevents_the_external_send(h):
    message = h.message()
    rid = h.ready(message)
    h.check.on_check = lambda *_: h.gateway.cancel(message.conversation_id, message.sender_id, rid)
    assert h.gateway.send_once()
    assert h.sender.calls == []
    assert h.store.get(rid)["status"] == "cancelled"


def test_cancel_after_send_has_started_does_not_claim_the_effect_was_stopped(h):
    message = h.message()
    rid = h.ready(message)
    cancellations = []
    h.sender.on_send = lambda *_: cancellations.append(
        h.gateway.cancel(message.conversation_id, message.sender_id, rid))
    assert h.gateway.send_once()
    assert cancellations[0]["output_in_flight"] == 1
    assert cancellations[0]["provider_stopped"] is False
    row = h.store.get(rid)
    assert row["status"] == "sent" and row["cancel_requested"] == 1
    assert len(h.sender.calls) == 1


def test_cancel_and_new_ingress_do_not_wait_for_blocked_transport(h):
    entered, release = threading.Event(), threading.Event()
    message = h.message()
    rid = h.admit(message)

    def blocked(_):
        entered.set()
        assert release.wait(timeout=3)

    h.transport.on_submit = blocked
    with ThreadPoolExecutor(max_workers=1) as pool:
        running = pool.submit(h.gateway.dispatch_once)
        try:
            assert entered.wait(timeout=2)
            outcome = h.gateway.cancel(message.conversation_id, message.sender_id, rid)
            assert outcome["cancelled"] == 1
            assert h.gateway.admit(h.message("om_other", chat_id="private-b")).handled
        finally:
            release.set()
        assert running.result(timeout=2)
    assert h.store.get(rid)["status"] == "cancelled"


def test_timeout_sends_only_status_and_never_revives_late_answer(h):
    rid = h.admit()
    assert h.gateway.dispatch_once()
    deadline = h.store.get(rid)["deadline_at"]
    late = h.result(rid)
    h.clock.advance(31)
    assert not h.gateway.dispatch_once()
    assert h.gateway.accept_result(late)["status"] in {"ignored", "conflict"}
    assert h.gateway.send_once()
    row = h.store.get(rid)
    assert row["deadline_at"] == deadline
    assert row["result_origin"] == "gateway_notice"
    assert row["error_code"] == "DEADLINE"
    assert row["useful"] == 0
    assert len(h.sender.calls) == 1 and h.sender.calls[0][1] != ANSWER
    assert not h.gateway.send_once()


def test_answer_that_expires_while_ready_is_not_sent_as_current(h):
    rid = h.ready()
    h.clock.advance(31)
    assert h.gateway.send_once()
    assert len(h.sender.calls) == 1
    assert h.sender.calls[0][1] != ANSWER
    assert h.store.get(rid)["result_origin"] == "gateway_notice"
    assert h.store.get(rid)["useful"] == 0


def test_dispatch_unknown_is_not_replayed_but_can_accept_its_own_late_result(h):
    h.transport.receipt = DispatchReceipt("unknown")
    rid = h.admit()
    assert h.gateway.dispatch_once()
    h.store.recover()
    for _ in range(3):
        assert not h.gateway.dispatch_once()
    assert len(h.transport.calls) == 1
    assert h.store.get(rid)["status"] == "dispatch_unknown"
    assert h.gateway.accept_result(h.result(rid))["status"] == "accepted"
    assert h.gateway.send_once()
    assert len(h.sender.calls) == 1


def test_unknown_send_survives_restart_and_duplicate_callback_without_resend(h):
    rid = h.ready()
    result = h.result(rid)
    h.sender.status = "unknown"
    assert h.gateway.send_once()
    peer = Harness(h.store.path, clock=h.clock, config=h.config)
    peer.store.recover()
    assert peer.gateway.accept_result(result)["status"] == "duplicate"
    assert not peer.gateway.dispatch_once()
    assert not peer.gateway.send_once()
    assert peer.store.get(rid)["status"] == "send_unknown"
    assert len(h.sender.calls) == 1 and peer.sender.calls == []
    assert peer.store.get(rid)["useful"] == 0


def test_crash_with_claimed_output_is_recovered_unknown_not_replayed(h):
    rid = h.ready()
    claim = h.store.claim_output()
    assert claim and claim["request_id"] == rid
    # Process dies after claiming; whether the remote send happened is unknown.
    peer = Harness(h.store.path, clock=h.clock, config=h.config)
    assert peer.store.recover() == 1
    assert peer.store.get(rid)["status"] == "send_unknown"
    assert not peer.gateway.send_once()
    assert peer.sender.calls == []


def test_sender_exception_is_unknown_without_leaking_details_or_retrying(h):
    rid = h.ready()

    def fail(*_):
        raise RuntimeError("FAKE_SECRET_SHOULD_NOT_REACH_EVIDENCE")

    h.sender.on_send = fail
    assert h.gateway.send_once()
    row = h.store.get(rid)
    assert row["status"] == "send_unknown"
    assert row["useful"] == 0
    assert "FAKE_SECRET" not in json.dumps(h.store.records())
    assert not h.gateway.send_once()
    assert len(h.sender.calls) == 1


def test_scope_revocation_after_generation_blocks_send(h):
    rid = h.ready()
    h.check.allowed = False
    assert h.gateway.send_once()
    assert h.sender.calls == []
    row = h.store.get(rid)
    assert row["status"] == "blocked" and row["error_code"] == "CURRENT_SCOPE_REVOKED"


def test_identity_expiry_before_output_blocks_even_an_accepted_result(h):
    auth = replace(h.message().auth, expires_at=h.clock() + 2)
    rid = h.ready(h.message(auth=auth))
    h.clock.advance(3)
    assert h.gateway.send_once()
    assert h.sender.calls == []
    assert h.store.get(rid)["error_code"] == "IDENTITY_EXPIRED"


def test_identity_expiry_while_queued_never_discloses_context_to_backend(h):
    auth = replace(
        h.message().auth, expires_at=h.clock() + 2,
        disclosure="project", allowed_source_ids=("ledger-HD17",),
    )
    message = h.message(auth=auth, context={
        "sources": [{"source_id": "ledger-HD17", "revision": "r1", "text": "范围内的合成项目摘要"}],
    })
    h.admit(message)
    h.clock.advance(3)
    h.gateway.dispatch_once()
    assert h.transport.calls == []


def test_scope_revocation_while_queued_prevents_backend_disclosure(h):
    rid = h.admit()
    h.authorize_allowed = False
    h.gateway.dispatch_once()
    assert h.transport.calls == []
    assert h.store.get(rid)["status"] == "blocked"


def test_cancel_during_pre_dispatch_authorization_never_wakes_bot(h):
    message = h.message()
    rid = h.admit(message)
    h.on_authorize = lambda *_: h.gateway.cancel(
        message.conversation_id, message.sender_id, rid)
    h.gateway.dispatch_once()
    assert h.transport.calls == []
    assert h.store.get(rid)["status"] == "cancelled"


@pytest.mark.parametrize("source", [
    {"source_id": "outside-scope", "revision": "r1"},
    {"source_id": "ledger-HD17", "revision": "different-revision"},
])
def test_result_source_and_revision_must_match_authorized_snapshot(h, source):
    auth = replace(h.message().auth, disclosure="project", allowed_source_ids=("ledger-HD17",))
    message = h.message(auth=auth, context={"sources": [{"source_id": "ledger-HD17", "revision": "r1"}]})
    rid = h.ready(message, sources=(source,))
    assert h.gateway.send_once()
    assert h.sender.calls == []
    assert h.store.get(rid)["error_code"] == "SOURCE_BINDING_MISMATCH"
    # Local source binding fails before a permissive host callback is consulted.
    assert h.check.calls == []


def test_unbound_host_guard_cannot_autopublish(h):
    rid = h.ready()
    h.gateway.guard = ScopeGuard(current_authorize=h.authorize, clock=h.clock)
    assert h.gateway.send_once()
    assert h.sender.calls == []
    assert h.store.get(rid)["error_code"] == "HOST_GUARD_UNBOUND"


def test_one_waiting_conversation_does_not_block_another_user_in_same_group(h):
    first = h.message(chat_type="group", chat_id="group-a", mentions_bot=True)
    second = h.message("om_b", chat_type="group", chat_id="group-a", mentions_bot=True, sender_id="peer-test")
    assert first.conversation_id != second.conversation_id
    first_rid = h.admit(first)
    second_rid = h.admit(second)
    assert h.gateway.dispatch_once()
    assert h.gateway.dispatch_once()
    assert {request["request_id"] for request in h.transport.calls} == {first_rid, second_rid}
    assert h.store.get(first_rid)["status"] == h.store.get(second_rid)["status"] == "waiting"


def test_same_conversation_retains_order_across_reconcile_and_realtime(h):
    first_rid = h.admit(h.message(origin="reconcile"))
    second_rid = h.admit(h.message("om_new"))
    assert h.gateway.dispatch_once()
    assert h.transport.calls[0]["request_id"] == first_rid
    assert not h.gateway.dispatch_once()
    assert h.gateway.accept_result(h.result(first_rid))["status"] == "accepted"
    # A ready but not-yet-delivered first answer also preserves turn ordering.
    assert not h.gateway.dispatch_once()
    assert h.gateway.send_once()
    assert h.gateway.dispatch_once()
    assert h.transport.calls[1]["request_id"] == second_rid


def test_realtime_priority_and_background_cap_reserve_foreground_capacity(tmp_path):
    h = Harness(tmp_path / "priority.sqlite", config=GatewayConfig(
        enabled=True, allow_private=True, max_inflight=3, max_reconcile_inflight=1))
    background1 = h.admit(h.message("om_background1", chat_id="bg1", origin="reconcile"))
    background2 = h.admit(h.message("om_background2", chat_id="bg2", origin="reconcile"))
    foreground1 = h.admit(h.message("om_live1", chat_id="live1"))
    assert h.gateway.dispatch_once()
    assert h.transport.calls[0]["request_id"] == foreground1
    assert h.gateway.dispatch_once()
    assert h.transport.calls[1]["request_id"] == background1
    assert not h.gateway.dispatch_once()
    foreground2 = h.admit(h.message("om_live2", chat_id="live2"))
    assert h.gateway.dispatch_once()
    assert h.transport.calls[2]["request_id"] == foreground2
    assert h.store.get(background2)["status"] == "queued"


def test_handoff_only_registers_with_original_business_port(tmp_path):
    handoff = FakeHandoff()
    h = Harness(tmp_path / "handoff.sqlite", handoff=handoff)
    rid = h.ready(kind="handoff", text="可以先准备一份跟进草稿。",
                  handoff={"workflow": "followup", "instruction": "准备跟进草稿，等待具体预览批准"})
    assert h.gateway.send_once()
    assert h.sender.calls == []
    assert len(handoff.calls) == 1
    assert handoff.calls[0][0].message_id == "om_a"
    row = h.store.get(rid)
    assert row["status"] == "handed_off"
    assert row["sent_at"] is None and row["useful"] == 0
    assert h.gateway.accept_result(h.result(
        rid, kind="handoff", text="可以先准备一份跟进草稿。",
        handoff={"workflow": "followup", "instruction": "准备跟进草稿，等待具体预览批准"}))["status"] == "duplicate"
    assert not h.gateway.send_once()


def test_handoff_cannot_fall_through_to_a_regular_reply_without_business_port(h):
    rid = h.ready(kind="handoff", handoff={"workflow": "document", "instruction": "准备文档"})
    assert h.gateway.send_once()
    assert h.sender.calls == []
    assert h.store.get(rid)["error_code"] == "BUSINESS_PORT_UNBOUND"


@pytest.mark.parametrize("text", ["收到", "好的。", "收到，正在处理", "收到，处理中"])
def test_acknowledgement_cannot_be_counted_as_useful_even_if_host_marks_it_useful(h, text):
    rid = h.ready(text=text)
    assert h.gateway.send_once()
    assert h.store.get(rid)["status"] == "sent"
    assert h.store.get(rid)["useful"] == 0


def test_overload_status_notice_does_not_count_as_an_answer(tmp_path):
    h = Harness(tmp_path / "overload.sqlite", config=GatewayConfig(
        enabled=True, allow_private=True, max_pending=1))
    first = h.admit()
    overflow = h.admit(h.message("om_overflow", chat_id="other-conversation"))
    assert h.gateway.send_once()
    row = h.store.get(overflow)
    assert row["result_origin"] == "gateway_notice" and row["useful"] == 0
    assert row["error_code"] == "QUEUE_FULL"
    assert h.store.get(first)["status"] == "queued"
    assert h.transport.calls == []


def test_outbound_request_is_task_specific_and_has_no_execution_grant(h):
    message = h.message(context={"recent_messages": [{"role": "user", "text": "上一条问题"}]})
    rid = h.admit(message)
    assert h.gateway.dispatch_once()
    request = h.transport.calls[0]
    assert request["request_id"] == rid
    assert request["message"]["message_id"] == message.message_id
    assert request["message"]["text"] == message.text
    assert request["context"] == message.context
    assert request["policy"]["side_effects"] == "propose_only"
    assert request["policy"]["direct_feishu_send"] is False
    assert request["policy"]["use_realtime_request_not_full_poll"] is True
    assert request["policy"]["reply_target"] == "bound_original_message"


def test_latency_report_keeps_pending_unknown_and_acknowledgements_in_denominator(h):
    good = h.ready(h.message("om_good", chat_id="good"))
    h.clock.advance(1)
    assert h.gateway.send_once()
    ack = h.ready(h.message("om_ack", chat_id="ack"), text="收到")
    assert h.gateway.send_once()
    unknown = h.ready(h.message("om_unknown", chat_id="unknown"))
    h.sender.status = "unknown"
    assert h.gateway.send_once()
    pending = h.admit(h.message("om_pending", chat_id="pending"))
    assert h.gateway.dispatch_once()
    report = summarize(h.store.records(), minimum_samples=1)
    group = report["groups"]["ordinary"]
    assert group["accepted_unique_requests"] == 4
    assert group["useful_sent"] == 1
    assert group["non_answer_notices"] == 1
    assert group["failed_pending_unknown_or_not_useful"] == 3
    assert group["event_to_send_ack_p95_ms"] is None
    assert group["event_to_visible_p95_ms"] is None
    assert group["verified_visible_answers"] == 0
    assert "MET" not in group["latency_verdict"]
    assert {s["request_id"] for s in group["samples"]} == {good, ack, unknown, pending}


def test_api_acknowledgement_is_not_phone_visibility(h):
    rid = h.ready()
    h.clock.advance(1)
    assert h.gateway.send_once()
    group = summarize(h.store.records(), minimum_samples=1)["groups"]["ordinary"]
    assert group["event_to_send_ack_p50_ms"] == 1000
    assert group["event_to_visible_p50_ms"] is None
    assert group["latency_verdict"] == "INCOMPLETE_OR_FAILED_VISIBLE_RESULTS"
    assert h.store.get(rid)["visible_at"] is None


def test_visibility_evidence_must_bind_actual_message_and_usefulness(h):
    rid = h.ready()
    h.clock.advance(1)
    assert h.gateway.send_once()
    row = h.store.get(rid)
    observation = {
        "request_id": rid, "output_message_id": "wrong-message-id",
        "visible_at": h.clock() + 1, "useful_review": True,
        "evidence_ref": "synthetic-observation-for-contract-test",
    }
    with pytest.raises(ContractError):
        summarize(h.store.records(), visibility=[observation], minimum_samples=1)
    observation["output_message_id"] = row["output_message_id"]
    observation["useful_review"] = False
    group = summarize(h.store.records(), visibility=[observation], minimum_samples=1)["groups"]["ordinary"]
    assert group["verified_visible_answers"] == 0
    assert group["latency_verdict"] == "INCOMPLETE_OR_FAILED_VISIBLE_RESULTS"


def test_synthetic_samples_cannot_pass_native_target_even_with_complete_timestamps(h):
    rid = h.ready(h.message(measurement_class="synthetic"))
    h.clock.advance(1)
    assert h.gateway.send_once()
    row = h.store.get(rid)
    observations = [{
        "request_id": rid, "output_message_id": row["output_message_id"],
        "visible_at": h.clock() + 1, "useful_review": True,
        "evidence_ref": "synthetic-contract-test",
    }]
    group = summarize(h.store.records(), visibility=observations, minimum_samples=1)["groups"]["synthetic"]
    assert group["latency_verdict"] == "SIMULATION_ONLY"


def test_dispatch_unknown_resolved_by_matching_result_can_count_actual_success(h):
    h.transport.receipt = DispatchReceipt("unknown")
    rid = h.admit()
    assert h.gateway.dispatch_once()
    assert h.store.get(rid)["error_code"] == "DISPATCH_UNKNOWN"
    h.clock.advance(1)
    assert h.gateway.accept_result(h.result(rid))["status"] == "accepted"
    assert h.gateway.send_once()
    row = h.store.get(rid)
    assert row["error_code"] == ""
    assert row["useful"] == 1
    group = summarize(h.store.records(), minimum_samples=1)["groups"]["ordinary"]
    assert group["useful_sent"] == 1
    # No phone-visible evidence is inferred from a resolved transport uncertainty.
    assert group["latency_verdict"] == "INCOMPLETE_OR_FAILED_VISIBLE_RESULTS"


def test_valid_phone_visibility_can_precede_http_send_acknowledgement(h):
    rid = h.ready()
    send_start = h.clock()
    h.sender.on_send = lambda *_: h.clock.advance(2)
    assert h.gateway.send_once()
    row = h.store.get(rid)
    assert row["sent_at"] == send_start + 2
    observation = {
        "request_id": rid, "output_message_id": row["output_message_id"],
        "visible_at": send_start + 1, "useful_review": True,
        "evidence_ref": "synthetic-ordering-contract-test",
    }
    group = summarize(h.store.records(), visibility=[observation], minimum_samples=1)["groups"]["ordinary"]
    assert group["verified_visible_answers"] == 1
    assert group["event_to_visible_p50_ms"] == 1000
    assert group["event_to_send_ack_p50_ms"] == 2000


def test_gateway_delay_cannot_be_hidden_by_a_fast_model(h):
    rid = h.admit()
    h.clock.advance(2)
    assert h.gateway.dispatch_once()
    assert h.gateway.accept_result(h.result(rid))["status"] == "accepted"
    h.clock.advance(1)
    assert h.gateway.send_once()
    row = h.store.get(rid)
    observation = {
        "request_id": rid, "output_message_id": row["output_message_id"],
        "visible_at": h.clock(), "useful_review": True,
        "evidence_ref": "synthetic-gateway-delay-test",
    }
    group = summarize(h.store.records(), visibility=[observation], minimum_samples=1)["groups"]["ordinary"]
    assert group["event_to_visible_p50_ms"] == 3000
    assert group["ingress_to_dispatch_p95_ms"] == 2000
    assert group["gateway_target_met"] is False
    assert group["latency_verdict"] == "TARGET_NOT_MET"


def test_duplicate_bot_error_preserves_original_callback_and_sends_one_status_notice(h):
    rid = h.admit()
    assert h.gateway.dispatch_once()
    callback = h.result(
        rid, kind="error", text="合成服务错误",
        error_code="SYNTHETIC_PROVIDER_ERROR",
    )
    assert h.gateway.accept_result(callback)["status"] == "accepted"
    assert h.gateway.accept_result(callback)["status"] == "duplicate"
    before_send = h.store.get(rid)
    assert json.loads(before_send["bot_result_json"]) == callback
    assert before_send["result_origin"] == "gateway_notice"
    assert json.loads(before_send["result_json"])["text"] != callback["text"]
    assert h.gateway.send_once()
    assert h.gateway.accept_result(callback)["status"] == "duplicate"
    assert not h.gateway.send_once()
    row = h.store.get(rid)
    assert json.loads(row["bot_result_json"]) == callback
    assert row["error_code"] == "SYNTHETIC_PROVIDER_ERROR"
    assert row["useful"] == 0
    assert len(h.sender.calls) == 1
