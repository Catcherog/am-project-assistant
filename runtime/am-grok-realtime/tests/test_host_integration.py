"""Synthetic tests of host routing, durable ownership and opt-in construction.

No Feishu/Grok API, credential file, websocket, or production host is used.
"""
from __future__ import annotations

from dataclasses import replace
import hashlib
import importlib.util
import json
from pathlib import Path
import sqlite3
import sys
import threading
from types import ModuleType, SimpleNamespace

import pytest

# The integration directory is deliberately deployment glue, not an installed
# replacement for the existing host. The deployment adds this repo root + src.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from integration.am_grok_host import (
    HostBindings, HostEnvelope, HostRuntime, HostSnapshot, OriginalMessageSender,
    R10BusinessFence, create_host_runtime, install,
)
from integration.patch_r10_bridge import EXPECTED, INGEST_ANCHOR, INGEST_REPLACEMENT, generate
from am_grok_gateway.config import GatewayConfig
from am_grok_gateway.core import Gateway, ScopeGuard
from am_grok_gateway.models import (
    AuthContext, BotResult, ContractError, DispatchReceipt, GuardDecision,
)
from am_grok_gateway.store import Store


NOW = 1_800_000_000.0


def event(mid="om_message", *, text="解释一下 DVT", chat_type="p2p", chat="oc_private", user="ou_user", mentioned=False):
    return {
        "header": {"app_id": "cli_app", "tenant_key": "tenant_test", "event_id": "ev_" + mid,
                   "event_type": "im.message.receive_v1"},
        "event": {
            "sender": {"sender_type": "user", "tenant_key": "tenant_test", "sender_id": {"open_id": user}},
            "message": {"message_id": mid, "chat_id": chat, "chat_type": chat_type,
                        "message_type": "text", "content": json.dumps({"text": text}),
                        "create_time": str(int(NOW * 1000)), "verified_bot_mention_for_test": mentioned},
        },
    }


def normalize(raw, **kwargs):
    # This trusted synthetic normalizer substitutes for fs_agent.message_text;
    # production imports the exact existing strict text/post parser.
    return SimpleNamespace(text=json.loads(raw["content"])["text"],
                           bot_mentioned=raw.get("verified_bot_mention_for_test", False))


class Rows:
    def __init__(self):
        self.conn = sqlite3.connect(":memory:")
        self.conn.row_factory = sqlite3.Row
        self.clock = lambda: NOW
        self.conn.executescript("""
            CREATE TABLE inbox(app TEXT, tenant TEXT, message_id TEXT);
            CREATE TABLE am_inbox(id TEXT);
            CREATE TABLE am_sessions(scope TEXT, focus TEXT, pending TEXT, expires REAL);
            CREATE TABLE am_followup_text_actions(nonce TEXT, actor TEXT, chat TEXT);
            CREATE TABLE am_cards(nonce TEXT, message_id TEXT, chat TEXT);
            CREATE TABLE reminder_occurrences(id TEXT, message_id TEXT);
            CREATE TABLE followup_adoptions(occurrence_id TEXT);
        """)

    def rows(self, sql, params=()):
        return list(self.conn.execute(sql, params))


def fake_bridge():
    db = sqlite3.connect(":memory:")
    db.execute("CREATE TABLE inbox(id TEXT)")
    rows = Rows()
    config = SimpleNamespace(app_id="cli_app", tenant_key="tenant_test", bot_open_id="ou_bot",
                             reminders_enabled=False)
    am = SimpleNamespace(
        s=rows, owner="ou_user", collection=False, catch_all_calls=0,
        scope=lambda row: json.dumps([row[k] for k in ("tenant", "app", "chat", "user")]
                                    + ([row["thread"]] if row.get("thread") else []), separators=(",", ":")),
    )
    am.ux_collection_at_receipt = lambda row: {"token": "active"} if am.collection else None
    am.ux_collection_in_chat = lambda row: am.collection
    def catch_all(payload):
        am.catch_all_calls += 1
        return True, True
    am.ingest = catch_all
    return SimpleNamespace(
        db=db, mutex=threading.RLock(), information=None, am_self=am,
        poc=SimpleNamespace(config=config, store=rows, _mode=lambda row: "chat", suspended=False),
        group_ledger=None, wakeup=threading.Event(),
    )


class Harness:
    def __init__(self, tmp_path):
        self.bridge = fake_bridge()
        self.dispatches, self.sends, self.authorizations, self.content_checks, self.snapshots = [], [], [], [], []
        self.authorized, self.output_allowed, self.have_snapshot = True, True, True
        self.business = False
        self.auth = AuthContext(True, NOW + 120, "verified-test-policy")
        self.context = {}
        self.store = Store(tmp_path / "gateway.sqlite", clock=lambda: NOW)
        self.cfg = GatewayConfig(enabled=True, allow_private=True,
                                 allowed_chat_ids=("oc_group",), allowed_sender_ids=("ou_user",),
                                 callback_url="https://callback.invalid/v1/results")
        self.bindings = HostBindings(self.snapshot, self.authorize, self.check,
                                     lambda envelope: self.business)
        self.sender = OriginalMessageSender(self.reply)
        self.gateway = Gateway(self.cfg, self.store, self, self.sender,
                               ScopeGuard(self.check, current_authorize=self.authorize, clock=lambda: NOW),
                               clock=lambda: NOW)
        self.fence = R10BusinessFence(self.bridge, explicit_text=lambda value: value.startswith("/"))
        self.runtime = HostRuntime(self.gateway, self.bindings, app_id="cli_app", tenant_id="tenant_test",
                                   bot_open_id="ou_bot", normalize=normalize, business_fence=self.fence,
                                   clock=lambda: NOW)
        self.bridge.grok_realtime = self.runtime

    def snapshot(self, envelope):
        self.snapshots.append(envelope)
        return HostSnapshot(self.auth, self.context) if self.have_snapshot else None

    def authorize(self, message):
        self.authorizations.append(message)
        return GuardDecision(self.authorized, reason="CURRENT_SCOPE_REVOKED")

    def check(self, message, result):
        self.content_checks.append(result)
        return GuardDecision(self.output_allowed, self.output_allowed, "FACTS_UNVERIFIED")

    def submit(self, request):
        self.dispatches.append(request)
        return DispatchReceipt("accepted", "test_run")

    def reply(self, mid, text):
        self.sends.append((mid, text))
        return "om_real_api_receipt"

    def result(self, rid):
        return BotResult(rid, self.store.get(rid)["attempt_id"], "answer", "DVT 是设计验证阶段。").to_dict()


@pytest.fixture
def h(tmp_path):
    value = Harness(tmp_path)
    yield value
    value.gateway.stop()
    value.bridge.db.close()
    value.bridge.am_self.s.conn.close()


def run_patched_ingress(bridge, payload):
    # Execute the actual patch's insertion in the original local AM ordering.
    # The old AM stub deliberately captures EVERY message, reproducing R10's
    # flags.query/conversation.wants behavior which broke a post-AM hook.
    original = ("def ingest(self, payload):\n" + INGEST_ANCHOR
                + "            handled, accepted = self.am_self.ingest(payload)\n"
                + "            if handled:\n                return accepted\n"
                + "        return False\n")
    namespace = {}
    exec(compile(original.replace(INGEST_ANCHOR, INGEST_REPLACEMENT), "pinned-ingress-hook", "exec"), namespace)
    return namespace["ingest"](bridge, payload)


@pytest.mark.parametrize("kind,chat,mentioned", [("p2p", "oc_private", False), ("group", "oc_group", True)])
def test_pre_am_hook_takes_ordinary_message_even_when_am_catches_all(h, kind, chat, mentioned):
    assert run_patched_ingress(h.bridge, event(chat_type=kind, chat=chat, mentioned=mentioned))
    assert h.bridge.am_self.catch_all_calls == 0
    assert len(h.store.records()) == 1
    # Admission performs no provider, Feishu send, or current network guard.
    assert not h.dispatches and not h.sends and not h.authorizations
    assert h.gateway.dispatch_once()
    rid = h.store.records()[0]["request_id"]
    h.gateway.accept_result(h.result(rid))
    assert h.gateway.send_once()
    assert h.sends == [("om_message", "DVT 是设计验证阶段。")]


@pytest.mark.parametrize("text", ["/doc logic", "/chat", "/progress", "/cancel", "/remind", "/info"])
def test_existing_explicit_commands_keep_am_priority(h, text):
    assert run_patched_ingress(h.bridge, event(text=text))
    assert h.bridge.am_self.catch_all_calls == 1
    assert not h.snapshots and not h.store.records()


@pytest.fixture
def r10_controls(monkeypatch):
    """Use the verbatim, fingerprinted R10 parser with no external source path."""
    fixture = Path(__file__).parent / "fixtures" / "r10_control_parser.py"
    provenance = json.loads(fixture.with_suffix(".json").read_text())
    assert provenance["source_sha256"] == EXPECTED[provenance["source_relative_path"]]
    assert hashlib.sha256(fixture.read_bytes()).hexdigest() == provenance["fixture_sha256"]
    package = ModuleType("am_harness")
    package.__path__ = []
    spec = importlib.util.spec_from_file_location("am_harness.controls", fixture)
    module = importlib.util.module_from_spec(spec)
    monkeypatch.setitem(sys.modules, "am_harness", package)
    monkeypatch.setitem(sys.modules, "am_harness.controls", module)
    spec.loader.exec_module(module)
    return module


@pytest.mark.parametrize("text,operation", [
    ("停止当前任务", "cancel"), ("取消当前任务", "cancel"),
    ("停止当前生成", "cancel"), ("停止生成。", "cancel"), ("停止生成！", "cancel"),
    ("取消任务 run_A-1", "cancel"), ("停止任务 abc_9!", "cancel"),
    ("更正当前任务：改成比较方案 A 与 B", "revise"),
    ("修改当前任务:保留来源", "revise"), ("当前任务改为：只给出结论", "revise"),
    ("更正任务 run_1：增加依据", "revise"), ("修改任务 run_2:缩短为三条", "revise"),
    (" 更正当前任务：第一条\n第二条 ", "revise"),
])
def test_actual_r10_control_grammar_retains_original_control_path(h, r10_controls, text, operation):
    assert r10_controls.parse_control(text).operation == operation
    # Use the default classifier, not the slash-only shortcut in other fixtures.
    # No pending/focus is present, so the parser itself must preserve control.
    h.runtime.business_fence = R10BusinessFence(h.bridge)
    assert run_patched_ingress(h.bridge, event(text=text))
    assert h.runtime.last_route_reason == "existing_am_workflow"
    assert h.bridge.am_self.catch_all_calls == 1
    assert not h.snapshots and not h.store.records() and not h.dispatches


@pytest.mark.parametrize("kind", ["am_receipt", "poc_receipt", "legacy_receipt", "poc_mode", "collection", "pending", "focus", "followup_parent", "native_quote"])
def test_local_business_ownership_is_not_stolen(h, kind):
    am, payload = h.bridge.am_self, event()
    conn = am.s.conn
    if kind == "am_receipt":
        conn.execute("INSERT INTO am_inbox VALUES('om_message')")
    elif kind == "poc_receipt":
        conn.execute("INSERT INTO inbox VALUES('cli_app','tenant_test','om_message')")
    elif kind == "legacy_receipt":
        h.bridge.db.execute("INSERT INTO inbox VALUES('om_message')")
    elif kind == "poc_mode":
        h.bridge.poc._mode = lambda row: "poc"
    elif kind == "collection":
        am.collection = True
    elif kind in {"pending", "focus"}:
        scope = json.dumps(["tenant_test", "cli_app", "oc_private", "ou_user"], separators=(",", ":"))
        conn.execute("INSERT INTO am_sessions VALUES(?,?,?,?)",
                     (scope, "project_x" if kind == "focus" else None,
                      "choose" if kind == "pending" else None, NOW + 60))
    else:
        payload["event"]["message"]["parent_id"] = "om_business_preview"
        if kind == "followup_parent":
            conn.execute("INSERT INTO reminder_occurrences VALUES('occ','om_business_preview')")
            conn.execute("INSERT INTO followup_adoptions VALUES('occ')")
        else:
            conn.execute("INSERT INTO am_cards VALUES('nonce','om_business_preview','oc_private')")
            conn.execute("INSERT INTO am_followup_text_actions VALUES('nonce','ou_user','oc_private')")
    assert not h.runtime.ingest(payload).handled
    assert h.runtime.last_route_reason == "existing_am_workflow"
    assert not h.store.records()


def test_unrelated_parent_does_not_make_every_thread_a_business_flow(h):
    payload = event()
    payload["event"]["message"]["parent_id"] = "om_unrelated"
    assert h.runtime.ingest(payload).handled


def test_legacy_qwen_pause_does_not_block_grok_chat_lane(h):
    h.bridge.poc.suspended = True
    h.bridge.poc.provider_pause = "WORKER_FAILED"
    h.bridge.resource_window = SimpleNamespace(owner="training")
    assert h.runtime.ingest(event()).handled
    assert h.gateway.dispatch_once()
    assert len(h.dispatches) == 1


def test_active_session_isolated_by_chat_and_person(h):
    scope = json.dumps(["tenant_test", "cli_app", "oc_other", "ou_other"], separators=(",", ":"))
    h.bridge.am_self.s.conn.execute("INSERT INTO am_sessions VALUES(?,?,?,?)", (scope, "x", "y", NOW + 60))
    assert h.runtime.ingest(event()).handled


def test_duplicate_keeps_gateway_ownership_after_new_am_state_and_auth_expiry(h):
    first = h.runtime.ingest(event())
    h.have_snapshot, h.business = False, True
    h.bridge.am_self.collection = True
    replay = h.runtime.ingest(event(text="/doc changed duplicate"), origin="reconcile")
    assert replay.handled and replay.status == "duplicate" and replay.request_id == first.request_id
    assert len(h.store.records()) == 1
    assert h.gateway.dispatch_once()
    assert not h.gateway.dispatch_once()
    assert len(h.dispatches) == 1


def test_cold_snapshot_keeps_legacy_path_and_records_no_grok_success(h):
    h.have_snapshot = False
    assert run_patched_ingress(h.bridge, event())
    assert h.bridge.am_self.catch_all_calls == 1
    assert h.runtime.last_route_reason == "host_snapshot_unavailable_legacy_path"
    assert not h.store.records() and not h.dispatches


def test_routing_metrics_expose_cold_cache_coverage_gap_without_content_or_ids(h, monkeypatch):
    assert h.runtime.ingest(event()).handled
    assert h.runtime.ingest(event()).status == "duplicate"
    h.have_snapshot = False
    assert not h.runtime.ingest(event("om_cold")).handled
    assert not h.runtime.ingest(event("om_command", text="/doc")).handled
    h.have_snapshot = True
    def unavailable(message):
        raise RuntimeError("synthetic storage failure")
    monkeypatch.setattr(h.gateway, "admit", unavailable)
    with pytest.raises(RuntimeError):
        h.runtime.ingest(event("om_error"))
    metrics = h.runtime.routing_metrics()
    assert (metrics["ingress_calls"], metrics["admitted"], metrics["duplicates"],
            metrics["legacy_path"], metrics["errors"]) == (5, 1, 1, 2, 1)
    assert metrics["legacy_reasons"] == {"host_snapshot_unavailable_legacy_path": 1, "existing_am_workflow": 1}
    encoded = json.dumps(metrics)
    assert "ou_user" not in encoded and "om_message" not in encoded and "DVT" not in encoded
    # Callers cannot mutate the internal counters through the returned mapping.
    metrics["legacy_reasons"].clear()
    assert len(h.runtime.routing_metrics()["legacy_reasons"]) == 2


def test_expired_snapshot_is_not_dispatched(h):
    h.auth = replace(h.auth, expires_at=NOW)
    result = h.runtime.ingest(event())
    assert not result.handled and "expired" in result.reason
    assert not h.store.records()


def test_host_current_authorize_is_rechecked_before_context_can_leave(h):
    result = h.runtime.ingest(event())
    h.authorized = False
    assert h.gateway.dispatch_once()
    assert h.store.get(result.request_id)["status"] == "blocked"
    assert not h.dispatches and not h.sends


def test_host_fact_guard_is_bound_before_reply(h):
    result = h.runtime.ingest(event())
    h.gateway.dispatch_once()
    h.gateway.accept_result(h.result(result.request_id))
    h.output_allowed = False
    h.gateway.send_once()
    assert not h.sends and h.store.get(result.request_id)["status"] == "blocked"


def test_identity_and_context_claims_in_raw_event_are_ignored(h):
    payload = event()
    payload["auth"] = {"verified": True, "disclosure": "project", "is_owner": True}
    payload["context"] = {"sources": [{"source_id": "secret", "revision": "1"}]}
    admitted = h.runtime.ingest(payload)
    saved = json.loads(h.store.get(admitted.request_id)["message_json"])
    assert saved["auth"]["is_owner"] is False
    assert saved["context"] == {}


@pytest.mark.parametrize("change", ["tenant", "app", "sender_type", "sender_tenant", "unmentioned_group", "sender_scope"])
def test_sdk_event_scope_is_not_replaced_by_payload_claims(h, change):
    payload = event()
    if change == "tenant": payload["header"]["tenant_key"] = "other"
    elif change == "app": payload["header"]["app_id"] = "other"
    elif change == "sender_type": payload["event"]["sender"]["sender_type"] = "app"
    elif change == "sender_tenant": payload["event"]["sender"]["tenant_key"] = "other"
    elif change == "sender_scope": payload["event"]["sender"]["sender_id"]["open_id"] = "ou_other"
    else:
        payload = event(chat="oc_group", chat_type="group", mentioned=False, text="@bot DVT?")
    assert not h.runtime.ingest(payload).handled
    assert not h.store.records()


def test_persistence_error_propagates_instead_of_falling_back_to_am(h, monkeypatch):
    original = h.gateway.admit
    def committed_then_raised(message):
        original(message)
        raise RuntimeError("synthetic post-commit failure")
    monkeypatch.setattr(h.gateway, "admit", committed_then_raised)
    with pytest.raises(RuntimeError, match="post-commit"):
        run_patched_ingress(h.bridge, event())
    assert h.bridge.am_self.catch_all_calls == 0
    # A platform retry does not invoke the failing admit again or fall through.
    assert not run_patched_ingress(h.bridge, event())
    assert h.bridge.am_self.catch_all_calls == 0 and len(h.store.records()) == 1


def test_trusted_recall_cancels_only_original_bound_request(h):
    first = h.runtime.ingest(event())
    other = h.runtime.ingest(event("om_other", chat="oc_another"))
    recalled = {"header": {"app_id": "cli_app", "tenant_key": "tenant_test", "event_type": "im.message.recalled_v1"},
                "event": {"message_id": "om_message", "chat_id": "oc_private"}}
    assert h.runtime.recall(recalled)["handled"]
    assert h.store.get(first.request_id)["status"] == "cancelled"
    assert h.store.get(other.request_id)["status"] == "queued"
    assert h.gateway.accept_result(h.result(first.request_id))["status"] != "accepted"


def test_unbound_recall_cannot_cancel_another_scope(h):
    first = h.runtime.ingest(event())
    recalled = {"header": {"app_id": "cli_app", "tenant_key": "tenant_test", "event_type": "im.message.recalled_v1"},
                "event": {"message_id": "om_message", "chat_id": "oc_wrong"}}
    assert not h.runtime.recall(recalled)["handled"]
    assert h.store.get(first.request_id)["status"] == "queued"


def test_sender_uses_original_target_only_and_has_no_auto_retry(h):
    admitted = h.runtime.ingest(event())
    message = h.snapshots[0]
    h.gateway.dispatch_once()
    result = BotResult(admitted.request_id, h.store.get(admitted.request_id)["attempt_id"], "answer", '<at user_id="all">所有人</at> DVT').to_dict()
    h.gateway.accept_result(result)
    h.gateway.send_once()
    assert len(h.sends) == 1 and h.sends[0][0] == message.message_id
    assert "<at" not in h.sends[0][1] and "</at" not in h.sends[0][1]
    assert h.store.get(admitted.request_id)["readback_at"] is None


def test_sender_exception_is_unknown_and_invoked_once(h):
    calls = []
    def ambiguous(mid, text):
        calls.append(mid)
        raise TimeoutError("no response")
    h.sender.reply = ambiguous
    admitted = h.runtime.ingest(event())
    h.gateway.dispatch_once()
    h.gateway.accept_result(h.result(admitted.request_id))
    h.gateway.send_once()
    assert not h.gateway.send_once()
    assert calls == ["om_message"]
    assert h.store.get(admitted.request_id)["status"] == "send_unknown"


def host_config(tmp_path, **settings):
    return {"app_id": "cli_app", "state_directory": str(tmp_path / "host-state"), "grok_realtime": {
        "enabled": True, "allow_private": True, "allowed_sender_ids": ["ou_user"],
        "webhook_url": "https://bot.invalid/wake", "callback_url": "https://callback.invalid/v1/results",
        **settings,
    }}


KEYS = {"AM_GROK_DISPATCH_KEY": "synthetic-dispatch-key-0123456789012345",
        "AM_GROK_CALLBACK_KEY": "synthetic-callback-key-0123456789012345"}


def test_factory_disabled_requires_no_profile_key_db_socket_or_import(tmp_path):
    assert create_host_runtime(bridge=None, config={}, reply=None, bindings=None, normalize=None, env={}) is None
    assert install(None, {}, None) is None
    assert list(tmp_path.iterdir()) == []


def test_factory_refuses_unbound_authority_even_with_valid_network_settings(h, tmp_path):
    with pytest.raises(ContractError, match="HOST_PROFILE_UNBOUND"):
        create_host_runtime(bridge=h.bridge, config=host_config(tmp_path), reply=h.reply,
                            bindings=None, normalize=normalize, env=KEYS)
    with pytest.raises(ContractError, match="HOST_PROFILE_UNBOUND"):
        install(h.bridge, host_config(tmp_path), h.reply)


@pytest.mark.parametrize("bad_url", ["", "http://callback.invalid/v1/results", "http://127.0.0.1:8789/v1/results",
                                     "https://callback.invalid/v1/result", "https://callback.invalid/v1/results?token=secret"])
def test_factory_requires_explicit_reachable_callback_route(h, tmp_path, bad_url):
    with pytest.raises(ContractError):
        create_host_runtime(bridge=h.bridge, config=host_config(tmp_path, callback_url=bad_url), reply=h.reply,
                            bindings=h.bindings, normalize=normalize, env=KEYS)


def test_factory_refuses_same_hmac_key_in_both_directions(h, tmp_path):
    with pytest.raises(ContractError, match="DIRECTION_KEYS_MUST_DIFFER"):
        create_host_runtime(bridge=h.bridge, config=host_config(tmp_path), reply=h.reply,
                            bindings=h.bindings, normalize=normalize,
                            env=dict.fromkeys(KEYS, "synthetic-shared-key-012345678901234567"))


def test_factory_is_constructible_without_starting_workers_or_listeners(h, tmp_path):
    runtime = create_host_runtime(bridge=h.bridge, config=host_config(tmp_path), reply=h.reply,
                                  bindings=h.bindings, normalize=normalize, env=KEYS, clock=lambda: NOW)
    assert runtime.gateway.config.callback_url == "https://callback.invalid/v1/results"
    assert runtime.gateway.guard.current_authorize == h.bindings.current_authorize
    assert runtime.gateway.guard.current_check == h.bindings.check_result
    assert runtime.gateway._thread is None
    with pytest.raises(RuntimeError, match="not running"):
        _ = runtime.callbacks.address
    assert not h.sends and not h.dispatches


def test_hash_bound_patch_rejects_unverified_or_newer_host_without_writing(tmp_path):
    candidate = tmp_path / "local-qwen3.8" / "feishu-bridge" / "bridge.py"
    candidate.parent.mkdir(parents=True)
    candidate.write_text("# synthetic newer host\n")
    with pytest.raises(ValueError, match="SOURCE_HASH_MISMATCH"):
        generate(tmp_path)
    assert candidate.read_text() == "# synthetic newer host\n"
