"""Real StudioSender request construction, with synthetic SDK responses only."""
from dataclasses import replace
import json
from pathlib import Path
import sys
import time
from types import SimpleNamespace as NS
from unittest.mock import Mock

import pytest
from lark_oapi.api.im.v1 import CreateMessageRequest, GetMessageRequest, ReplyMessageRequest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT.parent / "FS-AGENT-GROK-POC-20261017-01_R1_Studio" / "src"))

from am_gateway.assistant.policy import ActionContext, Actor
from am_gateway.collectors import CollectorEngine, SourceSpec
from am_gateway.collection_service import CollectionService
from am_gateway.models import Config, Event, dumps
from am_gateway.phase1 import Harness, HarnessConfig, Policy
from am_gateway.phase1.agents import ProposalRepository
from am_gateway.phase1.contracts import registry
from am_gateway.phase1.execution import ReceiverPump
from am_gateway.phase1.receiver import DurableReceiver, LocalReceiverAdapter
from am_gateway.phase1.service import RuntimeService
from am_gateway.store import Store
from integration.assistant_host import bind_assistant
from integration.assistant_identity import StudioAssistantDirectory, DEMOScope
from integration.studio_host import BindingError, ScopedReadTransport
from integration.studio_phase1_factory import HostAuthority, IdentityReaders
from integration.studio_phase1_factory import build_phase1_bundle
from integration.phase1_runtime import install as install_candidate


OWNER, PEER, OTHER = "ou_owner", "ou_peer", "ou_other"
DEMO = "od_demo_test"


class SyntheticSDK:
    def __init__(self):
        self.messages, self.reply_targets = {}, {"om_peer_inbound": "oc_peer"}
        self.bad_readback = None
        self.im = NS(v1=NS(message=NS(create=Mock(side_effect=self._create),
                                    reply=Mock(side_effect=self._reply),
                                    get=Mock(side_effect=self._get))))
        self.request = Mock(side_effect=AssertionError("collector network not expected"))

    def _save(self, request, chat, parent=""):
        mid = "om_synthetic_" + str(len(self.messages) + 1)
        body = request.request_body
        self.messages[mid] = NS(message_id=mid, chat_id=chat, deleted=False,
            msg_type=body.msg_type, parent_id=parent, thread_id="",
            sender=NS(open_bot_id=None, id="cli_test", sender_type="app"),
            body=NS(content=body.content), create_time="1794272400123")
        return NS(success=lambda: True, data=NS(message_id=mid))

    def _create(self, request):
        assert isinstance(request, CreateMessageRequest)
        return self._save(request, request.request_body.receive_id)

    def _reply(self, request):
        assert isinstance(request, ReplyMessageRequest)
        return self._save(request, self.reply_targets[request.message_id], request.message_id)

    def _get(self, request):
        assert isinstance(request, GetMessageRequest)
        original = self.messages[request.message_id]
        message = NS(**vars(original))
        if self.bad_readback:
            name, value = self.bad_readback
            setattr(message, name, value)
        return NS(success=lambda: True, data=NS(items=[message]))


@pytest.fixture
def host(tmp_path, monkeypatch):
    import lark_oapi.ws.client
    ws_start = Mock(side_effect=AssertionError("a second consumer must not start"))
    monkeypatch.setattr(lark_oapi.ws.client.Client, "start", ws_start)
    monkeypatch.setattr("socket.socket.connect", Mock(side_effect=AssertionError("network forbidden")))
    sdk = SyntheticSDK()
    cfg = HarnessConfig("tenant-test", "ou_bot", OWNER, "oc_owner")
    collection = Config(cfg.tenant_id, cfg.bot_id, OWNER, cfg.owner_chat_id, 0,
                        project_chats={"HD": ("oc_hd",)})
    store = Store(tmp_path / "runtime.sqlite", clock=lambda: 1000.0)
    harness = Harness(store, cfg)
    ingress = NS(gateway=harness)
    bridge = NS(config={"app_id": "cli_test"}, user_allowed=lambda uid: uid in {OWNER, PEER, OTHER},
        am_self=NS(query_settings={"denied_users": []},
            query_identity=NS(verify=Mock(return_value=None)),
            ledger=NS(port=NS(disclosure={"oc_hd": ["HD"]}, guard=Mock(return_value=None)))))
    settings = {"send_live": True, "poll_seconds": 57, "group_chat_ids": ["oc_hd"],
                "project_chats": {"HD": ["oc_hd"]},
                "assistant": {"demo_department_ids": [DEMO], "leader_user_ids": [],
                              "protected_person_ids": [], "routine_fields": {}}}
    actors = {PEER: Actor(PEER, "DEMO同事", (DEMO,), True),
              OTHER: Actor(OTHER, "另一位同事", (DEMO,), True)}
    directory = StudioAssistantDirectory(bridge, sdk, cfg,
        {**settings["assistant"], "group_chat_ids": settings["group_chat_ids"],
         "project_chats": settings["project_chats"]}, store, resolver=lambda uid: actors[uid])
    scope = DEMOScope(bridge, cfg, settings["group_chat_ids"], directory)
    authority = HostAuthority(bridge, cfg, scope, settings, collection)
    authority.directory, authority.harness = directory, harness
    reader = ScopedReadTransport(sdk, ["oc_owner", "oc_hd"], app_id="cli_test",
                                  bot_id=cfg.bot_id, tenant_id=cfg.tenant_id)
    engine = CollectorEngine(store, collection)
    for chat in ("oc_owner", "oc_hd"):
        engine.register_source(SourceSpec(cfg.tenant_id, chat, identity="am_bot",
            channel="bot_dm" if chat == "oc_owner" else "group", poll_seconds=57))
    service = NS(harness=harness, ingress=ingress, collector=CollectionService(engine),
                 proposals=ProposalRepository(store, OWNER),
                 receivers=[object()], servers=[object()], threads=[])
    bundle = NS(authority=authority, readers=IdentityReaders({"am_bot": reader}))
    return NS(service=service, bundle=bundle, bridge=bridge, settings=settings, sdk=sdk,
              root=tmp_path, directory=directory, store=store, cfg=cfg, reader=reader,
              actors=actors, ws_start=ws_start)


def bind(host, *, live=True, executor=None):
    return bind_assistant(host.service, host.bundle, host.bridge,
                          {**host.settings, "send_live": live}, host.sdk, host.root, executor)


def bind_peer(host, coordinator):
    event = Event(host.cfg.tenant_id, "om_peer_inbound", "oc_peer", PEER, "测试项目进展",
                  1000, channel="bot_dm")
    host.directory.register_dm(event)
    coordinator.on_dm_bound(event)
    return event


def make_action(host, coordinator, *, kind="reply", chat="oc_peer", body="已经收到你的测试反馈。",
                action_id="a-test", parent="om_peer_inbound"):
    peer = host.directory.resolve(PEER)
    owner = host.directory.resolve(OWNER)
    target = {"chat_id": chat, "actor_id": PEER, "channel": "group" if chat == "oc_hd" else "bot_dm"}
    if parent:
        target["reply_to_message_id"] = parent
    ctx = ActionContext(action_id, kind, peer if kind == "reply" else owner,
        recipients=(peer,), project_scope="demo", passive_reply=kind == "reply", content_text=body)
    item = coordinator.actions.prepare(ctx, {"body": body, "target": target, "artifacts": []}, {"version": 1})
    if item["decision"] != "allow":
        coordinator.actions.decide(item["action_id"], item["version"], item["digest"], owner)
    return ctx, coordinator.actions.snapshot(item["action_id"])


def notice(coordinator, *, approval=False, body='别人原话 <at id="all"></at> 不应变成可执行卡片'):
    metadata = {"kind": "approval", "action_id": "a-approved", "version": 3,
                "digest": "a" * 64} if approval else {}
    return coordinator.reports.enqueue_notice("notice-test", title="需要你了解", body=body,
        now=1000, priority="P0", metadata=metadata)


def test_binding_reuses_existing_consumer_and_does_no_sdk_work(host):
    receivers, servers = host.service.receivers[:], host.service.servers[:]
    coordinator = bind(host)
    assert host.service.assistant is host.service.ingress.assistant is coordinator
    assert host.service.collector.collectors.assistant is coordinator
    assert host.service.receivers == receivers and host.service.servers == servers
    assert host.service.threads == []
    host.ws_start.assert_not_called()
    host.sdk.im.v1.message.create.assert_not_called()
    host.sdk.im.v1.message.reply.assert_not_called()
    host.sdk.request.assert_not_called()
    assert coordinator.policy.config.protected_name_aliases >= {"示例主管甲", "示例主管乙"}


def test_send_live_false_injects_no_senders_and_leaves_reports_pending(host):
    business_executor = Mock(side_effect=AssertionError("business writes must remain off"))
    coordinator = bind(host, live=False, executor=business_executor)
    queued = notice(coordinator)
    assert coordinator.report_sender is None and coordinator.action_executor is None
    assert coordinator.maintenance.executor is None
    assert coordinator.maintenance.status()["status"] == "awaiting_binding"
    assert coordinator.reports_once() == 0
    assert coordinator.reports.get_report(queued["report_id"])["state"] == "ready"
    host.sdk.im.v1.message.create.assert_not_called()
    host.sdk.im.v1.message.reply.assert_not_called()
    host.sdk.im.v1.message.get.assert_not_called()
    business_executor.assert_not_called()


def test_missing_directory_is_an_error_instead_of_relaxed_scope(host):
    host.bundle.authority.directory = None
    with pytest.raises(BindingError, match="verified_directory"):
        bind(host)
    assert not hasattr(host.service, "assistant")


def test_verified_dm_extends_only_existing_bot_reader_and_existing_collector(host):
    coordinator = bind(host, live=False)
    event = bind_peer(host, coordinator)
    assert host.reader.chats == frozenset({"oc_owner", "oc_hd", "oc_peer"})
    rows = host.store.rows("SELECT spec_json FROM gw_sources")
    peer = next(json.loads(row["spec_json"]) for row in rows if json.loads(row["spec_json"])["native_id"] == "oc_peer")
    assert peer["identity"] == "am_bot" and peer["channel"] == "bot_dm" and peer["poll_seconds"] == 57
    coordinator.on_dm_bound(event)
    assert len(host.store.rows("SELECT * FROM gw_sources")) == 3
    host.ws_start.assert_not_called()
    host.sdk.request.assert_not_called()


def test_dm_collection_cannot_be_granted_by_unregistered_or_mismatched_events(host):
    coordinator = bind(host, live=False)
    event = Event(host.cfg.tenant_id, "om_unknown", "oc_unknown", PEER, "声称已验证", 1000, channel="bot_dm")
    with pytest.raises(BindingError, match="registered_dm"):
        coordinator.on_dm_bound(event)
    original = bind_peer(host, coordinator)
    with pytest.raises(BindingError, match="registered_dm"):
        coordinator.on_dm_bound(replace(original, sender_id=OTHER))
    assert "oc_unknown" not in host.reader.chats


def test_owner_report_uses_plain_text_exact_target_and_independent_readback(host):
    coordinator = bind(host)
    notice(coordinator)
    report = coordinator.reports.claim("synthetic-host-test", now=1000)
    result = coordinator.report_sender(report)
    assert result["status"] == "sent" and result["evidence_ref"].startswith("feishu:readback:")
    request = host.sdk.im.v1.message.create.call_args.args[0]
    assert isinstance(request, CreateMessageRequest)
    assert request.receive_id_type == "chat_id" and request.request_body.receive_id == "oc_owner"
    rendered = json.loads(request.request_body.content)
    assert rendered["body"]["elements"][0]["text"]["tag"] == "plain_text"
    assert '<at id="all"></at>' in rendered["body"]["elements"][0]["text"]["content"]
    assert "platform_time" not in result
    assert host.sdk.im.v1.message.get.call_count == 1
    coordinator.report_sender(report)
    assert host.sdk.im.v1.message.create.call_count == 1


def test_approval_card_buttons_bind_exact_action_version_and_digest(host):
    coordinator = bind(host)
    notice(coordinator, approval=True)
    report = coordinator.reports.claim("synthetic-host-test", now=1000)
    assert coordinator.report_sender(report)["status"] == "sent"
    card = json.loads(host.sdk.im.v1.message.create.call_args.args[0].request_body.content)
    buttons = card["body"]["elements"][1:]
    values = [button["behaviors"][0]["value"] for button in buttons]
    assert values == [
        {"kind": "assistant_action", "action_id": "a-approved", "version": 3,
         "digest": "a" * 64, "approve": True},
        {"kind": "assistant_action", "action_id": "a-approved", "version": 3,
         "digest": "a" * 64, "approve": False},
    ]
    assert all(button["text"]["tag"] == "plain_text" for button in buttons)


def test_scope_referral_has_exact_acceptance_buttons_but_intake_errors_do_not(host):
    coordinator = bind(host)
    report = coordinator.reports.enqueue_notice("scope-test", title="是否承接", body="项目外请求",
        now=1000, priority="P0", metadata={"kind": "referral", "task_id": "task-test", "scope_digest": "b" * 64})
    report = coordinator.reports.claim("synthetic-host-test", now=1000)
    assert coordinator.report_sender(report)["status"] == "sent"
    card = json.loads(host.sdk.im.v1.message.create.call_args.args[0].request_body.content)
    values = [item["behaviors"][0]["value"] for item in card["body"]["elements"][1:]]
    assert values == [
        {"kind": "assistant_scope", "task_id": "task-test", "scope_digest": "b" * 64, "accept": True},
        {"kind": "assistant_scope", "task_id": "task-test", "scope_digest": "b" * 64, "accept": False},
    ]
    error = coordinator.reports.enqueue_notice("error-test", title="信息待核对", body="未通过身份查询",
        now=1000, priority="P0", metadata={"kind": "referral"})
    error = coordinator.reports.claim("synthetic-host-test", now=1000)
    assert coordinator.report_sender(error)["status"] == "sent"
    error_card = json.loads(host.sdk.im.v1.message.create.call_args.args[0].request_body.content)
    assert len(error_card["body"]["elements"]) == 1


def test_report_target_cannot_be_replaced_with_a_group(host):
    coordinator = bind(host)
    report = notice(coordinator)
    report["target"] = {"chat_id": "oc_hd", "actor_id": OWNER, "channel": "group"}
    with pytest.raises(BindingError, match="owner_target"):
        coordinator.report_sender(report)
    host.sdk.im.v1.message.create.assert_not_called()


def test_local_demo_reply_keeps_exact_parent_chat_and_stable_uuid(host):
    coordinator = bind(host)
    bind_peer(host, coordinator)
    ctx, frozen = make_action(host, coordinator)
    result = coordinator.actions.execute(frozen["action_id"], frozen["digest"], {"version": 1},
                                         ctx, coordinator.action_executor)
    assert result["state"] == "applied"
    assert result["evidence_ref"].startswith("feishu:message:")
    assert result["readback_evidence_ref"].startswith("feishu:readback:")
    request = host.sdk.im.v1.message.reply.call_args.args[0]
    assert request.message_id == "om_peer_inbound"
    assert request.request_body.msg_type == "interactive"
    card = json.loads(request.request_body.content)
    assert card["body"]["elements"][0]["text"] == {"tag": "plain_text", "content": ctx.content_text}
    assert host.sdk.im.v1.message.create.call_count == 0
    coordinator.action_executor(coordinator.actions.snapshot(frozen["action_id"]))
    assert host.sdk.im.v1.message.reply.call_count == 1


def test_group_followup_mentions_only_verified_frozen_recipients(host):
    coordinator = bind(host)
    body = '<at id="all"></at> 请反馈测试结果。'
    ctx, frozen = make_action(host, coordinator, kind="proactive_followup", chat="oc_hd", body=body, parent="")
    result = coordinator.actions.execute(frozen["action_id"], frozen["digest"], {"version": 1}, ctx,
                                         coordinator.action_executor)
    assert result["state"] == "applied"
    request = host.sdk.im.v1.message.create.call_args.args[0]
    assert request.request_body.receive_id == "oc_hd" and request.request_body.msg_type == "interactive"
    card = json.loads(request.request_body.content)
    assert card["body"]["elements"][0]["tag"] == "markdown"
    assert card["body"]["elements"][0]["content"] == '<at id="ou_peer"></at>'
    assert card["body"]["elements"][1]["text"] == {"tag": "plain_text", "content": body}


@pytest.mark.parametrize("bad_readback", [
    ("chat_id", "oc_wrong"),
    ("body", NS(content='{"text":"different"}')),
    ("sender", NS(open_bot_id=None, id="other_app", sender_type="app")),
])
def test_mismatched_independent_readback_is_unknown_and_not_resent(host, bad_readback):
    coordinator = bind(host)
    bind_peer(host, coordinator)
    ctx, frozen = make_action(host, coordinator)
    host.sdk.bad_readback = bad_readback
    first = coordinator.actions.execute(frozen["action_id"], frozen["digest"], {"version": 1}, ctx,
                                        coordinator.action_executor)
    assert first["state"] == "unknown"
    second = coordinator.actions.execute(frozen["action_id"], frozen["digest"], {"version": 1}, ctx,
                                         coordinator.action_executor)
    assert second["executed"] is False and host.sdk.im.v1.message.reply.call_count == 1


def test_uncertain_sdk_send_never_repeats_create_or_reply(host):
    coordinator = bind(host)
    bind_peer(host, coordinator)
    ctx, frozen = make_action(host, coordinator)
    host.sdk.im.v1.message.reply.side_effect = TimeoutError("private body must not escape")
    outcome = coordinator.actions.execute(frozen["action_id"], frozen["digest"], {"version": 1}, ctx,
                                          coordinator.action_executor)
    assert outcome["state"] == "unknown"
    assert "private body" not in str(outcome)
    assert coordinator.action_executor(coordinator.actions.snapshot(frozen["action_id"]))["status"] == "unknown"
    assert host.sdk.im.v1.message.reply.call_count == 1


def test_report_confirmed_unsent_retries_new_attempt_and_preserves_unknown_receipt(host):
    coordinator = bind(host)
    queued = notice(coordinator)
    host.sdk.im.v1.message.create.side_effect = TimeoutError("synthetic lost receipt")
    assert coordinator.reports_once() == 0
    first = coordinator.reports.get_report(queued["report_id"])
    assert first["state"] == "unknown" and first["attempt_no"] == 1
    assert coordinator.reports_once() == 0
    assert host.sdk.im.v1.message.create.call_count == 1
    first_receipt = host.store.rows("SELECT * FROM studio_send_receipts")[0]
    assert first_receipt["platform_id"] is None
    ready = coordinator.handle("POST", "/v1/assistant/reports/" + queued["report_id"] + "/reconcile",
        {"outcome": "confirmed_unsent", "attempt_no": 1,
         "evidence_ref": "synthetic-independent-lookup:confirmed-no-message"}, actor=OWNER)
    assert ready["state"] == "ready"
    host.sdk.im.v1.message.create.side_effect = host.sdk._create
    assert coordinator.reports_once() == 1
    sent = coordinator.reports.get_report(queued["report_id"])
    assert sent["state"] == "sent" and sent["attempt_no"] == 2
    assert host.sdk.im.v1.message.create.call_count == 2
    assert host.sdk.im.v1.message.get.call_count == 1
    receipts = host.store.rows("SELECT * FROM studio_send_receipts")
    assert len(receipts) == 2 and first_receipt in receipts
    assert len({row["uuid"] for row in receipts}) == 2
    assert coordinator.reports_once() == 0
    assert host.sdk.im.v1.message.create.call_count == 2


def test_report_transport_rejects_unclaimed_or_forged_attempt(host):
    coordinator = bind(host)
    queued = notice(coordinator)
    with pytest.raises(BindingError, match="attempt_not_claimed"):
        coordinator.report_sender(queued)
    claimed = coordinator.reports.claim("synthetic-host-test", now=1000)
    with pytest.raises(BindingError, match="attempt_not_claimed"):
        coordinator.report_sender({**claimed, "attempt_no": claimed["attempt_no"] + 1})
    host.sdk.im.v1.message.create.assert_not_called()


def test_action_owner_negative_reconciliation_retries_new_attempt_without_losing_receipt(host):
    coordinator = bind(host)
    bind_peer(host, coordinator)
    context, frozen = make_action(host, coordinator)
    host.sdk.im.v1.message.reply.side_effect = TimeoutError("synthetic lost receipt")
    first = coordinator.actions.execute(frozen["action_id"], frozen["digest"], {"version": 1},
                                         context, coordinator.action_executor)
    assert first["state"] == "unknown" and first["attempt_count"] == 1
    assert coordinator.actions.execute(frozen["action_id"], frozen["digest"], {"version": 1},
                                        context, coordinator.action_executor)["executed"] is False
    assert host.sdk.im.v1.message.reply.call_count == 1
    receipt = host.store.rows("SELECT * FROM studio_send_receipts")[0]
    coordinator.handle("POST", "/v1/assistant/actions/" + frozen["action_id"] + "/reconcile",
        {"version": frozen["version"], "digest": frozen["digest"], "status": "not_applied",
         "evidence_ref": "synthetic-independent-lookup:confirmed-no-message"}, actor=OWNER)
    host.sdk.im.v1.message.reply.side_effect = host.sdk._reply
    second = coordinator.actions.execute(frozen["action_id"], frozen["digest"], {"version": 1},
                                          context, coordinator.action_executor)
    assert second["state"] == "applied" and second["attempt_count"] == 2
    assert host.sdk.im.v1.message.reply.call_count == 2
    assert host.sdk.im.v1.message.get.call_count == 1
    receipts = host.store.rows("SELECT * FROM studio_send_receipts")
    assert len(receipts) == 2 and receipt in receipts
    assert len({row["uuid"] for row in receipts}) == 2
    assert coordinator.actions.execute(frozen["action_id"], frozen["digest"], {"version": 1},
                                        context, coordinator.action_executor)["executed"] is False
    assert host.sdk.im.v1.message.reply.call_count == 2


def test_revoked_recipient_identity_and_unbound_target_do_not_send(host):
    coordinator = bind(host)
    bind_peer(host, coordinator)
    _, frozen = make_action(host, coordinator)
    host.actors[PEER] = Actor(PEER, "其他部门", ("od_other_test",), True)
    host.directory.invalidate(PEER)
    with pytest.raises(BindingError):
        coordinator.action_executor(frozen)
    host.sdk.im.v1.message.reply.assert_not_called()


def test_local_message_port_never_drops_requested_attachments(host):
    coordinator = bind(host)
    bind_peer(host, coordinator)
    _, frozen = make_action(host, coordinator)
    frozen["payload"]["artifacts"] = [{"name": "report.docx", "sha256": "synthetic"}]
    with pytest.raises(BindingError, match="attachment_port"):
        coordinator.action_executor(frozen)
    host.sdk.im.v1.message.reply.assert_not_called()


def test_real_production_bundle_admits_verified_demo_dm_and_keeps_dry_reports_pending(host):
    host.bridge.poc = NS(config=NS(tenant_key=host.cfg.tenant_id, bot_open_id=host.cfg.bot_id))
    host.bridge.am_self.owner = OWNER
    host.bridge.am_self.owner_origin = lambda: {"chat": "oc_owner"}
    settings = {**host.settings, "send_live": False, "live_cutover_at": 0,
                "database_name": "runtime.sqlite",
                "assistant": {**host.settings["assistant"], "enabled": True}}
    resolver = Mock(side_effect=lambda uid: host.actors[uid])
    receiver = DurableReceiver(host.store)
    adapter = LocalReceiverAdapter(receiver)
    capabilities = registry()
    for intent in receiver.intents:
        capabilities[intent] = adapter.capability(intent)
    bundle = build_phase1_bundle(host.bridge, {"phase1_gateway": settings}, host.sdk, host.root,
        capabilities=capabilities, adapters={"grok_bot": adapter},
        legacy_control_port=lambda _: False, assistant_identity_resolver=resolver)
    ingress, scheduler = install_candidate(bundle.ports, activate=False)
    service = RuntimeService(ingress, collector=bundle.bind(ingress), receivers=(receiver,),
                             sender=None, readback=bundle.readback)
    service.proposals = ProposalRepository(service.harness.store, OWNER)
    coordinator = bind_assistant(service, bundle, host.bridge, settings, host.sdk, host.root)
    assert service.collector.collectors.assistant is coordinator
    now = time.time()
    payload = {"header": {"app_id": "cli_test", "tenant_key": host.cfg.tenant_id,
                            "event_type": "im.message.receive_v1"},
        "event": {"sender": {"sender_id": {"open_id": PEER}, "sender_type": "user",
                                "tenant_key": host.cfg.tenant_id},
            "message": {"message_id": "om_combined_input", "chat_id": "oc_peer", "chat_type": "p2p",
                "create_time": str(int(now * 1000)), "message_type": "text",
                "content": dumps({"text": "HD 项目进展怎么样了？"})}}}
    admission = ingress.ingest(payload)
    assert admission.captured and admission.claimed
    resolver.assert_not_called()
    assert not service.harness.store.rows("SELECT * FROM h1_tasks")
    admitted = coordinator.intake_once()
    assert admitted["state"] == "admitted", admitted
    assert bundle.authority.directory.dm_actor("oc_peer") == PEER
    assert "oc_peer" in bundle.readers.readers["am_bot"].chats
    assert coordinator.book.get_task(admitted["task_id"], principal_id=OWNER)["project_id"] == "HD"
    harness = service.harness
    harness.route_one()
    task = harness.store.rows("SELECT * FROM h1_tasks")[-1]
    harness.register_cutover(task["scope"], evidence_ref="synthetic-existing-consumer")
    harness.dispatch_one()
    result = {"response_kind": "sufficient", "reply_text": "当前等待测试报告，计划未调整。",
              "remaining_requirements": [], "artifacts": []}
    ReceiverPump(receiver, lambda envelope, state: dict(result), harness=harness).step()
    receiver.reconcile(harness)
    completed = harness.task(task["task_id"])
    assert completed["execution_state"] == "succeeded"
    assert json.loads(completed["result_json"])["reply_text"] == result["reply_text"]
    assert service.delivery_once() == 0
    reports = coordinator.reports.close_conversation("oc_peer", closed_at=now + 1, actor_id=PEER)
    assert len(reports) == 1 and reports[0]["state"] == "ready"
    assert reports[0]["target"]["actor_id"] == OWNER
    assert not any("AM 已送达原文：" in item for item in reports[0]["payload"]["sections"]["promised"])
    assert result["reply_text"] not in reports[0]["body"]
    assert reports[0]["payload"]["sections"]["pending"]
    assert coordinator.reports_once() == 0
    assert coordinator.maintenance.status()["status"] == "awaiting_binding"
    assert service.threads == [] and not service.started
    assert scheduler.thread is None and scheduler.classifier_thread is None
    host.ws_start.assert_not_called()
    host.sdk.im.v1.message.create.assert_not_called()
    host.sdk.im.v1.message.reply.assert_not_called()
    host.sdk.request.assert_not_called()


def test_host_maintenance_does_not_infer_missing_executor_target_grants(host):
    capabilities = registry()
    capabilities["project.execute"] = replace(capabilities["project.execute"],
        available=True, durable_acceptance=True, evidence_ref="synthetic-routine-test")
    harness = host.service.harness
    harness.config = replace(harness.config, capabilities=capabilities)
    executor = Mock(spec=["targets", "actions"], side_effect=AssertionError("unbound target must not execute"))
    executor.targets = {"HD": {"work_item": ["different-item"]}}
    executor.actions = {"update_fields"}
    host.settings["assistant"]["routine_fields"] = {"HD": {"progress_text": "progress", "todo_text": "todo"}}
    coordinator = bind(host, executor=executor)
    event = Event(host.cfg.tenant_id, "om_progress_source", "oc_peer", PEER,
                  "把 HD 项目的进展改成测试已开始", time.time(), channel="bot_dm", project_ids=("HD",))
    harness.ingest(event, Policy(PEER, "synthetic-source-authority", "oc_peer", ("oc_peer",), ("HD",), allow_reply=True))
    task = harness.route_one()
    if task["route_state"] != "resolved" or task["intent"] != "project.execute":
        harness.review(task["task_id"], action="resume", actor=PEER, intent="project.execute")
    envelope = harness.claim()
    assert envelope is not None
    proposal = host.service.proposals.propose(envelope, "project.execute", {
        "project_id": "HD", "target": {"type": "work_item", "id": "intended-item"},
        "action": "update_fields", "changes": {"progress_text": "测试已开始"}})
    result = coordinator.maintenance.step()
    assert result["status"] == "awaiting_binding"
    assert result["results"][0]["reason"] == "executor_target_not_bound"
    assert host.service.proposals.get(proposal["proposal_id"])["status"] == "draft"
    executor.assert_not_called()
    host.sdk.im.v1.message.create.assert_not_called()
