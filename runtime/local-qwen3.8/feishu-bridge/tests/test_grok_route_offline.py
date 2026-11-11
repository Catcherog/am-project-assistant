"""Offline Grok routing. MOCK_GROK=1, no Feishu connection and no secret files."""
import json
import os
from pathlib import Path
import tempfile
import time
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from bridge import Bridge
from grok_backend import GrokBackend, GrokError
from grok_route import (
    FAILURE_TEXT, NO_BASIS, USER_PREFIX, GrokRoute, load_grok_settings, merge_grok_config,
)
from group_ai import QwenGroupAssistant
from group_ledger import GroupLedger
from test_group_ledger import LedgerTests, Port


def intent(route="general"):
    return {"route": route, "reference": "", "fields": [], "use_previous": False}


class Provider:
    def __init__(self, answers):
        self.answers = list(answers)
        self.calls = []

    def complete(self, prompt, **options):
        self.calls.append(prompt)
        item = self.answers.pop(0)
        if isinstance(item, Exception):
            raise item
        text = json.dumps(item, ensure_ascii=False) if isinstance(item, dict) else item
        return SimpleNamespace(text=text, elapsed=0.01)


class CaptureBackend:
    def __init__(self, payload=None, error=None):
        self.prompts = []
        self.payload = payload or {"status": "ok", "reply_text": "按摘录回答", "actions": []}
        self.error = error

    def complete(self, prompt):
        self.prompts.append(prompt)
        if self.error is not None:
            raise self.error
        return json.dumps(self.payload, ensure_ascii=False)


class RouteTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.kb = self.root / "kb"
        (self.kb / "口径规则").mkdir(parents=True)
        (self.kb / "最新台账导出").mkdir()
        (self.kb / "口径规则" / "核心口径.md").write_text(
            "需求76：只跟测试完成日期 11.9。\napp_secret: LEAK_MARKER\n", encoding="utf-8")
        (self.kb / "最新台账导出" / "台账.md").write_text(
            "需求76 台账原文 UNIQUE_LEDGER_SENTENCE。\n", encoding="utf-8")
        self.env = {
            "MOCK_GROK": os.environ.get("MOCK_GROK"),
            "MOCK_GROK_MODE": os.environ.get("MOCK_GROK_MODE"),
        }
        os.environ["MOCK_GROK"] = "1"
        os.environ.pop("MOCK_GROK_MODE", None)

    def tearDown(self):
        for key, value in self.env.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
        self.tmp.cleanup()

    def settings(self, **routing):
        body = {"enabled": True, "bot_p2p": True, "group_mention": True, "on_failure": "fixed",
                "answer_backend": "grok", "test_group_chat_id": "oc_test_only"}
        body.update(routing)
        return load_grok_settings({
            "state_directory": str(self.root),
            "knowledge_dir": str(self.kb),
            "send_live": False,
            "group_ledger": {"disclosure": {"oc_yes": ["需求76"]}},
            "grok_routing": body,
        })

    def test_mock_does_not_spawn_or_pause(self):
        def boom(*args, **kwargs):
            raise AssertionError("grok process started")
        with patch("grok_backend.subprocess.run", boom):
            text = GrokBackend("/nonexistent/grok").complete("问题")
        self.assertIn("MOCK_GROK_OK", text)

    def test_no_basis_skips_model(self):
        backend = CaptureBackend()
        route = GrokRoute(self.settings(), backend)
        answer = route.answer(kind="bot_p2p", question="需求99什么时候到料", chat_id="oc_p2p", message_id="om_1")
        self.assertEqual(answer.reply_text, NO_BASIS)
        self.assertEqual(answer.status, "need_owner")
        self.assertEqual(backend.prompts, [])
        self.assertTrue(answer.dry_run)
        self.assertEqual(route.read_outbox()[0]["sent"], False)

    def test_basis_goes_to_model_and_hides_secret_line(self):
        backend = CaptureBackend()
        route = GrokRoute(self.settings(), backend)
        answer = route.answer(kind="bot_p2p", question="需求76的测试完成日期", chat_id="oc_p2p", message_id="om_2")
        self.assertEqual(answer.reply_text, "按摘录回答")
        self.assertIn("测试完成日期", backend.prompts[0])
        self.assertIn("UNIQUE_LEDGER_SENTENCE", backend.prompts[0])
        self.assertNotIn("LEAK_MARKER", backend.prompts[0])

    def test_undisclosed_group_gets_no_ledger_body(self):
        backend = CaptureBackend()
        route = GrokRoute(self.settings(), backend)
        hidden = route.answer(kind="group_mention", question="介绍一下薛定谔方程", chat_id="oc_hidden", message_id="om_3")
        self.assertEqual(hidden.reply_text, "按摘录回答")
        self.assertNotIn("UNIQUE_LEDGER_SENTENCE", backend.prompts[0])
        self.assertNotIn("测试完成日期", backend.prompts[0])
        internal = route.answer(kind="group_mention", question="需求76什么时候到料", chat_id="oc_hidden", message_id="om_4")
        self.assertEqual(internal.reply_text, NO_BASIS)
        self.assertEqual(len(backend.prompts), 1)

    def test_disclosed_group_can_read_ledger_excerpt(self):
        backend = CaptureBackend()
        route = GrokRoute(self.settings(), backend)
        route.answer(kind="group_mention", question="需求76测试完成日期", chat_id="oc_yes", message_id="om_5")
        self.assertIn("UNIQUE_LEDGER_SENTENCE", backend.prompts[0])

    def test_user_send_is_need_confirm_and_prefixed(self):
        route = GrokRoute(self.settings(send_live=True), CaptureBackend())
        planned = route.plan_user_send({"text": "明天下午前给结论"}, chat_id="oc_p2p", message_id="om_6")
        self.assertTrue(planned.reply_text.startswith(USER_PREFIX))
        self.assertEqual(planned.status, "need_confirm")
        self.assertTrue(planned.dry_run)
        self.assertFalse(planned.actions[0]["sent"])
        os.environ["MOCK_GROK_MODE"] = "user"
        routed = GrokRoute(self.settings(send_live=True)).answer(
            kind="bot_p2p", question="介绍一下薛定谔方程", chat_id="oc_p2p", message_id="om_7")
        self.assertEqual(routed.status, "need_confirm")
        self.assertTrue(routed.dry_run)
        self.assertTrue(routed.actions[0]["text"].startswith(USER_PREFIX))
        self.assertFalse(routed.actions[0]["sent"])

    def test_bot_prefix_is_removed(self):
        backend = CaptureBackend({"status": "ok", "reply_text": USER_PREFIX + "直接说结论", "actions": []})
        answer = GrokRoute(self.settings(), backend).answer(
            kind="bot_p2p", question="介绍一下薛定谔方程", chat_id="oc_p2p", message_id="om_8")
        self.assertEqual(answer.reply_text, "直接说结论")

    def test_grok_error_uses_fixed_text_and_can_fall_back(self):
        route = GrokRoute(self.settings(), CaptureBackend(error=RuntimeError("STOP_UNKNOWN")))
        answer = route.answer_p2p("介绍一下薛定谔方程", [], "oc_p2p", "om_9", qwen=lambda: (_ for _ in ()).throw(AssertionError("qwen")))
        self.assertEqual(answer.reply_text, FAILURE_TEXT)
        self.assertEqual(answer.source, "fixed")
        calls = []
        fallback = GrokRoute(self.settings(on_failure="qwen"), CaptureBackend(error=GrokError("GROK_UNAVAILABLE")))
        answer = fallback.answer_p2p("介绍一下薛定谔方程", [], "oc_p2p", "om_10", qwen=lambda: calls.append("qwen") or "千问回退")
        self.assertEqual(answer.reply_text, "千问回退")
        self.assertEqual(calls, ["qwen"])
        paused = GrokRoute(self.settings(on_failure="qwen"), CaptureBackend(error=GrokError("GROK_UNAVAILABLE")))
        answer = paused.answer_p2p("介绍一下薛定谔方程", [], "oc_p2p", "om_11",
                                   qwen=lambda: (_ for _ in ()).throw(AssertionError("paused qwen")), qwen_paused=True)
        self.assertEqual(answer.source, "fixed")

    def test_sidecar_merge_drops_secrets_and_example_stays_off(self):
        sidecar = self.root / "grok_routing.json"
        sidecar.write_text(json.dumps({
            "app_secret": "SIDECAR_SECRET",
            "send_live": False,
            "grok_routing": {"enabled": True, "token": "SIDECAR_TOKEN"},
        }), encoding="utf-8")
        merged = merge_grok_config(
            {"app_id": "cli_test", "app_secret_file": str(self.root / "missing_secret"), "grok_routing": {"enabled": False}},
            sidecar_path=sidecar, use_sidecar=True)
        blob = json.dumps(merged, ensure_ascii=False)
        self.assertNotIn("SIDECAR_SECRET", blob)
        self.assertNotIn("SIDECAR_TOKEN", blob)
        self.assertTrue(merged["grok_routing"]["enabled"])
        self.assertNotIn("app_secret", merged)
        example = json.loads((Path(__file__).resolve().parents[1] / "config.example.json").read_text(encoding="utf-8"))
        self.assertIs(example["grok_routing"]["enabled"], False)
        self.assertIs(example["send_live"], False)


class BridgeGrokTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.env = {
            "MOCK_GROK": os.environ.get("MOCK_GROK"),
            "MOCK_GROK_MODE": os.environ.get("MOCK_GROK_MODE"),
        }
        os.environ["MOCK_GROK"] = "1"
        os.environ.pop("MOCK_GROK_MODE", None)
        self.sent = []
        self.calls = []
        self.pauses = []

    def tearDown(self):
        for key, value in self.env.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
        self.tmp.cleanup()

    def bridge(self, *, enabled=True, send_live=False, on_failure="fixed", busy=False):
        config = {
            "app_id": "cli_test", "state_directory": self.tmp.name, "access_mode": "all_private_users",
            "send_live": send_live, "knowledge_dir": str(Path(self.tmp.name) / "empty"),
            "grok_routing": {"enabled": enabled, "bot_p2p": True, "on_failure": on_failure, "answer_backend": "grok"},
        }
        poc = SimpleNamespace(
            ingest=lambda payload: (False, False),
            model_busy=lambda: busy,
            config=SimpleNamespace(users={}, bot_open_id="ou_bot"),
            store=SimpleNamespace(
                meta=lambda key: "PAUSED" if busy and key == "provider_pause" else None,
                set_meta=lambda *args: self.pauses.append(args),
            ),
        )
        def complete(messages):
            self.calls.append(messages)
            return "千问回退"
        return Bridge(config, lambda mid, text: self.sent.append((mid, text)), complete, poc=poc)

    def event(self, mid="om_1", text="介绍一下薛定谔方程"):
        return {"header": {"app_id": "cli_test", "event_type": "im.message.receive_v1", "tenant_key": "tenant"},
                "event": {"sender": {"sender_type": "user", "sender_id": {"open_id": "ou_owner"}, "tenant_key": "tenant"},
                          "message": {"message_id": mid, "chat_id": "oc_p2p", "chat_type": "p2p", "message_type": "text",
                                      "content": json.dumps({"text": text}), "create_time": str(int(time.time() * 1000))}}}

    def test_disabled_keeps_qwen_reply(self):
        bridge = self.bridge(enabled=False)
        self.assertTrue(bridge.ingest(self.event()))
        self.assertTrue(bridge.process_one())
        self.assertEqual(self.sent, [("om_1", "千问回退")])
        self.assertEqual(self.pauses, [])
        bridge.db.close()

    def test_dry_run_does_not_reply_and_is_once(self):
        bridge = self.bridge(busy=True)
        self.assertTrue(bridge.ingest(self.event()))
        self.assertTrue(bridge.process_one())
        self.assertEqual(self.sent, [])
        self.assertEqual(self.calls, [])
        self.assertEqual(self.pauses, [])
        self.assertEqual(bridge.db.execute("SELECT state FROM inbox").fetchone()[0], "done")
        rows = bridge.grok_route().read_outbox()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["reply_text"], "MOCK_GROK_OK")
        self.assertEqual(rows[0]["deliver"], "dry_run")
        self.assertFalse(rows[0]["sent"])
        self.assertFalse(bridge.ingest(self.event()))
        self.assertFalse(bridge.process_one())
        self.assertEqual(len(bridge.grok_route().read_outbox()), 1)
        bridge.db.close()

    def test_send_live_uses_existing_reply(self):
        bridge = self.bridge(send_live=True)
        bridge.ingest(self.event())
        bridge.process_one()
        self.assertEqual(self.sent, [("om_1", "MOCK_GROK_OK")])
        self.assertEqual(self.pauses, [])
        bridge.db.close()

    def test_need_confirm_does_not_send_even_if_live(self):
        os.environ["MOCK_GROK_MODE"] = "user"
        bridge = self.bridge(send_live=True)
        bridge.ingest(self.event())
        bridge.process_one()
        self.assertEqual(self.sent, [])
        self.assertEqual(bridge.grok_route().read_outbox()[0]["status"], "need_confirm")
        bridge.db.close()

    def test_failure_falls_back_without_pause(self):
        os.environ["MOCK_GROK_MODE"] = "error"
        bridge = self.bridge(on_failure="qwen")
        bridge.ingest(self.event())
        bridge.process_one()
        self.assertEqual(self.calls, [[{"role": "user", "content": "介绍一下薛定谔方程"}]])
        self.assertEqual(self.sent, [])
        self.assertEqual(self.pauses, [])
        self.assertEqual(bridge.grok_route().read_outbox()[-1]["source"], "qwen_fallback")
        bridge.db.close()


class GroupGrokTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.now = 2000000000
        self.pauses = []
        self.env = {
            "MOCK_GROK": os.environ.get("MOCK_GROK"),
            "MOCK_GROK_MODE": os.environ.get("MOCK_GROK_MODE"),
        }
        os.environ["MOCK_GROK"] = "1"
        os.environ.pop("MOCK_GROK_MODE", None)

    def tearDown(self):
        for key, value in self.env.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
        self.tmp.cleanup()

    def assistant(self, answers, *, send_live=False, on_failure="fixed"):
        settings = load_grok_settings({
            "state_directory": self.tmp.name,
            "knowledge_dir": str(Path(self.tmp.name) / "empty"),
            "send_live": send_live,
            "grok_routing": {"enabled": True, "group_mention": True, "answer_backend": "grok", "on_failure": on_failure,
                             "test_group_chat_id": "oc_test_only"},
        })
        self.provider = Provider(answers)
        return QwenGroupAssistant(self.provider, self.tmp.name, pause=self.pauses.append,
                                  answer_backend="grok", grok=GrokRoute(settings))

    def test_general_uses_grok_and_grok_stop_does_not_pause(self):
        brain = self.assistant([intent()])
        reply = brain.reply("介绍一下薛定谔方程", [], None, lambda: self.fail("ledger"))
        self.assertEqual(reply.text, "MOCK_GROK_OK")
        self.assertTrue(reply.via_grok)
        self.assertTrue(reply.dry_run)
        self.assertEqual(len(self.provider.calls), 1)
        self.assertEqual(self.pauses, [])
        os.environ["MOCK_GROK_MODE"] = "error"
        failed = self.assistant([intent()], on_failure="fixed")
        reply = failed.reply("你好", [], None, lambda: self.fail("ledger"))
        self.assertEqual(reply.text, FAILURE_TEXT)
        self.assertEqual(self.pauses, [])
        os.environ["MOCK_GROK_MODE"] = "error"
        fallback = self.assistant([intent(), "千问群回退"], on_failure="qwen")
        reply = fallback.reply("你好呀", [], None, lambda: self.fail("ledger"))
        self.assertEqual(reply.text, "千问群回退")
        self.assertTrue(reply.dry_run)
        self.assertEqual(self.pauses, [])

    def test_group_ledger_dry_run_and_live_reply_keep_project_path(self):
        sent = []
        port = Port()
        assistant = self.assistant([intent()])
        route = GroupLedger(self.tmp.name, port, lambda mid, text: sent.append((mid, text)),
                            clock=lambda: self.now, assistant=assistant)
        try:
            event = LedgerTests.event(self, "om_general", "介绍一下薛定谔方程")
            self.assertTrue(route.ingest(event))
            self.assertTrue(route.step())
            self.assertEqual(sent, [])
            self.assertEqual(route.db.execute("SELECT state FROM ledger_inbox").fetchone()[0], "dry_run")
            self.assertEqual(self.pauses, [])
            self.assertGreaterEqual(port.guards, 1)
        finally:
            route.close()

        sent_live = []
        live_root = self.tmp.name + "-live"
        Path(live_root).mkdir()
        assistant = self.assistant([intent()], send_live=True)
        route = GroupLedger(live_root, Port(), lambda mid, text: sent_live.append((mid, text)),
                            clock=lambda: self.now, assistant=assistant)
        try:
            self.assertTrue(route.ingest(LedgerTests.event(self, "om_live", "介绍一下薛定谔方程")))
            route.step()
            self.assertEqual(sent_live, [("om_live", "MOCK_GROK_OK")])
        finally:
            route.close()

        project_sent = []
        project_root = self.tmp.name + "-project"
        Path(project_root).mkdir()
        assistant = self.assistant([], send_live=True)
        route = GroupLedger(project_root, Port(), lambda mid, text: project_sent.append((mid, text)),
                            clock=lambda: self.now, assistant=assistant)
        try:
            self.assertTrue(route.ingest(LedgerTests.event(self, "om_project", "需求75进展如何")))
            route.step()
            self.assertIn("实际收货尚未核实", project_sent[0][1])
            self.assertEqual(assistant.provider.calls, [])
        finally:
            route.close()


if __name__ == "__main__":
    unittest.main()
