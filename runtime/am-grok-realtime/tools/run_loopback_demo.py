#!/usr/bin/env python3
"""Run the real HTTP + SQLite path with an explicitly synthetic Bot and sender.

No external URL, Feishu credential, or Grok credential is accepted. Temporary
keys exist only in memory; the temporary SQLite database is removed on exit.
Reported milliseconds measure this local simulation, never Grok / phone SLO.
"""
from __future__ import annotations

import argparse
from dataclasses import replace
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import http.client
import json
from pathlib import Path
import secrets
import sys
import tempfile
import threading
import time
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from am_grok_gateway.config import GatewayConfig
from am_grok_gateway.core import Gateway, ScopeGuard
from am_grok_gateway.http_server import CallbackServer
from am_grok_gateway.metrics import summarize
from am_grok_gateway.models import AuthContext, GuardDecision, InboundMessage, SendReceipt, canonical_json
from am_grok_gateway.store import Store
from am_grok_gateway.transport import WebhookTransport, sign_headers, strict_json, verify_signature


class _SyntheticSender:
    def __init__(self) -> None:
        self.calls: list[dict[str, str]] = []
        self.lock = threading.Lock()

    def send(self, message: InboundMessage, text: str, idempotency_key: str) -> SendReceipt:
        with self.lock:
            self.calls.append({"input_message_id": message.message_id, "idempotency_key": idempotency_key, "text": text})
        # This is an in-memory stand-in; do not manufacture platform readback or
        # phone visibility timestamps from this return value.
        return SendReceipt("sent", message_id="synthetic-output-message")


def run_demo(*, chat_type: str = "p2p", duplicate_callback: bool = True) -> dict[str, Any]:
    if chat_type not in {"p2p", "group"}:
        raise ValueError("unsupported simulation chat type")
    outbound_key, callback_key = secrets.token_bytes(32), secrets.token_bytes(32)
    while outbound_key == callback_key:
        callback_key = secrets.token_bytes(32)
    trace: list[dict[str, Any]] = []
    trace_lock = threading.Lock()
    seen_wakes: list[dict[str, Any]] = []
    callback_statuses: list[dict[str, Any]] = []
    sender = _SyntheticSender()
    holder: dict[str, Gateway] = {}

    def record(event: str, **fields: Any) -> None:
        with trace_lock:
            trace.append({"event": event, "at": time.time(), **fields})

    temporary_database = ""
    with tempfile.TemporaryDirectory(prefix="am-grok-loopback-") as temporary:
        database_path = Path(temporary) / "simulation.sqlite3"
        temporary_database = str(database_path)
        store = Store(database_path)
        receiver = CallbackServer(
            on_result=lambda data: holder["gateway"].accept_result(data),
            on_started=lambda data: holder["gateway"].mark_started(data),
            replay_guard=store.claim_nonce,
            callback_key=callback_key,
        ).start()

        def callback(path: str, payload: dict[str, Any]) -> dict[str, Any]:
            body = canonical_json(payload).encode("utf-8")
            headers = sign_headers(callback_key, "POST", path, body)
            connection = http.client.HTTPConnection(*receiver.address, timeout=2)
            try:
                connection.request("POST", path, body=body, headers=headers)
                response = connection.getresponse()
                raw = response.read(8193)
                if response.status != 200 or len(raw) > 8192:
                    raise RuntimeError("simulation callback rejected")
                result = strict_json(raw)
                if not isinstance(result, dict):
                    raise RuntimeError("simulation callback response invalid")
                with trace_lock:
                    callback_statuses.append({"path": path, "http_status": response.status, "core_status": result.get("status")})
                record("callback_committed", path=path, status=result.get("status"))
                return result
            finally:
                connection.close()

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, format: str, *args: Any) -> None:
                pass

            def do_POST(self) -> None:
                if self.path != "/fixture/wake":
                    self.send_error(404)
                    return
                try:
                    size = int(self.headers.get("Content-Length", "0"))
                    if not 0 < size <= 131072:
                        raise ValueError("simulation body limit")
                    raw = self.rfile.read(size)
                    verify_signature(outbound_key, "POST", self.path, raw, dict(self.headers.items()))
                    request = strict_json(raw)
                    if not isinstance(request, dict) or request.get("schema") != "am.grok.request.v1":
                        raise ValueError("simulation request contract")
                    with trace_lock:
                        seen_wakes.append(request)
                    record("synthetic_bot_woken")
                    started = {
                        "schema": "am.grok.started.v1", "request_id": request["request_id"],
                        "attempt_id": request["attempt_id"],
                    }
                    callback("/v1/started", started)
                    result = {
                        "schema": "am.grok.result.v1", "request_id": request["request_id"],
                        "attempt_id": request["attempt_id"], "kind": "answer", "sources": [],
                        "text": "本示例中，实时入口处理当前消息，五分钟补漏仅查漏补缺；相同消息 ID 只生成一份回复。",
                    }
                    callback("/v1/results", result)
                    if duplicate_callback:
                        # New signature nonce, identical task/attempt/result.
                        callback("/v1/results", result)
                    record("webhook_acceptance_response_started")
                    reply = canonical_json({"run_id": "synthetic-run"}).encode()
                    self.send_response(202)
                    self.send_header("Content-Type", "application/json")
                    self.send_header("Content-Length", str(len(reply)))
                    self.end_headers()
                    self.wfile.write(reply)
                except Exception:
                    record("synthetic_bot_error")
                    try:
                        self.send_response(500)
                        self.send_header("Content-Length", "0")
                        self.end_headers()
                    except OSError:
                        pass

        server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        server.daemon_threads = True
        server.block_on_close = False
        server_thread = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.01}, daemon=True)
        server_thread.start()
        transport = WebhookTransport(
            f"http://127.0.0.1:{server.server_port}/fixture/wake",
            outbound_key, allow_loopback_http=True, timeout=2,
        )
        host, port = receiver.address
        config = GatewayConfig(
            enabled=True, allow_private=True, allowed_chat_ids=("synthetic-chat",),
            allowed_sender_ids=("synthetic-user",), callback_url=f"http://{host}:{port}/v1/results",
            max_inflight=2, max_reconcile_inflight=1,
        )
        guard = ScopeGuard(
            current_authorize=lambda message: GuardDecision(
                message.measurement_class == "synthetic" and message.app_id == "synthetic-app",
                reason="SIMULATION_SCOPE_ONLY",
            ),
            current_check=lambda message, result: GuardDecision(
                message.measurement_class == "synthetic" and result.kind == "answer" and not result.sources,
                useful=True, reason="SIMULATION_RESULT_ONLY",
            ),
        )
        gateway = Gateway(config, store, transport, sender, guard)
        holder["gateway"] = gateway
        stop_report: dict[str, Any] = {}
        try:
            now = time.time()
            message = InboundMessage(
                tenant_id="synthetic-tenant", app_id="synthetic-app", event_id="synthetic-event",
                message_id="synthetic-input-message", chat_id="synthetic-chat", chat_type=chat_type,
                sender_id="synthetic-user", text="解释本示例中的实时入口和五分钟补漏关系。",
                event_created_at=now, received_at=now,
                auth=AuthContext(True, now + 60, "simulation-policy-v1"),
                mentions_bot=chat_type == "group", measurement_class="synthetic",
            )
            first = gateway.admit(message)
            second = gateway.admit(replace(message, origin="reconcile", received_at=time.time()))
            if not first.handled or not second.handled or first.request_id != second.request_id or second.status != "duplicate":
                raise RuntimeError("simulation ingress deduplication failed")
            gateway.start()
            deadline = time.monotonic() + 5
            while time.monotonic() < deadline:
                row = store.get(first.request_id)
                if row and row["status"] == "sent" and row["accepted_at"] is not None:
                    break
                if gateway.last_worker_error:
                    raise RuntimeError("simulation scheduler error")
                time.sleep(0.005)
            else:
                raise RuntimeError("simulation did not complete")
            # Let the already-committed transport function exit before disposing
            # the temporary database. No timeout is interpreted as completion.
            stop_report = gateway.stop()
            deadline = time.monotonic() + 2
            while stop_report["ports_in_flight"] and time.monotonic() < deadline:
                time.sleep(0.005)
                stop_report = gateway.stop()
            if stop_report["ports_in_flight"]:
                raise RuntimeError("simulation ports did not settle")
            rows = store.records()
            if len(rows) != 1 or len(seen_wakes) != 1 or len(sender.calls) != 1:
                raise RuntimeError("simulation duplicate side effect")
            row = rows[0]
            early = row["result_at"] is not None and row["accepted_at"] is not None and row["result_at"] <= row["accepted_at"]
            if not early or row["status"] != "sent":
                raise RuntimeError("simulation early callback state regressed")
            report = {
                "schema": "am.grok.loopback-demo.v1", "simulation": True,
                "real_grok_calls": 0, "real_feishu_sends": 0,
                "generated_at": datetime.now(timezone.utc).isoformat(),
                "chat_type": chat_type,
                "result": "PASS",
                "counts": {
                    "ingress_attempts": 2, "accepted_unique_requests": len(rows),
                    "duplicate_ingress": 1, "signed_webhook_posts": len(seen_wakes),
                    "signed_callback_posts": len(callback_statuses), "synthetic_reply_sends": len(sender.calls),
                },
                "checks": {
                    "realtime_and_reconcile_share_message_claim": True,
                    "callback_before_webhook_202": early,
                    "final_state_not_regressed_by_late_202": row["status"] == "sent",
                    "duplicate_callback_same_result": (not duplicate_callback or any(c["core_status"] == "duplicate" for c in callback_statuses)),
                    "single_reply_to_bound_message": sender.calls[0]["input_message_id"] == message.message_id,
                },
                "callback_receipts": callback_statuses,
                "observed_local_milliseconds": {
                    "ingress_to_dispatch": round((row["dispatch_at"] - row["received_at"]) * 1000, 3),
                    "dispatch_to_result_callback": round((row["result_at"] - row["dispatch_at"]) * 1000, 3),
                    "event_to_synthetic_send_return": round((row["sent_at"] - row["event_created_at"]) * 1000, 3),
                },
                "timing_scope": "Actual local HTTP + SQLite with synthetic Bot and sender; NOT Grok inference, Feishu API, platform readback, or phone visibility.",
                "phone_visible_at": None,
                "latency_report": summarize(rows),
                "stop_report": stop_report,
                "trace": trace,
            }
        finally:
            gateway.stop()
            server.shutdown()
            server.server_close()
            server_thread.join(1)
            receiver.stop()
    report["temporary_database_removed"] = not Path(temporary_database).exists()
    if not report["temporary_database_removed"]:
        raise RuntimeError("simulation database was not removed")
    return report


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, type=Path, help="JSON output path; the simulation DB and keys are not retained")
    parser.add_argument("--chat-type", choices=("p2p", "group"), default="p2p")
    args = parser.parse_args(argv)
    try:
        report = run_demo(chat_type=args.chat_type)
    except Exception as exc:
        report = {
            "schema": "am.grok.loopback-demo.v1", "simulation": True,
            "real_grok_calls": 0, "real_feishu_sends": 0, "result": "FAIL",
            "error_type": type(exc).__name__,
            "timing_scope": "Local simulation failed; no native acceptance claim.",
        }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"result": report["result"], "simulation": True, "real_grok_calls": 0, "real_feishu_sends": 0}))
    return 0 if report["result"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
