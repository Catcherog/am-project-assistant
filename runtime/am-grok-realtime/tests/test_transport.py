"""Local real HTTP exercises. No actual Grok / Feishu endpoint is contacted."""
from __future__ import annotations

from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import http.client
import json
import socket
import threading
import time

import pytest

from am_grok_gateway.http_server import CallbackServer
from am_grok_gateway.models import ContractError, canonical_json
from am_grok_gateway.transport import (
    SIGNATURE_HEADER, WebhookTransport, sign_headers, verify_signature,
)


OUTBOUND_KEY = b"outbound-fixture-key-32-bytes-only!"
CALLBACK_KEY = b"callback-fixture-key-32-bytes-only!"
NOW = 1793851200


class ReplayStore:
    def __init__(self):
        self.seen = set()
        self.lock = threading.Lock()

    def claim(self, key, expires_at):
        assert expires_at >= NOW
        with self.lock:
            if key in self.seen:
                return False
            self.seen.add(key)
            return True


def result_payload(**updates):
    data = {
        "schema": "am.grok.result.v1", "request_id": "request-fixture-1",
        "attempt_id": "attempt-fixture-1", "kind": "answer", "text": "有内容的测试答复。", "sources": [],
    }
    data.update(updates)
    return data


@contextmanager
def callback_server(**kwargs):
    results, starts = [], []
    replay = ReplayStore()
    defaults = dict(
        on_result=lambda data: results.append(data) or {"status": "accepted"},
        on_started=lambda data: starts.append(data) or {"status": "recorded"},
        replay_guard=replay.claim, callback_key=CALLBACK_KEY, clock=lambda: NOW,
    )
    defaults.update(kwargs)
    with CallbackServer(**defaults) as receiver:
        yield receiver, results, starts, replay


def post(receiver, body, path="/v1/results", *, headers=None):
    raw = canonical_json(body).encode() if isinstance(body, dict) else body
    signed = sign_headers(CALLBACK_KEY, "POST", path, raw, timestamp=NOW) if headers is None else headers
    connection = http.client.HTTPConnection(*receiver.address, timeout=2)
    try:
        connection.request("POST", path, body=raw, headers=signed)
        response = connection.getresponse()
        return response.status, json.loads(response.read())
    finally:
        connection.close()


@contextmanager
def fake_webhook(action):
    calls = []

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            raw = self.rfile.read(int(self.headers.get("Content-Length", "0")))
            calls.append((self.path, raw, dict(self.headers)))
            try:
                action(self, raw)
            except (BrokenPipeError, ConnectionResetError):
                pass

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    server.daemon_threads = True
    server.block_on_close = False
    thread = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.01}, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}", calls
    finally:
        server.shutdown()
        server.server_close()
        thread.join(1)


def http_reply(handler, status=202, payload=b'{"run_id":"run-fixture"}', extra_headers=()):
    handler.send_response(status)
    handler.send_header("Content-Length", str(len(payload)))
    for key, value in extra_headers:
        handler.send_header(key, value)
    handler.end_headers()
    handler.wfile.write(payload)


def test_hmac_binds_wire_bytes_path_timestamp_and_nonce():
    raw = b'{"schema":"am.grok.request.v1","text":"hello"}'
    headers = sign_headers(OUTBOUND_KEY, "POST", "/wake?type=realtime", raw, timestamp=NOW, nonce="a_valid_nonce_123456")
    nonce, expiry = verify_signature(OUTBOUND_KEY, "POST", "/wake?type=realtime", raw, headers, now=NOW)
    assert nonce == "a_valid_nonce_123456" and expiry == NOW + 300
    for wrong_key, wrong_path, wrong_body, wrong_now in [
        (CALLBACK_KEY, "/wake?type=realtime", raw, NOW),
        (OUTBOUND_KEY, "/other", raw, NOW),
        (OUTBOUND_KEY, "/wake?type=realtime", raw + b" ", NOW),
        (OUTBOUND_KEY, "/wake?type=realtime", raw, NOW + 301),
        (OUTBOUND_KEY, "/wake?type=realtime", raw, NOW - 301),
    ]:
        with pytest.raises(ContractError):
            verify_signature(wrong_key, "POST", wrong_path, wrong_body, headers, now=wrong_now)


@pytest.mark.parametrize("url", [
    "http://example.invalid/wake", "http://127.0.0.1/wake", "ftp://example.invalid/wake",
    "https://user:private@example.invalid/wake", "https://example.invalid/wake#fragment",
    "https://example.invalid/wake\r\nSecret: nope",
])
def test_transport_rejects_unapproved_endpoint_configuration(url):
    with pytest.raises(ContractError):
        WebhookTransport(url, OUTBOUND_KEY)


def test_transport_http_loopback_opt_in_does_not_allow_remote_http():
    with pytest.raises(ContractError):
        WebhookTransport("http://example.invalid/wake", OUTBOUND_KEY, allow_loopback_http=True)
    with pytest.raises(ContractError):
        WebhookTransport("https://example.invalid/wake", "weak")


def test_real_http_signed_webhook_ignores_ambient_proxy(monkeypatch):
    monkeypatch.setenv("http_proxy", "http://127.0.0.1:1")
    monkeypatch.setenv("HTTP_PROXY", "http://127.0.0.1:1")
    monkeypatch.setenv("ALL_PROXY", "http://127.0.0.1:1")

    def action(handler, raw):
        verify_signature(OUTBOUND_KEY, "POST", "/wake?lane=interactive", raw, dict(handler.headers), now=NOW)
        http_reply(handler)

    with fake_webhook(action) as (base, calls):
        transport = WebhookTransport(base + "/wake?lane=interactive", OUTBOUND_KEY, allow_loopback_http=True, clock=lambda: NOW)
        receipt = transport.submit({"schema": "am.grok.request.v1", "text": "实时消息"})
    assert receipt.status == "accepted" and receipt.run_id == "run-fixture"
    assert len(calls) == 1
    assert json.loads(calls[0][1])["text"] == "实时消息"
    assert "outbound-fixture" not in repr(receipt)


@pytest.mark.parametrize("status, expected", [(204, "accepted"), (400, "rejected"), (401, "rejected"), (429, "rejected"), (500, "unknown"), (503, "unknown")])
def test_webhook_acceptance_and_failure_are_separate_from_completion(status, expected):
    with fake_webhook(lambda handler, raw: http_reply(handler, status, b"")) as (base, calls):
        receipt = WebhookTransport(base + "/wake", OUTBOUND_KEY, allow_loopback_http=True).submit({"schema": "am.grok.request.v1"})
    assert receipt.status == expected
    assert len(calls) == 1  # no automatic retry for any response


def test_redirect_is_not_followed_and_signing_headers_are_not_forwarded():
    with fake_webhook(lambda handler, raw: http_reply(handler)) as (target, target_calls):
        with fake_webhook(lambda handler, raw: http_reply(handler, 302, b"", (("Location", target + "/steal"),))) as (source, source_calls):
            receipt = WebhookTransport(source + "/wake", OUTBOUND_KEY, allow_loopback_http=True).submit({"schema": "am.grok.request.v1"})
    assert receipt.status == "unknown" and receipt.detail == "redirect_blocked"
    assert len(source_calls) == 1 and target_calls == []


def test_transport_bounds_request_and_response_without_leaking_body():
    with fake_webhook(lambda handler, raw: http_reply(handler, 202, b"PRIVATE" * 500)) as (base, calls):
        transport = WebhookTransport(base + "/wake", OUTBOUND_KEY, allow_loopback_http=True, max_request_bytes=128, max_response_bytes=64)
        too_large = transport.submit({"schema": "am.grok.request.v1", "text": "x" * 200})
        accepted = transport.submit({"schema": "am.grok.request.v1"})
    assert too_large.status == "rejected" and too_large.detail == "request_too_large"
    assert accepted.status == "accepted" and "PRIVATE" not in repr(accepted)
    assert len(calls) == 1


def test_timeout_is_unknown_and_not_retried():
    received = threading.Event()

    def action(handler, raw):
        received.set()
        time.sleep(0.3)
        http_reply(handler)

    with fake_webhook(action) as (base, calls):
        transport = WebhookTransport(base + "/wake", OUTBOUND_KEY, allow_loopback_http=True, timeout=0.06)
        before = time.monotonic()
        receipt = transport.submit({"schema": "am.grok.request.v1"})
        elapsed = time.monotonic() - before
        assert received.wait(0.1)
    assert receipt.status == "unknown" and elapsed < 0.25
    assert len(calls) == 1


def test_slow_accepted_body_does_not_change_acceptance_into_failure():
    def action(handler, raw):
        handler.send_response(202)
        handler.send_header("Content-Length", "100")
        handler.end_headers()
        handler.wfile.flush()
        time.sleep(0.25)

    with fake_webhook(action) as (base, _):
        receipt = WebhookTransport(base + "/wake", OUTBOUND_KEY, allow_loopback_http=True, timeout=0.06).submit({"schema": "am.grok.request.v1"})
    assert receipt.status == "accepted"


def test_callback_result_and_started_are_authenticated_and_distinct():
    with callback_server() as (receiver, results, starts, replay):
        status, data = post(receiver, result_payload())
        started = {"schema": "am.grok.started.v1", "request_id": "request-fixture-1", "attempt_id": "attempt-fixture-1"}
        start_status, _ = post(receiver, started, "/v1/started")
    assert status == start_status == 200
    assert data == {"status": "accepted"}
    assert len(results) == len(starts) == 1 and len(replay.seen) == 2


def test_bad_signature_does_not_call_business_or_consume_nonce():
    raw = canonical_json(result_payload()).encode()
    bad = sign_headers(OUTBOUND_KEY, "POST", "/v1/results", raw, timestamp=NOW, nonce="same_valid_nonce_123456")
    good = sign_headers(CALLBACK_KEY, "POST", "/v1/results", raw, timestamp=NOW, nonce="same_valid_nonce_123456")
    with callback_server() as (receiver, results, _, replay):
        assert post(receiver, raw, headers=bad)[0] == 401
        assert len(replay.seen) == 0
        assert post(receiver, raw, headers=good)[0] == 200
    assert len(results) == 1


def test_duplicate_nonce_is_rejected_but_new_nonce_reaches_core_idempotency():
    raw = canonical_json(result_payload()).encode()
    signed = sign_headers(CALLBACK_KEY, "POST", "/v1/results", raw, timestamp=NOW)
    with callback_server() as (receiver, results, _, _):
        assert post(receiver, raw, headers=signed)[0] == 200
        assert post(receiver, raw, headers=signed)[0] == 409
        assert post(receiver, raw)[0] == 200
    assert len(results) == 2  # core, not HTTP nonce, deduplicates same task result


@pytest.mark.parametrize("raw", [
    b'{"schema":"am.grok.result.v1","schema":"am.grok.result.v1"}',
    b'{"schema":"am.grok.result.v1","not_a_number":NaN}',
    b"[1,2,3]", b"\xff", b'{"schema":"am.grok.request.v1","auth":{"verified":true}}',
])
def test_signed_malformed_or_ingress_payload_never_reaches_callback(raw):
    with callback_server() as (receiver, results, starts, _):
        assert post(receiver, raw)[0] == 400
    assert not results and not starts


def test_callback_has_no_ingress_endpoint_or_implicit_get_mutation():
    with callback_server() as (receiver, results, starts, _):
        assert post(receiver, {"auth": {"verified": True}}, "/v1/ingress")[0] == 404
        connection = http.client.HTTPConnection(*receiver.address, timeout=1)
        connection.request("GET", "/v1/results")
        response = connection.getresponse()
        assert response.status == 405
        response.read()
        connection.close()
    assert not results and not starts


def raw_request(receiver, wire):
    with socket.create_connection(receiver.address, timeout=1) as sock:
        sock.sendall(wire)
        return sock.recv(2048)


@pytest.mark.parametrize("headers, expected", [
    (b"", b" 411 "),
    (b"Content-Length: 2\r\nContent-Length: 2\r\n", b" 400 "),
    (b"Transfer-Encoding: chunked\r\nContent-Length: 2\r\n", b" 400 "),
    (b"Content-Length: 65537\r\n", b" 413 "),
    (b"Content-Length: -1\r\n", b" 400 "),
    (b"Content-Length: 2\r\nContent-Type: text/plain\r\n", b" 415 "),
])
def test_callback_rejects_ambiguous_or_unbounded_http_framing(headers, expected):
    with callback_server() as (receiver, results, _, _):
        response = raw_request(receiver, b"POST /v1/results HTTP/1.1\r\nHost: localhost\r\n" + headers + b"\r\n{}")
    assert expected in response
    assert not results


def test_callback_rejects_duplicate_signature_header():
    raw = canonical_json(result_payload()).encode()
    signed = sign_headers(CALLBACK_KEY, "POST", "/v1/results", raw, timestamp=NOW)
    headers = "".join(f"{key}: {value}\r\n" for key, value in signed.items())
    headers += f"{SIGNATURE_HEADER}: {signed[SIGNATURE_HEADER]}\r\nContent-Length: {len(raw)}\r\n"
    with callback_server() as (receiver, results, _, _):
        response = raw_request(receiver, b"POST /v1/results HTTP/1.1\r\nHost: localhost\r\n" + headers.encode() + b"\r\n" + raw)
    assert b" 401 " in response and not results


def test_replay_store_failure_fails_closed():
    def unavailable(key, expires_at):
        raise RuntimeError("private-store-error-not-returned")

    with callback_server(replay_guard=unavailable) as (receiver, results, _, _):
        status, data = post(receiver, result_payload())
    assert status == 503 and data == {"error": "replay_store_unavailable"}
    assert not results


def test_callback_commit_exception_is_explicitly_unknown_and_not_echoed():
    def broken(data):
        raise RuntimeError("private-data-not-returned")

    with callback_server(on_result=broken) as (receiver, _, _, _):
        status, data = post(receiver, result_payload())
    assert status == 503 and data == {"error": "callback_outcome_unknown"}


def test_slow_header_connection_expires_and_handler_capacity_is_bounded():
    with callback_server(max_handlers=1, request_timeout=0.12) as (receiver, results, _, _):
        slow = socket.create_connection(receiver.address, timeout=1)
        slow.sendall(b"POST /v1/results HTTP/1.1\r\nHost: local")
        # Wait for the first accepted socket to acquire the only handler slot.
        until = time.monotonic() + 0.5
        while time.monotonic() < until:
            if receiver._server and len(receiver._server._connections) == 1:
                break
            time.sleep(0.002)
        extra = socket.create_connection(receiver.address, timeout=1)
        assert b" 503 " in extra.recv(1024)
        extra.close()
        slow.settimeout(1)
        assert slow.recv(1024) == b""
        slow.close()
        # Capacity is returned after the absolute deadline, not per incoming byte.
        until = time.monotonic() + 0.5
        while receiver._server and receiver._server._connections and time.monotonic() < until:
            time.sleep(0.002)
        assert post(receiver, result_payload())[0] == 200
    assert len(results) == 1


def test_stop_does_not_wait_for_slow_connection_and_can_restart():
    receiver = CallbackServer(
        on_result=lambda data: {}, on_started=lambda data: {}, replay_guard=ReplayStore().claim,
        callback_key=CALLBACK_KEY, request_timeout=10,
    ).start()
    slow = socket.create_connection(receiver.address, timeout=1)
    slow.sendall(b"POST ")
    before = time.monotonic()
    receiver.stop()
    assert time.monotonic() - before < 0.5
    slow.close()
    receiver.stop()
    with receiver:
        assert receiver.address[1] > 0


def test_public_bind_is_not_enabled_by_default():
    with pytest.raises(ContractError):
        CallbackServer(on_result=lambda data: {}, on_started=lambda data: {}, replay_guard=lambda k, e: True,
                       callback_key=CALLBACK_KEY, host="0.0.0.0")


def test_callback_can_arrive_before_webhook_acceptance_response():
    with callback_server() as (receiver, results, starts, _):
        def action(handler, raw):
            request = json.loads(raw)
            started = {"schema": "am.grok.started.v1", "request_id": request["request_id"], "attempt_id": request["attempt_id"]}
            assert post(receiver, started, "/v1/started")[0] == 200
            assert post(receiver, result_payload(request_id=request["request_id"], attempt_id=request["attempt_id"]))[0] == 200
            http_reply(handler)

        with fake_webhook(action) as (base, calls):
            transport = WebhookTransport(base + "/wake", OUTBOUND_KEY, allow_loopback_http=True)
            receipt = transport.submit({"schema": "am.grok.request.v1", "request_id": "early-result", "attempt_id": "attempt-one"})
    assert receipt.status == "accepted" and len(calls) == 1
    assert len(results) == len(starts) == 1
    assert results[0]["request_id"] == "early-result"


def test_concurrent_replay_is_claimed_once():
    raw = canonical_json(result_payload()).encode()
    headers = sign_headers(CALLBACK_KEY, "POST", "/v1/results", raw, timestamp=NOW)
    outcomes = []
    barrier = threading.Barrier(3)
    with callback_server() as (receiver, results, _, _):
        def run():
            barrier.wait()
            outcomes.append(post(receiver, raw, headers=headers)[0])
        threads = [threading.Thread(target=run) for _ in range(2)]
        for thread in threads:
            thread.start()
        barrier.wait()
        for thread in threads:
            thread.join(2)
    assert sorted(outcomes) == [200, 409] and len(results) == 1


def test_stalled_connect_is_bounded_without_unlimited_daemon_workers(monkeypatch):
    original = http.client.HTTPConnection.connect
    entered, release = threading.Event(), threading.Event()

    def stalled(connection):
        entered.set()
        release.wait(1)
        return original(connection)

    with fake_webhook(lambda handler, raw: http_reply(handler)) as (base, calls):
        monkeypatch.setattr(http.client.HTTPConnection, "connect", stalled)
        transport = WebhookTransport(base + "/wake", OUTBOUND_KEY, allow_loopback_http=True, timeout=0.03, max_inflight=1)
        try:
            assert transport.submit({"schema": "am.grok.request.v1"}).status == "unknown"
            assert entered.is_set()
            second = transport.submit({"schema": "am.grok.request.v1"})
            assert second.status == "rejected" and second.detail == "transport_capacity_exhausted"
            assert calls == []
        finally:
            release.set()
        # The timed-out worker checks cancellation before sending the POST.
        deadline = time.monotonic() + 1
        while time.monotonic() < deadline:
            if transport._permits.acquire(blocking=False):
                transport._permits.release()
                break
            time.sleep(0.002)
        else:
            pytest.fail("transport permit was not released")
    assert calls == []


def test_stop_between_accept_and_handler_initialization_cleans_up_socket(monkeypatch):
    from am_grok_gateway.http_server import _BoundedServer

    entered, release, finished = threading.Event(), threading.Event(), threading.Event()
    original = _BoundedServer.process_request_thread

    def paused(self, request, address):
        entered.set()
        release.wait(2)
        try:
            return original(self, request, address)
        finally:
            finished.set()

    monkeypatch.setattr(_BoundedServer, "process_request_thread", paused)
    receiver = CallbackServer(
        on_result=lambda data: pytest.fail("closed socket reached business callback"),
        on_started=lambda data: pytest.fail("closed socket reached business callback"),
        replay_guard=ReplayStore().claim, callback_key=CALLBACK_KEY, max_handlers=1,
    ).start()
    server = receiver._server
    client = socket.create_connection(receiver.address, timeout=1)
    try:
        client.sendall(b"POST /v1/results HTTP/1.1\r\nHost: localhost\r\n")
        assert entered.wait(1)
        receiver.stop()  # closes request before request.settimeout() runs
    finally:
        release.set()
        client.close()
        receiver.stop()
    assert finished.wait(1)
    assert server is not None and server._connections == set()
    assert server._slots.acquire(blocking=False)
    server._slots.release()
