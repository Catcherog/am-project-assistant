"""Actual loopback HTTP and POSIX subprocess boundary checks; no real models."""
import json
import os
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from am_gateway.adapters import (
    AdapterError, CallableAdapter, GrokBotAdapter, HTTPJSONAdapter,
    QwenOllamaAdapter, SubprocessJsonAdapter,
)


def request(**overrides):
    return {
        "schema": "am.gateway.job.v2", "job_id": "job-1", "attempt_id": "attempt-1",
        "task_kind": "dialogue_reply", "worker": "grok_bot", "session_key": "t/c/th/a",
        "conversation": {"chat_id": "c", "thread_id": "th", "actor_id": "a"},
        "context": {"recent_messages": [], "project_snapshot": [], "input_items": []},
        "output_mode": "reply", "deadline_at": time.time() + 5, **overrides,
    }


def result(req, **overrides):
    return {
        "schema": "am.gateway.result.v2", "job_id": req["job_id"], "attempt_id": req["attempt_id"],
        "status": "completed", "decision_mode": "direct_reply", "reply_text": "收到",
        "hold_text": "", "approval_meta": {"reason": "", "risk_level": "low"},
        "task_requests": [], "source_item_ids": [], "answer_kind": "conversation",
        "cited_fact_ids": [], "action_intents": [], **overrides,
    }


@contextmanager
def server(responder):
    calls = []

    class Handler(BaseHTTPRequestHandler):
        def handle_request(self):
            data = self.rfile.read(int(self.headers.get("Content-Length", "0")))
            payload = json.loads(data) if data else None
            calls.append((self.command, self.path, payload, dict(self.headers)))
            status, content = responder(self.command, self.path, payload)
            content = content if isinstance(content, bytes) else json.dumps(content).encode()
            try:
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(content)))
                self.end_headers()
                self.wfile.write(content)
            except (BrokenPipeError, ConnectionResetError):
                pass

        do_POST = handle_request
        do_GET = handle_request

        def log_message(self, *args):
            pass

    httpd = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    httpd.daemon_threads = True
    thread = threading.Thread(target=lambda: httpd.serve_forever(poll_interval=0.01), daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{httpd.server_port}", calls
    finally:
        httpd.shutdown()
        httpd.server_close()
        thread.join(timeout=2)


def test_grok_sync_stateless_bearer_and_no_request_mutation():
    req = request()
    with server(lambda _, __, payload: (200, result(payload))) as (url, calls):
        reply = GrokBotAdapter(url, bearer_token="test-only").run(req)
    assert reply["reply_text"] == "收到"
    assert calls[0][2]["memory_mode"] == "stateless"
    assert calls[0][2]["session_key"] == req["session_key"]
    assert calls[0][3]["Authorization"] == "Bearer test-only"
    assert "memory_mode" not in req


def test_async_posts_once_and_persists_run_progress():
    req = request()
    events = []
    polls = 0

    def respond(method, path, payload):
        nonlocal polls
        if method == "GET":
            polls += 1
            assert path == "/status/run-1"
        state = "accepted" if method == "POST" else "running" if polls == 1 else "completed"
        envelope = {"schema": "am.gateway.async.v1", "job_id": req["job_id"], "attempt_id": req["attempt_id"], "run_id": "run-1", "status": state}
        if state == "completed":
            envelope["result"] = result(req)
        return 200, envelope

    with server(respond) as (url, calls):
        got = HTTPJSONAdapter(url, status_url=url + "/status/{run_id}", poll_interval=0.001, on_progress=events.append).run(req)
    assert got["status"] == "completed"
    assert [call[0] for call in calls] == ["POST", "GET", "GET"]
    assert [event["state"] for event in events] == ["accepted", "running", "completed"]
    assert all(event["provider_run_id"] == "run-1" and event["observed_at"] > 0 for event in events)


def test_async_acceptance_saved_even_without_poll_endpoint():
    req = request()
    events = []
    body = {"schema": "am.gateway.async.v1", "job_id": req["job_id"], "attempt_id": req["attempt_id"], "run_id": "run-1", "status": "accepted"}
    with server(lambda *_: (202, body)) as (url, calls):
        with pytest.raises(AdapterError) as caught:
            HTTPJSONAdapter(url, on_progress=events.append).run(req)
    assert caught.value.status == "submitted" and caught.value.unknown
    assert events[0]["provider_run_id"] == "run-1"
    assert len(calls) == 1


def test_progress_persistence_failure_stops_polling_as_unknown():
    req = request()
    body = {"schema": "am.gateway.async.v1", "job_id": req["job_id"], "attempt_id": req["attempt_id"], "run_id": "run-1", "status": "accepted"}

    def fail(_):
        raise RuntimeError("private database diagnostic")

    with server(lambda *_: (202, body)) as (url, calls):
        with pytest.raises(AdapterError) as caught:
            HTTPJSONAdapter(url, status_url=url + "/status/{run_id}", on_progress=fail).run(req)
    assert caught.value.code == "progress_persistence_failed"
    assert caught.value.unknown and caught.value.provider_run_id == "run-1"
    assert "private" not in str(caught.value) and len(calls) == 1


@pytest.mark.parametrize("status,unknown", [(400, False), (503, True), (302, True)])
def test_http_failure_semantics_and_no_retry(status, unknown):
    with server(lambda *_: (status, {"secret": "never in exception"})) as (url, calls):
        with pytest.raises(AdapterError) as caught:
            HTTPJSONAdapter(url).run(request())
    assert caught.value.unknown is unknown and len(calls) == 1
    assert "secret" not in str(caught.value)


def test_http_deadline_and_size_limits():
    def slow(*_):
        time.sleep(0.15)
        return 200, {}

    with server(slow) as (url, _):
        with pytest.raises(AdapterError) as caught:
            HTTPJSONAdapter(url).run(request(deadline_at=time.time() + 0.05))
    assert caught.value.unknown
    with server(lambda *_: (200, b"x" * 1000)) as (url, _):
        with pytest.raises(AdapterError, match="response_too_large"):
            HTTPJSONAdapter(url, max_response_bytes=32).run(request())


def test_grok_refuses_background_memory_and_fact_output():
    with pytest.raises(ValueError):
        GrokBotAdapter("http://127.0.0.1", background_jobs=True)
    with pytest.raises(AdapterError, match="dialogue_only"):
        GrokBotAdapter("http://127.0.0.1").run(request(task_kind="project_state_update"))
    with pytest.raises(AdapterError, match="stateless_required"):
        GrokBotAdapter("http://127.0.0.1").run(request(memory_mode="shared"))
    with server(lambda _, __, payload: (200, result(payload, fact_updates=[]))) as (url, _):
        with pytest.raises(AdapterError, match="forbidden_output"):
            GrokBotAdapter(url).run(request())


def test_ollama_uses_exact_installed_model_and_json_contract():
    candidate = {"tags": ["进度"], "normalized_text": "样件已完成", "source_item_ids": ["item-1"]}

    def respond(_, __, payload):
        assert payload["model"] == "my-trained-qwen:fixed-version"
        assert payload["stream"] is False and payload["format"] == "json"
        prompt = payload["messages"][0]["content"]
        assert "top-level candidates array" in prompt
        assert "Do not return top-level tag_candidates or normalized_text fields" in prompt
        req = json.loads(payload["messages"][1]["content"])
        answer = {"schema": "am.gateway.result.v2", "job_id": req["job_id"], "attempt_id": req["attempt_id"],
                  "status": "completed", "display_text": "候选整理完成", "candidates": [candidate]}
        return 200, {"done": True, "message": {"content": json.dumps(answer)}}

    with server(respond) as (url, calls):
        got = QwenOllamaAdapter(url + "/api/chat", "my-trained-qwen:fixed-version").run(
            request(worker="qwen", task_kind="tag_candidates", output_mode="internal",
                    context={"recent_messages": [], "project_snapshot": [], "input_items": [{"item_id": "item-1", "body": "样件已完成"}]}))
    assert got["status"] == "completed" and len(calls) == 1
    assert got["candidates"] == [candidate]
    assert "tag_candidates" not in got and "normalized_text" not in got


ECHO = "import json,sys; r=json.load(sys.stdin); print(json.dumps({'schema':'am.gateway.result.v2','job_id':r['job_id'],'attempt_id':r['attempt_id'],'status':'completed','display_text':sys.argv[1] if len(sys.argv)>1 else r['session_key']}))"


@pytest.mark.skipif(os.name != "posix", reason="POSIX process groups required")
def test_real_subprocess_json_and_literal_argv(tmp_path):
    marker = tmp_path / "must-not-exist"
    literal = "$(touch " + str(marker) + ")"
    got = SubprocessJsonAdapter([sys.executable, "-c", ECHO, literal]).run(request())
    assert got["display_text"] == literal and not marker.exists()


@pytest.mark.skipif(os.name != "posix", reason="POSIX process groups required")
def test_subprocess_concurrency_has_independent_contexts():
    adapter = SubprocessJsonAdapter([sys.executable, "-c", ECHO])
    requests = [request(job_id=f"job-{i}", attempt_id=f"try-{i}", session_key=f"isolated-{i}") for i in range(4)]
    with ThreadPoolExecutor(max_workers=4) as pool:
        outputs = list(pool.map(adapter.run, requests))
    assert [value["display_text"] for value in outputs] == [req["session_key"] for req in requests]


@pytest.mark.skipif(os.name != "posix", reason="POSIX process groups required")
@pytest.mark.parametrize("code,limits,expected", [
    ("import sys;sys.exit(4)", {}, "subprocess_nonzero_exit"),
    ("print('not json')", {}, "invalid_json_result"),
    ("print('x'*10000)", {"max_stdout_bytes": 128}, "subprocess_stdout_limit"),
    ("import sys;sys.stderr.write('x'*10000)", {"max_stderr_bytes": 128}, "subprocess_stderr_limit"),
])
def test_subprocess_failures_are_uncertain(code, limits, expected):
    with pytest.raises(AdapterError, match=expected) as caught:
        SubprocessJsonAdapter([sys.executable, "-c", code], **limits).run(request())
    assert caught.value.unknown


@pytest.mark.skipif(os.name != "posix", reason="POSIX process groups required")
def test_subprocess_deadline_returns_promptly():
    started = time.monotonic()
    with pytest.raises(AdapterError) as caught:
        SubprocessJsonAdapter([sys.executable, "-c", "import time;time.sleep(10)"]).run(request(deadline_at=time.time() + 0.08))
    assert caught.value.unknown and time.monotonic() - started < 2


def test_callable_request_copy_and_sanitized_failure():
    req = request()

    def trusted(detached):
        detached["context"]["input_items"].append("local-only")
        return result(detached)

    assert CallableAdapter(trusted).run(req)["status"] == "completed"
    assert req["context"]["input_items"] == []

    def bad(_):
        raise ValueError("private implementation details")

    with pytest.raises(AdapterError) as caught:
        CallableAdapter(bad).run(req)
    assert caught.value.unknown and str(caught.value) == "callable_execution_failed"
