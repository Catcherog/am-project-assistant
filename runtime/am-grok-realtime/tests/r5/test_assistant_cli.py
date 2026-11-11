"""The owner CLI sends bounded API requests and never prints its credential."""
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import importlib.util
import io
import json
from pathlib import Path
import socket
import subprocess
import sys
import threading

import pytest


CLI_PATH = Path(__file__).resolve().parents[2] / "tools" / "am_assistant.py"
SPEC = importlib.util.spec_from_file_location("am_assistant_cli_under_test", CLI_PATH)
cli = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(cli)
TOKEN = "SYNTHETIC_OWNER_OPERATION_TOKEN_FOR_TEST_ONLY"
TOKEN_ENV = "AM_ASSISTANT_OWNER_TOKEN"


@contextmanager
def api_server(*, status=200, result=None, raw=None, redirect=None, content_length=None):
    requests = []

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_):
            pass

        def handle_request(self):
            length = int(self.headers.get("Content-Length", "0"))
            body = self.rfile.read(length) if length else None
            requests.append({"method": self.command, "path": self.path,
                             "authorization": self.headers.get("Authorization"),
                             "body": json.loads(body) if body else None})
            content = raw if raw is not None else json.dumps({"ok": True} if result is None else result).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(content) if content_length is None else content_length))
            if redirect:
                self.send_header("Location", redirect)
            self.end_headers()
            self.wfile.write(content)

        do_GET = handle_request
        do_POST = handle_request

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=lambda: server.serve_forever(poll_interval=0.01), daemon=True)
    thread.start()
    try:
        yield "http://127.0.0.1:" + str(server.server_port), requests
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


def config_file(tmp_path, base, **extra):
    path = tmp_path / "owner-config.json"
    path.write_text(json.dumps({"base_url": base, "token_env": TOKEN_ENV, **extra}), encoding="utf-8")
    return path


def invoke(path, args, payload=None, environ=None):
    stdout, stderr = io.StringIO(), io.StringIO()
    stdin = io.StringIO("" if payload is None else json.dumps(payload, ensure_ascii=False))
    result = cli.main(["--config", str(path), *args], stdin=stdin, stdout=stdout,
                      stderr=stderr, environ={TOKEN_ENV: TOKEN} if environ is None else environ)
    return result, stdout.getvalue(), stderr.getvalue()


@pytest.mark.parametrize("command,path", [
    (["status"], "/v1/assistant/status"), (["tasks"], "/v1/assistant/tasks"),
    (["task", "task-123"], "/v1/assistant/tasks/task-123"),
    (["project", "DemoH20"], "/v1/assistant/projects/DemoH20"),
    (["reports"], "/v1/assistant/reports"), (["actions"], "/v1/assistant/actions"),
])
def test_read_commands_use_owner_header_and_exact_routes(tmp_path, command, path):
    with api_server(result={"status": "ok"}) as (base, requests):
        code, out, err = invoke(config_file(tmp_path, base), command)
    assert code == 0 and not err and json.loads(out) == {"status": "ok"}
    assert requests == [{"method": "GET", "path": path, "authorization": "Bearer " + TOKEN, "body": None}]
    assert TOKEN not in out + err


def test_submit_json_stdin_preserves_long_text_without_sending_identity(tmp_path):
    text = "请整理以下信息\n" + ("原文 `literal` $(literal) \"引文\"\n" * 500)
    payload = {"origin": "gpt_dot", "event_id": "unique-event-1", "text": text,
               "project_id": "DemoH20", "task_id": "task-existing"}
    with api_server() as (base, requests):
        code, _, err = invoke(config_file(tmp_path, base), ["submit", "--json-stdin"], payload)
    assert code == 0 and not err
    assert requests[0]["path"] == "/v1/assistant/tasks"
    assert requests[0]["body"] == payload
    assert "actor" not in requests[0]["body"] and "department" not in requests[0]["body"]


def test_submit_accepts_flags_and_global_json_stdin(tmp_path):
    with api_server() as (base, requests):
        config = config_file(tmp_path, base)
        first = invoke(config, ["submit", "--origin", "grok_bot", "--event-id", "e1", "--text", "进展如何？"])
        second = invoke(config, ["--json-stdin", "submit"],
                        {"origin": "gpt_dot", "event_id": "e2", "text": "补充任务"})
    assert first[0] == second[0] == 0
    assert requests[0]["body"] == {"origin": "grok_bot", "event_id": "e1", "text": "进展如何？"}
    assert requests[1]["body"]["event_id"] == "e2"


@pytest.mark.parametrize("field", ["actor", "actor_id", "authenticated_actor", "department", "owner_id", "token"])
def test_stdin_rejects_identity_and_credential_fields_before_network(tmp_path, field):
    with api_server() as (base, requests):
        code, out, err = invoke(config_file(tmp_path, base), ["submit", "--json-stdin"],
            {"origin": "gpt_dot", "event_id": "e", "text": "test", field: "spoof"})
    assert code == 2 and not out and "unsupported_request_fields" in err
    assert requests == []


@pytest.mark.parametrize("command,approved", [("approve", True), ("reject", False)])
def test_decisions_freeze_exact_version_digest_and_command_boolean(tmp_path, command, approved):
    digest = "0123456789abcdef" * 4
    with api_server() as (base, requests):
        code, _, err = invoke(config_file(tmp_path, base),
            [command, "action-1", "--version", "4", "--digest", digest])
    assert code == 0 and not err
    assert requests[0]["path"] == "/v1/assistant/actions/action-1/decision"
    assert requests[0]["body"] == {"version": 4, "digest": digest, "approve": approved}


def test_decision_json_cannot_override_approve_or_conflict_with_flag(tmp_path):
    with api_server() as (base, requests):
        config = config_file(tmp_path, base)
        overridden = invoke(config, ["reject", "action-1", "--json-stdin"],
                            {"version": 1, "digest": "hash", "approve": True})
        conflict = invoke(config, ["approve", "action-1", "--json-stdin", "--version", "2"],
                          {"version": 1, "digest": "hash"})
    assert overridden[0] == conflict[0] == 2 and requests == []


def test_followup_uses_fixed_recipient_target_due_and_source_fields(tmp_path):
    payload = {"task_id": "task-1", "recipient_id": "colleague-1", "target_chat": "chat-1",
               "text": "请反馈今天的测试进展", "due_at": 1794279600, "event_id": "followup-1",
               "source_ref": "message:owner-request"}
    with api_server() as (base, requests):
        code, _, err = invoke(config_file(tmp_path, base), ["followup", "--json-stdin"], payload)
    assert code == 0 and not err
    assert requests[0]["path"] == "/v1/assistant/followups"
    assert requests[0]["body"] == payload


@pytest.mark.parametrize("base", [
    "http://example.com", "http://192.168.1.2", "ftp://127.0.0.1", "http://u:p@127.0.0.1",
    "https://example.com/v1/tasks", "http://127.0.0.1?token=secret", "http://127.0.0.1/#fragment",
    "http://127.0.0.1:0", "http://127.0.0.1\\@evil.example", "http://127.0.0.1\n",
])
def test_config_rejects_unsafe_origin_or_path(tmp_path, base):
    code, out, err = invoke(config_file(tmp_path, base), ["status"])
    assert code == 2 and not out and TOKEN not in err


@pytest.mark.parametrize("base", ["http://localhost:8765", "http://127.0.0.2:8765/", "http://[::1]:8765", "https://operator.example"])
def test_config_accepts_loopback_http_or_verified_https(tmp_path, base):
    actual, token = cli._config(config_file(tmp_path, base), {TOKEN_ENV: TOKEN})
    assert actual == base.rstrip("/") and token == TOKEN


def test_token_must_come_from_environment_and_is_redacted_from_api_response(tmp_path):
    echo = {"message": "Bearer " + TOKEN, TOKEN: {"items": [TOKEN]}}
    with api_server(result=echo) as (base, requests):
        config = config_file(tmp_path, base)
        absent = invoke(config, ["status"], environ={})
        present = invoke(config, ["status"])
    assert absent[0] == 2 and "missing_or_invalid_token" in absent[2]
    assert present[0] == 0 and TOKEN not in present[1] + present[2]
    assert "[redacted]" in present[1] and len(requests) == 1
    bad_config = config_file(tmp_path, "http://127.0.0.1", token=TOKEN)
    assert invoke(bad_config, ["status"])[0] == 2


def test_http_error_does_not_print_server_body_or_credential(tmp_path):
    with api_server(status=403, result={"error": TOKEN}) as (base, requests):
        code, out, err = invoke(config_file(tmp_path, base), ["status"])
    assert code == 3 and not out and err.strip() == "http_error:403"
    assert TOKEN not in err and len(requests) == 1


def test_redirect_is_not_followed_or_given_the_owner_credential(tmp_path):
    with api_server() as (target, target_requests):
        with api_server(status=302, redirect=target + "/steal") as (base, requests):
            code, out, err = invoke(config_file(tmp_path, base), ["status"])
    assert code == 3 and not out and err.strip() == "http_error:302"
    assert len(requests) == 1 and target_requests == []


def test_network_and_bad_json_errors_are_nonzero_and_concise(tmp_path):
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    offline = invoke(config_file(tmp_path, "http://127.0.0.1:" + str(port)), ["status"])
    assert offline == (3, "", "network_error\n")
    with api_server(raw=b"not-json") as (base, requests):
        bad = invoke(config_file(tmp_path, base), ["status"])
    assert bad == (4, "", "invalid_api_response\n")


def test_truncated_http_body_reports_transport_error_without_leaking_partial_content(tmp_path):
    raw = json.dumps({"token": TOKEN}).encode()
    with api_server(raw=raw, content_length=len(raw) + 100) as (base, requests):
        code, out, err = invoke(config_file(tmp_path, base), ["status"])
    assert (code, out, err) == (3, "", "network_error\n")


@pytest.mark.parametrize("identifier", ["..", "../actions", "task/other", "task?owner=other", "task#fragment", "task\\other"])
def test_path_identifiers_cannot_select_another_route(tmp_path, identifier):
    with api_server() as (base, requests):
        code, out, err = invoke(config_file(tmp_path, base), ["task", identifier])
    assert code == 2 and not out and "invalid_identifier" in err and requests == []


def test_standalone_cli_executes_without_project_imports(tmp_path):
    with api_server(result={"assistant": "ready"}) as (base, requests):
        config = config_file(tmp_path, base)
        completed = subprocess.run([sys.executable, str(CLI_PATH), "--config", str(config), "status"],
            env={TOKEN_ENV: TOKEN}, text=True, capture_output=True, check=False, timeout=10)
    assert completed.returncode == 0 and json.loads(completed.stdout) == {"assistant": "ready"}
    assert TOKEN not in completed.stdout + completed.stderr
    assert len(requests) == 1


NEW_COMMANDS = [
    (["decide-task", "task-1"], "/v1/assistant/tasks/task-1/decision",
     {"accept": True, "source_ref": "owner:scope-confirmation", "scope_digest": "a" * 64}),
    (["task-state", "task-1"], "/v1/assistant/tasks/task-1/state",
     {"state": "completed", "source_ref": "owner:checked-result", "verification_ref": "readback:report-1"}),
    (["add-step", "task-1"], "/v1/assistant/tasks/task-1/steps",
     {"event_id": "step-1", "title": "整理原始记录\n保留 `literal` $(literal)",
      "assignee_id": "ou_test", "source_ref": "owner:task-breakdown"}),
    (["fact"], "/v1/assistant/facts",
     {"project_id": "HD", "field": "progress", "value": {"feedback": "测试已开始", "checks": [0, False, None]},
      "event_id": "fact-1", "source_ref": "feishu:message:om_test", "status": "reported", "category": "progress",
      "verification_ref": "", "observed_at": 1794272400, "supersedes_fact_id": "wf_previous"}),
    (["risk"], "/v1/assistant/risks",
     {"event_id": "risk-1", "text": "测试窗口与交付时间存在冲突", "source_ref": "owner:risk-review",
      "task_id": "task-1", "priority": "P1", "needs_owner": "是否协调额外测试资源"}),
]


@pytest.mark.parametrize("command,path,payload", NEW_COMMANDS)
@pytest.mark.parametrize("global_flag", [False, True])
def test_owner_book_commands_send_exact_json_over_real_http(tmp_path, command, path, payload, global_flag):
    args = ["--json-stdin", *command] if global_flag else [*command, "--json-stdin"]
    with api_server(result={"saved": True}) as (base, requests):
        code, out, err = invoke(config_file(tmp_path, base), args, payload)
    assert code == 0 and not err and json.loads(out) == {"saved": True}
    assert requests == [{"method": "POST", "path": path, "authorization": "Bearer " + TOKEN, "body": payload}]
    assert not {"actor", "actor_id", "principal_id", "owner_id", "department_ids", "token"} & set(requests[0]["body"])
    assert TOKEN not in out + err


@pytest.mark.parametrize("command,path,payload", NEW_COMMANDS)
def test_new_commands_require_explicit_json_input_and_reject_identity_fields(tmp_path, command, path, payload):
    with api_server() as (base, requests):
        config = config_file(tmp_path, base)
        absent = invoke(config, command, payload)
        forged = invoke(config, [*command, "--json-stdin"], {**payload, "principal_id": "someone-else"})
    assert absent == (2, "", "json_stdin_required\n")
    assert forged == (2, "", "unsupported_request_fields\n")
    assert requests == []


@pytest.mark.parametrize("command,payload,error", [
    (["decide-task", "t"], {"accept": "true", "source_ref": "owner:s"}, "missing_or_invalid_accept"),
    (["decide-task", "t"], {"accept": 1, "source_ref": "owner:s"}, "missing_or_invalid_accept"),
    (["decide-task", "t"], {"accept": True, "source_ref": "owner:s", "scope_digest": ""}, "missing_or_invalid_scope_digest"),
    (["decide-task", "t"], {"accept": True}, "missing_or_invalid_source_ref"),
    (["task-state", "t"], {"state": "done", "source_ref": "owner:s"}, "missing_or_invalid_state"),
    (["task-state", "t"], {"state": "completed", "source_ref": "owner:s"}, "missing_or_invalid_verification_ref"),
    (["task-state", "t"], {"state": "blocked", "source_ref": "owner:s", "verification_ref": 4}, "invalid_verification_ref"),
    (["add-step", "t"], {"event_id": "e", "title": " ", "source_ref": "owner:s"}, "missing_or_invalid_title"),
    (["add-step", "t"], {"title": "收集报告", "source_ref": "owner:s"}, "missing_or_invalid_event_id"),
    (["add-step", "t"], {"event_id": "e", "title": "收集报告", "source_ref": "owner:s", "assignee_id": []}, "invalid_assignee_id"),
    (["fact"], {"project_id": "HD", "field": "progress", "event_id": "e", "source_ref": "source:s"}, "missing_value"),
    (["fact"], {"project_id": "HD", "field": "progress", "event_id": "e", "source_ref": "source:s", "value": 0,
                "status": "certain"}, "missing_or_invalid_status"),
    (["fact"], {"project_id": "HD", "field": "progress", "event_id": "e", "source_ref": "source:s", "value": "完成",
                "status": "verified"}, "missing_or_invalid_verification_ref"),
    (["fact"], {"project_id": "HD", "field": "date", "event_id": "e", "source_ref": "source:s", "value": "11/20",
                "category": "plan"}, "invalid_plan_status"),
    (["fact"], {"project_id": "HD", "field": "progress", "event_id": "e", "source_ref": "source:s", "value": 0,
                "observed_at": True}, "invalid_observed_at"),
    (["fact"], {"project_id": "HD", "field": "progress", "event_id": "e", "source_ref": "source:s", "value": 0,
                "observed_at": -1}, "invalid_observed_at"),
    (["fact"], {"project_id": "HD", "field": "progress", "event_id": "e", "source_ref": "source:s", "value": 0,
                "observed_at": 10 ** 500}, "invalid_observed_at"),
    (["risk"], {"event_id": "e", "text": "测试排期冲突", "source_ref": "source:s", "needs_owner": True}, "invalid_needs_owner"),
    (["risk"], {"event_id": "e", "text": "测试排期冲突", "source_ref": "source:s", "priority": "urgent"}, "missing_or_invalid_priority"),
    (["risk"], {"event_id": "e", "text": "测试排期冲突", "source_ref": {"principal_id": "owner"}}, "missing_or_invalid_source_ref"),
])
def test_business_fields_are_validated_before_network(tmp_path, command, payload, error):
    with api_server() as (base, requests):
        code, out, err = invoke(config_file(tmp_path, base), [*command, "--json-stdin"], payload)
    assert (code, out, err) == (2, "", error + "\n")
    assert requests == []


@pytest.mark.parametrize("value", [0, False, None, {}])
def test_falsy_fact_values_are_preserved_instead_of_treated_as_missing(tmp_path, value):
    payload = {"project_id": "HD", "field": "measurement", "value": value,
               "event_id": "fact-falsy", "source_ref": "source:measurement"}
    with api_server() as (base, requests):
        result = invoke(config_file(tmp_path, base), ["fact", "--json-stdin"], payload)
    assert result[0] == 0 and requests[0]["body"]["value"] == value
    assert type(requests[0]["body"]["value"]) is type(value)


def test_minimal_optional_fields_are_not_fabricated_into_business_requests(tmp_path):
    samples = [
        (["decide-task", "t"], {"accept": False, "source_ref": "owner:decline"}),
        (["task-state", "t"], {"state": "reported_complete", "source_ref": "message:feedback"}),
        (["add-step", "t"], {"event_id": "e", "title": "核对报告", "source_ref": "owner:request"}),
        (["fact"], {"project_id": "HD", "field": "plan", "value": "周五测试", "event_id": "p", "source_ref": "plan:v1",
                    "status": "plan", "category": "plan", "observed_at": None, "supersedes_fact_id": None}),
        (["risk"], {"event_id": "r", "text": "测试日期可能受影响", "source_ref": "message:feedback"}),
    ]
    with api_server() as (base, requests):
        config = config_file(tmp_path, base)
        for command, payload in samples:
            assert invoke(config, [*command, "--json-stdin"], payload)[0] == 0
    assert [request["body"] for request in requests] == [payload for _, payload in samples]


@pytest.mark.parametrize("bad_value", [float("nan"), float("inf"), {"nested": float("nan")}])
def test_fact_nonfinite_json_never_becomes_a_traceback_or_network_request(tmp_path, bad_value):
    payload = {"project_id": "HD", "field": "measurement", "value": bad_value,
               "event_id": "nonfinite", "source_ref": "source:measurement"}
    with api_server() as (base, requests):
        result = invoke(config_file(tmp_path, base), ["fact", "--json-stdin"], payload)
    assert result == (2, "", "invalid_json_input\n") and requests == []


def test_new_route_id_cannot_inject_another_task_operation(tmp_path):
    with api_server() as (base, requests):
        result = invoke(config_file(tmp_path, base), ["task-state", "x/decision", "--json-stdin"],
                        {"state": "blocked", "source_ref": "owner:request"})
    assert result == (2, "", "invalid_identifier\n") and requests == []


def test_new_json_command_still_runs_as_a_standalone_owner_process(tmp_path):
    payload = {"event_id": "standalone-risk", "text": "测试资源需要协调", "source_ref": "owner:review",
               "needs_owner": "确认协调时间"}
    with api_server(result={"queued": True}) as (base, requests):
        completed = subprocess.run([sys.executable, str(CLI_PATH), "--config", str(config_file(tmp_path, base)),
                                    "risk", "--json-stdin"], input=json.dumps(payload, ensure_ascii=False),
            env={TOKEN_ENV: TOKEN}, text=True, capture_output=True, check=False, timeout=10)
    assert completed.returncode == 0 and json.loads(completed.stdout) == {"queued": True}
    assert requests[0]["path"] == "/v1/assistant/risks" and requests[0]["body"] == payload
    assert TOKEN not in completed.stdout + completed.stderr
