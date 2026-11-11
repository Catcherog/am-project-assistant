"""Expression-only providers. No business actions, fallback model, or auto retry.

Production Qwen must use CoordinatorProvider, injected by the original host.
Direct HTTP and CodexCLI are isolated synthetic comparison paths only.
"""
from __future__ import annotations

import hashlib
import ipaddress
import json
import math
import os
import re
import signal
import subprocess
import tempfile
import time
import tomllib
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

from .contracts import ModelRequest, ModelResult, canonical, digest
from .prompt import RESPONSE_SCHEMA
from .runtime import ModelUnavailable, StopUnconfirmed

MAX_BYTES = 4 * 1024 * 1024
SHOW_FIELDS = ("model_info", "details", "template", "parameters", "capabilities",
               "thinking", "renderer", "parser", "template_source")


def _file_sha(path):
    h = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def _failure(kind, reason, **metadata):
    exc = kind(reason)
    exc.metadata = {"reason": reason, **metadata}
    return exc


def _check_request(request):
    if not isinstance(request, ModelRequest) or request.timeout_seconds <= 0 or request.max_output_tokens <= 0:
        raise ModelUnavailable("invalid_model_request")
    messages = request.messages
    if not isinstance(messages, list) or not messages:
        raise ModelUnavailable("empty_messages")
    for m in messages:
        if not isinstance(m, dict) or m.get("role") not in {"system", "user", "assistant"} or not isinstance(m.get("content"), str):
            raise ModelUnavailable("invalid_messages")


class CoordinatorProvider:
    """An adapter around the original host's leased invocation callback.

    `invoke(request)` MUST use the original ModelCoordinator and its existing
    cancellation/identity/deadline guards. It returns ModelResult only after
    completion, and raises StopUnconfirmed if cancellation is not reconciled.
    This class cannot manufacture that bridge and never opens an Ollama socket.
    """
    def __init__(self, invoke):
        if not callable(invoke):
            raise ValueError("original_coordinator_callback_required")
        self.invoke = invoke
        self.last_model_attempted = False

    def generate(self, request):
        self.last_model_attempted = False
        _check_request(request)
        try:
            self.last_model_attempted = True
            result = self.invoke(request)
        except TimeoutError as exc:
            raise StopUnconfirmed("coordinator_timeout_requires_original_stop_reconciliation") from exc
        if not isinstance(result, ModelResult):
            raise ModelUnavailable("coordinator_result_contract")
        return result


def _isolated_endpoint(endpoint, production_endpoint):
    parsed = urllib.parse.urlsplit(endpoint)
    try:
        loopback = ipaddress.ip_address(parsed.hostname or "").is_loopback
    except ValueError:
        loopback = False
    if (parsed.scheme != "http" or not loopback or not parsed.port or parsed.username or parsed.password
            or parsed.query or parsed.fragment or parsed.path not in ("", "/")):
        raise ModelUnavailable("isolated_http_requires_explicit_loopback_ip_and_port")
    if not production_endpoint or endpoint.rstrip("/") == production_endpoint.rstrip("/") or parsed.port == 11434:
        raise ModelUnavailable("direct_http_cannot_target_production_ollama")
    return endpoint.rstrip("/")


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, request, fp, code, msg, headers, newurl):
        raise ModelUnavailable("isolated_endpoint_redirect_forbidden")


class OllamaHTTPProvider:
    """Dedicated loopback instance only; never use beside the original consumer.

    The isolation record must be observed by Codex on the real host. Changing a
    boolean in an example config is not evidence of a separate Ollama process.
    """
    def __init__(self, *, endpoint, model, production_endpoint, isolation_record,
                 expected_show_sha256, synthetic_only=True, opener=None):
        self.endpoint = _isolated_endpoint(endpoint, production_endpoint)
        if not model or not isinstance(model, str) or not synthetic_only:
            raise ModelUnavailable("explicit_model_and_synthetic_only_required")
        record = isolation_record or {}
        if (record.get("verified") is not True or record.get("endpoint") != self.endpoint
                or not record.get("instance_id") or not record.get("evidence_sha256")):
            raise ModelUnavailable("real_isolated_instance_evidence_required")
        if not re.fullmatch(r"[a-f0-9]{64}", str(expected_show_sha256 or "")):
            raise ModelUnavailable("show_metadata_fingerprint_required")
        self.model, self.expected_show = model, expected_show_sha256
        self.opener = opener or urllib.request.build_opener(urllib.request.ProxyHandler({}), _NoRedirect())
        self.last_model_attempted = False

    def _post(self, suffix, value, timeout, *, generation=False):
        request = urllib.request.Request(self.endpoint + suffix, data=canonical(value).encode(),
                                         headers={"Content-Type": "application/json"}, method="POST")
        try:
            with self.opener.open(request, timeout=timeout) as response:
                raw = response.read(MAX_BYTES + 1)
            if len(raw) > MAX_BYTES:
                raise _failure(StopUnconfirmed if generation else ModelUnavailable, "response_too_large")
            return json.loads(raw)
        except (TimeoutError, urllib.error.URLError, OSError) as exc:
            # The socket closing does not prove the Ollama model job stopped.
            kind = StopUnconfirmed if generation else ModelUnavailable
            raise _failure(kind, "ollama_transport_unconfirmed" if generation else "ollama_metadata_unavailable") from exc
        except json.JSONDecodeError as exc:
            raise _failure(StopUnconfirmed if generation else ModelUnavailable, "ollama_invalid_transport_json") from exc

    def generate(self, request):
        self.last_model_attempted = False
        _check_request(request)
        started = time.monotonic()
        show = self._post("/api/show", {"model": self.model}, min(request.timeout_seconds, 5))
        observed = {k: show.get(k) for k in SHOW_FIELDS}
        if digest(observed) != self.expected_show:
            raise ModelUnavailable("ollama_model_definition_changed")
        remaining = request.timeout_seconds - (time.monotonic() - started)
        if remaining <= 0:
            raise ModelUnavailable("budget_exhausted_before_generation")
        payload = {"model": self.model, "messages": request.messages, "stream": False,
                   "format": RESPONSE_SCHEMA, "options": {"num_predict": request.max_output_tokens, "temperature": 0.2}}
        # /api/show is the source of capability knowledge. Never assume every
        # model/tag accepts think=false; omit it when capability is not declared.
        advertised_values = (show.get("thinking") or {}).get("values", [])
        if "thinking" in (show.get("capabilities") or []) and any(value is False for value in advertised_values):
            payload["think"] = False
        self.last_model_attempted = True
        response = self._post("/api/chat", payload, remaining, generation=True)
        if response.get("done") is not True:
            raise StopUnconfirmed("ollama_done_not_confirmed")
        if response.get("model") != self.model:
            raise ModelUnavailable("ollama_returned_a_different_model")
        message = response.get("message", {})
        if message.get("tool_calls"):
            raise _failure(ModelUnavailable, "unexpected_tool_proposal", tools_called=message["tool_calls"], delivered=False)
        text = message.get("content")
        if not isinstance(text, str):
            raise ModelUnavailable("ollama_missing_content")
        return ModelResult(text, "ollama-isolated", self.model, (time.monotonic() - started) * 1000,
                           canonical({"done_reason": response.get("done_reason"), "show_sha256": self.expected_show,
                                      "prompt_sha256": digest(request.messages), "think_disabled": "think" in payload,
                                      "num_predict": request.max_output_tokens, "delivery": "none"}))


FIXED_OVERRIDES = {
    "memories.use_memories": False, "memories.generate_memories": False,
    "web_search": "disabled", "features.shell_tool": False, "features.shell_snapshot": False,
    "agents.enabled": False, "features.skill_mcp_dependency_install": False,
    "apps._default.enabled": False,
    # R3 did not cover these independent feature surfaces. These are supported
    # settings in the official 2026-11-02 documentation, not evidence that any
    # particular desktop build actually exposes an empty tool inventory.
    "features.code_mode.enabled": False, "features.multi_agent": False,
    "features.apps": False, "features.memories": False, "tools.view_image": False,
}


def capability_overrides(proof):
    overrides = dict(FIXED_OVERRIDES)
    for family, field in (("mcp_servers", "mcp_server_ids"), ("apps", "app_ids")):
        ids = proof.get(field)
        if not isinstance(ids, list) or len(ids) != len(set(ids)) or any(not isinstance(x, str) or not x for x in ids):
            raise ModelUnavailable("complete_capability_inventory_required")
        for ident in sorted(ids):
            # Codex CLI splits -c dotted keys itself; quoted segments become
            # literal new server IDs in 0.163.0, yielding an invalid transport.
            if not re.fullmatch(r"[A-Za-z0-9_-]+", ident):
                raise ModelUnavailable("unsupported_cli_dotted_identifier")
            overrides[f"{family}.{ident}.enabled"] = False
    return overrides


def restricted_environment(proof):
    # Keep the actual user's paths and all existing Codex/OpenAI policy/auth
    # settings. Do not re-home Codex or hide a managed policy through new roots.
    names = {"PATH", "HOME", "USER", "LOGNAME", "LANG", "LC_ALL", "TMPDIR", "SSL_CERT_FILE", "SSL_CERT_DIR"}
    names.update(k for k in os.environ if k.startswith(("CODEX_", "OPENAI_")))
    supplied = proof.get("additional_environment_names", [])
    if not isinstance(supplied, list) or any(not isinstance(x, str) for x in supplied):
        raise ModelUnavailable("invalid_environment_inventory")
    names.update(supplied)
    return {key: os.environ[key] for key in names if key in os.environ}


def _read_codex_artifact(record, label):
    """Read content-bound capture artifacts; hashes do not authenticate origin."""
    if not isinstance(record, dict):
        raise ModelUnavailable("codex_" + label + "_artifact_required")
    path = Path(record.get("path") or "")
    expected = record.get("sha256")
    if (not path.is_absolute() or not path.is_file()
            or not re.fullmatch(r"[a-f0-9]{64}", str(expected or ""))
            or path.stat().st_size > MAX_BYTES or _file_sha(path) != expected):
        raise ModelUnavailable("codex_" + label + "_artifact_missing_or_changed")
    from .data import strict_loads
    try:
        value = strict_loads(path.read_text(encoding="utf-8"))
    except (ValueError, UnicodeError) as exc:
        raise ModelUnavailable("codex_" + label + "_artifact_invalid_json") from exc
    if not isinstance(value, dict):
        raise ModelUnavailable("codex_" + label + "_artifact_not_object")
    return value


def _validate_codex_request_artifacts(proof, observed, model):
    """Verify saved request/catalog bytes, not two matching asserted hashes.

    Capture authenticity still depends on the trusted real-host observer. This
    routine cannot turn hand-written JSON or a mock-provider capture into proof.
    """
    for name, sha_key in (("model_catalog", "model_catalog_sha256"),
                          ("captured_request", "request_sha256")):
        record = proof.get(name)
        if (not isinstance(record, dict) or record.get("sha256") != proof.get(sha_key)
                or observed.get(sha_key) != proof.get(sha_key)):
            raise ModelUnavailable("codex_raw_model_request_artifacts_required")
    catalog = _read_codex_artifact(proof["model_catalog"], "model_catalog")
    payload = _read_codex_artifact(proof["captured_request"], "captured_request")
    models = catalog.get("models", catalog.get("data"))
    if (not isinstance(models, list) or not any(isinstance(item, dict)
            and model in (item.get("slug"), item.get("id")) for item in models)):
        raise ModelUnavailable("codex_selected_model_missing_from_captured_catalog")
    if payload.get("model") != model or payload.get("tools") != []:
        raise ModelUnavailable("codex_captured_request_model_or_tools_mismatch")
    if (any(key in payload for key in ("agent", "harness", "environment"))
            or payload.get("functions") not in (None, [])
            or payload.get("tool_choice") not in (None, "none", "auto")):
        raise ModelUnavailable("codex_captured_request_has_uninspected_capabilities")


def validate_codex_proof(binary, profile, proof, *, model=None):
    if not Path(binary).is_absolute() or not Path(binary).is_file():
        raise ModelUnavailable("absolute_existing_codex_binary_required")
    if not re.fullmatch(r"[A-Za-z0-9_-]+", str(profile)):
        raise ModelUnavailable("explicit_restricted_profile_required")
    try:
        expiry = float(proof.get("valid_until_unix", 0))
    except (TypeError, ValueError):
        expiry = 0
    if (proof.get("schema") != "am-codex-capabilities/2" or proof.get("profile") != profile
            or proof.get("effective_inventory_verified") is not True
            or proof.get("observed_tool_names") != [] or proof.get("synthetic_only") is not True
            or not math.isfinite(expiry) or expiry <= time.time()):
        raise ModelUnavailable("fresh_real_capability_proof_required")
    if _file_sha(binary) != proof.get("binary_sha256"):
        raise ModelUnavailable("codex_binary_changed")
    sources = proof.get("config_sources")
    if not isinstance(sources, list) or not sources:
        raise ModelUnavailable("effective_config_sources_required")
    server_ids, app_ids, profile_found = set(), set(), False
    for source in sources:
        path = Path(source["path"])
        if not path.is_absolute() or not path.is_file() or _file_sha(path) != source.get("sha256"):
            raise ModelUnavailable("effective_codex_configuration_changed")
        if path.name == profile + ".config.toml":
            profile_found = True
        config = tomllib.loads(path.read_text(encoding="utf-8"))
        server_ids.update(config.get("mcp_servers", {}).keys())
        app_ids.update(set(config.get("apps", {}).keys()) - {"_default"})
    if not profile_found:
        raise ModelUnavailable("restricted_profile_not_in_effective_sources")
    if not server_ids <= set(proof.get("mcp_server_ids", [])) or not app_ids <= set(proof.get("app_ids", [])):
        raise ModelUnavailable("specific_enabled_overrides_missing_from_inventory")
    overrides = capability_overrides(proof)
    if digest(overrides) != proof.get("overrides_sha256"):
        raise ModelUnavailable("capability_proof_does_not_cover_current_overrides")
    evidence = proof.get("tool_surface_evidence", {})
    if not evidence.get("path") or not evidence.get("sha256") or _file_sha(evidence["path"]) != evidence["sha256"]:
        raise ModelUnavailable("real_tool_surface_verification_evidence_required")
    observed = json.loads(Path(evidence["path"]).read_text(encoding="utf-8"))
    if (observed.get("schema") != "am-codex-observed-surface/2"
            or observed.get("profile") != profile or observed.get("cli_version") != proof.get("cli_version")
            or observed.get("overrides_sha256") != digest(overrides)
            or observed.get("config_sources_sha256") != digest(sources)
            or observed.get("observed_tool_names") != [] or not observed.get("verified_by")
            or observed.get("policy_and_agents_preserved") is not True):
        raise ModelUnavailable("tool_surface_evidence_not_bound_to_current_configuration")
    if (not isinstance(model, str) or not model or proof.get("model") != model or observed.get("model") != model
            or observed.get("capture_mode") != "before_send_actual_provider"
            or observed.get("is_fixture") is not False
            or observed.get("provider_override_used") is not False
            or not isinstance(proof.get("provider_id"), str) or not proof["provider_id"]
            or observed.get("provider_id") != proof["provider_id"]
            or observed.get("request_tool_inventory_complete") is not True
            or observed.get("uninspected_harness_present") is not False):
        raise ModelUnavailable("tool_surface_not_from_exact_real_model_profile")
    _validate_codex_request_artifacts(proof, observed, model)
    return overrides


def inspect_events(raw):
    """Post-run diagnostics only, never a substitute for preventive restrictions."""
    events, tools, last = 0, [], None
    tool_kinds = {"command_execution", "mcp_tool_call", "web_search", "file_change", "tool_call", "function_call", "collab_tool_call"}
    for line in raw.splitlines():
        if not line.strip():
            continue
        try:
            item = json.loads(line)
        except json.JSONDecodeError as exc:
            raise ModelUnavailable("invalid_codex_jsonl") from exc
        events += 1
        value = item.get("item", {})
        if item.get("type") in tool_kinds:
            tools.append({"event": item["type"], "type": item["type"], "name": item.get("name")})
        if value.get("type") in tool_kinds:
            tools.append({"event": item.get("type"), "type": value["type"], "name": value.get("name")})
        elif value and value.get("type") not in {"agent_message", "reasoning", "todo_list"}:
            tools.append({"event": "unsupported_item_surface", "type": value.get("type"), "name": value.get("name")})
        if item.get("type") == "item.completed" and value.get("type") == "agent_message":
            last = value.get("text")
    return {"events": events, "tools_called": tools, "last_agent_text": last}


class CodexCLIProvider:
    """Synthetic offline comparator. Preserves original policy and AGENTS rules.

    The required proof is produced from a real effective-config/tool-surface
    check on the host, not by this module. A missing proof blocks generation.
    This provider never disables policy, rewrites AGENTS, or changes account roots.
    """
    def __init__(self, *, binary, model, profile, capability_proof, synthetic_only=True):
        if not isinstance(model, str) or not model.strip() or synthetic_only is not True:
            raise ModelUnavailable("explicit_model_and_synthetic_only_required")
        self.binary, self.model, self.profile, self.proof = binary, model, profile, capability_proof or {}
        self.last_model_attempted = False

    def _preflight(self, cwd, env, deadline=None):
        overrides = validate_codex_proof(self.binary, self.profile, self.proof, model=self.model)
        def remaining():
            value = min(5, deadline - time.monotonic()) if deadline is not None else 5
            if value <= 0:
                raise ModelUnavailable("budget_exhausted_by_codex_preflight")
            return value
        try:
            version = subprocess.run([self.binary, "--version"], capture_output=True, text=True, timeout=remaining(),
                                     cwd=cwd, env=env, check=True).stdout.strip()
            help_text = subprocess.run([self.binary, "exec", "--help"], capture_output=True, text=True, timeout=remaining(),
                                       cwd=cwd, env=env, check=True).stdout
        except (OSError, subprocess.SubprocessError) as exc:
            raise ModelUnavailable("codex_version_help_preflight_failed") from exc
        if version != self.proof.get("cli_version"):
            raise ModelUnavailable("codex_cli_version_changed")
        needed = ["--json", "--output-schema", "--output-last-message", "--ephemeral", "--sandbox", "--profile", "--skip-git-repo-check"]
        if any(flag not in help_text for flag in needed):
            raise ModelUnavailable("codex_required_flag_unavailable")
        return overrides, version

    def generate(self, request):
        self.last_model_attempted = False
        _check_request(request)
        started = time.monotonic()
        if os.name != "posix":
            raise ModelUnavailable("process_group_stop_requires_posix")
        env = restricted_environment(self.proof)
        with tempfile.TemporaryDirectory(prefix="am-style-codex-") as temp:
            root = Path(temp)
            cwd = root / "empty-workdir"
            cwd.mkdir()
            overrides, version = self._preflight(str(cwd), env, deadline=started + request.timeout_seconds)
            remaining = request.timeout_seconds - (time.monotonic() - started)
            if remaining <= 0:
                raise ModelUnavailable("budget_exhausted_by_codex_preflight")
            schema, final = root / "response.schema.json", root / "last.txt"
            schema.write_text(canonical(RESPONSE_SCHEMA), encoding="utf-8")
            argv = [self.binary, "exec", "--json", "--output-schema", str(schema),
                    "--output-last-message", str(final), "--ephemeral", "--sandbox", "read-only",
                    "--profile", self.profile, "--model", self.model, "--skip-git-repo-check"]
            for key, value in sorted(overrides.items()):
                argv.extend(["-c", key + "=" + json.dumps(value)])
            argv.append("-")  # stdin; not a shell command and not a command-line prompt.
            stdout_path, stderr_path = root / "events.jsonl", root / "stderr.log"
            with stdout_path.open("wb") as out, stderr_path.open("wb") as err:
                try:
                    process = subprocess.Popen(argv, stdin=subprocess.PIPE, stdout=out, stderr=err,
                                               cwd=cwd, env=env, start_new_session=True)
                    self.last_model_attempted = True
                except OSError as exc:
                    raise ModelUnavailable("codex_process_start_failed") from exc
                try:
                    process.communicate(request.messages_json.encode("utf-8"), timeout=remaining)
                except subprocess.TimeoutExpired as exc:
                    try:
                        os.killpg(process.pid, signal.SIGTERM)
                        process.communicate(timeout=2)
                    except (ProcessLookupError, subprocess.TimeoutExpired):
                        try:
                            os.killpg(process.pid, signal.SIGKILL)
                        except ProcessLookupError:
                            pass
                        try:
                            process.communicate(timeout=2)
                        except subprocess.TimeoutExpired:
                            pass
                    # Local process death != remote model stop confirmation.
                    raise _failure(StopUnconfirmed, "codex_timeout_remote_stop_unknown",
                                   local_returncode=process.poll(), remote_stop_confirmed=False) from exc
            meta = {"returncode": process.returncode, "cli_version": version,
                    "prompt_sha256": digest(request.messages), "overrides_sha256": digest(overrides),
                    "stderr_sha256": _file_sha(stderr_path), "delivery": "none",
                    "max_output_tokens_hard_enforced": False,
                    "prompt_transport": "canonical_messages_json_via_stdin",
                    "comparison_scope": "provider-plus-harness; existing policy and AGENTS context retained"}
            if stdout_path.stat().st_size > MAX_BYTES or (final.exists() and final.stat().st_size > MAX_BYTES):
                raise _failure(ModelUnavailable, "codex_output_too_large", **meta)
            events = inspect_events(stdout_path.read_text(encoding="utf-8"))
            meta.update(events=events["events"], tools_called=events["tools_called"])
            if events["tools_called"]:
                raise _failure(ModelUnavailable, "tool_surface_violation_observed", **meta)
            if process.returncode != 0:
                raise _failure(ModelUnavailable, "codex_exit_nonzero", **meta)
            if not final.is_file():
                raise _failure(ModelUnavailable, "codex_final_file_missing", **meta)
            text = final.read_text(encoding="utf-8").strip()
            if not text:
                raise _failure(ModelUnavailable, "codex_empty_final", **meta)
            # Preserve the exact final text. TerminalPresenter owns schema/semantic
            # checks; a successful subprocess exit is not a successful response.
            return ModelResult(text, "codex-cli-synthetic", self.model,
                               (time.monotonic() - started) * 1000, canonical(meta))
