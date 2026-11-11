from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import signal
import subprocess
import time
import uuid
from dataclasses import dataclass
from pathlib import Path

from .config import BoundaryError, Config


@dataclass(frozen=True)
class Result:
    text: str
    model: str
    elapsed: float
    usage: dict | None = None


def atomic_json(path: Path, data):
    temporary = path.with_suffix(".new")
    temporary.write_text(json.dumps(data, ensure_ascii=False, indent=2) + "\n")
    temporary.chmod(0o600)
    with temporary.open("rb") as stream:
        os.fsync(stream.fileno())
    os.replace(temporary, path)


def process_identity(pid: int) -> str:
    result = subprocess.run(["/bin/ps", "-p", str(pid), "-o", "uid=,pgid=,lstart="],
                            capture_output=True, text=True, timeout=5)
    return " ".join(result.stdout.split())


def group_alive(pgid: int) -> bool:
    if pgid <= 1 or pgid == os.getpgrp():
        raise BoundaryError("INVALID_PROCESS_GROUP")
    try:
        os.killpg(pgid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True


def stop_group(pgid: int, process: subprocess.Popen | None = None) -> bool:
    for sig, duration in ((signal.SIGTERM, 0.8), (signal.SIGKILL, 2.0)):
        if not group_alive(pgid):
            return True
        try:
            os.killpg(pgid, sig)
        except ProcessLookupError:
            return True
        end = time.monotonic() + duration
        while time.monotonic() < end:
            if process:
                process.poll()  # Reap the leader before checking the process group.
            if not group_alive(pgid):
                return True
            time.sleep(0.05)
    return not group_alive(pgid)


class ProcessRunner:
    """Bounded subprocess transport; no shell, no inherited credentials, no raw output logging."""

    def run(self, argv, *, cwd: Path, env: dict, directory: Path, timeout: float, on_start=None):
        self.last_timing=[];execution_started=time.monotonic();execution_recorded=False
        def stop(pgid,process):
            started=time.monotonic()
            try:return stop_group(pgid,process)
            finally:self.last_timing.append({'stage':'local_process_stop','seconds':time.monotonic()-started})
        if timeout <= 0:
            raise BoundaryError("CONTENT_EXPIRED")
        output, errors = directory / "stdout.tmp", directory / "stderr.tmp"
        journal = directory / "process.json"
        atomic_json(journal, {"state": "starting"})
        process = None
        try:
            with output.open("wb") as out, errors.open("wb") as err:
                process = subprocess.Popen(argv, cwd=cwd, env=env, stdin=subprocess.DEVNULL,
                                           stdout=out, stderr=err, start_new_session=True)
                pgid = process.pid
                identity = process_identity(pgid)
                atomic_json(journal, {"state": "running", "pgid": pgid, "identity": identity})
                if on_start:
                    on_start(pgid, identity)
                deadline = time.monotonic() + timeout
                while process.poll() is None:
                    if time.monotonic() >= deadline:
                        raise BoundaryError("EXECUTION_TIMEOUT")
                    if output.stat().st_size + errors.stat().st_size > 2 * 1024 * 1024:
                        raise BoundaryError("OUTPUT_LIMIT")
                    time.sleep(0.025)
                # A leader exiting does not prove all of its descendants exited.
                self.last_timing.append({'stage':'worker_execution','seconds':time.monotonic()-execution_started})
                execution_recorded=True
                if group_alive(pgid) and not stop(pgid, process):
                    raise BoundaryError("STOP_UNKNOWN")
                atomic_json(journal, {"state": "stopped"})
                if output.stat().st_size + errors.stat().st_size > 2 * 1024 * 1024:
                    raise BoundaryError("OUTPUT_LIMIT")
                with output.open("rb") as stream:
                    stdout = stream.read(2 * 1024 * 1024)
                with errors.open("rb") as stream:
                    stderr = stream.read(65536)
                return process.returncode, stdout, stderr
        except BaseException:
            if not execution_recorded:self.last_timing.append({'stage':'worker_execution','seconds':time.monotonic()-execution_started})
            if process and not stop(process.pid, process):
                raise BoundaryError("STOP_UNKNOWN") from None
            atomic_json(journal, {"state": "stopped"})
            raise


def safe_environment(directory: Path, key: str | None = None) -> dict:
    env = {
        "PATH": "/usr/bin:/bin:/usr/sbin:/sbin", "HOME": str(directory / "home"),
        "GROK_HOME": str(directory / "grok"), "TMPDIR": str(directory / "tmp"),
        "XDG_CONFIG_HOME": str(directory / "home" / ".config"),
        "XDG_CACHE_HOME": str(directory / "home" / ".cache"),
        "LANG": "en_US.UTF-8", "TERM": "dumb", "NO_COLOR": "1",
        "GROK_MEMORY": "0", "GROK_SUBAGENTS": "0", "GROK_GOAL": "0",
        "GROK_WRITE_FILE": "0", "GROK_WEB_FETCH": "0", "GROK_TOOL_SEARCH": "0",
        "GROK_LSP_TOOLS": "0", "GROK_CRASH_HANDLER": "0", "GROK_DISABLE_AUTOUPDATER": "1",
        "GROK_LOG_FILE": str(directory / "grok" / "grok.log"), "RUST_LOG": "off",
    }
    for vendor in ("CURSOR", "CLAUDE"):
        for surface in ("SKILLS", "RULES", "AGENTS", "MCPS", "HOOKS"):
            env[f"GROK_{vendor}_{surface}_ENABLED"] = "0"
    if key:
        env["XAI_API_KEY"] = key
    return env


def sandbox_profile(root: Path, directory: Path, binary: Path) -> str:
    # OS boundary also hides other tasks, secrets and personal files from CLI auto-discovery.
    quote = lambda p: json.dumps(str(p))
    denied = ["/Users", "/Volumes", "/private/var/root", "/private/etc/grok",
              "/etc/grok", "/Library/Application Support/ClaudeCode", str(root)]
    return "\n".join([
        "(version 1)", "(allow default)",
        "(deny file-read* file-write* " + " ".join(f"(subpath {quote(p)})" for p in denied) + ")",
        f"(allow file-read* (literal {quote(binary)}))",
        f"(allow file-read* file-write* (subpath {quote(directory)}))",
        "(deny process-fork)",  # Grok itself executes; no descendants/tools may be launched.
    ])


def validate_inspect(report: dict, directory: Path):
    for name in ("projectInstructions", "hooks", "skills", "agents", "plugins", "mcpServers", "lspServers"):
        if report.get(name) != []:
            raise BoundaryError("CLI_DISCOVERED_EXTERNAL_CONFIGURATION")
    if report.get("configWarnings") or report.get("mcpConfigProblems"):
        raise BoundaryError("CLI_CONFIGURATION_WARNING")
    layers = report.get("configSources", {}).get("layers")
    if not isinstance(layers, list):
        raise BoundaryError("CLI_INSPECT_SCHEMA_UNVERIFIED")
    for layer in layers:
        if layer.get("note") == "empty":
            continue
        path = Path(layer.get("path", ""))
        if not path.is_absolute() or not path.is_relative_to(directory):
            raise BoundaryError("CLI_CONFIG_OUTSIDE_TASK")


def parse_result(raw: bytes, elapsed: float, expected_model: str | None) -> Result:
    try:
        obj = json.loads(raw)
        text = obj["text"]
        usage = obj["modelUsage"]
        models = [k for k, value in usage.items() if value.get("modelCalls", 0) > 0]
    except (ValueError, KeyError, TypeError, AttributeError) as exc:
        raise BoundaryError("CLI_OUTPUT_SCHEMA_UNVERIFIED") from exc
    if (not isinstance(text, str) or not text.strip() or len(text) > 12000
            or obj.get("stopReason") != "end_turn" or len(models) != 1
            or not re.fullmatch(r"grok[-a-zA-Z0-9_.:/]*", models[0])
            or obj.get("num_turns") != 1):
        raise BoundaryError("CLI_RESULT_REJECTED")
    if expected_model and models[0] != expected_model:
        raise BoundaryError("MODEL_CHANGED")
    return Result(text.strip(), models[0], elapsed)


def classify_error(raw: bytes) -> str:
    # Never propagate CLI stderr; it can contain prompts, credentials or response bodies.
    message = raw.decode("utf-8", errors="replace").lower()
    if any(x in message for x in ("quota", "insufficient", "credit", "billing", "payment_required")):
        return "QUOTA_EXHAUSTED"
    if any(x in message for x in ("401", "unauthorized", "authentication", "invalid api key")):
        return "AUTH_FAILED"
    if any(x in message for x in ("429", "rate limit", "too many requests")):
        return "RATE_LIMITED"
    return "CLI_FAILED"


class GrokCliProvider:
    def __init__(self, config: Config):
        self.config = config
        self.runner = ProcessRunner()

    def directory(self, run_id: str) -> Path:
        if not re.fullmatch(r"[a-f0-9]{32}", run_id):
            raise BoundaryError("INVALID_RUN_ID")
        directory = self.config.root / "tasks" / run_id
        directory.mkdir(mode=0o700)
        for name in ("home", "grok", "work", "tmp"):
            (directory / name).mkdir(mode=0o700)
        (directory / "grok" / "config.toml").write_text(
            '[cli]\nauto_update = false\n[memory]\nenabled = false\n'
            '[models]\nallowed_models = ["grok*"]\nmax_retries = 0\n'
            '[ui]\npermission_mode = "dontAsk"\n'
            '[permission]\nrules = [{ action = "deny", tool = "*" }]\n'
        )
        (directory / "sandbox.sb").write_text(sandbox_profile(self.config.root, directory, self.config.executable))
        return directory

    def invoke(self, directory, arguments, *, key=None, timeout=30, on_start=None):
        return self.runner.run(
            ["/usr/bin/sandbox-exec", "-f", str(directory / "sandbox.sb"), str(self.config.executable), *arguments],
            cwd=directory / "work", env=safe_environment(directory, key), directory=directory,
            timeout=timeout, on_start=on_start,
        )

    def cleanup(self, directory):
        journal = directory / "process.json"
        if journal.exists() and json.loads(journal.read_text()).get("state") != "stopped":
            raise BoundaryError("STOP_UNKNOWN")
        shutil.rmtree(directory)

    def save_oauth(self, directory):
        """Persist only the service's refreshed auth, after the process has stopped."""
        journal = directory / "process.json"
        if journal.exists() and json.loads(journal.read_text()).get("state") != "stopped":
            raise BoundaryError("STOP_UNKNOWN")
        target = self.config.credential_file
        if target.is_symlink() or not target.resolve().is_relative_to(self.config.root / "secrets"):
            raise BoundaryError("AUTH_NOT_DEDICATED")
        atomic_json(target, self.config.read_auth_json(directory / "grok" / "auth.json"))

    def device_login(self):
        """User-facing terminal flow only; never import an existing personal login."""
        if self.config.auth_method != "dedicated_device_oauth" or self.config.concurrency != 1:
            raise BoundaryError("OAUTH_SINGLE_LANE_REQUIRED")
        self.fingerprint()
        directory = self.directory(uuid.uuid4().hex)
        journal = directory / "process.json"
        process = None
        try:
            atomic_json(journal, {"state": "starting"})
            print("请按 Grok 显示的设备码在浏览器授权；认证仅保存到专用服务目录。", flush=True)
            process = subprocess.Popen([
                "/usr/bin/sandbox-exec", "-f", str(directory / "sandbox.sb"), str(self.config.executable),
                "--no-auto-update", "--cwd", str(directory / "work"), "login", "--device-auth",
            ], cwd=directory / "work", env=safe_environment(directory), stdin=subprocess.DEVNULL,
                start_new_session=True)
            atomic_json(journal, {"state": "running", "pgid": process.pid, "identity": process_identity(process.pid)})
            try:
                code = process.wait(timeout=600)
            except subprocess.TimeoutExpired:
                raise BoundaryError("LOGIN_TIMEOUT") from None
            if not stop_group(process.pid, process):
                raise BoundaryError("STOP_UNKNOWN")
            atomic_json(journal, {"state": "stopped"})
            if code:
                raise BoundaryError("LOGIN_FAILED")
            self.save_oauth(directory)
            atomic_json(self.config.root / "state" / "auth-status.json", {
                "method": "dedicated_device_oauth", "completed_at": time.time(),
                "credential_saved": True, "model_verified": False,
            })
            print("专用 Grok 登录已保存。尚未调用模型。", flush=True)
        finally:
            if process:
                if not stop_group(process.pid, process):
                    raise BoundaryError("STOP_UNKNOWN")
                atomic_json(journal, {"state": "stopped"})
            elif journal.exists():
                atomic_json(journal, {"state": "stopped"})
            self.cleanup(directory)

    def inspect(self, directory, timeout=30):
        code, out, _ = self.invoke(directory, ["--no-auto-update", "--cwd", str(directory / "work"), "inspect", "--json"], timeout=timeout)
        if code != 0:
            raise BoundaryError("CLI_INSPECT_FAILED")
        try:
            report = json.loads(out)
        except ValueError as exc:
            raise BoundaryError("CLI_INSPECT_SCHEMA_UNVERIFIED") from exc
        validate_inspect(report, directory)
        return report

    def fingerprint(self):
        self.config.check_service()
        receipt_path = self.config.root / "bin" / "release.json"
        receipt = json.loads(receipt_path.read_text())
        digest = hashlib.sha256(self.config.executable.read_bytes()).hexdigest()
        if receipt.get("publisher") != "@xai-official" or receipt.get("binary_sha256") != digest:
            raise BoundaryError("CLI_PROVENANCE_MISMATCH")
        directory = self.directory(uuid.uuid4().hex)
        try:
            code, version, _ = self.invoke(directory, ["--version"])
            if code:
                raise BoundaryError("CLI_VERSION_FAILED")
            version = version.decode().strip()
            if not re.fullmatch(r"[\w. +()/-]{1,150}", version) or receipt["package_version"] not in version:
                raise BoundaryError("CLI_VERSION_MISMATCH")
            code, help_text, _ = self.invoke(directory, ["--help"])
            flags = ["--prompt-file", "--output-format", "--tools", "--disallowed-tools", "--no-memory",
                     "--no-subagents", "--disable-web-search", "--no-auto-update", "--permission-mode", "--max-turns"]
            if code or any(f.encode() not in help_text for f in flags):
                raise BoundaryError("CLI_FLAGS_UNVERIFIED")
            self.inspect(directory)
            return {"cli_version": version, "binary_sha256": digest, "publisher": "@xai-official"}
        finally:
            self.cleanup(directory)

    def pin(self):
        path = self.config.root / "state" / "provider-pin.json"
        if not path.exists():
            raise BoundaryError("P1_REQUIRED")
        pin = json.loads(path.read_text())
        if not pin.get("real_p1_success") or not pin.get("model"):
            raise BoundaryError("P1_REQUIRED")
        current = self.fingerprint()
        if any(pin.get(k) != v for k, v in current.items()):
            raise BoundaryError("CLI_PIN_CHANGED")
        return pin

    def complete(self, prompt, *, run_id, timeout=120, on_start=None, discover=False):
        started = time.monotonic()
        timeout = min(timeout, 120)
        self.config.check_service()
        pin_path = self.config.root / "state" / "provider-pin.json"
        pin = None if discover else json.loads(pin_path.read_text())
        digest = hashlib.sha256(self.config.executable.read_bytes()).hexdigest()
        if pin and (not pin.get("real_p1_success") or digest != pin.get("binary_sha256")):
            raise BoundaryError("CLI_PIN_CHANGED")
        oauth = self.config.auth_method == "dedicated_device_oauth"
        if oauth and self.config.concurrency != 1:
            raise BoundaryError("OAUTH_SINGLE_LANE_REQUIRED")
        auth = self.config.read_auth_json(self.config.credential_file) if oauth else None
        key = None if oauth else self.config.read_secret(self.config.credential_file)
        directory = self.directory(run_id)
        try:
            self.inspect(directory, timeout=min(30, timeout))
            if auth:
                atomic_json(directory / "grok" / "auth.json", auth)
            path = directory / "prompt.txt"
            path.write_text(prompt)
            path.chmod(0o600)
            args = ["--no-auto-update", "--prompt-file", str(path), "--output-format", "json",
                    "--cwd", str(directory / "work"), "--tools", "", "--disallowed-tools", "search_tool,use_tool,Agent",
                    "--no-memory", "--no-subagents", "--no-plan", "--disable-web-search",
                    "--permission-mode", "dontAsk", "--deny", "*", "--max-turns", "1"]
            if pin:
                args += ["--model", pin["model"]]
            remaining = timeout - (time.monotonic() - started)
            code, out, err = self.invoke(directory, args, key=key, timeout=remaining, on_start=on_start)
            elapsed = time.monotonic() - started
            if code:
                raise BoundaryError(classify_error(err))
            return parse_result(out, elapsed, None if discover else pin["model"])
        finally:
            try:
                if oauth and (directory / "grok" / "auth.json").exists():
                    self.save_oauth(directory)
            finally:
                self.cleanup(directory)

    def p1(self):
        fingerprint = self.fingerprint()
        path = self.config.root / "state" / "provider-pin.json"
        if path.exists():
            self.pin()
        run_id = uuid.uuid4().hex
        result = self.complete("合成验证：2+3 等于多少？只回复数字 5，不使用工具。", run_id=run_id, discover=not path.exists())
        if result.text != "5":
            raise BoundaryError("P1_SYNTHETIC_ASSERTION_FAILED")
        receipt = dict(fingerprint, model=result.model, run_id=run_id, elapsed=result.elapsed,
                       real_p1_success=True, completed_at=time.time())
        atomic_json(path, receipt)
        return receipt
