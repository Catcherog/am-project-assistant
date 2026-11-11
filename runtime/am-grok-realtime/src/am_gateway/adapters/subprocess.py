"""Bounded, deadline-controlled JSON subprocess shims (POSIX)."""
from __future__ import annotations

import os
import selectors
import signal
import subprocess
from collections.abc import Mapping, Sequence
from typing import Any

from .base import AdapterError, Deadline, json_bytes, parse_json, validate_request, validate_result


class SubprocessJsonAdapter:
    """Run a host-owned argv once with JSON stdin and JSON-only stdout.

    No shell or model-supplied command expansion is used. The shim must finish
    its work before exiting and must not daemonize outside its process group.
    """

    def __init__(
        self,
        argv: Sequence[str],
        *,
        cwd: str | None = None,
        env: Mapping[str, str] | None = None,
        max_request_bytes: int = 1_048_576,
        max_stdout_bytes: int = 1_048_576,
        max_stderr_bytes: int = 262_144,
    ):
        if isinstance(argv, (str, bytes)) or not argv or any(not isinstance(x, str) or "\x00" in x for x in argv):
            raise ValueError("argv must be a nonempty sequence of host-configured strings")
        if any(value <= 0 for value in (max_request_bytes, max_stdout_bytes, max_stderr_bytes)):
            raise ValueError("limits must be positive")
        self.argv = tuple(argv)
        self.cwd = cwd
        self.env = None if env is None else dict(env)
        self.max_request_bytes = max_request_bytes
        self.max_stdout_bytes = max_stdout_bytes
        self.max_stderr_bytes = max_stderr_bytes

    @staticmethod
    def _stop(proc: subprocess.Popen) -> None:
        # Kill the group even if the direct child already exited: descendants
        # may still own pipes and consume resources.
        try:
            os.killpg(proc.pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
        try:
            proc.wait(timeout=0.15)
        except subprocess.TimeoutExpired:
            pass
        try:
            os.killpg(proc.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        try:
            proc.wait(timeout=2)
        except subprocess.TimeoutExpired:
            pass

    def run(self, request: dict[str, Any]) -> dict[str, Any]:
        validate_request(request)
        deadline = Deadline.from_request(request)
        payload = json_bytes(request) + b"\n"
        if len(payload) > self.max_request_bytes:
            raise AdapterError("request_too_large")
        if os.name != "posix":
            raise AdapterError("subprocess_requires_posix_process_groups")
        deadline.remaining()
        try:
            proc = subprocess.Popen(
                self.argv, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                cwd=self.cwd, env=self.env, shell=False, start_new_session=True, bufsize=0,
            )
        except (OSError, ValueError) as exc:
            raise AdapterError("subprocess_start_failed") from exc
        selector = selectors.DefaultSelector()
        output = bytearray()
        error_size = 0
        offset = 0
        try:
            assert proc.stdin is not None and proc.stdout is not None and proc.stderr is not None
            for name, pipe, mode in (("stdin", proc.stdin, selectors.EVENT_WRITE), ("stdout", proc.stdout, selectors.EVENT_READ), ("stderr", proc.stderr, selectors.EVENT_READ)):
                os.set_blocking(pipe.fileno(), False)
                selector.register(pipe, mode, name)
            while selector.get_map():
                for key, _ in selector.select(timeout=min(0.1, deadline.remaining(submitted=True))):
                    pipe = key.fileobj
                    if key.data == "stdin":
                        try:
                            written = os.write(pipe.fileno(), payload[offset:offset + 65536])
                            offset += written
                        except BlockingIOError:
                            continue
                        except BrokenPipeError:
                            offset = len(payload)
                        if offset == len(payload):
                            selector.unregister(pipe)
                            pipe.close()
                    else:
                        try:
                            chunk = os.read(pipe.fileno(), 65536)
                        except BlockingIOError:
                            continue
                        if not chunk:
                            selector.unregister(pipe)
                            pipe.close()
                        elif key.data == "stdout":
                            if len(output) + len(chunk) > self.max_stdout_bytes:
                                raise AdapterError("subprocess_stdout_limit", status="unknown", unknown=True)
                            output.extend(chunk)
                        else:
                            # Count and discard stderr; logs may contain private
                            # prompts or credentials and never enter exceptions.
                            error_size += len(chunk)
                            if error_size > self.max_stderr_bytes:
                                raise AdapterError("subprocess_stderr_limit", status="unknown", unknown=True)
            try:
                code = proc.wait(timeout=deadline.remaining(submitted=True))
            except subprocess.TimeoutExpired as exc:
                raise AdapterError("subprocess_timeout", status="timeout_unknown", unknown=True) from exc
            if code != 0:
                raise AdapterError("subprocess_nonzero_exit", status="unknown", unknown=True)
            deadline.remaining(submitted=True)
            return validate_result(parse_json(bytes(output)), request)
        except AdapterError:
            raise
        except (OSError, ValueError) as exc:
            raise AdapterError("subprocess_io_failure", status="unknown", unknown=True) from exc
        finally:
            selector.close()
            self._stop(proc)
            for pipe in (proc.stdin, proc.stdout, proc.stderr):
                if pipe is not None:
                    pipe.close()


class GrokCLIAdapter(SubprocessJsonAdapter):
    """Host Grok CLI shim; no undocumented upstream command flags are assumed."""


class CodexCLIAdapter(SubprocessJsonAdapter):
    """Host Codex CLI shim; the host supplies the tested command and version."""
