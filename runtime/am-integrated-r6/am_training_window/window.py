"""A recoverable Q8 memory window around the existing R5/R4 execution path.

Native service operations are explicit ports. There is intentionally no kill,
launchctl, Ollama mutation, subprocess trainer, or second model consumer here.
The host adapter must wire its existing durable admission fence and exact
provider lifecycle. R4 remains the sole authority for locked resource checks.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
from typing import Callable, ContextManager, Mapping, Protocol
import uuid


class WindowError(RuntimeError):
    pass


@dataclass(frozen=True)
class ModelIdentity:
    name: str
    digest: str

    def __post_init__(self):
        if not self.name.strip() or not re.fullmatch(r"[0-9a-f]{64}", self.digest):
            raise ValueError("exact model name and SHA256 digest required")


@dataclass(frozen=True)
class HostBinding:
    """Bound by original host metadata; not inferred from a process name."""
    service_instance: str
    protected_config_sha256: str
    model: ModelIdentity
    runner: str

    def __post_init__(self):
        if not self.service_instance or not self.runner or not re.fullmatch(r"[0-9a-f]{64}", self.protected_config_sha256):
            raise ValueError("host identity, runner and protected configuration hash required")


@dataclass(frozen=True)
class HostObservation:
    binding: HostBinding
    consumer_count: int
    ingress_alive: bool
    metadata_alive: bool
    active_inference: int
    pending_business: int
    loaded_models: tuple[ModelIdentity, ...]
    fence_owner: str | None


@dataclass(frozen=True)
class GateObservation:
    ready: bool
    binding_sha256: str
    blockers: tuple[str, ...] = ()


@dataclass(frozen=True)
class ExecutionObservation:
    """Summary from original R5/R4, not a newly fabricated training receipt."""
    status: str
    stop_confirmed: bool
    evidence_sha256: str

    def __post_init__(self):
        if not self.status or type(self.stop_confirmed) is not bool or not re.fullmatch(r"[0-9a-f]{64}", self.evidence_sha256):
            raise ValueError("original execution status, stop proof and evidence digest required")


class HostPort(Protocol):
    def inspect(self) -> HostObservation: ...
    def close_admission(self, window_id: str) -> None:
        """CAS a durable fence; keep the unique consumer/ingress alive.

        Every inference path must honor this fence. It must not expire or reopen
        merely because a timer elapsed or this controller process disappeared.
        """
        ...
    def drain(self, window_id: str) -> None:
        """Wait within the host's bounded maintenance budget, then return.

        Do not discard messages or manufacture task completions. New business
        must remain visible to the original R4 pending-work check.
        """
        ...
    def suspend_exact_model(self, window_id: str, binding: HostBinding) -> None:
        """Idempotent owned-model unload; preserve the metadata API service."""
        ...
    def restore_exact_model(self, window_id: str, binding: HostBinding) -> None:
        """Idempotent restore of exactly the captured Q8 and configuration."""
        ...
    def open_admission(self, window_id: str) -> None:
        """CAS release this window's fence only after verified restoration."""
        ...


class Journal:
    """One durable journal per window; persist intent before side effects."""
    def __init__(self, path: Path, data: dict):
        self.path, self.data = Path(path), data

    @classmethod
    def create(cls, path: Path, mode: str, binding: HostBinding):
        if mode not in {"diagnostic", "training"}:
            raise ValueError("invalid mode")
        data = {"schema": "am.training-window.v1", "window_id": str(uuid.uuid4()),
                "mode": mode, "binding": asdict(binding), "state": "created",
                "events": [], "observations": {}, "trainer_invoked": False,
                "suspend_attempted": False, "restore_verified": False,
                "status": "in_progress"}
        path = Path(path)
        # Creating a new path is required; old windows are recovered explicitly.
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            stream.write(json.dumps(data, ensure_ascii=False, indent=2) + "\n")
            stream.flush()
            os.fsync(stream.fileno())
        return cls(path, data)

    @classmethod
    def load(cls, path: Path):
        data = json.loads(Path(path).read_text(encoding="utf-8"))
        if data.get("schema") != "am.training-window.v1" or data.get("mode") not in {"diagnostic", "training"}:
            raise WindowError("journal_schema_invalid")
        uuid.UUID(data["window_id"])
        return cls(path, data)

    def write(self, state: str, **updates):
        self.data.update(updates)
        self.data["state"] = state
        self.data["events"].append({"state": state, "at": datetime.now(timezone.utc).isoformat()})
        content = json.dumps(self.data, ensure_ascii=False, indent=2, allow_nan=False) + "\n"
        temp = self.path.with_name(self.path.name + "." + uuid.uuid4().hex + ".tmp")
        try:
            fd = os.open(temp, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            with os.fdopen(fd, "w", encoding="utf-8") as stream:
                stream.write(content)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temp, self.path)
            directory = os.open(self.path.parent, os.O_RDONLY)
            try:
                os.fsync(directory)
            finally:
                os.close(directory)
        finally:
            if temp.exists():
                temp.unlink()


def _check_host(observed: HostObservation, expected: HostBinding, owner: str | None, *, idle=False, model=None):
    if observed.binding != expected:
        raise WindowError("host_binding_changed")
    if observed.consumer_count != 1 or not observed.ingress_alive or not observed.metadata_alive:
        raise WindowError("original_consumer_or_metadata_unavailable")
    if observed.fence_owner != owner:
        raise WindowError("admission_fence_owner_changed")
    if idle and (observed.active_inference != 0 or observed.pending_business != 0):
        raise WindowError("business_work_not_drained")
    if any(item != expected.model for item in observed.loaded_models):
        raise WindowError("unexpected_model_present")
    if model == "present" and observed.loaded_models != (expected.model,):
        raise WindowError("original_model_not_uniquely_present")
    if model == "absent" and observed.loaded_models:
        raise WindowError("model_unload_not_confirmed")


def _check_gate(value: GateObservation, prior: GateObservation | None = None):
    if isinstance(value, GateObservation) and (value.ready is not True or value.blockers):
        raise WindowError("original_R5_input_gate_blocked: " + "; ".join(value.blockers or ("not_ready",)))
    if not isinstance(value, GateObservation) or not re.fullmatch(r"[0-9a-f]{64}", value.binding_sha256):
        raise WindowError("original_R5_input_gate_blocked")
    if prior is not None and value.binding_sha256 != prior.binding_sha256:
        raise WindowError("input_binding_changed_during_window")


class TrainingWindow:
    def __init__(self, host: HostPort, binding: HostBinding,
                 original_lock: Callable[[], ContextManager],
                 probe: Callable[[], Mapping], journal: Journal):
        self.host, self.binding, self.original_lock = host, binding, original_lock
        self.probe, self.journal = probe, journal
        if journal.data["binding"] != asdict(binding):
            raise WindowError("journal_host_binding_mismatch")

    @property
    def owner(self):
        return self.journal.data["window_id"]

    def _cleanup_record(self, state: str, **updates):
        # A full disk must never prevent the attempted restoration itself.
        self.journal.data.update(updates)
        self.journal.data["state"] = state
        try:
            self.journal.write(state, **updates)
        except Exception as exc:
            self.journal.data["journal_error_type"] = type(exc).__name__
            self.journal.data["journal_persistence_confirmed"] = False

    def diagnostic(self) -> dict:
        """No training port exists on this method, including with no holdout."""
        if self.journal.data["mode"] != "diagnostic":
            raise WindowError("journal_mode_mismatch")
        return self._run(None, None, None)

    def train(self, fresh_r5_gate: Callable[[], GateObservation],
              execute_original_r5: Callable[[], ExecutionObservation],
              trainer_stopped: Callable[[], bool]) -> dict:
        """Execute original pilot-execute, which must re-read its own inputs.

        execute_original_r5 is called only after releasing original_lock; its
        R4 workers acquire the original coordinator lock themselves. Never
        replace this callback with a cached R4 argv or bypass R5 validation.
        """
        if self.journal.data["mode"] != "training":
            raise WindowError("journal_mode_mismatch")
        return self._run(fresh_r5_gate, execute_original_r5, trainer_stopped)

    def _run(self, fresh_gate, execute, trainer_stopped):
        if self.journal.data["state"] != "created":
            raise WindowError("window_already_used_recover_instead")
        error: BaseException | None = None
        gate = None
        try:
            if fresh_gate is not None:
                gate = fresh_gate()
                _check_gate(gate)
                self.journal.write("input_ready", input_binding=gate.binding_sha256)
            _check_host(self.host.inspect(), self.binding, None)
            # Intent is durable before attempting the fence, so partial failures
            # can be reconciled using the exact window owner on the real host.
            self.journal.write("fencing")
            self.host.close_admission(self.owner)
            self.host.drain(self.owner)
            # This is only the original coordinator lock, without R4's >=24 GiB
            # entry check, because freeing memory is the purpose of this phase.
            with self.original_lock():
                # The original Q8 may have expired while idle. Establish its
                # exact identity under the SAME fence/lock before unloading.
                observed = self.host.inspect()
                _check_host(observed, self.binding, self.owner, idle=True)
                if not observed.loaded_models:
                    self.journal.write("preloading_original_model", suspend_attempted=True)
                    self.host.restore_exact_model(self.owner, self.binding)
                    self.journal.write("original_model_preloaded")
                _check_host(self.host.inspect(), self.binding, self.owner, idle=True, model="present")
                before = dict(self.probe())
                self.journal.data["observations"]["before_unload"] = before
                self.journal.write("suspending", suspend_attempted=True)
                self.host.suspend_exact_model(self.owner, self.binding)
                _check_host(self.host.inspect(), self.binding, self.owner, idle=True, model="absent")
                self.journal.data["observations"]["after_unload"] = dict(self.probe())
                self.journal.write("model_suspended")
            if execute is not None:
                # A stale pre-unload green result cannot authorize a later run.
                current_gate = fresh_gate()
                _check_gate(current_gate, gate)
                _check_host(self.host.inspect(), self.binding, self.owner, idle=True, model="absent")
                self.journal.write("executing_" + self.journal.data.get("execution_kind", "original_R5"), trainer_invoked=True)
                result = execute()
                if not isinstance(result, ExecutionObservation):
                    raise WindowError("original_execution_receipt_missing")
                self.journal.write("original_execution_returned", execution=asdict(result))
                if not result.stop_confirmed or not trainer_stopped():
                    raise WindowError("trainer_stop_unconfirmed")
                if result.status != "completed":
                    raise WindowError("original_training_not_completed")
            self.journal.write("work_completed")
        except BaseException as exc:
            error = exc
            self._cleanup_record("work_failed", error_type=type(exc).__name__, error_code=str(exc) if isinstance(exc, WindowError) else "native_operation_failed")
        restored = self._restore(trainer_stopped)
        status = "completed" if error is None and restored else ("failed_restored" if restored else "recovery_required")
        if self.journal.data.get("journal_error_type") and status == "completed":
            status = "failed_restored"
        self._cleanup_record(status, status=status,
                             resource_gate_authority="original_R4_only",
                             production_expression_policy="remain_off_verify_host_config")
        if self.journal.data.get("journal_error_type") and status == "completed":
            self.journal.data["status"] = "failed_restored"
            self.journal.data["state"] = "failed_restored"
        if isinstance(error, (KeyboardInterrupt, SystemExit)):
            raise error
        return self.journal.data

    def _restore(self, trainer_stopped: Callable[[], bool] | None) -> bool:
        try:
            observed = self.host.inspect()
            if not self.journal.data["suspend_attempted"] and observed.fence_owner == self.owner:
                # Busy/preparation failure before any unload: leave existing
                # running business alone and release only our admission marker.
                # Taking its model lock here would block the very task draining.
                _check_host(observed, self.binding, self.owner)
                self._cleanup_record("unchanged_verified", restore_verified=True)
                self.host.open_admission(self.owner)
                _check_host(self.host.inspect(), self.binding, None)
                return True
            if observed.fence_owner is None and (not self.journal.data["suspend_attempted"] or self.journal.data["restore_verified"]):
                # Pre-input failure has no side effects. Verify, don't mutate.
                if self.journal.data["trainer_invoked"] and (trainer_stopped is None or trainer_stopped() is not True):
                    raise WindowError("trainer_stop_unconfirmed_no_model_reload")
                _check_host(observed, self.binding, None,
                            model="present" if self.journal.data["suspend_attempted"] else None)
                self._cleanup_record("unchanged_verified", restore_verified=True)
                return True
            if self.journal.data["trainer_invoked"] and (trainer_stopped is None or trainer_stopped() is not True):
                raise WindowError("trainer_stop_unconfirmed_no_model_reload")
            with self.original_lock():
                _check_host(self.host.inspect(), self.binding, self.owner)
                self._cleanup_record("restoring")
                if self.journal.data["suspend_attempted"]:
                    # Even a partly failed unload is reconciled idempotently.
                    self.host.restore_exact_model(self.owner, self.binding)
                _check_host(self.host.inspect(), self.binding, self.owner, model="present")
                self.journal.data["observations"]["after_restore"] = dict(self.probe())
                self._cleanup_record("restored_verified", restore_verified=True)
                self.host.open_admission(self.owner)
                _check_host(self.host.inspect(), self.binding, None, model="present")
            return True
        except Exception as exc:
            self._cleanup_record("recovery_required", recovery_error_type=type(exc).__name__,
                                 recovery_error_code=str(exc) if isinstance(exc, WindowError) else "native_recovery_failed",
                                 status="recovery_required")
            return False

    def recover(self, trainer_stopped: Callable[[], bool] | None = None) -> dict:
        """Explicit recovery after a crash; never continues or restarts training."""
        if self.journal.data["status"] == "completed":
            raise WindowError("completed_window_does_not_need_recovery")
        ok = self._restore(trainer_stopped)
        self._cleanup_record("recovered" if ok else "recovery_required",
                             status="recovered" if ok else "recovery_required")
        return self.journal.data

