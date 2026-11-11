"""Fresh adapter to existing R5-FIX1 validation; never caches executable argv."""
from pathlib import Path

from .window import GateObservation, WindowError


class OriginalR5InputGate:
    def __init__(self, plan_path):
        self.plan_path = Path(plan_path)
        if not self.plan_path.is_absolute():
            raise ValueError("use original absolute sealed plan path")

    def __call__(self) -> GateObservation:
        # Existing code validates original artifacts, holdout and live metadata,
        # then rereads all input bindings. No model or trainer is launched.
        from am_style_r5.pilot import execution_argv
        from am_style_r5.common import R5Error
        try:
            verified = execution_argv(str(self.plan_path))
        except R5Error as exc:
            # Keep the original concrete blocker in the private local journal.
            # Publication must redact private paths, not erase the reason.
            raise WindowError("original_R5_input_gate_blocked: " + str(exc)) from exc
        # gate hash covers exact plan context + all bound files. Never retain or
        # execute verified['argv']: real execution must call pilot-execute fresh.
        return GateObservation(True, verified["holdout_gate_sha256"])
