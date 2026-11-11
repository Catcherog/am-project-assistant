"""Reversible Phase 1 install into an already running, patched AM bridge.

Only the deterministic fastpath callback is changed. This function creates no
consumer, provider, model queue, transport, lifecycle binding or outbox. Call
during host configuration, not concurrently with arbitrary attribute mutation.
"""
from __future__ import annotations

from dataclasses import dataclass, field
import inspect
from pathlib import Path
import threading

from .fastpath import FastPathRouter, FocusCache


class InstallationError(RuntimeError):
    def __init__(self, code: str):
        self.code = code
        super().__init__(code)


_INSTALL_LOCK = threading.RLock()


@dataclass
class Installation:
    """Restore only the exact callback this installation still owns."""

    bridge: object
    agent: object
    router: FastPathRouter
    previous: object
    _restored: bool = field(default=False, init=False, repr=False)

    def restore(self) -> bool:
        """Return True once restored; refuse to overwrite another install."""
        with _INSTALL_LOCK:
            if self._restored:
                return False
            if getattr(self.bridge, "agent", None) is not self.agent:
                raise InstallationError("HARNESS_INSTALL_AGENT_CHANGED")
            if getattr(self.agent, "fastpath", None) is not self.router:
                raise InstallationError("HARNESS_INSTALL_CALLBACK_CHANGED")
            self.agent.fastpath = self.previous
            self._restored = True
            return True


def install_fastpath_only(bridge, *, focus_path: str | Path | None = None) -> Installation:
    """Install into the real LegacyConversationBridge with the engine patch.

    Existing lifecycle hooks are rejected: this Phase 1 helper must not obscure
    a partially wired durable runtime or relax ``strict_bound``.  The bridge's
    ordinary process/render/outbox pipeline remains the only delivery path.
    """
    from am_conversation.legacy import LegacyConversationBridge

    with _INSTALL_LOCK:
        if not isinstance(bridge, LegacyConversationBridge) or bridge.enabled is not True:
            raise InstallationError("HARNESS_INSTALL_BRIDGE_INVALID")
        agent = bridge.agent
        if agent is None:
            raise InstallationError("HARNESS_INSTALL_ENGINE_MISSING")
        try:
            parameters = inspect.signature(type(agent).__init__).parameters
        except (ValueError, TypeError):
            raise InstallationError("HARNESS_INSTALL_ENGINE_UNPATCHED") from None
        if (type(agent).__name__ != "ConversationAgent"
                or not type(agent).__module__.startswith("am_conversation.")
                or "fastpath" not in parameters or "lifecycle" not in parameters
                or not callable(getattr(agent, "handle_fastpath", None))
                or not callable(getattr(agent, "_checkpoint", None))
                or not hasattr(agent, "fastpath")):
            raise InstallationError("HARNESS_INSTALL_ENGINE_UNPATCHED")
        if getattr(agent, "lifecycle", None) is not None:
            raise InstallationError("HARNESS_INSTALL_LIFECYCLE_PRESENT")
        if (agent.store is not bridge.store or agent.registry is not bridge.registry
                or not callable(getattr(bridge, "authorize", None))
                or not callable(getattr(bridge.s, "clock", None))
                or not callable(getattr(bridge.projects, "list_projects", None))
                or not callable(getattr(bridge.projects, "read_project", None))):
            raise InstallationError("HARNESS_INSTALL_BINDINGS_INVALID")

        previous = agent.fastpath
        if focus_path is None:
            focus_path = Path(bridge.service.g.config.root) / "state" / "am-harness-r1" / "fastpath-focus.sqlite3"
        path = Path(focus_path)
        path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        existed = path.exists()
        focus = FocusCache(path, clock=bridge.s.clock)
        if not existed:
            path.chmod(0o600)
        router = FastPathRouter(bridge.projects, bridge.registry, authorize=bridge.authorize,
                                focus=focus, clock=bridge.s.clock,
                                max_reply_chars=agent.config.max_reply_chars)
        # Filesystem initialization may take time. Do not replace a callback or
        # lifecycle that another configuration step changed in the meantime.
        if bridge.agent is not agent or agent.fastpath is not previous:
            raise InstallationError("HARNESS_INSTALL_CALLBACK_CHANGED")
        if agent.lifecycle is not None:
            raise InstallationError("HARNESS_INSTALL_LIFECYCLE_PRESENT")
        agent.fastpath = router
        return Installation(bridge, agent, router, previous)
