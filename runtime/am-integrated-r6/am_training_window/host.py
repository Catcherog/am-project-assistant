"""Adapter to the actual AM ModelCoordinator + Store provider_pause boundary.

Read from the current host: ModelCoordinator.complete takes its original
thread/file locks and tests store.meta('provider_pause') before inference.
This adapter adds an owned, persistent pause via the same Store transaction;
it does not replace the coordinator or create another consumer.

The llama.cpp manager itself is not in the supplied source snapshot, so its
two exact lifecycle operations must be supplied from the original manager.
"""
from __future__ import annotations

from dataclasses import replace
import math
import time
from typing import Callable, Protocol
import uuid

from .window import HostBinding, HostObservation, WindowError


OWNER_KEY = "resource_window_owner"
PAUSE_KEY = "provider_pause"
PREFIX = "AM_RESOURCE_WINDOW:"


def _owner(value: str) -> str:
    parsed = str(uuid.UUID(value))
    if parsed != value:
        raise WindowError("window_owner_not_canonical")
    return parsed


def _get(connection, key):
    row = connection.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
    return row[0] if row else None


class StoreMaintenanceFence:
    """Two-phase CAS: stop new claims, drain, then seal the inference pause.

    Setting provider_pause before active tasks finish can turn their later
    model calls into failed/unknown tasks. The claim guards in host_overlay
    are mandatory: owner-only draining must block new jobs before seal().
    """
    def __init__(self, store):
        if not callable(getattr(store, "tx", None)) or not callable(getattr(store, "rows", None)):
            raise TypeError("original Store.tx and Store.rows are required")
        self.store = store

    def inspect(self) -> str | None:
        # One SELECT observes a consistent SQLite statement snapshot without
        # starting an unnecessary write transaction or running write guards.
        values = {row["key"]: row["value"] for row in self.store.rows(
            "SELECT key,value FROM meta WHERE key IN (?,?)", (OWNER_KEY, PAUSE_KEY))}
        owner, pause = values.get(OWNER_KEY), values.get(PAUSE_KEY)
        if owner is None:
            if pause:
                raise WindowError("preexisting_or_external_provider_pause")
            return None
        _owner(owner)
        if pause not in (None, "", PREFIX + owner):
            # Unknown-stop errors have priority. Never clear them as if they
            # were our ordinary temporary maintenance pause.
            raise WindowError("maintenance_pause_replaced_by_external_reason")
        return owner

    def claim(self, window_id: str):
        owner = _owner(window_id)
        with self.store.tx() as connection:
            old_owner, pause = _get(connection, OWNER_KEY), _get(connection, PAUSE_KEY)
            if old_owner == owner and pause in (None, "", PREFIX + owner):
                return
            if old_owner is not None or pause:
                raise WindowError("provider_pause_or_window_already_owned")
            connection.execute("INSERT INTO meta(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", (OWNER_KEY, owner))

    def seal(self, window_id: str):
        """Only after active work drained; prevents future inference reloads."""
        owner = _owner(window_id)
        with self.store.tx() as connection:
            old_owner, pause = _get(connection, OWNER_KEY), _get(connection, PAUSE_KEY)
            if old_owner != owner or pause not in (None, "", PREFIX + owner):
                raise WindowError("cannot_seal_changed_or_foreign_pause")
            connection.execute("INSERT INTO meta(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", (PAUSE_KEY, PREFIX + owner))

    def release(self, window_id: str):
        owner = _owner(window_id)
        with self.store.tx() as connection:
            old_owner, pause = _get(connection, OWNER_KEY), _get(connection, PAUSE_KEY)
            if old_owner != owner or pause not in (None, "", PREFIX + owner):
                raise WindowError("cannot_release_changed_or_foreign_pause")
            connection.execute("DELETE FROM meta WHERE key=? AND value=?", (PAUSE_KEY, PREFIX + owner))
            connection.execute("DELETE FROM meta WHERE key=? AND value=?", (OWNER_KEY, owner))


class ExactProviderLifecycle(Protocol):
    def suspend(self, expected: HostBinding) -> None: ...
    def restore(self, expected: HostBinding) -> None: ...


class ExistingCoordinatorHost:
    """Real existing admission gate; provider management is an explicit port.

    read_observation must freshly inspect service, metadata, configuration,
    queue and exact model facts. It may not manufacture an idle/healthy proof.
    It need not set fence_owner: that value is read from original Store here.
    Lifecycle callbacks run only while TrainingWindow holds the original lock.
    """
    def __init__(self, *, coordinator, store,
                 read_observation: Callable[[], HostObservation],
                 lifecycle: ExactProviderLifecycle, drain_timeout_seconds: float):
        if getattr(coordinator, "store", None) is not store:
            raise WindowError("coordinator_store_mismatch")
        if not hasattr(coordinator, "lock") or not callable(getattr(coordinator, "complete", None)):
            raise WindowError("original_model_coordinator_required")
        if not math.isfinite(drain_timeout_seconds) or drain_timeout_seconds < 0:
            raise ValueError("explicit finite nonnegative drain budget required")
        self.coordinator, self.store = coordinator, store
        self.read_observation, self.lifecycle = read_observation, lifecycle
        self.drain_timeout = drain_timeout_seconds
        self.fence = StoreMaintenanceFence(store)

    def inspect(self):
        observed = self.read_observation()
        if not isinstance(observed, HostObservation):
            raise WindowError("native_host_observation_missing")
        return replace(observed, fence_owner=self.fence.inspect())

    def close_admission(self, window_id):
        self.fence.claim(window_id)

    def drain(self, window_id):
        if self.fence.inspect() != window_id:
            raise WindowError("maintenance_window_not_owned")
        # Existing in-process work owns this exact lock. Cross-process work is
        # checked by the original file lock immediately before lifecycle change.
        deadline = time.monotonic() + self.drain_timeout
        while True:
            acquired = self.coordinator.lock.acquire(timeout=max(0, deadline-time.monotonic()))
            if not acquired:
                raise WindowError("existing_inference_not_drained")
            self.coordinator.lock.release()
            observed = self.inspect()
            if not observed.active_inference and not observed.pending_business:
                break
            if time.monotonic() >= deadline:
                raise WindowError("native_business_or_inference_still_pending")
            time.sleep(min(.1, max(0, deadline-time.monotonic())))
        self.fence.seal(window_id)

    def suspend_exact_model(self, window_id, binding):
        if self.fence.inspect() != window_id:
            raise WindowError("maintenance_window_not_owned")
        self.lifecycle.suspend(binding)

    def restore_exact_model(self, window_id, binding):
        if self.fence.inspect() != window_id:
            raise WindowError("maintenance_window_not_owned")
        self.lifecycle.restore(binding)

    def open_admission(self, window_id):
        self.fence.release(window_id)
