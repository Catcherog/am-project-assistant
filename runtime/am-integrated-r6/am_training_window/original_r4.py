"""Maintenance-only adapters to original R4 paths and metric semantics.

These helpers do not edit or replace R4. The normal original pilot executor
still constructs OriginalLease and applies its own 24/5 GiB, pressure, swap and
pending-business policy. A maintenance lock is released before launching it.
"""
from __future__ import annotations

import contextlib
from datetime import datetime, timezone
import fcntl
import json
import os
from pathlib import Path
import platform
import sqlite3
import subprocess
import urllib.request


class OriginalCoordinatorLock:
    """Only the existing coordination inode; never create an experiment lock."""
    def __init__(self, manifest, *, validate_paths=None):
        if validate_paths is None:
            from training.mlx_r4_resources import validate_service_paths
            validate_paths = validate_service_paths
        self.manifest, self.validate_paths = manifest, validate_paths
        self.stream = None

    def __enter__(self):
        paths = self.validate_paths(self.manifest)
        lock = Path(paths["lease_file"])
        if lock.name != "model-coordinator.lock" or not lock.is_file():
            raise ValueError("original coordinator lock required")
        stream = lock.open("rb")
        try:
            fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
            opened, current = os.fstat(stream.fileno()), lock.stat()
            if (opened.st_dev, opened.st_ino) != (current.st_dev, current.st_ino):
                raise RuntimeError("original lock path inode changed")
            # Recheck protected configuration after acquiring the real lock.
            if Path(self.validate_paths(self.manifest)["lease_file"]) != lock:
                raise RuntimeError("original lock binding changed")
            self.stream = stream
            return self
        except BaseException:
            stream.close()
            raise

    def __exit__(self, *_):
        if self.stream is not None:
            try:
                fcntl.flock(self.stream, fcntl.LOCK_UN)
            finally:
                self.stream.close()
                self.stream = None


def read_original_metrics(manifest) -> dict:
    """Read current metrics using R4's same system/DB/provider definitions.

    This can report available memory below 24 GiB without raising. It does not
    authorize training, reserve resources, acquire a lease, or load a model.
    Native environments must retain the same existing psutil dependency.
    """
    if platform.system() != "Darwin":
        raise RuntimeError("original resource observations require macOS")
    from training.mlx_r4_resources import TERMINAL, validate_service_paths
    import psutil
    paths = validate_service_paths(manifest)
    memory, swap = psutil.virtual_memory(), psutil.swap_memory()
    pressure = int(subprocess.run(
        ["/usr/sbin/sysctl", "-n", "kern.memorystatus_vm_pressure_level"],
        check=True, capture_output=True, text=True, timeout=3,
    ).stdout.strip())

    class NoRedirect(urllib.request.HTTPRedirectHandler):
        def redirect_request(self, *args, **kwargs):
            raise RuntimeError("metadata redirect is not permitted")

    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), NoRedirect())
    with opener.open("http://127.0.0.1:11434/api/ps", timeout=2) as response:
        payload = response.read(1024 * 1024 + 1)
        if len(payload) > 1024 * 1024:
            raise RuntimeError("metadata response too large")
        models = json.loads(payload)["models"]
    if not isinstance(models, list):
        raise RuntimeError("metadata models list missing")
    pending = 0
    with contextlib.closing(sqlite3.connect(paths["gateway_db"].as_uri() + "?mode=ro", uri=True, timeout=0.3)) as db:
        pending += db.execute("SELECT count(*) FROM am_harness_runs WHERE state IN ('queued','running')").fetchone()[0]
        pending += db.execute("SELECT count(*) FROM am_inbox WHERE state IN ('received','processing','harness_admitting') AND expires>strftime('%s','now')").fetchone()[0]
        pending += sum(count for state, count in db.execute("SELECT state,count(*) FROM tasks GROUP BY state") if state not in TERMINAL)
    with contextlib.closing(sqlite3.connect(paths["chat_db"].as_uri() + "?mode=ro", uri=True, timeout=0.3)) as db:
        pending += sum(count for state, count in db.execute("SELECT state,count(*) FROM inbox GROUP BY state") if state not in TERMINAL)
    return {"available_memory_GiB": memory.available / 2**30,
            "memory_pressure": pressure, "swap_used_GiB": swap.used / 2**30,
            "pending_business": pending, "loaded_models": models,
            "time_utc": datetime.now(timezone.utc).isoformat(),
            "resource_gate_status": "not_checked",
            "metric_semantics": "original_R4_psutil_and_existing_metadata_and_queue_SQL"}
