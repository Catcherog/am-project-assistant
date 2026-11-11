"""A disposable cache of reproducible phases, not an execution/approval journal.

Only read results and explicitly pure local computation may be stored. Every hit
requires current source versions, the same permission fingerprint and renewed
project authorization. Dependency TTLs cap the checkpoint's lifetime.
"""
from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
import hashlib
import json
import math
from pathlib import Path
import re
import sqlite3
import time
from typing import Any, Callable, Iterator, Mapping, Sequence


class CheckpointError(ValueError):
    pass


def canonical(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)


def digest(value: Any) -> str:
    return hashlib.sha256(canonical(value).encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class Checkpoint:
    run_id: str
    phase: str
    value: Any
    input_hash: str
    dependencies: dict[str, str]
    project_ids: tuple[str, ...]
    expires_at: float
    created_at: float
    version: int
    effect: str


class CheckpointStore:
    SCHEMA_VERSION = 1

    def __init__(self, path: str | Path, *, max_bytes: int = 262144, timeout: float = 5.0,
                 clock: Callable[[], float] = time.time) -> None:
        self.path = str(path)
        if self.path == ":memory:":
            raise CheckpointError("USE_FILE_BACKED_SQLITE")
        if type(max_bytes) is not int or max_bytes < 1 or timeout <= 0:
            raise CheckpointError("INVALID_CONFIGURATION")
        self.max_bytes, self.timeout, self.clock = max_bytes, timeout, clock
        Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as db:
            db.execute("PRAGMA journal_mode=WAL")
            db.execute("BEGIN IMMEDIATE")
            db.execute("CREATE TABLE IF NOT EXISTS am_harness_checkpoint_meta (key TEXT PRIMARY KEY, value INTEGER NOT NULL)")
            row = db.execute("SELECT value FROM am_harness_checkpoint_meta WHERE key='schema_version'").fetchone()
            if row is not None and row[0] != self.SCHEMA_VERSION:
                raise CheckpointError("UNSUPPORTED_CHECKPOINT_SCHEMA")
            db.execute("INSERT OR IGNORE INTO am_harness_checkpoint_meta VALUES ('schema_version', ?)", (self.SCHEMA_VERSION,))
            db.execute("""CREATE TABLE IF NOT EXISTS am_harness_checkpoints (
                scope_key TEXT NOT NULL, permission_fingerprint TEXT NOT NULL,
                run_id TEXT NOT NULL, phase TEXT NOT NULL, input_hash TEXT NOT NULL,
                dependencies_json TEXT NOT NULL, dependency_expiries_json TEXT NOT NULL,
                projects_json TEXT NOT NULL, value_json TEXT NOT NULL,
                expires_at REAL NOT NULL, created_at REAL NOT NULL,
                version INTEGER NOT NULL, effect TEXT NOT NULL,
                PRIMARY KEY (scope_key, run_id, phase))""")
            db.execute("CREATE INDEX IF NOT EXISTS am_harness_checkpoint_expiry ON am_harness_checkpoints(expires_at)")

    @contextmanager
    def _connect(self) -> Iterator[sqlite3.Connection]:
        db = sqlite3.connect(self.path, timeout=self.timeout, isolation_level=None)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA busy_timeout=" + str(int(self.timeout * 1000)))
        try:
            yield db
            if db.in_transaction:
                db.commit()
        except BaseException:
            if db.in_transaction:
                db.rollback()
            raise
        finally:
            db.close()

    @staticmethod
    def _identity(scope_key: str, run_id: str, phase: str, permission_fingerprint: str) -> None:
        if any(not isinstance(s, str) or not s or len(s) > 256 for s in (scope_key, run_id, permission_fingerprint)):
            raise CheckpointError("INVALID_CHECKPOINT_IDENTITY")
        if not isinstance(phase, str) or not re.fullmatch(r"[a-z][a-z0-9_.-]{0,63}", phase):
            raise CheckpointError("INVALID_PHASE")

    def _now(self, now: float | None) -> float:
        value = self.clock() if now is None else now
        if type(value) not in (int, float) or not math.isfinite(value) or value <= 0:
            raise CheckpointError("INVALID_CLOCK")
        return value

    def put(self, *, scope_key: str, permission_fingerprint: str, run_id: str, phase: str,
            input_value: Any, value: Any, dependencies: Mapping[str, str],
            dependency_expiries: Mapping[str, float], project_ids: Sequence[str],
            expires_at: float, effect: str = "read", now: float | None = None) -> Checkpoint:
        self._identity(scope_key, run_id, phase, permission_fingerprint)
        now = self._now(now)
        if effect not in {"read", "pure_local"}:
            raise CheckpointError("EFFECT_NOT_CACHEABLE")
        if type(expires_at) not in (int, float) or not math.isfinite(expires_at) or expires_at <= now:
            raise CheckpointError("INVALID_CHECKPOINT_EXPIRY")
        deps = dict(dependencies)
        expiries = dict(dependency_expiries)
        if set(deps) != set(expiries):
            raise CheckpointError("DEPENDENCY_EXPIRY_REQUIRED")
        if any(not isinstance(k, str) or not k or not isinstance(v, str) or not v for k, v in deps.items()):
            raise CheckpointError("INVALID_DEPENDENCIES")
        if any(type(v) not in (int, float) or not math.isfinite(v) or v <= now for v in expiries.values()):
            raise CheckpointError("EXPIRED_DEPENDENCY")
        projects = tuple(sorted(set(project_ids)))
        if any(not isinstance(p, str) or not p or len(p) > 256 for p in projects):
            raise CheckpointError("INVALID_PROJECTS")
        try:
            value_json = canonical(value)
            input_hash = digest(input_value)
            deps_json, expires_json, projects_json = canonical(deps), canonical(expiries), canonical(projects)
        except (TypeError, ValueError) as exc:
            raise CheckpointError("CHECKPOINT_MUST_BE_JSON") from exc
        if len((value_json + deps_json + expires_json + projects_json).encode("utf-8")) > self.max_bytes:
            raise CheckpointError("CHECKPOINT_TOO_LARGE")
        effective_expiry = min([expires_at, *expiries.values()])
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            old = db.execute("SELECT version FROM am_harness_checkpoints WHERE scope_key=? AND run_id=? AND phase=?",
                             (scope_key, run_id, phase)).fetchone()
            version = old[0] + 1 if old else 1
            db.execute("""INSERT INTO am_harness_checkpoints VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(scope_key,run_id,phase) DO UPDATE SET
                permission_fingerprint=excluded.permission_fingerprint,input_hash=excluded.input_hash,
                dependencies_json=excluded.dependencies_json,dependency_expiries_json=excluded.dependency_expiries_json,
                projects_json=excluded.projects_json,value_json=excluded.value_json,
                expires_at=excluded.expires_at,created_at=excluded.created_at,
                version=excluded.version,effect=excluded.effect""",
                       (scope_key, permission_fingerprint, run_id, phase, input_hash,
                        deps_json, expires_json, projects_json, value_json,
                        effective_expiry, now, version, effect))
        return Checkpoint(run_id, phase, json.loads(value_json), input_hash, deps, projects,
                          effective_expiry, now, version, effect)

    def get(self, *, scope_key: str, permission_fingerprint: str, run_id: str, phase: str,
            input_value: Any, current_versions: Mapping[str, str | None],
            authorize_project: Callable[[str], bool], revoked_sources: frozenset[str] = frozenset(),
            current_expiries: Mapping[str, float] | None = None, now: float | None = None) -> Checkpoint | None:
        self._identity(scope_key, run_id, phase, permission_fingerprint)
        now = self._now(now)
        with self._connect() as db:
            row = db.execute("SELECT * FROM am_harness_checkpoints WHERE scope_key=? AND run_id=? AND phase=?",
                             (scope_key, run_id, phase)).fetchone()
        if row is None or row["expires_at"] <= now or row["permission_fingerprint"] != permission_fingerprint:
            return None
        if row["input_hash"] != digest(input_value) or row["effect"] not in {"read", "pure_local"}:
            return None
        deps = json.loads(row["dependencies_json"])
        expiries = json.loads(row["dependency_expiries_json"])
        projects = tuple(json.loads(row["projects_json"]))
        if any(not authorize_project(project) for project in projects):
            return None
        for source_id, version in deps.items():
            if source_id in revoked_sources or current_versions.get(source_id) != version or expiries[source_id] <= now:
                return None
            if current_expiries is not None:
                expiry = current_expiries.get(source_id)
                if type(expiry) not in (int, float) or not math.isfinite(expiry) or expiry <= now:
                    return None
        valid_until = row["expires_at"]
        if current_expiries is not None and deps:
            valid_until = min(valid_until, *(current_expiries[s] for s in deps))
        return Checkpoint(run_id, phase, json.loads(row["value_json"]), row["input_hash"], deps,
                          projects, valid_until, row["created_at"], row["version"], row["effect"])

    def invalidate_sources(self, source_ids: Sequence[str]) -> int:
        ids = set(source_ids)
        if not ids:
            return 0
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            keys = [(r["scope_key"], r["run_id"], r["phase"])
                    for r in db.execute("SELECT scope_key,run_id,phase,dependencies_json FROM am_harness_checkpoints")
                    if ids.intersection(json.loads(r["dependencies_json"]))]
            db.executemany("DELETE FROM am_harness_checkpoints WHERE scope_key=? AND run_id=? AND phase=?", keys)
            return len(keys)

    def revoke_scope(self, scope_key: str) -> int:
        with self._connect() as db:
            return db.execute("DELETE FROM am_harness_checkpoints WHERE scope_key=?", (scope_key,)).rowcount

    def purge_expired(self, *, now: float | None = None) -> int:
        with self._connect() as db:
            return db.execute("DELETE FROM am_harness_checkpoints WHERE expires_at<=?", (self._now(now),)).rowcount
