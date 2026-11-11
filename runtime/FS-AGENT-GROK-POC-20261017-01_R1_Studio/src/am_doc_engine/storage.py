"""Small SQLite reference repository with idempotency and fenced leases.

Production integration may adapt these methods to AM Store.tx() and its lock.
No other AM tables are read or mutated; all table names are prefixed amdoc_.
"""
from __future__ import annotations

import json
import sqlite3
import threading
import time
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

from .models import (DraftDocument, FactSnapshot, IdempotencyConflict, RunBusy, RevisionConflict,
                     TemplateProfile, canonical, document_from_dict, snapshot_from_dict)


@dataclass(frozen=True)
class RunLease:
    key: str
    owner: str
    cached: DraftDocument | None = None


class SQLiteRunStore:
    def __init__(self, path: str | Path = ":memory:", *, clock: Callable[[], float] = time.time):
        self.path = str(path)
        self.clock = clock
        self._lock = threading.RLock()
        self.conn = sqlite3.connect(self.path, timeout=10, isolation_level=None, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA busy_timeout=10000")
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.executescript('''
        CREATE TABLE IF NOT EXISTS amdoc_runs (
            run_key TEXT PRIMARY KEY, input_hash TEXT NOT NULL, status TEXT NOT NULL,
            owner TEXT NOT NULL, lease_until REAL NOT NULL, result_json TEXT,
            error_code TEXT, updated_at REAL NOT NULL
        );
        CREATE TABLE IF NOT EXISTS amdoc_template_snapshots (
            content_hash TEXT PRIMARY KEY, tenant_id TEXT NOT NULL,
            template_id TEXT NOT NULL, revision TEXT NOT NULL, payload_json TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS amdoc_fact_snapshots (
            run_key TEXT NOT NULL, content_hash TEXT NOT NULL, payload_json TEXT NOT NULL,
            PRIMARY KEY (run_key, content_hash)
        );
        ''')

    @contextmanager
    def tx(self):
        with self._lock:
            self.conn.execute("BEGIN IMMEDIATE")
            try:
                yield self.conn
            except BaseException:
                self.conn.rollback()
                raise
            else:
                self.conn.commit()

    def pin_template(self, profile: TemplateProfile) -> None:
        with self.tx() as conn:
            conn.execute("INSERT OR IGNORE INTO amdoc_template_snapshots VALUES (?,?,?,?,?)",
                         (profile.content_hash, profile.tenant_id, profile.template_id,
                          profile.revision, canonical(profile)))

    def pin_snapshot(self, run_key: str, snapshot: FactSnapshot) -> None:
        """Internal host API. The engine passes only the already authorized projection."""
        with self.tx() as conn:
            conn.execute("INSERT OR IGNORE INTO amdoc_fact_snapshots VALUES (?,?,?)",
                         (run_key, snapshot.content_hash, canonical(snapshot)))

    def read_pinned_snapshot(self, run_key: str, content_hash: str) -> FactSnapshot | None:
        """Internal audit read. Host reauthorizes before displaying any retained evidence."""
        with self._lock:
            row = self.conn.execute("SELECT payload_json FROM amdoc_fact_snapshots WHERE run_key=? AND content_hash=?",
                                    (run_key, content_hash)).fetchone()
        return snapshot_from_dict(json.loads(row[0])) if row else None

    def claim(self, key: str, input_hash: str, *, lease_seconds: float = 180) -> RunLease:
        if not 1 <= lease_seconds <= 3600:
            raise ValueError("invalid lease duration")
        owner = uuid.uuid4().hex
        stamp = self.clock()
        with self.tx() as conn:
            row = conn.execute("SELECT * FROM amdoc_runs WHERE run_key=?", (key,)).fetchone()
            if row:
                if row["input_hash"] != input_hash:
                    raise IdempotencyConflict("相同请求标识对应的模板、资料或要求已改变；请使用新的草稿修订请求")
                if row["status"] == "complete":
                    return RunLease(key, "", document_from_dict(json.loads(row["result_json"])))
                if row["status"] == "running" and row["lease_until"] > stamp:
                    raise RunBusy("同一文档请求正在处理")
                conn.execute("UPDATE amdoc_runs SET status='running',owner=?,lease_until=?,error_code=NULL,updated_at=? WHERE run_key=?",
                             (owner, stamp + lease_seconds, stamp, key))
            else:
                conn.execute("INSERT INTO amdoc_runs VALUES (?,?, 'running', ?,?,NULL,NULL,?)",
                             (key, input_hash, owner, stamp + lease_seconds, stamp))
        return RunLease(key, owner)

    def complete(self, lease: RunLease, document: DraftDocument) -> None:
        stamp = self.clock()
        with self.tx() as conn:
            count = conn.execute("UPDATE amdoc_runs SET status='complete',result_json=?,updated_at=? "
                                 "WHERE run_key=? AND owner=? AND status='running' AND lease_until>?",
                                 (canonical(document), stamp, lease.key, lease.owner, stamp)).rowcount
            if count != 1:
                raise RevisionConflict("作业租约已失效，拒绝保存迟到结果")

    def fail(self, lease: RunLease, code: str) -> None:
        with self.tx() as conn:
            conn.execute("UPDATE amdoc_runs SET status='failed',error_code=?,updated_at=? "
                         "WHERE run_key=? AND owner=? AND status='running'",
                         (code[:80], self.clock(), lease.key, lease.owner))

    def close(self):
        self.conn.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
