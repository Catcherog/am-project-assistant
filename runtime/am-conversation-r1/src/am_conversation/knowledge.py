"""Scoped, versioned work knowledge with explicit owner approval.

This module stores AM's own work records.  It has no access to account memories,
external services, embeddings, or a language model.  Search uses local Chinese
characters/bigrams and word matching; its score is not a confidence estimate.
"""
from __future__ import annotations

from contextlib import contextmanager
import json
import math
from pathlib import Path
import re
import sqlite3
import threading
import time
import unicodedata
import uuid
from collections.abc import Mapping
from typing import Any, Callable, Iterator

from .types import AccessContext, ConversationError, canonical


KINDS = frozenset({"business", "rule", "fact", "case", "style"})
SCOPES = frozenset({"tenant", "project", "private"})
_HAN = re.compile(r"[\u3400-\u4dbf\u4e00-\u9fff]+")
_WORDS = re.compile(r"[a-z0-9]+(?:[-_][a-z0-9]+)*")


def _normalise(text: str) -> str:
    return unicodedata.normalize("NFKC", text).casefold()


def _terms(text: str) -> tuple[set[str], set[str], set[str]]:
    spans = _HAN.findall(text)
    characters = set("".join(spans))
    bigrams = {span[i:i + 2] for span in spans for i in range(len(span) - 1)}
    return set(_WORDS.findall(text)), characters, bigrams


def _lexical_score(query: str, content: str) -> float:
    """Return a lexical relevance score, never a semantic confidence value."""
    query, content = _normalise(query), _normalise(content)
    words, characters, bigrams = _terms(query)
    content_words, content_characters, content_bigrams = _terms(content)
    word_hits = words & content_words
    character_hits = characters & content_characters
    bigram_hits = bigrams & content_bigrams
    phrase_hit = bool(query.strip()) and query.strip() in content
    # A lone shared character in a longer query is usually Chinese stop-word
    # noise.  Keep useful single-character queries and two-character overlap.
    if not (phrase_hit or word_hits or bigram_hits or
            (len(characters) == 1 and character_hits) or len(character_hits) >= 2):
        return 0.0
    return (12.0 * phrase_hit + 4.0 * len(word_hits) +
            3.0 * len(bigram_hits) + 0.25 * len(character_hits) +
            (len(word_hits) + len(bigram_hits)) / max(1, len(words) + len(bigrams)))


class KnowledgeStore:
    """SQLite knowledge store; all public reads reapply the current access scope.

    ``owner_ids`` is trusted configuration keyed by ``(tenant_id, app_id)``.
    Both membership in that configuration and the current ``ctx.is_owner``
    capability are required for owner operations.  Owner status never widens
    project or private-session visibility.

    A submitted version is immutable.  A new candidate does not remove the old
    approved version from search; approving the new version replaces it in one
    transaction.  Only the latest submitted version can receive new approval.
    """

    def __init__(self, db_path: str | Path, *, owner_ids: Mapping | None = None,
                 clock: Callable[[], float] = time.time) -> None:
        self._owners: dict[tuple[str, str], frozenset[str]] = {}
        for key, actors in (owner_ids or {}).items():
            if (not isinstance(key, tuple) or len(key) != 2 or
                    any(not isinstance(part, str) or not part for part in key) or
                    isinstance(actors, str)):
                raise ConversationError("INVALID_OWNER_CONFIGURATION")
            try:
                actor_set = frozenset(actors)
            except TypeError as exc:
                raise ConversationError("INVALID_OWNER_CONFIGURATION") from exc
            if any(not isinstance(actor, str) or not actor for actor in actor_set):
                raise ConversationError("INVALID_OWNER_CONFIGURATION")
            self._owners[key] = actor_set
        self._clock = clock
        self._lock = threading.RLock()
        self._closed = False
        self._db = sqlite3.connect(str(db_path), timeout=5.0, isolation_level=None,
                                   check_same_thread=False)
        self._db.row_factory = sqlite3.Row
        self._db.execute("PRAGMA busy_timeout = 5000")
        self._db.execute("PRAGMA foreign_keys = ON")
        self._db.execute("PRAGMA journal_mode = WAL")
        self._db.executescript("""
            CREATE TABLE IF NOT EXISTS knowledge_records (
                id TEXT PRIMARY KEY,
                tenant_id TEXT NOT NULL,
                app_id TEXT NOT NULL,
                logical_id TEXT NOT NULL,
                version INTEGER NOT NULL CHECK (version >= 1),
                kind TEXT NOT NULL CHECK (kind IN ('business','rule','fact','case','style')),
                content TEXT NOT NULL,
                source TEXT NOT NULL,
                scope TEXT NOT NULL CHECK (scope IN ('tenant','project','private')),
                status TEXT NOT NULL CHECK (status IN ('candidate','approved','rejected','superseded')),
                project_id TEXT,
                session_id TEXT,
                actor_id TEXT NOT NULL,
                created_at REAL NOT NULL,
                approved_at REAL,
                reviewed_at REAL,
                reviewed_by TEXT,
                UNIQUE (tenant_id, app_id, logical_id, version),
                CHECK ((scope = 'tenant' AND project_id IS NULL AND session_id IS NULL)
                    OR (scope = 'project' AND project_id IS NOT NULL AND session_id IS NULL)
                    OR (scope = 'private' AND session_id IS NOT NULL))
            );
            CREATE UNIQUE INDEX IF NOT EXISTS knowledge_active_version
                ON knowledge_records (tenant_id, app_id, logical_id)
                WHERE status = 'approved';
            CREATE INDEX IF NOT EXISTS knowledge_visibility
                ON knowledge_records (tenant_id, app_id, status, scope, project_id, session_id);
        """)

    def close(self) -> None:
        with self._lock:
            if not self._closed:
                self._db.close()
                self._closed = True

    def __enter__(self) -> KnowledgeStore:
        self._ensure_open()
        return self

    def __exit__(self, *_: Any) -> None:
        self.close()

    def _ensure_open(self) -> None:
        if self._closed:
            raise ConversationError("KNOWLEDGE_STORE_CLOSED")

    def _now(self) -> float:
        value = self._clock()
        if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
            raise ConversationError("INVALID_CLOCK")
        return float(value)

    def _is_owner(self, ctx: AccessContext) -> bool:
        return ctx.is_owner and ctx.actor_id in self._owners.get((ctx.tenant_id, ctx.app_id), ())

    def _require_owner(self, ctx: AccessContext) -> None:
        if not self._is_owner(ctx):
            raise ConversationError("KNOWLEDGE_OWNER_REQUIRED")

    @contextmanager
    def _transaction(self) -> Iterator[None]:
        with self._lock:
            self._ensure_open()
            savepoint = "knowledge_" + uuid.uuid4().hex if self._db.in_transaction else None
            self._db.execute(f"SAVEPOINT {savepoint}" if savepoint else "BEGIN IMMEDIATE")
            try:
                from contextlib import nullcontext
                guard = getattr(self, "local_write_guard", None)
                with guard(self._db) if guard is not None and not savepoint else nullcontext():
                    yield
                    self._db.execute(f"RELEASE SAVEPOINT {savepoint}" if savepoint else "COMMIT")
            except BaseException:
                if self._db.in_transaction:
                    try:
                        if savepoint:
                            self._db.execute(f"ROLLBACK TO SAVEPOINT {savepoint}")
                            self._db.execute(f"RELEASE SAVEPOINT {savepoint}")
                        else:
                            self._db.execute("ROLLBACK")
                    except sqlite3.Error:
                        # Preserve the primary commit/operation exception. In
                        # particular, a committed local DB cannot be rolled back.
                        pass
                raise

    @staticmethod
    def _record(row: sqlite3.Row) -> dict[str, Any]:
        record = dict(row)
        record["source"] = json.loads(record["source"])
        return record

    @staticmethod
    def _version(expected_version: int) -> None:
        if type(expected_version) is not int or expected_version < 1:
            raise ConversationError("INVALID_KNOWLEDGE_VERSION")

    @staticmethod
    def _limit(limit: int, maximum: int) -> None:
        if type(limit) is not int or not 1 <= limit <= maximum:
            raise ConversationError("INVALID_KNOWLEDGE_LIMIT")

    @staticmethod
    def _source(source: dict[str, Any]) -> str:
        if not isinstance(source, dict):
            raise ConversationError("INVALID_KNOWLEDGE_SOURCE")
        for field in ("uri", "observed_at"):
            if not isinstance(source.get(field), str) or not source[field].strip():
                raise ConversationError("INVALID_KNOWLEDGE_SOURCE")
        try:
            encoded = canonical(source)
        except (TypeError, ValueError) as exc:
            raise ConversationError("INVALID_KNOWLEDGE_SOURCE") from exc
        if len(encoded) > 65536:
            raise ConversationError("INVALID_KNOWLEDGE_SOURCE")
        return encoded

    @staticmethod
    def _check_project(ctx: AccessContext, project_id: str | None) -> None:
        if project_id is not None:
            if not isinstance(project_id, str) or not project_id or len(project_id) > 256:
                raise ConversationError("INVALID_PROJECT")
            if not ctx.can_access_project(project_id):
                raise ConversationError("PROJECT_FORBIDDEN")

    def _visibility(self, ctx: AccessContext, project_id: str | None = None,
                    *, alias: str = "") -> tuple[str, list[Any]]:
        """Build the complete ACL in SQL, before records can be ranked."""
        self._check_project(ctx, project_id)
        p = f"{alias}." if alias else ""
        parameters: list[Any] = [ctx.tenant_id, ctx.app_id]
        if project_id is not None:
            project_clause = f"{p}project_id = ?"
            project_parameters: list[Any] = [project_id]
        elif ctx.all_projects:
            project_clause = f"{p}project_id IS NOT NULL"
            project_parameters = []
        elif ctx.allowed_project_ids:
            projects = sorted(ctx.allowed_project_ids)
            project_clause = f"{p}project_id IN ({','.join('?' for _ in projects)})"
            project_parameters = projects
        else:
            project_clause, project_parameters = "0", []
        sql = (f"{p}tenant_id = ? AND {p}app_id = ? AND ("
               f"{p}scope = 'tenant' OR ({p}scope = 'project' AND {project_clause}) OR "
               f"({p}scope = 'private' AND {p}session_id = ? AND {p}actor_id = ? AND "
               f"({p}project_id IS NULL OR {project_clause})))")
        parameters.extend(project_parameters)
        parameters.extend([ctx.session_id, ctx.actor_id])
        parameters.extend(project_parameters)
        # Conversation-derived candidates carry the same local source epoch.
        # Independently imported reference material has its own provenance.
        sql += f" AND (json_extract({p}source,'$.uri') NOT LIKE 'feishu-message:%' OR json_extract({p}source,'$.source_access_version') IS ?)"
        parameters.append(ctx.source_access_version)
        return sql, parameters

    def _visible_row(self, ctx: AccessContext, record_id: str) -> sqlite3.Row:
        if not isinstance(record_id, str) or not record_id:
            raise ConversationError("KNOWLEDGE_NOT_FOUND")
        where, params = self._visibility(ctx)
        row = self._db.execute(
            f"SELECT * FROM knowledge_records WHERE id = ? AND {where}",
            [record_id, *params],
        ).fetchone()
        if row is None:
            raise ConversationError("KNOWLEDGE_NOT_FOUND")
        # An unapproved tenant/project candidate must not leak through get(id).
        if (row["status"] != "approved" and row["approved_at"] is None and
                row["actor_id"] != ctx.actor_id and not self._is_owner(ctx)):
            raise ConversationError("KNOWLEDGE_NOT_FOUND")
        return row

    def _validate_submission(self, ctx: AccessContext, *, kind: str, content: str,
                             source: dict[str, Any], scope: str,
                             project_id: str | None) -> str:
        """Validate without writing; also used by the JSONL import dry-run."""
        if not isinstance(kind, str) or not isinstance(scope, str) or kind not in KINDS or scope not in SCOPES:
            raise ConversationError("INVALID_KNOWLEDGE_KIND_OR_SCOPE")
        if not isinstance(content, str) or not content.strip() or len(content) > 100000:
            raise ConversationError("INVALID_KNOWLEDGE_CONTENT")
        if not self._is_owner(ctx) and (kind != "fact" or scope != "private"):
            raise ConversationError("KNOWLEDGE_OWNER_REQUIRED")
        self._check_project(ctx, project_id)
        if (scope == "project" and project_id is None) or (scope == "tenant" and project_id is not None):
            raise ConversationError("INVALID_KNOWLEDGE_SCOPE")
        return self._source(source)

    def submit(self, ctx: AccessContext, *, kind: str, content: str,
               source: dict[str, Any], scope: str = "private",
               project_id: str | None = None, logical_id: str | None = None,
               expected_version: int | None = None) -> dict[str, Any]:
        if isinstance(source,dict) and str(source.get('uri','')).startswith('feishu-message:'):
            source=dict(source,source_access_version=ctx.source_access_version)
        source_json = self._validate_submission(ctx, kind=kind, content=content,
                                               source=source, scope=scope, project_id=project_id)
        if logical_id is None and expected_version is not None:
            raise ConversationError("INVALID_KNOWLEDGE_VERSION")
        if logical_id is not None:
            if not isinstance(logical_id, str) or not logical_id:
                raise ConversationError("KNOWLEDGE_NOT_FOUND")
            self._version(expected_version)  # type: ignore[arg-type]
        session_id = ctx.session_id if scope == "private" else None
        with self._transaction():
            version = 1
            if logical_id is not None:
                latest = self._db.execute(
                    "SELECT * FROM knowledge_records WHERE tenant_id=? AND app_id=? "
                    "AND logical_id=? ORDER BY version DESC LIMIT 1",
                    (ctx.tenant_id, ctx.app_id, logical_id),
                ).fetchone()
                if latest is None:
                    raise ConversationError("KNOWLEDGE_NOT_FOUND")
                # An owner cannot revise an inaccessible private conversation.
                self._visible_row(ctx, latest["id"])
                if latest["version"] != expected_version:
                    raise ConversationError("STALE_KNOWLEDGE_VERSION")
                if (latest["scope"], latest["project_id"], latest["session_id"], latest["kind"]) != (
                        scope, project_id, session_id, kind):
                    raise ConversationError("KNOWLEDGE_SCOPE_IMMUTABLE")
                version = latest["version"] + 1
            else:
                logical_id = uuid.uuid4().hex
            record_id = uuid.uuid4().hex
            self._db.execute(
                "INSERT INTO knowledge_records "
                "(id,tenant_id,app_id,logical_id,version,kind,content,source,scope,status,"
                "project_id,session_id,actor_id,created_at) VALUES (?,?,?,?,?,?,?,?,?,'candidate',?,?,?,?)",
                (record_id, ctx.tenant_id, ctx.app_id, logical_id, version, kind,
                 content, source_json, scope, project_id, session_id, ctx.actor_id, self._now()),
            )
            return self._record(self._visible_row(ctx, record_id))

    def approve(self, ctx: AccessContext, record_id: str,
                expected_version: int) -> dict[str, Any]:
        self._require_owner(ctx)
        self._version(expected_version)
        with self._transaction():
            row = self._visible_row(ctx, record_id)
            if row["version"] != expected_version:
                raise ConversationError("STALE_KNOWLEDGE_VERSION")
            if row["status"] == "approved":
                return self._record(row)
            if row["status"] != "candidate":
                raise ConversationError("KNOWLEDGE_NOT_CANDIDATE")
            latest_version = self._db.execute(
                "SELECT MAX(version) FROM knowledge_records WHERE tenant_id=? AND app_id=? AND logical_id=?",
                (ctx.tenant_id, ctx.app_id, row["logical_id"]),
            ).fetchone()[0]
            if latest_version != expected_version:
                raise ConversationError("STALE_KNOWLEDGE_VERSION")
            # The unique partial index also enforces one approved version across
            # independent processes.  Old candidates cannot later reappear.
            self._db.execute(
                "UPDATE knowledge_records SET status='superseded' WHERE tenant_id=? AND app_id=? "
                "AND logical_id=? AND version<? AND status IN ('approved','candidate')",
                (ctx.tenant_id, ctx.app_id, row["logical_id"], row["version"]),
            )
            reviewed_at = self._now()
            self._db.execute(
                "UPDATE knowledge_records SET status='approved',approved_at=?,reviewed_at=?,reviewed_by=? WHERE id=?",
                (reviewed_at, reviewed_at, ctx.actor_id, record_id),
            )
            return self._record(self._visible_row(ctx, record_id))

    def reject(self, ctx: AccessContext, record_id: str,
               expected_version: int) -> dict[str, Any]:
        self._require_owner(ctx)
        self._version(expected_version)
        with self._transaction():
            row = self._visible_row(ctx, record_id)
            if row["version"] != expected_version:
                raise ConversationError("STALE_KNOWLEDGE_VERSION")
            if row["status"] == "rejected":
                return self._record(row)
            if row["status"] != "candidate":
                raise ConversationError("KNOWLEDGE_NOT_CANDIDATE")
            self._db.execute(
                "UPDATE knowledge_records SET status='rejected',reviewed_at=?,reviewed_by=? WHERE id=?",
                (self._now(), ctx.actor_id, record_id),
            )
            return self._record(self._visible_row(ctx, record_id))

    def get(self, ctx: AccessContext, record_id: str) -> dict[str, Any]:
        with self._lock:
            self._ensure_open()
            return self._record(self._visible_row(ctx, record_id))

    def search(self, ctx: AccessContext, query: str, *, project_id: str | None = None,
               kinds: list[str] | tuple[str, ...] | set[str] | None = None,
               limit: int = 6) -> list[dict[str, Any]]:
        self._limit(limit, 100)
        if not isinstance(query, str) or len(query) > 2000:
            raise ConversationError("INVALID_KNOWLEDGE_QUERY")
        kind_filter: list[str] | None = None
        if kinds is not None:
            if isinstance(kinds, str):
                raise ConversationError("INVALID_KNOWLEDGE_KIND")
            try:
                kind_filter = sorted(set(kinds))
            except (TypeError, ValueError) as exc:
                raise ConversationError("INVALID_KNOWLEDGE_KIND") from exc
            if any(kind not in KINDS for kind in kind_filter):
                raise ConversationError("INVALID_KNOWLEDGE_KIND")
            if not kind_filter:
                return []
        where, parameters = self._visibility(ctx, project_id)
        where += " AND status = 'approved'"
        if kind_filter is not None:
            where += f" AND kind IN ({','.join('?' for _ in kind_filter)})"
            parameters.extend(kind_filter)
        with self._lock:
            self._ensure_open()
            # Filter visibility in SQL first; inaccessible content never reaches
            # the lexical ranker, even when its text is a perfect query match.
            rows = self._db.execute(
                f"SELECT * FROM knowledge_records WHERE {where}", parameters,
            ).fetchall()
        ranked: list[dict[str, Any]] = []
        for row in rows:
            record = self._record(row)
            title = record["source"].get("title", "")
            text = record["content"] + (" " + title if isinstance(title, str) else "")
            score = _lexical_score(query, text) if query.strip() else 0.0
            if query.strip() and score <= 0:
                continue
            record["lexical_score"] = score
            ranked.append(record)
        ranked.sort(key=lambda record: (-record["lexical_score"], -record["created_at"], record["id"]))
        return ranked[:limit]

    def review_candidates(self, ctx: AccessContext, limit: int = 20) -> list[dict[str, Any]]:
        self._limit(limit, 100)
        where, parameters = self._visibility(ctx, alias="k")
        where += (" AND k.status = 'candidate' AND NOT EXISTS (SELECT 1 FROM knowledge_records newer "
                  "WHERE newer.tenant_id=k.tenant_id AND newer.app_id=k.app_id "
                  "AND newer.logical_id=k.logical_id AND newer.version>k.version)")
        if not self._is_owner(ctx):
            where += " AND k.actor_id = ?"
            parameters.append(ctx.actor_id)
        parameters.append(limit)
        with self._lock:
            self._ensure_open()
            rows = self._db.execute(
                f"SELECT k.* FROM knowledge_records k WHERE {where} ORDER BY k.created_at DESC,k.id LIMIT ?",
                parameters,
            ).fetchall()
            return [self._record(row) for row in rows]
