"""Small host-managed business entity directory for bounded extraction context.

The host owns registration and supplies authorized subject IDs. This is not a
model tool, a permission store, a vector index or personal/account memory.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
from typing import Any, Iterable
from zoneinfo import ZoneInfo

from .models import Observation, canonical
from .store import Store


_MAX_SUBJECTS = 128
_MAX_ALIASES = 16
_SHANGHAI = ZoneInfo("Asia/Shanghai")


def _text(value: Any, label: str, maximum: int = 256) -> str:
    if type(value) is not str or not value.strip() or len(value) > maximum:
        raise ValueError(f"Invalid {label}")
    return value.strip()


def _identity(value: Any, label: str = "subject_id") -> str:
    result = _text(value, label)
    if result != value or result == "*":
        raise ValueError(f"Invalid {label}")
    return result


def _ids(values: Any) -> tuple[str, ...]:
    if type(values) not in {list, tuple, set, frozenset} or len(values) > _MAX_SUBJECTS:
        raise ValueError("An explicit bounded collection of subject IDs is required")
    return tuple(sorted({_identity(value) for value in values}))


def _entry(row: Any, subject_id: str) -> dict[str, Any]:
    if row is None:
        return {"subject_id": subject_id, "name": None, "kind": None,
                "project_id": None, "aliases": [], "resolved": False}
    return {"subject_id": row["subject_id"], "name": row["name"], "kind": row["kind"],
            "project_id": row["project_id"], "aliases": json.loads(row["aliases_json"]),
            "resolved": True}


def _time_context(observation: Observation) -> dict[str, Any]:
    created = observation.payload.get("created_at_ms")
    use_created = type(created) is int and created >= 0
    timestamp = created if use_created else observation.occurred_at_ms
    result: dict[str, Any] = {
        "timezone": "Asia/Shanghai", "reference_at_ms": timestamp,
        "basis": "message_created_at" if use_created else "source_observation_time",
        "interpretation_only": True,
    }
    try:
        reference = (datetime(1970, 1, 1, tzinfo=timezone.utc)
                     + timedelta(milliseconds=timestamp)).astimezone(_SHANGHAI)
        next_monday = reference.date() + timedelta(days=7 - reference.weekday())
    except (OverflowError, ValueError):
        result["reference_available"] = False
        return result
    result.update(
        reference_available=True, reference_at=reference.isoformat(),
        reference_date=reference.date().isoformat(),
        next_calendar_week_start=next_monday.isoformat(),
        next_calendar_week_end=(next_monday + timedelta(days=6)).isoformat(),
        week_rule="Monday through Sunday; relative dates remain interpretations until supported",
    )
    return result


class EntityCatalog:
    def __init__(self, store: Store):
        self.store = store
        with store.transaction() as db:
            db.execute("""CREATE TABLE IF NOT EXISTS entity_catalog(
                subject_id TEXT PRIMARY KEY, name TEXT NOT NULL, kind TEXT NOT NULL,
                project_id TEXT, aliases_json TEXT NOT NULL, updated_at_ms INTEGER NOT NULL
            )""")

    def register(self, id: str, name: str, kind: str = "project",
                 project_id: str | None = None, aliases: Iterable[str] = ()) -> None:
        """Host-only upsert; replaces display fields/aliases for one stable ID.

        Never expose this method through a model tool or derive its arguments
        from unreviewed model output. A parent project ID does not grant access.
        """
        subject_id = _identity(id)
        name, kind = _text(name, "name"), _text(kind, "kind", 64)
        project_id = None if project_id is None else _identity(project_id, "project_id")
        if type(aliases) not in {list, tuple, set, frozenset} or len(aliases) > _MAX_ALIASES:
            raise ValueError("Aliases must be a bounded explicit collection")
        normalized = sorted({_text(alias, "alias") for alias in aliases})
        with self.store.transaction() as db:
            db.execute("""INSERT INTO entity_catalog VALUES(?,?,?,?,?,?)
                ON CONFLICT(subject_id) DO UPDATE SET name=excluded.name,kind=excluded.kind,
                project_id=excluded.project_id,aliases_json=excluded.aliases_json,
                updated_at_ms=excluded.updated_at_ms""",
                (subject_id, name, kind, project_id, canonical(normalized), self.store.clock()))

    @staticmethod
    def _entries(db: Any, allowed: tuple[str, ...]) -> list[dict[str, Any]]:
        if not allowed:
            return []
        marks = ",".join("?" for _ in allowed)
        rows = db.execute(f"SELECT * FROM entity_catalog WHERE subject_id IN ({marks})", allowed).fetchall()
        indexed = {row["subject_id"]: row for row in rows}
        return [_entry(indexed.get(subject_id), subject_id) for subject_id in allowed]

    def resolve(self, text: str, allowed_subject_ids: Iterable[str]) -> list[dict[str, Any]]:
        """Return all exact whole-string matches inside the supplied subject set.

        Whitespace around the query is ignored; case, substrings and spelling
        are not guessed. Shared aliases remain ambiguous. No global fallback.
        """
        query = _text(text, "query", 1000)
        allowed = _ids(allowed_subject_ids)
        with self.store.transaction() as db:
            entries = self._entries(db, allowed)
        result = []
        for entity in entries:
            matched_on = []
            if query == entity["subject_id"]:
                matched_on.append("id")
            if entity["resolved"] and query == entity["name"]:
                matched_on.append("name")
            if query in entity["aliases"]:
                matched_on.append("alias")
            if matched_on:
                result.append({**entity, "matched_on": matched_on})
        return result

    def context_for(self, observation: Observation) -> dict[str, Any]:
        """Return only the currently configured source subject scope and hints.

        With both subject_id and subject_ids configured, their intersection
        applies, matching FactEngine's independent subject boundary checks.
        """
        with self.store.transaction() as db:
            source = self.store.get_source(observation.source_id)
            status = self.store.source_status(observation.source_id)
            configured = source.settings
            fixed = configured.get("subject_id")
            listed = configured.get("subject_ids")
            allowed = _ids(listed) if listed is not None else ()
            scope_status = "configured"
            if fixed is not None:
                fixed = _identity(fixed)
                if listed is None or fixed in allowed:
                    allowed = (fixed,)
                else:
                    allowed = ()
                    scope_status = "conflicting_configuration"
            elif listed is None:
                scope_status = "missing"
            if not source.enabled or status["availability"] == "denied":
                allowed = ()
                scope_status = "unavailable"
            subjects = self._entries(db, allowed)

        # Conversation metadata is a hint. It never adds a new subject or source.
        payload = observation.payload
        raw_hint = payload.get("subject_id")
        remote = payload.get("remote")
        remote = remote if type(remote) is dict else {}
        thread = payload.get("thread_id") or remote.get("thread_id")
        thread = thread if type(thread) is str and len(thread) <= 256 else None
        parent = observation.parent_id
        parent = parent if type(parent) is str and len(parent) <= 256 else None
        return {
            "source_id": source.source_id, "scope_origin": "source_settings",
            "scope_status": scope_status, "allowed_subject_ids": list(allowed),
            "subjects": subjects,
            "unresolved_subject_ids": [item["subject_id"] for item in subjects if not item["resolved"]],
            "conversation_hint": {
                "subject_id": raw_hint if type(raw_hint) is str and raw_hint in allowed else None,
                "thread_id": thread, "parent_object_id": parent,
                "expands_scope": False,
            },
            "time_context": _time_context(observation),
        }

