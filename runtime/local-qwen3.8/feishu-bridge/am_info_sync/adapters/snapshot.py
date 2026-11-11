"""Explicit mapping bridge for an existing authorized Meegle/Tasks reader.

This module does not invent a CLI or cloud endpoint. The host application supplies
the existing read-only callable, while IDs and fields stay in registered settings.
"""
from __future__ import annotations

from collections.abc import Callable, Mapping
import base64
import json
import re
from typing import Any

from ..models import (Discovery, InvalidCursor, Observation, Page, RetryableError,
                      ScopeViolation, Source, SyncError, canonical, digest)
from .http import InvalidResponse, remote_id
from .messages import _timestamp


SnapshotFetcher = Callable[[Source, int, int, str | None], Mapping[str, Any]]


def _select(record: Mapping[str, Any], path: str) -> Any:
    value: Any = record
    for key in path.split("."):
        if not isinstance(value, Mapping) or key not in value:
            raise InvalidResponse("Registered snapshot field was missing")
        value = value[key]
    return value


class SnapshotAdapter:
    """fetcher(source,start_ms,end_ms,remote_cursor) -> normalized envelope.

    Envelope: {items:[...], has_more:bool, next_cursor:str|None,
               discoveries:[{kind,remote_id,reason}] (optional)}.
    settings.allowed_object_ids and settings.field_map are mandatory. Canonical
    field_map keys object_id and updated_at_ms are required. Optional canonical
    keys: text, version, deleted, completeness; other mapped fields are retained
    under payload.fields. No object can change its own source ACL or scope.
    """

    def __init__(self, fetcher: SnapshotFetcher):
        self.fetcher = fetcher

    def fetch_page(self, source: Source, start_ms: int, end_ms: int,
                   cursor: str | None = None) -> Page:
        if not source.enabled:
            raise ScopeViolation("Source is disabled")
        if source.kind not in {"project_snapshot", "task_snapshot", "snapshot"}:
            raise ScopeViolation("Unsupported snapshot source kind")
        if start_ms < source.start_ms or end_ms < start_ms:
            raise ScopeViolation("Snapshot time request is outside registered bounds")
        allowed = source.settings.get("allowed_object_ids", source.settings.get("object_ids"))
        if not isinstance(allowed, (list, tuple)) or not allowed or any(not isinstance(v, str) or not v for v in allowed):
            raise ScopeViolation("Snapshot requires explicit allowed_object_ids")
        field_map = source.settings.get("field_map")
        if not isinstance(field_map, dict) or not {"object_id", "updated_at_ms"}.issubset(field_map):
            raise ScopeViolation("Snapshot requires registered identity/update field mappings")
        for name, path in field_map.items():
            if (not isinstance(name, str) or not name or not isinstance(path, str)
                    or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)*", path)):
                raise ScopeViolation("Snapshot field mapping is invalid")
        context = {"v": 1, "source": source.source_id, "remote_id": source.remote_id,
                   "start_ms": start_ms, "end_ms": end_ms,
                   "scope_hash": digest({"ids": sorted(allowed), "fields": field_map})}
        remote_cursor = None
        if cursor is not None:
            try:
                envelope = json.loads(base64.urlsafe_b64decode(cursor.encode("ascii")))
            except (ValueError, TypeError, UnicodeError):
                raise InvalidCursor("Snapshot cursor is invalid") from None
            if not isinstance(envelope, dict) or envelope.get("context") != context:
                raise InvalidCursor("Snapshot cursor belongs to a different scope or window")
            remote_cursor = envelope.get("token")
            if not isinstance(remote_cursor, str) or not remote_cursor:
                raise InvalidCursor("Snapshot cursor has no remote token")
        try:
            data = self.fetcher(source, start_ms, end_ms, remote_cursor)
        except SyncError:
            raise
        except Exception:
            raise RetryableError("Snapshot reader failed") from None
        if not isinstance(data, Mapping):
            raise InvalidResponse("Snapshot reader must return an explicit envelope")
        if "code" in data and data["code"] != 0:
            raise InvalidResponse("Snapshot reader returned a nonzero API code")
        items, more = data.get("items"), data.get("has_more")
        if not isinstance(items, list) or type(more) is not bool:
            raise InvalidResponse("Snapshot response lacks items/has_more")
        token = data.get("next_cursor") if more else None
        if more and (not isinstance(token, str) or not token or token == remote_cursor):
            raise InvalidCursor("Snapshot cursor did not advance")
        observations: list[Observation] = []
        allowed_set = set(allowed)
        seen_ids: set[str] = set()
        for item in items:
            if not isinstance(item, Mapping):
                raise InvalidResponse("Snapshot row must be an object")
            mapped = {name: _select(item, path) for name, path in field_map.items()}
            identity = mapped["object_id"]
            if not isinstance(identity, str) or identity not in allowed_set:
                raise ScopeViolation("Snapshot returned an object outside registered IDs")
            if identity in seen_ids:
                raise InvalidResponse("Snapshot page returned duplicate object IDs")
            seen_ids.add(identity)
            updated = _timestamp(mapped["updated_at_ms"], "updated_at_ms")
            deleted = mapped.get("deleted", False)
            if type(deleted) is not bool:
                raise InvalidResponse("Snapshot deleted field must be boolean")
            text = mapped.get("text", "")
            if not isinstance(text, str):
                raise InvalidResponse("Snapshot text field must be a string")
            completeness = mapped.get("completeness", "complete")
            if completeness not in {"complete", "partial", "unreadable"}:
                raise InvalidResponse("Snapshot completeness is invalid")
            try:
                body_hash = digest(mapped)
            except (ValueError, TypeError):
                raise InvalidResponse("Snapshot selected fields are not canonical JSON") from None
            version = f"{updated:020d}:{body_hash}"
            observations.append(Observation(source.source_id, identity, version, updated,
                                            text="" if deleted else text,
                                            payload={"fields": mapped, "remote_version": mapped.get("version"),
                                                     "scope": "registered_fields", "source_kind": source.kind,
                                                     "trusted_instruction": False},
                                            deleted=deleted, completeness=completeness))
        discoveries: list[Discovery] = []
        candidates = data.get("discoveries", [])
        if not isinstance(candidates, list):
            raise InvalidResponse("Snapshot discoveries must be a list")
        for candidate in candidates:
            if not isinstance(candidate, Mapping) or candidate.get("kind") not in {"docx", "im_thread"}:
                raise InvalidResponse("Snapshot discovery kind is unsupported")
            # Deliberately ignore ACL/source settings supplied by upstream content.
            discoveries.append(Discovery(candidate["kind"], remote_id(candidate.get("remote_id")),
                                         "Linked resource observed; independent authorization required"))
        next_cursor = None
        if token:
            next_cursor = base64.urlsafe_b64encode(canonical({"context": context, "token": token}).encode()).decode()
        return Page(tuple(observations), next_cursor, tuple(discoveries), time_coverage=False)
