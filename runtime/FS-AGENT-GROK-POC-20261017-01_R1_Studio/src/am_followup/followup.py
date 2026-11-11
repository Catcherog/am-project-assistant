"""One-shot follow-up admission for the existing AM sender.

This module is deliberately not a scheduler or a Feishu client.  The existing
single AM consumer calls ``dispatch_once`` and its adapter must invoke the
provided ``before_send`` guard immediately before its existing network send.
SQLite coordinates local admissions; it cannot make a remote HTTP side effect
and a local transaction atomic.  Uncertain results therefore retain their
attention reservations and are never retried by this module.

All ``Actor``, gate and readback values are trusted backend inputs.  None of
them authenticates an HTTP callback by its Python type alone.  The host must
verify the original platform callback, owner identity and tenant first.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import secrets
import sqlite3
import uuid
from contextlib import contextmanager, nullcontext
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Callable, ContextManager, Iterator, Mapping, Protocol, Sequence
from zoneinfo import ZoneInfo


class FollowupError(ValueError):
    """A typed rejection whose code can be translated for the owner."""

    def __init__(self, code: str, detail: str = "") -> None:
        self.code = code
        self.detail = detail
        super().__init__(f"{code}: {detail}" if detail else code)


class DispatchBlocked(FollowupError):
    pass


@dataclass(frozen=True)
class Actor:
    """Created only by the host's authenticated callback/session adapter."""

    tenant_id: str
    user_id: str
    session_id: str
    expires_at: datetime


@dataclass(frozen=True)
class Target:
    # ``owner`` means the configured owner, never an arbitrary engineer DM.
    kind: str
    target_id: str
    attention_ids: tuple[str, ...]
    mention_ids: tuple[str, ...] = ()
    directory_version: str = ""
    valid_until: datetime | None = None
    audience: str = "internal"


@dataclass(frozen=True)
class EvidenceSnapshot:
    versions: Mapping[str, str]
    acl_versions: Mapping[str, str]
    task_version: str
    disclosure_scopes: tuple[str, ...]


@dataclass(frozen=True)
class SendWindow:
    not_before: datetime
    expires_at: datetime
    # A trusted calendar adapter supplies the finite approved working dates.
    working_dates: tuple[str, ...]
    timezone: str = "Asia/Shanghai"
    local_start: str = "09:00"
    local_end: str = "18:00"
    allow_outside_hours: bool = False
    calendar_version: str = ""


@dataclass(frozen=True)
class Draft:
    task_id: str
    purpose: str
    episode_id: str
    body: str
    target: Target
    evidence: EvidenceSnapshot
    window: SendWindow
    producer_id: str = "am"
    action: str = "send_followup"
    # Only an unexecuted earlier intent may be explicitly replaced.
    supersedes_intent_id: str | None = None


@dataclass(frozen=True)
class GateResult:
    allowed: bool
    reason: str = ""


class EvidenceGate(Protocol):
    def __call__(self, tenant_id: str, owner_id: str, draft: Draft) -> GateResult:
        """Check current task, every source/ACL dependency, and disclosure."""
        ...


class RecipientGate(Protocol):
    def __call__(self, tenant_id: str, target: Target, now: datetime) -> GateResult:
        """Resolve exact IDs, memberships, directory and app reachability."""
        ...


@dataclass(frozen=True)
class Preview:
    preview_id: str
    content_hash: str
    token: str
    expires_at: datetime
    draft: Draft
    # Human meaning is fixed; a click approves exactly this one delivery.
    approval_scope: str = "this_message_once"


@dataclass(frozen=True)
class ApprovalRequest:
    preview_id: str
    token: str
    displayed_hash: str


@dataclass(frozen=True)
class ApprovalResult:
    preview_id: str
    intent_id: str
    status: str  # approved, replay, already_reserved
    intent_state: str
    reason: str = ""


@dataclass(frozen=True)
class BatchResult:
    preview_id: str
    result: ApprovalResult | None = None
    error: str | None = None


@dataclass(frozen=True)
class ReadbackEvidence:
    """Normalized by a trusted adapter from an exact original-AM send record.

    A same-text search result is not sufficient.  ``source_record_id`` points
    to the host's retained platform readback; the request/action mapping must
    have been established by the original sender, not inferred from text.
    """

    request_uuid: str
    external_action_id: str
    message_id: str
    target_kind: str
    target_id: str
    body_sha256: str
    mention_ids: tuple[str, ...]
    correlation: str  # send_response_message_id | existing_am_action
    source_record_id: str
    observed_at: datetime


@dataclass(frozen=True)
class SendOutcome:
    status: str  # confirmed | accepted | failed | unknown
    external_action_id: str = ""
    message_id: str = ""
    readback: ReadbackEvidence | None = None
    reason: str = ""
    definitive_no_send: bool = False


@dataclass(frozen=True)
class DispatchRequest:
    tenant_id: str
    owner_id: str
    intent_id: str
    request_uuid: str
    payload_hash: str
    body_sha256: str
    generation: int
    draft: Draft


class ExistingAMSender(Protocol):
    def dispatch(
        self, request: DispatchRequest, before_send: Callable[[], None]
    ) -> SendOutcome:
        """Map to existing AM action, call before_send, send/read back once.

        The adapter must bind ``request_uuid`` to its existing durable action,
        call ``before_send`` immediately before the provider operation, and
        preserve the original sender's unknown/reconciliation semantics.  It
        must not queue an unguarded background send after returning.
        """
        ...


@dataclass(frozen=True)
class IntentView:
    intent_id: str
    state: str
    reason: str
    request_uuid: str
    payload_hash: str
    external_action_id: str
    message_id: str
    stop_requested: str
    generation: int
    crossed_send_boundary: bool


@dataclass(frozen=True)
class StopResult:
    intent: IntentView
    effect: str  # unsent_stopped | in_flight_not_recalled | historical_only


PURPOSES = frozenset({"request_update", "request_decision", "request_deliverable", "owner_notice"})
TARGET_KINDS = frozenset({"owner", "recipient_p2p", "group"})


def _aware(value: datetime) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise FollowupError("timezone_required")
    return value.astimezone(timezone.utc)


def _stamp(value: datetime) -> float:
    return _aware(value).timestamp()


def _json(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _digest(value: object) -> str:
    return hashlib.sha256(_json(value).encode("utf-8")).hexdigest()


def _string(value: str, field_name: str) -> None:
    if not isinstance(value, str) or not value.strip() or len(value) > 512:
        raise FollowupError("invalid_identifier", field_name)


def _draft_dict(draft: Draft) -> dict:
    result = asdict(draft)
    result["target"]["valid_until"] = (
        _aware(draft.target.valid_until).isoformat() if draft.target.valid_until else None
    )
    result["window"]["not_before"] = _aware(draft.window.not_before).isoformat()
    result["window"]["expires_at"] = _aware(draft.window.expires_at).isoformat()
    return result


def _draft_load(raw: str | dict) -> Draft:
    value = json.loads(raw) if isinstance(raw, str) else raw
    target = dict(value["target"])
    target["attention_ids"] = tuple(target["attention_ids"])
    target["mention_ids"] = tuple(target["mention_ids"])
    target["valid_until"] = datetime.fromisoformat(target["valid_until"]) if target["valid_until"] else None
    evidence = dict(value["evidence"])
    evidence["disclosure_scopes"] = tuple(evidence["disclosure_scopes"])
    window = dict(value["window"])
    window["not_before"] = datetime.fromisoformat(window["not_before"])
    window["expires_at"] = datetime.fromisoformat(window["expires_at"])
    window["working_dates"] = tuple(window["working_dates"])
    return Draft(**{**value, "target": Target(**target), "evidence": EvidenceSnapshot(**evidence), "window": SendWindow(**window)})


def _body_hash(draft: Draft) -> str:
    # Approval also covers dependencies/window; readback covers delivered bytes.
    return hashlib.sha256(draft.body.encode("utf-8")).hexdigest()


def _view(row: sqlite3.Row) -> IntentView:
    return IntentView(
        intent_id=row["intent_id"], state=row["state"], reason=row["reason"],
        request_uuid=row["request_uuid"], payload_hash=row["payload_hash"],
        external_action_id=row["external_action_id"], message_id=row["message_id"],
        stop_requested=row["stop_requested"], generation=row["generation"],
        crossed_send_boundary=bool(row["crossed_send_boundary"]),
    )


class FollowupCoordinator:
    """Persistent one-shot admission shared by all follow-up producers.

    ``owner_ids`` is host configuration, never a callback field.  All existing
    scheduler and personal-assistant producers must use the same registry and
    canonical task/episode keys for cross-producer deduplication to hold.
    """

    def __init__(
        self,
        db_path: str | Path,
        *,
        signing_key: bytes,
        owner_ids: Mapping[str, str],
        evidence_gate: EvidenceGate,
        recipient_gate: RecipientGate,
        clock: Callable[[], datetime] | None = None,
        final_native_check: Callable[[sqlite3.Connection, DispatchRequest], bool] | None = None,
        final_native_guard: Callable[[DispatchRequest], ContextManager[Callable[[sqlite3.Connection], bool]]] | None = None,
    ) -> None:
        if not isinstance(signing_key, bytes) or len(signing_key) < 32:
            raise FollowupError("signing_key_too_short")
        if str(db_path) == ":memory:":
            raise FollowupError("durable_database_required")
        self.db_path = str(db_path)
        self.signing_key = signing_key
        self.owner_ids = dict(owner_ids)
        self.evidence_gate = evidence_gate
        self.recipient_gate = recipient_gate
        self.clock = clock or (lambda: datetime.now(timezone.utc))
        self.final_native_check = final_native_check
        self.final_native_guard = final_native_guard
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        with self._db() as db:
            db.executescript("""
                PRAGMA journal_mode=WAL;
                CREATE TABLE IF NOT EXISTS followup_episodes (
                    tenant_id TEXT NOT NULL, episode_id TEXT NOT NULL,
                    task_id TEXT NOT NULL, purpose TEXT NOT NULL, trigger_key TEXT NOT NULL,
                    PRIMARY KEY (tenant_id,episode_id),
                    UNIQUE (tenant_id,task_id,purpose,trigger_key)
                );
                CREATE TABLE IF NOT EXISTS followup_previews (
                    tenant_id TEXT NOT NULL, preview_id TEXT NOT NULL, owner_id TEXT NOT NULL,
                    draft_json TEXT NOT NULL, content_hash TEXT NOT NULL, token_hash TEXT NOT NULL,
                    expires_at REAL NOT NULL, state TEXT NOT NULL DEFAULT 'prepared',
                    intent_id TEXT NOT NULL DEFAULT '', created_at REAL NOT NULL,
                    PRIMARY KEY (tenant_id,preview_id)
                );
                CREATE TABLE IF NOT EXISTS followup_intents (
                    tenant_id TEXT NOT NULL, intent_id TEXT NOT NULL, owner_id TEXT NOT NULL,
                    preview_id TEXT NOT NULL, draft_json TEXT NOT NULL, payload_hash TEXT NOT NULL,
                    request_uuid TEXT NOT NULL, state TEXT NOT NULL,
                    generation INTEGER NOT NULL DEFAULT 0, claim_token TEXT NOT NULL DEFAULT '',
                    worker_id TEXT NOT NULL DEFAULT '', stop_requested TEXT NOT NULL DEFAULT '',
                    reason TEXT NOT NULL DEFAULT '', crossed_send_boundary INTEGER NOT NULL DEFAULT 0,
                    external_action_id TEXT NOT NULL DEFAULT '', message_id TEXT NOT NULL DEFAULT '',
                    readback_json TEXT NOT NULL DEFAULT '', created_at REAL NOT NULL, updated_at REAL NOT NULL,
                    PRIMARY KEY (tenant_id,intent_id), UNIQUE(request_uuid)
                );
                CREATE TABLE IF NOT EXISTS followup_attention (
                    tenant_id TEXT NOT NULL, semantic_key TEXT NOT NULL, intent_id TEXT NOT NULL,
                    PRIMARY KEY (tenant_id,semantic_key)
                );
                CREATE TABLE IF NOT EXISTS followup_delivery_barriers (
                    tenant_id TEXT NOT NULL, barrier_key TEXT NOT NULL, intent_id TEXT NOT NULL,
                    PRIMARY KEY (tenant_id,barrier_key)
                );
                CREATE TABLE IF NOT EXISTS followup_audit (
                    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                    tenant_id TEXT NOT NULL, object_id TEXT NOT NULL,
                    event TEXT NOT NULL, detail TEXT NOT NULL, recorded_at REAL NOT NULL
                );
            """)

    @contextmanager
    def _db(self, write: bool = False) -> Iterator[sqlite3.Connection]:
        db = sqlite3.connect(self.db_path, isolation_level=None, timeout=10)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA foreign_keys=ON")
        db.execute("PRAGMA busy_timeout=10000")
        try:
            if write:
                db.execute("BEGIN IMMEDIATE")
            yield db
            if write:
                db.execute("COMMIT")
        except BaseException:
            if write and db.in_transaction:
                db.execute("ROLLBACK")
            raise
        finally:
            db.close()

    def _now(self) -> datetime:
        return _aware(self.clock())

    def _actor(self, actor: Actor) -> None:
        _string(actor.tenant_id, "tenant_id")
        _string(actor.user_id, "user_id")
        _string(actor.session_id, "session_id")
        if self.owner_ids.get(actor.tenant_id) != actor.user_id:
            raise FollowupError("owner_required")
        if _stamp(actor.expires_at) <= _stamp(self._now()):
            raise FollowupError("actor_expired")

    def _audit(self, db: sqlite3.Connection, tenant: str, object_id: str, event: str, detail: str = "") -> None:
        db.execute(
            "INSERT INTO followup_audit(tenant_id,object_id,event,detail,recorded_at) VALUES(?,?,?,?,?)",
            (tenant, object_id, event, detail, _stamp(self._now())),
        )

    def register_episode(self, tenant_id: str, task_id: str, purpose: str, trigger_key: str) -> str:
        """Backend ingestion only: derive a stable episode from a verified trigger.

        ``trigger_key`` is a canonical new-request/source-event or approved
        reminder-window key, never an LLM-chosen nonce or a draft version.
        """
        for name, value in (("tenant_id", tenant_id), ("task_id", task_id), ("trigger_key", trigger_key)):
            _string(value, name)
        if tenant_id not in self.owner_ids or purpose not in PURPOSES:
            raise FollowupError("invalid_episode_scope")
        episode_id = "episode_" + _digest([tenant_id, task_id, purpose, trigger_key])[:40]
        with self._db(write=True) as db:
            db.execute(
                "INSERT OR IGNORE INTO followup_episodes VALUES(?,?,?,?,?)",
                (tenant_id, episode_id, task_id, purpose, trigger_key),
            )
        return episode_id

    def _validate_draft(self, actor: Actor, draft: Draft) -> None:
        for name, value in (("task_id", draft.task_id), ("episode_id", draft.episode_id), ("producer_id", draft.producer_id)):
            _string(value, name)
        if draft.purpose not in PURPOSES:
            raise FollowupError("invalid_purpose")
        if draft.action != "send_followup":
            raise FollowupError("unsupported_action")
        if not isinstance(draft.body, str) or not draft.body.strip() or len(draft.body.encode("utf-8")) > 16000:
            raise FollowupError("invalid_body")
        # Mention markup is generated by the host from verified IDs.  The body
        # is plain text; model text cannot inject a new recipient or @all.
        if "<at" in draft.body.lower() or "<mention" in draft.body.lower():
            raise FollowupError("inline_mention_markup_forbidden")
        target = draft.target
        if target.kind not in TARGET_KINDS or target.audience != "internal":
            raise FollowupError("unsupported_target")
        _string(target.target_id, "target_id")
        _string(target.directory_version, "directory_version")
        if not target.attention_ids or len(set(target.attention_ids)) != len(target.attention_ids):
            raise FollowupError("invalid_attention_targets")
        for value in target.attention_ids + target.mention_ids:
            _string(value, "person_id")
            if value.lower() in {"all", "@all", "everyone"}:
                raise FollowupError("broadcast_forbidden")
        if target.kind == "group":
            if tuple(sorted(target.mention_ids)) != tuple(sorted(target.attention_ids)):
                raise FollowupError("group_mentions_must_match_attention")
        elif target.mention_ids or target.attention_ids != (target.target_id,):
            raise FollowupError("invalid_private_target")
        if target.kind == "owner" and target.target_id != actor.user_id:
            raise FollowupError("owner_is_not_engineer_dm")
        if target.valid_until is None or _stamp(target.valid_until) <= _stamp(self._now()):
            raise FollowupError("recipient_snapshot_expired")
        ev = draft.evidence
        if not ev.versions or set(ev.versions) != set(ev.acl_versions) or not ev.disclosure_scopes:
            raise FollowupError("incomplete_evidence_snapshot")
        _string(ev.task_version, "task_version")
        for values in (ev.versions, ev.acl_versions):
            for key, value in values.items():
                _string(key, "dependency_key")
                _string(value, "dependency_version")
        window = draft.window
        if not isinstance(window.allow_outside_hours, bool):
            raise FollowupError("invalid_quiet_hours_override")
        if _stamp(window.not_before) >= _stamp(window.expires_at) or _stamp(window.expires_at) <= _stamp(self._now()):
            raise FollowupError("invalid_send_window")
        try:
            ZoneInfo(window.timezone)
            start = datetime.strptime(window.local_start, "%H:%M").time()
            end = datetime.strptime(window.local_end, "%H:%M").time()
            if start >= end or start.strftime("%H:%M") != window.local_start or end.strftime("%H:%M") != window.local_end:
                raise ValueError("window must not cross midnight")
            if not window.working_dates or len(set(window.working_dates)) != len(window.working_dates):
                raise ValueError("finite working dates required")
            for item in window.working_dates:
                if date.fromisoformat(item).isoformat() != item:
                    raise ValueError("noncanonical date")
        except (ValueError, KeyError) as exc:
            raise FollowupError("invalid_calendar_window") from exc
        _string(window.calendar_version, "calendar_version")
        with self._db() as db:
            episode = db.execute(
                "SELECT * FROM followup_episodes WHERE tenant_id=? AND episode_id=?",
                (actor.tenant_id, draft.episode_id),
            ).fetchone()
        if not episode or episode["task_id"] != draft.task_id or episode["purpose"] != draft.purpose:
            raise FollowupError("unknown_or_mismatched_episode")

    def _gates(self, tenant_id: str, owner_id: str, draft: Draft) -> None:
        now = self._now()
        if self.owner_ids.get(tenant_id) != owner_id:
            raise DispatchBlocked("owner_configuration_changed")
        if draft.target.valid_until is None or _stamp(draft.target.valid_until) <= _stamp(now):
            raise DispatchBlocked("recipient_snapshot_expired")
        for result, label in (
            (self.evidence_gate(tenant_id, owner_id, draft), "evidence"),
            (self.recipient_gate(tenant_id, draft.target, now), "recipient"),
        ):
            if not isinstance(result, GateResult) or result.allowed is not True:
                reason = result.reason if isinstance(result, GateResult) else "invalid_gate_response"
                raise DispatchBlocked(f"{label}_blocked", reason)

    def create_preview(self, actor: Actor, draft: Draft, *, preview_ttl_seconds: int = 600) -> Preview:
        self._actor(actor)
        if not 1 <= preview_ttl_seconds <= 1800:
            raise FollowupError("invalid_preview_ttl")
        # Snapshot mutable mappings before hashing or passing to collaborators.
        draft = _draft_load(_json(_draft_dict(draft)))
        self._validate_draft(actor, draft)
        self._gates(actor.tenant_id, actor.user_id, draft)
        now = self._now()
        expiry = min(_stamp(now) + preview_ttl_seconds, _stamp(draft.window.expires_at))
        preview_id = "preview_" + uuid.uuid4().hex
        body = {"tenant_id": actor.tenant_id, "owner_id": actor.user_id, "draft": _draft_dict(draft)}
        content_hash = _digest(body)
        nonce = secrets.token_urlsafe(24)
        signed = _json([actor.tenant_id, actor.user_id, preview_id, content_hash, expiry, nonce])
        token = nonce + "." + hmac.new(self.signing_key, signed.encode(), hashlib.sha256).hexdigest()
        with self._db(write=True) as db:
            db.execute(
                "INSERT INTO followup_previews(tenant_id,preview_id,owner_id,draft_json,content_hash,token_hash,expires_at,created_at) VALUES(?,?,?,?,?,?,?,?)",
                (actor.tenant_id, preview_id, actor.user_id, _json(_draft_dict(draft)), content_hash,
                 hashlib.sha256(token.encode()).hexdigest(), expiry, _stamp(now)),
            )
            self._audit(db, actor.tenant_id, preview_id, "preview_created", content_hash)
        return Preview(preview_id, content_hash, token, datetime.fromtimestamp(expiry, timezone.utc), draft)

    def _check_token(self, actor: Actor, row: sqlite3.Row, token: str, displayed_hash: str) -> None:
        if row["owner_id"] != actor.user_id:
            raise FollowupError("preview_owner_mismatch")
        if not isinstance(displayed_hash, str) or not hmac.compare_digest(row["content_hash"], displayed_hash):
            raise FollowupError("displayed_preview_changed")
        if not isinstance(token, str) or len(token) > 256 or "." not in token:
            raise FollowupError("invalid_approval_token")
        nonce, signature = token.rsplit(".", 1)
        signed = _json([actor.tenant_id, actor.user_id, row["preview_id"], row["content_hash"], row["expires_at"], nonce])
        expected = hmac.new(self.signing_key, signed.encode(), hashlib.sha256).hexdigest()
        if not hmac.compare_digest(signature, expected) or not hmac.compare_digest(
            row["token_hash"], hashlib.sha256(token.encode()).hexdigest()
        ):
            raise FollowupError("invalid_approval_token")
        # Detect unexpected stored payload corruption before any authorization.
        expected_hash = _digest({"tenant_id": actor.tenant_id, "owner_id": actor.user_id, "draft": json.loads(row["draft_json"])})
        if not hmac.compare_digest(expected_hash, row["content_hash"]):
            raise FollowupError("stored_preview_changed")

    @staticmethod
    def _attention_keys(tenant_id: str, draft: Draft) -> tuple[str, ...]:
        return tuple(sorted(_digest([tenant_id, draft.task_id, draft.purpose, draft.episode_id, person]) for person in draft.target.attention_ids))

    @staticmethod
    def _barrier_keys(tenant_id: str, draft: Draft) -> tuple[str, ...]:
        # A new reminder window must not bypass an unresolved older send.  This
        # key deliberately also excludes episode_id, unlike long-term dedup.
        return tuple(sorted(_digest([tenant_id, draft.task_id, draft.purpose, person]) for person in draft.target.attention_ids))

    def _other_barriers(self, db: sqlite3.Connection, tenant_id: str, draft: Draft, intent_id: str = "") -> set[str]:
        return {
            row["intent_id"] for key in self._barrier_keys(tenant_id, draft)
            if (row := db.execute(
                "SELECT intent_id FROM followup_delivery_barriers WHERE tenant_id=? AND barrier_key=?",
                (tenant_id, key),
            ).fetchone()) and row["intent_id"] != intent_id
        }

    @staticmethod
    def _release_barriers(db: sqlite3.Connection, tenant_id: str, intent_id: str) -> None:
        db.execute("DELETE FROM followup_delivery_barriers WHERE tenant_id=? AND intent_id=?", (tenant_id, intent_id))

    def approve(self, actor: Actor, preview_id: str, token: str, displayed_hash: str) -> ApprovalResult:
        self._actor(actor)
        with self._db() as db:
            first = db.execute(
                "SELECT * FROM followup_previews WHERE tenant_id=? AND preview_id=?", (actor.tenant_id, preview_id)
            ).fetchone()
        if not first:
            raise FollowupError("preview_not_found")
        self._check_token(actor, first, token, displayed_hash)
        # Replays after a successful approval return the original outcome, even
        # if the preview TTL passed.  They never renew the send authorization.
        if first["state"] == "prepared":
            if first["expires_at"] <= _stamp(self._now()):
                raise FollowupError("preview_expired")
            draft = _draft_load(first["draft_json"])
            self._gates(actor.tenant_id, actor.user_id, draft)
        self._actor(actor)
        with self._db(write=True) as db:
            row = db.execute(
                "SELECT * FROM followup_previews WHERE tenant_id=? AND preview_id=?", (actor.tenant_id, preview_id)
            ).fetchone()
            self._check_token(actor, row, token, displayed_hash)
            if row["state"] in {"approved", "already_reserved"}:
                intent = self._intent_row(db, actor.tenant_id, row["intent_id"])
                return ApprovalResult(preview_id, intent["intent_id"], "replay", intent["state"], "original_authorization_not_renewed")
            if row["expires_at"] <= _stamp(self._now()):
                raise FollowupError("preview_expired")
            draft = _draft_load(row["draft_json"])
            keys = self._attention_keys(actor.tenant_id, draft)
            existing = {
                hit["intent_id"] for key in keys
                if (hit := db.execute(
                    "SELECT intent_id FROM followup_attention WHERE tenant_id=? AND semantic_key=?", (actor.tenant_id, key)
                ).fetchone())
            }
            if self._other_barriers(db, actor.tenant_id, draft) - existing:
                raise FollowupError("prior_delivery_uncertain")
            if existing and draft.supersedes_intent_id:
                if existing != {draft.supersedes_intent_id}:
                    raise FollowupError("supersede_attention_conflict")
                previous = self._intent_row(db, actor.tenant_id, draft.supersedes_intent_id)
                if previous["owner_id"] != actor.user_id or previous["crossed_send_boundary"] or previous["state"] not in {"queued", "blocked", "paused", "revoked", "failed"}:
                    raise FollowupError("cannot_replace_executed_or_claimed_intent")
                # An exact replacement cannot drop reservations that belonged to
                # another audience; require the same attention set for R1.
                previous_keys = {hit[0] for hit in db.execute(
                    "SELECT semantic_key FROM followup_attention WHERE tenant_id=? AND intent_id=?",
                    (actor.tenant_id, previous["intent_id"]),
                )}
                if previous_keys != set(keys):
                    raise FollowupError("supersede_audience_changed")
                db.execute(
                    "UPDATE followup_intents SET state='superseded',stop_requested='revoke',generation=generation+1,reason='replaced_before_send',updated_at=? WHERE tenant_id=? AND intent_id=?",
                    (_stamp(self._now()), actor.tenant_id, previous["intent_id"]),
                )
                db.execute("DELETE FROM followup_attention WHERE tenant_id=? AND intent_id=?", (actor.tenant_id, previous["intent_id"]))
                self._audit(db, actor.tenant_id, previous["intent_id"], "superseded", preview_id)
                existing = set()
            elif draft.supersedes_intent_id and not existing:
                raise FollowupError("supersede_target_missing")
            if existing:
                # No partial group send: any person's reservation blocks this
                # entire message, preserving the exact reviewed recipient set.
                if len(existing) > 1:
                    raise FollowupError("attention_reserved_by_multiple_intents")
                existing_id = next(iter(existing))
                intent = self._intent_row(db, actor.tenant_id, existing_id)
                db.execute(
                    "UPDATE followup_previews SET state='already_reserved',intent_id=? WHERE tenant_id=? AND preview_id=?",
                    (existing_id, actor.tenant_id, preview_id),
                )
                self._audit(db, actor.tenant_id, preview_id, "duplicate_attention_blocked", existing_id)
                return ApprovalResult(preview_id, existing_id, "already_reserved", intent["state"], "new_preview_not_sent_or_substituted")
            intent_id = "intent_" + uuid.uuid4().hex
            request_uuid = str(uuid.uuid4())
            now = _stamp(self._now())
            db.execute(
                "INSERT INTO followup_intents(tenant_id,intent_id,owner_id,preview_id,draft_json,payload_hash,request_uuid,state,created_at,updated_at) VALUES(?,?,?,?,?,?,?,'queued',?,?)",
                (actor.tenant_id, intent_id, actor.user_id, preview_id, row["draft_json"], row["content_hash"], request_uuid, now, now),
            )
            for key in keys:
                db.execute("INSERT INTO followup_attention VALUES(?,?,?)", (actor.tenant_id, key, intent_id))
            db.execute("UPDATE followup_previews SET state='approved',intent_id=? WHERE tenant_id=? AND preview_id=?", (intent_id, actor.tenant_id, preview_id))
            self._audit(db, actor.tenant_id, intent_id, "approved_once", preview_id)
            return ApprovalResult(preview_id, intent_id, "approved", "queued")

    def approve_batch(self, actor: Actor, requests: Sequence[ApprovalRequest]) -> list[BatchResult]:
        self._actor(actor)
        if len(requests) > 50:
            raise FollowupError("batch_too_large")
        results: list[BatchResult] = []
        for item in requests:
            try:
                result = self.approve(actor, item.preview_id, item.token, item.displayed_hash)
                results.append(BatchResult(item.preview_id, result=result))
            except FollowupError as exc:
                results.append(BatchResult(item.preview_id, error=exc.code))
        return results

    def _intent_row(self, db: sqlite3.Connection, tenant_id: str, intent_id: str) -> sqlite3.Row:
        row = db.execute("SELECT * FROM followup_intents WHERE tenant_id=? AND intent_id=?", (tenant_id, intent_id)).fetchone()
        if not row:
            raise FollowupError("intent_not_found")
        return row

    def get_intent(self, actor: Actor, intent_id: str) -> IntentView:
        self._actor(actor)
        with self._db() as db:
            row = self._intent_row(db, actor.tenant_id, intent_id)
            if row["owner_id"] != actor.user_id:
                raise FollowupError("intent_owner_mismatch")
            return _view(row)

    def audit(self, actor: Actor, object_id: str) -> list[dict]:
        self._actor(actor)
        with self._db() as db:
            return [dict(row) for row in db.execute(
                "SELECT sequence,event,detail,recorded_at FROM followup_audit WHERE tenant_id=? AND object_id=? ORDER BY sequence",
                (actor.tenant_id, object_id),
            )]

    def _window_reason(self, draft: Draft) -> str:
        now = self._now()
        if _stamp(now) < _stamp(draft.window.not_before):
            return "not_before"
        if _stamp(now) >= _stamp(draft.window.expires_at):
            return "authorization_expired"
        local = now.astimezone(ZoneInfo(draft.window.timezone))
        if local.date().isoformat() not in draft.window.working_dates:
            return "not_approved_working_date"
        if not draft.window.allow_outside_hours:
            label = local.strftime("%H:%M")
            if not draft.window.local_start <= label < draft.window.local_end:
                return "quiet_hours"
        return ""

    @contextmanager
    def _final_admission_scope(self, request: DispatchRequest):
        """Classify failures of reads/local checks before the SDK is invoked.

        A directory outage or a rolled-back local check proves no SDK call was
        made by this guard. _finish_blocked subsequently reads the durable
        crossed_send_boundary flag under a fresh write transaction: an already
        committed sending transition still remains unknown, including failures
        while exiting the outer evidence fence. If that read itself fails, no
        definitive no-send classification is made. Process-level BaseExceptions
        deliberately propagate to the existing crash recovery path.
        """
        try:
            scope = self.final_native_guard(request) if self.final_native_guard else nullcontext(None)
            with scope as dependency_check:
                yield dependency_check
        except DispatchBlocked:
            raise
        except Exception as exc:
            raise DispatchBlocked("final_admission_unavailable", type(exc).__name__) from None

    def dispatch_once(self, tenant_id: str, intent_id: str, sender: ExistingAMSender, *, worker_id: str) -> IntentView:
        """Called by the original consumer; it does not start a worker loop."""
        if tenant_id not in self.owner_ids:
            raise FollowupError("unknown_tenant")
        _string(worker_id, "worker_id")
        claim = secrets.token_hex(24)
        with self._db(write=True) as db:
            row = self._intent_row(db, tenant_id, intent_id)
            if row["state"] != "queued" or row["stop_requested"]:
                return _view(row)
            draft = _draft_load(row["draft_json"])
            expected_hash = _digest({"tenant_id": tenant_id, "owner_id": row["owner_id"], "draft": json.loads(row["draft_json"])})
            if not hmac.compare_digest(expected_hash, row["payload_hash"]):
                db.execute("UPDATE followup_intents SET state='blocked',reason='stored_intent_changed',updated_at=? WHERE tenant_id=? AND intent_id=?", (_stamp(self._now()), tenant_id, intent_id))
                return _view(self._intent_row(db, tenant_id, intent_id))
            window_reason = self._window_reason(draft)
            if window_reason:
                state = "blocked" if window_reason == "authorization_expired" else "queued"
                db.execute("UPDATE followup_intents SET state=?,reason=?,updated_at=? WHERE tenant_id=? AND intent_id=?", (state, window_reason, _stamp(self._now()), tenant_id, intent_id))
                return _view(self._intent_row(db, tenant_id, intent_id))
            if self._other_barriers(db, tenant_id, draft, intent_id):
                db.execute("UPDATE followup_intents SET reason='prior_delivery_uncertain',updated_at=? WHERE tenant_id=? AND intent_id=?", (_stamp(self._now()), tenant_id, intent_id))
                return _view(self._intent_row(db, tenant_id, intent_id))
            for key in self._barrier_keys(tenant_id, draft):
                db.execute("INSERT OR IGNORE INTO followup_delivery_barriers VALUES(?,?,?)", (tenant_id, key, intent_id))
            generation = row["generation"] + 1
            db.execute(
                "UPDATE followup_intents SET state='claimed',generation=?,claim_token=?,worker_id=?,reason='',updated_at=? WHERE tenant_id=? AND intent_id=?",
                (generation, claim, worker_id, _stamp(self._now()), tenant_id, intent_id),
            )
            request = DispatchRequest(tenant_id, row["owner_id"], intent_id, row["request_uuid"], row["payload_hash"], _body_hash(draft), generation, draft)
            self._audit(db, tenant_id, intent_id, "claimed", worker_id)
        guarded = False

        def before_send() -> None:
            nonlocal guarded
            if guarded:
                raise DispatchBlocked("send_guard_already_used")
            self._gates(tenant_id, request.owner_id, request.draft)
            reason = self._window_reason(request.draft)
            if reason:
                raise DispatchBlocked(reason)
            # A host with another evidence database fences that database BEFORE
            # acquiring this AM write transaction. The yielded check performs
            # local reads only. Both fences remain held through the committed
            # sending transition, and are released before the SDK is called.
            with self._final_admission_scope(request) as dependency_check:
                with self._db(write=True) as db:
                    current = self._intent_row(db, tenant_id, intent_id)
                    if current["state"] != "claimed" or current["generation"] != generation or current["claim_token"] != claim or current["stop_requested"]:
                        raise DispatchBlocked("claim_revoked_or_fenced")
                    # The authorization's exact payload must still be the one
                    # the original AM adapter is about to send.
                    if current["payload_hash"] != request.payload_hash or current["draft_json"] != _json(_draft_dict(request.draft)):
                        raise DispatchBlocked("stored_intent_changed")
                    if dependency_check is not None and dependency_check(db) is not True:
                        raise DispatchBlocked("local_dependencies_changed")
                    if self.final_native_check is not None and self.final_native_check(db, request) is not True:
                        raise DispatchBlocked("native_admission_changed")
                    # Lock contention must not extend the exact approved window.
                    reason = self._window_reason(request.draft)
                    if reason:
                        raise DispatchBlocked(reason)
                    db.execute(
                        "UPDATE followup_intents SET state='sending',crossed_send_boundary=1,updated_at=? WHERE tenant_id=? AND intent_id=?",
                        (_stamp(self._now()), tenant_id, intent_id),
                    )
                    self._audit(db, tenant_id, intent_id, "send_boundary_entered", request.request_uuid)
            guarded = True

        try:
            # An early check reduces pointless admission work.  The adapter
            # still must call before_send immediately before its provider call.
            self._gates(tenant_id, request.owner_id, request.draft)
            outcome = sender.dispatch(request, before_send)
            if not isinstance(outcome, SendOutcome):
                outcome = SendOutcome("unknown", reason="invalid_adapter_outcome")
            elif any(not isinstance(value, str) for value in (outcome.status, outcome.external_action_id, outcome.message_id, outcome.reason)):
                outcome = SendOutcome("unknown", reason="invalid_adapter_outcome_fields")
            if not guarded and outcome.status != "failed":
                outcome = SendOutcome("unknown", external_action_id=outcome.external_action_id, reason="adapter_missing_final_gate")
        except DispatchBlocked as exc:
            return self._finish_blocked(tenant_id, intent_id, generation, claim, exc.code)
        except Exception as exc:
            # Error strings can contain provider tokens/response bodies; retain
            # the exception class only.  Exact host diagnostics stay upstream.
            outcome = SendOutcome("unknown", reason="adapter_exception:" + type(exc).__name__)
        return self._finish(tenant_id, intent_id, generation, claim, request, outcome, guarded)

    def _finish_blocked(self, tenant: str, intent_id: str, generation: int, claim: str, reason: str) -> IntentView:
        with self._db(write=True) as db:
            row = self._intent_row(db, tenant, intent_id)
            if row["generation"] == generation and row["claim_token"] == claim and row["state"] in {"claimed", "sending"}:
                # If the adapter raised this after the send boundary, delivery
                # is uncertain.  A local exception cannot prove no remote send.
                state = "unknown" if row["crossed_send_boundary"] else "blocked"
                if state == "blocked":
                    self._release_barriers(db, tenant, intent_id)
                db.execute("UPDATE followup_intents SET state=?,reason=?,updated_at=? WHERE tenant_id=? AND intent_id=?", (state, reason, _stamp(self._now()), tenant, intent_id))
                self._audit(db, tenant, intent_id, state, reason)
            return _view(self._intent_row(db, tenant, intent_id))

    def _valid_readback(self, request: DispatchRequest, readback: ReadbackEvidence | None, action_id: str, message_id: str = "") -> bool:
        if not isinstance(readback, ReadbackEvidence):
            return False
        if not isinstance(readback.mention_ids, (tuple, list)) or any(not isinstance(item, str) for item in readback.mention_ids):
            return False
        try:
            not_future = _stamp(readback.observed_at) <= _stamp(self._now())
        except (ValueError, TypeError, OverflowError):
            return False
        return bool(
            readback.request_uuid == request.request_uuid
            and readback.external_action_id and readback.external_action_id == action_id
            and readback.message_id and (not message_id or readback.message_id == message_id)
            and readback.target_kind == request.draft.target.kind
            and readback.target_id == request.draft.target.target_id
            and readback.body_sha256 == request.body_sha256
            and tuple(sorted(readback.mention_ids)) == tuple(sorted(request.draft.target.mention_ids))
            and readback.correlation in {"send_response_message_id", "existing_am_action"}
            and readback.source_record_id
            and not_future
        )

    def _finish(self, tenant: str, intent_id: str, generation: int, claim: str, request: DispatchRequest, outcome: SendOutcome, guarded: bool) -> IntentView:
        state, reason, readback_json = "unknown", outcome.reason, ""
        if outcome.status == "confirmed" and guarded:
            if self._valid_readback(request, outcome.readback, outcome.external_action_id, outcome.message_id):
                state = "confirmed"
                data = asdict(outcome.readback)
                data["observed_at"] = _aware(outcome.readback.observed_at).isoformat()
                readback_json = _json(data)
            else:
                reason = "readback_mismatch_or_missing"
        elif outcome.status == "failed" and outcome.definitive_no_send is True:
            state = "failed"
        elif outcome.status == "accepted":
            reason = outcome.reason or "awaiting_exact_readback"
        elif outcome.status not in {"unknown", "failed", "confirmed"}:
            reason = "invalid_adapter_status"
        with self._db(write=True) as db:
            row = self._intent_row(db, tenant, intent_id)
            if row["generation"] != generation or row["claim_token"] != claim or row["state"] not in {"claimed", "sending"}:
                return _view(row)
            db.execute(
                "UPDATE followup_intents SET state=?,reason=?,external_action_id=?,message_id=?,readback_json=?,updated_at=? WHERE tenant_id=? AND intent_id=?",
                (state, reason, outcome.external_action_id, outcome.message_id or (outcome.readback.message_id if state == "confirmed" and outcome.readback else ""), readback_json, _stamp(self._now()), tenant, intent_id),
            )
            if state in {"confirmed", "failed"}:
                self._release_barriers(db, tenant, intent_id)
            self._audit(db, tenant, intent_id, state, reason)
            return _view(self._intent_row(db, tenant, intent_id))

    def _stop(self, actor: Actor, intent_id: str, action: str) -> StopResult:
        self._actor(actor)
        with self._db(write=True) as db:
            row = self._intent_row(db, actor.tenant_id, intent_id)
            if row["owner_id"] != actor.user_id:
                raise FollowupError("intent_owner_mismatch")
            if row["state"] in {"queued", "claimed", "blocked", "paused", "revoked"} and not row["crossed_send_boundary"]:
                state = "revoked" if action == "revoke" or row["state"] == "revoked" else "paused"
                db.execute(
                    "UPDATE followup_intents SET state=?,stop_requested=?,generation=generation+1,reason='owner_stopped_before_send',updated_at=? WHERE tenant_id=? AND intent_id=?",
                    (state, action, _stamp(self._now()), actor.tenant_id, intent_id),
                )
                self._release_barriers(db, actor.tenant_id, intent_id)
                effect = "unsent_stopped"
            else:
                # Do not rewrite an actual delivery as "revoked" or "unsent".
                db.execute("UPDATE followup_intents SET stop_requested=?,updated_at=? WHERE tenant_id=? AND intent_id=?", (action, _stamp(self._now()), actor.tenant_id, intent_id))
                effect = "in_flight_not_recalled" if row["state"] in {"sending", "unknown"} else "historical_only"
            self._audit(db, actor.tenant_id, intent_id, action, effect)
            return StopResult(_view(self._intent_row(db, actor.tenant_id, intent_id)), effect)

    def pause(self, actor: Actor, intent_id: str) -> StopResult:
        return self._stop(actor, intent_id, "pause")

    def revoke(self, actor: Actor, intent_id: str) -> StopResult:
        return self._stop(actor, intent_id, "revoke")

    def recover_in_flight(self, tenant_id: str, worker_id: str) -> int:
        """Fence the old worker and mark ambiguous work unknown, never requeue.

        This may run after a restart or a handover.  It does not infer that an
        external send was absent.  A late old callback cannot reclaim/send a
        still-claimed intent, and a late completion cannot overwrite recovery.
        """
        if tenant_id not in self.owner_ids:
            raise FollowupError("unknown_tenant")
        with self._db(write=True) as db:
            rows = list(db.execute("SELECT * FROM followup_intents WHERE tenant_id=? AND worker_id=? AND state IN ('claimed','sending')", (tenant_id, worker_id)))
            for row in rows:
                db.execute(
                    "UPDATE followup_intents SET state='unknown',generation=generation+1,claim_token='',reason='worker_recovered_requires_reconciliation',updated_at=? WHERE tenant_id=? AND intent_id=?",
                    (_stamp(self._now()), tenant_id, row["intent_id"]),
                )
                self._audit(db, tenant_id, row["intent_id"], "unknown", "worker_recovery")
            return len(rows)

    def reconcile(self, tenant_id: str, intent_id: str, readback: ReadbackEvidence) -> IntentView:
        """Trusted original-AM reconciliation callback; no retry/no text search."""
        if tenant_id not in self.owner_ids:
            raise FollowupError("unknown_tenant")
        if not isinstance(readback, ReadbackEvidence):
            raise FollowupError("readback_mismatch")
        with self._db(write=True) as db:
            row = self._intent_row(db, tenant_id, intent_id)
            if row["state"] not in {"unknown", "confirmed"}:
                raise FollowupError("reconcile_requires_unknown")
            draft = _draft_load(row["draft_json"])
            request = DispatchRequest(tenant_id, row["owner_id"], intent_id, row["request_uuid"], row["payload_hash"], _body_hash(draft), row["generation"], draft)
            action_id = row["external_action_id"] or readback.external_action_id
            if not self._valid_readback(request, readback, action_id, row["message_id"]):
                raise FollowupError("readback_mismatch")
            if row["state"] == "confirmed":
                return _view(row)
            data = asdict(readback)
            data["observed_at"] = _aware(readback.observed_at).isoformat()
            db.execute(
                "UPDATE followup_intents SET state='confirmed',reason='reconciled_exact_message',external_action_id=?,message_id=?,readback_json=?,updated_at=? WHERE tenant_id=? AND intent_id=?",
                (readback.external_action_id, readback.message_id, _json(data), _stamp(self._now()), tenant_id, intent_id),
            )
            self._release_barriers(db, tenant_id, intent_id)
            self._audit(db, tenant_id, intent_id, "confirmed", "reconciled_exact_message")
            return _view(self._intent_row(db, tenant_id, intent_id))

