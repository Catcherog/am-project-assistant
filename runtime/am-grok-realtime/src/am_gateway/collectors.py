"""Durable read-only collection. Adapters have no permission to send messages.

Each claim reads one page outside SQLite; page admission and its cursor commit in
one transaction. Event callbacks must use Store.ingest, never this poll state.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass, field, replace
import json
import math
import subprocess
import uuid
from typing import Callable, Mapping, Protocol
from urllib.request import Request, urlopen

from .models import Config, ContractError, Event, dumps, key
from .normalization import normalized_content


class CursorExpired(RuntimeError):
    """An adapter must raise only for a documented expired/invalid page cursor."""


class SourceUnreadable(RuntimeError):
    pass


class PageProtocolError(RuntimeError):
    pass


class FeishuAPIError(RuntimeError):
    pass


@dataclass(frozen=True)
class SourceSpec:
    tenant_id: str
    native_id: str
    source_kind: str = "chat"
    adapter: str = "feishu_messages"
    channel: str = "group"
    chat_id: str = ""
    container_type: str = "chat"
    identity: str = "bot"
    project_ids: tuple[str, ...] = ()
    poll_seconds: float = 60
    overlap_seconds: float = 120
    metadata: dict = field(default_factory=dict)

    @property
    def source_id(self) -> str:
        return key(self.tenant_id, self.source_kind, self.native_id, self.identity)

    @property
    def effective_chat_id(self) -> str:
        return self.chat_id or (self.native_id if self.container_type == "chat" else "")

    def validate(self) -> None:
        if not all(isinstance(v, str) and v for v in
                   (self.tenant_id, self.native_id, self.source_kind, self.adapter, self.identity)):
            raise ContractError("source identity fields required")
        if self.container_type not in {"chat", "thread", "resource"}:
            raise ContractError("unsupported container_type")
        if self.channel not in {"group", "bot_dm", "personal_dm", "resource"}:
            raise ContractError("unsupported source channel")
        if self.channel != "resource" and not self.effective_chat_id:
            raise ContractError("message source requires trusted chat_id")
        for value in (self.poll_seconds, self.overlap_seconds):
            if isinstance(value, bool) or not isinstance(value, (float, int)) or not math.isfinite(value) or value < 0:
                raise ContractError("invalid poll/overlap interval")
        dumps(asdict(self))

    @classmethod
    def from_dict(cls, raw: dict) -> SourceSpec:
        data = dict(raw)
        data["project_ids"] = tuple(data.get("project_ids", ()))
        value = cls(**data)
        value.validate()
        return value


@dataclass(frozen=True)
class ScanRequest:
    lane: str
    window_start: float
    window_end: float
    page_token: str = ""


@dataclass(frozen=True)
class Page:
    events: tuple[Event, ...] = ()
    has_more: bool = False
    next_page_token: str = ""
    discovered_sources: tuple[SourceSpec, ...] = ()


class PageAdapter(Protocol):
    def read_page(self, source: SourceSpec, request: ScanRequest) -> Page: ...


_SCHEMA = (
    """CREATE TABLE IF NOT EXISTS gw_sources(
        source_id TEXT PRIMARY KEY, tenant_id TEXT NOT NULL, spec_json TEXT NOT NULL,
        active INTEGER NOT NULL DEFAULT 1, discovered_by TEXT NOT NULL DEFAULT '',
        observed_at REAL NOT NULL)""",
    """CREATE TABLE IF NOT EXISTS gw_source_scans(
        source_id TEXT NOT NULL, lane TEXT NOT NULL CHECK(lane IN ('live','history')),
        state TEXT NOT NULL DEFAULT 'idle', window_start REAL, window_end REAL,
        page_token TEXT NOT NULL DEFAULT '', completed_until REAL,
        claim_token TEXT NOT NULL DEFAULT '', lease_until REAL NOT NULL DEFAULT 0,
        retry_at REAL NOT NULL DEFAULT 0, last_attempt REAL NOT NULL DEFAULT 0,
        page_count INTEGER NOT NULL DEFAULT 0, item_count INTEGER NOT NULL DEFAULT 0,
        restart_count INTEGER NOT NULL DEFAULT 0, last_error TEXT NOT NULL DEFAULT '',
        PRIMARY KEY(source_id,lane))""",
)


class CollectorEngine:
    def __init__(self, store, config: Config, adapters: Mapping[str, PageAdapter] | None = None,
                 *, lease_seconds: float = 90, retry_seconds: float = 30):
        self.store, self.config = store, config
        self.adapters = dict(adapters or {})
        self.lease_seconds, self.retry_seconds = lease_seconds, retry_seconds
        with store.transaction() as db:
            for statement in _SCHEMA:
                db.execute(statement)

    def register_source(self, source: SourceSpec, *, discovered_by: str = "") -> str:
        with self.store.transaction() as db:
            return self.register_in_tx(db, source, discovered_by=discovered_by)

    def register_in_tx(self, db, source: SourceSpec, *, discovered_by: str = "") -> str:
        source.validate()
        if source.tenant_id != self.config.tenant_id:
            raise ContractError("source tenant mismatch")
        old = db.execute("SELECT spec_json FROM gw_sources WHERE source_id=?", (source.source_id,)).fetchone()
        if old:
            previous = SourceSpec.from_dict(json.loads(old["spec_json"]))
            if (previous.channel, previous.effective_chat_id, previous.container_type) != (
                    source.channel, source.effective_chat_id, source.container_type):
                raise ContractError("source scope change requires a new explicit binding")
            if discovered_by:
                # A periodic inventory refresh must not erase host-approved project
                # bindings or replace the chosen transport/polling configuration.
                source = replace(source, project_ids=previous.project_ids, adapter=previous.adapter,
                                 poll_seconds=previous.poll_seconds, overlap_seconds=previous.overlap_seconds,
                                 metadata={**previous.metadata, **source.metadata})
        db.execute("""INSERT INTO gw_sources(source_id,tenant_id,spec_json,discovered_by,observed_at)
            VALUES(?,?,?,?,?) ON CONFLICT(source_id) DO UPDATE SET
            spec_json=excluded.spec_json,active=1,observed_at=excluded.observed_at""",
            (source.source_id, source.tenant_id, dumps(asdict(source)), discovered_by, self.store.clock()))
        for lane in ("live", "history"):
            db.execute("INSERT OR IGNORE INTO gw_source_scans(source_id,lane) VALUES(?,?)",
                       (source.source_id, lane))
        return source.source_id

    def _claim(self, source_id: str | None, lane: str | None):
        now = self.store.clock()
        with self.store.transaction() as db:
            sql = """SELECT s.*,g.spec_json FROM gw_source_scans s
                JOIN gw_sources g USING(source_id) WHERE g.active=1 AND g.tenant_id=?
                AND s.state!='complete' AND s.lease_until<=? AND s.retry_at<=?"""
            args: list = [self.config.tenant_id, now, now]
            if source_id is not None:
                sql += " AND s.source_id=?"; args.append(source_id)
            if lane is not None:
                sql += " AND s.lane=?"; args.append(lane)
            sql += " ORDER BY s.last_attempt,s.source_id,CASE s.lane WHEN 'live' THEN 0 ELSE 1 END LIMIT 1"
            row = db.execute(sql, args).fetchone()
            if row is None:
                return None
            source = SourceSpec.from_dict(json.loads(row["spec_json"]))
            token = uuid.uuid4().hex
            # A monotonic tie-break prevents starvation under a frozen/coarse clock.
            latest = db.execute("SELECT COALESCE(MAX(last_attempt),0) FROM gw_source_scans").fetchone()[0]
            attempted_at = max(now, latest + 0.000001)
            start, end = row["window_start"], row["window_end"]
            if start is None:
                if row["lane"] == "history":
                    start, end = 0.0, self.config.live_cutover_at
                else:
                    start = max(self.config.live_cutover_at,
                                (row["completed_until"] or self.config.live_cutover_at) - source.overlap_seconds)
                    end = max(start, now)
            db.execute("""UPDATE gw_source_scans SET state='scanning',window_start=?,window_end=?,
                claim_token=?,lease_until=?,last_attempt=? WHERE source_id=? AND lane=?""",
                (start, end, token, now + self.lease_seconds, attempted_at, row["source_id"], row["lane"]))
            return source, ScanRequest(row["lane"], start, end, row["page_token"]), token

    def collect_one(self, source_id: str | None = None, lane: str | None = None) -> dict | None:
        claimed = self._claim(source_id, lane)
        if claimed is None:
            return None
        source, request, token = claimed
        result = {"source_id": source.source_id, "lane": request.lane}
        adapter = self.adapters.get(source.adapter)
        if adapter is None:
            self._fail(source, request, token, "unbound", "adapter not bound")
            return {**result, "status": "unbound"}
        try:
            page = adapter.read_page(source, request)
            self._validate_page(source, request, page)
            with self.store.transaction() as db:
                current = db.execute("SELECT claim_token FROM gw_source_scans WHERE source_id=? AND lane=?",
                                     (source.source_id, request.lane)).fetchone()
                if current is None or current["claim_token"] != token:
                    return {**result, "status": "superseded"}
                for event in page.events:
                    self.store.ingest_in_tx(db, replace(event, observed_via="poll:" + request.lane))
                for discovered in page.discovered_sources:
                    self.register_in_tx(db, discovered, discovered_by=source.source_id)
                if page.has_more:
                    db.execute("""UPDATE gw_source_scans SET page_token=?,claim_token='',lease_until=0,
                        state='scanning',last_error='',page_count=page_count+1,item_count=item_count+?
                        WHERE source_id=? AND lane=?""",
                        (page.next_page_token, len(page.events), source.source_id, request.lane))
                else:
                    db.execute("""UPDATE gw_source_scans SET page_token='',claim_token='',lease_until=0,
                        state=?,completed_until=?,window_start=NULL,window_end=NULL,retry_at=?,last_error='',
                        page_count=page_count+1,item_count=item_count+? WHERE source_id=? AND lane=?""",
                        ("complete" if request.lane == "history" else "idle", request.window_end,
                         self.store.clock() + source.poll_seconds, len(page.events), source.source_id, request.lane))
            return {**result, "status": "page" if page.has_more else "completed", "items": len(page.events)}
        except CursorExpired:
            self._fail(source, request, token, "scanning", "cursor expired; same window restarted", reset_cursor=True)
            return {**result, "status": "cursor_restarted"}
        except SourceUnreadable:
            self._fail(source, request, token, "unreadable", "source access denied or unavailable")
            return {**result, "status": "unreadable"}
        except Exception as exc:
            # Never persist credentials, response bodies or private content from exception text.
            self._fail(source, request, token, "error", type(exc).__name__)
            return {**result, "status": "error", "error_type": type(exc).__name__}

    def _validate_page(self, source: SourceSpec, request: ScanRequest, page: Page) -> None:
        if not isinstance(page, Page) or type(page.has_more) is not bool:
            raise PageProtocolError("adapter must return Page")
        if page.has_more and (not page.next_page_token or page.next_page_token == request.page_token):
            raise PageProtocolError("non-advancing page token")
        for event in page.events:
            event.validate()
            if event.tenant_id != source.tenant_id or event.channel != source.channel:
                raise PageProtocolError("page crossed tenant/channel scope")
            if not set(event.project_ids).issubset(source.project_ids):
                raise PageProtocolError("page crossed project scope")
            if source.channel != "resource" and event.chat_id != source.effective_chat_id:
                raise PageProtocolError("page crossed chat scope")
            if source.container_type == "thread" and event.thread_id != source.native_id:
                raise PageProtocolError("page crossed thread scope")
        for child in page.discovered_sources:
            if child.tenant_id != source.tenant_id or child.identity != source.identity:
                raise PageProtocolError("discovered source crossed identity")
            if child.channel != source.channel or child.effective_chat_id != source.effective_chat_id:
                raise PageProtocolError("discovered source crossed chat scope")
            if not set(child.project_ids).issubset(source.project_ids):
                raise PageProtocolError("discovered source crossed project scope")

    def _fail(self, source, request, token, state, detail, *, reset_cursor=False):
        with self.store.transaction() as db:
            db.execute("""UPDATE gw_source_scans SET state=?,last_error=?,claim_token='',lease_until=0,
                retry_at=?,page_token=CASE WHEN ? THEN '' ELSE page_token END,
                restart_count=restart_count+? WHERE source_id=? AND lane=? AND claim_token=?""",
                (state, detail, self.store.clock() + self.retry_seconds, int(reset_cursor), int(reset_cursor),
                 source.source_id, request.lane, token))

    def run_due(self, max_pages: int = 8) -> list[dict]:
        """Bounded fair rotation; no single large history monopolizes the scheduler."""
        results = []
        for _ in range(max_pages):
            result = self.collect_one()
            if result is None:
                break
            results.append(result)
        return results

    def coverage(self) -> list[dict]:
        rows = self.store.rows("""SELECT g.source_id,g.spec_json,g.active,g.discovered_by,
            s.lane,s.state,s.completed_until,s.window_start,s.window_end,s.page_count,s.item_count,
            s.restart_count,s.last_error,s.last_attempt FROM gw_sources g
            JOIN gw_source_scans s USING(source_id) WHERE g.tenant_id=? ORDER BY g.source_id,s.lane""",
            (self.config.tenant_id,))
        for row in rows:
            source = SourceSpec.from_dict(json.loads(row.pop("spec_json")))
            row.update(source_kind=source.source_kind, native_id=source.native_id, identity=source.identity,
                       adapter=source.adapter, adapter_bound=source.adapter in self.adapters,
                       history_policy="all_available_history", container_type=source.container_type)
            if source.adapter not in self.adapters:
                row["state"] = "unbound"
            row["coverage_meaning"] = ("visible full traversal; no thread time filter" if source.container_type == "thread"
                                       else "fixed-window traversal for this identity")
        return rows


def _api_data(response: Mapping) -> dict:
    if not isinstance(response, Mapping):
        raise PageProtocolError("API object required")
    if response.get("code", 0) != 0:
        # Host transport can classify documented expired tokens into CursorExpired.
        # No guessed Feishu error codes are silently interpreted as success.
        raise FeishuAPIError("Feishu API returned nonzero code")
    data = response.get("data")
    if not isinstance(data, dict) or type(data.get("has_more")) is not bool or not isinstance(data.get("items"), list):
        raise PageProtocolError("missing items/has_more; incomplete response")
    return data


class FeishuMessageAdapter:
    """GET /open-apis/im/v1/messages. request(url, params, identity) supplies auth.

    API identities and source scope are trusted deployment bindings. Bot tokens
    cannot be treated as access to the owner's colleague DMs. No tokens are stored.
    """
    url = "https://open.feishu.cn/open-apis/im/v1/messages"

    def __init__(self, request: Callable, *, bot_id: str, page_size: int = 50):
        if not 1 <= page_size <= 50:
            raise ContractError("message page_size must be 1..50")
        self.request, self.bot_id, self.page_size = request, bot_id, page_size

    def read_page(self, source: SourceSpec, request: ScanRequest) -> Page:
        if source.container_type not in {"chat", "thread"}:
            raise ContractError("message adapter needs chat or thread")
        params = {"container_id_type": source.container_type, "container_id": source.native_id,
                  "sort_type": "ByCreateTimeAsc", "page_size": self.page_size}
        if source.container_type == "chat":
            params.update(start_time=str(math.floor(request.window_start)),
                          end_time=str(math.ceil(request.window_end)))
        if request.page_token:
            params["page_token"] = request.page_token
        data = _api_data(self.request(self.url, params, source.identity))
        events, threads = [], {}
        for message in data["items"]:
            events.append(self._event(source, message))
            thread_id = message.get("thread_id")
            if source.container_type == "chat" and thread_id:
                thread = replace(source, native_id=thread_id, source_kind="thread", container_type="thread",
                                 chat_id=source.effective_chat_id)
                threads[thread.source_id] = thread
        return Page(tuple(events), data["has_more"], data.get("page_token", ""), tuple(threads.values()))

    def _event(self, source, message) -> Event:
        if message.get("chat_id") and message["chat_id"] != source.effective_chat_id:
            raise PageProtocolError("message chat mismatch")
        if source.container_type == "thread" and message.get("thread_id") not in (None, "", source.native_id):
            raise PageProtocolError("message thread mismatch")
        sender = message.get("sender", {}).get("id", "")
        if isinstance(sender, dict):
            sender = sender.get("open_id", "")
        raw = message.get("body", {}).get("content", "{}")
        text, attachments, status = normalized_content(raw, message.get("msg_type", ""))
        mention_ids = []
        for mention in message.get("mentions", []):
            value = mention.get("id", "")
            if isinstance(value, dict):
                value = value.get("open_id", "")
            if value:
                mention_ids.append(value)
        deleted = bool(message.get("deleted", False))
        return Event(tenant_id=source.tenant_id, native_id=message["message_id"],
                     chat_id=source.effective_chat_id, sender_id=sender, body=text,
                     original_time=int(message["create_time"]) / 1000,
                     version_time=int(message.get("update_time") or message["create_time"]) / 1000,
                     channel=source.channel, thread_id=(source.native_id if source.container_type == "thread"
                                                       else message.get("thread_id", "")),
                     parent_id=message.get("parent_id", ""), root_id=message.get("root_id", ""),
                     mentions=tuple(mention_ids), project_ids=source.project_ids,
                     attachments=tuple(attachments), is_self=sender == self.bot_id,
                     is_deleted=deleted, direction="outbound" if sender == self.bot_id else "inbound",
                     content_status=status, observed_via="poll")


class JsonCommandTransport:
    """Host-owned executable speaks one JSON request/response; no shell expansion.

    This is a transport contract, not a claim that lark-cli has any such command.
    Command configuration must come from trusted deployment configuration.
    """
    def __init__(self, argv: list[str], *, timeout: float = 30, max_bytes: int = 8_000_000):
        if not argv or not all(isinstance(a, str) for a in argv):
            raise ContractError("argv list required")
        self.argv, self.timeout, self.max_bytes = tuple(argv), timeout, max_bytes

    def __call__(self, payload: dict) -> dict:
        process = subprocess.run(self.argv, input=dumps(payload), text=True, capture_output=True,
                                 timeout=self.timeout, check=False, shell=False)
        if process.returncode:
            raise RuntimeError("collection command failed")
        if len(process.stdout.encode()) > self.max_bytes:
            raise PageProtocolError("oversized command response")
        return json.loads(process.stdout)


class JsonHTTPTransport:
    """Host-bound normalized JSON endpoint; it is not a Feishu API wrapper."""
    def __init__(self, url: str, *, headers: Mapping[str, str] | None = None, timeout: float = 30,
                 max_bytes: int = 8_000_000):
        if not url.startswith(("https://", "http://127.0.0.1:", "http://localhost:")):
            raise ContractError("HTTPS or explicit loopback endpoint required")
        self.url, self.headers, self.timeout, self.max_bytes = url, dict(headers or {}), timeout, max_bytes

    def __call__(self, payload: dict) -> dict:
        request = Request(self.url, data=dumps(payload).encode(),
                          headers={**self.headers, "Content-Type": "application/json"}, method="POST")
        with urlopen(request, timeout=self.timeout) as response:
            raw = response.read(self.max_bytes + 1)
        if len(raw) > self.max_bytes:
            raise PageProtocolError("oversized HTTP response")
        return json.loads(raw)


class JsonPageAdapter:
    """Normalized adapter for a host exporter or approved resource connector."""
    def __init__(self, transport: Callable[[dict], dict]):
        self.transport = transport

    def read_page(self, source: SourceSpec, request: ScanRequest) -> Page:
        raw = self.transport({"operation": "read_page", "source": asdict(source), "scan": asdict(request)})
        if raw.get("status") == "cursor_expired":
            raise CursorExpired()
        if raw.get("status") == "unreadable":
            raise SourceUnreadable()
        if raw.get("status") != "ok" or type(raw.get("has_more")) is not bool or not isinstance(raw.get("items"), list):
            raise PageProtocolError("normalized page contract invalid")
        return Page(tuple(Event.from_dict(v) for v in raw["items"]), raw["has_more"], raw.get("page_token", ""),
                    tuple(SourceSpec.from_dict(v) for v in raw.get("discovered_sources", [])))
