"""Durable source discovery. Inventory coverage is separate from content coverage."""
from __future__ import annotations

from dataclasses import asdict, dataclass, field
import json
import uuid
from typing import Callable, Mapping

from .collectors import (CollectorEngine, CursorExpired, PageProtocolError, SourceSpec,
                         SourceUnreadable, _api_data)
from .models import ContractError, dumps, key


@dataclass(frozen=True)
class DiscoverySpec:
    tenant_id: str
    name: str
    adapter: str
    identity: str = "bot"
    scope: str = "joined_groups"
    poll_seconds: float = 300
    metadata: dict = field(default_factory=dict)

    @property
    def discovery_id(self):
        return key(self.tenant_id, self.name, self.identity)


@dataclass(frozen=True)
class DiscoveryPage:
    sources: tuple[SourceSpec, ...] = ()
    has_more: bool = False
    next_page_token: str = ""
    complete_for_scope: bool = True


class DiscoveryEngine:
    def __init__(self, collectors: CollectorEngine, adapters: Mapping | None = None,
                 *, lease_seconds: float = 90, retry_seconds: float = 30):
        self.collectors, self.store = collectors, collectors.store
        self.adapters = dict(adapters or {})
        self.lease_seconds, self.retry_seconds = lease_seconds, retry_seconds
        with self.store.transaction() as db:
            db.execute("""CREATE TABLE IF NOT EXISTS gw_discovery(
                discovery_id TEXT PRIMARY KEY,tenant_id TEXT NOT NULL,spec_json TEXT NOT NULL,
                state TEXT NOT NULL DEFAULT 'idle',page_token TEXT NOT NULL DEFAULT '',
                generation INTEGER NOT NULL DEFAULT 0,started_at REAL,completed_at REAL,
                all_pages_complete INTEGER NOT NULL DEFAULT 1,
                claim_token TEXT NOT NULL DEFAULT '',lease_until REAL NOT NULL DEFAULT 0,
                retry_at REAL NOT NULL DEFAULT 0,last_attempt REAL NOT NULL DEFAULT 0,
                page_count INTEGER NOT NULL DEFAULT 0,source_count INTEGER NOT NULL DEFAULT 0,
                last_error TEXT NOT NULL DEFAULT '')""")

    def register(self, spec: DiscoverySpec) -> str:
        if spec.tenant_id != self.collectors.config.tenant_id or not all((spec.name, spec.adapter, spec.identity, spec.scope)):
            raise ContractError("invalid discovery binding")
        if spec.poll_seconds < 0:
            raise ContractError("invalid discovery interval")
        with self.store.transaction() as db:
            previous = db.execute("SELECT spec_json FROM gw_discovery WHERE discovery_id=?", (spec.discovery_id,)).fetchone()
            if previous:
                old = DiscoverySpec(**json.loads(previous["spec_json"]))
                if (old.scope, old.adapter) != (spec.scope, spec.adapter):
                    raise ContractError("new discovery scope/adapter requires a distinct binding name")
            db.execute("""INSERT INTO gw_discovery(discovery_id,tenant_id,spec_json) VALUES(?,?,?)
                ON CONFLICT(discovery_id) DO UPDATE SET spec_json=excluded.spec_json""",
                (spec.discovery_id, spec.tenant_id, dumps(asdict(spec))))
        return spec.discovery_id

    def run_due(self, max_pages: int = 4) -> list[dict]:
        results = []
        for _ in range(max_pages):
            result = self.discover_one()
            if result is None:
                break
            results.append(result)
        return results

    def discover_one(self, discovery_id: str | None = None) -> dict | None:
        now = self.store.clock()
        with self.store.transaction() as db:
            sql = "SELECT * FROM gw_discovery WHERE tenant_id=? AND retry_at<=? AND lease_until<=?"
            args = [self.collectors.config.tenant_id, now, now]
            if discovery_id is not None:
                sql += " AND discovery_id=?"; args.append(discovery_id)
            row = db.execute(sql + " ORDER BY last_attempt,discovery_id LIMIT 1", args).fetchone()
            if row is None:
                return None
            spec = DiscoverySpec(**json.loads(row["spec_json"]))
            token = uuid.uuid4().hex
            latest = db.execute("SELECT COALESCE(MAX(last_attempt),0) FROM gw_discovery").fetchone()[0]
            attempted_at = max(now, latest + 0.000001)
            fresh = row["started_at"] is None
            db.execute("""UPDATE gw_discovery SET claim_token=?,lease_until=?,last_attempt=?,state='scanning',
                started_at=COALESCE(started_at,?),generation=generation+?,
                all_pages_complete=CASE WHEN ? THEN 1 ELSE all_pages_complete END
                WHERE discovery_id=?""", (token, now + self.lease_seconds, attempted_at, now, int(fresh), int(fresh), spec.discovery_id))
            page_token = row["page_token"]
        try:
            adapter = self.adapters.get(spec.adapter)
            if adapter is None:
                self._fail(spec, token, "unbound", "adapter not bound")
                return {"discovery_id": spec.discovery_id, "status": "unbound"}
            page = adapter.discover_page(spec, page_token)
            if not isinstance(page, DiscoveryPage) or type(page.has_more) is not bool or type(page.complete_for_scope) is not bool:
                raise PageProtocolError("invalid discovery page")
            if page.has_more and (not page.next_page_token or page.next_page_token == page_token):
                raise PageProtocolError("discovery cursor did not advance")
            with self.store.transaction() as db:
                current = db.execute("SELECT * FROM gw_discovery WHERE discovery_id=?", (spec.discovery_id,)).fetchone()
                if current["claim_token"] != token:
                    return {"discovery_id": spec.discovery_id, "status": "superseded"}
                for source in page.sources:
                    if source.tenant_id != spec.tenant_id or source.identity != spec.identity:
                        raise PageProtocolError("discovered source identity mismatch")
                    self.collectors.register_in_tx(db, source, discovered_by=spec.discovery_id)
                complete = bool(current["all_pages_complete"]) and page.complete_for_scope
                state = "scanning" if page.has_more else ("complete_for_scope" if complete else "partial")
                db.execute("""UPDATE gw_discovery SET state=?,page_token=?,all_pages_complete=?,
                    claim_token='',lease_until=0,retry_at=?,last_error='',page_count=page_count+1,
                    source_count=source_count+?,started_at=CASE WHEN ? THEN started_at ELSE NULL END,
                    completed_at=CASE WHEN ? THEN ? ELSE completed_at END WHERE discovery_id=?""",
                    (state, page.next_page_token if page.has_more else "", int(complete),
                     0 if page.has_more else self.store.clock() + spec.poll_seconds, len(page.sources),
                     int(page.has_more), int(not page.has_more and complete), self.store.clock(), spec.discovery_id))
            return {"discovery_id": spec.discovery_id, "status": state, "sources": len(page.sources)}
        except CursorExpired:
            self._fail(spec, token, "scanning", "cursor expired; enumeration restarted", reset=True)
            return {"discovery_id": spec.discovery_id, "status": "cursor_restarted"}
        except SourceUnreadable:
            self._fail(spec, token, "unreadable", "inventory inaccessible")
            return {"discovery_id": spec.discovery_id, "status": "unreadable"}
        except Exception as exc:
            self._fail(spec, token, "error", type(exc).__name__)
            return {"discovery_id": spec.discovery_id, "status": "error", "error_type": type(exc).__name__}

    def _fail(self, spec, token, state, detail, *, reset=False):
        with self.store.transaction() as db:
            db.execute("""UPDATE gw_discovery SET state=?,last_error=?,claim_token='',lease_until=0,retry_at=?,
                page_token=CASE WHEN ? THEN '' ELSE page_token END,
                all_pages_complete=CASE WHEN ? THEN 1 ELSE all_pages_complete END
                WHERE discovery_id=? AND claim_token=?""",
                (state, detail, self.store.clock() + self.retry_seconds, int(reset), int(reset), spec.discovery_id, token))

    def coverage(self) -> list[dict]:
        rows = self.store.rows("""SELECT discovery_id,spec_json,state,generation,completed_at,
            page_count,source_count,last_attempt,last_error FROM gw_discovery WHERE tenant_id=?""",
            (self.collectors.config.tenant_id,))
        for row in rows:
            spec = DiscoverySpec(**json.loads(row.pop("spec_json")))
            row.update(scope=spec.scope, identity=spec.identity, adapter=spec.adapter,
                       adapter_bound=spec.adapter in self.adapters)
            if spec.adapter not in self.adapters:
                row["state"] = "unbound"
        return rows


class FeishuGroupsDiscovery:
    """GET /im/v1/chats lists joined groups ONLY; private chats need an authorized inventory."""
    url = "https://open.feishu.cn/open-apis/im/v1/chats"

    def __init__(self, request: Callable, *, message_adapter: str = "feishu_messages", page_size: int = 100):
        if not 1 <= page_size <= 100:
            raise ContractError("group page_size must be 1..100")
        self.request, self.message_adapter, self.page_size = request, message_adapter, page_size

    def discover_page(self, spec: DiscoverySpec, page_token: str) -> DiscoveryPage:
        if spec.scope != "joined_groups":
            raise ContractError("group API cannot discover the requested scope")
        params = {"sort_type": "ByCreateTimeAsc", "page_size": self.page_size}
        if page_token:
            params["page_token"] = page_token
        data = _api_data(self.request(self.url, params, spec.identity))
        sources = tuple(SourceSpec(tenant_id=spec.tenant_id, native_id=item["chat_id"],
                                  adapter=self.message_adapter, identity=spec.identity,
                                  metadata={"name": item.get("name", ""), "visibility": "joined_groups"})
                        for item in data["items"])
        return DiscoveryPage(sources, data["has_more"], data.get("page_token", ""), True)


class JsonDiscoveryAdapter:
    """Explicit normalized discovery contract for a trusted host connector."""
    def __init__(self, transport: Callable[[dict], dict]):
        self.transport = transport

    def discover_page(self, spec: DiscoverySpec, page_token: str) -> DiscoveryPage:
        raw = self.transport({"operation": "discover_page", "discovery": asdict(spec), "page_token": page_token})
        if raw.get("status") == "cursor_expired":
            raise CursorExpired()
        if raw.get("status") == "unreadable":
            raise SourceUnreadable()
        if (raw.get("status") != "ok" or type(raw.get("has_more")) is not bool
                or type(raw.get("complete_for_scope")) is not bool or not isinstance(raw.get("sources"), list)):
            raise PageProtocolError("normalized discovery contract invalid")
        return DiscoveryPage(tuple(SourceSpec.from_dict(v) for v in raw["sources"]), raw["has_more"],
                             raw.get("page_token", ""), raw["complete_for_scope"])


RESOURCE_CATALOG = (
    {"domain": "joined_groups", "implementation": "FeishuGroupsDiscovery + FeishuMessageAdapter",
     "host_binding": "required", "limit": "identity-visible joined groups; not personal DMs"},
    {"domain": "bot_dm", "implementation": "FeishuMessageAdapter; explicit trusted chat inventory",
     "host_binding": "required", "limit": "no p2p enumeration through group-list API"},
    {"domain": "personal_dm", "implementation": "JSON inventory + FeishuMessageAdapter/host exporter",
     "host_binding": "unbound", "limit": "authorized user-visible inventory required; bot identity insufficient"},
    {"domain": "threads", "implementation": "derived from message thread_id + FeishuMessageAdapter",
     "host_binding": "required", "limit": "full pagination without start/end; only visible threads discovered"},
    {"domain": "docs_wiki_drive", "implementation": "generic JSON discovery/page framework only",
     "host_binding": "unbound", "limit": "resource listing, document content and permission adapter not implemented"},
    {"domain": "meegle_tasks_bitable_calendar", "implementation": "generic JSON discovery/page framework only",
     "host_binding": "unbound", "limit": "platform-specific adapters not implemented"},
    {"domain": "attachments", "implementation": "message resource references only",
     "host_binding": "unbound", "limit": "download/parse/access control connector not implemented"},
)


def resource_coverage() -> list[dict]:
    return [dict(row) for row in RESOURCE_CATALOG]
