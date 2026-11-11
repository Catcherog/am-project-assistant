"""Production collection into the Harness inbox, with atomic page checkpoints.

This module reuses the existing source/discovery tables. It never opens a Feishu
long connection and never calls an LLM. A completed history traversal means the
configured identity's visible history, not a tenant-wide completeness claim.
"""
from __future__ import annotations

from dataclasses import dataclass, replace
import json
import math

from am_gateway.collectors import (CollectorEngine, CursorExpired, FeishuMessageAdapter,
                                   Page, SourceUnreadable, SourceSpec, _api_data)
from am_gateway.collection_service import CollectionService
from am_gateway.feishu_snapshot import normalize_feishu_snapshot
from am_gateway.models import ContractError, Event, dumps, key
from am_gateway.phase1.contracts import Policy


@dataclass(frozen=True)
class SnapshotPage(Page):
    excluded_items: tuple = ()


class SnapshotPageAdapter(FeishuMessageAdapter):
    """Every raw REST list item is a complete version, including attachment removal."""
    def __init__(self, request, config, verify_source, *, page_size=50):
        super().__init__(request, bot_id=config.bot_id, page_size=page_size)
        self.config, self.verify_source = config, verify_source

    def read_page(self, source, request):
        from integration.studio_host import platform_system_notice
        if source.container_type not in {'chat', 'thread'}:
            raise ContractError('message adapter needs chat or thread')
        params={'container_id_type':source.container_type,'container_id':source.native_id,
                'sort_type':'ByCreateTimeAsc','page_size':self.page_size}
        if source.container_type=='chat':
            params.update(start_time=str(math.floor(request.window_start)),end_time=str(math.ceil(request.window_end)))
        if request.page_token:params['page_token']=request.page_token
        data=_api_data(self.request(self.url,params,source.identity))
        events,threads,excluded=[],{},[]
        for message in data['items']:
            if platform_system_notice(message):
                mid=message.get('message_id')
                if (message.get('chat_id')!=source.effective_chat_id or not isinstance(mid,str) or not mid.startswith('om_')
                        or source.container_type=='thread' and message.get('thread_id')!=source.native_id):
                    raise SourceUnreadable('system notice source mismatch')
                excluded.append({'message_id':mid,'reason':'platform_system_without_actor'})
                continue
            events.append(self._event(source,message))
            tid=message.get('thread_id')
            if source.container_type=='chat' and tid:
                threads[tid]=replace(source,native_id=tid,source_kind='thread',container_type='thread',
                                     chat_id=source.effective_chat_id)
        return SnapshotPage(tuple(events),data['has_more'],data.get('page_token',''),tuple(threads.values()),tuple(excluded))

    def _event(self, source, message):
        if not self.verify_source(source, message):
            raise SourceUnreadable('raw source rejected')
        return normalize_feishu_snapshot(message, self.config, source)


class HarnessCollector(CollectorEngine):
    """Reuse leases/pagination and durably hand live work to the assistant.

    Assistant intake commits before the page checkpoint. If the later page
    transaction fails, replay sees the same intake identity and cannot execute
    the request through a second, direct Harness path.
    """
    def __init__(self, harness, config, adapters, *, authorization_for,
                 legacy_owned=None, source_allowed=None, notify=None, **kwargs):
        super().__init__(harness.store, config, adapters, **kwargs)
        self.harness, self.authorization_for = harness, authorization_for
        self.legacy_owned = legacy_owned or (lambda event: False)
        self.source_allowed = source_allowed or (lambda source: True)
        self.notify = notify or (lambda: None)
        self.assistant = None

    def collect_one(self, source_id=None, lane=None):
        claimed = self._claim(source_id, lane)
        if claimed is None:
            return None
        source, request, token = claimed
        result = {'source_id': source.source_id, 'lane': request.lane}
        adapter = self.adapters.get(source.adapter)
        if adapter is None:
            self._fail(source, request, token, 'unbound', 'adapter not bound')
            return {**result, 'status': 'unbound'}
        try:
            if not self.source_allowed(source):
                self.disable_source(source.source_id)
                return {**result, 'status': 'revoked'}
            page = adapter.read_page(source, request)
            self._validate_page(source, request, page)
            if not self.source_allowed(source):
                self.disable_source(source.source_id)
                return {**result, 'status': 'revoked'}
            # Host permission checks can read other databases or refresh identity;
            # perform them before opening the short write transaction.
            prepared = []
            for event in page.events:
                live = event.original_time >= self.config.live_cutover_at
                # Thread listings may contain newer messages despite a history
                # scan's time window. The live lane owns those messages: merely
                # collecting them here could also revoke an existing live task.
                if request.lane == 'history' and live:
                    continue
                legacy = bool(self.legacy_owned(event)) if live else False
                event = replace(event, observed_via='poll:' + request.lane)
                if live and self.assistant is not None:
                    captured = self.assistant.capture(event, legacy_owned=legacy)
                    if isinstance(captured, dict) and captured.get('captured') is True:
                        continue
                policy = self.authorization_for(event) if live else None
                prepared.append((event, policy, legacy, live))
            if not self.source_allowed(source):
                self.disable_source(source.source_id)
                return {**result, 'status': 'revoked'}
            with self.store.transaction() as db:
                current = db.execute('SELECT claim_token FROM gw_source_scans WHERE source_id=? AND lane=?',
                                     (source.source_id, request.lane)).fetchone()
                active = db.execute('SELECT active FROM gw_sources WHERE source_id=?', (source.source_id,)).fetchone()
                if not current or current[0] != token or not active or not active[0]:
                    return {**result, 'status': 'superseded'}
                for event, policy, legacy, live in prepared:
                    self.harness.ingest(event, policy,
                        collection_only=not live or policy is None, legacy_owned=legacy, _db=db)
                for excluded in getattr(page, 'excluded_items', ()):
                    self.store.audit(db, 'platform_system_excluded', source.source_id, dumps(excluded))
                for child in page.discovered_sources:
                    self.register_in_tx(db, child, discovered_by=source.source_id)
                if page.has_more:
                    db.execute('''UPDATE gw_source_scans SET page_token=?,claim_token='',lease_until=0,
                        state='scanning',last_error='',page_count=page_count+1,item_count=item_count+?
                        WHERE source_id=? AND lane=?''',
                        (page.next_page_token, len(page.events), source.source_id, request.lane))
                else:
                    db.execute('''UPDATE gw_source_scans SET page_token='',claim_token='',lease_until=0,
                        state=?,completed_until=?,window_start=NULL,window_end=NULL,retry_at=?,last_error='',
                        page_count=page_count+1,item_count=item_count+? WHERE source_id=? AND lane=?''',
                        ('complete' if request.lane == 'history' else 'idle', request.window_end,
                         self.store.clock() + source.poll_seconds, len(page.events), source.source_id, request.lane))
                coverage = {'lane': request.lane, 'window_start': request.window_start,
                            'window_end': request.window_end, 'has_more': page.has_more,
                            'history': 'visible_scope_traversal',
                            'excluded_system_notices':len(getattr(page,'excluded_items',()))}
                db.execute('''INSERT INTO h1_sync VALUES(?,?,'saved','',?,?)
                    ON CONFLICT(source) DO UPDATE SET cursor=excluded.cursor,state='saved',
                    detail='',coverage=excluded.coverage,updated=excluded.updated''',
                    (source.source_id, dumps({'page_token': page.next_page_token if page.has_more else ''}),
                     dumps(coverage), self.store.clock()))
            self.notify()
            return {**result, 'status': 'page' if page.has_more else 'completed', 'items': len(page.events)}
        except CursorExpired:
            self._fail(source, request, token, 'scanning', 'cursor expired; same window restarted', reset_cursor=True)
            return {**result, 'status': 'cursor_restarted'}
        except SourceUnreadable:
            self._fail(source, request, token, 'unreadable', 'source access denied or unavailable')
            return {**result, 'status': 'unreadable'}
        except Exception as exc:
            self._fail(source, request, token, 'error', type(exc).__name__)
            return {**result, 'status': 'error', 'error_type': type(exc).__name__}

    def disable_source(self, source_id):
        with self.store.transaction() as db:
            db.execute('UPDATE gw_sources SET active=0 WHERE source_id=?', (source_id,))
            db.execute("UPDATE gw_source_scans SET state='revoked',claim_token='',lease_until=0 WHERE source_id=?",
                       (source_id,))


class RefreshKnownMessages:
    """Durable round-robin exact-ID refresh catches edits outside overlap windows.

    At most max_messages are read on each background tick. Rotation covers all
    stored known messages; undiscovered history remains the paginator's job.
    """
    def __init__(self, ingress, collectors, *, interval=300, max_messages=4):
        if interval <= 0 or max_messages < 1:
            raise ContractError('positive refresh interval/budget required')
        self.ingress, self.collectors = ingress, collectors
        self.store, self.interval, self.max_messages = collectors.store, interval, max_messages
        with self.store.transaction() as db:
            db.execute('''CREATE TABLE IF NOT EXISTS r4_message_refresh(
                source_id TEXT NOT NULL,message_id TEXT NOT NULL,next_at REAL NOT NULL DEFAULT 0,
                last_attempt REAL NOT NULL DEFAULT 0,state TEXT NOT NULL DEFAULT 'pending',
                PRIMARY KEY(source_id,message_id))''')

    def run_due(self):
        sources = {}
        for row in self.store.rows('SELECT source_id,spec_json FROM gw_sources WHERE active=1'):
            source = SourceSpec.from_dict(json.loads(row['spec_json']))
            if source.container_type == 'chat' and self.collectors.source_allowed(source):
                sources[source.source_id] = source
        with self.store.transaction() as db:
            for source in sources.values():
                db.execute('''INSERT OR IGNORE INTO r4_message_refresh(source_id,message_id)
                    SELECT ?,native_id FROM gw_items WHERE tenant_id=? AND chat_id=? AND kind='message' ''',
                    (source.source_id, source.tenant_id, source.effective_chat_id))
            # The join excludes revoked sources and orders by oldest attempt.
            rows = db.execute('''SELECT r.* FROM r4_message_refresh r JOIN gw_sources s USING(source_id)
                WHERE s.active=1 AND r.next_at<=? ORDER BY r.last_attempt,r.source_id,r.message_id LIMIT ?''',
                (self.store.clock(), self.max_messages)).fetchall()
        results = []
        for row in rows:
            source = sources.get(row['source_id'])
            if source is None:
                continue
            state = 'saved'
            try:
                self.ingress.refresh_message(row['message_id'], source=source)
            except Exception as exc:
                state = type(exc).__name__
            with self.store.transaction() as db:
                db.execute('UPDATE r4_message_refresh SET next_at=?,last_attempt=?,state=? WHERE source_id=? AND message_id=?',
                    (self.store.clock() + self.interval, self.store.clock(), state, source.source_id, row['message_id']))
            results.append({'source_id': source.source_id, 'message_id': row['message_id'], 'state': state})
        return results


class Phase1CollectionService(CollectionService):
    def __init__(self, collectors, discovery=None, *, ingress, refresh_interval=300,
                 refresh_messages=4, authorization_refresh=None, owner_projection=None, **kwargs):
        super().__init__(collectors, discovery, **kwargs)
        self.refresh = RefreshKnownMessages(ingress, collectors, interval=refresh_interval,
                                            max_messages=refresh_messages)
        self.authorization_refresh = authorization_refresh
        self.owner_projection = owner_projection

    def run_due(self):
        # Read authorization changes even when no new source messages arrive.
        revoked = self.authorization_refresh() if self.authorization_refresh else []
        result = super().run_due()
        result['authorization_updates'] = revoked
        result['refresh'] = self.refresh.run_due()
        result['owner_projection'] = self.owner_projection() if self.owner_projection else 0
        return result


class LegacyOwnership:
    """A shared claim API for the old poller and the Harness, in the same SQLite DB.

    Old polling calls claim(event) before executing. The Harness's existing
    legacy_owned check then sees the claim inside its transaction. If Harness
    admitted first, the same transaction denies the old claim.
    """
    def __init__(self, harness):
        self.harness, self.store = harness, harness.store
        with self.store.transaction() as db:
            db.execute('''CREATE TABLE IF NOT EXISTS r4_legacy_exclusions(
                item_id TEXT PRIMARY KEY,scope TEXT NOT NULL,reason TEXT NOT NULL)''')
            db.execute('''CREATE TABLE IF NOT EXISTS r4_legacy_claims(
                item_id TEXT PRIMARY KEY,state TEXT NOT NULL,claimed_at REAL NOT NULL,
                result_ref TEXT NOT NULL DEFAULT '')''')

    def should_skip(self, event):
        with self.store.transaction() as db:
            return self._skip(db, event.item_id)

    legacy_should_skip = should_skip

    def ensure_scope(self, scope):
        # This method is called only after the host has bound the legacy claim
        # port. It records the actual shared database mechanism, not a boolean.
        self.harness.register_cutover(scope, evidence_ref='sqlite-shared-legacy-claim:' + self.store.path)
        return {'scope': scope, 'state': 'harness', 'claim_port': self.claim}

    @staticmethod
    def _skip(db, item_id):
        if db.execute('SELECT 1 FROM r4_legacy_exclusions WHERE item_id=?', (item_id,)).fetchone():
            return True
        if db.execute('SELECT 1 FROM r4_legacy_claims WHERE item_id=?', (item_id,)).fetchone():
            return True
        # R5 acknowledges a message as owned before background directory and
        # policy work creates an h1 inbox row. This includes referred/declined
        # requests: the old poller must not turn them into executable work.
        if (db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='r5_intake'").fetchone()
                and db.execute('SELECT 1 FROM r5_intake WHERE item_id=?', (item_id,)).fetchone()):
            return True
        task = db.execute('SELECT business_id,dispatch_state FROM h1_tasks WHERE item_id=?', (item_id,)).fetchone()
        if task:
            if task['business_id']:
                return True  # Unknown executions also remain with their original owner.
            if task['dispatch_state'] == 'legacy_owned':
                return False
        item = db.execute('SELECT legacy_owned FROM gw_items WHERE item_id=?', (item_id,)).fetchone()
        if item and item[0]:
            return True
        row = db.execute('SELECT accepted FROM h1_inbox WHERE item_id=?', (item_id,)).fetchone()
        return bool(row and row[0])

    def claim(self, event):
        event.validate()
        if event.tenant_id != self.harness.config.tenant_id:
            raise ContractError('legacy claim tenant mismatch')
        with self.store.transaction() as db:
            if self._skip(db, event.item_id):
                return False
            self.store.ingest_in_tx(db, event)
            db.execute('UPDATE gw_items SET legacy_owned=1 WHERE item_id=?', (event.item_id,))
            db.execute("INSERT INTO r4_legacy_claims(item_id,state,claimed_at) VALUES(?,'claimed',?)",
                       (event.item_id, self.store.clock()))
            self.store.audit(db, 'legacy_poll_claim', event.item_id, 'shared gateway claim')
            return True

    def status(self, item_id):
        return self.store.one('SELECT * FROM r4_legacy_claims WHERE item_id=?', (item_id,))

    def finish(self, item_id, *, state, result_ref=''):
        if state not in {'completed', 'failed', 'unknown'} or not isinstance(result_ref, str):
            raise ContractError('invalid legacy result state')
        with self.store.transaction() as db:
            row = db.execute('SELECT * FROM r4_legacy_claims WHERE item_id=?', (item_id,)).fetchone()
            if not row:
                raise ContractError('legacy operation not claimed')
            if row['state'] == 'completed' and (state != 'completed' or result_ref != row['result_ref']):
                raise ContractError('completed legacy result is immutable')
            db.execute('UPDATE r4_legacy_claims SET state=?,result_ref=? WHERE item_id=?',
                       (state, result_ref, item_id))
        return self.status(item_id)

    def filter_page(self, events):
        return [event for event in events if self.claim(event)]

    def prepare_rollback(self, scope):
        return self.harness.prepare_rollback(scope)

    def import_rollback(self, manifest):
        required = {r['item_id'] for group in ('completed', 'unresolved') for r in manifest[group]}
        with self.store.transaction() as db:
            for item_id in required:
                db.execute('INSERT OR REPLACE INTO r4_legacy_exclusions VALUES(?,?,?)',
                           (item_id, manifest['scope'], 'completed_or_unresolved'))
        return self.harness.acknowledge_legacy_import(manifest['scope'],
            fingerprint=manifest['fingerprint'], excluded_items=required,
            pending_owner={r['task_id']: 'legacy' for r in manifest['undispatched']})
