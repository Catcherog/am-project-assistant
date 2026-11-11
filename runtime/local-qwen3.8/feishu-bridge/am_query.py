"""Private read capability and shared source snapshots; no model or business writes."""
from __future__ import annotations
import copy
import json
import threading
import time
import re
import uuid
from concurrent.futures import ThreadPoolExecutor
from contextvars import copy_context
from fs_agent.config import BoundaryError
from fs_agent.project_catalog import digest
from fs_agent import request_timing as timing
from query_rules import identifiers, deterministic_intent

READ_PAGES = frozenset(('home', 'projects', 'search', 'project', 'recent', 'refresh',
                        'long_content', 'reconcile_navigation'))
ENTRY_TEXT = frozenset(('/menu', '/am', '/project', '项目', '项目查询', '项目列表', '查看项目'))

def field_followup(text):
    return bool(re.fullmatch(r'(?:进展|进度|日期|计划|人员|谁负责|负责人|待办|下一步|和|与|、|分别|是|什么|呢|的|[，？?。\s])+',text))


class SourceLoading(BoundaryError):
    pass


class QueryIdentityCache:
    """Optional short membership lease keyed only by trusted immutable user ID.

    Default is strict (zero seconds). Never used by management or feedback.
    A lease starts before the network check, not after a slow response.
    """
    def __init__(self, check, seconds=0):
        if type(seconds) is not int or not 0<=seconds<=600:raise BoundaryError('QUERY_IDENTITY_TTL_INVALID')
        self.check,self.seconds=check,seconds
        self.lock=threading.RLock();self.entries={};self.pending={};self.active={}
        self.pool=ThreadPoolExecutor(max_workers=3,thread_name_prefix='query-membership')

    def _refresh(self,user):
        started=time.monotonic()
        try:
            result=self.check(user)
            deadline=started+self.seconds
            if isinstance(result,dict):deadline=min(deadline,result.get('valid_until',deadline))
            with self.lock:self.entries[user]=deadline
        except Exception as exc:
            # A transport failure is not a revocation. Keep only the original
            # unextended lease; explicit negative membership expires now.
            if str(exc) in ('QUESTIONER_NOT_ACTIVE','DEMO_MEMBERSHIP_UNVERIFIED','QUERY_IDENTITY_REVOKED'):
                with self.lock:self.entries.pop(user,None)
            raise

    def valid(self,user):
        with self.lock:return self.entries.get(user,0)>time.monotonic()

    def verify(self,user):
        if not self.seconds:return self.check(user)
        now=time.monotonic()
        with self.lock:
            if user not in self.active and len(self.active)>=512:raise BoundaryError('QUERY_IDENTITY_CAPACITY')
            self.active[user]=now
            if self.entries.get(user,0)>now:return
            future=self.pending.get(user)
            if future is None or future.done():
                future=self.pool.submit(copy_context().run,self._refresh,user);self.pending[user]=future
        future.result(timeout=45)
        with self.lock:
            if self.entries.get(user,0)<=time.monotonic():raise BoundaryError('QUERY_IDENTITY_EXPIRED')

    def tick(self):
        if not self.seconds:return
        now=time.monotonic()
        with self.lock:
            for user,used in list(self.active.items()):
                future=self.pending.get(user)
                if now-used>self.seconds*2 and (future is None or future.done()):
                    self.active.pop(user,None);self.entries.pop(user,None);self.pending.pop(user,None)
                elif self.entries.get(user,0)-now<self.seconds/2 and (future is None or future.done()):
                    self.pending[user]=self.pool.submit(self._refresh,user)

    def close(self):self.pool.shutdown(wait=True,cancel_futures=True)


class QuerySnapshots:
    """One in-flight read, immutable versions, successful complete reads only."""
    def __init__(self, ledger, settings=None):
        settings = settings or {}
        self.ledger = ledger
        self.interval = max(15, int(settings.get('refresh_seconds', 60)))
        self.soft_ttl = max(self.interval, int(settings.get('soft_ttl_seconds', 300)))
        self.clock = ledger.clock
        self.lock = threading.RLock()
        self.pool = ThreadPoolExecutor(max_workers=1, thread_name_prefix='am-source')
        self.future = None
        self.versions = {}
        self.latest = None
        self.last_active = 0
        self.last_attempt = 0
        self.error = None
        self.wakeup = lambda: None

    def refresh(self):
        with self.lock:
            if self.future and not self.future.done():
                return self.future
            self.last_attempt = self.clock()
            self.future = self.pool.submit(copy_context().run,self._read)
            self.future.add_done_callback(lambda _: self.wakeup())
            return self.future

    def _read(self):
        started = time.monotonic()
        try:
            with timing.remote('source_refresh'):raw = self.ledger.port.read()
            if not raw or not all(p.get('read_status', 'complete') == 'complete' for p in raw):
                raise BoundaryError('SOURCE_PARTIAL')
            observed = raw[0].get('_sync_observed_at', self.clock())
            revision = raw[0].get('source_revision')
            with self.ledger.mutex:
                rows = self.ledger.catalog.sync(raw, complete=True, revision=revision, read_at=observed)
            rows.sort(key=lambda p: (p.get('business_id') or p['title'], p['key']))
            version = digest([observed, revision, rows])
            snap = {'version': version, 'observed_at': observed, 'source_revision': revision,
                    'source': self.ledger.catalog.namespace, 'range': 'REPLACE_WITH_SHEET_ID!A:G',
                    'complete': True, 'rows': rows, 'read_seconds': time.monotonic()-started}
            with self.lock:
                self.versions[version] = snap
                self.latest = version
                self.error = None
                self.versions = {k: v for k, v in self.versions.items() if observed-v['observed_at'] < 3600}
                self.versions = dict(list(self.versions.items())[-64:])
            return snap
        except Exception:
            with self.lock:
                self.error = 'SOURCE_READ_UNAVAILABLE'
            raise

    def get(self, version=None, *, force=False, wait=True):
        source_check = getattr(self.ledger.port, 'information_source_check', None)
        if source_check is not None:
            source_check()
        with self.lock:
            self.last_active = self.clock()
            if version and version not in self.versions:
                raise BoundaryError('QUERY_SNAPSHOT_EXPIRED')
            old = self.versions.get(version or self.latest)
            future = self.future
            needs_read = force or old is None
            if needs_read and not (future and not future.done()):
                if not force and self.error and self.clock()-self.last_attempt < self.interval:
                    raise BoundaryError('SOURCE_READ_UNAVAILABLE')
                future = self.refresh()
        if needs_read:
            if not wait and not future.done():
                raise SourceLoading('SOURCE_LOADING')
            try:
                old = future.result(timeout=45)
            except Exception:
                if old is None:
                    raise BoundaryError('SOURCE_READ_UNAVAILABLE') from None
        result = copy.deepcopy(old)
        result['cache_hit'] = not needs_read
        result['stale'] = self.clock()-result['observed_at'] > self.soft_ttl
        result['refresh_error'] = self.error
        return result

    def tick(self):
        with self.lock:
            if (self.last_active and self.clock()-self.last_active < self.soft_ttl
                    and self.clock()-self.last_attempt >= self.interval):
                self.refresh()

    def close(self):
        self.pool.shutdown(wait=True, cancel_futures=True)


class QueryMixin:
    def init_query(self, settings=None):
        self.query_settings = copy.deepcopy(settings or {})
        self.query_scope = self.query_settings.get('private_scope', {})
        if set(self.query_scope)-{'demo_projects', 'users'}:
            raise BoundaryError('PRIVATE_QUERY_SCOPE_INVALID')
        scopes = [self.query_scope.get('demo_projects', [])] + list(self.query_scope.get('users', {}).values())
        if any(not isinstance(s, list) or any(not isinstance(x, str) for x in s) for s in scopes):
            raise BoundaryError('PRIVATE_QUERY_SCOPE_INVALID')
        self.snapshots = QuerySnapshots(self.ledger, self.query_settings)
        check=getattr(self.ledger.port,'authorize_private_user',self.ledger.port.authorize_user)
        self.query_identity=QueryIdentityCache(check,self.query_settings.get('identity_cache_seconds',0))
        self.background_enabled = False
        with self.s.lock:
            self.s.conn.executescript('''
            CREATE TABLE IF NOT EXISTS am_query_deliveries(outbox_id TEXT PRIMARY KEY,scope_hash TEXT NOT NULL,denial INTEGER NOT NULL);
            CREATE TABLE IF NOT EXISTS am_source_deliveries(outbox_id TEXT PRIMARY KEY,source_access_version TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS am_query_recent(scope TEXT NOT NULL,project TEXT NOT NULL,opened REAL NOT NULL,expires REAL NOT NULL,PRIMARY KEY(scope,project));
            CREATE TABLE IF NOT EXISTS am_query_wait(event_id TEXT PRIMARY KEY,interaction_id TEXT,force INTEGER NOT NULL);
            CREATE TABLE IF NOT EXISTS am_query_probes(nonce TEXT PRIMARY KEY,actor TEXT NOT NULL,chat TEXT NOT NULL,outbox_id TEXT NOT NULL,message_id TEXT,expires REAL NOT NULL,response TEXT,returned REAL);
            ''')
            # Unbound pre-upgrade replies cannot prove source visibility.
            self.s.conn.execute("INSERT OR IGNORE INTO am_source_deliveries SELECT outbox_id,'legacy-unbound' FROM am_query_deliveries")

    def query_grant(self, row, *, verify=True):
        if (row['app'] != self.g.config.app_id or row['tenant'] != self.g.config.tenant_key
                or row['chat_type'] not in ('p2p','group') or row['expires'] <= self.s.clock()):
            raise BoundaryError('QUERY_IDENTITY_INVALID')
        group=self.query_group(row)
        if row['chat_type']=='group' and not group:raise BoundaryError('QUERY_IDENTITY_INVALID')
        if group and group not in self.ledger.port.disclosure:raise BoundaryError('GROUP_DISCLOSURE_NOT_APPROVED')
        if verify:
            if row['user'] in self.query_settings.get('denied_users',[]):raise BoundaryError('QUERY_IDENTITY_REVOKED')
            # Resolve the port at call time for revocation tests and runtime adapters.
            self.query_identity.check=getattr(self.ledger.port,'authorize_private_user',self.ledger.port.authorize_user)
            if row.get('_direct_return'):
                if not self.query_identity.valid(row['user']):raise BoundaryError('QUERY_IDENTITY_EXPIRED')
            else:
                with timing.remote('identity_policy_check'):self.query_identity.verify(row['user'])
        if group:return sorted(set(self.ledger.port.disclosure[group]))
        if row['user'] == self.owner:
            return ['*']
        scope = sorted(set(self.query_scope.get('demo_projects', []) + self.query_scope.get('users', {}).get(row['user'], [])))
        if not scope:
            raise BoundaryError('PRIVATE_QUERY_SCOPE_MISSING')
        return scope

    def query_request(self, row):
        if row['kind'] in ('group_query','group_open'):return True
        if row['chat_type'] != 'p2p':
            return False
        if row['kind'] == 'menu':
            return row['body'] in ('home', 'projects') or row['user'] != self.owner
        if row['kind'] == 'text':
            q = row['body'].strip()
            if getattr(self,'information',None) is not None and (q == '/info' or q.startswith('/info ')):return True
            followup=field_followup(q) and bool(self.s.rows('SELECT 1 FROM am_sessions WHERE scope=? AND focus IS NOT NULL AND expires>?',(self.scope(row),self.s.clock())) or self.source_meta(row).get('parent'))
            return q in ENTRY_TEXT or q == '/help' or q.startswith('/project ') or deterministic_intent(q) == 'project' or q.isdigit() or followup
        if row['kind'] == 'card':
            body = json.loads(row['body'])
            cards = self.s.rows("SELECT * FROM am_cards WHERE nonce=? AND flow='ux'", (body['value'].get('nonce'),))
            if not cards or cards[0]['actor'] != row['user']:
                return False
            snap = json.loads(cards[0]['snapshot'])
            action = snap.get('actions', {}).get(body['value'].get('action'), {})
            return action.get('page') in READ_PAGES and (action.get('page') != 'long_content' or snap.get('data', {}).get('query_view'))
        return False

    def query_projects(self, row):
        if '_query_grant' not in row:
            with timing.measure(self.s, row.get('id'), 'authorization'):
                row['_query_grant'] = self.query_grant(row)
        version = row.get('_snapshot_version')
        snap = self.snapshots.get(version, force=row.pop('_force_refresh', False), wait=not self.background_enabled)
        row['_snapshot_version'] = snap['version']
        row['_observed_at'] = snap['observed_at']
        row['_snapshot_meta'] = {k: v for k, v in snap.items() if k != 'rows'}
        timing.add(self.s, row.get('id'), 'snapshot_hit' if snap['cache_hit'] else 'source_read', 0 if snap['cache_hit'] else snap['read_seconds'])
        scope = row['_query_grant']
        with timing.measure(self.s,row.get('id'),'local_calculation'):
            return [p for p in snap['rows'] if '*' in scope or set(identifiers(p['title'])).intersection(scope)]

    def query_delivery_authorized(self, row):
        notes = self.s.rows('SELECT * FROM am_query_deliveries WHERE outbox_id=?', (row['id'],))
        if not notes:
            return None
        if row['app'] != self.g.config.app_id or row['tenant'] != self.g.config.tenant_key or row['expires'] <= self.s.clock():
            return False
        origin = timing.origin(self.s, row['id'])
        received = self.s.rows('SELECT * FROM am_inbox WHERE id=?', (origin,))
        group_routes=self.s.rows('SELECT * FROM am_group_delivery WHERE outbox_id=?',(row['id'],))
        if not received or any(received[0][k] != row[k] for k in ('app', 'tenant', 'user')):
            return False
        if not group_routes and received[0]['chat']!=row['chat']:return False
        if group_routes:
            route=group_routes[0]
            if route['source_chat'] not in self.ledger.port.disclosure:return False
            row=dict(row,_query_group=route['source_chat'])
        if notes[0]['denial']:
            return True  # Only fixed, data-free recovery text responding to this actor.
        try:
            bound=self.s.rows('SELECT source_access_version FROM am_source_deliveries WHERE outbox_id=?',(row['id'],))
            host=getattr(self,'information',None)
            version=bound[0]['source_access_version'] if bound else None
            from fs_agent.document_view_binding import scoped_delivery_authorized
            scoped=self.s.rows('SELECT source_fence FROM am_document_source_outputs WHERE outbox_id=?',(row['id'],))
            if scoped:
                if host is None or not bound or not scoped_delivery_authorized(self.g.logic,row,scoped[0]['source_fence']):return False
            elif host is not None and version!=host.store.access_version():return False
            conversation=getattr(self,'conversation',None)
            if conversation is not None and not conversation.delivery_source_authorized(row):return False
            return digest(self.query_grant(row)) == notes[0]['scope_hash']
        except Exception:
            return False

    def query_readback_row(self,row):
        if not self.s.rows('SELECT 1 FROM am_query_deliveries WHERE outbox_id=?',(row['id'],)):return row
        chats=self.s.rows('SELECT chat FROM am_group_private_chats WHERE outbox_id=?',(row['id'],))
        return dict(row,chat=chats[0]['chat'] if chats else row['chat'],_query_readback_guard=lambda:self.query_delivery_authorized(row) is True)

    def query_notice(self, row, code):
        message = ('你的私聊项目查询范围尚未配置，请联系管理员开通。' if code == 'PRIVATE_QUERY_SCOPE_MISSING'
                   else '这项操作仅供跟进管理人使用。你可以从“项目查询”查看已获准的资料。' if code == 'OWNER_ONLY'
                   else '当前无法查看这项资料。请重新打开项目查询；若仍不可用，请联系管理员。')
        row = dict(row, _query_read=False, _query_denial=True)
        row['_interaction']=digest(['query-denial',self.scope(row)])
        row['_view_kind']='recovery'
        row.pop('_view_version', None)
        return self.ux_page(row, '项目查询', message, [('打开项目查询', {'page': 'projects'})], data={'query_view': True})

    def enable_background(self, wakeup=None):
        self.background_enabled = True
        self.wakeup = wakeup or (lambda: None)
        self.s.outbox_wakeup = self.wakeup
        self.snapshots.wakeup = self.wakeup
        self.io_pool = ThreadPoolExecutor(max_workers=2 if getattr(self,'harness',None) is not None and self.harness.durable else 3, thread_name_prefix='am-query')
        self.io_futures = {}
        self.conversation_futures = set()
        self.last_io_status = 0
        self.snapshots.refresh()
        prewarm=getattr(self.ledger.port,'prewarm_directory',None)
        if prewarm:prewarm()

    def pump(self):
        harness=getattr(self,'harness',None)
        harness_work=harness.pump() if harness is not None and harness.durable else False
        if not self.background_enabled:
            return self.step()
        self.snapshots.tick()
        self.query_identity.tick()
        self.conversation_futures={f for f in self.conversation_futures if not f.done()}
        if self.s.clock()-self.last_io_status>=15:
            from fs_agent.provider import atomic_json
            self.last_io_status=self.s.clock()
            atomic_json(self.ledger.root/'query-io-status.json',{'time':self.s.clock(),'port_calls_including_background':dict(getattr(self.ledger.port,'io_counts',{})),
                'identity_seconds':self.query_identity.seconds,'snapshot_ready':self.snapshots.latest is not None,'query_workers':len(self.io_futures),
                'direct_return_enabled':bool(self.query_settings.get('direct_return_enabled'))})
        for key, future in list(self.io_futures.items()):
            if future.done():
                try:future.result()
                except Exception:self.s.set_meta('am_io_error', 'AM_WORKER_FAILED')
                del self.io_futures[key]
        if self.snapshots.future and self.snapshots.future.done():
            self.resume_source_waits()
        rows = self.s.rows("SELECT * FROM am_inbox WHERE state='received' AND expires>? ORDER BY created,rowid LIMIT 32",(self.s.clock(),))
        worked = False
        for row in rows:
            if row['kind']=='conversation' and harness is not None and harness.durable:continue
            key = (row['app'], row['tenant'], row['user'])
            if len(self.io_futures)>=3:break
            if key in self.io_futures:continue
            if row['kind']=='conversation' and len(self.conversation_futures)>=2:continue
            requires_model = self.training_requires_model(row)
            # The owner cell and this claim share one original Store transaction.
            # Already-claimed conversations recheck before handling in step().
            if not self.s.execute("UPDATE am_inbox SET state='processing' WHERE id=? AND state='received' AND (?=0 OR NOT EXISTS(SELECT 1 FROM meta WHERE key='resource_window_owner' AND value!=''))",(row['id'],int(requires_model))):continue
            self.io_futures[key] = self.io_pool.submit(self.step, row, claimed=True)
            if row['kind']=='conversation':self.conversation_futures.add(self.io_futures[key])
            self.io_futures[key].add_done_callback(lambda _: self.wakeup())
            worked = True
        if not rows and not self.io_futures:
            self.io_futures[(self.g.config.app_id,self.g.config.tenant_key,self.owner)] = self.io_pool.submit(self.step,maintenance=True)
        return worked or harness_work

    def close_query(self):
        if getattr(self,'harness',None) is not None:self.harness.close()
        if self.background_enabled:self.io_pool.shutdown(wait=True, cancel_futures=True)
        self.snapshots.close()
        self.query_identity.close()

    def resume_source_waits(self):
        # If the complete result won the race, cancel an unsent loading view.
        # Sending/unknown views stay bound and are never duplicated.
        with self.s.tx() as c:
            c.execute("""UPDATE outbox SET state='rejected',error='LOADING_COALESCED'
                WHERE state='pending' AND id IN (SELECT v.pending_outbox FROM am_query_wait w
                JOIN am_interactions v ON v.id=w.interaction_id JOIN am_view_outputs o ON o.outbox_id=v.pending_outbox
                WHERE json_extract(o.metrics,'$.task_state')='loading')""")
        self.s.execute("""UPDATE am_inbox SET state='received' WHERE state='source_wait' AND expires>?
            AND NOT EXISTS(SELECT 1 FROM am_query_wait w JOIN am_interactions v ON v.id=w.interaction_id
            JOIN outbox o ON o.id=v.pending_outbox WHERE w.event_id=am_inbox.id AND o.state IN ('pending','sending','unknown'))""",(self.s.clock(),))

    def prepare_query_probe(self,row):
        """Owner-only, content-free protocol experiment, separate from navigation."""
        from fs_agent.card_ui import card,paragraph,button,bind_rendered_card
        if row['user']!=self.owner:raise BoundaryError('OWNER_ONLY')
        nonce=uuid.uuid4().hex
        value=bind_rendered_card(card('回调协议验证',[paragraph('仅验证当前卡片能否通过按钮回调直接更新；不含项目资料。'),button('验证直接更新',{'flow':'am_query_probe','nonce':nonce,'version':1})]))
        with self.s.tx() as c:
            out=self.notify(row,card=value,conn=c)
            c.execute('INSERT INTO am_query_probes VALUES(?,?,?,?,NULL,?,NULL,NULL)',(nonce,row['user'],row['chat'],out,self.s.clock()+600))

    def query_probe_response(self,payload):
        from fs_agent.card_ui import card,paragraph,bind_rendered_card
        try:
            h,e=payload['header'],payload['event'];value=e['action']['value']
            if value.get('flow')!='am_query_probe':return None
            denied={'toast':{'type':'info','content':'此验证已结束或不属于当前会话'}}
            if h['app_id']!=self.g.config.app_id or h['tenant_key']!=self.g.config.tenant_key:return denied
            with self.s.tx() as c:
                r=c.execute('SELECT * FROM am_query_probes WHERE nonce=?',(value.get('nonce'),)).fetchone()
                if not r or r['actor']!=e['operator']['open_id'] or r['actor']!=self.owner or r['chat']!=e['context']['open_chat_id'] or r['message_id']!=e['context']['open_message_id'] or r['expires']<=self.s.clock() or value.get('version')!=1:return denied
                if r['response']:return json.loads(r['response'])
                response={'card':{'type':'raw','data':bind_rendered_card(card('回调直接更新完成',[paragraph('此页由按钮回调直接返回。平台是否已应用，将独立回读核对。')]))}}
                c.execute('UPDATE am_query_probes SET response=?,returned=? WHERE nonce=?',(json.dumps(response),self.s.clock(),r['nonce']))
                return response
        except (KeyError,TypeError,ValueError):return None
