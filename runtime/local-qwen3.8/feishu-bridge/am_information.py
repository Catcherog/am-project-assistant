"""Information sync embedded in the original AM service.

The host owns identity, delivery, ModelCoordinator and lifecycle. This module
only reads approved sources, maintains local evidence and answers read queries.
"""
from __future__ import annotations

from copy import deepcopy
from dataclasses import replace
from datetime import datetime
import hashlib
import json
import os
from pathlib import Path
import shutil
import threading
import time
from zoneinfo import ZoneInfo

from fs_agent.config import BoundaryError
from fs_agent.provider import atomic_json
from am_info_sync.models import Source, Observation, Page, Principal, canonical, digest, ScopeViolation
from am_info_sync.store import Store
from am_info_sync.catalog import EntityCatalog
from am_info_sync.extraction import ChunkedExtractor, ExtractionError
from info_extraction import BoundedCoordinatorExtractor
from am_info_sync.runtime import InformationService
from am_info_sync.adapters.messages import MessageAdapter
from am_info_sync.adapters.docx import DocxAdapter
from info_readers import ExistingSDKReader, ExistingMeegleReader, ExistingTasksReader


LEDGER_FIELDS = ('关键日期', '待办', '项目进展', '人员与职责')


class SharedLedgerReader:
    """One serialized read publishes both the original view and evidence.

    A ledger cell remains a ledger record. It is never interpreted as project
    acceptance, approved plan change, professional review or node completion.
    """
    def __init__(self, host):
        self.host = host
        self.original = host.am.ledger.port.read
        self.lock = threading.RLock()
        self.rows = None
        self.checked = 0

    def read(self, *, force=False):
        with self.lock:
            if self.rows is not None and not force and time.time() - self.checked < 60:
                self.host.require_ledger_available()
                return deepcopy(self.rows)
            h = self.host
            try:
                raw = self.original()
                if not raw or any(p.get('read_status', 'complete') != 'complete' for p in raw):
                    raise BoundaryError('SOURCE_PARTIAL')
                revisions = {p.get('source_revision') for p in raw}
                if len(revisions) != 1 or None in revisions:
                    raise BoundaryError('SOURCE_REVISION_UNVERIFIED')
                checked = time.time()
                with h.am.ledger.mutex:
                    rows = h.am.ledger.catalog.sync(raw, complete=True,
                        revision=raw[0]['source_revision'], read_at=checked)
                keys = sorted(p['key'] for p in rows)
                old = h.store.get_source('demo-ledger')
                source = replace(old, settings=dict(old.settings, subject_ids=keys))
                h.store.register_source(source)
                observations = []
                for p in rows:
                    record = {'title': p['title'], 'identity_status': p.get('identity_status'),
                              'fields': {k: p.get(k, '') for k in LEDGER_FIELDS}}
                    payload = {'record': record, 'subject_id': p['key'], 'quote': p['title'],
                               'revision_id': int(p['source_revision']),
                               'time_basis': 'observed_at', 'scope': 'fixed_demo_ledger'}
                    version = 'ledger:' + str(p['source_revision']) + ':' + digest(record)
                    observations.append(Observation(source.source_id, p['key'], version,
                        int(checked * 1000), text=p['title'] + '\n' + '\n'.join(
                            k + '：' + p.get(k, '') for k in LEDGER_FIELDS),
                        payload=payload, url=h.ledger_url))
                    h.catalog.register(p['key'], p['title'], aliases=tuple(p.get('aliases', ())))
                run = h.store.begin_sync(source.source_id, 'incremental')
                h.store.save_page(run['run_id'], run['lease_token'], run['cursor'],
                                  Page(tuple(observations), time_coverage=False))
                # Make deterministic fields queryable before publishing this view.
                for obs in observations:
                    persisted = h.store.get_observation(obs.source_id, obs.object_id, obs.version)
                    if persisted is not None:
                        h.service.facts.process(persisted, [], extractor_version='demo-ledger-fields-v1')
                for p in rows:
                    p['_sync_observed_at'] = checked
                self.rows, self.checked = deepcopy(rows), checked
                return deepcopy(rows)
            except Exception as exc:
                code = type(exc).__name__
                from am_info_sync.adapters.http import _PERMISSION_CODES, _AUTH_CODES
                denied = str(exc).startswith('PLATFORM_READ_') and str(exc).removeprefix('PLATFORM_READ_') in {
                    str(v) for v in _PERMISSION_CODES | _AUTH_CODES}
                h.store.mark_source_error('demo-ledger', code,
                    permission_denied=denied)
                raise


class AMInformation:
    def __init__(self, bridge, client, settings):
        if settings.get('enabled') is not True or bridge.am_self is None or bridge.poc is None:
            raise BoundaryError('INFORMATION_HOST_UNAVAILABLE')
        self.bridge, self.am, self.settings = bridge, bridge.am_self, deepcopy(settings)
        self.root = bridge.root / 'information'
        if self.root.is_symlink():
            raise BoundaryError('INFORMATION_DIRECTORY_INVALID')
        self.root.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.root.chmod(0o700)
        self.store = Store(self.root / 'information.sqlite3')
        self.catalog = EntityCatalog(self.store)
        self.start_ms = int(settings['start_ms'])
        self.stop = threading.Event()
        self.threads = []
        self.last_maintenance = 0
        self.last_backup = 0
        self.last_ledger_attempt = 0
        self.last_status = 0
        self.capture_result = None
        self.registered = set()
        cfg = self.am.g.config
        self.reader = ExistingSDKReader(client, app_id=cfg.app_id, tenant_id=cfg.tenant_key)
        adapters = {'im_chat': MessageAdapter(self.reader), 'im_thread': MessageAdapter(self.reader),
                    'docx': DocxAdapter(self.reader), 'task_snapshot': ExistingTasksReader(self.reader)}
        business = getattr(getattr(self.am.g, 'logic', None), 'business', None)
        if business is None:
            from fs_agent.meegle_port import load_meegle_port
            business = load_meegle_port(self.am.g)
        self.business = business
        if business is not None and hasattr(business, '_state'):
            adapters['project_snapshot'] = ExistingMeegleReader(business, self.am.owner_origin)
        self.bounded_extractor = BoundedCoordinatorExtractor(self.am.g.provider,
            self.catalog.context_for, may_run=lambda:not self.interactive_busy())
        self.extractor = ChunkedExtractor(self.bounded_extractor, chunk_chars=900,
                                         overlap_chars=100, max_chunks=32)
        self.service = InformationService(self.store, adapters, self._extract)
        self.service.extraction_worker.prefer_source_kinds=('im_chat','im_thread','manual')
        self.capture = self.service.bridge(cfg.app_id, cfg.tenant_key)
        from group_ledger import TOKEN, SHEET, SOURCE
        self.ledger_url = SOURCE + '?sheet=' + SHEET
        if settings.get('ledger') is True:
            self._register(Source('demo-ledger', 'ledger_snapshot', TOKEN + '/' + SHEET,
                cfg.tenant_key, self.am.owner, readers=('group:demo-query',),
                output_chats=tuple(self.am.ledger.port.disclosure), enabled=True,
                start_ms=self.start_ms, poll_seconds=180,
                settings={'host_managed': True, 'subject_ids': [], 'fact_mappings': {
                    'ledger_record': {'subject_path': ['subject_id'], 'value_path': ['record'],
                                      'quote_path': ['quote']}}}), preserve_subjects=True)
            self.ledger = SharedLedgerReader(self)
            self.am.ledger.port.read = self.ledger.read
            self.am.ledger.port.information_source_check = self.require_ledger_available
        else:
            self.ledger = None
        # Only the exact pre-existing business targets. Disclosure remains owner-only.
        for alias in settings.get('meegle_aliases', ()):
            if business is None or alias not in business.settings['targets']:
                raise BoundaryError('INFORMATION_TARGET_NOT_APPROVED')
            target = business.settings['targets'][alias]
            sid = 'meegle:' + target['deliverable']
            subject = 'deliverable:' + target['deliverable']
            self._register(Source(sid, 'project_snapshot', target['deliverable'], cfg.tenant_key,
                self.am.owner, enabled=True, start_ms=self.start_ms, poll_seconds=300,
                settings={'subject_id': subject, 'target_alias': alias, 'fact_mappings': {
                    'review': {'value_path': ['record', 'professional_review'], 'quote_path': ['review_quote']},
                    'node': {'value_path': ['record', 'node_status'], 'quote_path': ['node_quote']},
                    'plan': {'value_path': ['record', 'node_plan'], 'quote_path': ['plan_quote']},
                    'progress': {'value_path': ['record', 'deliverable_status'], 'quote_path': ['status_quote']}}}))
            self.catalog.register(subject, alias, kind='deliverable')
            self.store.open_gap(sid, target['deliverable'], 'coverage_limit',
                'Only registered deliverable and node fields; other nodes/comments and attachment bodies are not covered')
        # Explicit additional sources; never derive an ACL from a document or group link.
        for value in settings.get('sources', ()):
            source = Source.from_dict(value)
            if source.tenant_id != cfg.tenant_key or source.owner_user_id != self.am.owner:
                raise BoundaryError('INFORMATION_SOURCE_IDENTITY_INVALID')
            self._register(source)
        for old in self.store.list_sources():
            if old.source_id in self.registered:
                continue
            if old.source_id.startswith('owner-am:') and settings.get('owner_am_channel') is True:
                continue
            if old.settings.get('managed_by')=='owner_work_discovery' and settings.get('discovery') is True:
                continue
            if old.enabled:
                self.store.register_source(replace(old, enabled=False))
        with self.store.transaction() as db:
            db.execute('''CREATE TABLE IF NOT EXISTS host_evaluations(
                id TEXT PRIMARY KEY, source_id TEXT NOT NULL, object_id TEXT NOT NULL,
                version TEXT NOT NULL, state TEXT NOT NULL DEFAULT 'pending',
                result TEXT, created_ms INTEGER NOT NULL, finished_ms INTEGER)''')
            db.execute("UPDATE host_evaluations SET state='interrupted' WHERE state='running'")
            db.execute('''CREATE TABLE IF NOT EXISTS host_query_probes(
                id TEXT PRIMARY KEY, subject_id TEXT NOT NULL, state TEXT NOT NULL DEFAULT 'pending',
                result TEXT, created_ms INTEGER NOT NULL, finished_ms INTEGER)''')
        self.discovery=None
        if settings.get('discovery') is True:
            from info_sources import Discovery, PinnedUserReader
            from sheets_write_port import CliProtectedReader
            reader=bridge.config['am_self_metadata']
            self.discovery=Discovery(self,PinnedUserReader(CliProtectedReader(reader['binary'],reader['sha256'],reader['identity'])))

    def _query_probe_once(self):
        with self.store._lock:
            row = self.store.conn.execute("SELECT * FROM host_query_probes WHERE state='pending' LIMIT 1").fetchone()
        if row is None:
            return
        started = time.monotonic()
        try:
            origin = self.am.owner_origin()
            if origin is None:
                raise BoundaryError('OWNER_ORIGIN_UNAVAILABLE')
            snap = self.am.snapshots.get(wait=True)
            origin['_snapshot_version'] = snap['version']
            result = self.query(origin, row['subject_id'])
            local = self.service.query(row['subject_id'], Principal(self.am.owner, self.am.g.config.tenant_key))
            same = canonical(result['facts']) == canonical(local['facts'])
            result = {'same_facts':same, 'fact_count':len(result['facts']),
                      'version':result['version'], 'seconds':round(time.monotonic()-started,3),
                      'identity':'existing_AM_owner_resolver','external_sends':0}
            state = 'done' if same else 'failed'
        except Exception as exc:
            result = {'error_type':type(exc).__name__}
            state = 'failed'
        with self.store.transaction() as db:
            db.execute('UPDATE host_query_probes SET state=?,result=?,finished_ms=? WHERE id=?',
                       (state,canonical(result),self.store.clock(),row['id']))

    def require_ledger_available(self):
        source = self.store.get_source('demo-ledger')
        if not source.enabled or self.store.source_status('demo-ledger')['availability'] == 'denied':
            raise BoundaryError('INFORMATION_SOURCE_ACCESS_REVOKED')

    def _register(self, source, *, preserve_subjects=False):
        self.registered.add(source.source_id)
        try:
            old = self.store.get_source(source.source_id)
            source = replace(source, start_ms=old.start_ms)
            if preserve_subjects:
                source = replace(source, settings=dict(source.settings,
                                 subject_ids=old.settings.get('subject_ids', [])))
        except ScopeViolation:
            pass
        self.store.register_source(source)

    def _extract(self, observation):
        # MODEL_BUSY/PAUSED means defer, never consume a finite failure budget.
        if self.interactive_busy():
            raise ExtractionError('host_model_deferred')
        try:
            return self.extractor(observation)
        except BoundaryError as exc:
            if str(exc) in ('MODEL_BUSY', 'MODEL_PAUSED', 'STOP_UNKNOWN'):
                raise ExtractionError('host_model_deferred') from None
            raise

    def interactive_busy(self):
        if self.am.s.meta('provider_pause') or self.am.s.meta('resource_window_owner'):
            return True
        provider = self.am.g.provider
        if getattr(provider, 'lock', None) is not None and provider.lock.locked():
            return True
        if self.am.s.rows("SELECT 1 FROM am_inbox WHERE state IN ('received','processing','source_wait') LIMIT 1"):
            return True
        if self.am.s.rows("SELECT 1 FROM tasks WHERE state IN ('queued','running') LIMIT 1"):
            return True
        with self.bridge.mutex:
            if self.bridge.db.execute("SELECT 1 FROM inbox WHERE state IN ('queued','processing') LIMIT 1").fetchone():
                return True
        return False

    def capture_verified(self, payload):
        # Registration of the owner's AM channel is an explicit deployment
        # choice; it never opens peer-to-peer conversations with colleagues.
        header, event = payload.get('header', {}), payload.get('event', {})
        cfg = self.am.g.config
        if header.get('app_id') != cfg.app_id or header.get('tenant_key') != cfg.tenant_key:
            return {'accepted': False, 'reason': 'wrong_host_identity'}
        chat_id=event.get('message',event).get('chat_id')
        if any(s.enabled and s.kind=='im_chat' and s.remote_id==chat_id and
               s.settings.get('read_identity')=='existing_user' for s in self.store.list_sources()):
            return {'accepted':False,'reason':'user_identity_poll_source'}
        message, sender = event.get('message', {}), event.get('sender', {})
        if (self.settings.get('owner_am_channel') is True and
                header.get('event_type') == 'im.message.receive_v1' and
                message.get('chat_type') == 'p2p' and sender.get('sender_type') == 'user' and
                sender.get('sender_id', {}).get('open_id') == self.am.owner and
                sender.get('tenant_key', cfg.tenant_key) == cfg.tenant_key):
            chat = message.get('chat_id')
            if chat:
                sid = 'owner-am:' + hashlib.sha256(chat.encode()).hexdigest()[:16]
                subjects = self.store.get_source('demo-ledger').settings.get('subject_ids', []) if self.ledger else []
                self._register(Source(sid, 'im_chat', chat, cfg.tenant_key, self.am.owner,
                    enabled=True, start_ms=self.start_ms, poll_seconds=180,
                    settings={'subject_ids': subjects, 'review_known_messages': True,
                              'read_identity': 'original_bot', 'retain_derived': True,
                              'allowed_sender_ids': [self.am.owner]}))
        result = self.capture.accept_verified(payload)
        self.capture_result = {'time': time.time(), 'accepted': result.get('accepted', False),
                               'duplicate': result.get('duplicate', False), 'reason': result.get('reason')}
        return result

    def dependency_current(self, ctx, previous):
        """Read current LOCAL source records, never refresh or relax the global guard."""
        from am_outcome.types import fingerprint
        from am_info_sync.policy import can_read
        principal=Principal(ctx.actor_id,ctx.tenant_id,('demo-query',),None if ctx.is_owner else ctx.chat_id)
        current=[]
        for old in previous.get('objects',[]):
            row=dict(old,content_hash=None)
            try:
                source=self.store.get_source(old['source_id'])
                status=self.store.source_status(source.source_id)
                if not can_read(source,principal) or status.get('availability')=='denied':
                    row['permission_revoked']=True
                observation=self.store.get_observation(old['source_id'],old['object_id'])
                if observation is None:continue
                row['deleted']=observation.deleted;row['content_version']=observation.version
                fields=observation.payload.get('record',{}).get('fields',{})
                if old.get('locator') in fields:row['content_hash']=fingerprint(fields[old['locator']])
                # A local Source policy is not proof of the platform's inherited ACL.
                # Keep access_revision absent unless the adapter actually supplied it.
                row['access_revision']=observation.payload.get('access_revision')
                current.append(row)
            except (KeyError,ValueError):continue
        return {'objects':current,'query_set':{'predicate':previous.get('query_set',{}).get('predicate'),'watermark':None,'pagination_complete':False}}

    def evidence_bundle(self, row, project_code):
        from fs_agent.project_catalog import business_ids
        from am_outcome.evidence import EvidenceBundle
        from am_info_sync.document_snapshot import (capture_document_snapshot,
            request_scope, DocumentSnapshotError)
        # A new v2 document captures its own current, scoped authority and
        # evidence. An epoch copied from an earlier ordinary query is not its
        # authority; ordinary query/follow-up delivery keeps the global fence.
        version = self.store.access_version()
        # Authenticate before reading project scope. Do not reuse an old scope
        # supplied by a prior turn or inferred by the model.
        row['_query_grant'] = self.am.query_grant(row)
        projects=self.am.query_projects(row)
        selected=[p for p in projects if p['key']==project_code or project_code in business_ids(p['title'])]
        if len(selected)!=1:raise BoundaryError('DOCUMENT_PROJECT_AMBIGUOUS')
        project=selected[0]
        output = self.am.query_group(row) if row['chat_type'] == 'group' else None
        principal = Principal(row['user'], row['tenant'], ('demo-query',), output)
        try:
            scope = request_scope(row)
            info, binding = capture_document_snapshot(self.store, self.service, project,
                principal=principal, scope=scope, project_code=project_code,
                expires=row['expires'], expected_access_version=version)
        except DocumentSnapshotError as exc:
            raise BoundaryError(str(exc)) from exc
        bundle=EvidenceBundle.build(project,scope=scope,expires=binding['expires'],
            information=info,document_binding=binding)
        return project,bundle

    def authorize_document_evidence(self, row):
        """Original identity check at an interactive boundary, outside DB locks."""
        normal = dict(row)
        normal.pop('_direct_return', None)
        self.am.query_grant(normal)
        return True

    def document_evidence_current(self, row, binding):
        """Check current local evidence without refreshing identity or sources.

        This guard also runs inside the document store's commit fence. The
        approved host already has a 600-second QueryIdentityCache: use its
        original direct-return path, including local denies and expiry. A
        missing/expired lease fails closed; never invent a lease for strict
        zero-cache installations. A normal authorized read must happen outside
        the commit fence before retrying. No remote/model call occurs here.
        """
        from fs_agent.project_catalog import business_ids
        from query_rules import identifiers
        from am_info_sync.document_snapshot import (validate_binding, LEGACY_SCHEMA,
            capture_document_snapshot, request_scope, DocumentSnapshotError)
        try:
            validate_binding(binding, row, self.store.clock())
        except DocumentSnapshotError as exc:
            raise BoundaryError(str(exc)) from exc
        if binding['schema'] == LEGACY_SCHEMA and self.store.access_version() != binding['access_version']:
            raise BoundaryError('SOURCE_ACCESS_CHANGED')
        current_row = dict(row, _direct_return=True)
        grant = self.am.query_grant(current_row)
        # Do not call snapshots.get/query_projects here: both can initiate or
        # await platform I/O. "latest" means latest successfully read locally.
        snapshots = self.am.snapshots
        with snapshots.lock:
            snap = deepcopy(snapshots.versions.get(snapshots.latest))
        if not snap or snap.get('complete') is not True:
            raise BoundaryError('DOCUMENT_LOCAL_SNAPSHOT_UNAVAILABLE')
        projects = [p for p in snap['rows'] if '*' in grant or
                    set(identifiers(p['title'])).intersection(grant)]
        selected = [p for p in projects if p['key'] == binding['project_code'] or
                    binding['project_code'] in business_ids(p['title'])]
        if len(selected) != 1:
            raise BoundaryError('DOCUMENT_PROJECT_AMBIGUOUS')
        project = selected[0]
        output = self.am.query_group(current_row) if row['chat_type'] == 'group' else None
        principal = Principal(row['user'], row['tenant'], ('demo-query',), output)
        try:
            _, current = capture_document_snapshot(self.store, self.service, project,
                principal=principal, scope=request_scope(row), project_code=binding['project_code'],
                expires=min(row['expires'], binding['expires']),
                expected_access_version=binding['access_version'],binding_schema=binding['schema'])
        except DocumentSnapshotError as exc:
            raise BoundaryError(str(exc)) from exc
        if binding['schema'] != LEGACY_SCHEMA and (
                current.get('component_hashes',{}).get('authorization') !=
                binding.get('component_hashes',{}).get('authorization')):
            raise BoundaryError('SOURCE_ACCESS_CHANGED')
        if project['key'] != binding['project_id'] or current['state_hash'] != binding['state_hash']:
            raise BoundaryError('DOCUMENT_EVIDENCE_CHANGED')
        return True

    def query(self, row, subject_id):
        version=self.store.access_version()
        if row.setdefault('_source_access_version',version)!=version:
            raise BoundaryError('SOURCE_ACCESS_CHANGED')
        # Identity and project scope come from the existing resolver. Never
        # accept a Principal, department, user or output chat from model text.
        scope = self.am.query_grant(row)
        projects = self.am.query_projects(row)
        matches = [p for p in projects if p['key'] == subject_id]
        if not matches:
            # Explicit deliverable sources are owner-only and private-only.
            if row['user'] != self.am.owner or row['chat_type'] != 'p2p' or not any(
                    s.settings.get('subject_id') == subject_id for s in self.store.list_sources()):
                raise BoundaryError('INFORMATION_PROJECT_NOT_AVAILABLE')
        output = self.am.query_group(row) if row['chat_type'] == 'group' else None
        principal = Principal(row['user'], row['tenant'], ('demo-query',), output)
        from am_info_sync.document_snapshot import projection_version
        with self.store.transaction() as db:
            if self.store.access_version()!=version:raise BoundaryError('SOURCE_ACCESS_CHANGED')
            result = self.service.query(subject_id, principal, connection=db)
            result['version'] = projection_version(result)
        return result

    def respond(self, row):
        query = row['body'].strip()[5:].strip()
        if query in ('任务','审批','来源','文档','待核对'):
            self.am.query_grant(row)
            if row['user']!=self.am.owner or row['chat_type']!='p2p':
                raise BoundaryError('OWNER_ONLY')
            if query=='待核对':
                from info_semantics import pending_reviews
                version=self.store.access_version()
                if row.setdefault('_source_access_version',version)!=version:
                    raise BoundaryError('SOURCE_ACCESS_CHANGED')
                review=pending_reviews(self.store,Principal(row['user'],row['tenant']))
                if review['source_access_version']!=version or self.store.access_version()!=version:
                    raise BoundaryError('SOURCE_ACCESS_CHANGED')
                if review['raw_expires_at_ms'] is not None:
                    row['expires']=min(row['expires'],review['raw_expires_at_ms']/1000)
                lines=['资料待核对：'+str(review['total'])+'项。以下记录尚未成为可执行事项。']
                for index,item in enumerate(review['items'],1):
                    lines.append(str(index)+'. '+item['reason'])
                    lines.append('当前原文片段：'+item['text'] if item['raw_available']
                                 else '原文已过保留期或不可用，请重新提供资料。')
                    if item['url']:lines.append('来源：'+item['url'])
                if review['total']>len(review['items']):
                    lines.append('本条展示前30项，另有'+str(review['total']-len(review['items']))+'项待核对。')
                if not review['total']:lines.append('当前没有已登记的提取缺口；这不表示业务信息已经完整或准确。')
                return '\n'.join(lines)
            sources=self.store.list_sources()
            if query=='来源':
                counts={}
                names={'ledger_snapshot':'DEMO台账','im_chat':'聊天','im_thread':'话题','docx':'在线文档',
                       'file_snapshot':'云端文件','tasks_collection':'本人相关任务','approval_snapshot':'审批',
                       'workitem_snapshot':'项目、需求及实际交付物','sheet_snapshot':'在线表格','bitable_snapshot':'多维表格'}
                states={'ok':'读取正常','unknown':'待首次读取','denied':'访问不可用','error':'读取失败'}
                for source in sources:
                    if source.enabled:
                        state=self.store.source_status(source.source_id)['availability']
                        key=names.get(source.kind,source.kind)+'／'+states.get(state,state);counts[key]=counts.get(key,0)+1
                return '已登记信息源：\n'+'\n'.join(k+'：'+str(v) for k,v in sorted(counts.items()))+'\n仅表示已登记范围，未读取内容和解析积压仍需核对。'
            if query=='文档':
                selected=[s for s in sources if s.kind in ('docx','file_snapshot','sheet_snapshot','bitable_snapshot') and s.enabled]
                lines=['已登记云文档及文件：'+str(len(selected))]
                for source in selected[:30]:
                    state=self.store.source_status(source.source_id)
                    lines.append(source.settings.get('file_name','在线文档')+'｜'+('已读取，内容完整度另行核对' if state['availability']=='ok' else '待读取或需检查权限'))
                if len(selected)>30:lines.append('此处展示前 30 项，其余仍在同步。')
                return '\n'.join(lines)
            selected=[s for s in sources if s.kind==('tasks_collection' if query=='任务' else 'approval_snapshot') and s.enabled]
            lines=[query+'当前记录（来源状态与业务验收分别保留）：']
            for source in selected:
                subject=source.settings.get('subject_id',source.source_id)
                result=self.service.query(subject,Principal(row['user'],row['tenant']))
                for f in result['facts']:
                    r=f['value'];title=r.get('summary') or r.get('approval_name') or r.get('serial_number') or f['evidence']['object_id']
                    lines.append(str(title)+'｜源头状态：'+str(r.get('status',r.get('instance_status','待核'))))
            if len(lines)==1:lines.append('尚无已处理记录；不能据此判断没有任务或审批。')
            return '\n'.join(lines[:80]+(['另有 '+str(len(lines)-80)+' 条记录未在本条展开。'] if len(lines)>80 else []))
        projects = self.am.query_projects(row)
        if not query:
            results = [self.query(row, p['key']) for p in projects]
            visible = {s['source_id']: s for r in results for s in r['coverage']['sources']}
            checked = [s['last_checked_ms'] for s in visible.values() if s['last_checked_ms']]
            when = datetime.fromtimestamp(max(checked)/1000, ZoneInfo('Asia/Shanghai')).strftime('%Y.%m.%d %H:%M:%S') if checked else '尚未成功读取'
            return ('信息同步：当前可见 ' + str(len(visible)) + ' 个来源，' + str(len(projects)) + ' 个台账项目。\n'
                    '最近核对：' + when + '\n待分析或需复核：' + str(sum(
                        bool(s['unprocessed_jobs'] or s['stale'] or s['gap_kinds']) for s in visible.values())) +
                    '\n输入 /info 项目编号或名称 查看事实和来源。覆盖范围以已登记来源为限。')
        from group_ledger import select_projects
        matches = select_projects(query, projects, None)
        if len(matches) != 1:
            return '请提供唯一项目编号或名称，以核对同一组事实。'
        p = matches[0]
        result = self.query(row, p['key'])
        lines = [p['title'], '信息版本：' + result['version'][:12]]
        visible_facts=result['facts'][:12]
        for f in visible_facts:
            if f['field'] == 'ledger_record':
                # Existing presentation handles the full original record.
                value = f['value']['fields'].get('项目进展', '')
                lines.append('台账记录：' + value)
            elif f['field']=='system_record':
                value=f['value'];basic=value.get('basic') or {}
                title=value.get('title') or value.get('summary') or basic.get('name') or '系统记录'
                status=value.get('status',basic.get('status','已同步字段'))
                lines.append(str(title)+'｜源头状态：'+canonical(status))
            else:
                value = f['value'] if isinstance(f['value'], str) else canonical(f['value'])
                lines.append(('预测：' if f['assertion_type'] == 'forecast' else '来源反馈：') + value)
        if not result['facts']:
            lines.append('尚无已处理事实。')
        if len(result['facts'])>len(visible_facts):
            lines.append('另有 '+str(len(result['facts'])-len(visible_facts))+' 条已同步记录，保留在对应来源中。')
        if result['coverage']['processing_pending']:
            lines.append('有新资料等待分析。')
        if result['coverage']['needs_refresh_or_review']:
            lines.append('部分来源需刷新或复核，不能据此声称信息齐全。')
        lines.append('来源：' + self.ledger_url)
        return '\n'.join(lines)

    def _evaluate_once(self):
        if self.interactive_busy():
            return
        with self.store.transaction() as db:
            row = db.execute("SELECT * FROM host_evaluations WHERE state='pending' ORDER BY created_ms LIMIT 1").fetchone()
            if row is None:
                return
            row = dict(row)
            db.execute("UPDATE host_evaluations SET state='running' WHERE id=?", (row['id'],))
        started = time.monotonic()
        try:
            obs = self.store.get_observation(row['source_id'], row['object_id'], row['version'])
            if obs is None:
                raise ValueError('evaluation_evidence_expired')
            candidates = self._extract(obs)
            result = {'status': 'passed_schema_and_quotes', 'candidate_count': len(candidates),
                      'fields': sorted({v['field'] for v in candidates}),
                      'types': sorted({v['assertion_type'] for v in candidates}),
                      'seconds': round(time.monotonic()-started, 3),
                      'candidates': candidates, 'semantic_acceptance': 'requires_review',
                      'last_model_call':self.bounded_extractor.last_call}
            state = 'done'
        except Exception as exc:
            result = {'status': 'failed', 'code': getattr(exc, 'code', type(exc).__name__),
                      'seconds': round(time.monotonic()-started, 3)}
            state = 'pending' if result['code'] == 'host_model_deferred' else 'failed'
        with self.store.transaction() as db:
            db.execute('UPDATE host_evaluations SET state=?,result=?,finished_ms=? WHERE id=?',
                       (state, canonical(result), self.store.clock(), row['id']))

    def _maintenance(self):
        self.service.maintenance()
        now = self.store.clock()
        with self.store.transaction() as db:
            db.execute('DELETE FROM host_evaluations WHERE created_ms < ?', (now-86400000,))
            db.execute('DELETE FROM host_query_probes WHERE created_ms < ?', (now-86400000,))
        backups = self.root / 'backups'
        backups.mkdir(mode=0o700, exist_ok=True)
        # Sanitized backups contain no source text, quotes, evaluation results
        # or private raw inbox. Derived facts remain governed by source policy.
        if time.time() - self.last_backup > 3600:
            path = backups / (str(now) + '.sqlite3')
            self.store.backup(path)
            import sqlite3
            db = sqlite3.connect(path)
            db.execute('PRAGMA secure_delete=ON')
            db.execute("UPDATE inbox SET payload_json='{}',purged=1")
            db.execute("UPDATE observations SET text='',payload_json='{}',purged=1")
            db.execute("UPDATE fact_candidates SET quote=''")
            db.execute('DELETE FROM host_evaluations')
            db.execute('DELETE FROM host_query_probes')
            db.commit(); db.execute('VACUUM'); db.close()
            self.last_backup = time.time()
        for path in backups.glob('*.sqlite3'):
            if time.time() - path.stat().st_mtime > 86400:
                path.unlink()
        self.last_maintenance = time.time()

    def write_status(self):
        diagnostic = self.service.diagnostics()
        sources = diagnostic.get('sources', [])
        status = {'time': time.time(), 'enabled': True, 'pid': os.getpid(),
            'unique_original_consumer': True, 'model': 'existing_ModelCoordinator',
            'external_actions': 0, 'capture': self.capture_result,
            'workers': diagnostic['workers'], 'source_count': len(self.store.list_sources()),
            'sources': [{k: s.get(k) for k in ('source_id','availability','last_checked_ms','last_error')} for s in sources],
            'jobs': diagnostic.get('jobs'), 'disk_free_bytes': shutil.disk_usage(self.root).free}
        atomic_json(self.root/'status.json', status)
        self.last_status = time.time()

    def start(self):
        def collect():
            while not self.stop.is_set() and not self.bridge.stop.is_set():
                try:
                    self.service.collect_once(run_sync=False)
                    self._query_probe_once()
                    if self.ledger and time.time()-self.last_ledger_attempt >= 180:
                        self.last_ledger_attempt = time.time()
                        try:
                            self.ledger.read(force=True)
                        except Exception as exc:
                            self.service.pulse('ledger', 'error', type(exc).__name__)
                        else:
                            self.service.pulse('ledger', 'idle')
                    if time.time()-self.last_maintenance >= 60:
                        self._maintenance()
                    if time.time()-self.last_status >= 5:
                        self.write_status()
                except Exception as exc:
                    self.service.pulse('collector', 'error', type(exc).__name__)
                self.stop.wait(1)
            self.service.pulse('collector', 'stopped')

        def analyze():
            while not self.stop.is_set() and not self.bridge.stop.is_set():
                try:
                    for _ in range(10):
                        if self.interactive_busy() or self.service.analyze_once() is None:break
                    if not self.interactive_busy():
                        self._evaluate_once()
                except Exception as exc:
                    self.service.pulse('analysis', 'error', type(exc).__name__)
                self.stop.wait(1)
            self.service.pulse('analysis', 'stopped')

        def discover():
            while not self.stop.is_set() and not self.bridge.stop.is_set():
                try:self.discovery.step()
                except Exception as exc:self.service.pulse('discovery','error',type(exc).__name__)
                self.stop.wait(5)

        def synchronize(name):
            while not self.stop.is_set() and not self.bridge.stop.is_set():
                try:
                    self.service.pulse(name,'working')
                    self.service.sync_worker.run_once()
                    self.service.pulse(name,'idle')
                except Exception as exc:self.service.pulse(name,'error',type(exc).__name__)
                self.stop.wait(1)
            self.service.pulse(name,'stopped')

        for name, target in (('am-info-collector', collect), ('am-info-analysis', analyze)):
            thread = threading.Thread(name=name, target=target, daemon=True)
            thread.start(); self.threads.append(thread)
        for name in ('am-info-sync-1','am-info-sync-2'):
            thread=threading.Thread(name=name,target=synchronize,args=(name,),daemon=True)
            thread.start();self.threads.append(thread)
        if self.discovery is not None:
            thread=threading.Thread(name='am-info-discovery',target=discover,daemon=True)
            thread.start();self.threads.append(thread)

    def close(self):
        self.stop.set()
        for thread in self.threads:
            thread.join(timeout=2)
        if not any(t.is_alive() for t in self.threads):
            self.write_status()
            self.store.close()
            return True
        return False
