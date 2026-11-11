"""Concrete Studio factory for the existing Bridge, SDK, policy and read ports.

Construction performs no network requests and starts no consumer. The returned
bundle is composed by the R4 service. User-identity reads are a distinct optional
binding; a bot token is never used to discover/read colleague private chats.
"""
from __future__ import annotations

from dataclasses import dataclass, replace
import json
from pathlib import Path
import time
from urllib.parse import urlparse

from am_gateway.collectors import SourceSpec, JsonCommandTransport
from am_gateway.discovery import DiscoveryEngine, DiscoverySpec, JsonDiscoveryAdapter
from am_gateway.feishu_snapshot import FeishuMessageSnapshotReader
from am_gateway.models import Config, ContractError, Event, dumps, key
from am_gateway.phase1 import HarnessConfig, Policy
from am_gateway.store import Store
from integration.phase1_runtime import Phase1Ports
from integration.am_multi_ai_host import normalize_message
from integration.studio_host import (ApprovedGroupDiscovery, BindingError, HostScope,
    ScopedReadTransport, StudioSender, cutover_for, legacy_control)
from integration.phase1_collection import (HarnessCollector, LegacyOwnership,
    Phase1CollectionService, SnapshotPageAdapter)


class PersonalReadTransport:
    """Bind a host-owned user connector to explicitly approved personal chats."""
    def __init__(self, transport, *, tenant_id, owner_id, identity, chats):
        if not identity or identity in {'am_bot', 'bot'} or not callable(transport):
            raise BindingError('distinct_user_read_identity_required')
        self.transport, self.tenant_id = transport, tenant_id
        self.owner_id, self.identity, self.chats = owner_id, identity, frozenset(chats)
        self.threads = set()

    def __call__(self, url, params, identity):
        parsed = urlparse(url)
        if (parsed.scheme, parsed.netloc) != ('https', 'open.feishu.cn') or identity != self.identity:
            raise BindingError('personal_read_origin_or_identity')
        if parsed.path != '/open-apis/im/v1/messages':
            raise BindingError('personal_read_list_endpoint_required')
        kind, cid = params.get('container_id_type'), params.get('container_id')
        if not (kind == 'chat' and cid in self.chats or kind == 'thread' and cid in self.threads):
            raise BindingError('personal_read_container_outside_scope')
        value = self.transport(url, params, identity)
        self._check_items(value)
        return value

    def _check_items(self, value):
        if isinstance(value, dict) and value.get('code') == 0:
            for item in value.get('data', {}).get('items', []):
                if item.get('chat_id') not in self.chats:
                    raise BindingError('personal_read_chat_mismatch')
                sender = item.get('sender', {})
                if sender.get('tenant_key', self.tenant_id) != self.tenant_id:
                    raise BindingError('personal_read_tenant_mismatch')
                if item.get('thread_id'):
                    self.threads.add(item['thread_id'])

    def read_message(self, source, message_id):
        if (source.identity != self.identity or source.channel != 'personal_dm'
                or source.tenant_id != self.tenant_id or source.effective_chat_id not in self.chats):
            raise BindingError('personal_snapshot_outside_scope')
        if source.container_type == 'thread' and source.native_id not in self.threads:
            raise BindingError('personal_thread_unregistered')
        item = FeishuMessageSnapshotReader(self.transport).read_message(source, message_id)
        self._check_items({'code': 0, 'data': {'items': [item]}})
        if item.get('chat_id') != source.effective_chat_id:
            raise BindingError('personal_snapshot_chat_mismatch')
        return item


class IdentityReaders:
    def __init__(self, readers):
        self.readers = dict(readers)

    def read_message(self, source, message_id):
        reader = self.readers.get(source.identity)
        if reader is None:
            raise BindingError('source_identity_not_bound')
        return reader.read_message(source, message_id)


class PersonalSnapshotPages(SnapshotPageAdapter):
    def _event(self, source, message):
        event = super()._event(source, message)
        if event.sender_id == self.config.owner_id:
            return replace(event, is_self=True, direction='outbound')
        return event


class ApprovedPersonalDiscovery:
    def __init__(self, transport, allowed_chats, identity, projects_for_chat):
        self.adapter = JsonDiscoveryAdapter(transport)
        self.allowed_chats, self.identity = frozenset(allowed_chats), identity
        self.projects_for_chat = projects_for_chat

    def discover_page(self, spec, page_token):
        if spec.scope != 'personal_dm' or spec.identity != self.identity:
            raise BindingError('personal_inventory_scope_mismatch')
        page = self.adapter.discover_page(spec, page_token)
        for source in page.sources:
            if source.channel != 'personal_dm' or source.container_type != 'chat':
                raise BindingError('personal_inventory_source_mismatch')
        accepted = tuple(replace(source, adapter='personal_messages',
                         project_ids=self.projects_for_chat(source.effective_chat_id))
                         for source in page.sources if source.effective_chat_id in self.allowed_chats)
        # Completeness is relative to the supplied connector's inventory and the
        # explicit approved set; filtered-out sources do not expand authorization.
        return replace(page, sources=accepted)


class HostAuthority:
    def __init__(self, bridge, config, scope, settings, collection_config, directory=None):
        self.bridge, self.config, self.scope = bridge, config, scope
        self.settings, self.collection_config = settings, collection_config
        self.harness = None
        self.personal_chats = frozenset(settings.get('personal_chat_ids', ()))
        self.group_chats = frozenset(settings.get('group_chat_ids', ()))
        self.directory = directory

    def projects_for_chat(self, chat):
        return tuple(project for project, chats in self.collection_config.project_chats.items() if chat in chats)

    def source_allowed(self, source):
        if source.tenant_id != self.config.tenant_id:
            return False
        chat = source.effective_chat_id
        if source.channel == 'personal_dm':
            return (source.identity == self.settings.get('personal_identity')
                    and chat in self.personal_chats and self.settings.get('personal_read_enabled') is True)
        if source.identity != 'am_bot':
            return False
        if source.channel == 'bot_dm':
            if self.directory is not None:
                member = self.directory.dm_actor(chat)
                if member:
                    try:
                        self.directory.resolve(member)
                        return True
                    except Exception:
                        return False
            origin = self.bridge.am_self.owner_origin()
            return bool(chat == self.config.owner_chat_id and origin and origin.get('chat') == chat)
        return (source.channel == 'group' and chat in self.group_chats
                and chat in self.bridge.am_self.ledger.port.disclosure)

    def __call__(self, event):
        if event.tenant_id != self.config.tenant_id or event.original_time < self.collection_config.live_cutover_at:
            return None
        if event.is_self or event.direction != 'inbound':
            return None
        principal = self.config.owner_id if event.channel == 'personal_dm' else event.sender_id
        if event.channel == 'personal_dm':
            if (event.chat_id not in self.personal_chats
                    or self.settings.get('personal_read_enabled') is not True
                    or event.sender_id == self.config.owner_id):
                return None
            # Passive assistance is sent only to the owner's bot private chat.
            if self.settings.get('personal_assistance_enabled') is not True:
                return None
            conversation = {'actor_id': self.config.owner_id, 'chat_id': self.config.owner_chat_id,
                            'channel': 'bot_dm'}
        else:
            if (event.channel == 'bot_dm' and event.chat_id != self.config.owner_chat_id
                    and (self.directory is None or self.directory.dm_actor(event.chat_id) != principal)):
                return None  # Additional DM targets require a separately verified inventory.
            conversation = {'actor_id': principal, 'chat_id': event.chat_id, 'channel': event.channel}
        try:
            self.scope.authorize(conversation)
        except Exception:
            return None
        # Reuse only checks made within this authority invocation. Several
        # readable projects can share the same group; repeating the full SDK
        # membership check per project adds latency without a new boundary.
        # A later source/read/delivery invocation always checks again.
        checked_groups = {event.chat_id} if event.channel == 'group' else set()
        target = self.config.owner_chat_id if event.channel == 'personal_dm' else event.chat_id
        allowed_groups = tuple(c for c in self.group_chats if c in self.bridge.am_self.ledger.port.disclosure)
        if principal == self.config.owner_id and target == self.config.owner_chat_id:
            chats = tuple(sorted({event.chat_id, self.config.owner_chat_id, *allowed_groups,
                                  *(self.directory.dm_chats() if self.directory else ()),
                                  *(self.personal_chats if self.settings.get('personal_read_enabled') else ())}))
            projects = tuple(sorted(self.collection_config.project_chats))
        else:
            chats, projects = (event.chat_id,), self.projects_for_chat(event.chat_id)
            if self.directory is not None:
                try:
                    visible = self.directory.project_ids_for(self.directory.resolve(principal))
                except Exception:
                    return None
                if event.channel == 'bot_dm':
                    # The verified private-chat actor's current directory grant
                    # is the authority. Missing/ambiguous subject extraction
                    # cannot erase that grant before semantic understanding.
                    # Names and message-supplied IDs grant nothing here.
                    projects = tuple(sorted(visible))
                else:
                    group_scope = set(self.projects_for_chat(event.chat_id))
                    projects = tuple(p for p in (event.project_ids or projects)
                                     if p in visible and p in group_scope)
                readable = []
                for project in projects:
                    for chat in self.collection_config.project_chats.get(project, ()):
                        if chat in allowed_groups:
                            try:
                                if chat not in checked_groups:
                                    self.scope.authorize({'actor_id': principal, 'chat_id': chat, 'channel': 'group'})
                                    checked_groups.add(chat)
                                readable.append(chat)
                            except Exception:
                                pass
                chats = tuple(sorted({event.chat_id, *readable}))
        return Policy(principal, 'studio-existing-policy:' + key(principal, target)[:20],
                      target, chats, projects, allow_reply=True,
                      allow_supplement=principal == self.config.owner_id,
                      version=int(self.settings.get('policy_version', 1)))

    def refresh(self):
        if self.harness is None:
            return []
        updated = []
        rows = self.harness.store.rows('''SELECT t.task_id,t.policy_json,i.event_json
            FROM h1_tasks t JOIN gw_items i USING(item_id)
            WHERE t.execution_state!='cancelled'
            AND (t.delivery_state!='delivered' OR t.verification_state!='sufficient')''')
        for row in rows:
            old = Policy.parse(row['policy_json'])
            if old.revoked:
                continue
            current = self(Event.from_dict(json.loads(row['event_json'])))
            if current is None:
                revised = replace(old, revoked=True, allow_reply=False, allow_supplement=False,
                                  version=old.version + 1)
            else:
                revised = replace(old, source_chats=tuple(c for c in old.source_chats if c in current.source_chats),
                                  projects=tuple(p for p in old.projects if p in current.projects),
                                  allow_reply=old.allow_reply and current.allow_reply,
                                  allow_supplement=old.allow_supplement and current.allow_supplement)
                if revised == old:
                    continue
                revised = replace(revised, version=max(old.version + 1, current.version))
            self.harness.update_policy(row['task_id'], revised)
            updated.append(row['task_id'])
        return updated

    def project_for_owner(self, limit=100):
        """Project configured sources for the owner's verified private audience.

        The ordinary source projection retains its original audience. This
        additional view is authorized by the host's existing disclosure set and
        explicit personal-read setting; it never republishes into another group.
        """
        if self.harness is None:
            return 0
        h = self.harness
        with h.store.transaction() as db:
            db.execute('CREATE TABLE IF NOT EXISTS r4_owner_projection(item_id TEXT PRIMARY KEY,version INTEGER NOT NULL)')
            rows = db.execute('''SELECT i.* FROM gw_items i LEFT JOIN r4_owner_projection p USING(item_id)
                WHERE p.version IS NULL OR p.version<i.version ORDER BY i.received_at LIMIT ?''', (limit,)).fetchall()
            count = 0
            for row in rows:
                event = Event.from_dict(json.loads(row['event_json']))
                source = SourceSpec(event.tenant_id, event.chat_id, channel=event.channel,
                    identity=self.settings.get('personal_identity', '') if event.channel == 'personal_dm' else 'am_bot')
                if self.source_allowed(source) and not event.is_deleted and not event.is_self and event.body.strip():
                    for project in self.projects_for_chat(event.chat_id):
                        h.information.append(project=project, nature='feedback', body=event.body,
                            sources=[{'item_id': row['item_id'], 'version': row['version']}],
                            readers=[self.config.owner_id], audiences=[self.config.owner_chat_id],
                            operation_id=key('studio_owner_projection', row['item_id'], row['version'], project), db=db)
                        count += 1
                db.execute('INSERT OR REPLACE INTO r4_owner_projection VALUES(?,?)', (row['item_id'], row['version']))
        return count


class Phase1StudioSender:
    """Bind stable Harness effects to the existing SDK sender and exact readback."""
    def __init__(self, client, scope, path, authority, readers, *, send_live=False):
        self.existing = StudioSender(client, scope, path, send_live=send_live)
        self.authority, self.readers, self.enabled = authority, readers, send_live
        self.store = Store(path)
        self.scope = scope

    def _record_timing(self, phase, object_id, started):
        detail = {'phase': phase, 'seconds': time.monotonic() - started,
                  'clock_domain': 'host_monotonic', 'phone_visibility_measured': False}
        try:
            with self.store.transaction() as db:
                self.store.audit(db, 'h1_sdk_timing', object_id, dumps(detail))
        except Exception:
            pass  # Diagnostics never change a known send/readback outcome.

    def _effect(self, effect_id):
        row = self.store.one('''SELECT e.*,t.policy_json,i.event_json
            FROM h1_effects e JOIN h1_tasks t USING(task_id) JOIN gw_items i USING(item_id)
            WHERE e.effect_id=?''', (effect_id,))
        if row is None:
            raise BindingError('recorded_effect_required')
        return row

    def __call__(self, *, target, body, idempotency_key):
        if not self.enabled:
            raise BindingError('send_live_disabled')
        row = self._effect(idempotency_key)
        if (target, body) != (row['target'], row['body']):
            raise BindingError('effect_payload_mismatch')
        event, policy = Event.from_dict(json.loads(row['event_json'])), Policy.parse(row['policy_json'])
        current = self.authority(event)
        if (current is None or policy.revoked or not current.allow_reply
                or current.reply_target != target
                or not set(policy.source_chats) <= set(current.source_chats)
                or not set(policy.projects) <= set(current.projects)):
            raise BindingError('delivery_authorization_changed')
        # Context/source checks are repeated by Harness immediately before this
        # port. The host's identity/disclosure check runs again in StudioSender.
        target_info = {'chat_id': target, 'actor_id': policy.principal,
                       'channel': 'bot_dm' if event.channel == 'bot_dm' or target == self.scope.config.owner_chat_id else 'group',
                       'thread_id': event.thread_id if target == event.chat_id else '',
                       'reply_to_message_id': event.native_id if target == event.chat_id else ''}
        action = {'action_id': idempotency_key, 'action_type': 'text', 'body': body,
                  'payload_json': '{}', 'target_json': dumps(target_info)}
        began = time.monotonic()
        try:
            receipt = self.existing.send(action)
        finally:
            self._record_timing('send_call', idempotency_key, began)
        if receipt.status != 'sent' or not receipt.platform_id:
            return {'status': receipt.status, 'reason': receipt.detail}
        return {'platform_id': receipt.platform_id}

    def readback(self, task_id):
        row = self.store.one('SELECT * FROM h1_effects WHERE task_id=?', (task_id,))
        if not row or not row['platform_id']:
            raise BindingError('exact_platform_mapping_required')
        cfg = self.scope.config
        is_dm = row['target'] == cfg.owner_chat_id or (self.authority.directory is not None
                    and self.authority.directory.dm_actor(row['target']) is not None)
        source = SourceSpec(cfg.tenant_id, row['target'], identity='am_bot',
                            channel='bot_dm' if is_dm else 'group')
        began = time.monotonic()
        try:
            message = self.readers.read_message(source, row['platform_id'])
        finally:
            self._record_timing('independent_readback', task_id, began)
        if not self.scope.verified_snapshot(source, message):
            raise BindingError('readback_source_rejected')
        sender = message.get('sender', {})
        if message.get('deleted') is not False or message.get('msg_type') != 'text' or sender.get('id') != cfg.bot_id:
            raise BindingError('readback_author_or_type_mismatch')
        content = json.loads(message['body']['content'])
        if content.get('text') != row['body']:
            raise BindingError('readback_body_mismatch')
        return {'platform_id': message['message_id'], 'target': message['chat_id'],
                'body': content['text'], 'sender_id': cfg.bot_id,
                'platform_time': int(message['create_time']) / 1000}


@dataclass
class ProductionBundle:
    ports: Phase1Ports
    collection_factory: object
    sender: object
    readback: object
    authority: HostAuthority
    readers: IdentityReaders
    legacy_ownership: object = None

    def bind(self, ingress):
        self.authority.harness = ingress.gateway
        self.legacy_ownership = LegacyOwnership(ingress.gateway)
        return self.collection_factory(ingress)


def build_phase1_bundle(bridge, host_config, client, root, *, capabilities=None,
                       adapters=None, classifier=None, personal_transport=None,
                       personal_inventory=None, legacy_control_port=None, assistant_identity_resolver=None):
    settings = host_config.get('phase1_gateway', host_config.get('multi_ai_gateway', {}))
    if bridge.poc is None or bridge.am_self is None:
        raise BindingError('existing_host_services_required')
    origin = bridge.am_self.owner_origin()
    if not origin or not str(origin.get('chat', '')).startswith('oc_'):
        raise BindingError('verified_owner_bot_dm_required')
    chats = tuple(settings.get('group_chat_ids', ()))
    if any(chat not in bridge.am_self.ledger.port.disclosure for chat in chats):
        raise BindingError('group_outside_existing_disclosure')
    root = Path(root).resolve()
    db_path = root / settings.get('database_name', 'gateway-r4.sqlite3')
    if db_path.resolve().parent != root:
        raise BindingError('database_must_be_in_runtime_root')
    cfg_args = dict(tenant_id=bridge.poc.config.tenant_key, bot_id=bridge.poc.config.bot_open_id,
                    owner_id=bridge.am_self.owner, owner_chat_id=origin['chat'])
    if capabilities is not None:
        cfg_args['capabilities'] = capabilities
    for name in ('queue_limit', 'dispatch_deadline_seconds', 'execution_deadline_seconds',
                 'classification_budget_seconds', 'context_bytes', 'delta_limit'):
        if name in settings:
            cfg_args[name] = settings[name]
    cfg = HarnessConfig(**cfg_args)
    cfg.validate()
    proposed_cutover = settings.get('live_cutover_at')
    if proposed_cutover is None:
        # First installation uses the present instant. Persist it atomically so
        # restarts and concurrent constructions cannot turn history into tasks.
        cutover_store = Store(db_path)
        with cutover_store.transaction() as db:
            db.execute("INSERT OR IGNORE INTO gw_meta VALUES('live_cutover_at',?)", (str(time.time()),))
    cutover = cutover_for(db_path, proposed_cutover)
    collection_config = Config(cfg.tenant_id, cfg.bot_id, cfg.owner_id, cfg.owner_chat_id, cutover,
        project_chats={k: tuple(v) for k, v in settings.get('project_chats', {}).items()})
    allowed_source_chats = {cfg.owner_chat_id, *chats, *settings.get('personal_chat_ids', ())}
    if any(chat not in allowed_source_chats for values in collection_config.project_chats.values() for chat in values):
        raise BindingError('project_source_outside_approved_scope')
    directory = None
    if settings.get('assistant', {}).get('enabled') is True:
        from integration.assistant_identity import StudioAssistantDirectory, DEMOScope
        directory = StudioAssistantDirectory(bridge, client, cfg,
            {**settings, **settings['assistant']}, Store(db_path), resolver=assistant_identity_resolver)
        scope = DEMOScope(bridge, cfg, chats, directory)
    else:
        scope = HostScope(bridge, cfg, chats)
    authority = HostAuthority(bridge, cfg, scope, settings, collection_config, directory)
    bot_read = ScopedReadTransport(client, [cfg.owner_chat_id, *chats, *(directory.dm_chats() if directory else ())],
        app_id=bridge.config['app_id'], bot_id=cfg.bot_id, tenant_id=cfg.tenant_id)
    readers = {'am_bot': bot_read}
    personal = None
    if settings.get('personal_read_enabled') is True:
        if personal_transport is None and settings.get('personal_read_argv'):
            command = JsonCommandTransport(settings['personal_read_argv'],
                                            timeout=settings.get('read_timeout_seconds', 30))
            personal_transport = lambda url, params, identity: command({
                'operation': 'feishu_read', 'url': url, 'params': params, 'identity': identity})
        personal = PersonalReadTransport(personal_transport, tenant_id=cfg.tenant_id,
            owner_id=cfg.owner_id, identity=settings.get('personal_identity', ''),
            chats=settings.get('personal_chat_ids', ()))
        readers[personal.identity] = personal
        if personal_inventory is None and settings.get('personal_inventory_argv'):
            personal_inventory = JsonCommandTransport(settings['personal_inventory_argv'])
    mux = IdentityReaders(readers)

    def verify_snapshot(source, raw):
        if not authority.source_allowed(source):
            return False
        if source.channel != 'personal_dm':
            return scope.verified_snapshot(source, raw)
        sender = raw.get('sender', {})
        return bool(personal and raw.get('chat_id') == source.effective_chat_id
                    and sender.get('id_type') == 'open_id' and sender.get('id')
                    and sender.get('tenant_key', cfg.tenant_id) == cfg.tenant_id
                    and sender.get('sender_type') == 'user')

    legacy = legacy_control_port or (lambda payload: legacy_control(bridge, cfg, payload))

    def projects_for(payload):
        return authority.projects_for_chat(payload['event']['message']['chat_id'])

    def authenticated_actor(payload):
        header, event = payload.get('header', {}), payload.get('event', payload)
        if header and (header.get('app_id', bridge.config['app_id']) != bridge.config['app_id']
                       or header.get('tenant_key', cfg.tenant_id) != cfg.tenant_id):
            raise BindingError('callback_identity_mismatch')
        actor = event.get('operator', {}).get('open_id')
        if not actor or not bridge.user_allowed(actor):
            raise BindingError('verified_callback_actor_required')
        return actor

    def normalize_recall(payload):
        if authority.harness is None:
            raise BindingError('runtime_not_bound')
        header, event = payload.get('header', {}), payload.get('event', payload)
        if (header.get('app_id') != bridge.config['app_id'] or header.get('tenant_key') != cfg.tenant_id):
            raise BindingError('verified_recall_header_required')
        mid = event.get('message_id') or event.get('message', {}).get('message_id')
        row = authority.harness.store.one('SELECT event_json FROM gw_items WHERE tenant_id=? AND native_id=?',
                                          (cfg.tenant_id, mid))
        if row is None and authority.harness.store.one("SELECT 1 FROM sqlite_master WHERE type='table' AND name='r5_intake'"):
            row = authority.harness.store.one('SELECT event_json FROM r5_intake WHERE item_id=?',
                                              (key(cfg.tenant_id, 'message', mid),))
        if row is None:
            raise BindingError('recall_source_not_known')
        original = Event.from_dict(json.loads(row['event_json']))
        return replace(original, is_deleted=True, observed_via='recall',
                       version_time=max(original.version_time, authority.harness.store.clock()))

    ports = Phase1Ports(cfg, str(db_path), scope.verified, authority, projects_for, legacy,
                       classifier=classifier, adapters=adapters, authenticated_actor=authenticated_actor,
                       normalize_recall=normalize_recall, snapshot_reader=mux,
                       verified_snapshot_scope=verify_snapshot)
    sender = Phase1StudioSender(client, scope, db_path, authority, mux,
                                send_live=settings.get('send_live') is True)
    # Persist the explicit first cutover for stable history/live separation.
    with sender.store.transaction() as db:
        db.execute("INSERT OR IGNORE INTO gw_meta VALUES('live_cutover_at',?)", (str(cutover),))

    def legacy_event(event):
        if event.channel == 'personal_dm':
            return False
        payload = {'header': {'app_id': bridge.config['app_id'], 'tenant_key': cfg.tenant_id,
                             'event_type': 'im.message.receive_v1'},
                   'event': {'sender': {'sender_id': {'open_id': event.sender_id}},
                     'message': {'message_id': event.native_id, 'chat_id': event.chat_id,
                         'chat_type': 'p2p' if event.channel == 'bot_dm' else 'group',
                         'message_type': 'text', 'content': dumps({'text': event.body}),
                         'create_time': str(int(event.original_time * 1000)), 'thread_id': event.thread_id,
                         'parent_id': event.parent_id, 'root_id': event.root_id}}}
        return legacy(payload)

    def factory(ingress):
        authority.harness = ingress.gateway
        page_adapters = {'feishu_messages': SnapshotPageAdapter(bot_read, cfg, verify_snapshot)}
        if personal is not None:
            page_adapters['personal_messages'] = PersonalSnapshotPages(personal, cfg, verify_snapshot)
        engine = HarnessCollector(ingress.gateway, collection_config, page_adapters,
            authorization_for=authority, legacy_owned=legacy_event, source_allowed=authority.source_allowed,
            notify=lambda: ingress.scheduler.notify() if ingress.scheduler else None)
        for chat in sorted({cfg.owner_chat_id, *chats, *(directory.dm_chats() if directory else ())}):
            engine.register_source(SourceSpec(cfg.tenant_id, chat, identity='am_bot',
                channel='bot_dm' if chat == cfg.owner_chat_id or (directory and directory.dm_actor(chat)) else 'group',
                project_ids=authority.projects_for_chat(chat),
                poll_seconds=settings.get('poll_seconds', 60)))
        if personal is not None:
            for chat in personal.chats:
                engine.register_source(SourceSpec(cfg.tenant_id, chat, identity=personal.identity,
                    adapter='personal_messages', channel='personal_dm',
                    project_ids=authority.projects_for_chat(chat),
                    poll_seconds=settings.get('personal_poll_seconds', 60)))
        # Restore registered thread identities only for this binding's chats.
        for row in ingress.gateway.store.rows('SELECT spec_json FROM gw_sources WHERE active=1'):
            source = SourceSpec.from_dict(json.loads(row['spec_json']))
            reader = readers.get(source.identity)
            if source.container_type == 'thread' and reader and source.effective_chat_id in reader.chats:
                reader.threads.add(source.native_id)
        discoveries = {'approved_groups': ApprovedGroupDiscovery(bot_read, chats, collection_config.project_chats)}
        if personal is not None and personal_inventory is not None:
            discoveries['personal_inventory'] = ApprovedPersonalDiscovery(personal_inventory,
                personal.chats, personal.identity, authority.projects_for_chat)
        discovery = DiscoveryEngine(engine, discoveries)
        if chats:
            discovery.register(DiscoverySpec(cfg.tenant_id, 'approved_groups', 'approved_groups',
                identity='am_bot', scope='approved_joined_groups'))
        if personal is not None and personal_inventory is not None:
            discovery.register(DiscoverySpec(cfg.tenant_id, 'approved_personal_inventory', 'personal_inventory',
                identity=personal.identity, scope='personal_dm'))
        return Phase1CollectionService(engine, discovery, ingress=ingress,
            refresh_interval=settings.get('refresh_interval', 300),
            refresh_messages=settings.get('refresh_messages', 4), authorization_refresh=authority.refresh,
            owner_projection=authority.project_for_owner)

    return ProductionBundle(ports, factory, sender, sender.readback, authority, mux)
