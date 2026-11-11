"""Verified DEMO identities and bot-DM bindings for the R5 Studio host.

Construction and receive-event verification do not call the SDK. Resolution is
an explicit background operation; the cache-only accessor never waits for an
in-flight remote lookup. Incoming text cannot supply department or owner roles.
"""
from __future__ import annotations

from dataclasses import replace
import threading
import time

from am_gateway.assistant.policy import Actor
from am_gateway.models import ContractError, Event
from integration.studio_host import BindingError, HostScope


def _ids(value, name):
    if isinstance(value, (str, bytes)) or value is None:
        raise BindingError(name + '_must_be_ids')
    values = tuple(value)
    if any(not isinstance(item, str) or not item for item in values):
        raise BindingError(name + '_must_be_ids')
    return frozenset(values)


class StudioAssistantDirectory:
    cache_seconds = 600.0

    def __init__(self, bridge, client, cfg, settings, store, *, clock=time.time, resolver=None):
        self.bridge, self.client, self.config = bridge, client, cfg
        self.settings, self.store, self.clock = dict(settings), store, clock
        if resolver is not None and not callable(resolver):
            raise BindingError('trusted_identity_resolver_not_callable')
        self.resolver = resolver
        self.demo_department_ids = _ids(settings.get('demo_department_ids', ()), 'demo_department_ids')
        self.leader_user_ids = _ids(settings.get('leader_user_ids', ()), 'leader_user_ids')
        self.protected_person_ids = _ids(settings.get('protected_person_ids', ()), 'protected_person_ids')
        # Some installed apps can read status/department IDs but not names.
        # These host-owned, source-backed labels affect display only. The live
        # SDK and existing membership guard still establish all permissions.
        self.display_name_bindings = settings.get('display_name_bindings', {})
        if not isinstance(self.display_name_bindings, dict):
            raise BindingError('assistant_display_name_bindings_invalid')
        for uid, binding in self.display_name_bindings.items():
            if (not isinstance(uid, str) or not uid.startswith('ou_')
                    or not isinstance(binding, dict)
                    or any(not isinstance(binding.get(field), str) or not binding[field].strip()
                           for field in ('display_name', 'source_ref', 'verified_at'))):
                raise BindingError('assistant_display_name_binding_source_required')
        self._cache = {}
        self._lookup_locks = {}
        self._lock = threading.Lock()
        with self.store.transaction() as db:
            db.execute('''CREATE TABLE IF NOT EXISTS r5_dm_bindings(
                chat_id TEXT PRIMARY KEY,tenant_id TEXT NOT NULL,user_id TEXT NOT NULL,
                created_source_ref TEXT NOT NULL,created_at REAL NOT NULL,last_seen REAL NOT NULL)''')
            db.execute('CREATE INDEX IF NOT EXISTS r5_dm_user ON r5_dm_bindings(tenant_id,user_id)')
            existing = db.execute('SELECT user_id,tenant_id FROM r5_dm_bindings WHERE chat_id=?',
                                  (cfg.owner_chat_id,)).fetchone()
            if existing and (existing['user_id'] != cfg.owner_id or existing['tenant_id'] != cfg.tenant_id):
                raise BindingError('assistant_owner_dm_binding_conflict')

    def _owner(self):
        return Actor(self.config.owner_id, self.settings.get('owner_display_name', '示例负责人'),
                     (), True, is_owner=True)

    def _locally_allowed(self, user_id):
        try:
            return (bool(self.bridge.user_allowed(user_id))
                    and user_id not in self.bridge.am_self.query_settings.get('denied_users', ()))
        except (AttributeError, TypeError):
            return False

    def cached_actor(self, user_id):
        """Cache only: no SDK call or waiting on a remote-lookup lock."""
        if not isinstance(user_id, str) or not user_id:
            return None
        if user_id == self.config.owner_id:
            return self._owner()
        if not self._locally_allowed(user_id):
            with self._lock:
                self._cache.pop(user_id, None)
            return None
        with self._lock:
            entry = self._cache.get(user_id)
            if entry is None or entry[1] <= self.clock():
                return None
            return entry[0]

    def invalidate(self, user_id):
        with self._lock:
            self._cache.pop(user_id, None)

    def _read_actor(self, user_id):
        try:
            checked = self.bridge.am_self.query_identity.verify(user_id)
            if checked is False:
                raise BindingError('assistant_existing_identity_rejected')
        except BindingError:
            raise
        except Exception as exc:
            raise BindingError('assistant_existing_identity_rejected') from exc
        if self.resolver is not None:
            try:
                actor = self.resolver(user_id)
            except Exception as exc:
                raise BindingError('assistant_directory_lookup_failed') from exc
            if not isinstance(actor, Actor) or not actor.verified or actor.user_id != user_id:
                raise BindingError('assistant_directory_identity_mismatch')
        else:
            from lark_oapi.api.contact.v3 import GetUserRequest
            request = (GetUserRequest.builder().user_id(user_id).user_id_type('open_id')
                       .department_id_type('open_department_id').build())
            try:
                response = self.client.contact.v3.user.get(request)
                if not response.success() or not response.data or not response.data.user:
                    raise BindingError('assistant_directory_profile_unavailable')
                user = response.data.user
                if user.open_id != user_id:
                    raise BindingError('assistant_directory_identity_mismatch')
                status = user.status
                if (not status or status.is_activated is not True
                        or any(getattr(status, field, False)
                               for field in ('is_resigned', 'is_frozen', 'is_exited'))):
                    raise BindingError('assistant_directory_user_inactive')
                departments = _ids(user.department_ids or (), 'profile_department_ids')
                actor = Actor(user_id, user.name or '', tuple(sorted(departments)), True)
            except BindingError:
                raise
            except Exception as exc:
                raise BindingError('assistant_directory_lookup_failed') from exc
        # Roles are from host config, not a display name or a caller-supplied flag.
        actor = replace(actor, is_owner=False,
                        is_leader=user_id in (self.leader_user_ids | self.protected_person_ids))
        if not (set(actor.department_ids) & self.demo_department_ids) and not actor.is_leader:
            raise BindingError('assistant_actor_outside_demo')
        if not actor.display_name.strip():
            label = self.display_name_bindings.get(user_id, {}).get('display_name')
            actor = replace(actor, display_name=label or '姓名待核（' + user_id + '）')
        return actor

    def resolve(self, user_id):
        """Resolve in a background lane, preserving the original host policy."""
        if not isinstance(user_id, str) or not user_id:
            raise BindingError('assistant_user_id_required')
        if user_id == self.config.owner_id:
            return self._owner()
        if not self._locally_allowed(user_id):
            self.invalidate(user_id)
            raise BindingError('assistant_actor_not_allowed')
        cached = self.cached_actor(user_id)
        if cached is not None:
            return cached
        with self._lock:
            lookup_lock = self._lookup_locks.setdefault(user_id, threading.Lock())
        with lookup_lock:
            cached = self.cached_actor(user_id)
            if cached is not None:
                return cached
            started_at = self.clock()
            try:
                actor = self._read_actor(user_id)
                if not self._locally_allowed(user_id):
                    raise BindingError('assistant_actor_not_allowed')
                # A slow lookup does not extend the identity window indefinitely.
                expires_at = started_at + self.cache_seconds
                if expires_at <= self.clock():
                    raise BindingError('assistant_identity_expired_during_lookup')
                with self._lock:
                    self._cache[user_id] = (actor, expires_at)
                return actor
            except Exception:
                self.invalidate(user_id)
                raise

    def project_ids_for(self, actor):
        """Background visibility resolution, never department-wide disclosure."""
        if not isinstance(actor, Actor) or not actor.verified:
            raise BindingError('assistant_verified_actor_required')
        actual = self.resolve(actor.user_id)
        configured = self.settings.get('project_chats', {})
        user_projects = self.settings.get('user_project_ids', {})
        if not isinstance(configured, dict) or not isinstance(user_projects, dict):
            raise BindingError('assistant_project_mapping_invalid')
        projects = set(_ids(user_projects.get(actual.user_id, ()), 'user_project_ids'))
        if actual.is_owner:
            return tuple(sorted(projects | set(configured)
                                | _ids(self.settings.get('owner_project_ids', ()), 'owner_project_ids')))
        # A configured leader outside DEMO is record/approval-only, not a reader
        # of every DEMO project simply because their identity is protected.
        if not (set(actual.department_ids) & self.demo_department_ids):
            return ()
        approved_groups = _ids(self.settings.get('group_chat_ids', ()), 'group_chat_ids')
        port = self.bridge.am_self.ledger.port
        for project, chats in configured.items():
            if not isinstance(project, str) or not project:
                raise BindingError('assistant_project_mapping_invalid')
            for chat in _ids(chats, 'project_chats') & approved_groups:
                if chat not in port.disclosure:
                    continue
                try:
                    if port.guard(chat, actual.user_id) is not False:
                        projects.add(project)
                        break
                except Exception:
                    continue
        return tuple(sorted(projects))

    def register_dm(self, event):
        """Called after host receive-v1 or REST snapshot authentication.

        This is a trusted producer API, not a public JSON registration endpoint.
        The event's body, mentions and claimed project names are not consulted.
        """
        if not isinstance(event, Event):
            raise BindingError('assistant_verified_event_required')
        event.validate()
        if (event.tenant_id != self.config.tenant_id or event.channel != 'bot_dm'
                or event.kind != 'message' or event.direction != 'inbound'
                or event.is_self or event.is_deleted or event.sender_id == self.config.bot_id
                or not event.chat_id.startswith('oc_') or not event.native_id.startswith('om_')
                or event.observed_via not in ('event', 'feishu_snapshot', 'poll:live', 'poll:history')):
            raise BindingError('assistant_dm_source_invalid')
        if event.sender_id == self.config.owner_id and event.chat_id != self.config.owner_chat_id:
            raise BindingError('assistant_owner_dm_mismatch')
        if event.chat_id == self.config.owner_chat_id and event.sender_id != self.config.owner_id:
            raise BindingError('assistant_owner_dm_mismatch')
        actor = self.resolve(event.sender_id)
        source_ref = 'feishu:' + event.native_id
        now = self.clock()
        with self.store.transaction() as db:
            old = db.execute('SELECT * FROM r5_dm_bindings WHERE chat_id=?', (event.chat_id,)).fetchone()
            if old and (old['tenant_id'] != event.tenant_id or old['user_id'] != actor.user_id):
                raise BindingError('assistant_dm_binding_conflict')
            db.execute('''INSERT INTO r5_dm_bindings VALUES(?,?,?,?,?,?)
                ON CONFLICT(chat_id) DO UPDATE SET last_seen=max(last_seen,excluded.last_seen)''',
                (event.chat_id, event.tenant_id, actor.user_id, source_ref, now, now))
        return actor

    def dm_actor(self, chat_id):
        if chat_id == self.config.owner_chat_id:
            return self.config.owner_id
        row = self.store.one('SELECT user_id FROM r5_dm_bindings WHERE tenant_id=? AND chat_id=?',
                             (self.config.tenant_id, chat_id))
        return row['user_id'] if row else None

    def dm_chats(self):
        # Inventory is not a membership grant: authorize() rechecks identity.
        rows = self.store.rows('SELECT chat_id FROM r5_dm_bindings WHERE tenant_id=? ORDER BY chat_id',
                               (self.config.tenant_id,))
        return tuple(sorted({self.config.owner_chat_id, *(row['chat_id'] for row in rows)}))


class DEMOScope(HostScope):
    def __init__(self, bridge, config, chats, directory, *, identity='am_bot'):
        super().__init__(bridge, config, chats, identity=identity)
        identities = ('tenant_id', 'bot_id', 'owner_id', 'owner_chat_id')
        if (any(getattr(directory.config, key) != getattr(config, key) for key in identities)
                or directory.bridge is not bridge):
            raise BindingError('assistant_directory_scope_mismatch')
        self.directory = directory

    # verified(payload) deliberately inherits the local-only HostScope path.

    def authorize(self, conversation):
        super().authorize(conversation)
        actor = self.directory.resolve(conversation['actor_id'])
        if (conversation['channel'] == 'bot_dm'
                and self.directory.dm_actor(conversation['chat_id']) != actor.user_id):
            raise BindingError('assistant_dm_member_mismatch')

    def verified_snapshot(self, source, message):
        try:
            if source.channel != 'bot_dm':
                return super().verified_snapshot(source, message)
            source.validate()
            chat = source.effective_chat_id
            member = self.directory.dm_actor(chat)
            if (member is None or source.tenant_id != self.config.tenant_id
                    or source.identity != self.identity or source.adapter != 'feishu_messages'
                    or source.container_type not in ('chat', 'thread')
                    or (source.container_type == 'chat' and source.native_id != chat)
                    or not isinstance(message, dict) or message.get('chat_id') != chat
                    or not isinstance(message.get('message_id'), str)
                    or not message['message_id'].startswith('om_')
                    or (source.container_type == 'thread' and message.get('thread_id') != source.native_id)):
                return False
            sender = message.get('sender', {})
            if (not isinstance(sender, dict) or sender.get('id_type') != 'open_id'
                    or sender.get('tenant_key', self.config.tenant_id) != self.config.tenant_id):
                return False
            if sender.get('sender_type') in ('app', 'bot'):
                return sender.get('id') == self.config.bot_id
            if sender.get('sender_type') != 'user' or sender.get('id') != member:
                return False
            return self.directory.cached_actor(member) is not None
        except (AttributeError, KeyError, TypeError, ValueError, ContractError):
            return False
