"""Feishu artifact upload, unique file messages and independent exact readback.

The file side effects use a separate durable table. Text delivery alone never
proves artifact delivery. Ambiguous upload/send states are retained, not retried.
"""
from __future__ import annotations

import io
import hashlib
import json
import math
import threading
import uuid

from am_gateway.collectors import SourceSpec
from am_gateway.models import ContractError, Event, key
from am_gateway.phase1.contracts import Policy


class ArtifactDelivery:
    def __init__(self, harness, artifact_store, client, authority, readers, send_live=False):
        self.harness, self.store, self.artifact_store = harness, harness.store, artifact_store
        self.client, self.authority, self.readers, self.send_live = client, authority, readers, send_live
        self._delivery_claim = threading.local()
        with self.store.transaction() as db:
            db.execute('''CREATE TABLE IF NOT EXISTS h4_artifact_effects(
                effect_id TEXT PRIMARY KEY,task_id TEXT NOT NULL,artifact_id TEXT NOT NULL,
                business_id TEXT NOT NULL,generation INTEGER NOT NULL,target TEXT NOT NULL,
                name TEXT NOT NULL,sha256 TEXT NOT NULL,send_uuid TEXT NOT NULL,
                file_key TEXT NOT NULL DEFAULT '',platform_id TEXT NOT NULL DEFAULT '',
                state TEXT NOT NULL,detail TEXT NOT NULL DEFAULT '',created REAL NOT NULL,
                verified_at REAL,platform_time REAL,UNIQUE(task_id,artifact_id,target))''')
            if 'platform_time' not in {r[1] for r in db.execute('PRAGMA table_info(h4_artifact_effects)')}:
                db.execute('ALTER TABLE h4_artifact_effects ADD COLUMN platform_time REAL')
            present = {r[1] for r in db.execute('PRAGMA table_info(h4_artifact_effects)')}
            for column, declaration in (('resource_file_key', "TEXT NOT NULL DEFAULT ''"),
                    ('resource_sha256', "TEXT NOT NULL DEFAULT ''"),
                    ('resource_size_bytes', 'INTEGER')):
                if column not in present:
                    db.execute('ALTER TABLE h4_artifact_effects ADD COLUMN ' + column + ' ' + declaration)

    def _authorize(self, task_id):
        self._guard_claim()
        task = self.harness.task(task_id)
        with self.store.transaction() as db:
            task, policy = self.harness._authorize(db, task_id, task['generation'], require_running=False)
            row = db.execute('SELECT event_json FROM gw_items WHERE item_id=?', (task['item_id'],)).fetchone()
            event = Event.from_dict(json.loads(row[0]))
        current = self.authority(event)
        if (not self.send_live or current is None or policy.revoked or not policy.allow_reply
                or current.revoked or not current.allow_reply
                or current.principal != policy.principal or current.reply_target != policy.reply_target
                or not set(policy.source_chats) <= set(current.source_chats)
                or not set(policy.projects) <= set(current.projects)):
            raise ContractError('artifact delivery authority unavailable or changed')
        return task, policy, event

    def _row(self, effect_id):
        return self.store.one('SELECT * FROM h4_artifact_effects WHERE effect_id=?', (effect_id,))

    def _state(self, effect_id, state, detail='', **fields):
        allowed = {'file_key', 'platform_id', 'verified_at', 'platform_time',
                   'resource_file_key', 'resource_sha256', 'resource_size_bytes'}
        if set(fields) - allowed:
            raise ContractError('unsupported artifact effect field')
        assignments = ['state=?', 'detail=?'] + [name + '=?' for name in fields]
        with self.store.transaction() as db:
            self._guard_claim(db)
            db.execute('UPDATE h4_artifact_effects SET ' + ','.join(assignments) + ' WHERE effect_id=?',
                       (state, detail[:120], *fields.values(), effect_id))
        return self._row(effect_id)

    def _guard_claim(self, db=None):
        guard = getattr(self._delivery_claim, 'guard', None)
        if guard is not None and not guard(db):
            raise ContractError('artifact delivery claim no longer owns the scope')

    def deliver_task(self, task_id, *, claim_guard=None):
        # One ArtifactDelivery can serve concurrent chats. Per-call thread-local
        # fencing prevents a late readback from overwriting a replacement claim.
        previous = getattr(self._delivery_claim, 'guard', None)
        self._delivery_claim.guard = claim_guard
        try:
            return self._deliver_task(task_id)
        finally:
            self._delivery_claim.guard = previous

    def _deliver_task(self, task_id):
        task = self.harness.task(task_id)
        result = json.loads(task['result_json'] or '{}')
        artifacts = result.get('artifacts', [])
        if not isinstance(artifacts, list) or len(artifacts) > 16:
            raise ContractError('bounded artifact result list required')
        if not artifacts:
            return {'complete': True, 'artifacts': [], 'completed_platform_time': None}
        if task['execution_state'] not in {'succeeded', 'failed'}:
            raise ContractError('finished recorded artifact result required')
        outcomes = []
        for artifact in artifacts:
            aid = artifact.get('artifact_id') if isinstance(artifact, dict) else None
            if not aid:
                raise ContractError('registered artifact reference required')
            record = self.artifact_store.get(aid, task_id=task_id)
            if record['business_id'] != task['business_id']:
                raise ContractError('artifact belongs to a different business operation')
            _, policy, event = self._authorize(task_id)
            eid = 'file_send_' + key(task['business_id'], aid, policy.reply_target)[:32]
            with self.store.transaction() as db:
                self._guard_claim(db)
                old = db.execute('SELECT * FROM h4_artifact_effects WHERE effect_id=?', (eid,)).fetchone()
                if old:
                    if (old['name'], old['sha256'], old['target']) != (record['name'], record['sha256'], policy.reply_target):
                        raise ContractError('artifact effect identity conflict')
                else:
                    db.execute('''INSERT INTO h4_artifact_effects(effect_id,task_id,artifact_id,business_id,
                        generation,target,name,sha256,send_uuid,state,created)
                        VALUES(?,?,?,?,?,?,?,?,?,'ready',?)''',
                        (eid, task_id, aid, task['business_id'], task['generation'], policy.reply_target,
                         record['name'], record['sha256'], str(uuid.uuid5(uuid.NAMESPACE_URL, 'am-r4-artifact/' + eid)),
                         self.store.clock()))
            row = self._row(eid)
            if row['state'] == 'ready':
                row = self._upload(row)
            if row['state'] == 'uploaded':
                row = self._send(row, event)
            if row['state'] in {'sent', 'readback_pending'} or (row['state'] == 'verified' and
                    (not row['platform_time'] or not row['resource_sha256'])):
                row = self._verify(row)
            outcomes.append({name: row[name] for name in
                             ('effect_id', 'artifact_id', 'name', 'sha256', 'platform_id', 'platform_time',
                              'state', 'detail', 'resource_file_key', 'resource_sha256', 'resource_size_bytes')})
        complete = all(row['state'] == 'verified' for row in outcomes)
        return {'complete': complete, 'artifacts': outcomes,
                'completed_platform_time': max(row['platform_time'] for row in outcomes) if complete else None}

    def _upload(self, row):
        self._authorize(row['task_id'])
        # Only one worker can move ready -> uploading. A process death after this
        # point leaves an unresolved row for explicit reconciliation.
        with self.store.transaction() as db:
            self._guard_claim(db)
            changed = db.execute("UPDATE h4_artifact_effects SET state='uploading' WHERE effect_id=? AND state='ready'",
                                 (row['effect_id'],)).rowcount
        if not changed:
            return self._row(row['effect_id'])
        from lark_oapi.api.im.v1 import CreateFileRequest, CreateFileRequestBody
        data = self.artifact_store.read(row['artifact_id'], task_id=row['task_id'])
        stream = io.BytesIO(data)
        stream.name = row['name']
        request = CreateFileRequest.builder().request_body(CreateFileRequestBody.builder()
            .file_type('stream').file_name(row['name']).file(stream).build()).build()
        try:
            response = self.client.im.v1.file.create(request)
            if not response.success():
                return self._state(row['effect_id'], 'upload_failed', 'platform_rejected_upload')
            file_key = getattr(response.data, 'file_key', '')
            if not isinstance(file_key, str) or not file_key:
                return self._state(row['effect_id'], 'upload_unknown', 'upload_receipt_missing')
            return self._state(row['effect_id'], 'uploaded', file_key=file_key)
        except Exception as exc:
            return self._state(row['effect_id'], 'upload_unknown', type(exc).__name__)
        finally:
            stream.close()

    def _send(self, row, event):
        self._authorize(row['task_id'])
        with self.store.transaction() as db:
            self._guard_claim(db)
            changed = db.execute("UPDATE h4_artifact_effects SET state='sending' WHERE effect_id=? AND state='uploaded'",
                                 (row['effect_id'],)).rowcount
        if not changed:
            return self._row(row['effect_id'])
        from lark_oapi.api.im.v1 import (CreateMessageRequest, CreateMessageRequestBody,
                                       ReplyMessageRequest, ReplyMessageRequestBody)
        content = json.dumps({'file_key': row['file_key']}, ensure_ascii=False)
        try:
            if row['target'] == event.chat_id:
                body = (ReplyMessageRequestBody.builder().msg_type('file').content(content)
                        .uuid(row['send_uuid']).reply_in_thread(bool(event.thread_id)).build())
                response = self.client.im.v1.message.reply(ReplyMessageRequest.builder()
                    .message_id(event.native_id).request_body(body).build())
            else:
                body = (CreateMessageRequestBody.builder().receive_id(row['target']).msg_type('file')
                        .content(content).uuid(row['send_uuid']).build())
                response = self.client.im.v1.message.create(CreateMessageRequest.builder()
                    .receive_id_type('chat_id').request_body(body).build())
            if not response.success():
                return self._state(row['effect_id'], 'send_failed', 'platform_rejected_file_message')
            message_id = getattr(response.data, 'message_id', '')
            if not isinstance(message_id, str) or not message_id:
                return self._state(row['effect_id'], 'send_unknown', 'file_message_receipt_missing')
            return self._state(row['effect_id'], 'sent', platform_id=message_id)
        except Exception as exc:
            return self._state(row['effect_id'], 'send_unknown', type(exc).__name__)

    def _verify(self, row):
        try:
            _, policy, event = self._authorize(row['task_id'])
            cfg = self.harness.config
            source = SourceSpec(cfg.tenant_id, row['target'], identity='am_bot',
                                channel='bot_dm' if row['target'] == cfg.owner_chat_id or
                                    (event.channel == 'bot_dm' and row['target'] == event.chat_id) else 'group')
            message = self.readers.read_message(source, row['platform_id'])
            sender = message.get('sender', {})
            body = json.loads(message.get('body', {}).get('content', '{}'))
            native_time = message.get('create_time')
            if isinstance(native_time, bool) or not isinstance(native_time, (str, int)):
                raise ContractError('native file message time required')
            platform_time = int(native_time) / 1000
            if not math.isfinite(platform_time) or platform_time <= 0:
                raise ContractError('positive finite file message time required')
            if (message.get('message_id') != row['platform_id'] or message.get('chat_id') != row['target']
                    or message.get('deleted') is not False or message.get('msg_type') != 'file'
                    or sender.get('id') != cfg.bot_id or sender.get('sender_type') not in {'app', 'bot'}
                    or not isinstance(body.get('file_key'), str) or not body['file_key'].startswith('file_')
                    or body.get('file_name') != row['name']
                    or (row['target'] == event.chat_id and message.get('parent_id') != event.native_id)
                    or (row['target'] == event.chat_id and event.thread_id and message.get('thread_id') != event.thread_id)):
                return self._state(row['effect_id'], 'readback_pending', 'file_message_readback_mismatch')
            # Feishu can replace the upload key with a message resource key.
            # A matching name is insufficient: download the resource attached
            # to this exact message and compare its complete registered bytes.
            self._authorize(row['task_id'])
            record = self.artifact_store.get(row['artifact_id'], task_id=row['task_id'])
            from lark_oapi.api.im.v1 import GetMessageResourceRequest
            request = (GetMessageResourceRequest.builder().message_id(row['platform_id'])
                       .file_key(body['file_key']).type('file').build())
            response = self.client.im.v1.message_resource.get(request)
            stream = getattr(response, 'file', None)
            if not response.success() or not callable(getattr(stream, 'read', None)):
                return self._state(row['effect_id'], 'readback_pending', 'file_resource_unavailable')
            try:
                data = stream.read(record['size_bytes'] + 1)
            finally:
                stream.close()
            if (not isinstance(data, bytes) or len(data) != record['size_bytes']
                    or hashlib.sha256(data).hexdigest() != row['sha256']):
                return self._state(row['effect_id'], 'readback_pending', 'file_resource_integrity_mismatch')
            self._authorize(row['task_id'])
            current = self.readers.read_message(source, row['platform_id'])
            fields = ('message_id', 'chat_id', 'deleted', 'msg_type', 'parent_id', 'thread_id', 'create_time')
            if (any(current.get(field) != message.get(field) for field in fields)
                    or current.get('sender', {}).get('id') != sender.get('id')
                    or current.get('sender', {}).get('sender_type') != sender.get('sender_type')
                    or json.loads(current.get('body', {}).get('content', '{}')) != body):
                return self._state(row['effect_id'], 'readback_pending', 'file_message_changed_during_readback')
            return self._state(row['effect_id'], 'verified', verified_at=self.store.clock(),
                               platform_time=platform_time, resource_file_key=body['file_key'],
                               resource_sha256=hashlib.sha256(data).hexdigest(), resource_size_bytes=len(data))
        except Exception as exc:
            return self._state(row['effect_id'], 'readback_pending', type(exc).__name__)

    def reconcile(self, effect_id, *, file_key='', platform_id=''):
        """Host/operator supplies an exact SDK mapping, then normal readback verifies.

        This never sends another message. A model-provided filename/search hit
        is not a valid mapping; expose only through an authenticated host route.
        """
        row = self._row(effect_id)
        if row is None:
            raise ContractError('unknown artifact effect')
        self._authorize(row['task_id'])
        if row['state'] == 'verified':
            if ((file_key and row['file_key'] != file_key)
                    or (platform_id and row['platform_id'] != platform_id)):
                raise ContractError('verified artifact mapping is immutable')
            return row
        if file_key and row['file_key'] and row['file_key'] != file_key:
            raise ContractError('conflicting upload mapping')
        if platform_id and row['platform_id'] and row['platform_id'] != platform_id:
            raise ContractError('conflicting file-message mapping')
        fields = {}
        if file_key:
            fields['file_key'] = file_key
        if platform_id:
            if not (file_key or row['file_key']):
                raise ContractError('file-message reconciliation needs upload mapping')
            fields['platform_id'] = platform_id
            state = 'sent'
        elif file_key and row['state'] in {'uploading', 'upload_unknown'}:
            state = 'uploaded'
        else:
            raise ContractError('exact unresolved mapping required')
        row = self._state(effect_id, state, **fields)
        return self._verify(row) if platform_id else row
