"""Feishu private text/post chat -> local Qwen. No model tools or system prompt."""
from __future__ import annotations

import argparse
import fcntl
import json
import logging
import os
from pathlib import Path
import signal
import sqlite3
import stat
import threading
import time
import urllib.request
import uuid

from grok_route import load_grok_settings, merge_grok_config

MODEL = 'qwen3.8:27b-bartowski-q8_0'
OLLAMA = 'http://127.0.0.1:11434'
TTL = 86400


def incoming_text(message, *, bot_open_id=''):
    """Reuse the PoC parser when installed; preserve text-only standalone use."""
    try:
        from fs_agent.message_text import MessageTextError, normalize_message
    except ModuleNotFoundError as exc:
        if exc.name not in ('fs_agent', 'fs_agent.message_text'):
            raise
        # The original bridge can still run without the optional PoC package.
        # Do not invent a permissive post parser when that package is absent.
        if message.get('message_type') != 'text':
            raise ValueError('POST_NORMALIZER_UNAVAILABLE')
        text = json.loads(message['content'])['text']
        if not isinstance(text, str):
            raise ValueError('INVALID_MESSAGE_TEXT')
        return text.strip()
    try:
        return normalize_message(message, bot_open_id=bot_open_id, max_chars=6000).text
    except MessageTextError as exc:
        if exc.code == 'INPUT_TOO_LONG':
            return '/input-too-long'
        raise


def incoming_metadata(message, *, bot_open_id=''):
    """Record only fixed structure labels and parser codes, never content/IDs."""
    from fs_agent.message_text import MAX_CONTENT_BYTES, MessageTextError, normalize_message
    result = {}
    try:
        normalize_message(message, bot_open_id=bot_open_id, max_chars=6000)
        result['normalization'] = 'ok'
    except MessageTextError as exc:
        result['normalization'] = exc.code
    raw = message.get('content')
    if message.get('message_type') != 'post' or not isinstance(raw, str) or len(raw) > MAX_CONTENT_BYTES:
        return result
    try:
        content = json.loads(raw)
    except (ValueError, RecursionError):
        return result
    if not isinstance(content, dict):
        return result
    paragraphs = content.get('content')
    if not isinstance(paragraphs, list):
        result['post_structure'] = 'locale_wrapper' if any(k in content for k in ('zh_cn', 'en_us', 'ja_jp')) else 'other'
        return result
    result['post_structure'] = 'paragraphs'
    known_tags = {'text', 'at', 'a', 'img', 'media', 'emotion', 'code_block', 'md', 'hr'}
    tags = set()
    for paragraph in paragraphs[:256]:
        if isinstance(paragraph, list):
            for node in paragraph[:1024]:
                tag = node.get('tag') if isinstance(node, dict) else None
                tags.add(tag if isinstance(tag, str) and tag in known_tags else 'other')
    result['post_node_types'] = sorted(tags)
    return result


def atomic_json(path, value):
    path = Path(path)
    tmp = path.with_name(path.name + '.' + uuid.uuid4().hex + '.tmp')
    tmp.write_text(json.dumps(value, ensure_ascii=False, indent=2) + '\n')
    tmp.chmod(0o600)
    os.replace(tmp, path)


def model_reply(messages):
    # A production ordinary chat must use the same coordinator as the other routes.
    raise RuntimeError('MODEL_COORDINATOR_REQUIRED')


class Bridge:
    def __init__(self, config, reply, complete=model_reply, *, poc=None, group_ledger=None):
        self.config, self.reply, self.complete = config, reply, complete
        self.poc = poc
        self.group_ledger = group_ledger
        self.am_self = None
        self.information = None
        self.group_ledger_suspended = False
        self.root = Path(config['state_directory'])
        self.root.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.root.chmod(0o700)
        try:
            from health import Health
            self.health=Health(self.root)
        except ModuleNotFoundError as exc:
            if exc.name not in ('health','fs_agent','fs_agent.provider'):raise
            class MinimalHealth:
                def mark(inner,area,state,**details):
                    atomic_json(self.root/'business-health.json',{'area':area,'state':state,'time':time.time()})
            self.health=MinimalHealth()
        self.mutex = threading.RLock()
        self.stop = threading.Event()
        self.wakeup = threading.Event()
        self.db = sqlite3.connect(self.root/'chat.sqlite3', check_same_thread=False, timeout=1)
        self.db.row_factory = sqlite3.Row
        self.db.execute('PRAGMA journal_mode=WAL')
        self.db.execute('PRAGMA secure_delete=ON')
        self.db.executescript('''
          CREATE TABLE IF NOT EXISTS inbox(
            id TEXT PRIMARY KEY, session TEXT NOT NULL, sender TEXT NOT NULL,
            text TEXT NOT NULL, created REAL NOT NULL, received REAL NOT NULL,
            state TEXT NOT NULL, result TEXT);
          CREATE TABLE IF NOT EXISTS turns(
            id TEXT PRIMARY KEY, session TEXT NOT NULL, usertext TEXT NOT NULL,
            answer TEXT NOT NULL, created REAL NOT NULL, expires REAL NOT NULL);
        ''')
        self.response_journal = None
        try:
            from am_response.channels import ProtectedChannelJournal
        except ModuleNotFoundError as exc:
            if exc.name not in ('am_response', 'am_response.channels'):
                raise
        else:
            self.response_journal = ProtectedChannelJournal(self.db)
        # Never rerun inference or resend a response with an unknown outcome.
        with self.db:
            self.db.execute("UPDATE inbox SET state='interrupted', text='', result=NULL WHERE state='processing'")
            self.db.execute("UPDATE inbox SET state='delivery_unknown' WHERE state='sending'")
        self.group_future=None
        self.grok = load_grok_settings(config)
        self.prune()

    def user_allowed(self, user):
        if not isinstance(user, str) or not user.startswith('ou_'):
            return False
        return (self.config.get('access_mode') == 'all_private_users' or
                user in self.config.get('allowed_open_ids', []))

    def prune(self):
        now = time.time()
        with self.mutex, self.db:
            self.db.execute('DELETE FROM turns WHERE expires<=?', (now,))
            if self.response_journal is not None:
                self.response_journal.cleanup(now)
            self.db.execute("UPDATE inbox SET text='',result=NULL WHERE created<=?", (now-TTL,))
            self.db.execute('DELETE FROM inbox WHERE created<=?', (now-7*TTL,))

    def ingest(self, payload):
        if self.information is not None:
            self.information.capture_verified(payload)
        if self.am_self is not None:
            handled, accepted = self.am_self.ingest(payload)
            if handled:
                if accepted:self.wakeup.set()
                return accepted
        if self.group_ledger is not None and payload.get('event', {}).get('message', {}).get('chat_type') == 'group':
            accepted = self.group_ledger.ingest(payload)
            if accepted:
                self.wakeup.set()
            return accepted
        if self.poc is not None:
            mid = payload.get('event', {}).get('message', {}).get('message_id')
            with self.mutex:
                if mid and self.db.execute('SELECT 1 FROM inbox WHERE id=?', (mid,)).fetchone():
                    return False
            handled, accepted = self.poc.ingest(payload)
            if handled:
                if accepted:
                    self.wakeup.set()
                return accepted
        header, event = payload.get('header', {}), payload.get('event', {})
        sender, message = event.get('sender', {}), event.get('message', {})
        user = sender.get('sender_id', {}).get('open_id')
        if (header.get('app_id') != self.config['app_id'] or
                header.get('event_type') != 'im.message.receive_v1' or
                sender.get('sender_type') != 'user' or not self.user_allowed(user) or
                message.get('chat_type') != 'p2p' or message.get('message_type') not in ('text', 'post')):
            return False
        tenant = header.get('tenant_key')
        if not tenant or sender.get('tenant_key', tenant) != tenant:
            return False
        try:
            bot_open_id = self.poc.config.bot_open_id if self.poc is not None else ''
            text = incoming_text(message, bot_open_id=bot_open_id).strip()
            created = int(message['create_time'])/1000
            mid, chat = message['message_id'], message['chat_id']
        except (KeyError, TypeError, ValueError):
            return False
        now = time.time()
        if not text or not mid or not chat or not now-TTL < created <= now+60:
            return False
        if len(text) > 6000:
            text = '/input-too-long'
        session = json.dumps([tenant, self.config['app_id'], chat, user], separators=(',', ':'))
        with self.mutex, self.db:
            if self.db.execute('SELECT 1 FROM inbox WHERE id=?', (mid,)).fetchone():
                return False
            count = self.db.execute("SELECT count(*) FROM inbox WHERE state IN ('queued','processing','sending')").fetchone()[0]
            if count >= 8:
                # SDK returns a failed callback acknowledgement for a retry.
                raise RuntimeError('QUEUE_FULL')
            self.db.execute('INSERT INTO inbox VALUES(?,?,?,?,?,?,?,NULL)',
                            (mid, session, user, text, created, now, 'queued'))
        self.wakeup.set()
        return True

    @staticmethod
    def ordinary_chat_requires_model(text):
        """Shared with training admission; only exact controls bypass the model."""
        return not isinstance(text, str) or text not in (
            '/new', '/help', '/status', '/input-too-long',
        )

    def history(self, session, now):
        rows = list(self.db.execute('SELECT * FROM turns WHERE session=? AND expires>? ORDER BY created DESC LIMIT 4', (session, now)))
        selected, size = [], 0
        for row in rows:
            size += len(row['usertext']) + len(row['answer'])
            if size > 6000:
                break
            selected.append(row)
        return list(reversed(selected))

    def process_one(self):
        self.prune()
        with self.mutex, self.db:
            row = self.db.execute("SELECT * FROM inbox WHERE state='queued' ORDER BY received LIMIT 1").fetchone()
            if row is None:
                return False
            blocked = bool(self.poc is not None and getattr(self.poc, 'store', None) is not None and
                           (self.poc.store.meta('provider_pause') or self.poc.store.meta('resource_window_owner') or self.poc.model_busy() or (getattr(self,'group_future',None) is not None and not self.group_future.done())))
            if blocked and self.ordinary_chat_requires_model(row['text']) and not self._grok_p2p_enabled():
                row = next((candidate for candidate in self.db.execute(
                    "SELECT * FROM inbox WHERE state='queued' ORDER BY received")
                    if not self.ordinary_chat_requires_model(candidate['text'])), None)
                if row is None:
                    return False
            row = dict(row)
            self.db.execute("UPDATE inbox SET state='processing' WHERE id=?", (row['id'],))
            history = self.history(row['session'], time.time())
        text, learned, grok_result = row['text'], False, None
        expires = min([row['created']+TTL] + [r['expires'] for r in history])
        if time.time() >= row['created']+TTL:
            with self.mutex, self.db:
                self.db.execute("UPDATE inbox SET state='expired',text='',result=NULL WHERE id=?", (row['id'],))
            return True
        if time.time()-row['received'] > 600:
            answer = '排队已超过 10 分钟，请重新发送问题。'
        elif text == '/new':
            with self.mutex, self.db:
                self.db.execute('DELETE FROM turns WHERE session=?', (row['session'],))
            answer = '已开启新对话。接下来由本机 Qwen3.8-27B（8 比特）回答。'
        elif text == '/input-too-long':
            answer = '这条文字超过当前单次输入上限（6000 字符），请分段发送。'
        elif text in ('/help', '/status'):
            scope = '所有能访问本应用的用户均可私聊' if self.config.get('access_mode') == 'all_private_users' else '仅开放白名单私聊'
            answer = '已连接 Studio 本地 Qwen3.8-27B Q8_0。直接发送文字即可；/new 开启新对话。当前' + scope + '，每人独立上下文，没有接入项目资料或模型工具。'
            if self.group_ledger is not None:
                answer += '\n群内可 @ 我并提供项目编号或名称，查询 DEMO 台账记录的项目进展。群查询直接读取台账，独立于普通私聊。'
            if self.poc is not None and row['sender'] in self.poc.config.users:
                answer += '\n两人合成测试：/doc sample、/progress sample；/chat 返回普通聊天。'
        else:
            messages = []
            for h in history:
                messages += [{'role':'user','content':h['usertext']}, {'role':'assistant','content':h['answer']}]
            messages.append({'role':'user','content':text})
            if self._grok_p2p_enabled():
                try:
                    session = json.loads(row['session'])
                    chat_id = session[2] if isinstance(session, list) and len(session) > 2 else ''
                except (ValueError, TypeError, IndexError):
                    chat_id = ''
                paused = bool(self.poc is not None and getattr(self.poc, 'store', None) is not None
                              and self.poc.store.meta('provider_pause'))
                try:
                    grok_result = self.grok_route().answer_p2p(
                        text, [{'question': item['usertext'], 'answer': item['answer']} for item in history],
                        chat_id, row['id'], qwen=lambda: self.complete(messages), qwen_paused=paused)
                except Exception:
                    grok_result = self.grok_route()._pack(
                        self.grok.failure_text, status='error', kind='bot_p2p', chat_id=str(chat_id),
                        message_id=row['id'], source='fixed')
                answer = grok_result.reply_text
                learned = grok_result.status == 'ok' and not grok_result.dry_run
            else:
                try:
                    answer = self.complete(messages)
                    learned = True
                except Exception:
                    answer = '本地模型本次未能完成回答，请稍后重试。'
        if self.stop.is_set() or time.time() >= expires:
            with self.mutex, self.db:
                self.db.execute("UPDATE inbox SET state='interrupted',text='',result=NULL WHERE id=?", (row['id'],))
            return True
        # Limit text before publishing; never fall back to another recipient.
        if len(answer.encode('utf-8')) > 24000:
            answer = answer.encode('utf-8')[:23000].decode('utf-8', errors='ignore') + '\n（回答已截断）'
        if grok_result is not None and grok_result.dry_run:
            # send_live is off, or the result still needs confirmation. Do not call Feishu.
            with self.mutex, self.db:
                self.db.execute("UPDATE inbox SET state='done',text='',result=NULL WHERE id=?", (row['id'],))
            self.write_status()
            return True
        with self.mutex, self.db:
            if self.response_journal is not None:
                tenant, app, chat, actor = json.loads(row['session'])
                self.response_journal.prepare(row['id'], (tenant, app, actor, chat, ''),
                    'ordinary_chat', answer, expires)
            self.db.execute("UPDATE inbox SET state='sending',result=? WHERE id=?", (answer,row['id']))
        try:
            answer_mid = self.reply(row['id'], answer)
        except Exception:
            # No automatic resend after an ambiguous HTTP outcome.
            with self.mutex, self.db:
                if self.response_journal is not None:
                    self.response_journal.receipt(row['id'], unknown=True)
                self.db.execute("UPDATE inbox SET state='delivery_unknown' WHERE id=?", (row['id'],))
            return True
        with self.mutex, self.db:
            if self.response_journal is not None:
                self.response_journal.receipt(row['id'], message_id=answer_mid)
            self.db.execute("UPDATE inbox SET state='done',text='',result=NULL WHERE id=?", (row['id'],))
            if learned:
                self.db.execute('INSERT OR REPLACE INTO turns VALUES(?,?,?,?,?,?)',
                                (row['id'],row['session'],text,answer,row['created'],expires))
        self.write_status()
        return True

    def _grok_p2p_enabled(self):
        settings = getattr(self, 'grok', None)
        return bool(settings is not None and settings.enabled and settings.bot_p2p)

    def grok_route(self):
        route = getattr(self, '_grok_route', None)
        if route is None:
            from grok_route import GrokRoute
            route = GrokRoute(self.grok)
            self._grok_route = route
        return route

    def write_status(self):
        with self.mutex:
            counts = dict(self.db.execute('SELECT state,count(*) FROM inbox GROUP BY state').fetchall())
        atomic_json(self.root/'worker-status.json', {'time':time.time(),'model':MODEL,'counts':counts})
        self.health.mark('private_chat','ok',counts=counts)

    def process_cycle(self):
        # The one consumer continues handling receipts while document generation runs.
        # Ordinary chat must not overlap the existing single model lane.
        resource = getattr(self, 'resource_window', None)
        training = getattr(self, 'training_control', None)
        training_busy = training is not None and training.thread is not None and training.thread.is_alive()
        if resource is not None and not training_busy:
            resource.tick()
        resource_busy = resource is not None and resource.thread is not None and resource.thread.is_alive()
        if training is not None and not resource_busy:
            training.tick()
        worked = self.am_self.pump() if self.am_self is not None else False
        conversation = getattr(getattr(self, 'am_self', None), 'conversation', None)
        finalizer = getattr(getattr(conversation, 'agent', None), 'output_finalizer', None)
        if (finalizer is not None and hasattr(finalizer, 'tick')
                and not self.poc.store.meta('resource_window_owner')
                and not self.poc.store.meta('provider_pause')):
            finalizer.tick()
        worked = (self.poc.step() if self.poc is not None else False) or worked
        busy = self.poc is not None and ((hasattr(self.poc, "model_busy") and self.poc.model_busy()) or self.poc.store.meta('resource_window_owner'))
        future=getattr(self,'group_future',None)
        running=future is not None and not future.done()
        if future is not None and future.done():
            try:worked=bool(future.result()) or worked
            except Exception:atomic_json(self.root/'group-ledger-error.json', {'time':time.time(),'error':'LEDGER_WORKER_FAILED'})
            self.group_future=None
        needs_model=self.group_ledger is not None and getattr(self.group_ledger,'assistant',None) is not None
        if self.group_ledger is not None and not self.group_ledger_suspended and not self.stop.is_set() and not running and (not needs_model or not busy or hasattr(self.group_ledger,'can_process_without_model')):
            try:
                pool=getattr(getattr(self.poc,'gateway',None),'pool',None)
                if needs_model and pool is not None and not busy:
                    with self.group_ledger.mutex:
                        pending=self.group_ledger.db.execute("SELECT 1 FROM ledger_inbox WHERE state='queued' LIMIT 1").fetchone()
                    if pending:
                        self.group_future=pool.submit(self.group_ledger.step)
                        self.poc.gateway.group_query_future=self.group_future
                        worked=True
                else:worked=(self.group_ledger.step(model_available=not busy) if hasattr(self.group_ledger,'can_process_without_model') else self.group_ledger.step()) or worked
            except Exception:atomic_json(self.root/'group-ledger-error.json', {'time':time.time(),'error':'LEDGER_WORKER_FAILED'})
        # A timed-out HTTP client does not prove that shared-server inference
        # stopped. Keep every model lane paused until that outcome is resolved.
        paused = bool(self.poc is not None and getattr(self.poc, 'store', None) is not None
                      and self.poc.store.meta('provider_pause'))
        if not self.stop.is_set():
            if self.am_self is not None and self.am_self.background_enabled:
                chat_future=getattr(self,'chat_future',None)
                if chat_future is not None and chat_future.done():
                    try:worked=bool(chat_future.result()) or worked
                    finally:self.chat_future=None
                if getattr(self,'chat_future',None) is None:
                    # Reuse the existing single model worker and coordinator.
                    # Query and delivery I/O stay pumpable while ordinary chat runs.
                    self.chat_future=self.poc.gateway.pool.submit(self.process_one)
            else:worked = self.process_one() or worked
        return worked

    def run(self):
        while not self.stop.is_set():
            try:
                # Clear before checking durable work: a commit during/after the
                # check leaves the event set, so waiting cannot lose that wake.
                self.wakeup.clear()
                if not self.process_cycle():
                    self.wakeup.wait(1)
            except Exception:
                # Do not log incoming text, credentials, or exception bodies.
                atomic_json(self.root/'worker-error.json', {'time':time.time(),'error':'WORKER_FAILED'})
                self.stop.set()
                os.kill(os.getpid(), signal.SIGTERM)


def serve(config):
    config = merge_grok_config(config, use_sidecar=True)
    if not config.get('exclusive_consumer_confirmed'):
        raise RuntimeError('Confirm that the old event consumer is stopped before serving')
    if config.get('access_mode', 'allowlist') not in ('allowlist', 'all_private_users'):
        raise RuntimeError('Unknown access mode')
    if config.get('access_mode') != 'all_private_users' and (not config.get('allowed_open_ids') or any(not u.startswith('ou_') for u in config['allowed_open_ids'])):
        raise RuntimeError('An explicit user allowlist is required')
    root = Path(config['state_directory'])
    root.mkdir(mode=0o700, parents=True, exist_ok=True)
    lock = (root/'instance.lock').open('a')
    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    secret_file = Path(config['app_secret_file'])
    secret_stat = secret_file.stat()
    if (secret_file.is_symlink() or not stat.S_ISREG(secret_stat.st_mode) or
            secret_stat.st_uid != os.getuid() or secret_stat.st_mode & 0o077):
        raise RuntimeError('App Secret must be a regular private file with mode 0600')
    import lark_oapi as lark
    from lark_oapi.api.im.v1 import ReplyMessageRequest, ReplyMessageRequestBody
    logging.disable(logging.CRITICAL)
    secret = secret_file.read_text().strip()
    client = (lark.Client.builder().app_id(config['app_id']).app_secret(secret)
              .timeout(20).log_level(lark.LogLevel.CRITICAL).build())

    def reply(mid, text):
        body = (ReplyMessageRequestBody.builder().msg_type('text')
                .content(json.dumps({'text':text},ensure_ascii=False))
                .uuid(str(uuid.uuid5(uuid.NAMESPACE_URL, config['app_id']+'/'+mid))).build())
        response = client.im.v1.message.reply(ReplyMessageRequest.builder().message_id(mid).request_body(body).build())
        if not response.success() or not response.data or not response.data.message_id:
            atomic_json(root/'send-error.json', {'time':time.time(),'code':response.code})
            raise RuntimeError('SEND_REJECTED_OR_UNKNOWN')
        return response.data.message_id

    bridge = Bridge(config, reply)
    if config.get('poc_config'):
        from fs_agent.embedded import EmbeddedPoC
        bridge.poc = EmbeddedPoC.open(Path(config['poc_config']), client, app_id=config['app_id'])
        bridge.complete = bridge.poc.gateway.provider.chat
    if config.get('group_ledger', {}).get('enabled') is True:
        if bridge.poc is None:
            raise RuntimeError('Ledger route requires the verified existing bot identity')
        from group_ledger import FeishuLedgerPort, GroupLedger
        port = FeishuLedgerPort(client, app_id=config['app_id'],
                                tenant_key=bridge.poc.config.tenant_key,
                                bot_open_id=bridge.poc.config.bot_open_id,
                                department_root_id=config['group_ledger'].get('department_root_id'),
                                disclosure=config['group_ledger'].get('disclosure', {}))
        assistant = None
        if config['group_ledger'].get('qwen_enabled') is True:
            from group_ai import QwenGroupAssistant
            assistant = QwenGroupAssistant(bridge.poc.gateway.provider, root,
                paused=lambda: bool(bridge.poc.store.meta('provider_pause')),
                pause=lambda code: bridge.poc.store.set_meta('provider_pause', code),
                answer_backend='grok' if bridge.grok.uses_group else 'qwen',
                grok=bridge.grok_route() if bridge.grok.uses_group else None)
        bridge.group_ledger = GroupLedger(root, port, reply, stopped=bridge.stop.is_set, assistant=assistant)
        bridge.group_ledger.health=bridge.health
        bridge.group_ledger.write_status()
        if config.get('am_self'):
            from am_service import AmSelfService
            metadata=None
            if config.get('am_self_metadata'):
                from sheets_write_port import CliProtectedReader
                setting=config['am_self_metadata']
                metadata=CliProtectedReader(setting['binary'],setting['sha256'],setting['identity'])
            bridge.am_self=AmSelfService(bridge.poc.gateway,bridge.group_ledger,flags=config['am_self'],
                                        calendar=config.get('am_calendar'),metadata_reader=metadata,
                                        execution_policy=config.get('am_write_execution'),query_settings=config.get('am_query'),
                                        conversation_settings=config.get('am_conversation'),harness_settings=config.get('am_harness'))
            bridge.am_self.enable_background(bridge.wakeup.set)
    if config.get('am_information', {}).get('enabled') is True:
        from am_information import AMInformation
        bridge.information = AMInformation(bridge, client, config['am_information'])
        bridge.am_self.information = bridge.information
        bridge.am_self.g.logic.shared_evidence = bridge.information.evidence_bundle
        bridge.am_self.g.logic.shared_evidence_current = bridge.information.document_evidence_current
        bridge.am_self.g.logic.shared_evidence_authorize = bridge.information.authorize_document_evidence

    from am_gateway_r2_host import install
    bridge.multi_ai, bridge.multi_ai_scheduler = install(bridge, config, client, root)

    def on_message(event):
        receive_started=time.monotonic()
        payload = json.loads(lark.JSON.marshal(event))
        h, e = payload.get('header', {}), payload.get('event', {})
        s, m = e.get('sender', {}), e.get('message', {})
        diagnostic = {'time':time.time(), 'app_matches':h.get('app_id') == config['app_id'],
                      'event_type':h.get('event_type'), 'sender_type':s.get('sender_type'),
                      'sender_allowed':bridge.user_allowed(s.get('sender_id', {}).get('open_id')),
                      'tenant_present':bool(h.get('tenant_key')), 'chat_type':m.get('chat_type'),
                      'message_type':m.get('message_type')}
        if bridge.poc is not None:
            diagnostic.update(incoming_metadata(m, bot_open_id=bridge.poc.config.bot_open_id))
        try:
            multi = bridge.multi_ai.ingest(payload) if bridge.multi_ai is not None else None
            diagnostic['accepted'] = multi.captured if multi is not None and multi.claimed else bridge.ingest(payload)
        except Exception as exc:
            diagnostic['error_type'] = type(exc).__name__
            raise
        finally:
            diagnostic['acceptance_seconds']=round(time.monotonic()-receive_started,4)
            atomic_json(root/'event-status.json', diagnostic)
            bridge.health.mark('reception','ok' if diagnostic.get('accepted') else 'not_accepted',error=diagnostic.get('error_type'),elapsed=diagnostic['acceptance_seconds'])

    def on_card(event):
        from lark_oapi.event.callback.model.p2_card_action_trigger import P2CardActionTriggerResponse
        accepted = False
        if bridge.multi_ai is not None:
            r2_response = bridge.multi_ai.card_callback(json.loads(lark.JSON.marshal(event)))
            if r2_response is not None:
                return P2CardActionTriggerResponse(r2_response)
        if bridge.am_self is not None:
            payload=json.loads(lark.JSON.marshal(event))
            probe=bridge.am_self.query_probe_response(payload)
            if probe is not None:return P2CardActionTriggerResponse(probe)
            direct=bridge.am_self.direct_query_response(payload)
            if direct is not None:return P2CardActionTriggerResponse(direct)
            handled, accepted = bridge.am_self.ingest(payload)
        else:handled=False
        if bridge.poc is not None and not handled:
            _, accepted = bridge.poc.ingest(json.loads(lark.JSON.marshal(event)))
        if accepted:
            bridge.wakeup.set()
        response=P2CardActionTriggerResponse({'toast': {
            'type': 'info', 'content': '已收到，正在处理' if accepted else '已处理或当前无法访问'}})
        if bridge.am_self is not None:
            from fs_agent import request_timing
            payload=json.loads(lark.JSON.marshal(event))
            request_timing.anchor(bridge.am_self.s,payload.get('header',{}).get('event_id'),'callback')
        return response

    def on_menu(event):
        if bridge.am_self is not None:
            _,accepted=bridge.am_self.ingest(json.loads(lark.JSON.marshal(event)))
            if accepted:bridge.wakeup.set()

    def on_recalled(event):
        if bridge.multi_ai is not None:
            bridge.multi_ai.recall(json.loads(lark.JSON.marshal(event)))
        if bridge.information is not None:
            bridge.information.capture_verified(json.loads(lark.JSON.marshal(event)))

    handler = (lark.EventDispatcherHandler.builder('','',lark.LogLevel.CRITICAL)
               .register_p2_im_message_receive_v1(on_message)
               .register_p2_im_message_recalled_v1(on_recalled)
               .register_p2_card_action_trigger(on_card)
               .register_p2_application_bot_menu_v6(on_menu)
               .register_p2_im_message_message_read_v1(lambda event: None)
               .register_p2_im_message_reaction_created_v1(lambda event: None)
               .register_p2_im_message_reaction_deleted_v1(lambda event: None).build())

    socket_base = lark.ws.Client
    if bridge.poc is not None:
        from fs_agent.feishu import CardAwareClient
        socket_base = CardAwareClient

    class ObservedClient(socket_base):
        async def _handle_control_frame(self, frame):
            await super()._handle_control_frame(frame)
            atomic_json(root/'heartbeat-status.json', {'time':time.time(),'control_frame_received':True})

        async def _handle_data_frame(self, frame):
            atomic_json(root/'frame-status.json', {'time':time.time(),'received':True})
            await super()._handle_data_frame(frame)

        async def _connect(self):
            try:
                await super()._connect()
                atomic_json(root/'connection-status.json', {'time':time.time(),'state':'connected','app_id':config['app_id']})
                bridge.health.mark('connection','ok')
            except Exception as exc:
                atomic_json(root/'connection-status.json', {'time':time.time(),'state':'connection_failed','error_type':type(exc).__name__})
                raise

        async def _disconnect(self):
            try:
                await super()._disconnect()
            finally:
                atomic_json(root/'connection-status.json', {'time':time.time(),'state':'disconnected'})

    if bridge.poc is not None and bridge.am_self is not None:
        from am_resource_window import NativeResourceWindow
        from am_training_control import NativeTrainingControl
        bridge.resource_window = NativeResourceWindow(bridge)
        bridge.training_control = NativeTrainingControl(bridge)

    from health import loaded_manifest
    loaded_manifest(bridge,config)
    socket = ObservedClient(config['app_id'], secret, event_handler=handler, log_level=lark.LogLevel.CRITICAL)
    thread = threading.Thread(target=bridge.run, daemon=True)
    thread.start()
    if bridge.information is not None:
        bridge.information.start()
    def stop(*_):
        raise KeyboardInterrupt
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    try:
        socket.start()
    except KeyboardInterrupt:
        pass
    finally:
        bridge.stop.set()
        r2_stopped = True
        if bridge.multi_ai_scheduler is not None:
            r2_stop = bridge.multi_ai_scheduler.stop()
            r2_stopped = r2_stop['stopped']
        if bridge.poc is not None:
            bridge.poc.gateway.stopping.set()
        bridge.wakeup.set()
        thread.join(timeout=2)
        information_stopped = bridge.information is None or bridge.information.close()
        information_stopped = r2_stopped and information_stopped
        if bridge.am_self is not None and not thread.is_alive() and information_stopped:
            bridge.am_self.close_query()
            if bridge.am_self.conversation is not None:bridge.am_self.conversation.close()
        if bridge.poc is not None and not thread.is_alive() and information_stopped:
            bridge.poc.close()
        if bridge.group_ledger is not None and not thread.is_alive() and information_stopped:
            bridge.group_ledger.close()
        atomic_json(root/'connection-status.json', {'time':time.time(),'state':'stopped'})


def main():
    os.umask(0o077)
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', type=Path, required=True)
    args = parser.parse_args()
    config = json.loads(args.config.read_text())
    serve(config)


if __name__ == '__main__':
    try:
        main()
    except Exception as exc:
        # Never emit secret-bearing SDK exceptions.
        print(json.dumps({'error':'BRIDGE_START_FAILED','error_type':type(exc).__name__}), flush=True)
        raise SystemExit(1)
