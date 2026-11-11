"""Plain-text navigation and explicit approvals over the existing AM runtime.

No model, sender, scheduler, callback fabrication, or second task store lives
here. Text approval is a distinct authenticated-message path. It verifies both
the original inbound message and the entire delivered preview before reusing
the original coordinator, native adoption, and final dependency guards.
"""
from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime
import json
import re
import secrets
import uuid

from fs_agent.config import BoundaryError
from fs_agent.project_catalog import canonical, digest
from fs_agent.scheduler import TZ
from am_followup.followup import ApprovalRequest


MAX_TEXT = 5900
_COMMAND = re.compile(
    r'^(?:查看跟进|跟进状态|跟进反馈|反馈已阅|群聊跟进|私聊跟进|修改跟进|重审依据|采用候选)\s+\S+'
    r'|^(?:确认发送|取消发送|确认依据|确认采用|取消操作)\s+[A-F0-9]{8}$'
    r'|^我的跟进(?:\s+(?:待处理|待反馈|已关闭))?(?:\s+\d+)?$')
_APPROVAL = re.compile(r'^(确认发送|取消发送|确认依据|确认采用|取消操作)\s+([A-F0-9]{8})$')
_REPLY_ACTIONS = frozenset(('确认发送','取消发送','确认依据','确认采用','取消操作'))
_FLOW_ACTIONS = {
    'followup': ('确认发送', 'confirm_send_once'),
    'followup_source_review': ('确认依据', 'review_sources'),
    'followup_import': ('确认采用', 'adopt'),
}


def natural_action(text):
    """Explicit speech only; it still requires an exact quoted preview below."""
    value=re.sub(r'[，,。！!\s]','',text)
    if value in ('按这个发','就这样发吧','可以发给他吧','确认按这条发','照这个发送','发吧'):
        return '确认发送'
    if value in ('先别发','不用发了','取消这次发送','先不要发'):
        return '取消发送'
    return None


def wants_text(text):
    # Existing exact navigation keeps its original routing/thread precedence;
    # only the new explicit commands use this additional route.
    return (isinstance(text, str) and text.strip()!='我的跟进'
            and (natural_action(text) is not None or text.strip() in _REPLY_ACTIONS or bool(_COMMAND.match(text.strip()))))


def _plain(message):
    content = message.get('content')
    if isinstance(content, str):
        try: content = json.loads(content)
        except (ValueError, TypeError): raise BoundaryError('FOLLOWUP_TEXT_MESSAGE_INVALID') from None
    if message.get('msg_type') != 'text' or not isinstance(content, dict) or set(content) != {'text'} or not isinstance(content['text'], str):
        raise BoundaryError('FOLLOWUP_TEXT_MESSAGE_INVALID')
    return content['text']


class FollowupTextUI:
    def __init__(self, host):
        self.h, self.am, self.s = host, host.am, host.s
        with self.s.lock:
            self.s.conn.executescript('''
            CREATE TABLE IF NOT EXISTS am_followup_text_actions(
              code TEXT PRIMARY KEY,nonce TEXT UNIQUE NOT NULL,scope TEXT NOT NULL,
              actor TEXT NOT NULL,chat TEXT NOT NULL,flow TEXT NOT NULL,
              body TEXT NOT NULL,body_hash TEXT NOT NULL,snapshot_hash TEXT NOT NULL,
              created REAL NOT NULL,expires REAL NOT NULL,outbox_id TEXT UNIQUE NOT NULL,
              superseded INTEGER NOT NULL DEFAULT 0);
            CREATE TABLE IF NOT EXISTS am_followup_text_choices(
              scope TEXT PRIMARY KEY,items TEXT NOT NULL,expires REAL NOT NULL);
            ''')

    def _owner(self, row):
        if (row.get('app'), row.get('tenant'), row.get('user'), row.get('chat_type')) != (
                self.h.app, self.h.tenant, self.h.owner, 'p2p') or row.get('expires', 0) <= self.s.clock():
            raise BoundaryError('FOLLOWUP_OWNER_SCOPE_REQUIRED')
        if not self.am.flags['items']: raise BoundaryError('ITEMS_NOT_ENABLED')
        self.am.ledger.port.authorize_user(self.h.owner)

    @contextmanager
    def read_scope(self, row):
        """Identity first, AM mutex then information fence, as in the host."""
        self._owner(row)
        render = dict(row, _followup_text=True, _query_read=True,
                      _query_grant=self.am.query_grant(row))
        info = self.am.information
        with self.s.lock:
            if info is None:
                yield render
            else:
                with info.store.transaction():
                    epoch = info.store.access_version()
                    if row.get('_source_access_version', epoch) != epoch:
                        raise BoundaryError('SOURCE_ACCESS_CHANGED')
                    render['_source_access_version'] = epoch
                    yield render

    def publish(self, row, body, *, kind='followup_text', event_key=None):
        if not isinstance(body, str) or not body or len(body) > MAX_TEXT:
            raise BoundaryError('FOLLOWUP_TEXT_TOO_LARGE')
        if row.get('expires', 0) <= self.s.clock():
            raise BoundaryError('FOLLOWUP_TEXT_EXPIRED')
        # These are exact business projections, never a style-model prompt.
        row['_followup_text'] = True
        output = dict(row, _response_immutable=True)
        self.am.notify(output, text=body, kind=kind, event_key=event_key)
        return None

    def error(self,row):
        # Static recovery must not relabel any stale source-derived bytes with
        # a new access epoch. Drop source/view state and include no user text.
        clean={key:value for key,value in row.items() if not key.startswith('_')}
        clean.update(_followup_text=True,_query_read=False,_query_denial=True)
        return self.publish(clean,'这次操作未完成，已有记录保留。请重新查看当前事项或完整预览。',kind='followup_text_error')

    def publish_action(self, row, *, object_id, version, snapshot, title, body,
                       actions, flow, expires=None):
        self.h.actor(row)
        if flow not in _FLOW_ACTIONS: raise BoundaryError('FOLLOWUP_TEXT_FLOW_INVALID')
        valid_until = min(row['expires'], self.s.clock()+600, expires or row['expires'])
        if valid_until <= self.s.clock(): raise BoundaryError('FOLLOWUP_TEXT_EXPIRED')
        code, nonce = secrets.token_hex(4).upper(), uuid.uuid4().hex
        verb, expected_action = _FLOW_ACTIONS[flow]
        enabled = expected_action in {a for a, _ in actions}
        text = title+'\n\n'+body
        if enabled:
            text += ('\n\n这段内容和接收人都合适的话，直接回复这条预览说“按这个发”即可。'
                     if flow=='followup' else '\n\n内容合适的话，请直接回复这条预览明确确认。')
        cancel = '取消发送' if flow == 'followup' else '取消操作'
        text += '\n需要修改哪处，也可以直接告诉我；暂时不发就回复这条预览说“先别发”。' if flow=='followup' else '\n暂不采用也可以直接告诉我。'
        if flow == 'followup':
            text += '\n修改后我会重新给你看完整预览；这次确认只对应上面这一条。'
        if len(text) > MAX_TEXT: raise BoundaryError('FOLLOWUP_TEXT_TOO_LARGE')
        snapshot = dict(snapshot, _text_action_enabled=enabled)
        fingerprint = digest(snapshot)
        render = dict(row, expires=valid_until, _followup_text=True,
                      _response_immutable=True)
        with self.s.tx() as c:
            # Only the latest proposal in this exact user/chat/thread scope can
            # be confirmed. Previous sent occurrences and their UUIDs survive.
            c.execute('UPDATE am_followup_text_actions SET superseded=1 WHERE scope=?',
                      (self.am.scope(row),))
            out = self.am.notify(render, text=text, kind='followup_text_preview',
                                 object_id=object_id, version=version,
                                 hash=fingerprint, conn=c)
            saved=c.execute('SELECT kind,body FROM outbox WHERE id=?',(out,)).fetchone()
            if not saved or saved['kind']!='text' or saved['body']!=text:
                raise BoundaryError('FOLLOWUP_TEXT_PREVIEW_CHANGED')
            c.execute('INSERT INTO am_cards VALUES(?,?,?,?,?,?,?,?,?,?,NULL)',
                (nonce, flow, object_id, version, row['user'], row['chat'],
                 canonical(snapshot), fingerprint, valid_until, out))
            c.execute('INSERT INTO am_followup_text_actions VALUES(?,?,?,?,?,?,?,?,?,?,?,?,0)',
                (code, nonce, self.am.scope(row), row['user'], row['chat'], flow,
                 text, digest(text), fingerprint, self.s.clock(), valid_until, out))
        row['_followup_text'] = True
        return nonce

    def _record(self, nonce):
        rows = self.s.rows('SELECT * FROM am_followup_text_actions WHERE nonce=?', (nonce,))
        return rows[0] if len(rows) == 1 else None

    def valid_delivery(self, output):
        rows = self.s.rows('SELECT * FROM am_followup_text_actions WHERE outbox_id=?', (output['id'],))
        if not rows: return None
        view = rows[0]
        return (not view['superseded'] and view['expires'] > self.s.clock()
                and digest(view['body']) == view['body_hash']
                and output.get('kind')=='text' and output.get('body')==view['body'])

    def verify_delivered(self, row, card, *, check_inbound=False):
        """Read both sides from the existing platform port; no synthetic click."""
        self.h.actor(row)
        view = self._record(card['nonce'])
        if not view or view['superseded'] or view['expires'] <= self.s.clock():
            raise BoundaryError('FOLLOWUP_TEXT_PREVIEW_EXPIRED')
        same_scope=view['scope']==self.am.scope(row)
        if (view['actor'], view['chat'], view['flow']) != (
                row['user'], row['chat'], card['flow']):
            raise BoundaryError('FOLLOWUP_TEXT_SCOPE_CHANGED')
        if (view['snapshot_hash'] != card['hash']
                or digest(json.loads(card['snapshot'])) != card['hash']
                or digest(view['body']) != view['body_hash']):
            raise BoundaryError('FOLLOWUP_TEXT_PREVIEW_CHANGED')
        outputs = self.s.rows("SELECT * FROM outbox WHERE id=? AND state='sent'", (view['outbox_id'],))
        if len(outputs) != 1 or not card['message_id'] or outputs[0]['message_id'] != card['message_id']:
            raise BoundaryError('FOLLOWUP_TEXT_PREVIEW_NOT_DELIVERED')
        if outputs[0]['kind']!='text' or outputs[0]['body']!=view['body']:
            raise BoundaryError('FOLLOWUP_TEXT_PREVIEW_CHANGED')
        delivered = self.am.g.reminders.port.read(card['message_id'])
        if (delivered.get('message_id'), delivered.get('chat_id'), delivered.get('sender_id')) != (
                card['message_id'], row['chat'], self.am.g.config.bot_open_id):
            raise BoundaryError('FOLLOWUP_TEXT_PREVIEW_NOT_VERIFIED')
        if delivered.get('deleted') or delivered.get('sender_type') not in ('app', 'bot') or _plain(delivered) != view['body']:
            raise BoundaryError('FOLLOWUP_TEXT_PREVIEW_NOT_VERIFIED')
        if check_inbound or not same_scope:
            actual = self.am.g.reminders.port.read(row['id'])
            if (actual.get('message_id'), actual.get('chat_id'), actual.get('sender_id'), actual.get('sender_type')) != (
                    row['id'], row['chat'], row['user'], 'user') or actual.get('deleted'):
                raise BoundaryError('FOLLOWUP_TEXT_APPROVAL_IDENTITY')
            original = _plain(actual)
            meta = self.am.source_meta(row)
            if (original.strip() != row['body'].strip()
                    or meta.get('message_type') != 'text'
                    or meta.get('fingerprint_kind') != 'raw-content-v1'
                    or meta.get('content_fingerprint') != digest({'text': original})):
                raise BoundaryError('FOLLOWUP_TEXT_APPROVAL_CHANGED')
            parent=actual.get('parent_id')
            if parent and parent!=card['message_id']:
                raise BoundaryError('FOLLOWUP_TEXT_REPLY_MISMATCH')
            if actual.get('root_id') and actual['root_id']!=card['message_id'] and parent!=card['message_id']:
                raise BoundaryError('FOLLOWUP_TEXT_REPLY_MISMATCH')
            # Feishu can create a thread when the owner replies to a p2p
            # preview. Only an independently read exact parent may bridge that
            # new thread to the original same app/tenant/owner/chat preview.
            if not same_scope and (parent!=card['message_id'] or
                    json.loads(view['scope'])[:4]!=json.loads(self.am.scope(row))[:4]):
                raise BoundaryError('FOLLOWUP_TEXT_SCOPE_CHANGED')
            event_at = actual.get('created_at')
            if (not isinstance(event_at, (int, float)) or isinstance(event_at, bool)
                    or abs(event_at-row['created']) > 0.001
                    or not delivered['created_at'] <= event_at < view['expires']
                    or event_at > self.s.clock()+30):
                raise BoundaryError('FOLLOWUP_TEXT_APPROVAL_TIME')
        # A replacement or cancellation during either platform read must not
        # authorize a stale full preview after readback returns.
        if self._record(card['nonce'])!=view:
            raise BoundaryError('FOLLOWUP_TEXT_PREVIEW_CHANGED')
        return view

    def confirm(self, row, verb, code):
        actor = self.h.actor(row)
        row['_followup_text'] = True
        records = self.s.rows('SELECT nonce FROM am_followup_text_actions WHERE code=?', (code,))
        if len(records) != 1: raise BoundaryError('FOLLOWUP_TEXT_PREVIEW_UNKNOWN')
        cards = self.s.rows('SELECT * FROM am_cards WHERE nonce=?', (records[0]['nonce'],))
        if len(cards) != 1: raise BoundaryError('FOLLOWUP_TEXT_PREVIEW_UNKNOWN')
        card = cards[0]
        self.verify_delivered(row, card, check_inbound=True)
        expected, action = _FLOW_ACTIONS[card['flow']]
        if verb in ('取消发送', '取消操作'):
            if (verb == '取消发送') != (card['flow'] == 'followup'):
                raise BoundaryError('FOLLOWUP_TEXT_ACTION_MISMATCH')
            if card['flow'] == 'followup':
                result = self.h.card_action(row, card, 'stop')
            else: result = '本次操作已取消，原有事项和记录保留。'
            self.s.execute('UPDATE am_followup_text_actions SET superseded=1 WHERE nonce=?', (card['nonce'],))
            return result
        if verb != expected or not json.loads(card['snapshot']).get('_text_action_enabled'):
            raise BoundaryError('FOLLOWUP_TEXT_ACTION_MISMATCH')
        if card['flow'] != 'followup':
            # Reuses exact source/version/import checks, after independent text
            # delivery verification; row remains a real text message throughout.
            return self.am.handle_business_card(row, card, action)
        binding = self.s.rows('SELECT * FROM followup_bindings WHERE preview_id=?', (card['object_id'],))
        if len(binding) != 1 or binding[0]['invalidated']:
            raise BoundaryError('FOLLOWUP_TEXT_PREVIEW_CHANGED')
        snap = json.loads(card['snapshot'])
        pairs = snap.get('rows')
        current = self.s.rows('SELECT content_hash FROM followup_previews WHERE tenant_id=? AND preview_id=?',
                              (actor.tenant_id, card['object_id']))
        if len(current) != 1 or pairs != [[card['object_id'], current[0]['content_hash']]]:
            raise BoundaryError('FOLLOWUP_TEXT_PREVIEW_CHANGED')
        requests = [ApprovalRequest(pairs[0][0], binding[0]['token'], pairs[0][1])]
        # Source/recipient gates execute again here, and once more at dispatch.
        results = self.h.coordinator.approve_batch(actor, requests)
        return self.h.approval_result_text(results)

    def _remember(self, row, items):
        self.s.execute('INSERT INTO am_followup_text_choices VALUES(?,?,?) ON CONFLICT(scope) DO UPDATE SET items=excluded.items,expires=excluded.expires',
            (self.am.scope(row), canonical(items), min(row['expires'], self.s.clock()+600)))

    def _item(self, row, reference):
        if reference.isdecimal():
            choices = self.s.rows('SELECT * FROM am_followup_text_choices WHERE scope=? AND expires>?',
                                  (self.am.scope(row), self.s.clock()))
            values = json.loads(choices[0]['items']) if len(choices) == 1 else []
            index = int(reference)-1
            if index < 0 or index >= len(values): raise BoundaryError('FOLLOWUP_TEXT_SELECTION_EXPIRED')
            item_id = values[index]
        else: item_id = reference
        item = self.am.items.get(item_id)
        if item['owner'] != row['user']: raise BoundaryError('OWNER_ONLY')
        return item

    def listing(self, row, page=0, bucket='action'):
        labels = {'action':'待处理', 'waiting':'待反馈', 'closed':'已关闭'}
        if bucket not in labels or type(page) is not int or page < 0:
            raise BoundaryError('FOLLOWUP_TEXT_PAGE_INVALID')
        with self.read_scope(row) as render:
            rows = []
            for item in self.s.rows('SELECT * FROM action_items WHERE owner=? ORDER BY created DESC,id', (self.h.owner,)):
                group, status = self.am.followup_state(item)
                if group == bucket: rows.append((item, status))
            if page*3 >= len(rows) and page: raise BoundaryError('FOLLOWUP_TEXT_PAGE_INVALID')
            selected = rows[page*3:(page+1)*3]
            self._remember(row, [i['id'] for i, _ in selected])
            headings={'action':'目前需要你处理的事项','waiting':'目前还在等待反馈的事项','closed':'已经关闭的事项'}
            lines = [headings[bucket]+('（接上文）' if page else '')+'：']
            for index, (item, status) in enumerate(selected, 1):
                title = item['description']
                if len(title) > 180: title = title[:180]+'…（查看事项可见完整内容）'
                lines.append(f'{index}. {title}\n{status}')
            if not selected: lines.append('当前没有查到相应的已登记事项。')
            elif (page+1)*3 < len(rows): lines.append('还有其他事项，需要的话我继续往下看。')
            else: lines.append('需要我展开哪件事，或看看对方的原话，直接告诉我就好。')
            lines.append('以上是目前已登记的跟进记录。')
            return self.publish(render, '\n\n'.join(lines))

    def item(self, row, item_id):
        with self.read_scope(row) as render:
            item = self._item(row, item_id)
            self._remember(row, [item['id']])
            source = json.loads(item['source'])
            names = '、'.join(p['name'] for p in source.get('target', {}).get('people', [])) or '负责人待核对'
            _, status = self.am.followup_state(item)
            lines = [item['description'], '负责人：'+names, '当前状态：'+status,
                     '交付要求：'+(item['requirements'] or '尚未明确'),
                     '业务完成：'+('已按核验依据关闭。' if item['state']=='closed' and item['close_evidence'] else '尚未核验完成。'),
                     '你可以接着问我对方有没有回复，或让我展开原话。']
            incomplete = self.h.incomplete_import(item['id'])
            if incomplete: lines.append('导入尚未完成，请先核对候选和历史；不会发送跟进。')
            elif item['state'] not in ('closed', 'cancelled'):
                lines.append('需要我问一下进展的话，告诉我想私聊还是在原群里问；发送前我会把对象和完整正文给你看。')
                if self.s.rows('SELECT 1 FROM action_semantics WHERE item_id=?', (item['id'],)):
                    lines.append('如果依据变了，我们先重新核对。')
            return self.publish(render, '\n\n'.join(lines))

    def status(self, row, item_id):
        # The explicit status command joins the native follow-up records to the
        # existing expression presenter. Off keeps the established full view.
        # Capture/prepare happen outside read_scope; no model work under the
        # AM mutex or information transaction. Fastpath keeps this navigation
        # usable during a resource window without loading a model.
        from am_style.contracts import ContractError
        expression = self.h.expression
        if expression.presenter.mode == 'off':
            return self.item(row, item_id)
        try:
            prepared = expression.prepare(row, item_id, fastpath=True)
        except ContractError:
            # Legacy records or oversized details need the original full view.
            # Re-read it now, never publish bytes from an earlier snapshot.
            return self.item(row, item_id)
        expression.enqueue(prepared)
        return None

    def history(self, row, item_id, page=0):
        with self.read_scope(row) as render:
            item = self._item(row, item_id)
            if type(page) is not int or page < 0: raise BoundaryError('FOLLOWUP_TEXT_PAGE_INVALID')
            records = list(reversed(self.am.items.history(item['id'], include_raw=True)))
            if page >= len(records) and page: raise BoundaryError('FOLLOWUP_TEXT_PAGE_INVALID')
            expiry = self.am.items.feedback_expiry(item['id'])
            if expiry: render['expires'] = min(render['expires'], expiry)
            names = {p['id']:p['name'] for p in json.loads(item['source']).get('target', {}).get('people', [])}
            lines = ['反馈记录 · '+item['description']]
            # One complete record per page; no raw-text truncation disguised
            # as a complete preview. Existing original retention remains final.
            if records:
                h = records[page]
                labels = {'received':'已回应', 'reported_update':'自报进展，待核对'}
                lines.append(names.get(h['actor'], '授权反馈人')+'｜'+labels.get(h['kind'], h['kind'])+'｜'+datetime.fromtimestamp(h['created'], TZ).strftime('%Y-%m-%d %H:%M:%S'))
                lines.append(h['body'] or '无补充')
                lines.append('这条回复不代表业务已核验完成。')
                if h['applied']:
                    lines.append('这条可以标记为已阅，事项仍保留待核对状态。')
                self._remember(row, [item['id']])
                if page+1 < len(records): lines.append('还有更早的反馈，需要的话我接着展开。')
            else: lines.append('尚无已关联反馈。')
            result = self.publish(render, '\n\n'.join(lines))
            if records and records[page]['applied']:
                snapshot = {'item':item['id'],'version':item['version'],'feedback':records[page]['id']}
                self.s.execute('INSERT INTO am_followup_text_choices VALUES(?,?,?) ON CONFLICT(scope) DO UPDATE SET items=excluded.items,expires=excluded.expires',
                    (self.am.scope(row)+':feedback_seen',canonical(snapshot),min(render['expires'],self.s.clock()+600)))
            return result

    def seen(self, row, item_id):
        saved=self.s.rows('SELECT items FROM am_followup_text_choices WHERE scope=? AND expires>?',
            (self.am.scope(row)+':feedback_seen',self.s.clock()))
        snapshot=json.loads(saved[0]['items']) if len(saved)==1 else {}
        if snapshot.get('item')!=item_id:raise BoundaryError('FOLLOWUP_FEEDBACK_VIEW_EXPIRED')
        # Original owner, item-version and exact-latest-feedback checks remain
        # authoritative; a newer feedback message invalidates this view.
        return self.am.ux_extra(row,dict(snapshot,page='feedback_seen'),{}, {})

    def notice(self, row, item, summary, event):
        self._remember(row, [item['id']])
        return self.publish(row, summary+'\n\n需要的话，我可以展开对方的原话。自报内容仍待核对。',
                            kind='feedback_owner', event_key=event)

    def handle(self, row):
        text = row['body'].strip()
        if not wants_text(text): return False, None
        self._owner(row); row['_followup_text'] = True
        explicit=natural_action(text) or text
        if explicit in _REPLY_ACTIONS:
            parent=self.am.source_meta(row).get('parent')
            records=self.s.rows('SELECT t.code,t.scope FROM am_followup_text_actions t JOIN am_cards c ON c.nonce=t.nonce '
                'WHERE c.message_id=? AND t.actor=? AND t.chat=? AND t.superseded=0 AND t.expires>?',
                (parent,row['user'],row['chat'],self.s.clock())) if parent else []
            records=[r for r in records if json.loads(r['scope'])[:4]==json.loads(self.am.scope(row))[:4]]
            if len(records)!=1:raise BoundaryError('FOLLOWUP_TEXT_REPLY_MISMATCH')
            return True,self.confirm(row,explicit,records[0]['code'])
        match = _APPROVAL.fullmatch(text)
        if match: return True, self.confirm(row, match[1], match[2])
        match = re.fullmatch(r'我的跟进(?:\s+(待处理|待反馈|已关闭))?(?:\s+(\d+))?', text)
        if match:
            return True, self.listing(row, int(match[2] or '1')-1,
                                     {'待处理':'action', '待反馈':'waiting', '已关闭':'closed'}.get(match[1], 'action'))
        head, separator, body = text.partition('｜')
        parts = head.split()
        if len(parts) < 2: raise BoundaryError('FOLLOWUP_TEXT_COMMAND_INVALID')
        if parts[0] == '采用候选':
            self.h.candidate_text(row, parts[1]); return True, None
        item = self._item(row, parts[1])
        if parts[0] == '查看跟进': return True, self.item(row, item['id'])
        if parts[0] == '跟进状态':
            if len(parts) != 2: raise BoundaryError('FOLLOWUP_TEXT_COMMAND_INVALID')
            return True, self.status(row, item['id'])
        if parts[0] == '跟进反馈':
            if len(parts) > 3 or len(parts) == 3 and not parts[2].isdecimal(): raise BoundaryError('FOLLOWUP_TEXT_COMMAND_INVALID')
            return True, self.history(row, item['id'], int(parts[2])-1 if len(parts)==3 else 0)
        if parts[0] == '反馈已阅':
            if len(parts)!=2:raise BoundaryError('FOLLOWUP_TEXT_COMMAND_INVALID')
            return True,self.seen(row,item['id'])
        if parts[0] == '重审依据':
            self.h.source_review_text(row, item['id']); return True, None
        if parts[0] in ('群聊跟进', '私聊跟进', '修改跟进'):
            if parts[0] == '修改跟进':
                if len(parts) != 3 or parts[2] not in ('群聊', '私聊') or not separator or not body.strip():
                    raise BoundaryError('FOLLOWUP_TEXT_COMMAND_INVALID')
                channel = 'group' if parts[2] == '群聊' else 'recipient_p2p'
            else:
                if len(parts) != 2: raise BoundaryError('FOLLOWUP_TEXT_COMMAND_INVALID')
                channel = 'group' if parts[0] == '群聊跟进' else 'recipient_p2p'
            self.h.preview_text(row, item['id'], channel, body=body if separator else None)
            return True, None
        raise BoundaryError('FOLLOWUP_TEXT_COMMAND_INVALID')

    def cleanup(self):
        now = self.s.clock()
        # Identities/hashes survive for idempotency; source-derived bytes do not.
        self.s.execute("UPDATE am_followup_text_actions SET body='',superseded=1 WHERE expires<=? AND body!=''", (now,))
        self.s.execute('DELETE FROM am_followup_text_choices WHERE expires<=?', (now,))
