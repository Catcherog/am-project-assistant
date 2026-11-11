"""Owner-facing AM functions, called by the original consumer's business loop."""
from __future__ import annotations
import json
import re
import uuid
import time
from datetime import datetime
from zoneinfo import ZoneInfo
from fs_agent.config import BoundaryError
from fs_agent.message_text import normalize_message
from fs_agent.project_catalog import canonical,digest
from fs_agent import request_timing as timing
from group_ledger import render_project,select_projects,SOURCE,SHEET
from am_business import BusinessCommands
from am_updates import UpdateCommands
from am_ux import UxMixin,MENU_KEYS
from am_views import ViewsMixin
from am_briefing import BriefingMixin
from am_presentation import PresentationMixin
from am_documents import DocumentViewsMixin
from am_query import QueryMixin, SourceLoading, ENTRY_TEXT, READ_PAGES
from am_group_query import GroupQueryMixin
from am_direct_query import DirectQueryMixin
from query_rules import deterministic_intent,identifiers,resolve_identifiers,fields_for,choice_number
from am_followup_text import wants_text, natural_action

DEFAULT_FLAGS={'query':False,'snapshots':False,'items':False,'scheduler':False,'progress_collection':False,'ledger_write':False,'attachments':False,'precheck':False}

# Exact owner navigation, not a natural-language task classifier. Threaded
# replies and active collection input keep their existing routing below.
FOLLOWUP_ENTRY_TEXT=frozenset(('我的跟进','我的跟进？','我的跟进?','我的跟进。'))


class AmSelfService(DirectQueryMixin,GroupQueryMixin,QueryMixin,PresentationMixin,DocumentViewsMixin,BriefingMixin,ViewsMixin,UxMixin,BusinessCommands,UpdateCommands):
    def __init__(self,gateway,ledger,*,flags=None,calendar=None,metadata_reader=None,execution_policy=None,query_settings=None,conversation_settings=None,harness_settings=None):
        self.g,self.s,self.ledger=gateway,gateway.store,ledger
        if flags and (set(flags)-set(DEFAULT_FLAGS) or any(type(v) is not bool for v in flags.values())):raise BoundaryError('AM_FEATURE_FLAGS_INVALID')
        self.flags=dict(DEFAULT_FLAGS,**(flags or {}))
        self.owner=gateway.config.owner
        self.items=None;self.scheduler=None;self.updates=None
        timing.initialize(self.s)
        with self.s.lock:
            self.s.conn.executescript('''
            CREATE TABLE IF NOT EXISTS am_inbox(
              id TEXT PRIMARY KEY,app TEXT NOT NULL,tenant TEXT NOT NULL,user TEXT NOT NULL,chat TEXT NOT NULL,
              chat_type TEXT NOT NULL,created REAL NOT NULL,expires REAL NOT NULL,body TEXT NOT NULL,
              kind TEXT NOT NULL,state TEXT NOT NULL,error TEXT);
            CREATE TABLE IF NOT EXISTS am_source_meta(id TEXT PRIMARY KEY,metadata TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS am_query_refs(message_id TEXT PRIMARY KEY,scope TEXT NOT NULL,project_ref TEXT NOT NULL,expires REAL NOT NULL);
            CREATE TABLE IF NOT EXISTS am_sessions(
              scope TEXT PRIMARY KEY,focus TEXT,pending TEXT,version INTEGER NOT NULL,expires REAL NOT NULL);
            CREATE TABLE IF NOT EXISTS am_notice_keys(notice_key TEXT PRIMARY KEY,outbox_id TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS am_notifications(
              outbox_id TEXT PRIMARY KEY,kind TEXT NOT NULL,object_id TEXT,version INTEGER,hash TEXT);
            CREATE TABLE IF NOT EXISTS am_cards(
              nonce TEXT PRIMARY KEY,flow TEXT NOT NULL,object_id TEXT NOT NULL,version INTEGER NOT NULL,
              actor TEXT NOT NULL,chat TEXT NOT NULL,snapshot TEXT NOT NULL,hash TEXT NOT NULL,
              expires REAL NOT NULL,outbox_id TEXT,message_id TEXT);
            ''')
        self.s.execute("UPDATE am_inbox SET state='interrupted' WHERE state='processing'")
        self.s.execute("UPDATE am_inbox SET state='delivery_unknown' WHERE state='sending'")
        self.init_business(calendar)
        self.init_updates(metadata_reader=metadata_reader,execution_policy=execution_policy)
        self.init_ux()
        self.init_presentation()
        self.init_query(query_settings)
        self.init_group_query()
        self.init_direct_query()
        self.conversation=None
        self.information=None
        if conversation_settings is not None:
            if not isinstance(conversation_settings,dict) or type(conversation_settings.get('enabled',False)) is not bool:
                raise BoundaryError('AM_CONVERSATION_SETTINGS_INVALID')
            if conversation_settings.get('enabled'):
                from am_conversation.legacy import LegacyConversationBridge
                self.conversation=LegacyConversationBridge(self,conversation_settings)
        self.harness=None
        if harness_settings is not None:
            from am_harness.config import HarnessConfig
            configuration=HarnessConfig.from_mapping(harness_settings)
            if configuration.enabled:
                from am_harness_host import HarnessHost
                self.harness=HarnessHost(self,harness_settings)
        gateway.am_self=self

    def scope(self,row):
        keys=[row['tenant'],row['app'],row['chat'],row['user']]
        thread=row.get('thread') or self.source_meta(row).get('thread')
        return canonical(keys+[thread] if thread else keys)

    def source_meta(self,row):
        if '_source_meta' in row:return row['_source_meta']
        rows=self.s.rows('SELECT metadata FROM am_source_meta WHERE id=?',(row.get('id'),))
        return json.loads(rows[0]['metadata']) if rows else {}

    def message_meta(self,message):
        # normalize_message has validated the format before this is called. The
        # fingerprint covers the original content, not the stripped route text.
        content=json.loads(message['content']) if isinstance(message['content'],str) else message['content']
        return {'thread':message.get('thread_id'),'parent':message.get('parent_id'),'root':message.get('root_id'),
                'message_type':message['message_type'],'content_fingerprint':digest(content),'fingerprint_kind':'raw-content-v1'}

    def wants(self,text):
        if wants_text(text):return True
        if text.strip() in FOLLOWUP_ENTRY_TEXT:return True
        if self.information is not None and (text == '/info' or text.startswith('/info ')):return True
        if self.conversation is not None and self.conversation.wants(text):return True
        if text.strip()=='/query-probe':return True
        if text.strip() in ENTRY_TEXT:return True
        if text.strip() in ('/am','/menu','/help','/status'):return True
        if text.startswith(('/project','/changes','/todo','/briefing','/item','/plan','/update','/precheck','/mapping','/followup')):return True
        if text.startswith('/progress ') and text.strip()!='/progress sample':return True
        if text.startswith(('今天我需要跟进什么','我的待办','查看变化','项目变化','变化简报','订阅简报','取消订阅','新增跟进','查看跟进','更新需求')):return True
        if text.startswith('哪些项目') and re.search('待确认|未明确|缺少|资料',text):return True
        return self.flags['query'] and deterministic_intent(text)=='project'

    def ingest(self,payload):
        received=self.s.clock();started=time.monotonic()
        result=self._ingest(payload)
        if result[1]:
            h,e=payload['header'],payload['event']
            ident=e['message']['message_id'] if h['event_type']=='im.message.receive_v1' else h['event_id']
            provider=getattr(self.g,'provider',None);lock=getattr(provider,'lock',None)
            busy=bool(lock and lock.locked())
            self.s.execute('INSERT OR IGNORE INTO am_request_timings VALUES(?,?,?,NULL,NULL,NULL,NULL,NULL,?,?)',
                           (ident,received,self.s.clock(),int(busy),canonical({'ingress':{'seconds':time.monotonic()-started,'calls':1}})))
        return result

    def _ingest(self,payload):
        try:
            followup_reply=self.followup.ingest_reply(payload)
            if followup_reply is not None:return followup_reply
            meta=None;collection=None
            h,e=payload['header'],payload['event'];config=self.g.config
            if h['app_id']!=config.app_id or h['tenant_key']!=config.tenant_key:return False,False
            if h['event_type']=='application.bot.menu_v6':
                user=e['operator']['operator_id']['open_id'];key=e['event_key']
                if key not in MENU_KEYS:return True,False
                created=float(e['timestamp'])
                if created>10**12:created/=1000
                row={'id':h['event_id'],'app':config.app_id,'tenant':config.tenant_key,'user':user,
                     'chat':'menu:'+user,'chat_type':'p2p','created':created,'kind':'menu','body':MENU_KEYS[key]}
                prior=self.s.rows("SELECT chat FROM am_inbox WHERE user=? AND chat_type='p2p' AND chat NOT LIKE 'menu:%' ORDER BY created DESC LIMIT 1",(user,))
                if prior:row['chat']=prior[0]['chat']
            elif h['event_type']=='card.action.trigger':
                value=e['action']['value']
                if value.get('flow') not in ('am_self','am_group_open'):return False,False
                row={'id':h['event_id'],'app':config.app_id,'tenant':config.tenant_key,'user':e['operator']['open_id'],
                     'chat':e['context']['open_chat_id'],'chat_type':'p2p','created':self.s.clock(),'kind':'card',
                     'body':canonical({'value':value,'source_message':e['context']['open_message_id'],
                                       'form_value':e['action'].get('form_value'), 'action_meta':{k:e['action'].get(k) for k in ('name','tag','timezone')},
                                       'thread':e['context'].get('open_thread_id')})}
                if len(row['body'])>20000:return True,False
                if value.get('flow')=='am_group_open':
                    row.update(kind='group_open',chat_type='group')
                else:
                    saved=self.s.rows('SELECT snapshot FROM am_cards WHERE nonce=?',(value.get('nonce'),))
                    if saved:
                        source=json.loads(saved[0]['snapshot']).get('data',{}).get('query_group')
                        if source:meta={'query_group':source}
            elif h['event_type']=='im.message.receive_v1':
                m,sender=e['message'],e['sender']
                if sender['sender_type']!='user' or m['message_type'] not in ('text','post','file'):return False,False
                if sender.get('tenant_key',config.tenant_key)!=config.tenant_key:return True,False
                text=normalize_message(m,bot_open_id=config.bot_open_id,max_chars=6000).text.strip() if m['message_type']!='file' else ''
                meta=self.message_meta(m)
                user=sender['sender_id']['open_id']
                session={'app':config.app_id,'tenant':config.tenant_key,'user':user,'chat':m['chat_id'],'thread':meta.get('thread')}
                if m['chat_type']=='p2p' and user==self.owner:
                    if text=='/chat':self.ux_cancel_collection(session,all_chat=True)
                    elif not text.startswith('/'):
                        collection=self.ux_collection_at_receipt(session)
                        if not collection and self.ux_collection_in_chat(session):collection={'token':'scope-mismatch'}
                if m['message_type']=='file' and not collection:return False,False
                if text.startswith('/feedback '):
                    match=re.fullmatch(r'/feedback ([a-f0-9]{32}) (收到|处理中|预计|协助|提交|非本人)[｜|](.{1,2000})',text)
                    if not match or not m.get('parent_id'):return True,False
                    if m['chat_type']=='group' and not normalize_message(m,bot_open_id=config.bot_open_id,max_chars=6000).bot_mentioned:return True,False
                    occurrence=self.scheduler.occurrence(match[1]);saved=json.loads(occurrence['payload'])
                    body={'value':{'feedback':match[1],'nonce':saved.get('feedback_nonce'),'action':{'收到':'received','处理中':'processing','预计':'eta','协助':'help','提交':'submitted','非本人':'not_mine'}[match[2]]},'source_message':m['parent_id'],'detail':match[3]}
                    row={'id':m['message_id'],'app':config.app_id,'tenant':config.tenant_key,'user':user,'chat':m['chat_id'],'chat_type':m['chat_type'],'created':int(m['create_time'])/1000,'kind':'card','body':canonical(body)}
                    row['expires']=min(row['created'],self.s.clock())+86400
                    if row['expires']<=self.s.clock() or row['created']>self.s.clock()+60:return True,False
                    with self.s.tx() as c:
                        n=c.execute("INSERT OR IGNORE INTO am_inbox VALUES(?,?,?,?,?,?,?,?,?,?,'received',NULL)",tuple(row[k] for k in ('id','app','tenant','user','chat','chat_type','created','expires','body','kind'))).rowcount
                    return True,bool(n)
                if m['chat_type']=='group':
                    if not normalize_message(m,bot_open_id=config.bot_open_id,max_chars=6000).bot_mentioned:return True,False
                    updating=self.flags['progress_collection'] and text.startswith(('/update','更新需求','更新项目'))
                    if not updating and not self.flags['query']:return False,False
                    natural=not updating and self.conversation is not None and self.conversation.group_conversations and self.conversation.wants(text)
                    row={'id':m['message_id'],'app':config.app_id,'tenant':config.tenant_key,'user':user,'chat':m['chat_id'],'chat_type':'group','created':int(m['create_time'])/1000,'kind':'update' if updating else 'conversation' if natural else 'group_query','body':text}
                    row['expires']=min(row['created'],self.s.clock())+86400
                    if row['expires']<=self.s.clock() or row['created']>self.s.clock()+60:return True,False
                    with self.s.tx() as c:
                        if c.execute("SELECT count(*) FROM am_inbox WHERE state='received'").fetchone()[0]>=32 and not self.harness_control_admission(row,c):raise BoundaryError('AM_QUEUE_FULL')
                        n=c.execute("INSERT OR IGNORE INTO am_inbox VALUES(?,?,?,?,?,?,?,?,?,?,'received',NULL)",tuple(row[k] for k in ('id','app','tenant','user','chat','chat_type','created','expires','body','kind'))).rowcount
                        if n:c.execute('INSERT INTO am_source_meta VALUES(?,?)',(row['id'],canonical(meta)))
                    return True,bool(n)
                if m['chat_type']!='p2p':return False,False
                if not collection and not self.wants(text):
                    session={'app':config.app_id,'tenant':config.tenant_key,'user':user,'chat':m['chat_id'],'thread':meta.get('thread')}
                    pending=self.s.rows('SELECT 1 FROM am_sessions WHERE scope=? AND pending IS NOT NULL AND expires>?',(self.scope(session),self.s.clock()))
                    from am_query import field_followup
                    focus=self.s.rows('SELECT 1 FROM am_sessions WHERE scope=? AND focus IS NOT NULL AND expires>?',(self.scope(session),self.s.clock()))
                    if not (pending and choice_number(text)) and not (field_followup(text) and (focus or meta.get('parent'))):return False,False
                exact_native_quote=bool(meta.get('parent') and self.s.rows(
                    'SELECT 1 FROM am_followup_text_actions t JOIN am_cards c ON c.nonce=t.nonce WHERE c.message_id=? AND t.actor=? AND t.chat=?',
                    (meta['parent'],user,m['chat_id'])))
                followup_text=wants_text(text) and (natural_action(text) is None or exact_native_quote)
                followup_navigation=(user==self.owner and (followup_text or text in FOLLOWUP_ENTRY_TEXT)
                                     and m['message_type']=='text'
                                     and (followup_text or not meta.get('parent') and not meta.get('root')))
                row={'id':m['message_id'],'app':config.app_id,'tenant':config.tenant_key,'user':user,'chat':m['chat_id'],
                     'chat_type':'p2p','created':int(m['create_time'])/1000,'kind':'collection' if collection else 'text' if followup_navigation else 'conversation' if self.conversation is not None and self.conversation.wants(text) else 'text',
                     'body':canonical(json.loads(m['content']) if isinstance(m['content'],str) else m['content']) if m['message_type']=='file' else text}
            else:return False,False
            row['expires']=min(row['created'],self.s.clock())+86400
            if row['created']>self.s.clock()+60 or row['expires']<=self.s.clock():return True,False
            with self.s.tx() as c:
                if c.execute('SELECT 1 FROM am_inbox WHERE id=?',(row['id'],)).fetchone():return True,False
                if c.execute("SELECT count(*) FROM am_inbox WHERE state='received'").fetchone()[0]>=32 and not self.harness_control_admission(row,c):raise BoundaryError('AM_QUEUE_FULL')
                c.execute("INSERT INTO am_inbox VALUES(?,?,?,?,?,?,?,?,?,?,'received',NULL)",tuple(row[k] for k in ('id','app','tenant','user','chat','chat_type','created','expires','body','kind')))
                if meta:c.execute('INSERT INTO am_source_meta VALUES(?,?)',(row['id'],canonical(meta)))
                if collection:c.execute('INSERT INTO am_ux_receipts VALUES(?,?)',(row['id'],canonical(collection)))
            return True,True
        except (KeyError,TypeError,ValueError):return False,False

    def harness_control_admission(self,row,conn):
        harness=getattr(self,'harness',None)
        if harness is None or not harness.durable or not harness.config.control_enabled or row.get('kind')!='conversation':return False
        from am_harness.controls import parse_control
        if parse_control(row.get('body')) is None:return False
        # Reserve bounded capacity for explicit controls, even with a full normal queue.
        return conn.execute("SELECT count(*) FROM am_inbox WHERE state='received'").fetchone()[0]<64

    def notify(self,row,text=None,*,card=None,kind='status',object_id=None,version=None,hash=None,conn=None,event_key=None):
        if conn is None:
            with self.s.tx() as c:return self.notify(row,text,card=card,kind=kind,object_id=object_id,version=version,hash=hash,conn=c,event_key=event_key)
        notice_key=digest([event_key,row['app'],row['tenant'],row['user'],kind]) if event_key else None
        if notice_key:
            old=conn.execute('SELECT outbox_id FROM am_notice_keys WHERE notice_key=?',(notice_key,)).fetchone()
            if old:return old[0]
        binding=row.get('_document_binding')
        scoped_source=None
        if binding:
            from fs_agent.document_view_binding import source_fence
            scoped_source=source_fence(self.g.logic,row,binding)
            row=dict(row,expires=min(row['expires'],binding['expires']))
        if scoped_source is not None and self.information is None:
            raise BoundaryError('DOCUMENT_SHARED_EVIDENCE_VERIFIER_REQUIRED')
        source_version=row.get('_source_access_version')
        if self.information is not None:
            current=self.information.store.access_version()
            if scoped_source is None and source_version is not None and source_version!=current:raise BoundaryError('SOURCE_ACCESS_CHANGED')
            if source_version is None:source_version=current
        from am_response.topology import notice_family
        presented_row = dict(row, _response_kind=kind, _response_family=notice_family(kind),
                             _response_object=object_id, _response_version=version, _response_payload_hash=hash)
        out=self.s.add_outbox(presented_row,text=text,card=card,private=not row.get('_public_query'),conn=conn)
        if binding:
            conn.execute('INSERT INTO am_document_view_outputs VALUES(?,?,?)',(out,canonical(binding),binding['expires']))
        if scoped_source is not None:
            from fs_agent.document_view_binding import bind_source_output
            output_fence=bind_source_output(dict(conn.execute('SELECT * FROM outbox WHERE id=?',(out,)).fetchone()),scoped_source)
            conn.execute('INSERT INTO am_document_source_outputs VALUES(?,?)',(out,output_fence))
        if source_version is not None:
            conn.execute('INSERT INTO am_source_deliveries VALUES(?,?)',(out,source_version))
        if row.get('_direct_return'):conn.execute("UPDATE outbox SET state='callback_pending' WHERE id=?",(out,))
        source_group=self.query_group(row)
        if source_group:
            reply_to=(json.loads(row['body']).get('source_message') if row['kind']=='group_open' else row['id']) if row.get('_public_query') else None
            conn.execute('INSERT INTO am_group_delivery VALUES(?,?,?,?,?)',(out,source_group,reply_to,self.source_meta(row).get('thread'),'public' if row.get('_public_query') else 'private'))
        conn.execute('INSERT OR IGNORE INTO am_latency_anchors(event_id) VALUES(?)',(row['id'],))
        conn.execute('UPDATE am_latency_anchors SET outbox_ready=? WHERE event_id=?',(self.s.clock(),row['id']))
        conn.execute('INSERT INTO am_outbox_events VALUES(?,?)',(out,row['id']))
        if notice_key:conn.execute('INSERT INTO am_notice_keys VALUES(?,?)',(notice_key,out))
        if kind=='status' and text and conn.execute('SELECT 1 FROM am_query_refs WHERE message_id=? AND expires>?',(row['id'],self.s.clock())).fetchone():
            kind='query';object_id=row['id']
        conn.execute('INSERT INTO am_notifications VALUES(?,?,?,?,?)',(out,kind,object_id,version,hash))
        if row.get('_query_read') or row.get('_query_denial'):
            conn.execute('INSERT INTO am_query_deliveries VALUES(?,?,?)',(out,digest(row.get('_query_grant',[])),int(bool(row.get('_query_denial')))))
        return out

    def card(self,row,*,object_id,version,snapshot,title,body,actions,flow,expires=None):
        nonce=uuid.uuid4().hex;fingerprint=digest(snapshot);expiry=min(expires or row['expires'],row['expires'])
        buttons=[{'tag':'button','text':{'tag':'plain_text','content':label},'type':'primary' if index==0 else 'default',
                  'value':{'flow':'am_self','nonce':nonce,'action':action}} for index,(action,label) in enumerate(actions)]
        card={'config':{'wide_screen_mode':True},'header':{'title':{'tag':'plain_text','content':title}},
              'elements':[{'tag':'div','text':{'tag':'plain_text','content':body}},{'tag':'action','actions':buttons}]}
        if flow in ('followup','followup_import','followup_source_review'):
            from fs_agent.card_ui import card as native_card,paragraph,button,bind_rendered_card
            card=bind_rendered_card(native_card(title,[paragraph(body)]+[
                button(label,{'flow':'am_self','nonce':nonce,'action':action}) for action,label in actions]))
        with self.s.tx() as c:
            out=self.notify(row,card=card,kind='card',object_id=object_id,version=version,hash=fingerprint,conn=c)
            c.execute('INSERT INTO am_cards VALUES(?,?,?,?,?,?,?,?,?,?,NULL)',(nonce,flow,object_id,version,row['user'],row['chat'],canonical(snapshot),fingerprint,expiry,out))
        return nonce

    def valid_notification(self,row):
        notices=self.s.rows('SELECT * FROM am_notifications WHERE outbox_id=?',(row['id'],))
        if not notices:return None
        if any(n['kind']=='document_result' for n in notices) and not self.s.rows('SELECT 1 FROM am_document_view_outputs WHERE outbox_id=?',(row['id'],)):
            return False
        if self.information is None and self.s.rows('SELECT 1 FROM am_document_source_outputs WHERE outbox_id=?',(row['id'],)):
            return False
        from fs_agent.document_view_binding import valid_output
        if not valid_output(self.g.logic,row):return False
        if self.view_delivery(row) is False:return False
        if self.followup.text_ui.valid_delivery(row) is False:return False
        expression=getattr(self.followup,'expression',None)
        if expression is not None and expression.valid_notification(row) is False:return False
        cards=self.s.rows('SELECT * FROM am_cards WHERE outbox_id=?',(row['id'],))
        if cards and cards[0]['expires']<=self.s.clock():return False
        return True

    def authorized_notification(self,row):
        query=self.query_delivery_authorized(row)
        if query is not None:return query
        notes=self.s.rows("SELECT object_id FROM am_notifications WHERE outbox_id=? AND kind IN ('feedback_ack','feedback_correction')",(row['id'],))
        if not notes:return None
        try:
            o=self.scheduler.occurrence(notes[0]['object_id']);p=json.loads(o['payload']);item=self.items.get(o['item_id'])
            if row['app']!=self.g.config.app_id or row['tenant']!=self.g.config.tenant_key or row['expires']<=self.s.clock() or row['user'] not in json.loads(item['assignees']):return False
            self.ledger.port.guard(p['target']['payload']['group']['id'],row['user'])
            return True
        except Exception:return False

    def delivered(self,c,row,message_id):
        self.followup.expression.delivered(c,row,message_id)
        projection = getattr(self.s, 'response_projection', None)
        if projection is not None:
            projection.receipt(c, row, message_id)
        if getattr(self,'conversation',None) is not None:
            try:self.conversation.delivered(row,message_id,conn=c)
            except Exception:pass
        self.group_delivered(c,row,message_id)
        c.execute('UPDATE am_query_probes SET message_id=? WHERE outbox_id=?',(message_id,row['id']))
        c.execute('UPDATE am_cards SET message_id=? WHERE outbox_id=?',(message_id,row['id']))
        output=c.execute('SELECT * FROM am_view_outputs WHERE outbox_id=?',(row['id'],)).fetchone()
        if output:
            c.execute('UPDATE am_interactions SET message_id=?,pending_outbox=NULL WHERE id=? AND view_version=?',(message_id,output['interaction_id'],output['version']))
            metrics=json.loads(output['metrics']);origin=c.execute('SELECT received FROM am_request_timings WHERE event_id=?',(metrics['origin_event'],)).fetchone()
            metrics.update(delivered_at=self.s.clock(),actionable_seconds=max(0,self.s.clock()-origin[0]) if origin else None,operation='update' if output['target_message'] else 'create')
            c.execute('UPDATE am_view_outputs SET metrics=? WHERE outbox_id=?',(canonical(metrics),row['id']))
            card=c.execute('SELECT snapshot FROM am_cards WHERE outbox_id=?',(row['id'],)).fetchone()
            snap=json.loads(card['snapshot']) if card else {}
            project=snap.get('data',{}).get('opened_project')
            if project:
                scope=c.execute('SELECT scope FROM am_interactions WHERE id=?',(output['interaction_id'],)).fetchone()[0]
                c.execute('INSERT INTO am_query_recent VALUES(?,?,?,?) ON CONFLICT(scope,project) DO UPDATE SET opened=excluded.opened,expires=excluded.expires',(scope,project,self.s.clock(),min(row['expires'],self.s.clock()+86400)))
        ref=c.execute("SELECT q.* FROM am_notifications n JOIN am_query_refs q ON q.message_id=n.object_id WHERE n.outbox_id=? AND n.kind='query' AND q.expires>?",(row['id'],self.s.clock())).fetchone()
        if ref:c.execute('INSERT OR IGNORE INTO am_query_refs VALUES(?,?,?,?)',(message_id,ref['scope'],ref['project_ref'],ref['expires']))

    def validate_card(self,row):
        body=json.loads(row['body']);value=body['value']
        cards=self.s.rows('SELECT * FROM am_cards WHERE nonce=?',(value.get('nonce'),))
        if not cards:raise BoundaryError('CARD_UNKNOWN')
        c=cards[0]
        if self.followup.text_ui._record(c['nonce']):
            raise BoundaryError('FOLLOWUP_TEXT_ORIGINAL_MESSAGE_REQUIRED')
        if c['chat']=='menu:'+row['user'] and c['actor']==row['user'] and c['message_id']==body['source_message'] and c['expires']>self.s.clock():
            # Menu events have no chat ID. Bind the first callback to the actual
            # bot message after a platform read; never fabricate a message event.
            message=self.g.reminders.port.read(c['message_id'])
            if message['chat_id']!=row['chat'] or message['sender_id']!=self.g.config.bot_open_id:raise BoundaryError('CARD_IDENTITY_OR_VERSION_INVALID')
            snap=json.loads(c['snapshot']);ident=snap.get('interaction_id')
            if ident:
                scope=self.scope(row)
                with self.s.tx() as tx:
                    tx.execute('UPDATE am_interactions SET scope=? WHERE id=?',(scope,ident))
                    tx.execute('DELETE FROM am_navigation WHERE interaction_id=?',(ident,))
                    tx.execute('INSERT INTO am_navigation VALUES(?,?) ON CONFLICT(scope) DO UPDATE SET interaction_id=excluded.interaction_id',(scope,ident))
            self.s.execute('UPDATE am_cards SET chat=? WHERE nonce=?',(row['chat'],c['nonce']));c=dict(c,chat=row['chat'])
        if c['actor']!=row['user'] or c['chat']!=row['chat'] or not c['message_id'] or c['message_id']!=body['source_message'] or c['expires']<=self.s.clock():raise BoundaryError('CARD_IDENTITY_OR_VERSION_INVALID')
        if digest(json.loads(c['snapshot']))!=c['hash']:raise BoundaryError('CARD_SNAPSHOT_CHANGED')
        if c['flow']=='ux' and body.get('thread') and body['thread']!=json.loads(c['snapshot']).get('thread'):raise BoundaryError('CARD_IDENTITY_OR_VERSION_INVALID')
        if c['flow']=='ux':
            snap=json.loads(c['snapshot'])
            from fs_agent.document_view_binding import check_snapshot
            check_snapshot(self.g.logic,row,snap)
            ident=snap.get('interaction_id')
            if ident:
                view=self.s.rows('SELECT * FROM am_interactions WHERE id=?',(ident,))[0]
                if view['kind']=='navigation':
                    heads=self.s.rows('SELECT interaction_id FROM am_navigation WHERE scope=?',(view['scope'],))
                    if heads and heads[0]['interaction_id']!=ident:raise BoundaryError('VIEW_CHANGED')
                if view['view_version']!=snap['view_version'] and not row.get('_resumed_source'):raise BoundaryError('VIEW_CHANGED')
                if view['kind']!='immutable':row['_interaction']=ident;row['_view_version']=view['view_version']
                elif snap.get('navigation_parent') and snap.get('actions',{}).get(value.get('action'),{}).get('page') not in ('confirm_reminder','subscription_confirm','review_content','item_change','plan_control'):
                    parent=self.s.rows('SELECT * FROM am_interactions WHERE id=? AND expires>?',(snap['navigation_parent'],self.s.clock()))
                    if parent and parent[0]['scope']==self.scope(row):row['_interaction']=parent[0]['id'];row['_view_version']=parent[0]['view_version']
                row['_list_origin']=snap.get('data',{}).get('origin')
                if not row.get('_resumed_source'):row['_snapshot_version']=snap.get('data',{}).get('snapshot_version')
        return c,value.get('action')

    def projects(self,row):
        if row.get('_query_read'):return self.query_projects(row)
        if row['user']!=self.owner:raise BoundaryError('OWNER_ONLY')
        checked=row.get('_request_authorization')
        scope=tuple(row.get(k) for k in ('app','tenant','user','chat'))
        fresh=checked and checked[0]==scope and time.monotonic()-checked[1]<=2
        if not (row.get('_readonly_request') and fresh):
            with timing.measure(self.s,row.get('id'),'authorization'):self.ledger.port.authorize_user(row['user'])
        with timing.measure(self.s,row.get('id'),'source_read'):projects=self.ledger.port.read()
        with timing.measure(self.s,row.get('id'),'local_calculation'),self.ledger.mutex:
            result=self.ledger.catalog.sync(projects,complete=all(p.get('read_status','complete')=='complete' for p in projects),revision=projects[0].get('source_revision') if projects else None)
            self._briefing_projects=result
            return result

    def help(self):
        lines=['AM 自身功能（当前可用入口）：','/remind 事项：创建、修改、预览、确认、取消及查询提醒。'] if self.g.config.reminders_enabled else ['AM 自身功能：']
        if self.flags['query']:lines+=['/project 需求编号：真实台账快查；/todo：需要你推动的跟进清单。']
        if self.flags['snapshots']:lines+=['/changes：记录变化；/briefing：本人简报；订阅须明确时间。']
        if self.flags['items']:lines+=['/item 帮助：创建跟进事项、反馈、暂停、核验和关闭。']
        if self.flags['scheduler']:lines+=['/plan 帮助：一次或周期计划；显示完整规则后确认。']
        if self.flags['progress_collection']:lines+=['/update 项目编号：进展原文 → 核对 → 精确差异 → 批准。']
        if self.flags['precheck']:lines+=['/precheck 帮助：先绑定目标和要求，再预检材料。']
        lines+=['/doc logic 8d|test|charter：文档整理；/chat 返回普通对话。','/doc sample 与 /progress sample 为合成测试。']
        return '\n'.join(lines)

    def query(self,row):
        if row.get('_query_read') and '最新' in row['body']:
            row.pop('_snapshot_version',None)
            row['_force_refresh']=not row.get('_resumed_source')
        q=row['body'];projects=self.projects(row);scope=self.scope(row)
        meta=self.source_meta(row);reference=meta.get('parent') or meta.get('root')
        if reference:
            refs=self.s.rows('SELECT project_ref FROM am_query_refs WHERE message_id=? AND scope=? AND expires>?',(reference,scope,self.s.clock()))
            matches=[p for p in projects if refs and p['key']==refs[0]['project_ref'] and p.get('identity_status')=='verified']
            if len(matches)!=1:return '引用消息无法在当前身份和话题内确认有效项目，请明确项目编号或名称。'
            requested=identifiers(q)
            if requested and requested!=identifiers(matches[0]['title']):return '引用项目与本条编号不一致，请确认要查询的项目。'
            self.save_session(row,matches[0]['key'],None)
            return render_project(matches[0],fields_for(q),row.get('_observed_at',self.s.clock()))
        session=self.s.rows('SELECT * FROM am_sessions WHERE scope=? AND expires>?',(scope,self.s.clock()))
        session=session[0] if session else None
        previous=session['focus'] if session else None
        pending=json.loads(session['pending']) if session and session['pending'] else None
        choice=choice_number(q)
        if choice and pending:
            if choice>len(pending['ids']):return '序号不在当前候选中。'
            matches=[p for p in projects if p['key']==pending['ids'][choice-1]];fields=pending['fields']
        else:
            fields=fields_for(q);requested,matches,unresolved,conflicts=resolve_identifiers(q,projects)
            if requested:
                from group_ledger import render_projects
                parts=[render_projects(matches,fields,row.get('_observed_at',self.s.clock()),unresolved=unresolved,conflicts=conflicts)]
                focus=matches[0]['key'] if len(requested)==len(matches)==1 else None
                self.save_session(row,focus,None)
                return '\n\n'.join(parts)
            matches=select_projects(q,projects,previous)
        if len(matches)==1:
            self.save_session(row,matches[0]['key'],None)
            return render_project(matches[0],fields,row.get('_observed_at',self.s.clock()))
        if not matches:return '请提供唯一项目编号或名称；当前无法可靠确定对象。'
        self.save_session(row,None,{'question':q,'ids':[p['key'] for p in matches[:5]],'fields':fields})
        return self.ux_page(row,'选择项目','有多个匹配项目，请选择。',[(p['title'],{'page':'project','project':p['key'],'section':{'people':'people','schedule':'dates','todo':'todos'}.get(fields[0])}) for p in matches[:5]]+[('项目列表',{'page':'projects'})],data={'query_view':True})

    def save_session(self,row,focus,pending):
        scope=self.scope(row)
        with self.s.tx() as c:
            c.execute('INSERT INTO am_sessions VALUES(?,?,?,1,?) ON CONFLICT(scope) DO UPDATE SET focus=excluded.focus,pending=excluded.pending,version=am_sessions.version+1,expires=MIN(am_sessions.expires,excluded.expires)',
                      (scope,focus,canonical(pending) if pending else None,row['expires']))
            if focus:c.execute('INSERT OR IGNORE INTO am_query_refs VALUES(?,?,?,?)',(row['id'],scope,focus,row['expires']))

    def briefing(self,row,*,changes_only=False):
        return BriefingMixin.briefing(self,row,changes_only=changes_only)

    def todos(self,row,*,projects=None,uncertain=False):
        if projects is None:projects=self.projects(row)
        lines=['需要你推动的协调、确认和验收事项；不是要求你亲自完成所有专业工作。']
        today=datetime.fromtimestamp(row.get('created',self.s.clock()),ZoneInfo('Asia/Shanghai'))
        today_terms={f'{today.year}.{today.month}.{today.day}',f'{today.year}-{today.month:02d}-{today.day:02d}'}
        dated=[];other=[]
        for p in projects:
            if uncertain:
                values=[p.get(k,'') for k in ('关键日期','人员与职责','项目进展')]
                selected=[v for v in values if re.search('待确认|未明确|待定|缺少|待核',v)]
                if not p.get('关键日期'):selected+=['日期未记录']
                if not p.get('人员与职责'):selected+=['人员职责未记录']
                if selected:lines.append(p['title']+'\n'+'\n'.join(selected))
            else:
                todo=p.get('待办','').strip()
                if todo and todo!='—':
                    entry=p['title']+'\n'+todo+('' if re.search('跟进|协调|确认|推动|组织|催办|决策',todo) else '\n推动动作与执行归属待核对。')
                    (dated if any(term in todo for term in today_terms) else other).append(entry)
        if dated:lines+=['原文明确出现今天日期的跟进项（预计、承诺或检查性质仍按原文）：']+dated
        if other:lines+=['其他重点／尚未明确今天期限：']+other
        if len(lines)==1:lines+=['台账未记录当前待跟进项；不代表项目全部完成。']
        lines+=['未写明确日期的事项不判为今天到期；预计、承诺与检查时间按原文保留。','来源：'+SOURCE]
        return '\n\n'.join(lines)

    def handle(self,row):
        if row['kind']=='followup_reply':
            row['_followup_text']=True
            return self.followup.reply(row)
        if row['kind']=='text' and row['user']==self.owner and row['chat_type']=='p2p':
            handled,result=self.followup.text_ui.handle(row)
            if handled:return result
            if row['body'].strip().startswith('/followup'):row['_followup_text']=True
        if self.information is not None and row['kind']=='text' and (row['body'].strip()=='/info' or row['body'].strip().startswith('/info ')):
            return self.information.respond(row)
        if row['kind']=='group_query':return self.group_query_answer(row)
        if row['kind']=='group_open':return self.group_open_private(row)
        if row['user']!=self.owner and row['kind'] in ('menu','text'):
            if not self.query_request(row):raise BoundaryError('OWNER_ONLY')
            if row['kind']=='menu' or row['body'].strip() in ENTRY_TEXT or row['body'].strip()=='/help':return self.ux_projects(row)
            return self.query(row)
        if row['kind']=='menu':
            if row['body']=='home':return self.ux_home(row)
            if row['body']=='projects':return self.ux_projects(row)
            return self.ux_more_action(row,{'page':'more'})
        if row['kind']=='collection':return self.ux_collect_message(row)
        if row['kind']=='card':
            body=json.loads(row['body'])
            if body['value'].get('feedback'):return self.handle_feedback(row,body)
            if body['value'].get('briefing'):return self.handle_briefing_card(row,body)
            correction=self.s.rows("SELECT * FROM am_cards WHERE nonce=? AND flow='feedback_correction'",(body['value'].get('nonce'),))
            if correction:return self.handle_feedback_correction(row,correction[0])
            c,action=self.validate_card(row)
            if c['flow']=='ux':return self.ux_handle(row,c,action)
            if hasattr(self,'handle_business_card'):return self.handle_business_card(row,c,action)
            raise BoundaryError('CARD_FEATURE_NOT_READY')
        q=row['body'].strip()
        if q in FOLLOWUP_ENTRY_TEXT and row['user']==self.owner and row['chat_type']=='p2p':
            source_meta=self.source_meta(row)
            if not source_meta.get('parent') and not source_meta.get('root'):
                if not self.flags['items']:raise BoundaryError('ITEMS_NOT_ENABLED')
                row['_view_kind']='navigation';row['_resume_navigation']=True
                return self.ux_work(row)
        if q=='/query-probe':return self.prepare_query_probe(row)
        if q in ('/am','/menu'):return self.ux_home(row)
        if q in ENTRY_TEXT:return self.ux_projects(row)
        if q=='/help':return self.ux_more_action(row,{'page':'help'})
        if q=='/status':return self.ux_status(row)
        if q=='/diagnostics':
            def read_state(path):
                try:return json.loads(path.read_text())
                except (OSError,ValueError):return {}
            connection=read_state(self.ledger.root/'connection-status.json')
            reception=read_state(self.ledger.root/'event-status.json')
            model=read_state(self.g.config.root/'state/model-coordinator.json')
            baseline=self.ledger.db.execute('SELECT read_at FROM source_baselines WHERE namespace=?',(self.ledger.catalog.namespace,)).fetchone()
            fetch=datetime.fromtimestamp(baseline[0],ZoneInfo('Asia/Shanghai')).isoformat() if baseline else '尚未建立成功读取基线'
            delivery=self.s.rows('SELECT state,count(*) AS count FROM outbox GROUP BY state')
            scheduled=self.s.rows('SELECT state,count(*) AS count FROM reminder_occurrences GROUP BY state')
            writes=self.s.rows('SELECT state,count(*) AS count FROM ledger_write_attempts GROUP BY state')
            return ('连接最近记录：'+str(connection.get('state','未知'))+'\n接收最近记录：'+('已持久受理' if reception.get('accepted') else '未记录新的受理')+
                    '\n资料最近成功读取：'+fetch+'\n模型最近执行：'+str(model.get('state','尚无记录'))+'；暂停原因：'+str(self.s.meta('provider_pause') or '未记录暂停')+
                    '\n消息投递：'+canonical(delivery)+'\n计划实例：'+canonical(scheduled)+'\n台账写回：'+canonical(writes)+
                    '\n正式写回：'+('已配置，仍逐差异批准' if self.flags['ledger_write'] else '未启用；采集和提案可独立使用'))
        handled,result=self.update_command(row)
        if handled:return result
        handled,result=self.business_command(row)
        if handled:return result
        if not self.flags['query']:raise BoundaryError('QUERY_NOT_ENABLED')
        if q in ('/todo','我的待办','今天我需要跟进什么','今天我需要跟进什么？'):return self.ux_work(row)
        if q.startswith('哪些项目'):return self.todos(row,uncertain=True)
        if q in ('/changes','查看变化','项目变化'):
            if not self.flags['snapshots']:raise BoundaryError('SNAPSHOTS_NOT_ENABLED')
            return self.briefing(row,changes_only=True)
        if q in ('/briefing','变化简报'):return self.briefing(row)
        return self.query(row)

    def group_status(self,row,text):
        self.ledger.port.guard(row['chat'],row['user'])
        self.s.execute("UPDATE am_inbox SET state='sending' WHERE id=?",(row['id'],))
        try:self.ledger.reply(row['id'],re.sub(r'(?i)<(?=/?at\b)','＜',text))
        except Exception:
            self.s.execute("UPDATE am_inbox SET state='delivery_unknown' WHERE id=?",(row['id'],))
            raise BoundaryError('GROUP_REPLY_UNKNOWN')

    def readonly_request(self,row):
        """Only ordinary owner navigation may reuse a check in this request.

        Never persisted, never shared with another event/user. Approval,
        collection, feedback and document actions keep their original checks.
        Delivery still independently refreshes authorization.
        """
        if self.query_request(row):return True
        if row['user']!=self.owner or row['chat_type']!='p2p':return False
        if row['kind']=='menu':return row['body'] in ('home','projects','more')
        if row['kind']=='text':
            q=row['body'].strip()
            return q in FOLLOWUP_ENTRY_TEXT or q in ('/menu','/am','/help','/status','/briefing','/changes','/todo','变化简报','查看变化','项目变化','我的待办') or q.startswith('/project') or deterministic_intent(q)=='project'
        if row['kind']=='card':
            body=json.loads(row['body']);value=body['value']
            cards=self.s.rows("SELECT * FROM am_cards WHERE nonce=? AND flow='ux'",(value.get('nonce'),))
            if not cards or cards[0]['actor']!=row['user']:return False
            action=json.loads(cards[0]['snapshot']).get('actions',{}).get(value.get('action'),{})
            return action.get('page') in ('home','projects','search','project','project_actions','more','work','help','status','briefing','briefing_detail')
        return False

    def training_requires_model(self,row):
        if row.get('kind')!='conversation':return False
        harness=getattr(self,'harness',None)
        if harness is not None and harness.durable and harness.config.control_enabled:
            from am_harness.controls import parse_control
            if parse_control(row.get('body')) is not None:return False
        return True

    def step(self,row=None,*,claimed=False,maintenance=False):
        if self.conversation is not None:self.conversation.cleanup()
        self.ux_cleanup()
        self.followup.text_ui.cleanup()
        self.followup.expression.cleanup()
        self.items.expire_reply_content()
        self.s.execute("UPDATE am_inbox SET body='',state=CASE WHEN state='received' THEN 'expired' ELSE state END WHERE expires<=?",(self.s.clock(),))
        self.s.execute('DELETE FROM am_sessions WHERE expires<=?',(self.s.clock(),))
        self.s.execute('DELETE FROM am_query_refs WHERE expires<=?',(self.s.clock(),))
        self.s.execute('DELETE FROM am_source_meta WHERE id IN (SELECT id FROM am_inbox WHERE expires<=?)',(self.s.clock(),))
        self.s.execute('DELETE FROM am_request_timings WHERE event_id IN (SELECT id FROM am_inbox WHERE expires<=?)',(self.s.clock(),))
        self.s.execute('DELETE FROM am_outbox_events WHERE event_id NOT IN (SELECT event_id FROM am_request_timings)')
        self.s.execute('DELETE FROM am_latency_anchors WHERE event_id NOT IN (SELECT event_id FROM am_request_timings)')
        self.s.execute('DELETE FROM am_query_wait WHERE event_id NOT IN (SELECT id FROM am_inbox WHERE expires>?)',(self.s.clock(),))
        self.s.execute('DELETE FROM am_query_deliveries WHERE outbox_id NOT IN (SELECT id FROM outbox WHERE expires>?)',(self.s.clock(),))
        self.s.execute('DELETE FROM am_source_deliveries WHERE outbox_id NOT IN (SELECT id FROM outbox WHERE expires>?)',(self.s.clock(),))
        self.s.execute('DELETE FROM am_query_probes WHERE expires<=?',(self.s.clock(),))
        self.s.execute('DELETE FROM am_direct_responses WHERE expires<=?',(self.s.clock(),))
        self.s.execute('DELETE FROM am_group_cards WHERE expires<=?',(self.s.clock(),))
        self.s.execute('DELETE FROM am_group_refs WHERE expires<=?',(self.s.clock(),))
        self.s.execute('DELETE FROM am_group_turns WHERE expires<=?',(self.s.clock(),))
        self.s.execute('DELETE FROM am_group_delivery WHERE outbox_id NOT IN (SELECT id FROM outbox WHERE expires>?)',(self.s.clock(),))
        self.s.execute('DELETE FROM am_group_private_chats WHERE outbox_id NOT IN (SELECT id FROM outbox WHERE expires>?)',(self.s.clock(),))
        self.s.execute("UPDATE am_cards SET snapshot='{}' WHERE expires<=? AND (flow!='ux' OR outbox_id IN (SELECT id FROM outbox WHERE expires<=?))",(self.s.clock(),self.s.clock()))
        from fs_agent.document_view_binding import cleanup
        cleanup(self.s)
        self.s.execute('DELETE FROM am_query_recent WHERE expires<=?',(self.s.clock(),))
        future=self.snapshots.future
        if future is not None and future.done():
            self.resume_source_waits()
        rows=[] if maintenance else [row] if row else self.s.rows("SELECT * FROM am_inbox WHERE state='received' AND expires>? ORDER BY created LIMIT 1",(self.s.clock(),))
        if not maintenance and row is None and self.s.meta('resource_window_owner'):
            # A parked conversation must not starve later read-only navigation,
            # exact approvals, replies, or out-of-band cancel/revise messages.
            queued=self.s.rows("SELECT * FROM am_inbox WHERE state='received' AND expires>? ORDER BY created LIMIT 64",(self.s.clock(),))
            rows=next(([r] for r in queued if not self.training_requires_model(r)),[])
        if not rows:
            if self.scheduler:
                if self.flags['scheduler']:
                    for plan in self.s.rows("SELECT * FROM reminder_plans WHERE kind='briefing' AND state='active' AND next_at<=? AND valid_until>?",(self.s.clock(),self.s.clock())):
                        origin=self.owner_origin()
                        try:
                            if origin is None:raise BoundaryError('OWNER_ORIGIN_UNAVAILABLE')
                            self.projects(origin)
                            self.s.execute("UPDATE reminder_plans SET error=NULL WHERE id=? AND error='BRIEFING_SOURCE_UNAVAILABLE'",(plan['id'],))
                        except Exception:
                            self.s.execute("UPDATE reminder_plans SET error='BRIEFING_SOURCE_UNAVAILABLE' WHERE id=?",(plan['id'],))
                return self.scheduler.step()
            return False
        row=rows[0]
        needs_model=self.training_requires_model(row)
        # The training window first fences new model work, then drains the
        # already-running Q8 work. Keep waiting messages received; never turn a
        # maintenance pause into a failed/unknown business operation.
        with self.s.tx() as c:
            maintenance_owner=c.execute("SELECT value FROM meta WHERE key='resource_window_owner' AND value!=''").fetchone()
            if needs_model and maintenance_owner:
                if claimed:
                    c.execute("UPDATE am_inbox SET state='received' WHERE id=? AND state='processing' AND expires>?",
                              (row['id'],self.s.clock()))
                return False
            if not claimed and not c.execute("UPDATE am_inbox SET state='processing' WHERE id=? AND state='received' "
                    "AND (? OR NOT EXISTS(SELECT 1 FROM meta WHERE key='resource_window_owner' AND value!=''))",
                    (row['id'],int(not needs_model))).rowcount:return False
        started=time.monotonic()
        context_token=timing.set_context(self.s,row['id'])
        self.s.execute('UPDATE am_request_timings SET started=COALESCE(started,?) WHERE event_id=?',(self.s.clock(),row['id']))
        timing.anchor(self.s,row['id'],'task_start')
        try:
            if row.get('kind')=='conversation':
                if self.conversation is None:raise BoundaryError('CONVERSATION_DISABLED')
                self.conversation.process(row)
                if not (self.harness is not None and self.harness.durable):
                    self.s.execute("UPDATE am_inbox SET state='done' WHERE id=?",(row['id'],))
                return True
            self.observe_direct_callback(row)
            row['_readonly_request']=self.readonly_request(row)
            row['_query_read']=self.query_request(row)
            waiting=self.s.rows('SELECT * FROM am_query_wait WHERE event_id=?',(row['id'],))
            if waiting:
                row['_interaction']=waiting[0]['interaction_id']
                row['_resumed_source']=True
                self.s.execute('DELETE FROM am_query_wait WHERE event_id=?',(row['id'],))
            with timing.measure(self.s,row['id'],'authorization'):
                if row['_query_read']:row['_query_grant']=self.query_grant(row)
                elif row['chat_type']=='group':self.ledger.port.guard(row['chat'],row['user'])
                elif row['user']==self.owner:self.ledger.port.authorize_user(row['user'])
                elif row['kind']=='followup_reply':self.ledger.port.authorize_user(row['user'])
                elif row['kind']!='card':raise BoundaryError('OWNER_ONLY')
            row['_request_authorization']=(tuple(row.get(k) for k in ('app','tenant','user','chat')),time.monotonic())
            text=self.handle(row)
            if not row['_readonly_request']:
                with timing.measure(self.s,row['id'],'authorization'):
                    if row['user']==self.owner:self.ledger.port.authorize_user(row['user'])
            if text:
                if row['chat_type']=='group':self.group_status(row,text)
                elif row.get('_followup_text'):self.followup.text_ui.publish(row,text)
                else:self.ux_page(row,'处理结果',text,[('返回首页',{'page':'home'})])
            self.s.execute("UPDATE am_inbox SET state='done' WHERE id=?",(row['id'],))
        except SourceLoading:
            existing=self.s.rows('SELECT message_id,pending_outbox FROM am_interactions WHERE id=?',(row.get('_interaction'),))
            usable=bool(existing and existing[0]['message_id'] and not existing[0]['pending_outbox'])
            # Keep a known existing page and its honest old read time while an
            # explicit refresh runs. Publishing a loading replacement would add
            # a second platform round trip before the fresh result can appear.
            if row['chat_type']!='group' and not usable:self.ux_page(row,'项目查询','资料读取中，完成后会更新此页。',[('刷新资料',{'page':'refresh'})],data={'query_view':True,'task_state':'loading'})
            self.s.execute('INSERT OR REPLACE INTO am_query_wait VALUES(?,?,0)',(row['id'],row.get('_interaction')))
            self.s.execute("UPDATE am_inbox SET state='source_wait' WHERE id=?",(row['id'],))
        except Exception as exc:
            if str(exc)=='GROUP_REPLY_UNKNOWN':return True
            code=str(exc) if isinstance(exc,(BoundaryError,ValueError)) and re.fullmatch('[A-Z_0-9]{1,80}',str(exc)) else 'AM_REQUEST_FAILED'
            self.s.execute("UPDATE am_inbox SET state='failed',error=? WHERE id=?",(code,row['id']))
            if row.get('kind')=='conversation':
                self.notify(dict(row,_query_read=False,_query_denial=True),text='这条消息暂未处理完成，已保留原有事项。请稍后重试。',kind='conversation_error',event_key='conversation-error:'+row['id'])
            elif row.get('_feedback_context'):
                self.feedback_error(row,code)
            elif row.get('_query_read') and row['chat_type']=='group':
                self.group_public(row,'当前无法完成查询，请稍后重试，或重新提供项目名称。',denial=True)
            elif row['chat_type']=='group':
                try:
                    self.group_status(row,'本次操作未完成：'+code+'。未据此写入台账。')
                    self.s.execute("UPDATE am_inbox SET state='failed',error=? WHERE id=?",(code,row['id']))
                except Exception:pass
            elif row.get('_followup_text'):
                self.followup.text_ui.error(row)
            elif row['user']==self.owner:self.ux_error(row,code)
            elif row.get('_query_read') or row['kind'] in ('menu','text'):self.query_notice(row,code)
        finally:
            timing.add(self.s,row['id'],'processing_total',time.monotonic()-started)
            self.s.execute('UPDATE am_request_timings SET processed=? WHERE event_id=?',(self.s.clock(),row['id']))
            timing.reset_context(context_token)
        return True
