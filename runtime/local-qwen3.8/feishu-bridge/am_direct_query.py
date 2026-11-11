"""Local-only callback navigation, journalled separately from platform receipt."""
import json
import time
from fs_agent import request_timing as timing
from fs_agent.config import BoundaryError

FAST_PAGES=frozenset(('project','projects','recent','search','long_content'))


class DirectQueryMixin:
    def init_direct_query(self):
        with self.s.lock:self.s.conn.executescript('''
        CREATE TABLE IF NOT EXISTS am_direct_responses(event_id TEXT PRIMARY KEY,outbox_id TEXT NOT NULL,response TEXT NOT NULL,expires REAL NOT NULL);
        ''')

    def observe_direct_callback(self,row):
        """A trusted click on the new nonce is evidence that view was applied."""
        if row['kind']!='card':return
        body=json.loads(row['body'])
        cards=self.s.rows('SELECT * FROM am_cards WHERE nonce=?',(body['value'].get('nonce'),))
        if not cards:return
        c=cards[0]
        if (c['actor']!=row['user'] or c['chat']!=row['chat'] or not c['message_id']
                or c['message_id']!=body['source_message'] or c['expires']<=self.s.clock()):return
        snap=json.loads(c['snapshot'])
        if snap.get('app')!=row['app'] or snap.get('tenant')!=row['tenant']:return
        outs=self.s.rows("SELECT * FROM outbox WHERE id=? AND state='unknown' AND error='CALLBACK_APPLICATION_UNKNOWN'",(c['outbox_id'],))
        if not outs:return
        with self.s.tx() as tx:
            tx.execute("UPDATE outbox SET state='sent',error=NULL WHERE id=?",(c['outbox_id'],))
            self.delivered(tx,outs[0],c['message_id'])
        event=timing.origin(self.s,c['outbox_id'])
        timing.anchor(self.s,event,'client',state='observed_next_nonce')

    def direct_query_response(self,payload):
        if not self.query_settings.get('direct_return_enabled',False):return None
        if not self.s.lock.acquire(blocking=False):return None
        try:
            h,e=payload['header'],payload['event'];value=e['action']['value'];context=e['context'];user=e['operator']['open_id']
            if h.get('app_id')!=self.g.config.app_id or h.get('tenant_key')!=self.g.config.tenant_key or h.get('event_type')!='card.action.trigger' or value.get('flow')!='am_self':return None
            old=self.s.rows('SELECT * FROM am_direct_responses WHERE event_id=? AND expires>?',(h['event_id'],self.s.clock()))
            if old:
                out=self.s.rows('SELECT * FROM outbox WHERE id=?',(old[0]['outbox_id'],))
                return json.loads(old[0]['response']) if out and self.view_delivery(out[0]) else {'toast':{'type':'info','content':'页面已更新，请使用当前页面。'}}
            if not self.query_identity.valid(user) or user in self.query_settings.get('denied_users',[]):return None
            cards=self.s.rows('SELECT * FROM am_cards WHERE nonce=? AND flow=\'ux\'',(value.get('nonce'),))
            if not cards:return None
            c=cards[0];snap=json.loads(c['snapshot']);data=snap.get('data',{});action=snap.get('actions',{}).get(value.get('action'),{})
            if (c['actor']!=user or c['chat']!=context['open_chat_id'] or c['message_id']!=context['open_message_id']
                    or c['expires']<=self.s.clock() or not data.get('query_view') or action.get('page') not in FAST_PAGES
                    or action.get('next','project')!='project'):return None
            if context.get('open_thread_id') and context['open_thread_id']!=snap.get('thread'):return None
            version=data.get('snapshot_version')
            with self.snapshots.lock:
                if not version or version not in self.snapshots.versions:return None
            views=self.s.rows('SELECT * FROM am_interactions WHERE id=?',(snap.get('interaction_id'),))
            if not views or views[0]['kind']=='immutable' or views[0]['view_version']!=snap['view_version']:return None
            if self.s.rows("SELECT 1 FROM am_inbox WHERE app=? AND tenant=? AND user=? AND state IN ('received','processing','source_wait')",(h['app_id'],h['tenant_key'],user)):return None
            pending=views[0]['pending_outbox']
            if pending:
                outs=self.s.rows("SELECT * FROM outbox WHERE id=? AND state='unknown' AND error='CALLBACK_APPLICATION_UNKNOWN'",(pending,))
                if not outs or pending!=c['outbox_id']:return None
            handled,accepted=self.ingest(payload)
            if not accepted:return None
            row=self.s.rows('SELECT * FROM am_inbox WHERE id=?',(h['event_id'],))[0]
            row['_direct_return']=True
            self.step(row)
            outputs=self.s.rows("SELECT o.* FROM outbox o JOIN am_outbox_events e ON e.outbox_id=o.id WHERE e.event_id=? AND o.state='callback_pending'",(row['id'],))
            if len(outputs)!=1:return {'toast':{'type':'info','content':'已收到，正在处理。'}}
            out=outputs[0];response={'card':{'type':'raw','data':json.loads(out['body'])}}
            with self.s.tx() as tx:
                tx.execute("UPDATE outbox SET state='unknown',message_id=?,error='CALLBACK_APPLICATION_UNKNOWN' WHERE id=?",(context['open_message_id'],out['id']))
                tx.execute('UPDATE am_cards SET message_id=? WHERE outbox_id=?',(context['open_message_id'],out['id']))
                tx.execute('INSERT INTO am_direct_responses VALUES(?,?,?,?)',(row['id'],out['id'],json.dumps(response),out['expires']))
            timing.anchor(self.s,row['id'],'callback')
            return response
        except Exception:
            # No response has left this function. A generated but unreturned
            # card can enter ordinary delivery; never leave it undispatchable.
            if 'row' in locals():
                self.s.execute("UPDATE outbox SET state='pending' WHERE state='callback_pending' AND id IN (SELECT outbox_id FROM am_outbox_events WHERE event_id=?)",(row['id'],))
                self.s.outbox_wakeup()
                return {'toast':{'type':'info','content':'已收到，正在处理。'}}
            return None
        finally:self.s.lock.release()
