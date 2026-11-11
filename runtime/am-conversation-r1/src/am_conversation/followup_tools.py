"""Natural conversation tools over the original followup state and preview.

The language model can read or prepare a preview. Approval is deliberately
absent from this catalog and remains an independently read original message.
"""
import hashlib
import json

from .tools import Tool, obj, string
from .types import ConversationError, canonical


class NativeFollowupTools:
    def __init__(self, bridge):
        self.b,self.am,self.s=bridge,bridge.service,bridge.s
        self.h=self.am.followup
        self.s.execute('''CREATE TABLE IF NOT EXISTS am_followup_conversation_reads(
            event_id TEXT NOT NULL,item_id TEXT NOT NULL,hash TEXT NOT NULL,expires REAL NOT NULL,
            PRIMARY KEY(event_id,item_id))''')

    def register(self):
        self.b.registry.register(Tool('read_followup_status',
            '读取某项跟进的最新发送、准确回执、回复和业务状态。先从list_followups取得准确item_id；这几类状态不能混淆。',
            obj({'item_id':string(128)}),self.status,owner_only=True))
        self.b.registry.register(Tool('read_followup_feedback',
            '读取指定跟进的完整反馈原话、归属和时间。先确定item_id；按页取完整记录，不把收到或自报当成完成。',
            obj({'item_id':string(128),'offset':{'type':'integer','minimum':0,'maximum':1000}}),self.feedback,owner_only=True,final_reply=True))
        self.b.registry.register(Tool('prepare_followup_message',
            '用户请你问进度、催办或修改跟进时，给本人展示原渠道的完整发送预览。此工具不发送给负责人，不批准、不关闭事项。'
            '必须先确定item_id；对象或私聊/群聊不明确则先问。已有预览修改后必须重新展示。',
            obj({'item_id':string(128),'channel':{'type':'string','enum':['group','recipient_p2p']},
                 'body':string(4000)},('channel',)),self.preview,'local',True))

    def item(self,ctx,ident=None,*,manage=False):
        self.b.authorize(ctx,'manage' if manage else 'read')
        if not ctx.is_owner:raise ConversationError('OWNER_REQUIRED')
        if ident is None:
            row=self.b._current(ctx)['row']
            choices=self.s.rows('SELECT items FROM am_followup_text_choices WHERE scope=? AND expires>?',
                (self.am.scope(row),self.s.clock()))
            items=json.loads(choices[0]['items']) if len(choices)==1 else []
            if not isinstance(items,list) or len(items)!=1:raise ConversationError('FOLLOWUP_REFERENCE_AMBIGUOUS')
            ident=items[0]
        item=self.am.items.get(ident)
        if item['owner']!=ctx.actor_id:raise ConversationError('OWNER_REQUIRED')
        is_test=(item['project_ref'].startswith('validation:AM-FOLLOWUP-20261102-01:')
                 and json.loads(item['source']).get('kind')=='synthetic_validation')
        if not (ctx.can_access_project(item['project_ref']) or item['project_ref']=='personal:'+ctx.actor_id or is_test):
            raise ConversationError('PROJECT_ACCESS_DENIED')
        return item

    def fingerprint(self,ident):
        with self.s.lock:
            item=self.am.items.get(ident)
            value={'item':item,
                'feedback':self.s.rows('SELECT id,kind,applied,created FROM item_feedback WHERE item_id=? ORDER BY id',(ident,)),
                'links':self.s.rows('SELECT * FROM followup_reply_links WHERE item_id=? ORDER BY message_id',(ident,)),
                'occurrences':self.s.rows('SELECT id,state,message_id,readback_state,hash,request_key,reconcile_attempts,next_reconcile FROM reminder_occurrences WHERE item_id=? ORDER BY id',(ident,))}
            return hashlib.sha256(canonical(value).encode()).hexdigest()

    def remember(self,ctx,ident,*,expires=None):
        row=self.b._current(ctx)['row']
        self.s.execute('INSERT INTO am_followup_conversation_reads VALUES(?,?,?,?) ON CONFLICT(event_id,item_id) DO UPDATE SET hash=excluded.hash,expires=excluded.expires',
            (row['id'],ident,self.fingerprint(ident),min(row['expires'],self.s.clock()+600,expires or row['expires'])))

    def current(self,event):
        records=self.s.rows('SELECT * FROM am_followup_conversation_reads WHERE event_id=?',(event,))
        return all(r['expires']>self.s.clock() and self.fingerprint(r['item_id'])==r['hash'] for r in records)

    def output_authorized(self,row):
        records=self.s.rows('SELECT event_id FROM am_conversation_outputs WHERE outbox_id=?',(row['id'],))
        try:return not records or self.current(records[0]['event_id'])
        except Exception:return False

    def status(self,ctx,args,event):
        item=self.item(ctx,args.get('item_id'));row=self.b._current(ctx)['row']
        from am_style.contracts import ContractError
        from am_style.followup import render_followup
        try:
            snapshot=self.h.expression.capture(row,item['id'])
            result={'description':item['description'],'message':render_followup(snapshot),
                    'delivery':snapshot.data['delivery'],'feedback':snapshot.data['feedback'],
                    'business':snapshot.data['item']}
        except ContractError:
            # Older records have no exact shared receipt. Do not infer delivery.
            result={'description':item['description'],'business_state':item['state'],
                    'message':self.am.followup_state(item)[1],
                    'delivery_verified':False,'qualification':'当前记录不足以单独证明这次消息已准确发送。'}
        self.remember(ctx,item['id']);self.b.authorize(ctx,'read')
        return result

    def feedback(self,ctx,args,event):
        item=self.item(ctx,args.get('item_id'));offset=args.get('offset',0)
        records=list(reversed(self.am.items.history(item['id'],include_raw=True)))
        selected=records[offset:offset+1]
        self.remember(ctx,item['id'],expires=self.am.items.feedback_expiry(item['id']));self.b.authorize(ctx,'read')
        if selected:
            names={p['id']:p['name'] for p in json.loads(item['source']).get('target',{}).get('people',[])}
            record=selected[0]
            message=item['description']+'，'+names.get(record['actor'],'授权反馈人')+'最近这条回复的原话是：\n\n'+record['body']
            message+='\n\n这条回复不代表业务已核验完成。'
        else:message='这件事目前没有查到已关联的反馈原话。'
        return {'description':item['description'],'records':selected,'has_more':offset+1<len(records),
                'next_offset':offset+1,'message':message,
                'qualification':'反馈原话不构成业务验收；原文超过原24小时留存期时只能报告保留的回应状态。'}

    def preview(self,ctx,args,event):
        item=self.item(ctx,args.get('item_id'),manage=True)
        row=self.b._current(ctx)['row']
        # Host creates the immutable preview and its own outbox record. The
        # model never receives a confirmation token or a sender capability.
        self.h.preview_text(row,item['id'],args['channel'],body=args.get('body'))
        return {'status':'preview_queued','message':'完整预览已排入本人私聊，等待实际送达后确认；尚未给负责人发送消息。',
                'instruction':'不要让用户输入命令、编号或确认码。请他查看完整预览，直接回复该条预览确认或说明修改。'}
