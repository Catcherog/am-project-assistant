"""Immutable group answers and actor-bound private continuation.

Current schema-2 ephemeral messages cannot be PATCHed by the normal message
API (230001 in the approved compatibility probe). Use an explicit private
continuation, preserving the source group's scope throughout navigation.
"""
import json
import re
import uuid
import threading
from fs_agent.config import BoundaryError
from fs_agent.card_ui import card,paragraph,button,bind_rendered_card
from fs_agent.project_catalog import canonical,digest
from group_ledger import select_projects,render_project,render_projects,SOURCE,SHEET
from query_rules import identifiers,fields_for,resolve_identifiers,deterministic_intent


class GroupQueryMixin:
    def init_group_query(self):
        self.group_ai_lock=threading.Lock()
        with self.s.lock:self.s.conn.executescript('''
        CREATE TABLE IF NOT EXISTS am_group_cards(nonce TEXT PRIMARY KEY,app TEXT NOT NULL,tenant TEXT NOT NULL,chat TEXT NOT NULL,thread TEXT,projects TEXT NOT NULL,message_id TEXT,outbox_id TEXT,expires REAL NOT NULL);
        CREATE TABLE IF NOT EXISTS am_group_delivery(outbox_id TEXT PRIMARY KEY,source_chat TEXT NOT NULL,reply_to TEXT,thread TEXT,mode TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS am_group_refs(message_id TEXT PRIMARY KEY,app TEXT NOT NULL,tenant TEXT NOT NULL,chat TEXT NOT NULL,projects TEXT NOT NULL,expires REAL NOT NULL);
        CREATE TABLE IF NOT EXISTS am_group_private_chats(outbox_id TEXT PRIMARY KEY,chat TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS am_group_turns(outbox_id TEXT PRIMARY KEY,scope TEXT NOT NULL,question TEXT NOT NULL,answer TEXT NOT NULL,expires REAL NOT NULL);
        ''')

    def query_group(self,row):
        return row.get('_query_group') or self.source_meta(row).get('query_group') or (row['chat'] if row.get('kind') in ('group_query','group_open') else None)

    def group_public(self,row,text,projects=(),*,denial=False,remember=False):
        nonce=uuid.uuid4().hex
        scope=row.get('_query_grant',[])
        if '*' not in scope:
            text=text.replace(f'｜[查看来源原文]({SOURCE}?sheet={SHEET})','').replace(SOURCE+'?sheet='+SHEET,'已授权项目资料')
        elements=[paragraph(text)]
        if not denial:elements.append(button('私聊继续',{'flow':'am_group_open','nonce':nonce}))
        value=bind_rendered_card(card('项目查询',elements))
        public=dict(row,_public_query=True,_query_denial=denial)
        with self.s.tx() as c:
            out=self.notify(public,card=value,kind='group_query',conn=c)
            c.execute('INSERT INTO am_group_cards VALUES(?,?,?,?,?,?,NULL,?,?)',(nonce,row['app'],row['tenant'],row['chat'],self.source_meta(row).get('thread'),canonical(list(projects)),out,row['expires']))
            if remember:c.execute('INSERT INTO am_group_turns VALUES(?,?,?,?,?)',(out,self.scope(row),row['body'],text,row['expires']))
        return None

    def group_query_answer(self,row):
        q=row['body'].strip()
        if q in ('项目','项目查询','项目列表','查看项目','/menu','/project'):
            return self.group_public(row,'本群项目目录可在私聊中浏览，翻页和切换栏目只影响你自己的页面。')
        route=deterministic_intent(q)
        if route in ('help','cancel','action','explain'):
            return self.group_public(row,{'help':'可按项目名称或编号查询进展、人员、计划和待办。点“私聊继续”浏览本群项目目录。',
                'cancel':'本次查询没有创建发送或写入动作；管理事项请到本人私聊处理。',
                'action':'这是操作请求，请在本人私聊打开对应项目，核对完整预览后确认。',
                'explain':'编号含义需以适用的编号规则为准；编号解释不会查询项目进展。'}[route])
        projects=self.query_projects(row)
        meta=self.source_meta(row);reference=meta.get('parent') or meta.get('root')
        requested,matches,unresolved,conflicts=resolve_identifiers(q,projects)
        if reference:
            refs=self.s.rows('SELECT * FROM am_group_refs WHERE message_id=? AND app=? AND tenant=? AND chat=? AND expires>?',(reference,row['app'],row['tenant'],row['chat'],self.s.clock()))
            if not refs:return self.group_public(row,'引用的消息未能关联到有效项目，请提供项目名称或编号。')
            keys=json.loads(refs[0]['projects']);quoted=[p for p in projects if p['key'] in keys]
            row['expires']=min(row['expires'],refs[0]['expires'])
            if requested and set(p['key'] for p in matches)!=set(p['key'] for p in quoted):
                return self.group_public(row,'本条编号与引用项目不一致，请明确要查询的项目。')
            matches=quoted
        elif not requested:
            session=self.s.rows('SELECT focus FROM am_sessions WHERE scope=? AND expires>?',(self.scope(row),self.s.clock()))
            previous=session[0]['focus'] if session else None
            matches=select_projects(q,projects,previous)
        if len(matches)==1 and not unresolved and not conflicts:
            p=matches[0];self.save_session(row,p['key'],None)
            return self.group_public(row,render_project(p,fields_for(q),row['_observed_at']),[p['key']])
        if len(matches)>1:
            return self.group_public(row,'匹配到多个项目，请点“私聊继续”选择：\n'+'\n'.join('• '+p['title'] for p in matches[:5]),[p['key'] for p in matches])
        if requested:
            return self.group_public(row,render_projects(matches,fields_for(q),row['_observed_at'],unresolved=unresolved,conflicts=conflicts),[p['key'] for p in matches])
        assistant=self.ledger.assistant
        if reference or re.match(r'^(那|它|这个|谁|负责人|人员|下一步)',q):
            return self.group_public(row,'请提供项目名称，或引用 AM 的项目答案后再问。')
        if assistant and not assistant.paused():
            # Only semantic ambiguity reaches Qwen. The provider owns the one
            # model lane; membership/source HTTP stays in this bounded I/O pool.
            if not self.group_ai_lock.acquire(blocking=False):return self.group_public(row,'语言理解正在处理另一条请求；你可以直接提供项目编号查询，或稍后重试。')
            try:
                with self.ledger.mutex:history=self.ledger.history(row['chat'],row['user'],meta.get('thread') or '')
                newer=self.s.rows("SELECT t.question,t.answer,'general' mode,t.expires FROM am_group_turns t JOIN outbox o ON o.id=t.outbox_id WHERE t.scope=? AND t.expires>? AND o.state='sent' ORDER BY o.created DESC LIMIT 4",(self.scope(row),self.s.clock()))
                history=(history+list(reversed(newer)))[-4:]
                while sum(len(h['question'])+len(h['answer']) for h in history)>6000:history.pop(0)
                row['expires']=min([row['expires']]+[h['expires'] for h in history])
                result=assistant.reply(q,history,None,lambda:projects,timeout=min(120,row['expires']-self.s.clock()),chat_id=row['chat'],message_id=row['id'])
            finally:self.group_ai_lock.release()
            if getattr(result, 'via_grok', False) and result.mode == 'general':
                if getattr(result, 'dry_run', False):
                    return None
                return result.text
            if result.project:
                matched=[p for p in projects if p['key']==result.project]
                if matched:
                    self.save_session(row,result.project,None)
                    return self.group_public(row,render_project(matched[0],fields_for(q),row['_observed_at']),[result.project])
            if result.candidates:return self.group_public(row,'请点“私聊继续”选择匹配项目。',result.candidates)
            return self.group_public(row,result.text,remember=result.mode=='general')
        return self.group_public(row,'暂未确定项目，请提供准确名称或编号。')

    def group_open_private(self,row):
        body=json.loads(row['body'])
        cards=self.s.rows('SELECT * FROM am_group_cards WHERE nonce=?',(body['value'].get('nonce'),))
        if not cards:raise BoundaryError('CARD_UNKNOWN')
        source=cards[0]
        if (source['app']!=row['app'] or source['tenant']!=row['tenant'] or source['chat']!=row['chat']
                or not source['message_id'] or source['message_id']!=body['source_message'] or source['expires']<=self.s.clock()
                or body.get('thread') and body['thread']!=source['thread']):raise BoundaryError('CARD_IDENTITY_OR_VERSION_INVALID')
        private=dict(row,chat='menu:'+row['user'],chat_type='p2p',kind='text',body='项目',_query_group=source['chat'],
                     _interaction=digest(['group-private',source['chat'],row['user'],row['id']]),expires=min(row['expires'],source['expires']))
        private['_query_grant']=self.query_grant(private)
        projects=self.query_projects(private);keys=json.loads(source['projects']);matches=[p for p in projects if p['key'] in keys]
        if keys and not matches:raise BoundaryError('PROJECT_DISCLOSURE_CHANGED')
        if len(matches)==1:return self.ux_project_detail(private,matches[0]['key'])
        if matches:return self.ux_page(private,'选择项目','请选择要查看的项目。',[(p['title'],{'page':'project','project':p['key']}) for p in matches[:20]],data={'query_view':True})
        return self.ux_projects(private)

    def group_transport_row(self,row):
        records=self.s.rows('SELECT * FROM am_group_delivery WHERE outbox_id=?',(row['id'],))
        if not records:return row
        route=records[0]
        return dict(row,_group_reply_to=route['reply_to'] if route['mode']=='public' else None,_reply_in_thread=bool(route['thread']))

    def group_delivered(self,c,row,message_id):
        actual=row.get('_delivered_chat_id')
        private=c.execute("SELECT 1 FROM am_group_delivery WHERE outbox_id=? AND mode='private'",(row['id'],)).fetchone()
        if private and actual and actual.startswith('oc_') and row['chat']=='menu:'+row['user']:
            # The successful Feishu CREATE receipt owns the DM chat binding;
            # no extra GET is needed just to rediscover that same chat.
            c.execute('INSERT OR REPLACE INTO am_group_private_chats VALUES(?,?)',(row['id'],actual))
            c.execute('UPDATE am_cards SET chat=? WHERE outbox_id=?',(actual,row['id']))
            view=c.execute('SELECT v.* FROM am_interactions v JOIN am_view_outputs o ON o.interaction_id=v.id WHERE o.outbox_id=?',(row['id'],)).fetchone()
            if view:
                parts=json.loads(view['scope']);parts[2]=actual;scope=canonical(parts)
                c.execute('UPDATE am_interactions SET scope=? WHERE id=?',(scope,view['id']))
                c.execute('DELETE FROM am_navigation WHERE interaction_id=?',(view['id'],))
                if view['kind']=='navigation':c.execute('INSERT OR REPLACE INTO am_navigation VALUES(?,?)',(scope,view['id']))
        cards=c.execute('SELECT * FROM am_group_cards WHERE outbox_id=?',(row['id'],)).fetchone()
        if cards:
            c.execute('UPDATE am_group_cards SET message_id=? WHERE outbox_id=?',(message_id,row['id']))
            c.execute('INSERT OR IGNORE INTO am_group_refs VALUES(?,?,?,?,?,?)',(message_id,cards['app'],cards['tenant'],cards['chat'],cards['projects'],cards['expires']))
