"""Natural text briefing queries and explicitly confirmed subscriptions."""
import json,re
from datetime import datetime
from zoneinfo import ZoneInfo
from .tools import Tool,obj,string,display_time
from .types import ConversationError
from .business import absolute

class BriefingTools:
    def __init__(self,bridge):self.b=bridge;self.am=bridge.service;self.s=bridge.s
    def read(self,ctx,args,event):
        self.b.authorize(ctx,'read');row=self.b._current(ctx)['row'];projects=self.am.query_projects(row)
        with self.am.ledger.mutex:events=[dict(e) for e in self.am.ledger.catalog.changes() if e['project_id'] in ctx.allowed_project_ids]
        data=self.am.briefing_data(events,projects,['changes','todos']);data['read_at']=row.get('_observed_at',data['read_at'])
        parent=self.am.source_meta(row).get('parent')
        if parent:
            prior=self.s.rows("SELECT payload FROM reminder_occurrences WHERE message_id=? AND state='sent'",(parent,))
            if prior:
                payload=json.loads(prior[0]['payload']);target=payload.get('target',{})
                if target.get('kind')=='owner' and target.get('user')==ctx.actor_id and target.get('chat')==ctx.chat_id and payload.get('briefing_data'):
                    data=payload['briefing_data'];data['events']=[e for e in data['events'] if e['project_id'] in ctx.allowed_project_ids];data['todos']=[e for e in data['todos'] if e['project'] in ctx.allowed_project_ids]
        section=args.get('section','summary')
        if section=='summary':return {'summary':self.am.briefing_summary(data),'read_at':data['read_at'],'changes':len(data['events']),'todos':len(data['todos'])}
        values=data['events'] if section=='changes' else data['todos'];offset=args.get('offset',0);limit=args.get('limit',3)
        return {'section':section,'records':values[offset:offset+limit],'total':len(values),'has_more':offset+limit<len(values),'read_at':data['read_at'],'boundary':'记录变化不代表批准或验收改变。'}
    def prepared(self,ctx,args):
        self.b.authorize(ctx,'manage')
        if not self.am.flags['scheduler'] or not self.am.flags['snapshots']:raise ConversationError('BRIEFING_NOT_ENABLED')
        if not re.fullmatch(r'(?:[01]\d|2[0-3]):[0-5]\d',args['time']):raise ConversationError('SCHEDULE_TIME_REQUIRED')
        rule={'time':args['time'],'timezone':'Asia/Shanghai','workdays':args.get('frequency','daily')=='workdays','calendar_version':self.am.scheduler.calendar.data.get('version'),'briefing_content':args.get('topics',['changes','todos'])}
        next_at=self.am.scheduler.next_time('briefing',rule,self.s.clock()-1);until=absolute(args['until'])
        if not next_at<until<=self.s.clock()+366*86400:raise ConversationError('PLAN_VALIDITY_REQUIRED')
        text='范围：本人当前可访问项目\n目标：本人 AM 私聊\n内容：'+'、'.join({'changes':'记录变化','todos':'台账待办'}[k] for k in rule['briefing_content'])+'\n频率：'+('已核实工作日' if rule['workdays'] else '每天')+' '+args['time']+'（北京时间）\n下一次：'+display_time(datetime.fromtimestamp(next_at,ZoneInfo('Asia/Shanghai')).isoformat())+'\n有效至：'+display_time(args['until'])+'\n确认后按固定规则执行，执行窗口 60 秒，过期不补发。'
        return rule,until,text
    def preview(self,ctx,args):return self.prepared(ctx,args)[-1]
    def subscribe(self,ctx,args,key):
        rule,until,text=self.prepared(ctx,args)
        if text!=self.b.store.get_action(ctx,key)['preview']:return {'status':'failed'}
        row=self.b._current(ctx)['row'];projects=self.am.query_projects(row);self.am._briefing_projects=projects
        p=self.am.scheduler.create(ctx.actor_id,key,item_id=None,kind='briefing',rule=rule,target={'kind':'owner','user':ctx.actor_id,'chat':ctx.chat_id,'conversation_text':True},template='',policy='owner_subscription',valid_until=until)
        self.s.execute("INSERT OR IGNORE INTO am_subscriptions VALUES(?,?,?,'active')",(p['id'],ctx.actor_id,p['id']))
        self.am.scheduler.approve_plan(ctx.actor_id,p['id'],p['version'],p['hash'])
        self.s.execute('INSERT OR IGNORE INTO am_conversation_links VALUES(?,?,?,?)',('plan',p['id'],ctx.session_id,self.b.store.get_action(ctx,key)['presented_event_id']))
        return {'status':'succeeded','verified':self.am.scheduler.plan(p['id'])['state']=='active','receipt_id':p['id'],'message':'本人简报订阅已按所示规则启用；可随时暂停或取消。'}
    def register(self):
        self.b.registry.register(Tool('read_briefing','查看本人获准项目的记录变化、台账待办或展开引用的简报。支持分页，来源读取时间保留，不把记录变化写成验收。',obj({'section':{'type':'string','enum':['summary','changes','todos']},'offset':{'type':'integer','minimum':0,'maximum':10000},'limit':{'type':'integer','minimum':1,'maximum':5}}),self.read,owner_only=True))
        self.b.registry.register(Tool('subscribe_briefing','准备本人私聊简报订阅，完整预览并确认后才启用。需要明确北京时间和截止日期；不代表新增信息采集权限。',obj({'frequency':{'type':'string','enum':['daily','workdays']},'time':string(5),'until':string(64),'topics':{'type':'array','minItems':1,'maxItems':2,'uniqueItems':True,'items':{'type':'string','enum':['changes','todos']}}},('frequency','time','until')),self.subscribe,'external',True,self.preview))
