"""Explicit project feedback and review, without enabling ledger writes."""
import json
from datetime import datetime
from zoneinfo import ZoneInfo
from .tools import Tool,obj,string,display_time
from .types import ConversationError

class ReportTools:
    def __init__(self,bridge):self.b=bridge;self.am=bridge.service;self.s=bridge.s
    def current(self,ctx,ident):
        r=self.am.updates.collection(ident)
        if not ctx.can_access_project(r['project_ref']):raise ConversationError('PROJECT_ACCESS_DENIED')
        if not ctx.is_owner and r['reporter']!=ctx.actor_id:raise ConversationError('REPORT_ACCESS_DENIED')
        return r
    def submit(self,ctx,args,event):
        self.b.authorize(ctx,'report',args);self.am.ledger.port.authorize_user(ctx.actor_id)
        row=self.b._current(ctx)['row'];p=self.b.projects.read_project(ctx,args['project_id'])
        if p['identity_status']!='verified':raise ConversationError('PROJECT_IDENTITY_UNVERIFIED')
        if args['content'] not in row['body']:raise ConversationError('REPORT_MUST_QUOTE_CURRENT_INPUT')
        r=self.am.updates.collect(ctx.actor_id,event,args['project_id'],args['content'],dict(self.am.collection_source(row),sharing='reporter_and_owner'),row['created'])
        return {'report_id':r['id'],'version':r['version'],'state':r['state'],'facts':json.loads(r['facts']),'message':'已把这段原话纳入项目反馈，供本人和管理人核对；正式台账、计划及验收状态未修改。'}
    def list(self,ctx,args,event):
        self.b.authorize(ctx,'read',args)
        if args.get('report_id'):rows=[self.current(ctx,args['report_id'])]
        else:rows=self.s.rows('SELECT * FROM progress_collections ORDER BY created DESC LIMIT 100')
        result=[]
        for r in rows:
            if not ctx.can_access_project(r['project_ref']) or args.get('project_id') and args['project_id']!=r['project_ref']:continue
            if not ctx.is_owner and r['reporter']!=ctx.actor_id:continue
            result.append({'id':r['id'],'project_id':r['project_ref'],'version':r['version'],'state':r['state'],'reported_at':r['created'],'reporter_confirmed':bool(r['reporter_confirmed']),'facts':json.loads(r['facts'])})
        return {'reports':result[:15],'has_more':len(result)>15,'ledger_write_enabled':False,'scope':'本人上报内容或管理人获准项目内的反馈；不是正式验收记录'}
    def content(self,r):
        from fs_agent.reminder_content import dates
        facts=json.loads(r['facts'])
        when=display_time(datetime.fromtimestamp(r['created'],ZoneInfo('Asia/Shanghai')).isoformat())
        labels={'reported':'自报待核','planned':'预计或计划','not_completed':'尚未完成','reported_completed':'自报完成，仍需核验'}
        return '收到反馈：'+when+'\n'+'\n'.join('原话：'+dates(f['original'],f['time_anchor'])+'（'+labels.get(f['status'],'待核')+'）' for f in facts)
    def preview(self,ctx,args):
        self.b.authorize(ctx,'manage',args);r=self.current(ctx,args['report_id'])
        if r['version']!=args['version'] or r['state']=='withdrawn':raise ConversationError('REPORT_VERSION_CHANGED')
        title=self.b.projects.read_project(ctx,r['project_ref'])['title']
        return '准备采用这条项目反馈：'+title+'\n'+self.content(r)+'\n适用范围：仅在你当前私聊的该项目中使用，不自动分享给其他同事；不写正式台账、不代表专业验收。'
    def adopt(self,ctx,args,key):
        self.b.authorize(ctx,'execute',args);action=self.b.store.get_action(ctx,key)
        if self.preview(ctx,args)!=action['preview']:return {'status':'failed'}
        r=self.current(ctx,args['report_id'])
        record=self.b.knowledge.submit(ctx,kind='fact',content=self.content(r),source={'uri':'am-report:'+r['id'],'observed_at':datetime.fromtimestamp(r['created'],ZoneInfo('Asia/Shanghai')).isoformat(),'evidence_state':'owner_reviewed_report_not_professional_acceptance','report_version':r['version']},scope='private',project_id=r['project_ref'])
        record=self.b.knowledge.approve(ctx,record['id'],record['version'])
        return {'status':'succeeded','verified':record['status']=='approved','receipt_id':record['id'],'message':'已按所示范围采用这条反馈。原报告状态、正式台账和专业验收状态保持各自依据。'}
    def register(self):
        self.b.registry.register(Tool('submit_project_update','把用户明确提交的项目进展原话放入本人和管理人的待核队列。content逐字引用；不写台账，不把自报变验收。',obj({'project_id':string(256),'content':string(4000)},('project_id','content')),self.submit,'local',final_reply=True))
        self.b.registry.register(Tool('list_project_updates','查看获准项目的待核反馈。成员只看本人上报，管理人看其获准项目；需要确认计划和验收仍保留待核。',obj({'project_id':string(256),'report_id':string(128)}),self.list))
        self.b.registry.register(Tool('adopt_project_update','准备将一条精确版本的项目反馈纳入本人私聊项目知识；完整预览后确认，不自动变为全员知识或正式台账。',obj({'project_id':string(256),'report_id':string(128),'version':{'type':'integer','minimum':1}},('report_id','version')),self.adopt,'external',True,self.preview))

def install_knowledge_review(bridge):
    def candidates(ctx,args,event):return {'candidates':bridge.knowledge.review_candidates(ctx,limit=15)}
    def revise(ctx,args,event):
        bridge.authorize(ctx,'manage',args)
        old=bridge.knowledge.get(ctx,args['record_id'])
        if old['version']!=args['version']:raise ConversationError('KNOWLEDGE_VERSION_CHANGED')
        record=bridge.knowledge.submit(ctx,kind=old['kind'],content=args['content'],source={'uri':'feishu-message:'+event,'observed_at':datetime.fromtimestamp(bridge.s.clock(),ZoneInfo('Asia/Shanghai')).isoformat()},scope=old['scope'],project_id=old.get('project_id'),logical_id=old['logical_id'],expected_version=args['version'])
        return {'record_id':record['id'],'version':record['version'],'status':'candidate','content':record['content'],'scope':record['scope'],'project_id':record.get('project_id')}
    bridge.registry.register(Tool('list_knowledge_candidates','查看当前可见的待采用知识及版本；私有会话之间不互相借用。',obj({}),candidates,owner_only=True))
    bridge.registry.register(Tool('revise_knowledge','修订明确的知识对象并形成新候选，批准前旧版仍有效。先取得原记录ID与版本，保留原适用范围。',obj({'record_id':string(128),'version':{'type':'integer','minimum':1},'content':string(6000)},('record_id','version','content')),revise,'local',True))
