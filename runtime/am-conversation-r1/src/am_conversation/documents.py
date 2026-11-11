"""Text entry points over the existing document lifecycle and optional standards host."""
import json,re,uuid
from .tools import Tool,obj,string
from .types import ConversationError,canonical

class DocumentTools:
    def __init__(self,bridge):
        self.b=bridge;self.am=bridge.service;self.s=bridge.s;self.logic=self.am.g.logic
        with self.s.lock:self.s.conn.execute('CREATE TABLE IF NOT EXISTS am_conversation_documents(draft_id TEXT PRIMARY KEY,scope TEXT NOT NULL,project_id TEXT,event_id TEXT NOT NULL)')

    def principal(self,ctx):
        row=dict(self.b._current(ctx)['row'])
        if row['chat_type']!='p2p' or not self.am.g.authorized(row):raise ConversationError('ORIGINAL_DOCUMENT_SCOPE_REQUIRED')
        return dict(row,message_id=row['id'],received=row['created'])

    def get(self,ctx,ident):
        row=self.principal(ctx);d=self.logic.get(row,ident)
        bindings=self.s.rows('SELECT * FROM am_conversation_documents WHERE draft_id=?',(ident,))
        if bindings and bindings[0]['scope']!=ctx.session_id:raise ConversationError('DOCUMENT_CONVERSATION_SCOPE')
        if bindings and bindings[0]['project_id'] and not ctx.can_access_project(bindings[0]['project_id']):raise ConversationError('PROJECT_ACCESS_DENIED')
        return row,d

    def queue(self,row,draft,event):
        # A derived internal job is linked to its actual inbound event. It is
        # never presented as a second, fabricated Feishu user message.
        internal='conversation-doc/'+event+'/'+draft['id']+'/'+str(draft['revision'])
        with self.s.tx() as c:
            c.execute("INSERT OR IGNORE INTO inbox(app,tenant,event_id,message_id,user,chat,chat_type,kind,body,received,expires,state) VALUES(?,?,?,NULL,?,?,?,'text','conversation-derived document job',?,?,'handled')",(row['app'],row['tenant'],internal,row['user'],row['chat'],row['chat_type'],row['received'],min(row['expires'],draft['expires'])))
            seq=c.execute('SELECT seq FROM inbox WHERE app=? AND tenant=? AND event_id=?',(row['app'],row['tenant'],internal)).fetchone()[0]
        return self.logic.queue(dict(row,seq=seq),draft)

    def standard_result_current(self,draft):
        standards=getattr(self.logic,'standards',None)
        if not self.logic.is_standard_draft(draft):return True
        if standards is None or not standards.context(draft):return False
        return bool(self.s.rows('SELECT 1 FROM amdoc_results WHERE draft_id=? AND revision=? AND expires>?',
                               (draft['id'],draft['revision'],self.s.clock())))

    def read(self,ctx,args,event):
        row=self.principal(ctx)
        from fs_agent.store import scope_of
        result={'available_types':['test','8d','charter'],'template_types':['charter','change','closure'] if getattr(self.logic,'standards',None) else [],
                'registered_targets':getattr(self.logic.business,'settings',{}).get('targets',{}),'drafts':[]}
        docs=[self.get(ctx,args['draft_id'])[1]] if args.get('draft_id') else self.s.rows('SELECT * FROM logic_drafts WHERE scope=? AND expires>? ORDER BY rowid DESC LIMIT 12',(scope_of(row),self.s.clock()))
        for d in docs:
            bindings=self.s.rows('SELECT scope,project_id FROM am_conversation_documents WHERE draft_id=?',(d['id'],))
            if bindings and bindings[0]['scope']!=ctx.session_id:continue
            if args.get('project_id') and (not bindings or bindings[0]['project_id']!=args['project_id']):continue
            item={k:d.get(k) for k in ('id','revision','state','document_type','content_hash','accepted_hash','target','error')}
            item['requires_regeneration_for_cloud']=not self.standard_result_current(d)
            if self.logic.is_standard_draft(d):
                from fs_agent.config import BoundaryError
                try:
                    self.logic.require_current_document_evidence(row,d)
                    item['evidence_current']=True
                except BoundaryError as exc:
                    item.update(evidence_current=False,evidence_error=str(exc),requires_regeneration_for_cloud=True)
                    if args.get('section') in ('content','review'):
                        raise ConversationError(str(exc)) from exc
            if args.get('section')=='content':item['content']=d['content']
            if args.get('section')=='review':item.update(analysis=json.loads(d['analysis'] or '{}'),review=json.loads(d['review'] or '{}'))
            if self.s.rows("SELECT 1 FROM sqlite_master WHERE type='table' AND name='amdoc_native_writes'"):
                item['published_versions']=[{'revision':v['revision'],'receipt':json.loads(v['receipt'])} for v in self.s.rows("SELECT revision,receipt FROM amdoc_native_writes WHERE draft_id=? AND state='verified' AND receipt IS NOT NULL ORDER BY rowid DESC LIMIT 3",(d['id'],))]
            result['drafts'].append(item)
        return result

    def delivery(self,ctx,args,event):
        row,d=self.get(ctx,args['draft_id'])
        if args.get('version') is not None and d['revision']!=args['version']:
            raise ConversationError('DOCUMENT_VERSION_CHANGED')
        standards=getattr(self.logic,'standards',None)
        if standards is None:raise ConversationError('DOCUMENT_STANDARDS_NOT_CONNECTED')
        result=standards.delivery_status(row,d)
        return dict(result,draft_id=d['id'],version=d['revision'],read_only=True,
                    adoption_recorded=bool(d.get('content_hash') and d.get('accepted_hash')==d['content_hash']))

    def edit(self,ctx,args,event):
        row=self.principal(ctx);op=args['operation']
        from fs_agent.store import scope_of
        if args.get('content') and op not in ('create','template','add_text','generate'):raise ConversationError('DOCUMENT_OPERATION_ARGUMENTS_INVALID')
        if op=='precheck':
            d=self.logic.start_precheck(row,args['target_alias'],args.get('kind','test'))
            self.s.execute('INSERT OR REPLACE INTO am_conversation_documents VALUES(?,?,?,?)',(d['id'],ctx.session_id,args.get('project_id'),event))
            self.s.select(row,'doc')
            self.s.execute("INSERT INTO bridge_mode VALUES(?,'poc',?) ON CONFLICT(scope) DO UPDATE SET mode='poc',expires=excluded.expires",(scope_of(row),row['expires']))
            return {'draft_id':d['id'],'version':d['revision'],'message':'已绑定资料预检目标，请直接发送材料。'}
        if op in ('create','template'):
            kind=args.get('kind','test')
            if op=='create' and kind not in ('test','8d','charter'):raise ConversationError('DOCUMENT_TYPE_UNKNOWN')
            context=None
            if op=='template':
                standards=getattr(self.logic,'standards',None)
                if standards is None:raise ConversationError('DOCUMENT_STANDARDS_NOT_CONNECTED')
                standards.authorize(row)
                if kind not in ('charter','change','closure') or args.get('project_type') not in ('IPD','IRAD','other') or args.get('level') not in ('L1','L2','L3'):raise ConversationError('DOCUMENT_TEMPLATE_SELECTION_REQUIRED')
                project=self.b.projects.read_project(ctx,args['project_id'])
                from fs_agent.project_catalog import business_ids
                codes=business_ids(project['title'])
                if len(codes)!=1:raise ConversationError('DOCUMENT_EXACT_PROJECT_CODE_REQUIRED')
                context={'doc_type':kind,'project_code':codes[0],'project_type':args['project_type'],'level':args['level'],'stage':{'charter':'initiation','change':'change','closure':'closure'}[kind]}
            # New text-created drafts preserve earlier work. Existing card/API
            # create behavior is untouched; each tool continues by exact ID.
            ident=uuid.uuid4().hex
            with self.s.tx() as c:
                c.execute("INSERT INTO logic_drafts(id,scope,revision,state,document_type,sources,answers,expires) VALUES(?,?,1,'collecting',?,'[]','{}',?)",(ident,scope_of(row),kind,row['expires']))
                c.execute('INSERT INTO am_conversation_documents VALUES(?,?,?,?)',(ident,ctx.session_id,args.get('project_id'),event))
                if context:c.execute('INSERT INTO amdoc_context VALUES(?,?,?)',(ident,canonical(context),row['expires']))
            d=self.logic.get(row,ident)
            self.s.select(row,'doc')
            self.s.execute("INSERT INTO bridge_mode VALUES(?,'poc',?) ON CONFLICT(scope) DO UPDATE SET mode='poc',expires=excluded.expires",(scope_of(row),row['expires']))
        else:
            row,d=self.get(ctx,args['draft_id'])
            if d['revision']!=args.get('version'):raise ConversationError('DOCUMENT_VERSION_CHANGED')
        if args.get('content'):
            if args['content'] not in self.b._current(ctx)['row']['body']:raise ConversationError('DOCUMENT_SOURCE_MUST_QUOTE_INPUT')
            from fs_agent.logic_service import link_only
            if link_only(args['content']):raise ConversationError('DOCUMENT_LINK_BODY_NOT_READ')
            self.logic.add_source(row,d,'对话补充.txt',args['content'].encode());d=self.logic.get(row,d['id'])
        if op in ('generate','template') or args.get('content'):
            task=self.queue(row,d,event)
            return {'draft_id':d['id'],'version':d['revision'],'state':'queued','task_id':task,'message':'资料已接收，正在通过原文档服务整理；完成后给你候选稿和待确认项。'}
        if op=='adopt':
            if not re.search(r'采用|用这版|就这版|确认文字|这版可以',self.b._current(ctx)['row']['body']):raise ConversationError('EXPLICIT_CONTENT_ADOPTION_REQUIRED')
            if d['content_hash']!=args.get('content_hash'):raise ConversationError('DOCUMENT_VERSION_CHANGED')
            self.logic.adopt(row,d['id']);message='已采用当前版本文字；保存云文档和挂载仍需分别确认。'
        elif op=='target':
            self.logic.select_target(row,d['id'],args['target_alias'])
            fresh=self.logic.get(row,d['id'])
            message=('已选择目标；请先重新生成此版本，再核对采用和保存。' if not self.standard_result_current(fresh)
                     else '已选择所示目标；请核对当前版本文字后再采用。')
        elif op=='cancel':self.logic.cancel(row,d['id']);message='这份文稿已停止后续处理，已发生的外部结果保留。'
        elif op=='resend':
            self.logic.require_current_document_evidence(row,d)
            jobs=self.s.rows('SELECT t.* FROM tasks t JOIN logic_jobs j ON j.task_id=t.id WHERE j.draft_id=? AND j.revision=? AND t.state=\'completed\' ORDER BY t.created DESC LIMIT 1',(d['id'],d['revision']))
            if not jobs:raise ConversationError('DOCUMENT_ARTIFACT_NOT_READY')
            from pathlib import Path
            task=jobs[0];path=Path(self.am.g.config.root)/'artifacts'/(task['id']+'.md')
            if not path.is_file() or path.is_symlink():raise ConversationError('DOCUMENT_ARTIFACT_NOT_READY')
            self.s.add_outbox(row,artifact=str(path),task_id=task['id'],private=True)
            message='正在把当前版本稿件发送到这个私聊。'
        elif op=='requirements':
            fresh=self.logic.set_precheck_requirements(row,args['requirements'],draft_id=d['id'],expected_revision=args['version'])
            return {'draft_id':fresh['id'],'version':fresh['revision'],'state':fresh['state'],'message':'已记录预检要求，需要按新要求重新检查。'}
        elif op=='reconcile':message=self.logic.reconcile_draft(row,d['id'])
        elif op=='cloud_reconcile':
            if not getattr(self.logic,'standards',None):raise ConversationError('DOCUMENT_STANDARDS_NOT_CONNECTED')
            action=self.b.store.action_metadata(ctx,args['action_id']) if args.get('action_id') else None
            if action and (action['tool_name']!='publish_document' or not action.get('idempotency_key')):
                raise ConversationError('DOCUMENT_ACTION_IDENTITY_REQUIRED')
            result=self.logic.standards.native_action_result(row,d,'云文档对账',idempotency_key=action['idempotency_key'] if action else None)
            if action and result.get('state')=='verified' and result.get('receipt') and action['status']=='unknown':
                self.b.store.finish_execution(ctx,action['id'],status='succeeded',result={
                    'status':'succeeded','verified':True,'receipt_id':result['receipt']['url'],
                    'idempotency_key':result['idempotency_key'],'message':result['message']})
            message=result['message']
        else:message='已建立文稿；可以直接发送文字或附件继续。'
        fresh=self.logic.get(row,d['id'])
        return {'draft_id':fresh['id'],'version':fresh['revision'],'state':fresh['state'],
                'content_hash':fresh.get('content_hash'), 'accepted_hash':fresh.get('accepted_hash'),
                'message':message}

    def publish_preview(self,ctx,args):
        self.b.authorize(ctx,'manage',args);row,d=self.get(ctx,args['draft_id'])
        if d['revision']!=args['version'] or d['content_hash']!=args['content_hash']:raise ConversationError('DOCUMENT_VERSION_CHANGED')
        self.logic.require_current_document_evidence(row,d)
        if args['operation']!='mount':
            standards=getattr(self.logic,'standards',None)
            if standards is None or not standards.context(d):raise ConversationError('DOCUMENT_STANDARD_RESULT_REQUIRED')
            if not self.standard_result_current(d):
                raise ConversationError('DOCUMENT_GENERATION_PENDING' if d['state']=='queued' else 'DOCUMENT_REGENERATION_REQUIRED_AFTER_REVISION_CHANGE')
        if d['state']!='ready' or d['accepted_hash']!=d['content_hash']:raise ConversationError('CONTENT_ADOPTION_REQUIRED')
        if args['operation']=='mount':
            if not d['target']:raise ConversationError('BUSINESS_TARGET_MISSING')
            target=json.loads(d['target'])
            if self.logic.business.resolve(row,target['id'])!=target:raise ConversationError('TARGET_REVISION_CHANGED')
            destination='挂载目标：'+canonical(target)
        else:
            destination='保存到原 AM 已配置的云文档输出目录；修改会保留原稿。'
            if args.get('sections'):destination+='\n指定修订部分：'+'、'.join(args['sections'])
        return f"文稿：{d['document_type']}，当前第{d['revision']}版已采用稿件\n"+destination+'\n内容指纹：'+d['content_hash'][:12]+'\n此操作不改变专业审核、验收或节点完成状态。'

    def publish(self,ctx,args,key):
        action=self.b.store.get_action(ctx,key)
        if self.publish_preview(ctx,args)!=action['preview']:return {'status':'failed'}
        row,d=self.get(ctx,args['draft_id'])
        if args['operation']=='mount':
            result=self.logic.mount(row,d['id']);receipt=result.get('receipt')
            if not receipt:return {'status':'unknown','message':result['message']}
            return {'status':'succeeded','verified':True,'receipt_id':receipt['url'],'message':result['message']+'\n'+receipt['url']}
        command='云端更新 '+','.join(args['sections']) if args['operation']=='update_cloud' else '保存云文档'
        result=self.logic.standards.native_action_result(row,d,command)
        receipt=result.get('receipt') or {};url=receipt.get('url')
        idem=result.get('idempotency_key')
        # Only this invocation's exact action may confirm completion. Same draft,
        # revision and content can have several different field selections.
        if result.get('state')!='verified' or not idem or not url:
            return {'status':'unknown','verified':False,'idempotency_key':idem,'message':result['message']}
        rows=self.s.rows("SELECT receipt FROM amdoc_native_writes WHERE idem=? AND draft_id=? AND revision=? AND content_hash=? AND state='verified'",
                         (idem,d['id'],d['revision'],d['content_hash']))
        if len(rows)!=1 or json.loads(rows[0]['receipt'] or '{}')!=receipt:
            return {'status':'unknown','verified':False,'idempotency_key':idem,'message':'本次云文档结果仍待精确对账。'}
        return {'status':'succeeded','verified':True,'idempotency_key':idem,'receipt_id':url,'message':result['message']}

    def register(self):
        r=self.b.registry
        r.register(Tool('read_documents','读取当前身份原文档服务的稿件、全文、检查结果及已配置目标；文档原权限不扩大。',obj({'project_id':string(256),'draft_id':string(128),'section':{'type':'string','enum':['summary','content','review']}}),self.read))
        r.register(Tool('read_document_delivery','只读检查这份标准稿能否正式交付、有哪些准确项目和文种匹配的既有目标、还缺什么；不选择目标、不采用文字、不保存或挂载。用于“这份稿能挂到哪里”“还差什么才能交付”。',obj({'draft_id':string(128),'version':{'type':'integer','minimum':1}},('draft_id',)),self.delivery,'read',True))
        r.register(Tool('edit_document','在原文档服务建立或继续文稿/预检，读取准确版本后修改。content逐字取本条原文。template模式需明确项目类型与等级并接入正式模板模块；附件继续原链路；resend向当前本人私聊重发已完成稿件。',obj({'project_id':string(256),'operation':{'type':'string','enum':['create','template','add_text','generate','adopt','target','cancel','precheck','requirements','reconcile','cloud_reconcile','resend']},'draft_id':string(128),'action_id':string(128),'version':{'type':'integer','minimum':1},'kind':{'type':'string','enum':['test','8d','charter','change','closure']},'project_type':{'type':'string','enum':['IPD','IRAD','other']},'level':{'type':'string','enum':['L1','L2','L3']},'content':string(6000),'content_hash':string(128),'target_alias':string(256),'requirements':string(4000)},('operation',)),self.edit,'local',final_reply=True))
        r.register(Tool('publish_document','准备已采用稿件的准确挂载/云端保存预览。版本、目标和内容均要核对，确认后执行；标准稿更换目标后，须先重新生成当前版本，再采用和保存；结果不明只对账。',obj({'project_id':string(256),'draft_id':string(128),'version':{'type':'integer','minimum':1},'content_hash':string(128),'operation':{'type':'string','enum':['mount','save_cloud','update_cloud']},'sections':{'type':'array','maxItems':20,'items':string(200)}},('draft_id','version','content_hash','operation')),self.publish,'external',True,self.publish_preview))
