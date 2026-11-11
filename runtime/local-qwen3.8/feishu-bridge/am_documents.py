"""Document views reuse the original question ledger, adoption and mount journal."""
import json
import uuid
from fs_agent.config import BoundaryError
from fs_agent.project_catalog import canonical,digest
from fs_agent.presentation import ResponseEnvelope
from fs_agent.card_ui import card,paragraph,button,bind_rendered_card
from fs_agent.store import scope_of
from fs_agent.document_view_binding import capture

class DocumentViewsMixin:
    def document_summary(self,d):
        analysis=json.loads(d['analysis'] or '{}')
        status={'ready':'阶段稿已形成' if d['stage'] else '候选稿已形成','collecting':'正在收集资料','dirty':'已收到新资料，待重新整理','paused':'已暂停','cancelled':'已放弃'}.get(d['state'],'正在整理')
        lines=[status+'；'+('已采用当前文字，尚需核对挂载状态。' if d['accepted_hash'] and d['accepted_hash']==d['content_hash'] else '尚未采用或挂载。')]
        for q in analysis.get('questions',[])[:2]:lines.append(q['ask'])
        if not analysis.get('questions'):lines.append('可打开稿件与核对报告，确认后再选择后续操作。')
        return '\n'.join(lines)

    def document_completion(self,task,result,conn):
        if task['chat_type']!='p2p':return False
        job=conn.execute('SELECT draft_id FROM logic_jobs WHERE task_id=?',(task['id'],)).fetchone()
        if not job:return False
        d=dict(conn.execute('SELECT * FROM logic_drafts WHERE id=?',(job[0],)).fetchone())
        binding=capture(self.g.logic,task,d)
        row=dict(task,kind='text',id=task['id'],expires=binding['expires'],_document_binding=binding)
        if getattr(self,'conversation',None) is not None and conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='am_conversation_documents'").fetchone() and conn.execute('SELECT 1 FROM am_conversation_documents WHERE draft_id=?',(d['id'],)).fetchone():
            self.notify(row,text=self.document_summary(d)+'\n可以直接说“查看稿件”“查看核对报告”，或继续补充资料。',kind='document_result',object_id=d['id'],version=d['revision'],conn=conn,event_key='doc/'+task['id'])
            return True
        nonce=digest(['document-result',task['id'],d['revision']])
        actions=[('查看稿件',{'page':'documents','draft':d['id'],'section':'content'}),('处理稿件',{'page':'documents','draft':d['id'],'section':'actions'}),('核对报告',{'page':'documents','draft':d['id'],'section':'review'})]
        summary=self.document_summary(d)
        env=ResponseEnvelope(task['id'],1,[d['id']],d['state'],summary,detail_ref={'draft':d['id'],'revision':d['revision']}).validate()
        snapshot={'actions':{str(i):a for i,(_,a) in enumerate(actions)},'fields':None,'data':{'document_revision':d['revision'],'document_binding':binding},'thread':None,'app':task['app'],'tenant':task['tenant'],'envelope':env}
        controls=[button(label,{'flow':'am_self','nonce':nonce,'action':str(i)}) for i,(label,_) in enumerate(actions)]
        controls[0]['type']='primary'
        rendered=bind_rendered_card(card('文稿整理', [paragraph(summary)]+controls))
        out=self.notify(row,card=rendered,kind='document_result',object_id=d['id'],version=d['revision'],hash=digest(snapshot),conn=conn,event_key='doc/'+task['id'])
        conn.execute('INSERT OR IGNORE INTO am_cards VALUES(?,?,?,?,?,?,?,?,?,?,NULL)',(nonce,'ux',d['id'],d['revision'],task['user'],task['chat'],canonical(snapshot),digest(snapshot),binding['expires'],out))
        return True

    def ux_documents(self,row,a):
        logic=self.g.logic;ident=a.get('draft');section=a.get('section')
        standards=getattr(logic,'standards',None)
        if section=='standards_help':
            if not standards:raise BoundaryError('DOCUMENT_STANDARDS_UNAVAILABLE')
            standards.authorize(row)
            return self.ux_page(row,'按台账模板写作','在当前对话发送：\n/doc 模板 章程 项目编号 IRAD L1\n\n文种可选：章程、变更申请、结项报告。请填写实际项目编号、类型和级别；之后可继续发本项目文字或附件。模板与标准将实时读取，缺少的依据保留待补。',[('返回文稿整理',{'page':'documents'})])
        if a.get('kind'):
            d=logic.create(row,a['kind']);self.s.select(row,'doc')
            self.s.execute("INSERT INTO bridge_mode VALUES(?,'poc',?) ON CONFLICT(scope) DO UPDATE SET mode='poc',expires=excluded.expires",(self.scope(row),row['expires']))
            return self.ux_page(row,'文稿整理','请发送文字或附件。当前文种已选定；可一次提供已有资料。',[('查看当前稿',{'page':'documents','draft':d['id']}),('返回首页',{'page':'home'})])
        d=logic.get(row,ident) if ident else logic.current(row)
        if not d:
            actions=[(label,{'page':'documents','kind':kind}) for kind,label in [('test','测试报告'),('8d','异常分析'),('charter','项目章程')]]
            if standards and row['user']==self.owner:actions.insert(0,('按台账模板写作',{'page':'documents','section':'standards_help'}))
            return self.ux_page(row,'文稿整理','选择文种后发送资料。',actions)
        base={'page':'documents','draft':d['id']};back=[('返回当前稿',base)]
        # All document-derived pages (including summary/questions) inherit the
        # exact draft/evidence fence. Pure result reconciliation remains available.
        if section not in ('reconcile','native_reconcile','mount'):
            binding=capture(logic,row,d)
            row=dict(row,_document_binding=binding,expires=binding['expires'])
        else:
            row=dict(row,_document_receipt=True)
        if section in ('content','review'):
            if standards and logic.is_standard_draft(d):standards.authorize(row)
            logic.require_current_document_evidence(row,d)
            text=d['content'] or '尚无当前稿件。'
            if section=='review':
                review=json.loads(d['review'] or '{}');analysis=json.loads(d['analysis'] or '{}')
                text='核对报告\n'+analysis.get('assessment','尚未完成核对。')+'\n'+json.dumps(review,ensure_ascii=False,indent=2)
            return self.ux_page(row,'稿件全文' if section=='content' else '完整核对报告',text,back,data={'detail':True})
        if section=='actions':
            actions=[]
            if d['state']=='ready' and d['content_hash'] and d['accepted_hash']!=d['content_hash']:actions.append(('采用当前文字',dict(base,section='adopt',revision=d['revision'],hash=d['content_hash'])))
            actions += [('选择挂载目标',dict(base,section='targets'))]
            if standards and standards.context(d) and d['state']=='ready':
                actions += [('保存为本人云文档',dict(base,section='save_native',revision=d['revision'],hash=d['content_hash'])),
                    ('修订云文档章节',dict(base,section='native_sections')),('核对云文档',dict(base,section='native_reconcile'))]
            if d['accepted_hash'] and d['accepted_hash']==d['content_hash'] and d['target']:actions.append(('查看挂载预览',dict(base,section='mount_preview')))
            return self.ux_page(row,'稿件操作',self.document_summary(d),actions+back)
        if section in ('save_native','native_update','native_reconcile','native_sections'):
            if not standards:raise BoundaryError('DOCUMENT_STANDARDS_UNAVAILABLE')
            standards.authorize(row)
            if section=='native_sections':
                logic.require_current_document_evidence(row,d)
                stored=self.s.rows('SELECT payload FROM amdoc_results WHERE draft_id=? AND revision=?',(d['id'],d['revision']))
                if not stored:raise BoundaryError('DOCUMENT_STANDARD_RESULT_REQUIRED')
                profile=json.loads(stored[0]['payload'])['profile']
                return self.ux_page(row,'选择要更新的章节','先补充资料并形成新稿，再选择本次更新范围。已有人工修改会核对；保存为新版本，原稿保留。',
                    [(s['title'],dict(base,section='native_update',label=s['title'],revision=d['revision'],hash=d['content_hash'])) for s in profile['sections']]+back)
            if section!='native_reconcile' and (d['revision']!=a['revision'] or d['content_hash']!=a['hash']):raise BoundaryError('LOGIC_NO_CURRENT_DRAFT')
            command='云文档对账' if section=='native_reconcile' else '云端更新 '+a['label'] if section=='native_update' else '保存云文档'
            return self.ux_page(row,'云文档结果',standards.native_action(row,d,command),back)
        if section=='adopt':
            if d['revision']!=a['revision'] or d['content_hash']!=a['hash']:raise BoundaryError('LOGIC_NO_CURRENT_DRAFT')
            logic.adopt(row,d['id']);return self.ux_documents(row,dict(base,section='actions'))
        if section=='targets':
            targets=getattr(logic.business,'settings',{}).get('targets',{})
            return self.ux_page(row,'选择挂载目标','采用文字与挂载为独立操作。',[(v.get('label',key),dict(base,section='target',target=key)) for key,v in targets.items()]+back)
        if section=='target':
            logic.select_target(row,d['id'],a['target']);return self.ux_documents(row,base)
        if section=='mount_preview':
            if not d['target'] or d['accepted_hash']!=d['content_hash']:raise BoundaryError('CONTENT_ADOPTION_REQUIRED')
            target=json.loads(d['target'])
            if logic.business.resolve(row,target['id'])!=target:raise BoundaryError('TARGET_REVISION_CHANGED')
            text='准确挂载目标：\n'+'\n'.join(str(k)+'：'+str(v) for k,v in target.items())+'\n当前稿件版本：'+str(d['revision'])+'\n稿件指纹：'+d['content_hash']+'\n仅挂载已采用稿件，不改变专业审核或节点完成状态。'
            return self.ux_page(row,'准确挂载预览',text,[('确认挂载当前稿',dict(base,section='mount',revision=d['revision'],hash=d['content_hash'],target=target)),('查看稿件',dict(base,section='content'))],data={'full_preview':True})
        if section=='mount':
            if d['revision']!=a['revision'] or d['content_hash']!=a['hash'] or json.loads(d['target'] or '{}')!=a['target']:raise BoundaryError('TARGET_REVISION_CHANGED')
            result=logic.mount(row,d['id']);text=result['message']
            if result.get('receipt'):text+='\n查看交付物：'+result['receipt']['url']
            return self.ux_page(row,'挂载结果',text,[('核对原挂载',dict(base,section='reconcile')),('返回当前稿',base)],data={'immutable':True,'business_event':digest(['mount',d['id'],d['revision'],d['content_hash']])})
        if section=='reconcile':return self.ux_page(row,'挂载核对',logic.reconcile_draft(row,d['id']),back)
        return self.ux_page(row,'当前稿件',self.document_summary(d),[('查看稿件',dict(base,section='content')),('处理稿件',dict(base,section='actions')),('核对报告',dict(base,section='review'))])
