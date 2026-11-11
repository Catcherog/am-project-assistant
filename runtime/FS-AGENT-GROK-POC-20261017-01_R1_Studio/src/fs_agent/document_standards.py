"""AM's opt-in template document path, sharing the existing lifecycle and provider."""
from __future__ import annotations

import json
import os
import re
import time
import uuid
from dataclasses import replace
from pathlib import Path

from am_doc_engine.adapters import ProviderSession
from am_doc_engine.host_bridge import prepare_am_document
from am_doc_engine.rendering import render_markdown, build_report
from am_doc_engine.models import AccessContext, DraftRequest, SourceAsset, canonical, fingerprint, to_dict
from am_doc_engine.storage import SQLiteRunStore
from .config import BoundaryError
from .doc_logic import digest
from .document_mapping import blueprint, map_facts, policy_asset, admit_project_input
from .document_native import NativePublisher
from .document_sources import NativeDocs, OwnerCatalog, OwnerNativeDocs
from .document_evidence import CONTRACT, require_current, coverage_issues
from .document_delivery import discover_targets, render_targets
from .provider import Result
from .store import scope_of

SCHEMA='''
CREATE TABLE IF NOT EXISTS amdoc_context(draft_id TEXT PRIMARY KEY,context TEXT NOT NULL,expires REAL NOT NULL);
CREATE TABLE IF NOT EXISTS amdoc_results(draft_id TEXT NOT NULL,revision INTEGER NOT NULL,payload TEXT NOT NULL,expires REAL NOT NULL,PRIMARY KEY(draft_id,revision));
CREATE TABLE IF NOT EXISTS amdoc_run_bindings(run_key TEXT PRIMARY KEY,draft_id TEXT NOT NULL,expires REAL NOT NULL);
'''


def deferred_question_keys(text,questions):
    """An ambiguous reply cannot close unrelated gaps."""
    if not re.search('无法提供|暂时没有|不再追问',text):return set()
    if len(questions)==1 or re.search('全部|这些|以上|都无法|均无法',text):
        return {q['key'] for q in questions}
    matches=set()
    for question in questions:
        parts=[re.sub(r'^\d+[.、 ]*','',part).strip() for part in question.get('label','').split(' / ')]
        if any(len(part)>=3 and part in text for part in parts):matches.add(question['key'])
    return matches


class ProviderControlError(Exception):
    """The core must not turn AM runtime/stop errors into a successful fallback draft."""
    def __init__(self,original):self.original=original;super().__init__(str(original))


class SharedProvider:
    def __init__(self,provider,*,guard=None,deadline=None,trace=None):
        self.provider=provider;self.calls=0;self.elapsed=0;self.guard=guard;self.deadline=deadline;self.trace=trace;self.stage='model'
    def complete(self,*args,**kwargs):
        started=time.monotonic();result=None;error=None;authorized=None;attempted=False
        timing_before=getattr(self.provider,'last_timing',None)
        try:
            # Use the same pinned native Qwen path as the original AM query
            # worker. It explicitly disables thinking and tools, without
            # changing the model, output limit or shared remaining deadline.
            kwargs['native_chat']=True
            if self.guard and self.guard() is False:raise BoundaryError('DOCUMENT_PROVIDER_AUTH_CHANGED')
            authorized=time.monotonic()-started
            if self.deadline is not None:
                remaining=self.deadline-time.monotonic()
                if remaining<=0:raise BoundaryError('EXECUTION_TIMEOUT')
                kwargs['timeout']=min(kwargs['timeout'],remaining)
            attempted=True
            result=self.provider.complete(*args,**kwargs)
            if self.guard and self.guard() is False:raise BoundaryError('DOCUMENT_PROVIDER_AUTH_CHANGED')
        except BoundaryError as exc:
            error=str(exc) if re.fullmatch(r'[A-Z][A-Z0-9_]{0,100}',str(exc)) else type(exc).__name__
            raise ProviderControlError(exc) from exc
        except BaseException as exc:
            error=type(exc).__name__;raise
        finally:
            if self.trace:
                self.trace.call({'stage':self.stage,'seconds':time.monotonic()-started,
                    'authorization_seconds':authorized,'timeout_seconds':kwargs.get('timeout'),
                    'remaining_seconds':max(0,self.deadline-time.monotonic()) if self.deadline else None,
                    'prompt_chars':len(args[0]) if args else 0,'max_tokens':kwargs.get('max_tokens'),
                    'usage':getattr(result,'usage',None),'error':error,
                    'provider_timing':getattr(self.provider,'last_timing',[]) if attempted and getattr(self.provider,'last_timing',None) is not timing_before else []})
        self.calls+=1;self.elapsed+=result.elapsed
        return result


class HostRunStore(SQLiteRunStore):
    def __init__(self,host,draft,*,before_complete=None):
        self.host,self.draft=host,draft;self.conn=host.conn;self._lock=host.lock;self.clock=host.clock
        self.before_complete=before_complete
        # Reuse the reference schema, but never open a second connection to AM's DB.
        reference=SQLiteRunStore()
        schema=';\n'.join(r[0] for r in reference.conn.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name LIKE 'amdoc_%'"))+';'
        reference.close()
        with host.lock:host.conn.executescript(schema+SCHEMA)
    def tx(self):return self.host.tx()
    def close(self):pass
    def claim(self,key,input_hash,**kwargs):
        with self.tx() as conn:
            conn.execute('INSERT OR IGNORE INTO amdoc_run_bindings VALUES(?,?,?)',(key,self.draft['id'],self.draft['expires']))
        return super().claim(key,input_hash,**kwargs)
    def complete(self,lease,document):
        if self.before_complete is not None and self.before_complete() is False:
            raise BoundaryError('DOCUMENT_SHARED_EVIDENCE_CHANGED')
        return super().complete(lease,document)


def load_settings(root):
    path=root/'config'/'document-standards.json'
    if not path.exists():return None
    info=path.stat()
    if path.is_symlink() or info.st_uid!=os.getuid() or info.st_mode&0o077:
        raise BoundaryError('DOCUMENT_STANDARDS_CONFIG_NOT_PRIVATE')
    settings=json.loads(path.read_text())
    if settings.get('enabled') is not True:return None
    if settings.get('ledger_write') is not False or settings.get('catalog_identity')!='owner':
        raise BoundaryError('DOCUMENT_STANDARDS_SCOPE_INVALID')
    return settings


class DocumentStandards:
    def __init__(self,logic,settings):
        self.logic,self.g,self.s,self.cfg=logic,logic.g,logic.s,settings
        self.progress=lambda stage:None
        with self.s.lock:self.s.conn.executescript(SCHEMA)

    def context(self,draft):
        rows=self.s.rows('SELECT context FROM amdoc_context WHERE draft_id=? AND expires>?',(draft['id'],self.s.clock()))
        return json.loads(rows[0]['context']) if rows else None

    def validate_target(self,row,draft,target):
        context=self.context(draft)
        if not context:return
        if draft.get('state')=='ready' and draft.get('content_hash'):
            self.require_current_evidence(row,draft)
        # A legacy mount target is not evidence that it belongs to this project's
        # new template draft. Bind both document type and business project code.
        reader=getattr(self.logic.business,'document_identity',None)
        if not callable(reader):raise BoundaryError('DOCUMENT_MOUNT_IDENTITY_UNAVAILABLE')
        identity=reader(row,target)
        if identity.get('project_code')!=context['project_code'] or identity.get('doc_type')!=context['doc_type']:
            raise BoundaryError('DOCUMENT_MOUNT_PROJECT_OR_TYPE_MISMATCH')

    def require_current_evidence(self,row,draft,*,payload=None):
        """Guard reuse/adoption/publication; old unbound drafts require regeneration."""
        rules=json.loads(draft.get('rules') or '{}')
        context=self.context(draft)
        if not context:
            if rules.get('engine','').startswith('am-doc-standards'):
                raise BoundaryError('DOCUMENT_SHARED_EVIDENCE_EXPIRED')
            return True
        if payload is None:
            results=self.s.rows('SELECT payload FROM amdoc_results WHERE draft_id=? AND revision=? AND expires>?',
                (draft['id'],draft['revision'],self.s.clock()))
            payload=json.loads(results[0]['payload']) if results else {}
        binding=payload.get('document_binding') or rules.get('document_binding')
        if binding is None and getattr(self.logic,'shared_evidence',None) is None:
            return True  # Explicit legacy-only installation, not a shared fallback.
        project_id=payload.get('request',{}).get('project_id') or rules.get('project_id')
        return require_current(row,binding,getattr(self.logic,'shared_evidence_current',None),
            now=self.s.clock(),project_code=context['project_code'],project_id=project_id)

    def delivery_status(self,row,draft):
        """Discover existing official bindings without selecting or mounting one."""
        self.authorize(row);context=self.context(draft)
        if not context:raise BoundaryError('DOCUMENT_STANDARD_RESULT_REQUIRED')
        deadline=time.monotonic()+min(120,draft['expires']-self.s.clock())
        guard=lambda:self.guard(row,draft,{'revision':draft['revision']},deadline,live=True)
        result=discover_targets(self.logic.business,row,project_code=context['project_code'],
            doc_type=context['doc_type'],guard=guard,deadline=deadline)
        return dict(result,message=render_targets(result))

    def authorize(self,row):
        if (row['user']!=self.g.config.owner or row['chat_type']!='p2p' or not self.g.authorized(row)
                or row['expires']<=self.s.clock()):raise BoundaryError('DOCUMENT_STANDARDS_SCOPE_DENIED')
        # Interactive/live authorization is outside the local commit fence.
        # Renew only through the original identity path; the final verifier
        # remains a no-network check of the existing, unextended lease.
        checker=getattr(self.logic,'shared_evidence_authorize',None)
        if checker is not None and checker(row) is not True:
            raise BoundaryError('DOCUMENT_STANDARDS_SCOPE_DENIED')

    def begin(self,row,argument):
        self.authorize(row);parts=argument.split()
        if parts and parts[0]=='模板':parts[0]='template'
        if len(parts)>1:parts[1]={'章程':'charter','项目章程':'charter','变更申请':'change','项目更改申请':'change','结项报告':'closure'}.get(parts[1],parts[1])
        if len(parts)>3 and parts[3]=='其他':parts[3]='other'
        if len(parts)!=5 or parts[0]!='template' or parts[1] not in ('charter','change','closure') or parts[3] not in ('IPD','IRAD','other') or parts[4] not in ('L1','L2','L3'):
            raise BoundaryError('用法：/doc template charter或change或closure 项目编号 IPD或IRAD或other L1或L2或L3')
        from .project_catalog import business_ids
        if business_ids(parts[2])!=[parts[2].upper()]:raise BoundaryError('DOCUMENT_EXACT_PROJECT_CODE_REQUIRED')
        # Stage and type are declared selection parameters, not an approval claim.
        context={'doc_type':parts[1],'project_code':parts[2].upper(),'project_type':parts[3],
            'level':parts[4],'stage':{'charter':'initiation','change':'change','closure':'closure'}[parts[1]]}
        draft=self.logic.create(row,parts[1])
        self.s.execute('INSERT INTO amdoc_context VALUES(?,?,?)',(draft['id'],canonical(context),draft['expires']))
        self.logic.queue(row,draft)

    def guard(self,task,draft,job,deadline,*,live=False):
        if live:self.authorize(task)
        rows=self.s.rows('SELECT * FROM logic_drafts WHERE id=? AND scope=? AND expires>?',
                         (draft['id'],scope_of(task),self.s.clock()))
        if (not rows or task['user']!=self.g.config.owner or task['app']!=self.g.config.app_id
                or task['tenant']!=self.g.config.tenant_key or task['expires']<=self.s.clock()):
            raise BoundaryError('DOCUMENT_STANDARDS_SCOPE_DENIED')
        fresh=rows[0]
        if fresh['revision']!=job['revision'] or fresh['state'] in ('paused','cancelled','expired'):
            raise BoundaryError('LATE_RESULT_REJECTED')
        if time.monotonic()>=deadline:raise BoundaryError('EXECUTION_TIMEOUT')
        return True

    def project(self,docs,context):
        from group_ledger import parse_rows
        from .project_catalog import business_ids
        ledger=self.cfg['ledger'];token=ledger['token'];sheet=ledger['sheet_id']
        info=docs.call('GET','/open-apis/sheets/v3/spreadsheets/'+token+'/sheets/query')
        matches=[s for s in info.get('sheets',[]) if s.get('sheet_id')==sheet]
        if len(matches)!=1:raise BoundaryError('DOCUMENT_LEDGER_SHEET_CHANGED')
        count=matches[0].get('grid_properties',{}).get('row_count',0)
        if not isinstance(count,int) or not 2<=count<=1000:raise BoundaryError('DOCUMENT_LEDGER_SIZE_INVALID')
        selected_range=sheet+'!A1:G'+str(count)
        data=docs.call('GET','/open-apis/sheets/v2/spreadsheets/'+token+'/values/'+selected_range,
            query={'valueRenderOption':'ToString','dateTimeRenderOption':'FormattedString'})
        value=data.get('valueRange',{})
        if value.get('range')!=selected_range or data.get('revision') is None:raise BoundaryError('DOCUMENT_LEDGER_PARTIAL')
        projects=parse_rows(value['values'])
        selected=[p for p in projects if context['project_code'] in business_ids(p['title'])]
        if len(selected)!=1 or len(business_ids(selected[0]['title']))!=1:raise BoundaryError('DOCUMENT_PROJECT_AMBIGUOUS')
        item=selected[0];ident='project-'+fingerprint(context['project_code'])[:24]
        uri=ledger['url']+'?sheet='+sheet+'&range=A'+str(item['row'])+':G'+str(item['row'])
        source={'id':'ledger','uri':uri,'version':str(data['revision']),'read_status':'complete',
            'provenance':'current_project','project_id':ident,'segments':[]}
        for key in ('关键日期','项目进展','人员与职责','待办'):
            for i,line in enumerate(item.get(key,'').splitlines()):
                if line.strip():source['segments'].append({'id':'field-'+fingerprint(key)[:6]+'-'+str(i),'original':key+'：'+line})
        return {'id':ident,'title':item['title'],'code':context['project_code']},source

    def project_evidence(self,task,context,guard,deadline):
        shared=getattr(self.logic,'shared_evidence',None)
        if shared is not None:
            origin=dict(task,chat_type='p2p',body='',id=task['id'])
            item,bundle=shared(origin,context['project_code'])
            revision=item.get('source_revision') or (item.get('snapshot') or {}).get('version')
            if revision is None:raise BoundaryError('DOCUMENT_LEDGER_REVISION_UNVERIFIED')
            project={'id':item['key'],'title':item['title'],'code':context['project_code'],
                'source':{'source_id':'demo-ledger','uri':self.cfg['ledger']['url'],'version':str(revision)}}
            ledger_source=bundle.document_source(uri=self.cfg['ledger']['url'],
                legacy_project_id='project-'+fingerprint(context['project_code'])[:24])
            if ledger_source.get('evidence_contract')!=CONTRACT:
                raise BoundaryError('DOCUMENT_SHARED_EVIDENCE_REGENERATE_REQUIRED')
            require_current(origin,ledger_source.get('document_binding'),getattr(self.logic,'shared_evidence_current',None),
                now=self.s.clock(),project_code=context['project_code'],project_id=project['id'])
        else:
            project,ledger_source=self.project(NativeDocs(self.g.api.client,guard,deadline=deadline),context)

        return project,ledger_source

    def prepare(self,task,draft,job,deadline,started,sources):
        from .document_trace import DocumentTrace
        trace=DocumentTrace(self.s,task,draft,job,deadline)
        error=None
        try:return self._prepare(task,draft,job,deadline,started,sources,trace)
        except BaseException as exc:
            error=exc;raise
        finally:trace.finish(error)

    def _prepare(self,task,draft,job,deadline,started,sources,trace):
        trace.phase('authorization')
        context=self.context(draft)
        if not context:return None
        self.authorize(task)
        binding=None;project=None;evidence_expires=min(task['expires'],draft['expires'])
        def guard(*,live=False):
            checked=time.monotonic()
            try:
                self.guard(task,draft,job,deadline,live=live)
                if binding is not None:
                    require_current(task,binding,getattr(self.logic,'shared_evidence_current',None),now=self.s.clock(),
                        project_code=context['project_code'],project_id=project['id'])
            finally:trace.guard('full',time.monotonic()-checked)
            return True
        def local_guard():
            # Pure field assembly uses the already pinned snapshot. It cannot
            # call the provider or publish; those boundaries still use guard().
            # Never cache a positive authorization or extend evidence leases.
            checked=time.monotonic()
            try:
                self.guard(task,draft,job,deadline)
                if binding is not None:
                    from .document_evidence import validate_binding
                    validate_binding(task,binding,now=self.s.clock(),project_code=context['project_code'],project_id=project['id'])
            finally:trace.guard('local',time.monotonic()-checked)
            return True
        cached=self.s.rows('SELECT payload FROM amdoc_results WHERE draft_id=? AND revision=? AND expires>?',
                           (draft['id'],job['revision'],self.s.clock()))
        if cached and draft['state']=='ready' and draft['content'] and digest(draft['content'])==draft['content_hash']:
            guard()
            cached_payload=json.loads(cached[0]['payload'])
            self.require_current_evidence(task,draft,payload=cached_payload)
            cached_binding=cached_payload.get('document_binding')
            if cached_binding:evidence_expires=min(evidence_expires,cached_binding['expires'])
            return draft['content'],Result('复用当前候选稿；本次已核对依据版本，未重新调用模型。','cached:am-doc-standards',0),evidence_expires
        guard()
        # The app's access to a document is not an actor's access. Read template
        # bodies with the same pinned owner identity that can see their directory.
        docs=OwnerNativeDocs(self.cfg['catalog'],task,self.g.config.owner,guard,deadline=deadline)
        self.progress('catalog')
        trace.phase('catalog_read')
        start=time.monotonic();catalog=OwnerCatalog(self.cfg['catalog'],task,self.g.config.owner,guard,deadline=deadline).read()
        mapping=self.cfg['template_codes'][context['doc_type']]
        code=mapping[context['level']] if isinstance(mapping,dict) else mapping
        matches=[r for r in catalog['rows'] if r.get('文件编号')==code and r.get('模板状态')==['现行']]
        if len(matches)!=1:raise BoundaryError('DOCUMENT_TEMPLATE_SELECTION_CONFLICT')
        entry=matches[0];urls=re.findall(r'https://[^\s)\]]+',entry['来源链接'])
        urls=list(dict.fromkeys(urls))
        if len(urls)!=1:raise BoundaryError('DOCUMENT_TEMPLATE_LINK_AMBIGUOUS')
        trace.phase('template_read')
        native=docs.read(docs.resolve(urls[0],self.cfg['hosts']))
        self.progress('template_read')
        record={'record_id':entry['record_id'],'title':entry['模板名称'],'source_url':urls[0],
            'effective_at':entry['更新时间'],'activation_ref':self.cfg['catalog']['url']+'#'+entry['record_id']+'@'+catalog['revision']}
        assets=[SourceAsset('native-template','template',urls[0],str(native['revision']),native['text'],native['read_state'],native['title'])]
        instruction=None;source_versions=[]
        for configured in self.cfg['required_sources']:
            trace.phase('source_read:'+configured['id'])
            read=docs.read(docs.resolve(configured['url'],self.cfg['hosts']))
            self.progress('source_'+configured['id'])
            if read['read_state']!='complete':raise BoundaryError('DOCUMENT_STANDARD_PARTIAL')
            assets.append(SourceAsset(configured['id'],configured['role'],configured['url'],str(read['revision']),read['text'],'complete',read['title']))
            source_versions.append({'id':configured['id'],'revision':read['revision'],'hash':read['hash'],'pages':read['pages']})
            if configured['id']=='instructions':instruction=read
        if instruction is None:raise BoundaryError('DOCUMENT_INSTRUCTIONS_MISSING')
        assets.append(policy_asset(instruction))
        trace.phase('template_mapping')
        profile,slots=blueprint(native,record=record,tenant=task['tenant'],actor=task['user'],doc_type=context['doc_type'],
            project_type=context['project_type']+':'+context['level'],stage=context['stage'],assets=assets)
        read_elapsed=time.monotonic()-start
        trace.phase('project_evidence_read')
        project,ledger_source=self.project_evidence(task,context,guard,deadline)
        binding=ledger_source.get('document_binding')
        if binding is not None:evidence_expires=min(evidence_expires,binding['expires'])
        guard()
        self.progress('project_read')
        scoped=[ledger_source]
        for source in sources:
            # Existing draft attachments were admitted under this actor and draft;
            # their role stays project input, never a standard or approved decision.
            scoped.append(admit_project_input(source,project['id'],context['project_code']))
        access=AccessContext(task['tenant'],task['user'],(project['id'],),app_id=task['app'],chat_id=task['chat'])
        model_guard=lambda:guard(live=True)
        provider=SharedProvider(self.g.provider,guard=model_guard,deadline=deadline,trace=trace)
        # This is a subdivision of the original absolute deadline, never a
        # renewed budget. Keep time for composition, review and local commit.
        remaining=deadline-time.monotonic()
        if remaining<5:raise BoundaryError('DOCUMENT_MAPPING_BUDGET_INSUFFICIENT')
        mapping_deadline=deadline-min(35,remaining*.45)
        session=ProviderSession(provider,run_id=task['run_id'],deadline_monotonic=mapping_deadline,timeout=120,on_start=started,ensure_current=local_guard,max_tokens=1536)
        request=DraftRequest(draft['id']+'-r'+str(job['revision']),task['tenant'],project['id'],context['doc_type'],
            context['project_type']+':'+context['level'],context['stage'])
        try:
            self.progress('fact_mapping')
            trace.phase('fact_mapping');provider.stage='fact_mapping'
            snapshot=map_facts(profile,project=project,sources=scoped,access=access,session=session,
                source_uri=ledger_source['uri'],source_revision=ledger_source['version'],supplied_type=context['project_type'],compact=True)
            run_store=HostRunStore(self.s,dict(draft,expires=evidence_expires),before_complete=guard)
            self.progress('compose_and_review')
            trace.phase('compose_and_review');provider.stage='compose_and_review'
            result=prepare_am_document(task=task,request=request,access=access,profiles=(profile,),snapshot=snapshot,
                store=run_store,provider=provider,ensure_current=local_guard,deadline_monotonic=deadline,on_start=started,
                max_tokens=1536,review_reserve_seconds=min(15,max(0,deadline-time.monotonic())*.4))
            limitations=coverage_issues(ledger_source)
            if limitations or any(f.status in ('missing','conflict') for f in result.document.fields):
                # Optional blanks remain visible without inventing mandatory company
                # requirements or calling an incomplete document fully ready.
                doc=replace(result.document,state='draft_with_gaps',issues=result.document.issues+limitations)
                report=build_report(doc,profile,snapshot)
                result=replace(result,document=doc,content=render_markdown(doc,profile),report=report,reply_text='已按当前模板整理草稿；缺少的资料已明确保留。')
            self.progress('model_complete')
            trace.phase('result_validation_and_commit')
        except ProviderControlError as exc:raise exc.original
        model_guard()
        questions=[]
        for issue in result.document.issues:
            if issue.code=='shared_evidence_incomplete':
                questions.append({'key':'shared-evidence','label':'项目资料核对','ask':issue.message})
        for rule in sorted(profile.fields,key=lambda r:(not r.critical,not r.required)):
            issues=[i for i in result.document.issues if i.field_id==rule.field_id and i.severity=='error']
            if issues:questions.append({'key':rule.field_id,'label':rule.label,'ask':rule.label+'：'+issues[0].message})
        prior=json.loads(draft['analysis'] or '{}');suppressed=set(prior.get('suppressed_questions',[]))
        seen=set(json.loads(draft['rules'] or '{}').get('processed_source_ids',[]))
        for source in scoped[1:]:
            if source['id'] not in seen:
                for segment in source.get('segments',[]):
                    suppressed.update(deferred_question_keys(segment['original'],prior.get('questions',[])[:3]))
        questions=[q for q in questions if q['key'] not in suppressed]
        analysis={'assessment':'按当前模板形成待审草稿；缺口与条件保留。','questions':questions[:3],'suppressed_questions':sorted(suppressed)}
        payload={'document':to_dict(result.document),'profile':to_dict(profile),'snapshot':to_dict(snapshot),
            'request':to_dict(request),
            'document_binding':binding,
            'information_coverage':ledger_source.get('coverage',{}),'information_gaps':ledger_source.get('gaps',[]),
            'native':native,'slots':slots,'report':result.report,'project_title':project['title'],
            'catalog_revision':catalog['revision'],'source_versions':source_versions,
            'timings':{'source_read':read_elapsed,'model':provider.elapsed,'model_calls':provider.calls}}
        metadata={'engine':'am-doc-standards-r9','template_hash':profile.content_hash,'template_revision':native['revision'],
            'snapshot_hash':snapshot.content_hash,'ledger_write':False,'timings':payload['timings'],
            'document_binding':binding,'project_id':project['id'],
            'processed_source_ids':[source['id'] for source in scoped]}
        with self.s.tx() as conn:
            # The original scope/revision/TTL guard remains the last persistence gate.
            guard()
            changed=conn.execute("UPDATE logic_drafts SET state='ready',analysis=?,review=?,rules=?,content=?,content_hash=?,accepted_hash=NULL,error=NULL,expires=MIN(expires,?) "
                "WHERE id=? AND revision=? AND state NOT IN ('cancelled','paused') AND expires>?",
                (canonical(analysis),canonical(result.report),canonical(metadata),result.content,digest(result.content),evidence_expires,draft['id'],job['revision'],self.s.clock())).rowcount
            if not changed:raise BoundaryError('LATE_RESULT_REJECTED')
            conn.execute('UPDATE amdoc_context SET expires=MIN(expires,?) WHERE draft_id=?',(evidence_expires,draft['id']))
            conn.execute('INSERT OR REPLACE INTO amdoc_results VALUES(?,?,?,?)',(draft['id'],job['revision'],canonical(payload),evidence_expires))
        return result.content,Result(result.reply_text+'\n可继续补资料；采用、保存云文档和挂载分别执行。',result.model,provider.elapsed),evidence_expires

    def native_action(self,row,draft,argument):
        return self.native_action_result(row,draft,argument)['message']

    def native_action_result(self,row,draft,argument,*,idempotency_key=None):
        """Return the exact action receipt; legacy commands only render message."""
        self.authorize(row)
        if argument in ('正式交付目标','挂载目标'):
            return self.delivery_status(row,draft)
        deadline=time.monotonic()+min(120,draft['expires']-self.s.clock())
        if not self.cfg.get('output_folder'):raise BoundaryError('DOCUMENT_OUTPUT_FOLDER_NOT_CONFIGURED')
        if argument=='云文档对账':
            docs=OwnerNativeDocs(self.cfg['catalog'],row,self.g.config.owner,lambda:self.authorize(row),deadline=deadline)
            publisher=NativePublisher(self.s,docs,folder=self.cfg['output_folder'],host=self.cfg['hosts'][0])
            rows=self.s.rows('SELECT idem FROM amdoc_native_writes WHERE draft_id=? AND revision=? AND content_hash=? ORDER BY rowid DESC LIMIT 1',
                             (draft['id'],draft['revision'],draft['content_hash'])) if idempotency_key is None else self.s.rows(
                'SELECT idem FROM amdoc_native_writes WHERE idem=? AND draft_id=?',
                (idempotency_key,draft['id']))
            idem=rows[0]['idem'] if rows else idempotency_key
            receipt=publisher.reconcile(idem) if rows else None
            return {'state':'verified' if receipt else 'unknown','idempotency_key':idem,'receipt':receipt,
                    'message':receipt['url'] if receipt else '云文档结果仍待确认；未重新写入。'}
        if draft['state']!='ready' or not draft['content_hash']:raise BoundaryError('LOGIC_NO_CURRENT_DRAFT')
        result=self.s.rows('SELECT payload FROM amdoc_results WHERE draft_id=? AND revision=?',(draft['id'],draft['revision']))
        if not result:raise BoundaryError('DOCUMENT_STANDARD_RESULT_REQUIRED')
        payload=json.loads(result[0]['payload']);deadline=time.monotonic()+min(120,draft['expires']-self.s.clock())
        self.require_current_evidence(row,draft,payload=payload)
        def guard(*,live=False):
            self.guard(row,draft,{'revision':draft['revision']},deadline,live=live)
            return self.require_current_evidence(row,draft,payload=payload)
        live_guard=lambda:guard(live=True)
        docs=OwnerNativeDocs(self.cfg['catalog'],row,self.g.config.owner,guard,deadline=deadline,write_guard=live_guard)
        publisher=NativePublisher(self.s,docs,folder=self.cfg['output_folder'],host=self.cfg['hosts'][0])
        selected=None
        if argument.startswith('云端更新 '):
            requested=argument.split(maxsplit=1)[1].replace('，',',').split(',');selected=[]
            section_titles={x['section_id']:x['title'] for x in payload['profile']['sections']}
            field_sections={x['field_id']:section_titles[x['section_id']] for x in payload['profile']['fields']}
            for choice in requested:
                matches=[s['field_id'] for s in payload['slots'] if choice=='全部' or choice==s['field_id'] or choice==s['label'] or s['label'].split(' / ',1)[0]==choice or field_sections.get(s['field_id'])==choice]
                if not matches:raise BoundaryError('DOCUMENT_UPDATE_SECTION_UNKNOWN')
                selected.extend(matches)
        def revalidate(base,human,candidate,selected):
            from dataclasses import replace
            from am_doc_engine.models import document_from_dict,profile_from_dict,snapshot_from_dict,FactSnapshot,now_iso
            from am_doc_engine.revision import plan_revision
            from am_doc_engine.revalidation import revalidate_revision
            from am_doc_engine.review import EvidenceSemanticReviewer
            base=document_from_dict(base);current=replace(base,fields=tuple(
                replace(f,text=human[f.field_id],origin='human',status='filled') if f.field_id in human else f for f in base.fields),human_modified_fields=tuple(human))
            proposed=replace(document_from_dict(candidate),document_id=base.document_id,revision=base.revision+1)
            plan=plan_revision(base,current,proposed,selected,expected_current_revision=current.revision)
            profile=profile_from_dict(payload['profile']);request=DraftRequest(**payload['request'])
            snapshot=snapshot_from_dict(payload['snapshot']);kept={e.evidence_id:e for e in snapshot.evidence}
            # Unchanged fields need their original, still scoped snapshot. Never
            # discard their citations just because only progress was regenerated.
            histories=self.s.rows('SELECT payload FROM amdoc_results WHERE draft_id=? ORDER BY revision DESC',(draft['id'],))
            required={eid for f in plan.merged.fields for eid in f.evidence_ids}
            for h in histories:
                prior=json.loads(h['payload'])
                prior_evidence=snapshot_from_dict(prior['snapshot']).evidence
                if any(e.evidence_id in required and e.evidence_id not in kept for e in prior_evidence):
                    self.require_current_evidence(row,draft,payload=prior)
                for e in prior_evidence:
                    if e.evidence_id in required and e.evidence_id not in kept:kept[e.evidence_id]=e
            combined=FactSnapshot('merged-'+fingerprint(list(kept))[:24],now_iso(),tuple(kept.values()))
            access=AccessContext(row['tenant'],row['user'],(request.project_id,),app_id=row['app'],chat_id=row['chat'])
            session=ProviderSession(SharedProvider(self.g.provider,guard=live_guard,deadline=deadline),run_id=uuid.uuid4().hex,
                deadline_monotonic=deadline,timeout=120,ensure_current=guard,max_tokens=4096)
            try:checked=revalidate_revision(plan,profile,request,access,combined,reviewer=EvidenceSemanticReviewer(session),ensure_current=guard)
            except ProviderControlError as exc:raise exc.original
            return to_dict(checked)
        idem=publisher.action_key(draft,payload['document'],payload['slots'],selected)
        try:
            saved=publisher.save(dict(draft,title=payload['project_title']),payload['document'],payload['native'],payload['slots'],selected_fields=selected,revalidate=revalidate)
        except Exception:
            # A response lost after starting this exact action is unknown, even
            # when another action for this draft has an older verified receipt.
            if not self.s.rows('SELECT 1 FROM amdoc_native_writes WHERE idem=?',(idem,)):raise
            saved={'state':'unknown','receipt':None,'idempotency_key':idem}
        message=(('云文档新版本已保存并独立回读，原稿保留：' if saved.get('new_revision_copy') else '云文档已保存并独立回读：')+saved['receipt']['url'] if saved['receipt'] else '云文档结果待确认，请先执行 /doc 云文档对账。')
        return dict(saved,message=message)

    def cleanup(self):
        self.cleanup_store(self.s)

    @staticmethod
    def cleanup_store(store):
        now=store.clock()
        with store.tx() as conn:
            tables={r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            if 'amdoc_runs' in tables:
                conn.execute('DELETE FROM amdoc_fact_snapshots WHERE run_key IN (SELECT run_key FROM amdoc_run_bindings WHERE expires<=?)',(now,))
                conn.execute('DELETE FROM amdoc_runs WHERE run_key IN (SELECT run_key FROM amdoc_run_bindings WHERE expires<=?)',(now,))
                conn.execute('DELETE FROM amdoc_template_snapshots WHERE content_hash NOT IN (SELECT json_extract(result_json,\'$.template_hash\') FROM amdoc_runs WHERE result_json IS NOT NULL)')
            if 'amdoc_native_writes' in tables:
                conn.execute("UPDATE amdoc_native_writes SET payload='{}' WHERE expires<=?",(now,))
            for table in ('amdoc_context','amdoc_results','amdoc_run_bindings','amdoc_native_documents','amdoc_attempt_traces'):
                if table in tables:conn.execute('DELETE FROM '+table+' WHERE expires<=?',(now,))
