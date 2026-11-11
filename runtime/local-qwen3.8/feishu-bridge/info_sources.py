"""Owner-private, read-only user-identity sources and bounded discovery.

All commands are fixed here. Source text never supplies a shell command,
credential, output recipient, grant, or action to execute.
"""
from dataclasses import replace
from datetime import datetime, timezone
import hashlib
import json
import re
import subprocess
import tempfile
from pathlib import Path
import threading
import time

from am_info_sync.models import Source, Observation, Page, PermissionDenied, RetryableError, ScopeViolation, canonical, digest
from am_info_sync.adapters.http import InvalidResponse, remote_id, _AUTH_CODES, _PERMISSION_CODES
from am_info_sync.adapters.messages import MessageAdapter, document_discoveries
from am_info_sync.adapters.docx import DocxAdapter


class PinnedUserReader:
    def __init__(self, protected):
        self.protected=protected
        self.checked=0
        self.lock=threading.RLock()
        self.last_missing_scopes=[]

    def call(self,args):
        allowed={('im','+messages-search'),('im','+chat-search'),('im','+chat-messages-list'),
            ('im','+messages-mget'),('im','+threads-messages-list'),('drive','+search'),
            ('drive','+inspect'),('docs','+fetch'),('task','+get-related-tasks'),
            ('drive','+download'),
            ('sheets','+cells-get'),('sheets','+workbook-info'),
            ('base','+table-list'),('base','+record-list'),
            ('approval','instances','initiated'),('approval','instances','get'),
            ('approval','tasks','query'),('task','tasks','get')}
        if not any(tuple(args[:len(prefix)])==prefix for prefix in allowed):
            # Metadata and block APIs have no typed docs command in this CLI.
            if not (args[:2]==['api','GET'] and re.fullmatch(
                r'/open-apis/(?:docx/v1/documents/[A-Za-z0-9_-]+(?:/blocks)?|im/v1/messages(?:/[A-Za-z0-9_-]+)?)',args[2])):
                raise ScopeViolation('user_cli_operation_not_read_allowlisted')
        with self.lock:
            if time.time()-self.checked>60:
                who=self.protected.call(['whoami']);expected=self.protected.identity
                if (any(who.get(k)!=expected.get(k) for k in ('profile','appId','identity')) or
                    who.get('onBehalfOf',{}).get('openId')!=expected.get('onBehalfOf',{}).get('openId')):
                    raise PermissionDenied('user_cli_identity_changed')
                self.checked=time.time()
            binary=self.protected.binary
            if hashlib.sha256(binary.read_bytes()).hexdigest()!=self.protected.sha256:
                raise PermissionDenied('user_cli_binary_changed')
            try:
                response=subprocess.run([str(binary),*args,'--as','user','--profile',self.protected.identity['profile']],
                    capture_output=True,timeout=40)
            except (OSError,subprocess.TimeoutExpired):raise RetryableError('user_cli_timeout') from None
        raw=response.stdout if response.stdout else response.stderr
        if len(raw)>16*1024*1024:raise InvalidResponse('user_cli_response_limit')
        try:value=json.loads(raw)
        except (ValueError,TypeError):raise InvalidResponse('user_cli_invalid_response') from None
        if response.returncode or value.get('ok') is False or value.get('code',0)!=0:
            error=value.get('error') or {}
            if (error.get('type')=='authorization' or error.get('code') in _AUTH_CODES|_PERMISSION_CODES
                    or error.get('subtype') in ('missing_scope','permission_denied','access_denied','forbidden','token_expired')):
                self.last_missing_scopes=error.get('missing_scopes',[])
                raise PermissionDenied('user_cli_permission_missing')
            raise RetryableError('user_cli_read_failed')
        return value.get('data',value)

    def get(self,source,path,params=None):
        if not source.enabled or source.settings.get('read_identity')!='existing_user':
            raise ScopeViolation('user_source_not_enabled')
        rid=remote_id(source.remote_id)
        if source.kind=='docx' and path not in (f'/open-apis/docx/v1/documents/{rid}',f'/open-apis/docx/v1/documents/{rid}/blocks'):
            raise ScopeViolation('document_path_mismatch')
        if source.kind in ('im_chat','im_thread') and not re.fullmatch(r'/open-apis/im/v1/messages(?:/[A-Za-z0-9_-]+)?',path):
            raise ScopeViolation('message_path_mismatch')
        if source.kind not in ('im_chat','im_thread','docx'):
            raise ScopeViolation('user_source_kind_invalid')
        return self.call(['api','GET',path,'--params',canonical(params or {})])


class RoutedAdapter:
    def __init__(self,bot,user):self.bot,self.user=bot,user
    def fetch_page(self,source,*args,**kwargs):
        return (self.user if source.settings.get('read_identity')=='existing_user' else self.bot).fetch_page(source,*args,**kwargs)
    def fetch_message(self,source,*args,**kwargs):
        return (self.user if source.settings.get('read_identity')=='existing_user' else self.bot).fetch_message(source,*args,**kwargs)


def record_observation(source,identity,record,now,field='system_record',url=None):
    text=canonical(record)
    return Observation(source.source_id,str(identity),digest(record),now,text=text,
        payload={'record':record,'quote':text[:200],'time_basis':'observed_at'},url=url or source.settings.get('url'),
        completeness='complete' if len(text.encode('utf-8'))<=16000 else 'partial')


class TasksCollection:
    def __init__(self,user,store=None):self.user,self.store=user,store
    def fetch_page(self,source,start_ms,end_ms,cursor=None):
        args=['task','+get-related-tasks']
        if cursor:args+=['--page-token',cursor]
        data=self.user.call(args);items=data.get('items')
        if not isinstance(items,list) or type(data.get('has_more')) is not bool:raise InvalidResponse('task_collection_shape')
        observations=[]
        for item in items:
            if not item.get('guid'):raise InvalidResponse('task_guid_missing')
            record={k:item.get(k) for k in ('guid','summary','description','status','completed_at','created_at','due','subtask_count')}
            record['scope']='related_task_fields; comments and attachment bodies not read'
            observations.append(record_observation(source,item['guid'],record,end_ms,url=item.get('url')))
        token=data.get('page_token') if data['has_more'] else None
        if data['has_more'] and (not token or token==cursor):raise InvalidResponse('task_cursor_not_advanced')
        if self.store is not None:
            with self.store.transaction() as db:
                db.execute('CREATE TABLE IF NOT EXISTS host_seen_tasks(source_id TEXT,window INTEGER,object_id TEXT,PRIMARY KEY(source_id,window,object_id))')
                for obs in observations:db.execute('INSERT OR IGNORE INTO host_seen_tasks VALUES(?,?,?)',(source.source_id,end_ms,obs.object_id))
                if token is None:
                    prior=db.execute("SELECT object_id FROM objects WHERE source_id=? AND state='active' AND object_id NOT IN (SELECT object_id FROM host_seen_tasks WHERE source_id=? AND window=?)",(source.source_id,source.source_id,end_ms)).fetchall()
                    for row in prior:observations.append(Observation(source.source_id,row[0],'membership-removed:'+str(end_ms),end_ms,deleted=True,payload={'scope_membership_removed':True}))
                    db.execute('DELETE FROM host_seen_tasks WHERE source_id=? AND window<?',(source.source_id,end_ms))
        return Page(tuple(observations),token,time_coverage=False)


class ApprovalInstance:
    def __init__(self,user):self.user=user
    def fetch_page(self,source,start_ms,end_ms,cursor=None):
        if cursor:raise ScopeViolation('approval_snapshot_has_no_cursor')
        data=self.user.call(['approval','instances','get','--instance-code',source.remote_id])
        identity=data.get('instance_code',source.remote_id)
        if identity!=source.remote_id:raise ScopeViolation('approval_identity_changed')
        if 'status' not in data and 'instance_status' not in data:raise InvalidResponse('approval_status_missing')
        record={k:data.get(k) for k in ('approval_name','approval_code','instance_code','status','instance_status','start_time','end_time','serial_number') if k in data}
        # Overall instance state is separate from each approver's task state.
        record['tasks']=[{k:t.get(k) for k in ('id','status','start_time','end_time','type')} for t in data.get('task_list',[])]
        payload={'record':record,'quote':canonical(record),'form':data.get('form'),
                 'timeline':data.get('timeline',[]),'time_basis':'observed_at','attachment_bodies':'pending'}
        text=canonical(record)+'\n'+str(data.get('form',''))
        return Page((Observation(source.source_id,source.remote_id,digest(data),end_ms,text=text,payload=payload),),time_coverage=False)


class CloudFile:
    def __init__(self,user):self.user=user
    def fetch_page(self,source,start_ms,end_ms,cursor=None):
        if source.kind!='file_snapshot' or cursor is not None:raise ScopeViolation('file_source_invalid')
        from fs_agent.doc_logic import read_source
        from fs_agent.attachment_parser import MAX_FILE_BYTES
        with tempfile.TemporaryDirectory(prefix='am-info-file-',dir='/tmp') as directory:
            path=Path(directory)/'source'
            self.user.call(['drive','+download','--file-token',remote_id(source.remote_id),'--output',str(path)])
            if not path.is_file() or path.stat().st_size>MAX_FILE_BYTES:raise InvalidResponse('file_size_limit')
            data=path.read_bytes()
            parsed=read_source(source.settings.get('file_name','source'),data,source_id=source.source_id,
                               origin=source.remote_id,provenance='current_project')
        text='\n'.join(s['original'] for s in parsed.get('segments',[]))
        status={'complete':'complete','partial':'partial','failed':'unreadable'}.get(parsed['read_status'],'unreadable')
        payload={'file_name':parsed['name'],'parser_status':parsed['read_status'],
                 'limitation':parsed.get('limitation'),'segments':parsed.get('segments',[]),'time_basis':'observed_at'}
        return Page((Observation(source.source_id,source.remote_id,hashlib.sha256(data).hexdigest(),end_ms,
                     text=text,payload=payload,completeness=status,url=source.settings.get('url')),),time_coverage=False)


class MeegleProject:
    """Read real work items and real node members; never enumerate templates."""
    def __init__(self,business,owner_origin,register_child,child_allowed=lambda source:True,node_members=lambda *args:None):
        self.business,self.owner_origin,self.register_child=business,owner_origin,register_child
        self.child_allowed,self.node_members=child_allowed,node_members
    def _authorize(self):
        row=self.owner_origin()
        if row is None or not self.business.g.authorized(row):raise PermissionDenied('meegle_owner_authorization_failed')
        identity=self.business._run(['user','me'])
        if identity.get('user_key')!=self.business.settings['user_key']:raise PermissionDenied('meegle_user_changed')
    def fetch_page(self,source,start_ms,end_ms,cursor=None):
        self._authorize();s=source.settings
        if s.get('parent_id') and not self.child_allowed(source):raise PermissionDenied('child_not_in_current_parent')
        data=self.business._run(['workitem','get','--project-key',s['space'],'--work-item-id',source.remote_id,
                                 '--fields',','.join(s.get('field_keys',[]))])
        attr=data.get('work_item_attribute',{});fields=self.business._fields(data)
        if (str(attr.get('work_item_id'))!=source.remote_id or attr.get('owned_project',{}).get('key') not in (s['space'],s.get('space_key',s['space']))
                or attr.get('work_item_type',{}).get('key')!=s['type_key']):
            raise ScopeViolation('meegle_work_item_identity_changed')
        code=s.get('business_code')
        if code and str(fields.get(s['code_field']))!=code:raise ScopeViolation('meegle_business_code_changed')
        record={'id':source.remote_id,'title':attr.get('work_item_name'),'status':attr.get('work_item_status'),
                'updated_at':attr.get('update_time'),'fields':fields,'scope':'actual_instance_fields'}
        observations=[record_observation(source,source.remote_id,record,end_ms)]
        next_cursor=None
        if s.get('include_nodes'):
            page=int(cursor or '1')
            nodes=self.business._run(['workflow','get-node','--project-key',s['space'],
                '--work-item-id',source.remote_id,'--need-sub-task','--page-num',str(page)])
            if not isinstance(nodes.get('list'),list):raise InvalidResponse('node_list_missing')
            for node in nodes['list']:
                key=node.get('basic',{}).get('node_key')
                if not key:raise InvalidResponse('node_key_missing')
                nrecord={'project_id':source.remote_id,'node_id':key,'basic':node['basic'],
                    'schedule':node.get('schedule'),'actual_subtasks':node.get('sub_tasks',[]),
                    'actual_children':node.get('sub_workitems',[]),'form_items':node.get('form_items',[])}
                observations.append(record_observation(source,source.remote_id+':node:'+key,nrecord,end_ms))
                members=[]
                for group in node.get('sub_workitems',[]):
                    for child in group.get('workitems',[]):
                        ident=str(child.get('id',child.get('work_item_id',''))) if isinstance(child,dict) else str(child)
                        if not ident.isdigit():raise InvalidResponse('node_child_identity_invalid')
                        members.append(ident)
                        self.register_child(source,ident,key)
                self.node_members(source.remote_id,key,members)
            if nodes.get('pagination',{}).get('has_more'):next_cursor=str(page+1)
        return Page(tuple(observations),next_cursor,time_coverage=False)


class Discovery:
    def __init__(self,host,user):
        self.h,self.user=host,user
        self.last_approvals=0;self.last_projects=0;self.last_search=0;self.search_position=0
        self.last_documents=0
        self.project_index=None
        self.limitations={}
        with host.store.transaction() as db:
            db.execute('CREATE TABLE IF NOT EXISTS host_discovery_state(key TEXT PRIMARY KEY,value TEXT NOT NULL)')
        self.configure()

    def state(self,key,value=None):
        with self.h.store.transaction() as db:
            if value is not None:db.execute('INSERT OR REPLACE INTO host_discovery_state VALUES(?,?)',(key,canonical(value)))
            row=db.execute('SELECT value FROM host_discovery_state WHERE key=?',(key,)).fetchone()
        return json.loads(row[0]) if row else None

    def source(self,sid,kind,rid,subjects=(),**settings):
        h=self.h
        if kind in ('docx','file_snapshot'):
            try:
                old=h.store.get_source(sid)
                subjects=sorted(v for v in set(subjects)|set(old.settings.get('subject_ids') or [old.settings.get('subject_id')]) if v)
                if kind=='docx' and old.kind==kind and old.remote_id==rid:
                    # Multiple discovery paths supply different display labels
                    # and anchored links for the same immutable read identity.
                    # Keep the registered citation (including absence) stable.
                    # Actual document titles/content remain revision-checked by
                    # DocxAdapter; file_snapshot names still select its parser.
                    for key in ('file_name','url'):
                        if key in old.settings:settings[key]=old.settings[key]
                        else:settings.pop(key,None)
            except ScopeViolation:pass
        if kind=='im_chat':
            existing=[s for s in h.store.list_sources() if s.enabled and s.kind=='im_chat' and s.remote_id==rid]
            if existing:
                existing.sort(key=lambda s:(not s.source_id.startswith('owner-am:'),s.source_id))
                current=existing[0]
                all_subjects=sorted(set(current.settings.get('subject_ids',[]))|set(subjects))
                new_settings=dict(current.settings,subject_ids=all_subjects)
                if len(all_subjects)>1:new_settings.pop('subject_id',None)
                h._register(replace(current,settings=new_settings))
                for duplicate in existing[1:]:h.store.register_source(replace(duplicate,enabled=False))
                return h.store.get_source(current.source_id)
        mapping={'system_record':{'value_path':['record'],'quote_path':['quote']}}
        if len(subjects)==1:mapping['system_record']['subject_id']=subjects[0]
        elif kind in ('tasks_collection','approval_snapshot'):mapping['system_record']['subject_id']=sid
        source=Source(sid,kind,rid,h.am.g.config.tenant_key,h.am.owner,enabled=True,
            start_ms=h.start_ms,poll_seconds=300 if kind not in ('im_chat','im_thread') else 180,
            settings={'managed_by':'owner_work_discovery','read_identity':'existing_user',
                **({'subject_id':subjects[0]} if len(subjects)==1 else {'subject_ids':list(subjects)}),
                **({'fact_mappings':mapping} if kind in ('tasks_collection','approval_snapshot','workitem_snapshot','bitable_snapshot','sheet_snapshot') else {}),**settings})
        if kind in ('tasks_collection','approval_snapshot') and not subjects:
            source=replace(source,settings=dict(source.settings,subject_id=sid,subject_ids=[sid]))
        self.h._register(source)
        return self.h.store.get_source(sid)

    def configure(self):
        h=self.h;a=h.service.sync_engine.adapters
        for kind in ('im_chat','im_thread'):
            a[kind]=RoutedAdapter(a[kind],MessageAdapter(self.user))
        a['docx']=RoutedAdapter(a['docx'],DocxAdapter(self.user))
        a['tasks_collection']=TasksCollection(self.user,h.store)
        a['approval_snapshot']=ApprovalInstance(self.user)
        a['file_snapshot']=CloudFile(self.user)
        from info_cloud import BaseTable, SheetBook
        a['bitable_snapshot']=BaseTable(self.user,h.store)
        a['sheet_snapshot']=SheetBook(self.user,h.store)
        a['workitem_snapshot']=MeegleProject(h.business,h.am.owner_origin,self.child,self.child_allowed,self.node_members)
        h.service.message_reviewer.adapters=a
        self.source('my-related-tasks','tasks_collection','related-to-owner')
        h.catalog.register('my-related-tasks','本人相关飞书任务',kind='task')

    def child(self,parent,identity,node):
        previous=self.state('child:'+identity)
        if previous and time.time()-previous.get('at',0)<3600 and (
                previous.get('excluded') or (previous.get('parent')==parent.remote_id and previous.get('node')==node)):
            return
        key='child_pending:'+identity
        value={'parent_source':parent.source_id,'node':node,'identity':identity,'requested_at':time.time()}
        with self.h.store.transaction() as db:
            db.execute('INSERT OR IGNORE INTO host_discovery_state VALUES(?,?)',(key,canonical(value)))

    def node_members(self,parent,node,members):
        self.state('members:'+parent+':'+node,{'at':time.time(),'members':members})

    def child_allowed(self,source):
        data=self.state('members:'+source.settings['parent_id']+':'+source.settings['node_id']) or {}
        return time.time()-data.get('at',0)<1800 and source.remote_id in data.get('members',[])

    def resolve_child(self):
        with self.h.store._lock:
            row=self.h.store.conn.execute("SELECT key,value FROM host_discovery_state WHERE key LIKE 'child_pending:%' AND COALESCE(json_extract(value,'$.next_retry_at'),0)<=? ORDER BY COALESCE(json_extract(value,'$.requested_at'),0),key LIMIT 1",(time.time(),)).fetchone()
        if row is None:return
        value=json.loads(row['value'])
        try:
            self._resolve_child(row,value)
            self.h.store.resolve_gap(value['parent_source'],value['identity'],'discovery_child')
        except Exception as exc:
            # One failure consumes one bounded slot; its durable retry deadline
            # allows healthy later items to proceed in this same pass.
            attempts=int(value.get('attempts',0))+1
            code=getattr(exc,'code',type(exc).__name__)
            delay=max(60,min(3600,30*2**min(attempts,7)))
            value.update(attempts=attempts,last_attempt_at=time.time(),last_error=code,
                         next_retry_at=time.time()+delay)
            self.state(row['key'],value)
            self.h.store.open_gap(value['parent_source'],value['identity'],'discovery_child',code)
            self.h.service.pulse('discovery_children','error',code)

    def _resolve_child(self,row,value):
        parent=self.h.store.get_source(value['parent_source']);identity=value['identity'];node=value['node']
        if not parent.enabled:raise PermissionDenied('discovery_parent_disabled')
        # The parent read established this exact instance relationship. Type and
        # field mapping are verified from the child detail before registration.
        key='child:'+identity
        previous=self.state(key)
        if previous and time.time()-previous.get('at',0)<3600:
            with self.h.store.transaction() as db:db.execute('DELETE FROM host_discovery_state WHERE key=?',(row['key'],))
            return
        data=self.h.business._run(['workitem','get','--project-key',parent.settings['space'],'--work-item-id',identity])
        attr=data.get('work_item_attribute',{});typ=attr.get('work_item_type',{}).get('key')
        if str(attr.get('work_item_id'))!=identity:raise ScopeViolation('discovered_child_identity_changed')
        if not typ:raise InvalidResponse('discovered_child_mapping_missing')
        if typ=='6aca08500baf5c1b98ad7b64' or '资源库' in attr.get('work_item_type',{}).get('name',''):
            with self.h.store.transaction() as db:db.execute('DELETE FROM host_discovery_state WHERE key=?',(row['key'],))
            self.state(key,{'at':time.time(),'excluded':'template_resource_library'})
            return
        # Resource libraries are intentionally excluded; actual node members
        # of other types remain pending until their mapping is known.
        metadata=self.state('metadata:'+str(typ))
        if metadata is None or metadata.get('pagination',{}).get('has_more'):
            metadata=metadata_all(self.h.business,parent.settings['space'],typ)
            self.state('metadata:'+typ,metadata)
        fields=field_definitions(metadata)
        if not fields:raise InvalidResponse('discovered_child_mapping_missing')
        keys=[f['field_key'] for f in fields if f.get('field_type') not in ('_name',)]
        self.source('workitem:'+identity,'workitem_snapshot',identity,
            [parent.settings['subject_id']],space=parent.settings['space'],space_key=parent.settings.get('space_key'),
            type_key=typ,field_keys=keys,include_nodes=False,parent_id=parent.remote_id,node_id=node)
        self.state(key,{'at':time.time(),'parent':parent.remote_id,'node':node})
        with self.h.store.transaction() as db:db.execute('DELETE FROM host_discovery_state WHERE key=?',(row['key'],))

    def approvals(self):
        queries=[['approval','instances','initiated']]+[['approval','tasks','query','--topic',t] for t in ('1','2','17','18')]
        discovered=set()
        for args in queries:
            cursor=None;seen=set()
            for _ in range(20):
                options=args+['--page-size','100']
                if '--topic' not in args or args[-1] not in ('1','17'):
                    options+=['--start-timestamp',str(int(time.time()-30*86400)),
                              '--end-timestamp',str(int(time.time()))]
                if cursor:options+=['--page-token',cursor]
                data=self.user.call(options)
                rows=data.get('instances',data.get('tasks'))
                if not isinstance(rows,list):raise InvalidResponse('approval_list_missing')
                for row in rows:
                    identity=row.get('instance_code')
                    if not identity:raise InvalidResponse('approval_id_missing')
                    discovered.add(identity)
                    self.source('approval:'+identity,'approval_snapshot',identity)
                if not data.get('has_more'):break
                token=data.get('page_token')
                if not token or token in seen:raise InvalidResponse('approval_cursor_not_advanced')
                seen.add(token);cursor=token
            else:raise InvalidResponse('approval_page_budget')
        self.state('approvals',{'at':time.time(),'instances':len(discovered),'scope':'pending and unread; other lists last 30 days'})

    def projects(self):
        h=self.h
        if h.ledger is None:return
        identity=h.business._run(['user','me'])
        if identity.get('user_key')!=h.business.settings['user_key']:raise PermissionDenied('meegle_user_changed')
        rows=h.ledger.read()
        indexes={}
        for typename in ('项目','需求管理'):
            cols='`工作项id`, `名称`, `项目编号`, `状态`' if typename=='项目' else '`工作项id`, `名称`, `需求编号`, `状态`'
            codes=sorted({m.group() for r in rows for m in re.finditer(r'(?:QI|RA|IRAD|PT|CERT)-\d{4}-\d{3}',r['title'])})
            numbers=sorted({int(m.group(1)) for r in rows for m in re.finditer(r'需求\s*(\d+)',r['title'])})
            where=('`项目编号` in ('+','.join("'"+c+"'" for c in codes)+')') if typename=='项目' else ('`需求编号` in ('+','.join("'"+str(n)+"'" for n in numbers)+')')
            data=h.business._run(['workitem','query','--project-key','dpgjfw',
                '--mql',f'SELECT {cols} FROM `dpgjfw`.`{typename}` WHERE {where}'])
            indexes[typename]=mql_records(data)
            if sum(x.get('count',0) for x in data.get('list') or [])>len(indexes[typename]):
                raise InvalidResponse('workitem_index_requires_more_pages')
        matched=[];pending=[]
        for row in rows:
            code=re.search(r'(?:QI|RA|IRAD|PT|CERT)-\d{4}-\d{3}',row['title'])
            req=re.search(r'需求\s*(\d+)',row['title'])
            kind='项目' if code else '需求管理' if req else None
            candidates=[]
            if code:candidates=[v for v in indexes[kind] if v.get('项目编号')==code.group()]
            elif req:candidates=[v for v in indexes[kind] if str(v.get('需求编号'))==req.group(1)]
            if len(candidates)!=1:
                pending.append({'subject_id':row['key'],'reason':'no_unique_live_mapping'});continue
            item=candidates[0];identity=str(item['工作项id'])
            detail=h.business._run(['workitem','get','--project-key','dpgjfw','--work-item-id',identity])
            attr=detail['work_item_attribute'];typ=attr['work_item_type']['key'];space=attr['owned_project']['key']
            metadata=self.state('metadata:'+typ)
            if metadata is None or metadata.get('pagination',{}).get('has_more'):
                metadata=metadata_all(h.business,'dpgjfw',typ);self.state('metadata:'+typ,metadata)
            keys=[f['field_key'] for f in field_definitions(metadata) if f.get('field_type') not in ('_name',)]
            extra={'business_code':code.group(),'code_field':'field_09e17e'} if code else {'business_code':req.group(1),'code_field':'field_eca918'}
            self.source('workitem:'+identity,'workitem_snapshot',identity,[row['key']],space='dpgjfw',space_key=space,
                type_key=typ,field_keys=keys,include_nodes=True,**extra)
            matched.append(row['key'])
        self.state('project_mapping',{'at':time.time(),'matched':matched,'pending':pending})

    def search_one(self):
        h=self.h
        if h.ledger is None:return
        rows=h.ledger.read()
        if not rows:return
        self.search_position=self.state('search_position') or self.search_position
        item=rows[self.search_position%len(rows)];self.search_position+=1
        self.state('search_position',self.search_position)
        match=re.search(r'(?:QI|RA|IRAD|PT|CERT)-\d{4}-\d{3}|需求\s*\d+',item['title'])
        term=match.group() if match else item['title'].split('｜')[0]
        args=['im','+messages-search','--query',term,'--sender-type','user','--no-reactions','--page-size','50']
        cursor=None;seen=set();chats=set()
        for _ in range(20):
            data=self.user.call(args+(['--page-token',cursor] if cursor else []))
            for message in data.get('messages',[]):
                if message.get('chat_type') not in ('p2p','group'):continue
                chat=message.get('chat_id')
                if not chat:continue
                chats.add(chat);sid='user-chat:'+digest(chat)[:16]
                old=[]
                try:old=h.store.get_source(sid).settings.get('subject_ids',[])
                except ScopeViolation:pass
                subjects=sorted(set(old+[item['key']]))
                self.source(sid,'im_chat',chat,subjects,subject_ids=subjects,
                    review_known_messages=True,only_user_messages=True)
            if not data.get('has_more'):break
            token=data.get('page_token')
            if not token or token in seen:raise InvalidResponse('search_cursor_invalid')
            seen.add(token);cursor=token
        else:raise InvalidResponse('message_discovery_page_budget')
        self.state('chat-search:'+item['key'],{'at':time.time(),'chats':len(chats)})
        # A failed page is retried from this project's beginning; discoveries
        # already registered are idempotent. Never claim a truncated full scan.
        try:
            cursor=None;seen=set();found=0
            for _ in range(50):
                options=['drive','+search','--query',term[:30],'--page-size','20']
                if cursor:options+=['--page-token',cursor]
                data=self.user.call(options)
                if not isinstance(data.get('results'),list) or type(data.get('has_more')) is not bool:raise InvalidResponse('document_search_schema')
                for result in data['results']:
                    meta=result.get('result_meta') or {};url=meta.get('url')
                    title=re.sub(r'<[^>]+>','',result.get('title_highlighted',''))
                    if '模板' in title or '模版' in title:continue
                    if url:self.register_document(url,[item['key']],title=title);found+=1
                if not data.get('has_more'):break
                token=data.get('page_token')
                if not token or token in seen:raise InvalidResponse('document_search_cursor')
                seen.add(token);cursor=token
            else:raise InvalidResponse('document_search_page_budget')
            self.state('document-search:'+item['key'],{'at':time.time(),'discovered':found,'complete':True})
            self.limitations.pop('document_search',None);self.state('limitations',self.limitations)
        except PermissionDenied:
            self.limitations['document_search']=list(self.user.last_missing_scopes)
            self.state('limitations',self.limitations)

    def register_document(self,url,subjects,title=''):
        if not re.match(r'https://[A-Za-z0-9-]+\.feishu\.cn/(docx|wiki|sheets|base|file)/',url):
            return {'status':'unsupported_url','document_type':None,'source_ids':[]}
        key='document_url:'+digest(url)
        previous=self.state(key)
        if previous and time.time()-previous.get('at',0)<600:
            # A later project match still expands only this owner's subject map.
            if previous.get('kind') in ('docx','file'):
                kind=previous['kind'];source_kind='docx' if kind=='docx' else 'file_snapshot'
                self.source(kind+':'+previous['token'],source_kind,previous['token'],subjects,
                    file_name=title or previous.get('title','在线文档'),url=url)
                ids=[kind+':'+previous['token']]
            else:
                ids=previous.get('source_ids') or [s.source_id for s in self.h.store.list_sources()
                    if s.enabled and s.remote_id==previous['token'] and s.kind in ('sheet_snapshot','bitable_snapshot')]
            return {'status':previous.get('status','existing'),'document_type':previous['kind'],
                    'source_ids':ids}
        inspected=self.user.call(['drive','+inspect','--url',url])
        token=inspected.get('token');kind=inspected.get('type');name=inspected.get('title') or title or '云文档'
        if not token:raise InvalidResponse('document_token_missing')
        source_ids=[];status='registered'
        if kind=='docx':
            source_ids.append('docx:'+token)
            self.source('docx:'+token,'docx',token,subjects,file_name=name,url=url)
        elif kind=='file':
            source_ids.append('file:'+token)
            self.source('file:'+token,'file_snapshot',token,subjects,file_name=name,url=url)
        elif kind in ('sheet','bitable'):
            # Whole tables may contain multiple projects. Keep their records
            # under a document subject instead of assigning every row to one hit.
            subject='document:'+token;self.h.catalog.register(subject,name,kind='document')
            if kind=='sheet':
                source_ids.append('sheet:'+token)
                self.source('sheet:'+token,'sheet_snapshot',token,[subject],file_name=name,url=url)
            else:
                tables=self.user.call(['base','+table-list','--base-token',token]).get('tables')
                if not isinstance(tables,list):raise InvalidResponse('base_table_list_missing')
                for table in tables:
                    if any(word in table.get('name','') for word in ('虚构','程序维护','资源库','模板','模版')):continue
                    source_ids.append('base:'+token+':'+table['id'])
                    self.source('base:'+token+':'+table['id'],'bitable_snapshot',token,[subject],
                        table_id=table['id'],file_name=name+'／'+table['name'],url=url)
        else:
            status='unsupported_type'
            self.state('unsupported_document:'+digest(url),{'kind':kind,'url':url,'title':name,'status':'reader_pending'})
        self.state(key,{'at':time.time(),'kind':kind,'token':token,'title':name,'status':status,'source_ids':source_ids})
        return {'status':status,'document_type':kind,'source_ids':source_ids}

    def documents_from_ledger(self):
        h=self.h
        from group_ledger import TOKEN,SHEET
        data=self.user.call(['sheets','+cells-get','--spreadsheet-token',TOKEN,
            '--sheet-id',SHEET,'--range','A1:G100','--include','value,style,comment'])
        if data.get('has_more'):raise InvalidResponse('ledger_links_partial')
        urls=set()
        def walk(value):
            if isinstance(value,str):
                urls.update(re.findall(r'https://[^\s<>"\\]+',value))
            elif isinstance(value,dict):
                for v in value.values():walk(v)
            elif isinstance(value,list):
                for v in value:walk(v)
        walk(data)
        h.catalog.register('ledger-references','台账关联文档',kind='document')
        completed=0;pending=[];counts={};examined=0
        for url in sorted(urls):
            if not re.match(r'https://[A-Za-z0-9-]+\.feishu\.cn/(docx|wiki|sheets)/',url):continue
            result=self.register_document(url,['ledger-references']);examined+=1
            if result['status'] in ('registered','existing') and result['source_ids']:
                completed+=1;kind=result['document_type'];counts[kind]=counts.get(kind,0)+1
            else:pending.append({'kind':result['document_type'],'reference_sha256':digest(url),'status':result['status']})
        self.state('ledger_document_links',{'at':time.time(),'links_examined':examined,'registered_documents':completed,
            'registered_documents_by_type':counts,'docx_registered':counts.get('docx',0),'pending_other_types':pending})

    def linked_documents(self):
        h=self.h
        with h.store._lock:
            rows=h.store.conn.execute("SELECT * FROM discoveries WHERE state='pending' AND kind='docx' LIMIT 5").fetchall()
        for row in rows:
            parent=h.store.get_source(row['parent_source_id']) if 'parent_source_id' in row.keys() else None
            if parent is None:continue
            subjects=parent.settings.get('subject_ids') or [parent.settings.get('subject_id')]
            self.source('docx:'+row['remote_id'],'docx',row['remote_id'],[s for s in subjects if s])
            with h.store.transaction() as db:db.execute("UPDATE discoveries SET state='registered' WHERE discovery_id=?",(row['discovery_id'],))

    def step(self):
        now=time.time()
        for name,seconds,operation in [('approvals',300,self.approvals),('projects',3600,self.projects),
                                       ('documents',3600,self.documents_from_ledger)]:
            last=getattr(self,'last_'+name)
            if now-last>=seconds:
                setattr(self,'last_'+name,now)
                try:operation();self.h.service.pulse('discovery_'+name,'idle')
                except Exception as exc:
                    setattr(self,'last_'+name,now-seconds+60)
                    self.h.service.pulse('discovery_'+name,'error',getattr(exc,'code',type(exc).__name__))
        if now-self.last_search>=10:
            self.last_search=now
            try:self.search_one();self.linked_documents();self.h.service.pulse('discovery_chats','idle')
            except Exception as exc:self.h.service.pulse('discovery_chats','error',getattr(exc,'code',type(exc).__name__))
        for _ in range(4):self.resolve_child()


def field_definitions(value):
    result=[]
    if isinstance(value,dict):
        if 'field_key' in value and 'field_name' in value:return [value]
        for v in value.values():result.extend(field_definitions(v))
    elif isinstance(value,list):
        for v in value:result.extend(field_definitions(v))
    return result


def mql_records(data):
    result=[]
    for rows in (data.get('data') or {}).values():
        for row in rows:
            result.append({f['name']:f.get('value',{}).get(f.get('value_type')) for f in row.get('moql_field_list',[])})
    return result


def metadata_all(business,space,typ):
    rows=[]
    for page in range(1,21):
        data=business._run(['workitem','meta-fields','--project-key',space,'--work-item-type',typ,'--page-num',str(page)])
        if not isinstance(data.get('list'),list):raise InvalidResponse('field_metadata_missing')
        rows.extend(data['list'])
        if not data.get('pagination',{}).get('has_more'):return {'list':rows,'pagination':{'has_more':False}}
    raise InvalidResponse('field_metadata_page_budget')
