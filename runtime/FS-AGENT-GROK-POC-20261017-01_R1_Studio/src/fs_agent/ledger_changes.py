"""Explicit source collection, field approvals and per-cell write journals."""
import json
import re
import uuid
from datetime import datetime,timedelta
from zoneinfo import ZoneInfo
from .config import BoundaryError
from .project_catalog import canonical,digest

ALLOWED_FIELDS={'待办','项目进展'}


def split_facts(text,anchor):
    facts=[]
    for match in re.finditer(r'[^，,；;。\n]+[，,；;。]?',text):
        original=match[0].strip('，,；;。\n ')
        if not original:continue
        status='not_completed' if re.search('尚未|未完成|没做|未通过|未做',original) else 'planned' if re.search('预计|计划|暂定|明天|后天|将|拟',original) else 'reported_completed' if re.search('完成|已通过|已到货',original) else 'reported'
        facts.append({'original':original,'offset':match.start(),'length':len(match[0]),'status':status,'evidence_status':'self_reported','time_anchor':anchor,'timezone':'Asia/Shanghai'})
    return facts


def merge_progress(before,facts,checked_at):
    lines=before.splitlines()
    if lines and re.fullmatch(r'\d{4}[.年/-]\d{1,2}[.月/-]\d{1,2}日?',lines[0].strip()):lines=lines[1:]
    sections={'进度':[],'问题':[],'下一步':[],'要求':[]};current='进度'
    for line in lines:
        match=re.match(r'^(进度|问题|下一步|要求)：(.*)',line)
        if match:current=match[1];line=match[2]
        if line.strip():sections[current].append(line.strip())
    for f in facts:
        section='下一步' if f['status']=='planned' else '问题' if f['status']=='not_completed' else '进度'
        display=f['original']
        anchor=datetime.fromtimestamp(f['time_anchor'],ZoneInfo('Asia/Shanghai'))
        for word,days in (('今天',0),('明天',1),('后天',2)):
            d=anchor+timedelta(days=days);display=display.replace(word,f'{d.year}.{d.month}.{d.day}')
        line='报告人自报（待核验）：'+display
        if line not in sections[section]:sections[section].append(line)
    dt=datetime.fromtimestamp(checked_at,ZoneInfo('Asia/Shanghai'))
    result=[f'{dt.year}.{dt.month}.{dt.day}']
    for label,values in sections.items():
        for line in values:result.append(label+'：'+line)
    return '\n'.join(result)


class LedgerChanges:
    def __init__(self,store,owner,port,*,enabled=lambda:False):
        self.s,self.owner,self.port,self.enabled=store,owner,port,enabled
        with store.lock:store.conn.executescript('''
        CREATE TABLE IF NOT EXISTS progress_collections(
          id TEXT PRIMARY KEY,message_key TEXT UNIQUE NOT NULL,reporter TEXT NOT NULL,project_ref TEXT NOT NULL,
          version INTEGER NOT NULL,source TEXT NOT NULL,source_hash TEXT NOT NULL,facts TEXT NOT NULL,
          reporter_confirmed INTEGER NOT NULL,state TEXT NOT NULL,created REAL NOT NULL);
        CREATE TABLE IF NOT EXISTS progress_corrections(collection_id TEXT NOT NULL,version INTEGER NOT NULL,actor TEXT NOT NULL,prior_facts TEXT NOT NULL,correction TEXT,source TEXT,created REAL NOT NULL,PRIMARY KEY(collection_id,version));
        CREATE TABLE IF NOT EXISTS ledger_proposals(
          id TEXT PRIMARY KEY,collection_id TEXT NOT NULL,collection_version INTEGER NOT NULL,version INTEGER NOT NULL,
          project_ref TEXT NOT NULL,basis TEXT NOT NULL,patches TEXT NOT NULL,state TEXT NOT NULL,
          approval TEXT,approval_hash TEXT,expires REAL NOT NULL,request_key TEXT UNIQUE NOT NULL,created REAL NOT NULL);
        CREATE TABLE IF NOT EXISTS ledger_approval_snapshots(id TEXT PRIMARY KEY,proposal_id TEXT NOT NULL,version INTEGER NOT NULL,snapshot TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS ledger_write_attempts(
          id TEXT PRIMARY KEY,proposal_id TEXT NOT NULL,proposal_version INTEGER NOT NULL,field TEXT NOT NULL,
          request_key TEXT UNIQUE NOT NULL,before_value TEXT NOT NULL,after_value TEXT NOT NULL,state TEXT NOT NULL,
          receipt TEXT,error TEXT,readback TEXT,created REAL NOT NULL,UNIQUE(proposal_id,proposal_version,field));
        INSERT OR IGNORE INTO business_migrations VALUES('D-ledger-1');
        ''')
        self.s.execute("UPDATE ledger_write_attempts SET state='unknown',error='RESTART_WRITE_UNKNOWN' WHERE state='writing'")
        self.s.execute("UPDATE ledger_proposals SET state='needs_reconciliation' WHERE state='executing'")

    def collection(self,ident,c=None):
        rows=[dict(r) for r in c.execute('SELECT * FROM progress_collections WHERE id=?',(ident,))] if c else self.s.rows('SELECT * FROM progress_collections WHERE id=?',(ident,))
        if not rows:raise BoundaryError('COLLECTION_NOT_FOUND')
        return rows[0]
    def proposal(self,ident,c=None):
        rows=[dict(r) for r in c.execute('SELECT * FROM ledger_proposals WHERE id=?',(ident,))] if c else self.s.rows('SELECT * FROM ledger_proposals WHERE id=?',(ident,))
        if not rows:raise BoundaryError('PROPOSAL_NOT_FOUND')
        return rows[0]

    def collect(self,reporter,message_key,project_ref,text,source,anchor):
        if not text.strip() or len(text)>6000:raise BoundaryError('PROGRESS_TEXT_INVALID')
        with self.s.tx() as c:
            old=c.execute('SELECT * FROM progress_collections WHERE message_key=?',(message_key,)).fetchone()
            if old:return dict(old)
            ident=uuid.uuid4().hex
            c.execute("INSERT INTO progress_collections VALUES(?,?,?,?,1,?,?,?,0,'needs_reporter_check',?)",
                (ident,message_key,reporter,project_ref,canonical(source),digest(source),canonical(split_facts(text,anchor)),self.s.clock()))
            return self.collection(ident,c)

    def confirm_reporter(self,actor,ident,expected):
        with self.s.tx() as c:
            r=self.collection(ident,c)
            if actor!=r['reporter'] or r['version']!=expected or r['state']=='withdrawn':raise BoundaryError('REPORTER_CONFIRMATION_INVALID')
            c.execute("UPDATE progress_collections SET reporter_confirmed=1,state='checked_by_reporter' WHERE id=?",(ident,))
        return self.collection(ident)

    def propose(self,collection_id,patches=None):
        r=self.collection(collection_id)
        if r['state']=='withdrawn':raise BoundaryError('SOURCE_WITHDRAWN')
        basis=self.port.snapshot(r['project_ref'])
        if basis['identity_status']!='verified':raise BoundaryError('PROJECT_IDENTITY_UNVERIFIED')
        if patches is None:
            before=basis['fields']['项目进展']['value']
            if not isinstance(before,str):raise BoundaryError('RICH_TEXT_REQUIRES_EXACT_PATCH')
            patches={'项目进展':merge_progress(before,json.loads(r['facts']),self.s.clock())}
        if not patches or set(patches)-ALLOWED_FIELDS:raise BoundaryError('FIELD_NOT_ALLOWED')
        changes={}
        for field,after in patches.items():
            before=basis['fields'][field]['value']
            if isinstance(after,str) and (len(after)>40000 or after.startswith('=')):raise BoundaryError('PATCH_VALUE_INVALID')
            if before==after:continue
            changes[field]={'before':before,'after':after,'before_hash':digest(before),'source':collection_id,
                            'preservation':'未明确更正的旧事实与要求继续保留；当前新增项保持自报待核验'}
        if not changes:raise BoundaryError('NO_FIELD_CHANGES')
        with self.s.tx() as c:
            old=c.execute('SELECT * FROM ledger_proposals WHERE collection_id=? AND collection_version=? AND patches=?',(collection_id,r['version'],canonical(changes))).fetchone()
            if old:return dict(old)
            ident=uuid.uuid4().hex
            c.execute("INSERT INTO ledger_proposals VALUES(?,?,?,1,?,?,?,'proposed',NULL,NULL,?,?,?)",
                (ident,collection_id,r['version'],r['project_ref'],canonical(basis),canonical(changes),self.s.clock()+86400,ident,self.s.clock()))
            return self.proposal(ident,c)

    def approve(self,actor,ident,expected,fields,*,source_card,combined_reporter_check=False):
        if actor!=self.owner:raise BoundaryError('OWNER_WRITE_APPROVAL_REQUIRED')
        with self.s.tx() as c:
            p=self.proposal(ident,c);r=self.collection(p['collection_id'],c);patches=json.loads(p['patches'])
            if p['version']!=expected or p['expires']<=self.s.clock() or r['version']!=p['collection_version'] or r['state']=='withdrawn':raise BoundaryError('PROPOSAL_VERSION_CHANGED')
            if not r['reporter_confirmed']:
                if not(combined_reporter_check and r['reporter']==actor):raise BoundaryError('REPORTER_CHECK_REQUIRED')
                c.execute("UPDATE progress_collections SET reporter_confirmed=1,state='checked_by_reporter' WHERE id=?",(r['id'],))
            if not fields or set(fields)-set(patches):raise BoundaryError('APPROVAL_FIELD_SCOPE_INVALID')
            if p['state']=='approved':
                previous=json.loads(p['approval'])
                if set(previous['patches'])==set(fields) and previous['source_card']==source_card:return p
                raise BoundaryError('APPROVAL_ALREADY_BOUND')
            snapshot={'actor':actor,'source_card':source_card,'proposal_id':ident,'version':expected,'collection_version':r['version'],
                      'identity':json.loads(p['basis'])['identity'],'patches':{f:patches[f] for f in fields},'expires':min(p['expires'],self.s.clock()+600),'approved_at':self.s.clock()}
            if p['state'] in ('executing','needs_reconciliation','partial','verified'):raise BoundaryError('WRITE_ALREADY_STARTED')
            c.execute('INSERT INTO ledger_approval_snapshots VALUES(?,?,?,?)',(uuid.uuid4().hex,ident,expected,canonical(snapshot)))
            c.execute("UPDATE ledger_proposals SET state='approved',approval=?,approval_hash=? WHERE id=?",(canonical(snapshot),digest(snapshot),ident))
            return self.proposal(ident,c)

    def invalidate_source(self,actor,ident,expected,*,correction=None,correction_source=None):
        with self.s.tx() as c:
            r=self.collection(ident,c)
            if actor not in (self.owner,r['reporter']) or r['version']!=expected:raise BoundaryError('SOURCE_CORRECTION_INVALID')
            state='correction_candidate' if c.execute('SELECT 1 FROM ledger_write_attempts a JOIN ledger_proposals p ON p.id=a.proposal_id WHERE p.collection_id=?',(ident,)).fetchone() else 'withdrawn'
            c.execute('INSERT INTO progress_corrections VALUES(?,?,?,?,?,?,?)',(ident,expected,actor,r['facts'],correction,canonical(correction_source) if correction_source else None,self.s.clock()))
            if correction is not None:
                if not correction.strip() or not correction_source:raise BoundaryError('CORRECTION_SOURCE_REQUIRED')
                facts=canonical(split_facts(correction,correction_source.get('created',self.s.clock())))
                state='correction_candidate' if state=='correction_candidate' else 'needs_reporter_check'
                c.execute('UPDATE progress_collections SET facts=?,source=?,source_hash=? WHERE id=?',(facts,canonical(correction_source),digest(correction_source),ident))
            c.execute('UPDATE progress_collections SET version=version+1,state=?,reporter_confirmed=0 WHERE id=?',(state,ident))
            c.execute("UPDATE ledger_proposals SET state='source_changed',approval=NULL,approval_hash=NULL WHERE collection_id=? AND state IN ('proposed','approved')",(ident,))
        return self.collection(ident)

    def check_snapshot(self,current,basis,patches,completed):
        if current['identity']!=basis['identity'] or current['identity_status']!='verified':raise BoundaryError('PROJECT_IDENTITY_CHANGED')
        if current['headers']!=basis['headers']:raise BoundaryError('HEADERS_CHANGED')
        if current.get('protected')!=basis.get('protected'):raise BoundaryError('PROTECTED_STATE_CHANGED')
        for field,patch in patches.items():
            cell=current['fields'][field];expected=patch['after'] if field in completed else patch['before']
            if digest(cell['value'])!=digest(expected):raise BoundaryError('CELL_VALUE_CONFLICT')
            if cell.get('formula'):raise BoundaryError('FORMULA_PROTECTED')
            if cell.get('attributes')!=basis['fields'][field].get('attributes'):raise BoundaryError('CELL_ATTRIBUTES_CHANGED')
            if cell.get('rich') and not isinstance(patch['after'],(list,dict)):raise BoundaryError('RICH_TEXT_REQUIRES_EXACT_PATCH')

    def execute(self,ident):
        p=self.proposal(ident)
        if not self.enabled():return dict(p,execution_block='LEDGER_WRITE_NOT_ENABLED')
        if p['state'] in ('needs_reconciliation','partial','verified'):return self.reconcile(ident)
        if p['state']!='approved':raise BoundaryError('WRITE_APPROVAL_REQUIRED')
        approval=json.loads(p['approval'] or '{}');basis=json.loads(p['basis']);r=self.collection(p['collection_id'])
        if digest(approval)!=p['approval_hash'] or approval.get('actor')!=self.owner or approval.get('expires',0)<=self.s.clock() or r['version']!=p['collection_version']:raise BoundaryError('WRITE_APPROVAL_EXPIRED')
        if not self.port.source_current(json.loads(r['source'])):
            self.invalidate_source(r['reporter'],r['id'],r['version']);raise BoundaryError('SOURCE_CHANGED')
        patches=approval['patches'];current=self.port.snapshot(p['project_ref'])
        self.check_snapshot(current,basis,patches,set())
        if not current.get('write_ready'):return dict(p,execution_block=current.get('write_limitation','PROTECTED_STATE_UNVERIFIED'))
        if not self.s.execute("UPDATE ledger_proposals SET state='executing' WHERE id=? AND version=? AND state='approved'",(ident,p['version'])):return self.proposal(ident)
        completed=set()
        for field,patch in patches.items():
            attempt=uuid.uuid5(uuid.NAMESPACE_URL,p['request_key']+'/'+str(p['version'])+'/'+field).hex
            try:
                current=self.port.snapshot(p['project_ref']);self.check_snapshot(current,basis,patches,completed)
                latest=self.proposal(ident);source=self.collection(p['collection_id'])
                if latest['state']!='executing' or source['version']!=p['collection_version'] or approval['expires']<=self.s.clock():raise BoundaryError('WRITE_APPROVAL_CHANGED')
                with self.s.tx() as c:
                    n=c.execute("INSERT OR IGNORE INTO ledger_write_attempts VALUES(?,?,?,?,?,?,?,'writing',NULL,NULL,NULL,?)",
                        (attempt,ident,p['version'],field,attempt,canonical(patch['before']),canonical(patch['after']),self.s.clock())).rowcount
                    if not n:raise BoundaryError('WRITE_ATTEMPT_EXISTS')
                receipt=self.port.write_cell(current,field,patch['after'],attempt)
                self.s.execute("UPDATE ledger_write_attempts SET state='submitted',receipt=? WHERE id=?",(canonical(receipt),attempt))
                after=self.port.snapshot(p['project_ref']);self.check_snapshot(after,basis,patches,completed|{field})
                self.s.execute("UPDATE ledger_write_attempts SET state='verified',readback=? WHERE id=?",(canonical({'current_value_matches':True,'request_attribution':'platform_receipt_and_independent_read','identity':after['identity']}),attempt));completed.add(field)
            except Exception as exc:
                code=str(exc) if isinstance(exc,BoundaryError) else 'WRITE_OUTCOME_UNKNOWN'
                self.s.execute("UPDATE ledger_write_attempts SET state='unknown',error=? WHERE id=? AND state IN ('writing','submitted')",(code,attempt))
                self.s.execute("UPDATE ledger_proposals SET state=? WHERE id=?",('partial' if completed else 'needs_reconciliation',ident))
                return self.proposal(ident)
        self.s.execute("UPDATE ledger_proposals SET state='verified' WHERE id=?",(ident,))
        return self.proposal(ident)

    def reconcile(self,ident):
        p=self.proposal(ident);basis=json.loads(p['basis']);current=self.port.snapshot(p['project_ref'])
        attempts=self.s.rows('SELECT * FROM ledger_write_attempts WHERE proposal_id=?',(ident,))
        for a in attempts:
            if a['state']=='verified':continue
            matches=(current['identity']==basis['identity'] and current.get('protected')==basis.get('protected') and current['fields'][a['field']]['value']==json.loads(a['after_value']))
            if matches:
                state='verified' if a['receipt'] else 'matches_unattributed'
                self.s.execute('UPDATE ledger_write_attempts SET state=?,readback=? WHERE id=?',(state,canonical({'current_value_matches':True,'request_attribution':'platform_receipt_and_independent_read' if a['receipt'] else 'unverified'}),a['id']))
        all_attempts=self.s.rows('SELECT * FROM ledger_write_attempts WHERE proposal_id=?',(ident,))
        approved=json.loads(p['approval'] or '{}').get('patches',{})
        if all_attempts and len(all_attempts)==len(approved) and all(a['state']=='verified' for a in all_attempts):self.s.execute("UPDATE ledger_proposals SET state='verified' WHERE id=?",(ident,))
        return self.proposal(ident)
