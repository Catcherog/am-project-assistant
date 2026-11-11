"""AM-owned project identities, successful snapshots and non-consuming change queries."""
import hashlib
import json
import re
import time
import uuid


def canonical(value):
    return json.dumps(value,ensure_ascii=False,sort_keys=True,separators=(',',':'))


def digest(value):
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def business_ids(title):
    ids=re.findall(r'(?<![A-Za-z0-9])(?:IRAD|CERT|RA|QI|PT)-\d{4}-\d{3}(?!\d)',title,re.I)
    ids += ['需求'+str(int(x)) for x in re.findall(r'需求\s*(\d+)(?!\d)',title)]
    return list(dict.fromkeys(x.upper() for x in ids))


class ProjectCatalog:
    def __init__(self,db,namespace,clock=time.time):
        self.db,self.namespace,self.clock=db,namespace,clock
        db.executescript('''
        CREATE TABLE IF NOT EXISTS project_refs(
          id TEXT PRIMARY KEY,namespace TEXT NOT NULL,business_id TEXT,title TEXT NOT NULL,
          aliases TEXT NOT NULL,locator TEXT NOT NULL,identity_status TEXT NOT NULL,fingerprint TEXT NOT NULL,
          source_revision TEXT,UNIQUE(namespace,business_id));
        CREATE TABLE IF NOT EXISTS source_facts(id TEXT PRIMARY KEY,project_ref TEXT NOT NULL,source_fingerprint TEXT NOT NULL,field TEXT NOT NULL,position INTEGER NOT NULL,original TEXT NOT NULL,evidence_status TEXT NOT NULL,read_at REAL NOT NULL);
        CREATE TABLE IF NOT EXISTS project_snapshots(
          project_id TEXT PRIMARY KEY,body TEXT NOT NULL,fingerprint TEXT NOT NULL,read_at REAL NOT NULL,revision TEXT);
        CREATE TABLE IF NOT EXISTS source_baselines(namespace TEXT PRIMARY KEY,fingerprint TEXT NOT NULL,read_at REAL NOT NULL);
        CREATE TABLE IF NOT EXISTS project_changes(
          seq INTEGER PRIMARY KEY AUTOINCREMENT,id TEXT UNIQUE NOT NULL,namespace TEXT NOT NULL,project_id TEXT NOT NULL,
          field TEXT NOT NULL,before_value TEXT NOT NULL,after_value TEXT NOT NULL,kind TEXT NOT NULL,read_at REAL NOT NULL);
        CREATE TABLE IF NOT EXISTS briefing_subscriptions(
          id TEXT PRIMARY KEY,owner TEXT NOT NULL,scope TEXT NOT NULL,plan_id TEXT,active INTEGER NOT NULL,version INTEGER NOT NULL);
        CREATE TABLE IF NOT EXISTS briefing_deliveries(
          subscription_id TEXT NOT NULL,event_id TEXT NOT NULL,occurrence_id TEXT NOT NULL,state TEXT NOT NULL,
          PRIMARY KEY(subscription_id,event_id));
        CREATE TABLE IF NOT EXISTS catalog_current(namespace TEXT NOT NULL,project_id TEXT NOT NULL,fingerprint TEXT NOT NULL,PRIMARY KEY(namespace,project_id));
        CREATE TABLE IF NOT EXISTS project_mapping_decisions(source_ref TEXT PRIMARY KEY,target_ref TEXT NOT NULL,expected_hash TEXT NOT NULL,actor TEXT NOT NULL,recorded REAL NOT NULL);
        CREATE TABLE IF NOT EXISTS catalog_migrations(version TEXT PRIMARY KEY);
        INSERT OR IGNORE INTO catalog_migrations VALUES('B-catalog-1');
        ''')
        db.commit()

    def sync(self,projects,*,complete=True,revision=None,read_at=None):
        if not complete:
            raise ValueError('SOURCE_PARTIAL_BASELINE_UNCHANGED')
        now=self.clock() if read_at is None else read_at
        code_counts={}
        for p in projects:
            for code in business_ids(p['title']):code_counts[code]=code_counts.get(code,0)+1
        baseline=self.db.execute('SELECT 1 FROM source_baselines WHERE namespace=?',(self.namespace,)).fetchone()
        output=[]
        with self.db:
            for p in projects:
                codes=business_ids(p['title']);code=codes[0] if len(codes)==1 else None
                status='verified' if code and code_counts[code]==1 else 'conflict' if codes else 'needs_mapping'
                stable_code=code if status=='verified' else None
                ident=uuid.uuid5(uuid.NAMESPACE_URL,self.namespace+'/'+(stable_code or 'unmapped/'+p['title'])).hex
                if stable_code:
                    assigned=self.db.execute('SELECT id FROM project_refs WHERE namespace=? AND business_id=?',(self.namespace,stable_code)).fetchone()
                    if assigned:ident=assigned['id']
                mapping=self.db.execute('SELECT target_ref FROM project_mapping_decisions WHERE source_ref=?',(ident,)).fetchone()
                if mapping:ident=mapping['target_ref'];status='verified'
                prior=self.db.execute('SELECT * FROM project_refs WHERE id=?',(ident,)).fetchone()
                if prior and prior['identity_status']!='verified' and status!='conflict':status=prior['identity_status']
                # A changed number is never merged by similarity. Identical names
                # with another number require an explicit mapping decision.
                bare=re.sub(r'(?:IRAD|CERT|RA|QI|PT)-\d{4}-\d{3}|需求\s*\d+','',p['title'],flags=re.I).strip(' ｜:：\n')
                if not prior and stable_code and bare:
                    for old in self.db.execute('SELECT title,business_id FROM project_refs WHERE namespace=?',(self.namespace,)):
                        old_bare=re.sub(r'(?:IRAD|CERT|RA|QI|PT)-\d{4}-\d{3}|需求\s*\d+','',old['title'],flags=re.I).strip(' ｜:：\n')
                        if old_bare==bare and old['business_id']!=stable_code:status='needs_mapping'
                fields={k:p.get(k,'') for k in ('title','关键日期','待办','项目进展','人员与职责')}
                fingerprint=digest(fields)
                aliases=json.loads(prior['aliases']) if prior else []
                if p['title'] not in aliases:aliases.append(p['title'])
                locator=canonical({'row':p['row'],'progress_heading':p.get('progress_heading')})
                self.db.execute('INSERT INTO project_refs VALUES(?,?,?,?,?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET title=excluded.title,aliases=excluded.aliases,locator=excluded.locator,identity_status=excluded.identity_status,fingerprint=excluded.fingerprint,source_revision=excluded.source_revision',
                    (ident,self.namespace,stable_code,p['title'],canonical(aliases),locator,status,fingerprint,str(revision)))
                old=self.db.execute('SELECT * FROM project_snapshots WHERE project_id=?',(ident,)).fetchone()
                if status=='verified':
                    self.db.execute('DELETE FROM source_facts WHERE project_ref=?',(ident,))
                    for field,value in fields.items():
                        for position,line in enumerate(value.splitlines(),1):
                            if line.strip():self.db.execute('INSERT INTO source_facts VALUES(?,?,?,?,?,?,?,?)',(digest([ident,fingerprint,field,position]),ident,fingerprint,field,position,line,'recorded_not_independently_verified',now))
                    if baseline and old and old['fingerprint']!=fingerprint:
                        previous=json.loads(old['body'])
                        for field,value in fields.items():
                            if previous.get(field,'')==value:continue
                            event=digest([ident,old['fingerprint'],fingerprint,field,old['read_at']])
                            self.db.execute('INSERT OR IGNORE INTO project_changes(id,namespace,project_id,field,before_value,after_value,kind,read_at) VALUES(?,?,?,?,?,?,?,?)',
                                (event,self.namespace,ident,field,previous.get(field,''),value,'record_text_changed',now))
                    elif baseline and not old:
                        self.db.execute('INSERT OR IGNORE INTO project_changes(id,namespace,project_id,field,before_value,after_value,kind,read_at) VALUES(?,?,?,?,?,?,?,?)',
                            (digest([ident,fingerprint,'first_seen']),self.namespace,ident,'title','',p['title'],'newly_observed_record',now))
                    self.db.execute('INSERT INTO project_snapshots VALUES(?,?,?,?,?) ON CONFLICT(project_id) DO UPDATE SET body=excluded.body,fingerprint=excluded.fingerprint,read_at=excluded.read_at,revision=excluded.revision',
                                    (ident,canonical(fields),fingerprint,now,str(revision)))
                output.append(dict(p,key=ident,project_ref=ident,business_id=code,identity_status=status,content_fingerprint=fingerprint,aliases=aliases))
            if len({p['key'] for p in output})!=len(output):raise ValueError('PROJECT_MAPPING_CONFLICT')
            self.db.execute('DELETE FROM catalog_current WHERE namespace=?',(self.namespace,))
            self.db.executemany('INSERT INTO catalog_current VALUES(?,?,?)',[(self.namespace,p['key'],p['content_fingerprint']) for p in output])
            self.db.execute('INSERT INTO source_baselines VALUES(?,?,?) ON CONFLICT(namespace) DO UPDATE SET fingerprint=excluded.fingerprint,read_at=excluded.read_at',
                            (self.namespace,digest(sorted((p['key'],p['content_fingerprint']) for p in output)),now))
        return output

    def changes(self,limit=30):
        return [dict(r) for r in self.db.execute('SELECT c.*,r.title FROM project_changes c JOIN project_refs r ON r.id=c.project_id WHERE c.namespace=? ORDER BY c.seq DESC LIMIT ?', (self.namespace,limit))]

    def map_identity(self,ident,expected_fingerprint,*,owner_verified=False):
        if not owner_verified:raise ValueError('OWNER_MAPPING_REQUIRED')
        with self.db:
            return self.db.execute("UPDATE project_refs SET identity_status='verified' WHERE id=? AND namespace=? AND fingerprint=? AND business_id IS NOT NULL",(ident,self.namespace,expected_fingerprint)).rowcount==1

    def confirm_mapping(self,source_ref,target_ref,expected_hash,actor):
        with self.db:
            previous=self.db.execute('SELECT * FROM project_mapping_decisions WHERE source_ref=?',(source_ref,)).fetchone()
            if previous:
                if previous['target_ref']!=target_ref:raise ValueError('MAPPING_ALREADY_BOUND')
                return target_ref
            source=self.db.execute('SELECT r.* FROM project_refs r JOIN catalog_current c ON c.project_id=r.id WHERE r.id=? AND r.namespace=? AND c.fingerprint=?',(source_ref,self.namespace,expected_hash)).fetchone()
            target=self.db.execute('SELECT * FROM project_refs WHERE id=? AND namespace=?',(target_ref,self.namespace)).fetchone()
            if not source or not target or source_ref==target_ref:raise ValueError('MAPPING_SOURCE_CHANGED')
            if self.db.execute('SELECT 1 FROM catalog_current WHERE namespace=? AND project_id=?',(self.namespace,target_ref)).fetchone():raise ValueError('MAPPING_TARGET_STILL_PRESENT')
            aliases=list(dict.fromkeys(json.loads(target['aliases'])+json.loads(source['aliases'])))
            self.db.execute('UPDATE project_refs SET business_id=NULL,identity_status=? WHERE id=?',('needs_mapping',source_ref))
            self.db.execute('UPDATE project_refs SET business_id=?,aliases=?,identity_status=? WHERE id=?',(source['business_id'] or target['business_id'],canonical(aliases),'verified',target_ref))
            self.db.execute('INSERT INTO project_mapping_decisions VALUES(?,?,?,?,?)',(source_ref,target_ref,expected_hash,actor,self.clock()))
            return target_ref
