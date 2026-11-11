"""Long-lived AM follow-up records, separate from inference and chat retention."""
import json
import uuid
from contextlib import nullcontext
from .config import BoundaryError
from .project_catalog import canonical,digest

STATES={'waiting','in_progress','blocked','submitted','closed','cancelled'}
FEEDBACK={'received','processing','eta','help','submitted','not_mine','reported_update'}


class ActionItems:
    def __init__(self,store,owner):
        self.s,self.owner=store,owner
        with store.lock:store.conn.executescript('''
        CREATE TABLE IF NOT EXISTS action_items(
          id TEXT PRIMARY KEY,version INTEGER NOT NULL,owner TEXT NOT NULL,project_ref TEXT NOT NULL,
          description TEXT NOT NULL,requirements TEXT NOT NULL,assignees TEXT NOT NULL,verifier TEXT NOT NULL,
          source TEXT NOT NULL,deadline TEXT,deadline_kind TEXT NOT NULL,timezone TEXT NOT NULL,
          state TEXT NOT NULL,paused INTEGER NOT NULL,latest_feedback TEXT,next_check REAL,
          closed_by TEXT,close_evidence TEXT,created REAL NOT NULL,updated REAL NOT NULL);
        CREATE TABLE IF NOT EXISTS item_operations(
          id TEXT PRIMARY KEY,item_id TEXT NOT NULL,actor TEXT NOT NULL,request_hash TEXT NOT NULL,result TEXT NOT NULL,created REAL NOT NULL);
        CREATE TABLE IF NOT EXISTS item_feedback(
          id TEXT PRIMARY KEY,item_id TEXT NOT NULL,item_version INTEGER NOT NULL,actor TEXT NOT NULL,kind TEXT NOT NULL,
          body TEXT NOT NULL,source TEXT NOT NULL,applied INTEGER NOT NULL,created REAL NOT NULL);
        CREATE TABLE IF NOT EXISTS item_feedback_raw(
          feedback_id TEXT PRIMARY KEY,item_id TEXT NOT NULL,body TEXT NOT NULL,expires REAL NOT NULL);
        CREATE TABLE IF NOT EXISTS feedback_heads(stream TEXT PRIMARY KEY,generation INTEGER NOT NULL,request_hash TEXT NOT NULL,operation_id TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS feedback_claims(submission_key TEXT PRIMARY KEY,stream TEXT NOT NULL,request_hash TEXT NOT NULL,operation_id TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS business_tombstones(kind TEXT NOT NULL,id TEXT NOT NULL,request_hash TEXT,closed REAL,PRIMARY KEY(kind,id));
        CREATE TABLE IF NOT EXISTS business_migrations(version TEXT PRIMARY KEY);
        INSERT OR IGNORE INTO business_migrations VALUES('C-items-1');
        ''')
        with store.tx() as c:
            columns={r[1] for r in c.execute('PRAGMA table_info(action_items)')}
            if 'decision_version' not in columns:
                c.execute('ALTER TABLE action_items ADD COLUMN decision_version INTEGER NOT NULL DEFAULT 1')
                for item in c.execute('SELECT * FROM action_items').fetchall():
                    versions=set();decisions=[]
                    for op in c.execute('SELECT actor,result FROM item_operations WHERE item_id=?',(item['id'],)).fetchall():
                        result=json.loads(op['result'])
                        if result.get('id')==item['id'] and op['actor'] in (item['owner'],item['verifier']):
                            versions.add(result['version']);decisions.append(result['version'])
                        elif result.get('applied') and result.get('item',{}).get('id')==item['id']:
                            versions.add(result['item']['version'])
                    # Only a complete journal proves the older card crossed no
                    # owner decision. Incomplete legacy history fails closed.
                    fence=max(decisions) if decisions and versions==set(range(1,item['version']+1)) else item['version']
                    c.execute('UPDATE action_items SET decision_version=? WHERE id=?',(fence,item['id']))
            c.execute("INSERT OR IGNORE INTO business_migrations VALUES('R1-FIX1-item-decisions-1')")

    def get(self,ident,c=None):
        rows=[dict(r) for r in c.execute('SELECT * FROM action_items WHERE id=?',(ident,))] if c else self.s.rows('SELECT * FROM action_items WHERE id=?',(ident,))
        if not rows:raise BoundaryError('ITEM_NOT_FOUND')
        return rows[0]

    def replay(self,c,op,request):
        r=c.execute('SELECT * FROM item_operations WHERE id=?',(op,)).fetchone()
        if r:
            if r['request_hash']!=digest(request):raise BoundaryError('OPERATION_ID_REUSED')
            return json.loads(r['result'])
        if c.execute("SELECT 1 FROM business_tombstones WHERE kind='operation' AND id=?",(op,)).fetchone():raise BoundaryError('OPERATION_EXPIRED')
        return None

    def log(self,c,op,item,actor,request,result):
        c.execute('INSERT INTO item_operations VALUES(?,?,?,?,?,?)',(op,item,actor,digest(request),canonical(result),self.s.clock()))

    def transaction(self,conn=None):
        if conn is None:return self.s.tx()
        if conn is not self.s.conn or not conn.in_transaction:raise BoundaryError('ITEM_TRANSACTION_REQUIRED')
        return nullcontext(conn)

    def create(self,actor,op,*,project_ref,description,requirements,assignees,source,deadline=None,deadline_kind='unspecified',verifier=None,next_check=None,conn=None):
        if actor!=self.owner:raise BoundaryError('OWNER_ONLY')
        if not project_ref or not description.strip() or len(description)>4000:raise BoundaryError('ITEM_FIELDS_INVALID')
        if not isinstance(assignees,list) or not assignees or not all(isinstance(u,str) and u.startswith('ou_') for u in assignees):raise BoundaryError('ASSIGNEES_UNVERIFIED')
        if deadline_kind not in ('unspecified','estimate','commitment','requirement','check'):raise BoundaryError('DEADLINE_KIND_REQUIRED')
        request={'project_ref':project_ref,'description':description,'requirements':requirements,'assignees':assignees,'source':source,'deadline':deadline,'deadline_kind':deadline_kind,'verifier':verifier or actor,'next_check':next_check}
        with self.transaction(conn) as c:
            replay=self.replay(c,op,request)
            if replay:return replay
            ident=uuid.uuid4().hex;now=self.s.clock()
            c.execute("INSERT INTO action_items VALUES(?,1,?,?,?,?,?,?,?,?,?,'Asia/Shanghai','waiting',0,NULL,?,NULL,NULL,?,?,1)",
                (ident,actor,project_ref,description,requirements,canonical(list(dict.fromkeys(assignees))),verifier or actor,canonical(source),deadline,deadline_kind,next_check,now,now))
            result=self.get(ident,c);self.log(c,op,ident,actor,request,result);return result

    def change(self,actor,op,ident,expected,action,*,evidence=None,next_check=None):
        request={'id':ident,'expected':expected,'action':action,'evidence':evidence,'next_check':next_check}
        with self.s.tx() as c:
            replay=self.replay(c,op,request)
            if replay:return replay
            item=self.get(ident,c)
            if actor!=item['owner'] and not(action=='close' and actor==item['verifier']):raise BoundaryError('ITEM_OPERATION_DENIED')
            if item['version']!=expected:raise BoundaryError('ITEM_VERSION_CHANGED')
            if item['state'] in ('closed','cancelled'):raise BoundaryError('ITEM_TERMINAL')
            state=item['state'];paused=item['paused'];closed_by=None;close_evidence=None
            if action=='processing':state='in_progress'
            elif action=='pause':paused=1
            elif action=='resume':paused=0
            elif action=='cancel':state='cancelled'
            elif action=='close':
                if actor!=item['verifier'] or not evidence or not evidence.strip():raise BoundaryError('VERIFICATION_EVIDENCE_REQUIRED')
                state='closed';closed_by=actor;close_evidence=evidence
            elif action=='check':
                if not isinstance(next_check,(int,float)):raise BoundaryError('CHECK_TIME_REQUIRED')
            else:raise BoundaryError('ITEM_ACTION_INVALID')
            from am_harness.reservation import reserve_effect
            reserve_effect(c,op,'followup_change',request)
            c.execute('UPDATE action_items SET decision_version=version+1,version=version+1,state=?,paused=?,next_check=?,closed_by=?,close_evidence=?,updated=? WHERE id=? AND version=?',
                (state,paused,next_check if action=='check' else item['next_check'],closed_by,close_evidence,self.s.clock(),ident,expected))
            result=self.get(ident,c);self.log(c,op,ident,actor,request,result);return result

    def claim_feedback(self,c,actor,ident,request,submission_key):
        # A stream belongs to one person on one occurrence, never the global
        # item version. Claim and business mutation commit in the same tx.
        stream=digest([ident,source_occurrence:=request['source']['occurrence'],actor])
        fingerprint=digest(request)
        old=c.execute('SELECT * FROM feedback_claims WHERE submission_key=?',(submission_key,)).fetchone()
        if old:
            if old['stream']!=stream or old['request_hash']!=fingerprint:raise BoundaryError('OPERATION_ID_REUSED')
            return old['operation_id']
        head=c.execute('SELECT * FROM feedback_heads WHERE stream=?',(stream,)).fetchone()
        if not head:
            # Adopt the last legacy claim for this person/occurrence. Upgrading
            # must not turn an immediate repeat into a new feedback event.
            for previous in c.execute('SELECT * FROM item_feedback WHERE item_id=? AND actor=? ORDER BY rowid DESC',(ident,actor)).fetchall():
                if json.loads(previous['source']).get('occurrence')!=source_occurrence:continue
                operation=c.execute('SELECT request_hash FROM item_operations WHERE id=?',(previous['id'],)).fetchone()
                if operation:
                    c.execute('INSERT INTO feedback_heads VALUES(?,0,?,?)',(stream,operation['request_hash'],previous['id']))
                    head=c.execute('SELECT * FROM feedback_heads WHERE stream=?',(stream,)).fetchone()
                break
        if head and head['request_hash']==fingerprint:op=head['operation_id']
        else:
            generation=head['generation']+1 if head else 1
            op='feedback/'+digest([stream,generation,fingerprint])
            c.execute('INSERT INTO feedback_heads VALUES(?,?,?,?) ON CONFLICT(stream) DO UPDATE SET generation=excluded.generation,request_hash=excluded.request_hash,operation_id=excluded.operation_id',(stream,generation,fingerprint,op))
        c.execute('INSERT INTO feedback_claims VALUES(?,?,?,?)',(submission_key,stream,fingerprint,op))
        return op

    def feedback(self,actor,op,ident,expected,kind,body,source,*,submission_key=None,conn=None):
        if kind not in FEEDBACK or len(body)>4000:raise BoundaryError('FEEDBACK_INVALID')
        expiry=source.get('raw_expires_at')
        if expiry is not None and (type(expiry) not in (int,float) or not self.s.clock()<expiry<=self.s.clock()+86400):
            raise BoundaryError('FEEDBACK_RAW_EXPIRED')
        request={'id':ident,'expected':expected,'kind':kind,'body':body,'source':source}
        with self.transaction(conn) as c:
            if submission_key:
                item=self.get(ident,c)
                if actor not in json.loads(item['assignees']):raise BoundaryError('NOT_ASSIGNEE')
                op=self.claim_feedback(c,actor,ident,request,submission_key)
            replay=self.replay(c,op,request)
            if replay:return replay
            item=self.get(ident,c)
            if actor not in json.loads(item['assignees']):raise BoundaryError('NOT_ASSIGNEE')
            applied=item['decision_version']<=expected<=item['version'] and not item['paused'] and item['state'] not in ('closed','cancelled')
            # Current chat text follows the original 24-hour policy. Long-lived
            # operations store only a neutral marker and a source-bound ID.
            stored_body=('自报进展，待核对。' if kind=='reported_update' else '已回应。') if expiry is not None else body
            c.execute('INSERT INTO item_feedback VALUES(?,?,?,?,?,?,?,?,?)',(op,ident,expected,actor,kind,stored_body,canonical(source),int(applied),self.s.clock()))
            if expiry is not None:
                c.execute('INSERT INTO item_feedback_raw VALUES(?,?,?,?)',(op,ident,body,expiry))
            if applied:
                state={'processing':'in_progress','help':'blocked','submitted':'submitted'}.get(kind,item['state'])
                # Received only records a response. ETA is not a committed deadline;
                # not_mine is a request for adjustment, never an automatic transfer.
                feedback=canonical(dict({'kind':kind,'body':stored_body,'actor':actor,'time':self.s.clock()},
                    **({'raw_feedback_id':op,'raw_expires_at':expiry} if expiry is not None else {})))
                c.execute('UPDATE action_items SET version=version+1,state=?,latest_feedback=?,updated=? WHERE id=? AND version=?',(state,feedback,self.s.clock(),ident,item['version']))
            result={'applied':applied,'late_evidence':not applied,'item':self.get(ident,c),'business_event_id':op}
            self.log(c,op,ident,actor,request,result);return result

    def history(self,ident,*,include_raw=False):
        rows=self.s.rows('SELECT id,actor,kind,body,source,applied,created FROM item_feedback WHERE item_id=? ORDER BY created,rowid',(ident,))
        for row in rows:
            if include_raw and json.loads(row['source']).get('raw_expires_at') is not None:
                row['body']=self.feedback_body({'raw_feedback_id':row['id'],'raw_expires_at':json.loads(row['source'])['raw_expires_at']})
        return rows

    def feedback_expiry(self,ident):
        rows=self.s.rows('SELECT MIN(expires) AS expires FROM item_feedback_raw WHERE item_id=? AND expires>?',
            (ident,self.s.clock()))
        return rows[0]['expires'] if rows else None

    def feedback_body(self,feedback):
        if not feedback.get('raw_feedback_id'):return feedback.get('body','')
        if feedback.get('raw_expires_at',0)<=self.s.clock():return '原文已过 24 小时留存期；回应记录保留，内容需重新提供。'
        raw=self.s.rows('SELECT body FROM item_feedback_raw WHERE feedback_id=? AND expires>?',
            (feedback['raw_feedback_id'],self.s.clock()))
        return raw[0]['body'] if raw else '原文当前不可读；请重新提供，尚未据此完成业务。'

    def expire_reply_content(self):
        return self.s.execute('DELETE FROM item_feedback_raw WHERE expires<=?',(self.s.clock(),))

    def cleanup_preview(self,terminal_days=30):
        if type(terminal_days) is not int or terminal_days<7:raise BoundaryError('RETENTION_TOO_SHORT')
        return self.s.rows("SELECT id,version FROM action_items WHERE state IN ('closed','cancelled') AND updated<? AND description!='已归档' AND id NOT IN (SELECT item_id FROM reminder_plans WHERE item_id IS NOT NULL AND state IN ('active','paused')) AND id NOT IN (SELECT item_id FROM reminder_occurrences WHERE item_id IS NOT NULL AND (state IN ('sending','unknown') OR state='sent' AND COALESCE(readback_state,'pending')!='verified'))",(self.s.clock()-terminal_days*86400,))

    def apply_cleanup(self,actor,op,candidates,terminal_days=30):
        if actor!=self.owner:raise BoundaryError('OWNER_ONLY')
        request={'cleanup':candidates,'terminal_days':terminal_days}
        with self.s.tx() as c:
            old=self.replay(c,op,request)
            if old:return old
            current={r['id']:r['version'] for r in self.cleanup_preview(terminal_days)}
            if any(current.get(r['id'])!=r['version'] for r in candidates):raise BoundaryError('RETENTION_PREVIEW_CHANGED')
            for row in candidates:
                ident=row['id']
                for operation in c.execute('SELECT * FROM item_operations WHERE item_id=?',(ident,)).fetchall():
                    c.execute("INSERT OR IGNORE INTO business_tombstones VALUES('operation',?,?,?)",(operation['id'],operation['request_hash'],self.s.clock()))
                    c.execute('DELETE FROM item_operations WHERE id=?',(operation['id'],))
                for plan in c.execute('SELECT * FROM reminder_plans WHERE item_id=?',(ident,)).fetchall():
                    for approval in c.execute("SELECT * FROM action_approvals WHERE kind='plan' AND object_id=?",(plan['id'],)).fetchall():
                        c.execute("INSERT OR IGNORE INTO business_tombstones VALUES('approval',?,?,?)",(approval['id'],approval['hash'],self.s.clock()))
                        c.execute('DELETE FROM action_approvals WHERE id=?',(approval['id'],))
                    c.execute("UPDATE reminder_plans SET target='{}',template='',allowed_variables='[]' WHERE id=?",(plan['id'],))
                c.execute("UPDATE item_feedback SET body='',source='{}' WHERE item_id=?",(ident,))
                c.execute('DELETE FROM item_feedback_raw WHERE item_id=?',(ident,))
                c.execute("UPDATE reminder_occurrences SET payload='{}',receipt=NULL WHERE item_id=? AND state NOT IN ('sending','unknown')",(ident,))
                c.execute("UPDATE action_items SET description='已归档',requirements='',source='{}',latest_feedback=NULL,close_evidence=NULL WHERE id=?",(ident,))
                c.execute("INSERT OR IGNORE INTO business_tombstones VALUES('item',?,?,?)",(ident,digest(row),self.s.clock()))
            result={'archived_count':len(candidates),'retained':'identities, states, versions, hashes and idempotency tombstones'}
            self.log(c,op,'retention',actor,request,result);return result
