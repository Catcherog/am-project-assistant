"""Single-process scanning with SQLite claims; unknown outcomes never become sends."""
from __future__ import annotations
import json
import re
import string
import uuid
from datetime import datetime,timedelta
from zoneinfo import ZoneInfo
from .config import BoundaryError
from .project_catalog import canonical,digest
from .feishu import SendRejected,SendUnknown

TZ=ZoneInfo('Asia/Shanghai')


def parse_time(value,anchor):
    if not isinstance(value,str):raise BoundaryError('EXACT_SEND_TIME_REQUIRED')
    value=value.strip();base=datetime.fromtimestamp(anchor,TZ)
    m=re.fullmatch(r'(今天|明天|后天)\s*(\d{1,2}):(\d{2})',value)
    if m:
        return (base+timedelta(days={'今天':0,'明天':1,'后天':2}[m[1]])).replace(hour=int(m[2]),minute=int(m[3]),second=0,microsecond=0).timestamp()
    try:
        dt=datetime.fromisoformat(value)
        if dt.tzinfo is None:dt=dt.replace(tzinfo=TZ)
        return dt.timestamp()
    except ValueError:raise BoundaryError('EXACT_SEND_TIME_REQUIRED') from None


class Calendar:
    def __init__(self,data=None):self.data=data or {}
    def working(self,date):
        d=self.data
        if not all(d.get(k) for k in ('source','version','verified_at','start','end')) or not d.get('start','')<=date<=d.get('end','') or date not in d.get('days',{}):
            raise BoundaryError('CALENDAR_COVERAGE_MISSING')
        if type(d['days'][date]) is not bool:raise BoundaryError('CALENDAR_INVALID')
        return d['days'][date]
    def next_work_morning(self,stamp):
        dt=datetime.fromtimestamp(stamp,TZ)
        for offset in range(367):
            day=(dt+timedelta(days=offset)).date()
            if self.working(day.isoformat()):
                candidate=datetime.combine(day,datetime.min.time(),TZ).replace(hour=9).timestamp()
                if candidate>=stamp:return candidate
        raise BoundaryError('CALENDAR_COVERAGE_MISSING')


class Scheduler:
    def __init__(self,store,items,owner,port,*,calendar=None,enabled=lambda:False,notify=lambda *a:None,briefing=None):
        self.s,self.items,self.owner,self.port=store,items,owner,port
        self.calendar=Calendar(calendar);self.enabled=enabled;self.notify=notify;self.briefing=briefing
        self.followup=None
        with store.lock:store.conn.executescript('''
        CREATE TABLE IF NOT EXISTS reminder_plans(
          id TEXT PRIMARY KEY,version INTEGER NOT NULL,item_id TEXT,owner TEXT NOT NULL,kind TEXT NOT NULL,
          rule TEXT NOT NULL,target TEXT NOT NULL,template TEXT NOT NULL,allowed_variables TEXT NOT NULL,
          policy TEXT NOT NULL,state TEXT NOT NULL,next_at REAL NOT NULL,valid_until REAL NOT NULL,
          window_seconds INTEGER NOT NULL,approval_id TEXT,hash TEXT NOT NULL,error TEXT,created REAL NOT NULL);
        CREATE TABLE IF NOT EXISTS action_approvals(
          id TEXT PRIMARY KEY,kind TEXT NOT NULL,object_id TEXT NOT NULL,version INTEGER NOT NULL,
          actor TEXT NOT NULL,snapshot TEXT NOT NULL,hash TEXT NOT NULL,expires REAL NOT NULL,created REAL NOT NULL);
        CREATE TABLE IF NOT EXISTS reminder_occurrences(
          id TEXT PRIMARY KEY,plan_id TEXT NOT NULL,plan_version INTEGER NOT NULL,scheduled_at REAL NOT NULL,
          item_id TEXT,item_version INTEGER,approval_id TEXT,payload TEXT NOT NULL,hash TEXT NOT NULL,
          request_key TEXT UNIQUE NOT NULL,deadline REAL NOT NULL,state TEXT NOT NULL,attempts INTEGER NOT NULL,
          next_attempt REAL NOT NULL,send_started REAL,message_id TEXT,receipt TEXT,readback_state TEXT,
          cancel_requested INTEGER NOT NULL DEFAULT 0,error TEXT,reconcile_attempts INTEGER NOT NULL DEFAULT 0,
          next_reconcile REAL NOT NULL DEFAULT 0,UNIQUE(plan_id,plan_version,scheduled_at));
        CREATE TABLE IF NOT EXISTS scheduler_operations(id TEXT PRIMARY KEY,request_hash TEXT NOT NULL,result TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS am_subscriptions(id TEXT PRIMARY KEY,owner TEXT NOT NULL,plan_id TEXT UNIQUE NOT NULL,state TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS am_briefing_deliveries(subscription_id TEXT NOT NULL,event_id TEXT NOT NULL,occurrence_id TEXT NOT NULL,state TEXT NOT NULL,PRIMARY KEY(subscription_id,event_id));
        CREATE TABLE IF NOT EXISTS am_briefing_delivery_attempts(subscription_id TEXT NOT NULL,event_id TEXT NOT NULL,occurrence_id TEXT NOT NULL,state TEXT NOT NULL,PRIMARY KEY(subscription_id,event_id,occurrence_id));
        INSERT OR IGNORE INTO am_briefing_delivery_attempts SELECT * FROM am_briefing_deliveries;
        INSERT OR IGNORE INTO business_migrations VALUES('C-scheduler-1');
        INSERT OR IGNORE INTO business_migrations VALUES('R1-FIX1-deliveries-1');
        ''')
        self.s.execute("UPDATE reminder_occurrences SET state='unknown',error='RESTART_SEND_UNKNOWN',readback_state='pending' WHERE state='sending'")
        with self.s.tx() as c:self.sync_deliveries(c)

    def sync_deliveries(self,c):
        # Retain every association. Only proven unsent attempts release an event;
        # absence of an occurrence/receipt is never evidence of non-delivery.
        rows=c.execute('SELECT d.*,o.state AS outcome,o.readback_state,o.deadline,o.attempts FROM am_briefing_deliveries d LEFT JOIN reminder_occurrences o ON o.id=d.occurrence_id').fetchall()
        for r in rows:
            state='pending'
            if r['state']=='verified' or r['outcome']=='sent' and r['readback_state']=='verified':state='verified'
            elif r['outcome'] in (None,'sending','unknown','sent'):state='unknown'
            elif r['outcome']=='cancelled' or r['outcome'] in ('ready','awaiting_confirmation','confirmed_not_sent') and (r['deadline']<=self.s.clock() or r['attempts']>=3):state='not_sent'
            c.execute('UPDATE am_briefing_deliveries SET state=? WHERE subscription_id=? AND event_id=?',(state,r['subscription_id'],r['event_id']))
            c.execute('INSERT INTO am_briefing_delivery_attempts VALUES(?,?,?,?) ON CONFLICT(subscription_id,event_id,occurrence_id) DO UPDATE SET state=excluded.state',(r['subscription_id'],r['event_id'],r['occurrence_id'],state))

    def blocked(self,c,o):
        candidates=c.execute("SELECT * FROM reminder_occurrences WHERE id!=? AND (item_id=? OR plan_id=?) AND (state IN ('sending','unknown') OR state='sent' AND COALESCE(readback_state,'pending')!='verified')",(o['id'],o['item_id'],o['plan_id'])).fetchall()
        payload=json.loads(o['payload'])
        for other in candidates:
            if other['plan_id']==o['plan_id']:return True
            prior=json.loads(other['payload'])
            if prior.get('target')==payload.get('target') and prior.get('body')==payload.get('body'):return True
        return False

    def plan(self,ident,c=None):
        rows=[dict(r) for r in c.execute('SELECT * FROM reminder_plans WHERE id=?',(ident,))] if c else self.s.rows('SELECT * FROM reminder_plans WHERE id=?',(ident,))
        if not rows:raise BoundaryError('PLAN_NOT_FOUND')
        return rows[0]

    def occurrence(self,ident,c=None):
        rows=[dict(r) for r in c.execute('SELECT * FROM reminder_occurrences WHERE id=?',(ident,))] if c else self.s.rows('SELECT * FROM reminder_occurrences WHERE id=?',(ident,))
        if not rows:raise BoundaryError('OCCURRENCE_NOT_FOUND')
        return rows[0]

    def create(self,actor,op,*,item_id,kind,rule,target,template,policy='preview_each',valid_until,window_seconds=60,allowed_variables=None):
        if actor!=self.owner:raise BoundaryError('OWNER_ONLY')
        if kind not in ('once','daily','workdays','briefing'):raise BoundaryError('PLAN_KIND_INVALID')
        if policy not in ('preview_each','fixed_rule','owner_subscription'):raise BoundaryError('PLAN_APPROVAL_POLICY_INVALID')
        if not isinstance(window_seconds,int) or not 1<=window_seconds<=3600:raise BoundaryError('EXECUTION_WINDOW_INVALID')
        allowed_variables=allowed_variables or []
        variables=[field for _,field,fmt,conv in string.Formatter().parse(template) if field is not None]
        if set(variables)-set(allowed_variables) or set(allowed_variables)-{'description','requirements','deadline','state'}:raise BoundaryError('TEMPLATE_VARIABLE_NOT_ALLOWED')
        if any(fmt or conv for _,field,fmt,conv in string.Formatter().parse(template)):raise BoundaryError('TEMPLATE_FORMAT_INVALID')
        if target.get('kind')=='owner':
            if target.get('user')!=actor:raise BoundaryError('SUBSCRIPTION_TARGET_INVALID')
        elif target.get('kind')!='group' or not target.get('payload'):raise BoundaryError('EXACT_TARGET_REQUIRED')
        if policy=='owner_subscription' and (target.get('kind')!='owner' or kind!='briefing'):raise BoundaryError('SUBSCRIPTION_TARGET_INVALID')
        if kind!='briefing':self.items.get(item_id)
        if kind=='once':
            next_at=rule.get('at')
            if not isinstance(next_at,(float,int)) or next_at<self.s.clock():raise BoundaryError('FUTURE_SEND_TIME_REQUIRED')
        else:
            if rule.get('timezone')!='Asia/Shanghai' or not re.fullmatch(r'(?:[01]\d|2[0-3]):[0-5]\d',rule.get('time','')):raise BoundaryError('SCHEDULE_TIME_REQUIRED')
            next_at=self.next_time(kind,rule,self.s.clock()-1)
        if valid_until<=next_at or valid_until>self.s.clock()+366*86400:raise BoundaryError('PLAN_VALIDITY_REQUIRED')
        request=dict(item_id=item_id,kind=kind,rule=rule,target=target,template=template,policy=policy,valid_until=valid_until,window_seconds=window_seconds,allowed_variables=allowed_variables)
        with self.s.tx() as c:
            old=c.execute('SELECT * FROM scheduler_operations WHERE id=?',(op,)).fetchone()
            if old:
                if old['request_hash']!=digest(request):raise BoundaryError('OPERATION_ID_REUSED')
                return self.plan(json.loads(old['result'])['id'],c)
            ident=uuid.uuid4().hex
            c.execute("INSERT INTO reminder_plans VALUES(?,1,?,?,?,?,?,?,?,?, 'draft',?,?,?,NULL,?,NULL,?)",
                (ident,item_id,actor,kind,canonical(rule),canonical(target),template,canonical(allowed_variables),policy,next_at,valid_until,window_seconds,digest(request),self.s.clock()))
            c.execute('INSERT INTO scheduler_operations VALUES(?,?,?)',(op,digest(request),canonical({'id':ident})))
            return self.plan(ident,c)

    def next_time(self,kind,rule,after):
        if kind=='once':return float('inf')
        after=max(after,rule.get('start_at',after+1)-1)
        dt=datetime.fromtimestamp(after,TZ);hour,minute=map(int,rule['time'].split(':'))
        for offset in range(367):
            candidate=(dt+timedelta(days=offset)).replace(hour=hour,minute=minute,second=0,microsecond=0)
            if candidate.timestamp()<=after:continue
            if kind=='workdays' or rule.get('workdays') or rule.get('quiet_policy')=='business_hours':
                if rule.get('calendar_version')!=self.calendar.data.get('version'):raise BoundaryError('CALENDAR_VERSION_CHANGED')
                if not self.calendar.working(candidate.date().isoformat()):continue
                if candidate.hour>=18:
                    return self.calendar.next_work_morning(candidate.timestamp())
            return candidate.timestamp()
        raise BoundaryError('CALENDAR_COVERAGE_MISSING')

    def approve_plan(self,actor,ident,expected,expected_hash):
        with self.s.tx() as c:
            p=self.plan(ident,c)
            if actor!=p['owner'] or p['version']!=expected or p['hash']!=expected_hash:raise BoundaryError('PLAN_APPROVAL_CHANGED')
            if p['state']=='active' and p['approval_id']:return p
            if p['state']!='draft' or p['valid_until']<=self.s.clock():raise BoundaryError('PLAN_APPROVAL_EXPIRED')
            from am_harness.reservation import reserve_effect
            reserve_effect(c,ident,'schedule',{'version':expected,'hash':expected_hash})
            approval=uuid.uuid4().hex;snapshot={k:p[k] for k in ('id','version','item_id','owner','kind','rule','target','template','allowed_variables','policy','next_at','valid_until','window_seconds','hash')}
            c.execute('INSERT INTO action_approvals VALUES(?,?,?,?,?,?,?,?,?)',(approval,'plan',ident,expected,actor,canonical(snapshot),digest(snapshot),p['valid_until'],self.s.clock()))
            c.execute("UPDATE reminder_plans SET state='active',approval_id=? WHERE id=?",(approval,ident))
            return self.plan(ident,c)

    def control(self,actor,op,ident,expected,action):
        if action not in ('pause','resume','cancel'):raise BoundaryError('PLAN_CONTROL_INVALID')
        with self.s.tx() as c:
            p=self.plan(ident,c);req=[actor,ident,expected,action]
            old=c.execute('SELECT * FROM scheduler_operations WHERE id=?',(op,)).fetchone()
            if old:
                if old['request_hash']!=digest(req):raise BoundaryError('OPERATION_ID_REUSED')
                return p
            if actor!=p['owner'] or p['version']!=expected:raise BoundaryError('PLAN_VERSION_CHANGED')
            if p['state']=='cancelled':raise BoundaryError('PLAN_CANCELLED')
            state={'pause':'paused','resume':'draft','cancel':'cancelled'}[action]
            # Resume changes plan version and requires a new complete preview.
            c.execute('UPDATE reminder_plans SET state=?,version=version+1,approval_id=NULL WHERE id=?',(state,ident))
            c.execute("UPDATE reminder_occurrences SET cancel_requested=1,state=CASE WHEN state IN ('ready','awaiting_confirmation','confirmed_not_sent') THEN 'cancelled' ELSE state END WHERE plan_id=?",(ident,))
            c.execute('INSERT INTO scheduler_operations VALUES(?,?,?)',(op,digest(req),canonical({'id':ident})))
            return self.plan(ident,c)

    def revise(self,actor,op,ident,expected,*,template=None,target=None,rule=None,valid_until=None):
        with self.s.tx() as c:
            p=self.plan(ident,c);request=[actor,ident,expected,template,target]
            if rule is not None or valid_until is not None:request.extend([rule,valid_until])
            old=c.execute('SELECT * FROM scheduler_operations WHERE id=?',(op,)).fetchone()
            if old:
                if old['request_hash']!=digest(request):raise BoundaryError('OPERATION_ID_REUSED')
                return p
            if actor!=p['owner'] or p['version']!=expected or p['state']=='cancelled':raise BoundaryError('PLAN_VERSION_CHANGED')
            if p['next_at']>=p['valid_until'] or p['valid_until']<=self.s.clock():raise BoundaryError('PLAN_HAS_NO_FUTURE_OCCURRENCE')
            if template is not None:
                if not template or len(template)>4000:raise BoundaryError('PLAN_BODY_INVALID')
                allowed=set(json.loads(p['allowed_variables']))
                if any(field not in allowed or fmt or conv for _,field,fmt,conv in string.Formatter().parse(template) if field is not None):raise BoundaryError('TEMPLATE_VARIABLE_NOT_ALLOWED')
            if target is not None and (target.get('kind')!='group' or not target.get('payload')):raise BoundaryError('EXACT_TARGET_REQUIRED')
            next_at=p['next_at'];until=p['valid_until'] if valid_until is None else valid_until
            if rule is not None:
                if p['kind']=='once':
                    next_at=rule.get('at')
                    if not isinstance(next_at,(int,float)) or next_at<=self.s.clock():raise BoundaryError('FUTURE_SEND_TIME_REQUIRED')
                else:
                    if rule.get('timezone')!='Asia/Shanghai' or not re.fullmatch(r'(?:[01]\d|2[0-3]):[0-5]\d',rule.get('time','')):raise BoundaryError('SCHEDULE_TIME_REQUIRED')
                    next_at=self.next_time(p['kind'],rule,self.s.clock()-1)
            if not next_at<until<=self.s.clock()+366*86400:raise BoundaryError('PLAN_VALIDITY_REQUIRED')
            changed=dict(p,template=template if template is not None else p['template'],target=canonical(target) if target is not None else p['target'],rule=canonical(rule) if rule is not None else p['rule'],valid_until=until)
            fingerprint=digest({k:changed[k] for k in ('item_id','kind','rule','target','template','allowed_variables','policy','valid_until','window_seconds')})
            from am_harness.reservation import reserve_effect
            reserve_effect(c,op,'schedule_revision',{'expected':expected,'request':request,'hash':fingerprint})
            c.execute("UPDATE reminder_plans SET version=version+1,template=?,target=?,rule=?,next_at=?,valid_until=?,hash=?,state='draft',approval_id=NULL WHERE id=?",(changed['template'],changed['target'],changed['rule'],next_at,until,fingerprint,ident))
            c.execute("UPDATE reminder_occurrences SET cancel_requested=1,state=CASE WHEN state IN ('ready','awaiting_confirmation','confirmed_not_sent') THEN 'cancelled' ELSE state END WHERE plan_id=?",(ident,))
            c.execute('INSERT INTO scheduler_operations VALUES(?,?,?)',(op,digest(request),canonical({'id':ident})))
            return self.plan(ident,c)

    def render(self,p,item):
        values={key:(item.get(key) or '未记录') for key in json.loads(p['allowed_variables'])} if item else {}
        text=p['template'].format(**values)
        target=json.loads(p['target'])
        return {'target':target,'body':text,'item_id':p['item_id'],'item_version':item['version'] if item else None}

    def scan(self):
        if not self.enabled():return False
        changed=False;now=self.s.clock()
        self.s.execute("UPDATE reminder_plans SET state='expired' WHERE valid_until<=? AND state IN ('active','draft')",(now,))
        for original in self.s.rows("SELECT * FROM reminder_plans WHERE state='active' AND next_at<=? AND valid_until>?",(now,now)):
            with self.s.tx() as c:
                p=self.plan(original['id'],c)
                if p['state']!='active' or p['next_at']>now:continue
                if p['kind']=='briefing' and p['error']=='BRIEFING_SOURCE_UNAVAILABLE':continue
                item=self.items.get(p['item_id'],c) if p['item_id'] else None
                if item and (item['paused'] or item['state'] in ('closed','cancelled')):continue
                if c.execute("SELECT 1 FROM reminder_occurrences WHERE (item_id=? OR plan_id=?) AND (state IN ('sending','unknown') OR state='sent' AND COALESCE(readback_state,'pending')!='verified')",(p['item_id'],p['id'])).fetchone():continue
                scheduled=p['next_at'];rule=json.loads(p['rule'])
                try:
                    # Skip missed windows without producing a backlog. Only one
                    # current-window instance can survive a restart.
                    while scheduled+p['window_seconds']<=now and p['kind']!='once':
                        nxt=self.next_time(p['kind'],rule,scheduled)
                        if nxt>now:break
                        scheduled=nxt
                    next_at=self.next_time(p['kind'],rule,scheduled)
                except BoundaryError as exc:
                    c.execute('UPDATE reminder_plans SET error=? WHERE id=?',(str(exc),p['id']));continue
                if scheduled+p['window_seconds']<=now:
                    c.execute("UPDATE reminder_plans SET next_at=?,state=CASE WHEN kind='once' THEN 'expired' ELSE state END,error='MISSED_WINDOW_SKIPPED' WHERE id=?",(next_at,p['id']));continue
                payload=self.render(p,item)
                event_ids=[]
                if p['kind']=='briefing':
                    if self.briefing is None:continue
                    self.sync_deliveries(c)
                    # Callback only reads AM snapshots; external source refresh is
                    # completed outside this transaction by the normal worker.
                    body,event_ids=self.briefing(p)
                    if isinstance(body,dict):payload.update(body)
                    else:payload['body']=body
                    payload['event_ids']=event_ids
                ident=uuid.uuid5(uuid.NAMESPACE_URL,p['id']+'/'+str(p['version'])+'/'+str(scheduled)).hex
                payload=self.port.prepare(payload,ident) if hasattr(self.port,'prepare') else payload
                state='awaiting_confirmation' if p['policy']=='preview_each' else 'ready'
                approval=p['approval_id'] if state=='ready' else None
                n=c.execute("INSERT OR IGNORE INTO reminder_occurrences(id,plan_id,plan_version,scheduled_at,item_id,item_version,approval_id,payload,hash,request_key,deadline,state,attempts,next_attempt) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,0,0)",
                    (ident,p['id'],p['version'],scheduled,p['item_id'],item['version'] if item else None,approval,canonical(payload),digest(payload),ident,min(scheduled+p['window_seconds'],p['valid_until']),state)).rowcount
                c.execute('UPDATE reminder_plans SET next_at=?,error=NULL WHERE id=?',(next_at,p['id']))
                if n and p['kind']=='briefing':
                    sub=c.execute('SELECT id FROM am_subscriptions WHERE plan_id=?',(p['id'],)).fetchone()
                    if sub:
                        for event in event_ids:
                            c.execute("INSERT INTO am_briefing_deliveries VALUES(?,?,?,'pending') ON CONFLICT(subscription_id,event_id) DO UPDATE SET occurrence_id=excluded.occurrence_id,state='pending' WHERE am_briefing_deliveries.state='not_sent'",(sub[0],event,ident))
                            c.execute("INSERT OR IGNORE INTO am_briefing_delivery_attempts VALUES(?,?,?,'pending')",(sub[0],event,ident))
                changed=bool(n) or changed
            if n and state=='awaiting_confirmation':self.notify('occurrence_preview',ident)
        return changed

    def approve_occurrence(self,actor,ident,expected_hash):
        with self.s.tx() as c:
            o=self.occurrence(ident,c);p=self.plan(o['plan_id'],c)
            if actor!=p['owner'] or o['hash']!=expected_hash or o['deadline']<=self.s.clock():raise BoundaryError('OCCURRENCE_APPROVAL_INVALID')
            if o['state']=='ready' and o['approval_id']:return o
            if o['state']!='awaiting_confirmation' or not self.current(c,o,p):raise BoundaryError('OCCURRENCE_SUPERSEDED')
            ident_a=uuid.uuid4().hex
            c.execute('INSERT INTO action_approvals VALUES(?,?,?,?,?,?,?,?,?)',(ident_a,'occurrence',ident,o['plan_version'],actor,o['payload'],o['hash'],o['deadline'],self.s.clock()))
            c.execute("UPDATE reminder_occurrences SET state='ready',approval_id=? WHERE id=?",(ident_a,ident))
            return self.occurrence(ident,c)

    def current(self,c,o,p):
        if p['state']!='active' or p['version']!=o['plan_version'] or o['deadline']<=self.s.clock() or o['cancel_requested']:return False
        if o['item_id']:
            item=self.items.get(o['item_id'],c)
            if item['version']!=o['item_version'] or item['paused'] or item['state'] in ('closed','cancelled'):return False
        if digest(json.loads(o['payload']))!=o['hash']:return False
        return True

    def approval_valid(self,c,o,p):
        a=c.execute('SELECT * FROM action_approvals WHERE id=?',(o['approval_id'],)).fetchone()
        if not a or a['actor']!=p['owner'] or a['expires']<=self.s.clock():return False
        if digest(json.loads(a['snapshot']))!=a['hash']:return False
        if a['kind']=='occurrence':return a['object_id']==o['id'] and a['hash']==o['hash']
        snapshot=json.loads(a['snapshot'])
        return a['kind']=='plan' and a['object_id']==p['id'] and a['version']==p['version'] and p['policy'] in ('fixed_rule','owner_subscription') and all(snapshot.get(k)==p[k] for k in ('target','template','rule','policy','window_seconds','valid_until','hash'))

    def dispatch(self):
        if not self.enabled():return False
        rows=self.s.rows("SELECT * FROM reminder_occurrences WHERE state IN ('ready','confirmed_not_sent') AND next_attempt<=? ORDER BY scheduled_at,rowid",(self.s.clock(),))
        try:
            for o in rows:
                if self.dispatch_one(o):return True
            return False
        finally:
            with self.s.tx() as c:self.sync_deliveries(c)

    def dispatch_one(self,o):
        if self.followup is not None:
            handled=self.followup.dispatch_occurrence(o['id'])
            if handled is not None:return handled
        with self.s.tx() as c:
            o=self.occurrence(o['id'],c);p=self.plan(o['plan_id'],c)
            if o['state'] not in ('ready','confirmed_not_sent'):return False
            if self.followup is not None and not self.followup.legacy_allowed(c,o):
                c.execute("UPDATE reminder_occurrences SET state='cancelled',error='SHARED_FOLLOWUP_ADMISSION_REQUIRED' WHERE id=?",(o['id'],))
                return True
            if not self.current(c,o,p) or not self.approval_valid(c,o,p) or o['attempts']>=3:
                c.execute("UPDATE reminder_occurrences SET state='cancelled',error='CURRENT_STATE_OR_APPROVAL_CHANGED' WHERE id=? AND state IN ('ready','confirmed_not_sent')",(o['id'],));return True
            if self.blocked(c,o):return False
        payload=json.loads(o['payload'])
        try:self.port.refresh(payload)
        except Exception:
            self.s.execute("UPDATE reminder_occurrences SET state='cancelled',error='PREFLIGHT_UNVERIFIED' WHERE id=? AND state IN ('ready','confirmed_not_sent')",(o['id'],));return True
        with self.s.tx() as c:
            fresh=self.occurrence(o['id'],c);p=self.plan(o['plan_id'],c)
            if fresh['state'] not in ('ready','confirmed_not_sent'):return False
            if not self.current(c,fresh,p) or not self.approval_valid(c,fresh,p):
                c.execute("UPDATE reminder_occurrences SET state='cancelled',error='PREFLIGHT_CHANGED' WHERE id=? AND state IN ('ready','confirmed_not_sent')",(o['id'],));return True
            if self.blocked(c,fresh):return False
            n=c.execute("UPDATE reminder_occurrences SET state='sending',attempts=attempts+1,send_started=? WHERE id=? AND state IN ('ready','confirmed_not_sent')",(self.s.clock(),o['id'])).rowcount
            if not n:return False
        def guard():
            if not self.enabled():return False
            with self.s.tx() as c:
                current=self.occurrence(o['id'],c);p=self.plan(o['plan_id'],c)
                if self.followup is not None and not self.followup.legacy_allowed(c,current):return False
                return current['state']=='sending' and self.current(c,current,p) and self.approval_valid(c,current,p) and not self.blocked(c,current)
        try:
            receipt=self.port.send(payload,o['request_key'],guard)
            if not receipt.get('message_id'):raise SendUnknown('RECEIPT_MISSING')
            self.s.execute("UPDATE reminder_occurrences SET state='sent',message_id=?,receipt=?,readback_state='pending' WHERE id=? AND state='sending'",(receipt['message_id'],canonical(receipt),o['id']))
            self.reconcile(o['id'])
        except SendRejected as exc:
            retry=getattr(exc,'retry_after',min(60,2**(o['attempts']+1)))
            self.s.execute("UPDATE reminder_occurrences SET state='confirmed_not_sent',next_attempt=?,error=? WHERE id=? AND state='sending'",(self.s.clock()+retry,str(exc),o['id']))
        except Exception:
            self.s.execute("UPDATE reminder_occurrences SET state='unknown',readback_state='pending',error='SEND_UNKNOWN' WHERE id=? AND state='sending'",(o['id'],))
        self.notify('occurrence_result',o['id']);return True

    def reconcile(self,ident,*,manual=False):
        o=self.occurrence(ident)
        if o['state'] not in ('unknown','sent'):return False
        if o['readback_state']=='verified':
            # Native verification may have committed before the follow-up
            # result. Retry that local convergence, never a network read/send.
            try:return bool(self.followup and self.followup.reconciled(ident))
            except Exception:return False
        if o['reconcile_attempts']>=6:return False
        if not manual and o['next_reconcile']>self.s.clock():return False
        self.s.execute('UPDATE reminder_occurrences SET reconcile_attempts=reconcile_attempts+1,next_reconcile=? WHERE id=?',(self.s.clock()+min(3600,30*2**o['reconcile_attempts']),ident))
        try:
            receipt=self.port.reconcile(json.loads(o['payload']),o['request_key'],o['send_started'],o['message_id'])
            if not receipt or not receipt.get('verified'):
                if o['reconcile_attempts']>=5:self.s.execute("UPDATE reminder_occurrences SET readback_state='needs_review' WHERE id=?",(ident,))
                return False
            with self.s.tx() as c:
                c.execute("UPDATE reminder_occurrences SET state='sent',message_id=?,receipt=?,readback_state='verified',error=NULL WHERE id=? AND state IN ('sent','unknown')",(receipt['message_id'],canonical(receipt),ident))
                c.execute("UPDATE am_briefing_deliveries SET state='verified' WHERE occurrence_id=?",(ident,))
                c.execute("UPDATE am_briefing_delivery_attempts SET state='verified' WHERE occurrence_id=?",(ident,))
        except Exception:
            if o['reconcile_attempts']>=5:self.s.execute("UPDATE reminder_occurrences SET readback_state='needs_review' WHERE id=?",(ident,))
            return False
        if self.followup is not None:
            try:self.followup.reconciled(ident)
            except Exception:return False  # keep the native verified checkpoint
        return True

    def step(self):
        # Reconciliation continues even when new execution is disabled.
        recovered=bool(self.followup and self.followup.recover_adoptions())
        for o in self.s.rows("SELECT id FROM reminder_occurrences WHERE state IN ('unknown','sent') AND (readback_state IS NULL OR readback_state!='verified') AND reconcile_attempts<6 AND next_reconcile<=? LIMIT 1",(self.s.clock(),)):
            self.reconcile(o['id'])
        scanned=self.scan()
        return self.dispatch() or scanned or recovered
