"""Durable obligation journal; original tools/receipts remain execution authority."""
import json
import re
from .completion import resolve_goals, check_goals, matches_goal
from .adapters import adapt
from .types import encode, fingerprint
from am_conversation.types import ConversationError


class GoalJournal:
    def __init__(self, store, ctx, event, text, catalog, *, reconcile=None):
        self.store,self.ctx,self.event=store,ctx,event
        self.reconcile=reconcile;self.active=None
        now=store._now();self.expires=min(ctx.content_expires_at or now+86400,now+86400)
        with store._connection() as db:
            rows=db.execute("SELECT * FROM conversation_goals_v2 WHERE scope_key=? AND expires>? AND state NOT IN ('fulfilled','cancelled') ORDER BY updated DESC",(ctx.session_id,now)).fetchall()
        continuing=bool(re.match(r'^(?:继续|剩下|接着|刚才|取消剩余)',text))
        if continuing and len(rows)==1:
            self.id=rows[0]['goal_id'];self.data=json.loads(rows[0]['data_json']);self.expires=min(self.expires,rows[0]['expires'])
            if self.data.get('source_access_version') != ctx.source_access_version:
                raise ConversationError('SOURCE_ACCESS_CHANGED')
            if any(g.get('project_id') and not ctx.can_access_project(g['project_id']) for g in self.data['goals']):
                raise ConversationError('SOURCE_ACCESS_CHANGED')
            self.data['continuation_input']=text
            if text.startswith('取消剩余'):
                self.data['cancelled']=True
            # Reconcile confirmed external results through the original store.
            for key,u in list(self.data['units'].items()):
                if u.get('effect')!='external':continue
                action=u.get('result',{}).get('action_id')
                if not action or u['completion']=='fulfilled':continue
                # Full receipt access must retain the original source/scope
                # checks. Metadata deliberately omits the result payload.
                if not ctx.is_owner:raise ConversationError('OWNER_REQUIRED')
                row=store.get_action(ctx,action)
                if (row['tool_name']!=u['operation'] or row['arguments']!=u['target']
                        or row['version']!=u['result'].get('version')):
                    self.data['ambiguous']=True
                    continue
                if row['status'] in ('succeeded','unknown','executing','failed','cancelled','expired'):
                    raw=dict(row['result']) if isinstance(row.get('result'),dict) else {}
                    if row['status']!='succeeded':
                        raw.update(status=row['status'],verified=False,action_id=action,version=row['version'])
                    restored=adapt(u['operation'],u['target'],raw,u['effect'],key=key)
                    restored.update({k:u[k] for k in ('goal_step_id','submission_id','call_fingerprint') if k in u})
                    self.data['units'][key]=restored
                    # Restore both views. Reusing the old draft cache would
                    # downgrade the reconciled unit on an exact repeated call.
                    self.data['calls'][key]={**self.data['calls'].get(key,{}),'state':'unknown' if restored['status']=='unknown' else restored['completion'],
                                             'result':raw,'native_action_id':action}
        else:
            self.id=fingerprint([ctx.scope,event,'goals'])
            goals,complete=resolve_goals(text,catalog)
            self.data={'schema_version':'am-goals/1','source_access_version':ctx.source_access_version,'request':text,'goals':goals,'complete':complete,'units':{},'calls':{},'ambiguous':continuing and len(rows)>1,'cancelled':False}
        # Older journals lack explicit step links. Recover only an unambiguous
        # exact match; do not invent a missing original submission event.
        for key,entry in self.data['calls'].items():
            unit=self.data['units'].get(key,{})
            entry.setdefault('operation',unit.get('operation'))
            entry.setdefault('arguments',unit.get('target',{}))
            if not entry.get('step_id') and entry.get('operation'):
                matches=[g for g in self.data['goals'] if matches_goal(g,{'operation':entry['operation'],'target':entry['arguments']},require_matter=True)]
                if len(matches)==1:entry['step_id']=matches[0]['step_id']
            if entry.get('step_id') and unit:
                unit['goal_step_id']=entry['step_id']
        self.save()

    def save(self):
        progress,state=check_goals(self.data['goals'],self.data['complete'],list(self.data['units'].values()))
        self.data['progress']=progress
        self.data['completion']=state
        if self.data['cancelled']:state='cancelled'
        with self.store._connection(write=True) as db:
            db.execute('INSERT INTO conversation_goals_v2 VALUES(?,?,?,?,?,?) ON CONFLICT(goal_id) DO UPDATE SET data_json=excluded.data_json,state=excluded.state,updated=excluded.updated,expires=MIN(expires,excluded.expires)',
                (self.id,self.ctx.session_id,encode(self.data),state,self.expires,self.store._now()))

    def before(self,name,args,effect):
        if effect=='read':return None
        self.active=None
        if self.data['ambiguous'] or self.data['cancelled']:raise ConversationError('GOAL_SELECTION_REQUIRED')
        key=fingerprint([name,args]);prior=self.data['calls'].get(key)
        probe={'operation':name,'target':args}
        peers=[g for g in self.data['goals'] if matches_goal(g,probe)]
        exact=[g for g in peers if matches_goal(g,probe,require_matter=True)]
        if len(exact)>1:raise ConversationError('GOAL_SELECTION_REQUIRED')
        step=exact[0]['step_id'] if exact else None
        # An unresolved original submission bars a changed description, even
        # when the new wording cannot be proven equivalent by the bounded parser.
        possible={step} if step else {g['step_id'] for g in peers}
        for oldkey,old in self.data['calls'].items():
            oldunit=self.data['units'].get(oldkey,{})
            oldstep=old.get('step_id') or oldunit.get('goal_step_id')
            oldop=old.get('operation') or oldunit.get('operation')
            oldargs=old.get('arguments') or oldunit.get('target',{})
            same_family=oldop==name and oldargs.get('project_id')==args.get('project_id')
            if old['state'] in ('executing','unknown') and (oldstep in possible or not oldstep and same_family):
                raw=self.reconcile(self.ctx,dict(old)) if self.reconcile else None
                if raw is None:raise ConversationError('GOAL_RESULT_RECONCILE_REQUIRED')
                self.active=(key,oldkey,False)
                self.record(oldop,oldargs,raw,effect)
                if self.data['calls'][oldkey]['state']!='fulfilled':raise ConversationError('GOAL_RESULT_RECONCILE_REQUIRED')
                self.active=(key,oldkey,True)
                return self.data['calls'][oldkey]['result']
        if prior:
            if prior['state']=='fulfilled':
                self.active=(key,key,True);return prior['result']
            if prior['state'] in ('executing','unknown'):raise ConversationError('GOAL_RESULT_RECONCILE_REQUIRED')
            if prior['state']=='awaiting_approval' or (prior['state']=='blocked' and prior.get('native_action_id')):
                self.active=(key,key,True);return prior['result']
        # Only one stable goal can own an invocation. A cache replay retains
        # its original arguments, unit ID and submission identity.
        for oldkey,old in self.data['calls'].items():
            if step and old.get('step_id')==step and old['state']=='fulfilled':
                self.active=(key,oldkey,True);return old['result']
        if (peers or self.data['complete']) and not step:raise ConversationError('GOAL_SELECTION_REQUIRED')
        self.data['calls'][key]={'state':'executing','operation':name,'arguments':args,'engine_event_id':self.event,
            'step_id':step,'submission_id':fingerprint([self.ctx.scope,self.id,step,self.event,key]),'effect':effect}
        self.active=(key,key,False)
        self.save();return None

    def record(self,name,args,result,effect):
        key=fingerprint([name,args])
        if effect!='read' and self.active:
            _,key,replayed=self.active
            if replayed:return self.data['units'][key]
        prior=self.data['calls'].get(key,{})
        name=prior.get('operation',name);args=prior.get('arguments',args)
        unit=adapt(name,args,result,effect,key=key)
        if prior.get('step_id'):unit['goal_step_id']=prior['step_id']
        if prior.get('submission_id'):unit['submission_id']=prior['submission_id']
        unit['call_fingerprint']=key
        self.data['units'][key]=unit
        if effect!='read':
            self.data['calls'][key]={**prior,'state':'unknown' if unit['status']=='unknown' else unit['completion'],'result':result}
            if prior.get('native_action_id'):self.data['calls'][key]['native_action_id']=prior['native_action_id']
        self.save();return unit

    def context(self):
        return {'goal_id':self.id,'goals':self.data['progress'],'request':self.data['request'],
                'complete_request_coverage':self.data['complete'],'ambiguous':self.data['ambiguous'],
                'rule':'仅处理未完成目标；已提交步骤不得重放，未知结果先核对。'}

    def fulfilled(self):
        return self.data['completion']=='fulfilled'
