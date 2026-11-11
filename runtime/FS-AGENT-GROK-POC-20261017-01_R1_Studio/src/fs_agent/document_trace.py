"""Bounded timing journal. Never stores prompts, source text or model output."""
import json
import re
import time


class DocumentTrace:
    def __init__(self,store,task,draft,job,deadline):
        self.s,self.task,self.draft,self.job=store,task,draft,job
        self.start=time.monotonic();self.deadline=deadline;self.events=[]
        self.phase_name=None;self.phase_start=self.start;self.error=None
        self.guard_counts={'local':0,'full':0};self.guard_seconds={'local':0.,'full':0.}

    def guard(self,kind,seconds):
        self.guard_counts[kind]+=1;self.guard_seconds[kind]+=seconds

    def phase(self,name):
        now=time.monotonic()
        if self.phase_name is not None:
            self.events.append({'stage':self.phase_name,'seconds':now-self.phase_start,
                'elapsed_seconds':now-self.start,'remaining_seconds':max(0,self.deadline-now)})
        self.phase_name=name;self.phase_start=now

    def call(self,entry):
        self.events.append(dict(entry,event='model_call',elapsed_seconds=time.monotonic()-self.start))

    def finish(self,error=None):
        self.phase(None)
        if error is not None:
            code=str(error);self.error=code if re.fullmatch(r'[A-Z][A-Z0-9_]{0,100}',code) else type(error).__name__
        payload={'run_id':self.task['run_id'],'events':self.events,'error':self.error,
            'guard_counts':self.guard_counts,'guard_seconds':self.guard_seconds,
            'total_seconds':time.monotonic()-self.start,'deadline_monotonic':self.deadline}
        self.s.execute('CREATE TABLE IF NOT EXISTS amdoc_attempt_traces ('
            'run_id TEXT PRIMARY KEY,draft_id TEXT NOT NULL,revision INTEGER NOT NULL,trace TEXT NOT NULL,expires REAL NOT NULL)')
        self.s.execute('INSERT OR REPLACE INTO amdoc_attempt_traces VALUES(?,?,?,?,?)',(
            self.task['run_id'],self.draft['id'],self.job['revision'],json.dumps(payload),min(self.task['expires'],self.draft['expires'])))
