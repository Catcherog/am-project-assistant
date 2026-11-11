"""All outcomes remain in the denominator; ACK is neither start nor completion."""
from collections import Counter
import json
import math
import statistics


def describe(values):
    values = [x for x in values if x is not None]
    if not values:
        return {'n':0,'median':None,'max':None,'p95':None}
    ordered = sorted(values)
    return {'n':len(values),'median':statistics.median(values),'max':max(values),
            'p95':ordered[math.ceil(len(values)*.95)-1] if len(values)>=20 else None}


def sample(task):
    t = json.loads(task['stamps'])
    result = json.loads(task['result_json'] or '{}')
    intervals = {'T1':('t_i','t_m'), 'T2':('t_r','t_i'), 'T3':('t_a','t_r'),
                 'T4':('t_p','t_a'), 'end_to_end':('t_p','t_m'), 'observation':('t_o','t_p'),
                 'full_completion':('full_completed_at','t_m')}
    values = {k:t[a]-t[b] if t.get(a) is not None and t.get(b) is not None else None for k,(a,b) in intervals.items()}
    return {'task_id':task['task_id'],'response_kind':result.get('response_kind','no_answer'),
            'dispatch':task['dispatch_state'],'execution':task['execution_state'],
            'delivery':task['delivery_state'],'verification':task['verification_state'],
            'raw_timestamps':t,'intervals_seconds':values,
            'clock_anomaly':any(v<0 for v in values.values() if v is not None),
            'full_answer':bool(result.get('response_kind')=='sufficient' and not result.get('remaining_requirements')
                               and task['verification_state']=='sufficient' and task['delivery_state']=='delivered')}


def report(store):
    samples = [sample(t) for t in store.rows('SELECT * FROM h1_tasks ORDER BY created')]
    return {'samples':samples,'n':len(samples),'response_kinds':dict(Counter(s['response_kind'] for s in samples)),
            'fully_answered':sum(s['full_answer'] for s in samples),
            'clock_anomalies':sum(s['clock_anomaly'] for s in samples),
            'missing_ACK':sum(s['raw_timestamps'].get('t_a') is None for s in samples),
            'latency':{k:describe([s['intervals_seconds'][k] for s in samples])
                       for k in ('T1','T2','T3','T4','end_to_end','observation','full_completion')},
            'limits':'Platform generation time is not mobile display time. Below 20 observations no P95; '
                     'even larger samples do not establish stable tails. Missing/error/partial outcomes retained.'}


def paired_overhead(pairs):
    """Each pair has identical question/scope/model/temperature/start condition, distinct request IDs."""
    return describe([p['harness_seconds']-p['direct_seconds'] for p in pairs])
