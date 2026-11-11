#!/usr/bin/env python3
"""Synthetic local candidate check; no credentials, network, model calls or sends."""
import argparse
from dataclasses import replace
import json
from pathlib import Path
import statistics
import sys
import tempfile
import time

sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'src'))
from am_gateway.models import Event
from am_gateway.phase1 import Harness,HarnessConfig,Policy
from am_gateway.store import Store


def run():
    with tempfile.TemporaryDirectory(prefix='phase1-local-check-') as directory:
        h=Harness(Store(Path(directory)/'gateway.sqlite'),HarnessConfig('synthetic','bot','owner','owner-dm'))
        policy=Policy('owner','synthetic-test','owner-dm',('owner-dm',),('A',),allow_supplement=True)
        supplement=Event('synthetic','supplement','owner-dm','owner','补充：客户希望周四交付，正式计划尚未调整',
                         time.time(),channel='bot_dm',project_ids=('A',),version_time=time.time())
        h.ingest(supplement,policy)
        recorded=h.route_one()
        snapshot=h.information.snapshot(policy,minimum_visible_version=json.loads(recorded['result_json'])['minimum_visible_version'])
        latencies=[]
        for n in range(30):
            e=Event('synthetic','query-'+str(n),'owner-dm','owner','A 项目目前卡在哪里？',
                    time.time(),channel='bot_dm',project_ids=('A',))
            start=time.perf_counter()
            h.ingest(e,policy)
            task=h.route_one()
            latencies.append(time.perf_counter()-start)
            assert task['intent']=='project.query' and task['route_state']=='capability_unavailable'
        h.ingest(replace(supplement,is_deleted=True,version_time=time.time()))
        h.route_one()
        after=h.information.snapshot(policy)
        return {'level':'synthetic local only','external_calls':0,'new_feishu_consumers':0,'real_sends':0,
                'grok_bot_connected':False,'supplement_visible_before_projection':len(snapshot['evidence_items'])==1,
                'withdrawn_source_hidden':not after['evidence_items'],
                'rule_path_samples':len(latencies),'terra_calls':0,
                'local_ingest_and_route_seconds':{'samples':latencies,'median':statistics.median(latencies),
                    'max':max(latencies),'observed_p95':sorted(latencies)[28]},
                'limits':'Only local SQLite admission and rule routing to unavailable. No ACK, Grok execution, '
                         'Feishu delivery, paired baseline, model accuracy or stable P95 claim.'}


if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--output',required=True);args=parser.parse_args()
    result=run();Path(args.output).write_text(json.dumps(result,ensure_ascii=False,indent=2)+'\n')
    print(json.dumps({k:v for k,v in result.items() if k!='local_ingest_and_route_seconds'},ensure_ascii=False,indent=2))
