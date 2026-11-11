#!/usr/bin/env python3
"""Bounded real reads. No sends, consumers, business writes or model calls."""
from pathlib import Path
import argparse
import json
import sqlite3
import sys
import time

PACKAGE = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(PACKAGE), str(PACKAGE/'src')]

from fs_agent.config import Config as ExistingConfig
from fs_agent.feishu import Feishu
from am_gateway.collectors import FeishuMessageAdapter, SourceSpec, ScanRequest
from am_gateway.discovery import DiscoverySpec
from am_gateway.core import Gateway
from am_gateway.models import Config
from am_gateway.store import Store
from integration.studio_host import ScopedReadTransport, ApprovedGroupDiscovery


def run(host_file, output):
    host = json.loads(Path(host_file).read_text())
    cfg = ExistingConfig.load(Path(host['poc_config']))
    with sqlite3.connect(cfg.db.as_uri()+'?mode=ro', uri=True) as db:
        row = db.execute("SELECT chat FROM am_inbox WHERE user=? AND chat_type='p2p' AND chat LIKE 'oc_%' ORDER BY created DESC LIMIT 1", (cfg.owner,)).fetchone()
    if not row:
        raise RuntimeError('owner_bot_dm_unverified')
    owner_chat, chats = row[0], list(host['group_ledger']['disclosure'])
    client = Feishu(cfg).client
    read = ScopedReadTransport(client, [owner_chat, *chats], app_id=cfg.app_id,
                               bot_id=cfg.bot_open_id, tenant_id=cfg.tenant_key)
    report = {'real_feishu_read_calls':0, 'real_sends':0, 'new_consumers':0, 'results':[]}
    for label, chat, channel in [('owner_am_dm', owner_chat, 'bot_dm'), ('approved_group', chats[0], 'group')]:
        started = time.monotonic()
        try:
            raw = read(FeishuMessageAdapter.url, {'container_id_type':'chat', 'container_id':chat,
                'sort_type':'ByCreateTimeDesc', 'page_size':2}, 'am_bot')
            report['real_feishu_read_calls'] += 1
            page = FeishuMessageAdapter(lambda *_:raw, bot_id=cfg.bot_open_id).read_page(
                SourceSpec(cfg.tenant_key,chat,channel=channel,identity='am_bot'), ScanRequest('live',0,time.time()))
            report['results'].append({'scope':label,'status':'read_and_normalized','events':len(page.events),
                'has_more':page.has_more,'self_messages':sum(e.is_self for e in page.events),
                'elapsed_seconds':round(time.monotonic()-started,3)})
        except Exception as exc:
            report['results'].append({'scope':label,'status':'failed','error_type':type(exc).__name__})
    try:
        discovery = ApprovedGroupDiscovery(read,chats,{})
        spec = DiscoverySpec(cfg.tenant_key,'probe','approved_groups',identity='am_bot',scope='approved_joined_groups')
        page = discovery.discover_page(spec,'')
        report['real_feishu_read_calls'] += 1
        report['directory']={'scope':'approved_joined_groups','returned_approved_sources':len(page.sources),
                             'has_more':page.has_more,'complete_for_scope':not page.has_more and page.complete_for_scope}
    except Exception as exc:
        report['directory']={'status':'failed','error_type':type(exc).__name__}
    report['self_sender_mappings'] = read.normalized_self_messages
    report['coverage_limit']='At most two latest messages per selected chat; directory first page. No full history claim.'
    Path(output).write_text(json.dumps(report,ensure_ascii=False,indent=2)+'\n')
    print(json.dumps(report,ensure_ascii=False,indent=2))


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--host-config',required=True);p.add_argument('--output',required=True)
    args=p.parse_args();run(args.host_config,args.output)
