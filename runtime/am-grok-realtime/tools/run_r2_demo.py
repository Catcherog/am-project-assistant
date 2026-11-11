#!/usr/bin/env python3
"""Synthetic E2E through real local HTTP, subprocesses, SQLite and approval edits."""
from __future__ import annotations
from dataclasses import replace
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
import json
from pathlib import Path
import sys
import tempfile
import threading
import time

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/"src"))
from am_gateway.adapters import GrokBotAdapter,SubprocessJsonAdapter
from am_gateway.core import Gateway
from am_gateway.metrics import report
from am_gateway.models import Config,Event,SendReceipt
from am_gateway.store import Store
from integration.am_multi_ai_host import BridgeIngress


WORKER = r"""
import json,sys
from pathlib import Path
r=json.load(sys.stdin)
out={"schema":"am.gateway.result.v2","job_id":r["job_id"],"attempt_id":r["attempt_id"],"status":"completed"}
if r["task_kind"]=="project_state_update":
    src=r["context"]["input_items"][0]
    out["fact_updates"]=[{"project_id":"demo-project","field":"progress","value":"样机测试中",
                        "source_item_ids":[src["item_id"]]}]
elif r["task_kind"]=="build_document":
    p=Path("demo-report.txt").resolve()
    p.write_text("合成演示报告：样机测试中。\n")
    out["display_text"]="报告文件已生成。"
    out["artifacts"]=[{"name":p.name,"uri":str(p)}]
elif r["task_kind"]=="project_analysis":
    out["display_text"]="分析完成：下一步核对测试结果。"
else:
    out["candidates"]=[]
json.dump(out,sys.stdout,ensure_ascii=False)
"""


def main():
    calls=[]
    class Handler(BaseHTTPRequestHandler):
        def log_message(self,*args): pass
        def do_POST(self):
            req=json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            calls.append(req)
            raw=req["context"]["input_items"][0]["body"]
            result={"schema":"am.gateway.result.v2","job_id":req["job_id"],"attempt_id":req["attempt_id"],"status":"completed"}
            if "新承诺" in raw:
                result.update(decision_mode="require_approval",reply_text="拟答复：建议安排下周交付。",
                              approval_meta={"reason":"需要示例负责人确认新交付安排","risk_level":"high"},
                              action_intents=["new_commitment"])
            elif "生成报告" in raw:
                result.update(decision_mode="submit_task",task_requests=[
                    {"task_kind":"project_analysis","instruction":"分析当前情况"},
                    {"task_kind":"build_document","instruction":"生成演示文件"}])
            else:
                result.update(decision_mode="direct_reply",reply_text="已收到，当前消息提到样机测试中。")
            blob=json.dumps(result,ensure_ascii=False).encode()
            self.send_response(200);self.send_header("Content-Type","application/json")
            self.send_header("Content-Length",str(len(blob)));self.end_headers();self.wfile.write(blob)
    server=ThreadingHTTPServer(("127.0.0.1",0),Handler)
    thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
    try:
        with tempfile.TemporaryDirectory(prefix="am-r2-demo-") as tmp:
            root=Path(tmp);script=root/"worker.py";script.write_text(WORKER)
            config=Config("demo-tenant","demo-bot","demo-owner","demo-owner-dm",1,
                batch_wait_seconds=0,project_chats={"demo-project":("demo-group",)})
            local=SubprocessJsonAdapter([sys.executable,str(script)],cwd=str(root))
            def verify_file(artifact):
                p=Path(artifact["uri"]).resolve()
                if not p.is_file() or not p.is_relative_to(root):
                    raise ValueError("unverified artifact")
                return {"name":p.name,"uri":str(p),"bytes":p.stat().st_size}
            gw=Gateway(Store(root/"gateway.sqlite"),config,{
                "grok_bot":GrokBotAdapter(f"http://127.0.0.1:{server.server_port}/dialogue"),
                "grok_cli":local,"codex_cli":local,"qwen":local},artifact_resolver=verify_file)
            sent=[]
            class Sender:
                def send(self,action):
                    sent.append(action)
                    return SendReceipt("sent","demo-output-"+str(len(sent)))
            sender=Sender()
            def send_all():
                while gw.outbox.send_one(sender): pass
            def background():
                for _ in range(50):
                    if not gw.run_one({"grok_cli","codex_cli","qwen"}): break
                send_all()
            def message(mid,body):
                return Event(config.tenant_id,mid,"demo-group","demo-user",body,time.time(),
                    mentions=(config.bot_id,),project_ids=("demo-project",),version_time=time.time())

            first=message("demo-in-1","当前进度：样机测试中，请回答。")
            gw.ingest(first);gw.ingest(replace(first,observed_via="poll"));gw.route()
            gw.run_one({"grok_bot"});send_all()
            before_background=len(sent);background()
            assert len(sent)==before_background==1
            assert len(gw.store.rows("SELECT * FROM gw_items WHERE native_id='demo-in-1'"))==1

            sensitive=message("demo-in-2","请做出新承诺。")
            gw.ingest(sensitive);gw.route();gw.run_one({"grok_bot"});send_all()
            approval=gw.store.one("SELECT * FROM gw_outbox WHERE state='pending_approval'")
            assert approval and "拟答复" not in json.dumps(gw.store.rows("SELECT event_json FROM gw_items WHERE is_self=1"),ensure_ascii=False)
            hook=BridgeIngress(gw,legacy_control=lambda p:False,verified_scope=lambda p:True,
                               projects_for=lambda p:("demo-project",))
            hook.review(config.owner_id,{"gateway":"r2","action_id":approval["action_id"],"draft_version":1,"decision":"edit"})
            edit_payload={"header":{"tenant_key":config.tenant_id},"event":{
                "sender":{"sender_id":{"open_id":config.owner_id}},
                "message":{"message_id":"demo-edit","chat_id":config.owner_chat_id,"chat_type":"p2p",
                           "create_time":str(int(time.time()*1000)),"content":json.dumps({"text":"最终口径：具体交期确认后另行回复。"},ensure_ascii=False)}}}
            hook.ingest(edit_payload);gw.route()
            amended=gw.store.one("SELECT * FROM gw_outbox WHERE action_id=?",(approval["action_id"],))
            assert amended["draft_version"]==2
            hook.review(config.owner_id,{"gateway":"r2","action_id":approval["action_id"],"draft_version":2,"decision":"approve"})
            send_all();background()
            group_finals=[a for a in sent if a["purpose"]=="final"]
            assert len(group_finals)==2 and group_finals[-1]["body"]=="最终口径：具体交期确认后另行回复。"
            assert not gw.store.rows("SELECT * FROM gw_jobs WHERE input_refs LIKE '%"+Event.from_dict(json.loads(gw.store.one("SELECT event_json FROM gw_items WHERE native_id='demo-edit'")["event_json"])).item_id+"%'")

            request=message("demo-in-3","生成报告并分析。")
            gw.ingest(request);gw.route();gw.run_one({"grok_bot"});send_all();background()
            final=gw.store.one("SELECT * FROM gw_outbox WHERE job_id LIKE 'bundle_%' AND purpose='final'")
            assert final and final["state"]=="sent"
            assert (root/"demo-report.txt").is_file()
            artifacts=json.loads(final["payload_json"])["artifacts"]
            assert len(artifacts)==1
            summary={"status":"PASS","real_local_http":True,"actual_bot_http_requests":len(calls),
                "real_local_subprocesses":True,"real_feishu_calls":0,"real_model_calls":0,
                "single_message_dual_route":True,"internal_update_silent":True,
                "owner_edit_confirm_original_target":True,"multi_child_single_final":True,
                "local_file_verified":True,"formal_final_count":len([a for a in sent if a["purpose"]=="final"]),
                "metrics_are_synthetic":True,"metrics":report(gw.store)}
            print(json.dumps(summary,ensure_ascii=False,indent=2))
    finally:
        server.shutdown();server.server_close();thread.join(2)


if __name__=="__main__":main()
