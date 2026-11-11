"""Behavior-level tests for actual SQLite + core + existing-host ports."""
from dataclasses import replace
import importlib.util
import json
from pathlib import Path
import threading
import time
import urllib.request

import pytest

from am_gateway.core import Gateway
from am_gateway.http_server import WorkerServer
from am_gateway.metrics import report
from am_gateway.models import Config, ContractError, Event, SendReceipt
from am_gateway.scheduler import Scheduler
from am_gateway.store import Store


@pytest.fixture
def gw(tmp_path):
    return Gateway(Store(tmp_path/"gateway.sqlite"),Config("tenant","bot","owner","owner-dm",1,
        batch_wait_seconds=0,project_chats={"p":("group",)}))


def event(mid="m1",**kw):
    return replace(Event("tenant",mid,"group","user","当前进展？",time.time(),mentions=("bot",),
                         version_time=time.time()),**kw)


def start(gw,e,worker="grok_bot"):
    gw.ingest(e)
    gw.route()
    job=gw.store.claim_job({worker},gw.config.job_lease_seconds)
    assert job
    req=gw.request(job)
    return job,req


def finish(gw,job,**kw):
    return gw.complete(job["job_id"],job["attempt_id"],{"schema":"am.gateway.result.v2",
       "job_id":job["job_id"],"attempt_id":job["attempt_id"],"status":"completed",**kw})


class Sender:
    def __init__(self,status="sent"):
        self.actions=[]
        self.status=status
    def send(self,action):
        self.actions.append(action)
        return SendReceipt(self.status,"platform-"+action["action_id"] if self.status=="sent" else "")


def test_one_item_two_jobs_no_background_reply(gw):
    e=event(project_ids=("p",))
    job,_=start(gw,e)
    finish(gw,job,decision_mode="direct_reply",reply_text="收到这条进展，我按当前记录回答。")
    gw.ingest(replace(e,observed_via="poll"))
    gw.route()
    background=gw.store.claim_job({"grok_cli"},120)
    gw.request(background)
    finish(gw,background,display_text="不应外发的后台摘要",fact_updates=[
       {"project_id":"p","field":"progress","value":"验证中","source_item_ids":[e.item_id]}])
    assert len(gw.store.rows("SELECT * FROM gw_items WHERE is_self=0"))==1
    assert len(gw.store.rows("SELECT * FROM gw_jobs"))==2
    assert len(gw.store.rows("SELECT * FROM gw_outbox"))==1
    assert len(gw.store.rows("SELECT * FROM gw_facts"))==1


@pytest.mark.parametrize("channel,mentions,expected",[
    ("bot_dm",(),True),("personal_dm",(),False),("group",(),False),
    ("group",("another_bot",),False),("group",("bot",),True)])
def test_metadata_routes_only_true_dialogue(gw,channel,mentions,expected):
    e=event(channel=channel,mentions=mentions,body="文本提到 @AM")
    admission=gw.ingest(e)
    gw.route()
    assert admission.handled is expected
    assert bool(gw.store.rows("SELECT * FROM gw_jobs WHERE worker='grok_bot'")) is expected


def test_history_collected_without_replaying_replies(gw):
    e=event(original_time=.5,project_ids=("p",))
    gw.ingest(e); gw.route()
    assert not gw.store.rows("SELECT * FROM gw_jobs WHERE worker='grok_bot'")
    assert gw.store.rows("SELECT * FROM gw_jobs WHERE worker='grok_cli'")


def test_private_derived_project_fact_never_reaches_group(gw):
    e=event("private",channel="personal_dm",chat_id="private-chat",mentions=(),project_ids=("p",),body="内部交期")
    job,_=start(gw,e,"grok_cli")
    finish(gw,job,fact_updates=[{"project_id":"p","field":"delivery","value":"保密日期","source_item_ids":[e.item_id]}])
    group,req=start(gw,event("public-query",project_ids=("p",)))
    assert not req["context"]["project_snapshot"]
    finish(gw,group,reply_text="当前没有可在此群确认的记录。")
    owner,req=start(gw,event("owner-query",channel="bot_dm",chat_id="owner-dm",sender_id="owner",mentions=()))
    assert req["context"]["project_snapshot"][0]["value"]=="保密日期"


def test_group_actor_and_thread_isolation(gw):
    job,_=start(gw,event(sender_id="a",thread_id="thread-a",body="A的会话"))
    finish(gw,job,reply_text="A的回答")
    gw.outbox.send_one(Sender())
    other,req=start(gw,event("m2",sender_id="b",thread_id="thread-b"))
    assert not req["context"]["recent_messages"]


@pytest.mark.parametrize("status",["failed","unknown"])
def test_unsent_result_never_enters_memory(gw,status):
    job,_=start(gw,event())
    finish(gw,job,reply_text="尚未真正送达")
    gw.outbox.send_one(Sender(status))
    nxt,req=start(gw,event("m2"))
    assert all(m["role"]!="assistant" for m in req["context"]["recent_messages"])


def test_preview_edit_owner_confirm_exactly_once(gw):
    job,_=start(gw,event())
    finish(gw,job,decision_mode="require_approval",reply_text="原始草稿")
    action=gw.store.one("SELECT * FROM gw_outbox WHERE purpose='final'")
    with pytest.raises(ContractError):
        gw.outbox.review(action["action_id"],actor_id="stranger",draft_version=1,decision="approve")
    edited=gw.outbox.review(action["action_id"],actor_id="owner",draft_version=1,decision="edit",edited_text="示例负责人修改后的全文")
    with pytest.raises(ContractError):
        gw.outbox.review(action["action_id"],actor_id="owner",draft_version=1,decision="approve")
    threads=[threading.Thread(target=lambda:gw.outbox.review(action["action_id"],actor_id="owner",draft_version=edited["draft_version"],decision="approve")) for _ in range(2)]
    for t in threads:t.start()
    for t in threads:t.join()
    sender=Sender()
    while gw.outbox.send_one(sender): pass
    final=[a for a in sender.actions if a["purpose"]=="final"]
    assert len(final)==1 and final[0]["body"]=="示例负责人修改后的全文"
    rows=gw.store.rows("SELECT event_json FROM gw_items WHERE is_self=1")
    assert [json.loads(r["event_json"])["body"] for r in rows]==["示例负责人修改后的全文"]


def test_bot_fact_writes_rejected(gw):
    job,_=start(gw,event())
    with pytest.raises(ContractError):
        finish(gw,job,reply_text="答复",fact_updates=[])
    assert not gw.store.rows("SELECT * FROM gw_facts")


def test_child_cannot_shadow_checked_display_or_write_facts(gw):
    job,_=start(gw,event())
    finish(gw,job,decision_mode="submit_task",task_requests=[{"task_kind":"code_task","instruction":"创建文件"}])
    child=gw.store.claim_job({"codex_cli"},120); gw.request(child)
    with pytest.raises(ContractError):
        finish(gw,child,reply_text="检查用安全字段",display_text="我们保证明天交付")
    with pytest.raises(ContractError):
        finish(gw,child,display_text="完成",fact_updates=[{"project_id":"p","field":"x","value":"y","source_item_ids":[]}])
    finish(gw,child,display_text="已完成本地变更说明。")
    assert len(gw.store.rows("SELECT * FROM gw_outbox WHERE purpose='final'"))==1


def test_multi_child_partial_failure_one_final(gw):
    job,_=start(gw,event())
    finish(gw,job,decision_mode="submit_task",task_requests=[
       {"task_kind":"project_analysis","instruction":"分析"},{"task_kind":"build_document","instruction":"制表"}])
    c1=gw.store.claim_job({"grok_cli"},120); gw.request(c1)
    c2=gw.store.claim_job({"codex_cli"},120); gw.request(c2)
    finish(gw,c1,display_text="分析已完成。")
    gw.fail(c2["job_id"],c2["attempt_id"],"template_not_found")
    final=gw.store.rows("SELECT * FROM gw_outbox WHERE purpose='final'")
    assert len(final)==1 and "分析已完成" in final[0]["body"] and "未完成" in final[0]["body"]


def test_unknown_send_is_reconciled_without_resend(gw):
    job,_=start(gw,event()); finish(gw,job,reply_text="已确认口径")
    sender=Sender("unknown")
    gw.outbox.send_one(sender)
    assert gw.outbox.send_one(sender) is None
    sender.lookup=lambda action:SendReceipt("sent","recovered-platform-id")
    gw.outbox.reconcile_unknown(sender)
    assert len(sender.actions)==1
    assert gw.store.one("SELECT state FROM gw_outbox WHERE purpose='final'")["state"]=="sent"
    assert gw.store.one("SELECT native_id FROM gw_items WHERE is_self=1")["native_id"]=="recovered-platform-id"


def test_background_busy_does_not_take_dialogue_slot(gw):
    entered=threading.Event(); release=threading.Event(); bot_ran=threading.Event()
    class Background:
        def run(self,req):
            entered.set()
            assert release.wait(3)
            return {"schema":"am.gateway.result.v2","job_id":req["job_id"],"attempt_id":req["attempt_id"],"status":"completed","fact_updates":[]}
    class Bot:
        def run(self,req):
            bot_ran.set()
            return {"schema":"am.gateway.result.v2","job_id":req["job_id"],"attempt_id":req["attempt_id"],"status":"completed","reply_text":"快速回答"}
    gw.adapters={"grok_bot":Bot(),"grok_cli":Background()}
    sender=Sender(); scheduler=Scheduler(gw,sender).start()
    try:
        gw.ingest(event("background",mentions=(),project_ids=("p",)))
        assert entered.wait(2)
        gw.ingest(event("dialogue"))
        assert bot_ran.wait(2), "dialogue was blocked by the background execution pool"
    finally:
        release.set()
        assert scheduler.stop(3)["stopped"]


def test_worker_http_scoping_and_duplicate_callback(gw):
    gw.ingest(event()); gw.route()
    token="test-worker-token-1234567890123456"
    server=WorkerServer(gw,{"grok_bot":token}).start()
    def post(path,body):
        req=urllib.request.Request(server.url+path,data=json.dumps(body).encode(),
                                   headers={"Authorization":"Bearer "+token,"Content-Type":"application/json"})
        return json.load(urllib.request.urlopen(req,timeout=2))
    try:
        task=post("/v1/jobs/claim",{})["job"]
        body={"schema":"am.gateway.result.v2","job_id":task["job_id"],"attempt_id":task["attempt_id"],
              "status":"completed","reply_text":"HTTP结果"}
        post("/v1/jobs/"+task["job_id"]+"/result",body)
        post("/v1/jobs/"+task["job_id"]+"/result",body)
        assert len(gw.store.rows("SELECT * FROM gw_outbox WHERE purpose='final'"))==1
    finally:server.close()


def test_metrics_keep_failure_denominator_and_separate_mobile(gw):
    good,_=start(gw,event()); finish(gw,good,reply_text="结果")
    gw.outbox.send_one(Sender())
    bad,_=start(gw,event("fail")); gw.fail(bad["job_id"],bad["attempt_id"],"unavailable")
    gw.outbox.send_one(Sender())
    summary=report(gw.store)["summary"]["ordinary"]
    assert summary["total"]==2 and summary["valid_api_results"]==1
    assert summary["mobile_evidence_count"]==0 and summary["target_met"] is False
