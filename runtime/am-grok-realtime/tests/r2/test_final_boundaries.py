"""Cross-module regressions for durable edits, provenance and worker progress."""
from dataclasses import replace
import json
import time

import pytest

from am_gateway.core import Gateway
from am_gateway.models import Config, ContractError, Event
from am_gateway.store import Store
from integration.am_multi_ai_host import BridgeIngress


def setup(tmp_path, **kw):
    return Gateway(Store(tmp_path/"gateway.db"),Config("t","bot","owner","owner-dm",1,
        batch_wait_seconds=0,project_chats={"p":("g",)},**kw))


def run(gw,event,worker):
    gw.ingest(event);gw.route()
    job=gw.claim({worker})
    assert job
    return job,gw.request(job)


def finish(gw,job,**fields):
    return gw.complete(job["job_id"],job["attempt_id"],{
        "schema":"am.gateway.result.v2","job_id":job["job_id"],"attempt_id":job["attempt_id"],
        "status":"completed",**fields})


def test_resource_conflict_checks_candidate_scope_independently(tmp_path):
    gw=setup(tmp_path,shared_resource_chats={"public-doc":("g",)})
    public=Event("t","public-doc","","owner","公开材料",100,channel="resource",kind="doc",
                 project_ids=("p",),version_time=100)
    private=replace(public,native_id="private-doc",body="内部材料",original_time=101,version_time=101)
    for event,value in ((public,"公开交期"),(private,"绝不能出现在群里的候选")):
        job,_=run(gw,event,"grok_cli")
        finish(gw,job,fact_updates=[{"project_id":"p","field":"delivery","value":value,
                                    "source_item_ids":[event.item_id]}])
    group=Event("t","query","g","u","查进度",time.time(),mentions=("bot",),project_ids=("p",))
    job,request=run(gw,group,"grok_bot")
    facts=request["context"]["project_snapshot"]
    assert facts[0]["value"]=="公开交期"
    assert facts[0]["candidate"] is None and facts[0]["candidate_withheld"]
    assert "绝不能" not in json.dumps(request,ensure_ascii=False)
    with pytest.raises(ContractError):
        gw.read_input(job["job_id"],"grok_bot",private.item_id)
    owner=replace(group,native_id="owner-query",channel="bot_dm",chat_id="owner-dm",
                  sender_id="owner",mentions=())
    _,request=run(gw,owner,"grok_bot")
    assert request["context"]["project_snapshot"][0]["candidate"]["value"]=="绝不能出现在群里的候选"


def test_edit_input_draft_and_session_commit_atomically(tmp_path,monkeypatch):
    gw=setup(tmp_path)
    ingress=BridgeIngress(gw,legacy_control=lambda p:False,verified_scope=lambda p:True,
                          projects_for=lambda p:("p",))
    query=Event("t","query","g","u","修改交期",time.time(),mentions=("bot",))
    job,_=run(gw,query,"grok_bot")
    finish(gw,job,decision_mode="require_approval",reply_text="原始草稿")
    action=gw.store.one("SELECT * FROM gw_outbox WHERE purpose='final'")
    ingress.review("owner",{"gateway":"r2","action_id":action["action_id"],"draft_version":1,"decision":"edit"})
    payload={"header":{"tenant_key":"t"},"event":{"sender":{"sender_id":{"open_id":"owner"}},
        "message":{"message_id":"edit-input","chat_id":"owner-dm","chat_type":"p2p",
        "create_time":str(int(time.time()*1000)),"content":json.dumps({"text":"修改后的全文"})}}}
    original=gw.outbox.review_in_transaction
    def crash_after_edit(*args,**kwargs):
        original(*args,**kwargs)
        raise RuntimeError("simulated crash before transaction commit")
    monkeypatch.setattr(gw.outbox,"review_in_transaction",crash_after_edit)
    with pytest.raises(RuntimeError): ingress.ingest(payload)
    assert gw.store.one("SELECT body FROM gw_outbox WHERE purpose='final'")["body"]=="原始草稿"
    assert not gw.store.one("SELECT 1 FROM gw_items WHERE native_id='edit-input'")
    assert gw.store.one("SELECT 1 FROM gw_edit_sessions")
    monkeypatch.setattr(gw.outbox,"review_in_transaction",original)
    first=ingress.ingest(payload)
    duplicate=ingress.ingest(payload)
    gw.route()
    assert first.claimed and duplicate.duplicate
    assert gw.store.one("SELECT draft_version FROM gw_outbox WHERE purpose='final'")["draft_version"]==2
    assert not gw.store.one("SELECT 1 FROM gw_edit_sessions")
    assert not gw.store.rows("SELECT * FROM gw_jobs WHERE input_refs LIKE ?",("%"+first.item_id+"%",))
    saved=gw.store.get_event({"item_id":first.item_id,"version":1})
    gw.ingest(replace(saved,body="飞书编辑后的控制输入",content_status="parsed",observed_via="poll",
                      version_time=saved.version_time+1))
    gw.route()
    assert not gw.store.rows("SELECT * FROM gw_jobs WHERE input_refs LIKE ?",("%"+first.item_id+"%",))


def test_restart_cannot_silently_move_live_cutover(tmp_path):
    gw=setup(tmp_path)
    Gateway(gw.store,gw.config)
    with pytest.raises(ContractError,match="live_cutover_at"):
        Gateway(gw.store,replace(gw.config,live_cutover_at=100))
    assert gw.store.one("SELECT value FROM gw_meta WHERE key='live_cutover_at'")["value"]=="1"


def test_provider_progress_survives_unknown_and_late_completion(tmp_path):
    class Unknown(RuntimeError):
        unknown=True
    class Adapter:
        on_progress=None
        def run(self,req):
            for state in ("accepted","running"):
                self.on_progress({"job_id":req["job_id"],"attempt_id":req["attempt_id"],
                                  "provider_run_id":"provider-42","state":state})
            raise Unknown("response lost")
    gw=setup(tmp_path)
    gw=Gateway(gw.store,gw.config,{"grok_bot":Adapter()})
    e=Event("t","query","g","u","进展",time.time(),mentions=("bot",))
    gw.ingest(e);gw.route();job=gw.run_one({"grok_bot"})
    record=gw.store.one("SELECT * FROM gw_jobs WHERE job_id=?",(job["job_id"],))
    assert record["state"]=="unknown" and record["provider_run_id"]=="provider-42"
    assert record["provider_accepted_at"] and record["provider_started_at"]
    finish(gw,record,reply_text="核实后的结果")
    gw.record_provider_progress({"job_id":job["job_id"],"attempt_id":job["attempt_id"],
                                "provider_run_id":"provider-42","state":"accepted"})
    assert gw.store.one("SELECT state FROM gw_jobs WHERE job_id=?",(job["job_id"],))["state"]=="completed"


def test_document_job_lease_covers_its_actual_deadline(tmp_path):
    gw=setup(tmp_path)
    e=Event("t","query","g","u","制表",time.time(),mentions=("bot",))
    job,_=run(gw,e,"grok_bot")
    finish(gw,job,decision_mode="submit_task",task_requests=[{"task_kind":"build_document","instruction":"制作表格"}])
    child=gw.claim({"codex_cli"})
    request=gw.request(child)
    assert child["lease_until"]-child["started_at"]>=630
    assert 599<=request["deadline_at"]-time.time()<=601
    with pytest.raises(ContractError,match="verified artifact"):
        finish(gw,child,display_text="声称文件已完成，但没有文件")
    assert not gw.store.rows("SELECT * FROM gw_outbox WHERE purpose='final'")


def test_qwen_candidates_are_persisted_without_materializing_facts(tmp_path):
    gw=setup(tmp_path)
    e=Event("t","unbound","g","u","待归类的新进展",time.time())
    job,_=run(gw,e,"qwen")
    with pytest.raises(ContractError,match="candidates"):
        finish(gw,job,tag_candidates=[{"tags":["progress"]}])
    finish(gw,job,candidates=[{"tags":["progress"],"source_item_ids":[e.item_id]}])
    saved=gw.store.one("SELECT payload FROM gw_candidates WHERE job_id=?",(job["job_id"],))
    assert json.loads(saved["payload"])["candidates"][0]["source_item_ids"]==[e.item_id]
    assert not gw.store.rows("SELECT * FROM gw_facts")
