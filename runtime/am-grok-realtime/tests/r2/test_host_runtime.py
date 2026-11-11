"""Verified host entry and card editing exercise; no actual Feishu SDK needed."""
import json
from pathlib import Path
import time
from dataclasses import replace
import importlib.util

import pytest
from am_gateway.core import Gateway
from am_gateway.models import Config,Event,ContractError,SendReceipt
from am_gateway.store import Store
from integration.am_multi_ai_host import BridgeIngress,ExistingSender,normalize_message,ExistingStateProjection


def setup(tmp_path):
    gw=Gateway(Store(tmp_path/"db"),Config("t","bot","owner","owner-dm",1,batch_wait_seconds=0,
                                        project_chats={"p":("g",)}))
    ingress=BridgeIngress(gw,legacy_control=lambda p:p.get("control",False),
                          verified_scope=lambda p:True,projects_for=lambda p:("p",))
    return gw,ingress


def payload(mid,text,*,actor="owner",chat="owner-dm",type="p2p",mentions=()):
    return {"header":{"tenant_key":"t"},"event":{"sender":{"sender_id":{"open_id":actor}},
        "message":{"message_id":mid,"chat_id":chat,"chat_type":type,"create_time":str(int(time.time()*1000)),
                   "content":json.dumps({"text":text}),"mentions":[{"id":{"open_id":m}} for m in mentions]}}}


def approve_action(gw):
    event=Event("t","sensitive","g","user","交期能改吗",time.time(),mentions=("bot",))
    gw.ingest(event);gw.route()
    job=gw.claim({"grok_bot"});gw.request(job)
    gw.complete(job["job_id"],job["attempt_id"],{
        "schema":"am.gateway.result.v2","job_id":job["job_id"],"attempt_id":job["attempt_id"],
        "status":"completed","decision_mode":"require_approval","reply_text":"原始草稿"})
    return gw.store.one("SELECT * FROM gw_outbox WHERE purpose='final'")


def test_card_edit_next_dm_is_archived_without_fact_extraction(tmp_path):
    gw,ingress=setup(tmp_path);action=approve_action(gw)
    card={"event":{"operator":{"open_id":"owner"},"action":{"value":{
       "gateway":"r2","action_id":action["action_id"],"draft_version":1,"decision":"edit"}}}}
    response=ingress.card_callback(card)
    assert "完整草稿" in response["toast"]["content"]
    result=ingress.ingest(payload("edit-message","修改后的最终文本"))
    gw.route()
    assert result.claimed
    assert not gw.store.rows("SELECT * FROM gw_jobs WHERE input_refs LIKE ?",("%"+result.item_id+"%",))
    row=gw.store.one("SELECT * FROM gw_outbox WHERE action_id=?",(action["action_id"],))
    assert row["body"]=="修改后的最终文本" and row["draft_version"]==2
    with pytest.raises(ContractError):
        ingress.card_callback({"event":{"operator":{"open_id":"not-owner"},"action":{"value":{
            "gateway":"r2","action_id":action["action_id"],"draft_version":2,"decision":"approve"}}}})


def test_control_commands_keep_existing_flow_and_capture(tmp_path):
    gw,ingress=setup(tmp_path)
    p=payload("control","确认已有动作");p["control"]=True
    result=ingress.ingest(p);gw.route()
    assert result.captured and not result.claimed and not result.dialogue
    assert not gw.store.rows("SELECT * FROM gw_jobs WHERE task_kind='dialogue_reply'")


def test_sender_action_uuid_distinguishes_ack_and_final(tmp_path):
    gw,_=setup(tmp_path)
    ids=[]
    sender=ExistingSender(lambda target,text,uid,artifacts:ids.append(uid) or "sent-"+str(len(ids)),
                          lambda target,card,uid:"card")
    one={"action_id":"a","action_type":"message","target_json":json.dumps({}),"payload_json":"{}","body":"已受理"}
    two={**one,"action_id":"b","body":"最终结果"}
    sender.send(one);sender.send(two);sender.send(one)
    assert ids[0]!=ids[1] and ids[0]==ids[2]


def test_host_message_normalizer_uses_real_mentions(tmp_path):
    gw,_=setup(tmp_path)
    e=normalize_message(payload("m","只是文本 @AM",actor="u",chat="g",type="group",mentions=("another",)),gw.config)
    assert e.mentions==("another",) and e.channel=="group"
    private=normalize_message(payload("p","本人普通私聊"),gw.config,observed_via="poll",channel="personal_dm")
    assert private.channel=="personal_dm"


def test_patch_generator_preserves_input_and_compiles(tmp_path):
    # Assert the same observed hook shape, not the private user's live file.
    spec=importlib.util.spec_from_file_location("r2_patch",Path(__file__).parents[2]/"tools/patch_bridge_r2.py")
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    source="""def serve(config):
    def on_message(event):
        try:
            diagnostic['accepted'] = bridge.ingest(payload)
        finally:
            pass
    def on_card(event):
        accepted = False
        if bridge.am_self is not None:
            pass
    def on_recalled(event):
        pass
    try:
        pass
    finally:
        bridge.stop.set()
        information_stopped = bridge.information is None or bridge.information.close()
"""
    candidate=module.patched(source)
    assert "bridge.multi_ai.card_callback" in candidate and "bridge.multi_ai.ingest(payload)" in candidate
    assert "bridge.multi_ai" not in source
    compile(candidate,"candidate","exec")
