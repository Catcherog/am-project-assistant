"""Behavioral regressions for permission changes between work and tool calls."""
from contextlib import contextmanager
from dataclasses import replace
import json

import pytest

from am_gateway.models import ContractError, Event
from am_gateway.phase1 import Policy
from conftest import admit, receipt


def test_reply_permission_rechecked_after_effect_commit_before_sender(rig,monkeypatch):
    h,_=rig
    task=admit(h)
    envelope=h.claim()
    tid=task['task_id']
    original=Policy.parse(task['policy_json'])
    tightened=replace(original,version=original.version+1,allow_reply=False)
    transaction=h.store.transaction
    injected=[]
    sender_calls=[]

    @contextmanager
    def interleave_policy_update():
        # Let the first send transaction commit before changing policy. This is
        # the actual gap between effect creation and the final authorization
        # check; policy updates never run inside an open SQLite transaction.
        with transaction() as db:
            yield db
        if not injected:
            effect=h.store.one("SELECT * FROM h1_effects WHERE task_id=? AND state='sending'",(tid,))
            if effect is not None:
                injected.append(effect['effect_id'])
                h.update_policy(tid,tightened)

    monkeypatch.setattr(h.store,'transaction',interleave_policy_update)
    with pytest.raises(ContractError):
        h.tool_action(tid,envelope['execution']['generation'],'reply',target='owner-dm',body='Must not send',
                      sender=lambda **kwargs: sender_calls.append(kwargs) or {'platform_id':'unexpected'})
    assert len(injected)==1
    assert sender_calls==[]
    effect=h.store.one('SELECT * FROM h1_effects WHERE effect_id=?',(injected[0],))
    assert effect['state']=='denied' and effect['platform_id'] is None
    assert Policy.parse(h.task(tid)['policy_json']).allow_reply is False


def test_explicit_collection_only_disposition_is_not_undone_by_weak_poll(rig):
    h,_=rig
    task=admit(h)
    event=Event.from_dict(json.loads(task['request_json']))
    disposition=h.ingest(event,collection_only=True)
    assert disposition.handled is False
    h.route_one()
    duplicate=h.ingest(replace(event,observed_via='poll'))
    assert duplicate.handled is False
    h.route_one()
    assert h.store.one('SELECT accepted FROM h1_inbox WHERE item_id=?',(event.item_id,))['accepted']==0
    assert h.claim() is None
    assert len(h.store.rows('SELECT * FROM h1_tasks'))==1
    assert h.task(task['task_id'])['business_id'] is None


def test_legacy_ownership_after_claim_blocks_tools_but_retains_reconciliation(rig):
    h,_=rig
    task=admit(h)
    event=Event.from_dict(json.loads(task['request_json']))
    envelope=h.claim()
    receipt(h,envelope,'accepted',{'run_id':'existing-run','durable_record':'receiver-existing-row'})
    receipt(h,envelope,'running',{'run_id':'existing-run'})
    tid=task['task_id']
    identity_names=('business_id','generation','attempt','run_id')
    before=h.task(tid)
    identity={name:before[name] for name in identity_names}
    saved_receipts=h.store.rows('SELECT * FROM h1_receipts WHERE task_id=? ORDER BY event_id',(tid,))
    saved_session=h.store.one('SELECT * FROM h1_sessions WHERE task_id=?',(tid,))

    ownership=h.ingest(event,legacy_owned=True)
    assert ownership.handled is False
    # No route worker has run since the ownership change. Immediate tool checks
    # must already reject this new effect, without claiming the old run stopped.
    sender_calls=[]
    with pytest.raises(ContractError):
        h.tool_action(tid,envelope['execution']['generation'],'reply',target='owner-dm',body='Must not send',
                      sender=lambda **kwargs: sender_calls.append(kwargs) or {'platform_id':'unexpected'})
    assert sender_calls==[]
    assert not h.store.rows('SELECT * FROM h1_effects WHERE task_id=?',(tid,))
    assert h.store.one('SELECT legacy_owned FROM gw_items WHERE item_id=?',(event.item_id,))['legacy_owned']==1
    after=h.task(tid)
    assert {name:after[name] for name in identity_names}==identity
    assert h.store.rows('SELECT * FROM h1_receipts WHERE task_id=? ORDER BY event_id',(tid,))==saved_receipts
    assert h.store.one('SELECT * FROM h1_sessions WHERE task_id=?',(tid,))==saved_session
    assert saved_session['state']=='held'
    assert h.ingest(replace(event,observed_via='poll')).handled is False
    h.route_one()
    assert h.claim() is None
    assert h.task(tid)['business_id']==identity['business_id']
