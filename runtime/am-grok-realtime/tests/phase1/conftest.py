from dataclasses import replace
import pytest

from am_gateway.phase1 import Harness, HarnessConfig, Policy
from am_gateway.phase1.contracts import registry
from am_gateway.models import Event
from am_gateway.store import Store


@pytest.fixture
def rig(tmp_path):
    clock = [1000.0]
    caps = registry()
    for intent in ('project.query','conversation.reply','work_message.assist'):
        caps[intent] = replace(caps[intent],available=True,durable_acceptance=True,
                               evidence_ref='synthetic_contract_fixture_only')
    cfg = HarnessConfig('tenant','bot','owner','owner-dm',capabilities=caps)
    harness = Harness(Store(tmp_path/'gateway.sqlite',clock=lambda:clock[0]),cfg)
    return harness,clock


def admit(h, mid='m1', text='A 项目目前卡在哪里？', *, chat='owner-dm', actor='owner',
          channel='bot_dm', projects=('A',), target=None, **kwargs):
    event = Event('tenant',mid,chat,actor,text,h.store.clock()-1,channel=channel,
                  project_ids=projects,mentions=('bot',) if channel=='group' else (),**kwargs)
    policy = Policy('owner' if channel=='personal_dm' else actor,'test_existing_authorization',
                    target or ('owner-dm' if channel=='personal_dm' else chat),(chat,),projects,
                    allow_reply=True,allow_supplement=actor=='owner')
    h.ingest(event,policy,cursor={'opaque':mid},coverage={'history':'not_backfilled','has_more':True})
    return h.route_one()


def receipt(h,envelope,kind,payload,*,event_id=None,generation=None):
    return h.record_receipt(event_id=event_id or kind+'-'+envelope['identity']['task_id'],
        task_id=envelope['identity']['task_id'], business_id=envelope['identity']['business_operation_id'],
        generation=generation if generation is not None else envelope['execution']['generation'],
        kind=kind,payload=payload)
