from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'tests' / 'r5'))
from test_assistant_coordinator import ALICE, OWNER, admit, message, rig
from am_gateway.assistant.response import response_guidance
from am_gateway.phase1.agents import AgentRunner, ArtifactStore


def envelope(rig, *, owner=False):
    event = message(rig, actor=OWNER if owner else ALICE, chat='oc_owner' if owner else 'oc_alice')
    admit(rig, event)
    rig.h.route_one()
    return rig.h.claim()


def test_guidance_is_carried_in_actual_worker_envelope_without_another_call(rig):
    task = envelope(rig)
    guidance = task['output']['response_guidance']
    assert guidance['requires_extra_model_call'] is False
    assert '保留预计' in guidance['instructions']
    assert guidance['authority'] == 'expression_only_no_new_permissions'
    assert task['policy']['allow_reply'] is True


def test_internal_document_model_gets_same_guidance_and_original_request(rig, tmp_path):
    task = envelope(rig)
    agent = AgentRunner('document.process', ArtifactStore(tmp_path))
    request = agent._model_request(task, {})
    assert task['output']['response_guidance'] == request['response_guidance']
    assert '不能只回答最快的一项' in request['instructions']
    assert request['request']['raw_text'] == task['payload']['raw_text']


def test_owner_and_colleague_addressing_differ_without_new_permissions():
    owner = response_guidance(channel='bot_dm', owner_request=True)
    peer = response_guidance(channel='group')
    assert owner['addressing'] != peer['addressing']
    assert owner['authority'] == peer['authority']


def test_mutating_a_returned_guidance_does_not_change_other_requests():
    first = response_guidance(channel='group')
    first['instructions'] = 'changed in a synthetic test'
    assert response_guidance(channel='group')['instructions'] != first['instructions']
