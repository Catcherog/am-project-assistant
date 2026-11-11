"""Business confirmation uses current authority at the draft claim point.

Real SQLite, Harness ingestion/policy APIs and confirmation code are exercised.
Thread gates choose a legal interleaving; the executor is an explicit local
fixture and no real business data or platform is written.
"""
from dataclasses import replace
import threading
import time
from types import SimpleNamespace

import pytest

from am_gateway.models import ContractError, Event
from am_gateway.phase1 import Harness, HarnessConfig, Policy
from am_gateway.phase1.agents import ProposalRepository
from am_gateway.phase1.contracts import registry
from am_gateway.phase1.control_api import ControlAPI
from am_gateway.store import Store


def make_proposal(tmp_path):
    store = Store(tmp_path / 'confirmation.sqlite')
    capabilities = registry()
    capabilities['project.execute'] = replace(capabilities['project.execute'],
        available=True, durable_acceptance=True, evidence_ref='synthetic-confirmation-fixture')
    harness = Harness(store, HarnessConfig('tenant', 'bot', 'owner', 'owner-dm',
                                           capabilities=capabilities))
    policy = Policy('owner', 'synthetic-owner-authority', 'owner-dm', ('owner-dm',),
                    ('P',), allow_reply=True)
    event = Event('tenant', 'm1', 'owner-dm', 'owner', '把项目 P 状态改成完成',
                  time.time(), channel='bot_dm', project_ids=('P',), revision='v1')
    harness.ingest(event, policy)
    task = harness.route_one()
    envelope = harness.claim()
    assert task['route_state'] == 'resolved' and envelope
    repository = ProposalRepository(store, 'owner')
    proposal = repository.propose(envelope, 'project.execute', {
        'project_id': 'P', 'target': {'type': 'work_item', 'id': 'item1'},
        'action': 'update_fields', 'changes': {'status': 'completed'},
    })
    calls = []

    def execute(row, *, idempotency_key):
        # An independent writer must succeed while the external executor runs;
        # the authority transaction cannot remain open across this call.
        with Store(store.path).transaction() as db:
            db.execute('INSERT INTO gw_audit(at,kind,object_id,detail) VALUES(?,?,?,?)',
                       (time.time(), 'synthetic-business-effect', idempotency_key, 'test only'))
        calls.append((row, idempotency_key))
        return {'status': 'applied', 'evidence_ref': 'synthetic-independent-readback'}

    control = ControlAPI(SimpleNamespace(harness=harness),
        token='synthetic-operator-token-0123456789', proposals=repository, executor=execute)
    return SimpleNamespace(h=harness, policy=policy, event=event, repository=repository,
                           proposal=proposal, control=control, calls=calls, executor=execute)


def confirm(rig, proposal=None):
    proposal = proposal or rig.proposal
    return rig.control.handle('POST', '/v1/proposals/' + proposal['proposal_id'] + '/confirm',
        {'version': proposal['version'], 'digest': proposal['digest']})


@pytest.mark.parametrize('change', ['source_revision', 'policy_narrowing', 'proposal_revision'])
def test_update_committed_before_draft_claim_prevents_stale_execution(tmp_path, monkeypatch, change):
    rig = make_proposal(tmp_path)
    entered, resume = threading.Event(), threading.Event()
    original = rig.repository.confirm
    outcomes = []

    def at_repository_entry(*args, **kwargs):
        entered.set()
        assert resume.wait(5)
        return original(*args, **kwargs)

    monkeypatch.setattr(rig.repository, 'confirm', at_repository_entry)

    def request_confirmation():
        try:
            outcomes.append(confirm(rig))
        except Exception as exc:
            outcomes.append(exc)

    request = threading.Thread(target=request_confirmation)
    request.start()
    try:
        assert entered.wait(5)
        assert rig.repository.get(rig.proposal['proposal_id'])['status'] == 'draft'
        if change == 'source_revision':
            rig.h.ingest(replace(rig.event, body='撤销修改要求', revision='v2',
                                 version_time=time.time() + 1), rig.policy)
        elif change == 'policy_narrowing':
            rig.h.update_policy(rig.proposal['task_id'], replace(rig.policy, version=2, projects=()))
        else:
            payload = {**rig.proposal['payload'], 'changes': {'status': 'in_progress'}}
            rig.repository.revise(rig.proposal['proposal_id'], expected_version=1,
                                  actor='owner', payload=payload)
    finally:
        resume.set()
        request.join(5)
    assert not request.is_alive()
    assert len(outcomes) == 1 and isinstance(outcomes[0], ContractError)
    assert rig.calls == []
    assert rig.repository.get(rig.proposal['proposal_id'])['status'] == 'draft'


def test_runtime_repository_direct_call_requires_current_harness(tmp_path):
    rig = make_proposal(tmp_path)
    with pytest.raises(ContractError, match='Harness authority'):
        rig.repository.confirm(rig.proposal['proposal_id'], version=rig.proposal['version'],
            digest=rig.proposal['digest'], actor='owner', executor=rig.executor)
    assert rig.calls == []
    assert rig.repository.get(rig.proposal['proposal_id'])['status'] == 'draft'


def test_authority_from_another_database_cannot_guard_runtime_proposal(tmp_path):
    rig = make_proposal(tmp_path)
    other = Harness(Store(tmp_path / 'unrelated.sqlite'), rig.h.config)
    with pytest.raises(ContractError, match='Harness authority'):
        rig.repository.confirm(rig.proposal['proposal_id'], version=rig.proposal['version'],
            digest=rig.proposal['digest'], actor='owner', executor=rig.executor, harness=other)
    assert rig.calls == []


def test_current_revised_payload_is_checked_inside_confirmation(tmp_path):
    rig = make_proposal(tmp_path)
    changed = rig.repository.revise(rig.proposal['proposal_id'], expected_version=1, actor='owner',
        payload={**rig.proposal['payload'], 'project_id': 'outside-scope'})
    with pytest.raises(ContractError, match='outside authorized contract'):
        confirm(rig, changed)
    assert rig.calls == []


def test_committed_effect_is_idempotent_and_executor_runs_outside_database_lock(tmp_path):
    rig = make_proposal(tmp_path)
    first = confirm(rig)
    assert first['status'] == 'applied'
    # After an effect has been claimed/executed, a later source update must not
    # turn another click into a second business write.
    rig.h.ingest(replace(rig.event, body='新的要求', revision='v2', version_time=time.time() + 1), rig.policy)
    assert confirm(rig) == first
    assert len(rig.calls) == 1


def test_authority_check_and_draft_claim_share_one_writer_transaction(tmp_path, monkeypatch):
    rig = make_proposal(tmp_path)
    checked, mutation_started, mutation_done = threading.Event(), threading.Event(), threading.Event()
    original_authorize = rig.h._authorize
    outcomes, writer_errors = [], []

    def authorized_while_writer_waits(db, *args, **kwargs):
        value = original_authorize(db, *args, **kwargs)
        checked.set()
        assert mutation_started.wait(5)
        assert not mutation_done.wait(.1)
        return value

    monkeypatch.setattr(rig.h, '_authorize', authorized_while_writer_waits)

    def writer():
        try:
            assert checked.wait(5)
            mutation_started.set()
            rig.h.update_policy(rig.proposal['task_id'], replace(rig.policy, version=2, projects=()))
            mutation_done.set()
        except Exception as exc:
            writer_errors.append(exc)

    thread = threading.Thread(target=writer)
    thread.start()
    try:
        outcomes.append(confirm(rig))
    finally:
        thread.join(5)
    assert not thread.is_alive() and not writer_errors
    assert mutation_done.is_set() and outcomes[0]['status'] == 'applied'
    assert len(rig.calls) == 1
