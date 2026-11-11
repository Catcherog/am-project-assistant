"""Real coordinator/actions/SQLite concurrency with controlled transport only."""
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from pathlib import Path
import sys
import threading
from types import SimpleNamespace as NS

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'tests' / 'r5'))
from test_assistant_coordinator import ALICE, OWNER, message, rig
from am_gateway.phase1.service import RuntimeService


def prepare(rig, mid, *, owner=False, group=False):
    event = message(rig, '你好', mid=mid, actor=OWNER if owner else ALICE,
        chat='oc_hd' if group else 'oc_owner' if owner else 'oc_alice',
        channel='group' if group else 'bot_dm',
        mentions=(rig.cfg.bot_id,) if group else ())
    rig.c.capture(event)
    assert rig.c.intake_once()['state'] == 'local_ready'
    return event


def controlled_sender(rig, first_id, entered, release, completed):
    original = rig.c.action_executor
    calls = []

    def sender(frozen):
        calls.append(frozen['action_id'])
        if frozen['source_stamp']['item_id'] == first_id:
            entered.set()
            assert release.wait(5), 'test did not release first transport'
        result = original(frozen)
        completed.append(frozen['source_stamp']['item_id'])
        return result

    rig.c.action_executor = sender
    return calls


def test_other_chat_delivers_while_first_transport_blocks_and_same_chat_stays_ordered(rig):
    first = prepare(rig, 'slow-a')
    same = prepare(rig, 'next-a')
    other = prepare(rig, 'fast-b', owner=True)
    entered, release = threading.Event(), threading.Event()
    completed = []
    calls = controlled_sender(rig, first.item_id, entered, release, completed)
    with ThreadPoolExecutor(max_workers=2) as pool:
        slow = pool.submit(rig.c.local_delivery_once)
        try:
            assert entered.wait(2)
            fast = pool.submit(rig.c.local_delivery_once)
            assert fast.result(timeout=2) == 1
            assert completed == [other.item_id]
            assert len(calls) == 2
        finally:
            release.set()
        assert slow.result(timeout=3) == 2
    assert completed == [other.item_id, first.item_id, same.item_id]
    assert len(calls) == len(set(calls)) == 3
    assert rig.c.local_delivery_once() == 0


def test_group_members_share_one_thread_delivery_order(rig):
    first = prepare(rig, 'group-a', group=True)
    second = prepare(rig, 'group-b', owner=True, group=True)
    entered, release = threading.Event(), threading.Event()
    completed = []
    controlled_sender(rig, first.item_id, entered, release, completed)
    with ThreadPoolExecutor(max_workers=1) as pool:
        task = pool.submit(rig.c.local_delivery_once)
        try:
            assert entered.wait(2)
            assert rig.c.local_delivery_once() == 0
            assert completed == []
        finally:
            release.set()
        assert task.result(timeout=3) == 2
    assert completed == [first.item_id, second.item_id]


def test_two_coordinators_share_durable_claim_and_send_once(rig):
    first = prepare(rig, 'once')
    second_coordinator = rig.rebuild()
    entered, release = threading.Event(), threading.Event()
    completed = []
    calls = controlled_sender(rig, first.item_id, entered, release, completed)
    second_coordinator.action_executor = rig.c.action_executor
    with ThreadPoolExecutor(max_workers=1) as pool:
        task = pool.submit(rig.c.local_delivery_once)
        try:
            assert entered.wait(2)
            assert second_coordinator.local_delivery_once() == 0
        finally:
            release.set()
        assert task.result(timeout=3) == 1
    assert second_coordinator.local_delivery_once() == 0
    assert len(calls) == 1
    assert rig.c.actions.list()[0]['attempt_count'] == 1


def test_abandoned_preparation_claim_can_expire_without_recreating_action(rig):
    prepare(rig, 'abandoned')
    original = rig.c._claim_local_delivery()
    assert original is not None
    assert rig.c.local_delivery_once() == 0
    rig.clock[0] += rig.c.delivery_lease_seconds + 1
    recovered = rig.rebuild()
    assert recovered.local_delivery_once() == 1
    assert len(rig.sends) == 1
    assert rig.sends[0]['action_id'] == original['action_id']
    assert recovered.actions.list()[0]['attempt_count'] == 1
    assert not rig.c._renew_delivery_claim(original)


def test_expired_queue_lease_cannot_repeat_inflight_send_or_overtake_same_chat(rig):
    first = prepare(rig, 'inflight')
    same = prepare(rig, 'after-inflight')
    other = prepare(rig, 'other-scope', owner=True)
    recovered = rig.rebuild()
    entered, release = threading.Event(), threading.Event()
    completed = []
    calls = controlled_sender(rig, first.item_id, entered, release, completed)
    recovered.action_executor = rig.c.action_executor
    with ThreadPoolExecutor(max_workers=1) as pool:
        task = pool.submit(rig.c.local_delivery_once)
        try:
            assert entered.wait(2)
            rig.clock[0] += rig.c.delivery_lease_seconds + 1
            assert recovered.local_delivery_once() == 1
            assert completed == [other.item_id]
        finally:
            release.set()
        assert task.result(timeout=3) == 2
    assert completed == [other.item_id, first.item_id, same.item_id]
    assert len(calls) == len(set(calls)) == 3


def test_superseded_claim_is_fenced_before_transport_and_cannot_release_new_claim(rig):
    prepare(rig, 'fenced')
    first = rig.c._claim_local_delivery()
    rig.clock[0] += rig.c.delivery_lease_seconds + 1
    current = rig.c._claim_local_delivery()
    assert current['action_id'] == first['action_id']
    assert current['claim_token'] != first['claim_token']
    assert rig.c._deliver_local_row(first, first) == 0
    rig.c._release_delivery_claim(first)
    assert rig.sends == []
    assert rig.c._renew_delivery_claim(current)
    assert rig.c._deliver_local_row(current, current) == 1
    rig.c._release_delivery_claim(current)
    assert len(rig.sends) == 1


@pytest.mark.parametrize('mode', ['unknown', 'exception', 'unverified'])
def test_unresolved_result_is_not_replayed_after_restart_or_by_another_worker(rig, mode):
    prepare(rig, 'unresolved')
    calls = []

    def sender(frozen):
        calls.append(frozen['action_id'])
        if mode == 'exception':
            raise OSError('uncertain network response')
        if mode == 'unverified':
            return {'status': 'applied', 'evidence_ref': 'feishu:message:unverified'}
        return {'status': 'unknown'}

    rig.c.action_executor = sender
    assert rig.c.local_delivery_once() == 0
    assert rig.c.local_delivery_once() == 0
    recovered = rig.rebuild()
    recovered.action_executor = sender
    rig.clock[0] += 121
    assert recovered.local_delivery_once() == 0
    assert len(calls) == 1
    # A later, separately authorised reply is still allowed, without replaying
    # the uncertain earlier effect. This preserves the R6 non-starvation rule.
    rig.c = rig.rebuild()
    prepare(rig, 'fresh-after-unresolved')
    assert rig.c.local_delivery_once() == 1
    assert len(calls) == 1 and len(rig.sends) == 1


def test_recall_after_queue_claim_prevents_send(rig):
    event = prepare(rig, 'recall-after-claim')
    claim = rig.c._claim_local_delivery()
    rig.c.revoke(replace(event, is_deleted=True, version_time=event.version_time + 1))
    assert rig.c._deliver_local_row(claim, claim) == 0
    rig.c._release_delivery_claim(claim)
    assert rig.sends == []


def test_existing_local_queue_gets_scope_migration_and_preserves_action_identity(rig):
    prepare(rig, 'migration')
    original = rig.store.one('SELECT action_id FROM r5_local_replies')['action_id']
    with rig.store.transaction() as db:
        db.execute('DELETE FROM r7_delivery_scopes')
    recovered = rig.rebuild()
    assert recovered.local_delivery_once() == 1
    assert rig.sends[0]['action_id'] == original


def test_live_service_has_parallel_delivery_lanes_without_another_consumer(rig):
    first = prepare(rig, 'runtime-slow')
    entered, release, fast_delivered = threading.Event(), threading.Event(), threading.Event()
    original = rig.c.action_executor

    def sender(frozen):
        if frozen['source_stamp']['item_id'] == first.item_id:
            entered.set()
            assert release.wait(5)
        value = original(frozen)
        if frozen['payload']['target']['chat_id'] == 'oc_owner':
            fast_delivered.set()
        return value

    rig.c.action_executor = sender
    service = RuntimeService(NS(gateway=rig.h, scheduler=None), dispatch_enabled=False)
    service.assistant = rig.c
    try:
        service.start()
        assert entered.wait(2)
        rig.c.capture(message(rig, '你好', mid='runtime-fast', actor=OWNER, chat='oc_owner'))
        assert fast_delivered.wait(2), 'independent chat waited for slow transport'
        lanes = [t for t in service.threads if t.name.startswith('am-r4-assistant-local-delivery')]
        assert len(lanes) == rig.c.delivery_workers == 4
    finally:
        release.set()
        assert service.stop()['stopped']
    assert len(rig.sends) == 2
