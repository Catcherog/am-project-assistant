"""Exercise real scheduler threads with a controlled, non-network classifier."""
from dataclasses import replace
import json
import threading
import time

from am_gateway.models import Event
from am_gateway.phase1 import Harness, Policy
from am_gateway.phase1.routing import result
from am_gateway.store import Store
from integration.phase1_runtime import Phase1Scheduler


def enqueue(h, mid, body, chat):
    event = Event('tenant', mid, chat, 'owner', body, h.store.clock() - 1, channel='bot_dm')
    policy = Policy('owner', 'existing-auth', chat, (chat,), allow_reply=True)
    h.ingest(event, policy)
    return event


def await_task(h, item_id, timeout=2):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        task = h.store.one('SELECT * FROM h1_tasks WHERE item_id=?', (item_id,))
        if task is not None:
            return task
        threading.Event().wait(.01)
    return None


def test_busy_classifier_and_its_backlog_do_not_block_clear_queries(rig):
    h, _ = rig
    started, release = threading.Event(), threading.Event()
    calls = []

    class Slow:
        def classify(self, event, budget):
            calls.append(event.native_id)
            started.set()
            assert release.wait(5), 'test classifier was not released'
            raise TimeoutError('controlled timeout')

    h.classifier = Slow()
    scheduler = Phase1Scheduler(h)
    enqueue(h, 'slow', '帮我处理一下', 'slow-chat')
    scheduler.start()
    try:
        assert started.wait(2)
        for n in range(4):
            enqueue(h, 'backlog-' + str(n), '帮我处理一下', 'slow-chat')
        quick = enqueue(h, 'quick', 'A 项目目前卡在哪里？', 'independent-chat')
        scheduler.notify()
        task = await_task(h, quick.item_id)
        assert task is not None and task['route_state'] == 'resolved'
        assert not release.is_set() and calls == ['slow']
        assert h.store.one("SELECT count(*) n FROM h1_inbox WHERE state='classification_pending'")['n'] == 4
    finally:
        release.set()
        assert scheduler.stop()['stopped']


def test_deferred_classification_is_durable_across_harness_restart(rig):
    h, clock = rig
    event = enqueue(h, 'deferred', '帮我处理一下', 'owner-dm')
    assert h.route_one(defer_classification=True)['disposition'] == 'classification_pending'

    class Query:
        def classify(self, event, budget):
            return result('project.query')

    restarted = Harness(Store(h.store.path, clock=lambda: clock[0]), h.config, classifier=Query())
    task = restarted.classify_one()
    assert task['item_id'] == event.item_id and task['route_state'] == 'resolved'
    assert restarted.classify_one() is None


def test_expired_classifier_claim_recovers_without_extending_request_deadline(rig):
    h, clock = rig
    event = enqueue(h, 'lost-worker', '帮我处理一下', 'owner-dm')
    h.route_one(defer_classification=True)
    claimed = h._claim_route_input(classification=True)
    assert claimed is not None
    original_created = claimed[0]['created']
    clock[0] += 121

    class MustNotCall:
        def classify(self, event, budget):
            raise AssertionError('expired request must not invoke the model')

    restarted = Harness(Store(h.store.path, clock=lambda: clock[0]), h.config, classifier=MustNotCall())
    task = restarted.classify_one()
    assert task['route_state'] == 'classification_error' and task['last_error'] == 'TimeoutError'
    assert task['deadline'] == original_created + h.config.dispatch_deadline_seconds
    assert restarted.claim() is None


def test_late_classification_result_cannot_overwrite_new_source_version(rig):
    h, _ = rig
    started, release = threading.Event(), threading.Event()

    class Slow:
        def classify(self, event, budget):
            started.set()
            assert release.wait(5)
            return result('document.process')

    h.classifier = Slow()
    event = enqueue(h, 'edited', '帮我处理一下', 'owner-dm')
    h.route_one(defer_classification=True)
    worker = threading.Thread(target=h.classify_one)
    worker.start()
    try:
        assert started.wait(2)
        h.ingest(replace(event, body='查询 A 项目进展', version_time=1001),
                 Policy('owner', 'existing-auth', 'owner-dm', ('owner-dm',), allow_reply=True))
        updated = h.route_one(defer_classification=True)
        assert updated['intent'] == 'project.query'
    finally:
        release.set()
        worker.join(2)
    assert not worker.is_alive()
    task = h.store.one('SELECT * FROM h1_tasks WHERE item_id=?', (event.item_id,))
    assert task['intent'] == 'project.query' and task['source_version'] == 2


def test_scheduler_stop_and_restart_preserves_pending_work(rig):
    h, _ = rig
    scheduler = Phase1Scheduler(h)
    scheduler.start()
    assert scheduler.stop()['stopped']
    event = enqueue(h, 'after-stop', '查询 A 项目进展', 'owner-dm')
    assert not h.store.rows('SELECT * FROM h1_tasks')
    scheduler.start()
    try:
        task = await_task(h, event.item_id)
        assert task is not None and task['route_state'] == 'resolved'
    finally:
        assert scheduler.stop()['stopped']
