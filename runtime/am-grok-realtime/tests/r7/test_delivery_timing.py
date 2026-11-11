import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'tests' / 'r5'))
from test_assistant_host import host, bind, bind_peer, make_action


def test_sdk_publish_and_independent_readback_spans_are_distinct(host):
    coordinator = bind(host)
    bind_peer(host, coordinator)
    context, action = make_action(host, coordinator)
    result = coordinator.actions.execute(action['action_id'], action['digest'],
        action['source_stamp'], context, coordinator.action_executor)
    assert result['state'] == 'applied'
    timing = host.store.one("SELECT detail FROM gw_audit WHERE kind='assistant_delivery_timing'")
    value = json.loads(timing['detail'])
    assert value['clock_domain'] == 'host_monotonic'
    assert value['send_call_seconds'] >= 0 and value['independent_readback_seconds'] >= 0
    assert value['total_seconds'] >= value['send_call_seconds'] + value['independent_readback_seconds']
    assert value['send_status'] == value['readback_status'] == 'sent'
    assert value['phone_visibility_measured'] is False
    assert host.sdk.im.v1.message.get.call_count == 1


def test_optional_timing_failure_does_not_lose_verified_delivery(host, monkeypatch):
    coordinator = bind(host)
    bind_peer(host, coordinator)
    context, action = make_action(host, coordinator)
    original = host.store.audit
    def fail_only_timing(db, kind, object_id, detail=''):
        if kind == 'assistant_delivery_timing':
            raise RuntimeError('synthetic diagnostic failure')
        return original(db, kind, object_id, detail)
    monkeypatch.setattr(host.store, 'audit', fail_only_timing)
    result = coordinator.actions.execute(action['action_id'], action['digest'],
        action['source_stamp'], context, coordinator.action_executor)
    assert result['state'] == 'applied'
    assert host.sdk.im.v1.message.reply.call_count == 1


def test_h1_timing_records_native_port_phase_without_inventing_visibility(host, monkeypatch):
    from integration.studio_phase1_factory import Phase1StudioSender
    sender = Phase1StudioSender(host.sdk, host.bundle.authority.scope, host.store.path,
        host.bundle.authority, host.bundle.readers, send_live=False)
    import time
    sender._record_timing('send_call', 'synthetic_effect', time.monotonic())
    item = host.store.one("SELECT detail FROM gw_audit WHERE kind='h1_sdk_timing'")
    value = json.loads(item['detail'])
    assert value['phase'] == 'send_call' and value['seconds'] >= 0
    assert value['phone_visibility_measured'] is False
    monkeypatch.setattr(sender.store, 'audit', lambda *a: (_ for _ in ()).throw(RuntimeError('diagnostic')))
    sender._record_timing('independent_readback', 'synthetic_task', time.monotonic())
