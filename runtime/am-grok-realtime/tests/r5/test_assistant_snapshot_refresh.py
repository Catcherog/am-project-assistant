"""Complete snapshots remain observable while assistant intake is deferred."""
from types import SimpleNamespace as NS
import json

import pytest

from am_gateway.assistant.coordinator import AssistantCoordinator
from am_gateway.assistant.policy import Actor, AssistantPolicy, PolicyConfig
from am_gateway.collectors import SourceSpec
from am_gateway.feishu_snapshot import FeishuMessageSnapshotReader, SnapshotIncomplete
from am_gateway.models import Event
from am_gateway.phase1 import Harness, HarnessConfig, Policy
from am_gateway.store import Store
from integration.phase1_runtime import Phase1Ingress, Phase1Ports


SOURCE = SourceSpec("tenant", "owner-dm", channel="bot_dm", identity="am_bot", project_ids=("A",))


def snapshot(*, updated=1000000, image="old"):
    return {"message_id": "om_snapshot", "chat_id": "owner-dm", "msg_type": "post",
        "create_time": "999000", "update_time": str(updated), "deleted": False,
        "sender": {"id": "owner", "id_type": "open_id", "sender_type": "user", "tenant_key": "tenant"},
        "body": {"content": json.dumps({"content": [[
            {"tag": "text", "text": "A 项目的进展怎么样了？"}, {"tag": "img", "image_key": image}]]})}}


@pytest.fixture
def rig(tmp_path):
    store = Store(tmp_path / "snapshots.sqlite", clock=lambda: 1010.0)
    config = HarnessConfig("tenant", "bot", "owner", "owner-dm")
    harness = Harness(store, config)
    directory = NS(resolve=lambda uid: Actor(uid, "本人", (), True, is_owner=uid == "owner"),
        project_ids_for=lambda actor: ("A",), register_dm=lambda event: None)

    def authority(event):
        return Policy(event.sender_id, "synthetic-current-source", event.chat_id,
                      (event.chat_id,), event.project_ids, allow_reply=True)

    authority.collection_config = NS(live_cutover_at=0)
    coordinator = AssistantCoordinator(harness, directory=directory, authority=authority,
        policy=AssistantPolicy(PolicyConfig(owner_user_ids={"owner"})))
    response = [{"code": 0, "data": {"items": [snapshot()]}}]
    reader = FeishuMessageSnapshotReader(lambda *args: response[0])
    ports = Phase1Ports(config, store.path, lambda payload: True, authority,
        lambda payload: ("A",), lambda payload: False,
        snapshot_reader=reader, verified_snapshot_scope=lambda source, raw: True)
    ingress = Phase1Ingress(harness, ports)
    ingress.assistant = coordinator
    return NS(store=store, harness=harness, coordinator=coordinator, response=response, ingress=ingress)


def refresh(rig):
    return rig.ingress.refresh_message("om_snapshot", source=SOURCE)


def status(rig):
    return rig.ingress.snapshot_refresh_status("om_snapshot", source=SOURCE)


def test_first_snapshot_records_real_intake_revision_without_gateway_row(rig):
    admitted = refresh(rig)
    assert admitted.captured
    assert rig.store.rows("SELECT * FROM gw_items") == []
    intake = rig.store.one("SELECT * FROM r5_intake WHERE item_id=?", (admitted.item_id,))
    saved = status(rig)
    assert saved["state"] == "saved" and saved["detail"] == "assistant_intake:pending"
    assert saved["accepted_revision"] == json.loads(intake["event_json"])["revision"]
    assert saved["accepted_revision"].startswith("feishu-message-v1:")
    assert rig.coordinator.intake_once()["state"] == "admitted"


def test_new_snapshot_reports_intake_version_instead_of_older_gateway_version(rig):
    first = refresh(rig)
    assert rig.coordinator.intake_once()["state"] == "admitted"
    old = rig.store.one("SELECT event_json FROM gw_items WHERE item_id=?", (first.item_id,))
    old_revision = json.loads(old["event_json"])["revision"]
    rig.response[0] = {"code": 0, "data": {"items": [snapshot(updated=1001000, image="new")]}}
    assert refresh(rig).captured
    saved = status(rig)
    current = rig.store.one("SELECT event_json FROM r5_intake WHERE item_id=?", (first.item_id,))
    assert saved["accepted_revision"] == json.loads(current["event_json"])["revision"]
    assert saved["accepted_revision"] != old_revision
    assert saved["detail"] == "assistant_intake:pending"
    unchanged = rig.store.one("SELECT event_json FROM gw_items WHERE item_id=?", (first.item_id,))
    assert json.loads(unchanged["event_json"])["revision"] == old_revision
    assert rig.coordinator.intake_once()["state"] == "admitted"
    applied = rig.store.one("SELECT event_json FROM gw_items WHERE item_id=?", (first.item_id,))
    assert json.loads(applied["event_json"])["revision"] == saved["accepted_revision"]


def test_failed_refresh_preserves_last_saved_intake_revision(rig):
    refresh(rig)
    accepted = status(rig)["accepted_revision"]
    rig.response[0] = {"code": 999}
    with pytest.raises(SnapshotIncomplete):
        refresh(rig)
    failed = status(rig)
    assert failed["state"] == "failed" and failed["detail"] == "SnapshotIncomplete"
    assert failed["accepted_revision"] == accepted
    assert rig.store.rows("SELECT state FROM r5_intake") == [{"state": "pending"}]


def test_scope_rejection_preserves_accepted_revision_without_admitting_new_source(rig):
    refresh(rig)
    accepted = status(rig)["accepted_revision"]
    rig.ingress.ports.verified_snapshot_scope = lambda source, raw: False
    rig.response[0] = {"code": 0, "data": {"items": [snapshot(updated=1001000, image="new")]}}
    assert not refresh(rig).captured
    rejected = status(rig)
    assert rejected["state"] == "scope_rejected" and rejected["accepted_revision"] == accepted
    row = rig.store.rows("SELECT event_json FROM r5_intake")[0]
    assert Event.from_dict(json.loads(row["event_json"])).attachments[0]["resource_key"] == "old"


def test_legacy_owned_snapshot_still_records_gateway_version(rig):
    rig.ingress.ports.legacy_control = lambda payload: True
    admitted = refresh(rig)
    assert admitted.captured and rig.store.rows("SELECT * FROM r5_intake") == []
    source = rig.store.one("SELECT event_json FROM gw_items WHERE item_id=?", (admitted.item_id,))
    saved = status(rig)
    assert saved["state"] == "saved" and saved["detail"] == ""
    assert saved["accepted_revision"] == json.loads(source["event_json"])["revision"]
