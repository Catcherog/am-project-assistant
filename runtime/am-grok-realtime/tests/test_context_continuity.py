from dataclasses import replace
import json

from test_gateway_core import Harness
from am_grok_gateway.models import AuthContext


def test_same_chat_user_gets_prior_answer_without_full_poll(tmp_path):
    h = Harness(tmp_path / "memory.sqlite")
    rid = h.ready()
    assert h.gateway.send_once()
    second = h.admit(h.message("om_second", text="刚才说的验证包括什么？"))
    assert h.gateway.dispatch_once()
    request = h.transport.calls[-1]
    assert request["request_id"] == second
    assert request["context"]["recent_messages"][0]["text"] == "解释一下 DVT"
    assert "DVT" in request["context"]["recent_messages"][1]["text"]
    assert request["context"]["recent_messages"][1]["historical_context_only"] is True


def test_other_person_and_changed_grant_get_no_prior_history(tmp_path):
    h = Harness(tmp_path / "isolation.sqlite")
    h.ready()
    h.gateway.send_once()
    assert h.store.history(h.message("other", sender_id="ou_someone_else")) == []
    auth = replace(h.message().auth, policy_version="scope-v2")
    assert h.store.history(h.message("newscope", auth=auth)) == []
    h.clock.advance(86401)
    assert h.store.history(h.message("old")) == []


def test_dispatch_metric_includes_host_authorization_work(tmp_path):
    h = Harness(tmp_path / "timing.sqlite")
    h.on_authorize = lambda message: h.clock.advance(2)
    rid = h.admit()
    assert h.gateway.dispatch_once()
    row = h.store.get(rid)
    assert row["dispatch_claimed_at"] == row["created_at"]
    assert row["dispatch_at"] - row["received_at"] == 2
