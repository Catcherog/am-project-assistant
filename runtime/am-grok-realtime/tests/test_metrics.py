from copy import deepcopy

import pytest

from am_grok_gateway.metrics import summarize
from am_grok_gateway.models import ContractError


def row(rid="one"):
    return dict(request_id=rid, measurement_class="ordinary", status="sent", useful=1,
                result_origin="bot", error_code="", created_at=100., event_created_at=99.,
                dispatch_at=100.01, sent_at=103., output_message_id="reply_" + rid,
                origin="realtime")


def observation(rid="one", useful=True):
    return dict(request_id=rid, output_message_id="reply_" + rid, visible_at=103.2,
                useful_review=useful, evidence_ref="actual-native-proof-" + rid)


def test_send_ack_does_not_prove_phone_visibility():
    report = summarize([row()], minimum_samples=1)["groups"]["ordinary"]
    assert report["event_to_send_ack_p95_ms"] == 4000
    assert report["event_to_visible_p95_ms"] is None
    assert report["latency_verdict"] == "INCOMPLETE_OR_FAILED_VISIBLE_RESULTS"


def test_failures_and_notices_stay_in_the_denominator():
    rows = [row(str(i)) for i in range(20)]
    rows[-1]["status"] = "send_unknown"
    rows[-2].update(result_origin="gateway_notice", error_code="DEADLINE", useful=0)
    report = summarize(rows, minimum_samples=1)["groups"]["ordinary"]
    assert report["accepted_unique_requests"] == 20
    assert report["useful_sent"] == 18
    assert report["event_to_send_ack_p95_ms"] is None
    assert report["non_answer_notices"] == 1


def test_simulation_cannot_pass_native_latency():
    data = row()
    data["measurement_class"] = "synthetic"
    group = summarize([data], visibility=[observation()], minimum_samples=1)["groups"]["synthetic"]
    assert group["latency_verdict"] == "SIMULATION_ONLY"


def test_visibility_must_bind_actual_message_and_review():
    observed = observation()
    observed["output_message_id"] = "other"
    with pytest.raises(ContractError):
        summarize([row()], visibility=[observed], minimum_samples=1)
    group = summarize([row()], visibility=[observation(useful=False)], minimum_samples=1)["groups"]["ordinary"]
    assert group["verified_visible_answers"] == 0
    assert group["event_to_visible_p95_ms"] is None


def test_observed_target_does_not_claim_production_acceptance():
    report = summarize([row()], visibility=[observation()], minimum_samples=1)
    assert report["groups"]["ordinary"]["latency_verdict"] == "OBSERVED_TARGET_MET_REQUIRES_NATIVE_REVIEW"
    assert report["production_acceptance"] == "NOT_INFERRED_FROM_CODE_OR_SIMULATION"


def test_missing_sample_size_is_explicit():
    group = summarize([row()], visibility=[observation()])["groups"]["ordinary"]
    assert group["latency_verdict"] == "INSUFFICIENT_SAMPLES"


def test_duplicate_unknown_or_inconsistent_observations_rejected():
    with pytest.raises(ContractError):
        summarize([row()], visibility=[observation(), observation()])
    with pytest.raises(ContractError):
        summarize([row()], visibility=[observation("other")])
    observed = observation()
    observed["visible_at"] = 102
    with pytest.raises(ContractError):
        summarize([row()], visibility=[observed])
