"""Evaluation must measure evidence rather than declarations of success."""
from copy import deepcopy
import json
from pathlib import Path
import subprocess
import sys

import pytest

from am_gateway.assistant.evaluation import (
    SCHEMA_VERSION, EvaluationInputError, compare_runs, distribution, evaluate_record,
    evaluate_run, load_cases, timing_metrics, validate_cases,
)


ROOT = Path(__file__).resolve().parents[2]


def case(identifier="known-group", channel="group"):
    return {
        "case_id": identifier, "channel": channel, "intent": "project_overview",
        "source_version": "synthetic-r6-v1", "status": "not_run",
        "oracle": {
            "required_facts": {"project_id": "P-SYN-ALPHA", "test_started": False},
            "required_text": {"project": ["样例甲"], "blocker": ["工装未到"],
                              "next_step": ["确认工装交期"]},
            "required_qualifiers": {"progress": ["尚未启动", "还没开始"]},
            "forbidden_text": ["没查到", "不存在", "测试已经完成"],
            "required_subtasks": ["progress", "blocker", "next_step"],
            "expected_retrieval_status": ["FOUND"], "required_metrics": ["completion_s"],
            "allowed_kinds": ["answer"],
        },
    }


def record(expected=None, layer="offline", elapsed=4.0):
    expected = expected or case()
    value = {
        "schema_version": SCHEMA_VERSION, "case_id": expected["case_id"], "attempt_id": "1",
        "channel": expected["channel"], "intent": expected["intent"], "cold_or_warm": "warm",
        "source_version": expected["source_version"], "layer": layer,
        "input_event_id": "input-1", "conversation_id": "chat-1",
        "output": {"kind": "answer", "text": "样例甲：测试尚未启动，工装未到；下一步确认工装交期。",
                   "facts": {"project_id": "P-SYN-ALPHA", "test_started": False},
                   "completed_subtasks": ["progress", "blocker", "next_step"],
                   "retrieval_status": "FOUND", "channel": expected["channel"],
                   "origin_event_id": "input-1", "conversation_id": "chat-1", "reply_id": "reply-1"},
        "assertions": [], "artifacts": [], "outcome": {"status": "completed"}, "readback": None,
        "timing": {"clock_domain": "host.monotonic:run-1",
                   "events": {"received": 100.0, "ack_visible": 100.1,
                              "first_substantive_visible": 100.0 + elapsed,
                              "completed": 100.0 + elapsed, "final_visible": 100.0 + elapsed},
                   "spans": {"model": [], "retrieval": [{"start": 100.1, "end": 100.3}]}},
    }
    if layer == "real_delivery":
        value["readback"] = {
            "independent": True, "visible": True, "method": "sdk", "evidence_ref": "evidence:reply-1",
            "channel": expected["channel"], "origin_event_id": "input-1", "conversation_id": "chat-1",
            "reply_id": "reply-1", "text": value["output"]["text"], "final_reply_count": 1,
            "duplicate_reply_count": 0,
        }
    return value


def test_offline_success_is_explicitly_not_real_delivery():
    result = evaluate_run([case()], [record()])
    assert result["ci_pass"]
    assert result["results"][0]["status"] == "offline_success"
    assert result["summary"]["successful_completion_s"]["median"] == 4
    assert result["results"][0]["style"]["human_score"] is None
    rejected = evaluate_run([case()], [record()], expected_layer="real_delivery")
    assert not rejected["ci_pass"]
    assert "unexpected_evidence_layer" in rejected["results"][0]["issues"]


def test_real_delivery_requires_same_identity_and_independent_readback():
    value = record(layer="real_delivery")
    assert evaluate_record(value, case())["status"] == "real_delivery_success"
    value["readback"] = None
    result = evaluate_record(value, case())
    assert result["status"] == "failed"
    assert "missing_independent_delivery_readback" in result["issues"]


@pytest.mark.parametrize("kind", ["ack", "answer"])
def test_fast_ack_cannot_be_counted_as_substantive_answer_even_when_mislabelled(kind):
    value = record(elapsed=0.1)
    value["output"]["text"] = "收到，我来查一下。"
    value["output"]["kind"] = kind
    result = evaluate_run([case()], [value])
    assert not result["ci_pass"]
    assert result["summary"]["successful_completion_s"]["n"] == 0
    row = result["results"][0]
    assert "ack_is_not_substantive_completion" in row["issues"]
    assert row["timing"]["first_substantive_s"] is None
    assert row["timing"]["completion_s"] is None
    assert row["timing"]["reported_elapsed_to_end_s"] == pytest.approx(.1)


@pytest.mark.parametrize("outcome", ["error", "timeout", "cancelled"])
def test_failed_outcomes_remain_in_denominator_and_outside_success_latency(outcome):
    value = record(elapsed=1)
    value["outcome"]["status"] = outcome
    result = evaluate_run([case()], [value])
    assert result["summary"]["denominator"] == 1
    assert result["summary"]["successes"] == 0
    assert result["summary"]["outcome_counts"][outcome] == 1
    assert result["summary"]["all_observed_elapsed_s"]["n"] == 1
    assert result["summary"]["successful_completion_s"]["n"] == 0


def test_missing_cases_are_not_run_and_do_not_disappear():
    expected = [case(), case("known-private", "private")]
    result = evaluate_run(expected, [record()])
    assert result["summary"]["denominator"] == 2
    assert result["summary"]["success_rate"] == .5
    assert result["summary"]["unrun_cases"] == 1
    assert result["summary"]["status_counts"]["not_run"] == 1
    assert not result["ci_pass"]


def test_explicit_not_run_record_does_not_count_as_executed_case():
    observed = record()
    observed["outcome"]["status"] = "not_run"
    result = evaluate_run([case()], [observed])
    assert result["summary"]["cases_with_records"] == 1
    assert result["summary"]["cases_attempted"] == 0
    assert result["summary"]["unrun_cases"] == 1
    assert result["summary"]["status_counts"] == {"not_run": 1}


def test_unknown_cases_are_failed_records_in_addition_to_unrun_planned_cases():
    unknown = record()
    unknown["case_id"] = "not-planned"
    result = evaluate_run([case()], [unknown])
    assert result["summary"]["denominator"] == 2
    assert result["summary"]["status_counts"] == {"failed": 1, "not_run": 1}


def test_duplicate_attempts_are_all_failed_instead_of_silently_overwritten():
    result = evaluate_run([case()], [record(), record()])
    assert result["summary"]["denominator"] == 2
    assert result["summary"]["successes"] == 0
    assert all("duplicate_attempt_identity" in row["issues"] for row in result["results"])


def test_explicit_repetitions_are_counted_as_repetitions():
    second = record(elapsed=6)
    second["attempt_id"] = "2"
    result = evaluate_run([case()], [record(), second])
    assert result["ci_pass"]
    assert result["summary"]["successful_completion_s"]["median"] == 5
    assert result["summary"]["successful_completion_s"]["p95"] is None


def test_cross_clock_subtraction_is_unknown_not_zero_or_negative():
    value = record(layer="real_delivery")
    value["timing"]["events"]["final_visible"] = {"at": 50, "clock_domain": "platform.epoch"}
    result = evaluate_record(value, case())
    assert result["timing"]["completion_s"] is None
    assert result["timing"]["unattributed_s"] is None
    assert "mixed_clock:received:final_visible" in result["timing"]["issues"]
    assert "required_metric_unknown:completion_s" in result["issues"]


def test_overlapping_model_and_retrieval_spans_use_union_for_residual():
    value = {"clock_domain": "host-1", "events": {"received": 0, "completed": 12},
             "spans": {"model": [{"start": 1, "end": 6}, {"start": 4, "end": 9}],
                       "retrieval": [{"start": 0, "end": 2}]}}
    measured = timing_metrics(value)
    assert measured["model_sum_s"] == 10
    assert measured["model_union_s"] == 8
    assert measured["accounted_union_s"] == 9
    assert measured["unattributed_s"] == 3


def test_missing_spans_are_unknown_and_explicit_empty_spans_are_zero():
    value = {"clock_domain": "host-1", "events": {"received": 0, "completed": 8},
             "spans": {"retrieval": []}}
    assert timing_metrics(value)["model_union_s"] is None
    assert timing_metrics(value)["unattributed_s"] is None
    value["spans"]["model"] = []
    assert timing_metrics(value)["model_union_s"] == 0
    assert timing_metrics(value)["unattributed_s"] == 8
    assert timing_metrics(None)["completion_s"] is None


@pytest.mark.parametrize("span", [
    {"start": 1, "end": 3, "clock_domain": "different-host"},
    {"start": 3, "end": 1}, {"start": 1, "end": 99},
])
def test_invalid_span_is_not_dropped_to_make_remaining_metrics_look_complete(span):
    value = {"clock_domain": "host-1", "events": {"received": 0, "completed": 8},
             "spans": {"model": [span], "retrieval": []}}
    measured = timing_metrics(value)
    assert measured["model_union_s"] is None
    assert measured["accounted_union_s"] is None
    assert measured["unattributed_s"] is None


def test_missing_required_subtask_and_qualifier_fail_even_with_success_outcome():
    value = record()
    value["output"]["completed_subtasks"].remove("next_step")
    value["output"]["text"] = value["output"]["text"].replace("尚未启动", "已经完成")
    result = evaluate_record(value, case())
    assert "missing_subtask:next_step" in result["issues"]
    assert "required_qualifiers:progress" in result["issues"]
    assert result["status"] == "failed"


def test_boolean_fact_is_not_equal_to_integer():
    value = record()
    value["output"]["facts"]["test_started"] = 0
    assert "required_fact:test_started" in evaluate_record(value, case())["issues"]


@pytest.mark.parametrize("container,key,value", [
    ("output", "channel", "private"), ("readback", "channel", "private"),
    ("readback", "conversation_id", "other-chat"), ("readback", "origin_event_id", "other-input"),
    ("readback", "final_reply_count", 2), ("readback", "duplicate_reply_count", 1),
    ("readback", "visible", False), ("readback", "independent", False),
    ("readback", "text", "另一条答复"), ("readback", "reply_id", "other-reply"),
])
def test_real_delivery_binding_and_uniqueness(container, key, value):
    observed = record(layer="real_delivery")
    observed[container][key] = value
    assert evaluate_record(observed, case())["status"] == "failed"


def test_multiple_progress_messages_are_not_mistaken_for_duplicate_final_answers():
    observed = record(layer="real_delivery")
    observed["readback"]["total_message_count"] = 3
    assert evaluate_record(observed, case())["status"] == "real_delivery_success"


def test_source_or_channel_binding_mismatch_fails_before_comparing_speed():
    observed = record()
    observed["source_version"] = "different-snapshot"
    observed["channel"] = "private"
    result = evaluate_record(observed, case())
    assert "case_binding_mismatch:source_version" in result["issues"]
    assert "case_binding_mismatch:channel" in result["issues"]


def test_false_zero_result_reproduces_known_business_failure():
    observed = record()
    observed["output"].update(text="系统里没查到样例甲，所以无法提供进展。", retrieval_status="NO_MATCH_CONFIRMED")
    result = evaluate_record(observed, case())
    assert result["status"] == "failed"
    assert "wrong_retrieval_status" in result["issues"]
    assert "forbidden_claim:没查到" in result["issues"]


def test_model_self_assessment_cannot_prove_required_business_assertion():
    expected = case()
    expected["oracle"]["required_assertions"] = ["no_business_mutation"]
    observed = record()
    observed["assertions"] = [{"id": "no_business_mutation", "status": "pass", "method": "model_self",
                               "evidence_ref": "model:says-no-write"}]
    assert "unverified_assertion:no_business_mutation" in evaluate_record(observed, expected)["issues"]
    observed["assertions"][0].update(method="independent_readback", evidence_ref="sqlite:before-after")
    assert evaluate_record(observed, expected)["status"] == "offline_success"


def test_required_artifact_must_have_matching_independent_delivery_evidence():
    expected = case()
    expected["oracle"]["required_artifact_kinds"] = ["docx"]
    observed = record(layer="real_delivery")
    assert "missing_artifact:docx" in evaluate_record(observed, expected)["issues"]
    artifact = {"kind": "docx", "sha256": "a" * 64,
                "readback": {"independent": True, "visible": True, "method": "sdk", "sha256": "b" * 64,
                             "channel": "group", "conversation_id": "chat-1", "origin_event_id": "input-1",
                             "evidence_ref": "evidence:attachment"}}
    observed["artifacts"] = [artifact]
    assert "artifact_not_verified:docx" in evaluate_record(observed, expected)["issues"]
    artifact["readback"]["sha256"] = "a" * 64
    assert evaluate_record(observed, expected)["status"] == "real_delivery_success"


def test_raw_text_and_all_records_are_preserved_and_style_is_only_a_flag():
    expected = case()
    expected["oracle"]["style_max_chars"] = 5
    observed = record()
    before = deepcopy(observed)
    result = evaluate_record(observed, expected)
    assert result["record"] == before
    assert observed == before
    assert result["style"]["flags"] == ["longer_than_case_guideline"]
    assert result["style"]["human_evaluation_performed"] is False
    assert result["style"]["human_score"] is None
    assert result["status"] == "offline_success"


def test_p95_requires_at_least_twenty_observations():
    small = distribution(range(1, 20))
    assert small["median"] == 10 and small["max"] == 19
    assert small["p95"] is None and small["p95_status"] == "insufficient_samples"
    enough = distribution(range(1, 21))
    assert enough["p95"] == 19 and enough["p95_status"] == "available"


def test_comparison_does_not_call_wrong_fast_answer_a_speedup():
    faster = record(elapsed=1)
    faster["output"]["text"] = "目前没查到样例甲。"
    report = compare_runs([case()], [record(elapsed=10)], [faster])
    assert not report["ci_pass"]
    assert report["comparison"]["speedup_status"] == "not_demonstrated"
    assert report["comparison"]["paired_completion_delta_s"]["n"] == 0


def test_comparison_matches_conditions_and_separates_quality_improvement_from_speed():
    failed = record(elapsed=60)
    failed["outcome"]["status"] = "timeout"
    report = compare_runs([case()], [failed], [record(elapsed=4)])
    assert report["ci_pass"]
    assert report["comparison"]["speedup_status"] == "not_demonstrated"
    cold = record(elapsed=10)
    cold["cold_or_warm"] = "cold"
    report = compare_runs([case()], [cold], [record(elapsed=4)])
    assert not report["comparison"]["paired"][0]["same_conditions"]
    assert report["comparison"]["paired_completion_delta_s"]["n"] == 0
    report = compare_runs([case()], [record(elapsed=10)], [record(elapsed=4)])
    assert report["comparison"]["paired_completion_delta_s"]["median"] == -6
    assert report["comparison"]["paired_relative_speedup"]["median"] == pytest.approx(.6)


def test_configured_budget_fails_when_missing_or_exceeded():
    expected = case()
    expected["oracle"]["timing_budget_s"] = {"completion_s": 3}
    assert "timing_budget_exceeded:completion_s" in evaluate_record(record(), expected)["issues"]
    observed = record()
    del observed["timing"]["events"]["completed"]
    assert "budget_metric_unknown:completion_s" in evaluate_record(observed, expected)["issues"]


def test_empty_or_duplicate_oracle_is_rejected():
    with pytest.raises(EvaluationInputError, match="duplicate"):
        validate_cases([case(), case()])
    invalid = case()
    invalid["oracle"] = {}
    with pytest.raises(EvaluationInputError, match="empty oracle"):
        validate_cases([invalid])


def test_committed_cases_have_24_main_cases_and_explicit_unrun_synthetic_origin():
    cases = load_cases(ROOT / "evaluation/r6_cases.json")
    primary = [item for item in cases if item["suite"] == "primary"]
    assert len(primary) == 24
    assert {item["channel"] for item in primary} == {"group", "private"}
    assert len({item["scenario_group"] for item in primary}) == 12
    assert all(item["status"] == "not_run" and item["data_origin"] == "controlled_synthetic" for item in cases)
    assert evaluate_run(cases, [])["summary"]["status_counts"] == {"not_run": len(cases)}


def test_cli_compares_complete_reports_and_exits_one_for_missing_case(tmp_path):
    cases_path = tmp_path / "cases.json"
    cases_path.write_text(json.dumps({"schema_version": SCHEMA_VERSION, "cases": [case()]}), encoding="utf-8")
    baseline = tmp_path / "baseline.jsonl"
    candidate = tmp_path / "candidate.jsonl"
    output = tmp_path / "report.json"
    baseline.write_text(json.dumps(record(elapsed=10)) + "\n", encoding="utf-8")
    candidate.write_text(json.dumps(record()) + "\n", encoding="utf-8")
    command = [sys.executable, str(ROOT / "tools/evaluate_assistant.py"), "--cases", str(cases_path),
               "--candidate", str(candidate), "--baseline", str(baseline), "--expected-layer", "offline",
               "--output", str(output)]
    assert subprocess.run(command, capture_output=True, text=True).returncode == 0
    report = json.loads(output.read_text(encoding="utf-8"))
    assert report["candidate"]["results"][0]["record"]["output"]["text"] == record()["output"]["text"]
    candidate.write_text("", encoding="utf-8")
    assert subprocess.run(command, capture_output=True, text=True).returncode == 1
    assert json.loads(output.read_text())["candidate"]["summary"]["status_counts"] == {"not_run": 1}


def test_cli_rejects_invalid_json_instead_of_generating_a_passing_report(tmp_path):
    cases_path = tmp_path / "cases.json"
    cases_path.write_text(json.dumps({"schema_version": SCHEMA_VERSION, "cases": [case()]}), encoding="utf-8")
    candidate = tmp_path / "candidate.jsonl"
    candidate.write_text('{"case_id": NaN}\n', encoding="utf-8")
    result = subprocess.run([sys.executable, str(ROOT / "tools/evaluate_assistant.py"), "--cases", str(cases_path),
                             "--candidate", str(candidate)], capture_output=True, text=True)
    assert result.returncode == 2
    assert "evaluation input error" in result.stderr
