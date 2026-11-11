"""Quality tests attack false acceptance, evidence mixing and success bias."""
from copy import deepcopy
import importlib.util
import json
from pathlib import Path

import pytest

from am_gateway.assistant.quality import (LAYERS, SCHEMA_VERSION, QualityInputError,
    compare_quality, concept_matches, distribution, evaluate_attempt, evaluate_quality,
    load_suite, output_hash, regrade_r6)


def case():
    return {"case_id": "TEST-1", "channel": "private", "intent": "progress", "source_version": "v1",
            "family": "progress", "oracle": {
                "concepts": {"progress": {"any_of": ["样机已到", "样机已经到货"]},
                             "qualification": {"any_of": ["尚未启动", "还没开始"]}},
                "required_facts": {"test_started": False}, "required_subtasks": ["progress"],
                "semantic_review_required": True,
                "forbidden_assertions": [r"测试(?:已经|已|现已|现在)?(?:正式)?(?:完成了|已完成)", r"测试已完成"],
                "latency_targets_s": {"first_substantive_s": 3, "completion_s": 6}}}


def record(text="样机已到，寿命测试尚未启动。", *, layer="offline", attempt="1"):
    return {"schema_version": SCHEMA_VERSION, "case_id": "TEST-1", "channel": "private",
            "intent": "progress", "source_version": "v1", "attempt_id": attempt,
            "cold_or_warm": "warm", "layer": layer,
            "output": {"text": text, "first_substantive_text": text, "kind": "answer", "facts": {"test_started": False},
                       "completed_subtasks": ["progress"]},
            "outcome": {"status": "completed"},
            "execution_provenance": {"actual_assistant_intake": True},
            "semantic_review": {"status": "pass", "method": "source_check", "independent": True,
                "evidence_ref": "TEST:source-comparison", "output_sha256": output_hash(text), "source_version": "v1"},
            "timing": {"clock_domain": "host:test", "events": {"received": 10, "ack_visible": 10.2,
                "first_substantive_visible": 12, "completed": 14}}}


def test_concepts_accept_punctuation_and_noncontiguous_words_without_contiguous_keyword():
    concept = {"any_of": [{"all_of": ["当前", "台账"]}, "已授权的来源"]}
    assert concept_matches("当前可读的台账记录，暂未匹配到这项。", concept)
    assert concept_matches("已授权 的 来源", concept)
    assert not concept_matches("所有数据库里都没有", concept)


def test_changed_body_cannot_reuse_good_structured_facts_or_semantic_review():
    r = record(); r["output"]["text"] = "样机已到，此前测试尚未启动；现在测试完成了。"
    result = evaluate_attempt(case(), r)
    assert result["status"] == "failed"
    assert any("forbidden_assertion" in issue for issue in result["correctness"]["issues"])
    assert "source_grounded_semantic_review_missing" in result["correctness"]["issues"]


def test_nonexhaustive_rules_need_bound_source_review_not_model_self_grade():
    r = record(); r.pop("semantic_review")
    r["model_grade"] = {"correctness": 1.0, "style": 5, "method": "model_self"}
    result = evaluate_attempt(case(), r)
    assert result["status"] == "needs_review"
    assert result["style"]["model_advisory"]["style"] == 5
    assert result["style"]["human_score"] is None
    assert not result["release_gate_pass"]


def test_bad_source_review_cannot_get_automatic_semantic_acceptance():
    r = record(); r["semantic_review"]["source_version"] = "old"
    assert evaluate_attempt(case(), r)["status"] == "needs_review"
    r = record(); r["semantic_review"]["method"] = "independent_model"
    assert evaluate_attempt(case(), r)["status"] == "needs_review"


def test_assertions_are_bound_to_exact_body_and_source():
    c = case(); c["oracle"]["required_assertions"] = ["zero_send"]
    r = record(); r["assertions"] = [{"id": "zero_send", "status": "pass", "method": "independent_readback", "evidence_ref": "TEST:outbox"}]
    assert evaluate_attempt(c, r)["status"] == "failed"
    r["assertions"][0].update(output_sha256=output_hash(r["output"]["text"]), source_version="v1")
    assert evaluate_attempt(c, r)["status"] == "passed"


def test_all_missing_layers_keep_full_plan_denominator():
    report = evaluate_quality([case()], [])
    assert len(report["results"]) == 5
    for layer in LAYERS:
        assert report["layers"][layer]["planned_cases"] == 1
        assert report["layers"][layer]["case_pass_rate"] == 0
        assert report["layers"][layer]["status_counts"] == {"not_run": 1}
    assert not report["ci_pass"]


def test_offline_evidence_is_not_promoted_to_model_realtime_phone_human():
    report = evaluate_quality([case()], [record()])
    assert report["layers"]["offline"]["case_pass_count"] == 1
    for layer in LAYERS[1:]:
        assert report["layers"][layer]["status_counts"] == {"not_run": 1}
        assert evaluate_attempt(case(), record(layer=layer))["status"] == "failed"


def test_successful_retry_does_not_erase_timeout_or_inflate_first_attempt_accuracy():
    failed = record(attempt="first"); failed["outcome"]["status"] = "timeout"
    report = evaluate_quality([case()], [failed, record(attempt="retry")], layers=("offline",))
    summary = report["layers"]["offline"]
    assert summary["attempt_count"] == 2
    assert summary["case_pass_count"] == 0
    assert summary["first_attempt_pass_rate"] == 0
    assert summary["case_has_pass_count"] == 1
    assert summary["correct_completion_s"]["n"] == 1


def test_duplicate_attempt_ids_invalidate_both_preserving_records():
    report = evaluate_quality([case()], [record(), record()], layers=("offline",))
    assert len(report["results"]) == 2
    assert report["layers"]["offline"]["status_counts"] == {"failed": 2}
    assert not report["release_gate_pass"]


def test_fast_ack_is_not_substantive_or_completion_even_when_mislabeled():
    r = record("收到，我来查一下。")
    result = evaluate_attempt(case(), r)
    assert result["status"] == "failed"
    assert result["timing"]["ack_s"] == pytest.approx(.2)
    assert result["timing"]["first_substantive_s"] is None
    assert result["timing"]["completion_s"] is None


def test_mixed_clock_and_first_content_after_final_are_not_fast_samples():
    r = record(); r["timing"]["events"]["completed"] = {"at": 14, "clock_domain": "platform:other"}
    result = evaluate_attempt(case(), r)
    assert result["correctness"]["status"] == "pass"
    assert result["timing"]["completion_s"] is None
    assert not result["release_gate_pass"]
    r = record(); r["timing"]["events"]["first_substantive_visible"] = 15
    assert evaluate_attempt(case(), r)["timing"]["first_substantive_s"] is None


def test_accuracy_and_latency_are_separate_no_weighted_compensation():
    r = record(); r["timing"]["events"]["completed"] = 50
    result = evaluate_attempt(case(), r)
    assert result["status"] == "passed"
    assert result["correctness"]["status"] == "pass"
    assert result["timing"]["target_status"] == "missed"
    assert not result["release_gate_pass"]
    r["output"]["facts"]["test_started"] = True
    r["timing"]["events"]["completed"] = 12
    assert evaluate_attempt(case(), r)["status"] == "failed"


def test_only_output_bound_human_style_score_counts():
    r = record(); r["style_review"] = {"independent": True, "reviewer_id": "SYN-HUMAN", "identity_source": "TEST:identity",
        "evidence_ref": "TEST:feedback", "feedback": "这版比较自然", "score": 4,
        "output_sha256": output_hash(r["output"]["text"]), "source_version": "v1"}
    assert evaluate_attempt(case(), r)["style"]["human_score"] == 4
    r["style_review"]["output_sha256"] = "0" * 64
    assert evaluate_attempt(case(), r)["style"]["human_score"] is None


def test_small_sample_tails_are_unknown_not_zero():
    assert distribution([])["p50"] is None
    assert distribution([1, 2])["p95"] is None
    assert distribution(range(1, 21))["p95"] == 19
    assert distribution(range(1, 21))["p99"] is None
    assert distribution(range(1, 101))["p99"] == 99


def test_comparison_only_pairs_correct_same_condition_trials():
    a, b = record(), record(); b["timing"]["events"]["completed"] = 13
    report = compare_quality([case()], [a], [b], layers=("offline",))
    assert report["paired_completion_delta_s"] == {"n": 1, "p50": -1}
    b["cold_or_warm"] = "cold"
    assert compare_quality([case()], [a], [b], layers=("offline",))["paired_completion_delta_s"]["n"] == 0


def test_expected_silence_requires_outbound_evidence_not_a_fake_answer():
    c = case(); c["oracle"] = {"required_subtasks": ["ignore"], "required_assertions": ["no_reply_sent"],
        "allowed_kinds": ["silent"], "semantic_review_required": False, "latency_targets_s": {"processing_s": 6}}
    r = record(""); r["output"] = {"text": "", "kind": "silent", "completed_subtasks": ["ignore"]}
    assert evaluate_attempt(c, r)["status"] == "failed"
    r["assertions"] = [{"id": "no_reply_sent", "status": "pass", "method": "independent_readback",
        "evidence_ref": "TEST:outbox", "output_sha256": output_hash(""), "source_version": "v1"}]
    result = evaluate_attempt(c, r)
    assert result["status"] == "passed"
    assert result["timing"]["completion_s"] is None
    assert result["timing"]["processing_s"] == 4


def test_suite_covers_both_channels_without_claiming_independent_holdout():
    root = Path(__file__).resolve().parents[2]
    suite = load_suite(root / "evaluation/r7_cases.json")
    assert len(suite["cases"]) == 102
    assert len({c["family"] for c in suite["cases"]}) == 51
    assert all(c["status"] == "not_run" and c["split"] == "development" for c in suite["cases"])
    assert suite["holdout_status"] == "not_supplied"
    for family in {c["family"] for c in suite["cases"]}:
        values = [c for c in suite["cases"] if c["family"] == family]
        assert {c["channel"] for c in values} == {"group", "private"}
        assert len({c["split_group"] for c in values}) == 1
        assert all(c["input"]["text"] and c["required_ports"] for c in values)


def test_empty_or_unknown_layers_do_not_pass():
    with pytest.raises(QualityInputError):
        evaluate_quality([case()], [], layers=())
    r = record(); r["case_id"] = "UNKNOWN"
    report = evaluate_quality([case()], [r], layers=("offline",))
    assert report["invalid_records"]
    assert report["layers"]["offline"]["status_counts"] == {"not_run": 1}


def _tool(name):
    path = Path(__file__).resolve().parents[2] / "tools" / (name + ".py")
    spec = importlib.util.spec_from_file_location(name + "_test", path)
    module = importlib.util.module_from_spec(spec); spec.loader.exec_module(module)
    return module


def test_native_runner_repeats_appends_raw_records_and_keeps_oracle_from_worker(tmp_path):
    runner = _tool("evaluate_r7")
    seen = []
    def callback(execution_case, source_fixture, *, layer):
        assert "oracle" not in execution_case
        seen.append(execution_case["evaluation_attempt_id"])
        r = record(); r["attempt_id"] = execution_case["evaluation_attempt_id"]
        return r
    raw = tmp_path / "raw.jsonl"
    results = runner.run_adapter({"cases": [case()]}, callback, "offline", record_output=raw)
    assert len(results) == len(seen) == len(set(seen)) == 3
    assert len(raw.read_text().splitlines()) == 3
    assert all(r["runner_observation"]["elapsed_is_not_user_visible_latency"] for r in results)


def test_native_runner_exception_does_not_copy_credentials_into_report(tmp_path):
    runner = _tool("evaluate_r7")
    def callback(*args, **kwargs):
        raise TimeoutError("secret_token=SHOULD_NEVER_APPEAR")
    raw = tmp_path / "raw.jsonl"
    results = runner.run_adapter({"cases": [case()]}, callback, "offline", repetitions=2, record_output=raw)
    assert all(r["outcome"] == {"status": "timeout", "reason": "TimeoutError"} for r in results)
    assert "SHOULD_NEVER_APPEAR" not in raw.read_text()


def test_optional_phone_and_human_sampling_does_not_force_all_cases_to_be_reviewed():
    c = case(); c["required_layers"] = ["offline"]
    report = evaluate_quality([c], [record()])
    assert report["ci_pass"] is True
    assert report["layers"]["human"]["required_cases"] == 0
    assert report["layers"]["human"]["status_counts"] == {"not_run": 1}


def test_phone_time_never_uses_sdk_creation_time_as_device_display_time():
    r = record(layer="phone")
    r["timing"]["events"]["final_visible"] = 14
    result = evaluate_attempt(case(), r)
    assert result["timing"]["endpoint"] == "phone_visible"
    assert result["timing"]["completion_s"] is None


def test_controlled_adapter_uses_actual_source_and_leaves_complex_cases_unrun(tmp_path, monkeypatch):
    adapter = _tool("r7_offline_adapter")
    root = Path(__file__).resolve().parents[2]
    suite = load_suite(root / "evaluation/r7_cases.json")
    monkeypatch.setenv("AM_R7_EVIDENCE_DIR", str(tmp_path))
    c = deepcopy(suite["cases"][0]); c.pop("oracle")
    r = adapter.run_case(c, suite["source_fixture"])
    assert r["outcome"]["status"] == "completed"
    assert "尚未启动" in r["output"]["text"]
    assert r["execution_provenance"]["actual_component_run"] is True
    assert r["execution_provenance"]["actual_assistant_intake"] is False
    assert Path(r["execution_provenance"]["execution_ref"]).is_file()
    c = next(c for c in suite["cases"] if c["family"] == "document_draft")
    assert adapter.run_case(c, suite["source_fixture"])["outcome"]["status"] == "not_run"


def test_closed_source_validator_rejects_changed_output_even_with_correct_result_ids():
    adapter = _tool("r7_offline_adapter")
    snapshot = {"rows": [{"project_ref": "p1", "title": "样例甲", "项目进展": "寿命测试尚未启动。",
        "待办": "", "关键日期": "", "人员与职责": ""}], "complete": True}
    result = {"status": "found", "project_ids": ["p1"], "body": "样例甲（按台账记录）\n进展：寿命测试已完成。"}
    ok, reason = adapter._independent_source_check(result, snapshot, ["p1"], "样例甲进展")
    assert not ok and reason == "field_not_verbatim_from_selected_source"


def test_refresh_acceptance_requires_completed_refresh_and_current_version_even_with_same_good_body(tmp_path, monkeypatch):
    adapter = _tool("r7_offline_adapter")
    suite = load_suite(Path(__file__).resolve().parents[2] / "evaluation/r7_cases.json")
    c = next(c for c in suite["cases"] if c["case_id"] == "R7-08-private")
    monkeypatch.setenv("AM_R7_EVIDENCE_DIR", str(tmp_path))
    r = adapter.run_case(c, suite["source_fixture"])
    assert "刷新" not in r["output"]["text"]
    assert evaluate_attempt(c, r)["status"] == "passed"
    no_refresh = deepcopy(r)
    next(a for a in no_refresh["assertions"] if a["id"] == "refresh_completed")["status"] = "fail"
    assert evaluate_attempt(c, no_refresh)["status"] == "failed"
    old_version = deepcopy(r)
    next(a for a in old_version["assertions"] if a["id"] == "refreshed_source_version_bound")["source_version"] = "old-version"
    assert evaluate_attempt(c, old_version)["status"] == "failed"


def test_first_substantive_timestamp_needs_its_actual_content_not_final_answer_substitution():
    r = record(); r["output"].pop("first_substantive_text")
    assert evaluate_attempt(case(), r)["timing"]["first_substantive_s"] is None
    r = record(); r["output"]["first_substantive_text"] = "收到，我来查一下。"
    assert evaluate_attempt(case(), r)["timing"]["first_substantive_s"] is None
    r["output"]["first_substantive_text"] = "测试完成了。"
    assert evaluate_attempt(case(), r)["timing"]["first_substantive_s"] is None
