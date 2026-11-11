"""Behavioral checks for evidence-driven configuration changes, not model UAT."""
from copy import deepcopy
import json

import pytest

from am_gateway.assistant.evolution import EvolutionEngine, ReplayBudget
from am_gateway.assistant.improvement import ImprovementJournal
from am_gateway.assistant.quality import SCHEMA_VERSION, output_hash
from am_gateway.models import ContractError, dumps, key
from am_gateway.store import Store


class Clock:
    now = 1000.0

    def __call__(self):
        return self.now


def cases(prefix, count, split):
    return [{"case_id": f"{prefix}-{n}", "channel": "group" if n % 2 else "private",
             "intent": "project_overview", "source_version": "test-source-7",
             "split": split, "split_group": f"{prefix}-task-{n}",
             "input": {"text": f"查询合成任务{prefix}{n}的状态。", "actor_id": "TEST-OWNER"},
             "oracle": {"required_facts": {"state": "pending"},
                        "required_subtasks": ["status"], "semantic_review_required": True,
                        "latency_targets_s": {"completion_s": 10.0}, "style_max_chars": 600}}
            for n in range(count)]


def make_record(case, context, *, seconds=2.0, state="pending", text="合成任务仍待验收。", thermal="warm"):
    """A controlled source-verification fixture; not genuine business evidence."""
    return {"schema_version": SCHEMA_VERSION, "case_id": case["case_id"],
            "channel": case["channel"], "intent": context["intent"],
            "source_version": case["source_version"], "attempt_id": context["attempt_id"],
            "layer": context["layer"], "cold_or_warm": thermal,
            "outcome": {"status": "completed"},
            "output": {"kind": "answer", "text": text, "facts": {"state": state},
                       "completed_subtasks": ["status"]},
            "semantic_review": {"status": "pass", "method": "source_check", "independent": True,
                "output_sha256": output_hash(text), "source_version": case["source_version"],
                "evidence_ref": "unit-test-source-check:" + case["case_id"]},
            "execution_provenance": {"actual_assistant_run": True},
            "timing": {"clock_domain": "controlled-test-clock", "events": {
                "received": 100.0, "completed": 100.0 + seconds,
                "first_substantive_visible": 100.0 + min(seconds, 1.0)}}}


@pytest.fixture
def setup(tmp_path):
    clock = Clock()
    store = Store(tmp_path / "evolution.sqlite", clock=clock)
    suites = {"dev:v1": cases("dev", 2, "development"),
              "held:v1": cases("held", 20, "holdout")}
    invocations = []
    def worker(*, case, config, context):
        assert "oracle" not in case and "expected_output" not in case
        invocations.append(deepcopy(context))
        return make_record(case, context, seconds=4.0 if config["reply.layout"] == "paragraph" else 2.0)
    engine = EvolutionEngine(store, "owner", replayer=worker,
        suite_loader=lambda ref: deepcopy(suites[ref]), known_model_ids=("qwen", "codex"),
        enabled_scopes=("owner/global",), monotonic=clock)
    engine.register_baseline("owner/global", {"reply.layout": "paragraph", "reply.max_chars": 600,
                                             "reply.cache_ttl_seconds": 20})
    engine.freeze_suite("dev:v1", suites["dev:v1"], split="development")
    engine.freeze_suite("held:v1", suites["held:v1"], split="holdout", independence_ref="host:test-distinct-tasks")
    return engine, store, clock, suites, invocations


def candidate(engine, **changes):
    values = {"scope": "owner/global", "patch": {"reply.layout": "compact"},
              "development_ref": "dev:v1", "holdout_ref": "held:v1"}
    values.update(changes)
    return engine.create_candidate(**values)


def test_faster_correct_candidate_activates_only_local_configuration_and_rolls_back(setup):
    engine, store, _, _, calls = setup
    item = candidate(engine)
    report = engine.replay(item["candidate_id"])
    assert report["eligible"] is True
    assert report["calls_used"] == 44
    assert report["latency"]["baseline"]["p95"] == 4
    assert report["latency"]["candidate"]["p95"] == 2
    assert report["production_deployment_proven"] is False
    assert all(c["effects_allowed"] is False for c in calls)
    activated = engine.activate(item["candidate_id"], expected_version=1)
    assert activated["version"] == 2
    assert engine.active_config("owner/global")["config"]["reply.layout"] == "compact"
    restored = engine.rollback("owner/global", expected_version=2, reason="host_observed_new_regression")
    assert restored["version"] == 3 and restored["restored_from_version"] == 1
    assert engine.active_config("owner/global")["config"]["reply.layout"] == "paragraph"
    assert restored["business_effects_rolled_back"] is False
    for table in ("gw_items", "gw_jobs", "gw_facts", "gw_outbox"):
        assert store.rows("SELECT * FROM " + table) == []


def test_unbound_worker_does_not_produce_a_pass_or_zero_latency(setup):
    engine, store, _, _, _ = setup
    engine.replayer = None
    item = candidate(engine)
    report = engine.replay(item["candidate_id"])
    assert report["state"] == "unbound" and report["eligible"] is False
    assert report["comparisons"] == {}
    assert store.rows("SELECT * FROM r7_evo_trials") == []
    with pytest.raises(ContractError, match="completed replay"):
        engine.activate(item["candidate_id"], expected_version=1)


def test_shadow_scope_never_activates_even_a_good_candidate(setup):
    engine, _, _, _, _ = setup
    engine.enabled_scopes = frozenset()
    item = candidate(engine)
    assert engine.replay(item["candidate_id"])["eligible"]
    assert engine.activate(item["candidate_id"], expected_version=1)["state"] == "shadow_only"
    assert engine.active_config("owner/global")["version"] == 1


def test_development_only_can_make_progress_but_not_claim_generalization(setup):
    engine, store, _, _, _ = setup
    item = candidate(engine, holdout_ref=None)
    report = engine.replay(item["candidate_id"])
    assert report["state"] == "needs_holdout" and report["eligible"] is False
    assert report["comparisons"]["development"]["candidate"]["ci_pass"]
    assert report["calls_used"] == 4
    assert len(store.rows("SELECT * FROM r7_evo_trials")) == 4


def test_faster_wrong_fact_cannot_compensate_for_accuracy(setup):
    engine, _, _, _, _ = setup
    def wrong(*, case, config, context):
        return make_record(case, context, seconds=.1 if context["variant"] == "candidate" else 5,
                           state="finished" if context["variant"] == "candidate" else "pending")
    engine.replayer = wrong
    item = candidate(engine)
    report = engine.replay(item["candidate_id"])
    assert report["state"] == "rejected" and report["eligible"] is False
    assert report["calls_used"] == 4
    assert report["comparisons"]["holdout_not_run_reason"] == "development_gate_or_budget"
    with pytest.raises(ContractError, match="did not pass"):
        engine.activate(item["candidate_id"], expected_version=1)


def test_false_improvement_identical_results_is_rejected(setup):
    engine, _, _, _, _ = setup
    engine.replayer = lambda **kw: make_record(kw["case"], kw["context"], seconds=2)
    report = engine.replay(candidate(engine)["candidate_id"])
    assert report["eligible"] is False
    assert "no_measured_improvement" in report["reasons"]


def test_missing_case_record_cannot_inflate_accuracy(setup):
    engine, _, _, _, _ = setup
    def missing(*, case, config, context):
        if case["case_id"] == "held-19" and context["variant"] == "candidate":
            return None
        return make_record(case, context, seconds=4 if context["variant"] == "baseline" else 2)
    engine.replayer = missing
    report = engine.replay(candidate(engine)["candidate_id"])
    assert report["eligible"] is False
    summary = report["comparisons"]["holdout"]["candidate"]["layers"]["offline"]
    assert summary["planned_cases"] == 20 and summary["case_pass_count"] == 19
    assert summary["status_counts"]["error"] == 1


def test_budget_retains_unrun_cases_and_does_not_retry_forever(setup):
    engine, store, _, _, calls = setup
    item = candidate(engine, budget=ReplayBudget(max_calls=1, max_seconds=20, per_call_seconds=5))
    report = engine.replay(item["candidate_id"])
    assert report["state"] == "budget_exhausted" and len(calls) == 1
    records = store.rows("SELECT record_json FROM r7_evo_trials")
    assert len(records) == 4
    assert sum(json.loads(r["record_json"])["outcome"]["status"] == "not_run" for r in records) == 3


def test_late_worker_success_is_timeout_not_improvement(setup):
    engine, _, clock, _, _ = setup
    def late(*, case, config, context):
        clock.now += 2
        return make_record(case, context, seconds=.01)
    engine.replayer = late
    item = candidate(engine, budget=ReplayBudget(max_calls=50, max_seconds=100, per_call_seconds=1))
    report = engine.replay(item["candidate_id"])
    rows = report["comparisons"]["development"]["candidate"]["results"]
    assert all(r["status"] == "timeout" for r in rows)
    assert report["eligible"] is False
    assert rows[0]["record"]["physical_cancellation_proven"] is False


def test_suite_tampering_is_rejected_before_any_replay(setup):
    engine, _, _, suites, calls = setup
    item = candidate(engine)
    suites["dev:v1"][0]["oracle"]["required_facts"]["state"] = "finished"
    with pytest.raises(ContractError, match="content changed"):
        engine.replay(item["candidate_id"])
    assert calls == []


def test_suite_tampering_after_success_blocks_activation(setup):
    engine, _, _, suites, _ = setup
    item = candidate(engine)
    assert engine.replay(item["candidate_id"])["eligible"]
    suites["held:v1"][0]["input"]["text"] = "replaced heldout question"
    with pytest.raises(ContractError, match="content changed"):
        engine.activate(item["candidate_id"], expected_version=1)
    assert engine.active_config("owner/global")["version"] == 1


def test_frozen_reference_cannot_be_overwritten(setup):
    engine, _, _, suites, _ = setup
    revised = deepcopy(suites["held:v1"])
    revised[0]["source_version"] = "new-source"
    with pytest.raises(ContractError, match="version conflict"):
        engine.freeze_suite("held:v1", revised, split="holdout", independence_ref="host:new-claim")


@pytest.mark.parametrize("field", ["case_id", "input", "split_group"])
def test_development_leakage_cannot_be_relabeled_holdout(setup, field):
    engine, _, _, suites, _ = setup
    held = deepcopy(suites["held:v1"])
    held[0][field] = deepcopy(suites["dev:v1"][0][field])
    engine.freeze_suite("contaminated", held, split="holdout", independence_ref="host:claimed-independent")
    with pytest.raises(ContractError, match="contaminated"):
        candidate(engine, holdout_ref="contaminated")


def test_holdout_requires_explicit_independence_attestation(setup):
    engine, _, _, suites, _ = setup
    with pytest.raises(ContractError, match="independence"):
        engine.freeze_suite("not-independent", suites["held:v1"], split="holdout")


def test_source_channel_or_attempt_mismatch_is_retained_as_failure(setup):
    engine, _, _, _, _ = setup
    def crossed(*, case, config, context):
        record = make_record(case, context)
        if context["variant"] == "candidate":
            record["source_version"] = "another-project-snapshot"
        return record
    engine.replayer = crossed
    report = engine.replay(candidate(engine)["candidate_id"])
    assert report["eligible"] is False
    row = report["comparisons"]["development"]["candidate"]["results"][0]
    assert row["record"]["source_version"] == "another-project-snapshot"
    assert row["record"]["reason"] == "worker_identity_mismatch"


def test_different_cold_warm_conditions_cannot_prove_speedup(setup):
    engine, _, _, _, _ = setup
    def different(*, case, config, context):
        return make_record(case, context, seconds=4 if context["variant"] == "baseline" else 2,
                           thermal="cold" if context["variant"] == "baseline" else "warm")
    engine.replayer = different
    report = engine.replay(candidate(engine)["candidate_id"])
    assert not report["eligible"]
    assert not report["comparisons"]["development"]["paired"][0]["same_conditions"]


def test_too_few_tail_samples_are_not_reported_as_p95_success(setup):
    engine, _, _, suites, _ = setup
    suites["tiny"] = cases("tiny", 3, "holdout")
    engine.freeze_suite("tiny", suites["tiny"], split="holdout", independence_ref="host:small-independent-set")
    report = engine.replay(candidate(engine, holdout_ref="tiny")["candidate_id"])
    assert report["state"] == "needs_evidence" and not report["eligible"]
    assert report["latency"]["candidate"]["p95"] is None


def test_changed_model_route_requires_real_model_evidence(setup):
    engine, _, _, _, _ = setup
    item = candidate(engine, patch={"reply.layout": "compact", "reply.model_id": "qwen"})
    report = engine.replay(item["candidate_id"])
    assert report["state"] == "needs_evidence"
    assert "model_setting_change_requires_actual_model_replay" in report["reasons"]


@pytest.mark.parametrize("patch", [
    {"sender_id": "other"}, {"business.authorization": True}, {"reply.model_id": "unavailable-model"},
    {"reply.max_chars": 1201}, {"reply.max_chars": True}, {"reply.model_budget_seconds": 30},
    {"reply.cache_ttl_seconds": 61}, {"reply.cache_ttl_seconds": float("nan")},
])
def test_candidates_cannot_modify_business_authority_or_unbounded_settings(setup, patch):
    engine, _, _, _, _ = setup
    with pytest.raises(ContractError):
        candidate(engine, patch=patch)


def test_cache_cannot_outlive_host_source_freshness(setup):
    engine, _, _, _, _ = setup
    engine.source_freshness = 25
    with pytest.raises(ContractError, match="freshness"):
        candidate(engine, patch={"reply.cache_ttl_seconds": 30})


def test_compare_and_swap_prevents_stale_candidate_overwriting_new_config(setup):
    engine, _, _, _, _ = setup
    first = candidate(engine)
    second = candidate(engine, patch={"reply.layout": "compact", "reply.max_chars": 400})
    assert engine.replay(first["candidate_id"])["eligible"]
    assert engine.replay(second["candidate_id"])["eligible"]
    engine.activate(first["candidate_id"], expected_version=1)
    with pytest.raises(ContractError, match="version conflict"):
        engine.activate(second["candidate_id"], expected_version=1)
    with pytest.raises(ContractError, match="version conflict"):
        engine.rollback("owner/global", expected_version=3, reason="wrong version")


def test_candidate_config_and_report_hashes_are_checked(setup):
    engine, store, _, _, _ = setup
    item = candidate(engine)
    report = engine.replay(item["candidate_id"])
    with store.transaction() as db:
        forged = {**report, "calls_used": 0}
        db.execute("UPDATE r7_evo_runs SET report_json=? WHERE run_id=?", (dumps(forged), report["run_id"]))
    with pytest.raises(ContractError, match="integrity mismatch"):
        engine.activate(item["candidate_id"], expected_version=1)


def test_journal_proposer_receives_raw_feedback_and_development_only(setup):
    engine, store, _, suites, _ = setup
    journal = ImprovementJournal(store, "owner")
    journal.record_feedback("f1", actor_id="owner", text="回复太长，语气再自然一点。", source_ref="local:feedback:f1")
    proposal = journal.analyze_once()
    seen = []
    def propose(**kwargs):
        seen.append(kwargs)
        return {"patch": {"reply.layout": "compact"}, "rationale": "缩短结构并回放核对"}
    engine.proposer = propose
    item = engine.propose_from_journal(journal, scope="owner/global", development_ref="dev:v1",
                                       holdout_ref="held:v1")
    assert item["proposal_id"] == proposal["proposal_id"]
    assert seen[0]["feedback"][0]["text"] == "回复太长，语气再自然一点。"
    assert seen[0]["development_cases"] == suites["dev:v1"]
    assert "held-" not in dumps(seen[0]) and "holdout" not in seen[0]
    assert engine.propose_from_journal(journal, scope="owner/global", development_ref="dev:v1")["state"] == "idle"


def test_no_proposer_binding_returns_unbound_and_no_candidate(setup):
    engine, store, _, _, _ = setup
    journal = ImprovementJournal(store, "owner")
    journal.record_feedback("f1", actor_id="owner", text="太慢。")
    journal.analyze_once()
    assert engine.propose_from_journal(journal, scope="owner/global", development_ref="dev:v1")["state"] == "unbound"
    assert store.rows("SELECT * FROM r7_evo_candidates") == []


def test_first_failure_cannot_be_hidden_by_a_successful_retry(setup):
    engine, store, _, _, _ = setup
    original = engine.replayer
    engine.replayer = lambda **kw: None
    item = candidate(engine)
    failed = engine.replay(item["candidate_id"])
    engine.replayer = original
    passed = engine.replay(item["candidate_id"])
    assert not failed["eligible"] and not passed["eligible"]
    assert passed["comparisons"]["holdout"]["candidate"]["ci_pass"]
    assert "same_candidate_has_prior_failed_attempts" in passed["reasons"]
    runs = store.rows("SELECT state FROM r7_evo_runs WHERE candidate_id=? ORDER BY rowid", (item["candidate_id"],))
    assert [r["state"] for r in runs] == ["rejected", "rejected"]
    assert len(store.rows("SELECT * FROM r7_evo_trials WHERE run_id=?", (failed["run_id"],))) == 4


def test_state_survives_restart_without_recreating_baseline(setup):
    engine, store, clock, suites, _ = setup
    item = candidate(engine)
    assert engine.replay(item["candidate_id"])["eligible"]
    restarted = EvolutionEngine(store, "owner", suite_loader=lambda ref: suites[ref],
                               enabled_scopes=("owner/global",), monotonic=clock)
    assert restarted.active_config("owner/global")["version"] == 1
    assert restarted.activate(item["candidate_id"], expected_version=1)["version"] == 2
    assert restarted.status()["candidate_counts"]["active"] == 1


def test_different_runtime_cannot_activate_old_evidence(setup):
    engine, store, clock, suites, _ = setup
    item = candidate(engine)
    assert engine.replay(item["candidate_id"])["eligible"]
    changed = EvolutionEngine(store, "owner", suite_loader=lambda ref: suites[ref],
                              enabled_scopes=("owner/global",), runtime_revision="new-code-hash", monotonic=clock)
    with pytest.raises(ContractError, match="runtime changed"):
        changed.activate(item["candidate_id"], expected_version=1)


def test_unbound_candidate_resumes_when_worker_is_connected_without_new_feedback(setup):
    engine, store, _, _, _ = setup
    journal = ImprovementJournal(store, "owner")
    item = candidate(engine)
    worker = engine.replayer
    engine.replayer = None
    arguments = dict(scope="owner/global", development_ref="dev:v1", holdout_ref="held:v1")
    assert engine.one_cycle(journal, **arguments)["state"] == "unbound"
    engine.replayer = worker
    report = engine.one_cycle(journal, **arguments)
    assert report["candidate_id"] == item["candidate_id"]
    assert report["activation"]["activated"] is True
    assert len(store.rows("SELECT * FROM r7_evo_candidates")) == 1


def test_missing_required_layers_are_evidence_gap_not_fake_timing_failure(setup):
    engine, _, _, suites, _ = setup
    suites["extra-layer"] = cases("extra", 20, "holdout")
    for case in suites["extra-layer"]:
        case["required_layers"] = ["offline", "phone"]
    engine.freeze_suite("extra-layer", suites["extra-layer"], split="holdout", independence_ref="host:extra-layer")
    report = engine.replay(candidate(engine, holdout_ref="extra-layer")["candidate_id"])
    assert report["state"] == "needs_evidence"
    assert "candidate_required_layers_unverified" in report["reasons"]
    assert "candidate_frozen_latency_targets_not_met" not in report["reasons"]


def test_no_change_feedback_does_not_repeat_model_calls_each_background_tick(setup):
    engine, store, _, _, _ = setup
    journal = ImprovementJournal(store, "owner")
    journal.record_feedback("f1", actor_id="owner", text="这次回复很好。")
    journal.analyze_once()
    calls = []
    def no_change(**kwargs):
        calls.append(1)
        return {"patch": {}, "rationale": "保留当前配置，暂无需要变更的依据。"}
    engine.proposer = no_change
    args = dict(scope="owner/global", development_ref="dev:v1")
    assert engine.one_cycle(journal, **args)["state"] == "no_change"
    assert engine.one_cycle(journal, **args)["state"] == "idle"
    assert len(calls) == 1
    assert engine.status()["proposal_attempt_counts"] == {"no_change": 1}


def test_invalid_proposals_have_bounded_retries_and_keep_each_failure(setup):
    engine, store, _, _, _ = setup
    journal = ImprovementJournal(store, "owner")
    journal.record_feedback("f1", actor_id="owner", text="回复太慢。")
    journal.analyze_once()
    engine.proposer = lambda **kwargs: {"patch": {"sender_id": "do-not-allow"}}
    args = dict(scope="owner/global", development_ref="dev:v1")
    assert [engine.one_cycle(journal, **args)["state"] for _ in range(4)] == [
        "proposal_rejected", "proposal_rejected", "proposal_rejected", "idle"]
    assert engine.status()["proposal_attempt_counts"] == {"rejected": 3}
    assert store.rows("SELECT * FROM r7_evo_candidates") == []


def test_single_layer_holdout_cannot_bypass_development_system_requirements(setup):
    engine, _, _, suites, _ = setup
    engine.layer = "real_model"
    suites["multi-development"] = cases("multi-dev", 2, "development")
    for case in suites["multi-development"]:
        case["required_layers"] = ["offline", "real_model", "realtime", "phone", "human"]
    suites["single-layer-holdout"] = cases("single-held", 20, "holdout")
    for case in suites["single-layer-holdout"]:
        case["required_layers"] = ["real_model"]
    engine.freeze_suite("multi-development", suites["multi-development"], split="development")
    engine.freeze_suite("single-layer-holdout", suites["single-layer-holdout"], split="holdout",
                        independence_ref="unit-test:separate-heldout")
    def worker(*, case, config, context):
        record = make_record(case, context, seconds=4 if context["variant"] == "baseline" else 2)
        record["execution_provenance"].update(actual_model_call=True,
                                               model_run_ref="controlled-unit-test-fixture-only")
        return record
    engine.replayer = worker
    item = candidate(engine, development_ref="multi-development", holdout_ref="single-layer-holdout")
    report = engine.replay(item["candidate_id"])
    assert report["comparisons"]["development"]["candidate"]["ci_pass"]
    assert not report["comparisons"]["development"]["candidate"]["covers_all_required_layers"]
    assert report["state"] == "needs_evidence" and not report["eligible"]
    assert report["comparisons"]["holdout_not_run_reason"] == "development_required_layers_unverified"
    assert report["calls_used"] == 4
    with pytest.raises(ContractError, match="did not pass"):
        engine.activate(item["candidate_id"], expected_version=1)
    assert engine.active_config("owner/global")["version"] == 1
