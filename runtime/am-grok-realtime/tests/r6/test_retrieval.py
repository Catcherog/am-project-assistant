from copy import deepcopy

import pytest

from am_gateway.assistant.retrieval import LedgerIndex, eligible_question


def project(pid, code, name, **changes):
    row = {"key": pid, "project_ref": pid, "business_id": code,
           "title": code + "｜" + name, "aliases": [name], "identity_status": "verified",
           "项目进展": "测试已标记完成，实际验收仍待核验。",
           "关键日期": "预计 11 月 20 日完成；以测试通过为前提。",
           "待办": "等待工程师确认报告；确认后安排评审。", "人员与职责": "示例同事甲负责软件测试。"}
    row.update(changes)
    return row


def snapshot(*rows, **changes):
    value = {"version": "v574", "source_revision": 574, "source": "demo-ledger",
             "observed_at": 1794286800, "complete": True, "rows": list(rows),
             "stale": False, "refresh_error": None, "cache_hit": False}
    value.update(changes)
    return value


@pytest.fixture
def source():
    return snapshot(project("p1", "PT-2099-003", "示例传感器"),
                    project("p2", "PT-2099-004", "示例平台"))


@pytest.mark.parametrize("question,expected", [
    ("说说示例传感器进展，卡点和下一步", "p1"),
    ("示例平台现在进度怎样", "p2"),
    ("请帮我看看示例平台的进度", "p2"),
    ("示例平台卡点有哪些", "p2"),
    ("示例平台负责人是谁", "p2"),
])
def test_natural_question_matches_recorded_name_without_a_model(source, question, expected):
    result = LedgerIndex(source).query(question, ["p1", "p2"])
    assert result["status"] == "found"
    assert result["project_ids"] == [expected]
    assert result["source_version"] == "v574"
    assert result["source_revision"] == 574
    assert result["source_ref"] == "demo-ledger"
    assert not result["requires_refresh"]


def test_summary_preserves_plan_reported_completion_and_missing_fact(source):
    result = LedgerIndex(source).query("说说示例平台进展，卡点和下一步", ["p2"])
    for expected in ("测试已标记完成，实际验收仍待核验。", "预计 11 月 20 日完成；以测试通过为前提。",
                     "等待工程师确认报告；确认后安排评审。"):
        assert expected in result["body"]
    assert "已核实完成" not in result["body"]
    assert "进展：" in result["body"] and "计划与日期：" in result["body"]
    assert "卡点／风险：" in result["body"] and "下一步／待办：" in result["body"]


def test_missing_blocker_is_not_rendered_as_no_risk(source):
    source["rows"][1].update({"项目进展": "试验进行中。", "待办": "收集数据。"})
    result = LedgerIndex(source).query("示例平台卡点有哪些", ["p2"])
    assert "台账未明确记录；不能据此判断没有风险。" in result["body"]


def test_one_field_query_does_not_dump_all_columns(source):
    result = LedgerIndex(source).query("示例平台负责人是谁", ["p2"])
    assert "示例同事甲负责软件测试。" in result["body"]
    assert "预计 11 月" not in result["body"]
    assert "测试已标记完成" not in result["body"]


def test_snapshot_and_grants_are_not_mutated(source):
    old = deepcopy(source)
    index = LedgerIndex(source)
    source["rows"][0]["项目进展"] = "later mutation"
    allowed = ["p1"]
    result = index.query("示例传感器进展", allowed)
    assert old["rows"][0]["项目进展"] in result["body"]
    assert allowed == ["p1"]


def test_name_similarity_cannot_grant_access(source):
    result = LedgerIndex(source).query("示例平台项目进度怎样", ["示例平台"])
    assert result["status"] == "unbound"
    assert not result["matches"]
    assert "PT-2099-004" not in result["body"]


def test_explicit_binding_uses_stable_row_reference_only(source):
    index = LedgerIndex(source, {"authority-project": "p2"})
    assert index.query("示例平台进度", ["authority-project"])["project_ids"] == ["authority-project"]
    bad = LedgerIndex(source, {"authority-project": "示例平台"})
    assert bad.query("示例平台进度", ["authority-project"])["status"] == "unbound"


def test_business_id_binding_normalizes_case_and_fullwidth_digits(source):
    result = LedgerIndex(source).query("ｐｔ－２０９９－００４目前进展", ["pt-2099-004"])
    assert result["status"] == "found"
    assert result["project_ids"] == ["pt-2099-004"]


def test_request_id_normalizes_leading_zeroes():
    src = snapshot(project("r75", "需求75", "寿命监控"))
    result = LedgerIndex(src).query("需求０７５进度", ["需求 75"])
    assert result["status"] == "found"


def test_hidden_project_is_never_listed_as_a_choice(source):
    result = LedgerIndex(source).query("示例平台项目进度怎样", ["p1"])
    assert result["status"] == "no_match"
    assert result["matches"] == []
    assert "PT-2099-004" not in result["body"]
    assert "示例平台" not in result["body"]


def test_current_grants_are_applied_on_every_query(source):
    index = LedgerIndex(source)
    assert index.query("示例平台进度", ["p2"])["status"] == "found"
    second = index.query("示例平台项目进度", ["p1"])
    assert second["status"] == "no_match"
    assert second["matches"] == []


@pytest.mark.parametrize("changes,status", [
    ({"stale": True}, "incomplete"),
    ({"complete": False}, "incomplete"),
    ({"refresh_error": "network unavailable"}, "unavailable"),
    ({"version": ""}, "incomplete"),
    ({"rows": None}, "incomplete"),
])
def test_source_problem_cannot_be_reported_as_project_absence(source, changes, status):
    source.update(changes)
    result = LedgerIndex(source).query("PT-2099-006进展", ["p1", "p2"])
    assert result["status"] == status
    assert result["requires_refresh"]
    assert "不存在" not in result["body"] and "没查到" not in result["body"]


def test_zero_result_from_cache_requests_one_refresh(source):
    source["cache_hit"] = True
    result = LedgerIndex(source).query("PT-2099-006进展", ["p1", "p2"])
    assert result["status"] == "no_match" and result["requires_refresh"]
    source["cache_hit"] = False
    result = LedgerIndex(source).query("PT-2099-006进展", ["p1", "p2"])
    assert result["status"] == "no_match" and not result["requires_refresh"]


def test_explicit_unknown_id_never_falls_back_to_name_or_focus(source):
    result = LedgerIndex(source).query("PT-2099-006示例平台进展", ["p1", "p2"], focus_project_id="p2")
    assert result["status"] == "no_match"
    assert not result["matches"]
    assert result["unresolved_ids"] == ["PT-2099-006"]


def test_multi_identifier_request_accounts_for_missing_requested_project(source):
    result = LedgerIndex(source).query("PT-2099-003、PT-2099-006分别进展怎样", ["p1", "p2"])
    assert result["status"] == "incomplete"
    assert result["project_ids"] == ["p1"]
    assert result["unresolved_ids"] == ["PT-2099-006"]
    assert "示例传感器" in result["body"] and "PT-2099-006" in result["body"]
    assert not result["requires_refresh"]


def test_multi_identifier_request_returns_each_known_project_once(source):
    result = LedgerIndex(source).query("PT-2099-003和PT-2099-004的进展", ["p1", "p2"])
    assert result["status"] == "found"
    assert result["project_ids"] == ["p1", "p2"]


def test_two_explicit_project_names_can_be_answered_together(source):
    result = LedgerIndex(source).query("示例平台和示例传感器分别进度怎样", ["p1", "p2"])
    assert result["status"] == "found"
    assert set(result["project_ids"]) == {"p1", "p2"}


def test_named_second_subject_is_not_silently_dropped_when_not_readable(source):
    result = LedgerIndex(source).query("示例平台和示例传感器分别进度怎样", ["p2"])
    assert result["status"] == "incomplete"
    assert result["project_ids"] == ["p2"]
    assert "示例传感器" not in result["body"]
    assert "另一项目" in result["body"]
    assert not result["requires_refresh"]


def test_known_name_does_not_swallow_unknown_model(source):
    result = LedgerIndex(source).query("示例平台和HD999的进度", ["p1", "p2"])
    assert result["status"] == "incomplete"
    assert result["unresolved_ids"] == ["hd999"]


def test_field_conjunction_is_not_mistaken_for_a_missing_project(source):
    result = LedgerIndex(source).query("示例平台的进展和计划", ["p2"])
    assert result["status"] == "found"


def test_shared_alias_is_ambiguous_not_first_row(source):
    for row in source["rows"]:
        row["aliases"].append("实验平台")
    result = LedgerIndex(source).query("实验平台进度怎样", ["p1", "p2"])
    assert result["status"] == "ambiguous"
    assert len(result["matches"]) == 2
    assert "实际验收仍待核验" not in result["body"]


def test_shared_model_cannot_silently_select_one_project():
    src = snapshot(project("p1", "PT-2099-003", "DemoDrive80I减速机导入"),
                   project("p2", "PT-2099-004", "DemoDrive80I刹车线整改"))
    result = LedgerIndex(src).query("DemoDrive80I进展怎样", ["p1", "p2"])
    assert result["status"] == "ambiguous"
    assert set(result["project_ids"]) == {"p1", "p2"}


@pytest.mark.parametrize("question", ["DEMOH140进展怎样", "DEMOX14进展怎样", "DemoDrive80II进展怎样"])
def test_model_token_does_not_match_a_different_identifier(question):
    src = snapshot(project("p1", "PT-2099-003", "DEMOH14减速机导入"),
                   project("p2", "PT-2099-004", "DemoDrive80I刹车线整改"))
    result = LedgerIndex(src).query(question, ["p1", "p2"])
    assert result["status"] != "found"


def test_focus_is_scoped_and_only_used_for_a_real_followup(source):
    index = LedgerIndex(source)
    assert index.query("那下一步呢", ["p2"], focus_project_id="p2")["project_ids"] == ["p2"]
    result = index.query("另一个项目进展如何", ["p1", "p2"], focus_project_id="p2")
    assert result["status"] != "found"
    assert index.query("那下一步呢", ["p1"], focus_project_id="p2")["status"] != "found"
    assert index.query("那另一个项目进展呢", ["p1", "p2"], focus_project_id="p2")["status"] != "found"


@pytest.mark.parametrize("question", [
    "示例平台进度怎样，再给我建议", "示例平台进度怎样，然后生成报告", "告诉我进展并发给示例同事甲",
    "把示例平台状态改为完成", "解释项目进度是什么意思", "明天天气怎么样", "帮我制定项目计划",
    "项目为什么延期", "示例平台和传感器哪个先做", "继续修改报告第三段",
    "示例平台进度怎样，再写报告", "示例平台进度怎样，做一份报告", "示例平台进度怎样，帮我催一下示例同事甲",
    "示例平台标记完成，再告诉我进度",
])
def test_compound_actions_and_general_reasoning_stay_with_existing_router(source, question):
    assert not eligible_question(question)
    result = LedgerIndex(source).query(question, ["p1", "p2"])
    assert result["status"] == "unsupported" and result["body"] == ""


def test_general_progress_question_is_not_project_absence(source):
    result = LedgerIndex(source).query("行业进展如何", ["p1", "p2"])
    assert result["status"] == "unsupported"


def test_question_about_already_marked_completion_is_read_only(source):
    result = LedgerIndex(source).query("示例平台是否已标记完成", ["p2"])
    assert result["status"] == "found"
    assert "实际验收仍待核验" in result["body"]


def test_partial_permission_mapping_cannot_prove_no_record(source):
    result = LedgerIndex(source).query("另一个项目进展", ["p1", "not-bound"])
    assert result["status"] == "unbound"


def test_repeated_stable_row_reference_is_incomplete(source):
    source["rows"].append(deepcopy(source["rows"][0]))
    result = LedgerIndex(source).query("示例传感器进展", ["p1"])
    assert result["status"] == "incomplete" and result["requires_refresh"]


def test_invalid_field_representation_is_not_treated_as_an_empty_fact(source):
    source["rows"][0]["项目进展"] = {"url": "not a fact"}
    result = LedgerIndex(source).query("示例传感器进展", ["p1"])
    assert result["status"] == "incomplete"
    assert result["requires_refresh"]


def test_invalid_scope_is_not_interpreted_as_characters(source):
    with pytest.raises(ValueError):
        LedgerIndex(source).query("示例平台进度", "p2")


@pytest.mark.parametrize("question", [
    "示例平台下一步是什么", "示例平台进展、卡点和下一步是什么", "示例平台目前的状态是什么",
])
def test_field_value_questions_are_distinct_from_concept_explanations(source, question):
    assert eligible_question(question)
    result = LedgerIndex(source).query(question, ["p1", "p2"])
    assert result["status"] == "found"
    assert result["project_ids"] == ["p2"]


@pytest.mark.parametrize("question", ["项目进度是什么", "什么是项目进展", "解释一下项目进度是什么意思"])
def test_concept_explanation_is_still_left_for_general_reasoning(source, question):
    assert not eligible_question(question)
    assert LedgerIndex(source).query(question, ["p1", "p2"])["status"] == "unsupported"


@pytest.mark.parametrize("question", [
    "示例平台进展怎么样，给示例主管乙发一下", "示例平台进展怎么样，叫小王跟进",
    "示例平台进度怎么样，发一下给示例主管乙", "示例平台进度，跟小王说一下",
    "示例平台进度，帮我问问示例同事甲", "示例平台进度，帮我问一下示例同事甲",
    "示例平台进度，向小王说一声", "示例平台进度，让小王确认一下",
])
def test_colloquial_outreach_is_not_swallowed_by_fact_reply(source, question):
    assert not eligible_question(question)
    result = LedgerIndex(source).query(question, ["p1", "p2"])
    assert result["status"] == "unsupported" and result["body"] == ""


@pytest.mark.parametrize("question", [
    "告诉我项目进展，顺便明天天气如何", "示例平台进展和明天天气如何",
    "明天天气如何，示例平台进展怎样", "示例平台进度，顺便讲个笑话",
    "示例平台进度再告诉我一首诗", "示例平台进度并讲个笑话",
])
def test_mixed_non_project_request_keeps_the_complete_question_for_router(source, question):
    assert not eligible_question(question)
    result = LedgerIndex(source).query(question, ["p1", "p2"])
    assert result["status"] == "unsupported" and result["body"] == ""


def test_telling_the_current_questioner_is_not_outreach(source):
    result = LedgerIndex(source).query("给我说说示例平台进度", ["p2"])
    assert result["status"] == "found"


def test_explicit_identifier_and_named_project_are_both_answered(source):
    source["rows"][0].update(business_id="需求75", title="需求75｜示例传感器")
    result = LedgerIndex(source).query("需求75和示例平台进展如何", ["p1", "p2"])
    assert result["status"] == "found"
    assert result["project_ids"] == ["p1", "p2"]
    assert "示例传感器" in result["body"] and "示例平台" in result["body"]


def test_explicit_identifier_and_undefined_other_project_is_partial(source):
    source["rows"][0].update(business_id="需求75", title="需求75｜示例传感器")
    result = LedgerIndex(source).query("需求75以及另一个项目目前进度如何", ["p1", "p2"])
    assert result["status"] == "incomplete"
    assert result["project_ids"] == ["p1"]
    assert "另一项目尚未定位" in result["body"]
    assert not result["requires_refresh"]


def test_unknown_identifier_plus_separately_named_project_preserves_both_outcomes(source):
    result = LedgerIndex(source).query("需求999和示例平台进展如何", ["p1", "p2"])
    assert result["status"] == "incomplete"
    assert result["project_ids"] == ["p2"]
    assert result["unresolved_ids"] == ["需求999"]
    assert "示例平台" in result["body"] and "需求999" in result["body"]


@pytest.mark.parametrize("question", [
    "所有项目目前进展如何", "台账整体进度怎么样", "我的项目待办有哪些", "项目进展如何",
    "请看看全部项目的计划", "示例同事甲负责的项目进展如何", "当前项目总览",
])
def test_collection_or_missing_project_subject_is_not_a_failed_scalar_lookup(source, question):
    assert not eligible_question(question)
    result = LedgerIndex(source).query(question, ["p2"])
    assert result["status"] == "unsupported"
    assert result["body"] == "" and not result["requires_refresh"]
    # The result is independent of source availability: this path must not
    # commandeer a non-scalar question merely because a source read is slow.
    assert LedgerIndex({}).query(question, ["p2"])["status"] == "unsupported"


def test_overall_progress_of_a_named_project_is_still_a_scalar_lookup(source):
    result = LedgerIndex(source).query("示例平台项目整体进度怎么样", ["p2"])
    assert result["status"] == "found" and result["project_ids"] == ["p2"]


@pytest.mark.parametrize("question", [
    "那示例测量器项目进展呢", "那示例测量器进展呢", "它的示例测量器项目进展呢",
    "这个示例测量器项目的下一步呢", "那下一个项目进展呢",
])
def test_unresolved_new_subject_cannot_reuse_an_older_project_focus(question):
    src = snapshot(project("HD", "PT-2099-004", "示例平台", 项目进展="只属于示例平台的进度。"))
    result = LedgerIndex(src).query(question, ["HD"], focus_project_id="HD")
    assert result["status"] in {"no_match", "unsupported", "ambiguous"}
    assert result["project_ids"] == []
    assert "只属于示例平台的进度" not in result["body"]
    assert "示例平台" not in result["body"]


@pytest.mark.parametrize("question", ["那下一步呢", "它的卡点呢", "这个项目的负责人是谁", "下一步是什么"])
def test_pure_field_followups_can_reuse_current_authorized_focus(source, question):
    result = LedgerIndex(source).query(question, ["p1", "p2"], focus_project_id="p2")
    assert result["status"] == "found"
    assert result["project_ids"] == ["p2"]


def test_explicitly_named_new_project_wins_over_previous_focus(source):
    result = LedgerIndex(source).query("那示例传感器项目进展呢", ["p1", "p2"], focus_project_id="p2")
    assert result["status"] == "found"
    assert result["project_ids"] == ["p1"]
