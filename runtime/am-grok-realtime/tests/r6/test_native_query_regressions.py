"""Sanitized reproductions of the actual R6 native query failures."""
import pytest
from am_gateway.assistant.retrieval import LedgerIndex
from test_retrieval import project, snapshot


@pytest.mark.parametrize('modifier', ['主要', '关键', '当前', '现有'])
@pytest.mark.parametrize('field', ['卡点', '风险', '下一步'])
def test_field_modifier_is_not_a_second_project(modifier, field):
    index = LedgerIndex(snapshot(project('p1', 'QI-2099-007', '样件防夹质量改善项目')))
    result = index.query(f'样件防夹质量改善项目现在进展怎么样，{modifier}{field}和下一步是什么？', ['p1'])
    assert result['status'] == 'found'
    assert '另一项目' not in result['body']


def test_field_modifier_does_not_hide_an_unknown_second_subject():
    index = LedgerIndex(snapshot(project('p1', 'QI-2099-007', '样件防夹质量改善项目')))
    result = index.query('样件防夹质量改善项目和示例测量器的主要卡点是什么？', ['p1'])
    assert result['status'] == 'incomplete'


@pytest.mark.parametrize('clause', ['我刚才问的是', '我指的是', '问的是'])
def test_explicit_correction_selects_new_named_subject(clause):
    index = LedgerIndex(snapshot(project('a', 'QI-2099-007', '样例甲'),
        project('b', 'QI-2099-008', '样例乙', **{'项目进展': '待评审。'})))
    result = index.query('不是样例甲，'+clause+'样例乙。那它现在什么状态？', ['a','b'],focus_project_id='a')
    assert result['status'] == 'found' and result['project_ids'] == ['b']
    assert '待评审' in result['body'] and '样例甲' not in result['body']


def test_corrected_hidden_subject_cannot_fall_back_to_old_focus():
    index = LedgerIndex(snapshot(project('a', 'QI-2099-007', '样例甲')))
    result = index.query('不是样例甲，我刚才问的是样例乙。那它现在什么状态？', ['a'],focus_project_id='a')
    assert result['status'] != 'found' and not result['project_ids']


def structured():
    return project('p1', 'QI-2099-007', '样件防夹质量改善项目', **{
        '项目进展': '2026.11.9\n进度：三项方案已发图打样，实际验证待核。\n卡点：回样交期待确认。\n下一步：回样后试装，通过验证后导入。\n备注：发图不等于验证完成；三项闭环齐备后结项。',
        '待办': '找负责人确认加工商交期。', '关键日期': '章程计划：2027.1.31；过程计划：2026.12.31。'})


def test_structured_progress_avoids_duplicates_and_keeps_conditions():
    result = LedgerIndex(snapshot(structured())).query('样件防夹质量改善项目现在进展怎么样，主要卡点和下一步是什么？', ['p1'])
    assert result['status'] == 'found'
    body = result['body']
    for text in ['三项方案已发图打样，实际验证待核。', '回样交期待确认。', '回样后试装，通过验证后导入。',
                 '发图不等于验证完成；三项闭环齐备后结项。']:
        assert body.count(text) == 1
    assert '2027.1.31' not in body and '找负责人' not in body


def test_explicit_schedule_and_owner_actions_remain_separate():
    result = LedgerIndex(snapshot(structured())).query('样件防夹质量改善项目的计划日期、下一步和我的待办是什么？', ['p1'])
    body = result['body']
    assert '章程计划：2027.1.31；过程计划：2026.12.31。' in body
    assert '下一步：回样后试装，通过验证后导入。' in body
    assert '本人待办：找负责人确认加工商交期。' in body


@pytest.mark.parametrize('prefix', ['尚未核实的补充条件\n', '进度：旧段仍有效。\n'])
def test_ambiguous_progress_structure_preserves_all_original_text(prefix):
    row = structured();row['项目进展'] = prefix + row['项目进展']
    body = LedgerIndex(snapshot(row)).query('样件防夹质量改善项目进展', ['p1'])['body']
    assert row['项目进展'] in body
