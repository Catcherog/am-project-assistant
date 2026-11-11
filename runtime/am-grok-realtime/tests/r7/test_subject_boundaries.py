"""User-subject and coverage failures independently found during R6 audit."""
from pathlib import Path
import sys

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'tests' / 'r6'))
from test_retrieval import project, snapshot
from am_gateway.assistant.retrieval import LedgerIndex


@pytest.mark.parametrize('text', [
    '不是样例甲，是样例乙项目现在进展如何？',
    '别查样例甲，查样例乙项目现在进展如何？',
    '不要查样例甲，问的是样例乙项目。进展如何？',
    '我说的不是样例甲，是样例乙项目进展。',
])
def test_unparsed_correction_does_not_answer_negated_old_subject(text):
    index = LedgerIndex(snapshot(project('a', 'QI-2099-007', '样例甲')))
    result = index.query(text, ['a'], focus_project_id='a')
    assert result['status'] == 'unsupported'
    assert result['project_ids'] == [] and result['body'] == ''
    assert result['reason_code'] == 'subject_correction_requires_understanding'


@pytest.mark.parametrize('join', ['，另外', '还有', '，此外', '，再问', '，同时'])
def test_unknown_additional_subject_cannot_be_silently_dropped(join):
    index = LedgerIndex(snapshot(project('a', 'QI-2099-007', '样例甲')))
    result = index.query('样例甲' + join + '样例乙项目目前进展如何？', ['a'])
    assert result['status'] == 'incomplete'
    assert result['project_ids'] == ['a']
    assert '另一项目尚未定位' in result['body']


@pytest.mark.parametrize('join', ['，另外', '还有', '，此外', '，再问', '，同时'])
def test_two_explicit_readable_subjects_with_additional_connector_are_both_answered(join):
    index = LedgerIndex(snapshot(project('a', 'QI-2099-007', '样例甲'),
                                 project('b', 'QI-2099-008', '样例乙')))
    result = index.query('样例甲' + join + '样例乙项目目前进展如何？', ['a', 'b'])
    assert result['status'] == 'found'
    assert result['project_ids'] == ['a', 'b']


@pytest.mark.parametrize('join', ['，另外', '还有', '，此外', '，再问', '，同时'])
def test_additional_field_is_not_mistaken_for_another_subject(join):
    index = LedgerIndex(snapshot(project('a', 'QI-2099-007', '样例甲')))
    result = index.query('样例甲的进展' + join + '卡点是什么？', ['a'])
    assert result['status'] == 'found'
    assert result['project_ids'] == ['a']


def test_existing_complete_correction_grammar_remains_fast():
    index = LedgerIndex(snapshot(project('a', 'QI-2099-007', '样例甲'),
                                 project('b', 'QI-2099-008', '样例乙')))
    result = index.query('不是样例甲，我刚才问的是样例乙。那它现在什么状态？',
                         ['a', 'b'], focus_project_id='a')
    assert result['status'] == 'found' and result['project_ids'] == ['b']
