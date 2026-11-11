"""Regression for the actual legacy selector, with synthetic ledger rows."""
from pathlib import Path
import sys

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT.parent / 'FS-AGENT-GROK-POC-20261017-01_R1_Studio' / 'src'))
sys.path.insert(0, str(ROOT.parent / 'local-qwen3.8' / 'feishu-bridge'))
from group_ledger import select_projects


@pytest.mark.parametrize('question,title', [
    ('说说示例传感器进展，卡点和下一步', '示例传感器'),
    ('示例平台现在进度怎样', '示例平台'),
    ('请帮我看看示例平台的进度', '示例平台'),
    ('示例平台卡点有哪些', '示例平台'),
])
def test_recorded_name_inside_natural_question(question, title):
    rows = [{'key': 'a', 'title': '示例传感器'}, {'key': 'b', 'title': '示例平台'}]
    assert [r['title'] for r in select_projects(question, rows)] == [title]


def test_multiple_literal_names_remain_multiple_and_explicit_unknown_never_falls_back():
    rows = [{'key': 'a', 'title': 'QI-2099-007｜示例传感器'},
            {'key': 'b', 'title': 'QI-2099-008｜示例平台'}]
    assert len(select_projects('示例平台和示例传感器目前怎样', rows)) == 2
    assert select_projects('QI-2099-009示例平台进展', rows) == []
