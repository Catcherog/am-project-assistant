"""Shared lexical checks used at both the provider and independent validator boundary.

Numbers are normalized as quantities; list markers and alphanumeric identifiers are
not quantities. Conclusion checks recognise a bounded set of business phrases and
nearby negation/uncertainty. They are deliberately not a semantic-entailment proof.
"""
from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass
from typing import Iterable

from .models import Evidence, canonical


_DATE = re.compile(r"(?<![A-Za-z0-9_])\d{4}-\d{2}-\d{2}(?![A-Za-z0-9_])")
_NUMBER = re.compile(
    r"(?<![A-Za-z0-9_.])[-+]?(?:\d{1,3}(?:,\d{3})+|\d+)"
    r"(?:\.\d+)?%?(?![A-Za-z0-9_])"
)
_ORDINAL = re.compile(
    r"(?m)^[ \t]*(?:[-*+][ \t]+)?"
    r"(?:\d{1,3}[.)、](?!\d)|\(\d{1,3}\))"
    r"(?:[ \t]+|(?=[\u4e00-\u9fff]))"
)
_CLAUSES = re.compile(r"[，,。；;！!？?\n]|但是|然而|不过|而是|但")
_CLAIMS = {
    "acceptance_passed": re.compile(r"验收(?:结果|结论)?\s*(?:为|[:：])?\s*(?:已)?通过|通过(?:了)?验收"),
    "verification_passed": re.compile(r"验证(?:结果|结论)?\s*(?:为|[:：])?\s*(?:已)?通过|通过(?:了)?验证"),
    "test_passed": re.compile(r"测试(?:结果|结论)?\s*(?:为|[:：])?\s*(?:已)?通过|通过(?:了)?测试"),
    "approved": re.compile(r"已(?:获)?批准|审批(?:已)?通过|已审批"),
    "closed": re.compile(r"已结项|项目(?:已)?结项|结项(?:已)?完成"),
    "risk_closed": re.compile(r"风险(?:均|全部)?(?:已)?关闭|风险(?:已经)?解除"),
}
_QUOTE = r"[\s:：\"'“‘「『(]*"
# Scope ends at the current clause and must reach the claim. A negation about an
# unrelated item earlier in a sentence does not negate a later positive conclusion.
_NONASSERTIVE_PREFIX = re.compile(
    r"(?:"
    r"(?:不|未|尚未|没有|并未|并非|不是|不算|不代表|不等于|未获|不得|不能)"
    r"(?:已|已经|正式|最终|全部|完全|均|被|获)?"
    r"|(?:尚|仍|还|暂)?(?:不能|无法|未能|不宜|不应|未)"
    r"(?:明确|完全|最终|直接|据此|可靠地|充分)?"
    r"(?:确认|证明|认定|判定|判断|断言|保证|视为)"
    r"|(?:没有|尚无|缺少|缺乏|未有)(?:足够|充分|可靠)?(?:的)?"
    r"(?:依据|证据)(?:能够|可以|可|能|来|足以)?(?:证明|表明|确认|支持|说明|认定)?"
    r"|(?:没有|未)(?:写明|写|注明|说明|记录|给出|确认|报告|提供|出具)"
    r"|(?:待|等待|尚待)(?:进一步|最终)?(?:确认|核实|验证|证明|判定)?"
    r"|(?:是否|能否|预计|计划|拟|目标(?:是|为)?)(?:已|已经)?"
    r")" + _QUOTE + r"$"
)
_NONASSERTIVE_SUFFIX = re.compile(
    r"^[\s\"'”’」』)]*(?:"
    r"[?？]|条件|之前|前|之后|后|与否|待确认|待核实|尚未确认|尚未核实|未经证实|未满足"
    r"|(?:的)?(?:结论|说法|消息|表述)(?:尚未|未经|未)(?:确认|证实|核实)"
    r")"
)


@dataclass(frozen=True)
class ContentCheck:
    unsupported_numbers: tuple[str, ...]
    unsupported_claims: tuple[str, ...]


def numeric_tokens(text: str) -> set[str]:
    """Return normalized numeric values and atomic ISO dates found in prose.

    For example 1,000, 1000.0 and １０００ normalize to the same value; 50% remains
    distinct from 50. A leading ``1. `` is a list marker, while ``1.5 台`` is a value.
    """
    normalized = unicodedata.normalize("NFKC", text)
    normalized = _ORDINAL.sub("", normalized)
    dates = set(_DATE.findall(normalized))
    normalized = _DATE.sub(" ", normalized)
    numbers = set()
    for match in _NUMBER.finditer(normalized):
        raw = match.group(0).replace(",", "")
        percent = raw.endswith("%")
        if percent:
            raw = raw[:-1]
        # Normalize decimal spelling without float or decimal-context rounding.
        # Precision must not collapse two different large identifiers/quantities.
        negative = raw.startswith("-")
        unsigned = raw.lstrip("+-")
        integer, dot, fraction = unsigned.partition(".")
        integer = integer.lstrip("0") or "0"
        fraction = fraction.rstrip("0")
        rendered = integer + ("." + fraction if fraction else "")
        if negative and rendered != "0":
            rendered = "-" + rendered
        numbers.add(rendered + ("%" if percent else ""))
    return dates | numbers


def positive_business_claims(text: str) -> set[str]:
    """Find unqualified positive conclusions in a bounded phrase vocabulary.

    Negated, prospective, conditional and explicitly uncertain mentions do not
    supply evidence of success. Clauses/contrast markers delimit local scope.
    """
    normalized = unicodedata.normalize("NFKC", text)
    result = set()
    for name, pattern in _CLAIMS.items():
        for match in pattern.finditer(normalized):
            prefix = _CLAUSES.split(normalized[:match.start()])[-1]
            suffix = normalized[match.end():]
            if not _NONASSERTIVE_PREFIX.search(prefix) and not _NONASSERTIVE_SUFFIX.search(suffix):
                result.add(name)
    return result


def check_content(
    text: str, evidence: Iterable[Evidence], *, check_conclusions: bool = True,
) -> ContentCheck:
    """Check only the supplied evidence; examples/unread sources cannot justify claims."""
    evidence = tuple(evidence)
    source_text = "\n".join(item.value if isinstance(item.value, str) else canonical(item.value) for item in evidence)
    numbers = numeric_tokens(text) - numeric_tokens(source_text)
    unsupported: set[str] = set()
    if check_conclusions:
        supported: set[str] = set()
        for item in evidence:
            if (item.role == "project_fact" and item.source_read_state != "unavailable"
                    and item.state == "confirmed" and item.semantic in {"actual", "decision"}):
                value_text = item.value if isinstance(item.value, str) else canonical(item.value)
                supported.update(positive_business_claims(value_text))
        unsupported = positive_business_claims(text) - supported
    return ContentCheck(tuple(sorted(numbers)), tuple(sorted(unsupported)))
