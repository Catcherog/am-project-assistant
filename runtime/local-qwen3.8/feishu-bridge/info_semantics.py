"""Conservative checks for explicit, quoted business signals.

This is a bounded completeness check, not a language model or an authority
classifier. A match requires the salient words in both quote and value. A
paraphrase which cannot satisfy that check is routed to review, never repaired
by guessing a project, person, date or formal business state. No source text is
included in diagnostics, and no candidate/action is generated here.
"""
from __future__ import annotations

from dataclasses import dataclass
import json
import re

from am_info_sync.models import Source
from am_info_sync.policy import can_read


FIELDS = (
    'progress', 'plan', 'forecast', 'review', 'node', 'decision', 'risk', 'owner',
    'response', 'action_request', 'delivery', 'acceptance', 'context', 'constraint',
)


@dataclass(frozen=True)
class SignalRule:
    code: str
    pattern: str
    fields: tuple[str, ...]
    assertion_type: str = 'reported'


# Share the same bounded evidence-noun grammar with the execution-dimension
# check. Absence of a record is not absence of the event; a second independent
# negative event statement must still be checked separately.
_MISSING_EXECUTION_EVIDENCE = re.compile(
    r'(?:没有|尚未|仍未|并未|未)(?:提供|找到|看到|发现)?(?:实际)?'
    r'(?:开始测试|开测|发布)[\s的]*(?:(?:相关|有效|完整|正式|任何)[\s的]*)?'
    r'(?:记录|证据)')
_EXECUTION_ABSENCE = re.compile(
    r'(?:尚未|仍未|并未|没有|未)(?:实际)?(?:开始测试|开测)')


# Deliberately narrow phrases. Other languages, paraphrases and implicit facts
# remain model/human-review responsibilities. These rules do not establish
# that an input was fully understood merely because no rule failed.
RULES = (
    SignalRule('response', r'(?:回复|回应|答复)\s*[“"：:]?\s*(?:收到|OK\b|了解|知道了|好的|明白)', ('response',)),
    SignalRule('approval', r'审批(?:已经|已)?通过', ('review',)),
    SignalRule('approval_not_withdrawn', r'不是审批撤回', ('review', 'constraint')),
    SignalRule('not_project_acceptance', r'不代表(?:项目)?完成', ('acceptance', 'constraint')),
    SignalRule('retire_reminder_request', r'(?:旧的)?待提审提醒可以撤除', ('action_request',), 'proposal'),
    SignalRule('plan', r'计划(?:今天|明天|本周|下周|本月|下月|周[一二三四五六日天])[^，。；;\n]{1,35}', ('plan',)),
    SignalRule('execution', r'正在测试|仍在测试中|实际已开测|已开始测试', ('progress',)),
    SignalRule('conditional_forecast_dimension', r'才可能(?:开始)?(?:测试|开测|发布)', ('forecast',), 'forecast'),
    SignalRule('execution_evidence_missing', _MISSING_EXECUTION_EVIDENCE.pattern, ('progress', 'acceptance')),
    SignalRule('acceptance_pending', r'实际是否开测尚待核实|可用性尚未确定|业务核验尚未完成|正式可对外使用的版本待确认', ('acceptance',)),
    SignalRule('delivery', r'样机已到仓|材料已交付|稿件与版本已经齐备|测试方案已经发出|已经有测试概述', ('delivery', 'context')),
    SignalRule('disclosure_constraint', r'(?:仍)?禁止对外(?:发送|披露)', ('constraint',)),
    SignalRule('action_request', r'(?:请把|请将|请|需要)[^。；;\n]{0,60}(?:记下来|记录|登记|补充|确认)', ('action_request',), 'proposal'),
)


def _normal(value):
    return re.sub(r'\s+', '', value).casefold()


# These are grammar/scope markers, not a catalogue of accepted business facts.
# They establish a bounded rejection contract only. In particular, an unknown
# idiom or a passed report is never evidence of semantic correctness.
_BUSINESS = re.compile(
    r'完成|通过|交付|到仓|入库|到货|收到|发出|提交|测试|开测|发布|验收|'
    r'审核|审批|评审|计划|预计|延期|延迟|阻塞|撤回|提醒')
_NEGATIVE = re.compile(r'否认|未(?=[^来])|没(?=[\u4e00-\u9fff])|不(?=[\u4e00-\u9fff])|无(?=[\u4e00-\u9fff])')
_CONDITION = re.compile(r'如果|假如|假设|倘若|除非|只有|只要|仅当|'
    r'若(?=[^。；;\n]{1,60}[，,])|以[^。；;\n]{1,50}为前提|'
    r'[^，,。；;\n]{1,50}(?:之后|之前|后|前)(?:才|方可|方能|再|可|允许)|'
    r'[^，,。；;\n]{1,50}(?:才可以|才能|方可|方能)')
_ATTRIBUTION = re.compile(r'(?:据[^，,。；;\n]{1,24}(?:所说|反馈|报告)|'
    r'[^，,。；;\n“”"：:]{1,24}(?:表示|反馈|声称|(?<!名)称(?!谓|重|号)|说(?!明|法|服)|澄清|告知))')
_UNRESOLVED_SUBJECT = re.compile(
    r'(?:项目|主体)(?=、|，|和|及|均未|未明|尚未)[^。；;\n]{0,48}(?:未明确|不明确)|'
    r'(?:归属|指代)(?:尚)?(?:待核实|待核对|未明|不明|不清|不确定)|'
    r'(?:没有|未|尚未)(?:说明|明确|确定)[^。；;\n]{0,28}(?:哪个项目|项目归属|对应项目)')
_LEADING_CONNECTOR = re.compile(r'^(?:(?:但(?:是)?|然而|而且|并且|以及|并|也|目前|现在|当前)\s*)+')
_REPORT_BREAK = re.compile(r'^\s*(?:但|然而|不过|只是|而实际|实际|另据|另一方面)')


def _units(text, *, split_commas=True):
    """Return source offsets; commas inside quoted text retain their scope."""
    start = 0
    quote = None
    closing = {'“': '”', '「': '」', '『': '』', '‘': '’'}
    for index, char in enumerate(text):
        if quote:
            if char == quote:
                quote = None
        elif char in closing:
            quote = closing[char]
        elif char == '"':
            quote = '"'
        elif char in ('，,。；;！!？?\n' if split_commas else '。；;！!？?\n'):
            if text[start:index].strip():
                yield start, index, text[start:index]
            start = index + 1
    if text[start:].strip():
        yield start, len(text), text[start:]


def _core(text, subject_titles):
    value = text.strip()
    for title in sorted(subject_titles, key=len, reverse=True):
        if title and value.startswith(title):
            value = value[len(title):].lstrip(' ：:')
            break
    return _LEADING_CONNECTOR.sub('', value)


def _preserves(fragment, candidate):
    anchor = _normal(fragment)
    return (bool(anchor) and anchor in _normal(candidate['quote'])
            and anchor in _normal(candidate['value']))


def _reported_scope_end(core, attribution):
    """Protect a bounded run of positive reported clauses, not all discourse.

    An explicit closing quote ends the report. A later contrast, negation,
    condition or different reporting phrase retains its own existing scope;
    this helper does not infer its speaker. Unsupported discourse remains
    requires_review, even when the mechanical checks produce no issue.
    """
    remainder = core[attribution.end():].lstrip(' ，,:：')
    closing = {'“': '”', '「': '」', '『': '』', '‘': '’', '"': '"'}
    if remainder and remainder[0] in closing:
        quote_start = core.find(remainder, attribution.end())
        quote_end = core.find(closing[remainder[0]], quote_start + 1)
        if quote_end >= 0:
            return (quote_end + 1 if _BUSINESS.search(core[quote_start:quote_end])
                    else None)
    end = None
    for start, stop, unit in _units(core):
        if end is None:
            if _BUSINESS.search(core[max(start, attribution.end()):stop]):
                end = stop
            continue
        if (_REPORT_BREAK.search(unit) or _NEGATIVE.search(unit)
                or _CONDITION.search(unit) or _ATTRIBUTION.search(unit)
                or not _BUSINESS.search(unit)):
            break
        end = stop
    return end


def _has_execution_absence(core):
    gaps = list(_MISSING_EXECUTION_EVIDENCE.finditer(core))
    return any(not any(gap.start() <= match.start() and match.end() <= gap.end()
                       for gap in gaps)
               for match in _EXECUTION_ABSENCE.finditer(core))


def _protected_spans(text, subject_titles):
    """Return offset-bound scopes; these never leave the transient check."""
    spans = []
    for start, _, original in _units(text):
        core = _core(original, subject_titles)
        if not _BUSINESS.search(core):
            continue
        offset = start + original.find(core)
        if _NEGATIVE.search(core):
            spans.append((offset, offset + len(core), core, 'negation'))
        if _ATTRIBUTION.search(core):
            spans.append((offset, offset + len(core), core, 'attribution'))
    for part in re.finditer(r'[^。；;！!？?\n]+', text):
        sentence = part.group()
        core = _core(sentence, subject_titles)
        condition = _CONDITION.search(core)
        if condition and _BUSINESS.search(core[condition.start():]):
            stop=len(core)
            for start_unit,_,unit in _units(core):
                if start_unit>condition.start() and _REPORT_BREAK.match(unit):
                    stop=start_unit;break
            fragment = core[condition.start():stop].rstrip(' ，,')
            offset = part.start() + sentence.find(core) + condition.start()
            spans.append((offset, offset + len(fragment), fragment, 'condition'))
        attribution = _ATTRIBUTION.match(core)
        if attribution:
            end = _reported_scope_end(core, attribution)
            if end is not None:
                offset = part.start() + sentence.find(core)
                spans.append((offset, offset + end, core[:end], 'attribution'))
    return spans


def _source_span_issues(text, candidates, subject_titles, protected_spans):
    """Protect source scope without manufacturing claims or changing fields.

    A protected clause must occur together in one candidate's evidence AND
    value. Splitting 'if X, then Y' or quoting only 'done' from a correction
    does not satisfy that contract. Diagnostics contain codes/counts only.
    """
    issues = set()
    business_units = 0
    for start, end, original in _units(text):
        core = _core(original, subject_titles)
        predicates = list(_BUSINESS.finditer(original))
        if not predicates:
            continue
        business_units += 1
        core_start=start+original.rfind(core)
        # Cover each explicit business clause independently. This schema has
        # verbatim quotes, not model-selected source offsets; repeated identical
        # wording cannot by itself establish the intended project identity.
        # Commands in source data still cannot become executable authority;
        # an unrepresented command is a review gap, not an execution request.
        if not any(
            any((start <= occurrence.start() and occurrence.end() <= end
                 or bool(core) and occurrence.start() <= core_start
                    and core_start+len(core) <= occurrence.end())
                and any(occurrence.start() <= start + p.start()
                        and start + p.end() <= occurrence.end()
                        and _normal(p.group()) in _normal(candidate['value'])
                        for p in predicates)
                for occurrence in re.finditer(re.escape(candidate['quote']), text))
            or (original in candidate['quote']
                and any(_normal(p.group()) in _normal(candidate['value']) for p in predicates))
            for candidate in candidates):
            issues.add('source_clause_uncovered')
        # The original exact-anchor rule already checks these whole phrases.
        # Avoid emitting a redundant second code for the same violation.
        exact_rule = any(_normal(m.group()) == _normal(core)
                         for rule in RULES for m in re.finditer(rule.pattern, core, re.I))
        if _NEGATIVE.search(core) and not exact_rule:
            if not any(_preserves(core, candidate) for candidate in candidates):
                issues.add('negation_or_correction_uncovered')
        if _ATTRIBUTION.search(core):
            if not any(_preserves(core, candidate) for candidate in candidates):
                issues.add('attribution_uncovered')
        # A formal review result remains a review, irrespective of whether it
        # is positive or negative. Lack of execution evidence, by contrast,
        # may validly be displayed in progress or acceptance.
        is_correction = bool(re.search(r'(?:说法|表述|结论|记录|消息).*(?:不成立|错误|有误|不实)', core))
        if (_NEGATIVE.search(core) and re.search(r'评审|审批|审核', core)
                and re.search(r'通过|撤回|否决|驳回', core) and not is_correction):
            review_fields = {'review', 'constraint'} if re.search(r'不是|不代表|并非', core) else {'review'}
            if not any(candidate['field'] in review_fields and _preserves(core, candidate)
                       for candidate in candidates):
                issues.add('review_dimension')
        # Missing evidence does not prove that execution did not happen. Keep
        # the evidence exclusion local to each match: a later independent
        # '尚未开测' still requires progress even when the same clause also
        # mentions absent records. Do not waive the scope/negation contracts.
        if _has_execution_absence(core):
            if not any(candidate['field'] == 'progress' and _preserves(core, candidate)
                       for candidate in candidates):
                issues.add('execution_dimension')
    for _, _, fragment, kind in protected_spans:
        if kind == 'condition':
            if not any(_preserves(fragment, candidate) for candidate in candidates):
                issues.add('condition_scope_uncovered')
        elif kind == 'attribution':
            if not any(_preserves(fragment, candidate) for candidate in candidates):
                issues.add('attribution_uncovered')
    # One correct candidate cannot shield a second, scope-stripped candidate.
    # Every quote touching a protected scope must preserve that scope in its
    # own quote AND value. Repeated identical quotes are deliberately treated
    # conservatively because this output schema has no occurrence selector.
    for candidate in candidates:
        occurrences = list(re.finditer(re.escape(candidate['quote']), text))
        for start, end, fragment, _ in protected_spans:
            if any(occurrence.start() < end and start < occurrence.end()
                   for occurrence in occurrences) and not _preserves(fragment, candidate):
                issues.add('candidate_scope_incomplete')
    if _UNRESOLVED_SUBJECT.search(text):
        # There is no subject-free candidate in the current persisted schema.
        # Preserve the entire observation as a visible review gap. Do not
        # fabricate a subject id or quietly drop the ambiguous clause.
        issues.add('subject_unresolved')
    return issues, business_units


def _mentioned_subjects(text, bindings):
    """Longest literal titles win over their own substrings, never fuzzy ids."""
    matches = []
    for title, subject_id in bindings:
        for occurrence in re.finditer(re.escape(title), text):
            matches.append((occurrence.start(), occurrence.end(), subject_id))
    return {subject_id for start, end, subject_id in matches
            if not any(other_start <= start and end <= other_end
                       and (other_start, other_end) != (start, end)
                       for other_start, other_end, _ in matches)}


def explicit_hint_conflict(text, bindings, hint):
    return bool(_mentioned_subjects(text,bindings)-{hint})


def _subject_binding_issues(text, candidates, bindings, subject_scope_ids=()):
    issues = set()
    if not bindings and len(set(subject_scope_ids)) <= 1:
        return issues
    units = list(_units(text))
    sentences = list(_units(text, split_commas=False))
    source_subjects = _mentioned_subjects(text, bindings)
    for candidate in candidates:
        explicit = _mentioned_subjects(candidate['quote'], bindings)
        possible = set(explicit)
        unbound_occurrence = False
        if not explicit:
            # Prefer a literal title in the same source unit. A unique title
            # in the same sentence also handles "Project A, delivery...".
            # Do not carry the last-mentioned project across sentence breaks.
            for occurrence in re.finditer(re.escape(candidate['quote']), text):
                local = set()
                for start, end, unit in units:
                    if start <= occurrence.start() and occurrence.end() <= end:
                        local.update(_mentioned_subjects(unit, bindings))
                if not local:
                    for start, end, sentence in sentences:
                        if start <= occurrence.start() and occurrence.end() <= end:
                            local.update(_mentioned_subjects(sentence, bindings))
                if not local:
                    unbound_occurrence = True
                possible.update(local)
        if len(possible) > 1:
            issues.add('subject_binding_ambiguous')
        elif possible and candidate['subject_id'] not in possible:
            issues.add('subject_binding_mismatch')
        if unbound_occurrence and (len(source_subjects) > 1
                                   or len(set(subject_scope_ids)) > 1):
            # A bare "then testing began" in a multi-project observation
            # cannot inherit the model's choice or a conversation hint.
            issues.add('subject_binding_ambiguous')
    return issues


def coverage_report(text, candidates, *, subject_titles=(), subject_bindings=(),
                    subject_scope_ids=()):
    """Return only codes/counts; never raw text or inferred business facts."""
    missing = set()
    matched = 0
    protected_spans = _protected_spans(text, subject_titles)
    for rule in RULES:
        for match in re.finditer(rule.pattern, text, re.IGNORECASE):
            matched += 1
            # A positive phrase inside a correction or conditional clause is
            # governed by the whole clause, not an independent positive fact.
            # The scope checks below still require its exact preservation.
            if rule.code in {'approval', 'plan', 'execution', 'delivery'} and any(
                    kind in {'negation', 'condition'} and start <= match.start()
                    and match.end() <= end for start, end, _, kind in protected_spans):
                continue
            anchor = _normal(match.group())
            if not any(c['field'] in rule.fields
                       and c['assertion_type'] == rule.assertion_type
                       and anchor in _normal(c['quote'])
                       and anchor in _normal(c['value']) for c in candidates):
                missing.add(rule.code)
    protected, business_units = _source_span_issues(text, candidates, subject_titles, protected_spans)
    missing.update(protected)
    missing.update(_subject_binding_issues(text, candidates, subject_bindings, subject_scope_ids))
    for candidate in candidates:
        if (candidate['field'] == 'forecast') != (candidate['assertion_type'] == 'forecast'):
            missing.add('forecast_dimension')
    return {'check': 'source_span_coverage_v3', 'matched_signals': matched,
            'business_units_checked': business_units,
            'issues': sorted(missing), 'semantic_acceptance': 'requires_review'}


def pending_reviews(store, principal, *, limit=30):
    """Read current authorized gaps with retention-limited source excerpts.

    One local read transaction covers ACL, gap and current observation. Old
    versions are never used to fill missing current evidence. No raw text is
    persisted by this view, and revoked/disabled sources disappear immediately.
    """
    if type(limit) is not int or not 1 <= limit <= 100:
        raise ValueError('review_limit_invalid')
    items = []
    total = 0
    expires_at_ms = None
    with store.transaction() as db:
        epoch = db.execute('SELECT namespace,revision FROM source_access_version WHERE singleton=1').fetchone()
        access_version = epoch[0] + ':' + str(epoch[1])
        rows = db.execute("""SELECT g.*,s.config_json,s.availability,
            o.text,o.url,o.raw_expires_at_ms,o.purged,o.deleted,o.completeness,
            b.state AS object_state FROM gaps g
            JOIN sources s ON s.source_id=g.source_id
            LEFT JOIN objects b ON b.source_id=g.source_id AND b.object_id=g.object_id
            LEFT JOIN observations o ON o.source_id=b.source_id AND o.object_id=b.object_id
                AND o.version=b.current_version
            WHERE g.state='open' AND g.kind='extraction'
            ORDER BY g.last_seen_ms DESC,g.source_id,g.object_id""")
        now = store.clock()
        for row in rows:
            source = Source.from_dict(json.loads(row['config_json']))
            if row['availability'] == 'denied' or not can_read(source, principal):
                continue
            total += 1
            if len(items) >= limit:
                continue
            raw_available = (row['object_state'] == 'active'
                and row['raw_expires_at_ms'] is not None and row['raw_expires_at_ms'] > now
                and row['purged'] == 0 and row['deleted'] == 0
                and row['completeness'] == 'complete')
            detail = row['detail']
            if detail=='subject_hint_conflict':
                reason='原文明示的项目与当前会话项目不一致，需要先核对项目范围'
            elif detail.startswith('semantic_coverage_incomplete'):
                reason = '明确业务信息未完整保留或分类需核对'
                if any(code in detail for code in ('subject_unresolved', 'subject_binding_ambiguous', 'subject_binding_mismatch')):
                    reason = '资料中的项目归属尚未明确，需要核对'
                elif 'negation_or_correction_uncovered' in detail:
                    reason = '资料中的否定或更正未完整保留，需要核对'
                elif 'condition_scope_uncovered' in detail:
                    reason = '条件与后续结果未完整保留，需要核对'
                elif 'attribution_uncovered' in detail:
                    reason = '转述者与原话未一起保留，需要核对'
                elif 'candidate_scope_incomplete' in detail:
                    reason = '候选引用脱离了原文的条件、否定或转述范围，需要核对'
            elif detail == 'semantic_model_incomplete':
                reason = '模型未完整提取这段资料，需要核对；尚未确定具体原因'
            elif detail == 'semantic_budget_incomplete':
                reason = '本次资料超出单轮完整提取范围'
            elif detail == 'raw_evidence_unavailable':
                reason = '原始资料已不可用，需要重新提供'
            else:
                reason = '资料提取未完成，需要复核来源'
            items.append({'source_id': source.source_id, 'object_id': row['object_id'],
                'reason': reason, 'text': row['text'][:220] if raw_available else '',
                'url': row['url'] if raw_available else None,
                'raw_available': bool(raw_available)})
            if raw_available:
                expires_at_ms = min(expires_at_ms or row['raw_expires_at_ms'], row['raw_expires_at_ms'])
    return {'items': items, 'total': total, 'source_access_version': access_version,
            'raw_expires_at_ms': expires_at_ms}
