"""Deterministic, audience-scoped answers from an existing ledger snapshot.

This module owns no credentials, readers, models, queues or external effects.
The host supplies a successful snapshot and the *current* authorized project
IDs. Names are search terms, never authorization. An unavailable or incomplete
snapshot is not evidence that a project does not exist.
"""
from __future__ import annotations

from copy import deepcopy
from datetime import datetime
import math
import re
import unicodedata
from zoneinfo import ZoneInfo


_CODE = re.compile(r"(?<![A-Za-z0-9])(?:IRAD|CERT|RA|QI|PT)\s*-\s*\d{4}\s*-\s*\d{3}(?![A-Za-z0-9])", re.I)
_REQUEST = re.compile(r"需求\s*(\d+)(?!\d)")
_MODELS = re.compile(r"(?<![a-z0-9])(?:demodrive\d+[a-z0-9]*|hd\d+[a-z0-9]*|demotuner|vts|ota|sdk|8d)(?![a-z0-9])", re.I)
_FIELD_QUERY = re.compile(r"进展|进度|现状|状态|卡点|卡在哪|卡在哪里|风险|待办|下一步|接下来|计划|排期|日期|交期|节点|负责人|谁负责|人员|分工|何时|什么时候|多久|完成了吗|是否.*完成")
_ACTION = re.compile(r"生成|编制|制作|导出|新建|修改|转换|修复|改写|编写|运行|执行|部署|更新|删除|创建|关闭|提交|改动|改成|改为|设为|标为|发给|发送|转发|通知|抄送|提醒|催|保存|记录|整理|挂载|采用|批准|同意|安排|写一|写份|写个|写(?:报告|文档|文件|代码|纪要|邮件|周报|日报|总结|计划|说明)|(?:做|出|给)(?:一份|一个|个|份)(?:报告|文档|文件|代码|纪要|邮件|周报|日报|总结|计划|说明)|(?<!已)标记|(?:把|将).*(?:标记|设定)", re.I)
_REASONING = re.compile(r"建议|推荐|分析|判断|为什么|原因|原理|含义|什么是|什么意思|如何.*(?:开展|推进|改进|处理|解决|制定|设计)|怎么做|怎么办|怎么推进|怎么处理|怎么解决|比较|对比|哪个|是否应该|要不要|设计.*方案|制定.*计划")
_CONCEPT = re.compile(r"^(?:(?:请问|解释一下|解释|说明一下)\s*)?(?:项目|需求|台账)(?:进度|进展|计划|状态|风险|节点|排期|负责人|分工)(?:是指什么|是什么)[？?。\s]*$")
_OUTREACH = re.compile(
    r"(?:给|跟|向|让|叫)[^，,。；;！？?!\n]{1,24}?(?:发(?:一下|下|个|条|份|一|消息|过去|出去)?|说(?:一下|下|一声)?|问(?:问|一下|下)?|跟进|确认|联系|沟通|转告|告知)"
    r"|发(?:一下|下|个|条|份|一条|一份|消息|过去|出去)"
    r"|帮我(?:问问|问一下|问下|联系|沟通|转告|告知)"
    r"|请(?!问|帮我|说|告诉)[^，,。；;！？?!\n]{1,16}?(?:跟进|确认|联系|沟通|问问|转告|告知)")
_CLAUSES = re.compile(r"[，,。；;！？?!\n]+|顺便|另外|此外|同时|再问|还有|以及|和|与|、"
                      r"|(?=再(?:说|讲|查|看|告诉|问|给|帮|来|要|解释))"
                      r"|并(?=讲|说|查|看|告诉|问|给|帮|来|要|解释)")
_QUESTION = re.compile(r"如何|怎么样|怎样|是什么|多少|哪里|哪个|哪儿|能否|可否|什么|吗|几点|几天")
_EXTRA_REQUEST = re.compile(r"^(?:(?:再|请|帮我|给我|也|先)\s*)*(?:讲|说|告诉|介绍|看|查|解释|来|给|想|要)")
_PROJECT_WORD = re.compile(r"项目|需求|台账")
# Focus is a fallback only for a whole field-only follow-up. A pronoun prefix
# does not authorize discarding an unrecognized new subject after it.
_FOCUS_WORDS = tuple(sorted({"那", "那么", "它", "这个", "这个项目", "该项目", "这个需求", "该需求",
    "的", "请", "帮我", "查一下", "看看", "目前", "现在", "最新", "进展", "进度", "现状", "状态",
    "计划", "排期", "日期", "交期", "节点", "时间", "负责人", "谁负责", "人员", "人员与职责", "分工",
    "待办", "下一步", "接下来", "卡点", "风险", "何时", "什么时候", "多久", "完成", "是否已标记完成",
    "已标记完成", "是否完成", "和", "与", "及", "还有", "呢", "吗", "了", "是什么", "怎么样", "如何",
    "怎样", "有哪些", "有什么", "在哪", "在哪里", "多少", "是谁"}, key=len, reverse=True))
_FOCUS = re.compile(r"(?:" + "|".join(re.escape(word) for word in _FOCUS_WORDS) + r"|[？?。!！、，,；;\s])+")
_BLOCKER = re.compile(r"卡点|阻塞|受阻|风险|延期|卡住|资源不足|人力不足|缺少|缺资源|等待|待.*确认|暂停|顺延|未到|未解决")
_GENERICS = frozenset({"项目", "需求", "台账", "进展", "进度", "计划", "待办", "工作", "报告", "测试"})
_QUERY_WORDS = tuple(sorted({"请帮我看看", "请帮我查询", "帮我查一下", "请帮我", "我想了解", "了解一下",
    "人员与职责", "什么情况", "什么时候", "谁负责", "负责人", "下一步", "接下来", "怎样", "怎么样", "如何",
    "目前", "现在", "最新", "分别", "进展", "进度", "现状", "状态", "计划", "日期", "排期", "交期", "节点",
    "人员", "职责", "责任", "卡点", "风险", "待办", "跟进", "时间", "情况", "有哪些", "有什么", "是什么",
    "有没有", "是否", "请问", "看看", "看下", "查询", "查查", "查一下", "介绍", "概况", "告诉我", "说说",
    "说一下", "需要我", "预计", "何时", "多久", "完成", "项目", "台账", "你好", "还有", "和", "与", "及",
    "请", "的", "呢", "吗", "了"}, key=len, reverse=True))
_GENERIC_QUERY_WORDS = tuple(sorted({*_QUERY_WORDS, "所有", "全部", "各个", "各项", "各", "这些", "多个",
    "几个", "整体", "总体", "全局", "我的", "我们的", "我们", "我", "本周", "今天", "当月", "近期", "最近",
    "当前", "哪些", "什么", "正在", "在研", "进行中", "需求", "总览", "清单", "列表", "汇总"}, key=len, reverse=True))
_COLLECTION = re.compile(r"(?:所有|全部|各个|各项|这些|多个|各)(?:的)?(?:项目|需求)"
                         r"|(?:负责|参与|经手|相关)(?:的)?项目|(?:项目|需求)(?:列表|清单|总览|汇总)")
# These markers change the requested subject. A fast lookup must not interpret
# an excluded old name as the current target when the whole correction was not
# understood. The existing semantic route receives the original request.
_SUBJECT_CORRECTION = re.compile(r"不是|别(?:查|问)|不要(?:查|问)|不(?:查|问)")
_ADDITIONAL_SUBJECT = re.compile(r"和|与|及|、|分别|另外|还有|此外|再问|同时")


def _text(value):
    return unicodedata.normalize("NFKC", value).replace("—", "-").replace("–", "-").replace("−", "-")


def _compact(value):
    return re.sub(r"[^a-z0-9\u3400-\u9fff]", "", _text(value).casefold())


def _identifier(value):
    value = re.sub(r"\s+", "", _text(value)).upper()
    request = re.fullmatch(r"需求(\d+)", value)
    return "需求" + str(int(request[1])) if request else value


def _identifiers(value):
    value = _text(value)
    found = [(m.start(), _identifier(m[0])) for m in _CODE.finditer(value)]
    found += [(m.start(), "需求" + str(int(m[1]))) for m in _REQUEST.finditer(value)]
    return tuple(dict.fromkeys(ident for _, ident in sorted(found)))


def _generic_project_query(question):
    if _identifiers(question) or _MODELS.search(question):
        return False
    if _COLLECTION.search(question):
        return True
    if not _PROJECT_WORD.search(question):
        return False
    remaining = _compact(question)
    for word in _GENERIC_QUERY_WORDS:
        remaining = remaining.replace(_compact(word), "")
    return not remaining


def eligible_question(question):
    """Whether this may be a short read-only lookup, not a task shortcut.

    Final project anchoring happens in ``query``. A compound operation, a
    request for advice or an explanation is left intact for the normal router.
    """
    if not isinstance(question, str) or not question.strip():
        return False
    q = _text(question).strip()
    if _generic_project_query(q):
        return False
    # A request to tell the current questioner the result is ordinary reply;
    # colloquial requests to tell, ask or contact another person are tasks.
    outreach_text = re.sub(r"(?:给我|跟我)(?:说说|说一下|说下|讲讲|介绍一下)", "", q)
    if (_ACTION.search(q) or _REASONING.search(q) or _CONCEPT.fullmatch(q)
            or _OUTREACH.search(outreach_text)):
        return False
    clauses = [clause.strip() for clause in _CLAUSES.split(q) if clause.strip()]
    if len(clauses) > 1 and any((_QUESTION.search(clause) or _EXTRA_REQUEST.search(clause)) and not (
            _FIELD_QUERY.search(clause) or _identifiers(clause) or _MODELS.search(clause)
            or _PROJECT_WORD.search(clause)) for clause in clauses):
        # '项目进展，顺便明天天气如何' is one compound request. A useful
        # answer to the first clause must not masquerade as its completion.
        return False
    if _FIELD_QUERY.search(q):
        return True
    if _PROJECT_WORD.search(q) and re.search(r"怎么样|如何|情况|看看|查询|查一下|介绍|概况", q):
        return True
    ids = _identifiers(q)
    if ids:
        rest = _REQUEST.sub("", _CODE.sub("", q))
        return not rest.strip(" ,，、？?；;与和及呢的请查一下查询")
    models = tuple(_MODELS.finditer(q))
    if models:
        return not _MODELS.sub("", q).strip(" ,，、？?；;与和及呢的请查一下查询")
    return False


def _names(row):
    aliases = row.get("aliases", [])
    values = [row["title"], *(aliases if isinstance(aliases, (list, tuple)) else ())]
    result = []
    for value in values:
        if not isinstance(value, str):
            continue
        bare = _REQUEST.sub("", _CODE.sub("", _text(value))).strip(" ｜|:：\n")
        candidates = [bare]
        if bare.endswith("项目"):
            candidates.append(bare[:-2])
        for candidate in candidates:
            normalized = _compact(candidate)
            if len(normalized) >= 2 and normalized not in _GENERICS:
                result.append((candidate.casefold(), normalized))
    return tuple(dict.fromkeys(result))


def _name_present(raw_name, compact_name, question):
    # Latin model boundaries survive punctuation/space normalization. In
    # particular DEMOH14 does not silently identify DEMOH140, or DemoTuner identify
    # DemoTunerV2. Mixed Chinese names are matched as recorded phrases.
    if re.fullmatch(r"[a-z0-9 ._/-]+", raw_name):
        pattern = re.escape(raw_name).replace(r"\ ", r"\s*")
        return bool(re.search(r"(?<![a-z0-9])" + pattern + r"(?![a-z0-9])", question.casefold()))
    left = r"(?<![a-z0-9])" if compact_name[0].isascii() else ""
    right = r"(?![a-z0-9])" if compact_name[-1].isascii() else ""
    return bool(re.search(left + re.escape(compact_name) + right, _compact(question)))


def _model_present(token, row):
    suffix = r"(?!\d)" if token[-1].isdigit() else r"(?![a-z0-9])"
    pattern = r"(?<![a-z0-9])" + re.escape(token) + suffix
    aliases = row.get("aliases", [])
    values = [row["title"], *(aliases if isinstance(aliases, (list, tuple)) else ())]
    return any(isinstance(value, str) and re.search(pattern, _text(value).casefold()) for value in values)


def _fields(question):
    q = _text(question)
    fields = []
    broad = bool(re.search(r"现状|概况|总体|整体|什么情况|情况如何|怎么样|进展.*(?:卡点|下一步)|进度.*(?:卡点|下一步)", q))
    if broad or re.search(r"进展|进度|状态|情况|完成了吗|是否.*完成", q):
        fields.append("progress")
    if broad or re.search(r"计划|排期|日期|节点|交期|何时|什么时候|多久|时间", q):
        fields.append("schedule")
    if broad or re.search(r"卡点|卡在哪|卡在哪里|风险|什么问题", q):
        fields.append("blockers")
    if broad or re.search(r"待办|跟进|下一步|接下来", q):
        fields.append("todo")
    if re.search(r"谁|负责|人员|分工|责任|联系人", q):
        fields.append("people")
    return tuple(fields or ("progress",))


def _unresolved_subject(question, entries):
    """Account for an explicitly combined subject without searching hidden rows.

    If a named second subject is not covered, return a partial result rather
    than silently answering the first one. Field conjunctions ('进展和计划')
    have no residual subject after the recorded names and query words leave.
    """
    if not _ADDITIONAL_SUBJECT.search(question):
        return False
    remainder = _compact(_REQUEST.sub("", _CODE.sub("", question)))
    # Modifiers of requested fields are not a second project name. Keep this
    # anchored to field words so an unknown subject still requires clarification.
    remainder = re.sub(r"(?:主要|关键|当前|现有)(?=卡点|风险|下一步|进展|进度|问题|待办)", "", remainder)
    names = sorted({compact for _, row in entries for _, compact in _names(row)}, key=len, reverse=True)
    for name in names:
        remainder = remainder.replace(name, "")
    for token in _MODELS.findall(question):
        if any(_model_present(token.casefold(), row) for _, row in entries):
            remainder = remainder.replace(_compact(token), "")
    for word in _QUERY_WORDS:
        remainder = remainder.replace(_compact(word), "")
    for connective in ('另外', '此外', '再问', '同时'):
        remainder = remainder.replace(connective, '')
    return len(remainder) >= 2


def _field_value(row, field):
    value = row.get(field, "")
    return value.strip() if isinstance(value, str) else ""


def _progress_sections(row):
    """Use exact DEMO sections only when all content has a known destination."""
    raw = _field_value(row, "项目进展")
    matches = list(re.finditer(r"(?m)^(进度|卡点|问题|下一步|备注|要求)[：:]\s*", raw))
    if not matches:
        return None
    preamble = raw[:matches[0].start()].strip()
    if preamble and not re.fullmatch(r"\d{4}[./-]\d{1,2}[./-]\d{1,2}", preamble):
        return None
    aliases = {"问题": "卡点", "要求": "备注"}
    sections = {}
    for i, match in enumerate(matches):
        label = aliases.get(match[1], match[1])
        if label in sections:
            return None
        end = matches[i + 1].start() if i + 1 < len(matches) else len(raw)
        sections[label] = raw[match.end():end].strip()
    return sections if set(sections) == {"进度", "卡点", "下一步", "备注"} else None


def _render(row, question):
    lines = [row["title"] + "（按台账记录）"]
    sections = _progress_sections(row)
    if sections is not None:
        fields = _fields(question)
        if "progress" in fields:
            lines.append("进展：" + (sections["进度"] or "当前台账未明确记录。"))
        if "schedule" in fields and re.search(r"计划|排期|日期|节点|交期|何时|什么时候|多久|时间", question):
            lines.append("计划与日期：" + (_field_value(row, "关键日期") or "当前台账未明确记录。"))
        if "blockers" in fields:
            lines.append("卡点／风险：" + (sections["卡点"] or "台账未明确记录；不能据此判断没有风险。"))
        if "todo" in fields:
            lines.append("下一步：" + (sections["下一步"] or "当前台账未明确记录。"))
            if re.search(r"待办|跟进|需要我|我要", question):
                lines.append("本人待办：" + (_field_value(row, "待办") or "当前台账未明确记录。"))
        if "people" in fields:
            lines.append("人员与职责：" + (_field_value(row, "人员与职责") or "当前台账未明确记录。"))
        # These conditions can qualify completion or the next stage. Preserve
        # them verbatim once rather than guessing which clauses are dispensable.
        if any(field in fields for field in ("progress", "blockers", "todo", "schedule")) and sections["备注"]:
            lines.append("备注：" + sections["备注"])
        return "\n".join(lines)
    labels = {"progress": ("进展", "项目进展"), "schedule": ("计划与日期", "关键日期"),
              "todo": ("下一步／待办", "待办"), "people": ("人员与职责", "人员与职责")}
    for field in _fields(question):
        if field == "blockers":
            explicit = _field_value(row, "卡点") or _field_value(row, "风险")
            if explicit:
                value = explicit
            else:
                # Keep whole source lines, including negation, plans and
                # conditions. No generated claim that a risk is absent.
                candidates = (_field_value(row, "项目进展") + "\n" + _field_value(row, "待办")).splitlines()
                value = "\n".join(dict.fromkeys(line.strip() for line in candidates if _BLOCKER.search(line)))
            lines.append("卡点／风险：" + (value or "台账未明确记录；不能据此判断没有风险。"))
        else:
            label, column = labels[field]
            value = _field_value(row, column)
            lines.append(label + "：" + (value or "当前台账未明确记录。"))
    return "\n".join(lines)


class LedgerIndex:
    """An immutable view of one ledger version, queried within current grants."""

    def __init__(self, snapshot, bindings=None):
        self.snapshot = deepcopy(snapshot) if isinstance(snapshot, dict) else {}
        self.bindings = dict(bindings or {})
        if any(not isinstance(k, str) or not isinstance(v, str) or not k or not v
               for k, v in self.bindings.items()):
            raise ValueError("ledger bindings must map project IDs to stable row references")
        rows = self.snapshot.get("rows")
        self.rows = rows if isinstance(rows, list) else []
        self.valid_rows = []
        self.invalid = not isinstance(rows, list)
        stable_seen = set()
        for row in self.rows:
            if (not isinstance(row, dict) or not isinstance(row.get("title"), str)
                    or not row["title"].strip() or not isinstance(row.get("project_ref") or row.get("key"), str)
                    or not (row.get("project_ref") or row.get("key"))
                    or any(field in row and not isinstance(row[field], str)
                           for field in ("项目进展", "关键日期", "待办", "人员与职责"))):
                self.invalid = True
                continue
            stable = row.get("project_ref") or row["key"]
            if stable in stable_seen:
                self.invalid = True
            stable_seen.add(stable)
            self.valid_rows.append(row)

    def _result(self, status, body="", *, entries=(), refresh=False, **extra):
        return {"status": status, "project_ids": [entry[0] for entry in entries], "body": body,
                "source_version": self.snapshot.get("version"),
                "source_revision": self.snapshot.get("source_revision"),
                "source_ref": self.snapshot.get("source", ""), "requires_refresh": bool(refresh),
                "matches": [{"id": pid, "title": row["title"]} for pid, row in entries], **extra}

    def _audience(self, allowed_project_ids):
        if isinstance(allowed_project_ids, (str, bytes)):
            raise ValueError("allowed project IDs must be a collection")
        allowed = tuple(dict.fromkeys(allowed_project_ids))
        if any(not isinstance(pid, str) or not pid for pid in allowed):
            raise ValueError("invalid allowed project ID")
        entries, unresolved, seen = [], [], set()
        for project_id in allowed:
            if project_id in self.bindings:
                target = self.bindings[project_id]
                matches = [row for row in self.valid_rows if target in (row.get("project_ref"), row.get("key"))]
            else:
                matches = [row for row in self.valid_rows if project_id in (row.get("project_ref"), row.get("key"))]
                if not matches and _identifiers(project_id) == (_identifier(project_id),):
                    matches = [row for row in self.valid_rows
                               if isinstance(row.get("business_id"), str)
                               and _identifier(row["business_id"]) == _identifier(project_id)]
            if len(matches) != 1 or matches[0].get("identity_status") == "conflict":
                unresolved.append(project_id)
                continue
            stable = matches[0].get("project_ref") or matches[0]["key"]
            if stable not in seen:
                seen.add(stable)
                entries.append((project_id, matches[0]))
        return entries, unresolved

    def _footer(self):
        if self.snapshot.get('include_read_time') is False:
            return ''
        value = self.snapshot.get("observed_at")
        if isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value):
            try:
                observed = datetime.fromtimestamp(value, ZoneInfo("Asia/Shanghai"))
                return f"\n\n台账读取时间：{observed:%Y-%m-%d %H:%M}。"
            except (ValueError, OverflowError, OSError):
                pass
        return ""

    def _answer(self, entries, question):
        return "\n\n".join(_render(row, question) for _, row in entries) + self._footer()

    def _named_answer(self, entries, question, all_entries):
        """Account for model tokens and named subjects beyond the first match."""
        covered = list(entries)
        unresolved = []
        for token in dict.fromkeys(m[0].casefold() for m in _MODELS.finditer(question)):
            if any(_model_present(token, row) for _, row in covered):
                continue
            candidates = [entry for entry in all_entries if _model_present(token, entry[1])]
            if len(candidates) > 1:
                choices = "\n".join("• " + row["title"] for _, row in candidates[:5])
                return self._result("ambiguous", "所问的另一型号关联了多个项目，请确认具体项目：\n" + choices,
                                    entries=[*covered, *candidates])
            if candidates:
                covered.append(candidates[0])
            else:
                unresolved.append(token)
        if unresolved or _unresolved_subject(question, covered):
            body = self._answer(covered, question) + "\n\n所问的另一项目尚未定位到可读记录，需要补充它的项目编号或全称。"
            return self._result("incomplete", body, entries=covered,
                                refresh=bool(self.snapshot.get("cache_hit")), unresolved_ids=unresolved)
        return self._result("found", self._answer(covered, question), entries=covered)

    def _additional_named(self, entries, question, all_entries):
        """Resolve named subjects joined to an explicit identifier, if any."""
        if not _ADDITIONAL_SUBJECT.search(question):
            return list(entries), False
        covered = list(entries)
        additions = []
        for entry in all_entries:
            if entry in covered:
                continue
            names = {compact for raw, compact in _names(entry[1]) if _name_present(raw, compact, question)}
            if names:
                additions.append((entry, names))
        if not additions:
            return covered, False
        strongest = [max(names, key=len) for _, names in additions]
        ambiguous = (len(strongest) != len(set(strongest))
                     or any(a in b for a in strongest for b in strongest if a != b))
        return [*covered, *(entry for entry, _ in additions)], ambiguous

    def query(self, question, allowed_project_ids, *, focus_project_id=None):
        if not eligible_question(question):
            return self._result("unsupported")
        if not self.snapshot or self.snapshot.get("refresh_error"):
            return self._result("unavailable", "当前台账读取未完成或读取失败，暂时无法可靠核对这项信息。", refresh=True)
        if self.invalid or self.snapshot.get("complete") is not True or not self.snapshot.get("version"):
            return self._result("incomplete", "当前台账记录还不完整，需要补齐读取后再核对。", refresh=True)
        if self.snapshot.get("stale") is True:
            return self._result("incomplete", "台账快照需要更新，暂时不能把旧记录作为当前进展。", refresh=True)
        entries, unbound = self._audience(allowed_project_ids)
        if not entries:
            return self._result("unbound", "当前入口还没有已绑定的可读台账记录，暂时无法核对项目；这不代表项目不存在。")
        q = _text(question).strip()
        correction = re.fullmatch(
            r"不是[^，,。；;！？?]+[，,]\s*(?:我)?(?:刚才)?(?:问的|指的)是"
            r"([^。；;！？?]+)[。；;]\s*(.+)", q)
        if correction and _FOCUS.fullmatch(re.sub(
                r"什么(?=状态|进展|进度|计划|风险|卡点)", "", correction[2])):
            # An explicit correction names the new subject. Never search the
            # negated name, and never use old focus if the new subject is hidden.
            q = correction[1].strip() + " " + correction[2]
            focus_project_id = None
        elif _SUBJECT_CORRECTION.search(q):
            # An unsupported correction is a semantic routing decision, never
            # evidence that the named replacement does not exist or that the
            # negated project should be reused.
            return self._result('unsupported', reason_code='subject_correction_requires_understanding')
        explicit = _identifiers(q)
        if explicit:
            found, unresolved, ambiguous = [], [], []
            for ident in explicit:
                matches = [entry for entry in entries if ident in _identifiers(entry[1]["title"])
                           or isinstance(entry[1].get("business_id"), str) and _identifier(entry[1]["business_id"]) == ident]
                if len(matches) == 1:
                    if matches[0] not in found:
                        found.append(matches[0])
                elif matches:
                    ambiguous.append(ident)
                else:
                    unresolved.append(ident)
            if ambiguous:
                return self._result("ambiguous", "所问编号存在多个匹配，需要核对具体项目：" + "、".join(ambiguous) + "。",
                                    entries=found, unresolved_ids=unresolved, ambiguous_ids=ambiguous)
            found, ambiguous_names = self._additional_named(found, q, entries)
            if ambiguous_names:
                choices = "\n".join("• " + row["title"] for _, row in found[:5])
                return self._result("ambiguous", "所问的另一名称关联了多个可读项目，请确认具体项目：\n" + choices,
                                    entries=found, unresolved_ids=unresolved)
            if unresolved:
                refresh = bool(self.snapshot.get("cache_hit"))
                if unbound:
                    missing = "部分所问项目尚未绑定可读台账记录，需要先核对绑定。"
                else:
                    missing = "当前可读台账中尚未匹配到编号：" + "、".join(unresolved) + "。"
                body = (self._answer(found, q) + "\n\n" if found else "") + missing
                return self._result("incomplete" if found else "unbound" if unbound else "no_match", body,
                                    entries=found, refresh=refresh and not unbound, unresolved_ids=unresolved)
            return self._named_answer(found, q, entries)

        # Strong, explicitly recorded names take precedence over shared model
        # family names. The names are taken only from already-scoped rows.
        name_matches = []
        for entry in entries:
            matched = {compact for raw, compact in _names(entry[1]) if _name_present(raw, compact, q)}
            if matched:
                name_matches.append((entry, matched))
        if name_matches:
            candidates = [entry for entry, _ in name_matches]
            if len(candidates) == 1:
                return self._named_answer(candidates, q, entries)
            # Two genuinely named projects can be answered together. A shared
            # alias, nested name or shared model must first be disambiguated.
            unique = [max(names, key=len) for _, names in name_matches]
            independently_named = (len(set(unique)) == len(unique)
                                   and not any(a in b for a in unique for b in unique if a != b))
            if independently_named and _ADDITIONAL_SUBJECT.search(q):
                return self._named_answer(candidates, q, entries)
            choices = "\n".join("• " + row["title"] for _, row in candidates[:5])
            return self._result("ambiguous", "匹配到多个可读项目，请确认你指哪一个：\n" + choices,
                                entries=candidates)

        tokens = tuple(dict.fromkeys(m[0].casefold() for m in _MODELS.finditer(q)))
        if tokens:
            matched, unresolved, ambiguous = [], [], False
            for token in tokens:
                candidates = [entry for entry in entries if _model_present(token, entry[1])]
                ambiguous = ambiguous or len(candidates) > 1
                if not candidates:
                    unresolved.append(token)
                for entry in candidates:
                    if entry not in matched:
                        matched.append(entry)
            if ambiguous:
                choices = "\n".join("• " + row["title"] for _, row in matched[:5])
                return self._result("ambiguous", "同一型号关联了多个项目，请确认具体项目：\n" + choices,
                                    entries=matched, unresolved_ids=unresolved)
            if matched and not unresolved:
                return self._result("found", self._answer(matched, q), entries=matched)
            if matched:
                return self._result("incomplete", self._answer(matched, q) + "\n\n仍需核对型号：" + "、".join(unresolved) + "。",
                                    entries=matched, refresh=bool(self.snapshot.get("cache_hit")), unresolved_ids=unresolved)

        if not tokens and focus_project_id is not None and _FOCUS.fullmatch(q):
            focused = [entry for entry in entries if entry[0] == focus_project_id]
            if len(focused) == 1:
                return self._result("found", self._answer(focused, q), entries=focused)
        if unbound:
            return self._result("unbound", "当前项目与台账的读取绑定尚未完整，暂时不能据此判断所问项目是否有记录。")
        # An unanchored question such as '行业进展如何' remains a general
        # question. Only an explicit project/ledger lookup can claim no match.
        if not tokens and not _PROJECT_WORD.search(q):
            return self._result("unsupported")
        return self._result("no_match", "我查了当前可读的台账记录，暂未匹配到这项。请补充项目编号或全称。",
                            refresh=bool(self.snapshot.get("cache_hit")))
