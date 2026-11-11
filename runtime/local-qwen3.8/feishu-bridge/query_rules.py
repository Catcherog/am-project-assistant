"""Code-owned query intent, field selection and exact identifier accounting."""
import re
import unicodedata

CODE = re.compile(r"(?<![A-Za-z0-9])(?:IRAD|CERT|RA|QI|PT)-\d{4}-\d{3}(?!\d)", re.I)
REQUEST = re.compile(r"需求\s*([0-9]+)(?!\d)")
FIELDS = {"progress": "项目进展", "schedule": "关键日期", "todo": "待办", "people": "人员与职责"}


def identifiers(text):
    text = unicodedata.normalize("NFKC", text)
    found = [(m.start(), m[0].upper()) for m in CODE.finditer(text)]
    found += [(m.start(), "需求" + str(int(m[1]))) for m in REQUEST.finditer(text)]
    return list(dict.fromkeys(value for _, value in sorted(found)))


def fields_for(text):
    fields = []
    for pattern, field in ((r"进展|进度|现状|状态|情况|怎么样|如何|卡在哪|什么问题", "progress"),
                           (r"何时|什么时候|日期|节点|交期|排期|计划|多久|时间", "schedule"),
                           (r"待办|跟进|下一步|接下来", "todo"),
                           (r"谁|负责|责任|人员|分工|联系人", "people")):
        if re.search(pattern, text):
            fields.append(field)
    return fields or ["progress"]


def deterministic_intent(text):
    q = unicodedata.normalize("NFKC", text).strip()
    if q in ("/help", "/menu", "帮助", "你能做什么", "/status", "状态"):
        return "help"
    if q in ("取消", "/cancel"):
        return "cancel"
    if q.startswith(("/remind", "/update", "更新需求", "更新项目")) or re.search(r"(?:帮我|请)?(?:修改|更新|删除|批准|发送|发给|通知|提醒|催办)", q):
        return "action"
    if re.search(r"编号.*(?:格式|含义|意思|解释)|(?:解释|什么是).*编号", q):
        return "explain"
    if q.startswith("/progress ") and q != "/progress sample":
        return "project"
    if (identifiers(q) or re.search(r"项目|需求|demodrive|柔轮|减速机|模组|^那|^它|^这个|^该项目",q,re.I)) and re.search(r"进展|进度|如何|怎么样|谁负责|负责人|待办|下一步|日期|交期|排期|查一下|查询|什么时候|卡在哪|什么问题|负责", q):
        return "project"
    if identifiers(q) and re.sub(CODE, "", REQUEST.sub("", q)).strip(" ,，、？?；;与和及") == "":
        return "project"
    return None


def resolve_identifiers(question, projects):
    requested = identifiers(question)
    resolved, unresolved, conflicts = [], [], []
    for ident in requested:
        matches = [p for p in projects if ident in identifiers(p["title"])]
        if len(matches) == 1:
            if matches[0] not in resolved:
                resolved.append(matches[0])
        elif not matches:
            unresolved.append(ident)
        else:
            conflicts.append(ident)
    return requested, resolved, unresolved, conflicts


def choice_number(question):
    match = re.fullmatch(r"(?:第)?([1-9一二三四五六七八九])(?:个|项)?[。！!]?", question.strip())
    if not match:
        return None
    n = match[1]
    return int(n) if n.isdigit() else "一二三四五六七八九".index(n) + 1
