"""Trusted builders and a deliberately conservative adapter for the old ledger.

This module does not use an LLM to certify the LLM's own facts. Structured
builders are for already authorized, source-validated host data. For arbitrary
ledger prose, the complete field (and dependent progress field) stays intact.
"""
from __future__ import annotations

import json
import re
from .types import Claim, ContractError, ReplyContract, digest

_PERSON = re.compile(r"(?P<code>AR\d+(?:/AR\d+)*)?(?P<name>[\u4e00-\u9fff]{2,10})\Z")
_ROLE_LINE = re.compile(r"(?P<person>[^|｜\n]+)\s*[|｜]\s*(?P<duty>[^|｜\n]+)\Z")
_COMPLEX_DUTY = re.compile(r"[。；;!?！？\n]|前提|如果|除非|否则|尚未|不负责|暂不|仅限|不代表|待确认")
_KNOWN_DUTIES = {
    "组织", "小组组织", "审批", "审批跟进", "组织协调", "项目协调", "项目管理",
    "测试", "测试计划", "测试报告", "测试验证", "型号清单", "台架资源", "台架",
    "采购", "供应商沟通", "采购跟进", "质量", "质量检查", "产品", "需求确认",
    "硬件", "硬件设计", "软件", "软件开发", "结构", "结构设计", "研发", "ECR/ECN",
    "ECR", "ECN", "资料整理", "资料核对", "工艺", "工艺验证", "工艺设计", "推进", "审核",
}
_ROLE_TITLE = re.compile(r"(?:TH\d+)?需求申请人|申请人|联系人|项目经理|(?:项目|测试|采购|质量|硬件|软件|结构)?负责人")
_UNKNOWN_PERSON = re.compile(r"未|待定|待确认|待核|不详|未知|暂无|尚|确定|所有|大家|全体")
_FIELD_PATTERNS = {
    "人员与职责": re.compile(r"谁|负责|责任|人员|分工|联系人"),
    "项目进展": re.compile(r"进展|进度|现状|状态|情况|怎么样"),
    "关键日期": re.compile(r"日期|节点|交期|排期|计划|多久|时间|何时|什么时候"),
    "待办": re.compile(r"待办|下一步|接下来|跟进什么"),
}
_COMPLEX_QUESTION = re.compile(r"为什么|原因|建议|比较|风险|能否|是否|会不会|提醒|发送|发给|挂载|修改|更改|写入|生成|章程|报告怎么|最新|新反馈|刚才收到|刚收到|审批消息")
_PROVENANCE_QUESTION = re.compile(r"来源|依据|出处|快照|采集|读取时间|观察时间|核验|版本号")


def _plain(value: str, limit=1000):
    if type(value) is not str or not value.strip() or len(value) > limit or "<" in value or "\x00" in value:
        raise ContractError("INVALID_STRUCTURED_VALUE")
    return value.strip()


def role_claim(claim_id: str, person: str, duty: str, *, source_id: str,
               role_title=False, qualifier: str = "") -> Claim:
    """person/duty are host-resolved roles, not model-generated guesses."""
    person, duty = _plain(person, 80), _plain(duty, 300)
    suffix = ("；" + _plain(qualifier, 500)) if qualifier else ""
    if role_title:
        variants = (f"{person}是{duty}{suffix}", f"{duty}是{person}{suffix}")
    else:
        variants = (f"{person}负责{duty}{suffix}", f"{duty}由{person}负责{suffix}")
    return Claim(claim_id, tuple(dict.fromkeys(variants)), (source_id,), relation="responsibility")


def milestone_claim(claim_id: str, subject: str, event: str, date: str | None, *,
                    kind: str, source_id: str, qualifier: str = "") -> Claim:
    """Keep date kind and scope INSIDE every alternative; no date conversion."""
    subject, event = _plain(subject, 100), _plain(event, 200)
    labels = {"estimate": "预计时间", "commitment": "承诺时间", "requirement": "要求时间",
              "actual": "实际完成时间", "check": "检查时间"}
    suffix = ("；" + _plain(qualifier, 500)) if qualifier else ""
    if kind == "unknown":
        if date is not None:
            raise ContractError("UNKNOWN_DATE_HAS_VALUE")
        variants = (f"{subject}{event}的时间还没确认{suffix}", f"{subject}{event}的具体时间仍待确认{suffix}")
    else:
        if kind not in labels or date is None:
            raise ContractError("DATE_KIND_REQUIRED")
        date = _plain(date, 80)
        label = labels[kind]
        if kind == "estimate":
            variants = (f"{subject}预计在{date}{event}{suffix}", f"{subject}{event}的预计时间是{date}{suffix}")
        else:
            variants = (f"{subject}{event}的{label}是{date}{suffix}", f"{subject}{event}：{label}为{date}{suffix}")
    return Claim(claim_id, variants, (source_id,), relation="milestone:" + kind)


def todo_claim(claim_id: str, action: str, *, source_id: str, owner: str | None = None,
               qualifier: str = "") -> Claim:
    """Only for host-known pending work; never turn completed work into a todo."""
    action = _plain(action, 500)
    suffix = ("；" + _plain(qualifier, 500)) if qualifier else ""
    if owner:
        owner = _plain(owner, 100)
        variants = (f"还有一项待办：{owner}负责{action}{suffix}", f"{action}这项待办由{owner}负责{suffix}")
    else:
        variants = (f"还有一项待办：{action}{suffix}", f"接下来还需要{action}{suffix}")
    return Claim(claim_id, variants, (source_id,), relation="pending_work")


def intact_claim(claim_id: str, text: str, *, source_id: str,
                 depends_on: tuple[str, ...] = ()) -> Claim:
    return Claim(claim_id, (_plain(text, 5000),), (source_id,), depends_on=depends_on, relation="source_block")


def _project_records(value):
    """Walk only understood read-result containers, not arbitrary text blobs."""
    if type(value) is dict:
        if type(value.get("fields")) is dict and type(value.get("id")) is str and "title" in value:
            yield value
            return
        for key in ("project", "projects", "fresh_project_sources", "data"):
            if key in value:
                yield from _project_records(value[key])
    elif type(value) is list:
        for item in value:
            yield from _project_records(item)


def _source_id(project, field):
    return "ledger:" + digest([project["id"], project.get("snapshot"), field, project["fields"].get(field)])


def _roles(raw, source_id):
    # One unparsed header/global caveat keeps the entire field as one block.
    lines = [line.strip() for line in raw.splitlines() if line.strip()]
    rows = []
    for line in lines:
        match = _ROLE_LINE.fullmatch(line)
        if not match:
            return None
        person = match["person"].strip()
        name = _PERSON.fullmatch(person)
        duty = match["duty"].strip()
        if not name or _UNKNOWN_PERSON.search(name["name"]) or _COMPLEX_DUTY.search(duty) or not duty:
            return None
        checked_duty = duty.removeprefix("负责").strip()
        if not _ROLE_TITLE.fullmatch(checked_duty) and not all(
                token.strip() in _KNOWN_DUTIES for token in re.split(r"[、及与和/]", checked_duty)):
            return None
        rows.append((person, name["name"], duty))
    if not rows or len(rows) > 20:
        return None
    names = [row[1] for row in rows]
    claims = []
    for index, (original, name, duty) in enumerate(rows):
        # Preserve stable staff code when multiple rows share the same name.
        # Keep source-provided AR identities, including parallel codes.
        person = original
        if duty.startswith("负责"):
            duty = duty[2:].strip()
        role_title = bool(_ROLE_TITLE.fullmatch(duty))
        claims.append(role_claim(f"role_{index}", person, duty, source_id=source_id, role_title=role_title))
    return claims


def _coverage_claim(project):
    snapshot = project.get("snapshot", {})
    info = project.get("information", {})
    coverage = info.get("coverage", {}) if isinstance(info, dict) else {}
    gap = (isinstance(snapshot, dict) and (snapshot.get("complete") is False
           or any(snapshot.get(k) for k in ("stale", "refresh_failed", "refresh_error"))))
    gap = gap or (isinstance(coverage, dict) and any(coverage.get(k) for k in ("needs_refresh_or_review", "processing_pending")))
    if gap:
        return Claim("coverage", ("另有资料尚待更新或核对", "这份记录还存在更新或核对缺口"),
                     ("coverage:" + digest([snapshot, coverage]),), relation="coverage_gap")
    return None


def _explicit_ids(text):
    found = {"需求" + str(int(n)) for n in re.findall(r"需求\s*(\d+)(?!\d)", text)}
    found.update(x.upper() for x in re.findall(r"(?<![A-Za-z0-9])(?:IRAD|CERT|RA|QI|PT)-\d{4}-\d{3}(?!\d)", text, re.I))
    return found


def _declared_ids(project):
    found = set()
    title = project.get("title", "").lstrip("《 ")
    match = re.match(r"(?:需求\s*\d+|(?:IRAD|CERT|RA|QI|PT)-\d{4}-\d{3})", title, re.I)
    if match:
        found.update(_explicit_ids(match[0]))
    for alias in project.get("aliases", []):
        if type(alias) is str and re.fullmatch(r"需求\s*\d+|(?:IRAD|CERT|RA|QI|PT)-\d{4}-\d{3}", alias, re.I):
            found.update(_explicit_ids(alias))
    return found


def _unmerged_information(value):
    if type(value) is dict:
        if value.get("facts"):
            return True
        return any(_unmerged_information(v) for k, v in value.items() if k in ("information", "data", "project", "projects", "fresh_project_sources"))
    if type(value) is list:
        return any(_unmerged_information(v) for v in value)
    return False


def build_legacy_contract(ctx, event_id: str, user_text: str, draft: str,
                          read_evidence: list[str]) -> ReplyContract | None:
    """Compile only an unambiguous single-project field query.

    The model draft is NOT evidence. Complex questions and arbitrary prose are
    not semantically extracted. A host can inject a richer structured builder.
    """
    if _COMPLEX_QUESTION.search(user_text) or _PROVENANCE_QUESTION.search(user_text):
        return None
    requested = [name for name, pattern in _FIELD_PATTERNS.items() if pattern.search(user_text)]
    if len(requested) != 1:
        return None
    records = {}
    for raw in read_evidence:
        if type(raw) is not str or len(raw) > 100000:
            # Uninspectable evidence may contain a newer fact. Do not treat it
            # as absent and silently replace it with an older ledger field.
            return None
        try:
            value = json.loads(raw)
        except (ValueError, RecursionError):
            return None
        if _unmerged_information(value):
            return None
        projected = list(_project_records(value))
        if not projected:
            # A list-projects catalog carries identities only. Any other result
            # could change the answer; never silently replace it with the ledger.
            if not (type(value) is dict and set(value) <= {"projects", "has_more", "total", "next_offset"}):
                return None
        for project in projected:
            old = records.get(project["id"])
            if old is not None and old != project:
                return None  # Conflicting snapshots: leave reasoning to host.
            records[project["id"]] = project
    if len(records) != 1:
        return None
    project = next(iter(records.values()))
    if project.get("identity_status") != "verified":
        return None
    explicit = _explicit_ids(user_text)
    if explicit and (len(explicit) != 1 or not explicit <= _declared_ids(project)):
        return None
    allowed = getattr(ctx, "can_access_project", None)
    if not callable(allowed) or not allowed(project["id"]):
        return None
    fields, field = project["fields"], requested[0]
    raw = fields.get(field)
    if type(raw) is not str or not raw.strip() or len(raw) > 4800:
        return None
    claims = _roles(raw, _source_id(project, field)) if field == "人员与职责" else None
    origin = "ledger_role_projection" if claims else "ledger_source_block"
    if not claims:
        dependencies = ()
        progress = fields.get("项目进展", "")
        # Cross-field qualifiers are not guessed. Dates/todos retain the full
        # progress dependency if available, or decline if it cannot fit.
        if field in ("关键日期", "待办") and type(progress) is str and progress.strip() and progress != raw:
            if len(progress) > 4800 or len(progress) + len(raw) > 5600:
                return None
            dependencies = ("progress_dependency",)
        claims = [intact_claim("field", raw, source_id=_source_id(project, field), depends_on=dependencies)]
        if dependencies:
            claims.append(intact_claim("progress_dependency", progress, source_id=_source_id(project, "项目进展")))
    coverage = _coverage_claim(project)
    if coverage:
        claims.append(coverage)
    snapshot = project.get("snapshot", {})
    # Never strip generic numbers, model names or document numbers. We only
    # identify values explicitly classified as internal by the host projection.
    forbidden = []
    if isinstance(snapshot, dict) and type(snapshot.get("version")) is str:
        version = snapshot["version"]
        if len(version) >= 32 and not any(version in value for value in fields.values() if type(value) is str):
            forbidden.append(version)
    total = sum(max(map(len, c.variants)) for c in claims) + len(claims) * 2 + 15
    if total > 6000:
        return None
    return ReplyContract(
        contract_id="reply:" + digest([event_id, project["id"], field])[:24], claims=tuple(claims),
        source_version=str(getattr(ctx, "source_access_version", None) or "unversioned"),
        scope_key=ctx.session_id, forbidden_literals=tuple(forbidden), preserve_order=True,
        max_chars=max(500, total), max_groups=3, origin=origin,
        require_record_attribution=True,
    )
