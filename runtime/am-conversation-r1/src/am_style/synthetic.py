"""Small review-only seed set. These are authored drafts, never training truth.

The generator expands entity variants only after group assignment. It does not
call an LLM or claim model quality. Change the policy, then regenerate and review.
"""
from __future__ import annotations

from .data import canonical, digest, split_for_group, assert_split_integrity

POLICY_VERSION = "r3-annotated-draft/1"
SYSTEM_DRAFT = (
    "这是待人工核验的表达样本。像同事日常沟通一样直接回答；保留事实、时间和限定。"
    "默认不展示来源标签；追问来源时可给证据。缺信息就具体问。"
    "不能把无资料说成不存在、不能把草稿说成已执行、不能承诺没有排入队列的重试。"
    "这些草案提示词尚未与真实宿主投影器对齐；对齐后必须重新核验输出。"
)

# Every condition is fictional; no company facts or original private blind text.
# 'draft'/'stiff' are editorial roles, not positive/negative human labels.
SEEDS = [
    {
        "group_id": "owner-and-relevant-progress", "family": "query",
        "question": "{project}现在谁负责？",
        "facts": {"owner": "{person}", "related_progress": "测试方案已整理好，测试还没开始"},
        "draft": "{project}现在由{person}负责。测试方案已经整理好了，测试还没开始。",
        "stiff": "负责人：{person}。备注：关联进展为测试方案已整理好，测试尚未开始。",
        "review_focus": ["关联进展是否有必要", "没有来源前缀", "未开始限定保留"],
    },
    {
        "group_id": "empty-todo-complete-read", "family": "query",
        "question": "{project}还有什么待办？",
        "facts": {"query_status": "success", "scope_complete": True, "open_todos": []},
        "draft": "{project}暂时没有待办。",
        "stiff": "根据本轮返回的待办字段，可呈现的未完成事项集合为空。",
        "review_focus": ["完整读取才能直接说没有待办", "避免内部字段用语"],
    },
    {
        "group_id": "empty-analysis-known-gap", "family": "multi_source_analysis",
        "question": "{project}这周能完成测试吗？",
        "facts": {"planned_test_end": None, "latest_progress": "样机已到", "gap": "资料中没有测试开始及结束时间"},
        "draft": "还判断不了。样机已经到了，但资料里没有测试的起止时间。测试准备什么时候开始、预计做多久？",
        "stiff": "资料缺口：测试开始时间及结束时间缺失，因此本轮无法输出是否本周完成测试的判定。请补齐字段。",
        "review_focus": ["回答具体问题", "缺答案原因来自事实", "合并询问关键缺口"],
    },
    {
        "group_id": "ambiguous-engineer-id", "family": "followup",
        "question": "帮我让陈工跟进{project}。",
        "facts": {"recipient_resolution": "ambiguous", "candidate_count": 2, "followup_created": False},
        "draft": "你说的陈工具体是哪位？给我姓名或工号，我确认一下。",
        "stiff": "收件人标识存在歧义，候选数为2，需补充唯一标识后方可继续。",
        "review_focus": ["自然追问", "不能声称已创建", "不要求同时提供姓名和工号"],
    },
    {
        "group_id": "note-saved-reminder-not-scheduled", "family": "worknote",
        "question": "记一下，{project}需要跟进。",
        "facts": {"note_saved": True, "readback_verified": True, "reminder_scheduled": False, "reminder_time": None},
        "draft": "记下了。你想什么时候跟进？",
        "stiff": "笔记保存回读成功。提醒计划尚未创建，时间参数缺失，请提供。",
        "review_focus": ["记下了必须有保存证据", "不将笔记保存说成提醒已设定"],
    },
    {
        "group_id": "knowledge-rejection-scope", "family": "knowledge_candidate",
        "question": "以后不要采用{project}这条规则。",
        "facts": {"scope": None, "expires_at": None, "rule_changed": False},
        "draft": "你说的“不要采用”，是只针对{project}，还是所有项目都停用？有没有时间限制？",
        "stiff": "规则停用缺少适用范围与有效期，当前不执行知识变更。请补齐范围和时间限制。",
        "review_focus": ["范围与有效期一次询问", "提问不暗示已变更"],
    },
    {
        "group_id": "document-missing-inputs", "family": "document",
        "question": "帮我写{project}的结项报告。",
        "facts": {"template_available": True, "facts_read": True, "missing": ["验收结果", "遗留问题"], "document_created": False},
        "draft": "模板已经有了，还缺验收结果和遗留问题。你把这两项资料给我，我就能继续写。",
        "stiff": "文档生成前置检查：模板可用。必需输入验收结果、遗留问题缺失，无法生成完整结项文档。",
        "review_focus": ["已有资料不重复索要", "清楚指出必要缺口", "不假称文档已生成"],
    },
    {
        "group_id": "approval-draft-only", "family": "preview_confirmation",
        "question": "{project}的消息准备好了吗？",
        "facts": {"draft_ready": True, "approved": False, "sent": False},
        "draft": "已经写好了，等你确认后就可以发送。",
        "stiff": "草稿状态为ready，审批状态为pending，发送状态为not_sent。",
        "review_focus": ["区分准备好与已发送", "不向用户输出内部状态名"],
    },
    {
        "group_id": "outbound-readback-success", "family": "outbound_mount",
        "question": "{project}的通知发出去了吗？",
        "facts": {"sent": True, "readback_verified": True, "recipient": "{person}", "delivery_count": 1},
        "draft": "已经发给{person}了。",
        "stiff": "外发结果：success；收件人：{person}；平台消息回读一致。",
        "review_focus": ["明确完成", "完成须依赖回读", "无需每次复述内部核验"],
    },
    {
        "group_id": "reminder-draft-missing-time", "family": "reminder_plan",
        "question": "提醒我跟进{project}。",
        "facts": {"reminder_scheduled": False, "time_missing": True},
        "draft": "你想什么时候提醒？时间定下来我再安排。",
        "stiff": "提醒任务缺失计划触发时间，当前状态为待补充信息，未创建提醒计划。",
        "review_focus": ["只问必要缺口", "不得说提醒已设定"],
    },
    {
        "group_id": "cancel-future-preserve-history", "family": "correction_cancel",
        "question": "{project}之后的提醒都停掉。",
        "facts": {"future_reminders_cancelled": True, "readback_verified": True, "historical_messages_deleted": False},
        "draft": "后续提醒已经停掉了，之前发出的消息会保留。",
        "stiff": "取消操作已完成，作用范围为未来触发的提醒，不包含历史已发送消息的删除。",
        "review_focus": ["先说结果", "保留取消范围", "不过度承诺回滚"],
    },
    {
        "group_id": "partial-second-outcome-unknown", "family": "partial_completion",
        "question": "那两条跟进都建好了吗？",
        "facts": {"first": "created_and_readback", "second": "unknown_after_timeout", "retry_queued": False},
        "draft": "第一条已经建好了。第二条提交后超时了，还不能确定有没有建成，需要先核对结果。",
        "stiff": "目标一状态：completed。目标二状态：unknown。需要只读对账，当前未再次创建。",
        "review_focus": ["每项结果明确", "未知不说失败", "不得重复创建或假称已排重试"],
    },
    {
        "group_id": "read-timeout-retry-actually-queued", "family": "failure_unknown",
        "question": "查到{project}的进展了吗？",
        "facts": {"read_result": "timeout", "cause": "读取超时", "read_retry_queued": True, "write_attempted": False},
        "draft": "刚才读取超时了，还没查到进展。我会再查一次。",
        "stiff": "查询因读取超时失败，只读重试任务已入队。",
        "review_focus": ["先解释已有原因", "只有真实入队才能承诺再查", "失败不等于项目无进展"],
    },
]


def _render(value, values):
    if isinstance(value, str):
        return value.format(**values)
    if isinstance(value, list):
        return [_render(item, values) for item in value]
    if isinstance(value, dict):
        return {key: _render(item, values) for key, item in value.items()}
    return value


def generate_seeds() -> list[dict]:
    rows = []
    for seed in SEEDS:
        # Split is selected before variants, not per output or per paraphrase.
        group = seed["group_id"]
        split = split_for_group(group)
        for number, values in enumerate((
            {"project": "演示项目甲", "person": "林工"},
            {"project": "演示项目乙", "person": "周工"},
        ), 1):
            item = _render(seed, values)
            user_message = canonical({"question": item["question"], "verified_facts": item["facts"]})
            prompt = [{"role": "system", "content": SYSTEM_DRAFT}, {"role": "user", "content": user_message}]
            candidates = {label: {"text": item[label], "sha256": digest(item[label]),
                                  "available": True, "origin": "assistant_authored_synthetic_draft"}
                          for label in ("draft", "stiff")}
            rows.append({
                "id": f"R3-SEED-{group}-{number}", "schema": "am-style-review/1",
                "group_id": group, "parent_group_id": group, "family": seed["family"],
                "split": split, "synthetic": True, "style_policy_version": POLICY_VERSION,
                "source_sha256": digest(canonical(seed)), "prompt": prompt,
                "prompt_sha256": digest(canonical(prompt)), "facts": item["facts"],
                "question": item["question"], "entities": values,
                "fact_entities": list(values.values()), "candidates": candidates,
                "reviews": {}, "selection": {}, "severe_error": {},
                "review_focus": seed["review_focus"], "training_approved": False,
                "runtime_prompt_alignment": "pending_host_projector",
                "note": "人工编写的合成候选；不是Qwen或Codex实测，也不是人工认可的训练样本。",
            })
    assert_split_integrity(rows)
    return rows


def project_seed(record: dict, policy=None, *, gap_profile="r3") -> dict:
    """Map fictional typed evidence to the same host projector used at inference.

    This is an explicit adapter for the thirteen public synthetic prototypes,
    not a generic converter from arbitrary documents into verified business facts.
    """
    from .contracts import StylePolicy
    from .render import make_frame
    from .prompt import build_messages, PROMPT_VERSION
    from .data import align_runtime_prompt, DataError
    if gap_profile not in ("r3", "r4"):
        raise DataError("unknown synthetic gap profile")
    policy = policy or StylePolicy(realization="free")
    if getattr(policy, "realization", None) != "free":
        raise DataError("training candidates require the free realization profile")
    group = record["group_id"]
    project, person = record["entities"]["project"], record["entities"]["person"]
    facts, effects, gaps, empty = [], [], [], None

    def statement(uid, value, required=True):
        return {"id": uid, "kind": "statement", "value": value, "required": required}

    def receipt(uid, kind, obj):
        return {"id": uid, "kind": kind, "object": obj, "state": "succeeded",
                "receipt": {"id": f"synthetic-receipt:{record['id']}:{uid}",
                            "operation_id": f"synthetic-op:{record['id']}:{uid}",
                            "object_id": f"synthetic-object:{record['id']}:{uid}", "verified": True}}

    if group == "owner-and-relevant-progress":
        facts = [{"id": "owner", "kind": "owner", "subject": project, "value": person},
                 statement("progress", "测试方案已经整理好了，测试还没开始", required=False)]
    elif group == "empty-todo-complete-read":
        empty = {"state": "confirmed_empty", "coverage_complete": True, "object": f"{project}的待办"}
    elif group == "empty-analysis-known-gap":
        facts = [statement("unknown", "目前还不能判断这周能否完成测试"),
                 statement("sample", "样机已经到了"),
                 statement("missing", "资料里没有测试的起止时间")]
        gaps = ([{"id": "test-time", "slot": "start_and_duration", "object": "测试"}]
                if gap_profile == "r4" else
                [{"id": "test-time", "slot": "content", "object": "测试的开始时间和预计时长"}])
    elif group == "ambiguous-engineer-id":
        gaps = [{"id": "who", "slot": "person", "object": "陈工"}]
    elif group == "note-saved-reminder-not-scheduled":
        effects = [receipt("note", "save_note", f"{project}需要跟进这件事")]
        gaps = [{"id": "when", "slot": "time", "object": "这件事"}]
    elif group == "knowledge-rejection-scope":
        gaps = [{"id": "scope", "slot": "scope", "object": "不要采用这条规则"}]
    elif group == "document-missing-inputs":
        facts = [statement("template", "模板已经有了"),
                 statement("missing", "还缺验收结果和遗留问题这两项资料")]
        gaps = [{"id": "materials", "slot": "materials", "object": "验收结果和遗留问题"}]
    elif group == "approval-draft-only":
        facts = [statement("ready", "消息已经写好了")]
        effects = [{"id": "send", "kind": "send_message", "object": "这条消息", "state": "pending"}]
    elif group == "outbound-readback-success":
        effects = [receipt("send", "send_message", f"给{person}的通知")]
    elif group == "reminder-draft-missing-time":
        gaps = [{"id": "when", "slot": "time", "object": f"{project}的跟进"}]
    elif group == "cancel-future-preserve-history":
        effects = [{"id": "cancel", "kind": "cancel", "object": f"{project}的提醒",
                    "state": "cancelled", "cancel_verified": True}]
        facts = [statement("history", "之前发出的消息会保留")]
    elif group == "partial-second-outcome-unknown":
        effects = [receipt("first", "create_followup", "第一条需求"),
                   {"id": "second", "kind": "create_followup", "object": "第二条需求", "state": "unknown"}]
        facts = [statement("timeout", "第二条提交后超时了，需要先核对结果")]
    elif group == "read-timeout-retry-actually-queued":
        facts = [statement("timeout", "刚才读取超时了，还没查到进展"),
                 statement("queued", "再次查询的任务已经排入队列")]
    else:
        raise DataError(f"no reviewed synthetic adapter for {group}")
    projected_input = {"facts": facts, "effects": effects, "gaps": gaps, "empty": empty}
    binding = {"turn_id": record["id"], "scope_id": "synthetic-offline-review",
               "outcome_hash": digest(canonical(projected_input)), "source_revision": "synthetic-" + gap_profile + "/1",
               "revision": 0, "cancel_epoch": 0}
    frame = make_frame(binding=binding, expires_at=4102444800,
        question=record["question"], family=record["family"], facts=facts, effects=effects,
        gaps=gaps, empty=empty, context=["本样本完全虚构，仅用于离线表达评审。"], policy=policy)
    aligned = align_runtime_prompt(record, build_messages(frame, policy), PROMPT_VERSION)
    aligned.update(frame=frame.data, frame_sha256=frame.fingerprint,
                   style_policy=policy.as_dict(), style_policy_version=policy.version,
                   realization="free", source_sha256=digest(canonical(projected_input)))
    return aligned


def generate_projected_seeds(policy=None) -> list[dict]:
    records = [project_seed(record, policy) for record in generate_seeds()]
    assert_split_integrity(records)
    return records


def generate_r4_projected_seeds(policy=None) -> list[dict]:
    """New host-authored development frames; never rewrite R3 gold or frames.

    Only the synthetic producer has authoritative typed time-gap inputs here.
    The live terminal projector still refuses untyped model clarifications.
    """
    records = []
    for record in generate_seeds():
        record["id"] = record["id"].replace("R3-SEED-", "R4-SEED-", 1)
        records.append(project_seed(record, policy, gap_profile="r4"))
    assert_split_integrity(records)
    return records


def semantic_challenges() -> list[dict]:
    """Known constructed mutations. These are assertions to test, not DPO labels."""
    cases = [
        ("estimated-to-confirmed", {"date": "2026-11-20", "modality": "estimated"},
         "预计2026-11-20完成。", "已经确认2026-11-20完成。", "不得去掉预计限定"),
        ("unknown-to-success", {"operation": "create_followup", "state": "unknown"},
         "还不能确定跟进有没有建成。", "跟进已经建好了。", "未知不能改成成功"),
        ("one-receipt-two-success", {"first": "receipt-1", "second": None},
         "第一条跟进已经建好，第二条还没确认。", "两条跟进都建好了。", "一个回执不能覆盖两个独立创建目标"),
        ("read-failure-to-empty", {"read_status": "timeout", "coverage_complete": False},
         "读取超时了，还没查到待办。", "暂时没有待办。", "读取失败不能证明结果为空"),
        ("retry-promise-without-job", {"read_status": "timeout", "retry_queued": False},
         "刚才读取超时了，目前还没有结果。", "刚才超时了，我会自动重试。", "没有已安排任务不能承诺重试"),
        ("scope-removal", {"rule_scope": "演示项目甲", "expires": "2026-11-20"},
         "这条规则在演示项目甲暂时停用，到2026-11-20为止。", "以后所有项目都不再用这条规则。", "不能扩大范围或有效期"),
    ]
    return [{"id": "R3-CHALLENGE-" + ident, "family": "semantic_challenge",
             "facts": facts, "reference_surface": reference, "mutated_surface": mutation,
             "expected_check": rule, "synthetic": True,
             "label_origin": "constructed_counterexample", "human_preference": None,
             "training_approved": False, "dataset_role": "challenge_only_not_style_dpo"}
            for ident, facts, reference, mutation, rule in cases]
