"""Natural host fallback and semantic surfaces for known business outcomes.

This is a bounded realization grammar, not a general semantic correctness oracle.
New semantic paraphrases require a separately validated verifier or human review.
"""
from .contracts import SpeechFrame, StylePolicy, ContractError


def sentence(text):
    text = text.strip()
    return text if text[-1:] in "。！？?!；" else text + "。"


def unit(uid, texts, covers=None, authority="typed_host"):
    return {"id": uid, "covers": covers or [uid],
            "variants": list(dict.fromkeys(texts)), "authority": authority}


def fact_unit(fact):
    uid = fact["id"]
    unconsumed = {"scope", "time", "time_scope", "polarity", "business_state", "conflict_group"}
    if any(k in fact for k in unconsumed):
        # The host must first project these qualifiers into a reviewed atomic
        # statement; silently dropping any of them would change the proposition.
        raise ContractError("unprojected_semantic_qualifier")
    kind = fact.get("kind", "statement")
    subject = str(fact.get("subject", "")).strip()
    value = str(fact.get("value", "")).strip()
    modality = fact.get("modality", "recorded")
    if modality not in ("recorded", "confirmed", "estimated", "unverified"):
        raise ContractError("unsupported_fact_modality")
    if kind == "owner":
        if not subject or not value:
            raise ContractError("owner_without_subject_or_value")
        texts = [f"{subject}由{value}负责。", f"{value}负责{subject}。", f"{subject}的负责人是{value}。"]
    elif kind == "progress":
        if not subject or not value:
            raise ContractError("progress_without_subject_or_value")
        texts = [sentence(f"{subject}目前{value}"), sentence(f"{subject}的进展是：{value}")]
    elif kind == "date":
        if not subject or not value:
            raise ContractError("date_without_subject_or_value")
        texts = [sentence(f"{subject}的时间是{value}")]
    elif kind == "statement":
        if not value:
            raise ContractError("empty_statement")
        texts = [sentence(value)]
    else:
        raise ContractError("unsupported_fact_kind")
    # Recorded != necessarily doubtful; sources stay in provenance, not every sentence.
    # Modality is semantic: estimated/unverified cannot be removed for style.
    if modality == "estimated":
        texts = ["预计" + t for t in texts]
    if modality == "unverified":
        texts = [t.rstrip("。") + "，这点还没有确认。" for t in texts]
    return unit(uid, texts)


SUCCESS = {
    "create_followup": "{object}的跟进已建好。",
    "save_note": "{object}已记下。",
    "send_message": "{object}已发送。",
    "schedule_reminder": "{object}的提醒已设好。",
    "generate_document": "{object}已生成。",
    "publish_document": "{object}已发布。",
    "mount_document": "{object}已挂载。",
    "record_knowledge": "{object}已保存。",
    "cancel": "{object}已取消。",
    "update": "{object}已更新。",
}
ACTION_LABEL = {
    "create_followup": "建立跟进", "save_note": "记录", "send_message": "发送",
    "schedule_reminder": "设置提醒", "generate_document": "生成", "publish_document": "发布",
    "mount_document": "挂载", "record_knowledge": "保存", "cancel": "取消", "update": "更新",
}


def effect_unit(effect):
    kind, state = effect["kind"], effect["state"]
    obj = effect["object"]
    if kind not in SUCCESS or not isinstance(obj, str) or not obj.strip():
        raise ContractError("unsupported_effect")
    if state == "succeeded":
        receipt = effect.get("receipt", {})
        if (receipt.get("verified") is not True
                or not all(receipt.get(k) for k in ("id", "operation_id", "object_id"))):
            raise ContractError("success_without_bound_receipt")
        text = SUCCESS[kind].format(object=obj)
        if kind == "schedule_reminder":
            # The host verifies timezone and schedule; never infer a clock time.
            if not effect.get("schedule_text"):
                raise ContractError("reminder_without_schedule")
            text = f"{obj}的提醒已设好，时间是{effect['schedule_text']}。"
    elif state == "failed":
        reason = effect.get("reason")
        text = f"{obj}{ACTION_LABEL[kind]}失败了。"
        if reason:
            text = sentence(str(reason)) + text
        retry = effect.get("retry")
        if retry:
            if retry.get("verified") is not True or not retry.get("job_id"):
                raise ContractError("retry_without_scheduled_job")
            text += "已安排重试。"
    elif state == "unknown":
        text = f"{obj}是否{ACTION_LABEL[kind]}成功，目前还没确认。"
    elif state == "pending":
        text = f"{obj}还没{ACTION_LABEL[kind]}，等你确认后才能继续。"
    elif state == "active":
        if not effect.get("job_id"):
            raise ContractError("active_without_job")
        text = f"{obj}正在处理，还没完成。"
    elif state == "cancelled":
        if effect.get("cancel_verified") is not True:
            raise ContractError("cancel_without_receipt")
        text = f"{obj}的后续处理已停止。"
    else:
        raise ContractError("unsupported_effect_state")
    return unit(effect["id"], [text])


def gap_unit(gap):
    label = str(gap.get("object", "")).strip()
    slot = gap["slot"]
    templates = {
        "person": f"你说的“{label}”具体是哪位？有姓名或岗位编号吗？",
        "project": f"“{label}”是哪个项目？给我项目名称或编号就行。",
        "time": f"{label}需要在什么时间提醒？",
        "reminder_time": f"{label}需要在什么时间提醒？",
        "start_time": f"{label}准备什么时候开始？",
        "duration": f"{label}预计需要多久？",
        "time_window": f"{label}计划什么时候开始、什么时候结束？",
        "start_and_duration": f"{label}准备什么时候开始，预计需要多久？",
        "scope": f"你说的“{label}”有适用范围或时间限制吗？",
        "materials": f"关于{label}，你有什么资料可以提供给我吗？目前的信息还不够。",
        "content": f"{label}具体需要包含哪些内容？",
    }
    if slot not in templates or not label:
        raise ContractError("unsupported_gap")
    return unit(gap["id"], [templates[slot]])


def make_frame(*, binding, expires_at, question, family, facts=(), effects=(),
               gaps=(), previews=(), source_links=(), sources_requested=False,
               empty=None, context=(), policy=None):
    """Inputs come from the host's verified outcome projector, not an LLM.

    For statement facts, value must already be an atomic, scope-correct proposition.
    Do not pass an arbitrary source document into value and call it a verified fact.
    """
    policy = policy or StylePolicy()
    units, chosen, obligations = [], [], []
    fact_ids = [f["id"] for f in facts]
    if len(set(fact_ids)) != len(fact_ids):
        raise ContractError("duplicate_fact_id")
    for fact in facts:
        u = fact_unit(fact)
        units.append(u)
        if fact.get("required", True):
            chosen.append(u["id"])
            obligations.append(u["id"])
    success_receipts, success_operations, created_objects = [], [], []
    for effect in effects:
        u = effect_unit(effect)
        units.append(u); chosen.append(u["id"]); obligations.append(u["id"])
        if effect["state"] == "succeeded":
            success_receipts.append(effect["receipt"]["id"])
            success_operations.append((effect["kind"], effect["receipt"]["operation_id"]))
            if effect["kind"] == "create_followup":
                created_objects.append(effect["receipt"]["object_id"])
    if len(success_receipts) != len(set(success_receipts)):
        raise ContractError("one_receipt_cannot_complete_two_effects")
    if len(success_operations) != len(set(success_operations)):
        raise ContractError("one_operation_cannot_complete_two_effects")
    if len(created_objects) != len(set(created_objects)):
        raise ContractError("one_created_object_cannot_complete_two_goals")
    # Grouping preserves both independent receipts and both object identities.
    followups = [e for e in effects if e["kind"] == "create_followup" and e["state"] == "succeeded"]
    if len(followups) == 2:
        ids = [e["id"] for e in followups]
        text = f"这两个需求都建立好跟进了：{followups[0]['object']}、{followups[1]['object']}。"
        combined = unit("combined_followups", [text], covers=ids)
        units.append(combined)
        position = min(chosen.index(i) for i in ids)
        chosen = [i for i in chosen if i not in ids]
        chosen.insert(position, combined["id"])
    selected_gaps = sorted(gaps, key=lambda g: not g.get("blocking", True))[:policy.max_questions]
    for gap in selected_gaps:
        u = gap_unit(gap)
        units.append(u); chosen.append(u["id"]); obligations.append(u["id"])
    for preview in previews:
        u = unit(preview["id"], [preview["text"]], authority="exact_preview")
        units.append(u); chosen.append(u["id"]); obligations.append(u["id"])
    if empty:
        # Do not turn unavailable/partial/failed reads into "no todos".
        obj = empty.get("object", "相关信息")
        if empty.get("state") == "confirmed_empty" and empty.get("coverage_complete") is True:
            text = f"暂时没有{obj}。"
        elif empty.get("state") in ("not_read", "partial", "unavailable", "unknown"):
            text = f"{obj}目前还没查清。"
            if empty.get("reason"):
                text += sentence(empty["reason"])
        else:
            raise ContractError("unproven_absence")
        u = unit("empty_result", [text])
        units.append(u); chosen.insert(0, u["id"]); obligations.append(u["id"])
    if sources_requested or policy.show_sources == "always":
        for i, source in enumerate(source_links):
            # Preserve a host-validated source pointer exactly.
            text = f"{source['label']}：{source['url']}"
            u = unit(f"source:{i}", [text], authority="exact_preview")
            units.append(u); chosen.append(u["id"]); obligations.append(u["id"])
    if not chosen:
        raise ContractError("no_answer_or_known_gap")
    return SpeechFrame.freeze({
        "schema_version": "am-style-frame/1", "binding": binding, "expires_at": expires_at,
        "question": question, "family": family,
        "units": units, "canonical_units": chosen, "obligations": obligations,
        "context": list(context), "deferred_gap_count": max(0, len(gaps) - len(selected_gaps)),
        "sources_requested": sources_requested,
        "provenance": {"facts": list(facts), "effects": list(effects), "sources": list(source_links)},
    })


def render_canonical(frame):
    d = frame.data
    index = {u["id"]: u for u in d["units"]}
    pieces = [index[i]["variants"][0] for i in d["canonical_units"]]
    if d["family"] == "todos" and len(pieces) > 1:
        return "\n".join(f"{i + 1}. {text}" for i, text in enumerate(pieces))
    return "\n".join(pieces)
