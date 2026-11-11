"""Small human-calibration batches with immutable context and pending defaults.

This module never calls a model and never treats assistant suggestions as user
approval. Import accepts explicitly attributed human feedback; hashes bind its
scope but are not authentication of a person's identity.
"""
from __future__ import annotations

from copy import deepcopy
from datetime import datetime
import re

from .contracts import SpeechFrame, StylePolicy, digest as object_hash
from .data import (DataError, canonical, digest, export_training, split_for_group,
                   strict_loads)
from .prompt import build_messages
from .transport import (COORDINATOR_NATIVE, runtime_binding, student_contract_version,
                        student_prompt, validate_student_record)

SOURCE_REF = "7d8a8a8dda4cc42f7dd6260c2f10adb1f6405757"
BATCH_SCHEMA = "am-style-human-review-batch/1"
FEEDBACK_SCHEMA = "am-style-human-review-feedback/1"
REVIEW_GROUPS = (
    "note-saved-reminder-not-scheduled", "document-missing-inputs", "approval-draft-only",
    "outbound-readback-success", "cancel-future-preserve-history", "partial-second-outcome-unknown",
    "read-timeout-retry-actually-queued", "knowledge-rejection-scope",
)
PROPOSALS = {
    "note-saved-reminder-not-scheduled": "记下了。你想什么时候提醒？",
    "document-missing-inputs": "模板有了，还缺验收结果和遗留问题。你方便把这两项资料给我吗？",
    "approval-draft-only": "消息写好了，还没发送，等你确认。",
    "outbound-readback-success": "给林工的通知已发送。",
    "cancel-future-preserve-history": "后续提醒已经停了，之前发出的消息会保留。",
    "partial-second-outcome-unknown": "第一条建好了。第二条提交超时，还没确认是否建好，要先核对结果。",
    "read-timeout-retry-actually-queued": "刚才读取超时，还没查到进展。我会再查一次。",
    "knowledge-rejection-scope": "是只在这个项目里不用，还是所有场景都不用？需要停用到什么时候？",
}
FACT_SUMMARIES = {
    "note-saved-reminder-not-scheduled": "事项已保存并有回执；提醒时间尚缺，未建立定时提醒。",
    "document-missing-inputs": "结项报告模板已存在；还缺验收结果和遗留问题，未生成完整报告。",
    "approval-draft-only": "消息已写好但尚未发送；宿主在等待用户确认。",
    "outbound-readback-success": "给林工的通知已发送，并有与该动作对应的成功回执。",
    "cancel-future-preserve-history": "后续提醒已停止；之前发出的消息继续保留。",
    "partial-second-outcome-unknown": "第一条跟进已建好；第二条提交超时、结果未知，须先核对。",
    "read-timeout-retry-actually-queued": "刚才读取超时，尚未查到进展；真实的重试任务已排入队列。",
    "knowledge-rejection-scope": "用户提出不再采用一条规则；适用范围与时间限制尚不确定，尚未变更规则。",
}


def _group(case_id):
    match = re.fullmatch(r"R3-SEED-(.+)-([12])", case_id)
    if not match:
        raise DataError("review: unsupported source case ID")
    return match.group(1)


def _context(record):
    return {"source_case_id": record["source_case_id"], "group_id": record["group_id"],
            "split": record["split"], "frame_sha256": object_hash(record["frame"]),
            "policy_sha256": object_hash(record["style_policy"]),
            "prompt_sha256": object_hash(record["prompt"]),
            "target_binding_sha256": record["target_binding"]["binding_sha256"],
            "candidate_sha256": {k: v["sha256"] for k, v in record["candidates"].items()}}


def build_review_batch(frozen, comparison, gold_rows, *, source_files):
    """Join fixed R3 evidence and select eight train-group representatives.

    The two old human approvals stay references; malformed gap cases are repair
    work, not a forced preference question. No draft becomes a training label.
    """
    policy = StylePolicy(**frozen["policy"])
    by_id = {case["case_id"]: case for case in frozen["cases"]}
    if len(by_id) != len(frozen["cases"]):
        raise DataError("review: duplicate frozen case IDs")
    qwen = {row["case_id"]: row for row in comparison["rows"] if row["arm"] == "qwen"}
    if len(qwen) != len([r for r in comparison["rows"] if r["arm"] == "qwen"]) or set(qwen) != set(by_id):
        raise DataError("review: Qwen/frozen case join differs")
    records = {}
    for case_id, case in by_id.items():
        frame = SpeechFrame.freeze(case["frame"])
        row = qwen[case_id]
        logical = build_messages(frame, policy)
        if (row["frame_sha256"] != frame.fingerprint or row["prompt_sha256"] != object_hash(logical)
                or row["policy_sha256"] != object_hash(policy.as_dict())):
            raise DataError("review: frozen frame/prompt/policy hash mismatch")
        if (row.get("status") != "completed" or row.get("attempted") is not True
                or strict_loads(row["raw_text"]) != {"text": row["candidate_text"]}):
            raise DataError("review: original completed Qwen response is missing or inconsistent")
        group = _group(case_id)
        target = runtime_binding(frame, policy, profile=COORDINATOR_NATIVE)
        records[case_id] = {
            "id": "R4-REVIEW-" + case_id, "source_case_id": case_id, "group_id": group,
            "parent_group_id": group, "split": split_for_group(group), "synthetic": True,
            "source_sha256": object_hash({"frozen_case": case, "qwen_row": row}),
            "frame": frame.data, "style_policy": policy.as_dict(), "style_policy_version": policy.version,
            "prompt": student_prompt(target), "target_binding": target,
            "prompt_sha256": object_hash(student_prompt(target)),
            "host_contract_version": student_contract_version(target), "runtime_prompt_alignment": "aligned",
            "source_evaluation": {"bounded_decision": row["bounded_decision"],
                                  "bounded_reason": row["bounded_reason"], "human_review": row.get("human_review")},
            "candidates": {"qwen_r3": {"available": True, "text": row["candidate_text"],
                "sha256": digest(row["candidate_text"]), "origin": "model_call",
                "provider": row["provider"], "model": row["model"], "run_id": row["label"],
                "raw_response": row["raw_text"], "raw_response_sha256": digest(row["raw_text"]),
                "source_ref": SOURCE_REF}},
            "reviews": {}, "selection": {}, "status": "pending", "training_approved": False,
        }
    already_reviewed = []
    for gold in gold_rows:
        record = records.get(gold["id"])
        if record is None:
            raise DataError("review: approved source case is absent")
        provenance = gold["provenance"]
        completion = [{"role": "assistant", "content": canonical({"text": record["candidates"]["qwen_r3"]["text"]})}]
        if (gold["prompt"] != record["prompt"] or gold["completion"] != completion
                or provenance.get("split") != record["split"]
                or provenance.get("prompt_sha256") != object_hash(gold["prompt"])
                or provenance.get("candidate_sha256") != object_hash(completion)
                or not all(provenance.get(k) is True for k in ("human_approved", "facts_pass", "style_pass", "acceptable"))):
            raise DataError("review: existing approval does not match fixed source bytes")
        already_reviewed.append({"case_id": gold["id"], "split": provenance["split"],
            "status": "already_human_accepted_do_not_reask", "answer": record["candidates"]["qwen_r3"]["text"],
            "provenance": deepcopy(provenance), "eligible_as_anchor": provenance["split"] == "train"})
    if len(already_reviewed) != 2 or {x["case_id"] for x in already_reviewed} != {
        "R3-SEED-owner-and-relevant-progress-1", "R3-SEED-empty-todo-complete-read-1"}:
        raise DataError("review: preserve exactly the two supplied R3 gold approvals")
    selected = []
    for group in REVIEW_GROUPS:
        record = records["R3-SEED-" + group + "-1"]
        if record["split"] != "train":
            raise DataError("review: selected batch unexpectedly crosses a held-out split")
        proposal = PROPOSALS[group]
        record["candidates"]["proposed_revision"] = {"available": True, "text": proposal,
            "sha256": digest(proposal), "origin": "assistant_authored_proposal",
            "human_approved": False, "model_run": False}
        record["facts_for_review"] = FACT_SUMMARIES[group]
        record["review_context_sha256"] = object_hash(_context(record))
        selected.append(record)
    blocked = [{"case_id": case_id, "split": record["split"], "status": "host_projection_repair_then_regenerate",
                "original_answer": record["candidates"]["qwen_r3"]["text"],
                "reason": "The content-slot question asks what time information contains, not the missing start time/duration.",
                "reuse_as_same_prompt_dpo": False}
               for case_id, record in records.items() if record["group_id"] == "empty-analysis-known-gap"]
    batch = {"schema": BATCH_SCHEMA, "version": "R4-calibration-01", "source_ref": SOURCE_REF,
        "source_files": deepcopy(source_files), "review_type": "development_calibration_not_independent_test",
        "counts": {"pending_cases": len(selected), "already_human_accepted": len(already_reviewed),
                   "host_repair_cases": len(blocked), "new_human_approvals": 0, "new_trainable_rows": 0},
        "already_reviewed": already_reviewed, "host_repair": blocked,
        "unselected_case_ids": sorted(set(records) - {x["source_case_id"] for x in selected}
            - {x["case_id"] for x in already_reviewed} - {x["case_id"] for x in blocked}),
        "cases": selected}
    batch["batch_sha256"] = object_hash(batch)
    return batch


def validate_review_batch(batch):
    if not isinstance(batch, dict) or batch.get("schema") != BATCH_SCHEMA:
        raise DataError("review: invalid batch schema")
    content = {k: v for k, v in batch.items() if k != "batch_sha256"}
    if batch.get("batch_sha256") != object_hash(content):
        raise DataError("review: batch bytes changed")
    ids = set()
    for record in batch["cases"]:
        if record["id"] in ids:
            raise DataError("review: duplicate review ID")
        ids.add(record["id"])
        if (record.get("status") != "pending" or record.get("reviews") != {}
                or record.get("selection") != {} or record.get("training_approved") is not False):
            raise DataError("review: input batch must remain pending and cannot embed approvals")
        validate_student_record(record)
        for candidate in record["candidates"].values():
            if candidate["sha256"] != digest(candidate["text"]):
                raise DataError("review: candidate bytes changed")
        if record["review_context_sha256"] != object_hash(_context(record)):
            raise DataError("review: context binding differs")


def feedback_template(batch):
    validate_review_batch(batch)
    return {"schema": FEEDBACK_SCHEMA, "batch_sha256": batch["batch_sha256"],
        "origin": "pending", "reviewer": None, "reviewed_at": None,
        "ratings": [{"id": r["id"], "review_context_sha256": r["review_context_sha256"],
            "status": "pending", "accepted_candidate": None, "preference": None, "comment": "",
            "assessments": {k: {"candidate_sha256": c["sha256"], "facts_pass": None,
                "style_pass": None, "acceptable": None} for k, c in r["candidates"].items()}}
            for r in batch["cases"]]}


def import_feedback(batch, feedback):
    """Return new records/exports; never mutate an old batch or its gold files."""
    validate_review_batch(batch)
    if feedback.get("schema") != FEEDBACK_SCHEMA or feedback.get("batch_sha256") != batch["batch_sha256"]:
        raise DataError("review: feedback belongs to another batch")
    ratings = feedback.get("ratings")
    if not isinstance(ratings, list) or {r.get("id") for r in ratings} != {r["id"] for r in batch["cases"]} or len(ratings) != len(batch["cases"]):
        raise DataError("review: feedback case IDs differ")
    by_id = {r["id"]: r for r in ratings}
    active = any(r.get("status") != "pending" for r in ratings)
    if active:
        if feedback.get("origin") != "human" or not isinstance(feedback.get("reviewer"), str) or not feedback["reviewer"].strip():
            raise DataError("review: explicit human origin/reviewer is required")
        try:
            stamp = datetime.fromisoformat(feedback["reviewed_at"].replace("Z", "+00:00"))
        except (KeyError, AttributeError, TypeError, ValueError) as exc:
            raise DataError("review: timestamp with timezone is required") from exc
        if stamp.tzinfo is None:
            raise DataError("review: timestamp with timezone is required")
    output, repair, pending = [], [], []
    for original in batch["cases"]:
        record = deepcopy(original)
        rating = by_id[record["id"]]
        if rating.get("review_context_sha256") != record["review_context_sha256"]:
            raise DataError("review: feedback context changed")
        assessments = rating.get("assessments", {})
        if set(assessments) != set(record["candidates"]):
            raise DataError("review: assessment candidate set differs")
        for key, assessment in assessments.items():
            if assessment.get("candidate_sha256") != record["candidates"][key]["sha256"]:
                raise DataError("review: assessment text hash differs")
            if any(assessment.get(flag) is not None and type(assessment[flag]) is not bool
                   for flag in ("facts_pass", "style_pass", "acceptable")):
                raise DataError("review: assessment flags must be explicit booleans or null")
        status = rating.get("status")
        if status not in {"pending", "reviewed", "both_bad", "cannot_judge"}:
            raise DataError("review: unsupported review status")
        if status == "pending":
            if (rating.get("accepted_candidate") is not None or rating.get("preference") is not None
                    or any(a.get(f) is not None for a in assessments.values() for f in ("facts_pass", "style_pass", "acceptable"))):
                raise DataError("review: pending feedback cannot contain approval labels")
            pending.append(record["id"])
        else:
            for key, assessment in assessments.items():
                record["reviews"][key] = {"origin": "human", "human_approved": assessment.get("facts_pass") is True,
                    "reviewer": feedback["reviewer"], "reviewed_at": feedback["reviewed_at"],
                    "prompt_sha256": object_hash(record["prompt"]), "candidate_sha256": assessment["candidate_sha256"],
                    "facts_pass": assessment.get("facts_pass"), "style_pass": assessment.get("style_pass"),
                    "acceptable": assessment.get("acceptable"), "batch_sha256": batch["batch_sha256"],
                    "feedback_sha256": object_hash(feedback),
                    "frame_sha256": object_hash(record["frame"]), "policy_sha256": object_hash(record["style_policy"]),
                    "review_context_sha256": record["review_context_sha256"]}
            chosen = rating.get("accepted_candidate")
            preference = rating.get("preference")
            if status in {"both_bad", "cannot_judge"}:
                if chosen is not None or preference is not None:
                    raise DataError("review: rejected/unjudgeable case cannot contain a choice")
                repair.append({"id": record["id"], "status": status, "comment": rating.get("comment", "")})
            elif chosen is not None:
                if chosen not in assessments or not all(assessments[chosen].get(k) is True for k in ("facts_pass", "style_pass", "acceptable")):
                    raise DataError("review: relative preference does not certify absolute acceptance")
                record["selection"] = {"chosen": chosen}
                record["training_approved"] = True
                if preference is not None:
                    rejected = preference.get("rejected")
                    if (preference.get("chosen") != chosen or rejected == chosen or rejected not in assessments
                            or assessments[rejected].get("facts_pass") is not True):
                        raise DataError("review: style preference requires two distinct fact-checked responses")
                    record["selection"].update(rejected=rejected, source="human", reviewer=feedback["reviewer"],
                        prompt_sha256=object_hash(record["prompt"]), chosen_sha256=record["candidates"][chosen]["sha256"],
                        rejected_sha256=record["candidates"][rejected]["sha256"])
            elif preference is not None:
                raise DataError("review: preference lacks explicitly accepted chosen response")
            record["status"] = status
        output.append(record)
    exports = export_training(output)
    return {"schema": "am-style-human-review-import/1", "batch_sha256": batch["batch_sha256"],
            "feedback_sha256": object_hash(feedback), "records": output, "exports": exports,
            "repair_queue": repair, "pending_ids": pending,
            "existing_r3_gold_untouched": True, "independent_test_claim": False}


def markdown_review(batch):
    validate_review_batch(batch)
    url = f"https://github.com/Catcherog/project-workbench-review/blob/{SOURCE_REF}/AM-HARNESS-20261031-01/R3/evidence/comparison.json"
    lines = ["# R4 小批表达校准（8 个场景，全部待评）", "",
        "本批只收集人工意见，没有自动批准或训练。A 是固定 R3 真实 Qwen 答复；B 是本轮助手撰写的待审草案，未调用模型。全部业务事实为虚构情境。",
        "", f"[原始答复固定证据]({url})", "",
        "请分别判断事实是否准确、语气是否自然、能否直接采用；如有偏好，再选 A 或 B。可以两者都不合格或无法判断。偏好不等于绝对认可。",
        "", "已认可的 owner-1 和 empty-todo-1 不重复询问；两条错误的测试时间追问先修宿主并重生成。本批使用原训练场景组，不能作为独立验收。", ""]
    for i, r in enumerate(batch["cases"], 1):
        lines.extend([f"## {i}. {r['group_id']}", "", f"**用户问题：**{r['frame']['question']}", "",
            f"**冻结事实：**{r['facts_for_review']}", "", f"**A · 真实 Qwen：**{r['candidates']['qwen_r3']['text']}", "",
            f"**B · 助手待审草案：**{r['candidates']['proposed_revision']['text']}", "",
            "**待填写：**A/B 各自事实、语气、可直接采用；绝对认可的版本；可选偏好；修改意见。", "",
            f"`{r['id']}`", ""])
    return "\n".join(lines)
