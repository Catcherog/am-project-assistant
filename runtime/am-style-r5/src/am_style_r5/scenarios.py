"""Public, fictional calibration proposals. All new labels remain pending.

Group ownership is specified before rendering frames or creating candidates.
These 22 exposed examples are never an independent effectiveness holdout.
"""
from __future__ import annotations

from copy import deepcopy

from am_style.contracts import StylePolicy
from am_style.render import make_frame
from .common import object_hash


GROUP_SPLITS = {
    "r5-owner-stage-limit": "train",
    "r5-estimate-prerequisite": "train",
    "r5-partial-effect-unknown": "train",
    "r5-start-duration-gap": "train",
    "r5-cancel-scope-history": "train",
    "r5-conflicting-records": "dev",
    "r5-homonymous-projects": "dev",
    "r5-preview-version-change": "dev",
    "r5-unread-source-boundary": "dev",
}


def _statement(ident, text):
    return {"id": ident, "kind": "statement", "value": text}


def _success(ident, kind, obj):
    return {"id": ident, "kind": kind, "object": obj, "state": "succeeded",
            "receipt": {"id": "synthetic-receipt:" + ident,
                        "operation_id": "synthetic-operation:" + ident,
                        "object_id": "synthetic-object:" + ident, "verified": True}}


def calibration_specs():
    """Return 15 new train + 7 dev proposals; no human approval is inferred."""
    # R5 is a data/workflow version, not a change to the approved prompt policy.
    # Preserve the actual R4 gold policy so the original SFT loader can mix rows.
    policy = StylePolicy(version="owner-style/3.0-draft.1", realization="free")
    cases = []

    def add(group, num, question, facts, proposal, *, effects=(), gaps=(), previews=()):
        ident = "R5-CAL-" + group + "-" + str(num)
        facts, effects, gaps, previews = map(deepcopy, (facts, list(effects), list(gaps), list(previews)))
        source = {"facts": facts, "effects": effects, "gaps": gaps, "previews": previews,
                  "authority": "fictional_typed_fixture", "actual_business": False}
        frame = make_frame(binding={"turn_id": ident, "scope_id": "r5-public-calibration",
            "outcome_hash": object_hash(source), "source_revision": "r5-calibration/1",
            "revision": 0, "cancel_epoch": 0}, expires_at=2000000000,
            question=question, family="calibration", facts=facts, effects=effects,
            gaps=gaps, previews=previews, policy=policy)
        # Text here is a readable view of the typed fixture, not a separate source.
        fact_text = [f["value"] for f in facts]
        fact_text.extend(f"动作 {e['object']}：{e['state']}；回执为合成测试回执。" for e in effects)
        fact_text.extend(f"缺口 {g['object']}：{g['slot']}。" for g in gaps)
        cases.append({"id": ident, "group_id": group, "split": GROUP_SPLITS[group],
            "frame": frame.data, "style_policy": policy.as_dict(),
            "facts_for_review": "\n".join(fact_text), "proposal": proposal,
            "synthetic": True, "fixture_source": source,
            "independent_test_eligible": False, "human_approved": False})

    for i, (project, person) in enumerate((("样件项目甲", "林工"), ("装配项目乙", "周工"), ("测试项目丙", "顾工")), 1):
        add("r5-owner-stage-limit", i, f"{project}是谁负责的？",
            [_statement("stage-owner", f"{project}仅试制阶段由{person}负责"),
             _statement("volume-owner", f"{project}量产阶段的负责人尚未确认")],
            f"{project}的试制阶段由{person}负责，量产阶段的负责人还没确认。")

    for i, (project, date, dependency) in enumerate((("标定项目甲", "11月20日", "样件按计划到齐"),
            ("控制项目乙", "11月23日", "接口版本按计划冻结"),
            ("验证项目丙", "11月26日", "试验台按计划空出")), 1):
        add("r5-estimate-prerequisite", i, f"{project}能确定什么时候完成吗？",
            [_statement("estimate", f"{project}预计{date}完成测试"),
             _statement("prerequisite", f"上述预计以{dependency}为前提，目前尚不能保证该前提兑现")],
            f"{project}预计{date}完成测试，前提是{dependency}。这个条件还没确定，所以目前还不能承诺完成日期。")

    for i, (first, second) in enumerate((("样件组甲", "样件组乙"), ("工装组丙", "工装组丁"), ("验证组戊", "验证组己")), 1):
        add("r5-partial-effect-unknown", i, f"给{first}和{second}的两条通知都发出去了吗？",
            [_statement("reconcile", f"{second}的通知提交后超时，是否送达需要先查询回执")],
            f"给{first}的通知已发送。给{second}的那条提交后超时了，是否送达还没确认，需要先查回执。",
            effects=[_success("first-send", "send_message", f"给{first}的通知"),
                     {"id": "second-send", "kind": "send_message", "object": f"给{second}的通知", "state": "unknown"}])

    for i, activity in enumerate(("样件耐久测试", "驱动板复测", "工装标定试验"), 1):
        add("r5-start-duration-gap", i, f"{activity}这周能做完吗？",
            [_statement("ready", f"{activity}所需材料已经到齐"),
             _statement("unknown-window", f"资料尚未登记{activity}的开始时间和预计时长，因此暂时不能判断本周能否完成")],
            f"{activity}的材料已经到齐了，但还缺开始时间和预计时长。你准备什么时候开始，大概需要多久？有了这两项才能判断这周能否完成。",
            gaps=[{"id": "schedule-gap", "slot": "start_and_duration", "object": activity}])

    for i, (project, kept) in enumerate((("试制项目甲", "结项资料提醒"), ("电机项目乙", "评审准备提醒"), ("控制项目丙", "供应商交期提醒")), 1):
        add("r5-cancel-scope-history", i, f"只停止{project}的每日提醒，其他的继续。",
            [_statement("history", f"{project}之前已经发出的消息继续保留"),
             _statement("other", f"{kept}仍然有效，没有取消")],
            f"{project}的每日提醒已停止，之前发出的消息会保留。{kept}继续执行。",
            effects=[{"id": "cancel-daily", "kind": "cancel", "object": f"{project}的每日提醒",
                      "state": "cancelled", "cancel_verified": True}])

    add("r5-conflicting-records", 1, "仿真项目的验收到底是哪天？",
        [_statement("ledger", "仿真项目台账登记验收日期为11月18日"),
         _statement("minutes", "仿真项目会议纪要登记验收日期为11月21日"),
         _statement("unresolved", "两条记录的优先关系尚未确定，未取得日期变更的批准记录")],
        "仿真项目的日期有冲突：台账是11月18日，会议纪要是11月21日。目前还没有日期变更的批准记录，需要先核对哪一版有效。")
    add("r5-conflicting-records", 2, "传感项目已经结束测试了吗？",
        [_statement("ledger", "传感项目台账状态仍为测试中"),
         _statement("message", "一条工程师反馈称某台样件测试已经结束，但未说明是否覆盖全部样件"),
         _statement("unresolved", "目前不能据此确认整个项目测试已结束")],
        "目前还不能确认整个传感项目都测完了。台账仍是测试中，工程师只提到一台样件结束测试，还需要核对其余样件。")
    add("r5-homonymous-projects", 1, "帮我查一下校准项目的负责人。",
        [_statement("ambiguity", "目录中有两个名称同为校准项目的条目：项目A31属于机械组，项目B72属于软件组；用户未说明目标")],
        "有两个同名的校准项目：机械组的A31和软件组的B72。你要查哪一个？")
    add("r5-homonymous-projects", 2, "验证平台现在什么进度？",
        [_statement("ambiguity", "验证平台有两个独立项目：硬件平台H12和算法平台S45；本次没有足够信息选定其中一个")],
        "你指的是硬件平台H12，还是算法平台S45？这两个项目的进展需要分开查。")
    add("r5-preview-version-change", 1, "就按刚才那条发给测试组。",
        [_statement("old", "刚才预览绑定的是消息版本3"),
         _statement("changed", "当前草稿已改为版本4，正文发生变化，原确认不能用于新版"),
         _statement("not-sent", "版本3和版本4本轮均未发送")],
        "草稿已经改成版本4，和你刚才看到的内容不同。这条还没发送，需要先确认新版正文。")
    add("r5-preview-version-change", 2, "把刚确认的安排执行一下。",
        [_statement("changed-target", "预览时的接收对象是装配组甲，当前目标变为装配组乙"),
         _statement("approval-invalid", "原确认绑定装配组甲，不能用于装配组乙"),
         _statement("not-executed", "本轮没有向任一组发送该安排")],
        "接收对象已从装配组甲变成装配组乙，原来的确认不能沿用。目前还没发送，需要你确认新的接收对象和正文。")
    add("r5-unread-source-boundary", 1, "这个项目是不是没有遗留问题？",
        [_statement("coverage", "已读取结项摘要，其中没有列出遗留问题"),
         _statement("unread", "遗留问题清单附件尚未读取，当前资料覆盖不完整")],
        "结项摘要里没有列遗留问题，但附件清单还没读到。现在还不能判断没有遗留问题，需要先核对那份清单。")
    return cases


def evaluation_cases(specs):
    """Public examples for pipeline smoke tests, explicitly exposed calibration."""
    from am_style.prompt import build_messages, coordinator_messages
    return [{"case_id": s["id"], "group_id": s["group_id"], "split": "calibration",
             "frame": s["frame"], "style_policy": s["style_policy"],
             "logical_messages": build_messages(s["frame"], StylePolicy(**s["style_policy"])),
             "student_wire_messages": coordinator_messages(build_messages(s["frame"], StylePolicy(**s["style_policy"]))),
             "review_context": s["facts_for_review"], "public_exposed": True,
             "independent_test_eligible": False} for s in specs]
