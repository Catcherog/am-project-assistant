"""Behavior-based reply decisions; keywords are a narrow fallback, not a blanket gate."""
import re
from .models import ContractError

ACTION_INTENTS={"new_commitment","change_schedule","assign_work","external_followup","new_position",
                "quote_or_contract","disclose_unpublished","write_external"}
DECISIONS={"direct_reply","require_approval","submit_task"}


def decision(result, context, *, owner=False):
    mode=result.get("decision_mode","direct_reply")
    if mode not in DECISIONS:
        raise ContractError("invalid decision_mode")
    if any(k in result for k in ("target","recipients","authorization_ref")):
        raise ContractError("worker cannot choose recipients or grant authority")
    intents=result.get("action_intents",[])
    if not isinstance(intents,list):
        raise ContractError("action_intents must be a list")
    if set(intents)-ACTION_INTENTS:
        raise ContractError("unknown action intent")
    text=result.get("reply_text",result.get("display_text",""))
    if not isinstance(text,str) or len(text)>16000:
        raise ContractError("invalid reply text")
    if intents:
        return "require_approval","涉及新业务决定："+", ".join(intents)
    # Only explicit first-person commitments. Historical quotations and
    # qualified facts are allowed through when their cited fact is known.
    commitment=re.search(r"(?:^|[。！？\n])\s*(?:我|我们|AM)(?:保证|承诺|同意延期|决定将|安排你|批准)",text)
    if commitment:
        return "require_approval","草稿包含新的第一人称业务承诺"
    visible={f["fact_id"]:f for f in context.get("project_snapshot",[])}
    cited=result.get("cited_fact_ids",[])
    if not isinstance(cited,list) or any(fid not in visible for fid in cited):
        raise ContractError("cited fact outside context")
    bad=[visible[fid] for fid in cited if visible[fid]["status"]!="known"]
    if bad:
        return "require_approval","本次引用的依据存在待审冲突"
    # Staleness alone is not a universal approval gate. A dated historical
    # response can be delivered; claiming current state requires a fresh lookup.
    if any(visible[fid]["stale"] for fid in cited) and result.get("claims_current") is True:
        return "require_approval","拟作为最新口径使用的依据已过期"
    if mode=="require_approval":
        meta=result.get("approval_meta",{})
        return mode,str(meta.get("reason","模型请求核对本次口径"))[:1000]
    return mode,""
