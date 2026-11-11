"""Count all admitted requests; API receipts are distinct from mobile visibility."""
import json
import math
from collections import Counter


def percentile(values,p):
    if not values: return None
    ordered=sorted(values)
    return ordered[max(0,math.ceil(len(ordered)*p)-1)]


def report(store):
    jobs=store.rows("SELECT * FROM gw_jobs WHERE task_kind='dialogue_reply' ORDER BY created_at")
    samples=[]
    for job in jobs:
        input_ref=json.loads(job["input_refs"])[0]
        item=store.one("SELECT received_at FROM gw_items WHERE item_id=?",(input_ref["item_id"],))
        received=item["received_at"] if item else job["created_at"]
        actions=store.rows("SELECT * FROM gw_outbox WHERE request_id=?",(job["request_id"],))
        final=next((a for a in actions if a["purpose"]=="final"),None)
        result=json.loads(job["result_json"]) if job["result_json"] else {}
        cls="complex" if result.get("decision_mode")=="submit_task" else "approval" if final and (final["reason"] or final["authorization_ref"]) else "ordinary"
        valid=bool(final and final["state"]=="sent" and job["state"]=="completed")
        samples.append({"job_id":job["job_id"],"class":cls,"state":job["state"],
            "action_state":final["state"] if final else None,"platform_id":final["platform_id"] if final else None,
            "queued_seconds":job["started_at"]-job["created_at"] if job["started_at"] else None,
            "routing_seconds":job["created_at"]-received,
            "adapter_seconds":job["completed_at"]-job["started_at"] if job["completed_at"] and job["started_at"] else None,
            "provider_accepted_at":job.get("provider_accepted_at"),
            "provider_started_at":job.get("provider_started_at"),
            "api_receipt_seconds":final["sent_at"]-received if valid else None,
            "mobile_visible_seconds":final["visible_at"]-received if valid and final["visible_at"] else None,
            "valid_api_result":valid})
    summary={}
    for cls in ("ordinary","approval","complex"):
        subset=[s for s in samples if s["class"]==cls]
        valid=[s["api_receipt_seconds"] for s in subset if s["valid_api_result"]]
        mobile=[s["mobile_visible_seconds"] for s in subset if s["mobile_visible_seconds"] is not None]
        summary[cls]={"total":len(subset),"valid_api_results":len(valid),
                      "incomplete_or_failed":len(subset)-len(valid),
                      "states":dict(Counter(s["state"] for s in subset)),
                      "successful_api_receipt_p50_seconds":percentile(valid,.5),
                      "successful_api_receipt_p95_seconds":percentile(valid,.95),
                      "mobile_evidence_count":len(mobile),"mobile_p50_seconds":percentile(mobile,.5),
                      "mobile_p95_seconds":percentile(mobile,.95),
                      "target_met":bool(subset and len(mobile)==len(subset)
                                        and percentile(mobile,.5)<=5 and percentile(mobile,.95)<=10) if cls=="ordinary" else None}
    return {"measurement":"API receipt is not mobile visibility; percentiles exclude missing results, counts retain them",
            "summary":summary,"samples":samples}
