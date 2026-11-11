"""Auditable preference preparation. No model calls or implicit training labels.

The legacy blind review's SHA is over its exact JavaScript-embedded JSON payload,
not the pretty-printed file and not the rating file. Keep that distinction.
"""
from __future__ import annotations

from collections import Counter, defaultdict
from copy import deepcopy
from datetime import datetime
from difflib import SequenceMatcher
import hashlib
import json
import re
from pathlib import Path
from typing import Any, Iterable

SCHEMA = "am-style-data/1"
RATINGS_VERSION = "r2-fix1-blind/1"
PLACEHOLDER = "本轮没有可呈现的答复。"
CHOICES = {"A 更好", "B 更好", "相当", "两者都不合格", "无法评", None}
TRAINABLE_SPLITS = {"train", "dev"}


class DataError(ValueError):
    pass


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result = {}
    for key, value in pairs:
        if key in result:
            raise DataError(f"duplicate JSON key: {key}")
        result[key] = value
    return result


def strict_loads(text: str) -> Any:
    def bad_constant(value: str) -> None:
        raise DataError(f"non-finite JSON constant: {value}")
    return json.loads(text, object_pairs_hook=_unique_object, parse_constant=bad_constant)


def read_json(path: str | Path) -> Any:
    return strict_loads(Path(path).read_text(encoding="utf-8"))


def digest(text: str | bytes) -> str:
    return hashlib.sha256(text.encode("utf-8") if isinstance(text, str) else text).hexdigest()


def canonical(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)


def legacy_payload_sha256(rows: list[dict[str, Any]]) -> str:
    """Exact formula in fixed R2-FIX1/scripts/build_blind_review.py."""
    return digest(json.dumps(rows, ensure_ascii=False, allow_nan=False).replace("<", "\\u003c"))


def validate_ratings(ratings: dict, index: dict) -> None:
    if ratings.get("version") != RATINGS_VERSION:
        raise DataError("unsupported ratings version")
    if not re.fullmatch(r"[0-9a-f]{64}", str(index.get("dataset_sha256", ""))):
        raise DataError("invalid dataset SHA")
    if ratings.get("dataset_sha256") != index["dataset_sha256"]:
        raise DataError("rating/index dataset SHA mismatch")
    expected = index.get("case_ids", [])
    if len(expected) != len(set(expected)) or len(expected) != index.get("entries"):
        raise DataError("invalid index case identity")
    actual = ratings.get("ratings")
    if not isinstance(actual, dict) or set(actual) != set(expected):
        raise DataError("rating/index case IDs differ; missing rows are not negative labels")
    for case_id, row in actual.items():
        if row.get("rating") not in CHOICES:
            raise DataError(f"{case_id}: unknown rating")
        for flag in ("severe_error_a", "severe_error_b"):
            if type(row.get(flag)) is not bool:
                raise DataError(f"{case_id}: {flag} must be a boolean")
        if not isinstance(row.get("comment", ""), str):
            raise DataError(f"{case_id}: comment must be text")


def verify_pairs(rows: list[dict], index: dict) -> None:
    if not isinstance(rows, list) or legacy_payload_sha256(rows) != index["dataset_sha256"]:
        raise DataError("private pairs do not match frozen dataset SHA")
    ids = [row.get("case_id") for row in rows]
    if ids != index["case_ids"]:
        raise DataError("private pairs order/identity differs from frozen index")
    if sum(row.get("pair_available") is True for row in rows) != index["available_pairs"]:
        raise DataError("private available-pair count differs from frozen index")
    for row in rows:
        if type(row.get("pair_available")) is not bool:
            raise DataError("pair_available must be boolean")
        if not all(isinstance(row.get(k), str) for k in ("a", "b", "question")):
            raise DataError("private pair missing text")


def verify_blind_key(key: list[dict], metadata: dict, rows: list[dict], index: dict) -> dict:
    """Optional model attribution; A/B preference preparation does not require it."""
    if metadata.get("dataset_sha256") != index["dataset_sha256"]:
        raise DataError("key metadata belongs to another dataset")
    if [r.get("case_id") for r in key] != index["case_ids"]:
        raise DataError("key case IDs/order differ")
    lookup = {}
    for entry, row in zip(key, rows):
        if entry.get("number") != row.get("number"):
            raise DataError("key row number differs")
        if {entry.get("A"), entry.get("B")} != {"legacy", "candidate"}:
            raise DataError("key has invalid model assignment")
        lookup[entry["case_id"]] = {"A": entry["A"], "B": entry["B"]}
    # Legacy metadata has no signature/hash binding the key itself. Preserve this
    # limit: attribution is local evidence, not a cryptographic proof of A/B origin.
    return lookup


def analyze_ratings(ratings: dict, index: dict, rows: list[dict] | None = None,
                    key: list[dict] | None = None, key_metadata: dict | None = None) -> dict:
    validate_ratings(ratings, index)
    if rows is not None:
        verify_pairs(rows, index)
    if key is not None and (rows is None or key_metadata is None):
        raise DataError("key attribution requires verified pairs and key metadata")
    models = verify_blind_key(key, key_metadata, rows, index) if key is not None else {}
    pairs = {r["case_id"]: r for r in rows or []}
    counts = Counter()
    by_family = defaultdict(Counter)
    queues: dict[str, list] = {k: [] for k in (
        "relative_preference", "repair", "unrated", "tie_or_unjudgeable", "quarantined_winner")}
    severe = {"A": [], "B": []}
    pairable = 0
    for case_id in index["case_ids"]:
        rating = ratings["ratings"][case_id]
        choice = rating["rating"]
        name = choice or "未评分"
        counts[name] += 1
        by_family[case_id.split("-")[1]][name] += 1
        for side in ("A", "B"):
            if rating[f"severe_error_{side.lower()}"]:
                severe[side].append(case_id)
        item = {"case_id": case_id, "rating": choice, "comment": rating["comment"],
                "split": "calibration", "training_approved": False,
                "source_dataset_sha256": index["dataset_sha256"]}
        if choice is None:
            queues["unrated"].append(item)
        elif choice == "两者都不合格":
            queues["repair"].append(item)
        elif choice in ("A 更好", "B 更好"):
            side = choice[0]
            item["chosen_slot"] = side
            item["rejected_slot"] = "B" if side == "A" else "A"
            item["model_attribution"] = models.get(case_id)
            row = pairs.get(case_id)
            item["pair_available"] = row.get("pair_available") if row else None
            item["required_review"] = ["facts", "style", "absolute_acceptability"]
            if rating[f"severe_error_{side.lower()}"]:
                item["reason"] = "preferred answer also marked severe error"
                queues["quarantined_winner"].append(item)
            else:
                if row and row["pair_available"]:
                    left, right = row["a"], row["b"]
                    if left != right and PLACEHOLDER not in (left.strip(), right.strip()):
                        pairable += 1
                        item["chosen_text_sha256"] = digest(row[side.lower()])
                        item["rejected_text_sha256"] = digest(row[item["rejected_slot"].lower()])
                queues["relative_preference"].append(item)
        else:
            queues["tie_or_unjudgeable"].append(item)
    return {
        "schema": SCHEMA, "dataset_sha256": index["dataset_sha256"],
        "entries": index["entries"], "rated": index["entries"] - counts["未评分"],
        "counts": dict(counts), "by_family": {k: dict(v) for k, v in by_family.items()},
        "severe_error_flags": severe,
        "public_available_pairs": index["available_pairs"],
        "private_pairs_verified": rows is not None,
        "model_key_available": key is not None,
        "model_attribution_limit": "legacy key metadata lacks a key SHA binding",
        "relative_preferences_with_verified_nonempty_pairs": pairable if rows is not None else None,
        "sft_trainable_rows": 0, "dpo_trainable_pairs": 0,
        "split": "calibration", "queues": queues,
        "limits": [
            "A/B labels are presentation slots, not model identities.",
            "A preference is relative and does not certify absolute acceptability.",
            "False severe-error checkbox does not certify factual correctness.",
            f"Only {index['available_pairs']} of {index['entries']} public index entries have both actual answers.",
            "The feedback has informed design; this is development calibration, not a fresh test.",
            "Comments guide policy but are not paired full-response training targets.",
        ],
    }


def split_for_group(group_id: str, seed: str = "am-r3-split-v1") -> str:
    """Freeze group ownership BEFORE creating paraphrases/entities/counterfactuals."""
    if not group_id:
        raise DataError("group_id is required")
    bucket = int(digest(seed + "\0" + group_id)[:16], 16) / 2**64
    return "train" if bucket < .75 else "dev" if bucket < .875 else "sealed_test"


def validate_messages(messages: Any, *, completion: bool = False) -> None:
    if not isinstance(messages, list) or not messages:
        raise DataError("messages must be a nonempty list")
    for message in messages:
        if not isinstance(message, dict) or set(message) != {"role", "content"}:
            raise DataError("message must contain exactly role and content")
        if message["role"] not in {"system", "user", "assistant"}:
            raise DataError("unsupported message role")
        if not isinstance(message["content"], str) or not message["content"].strip():
            raise DataError("empty message content")
    if completion and (len(messages) != 1 or messages[0]["role"] != "assistant"):
        raise DataError("completion must be one assistant message")
    if not completion and messages[-1]["role"] != "user":
        raise DataError("prompt must end in user turn")


def normalized_prompt(record: dict) -> str:
    # Compare the logical task, not shared wrapper/system text. R3 native_chat
    # encodes system+user as JSON inside one user turn, so merely filtering the
    # outer roles would inflate near-duplicate scores across unrelated groups.
    messages = record["prompt"]
    if len(messages) == 1 and messages[0]["role"] == "user":
        try:
            envelope = strict_loads(messages[0]["content"])
        except (ValueError, TypeError):
            envelope = None
        if isinstance(envelope, dict) and set(envelope) == {"messages", "tools"} and envelope["tools"] == []:
            validate_messages(envelope["messages"])
            from .prompt import coordinator_messages
            if coordinator_messages(envelope["messages"]) == messages:
                messages = envelope["messages"]
    text = "\n".join(m["content"] for m in messages if m["role"] != "system")
    for entity in sorted(record.get("fact_entities", []), key=len, reverse=True):
        if entity:
            text = text.replace(entity, "<entity>")
    text = re.sub(r"\d+(?:[./:-]\d+)*", "<n>", text)
    return re.sub(r"\s+", "", text).lower()


def assert_split_integrity(records: Iterable[dict], *, similarity_threshold: float = .96) -> None:
    records = list(records)
    seen_ids: set[str] = set()
    group_splits: dict[str, str] = {}
    signatures: list[tuple[str, str, str]] = []
    for record in records:
        ident, group, split = record["id"], record["group_id"], record["split"]
        if not ident or ident in seen_ids:
            raise DataError("duplicate/empty record ID")
        seen_ids.add(ident)
        validate_messages(record["prompt"])
        if split not in {"train", "dev", "sealed_test", "calibration"}:
            raise DataError("unknown split")
        if record.get("parent_group_id", group) != group:
            raise DataError("derived sample must retain its parent's scenario group")
        if group in group_splits and group_splits[group] != split:
            raise DataError(f"group appears in multiple splits: {group}")
        group_splits[group] = split
        signature = normalized_prompt(record)
        for old_signature, old_split, old_id in signatures:
            if old_split == split:
                continue
            if signature == old_signature or SequenceMatcher(None, signature, old_signature).ratio() >= similarity_threshold:
                raise DataError(f"cross-split near duplicate: {old_id}, {ident}")
        signatures.append((signature, split, ident))


def _human_check(review: dict, text: str, prompt_sha: str, *, require_style: bool) -> bool:
    if not isinstance(review, dict):
        return False
    flags = ["human_approved", "facts_pass"] + (["style_pass", "acceptable"] if require_style else [])
    if review.get("origin") != "human" or not all(review.get(f) is True for f in flags):
        return False
    if review.get("candidate_sha256") != digest(text) or review.get("prompt_sha256") != prompt_sha:
        return False
    if not isinstance(review.get("reviewer"), str) or not review["reviewer"].strip():
        return False
    try:
        stamp = datetime.fromisoformat(review.get("reviewed_at", "").replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return False
    return stamp.tzinfo is not None


def _candidate(record: dict, key: str) -> tuple[str, dict] | None:
    candidate = record.get("candidates", {}).get(key)
    if not isinstance(candidate, dict) or candidate.get("available") is not True:
        return None
    text = candidate.get("text")
    if not isinstance(text, str) or not text.strip() or text.strip() == PLACEHOLDER:
        return None
    if candidate.get("sha256") != digest(text):
        raise DataError(f"{record['id']}: candidate bytes changed")
    if record.get("severe_error", {}).get(key) is True:
        return None
    return text, record.get("reviews", {}).get(key, {})


def align_runtime_prompt(record: dict, messages: list[dict], host_contract_version: str) -> dict:
    """Use build_messages(frame, policy), then invalidate any old prompt reviews.

    Caller must supply the actual host projection; this function does not invent
    an adapter for unavailable private business evidence.
    """
    validate_messages(messages)
    if not host_contract_version:
        raise DataError("host contract version is required")
    updated = deepcopy(record)
    if updated.get("reviews") or updated.get("selection"):
        updated.setdefault("review_history", []).append({
            "prompt_sha256": digest(canonical(record["prompt"])),
            "host_contract_version": record.get("host_contract_version"),
            "reviews": deepcopy(record.get("reviews", {})),
            "selection": deepcopy(record.get("selection", {})),
            "status": "superseded_by_prompt_reprojection",
        })
    updated["prompt"] = deepcopy(messages)
    updated["prompt_sha256"] = digest(canonical(messages))
    updated["runtime_prompt_alignment"] = "aligned"
    updated["host_contract_version"] = host_contract_version
    updated["reviews"] = {}
    updated["selection"] = {}
    updated["training_approved"] = False
    return updated


def export_training(records: list[dict]) -> dict:
    """Pure validation/export; no preference generation and no test-data training."""
    assert_split_integrity(records)
    outputs = {f"{kind}.{split}": [] for kind in ("sft", "dpo") for split in ("train", "dev")}
    skipped = []
    for record in records:
        ident, split = record["id"], record["split"]
        if split not in TRAINABLE_SPLITS:
            skipped.append({"id": ident, "reason": "held_out_or_calibration"})
            continue
        if record.get("runtime_prompt_alignment") != "aligned" or not record.get("host_contract_version"):
            skipped.append({"id": ident, "reason": "runtime_prompt_alignment_required"})
            continue
        target = None
        if "target_binding" in record:
            from .transport import validate_student_record
            target = validate_student_record(record)
        prompt_sha = digest(canonical(record["prompt"]))
        choice = record.get("selection", {})
        chosen_key = choice.get("chosen")
        selected = _candidate(record, chosen_key) if isinstance(chosen_key, str) else None
        if not selected or not _human_check(selected[1], selected[0], prompt_sha, require_style=True):
            skipped.append({"id": ident, "reason": "missing_valid_human_acceptance"})
            continue
        text, review = selected
        if target is not None and (review.get("frame_sha256") != target["frame_sha256"]
                or review.get("policy_sha256") != target["policy_sha256"]):
            skipped.append({"id": ident, "reason": "human_acceptance_does_not_bind_frame_policy"})
            continue
        source_sha = record.get("source_sha256", "")
        if not re.fullmatch(r"[a-f0-9]{64}", source_sha):
            raise DataError(f"{ident}: missing source SHA")
        policy = record.get("style_policy_version")
        if not isinstance(policy, str) or not policy:
            raise DataError(f"{ident}: missing style policy version")
        completion = [{"role": "assistant", "content": canonical({"text": text})}]
        provenance = {
            "schema": SCHEMA, "group_id": record["group_id"], "split": split,
            "source_kind": "human_reviewed_synthetic" if record.get("synthetic") else "human_reviewed_observation",
            "source_sha256": source_sha, "style_policy_version": policy,
            "reviewer": review["reviewer"], "reviewed_at": review["reviewed_at"],
            "human_approved": True, "facts_pass": True, "style_pass": True, "acceptable": True,
            "prompt_sha256": prompt_sha, "candidate_sha256": digest(canonical(completion)),
            "surface_text_sha256": digest(text), "host_contract_version": record["host_contract_version"],
            "runtime_alignment": "aligned",
        }
        if target is not None:
            provenance.update(target_transport_profile=target["profile"],
                target_binding_sha256=target["binding_sha256"],
                logical_prompt_sha256=target["logical_prompt_sha256"],
                frame_sha256=target["frame_sha256"], policy_sha256=target["policy_sha256"],
                review_context_sha256=review.get("review_context_sha256"),
                review_batch_sha256=review.get("batch_sha256"),
                feedback_sha256=review.get("feedback_sha256"))
        outputs[f"sft.{split}"].append({"id": ident, "prompt": record["prompt"],
            "completion": completion, "provenance": provenance})
        rejected_key = choice.get("rejected")
        rejected = _candidate(record, rejected_key) if isinstance(rejected_key, str) else None
        # A style DPO pair has two factually acceptable answers. Factual failures
        # belong to a separate safety dataset and are never silently mixed here.
        if rejected is None or rejected[0] == text or not _human_check(rejected[1], rejected[0], prompt_sha, require_style=False):
            skipped.append({"id": ident, "reason": "no_fact_checked_distinct_rejected_answer"})
            continue
        if target is not None and (rejected[1].get("frame_sha256") != target["frame_sha256"]
                or rejected[1].get("policy_sha256") != target["policy_sha256"]):
            skipped.append({"id": ident, "reason": "rejected_review_does_not_bind_frame_policy"})
            continue
        if (choice.get("source") != "human" or not choice.get("reviewer") or
                choice.get("prompt_sha256") != prompt_sha or
                choice.get("chosen_sha256") != digest(text) or
                choice.get("rejected_sha256") != digest(rejected[0])):
            skipped.append({"id": ident, "reason": "no_bound_human_preference"})
            continue
        rejected_completion = [{"role": "assistant", "content": canonical({"text": rejected[0]})}]
        pair_provenance = dict(provenance, chosen_sha256=digest(canonical(completion)), rejected_sha256=digest(canonical(rejected_completion)),
                               rejected_facts_pass=True, preference_source="human",
                               preference_reviewer=choice["reviewer"])
        outputs[f"dpo.{split}"].append({"id": ident, "prompt": record["prompt"],
            "chosen": completion,
            "rejected": rejected_completion, "provenance": pair_provenance})
    return {"schema": SCHEMA, "outputs": outputs, "skipped": skipped}


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2, allow_nan=False) + "\n", encoding="utf-8")


def write_jsonl(path: Path, rows: list[dict]) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = "".join(canonical(row) + "\n" for row in rows)
    path.write_text(payload, encoding="utf-8")
    return digest(payload)
