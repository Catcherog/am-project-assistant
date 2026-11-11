"""Recover immutable per-attempt records without starting or stopping workers."""
from __future__ import annotations

from collections import Counter
import hashlib
from pathlib import Path

from .common import R5Error, check_seal, file_hash, loads, seal, write_jsonl
from .evaluation import import_results, validate_plan


def _direct_records(directory):
    paths = sorted(directory.glob("*-record.json"), key=lambda path: path.name)
    for path in paths:
        if path.is_symlink() or not path.is_file():
            raise R5Error("record must be a direct regular file: " + str(path))
    return paths


def collect_run_records(plan, run_directories, output_path):
    """Validate and copy only original direct-child *-record.json records to JSONL.

    No fallback to partial results.jsonl, recursive scan, duplicate selection, model
    invocation or process inspection is performed. Missing rows appear in the return
    summary only. The caller must separately reconcile any unconfirmed live worker.
    """
    validate_plan(plan)
    if isinstance(run_directories, (str, Path)):
        raise R5Error("run_directories must be an explicit list of directories")
    directories = list(run_directories)
    if not directories:
        raise R5Error("at least one explicit run directory is required")
    roots, observed_files = [], {}
    for raw in directories:
        requested = Path(raw)
        if requested.is_symlink() or not requested.is_dir():
            raise R5Error("run directory is missing, non-directory or symlink: " + str(requested))
        directory = requested.resolve()
        if directory in roots:
            raise R5Error("duplicate run directory: " + str(directory))
        roots.append(directory)
        observed_files[directory] = _direct_records(directory)
    output_path = Path(output_path)
    if output_path.exists() or output_path.is_symlink():
        raise R5Error("collection output must be a fresh file: " + str(output_path))
    if output_path.name.endswith("-record.json"):
        raise R5Error("collection JSONL must not masquerade as an original per-attempt record")
    records, sources = [], []
    for directory in roots:
        for path in observed_files[directory]:
            raw = path.read_bytes()
            try:
                record = check_seal(loads(raw.decode("utf-8")))
            except (ValueError, TypeError, UnicodeError) as exc:
                raise R5Error("damaged record; collection refused: " + str(path)) from exc
            records.append(record)
            sources.append({"path": str(path), "sha256": hashlib.sha256(raw).hexdigest(),
                            "record_sha256": record["sha256"], "case_id": record.get("case_id"),
                            "arm": record.get("arm"), "status": record.get("status")})
    # The ordinary importer checks every original input/output/artifact binding and
    # rejects duplicate case/arm pairs, including worker/parent observation races.
    imported = import_results(plan, records)
    for directory in roots:
        if _direct_records(directory) != observed_files[directory]:
            raise R5Error("record file set changed during collection; reconcile before retrying")
    for source in sources:
        if file_hash(source["path"]) != source["sha256"]:
            raise R5Error("record changed during collection: " + source["path"])
    output_sha = write_jsonl(output_path, records)
    return seal({"schema_version": "am-r5-collected-records/1", "plan_sha256": plan["sha256"],
                 "mode": plan["mode"], "run_directories": [str(path) for path in roots],
                 "planned": plan["expected_results"], "recorded": len(records),
                 "missing": plan["expected_results"] - len(records),
                 "recorded_statuses": dict(Counter(record["status"] for record in records)),
                 "all_planned_statuses": dict(Counter(row["status"] for row in imported["rows"])),
                 "sources": sources, "output_path": str(output_path.resolve()), "output_sha256": output_sha,
                 "source_records_unchanged": True, "synthetic_missing_records_written": 0,
                 "new_model_calls": 0, "workers_started": 0, "workers_stopped": 0,
                 "current_worker_stop_status": "not_checked",
                 "recovery_boundary": "Collects recorded evidence only; stop_unknown never proves that a worker stopped."})
