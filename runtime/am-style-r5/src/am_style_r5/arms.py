"""Read existing local evidence to prepare real-run identities; never load a model."""
from __future__ import annotations

from copy import deepcopy
from pathlib import Path

from .common import R5Error, file_hash, object_hash, read_json


def build_runtime_arms(manifest_path, sft_run):
    """Create the three-arm identity file from a complete, same-base pilot.

    This does not assert that generation has occurred. The runners re-read the
    manifest, weights and checkpoint before execution. The full result/request
    hashes retain the chosen pilot, not just an arbitrary adapter filename.
    """
    from training.mlx_r4_contracts import validate_base
    from training.mlx_r4_resources import validate_service_paths
    from .runners import validate_sft_run
    manifest_path = Path(manifest_path).resolve()
    manifest = read_json(manifest_path)
    validate_base(manifest, hash_weights=False)
    validate_service_paths(manifest)
    chosen = validate_sft_run(sft_run, manifest)
    root = Path(chosen["root"])
    if file_hash(root / "result.json") != chosen["result_file_sha256"]:
        raise R5Error("Selected pilot result changed during arm preparation")
    return _compose_runtime_arms(manifest, chosen)


def _compose_runtime_arms(manifest, chosen):
    """Pure identity formatting, not a validation gate or evidence of execution.

    The public builder validates the actual files first. Keeping this operation
    separate lets metadata-only unit fixtures check three-arm attribution
    without pretending that a fake weight or pilot passed the real validators.
    """
    from training.mlx_r4_resources import PRODUCTION_MODEL, PRODUCTION_DIGEST
    from .runners import MLX_SETTINGS, QWEN_SETTINGS
    shared = {
        "base_revision": manifest["revision"],
        "model_manifest_sha256": object_hash(manifest),
        "tokenizer_sha256": manifest["files"]["tokenizer.json"]["sha256"],
        "generation_settings": deepcopy(MLX_SETTINGS),
        "production_equivalence_verified": False,
    }
    return {
        "production_q8": {
            "provider": "existing_original_coordinator_queue",
            "model": PRODUCTION_MODEL,
            "model_digest": PRODUCTION_DIGEST,
            "generation_settings": deepcopy(QWEN_SETTINGS),
            "comparison_role": "external_reference",
        },
        "mlx_base": {**deepcopy(shared), "adapter_sha256": None},
        "mlx_sft": {
            **deepcopy(shared), "adapter_sha256": chosen["adapter_sha256"],
            "pilot_request_sha256": chosen["request_sha256"],
            "pilot_result_file_sha256": chosen["result_file_sha256"],
            "train_file_sha256": chosen["train_sha256"],
            "dev_file_sha256": chosen["dev_sha256"],
        },
    }


def build_queue_config(manifest_path, host_config_path, queue_root, consumer_pid):
    """Bind the queue to protected original configs and an observed consumer PID.

    PID identification remains a real-machine observation by the operator; the
    builder does not discover, create, restart, or authenticate a consumer.
    """
    from .runners import validate_queue_config
    paths = [Path(p).expanduser() for p in (manifest_path, host_config_path, queue_root)]
    if any(not p.is_absolute() for p in paths):
        raise R5Error("Manifest, original host config and queue paths must be absolute")
    if type(consumer_pid) is not int or consumer_pid <= 0:
        raise R5Error("Supply the observed original consumer PID")
    manifest_path, host_config_path, queue_root = [p.resolve() for p in paths]
    config = {
        "schema": "am-r5-original-queue-config/1",
        "service_manifest_path": str(manifest_path),
        "service_manifest_sha256": file_hash(manifest_path),
        "host_config_path": str(host_config_path),
        "queue_root": str(queue_root),
        "expected_consumer_pid": consumer_pid,
        "operator_must_verify_single_original_consumer": True,
    }
    validate_queue_config(config)
    return config
