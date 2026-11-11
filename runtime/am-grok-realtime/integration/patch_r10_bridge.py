"""Generate a minimal patch ONLY for the pinned, offline R10 delivery source.

This tool never writes into the supplied source tree. Hash mismatch means merge
against the current host instead; it is not permission to overwrite a newer AM.
"""
from __future__ import annotations

import argparse
import difflib
import hashlib
import json
from pathlib import Path


BRIDGE = "local-qwen3.8/feishu-bridge/bridge.py"
EXPECTED = {
    BRIDGE: "61f95674c8e5ba4469ba5ae83fc30db2e534788c49aec9aeea9f6f46af0a84d1",
    "local-qwen3.8/feishu-bridge/am_service.py": "f7e1736ab0d032c5dae34afc189094e85c33ef3452e3790d7fe67dbb952f91f6",
    "local-qwen3.8/feishu-bridge/group_ledger.py": "1ede70a488591bb1d7f4da4dc09fc332b2aebd5b4338a77e7bd31dde5b158760",
    "local-qwen3.8/feishu-bridge/am_query.py": "9a83c13e27fb1fa8535362287b3aae2d4be478fb2e0b2488b0de5cf54dc2ddcc",
    "local-qwen3.8/feishu-bridge/am_ux.py": "6e1d868182cdc2633f34384de20d2fe435973cc56016f8ad70b88d8dcabed772",
    "local-qwen3.8/feishu-bridge/am_followup_host.py": "f9216d47bf98522097ce37ae2f47fd4e3dfd869c82670bfff6faecb9a2ac367f",
    "local-qwen3.8/feishu-bridge/am_followup_text.py": "587aaca223d5f28f5ed1d16d95ffd7db5b5002731b6d3a0c61903b3889f2f3dc",
    "FS-AGENT-GROK-POC-20261017-01_R1_Studio/src/fs_agent/embedded.py": "28ba8d612033c7538465f2fe08c1df0b52c98acfab585679ee2ac31300938f05",
    "FS-AGENT-GROK-POC-20261017-01_R1_Studio/src/fs_agent/gateway.py": "616727f04030bd678e16c32b60ac81e4c2e2e8f6e572cc0600eda1379c830107",
    "FS-AGENT-GROK-POC-20261017-01_R1_Studio/src/fs_agent/store.py": "9e3ff5a7e3de0529f769f527b4dc4c236846bf4c2b6fd708e54d7451553d4d0b",
    "FS-AGENT-GROK-POC-20261017-01_R1_Studio/src/fs_agent/message_text.py": "ddd3890fac97382650b8fa7c345c144e94ac7d8f5ed4d35e5deacd475e1b2167",
    "FS-AGENT-GROK-POC-20261017-01_R1_Studio/src/fs_agent/reminder.py": "30e2529061522b668b6f1f5dbeda62dbfdee09ca4457af485a90614c49c7e95c",
    "FS-AGENT-GROK-POC-20261017-01_R1_Studio/src/am_harness/controls.py": "25b8458ecffb59b99bf8bfa1034933310bb6871d9614138e754d148c87ea08a7",
}

# Kept as narrow replacements for review. None changes a Qwen scheduler, model,
# business flag, approval flow, Feishu app identity, or websocket registration.
INGEST_ANCHOR = """        if self.information is not None:
            self.information.capture_verified(payload)
        if self.am_self is not None:
"""
INGEST_REPLACEMENT = """        if self.information is not None:
            self.information.capture_verified(payload)
        realtime = getattr(self, 'grok_realtime', None)
        if realtime is not None:
            admission = realtime.ingest(payload)
            if admission.handled:
                return admission.status != 'duplicate'
        if self.am_self is not None:
"""
REPLACEMENTS = (
    (INGEST_ANCHOR, INGEST_REPLACEMENT),
    (
        "    def on_message(event):\n",
        """    bridge.grok_realtime = None
    grok_settings = config.get('grok_realtime', {})
    if not isinstance(grok_settings, dict) or type(grok_settings.get('enabled', False)) is not bool:
        raise RuntimeError('INVALID_GROK_SETTINGS')
    if grok_settings.get('enabled', False):
        from integration.am_grok_host import install
        bridge.grok_realtime = install(bridge, config, reply)

    def on_message(event):
""",
    ),
    (
        """    def on_recalled(event):
        if bridge.information is not None:
            bridge.information.capture_verified(json.loads(lark.JSON.marshal(event)))
""",
        """    def on_recalled(event):
        payload = json.loads(lark.JSON.marshal(event))
        try:
            if bridge.grok_realtime is not None:
                bridge.grok_realtime.recall(payload)
        finally:
            if bridge.information is not None:
                bridge.information.capture_verified(payload)
""",
    ),
    (
        """    finally:
        bridge.stop.set()
        if bridge.poc is not None:
""",
        """    finally:
        bridge.stop.set()
        grok_stopped = True
        if bridge.grok_realtime is not None:
            grok_stop = bridge.grok_realtime.stop()
            grok_stopped = grok_stop['scheduler_stopped'] and not grok_stop['ports_in_flight']
        if bridge.poc is not None:
""",
    ),
    (
        "        information_stopped = bridge.information is None or bridge.information.close()\n",
        "        information_stopped = grok_stopped and (bridge.information is None or bridge.information.close())\n",
    ),
)


def patch_text(source: str) -> str:
    for before, after in REPLACEMENTS:
        if source.count(before) != 1:
            raise ValueError("R10_PATCH_ANCHOR_MISMATCH")
        source = source.replace(before, after, 1)
    compile(source, "r10_bridge_grok_candidate.py", "exec")
    return source


def generate(source_root: Path) -> tuple[str, dict]:
    for relative, expected in EXPECTED.items():
        target = source_root / relative
        if not target.is_file() or hashlib.sha256(target.read_bytes()).hexdigest() != expected:
            raise ValueError("R10_SOURCE_HASH_MISMATCH: " + relative)
    before = (source_root / BRIDGE).read_text(encoding="utf-8")
    after = patch_text(before)
    patch = "".join(difflib.unified_diff(before.splitlines(True), after.splitlines(True),
                                       fromfile="a/" + BRIDGE, tofile="b/" + BRIDGE))
    manifest = {
        "kind": "offline-r10-hash-bound-incremental-reference",
        "host_installed": False, "production_enabled": False,
        "required_source_sha256": EXPECTED,
        "changed_files": [BRIDGE],
        "bridge_after_sha256": hashlib.sha256(after.encode()).hexdigest(),
        "patch_sha256": hashlib.sha256(patch.encode()).hexdigest(),
        "source_tree_modified": False,
        "mismatch_action": "stop; review and merge the five local hook changes against the current host",
    }
    return patch, manifest


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-root", type=Path, required=True)
    parser.add_argument("--write-patch", type=Path)
    parser.add_argument("--write-manifest", type=Path)
    args = parser.parse_args()
    try:
        patch, manifest = generate(args.source_root)
    except (OSError, ValueError) as exc:
        raise SystemExit(str(exc)) from None
    for target, content in ((args.write_patch, patch),
                            (args.write_manifest, json.dumps(manifest, ensure_ascii=False, indent=2) + "\n")):
        if target is not None:
            # Patch generation is an artifact operation, never an install step.
            if args.source_root.resolve() in target.resolve().parents:
                raise SystemExit("PATCH_OUTPUT_MUST_BE_OUTSIDE_SOURCE_TREE")
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(content, encoding="utf-8")
    print(json.dumps(manifest, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
