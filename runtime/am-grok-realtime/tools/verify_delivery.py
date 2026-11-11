#!/usr/bin/env python3
"""Read-only package integrity check. This tool never edits a running host."""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path, PurePosixPath
import sys


def verify(root: Path) -> dict:
    root = root.resolve()
    manifest_path = root / "FILE_MANIFEST.json"
    data = json.loads(manifest_path.read_text())
    if data.get("schema") != "am.grok.delivery.v1" or not isinstance(data.get("files"), list):
        raise ValueError("unsupported manifest")
    seen, errors, checked = set(), [], 0
    for item in data["files"]:
        name = item["path"]
        relative = PurePosixPath(name)
        if relative.is_absolute() or ".." in relative.parts or name in seen:
            errors.append({"path": name, "error": "unsafe_or_duplicate_path"})
            continue
        seen.add(name)
        path = root / name
        if not path.is_file() or path.is_symlink() or not path.resolve().is_relative_to(root):
            errors.append({"path": name, "error": "missing_or_unsafe_file"})
            continue
        content = path.read_bytes()
        if hashlib.sha256(content).hexdigest() != item["sha256"] or len(content) != item["bytes"]:
            errors.append({"path": name, "error": "content_mismatch"})
        checked += 1
    actual = {
        p.relative_to(root).as_posix() for p in root.rglob("*")
        if p.is_file() and p.name != "FILE_MANIFEST.json"
        and "__pycache__" not in p.parts and ".pytest_cache" not in p.parts
        and p.suffix not in {".pyc", ".pyo"}
    }
    for extra in sorted(actual - seen):
        errors.append({"path": extra, "error": "unlisted_file"})
    return {"status": "PASS" if not errors else "FAIL", "checked": checked, "errors": errors,
            "host_modified": False, "real_grok_called": False}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[1])
    args = parser.parse_args()
    result = verify(args.root)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if result["status"] == "PASS" else 1


if __name__ == "__main__":
    sys.exit(main())
