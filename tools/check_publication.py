#!/usr/bin/env python3
"""Check this documentation preview. This does not run the AM assistant."""

import json
import re
import sys
from pathlib import Path
from urllib.parse import unquote, urlsplit


ROOT = Path(__file__).resolve().parents[1]
EXCLUDED = {".git", ".venv", "__pycache__"}
REQUIRED = {
    "README.md", "README.zh-CN.md", "LICENSE", "CONTRIBUTING.md",
    "GOVERNANCE.md", "SECURITY.md", "CODE_OF_CONDUCT.md",
    "docs/status.md", "docs/publishing.md", "docs/release-checklist.md",
    "examples/project-cases.jsonl", "research/protocol.md",
}
CASE_FIELDS = {
    "id", "family", "request", "evidence", "expected_behavior", "origin",
    "annotation_origin", "independent_review",
}
PATTERNS = {
    "private home path": re.compile("/" + r"(?:Users|home)/[A-Za-z0-9_.-]+"),
    "platform identifier": re.compile(r"\b(?:ou|oc|cli)_[A-Za-z0-9]{12,}\b"),
    "GitHub credential": re.compile(r"\b(?:gh[pousr]_[A-Za-z0-9]{20,}|github_pat_[A-Za-z0-9_]{20,})\b"),
    "private key": re.compile("-----BEGIN " + r"[A-Z ]*PRIVATE KEY-----"),
    "tenant document URL": re.compile(r"https://[^/\s]+\.feishu\.cn/(?:docx|sheets|base|file|wiki)/[A-Za-z0-9]+"),
}


def check(root):
    issues = []
    paths = []
    for path in root.rglob("*"):
        relative = path.relative_to(root)
        if any(part in EXCLUDED for part in relative.parts):
            continue
        if path.is_symlink():
            issues.append(f"{relative}: unexpected symlink")
        elif path.is_file():
            paths.append(path)
    names = {path.relative_to(root).as_posix() for path in paths}
    issues.extend(f"missing required file: {name}" for name in sorted(REQUIRED - names))
    for path in paths:
        relative = path.relative_to(root)
        if path.suffix in {".db", ".sqlite", ".sqlite3", ".log", ".zip", ".pem", ".key"} or path.name.startswith(".env"):
            issues.append(f"{relative}: unexpected private or binary artifact")
            continue
        try:
            content = path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            issues.append(f"{relative}: non-text file needs a separate publication review")
            continue
        for label, pattern in PATTERNS.items():
            if pattern.search(content):
                issues.append(f"{relative}: possible {label}")
        if path.suffix == ".md":
            for link in re.findall(r"\[[^\]]*\]\(([^)]+)\)", content):
                target = link.strip().strip("<>")
                if urlsplit(target).scheme or target.startswith("#"):
                    continue
                target = unquote(target.split("#", 1)[0])
                resolved = (path.parent / target).resolve()
                if root not in resolved.parents or not resolved.is_file():
                    issues.append(f"{relative}: missing or out-of-repository local link: {target}")

    case_file = root / "examples/project-cases.jsonl"
    cases = []
    if case_file.exists():
        seen = set()
        for number, line in enumerate(case_file.read_text(encoding="utf-8").splitlines(), 1):
            try:
                case = json.loads(line)
            except json.JSONDecodeError:
                issues.append(f"case line {number}: invalid JSON")
                continue
            if not isinstance(case, dict) or set(case) != CASE_FIELDS:
                issues.append(f"case line {number}: unexpected fields")
                continue
            if any(not isinstance(case[key], str) or not case[key].strip() for key in CASE_FIELDS - {"evidence"}):
                issues.append(f"case line {number}: expected nonempty text fields")
                continue
            if not isinstance(case["evidence"], list) or not case["evidence"] or any(not isinstance(x, str) or not x.strip() for x in case["evidence"]):
                issues.append(f"case line {number}: evidence must contain nonempty text")
            if case["id"] in seen:
                issues.append(f"case line {number}: duplicate case ID")
            seen.add(case["id"])
            if (case["origin"], case["annotation_origin"], case["independent_review"]) != ("fully_synthetic", "ai_assisted_proposed", "pending"):
                issues.append(f"case line {number}: preview provenance/review status changed; review publication scope")
            cases.append(case)
        if len(cases) != 12:
            issues.append("the preview documents twelve cases; update the scope and checker together")
    return issues, len(paths), len(cases)


if __name__ == "__main__":
    errors, files, examples = check(ROOT)
    if errors:
        print("Publication checks failed:")
        for error in errors:
            print("- " + error)
        sys.exit(1)
    print(f"Checked {files} text files and {examples} synthetic cases; local links and configured patterns passed.")
    print("This is a structure check, not semantic validation, a privacy guarantee, or an AM runtime test.")
