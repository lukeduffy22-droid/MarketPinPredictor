"""Fail closed when a GitHub-bound change contains runtime or secret material."""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
MAX_BLOB_BYTES = 50 * 1024 * 1024

FORBIDDEN_PREFIXES = (
    ".codex_tmp/",
    ".codex-tmp/",
    ".history/",
    ".playwright-cli/",
    ".pytest_cache/",
    ".snapshots/",
    ".venv/",
    "Git/",
    "attached_assets/",
    "backups/",
    "data/",
    "exports/",
    "exports_",
    "historical_data/",
    "logs/",
    "models/",
    "output/",
    "outputs/",
    "plugins/dist/",
    "pytorch/",
    "research/outputs/",
    "tmp/",
    "vision/",
)
FORBIDDEN_SEGMENTS = {"__pycache__", "node_modules"}
FORBIDDEN_SUFFIXES = (
    ".arrow",
    ".bundle",
    ".db",
    ".db-journal",
    ".db-memo",
    ".db-shm",
    ".db-wal",
    ".dbn",
    ".dbn.zst",
    ".dll",
    ".docx",
    ".exe",
    ".feather",
    ".joblib",
    ".jsonl",
    ".log",
    ".ndjson",
    ".onnx",
    ".parquet",
    ".pdf",
    ".pickle",
    ".pkl",
    ".pt",
    ".pth",
    ".pyc",
    ".pyd",
    ".sqlite",
    ".sqlite3",
    ".wasm",
    ".xlsx",
    ".zip",
)

SECRET_PATTERNS = (
    ("OpenAI-style token", re.compile(r"\bsk-[A-Za-z0-9_-]{20,}\b")),
    ("GitHub token", re.compile(r"\b(?:gh[pousr]_[A-Za-z0-9]{20,}|github_pat_[A-Za-z0-9_]{20,})\b")),
    ("AWS access key", re.compile(r"\b(?:AKIA|ASIA)[A-Z0-9]{16}\b")),
    (
        "literal credential assignment",
        re.compile(
            r"(?i)\b(?:api[_-]?key|secret(?:[_-]?key)?|access[_-]?token|auth[_-]?token)"
            r"\b\s*[:=]\s*[rubf]*[\"']([^\"']{16,})[\"']"
        ),
    ),
)
PLACEHOLDER_MARKERS = (
    "${",
    "<",
    "changeme",
    "dummy",
    "example",
    "fake",
    "placeholder",
    "replace",
    "test",
    "your",
)


def run_git(*args: str, input_text: str | None = None) -> str:
    result = subprocess.run(
        ["git", *args],
        cwd=ROOT,
        input=input_text,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    if result.returncode:
        detail = result.stderr.strip() or result.stdout.strip()
        raise RuntimeError(f"git {' '.join(args)} failed: {detail}")
    return result.stdout


def collect_candidate_paths(base: str) -> list[str]:
    paths: set[str] = set()
    commands = (
        ("diff", "--name-only", "--diff-filter=ACMR", f"{base}..HEAD"),
        ("diff", "--name-only", "--diff-filter=ACMR", "HEAD"),
        ("diff", "--cached", "--name-only", "--diff-filter=ACMR", "HEAD"),
        ("ls-files", "--others", "--exclude-standard"),
    )
    for command in commands:
        paths.update(line.strip() for line in run_git(*command).splitlines() if line.strip())
    return sorted(paths)


def path_problem(path: str) -> str | None:
    normalized = path.replace("\\", "/")
    lowered = normalized.lower()
    if lowered == ".env" or (lowered.startswith(".env.") and lowered != ".env.example"):
        return "environment secret file"
    if normalized.startswith("~$"):
        return "Office temporary file"
    for prefix in FORBIDDEN_PREFIXES:
        if lowered.startswith(prefix.lower()):
            return f"forbidden runtime path prefix {prefix}"
    if FORBIDDEN_SEGMENTS.intersection(lowered.split("/")):
        return "generated dependency or bytecode directory"
    if lowered.endswith(FORBIDDEN_SUFFIXES):
        return "forbidden generated or binary file type"
    return None


def secret_findings(path: str, absolute: Path) -> list[str]:
    if not absolute.is_file() or absolute.stat().st_size > 2 * 1024 * 1024:
        return []
    try:
        text = absolute.read_text(encoding="utf-8-sig")
    except (UnicodeDecodeError, OSError):
        return []

    findings: list[str] = []
    for line_number, line in enumerate(text.splitlines(), start=1):
        lowered = line.lower()
        for label, pattern in SECRET_PATTERNS:
            match = pattern.search(line)
            if not match:
                continue
            candidate = match.group(1).lower() if match.lastindex else match.group(0).lower()
            if any(marker in candidate or marker in lowered for marker in PLACEHOLDER_MARKERS):
                continue
            findings.append(f"{path}:{line_number}: possible {label}")
    return findings


def audit_new_objects(base: str) -> list[str]:
    object_lines = run_git("rev-list", "--objects", f"{base}..HEAD").splitlines()
    if not object_lines:
        return []
    paths_by_oid: dict[str, str] = {}
    object_ids: list[str] = []
    for line in object_lines:
        oid, _, path = line.partition(" ")
        object_ids.append(oid)
        if path:
            paths_by_oid.setdefault(oid, path)
    metadata = run_git(
        "cat-file",
        "--batch-check=%(objectname) %(objecttype) %(objectsize)",
        input_text="\n".join(object_ids) + "\n",
    )
    findings: list[str] = []
    for line in metadata.splitlines():
        oid, object_type, raw_size = line.split()
        if object_type != "blob":
            continue
        size = int(raw_size)
        if size > MAX_BLOB_BYTES:
            path = paths_by_oid.get(oid, "<unknown path>")
            findings.append(f"{path}: new blob is {size} bytes (limit {MAX_BLOB_BYTES})")
    return findings


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base", default="origin/friday-1/9")
    args = parser.parse_args()

    findings: list[str] = []
    try:
        run_git("merge-base", "--is-ancestor", args.base, "HEAD")
        paths = collect_candidate_paths(args.base)
        for path in paths:
            problem = path_problem(path)
            if problem:
                findings.append(f"{path}: {problem}")
                continue
            absolute = ROOT / Path(path)
            if absolute.is_file() and absolute.stat().st_size > MAX_BLOB_BYTES:
                findings.append(
                    f"{path}: working file is {absolute.stat().st_size} bytes "
                    f"(limit {MAX_BLOB_BYTES})"
                )
                continue
            findings.extend(secret_findings(path, absolute))
        findings.extend(audit_new_objects(args.base))
        for diff_args in (
            ("diff", "--check", f"{args.base}..HEAD"),
            ("diff", "--check"),
            ("diff", "--cached", "--check"),
        ):
            output = run_git(*diff_args).strip()
            if output:
                findings.append(f"git {' '.join(diff_args)} reported:\n{output}")
    except RuntimeError as exc:
        findings.append(str(exc))

    if findings:
        print("Publication boundary check FAILED:", file=sys.stderr)
        for finding in findings:
            print(f"- {finding}", file=sys.stderr)
        return 1

    print(f"Publication boundary check passed for {len(paths)} changed paths against {args.base}.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
