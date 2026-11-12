"""Run offline validation and bind its result to exact, secret-free source hashes.

This is a local provenance receipt, not a signature or deployment approval.
Runtime databases, captures, logs, environment files and model artifacts are
deliberately outside the source inventory. No application modules are imported.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
from datetime import datetime, timezone


SOURCE_DIRS = {"app", "backend", "tools", "tests", "docs", "config", ".github", ".streamlit", "scripts", "assets"}
SOURCE_SUFFIXES = {".py", ".ps1", ".psm1", ".md", ".json", ".toml", ".yml", ".yaml", ".css", ".html", ".js", ".ts", ".sql", ".txt"}
SECRET_ENV_NAMES = {"DATABENTO_API_KEY", "OPENAI_API_KEY", "POLYGON_API_KEY", "Massive_API", "MASSIVE_API_KEY"}


def digest(value: object) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()).hexdigest()


def source_path(relative: str) -> bool:
    path = Path(relative)
    if not path.parts or any(part.startswith(".env") for part in path.parts):
        return False
    if path.is_absolute() or ".." in path.parts:
        return False
    return (
        path.parts[0] in SOURCE_DIRS and path.suffix.lower() in SOURCE_SUFFIXES
    ) or (
        len(path.parts) == 1 and (
            path.suffix.lower() in {".py", ".ps1", ".psm1", ".toml", ".lock", ".md"}
            or path.name.startswith("requirements")
            or path.name in {".gitignore", ".gitattributes"}
        )
    )


def source_manifest(root: Path) -> dict[str, str | None]:
    """Include tracked deletions and eligible untracked source, never file contents."""
    root = root.resolve()
    raw = subprocess.check_output(
        ["git", "--no-optional-locks", "ls-files", "-c", "-o", "--exclude-standard", "-z"], cwd=root,
    )
    result = {}
    for relative in sorted(set(raw.decode("utf-8").split("\0"))):
        if not source_path(relative):
            continue
        path = root / relative
        if path.is_symlink() or not path.resolve().is_relative_to(root):
            raise ValueError(f"Source path escapes checkout: {relative}")
        result[relative] = hashlib.sha256(path.read_bytes()).hexdigest() if path.is_file() else None
    return result


def verify_receipt(root: Path, receipt: dict, *, receipt_path: Path | None = None) -> dict:
    claimed = receipt.get("receipt_sha256")
    body = {key: value for key, value in receipt.items() if key != "receipt_sha256"}
    current = source_manifest(root)
    bound = receipt.get("source_manifest", {})
    changed = sorted(key for key in set(current) | set(bound) if current.get(key) != bound.get(key) or (key in current) != (key in bound))
    reasons = []
    if claimed != digest(body):
        reasons.append("RECEIPT_DIGEST_MISMATCH")
    if receipt.get("schema") != "marketpin-source-validation.v1":
        reasons.append("RECEIPT_SCHEMA_UNSUPPORTED")
    if receipt.get("exit_code") != 0:
        reasons.append("VALIDATION_COMMAND_FAILED")
    if receipt.get("source_stable") is not True:
        reasons.append("SOURCE_CHANGED_DURING_VALIDATION")
    if digest(bound) != receipt.get("source_fingerprint"):
        reasons.append("SOURCE_MANIFEST_DIGEST_MISMATCH")
    if changed:
        reasons.append("CURRENT_SOURCE_DIFFERS")
    if receipt_path is None:
        reasons.append("VALIDATION_LOG_LOCATION_REQUIRED")
    else:
        log_path = receipt_path.resolve().parent / "validation.log"
        try:
            if hashlib.sha256(log_path.read_bytes()).hexdigest() != receipt.get("log_sha256"):
                reasons.append("VALIDATION_LOG_DIGEST_MISMATCH")
        except OSError:
            reasons.append("VALIDATION_LOG_UNAVAILABLE")
    return {"valid": not reasons, "reasons": reasons, "changed_paths": changed}


def run_validation(root: Path, output: Path, command: list[str], timeout: float = 900) -> dict:
    root, output = root.resolve(), output.resolve()
    if not command:
        raise ValueError("A validation command is required")
    # A unique run directory keeps a prior receipt and its logs immutable.
    output.mkdir(parents=True, exist_ok=False)
    before = source_manifest(root)
    env = os.environ.copy()
    for name in SECRET_ENV_NAMES:
        env[name] = ""
    env.update({
        "DATABASE_URL": "sqlite:///" + (output / "validation.sqlite").as_posix(),
        "PYTHONDONTWRITEBYTECODE": "1",
        "MARKETPIN_FORECAST_RESEARCH_AUTOSTART": "0",
        "MARKETPIN_SOURCE_VALIDATION": "1",
        "TMP": str(output), "TEMP": str(output),
    })
    started = datetime.now(timezone.utc).isoformat()
    log = output / "validation.log"
    failure = None
    with log.open("wb") as stream:
        try:
            completed = subprocess.run(command, cwd=root, env=env, stdout=stream, stderr=subprocess.STDOUT, timeout=timeout, check=False)
            exit_code = completed.returncode
        except subprocess.TimeoutExpired:
            exit_code, failure = 124, "VALIDATION_TIMEOUT"
        except OSError as exc:
            exit_code, failure = 126, type(exc).__name__
    after = source_manifest(root)
    receipt = {
        "schema": "marketpin-source-validation.v1",
        "started_at_utc": started,
        "finished_at_utc": datetime.now(timezone.utc).isoformat(),
        "head": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=root, text=True).strip(),
        "command": command,
        "exit_code": exit_code,
        "failure": failure,
        "source_manifest": before,
        "source_fingerprint": digest(before),
        "source_stable": before == after,
        "changed_during_validation": sorted(key for key in set(before) | set(after) if before.get(key) != after.get(key) or (key in before) != (key in after)),
        "log_sha256": hashlib.sha256(log.read_bytes()).hexdigest(),
        "scope": "Offline validation only; no live readiness, accuracy or promotion authority",
    }
    receipt["receipt_sha256"] = digest(receipt)
    (output / "receipt.json").write_text(json.dumps(receipt, indent=2) + "\n", encoding="utf-8")
    return receipt


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[1])
    sub = parser.add_subparsers(dest="action", required=True)
    run = sub.add_parser("run")
    run.add_argument("--output", type=Path, required=True, help="New run directory; never overwritten")
    run.add_argument("--timeout", type=float, default=900)
    run.add_argument("command", nargs=argparse.REMAINDER)
    verify = sub.add_parser("verify")
    verify.add_argument("receipt", type=Path)
    args = parser.parse_args(argv)
    if args.action == "verify":
        result = verify_receipt(args.root, json.loads(args.receipt.read_text(encoding="utf-8")), receipt_path=args.receipt)
        print(json.dumps(result, indent=2))
        return 0 if result["valid"] else 1
    command = args.command[1:] if args.command[:1] == ["--"] else args.command
    receipt = run_validation(args.root, args.output, command, args.timeout)
    print(json.dumps({"receipt": str(args.output / "receipt.json"), "exit_code": receipt["exit_code"], "source_stable": receipt["source_stable"]}))
    return 0 if receipt["exit_code"] == 0 and receipt["source_stable"] else 1


if __name__ == "__main__":
    sys.exit(main())
