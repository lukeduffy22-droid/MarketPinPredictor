"""Print a fail-closed audit of retired gamma-model checkpoint bundles."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from backend.legacy_model_artifact_audit import audit_legacy_model_directory


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Audit legacy gamma_model_*.pt bundles without converting, "
            "promoting, or loading them into the live runtime"
        )
    )
    parser.add_argument("--project-root", default=str(PROJECT_ROOT))
    parser.add_argument(
        "--symbol",
        action="append",
        dest="symbols",
        help="required symbol to audit; may be repeated (default: SPX and NDX)",
    )
    parser.add_argument("--readiness-snapshot")
    parser.add_argument(
        "--snapshot-only",
        action="store_true",
        help=(
            "do not run the current read-only readiness audit; clearly label "
            "the cached readiness snapshot instead"
        ),
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="return exit code 2 while migration/readiness remains refused",
    )
    args = parser.parse_args(argv)

    audit = audit_legacy_model_directory(
        args.project_root,
        symbols=tuple(args.symbols or ("SPX", "NDX")),
        readiness_snapshot_path=args.readiness_snapshot,
        refresh_readiness=not args.snapshot_only,
    )
    print(json.dumps(audit.to_dict(), indent=2, sort_keys=True, allow_nan=False))
    if args.strict and not audit.migration_permitted:
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

