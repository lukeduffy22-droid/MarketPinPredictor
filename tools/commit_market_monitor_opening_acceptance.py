"""Durably commit one opening-acceptance milestone from inspector evidence."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from backend.monitor_opening_acceptance import (  # noqa: E402
    RESULT_SCHEMA,
    commit_opening_acceptance_milestone,
)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Append a deterministic opening milestone receipt before atomically "
            "updating the session-scoped monitor state."
        )
    )
    parser.add_argument(
        "--milestone",
        required=True,
        choices=(
            "startup",
            "first_eligible_gamma_capture",
            "complete_5m_orb",
            "complete_60m_orb",
        ),
    )
    parser.add_argument(
        "--input",
        default="-",
        help="Compact inspect_opening_capture.py report path, or '-' for stdin.",
    )
    parser.add_argument("--project-root", type=Path, default=PROJECT_ROOT)
    parser.add_argument("--state-path", type=Path)
    parser.add_argument("--journal-dir", type=Path)
    parser.add_argument("--pretty", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    root = args.project_root.resolve()
    state_path = (
        args.state_path or root / "exports" / "market_monitor" / "state.json"
    ).resolve()
    journal_dir = (
        args.journal_dir or root / "exports" / "market_monitor"
    ).resolve()
    try:
        raw = (
            sys.stdin.read()
            if args.input == "-"
            else Path(args.input).read_text("utf-8")
        )
        report = json.loads(raw)
        if not isinstance(report, dict):
            raise ValueError("inspector report JSON must be an object")
        result = commit_opening_acceptance_milestone(
            milestone=args.milestone,
            inspector_report=report,
            state_path=state_path,
            journal_dir=journal_dir,
        )
    except (
        OSError,
        UnicodeError,
        json.JSONDecodeError,
        TypeError,
        ValueError,
        OverflowError,
    ) as exc:
        result = {
            "schema_version": RESULT_SCHEMA,
            "accepted": False,
            "action": "abstain",
            "issues": [f"input_error:{type(exc).__name__}:{exc}"],
        }
    print(
        json.dumps(
            result,
            sort_keys=True,
            indent=2 if args.pretty else None,
            separators=None if args.pretty else (",", ":"),
        )
    )
    return 0 if result.get("accepted") else 1


if __name__ == "__main__":
    raise SystemExit(main())
