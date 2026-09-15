"""Prepare the heartbeat's monitor namespace for one verified open session."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from backend.monitor_session_rollover import prepare_monitor_session  # noqa: E402


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Append a durable session-rollover event and atomically re-arm the "
            "MarketPin monitor state for a verified open session."
        )
    )
    parser.add_argument("--project-root", type=Path, default=PROJECT_ROOT)
    parser.add_argument("--state-path", type=Path)
    parser.add_argument("--journal-dir", type=Path)
    parser.add_argument("--session-date")
    parser.add_argument(
        "--observed-at-utc",
        help="Aware ISO-8601 timestamp; defaults to the current time.",
    )
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--pretty", action="store_true")
    return parser


def _observed_at(value: str | None) -> datetime | None:
    if value is None:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    root = args.project_root.resolve()
    state_path = (args.state_path or root / "exports" / "market_monitor" / "state.json").resolve()
    journal_dir = (args.journal_dir or root / "exports" / "market_monitor").resolve()
    try:
        observed = _observed_at(args.observed_at_utc)
    except ValueError as exc:
        result = {
            "schema_version": "marketpin-monitor-session-rollover.result.v1",
            "output_mode": "compact",
            "accepted": False,
            "action": "abstain",
            "issues": [f"observed_at_utc_invalid:{type(exc).__name__}"],
        }
    else:
        result = prepare_monitor_session(
            state_path=state_path,
            journal_dir=journal_dir,
            observed_at_utc=observed,
            session_date=args.session_date,
            dry_run=args.dry_run,
        )
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
