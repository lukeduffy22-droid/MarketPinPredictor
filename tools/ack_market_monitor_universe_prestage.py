"""Acknowledge a universe pre-stage alert proven delivered in a prior turn."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from backend.monitor_universe_prestage import (  # noqa: E402
    ACK_RESULT_SCHEMA,
    ack_universe_prestage_notifications,
)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default="-")
    parser.add_argument("--project-root", type=Path, default=PROJECT_ROOT)
    parser.add_argument("--journal-dir", type=Path)
    parser.add_argument("--pretty", action="store_true")
    return parser


def _aware(value: object) -> datetime:
    if not isinstance(value, str):
        raise ValueError("observed_at_utc must be a string")
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("observed_at_utc must include an offset")
    return parsed


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    root = args.project_root.resolve()
    journal_dir = (
        args.journal_dir
        or root / "exports" / "market_monitor" / "universe_prestage"
    ).resolve()
    try:
        raw = sys.stdin.read() if args.input == "-" else Path(args.input).read_text("utf-8")
        request = json.loads(raw)
        if not isinstance(request, dict) or set(request) != {
            "session_date",
            "event_ids",
            "observed_at_utc",
            "delivery_proof",
        }:
            raise ValueError("acknowledgement request fields invalid")
        result = ack_universe_prestage_notifications(
            session_date=request["session_date"],
            event_ids=request["event_ids"],
            observed_at_utc=_aware(request["observed_at_utc"]),
            delivery_proof=request["delivery_proof"],
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
            "schema_version": ACK_RESULT_SCHEMA,
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
