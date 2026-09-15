"""Acknowledge data-quality notifications proven delivered in a prior turn."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from backend.monitor_data_quality_outbox import (  # noqa: E402
    ACK_RESULT_SCHEMA,
    ack_data_quality_notifications,
)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Append an exact prior-heartbeat delivery acknowledgement before "
            "removing data-quality notification IDs from the durable outbox."
        )
    )
    parser.add_argument("--input", default="-", help="Request JSON path or '-'.")
    parser.add_argument("--project-root", type=Path, default=PROJECT_ROOT)
    parser.add_argument("--state-path", type=Path)
    parser.add_argument("--journal-dir", type=Path)
    parser.add_argument("--pretty", action="store_true")
    return parser


def _aware_datetime(value: object) -> datetime:
    if not isinstance(value, str) or not value:
        raise ValueError("observed_at_utc must be a timezone-aware ISO timestamp")
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("observed_at_utc must be a timezone-aware ISO timestamp")
    return parsed


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
        request = json.loads(raw)
        required = {
            "session_date",
            "event_ids",
            "observed_at_utc",
            "delivery_proof",
        }
        if not isinstance(request, dict) or set(request) != required:
            raise ValueError(
                "acknowledgement request fields must be exactly: "
                + ",".join(sorted(required))
            )
        result = ack_data_quality_notifications(
            session_date=request["session_date"],
            event_ids=request["event_ids"],
            observed_at_utc=_aware_datetime(request["observed_at_utc"]),
            delivery_proof=request["delivery_proof"],
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
