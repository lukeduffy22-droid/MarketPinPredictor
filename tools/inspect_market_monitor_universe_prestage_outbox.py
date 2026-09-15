"""Read and validate one universe pre-stage notification outbox."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from backend.monitor_universe_prestage import (  # noqa: E402
    MonitorUniversePrestageError,
    inspect_universe_prestage_outbox,
)


OUTBOX_SCHEMA = "marketpin-monitor-universe-prestage-outbox.v1"


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("--session-date", required=True)
    parser.add_argument("--project-root", type=Path, default=PROJECT_ROOT)
    parser.add_argument("--journal-dir", type=Path)
    parser.add_argument("--pretty", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    root = args.project_root.resolve()
    journal_dir = (
        args.journal_dir
        or root / "exports" / "market_monitor" / "universe_prestage"
    ).resolve()
    try:
        result = inspect_universe_prestage_outbox(
            session_date=args.session_date,
            journal_dir=journal_dir,
        )
    except (
        MonitorUniversePrestageError,
        OSError,
        UnicodeError,
        TypeError,
        ValueError,
        OverflowError,
    ) as exc:
        result = {
            "schema_version": OUTBOX_SCHEMA,
            "read_only": True,
            "status": "ERROR",
            "session_date": args.session_date,
            "journal_path": str(journal_dir / f"{args.session_date}.jsonl"),
            "event": None,
            "ack": None,
            "pending_notification_event_ids": None,
            "issues": [f"outbox_error:{type(exc).__name__}:{exc}"],
        }
    print(
        json.dumps(
            result,
            sort_keys=True,
            indent=2 if args.pretty else None,
            separators=None if args.pretty else (",", ":"),
        )
    )
    return 0 if result.get("status") == "OK" else 1


if __name__ == "__main__":
    raise SystemExit(main())
