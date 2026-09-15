from __future__ import annotations

import argparse
import json
import sys
from datetime import date
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from backend.closing_tape.postclose import finalize_closed_session_if_due


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Idempotently upgrade a safely stopped MarketPin tape after the options session"
    )
    parser.add_argument("--project-root", default=str(PROJECT_ROOT))
    parser.add_argument("--trading-date", required=True)
    args = parser.parse_args(argv)
    report = finalize_closed_session_if_due(
        args.project_root, trading_day=date.fromisoformat(args.trading_date)
    )
    print(json.dumps(report, sort_keys=True, separators=(",", ":"), default=str))
    return 2 if report["action"] == "incomplete" else 0


if __name__ == "__main__":
    raise SystemExit(main())
