from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from backend.closing_tape.finalize import finalize_closed_session


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description="Finalize one safely closed MarketPin DBN tape")
    result.add_argument("--project-root", default=str(PROJECT_ROOT))
    result.add_argument("--trading-date", required=True)
    result.add_argument("--session-id")
    result.add_argument("--feed-name", default="opra_options")
    result.add_argument("--rebuild-minutes", action="store_true")
    return result


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    report = finalize_closed_session(
        args.project_root,
        trading_date=args.trading_date,
        session_id=args.session_id,
        feed_name=args.feed_name,
        rebuild_minutes=args.rebuild_minutes,
    )
    print(json.dumps(report, sort_keys=True, indent=2, default=str))
    return 0 if report["complete"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
