from __future__ import annotations

import argparse
import json
import math
import os
import sys
from datetime import date
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from backend.closing_tape.historical_backfill import inclusive_dates
from backend.closing_tape.config import _market_times
from backend.closing_tape.historical_bulk import (
    execute_historical_bulk,
    plan_historical_bulk,
)


def _parse_date(raw: str) -> date:
    try:
        return date.fromisoformat(raw)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid ISO date: {raw}") from exc


def _requested_dates(args: argparse.Namespace) -> tuple[date, ...]:
    if args.date:
        requested = tuple(dict.fromkeys(args.date))
        for trading_day in requested:
            try:
                _market_times(trading_day)
            except ValueError as exc:
                raise ValueError(
                    f"explicit date {trading_day.isoformat()} is not a configured "
                    "US cash-market session"
                ) from exc
        return requested
    if args.start is None or args.end is None:
        raise ValueError("provide --date or both --start and --end")
    configured_sessions: list[date] = []
    for trading_day in inclusive_dates(args.start, args.end):
        try:
            _market_times(trading_day)
        except ValueError:
            continue
        configured_sessions.append(trading_day)
    if not configured_sessions:
        raise ValueError("requested range contains no configured US cash-market sessions")
    return tuple(configured_sessions)


def _finite_nonnegative(raw: str) -> float:
    try:
        value = float(raw)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            "must be a finite nonnegative number"
        ) from exc
    if not math.isfinite(value) or value < 0:
        raise argparse.ArgumentTypeError("must be a finite nonnegative number")
    return value


def _positive_integer(raw: str) -> int:
    try:
        value = int(raw)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be a positive integer") from exc
    if value <= 0:
        raise argparse.ArgumentTypeError("must be a positive integer")
    return value


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(
        description=(
            "Plan or execute a resumable, hash-approved Databento historical "
            "acquisition/import batch. The default is a no-download, no-journal dry run."
        )
    )
    result.add_argument("--project-root", default=str(PROJECT_ROOT))
    result.add_argument("--date", action="append", type=_parse_date)
    result.add_argument("--start", type=_parse_date)
    result.add_argument("--end", type=_parse_date)
    result.add_argument("--output-root")
    result.add_argument("--journal-root")
    result.add_argument("--execute", action="store_true")
    result.add_argument(
        "--download-only",
        action="store_true",
        help="acquire and verify immutable bundles without importing catalogs",
    )
    result.add_argument(
        "--approve-batch-plan-sha256",
        help="exact batch_plan_sha256 from the reviewed dry run",
    )
    result.add_argument(
        "--max-estimated-cost-usd",
        "--max-cost-usd",
        dest="max_estimated_cost_usd",
        type=_finite_nonnegative,
        help="aggregate local estimate ceiling; not a provider billing control",
    )
    result.add_argument(
        "--max-estimated-billable-bytes",
        "--max-billable-bytes",
        dest="max_estimated_billable_bytes",
        type=_positive_integer,
        help="aggregate local estimate ceiling; not a provider billing control",
    )
    return result


def main(argv: list[str] | None = None) -> int:
    argument_parser = parser()
    args = argument_parser.parse_args(argv)
    try:
        dates = _requested_dates(args)
    except ValueError as exc:
        argument_parser.error(str(exc))
    if args.download_only and not args.execute:
        argument_parser.error("--download-only requires --execute")
    if args.execute and (
        not args.approve_batch_plan_sha256
        or args.max_estimated_cost_usd is None
        or args.max_estimated_billable_bytes is None
    ):
        argument_parser.error(
            "--execute requires --approve-batch-plan-sha256 plus explicit "
            "--max-estimated-cost-usd and --max-estimated-billable-bytes"
        )

    project_root = Path(args.project_root).resolve()
    from dotenv import load_dotenv

    load_dotenv(project_root / ".env")
    api_key = os.getenv("DATABENTO_API_KEY")
    if not api_key:
        argument_parser.error("DATABENTO_API_KEY is not configured")
    import databento as db

    client = db.Historical(api_key)
    try:
        plan = plan_historical_bulk(
            client,
            project_root=project_root,
            trading_dates=dates,
            output_root=args.output_root,
        )
        if not args.execute:
            payload = {"mode": "dry-run", **plan.to_dict()}
            print(json.dumps(payload, indent=2, sort_keys=True, allow_nan=False))
            return 0 if plan.ready_for_execute else 2

        payload = execute_historical_bulk(
            client,
            plan=plan,
            approved_batch_plan_sha256=str(args.approve_batch_plan_sha256),
            max_estimated_cost_usd=float(args.max_estimated_cost_usd),
            max_estimated_billable_bytes=int(args.max_estimated_billable_bytes),
            journal_root=args.journal_root,
            perform_import=not args.download_only,
        )
    except (
        FileNotFoundError,
        KeyError,
        OSError,
        RuntimeError,
        TypeError,
        ValueError,
        json.JSONDecodeError,
    ) as exc:
        print(
            json.dumps(
                {
                    "mode": "execute" if args.execute else "dry-run",
                    "status": "refused",
                    "error": f"{type(exc).__name__}: {exc}",
                },
                indent=2,
                sort_keys=True,
            ),
            file=sys.stderr,
        )
        return 2
    print(json.dumps(payload, indent=2, sort_keys=True, allow_nan=False))
    return 0 if payload.get("status") == "complete" else 2


if __name__ == "__main__":
    raise SystemExit(main())
