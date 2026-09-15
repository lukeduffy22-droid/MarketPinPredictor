from __future__ import annotations

import argparse
import json
import math
import os
import sys
from dataclasses import replace
from datetime import date
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from backend.closing_tape.historical_backfill import (
    DEFAULT_OUTPUT_DIRECTORY,
    SUPPORTED_SCHEMAS,
    discover_existing_bundle,
    download_historical_day,
    eligible_capture_dates,
    inclusive_dates,
    plan_historical_day,
)


def _parse_date(raw: str) -> date:
    try:
        return date.fromisoformat(raw)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid ISO date: {raw}") from exc


def _requested_dates(args: argparse.Namespace) -> tuple[date, ...]:
    if args.date:
        return tuple(dict.fromkeys(args.date))
    if args.start is None or args.end is None:
        raise ValueError("provide --date or both --start and --end")
    return inclusive_dates(args.start, args.end)


def _finite_nonnegative(raw: str) -> float:
    try:
        value = float(raw)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be a finite nonnegative number") from exc
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


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Plan or download hash-bound Databento OPRA Historical bundles without "
            "modifying live MarketPin captures"
        )
    )
    parser.add_argument("--project-root", default=str(PROJECT_ROOT))
    parser.add_argument("--date", action="append", type=_parse_date)
    parser.add_argument("--start", type=_parse_date)
    parser.add_argument("--end", type=_parse_date)
    parser.add_argument(
        "--schema",
        action="append",
        choices=SUPPORTED_SCHEMAS,
        help="defaults to the complete TCBBO/statistics/definition bundle",
    )
    parser.add_argument("--include-eligible", action="store_true")
    parser.add_argument("--execute", action="store_true")
    parser.add_argument(
        "--approve-plan-sha256",
        action="append",
        help="exact plan_sha256 from the reviewed dry run; repeat for multiple dates",
    )
    parser.add_argument(
        "--max-estimated-cost-usd",
        "--max-cost-usd",
        dest="max_estimated_cost_usd",
        type=_finite_nonnegative,
        help="local preflight estimate ceiling; not a provider billing control",
    )
    parser.add_argument(
        "--max-estimated-billable-bytes",
        "--max-billable-bytes",
        dest="max_estimated_billable_bytes",
        type=_positive_integer,
        help="local preflight estimate ceiling; not a provider billing control",
    )
    parser.add_argument("--output-root")
    args = parser.parse_args(argv)

    try:
        requested_dates = _requested_dates(args)
    except ValueError as exc:
        parser.error(str(exc))
    schemas = tuple(args.schema or SUPPORTED_SCHEMAS)
    if set(schemas) != set(SUPPORTED_SCHEMAS):
        parser.error(
            "--schema must include tcbbo, statistics, and definition; partial bundles "
            "are not complete acquisition evidence"
        )
    if args.execute and (
        args.max_estimated_cost_usd is None
        or args.max_estimated_billable_bytes is None
        or not args.approve_plan_sha256
    ):
        parser.error(
            "--execute requires --approve-plan-sha256 plus explicit "
            "--max-estimated-cost-usd and --max-estimated-billable-bytes"
        )

    project_root = Path(args.project_root).resolve()
    output_root = (
        Path(args.output_root).resolve()
        if args.output_root
        else (project_root / DEFAULT_OUTPUT_DIRECTORY).resolve()
    )

    from dotenv import load_dotenv

    load_dotenv(project_root / ".env")
    api_key = os.getenv("DATABENTO_API_KEY")
    if not api_key:
        parser.error("DATABENTO_API_KEY is not configured")
    import databento as db

    client = db.Historical(api_key)
    eligible = set() if args.include_eligible else eligible_capture_dates(project_root)
    dataset_range = client.metadata.get_dataset_range(dataset="OPRA.PILLAR")
    plans = []
    for trading_day in requested_dates:
        plan = plan_historical_day(
            client,
            trading_day=trading_day,
            schemas=schemas,
            already_eligible=trading_day.isoformat() in eligible,
            dataset_range=dataset_range,
        )
        try:
            existing = discover_existing_bundle(
                output_root, trading_day, expected_plan=plan
            )
        except (FileNotFoundError, KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
            plan = replace(
                plan,
                blocked_reasons=tuple(
                    dict.fromkeys(
                        (*plan.blocked_reasons, f"existing bundle conflict: {exc}")
                    )
                ),
            )
        else:
            if existing:
                plan = replace(plan, existing_bundle=existing)
        plans.append(plan)

    payload: dict[str, object] = {
        "mode": "execute" if args.execute else "dry-run",
        "project_root": str(project_root),
        "output_root": str(output_root),
        "plans": [plan.to_dict() for plan in plans],
        "estimated_cost_usd": sum(plan.estimated_cost_usd for plan in plans if plan.executable),
        "estimated_billable_bytes": sum(
            plan.estimated_billable_bytes for plan in plans if plan.executable
        ),
        "estimate_enforcement_scope": "client_preflight_only",
        "billing_notice": (
            "Estimate ceilings are checked locally before get_range; they are not "
            "provider-enforced spending limits."
        ),
        "downloaded": [],
    }
    if not args.execute:
        print(json.dumps(payload, indent=2, sort_keys=True))
        return 2 if any(plan.blocked_reasons for plan in plans) else 0

    executable = [plan for plan in plans if plan.executable]
    approved_plan_hashes = set(args.approve_plan_sha256 or ())
    missing_approvals = [
        plan.plan_sha256
        for plan in executable
        if plan.plan_sha256 not in approved_plan_hashes
    ]
    if missing_approvals:
        parser.error(
            "missing exact --approve-plan-sha256 value(s): "
            + ", ".join(missing_approvals)
        )
    total_cost = sum(plan.estimated_cost_usd for plan in executable)
    total_bytes = sum(plan.estimated_billable_bytes for plan in executable)
    if total_cost > float(args.max_estimated_cost_usd) + 1e-9:
        parser.error(
            f"total estimated cost ${total_cost:.6f} exceeds local preflight ceiling "
            f"${args.max_estimated_cost_usd:.6f}"
        )
    if total_bytes > int(args.max_estimated_billable_bytes):
        parser.error(
            f"total estimated billable bytes {total_bytes} exceed local preflight ceiling "
            f"{args.max_estimated_billable_bytes}"
        )
    if blocked := [plan for plan in plans if plan.blocked_reasons]:
        payload["blocked_dates"] = [plan.trading_date for plan in blocked]
        print(json.dumps(payload, indent=2, sort_keys=True))
        return 2

    downloaded: list[dict[str, object]] = []
    for plan in executable:
        print(
            f"Downloading {plan.trading_date}: {plan.estimated_billable_bytes} estimated bytes",
            file=sys.stderr,
            flush=True,
        )
        manifest = download_historical_day(
            client,
            plan=plan,
            output_root=output_root,
            max_cost_usd=float(args.max_estimated_cost_usd),
            max_billable_bytes=int(args.max_estimated_billable_bytes),
            approved_plan_sha256=plan.plan_sha256,
        )
        manifest_path = discover_existing_bundle(
            output_root, date.fromisoformat(plan.trading_date), expected_plan=plan
        )
        downloaded.append(
            {
                "trading_date": plan.trading_date,
                "manifest": manifest_path,
                "bundle_sha256": manifest["bundle_sha256"],
                "component_bytes": sum(
                    int(item["file_bytes"]) for item in manifest["components"]
                ),
            }
        )
    payload["downloaded"] = downloaded
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
