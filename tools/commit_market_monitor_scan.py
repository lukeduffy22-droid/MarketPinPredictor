"""Durably commit one normalized MarketPin substantive scan."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from backend.monitor_scan_ledger import (  # noqa: E402
    RESULT_SCHEMA,
    commit_monitor_scan,
    with_policy_input_hashes,
    with_scan_event_id,
)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Append a stable monitor scan and deterministic helper events before "
            "atomically replacing monitor state."
        )
    )
    parser.add_argument(
        "--input",
        default="-",
        help="Request JSON path, or '-' (default) to read stdin.",
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
        raw = sys.stdin.read() if args.input == "-" else Path(args.input).read_text("utf-8")
        request = json.loads(raw)
        if not isinstance(request, dict):
            raise ValueError("request JSON must be an object")
        scan = request.get("scan")
        if not isinstance(scan, dict):
            raise ValueError("request.scan must be an object")
        cadence = scan.get("cadence")
        if not isinstance(cadence, dict) or not isinstance(
            cadence.get("adaptive_evidence"), dict
        ):
            raise ValueError(
                "request.scan.cadence.adaptive_evidence must be an object"
            )
        policy_inputs = request.get("policy_inputs")
        prepared_scan = with_scan_event_id(
            with_policy_input_hashes(scan, policy_inputs)
        )
        result = commit_monitor_scan(
            scan=prepared_scan,
            policy_inputs=policy_inputs,
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
