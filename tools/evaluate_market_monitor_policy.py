"""Evaluate normalized MarketPin monitor evidence as JSON, without side effects."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from backend.monitor_policy import OUTPUT_SCHEMA, evaluate_monitor_policy  # noqa: E402


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Evaluate two or more normalized monitor observations."
    )
    parser.add_argument(
        "--input",
        default="-",
        help="JSON input path, or '-' (default) to read stdin.",
    )
    parser.add_argument("--pretty", action="store_true", help="Indent JSON output.")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        raw = sys.stdin.read() if args.input == "-" else Path(args.input).read_text("utf-8")
        payload = json.loads(raw)
        if not isinstance(payload, dict):
            raise ValueError("input JSON must be an object")
        result = evaluate_monitor_policy(payload)
    except (
        OSError,
        UnicodeError,
        json.JSONDecodeError,
        TypeError,
        ValueError,
        OverflowError,
    ) as exc:
        result = {
            "schema_version": OUTPUT_SCHEMA,
            "accepted": False,
            "issues": [f"input_error:{type(exc).__name__}:{exc}"],
            "events": [],
        }
        print(json.dumps(result, sort_keys=True, separators=(",", ":")))
        return 2
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
