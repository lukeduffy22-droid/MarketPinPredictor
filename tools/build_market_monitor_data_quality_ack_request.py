"""Build, but never execute, a data-quality notification ACK request.

The input contains a caller-supplied normalized completed-final record.  On
success stdout is exactly the existing four-field ACK request as one compact
JSON document.  This tool does not read or write monitor state, journals, or
conversation history and does not invoke the acknowledgement owner.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from backend.monitor_completed_final_delivery import (  # noqa: E402
    COMPLETED_FINAL_SCHEMA,
    MonitorCompletedFinalDeliveryError,
    build_data_quality_ack_request,
)


ERROR_SCHEMA = "marketpin-monitor-data-quality-ack-request-build.error.v1"
_INPUT_FIELDS = {
    "session_date",
    "event_ids",
    "observed_at_utc",
    "completed_final",
}


class _JsonArgumentParser(argparse.ArgumentParser):
    def error(self, message: str) -> None:
        raise ValueError("argument_error:" + message)


def _parser() -> argparse.ArgumentParser:
    parser = _JsonArgumentParser(
        add_help=False,
        description="Build a validated prior-completed-final ACK request.",
    )
    parser.add_argument("--input", default="-", help="Input JSON path or '-'.")
    return parser


def _error(exc: BaseException) -> dict[str, Any]:
    return {
        "schema_version": ERROR_SCHEMA,
        "accepted": False,
        "issues": [f"input_error:{type(exc).__name__}:{exc}"],
        "expected_completed_final_schema": COMPLETED_FINAL_SCHEMA,
    }


def _emit(payload: object) -> None:
    sys.stdout.write(
        json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n"
    )


def main(argv: list[str] | None = None) -> int:
    try:
        args = _parser().parse_args(argv)
        raw = (
            sys.stdin.buffer.read().decode("utf-8", errors="strict")
            if args.input == "-"
            else Path(args.input).read_text(encoding="utf-8")
        )
        request = json.loads(raw)
        if not isinstance(request, dict) or set(request) != _INPUT_FIELDS:
            raise ValueError(
                "request fields must be exactly: "
                + ",".join(sorted(_INPUT_FIELDS))
            )
        result = build_data_quality_ack_request(
            session_date=request["session_date"],
            event_ids=request["event_ids"],
            observed_at_utc=request["observed_at_utc"],
            completed_final=request["completed_final"],
        )
    except (
        MonitorCompletedFinalDeliveryError,
        OSError,
        UnicodeError,
        json.JSONDecodeError,
        TypeError,
        ValueError,
        OverflowError,
    ) as exc:
        _emit(_error(exc))
        return 1
    _emit(result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
