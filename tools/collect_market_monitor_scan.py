"""Build one read-only, source-aligned MarketPin monitor commit request.

The collector reads existing HTTP, audit, SQLite, state, and journal evidence.
It does not open a provider connection and does not write or commit anything.
On success stdout contains only the exact JSON object accepted by
``tools/commit_market_monitor_scan.py``.  Failures are written to stderr so a
shell pipeline cannot mistake an error envelope for a commit request.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from backend.database_target import configured_market_database_path  # noqa: E402
from backend.monitor_scan_collector import (  # noqa: E402
    MonitorScanCollectorError,
    collect_commit_request,
)


ERROR_SCHEMA = "marketpin-monitor-scan-collection-error.v1"


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Read existing MarketPin authorities and emit one exact substantive-"
            "scan commit request without changing runtime or durable state."
        )
    )
    parser.add_argument("--project-root", type=Path, default=PROJECT_ROOT)
    parser.add_argument("--backend-url", default="http://127.0.0.1:8000")
    parser.add_argument("--database-path", type=Path)
    parser.add_argument("--state-path", type=Path)
    parser.add_argument("--journal-dir", type=Path)
    parser.add_argument("--timeout-seconds", type=float, default=5.0)
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
        if args.database_path is not None:
            database_path = args.database_path.resolve()
        else:
            database_path = configured_market_database_path(root).resolve()
            canonical_database_path = (root / "data" / "market_data.db").resolve()
            if (
                canonical_database_path.is_file()
                and database_path != canonical_database_path
            ):
                raise MonitorScanCollectorError(
                    "implicit_database_target_conflict: "
                    f"selected={database_path}; canonical={canonical_database_path}; "
                    "pass --database-path explicitly"
                )
        request = collect_commit_request(
            project_root=root,
            backend_url=str(args.backend_url),
            database_path=database_path,
            state_path=state_path,
            journal_dir=journal_dir,
            sample_seconds=5.0,
            timeout_seconds=float(args.timeout_seconds),
        )
        rendered = json.dumps(
            request,
            sort_keys=True,
            indent=2 if args.pretty else None,
            separators=None if args.pretty else (",", ":"),
        )
    except (
        MonitorScanCollectorError,
        OSError,
        OverflowError,
        RuntimeError,
        TypeError,
        UnicodeError,
        ValueError,
    ) as exc:
        error = {
            "schema_version": ERROR_SCHEMA,
            "accepted": False,
            "action": "abstain",
            "issues": [f"collection_error:{type(exc).__name__}:{exc}"[:500]],
        }
        print(
            json.dumps(error, sort_keys=True, separators=(",", ":")),
            file=sys.stderr,
        )
        return 1
    print(rendered)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
