"""Inspect retained opening NDJSON; no provider connection, app import or writes."""
from __future__ import annotations

import argparse
from datetime import datetime
import hashlib
import json
from pathlib import Path
import sys

sys.dont_write_bytecode = True
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backend.opening_ingress_report import opening_ingress_report, read_timeline


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("timeline", type=Path)
    parser.add_argument("--epoch", required=True)
    parser.add_argument("--generation", type=int, required=True)
    parser.add_argument("--cash-open", required=True, help="Timezone-aware ISO cash-open timestamp")
    parser.add_argument("--writer-snapshot", type=Path, help="Retained opening_readiness_timeline snapshot JSON, including instrumentation status")
    args = parser.parse_args(argv)
    try:
        cash_open = datetime.fromisoformat(args.cash_open)
        if cash_open.tzinfo is None:
            raise ValueError("CASH_OPEN_TIMEZONE_REQUIRED")
        source_before = hashlib.sha256(args.timeline.read_bytes()).hexdigest()
        snapshot = json.loads(args.writer_snapshot.read_text(encoding="utf-8")) if args.writer_snapshot else None
        report = opening_ingress_report(read_timeline(args.timeline), epoch_id=args.epoch,
                                        generation=args.generation,
                                        cash_open_ns=int(cash_open.timestamp() * 1_000_000_000),
                                        writer_snapshot=snapshot)
        source_after = hashlib.sha256(args.timeline.read_bytes()).hexdigest()
        if source_before != source_after:
            raise ValueError("TIMELINE_CHANGED_DURING_READ_USE_SEALED_COPY")
        report["timeline_sha256"] = source_before
    except (OSError, ValueError) as exc:
        parser.error(str(exc))
    print(json.dumps(report, indent=2, allow_nan=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
