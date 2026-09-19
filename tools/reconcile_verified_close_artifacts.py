"""Append byte-proven alias receipts without writing any tape or close ledger."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from backend.closing_tape.close_registry import reconcile_verified_close_artifact


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--vault-root", type=Path, default=PROJECT_ROOT / "data/verified_close_sources")
    parser.add_argument("--trading-date", required=True)
    parser.add_argument("--symbol", required=True)
    parser.add_argument("--source-artifact-sha256", required=True)
    args = parser.parse_args(argv)
    try:
        receipt = reconcile_verified_close_artifact(
            args.vault_root, trading_date=args.trading_date, symbol=args.symbol,
            source_artifact_sha256=args.source_artifact_sha256,
        )
    except (OSError, ValueError) as exc:
        print(json.dumps({"status": "blocked", "reason": str(exc)}, sort_keys=True))
        return 1
    print(json.dumps({"status": "reconciled", "receipt": str(receipt)}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
