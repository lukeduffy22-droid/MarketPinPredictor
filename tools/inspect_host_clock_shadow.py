"""One local clock observation; no app imports, provider session or repair.

Run as ``python -m tools.inspect_host_clock_shadow --output <new-file.json>``.
Optional --provider-evidence reads a retained processing_clock_telemetry object.
It is labeled retained evidence, never proof of current provider readiness.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path

from backend.host_clock_shadow import HostClockShadow


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--provider-evidence", type=Path)
    args = parser.parse_args()
    provider = json.loads(args.provider_evidence.read_text(encoding="utf-8")) if args.provider_evidence else {}
    if not isinstance(provider, dict):
        parser.error("provider evidence must be a JSON object")
    observer = HostClockShadow()
    observer.sample_once()
    receipt = {
        "observed_at_utc": datetime.now(timezone.utc).isoformat(),
        "classification": "OBSERVED_HOST_ONLY",
        "provider_evidence_kind": "retained_not_current" if args.provider_evidence else "not_collected",
        "source_root": str(Path(__file__).resolve().parents[1]),
        "evidence": observer.snapshot(provider),
    }
    # Refuse replacement of any existing receipt.
    with args.output.open("x", encoding="utf-8") as handle:
        json.dump(receipt, handle, indent=2, allow_nan=False)
    print(json.dumps({"receipt": str(args.output), "host_status": receipt["evidence"]["host"]["status"],
                      "authority": receipt["evidence"]["authority"]}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
