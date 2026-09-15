from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from backend.closing_tape.readiness import audit_training_readiness


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Audit all retained MarketPin TCBBO sessions without modifying them"
    )
    parser.add_argument("--project-root", default=str(PROJECT_ROOT))
    parser.add_argument("--eligible-only", action="store_true")
    args = parser.parse_args(argv)
    payload = audit_training_readiness(args.project_root).to_dict()
    if args.eligible_only:
        payload["sessions_detail"] = [
            row for row in payload["sessions_detail"] if row["eligible"]
        ]
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
