"""Run one due backend review, or collect evidence without an AI request."""
import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--evidence-only', action='store_true')
    args = parser.parse_args()
    from dotenv import load_dotenv
    load_dotenv(ROOT / '.env', override=False)
    from backend.diagnostic_automation import run_once
    result = run_once(ROOT, evidence_only=args.evidence_only)
    print(json.dumps(result, indent=2))
    return 0 if result['status'] in ('COMPLETE', 'EVIDENCE_SAVED', 'NOT_DUE') else 1


if __name__ == '__main__':
    raise SystemExit(main())
