"""One explicit, paid end-to-end diagnostic review; no service restart or job reset."""
from datetime import datetime, timezone
import argparse
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--ask-ai', action='store_true', help='Authorize one real model request')
    parser.add_argument('--session-date')
    args = parser.parse_args()
    if not args.ask_ai:
        parser.error('--ask-ai is required for the paid verification request')
    from dotenv import load_dotenv
    load_dotenv(ROOT / '.env', override=False)
    from app.services.advisor_workflow import inspect_capabilities
    from app.services.diagnostic_agent import diagnose
    from app.services.diagnostic_history import CT, save_review, load_reviews
    from app.services.diagnostic_receipts import DiagnosticError
    from app.services.diagnostic_runtime import collect_live_packet
    day = args.session_date or datetime.now(timezone.utc).astimezone(CT).date().isoformat()
    packet = collect_live_packet(['SPX', 'NDX', 'RUT', 'VIX'], inspect_capabilities(ROOT), ROOT, day)
    print(json.dumps({'stage': 'COLLECTED', 'created_at_utc': packet['created_at_utc'],
                      'packet_bytes': len(json.dumps(packet).encode()),
                      'endpoint_errors': len(packet['evidence']['collection_errors'])}), flush=True)
    try:
        review = diagnose('Review the running application and selected capture session. Lead with working, failing, changed, and unverified findings. Cite the observations and distinguish missing opening proof and elapsed closing-window coverage from actual capture completeness.', packet, root=ROOT)
    except DiagnosticError as exc:
        print(json.dumps(exc.receipt, indent=2))
        return 1
    path = save_review(review, ROOT)
    verified = any(r == review for r in load_reviews(ROOT, day))
    print(json.dumps({'outcome': 'SAVED_AND_READ_BACK' if verified else 'READ_BACK_FAILED',
                      'actual_model': review['model'], 'review_path': str(path),
                      'receipt': review['request_receipt'], 'review': review['review']}, indent=2))
    return 0 if verified else 1


if __name__ == '__main__':
    raise SystemExit(main())
