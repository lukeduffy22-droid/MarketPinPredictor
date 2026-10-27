"""Provision or verify private local passport access. Never prints the credential."""
import argparse
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from backend.governance_credentials import ensure_governance_token, governance_token


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--ensure', action='store_true')
    parser.add_argument('--check-live', action='store_true', help='Check authenticated API access without an AI request')
    args = parser.parse_args()
    try:
        result = ensure_governance_token() if args.ensure else {
            'status': 'CONFIGURED' if governance_token() else 'MISSING', 'runtime_loading': 'NOT_VERIFIED'}
        if args.check_live:
            import requests
            try:
                response = requests.get('http://127.0.0.1:8000/v1/workstation/passports',
                    params={'limit':1}, headers={'X-MarketPin-Governance-Token':governance_token()},
                    timeout=(2, 10), allow_redirects=False)
                result['http_status'] = response.status_code
                result['runtime_loading'] = 'AUTHENTICATED' if response.status_code == 200 else 'ACCESS_NOT_VERIFIED'
            except requests.Timeout:
                result['runtime_loading'] = 'API_TIMEOUT'
            except requests.RequestException:
                result['runtime_loading'] = 'API_UNAVAILABLE'
    except (OSError, ValueError):
        print(json.dumps({'status': 'CONFIGURATION_FAILED', 'detail': 'Check local credential conflict, length or file access.'}))
        return 1
    print(json.dumps(result))
    return 0 if result['status'] == 'CONFIGURED' and (not args.check_live or result['runtime_loading']=='AUTHENTICATED') else 1


if __name__ == '__main__':
    raise SystemExit(main())
