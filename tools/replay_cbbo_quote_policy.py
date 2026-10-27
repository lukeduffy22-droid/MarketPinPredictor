"""Compare timing filters on retained input copies; never writes market history."""
from __future__ import annotations

import argparse
from collections import defaultdict
from datetime import datetime, time
import hashlib
import json
import math
from pathlib import Path
import re
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from backend.quote_timing import EASTERN, OPTIONAL_LAST_SALE, SAME_DAY_LAST_SALE, cbbo_timestamps_valid


def compare(path: Path) -> dict:
    raw = path.read_bytes()
    payload = json.loads(raw)
    observed = datetime.fromisoformat(payload['calculated_at_utc'])
    params = payload['parameters']
    observed_ns = int(observed.timestamp() * 1e9)
    # Retained timestamps without offsets are UTC, never the workstation zone.
    if observed.tzinfo is None:
        from datetime import timezone
        observed = observed.replace(tzinfo=timezone.utc)
        observed_ns = int(observed.timestamp() * 1e9)
    observed_day = observed.astimezone(EASTERN).date()
    freshness = params['quote_freshness_seconds']
    schema = params['schema']
    primary = params['subscription_metadata']['primary_pair_admission'][payload['symbol']]['primary_expiration']
    result = {'source_copy': str(path), 'source_copy_sha256': hashlib.sha256(raw).hexdigest(),
              'calculation_id': payload['calculation_id'], 'symbol': payload['symbol'],
              'observed_calculation_at_utc': observed.isoformat(),
              'classification': 'OFFLINE FILTER COMPARISON; not an opening-bucket replay or live acceptance',
              'freshness_seconds_unchanged': freshness, 'policies': {}}
    for policy in (SAME_DAY_LAST_SALE, OPTIONAL_LAST_SALE):
        legs = defaultdict(lambda: {'C': [], 'P': []})
        accepted = 0
        for row in payload['raw_fresh_chain_rows']:
            recv = row.get('ts_recv_ns')
            if not cbbo_timestamps_valid(schema, row.get('ts_event_ns'), recv, row.get('ts_index_ns'), policy=policy):
                continue
            at = datetime.fromtimestamp(recv / 1e9, EASTERN)
            if (at.date() != observed_day or not time(9, 30) <= at.time() < time(16)
                    or not -.05 <= (observed_ns - recv) / 1e9 <= freshness
                    or not 0 <= row['quote_age_seconds'] <= freshness
                    or row['subscription_generation'] != payload['subscription_generation']
                    or re.fullmatch('[a-f0-9]{64}', row.get('mapping_version', '')) is None):
                continue
            bid, ask, mid = row['bid'], row['ask'], row['mid']
            if not all(math.isfinite(v) for v in (bid, ask, mid)) or bid < 0 or ask < bid or mid <= 0:
                continue
            side = row['option_type']
            if side not in ('C', 'P'):
                continue
            key = (row['symbol'].split()[0], row['expiration_date'], row['strike'])
            legs[key][side].append(row['symbol'])
            accepted += 1
        pairs = [key for key, sides in legs.items() if len(sides['C']) == len(sides['P']) == 1]
        result['policies'][policy] = {'eligible_rows_in_retained_copy': accepted,
                                    'complete_pairs': len(pairs),
                                    'primary_complete_pairs': sum(key[1] == primary for key in pairs)}
    return result


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--input-directory', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    results = [compare(path) for path in sorted(args.input_directory.glob('*-retained-input.json'))]
    args.output.write_text(json.dumps(results, indent=2), encoding='utf-8')
    for row in results:
        print(json.dumps({key: row[key] for key in ('symbol', 'observed_calculation_at_utc', 'policies')}))
