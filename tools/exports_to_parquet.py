"""
Convert app audit NDJSON files under `exports/` into a single Parquet dataset
and a small CSV summary. The script is defensive about schema differences and will
preserve the raw JSON for later inspection.

Usage:
  python tools/exports_to_parquet.py --exports-dir exports --out exports/collected/parquet

Outputs:
- `combined_snapshots.parquet` (full dataset)
- `snapshot_summary.csv` (counts by symbol and validity)
"""
import argparse
import json
import os
import glob
from datetime import datetime

import pandas as pd


def safe_get(d, *keys):
    for k in keys:
        if isinstance(d, dict) and k in d:
            return d[k]
    return None


def normalize_snapshot(obj, source_path):
    # obj: dict parsed from NDJSON line or snapshot
    out = {}
    out['source_file'] = source_path
    out['fetched_at'] = obj.get('generated_at_utc') or obj.get('timestamp_utc') or obj.get('snapshot_time') or obj.get('fetched_at_utc')
    out['symbol'] = obj.get('symbol') or safe_get(obj, 'underlying') or os.path.basename(os.path.dirname(source_path))

    # Common gamma fields (try several possible keys)
    out['pin_strike'] = safe_get(obj, 'primary_gamma_pin_strike', 'pin_strike', 'aggregate_pin')
    out['total_gex_abs'] = safe_get(obj, 'total_gex_abs', 'total_gex', 'total_gex_abs')
    out['total_gex_net'] = safe_get(obj, 'total_gex_net', 'net_gex', 'total_gex_net')
    out['is_mock_data'] = safe_get(obj, 'is_mock_data')
    out['data_unavailable'] = safe_get(obj, 'data_unavailable')
    out['is_valid'] = safe_get(obj, 'is_valid') or (not safe_get(obj, 'validation_failure_reasons'))

    # store a few supporting fields
    out['spot_price'] = safe_get(obj, 'spot_last', 'spot_price', 'spot')
    out['max_dte'] = safe_get(obj, 'max_dte')
    out['raw'] = json.dumps(obj)
    return out


def find_ndjson_files(exports_dir):
    # Look for any .ndjson files under exports dir and also any .json files in exports/* directories
    patterns = [os.path.join(exports_dir, '**', '*.ndjson'), os.path.join(exports_dir, '**', '*.json')]
    files = []
    for p in patterns:
        files.extend(glob.glob(p, recursive=True))
    # filter out files under exports/collected to avoid reprocessing
    files = [f for f in files if 'exports/collected' not in f]
    return sorted(files)


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--exports-dir', default='exports', help='Path to exports directory')
    p.add_argument('--out', default='exports/collected/parquet', help='Output directory')
    args = p.parse_args()

    files = find_ndjson_files(args.exports_dir)
    if not files:
        print('No NDJSON/JSON files found under', args.exports_dir)
        return

    records = []
    for fp in files:
        try:
            with open(fp, 'r') as fh:
                for line in fh:
                    line=line.strip()
                    if not line:
                        continue
                    try:
                        obj = json.loads(line)
                    except Exception:
                        # maybe the file itself is a JSON object (not NDJSON)
                        try:
                            fh.seek(0)
                            obj = json.load(fh)
                            # make sure we don't loop forever
                            records.append(normalize_snapshot(obj, fp))
                            break
                        except Exception:
                            continue
                    # If the object contains an inner 'snapshot' or 'data' field, prefer it
                    if isinstance(obj, dict) and 'snapshot' in obj and isinstance(obj['snapshot'], dict):
                        obj2 = obj['snapshot']
                        obj2.update({k:v for k,v in obj.items() if k!='snapshot'})
                        obj = obj2
                    records.append(normalize_snapshot(obj, fp))
        except Exception as e:
            print('Skipping', fp, 'error', e)

    if not records:
        print('No records parsed')
        return

    df = pd.DataFrame.from_records(records)
    os.makedirs(args.out, exist_ok=True)
    out_parquet = os.path.join(args.out, 'combined_snapshots.parquet')
    out_csv = os.path.join(args.out, 'snapshot_summary.csv')

    # convert fetched_at to datetime if present
    if 'fetched_at' in df.columns:
        try:
            df['fetched_at'] = pd.to_datetime(df['fetched_at'], errors='coerce')
        except Exception:
            pass

    df.to_parquet(out_parquet, index=False)

    # produce summary counts
    summary = df.groupby(['symbol', 'is_valid']).size().unstack(fill_value=0)
    summary.to_csv(out_csv)

    print('Wrote', out_parquet)
    print('Wrote', out_csv)
    print('Total snapshots:', len(df))


if __name__ == '__main__':
    main()
