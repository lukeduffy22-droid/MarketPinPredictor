"""
Add data-quality flags and reason codes to the combined snapshots parquet.

Outputs:
- `exports/collected/parquet/combined_snapshots_qc.parquet`
- `exports/collected/parquet/snapshot_summary_qc.csv`

Rules (heuristic):
- If `data_unavailable` True -> reason `data_unavailable`
- If `is_mock_data` True -> reason `mock_data`
- If `validation_failure_reasons` present -> reason `validation:<first_reason>`
- If `pin_strike` missing/null -> reason `missing_pin`
- If `gamma_walls` missing in raw -> reason `missing_gamma_walls`
- If `total_gex_abs` is 0 or missing -> reason `zero_gex`
"""
import os
import json
import pandas as pd


def detect_reason(raw_json_str, row):
    try:
        obj = json.loads(raw_json_str) if raw_json_str else {}
    except Exception:
        return 'raw_parse_error'

    # 1) explicit flags
    if obj.get('data_unavailable'):
        return 'data_unavailable'
    if obj.get('is_mock_data'):
        return 'mock_data'

    # 2) validation failures
    v = obj.get('validation_failure_reasons') or obj.get('validation_reasons')
    if v:
        if isinstance(v, list) and v:
            return 'validation:' + str(v[0])
        return 'validation'

    # 3) missing pin
    pin = row.get('pin_strike') if 'pin_strike' in row else obj.get('primary_gamma_pin_strike')
    if pin is None:
        return 'missing_pin'

    # 4) missing gamma walls
    if 'gamma_walls' not in obj and 'gamma_walls' not in row:
        return 'missing_gamma_walls'

    # 5) zero gex
    try:
        gex = row.get('total_gex_abs')
        if gex is None:
            gex = obj.get('total_gex_abs') or obj.get('total_gex')
        if gex is None:
            return 'missing_gex'
        try:
            if float(gex) == 0.0:
                return 'zero_gex'
        except Exception:
            pass
    except Exception:
        pass

    return ''


def main():
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument('--inp', default=os.environ.get('QC_INPUT', 'exports/collected/parquet/combined_snapshots.parquet'), help='Input combined parquet path')
    p.add_argument('--out-dir', default=os.environ.get('QC_OUT_DIR', 'exports/collected/parquet'), help='Output directory for QC artifacts')
    args = p.parse_args()

    inp = args.inp
    out_dir = args.out_dir
    print(f"add_quality_flags: using inp={inp} out_dir={out_dir}")

    os.makedirs(out_dir, exist_ok=True)
    df = pd.read_parquet(inp)

    reasons = []
    for _, row in df.iterrows():
        raw = row.get('raw') if 'raw' in row else None
        reason = detect_reason(raw, row)
        reasons.append(reason)

    df['invalid_reason'] = reasons
    df['is_valid_qc'] = df['invalid_reason'].apply(lambda x: False if x else True)

    out_parquet = os.path.join(out_dir, 'combined_snapshots_qc.parquet')
    out_csv = os.path.join(out_dir, 'snapshot_summary_qc.csv')
    df.to_parquet(out_parquet, index=False)

    summary = df.groupby(['symbol', 'is_valid_qc']).size().unstack(fill_value=0)
    summary.to_csv(out_csv)
    # also write reason counts
    reason_counts = df['invalid_reason'].value_counts()
    reason_counts.to_csv(os.path.join(out_dir, 'reason_counts.csv'))

    print('Wrote', out_parquet)
    print('Wrote', out_csv)
    print('Reason counts:')
    print(reason_counts.head(50))


if __name__ == '__main__':
    main()
