# build_full_gamma_dataset.py
import sys
import os
import json
import pandas as pd

def load_snapshot(path):
    with open(path, "r") as f:
        return json.load(f)

def extract_row(snap):
    symbol = snap.get("symbol", "")
    ts = snap.get("timestamp_utc", snap.get("generated_at_utc", None))
    spot = snap.get("spot_last", snap.get("spot", None))
    gamma_pin = snap.get("primary_gamma_pin_strike") or snap.get("gamma_pin", None)
    gross_gex = snap.get("gross_gex", None)
    net_gex = snap.get("net_gex", None)
    call_gex = snap.get("call_gex_total", snap.get("call_gex", None))
    put_gex = snap.get("put_gex_total", snap.get("put_gex", None))
    is_valid = snap.get("validation_is_valid", False)

    return {
        "symbol": symbol,
        "timestamp_utc": ts,
        "spot": spot,
        "gamma_pin": gamma_pin,
        "distance_to_pin": spot - gamma_pin if (spot is not None and gamma_pin is not None) else None,
        "gross_gex": gross_gex,
        "net_gex": net_gex,
        "call_gex": call_gex,
        "put_gex": put_gex,
        "is_valid": is_valid
    }

def build_dataset(exports_dir, date_filter=None, include_invalid=False):
    """
    Build dataset from NDJSON files in exports/{SYMBOL}/{DATE}.ndjson structure.
    
    Args:
        exports_dir: Root exports directory (e.g., ./exports)
        date_filter: Optional date string (YYYY-MM-DD) to filter specific date only
    """
    rows = []
    files_processed = 0
    
    if not os.path.exists(exports_dir):
        print(f"Error: Directory not found: {exports_dir}")
        return pd.DataFrame()
    
    for root, _, files in os.walk(exports_dir):
        for fname in files:
            if not fname.endswith(".ndjson"):
                continue

            if date_filter:
                file_date = fname.replace('.ndjson', '')
                if file_date != date_filter:
                    continue

            full_path = os.path.join(root, fname)
            symbol_guess = os.path.basename(os.path.dirname(full_path))
            file_rows = 0

            with open(full_path, "r") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        snap = json.loads(line)
                        if not snap.get("symbol"):
                            snap["symbol"] = symbol_guess

                        row = extract_row(snap)
                        if include_invalid or row.get("is_valid"):
                            rows.append(row)
                            file_rows += 1
                    except Exception as e:
                        print(f"Error parsing {fname}: {e}")

            files_processed += 1
            print(f"  Processed {fname}: {file_rows} rows")
    
    if not rows:
        print("Warning: No data found")
        return pd.DataFrame()
    
    df = pd.DataFrame(rows)
    df["timestamp_utc"] = pd.to_datetime(df["timestamp_utc"], errors="coerce", utc=True)
    df = df.dropna(subset=["timestamp_utc"])
    df = df.sort_values("timestamp_utc").reset_index(drop=True)
    
    print(f"Total: {len(rows)} rows from {files_processed} files")
    return df

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python build_full_gamma_dataset.py <exports_dir> <output_csv> [date_filter] [--include-invalid]")
        print("")
        print("Arguments:")
        print("  exports_dir   Root exports directory (e.g., ./exports)")
        print("  output_csv    Output CSV file path")
        print("  date_filter   Optional: Filter to specific date (YYYY-MM-DD)")
        print("")
        print("Examples:")
        print("  python build_full_gamma_dataset.py ./exports all_data.csv")
        print("  python build_full_gamma_dataset.py ./exports data_2025-12-22.csv 2025-12-22")
        sys.exit(1)

    exports_dir = sys.argv[1]
    output_csv = sys.argv[2]
    include_invalid = "--include-invalid" in sys.argv
    date_filter = None
    if len(sys.argv) > 3 and not sys.argv[3].startswith("--"):
        date_filter = sys.argv[3]

    print(f"Building dataset from: {exports_dir}")
    if date_filter:
        print(f"Filtering to date: {date_filter}")
    
    df = build_dataset(exports_dir, date_filter, include_invalid=include_invalid)
    
    if not df.empty:
        df.to_csv(output_csv, index=False)
        print(f"Saved dataset to {output_csv}")
    else:
        print("No data to save")
        sys.exit(1)
