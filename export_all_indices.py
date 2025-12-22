import os
import json
import pandas as pd
from glob import glob
from datetime import datetime

# ============================================================
# CONFIG
# ============================================================

SNAPSHOT_FOLDER = "exports/"  # Folder containing SPX/, NDX/, RUT/ subdirectories with .ndjson files
OUTPUT_CSV = f"all_indices_{datetime.now().strftime('%Y-%m-%d')}.csv"
VALID_SYMBOLS = {"SPX", "NDX", "RUT"}

# ============================================================
# LOAD SNAPSHOTS
# ============================================================

def load_ndjson_files(folder):
    rows = []
    
    # Look in subdirectories (exports/SPX/, exports/NDX/, etc.)
    for symbol_dir in os.listdir(folder):
        symbol_path = os.path.join(folder, symbol_dir)
        if os.path.isdir(symbol_path):
            for file in glob(f"{symbol_path}/*.ndjson"):
                with open(file, "r") as f:
                    for line in f:
                        line = line.strip()
                        if line:
                            try:
                                data = json.loads(line)
                                data["symbol"] = symbol_dir  # Add symbol from folder name
                                rows.append(data)
                            except:
                                pass
    
    return pd.DataFrame(rows)

# ============================================================
# EXTRACT FIELDS
# ============================================================

def extract_fields(df):
    if df.empty:
        return pd.DataFrame()
    
    # Filter to valid symbols
    df = df[df["symbol"].isin(VALID_SYMBOLS)]
    
    # Filter for valid entries only
    if "validation_is_valid" in df.columns:
        df = df[df["validation_is_valid"] == True]
    elif "is_valid" in df.columns:
        df = df[df["is_valid"] == True]

    out = pd.DataFrame()
    out["symbol"] = df["symbol"]
    out["date"] = pd.to_datetime(df.get("generated_at_utc", df.get("timestamp_utc", ""))).dt.date
    out["timestamp_utc"] = pd.to_datetime(df.get("generated_at_utc", df.get("timestamp_utc", "")))
    out["spot"] = df.get("spot_last", df.get("spot", 0))
    out["gamma_pin"] = df.get("primary_gamma_pin_strike", df.get("gamma_pin_strike", None))
    out["gross_gex"] = df.get("gross_gex", 0)
    out["net_gex"] = df.get("net_gex", 0)
    out["call_gex"] = df.get("call_gex", 0)
    out["put_gex"] = df.get("put_gex", 0)
    out["max_pain"] = df.get("max_pain_strike", df.get("max_pain", None))
    out["is_valid"] = True

    return out.sort_values(["symbol", "timestamp_utc"])

# ============================================================
# MAIN EXPORT FUNCTION
# ============================================================

def export_all_indices(output_file=None):
    if output_file is None:
        output_file = OUTPUT_CSV
        
    print(f"Loading snapshot files from {SNAPSHOT_FOLDER}...")
    df = load_ndjson_files(SNAPSHOT_FOLDER)
    
    if df.empty:
        print("No snapshot data found!")
        return

    print(f"Found {len(df)} total snapshots")
    print("Extracting fields...")
    clean = extract_fields(df)

    if clean.empty:
        print("No valid snapshots to export!")
        return

    print(f"Saving {len(clean)} rows to {output_file}...")
    clean.to_csv(output_file, index=False)
    print("Export complete.")
    
    # Summary by symbol
    print("\nSummary:")
    for sym in VALID_SYMBOLS:
        count = len(clean[clean["symbol"] == sym])
        print(f"  {sym}: {count} snapshots")

# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1:
        export_all_indices(sys.argv[1])
    else:
        export_all_indices()
