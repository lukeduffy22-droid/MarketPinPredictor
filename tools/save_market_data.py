"""
Simple market-data collector for this repo.

Usage:
  python tools/save_market_data.py --endpoints health /gamma/multi-expiry /orb --interval 0

Features:
- Fetches endpoints from the running local server (configurable base URL via env)
- Saves raw JSON under `data/raw/{endpoint}/{YYYY-MM-DD}/{timestamp}.json`
- Flattens JSON and writes Parquet under `data/parquet/{endpoint}/{YYYY-MM-DD}/{timestamp}.parquet`

Notes:
- Install dependencies: `pip install requests pandas pyarrow`
- For production-scale collection, adapt to stream sockets or use a scheduler (cron, systemd, Airflow)
"""
import argparse
import os
import time
import json
from datetime import datetime, timezone
from urllib.parse import urljoin

import requests
import pandas as pd


def now_ts():
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def safe_mkdir(path):
    os.makedirs(path, exist_ok=True)


def write_json(path, data):
    with open(path, "w") as f:
        json.dump(data, f, default=str)


def write_parquet(path, data):
    # Try to normalize JSON into a flat table; if it fails, wrap into one-row table
    try:
        if isinstance(data, list):
            df = pd.json_normalize(data)
        elif isinstance(data, dict):
            df = pd.json_normalize(data)
        else:
            df = pd.DataFrame([{"value": data}])
    except Exception:
        df = pd.DataFrame([{"raw": json.dumps(data)}])

    df.to_parquet(path, index=False)


def fetch_and_store(base_url, endpoint, out_dir):
    url = urljoin(base_url, endpoint.lstrip("/"))
    ts = now_ts()
    date = ts[:8]
    raw_dir = os.path.join(out_dir, "raw", endpoint.strip("/"), date)
    pq_dir = os.path.join(out_dir, "parquet", endpoint.strip("/"), date)
    safe_mkdir(raw_dir)
    safe_mkdir(pq_dir)

    try:
        r = requests.get(url, timeout=10)
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        print(f"  ⚠️ Error fetching {url}: {e}")
        return

    raw_path = os.path.join(raw_dir, f"{ts}.json")
    pq_path = os.path.join(pq_dir, f"{ts}.parquet")

    write_json(raw_path, {"fetched_at_utc": ts, "endpoint": endpoint, "data": data})
    try:
        write_parquet(pq_path, data)
    except Exception as e:
        print(f"  ⚠️ Error writing parquet for {endpoint}: {e}")

    print(f"Saved {endpoint} -> {raw_path} , {pq_path}")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--base-url", default=os.environ.get("BASE_URL", "http://127.0.0.1:8000/"), help="Server base URL")
    p.add_argument("--endpoints", nargs="+", default=["/health"], help="List of endpoints to fetch (paths)")
    p.add_argument("--out-dir", default="data", help="Output base directory")
    p.add_argument("--interval", type=float, default=0, help="Seconds between polls (0 = run once)")
    args = p.parse_args()

    print("Collector starting", args)

    try:
        while True:
            for ep in args.endpoints:
                fetch_and_store(args.base_url, ep, args.out_dir)
            if args.interval <= 0:
                break
            time.sleep(args.interval)
    except KeyboardInterrupt:
        print("Interrupted")


if __name__ == "__main__":
    main()
