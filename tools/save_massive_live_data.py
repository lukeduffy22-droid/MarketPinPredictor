#!/usr/bin/env python3
"""
Collect live market data directly from Massive/Polygon API.

Fetches real-time index data and options data for top index funds:
- SPX, NDX, RUT, VIX, DJI (indices)
- Options trades and quotes

Saves to:
- Raw JSON: exports/massive/indices/{YYYY-MM-DD}/{timestamp}.json
- Parquet: exports/massive/indices/{YYYY-MM-DD}/{timestamp}.parquet

Usage:
    python tools/save_massive_live_data.py --api-key YOUR_API_KEY --out-dir exports --interval 60
    
Environment:
    MASSIVE_API: Your Polygon/Massive API key (if --api-key not provided)
"""
import argparse
import os
import time
import json
from datetime import datetime, timezone
from typing import Dict, List, Any
import logging

try:
    from polygon.rest import RESTClient
    import pandas as pd
except ImportError:
    print("ERROR: Please install polygon-api-client and pandas")
    print("  pip install polygon-api-client pandas pyarrow")
    exit(1)

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
log = logging.getLogger("massive_collector")

# Top index funds to collect
INDEX_SYMBOLS = ["SPX", "NDX", "RUT", "VIX", "DJI"]

# Options roots to collect
OPTIONS_ROOTS = ["SPX", "NDX", "RUT"]


def now_ts():
    """Get current UTC timestamp in ISO format."""
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def safe_mkdir(path):
    """Create directory recursively."""
    os.makedirs(path, exist_ok=True)


def write_json(path, data):
    """Write data to JSON file."""
    with open(path, "w") as f:
        json.dump(data, f, default=str)


def write_parquet(path, data):
    """Write data to Parquet file, normalizing JSON to flat table."""
    try:
        if isinstance(data, list):
            df = pd.json_normalize(data)
        elif isinstance(data, dict):
            # For single dict, try to expand it to multiple rows if it has list fields
            if any(isinstance(v, list) for v in data.values()):
                df = pd.json_normalize(data)
            else:
                df = pd.DataFrame([data])
        else:
            df = pd.DataFrame([{"value": str(data)}])
    except Exception as e:
        log.warning(f"Failed to normalize, saving as raw: {e}")
        df = pd.DataFrame([{"raw": json.dumps(data)}])

    df.to_parquet(path, index=False)


def fetch_index_data(client: RESTClient, symbol: str, out_dir: str) -> bool:
    """Fetch latest index data from Massive API."""
    ts = now_ts()
    date = ts[:8]
    
    try:
        log.info(f"Fetching index: {symbol}")
        response = client.get_snapshot_index(ticker=symbol)
        
        if not response:
            log.warning(f"No data for {symbol}")
            return False
        
        # Save raw JSON
        raw_dir = os.path.join(out_dir, "massive", "indices", date)
        safe_mkdir(raw_dir)
        raw_path = os.path.join(raw_dir, f"{symbol}_{ts}.json")
        
        data = {
            "fetched_at_utc": ts,
            "symbol": symbol,
            "type": "index_snapshot",
            "data": response.model_dump() if hasattr(response, 'model_dump') else dict(response)
        }
        
        write_json(raw_path, data)
        
        # Save parquet
        pq_dir = os.path.join(out_dir, "massive", "indices", date)
        safe_mkdir(pq_dir)
        pq_path = os.path.join(pq_dir, f"{symbol}_{ts}.parquet")
        
        write_parquet(pq_path, data["data"])
        log.info(f"Saved {symbol} -> {raw_path}")
        return True
        
    except Exception as e:
        log.error(f"Failed to fetch {symbol}: {e}")
        return False


def fetch_options_data(client: RESTClient, root: str, out_dir: str) -> bool:
    """Fetch latest options chain data from Massive API."""
    ts = now_ts()
    date = ts[:8]
    
    try:
        log.info(f"Fetching options for: {root}")
        
        # Get the latest options chain snapshot
        response = client.get_snapshot_option_chain(
            underlying_ticker=root,
            order="desc",
            limit=100  # Get top 100 active contracts by volume
        )
        
        if not response or not response.results:
            log.warning(f"No options data for {root}")
            return False
        
        # Save raw JSON
        raw_dir = os.path.join(out_dir, "massive", "options", date)
        safe_mkdir(raw_dir)
        raw_path = os.path.join(raw_dir, f"{root}_chain_{ts}.json")
        
        data = {
            "fetched_at_utc": ts,
            "underlying": root,
            "type": "options_chain_snapshot",
            "data": [
                r.model_dump() if hasattr(r, 'model_dump') else dict(r)
                for r in response.results
            ]
        }
        
        write_json(raw_path, data)
        
        # Save parquet
        pq_dir = os.path.join(out_dir, "massive", "options", date)
        safe_mkdir(pq_dir)
        pq_path = os.path.join(pq_dir, f"{root}_chain_{ts}.parquet")
        
        write_parquet(pq_path, data["data"])
        log.info(f"Saved {root} options -> {raw_path}")
        return True
        
    except Exception as e:
        log.error(f"Failed to fetch options for {root}: {e}")
        return False


def collect_all_live_data(client: RESTClient, out_dir: str) -> int:
    """Collect all live data (indices + options) and return count of successes."""
    success_count = 0
    
    # Collect index data
    for symbol in INDEX_SYMBOLS:
        if fetch_index_data(client, symbol, out_dir):
            success_count += 1
    
    # Collect options data
    for root in OPTIONS_ROOTS:
        if fetch_options_data(client, root, out_dir):
            success_count += 1
    
    return success_count


def main():
    parser = argparse.ArgumentParser(
        description="Collect live market data from Massive/Polygon API"
    )
    parser.add_argument(
        "--api-key",
        default=os.environ.get("Massive_API", ""),
        help="Massive/Polygon API key (default: MASSIVE_API env var)"
    )
    parser.add_argument(
        "--out-dir",
        default="exports",
        help="Output directory for collected data"
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=60,
        help="Seconds between collections (default: 60)"
    )
    parser.add_argument(
        "--indices",
        nargs="+",
        default=INDEX_SYMBOLS,
        help="Index symbols to collect (default: SPX NDX RUT VIX DJI)"
    )
    
    args = parser.parse_args()
    
    if not args.api_key:
        raise ValueError("Massive_API environment variable or --api-key argument required")
    
    log.info(f"Starting Massive live data collector")
    log.info(f"  API Key: {args.api_key[:10]}...")
    log.info(f"  Out Dir: {args.out_dir}")
    log.info(f"  Interval: {args.interval}s")
    log.info(f"  Indices: {', '.join(args.indices)}")
    
    client = RESTClient(api_key=args.api_key)
    
    try:
        while True:
            log.info(f"Collection cycle at {now_ts()}")
            success = collect_all_live_data(client, args.out_dir)
            log.info(f"Collected {success} data points successfully")
            
            if args.interval <= 0:
                break
            
            time.sleep(args.interval)
    except KeyboardInterrupt:
        log.info("Collector interrupted")
    except Exception as e:
        log.error(f"Fatal error: {e}")
        raise


if __name__ == "__main__":
    main()
