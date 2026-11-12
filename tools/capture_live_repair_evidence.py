"""Bounded, read-only local runtime/persistence receipts; never calls providers."""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import sqlite3
import time
import urllib.error
import urllib.request


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--samples", type=int, default=6, choices=range(2, 41))
    parser.add_argument("--interval", type=int, default=15, choices=range(5, 61))
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    database = Path(__file__).resolve().parents[1] / "data/market_data.db"
    manifest = {}
    for sample in range(args.samples):
        started = time.monotonic()
        receipt = {"observed_at_utc": datetime.now(timezone.utc).isoformat()}
        for name, route in (("health", "/health"), ("live", "/health/live"),
                            ("SPX", "/predict/SPX"), ("NDX", "/predict/NDX")):
            try:
                with urllib.request.urlopen("http://127.0.0.1:8000" + route, timeout=5) as response:
                    receipt[name] = {"http_status": response.status, "body": json.load(response)}
            except urllib.error.HTTPError as exc:
                receipt[name] = {"http_status": exc.code, "body": exc.read().decode("utf-8", errors="replace")}
            except Exception as exc:
                receipt[name] = {"error_type": type(exc).__name__}
        receipt["persistence"] = {}
        try:
            with sqlite3.connect(database.resolve().as_uri() + "?mode=ro", uri=True, timeout=1) as connection:
                connection.row_factory = sqlite3.Row
                for table, columns in (
                    ("gamma_calculation_runs", "id,symbol,calculation_id,calculated_at_utc,subscription_epoch_id,subscription_generation,status"),
                    ("prediction_snapshots", "id,symbol,timestamp_utc,predicted_close,is_valid,subscription_epoch_id,subscription_generation"),
                ):
                    # Indexed tail only; no large table count or integrity scan during ingestion.
                    rows = connection.execute(f"SELECT {columns} FROM {table} ORDER BY id DESC LIMIT 20").fetchall()
                    receipt["persistence"][table] = [dict(row) for row in rows]
        except Exception as exc:
            receipt["persistence"]["error_type"] = type(exc).__name__
        path = args.output / f"{sample:03d}.json"
        encoded = json.dumps(receipt, indent=2, sort_keys=True, default=str).encode()
        with path.open("xb") as target:
            target.write(encoded)
        path.chmod(0o444)
        manifest[path.name] = hashlib.sha256(encoded).hexdigest()
        live = receipt.get("live", {}).get("body", {})
        live = live if isinstance(live, dict) else {}
        print(json.dumps({"sample": sample, "at": receipt["observed_at_utc"],
            "generation": live.get("active_generation"), "messages": live.get("messages_received"),
            "reconnects": live.get("reconnect_attempts"),
            "SPX_http": receipt["SPX"].get("http_status"), "NDX_http": receipt["NDX"].get("http_status")}), flush=True)
        if sample + 1 < args.samples:
            time.sleep(max(0, args.interval - (time.monotonic() - started)))
    path = args.output / "manifest.json"
    with path.open("x", encoding="utf-8") as target:
        json.dump(manifest, target, indent=2, sort_keys=True)
    path.chmod(0o444)


if __name__ == "__main__":
    main()
