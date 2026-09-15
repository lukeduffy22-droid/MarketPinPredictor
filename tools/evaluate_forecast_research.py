"""Score the frozen research journal against verified, available close evidence."""
from __future__ import annotations
import argparse
from contextlib import closing
from datetime import datetime, timezone
import json
from pathlib import Path
import sqlite3
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


def evaluate_journal(source_path, journal_path, *, as_of_utc):
    from backend.forecast_horizons import evaluate_methods
    from backend.forecast_repository import load_research_history
    from backend.market_universe import DEFAULT_TRACKED_SYMBOLS
    histories, diagnostics = load_research_history(source_path, DEFAULT_TRACKED_SYMBOLS, as_of_utc=as_of_utc)
    outcomes = [row for rows in histories.values() for row in rows if row.get("kind") == "daily_close"]
    forecasts = []
    if Path(journal_path).is_file():
        with closing(sqlite3.connect(Path(journal_path).resolve().as_uri() + "?mode=ro", uri=True, timeout=0.25)) as conn:
            conn.execute("PRAGMA query_only=ON")
            for row in conn.execute("SELECT payload_json FROM research_forecasts ORDER BY as_of_utc LIMIT 100000"):
                forecasts.append(json.loads(row[0])["forecast"])
    result = evaluate_methods(forecasts, outcomes, as_of_utc=as_of_utc)
    result["frozen_forecast_count"] = len(forecasts)
    result["verified_daily_close_count"] = len(outcomes)
    result["retained_data_diagnostics"] = diagnostics
    result["daily_coverage"] = {s: {"verified_closes": len([r for r in rows if r.get("kind") == "daily_close"]),
                                   "latest_verified_session": max([r["session_date"] for r in rows if r.get("kind") == "daily_close"], default=None)}
                                for s, rows in histories.items()}
    result["accuracy_improvement_verified"] = False
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--source-database", type=Path, default=ROOT / "data" / "market_data.db")
    args = parser.parse_args()
    report = evaluate_journal(args.source_database,
                              ROOT / "data" / "forecast_research.db", as_of_utc=datetime.now(timezone.utc))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2, allow_nan=False), encoding="utf-8")
    print(json.dumps({k: report[k] for k in ("status", "frozen_forecast_count", "verified_daily_close_count", "accuracy_improvement_verified")}))


if __name__ == "__main__":
    main()
