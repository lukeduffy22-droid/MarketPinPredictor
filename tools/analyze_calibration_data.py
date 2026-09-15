from __future__ import annotations

import sqlite3
from pathlib import Path

import pandas as pd

BASE = Path(__file__).resolve().parent.parent


def inspect_csvs() -> None:
    for name in [
        "historical_market_data_3years.csv",
        "multi_index_predictions.csv",
        "gamma_snapshots.csv",
        "gamma_summary.csv",
    ]:
        path = BASE / name
        print(f"\nCSV {name} exists={path.exists()}")
        if not path.exists():
            continue
        df = pd.read_csv(path)
        print("rows", len(df))
        print("columns", list(df.columns))
        if "timestamp" in df.columns:
            ts = pd.to_datetime(df["timestamp"], errors="coerce")
            print("time range", ts.min(), ts.max())
        if "symbol" in df.columns:
            print("symbols", sorted(df["symbol"].dropna().astype(str).unique().tolist())[:30])
            print(df.groupby("symbol").size().sort_values(ascending=False).head(20).to_string())
        print(df.head(3).to_string(index=False))


def inspect_join() -> None:
    market_path = BASE / "historical_market_data_3years.csv"
    gamma_path = BASE / "gamma_snapshots.csv"
    if not market_path.exists() or not gamma_path.exists():
        return

    market = pd.read_csv(market_path)
    gamma = pd.read_csv(gamma_path)
    market["timestamp"] = pd.to_datetime(market["timestamp"], errors="coerce")
    gamma["timestamp"] = pd.to_datetime(gamma["timestamp"], errors="coerce")
    market["trading_date"] = market["timestamp"].dt.date
    gamma["trading_date"] = gamma["timestamp"].dt.date
    daily = market[market.get("timeframe", "day").astype(str).eq("day")].copy() if "timeframe" in market.columns else market.copy()
    closes = daily.sort_values("timestamp").groupby(["symbol", "trading_date"], as_index=False).tail(1)[["symbol", "trading_date", "close"]]
    joined = gamma.merge(closes, on=["symbol", "trading_date"], how="left", suffixes=("", "_actual"))
    print("\nJOIN gamma -> same-day close")
    print("gamma rows", len(gamma), "joined actual close rows", joined["close"].notna().sum())
    print(joined.groupby("symbol")["close"].agg(["count", "size"]).to_string())
    usable = joined[joined["close"].notna()].copy()
    if usable.empty:
        print("No overlapping same-day close rows. Gamma calibration cannot be trained from this price file yet.")
    else:
        for symbol, frame in usable.groupby("symbol"):
            spot_mae = (frame["close"] - frame["spot_price"]).abs().mean()
            pin_mae = (frame["close"] - frame["gamma_pin"]).abs().mean()
            print(symbol, "spot_mae", round(spot_mae, 3), "pin_mae", round(pin_mae, 3), "rows", len(frame))


def inspect_dbs() -> None:
    for name in ["market_predictor.db", "gamma_analysis.db", "market.db", "data/market_data.db"]:
        path = BASE / name
        print(f"\nDB {name} exists={path.exists()}")
        if not path.exists():
            continue
        con = sqlite3.connect(path)
        cur = con.cursor()
        tables = [row[0] for row in cur.execute("select name from sqlite_master where type='table' order by name")]
        print("tables", tables)
        for table in tables:
            count = cur.execute(f"select count(*) from {table}").fetchone()[0]
            print(table, count)
            if count:
                cols = [row[1] for row in cur.execute(f"pragma table_info({table})")]
                print(" columns", cols[:20])
        con.close()


if __name__ == "__main__":
    inspect_csvs()
    inspect_join()
    inspect_dbs()
