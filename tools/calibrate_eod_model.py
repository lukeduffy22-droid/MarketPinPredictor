from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.linear_model import Ridge
from sklearn.metrics import mean_absolute_error, mean_squared_error
from sklearn.preprocessing import StandardScaler

BASE = Path(__file__).resolve().parent.parent
if str(BASE) not in sys.path:
    sys.path.insert(0, str(BASE))

from backend.historical_context import MARKET_HISTORY_FILES, get_market_history

GAMMA_SNAPSHOTS = BASE / "gamma_snapshots.csv"
OUTPUT = BASE / "models" / "eod_calibration.json"
FEATURES = [
    "pin_gap",
    "pin_distance_pct",
    "net_gex",
    "pull_force",
    "expected_move",
    "time_weight",
    "minutes_to_close",
    "weighted_gex",
]
MIN_ROWS = 50
MIN_IMPROVEMENT = 0.02


def load_joined() -> pd.DataFrame:
    market = get_market_history()
    gamma = pd.read_csv(GAMMA_SNAPSHOTS)
    gamma["timestamp"] = pd.to_datetime(gamma["timestamp"], errors="coerce")
    market["trading_date"] = market["timestamp"].dt.date
    gamma["trading_date"] = gamma["timestamp"].dt.date

    if "timeframe" in market.columns:
        daily = market[market["timeframe"].astype(str).str.lower().eq("day")].copy()
        if not daily.empty:
            market = daily

    closes = (
        market.sort_values("timestamp")
        .groupby(["symbol", "trading_date"], as_index=False)
        .tail(1)[["symbol", "trading_date", "close"]]
        .rename(columns={"close": "actual_close"})
    )
    joined = gamma.merge(closes, on=["symbol", "trading_date"], how="left")
    joined = joined.dropna(subset=["actual_close", "spot_price", "gamma_pin"]).copy()

    joined["pin_gap"] = joined["gamma_pin"] - joined["spot_price"]
    joined["target_delta"] = joined["actual_close"] - joined["spot_price"]
    for column in FEATURES + ["target_delta", "actual_close", "spot_price"]:
        joined[column] = pd.to_numeric(joined[column], errors="coerce")
    joined = joined.dropna(subset=FEATURES + ["target_delta"])
    return joined.sort_values("timestamp")


def fit_symbol(symbol: str, frame: pd.DataFrame) -> dict:
    frame = frame.sort_values("timestamp").copy()
    result = {
        "enabled": False,
        "rows": int(len(frame)),
        "reason": None,
        "features": FEATURES,
    }
    if len(frame) < MIN_ROWS:
        result["reason"] = f"insufficient rows ({len(frame)} < {MIN_ROWS})"
        return result

    split = int(len(frame) * 0.70)
    train = frame.iloc[:split].copy()
    valid = frame.iloc[split:].copy()
    if train.empty or valid.empty:
        result["reason"] = "empty chronological split"
        return result

    scaler = StandardScaler()
    x_train = scaler.fit_transform(train[FEATURES].values)
    y_train = train["target_delta"].values
    x_valid = scaler.transform(valid[FEATURES].values)
    y_valid = valid["target_delta"].values

    model = Ridge(alpha=2.0)
    model.fit(x_train, y_train)
    pred_delta = model.predict(x_valid)
    pred_close = valid["spot_price"].values + pred_delta

    spot_pred = valid["spot_price"].values
    pin_pred = valid["gamma_pin"].values
    expected_move_pred = valid["spot_price"].values + valid["expected_move"].fillna(0).values

    validation_mae = float(mean_absolute_error(valid["actual_close"].values, pred_close))
    spot_mae = float(mean_absolute_error(valid["actual_close"].values, spot_pred))
    pin_mae = float(mean_absolute_error(valid["actual_close"].values, pin_pred))
    expected_move_mae = float(mean_absolute_error(valid["actual_close"].values, expected_move_pred))
    rmse = float(mean_squared_error(valid["actual_close"].values, pred_close) ** 0.5)
    improvement = (spot_mae - validation_mae) / spot_mae if spot_mae else 0.0

    result.update({
        "enabled": bool(improvement >= MIN_IMPROVEMENT),
        "reason": None if improvement >= MIN_IMPROVEMENT else "did not beat spot baseline by required margin",
        "train_rows": int(len(train)),
        "validation_rows": int(len(valid)),
        "validation_mae": validation_mae,
        "validation_rmse": rmse,
        "spot_baseline_mae": spot_mae,
        "pin_baseline_mae": pin_mae,
        "expected_move_baseline_mae": expected_move_mae,
        "improvement_pct": float(improvement * 100.0),
        "intercept": float(model.intercept_),
        "coef": {feature: float(coef) for feature, coef in zip(FEATURES, model.coef_)},
        "feature_mean": {feature: float(value) for feature, value in zip(FEATURES, scaler.mean_)},
        "feature_scale": {feature: float(value if value != 0 else 1.0) for feature, value in zip(FEATURES, scaler.scale_)},
        "max_adjustment_pct": 0.0025,
    })
    return result


def main() -> None:
    joined = load_joined()
    output = {
        "version": "eod_calibration_v1",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "source_files": {
            "market_history": [str(path) for path, _ in MARKET_HISTORY_FILES if path.exists()],
            "gamma_snapshots": str(GAMMA_SNAPSHOTS),
        },
        "joined_rows": int(len(joined)),
        "min_rows": MIN_ROWS,
        "min_improvement": MIN_IMPROVEMENT,
        "symbols": {},
    }
    print(f"Joined rows: {len(joined)}")
    for symbol, frame in joined.groupby("symbol"):
        result = fit_symbol(symbol, frame)
        output["symbols"][symbol] = result
        status = "ENABLED" if result.get("enabled") else "disabled"
        print(
            f"{symbol}: {status} rows={result.get('rows')} "
            f"mae={result.get('validation_mae')} spot_mae={result.get('spot_baseline_mae')} "
            f"improve={result.get('improvement_pct')} reason={result.get('reason')}"
        )

    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT, "w", encoding="utf-8") as handle:
        json.dump(output, handle, indent=2)
    print(f"Wrote {OUTPUT}")


if __name__ == "__main__":
    main()
