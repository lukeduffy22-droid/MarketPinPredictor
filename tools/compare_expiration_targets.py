"""Compare same-day and multi-expiration targets from saved prediction snapshots.

Usage:
    python tools/compare_expiration_targets.py exports actual_closes.csv

actual_closes.csv columns: symbol,trading_date,actual_close
The live multi-expiration target remains disabled until this report shows an
out-of-sample improvement over the same-day baseline.
"""
from __future__ import annotations

import argparse
import json
import re
from numbers import Integral
from pathlib import Path
from typing import Any

import pandas as pd


SUBSCRIPTION_EPOCH_PATTERN = re.compile(r"[0-9a-f]{64}")


def _canonical_subscription_epoch(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    return value if SUBSCRIPTION_EPOCH_PATTERN.fullmatch(value) else None


def _positive_subscription_generation(value: object) -> int | None:
    if isinstance(value, bool) or not isinstance(value, Integral) or value <= 0:
        return None
    return int(value)


def load_snapshots(exports_dir: Path, max_quote_age_seconds: float = 30.0) -> pd.DataFrame:
    rows: list[dict] = []
    for path in exports_dir.glob("*/*.ndjson"):
        for line in path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            payload = json.loads(line)
            if not payload.get("validation_is_valid"):
                continue
            if float(payload.get("spot_last") or 0) <= 0:
                continue
            if payload.get("same_day_target") is None or payload.get("multi_expiration_target") is None:
                continue
            if float(payload.get("contracts_count") or payload.get("contracts") or 0) <= 0:
                continue
            quote_age = payload.get("quote_age_seconds")
            if quote_age is not None and float(quote_age) > max_quote_age_seconds:
                continue
            subscription_epoch_id = _canonical_subscription_epoch(
                payload.get("subscription_epoch_id")
            )
            subscription_generation = _positive_subscription_generation(
                payload.get("subscription_generation")
            )
            if subscription_epoch_id is None or subscription_generation is None:
                continue
            rows.append({
                "symbol": str(payload.get("symbol", "")).upper(),
                "timestamp_utc": payload.get("timestamp_utc"),
                "spot": float(payload["spot_last"]),
                "same_day_target": float(payload["same_day_target"]),
                "multi_expiration_target": float(payload["multi_expiration_target"]),
                "pin_dispersion": float(payload.get("pin_dispersion") or 0.0),
                "subscription_epoch_id": subscription_epoch_id,
                "subscription_generation": subscription_generation,
                "quote_age_seconds": float(quote_age) if quote_age is not None else None,
                "fresh_quote_count": int(payload.get("fresh_quote_count") or 0),
                "active_contract_count": int(payload.get("contracts_count") or payload.get("contracts") or 0),
                "confidence": float(payload.get("confidence")) if payload.get("confidence") is not None else None,
            })
    frame = pd.DataFrame(rows)
    if frame.empty:
        return frame
    frame["timestamp_utc"] = pd.to_datetime(frame["timestamp_utc"], utc=True)
    # A dashboard refresh can repeat the same prediction. Process epochs are
    # part of semantic identity because generation counters restart.
    identity = [
        "symbol", "timestamp_utc", "subscription_epoch_id", "subscription_generation"
    ]
    value_columns = [
        column for column in frame.columns if column not in identity
    ]
    conflicting = frame.groupby(identity, dropna=False)[value_columns].nunique(
        dropna=False
    ).gt(1).any(axis=1)
    if conflicting.any():
        raise ValueError("conflicting snapshots share a subscription epoch identity")
    return frame.drop_duplicates(subset=identity)


def score(predictions: pd.Series, actual: pd.Series, spot: pd.Series) -> dict[str, float | int | None]:
    errors = predictions - actual
    direction = ((predictions - spot) * (actual - spot)) >= 0
    return {
        "count": int(len(errors)),
        "mae": float(errors.abs().mean()) if len(errors) else None,
        "rmse": float((errors.pow(2).mean() ** 0.5)) if len(errors) else None,
        "direction_accuracy": float(direction.mean() * 100.0) if len(errors) else None,
    }


def calibration_summary(confidence: pd.Series, errors: pd.Series) -> dict[str, Any]:
    if confidence.empty:
        return {"count": 0, "mean_confidence": None, "high_confidence_mae": None}
    high = errors[confidence >= 80]
    return {
        "count": int(len(confidence)),
        "mean_confidence": float(confidence.mean()),
        "high_confidence_count": int(len(high)),
        "high_confidence_mae": float(high.abs().mean()) if len(high) else None,
    }


def evaluate(joined: pd.DataFrame) -> dict[str, Any]:
    report: dict[str, Any] = {"symbols": {}, "deployment": {}}
    for symbol, frame in joined.groupby("symbol"):
        same = score(frame["same_day_target"], frame["actual_close"], frame["spot"])
        multi = score(frame["multi_expiration_target"], frame["actual_close"], frame["spot"])
        same_errors = frame["same_day_target"] - frame["actual_close"]
        multi_errors = frame["multi_expiration_target"] - frame["actual_close"]
        report["symbols"][symbol] = {
            "same_day": same,
            "multi_expiration": multi,
            "mean_pin_dispersion": float(frame["pin_dispersion"].mean()),
            "same_day_confidence": calibration_summary(frame.get("confidence", pd.Series(dtype=float)), same_errors),
            "multi_expiration_mae_improvement": (
                same["mae"] - multi["mae"] if same["mae"] is not None and multi["mae"] is not None else None
            ),
            "subscription_identities": [
                {
                    "subscription_epoch_id": str(epoch),
                    "subscription_generation": int(generation),
                }
                for epoch, generation in sorted({
                    (str(row.subscription_epoch_id), int(row.subscription_generation))
                    for row in frame[["subscription_epoch_id", "subscription_generation"]].itertuples(index=False)
                })
            ] if {"subscription_epoch_id", "subscription_generation"} <= set(frame.columns) else [],
        }

    improvements = [
        result["multi_expiration_mae_improvement"]
        for result in report["symbols"].values()
        if result["multi_expiration_mae_improvement"] is not None
    ]
    report["deployment"] = {
        "multi_expiration_enabled": False,
        "recommendation": "enable" if improvements and all(value > 0 for value in improvements) else "keep_0DTE_baseline",
        "reason": "Requires lower MAE for every evaluated symbol; otherwise retain the validated 0DTE baseline.",
    }
    return report


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("exports_dir", type=Path)
    parser.add_argument("actuals_csv", type=Path)
    parser.add_argument("--output", type=Path, help="Optional JSON report path")
    parser.add_argument("--max-quote-age", type=float, default=30.0)
    args = parser.parse_args()

    snapshots = load_snapshots(args.exports_dir, max_quote_age_seconds=args.max_quote_age)
    actuals = pd.read_csv(args.actuals_csv)
    actuals["symbol"] = actuals["symbol"].astype(str).str.upper()
    snapshots["trading_date"] = pd.to_datetime(snapshots["timestamp_utc"]).dt.date.astype(str)
    actuals["trading_date"] = actuals["trading_date"].astype(str)
    joined = snapshots.merge(actuals, on=["symbol", "trading_date"], how="inner")

    if joined.empty:
        print("No overlapping valid snapshots and actual closes found.")
        return 1

    report = evaluate(joined)
    print(json.dumps(report, indent=2))
    if args.output:
        args.output.write_text(json.dumps(report, indent=2), encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
