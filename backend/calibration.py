"""Calibration helpers for EOD prediction weights.

The calibration file is produced by tools/calibrate_eod_model.py. It is optional:
if no calibration exists, or validation did not beat the spot baseline, the live
predictor falls back to conservative deterministic weights.
"""
from __future__ import annotations

import json
import math
from datetime import datetime
from functools import lru_cache
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

BASE_DIR = Path(__file__).resolve().parent.parent
CALIBRATION_PATH = BASE_DIR / "models" / "eod_calibration.json"
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


def _num(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        number = float(value)
        return number if math.isfinite(number) else default
    except (TypeError, ValueError):
        return default


@lru_cache(maxsize=1)
def load_calibration() -> dict[str, Any]:
    if not CALIBRATION_PATH.exists():
        return {"version": None, "symbols": {}}
    try:
        with open(CALIBRATION_PATH, "r", encoding="utf-8") as handle:
            return json.load(handle)
    except Exception:
        return {"version": None, "symbols": {}}


def _minutes_to_close() -> float:
    now = datetime.now(ZoneInfo("America/New_York"))
    close = now.replace(hour=16, minute=0, second=0, microsecond=0)
    return max(0.0, (close - now).total_seconds() / 60.0)


def live_feature_vector(symbol: str, payload: dict[str, Any]) -> dict[str, float]:
    spot = _num(payload.get("price"))
    gamma_pin = _num(payload.get("gamma_pin"), spot)
    likely = _num(payload.get("likely_close") or payload.get("predicted_close"), spot)
    net_gex = _num(payload.get("net_gex"))
    gross_gex = abs(_num(payload.get("gross_gex"), abs(net_gex)))
    pin_gap = gamma_pin - spot
    pull_force = abs(net_gex / gross_gex) if gross_gex else 0.0
    expected_move = likely - spot
    minutes = _minutes_to_close()
    time_weight = 1.0 if minutes <= 30 else 0.75 if minutes <= 60 else 0.5
    weighted_gex = net_gex / max(abs(spot), 1.0)
    return {
        "pin_gap": pin_gap,
        "pin_distance_pct": (pin_gap / spot * 100.0) if spot else 0.0,
        "net_gex": net_gex,
        "pull_force": pull_force,
        "expected_move": expected_move,
        "time_weight": time_weight,
        "minutes_to_close": minutes,
        "weighted_gex": weighted_gex,
    }


def calibration_adjustment(symbol: str, payload: dict[str, Any]) -> tuple[float, dict[str, Any] | None]:
    calibration = load_calibration()
    symbol_model = calibration.get("symbols", {}).get(symbol.upper())
    if not symbol_model or not symbol_model.get("enabled"):
        return 0.0, None

    features = live_feature_vector(symbol, payload)
    means = symbol_model.get("feature_mean", {})
    scales = symbol_model.get("feature_scale", {})
    coefficients = symbol_model.get("coef", {})
    intercept = _num(symbol_model.get("intercept"))

    delta = intercept
    for feature in FEATURES:
        scale = _num(scales.get(feature), 1.0) or 1.0
        z_value = (_num(features.get(feature)) - _num(means.get(feature))) / scale
        delta += _num(coefficients.get(feature)) * z_value

    spot = _num(payload.get("price"))
    max_abs_adjustment = spot * _num(symbol_model.get("max_adjustment_pct"), 0.0025)
    bounded_delta = max(min(delta, max_abs_adjustment), -max_abs_adjustment)
    signal = {
        "name": "historical_calibration_model",
        "value": delta,
        "distance_points": bounded_delta,
        "weight": 1.0,
        "enabled": True,
        "validation_mae": symbol_model.get("validation_mae"),
        "spot_baseline_mae": symbol_model.get("spot_baseline_mae"),
        "improvement_pct": symbol_model.get("improvement_pct"),
        "features": features,
    }
    return bounded_delta, signal
