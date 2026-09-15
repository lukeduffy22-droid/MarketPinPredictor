"""Historical context features from existing Polygon-era data files.

This module reads the local multi-year market history collected by the prior
Polygon workflow and exposes lightweight, leakage-safe context features for the
live Databento prediction layer. It does not train a model in the request path;
it computes recent realized trend/volatility and VIX regime context.
"""
from __future__ import annotations

import math
import os
from functools import lru_cache
from pathlib import Path
from typing import Any

import pandas as pd

BASE_DIR = Path(__file__).resolve().parent.parent
MARKET_HISTORY_FILES = [
    (BASE_DIR / "historical_market_data_20years.csv", 0),
    (BASE_DIR / "historical_market_data_extended.csv", 1),
    (BASE_DIR / "historical_market_data_3years.csv", 2),
]
GAMMA_HISTORY_CSV = BASE_DIR / "gamma_snapshots.csv"
HISTORICAL_CONTEXT_MAX_AGE_DAYS = int(os.getenv("HISTORICAL_CONTEXT_MAX_AGE_DAYS", "10"))


def _age_days(timestamp: Any) -> float | None:
    if timestamp is None or pd.isna(timestamp):
        return None
    parsed = pd.Timestamp(timestamp)
    if parsed.tzinfo is None:
        parsed = parsed.tz_localize("UTC")
    else:
        parsed = parsed.tz_convert("UTC")
    return max(0.0, (pd.Timestamp.now(tz="UTC") - parsed).total_seconds() / 86400.0)


@lru_cache(maxsize=1)
def _load_market_history() -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    columns = ["symbol", "timestamp", "close", "timeframe", "source"]
    for path, priority in MARKET_HISTORY_FILES:
        if not path.exists():
            continue
        frame = pd.read_csv(path, usecols=lambda column: column in columns)
        frame["timestamp"] = pd.to_datetime(frame["timestamp"], errors="coerce")
        frame["close"] = pd.to_numeric(frame["close"], errors="coerce")
        frame = frame.dropna(subset=["symbol", "timestamp", "close"])
        if "timeframe" in frame.columns:
            daily = frame[frame["timeframe"].astype(str).str.lower().eq("day")].copy()
            if not daily.empty:
                frame = daily
        if "source" not in frame.columns:
            frame["source"] = path.stem
        frame["history_file"] = path.name
        frame["history_priority"] = priority
        frames.append(frame)

    if not frames:
        return pd.DataFrame(columns=[
            *columns,
            "history_file",
            "history_priority",
        ])

    df = pd.concat(frames, ignore_index=True)
    df["symbol"] = df["symbol"].astype(str).str.upper()
    # Prefer more recent curated files on overlapping symbol/date rows.
    df = df.sort_values(["symbol", "timestamp", "history_priority"])
    df = df.drop_duplicates(subset=["symbol", "timestamp"], keep="last")
    return df.sort_values(["symbol", "timestamp"])


def get_market_history() -> pd.DataFrame:
    """Return the combined historical market file used by live context."""
    return _load_market_history().copy()


@lru_cache(maxsize=1)
def _load_gamma_history() -> pd.DataFrame:
    if not GAMMA_HISTORY_CSV.exists():
        return pd.DataFrame()
    df = pd.read_csv(GAMMA_HISTORY_CSV)
    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    for column in ["spot_price", "gamma_pin", "pin_distance_pct", "net_gex", "pull_force", "expected_move", "minutes_to_close"]:
        if column in df.columns:
            df[column] = pd.to_numeric(df[column], errors="coerce")
    return df.dropna(subset=["symbol"]).sort_values(["symbol", "timestamp"] if "timestamp" in df.columns else ["symbol"])


def _safe_return(series: pd.Series, periods: int) -> float | None:
    if len(series) <= periods:
        return None
    start = float(series.iloc[-periods - 1])
    end = float(series.iloc[-1])
    if start <= 0:
        return None
    return (end / start) - 1.0


def _realized_vol(series: pd.Series, window: int = 20) -> float | None:
    if len(series) < window + 1:
        return None
    returns = series.astype(float).pct_change().dropna().tail(window)
    if returns.empty:
        return None
    return float(returns.std() * math.sqrt(252))


def _percentile(series: pd.Series, value: float) -> float | None:
    clean = series.dropna().astype(float)
    if clean.empty:
        return None
    return float((clean <= value).mean())


def get_historical_context(symbol: str) -> dict[str, Any]:
    """Return historical context features for a live symbol.

    Features are computed from data already on disk and use only trailing local
    observations. This is safe to consume in a live prediction as context, not as
    a trained supervised model.
    """
    symbol = symbol.upper()
    market = _load_market_history()
    gamma = _load_gamma_history()

    symbol_history = market[market["symbol"].astype(str).str.upper().eq(symbol)].copy()
    closes = symbol_history["close"] if not symbol_history.empty else pd.Series(dtype=float)

    vix_history = market[market["symbol"].astype(str).str.upper().eq("VIX")].copy()
    vix_close = float(vix_history["close"].iloc[-1]) if not vix_history.empty else None
    vix_percentile = _percentile(vix_history["close"], vix_close) if vix_close is not None else None

    gamma_history = gamma[gamma["symbol"].astype(str).str.upper().eq(symbol)].copy() if not gamma.empty else pd.DataFrame()
    gamma_bias = None
    gamma_samples = 0
    if not gamma_history.empty and "expected_move" in gamma_history.columns:
        recent = gamma_history["expected_move"].dropna().tail(50)
        gamma_samples = int(len(recent))
        if not recent.empty:
            gamma_bias = float(recent.mean())

    history_end = symbol_history["timestamp"].max() if not symbol_history.empty else None
    vix_history_end = vix_history["timestamp"].max() if not vix_history.empty else None
    history_age_days = _age_days(history_end)
    vix_history_age_days = _age_days(vix_history_end)
    history_is_fresh = history_age_days is not None and history_age_days <= HISTORICAL_CONTEXT_MAX_AGE_DAYS
    vix_history_is_fresh = vix_history_age_days is not None and vix_history_age_days <= HISTORICAL_CONTEXT_MAX_AGE_DAYS

    context = {
        "symbol": symbol,
        "history_rows": int(len(symbol_history)),
        "history_start": symbol_history["timestamp"].min().isoformat() if not symbol_history.empty else None,
        "history_end": history_end.isoformat() if history_end is not None else None,
        "history_age_days": history_age_days,
        "history_max_age_days": HISTORICAL_CONTEXT_MAX_AGE_DAYS,
        "history_is_fresh": history_is_fresh,
        "history_files": sorted(symbol_history["history_file"].dropna().unique().tolist()) if "history_file" in symbol_history.columns and not symbol_history.empty else [],
        "history_source_counts": symbol_history.groupby("history_file").size().to_dict() if "history_file" in symbol_history.columns and not symbol_history.empty else {},
        "return_1d": _safe_return(closes, 1),
        "return_5d": _safe_return(closes, 5),
        "return_20d": _safe_return(closes, 20),
        "realized_vol_20d": _realized_vol(closes, 20),
        "vix_last": vix_close,
        "vix_percentile": vix_percentile,
        "vix_history_end": vix_history_end.isoformat() if vix_history_end is not None else None,
        "vix_history_age_days": vix_history_age_days,
        "vix_history_is_fresh": vix_history_is_fresh,
        "gamma_history_samples": gamma_samples,
        "gamma_expected_move_bias": gamma_bias,
        "historical_adjustment_eligible": bool(history_is_fresh and vix_history_is_fresh),
    }
    return context


def historical_adjustment(symbol: str, spot: float, context: dict[str, Any]) -> tuple[float, list[dict[str, Any]]]:
    """Small bounded adjustment derived from historical context.

    This is intentionally conservative until a supervised backtest proves a
    larger adjustment improves EOD error.
    """
    signals: list[dict[str, Any]] = []
    adjustment = 0.0

    if context.get("historical_adjustment_eligible") is False:
        return 0.0, [{
            "name": "historical_context_stale_excluded",
            "value": context.get("history_age_days"),
            "distance_points": 0.0,
            "weight": 0.0,
            "max_age_days": context.get("history_max_age_days", HISTORICAL_CONTEXT_MAX_AGE_DAYS),
        }]

    ret_5d = context.get("return_5d")
    ret_20d = context.get("return_20d")
    realized_vol = context.get("realized_vol_20d")
    vix_percentile = context.get("vix_percentile")
    gamma_bias = context.get("gamma_expected_move_bias")

    if isinstance(ret_5d, (int, float)) and math.isfinite(ret_5d):
        contribution = spot * max(min(ret_5d * 0.015, 0.0008), -0.0008)
        adjustment += contribution
        signals.append({"name": "historical_5d_momentum", "value": ret_5d, "distance_points": contribution, "weight": 0.015})

    if isinstance(ret_20d, (int, float)) and math.isfinite(ret_20d):
        contribution = spot * max(min(ret_20d * 0.006, 0.0006), -0.0006)
        adjustment += contribution
        signals.append({"name": "historical_20d_trend", "value": ret_20d, "distance_points": contribution, "weight": 0.006})

    if isinstance(realized_vol, (int, float)) and math.isfinite(realized_vol):
        # Higher realized vol lowers confidence more than it changes directional target.
        signals.append({"name": "historical_realized_vol_20d", "value": realized_vol, "distance_points": 0.0, "weight": 0.0})

    if isinstance(vix_percentile, (int, float)) and math.isfinite(vix_percentile):
        # High VIX percentile is risk-off for equity indexes; low percentile is mildly risk-on.
        if symbol.upper() != "VIX":
            centered = vix_percentile - 0.5
            contribution = -spot * max(min(centered * 0.0012, 0.0006), -0.0006)
            adjustment += contribution
            signals.append({"name": "historical_vix_regime", "value": vix_percentile, "distance_points": contribution, "weight": 0.0012})

    if isinstance(gamma_bias, (int, float)) and math.isfinite(gamma_bias):
        contribution = max(min(gamma_bias * 0.10, spot * 0.0005), -spot * 0.0005)
        adjustment += contribution
        signals.append({"name": "historical_gamma_expected_move_bias", "value": gamma_bias, "distance_points": contribution, "weight": 0.10})

    return adjustment, signals
