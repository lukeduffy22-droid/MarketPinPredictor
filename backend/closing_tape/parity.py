from __future__ import annotations

import math
import re
import json
from datetime import datetime, timezone
from typing import Mapping

import numpy as np
import pandas as pd


PARITY_REFERENCE_METHOD = "tcbbo-pretrade-nbbo-parity-estimate-v1"
PARITY_REFERENCE_VERSION = "1.0"
SERIES_ROOT_RE = re.compile(r"^(?P<root>.{1,6})\s+\d{6}[CP]\d{8}$")


def estimate_tcbbo_parity_reference_prices(
    contract_rows: pd.DataFrame,
    *,
    minimum_pairs: int = 5,
    max_pair_age_seconds: float = 5.0,
    annual_rate: float = 0.0,
    settlement_hour_utc: int = 20,
    excluded_families: tuple[str, ...] = ("VIX",),
) -> pd.DataFrame:
    """Estimate a point-in-time reference from paired call/put TCBBO midpoints.

    This is explicitly calculated evidence, not an observed underlying print.
    Pairs must share series root, expiration, strike, and minute, and their last
    valid pre-trade NBBO timestamps must be close enough to be comparable.
    """
    if minimum_pairs < 1 or max_pair_age_seconds <= 0:
        raise ValueError("minimum_pairs and max_pair_age_seconds must be positive")
    if not math.isfinite(annual_rate):
        raise ValueError("annual_rate must be finite")
    required = {
        "trading_date", "session_id", "family_root", "raw_symbol", "expiration",
        "option_type", "strike", "minute_utc", "last_pretrade_midpoint",
        "last_nbbo_event_ns", "capture_integrity_verified", "source_sha256",
    }
    missing = sorted(required - set(contract_rows.columns))
    if missing:
        raise ValueError(f"parity reference columns missing: {', '.join(missing)}")
    output_columns = [
        "family_root", "trading_date", "timestamp_utc", "quote_timestamp_utc",
        "session_id", "minute_utc",
        "current_price", "provider", "reference_price_method",
        "reference_price_is_estimate", "reference_price_provider",
        "parity_pair_count", "parity_dispersion_bps", "source_sha256",
    ]
    if contract_rows.empty:
        return pd.DataFrame(columns=output_columns)
    frame = contract_rows.copy()
    if not frame["capture_integrity_verified"].fillna(False).astype(bool).all():
        raise ValueError("parity reference refuses rows without verified capture integrity")
    frame = frame[~frame["family_root"].isin(excluded_families)].copy()
    frame["midpoint"] = pd.to_numeric(frame["last_pretrade_midpoint"], errors="coerce")
    frame["event_ns"] = pd.to_numeric(frame["last_nbbo_event_ns"], errors="coerce")
    frame["strike"] = pd.to_numeric(frame["strike"], errors="coerce")
    frame["minute_utc"] = pd.to_datetime(frame["minute_utc"], utc=True)
    frame["expiration"] = pd.to_datetime(frame["expiration"]).dt.normalize()
    frame["series_root"] = frame["raw_symbol"].astype(str).str.extract(SERIES_ROOT_RE)["root"].str.strip()
    frame = frame[
        frame["option_type"].isin(["C", "P"])
        & frame["series_root"].notna()
        & (frame["midpoint"] > 0)
        & (frame["strike"] > 0)
        & frame["event_ns"].notna()
    ].copy()
    if frame.empty:
        return pd.DataFrame(columns=output_columns)

    pair_keys = [
        "trading_date", "session_id", "family_root", "series_root",
        "expiration", "strike", "minute_utc", "source_sha256",
    ]
    if frame.duplicated([*pair_keys, "option_type"]).any():
        raise ValueError("parity reference has duplicate call/put contract rows")
    paired = frame.pivot(index=pair_keys, columns="option_type", values=["midpoint", "event_ns"])
    paired.columns = [f"{field}_{kind}" for field, kind in paired.columns]
    paired = paired.reset_index()
    needed = ["midpoint_C", "midpoint_P", "event_ns_C", "event_ns_P"]
    paired = paired.dropna(subset=needed)
    paired["pair_age_seconds"] = (
        (paired["event_ns_C"] - paired["event_ns_P"]).abs() / 1_000_000_000.0
    )
    paired = paired[paired["pair_age_seconds"] <= max_pair_age_seconds].copy()
    if paired.empty:
        return pd.DataFrame(columns=output_columns)
    paired["available_ns"] = paired[["event_ns_C", "event_ns_P"]].max(axis=1).astype("int64")
    available = pd.to_datetime(paired["available_ns"], unit="ns", utc=True)
    expiry = pd.to_datetime(paired["expiration"], utc=True) + pd.to_timedelta(
        settlement_hour_utc, unit="h"
    )
    paired["years_to_expiry"] = ((expiry - available).dt.total_seconds() / (365.25 * 86400)).clip(lower=0)
    paired["candidate_price"] = (
        paired["midpoint_C"] - paired["midpoint_P"]
        + paired["strike"] * np.exp(-annual_rate * paired["years_to_expiry"])
    )
    paired = paired[np.isfinite(paired["candidate_price"]) & (paired["candidate_price"] > 0)]

    rows: list[dict[str, object]] = []
    group_keys = ["trading_date", "session_id", "family_root", "minute_utc", "source_sha256"]
    for keys, group in paired.groupby(group_keys, sort=True, observed=True):
        values = group["candidate_price"].to_numpy(dtype=float)
        if len(values) < minimum_pairs:
            continue
        median = float(np.median(values))
        deviations = np.abs(values - median)
        mad = float(np.median(deviations))
        accepted = group if mad <= 0 else group[deviations <= 5.0 * mad]
        if len(accepted) < minimum_pairs:
            continue
        estimate = float(np.median(accepted["candidate_price"].to_numpy(dtype=float)))
        dispersion = float(
            np.median(np.abs(accepted["candidate_price"].to_numpy(dtype=float) - estimate))
            / estimate * 10_000.0
        )
        timestamp = pd.to_datetime(int(accepted["available_ns"].max()), unit="ns", utc=True)
        trading_date, _session_id, family_root, _minute, source_sha256 = keys
        rows.append(
            {
                "family_root": str(family_root), "trading_date": str(trading_date),
                "session_id": str(_session_id), "minute_utc": _minute,
                "timestamp_utc": timestamp, "quote_timestamp_utc": timestamp,
                "current_price": estimate, "provider": "databento-opra-tcbbo",
                "reference_price_method": PARITY_REFERENCE_METHOD,
                "reference_price_is_estimate": True,
                "reference_price_provider": "databento-opra-tcbbo",
                "parity_pair_count": int(len(accepted)),
                "parity_dispersion_bps": dispersion,
                "source_sha256": str(source_sha256),
            }
        )
    if not rows:
        return pd.DataFrame(columns=output_columns)
    return pd.DataFrame(rows, columns=output_columns).sort_values(
        ["timestamp_utc", "family_root"]
    ).reset_index(drop=True)


def persist_parity_reference_prices(
    catalog,
    prices: pd.DataFrame,
    *,
    feed_name: str = "opra_options",
    parameters: Mapping[str, object] | None = None,
) -> None:
    """Persist parity output only in the explicitly inferred reference table."""
    if prices.empty:
        return
    required = {
        "session_id", "family_root", "minute_utc", "timestamp_utc", "current_price",
        "reference_price_method", "parity_pair_count", "parity_dispersion_bps",
        "source_sha256",
    }
    missing = sorted(required - set(prices.columns))
    if missing:
        raise ValueError(f"parity persistence columns missing: {', '.join(missing)}")
    timestamp = datetime.now(timezone.utc).isoformat()
    parameters_json = json.dumps(
        dict(parameters or {}), sort_keys=True, separators=(",", ":"), allow_nan=False
    )
    rows = []
    for raw in prices.to_dict(orient="records"):
        rows.append(
            {
                "session_id": str(raw["session_id"]), "feed_name": feed_name,
                "family_root": str(raw["family_root"]),
                "minute_utc": pd.Timestamp(raw["minute_utc"]).isoformat(),
                "reference_method": str(raw["reference_price_method"]),
                "reference_version": PARITY_REFERENCE_VERSION,
                "source_sha256": str(raw["source_sha256"]),
                "estimated_price": float(raw["current_price"]),
                "available_at_utc": pd.Timestamp(raw["timestamp_utc"]).isoformat(),
                "pair_count": int(raw["parity_pair_count"]),
                "dispersion_bps": float(raw["parity_dispersion_bps"]),
                "parameters_json": parameters_json, "updated_at_utc": timestamp,
            }
        )
    catalog.upsert_inferred_reference_minutes(rows)
