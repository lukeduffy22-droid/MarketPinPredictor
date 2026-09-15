from __future__ import annotations

import hashlib
import json
import re

import numpy as np
import pandas as pd


SURFACE_FEATURE_VERSION = "contract-surface-v4"
SUBSCRIPTION_EPOCH_PATTERN = re.compile(r"[0-9a-f]{64}")
GROUP_KEYS = ["trading_date", "session_id", "feed_name", "family_root", "minute_utc"]
CONTEXT_MODEL_FEATURES = (
    "reference_price_age_seconds", "reference_price_is_estimate", "minutes_to_cash_close",
    "family_is_spx", "family_is_ndx", "family_is_rut", "family_is_vix", "family_is_spy",
)
OBSERVED_MODEL_FEATURES = (
    "observed_log1p_contract_count", "observed_log1p_trade_count",
    "observed_log1p_volume", "observed_log1p_notional",
    "observed_call_volume_share", "observed_0dte_volume_share",
    "observed_1_7dte_volume_share", "observed_atm_25bps_volume_share",
    "observed_near_100bps_volume_share", "observed_wing_gt100bps_volume_share",
    "observed_nbbo_coverage_ratio", "observed_avg_quoted_spread_bps",
    "observed_data_quality_flag_ratio", "observed_contract_volume_hhi",
    "observed_strike_volume_hhi", "observed_log1p_traded_contract_oi",
    "observed_traded_contract_oi_coverage_ratio",
)
INFERRED_MODEL_FEATURES = (
    "inferred_at_ask_count_share", "inferred_net_at_ask_minus_bid_notional_ratio",
    "inferred_call_net_at_ask_minus_bid_notional_ratio",
    "inferred_put_net_at_ask_minus_bid_notional_ratio",
)
MODEL_FEATURE_COLUMNS = (
    *CONTEXT_MODEL_FEATURES, *OBSERVED_MODEL_FEATURES, *INFERRED_MODEL_FEATURES,
)
MODEL_FEATURE_CONTRACT_HASH = hashlib.sha256(
    json.dumps(
        {"surface_feature_version": SURFACE_FEATURE_VERSION, "features": MODEL_FEATURE_COLUMNS},
        sort_keys=True, separators=(",", ":"),
    ).encode("utf-8")
).hexdigest()


def _schema_hash(columns: list[str]) -> str:
    payload = json.dumps(
        {"version": SURFACE_FEATURE_VERSION, "columns": columns},
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def build_contract_surface_features(contract_rows: pd.DataFrame) -> pd.DataFrame:
    """Collapse point-in-time contract rows into leakage-safe family-minute features.

    Inputs must already carry a reference price joined as-of minute end. Fields
    prefixed ``observed_`` are reproducible measurements; fields prefixed
    ``inferred_`` retain execution-location estimates and never claim holdings.
    """
    required = {
        *GROUP_KEYS, "raw_symbol", "expiration", "option_type", "strike",
        "cash_open_utc", "cash_close_utc",
        "feature_available_at_utc", "reference_price", "reference_price_timestamp_utc",
        "reference_price_age_seconds", "capture_integrity_verified", "trade_count",
        "inference_method", "inference_version", "source_sha256",
        "reference_price_method", "reference_price_is_estimate", "reference_price_provider",
        "reference_price_tier", "reference_subscription_epoch_id",
        "reference_subscription_generation", "reference_price_epoch_eligible",
        "reference_price_epoch_status",
        "volume", "notional", "nbbo_valid_count", "quoted_spread_bps_sum",
        "data_quality_flagged_count", "at_ask_count", "at_bid_count",
        "at_ask_notional", "at_bid_notional", "open_interest",
        "open_interest_available_at_utc",
    }
    missing = sorted(required - set(contract_rows.columns))
    if missing:
        raise ValueError(f"contract surface columns missing: {', '.join(missing)}")
    if contract_rows.empty:
        return pd.DataFrame(columns=[*GROUP_KEYS, "surface_feature_version", "feature_schema_hash"])

    frame = contract_rows.copy()
    if not frame["capture_integrity_verified"].fillna(False).astype(bool).all():
        raise ValueError("contract surface refuses rows without verified capture integrity")
    frame["minute_utc"] = pd.to_datetime(frame["minute_utc"], utc=True)
    frame["feature_available_at_utc"] = pd.to_datetime(frame["feature_available_at_utc"], utc=True)
    frame["reference_price_timestamp_utc"] = pd.to_datetime(
        frame["reference_price_timestamp_utc"], utc=True
    )
    frame["open_interest_available_at_utc"] = pd.to_datetime(
        frame["open_interest_available_at_utc"], utc=True, errors="coerce"
    )
    frame["cash_open_utc"] = pd.to_datetime(frame["cash_open_utc"], utc=True)
    frame["cash_close_utc"] = pd.to_datetime(frame["cash_close_utc"], utc=True)
    frame["expiration"] = pd.to_datetime(frame["expiration"]).dt.normalize()
    trading_day = pd.to_datetime(frame["trading_date"]).dt.normalize()
    frame["dte"] = (frame["expiration"] - trading_day).dt.days
    numeric = [
        "strike", "reference_price", "reference_price_age_seconds", "trade_count",
        "volume", "notional", "nbbo_valid_count", "quoted_spread_bps_sum",
        "data_quality_flagged_count", "at_ask_count", "at_bid_count",
        "at_ask_notional", "at_bid_notional", "open_interest",
    ]
    for column in numeric:
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    invalid_time = (
        (frame["reference_price_timestamp_utc"] > frame["feature_available_at_utc"])
        | (frame["reference_price_age_seconds"] < 0)
        | (frame["feature_available_at_utc"] < frame["cash_open_utc"])
        | (frame["feature_available_at_utc"] > frame["cash_close_utc"])
    )
    invalid_identity = (
        (frame["reference_price"] <= 0) | frame["reference_price"].isna()
        | (frame["strike"] <= 0) | frame["strike"].isna()
        | (frame["dte"] < 0) | ~frame["option_type"].isin(["C", "P"])
    )
    invalid_open_interest = frame["open_interest"].notna() & (
        frame["open_interest_available_at_utc"].isna()
        | (
            frame["open_interest_available_at_utc"]
            > frame["feature_available_at_utc"]
        )
    )
    if (invalid_time | invalid_identity | invalid_open_interest).any():
        raise ValueError("contract surface contains future, stale-direction, or invalid contract inputs")
    epoch_valid = frame["reference_subscription_epoch_id"].map(
        lambda value: isinstance(value, str)
        and SUBSCRIPTION_EPOCH_PATTERN.fullmatch(value) is not None
    )
    generation = pd.to_numeric(
        frame["reference_subscription_generation"], errors="coerce"
    )
    generation_valid = generation.notna() & (generation > 0) & (generation % 1 == 0)
    epoch_eligible = frame["reference_price_epoch_eligible"].map(
        lambda value: isinstance(value, (bool, np.bool_)) and bool(value)
    )
    primary = frame["reference_price_tier"].eq("primary_marketpin_snapshot")
    fallback = frame["reference_price_tier"].eq("tcbbo_parity_fallback_estimate")
    primary_valid = (
        primary & epoch_valid & generation_valid & epoch_eligible
        & frame["reference_price_epoch_status"].eq("eligible_current_epoch")
    )
    fallback_valid = (
        fallback & ~epoch_valid & generation.isna() & ~epoch_eligible
        & frame["reference_price_epoch_status"].eq("not_applicable_tcbbo_fallback")
    )
    if not (primary_valid | fallback_valid).fillna(False).all():
        raise ValueError("contract surface contains invalid reference subscription epoch provenance")
    provenance_counts = frame.groupby(
        GROUP_KEYS, sort=True, observed=True
    )[[
        "inference_method", "inference_version", "source_sha256",
        "reference_price_method", "reference_price_is_estimate", "reference_price_provider",
        "reference_price_tier", "reference_subscription_epoch_id",
        "reference_subscription_generation", "reference_price_epoch_eligible",
        "reference_price_epoch_status",
    ]].nunique(dropna=False)
    if (provenance_counts > 1).any().any():
        raise ValueError("contract surface mixes inference or source provenance within a minute")

    frame["abs_moneyness_bps"] = (
        (frame["strike"] / frame["reference_price"] - 1.0).abs() * 10_000.0
    )
    frame["is_call"] = (frame["option_type"] == "C").astype("int64")
    frame["is_put"] = (frame["option_type"] == "P").astype("int64")
    frame["call_volume"] = frame["volume"] * frame["is_call"]
    frame["put_volume"] = frame["volume"] * frame["is_put"]
    frame["volume_0dte"] = frame["volume"].where(frame["dte"] == 0, 0.0)
    frame["volume_1_7dte"] = frame["volume"].where(frame["dte"].between(1, 7), 0.0)
    frame["volume_gt7dte"] = frame["volume"].where(frame["dte"] > 7, 0.0)
    frame["volume_atm_25bps"] = frame["volume"].where(frame["abs_moneyness_bps"] <= 25, 0.0)
    frame["volume_near_100bps"] = frame["volume"].where(
        frame["abs_moneyness_bps"].between(25, 100, inclusive="right"), 0.0
    )
    frame["volume_wing_gt100bps"] = frame["volume"].where(
        frame["abs_moneyness_bps"] > 100, 0.0
    )
    frame["contract_volume_squared"] = frame["volume"] ** 2
    frame["inferred_call_at_ask_notional"] = frame["at_ask_notional"] * frame["is_call"]
    frame["inferred_call_at_bid_notional"] = frame["at_bid_notional"] * frame["is_call"]
    frame["inferred_put_at_ask_notional"] = frame["at_ask_notional"] * frame["is_put"]
    frame["inferred_put_at_bid_notional"] = frame["at_bid_notional"] * frame["is_put"]
    frame["oi_present"] = frame["open_interest"].notna().astype("int64")
    if "predicted_close" not in frame.columns:
        frame["predicted_close"] = np.nan
    frame["predicted_close"] = pd.to_numeric(frame["predicted_close"], errors="coerce")

    grouped = frame.groupby(GROUP_KEYS, sort=True, observed=True)
    result = grouped.agg(
        feature_available_at_utc=("feature_available_at_utc", "first"),
        cash_open_utc=("cash_open_utc", "first"),
        cash_close_utc=("cash_close_utc", "first"),
        reference_price=("reference_price", "first"),
        reference_price_timestamp_utc=("reference_price_timestamp_utc", "first"),
        reference_price_age_seconds=("reference_price_age_seconds", "max"),
        inference_method=("inference_method", "first"),
        inference_version=("inference_version", "first"),
        source_sha256=("source_sha256", "first"),
        capture_integrity_verified=("capture_integrity_verified", "all"),
        reference_price_method=("reference_price_method", "first"),
        reference_price_is_estimate=("reference_price_is_estimate", "first"),
        reference_price_provider=("reference_price_provider", "first"),
        reference_price_tier=("reference_price_tier", "first"),
        reference_subscription_epoch_id=("reference_subscription_epoch_id", "first"),
        reference_subscription_generation=("reference_subscription_generation", "first"),
        reference_price_epoch_eligible=("reference_price_epoch_eligible", "first"),
        reference_price_epoch_status=("reference_price_epoch_status", "first"),
        predicted_close=("predicted_close", "first"),
        open_interest_available_at_utc=("open_interest_available_at_utc", "max"),
        observed_contract_count=("raw_symbol", "nunique"),
        observed_strike_count=("strike", "nunique"),
        observed_expiration_count=("expiration", "nunique"),
        observed_trade_count=("trade_count", "sum"),
        observed_volume=("volume", "sum"), observed_notional=("notional", "sum"),
        observed_call_volume=("call_volume", "sum"), observed_put_volume=("put_volume", "sum"),
        observed_0dte_volume=("volume_0dte", "sum"),
        observed_1_7dte_volume=("volume_1_7dte", "sum"),
        observed_gt7dte_volume=("volume_gt7dte", "sum"),
        observed_atm_25bps_volume=("volume_atm_25bps", "sum"),
        observed_near_100bps_volume=("volume_near_100bps", "sum"),
        observed_wing_gt100bps_volume=("volume_wing_gt100bps", "sum"),
        observed_nbbo_valid_count=("nbbo_valid_count", "sum"),
        observed_quoted_spread_bps_sum=("quoted_spread_bps_sum", "sum"),
        observed_data_quality_flagged_count=("data_quality_flagged_count", "sum"),
        observed_traded_contract_oi_sum=("open_interest", "sum"),
        observed_traded_contract_oi_coverage_count=("oi_present", "sum"),
        _contract_volume_squared_sum=("contract_volume_squared", "sum"),
        inferred_at_ask_count=("at_ask_count", "sum"),
        inferred_at_bid_count=("at_bid_count", "sum"),
        inferred_at_ask_notional=("at_ask_notional", "sum"),
        inferred_at_bid_notional=("at_bid_notional", "sum"),
        inferred_call_at_ask_notional=("inferred_call_at_ask_notional", "sum"),
        inferred_call_at_bid_notional=("inferred_call_at_bid_notional", "sum"),
        inferred_put_at_ask_notional=("inferred_put_at_ask_notional", "sum"),
        inferred_put_at_bid_notional=("inferred_put_at_bid_notional", "sum"),
    ).reset_index()

    strike_volume = frame.groupby(
        [*GROUP_KEYS, "strike"], sort=True, observed=True
    )["volume"].sum().pow(2).groupby(level=list(range(len(GROUP_KEYS)))).sum()
    strike_hhi = strike_volume.rename("_strike_volume_squared_sum").reset_index()
    result = result.merge(strike_hhi, on=GROUP_KEYS, how="left", validate="one_to_one")
    volume_squared = result["observed_volume"] ** 2
    result["observed_contract_volume_hhi"] = np.where(
        volume_squared > 0, result["_contract_volume_squared_sum"] / volume_squared, np.nan
    )
    result["observed_strike_volume_hhi"] = np.where(
        volume_squared > 0, result["_strike_volume_squared_sum"] / volume_squared, np.nan
    )
    result["observed_traded_contract_oi_coverage_ratio"] = (
        result["observed_traded_contract_oi_coverage_count"]
        / result["observed_contract_count"].replace(0, np.nan)
    )
    result.loc[
        result["observed_traded_contract_oi_coverage_count"] == 0,
        "observed_traded_contract_oi_sum",
    ] = np.nan
    result["inferred_net_at_ask_minus_bid_notional"] = (
        result["inferred_at_ask_notional"] - result["inferred_at_bid_notional"]
    )
    result["minutes_to_cash_close"] = (
        result["cash_close_utc"] - result["feature_available_at_utc"]
    ).dt.total_seconds() / 60.0
    def _ratio(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
        return numerator.div(denominator).where(denominator > 0)

    result["observed_log1p_contract_count"] = np.log1p(result["observed_contract_count"])
    result["observed_log1p_trade_count"] = np.log1p(result["observed_trade_count"])
    result["observed_log1p_volume"] = np.log1p(result["observed_volume"])
    result["observed_log1p_notional"] = np.log1p(result["observed_notional"])
    result["observed_call_volume_share"] = _ratio(
        result["observed_call_volume"], result["observed_volume"]
    )
    for source, target in (
        ("observed_0dte_volume", "observed_0dte_volume_share"),
        ("observed_1_7dte_volume", "observed_1_7dte_volume_share"),
        ("observed_atm_25bps_volume", "observed_atm_25bps_volume_share"),
        ("observed_near_100bps_volume", "observed_near_100bps_volume_share"),
        ("observed_wing_gt100bps_volume", "observed_wing_gt100bps_volume_share"),
    ):
        result[target] = _ratio(result[source], result["observed_volume"])
    result["observed_nbbo_coverage_ratio"] = _ratio(
        result["observed_nbbo_valid_count"], result["observed_trade_count"]
    )
    result["observed_avg_quoted_spread_bps"] = _ratio(
        result["observed_quoted_spread_bps_sum"], result["observed_nbbo_valid_count"]
    )
    result["observed_data_quality_flag_ratio"] = _ratio(
        result["observed_data_quality_flagged_count"], result["observed_trade_count"]
    )
    result["observed_log1p_traded_contract_oi"] = np.log1p(
        result["observed_traded_contract_oi_sum"]
    )
    classified_count = result["inferred_at_ask_count"] + result["inferred_at_bid_count"]
    result["inferred_at_ask_count_share"] = _ratio(
        result["inferred_at_ask_count"], classified_count
    )
    classified_notional = result["inferred_at_ask_notional"] + result["inferred_at_bid_notional"]
    result["inferred_net_at_ask_minus_bid_notional_ratio"] = _ratio(
        result["inferred_net_at_ask_minus_bid_notional"], classified_notional
    )
    for kind in ("call", "put"):
        at_ask = result[f"inferred_{kind}_at_ask_notional"]
        at_bid = result[f"inferred_{kind}_at_bid_notional"]
        result[f"inferred_{kind}_net_at_ask_minus_bid_notional_ratio"] = _ratio(
            at_ask - at_bid, at_ask + at_bid
        )
    for family in ("SPX", "NDX", "RUT", "VIX", "SPY"):
        result[f"family_is_{family.lower()}"] = (result["family_root"] == family).astype("int64")
    result = result.drop(columns=["_contract_volume_squared_sum", "_strike_volume_squared_sum"])
    result["surface_feature_version"] = SURFACE_FEATURE_VERSION
    schema_columns = sorted(column for column in result.columns if column not in GROUP_KEYS)
    result["feature_schema_hash"] = _schema_hash(schema_columns)
    missing_model_features = sorted(set(MODEL_FEATURE_COLUMNS) - set(result.columns))
    if missing_model_features:
        raise RuntimeError(f"surface model feature contract missing: {', '.join(missing_model_features)}")
    return result.sort_values(["minute_utc", "session_id", "family_root"]).reset_index(drop=True)


def select_decision_horizon_features(
    surface: pd.DataFrame,
    *,
    minutes_before_close: int = 15,
) -> pd.DataFrame:
    """Select rows available at exactly the declared close-decision horizon."""
    if minutes_before_close < 0:
        raise ValueError("minutes_before_close must be nonnegative")
    required = {
        "session_id", "family_root", "feature_available_at_utc", "cash_close_utc",
        "minutes_to_cash_close", "surface_feature_version", "feature_schema_hash",
    }
    missing = sorted(required - set(surface.columns))
    if missing:
        raise ValueError(f"decision horizon columns missing: {', '.join(missing)}")
    if surface.empty:
        return surface.copy()
    result = surface.copy()
    result["feature_available_at_utc"] = pd.to_datetime(result["feature_available_at_utc"], utc=True)
    result["cash_close_utc"] = pd.to_datetime(result["cash_close_utc"], utc=True)
    expected = result["cash_close_utc"] - pd.Timedelta(minutes=minutes_before_close)
    result = result[result["feature_available_at_utc"] == expected].copy()
    keys = ["session_id", "family_root"]
    if result.duplicated(keys).any():
        raise ValueError("decision horizon has duplicate session/family rows")
    result["decision_horizon_minutes_before_close"] = minutes_before_close
    return result.sort_values(["feature_available_at_utc", "session_id", "family_root"]).reset_index(drop=True)
