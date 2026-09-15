"""Pure, point-in-time research candidates for cash closes 0..10 sessions out.

Input is an iterable of mappings, never a live client or database connection.
Intraday records require symbol, event timestamp (timestamp_utc or
source_timestamp_utc), spot/reference_price/spot_price, valid=True,
source_verified=True, source, subscription_epoch_id and subscription_generation.
available_at_utc records when a feature became observable; defaulting it to the
event time is only appropriate for already point-in-time producer observations.
OPRA parity references additionally require same_day_profile_available=True
and a primary_expiration/options_expiration_date equal to the source session.
VIX option parity is forward context and cannot supply a cash-close baseline.

Daily records additionally use kind='daily_close', session_date, official_close,
source_sha256 and available_at_utc. Their timestamp is the cash close instant,
not the import time. Prices are official, unadjusted closes for that exact
symbol. Missing sessions are never collapsed into shorter horizons.

All outputs remain research-only. No probability, validated interval, model
promotion, forecast passport, or production override is issued here. Numeric
candidates are pre-registered comparisons until held-out outcomes prove skill.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from numbers import Integral
from statistics import fmean
from typing import Any, Iterable, Mapping

from backend.forecast_calendar import ET, resolve_target_session, session_bounds, shift_session

SCHEMA_VERSION = "marketpin-forecast-horizons.v1"
MAX_SPOT_AGE_SECONDS = 120.0
MIN_DAILY_RETURN_SAMPLES = 60
MAX_DAILY_OBSERVATIONS = 300
MIN_WALK_FORWARD_TRAIN = 40
# Unadjusted ETF split artifacts must not silently become forecast drift.
# This is a research quality gate, not a bound on possible market moves.
MAX_DAILY_ABS_RETURN = 0.30
UTC = timezone.utc
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_DISALLOWED_PROVENANCE = ("fallback", "proxy", "delayed", "synthetic", "yahoo", "yfinance")


def _utc(value: Any) -> datetime | None:
    if isinstance(value, str):
        try:
            value = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
    if not isinstance(value, datetime) or value.tzinfo is None:
        return None
    return value.astimezone(UTC)


def _iso(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _positive(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    try:
        value = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return value if math.isfinite(value) and value > 0 else None


def _first(record: Mapping, *keys: str) -> Any:
    return next((record[key] for key in keys if record.get(key) is not None), None)


def _true(value: Any) -> bool:
    # Accept SQLite's exact integer boolean, but not truthy strings.
    return value is True or type(value) is int and value == 1


def _mean(values: Iterable[float]) -> float:
    """Scale the sum so finite observations cannot overflow their mean."""
    items = list(values)
    scale = max(abs(value) for value in items)
    return scale * fmean(value / scale for value in items) if scale else 0.0


@dataclass(frozen=True)
class _Observation:
    timestamp: datetime
    available: datetime
    price: float
    source: str
    epoch: str | None
    generation: int | None
    day: date
    daily: bool
    source_sha256: str | None


def _normalize(record: Mapping, symbol: str, as_of: datetime) -> tuple[_Observation | None, str | None]:
    if str(record.get("symbol", "")).upper() != symbol:
        return None, "symbol_mismatch"
    daily = record.get("kind") == "daily_close"
    source = record.get("source", record.get("data_source"))
    provenance = str(source).lower() + " " + str(record.get("provenance", "")).lower()
    if not isinstance(source, str) or not source.strip() or not _true(record.get("source_verified")):
        return None, "unverified_source"
    if any(word in provenance for word in _DISALLOWED_PROVENANCE) or any(
        _true(record.get(key)) for key in ("fallback", "is_fallback", "fallback_provenance", "is_proxy", "is_delayed")
    ):
        return None, "non_direct_provenance"
    valid = _first(record, "valid", "validation_is_valid", "is_valid")
    if (not daily or valid is not None) and not _true(valid):
        return None, "invalid_observation"
    timestamp = _utc(_first(record, "timestamp_utc", "source_timestamp_utc", "timestamp"))
    available = _utc(record.get("available_at_utc", timestamp))
    if timestamp is None or available is None:
        return None, "timestamp_missing_or_naive"
    if available < timestamp:
        return None, "availability_before_event"
    if timestamp > as_of or available > as_of:
        return None, "future_observation"
    price = _positive(record.get("official_close") if daily else _first(record, "spot", "reference_price", "spot_price", "price"))
    if price is None:
        return None, "invalid_price"
    day = timestamp.astimezone(ET).date()
    try:
        opened, closed = session_bounds(day)
    except ValueError:
        return None, "outside_reviewed_session"
    if not opened <= timestamp <= closed:
        return None, "outside_regular_hours"
    if daily:
        source_hash = record.get("source_sha256")
        if record.get("session_date") != day.isoformat() or timestamp != closed:
            return None, "daily_close_session_mismatch"
        if "available_at_utc" not in record or not isinstance(source_hash, str) or not _SHA256.fullmatch(source_hash):
            return None, "daily_close_provenance_incomplete"
        if record.get("price_basis", "unadjusted") != "unadjusted":
            return None, "daily_close_price_basis_unsupported"
        return _Observation(timestamp, available, price, source, None, None, day, True, source_hash), None
    parity_source = "parity" in (source + " " + str(record.get("spot_source", ""))).lower()
    if parity_source:
        if symbol == "VIX":
            return None, "vix_parity_is_forward_context"
        expirations = [record[key] for key in ("primary_expiration", "options_expiration_date") if record.get(key) is not None]
        if not _true(record.get("same_day_profile_available")) or not expirations or any(str(expiry) != day.isoformat() for expiry in expirations):
            return None, "parity_same_session_profile_required"
    epoch = record.get("subscription_epoch_id")
    generation = record.get("subscription_generation")
    if not isinstance(epoch, str) or not _SHA256.fullmatch(epoch) or isinstance(generation, bool) or not isinstance(generation, Integral) or generation <= 0:
        return None, "subscription_identity_missing"
    return _Observation(timestamp, available, price, source, epoch, int(generation), day, False, None), None


def _candidate(method_id: str, value: float | None, reason: str | None = None, **details: Any) -> dict:
    supplied = value
    value = _positive(value)
    if supplied is not None and value is None and reason is None:
        reason = "nonfinite_or_nonpositive_candidate"
    return {
        "method_id": method_id,
        "status": "RESEARCH_ONLY" if value is not None else "ABSTAIN",
        "predicted_close": value,
        "reasons": [reason] if reason else [],
        "decision_grade": False,
        **details,
    }


def _forecast_digest(result: Mapping) -> str:
    identity = json.dumps({key: result.get(key) for key in ("schema_version", "symbol", "as_of_utc", "horizon_sessions", "target_close_utc", "evidence", "candidates")}, sort_keys=True, allow_nan=False)
    return hashlib.sha256(identity.encode("utf-8")).hexdigest()


def _history_returns(daily: list[_Observation], horizon: int) -> list[dict]:
    by_day = {row.day: row for row in daily}
    samples = []
    for origin in daily:
        # Every intervening session must exist. Do not turn two separated rows
        # into an h-day return by their adjacent array positions.
        try:
            days = [shift_session(origin.day, offset) for offset in range(1, horizon + 1)]
        except ValueError:
            continue
        if not all(day in by_day for day in days):
            continue
        end = by_day[days[-1]]
        change = end.price / origin.price - 1.0
        if not math.isfinite(change):
            continue
        samples.append({
            "start": origin,
            "end": end,
            "available": max(by_day[day].available for day in [origin.day, *days]),
            "return": change,
        })
    return samples


def _metrics(predictions: list[float], actuals: list[float], baselines: list[float]) -> dict:
    errors = [pred - actual for pred, actual in zip(predictions, actuals)]
    baseline_errors = [base - actual for base, actual in zip(baselines, actuals)]
    mae = _mean(abs(value) for value in errors)
    base_mae = _mean(abs(value) for value in baseline_errors)
    scale = max(abs(value) for value in errors)
    rmse = scale * math.sqrt(fmean((value / scale) ** 2 for value in errors)) if scale else 0.0
    skill = 1.0 - mae / base_mae if base_mae > 0 else None
    return {
        "n": len(errors),
        "mae": mae,
        "rmse": rmse,
        "mean_error": _mean(errors),
        "baseline_mae": base_mae,
        "mae_skill_vs_last_price": skill if skill is None or math.isfinite(skill) else None,
        "units": "symbol_price_points",
    }


def _walk_forward_daily(samples: list[dict]) -> dict:
    predictions, actuals, baselines, folds = [], [], [], []
    last_test_end = None
    for test in samples:
        origin = test["start"]
        # Backfilled official closes are usable now, but cannot create a
        # historical forecast at a date before those closes became available.
        if origin.available >= test["end"].timestamp:
            continue
        # Disjoint test horizons and a strict maturity boundary for every
        # training window. Overlapping labels cannot leak across this boundary.
        if last_test_end is not None and origin.timestamp <= last_test_end:
            continue
        training = [sample for sample in samples if sample["end"].timestamp < origin.timestamp and sample["available"] <= origin.available]
        if len(training) < MIN_WALK_FORWARD_TRAIN:
            continue
        train = training[-120:]
        predicted = origin.price * (1.0 + 0.5 * _mean(row["return"] for row in train))
        if _positive(predicted) is None:
            continue
        predictions.append(predicted)
        actuals.append(test["end"].price)
        baselines.append(origin.price)
        last_test_end = test["end"].timestamp
        folds.append({
            "origin_session": origin.day.isoformat(),
            "target_session": test["end"].day.isoformat(),
            "train_samples": len(train),
            "train_labels_end_utc": _iso(max(row["end"].timestamp for row in train)),
            "train_available_at_utc": _iso(max(row["available"] for row in train)),
            "origin_available_at_utc": _iso(origin.available),
        })
    return {
        "status": "EVALUATED_RESEARCH" if predictions else "INSUFFICIENT_MATURED_HISTORY",
        "method_id": "daily_shrunk_return_v1",
        "split": "chronological_expanding_train_purged_disjoint_test_horizons",
        "metrics": _metrics(predictions, actuals, baselines) if predictions else None,
        "folds": folds,
        "production_promotion_allowed": False,
        "limitation": "Daily close origins; intraday-to-close residual remains unvalidated.",
    }


def evaluate_forecast(
    symbol: str,
    history: Iterable[Mapping],
    *,
    as_of_utc: datetime | str,
    horizon_sessions: int = 0,
    current_snapshot: Mapping | None = None,
) -> dict:
    """Compute bounded research candidates without modifying any input or state."""
    as_of = _utc(as_of_utc)
    if as_of is None:
        raise ValueError("as_of_utc_must_be_timezone_aware")
    symbol = str(symbol).strip().upper()
    if not symbol:
        raise ValueError("symbol_required")
    # Validate horizon before constructing an abstention response.
    if isinstance(horizon_sessions, bool) or not isinstance(horizon_sessions, Integral) or not 0 <= horizon_sessions <= 10:
        raise ValueError("horizon_sessions_must_be_0_to_10")
    result = {
        "schema_version": SCHEMA_VERSION,
        "symbol": symbol,
        "as_of_utc": _iso(as_of),
        "horizon_sessions": int(horizon_sessions),
        "status": "ABSTAIN",
        "research_only": True,
        "decision_grade": False,
        "production_promotion_allowed": False,
        "predicted_close": None,
        "selected_method": None,
        "candidates": [],
        "reasons": [],
        "evidence": {},
        "evaluation": None,
    }
    try:
        result.update(resolve_target_session(as_of, horizon_sessions))
    except ValueError as exc:
        result["reasons"] = [str(exc)]
        return result
    opened, closed = _utc(result["session_open_utc"]), _utc(result["session_close_utc"])
    if not opened <= as_of < closed:
        result["reasons"] = ["outside_regular_hours"]
        return result
    excluded: Counter = Counter()
    intraday, daily = [], []
    rejected_at = []
    for record in history:
        if not isinstance(record, Mapping):
            excluded["malformed_record"] += 1
            continue
        row, reason = _normalize(record, symbol, as_of)
        if row is None:
            excluded[reason] += 1
            event = _utc(_first(record, "timestamp_utc", "source_timestamp_utc", "timestamp"))
            available = _utc(record.get("available_at_utc", event))
            if (str(record.get("symbol", "")).upper() == symbol and record.get("kind") != "daily_close"
                    and event is not None and available is not None
                    and event <= available <= as_of):
                rejected_at.append(event)
        else:
            (daily if row.daily else intraday).append(row)
    snapshot_row = None
    if current_snapshot is not None:
        snapshot_row, reason = _normalize(current_snapshot, symbol, as_of)
        if snapshot_row is None or snapshot_row.daily:
            result["reasons"] = ["current_snapshot_invalid", reason or "daily_snapshot_not_live"]
            result["evidence"] = {"excluded_rows": dict(excluded)}
            return result
        intraday.append(snapshot_row)
    today = as_of.astimezone(ET).date()
    intraday = sorted((row for row in intraday if row.day == today), key=lambda row: (row.timestamp, row.available))
    if not intraday:
        result["reasons"] = ["no_verified_current_session_spot"]
        result["evidence"] = {"excluded_rows": dict(excluded), "daily_close_count": len(daily)}
        return result
    latest = snapshot_row or intraday[-1]
    if snapshot_row is None and any(event >= latest.timestamp for event in rejected_at):
        result["reasons"] = ["latest_observation_invalid"]
        result["evidence"] = {"excluded_rows": dict(excluded)}
        return result
    if (as_of - latest.timestamp).total_seconds() > MAX_SPOT_AGE_SECONDS:
        result["reasons"] = ["stale_current_spot"]
        result["evidence"] = {"source_timestamp_utc": _iso(latest.timestamp), "excluded_rows": dict(excluded)}
        return result
    # Equal-timestamp disagreement is ambiguous, not a choice of whichever row
    # happened to be loaded last. Generations may restart only with a new epoch.
    aligned = [row for row in intraday if (row.epoch, row.generation) == (latest.epoch, latest.generation) and row.timestamp <= latest.timestamp]
    by_time: dict[datetime, _Observation] = {}
    for row in aligned:
        if row.timestamp in by_time and by_time[row.timestamp].price != row.price:
            result["reasons"] = ["conflicting_spot_at_same_timestamp"]
            return result
        by_time[row.timestamp] = row
    aligned = sorted(by_time.values(), key=lambda row: row.timestamp)
    result["evidence"] = {
        "source_timestamp_utc": _iso(latest.timestamp),
        "available_at_utc": _iso(latest.available),
        "source": latest.source,
        "spot": latest.price,
        "subscription_epoch_id": latest.epoch,
        "subscription_generation": latest.generation,
        "current_generation_observations": len(aligned),
        "excluded_other_generation_rows": len(intraday) - len([row for row in intraday if (row.epoch, row.generation) == (latest.epoch, latest.generation)]),
        "excluded_rows": dict(excluded),
    }
    input_identity = [
        (_iso(row.timestamp), _iso(row.available), row.price, row.source, row.epoch,
         row.generation, row.daily, row.source_sha256)
        for row in sorted([*aligned, *daily], key=lambda row: (row.timestamp, row.available, row.price, row.source))
    ]
    result["evidence"]["input_observations_sha256"] = hashlib.sha256(
        json.dumps(input_identity, separators=(",", ":"), allow_nan=False).encode("utf-8")
    ).hexdigest()
    baseline = _candidate("last_price_v1", latest.price, equation="target_close = current_spot")
    result["candidates"].append(baseline)
    if horizon_sessions == 0:
        _intraday_candidates(result, aligned, latest, as_of, closed, current_snapshot)
    else:
        by_day: dict[date, _Observation] = {}
        conflict = False
        for row in daily:
            if row.day in by_day and by_day[row.day].price != row.price:
                conflict = True
            by_day[row.day] = row
        if conflict:
            result["reasons"] = ["conflicting_official_close"]
            return result
        daily = sorted(by_day.values(), key=lambda row: row.timestamp)[-MAX_DAILY_OBSERVATIONS:]
        samples = _history_returns(daily, int(horizon_sessions))
        result["evidence"].update({"daily_close_count": len(daily), "horizon_return_samples": len(samples), "minimum_return_samples": MIN_DAILY_RETURN_SAMPLES})
        try:
            previous_session = shift_session(today, -1)
        except ValueError:
            result["reasons"] = ["daily_history_calendar_unsupported"]
            return result
        if not daily or daily[-1].day != previous_session:
            reason = "daily_close_history_not_current"
        elif len(samples) < MIN_DAILY_RETURN_SAMPLES:
            reason = "insufficient_verified_daily_history"
        else:
            reason = None
        if reason:
            result["candidates"].append(_candidate("daily_shrunk_return_v1", None, reason))
            result["reasons"] = [reason]
            return result
        for previous, following in zip(daily, daily[1:]):
            if (shift_session(previous.day, 1) == following.day
                    and abs(following.price / previous.price - 1.0) > MAX_DAILY_ABS_RETURN):
                result["reasons"] = ["daily_close_discontinuity_requires_review"]
                result["candidates"].append(_candidate("daily_shrunk_return_v1", None, result["reasons"][0]))
                return result
        train = samples[-120:]
        candidate = _candidate(
            "daily_shrunk_return_v1",
            latest.price * (1.0 + 0.5 * _mean(row["return"] for row in train)),
            equation="current_spot * (1 + 0.5 * mean(verified_h_session_close_returns[-120:]))",
            training_samples=len(train),
            assumption="Zero expected return for the remainder of today's cash session; future h-session drift is shrunk 50% toward the no-change baseline.",
        )
        result["candidates"].append(candidate)
        result["selected_method"] = candidate["method_id"]
        result["predicted_close"] = candidate["predicted_close"]
        result["evaluation"] = _walk_forward_daily(samples)
        if candidate["predicted_close"] is None:
            result["reasons"] = ["nonfinite_or_nonpositive_candidate"]
    if result["predicted_close"] is not None:
        result["status"] = "RESEARCH_ONLY"
        result["reasons"] = ["unpromoted_research_candidate", "no_calibrated_prediction_interval"]
    result["forecast_id"] = _forecast_digest(result)
    return result


def _intraday_candidates(result: dict, rows: list[_Observation], latest: _Observation, as_of: datetime, closed: datetime, snapshot: Mapping | None) -> None:
    recent = [row for row in rows if latest.timestamp - timedelta(minutes=30) <= row.timestamp <= latest.timestamp]
    lookback = (recent[-1].timestamp - recent[0].timestamp).total_seconds() if len(recent) >= 2 else None
    enough = len(recent) >= 6 and lookback >= 15 * 60
    continuous = enough and all((b.timestamp - a.timestamp).total_seconds() <= 180 for a, b in zip(recent, recent[1:]))
    reason = None if continuous else "insufficient_contiguous_intraday_history"
    momentum, reversion = None, None
    if continuous:
        remaining = (closed - as_of).total_seconds()
        decay = min(1.0, remaining / lookback)
        change = max(-0.02, min(0.02, latest.price / recent[0].price - 1.0))
        momentum = latest.price * (1.0 + 0.5 * decay * change)
        # Equal-observation mean is labeled exactly; volume is not fabricated.
        reversion = latest.price + 0.25 * decay * (_mean(row.price for row in recent) - latest.price)
    result["candidates"].extend([
        _candidate("eod_damped_momentum_v1", momentum, reason, lookback_seconds=lookback,
                   lookback_definition="actual observed 15-30 minute window",
                   equation="spot*(1 + 0.5*min(1,remaining/lookback)*clip(return_over_lookback,-0.02,0.02))"),
        _candidate("eod_sample_mean_reversion_v1", reversion, reason, lookback_seconds=lookback,
                   lookback_definition="actual observed 15-30 minute window",
                   equation="spot + 0.25*min(1,remaining/lookback)*(mean(sampled_spot_over_lookback)-spot)"),
    ])
    pin = None
    pin_reason = "same_session_valid_options_context_unavailable"
    options_timestamp = _utc(snapshot.get("options_source_timestamp_utc")) if snapshot else None
    options_available = _utc(snapshot.get("options_available_at_utc")) if snapshot else None
    options_fresh = (
        options_timestamp is not None and options_available is not None
        and options_timestamp <= options_available <= as_of
        and 0 <= (as_of - options_timestamp).total_seconds() <= MAX_SPOT_AGE_SECONDS
    )
    if snapshot and options_fresh and snapshot.get("options_expiration_date") == result["target_session_date"] and _true(snapshot.get("options_context_valid")):
        pin_value = _positive(snapshot.get("gamma_pin"))
        net_gex = _positive(snapshot.get("net_gex"))
        if pin_value is not None and net_gex is not None and abs(pin_value / latest.price - 1) <= 0.03:
            remaining = (closed - as_of).total_seconds()
            pin = latest.price + 0.25 * min(1.0, remaining / 1800.0) * (pin_value - latest.price)
            pin_reason = None
    result["candidates"].append(_candidate("eod_positive_gex_pin_context_v1", pin, pin_reason, equation="spot + 0.25*min(1,remaining/1800)*(same_session_gamma_pin-spot); positive net GEX only"))
    # A deliberately fixed equal blend is reproducible. It is never represented
    # as a learned optimum or inserted into the production 0DTE formula.
    eligible = [row for row in result["candidates"] if row["predicted_close"] is not None]
    blend = _candidate("eod_equal_candidate_blend_v1", _mean(row["predicted_close"] for row in eligible), components=[row["method_id"] for row in eligible], equation="equal mean of available pre-registered candidates")
    if len(eligible) > 1:
        result["candidates"].append(blend)
        result["selected_method"] = blend["method_id"]
        result["predicted_close"] = blend["predicted_close"]
    else:
        result["selected_method"] = "last_price_v1"
        result["predicted_close"] = latest.price
    result["evaluation"] = {"status": "AWAITING_MATURED_OUTCOMES", "production_promotion_allowed": False}


def evaluate_methods(forecasts: Iterable[Mapping], outcomes: Iterable[Mapping], *, as_of_utc: datetime | str) -> dict:
    """Score frozen candidates against verified closes, separately per symbol/h.

    Outcomes use the daily-close contract. Forecasts must have been generated
    before their target close; feature event times cannot exceed their origin.
    Per method, retain the earliest eligible origin, then purge overlapping
    target windows. This is retrospective scoring of fixed methods, not fitting
    or a promotion receipt. Exported forecasts/outcomes remain caller-owned.
    """
    as_of = _utc(as_of_utc)
    if as_of is None:
        raise ValueError("as_of_utc_must_be_timezone_aware")
    excluded: Counter = Counter()
    labels: dict[tuple[str, str], _Observation] = {}
    conflicts = set()
    for outcome in outcomes:
        symbol = str(outcome.get("symbol", "")).upper()
        row, reason = _normalize(outcome, symbol, as_of)
        if row is None or not row.daily:
            excluded[reason or "not_official_close"] += 1
            continue
        key = (symbol, row.day.isoformat())
        if key in labels and labels[key].price != row.price:
            conflicts.add(key)
        labels[key] = row
    groups = defaultdict(list)
    for forecast in forecasts:
        if forecast.get("schema_version") != SCHEMA_VERSION or forecast.get("status") != "RESEARCH_ONLY":
            excluded["unissued_forecast"] += 1
            continue
        try:
            intact = forecast.get("forecast_id") == _forecast_digest(forecast)
        except (ValueError, TypeError, OverflowError):
            intact = False
        if not intact:
            excluded["forecast_payload_changed"] += 1
            continue
        origin = _utc(forecast.get("as_of_utc"))
        target = _utc(forecast.get("target_close_utc"))
        evidence = forecast.get("evidence") or {}
        feature_time = _utc(evidence.get("source_timestamp_utc"))
        feature_available = _utc(evidence.get("available_at_utc"))
        baseline = _positive(evidence.get("spot"))
        symbol = str(forecast.get("symbol", "")).upper()
        horizon = forecast.get("horizon_sessions")
        if origin is None or target is None or feature_time is None or feature_available is None or baseline is None or not feature_time <= feature_available <= origin < target <= as_of:
            excluded["immature_or_noncausal_forecast"] += 1
            continue
        try:
            expected = resolve_target_session(origin, horizon)
        except (ValueError, TypeError):
            excluded["invalid_forecast_horizon"] += 1
            continue
        if expected["target_close_utc"] != _iso(target) or expected["target_session_date"] != forecast.get("target_session_date"):
            excluded["forecast_target_calendar_mismatch"] += 1
            continue
        key = (symbol, expected["target_session_date"])
        if key in conflicts or key not in labels:
            excluded["missing_or_conflicting_verified_outcome"] += 1
            continue
        for candidate in forecast.get("candidates", []):
            predicted = _positive(candidate.get("predicted_close"))
            method = candidate.get("method_id")
            if predicted is None or not isinstance(method, str) or candidate.get("status") != "RESEARCH_ONLY":
                continue
            groups[(symbol, int(horizon), method)].append((origin, target, predicted, labels[key].price, baseline))
    reports = []
    for (symbol, horizon, method), rows in sorted(groups.items()):
        selected, end = [], None
        by_origin = defaultdict(list)
        for row in rows:
            by_origin[(row[0], row[1])].append(row)
        for _, origin_rows in sorted(by_origin.items()):
            if len({row[2:] for row in origin_rows}) > 1:
                excluded["conflicting_forecast_at_same_origin"] += 1
                continue
            row = origin_rows[0]
            if end is not None and row[0] <= end:
                excluded["overlapping_test_horizon_purged"] += 1
                continue
            selected.append(row)
            end = row[1]
        if not selected:
            continue
        reports.append({"symbol": symbol, "horizon_sessions": horizon, "method_id": method, **_metrics([row[2] for row in selected], [row[3] for row in selected], [row[4] for row in selected])})
    return {
        "schema_version": "marketpin-forecast-method-evaluation.v1",
        "as_of_utc": _iso(as_of),
        "status": "EVALUATED_RESEARCH" if reports else "AWAITING_MATURED_OUTCOMES",
        "split": "chronological_disjoint_test_horizons_fixed_methods",
        "reports": reports,
        "excluded": dict(excluded),
        "production_promotion_allowed": False,
    }
