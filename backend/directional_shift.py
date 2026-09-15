"""Pure, point-in-time research diagnostics for an intraday direction change.

This is a descriptive detector, not a calibrated forecast or trading signal.
No state, database, model, subscriptions, or production prediction is changed.
History may be retained ``market_structure`` rows or normalized observations.
Required: source timestamp, positive price, explicit validity, provider/source,
subscription epoch/generation and universe identity. OPRA parity additionally
requires same-day expiry. Optional ``active_generation`` binds the latest row
to a live runtime. A caller displaying live status must supply that binding.

Explicit ``*_utc`` fields from SQLite may be naive UTC; generic ``timestamp``
must carry an offset. ``captured_at_utc``/``available_at_utc`` are honored for
as-of replay. ``volume`` is used only with ``volume_kind='interval_traded'``
and ``volume_price`` (the true volume-weighted price of that same interval).
Option volume, OI, cumulative volume and quote counts are never spot VWAP.
"""

from __future__ import annotations

import math
import statistics
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any, Mapping, Sequence
from zoneinfo import ZoneInfo

from backend.forecast_calendar import session_bounds

EASTERN = ZoneInfo("America/New_York")
SCHEMA_VERSION = "marketpin-directional-shift.v1"
FRESHNESS_SECONDS = 90.0
MAX_GAP_SECONDS = 120.0
BASELINE_SECONDS = 600.0
RECENT_SECONDS = 300.0
FAMILIES = {
    "SPX": "sp500", "SPY": "sp500", "IVV": "sp500", "VOO": "sp500",
    "NDX": "nasdaq100", "QQQ": "nasdaq100", "QQQM": "nasdaq100",
    "DJI": "dow30", "DIJ": "dow30", "DJX": "dow30", "DIA": "dow30",
    "RUT": "russell2000", "IWM": "russell2000",
}


def _number(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    try:
        result = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return result if math.isfinite(result) else None


def _utc(value: Any, *, explicit_utc: bool = False) -> datetime | None:
    try:
        parsed = value if isinstance(value, datetime) else datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            if not explicit_utc:
                return None
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    except (TypeError, ValueError, OverflowError):
        return None


def _timestamp(row: Mapping[str, Any]) -> datetime | None:
    for key in ("source_timestamp_utc", "latest_ts_recv_utc", "timestamp_utc", "as_of_utc", "timestamp"):
        if row.get(key) is not None:
            return _utc(row[key], explicit_utc=key.endswith("_utc"))
    return None


def _identity(row: Mapping[str, Any]) -> tuple[Any, ...]:
    generation = _number(row.get("subscription_generation", row.get("generation")))
    if generation is None or generation < 1 or not generation.is_integer():
        generation = None
    return (
        str(row.get("provider") or "").lower(),
        str(row.get("subscription_epoch_id") or ""),
        generation,
        str(row.get("universe_sha256") or ""),
        str(row.get("spot_source") or row.get("source") or "").lower(),
        str(row.get("primary_expiration") or ""),
    )


def _row_error(row: Mapping[str, Any], symbol: str, timestamp: datetime, now: datetime) -> str | None:
    if row.get("symbol") and str(row["symbol"]).upper().strip() != symbol:
        return "SYMBOL_MISMATCH"
    if timestamp.date() != now.date() or timestamp.astimezone(EASTERN).date() != now.astimezone(EASTERN).date():
        return "PRIOR_SESSION"
    if timestamp > now:
        return "FUTURE_OBSERVATION"
    for key in ("captured_at_utc", "available_at_utc"):
        if row.get(key) is not None:
            available = _utc(row[key], explicit_utc=True)
            if available is None or available > now or available < timestamp:
                return "OBSERVATION_NOT_AVAILABLE_AS_OF"
    try:
        opened, closed = session_bounds(timestamp.astimezone(EASTERN).date())
    except ValueError:
        return "SESSION_CALENDAR_UNAVAILABLE"
    if not opened <= timestamp <= closed:
        return "OUTSIDE_REGULAR_SESSION"
    validity = [row[key] for key in ("valid", "is_valid", "validation_is_valid") if key in row]
    if any(value is not True for value in validity) or row.get("validation_status", "valid") != "valid":
        return "OBSERVATION_INVALID"
    if not validity and row.get("validation_status") != "valid":
        return "VALIDITY_UNPROVEN"
    if row.get("source_verified") is False:
        return "SOURCE_NOT_VERIFIED"
    for key in ("is_fallback", "historical_context_only", "is_proxy", "is_delayed", "universe_is_fallback"):
        if row.get(key):
            return "NON_CURRENT_PROVENANCE"
    for key in ("universe_provenance", "oi_analytics_provenance"):
        nested = row.get(key)
        if isinstance(nested, Mapping) and nested.get("is_fallback"):
            return "NON_CURRENT_PROVENANCE"
    identity = _identity(row)
    if not all(identity[:5]):
        return "PROVENANCE_INCOMPLETE"
    if any(token in identity[0] + " " + identity[4] for token in ("fallback", "historical", "delayed", "proxy", "synthetic")):
        return "NON_CURRENT_PROVENANCE"
    active_generation = row.get("active_generation")
    if active_generation is not None and _number(active_generation) != identity[2]:
        return "ACTIVE_GENERATION_MISMATCH"
    if row.get("active_subscription_epoch_id") is not None and row["active_subscription_epoch_id"] != identity[1]:
        return "ACTIVE_EPOCH_MISMATCH"
    if row.get("handoff_status", "active") != "active":
        return "HANDOFF_NOT_ACTIVE"
    if "parity" in identity[4] and (
        symbol == "VIX" or row.get("same_day_profile_available") is not True
        or identity[5] != now.astimezone(EASTERN).date().isoformat()
    ):
        return "OPTION_FORWARD_CONTEXT_ONLY"
    if row.get("directional_base_eligible") is False:
        return "DIRECTIONAL_BASE_INELIGIBLE"
    return None


def _prepare(symbol: str, history: Sequence[Mapping[str, Any]], now: datetime) -> tuple[list[dict[str, Any]], dict[str, Any], list[str]]:
    rejected: Counter[str] = Counter()
    available = []
    for row in history:
        if not isinstance(row, Mapping):
            rejected["OBSERVATION_INVALID"] += 1
            continue
        timestamp = _timestamp(row)
        if timestamp is None:
            rejected["TIMESTAMP_INVALID"] += 1
            continue
        if timestamp > now:
            rejected["FUTURE_OBSERVATION"] += 1
            continue
        if any(
            (_utc(row[key], explicit_utc=True) or now) > now
            for key in ("captured_at_utc", "available_at_utc") if row.get(key) is not None
        ):
            rejected["OBSERVATION_NOT_AVAILABLE_AS_OF"] += 1
            continue
        available.append((timestamp, row))
    available.sort(key=lambda pair: pair[0])
    coverage: dict[str, Any] = {"input_observations": len(history), "rejected": {}, "eligible_observations": 0}
    if not available:
        coverage["rejected"] = dict(rejected)
        return [], coverage, ["NO_AVAILABLE_HISTORY"]
    latest_timestamp, latest = available[-1]
    latest_error = _row_error(latest, symbol, latest_timestamp, now)
    if latest_error:
        coverage["rejected"] = dict(rejected)
        return [], coverage, [latest_error]
    identity = _identity(latest)
    coverage.update({
        "latest_source_timestamp_utc": latest_timestamp.isoformat(),
        "age_seconds": (now - latest_timestamp).total_seconds(),
        "subscription_generation": identity[2],
        "subscription_epoch_id": identity[1],
        "live_generation_bound": latest.get("active_generation") is not None,
    })
    if coverage["age_seconds"] > FRESHNESS_SECONDS:
        return [], coverage, ["LATEST_OBSERVATION_STALE"]
    # Requiring a continuous suffix prevents confirmation across an invalid
    # observation or reconnect. Sorting alone must never bridge a data outage.
    cleaned: list[dict[str, Any]] = []
    seen_ids: dict[str, datetime] = {}
    seen_times: dict[datetime, dict[str, Any]] = {}
    conflict = False
    for timestamp, row in available:
        error = _row_error(row, symbol, timestamp, now)
        if not error and _identity(row) != identity:
            error = "PROVENANCE_CHANGED"
        price = _number(row.get("reference_price", row.get("price", row.get("underlying_price"))))
        if price is None or price <= 0:
            error = error or "PRICE_INVALID"
        if error:
            rejected[error] += 1
            cleaned, seen_ids, seen_times = [], {}, {}
            continue
        sample_id = str(row.get("observation_id") or row.get("sample_id") or row.get("source_revision") or row.get("calculation_id") or "")
        if sample_id and sample_id in seen_ids and seen_ids[sample_id] != timestamp:
            rejected["REUSED_OBSERVATION_ID"] += 1
            continue
        point = {"time": timestamp, "price": price, "row": row}
        if timestamp in seen_times:
            if price != seen_times[timestamp]["price"]:
                conflict = True
                rejected["CONFLICTING_DUPLICATE_TIMESTAMP"] += 1
            else:
                rejected["DUPLICATE_OBSERVATION"] += 1
            continue
        if sample_id:
            seen_ids[sample_id] = timestamp
        seen_times[timestamp] = point
        cleaned.append(point)
    coverage["rejected"] = dict(rejected)
    coverage["eligible_observations"] = len(cleaned)
    if conflict:
        return [], coverage, ["CONFLICTING_DUPLICATE_TIMESTAMP"]
    if not cleaned:
        return [], coverage, ["NO_ELIGIBLE_HISTORY"]
    if (now - cleaned[-1]["time"]).total_seconds() > FRESHNESS_SECONDS:
        return [], coverage, ["LATEST_DISTINCT_OBSERVATION_STALE"]
    return cleaned, coverage, []


def _window(points: list[dict[str, Any]], start: datetime, end: datetime, *, min_span: float, floor_bps: float = 5.0) -> dict[str, Any]:
    selected = [point for point in points if start <= point["time"] <= end]
    # Provider events rarely land exactly on a rolling boundary. Include the
    # latest real observation at/before the start when it is within the same
    # continuity limit. This preserves >=5 minutes of *observed* elapsed time
    # without interpolating a boundary price or relaxing confirmation duration.
    anchor = next((point for point in reversed(points) if point["time"] <= start), None)
    anchor_age = (start - anchor["time"]).total_seconds() if anchor else None
    if anchor is not None and 0 < anchor_age <= MAX_GAP_SECONDS:
        selected.insert(0, anchor)
    else:
        anchor_age = 0.0 if anchor is not None and anchor["time"] == start else None
    result: dict[str, Any] = {
        "eligible": False, "direction": "FLAT", "observations": len(selected),
        "start_utc": start.isoformat(), "end_utc": end.isoformat(),
        "observed_start_utc": selected[0]["time"].isoformat() if selected else None,
        "observed_end_utc": selected[-1]["time"].isoformat() if selected else None,
        "boundary_anchor_age_seconds": anchor_age,
    }
    if len(selected) < 3:
        result["reason"] = "INSUFFICIENT_DISTINCT_OBSERVATIONS"
        return result
    span = (selected[-1]["time"] - selected[0]["time"]).total_seconds()
    gaps = [(b["time"] - a["time"]).total_seconds() for a, b in zip(selected, selected[1:])]
    result.update({"span_seconds": span, "maximum_gap_seconds": max(gaps)})
    if span < min_span or max(gaps) > MAX_GAP_SECONDS:
        result["reason"] = "INSUFFICIENT_CONTINUOUS_HISTORY"
        return result
    log_returns = [math.log(b["price"]) - math.log(a["price"]) for a, b in zip(selected, selected[1:])]
    minute_rates = [change / (gap / 60.0) for change, gap in zip(log_returns, gaps)]
    noise = statistics.pstdev(minute_rates) * math.sqrt(span / 60.0) * 10000.0
    change_bps = (math.log(selected[-1]["price"]) - math.log(selected[0]["price"])) * 10000.0
    threshold = max(floor_bps, 1.25 * noise)
    direction_sign = 1 if change_bps > 0 else -1
    aligned_seconds = sum(gap for change, gap in zip(log_returns, gaps) if change * direction_sign > 0)
    persistence = aligned_seconds / span
    direction = "UP" if change_bps > 0 else "DOWN"
    if abs(change_bps) < threshold or persistence < 2.0 / 3.0:
        direction = "FLAT"
    result.update({"eligible": True, "direction": direction, "log_return_bps": change_bps, "noise_bps": noise, "threshold_bps": threshold, "directional_time_fraction": persistence})
    # A volume-weighted price requires actual interval trades and their VWAP.
    weighted = []
    for point in selected:
        row = point["row"]
        volume, price = _number(row.get("volume")), _number(row.get("volume_price"))
        if row.get("volume_kind") != "interval_traded" or volume is None or volume < 0 or price is None or price <= 0:
            break
        if "parity" in _identity(row)[4]:
            break
        weighted.append((volume, price))
    volume_total = sum(volume for volume, _ in weighted)
    vwap = None
    if len(weighted) == len(selected) and volume_total > 0:
        vwap = sum(volume * price for volume, price in weighted) / volume_total
    result["window_vwap"] = vwap
    result["vwap_status"] = "AVAILABLE" if vwap is not None else "NO_MATCHED_UNDERLYING_TRADE_VOLUME"
    return result


def _breadth(histories: Mapping[str, Sequence[Mapping[str, Any]]], now: datetime) -> dict[str, Any]:
    members: dict[str, list[dict[str, Any]]] = defaultdict(list)
    excluded = {}
    for raw_symbol, history in sorted(histories.items()):
        symbol = str(raw_symbol).upper().strip()
        family = FAMILIES.get(symbol)
        if family is None:
            excluded[symbol] = ["OUTSIDE_CORE_EQUITY_FAMILIES"]
            continue
        points, coverage, errors = _prepare(symbol, history, now)
        if errors:
            excluded[symbol] = errors
            continue
        trend = _window(points, now - timedelta(seconds=RECENT_SECONDS), now, min_span=240.0)
        if not trend["eligible"]:
            excluded[symbol] = [trend["reason"]]
            continue
        members[family].append({"symbol": symbol, "direction": trend["direction"], "age_seconds": coverage["age_seconds"]})
    families = {}
    for family, votes in sorted(members.items()):
        directions = {vote["direction"] for vote in votes}
        conflict = "UP" in directions and "DOWN" in directions
        direction = next(iter(directions)) if len(directions) == 1 else "MIXED"
        families[family] = {"direction": direction, "conflict": conflict, "members": votes}
    counts = Counter(item["direction"] for item in families.values())
    return {
        "kind": "core_equity_family_agreement",
        "scope": "Observed index/ETF family agreement; not exchange advance/decline breadth or independent votes.",
        "expected_families": 4, "covered_families": len(families),
        "coverage_fraction": len(families) / 4.0,
        "up_families": counts["UP"], "down_families": counts["DOWN"],
        "mixed_families": counts["MIXED"], "flat_families": counts["FLAT"],
        "conflict": bool(counts["UP"] and counts["DOWN"]) or any(item["conflict"] for item in families.values()),
        "families": families, "excluded_symbols": excluded,
    }


def evaluate_directional_shift(
    symbol: str,
    history: Sequence[Mapping[str, Any]],
    *,
    as_of_utc: datetime | str,
    cross_market_history: Mapping[str, Sequence[Mapping[str, Any]]] | None = None,
) -> dict[str, Any]:
    """Describe a persistent reversal after an established opposing baseline.

    Confirmation needs a continuous 10-minute baseline and two opposing
    2.5-minute recent windows (>=5 minutes elapsed in all). Duplicate reads
    cannot advance evidence. New direction from a flat baseline is NO_SHIFT.
    Cross-family disagreement downgrades a confirmed local reversal to WATCH;
    absent breadth is disclosed without inventing corroboration. Thresholds
    are fixed research hypotheses, not fitted win rates or probabilities.
    """
    normalized = str(symbol).upper().strip()
    now = _utc(as_of_utc)
    result: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION, "symbol": normalized,
        "as_of_utc": now.isoformat() if now else None,
        "authority": "RESEARCH_ONLY", "prediction_eligible": False,
        "status": "ABSTAIN", "direction": None, "reason_codes": [],
        "baseline": None, "recent": None, "confirmation_windows": [],
        "coverage": {}, "cross_market": None,
        "limitations": ["Descriptive change detection; future directional accuracy has not been established.", "Thresholds require point-in-time out-of-sample evaluation before promotion."],
    }
    if now is None:
        result["reason_codes"] = ["AS_OF_TIMESTAMP_INVALID"]
        return result
    try:
        opened, closed = session_bounds(now.astimezone(EASTERN).date())
    except ValueError:
        result["reason_codes"] = ["SESSION_CALENDAR_UNAVAILABLE"]
        return result
    if not opened <= now <= closed:
        result["reason_codes"] = ["OUTSIDE_REGULAR_SESSION"]
        return result
    points, coverage, reasons = _prepare(normalized, history, now)
    result["coverage"] = coverage
    if reasons:
        result["reason_codes"] = reasons
        return result
    end = points[-1]["time"]
    recent_start = end - timedelta(seconds=RECENT_SECONDS)
    baseline = _window(points, recent_start - timedelta(seconds=BASELINE_SECONDS), recent_start, min_span=480.0)
    recent = _window(points, recent_start, end, min_span=RECENT_SECONDS)
    midpoint = end - timedelta(seconds=RECENT_SECONDS / 2.0)
    windows = [
        _window(points, recent_start, midpoint, min_span=120.0, floor_bps=2.5),
        _window(points, midpoint, end, min_span=120.0, floor_bps=2.5),
    ]
    result.update({"baseline": baseline, "recent": recent, "confirmation_windows": windows})
    if not baseline["eligible"] or not recent["eligible"]:
        result["reason_codes"] = ["INSUFFICIENT_BASELINE_OR_RECENT_HISTORY"]
        return result
    histories = dict(cross_market_history or {})
    histories[normalized] = history
    breadth = _breadth(histories, now)
    result["cross_market"] = breadth
    opposing = baseline["direction"] in {"UP", "DOWN"} and recent["direction"] in {"UP", "DOWN"} and baseline["direction"] != recent["direction"]
    if not opposing:
        result["status"] = "NO_SHIFT"
        result["reason_codes"] = ["NO_ESTABLISHED_OPPOSING_BASELINE" if baseline["direction"] == "FLAT" else "NO_PERSISTENT_REVERSAL"]
        return result
    result["direction"] = recent["direction"]
    confirmed = all(window["eligible"] and window["direction"] == recent["direction"] for window in windows)
    result["status"] = "CONFIRMED_SHIFT" if confirmed else "WATCH"
    result["reason_codes"] = ["PERSISTENT_OPPOSING_DIRECTION" if confirmed else "REVERSAL_CONFIRMATION_PENDING"]
    if breadth["conflict"]:
        result["status"] = "WATCH"
        result["reason_codes"].append("CROSS_MARKET_DIRECTION_CONFLICT")
    if breadth["covered_families"] < 2:
        result["reason_codes"].append("CROSS_MARKET_COVERAGE_LIMITED")
    if not coverage["live_generation_bound"]:
        result["reason_codes"].append("LIVE_GENERATION_BINDING_NOT_SUPPLIED")
    return result
