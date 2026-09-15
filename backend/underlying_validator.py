"""Independent Databento underlying-price validation for option-implied spot."""
from __future__ import annotations

import hashlib
import logging
import os
import re
import threading
from datetime import datetime, timedelta, timezone
from typing import Any

import databento as db

logger = logging.getLogger(__name__)

PROXY_SYMBOLS = {
    "SPX": "SPY",
    "NDX": "QQQ",
    "SPY": "SPY",
    "QQQ": "QQQ",
    "DIA": "DIA",
    "IWM": "IWM",
    "RUT": "IWM",
    "DJI": "DIA",
}
VALIDATION_MAX_AGE_SECONDS = float(os.getenv("DATABENTO_UNDERLYING_MAX_AGE_SECONDS", "120"))
VALIDATION_REFRESH_SECONDS = float(os.getenv("DATABENTO_UNDERLYING_REFRESH_SECONDS", "60"))
STRUCTURAL_LICENSE_BACKOFF_SECONDS = 1_800.0
LOOKBACK_DAYS = int(os.getenv("DATABENTO_UNDERLYING_LOOKBACK_DAYS", "2"))
EQUS_DATASET = "EQUS.MINI"
EQUS_SCHEMA = "ohlcv-1m"

_lock = threading.Lock()
_RequestIdentity = tuple[str, str, str, str]
_StructuralIdentity = tuple[str, str, str]
_latest: dict[_RequestIdentity, dict[str, Any]] = {}
_latest_checked: dict[_RequestIdentity, datetime] = {}
_refreshing: set[_RequestIdentity] = set()
_structural_denials: dict[_StructuralIdentity, tuple[datetime, str]] = {}


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _credential_fingerprint() -> str:
    api_key = os.getenv("DATABENTO_API_KEY", "").strip()
    if not api_key:
        return "missing"
    return hashlib.sha256(api_key.encode("utf-8")).hexdigest()[:16]


def _structural_request_identity() -> _StructuralIdentity:
    return (EQUS_DATASET, EQUS_SCHEMA, _credential_fingerprint())


def _validation_request_identity(proxy: str) -> _RequestIdentity:
    return (*_structural_request_identity(), proxy.upper())


def _is_structural_license_denial(value: object) -> bool:
    text = str(value).casefold()
    return "403" in text and (
        "license_not_found_unauthorized" in text
        or "live data license is required" in text
    )


def _proxy_bar_query_window(now: datetime | None = None) -> tuple[str, str]:
    """Return an inclusive start and exclusive end that includes today's bars."""
    end_exclusive = now or _utcnow()
    if end_exclusive.tzinfo is None:
        end_exclusive = end_exclusive.replace(tzinfo=timezone.utc)
    end_exclusive = end_exclusive.astimezone(timezone.utc)

    start_day = end_exclusive.date() - timedelta(days=max(0, LOOKBACK_DAYS))
    # A short calendar lookback can land entirely on a weekend (notably on
    # Monday). Roll its inclusive start back to Friday so a prior bar remains
    # available before the current session has printed.
    while start_day.weekday() >= 5:
        start_day -= timedelta(days=1)

    # Databento's `end` is exclusive. Passing today's date alone means midnight
    # at the start of today and therefore excludes every current-day bar.
    return (
        start_day.isoformat(),
        end_exclusive.isoformat().replace("+00:00", "Z"),
    )


def _available_end_from_error(exc: Exception, requested_end: str) -> str | None:
    """Extract Databento's published dataset cutoff for one bounded retry."""
    text = str(exc)
    if "data_end_after_available_end" not in text:
        return None
    match = re.search(r"available up to '([^']+)'", text, flags=re.IGNORECASE)
    if not match:
        return None
    try:
        available_end = datetime.fromisoformat(match.group(1).replace("Z", "+00:00"))
        requested = datetime.fromisoformat(requested_end.replace("Z", "+00:00"))
    except ValueError:
        return None
    if available_end.tzinfo is None:
        available_end = available_end.replace(tzinfo=timezone.utc)
    if requested.tzinfo is None:
        requested = requested.replace(tzinfo=timezone.utc)
    available_end = available_end.astimezone(timezone.utc)
    requested = requested.astimezone(timezone.utc)
    if available_end >= requested:
        return None
    return available_end.isoformat().replace("+00:00", "Z")


def _unavailable(symbol: str, reason: str) -> dict[str, Any]:
    normalized = symbol.upper()
    proxy_symbol = PROXY_SYMBOLS.get(normalized)
    if proxy_symbol is None and normalized in PROXY_SYMBOLS.values():
        proxy_symbol = normalized
    return {
        "underlying_validation_status": "unavailable",
        "underlying_proxy_symbol": proxy_symbol,
        "underlying_price": None,
        "underlying_timestamp_utc": None,
        "underlying_age_seconds": None,
        "underlying_divergence_points": None,
        "underlying_divergence_pct": None,
        "underlying_validation_reason": reason,
    }


def _license_denied(
    proxy: str,
    reason: str,
    *,
    retry_after: datetime,
) -> dict[str, Any]:
    result = _unavailable(proxy, reason)
    result["underlying_validation_failure_class"] = "structural_license_denial"
    result["underlying_validation_retry_after_utc"] = retry_after.isoformat().replace(
        "+00:00", "Z"
    )
    result["underlying_validation_backoff_seconds"] = (
        STRUCTURAL_LICENSE_BACKOFF_SECONDS
    )
    return result


def _latest_proxy_bar(proxy: str) -> dict[str, Any]:
    api_key = os.getenv("DATABENTO_API_KEY", "").strip()
    if not api_key:
        return _unavailable(proxy, "DATABENTO_API_KEY is not configured")

    try:
        client = db.Historical(api_key)
        start, end_exclusive = _proxy_bar_query_window()
        request = {
            "dataset": EQUS_DATASET,
            "symbols": [proxy],
            "schema": EQUS_SCHEMA,
            "start": start,
            "end": end_exclusive,
        }
        try:
            result = client.timeseries.get_range(**request)
        except Exception as exc:
            available_end = _available_end_from_error(exc, end_exclusive)
            if available_end is None:
                raise
            request["end"] = available_end
            logger.info(
                "Capping EQUS validation request for %s at Databento available_end %s",
                proxy,
                available_end,
            )
            result = client.timeseries.get_range(**request)
        frame = result.to_df()
        if frame is None or frame.empty:
            return _unavailable(proxy, "No EQUS.MINI 1-minute bars returned")
        frame = frame.reset_index()
        timestamp_column = next((column for column in ("ts_event", "ts_recv", "timestamp") if column in frame.columns), None)
        if timestamp_column is None or "close" not in frame.columns:
            return _unavailable(proxy, "EQUS bar response lacks timestamp or close")
        row = frame.iloc[-1]
        timestamp = row[timestamp_column]
        if hasattr(timestamp, "to_pydatetime"):
            timestamp = timestamp.to_pydatetime()
        if not isinstance(timestamp, datetime):
            timestamp = datetime.fromisoformat(str(timestamp).replace("Z", "+00:00"))
        if timestamp.tzinfo is None:
            timestamp = timestamp.replace(tzinfo=timezone.utc)
        timestamp = timestamp.astimezone(timezone.utc)
        age = max(0.0, (_utcnow() - timestamp).total_seconds())
        if age > VALIDATION_MAX_AGE_SECONDS:
            return _unavailable(proxy, f"Latest EQUS bar is stale ({age:.1f}s)")
        return {
            "underlying_validation_status": "bar_available",
            "underlying_proxy_symbol": proxy,
            "underlying_price": float(row["close"]),
            "underlying_timestamp_utc": timestamp.isoformat().replace("+00:00", "Z"),
            "underlying_age_seconds": age,
            "underlying_divergence_points": None,
            "underlying_divergence_pct": None,
            "underlying_validation_reason": None,
        }
    except Exception as exc:  # EQUS must never block OPRA.
        logger.warning("EQUS underlying validation unavailable for %s: %s", proxy, exc)
        return _unavailable(proxy, str(exc))


def validate_underlying(symbol: str, option_implied_spot: float | None) -> dict[str, Any]:
    """Compare EQUS proxy close with option-implied spot without blocking live OPRA."""
    symbol = symbol.upper()
    proxy = PROXY_SYMBOLS.get(symbol)
    if proxy is None:
        return _unavailable(symbol, "No EQUS proxy configured")
    if not option_implied_spot or option_implied_spot <= 0:
        return _unavailable(proxy, "Option-implied spot is unavailable")

    cache_key = _validation_request_identity(proxy)
    structural_key = cache_key[:3]
    now = _utcnow()
    with _lock:
        structural_denial = _structural_denials.get(structural_key)
        if structural_denial is not None:
            retry_after, reason = structural_denial
            if now < retry_after:
                return _license_denied(proxy, reason, retry_after=retry_after)
            _structural_denials.pop(structural_key, None)
        cached = _latest.get(cache_key)
        checked = _latest_checked.get(cache_key)
        refresh_due = (
            checked is None
            or (now - checked).total_seconds() >= VALIDATION_REFRESH_SECONDS
        )
        if cached is not None and not refresh_due:
            return dict(cached)
        should_refresh = refresh_due and cache_key not in _refreshing
        if should_refresh:
            _refreshing.add(cache_key)
    if should_refresh:
        threading.Thread(
            target=_refresh_validation,
            args=(cache_key, structural_key, proxy),
            daemon=True,
            name=f"eq-validation-{proxy}",
        ).start()
    return dict(cached) if cached else _unavailable(proxy, "EQUS validation warming")


def _refresh_validation(
    cache_key: _RequestIdentity,
    structural_key: _StructuralIdentity,
    proxy: str,
) -> None:
    try:
        result = _latest_proxy_bar(proxy)
        checked_at = _utcnow()
        reason = str(result.get("underlying_validation_reason") or "")
        structural_license_denial = (
            result.get("underlying_validation_status") == "unavailable"
            and _is_structural_license_denial(reason)
        )
        if structural_license_denial:
            retry_after = checked_at + timedelta(
                seconds=STRUCTURAL_LICENSE_BACKOFF_SECONDS
            )
            result = _license_denied(proxy, reason, retry_after=retry_after)
            logger.warning(
                "EQUS structural license denial; suppressing equivalent %s/%s requests for %.0fs",
                EQUS_DATASET,
                EQUS_SCHEMA,
                STRUCTURAL_LICENSE_BACKOFF_SECONDS,
            )
        if result["underlying_validation_status"] != "unavailable":
            result["underlying_divergence_points"] = None
            result["underlying_divergence_pct"] = None
            result["underlying_validation_status"] = "bar_available_proxy_only"
        with _lock:
            # Cache both success and explicit unavailability. Callers should see
            # the provider's actual stale/no-data reason during the refresh
            # interval rather than reverting to a perpetual "warming" label.
            _latest[cache_key] = result
            _latest_checked[cache_key] = checked_at
            if structural_license_denial:
                _structural_denials[structural_key] = (retry_after, reason)
            else:
                _structural_denials.pop(structural_key, None)
    finally:
        with _lock:
            _refreshing.discard(cache_key)
