"""Databento OPRA live streamer for same-day SPXW/NDXP gamma pins.

This is an alternate market data provider for the existing FastAPI backend. It
preserves the same high-level streamer interface as the Polygon streamer:
start/stop, get_latest_data, get_all_latest, and callback support.
"""
from __future__ import annotations

import asyncio
from concurrent.futures import Future, ThreadPoolExecutor
import heapq
import hashlib
import json
import logging
import math
import os
import re
import threading
import time
import uuid
import weakref
from collections import deque
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from pathlib import Path
from typing import Callable, Mapping, Optional

import databento as db
import numpy as np
import pandas as pd
from scipy.optimize import brentq
from scipy.special import ndtr
from scipy.stats import norm

from backend.config import DATA_DIR, DATABENTO_REQUIRED_SYMBOLS, MAX_BUFFER_SIZE
from backend.market_universe import OPTION_ETF_SYMBOLS
from backend.optional_family_canary import (
    CanaryDecision,
    CORE_PRIMARY_PAIR_COVERAGE_RATIO,
    RUTOptionalFamilyCanary,
)
from backend.underlying_validator import validate_underlying
from backend.snapshot_export import finalize_snapshot_export_payload
from backend.validation_method import (
    validation_method_fields, MONEYNESS_LOWER, MONEYNESS_UPPER,
    MIN_PRIMARY_STRIKES, MIN_NONZERO_STRIKES, MAX_STRIKE_CONCENTRATION,
)
from app.utils.market_time import is_holiday, market_is_closed
from app.utils.time_et import close_time_et, open_time_et

logger = logging.getLogger(__name__)

DATASET = "OPRA.PILLAR"
RISK_FREE_RATE = float(os.getenv("DATABENTO_RISK_FREE_RATE", "0.0525"))
CONTRACT_MULTIPLIER = 100.0
RISK_FREE_RATE_MIN = float(os.getenv("DATABENTO_RISK_FREE_RATE_MIN", "0.0"))
RISK_FREE_RATE_MAX = float(os.getenv("DATABENTO_RISK_FREE_RATE_MAX", "1.0"))
DATABENTO_EXPIRY_HOUR_ET = int(os.getenv("DATABENTO_EXPIRY_HOUR_ET", "16"))
DATABENTO_EXPIRY_MINUTE_ET = int(os.getenv("DATABENTO_EXPIRY_MINUTE_ET", "0"))
MIN_TTE_DAYS = float(os.getenv("DATABENTO_MIN_TTE_DAYS", f"{1.0/365.0}"))
GEX_FORMULA_VERSION = "databento-gex-v2-call-minus-put"
TARGET_FORMULA_VERSION = "expiration-blend-v2-bucket-gex-normalized"
SPOT_FORMULA_VERSION = "put-call-parity-v2-discounted-strike"
GEX_INFERENCE_FEATURE_SCHEMA_VERSION = "databento-gex-structure-v1"
OI_STAT_TYPE = 9
MAX_SYMBOLS_PER_REQUEST = 200
RECONNECT_BASE_DELAY_SECONDS = float(os.getenv("DATABENTO_RECONNECT_BASE_SECONDS", "3"))
RECONNECT_MAX_DELAY_SECONDS = float(os.getenv("DATABENTO_RECONNECT_MAX_SECONDS", "30"))
LIVE_CLIENT_CLOSE_TIMEOUT_SECONDS = 5.0
OPEN_CONNECTION_LIMIT_PHRASE = "user has reached their open connection limit"
OPEN_CONNECTION_LIMIT_COOLDOWNS_SECONDS = (60.0, 120.0, 240.0, 300.0)
# Keep the imported SDK class stable even when tests replace ``db.Live`` with a
# local double.  Real SDK clients must carry the pre-authentication transport
# guard below; test doubles do not own a TCP transport and remain unaffected.
_DATABENTO_SDK_LIVE_CLASS = db.Live
PREOPEN_SUBSCRIPTION_LEAD_SECONDS = max(
    0.0,
    float(os.getenv("DATABENTO_PREOPEN_SUBSCRIPTION_LEAD_SECONDS", "2700")),
)
OFF_HOURS_POLL_SECONDS = min(
    1.0,
    max(0.05, float(os.getenv("DATABENTO_OFF_HOURS_POLL_SECONDS", "1"))),
)
STREAM_STALL_SECONDS = float(os.getenv("DATABENTO_STREAM_STALL_SECONDS", "45"))
STREAM_PROGRESS_WINDOW_SECONDS = float(os.getenv("DATABENTO_PROGRESS_WINDOW_SECONDS", "20"))
RECONNECT_HEALTHY_RESET_SECONDS = max(
    1.0,
    float(os.getenv("DATABENTO_RECONNECT_HEALTHY_RESET_SECONDS", "60")),
)
UNIVERSE_REFRESH_SECONDS = float(os.getenv("DATABENTO_UNIVERSE_REFRESH_SECONDS", "900"))
UNIVERSE_FALLBACK_REFRESH_SECONDS = float(os.getenv("DATABENTO_UNIVERSE_FALLBACK_REFRESH_SECONDS", "300"))
UNIVERSE_FALLBACK_MAX_AGE_DAYS = int(os.getenv("DATABENTO_UNIVERSE_FALLBACK_MAX_AGE_DAYS", "4"))
# The pre-start preparer may look back across a weekend/holiday, but stages only
# the newest provider-complete session it finds.  The resulting cache retains
# its source date and is never relabelled as current-day discovery.
PROVIDER_PRIOR_CACHE_LOOKBACK_DAYS = 4
ALLOW_PRIOR_UNIVERSE_FALLBACK = os.getenv("DATABENTO_ALLOW_PRIOR_UNIVERSE_FALLBACK", "1") == "1"
QUOTE_FRESHNESS_SECONDS = float(os.getenv("DATABENTO_QUOTE_FRESHNESS_SECONDS", "10"))
HANDOFF_TIMEOUT_SECONDS = float(os.getenv("DATABENTO_HANDOFF_TIMEOUT_SECONDS", "30"))
OPEN_TRANSITION_GRACE_SECONDS = float(os.getenv("DATABENTO_OPEN_TRANSITION_GRACE_SECONDS", "90"))
REPLAY_ON_RECONNECT = os.getenv("DATABENTO_REPLAY_ON_RECONNECT", "0") == "1"
REPLAY_DURING_REGULAR_SESSION = (
    os.getenv("DATABENTO_REPLAY_DURING_REGULAR_SESSION", "0") == "1"
)
COMPUTE_WARMUP_GRACE_SECONDS = max(
    0.0,
    float(os.getenv("DATABENTO_COMPUTE_WARMUP_GRACE_SECONDS", "30")),
)
PROVIDER_OVERLOAD_COMPUTE_BACKOFF_SECONDS = max(
    0.0,
    float(os.getenv("DATABENTO_OVERLOAD_COMPUTE_BACKOFF_SECONDS", "30")),
)
MIN_PAIRED_QUOTES = int(os.getenv("DATABENTO_MIN_PAIRED_QUOTES", "5"))
ORB_REFERENCE_INTERVAL_SECONDS = max(
    1,
    int(os.getenv("DATABENTO_ORB_REFERENCE_INTERVAL_SECONDS", "5")),
)
ORB_REFERENCE_ATTEMPT_TIMEOUT_SECONDS = min(
    float(ORB_REFERENCE_INTERVAL_SECONDS),
    max(
        0.05,
        float(
            os.getenv(
                "DATABENTO_ORB_REFERENCE_ATTEMPT_TIMEOUT_SECONDS",
                str(max(0.5, float(ORB_REFERENCE_INTERVAL_SECONDS) - 1.0)),
            )
        ),
    ),
)
ORB_REFERENCE_SHUTDOWN_BARRIER_TIMEOUT_SECONDS = max(
    0.05,
    float(
        os.getenv(
            "DATABENTO_ORB_REFERENCE_SHUTDOWN_BARRIER_TIMEOUT_SECONDS", "2"
        )
    ),
)
ORB_REFERENCE_FAILED_ATTEMPT_LIMIT = 64
ORB_REFERENCE_FAILURE_WARNING_INTERVAL_SECONDS = 60.0
ORB_STAGED_SUBSCRIPTION_FAMILIES = ("SPX", "NDX", "VIX", "RUT")
ORB_STAGE_CLEAN_TRANSPORT_SECONDS = 60.0
ORB_STAGE_MAX_DATA_AGE_SECONDS = 15.0
ORB_STAGE_MAX_P95_LAG_SECONDS = 2.0
ORB_STAGE_MIN_PRIMARY_PAIR_COVERAGE_RATIO = 0.10
ORB_STAGE_MIN_FRESH_COVERAGE_RATIO = 0.50
ORB_STAGE_EVALUATION_INTERVAL_SECONDS = 5.0
MULTI_EXPIRATION_MAX_DTE = int(os.getenv("DATABENTO_MULTI_EXPIRATION_MAX_DTE", "45"))
USE_MULTI_EXPIRATION_TARGET = os.getenv("DATABENTO_USE_MULTI_EXPIRATION", "0") == "1"
SUBSCRIPTION_PROFILE = os.getenv("DATABENTO_SUBSCRIPTION_PROFILE", "near-term-shadow").strip().lower()
SUPPORTED_SUBSCRIPTION_PROFILES = {"full", "primary-only", "near-term-shadow"}
# Cboe VIX Options specifications: the last trading day is the business day
# immediately preceding the AM exercise-settlement date.
VIX_FORWARD_CONTEXT_AUTHORITY = "vix_forward_expiration_context_only"
VIX_FORWARD_CONTEXT_SELECTION_BASIS = (
    "vix_last_trading_day_precedes_settlement_date"
)
VIX_SETTLEMENT_INELIGIBLE_REASON = "VIX_AM_SETTLED_LAST_TRADING_DAY_PASSED"
VIX_PRIMARY_NOT_FORWARD_REASON = "VIX_AM_SETTLED_PRIMARY_NOT_FORWARD"
PRIMARY_MAX_STRIKE_PAIRS = max(1, int(os.getenv("DATABENTO_PRIMARY_MAX_STRIKE_PAIRS", "600")))
SHADOW_MAX_STRIKE_PAIRS = max(1, int(os.getenv("DATABENTO_SHADOW_MAX_STRIKE_PAIRS", "200")))
MIN_PRIMARY_STRIKE_PAIRS = max(1, int(os.getenv("DATABENTO_MIN_PRIMARY_STRIKE_PAIRS", "100")))
MIN_CORE_PRIMARY_PAIR_COMPLETENESS_RATIO = min(
    1.0,
    max(0.0, float(os.getenv("DATABENTO_MIN_CORE_PRIMARY_PAIR_COMPLETENESS_RATIO", "0.90"))),
)
MIN_LIVE_PRIMARY_PAIR_COVERAGE_RATIO = CORE_PRIMARY_PAIR_COVERAGE_RATIO
MIN_NEXT_LISTED_STRIKE_PAIRS = max(1, int(os.getenv("DATABENTO_MIN_NEXT_LISTED_STRIKE_PAIRS", "50")))
MAX_CONTRACTS_PER_MARKET = max(2, int(os.getenv("DATABENTO_MAX_CONTRACTS_PER_MARKET", "1800")))
MAX_SUBSCRIPTION_CONTRACTS = max(2, int(os.getenv("DATABENTO_MAX_SUBSCRIPTION_CONTRACTS", "3600")))
SUBSCRIPTION_CENTER_PAIR_FRACTION = min(
    1.0,
    max(0.0, float(os.getenv("DATABENTO_CENTER_PAIR_FRACTION", "0.60"))),
)
ALLOW_LIVE_UNIVERSE_REFRESH_RECONNECT = (
    os.getenv("DATABENTO_ALLOW_LIVE_UNIVERSE_REFRESH_RECONNECT", "0") == "1"
)
PIN_CONTESTED_LEAD_RATIO = float(os.getenv("DATABENTO_PIN_CONTESTED_LEAD_RATIO", "0.10"))
CLOCK_SYNC_WINDOW_RECORDS = 2048
CLOCK_SYNC_MIN_SAMPLES = 50
CLOCK_SYNC_NEGATIVE_TOLERANCE_SECONDS = 0.050
CLOCK_SYNC_MAX_NEGATIVE_RATIO = 0.01
UNIVERSE_CACHE_VERSION = "definitions-v2"
UNIVERSE_CACHE_METADATA_VERSION = "databento-universe-cache-metadata-v1"
UNIVERSE_STAGE_FRAGMENT_VERSION = "market-fragment-v1"
UNIVERSE_STAGE_FRAGMENT_COLUMNS = (
    "_stage_fragment_version",
    "_stage_cache_hash",
    "_stage_source_date",
    "_stage_market",
    "_stage_definition_end",
    "_stage_statistics_end",
    "_stage_row_count",
    "_stage_saved_at_utc",
    "_stage_definition_discovery",
)
RAW_SYMBOL_RE = re.compile(r"^(?P<root>[A-Z]+)\s+(?P<yymmdd>\d{6})(?P<cp>[CP])(?P<strike>\d{8})$")
_NY_TZ = ZoneInfo("America/New_York")
_SECONDS_PER_YEAR = 365.0 * 24.0 * 60.0 * 60.0


def _utcnow_naive() -> datetime:
    """Return UTC now as a naive datetime for compatibility with existing payloads."""
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _abort_pre_auth_transport_once(
    protocol: object,
    authenticated_future: object,
    *,
    on_event: Callable[[str, str], None] | None = None,
) -> None:
    """Abort one SDK protocol transport after authentication fails or cancels.

    Databento 0.84 keeps the newly-created transport local to
    ``LiveSession._connect_task`` until authentication succeeds.  If that await
    fails, ``Live.stop()`` and ``Live.terminate()`` cannot reach the transport.
    This callback runs on the SDK event-loop thread, consumes the future's
    exception, and aborts only the protocol that owns the failed future.
    """

    def emit(outcome: str, reason: str) -> None:
        if on_event is None:
            return
        try:
            on_event(outcome, reason)
        except Exception:
            logger.exception(
                "Databento pre-auth transport telemetry callback failed"
            )

    try:
        if not bool(authenticated_future.done()):
            return
        if bool(authenticated_future.cancelled()):
            failure_reason = "authentication_cancelled"
        else:
            try:
                authentication_error = authenticated_future.exception()
            except asyncio.CancelledError:
                failure_reason = "authentication_cancelled"
            except BaseException as exc:
                # A malformed future implementation must never poison the SDK
                # event loop callback runner.
                failure_reason = f"authentication_future_error:{type(exc).__name__}"
            else:
                if authentication_error is None:
                    return
                failure_reason = (
                    f"authentication_failed:{type(authentication_error).__name__}"
                )
    except BaseException as exc:
        # A defensive contract failure is not authority to touch a transport.
        emit("future_state_error", type(exc).__name__)
        return

    marker = "_marketpin_pre_auth_transport_abort_attempted"
    try:
        if bool(getattr(protocol, marker, False)):
            return
        setattr(protocol, marker, True)
    except Exception as exc:
        emit("protocol_marker_error", type(exc).__name__)
        return

    try:
        transport = getattr(protocol, "transport")
    except Exception as exc:
        emit("transport_unavailable", f"{failure_reason}:{type(exc).__name__}")
        return

    try:
        transport.abort()
    except Exception as exc:
        emit("transport_abort_error", f"{failure_reason}:{type(exc).__name__}")
        return

    emit("transport_aborted", failure_reason)


def _install_pre_auth_transport_guard(
    live_client: object,
    *,
    on_event: Callable[[str, str], None] | None = None,
) -> bool:
    """Install the Databento 0.84 pre-auth transport guard on one client.

    The contract is validated without opening a provider connection.  Returning
    ``False`` for a real SDK client makes the caller fail closed before
    ``subscribe()``.  Local test doubles intentionally return ``False`` and are
    exempted by the caller because they own no SDK transport.
    """

    session = getattr(live_client, "_session", None)
    create_protocol = getattr(session, "_create_protocol", None)
    if session is None or not callable(create_protocol):
        return False
    if bool(getattr(session, "_marketpin_pre_auth_transport_guard", False)):
        return True

    create_protocol_function = getattr(
        create_protocol, "__func__", create_protocol
    )
    create_protocol_globals = getattr(create_protocol_function, "__globals__", {})
    protocol_type = (
        create_protocol_globals.get("_SessionProtocol")
        if isinstance(create_protocol_globals, dict)
        else None
    )
    if protocol_type is None:
        return False
    if not isinstance(getattr(protocol_type, "authenticated", None), property):
        return False
    if not isinstance(getattr(protocol_type, "transport", None), property):
        return False

    def guarded_create_protocol(*args, **kwargs):
        protocol = create_protocol(*args, **kwargs)
        authenticated = getattr(protocol, "authenticated", None)
        add_done_callback = getattr(authenticated, "add_done_callback", None)
        if not callable(add_done_callback):
            raise RuntimeError(
                "DATABENTO_PRE_AUTH_TRANSPORT_GUARD_CONTRACT_CHANGED"
            )

        def abort_failed_auth(future: object) -> None:
            _abort_pre_auth_transport_once(
                protocol,
                future,
                on_event=on_event,
            )

        add_done_callback(abort_failed_auth)
        return protocol

    try:
        setattr(session, "_create_protocol", guarded_create_protocol)
        setattr(session, "_marketpin_pre_auth_transport_guard", True)
    except Exception:
        return False
    return True


def _record_timestamp_ns(record: object, field: str) -> int | None:
    """Return a usable DBN nanosecond timestamp without accepting sentinels."""
    try:
        raw_value = record.get(field) if isinstance(record, Mapping) else getattr(record, field)
        value = int(raw_value)
    except (AttributeError, TypeError, ValueError, OverflowError):
        return None
    # Databento uses an unsigned max-value sentinel for undefined timestamps.
    return value if 0 < value < 10**19 else None


def _timestamp_ns_to_utc_iso(value: int | None) -> str | None:
    if value is None:
        return None
    try:
        return datetime.fromtimestamp(value / 1_000_000_000, tz=timezone.utc).isoformat()
    except (OSError, OverflowError, ValueError):
        return None


def _dataframe_records(frame: pd.DataFrame) -> list[dict[str, object]]:
    """Convert pandas missing values to JSON-safe nulls for canonical replay blobs."""
    if frame.empty:
        return []
    normalized = frame.astype(object).where(pd.notna(frame), None)
    return normalized.to_dict(orient="records")


def current_market_date() -> date:
    """Return the exchange trading date rather than the host's local date."""
    return datetime.now(_NY_TZ).date()


def _expiration_is_live_eligible(
    market: object,
    expiration: date,
    trading_date: date,
) -> bool:
    """Apply the exchange trading cutoff before an expiry can be authoritative.

    VIX options are AM-settled and stop trading on the business day before their
    settlement date. Once the exchange date reaches the printed expiration, that
    series is no longer a live option series even though a calendar-only filter
    would still call it unexpired.
    """

    if str(market or "").strip().upper() == "VIX":
        return expiration > trading_date
    return expiration >= trading_date


def _expiration_authority_metadata(
    market: object,
    expiration: date | None,
    trading_date: date,
    *,
    role: object = "primary",
) -> dict[str, object]:
    """Describe whether an exact-date expiry is signal authority or context."""

    if str(market or "").strip().upper() == "VIX":
        return {
            "authority": VIX_FORWARD_CONTEXT_AUTHORITY,
            "context_only": True,
            "same_day_authority": False,
            "selection_basis": VIX_FORWARD_CONTEXT_SELECTION_BASIS,
        }
    if str(role or "").strip().lower() != "primary":
        return {
            "authority": "shadow_expiration_context_only",
            "context_only": True,
            "same_day_authority": False,
            "selection_basis": "supplemental_expiration_context",
        }
    return {
        "authority": "primary_expiration",
        "context_only": False,
        "same_day_authority": bool(expiration == trading_date),
        "selection_basis": "earliest_live_eligible_expiration",
    }


def live_subscription_window(now_utc: datetime | None = None) -> dict[str, object]:
    """Describe the bounded ET window in which a live OPRA client may connect.

    The backend starts 45 minutes before the cash open so symbol mappings and the
    provider connection are warm for the opening print. Quotes remain connected
    for a 15-minute post-cash-close research window; calculations retain the
    cash-session boundary. After collection closes, on weekends and holidays,
    the process remains available for audit without empty subscription churn.
    """
    observed_utc = now_utc or datetime.now(timezone.utc)
    if observed_utc.tzinfo is None:
        raise ValueError("now_utc must be timezone-aware")
    observed_utc = observed_utc.astimezone(timezone.utc)
    observed_et = observed_utc.astimezone(_NY_TZ)
    market_day = observed_et.date()
    non_trading_day = market_day.weekday() >= 5 or is_holiday(market_day)
    cash_open_et = open_time_et(observed_et)
    cash_close_et = close_time_et(observed_et)
    collection_close_et = cash_close_et + timedelta(minutes=15)
    connect_from_et = cash_open_et - timedelta(
        seconds=PREOPEN_SUBSCRIPTION_LEAD_SECONDS
    )

    if non_trading_day:
        state = "non_trading_day"
        allowed = False
    elif observed_et < connect_from_et:
        state = "preopen_wait"
        allowed = False
    elif observed_et < cash_open_et:
        state = "preopen"
        allowed = True
    elif observed_et < cash_close_et:
        state = "regular_session"
        allowed = True
    elif observed_et < collection_close_et:
        state = "post_close_research"
        allowed = True
    else:
        state = "post_close"
        allowed = False

    return {
        "state": state,
        "subscription_allowed": allowed,
        "observed_at_utc": observed_utc.isoformat(),
        "trading_date": market_day.isoformat(),
        "connect_from_utc": connect_from_et.astimezone(timezone.utc).isoformat(),
        "cash_open_utc": cash_open_et.astimezone(timezone.utc).isoformat(),
        "cash_close_utc": cash_close_et.astimezone(timezone.utc).isoformat(),
        "collection_close_utc": collection_close_et.astimezone(timezone.utc).isoformat(),
        "prediction_session_allowed": state == "regular_session",
        "preopen_lead_seconds": PREOPEN_SUBSCRIPTION_LEAD_SECONDS,
    }


@dataclass(frozen=True)
class DatabentoMarketConfig:
    label: str
    daily_root: str
    strike_step: int
    strike_min: int
    strike_max: int
    days_forward: int = MULTI_EXPIRATION_MAX_DTE


@dataclass(frozen=True)
class _OrbReferenceAttempt:
    market: str
    subscription_epoch_id: str
    subscription_generation: int
    intended_bucket_utc: datetime
    started_at_utc: datetime
    deadline_utc: datetime
    deadline_monotonic: float
    cancel_event: threading.Event


MARKETS = {
    "SPX": DatabentoMarketConfig("SPX", "SPXW", 5, 7000, 8200),
    "NDX": DatabentoMarketConfig("NDX", "NDXP", 25, 29400, 30100),
    "SPY": DatabentoMarketConfig("SPY", "SPY", 1, 500, 900),
    "QQQ": DatabentoMarketConfig("QQQ", "QQQ", 1, 400, 900),
    "DIA": DatabentoMarketConfig("DIA", "DIA", 1, 300, 600),
    "IWM": DatabentoMarketConfig("IWM", "IWM", 1, 100, 400),
    "XSP": DatabentoMarketConfig("XSP", "XSP", 1, 700, 760),
    "XND": DatabentoMarketConfig("XND", "XND", 1, 280, 310),
    "RUT": DatabentoMarketConfig("RUT", "RUTW", 5, 2100, 2300),
    "MRUT": DatabentoMarketConfig("MRUT", "MRUT", 1, 210, 230),
    "VIX": DatabentoMarketConfig("VIX", "VIXW", 1, 10, 35, days_forward=45),
    "OEX": DatabentoMarketConfig("OEX", "OEX", 5, 3600, 3900),
    "DJX": DatabentoMarketConfig("DJX", "DJX", 1, 420, 460),
    "RUI": DatabentoMarketConfig("RUI", "RUI", 5, 4000, 4600),
    "XAU": DatabentoMarketConfig("XAU", "XAU", 5, 250, 360),
    "HGX": DatabentoMarketConfig("HGX", "HGX", 5, 550, 700),
    "OSX": DatabentoMarketConfig("OSX", "OSX", 5, 75, 115),
    "UTY": DatabentoMarketConfig("UTY", "UTY", 5, 950, 1100),
}


def raw_option_symbol(root: str, expiration: date, option_type: str, strike: float) -> str:
    return f"{root:<6}{expiration.strftime('%y%m%d')}{option_type}{int(round(strike * 1000)):08d}"


def parse_raw_option_symbol(symbol: str) -> dict[str, object] | None:
    match = RAW_SYMBOL_RE.match(str(symbol))
    if not match:
        return None
    yymmdd = match.group("yymmdd")
    expiration = date(2000 + int(yymmdd[:2]), int(yymmdd[2:4]), int(yymmdd[4:6]))
    return {
        "expiration": expiration,
        "option_type": match.group("cp"),
        "strike": int(match.group("strike")) / 1000.0,
    }


def chunks(values: list[str], size: int = MAX_SYMBOLS_PER_REQUEST):
    for start in range(0, len(values), size):
        yield values[start:start + size]


def parse_db_time(value: str) -> datetime:
    cleaned = value.replace("Z", "+00:00")
    if "." in cleaned:
        head, tail = cleaned.split(".", 1)
        frac, zone = tail[:9], tail[9:]
        cleaned = f"{head}.{frac[:6]}{zone}"
    return datetime.fromisoformat(cleaned)


def _configured_risk_free_rate() -> float:
    """Read risk-free rate from env with sane clamping boundaries."""
    value = float(os.getenv("DATABENTO_RISK_FREE_RATE", str(RISK_FREE_RATE)))
    if not (RISK_FREE_RATE_MIN <= value <= RISK_FREE_RATE_MAX):
        logger.warning("DATABENTO_RISK_FREE_RATE=%s outside [%s, %s]; clamped", value, RISK_FREE_RATE_MIN, RISK_FREE_RATE_MAX)
        value = min(max(value, RISK_FREE_RATE_MIN), RISK_FREE_RATE_MAX)
    return value


def _expiration_cutoff_utc(expiration: date) -> datetime:
    cutoff_et = datetime.combine(
        expiration,
        datetime.min.time().replace(
            hour=DATABENTO_EXPIRY_HOUR_ET,
            minute=DATABENTO_EXPIRY_MINUTE_ET,
            second=0,
            microsecond=0,
        ),
        tzinfo=_NY_TZ,
    )
    return cutoff_et.astimezone(timezone.utc)


def years_to_expiration(expiration: date | datetime, now: datetime | None = None) -> float:
    """Return time-to-expiration in years with expiry cut-off at NY close."""
    if not isinstance(expiration, date):
        return MIN_TTE_DAYS

    cutoff_utc = _expiration_cutoff_utc(expiration)
    if now is None:
        now_utc = datetime.now(timezone.utc)
    elif now.tzinfo is None:
        now_utc = now.replace(tzinfo=timezone.utc)
    else:
        now_utc = now.astimezone(timezone.utc)
    remaining_seconds = max(0.0, (cutoff_utc - now_utc).total_seconds())
    return max(MIN_TTE_DAYS, remaining_seconds / _SECONDS_PER_YEAR)


def normalize_price(value) -> float | None:
    if value is None:
        return None
    try:
        price = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(price) or price <= 0:
        return None
    if abs(price) > 1_000_000:
        price /= 1_000_000_000.0
    return price


def option_intrinsic(spot: float, strike: float, option_type: str) -> float:
    return max(spot - strike, 0.0) if option_type == "C" else max(strike - spot, 0.0)


def black_scholes_price(spot: float, strike: float, years: float, vol: float, option_type: str) -> float:
    if years <= 0 or vol <= 0:
        return option_intrinsic(spot, strike, option_type)
    risk_free_rate = _configured_risk_free_rate()
    d1 = (math.log(spot / strike) + (risk_free_rate + 0.5 * vol * vol) * years) / (vol * math.sqrt(years))
    d2 = d1 - vol * math.sqrt(years)
    discounted_strike = strike * math.exp(-risk_free_rate * years)
    if option_type == "C":
        return spot * norm.cdf(d1) - discounted_strike * norm.cdf(d2)
    return discounted_strike * norm.cdf(-d2) - spot * norm.cdf(-d1)


def implied_volatility(spot: float, strike: float, years: float, mid: float, option_type: str) -> float | None:
    if mid <= option_intrinsic(spot, strike, option_type):
        return None

    def error(vol: float) -> float:
        return black_scholes_price(spot, strike, years, vol, option_type) - mid

    try:
        return brentq(error, 0.0001, 5.0, maxiter=60)
    except ValueError:
        return None


def black_scholes_gamma(spot: float, strike: float, years: float, vol: float) -> float:
    if spot <= 0 or strike <= 0 or years <= 0 or vol <= 0:
        return 0.0
    risk_free_rate = _configured_risk_free_rate()
    d1 = (math.log(spot / strike) + (risk_free_rate + 0.5 * vol * vol) * years) / (vol * math.sqrt(years))
    return norm.pdf(d1) / (spot * vol * math.sqrt(years))


def _black_scholes_price_array(
    spot: float,
    strikes: np.ndarray,
    years: np.ndarray,
    vols: np.ndarray,
    is_call: np.ndarray,
    risk_free_rate: float,
) -> np.ndarray:
    """Vectorized Black-Scholes prices for already validated fixed arrays."""
    sqrt_years = np.sqrt(years)
    d1 = (
        np.log(spot / strikes)
        + (risk_free_rate + 0.5 * vols * vols) * years
    ) / (vols * sqrt_years)
    d2 = d1 - vols * sqrt_years
    discounted_strikes = strikes * np.exp(-risk_free_rate * years)
    call_prices = spot * ndtr(d1) - discounted_strikes * ndtr(d2)
    put_prices = discounted_strikes * ndtr(-d2) - spot * ndtr(-d1)
    return np.where(is_call, call_prices, put_prices)


def batch_iv_gamma_gex(
    spot: float,
    strikes,
    years,
    mids,
    option_types,
    open_interest,
    *,
    max_iterations: int = 60,
) -> dict[str, np.ndarray]:
    """Solve IV, gamma, and signed GEX over fixed NumPy arrays.

    The validity contract intentionally matches :func:`implied_volatility`:
    prices at or below intrinsic value and prices without a volatility root in
    ``[0.0001, 5.0]`` are rejected. Invalid rows remain in the returned arrays
    as ``NaN`` and are identified by ``valid_mask``. Calls carry positive GEX;
    puts carry negative GEX.

    This pure CPU interface is deliberately independent of pandas and streamer
    state so the same input arrays can be used for isolated CPU-versus-CUDA
    benchmarks without changing the live ingestion path.
    """
    strike_values = np.asarray(strikes, dtype=np.float64)
    year_values = np.asarray(years, dtype=np.float64)
    mid_values = np.asarray(mids, dtype=np.float64)
    option_type_values = np.asarray(option_types, dtype="U1")
    oi_values = np.asarray(open_interest, dtype=np.float64)

    arrays = (strike_values, year_values, mid_values, option_type_values, oi_values)
    if any(values.ndim != 1 for values in arrays):
        raise ValueError("batch IV/GEX inputs must be one-dimensional")
    if len({len(values) for values in arrays}) != 1:
        raise ValueError("batch IV/GEX inputs must have equal lengths")
    if max_iterations < 1:
        raise ValueError("max_iterations must be positive")

    row_count = len(strike_values)
    valid_mask = np.zeros(row_count, dtype=bool)
    iv_values = np.full(row_count, np.nan, dtype=np.float64)
    gamma_values = np.full(row_count, np.nan, dtype=np.float64)
    gex_values = np.full(row_count, np.nan, dtype=np.float64)
    empty_result = {
        "valid_mask": valid_mask,
        "iv": iv_values,
        "gamma": gamma_values,
        "gex": gex_values,
    }
    if row_count == 0:
        return empty_result

    try:
        spot_value = float(spot)
    except (TypeError, ValueError, OverflowError):
        spot_value = math.nan
    if not math.isfinite(spot_value) or spot_value <= 0:
        return empty_result

    is_call = option_type_values == "C"
    is_put = option_type_values == "P"
    finite_inputs = (
        np.isfinite(strike_values)
        & np.isfinite(year_values)
        & np.isfinite(mid_values)
        & np.isfinite(oi_values)
    )
    base_valid = (
        finite_inputs
        & (strike_values > 0.0)
        & (year_values > 0.0)
        & (is_call | is_put)
    )
    intrinsic_values = np.where(
        is_call,
        np.maximum(spot_value - strike_values, 0.0),
        np.maximum(strike_values - spot_value, 0.0),
    )
    candidate_indices = np.flatnonzero(base_valid & (mid_values > intrinsic_values))
    if candidate_indices.size == 0:
        return empty_result

    candidate_strikes = strike_values[candidate_indices]
    candidate_years = year_values[candidate_indices]
    candidate_mids = mid_values[candidate_indices]
    candidate_is_call = is_call[candidate_indices]
    risk_free_rate = _configured_risk_free_rate()
    lower_vols = np.full(candidate_indices.size, 0.0001, dtype=np.float64)
    upper_vols = np.full(candidate_indices.size, 5.0, dtype=np.float64)
    with np.errstate(divide="ignore", invalid="ignore", over="ignore", under="ignore"):
        lower_errors = _black_scholes_price_array(
            spot_value,
            candidate_strikes,
            candidate_years,
            lower_vols,
            candidate_is_call,
            risk_free_rate,
        ) - candidate_mids
        upper_errors = _black_scholes_price_array(
            spot_value,
            candidate_strikes,
            candidate_years,
            upper_vols,
            candidate_is_call,
            risk_free_rate,
        ) - candidate_mids

    bracket_mask = (
        np.isfinite(lower_errors)
        & np.isfinite(upper_errors)
        & (lower_errors <= 0.0)
        & (upper_errors >= 0.0)
    )
    solved_indices = candidate_indices[bracket_mask]
    if solved_indices.size == 0:
        return empty_result

    solve_strikes = candidate_strikes[bracket_mask]
    solve_years = candidate_years[bracket_mask]
    solve_mids = candidate_mids[bracket_mask]
    solve_is_call = candidate_is_call[bracket_mask]
    low = lower_vols[bracket_mask]
    high = upper_vols[bracket_mask]
    low_at_root = lower_errors[bracket_mask] == 0.0
    high_at_root = upper_errors[bracket_mask] == 0.0
    active = ~(low_at_root | high_at_root)

    if np.any(active):
        # Keep the error-state context outside the fixed iteration loop. Entering
        # and leaving it once per bisection step was measurable overhead at the
        # live batch sizes, while ``active`` itself is intentionally constant.
        with np.errstate(divide="ignore", invalid="ignore", over="ignore", under="ignore"):
            for _ in range(max_iterations):
                midpoint = (low + high) * 0.5
                midpoint_errors = _black_scholes_price_array(
                    spot_value,
                    solve_strikes,
                    solve_years,
                    midpoint,
                    solve_is_call,
                    risk_free_rate,
                ) - solve_mids
                move_high = active & (midpoint_errors >= 0.0)
                move_low = active & ~move_high
                high = np.where(move_high, midpoint, high)
                low = np.where(move_low, midpoint, low)

    solved_vols = np.where(
        low_at_root,
        0.0001,
        np.where(high_at_root, 5.0, (low + high) * 0.5),
    )
    sqrt_years = np.sqrt(solve_years)
    with np.errstate(divide="ignore", invalid="ignore", over="ignore", under="ignore"):
        d1 = (
            np.log(spot_value / solve_strikes)
            + (risk_free_rate + 0.5 * solved_vols * solved_vols) * solve_years
        ) / (solved_vols * sqrt_years)
        solved_gammas = (
            np.exp(-0.5 * d1 * d1)
            / math.sqrt(2.0 * math.pi)
            / (spot_value * solved_vols * sqrt_years)
        )
    solved_gex = (
        np.where(solve_is_call, 1.0, -1.0)
        * solved_gammas
        * oi_values[solved_indices]
        * CONTRACT_MULTIPLIER
    )
    finite_results = (
        np.isfinite(solved_vols)
        & np.isfinite(solved_gammas)
        & np.isfinite(solved_gex)
    )
    final_indices = solved_indices[finite_results]
    valid_mask[final_indices] = True
    iv_values[final_indices] = solved_vols[finite_results]
    gamma_values[final_indices] = solved_gammas[finite_results]
    gex_values[final_indices] = solved_gex[finite_results]
    return empty_result


def gex_invariant_errors(
    call_gex_total: float,
    put_gex_total: float,
    gross_gex: float,
    net_gex: float,
) -> list[str]:
    """Return canonical GEX invariant failures; an empty list means valid."""
    values = {
        "call_gex_total": call_gex_total,
        "put_gex_total": put_gex_total,
        "gross_gex": gross_gex,
        "net_gex": net_gex,
    }
    errors = [f"{name} is not finite" for name, value in values.items() if not math.isfinite(value)]
    if errors:
        return errors

    errors.extend(
        f"{name} must be non-negative"
        for name in ("call_gex_total", "put_gex_total", "gross_gex")
        if values[name] < 0
    )
    tolerance = max(1e-6, gross_gex * 1e-9)
    expected_gross = call_gex_total + put_gex_total
    expected_net = call_gex_total - put_gex_total
    if not math.isclose(gross_gex, expected_gross, rel_tol=1e-9, abs_tol=tolerance):
        errors.append(f"gross_gex {gross_gex} != call_gex_total + put_gex_total {expected_gross}")
    if not math.isclose(net_gex, expected_net, rel_tol=1e-9, abs_tol=tolerance):
        errors.append(f"net_gex {net_gex} != call_gex_total - put_gex_total {expected_net}")
    if gross_gex + tolerance < abs(net_gex):
        errors.append(f"gross_gex {gross_gex} < abs(net_gex) {abs(net_gex)}")
    return errors


def pin_competition_metrics(
    ranked_gex_by_strike: list[tuple[float, float]],
    *,
    contested_lead_ratio: float = PIN_CONTESTED_LEAD_RATIO,
) -> dict[str, object]:
    """Describe how decisively the raw gamma pin leads its runner-up.

    The primary pin remains the strike with maximum absolute net GEX.  These
    metrics expose a near tie without smoothing, suppressing, or rewriting that
    raw result.
    """
    threshold = min(1.0, max(0.0, float(contested_lead_ratio)))
    if not ranked_gex_by_strike:
        return {
            "pin_runner_up_strike": None,
            "pin_runner_up_abs_gex": None,
            "pin_lead_abs_gex": None,
            "pin_lead_ratio": None,
            "pin_competition_threshold": threshold,
            "pin_is_contested": False,
            "pin_competition_reason": None,
            "pin_competition_formula_version": "pin-competition-v1-top-two-abs-net-gex",
        }

    primary_strike, primary_gex = ranked_gex_by_strike[0]
    primary_abs = abs(float(primary_gex))
    if len(ranked_gex_by_strike) < 2:
        runner_strike = None
        runner_abs = None
        lead_abs = primary_abs
        lead_ratio = 1.0 if primary_abs > 0 else None
    else:
        runner_strike, runner_gex = ranked_gex_by_strike[1]
        runner_abs = abs(float(runner_gex))
        lead_abs = max(0.0, primary_abs - runner_abs)
        lead_ratio = lead_abs / primary_abs if primary_abs > 0 else None

    contested = bool(
        runner_strike is not None
        and lead_ratio is not None
        and lead_ratio <= threshold
    )
    reason = None
    if contested:
        reason = (
            f"PIN_CONTESTED: {float(primary_strike):g} leads "
            f"{float(runner_strike):g} by {lead_ratio * 100.0:.1f}% of primary |net GEX| "
            f"(threshold {threshold * 100.0:.1f}%)"
        )
    return {
        "pin_runner_up_strike": float(runner_strike) if runner_strike is not None else None,
        "pin_runner_up_abs_gex": runner_abs,
        "pin_lead_abs_gex": lead_abs,
        "pin_lead_ratio": lead_ratio,
        "pin_competition_threshold": threshold,
        "pin_is_contested": contested,
        "pin_competition_reason": reason,
        "pin_competition_formula_version": "pin-competition-v1-top-two-abs-net-gex",
    }


def expiration_blend_weights(expiration_profiles: list[dict]) -> list[float]:
    """Return normalized weights while assigning each expiry bucket only once."""
    bucket_weights = {"0DTE": 0.7, "1-3DTE": 0.2, "4-45DTE": 0.1}
    raw_weights = [0.0] * len(expiration_profiles)
    present_bucket_weight = 0.0

    for bucket, bucket_weight in bucket_weights.items():
        indexes = [
            index
            for index, profile in enumerate(expiration_profiles)
            if profile.get("bucket") == bucket
        ]
        if not indexes:
            continue
        present_bucket_weight += bucket_weight
        magnitudes = [max(float(expiration_profiles[index].get("gross_gex") or 0.0), 0.0) for index in indexes]
        magnitude_total = sum(magnitudes)
        if magnitude_total > 0:
            for index, magnitude in zip(indexes, magnitudes):
                raw_weights[index] = bucket_weight * magnitude / magnitude_total
        else:
            even_weight = bucket_weight / len(indexes)
            for index in indexes:
                raw_weights[index] = even_weight

    if present_bucket_weight <= 0:
        return raw_weights
    return [weight / present_bucket_weight for weight in raw_weights]


def _subscription_bound_values() -> dict[str, object]:
    return {
        "primary_max_strike_pairs": PRIMARY_MAX_STRIKE_PAIRS,
        "shadow_max_strike_pairs": SHADOW_MAX_STRIKE_PAIRS,
        "minimum_primary_strike_pairs": MIN_PRIMARY_STRIKE_PAIRS,
        "minimum_next_listed_strike_pairs": MIN_NEXT_LISTED_STRIKE_PAIRS,
        "max_contracts_per_market": MAX_CONTRACTS_PER_MARKET,
        "max_subscription_contracts": MAX_SUBSCRIPTION_CONTRACTS,
        "center_pair_fraction": SUBSCRIPTION_CENTER_PAIR_FRACTION,
        "pair_selection_method": "complete-pairs-v1-central-plus-open-interest",
        "equity_addition_allocation": "residual-after-existing-index-pairs-v1",
    }


def _option_root_from_symbol(raw_symbol: object) -> str:
    text = str(raw_symbol or "").strip().upper()
    if not text:
        return ""
    first_token = text.split()[0]
    return first_token.split("-", 1)[0]


def _select_complete_strike_pairs(
    expiration_frame: pd.DataFrame,
    *,
    pair_limit: int,
) -> tuple[pd.DataFrame, dict[str, object]]:
    """Bound one expiry to complete call/put pairs without static strike ranges."""
    if expiration_frame.empty:
        return expiration_frame.copy(), {
            "available_strike_pairs": 0,
            "selected_strike_pairs": 0,
            "incomplete_strikes_dropped": 0,
            "selection_center_strike": None,
            "pair_limit": int(pair_limit),
        }

    working = expiration_frame.copy()
    if "_root" not in working.columns:
        working["_root"] = working["symbol"].map(_option_root_from_symbol)
    working["option_type"] = working["option_type"].astype(str).str.upper()
    working = working[working["option_type"].isin(["C", "P"])].copy()
    working = working.sort_values(
        ["open_interest", "symbol"], ascending=[False, True], kind="stable"
    ).drop_duplicates(subset=["_root", "strike", "option_type"], keep="first")
    working["_pair_key"] = (
        working["_root"].astype(str) + "|" + working["strike"].astype(str)
    )
    coverage = working.groupby("_pair_key")["option_type"].nunique()
    complete_pair_keys = coverage[coverage == 2].index
    incomplete_count = int((coverage != 2).sum())
    complete = working[working["_pair_key"].isin(complete_pair_keys)].copy()
    if complete.empty:
        return complete, {
            "available_strike_pairs": 0,
            "selected_strike_pairs": 0,
            "incomplete_strikes_dropped": incomplete_count,
            "selection_center_strike": None,
            "pair_limit": int(pair_limit),
        }

    pair_stats = (
        complete.groupby(["_pair_key", "strike"], as_index=False)["open_interest"].sum()
        .sort_values("strike", kind="stable")
    )
    oi_weights = pair_stats["open_interest"].clip(lower=0.0)
    if float(oi_weights.sum()) > 0:
        cumulative = oi_weights.cumsum()
        center_index = int(np.searchsorted(cumulative.to_numpy(), float(oi_weights.sum()) / 2.0))
        center_index = min(center_index, len(pair_stats) - 1)
        center_strike = float(pair_stats.iloc[center_index]["strike"])
    else:
        center_strike = float(pair_stats["strike"].median())

    available_pairs = int(len(pair_stats))
    limit = min(max(1, int(pair_limit)), available_pairs)
    central_count = min(limit, max(1, int(math.ceil(limit * SUBSCRIPTION_CENTER_PAIR_FRACTION))))
    central = pair_stats.assign(
        _distance=(pair_stats["strike"].astype(float) - center_strike).abs()
    ).sort_values(["_distance", "open_interest", "strike"], ascending=[True, False, True])
    selected_pair_keys = central.head(central_count)["_pair_key"].tolist()
    remaining = pair_stats[~pair_stats["_pair_key"].isin(selected_pair_keys)].sort_values(
        ["open_interest", "strike"], ascending=[False, True], kind="stable"
    )
    selected_pair_keys.extend(
        remaining.head(limit - len(selected_pair_keys))["_pair_key"].tolist()
    )
    pair_rank = {str(pair_key): rank for rank, pair_key in enumerate(selected_pair_keys)}
    selected = complete[complete["_pair_key"].isin(selected_pair_keys)].copy()
    selected["_pair_rank"] = selected["_pair_key"].astype(str).map(pair_rank).astype(int)
    selected = selected.sort_values(["_pair_rank", "option_type", "symbol"], kind="stable")
    return selected, {
        "available_strike_pairs": available_pairs,
        "selected_strike_pairs": int(len(selected_pair_keys)),
        "incomplete_strikes_dropped": incomplete_count,
        "selection_center_strike": center_strike,
        "pair_limit": int(pair_limit),
    }


def _active_pair_universe(universe: pd.DataFrame) -> pd.DataFrame:
    """Keep both definition legs when a complete pair has OI on either leg."""
    if universe.empty:
        return universe.copy()
    frame = universe.copy()
    frame["open_interest"] = pd.to_numeric(frame["open_interest"], errors="coerce").fillna(0.0)
    if "_root" not in frame.columns:
        frame["_root"] = frame["symbol"].map(_option_root_from_symbol)
    pair_keys = ["market", "_root", "expiration_date", "strike"]
    pair_types = frame.groupby(pair_keys)["option_type"].transform(
        lambda values: {"C", "P"}.issubset({str(value).upper() for value in values})
    )
    pair_has_oi = frame.groupby(pair_keys)["open_interest"].transform("max") > 0
    return frame[pair_types & pair_has_oi].copy()


def select_subscription_universe(
    universe: pd.DataFrame,
    market_order: list[str],
    profile: str = SUBSCRIPTION_PROFILE,
    as_of: date | None = None,
) -> tuple[pd.DataFrame, dict[str, object]]:
    """Build a dynamically centered, pair-complete and hard-bounded quote plan.

    The full OI universe is intentionally an input, not the live subscription.
    Primary expiry pairs are ordered first, followed by the mandatory next
    listed expiry, then optional OI/bucket shadows. Bounds select strikes from
    current definitions and OI, so no stale static SPX/NDX strike range is used.
    """
    normalized_profile = str(profile or "").strip().lower()
    if normalized_profile not in SUPPORTED_SUBSCRIPTION_PROFILES:
        raise ValueError(
            f"Unsupported Databento subscription profile {profile!r}; "
            f"expected one of {sorted(SUPPORTED_SUBSCRIPTION_PROFILES)}"
        )
    bounds = _subscription_bound_values()
    empty_metadata = {
        "profile": normalized_profile,
        "full_contract_count": 0,
        "selected_contract_count": 0,
        "expired_contract_count": 0,
        "settlement_ineligible_contract_count": 0,
        "zero_oi_contract_count": 0,
        "beyond_expiration_window_contract_count": 0,
        "markets": {},
        "bounds": bounds,
        "global_cap_applied": False,
        "primary_cap_override": False,
        "selected_universe_sha256": None,
    }
    if universe.empty:
        return universe.copy(), empty_metadata

    required = {"market", "symbol", "expiration_date", "option_type", "strike", "open_interest"}
    missing = sorted(required.difference(universe.columns))
    if missing:
        raise ValueError(f"Databento universe is missing required columns: {missing}")

    today = as_of or current_market_date()
    full = universe.copy()
    full["expiration_date"] = pd.to_datetime(full["expiration_date"], errors="coerce").dt.date
    full["strike"] = pd.to_numeric(full["strike"], errors="coerce")
    full = full.dropna(subset=["market", "symbol", "expiration_date", "option_type", "strike"])
    full = full.drop_duplicates(subset=["symbol"], keep="last").copy()
    full["open_interest"] = pd.to_numeric(full["open_interest"], errors="coerce").fillna(0.0)
    calendar_unexpired = full[full["expiration_date"] >= today].copy()
    expired_contract_count = int(len(full) - len(calendar_unexpired))
    settlement_ineligible_mask = calendar_unexpired.apply(
        lambda row: not _expiration_is_live_eligible(
            row["market"], row["expiration_date"], today
        ),
        axis=1,
    ).astype(bool)
    settlement_ineligible = calendar_unexpired[settlement_ineligible_mask].copy()
    settlement_ineligible_contract_count = int(len(settlement_ineligible))
    live_eligible = calendar_unexpired[~settlement_ineligible_mask].copy()
    zero_oi_contract_count = int((live_eligible["open_interest"] <= 0).sum())
    full = _active_pair_universe(live_eligible)
    full["_dte"] = full["expiration_date"].map(lambda expiration: (expiration - today).days)
    beyond_expiration_window_contract_count = int(
        (full["_dte"] > MULTI_EXPIRATION_MAX_DTE).sum()
    )
    full = full[full["_dte"] <= MULTI_EXPIRATION_MAX_DTE].copy()

    market_rank = {market: index for index, market in enumerate(market_order)}
    selected_parts: list[pd.DataFrame] = []
    market_metadata: dict[str, dict[str, object]] = {}
    planned_by_market: dict[str, list[dict[str, object]]] = {}

    for market in market_order:
        market_frame = full[full["market"] == market].copy()
        market_live_eligible = live_eligible[live_eligible["market"] == market]
        market_settlement_ineligible = settlement_ineligible[
            settlement_ineligible["market"] == market
        ]
        settlement_exclusions = [
            {
                "expiration": expiration.isoformat(),
                "contracts": int(len(expiration_rows)),
                "reason": VIX_SETTLEMENT_INELIGIBLE_REASON,
            }
            for expiration, expiration_rows in market_settlement_ineligible.groupby(
                "expiration_date", sort=True
            )
        ]
        if market_frame.empty:
            if not settlement_exclusions:
                continue
            planned_by_market[market] = []
            market_metadata[market] = {
                "full_contract_count": 0,
                "selected_contract_count_before_global_cap": 0,
                "selected_contract_count": 0,
                "market_contract_cap": MAX_CONTRACTS_PER_MARKET,
                "market_cap_applied": False,
                "market_reservation_pairs_requested": 0,
                "market_reservation_pairs_retained": 0,
                "market_reservation_shortfall_pairs": 0,
                "primary_reserved_pairs_retained": 0,
                "next_listed_reserved_pairs_retained": 0,
                "selected_expirations": [],
                "settlement_ineligible_contract_count": int(
                    len(market_settlement_ineligible)
                ),
                "settlement_ineligible_expirations": settlement_exclusions,
                "primary_expiration_unavailable_reason": (
                    "NO_FORWARD_VIX_EXPIRATION_AVAILABLE"
                    if market_live_eligible.empty
                    else "NO_ACTIVE_FORWARD_VIX_PAIRS_AVAILABLE"
                ),
                **{
                    f"primary_expiration_{key}": value
                    for key, value in _expiration_authority_metadata(
                        market, None, today
                    ).items()
                },
            }
            continue
        expiration_stats = (
            market_frame.groupby(["expiration_date", "_dte"], as_index=False)["open_interest"]
            .sum()
            .sort_values("expiration_date", kind="stable")
            .reset_index(drop=True)
        )
        expiration_records = expiration_stats.to_dict(orient="records")
        primary_expiration = expiration_records[0]["expiration_date"]
        selected_expirations: list[tuple[date, int, str]] = [(primary_expiration, 0, "primary")]

        if normalized_profile == "full":
            selected_expirations.extend(
                (record["expiration_date"], index, "next-listed" if index == 1 else "full")
                for index, record in enumerate(expiration_records[1:], start=1)
            )
        elif normalized_profile == "near-term-shadow":
            future_records = [
                record for record in expiration_records if record["expiration_date"] > primary_expiration
            ]
            if future_records:
                selected_expirations.append((future_records[0]["expiration_date"], 1, "next-listed"))
            already = {expiration for expiration, _, _ in selected_expirations}
            near = [
                record
                for record in expiration_records
                if 1 <= int(record["_dte"]) <= 3 and record["expiration_date"] not in already
            ]
            if near:
                representative = sorted(
                    near, key=lambda record: (-float(record["open_interest"]), record["expiration_date"])
                )[0]
                selected_expirations.append((representative["expiration_date"], 2, "1-3DTE-highest-OI"))
            already = {expiration for expiration, _, _ in selected_expirations}
            later = [
                record
                for record in expiration_records
                if 4 <= int(record["_dte"]) <= MULTI_EXPIRATION_MAX_DTE
                and record["expiration_date"] not in already
            ]
            if later:
                selected_expirations.append((later[0]["expiration_date"], 3, "4-45DTE"))

        expiry_entries: list[dict[str, object]] = []
        market_parts: list[pd.DataFrame] = []
        for expiration, stage, role in selected_expirations:
            expiry_frame = market_frame[market_frame["expiration_date"] == expiration].copy()
            bounded, pair_metadata = _select_complete_strike_pairs(
                expiry_frame,
                pair_limit=PRIMARY_MAX_STRIKE_PAIRS if stage == 0 else SHADOW_MAX_STRIKE_PAIRS,
            )
            if bounded.empty:
                continue
            bounded["_stage_rank"] = int(stage)
            bounded["_expiry_role"] = role
            bounded["_market_rank"] = market_rank.get(market, len(market_rank))
            market_parts.append(bounded)
            expiry_entries.append(
                {
                    "expiration": expiration.isoformat(),
                    "stage": int(stage),
                    "role": role,
                    "contracts_before_market_and_global_caps": int(len(bounded)),
                    **_expiration_authority_metadata(
                        market, expiration, today, role=role
                    ),
                    **pair_metadata,
                }
            )

        market_reservation_requested = 0
        market_reservation_retained = 0
        if market_parts:
            market_selected = pd.concat(market_parts, ignore_index=True)
            pair_table = market_selected[
                ["expiration_date", "_pair_key", "strike", "_stage_rank", "_expiry_role", "_pair_rank"]
            ].drop_duplicates(subset=["expiration_date", "_pair_key"])
            market_pair_cap = max(1, MAX_CONTRACTS_PER_MARKET // 2)
            market_reserved = pair_table[
                (
                    (pair_table["_stage_rank"] == 0)
                    & (pair_table["_pair_rank"] < MIN_PRIMARY_STRIKE_PAIRS)
                )
                | (
                    (pair_table["_expiry_role"] == "next-listed")
                    & (pair_table["_pair_rank"] < MIN_NEXT_LISTED_STRIKE_PAIRS)
                )
            ].copy()
            market_reservation_requested = int(len(market_reserved))
            market_reserved["_reservation_role_rank"] = np.where(
                market_reserved["_expiry_role"] == "next-listed", 1, 0
            )
            market_reserved = market_reserved.sort_values(
                ["_pair_rank", "_reservation_role_rank", "expiration_date", "strike"],
                kind="stable",
            ).head(market_pair_cap)
            market_reservation_retained = int(len(market_reserved))
            market_reserved_keys = market_reserved[["expiration_date", "_pair_key"]].assign(
                _reserved=True
            )
            market_remaining = pair_table.merge(
                market_reserved_keys,
                on=["expiration_date", "_pair_key"],
                how="left",
            )
            market_remaining = market_remaining[
                market_remaining["_reserved"].isna()
            ].drop(columns=["_reserved"])
            market_remaining = market_remaining.sort_values(
                ["_stage_rank", "_pair_rank", "expiration_date", "strike"], kind="stable"
            )
            keep_pairs = pd.concat(
                [
                    market_reserved[["expiration_date", "_pair_key"]],
                    market_remaining.head(max(0, market_pair_cap - len(market_reserved)))[
                        ["expiration_date", "_pair_key"]
                    ],
                ],
                ignore_index=True,
            )
            market_selected = market_selected.merge(
                keep_pairs.assign(_keep_pair=True), on=["expiration_date", "_pair_key"], how="inner"
            ).drop(columns=["_keep_pair"])
            selected_parts.append(market_selected)
        else:
            market_selected = market_frame.iloc[0:0].copy()
        planned_by_market[market] = expiry_entries
        primary_entry = next(
            (entry for entry in expiry_entries if entry.get("role") == "primary"),
            {},
        )
        market_metadata[market] = {
            "full_contract_count": int(len(market_frame)),
            "selected_contract_count_before_global_cap": int(len(market_selected)),
            "market_contract_cap": MAX_CONTRACTS_PER_MARKET,
            "market_cap_applied": bool(sum(entry["contracts_before_market_and_global_caps"] for entry in expiry_entries) > len(market_selected)),
            "market_reservation_pairs_requested": market_reservation_requested,
            "market_reservation_pairs_retained": market_reservation_retained,
            "market_reservation_shortfall_pairs": max(
                0, market_reservation_requested - market_reservation_retained
            ),
            "selected_expirations": expiry_entries,
            "settlement_ineligible_contract_count": int(
                len(market_settlement_ineligible)
            ),
            "settlement_ineligible_expirations": settlement_exclusions,
            "primary_expiration_authority": primary_entry.get("authority"),
            "primary_expiration_context_only": bool(
                primary_entry.get("context_only")
            ),
            "primary_expiration_same_day_authority": bool(
                primary_entry.get("same_day_authority")
            ),
            "primary_expiration_selection_basis": primary_entry.get(
                "selection_basis"
            ),
        }

    if not selected_parts:
        empty = full.drop(columns=["_dte"], errors="ignore").iloc[0:0].copy()
        return empty, {
            **empty_metadata,
            "full_contract_count": int(len(full)),
            "expired_contract_count": expired_contract_count,
            "settlement_ineligible_contract_count": (
                settlement_ineligible_contract_count
            ),
            "zero_oi_contract_count": zero_oi_contract_count,
            "beyond_expiration_window_contract_count": beyond_expiration_window_contract_count,
            "markets": market_metadata,
        }

    selected = pd.concat(selected_parts, ignore_index=True)
    pair_table = selected[
        [
            "market", "expiration_date", "_pair_key", "strike", "_stage_rank", "_expiry_role",
            "_pair_rank", "_market_rank",
        ]
    ].drop_duplicates(subset=["market", "expiration_date", "_pair_key"])
    pair_table = pair_table.sort_values(
        ["_stage_rank", "_pair_rank", "_market_rank", "expiration_date", "strike"], kind="stable"
    )
    configured_pair_cap = max(1, MAX_SUBSCRIPTION_CONTRACTS // 2)
    primary_reservation = (
        (pair_table["_stage_rank"] == 0)
        & (pair_table["_pair_rank"] < MIN_PRIMARY_STRIKE_PAIRS)
    )
    next_reservation = (
        (pair_table["_expiry_role"] == "next-listed")
        & (pair_table["_pair_rank"] < MIN_NEXT_LISTED_STRIKE_PAIRS)
    )
    reserved = pair_table[primary_reservation | next_reservation].copy()
    reserved["_reservation_role_rank"] = np.where(
        reserved["_expiry_role"] == "next-listed", 1, 0
    )
    reserved = reserved.sort_values(
        ["_pair_rank", "_reservation_role_rank", "_market_rank"], kind="stable"
    )
    # ETF option additions spend residual capacity only. In particular adding
    # SPY/QQQ/DIA/IWM cannot displace any previously selected SPX/NDX primary,
    # next-listed or shadow pair. Equities context uses no OPRA capacity.
    protect_existing = bool(set(market_order).intersection({"SPX", "NDX"}))
    if protect_existing:
        existing_reserved = reserved[~reserved["market"].isin(OPTION_ETF_SYMBOLS)].head(configured_pair_cap)
        existing_keys = existing_reserved[["market", "expiration_date", "_pair_key"]].assign(_reserved=True)
        existing_remaining = pair_table[~pair_table["market"].isin(OPTION_ETF_SYMBOLS)].merge(
            existing_keys, on=["market", "expiration_date", "_pair_key"], how="left"
        )
        existing_remaining = existing_remaining[existing_remaining["_reserved"].isna()].drop(columns=["_reserved"])
        existing_remaining = existing_remaining.sort_values(
            ["_stage_rank", "_pair_rank", "_market_rank", "expiration_date", "strike"], kind="stable"
        ).head(max(0, configured_pair_cap - len(existing_reserved)))
        residual = max(0, configured_pair_cap - len(existing_reserved) - len(existing_remaining))
        reserved = pd.concat([
            existing_reserved,
            reserved[reserved["market"].isin(OPTION_ETF_SYMBOLS)].head(residual),
        ], ignore_index=True)
    else:
        reserved = reserved.head(configured_pair_cap)
    reserved_keys = reserved[["market", "expiration_date", "_pair_key"]].assign(_reserved=True)
    remaining = pair_table.merge(
        reserved_keys,
        on=["market", "expiration_date", "_pair_key"],
        how="left",
    )
    remaining = remaining[remaining["_reserved"].isna()].drop(columns=["_reserved"])
    remaining = remaining.sort_values(
        ["_stage_rank", "_pair_rank", "_market_rank", "expiration_date", "strike"],
        kind="stable",
    )
    if protect_existing:
        remaining = pd.concat([
            remaining[~remaining["market"].isin(OPTION_ETF_SYMBOLS)],
            remaining[remaining["market"].isin(OPTION_ETF_SYMBOLS)],
        ], ignore_index=True)
    remaining_capacity = max(0, configured_pair_cap - len(reserved))
    kept_pair_table = pd.concat(
        [reserved.drop(columns=["_reservation_role_rank"], errors="ignore"), remaining.head(remaining_capacity)],
        ignore_index=True,
    )
    keep_pairs = kept_pair_table[["market", "expiration_date", "_pair_key"]]
    selected = selected.merge(
        keep_pairs.assign(_keep_pair=True),
        on=["market", "expiration_date", "_pair_key"],
        how="inner",
    ).drop(columns=["_keep_pair"])
    selected["_sequence_within_market"] = selected.groupby(["_stage_rank", "market"]).cumcount()
    selected = selected.sort_values(
        ["_stage_rank", "_sequence_within_market", "_market_rank"], kind="stable"
    ).reset_index(drop=True)

    for market, metadata in market_metadata.items():
        market_selected = selected[selected["market"] == market]
        metadata["selected_contract_count"] = int(len(market_selected))
        finalized_expirations: list[dict[str, object]] = []
        for entry in planned_by_market.get(market, []):
            expiration = date.fromisoformat(str(entry["expiration"]))
            expiration_rows = market_selected[market_selected["expiration_date"] == expiration]
            if expiration_rows.empty:
                continue
            finalized = dict(entry)
            finalized["contracts"] = int(len(expiration_rows))
            finalized["selected_strike_pairs"] = int(expiration_rows["_pair_key"].nunique())
            finalized_expirations.append(finalized)
        metadata["selected_expirations"] = finalized_expirations
        primary_entry = next((entry for entry in finalized_expirations if entry.get("role") == "primary"), {})
        next_entry = next((entry for entry in finalized_expirations if entry.get("role") == "next-listed"), {})
        metadata["primary_reserved_pairs_retained"] = int(primary_entry.get("selected_strike_pairs") or 0)
        metadata["next_listed_reserved_pairs_retained"] = int(next_entry.get("selected_strike_pairs") or 0)
        metadata["primary_expiration_authority"] = primary_entry.get("authority")
        metadata["primary_expiration_context_only"] = bool(
            primary_entry.get("context_only")
        )
        metadata["primary_expiration_same_day_authority"] = bool(
            primary_entry.get("same_day_authority")
        )
        metadata["primary_expiration_selection_basis"] = primary_entry.get(
            "selection_basis"
        )

    selected_symbols = selected["symbol"].astype(str).tolist()
    selected_hash = hashlib.sha256("\n".join(selected_symbols).encode("utf-8")).hexdigest()
    selected = selected.drop(
        columns=[
            "_dte", "_root", "_pair_key", "_stage_rank", "_expiry_role", "_market_rank", "_pair_rank",
            "_sequence_within_market",
        ],
        errors="ignore",
    )
    return selected, {
        "profile": normalized_profile,
        "full_contract_count": int(len(full)),
        "selected_contract_count": int(len(selected)),
        "expired_contract_count": expired_contract_count,
        "settlement_ineligible_contract_count": settlement_ineligible_contract_count,
        "zero_oi_contract_count": zero_oi_contract_count,
        "beyond_expiration_window_contract_count": beyond_expiration_window_contract_count,
        "markets": market_metadata,
        "bounds": bounds,
        "global_cap_applied": bool(len(pair_table) > configured_pair_cap),
        "primary_cap_override": False,
        "reservation_pairs_requested": int((primary_reservation | next_reservation).sum()),
        "reservation_pairs_retained": int(len(reserved)),
        "reservation_shortfall_pairs": max(
            0, int((primary_reservation | next_reservation).sum()) - int(len(reserved))
        ),
        "selected_universe_sha256": selected_hash,
    }


def gamma_distance_profile(gex_by_strike: dict[float, float], spot: float) -> dict[str, float]:
    """Summarize absolute strike-level GEX by distance from spot."""
    buckets = {"ATM": 0.0, "NEAR": 0.0, "MID": 0.0, "FAR": 0.0}
    if spot <= 0:
        return buckets
    for strike, net_gex in gex_by_strike.items():
        distance = abs(float(strike) - spot) / spot
        bucket = "ATM" if distance < 0.002 else "NEAR" if distance < 0.01 else "MID" if distance < 0.03 else "FAR"
        buckets[bucket] += abs(float(net_gex))
    total = sum(buckets.values())
    if total <= 0:
        return buckets
    return {bucket: round(value / total * 100.0, 1) for bucket, value in buckets.items()}


def zero_gamma_level(gex_by_strike: dict[float, float]) -> float | None:
    strikes = sorted(gex_by_strike)
    for lower, upper in zip(strikes, strikes[1:]):
        lower_gex = gex_by_strike[lower]
        upper_gex = gex_by_strike[upper]
        if lower_gex * upper_gex < 0:
            return lower + (upper - lower) * abs(lower_gex) / (abs(lower_gex) + abs(upper_gex))
    return None


def gamma_structure_inference_features(
    *,
    spot: float,
    zero_gamma: float | None,
    positive_gex_wall: float | None,
    negative_gex_wall: float | None,
    top_strike_concentration: float | None,
) -> dict[str, float | None]:
    """Return scale-stable, point-in-time features from strike-level net GEX.

    ``zero_gamma_distance`` is signed relative distance from spot to the first
    strike-ordered adjacent sign crossing. ``wall_asymmetry`` compares the
    absolute spot distances of the strongest positive and negative GEX walls;
    positive values mean the positive wall is farther away. Concentration is
    the leading strike's absolute net GEX divided by primary-expiry gross GEX.
    """
    normalized_spot = float(spot)
    if not math.isfinite(normalized_spot) or normalized_spot <= 0:
        return {
            "zero_gamma_distance": None,
            "wall_asymmetry": None,
            "top_strike_concentration": None,
        }

    zero_gamma_distance = None
    if zero_gamma is not None and math.isfinite(float(zero_gamma)):
        zero_gamma_distance = (float(zero_gamma) - normalized_spot) / normalized_spot

    wall_asymmetry = None
    if (
        positive_gex_wall is not None
        and negative_gex_wall is not None
        and math.isfinite(float(positive_gex_wall))
        and math.isfinite(float(negative_gex_wall))
    ):
        positive_distance = abs(float(positive_gex_wall) - normalized_spot) / normalized_spot
        negative_distance = abs(normalized_spot - float(negative_gex_wall)) / normalized_spot
        distance_sum = positive_distance + negative_distance
        if distance_sum > 0:
            wall_asymmetry = (positive_distance - negative_distance) / distance_sum

    concentration = None
    if top_strike_concentration is not None:
        candidate = float(top_strike_concentration)
        if math.isfinite(candidate) and 0.0 <= candidate <= 1.0:
            concentration = candidate

    return {
        "zero_gamma_distance": zero_gamma_distance,
        "wall_asymmetry": wall_asymmetry,
        "top_strike_concentration": concentration,
    }


def max_pain(chain: pd.DataFrame) -> float | None:
    settlement_values = np.sort(
        pd.to_numeric(chain.get("strike"), errors="coerce").dropna().unique().astype(np.float64)
    )
    if settlement_values.size == 0:
        return None
    option_types = chain["option_type"].astype(str).str.upper().to_numpy(dtype="U1")
    strike_values = pd.to_numeric(chain["strike"], errors="coerce").to_numpy(dtype=np.float64)
    oi_values = pd.to_numeric(chain["open_interest"], errors="coerce").fillna(0.0).to_numpy(dtype=np.float64)
    call_mask = (option_types == "C") & np.isfinite(strike_values) & np.isfinite(oi_values)
    put_mask = (option_types == "P") & np.isfinite(strike_values) & np.isfinite(oi_values)
    best_settlement: float | None = None
    best_pain = math.inf
    # Chunking bounds temporary matrices while still eliminating per-strike
    # pandas filters and Python row work.
    for start in range(0, len(settlement_values), 512):
        settlements = settlement_values[start:start + 512, np.newaxis]
        call_pain = (
            np.maximum(settlements - strike_values[call_mask][np.newaxis, :], 0.0)
            * oi_values[call_mask][np.newaxis, :]
        ).sum(axis=1)
        put_pain = (
            np.maximum(strike_values[put_mask][np.newaxis, :] - settlements, 0.0)
            * oi_values[put_mask][np.newaxis, :]
        ).sum(axis=1)
        pain = call_pain + put_pain
        local_index = int(np.argmin(pain))
        if float(pain[local_index]) < best_pain:
            best_pain = float(pain[local_index])
            best_settlement = float(settlement_values[start + local_index])
    return best_settlement


def build_full_oi_analytics(
    full_universe: pd.DataFrame,
    market_order: list[str],
    *,
    as_of: date,
    provenance: dict[str, object] | None = None,
) -> dict[str, dict[str, object]]:
    """Precompute quote-independent expiry OI/max-pain metrics once per universe."""
    source = dict(provenance or {})
    source_as_of = (
        source.get("provider_statistics_end")
        or source.get("source_date")
        or as_of.isoformat()
    )
    analytics: dict[str, dict[str, object]] = {}
    if full_universe.empty:
        return analytics
    for market in market_order:
        market_frame = full_universe[full_universe["market"] == market].copy()
        if market_frame.empty:
            continue
        expiration_metrics: list[dict[str, object]] = []
        for expiration, expiration_frame in market_frame.groupby("expiration_date", sort=True):
            option_types = expiration_frame["option_type"].astype(str).str.upper()
            call_oi = float(expiration_frame.loc[option_types == "C", "open_interest"].sum())
            put_oi = float(expiration_frame.loc[option_types == "P", "open_interest"].sum())
            pair_coverage = expiration_frame.groupby("strike")["option_type"].nunique()
            expiration_metrics.append({
                "expiration": expiration.isoformat() if isinstance(expiration, date) else str(expiration),
                "dte": max((expiration - as_of).days, 0) if isinstance(expiration, date) else None,
                "max_pain": max_pain(expiration_frame),
                "call_open_interest": call_oi,
                "put_open_interest": put_oi,
                "total_open_interest": call_oi + put_oi,
                "contracts": int(len(expiration_frame)),
                "strikes": int(expiration_frame["strike"].nunique()),
                "complete_strike_pairs": int((pair_coverage >= 2).sum()),
            })
        analytics[market] = {
            "formula_version": "full-oi-max-pain-v1",
            "source": "databento-statistics-open-interest-full-universe",
            "as_of": str(source_as_of),
            "trading_date": as_of.isoformat(),
            "is_fallback": bool(source.get("is_fallback")),
            "source_date": source.get("source_date"),
            "provider_statistics_end": source.get("provider_statistics_end"),
            "universe_sha256": source.get("source_sha256"),
            "full_contract_count": int(len(market_frame)),
            "expirations": expiration_metrics,
        }
    return analytics


def infer_spot_from_pairs(chain: pd.DataFrame, years: float) -> float | None:
    paired = chain.pivot_table(index="strike", columns="option_type", values="mid", aggfunc="last")
    if "C" not in paired.columns or "P" not in paired.columns:
        return None
    paired = paired.dropna(subset=["C", "P"])
    paired = paired[(paired["C"] > 0) & (paired["P"] > 0)].copy()
    if paired.empty:
        return None
    # European put-call parity (no dividend adjustment):
    #   C - P = S - K*exp(-rT), so S = C - P + K*exp(-rT).
    risk_free_rate = _configured_risk_free_rate()
    paired["spot"] = paired["C"] - paired["P"] + paired.index.to_series().astype(float) * math.exp(-risk_free_rate * years)
    paired["pair_gap"] = (paired["C"] - paired["P"]).abs()
    paired = paired.assign(_strike=paired.index.to_series().astype(float))
    return float(
        paired.sort_values(["pair_gap", "_strike"], kind="stable")
        .head(15)["spot"]
        .median()
    )


class _DatabentoWarningTelemetryHandler(logging.Handler):
    """Mirror Databento SDK overload warnings into in-process health counters."""

    def __init__(self, streamer: "DatabentoGammaStreamer"):
        super().__init__(level=logging.WARNING)
        self._streamer_ref = weakref.ref(streamer)

    def emit(self, record: logging.LogRecord) -> None:
        streamer = self._streamer_ref()
        if streamer is None:
            return
        try:
            streamer.record_provider_warning(record.getMessage())
        except Exception:
            # Telemetry must never break Databento's logging or live reader.
            return


class DatabentoGammaStreamer:
    """Live Databento OPRA streamer compatible with the old backend streamer API."""

    provider_name = "databento"

    def __init__(
        self,
        symbols: list[str] | None = None,
        *,
        subscription_epoch_id: str | None = None,
    ):
        self.symbols = [symbol for symbol in (symbols or ["SPX", "NDX"]) if symbol in MARKETS]
        # Optional/context families must never hold the core handoff in
        # ``warming``.  Preserve the full configured requirement so an omitted
        # or unknown required family cannot be silently intersected away and
        # accidentally promote a partial subscription.
        self.required_handoff_symbols = tuple(
            dict.fromkeys(
                str(symbol).upper().strip()
                for symbol in DATABENTO_REQUIRED_SYMBOLS
                if str(symbol).strip()
            )
        )
        self.missing_required_handoff_symbols = tuple(
            symbol
            for symbol in self.required_handoff_symbols
            if symbol not in self.symbols
        )
        epoch_id = str(subscription_epoch_id or hashlib.sha256(uuid.uuid4().bytes).hexdigest())
        if re.fullmatch(r"[0-9a-f]{64}", epoch_id) is None:
            raise ValueError("subscription_epoch_id must be canonical lowercase SHA-256 hex")
        # ``active_generation`` is connection-local and restarts at zero with
        # every backend process.  The epoch prevents a later process's
        # generation 1 from being aligned with an earlier process's generation
        # 1 on the same trading date.
        self.subscription_epoch_id = epoch_id
        self.api_key = os.getenv("DATABENTO_API_KEY")
        self.is_running = False
        self.callbacks: list[Callable] = []
        self.buffers: dict[str, deque] = {symbol: deque(maxlen=MAX_BUFFER_SIZE) for symbol in self.symbols}
        self.latest_pins: dict[str, dict] = {}
        self.id_to_symbol: dict[int, str] = {}
        self.symbol_mappings: dict[int, dict[str, object]] = {}
        self.quotes: dict[str, dict[str, object]] = {}
        self.universe = pd.DataFrame()
        self.full_universe = pd.DataFrame()
        self._universe_records_by_market: dict[str, tuple[dict[str, object], ...]] = {}
        self._paired_strikes_by_market_expiration: dict[
            tuple[str, date], np.ndarray
        ] = {}
        self._symbol_to_market: dict[str, str] = {}
        self._indexed_universe_frame_id: int | None = None
        self._indexed_universe_row_count = -1
        self._universe_index_lock = threading.RLock()
        self.oi_analytics_by_market: dict[str, dict[str, object]] = {}
        self.definition_discovery_metadata: dict[str, dict[str, object]] = {}
        self.live_symbols: list[str] = []
        self.subscription_profile = (
            SUBSCRIPTION_PROFILE
            if SUBSCRIPTION_PROFILE in SUPPORTED_SUBSCRIPTION_PROFILES
            else "near-term-shadow"
        )
        self.optional_family_canary = RUTOptionalFamilyCanary(
            enabled=os.getenv("DATABENTO_RUT_CANARY_ENABLED", "0") == "1",
            requested_families=tuple(self.symbols),
        )
        self.optional_family_canary_warmup_seconds = max(
            0.0,
            float(os.getenv("DATABENTO_RUT_CANARY_WARMUP_SECONDS", "180")),
        )
        self._optional_family_canary_generation = 0
        self._optional_family_canary_active_since_monotonic = 0.0
        self._optional_family_canary_lock = threading.Lock()
        self.optional_family_rollback_evidence: dict[str, object] | None = None
        self.optional_family_canary_evaluation_evidence: dict[str, object] = {
            "evaluation_deferred": False,
            "opening_orb_protection_active": False,
            "session_state": "uninitialized",
            "observed_at_utc": None,
            "reasons": [],
        }
        self.primary_pair_admission_diagnostics: dict[str, dict[str, object]] = {}
        self.subscription_metadata: dict[str, object] = {
            "profile": self.subscription_profile,
            "full_contract_count": 0,
            "selected_contract_count": 0,
            "expired_contract_count": 0,
            "markets": {},
            "bounds": _subscription_bound_values(),
            "selected_universe_sha256": None,
            "universe_provenance": {
                "mode": "uninitialized",
                "trading_date": current_market_date().isoformat(),
                "is_fallback": False,
            },
        }
        self.latest_invalid: dict[str, dict] = {}
        self.last_calculation_diagnostics: dict[str, dict[str, object]] = {}
        self.quote_generation = 0
        # Subscription transitions and forecast persistence share this barrier.
        # A lifecycle publisher may therefore linearize its SQLite/passport
        # commits against every generation or handoff-status change.
        self._prediction_publication_lock = threading.RLock()
        self.active_generation = 0
        self.subscription_cutoff_monotonic = 0.0
        self._fresh_markets_seen: set[str] = set()
        self._fresh_market_observations: dict[str, tuple[int, float]] = {}
        self._fresh_quote_counts_by_market: dict[str, int] = {symbol: 0 for symbol in self.symbols}
        self._fresh_quote_expirations: list[tuple[float, int, str, str, int, float]] = []
        self._fresh_quote_sequence = 0
        # Mapping changes and quote publication share this lock so a sampler
        # cannot combine a quote from one mapping generation with the mapping
        # catalog from another.
        self._fresh_quote_lock = threading.RLock()
        self.handoff_started_monotonic = 0.0
        self.handoff_status = "initializing"
        self.handoff_reason: str | None = None
        self.thread: Optional[threading.Thread] = None
        self.compute_thread: Optional[threading.Thread] = None
        self.watchdog_thread: Optional[threading.Thread] = None
        self.orb_reference_thread: Optional[threading.Thread] = None
        self._last_orb_reference_bucket_utc: datetime | None = None
        self.last_orb_reference_results: dict[str, dict[str, object]] = {}
        self._orb_reference_progress_lock = threading.Lock()
        # Serialize only the final ORB state recheck and local SQLite append
        # with shutdown. Expensive quote copying/parity work stays concurrent,
        # while stop can establish that no reference commit remains in flight.
        self._orb_reference_persist_barrier = threading.Lock()
        self._orb_reference_recent_buckets_by_market: dict[
            str, deque[tuple[datetime, int, str]]
        ] = {symbol: deque(maxlen=4) for symbol in self.symbols}
        self._orb_reference_failed_attempt_count = 0
        self._orb_reference_evidence_lock = threading.Lock()
        self._orb_reference_failed_attempts: deque[dict[str, object]] = deque(
            maxlen=ORB_REFERENCE_FAILED_ATTEMPT_LIMIT
        )
        self._orb_reference_failure_warning_states: dict[
            tuple[str, str], dict[str, float | int]
        ] = {}
        self._orb_reference_failure_warning_suppressed_count = 0
        self._stop_event = threading.Event()
        self._last_off_hours_log_state: str | None = None
        self.client: Optional[db.Live] = None
        self._client_lock = threading.Lock()
        self._connection_lifecycle_lock = threading.Lock()
        self._subscription_stage_lock = threading.RLock()
        self.active_live_symbols: list[str] = []
        self.deferred_live_symbols: list[str] = []
        self.subscription_stage_state = "uninitialized"
        self.subscription_stage_active = "none"
        self.subscription_stage_generation = 0
        self.subscription_stage_started_monotonic = 0.0
        self.subscription_stage_clean_started_monotonic = 0.0
        self.subscription_stage_clean_start_messages = 0
        self.subscription_stage_last_evaluated_messages = 0
        self.subscription_stage_last_evaluated_monotonic = 0.0
        self.subscription_stage_last_evaluated_utc: datetime | None = None
        self.subscription_stage_baseline_counters: dict[str, int] = {}
        self.subscription_stage_primary_counts: dict[str, int] = {}
        self.subscription_stage_primary_minimum_pairs: dict[str, int] = {}
        self.subscription_stage_promotion_eligible = False
        self.subscription_stage_promotion_reasons: list[str] = ["NOT_PLANNED"]
        self.subscription_stage_metrics: dict[str, object] = {}
        self.subscription_stage_orb_evidence: dict[str, object] = {}
        self.subscription_stage_promoted_at_utc: datetime | None = None
        self.subscription_stage_additive_subscription_id: int | None = None
        self.subscription_stage_additive_request_sent = False
        self.subscription_stage_selected_universe_sha256: str | None = None
        self.messages_received = 0
        self.quote_records_seen = 0
        self.invalid_quote_records = 0
        self.crossed_quote_records = 0
        self.unmapped_quote_records = 0
        self.provider_timestamp_missing_records = 0
        self.provider_timestamp_order_errors = 0
        self.negative_receive_lag_records = 0
        self.material_negative_receive_lag_records = 0
        self.last_receive_to_process_lag_seconds: float | None = None
        self.max_receive_to_process_lag_seconds: float | None = None
        self._receive_lag_window: deque[float] = deque(maxlen=CLOCK_SYNC_WINDOW_RECORDS)
        self.last_message_time: dict[str, datetime] = {}
        self.last_diagnostic_time: dict[str, datetime] = {}
        self.last_error: str | None = None
        self.formula_validation_errors: dict[str, str] = {}
        self.formula_validation_events: deque[tuple[float, str]] = deque(maxlen=720)
        self.formula_monitor_window_seconds = float(os.getenv("DATABENTO_FORMULA_MONITOR_WINDOW_SECONDS", "900"))
        self.reconnect_attempts = 0
        self.last_reconnect_utc: datetime | None = None
        self.last_reconnect_reason: str | None = None
        self.connection_limit_rejections_total = 0
        self.connection_limit_consecutive = 0
        self.connection_limit_circuit_state = "closed"
        self.connection_limit_retry_not_before_monotonic = 0.0
        self.connection_limit_retry_not_before_utc: datetime | None = None
        self.connection_limit_cooldown_seconds = 0.0
        self.last_client_close_status = "not_attempted"
        self.last_client_close_elapsed_seconds: float | None = None
        self.pre_auth_transport_guard_status = "not_attempted"
        self.pre_auth_transport_aborts_total = 0
        self.pre_auth_transport_abort_failures_total = 0
        self.last_pre_auth_transport_event: str | None = None
        self.last_pre_auth_transport_reason: str | None = None
        self.last_pre_auth_transport_event_utc: datetime | None = None
        self.subscription_attempts = 0
        self.last_subscription_utc: datetime | None = None
        self.last_subscription_replay_start_utc: str | None = None
        self.provider_queue_full_warnings = 0
        self.provider_slow_client_warnings = 0
        self.provider_skipped_record_warnings = 0
        self.provider_skipped_records = 0
        self.provider_pending_records_peak = 0
        self.last_provider_warning_utc: datetime | None = None
        self.last_provider_warning: str | None = None
        self._provider_warning_handler: _DatabentoWarningTelemetryHandler | None = None
        self._last_progress_monotonic = 0.0
        self._universe_built_monotonic = 0.0
        self._market_was_closed: bool | None = None
        self._open_grace_until_monotonic = 0.0
        self._watchdog_stop_reason: str | None = None
        self.schema = os.getenv("DATABENTO_LIVE_SCHEMA", "cbbo-1s")
        self.replay_minutes = int(os.getenv("DATABENTO_REPLAY_MINUTES", "2"))
        self.update_interval = float(os.getenv("DATABENTO_UPDATE_INTERVAL", "5"))
        self.snapshot_interval = float(os.getenv("DATABENTO_SNAPSHOT_INTERVAL_SECONDS", "60"))
        self._last_compute = 0.0
        self._compute_suspended_until_monotonic = 0.0
        self._compute_backpressure_lock = threading.Lock()
        self._last_snapshot_write: dict[str, float] = {}
        self._last_invalid_snapshot_write: dict[str, float] = {}
        self.cache_dir = DATA_DIR / "databento_cache"
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.exports_dir = DATA_DIR.parent / "exports"
        self.audit_dir = DATA_DIR.parent / "logs" / "audit"

    def _reset_stream_telemetry(self) -> None:
        """Reset connection-local latency gauges, not lifetime loss counters."""
        self.last_receive_to_process_lag_seconds = None
        self.max_receive_to_process_lag_seconds = None
        self._receive_lag_window.clear()

    def _processing_clock_telemetry(self) -> dict[str, object]:
        samples = tuple(self._receive_lag_window)
        material_negative = sum(
            value < -CLOCK_SYNC_NEGATIVE_TOLERANCE_SECONDS for value in samples
        )
        ratio = material_negative / len(samples) if samples else None
        if len(samples) < CLOCK_SYNC_MIN_SAMPLES:
            status = "unknown"
        elif ratio is not None and ratio > CLOCK_SYNC_MAX_NEGATIVE_RATIO:
            status = "unsynchronized"
        else:
            status = "synchronized"
        return {
            "status": status,
            "sample_count": len(samples),
            "material_negative_count": material_negative,
            "material_negative_ratio": ratio,
            "negative_tolerance_seconds": CLOCK_SYNC_NEGATIVE_TOLERANCE_SECONDS,
            "maximum_negative_ratio": CLOCK_SYNC_MAX_NEGATIVE_RATIO,
            "minimum_samples": CLOCK_SYNC_MIN_SAMPLES,
            "minimum_lag_seconds": min(samples) if samples else None,
            "maximum_lag_seconds": max(samples) if samples else None,
        }

    def _install_provider_warning_telemetry(self) -> None:
        if self._provider_warning_handler is not None:
            return
        handler = _DatabentoWarningTelemetryHandler(self)
        logging.getLogger("databento").addHandler(handler)
        self._provider_warning_handler = handler

    def _remove_provider_warning_telemetry(self) -> None:
        handler = self._provider_warning_handler
        if handler is None:
            return
        logging.getLogger("databento").removeHandler(handler)
        self._provider_warning_handler = None

    def record_provider_warning(self, message: object) -> None:
        """Record SDK/gateway overload evidence without relying on log files."""
        text = str(message or "").strip()
        if not text:
            return
        normalized = text.lower()
        recognized = False
        queue_full_observed = False
        integrity_loss_observed = False
        if "record queue is full" in normalized:
            self.provider_queue_full_warnings += 1
            pending_match = re.search(
                r"([\d,]+)\s+(?:pending records|record\(s\) to be processed)",
                text,
                flags=re.IGNORECASE,
            )
            if pending_match:
                pending = int(pending_match.group(1).replace(",", ""))
                self.provider_pending_records_peak = max(self.provider_pending_records_peak, pending)
            recognized = True
            queue_full_observed = True
        if "slow client detected" in normalized:
            self.provider_slow_client_warnings += 1
            recognized = True
            integrity_loss_observed = True
        if re.search(r"skipped(?:\s+[\d,]+)?\s+records?", normalized):
            self.provider_skipped_record_warnings += 1
            skipped_match = re.search(r"skipped\s+([\d,]+)\s+records", text, flags=re.IGNORECASE)
            if skipped_match:
                self.provider_skipped_records += int(skipped_match.group(1).replace(",", ""))
            recognized = True
            integrity_loss_observed = True
        if recognized:
            self.last_provider_warning_utc = _utcnow_naive()
            self.last_provider_warning = text[:500]
            self._suspend_compute_for(PROVIDER_OVERLOAD_COMPUTE_BACKOFF_SECONDS)
            if integrity_loss_observed:
                self._set_subscription_stage_terminal(
                    "canceled", "SLOW_OR_SKIPPED_RECORD_DELTA_CANCELS_PROMOTION"
                )
            elif queue_full_observed:
                self._set_subscription_stage_terminal(
                    "frozen", "QUEUE_FULL_DELTA_FREEZES_PROMOTION"
                )

    def _suspend_compute_for(self, seconds: float) -> None:
        deadline = time.monotonic() + max(0.0, float(seconds))
        with self._compute_backpressure_lock:
            self._compute_suspended_until_monotonic = max(
                self._compute_suspended_until_monotonic,
                deadline,
            )

    def _compute_is_suspended(self, now: float | None = None) -> bool:
        observed = time.monotonic() if now is None else now
        with self._compute_backpressure_lock:
            return observed < self._compute_suspended_until_monotonic

    def _compute_backpressure_remaining(self) -> float:
        observed = time.monotonic()
        with self._compute_backpressure_lock:
            return max(0.0, self._compute_suspended_until_monotonic - observed)

    def _subscription_replay_start(self) -> str | None:
        """Request intraday replay only when it can improve initial warm-up.

        Replayed records cannot reconstruct historical snapshots in this live
        calculation path. On a transport reconnect they instead compete with
        the current feed and have repeatedly amplified queue bursts.
        """
        if self.replay_minutes <= 0:
            return None
        if not market_is_closed() and not REPLAY_DURING_REGULAR_SESSION:
            return None
        if self.subscription_attempts > 0 and not REPLAY_ON_RECONNECT:
            return None
        return (
            datetime.now(timezone.utc) - timedelta(minutes=self.replay_minutes)
        ).isoformat()

    def _record_subscription_started(self, replay_start: str | None = None) -> None:
        self.subscription_attempts += 1
        self.last_subscription_utc = _utcnow_naive()
        self.last_subscription_replay_start_utc = replay_start
        self._suspend_compute_for(COMPUTE_WARMUP_GRACE_SECONDS)

    def _record_reconnect(self, reason: str) -> None:
        self.reconnect_attempts += 1
        self.last_reconnect_utc = _utcnow_naive()
        self.last_reconnect_reason = str(reason)

    @staticmethod
    def _is_open_connection_limit_error(error: object) -> bool:
        return OPEN_CONNECTION_LIMIT_PHRASE in str(error or "").casefold()

    def _open_connection_limit_circuit(self) -> float:
        observed_monotonic = time.monotonic()
        observed_utc = datetime.now(timezone.utc)
        with self._connection_lifecycle_lock:
            self.connection_limit_rejections_total += 1
            self.connection_limit_consecutive += 1
            cooldown_index = min(
                self.connection_limit_consecutive - 1,
                len(OPEN_CONNECTION_LIMIT_COOLDOWNS_SECONDS) - 1,
            )
            cooldown_seconds = OPEN_CONNECTION_LIMIT_COOLDOWNS_SECONDS[
                cooldown_index
            ]
            self.connection_limit_circuit_state = "open"
            self.connection_limit_cooldown_seconds = cooldown_seconds
            self.connection_limit_retry_not_before_monotonic = (
                observed_monotonic + cooldown_seconds
            )
            self.connection_limit_retry_not_before_utc = (
                observed_utc + timedelta(seconds=cooldown_seconds)
            )
            return cooldown_seconds

    def _close_connection_limit_circuit(self) -> None:
        with self._connection_lifecycle_lock:
            self.connection_limit_consecutive = 0
            self.connection_limit_circuit_state = "closed"
            self.connection_limit_cooldown_seconds = 0.0
            self.connection_limit_retry_not_before_monotonic = 0.0
            self.connection_limit_retry_not_before_utc = None

    def _reopen_connection_limit_circuit_after_probe_failure(self) -> float | None:
        """Re-arm an inconclusive half-open probe without inventing a rejection."""
        observed_monotonic = time.monotonic()
        observed_utc = datetime.now(timezone.utc)
        with self._connection_lifecycle_lock:
            if self.connection_limit_circuit_state != "half_open":
                return None
            cooldown_seconds = (
                self.connection_limit_cooldown_seconds
                or OPEN_CONNECTION_LIMIT_COOLDOWNS_SECONDS[0]
            )
            self.connection_limit_circuit_state = "open"
            self.connection_limit_cooldown_seconds = cooldown_seconds
            self.connection_limit_retry_not_before_monotonic = (
                observed_monotonic + cooldown_seconds
            )
            self.connection_limit_retry_not_before_utc = (
                observed_utc + timedelta(seconds=cooldown_seconds)
            )
            return cooldown_seconds

    def _wait_for_connection_limit_probe(self, cooldown_seconds: float) -> bool:
        if self._stop_event.wait(timeout=max(0.0, cooldown_seconds)):
            return False
        if not self.is_running:
            return False
        subscription_window = self._subscription_window()
        if not bool(subscription_window.get("subscription_allowed")):
            return False
        with self._connection_lifecycle_lock:
            # Only the stream thread creates clients, so this transition admits
            # exactly one half-open authentication probe.
            if self.connection_limit_circuit_state != "open":
                return False
            self.connection_limit_circuit_state = "half_open"
            self.connection_limit_retry_not_before_monotonic = 0.0
            self.connection_limit_retry_not_before_utc = None
        return True

    def _close_live_client(
        self,
        live_client: db.Live,
        *,
        subscription_established: bool,
    ) -> str:
        """Release one SDK client without masking its original stream error.

        Databento 0.84 exposes a local close barrier only after subscribe has
        successfully installed the session protocol. A pre-authentication
        failure cannot be acknowledged through the public SDK API, so it is
        explicitly reported as unacknowledged and handled by the quota circuit.
        """
        started_monotonic = time.monotonic()
        stop_error: Exception | None = None
        barrier_error: Exception | None = None
        try:
            live_client.stop()
        except Exception as exc:
            stop_error = exc

        if subscription_established:
            try:
                live_client.block_for_close(
                    timeout=LIVE_CLIENT_CLOSE_TIMEOUT_SECONDS
                )
            except Exception as exc:
                barrier_error = exc
            if barrier_error is not None:
                close_status = "sdk_close_barrier_error"
            elif stop_error is not None:
                close_status = "sdk_close_barrier_returned_after_stop_error"
            else:
                close_status = "sdk_close_barrier_returned"
        else:
            close_status = "pre_auth_close_unacknowledged"

        elapsed_seconds = max(0.0, time.monotonic() - started_monotonic)
        with self._connection_lifecycle_lock:
            self.last_client_close_status = close_status
            self.last_client_close_elapsed_seconds = elapsed_seconds

        if stop_error is not None or barrier_error is not None:
            logger.warning(
                "Databento client cleanup status=%s stop_error_type=%s "
                "barrier_error_type=%s elapsed_seconds=%.3f",
                close_status,
                type(stop_error).__name__ if stop_error is not None else "none",
                type(barrier_error).__name__
                if barrier_error is not None
                else "none",
                elapsed_seconds,
            )
        else:
            logger.info(
                "Databento client cleanup status=%s elapsed_seconds=%.3f",
                close_status,
                elapsed_seconds,
            )
        return close_status

    def _record_pre_auth_transport_event(self, outcome: str, reason: str) -> None:
        observed_utc = datetime.now(timezone.utc)
        with self._connection_lifecycle_lock:
            if outcome == "transport_aborted":
                self.pre_auth_transport_aborts_total += 1
            else:
                self.pre_auth_transport_abort_failures_total += 1
            self.last_pre_auth_transport_event = outcome
            self.last_pre_auth_transport_reason = reason
            self.last_pre_auth_transport_event_utc = observed_utc

        log_method = (
            logger.info if outcome == "transport_aborted" else logger.warning
        )
        log_method(
            "Databento pre-auth transport guard outcome=%s reason=%s",
            outcome,
            reason,
        )

    def _connection_lifecycle_health(self) -> dict[str, object]:
        observed_monotonic = time.monotonic()
        with self._connection_lifecycle_lock:
            retry_not_before_utc = self.connection_limit_retry_not_before_utc
            return {
                "connection_limit_rejections_total": (
                    self.connection_limit_rejections_total
                ),
                "connection_limit_consecutive": self.connection_limit_consecutive,
                "connection_limit_circuit_state": (
                    self.connection_limit_circuit_state
                ),
                "connection_limit_retry_not_before_utc": (
                    retry_not_before_utc.isoformat()
                    if retry_not_before_utc is not None
                    else None
                ),
                "connection_limit_cooldown_remaining_seconds": max(
                    0.0,
                    self.connection_limit_retry_not_before_monotonic
                    - observed_monotonic,
                ),
                "last_client_close_status": self.last_client_close_status,
                "last_client_close_elapsed_seconds": (
                    self.last_client_close_elapsed_seconds
                ),
                "pre_auth_transport_guard_status": (
                    self.pre_auth_transport_guard_status
                ),
                "pre_auth_transport_aborts_total": (
                    self.pre_auth_transport_aborts_total
                ),
                "pre_auth_transport_abort_failures_total": (
                    self.pre_auth_transport_abort_failures_total
                ),
                "last_pre_auth_transport_event": (
                    self.last_pre_auth_transport_event
                ),
                "last_pre_auth_transport_reason": (
                    self.last_pre_auth_transport_reason
                ),
                "last_pre_auth_transport_event_utc": (
                    self.last_pre_auth_transport_event_utc.isoformat()
                    if self.last_pre_auth_transport_event_utc is not None
                    else None
                ),
            }

    def _subscription_stage_counter_values(self) -> dict[str, int]:
        return {
            "queue_full": int(self.provider_queue_full_warnings),
            "slow_client": int(self.provider_slow_client_warnings),
            "skipped_warning": int(self.provider_skipped_record_warnings),
            "skipped_records": int(self.provider_skipped_records),
            "reconnect": int(self.reconnect_attempts),
        }

    def _set_subscription_stage_terminal(self, state: str, reason: str) -> None:
        with self._subscription_stage_lock:
            current_state = self.subscription_stage_state
            if current_state not in {
                "planned_primary",
                "primary_active",
                "promoting",
                "frozen",
            }:
                return
            if current_state == "frozen" and state != "canceled":
                return
            self.subscription_stage_state = state
            self.subscription_stage_promotion_eligible = False
            self.subscription_stage_promotion_reasons = list(
                dict.fromkeys([*self.subscription_stage_promotion_reasons, reason])
            )

    def _fail_subscription_stage_plan(
        self,
        reasons: list[str],
        *,
        metrics: Mapping[str, object] | None = None,
    ) -> None:
        normalized_reasons = list(dict.fromkeys(str(reason) for reason in reasons))
        with self._subscription_stage_lock:
            self.active_live_symbols = []
            self.deferred_live_symbols = list(self.live_symbols)
            self.subscription_stage_state = "blocked"
            self.subscription_stage_active = "none"
            self.subscription_stage_promotion_eligible = False
            self.subscription_stage_promotion_reasons = normalized_reasons
            self.subscription_stage_metrics = dict(metrics or {})
        raise RuntimeError(
            "STAGED_SUBSCRIPTION_BLOCKED: " + "; ".join(normalized_reasons)
        )

    def _prepare_live_subscription_stages(self) -> list[str]:
        """Return the all-primary opening stage without changing full-universe identity."""
        with self._universe_index_lock:
            full_symbols = list(dict.fromkeys(str(value) for value in self.live_symbols))
            selected = self.universe.copy()
            metadata = json.loads(
                json.dumps(self.subscription_metadata, sort_keys=True, default=str)
            )

        selected_hash = str(metadata.get("selected_universe_sha256") or "")
        expected_hash = hashlib.sha256(
            "\n".join(full_symbols).encode("utf-8")
        ).hexdigest()
        if not full_symbols:
            self._fail_subscription_stage_plan(["FULL_SELECTED_UNIVERSE_EMPTY"])
        if selected.empty:
            # Unit/research callers may inject a raw symbol list without a
            # selected-universe frame. Production plans always carry a
            # canonical hash and must fail closed if that frame disappears.
            if re.fullmatch(r"[0-9a-f]{64}", selected_hash):
                self._fail_subscription_stage_plan(
                    ["FULL_SELECTED_UNIVERSE_FRAME_MISSING"]
                )
            with self._subscription_stage_lock:
                self.active_live_symbols = list(full_symbols)
                self.deferred_live_symbols = []
                self.subscription_stage_state = "legacy_full"
                self.subscription_stage_active = "full"
                self.subscription_stage_primary_counts = {}
                self.subscription_stage_primary_minimum_pairs = {}
                self.subscription_stage_promotion_eligible = False
                self.subscription_stage_promotion_reasons = [
                    "CANONICAL_SELECTED_UNIVERSE_NOT_AVAILABLE"
                ]
            return full_symbols
        if selected_hash != expected_hash:
            self._fail_subscription_stage_plan(
                ["FULL_SELECTED_UNIVERSE_HASH_MISMATCH"],
                metrics={
                    "declared_selected_universe_sha256": selected_hash or None,
                    "computed_selected_universe_sha256": expected_hash,
                },
            )
        capacity_reasons = []
        if len(full_symbols) > MAX_SUBSCRIPTION_CONTRACTS:
            capacity_reasons.append(
                "FULL_SELECTED_UNIVERSE_EXCEEDS_CAP: "
                f"{len(full_symbols)}>{MAX_SUBSCRIPTION_CONTRACTS}"
            )

        requested_families = tuple(
            family
            for family in ORB_STAGED_SUBSCRIPTION_FAMILIES
            if family in self.symbols
        )
        if not requested_families:
            with self._subscription_stage_lock:
                self.active_live_symbols = list(full_symbols)
                self.deferred_live_symbols = []
                self.subscription_stage_state = "full_active"
                self.subscription_stage_active = "full"
                self.subscription_stage_promotion_eligible = False
                self.subscription_stage_promotion_reasons = [
                    "NO_ORB_FAMILIES_REQUESTED"
                ]
            return full_symbols

        required_columns = {
            "market",
            "symbol",
            "expiration_date",
            "option_type",
            "strike",
        }
        missing_columns = sorted(required_columns.difference(selected.columns))
        if missing_columns:
            self._fail_subscription_stage_plan(
                [f"SELECTED_UNIVERSE_COLUMNS_MISSING:{','.join(missing_columns)}"]
            )
        selected["expiration_date"] = pd.to_datetime(
            selected["expiration_date"], errors="coerce"
        ).dt.date
        selected = selected.dropna(subset=list(required_columns)).copy()
        selected["_stage_pair_root"] = selected["symbol"].map(
            _option_root_from_symbol
        )
        selected_symbols = set(selected["symbol"].astype(str))
        if selected_symbols != set(full_symbols):
            self._fail_subscription_stage_plan(
                ["FULL_SELECTED_UNIVERSE_SYMBOL_SET_MISMATCH"]
            )

        trading_date = current_market_date()
        market_plans = metadata.get("markets") or {}
        primary_symbol_set: set[str] = set()
        primary_counts: dict[str, int] = {}
        minimum_pairs_by_family: dict[str, int] = {}
        minimum_contracts_total = 0
        plan_reasons: list[str] = list(capacity_reasons)
        for family in requested_families:
            market_plan = market_plans.get(family) or {}
            primary_entries = [
                entry
                for entry in market_plan.get("selected_expirations") or []
                if isinstance(entry, Mapping) and entry.get("role") == "primary"
            ]
            if len(primary_entries) != 1:
                plan_reasons.append(
                    f"PRIMARY_EXPIRATION_NOT_UNIQUE:{family}"
                )
                continue
            try:
                primary_expiration = date.fromisoformat(
                    str(primary_entries[0].get("expiration") or "")
                )
            except ValueError:
                plan_reasons.append(f"PRIMARY_EXPIRATION_INVALID:{family}")
                continue
            if family in {"SPX", "NDX"} and primary_expiration != trading_date:
                plan_reasons.append(
                    f"PRIMARY_EXPIRATION_NOT_SAME_DAY:{family}"
                )
            if family == "VIX" and (
                not _expiration_is_live_eligible(
                    family, primary_expiration, trading_date
                )
                or market_plan.get("primary_expiration_authority")
                != VIX_FORWARD_CONTEXT_AUTHORITY
                or market_plan.get("primary_expiration_context_only") is not True
            ):
                plan_reasons.append("VIX_FORWARD_PRIMARY_CONTEXT_INVALID")

            family_rows = selected[
                (selected["market"] == family)
                & (selected["expiration_date"] == primary_expiration)
            ].copy()
            family_symbols = set(family_rows["symbol"].astype(str))
            if not family_symbols:
                plan_reasons.append(f"PRIMARY_CONTRACTS_MISSING:{family}")
                continue
            pair_types = family_rows.groupby(
                ["_stage_pair_root", "strike"]
            )["option_type"].agg(
                lambda values: {str(value).upper() for value in values}
            )
            complete_pairs = int(
                sum(types == {"C", "P"} for types in pair_types)
            )
            if complete_pairs * 2 != len(family_rows):
                plan_reasons.append(f"PRIMARY_CONTRACT_PAIRS_INCOMPLETE:{family}")
            admission = market_plan.get("primary_pair_admission") or {}
            default_minimum = (
                MIN_PRIMARY_STRIKE_PAIRS if family in {"SPX", "NDX"} else 1
            )
            minimum_pairs = int(
                admission.get("minimum_pair_count") or default_minimum
            )
            if family in {"VIX", "RUT"}:
                # Every ORB sample uses the same global pseudo-parity floor.
                # Enforce it again at the streamer boundary so a direct/manual
                # launch cannot bypass the stronger launcher receipt check by
                # presenting stale optional-family admission metadata.
                minimum_pairs = max(5, MIN_PAIRED_QUOTES, minimum_pairs)
            minimum_pairs_by_family[family] = minimum_pairs
            minimum_contracts_total += minimum_pairs * 2
            if complete_pairs < minimum_pairs:
                plan_reasons.append(
                    f"PRIMARY_PAIR_MINIMUM_NOT_MET:{family}:"
                    f"{complete_pairs}<{minimum_pairs}"
                )
            declared_contracts = int(primary_entries[0].get("contracts") or 0)
            if declared_contracts != len(family_rows):
                plan_reasons.append(
                    f"PRIMARY_CONTRACT_COUNT_MISMATCH:{family}:"
                    f"{len(family_rows)}!={declared_contracts}"
                )
            primary_counts[family] = len(family_rows)
            primary_symbol_set.update(family_symbols)

        if minimum_contracts_total > MAX_SUBSCRIPTION_CONTRACTS:
            plan_reasons.append(
                "ALL_PRIMARY_MINIMA_EXCEED_SUBSCRIPTION_CAP: "
                f"{minimum_contracts_total}>{MAX_SUBSCRIPTION_CONTRACTS}"
            )
        initial_symbols = [
            symbol for symbol in full_symbols if symbol in primary_symbol_set
        ]
        if len(initial_symbols) > MAX_SUBSCRIPTION_CONTRACTS:
            plan_reasons.append(
                "ALL_PRIMARY_CONTRACTS_EXCEED_SUBSCRIPTION_CAP: "
                f"{len(initial_symbols)}>{MAX_SUBSCRIPTION_CONTRACTS}"
            )
        if set(primary_counts) != set(requested_families):
            plan_reasons.append("ALL_REQUESTED_ORB_FAMILIES_NOT_PRESENT")
        if plan_reasons:
            self._fail_subscription_stage_plan(
                plan_reasons,
                metrics={
                    "requested_orb_families": list(requested_families),
                    "primary_contract_counts": primary_counts,
                    "primary_minimum_pairs": minimum_pairs_by_family,
                    "primary_minimum_contracts_total": minimum_contracts_total,
                    "subscription_cap": MAX_SUBSCRIPTION_CONTRACTS,
                },
            )

        deferred_symbols = [
            symbol for symbol in full_symbols if symbol not in primary_symbol_set
        ]
        plan_counters = self._subscription_stage_counter_values()
        with self._subscription_stage_lock:
            self.active_live_symbols = list(initial_symbols)
            self.deferred_live_symbols = list(deferred_symbols)
            self.subscription_stage_state = (
                "planned_primary" if deferred_symbols else "planned_full"
            )
            self.subscription_stage_active = (
                "primary" if deferred_symbols else "full"
            )
            self.subscription_stage_generation = 0
            self.subscription_stage_primary_counts = dict(primary_counts)
            self.subscription_stage_primary_minimum_pairs = dict(
                minimum_pairs_by_family
            )
            self.subscription_stage_promotion_eligible = False
            self.subscription_stage_promotion_reasons = (
                ["INITIAL_PRIMARY_SUBSCRIPTION_NOT_ACTIVE"]
                if deferred_symbols
                else ["NO_DEFERRED_SHADOW_SYMBOLS"]
            )
            self.subscription_stage_metrics = {
                "requested_orb_families": list(requested_families),
                "primary_minimum_contracts_total": minimum_contracts_total,
                "subscription_cap": MAX_SUBSCRIPTION_CONTRACTS,
            }
            self.subscription_stage_baseline_counters = plan_counters
            self.subscription_stage_orb_evidence = {}
            self.subscription_stage_promoted_at_utc = None
            self.subscription_stage_additive_subscription_id = None
            self.subscription_stage_additive_request_sent = False
            self.subscription_stage_selected_universe_sha256 = selected_hash
        return initial_symbols

    def _activate_initial_subscription_stage(self, generation: int) -> None:
        now_monotonic = time.monotonic()
        with self._subscription_stage_lock:
            prior_state = self.subscription_stage_state
            self.subscription_stage_generation = int(generation)
            self.subscription_stage_started_monotonic = now_monotonic
            self.subscription_stage_clean_started_monotonic = 0.0
            self.subscription_stage_clean_start_messages = self.messages_received
            self.subscription_stage_last_evaluated_messages = self.messages_received
            self.subscription_stage_last_evaluated_monotonic = 0.0
            self.subscription_stage_last_evaluated_utc = None
            if prior_state in {"frozen", "canceled"}:
                self.subscription_stage_active = (
                    "primary" if self.deferred_live_symbols else "full"
                )
            elif self.deferred_live_symbols:
                self.subscription_stage_state = "primary_active"
                self.subscription_stage_active = "primary"
                self.subscription_stage_promotion_reasons = [
                    "PROMOTION_GATES_NOT_EVALUATED"
                ]
            else:
                self.subscription_stage_state = "full_active"
                self.subscription_stage_active = "full"
                self.subscription_stage_promotion_reasons = [
                    "NO_DEFERRED_SHADOW_SYMBOLS"
                ]

    def _orb_60m_subscription_stage_evidence(
        self, observed_at_utc: datetime
    ) -> dict[str, object]:
        from backend.market_structure import get_market_structure_journal

        journal = get_market_structure_journal()
        evidence: dict[str, object] = {}
        for family in ORB_STAGED_SUBSCRIPTION_FAMILIES:
            if family not in self.symbols:
                continue
            try:
                state = journal.snapshot(
                    family,
                    trading_date=observed_at_utc.astimezone(_NY_TZ).date(),
                    as_of_utc=observed_at_utc,
                    configured=True,
                    active_subscription_epoch_id=self.subscription_epoch_id,
                    active_subscription_generation=self.active_generation,
                    active_handoff_status=self.handoff_status,
                )
                window = (state.get("opening_ranges") or {}).get("60m") or {}
                provenance = window.get("provenance") or {}
                exact_complete = bool(
                    window.get("orb_complete") is True
                    and window.get("capture_status") == "complete"
                    and window.get("current_reference_fresh") is True
                    and provenance.get("range_provenance_aligned") is True
                    and provenance.get("current_vs_range_aligned") is True
                    and provenance.get("active_runtime_epoch_aligned") is True
                )
                evidence[family] = {
                    "exact_complete": exact_complete,
                    "capture_status": window.get("capture_status"),
                    "orb_complete": window.get("orb_complete"),
                    "provenance": dict(provenance),
                    "capture_evidence": dict(window.get("capture_evidence") or {}),
                    "warnings": list(window.get("warnings") or []),
                }
            except Exception as exc:
                evidence[family] = {
                    "exact_complete": False,
                    "capture_status": "error",
                    "reason": f"{type(exc).__name__}: {exc}",
                }
        return evidence

    def _evaluate_subscription_stage_promotion(
        self,
        *,
        observed_monotonic: float | None = None,
        observed_at_utc: datetime | None = None,
        orb_evidence: Mapping[str, object] | None = None,
    ) -> bool:
        now_monotonic = (
            time.monotonic() if observed_monotonic is None else observed_monotonic
        )
        now_utc = (observed_at_utc or datetime.now(timezone.utc)).astimezone(
            timezone.utc
        )
        with self._subscription_stage_lock:
            if self.subscription_stage_state != "primary_active":
                return False
            expected_counts = dict(self.subscription_stage_primary_counts)
            baseline = dict(self.subscription_stage_baseline_counters)
            clean_started = self.subscription_stage_clean_started_monotonic
            clean_start_messages = self.subscription_stage_clean_start_messages
            previous_messages = self.subscription_stage_last_evaluated_messages

        current_counters = self._subscription_stage_counter_values()
        counter_deltas = {
            key: max(0, current_counters.get(key, 0) - baseline.get(key, 0))
            for key in current_counters
        }
        if counter_deltas["queue_full"] > 0:
            self._set_subscription_stage_terminal(
                "frozen", "QUEUE_FULL_DELTA_FREEZES_PROMOTION"
            )
            return False
        cancel_deltas = {
            key: value
            for key, value in counter_deltas.items()
            if key in {"slow_client", "skipped_warning", "skipped_records", "reconnect"}
            and value > 0
        }
        if cancel_deltas:
            self._set_subscription_stage_terminal(
                "canceled",
                "INTEGRITY_DELTA_CANCELS_PROMOTION:"
                + ",".join(sorted(cancel_deltas)),
            )
            return False

        with self._prediction_publication_lock:
            latest_pins = dict(self.latest_pins)
            latest_invalid = dict(self.latest_invalid)
            last_valid = dict(self.last_message_time)
            last_diagnostic = dict(self.last_diagnostic_time)
            handoff_status = str(self.handoff_status)
            active_generation = int(self.active_generation)
        fresh_counts = self._fresh_quote_counts()
        progress_age_seconds = (
            max(0.0, now_monotonic - self._last_progress_monotonic)
            if self._last_progress_monotonic
            else None
        )
        current_messages = int(self.messages_received)
        advancing = current_messages > previous_messages
        quality_reasons: list[str] = []
        if handoff_status != "active":
            quality_reasons.append("HANDOFF_NOT_ACTIVE")
        if active_generation != self.subscription_stage_generation:
            quality_reasons.append("SUBSCRIPTION_GENERATION_CHANGED")
        if (
            progress_age_seconds is None
            or progress_age_seconds > ORB_STAGE_MAX_DATA_AGE_SECONDS
        ):
            quality_reasons.append("TRANSPORT_NOT_FRESH")
        if not advancing:
            quality_reasons.append("TRANSPORT_NOT_ADVANCING")

        family_metrics: dict[str, object] = {}
        for family, expected_count in expected_counts.items():
            payload = latest_pins.get(family) or latest_invalid.get(family)
            updated = last_valid.get(family) or last_diagnostic.get(family)
            data_age_seconds = (
                max(
                    0.0,
                    (now_utc.replace(tzinfo=None) - updated).total_seconds(),
                )
                if updated is not None
                else None
            )
            p95_lag = (payload or {}).get("receive_to_process_lag_p95_seconds")
            primary_coverage = (payload or {}).get("primary_pair_coverage_ratio")
            fresh_count = int(fresh_counts.get(family, 0))
            fresh_coverage = (
                fresh_count / float(expected_count) if expected_count > 0 else 0.0
            )
            family_reasons: list[str] = []
            if payload is None:
                family_reasons.append("DIAGNOSTIC_MISSING")
            if (
                data_age_seconds is None
                or data_age_seconds > ORB_STAGE_MAX_DATA_AGE_SECONDS
            ):
                family_reasons.append("DATA_AGE_EXCEEDED")
            try:
                p95_lag_value = float(p95_lag)
            except (TypeError, ValueError, OverflowError):
                p95_lag_value = math.nan
            if (
                not math.isfinite(p95_lag_value)
                or p95_lag_value < 0.0
                or p95_lag_value > ORB_STAGE_MAX_P95_LAG_SECONDS
            ):
                family_reasons.append("P95_LAG_EXCEEDED_OR_INVALID")
            try:
                primary_coverage_value = float(primary_coverage)
            except (TypeError, ValueError, OverflowError):
                primary_coverage_value = math.nan
            if (
                not math.isfinite(primary_coverage_value)
                or primary_coverage_value
                < ORB_STAGE_MIN_PRIMARY_PAIR_COVERAGE_RATIO
            ):
                family_reasons.append("PRIMARY_PAIR_COVERAGE_LOW")
            if fresh_coverage < ORB_STAGE_MIN_FRESH_COVERAGE_RATIO:
                family_reasons.append("FRESH_COVERAGE_LOW")
            family_metrics[family] = {
                "data_age_seconds": data_age_seconds,
                "receive_to_process_lag_p95_seconds": (
                    p95_lag_value if math.isfinite(p95_lag_value) else None
                ),
                "primary_pair_coverage_ratio": (
                    primary_coverage_value
                    if math.isfinite(primary_coverage_value)
                    else None
                ),
                "fresh_quote_count": fresh_count,
                "expected_primary_contract_count": expected_count,
                "fresh_coverage_ratio": fresh_coverage,
                "eligible": not family_reasons,
                "reasons": family_reasons,
            }
            quality_reasons.extend(
                f"{reason}:{family}" for reason in family_reasons
            )

        if quality_reasons:
            clean_started = 0.0
            clean_start_messages = current_messages
            clean_elapsed = 0.0
        else:
            if clean_started <= 0.0:
                clean_started = now_monotonic
                clean_start_messages = current_messages
            clean_elapsed = max(0.0, now_monotonic - clean_started)
        clean_advancing = current_messages > clean_start_messages
        if clean_elapsed < ORB_STAGE_CLEAN_TRANSPORT_SECONDS:
            quality_reasons.append("CLEAN_TRANSPORT_WINDOW_INCOMPLETE")
        elif not clean_advancing:
            quality_reasons.append("CLEAN_TRANSPORT_WINDOW_NOT_ADVANCING")

        if orb_evidence is None:
            subscription_window = self._subscription_window()
            cash_open_utc = self._canary_window_timestamp(
                subscription_window, "cash_open_utc"
            )
            if (
                cash_open_utc is None
                or now_utc < cash_open_utc + timedelta(hours=1)
            ):
                orb_result = {
                    family: {
                        "exact_complete": False,
                        "capture_status": "forming",
                    }
                    for family in expected_counts
                }
            else:
                orb_result = self._orb_60m_subscription_stage_evidence(now_utc)
        else:
            orb_result = {
                str(key): dict(value) if isinstance(value, Mapping) else value
                for key, value in orb_evidence.items()
            }
        orb_reasons = [
            f"ORB_60M_NOT_EXACT_COMPLETE:{family}"
            for family in expected_counts
            if not isinstance(orb_result.get(family), Mapping)
            or (orb_result.get(family) or {}).get("exact_complete") is not True
        ]
        reasons = list(dict.fromkeys([*quality_reasons, *orb_reasons]))
        eligible = not reasons
        with self._subscription_stage_lock:
            if self.subscription_stage_state != "primary_active":
                return False
            self.subscription_stage_clean_started_monotonic = clean_started
            self.subscription_stage_clean_start_messages = clean_start_messages
            self.subscription_stage_last_evaluated_messages = current_messages
            self.subscription_stage_last_evaluated_monotonic = now_monotonic
            self.subscription_stage_last_evaluated_utc = now_utc
            self.subscription_stage_promotion_eligible = eligible
            self.subscription_stage_promotion_reasons = reasons
            self.subscription_stage_orb_evidence = dict(orb_result)
            self.subscription_stage_metrics = {
                "progress_age_seconds": progress_age_seconds,
                "messages_received": current_messages,
                "messages_advancing_since_evaluation": advancing,
                "clean_transport_elapsed_seconds": clean_elapsed,
                "clean_transport_advancing": clean_advancing,
                "counter_deltas": counter_deltas,
                "families": family_metrics,
            }
        return eligible

    def _maybe_promote_deferred_subscription_stage(self) -> bool:
        now_monotonic = time.monotonic()
        with self._subscription_stage_lock:
            if self.subscription_stage_state != "primary_active":
                return False
            if (
                self.subscription_stage_last_evaluated_monotonic
                and now_monotonic
                - self.subscription_stage_last_evaluated_monotonic
                < ORB_STAGE_EVALUATION_INTERVAL_SECONDS
            ):
                return False
        if not self._evaluate_subscription_stage_promotion(
            observed_monotonic=now_monotonic
        ):
            return False

        with self._client_lock:
            live_client = self.client
            with self._subscription_stage_lock:
                if (
                    live_client is None
                    or self.subscription_stage_state != "primary_active"
                    or not self.subscription_stage_promotion_eligible
                    or self.subscription_stage_generation != self.active_generation
                    or self.handoff_status != "active"
                ):
                    return False
                if (
                    self.subscription_stage_selected_universe_sha256
                    != self.subscription_metadata.get("selected_universe_sha256")
                ):
                    self.subscription_stage_state = "canceled"
                    self.subscription_stage_promotion_eligible = False
                    self.subscription_stage_promotion_reasons = [
                        "SELECTED_UNIVERSE_IDENTITY_CHANGED"
                    ]
                    return False
                deferred_symbols = list(self.deferred_live_symbols)
                if not deferred_symbols:
                    return False
                self.subscription_stage_state = "promoting"
                self.subscription_stage_promotion_eligible = False
                self.subscription_stage_promotion_reasons = [
                    "ADDITIVE_SUBSCRIBE_IN_PROGRESS"
                ]
            try:
                subscription_id = live_client.subscribe(
                    dataset=DATASET,
                    schema=self.schema,
                    symbols=deferred_symbols,
                    stype_in="raw_symbol",
                )
            except Exception as exc:
                with self._subscription_stage_lock:
                    self.subscription_stage_state = "canceled"
                    self.subscription_stage_promotion_reasons = [
                        f"ADDITIVE_SUBSCRIBE_FAILED:{type(exc).__name__}"
                    ]
                self.last_error = f"Databento additive subscription failed: {exc}"
                logger.exception(self.last_error)
                return False

            with self._subscription_stage_lock:
                interrupted_state = self.subscription_stage_state
                interrupted_reasons = list(
                    self.subscription_stage_promotion_reasons
                )
                self.active_live_symbols = list(self.live_symbols)
                self.deferred_live_symbols = []
                self.subscription_stage_active = "full"
                self.subscription_stage_state = (
                    "full_active"
                    if interrupted_state == "promoting"
                    else "full_active_integrity_warning"
                )
                self.subscription_stage_promotion_reasons = (
                    []
                    if interrupted_state == "promoting"
                    else list(
                        dict.fromkeys(
                            [
                                *interrupted_reasons,
                                "ADDITIVE_REQUEST_ALREADY_SENT",
                            ]
                        )
                    )
                )
                self.subscription_stage_promoted_at_utc = datetime.now(
                    timezone.utc
                )
                self.subscription_stage_additive_request_sent = True
                self.subscription_stage_additive_subscription_id = (
                    int(subscription_id)
                    if isinstance(subscription_id, int)
                    and not isinstance(subscription_id, bool)
                    else None
                )
        logger.info(
            "Databento same-session shadow stage requested %s deferred symbols "
            "without changing epoch=%s generation=%s",
            len(deferred_symbols),
            self.subscription_epoch_id,
            self.active_generation,
        )
        return True

    def _subscription_stage_health(self) -> dict[str, object]:
        current_counters = self._subscription_stage_counter_values()
        with self._subscription_stage_lock:
            baseline = dict(self.subscription_stage_baseline_counters)
            active_symbols = list(self.active_live_symbols)
            deferred_symbols = list(self.deferred_live_symbols)
            state = self.subscription_stage_state
            active_stage = self.subscription_stage_active
            clean_started = self.subscription_stage_clean_started_monotonic
            return {
                "mode": "all-primary-then-same-session-shadow",
                "state": state,
                "active_stage": active_stage,
                "deferred_stage": "shadow" if deferred_symbols else None,
                "full_selected_contract_count": len(self.live_symbols),
                "active_contract_count": len(active_symbols),
                "deferred_contract_count": len(deferred_symbols),
                "requested_orb_families": [
                    family
                    for family in ORB_STAGED_SUBSCRIPTION_FAMILIES
                    if family in self.symbols
                ],
                "primary_contract_counts": dict(
                    self.subscription_stage_primary_counts
                ),
                "primary_minimum_pairs": dict(
                    self.subscription_stage_primary_minimum_pairs
                ),
                "subscription_epoch_id": self.subscription_epoch_id,
                "subscription_generation": self.subscription_stage_generation,
                "full_selected_universe_sha256": (
                    self.subscription_stage_selected_universe_sha256
                    or self.subscription_metadata.get("selected_universe_sha256")
                ),
                "same_client_additive_subscription": True,
                "intraday_replay_for_deferred_stage": False,
                "promotion_eligible": self.subscription_stage_promotion_eligible,
                "promotion_reasons": list(
                    self.subscription_stage_promotion_reasons
                ),
                "promotion_thresholds": {
                    "clean_transport_seconds": ORB_STAGE_CLEAN_TRANSPORT_SECONDS,
                    "maximum_data_age_seconds": ORB_STAGE_MAX_DATA_AGE_SECONDS,
                    "maximum_p95_lag_seconds": ORB_STAGE_MAX_P95_LAG_SECONDS,
                    "minimum_primary_pair_coverage_ratio": (
                        ORB_STAGE_MIN_PRIMARY_PAIR_COVERAGE_RATIO
                    ),
                    "minimum_fresh_coverage_ratio": (
                        ORB_STAGE_MIN_FRESH_COVERAGE_RATIO
                    ),
                    "requires_all_60m_orbs_exact_complete": True,
                },
                "clean_transport_elapsed_seconds": (
                    max(0.0, time.monotonic() - clean_started)
                    if clean_started > 0.0
                    else 0.0
                ),
                "baseline_counters": baseline,
                "counter_deltas": {
                    key: max(0, current_counters.get(key, 0) - baseline.get(key, 0))
                    for key in current_counters
                },
                "metrics": json.loads(
                    json.dumps(self.subscription_stage_metrics, default=str)
                ),
                "orb_60m_evidence": json.loads(
                    json.dumps(self.subscription_stage_orb_evidence, default=str)
                ),
                "last_evaluated_utc": (
                    self.subscription_stage_last_evaluated_utc.isoformat()
                    if self.subscription_stage_last_evaluated_utc
                    else None
                ),
                "promoted_at_utc": (
                    self.subscription_stage_promoted_at_utc.isoformat()
                    if self.subscription_stage_promoted_at_utc
                    else None
                ),
                "additive_subscription_id": (
                    self.subscription_stage_additive_subscription_id
                ),
                "additive_request_sent": (
                    self.subscription_stage_additive_request_sent
                ),
            }

    def _index_universe_metadata(self) -> None:
        """Rebuild shared universe indexes without exposing partial dictionaries."""
        with self._universe_index_lock:
            self._index_universe_metadata_locked()

    def _index_universe_metadata_locked(self) -> None:
        records_by_market: dict[str, list[dict[str, object]]] = {
            market: [] for market in self.symbols
        }
        option_types_by_pair: dict[
            tuple[str, str, date, float, object, object], set[str]
        ] = {}
        symbol_to_market: dict[str, str] = {}
        if not self.universe.empty:
            columns = [
                "market",
                "symbol",
                "strike",
                "option_type",
                "expiration_date",
                "open_interest",
                "contract_multiplier",
                "multiplier",
                "settlement_type",
                "settlement",
            ]
            available_columns = [column for column in columns if column in self.universe.columns]
            for record in self.universe[available_columns].to_dict(orient="records"):
                market = str(record.get("market") or "").upper()
                raw_symbol = str(record.get("symbol") or "")
                if market not in records_by_market or not raw_symbol:
                    continue
                expiration = record.get("expiration_date")
                if isinstance(expiration, pd.Timestamp):
                    expiration = expiration.date()
                option_root = _option_root_from_symbol(raw_symbol)
                multiplier = record.get("contract_multiplier")
                if multiplier is None:
                    multiplier = record.get("multiplier")
                settlement = record.get("settlement_type")
                if settlement is None:
                    settlement = record.get("settlement")
                normalized = {
                    "market": market,
                    "symbol": raw_symbol,
                    "option_root": option_root,
                    "strike": float(record.get("strike") or 0.0),
                    "option_type": str(record.get("option_type") or ""),
                    "expiration_date": expiration,
                    "open_interest": float(record.get("open_interest") or 0.0),
                    "contract_multiplier": multiplier,
                    "settlement_type": settlement,
                }
                records_by_market[market].append(normalized)
                symbol_to_market[raw_symbol] = market
                option_type = str(normalized["option_type"])
                strike = float(normalized["strike"])
                if (
                    option_root
                    and isinstance(expiration, date)
                    and option_type in {"C", "P"}
                ):
                    option_types_by_pair.setdefault(
                        (
                            market,
                            option_root,
                            expiration,
                            strike,
                            multiplier,
                            settlement,
                        ),
                        set(),
                    ).add(option_type)
        self._universe_records_by_market = {
            market: tuple(records) for market, records in records_by_market.items()
        }
        paired_strikes: dict[tuple[str, date], set[float]] = {}
        for (
            market,
            _option_root,
            expiration,
            strike,
            _multiplier,
            _settlement,
        ), option_types in option_types_by_pair.items():
            if {"C", "P"}.issubset(option_types):
                paired_strikes.setdefault((market, expiration), set()).add(strike)
        self._paired_strikes_by_market_expiration = {
            key: np.asarray(sorted(strikes), dtype=np.float64)
            for key, strikes in paired_strikes.items()
        }
        self._symbol_to_market = symbol_to_market
        self._indexed_universe_frame_id = id(self.universe)
        self._indexed_universe_row_count = len(self.universe)

    def _ensure_universe_index(self) -> None:
        with self._universe_index_lock:
            if (
                self._indexed_universe_frame_id != id(self.universe)
                or self._indexed_universe_row_count != len(self.universe)
            ):
                self._index_universe_metadata_locked()

    def _market_for_raw_symbol(self, raw_symbol: str) -> str | None:
        market = self._symbol_to_market.get(raw_symbol)
        if market:
            return market
        root = str(raw_symbol).split()[0].strip().upper()
        for market_name in self.symbols:
            config = MARKETS[market_name]
            if root in {config.daily_root, config.label}:
                return market_name
        return None

    def _reset_fresh_quote_index(self) -> None:
        with self._prediction_publication_lock:
            self._fresh_market_observations.clear()
        with self._fresh_quote_lock:
            self._fresh_quote_counts_by_market = {symbol: 0 for symbol in self.symbols}
            self._fresh_quote_expirations.clear()
            self._fresh_quote_sequence = 0

    def _expire_fresh_quotes_locked(self, now_monotonic: float) -> None:
        while self._fresh_quote_expirations and self._fresh_quote_expirations[0][0] <= now_monotonic:
            _, _, raw_symbol, market, generation, received = heapq.heappop(
                self._fresh_quote_expirations
            )
            latest = self.quotes.get(raw_symbol)
            if not latest:
                continue
            if (
                int(latest.get("generation") or 0) == generation
                and float(latest.get("received_monotonic") or 0.0) == received
            ):
                self._fresh_quote_counts_by_market[market] = max(
                    0, self._fresh_quote_counts_by_market.get(market, 0) - 1
                )

    def _store_quote(self, raw_symbol: str, quote: dict[str, object]) -> None:
        """Replace one quote and update freshness in amortized O(1) time."""
        received = float(quote.get("received_monotonic") or time.monotonic())
        generation = int(quote.get("generation") or 0)
        market = self._market_for_raw_symbol(raw_symbol)
        with self._fresh_quote_lock:
            self._expire_fresh_quotes_locked(received)
            previous = self.quotes.get(raw_symbol)
            previous_was_fresh = bool(
                previous
                and market
                and int(previous.get("generation") or 0) == generation
                and float(previous.get("received_monotonic") or 0.0) >= self.subscription_cutoff_monotonic
                and received - float(previous.get("received_monotonic") or 0.0) <= QUOTE_FRESHNESS_SECONDS
            )
            self.quotes[raw_symbol] = quote
            if market:
                if not previous_was_fresh:
                    self._fresh_quote_counts_by_market[market] = (
                        self._fresh_quote_counts_by_market.get(market, 0) + 1
                    )
                self._fresh_quote_sequence += 1
                heapq.heappush(
                    self._fresh_quote_expirations,
                    (
                        received + QUOTE_FRESHNESS_SECONDS,
                        self._fresh_quote_sequence,
                        raw_symbol,
                        market,
                        generation,
                        received,
                    ),
                )

    @staticmethod
    def _mapping_record(record: object, symbol: str) -> dict[str, object]:
        payload: dict[str, object] = {
            "instrument_id": int(getattr(record, "instrument_id")),
            "raw_symbol": symbol,
            "stype_in_symbol": getattr(record, "stype_in_symbol", None),
            "stype_out_symbol": getattr(record, "stype_out_symbol", None),
            "ts_event_ns": _record_timestamp_ns(record, "ts_event"),
            "ts_index_ns": _record_timestamp_ns(record, "ts_index"),
            "start_ts_ns": _record_timestamp_ns(record, "start_ts"),
            "end_ts_ns": _record_timestamp_ns(record, "end_ts"),
        }
        payload["mapping_version"] = hashlib.sha256(
            json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
        ).hexdigest()
        return payload

    def _cache_hash(self) -> str:
        symbol_key = "-".join(sorted(self.symbols)) or "none"
        cache_key = f"{UNIVERSE_CACHE_VERSION}:{symbol_key}"
        return hashlib.sha1(cache_key.encode("utf-8")).hexdigest()[:10]

    def _cache_path(self, cache_date: date | None = None) -> Path:
        target_date = cache_date or current_market_date()
        return self.cache_dir / f"opra_universe_{target_date.isoformat()}_{self._cache_hash()}.csv"

    @staticmethod
    def _cache_metadata_path(cache_path: Path) -> Path:
        return cache_path.with_suffix(cache_path.suffix + ".metadata.json")

    def _load_cache_metadata(
        self, cache_path: Path, *, trading_date: date
    ) -> dict[str, object]:
        metadata_path = self._cache_metadata_path(cache_path)
        if not metadata_path.is_file():
            return {}
        try:
            raw = metadata_path.read_bytes()
            metadata = json.loads(raw.decode("utf-8"))
            expected_fields = {
                "contract_version",
                "trading_date",
                "source_date",
                "source_sha256",
                "provider_definition_end",
                "provider_statistics_end",
            }
            if not isinstance(metadata, dict):
                raise ValueError("universe cache metadata root is invalid")
            definition_end = datetime.fromisoformat(
                str(metadata.get("provider_definition_end") or "").replace(
                    "Z", "+00:00"
                )
            )
            statistics_end = datetime.fromisoformat(
                str(metadata.get("provider_statistics_end") or "").replace(
                    "Z", "+00:00"
                )
            )
            if (
                set(metadata) != expected_fields
                or metadata.get("contract_version") != UNIVERSE_CACHE_METADATA_VERSION
                or metadata.get("trading_date") != trading_date.isoformat()
                or metadata.get("source_date") != trading_date.isoformat()
                or metadata.get("source_sha256") != self._file_sha256(cache_path)
                or definition_end.tzinfo is None
                or statistics_end.tzinfo is None
                or raw != (
                    json.dumps(metadata, sort_keys=True, separators=(",", ":")) + "\n"
                ).encode("utf-8")
            ):
                raise ValueError("universe cache metadata identity is invalid")
            return metadata
        except (OSError, UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
            logger.warning("Ignoring invalid Databento universe cache metadata %s: %s", metadata_path, exc)
            return {}

    def _staged_market_cache_path(self, source_date: date, market: str) -> Path:
        return self.cache_dir / (
            f"opra_universe_fragment_{source_date.isoformat()}_"
            f"{self._cache_hash()}_{market}.csv"
        )

    @staticmethod
    def _file_sha256(path: Path) -> str:
        digest = hashlib.sha256()
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
        return digest.hexdigest()

    def _register_formula_event(self, market: str, reasons: list[str]) -> None:
        """Track rolling formula health for dashboard and alerting."""
        status = "valid" if not reasons else "invalid"
        self.formula_validation_events.append((time.monotonic(), f"{market}:{status}:{'|'.join(reasons)}"))

    def _formula_health_snapshot(self) -> dict[str, float | int | bool | None]:
        if not self.formula_validation_events:
            return {
                "formula_samples": 0,
                "formula_invalid_ratio": None,
                "formula_last_status": None,
                "formula_last_invalid_reasons": [],
            }

        window = self.formula_monitor_window_seconds
        now = time.monotonic()
        recent = [
            event
            for event in self.formula_validation_events
            if now - event[0] <= window
        ]
        total = len(recent)
        if total == 0:
            return {
                "formula_samples": 0,
                "formula_invalid_ratio": 0.0,
                "formula_last_status": None,
                "formula_last_invalid_reasons": [],
            }
        invalid = sum(1 for _, reason in recent if ":invalid:" in reason)
        latest_raw = recent[-1][1]
        last_status = latest_raw.split(":", 2)[1] if len(latest_raw.split(":", 2)) >= 2 else None
        last_reasons = latest_raw.split(":", 2)[2].split("|") if ":invalid:" in latest_raw else []
        return {
            "formula_samples": total,
            "formula_invalid_ratio": invalid / float(total),
            "formula_last_status": last_status,
            "formula_last_invalid_reasons": [reason for reason in last_reasons if reason],
        }

    def _apply_subscription_profile(
        self,
        full_universe: pd.DataFrame,
        *,
        as_of: date | None = None,
        provenance: dict[str, object] | None = None,
    ) -> None:
        trading_date = as_of or current_market_date()
        normalized_full = full_universe.copy()
        if not normalized_full.empty:
            normalized_full["expiration_date"] = pd.to_datetime(
                normalized_full["expiration_date"], errors="coerce"
            ).dt.date
            normalized_full["open_interest"] = pd.to_numeric(
                normalized_full["open_interest"], errors="coerce"
            ).fillna(0.0)
            normalized_full = normalized_full.dropna(
                subset=["market", "symbol", "expiration_date", "option_type", "strike"]
            ).drop_duplicates(subset=["symbol"], keep="last")
            normalized_full = normalized_full[
                normalized_full["expiration_date"] >= trading_date
            ].copy()
            normalized_full = _active_pair_universe(normalized_full)
        selected, metadata = select_subscription_universe(
            normalized_full,
            self.symbols,
            profile=self.subscription_profile,
            as_of=trading_date,
        )
        admission_required = set(self._universe_admission_required_markets())
        admission_metadata: dict[str, dict[str, object]] = {}
        optional_unavailable_reasons: dict[str, str] = {}
        for market in self.symbols:
            diagnostic = self.primary_pair_admission_diagnostics.get(market)
            if diagnostic is None:
                diagnostic = self._primary_pair_admission_diagnostic(
                    normalized_full,
                    market=market,
                    trading_date=trading_date,
                )
                diagnostic["required_for_universe_admission"] = (
                    market in admission_required
                )
                self.primary_pair_admission_diagnostics[market] = diagnostic
            admission_metadata[market] = dict(diagnostic)

            market_plan = metadata["markets"].setdefault(
                market,
                {
                    "full_contract_count": 0,
                    "selected_contract_count_before_global_cap": 0,
                    "selected_contract_count": 0,
                    "selected_expirations": [],
                },
            )
            selected_contract_count = int(
                market_plan.get("selected_contract_count") or 0
            )
            subscription_available = bool(
                diagnostic.get("passes") and selected_contract_count > 0
            )
            required_for_admission = bool(
                diagnostic.get("required_for_universe_admission")
            )
            optional_unavailable_reason: str | None = None
            if not required_for_admission and not subscription_available:
                optional_unavailable_reason = str(
                    diagnostic.get("reason") or "NO_OPTIONAL_CONTRACTS_SELECTED"
                )
                optional_unavailable_reasons[market] = optional_unavailable_reason
            market_plan["primary_pair_admission"] = dict(diagnostic)
            market_plan["required_for_universe_admission"] = required_for_admission
            market_plan["subscription_available"] = subscription_available
            market_plan["optional_family_unavailable_reason"] = (
                optional_unavailable_reason
            )

        metadata["primary_pair_admission"] = admission_metadata
        metadata["optional_family_unavailable_reasons"] = (
            optional_unavailable_reasons
        )
        resolved_provenance = provenance or {
            "mode": "in_memory",
            "trading_date": trading_date.isoformat(),
            "is_fallback": False,
        }
        metadata["universe_provenance"] = resolved_provenance
        metadata["watchdog_policy"] = {
            "open_transition_grace_seconds": OPEN_TRANSITION_GRACE_SECONDS,
            "allow_live_universe_refresh_reconnect": ALLOW_LIVE_UNIVERSE_REFRESH_RECONNECT,
        }
        metadata["definition_discovery"] = {
            market: dict(values)
            for market, values in self.definition_discovery_metadata.items()
            if market in self.symbols
        }
        oi_analytics = build_full_oi_analytics(
            normalized_full,
            self.symbols,
            as_of=trading_date,
            provenance=resolved_provenance,
        )
        live_symbols = (
            selected["symbol"].astype(str).tolist() if not selected.empty else []
        )
        # Publish the selected frame, all indexes, and their provenance as one
        # coherent state transition. Sampler workers hold the same lock while
        # copying a market snapshot, so they can see either plan, never a mix.
        with self._universe_index_lock:
            self.full_universe = normalized_full
            self.universe = selected
            self._index_universe_metadata_locked()
            self.oi_analytics_by_market = oi_analytics
            self.live_symbols = live_symbols
            self.subscription_metadata = metadata
        logger.info(
            "Databento subscription profile %s selected %s of %s contracts: %s",
            self.subscription_profile,
            len(self.live_symbols),
            metadata.get("full_contract_count"),
            metadata.get("markets"),
        )

    def _primary_pair_admission_diagnostic(
        self,
        full_universe: pd.DataFrame,
        *,
        market: str,
        trading_date: date,
    ) -> dict[str, object]:
        if full_universe.empty:
            return {
                "passes": False,
                "reason": "NO_POSITIVE_OI_CONTRACTS",
                "complete_pair_count": 0,
                "primary_strike_count": 0,
                "pair_completeness_ratio": 0.0,
            }
        frame = full_universe.copy()
        frame["expiration_date"] = pd.to_datetime(frame["expiration_date"], errors="coerce").dt.date
        frame["open_interest"] = pd.to_numeric(frame["open_interest"], errors="coerce").fillna(0.0)
        frame["_root"] = frame["symbol"].map(_option_root_from_symbol)
        frame = frame.dropna(subset=["expiration_date"])
        frame = frame[frame["market"] == market]
        had_settlement_ineligible_vix = bool(
            market == "VIX"
            and not frame.empty
            and (frame["expiration_date"] == trading_date).any()
        )
        frame = frame[
            frame["expiration_date"].map(
                lambda expiration: _expiration_is_live_eligible(
                    market, expiration, trading_date
                )
            )
        ]
        if frame.empty:
            return {
                "passes": False,
                "reason": (
                    "NO_FORWARD_VIX_EXPIRATION_AVAILABLE"
                    if had_settlement_ineligible_vix
                    else "NO_POSITIVE_OI_CONTRACTS"
                ),
                "complete_pair_count": 0,
                "primary_strike_count": 0,
                "pair_completeness_ratio": 0.0,
            }
        primary = trading_date if market in {"SPX", "NDX"} else frame["expiration_date"].min()
        primary_frame = frame[frame["expiration_date"] == primary]
        coverage = primary_frame.groupby(["_root", "strike"])["option_type"].agg(
            lambda values: {str(value).upper() for value in values}
        )
        active_strikes = primary_frame.groupby(["_root", "strike"])["open_interest"].max() > 0
        coverage = coverage[active_strikes]
        complete_pair_count = sum({"C", "P"}.issubset(types) for types in coverage)
        primary_strike_count = int(active_strikes.sum())
        completeness_ratio = complete_pair_count / primary_strike_count if primary_strike_count else 0.0
        if market in {"SPX", "NDX"}:
            minimum_pair_count = MIN_PRIMARY_STRIKE_PAIRS
        elif market in {"VIX", "RUT"}:
            minimum_pair_count = max(5, MIN_PAIRED_QUOTES)
        else:
            minimum_pair_count = 1
        minimum_ratio = MIN_CORE_PRIMARY_PAIR_COMPLETENESS_RATIO if market in {"SPX", "NDX"} else 0.0
        passes = complete_pair_count >= minimum_pair_count and completeness_ratio >= minimum_ratio
        return {
            "passes": passes,
            "reason": None if passes else "PRIMARY_PAIR_COVERAGE_INCOMPLETE",
            "primary_expiration": primary.isoformat(),
            "complete_pair_count": int(complete_pair_count),
            "primary_strike_count": primary_strike_count,
            "pair_completeness_ratio": round(float(completeness_ratio), 6),
            "minimum_pair_count": int(minimum_pair_count),
            "minimum_pair_completeness_ratio": float(minimum_ratio),
        }

    def _universe_admission_required_markets(self) -> tuple[str, ...]:
        """Return families which may reject a universe before publication.

        In the production multi-family stream, configured handoff requirements
        (normally SPX and NDX) are the admission authority.  A standalone
        family still gates itself so focused/research invocations cannot accept
        an empty unusable universe merely because no core sibling was requested.
        """
        configured_required = tuple(
            market
            for market in self.required_handoff_symbols
            if market in self.symbols
        )
        return configured_required or tuple(self.symbols)

    def _optional_family_unavailable_reasons(self) -> dict[str, str]:
        return {
            market: str(
                diagnostic.get("reason") or "NO_OPTIONAL_CONTRACTS_SELECTED"
            )
            for market, diagnostic in self.primary_pair_admission_diagnostics.items()
            if not diagnostic.get("required_for_universe_admission")
            and not diagnostic.get("passes")
        }

    def _has_required_primary_pair_coverage(
        self,
        full_universe: pd.DataFrame,
        *,
        trading_date: date,
    ) -> bool:
        self.primary_pair_admission_diagnostics = {}
        admission_required = set(self._universe_admission_required_markets())
        ordered_markets = [
            market for market in self.required_handoff_symbols if market in self.symbols
        ]
        ordered_markets.extend(
            market for market in self.symbols if market not in ordered_markets
        )
        for market in ordered_markets:
            diagnostic = self._primary_pair_admission_diagnostic(
                full_universe,
                market=market,
                trading_date=trading_date,
            )
            diagnostic["required_for_universe_admission"] = (
                market in admission_required
            )
            self.primary_pair_admission_diagnostics[market] = diagnostic
        return bool(admission_required) and all(
            self.primary_pair_admission_diagnostics[market]["passes"]
            for market in admission_required
        )

    def _load_cached_universe(
        self, trading_date: date | None = None, *, ignore_refresh_flag: bool = False
    ) -> bool:
        target_date = trading_date or current_market_date()
        cache_path = self._cache_path(target_date)
        if not cache_path.exists() or (
            os.getenv("DATABENTO_REFRESH_CACHE", "0") == "1" and not ignore_refresh_flag
        ):
            return False
        cache_age = time.time() - cache_path.stat().st_mtime
        refresh_recommended = cache_age >= UNIVERSE_REFRESH_SECONDS
        if refresh_recommended:
            logger.info(
                "Databento current-day universe cache is %.0fs old; retaining its validated scaffold while an atomic metadata refresh is recommended",
                cache_age,
            )
        try:
            full_universe = pd.read_csv(cache_path)
            full_universe["expiration_date"] = pd.to_datetime(full_universe["expiration_date"]).dt.date
            if not self._has_required_primary_pair_coverage(
                full_universe, trading_date=target_date
            ):
                logger.warning(
                    "Skipping current-day Databento cache %s; required primary call/put pairs are incomplete",
                    cache_path,
                )
                return False
            retained_metadata = self._load_cache_metadata(
                cache_path, trading_date=target_date
            )
            self._apply_subscription_profile(
                full_universe,
                as_of=target_date,
                provenance={
                    "mode": "current_day_cache",
                    "trading_date": target_date.isoformat(),
                    "source_date": target_date.isoformat(),
                    "source_path": str(cache_path),
                    "source_sha256": self._file_sha256(cache_path),
                    "is_fallback": False,
                    "source_rows": int(len(full_universe)),
                    "cache_age_seconds": round(max(0.0, cache_age), 3),
                    "refresh_recommended": refresh_recommended,
                    "provider_definition_end": retained_metadata.get(
                        "provider_definition_end"
                    ),
                    "provider_statistics_end": retained_metadata.get(
                        "provider_statistics_end"
                    ),
                    "cache_metadata_status": (
                        "verified" if retained_metadata else "missing_or_invalid"
                    ),
                },
            )
            self._universe_built_monotonic = time.monotonic()
            logger.info(
                "Loaded Databento universe cache: %s selected symbols from %s",
                len(self.live_symbols),
                cache_path,
            )
            return bool(self.live_symbols)
        except Exception as exc:
            logger.warning("Failed to load Databento universe cache: %s", exc)
            return False

    def _load_prior_cached_universe(
        self,
        *,
        trading_date: date,
        reason: str,
        provider_definition_end: datetime | None = None,
        provider_statistics_end: datetime | None = None,
    ) -> bool:
        """Use a recent cache only as a filtered, explicitly labeled universe scaffold."""
        if not ALLOW_PRIOR_UNIVERSE_FALLBACK:
            return False

        pattern = re.compile(
            rf"^opra_universe_(?P<cache_date>\d{{4}}-\d{{2}}-\d{{2}})_{re.escape(self._cache_hash())}\.csv$"
        )
        candidates: list[tuple[date, Path]] = []
        for path in self.cache_dir.glob(f"opra_universe_*_{self._cache_hash()}.csv"):
            match = pattern.match(path.name)
            if not match:
                continue
            try:
                source_date = date.fromisoformat(match.group("cache_date"))
            except ValueError:
                continue
            age_days = (trading_date - source_date).days
            if 1 <= age_days <= UNIVERSE_FALLBACK_MAX_AGE_DAYS:
                candidates.append((source_date, path))

        for source_date, path in sorted(candidates, reverse=True):
            try:
                source = pd.read_csv(path)
                required = {"market", "symbol", "expiration_date", "option_type", "strike", "open_interest"}
                missing = sorted(required.difference(source.columns))
                if missing:
                    logger.warning("Skipping prior Databento cache %s; missing columns: %s", path, missing)
                    continue

                source_rows = int(len(source))
                source["expiration_date"] = pd.to_datetime(source["expiration_date"], errors="coerce").dt.date
                source = source.dropna(subset=["market", "symbol", "expiration_date", "option_type", "strike"])
                source = source[source["market"].isin(self.symbols)].copy()
                source = source[source["expiration_date"] >= trading_date].copy()
                source = source.drop_duplicates(subset=["symbol"], keep="last")
                eligible_rows = int(len(source))
                if source.empty:
                    continue

                available_markets = set(source["market"].astype(str))
                admission_required = set(
                    self._universe_admission_required_markets()
                )
                missing_required_markets = sorted(
                    admission_required.difference(available_markets)
                )
                if missing_required_markets:
                    logger.warning(
                        "Skipping prior Databento cache %s; required markets missing after filtering: %s",
                        path,
                        missing_required_markets,
                    )
                    continue

                missing_same_day = [
                    market
                    for market in admission_required
                    if market in {"SPX", "NDX"}
                    and not (
                        (source["market"] == market)
                        & (source["expiration_date"] == trading_date)
                    ).any()
                ]
                if missing_same_day:
                    logger.warning(
                        "Skipping prior Databento cache %s; no same-day contracts for %s",
                        path,
                        missing_same_day,
                    )
                    continue

                if not self._has_required_primary_pair_coverage(
                    source, trading_date=trading_date
                ):
                    logger.warning(
                        "Skipping prior Databento cache %s; required primary same-strike call/put coverage is incomplete",
                        path,
                    )
                    continue

                retained_metadata = self._load_cache_metadata(
                    path, trading_date=source_date
                )
                provenance = {
                    "mode": "prior_cache_filtered",
                    "trading_date": trading_date.isoformat(),
                    "source_date": source_date.isoformat(),
                    "source_path": str(path),
                    "source_sha256": self._file_sha256(path),
                    "is_fallback": True,
                    "reason": reason,
                    "source_rows": source_rows,
                    "eligible_rows": eligible_rows,
                    "dropped_rows": source_rows - eligible_rows,
                    "provider_definition_end": retained_metadata.get(
                        "provider_definition_end"
                    ),
                    "provider_statistics_end": retained_metadata.get(
                        "provider_statistics_end"
                    ),
                    "cache_metadata_status": (
                        "verified" if retained_metadata else "missing_or_invalid"
                    ),
                }
                self._apply_subscription_profile(source, as_of=trading_date, provenance=provenance)
                selected_markets = set(self.universe["market"].astype(str)) if not self.universe.empty else set()
                if admission_required.difference(selected_markets):
                    continue

                self._universe_built_monotonic = time.monotonic()
                logger.warning(
                    "Using filtered prior Databento universe cache %s for %s: %s source rows, %s eligible, %s selected; live quotes remain required",
                    path,
                    trading_date,
                    source_rows,
                    eligible_rows,
                    len(self.live_symbols),
                )
                return bool(self.live_symbols)
            except Exception as exc:
                logger.warning("Failed to load prior Databento universe cache %s: %s", path, exc)
        return False

    def _save_cached_universe(
        self,
        full_universe: pd.DataFrame | None = None,
        *,
        trading_date: date | None = None,
        mark_active: bool = True,
        provider_definition_end: datetime | None = None,
        provider_statistics_end: datetime | None = None,
    ) -> Path | None:
        frame = full_universe if full_universe is not None else self.full_universe
        if frame.empty:
            return
        target_date = trading_date or current_market_date()
        cache_path = self._cache_path(target_date)
        temporary_path = cache_path.with_suffix(cache_path.suffix + f".{uuid.uuid4().hex}.tmp")
        metadata_path = self._cache_metadata_path(cache_path)
        temporary_metadata_path = metadata_path.with_suffix(
            metadata_path.suffix + f".{uuid.uuid4().hex}.tmp"
        )
        try:
            frame.to_csv(temporary_path, index=False)
            os.replace(temporary_path, cache_path)
            if provider_definition_end is not None and provider_statistics_end is not None:
                if (
                    provider_definition_end.tzinfo is None
                    or provider_statistics_end.tzinfo is None
                ):
                    raise ValueError("provider cache cutoffs must be timezone-aware")
                metadata = {
                    "contract_version": UNIVERSE_CACHE_METADATA_VERSION,
                    "trading_date": target_date.isoformat(),
                    "source_date": target_date.isoformat(),
                    "source_sha256": self._file_sha256(cache_path),
                    "provider_definition_end": provider_definition_end.astimezone(
                        timezone.utc
                    ).isoformat(),
                    "provider_statistics_end": provider_statistics_end.astimezone(
                        timezone.utc
                    ).isoformat(),
                }
                temporary_metadata_path.write_text(
                    json.dumps(metadata, sort_keys=True, separators=(",", ":")) + "\n",
                    encoding="utf-8",
                )
                os.replace(temporary_metadata_path, metadata_path)
            else:
                metadata_path.unlink(missing_ok=True)
            if mark_active:
                self._universe_built_monotonic = time.monotonic()
            logger.info("Saved Databento universe cache: %s", cache_path)
            return cache_path
        except Exception as exc:
            logger.warning("Failed to save Databento universe cache: %s", exc)
            try:
                temporary_path.unlink(missing_ok=True)
                temporary_metadata_path.unlink(missing_ok=True)
            except OSError:
                pass
            return None

    def _load_staged_market_universe(
        self,
        *,
        market: str,
        source_date: date,
        admission_date: date,
        request_definition_end: datetime,
        request_statistics_end: datetime,
    ) -> tuple[pd.DataFrame, dict[str, object]] | None:
        """Load one validated, exact-day provider fragment from an earlier attempt.

        Fragments are never subscription caches. They may only avoid repeating a
        completed market family inside another staging attempt; the caller must
        still admit every configured family and atomically publish the aggregate.
        """
        fragment_path = self._staged_market_cache_path(source_date, market)
        if not fragment_path.exists():
            return None
        try:
            staged = pd.read_csv(fragment_path)
            required = {
                "market",
                "symbol",
                "expiration_date",
                "option_type",
                "strike",
                "open_interest",
                *UNIVERSE_STAGE_FRAGMENT_COLUMNS,
            }
            missing = sorted(required.difference(staged.columns))
            if missing:
                raise ValueError(f"missing columns: {missing}")
            if staged.empty:
                raise ValueError("fragment has no rows")

            def singleton(column: str) -> str:
                values = {str(value) for value in staged[column].dropna().unique()}
                if len(values) != 1:
                    raise ValueError(f"{column} is not constant")
                return values.pop()

            if singleton("_stage_fragment_version") != UNIVERSE_STAGE_FRAGMENT_VERSION:
                raise ValueError("fragment version does not match")
            if singleton("_stage_cache_hash") != self._cache_hash():
                raise ValueError("configured market set does not match")
            if singleton("_stage_source_date") != source_date.isoformat():
                raise ValueError("source date does not match")
            if singleton("_stage_market") != market:
                raise ValueError("market does not match")
            if int(float(singleton("_stage_row_count"))) != len(staged):
                raise ValueError("row count does not match")

            stored_definition_end = parse_db_time(singleton("_stage_definition_end"))
            stored_statistics_end = parse_db_time(singleton("_stage_statistics_end"))
            source_start = datetime.combine(source_date, datetime.min.time(), tzinfo=timezone.utc)
            source_stop = source_start + timedelta(days=1)
            for label, stored_end, current_end in (
                ("definition", stored_definition_end, request_definition_end),
                ("statistics", stored_statistics_end, request_statistics_end),
            ):
                if stored_end.tzinfo is None:
                    raise ValueError(f"{label} endpoint is timezone-naive")
                if not source_start < stored_end <= source_stop:
                    raise ValueError(f"{label} endpoint is outside the exact source day")
                if stored_end > current_end:
                    raise ValueError(f"{label} endpoint exceeds current provider availability")

            discovery_values = staged["_stage_definition_discovery"].dropna()
            discovery_raw = next(
                (str(value) for value in discovery_values if str(value).strip()),
                "{}",
            )
            discovery = json.loads(discovery_raw)
            if not isinstance(discovery, dict):
                raise ValueError("definition discovery metadata is not an object")
            discovery_date = discovery.get("trading_date")
            if discovery_date not in (None, source_date.isoformat()):
                raise ValueError("definition discovery date does not match")

            frame = staged.drop(columns=list(UNIVERSE_STAGE_FRAGMENT_COLUMNS)).copy()
            if frame["symbol"].duplicated().any():
                raise ValueError("fragment contains duplicate symbols")
            if set(frame["market"].astype(str)) != {market}:
                raise ValueError("fragment contains another market")
            frame["expiration_date"] = pd.to_datetime(
                frame["expiration_date"], errors="coerce"
            ).dt.date
            frame["strike"] = pd.to_numeric(frame["strike"], errors="coerce")
            frame["open_interest"] = pd.to_numeric(
                frame["open_interest"], errors="coerce"
            )
            if frame[["expiration_date", "strike", "open_interest"]].isna().any().any():
                raise ValueError("fragment contains invalid typed values")
            active = _active_pair_universe(frame)
            if len(active) != len(frame):
                raise ValueError("fragment is no longer pair-complete and OI-active")
            diagnostic = self._primary_pair_admission_diagnostic(
                active,
                market=market,
                trading_date=admission_date,
            )
            if not diagnostic["passes"]:
                raise ValueError(f"fragment failed admission: {diagnostic}")

            source_status = discovery.get("status")
            discovery.update(
                {
                    "status": "reused_staged_fragment",
                    "staged_source_status": source_status,
                    "staged_fragment_path": str(fragment_path),
                    "staged_fragment_sha256": self._file_sha256(fragment_path),
                    "staged_fragment_saved_at_utc": singleton("_stage_saved_at_utc"),
                    "staged_provider_request_definition_end": stored_definition_end.isoformat(),
                    "staged_provider_request_statistics_end": stored_statistics_end.isoformat(),
                }
            )
            logger.info(
                "Reused validated Databento %s universe fragment for %s: %s rows",
                market,
                source_date,
                len(active),
            )
            return active, discovery
        except Exception as exc:
            logger.warning(
                "Ignoring Databento %s universe fragment %s: %s",
                market,
                fragment_path,
                exc,
            )
            return None

    def _save_staged_market_universe(
        self,
        frame: pd.DataFrame,
        *,
        market: str,
        source_date: date,
        admission_date: date,
        request_definition_end: datetime,
        request_statistics_end: datetime,
    ) -> Path | None:
        """Atomically checkpoint one fully admitted provider market family."""
        prepared = frame.drop(
            columns=list(UNIVERSE_STAGE_FRAGMENT_COLUMNS), errors="ignore"
        ).drop_duplicates(subset=["symbol"], keep="last")
        diagnostic = self._primary_pair_admission_diagnostic(
            prepared,
            market=market,
            trading_date=admission_date,
        )
        if set(prepared["market"].astype(str)) != {market} or not diagnostic["passes"]:
            logger.warning(
                "Not checkpointing incomplete Databento %s universe fragment for %s: %s",
                market,
                source_date,
                diagnostic,
            )
            return None

        staged = prepared.copy().reset_index(drop=True)
        staged["expiration_date"] = pd.to_datetime(
            staged["expiration_date"], errors="raise"
        ).dt.strftime("%Y-%m-%d")
        staged["_stage_fragment_version"] = UNIVERSE_STAGE_FRAGMENT_VERSION
        staged["_stage_cache_hash"] = self._cache_hash()
        staged["_stage_source_date"] = source_date.isoformat()
        staged["_stage_market"] = market
        staged["_stage_definition_end"] = request_definition_end.isoformat()
        staged["_stage_statistics_end"] = request_statistics_end.isoformat()
        staged["_stage_row_count"] = len(staged)
        staged["_stage_saved_at_utc"] = datetime.now(timezone.utc).isoformat()
        staged["_stage_definition_discovery"] = ""
        staged.loc[0, "_stage_definition_discovery"] = json.dumps(
            self.definition_discovery_metadata.get(market, {}),
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        )

        fragment_path = self._staged_market_cache_path(source_date, market)
        temporary_path = fragment_path.with_suffix(
            fragment_path.suffix + f".{uuid.uuid4().hex}.tmp"
        )
        try:
            staged.to_csv(temporary_path, index=False)
            os.replace(temporary_path, fragment_path)
            logger.info(
                "Checkpointed Databento %s universe fragment for %s: %s rows",
                market,
                source_date,
                len(staged),
            )
            return fragment_path
        except Exception as exc:
            logger.warning(
                "Failed to checkpoint Databento %s universe fragment for %s: %s",
                market,
                source_date,
                exc,
            )
            try:
                temporary_path.unlink(missing_ok=True)
            except OSError:
                pass
            return None

    def add_callback(self, callback: Callable):
        self.callbacks.append(callback)

    def _generate_symbols(self, config: DatabentoMarketConfig) -> list[str]:
        today = current_market_date()
        values: list[str] = []
        roots = [config.daily_root]
        if config.label in {"SPX", "NDX"} and config.label not in roots:
            roots.append(config.label)
        for root in roots:
            for strike in range(config.strike_min, config.strike_max + config.strike_step, config.strike_step):
                values.append(raw_option_symbol(root, today, "C", strike))
                values.append(raw_option_symbol(root, today, "P", strike))
        return values

    def _definition_parents(self, config: DatabentoMarketConfig) -> list[str]:
        roots = [config.daily_root]
        if config.label not in roots:
            roots.append(config.label)
        return [f"{root}.OPT" for root in roots]

    def _fetch_definition_universe(
        self,
        historical: db.Historical,
        config: DatabentoMarketConfig,
        *,
        trading_date: date | None = None,
        available_end: datetime | None = None,
    ) -> pd.DataFrame:
        today = trading_date or current_market_date()
        end = available_end or self._available_end(historical, "definition")
        start_dt = datetime.combine(today, datetime.min.time(), tzinfo=timezone.utc)
        if end <= start_dt:
            logger.warning("Databento definition schema has no data available for %s yet (start=%s end=%s)", today, start_dt, end)
            return pd.DataFrame(columns=["market", "symbol", "expiration_date", "option_type", "strike"])
        frames: list[pd.DataFrame] = []
        provider_errors: list[dict[str, str]] = []

        for parent in self._definition_parents(config):
            try:
                data = historical.timeseries.get_range(
                    dataset=DATASET,
                    schema="definition",
                    symbols=parent,
                    stype_in="parent",
                    start=today.isoformat(),
                    end=end,
                )
            except db.BentoClientError as exc:
                logger.warning("Databento definition discovery failed for %s: %s", parent, exc)
                details = exc.args[0] if exc.args and isinstance(exc.args[0], dict) else {}
                provider_errors.append({
                    "parent": parent,
                    "case": str(details.get("case") or type(exc).__name__),
                    "message": str(details.get("message") or str(exc).splitlines()[0]),
                })
                continue
            frame = data.to_df()
            if not frame.empty:
                frames.append(frame.reset_index(drop=False))

        if not frames:
            self.definition_discovery_metadata[config.label] = {
                "trading_date": today.isoformat(),
                "request_start": start_dt.isoformat(),
                "request_end": end.isoformat(),
                "parents": self._definition_parents(config),
                "query_mode": "historical-parent-unbounded",
                "status": "unavailable",
                "provider_errors": provider_errors,
            }
            return pd.DataFrame(columns=["market", "symbol", "expiration_date", "option_type", "strike"])

        definitions = pd.concat(frames, axis=0).drop_duplicates(subset=["symbol"], keep="last")
        definitions["expiration_date"] = pd.to_datetime(definitions["expiration"], errors="coerce").dt.date
        definitions["option_type"] = definitions["instrument_class"].astype(str)
        definitions["strike"] = pd.to_numeric(definitions["strike_price"], errors="coerce")
        definitions["market"] = config.label
        definitions = definitions[
            definitions["option_type"].isin(["C", "P"]) &
            definitions["strike"].notna()
        ].copy()
        family_contract_count = int(len(definitions))
        expiration_cutoff = today + timedelta(days=min(config.days_forward, MULTI_EXPIRATION_MAX_DTE))
        definitions = definitions[
            definitions["expiration_date"].between(today, expiration_cutoff, inclusive="both")
        ].copy()
        self.definition_discovery_metadata[config.label] = {
            "trading_date": today.isoformat(),
            "expiration_cutoff": expiration_cutoff.isoformat(),
            "max_dte": int((expiration_cutoff - today).days),
            "family_contract_count": family_contract_count,
            "eligible_contract_count": int(len(definitions)),
            "contracts_dropped_outside_expiration_window": family_contract_count - int(len(definitions)),
            "filter_stage": "before-open-interest-requests",
            "request_start": start_dt.isoformat(),
            "request_end": end.isoformat(),
            "parents": self._definition_parents(config),
            "query_mode": "historical-parent-unbounded",
            "status": "ready",
            "provider_errors": provider_errors,
        }
        return definitions[["market", "symbol", "expiration_date", "option_type", "strike"]]

    def _available_end(self, historical: db.Historical, schema: str) -> datetime:
        range_info = historical.metadata.get_dataset_range(dataset=DATASET)
        return parse_db_time(range_info["schema"][schema]["end"])

    def _fetch_open_interest(
        self,
        historical: db.Historical,
        symbols: list[str],
        *,
        trading_date: date | None = None,
        available_end: datetime | None = None,
    ) -> pd.DataFrame:
        target_date = trading_date or current_market_date()
        today = target_date.isoformat()
        end = available_end or self._available_end(historical, "statistics")
        start_dt = datetime.combine(target_date, datetime.min.time(), tzinfo=timezone.utc)
        if end <= start_dt:
            logger.warning("Databento statistics schema has no data available for %s yet (start=%s end=%s)", today, start_dt, end)
            return pd.DataFrame(columns=["symbol", "open_interest"])
        frames: list[pd.DataFrame] = []
        for symbol_chunk in chunks(symbols):
            try:
                data = historical.timeseries.get_range(
                    dataset=DATASET,
                    schema="statistics",
                    symbols=symbol_chunk,
                    stype_in="raw_symbol",
                    start=today,
                    end=end,
                )
            except db.BentoClientError as exc:
                if "None of the symbols could be resolved" in str(exc):
                    continue
                raise
            frame = data.to_df()
            if not frame.empty:
                frames.append(frame)
        if not frames:
            return pd.DataFrame(columns=["symbol", "open_interest"])
        df = pd.concat(frames, axis=0)
        oi = df[df["stat_type"] == OI_STAT_TYPE].copy()
        if oi.empty:
            return pd.DataFrame(columns=["symbol", "open_interest"])
        return oi.groupby("symbol", as_index=False)["quantity"].max().rename(columns={"quantity": "open_interest"})

    def _fetch_parent_open_interest(
        self,
        historical: db.Historical,
        config: DatabentoMarketConfig,
        candidate_symbols: list[str],
        *,
        trading_date: date,
        available_end: datetime,
    ) -> pd.DataFrame:
        """Fetch staged-cache OI with bounded documented parent requests.

        Databento supports ``[ROOT].OPT`` with ``stype_in='parent'`` for OPRA
        Historical schemas, including ``statistics``.  Querying once per parent
        avoids hundreds of sequential raw-symbol chunks during controlled
        pre-start preparation.  The returned rows are still restricted to the
        exact definition candidates before they can enter the cache.
        """
        if not candidate_symbols:
            return pd.DataFrame(columns=["symbol", "open_interest"])
        frames: list[pd.DataFrame] = []
        for parent in self._definition_parents(config):
            try:
                data = historical.timeseries.get_range(
                    dataset=DATASET,
                    schema="statistics",
                    symbols=parent,
                    stype_in="parent",
                    start=trading_date.isoformat(),
                    end=available_end,
                )
            except db.BentoClientError as exc:
                logger.warning("Databento OI discovery failed for %s: %s", parent, exc)
                continue
            frame = data.to_df()
            if not frame.empty:
                frames.append(frame.reset_index(drop=False))
        if not frames:
            return pd.DataFrame(columns=["symbol", "open_interest"])
        statistics = pd.concat(frames, axis=0)
        statistics = statistics[
            (statistics["stat_type"] == OI_STAT_TYPE)
            & statistics["symbol"].isin(set(candidate_symbols))
        ].copy()
        if statistics.empty:
            return pd.DataFrame(columns=["symbol", "open_interest"])
        return (
            statistics.groupby("symbol", as_index=False)["quantity"]
            .max()
            .rename(columns={"quantity": "open_interest"})
        )

    def _stage_provider_universe_cache(
        self,
        *,
        historical: db.Historical,
        source_date: date,
        admission_date: date,
        definition_end: datetime,
        statistics_end: datetime,
        is_fallback: bool,
    ) -> dict[str, object]:
        """Stage one exact provider-day without touching the live subscription."""
        source_start = datetime.combine(source_date, datetime.min.time(), tzinfo=timezone.utc)
        source_stop = source_start + timedelta(days=1)
        request_definition_end = min(definition_end, source_stop)
        request_statistics_end = min(statistics_end, source_stop)
        if request_definition_end <= source_start or request_statistics_end <= source_start:
            raise RuntimeError(
                f"Databento has not published a complete OPRA universe for {source_date} "
                f"(definition_end={definition_end.isoformat()}, statistics_end={statistics_end.isoformat()})"
            )

        # A failed current-day attempt must not leak its diagnostics into a
        # separately sourced prior-session cache.
        self.definition_discovery_metadata = {}
        frames: list[pd.DataFrame] = []
        for symbol in self.symbols:
            config = MARKETS[symbol]
            staged_fragment = self._load_staged_market_universe(
                market=symbol,
                source_date=source_date,
                admission_date=admission_date,
                request_definition_end=request_definition_end,
                request_statistics_end=request_statistics_end,
            )
            if staged_fragment is not None:
                frame, discovery = staged_fragment
                self.definition_discovery_metadata[symbol] = discovery
                frames.append(frame)
                continue
            frame = self._fetch_definition_universe(
                historical,
                config,
                trading_date=source_date,
                available_end=request_definition_end,
            )
            candidates = sorted(frame["symbol"].dropna().unique().tolist()) if not frame.empty else []
            if not candidates:
                continue
            oi = self._fetch_parent_open_interest(
                historical,
                config,
                candidates,
                trading_date=source_date,
                available_end=request_statistics_end,
            )
            frame = frame.merge(oi, on="symbol", how="left")
            frame["open_interest"] = pd.to_numeric(
                frame["open_interest"], errors="coerce"
            ).fillna(0.0)
            active = _active_pair_universe(frame)
            if not active.empty:
                frames.append(active)
                self._save_staged_market_universe(
                    active,
                    market=symbol,
                    source_date=source_date,
                    admission_date=admission_date,
                    request_definition_end=request_definition_end,
                    request_statistics_end=request_statistics_end,
                )
        if not frames:
            raise RuntimeError(f"No provider-backed Databento OPRA universe for {source_date}")
        full_universe = pd.concat(frames, axis=0).drop_duplicates(subset=["symbol"])
        if not self._has_required_primary_pair_coverage(
            full_universe, trading_date=admission_date
        ):
            raise RuntimeError(
                f"Provider-backed Databento universe sourced from {source_date} lacks "
                f"required primary call/put pairs for {admission_date}"
            )
        cache_path = self._save_cached_universe(
            full_universe,
            trading_date=source_date,
            mark_active=False,
            provider_definition_end=definition_end,
            provider_statistics_end=statistics_end,
        )
        if cache_path is None:
            raise RuntimeError(f"Failed to atomically stage Databento universe cache for {source_date}")
        return {
            "mode": (
                "databento_prior_session_discovery_staged"
                if is_fallback
                else "databento_discovery_staged"
            ),
            "trading_date": admission_date.isoformat(),
            "source_date": source_date.isoformat(),
            "source_path": str(cache_path),
            "source_sha256": self._file_sha256(cache_path),
            "is_fallback": is_fallback,
            "source_rows": int(len(full_universe)),
            "provider_definition_end": definition_end.isoformat(),
            "provider_statistics_end": statistics_end.isoformat(),
            "provider_request_definition_end": request_definition_end.isoformat(),
            "provider_request_statistics_end": request_statistics_end.isoformat(),
            "definition_discovery": {
                market: dict(values)
                for market, values in self.definition_discovery_metadata.items()
                if market in self.symbols
            },
            "primary_pair_admission": {
                market: dict(values)
                for market, values in self.primary_pair_admission_diagnostics.items()
            },
            "optional_family_unavailable_reasons": (
                self._optional_family_unavailable_reasons()
            ),
            "applied_to_live_subscription": False,
        }

    def stage_current_day_universe_cache(
        self,
        *,
        trading_date: date | None = None,
    ) -> dict[str, object]:
        """Build an atomic provider-backed cache without touching the live subscription.

        This is intentionally suitable for a separate pre-open maintenance job.
        It never applies a universe, stops a client, advances a generation, or
        relabels a prior-session scaffold as current-day discovery.
        """
        if not self.api_key:
            raise RuntimeError("DATABENTO_API_KEY is not set")
        target_date = trading_date or current_market_date()
        historical = db.Historical(self.api_key)
        definition_end = self._available_end(historical, "definition")
        statistics_end = self._available_end(historical, "statistics")
        return self._stage_provider_universe_cache(
            historical=historical,
            source_date=target_date,
            admission_date=target_date,
            definition_end=definition_end,
            statistics_end=statistics_end,
            is_fallback=False,
        )

    def stage_prior_session_universe_cache(
        self,
        *,
        trading_date: date | None = None,
    ) -> dict[str, object]:
        """Stage the newest complete prior provider day as fallback evidence.

        The bounded lookback only bridges weekends/holidays.  Every candidate is
        requested using its own exact UTC day, admitted against the target
        trading date, saved under its source date, and later loaded only through
        the explicit ``prior_cache_filtered`` path.
        """
        if not self.api_key:
            raise RuntimeError("DATABENTO_API_KEY is not set")
        target_date = trading_date or current_market_date()
        historical = db.Historical(self.api_key)
        definition_end = self._available_end(historical, "definition")
        statistics_end = self._available_end(historical, "statistics")
        failures: list[str] = []
        lookback_days = min(
            PROVIDER_PRIOR_CACHE_LOOKBACK_DAYS,
            max(1, UNIVERSE_FALLBACK_MAX_AGE_DAYS),
        )
        for calendar_days_back in range(1, lookback_days + 1):
            source_date = target_date - timedelta(days=calendar_days_back)
            try:
                return self._stage_provider_universe_cache(
                    historical=historical,
                    source_date=source_date,
                    admission_date=target_date,
                    definition_end=definition_end,
                    statistics_end=statistics_end,
                    is_fallback=True,
                )
            except Exception as exc:
                failures.append(f"{source_date.isoformat()}={type(exc).__name__}: {exc}")
        raise RuntimeError(
            f"No provider-complete prior-session Databento universe could be staged for "
            f"{target_date} within {lookback_days} calendar days ({'; '.join(failures)})"
        )

    def _active_universe_matches_trading_date(self, trading_date: date) -> bool:
        """Return whether the bounded in-memory universe is safe to reuse.

        Transport reconnects do not change the contract plan. Re-running
        historical definition/OI discovery during every reconnect delays live
        resubscription and can create a second opening-sized burst. A deliberate
        universe-refresh reconnect still passes ``force_refresh=True`` below,
        and a trading-date change always invalidates this reuse path.
        """
        provenance = self.subscription_metadata.get("universe_provenance") or {}
        return bool(
            self.live_symbols
            and not self.universe.empty
            and provenance.get("trading_date") == trading_date.isoformat()
        )

    def _universe_trading_date_changed(self) -> bool:
        """Require a new contract plan on the next weekday trading date."""
        provenance = self.subscription_metadata.get("universe_provenance") or {}
        active_date = provenance.get("trading_date")
        if not active_date:
            return False
        trading_date = current_market_date()
        # Weekend subscriptions are never valid same-day plans. Wait until the
        # next weekday instead of forcing repeated Saturday/Sunday rebuilds.
        if trading_date.weekday() >= 5 or is_holiday(trading_date):
            return False
        return str(active_date) != trading_date.isoformat()

    def _build_universe(self, *, force_refresh: bool = False) -> None:
        if not self.api_key:
            raise RuntimeError("DATABENTO_API_KEY is not set")
        trading_date = current_market_date()

        def load_fallback(
            reason: str,
            *,
            provider_definition_end: datetime | None = None,
            provider_statistics_end: datetime | None = None,
        ) -> bool:
            # A forced refresh must query the provider first. If that fails, a
            # previously validated same-day cache is safer than dropping the
            # live plan; record the failure in provenance for auditability.
            if force_refresh and self._load_cached_universe(
                trading_date, ignore_refresh_flag=True
            ):
                provenance = self.subscription_metadata.get("universe_provenance") or {}
                provenance["refresh_failure_reason"] = reason
                self.subscription_metadata["universe_provenance"] = provenance
                logger.warning(
                    "Databento universe refresh failed; retained validated current-day cache: %s",
                    reason,
                )
                return True
            return self._load_prior_cached_universe(
                trading_date=trading_date,
                reason=reason,
                provider_definition_end=provider_definition_end,
                provider_statistics_end=provider_statistics_end,
            )

        if not force_refresh and self._active_universe_matches_trading_date(trading_date):
            active_provenance = (
                self.subscription_metadata.get("universe_provenance") or {}
            )
            # A separate bounded preparer can atomically publish an exact-date
            # cache after startup fell back to a prior-session scaffold.  An
            # ordinary transport reconnect is already rebuilding the provider
            # subscription, so prefer that validated cache here without
            # opening Historical or forcing another reconnect.  If the cache
            # is absent or invalid, retain the known bounded fallback.
            if active_provenance.get("is_fallback") and self._load_cached_universe(
                trading_date
            ):
                logger.info(
                    "Promoted staged current-day Databento universe for %s during transport reconnect: %s symbols",
                    trading_date,
                    len(self.live_symbols),
                )
                return
            logger.info(
                "Reusing bounded in-memory Databento universe for %s after transport reconnect: %s symbols",
                trading_date,
                len(self.live_symbols),
            )
            return
        if not force_refresh and self._load_cached_universe(trading_date):
            return
        # Startup must not block indefinitely on historical metadata when a
        # recent, bounded scaffold already passed the family/expiry/leg gates.
        # Live quote freshness is still mandatory before any calculation can
        # become valid. Later explicit refreshes may attempt current discovery.
        if not self._universe_built_monotonic and self._load_prior_cached_universe(
            trading_date=trading_date,
            reason=(
                "current-day universe cache is unavailable; using the bounded "
                "prior-session scaffold for startup while live quotes validate contracts"
            ),
        ):
            return
        historical = db.Historical(self.api_key)
        start_dt = datetime.combine(trading_date, datetime.min.time(), tzinfo=timezone.utc)
        try:
            definition_end = self._available_end(historical, "definition")
            statistics_end = self._available_end(historical, "statistics")
        except Exception as exc:
            reason = f"Databento availability discovery failed for {trading_date}: {exc}"
            if load_fallback(reason):
                return
            raise RuntimeError(reason) from exc

        if definition_end <= start_dt or statistics_end <= start_dt:
            reason = (
                f"Databento has not published a complete OPRA universe for {trading_date} "
                f"(definition_end={definition_end.isoformat()}, statistics_end={statistics_end.isoformat()})"
            )
            if load_fallback(
                reason,
                provider_definition_end=definition_end,
                provider_statistics_end=statistics_end,
            ):
                return
            raise RuntimeError(reason)

        frames: list[pd.DataFrame] = []
        for symbol in self.symbols:
            config = MARKETS[symbol]
            frame = self._fetch_definition_universe(
                historical,
                config,
                trading_date=trading_date,
                available_end=definition_end,
            )
            candidates = sorted(frame["symbol"].dropna().unique().tolist())

            if frame.empty or not candidates:
                logger.warning("No Databento definition rows for %s", symbol)
                continue

            oi = self._fetch_open_interest(
                historical,
                candidates,
                trading_date=trading_date,
                available_end=statistics_end,
            )
            frame = frame.merge(oi, on="symbol", how="left")
            frame["open_interest"] = pd.to_numeric(frame["open_interest"], errors="coerce").fillna(0.0)
            active = _active_pair_universe(frame)
            if active.empty:
                logger.warning("No Databento OI rows for %s", symbol)
                continue
            frames.append(active)
            logger.info("Databento %s universe: %s active contracts with OI", symbol, len(active))
        if not frames:
            reason = (
                f"No Databento OPRA universe could be built for {trading_date}. "
                "OPRA may be closed, today may be a market holiday, or current definitions/statistics have no active contracts."
            )
            if load_fallback(
                reason,
                provider_definition_end=definition_end,
                provider_statistics_end=statistics_end,
            ):
                return
            raise RuntimeError(reason)
        full_universe = pd.concat(frames, axis=0).drop_duplicates(subset=["symbol"])
        if not self._has_required_primary_pair_coverage(
            full_universe, trading_date=trading_date
        ):
            reason = (
                f"Databento OPRA universe refresh for {trading_date} was partial or "
                "failed required primary call/put coverage for a core market"
            )
            if load_fallback(
                reason,
                provider_definition_end=definition_end,
                provider_statistics_end=statistics_end,
            ):
                return
            raise RuntimeError(reason)
        cache_path = self._save_cached_universe(
            full_universe,
            trading_date=trading_date,
            provider_definition_end=definition_end,
            provider_statistics_end=statistics_end,
        )
        self._apply_subscription_profile(
            full_universe,
            as_of=trading_date,
            provenance={
                "mode": "databento_discovery",
                "trading_date": trading_date.isoformat(),
                "source_date": trading_date.isoformat(),
                "source_path": str(cache_path) if cache_path and cache_path.exists() else None,
                "source_sha256": self._file_sha256(cache_path) if cache_path and cache_path.exists() else None,
                "is_fallback": False,
                "source_rows": int(len(full_universe)),
                "provider_definition_end": definition_end.isoformat(),
                "provider_statistics_end": statistics_end.isoformat(),
            },
        )

    def _universe_refresh_due(self) -> bool:
        if self._watchdog_session_blocked():
            return False
        provenance = self.subscription_metadata.get("universe_provenance") or {}
        refresh_seconds = (
            UNIVERSE_FALLBACK_REFRESH_SECONDS
            if provenance.get("is_fallback")
            else UNIVERSE_REFRESH_SECONDS
        )
        return bool(
            self._universe_built_monotonic
            and time.monotonic() - self._universe_built_monotonic >= refresh_seconds
        )

    def _planned_primary_expiration(self, market: str) -> tuple[date | None, str | None]:
        """Return the auditable primary expiry with market settlement defenses.

        The live subscription deliberately includes later expirations for shadow
        comparison. Those rows must never become the primary signal merely
        because their quotes arrived before, or remained fresher than, 0DTE.
        """
        market_plan = (self.subscription_metadata.get("markets") or {}).get(market, {})
        planned_entries = market_plan.get("selected_expirations") or []
        primary_candidates: list[date] = []
        for entry in planned_entries:
            if not isinstance(entry, dict):
                continue
            if entry.get("role") != "primary" and int(entry.get("stage") or 0) != 0:
                continue
            value = entry.get("expiration")
            try:
                primary_candidates.append(value if isinstance(value, date) else date.fromisoformat(str(value)))
            except (TypeError, ValueError):
                continue

        self._ensure_universe_index()
        if not primary_candidates:
            universe_expirations = [
                record.get("expiration_date")
                for record in self._universe_records_by_market.get(market, ())
                if isinstance(record.get("expiration_date"), date)
            ]
            if universe_expirations:
                primary_candidates.append(min(universe_expirations))

        if not primary_candidates:
            return None, "PRIMARY_EXPIRATION_UNAVAILABLE_IN_UNIVERSE"

        planned_primary = min(primary_candidates)
        trading_date = current_market_date()
        if market in {"SPX", "NDX"} and planned_primary != trading_date:
            return (
                None,
                "PRIMARY_EXPIRATION_NOT_SAME_DAY: "
                f"planned={planned_primary.isoformat()} required={trading_date.isoformat()}",
            )
        if market == "VIX" and not _expiration_is_live_eligible(
            market, planned_primary, trading_date
        ):
            return (
                None,
                f"{VIX_PRIMARY_NOT_FORWARD_REASON}: "
                f"planned={planned_primary.isoformat()} "
                f"required_after={trading_date.isoformat()}",
            )
        return planned_primary, None

    def _calculate_pin(
        self,
        market: str,
        capture_inputs: bool = False,
        capture_invalid_inputs: bool | None = None,
    ) -> dict | None:
        formula_reasons: list[str] = []
        diagnostic_only_reasons: list[str] = []
        diagnostic: dict[str, object] = {
            "calculated_at_utc": datetime.now(timezone.utc).isoformat(),
            "subscription_generation": self.active_generation,
            "market": market,
        }
        rows = []

        def _fail(result_reason: str) -> None:
            formula_reasons.append(result_reason)
            combined_reasons = list(
                dict.fromkeys([*diagnostic_only_reasons, *formula_reasons])
            )
            combined_reason = "; ".join(combined_reasons)
            self.formula_validation_errors[market] = combined_reason
            self._register_formula_event(market, combined_reasons)
            diagnostic["failure_reason"] = combined_reason
            self.last_calculation_diagnostics[market] = dict(diagnostic)

        planned_primary_expiration, primary_plan_error = self._planned_primary_expiration(market)
        if primary_plan_error or planned_primary_expiration is None:
            _fail(primary_plan_error or "PRIMARY_EXPIRATION_UNAVAILABLE_IN_UNIVERSE")
            return None

        self._ensure_universe_index()
        for row in self._universe_records_by_market.get(market, ()):
            quote = self.quotes.get(str(row["symbol"]))
            if not quote or not self._quote_is_current(quote):
                continue
            rows.append({
                "symbol": row["symbol"],
                "strike": float(row["strike"]),
                "option_type": str(row["option_type"]),
                "expiration_date": row["expiration_date"],
                "open_interest": float(row["open_interest"]),
                "bid": float(quote["bid"]),
                "ask": float(quote["ask"]),
                "mid": float(quote["mid"]),
                "quote_age_seconds": max(0.0, time.monotonic() - float(quote.get("received_monotonic") or 0.0)),
                "subscription_generation": int(quote.get("generation") or 0),
                "instrument_id": quote.get("instrument_id"),
                "ts_event_ns": quote.get("ts_event_ns"),
                "ts_recv_ns": quote.get("ts_recv_ns"),
                "ts_index_ns": quote.get("ts_index_ns"),
                "processed_at_ns": quote.get("processed_at_ns"),
                "receive_to_process_lag_seconds": quote.get("receive_to_process_lag_seconds"),
                "provider_timestamp_order_valid": quote.get("provider_timestamp_order_valid"),
                "mapping_version": quote.get("mapping_version"),
                "mapping_ts_event_ns": quote.get("mapping_ts_event_ns"),
                "mapping_start_ts_ns": quote.get("mapping_start_ts_ns"),
                "mapping_end_ts_ns": quote.get("mapping_end_ts_ns"),
            })
        chain = pd.DataFrame(rows)
        if chain.empty:
            _fail("NO_CURRENT_QUOTES")
            return None
        for column in (
            "instrument_id",
            "ts_event_ns",
            "ts_recv_ns",
            "ts_index_ns",
            "processed_at_ns",
            "mapping_ts_event_ns",
            "mapping_start_ts_ns",
            "mapping_end_ts_ns",
        ):
            chain[column] = pd.array([row.get(column) for row in rows], dtype="Int64")
        fresh_chain = chain.copy()
        diagnostic["fresh_chain_rows"] = int(len(fresh_chain))
        quote_age_series = pd.to_numeric(fresh_chain["quote_age_seconds"], errors="coerce").dropna()
        lag_series = pd.to_numeric(fresh_chain["receive_to_process_lag_seconds"], errors="coerce").dropna()
        ts_event_series = pd.to_numeric(fresh_chain["ts_event_ns"], errors="coerce").dropna()
        ts_recv_series = pd.to_numeric(fresh_chain["ts_recv_ns"], errors="coerce").dropna()
        ts_index_series = pd.to_numeric(fresh_chain["ts_index_ns"], errors="coerce").dropna()
        mapping_versions = sorted(
            str(value)
            for value in fresh_chain["mapping_version"].dropna().unique()
            if str(value).strip()
        )
        mapping_version_missing_count = int(fresh_chain["mapping_version"].isna().sum())
        symbol_mapping_version = None
        if mapping_versions and mapping_version_missing_count == 0:
            symbol_mapping_version = hashlib.sha256(
                json.dumps(mapping_versions, separators=(",", ":")).encode("utf-8")
            ).hexdigest()
        quote_age_p50 = float(quote_age_series.quantile(0.50)) if not quote_age_series.empty else None
        quote_age_p95 = float(quote_age_series.quantile(0.95)) if not quote_age_series.empty else None
        quote_age_max = float(quote_age_series.max()) if not quote_age_series.empty else None
        receive_lag_p50 = float(lag_series.quantile(0.50)) if not lag_series.empty else None
        receive_lag_p95 = float(lag_series.quantile(0.95)) if not lag_series.empty else None
        receive_lag_max = float(lag_series.max()) if not lag_series.empty else None
        latest_ts_event_ns = int(ts_event_series.max()) if not ts_event_series.empty else None
        latest_ts_recv_ns = int(ts_recv_series.max()) if not ts_recv_series.empty else None
        observation_index_ns = int(ts_index_series.max()) if not ts_index_series.empty else None
        # Put-call parity must use the planned primary expiration. Mixing
        # expirations creates a false spot, and choosing the earliest *fresh*
        # expiry can silently promote a later shadow expiry when 0DTE is stale.
        spot_expiration = planned_primary_expiration
        spot_chain = chain[chain["expiration_date"] == spot_expiration].copy()
        if spot_chain.empty:
            _fail(f"PRIMARY_EXPIRATION_NO_CURRENT_QUOTES: expiration={spot_expiration.isoformat()}")
            return None
        paired = spot_chain.pivot_table(index="strike", columns="option_type", values="mid", aggfunc="last")
        paired = paired.dropna(subset=["C", "P"]) if "C" in paired.columns and "P" in paired.columns else pd.DataFrame()
        paired_quote_count = len(paired)
        call_quote_count = int((spot_chain["option_type"] == "C").sum())
        put_quote_count = int((spot_chain["option_type"] == "P").sum())
        market_plan = (self.subscription_metadata.get("markets") or {}).get(market, {})
        primary_plan = next(
            (
                item
                for item in market_plan.get("selected_expirations", [])
                if item.get("role") == "primary"
            ),
            {},
        )
        primary_semantics = _expiration_authority_metadata(
            market, spot_expiration, current_market_date()
        )
        selected_primary_pairs = int(primary_plan.get("selected_strike_pairs") or 0)
        diagnostic.update({
            "primary_expiration": spot_expiration.isoformat(),
            "primary_chain_rows": int(len(spot_chain)),
            "paired_quote_count": int(paired_quote_count),
            "call_quote_count": call_quote_count,
            "put_quote_count": put_quote_count,
            "selected_primary_pair_count": selected_primary_pairs,
        })
        if paired_quote_count < MIN_PAIRED_QUOTES:
            _fail(f"PSEUDO_PARITY_TOO_FEW_PAIRED_QUOTES: {paired_quote_count} pairs < {MIN_PAIRED_QUOTES}")
            return None
        spot = infer_spot_from_pairs(spot_chain, years_to_expiration(spot_expiration))
        if spot is None:
            _fail("PSEUDO_PARITY_NO_SPOT")
            return None
        if not math.isfinite(spot) or spot <= 0:
            _fail("SPOT_NON_FINITE_OR_NONPOSITIVE")
            return None
        diagnostic["parity_spot"] = float(spot)

        # Pair coverage must describe the strikes that can contribute to the
        # calculation. The live universe deliberately retains far-tail 0DTE
        # strikes for OI context, but those strikes often do not quote and were
        # inflating the denominator enough to reject otherwise well-covered
        # at-the-money chains. Keep the gate fail-closed while measuring both
        # the numerator and denominator inside the same +/-4% calculation band.
        calculation_lower_strike = spot * MONEYNESS_LOWER
        calculation_upper_strike = spot * MONEYNESS_UPPER
        expected_primary_strikes = self._paired_strikes_by_market_expiration.get(
            (market, spot_expiration), np.empty(0, dtype=np.float64)
        )
        coverage_expected_primary_pairs = int(
            np.searchsorted(expected_primary_strikes, calculation_upper_strike, side="right")
            - np.searchsorted(expected_primary_strikes, calculation_lower_strike, side="left")
        )
        paired_strikes = paired.index.to_numpy(dtype=np.float64, copy=False)
        paired_in_calculation_band = int(
            np.count_nonzero(
                (paired_strikes >= calculation_lower_strike)
                & (paired_strikes <= calculation_upper_strike)
            )
        )
        primary_pair_coverage_ratio = (
            min(1.0, paired_in_calculation_band / coverage_expected_primary_pairs)
            if coverage_expected_primary_pairs > 0
            else None
        )
        diagnostic.update({
            "expected_primary_pair_count": int(coverage_expected_primary_pairs),
            "paired_primary_pair_count": paired_in_calculation_band,
            "primary_pair_coverage_ratio": primary_pair_coverage_ratio,
            "primary_pair_coverage_strike_min": float(calculation_lower_strike),
            "primary_pair_coverage_strike_max": float(calculation_upper_strike),
        })
        if market in {"SPX", "NDX"} and (
            primary_pair_coverage_ratio is None
            or primary_pair_coverage_ratio < MIN_LIVE_PRIMARY_PAIR_COVERAGE_RATIO
        ):
            diagnostic_only_reasons.append(
                "PRIMARY_PAIR_COVERAGE_LOW: "
                f"{primary_pair_coverage_ratio} < {MIN_LIVE_PRIMARY_PAIR_COVERAGE_RATIO}"
            )
        clock_status = str(self._processing_clock_telemetry().get("status") or "unknown")
        diagnostic["processing_clock_status"] = clock_status
        if self.handoff_status == "active" and clock_status != "synchronized":
            _fail(f"PROCESSING_CLOCK_NOT_SYNCHRONIZED: {clock_status}")
            return None
        chain = chain[
            (chain["open_interest"] > 0)
            & chain["strike"].between(calculation_lower_strike, calculation_upper_strike)
        ].copy()
        diagnostic["moneyness_filtered_rows"] = int(len(chain))
        calculation_now = datetime.now(timezone.utc)
        expiration_years = {
            expiration_date: years_to_expiration(expiration_date, now=calculation_now)
            for expiration_date in pd.unique(chain["expiration_date"])
        }
        year_values = chain["expiration_date"].map(expiration_years).to_numpy(dtype=np.float64)
        batch = batch_iv_gamma_gex(
            spot,
            chain["strike"].to_numpy(dtype=np.float64, copy=False),
            year_values,
            chain["mid"].to_numpy(dtype=np.float64, copy=False),
            chain["option_type"].to_numpy(dtype="U1", copy=False),
            chain["open_interest"].to_numpy(dtype=np.float64, copy=False),
        )
        valid_mask = batch["valid_mask"]
        expiration_timestamps = pd.to_datetime(chain["expiration_date"], errors="coerce")
        days_to_expiry = (
            (expiration_timestamps - pd.Timestamp(current_market_date()))
            .dt.days
            .clip(lower=0)
            .fillna(0)
            .to_numpy(dtype=np.float64)
        )
        calc = chain.loc[
            valid_mask,
            ["symbol", "strike", "option_type", "open_interest", "mid", "expiration_date"],
        ].copy()
        calc["symbol"] = calc["symbol"].astype(str)
        calc["strike"] = calc["strike"].astype(float)
        calc["gex"] = batch["gex"][valid_mask]
        calc["option_type"] = calc["option_type"].astype(str)
        calc["open_interest"] = calc["open_interest"].astype(float)
        calc["mid"] = calc["mid"].astype(float)
        calc["iv"] = batch["iv"][valid_mask]
        calc["gamma"] = batch["gamma"][valid_mask]
        calc["days_to_expiry"] = days_to_expiry[valid_mask]
        calc = calc[
            [
                "symbol",
                "strike",
                "gex",
                "option_type",
                "open_interest",
                "mid",
                "iv",
                "gamma",
                "days_to_expiry",
                "expiration_date",
            ]
        ]
        diagnostic["iv_gamma_rows"] = int(len(calc))
        if calc.empty:
            _fail("NO_GAMMA_ROWS_FROM_IV")
            return None
        chain_mid_bid = float(chain["bid"].median())
        chain_mid_ask = float(chain["ask"].median())
        if market not in self.oi_analytics_by_market:
            analytics_frame = self.full_universe if not self.full_universe.empty else self.universe
            self.oi_analytics_by_market.update(
                build_full_oi_analytics(
                    analytics_frame,
                    [market],
                    as_of=current_market_date(),
                    provenance=self.subscription_metadata.get("universe_provenance") or {},
                )
            )
        oi_analytics = self.oi_analytics_by_market.get(market, {})
        oi_metrics_by_expiration = {
            str(profile.get("expiration")): profile
            for profile in (oi_analytics.get("expirations") or [])
            if isinstance(profile, dict) and profile.get("expiration")
        }
        expiration_profiles: list[dict] = []
        for expiration_date, expiration_calc in calc.groupby("expiration_date"):
            profile_gex = expiration_calc.groupby("strike")["gex"].sum().to_dict()
            profile_pin = max(profile_gex, key=lambda strike: abs(profile_gex[strike]))
            profile_zero = zero_gamma_level(profile_gex)
            oi_metric = oi_metrics_by_expiration.get(str(expiration_date), {})
            profile_pain = oi_metric.get("max_pain")
            dte = max((expiration_date - current_market_date()).days, 0)
            bucket = "0DTE" if dte == 0 else "1-3DTE" if dte <= 3 else "4-45DTE" if dte <= 45 else "other"
            expiration_profiles.append({
                "expiration": str(expiration_date),
                "dte": dte,
                "bucket": bucket,
                "pin": float(profile_pin),
                "pin_abs_gex": abs(float(profile_gex[profile_pin])),
                "zero_gamma": profile_zero,
                "max_pain": profile_pain,
                "max_pain_source": "full-oi-universe",
                "max_pain_as_of": oi_analytics.get("as_of"),
                "full_chain_open_interest": oi_metric.get("total_open_interest"),
                "full_chain_contracts": oi_metric.get("contracts"),
                "full_chain_strikes": oi_metric.get("strikes"),
                "net_gex": float(expiration_calc["gex"].sum()),
                "gross_gex": float(expiration_calc["gex"].abs().sum()),
                "contracts": int(len(expiration_calc)),
            })
        expiration_profiles.sort(key=lambda profile: profile["dte"])
        primary_expiration = planned_primary_expiration
        primary_profile = next(
            (
                profile
                for profile in expiration_profiles
                if datetime.strptime(profile["expiration"], "%Y-%m-%d").date() == primary_expiration
            ),
            None,
        )
        if primary_profile is None:
            _fail(f"PRIMARY_EXPIRATION_MISSING_GEX_PROFILE: expiration={primary_expiration.isoformat()}")
            return None
        same_day_calc = calc[calc["expiration_date"] == primary_expiration].copy()
        if same_day_calc.empty:
            _fail("PRIMARY_EXPIRATION_MISSING_CHAIN_ROWS")
            return None
        gex_by_strike = same_day_calc.groupby("strike")["gex"].sum().to_dict()
        if not gex_by_strike:
            _fail("PRIMARY_EXPIRATION_MISSING_GEX")
            return None
        max_gamma_pin = max(gex_by_strike, key=lambda strike: abs(gex_by_strike[strike]))
        pin_abs_gex = abs(float(gex_by_strike[max_gamma_pin]))
        zero_gamma = zero_gamma_level(gex_by_strike)
        pain = primary_profile["max_pain"]
        anchors = [("Max Gamma Pin", max_gamma_pin)]
        if zero_gamma is not None:
            anchors.append(("Zero-Gamma", zero_gamma))
        if pain is not None:
            anchors.append(("Max Pain", pain))
        likely_name, likely = min(anchors, key=lambda item: abs(item[1] - spot))
        profile_weights = expiration_blend_weights(expiration_profiles)
        for profile, blend_weight in zip(expiration_profiles, profile_weights):
            profile["blend_weight"] = round(float(blend_weight), 6)
        multi_target = (
            sum(profile["pin"] * weight for profile, weight in zip(expiration_profiles, profile_weights))
            if any(profile_weights) else likely
        )
        profile_mean = multi_target
        pin_dispersion = (
            sum(weight * (profile["pin"] - profile_mean) ** 2 for profile, weight in zip(expiration_profiles, profile_weights))
            if any(profile_weights) else 0.0
        ) ** 0.5
        selected_target = multi_target if USE_MULTI_EXPIRATION_TARGET else likely
        pos_wall = max((strike for strike, value in gex_by_strike.items() if value > 0), key=lambda strike: gex_by_strike[strike], default=None)
        neg_wall = min((strike for strike, value in gex_by_strike.items() if value < 0), key=lambda strike: gex_by_strike[strike], default=None)
        top = sorted(gex_by_strike.items(), key=lambda item: abs(item[1]), reverse=True)[:5]
        pin_competition = pin_competition_metrics(top)
        call_gex_total = float(same_day_calc.loc[same_day_calc["option_type"] == "C", "gex"].abs().sum())
        put_gex_total = float(same_day_calc.loc[same_day_calc["option_type"] == "P", "gex"].abs().sum())
        gross_gex = float(same_day_calc["gex"].abs().sum())
        net_gex = float(same_day_calc["gex"].sum())
        invariant_errors = gex_invariant_errors(call_gex_total, put_gex_total, gross_gex, net_gex)
        if invariant_errors:
            reason = "; ".join(invariant_errors)
            _fail(f"INVARIANT_GEX:{reason}")
            logger.error("Databento GEX validation failed for %s: %s", market, reason)
            return None
        if diagnostic_only_reasons:
            self._register_formula_event(market, diagnostic_only_reasons)
            self.formula_validation_errors[market] = "; ".join(diagnostic_only_reasons)
            diagnostic["failure_reason"] = "; ".join(diagnostic_only_reasons)
        else:
            self._register_formula_event(market, [])
            self.formula_validation_errors.pop(market, None)
            diagnostic["failure_reason"] = None
        self.last_calculation_diagnostics[market] = dict(diagnostic)
        min_days = float(calc["days_to_expiry"].min())
        max_days = float(calc["days_to_expiry"].max())
        top_strikes = []
        for strike, value in top:
            strike_rows = same_day_calc[same_day_calc["strike"] == strike].copy()
            call_gex = float(strike_rows.loc[strike_rows["option_type"] == "C", "gex"].abs().sum())
            put_gex = float(strike_rows.loc[strike_rows["option_type"] == "P", "gex"].abs().sum())
            net_strike_gex = float(strike_rows["gex"].sum())
            weights = strike_rows["gex"].abs()
            if weights.sum() > 0:
                days_to_expiry = float((strike_rows["days_to_expiry"] * weights).sum() / weights.sum())
            else:
                days_to_expiry = float(strike_rows["days_to_expiry"].min())
            expirations = sorted(str(expiration) for expiration in strike_rows["expiration_date"].dropna().unique())
            top_strikes.append({
                "strike": strike,
                "gex": net_strike_gex,
                "call_gex": call_gex,
                "put_gex": put_gex,
                "net_gex": net_strike_gex,
                "abs_gex": abs(net_strike_gex),
                "total_gex": call_gex + put_gex,
                "days_to_expiry": days_to_expiry,
                "expiration_count": len(expirations),
                "expirations": expirations[:4],
            })
        strike_count = len(gex_by_strike)
        nonzero_strike_count = sum(abs(float(value)) > 1e-10 for value in gex_by_strike.values())
        top_strike_share = abs(float(top[0][1])) / gross_gex if top and gross_gex > 0 else 0.0
        inference_features = gamma_structure_inference_features(
            spot=spot,
            zero_gamma=zero_gamma,
            positive_gex_wall=pos_wall,
            negative_gex_wall=neg_wall,
            top_strike_concentration=top_strike_share,
        )
        gamma_by_distance = gamma_distance_profile(gex_by_strike, spot)
        dispersion_ratio = (
            round(gamma_by_distance["FAR"] / gamma_by_distance["ATM"], 3)
            if gamma_by_distance["ATM"] >= 0.001 else gamma_by_distance["FAR"]
        )
        avg_iv = float(same_day_calc["iv"].mean())
        vol_regime = "LOW" if avg_iv < 0.15 else "MEDIUM" if avg_iv < 0.25 else "HIGH"
        call_iv_series = same_day_calc.loc[same_day_calc["option_type"] == "C", "iv"]
        put_iv_series = same_day_calc.loc[same_day_calc["option_type"] == "P", "iv"]
        call_iv_mean = float(call_iv_series.mean()) if not call_iv_series.empty else None
        put_iv_mean = float(put_iv_series.mean()) if not put_iv_series.empty else None
        primary_universe_records = [
            record
            for record in self._universe_records_by_market.get(market, ())
            if record.get("expiration_date") == primary_expiration
        ]
        contracts_available = int(len(primary_universe_records))
        available_strikes = sorted(
            {float(record["strike"]) for record in primary_universe_records}
        )
        used_strikes = sorted(float(value) for value in gex_by_strike)
        truncation_pct = (
            round(max(0.0, 1.0 - len(same_day_calc) / contracts_available) * 100.0, 1)
            if contracts_available else 0.0
        )
        truncation = {
            "contracts_used": int(len(same_day_calc)),
            "contracts_available": contracts_available,
            "excluded_above": round(max(available_strikes) - max(used_strikes), 2) if available_strikes and used_strikes and max(available_strikes) > max(used_strikes) else 0.0,
            "excluded_below": round(min(used_strikes) - min(available_strikes), 2) if available_strikes and used_strikes and min(used_strikes) > min(available_strikes) else 0.0,
            "truncation_pct": truncation_pct,
        }
        validation_failure_reasons = list(diagnostic_only_reasons)
        if strike_count < MIN_PRIMARY_STRIKES:
            validation_failure_reasons.append(f"CHAIN_TOO_THIN: Only {strike_count} primary-expiration strikes (min: {MIN_PRIMARY_STRIKES})")
        elif nonzero_strike_count < MIN_NONZERO_STRIKES:
            validation_failure_reasons.append(f"TOO_FEW_ACTIVE_STRIKES: Only {nonzero_strike_count} primary-expiration strikes with non-zero GEX (min: {MIN_NONZERO_STRIKES})")
        if top_strike_share > MAX_STRIKE_CONCENTRATION:
            validation_failure_reasons.append(f"EXTREME_CONCENTRATION: Top strike has {top_strike_share * 100.0:.1f}% of gross GEX")
        confidence = 1.0
        confidence_factors = []
        if len(same_day_calc) < 400:
            confidence -= 0.2
            confidence_factors.append(f"Low primary-expiration contract count ({len(same_day_calc)} < 400)")
        if validation_failure_reasons:
            confidence -= 0.4
            confidence_factors.extend(f"GATE_FAILED: {reason}" for reason in validation_failure_reasons)
        if vol_regime == "HIGH":
            confidence -= 0.1
            confidence_factors.append("High volatility regime")
        if truncation_pct > 20:
            confidence -= 0.15
            confidence_factors.append(f"High primary-expiration input exclusion ({truncation_pct:.1f}% not used)")
        if pin_competition["pin_is_contested"]:
            confidence -= 0.1
            confidence_factors.append(str(pin_competition["pin_competition_reason"]))
        if not confidence_factors:
            confidence_factors.append("No confidence degradation factors")
        market_subscription_plan = (self.subscription_metadata.get("markets") or {}).get(market, {})
        expected_quote_count = int(market_subscription_plan.get("selected_contract_count") or 0)
        universe_sha256 = (self.subscription_metadata.get("universe_provenance") or {}).get("source_sha256")
        instrument_definition_version = (
            f"universe-sha256:{universe_sha256}" if universe_sha256 else None
        )
        result = {
            "symbol": market,
            "subscription_epoch_id": self.subscription_epoch_id,
            "timestamp": _utcnow_naive(),
            "price": spot,
            "market_bid": chain_mid_bid,
            "market_ask": chain_mid_ask,
            "market_volume": None,
            "market_vwap": None,
            "provider": self.provider_name,
            "predicted_close": selected_target,
            "likely_close": selected_target,
            "likely_anchor": likely_name,
            "gamma_pin": max_gamma_pin,
            "gamma_pin_abs_gex": pin_abs_gex,
            **pin_competition,
            "zero_gamma": zero_gamma,
            "max_pain": pain,
            "max_pain_source": "full-oi-universe",
            "max_pain_formula_version": oi_analytics.get("formula_version"),
            "max_pain_as_of": oi_analytics.get("as_of"),
            "oi_analytics_provenance": {
                key: oi_analytics.get(key)
                for key in (
                    "formula_version",
                    "source",
                    "as_of",
                    "trading_date",
                    "is_fallback",
                    "source_date",
                    "provider_statistics_end",
                    "universe_sha256",
                    "full_contract_count",
                )
            },
            "oi_expiration_profiles": oi_analytics.get("expirations") or [],
            "positive_gex_wall": pos_wall,
            "negative_gex_wall": neg_wall,
            "inference_feature_schema_version": GEX_INFERENCE_FEATURE_SCHEMA_VERSION,
            "inference_features": inference_features,
            **inference_features,
            "net_gex": net_gex,
            "gross_gex": gross_gex,
            "call_gex_total": call_gex_total,
            "put_gex_total": put_gex_total,
            "gex_formula_version": GEX_FORMULA_VERSION,
            "gex_sign_convention": "call_gex - put_gex",
            "target_formula_version": TARGET_FORMULA_VERSION,
            "spot_formula_version": SPOT_FORMULA_VERSION,
            "time_to_expiry_convention": "calendar-days-minimum-one-day",
            "risk_free_rate": _configured_risk_free_rate(),
            "contract_multiplier": CONTRACT_MULTIPLIER,
            "expirations_min_days": min_days,
            "expirations_max_days": max_days,
            "expiration_scope": sorted(str(expiration) for expiration in calc["expiration_date"].dropna().unique()),
            "expiration_profiles": expiration_profiles,
            "primary_expiration": str(primary_expiration),
            "same_day_profile_available": bool(primary_expiration == current_market_date()),
            "same_day_target": likely if primary_expiration == current_market_date() else None,
            "primary_expiration_target": likely,
            "primary_expiration_authority": primary_semantics["authority"],
            "primary_expiration_context_only": primary_semantics["context_only"],
            "same_day_authority": primary_semantics["same_day_authority"],
            "primary_expiration_selection_basis": primary_semantics[
                "selection_basis"
            ],
            "multi_expiration_target": multi_target,
            "multi_expiration_enabled": USE_MULTI_EXPIRATION_TARGET,
            "selected_target_mode": (
                (
                    "multi_expiration_forward_context"
                    if USE_MULTI_EXPIRATION_TARGET
                    else "primary_expiration_forward_context"
                )
                if market == "VIX"
                else (
                    "multi_expiration"
                    if USE_MULTI_EXPIRATION_TARGET
                    else "primary_expiration"
                )
            ),
            "pin_dispersion": pin_dispersion,
            "contracts": int(len(same_day_calc)),
            "blend_contracts_count": int(len(calc)),
            "contracts_available": contracts_available,
            "strike_count": strike_count,
            "nonzero_strike_count": nonzero_strike_count,
            "top_strike_share": top_strike_share,
            "pregate_reason": validation_failure_reasons[0] if validation_failure_reasons else None,
            "gamma_by_distance": gamma_by_distance,
            "dispersion_ratio": dispersion_ratio,
            "vol_regime": vol_regime,
            "vol_regime_iv": avg_iv,
            "skew_metrics": {
                "call_iv_mean": call_iv_mean,
                "put_iv_mean": put_iv_mean,
                "skew": put_iv_mean - call_iv_mean if put_iv_mean is not None and call_iv_mean is not None else None,
            },
            "truncation": truncation,
            "confidence": round(max(confidence, 0.0), 2),
            "confidence_factors": confidence_factors,
            "validation_is_valid": not validation_failure_reasons,
            "validation_failure_reasons": validation_failure_reasons,
            "gamma_excluded_from_model": bool(validation_failure_reasons),
            "quotes_cached": int(len(self.quotes)),
            "fresh_quote_count": int(len(fresh_chain)),
            "subscription_generation": self.active_generation,
            "subscription_profile": self.subscription_profile,
            "subscription_contract_count": int(len(self.live_symbols)),
            "subscription_expirations": (
                (self.subscription_metadata.get("markets") or {}).get(market, {}).get("selected_expirations", [])
            ),
            "selected_universe_sha256": self.subscription_metadata.get("selected_universe_sha256"),
            "universe_sha256": universe_sha256,
            "universe_provenance": self.subscription_metadata.get("universe_provenance") or {},
            "instrument_definition_version": instrument_definition_version,
            "symbol_mapping_version": symbol_mapping_version,
            "mapping_version_count": len(mapping_versions),
            "mapping_version_missing_count": mapping_version_missing_count,
            # Crossed records are rejected before entering ``self.quotes``;
            # therefore none can contribute to this calculation.  Keep the
            # stream-wide rejection counter separately below.
            "contributing_crossed_quote_count": 0,
            "expected_quote_count": expected_quote_count,
            "quote_age_seconds": quote_age_p95,
            "quote_age_p50_seconds": quote_age_p50,
            "quote_age_p95_seconds": quote_age_p95,
            "quote_age_max_seconds": quote_age_max,
            "receive_to_process_lag_p50_seconds": receive_lag_p50,
            "receive_to_process_lag_p95_seconds": receive_lag_p95,
            "receive_to_process_lag_max_seconds": receive_lag_max,
            "latest_ts_event_ns": latest_ts_event_ns,
            "latest_ts_recv_ns": latest_ts_recv_ns,
            "observation_index_ns": observation_index_ns,
            "latest_ts_event_utc": _timestamp_ns_to_utc_iso(latest_ts_event_ns),
            "latest_ts_recv_utc": _timestamp_ns_to_utc_iso(latest_ts_recv_ns),
            "observation_index_utc": _timestamp_ns_to_utc_iso(observation_index_ns),
            "quote_records_seen": self.quote_records_seen,
            "invalid_quote_records": self.invalid_quote_records,
            "crossed_quote_records": self.crossed_quote_records,
            "unmapped_quote_records": self.unmapped_quote_records,
            "provider_timestamp_missing_records": self.provider_timestamp_missing_records,
            "provider_timestamp_order_errors": self.provider_timestamp_order_errors,
            "negative_receive_lag_records": self.negative_receive_lag_records,
            "material_negative_receive_lag_records": self.material_negative_receive_lag_records,
            "processing_clock_telemetry": self._processing_clock_telemetry(),
            "paired_quote_count": paired_quote_count,
            "selected_primary_pair_count": selected_primary_pairs,
            "expected_primary_pair_count": int(coverage_expected_primary_pairs),
            "paired_primary_pair_count": paired_in_calculation_band,
            "primary_pair_coverage_ratio": primary_pair_coverage_ratio,
            "call_quote_count": call_quote_count,
            "put_quote_count": put_quote_count,
            "formula_health_reasons": list(validation_failure_reasons),
            "validation_status": "valid" if not validation_failure_reasons else "invalid",
            **validate_underlying(market, spot),
            "top_strikes": top_strikes,
        }
        should_capture_inputs = bool(
            capture_inputs
            if result["validation_is_valid"]
            else (
                capture_inputs
                if capture_invalid_inputs is None
                else capture_invalid_inputs
            )
        )
        if should_capture_inputs:
            calculation_id = str(uuid.uuid4())
            provenance = self.subscription_metadata.get("universe_provenance") or {}
            source_path = provenance.get("source_path")
            cache_path = Path(str(source_path)) if source_path else self._cache_path()
            universe_sha256 = provenance.get("source_sha256")
            try:
                if not universe_sha256 and cache_path.exists():
                    universe_sha256 = hashlib.sha256(cache_path.read_bytes()).hexdigest()
            except OSError:
                pass
            result["calculation_id"] = calculation_id
            result["universe_sha256"] = universe_sha256
            result["_calculation_inputs"] = {
                "input_schema_version": "gamma-inputs-v2-point-in-time",
                "calculation_id": calculation_id,
                "symbol": market,
                "subscription_epoch_id": self.subscription_epoch_id,
                "subscription_generation": int(
                    result.get("subscription_generation") or 0
                ),
                "calculated_at_utc": result["timestamp"],
                "raw_fresh_chain_rows": _dataframe_records(fresh_chain),
                "calculated_gex_rows": _dataframe_records(calc),
                "parameters": {
                    "dataset": DATASET,
                    "schema": self.schema,
                    "quote_freshness_seconds": QUOTE_FRESHNESS_SECONDS,
                    "minimum_paired_quotes": MIN_PAIRED_QUOTES,
                    "moneyness_lower": MONEYNESS_LOWER,
                    "moneyness_upper": MONEYNESS_UPPER,
                    "risk_free_rate": _configured_risk_free_rate(),
                    "contract_multiplier": CONTRACT_MULTIPLIER,
                    "gex_formula_version": GEX_FORMULA_VERSION,
                    "spot_formula_version": SPOT_FORMULA_VERSION,
                    "target_formula_version": TARGET_FORMULA_VERSION,
                    "max_pain_formula_version": oi_analytics.get("formula_version"),
                    "max_pain_source": "full-oi-universe",
                    "oi_analytics_provenance": result.get("oi_analytics_provenance"),
                    "time_to_expiry_convention": "calendar-days-minimum-one-day",
                    "multi_expiration_enabled": USE_MULTI_EXPIRATION_TARGET,
                    "expiration_bucket_weights": {"0DTE": 0.7, "1-3DTE": 0.2, "4-45DTE": 0.1},
                    "subscription_profile": self.subscription_profile,
                    "subscription_metadata": self.subscription_metadata,
                },
                "rejection_counts": {
                    "fresh_chain_rows": int(len(fresh_chain)),
                    "primary_expiration_pair_rows": int(len(spot_chain)),
                    "within_moneyness_and_positive_oi_rows": int(len(chain)),
                    "calculated_gex_rows": int(len(calc)),
                    "rejected_before_moneyness_or_oi": int(len(fresh_chain) - len(chain)),
                    "rejected_iv_or_gamma": int(len(chain) - len(calc)),
                },
                "output_summary": {
                    key: value for key, value in result.items() if not key.startswith("_")
                },
            }
        return result

    def _quote_is_current(self, quote: dict) -> bool:
        received = float(quote.get("received_monotonic") or 0.0)
        generation = int(quote.get("generation") or 0)
        if generation != self.active_generation or received < self.subscription_cutoff_monotonic:
            return False
        return time.monotonic() - received <= QUOTE_FRESHNESS_SECONDS

    def _fresh_quote_counts(self) -> dict[str, int]:
        now_monotonic = time.monotonic()
        with self._fresh_quote_lock:
            self._expire_fresh_quotes_locked(now_monotonic)
            return {
                symbol: int(self._fresh_quote_counts_by_market.get(symbol, 0))
                for symbol in self.symbols
            }

    def _snapshot_payload(self, result: dict) -> dict:
        timestamp = result.get("timestamp")
        if isinstance(timestamp, datetime):
            generated_at_utc = timestamp.replace(tzinfo=timezone.utc).isoformat()
        else:
            generated_at_utc = datetime.now(timezone.utc).isoformat()

        gross_gex = float(result.get("gross_gex") or abs(float(result.get("net_gex") or 0)))
        net_gex = float(result.get("net_gex") or 0)
        # Audit-shaped diagnostics and the legacy public payload must describe
        # the same observation.  Prefer an explicitly supplied diagnostic key
        # even when its value is zero or None; falling through on truthiness
        # could resurrect a stale/conflicting public alias on invalid rows.
        spot_value = (
            result.get("spot_last")
            if "spot_last" in result
            else result.get("price")
        )
        spot = float(spot_value or 0)
        gamma_pin = (
            result.get("primary_gamma_pin_strike")
            if "primary_gamma_pin_strike" in result
            else result.get("gamma_pin")
        )
        max_pain = (
            result.get("max_pain_strike")
            if "max_pain_strike" in result
            else result.get("max_pain")
        )
        validation_is_valid = result.get("validation_is_valid") is True
        gamma_excluded_from_model = (
            result.get("gamma_excluded_from_model") is not False
        )
        validation_failure_reasons = list(
            result.get("validation_failure_reasons") or []
        )
        if "validation_is_valid" not in result:
            validation_failure_reasons.append(
                "SOURCE_VALIDATION_PROOF_MISSING"
            )
        elif result.get("validation_is_valid") is not True and not validation_failure_reasons:
            validation_failure_reasons.append("SOURCE_VALIDATION_FAILED")
        if "gamma_excluded_from_model" not in result:
            validation_failure_reasons.append(
                "SOURCE_MODEL_ELIGIBILITY_PROOF_MISSING"
            )
        elif (
            result.get("gamma_excluded_from_model") is not False
            and not validation_failure_reasons
        ):
            validation_failure_reasons.append("GAMMA_EXCLUDED_FROM_MODEL")
        validation_failure_reasons = list(
            dict.fromkeys(validation_failure_reasons)
        )
        usable_for_prediction = bool(
            result.get("usable_for_prediction", True)
            and validation_is_valid
            and not gamma_excluded_from_model
            and spot > 0.0
        )
        top_strikes = result.get("top_strikes") or []

        return {
            "snapshot_version": "databento-live-2.0",
            **validation_method_fields(MIN_PAIRED_QUOTES),
            "generated_at_utc": generated_at_utc,
            "timestamp_utc": generated_at_utc,
            "timestamp": result.get("timestamp") or generated_at_utc,
            "symbol": result.get("symbol"),
            "subscription_epoch_id": result.get("subscription_epoch_id"),
            "calculation_id": result.get("calculation_id"),
            "universe_sha256": result.get("universe_sha256"),
            "selected_universe_sha256": result.get("selected_universe_sha256"),
            "spot_last": spot,
            "price": spot,
            "spot_source": "databento_opra_put_call_parity",
            "chain_symbol_used": "OPRA",
            "underlying_reported": result.get("symbol"),
            "is_etf_proxy": False,
            "primary_gamma_pin_strike": gamma_pin,
            "gamma_pin_strike": gamma_pin,
            "gamma_pin": gamma_pin,
            "primary_gamma_pin_abs_gex": float(result.get("gamma_pin_abs_gex") or 0.0),
            "pin_runner_up_strike": result.get("pin_runner_up_strike"),
            "pin_runner_up_abs_gex": result.get("pin_runner_up_abs_gex"),
            "pin_lead_abs_gex": result.get("pin_lead_abs_gex"),
            "pin_lead_ratio": result.get("pin_lead_ratio"),
            "pin_competition_threshold": result.get("pin_competition_threshold"),
            "pin_is_contested": bool(result.get("pin_is_contested")),
            "pin_competition_reason": result.get("pin_competition_reason"),
            "pin_competition_formula_version": result.get("pin_competition_formula_version"),
            "zero_gamma_level": result.get("zero_gamma"),
            "zero_gamma": result.get("zero_gamma"),
            "zero_gamma_method": "linear_interpolation",
            "max_pain_strike": max_pain,
            "max_pain": max_pain,
            "max_pain_source": result.get("max_pain_source"),
            "max_pain_formula_version": result.get("max_pain_formula_version"),
            "max_pain_as_of": result.get("max_pain_as_of"),
            "oi_analytics_provenance": result.get("oi_analytics_provenance") or {},
            "oi_expiration_profiles": result.get("oi_expiration_profiles") or [],
            "likely_close": result.get("likely_close"),
            "likely_anchor": result.get("likely_anchor"),
            "call_gex_total": float(result.get("call_gex_total") or 0),
            "put_gex_total": float(result.get("put_gex_total") or 0),
            "gex_formula_version": result.get("gex_formula_version") or GEX_FORMULA_VERSION,
            "gex_sign_convention": result.get("gex_sign_convention") or "call_gex - put_gex",
            "risk_free_rate": float(result.get("risk_free_rate") or _configured_risk_free_rate()),
            "contract_multiplier": float(result.get("contract_multiplier") or CONTRACT_MULTIPLIER),
            "target_formula_version": result.get("target_formula_version") or TARGET_FORMULA_VERSION,
            "spot_formula_version": result.get("spot_formula_version") or SPOT_FORMULA_VERSION,
            "time_to_expiry_convention": result.get("time_to_expiry_convention") or "calendar-days-minimum-one-day",
            "gross_gex": gross_gex,
            "net_gex": net_gex,
            "positive_gex_wall": result.get("positive_gex_wall"),
            "negative_gex_wall": result.get("negative_gex_wall"),
            "total_gex_abs": gross_gex,
            "total_gex_net": net_gex,
            "market_bid": result.get("market_bid"),
            "market_ask": result.get("market_ask"),
            "market_vwap": result.get("market_vwap"),
            "contracts_count": int(result.get("contracts") or 0),
            "expirations_min_days": int(float(result.get("expirations_min_days") or 0)),
            "expirations_max_days": int(float(result.get("expirations_max_days") or 0)),
            "expiration_scope": result.get("expiration_scope") or [],
            "expiration_profiles": result.get("expiration_profiles") or [],
            "primary_expiration": result.get("primary_expiration"),
            "same_day_profile_available": bool(result.get("same_day_profile_available")),
            "same_day_target": result.get("same_day_target"),
            "primary_expiration_target": result.get("primary_expiration_target"),
            "primary_expiration_authority": result.get(
                "primary_expiration_authority"
            ),
            "primary_expiration_context_only": bool(
                result.get("primary_expiration_context_only")
            ),
            "same_day_authority": bool(result.get("same_day_authority")),
            "primary_expiration_selection_basis": result.get(
                "primary_expiration_selection_basis"
            ),
            "multi_expiration_target": result.get("multi_expiration_target"),
            "multi_expiration_enabled": bool(result.get("multi_expiration_enabled")),
            "selected_target_mode": result.get("selected_target_mode") or "primary_expiration",
            "pin_dispersion": result.get("pin_dispersion"),
            "blend_contracts_count": int(result.get("blend_contracts_count") or 0),
            "contracts_available": int(result.get("contracts_available") or 0),
            "strike_count": int(result.get("strike_count") or 0),
            "nonzero_strike_count": int(result.get("nonzero_strike_count") or 0),
            "top_strike_share": float(result.get("top_strike_share") or 0.0),
            "inference_feature_schema_version": result.get(
                "inference_feature_schema_version"
            ) or GEX_INFERENCE_FEATURE_SCHEMA_VERSION,
            "inference_features": result.get("inference_features") or {},
            "zero_gamma_distance": result.get("zero_gamma_distance"),
            "wall_asymmetry": result.get("wall_asymmetry"),
            "top_strike_concentration": result.get("top_strike_concentration"),
            "pregate_reason": result.get("pregate_reason"),
            "gamma_by_distance": result.get("gamma_by_distance"),
            "dispersion_ratio": result.get("dispersion_ratio"),
            "vol_regime": result.get("vol_regime"),
            "vol_regime_iv": result.get("vol_regime_iv"),
            "skew_metrics": result.get("skew_metrics"),
            "truncation": result.get("truncation"),
            "validation_is_valid": validation_is_valid,
            "validation_failure_reasons": validation_failure_reasons,
            "gamma_excluded_from_model": gamma_excluded_from_model,
            "usable_for_prediction": usable_for_prediction,
            "confidence": result.get("confidence"),
            "confidence_factors": result.get("confidence_factors") or [],
            "top_strikes_by_abs_gex": top_strikes,
            "quotes_cached": int(result.get("quotes_cached") or 0),
            "fresh_quote_count": int(result.get("fresh_quote_count") or 0),
            "subscription_generation": int(result.get("subscription_generation") or 0),
            "subscription_profile": result.get("subscription_profile") or self.subscription_profile,
            "subscription_contract_count": int(result.get("subscription_contract_count") or 0),
            "subscription_expirations": result.get("subscription_expirations") or [],
            "selected_universe_sha256": result.get("selected_universe_sha256"),
            "universe_sha256": result.get("universe_sha256"),
            "universe_provenance": result.get("universe_provenance") or {},
            "instrument_definition_version": result.get("instrument_definition_version"),
            "symbol_mapping_version": result.get("symbol_mapping_version"),
            "mapping_version_count": int(result.get("mapping_version_count") or 0),
            "mapping_version_missing_count": int(result.get("mapping_version_missing_count") or 0),
            "contributing_crossed_quote_count": int(result.get("contributing_crossed_quote_count") or 0),
            "expected_quote_count": int(result.get("expected_quote_count") or 0),
            "quote_age_seconds": float(result.get("quote_age_seconds") or 0.0),
            "quote_age_p50_seconds": result.get("quote_age_p50_seconds"),
            "quote_age_p95_seconds": result.get("quote_age_p95_seconds"),
            "quote_age_max_seconds": result.get("quote_age_max_seconds"),
            "receive_to_process_lag_p50_seconds": result.get("receive_to_process_lag_p50_seconds"),
            "receive_to_process_lag_p95_seconds": result.get("receive_to_process_lag_p95_seconds"),
            "receive_to_process_lag_max_seconds": result.get("receive_to_process_lag_max_seconds"),
            "latest_ts_event_ns": result.get("latest_ts_event_ns"),
            "latest_ts_recv_ns": result.get("latest_ts_recv_ns"),
            "observation_index_ns": result.get("observation_index_ns"),
            "latest_ts_event_utc": result.get("latest_ts_event_utc"),
            "latest_ts_recv_utc": result.get("latest_ts_recv_utc"),
            "observation_index_utc": result.get("observation_index_utc"),
            "quote_records_seen": int(result.get("quote_records_seen") or 0),
            "invalid_quote_records": int(result.get("invalid_quote_records") or 0),
            "crossed_quote_records": int(result.get("crossed_quote_records") or 0),
            "unmapped_quote_records": int(result.get("unmapped_quote_records") or 0),
            "provider_timestamp_missing_records": int(result.get("provider_timestamp_missing_records") or 0),
            "provider_timestamp_order_errors": int(result.get("provider_timestamp_order_errors") or 0),
            "negative_receive_lag_records": int(result.get("negative_receive_lag_records") or 0),
            "material_negative_receive_lag_records": int(
                result.get("material_negative_receive_lag_records") or 0
            ),
            "processing_clock_telemetry": result.get("processing_clock_telemetry") or {},
            "paired_quote_count": int(result.get("paired_quote_count") or 0),
            "selected_primary_pair_count": int(
                result.get("selected_primary_pair_count") or 0
            ),
            "expected_primary_pair_count": int(result.get("expected_primary_pair_count") or 0),
            "paired_primary_pair_count": int(result.get("paired_primary_pair_count") or 0),
            "primary_pair_coverage_ratio": result.get("primary_pair_coverage_ratio"),
            "call_quote_count": int(result.get("call_quote_count") or 0),
            "put_quote_count": int(result.get("put_quote_count") or 0),
            "validation_status": (
                "valid"
                if validation_is_valid and not gamma_excluded_from_model
                else "invalid"
            ),
            "underlying_validation_status": result.get("underlying_validation_status", "unavailable"),
            "underlying_proxy_symbol": result.get("underlying_proxy_symbol"),
            "underlying_price": result.get("underlying_price"),
            "underlying_timestamp_utc": result.get("underlying_timestamp_utc"),
            "underlying_age_seconds": result.get("underlying_age_seconds"),
            "underlying_divergence_points": result.get("underlying_divergence_points"),
            "underlying_divergence_pct": result.get("underlying_divergence_pct"),
            "underlying_validation_reason": result.get("underlying_validation_reason"),
            "provider": self.provider_name,
        }

    def _write_snapshot(self, result: dict) -> bool:
        """Write retained evidence; acknowledge only a committed calculation input set."""
        symbol = str(result.get("symbol") or "").upper()
        if not symbol:
            return False

        payload = self._snapshot_payload(result)
        is_usable = (
            payload.get("validation_is_valid") is True
            and payload.get("gamma_excluded_from_model") is False
        )
        write_times = (
            self._last_snapshot_write
            if is_usable
            else self._last_invalid_snapshot_write
        )
        now = time.monotonic()
        last_write = write_times.get(symbol, 0.0)
        if now - last_write < self.snapshot_interval:
            return False
        # A valid capture is not due-complete until its calculation run and
        # full input blob commit atomically.  Advancing the success throttle
        # before that commit would turn every calculation for the next minute
        # into a non-capture cycle after one transient persistence failure.
        # Invalid diagnostics and legacy/no-lineage direct writes retain their
        # ordinary bounded cadence; a lineage-bearing valid capture advances
        # only after the commit below succeeds.
        if not is_usable or not result.get("calculation_id"):
            write_times[symbol] = now

        date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")

        calculation_persisted = False
        calculation_id = str(result.get("calculation_id") or "").strip()
        calculation_inputs = result.get("_calculation_inputs")
        calculation_persistence_attempted = bool(
            calculation_id and isinstance(calculation_inputs, dict)
        )
        if calculation_persistence_attempted:
            try:
                from backend.database import save_gamma_calculation_inputs

                rejection_counts = calculation_inputs.get("rejection_counts") or {}
                persistence_result = save_gamma_calculation_inputs(
                    {
                        "calculation_id": result.get("calculation_id"),
                        "symbol": symbol,
                        "calculated_at_utc": payload.get("timestamp_utc"),
                        "provider": self.provider_name,
                        "subscription_epoch_id": payload.get(
                            "subscription_epoch_id"
                        ),
                        "subscription_generation": payload.get("subscription_generation"),
                        "status": "valid" if payload.get("validation_is_valid") else "invalid",
                        "formula_version": payload.get("gex_formula_version"),
                        "target_formula_version": payload.get("target_formula_version"),
                        "spot_formula_version": payload.get("spot_formula_version"),
                        "universe_sha256": result.get("universe_sha256"),
                        "risk_free_rate": payload.get("risk_free_rate"),
                        "contract_multiplier": payload.get("contract_multiplier"),
                        "spot_price": payload.get("spot_last"),
                        "gamma_pin": payload.get("primary_gamma_pin_strike"),
                        "selected_target": payload.get("likely_close"),
                        "primary_expiration_target": payload.get("primary_expiration_target"),
                        "multi_expiration_target": payload.get("multi_expiration_target"),
                        "zero_gamma": payload.get("zero_gamma_level"),
                        "max_pain": payload.get("max_pain_strike"),
                        "call_gex": payload.get("call_gex_total"),
                        "put_gex": payload.get("put_gex_total"),
                        "gross_gex": payload.get("gross_gex"),
                        "net_gex": payload.get("net_gex"),
                        "chain_row_count": rejection_counts.get("fresh_chain_rows", 0),
                        "gex_row_count": rejection_counts.get("calculated_gex_rows", 0),
                        "rejection_counts": rejection_counts,
                    },
                    calculation_inputs,
                )
                calculation_persisted = bool(
                    isinstance(persistence_result, dict)
                    and persistence_result.get("calculation_id") == result.get("calculation_id")
                    and persistence_result.get("payload_sha256")
                )
                if not calculation_persisted:
                    logger.warning("Gamma input capture was not persisted for %s calculation %s", symbol, result.get("calculation_id"))
            except Exception as exc:
                logger.warning("Failed to persist full Databento calculation inputs for %s: %s", symbol, exc)

        if is_usable and calculation_id and calculation_persisted:
            self._last_snapshot_write[symbol] = now

        if calculation_id and not calculation_persisted:
            # Every sink must agree that an uncommitted capture is diagnostic.
            # Retain its observations without claiming persisted lineage.
            payload.pop("calculation_id", None)
            payload["validation_is_valid"] = False
            payload["validation_status"] = "invalid"
            payload["gamma_excluded_from_model"] = True
            payload["usable_for_prediction"] = False
            if calculation_persistence_attempted:
                payload["validation_failure_reasons"] = list(dict.fromkeys([
                    *list(payload.get("validation_failure_reasons") or []),
                    "CALCULATION_INPUT_PERSISTENCE_FAILED",
                ]))
            is_usable = False

        payload = finalize_snapshot_export_payload(
            payload,
            source=result,
            calculation_persisted=calculation_persisted,
            subscription_context=self.get_subscription_context(),
        )

        if is_usable:
            try:
                export_dir = self.exports_dir / symbol
                export_dir.mkdir(parents=True, exist_ok=True)
                export_file = export_dir / f"{date_str}.ndjson"
                with open(export_file, "a", encoding="utf-8") as handle:
                    handle.write(json.dumps(payload, separators=(",", ":"), default=str) + "\n")
            except Exception as exc:
                logger.warning("Failed to write Databento NDJSON snapshot for %s: %s", symbol, exc)

        try:
            audit_dir = self.audit_dir / symbol
            audit_dir.mkdir(parents=True, exist_ok=True)
            audit_file = audit_dir / (datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S") + ".json")
            with open(audit_file, "w", encoding="utf-8") as handle:
                json.dump(payload, handle, indent=2, default=str)
        except Exception as exc:
            logger.warning("Failed to write Databento audit snapshot for %s: %s", symbol, exc)

        if is_usable:
            try:
                from backend.database import save_market_snapshot

                save_market_snapshot(
                    symbol,
                    payload.get("timestamp_utc"),
                    result.get("price"),
                    bid=result.get("market_bid"),
                    ask=result.get("market_ask"),
                    volume=result.get("market_volume"),
                    vwap=result.get("market_vwap"),
                )
            except Exception as exc:
                logger.debug(
                    "Failed to persist Databento market snapshot for %s: %s",
                    symbol,
                    exc,
                )

        try:
            from database import save_audit_snapshot_to_db, save_gamma_snapshot

            save_audit_snapshot_to_db(payload)
            if (
                payload.get("validation_is_valid") is True
                and payload.get("gamma_excluded_from_model") is False
            ):
                save_gamma_snapshot(
                    ticker=symbol,
                    interval_timestamp=datetime.now(timezone.utc),
                    pin_strike=float(payload.get("primary_gamma_pin_strike") or 0.0),
                    pull_strength=abs(float(payload.get("net_gex") or 0.0)),
                    spot_price=float(payload.get("spot_last") or 0.0),
                    total_gex=float(payload.get("gross_gex") or 0.0),
                    net_gex=float(payload.get("net_gex") or 0.0),
                    is_mock_data=False,
                )
        except Exception as exc:
            logger.warning("Failed to write Databento snapshot to DB for %s: %s", symbol, exc)
        return calculation_persisted

    def _write_invalid_snapshot(self, symbol: str, reason: str) -> None:
        symbol = symbol.upper()
        now = time.monotonic()
        last_write = self._last_invalid_snapshot_write.get(symbol, 0.0)
        if now - last_write < self.snapshot_interval:
            return
        self._last_invalid_snapshot_write[symbol] = now

        generated_at_utc = datetime.now(timezone.utc).isoformat()
        self._ensure_universe_index()
        symbol_universe_records = self._universe_records_by_market.get(symbol, ())
        market_plan = (self.subscription_metadata.get("markets") or {}).get(symbol, {})
        subscription_expirations = market_plan.get("selected_expirations") or []
        primary_expiration = next(
            (
                str(entry.get("expiration"))
                for entry in subscription_expirations
                if isinstance(entry, dict)
                and (entry.get("role") == "primary" or int(entry.get("stage") or 0) == 0)
                and entry.get("expiration")
            ),
            None,
        )
        try:
            primary_expiration_date = (
                date.fromisoformat(primary_expiration)
                if primary_expiration is not None
                else None
            )
        except ValueError:
            primary_expiration_date = None
        primary_semantics = _expiration_authority_metadata(
            symbol, primary_expiration_date, current_market_date()
        )
        provenance = self.subscription_metadata.get("universe_provenance") or {}
        oi_analytics = self.oi_analytics_by_market.get(symbol, {})
        calculation_diagnostic = self.last_calculation_diagnostics.get(symbol, {})
        primary_oi_profile = next(
            (
                profile
                for profile in (oi_analytics.get("expirations") or [])
                if isinstance(profile, dict)
                and str(profile.get("expiration")) == str(primary_expiration)
            ),
            {},
        )
        payload = {
            "snapshot_version": "databento-live-2.0",
            **validation_method_fields(MIN_PAIRED_QUOTES),
            "generated_at_utc": generated_at_utc,
            "timestamp_utc": generated_at_utc,
            "symbol": symbol,
            "subscription_epoch_id": self.subscription_epoch_id,
            "timestamp": _utcnow_naive(),
            "provider": self.provider_name,
            "spot_last": 0.0,
            "price": 0.0,
            "spot_source": "databento_opra_put_call_parity",
            "chain_symbol_used": "OPRA",
            "underlying_reported": symbol,
            "is_etf_proxy": False,
            "primary_gamma_pin_strike": 0.0,
            "gamma_pin_strike": 0.0,
            "gamma_pin": None,
            "primary_gamma_pin_abs_gex": 0.0,
            "pin_runner_up_strike": None,
            "pin_runner_up_abs_gex": None,
            "pin_lead_abs_gex": None,
            "pin_lead_ratio": None,
            "pin_competition_threshold": PIN_CONTESTED_LEAD_RATIO,
            "pin_is_contested": False,
            "pin_competition_reason": None,
            "pin_competition_formula_version": "pin-competition-v1-top-two-abs-net-gex",
            "zero_gamma_level": None,
            "zero_gamma": None,
            "zero_gamma_method": None,
            "max_pain_strike": primary_oi_profile.get("max_pain"),
            "max_pain": primary_oi_profile.get("max_pain"),
            "max_pain_source": "full-oi-universe" if primary_oi_profile else None,
            "max_pain_formula_version": oi_analytics.get("formula_version"),
            "max_pain_as_of": oi_analytics.get("as_of"),
            "oi_analytics_provenance": {
                key: oi_analytics.get(key)
                for key in (
                    "formula_version", "source", "as_of", "trading_date", "is_fallback",
                    "source_date", "provider_statistics_end", "universe_sha256", "full_contract_count",
                )
            },
            "oi_expiration_profiles": oi_analytics.get("expirations") or [],
            "likely_close": None,
            "predicted_close": None,
            "likely_anchor": None,
            "call_gex_total": 0.0,
            "put_gex_total": 0.0,
            "gex_formula_version": GEX_FORMULA_VERSION,
            "gex_sign_convention": "call_gex - put_gex",
            "gross_gex": 0.0,
            "net_gex": 0.0,
            "total_gex_abs": 0.0,
            "total_gex_net": 0.0,
            "contracts_count": 0,
            "contracts_available": int(len(symbol_universe_records)),
            "expirations_min_days": 0,
            "expirations_max_days": 0,
            "expiration_scope": [
                str(entry.get("expiration"))
                for entry in subscription_expirations
                if isinstance(entry, dict) and entry.get("expiration")
            ],
            "expiration_profiles": [],
            "primary_expiration": primary_expiration,
            "same_day_profile_available": False,
            "same_day_target": None,
            "primary_expiration_target": None,
            "primary_expiration_authority": primary_semantics["authority"],
            "primary_expiration_context_only": primary_semantics["context_only"],
            "same_day_authority": primary_semantics["same_day_authority"],
            "primary_expiration_selection_basis": primary_semantics[
                "selection_basis"
            ],
            "multi_expiration_target": None,
            "multi_expiration_enabled": USE_MULTI_EXPIRATION_TARGET,
            "selected_target_mode": (
                "primary_expiration_forward_context"
                if symbol == "VIX"
                else "primary_expiration"
            ),
            "validation_is_valid": False,
            "validation_failure_reasons": [reason],
            "gamma_excluded_from_model": True,
            "confidence": 0.0,
            "top_strikes_by_abs_gex": [],
            "quotes_cached": int(len(self.quotes)),
            "fresh_quote_count": int(self._fresh_quote_counts().get(symbol, 0)),
            "paired_quote_count": calculation_diagnostic.get("paired_quote_count"),
            "selected_primary_pair_count": calculation_diagnostic.get(
                "selected_primary_pair_count"
            ),
            "expected_primary_pair_count": calculation_diagnostic.get(
                "expected_primary_pair_count"
            ),
            "paired_primary_pair_count": calculation_diagnostic.get(
                "paired_primary_pair_count"
            ),
            "primary_pair_coverage_ratio": calculation_diagnostic.get(
                "primary_pair_coverage_ratio"
            ),
            "subscription_generation": self.active_generation,
            "subscription_profile": self.subscription_profile,
            "subscription_contract_count": int(len(self.live_symbols)),
            "subscription_expirations": subscription_expirations,
            "selected_universe_sha256": self.subscription_metadata.get("selected_universe_sha256"),
            "universe_sha256": provenance.get("source_sha256"),
            "universe_provenance": provenance,
            "processing_clock_telemetry": self._processing_clock_telemetry(),
            "pregate_reason": reason,
        }

        self.buffers[symbol].append(payload)
        self.latest_pins.pop(symbol, None)
        self.last_diagnostic_time[symbol] = _utcnow_naive()
        self.latest_invalid[symbol] = payload

        try:
            audit_dir = self.audit_dir / symbol
            audit_dir.mkdir(parents=True, exist_ok=True)
            audit_file = audit_dir / (datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S") + ".json")
            with open(audit_file, "w", encoding="utf-8") as handle:
                json.dump(payload, handle, indent=2, default=str)
        except Exception as exc:
            logger.warning("Failed to write invalid Databento audit snapshot for %s: %s", symbol, exc)

        try:
            from database import save_audit_snapshot_to_db

            # Keep invalid diagnostics, but never let zero/no-data rows overwrite
            # a valid 15-minute gamma_pin_snapshots bucket.
            save_audit_snapshot_to_db(payload)
        except Exception as exc:
            logger.warning("Failed to write invalid Databento audit snapshot to DB for %s: %s", symbol, exc)

    def _mark_stream_progress(self) -> None:
        self._last_progress_monotonic = time.monotonic()

    def _record_fresh_market_and_maybe_activate(
        self, market: str, *, generation: int | None = None,
        received_monotonic: float | None = None,
    ) -> bool:
        """Activate required fresh families; recover only their exact timeout."""
        # This snapshot only skips work; activation is rechecked under the lock.
        # Ordinary active ingestion must not wait on forecast persistence here.
        status = self._handoff_status
        if status != "warming" and not (
            status == "degraded"
            and self.handoff_reason == "Fresh post-refresh quotes did not arrive before timeout"
        ):
            return status == "active"
        with self._prediction_publication_lock:
            status = self.handoff_status
            timeout_recovery = (
                status == "degraded"
                and self.handoff_reason == "Fresh post-refresh quotes did not arrive before timeout"
            )
            if status != "warming" and not timeout_recovery:
                return status == "active"
            now = time.monotonic()
            current_generation = self.active_generation
            observed_generation = current_generation if generation is None else int(generation)
            received = now if received_monotonic is None else float(received_monotonic)
            if (
                observed_generation != current_generation
                or received < self.subscription_cutoff_monotonic
                or not 0.0 <= now - received <= QUOTE_FRESHNESS_SECONDS
            ):
                return False
            normalized = str(market or "").upper().strip()
            if normalized in self.symbols:
                self._fresh_markets_seen.add(normalized)
                self._fresh_market_observations[normalized] = (observed_generation, received)
            required = self.required_handoff_symbols
            if not required:
                if status == "warming":
                    self.handoff_reason = "No required handoff families are configured"
                return False
            if self.missing_required_handoff_symbols:
                if status == "warming":
                    self.handoff_reason = (
                        "Configured required families missing from subscription: "
                        + ",".join(self.missing_required_handoff_symbols)
                    )
                return False
            if not all(symbol in self._fresh_markets_seen for symbol in required):
                return False
            if timeout_recovery and not all(
                (observation := self._fresh_market_observations.get(symbol)) is not None
                and observation[0] == current_generation
                and observation[1] >= self.subscription_cutoff_monotonic
                and 0.0 <= now - observation[1] <= QUOTE_FRESHNESS_SECONDS
                for symbol in required
            ):
                return False
            self._optional_family_canary_generation = current_generation
            # The first regular-session evaluation owns the warmup clock; a
            # preopen handoff must not consume it.
            self._optional_family_canary_active_since_monotonic = 0.0
            self.handoff_status = "active"
            self.handoff_reason = None
            return True

    def _subscription_window(self) -> dict[str, object]:
        """Return the current provider-connect window for easy deterministic tests."""
        return live_subscription_window()

    def _pause_outside_subscription_window(
        self,
        window: dict[str, object],
        *,
        transport_reason: str | None = None,
    ) -> None:
        """Preserve final state and wait interruptibly while live OPRA is off-hours."""
        state = str(window.get("state") or "off_hours")
        reason = f"Databento live subscription paused: {state}"
        self.handoff_status = "off_hours"
        self.handoff_reason = reason
        if self._last_off_hours_log_state != state:
            suffix = f" after transport ended ({transport_reason})" if transport_reason else ""
            logger.info(
                "Databento live subscription suppressed during %s%s; final generation %s preserved",
                state,
                suffix,
                self.active_generation,
            )
            self._last_off_hours_log_state = state
        self._stop_event.wait(timeout=OFF_HOURS_POLL_SECONDS)

    def _watchdog_session_blocked(self) -> bool:
        """Reset progress at a closed-to-open edge and provide an opening grace."""
        now_monotonic = time.monotonic()
        if market_is_closed():
            if self._market_was_closed is not True:
                self._last_progress_monotonic = 0.0
                self._open_grace_until_monotonic = 0.0
            self._market_was_closed = True
            return True
        if self._market_was_closed is None:
            # A process first observed during regular hours did not cross the
            # opening boundary. Establish current progress immediately and let
            # the separate subscription warm-up gate control cold-start load.
            self._market_was_closed = False
            self._last_progress_monotonic = now_monotonic
            return False
        if self._market_was_closed is True:
            self._market_was_closed = False
            self._last_progress_monotonic = now_monotonic
            self._open_grace_until_monotonic = now_monotonic + OPEN_TRANSITION_GRACE_SECONDS
            return True
        return now_monotonic < self._open_grace_until_monotonic

    def _stream_stalled(self) -> bool:
        if self._watchdog_session_blocked():
            return False
        if not self._last_progress_monotonic:
            return False
        return (time.monotonic() - self._last_progress_monotonic) > STREAM_STALL_SECONDS

    def _record_blocked_capture(self, reason: str, now: float) -> None:
        try:
            from backend.capture_attempts import record_blocked_capture
            record_blocked_capture(self, reason, now)
        except OSError as exc:
            logger.warning("Blocked capture receipt failed: %s", exc)

    def _compute_and_publish(self) -> None:
        # Quote calculations and persisted audit cadence are regular-session
        # concerns. Preserve the final session state overnight instead of
        # manufacturing one invalid zero-GEX record per symbol per minute.
        # The separate opening grace protects only the stall watchdog from
        # tearing down a warming connection. It must not suppress opening gamma
        # observations needed by ORB capture.
        if market_is_closed():
            self._capture_post_close_quotes()
            return
        now = time.monotonic()
        if self._compute_is_suspended(now):
            self._record_blocked_capture("COMPUTE_SUSPENDED", now)
            return
        if now - self._last_compute < self.update_interval:
            return
        if self.handoff_status != "active":
            self._record_blocked_capture("HANDOFF_NOT_ACTIVE", now)
            return
        calculation_generation = self.active_generation
        self._last_compute = now
        for market in self.symbols:
            # Queue pressure may begin while an earlier market is calculating.
            # Yield immediately instead of starting another expensive pass.
            if self._compute_is_suspended() or self.handoff_status != "active":
                return
            capture_valid_inputs = (
                now - self._last_snapshot_write.get(market, 0.0)
                >= self.snapshot_interval
            )
            capture_invalid_inputs = (
                now - self._last_invalid_snapshot_write.get(market, 0.0)
                >= self.snapshot_interval
            )
            result = self._calculate_pin(
                market,
                capture_inputs=capture_valid_inputs,
                capture_invalid_inputs=capture_invalid_inputs,
            )
            # A reconnect can advance the generation while a calculation is in
            # progress. Never publish or persist a result assembled across two
            # subscription generations.
            if (
                calculation_generation != self.active_generation
                or self.handoff_status != "active"
            ):
                logger.info(
                    "Discarding %s calculation from subscription generation %s after handoff to %s",
                    market,
                    calculation_generation,
                    self.active_generation,
                )
                return
            if not result:
                reason = self.formula_validation_errors.get(
                    market,
                    "No valid Databento pin: insufficient live quote pairs or IV/gamma rows",
                )
                self._write_invalid_snapshot(market, reason)
                continue
            result_was_usable = bool(
                result.get("validation_is_valid") is True
                and result.get("gamma_excluded_from_model") is False
            )
            calculation_id_claimed = bool(
                str(result.get("calculation_id") or "").strip()
            )
            calculation_inputs_captured = isinstance(
                result.get("_calculation_inputs"), dict
            )
            capture_lineage_missing = bool(
                capture_valid_inputs
                and result_was_usable
                and (
                    not str(result.get("calculation_id") or "").strip()
                    or not isinstance(result.get("_calculation_inputs"), dict)
                )
            )
            if capture_lineage_missing:
                # A due capture without replayable lineage is never a valid
                # publication.  Preserve it as a diagnostic and keep the
                # success throttle open for the next compute cycle.
                result = dict(result)
                result["validation_is_valid"] = False
                result["gamma_excluded_from_model"] = True
                result["usable_for_prediction"] = False
                result["validation_failure_reasons"] = list(dict.fromkeys([
                    *list(result.get("validation_failure_reasons") or []),
                    "CALCULATION_INPUT_CAPTURE_MISSING",
                ]))
            # The lifecycle polls these buffers independently. Do not expose a
            # calculation ID until its run and full input set have committed.
            calculation_persisted = self._write_snapshot(result)
            if (
                calculation_generation != self.active_generation
                or self.handoff_status != "active"
            ):
                return
            if calculation_persisted is not True and (
                calculation_id_claimed or calculation_inputs_captured
            ):
                result = dict(result)
                result.pop("calculation_id", None)
                result.pop("_calculation_inputs", None)
            if (
                capture_valid_inputs
                and result_was_usable
                and not capture_lineage_missing
                and calculation_persisted is not True
            ):
                result = dict(result)
                result.pop("calculation_id", None)
                result.pop("_calculation_inputs", None)
                result["validation_is_valid"] = False
                result["gamma_excluded_from_model"] = True
                result["usable_for_prediction"] = False
                result["validation_failure_reasons"] = list(dict.fromkeys([
                    *list(result.get("validation_failure_reasons") or []),
                    "CALCULATION_INPUT_PERSISTENCE_FAILED",
                ]))
            elif (
                capture_invalid_inputs
                and not result_was_usable
                and calculation_id_claimed
                and calculation_inputs_captured
                and calculation_persisted is not True
            ):
                # Preserve the source invalidity as the authoritative reason;
                # persistence failure is secondary diagnostic context only.
                result["validation_failure_reasons"] = list(dict.fromkeys([
                    *list(result.get("validation_failure_reasons") or []),
                    "CALCULATION_INPUT_PERSISTENCE_FAILED",
                ]))
            if (
                result.get("validation_is_valid") is not True
                or result.get("gamma_excluded_from_model") is not False
            ):
                payload = self._snapshot_payload(result)
                self.latest_pins.pop(market, None)
                self.latest_invalid[market] = payload
                self.buffers[market].append(payload)
                self.last_diagnostic_time[market] = _utcnow_naive()
                continue
            public_result = {key: value for key, value in result.items() if not key.startswith("_")}
            self.latest_pins[market] = public_result
            self.latest_invalid.pop(market, None)
            self.buffers[market].append(public_result)
            self.last_message_time[market] = _utcnow_naive()
            for callback in self.callbacks:
                try:
                    output = callback(public_result)
                    if asyncio.iscoroutine(output):
                        asyncio.run(output)
                except Exception as exc:
                    logger.error("Databento callback error: %s", exc)

    def _capture_post_close_quotes(self) -> None:
        """Retain sampled post-cash-close quotes without publishing forecasts."""
        window = self._subscription_window()
        if window.get("state") != "post_close_research":
            return
        now = time.monotonic()
        if now - getattr(self, "_last_post_close_capture", float("-inf")) < 5.0:
            return
        self._last_post_close_capture = now
        from backend.post_close_quotes import append_quote_sample
        with self._prediction_publication_lock, self._fresh_quote_lock:
            epoch = self.subscription_epoch_id
            generation = self.active_generation
            quotes = {symbol: dict(quote) for symbol, quote in self.quotes.items()}
        try:
            append_quote_sample(
                Path(__file__).resolve().parents[1] / "logs" / "post_close_quotes",
                window, quotes, epoch=epoch, generation=generation,
            )
        except OSError as exc:
            logger.warning("Post-close research quote capture failed: %s", exc)

    def _compute_loop(self) -> None:
        """Run calculations away from the latency-sensitive record consumer."""
        while self.is_running:
            try:
                self._compute_and_publish()
            except Exception as exc:
                logger.exception("Databento compute worker error: %s", exc)
            time.sleep(min(max(self.update_interval / 10.0, 0.1), 0.5))

    @staticmethod
    def _orb_reference_bucket(observed_at_utc: datetime) -> datetime:
        observed = observed_at_utc.astimezone(timezone.utc)
        epoch_seconds = int(observed.timestamp())
        bucket_seconds = (
            epoch_seconds
            - epoch_seconds % ORB_REFERENCE_INTERVAL_SECONDS
        )
        return datetime.fromtimestamp(bucket_seconds, tz=timezone.utc)

    @staticmethod
    def _orb_reference_context_identity(
        subscription_epoch_id: object,
        selected_universe_sha256: object,
        universe_provenance: object,
        market_plan: object,
        symbol_mapping_version: object,
    ) -> str:
        return hashlib.sha256(
            json.dumps(
                {
                    "subscription_epoch_id": subscription_epoch_id,
                    "selected_universe_sha256": selected_universe_sha256,
                    "universe_provenance": universe_provenance,
                    "market_plan": market_plan,
                    "symbol_mapping_version": symbol_mapping_version,
                },
                sort_keys=True,
                separators=(",", ":"),
                default=str,
            ).encode("utf-8")
        ).hexdigest()

    def _snapshot_opening_reference_inputs(
        self,
        market: str,
        *,
        trading_date: date,
        captured_at_utc: datetime | None = None,
        now_utc: Callable[[], datetime] | None = None,
        gate_evidence: dict[str, object] | None = None,
    ) -> tuple[dict[str, object] | None, str | None]:
        """Snapshot one market's quote and universe state under the quote lock."""
        # Index rebuilds are serialized, and each worker snapshots the complete
        # immutable per-market tuple while that index remains stable. The
        # expensive parity calculation happens after the snapshot locks are released.
        # Publication must precede universe/quote locks. Waiting for a forecast
        # save while holding the quote lock would also stall the live reader.
        with self._prediction_publication_lock, self._universe_index_lock:
            self._ensure_universe_index()
            subscription_epoch_id = self.subscription_epoch_id
            generation = int(self.active_generation)
            handoff_status = str(self.handoff_status)
            selected_hash = str(
                self.subscription_metadata.get("selected_universe_sha256") or ""
            ).lower().strip()
            provenance = json.loads(
                json.dumps(
                    self.subscription_metadata.get("universe_provenance") or {},
                    sort_keys=True,
                    default=str,
                )
            )
            market_plan = json.loads(
                json.dumps(
                    (self.subscription_metadata.get("markets") or {}).get(market, {}),
                    sort_keys=True,
                    default=str,
                )
            )
            records = [
                dict(record)
                for record in self._universe_records_by_market.get(market, ())
            ]
            planned_primary_entries = [
                entry
                for entry in market_plan.get("selected_expirations", [])
                if isinstance(entry, dict) and entry.get("role") == "primary"
            ]
            try:
                mapping_primary_expiration = (
                    date.fromisoformat(
                        str(planned_primary_entries[0].get("expiration") or "")
                    )
                    if len(planned_primary_entries) == 1
                    else None
                )
            except ValueError:
                mapping_primary_expiration = None
            # Shadow contracts are deliberately absent during the opening
            # stage. ORB identity remains bound to the complete selected
            # universe/hash, but mapping completeness is required only for the
            # primary rows that can enter the sampled parity calculation.
            orb_records = [
                record
                for record in records
                if record.get("expiration_date") == mapping_primary_expiration
            ]
            selected_symbols = {
                str(record.get("symbol") or "") for record in orb_records
            }
            # Copy only stable object references while holding the lock shared
            # with the OPRA reader. Mapping and quote payloads are replaced,
            # never mutated in place; filtering, hashing, and dict copies can
            # therefore happen after release without widening the reader's
            # critical section during the opening burst.
            with self._fresh_quote_lock:
                mapping_snapshot = tuple(self.symbol_mappings.values())
                quote_snapshot = {
                    raw_symbol: self.quotes.get(raw_symbol)
                    for raw_symbol in selected_symbols
                }
                subscription_cutoff = float(self.subscription_cutoff_monotonic)
                # Take both clocks after the reference copy. A newer quote
                # cannot appear artificially in the future merely because this
                # sampler waited behind the live record consumer.
                now_monotonic = time.monotonic()
                captured_at = (
                    captured_at_utc.astimezone(timezone.utc)
                    if captured_at_utc is not None
                    else (now_utc or (lambda: datetime.now(timezone.utc)))().astimezone(
                        timezone.utc
                    )
                )

        mapping_versions_by_symbol: dict[str, str] = {}
        mapping_conflict = False
        for mapping in mapping_snapshot:
            raw_symbol = str(mapping.get("raw_symbol") or "")
            if raw_symbol not in selected_symbols:
                continue
            version = str(mapping.get("mapping_version") or "").lower().strip()
            previous = mapping_versions_by_symbol.get(raw_symbol)
            if previous is not None and previous != version:
                mapping_conflict = True
            mapping_versions_by_symbol[raw_symbol] = version
        mapping_entries = sorted(mapping_versions_by_symbol.items())
        mapped_symbols = set(mapping_versions_by_symbol)
        symbol_mapping_version = hashlib.sha256(
            json.dumps(mapping_entries, separators=(",", ":")).encode("utf-8")
        ).hexdigest()
        quotes = {
            raw_symbol: dict(quote)
            for raw_symbol, quote in quote_snapshot.items()
            if quote
        }

        if gate_evidence is not None:
            gate_evidence.update(
                snapshot_subscription_epoch_id=subscription_epoch_id,
                snapshot_generation=generation,
                snapshot_handoff_status=handoff_status,
                selected_universe_sha256=selected_hash,
                universe_provenance=provenance,
                planned_primary_expiration=(
                    mapping_primary_expiration.isoformat()
                    if mapping_primary_expiration is not None else None
                ),
                selected_primary_contract_count=len(selected_symbols),
                mapped_primary_contract_count=len(mapped_symbols),
                mapping_conflict=mapping_conflict,
                symbol_mapping_version=symbol_mapping_version,
                available_quote_count=len(quotes),
            )

        if handoff_status != "active":
            return None, "HANDOFF_NOT_ACTIVE"
        if generation <= 0:
            return None, "SUBSCRIPTION_GENERATION_INVALID"
        primary_entries = [
            entry
            for entry in market_plan.get("selected_expirations", [])
            if isinstance(entry, dict) and entry.get("role") == "primary"
        ]
        if len(primary_entries) != 1:
            return None, "PLANNED_PRIMARY_EXPIRATION_UNAVAILABLE"
        try:
            primary_expiration = date.fromisoformat(
                str(primary_entries[0].get("expiration") or "")
            )
        except ValueError:
            return None, "PLANNED_PRIMARY_EXPIRATION_INVALID"
        if market in {"SPX", "NDX"} and primary_expiration != trading_date:
            return None, "PRIMARY_EXPIRATION_NOT_SAME_DAY"
        if market == "VIX" and not _expiration_is_live_eligible(
            market, primary_expiration, trading_date
        ):
            return None, VIX_PRIMARY_NOT_FORWARD_REASON
        primary_semantics = _expiration_authority_metadata(
            market, primary_expiration, trading_date
        )
        if mapping_conflict:
            return None, "MAPPING_PROVENANCE_AMBIGUOUS"
        if not selected_symbols or mapped_symbols != selected_symbols:
            return None, "MAPPING_PROVENANCE_INCOMPLETE"
        if any(
            re.fullmatch(r"[0-9a-f]{64}", version) is None
            for _raw_symbol, version in mapping_entries
        ):
            return None, "MAPPING_PROVENANCE_INVALID"
        if re.fullmatch(r"[0-9a-f]{64}", selected_hash) is None:
            return None, "UNIVERSE_PROVENANCE_INVALID"
        if provenance.get("is_fallback") is not False:
            return None, "FALLBACK_PROVENANCE"
        if str(provenance.get("trading_date") or "") != trading_date.isoformat():
            return None, "UNIVERSE_TRADING_DATE_MISMATCH"

        return {
            "market": market,
            "trading_date": trading_date,
            "subscription_epoch_id": subscription_epoch_id,
            "generation": generation,
            "handoff_status": handoff_status,
            "selected_universe_sha256": selected_hash,
            "universe_provenance": provenance,
            "market_plan": market_plan,
            "symbol_mapping_version": symbol_mapping_version,
            "mapping_versions_by_symbol": mapping_versions_by_symbol,
            "context_identity": self._orb_reference_context_identity(
                subscription_epoch_id,
                selected_hash,
                provenance,
                market_plan,
                symbol_mapping_version,
            ),
            "primary_expiration": primary_expiration,
            "primary_expiration_authority": primary_semantics["authority"],
            "primary_expiration_context_only": primary_semantics["context_only"],
            "same_day_authority": primary_semantics["same_day_authority"],
            "primary_expiration_selection_basis": primary_semantics[
                "selection_basis"
            ],
            "records": orb_records,
            "quotes": quotes,
            "subscription_cutoff_monotonic": subscription_cutoff,
            "snapshot_monotonic": now_monotonic,
            "captured_at_utc": captured_at,
            "opening_gate_evidence": gate_evidence,
        }, None

    def _compute_opening_reference_payload(
        self,
        snapshot: dict[str, object],
        *,
        sample_timestamp_utc: datetime,
        captured_at_utc: datetime,
    ) -> tuple[dict[str, object] | None, str | None]:
        """Compute deterministic primary-series parity from a copied snapshot."""
        market = str(snapshot["market"])
        generation = int(snapshot["generation"])
        primary_expiration = snapshot["primary_expiration"]
        assert isinstance(primary_expiration, date)
        now_monotonic = float(snapshot["snapshot_monotonic"])
        subscription_cutoff = float(snapshot["subscription_cutoff_monotonic"])
        quote_snapshot = snapshot["quotes"]
        assert isinstance(quote_snapshot, dict)
        mapping_versions_by_symbol = snapshot["mapping_versions_by_symbol"]
        assert isinstance(mapping_versions_by_symbol, dict)
        trading_date = snapshot["trading_date"]
        assert isinstance(trading_date, date)
        session_start_ns = int(datetime(
            trading_date.year, trading_date.month, trading_date.day,
            9, 30, tzinfo=_NY_TZ,
        ).timestamp() * 1_000_000_000)
        session_end_ns = int(datetime(
            trading_date.year, trading_date.month, trading_date.day,
            16, 0, tzinfo=_NY_TZ,
        ).timestamp() * 1_000_000_000)
        captured_at_ns = int(captured_at_utc.timestamp() * 1_000_000_000)
        pair_legs: dict[tuple[object, ...], dict[str, list[dict[str, object]]]] = {}
        gate_evidence = snapshot.get("opening_gate_evidence")
        rejections: dict[str, int] = {}

        def reject_quote(reason: str) -> None:
            rejections[reason] = rejections.get(reason, 0) + 1

        if isinstance(gate_evidence, dict):
            gate_evidence.update(
                pair_evaluation="started", minimum_pair_count=MIN_PAIRED_QUOTES,
                quote_rejections=rejections,
            )

        for record in snapshot["records"]:
            if not isinstance(record, dict) or record.get("expiration_date") != primary_expiration:
                continue
            raw_symbol = str(record.get("symbol") or "").strip()
            parsed_symbol = RAW_SYMBOL_RE.match(raw_symbol)
            if parsed_symbol is None:
                return None, "OPTION_SERIES_IDENTITY_MISSING"
            option_root = str(record.get("option_root") or "").upper().strip()
            if not option_root or option_root != parsed_symbol.group("root"):
                return None, "OPTION_SERIES_IDENTITY_MISSING"
            option_type = str(record.get("option_type") or "").upper()
            if option_type not in {"C", "P"}:
                continue
            quote = quote_snapshot.get(raw_symbol)
            if not isinstance(quote, dict):
                reject_quote("missing_quote")
                continue
            received_monotonic = float(quote.get("received_monotonic") or 0.0)
            quote_age = now_monotonic - received_monotonic
            if (
                int(quote.get("generation") or 0) != generation
                or received_monotonic < subscription_cutoff
                or quote_age < 0.0
                or quote_age > QUOTE_FRESHNESS_SECONDS
            ):
                reject_quote("generation_or_local_freshness_invalid")
                continue
            try:
                bid = float(quote.get("bid"))
                ask = float(quote.get("ask"))
                mid = float(quote.get("mid"))
                strike = float(record.get("strike"))
            except (TypeError, ValueError):
                reject_quote("invalid_price")
                continue
            if (
                not all(math.isfinite(value) for value in (bid, ask, mid, strike))
                or bid < 0.0
                or ask < bid
                or mid <= 0.0
                or strike <= 0.0
            ):
                reject_quote("invalid_price")
                continue
            ts_event_ns = _record_timestamp_ns(quote, "ts_event_ns")
            ts_recv_ns = _record_timestamp_ns(quote, "ts_recv_ns")
            ts_index_ns = _record_timestamp_ns(quote, "ts_index_ns")
            mapping_version = str(quote.get("mapping_version") or "").lower().strip()
            current_mapping_version = str(
                mapping_versions_by_symbol.get(raw_symbol) or ""
            ).lower().strip()
            if (
                ts_event_ns is None
                or ts_recv_ns is None
                or ts_index_ns is None
                or quote.get("provider_timestamp_order_valid") is not True
                or ts_event_ns > ts_recv_ns
                or re.fullmatch(r"[0-9a-f]{64}", mapping_version) is None
                or mapping_version != current_mapping_version
            ):
                reject_quote("timestamp_or_mapping_invalid")
                continue
            # Reject ineligible candidates before ranking the contributing pairs.
            # A freshly received CBBO can retain an earlier session's event
            # timestamp; selecting it first used to poison the entire sample
            # even when enough other fully eligible pairs were available.
            provider_age = (captured_at_ns - ts_recv_ns) / 1_000_000_000
            if (provider_age < -CLOCK_SYNC_NEGATIVE_TOLERANCE_SECONDS
                    or provider_age > QUOTE_FRESHNESS_SECONDS):
                reject_quote("provider_timestamp_freshness_invalid")
                continue
            if not (session_start_ns <= ts_event_ns < session_end_ns
                    and session_start_ns <= ts_recv_ns < session_end_ns):
                reject_quote("provider_timestamp_outside_regular_session")
                continue
            multiplier = record.get("contract_multiplier")
            if multiplier is not None and pd.isna(multiplier):
                multiplier = None
            settlement = record.get("settlement_type")
            if settlement is not None and pd.isna(settlement):
                settlement = None
            pair_key = (
                option_root,
                primary_expiration.isoformat(),
                strike,
                multiplier,
                settlement,
            )
            pair_legs.setdefault(pair_key, {"C": [], "P": []})[option_type].append(
                {
                    "symbol": raw_symbol,
                    "option_root": option_root,
                    "strike": strike,
                    "option_type": option_type,
                    "mid": mid,
                    "mapping_version": mapping_version,
                    "ts_event_ns": ts_event_ns,
                    "ts_recv_ns": ts_recv_ns,
                    "ts_index_ns": ts_index_ns,
                    "contract_multiplier": multiplier,
                    "settlement_type": settlement,
                }
            )

        complete_pairs: list[dict[str, object]] = []
        for pair_key, legs in pair_legs.items():
            # Ambiguous duplicate legs are not resolved by arrival order.
            if len(legs["C"]) != 1 or len(legs["P"]) != 1:
                continue
            call = legs["C"][0]
            put = legs["P"][0]
            complete_pairs.append(
                {
                    "pair_key": pair_key,
                    "option_root": pair_key[0],
                    "strike": float(pair_key[2]),
                    "call": call,
                    "put": put,
                    "pair_gap": abs(float(call["mid"]) - float(put["mid"])),
                }
            )
        if isinstance(gate_evidence, dict):
            gate_evidence.update(
                pair_evaluation="evaluated", complete_pair_count=len(complete_pairs),
                incomplete_or_ambiguous_pair_count=len(pair_legs) - len(complete_pairs),
            )
        if len(complete_pairs) < MIN_PAIRED_QUOTES:
            return None, "COMPLETE_PAIR_MINIMUM_NOT_MET"

        complete_pairs.sort(
            key=lambda pair: (
                float(pair["pair_gap"]),
                str(pair["option_root"]),
                float(pair["strike"]),
                json.dumps(pair["pair_key"], default=str, separators=(",", ":")),
            )
        )
        contributing_pairs = complete_pairs[:15]
        years = years_to_expiration(primary_expiration, now=captured_at_utc)
        risk_free_rate = _configured_risk_free_rate()
        discount = math.exp(-risk_free_rate * years)
        parity_values = [
            float(pair["call"]["mid"])
            - float(pair["put"]["mid"])
            + float(pair["strike"]) * discount
            for pair in contributing_pairs
        ]
        reference_price = float(np.median(np.asarray(parity_values, dtype=np.float64)))
        if not math.isfinite(reference_price) or reference_price <= 0.0:
            return None, "REFERENCE_PRICE_INVALID"

        contributing_legs = [
            leg
            for pair in contributing_pairs
            for leg in (pair["call"], pair["put"])
        ]
        event_values = [int(leg["ts_event_ns"]) for leg in contributing_legs]
        recv_values = [int(leg["ts_recv_ns"]) for leg in contributing_legs]
        index_values = [int(leg["ts_index_ns"]) for leg in contributing_legs]
        captured_at_ns = int(captured_at_utc.timestamp() * 1_000_000_000)
        quote_ages = [
            (captured_at_ns - timestamp_ns) / 1_000_000_000
            for timestamp_ns in recv_values
        ]
        if isinstance(gate_evidence, dict):
            gate_evidence.update(
                minimum_source_quote_age_seconds=min(quote_ages),
                maximum_source_quote_age_seconds=max(quote_ages),
                earliest_ts_event_ns=min(event_values), latest_ts_event_ns=max(event_values),
                earliest_ts_recv_ns=min(recv_values), latest_ts_recv_ns=max(recv_values),
            )
        if (
            min(quote_ages) < -CLOCK_SYNC_NEGATIVE_TOLERANCE_SECONDS
            or max(quote_ages) > QUOTE_FRESHNESS_SECONDS
        ):
            return None, "PROVIDER_TIMESTAMP_FRESHNESS_INVALID"
        trading_date = snapshot["trading_date"]
        assert isinstance(trading_date, date)
        for timestamp_ns in (*event_values, *recv_values):
            timestamp_et = datetime.fromtimestamp(
                timestamp_ns / 1_000_000_000,
                tz=timezone.utc,
            ).astimezone(_NY_TZ)
            if (
                timestamp_et.date() != trading_date
                or timestamp_et.time() < datetime.strptime("09:30", "%H:%M").time()
                or timestamp_et.time() >= datetime.strptime("16:00", "%H:%M").time()
            ):
                return None, "PROVIDER_TIMESTAMP_OUTSIDE_REGULAR_SESSION"

        replay_pairs = [
            {
                "pair_identity": list(pair["pair_key"]),
                "strike": pair["strike"],
                "call_symbol": pair["call"]["symbol"],
                "call_mid": pair["call"]["mid"],
                "call_mapping_version": pair["call"]["mapping_version"],
                "put_symbol": pair["put"]["symbol"],
                "put_mid": pair["put"]["mid"],
                "put_mapping_version": pair["put"]["mapping_version"],
            }
            for pair in contributing_pairs
        ]
        formula_inputs = {
            "formula_version": SPOT_FORMULA_VERSION,
            "risk_free_rate": risk_free_rate,
            "time_to_expiration_years": years,
            "pair_limit": 15,
            "pairs": replay_pairs,
            "primary_expiration_authority": snapshot[
                "primary_expiration_authority"
            ],
            "primary_expiration_context_only": snapshot[
                "primary_expiration_context_only"
            ],
            "same_day_authority": snapshot["same_day_authority"],
            "primary_expiration_selection_basis": snapshot[
                "primary_expiration_selection_basis"
            ],
        }
        pair_identity_sha256 = hashlib.sha256(
            json.dumps(
                replay_pairs,
                sort_keys=True,
                separators=(",", ":"),
                default=str,
            ).encode("utf-8")
        ).hexdigest()
        earliest_recv_ns = min(recv_values)
        latest_recv_ns = max(recv_values)
        return {
            "symbol": market,
            "provider": self.provider_name,
            "subscription_epoch_id": snapshot["subscription_epoch_id"],
            "sample_timestamp_utc": sample_timestamp_utc.isoformat(),
            "source_timestamp_utc": _timestamp_ns_to_utc_iso(latest_recv_ns),
            "captured_at_utc": captured_at_utc.isoformat(),
            "subscription_generation": generation,
            "active_generation": generation,
            "reference_price": reference_price,
            "spot_source": "databento_opra_put_call_parity",
            "spot_formula_version": SPOT_FORMULA_VERSION,
            "risk_free_rate": risk_free_rate,
            "time_to_expiration_years": years,
            "primary_expiration": primary_expiration.isoformat(),
            "planned_primary_expiration": primary_expiration.isoformat(),
            "same_day_profile_available": primary_expiration == trading_date,
            "primary_expiration_authority": snapshot[
                "primary_expiration_authority"
            ],
            "primary_expiration_context_only": snapshot[
                "primary_expiration_context_only"
            ],
            "same_day_authority": snapshot["same_day_authority"],
            "primary_expiration_selection_basis": snapshot[
                "primary_expiration_selection_basis"
            ],
            "universe_sha256": snapshot["selected_universe_sha256"],
            "universe_is_fallback": False,
            "universe_provenance": snapshot["universe_provenance"],
            "paired_quote_count": len(complete_pairs),
            "minimum_paired_quote_count": MIN_PAIRED_QUOTES,
            "contributing_pair_count": len(contributing_pairs),
            "contributing_quote_count": len(contributing_legs),
            "earliest_ts_event_ns": min(event_values),
            "latest_ts_event_ns": max(event_values),
            "earliest_ts_recv_ns": earliest_recv_ns,
            "latest_ts_recv_ns": latest_recv_ns,
            "observation_index_ns": max(index_values),
            "source_quote_age_seconds": min(quote_ages),
            "maximum_source_quote_age_seconds": max(quote_ages),
            "source_timestamp_span_seconds": (
                latest_recv_ns - earliest_recv_ns
            ) / 1_000_000_000,
            "quote_freshness_limit_seconds": QUOTE_FRESHNESS_SECONDS,
            "pair_identity_sha256": pair_identity_sha256,
            "symbol_mapping_version": snapshot["symbol_mapping_version"],
            "formula_inputs": formula_inputs,
            "processing_clock_status": "synchronized",
            "timestamp_order_valid": True,
            "handoff_status": "active",
            "generation_state_unchanged": True,
            "universe_state_unchanged": True,
        }, None

    def _opening_reference_context_is_current(
        self,
        snapshot: dict[str, object],
    ) -> bool:
        market = str(snapshot["market"])
        with self._prediction_publication_lock, self._universe_index_lock:
            records = self._universe_records_by_market.get(market, ())
            current_subscription_epoch_id = self.subscription_epoch_id
            current_handoff_status = self.handoff_status
            current_generation = int(self.active_generation)
            selected_universe_sha256 = str(
                self.subscription_metadata.get("selected_universe_sha256") or ""
            )
            universe_provenance = json.loads(
                json.dumps(
                    self.subscription_metadata.get("universe_provenance") or {},
                    sort_keys=True,
                    default=str,
                )
            )
            market_plan = json.loads(
                json.dumps(
                    (self.subscription_metadata.get("markets") or {}).get(
                        market, {}
                    ),
                    sort_keys=True,
                    default=str,
                )
            )
            current_primary_entries = [
                entry
                for entry in market_plan.get("selected_expirations", [])
                if isinstance(entry, dict) and entry.get("role") == "primary"
            ]
            try:
                current_primary_expiration = (
                    date.fromisoformat(
                        str(current_primary_entries[0].get("expiration") or "")
                    )
                    if len(current_primary_entries) == 1
                    else None
                )
            except ValueError:
                current_primary_expiration = None
            selected_symbols = {
                str(record.get("symbol") or "")
                for record in records
                if record.get("expiration_date") == current_primary_expiration
            }
            # As in the initial snapshot, retain stable mapping object
            # references under the reader lock and do Python filtering/hashing
            # only after release.
            with self._fresh_quote_lock:
                mapping_snapshot = tuple(self.symbol_mappings.values())

        mapping_versions_by_symbol: dict[str, str] = {}
        mapping_conflict = False
        for mapping in mapping_snapshot:
            raw_symbol = str(mapping.get("raw_symbol") or "")
            if raw_symbol not in selected_symbols:
                continue
            version = str(mapping.get("mapping_version") or "").lower().strip()
            previous = mapping_versions_by_symbol.get(raw_symbol)
            if previous is not None and previous != version:
                mapping_conflict = True
            mapping_versions_by_symbol[raw_symbol] = version
        mapping_entries = sorted(mapping_versions_by_symbol.items())
        symbol_mapping_version = hashlib.sha256(
            json.dumps(mapping_entries, separators=(",", ":")).encode("utf-8")
        ).hexdigest()
        current_identity = self._orb_reference_context_identity(
            current_subscription_epoch_id,
            selected_universe_sha256,
            universe_provenance,
            market_plan,
            symbol_mapping_version,
        )
        return bool(
            not mapping_conflict
            and current_subscription_epoch_id
            == snapshot["subscription_epoch_id"]
            and current_handoff_status == "active"
            and current_generation == int(snapshot["generation"])
            and current_identity == snapshot["context_identity"]
        )

    def _finalize_opening_reference_decision(
        self,
        *,
        journal: object | None,
        result: dict[str, object],
        intended_bucket_utc: datetime,
        attempt_completed_at_utc: datetime,
    ) -> dict[str, object]:
        """Append a final progress decision without modifying raw evidence."""
        if result.get("recorded") is not True:
            return result
        if journal is None:
            from backend.market_structure import get_market_structure_journal

            journal = get_market_structure_journal()
        decision_recorder = getattr(journal, "record_reference_decision", None)
        if not callable(decision_recorder):
            result.update(
                progress_eligible=False,
                progress_decision_pending=True,
                progress_decision_recorded=False,
                reason="REFERENCE_PROGRESS_DECISION_PERSISTENCE_FAILED",
            )
            return result
        try:
            sample_timestamp = datetime.fromisoformat(
                str(result.get("sample_timestamp_utc") or "").replace("Z", "+00:00")
            ).astimezone(timezone.utc)
        except ValueError:
            result.update(
                progress_eligible=False,
                progress_decision_pending=True,
                progress_decision_recorded=False,
                reason="REFERENCE_SAMPLE_BUCKET_MISMATCH",
            )
            return result
        eligible = result.get("progress_eligible") is True
        reason = None if eligible else str(
            result.get("reason") or "REFERENCE_PROGRESS_INELIGIBLE"
        )
        try:
            decision = dict(
                decision_recorder(
                    sample_id=str(result.get("sample_id") or ""),
                    sample_timestamp_utc=sample_timestamp,
                    intended_bucket_utc=intended_bucket_utc,
                    attempt_completed_at_utc=attempt_completed_at_utc,
                    progress_eligible=eligible,
                    reason=reason,
                )
            )
        except Exception as exc:
            logger.exception("ORB reference decision persistence failed: %s", exc)
            decision = {"recorded": False}
        if decision.get("recorded") is not True:
            result.update(
                progress_eligible=False,
                progress_decision_pending=True,
                progress_decision_recorded=False,
                reason="REFERENCE_PROGRESS_DECISION_PERSISTENCE_FAILED",
            )
            return result
        result.update(
            progress_decision_pending=False,
            progress_decision_recorded=True,
            progress_decided_at_utc=attempt_completed_at_utc.isoformat(),
        )
        return result

    def _complete_opening_reference_persisted_sample(
        self,
        *,
        journal: object,
        result: dict[str, object],
        snapshot: dict[str, object],
        intended_bucket: datetime | None,
        clock: Callable[[], datetime],
        attempt_deadline_utc: datetime | None = None,
        attempt_deadline_monotonic: float | None = None,
        attempt_cancel_event: threading.Event | None = None,
    ) -> dict[str, object]:
        """Evaluate and retain the final decision while holding the save barrier."""
        if result.get("recorded") is not True:
            return result
        result["progress_decision_pending"] = True
        try:
            persisted_generation = int(result.get("subscription_generation"))
        except (TypeError, ValueError):
            persisted_generation = None
        persisted_epoch_id = str(result.get("subscription_epoch_id") or "")
        active_generation_after_persist = int(self.active_generation)
        post_persist_observed_at = clock().astimezone(timezone.utc)
        captured_at = snapshot.get("captured_at_utc")
        decision_completed_at = (
            captured_at.astimezone(timezone.utc)
            if intended_bucket is None and isinstance(captured_at, datetime)
            else post_persist_observed_at
        )
        persisted_inside_intended_bucket = bool(
            intended_bucket is None
            or self._orb_reference_bucket(post_persist_observed_at)
            == intended_bucket
        )
        deadline_current_after_persist = bool(
            not (
                attempt_cancel_event is not None
                and attempt_cancel_event.is_set()
            )
            and (
                attempt_deadline_monotonic is None
                or time.monotonic() < float(attempt_deadline_monotonic)
            )
            and (
                attempt_deadline_utc is None
                or post_persist_observed_at < attempt_deadline_utc
            )
        )
        context_current_after_persist = bool(
            persisted_generation == int(snapshot["generation"])
            and persisted_epoch_id == self.subscription_epoch_id
            and persisted_epoch_id == snapshot["subscription_epoch_id"]
            and self._opening_reference_context_is_current(snapshot)
            and str(self._processing_clock_telemetry().get("status") or "unknown")
            == "synchronized"
            and persisted_inside_intended_bucket
            and deadline_current_after_persist
        )
        result.update(
            {
                "captured_context_identity": snapshot["context_identity"],
                "active_subscription_epoch_id_after_persist": self.subscription_epoch_id,
                "active_generation_after_persist": active_generation_after_persist,
                "post_persist_context_current": context_current_after_persist,
                "progress_eligible": context_current_after_persist,
            }
        )
        if re.fullmatch(r"[0-9a-f]{64}", persisted_epoch_id) is None:
            result["reason"] = "PERSISTED_SUBSCRIPTION_EPOCH_UNAVAILABLE"
        elif persisted_epoch_id != self.subscription_epoch_id:
            result["reason"] = "PERSISTED_SUBSCRIPTION_EPOCH_MISMATCH"
        elif persisted_generation is None:
            result["reason"] = "PERSISTED_GENERATION_UNAVAILABLE"
        elif persisted_generation != int(snapshot["generation"]):
            result["reason"] = "PERSISTED_GENERATION_MISMATCH"
        elif not persisted_inside_intended_bucket:
            result["reason"] = "REFERENCE_CAPTURE_COMPLETED_OUTSIDE_INTENDED_BUCKET"
        elif not deadline_current_after_persist:
            result["reason"] = "REFERENCE_ATTEMPT_DEADLINE_EXCEEDED"
        elif not context_current_after_persist:
            # The raw row remains valid audit evidence for its captured
            # generation, but the immutable sidecar makes it ineligible.
            result["reason"] = "REFERENCE_CONTEXT_CHANGED_AFTER_PERSIST"
        decision_bucket = intended_bucket
        if decision_bucket is None:
            try:
                decision_bucket = self._orb_reference_bucket(
                    datetime.fromisoformat(
                        str(result.get("sample_timestamp_utc") or "").replace(
                            "Z", "+00:00"
                        )
                    ).astimezone(timezone.utc)
                )
            except ValueError:
                decision_bucket = post_persist_observed_at
        return self._finalize_opening_reference_decision(
            journal=journal,
            result=result,
            intended_bucket_utc=decision_bucket,
            attempt_completed_at_utc=decision_completed_at,
        )

    def _opening_reference_persist_rejection_reason(
        self,
        *,
        snapshot: dict[str, object],
        observed_at_utc: datetime,
        intended_bucket_utc: datetime | None,
        attempt_deadline_utc: datetime | None,
        attempt_deadline_monotonic: float | None,
        attempt_cancel_event: threading.Event | None,
        expected_subscription_epoch_id: str | None,
        expected_subscription_generation: int | None,
    ) -> str | None:
        """Revalidate a scheduled attempt immediately before its raw append."""
        if self._stop_event.is_set() or (
            intended_bucket_utc is not None and not self.is_running
        ):
            return "REFERENCE_STREAMER_STOPPING"
        if (
            (attempt_cancel_event is not None and attempt_cancel_event.is_set())
            or (
                attempt_deadline_monotonic is not None
                and time.monotonic() >= float(attempt_deadline_monotonic)
            )
            or (
                attempt_deadline_utc is not None
                and observed_at_utc >= attempt_deadline_utc
            )
        ):
            return "REFERENCE_ATTEMPT_DEADLINE_EXCEEDED"
        if (
            intended_bucket_utc is not None
            and self._orb_reference_bucket(observed_at_utc)
            != intended_bucket_utc
        ):
            return "REFERENCE_CAPTURE_COMPLETED_OUTSIDE_INTENDED_BUCKET"
        if (
            expected_subscription_epoch_id is not None
            and (
                self.subscription_epoch_id != expected_subscription_epoch_id
                or str(snapshot.get("subscription_epoch_id") or "")
                != expected_subscription_epoch_id
            )
        ):
            return "REFERENCE_SUBSCRIPTION_EPOCH_CHANGED_DURING_SAMPLE"
        if (
            expected_subscription_generation is not None
            and (
                int(self.active_generation) != expected_subscription_generation
                or int(snapshot.get("generation") or 0)
                != expected_subscription_generation
            )
        ):
            return "REFERENCE_SUBSCRIPTION_GENERATION_CHANGED_DURING_SAMPLE"
        if (
            not self._opening_reference_context_is_current(snapshot)
            or str(self._processing_clock_telemetry().get("status") or "unknown")
            != "synchronized"
        ):
            return "REFERENCE_STATE_CHANGED_DURING_SAMPLE"
        return None

    def _capture_opening_reference_once(
        self,
        market: str,
        *,
        observed_at_utc: datetime | None = None,
        journal: object | None = None,
        intended_bucket_utc: datetime | None = None,
        now_utc: Callable[[], datetime] | None = None,
        attempt_diagnostics: dict[str, object] | None = None,
        attempt_deadline_utc: datetime | None = None,
        attempt_deadline_monotonic: float | None = None,
        attempt_cancel_event: threading.Event | None = None,
        expected_subscription_epoch_id: str | None = None,
        expected_subscription_generation: int | None = None,
    ) -> dict[str, object]:
        """Capture one ORB reference without invoking the full GEX calculation."""
        clock = now_utc or (lambda: datetime.now(timezone.utc))
        observed_at = (observed_at_utc or clock()).astimezone(timezone.utc)
        normalized_deadline_utc = (
            attempt_deadline_utc.astimezone(timezone.utc)
            if attempt_deadline_utc is not None
            else None
        )
        if self._stop_event.is_set():
            return {
                "recorded": False,
                "reason": "REFERENCE_STREAMER_STOPPING",
                "progress_eligible": False,
            }
        if (
            (attempt_cancel_event is not None and attempt_cancel_event.is_set())
            or (
                attempt_deadline_monotonic is not None
                and time.monotonic() >= float(attempt_deadline_monotonic)
            )
            or (
                normalized_deadline_utc is not None
                and observed_at >= normalized_deadline_utc
            )
        ):
            return {
                "recorded": False,
                "reason": "REFERENCE_ATTEMPT_DEADLINE_EXCEEDED",
                "progress_eligible": False,
            }
        if (
            expected_subscription_epoch_id is not None
            and self.subscription_epoch_id != expected_subscription_epoch_id
        ):
            return {
                "recorded": False,
                "reason": "REFERENCE_SUBSCRIPTION_EPOCH_CHANGED_DURING_SAMPLE",
                "progress_eligible": False,
            }
        if (
            expected_subscription_generation is not None
            and int(self.active_generation) != expected_subscription_generation
        ):
            return {
                "recorded": False,
                "reason": "REFERENCE_SUBSCRIPTION_GENERATION_CHANGED_DURING_SAMPLE",
                "progress_eligible": False,
            }
        intended_bucket = None
        if intended_bucket_utc is not None:
            intended_bucket = intended_bucket_utc.astimezone(timezone.utc)
            if self._orb_reference_bucket(intended_bucket) != intended_bucket:
                return {
                    "recorded": False,
                    "reason": "INTENDED_REFERENCE_BUCKET_INVALID",
                    "progress_eligible": False,
                }
            if self._orb_reference_bucket(observed_at) != intended_bucket:
                return {
                    "recorded": False,
                    "reason": "REFERENCE_CAPTURE_STARTED_OUTSIDE_INTENDED_BUCKET",
                    "progress_eligible": False,
                }
            if not self.is_running:
                return {
                    "recorded": False,
                    "reason": "REFERENCE_STREAMER_STOPPING",
                    "progress_eligible": False,
                }
        window = live_subscription_window(observed_at)
        if window.get("state") != "regular_session":
            return {"recorded": False, "reason": "OUTSIDE_REGULAR_SESSION"}
        clock_evidence = self._processing_clock_telemetry()
        clock_status = str(clock_evidence.get("status") or "unknown")
        gate_evidence: dict[str, object] = {
            "observed_at_utc": observed_at.isoformat(),
            "processing_clock": clock_evidence,
            "pair_evaluation": "not_evaluated",
        }
        with self._prediction_publication_lock:
            gate_evidence["handoff"] = {
                "status": self.handoff_status,
                "reason": self.handoff_reason,
                "required_symbols": list(self.required_handoff_symbols),
                "missing_configured_symbols": list(self.missing_required_handoff_symbols),
                "missing_fresh_symbols": [
                    symbol for symbol in self.required_handoff_symbols
                    if symbol not in self._fresh_markets_seen
                ],
                "fresh_observations": {
                    symbol: {"generation": generation, "age_seconds": time.monotonic() - received}
                    for symbol, (generation, received) in self._fresh_market_observations.items()
                },
            }
        if attempt_diagnostics is not None:
            attempt_diagnostics["opening_gate_evidence"] = gate_evidence
        if clock_status != "synchronized":
            return {
                "recorded": False,
                "reason": "PROCESSING_CLOCK_NOT_SYNCHRONIZED",
            }
        trading_date = observed_at.astimezone(_NY_TZ).date()
        snapshot, reason = self._snapshot_opening_reference_inputs(
            market,
            trading_date=trading_date,
            captured_at_utc=observed_at if observed_at_utc is not None else None,
            now_utc=clock,
            gate_evidence=gate_evidence,
        )
        if snapshot is None:
            return {"recorded": False, "reason": reason}
        captured_at = snapshot["captured_at_utc"]
        assert isinstance(captured_at, datetime)
        if (
            intended_bucket is not None
            and self._orb_reference_bucket(captured_at) != intended_bucket
        ):
            return {
                "recorded": False,
                "reason": "REFERENCE_CAPTURE_STARTED_OUTSIDE_INTENDED_BUCKET",
                "progress_eligible": False,
            }
        payload, reason = self._compute_opening_reference_payload(
            snapshot,
            sample_timestamp_utc=self._orb_reference_bucket(captured_at),
            captured_at_utc=captured_at,
        )
        if payload is None:
            return {"recorded": False, "reason": reason}
        if journal is None:
            from backend.market_structure import get_market_structure_journal

            journal = get_market_structure_journal()
        # Shutdown publishes its stop request before waiting on this barrier.
        # A save already inside the critical section completes before stop
        # returns; a worker arriving afterward observes the stop and cannot
        # begin persistence. Lock order is persist -> publication -> universe
        # -> quote; stop releases its publication/quote locks before this barrier.
        persist_lock_wait_started = time.monotonic()
        with self._orb_reference_persist_barrier:
            persist_lock_acquired = time.monotonic()
            if attempt_diagnostics is not None:
                attempt_diagnostics["persist_lock_wait_seconds"] = max(
                    0.0, persist_lock_acquired - persist_lock_wait_started
                )
            try:
                persist_observed_at = clock().astimezone(timezone.utc)
                rejection_reason = self._opening_reference_persist_rejection_reason(
                    snapshot=snapshot,
                    observed_at_utc=persist_observed_at,
                    intended_bucket_utc=intended_bucket,
                    attempt_deadline_utc=normalized_deadline_utc,
                    attempt_deadline_monotonic=attempt_deadline_monotonic,
                    attempt_cancel_event=attempt_cancel_event,
                    expected_subscription_epoch_id=expected_subscription_epoch_id,
                    expected_subscription_generation=(
                        expected_subscription_generation
                    ),
                )
                if rejection_reason is not None:
                    # Do not append a row after its deadline or for a bucket whose
                    # computation missed its wall-clock boundary. Later buckets
                    # may retry from fresh inputs; missed buckets are never rebuilt.
                    rejection = {
                        "recorded": False,
                        "reason": rejection_reason,
                    }
                    if rejection_reason != "REFERENCE_STATE_CHANGED_DURING_SAMPLE":
                        rejection["progress_eligible"] = False
                    return rejection
                result = dict(
                    journal.record_reference(
                        {**payload, "_defer_progress_decision": True}
                    )
                )
                return self._complete_opening_reference_persisted_sample(
                    journal=journal,
                    result=result,
                    snapshot=snapshot,
                    intended_bucket=intended_bucket,
                    clock=clock,
                    attempt_deadline_utc=normalized_deadline_utc,
                    attempt_deadline_monotonic=attempt_deadline_monotonic,
                    attempt_cancel_event=attempt_cancel_event,
                )
            finally:
                if attempt_diagnostics is not None:
                    attempt_diagnostics["persist_lock_held_seconds"] = max(
                        0.0, time.monotonic() - persist_lock_acquired
                    )

    def _capture_opening_reference_for_bucket(
        self,
        market: str,
        intended_bucket_utc: datetime,
        *,
        now_utc: Callable[[], datetime] | None = None,
        journal: object | None = None,
        attempt_deadline_utc: datetime | None = None,
        attempt_deadline_monotonic: float | None = None,
        attempt_cancel_event: threading.Event | None = None,
        expected_subscription_epoch_id: str | None = None,
        expected_subscription_generation: int | None = None,
    ) -> dict[str, object]:
        """Run one isolated market attempt and bind its result to one bucket."""
        clock = now_utc or (lambda: datetime.now(timezone.utc))
        intended_bucket = intended_bucket_utc.astimezone(timezone.utc)
        started_at = clock().astimezone(timezone.utc)
        bucket_end = intended_bucket + timedelta(
            seconds=ORB_REFERENCE_INTERVAL_SECONDS
        )
        deadline_utc = (
            attempt_deadline_utc.astimezone(timezone.utc)
            if attempt_deadline_utc is not None
            else min(
                bucket_end,
                started_at
                + timedelta(seconds=ORB_REFERENCE_ATTEMPT_TIMEOUT_SECONDS),
            )
        )
        deadline_monotonic = (
            float(attempt_deadline_monotonic)
            if attempt_deadline_monotonic is not None
            else time.monotonic()
            + max(0.0, (deadline_utc - started_at).total_seconds())
        )
        cancel_event = attempt_cancel_event or threading.Event()
        expected_epoch_id = (
            expected_subscription_epoch_id
            if expected_subscription_epoch_id is not None
            else self.subscription_epoch_id
        )
        expected_generation = (
            int(expected_subscription_generation)
            if expected_subscription_generation is not None
            else int(self.active_generation)
        )
        attempt_diagnostics: dict[str, object] = {
            "opening_gate_evidence": {"pair_evaluation": "not_evaluated", "gates_evaluated": False}
        }
        try:
            result = dict(
                self._capture_opening_reference_once(
                    market,
                    journal=journal,
                    intended_bucket_utc=intended_bucket,
                    now_utc=clock,
                    attempt_diagnostics=attempt_diagnostics,
                    attempt_deadline_utc=deadline_utc,
                    attempt_deadline_monotonic=deadline_monotonic,
                    attempt_cancel_event=cancel_event,
                    expected_subscription_epoch_id=expected_epoch_id,
                    expected_subscription_generation=expected_generation,
                )
            )
        except Exception as exc:
            result = {"recorded": False, "reason": "SAMPLER_EXCEPTION"}
            logger.exception("ORB reference sampler error for %s: %s", market, exc)
        completed_at = None
        try:
            completed_at = datetime.fromisoformat(
                str(result.get("progress_decided_at_utc") or "").replace(
                    "Z", "+00:00"
                )
            ).astimezone(timezone.utc)
        except ValueError:
            pass
        if completed_at is None:
            completed_at = clock().astimezone(timezone.utc)
        result.update(
            {
                "intended_sample_timestamp_utc": intended_bucket.isoformat(),
                "attempt_started_at_utc": started_at.isoformat(),
                "attempt_completed_at_utc": completed_at.isoformat(),
                "attempt_duration_seconds": max(
                    0.0, (completed_at - started_at).total_seconds()
                ),
                "attempt_deadline_utc": deadline_utc.isoformat(),
                "attempt_deadline_seconds": max(
                    0.0, (deadline_utc - started_at).total_seconds()
                ),
                **attempt_diagnostics,
            }
        )
        actual_bucket = None
        try:
            actual_bucket = self._orb_reference_bucket(
                datetime.fromisoformat(
                    str(result.get("sample_timestamp_utc") or "").replace("Z", "+00:00")
                ).astimezone(timezone.utc)
            )
        except ValueError:
            pass
        completion_bucket = self._orb_reference_bucket(completed_at)
        deadline_exceeded = bool(
            cancel_event.is_set()
            or completed_at >= deadline_utc
            or time.monotonic() >= deadline_monotonic
        )
        if (
            result.get("recorded") is True
            and result.get("progress_decision_recorded") is not True
            and actual_bucket != intended_bucket
        ):
            result["progress_eligible"] = False
            result["reason"] = "REFERENCE_SAMPLE_BUCKET_MISMATCH"
        elif (
            result.get("recorded") is True
            and result.get("progress_decision_recorded") is not True
            and completion_bucket != intended_bucket
        ):
            result["progress_eligible"] = False
            result["reason"] = "REFERENCE_CAPTURE_COMPLETED_OUTSIDE_INTENDED_BUCKET"
        elif (
            result.get("recorded") is True
            and result.get("progress_decision_recorded") is not True
            and deadline_exceeded
        ):
            result["progress_eligible"] = False
            result["reason"] = "REFERENCE_ATTEMPT_DEADLINE_EXCEEDED"
        return result

    def _record_opening_reference_failed_attempt(
        self,
        *,
        market: str,
        subscription_epoch_id: str,
        subscription_generation: int,
        intended_bucket_utc: datetime,
        result: dict[str, object],
    ) -> None:
        """Retain every recent rejection and rate-limit structured warnings."""
        if (
            result.get("recorded") is True
            and result.get("progress_eligible") is True
        ):
            return
        reason = str(result.get("reason") or "REFERENCE_PROGRESS_INELIGIBLE")
        result["reason"] = reason
        failure: dict[str, object] = {
            "event": "orb_reference_attempt_failed",
            "schema_version": "orb-reference-failed-attempt-v1",
            "market": market,
            "reason": reason,
            "recorded": result.get("recorded") is True,
            "progress_eligible": result.get("progress_eligible") is True,
            "intended_bucket_utc": intended_bucket_utc.astimezone(
                timezone.utc
            ).isoformat(),
            "subscription_epoch_id": str(subscription_epoch_id),
            "subscription_generation": int(subscription_generation),
        }
        for key in (
            "attempt_started_at_utc",
            "attempt_completed_at_utc",
            "attempt_duration_seconds",
            "attempt_deadline_utc",
            "attempt_deadline_seconds",
            "persist_lock_wait_seconds",
            "persist_lock_held_seconds",
            "sample_timestamp_utc",
            "subscription_epoch_id",
            "subscription_generation",
            "active_generation_after_persist",
            "active_generation_after_return",
            "opening_gate_evidence",
        ):
            value = result.get(key)
            if value is not None:
                result_key = (
                    f"result_{key}"
                    if key in {"subscription_epoch_id", "subscription_generation"}
                    else key
                )
                failure[result_key] = value
        warning_payload: dict[str, object] | None = None
        warning_observed_monotonic = time.monotonic()
        warning_key = (market, reason)
        with self._orb_reference_progress_lock:
            self._orb_reference_failed_attempt_count += 1
            failure["failure_sequence"] = self._orb_reference_failed_attempt_count
            self._orb_reference_failed_attempts.append(failure)
            warning_state = self._orb_reference_failure_warning_states.get(
                warning_key
            )
            if warning_state is None:
                if (
                    len(self._orb_reference_failure_warning_states)
                    >= ORB_REFERENCE_FAILED_ATTEMPT_LIMIT
                ):
                    oldest_key = next(
                        iter(self._orb_reference_failure_warning_states)
                    )
                    self._orb_reference_failure_warning_states.pop(oldest_key)
                warning_state = {
                    "last_warning_monotonic": warning_observed_monotonic,
                    "suppressed_since_last_warning": 0,
                }
                self._orb_reference_failure_warning_states[warning_key] = (
                    warning_state
                )
                warning_payload = dict(failure)
                warning_payload["suppressed_attempts_since_last_warning"] = 0
            elif (
                warning_observed_monotonic
                - float(warning_state["last_warning_monotonic"])
                >= ORB_REFERENCE_FAILURE_WARNING_INTERVAL_SECONDS
            ):
                warning_payload = dict(failure)
                warning_payload["suppressed_attempts_since_last_warning"] = int(
                    warning_state["suppressed_since_last_warning"]
                )
                warning_state["last_warning_monotonic"] = (
                    warning_observed_monotonic
                )
                warning_state["suppressed_since_last_warning"] = 0
            else:
                warning_state["suppressed_since_last_warning"] = (
                    int(warning_state["suppressed_since_last_warning"]) + 1
                )
                self._orb_reference_failure_warning_suppressed_count += 1
        # The bounded health deque and rate-limited log cannot retain a complete
        # opening failure history. Append each opening-hour rejection separately;
        # it is diagnostic evidence only and never an ORB sample or prediction.
        opening_local = intended_bucket_utc.astimezone(_NY_TZ)
        if (
            (opening_local.hour == 9 and opening_local.minute >= 30
             or opening_local.hour == 10 and opening_local.minute < 30)
        ):
            from backend.capture_attempts import record_opening_reference_failure

            try:
                with self._orb_reference_evidence_lock:
                    record_opening_reference_failure(self.audit_dir, failure)
                result["failure_evidence_recorded"] = True
            except OSError as exc:
                result["failure_evidence_recorded"] = False
                result["failure_evidence_error"] = type(exc).__name__
                logger.error("ORB opening failure evidence write failed: %s; evidence=%s",
                             type(exc).__name__, json.dumps(failure, sort_keys=True, default=str))
        if warning_payload is not None:
            warning_payload["event"] = "orb_reference_attempt_failure_summary"
            warning_payload["warning_interval_seconds"] = (
                ORB_REFERENCE_FAILURE_WARNING_INTERVAL_SECONDS
            )
            logger.warning(
                "ORB reference attempt failure summary: %s",
                json.dumps(
                    warning_payload,
                    sort_keys=True,
                    separators=(",", ":"),
                    default=str,
                ),
            )

    def _opening_reference_loop(self) -> None:
        """Persist one lightweight reference per configured wall-clock bucket."""
        completed_by_market_generation: dict[tuple[str, str, int], datetime] = {}
        timed_out_by_market_generation: dict[tuple[str, str, int], datetime] = {}
        startup_markets = tuple(dict.fromkeys(self.symbols))
        if not startup_markets:
            return
        in_flight: dict[Future[dict[str, object]], _OrbReferenceAttempt] = {}
        executor = ThreadPoolExecutor(
            # One spare lane per configured market lets a later bucket proceed
            # after an earlier worker exceeded its deadline but cannot be killed.
            max_workers=max(2, len(startup_markets) * 2),
            thread_name_prefix="marketpin-orb-reference",
        )

        def publish_result(
            attempt: _OrbReferenceAttempt,
            result: dict[str, object],
        ) -> None:
            market = attempt.market
            attempted_epoch_id = attempt.subscription_epoch_id
            attempted_generation = attempt.subscription_generation
            intended_bucket = attempt.intended_bucket_utc
            if result.get("recorded") is True:
                try:
                    actual_bucket = datetime.fromisoformat(
                        str(result["sample_timestamp_utc"]).replace("Z", "+00:00")
                    ).astimezone(timezone.utc)
                except (KeyError, ValueError):
                    actual_bucket = None
                try:
                    persisted_generation = int(
                        result.get("subscription_generation")
                    )
                except (TypeError, ValueError):
                    persisted_generation = None
                persisted_epoch_id = str(
                    result.get("subscription_epoch_id") or ""
                )
                active_generation_after_return = int(self.active_generation)
                current_generation_progress = bool(
                    actual_bucket == intended_bucket
                    and attempted_epoch_id == persisted_epoch_id
                    and attempted_generation == persisted_generation
                    and persisted_generation is not None
                    and persisted_epoch_id == self.subscription_epoch_id
                    and persisted_generation == active_generation_after_return
                    and result.get("progress_eligible") is True
                    and not attempt.cancel_event.is_set()
                )
                result["active_generation_after_return"] = (
                    active_generation_after_return
                )
                if current_generation_progress:
                    completed_by_market_generation[
                        (
                            market,
                            persisted_epoch_id,
                            persisted_generation,
                        )
                    ] = actual_bucket
                    self._last_orb_reference_bucket_utc = actual_bucket
                    with self._orb_reference_progress_lock:
                        recent = self._orb_reference_recent_buckets_by_market.setdefault(
                            market, deque(maxlen=4)
                        )
                        persisted = (
                            actual_bucket,
                            persisted_generation,
                            persisted_epoch_id,
                        )
                        if not recent or recent[-1] != persisted:
                            recent.append(persisted)
                else:
                    result["progress_eligible"] = False
                    if attempt.cancel_event.is_set():
                        result["reason"] = "REFERENCE_ATTEMPT_DEADLINE_EXCEEDED"
                    elif actual_bucket != intended_bucket:
                        result["reason"] = "REFERENCE_SAMPLE_BUCKET_MISMATCH"
                    elif attempted_epoch_id != persisted_epoch_id:
                        result["reason"] = "PERSISTED_SUBSCRIPTION_EPOCH_MISMATCH"
                    elif persisted_generation is None:
                        if not result.get("reason"):
                            result["reason"] = "PERSISTED_GENERATION_UNAVAILABLE"
                    elif persisted_generation != active_generation_after_return:
                        result["post_persist_context_current"] = False
                        if not result.get("reason"):
                            result["reason"] = (
                                "REFERENCE_CONTEXT_CHANGED_AFTER_PERSIST"
                            )
            self._record_opening_reference_failed_attempt(
                market=market,
                subscription_epoch_id=attempted_epoch_id,
                subscription_generation=attempted_generation,
                intended_bucket_utc=intended_bucket,
                result=result,
            )
            with self._orb_reference_progress_lock:
                self.last_orb_reference_results[market] = result

        def collect_completed() -> None:
            observed_monotonic = time.monotonic()
            for future, attempt in tuple(in_flight.items()):
                if not future.done():
                    if observed_monotonic < attempt.deadline_monotonic:
                        continue
                    del in_flight[future]
                    attempt.cancel_event.set()
                    timeout_key = (
                        attempt.market,
                        attempt.subscription_epoch_id,
                        attempt.subscription_generation,
                    )
                    timed_out_by_market_generation[timeout_key] = (
                        attempt.intended_bucket_utc
                    )
                    completed_at = datetime.now(timezone.utc)
                    publish_result(
                        attempt,
                        {
                            "recorded": False,
                            "reason": "REFERENCE_ATTEMPT_DEADLINE_EXCEEDED",
                            "progress_eligible": False,
                            "intended_sample_timestamp_utc": (
                                attempt.intended_bucket_utc.isoformat()
                            ),
                            "attempt_started_at_utc": (
                                attempt.started_at_utc.isoformat()
                            ),
                            "attempt_completed_at_utc": completed_at.isoformat(),
                            "attempt_duration_seconds": max(
                                0.0,
                                (completed_at - attempt.started_at_utc).total_seconds(),
                            ),
                            "attempt_deadline_utc": attempt.deadline_utc.isoformat(),
                            "attempt_deadline_seconds": max(
                                0.0,
                                (
                                    attempt.deadline_utc - attempt.started_at_utc
                                ).total_seconds(),
                            ),
                        },
                    )
                    continue
                del in_flight[future]
                try:
                    result = dict(future.result())
                except Exception as exc:
                    result = {"recorded": False, "reason": "SAMPLER_EXCEPTION"}
                    logger.exception(
                        "ORB reference sampler result error for %s: %s",
                        attempt.market,
                        exc,
                    )
                publish_result(attempt, result)

        try:
            while self.is_running:
                try:
                    collect_completed()
                    observed = datetime.now(timezone.utc)
                    if (
                        live_subscription_window(observed).get("state")
                        == "regular_session"
                    ):
                        outstanding_markets = {
                            attempt.market for attempt in in_flight.values()
                        }
                        # Optional-family rollback mutates self.symbols under this
                        # lock. Keep dispatch inside the same short critical section
                        # so every submission is ordered before or after rollback.
                        # Runtime identity is guarded by the publication lock.
                        # Keep the global lock order used by snapshot/recheck:
                        # publication -> universe -> quote.
                        with (
                            self._prediction_publication_lock,
                            self._universe_index_lock,
                        ):
                            # The lock may have been contended by an index rebuild
                            # or staged-universe transition. Re-read both clocks and
                            # runtime identity after acquiring it; otherwise the
                            # pre-lock wall time shortens the UTC deadline even
                            # though the monotonic deadline starts only now.
                            dispatch_observed = datetime.now(timezone.utc)
                            if (
                                live_subscription_window(dispatch_observed).get(
                                    "state"
                                )
                                == "regular_session"
                            ):
                                intended_bucket = self._orb_reference_bucket(
                                    dispatch_observed
                                )
                                attempted_epoch_id = self.subscription_epoch_id
                                attempted_generation = int(self.active_generation)
                                scheduled_markets = tuple(dict.fromkeys(self.symbols))
                                for market in scheduled_markets:
                                    completion_key = (
                                        market,
                                        attempted_epoch_id,
                                        attempted_generation,
                                    )
                                    if (
                                        completed_by_market_generation.get(
                                            completion_key
                                        )
                                        == intended_bucket
                                        or timed_out_by_market_generation.get(
                                            completion_key
                                        )
                                        == intended_bucket
                                        or market in outstanding_markets
                                    ):
                                        continue
                                    bucket_end = intended_bucket + timedelta(
                                        seconds=ORB_REFERENCE_INTERVAL_SECONDS
                                    )
                                    deadline_seconds = min(
                                        ORB_REFERENCE_ATTEMPT_TIMEOUT_SECONDS,
                                        max(
                                            0.0,
                                            (
                                                bucket_end - dispatch_observed
                                            ).total_seconds(),
                                        ),
                                    )
                                    if deadline_seconds <= 0.0:
                                        continue
                                    attempt = _OrbReferenceAttempt(
                                        market=market,
                                        subscription_epoch_id=attempted_epoch_id,
                                        subscription_generation=attempted_generation,
                                        intended_bucket_utc=intended_bucket,
                                        started_at_utc=dispatch_observed,
                                        deadline_utc=dispatch_observed
                                        + timedelta(seconds=deadline_seconds),
                                        deadline_monotonic=time.monotonic()
                                        + deadline_seconds,
                                        cancel_event=threading.Event(),
                                    )
                                    future = executor.submit(
                                        self._capture_opening_reference_for_bucket,
                                        market,
                                        intended_bucket,
                                        attempt_deadline_utc=attempt.deadline_utc,
                                        attempt_deadline_monotonic=(
                                            attempt.deadline_monotonic
                                        ),
                                        attempt_cancel_event=attempt.cancel_event,
                                        expected_subscription_epoch_id=(
                                            attempt.subscription_epoch_id
                                        ),
                                        expected_subscription_generation=(
                                            attempt.subscription_generation
                                        ),
                                    )
                                    in_flight[future] = attempt
                except Exception as exc:
                    logger.exception("ORB reference sampler error: %s", exc)
                self._stop_event.wait(
                    timeout=min(1.0, ORB_REFERENCE_INTERVAL_SECONDS)
                )
                collect_completed()
        finally:
            for attempt in in_flight.values():
                attempt.cancel_event.set()
            for future in in_flight:
                future.cancel()
            # Python cannot kill a running worker. Cooperative cancellation and
            # the pre-persist guard prevent late writes; do not let one blocked
            # calculation make the sampler thread's shutdown unbounded.
            executor.shutdown(wait=False, cancel_futures=True)

    def _run_live(self) -> None:
        assert self.api_key is not None
        reconnect_delay = RECONNECT_BASE_DELAY_SECONDS
        while self.is_running:
            live_client: db.Live | None = None
            live_iterator = None
            subscription_established = False
            try:
                subscription_window = self._subscription_window()
                if not bool(subscription_window.get("subscription_allowed")):
                    reconnect_delay = RECONNECT_BASE_DELAY_SECONDS
                    self._close_connection_limit_circuit()
                    self._pause_outside_subscription_window(subscription_window)
                    continue
                self._last_off_hours_log_state = None
                with self._client_lock:
                    pending_watchdog_reason = self._watchdog_stop_reason
                    self._watchdog_stop_reason = None
                rollover_requested = pending_watchdog_reason == (
                    "Databento watchdog forced reconnect (trading date rollover)"
                )
                refresh_requested = pending_watchdog_reason == (
                    "Databento watchdog forced reconnect (universe refresh)"
                )
                if rollover_requested:
                    # Permit the bounded prior-cache startup path for the new
                    # date; live quotes still gate every calculation.
                    self._universe_built_monotonic = 0.0
                self._build_universe(force_refresh=refresh_requested)
                initial_subscription_symbols = (
                    self._prepare_live_subscription_stages()
                )
                with self._fresh_quote_lock:
                    self.id_to_symbol.clear()
                    self.symbol_mappings.clear()
                self.handoff_status = "subscribing"
                self.handoff_reason = None
                self.handoff_started_monotonic = time.monotonic()
                self.latest_pins.clear()
                self.latest_invalid.clear()
                next_generation = self.active_generation + 1
                # Receive-lag evidence is connection-local. A corrected system
                # clock must be able to recover on the next subscription rather
                # than inheriting a poisoned window from an older connection.
                self._reset_stream_telemetry()
                live_client = db.Live(
                    key=self.api_key,
                    heartbeat_interval_s=5,
                    reconnect_policy="none",
                    slow_reader_behavior="skip",
                )
                sdk_client_requires_guard = isinstance(
                    live_client, _DATABENTO_SDK_LIVE_CLASS
                )
                streamer_reference = weakref.ref(self)

                def record_pre_auth_transport_event(
                    outcome: str, reason: str
                ) -> None:
                    observed_streamer = streamer_reference()
                    if observed_streamer is not None:
                        observed_streamer._record_pre_auth_transport_event(
                            outcome, reason
                        )

                try:
                    pre_auth_guard_installed = (
                        _install_pre_auth_transport_guard(
                            live_client,
                            on_event=record_pre_auth_transport_event,
                        )
                    )
                except Exception as exc:
                    pre_auth_guard_installed = False
                    logger.warning(
                        "Databento pre-auth transport guard installation "
                        "failed error_type=%s",
                        type(exc).__name__,
                    )
                with self._connection_lifecycle_lock:
                    if pre_auth_guard_installed:
                        self.pre_auth_transport_guard_status = "installed"
                    elif sdk_client_requires_guard:
                        self.pre_auth_transport_guard_status = "unavailable"
                    else:
                        self.pre_auth_transport_guard_status = (
                            "not_applicable_test_double"
                        )
                if sdk_client_requires_guard and not pre_auth_guard_installed:
                    # The SDK constructor is connection-free.  Refuse to call
                    # subscribe() when its private protocol hook has drifted so
                    # an upgrade cannot silently reintroduce unreachable TCP
                    # transports.
                    raise RuntimeError(
                        "DATABENTO_PRE_AUTH_TRANSPORT_GUARD_UNAVAILABLE"
                    )
                with self._client_lock:
                    self.client = live_client
                replay_start = self._subscription_replay_start()
                subscription_kwargs = dict(
                    dataset=DATASET,
                    schema=self.schema,
                    symbols=initial_subscription_symbols,
                    stype_in="raw_symbol",
                )
                if replay_start is not None:
                    subscription_kwargs["start"] = replay_start
                live_client.subscribe(**subscription_kwargs)
                subscription_established = True
                # Provider acceptance closes only the quota circuit. Ordinary
                # reconnect delay still requires sustained accepted quotes.
                self._close_connection_limit_circuit()
                self._record_subscription_started(replay_start)
                self.last_error = None
                self._mark_stream_progress()
                healthy_progress_started_monotonic: float | None = None
                healthy_progress_start_messages = self.messages_received
                self.active_generation = next_generation
                self._activate_initial_subscription_stage(next_generation)
                self.subscription_cutoff_monotonic = time.monotonic()
                self._reset_fresh_quote_index()
                self._fresh_markets_seen.clear()
                self.handoff_status = "warming"
                logger.info(
                    "Databento Live subscribed to initial stage %s/%s OPRA "
                    "symbols (%s)",
                    len(initial_subscription_symbols),
                    len(self.live_symbols),
                    self.schema,
                )

                # Keep the iterator alive through explicit client cleanup. If a
                # record-processing exception unwinds a temporary iterator,
                # Databento's destructor terminates immediately and removes the
                # protocol state needed by block_for_close().
                live_iterator = iter(live_client)
                for record in live_iterator:
                    if not self.is_running:
                        break
                    class_name = type(record).__name__
                    if class_name == "SymbolMappingMsg":
                        symbol = getattr(record, "stype_out_symbol", None) or getattr(record, "stype_in_symbol", None)
                        if symbol:
                            instrument_id = int(record.instrument_id)
                            raw_symbol = str(symbol)
                            mapping = self._mapping_record(record, raw_symbol)
                            with self._fresh_quote_lock:
                                self.id_to_symbol[instrument_id] = raw_symbol
                                self.symbol_mappings[instrument_id] = mapping
                        if self._stream_stalled():
                            raise RuntimeError("Databento stream stalled (no quote progress); reconnecting")
                        continue
                    if hasattr(record, "bid_px_00") and hasattr(record, "ask_px_00"):
                        self.quote_records_seen += 1
                        instrument_id = int(record.instrument_id)
                        with self._fresh_quote_lock:
                            symbol = self.id_to_symbol.get(instrument_id)
                            mapping = dict(
                                self.symbol_mappings.get(instrument_id) or {}
                            )
                        if not symbol:
                            self.unmapped_quote_records += 1
                            continue
                        bid = normalize_price(getattr(record, "bid_px_00", None))
                        ask = normalize_price(getattr(record, "ask_px_00", None))
                        if bid is None or ask is None:
                            self.invalid_quote_records += 1
                            continue
                        if ask < bid:
                            self.crossed_quote_records += 1
                            continue
                        processed_monotonic = time.monotonic()
                        processed_at_ns = time.time_ns()
                        ts_event_ns = _record_timestamp_ns(record, "ts_event")
                        ts_recv_ns = _record_timestamp_ns(record, "ts_recv")
                        ts_index_ns = _record_timestamp_ns(record, "ts_index")
                        if ts_event_ns is None or ts_recv_ns is None or ts_index_ns is None:
                            self.provider_timestamp_missing_records += 1
                        timestamp_order_valid = (
                            ts_event_ns <= ts_recv_ns
                            if ts_event_ns is not None and ts_recv_ns is not None
                            else None
                        )
                        if timestamp_order_valid is False:
                            self.provider_timestamp_order_errors += 1
                        receive_lag = (
                            (processed_at_ns - ts_recv_ns) / 1_000_000_000
                            if ts_recv_ns is not None
                            else None
                        )
                        if receive_lag is not None and receive_lag < 0:
                            self.negative_receive_lag_records += 1
                        if receive_lag is not None:
                            self._receive_lag_window.append(receive_lag)
                            if receive_lag < -CLOCK_SYNC_NEGATIVE_TOLERANCE_SECONDS:
                                self.material_negative_receive_lag_records += 1
                        self.last_receive_to_process_lag_seconds = receive_lag
                        if receive_lag is not None:
                            self.max_receive_to_process_lag_seconds = max(
                                receive_lag,
                                self.max_receive_to_process_lag_seconds
                                if self.max_receive_to_process_lag_seconds is not None
                                else receive_lag,
                            )
                        quote_payload = {
                            "bid": bid,
                            "ask": ask,
                            "mid": (bid + ask) / 2.0,
                            "received_monotonic": processed_monotonic,
                            "generation": next_generation,
                            "instrument_id": instrument_id,
                            "ts_event_ns": ts_event_ns,
                            "ts_recv_ns": ts_recv_ns,
                            "ts_index_ns": ts_index_ns,
                            "processed_at_ns": processed_at_ns,
                            "receive_to_process_lag_seconds": receive_lag,
                            "provider_timestamp_order_valid": timestamp_order_valid,
                            "mapping_version": mapping.get("mapping_version"),
                            "mapping_ts_event_ns": mapping.get("ts_event_ns"),
                            "mapping_start_ts_ns": mapping.get("start_ts_ns"),
                            "mapping_end_ts_ns": mapping.get("end_ts_ns"),
                        }
                        # Publish only if the mapping used above is still the
                        # active mapping. The reentrant lock makes this check
                        # and quote replacement one atomic catalog operation.
                        with self._fresh_quote_lock:
                            current_symbol = self.id_to_symbol.get(instrument_id)
                            current_mapping = self.symbol_mappings.get(instrument_id) or {}
                            if (
                                current_symbol != symbol
                                or current_mapping.get("mapping_version")
                                != mapping.get("mapping_version")
                            ):
                                self.unmapped_quote_records += 1
                                continue
                            self._store_quote(symbol, quote_payload)
                        self.messages_received += 1
                        if healthy_progress_started_monotonic is None:
                            # A subscription acknowledgement is not transport
                            # health. Anchor on the first accepted quote, then
                            # require another accepted quote after the full
                            # interval before forgiving prior rapid failures.
                            healthy_progress_started_monotonic = processed_monotonic
                            healthy_progress_start_messages = self.messages_received
                        elif (
                            reconnect_delay > RECONNECT_BASE_DELAY_SECONDS
                            and self.messages_received > healthy_progress_start_messages
                            and processed_monotonic - healthy_progress_started_monotonic
                            >= RECONNECT_HEALTHY_RESET_SECONDS
                        ):
                            logger.info(
                                "Databento reconnect backoff reset after %.1fs of "
                                "sustained quote progress (%s accepted messages)",
                                processed_monotonic - healthy_progress_started_monotonic,
                                self.messages_received - healthy_progress_start_messages + 1,
                            )
                            reconnect_delay = RECONNECT_BASE_DELAY_SECONDS
                        self._mark_stream_progress()
                        # A full fresh-count scan walks every cached quote. Doing
                        # that for every OPRA record made the consumer a slow
                        # reader and allowed Databento's queue to overflow. This
                        # record is fresh by construction, so track only which
                        # requested market roots have appeared in this generation.
                        market_name = self._market_for_raw_symbol(symbol)
                        if market_name:
                            self._record_fresh_market_and_maybe_activate(
                                market_name, generation=int(quote_payload["generation"]),
                                received_monotonic=processed_monotonic,
                            )
                    if self._stream_stalled():
                        raise RuntimeError("Databento stream stalled (no quote progress); reconnecting")

                if self.is_running:
                    subscription_window = self._subscription_window()
                    if bool(subscription_window.get("subscription_allowed")):
                        raise RuntimeError(
                            "Databento live stream ended during active subscription window"
                        )

            except Exception as exc:
                with self._client_lock:
                    watchdog_reason = self._watchdog_stop_reason
                failure_reason = watchdog_reason or str(exc)
                self._set_subscription_stage_terminal(
                    "canceled", "RECONNECT_CANCELS_DEFERRED_STAGE"
                )
                connection_limit_rejected = bool(
                    self._is_open_connection_limit_error(exc)
                    or self._is_open_connection_limit_error(failure_reason)
                )
                # Cleanup precedes every reconnect/off-hours wait. In
                # particular, an exception that races the closing bell must
                # not leave the subscribed SDK session alive while the stream
                # thread parks outside the subscription window.
                with self._client_lock:
                    if self.client is live_client:
                        self.client = None
                if live_client is not None:
                    self._close_live_client(
                        live_client,
                        subscription_established=subscription_established,
                    )
                    # The retained iterator may now be released without letting
                    # its destructor race the SDK close barrier.
                    live_iterator = None
                    live_client = None
                if not self.is_running:
                    continue
                subscription_window = self._subscription_window()
                if not bool(subscription_window.get("subscription_allowed")):
                    reconnect_delay = RECONNECT_BASE_DELAY_SECONDS
                    self._pause_outside_subscription_window(
                        subscription_window,
                        transport_reason=failure_reason,
                    )
                    continue
                self.last_error = failure_reason
                with self._prediction_publication_lock:
                    self.handoff_status = "degraded"
                    self.handoff_reason = failure_reason
                if not watchdog_reason:
                    self._record_reconnect(failure_reason)
                logger.error("Databento live stream error: %s", failure_reason)
                if self.is_running:
                    if connection_limit_rejected:
                        cooldown_seconds = self._open_connection_limit_circuit()
                        logger.warning(
                            "Databento open-connection-limit circuit open; "
                            "one probe is eligible in %.1fs",
                            cooldown_seconds,
                        )
                        if not self._wait_for_connection_limit_probe(
                            cooldown_seconds
                        ):
                            continue
                    else:
                        quota_probe_cooldown = (
                            self._reopen_connection_limit_circuit_after_probe_failure()
                        )
                        if quota_probe_cooldown is not None:
                            logger.warning(
                                "Databento open-connection-limit half-open probe "
                                "was inconclusive; one probe is eligible in %.1fs",
                                quota_probe_cooldown,
                            )
                            if not self._wait_for_connection_limit_probe(
                                quota_probe_cooldown
                            ):
                                continue
                        else:
                            logger.warning(
                                "Databento stream reconnecting in %.1fs",
                                reconnect_delay,
                            )
                            time.sleep(reconnect_delay)
                        reconnect_delay = min(
                            reconnect_delay * 2.0,
                            RECONNECT_MAX_DELAY_SECONDS,
                        )
            finally:
                with self._client_lock:
                    if self.client is live_client:
                        self.client = None
                if live_client is not None:
                    self._close_live_client(
                        live_client,
                        subscription_established=subscription_established,
                    )
                live_iterator = None

    def _watchdog_loop(self) -> None:
        while self.is_running:
            try:
                with self._client_lock:
                    observed_client = self.client
                self._maybe_promote_deferred_subscription_stage()
                self._confirm_optional_family_canary_rollback()
                if self.handoff_status == "active":
                    canary_decision = self._optional_family_canary_decision()
                    self._apply_optional_family_canary_rollback(canary_decision)
                stalled = self._stream_stalled()
                refresh_due = (
                    ALLOW_LIVE_UNIVERSE_REFRESH_RECONNECT
                    and self._universe_refresh_due()
                )
                trading_date_rollover = self._universe_trading_date_changed()
                client_to_stop: db.Live | None = None
                reconnect_reason: str | None = None
                with self._client_lock:
                    if (
                        observed_client is not None
                        and self.client is observed_client
                        # ``db.Live.subscribe`` and symbol mapping setup have a
                        # brief window where a new client exists but progress
                        # still belongs to the old connection. Never interrupt
                        # that in-flight handoff.
                        and self.handoff_status in {"warming", "active"}
                        and self._watchdog_stop_reason is None
                        and (stalled or refresh_due or trading_date_rollover)
                    ):
                        if trading_date_rollover:
                            reason = "trading date rollover"
                        elif refresh_due:
                            reason = "universe refresh"
                        else:
                            reason = "stalled quote progress"
                        with self._prediction_publication_lock:
                            self.handoff_status = (
                                "handoff"
                                if reason in {"universe refresh", "trading date rollover"}
                                else "degraded"
                            )
                            self.handoff_started_monotonic = time.monotonic()
                            self.handoff_reason = reason
                        reconnect_reason = f"Databento watchdog forced reconnect ({reason})"
                        self.last_reconnect_reason = reconnect_reason
                        self._watchdog_stop_reason = reconnect_reason
                        self._record_reconnect(reconnect_reason)
                        self.last_error = reconnect_reason
                        client_to_stop = observed_client
                if client_to_stop is not None and reconnect_reason is not None:
                    logger.warning(reconnect_reason)
                    try:
                        client_to_stop.stop()
                    except Exception:
                        # If stop itself fails, permit a later watchdog attempt
                        # instead of permanently latching a dead reason.
                        with self._client_lock:
                            if (
                                self.client is client_to_stop
                                and self._watchdog_stop_reason == reconnect_reason
                            ):
                                self._watchdog_stop_reason = None
                        raise
            except Exception as exc:
                logger.debug("Databento watchdog check failed: %s", exc)
            time.sleep(1.0)

    def _optional_family_canary_observations(self) -> dict[str, dict[str, object]]:
        observations: dict[str, dict[str, object]] = {}
        for family in ("SPX", "NDX", "RUT"):
            payload = self.latest_pins.get(family) or self.latest_invalid.get(family)
            updated = self.last_message_time.get(family)
            if payload is None and updated is None:
                continue
            observations[family] = {
                "validation_is_valid": family in self.latest_pins,
                "data_age_seconds": max(0.0, (_utcnow_naive() - updated).total_seconds()) if updated else None,
                "receive_to_process_lag_p95_seconds": (payload or {}).get("receive_to_process_lag_p95_seconds"),
                "primary_pair_coverage_ratio": (payload or {}).get("primary_pair_coverage_ratio"),
                "fresh_quote_count": self._fresh_quote_counts_by_market.get(family),
                "expected_quote_count": int(
                    ((self.subscription_metadata.get("markets") or {}).get(family) or {}).get(
                        "selected_contract_count"
                    ) or 0
                ),
            }
        return observations

    @staticmethod
    def _canary_window_timestamp(
        window: Mapping[str, object], key: str
    ) -> datetime | None:
        raw_value = window.get(key)
        if not raw_value:
            return None
        try:
            value = datetime.fromisoformat(str(raw_value).replace("Z", "+00:00"))
        except ValueError:
            return None
        if value.tzinfo is None:
            return None
        return value.astimezone(timezone.utc)

    def _rut_orb_reference_progress(
        self, observed_at_utc: datetime
    ) -> dict[str, object]:
        """Describe independent RUT reference progress in this generation."""
        with self._orb_reference_progress_lock:
            recent = tuple(
                self._orb_reference_recent_buckets_by_market.get("RUT") or ()
            )
        buckets = sorted(
            {
                bucket.astimezone(timezone.utc)
                for bucket, generation, epoch_id in recent
                if int(generation) == int(self.active_generation)
                and str(epoch_id) == self.subscription_epoch_id
            }
        )
        latest = buckets[-1] if buckets else None
        previous = buckets[-2] if len(buckets) >= 2 else None
        max_gap_seconds = max(15.0, float(ORB_REFERENCE_INTERVAL_SECONDS) * 3.0)
        latest_age_seconds = (
            (observed_at_utc - latest).total_seconds() if latest is not None else None
        )
        gap_seconds = (
            (latest - previous).total_seconds()
            if latest is not None and previous is not None
            else None
        )
        advancing = bool(
            latest_age_seconds is not None
            and 0.0 <= latest_age_seconds <= max_gap_seconds
            and gap_seconds is not None
            and 0.0 < gap_seconds <= max_gap_seconds
        )
        return {
            "advancing": advancing,
            "current_generation_bucket_count": len(buckets),
            "latest_bucket_utc": latest.isoformat() if latest else None,
            "latest_bucket_age_seconds": latest_age_seconds,
            "latest_interbucket_gap_seconds": gap_seconds,
            "maximum_allowed_gap_seconds": max_gap_seconds,
        }

    def _optional_family_canary_warmup_remaining(
        self, window: Mapping[str, object] | None = None
    ) -> float:
        """Give a regular-session generation time to calculate before judgment."""
        if (
            not self.optional_family_canary.enabled
            or "RUT" not in self.symbols
            or self.handoff_status != "active"
        ):
            return 0.0
        current_window = window or self._subscription_window()
        if str(current_window.get("state") or "unknown") != "regular_session":
            # A preopen handoff is useful for quote warmup, but it must not
            # consume the canary's calculation grace or latch a RUT rollback.
            self._optional_family_canary_generation = self.active_generation
            self._optional_family_canary_active_since_monotonic = 0.0
            return self.optional_family_canary_warmup_seconds
        if (
            self._optional_family_canary_generation != self.active_generation
            or self._optional_family_canary_active_since_monotonic <= 0.0
        ):
            self._optional_family_canary_generation = self.active_generation
            self._optional_family_canary_active_since_monotonic = time.monotonic()
        elapsed = time.monotonic() - self._optional_family_canary_active_since_monotonic
        return max(0.0, self.optional_family_canary_warmup_seconds - elapsed)

    def _optional_family_canary_decision(
        self, window: Mapping[str, object] | None = None
    ) -> CanaryDecision:
        """Evaluate the RUT canary without sacrificing truthful opening capture."""
        current_window = dict(window or self._subscription_window())
        session_state = str(current_window.get("state") or "unknown")
        observed_at = self._canary_window_timestamp(
            current_window, "observed_at_utc"
        ) or datetime.now(timezone.utc)
        status = self.optional_family_canary.status()
        rut_progress = self._rut_orb_reference_progress(observed_at)
        evidence: dict[str, object] = {
            "evaluation_deferred": False,
            "opening_orb_protection_active": False,
            "cash_session_reconnect_protection_active": False,
            "session_state": session_state,
            "observed_at_utc": observed_at.isoformat(),
            "reasons": list(status.reasons),
            "rut_orb_reference_progress": rut_progress,
            "compute_backpressure_active": self._compute_backpressure_remaining() > 0.0,
        }

        with self._subscription_stage_lock:
            opening_stage_state = self.subscription_stage_state
        if (
            "RUT" in self.symbols
            and status.state not in {"disabled", "blocked", "rolled_back"}
            and opening_stage_state
            in {
                "planned_primary",
                "primary_active",
                "promoting",
                "frozen",
                "canceled",
            }
        ):
            decision = CanaryDecision(
                "protected_all_index_orb",
                status.family,
                False,
                ("ALL_INDEX_PRIMARY_STAGE_PRESERVES_RUT",),
                status.budgets,
                status.allowed_families,
            )
            evidence.update(
                {
                    "decision_state": decision.state,
                    "evaluation_deferred": True,
                    "opening_orb_protection_active": True,
                    "reasons": list(decision.reasons),
                }
            )
            self.optional_family_canary_evaluation_evidence = evidence
            return decision

        if status.state in {"disabled", "blocked", "rolled_back"}:
            evidence["decision_state"] = status.state
            self.optional_family_canary_evaluation_evidence = evidence
            return status

        if self.handoff_status != "active":
            evidence["decision_state"] = status.state
            evidence["reasons"] = ["HANDOFF_NOT_ACTIVE"]
            self.optional_family_canary_evaluation_evidence = evidence
            return status

        if session_state != "regular_session":
            self._optional_family_canary_generation = self.active_generation
            self._optional_family_canary_active_since_monotonic = 0.0
            deferred_state = (
                "deferred_preopen"
                if session_state in {"preopen", "preopen_wait"}
                else "deferred_off_hours"
            )
            deferred_reason = (
                "PREOPEN_EVALUATION_DEFERRED"
                if deferred_state == "deferred_preopen"
                else "OFF_HOURS_EVALUATION_DEFERRED"
            )
            decision = CanaryDecision(
                deferred_state,
                status.family,
                False,
                (deferred_reason,),
                status.budgets,
                status.allowed_families,
            )
            evidence.update(
                {
                    "decision_state": decision.state,
                    "evaluation_deferred": True,
                    "reasons": list(decision.reasons),
                }
            )
            self.optional_family_canary_evaluation_evidence = evidence
            return decision

        observations = self._optional_family_canary_observations()
        preview = self.optional_family_canary.preview(observations)
        preview_reason_set = set(preview.reasons)
        overload_active = bool(evidence["compute_backpressure_active"])
        provider_integrity_risk_active = bool(
            self.provider_slow_client_warnings
            or self.provider_skipped_record_warnings
            or self.provider_skipped_records
        )
        # One queue-full warning at the opening burst suspends expensive compute,
        # so the absence of every first calculation is not yet evidence that RUT
        # harmed the core.  Honor the configured warmup for that exact transient;
        # repeated queue pressure or explicit slow/skipped-record telemetry still
        # represents measured transport risk and may roll the canary back at once.
        initial_queue_backpressure_with_no_observations = bool(
            overload_active
            and not observations
            and self.provider_queue_full_warnings == 1
            and not provider_integrity_risk_active
        )
        core_degraded = any(
            reason.startswith("CORE_") for reason in preview_reason_set
        )
        # Before the first calculation, absence is expected during warmup.
        # Established output loss retains a publication timestamp and produces
        # invalidity/freshness/lag evidence instead; those still bypass grace.
        measured_core_degraded = any(
            reason.startswith("CORE_") and not reason.startswith("CORE_MISSING:")
            for reason in preview_reason_set
        )
        measured_canary_lag = "CANARY_LAG_BUDGET:RUT" in preview_reason_set
        evidence["rut_lag_evidence_unavailable"] = (
            "CANARY_LAG_UNAVAILABLE:RUT" in preview_reason_set
        )
        evidence["rut_lag_breach_measured"] = measured_canary_lag
        evidence["provider_integrity_risk_active"] = provider_integrity_risk_active
        evidence["initial_queue_backpressure_with_no_observations"] = (
            initial_queue_backpressure_with_no_observations
        )
        warmup_remaining = self._optional_family_canary_warmup_remaining(
            current_window
        )
        if warmup_remaining > 0.0 and not (
            (overload_active and not initial_queue_backpressure_with_no_observations)
            or measured_core_degraded
            or measured_canary_lag
            or provider_integrity_risk_active
        ):
            evidence.update(
                {
                    "decision_state": status.state,
                    "reasons": ["REGULAR_SESSION_WARMUP_ACTIVE"],
                    "warmup_remaining_seconds": warmup_remaining,
                }
            )
            self.optional_family_canary_evaluation_evidence = evidence
            return status
        if warmup_remaining > 0.0:
            evidence["warmup_bypassed_for_measured_risk"] = True
            evidence["warmup_remaining_seconds"] = warmup_remaining

        cash_open = self._canary_window_timestamp(current_window, "cash_open_utc")
        first_hour = bool(
            cash_open is not None
            and cash_open <= observed_at < cash_open + timedelta(hours=1)
        )
        protective_reasons = {
            "CANARY_MISSING:RUT",
            "CANARY_INVALID:RUT",
            "CANARY_FRESHNESS_BUDGET:RUT",
            "CANARY_LAG_UNAVAILABLE:RUT",
        }
        protective_context_reasons = {
            "CANARY_MISSING:RUT",
            "CANARY_INVALID:RUT",
            "CANARY_FRESHNESS_BUDGET:RUT",
            "CANARY_LAG_UNAVAILABLE:RUT",
        }
        # During the cash session, an optional-only absence/quality problem
        # must not force a resubscription that changes the core generation and
        # makes an otherwise complete SPX/NDX ORB permanently mixed.  Measured
        # overload, a measured RUT lag breach, or any core deficiency still
        # bypasses this protection and can roll the canary back immediately.
        protect_cash_session = bool(
            preview.rollback_required
            and preview.reasons
            and preview_reason_set.issubset(protective_reasons)
            and bool(preview_reason_set.intersection(protective_context_reasons))
            and not overload_active
            and not core_degraded
            and not measured_canary_lag
            and not provider_integrity_risk_active
        )
        if protect_cash_session:
            decision = CanaryDecision(
                "protected_opening_orb" if first_hour else "protected_cash_session",
                preview.family,
                False,
                preview.reasons,
                preview.budgets,
                status.allowed_families,
            )
            evidence.update(
                {
                    "decision_state": decision.state,
                    "opening_orb_protection_active": first_hour,
                    "cash_session_reconnect_protection_active": True,
                    "first_session_hour": first_hour,
                    "reasons": list(decision.reasons),
                }
            )
            self.optional_family_canary_evaluation_evidence = evidence
            return decision

        decision = self.optional_family_canary.evaluate(observations)
        evidence.update(
            {
                "decision_state": decision.state,
                "first_session_hour": first_hour,
                "reasons": list(decision.reasons),
            }
        )
        self.optional_family_canary_evaluation_evidence = evidence
        return decision

    def _apply_optional_family_canary_rollback(self, decision) -> bool:
        """Remove only RUT and request one bounded in-process resubscription."""
        if not decision.rollback_required:
            return False
        with self._subscription_stage_lock:
            opening_stage_state = self.subscription_stage_state
        if (
            "RUT" in self.symbols
            and opening_stage_state
            in {
                "planned_primary",
                "primary_active",
                "promoting",
                "frozen",
                "canceled",
            }
        ):
            # The all-index primary stage is the authoritative ORB capture
            # surface. A canary can freeze/cancel later shadow promotion, but
            # it must remain observational here and may not remove RUT or force
            # a generation-changing reconnect before that ORB is complete.
            self.optional_family_canary_evaluation_evidence = {
                **self.optional_family_canary_evaluation_evidence,
                "mutation_suppressed_for_all_index_orb": True,
                "subscription_stage_state": opening_stage_state,
            }
            return False
        with self._optional_family_canary_lock:
            evidence = self.optional_family_rollback_evidence
            if evidence is not None:
                if (
                    evidence.get("status") == "requested"
                    and self.client
                    and int(evidence.get("stop_attempts") or 0) < 3
                ):
                    evidence["stop_attempts"] = int(evidence.get("stop_attempts") or 0) + 1
                    try:
                        self.client.stop()
                    except Exception as exc:
                        evidence["stop_errors"].append(str(exc))
                return False
            if "RUT" not in self.symbols:
                return False
            previous_symbols = tuple(self.symbols)
            with self._universe_index_lock:
                self.symbols = [
                    symbol for symbol in self.symbols if symbol != "RUT"
                ]
                self.live_symbols = [
                    raw_symbol
                    for raw_symbol in self.live_symbols
                    if self._market_for_raw_symbol(raw_symbol) != "RUT"
                ]
                if not self.universe.empty and "market" in self.universe.columns:
                    self.universe = self.universe[
                        self.universe["market"] != "RUT"
                    ].copy()
                self._index_universe_metadata_locked()
            reason = "RUT optional-family canary rollback: " + ",".join(decision.reasons)
            self.optional_family_rollback_evidence = {
                "status": "requested",
                "requested_at_utc": datetime.now(timezone.utc).isoformat(),
                "requested_generation": int(self.active_generation),
                "reasons": list(decision.reasons),
                "previous_families": list(previous_symbols),
                "remaining_families": list(self.symbols),
                "resubscribe_requested": bool(self.client),
                "stop_attempts": 0,
                "stop_errors": [],
            }
            self.handoff_status = "handoff"
            self.handoff_started_monotonic = time.monotonic()
            self.handoff_reason = reason
            self._watchdog_stop_reason = reason
            self._record_reconnect(reason)
            self.last_error = reason
            logger.warning(reason)
            if self.client:
                self.optional_family_rollback_evidence["stop_attempts"] = 1
                try:
                    self.client.stop()
                except Exception as exc:
                    self.optional_family_rollback_evidence["stop_errors"].append(str(exc))
            return True

    def _confirm_optional_family_canary_rollback(self) -> None:
        evidence = self.optional_family_rollback_evidence
        if not evidence or evidence.get("status") != "requested":
            return
        if self.handoff_status != "active" or self.active_generation <= int(evidence["requested_generation"]):
            return
        root_counts = {
            str(raw_symbol).split()[0].strip().upper() for raw_symbol in self.live_symbols
        }
        if "RUT" in self.symbols or any(root in {"RUT", "RUTW", "MRUT"} for root in root_counts):
            return
        evidence["status"] = "confirmed"
        evidence["confirmed_at_utc"] = datetime.now(timezone.utc).isoformat()
        evidence["confirmed_generation"] = int(self.active_generation)

    async def start(self):
        if self.is_running:
            return
        logger.info("Starting Databento OPRA streamer...")
        self._install_provider_warning_telemetry()
        self._stop_event.clear()
        self.is_running = True
        self.thread = threading.Thread(target=self._run_live, daemon=True)
        self.compute_thread = threading.Thread(target=self._compute_loop, daemon=True)
        self.watchdog_thread = threading.Thread(target=self._watchdog_loop, daemon=True)
        self.orb_reference_thread = threading.Thread(
            target=self._opening_reference_loop,
            daemon=True,
        )
        self.thread.start()
        self.compute_thread.start()
        self.watchdog_thread.start()
        self.orb_reference_thread.start()

    async def stop(self):
        # Publish the terminal handoff state before any potentially blocking
        # client/thread joins. Live readers bind to this state and must stop
        # treating retained prices or pins as current immediately.
        self._stop_event.set()
        with self._prediction_publication_lock, self._fresh_quote_lock:
            self.handoff_status = "stopped"
            self.handoff_reason = "streamer stopped"
            self.is_running = False
        # The quote lock is deliberately released before this wait: capture
        # takes the barrier first and may then verify universe/quote identity.
        # Wait only a bounded interval for a save already in progress. Workers
        # reaching the barrier later observe the stop request and fail closed.
        persist_barrier_acquired = self._orb_reference_persist_barrier.acquire(
            timeout=ORB_REFERENCE_SHUTDOWN_BARRIER_TIMEOUT_SECONDS
        )
        if persist_barrier_acquired:
            self._orb_reference_persist_barrier.release()
        else:
            logger.error(
                "Timed out waiting %.2fs for ORB reference persistence during shutdown",
                ORB_REFERENCE_SHUTDOWN_BARRIER_TIMEOUT_SECONDS,
            )
        with self._client_lock:
            live_client = self.client
        if live_client:
            live_client.stop()
        if self.thread and self.thread.is_alive():
            self.thread.join(timeout=2)
        if self.compute_thread and self.compute_thread.is_alive():
            self.compute_thread.join(timeout=2)
        if self.watchdog_thread and self.watchdog_thread.is_alive():
            self.watchdog_thread.join(timeout=2)
        if self.orb_reference_thread and self.orb_reference_thread.is_alive():
            self.orb_reference_thread.join(timeout=2)
        self._remove_provider_warning_telemetry()

    def get_latest_data(self, symbol: str, n: int = 1) -> list:
        symbol = symbol.upper()
        if symbol not in self.buffers:
            return []
        buffer = self.buffers[symbol]
        return list(buffer)[-n:] if n < len(buffer) else list(buffer)

    def get_all_latest(self) -> dict[str, dict]:
        return {symbol: values[-1] for symbol, values in self.buffers.items() if values}

    def get_latest_pin(self, symbol: str) -> dict | None:
        return self.latest_pins.get(symbol.upper()) if self.handoff_status == "active" else None

    @property
    def active_generation(self) -> int:
        lock = getattr(self, "_prediction_publication_lock", None)
        if lock is None:
            return int(getattr(self, "_active_generation", 0))
        with lock:
            return int(getattr(self, "_active_generation", 0))

    @active_generation.setter
    def active_generation(self, value: int) -> None:
        lock = getattr(self, "_prediction_publication_lock", None)
        if lock is None:
            self._active_generation = int(value)
            return
        with lock:
            self._active_generation = int(value)

    @property
    def handoff_status(self) -> str:
        lock = getattr(self, "_prediction_publication_lock", None)
        if lock is None:
            return str(getattr(self, "_handoff_status", ""))
        with lock:
            return str(getattr(self, "_handoff_status", ""))

    @handoff_status.setter
    def handoff_status(self, value: str) -> None:
        lock = getattr(self, "_prediction_publication_lock", None)
        if lock is None:
            self._handoff_status = str(value)
            return
        with lock:
            self._handoff_status = str(value)

    @contextmanager
    def prediction_publication_guard(
        self,
        *,
        subscription_epoch_id: str,
        subscription_generation: int,
    ):
        """Hold runtime identity stable through forecast persistence."""

        with self._prediction_publication_lock:
            allowed = bool(
                str(subscription_epoch_id) == str(self.subscription_epoch_id)
                and int(subscription_generation) == int(self.active_generation)
                and int(subscription_generation) > 0
                and self.handoff_status == "active"
            )
            yield allowed

    def get_subscription_context(self) -> dict[str, object]:
        """Return one coherent process/generation identity for live API binding."""

        with self._prediction_publication_lock:
            return {
                "subscription_epoch_id": self.subscription_epoch_id,
                "subscription_generation": int(self.active_generation),
                "handoff_status": str(self.handoff_status),
            }

    def get_health(self) -> dict:
        self._confirm_optional_family_canary_rollback()
        subscription_window = self._subscription_window()
        subscription_allowed = bool(
            subscription_window.get("subscription_allowed")
        )
        latest_update = max(self.last_message_time.values()) if self.last_message_time else None
        latest_diagnostic = max(self.last_diagnostic_time.values()) if self.last_diagnostic_time else None
        data_age_seconds = (_utcnow_naive() - latest_update).total_seconds() if latest_update else None
        progress_age_seconds = None
        if self._last_progress_monotonic:
            progress_age_seconds = max(0.0, time.monotonic() - self._last_progress_monotonic)
        stream_progressing = bool(
            self.is_running
            and subscription_allowed
            and progress_age_seconds is not None
            and progress_age_seconds <= STREAM_PROGRESS_WINDOW_SECONDS
        )
        fresh_quote_counts = self._fresh_quote_counts()
        with self._prediction_publication_lock:
            if (
                self.handoff_status == "warming"
                and self.handoff_started_monotonic
                and subscription_window.get("state") == "regular_session"
            ):
                # A preopen subscription can legitimately wait for the first cash
                # quotes. Charge its timeout only after that session actually opens.
                cash_open = self._canary_window_timestamp(subscription_window, "cash_open_utc")
                observed_at = self._canary_window_timestamp(subscription_window, "observed_at_utc")
                handoff_elapsed = None
                if cash_open is not None and observed_at is not None:
                    handoff_elapsed = min(
                        time.monotonic() - self.handoff_started_monotonic,
                        max(0.0, (observed_at - cash_open).total_seconds()),
                    )
                if handoff_elapsed is not None and handoff_elapsed > HANDOFF_TIMEOUT_SECONDS:
                    self.handoff_status = "degraded"
                    self.handoff_reason = "Fresh post-refresh quotes did not arrive before timeout"
        # This block reads runtime identity and retained result dictionaries,
        # not quotes. Never retain the quote lock while waiting for publication.
        with self._prediction_publication_lock:
            active_epoch_id = self.subscription_epoch_id
            active_generation = int(self.active_generation)
            active_handoff_status = str(self.handoff_status)
            retained_pins = dict(self.latest_pins)
            retained_invalid = dict(self.latest_invalid)

        def current_process_payload(payload: object) -> bool:
            if not isinstance(payload, Mapping):
                return False
            try:
                payload_generation = int(payload.get("subscription_generation"))
            except (TypeError, ValueError, OverflowError):
                return False
            return bool(
                active_handoff_status == "active"
                and payload.get("subscription_epoch_id") == active_epoch_id
                and payload_generation == active_generation
                and active_generation > 0
            )

        valid_symbols = sorted(
            symbol
            for symbol, payload in retained_pins.items()
            if current_process_payload(payload)
        )
        invalid_symbols = sorted(
            symbol
            for symbol, payload in retained_invalid.items()
            if symbol not in valid_symbols and current_process_payload(payload)
        )
        invalid_reasons = {
            symbol: (retained_invalid.get(symbol, {}).get("validation_failure_reasons") or [retained_invalid.get(symbol, {}).get("pregate_reason") or "Unavailable"])[0]
            for symbol in invalid_symbols
        }

        subscription_stage_health = self._subscription_stage_health()
        with self._subscription_stage_lock:
            active_live_symbols = list(self.active_live_symbols)
            deferred_live_symbols = list(self.deferred_live_symbols)
        active_market_counts: dict[str, int] = {}
        deferred_market_counts: dict[str, int] = {}
        for raw_symbol in active_live_symbols:
            market = self._market_for_raw_symbol(raw_symbol)
            if market:
                active_market_counts[market] = active_market_counts.get(market, 0) + 1
        for raw_symbol in deferred_live_symbols:
            market = self._market_for_raw_symbol(raw_symbol)
            if market:
                deferred_market_counts[market] = deferred_market_counts.get(market, 0) + 1

        root_counts: dict[str, int] = {}
        for raw_symbol in active_live_symbols:
            root = str(raw_symbol).split()[0].strip().upper()
            if not root:
                continue
            root_counts[root] = root_counts.get(root, 0) + 1

        core_root_aliases = {
            "SPX": ("SPXW", "SPX", "XSP"),
            "NDX": ("NDXP", "NDX", "XND"),
            "SPY": ("SPY",),
            "QQQ": ("QQQ",),
            "DIA": ("DIA",),
            "IWM": ("IWM",),
            "VIX": ("VIXW", "VIX"),
            "RUT": ("RUTW", "RUT", "MRUT"),
        }
        core_symbol_status = {
            core: {
                "requested": core in self.symbols,
                "contracts_subscribed": int(sum(root_counts.get(alias, 0) for alias in aliases)),
                "aliases": list(aliases),
            }
            for core, aliases in core_root_aliases.items()
        }
        market_plans = self.subscription_metadata.get("markets") or {}
        market_subscription_status = {
            market: {
                "requested": market in self.symbols,
                "selected_contract_count": int(
                    (market_plans.get(market) or {}).get("selected_contract_count") or 0
                ),
                "active_contract_count": int(active_market_counts.get(market, 0)),
                "deferred_contract_count": int(
                    deferred_market_counts.get(market, 0)
                ),
                "full_contract_count": int(
                    (market_plans.get(market) or {}).get("full_contract_count") or 0
                ),
                "selected_expirations": (
                    (market_plans.get(market) or {}).get("selected_expirations") or []
                ),
                "primary_expiration_authority": (
                    (market_plans.get(market) or {}).get(
                        "primary_expiration_authority"
                    )
                ),
                "primary_expiration_context_only": bool(
                    (market_plans.get(market) or {}).get(
                        "primary_expiration_context_only"
                    )
                ),
                "primary_expiration_same_day_authority": bool(
                    (market_plans.get(market) or {}).get(
                        "primary_expiration_same_day_authority"
                    )
                ),
                "primary_expiration_selection_basis": (
                    (market_plans.get(market) or {}).get(
                        "primary_expiration_selection_basis"
                    )
                ),
                "settlement_ineligible_contract_count": int(
                    (market_plans.get(market) or {}).get(
                        "settlement_ineligible_contract_count"
                    )
                    or 0
                ),
                "settlement_ineligible_expirations": (
                    (market_plans.get(market) or {}).get(
                        "settlement_ineligible_expirations"
                    )
                    or []
                ),
                "primary_expiration_unavailable_reason": (
                    (market_plans.get(market) or {}).get(
                        "primary_expiration_unavailable_reason"
                    )
                ),
                "primary_pair_admission": (
                    (market_plans.get(market) or {}).get(
                        "primary_pair_admission"
                    )
                    or self.primary_pair_admission_diagnostics.get(market)
                    or {}
                ),
                "required_for_universe_admission": bool(
                    (market_plans.get(market) or {}).get(
                        "required_for_universe_admission"
                    )
                ),
                "subscription_available": bool(
                    (market_plans.get(market) or {}).get(
                        "subscription_available"
                    )
                ),
                "optional_family_unavailable_reason": (
                    (market_plans.get(market) or {}).get(
                        "optional_family_unavailable_reason"
                    )
                ),
            }
            for market in sorted(set(self.symbols) | set(market_plans))
        }
        universe_provenance = self.subscription_metadata.get("universe_provenance") or {}
        active_cache_file = universe_provenance.get("source_path") or str(self._cache_path())
        universe_refresh_interval = (
            UNIVERSE_FALLBACK_REFRESH_SECONDS
            if universe_provenance.get("is_fallback")
            else UNIVERSE_REFRESH_SECONDS
        )
        universe_age_seconds = (
            max(0.0, time.monotonic() - self._universe_built_monotonic)
            if self._universe_built_monotonic
            else None
        )
        open_grace_remaining = max(
            0.0, self._open_grace_until_monotonic - time.monotonic()
        )
        compute_backpressure_remaining = self._compute_backpressure_remaining()
        connection_lifecycle_health = self._connection_lifecycle_health()
        canary_decision = self._optional_family_canary_decision(subscription_window)
        canary_warmup_remaining = self._optional_family_canary_warmup_remaining(
            subscription_window
        )
        optional_family_canary = canary_decision.to_dict()
        optional_family_canary["warmup_seconds"] = self.optional_family_canary_warmup_seconds
        optional_family_canary["warmup_remaining_seconds"] = canary_warmup_remaining
        optional_family_canary["warmup_generation"] = self._optional_family_canary_generation
        optional_family_canary["rollback_evidence"] = self.optional_family_rollback_evidence
        optional_family_canary["evaluation_evidence"] = dict(
            self.optional_family_canary_evaluation_evidence
        )
        with self._orb_reference_progress_lock:
            orb_reference_results = dict(self.last_orb_reference_results)
            orb_reference_failed_attempts = [
                dict(attempt) for attempt in self._orb_reference_failed_attempts
            ]
            orb_reference_failed_attempt_count = (
                self._orb_reference_failed_attempt_count
            )
            orb_reference_failure_warning_suppressed_count = (
                self._orb_reference_failure_warning_suppressed_count
            )
        return {
            "provider": self.provider_name,
            "subscription_epoch_id": active_epoch_id,
            "websocket": (
                "active"
                if stream_progressing
                else "off_hours"
                if self.is_running and not subscription_allowed
                else "degraded"
                if self.is_running
                else "stopped"
            ),
            "buffer_health": "healthy" if (valid_symbols and stream_progressing) else "warming",
            "subscription_session_state": subscription_window.get("state"),
            "subscription_allowed": subscription_allowed,
            "subscription_suppressed": not subscription_allowed,
            "subscription_window": subscription_window,
            "messages_received": self.messages_received,
            "quote_records_seen": self.quote_records_seen,
            "invalid_quote_records": self.invalid_quote_records,
            "crossed_quote_records": self.crossed_quote_records,
            "unmapped_quote_records": self.unmapped_quote_records,
            "provider_timestamp_missing_records": self.provider_timestamp_missing_records,
            "provider_timestamp_order_errors": self.provider_timestamp_order_errors,
            "negative_receive_lag_records": self.negative_receive_lag_records,
            "material_negative_receive_lag_records": self.material_negative_receive_lag_records,
            "processing_clock_telemetry": self._processing_clock_telemetry(),
            "last_receive_to_process_lag_seconds": self.last_receive_to_process_lag_seconds,
            "max_receive_to_process_lag_seconds": self.max_receive_to_process_lag_seconds,
            "quotes_cached": len(self.quotes),
            "fresh_quote_counts": fresh_quote_counts,
            "fresh_quote_counter_mode": "incremental-expiry-heap-v1",
            "active_generation": active_generation,
            "subscription_cutoff_monotonic": self.subscription_cutoff_monotonic,
            "handoff_status": active_handoff_status,
            "handoff_reason": self.handoff_reason,
            "symbols_subscribed": len(active_live_symbols),
            "subscribed_symbols": active_live_symbols,
            "symbols_selected": len(self.live_symbols),
            "selected_symbols": self.live_symbols,
            "subscription_profile": self.subscription_profile,
            "subscription_staging": subscription_stage_health,
            "subscription_bounds": self.subscription_metadata.get("bounds") or _subscription_bound_values(),
            "subscription_metadata": self.subscription_metadata,
            "root_contract_counts": root_counts,
            "core_symbol_status": core_symbol_status,
            "market_subscription_status": market_subscription_status,
            "optional_family_canary": optional_family_canary,
            "primary_pair_admission_diagnostics": self.primary_pair_admission_diagnostics,
            "optional_family_unavailable_reasons": (
                self.subscription_metadata.get(
                    "optional_family_unavailable_reasons"
                )
                or self._optional_family_unavailable_reasons()
            ),
            "symbols_requested": self.symbols,
            "valid_symbols": valid_symbols,
            "retained_valid_symbols": sorted(retained_pins),
            "invalid_symbols": invalid_symbols,
            "retained_invalid_symbols": sorted(retained_invalid),
            "invalid_reasons": invalid_reasons,
            "calculation_diagnostics": self.last_calculation_diagnostics,
            "cache_file": str(active_cache_file),
            "expected_current_cache_file": str(self._cache_path()),
            "universe_fallback_active": bool(universe_provenance.get("is_fallback")),
            "universe_provenance": universe_provenance,
            "full_oi_contracts_cached": int(len(self.full_universe)),
            "oi_analytics_by_market": self.oi_analytics_by_market,
            "universe_age_seconds": universe_age_seconds,
            "universe_refresh_interval_seconds": universe_refresh_interval,
            "universe_refresh_pending": bool(
                universe_age_seconds is not None
                and universe_age_seconds >= universe_refresh_interval
            ),
            "allow_live_universe_refresh_reconnect": ALLOW_LIVE_UNIVERSE_REFRESH_RECONNECT,
            "open_transition_grace_seconds": OPEN_TRANSITION_GRACE_SECONDS,
            "open_transition_grace_remaining_seconds": open_grace_remaining,
            "compute_warmup_grace_seconds": COMPUTE_WARMUP_GRACE_SECONDS,
            "provider_overload_compute_backoff_seconds": PROVIDER_OVERLOAD_COMPUTE_BACKOFF_SECONDS,
            "compute_backpressure_remaining_seconds": compute_backpressure_remaining,
            "orb_reference_sampler": {
                "thread_alive": bool(
                    self.orb_reference_thread
                    and self.orb_reference_thread.is_alive()
                ),
                "interval_seconds": ORB_REFERENCE_INTERVAL_SECONDS,
                "attempt_timeout_seconds": (
                    ORB_REFERENCE_ATTEMPT_TIMEOUT_SECONDS
                ),
                "shutdown_barrier_timeout_seconds": (
                    ORB_REFERENCE_SHUTDOWN_BARRIER_TIMEOUT_SECONDS
                ),
                "failed_attempt_count": orb_reference_failed_attempt_count,
                "failed_attempt_history_limit": ORB_REFERENCE_FAILED_ATTEMPT_LIMIT,
                "failure_warning_interval_seconds": (
                    ORB_REFERENCE_FAILURE_WARNING_INTERVAL_SECONDS
                ),
                "failure_warning_suppressed_count": (
                    orb_reference_failure_warning_suppressed_count
                ),
                "recent_failed_attempts": orb_reference_failed_attempts,
                "last_bucket_utc": (
                    self._last_orb_reference_bucket_utc.isoformat()
                    if self._last_orb_reference_bucket_utc
                    else None
                ),
                "markets": {
                    market: {
                        key: result.get(key)
                        for key in (
                            "recorded",
                            "reason",
                            "sample_id",
                            "sample_timestamp_utc",
                            "source_timestamp_utc",
                            "subscription_generation",
                            "subscription_epoch_id",
                            "captured_context_identity",
                            "active_generation_after_persist",
                            "active_generation_after_return",
                            "post_persist_context_current",
                            "progress_eligible",
                        )
                    }
                    for market, result in orb_reference_results.items()
                },
            },
            "schema": self.schema,
            "last_update_utc": latest_update.isoformat() if latest_update else None,
            "last_diagnostic_utc": latest_diagnostic.isoformat() if latest_diagnostic else None,
            "data_age_seconds": data_age_seconds,
            "stream_progressing": stream_progressing,
            "stream_stalled_seconds": None if progress_age_seconds is None else max(0.0, progress_age_seconds - STREAM_PROGRESS_WINDOW_SECONDS),
            "reconnect_attempts": self.reconnect_attempts,
            "last_reconnect_utc": self.last_reconnect_utc.isoformat() if self.last_reconnect_utc else None,
            "last_reconnect_reason": self.last_reconnect_reason,
            **connection_lifecycle_health,
            "subscription_attempts": self.subscription_attempts,
            "last_subscription_utc": self.last_subscription_utc.isoformat() if self.last_subscription_utc else None,
            "configured_replay_minutes": self.replay_minutes,
            "replay_on_reconnect": REPLAY_ON_RECONNECT,
            "replay_during_regular_session": REPLAY_DURING_REGULAR_SESSION,
            "last_subscription_replay_start_utc": self.last_subscription_replay_start_utc,
            "trading_date_rollover_pending": self._universe_trading_date_changed(),
            "provider_queue_full_warnings": self.provider_queue_full_warnings,
            "provider_slow_client_warnings": self.provider_slow_client_warnings,
            "provider_skipped_record_warnings": self.provider_skipped_record_warnings,
            "provider_skipped_records": self.provider_skipped_records,
            "provider_pending_records_peak": self.provider_pending_records_peak,
            "last_provider_warning_utc": (
                self.last_provider_warning_utc.isoformat()
                if self.last_provider_warning_utc else None
            ),
            "last_provider_warning": self.last_provider_warning,
            "last_error": self.last_error,
            "gex_formula_version": GEX_FORMULA_VERSION,
            "formula_validation_errors": dict(self.formula_validation_errors),
            "formula_health": self._formula_health_snapshot(),
        }
