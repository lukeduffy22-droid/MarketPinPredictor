"""Display-only technical indicators from verified retained bars."""
from datetime import datetime, timezone
import math
from zoneinfo import ZoneInfo

import pandas as pd
import requests

from backend.indicator_bars import SCHEMA, bar_symbol, utc, validate_bars, unavailable

REASONS = {
    "RETAINED_BAR_DATABASE_MISSING": "No retained observed minute-bar dataset has been captured.",
    "RETAINED_BAR_READ_UNAVAILABLE": "The retained minute-bar store could not be read.",
    "OBSERVED_INTRADAY_BARS_UNAVAILABLE": "No observed minute bars are available for this session.",
    "OBSERVED_BAR_SOURCE_UNSUPPORTED": "No supported observed price-bar source is connected for this instrument.",
    "INDICATOR_LOOKBACK_INSUFFICIENT": "At least 35 completed one-minute bars are required.",
    "OBSERVED_BARS_STALE": "The retained price bars are stale; refresh after capture resumes.",
    "BAR_SESSION_OPEN_MISSING": "The first session bar is missing; a full-session volume-weighted reference cannot be calculated.",
    "BAR_HISTORY_GAPPED_OR_DUPLICATED": "The session bar history has missing or duplicated minutes.",
    "BAR_VOLUME_UNAVAILABLE": "Observed trading volume is unavailable.",
    "OUTSIDE_CASH_SESSION": "Current-session technical indicators are unavailable outside the cash session.",
    "BAR_API_NOT_LOADED": "The running research service has not loaded the observed-bar endpoint.",
    "BAR_API_UNAVAILABLE": "The observed-bar research service is unavailable.",
    "BAR_API_TIMEOUT": "The observed-bar research request timed out.",
    "ENTITLEMENT_UNAVAILABLE": "The current price-data request was denied by the provider's live-data license gate.",
    "EQUITY_CONTEXT_FETCH_FAILED": "The minute-bar capture request failed; the background collector will retry.",
    "INDICATOR_CAPTURE_WRITER_UNAVAILABLE": "Minute-bar capture could not acquire its single-writer store lock.",
    "INDICATOR_CAPTURE_START_FAILED": "The minute-bar collector could not start; inspect research-service health.",
}


def fetch_indicator_bars(symbol, *, requester=None):
    if not bar_symbol(symbol):
        return unavailable(symbol, "OBSERVED_BAR_SOURCE_UNSUPPORTED")
    try:
        response = (requester or requests.get)("http://127.0.0.1:8001/v1/research/indicator-bars",
                                               params={"symbol": symbol}, timeout=2)
        if response.status_code != 200:
            return unavailable(symbol, "BAR_API_NOT_LOADED" if response.status_code == 404 else "BAR_API_UNAVAILABLE")
        return response.json()
    except requests.Timeout:
        return unavailable(symbol, "BAR_API_TIMEOUT")
    except (requests.RequestException, ValueError):
        return unavailable(symbol, "BAR_API_UNAVAILABLE")


def indicator_surface(symbol, evidence, *, now=None):
    now = utc(now or datetime.now(timezone.utc))
    target = bar_symbol(symbol)
    provenance = {"status": "unavailable", "required_source_kind": "observed_intraday_ohlcv",
                  "is_observed": False, "is_synthetic": False, "row_count": 0,
                  "requested_symbol": symbol, "bar_symbol": target,
                  "is_proxy_context": target != symbol, "units": "ETF USD/share"}
    empty = pd.DataFrame({"timestamp": pd.Series(dtype="datetime64[ns, UTC]"),
                          **{key: pd.Series(dtype="float64") for key in ("open", "high", "low", "close", "volume")}})
    try:
        if (not isinstance(evidence, dict) or evidence.get("schema_version") != SCHEMA
                or evidence.get("requested_symbol") != symbol or evidence.get("bar_symbol") != target
                or not 0 <= (now - utc(evidence.get("as_of_utc"))).total_seconds() <= 30):
            raise ValueError("BAR_RESPONSE_INVALID_OR_STALE")
        if evidence.get("status") != "available":
            reason = evidence.get("reason_code")
            raise ValueError(reason if reason in REASONS else "BAR_EVIDENCE_INVALID")
        rows = validate_bars(symbol, evidence.get("bars"), now=now)
        frame = pd.DataFrame(rows).rename(columns={"timestamp_utc": "timestamp"})
        frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True)
        close = frame["close"]
        frame["SMA_20"] = close.rolling(20, min_periods=20).mean()
        delta = close.diff()
        gain = delta.clip(lower=0).rolling(14, min_periods=14).mean()
        loss = (-delta.clip(upper=0)).rolling(14, min_periods=14).mean()
        frame["RSI"] = 100 - 100 / (1 + gain / loss)
        frame.loc[(gain == 0) & (loss == 0), "RSI"] = 50.0
        frame["MACD"] = close.ewm(span=12, adjust=False, min_periods=12).mean() - close.ewm(span=26, adjust=False, min_periods=26).mean()
        frame["Signal_Line"] = frame["MACD"].ewm(span=9, adjust=False, min_periods=9).mean()
        frame["Momentum"] = close.diff(10)
        # Minute-bar typical-price weighting is an approximation, not trade VWAP.
        typical = (frame["high"] + frame["low"] + close) / 3
        volume = frame["volume"].cumsum()
        frame["VWAP"] = (typical * frame["volume"]).cumsum() / volume.where(volume > 0)
        ama = [float("nan")] * len(close)
        ama[9] = float(close.iloc[9])
        for i in range(10, len(close)):
            movement = float(close.diff().abs().iloc[i-9:i+1].sum())
            efficiency = abs(float(close.iloc[i] - close.iloc[i-10])) / movement if movement else 0.0
            smoothing = (efficiency * (2 / 3 - 2 / 31) + 2 / 31) ** 2
            ama[i] = ama[i-1] + smoothing * (float(close.iloc[i]) - ama[i-1])
        frame["AMA"] = ama
        if not all(math.isfinite(float(frame.iloc[-1][k])) for k in ("RSI", "MACD", "Signal_Line", "SMA_20", "Momentum", "VWAP", "AMA")):
            raise ValueError("INDICATOR_VALUES_UNAVAILABLE")
    except (ValueError, TypeError, KeyError, OverflowError) as exc:
        code = str(exc) if str(exc).isupper() and len(str(exc)) < 80 else "BAR_EVIDENCE_INVALID"
        return empty, {**provenance, "reason_code": code, "reason": REASONS.get(code, "Observed bars failed source, time, or value validation.")}
    return frame, {**provenance, "status": "available", "is_observed": True, "row_count": len(frame),
                   "dataset": "EQUS.MINI", "schema": "ohlcv-1m", "reason": "Indicators derived from retained observed ETF bars.",
                   "indicator_version": "observed-minute-indicators-v1", "derived_not_observed": True,
                   "as_of_utc": now.isoformat(), "latest_bar_utc": rows[-1]["timestamp_utc"],
                   "bar_weighted_reference_kind": "session_typical_price_volume_weighted_approximation"}


def attach_indicators(prediction, evidence, *, now=None):
    result = dict(prediction)
    frame, provenance = indicator_surface(result["ticker"], evidence, now=now)
    result.update(df=frame, indicator_provenance=provenance,
                  indicators_available=provenance["status"] == "available", indicator_status=provenance["status"])
    return result


def render_observed_indicators(prediction, *, now=None):
    """Render only correctly labeled bar context; never overlay an index target."""
    import streamlit as st

    p = prediction.get("indicator_provenance") or {}
    frame = prediction.get("df")
    st.subheader("Technical indicators from observed price bars")
    if prediction.get("indicators_available") is not True or not isinstance(frame, pd.DataFrame) or frame.empty:
        st.info("Unavailable: " + p.get("reason", "No verified observed bars are available."))
        return
    now = utc(now or datetime.now(timezone.utc))
    # Revalidate retained UI evidence on every render, including clock rollback/session rollover.
    try:
        rows = frame.rename(columns={"timestamp": "timestamp_utc"}).to_dict("records")
        for row in rows:
            row["timestamp_utc"] = utc(row["timestamp_utc"]).isoformat()
        validate_bars(prediction["ticker"], rows, now=now)
    except (ValueError, TypeError, KeyError, OverflowError):
        st.info("Unavailable: retained bar evidence is stale or invalid; refresh to recheck.")
        return
    symbol = p["bar_symbol"]
    label = f"{symbol} ETF context for {prediction['ticker']}" if p.get("is_proxy_context") else f"{symbol} observed ETF bars"
    st.caption(f"{label} · EQUS.MINI · 1-minute completed bars · {len(frame)} bars · USD/share")
    st.caption("Indicators are calculated values. ETF context is not an index price or an index forecast.")
    latest = frame.iloc[-1]
    for column, (label, key) in zip(st.columns(4), [("RSI 14 (rolling)", "RSI"), ("MACD (12,26,9)", "MACD"), ("SMA 20", "SMA_20"), ("Momentum 10", "Momentum")]):
        column.metric(label, f"{latest[key]:,.2f}")
    st.caption(f"Session bar-weighted price: ${latest['VWAP']:,.2f} (typical-price approximation; not trade VWAP).")
    st.caption(f"Adaptive moving average (KAMA 10,2,30): ${latest['AMA']:,.2f} · MACD signal: {latest['Signal_Line']:,.2f}")
    chart = frame.set_index("timestamp")[["close", "SMA_20"]].copy()
    chart.index = chart.index.tz_convert("America/Chicago")
    st.line_chart(chart.rename(columns={"close": f"{symbol} close", "SMA_20": f"{symbol} SMA 20"}))
    if p.get("is_proxy_context"):
        st.caption("Index technical-indicator AI remains unavailable until matching index observations are connected.")
    return latest


def render_indicator_panel(*, loader=None, now=None):
    """A forecast-independent, explicit-refresh view of retained observations."""
    import streamlit as st

    with st.expander("Observed price indicators"):
        st.caption("RSI, MACD, moving averages, momentum and a session bar-weighted price. "
                   "SPY, QQQ, IWM and DIA provide ETF context for SPX, NDX, RUT and DJI.")
        st.caption("Requires live U.S. equities data access and at least 35 completed current-session "
                   "minute bars. A forecast is not required to view indicators.")
        with st.form("observed_price_indicator_selection"):
            symbols = st.multiselect("Indicator context", ["SPX", "NDX", "RUT", "DJI"],
                                     default=["SPX", "NDX"], key="observed_price_indicator_symbols")
            submitted = st.form_submit_button("Load / refresh price indicators")
        if submitted:
            captured = utc(now or datetime.now(timezone.utc))
            st.session_state["observed_price_indicator_capture"] = {
                "symbols": tuple(symbols), "captured_at": captured,
                "evidence": {symbol: (loader or fetch_indicator_bars)(symbol) for symbol in symbols},
            }
        snapshot = st.session_state.get("observed_price_indicator_capture")
        if not snapshot:
            st.info("Load price indicators to check the latest captured bars.")
            return
        if tuple(symbols) != snapshot["symbols"]:
            st.info("Selection changed; load price indicators to check the selected markets.")
            return
        if not symbols:
            st.info("Select at least one market.")
            return
        checked = snapshot["captured_at"].astimezone(ZoneInfo("America/Chicago"))
        st.caption(f"Last requested: {checked.strftime('%b %d, %Y %I:%M:%S %p %Z')} · Manual refresh")
        for symbol in symbols:
            st.markdown(f"**{symbol} — {bar_symbol(symbol)} price-bar context**")
            prediction = attach_indicators({"ticker": symbol}, snapshot["evidence"][symbol], now=now)
            render_observed_indicators(prediction, now=now)
