"""Manual, per-symbol research horizon selection with explicit availability."""
from __future__ import annotations

import math
from datetime import datetime, timezone
from typing import Mapping

import requests

RESEARCH_URL = "http://127.0.0.1:8001"


def fetch_forecasts(selections: Mapping[str, int], *, requester=requests.get):
    grouped = {}
    for symbol, horizon in selections.items():
        grouped.setdefault(horizon, []).append(symbol)
    result = {"symbols": {}, "captures": [], "failures": []}
    for horizon, symbols in grouped.items():
        try:
            response = requester(RESEARCH_URL + "/v1/research/forecasts",
                                 params={"symbols": ",".join(symbols), "horizon_sessions": horizon}, timeout=3)
            response.raise_for_status()
            payload = response.json()
            if payload.get("schema_version") != "marketpin-market-research.v1":
                raise ValueError("Unknown research schema")
            result["symbols"].update(payload.get("symbols", {}))
            capture = {k: payload.get(k) for k in
                       ("as_of_utc", "capture_age_seconds", "status", "reasons", "equity_context")}
            if not any(c.get("as_of_utc") == capture.get("as_of_utc") and c.get("status") == capture.get("status")
                       for c in result["captures"]):
                result["captures"].append(capture)
        except (requests.RequestException, ValueError, TypeError, AttributeError):
            result["failures"].append("Research service unavailable for " + ", ".join(symbols))
    result["symbols"] = {s: result["symbols"][s] for s in selections if s in result["symbols"]}
    return result


def _number(value):
    if isinstance(value, bool):
        return None
    try:
        value = float(value)
        return value if math.isfinite(value) and value > 0 else None
    except (TypeError, ValueError):
        return None


def forecast_rows(payload):
    rows = []
    for symbol, item in payload.get("symbols", {}).items():
        forecast = item.get("forecast", {})
        direction = item.get("directional_shift", {})
        status = forecast.get("status", "ABSTAIN")
        try:
            age = (datetime.now(timezone.utc) - datetime.fromisoformat(forecast["as_of_utc"].replace("Z", "+00:00"))).total_seconds()
            if not 0 <= age <= 120:
                status = "STALE"
        except (KeyError, TypeError, ValueError):
            status = "ABSTAIN"
        eligible = status == "RESEARCH_ONLY"
        rows.append({"Symbol": symbol, "Sessions ahead": forecast.get("horizon_sessions"),
                     "Target session": forecast.get("target_session_date"), "Status": status,
                     "Research close": _number(forecast.get("predicted_close")) if eligible else None,
                     "Method": forecast.get("selected_method") if eligible else None,
                     "Shift state": "STALE" if status == "STALE" else direction.get("status", "ABSTAIN"),
                     "Direction": direction.get("direction") if status != "STALE" and direction.get("status") not in ("ABSTAIN", None) else None,
                     "Reason": "; ".join(forecast.get("reasons") or []),
                     "As of (UTC)": forecast.get("as_of_utc")})
    return rows


def render_forecast_research():
    import streamlit as st
    from backend.market_universe import DEFAULT_TRACKED_SYMBOLS, MARKET_UNIVERSE

    with st.expander("EOD, multi-day forecasts and directional shifts", expanded=False):
        st.caption("Select a horizon for each symbol. 0 = today's close; 1–10 = trading sessions ahead. "
                   "These are retained research estimates. Accuracy improvement has not been established.")
        symbols = st.multiselect("Research markets", list(DEFAULT_TRACKED_SYMBOLS),
                                default=["SPX", "NDX", "SPY", "QQQ"],
                                format_func=lambda s: f"{s} · {MARKET_UNIVERSE[s].name}")
        with st.form("forecast_research_selection"):
            horizons = {}
            columns = st.columns(4)
            for index, symbol in enumerate(symbols):
                with columns[index % 4]:
                    horizons[symbol] = st.selectbox(f"{symbol} forecast horizon", list(range(11)),
                        key=f"research_horizon_{symbol}",
                        format_func=lambda n: "Today's close" if n == 0 else f"{n} trading session{'s' if n != 1 else ''} ahead")
            refresh = st.form_submit_button("Load forecast research")
        if refresh:
            st.session_state["forecast_research_result"] = fetch_forecasts(horizons)
        payload = st.session_state.get("forecast_research_result")
        if payload:
            for failure in payload["failures"]:
                st.warning(failure)
            rows = forecast_rows(payload)
            if rows:
                st.dataframe(rows, hide_index=True, width="stretch", column_config={
                    "Research close": st.column_config.NumberColumn(format="%.2f"),
                })
            for capture in payload["captures"]:
                st.caption(f"Research captured at {capture.get('as_of_utc') or 'unavailable'} UTC · "
                           f"State: {capture.get('status') or 'unavailable'} · Manual refresh")
                if capture.get("reasons"):
                    st.caption("; ".join(capture["reasons"]))
                equity = capture.get("equity_context") or {}
                if equity.get("enabled") is False:
                    st.caption("ETF price collection is disabled; unavailable ETF forecasts remain blank.")
            st.caption("SPX/SPY, NDX/QQQ, DJI/DIA and RUT/IWM each count as one market family. "
                       "ETF prices stay in ETF units. DJI cash data is unavailable through the current feed.")
            with st.expander("Candidate methods, confirmation evidence and coverage"):
                st.json(payload)
        else:
            st.info("Load research to inspect each symbol's available forecast horizons and coverage gaps.")
