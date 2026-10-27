"""Read-only educational walkthrough from live OPRA evidence to research forecasts."""
from __future__ import annotations

import json
import math
from datetime import datetime, timezone

import requests

from app.services.forecast_research_view import (
    RESEARCH_URL, ResearchSchemaError, _number,
    _validate_group, central_timestamp, forecast_kind, current_publication_projection,
)

HEALTH_URL = "http://127.0.0.1:8000/health/live"
SYMBOLS = ("SPX", "NDX")
HORIZONS = {0: "Today's close", 1: "Next trading session", 5: "Five trading sessions ahead"}


def _stamp(value):
    try:
        stamp = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return stamp if stamp.tzinfo else None
    except (AttributeError, TypeError, ValueError):
        return None


def _identity(payload):
    epoch, generation = payload.get("subscription_epoch_id"), payload.get("subscription_generation")
    if (not isinstance(epoch, str) or len(epoch) != 64
            or any(c not in "0123456789abcdef" for c in epoch)
            or type(generation) is not int or generation <= 0):
        return None
    return epoch, generation


def _finite(value):
    if isinstance(value, bool):
        return None
    try:
        number = float(value)
        return number if math.isfinite(number) else None
    except (TypeError, ValueError):
        return None


def _runtime_reason(health, now):
    stamp = _stamp(health.get("sampled_at_utc"))
    if stamp is None or not 0 <= (now - stamp).total_seconds() <= 15:
        return "LIVE_HEALTH_STALE_OR_UNKNOWN"
    if health.get("subscription_session_state") == "post_close":
        return "MARKET_SESSION_CLOSED"
    if not _identity(health) or health.get("runtime_context_stable") is not True:
        return "LIVE_RUNTIME_IDENTITY_UNVERIFIED"
    if (health.get("provider") != "databento" or health.get("stream_connected") is not True
            or health.get("stream_progressing") is not True or health.get("handoff_status") != "active"):
        return "LIVE_TRANSPORT_NOT_READY"
    if (health.get("processing_clock_telemetry") or {}).get("status") != "synchronized":
        return "LIVE_PROCESSING_CLOCK_NOT_READY"
    return None


def _symbol_reason(health, symbol):
    detail = (health.get("symbol_status") or {}).get(symbol) or {}
    age, limit = _finite(detail.get("data_age_seconds")), _finite(detail.get("stale_after_seconds"))
    if (detail.get("usable_for_prediction") is not True or detail.get("validation_is_valid") is not True
            or detail.get("is_stale") is not False or detail.get("epoch_is_current") is not True
            or detail.get("generation_is_current") is not True or _identity(detail) != _identity(health)
            or age is None or limit is None or limit <= 0 or not 0 <= age <= limit
            or (_finite(detail.get("fresh_quote_count")) or 0) <= 0):
        return "LIVE_SYMBOL_EVIDENCE_UNVERIFIED"
    return None


def collect_demo(selections, *, requester=None, clock=None):
    """Four bounded loopback GETs at most; independent symbol errors stay isolated."""
    requester = requester or requests.get
    clock = clock or (lambda: datetime.now(timezone.utc))
    receipt = {"schema_version": "marketpin-live-api-demo.v1", "started_at_utc": clock().isoformat(),
               "selections": dict(selections), "health_before": {}, "health_after": {},
               "research": {}, "failures": [], "authority": "RESEARCH_ONLY", "decision_grade": False}

    def get(url, **kwargs):
        response = requester(url, timeout=3, **kwargs)
        response.raise_for_status()
        result = response.json()
        if not isinstance(result, dict):
            raise ResearchSchemaError()
        return result

    for phase in ("before", "after"):
        try:
            receipt[f"health_{phase}"] = get(HEALTH_URL)
        except (requests.RequestException, ValueError, TypeError):
            receipt["failures"].append({"scope": f"health_{phase}", "code": "LIVE_HEALTH_READ_FAILED"})
        if phase == "before":
            for symbol, horizon in selections.items():
                if symbol not in SYMBOLS or type(horizon) is not int or horizon not in HORIZONS:
                    receipt["failures"].append({"scope": symbol, "code": "INVALID_SELECTION"})
                    continue
                try:
                    payload = get(RESEARCH_URL + "/v1/research/forecasts",
                                  params={"symbols": symbol, "horizon_sessions": horizon})
                    _validate_group(payload, [symbol], horizon)
                    receipt["research"][symbol] = payload
                except (requests.RequestException, ValueError, TypeError, AttributeError):
                    receipt["failures"].append({"scope": symbol, "code": "RESEARCH_READ_OR_SCHEMA_FAILED"})
    receipt["completed_at_utc"] = clock().isoformat()
    return receipt


def message_delta(previous, current):
    """Counter movement is observed only across ordered samples of one identity."""
    before, after = (previous or {}).get("health_after", {}), current.get("health_after", {})
    first, last = _stamp(before.get("sampled_at_utc")), _stamp(after.get("sampled_at_utc"))
    counts = before.get("messages_received"), after.get("messages_received")
    if (not _identity(before) or _identity(before) != _identity(after) or first is None or last is None
            or last <= first or any(type(n) is not int or n < 0 for n in counts) or counts[1] < counts[0]):
        return None
    return {"messages": counts[1] - counts[0], "seconds": (last - first).total_seconds()}


def demo_rows(receipt, *, now=None):
    """Publication projection; raw retained payload numbers never bypass these gates."""
    now = now or datetime.now(timezone.utc)
    before, after = receipt.get("health_before") or {}, receipt.get("health_after") or {}
    global_reason = _runtime_reason(before, now) or _runtime_reason(after, now)
    start, end = _stamp(before.get("sampled_at_utc")), _stamp(after.get("sampled_at_utc"))
    if not global_reason and (_identity(before) != _identity(after) or end < start):
        global_reason = "RUNTIME_CHANGED_DURING_READ"
    rows = []
    for symbol, horizon in receipt.get("selections", {}).items():
        payload = receipt.get("research", {}).get(symbol) or {}
        item = (payload.get("symbols") or {}).get(symbol) or {}
        forecast = item.get("forecast") or {}
        evidence = forecast.get("evidence") or {}
        projection = current_publication_projection(forecast, payload, payload.get("publication_gate"),
            symbol=symbol, now=now, expected_horizon=horizon)
        reasons = list(projection["reasons"])
        reasons.extend(f["code"] for f in receipt.get("failures", []) if f["scope"] in (symbol, "health_before", "health_after"))
        reason = global_reason or _symbol_reason(before, symbol) or _symbol_reason(after, symbol)
        if not reason and (_identity(evidence) != _identity(after)
                           or _identity(payload.get("runtime_context") or {}) != _identity(after)):
            reason = "FORECAST_RUNTIME_BINDING_MISMATCH"
        if not reason:
            reason = projection["reason"]
        if not reason and evidence.get("source") != "databento_opra_put_call_parity":
            reason = "OPRA_PARITY_SOURCE_UNVERIFIED"
        eligible_input = not reason and payload.get("status") == "RESEARCH_ONLY"
        eligible_forecast = eligible_input and projection["eligible"]
        if reason:
            reasons.append(reason)
        if payload.get("status") != "RESEARCH_ONLY":
            reasons.extend(payload.get("reasons") or ["RESEARCH_CAPTURE_NOT_READY"])
        predicted = _number(forecast.get("predicted_close")) if eligible_forecast else None
        if eligible_forecast and predicted is None:
            reasons.append("INVALID_FORECAST_NUMBER")
        comparisons = []
        for candidate in forecast.get("candidates") or []:
            comparisons.append({"Method": candidate.get("method_id"),
                "Use": "Experimental comparison; not selected" if candidate.get("prospective_only") is True
                    else "Selected forecast" if candidate.get("method_id") == forecast.get("selected_method")
                    else "Comparison candidate",
                "Evidence limitation": candidate.get("limitation"),
                "Status": candidate.get("status") if eligible_forecast else "ABSTAIN",
                "Research estimate": _number(candidate.get("predicted_close"))
                    if eligible_forecast and candidate.get("status") == "RESEARCH_ONLY" else None,
                "Reason": "; ".join(candidate.get("reasons") or []) if eligible_forecast
                    else "; ".join(dict.fromkeys(reasons)), "Equation": candidate.get("equation")})
        rows.append({"symbol": symbol, "status": "RESEARCH_ONLY" if predicted is not None else "ABSTAIN",
            "reference": _number(evidence.get("spot")) if eligible_input else None,
            "predicted_close": predicted, "method": forecast.get("selected_method") if predicted is not None else None,
            "horizon_sessions": horizon, "target_session": forecast.get("target_session_date"),
            "source_time": evidence.get("source_timestamp_utc"), "forecast_time": forecast.get("as_of_utc"),
            "forecast_id": forecast.get("forecast_id"), "reasons": list(dict.fromkeys(reasons)),
            "candidates": comparisons, "history": item.get("history_availability") or {},
            "fresh_quotes": ((after.get("symbol_status") or {}).get(symbol) or {}).get("fresh_quote_count"),
            "quote_age": ((after.get("symbol_status") or {}).get(symbol) or {}).get("data_age_seconds")})
    return rows


def render_live_api_demo():
    import streamlit as st
    # The fragment always rechecks retained ages, but reads the network only on opt-in.
    st.fragment(run_every="15s")(_render_live_api_demo)()


def _render_live_api_demo():
    import streamlit as st
    st.subheader("Live Databento forecast demonstration")
    st.caption("Databento OPRA option quotes → option-implied index reference → research forecast. "
               "SPX/NDX references are calculated put/call parity, not observed cash-index trades. "
               "Forecasts are INFERRED · RESEARCH_ONLY; accuracy is not calibrated.")
    columns = st.columns(2)
    selections = {}
    for column, symbol in zip(columns, SYMBOLS):
        with column:
            selections[symbol] = st.selectbox(f"{symbol} demonstration horizon", list(HORIZONS),
                format_func=HORIZONS.get, key=f"live_api_demo_horizon_{symbol}")
    auto = st.toggle("Keep live demonstration refreshing", value=False, key="live_api_demo_auto",
        help="Read local API projections every 15 seconds. Switch off to stop automatic reads. No provider subscriptions change.")
    refresh = st.button("Load / refresh live demonstration", key="live_api_demo_refresh")
    retained = st.session_state.get("live_api_demo_receipt")
    if refresh or auto:
        st.session_state["live_api_demo_previous"] = retained
        retained = collect_demo(selections)
        st.session_state["live_api_demo_receipt"] = retained
    if not retained:
        st.info("Load the demonstration to inspect the live API and eligible forecasts.")
        return
    if retained.get("selections") != selections:
        st.info("Horizon selection changed. Load the demonstration for the selected horizons.")
        return
    now = datetime.now(timezone.utc)
    health = retained.get("health_after") or {}
    reason = _runtime_reason(health, now)
    st.caption("API read: " + central_timestamp(retained.get("completed_at_utc"))
               + (" · Automatic local refresh on" if auto else " · Automatic local refresh off"))
    if reason == "MARKET_SESSION_CLOSED":
        st.info("The cash session is closed. The local API is responding; fresh session forecasts are withheld. "
                "Previously saved forecasts remain historical evidence, not current live predictions.")
    elif reason:
        st.warning(f"Live source not established: {reason}. Invalid estimates are withheld.")
    else:
        st.success("OBSERVED: Databento transport connected; current runtime and processing-clock checks passed at this read.")
    delta = message_delta(st.session_state.get("live_api_demo_previous"), retained)
    st.caption(f"Provider messages recorded: {health.get('messages_received', 'Unavailable')} · "
               + (f"Observed change: {delta['messages']:+,} over {delta['seconds']:.1f}s"
                  if delta else "Advancement: NOT YET VERIFIED across two comparable reads"))
    st.caption(f"Epoch: {health.get('subscription_epoch_id') or 'Unavailable'} · "
               f"Generation: {health.get('subscription_generation') or 'Unavailable'}")
    rows = demo_rows(retained, now=now)
    available_services = len(retained.get("research") or {})
    eligible_count = sum(row["status"] == "RESEARCH_ONLY" for row in rows)
    st.caption(f"Research API: {available_services}/{len(selections)} symbol requests returned valid responses · "
               f"Authority: research only · Currently display-eligible: {eligible_count}/{len(rows)} symbols")
    for column, row in zip(st.columns(2), rows):
        with column:
            st.markdown(f"**{row['symbol']} · {row['status']}**")
            st.metric("Option-implied reference (index points)",
                      f"{row['reference']:,.2f}" if row["reference"] is not None else "Unavailable")
            kind = forecast_kind(row["method"], row["predicted_close"] is not None)
            st.metric("No-change baseline only (index points)" if kind == "No-change baseline only"
                      else "Research forecast (index points)",
                      f"{row['predicted_close']:,.2f}" if row["predicted_close"] is not None else "ABSTAIN")
            if kind == "No-change baseline only":
                st.info("The only selected estimate holds the input reference unchanged. "
                        "This benchmark does not demonstrate predictive skill.")
            st.caption(f"Target session: {row['target_session'] or 'Unavailable'} · "
                       f"Method: {row['method'] or 'Unavailable'}")
            st.caption("Input: " + central_timestamp(row["source_time"]) + " · Forecast: "
                       + central_timestamp(row["forecast_time"]))
            st.caption(f"Fresh option quote count: {row['fresh_quotes'] if row['fresh_quotes'] is not None else 'Unavailable'} · "
                       f"Backend calculation age at read: {format(row['quote_age'], '.1f') if _finite(row['quote_age']) is not None else 'Unavailable'} seconds")
            if row["reasons"]:
                st.caption("Reasons: " + "; ".join(row["reasons"]))
            if row["horizon_sessions"]:
                history = row["history"]
                st.caption(f"Qualified historical returns: {history.get('qualified_return_samples', 'Unavailable')} / "
                           f"{history.get('required_return_samples', 'Unavailable')} required. "
                           "Today's EOD forecast is never substituted for a future session.")
            if row["candidates"]:
                with st.expander(f"{row['symbol']} candidate methods and baseline"):
                    st.dataframe(row["candidates"], hide_index=True, width="stretch")
                    st.caption("Differences between candidate estimates are model disagreement, not a confidence interval. "
                               "The last-price method is an unchanged-reference baseline.")
    indicators = [p.get("indicator_capture") or {} for p in retained.get("research", {}).values()]
    if any(p.get("status") == "ENTITLEMENT_UNAVAILABLE" for p in indicators):
        st.info("Observed ETF indicators: the Databento equities entitlement is unavailable. "
                "OPRA-derived index references remain a separate data source.")
    export = {"display_checked_at_utc": now.isoformat(), "display": rows, "receipt": retained,
              "receipt_scope": "Retained source evidence; receipt values do not bypass display eligibility gates."}
    st.download_button("Download forecast provenance JSON", json.dumps(export, indent=2, allow_nan=False),
                       file_name="marketpin_live_api_demo.json", mime="application/json",
                       key="live_api_demo_download")
