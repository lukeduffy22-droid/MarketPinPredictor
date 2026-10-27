"""Manual, per-symbol research horizon selection with explicit availability."""
from __future__ import annotations

import math
import os
from datetime import datetime, timezone
from typing import Mapping
from zoneinfo import ZoneInfo

import requests

_research_port = int(os.environ.get("MARKETPIN_FORECAST_RESEARCH_PORT", "8001"))
if not 1 <= _research_port <= 65535:
    raise ValueError("Invalid loopback research port")
RESEARCH_URL = f"http://127.0.0.1:{_research_port}"


class ResearchSchemaError(ValueError):
    pass


def _validate_group(payload, symbols, horizon):
    if not isinstance(payload, dict) or payload.get("schema_version") != "marketpin-market-research.v1":
        raise ResearchSchemaError()
    if (type(payload.get("horizon_sessions")) is not int or payload.get("horizon_sessions") != horizon or payload.get("requested_symbols") != symbols
            or not isinstance(payload.get("symbols"), dict) or set(payload["symbols"]) != set(symbols)):
        raise ResearchSchemaError()
    if not isinstance(payload.get("reasons"), list) or not all(isinstance(r, str) for r in payload["reasons"]):
        raise ResearchSchemaError()
    if payload.get("status") not in ("WARMING", "RESEARCH_ONLY", "UNAVAILABLE", "STALE"):
        raise ResearchSchemaError()
    if payload.get("equity_context") is not None and not isinstance(payload["equity_context"], dict):
        raise ResearchSchemaError()
    for symbol, item in payload["symbols"].items():
        if not isinstance(item, dict):
            raise ResearchSchemaError()
        forecast = item.get("forecast")
        if (not isinstance(forecast, dict) or forecast.get("symbol") != symbol
                or type(forecast.get("horizon_sessions")) is not int or forecast.get("horizon_sessions") != horizon
                or forecast.get("status") not in ("ABSTAIN", "RESEARCH_ONLY")
                or not isinstance(forecast.get("reasons"), list)
                or not all(isinstance(r, str) for r in forecast["reasons"])
                or not isinstance(forecast.get("evidence"), dict)
                or not isinstance(item.get("history_availability"), dict)
                or not isinstance(item.get("directional_shift", {}), dict)):
            raise ResearchSchemaError()
        if forecast["status"] == "RESEARCH_ONLY" and _number(forecast.get("predicted_close")) is None:
            raise ResearchSchemaError()
        history = item["history_availability"]
        if (not isinstance(history.get("reasons"), list)
                or not all(isinstance(r, str) for r in history["reasons"])):
            raise ResearchSchemaError()
        for key in ("qualified_return_samples", "required_return_samples", "verified_daily_close_count"):
            if history.get(key) is not None and (type(history[key]) is not int or history[key] < 0):
                raise ResearchSchemaError()
        if _freshness(forecast.get("as_of_utc"), datetime.now(timezone.utc)) == "RESEARCH_TIMESTAMP_INVALID":
            raise ResearchSchemaError()


def fetch_forecasts(selections: Mapping[str, int], *, requester=None):
    from backend.market_universe import DEFAULT_TRACKED_SYMBOLS
    requester = requester or requests.get
    grouped = {}
    result = {"symbols": {}, "captures": [], "failures": [], "selections": dict(selections),
              "submitted_at_utc": datetime.now(timezone.utc).isoformat()}
    for symbol, horizon in selections.items():
        if symbol not in DEFAULT_TRACKED_SYMBOLS or type(horizon) is not int or not 0 <= horizon <= 10:
            result["failures"].append({"code": "INVALID_SELECTION", "symbols": [symbol], "horizon_sessions": horizon})
        else:
            grouped.setdefault(horizon, []).append(symbol)
    for horizon, symbols in grouped.items():
        try:
            response = requester(RESEARCH_URL + "/v1/research/forecasts",
                                 params={"symbols": ",".join(symbols), "horizon_sessions": horizon}, timeout=3)
            response.raise_for_status()
            try:
                payload = response.json()
            except (ValueError, TypeError):
                result["failures"].append({"code": "DECODE_ERROR", "symbols": symbols, "horizon_sessions": horizon})
                continue
            _validate_group(payload, symbols, horizon)
            for symbol, item in payload["symbols"].items():
                item["capture"] = {k: payload.get(k) for k in ("as_of_utc", "status", "reasons", "runtime_context")}
                item["publication_gate"] = payload.get("publication_gate")
                result["symbols"][symbol] = item
            capture = {k: payload.get(k) for k in
                       ("as_of_utc", "capture_age_seconds", "status", "reasons", "equity_context", "publication_gate")}
            if not any(c.get("as_of_utc") == capture.get("as_of_utc") and c.get("status") == capture.get("status")
                       for c in result["captures"]):
                result["captures"].append(capture)
        except requests.Timeout:
            result["failures"].append({"code": "TIMEOUT", "symbols": symbols, "horizon_sessions": horizon})
        except requests.HTTPError as exc:
            result["failures"].append({"code": "HTTP_ERROR", "http_status": getattr(exc.response, "status_code", None),
                                       "symbols": symbols, "horizon_sessions": horizon})
        except requests.RequestException:
            result["failures"].append({"code": "TRANSPORT_ERROR", "symbols": symbols, "horizon_sessions": horizon})
        except (ResearchSchemaError, TypeError, AttributeError):
            result["failures"].append({"code": "SCHEMA_ERROR", "symbols": symbols, "horizon_sessions": horizon})
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


def _freshness(stamp, now):
    try:
        captured = datetime.fromisoformat(stamp.replace("Z", "+00:00"))
        if captured.tzinfo is None:
            return "RESEARCH_TIMESTAMP_INVALID"
        age = (now - captured).total_seconds()
        return "RESEARCH_CAPTURE_TIMESTAMP_IN_FUTURE" if age < 0 else "RESEARCH_CAPTURE_STALE" if age > 120 else None
    except (AttributeError, TypeError, ValueError):
        return "RESEARCH_TIMESTAMP_INVALID"


def _display_gate(forecast, now):
    reason = _freshness(forecast.get("as_of_utc"), now)
    if reason:
        return reason
    if forecast.get("horizon_sessions") == 0:
        try:
            close = datetime.fromisoformat(forecast["target_close_utc"].replace("Z", "+00:00"))
            if close.tzinfo is None:
                return "RESEARCH_TARGET_TIMESTAMP_INVALID"
            if now >= close:
                return "TARGET_SESSION_HAS_CLOSED"
        except (KeyError, TypeError, ValueError, AttributeError):
            return "RESEARCH_TARGET_TIMESTAMP_INVALID"
    return None


def research_refresh_due(payload, selections, *, enabled, now):
    """Refresh only submitted selections; edits to the form need submission."""
    if not enabled or not payload or payload.get("selections") != selections:
        return False
    try:
        submitted = datetime.fromisoformat(payload["submitted_at_utc"].replace("Z", "+00:00"))
        return submitted.tzinfo is None or (now - submitted).total_seconds() >= 15 or submitted > now
    except (KeyError, TypeError, ValueError, AttributeError):
        return True


def central_timestamp(value):
    try:
        stamp = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if stamp.tzinfo is None:
            return "Time unavailable"
        return stamp.astimezone(ZoneInfo("America/Chicago")).strftime("%b %d, %Y %I:%M:%S %p %Z")
    except (AttributeError, TypeError, ValueError):
        return "Time unavailable"


def forecast_kind(method, eligible=True):
    """Keep an unchanged-reference benchmark visible as a baseline, not an edge."""
    if not eligible:
        return "Unavailable"
    return "No-change baseline only" if method == "last_price_v1" else "Unvalidated research forecast"


def _publication_identity(payload):
    if not isinstance(payload, dict):
        return None
    epoch, generation = payload.get("subscription_epoch_id"), payload.get("subscription_generation")
    if (not isinstance(epoch, str) or len(epoch) != 64
            or any(c not in "0123456789abcdef" for c in epoch)
            or type(generation) is not int or generation <= 0):
        return None
    return epoch, generation


def current_publication_projection(forecast, capture, gate, *, symbol, now=None, expected_horizon=None):
    """One display contract for both research surfaces; never modifies a receipt.

    The API supplies request-time runtime checks. Their short authority lifetime
    is independent of the longer retained research-capture age allowance.
    """
    now = now or datetime.now(timezone.utc)
    forecast = forecast if isinstance(forecast, dict) else {}
    capture = capture if isinstance(capture, dict) else {}
    reasons = list(forecast.get("reasons") or [])
    reason = _display_gate(forecast, now)
    if not reason and (capture.get("as_of_utc") is not None or capture.get("status") == "RESEARCH_ONLY"):
        reason = _freshness(capture.get("as_of_utc"), now)
    if capture.get("status") != "RESEARCH_ONLY":
        reasons.extend(capture.get("reasons") or ["RESEARCH_CAPTURE_NOT_READY"])
        reason = reason or "RESEARCH_CAPTURE_NOT_READY"
    if not reason and (forecast.get("symbol") != symbol or expected_horizon is not None
                       and forecast.get("horizon_sessions") != expected_horizon):
        reason = "FORECAST_HORIZON_OR_SYMBOL_MISMATCH"
    entry = {}
    if (not isinstance(gate, dict) or gate.get("schema_version") != "marketpin-forecast-publication-gate.v1"
            or gate.get("status") not in ("ELIGIBLE", "ABSTAIN")):
        reason = reason or "CURRENT_PUBLICATION_GATE_UNAVAILABLE"
    else:
        try:
            checked = datetime.fromisoformat(gate["checked_at_utc"].replace("Z", "+00:00"))
            if checked.tzinfo is None or not 0 <= (now - checked).total_seconds() <= 15:
                reason = reason or "CURRENT_PUBLICATION_GATE_EXPIRED"
        except (KeyError, AttributeError, TypeError, ValueError):
            reason = reason or "CURRENT_PUBLICATION_GATE_TIMESTAMP_INVALID"
        entries = gate.get("symbols")
        entry = entries.get(symbol) if isinstance(entries, dict) else None
        entry = entry if isinstance(entry, dict) else {}
        expiry = entry.get("valid_until_utc", gate.get("valid_until_utc"))
        if expiry is not None:
            try:
                until = datetime.fromisoformat(expiry.replace("Z", "+00:00"))
                if until.tzinfo is None or now >= until:
                    reason = reason or "CURRENT_PUBLICATION_GATE_EXPIRED"
            except (AttributeError, TypeError, ValueError):
                reason = reason or "CURRENT_PUBLICATION_GATE_TIMESTAMP_INVALID"
        elif entry.get("status") == "ELIGIBLE":
            reason = reason or "CURRENT_PUBLICATION_GATE_EXPIRY_UNAVAILABLE"
        if entry.get("status") != "ELIGIBLE":
            codes = entry.get("reasons")
            reasons.extend([code for code in codes if isinstance(code, str)]
                           if isinstance(codes, list) else ["CURRENT_PUBLICATION_GATE_REJECTED"])
            reason = reason or "CURRENT_PUBLICATION_GATE_REJECTED"
        forecast_id = forecast.get("forecast_id")
        if not isinstance(forecast_id, str) or not forecast_id or entry.get("source_forecast_id") != forecast_id:
            reason = reason or "PUBLICATION_FORECAST_BINDING_MISMATCH"
        evidence = forecast.get("evidence") or {}
        identity = _publication_identity(gate.get("runtime_context"))
        if not identity or identity != _publication_identity(evidence):
            reason = reason or "PUBLICATION_RUNTIME_BINDING_MISMATCH"
        if capture.get("runtime_context") is not None and identity != _publication_identity(capture["runtime_context"]):
            reason = reason or "PUBLICATION_RUNTIME_BINDING_MISMATCH"
        if not reason:
            reason = _freshness(evidence.get("source_timestamp_utc"), now)
    if forecast.get("status") != "RESEARCH_ONLY":
        reason = reason or "CURRENT_FORECAST_NOT_ELIGIBLE"
    if not reason and _number(forecast.get("predicted_close")) is None:
        reason = "INVALID_FORECAST_NUMBER"
    if not reason and (not isinstance(forecast.get("selected_method"), str) or not forecast["selected_method"].strip()):
        reason = "FORECAST_METHOD_UNAVAILABLE"
    if reason:
        reasons.append(reason)
    eligible = reason is None
    return {"eligible": eligible, "status": "RESEARCH_ONLY" if eligible else
            "STALE" if reason == "RESEARCH_CAPTURE_STALE" else "ABSTAIN",
            "reason": reason, "reasons": list(dict.fromkeys(reasons)),
            "predicted_close": _number(forecast.get("predicted_close")) if eligible else None,
            "reference": _number((forecast.get("evidence") or {}).get("spot")) if eligible else None,
            "method": forecast.get("selected_method") if eligible else None}


def forecast_rows(payload, *, now=None):
    from backend.market_universe import MARKET_UNIVERSE
    now = now or datetime.now(timezone.utc)
    rows = []
    for symbol, item in payload.get("symbols", {}).items():
        forecast = item.get("forecast", {})
        direction = item.get("directional_shift", {})
        capture = item.get("capture")
        projection = current_publication_projection(forecast, capture, item.get("publication_gate"),
            symbol=symbol, now=now, expected_horizon=payload.get("selections", {}).get(symbol))
        status, reasons, eligible = projection["status"], projection["reasons"], projection["eligible"]
        history = item.get("history_availability") or {}
        reasons.extend(history.get("reasons") or [])
        evidence = forecast.get("evidence") or {}
        instrument = MARKET_UNIVERSE.get(symbol)
        rows.append({"Symbol": symbol, "Sessions ahead": forecast.get("horizon_sessions"),
                     "Target session": forecast.get("target_session_date"), "Status": status,
                     "Research close": _number(forecast.get("predicted_close")) if eligible else None,
                     "Method": forecast.get("selected_method") if eligible else None,
                     "Estimate type": forecast_kind(forecast.get("selected_method"), eligible),
                     "Units": "ETF USD/share" if instrument and instrument.kind == "etf" else "Index points",
                     "Qualified returns": history.get("qualified_return_samples"),
                     "Required returns": history.get("required_return_samples"),
                     "Verified daily closes": history.get("verified_daily_close_count"),
                     "Latest verified session": history.get("latest_verified_session_date"),
                     "Source": evidence.get("source") or "No verified current source",
                     "Source time (UTC)": evidence.get("source_timestamp_utc"),
                     "Shift state": direction.get("status", "ABSTAIN") if eligible else status,
                     "Direction": direction.get("direction") if eligible and direction.get("status") not in ("ABSTAIN", None) else None,
                     "Reason": "; ".join(dict.fromkeys(reasons)),
                     "As of (UTC)": forecast.get("as_of_utc")})
    for failure in payload.get("failures", []):
        for symbol in failure["symbols"]:
            rows.append({"Symbol": symbol, "Sessions ahead": failure["horizon_sessions"], "Status": "REQUEST_FAILED",
                         "Research close": None, "Reason": failure["code"] + (f" ({failure['http_status']})" if failure.get("http_status") else "")})
    return rows


def eod_comparison_rows(payload, *, now=None):
    """Display only exact-bound, current EOD comparisons; never reuse h=0 at h=1."""
    now = now or datetime.now(timezone.utc)
    labels = {"last_price_v1": "Last price baseline", "eod_damped_momentum_v1": "Recent momentum",
              "eod_sample_mean_reversion_v1": "Recent mean reversion",
              "eod_positive_gex_pin_context_v1": "Gamma pin context",
              "eod_equal_candidate_blend_v1": "Fixed candidate blend"}
    rows = []
    for symbol, item in payload.get("symbols", {}).items():
        forecast, learning = item.get("forecast", {}), item.get("eod_learning", {})
        capture = item.get("capture") or {}
        projection = current_publication_projection(forecast, capture, item.get("publication_gate"), symbol=symbol, now=now)
        if (forecast.get("horizon_sessions") != 0 or forecast.get("status") != "RESEARCH_ONLY"
                or not projection["eligible"]
                or learning.get("status") != "RESEARCH_ONLY"
                or not forecast.get("forecast_id")
                or learning.get("source_forecast_id") != forecast.get("forecast_id")
                or _display_gate(forecast, now)
                or _freshness(learning.get("as_of_utc"), now)
                or capture and (capture.get("status") != "RESEARCH_ONLY" or _freshness(capture.get("as_of_utc"), now))):
            continue
        for comparison in learning.get("comparisons", []):
            rows.append({"Symbol": symbol, "Method": labels.get(comparison["method_id"], comparison["method_id"]),
                "EOD research estimate": _number(comparison.get("current_estimate")),
                "Scored sessions": comparison.get("sessions", 0),
                "Historical mean absolute error": comparison.get("mae_points"),
                "Baseline error on same sessions": comparison.get("baseline_mae_points"),
                "Error improvement vs baseline": comparison.get("skill_vs_last_price")})
    return rows


def render_forecast_research():
    import streamlit as st
    # Recheck age while the page is open, without rerunning live capture panels.
    st.fragment(run_every="15s")(_render_forecast_research)()


def _render_forecast_research():
    import streamlit as st
    from backend.market_universe import DEFAULT_TRACKED_SYMBOLS, MARKET_UNIVERSE

    with st.expander("EOD, multi-day forecasts and directional shifts", expanded=False):
        st.caption("Select a horizon for each symbol. 0 = today's close; 1–10 = trading sessions ahead. "
                   "On weekends/holidays the calendar anchors to the next session; forecasts still abstain outside regular hours. "
                    "These are retained research estimates. Accuracy improvement has not been established. "
                    "Quality scores are uncalibrated unless a matched out-of-sample calibration artifact is explicitly shown; they are not win probabilities.")
        symbols = st.multiselect("Research markets", list(DEFAULT_TRACKED_SYMBOLS),
                                default=["SPX", "NDX", "SPY", "QQQ"],
                                format_func=lambda s: f"{s} · {MARKET_UNIVERSE[s].name}")
        auto_refresh = st.toggle("Keep submitted forecasts fresh", value=True,
            help="Recheck local publication eligibility every 15 seconds. No new provider subscriptions or calculations are requested.")
        with st.form("forecast_research_selection"):
            horizons = {}
            columns = st.columns(4)
            for index, symbol in enumerate(symbols):
                with columns[index % 4]:
                    horizons[symbol] = st.selectbox(f"{symbol} forecast horizon", list(range(11)),
                        key=f"research_horizon_{symbol}",
                        format_func=lambda n: "Today's close" if n == 0 else f"{n} trading session{'s' if n != 1 else ''} ahead")
            refresh = st.form_submit_button("Load forecast research")
        payload = st.session_state.get("forecast_research_result")
        if refresh or research_refresh_due(payload, horizons, enabled=auto_refresh, now=datetime.now(timezone.utc)):
            st.session_state["forecast_research_result"] = fetch_forecasts(horizons)
        payload = st.session_state.get("forecast_research_result")
        if payload:
            submitted = payload.get("selections", {})
            st.caption("Submitted selection: " + ", ".join(f"{s}: {h} sessions" for s, h in submitted.items())
                       + " · " + central_timestamp(payload.get("submitted_at_utc")))
            if submitted != horizons:
                st.info("Selections changed. Submit Load forecast research to replace the previous results.")
                return
            if payload["failures"]:
                codes = ", ".join(dict.fromkeys(f["code"] for f in payload["failures"]))
                st.warning(("Some research requests failed" if payload["symbols"] else "Research request failed")
                           + f" ({codes}). See affected symbols below; submit again to retry.")
            rows = forecast_rows(payload)
            eligible_count = sum(row.get("Status") == "RESEARCH_ONLY" for row in rows)
            st.caption(f"Local research service: {'request failures recorded' if payload['failures'] else 'responded at the last read'} · "
                       f"Authority: research only · Currently display-eligible: {eligible_count}/{len(rows)} symbols")
            baseline_symbols = [row["Symbol"] for row in rows if row.get("Estimate type") == "No-change baseline only"]
            if baseline_symbols:
                st.info("No-change baseline only: " + ", ".join(baseline_symbols)
                        + ". These estimates hold the input reference unchanged; they do not demonstrate predictive skill.")
            if rows:
                st.dataframe(rows, hide_index=True, width="stretch", column_config={
                    "Research close": st.column_config.NumberColumn(format="%.2f"),
                })
            comparisons = eod_comparison_rows(payload)
            if comparisons:
                st.subheader("EOD candidates and verified close comparisons")
                st.caption("Each score uses one frozen forecast per trading session, matched within the same "
                           "15-minute window before the close. Positive error improvement means lower historical "
                           "error than the last price baseline. Sparse scores do not establish future accuracy.")
                st.dataframe(comparisons, hide_index=True, width="stretch", column_config={
                    "EOD research estimate": st.column_config.NumberColumn(format="%.2f"),
                    "Historical mean absolute error": st.column_config.NumberColumn(format="%.2f"),
                    "Baseline error on same sessions": st.column_config.NumberColumn(format="%.2f"),
                    "Error improvement vs baseline": st.column_config.NumberColumn(format="percent"),
                })
                for symbol in dict.fromkeys(row["Symbol"] for row in comparisons):
                    learning = payload["symbols"][symbol]["eod_learning"]
                    spread, adaptive = learning.get("disagreement") or {}, learning["adaptive"]
                    bucket = learning["lead_bucket_minutes"]
                    st.caption(f"{symbol}: comparisons use forecasts {bucket['lower_exclusive']}–{bucket['upper_inclusive']} "
                               f"minutes before close. Candidate spread: {spread['low']:,.2f} to {spread['high']:,.2f}. "
                               "This spread is model disagreement, not a confidence interval.")
                    if adaptive.get("status") == "RESEARCH_ONLY" and _number(adaptive.get("predicted_close")):
                        st.info(f"{symbol} adaptive research challenger: {adaptive['predicted_close']:,.2f}. "
                                "Separate research estimate; live accuracy is not established.")
                        band = adaptive.get("historical_error_band")
                        if band:
                            st.caption(f"Historical 90% replay error band: {band['lower']:,.2f} to {band['upper']:,.2f}. "
                                       "Future coverage is unverified.")
                    else:
                        st.caption(f"{symbol} adaptive challenger withheld: {adaptive.get('training_sessions', 0)}/"
                                   f"{adaptive['minimum_training_sessions']} matched training sessions; "
                                   f"{adaptive.get('test_sessions', 0)}/{adaptive['minimum_test_sessions']} later test sessions. "
                                   + "; ".join(adaptive.get("reasons") or []))
                        legacy = adaptive.get("legacy_sessions_without_verified_issuance", 0)
                        if legacy:
                            st.caption(f"{symbol}: {legacy} older scored session(s) lack proof that the forecast was saved "
                                       "before the close. They can be compared, but are excluded from adaptive training.")
            for capture in payload["captures"]:
                st.caption(f"Research captured at {central_timestamp(capture.get('as_of_utc'))} · "
                           f"State at capture: {capture.get('status') or 'unavailable'} · "
                           + ("Publication eligibility rechecked every 15 seconds" if auto_refresh else "Manual refresh; retained publication eligibility expires within 15 seconds"))
                if capture.get("reasons"):
                    st.caption("; ".join(capture["reasons"]))
                equity = capture.get("equity_context") or {}
                if equity.get("enabled") is False:
                    st.caption("ETF price collection is disabled; unavailable ETF forecasts remain blank.")
            st.caption("SPX/SPY, NDX/QQQ, DJI/DIA and RUT/IWM retain separate instrument identities and units. "
                       "Direct cash-index observations are unsupported by this feed; OPRA parity is an option-implied reference. "
                       "DJI has no current cash-index source here. Future horizons require 60 qualified returns and current verified inputs.")
            with st.expander("Candidate methods, confirmation evidence and coverage"):
                st.caption("Retained receipts can include earlier forecasts now withheld by current publication checks. "
                           "Their stored values are historical evidence, not current live estimates.")
                st.json(payload)
        else:
            st.info("Load research to inspect each symbol's available forecast horizons and coverage gaps.")
