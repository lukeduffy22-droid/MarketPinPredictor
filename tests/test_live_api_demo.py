import copy
from datetime import datetime, timedelta, timezone

import pytest
import requests

from app.services.live_api_demo import collect_demo, demo_rows, message_delta


NOW = datetime(2026, 10, 6, 18, tzinfo=timezone.utc)
EPOCH = "a" * 64


def health(now=NOW):
    identity = {"subscription_epoch_id": EPOCH, "subscription_generation": 1}
    return {**identity, "sampled_at_utc": now.isoformat(), "provider": "databento",
        "runtime_context_stable": True, "stream_connected": True, "stream_progressing": True,
        "handoff_status": "active", "processing_clock_telemetry": {"status": "synchronized"},
        "messages_received": 100,
        "symbol_status": {symbol: {**identity, "usable_for_prediction": True, "validation_is_valid": True,
            "is_stale": False, "epoch_is_current": True, "generation_is_current": True,
            "data_age_seconds": 1, "stale_after_seconds": 30, "fresh_quote_count": 150}
            for symbol in ("SPX", "NDX")}}


def payload(symbol, horizon=0, now=NOW):
    identity = {"subscription_epoch_id": EPOCH, "subscription_generation": 1}
    forecast = {"symbol": symbol, "horizon_sessions": horizon, "status": "RESEARCH_ONLY",
        "as_of_utc": now.isoformat(), "predicted_close": 7010.0, "selected_method": "test_v1",
        "target_session_date": now.date().isoformat(), "target_close_utc": (now + timedelta(hours=2)).isoformat(),
        "forecast_id": "f" * 64, "reasons": [],
        "evidence": {**identity, "spot": 7000.0, "source": "databento_opra_put_call_parity",
                     "source_timestamp_utc": now.isoformat()},
        "candidates": [{"method_id": "last_price_v1", "status": "RESEARCH_ONLY", "predicted_close": 7000.0, "reasons": []},
                       {"method_id": "thin_method", "status": "ABSTAIN", "predicted_close": 1234, "reasons": ["THIN"]}]}
    return {"schema_version": "marketpin-market-research.v1", "status": "RESEARCH_ONLY", "reasons": [],
        "as_of_utc": now.isoformat(), "runtime_context": identity, "horizon_sessions": horizon,
        "publication_gate": {"schema_version": "marketpin-forecast-publication-gate.v1", "status": "ELIGIBLE",
            "checked_at_utc": now.isoformat(), "runtime_context": identity,
            "symbols": {symbol: {"status": "ELIGIBLE", "source_forecast_id": forecast["forecast_id"], "reasons": [],
                "valid_until_utc": (now+timedelta(seconds=15)).isoformat()}}},
        "requested_symbols": [symbol], "symbols": {symbol: {"forecast": forecast, "history_availability": {"reasons": []}}}}


def receipt(now=NOW):
    return {"health_before": health(now), "health_after": health(now), "selections": {"SPX": 0, "NDX": 0},
            "research": {s: payload(s, now=now) for s in ("SPX", "NDX")}, "failures": []}


def test_good_and_abstaining_candidates_are_distinct():
    rows = demo_rows(receipt(), now=NOW)
    assert len(rows) == 2 and all(row["status"] == "RESEARCH_ONLY" for row in rows)
    assert rows[0]["reference"] == 7000 and rows[0]["predicted_close"] == 7010
    assert rows[0]["candidates"][1]["Research estimate"] is None


def test_experimental_candidate_retains_holdout_limit_and_cannot_replace_selected_forecast():
    data = receipt()
    data["research"]["SPX"]["symbols"]["SPX"]["forecast"]["candidates"].append({
        "method_id": "eod_robust_trend_v1", "status": "RESEARCH_ONLY", "predicted_close": 7100,
        "prospective_only": True, "reasons": [],
        "limitation": "Retrospective diagnostic holdout did not beat baseline; prospective research comparison only",
    })
    row = demo_rows(data, now=NOW)[0]
    assert row["predicted_close"] == 7010
    assert row["candidates"][-1]["Use"] == "Experimental comparison; not selected"
    assert "did not beat baseline" in row["candidates"][-1]["Evidence limitation"]


@pytest.mark.parametrize("change,reason", [
    (lambda r: r["health_after"].update(subscription_generation=2), "RUNTIME_CHANGED_DURING_READ"),
    (lambda r: r["health_after"].update(sampled_at_utc=(NOW-timedelta(seconds=16)).isoformat()), "LIVE_HEALTH_STALE_OR_UNKNOWN"),
    (lambda r: r["health_after"].update(runtime_context_stable=False), "LIVE_RUNTIME_IDENTITY_UNVERIFIED"),
    (lambda r: r["health_after"].update(stream_connected=False), "LIVE_TRANSPORT_NOT_READY"),
    (lambda r: r["health_after"]["processing_clock_telemetry"].update(status="unknown"), "LIVE_PROCESSING_CLOCK_NOT_READY"),
])
def test_global_invalidity_hides_forecast_reference_and_candidates(change, reason):
    data = receipt()
    change(data)
    rows = demo_rows(data, now=NOW)
    assert all(r["predicted_close"] is None and r["reference"] is None for r in rows)
    assert reason in rows[0]["reasons"]
    assert all(c["Research estimate"] is None for r in rows for c in r["candidates"])


def test_one_symbol_failure_does_not_hide_healthy_other():
    data = receipt()
    data["health_before"]["symbol_status"]["NDX"]["usable_for_prediction"] = False
    rows = demo_rows(data, now=NOW)
    assert rows[0]["predicted_close"] == 7010
    assert rows[1]["predicted_close"] is None
    assert "LIVE_SYMBOL_EVIDENCE_UNVERIFIED" in rows[1]["reasons"]


@pytest.mark.parametrize("field", ["evidence", "runtime_context"])
def test_forecast_exact_runtime_binding_required(field):
    data = receipt()
    target = data["research"]["SPX"]
    target = target["symbols"]["SPX"]["forecast"]["evidence"] if field == "evidence" else target[field]
    target["subscription_epoch_id"] = "b" * 64
    rows = demo_rows(data, now=NOW)
    assert rows[0]["predicted_close"] is None
    assert "FORECAST_RUNTIME_BINDING_MISMATCH" in rows[0]["reasons"]
    assert rows[1]["predicted_close"] == 7010


def test_abstention_and_future_horizon_never_fall_back_to_eod():
    data = receipt()
    data["research"]["SPX"]["symbols"]["SPX"]["forecast"]["status"] = "ABSTAIN"
    data["selections"]["NDX"] = 1
    rows = demo_rows(data, now=NOW)
    assert all(r["predicted_close"] is None for r in rows)
    assert "FORECAST_HORIZON_OR_SYMBOL_MISMATCH" in rows[1]["reasons"]


def test_source_stale_hides_even_when_runtime_healthy():
    data = receipt()
    data["research"]["SPX"]["symbols"]["SPX"]["forecast"]["evidence"]["source_timestamp_utc"] = (NOW-timedelta(seconds=121)).isoformat()
    assert demo_rows(data, now=NOW)[0]["predicted_close"] is None


def test_closed_session_cannot_republish_a_retained_live_forecast():
    data = receipt()
    data["health_after"]["subscription_session_state"] = "post_close"
    rows = demo_rows(data, now=NOW)
    assert all(row["predicted_close"] is None and row["reference"] is None for row in rows)
    assert all("MARKET_SESSION_CLOSED" in row["reasons"] for row in rows)


def test_message_advancement_requires_ordered_same_identity_and_no_counter_reset():
    first = receipt()
    second = copy.deepcopy(first)
    assert message_delta(None, second) is None
    assert message_delta(first, second) is None
    second["health_after"]["sampled_at_utc"] = (NOW + timedelta(seconds=15)).isoformat()
    second["health_after"]["messages_received"] = 110
    assert message_delta(first, second) == {"messages": 10, "seconds": 15}
    second["health_after"]["messages_received"] = 99
    assert message_delta(first, second) is None
    second["health_after"]["messages_received"] = 110
    second["health_after"]["subscription_generation"] = 2
    assert message_delta(first, second) is None


def test_collector_only_loopback_gets_and_keeps_partial_success():
    now = datetime.now(timezone.utc)
    seen = []
    class Response:
        def __init__(self, body): self.body = body
        def raise_for_status(self): pass
        def json(self): return self.body
    def get(url, **kwargs):
        seen.append((url, kwargs))
        if "health/live" in url:
            return Response(health(now))
        symbol = kwargs["params"]["symbols"]
        if symbol == "NDX":
            raise requests.Timeout()
        return Response(payload(symbol, now=now))
    data = collect_demo({"SPX": 0, "NDX": 0}, requester=get, clock=lambda: now)
    assert len(seen) == 4 and all(url.startswith("http://127.0.0.1:") for url, _ in seen)
    assert all(options["timeout"] == 3 for _, options in seen)
    assert demo_rows(data, now=now)[0]["predicted_close"] == 7010
    assert demo_rows(data, now=now)[1]["predicted_close"] is None
    assert data["failures"] == [{"scope": "NDX", "code": "RESEARCH_READ_OR_SCHEMA_FAILED"}]


def test_render_smoke_opt_in_and_stop_no_automatic_network(monkeypatch):
    from streamlit.testing.v1 import AppTest
    import app.services.live_api_demo as demo
    calls = []
    def collect(selections):
        calls.append(dict(selections))
        result = receipt(datetime.now(timezone.utc))
        result["completed_at_utc"] = datetime.now(timezone.utc).isoformat()
        return result
    monkeypatch.setattr(demo, "collect_demo", collect)
    ui = AppTest.from_string("from app.services.live_api_demo import render_live_api_demo\nrender_live_api_demo()").run()
    assert not ui.exception and not calls
    ui.button(key="live_api_demo_refresh").click().run()
    assert not ui.exception and len(calls) == 1 and len(ui.metric) == 4
    ui.toggle(key="live_api_demo_auto").set_value(True).run()
    assert not ui.exception and len(calls) == 2
    ui.toggle(key="live_api_demo_auto").set_value(False).run()
    assert not ui.exception and len(calls) == 2
    ui.run()
    assert not ui.exception and len(calls) == 2


def test_baseline_is_prominent_in_main_metric(monkeypatch):
    from streamlit.testing.v1 import AppTest
    import app.services.live_api_demo as demo
    def collect(selections):
        now = datetime.now(timezone.utc)
        result = receipt(now)
        result["completed_at_utc"] = now.isoformat()
        for symbol in selections:
            result["research"][symbol]["symbols"][symbol]["forecast"]["selected_method"] = "last_price_v1"
        return result
    monkeypatch.setattr(demo, "collect_demo", collect)
    ui = AppTest.from_string("from app.services.live_api_demo import render_live_api_demo\nrender_live_api_demo()").run()
    ui.button(key="live_api_demo_refresh").click().run()
    assert not ui.exception
    assert sum(metric.label == "No-change baseline only (index points)" for metric in ui.metric) == 2
    assert any("does not demonstrate predictive skill" in item.value for item in ui.info)
