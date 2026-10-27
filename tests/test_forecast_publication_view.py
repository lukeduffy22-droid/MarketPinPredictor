"""The two research surfaces obey the same request-time publication authority."""
from copy import deepcopy
from datetime import timedelta

import pytest

from app.services.forecast_research_view import forecast_rows, eod_comparison_rows, fetch_forecasts
from app.services.live_api_demo import demo_rows
from tests.test_live_api_demo import NOW, receipt


def legacy_payload(data):
    result = {"symbols": {}, "selections": data["selections"]}
    for symbol, capture in data["research"].items():
        item = deepcopy(capture["symbols"][symbol])
        item["capture"] = {k: capture.get(k) for k in ("status", "as_of_utc", "runtime_context", "reasons")}
        item["publication_gate"] = deepcopy(capture.get("publication_gate"))
        result["symbols"][symbol] = item
    return result


@pytest.mark.parametrize("mutate,code", [
    (lambda p: p.pop("publication_gate"), "CURRENT_PUBLICATION_GATE_UNAVAILABLE"),
    (lambda p: p["publication_gate"].update(checked_at_utc=(NOW-timedelta(seconds=16)).isoformat()), "CURRENT_PUBLICATION_GATE_EXPIRED"),
    (lambda p: p["publication_gate"].update(checked_at_utc=(NOW+timedelta(seconds=1)).isoformat()), "CURRENT_PUBLICATION_GATE_EXPIRED"),
    (lambda p: p["publication_gate"]["symbols"]["NDX"].update(status="ABSTAIN", reasons=["PROCESSING_CLOCK_NOT_READY"]), "PROCESSING_CLOCK_NOT_READY"),
    (lambda p: p["publication_gate"]["symbols"]["NDX"].update(source_forecast_id="other"), "PUBLICATION_FORECAST_BINDING_MISMATCH"),
    (lambda p: p["publication_gate"]["runtime_context"].update(subscription_generation=2), "PUBLICATION_RUNTIME_BINDING_MISMATCH"),
    (lambda p: p["publication_gate"]["symbols"]["NDX"].update(valid_until_utc=NOW.isoformat()), "CURRENT_PUBLICATION_GATE_EXPIRED"),
    (lambda p: p["publication_gate"]["symbols"]["NDX"].pop("valid_until_utc"), "CURRENT_PUBLICATION_GATE_EXPIRY_UNAVAILABLE"),
    (lambda p: p["publication_gate"]["symbols"]["NDX"].update(valid_until_utc="bad"), "CURRENT_PUBLICATION_GATE_TIMESTAMP_INVALID"),
    (lambda p: p["symbols"]["NDX"]["forecast"].update(selected_method=None), "FORECAST_METHOD_UNAVAILABLE"),
])
def test_legacy_and_demo_both_fail_closed_without_rewriting_receipts(mutate, code):
    data = deepcopy(receipt())
    # Break fixture object aliasing so gate edits cannot change source identity.
    data["research"]["NDX"]["publication_gate"] = deepcopy(data["research"]["NDX"]["publication_gate"])
    mutate(data["research"]["NDX"])
    before = deepcopy(data)
    old = {r["Symbol"]: r for r in forecast_rows(legacy_payload(data), now=NOW)}
    new = {r["symbol"]: r for r in demo_rows(data, now=NOW)}
    assert old["NDX"]["Research close"] is None and new["NDX"]["predicted_close"] is None
    assert code in old["NDX"]["Reason"] and code in new["NDX"]["reasons"]
    assert old["SPX"]["Research close"] == 7010 and new["SPX"]["predicted_close"] == 7010
    assert data == before


def test_independent_symbol_expiry_overrides_global_summary():
    data = receipt()
    for symbol, capture in data["research"].items():
        capture["publication_gate"].update(status="ABSTAIN", valid_until_utc=NOW.isoformat())
        capture["publication_gate"]["symbols"][symbol]["valid_until_utc"] = (NOW+timedelta(seconds=10)).isoformat()
    assert all(row["Research close"] == 7010 for row in forecast_rows(legacy_payload(data), now=NOW))
    assert all(row["predicted_close"] == 7010 for row in demo_rows(data, now=NOW))


def test_no_change_baseline_label_is_shared_and_old_gate_never_enables_it():
    data = receipt()
    data["research"]["NDX"]["symbols"]["NDX"]["forecast"]["selected_method"] = "last_price_v1"
    rows = forecast_rows(legacy_payload(data), now=NOW)
    assert rows[1]["Estimate type"] == "No-change baseline only"
    data["research"]["NDX"].pop("publication_gate")
    row = forecast_rows(legacy_payload(data), now=NOW)[1]
    assert row["Research close"] is None and row["Estimate type"] == "Unavailable"


def test_rejected_gate_hides_direction_and_comparisons_together():
    data = legacy_payload(receipt())
    ndx = data["symbols"]["NDX"]
    ndx["directional_shift"] = {"status": "CONFIRMED_SHIFT", "direction": "UP"}
    ndx["eod_learning"] = {"status": "RESEARCH_ONLY", "source_forecast_id": ndx["forecast"]["forecast_id"],
        "as_of_utc": NOW.isoformat(), "comparisons": [{"method_id": "last_price_v1", "current_estimate": 7010}]}
    ndx["publication_gate"]["symbols"]["NDX"].update(status="ABSTAIN", reasons=["SYMBOL_INVALID"])
    row = forecast_rows(data, now=NOW)[1]
    assert row["Direction"] is None and row["Research close"] is None
    assert not eod_comparison_rows(data, now=NOW)


def test_fetch_preserves_gate_and_runtime_without_reusing_historical_forecast():
    capture = receipt()["research"]["SPX"]
    capture["symbols"]["SPX"]["retained_forecast"] = deepcopy(capture["symbols"]["SPX"]["forecast"])
    capture["symbols"]["SPX"]["forecast"]["status"] = "ABSTAIN"
    capture["symbols"]["SPX"]["forecast"]["predicted_close"] = None
    class Response:
        def raise_for_status(self): pass
        def json(self): return deepcopy(capture)
    fetched = fetch_forecasts({"SPX": 0}, requester=lambda *a, **k: Response())
    assert not fetched["failures"]
    assert fetched["symbols"]["SPX"]["publication_gate"] == capture["publication_gate"]
    assert fetched["symbols"]["SPX"]["capture"]["runtime_context"] == capture["runtime_context"]
    assert fetched["symbols"]["SPX"]["retained_forecast"]["predicted_close"] == 7010
    assert forecast_rows(fetched, now=NOW)[0]["Research close"] is None


@pytest.mark.parametrize("failure", [None, "clock", "ndx", "expiry"])
def test_real_backend_publication_contract_drives_both_views(failure):
    from backend.forecast_publication_gate import evaluate_publication_gate, project_publication_item
    from tests.test_forecast_publication_gate import NOW as checked, forecast, health

    issued = {symbol: forecast(symbol) for symbol in ("SPX", "NDX")}
    original = deepcopy(issued)
    context = health()
    context["provider"] = "databento"
    if failure == "clock":
        context["processing_clock_telemetry"]["status"] = "unknown"
    elif failure == "ndx":
        context["symbol_status"]["NDX"]["usable_for_prediction"] = False
    gate = evaluate_publication_gate(issued, context, checked_at_utc=checked)
    data = receipt(checked)
    data.update(health_before=deepcopy(context), health_after=deepcopy(context))
    for symbol, capture in data["research"].items():
        capture["publication_gate"] = deepcopy(gate)
        capture["symbols"][symbol] = project_publication_item(
            {}, issued[symbol], issued[symbol], gate["symbols"][symbol])
    displayed_at = checked+timedelta(seconds=15) if failure == "expiry" else checked
    legacy = {r["Symbol"]: r for r in forecast_rows(legacy_payload(data), now=displayed_at)}
    demo = {r["symbol"]: r for r in demo_rows(data, now=displayed_at)}
    for symbol in issued:
        expected = None if failure in ("clock", "expiry") or (failure == "ndx" and symbol == "NDX") else 7700
        assert legacy[symbol]["Research close"] == demo[symbol]["predicted_close"] == expected
        if expected is not None:
            assert legacy[symbol]["Estimate type"] == "No-change baseline only"
        assert data["research"][symbol]["symbols"][symbol]["retained_forecast"] == original[symbol]
    assert issued == original
