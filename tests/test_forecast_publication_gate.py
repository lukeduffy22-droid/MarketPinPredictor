from copy import deepcopy
from datetime import datetime, timedelta, timezone
import json
import sqlite3

from fastapi.testclient import TestClient
import pytest

from backend.forecast_horizons import evaluate_forecast
from backend.forecast_publication_gate import evaluate_publication_gate, project_publication_item
from backend.market_research_service import MarketResearchService
from backend.research_api import create_research_app

NOW = datetime(2026, 10, 8, 19, 0, tzinfo=timezone.utc)
EPOCH = "a" * 64


@pytest.fixture(autouse=True)
def isolated_database(tmp_path, monkeypatch):
    monkeypatch.setenv("DATABASE_URL", "sqlite:///" + (tmp_path / "unused.db").as_posix())


def forecast(symbol="SPX", *, source_age=10):
    result = evaluate_forecast(symbol, [{"symbol": symbol, "timestamp_utc": (NOW-timedelta(seconds=source_age)).isoformat(),
        "available_at_utc": (NOW-timedelta(seconds=source_age)).isoformat(), "spot": 7700,
        "source": "databento_opra_put_call_parity", "valid": True, "source_verified": True,
        "subscription_epoch_id": EPOCH, "subscription_generation": 1,
        "same_day_profile_available": True, "primary_expiration": "2026-10-08"}], as_of_utc=NOW, horizon_sessions=0)
    assert result["status"] == "RESEARCH_ONLY"
    return result


def health():
    detail = {"usable_for_prediction": True, "validation_is_valid": True, "is_stale": False,
              "validation_failure_reasons": [], "epoch_is_current": True, "generation_is_current": True,
              "subscription_epoch_id": EPOCH, "active_subscription_epoch_id": EPOCH,
              "subscription_generation": 1, "active_generation": 1, "fresh_quote_count": 20,
              "data_age_seconds": 10, "stale_after_seconds": 30}
    return {"sampled_at_utc": NOW.isoformat(), "subscription_epoch_id": EPOCH, "subscription_generation": 1,
            "runtime_context_stable": True, "handoff_status": "active", "transport_ready": True,
            "stream_connected": True, "stream_progressing": True,
            "processing_clock_telemetry": {"status": "synchronized", "replay_pending": False},
            "symbol_status": {"SPX": deepcopy(detail), "NDX": deepcopy(detail)}}


def test_same_generation_clock_flip_revokes_immutable_forecast():
    original, context = forecast(), health()
    before = deepcopy(original)
    gate = evaluate_publication_gate({"SPX": original}, context, checked_at_utc=NOW)
    assert gate["symbols"]["SPX"]["status"] == "ELIGIBLE"
    context["processing_clock_telemetry"]["status"] = "unsynchronized"
    gate = evaluate_publication_gate({"SPX": original}, context, checked_at_utc=NOW)
    assert gate["symbols"]["SPX"]["reasons"] == ["CURRENT_CLOCK_NOT_SYNCHRONIZED"]
    item = {"directional_shift": {"status": "CONFIRMED_SHIFT", "direction": "UP"},
            "eod_learning": {"comparisons": [{"current_estimate": 7700, "historical_mae": 11}],
                             "adaptive": {"predicted_close": 7700}}}
    projected = project_publication_item(item, original, original, gate["symbols"]["SPX"])
    assert projected["retained_forecast"] == before == original
    assert projected["forecast"]["predicted_close"] is None and projected["forecast"]["candidates"] == []
    assert projected["directional_shift"]["direction"] is None
    assert projected["eod_learning"]["comparisons"] == [{"current_estimate": None, "historical_mae": 11}]
    assert item["directional_shift"]["direction"] == "UP"


@pytest.mark.parametrize("mutate,code", [
    (lambda h: h.update(sampled_at_utc=(NOW-timedelta(seconds=16)).isoformat()), "CURRENT_RUNTIME_HEALTH_UNAVAILABLE_OR_STALE"),
    (lambda h: h.update(sampled_at_utc=NOW.replace(tzinfo=None).isoformat()), "CURRENT_RUNTIME_HEALTH_UNAVAILABLE_OR_STALE"),
    (lambda h: h.update(stream_progressing=False), "CURRENT_TRANSPORT_NOT_READY"),
    (lambda h: h.update(subscription_generation=2), "FORECAST_RUNTIME_IDENTITY_MISMATCH"),
    (lambda h: h.update(runtime_context_stable=False), "CURRENT_RUNTIME_IDENTITY_UNVERIFIED"),
    (lambda h: h["processing_clock_telemetry"].update(replay_pending=True), "CURRENT_CLOCK_NOT_SYNCHRONIZED"),
    (lambda h: h["symbol_status"]["SPX"].update(data_age_seconds=30), "CURRENT_SYMBOL_QUOTES_UNVERIFIED"),
    (lambda h: h["symbol_status"]["SPX"].update(active_generation=True), "CURRENT_SYMBOL_IDENTITY_MISMATCH"),
    (lambda h: h["symbol_status"]["SPX"].update(validation_failure_reasons=["CLOCK_INVALID"]), "CURRENT_SYMBOL_INELIGIBLE"),
])
def test_current_authority_fails_closed(mutate, code):
    context = health()
    mutate(context)
    gate = evaluate_publication_gate({"SPX": forecast()}, context, checked_at_utc=NOW)
    assert code in gate["symbols"]["SPX"]["reasons"]
    assert gate["symbols"]["SPX"]["valid_until_utc"] is None


def test_independent_symbol_deadlines_and_source_age_are_hard_caps():
    context = health()
    context["symbol_status"]["NDX"].update(data_age_seconds=29)
    gate = evaluate_publication_gate({"SPX": forecast(source_age=118), "NDX": forecast("NDX")}, context, checked_at_utc=NOW)
    assert gate["symbols"]["SPX"]["valid_until_utc"] == (NOW+timedelta(seconds=2)).isoformat()
    assert gate["symbols"]["NDX"]["valid_until_utc"] == (NOW+timedelta(seconds=1)).isoformat()
    context["symbol_status"]["NDX"].update(usable_for_prediction=False)
    gate = evaluate_publication_gate({"SPX": forecast(), "NDX": forecast("NDX")}, context, checked_at_utc=NOW)
    assert gate["status"] == "ABSTAIN" and gate["symbols"]["SPX"]["status"] == "ELIGIBLE"


@pytest.mark.parametrize("field,value", [("predicted_close", 9999), ("selected_method", "missing")])
def test_selected_number_must_match_digest_bound_candidate(field, value):
    original = forecast()
    original[field] = value
    gate = evaluate_publication_gate({"SPX": original}, health(), checked_at_utc=NOW)
    assert "ISSUED_SELECTION_UNVERIFIED" in gate["symbols"]["SPX"]["reasons"]


def test_api_one_health_read_preserves_retained_evidence_and_get_is_query_only(tmp_path, monkeypatch):
    import backend.research_api as api
    class FixedDatetime(datetime):
        @classmethod
        def now(cls, tz=None): return NOW
    monkeypatch.setattr(api, "datetime", FixedDatetime)
    calls = []
    context = health()
    def reader():
        calls.append(1)
        return deepcopy(context)
    service = MarketResearchService(tmp_path/"source.db", tmp_path/"journal.db", context_reader=reader, symbols=["SPX", "NDX"])
    service.initialize_journal()
    service._state = {"schema_version": "marketpin-market-research.v1", "status": "RESEARCH_ONLY",
        "reasons": [], "as_of_utc": NOW.isoformat(), "runtime_context": {"subscription_epoch_id": EPOCH, "subscription_generation": 1},
        "symbols": {s: {"forecasts": {"0": forecast(s)}, "directional_shift": {"status": "ABSTAIN"},
                        "history_by_horizon": {"0": {"status": "AVAILABLE", "reasons": []}}} for s in service.symbols}}
    service.snapshot = lambda: deepcopy(service._state)
    before = deepcopy(service._state)
    journal_before = service.journal_database.read_bytes()
    with TestClient(create_research_app(service, manage_lifecycle=False)) as client:
        first = client.get("/v1/research/forecasts").json()
        assert first["publication_gate"]["status"] == "ELIGIBLE" and len(calls) == 1
        context["processing_clock_telemetry"]["status"] = "unsynchronized"
        second = client.get("/v1/research/forecasts").json()
        assert len(calls) == 2
        assert second["symbols"]["SPX"]["forecast"]["predicted_close"] is None
        assert second["symbols"]["SPX"]["retained_forecast"] == first["symbols"]["SPX"]["forecast"]
        assert client.get("/v1/research/attempts").json()["attempts"] == []
    assert service._state == before and service.journal_database.read_bytes() == journal_before


def test_snapshot_retains_issuance_when_stopped(tmp_path):
    service = MarketResearchService(tmp_path/"source.db", tmp_path/"journal.db", symbols=["SPX"])
    original = forecast()
    service._state = {"as_of_utc": NOW.isoformat(), "reasons": [], "symbols": {
        "SPX": {"forecasts": {"0": original}, "directional_shift": {}}}}
    service._stop.set()
    result = service.snapshot(now=NOW)
    assert result["symbols"]["SPX"]["forecasts"]["0"]["predicted_close"] is None
    assert result["symbols"]["SPX"]["retained_forecasts"]["0"] == original
