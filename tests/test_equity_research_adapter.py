from datetime import datetime, timedelta, timezone

import pytest

from backend.equity_context import EquityContextTracker
from backend.market_research_service import MarketResearchService

NOW = datetime(2026, 9, 10, 17, 0, tzinfo=timezone.utc)


def _run(tmp_path, monkeypatch, tracker):
    captured = []
    def evaluate(symbol, rows, **kwargs):
        captured.append(rows)
        return {"status": "ABSTAIN", "predicted_close": None}
    base = {"symbol": "SPY", "source": "opra", "price": 600, "valid": True}
    monkeypatch.setattr("backend.market_research_service.load_research_history", lambda *args, **kwargs: ({"SPY": [base]}, []))
    monkeypatch.setattr("backend.forecast_horizons.evaluate_forecast", evaluate)
    monkeypatch.setattr("backend.directional_shift.evaluate_directional_shift", lambda *args, **kwargs: {"status": "ABSTAIN"})
    service = MarketResearchService(tmp_path / "source.db", tmp_path / "journal.db", symbols=["SPY"],
                                   context_reader=lambda: {"runtime_context_stable": True}, equity_tracker=tracker)
    service.collect_once(now=NOW)
    return captured[0]


def _tracker(*, enabled=True, observed=NOW, start=None):
    tracker = EquityContextTracker(symbols=("SPY",), enabled=enabled)
    tracker.ingest([{"symbol": "SPY", "ts_event": start or NOW - timedelta(minutes=1),
                     "open": 610, "high": 612, "low": 609, "close": 611, "volume": 100}], observed_at=observed)
    return tracker


@pytest.mark.parametrize("kind", ["disabled", "stale", "entitlement_denied", "not_yet_available"])
def test_unusable_equities_do_not_displace_retained_option_evidence(tmp_path, monkeypatch, kind):
    tracker = _tracker(enabled=kind != "disabled", start=NOW - timedelta(minutes=10) if kind == "stale" else None,
                       observed=NOW + timedelta(minutes=1) if kind == "not_yet_available" else NOW)
    if kind == "entitlement_denied":
        tracker._failure = "ENTITLEMENT_UNAVAILABLE"
    rows = _run(tmp_path, monkeypatch, tracker)
    assert rows[0]["source"] == "opra"


def test_fresh_observed_etf_bars_replace_only_same_symbol_reference_at_asof(tmp_path, monkeypatch):
    rows = _run(tmp_path, monkeypatch, _tracker())
    assert rows[0]["source_kind"] == "observed_etf_bar"
    assert rows[0]["symbol"] == "SPY"
    assert rows[0]["price"] == 611
    assert rows[0]["timestamp_utc"] == NOW.isoformat()
    assert rows[0]["available_at_utc"] == NOW.isoformat()
