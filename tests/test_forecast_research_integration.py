import hashlib
import json
import sqlite3
from datetime import datetime, timedelta, timezone

from fastapi.testclient import TestClient

from backend.forecast_repository import load_research_history
from backend.market_research_service import MarketResearchService
from backend.research_api import create_research_app
from app.services.forecast_research_view import fetch_forecasts, forecast_rows

NOW = datetime(2026, 9, 10, 17, 0, tzinfo=timezone.utc)
EPOCH = "a" * 64


def source_db(tmp_path):
    path = tmp_path / "source.db"
    with sqlite3.connect(path) as c:
        c.execute("CREATE TABLE market_structure_observations (observation_id TEXT, symbol TEXT, trading_date TEXT, "
                  "source_timestamp_utc TEXT, captured_at_utc TEXT, reference_price REAL, provider TEXT, "
                  "spot_source TEXT, subscription_epoch_id TEXT, subscription_generation INTEGER, validation_status TEXT, "
                  "universe_sha256 TEXT, same_day_profile_available INTEGER, primary_expiration TEXT, gamma_pin REAL, net_gex REAL)")
        for i in range(40):
            stamp = NOW - timedelta(minutes=39-i)
            c.execute("INSERT INTO market_structure_observations VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (str(i), "SPX", "2026-09-10", stamp.replace(tzinfo=None).isoformat(" "),
                 (stamp+timedelta(seconds=1)).replace(tzinfo=None).isoformat(" "), 6000+i,
                 "databento", "databento_opra_put_call_parity", EPOCH, 1, "valid", "b"*64, 1, "2026-09-10", 6040, 12))
    return path


def context():
    return {"subscription_epoch_id": EPOCH, "subscription_generation": 1, "handoff_status": "active",
            "runtime_context_stable": True, "symbol_status": {"SPX": {"usable_for_prediction": True}}}


def test_repository_read_only_point_in_time_and_runtime_binding(tmp_path):
    path = source_db(tmp_path)
    before = hashlib.sha256(path.read_bytes()).hexdigest()
    histories, errors = load_research_history(path, ["SPX"], as_of_utc=NOW, runtime_context=context())
    assert not errors and len(histories["SPX"]) == 39  # latest not available until NOW+1s
    assert histories["SPX"][-1]["valid"] is True
    assert hashlib.sha256(path.read_bytes()).hexdigest() == before
    changed = {**context(), "subscription_generation": 2}
    rows, _ = load_research_history(path, ["SPX"], as_of_utc=NOW, runtime_context=changed)
    assert all(not row["valid"] for row in rows["SPX"])


def test_service_end_to_end_journals_only_research_and_get_is_pure(tmp_path):
    path = source_db(tmp_path)
    journal = tmp_path / "research.db"
    service = MarketResearchService(path, journal, context_reader=context, symbols=["SPX", "SPY"])
    service.initialize_journal()
    state = service.collect_once(now=NOW)
    assert state["symbols"]["SPX"]["forecasts"]["0"]["status"] == "RESEARCH_ONLY"
    assert state["symbols"]["SPX"]["forecasts"]["3"]["status"] == "ABSTAIN"
    assert state["symbols"]["SPY"]["forecasts"]["0"]["predicted_close"] is None
    frozen = service._state
    # Pin read clock independent of test's wall date by serving captured state.
    service.snapshot = lambda: json.loads(json.dumps(frozen))
    before = journal.read_bytes()
    with TestClient(create_research_app(service, manage_lifecycle=False)) as client:
        result = client.get("/v1/research/forecasts", params={"symbols": "SPX,SPY", "horizon_sessions": 3})
        assert result.status_code == 200
        assert result.json()["symbols"]["SPX"]["forecast"]["horizon_sessions"] == 3
        assert client.get("/v1/research/forecasts?horizon_sessions=11").status_code == 422
        assert client.get("/v1/research/forecasts?symbols=UNKNOWN").status_code == 422
    assert journal.read_bytes() == before


def test_context_transition_abstains_and_no_production_db_write(tmp_path):
    path = source_db(tmp_path)
    states = iter([context(), {**context(), "subscription_generation": 2}])
    service = MarketResearchService(path, tmp_path / "research.db", context_reader=lambda: next(states), symbols=["SPX"])
    service.initialize_journal()
    result = service.collect_once(now=NOW)
    assert result["symbols"]["SPX"]["forecasts"]["0"]["predicted_close"] is None
    assert result["symbols"]["SPX"]["directional_shift"]["status"] == "ABSTAIN"
    assert "RUNTIME_CONTEXT_CHANGED_OR_UNVERIFIED" in result["reasons"]


def test_no_runtime_context_cannot_turn_retained_prices_live(tmp_path):
    path = source_db(tmp_path)
    def unavailable():
        raise OSError("offline")
    service = MarketResearchService(path, tmp_path / "research.db", context_reader=unavailable, symbols=["SPX"])
    service.initialize_journal()
    result = service.collect_once(now=NOW)
    assert all(f["predicted_close"] is None for f in result["symbols"]["SPX"]["forecasts"].values())


def test_per_symbol_horizon_requests_and_abstention_display():
    requests_seen = []
    class Response:
        def raise_for_status(self):
            pass
        def json(self):
            return {"schema_version": "marketpin-market-research.v1", "symbols": {}}
    def request(url, **kwargs):
        requests_seen.append(kwargs["params"])
        return Response()
    fetch_forecasts({"SPX": 0, "NDX": 3, "SPY": 3}, requester=request)
    assert requests_seen == [{"symbols": "SPX", "horizon_sessions": 0}, {"symbols": "NDX,SPY", "horizon_sessions": 3}]
    rows = forecast_rows({"symbols": {"SPX": {"forecast": {"status": "ABSTAIN", "predicted_close": 7000}}}})
    assert rows[0]["Research close"] is None


def test_expired_capture_hides_all_numbers(tmp_path):
    service = MarketResearchService(tmp_path / "source.db", tmp_path / "research.db", symbols=["SPX"])
    service._state = {"schema_version": "marketpin-market-research.v1", "status": "RESEARCH_ONLY", "reasons": [],
        "as_of_utc": (datetime.now(timezone.utc)-timedelta(seconds=121)).isoformat(),
        "symbols": {"SPX": {"forecasts": {"0": {"predicted_close": 7000, "candidates": [{"predicted_close": 7000}]}},
                            "directional_shift": {"status": "CONFIRMED_SHIFT", "direction": "UP"}}}}
    result = service.snapshot()
    assert result["status"] == "STALE"
    assert result["symbols"]["SPX"]["forecasts"]["0"]["predicted_close"] is None
    assert result["symbols"]["SPX"]["forecasts"]["0"]["candidates"] == []


def test_ui_saved_result_cannot_republish_expired_number():
    stamp = (datetime.now(timezone.utc)-timedelta(seconds=121)).isoformat()
    rows = forecast_rows({"symbols": {"SPX": {"forecast": {
        "status": "RESEARCH_ONLY", "predicted_close": 7000, "as_of_utc": stamp},
        "directional_shift": {"status": "CONFIRMED_SHIFT", "direction": "UP"}}}})
    assert rows[0]["Status"] == "STALE"
    assert rows[0]["Research close"] is None and rows[0]["Direction"] is None


def test_symbol_health_flip_without_generation_change_abstains(tmp_path):
    path = source_db(tmp_path)
    after = context()
    after["symbol_status"]["SPX"]["usable_for_prediction"] = False
    contexts = iter([context(), after])
    service = MarketResearchService(path, tmp_path / "research.db", context_reader=lambda: next(contexts), symbols=["SPX"])
    service.initialize_journal()
    result = service.collect_once(now=NOW)
    assert result["symbols"]["SPX"]["directional_shift"]["status"] == "ABSTAIN"
    assert all(forecast["predicted_close"] is None for forecast in result["symbols"]["SPX"]["forecasts"].values())
    assert "SPX:LIVE_SYMBOL_EVIDENCE_UNVERIFIED" in result["reasons"]


def test_initial_unstable_context_cannot_gain_retrospective_validity(tmp_path):
    path = source_db(tmp_path)
    contexts = iter([{**context(), "runtime_context_stable": False}, context()])
    service = MarketResearchService(path, tmp_path / "research.db", context_reader=lambda: next(contexts), symbols=["SPX"])
    service.initialize_journal()
    result = service.collect_once(now=NOW)
    assert result["symbols"]["SPX"]["forecasts"]["0"]["predicted_close"] is None
    assert "RUNTIME_CONTEXT_CHANGED_OR_UNVERIFIED" in result["reasons"]


def test_future_capture_after_clock_rollback_abstains(tmp_path):
    service = MarketResearchService(tmp_path / "source.db", tmp_path / "research.db", symbols=["SPX"])
    service._state = {"schema_version": "marketpin-market-research.v1", "status": "RESEARCH_ONLY", "reasons": [],
        "as_of_utc": (datetime.now(timezone.utc)+timedelta(seconds=30)).isoformat(),
        "symbols": {"SPX": {"forecasts": {"0": {"predicted_close": 7000, "candidates": [{"predicted_close": 7000}]}},
                            "directional_shift": {"status": "CONFIRMED_SHIFT", "direction": "UP"}}}}
    result = service.snapshot()
    assert result["status"] == "UNAVAILABLE"
    assert result["symbols"]["SPX"]["forecasts"]["0"]["predicted_close"] is None
    assert result["symbols"]["SPX"]["directional_shift"]["direction"] is None
    assert "RESEARCH_CAPTURE_TIMESTAMP_IN_FUTURE" in result["reasons"]
    # Snapshot invalidation is read-only and leaves retained evidence intact.
    assert service._state["symbols"]["SPX"]["forecasts"]["0"]["predicted_close"] == 7000


def _eod_source_db(tmp_path, closes):
    """Source db with an eod_closes table: closes = [(symbol, date, close, source), ...]."""
    path = tmp_path / "eod_source.db"
    with sqlite3.connect(path) as c:
        c.execute("CREATE TABLE eod_closes (id INTEGER PRIMARY KEY, symbol TEXT NOT NULL, trading_date TEXT NOT NULL, "
                  "official_close REAL NOT NULL, source TEXT, ingested_at_utc TEXT NOT NULL, "
                  "UNIQUE(symbol, trading_date))")
        for i, (sym, day, close, src) in enumerate(closes):
            c.execute("INSERT INTO eod_closes VALUES (?,?,?,?,?,?)",
                      (i, sym, day, close, src, "2026-09-14T00:00:00"))
    return path


def test_trend_endpoint_serves_metrics_from_close_evidence(tmp_path):
    closes = [("SPX", f"2026-09-{d:02d}", close, "databento")
              for d, close in enumerate((100.0, 102.0, 104.0, 103.0, 106.0), start=1)]
    service = MarketResearchService(_eod_source_db(tmp_path, closes), tmp_path / "research.db", symbols=["SPX"])
    with TestClient(create_research_app(service, manage_lifecycle=False)) as client:
        result = client.get("/v1/research/trend", params={"symbol": "SPX", "lookback_days": 30})
        assert result.status_code == 200
        body = result.json()
        assert body["schema_version"] == "marketpin-market-research.v1"
        assert body["sessions"] == 5
        assert len(body["trend"]["prices"]) == 5
        metrics = body["trend"]["metrics"]
        assert metrics["trend_direction"] == "up"          # +6% over the window
        assert abs(metrics["slope_pct"] - 6.0) < 1e-6
        assert -1.0 <= metrics["momentum_score"] <= 1.0


def test_trend_endpoint_rejects_untracked_and_thin_evidence(tmp_path):
    closes = [("SPX", "2026-09-01", 100.0, "databento")]
    service = MarketResearchService(_eod_source_db(tmp_path, closes), tmp_path / "research.db", symbols=["SPX"])
    with TestClient(create_research_app(service, manage_lifecycle=False)) as client:
        assert client.get("/v1/research/trend", params={"symbol": "VIX"}).status_code == 422
        assert client.get("/v1/research/trend", params={"symbol": "SPX"}).status_code == 422  # one session only
        assert client.get("/v1/research/trend", params={"symbol": "SPX", "lookback_days": 0}).status_code == 422


def test_trend_endpoint_excludes_unit_test_rows(tmp_path):
    closes = [("SPX", "2026-09-01", 100.0, "databento"),
              ("SPX", "2026-09-02", 110.0, "databento"),
              ("SPX", "2026-09-03", 999.0, "unit-test")]
    service = MarketResearchService(_eod_source_db(tmp_path, closes), tmp_path / "research.db", symbols=["SPX"])
    with TestClient(create_research_app(service, manage_lifecycle=False)) as client:
        body = client.get("/v1/research/trend", params={"symbol": "SPX"}).json()
        assert body["sessions"] == 2
        assert abs(body["trend"]["metrics"]["slope_pct"] - 10.0) < 1e-6
