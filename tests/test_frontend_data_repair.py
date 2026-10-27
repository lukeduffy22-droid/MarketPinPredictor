"""Offline repair acceptance: temporary DBs, in-process API and rendered Streamlit."""
from copy import deepcopy
from datetime import datetime, timedelta, timezone
import hashlib
import inspect
import json
import socket
import sqlite3
import sys

import pytest
import requests
from fastapi.testclient import TestClient
from streamlit.testing.v1 import AppTest

from app.services import forecast_research_view as view
from backend.forecast_horizons import inventory_daily_history, evaluate_forecast
from backend.market_research_service import MarketResearchService
from backend.market_universe import DEFAULT_TRACKED_SYMBOLS
from backend.research_api import create_research_app
from tests.test_forecast_horizons import AS_OF, daily_history, series


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    original_connect = socket.socket.connect
    def blocked(*args, **kwargs):
        raise AssertionError("Offline acceptance must not open a network connection")
    def connect(sock, address):
        # Windows implements asyncio's internal wakeup socketpair on loopback.
        # Permit that exact stdlib caller only, never application HTTP/provider I/O.
        if inspect.currentframe().f_back.f_code is socket.socketpair.__code__:
            return original_connect(sock, address)
        return blocked()
    monkeypatch.setattr(socket.socket, "connect", connect)
    monkeypatch.setattr(socket, "create_connection", blocked)


def group(symbols, horizon, *, stamp=None):
    stamp = stamp or datetime.now(timezone.utc).isoformat()
    identity = {"subscription_epoch_id": "a" * 64, "subscription_generation": 1}
    return {"schema_version": "marketpin-market-research.v1", "horizon_sessions": horizon,
            "requested_symbols": list(symbols), "as_of_utc": stamp, "status": "RESEARCH_ONLY", "reasons": [],
            "runtime_context": identity,
            "publication_gate": {"schema_version": "marketpin-forecast-publication-gate.v1", "status": "ELIGIBLE",
                "checked_at_utc": stamp, "runtime_context": identity,
                "symbols": {s: {"status": "ELIGIBLE", "source_forecast_id": f"fixture-{s}-{horizon}", "reasons": [],
                    "valid_until_utc": (datetime.fromisoformat(stamp)+timedelta(seconds=15)).isoformat()} for s in symbols}},
            "symbols": {s: {"forecast": {"symbol": s, "horizon_sessions": horizon, "as_of_utc": stamp,
                "status": "RESEARCH_ONLY" if horizon == 0 else "ABSTAIN", "predicted_close": 100 if horizon == 0 else None,
                "forecast_id": f"fixture-{s}-{horizon}", "selected_method": "fixture_method",
                "target_close_utc": (datetime.fromisoformat(stamp) + timedelta(hours=1)).isoformat(),
                "target_session_date": "2026-09-22", "reasons": [] if horizon == 0 else ["insufficient_verified_daily_history"],
                "evidence": {**identity, "source": "offline_fixture", "source_timestamp_utc": stamp}},
                "history_availability": {"qualified_return_samples": 3 if horizon else None,
                    "required_return_samples": 60 if horizon else None, "verified_daily_close_count": 13,
                    "reasons": []}, "directional_shift": {"status": "ABSTAIN"}}
                for s in symbols}}


class Response:
    def __init__(self, payload):
        self.payload = payload
    def raise_for_status(self):
        pass
    def json(self):
        return deepcopy(self.payload)


def requester(url, *, params, timeout):
    assert url == view.RESEARCH_URL + "/v1/research/forecasts"
    assert timeout == 3
    return Response(group(params["symbols"].split(","), params["horizon_sessions"]))


@pytest.mark.parametrize("error,code", [(requests.Timeout("secret"), "TIMEOUT"),
    (requests.ConnectionError("secret"), "TRANSPORT_ERROR"), (requests.HTTPError("secret"), "HTTP_ERROR")])
def test_transport_errors_are_structured_and_safe(error, code):
    def request(*args, **kwargs):
        raise error
    result = view.fetch_forecasts({"SPX": 0, "NDX": 5}, requester=request)
    assert [f["code"] for f in result["failures"]] == [code, code]
    assert "secret" not in json.dumps(result)


def test_decode_and_http_status():
    class BadResponse(Response):
        def json(self):
            raise ValueError("private body")
    assert view.fetch_forecasts({"SPX": 0}, requester=lambda *a, **k: BadResponse(None))["failures"][0]["code"] == "DECODE_ERROR"
    def fail(*args, **kwargs):
        response = requests.Response()
        response.status_code = 503
        raise requests.HTTPError(response=response)
    assert view.fetch_forecasts({"SPX": 0}, requester=fail)["failures"][0]["http_status"] == 503


@pytest.mark.parametrize("mutation", [lambda p: [], lambda p: {**p, "symbols": {}},
    lambda p: {**p, "horizon_sessions": 2}, lambda p: {**p, "requested_symbols": ["SPY"]},
    lambda p: {**p, "symbols": {"SPX": []}}, lambda p: {**p, "reasons": "bad"},
    lambda p: {**p, "symbols": {"SPX": {**p["symbols"]["SPX"], "forecast": {"symbol": "SPY"}}}}])
def test_malformed_or_misbound_groups_cannot_show_numbers(mutation):
    result = view.fetch_forecasts({"SPX": 0}, requester=lambda *a, **k: Response(mutation(group(["SPX"], 0))))
    assert not result["symbols"]
    assert result["failures"][0]["code"] == "SCHEMA_ERROR"


def test_mixed_horizons_partial_failure_and_units():
    def partial(url, **kwargs):
        if kwargs["params"]["horizon_sessions"] == 5:
            raise requests.Timeout()
        return requester(url, **kwargs)
    selection = {"SPX": 0, "SPY": 1, "NDX": 5, "QQQ": 10}
    result = view.fetch_forecasts(selection, requester=partial)
    assert result["selections"] == selection
    rows = {r["Symbol"]: r for r in view.forecast_rows(result)}
    assert rows["SPX"]["Research close"] == 100
    assert rows["SPX"]["Units"] == "Index points"
    assert rows["SPY"]["Units"] == "ETF USD/share"
    assert rows["NDX"]["Status"] == "REQUEST_FAILED"
    assert rows["QQQ"]["Qualified returns"] == 3 and rows["QQQ"]["Required returns"] == 60


@pytest.mark.parametrize("offset,reason", [(-121, "RESEARCH_CAPTURE_STALE"), (30, "RESEARCH_CAPTURE_TIMESTAMP_IN_FUTURE")])
def test_saved_results_cannot_become_current_after_time_change(offset, reason):
    now = datetime.now(timezone.utc)
    result = view.fetch_forecasts({"SPX": 0}, requester=lambda *a, **k: Response(group(["SPX"], 0, stamp=(now+timedelta(seconds=offset)).isoformat())))
    row = view.forecast_rows(result, now=now)[0]
    assert row["Research close"] is None and reason in row["Reason"]


@pytest.mark.parametrize("selection", [{"INVALID": 0}, {"SPX": -1}, {"SPY": 11}, {"SPY": True}, {"SPY": "1"}])
def test_bad_selection_never_calls_transport(selection):
    def forbidden(*a, **k):
        raise AssertionError("Invalid selection reached transport")
    assert view.fetch_forecasts(selection, requester=forbidden)["failures"][0]["code"] == "INVALID_SELECTION"


@pytest.mark.parametrize("horizon", range(11))
def test_api_supports_all_symbols_and_horizon_boundaries_without_lifespan(tmp_path, horizon):
    service = MarketResearchService(tmp_path / "source.db", tmp_path / "journal.db", symbols=DEFAULT_TRACKED_SYMBOLS)
    with TestClient(create_research_app(service, manage_lifecycle=False)) as client:
        result = client.get("/v1/research/forecasts", params={"horizon_sessions": horizon}).json()
        assert set(result["symbols"]) == set(DEFAULT_TRACKED_SYMBOLS)
        for symbol, item in result["symbols"].items():
            assert item["forecast"]["symbol"] == symbol
            assert item["forecast"]["horizon_sessions"] == horizon
            assert item["forecast"].get("target_session_date")
            assert item["forecast"]["predicted_close"] is None
        health = client.get("/health").json()
        assert health["process_alive"] is True
        assert health["worker"]["status"] == "NOT_STARTED"
        assert "source_database" not in health
        view._validate_group(result, list(DEFAULT_TRACKED_SYMBOLS), horizon)
    assert not service.journal_database.exists()


@pytest.mark.parametrize("query", ["symbols=", "symbols=SPX,", "symbols=SPX,WRONG", "horizon_sessions=-1", "horizon_sessions=11", "horizon_sessions=1.5"])
def test_api_invalid_inputs_are_422(tmp_path, query):
    service = MarketResearchService(tmp_path / "s", tmp_path / "j")
    with TestClient(create_research_app(service, manage_lifecycle=False)) as client:
        assert client.get("/v1/research/forecasts?" + query).status_code == 422


def test_history_inventory_exists_without_live_spot_and_agrees_with_evaluator():
    history = daily_history(80)
    inventory = inventory_daily_history("SPY", history, as_of_utc=AS_OF)
    for horizon in range(1, 11):
        forecast = evaluate_forecast("SPY", history + series(), as_of_utc=AS_OF, horizon_sessions=horizon)
        assert inventory[str(horizon)]["qualified_return_samples"] == forecast["evidence"]["horizon_return_samples"] == 80-horizon
        assert inventory[str(horizon)]["required_return_samples"] == 60
    gap = inventory_daily_history("SPY", history[:40] + history[41:], as_of_utc=AS_OF)
    assert gap["10"]["qualified_return_samples"] == 59
    assert gap["10"]["status"] == "BLOCKED"
    assert "insufficient_verified_daily_history" in gap["10"]["reasons"]


def test_inventory_excludes_invalid_provenance_and_never_substitutes_cash_index():
    history = daily_history(80)
    assert inventory_daily_history("SPX", history, as_of_utc=AS_OF)["1"]["qualified_return_samples"] == 0
    for r in history:
        r["source_verified"] = False
    result = inventory_daily_history("SPY", history, as_of_utc=AS_OF)["1"]
    assert result["verified_daily_close_count"] == 0
    assert result["excluded_daily_rows"]["unverified_source"] == 80


def test_all_abstain_progress_lock_and_worker_error(tmp_path, monkeypatch):
    source = tmp_path / "source.db"
    with sqlite3.connect(source) as c:
        c.execute("CREATE TABLE empty_fixture (id INTEGER)")
    digest = hashlib.sha256(source.read_bytes()).hexdigest()
    service = MarketResearchService(source, tmp_path / "journal.db", context_reader=lambda: {}, symbols=["SPX"])
    service.initialize_journal()
    for offset in (0, 60, 120):
        service.collect_once(now=AS_OF + timedelta(seconds=offset))
    health = service.snapshot(now=AS_OF + timedelta(seconds=121))
    assert health["worker"]["completed_capture_count"] == 3
    with sqlite3.connect(service.journal_database) as c:
        assert c.execute("SELECT count(*) FROM research_forecasts").fetchone()[0] == 0
    with sqlite3.connect(source) as lock:
        lock.execute("BEGIN EXCLUSIVE")
        result = service.collect_once(now=AS_OF + timedelta(seconds=180))
        assert "RETAINED_DATABASE_LOCKED" in result["reasons"]
        assert result["symbols"]["SPX"]["history_by_horizon"]["1"]["qualified_return_samples"] is None
        lock.rollback()
    assert hashlib.sha256(source.read_bytes()).hexdigest() == digest
    assert service.snapshot(now=AS_OF + timedelta(seconds=400))["worker"]["status"] == "STALLED"
    def broken(*a, **k):
        raise RuntimeError("private diagnostic")
    monkeypatch.setattr(service, "_persist", broken)
    with pytest.raises(RuntimeError):
        service.collect_once(now=AS_OF + timedelta(seconds=401))
    failed = service.snapshot(now=AS_OF + timedelta(seconds=402))
    assert failed["worker"]["last_error_code"] == "RESEARCH_CAPTURE_FAILED"
    assert "private diagnostic" not in json.dumps(failed)


def test_runner_check_has_no_lifecycle_or_provider_side_effect(tmp_path, monkeypatch, capsys):
    from tools.run_forecast_research import main, build_service
    source = tmp_path / "source.db"
    source.touch()
    journal = tmp_path / "journal.db"
    monkeypatch.setenv("DATABENTO_EQUITY_CONTEXT_ENABLED", "1")
    monkeypatch.setattr(sys, "argv", ["run_forecast_research.py", "--check", "--source-database", str(source), "--journal-database", str(journal)])
    main()
    assert len(json.loads(capsys.readouterr().out)["loaded_source_sha256"]) == 64
    assert not journal.exists()
    assert build_service(source, journal).equity_tracker is None
    with pytest.raises(ValueError, match="separate"):
        build_service(source, source)
    with pytest.raises(ValueError, match="MISSING"):
        build_service(tmp_path / "missing", journal)


def test_hard_link_journal_alias_is_rejected(tmp_path):
    from tools.run_forecast_research import build_service
    source = tmp_path / "source.db"
    source.write_bytes(b"retained evidence")
    alias = tmp_path / "journal.db"
    alias.hardlink_to(source)
    with pytest.raises(ValueError, match="separate"):
        build_service(source, alias)
    assert source.read_bytes() == b"retained evidence"


def test_dead_worker_is_distinct_from_api_liveness(tmp_path):
    from types import SimpleNamespace
    service = MarketResearchService(tmp_path / "source", tmp_path / "journal")
    service._thread = SimpleNamespace(is_alive=lambda: False)
    state = service.snapshot()
    assert state["process_alive"] and state["worker"]["status"] == "STOPPED"
    assert state["status"] == "UNAVAILABLE"


def test_reachable_warming_api_has_a_warming_reason_without_an_outage(tmp_path):
    service = MarketResearchService(tmp_path / "source", tmp_path / "journal", symbols=["SPX"])
    with TestClient(create_research_app(service, manage_lifecycle=False)) as client:
        def request(url, *, params, timeout):
            return Response(client.get("/v1/research/forecasts", params=params).json())
        result = view.fetch_forecasts({"SPX": 1}, requester=request)
    assert not result["failures"]
    row = view.forecast_rows(result)[0]
    assert row["Status"] == "ABSTAIN" and row["Research close"] is None
    assert "RESEARCH_WARMING" in row["Reason"]
    assert "TIMESTAMP_INVALID" not in row["Reason"]


def test_streamlit_rendered_outage_restoration_and_selection_changes(monkeypatch):
    def missing(*a, **k):
        raise requests.ConnectionError()
    monkeypatch.setattr(view.requests, "get", missing)
    ui = AppTest.from_string("from app.services.forecast_research_view import render_forecast_research\nrender_forecast_research()")
    ui.run().button[0].click().run()
    assert not ui.exception and len(ui.warning) == 1
    assert "TRANSPORT_ERROR" in ui.warning[0].value
    monkeypatch.setattr(view.requests, "get", requester)
    ui.selectbox(key="research_horizon_SPX").set_value(0)
    ui.selectbox(key="research_horizon_NDX").set_value(1)
    ui.selectbox(key="research_horizon_SPY").set_value(5)
    ui.selectbox(key="research_horizon_QQQ").set_value(10)
    ui.button[0].click().run()
    assert not ui.exception and len(ui.warning) == 0
    table = ui.dataframe[0].value
    assert list(table["Sessions ahead"]) == [0, 1, 5, 10]
    assert table.iloc[1]["Required returns"] == 60
    assert "insufficient_verified_daily_history" in table.iloc[1]["Reason"]
    assert not any("Research service unavailable" in c.value for c in ui.caption)
    ui.selectbox(key="research_horizon_NDX").set_value(2).run()
    assert not ui.dataframe and "Selections changed" in ui.info[0].value
    ui.button[0].click().run()
    assert list(ui.dataframe[0].value["Sessions ahead"]) == [0, 2, 5, 10]
    ui.multiselect[0].set_value(["SPX"]).run()
    assert not ui.exception and not ui.dataframe
    assert "Selections changed" in ui.info[0].value
    ui.button[0].click().run()
    assert list(ui.dataframe[0].value["Symbol"]) == ["SPX"]


def test_rendered_partial_groups_keep_success_and_show_failed_group(monkeypatch):
    def partial(url, **kwargs):
        if kwargs["params"]["horizon_sessions"] == 5:
            return Response({"invalid": "schema"})
        return requester(url, **kwargs)
    monkeypatch.setattr(view.requests, "get", partial)
    ui = AppTest.from_string("from app.services.forecast_research_view import render_forecast_research\nrender_forecast_research()").run()
    ui.selectbox(key="research_horizon_NDX").set_value(5)
    ui.button[0].click().run()
    assert not ui.exception and len(ui.warning) == 1
    table = ui.dataframe[0].value.set_index("Symbol")
    assert table.loc["SPX", "Research close"] == 100
    assert table.loc["NDX", "Status"] == "REQUEST_FAILED"
    assert table.loc["NDX", "Reason"] == "SCHEMA_ERROR"


@pytest.mark.parametrize("key,value", [("qualified_return_samples", "60"), ("reasons", "bad"), ("required_return_samples", True)])
def test_malformed_history_metadata_is_a_group_schema_error(key, value):
    payload = group(["SPX"], 0)
    payload["symbols"]["SPX"]["history_availability"][key] = value
    result = view.fetch_forecasts({"SPX": 0}, requester=lambda *a, **k: Response(payload))
    assert result["failures"][0]["code"] == "SCHEMA_ERROR"
