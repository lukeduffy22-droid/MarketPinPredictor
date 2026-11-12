"""Exercise the producer, real read adapters, Streamlit, and close import hook."""
import asyncio
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from types import SimpleNamespace

import pytest
from streamlit.testing.v1 import AppTest

from backend.api import lifecycle
from backend.api.routers import gex, predict
from backend import workstation
from backend.structural_regime_journal import StructuralRegimeJournal
from backend.structural_regime_live import FIELD, build_structural_prediction
from tests.test_structural_regime import NOW, model, payload
from tests.test_workstation_state import _passport


class FrozenDatetime(datetime):
    @classmethod
    def now(cls, tz=None):
        return NOW if tz is not None else NOW.replace(tzinfo=None)


@pytest.fixture
def published(monkeypatch, tmp_path):
    workstation.workstation_state_store.reset()
    monkeypatch.setattr(workstation, "datetime", FrozenDatetime)
    monkeypatch.setattr(lifecycle, "build_structural_prediction", lambda p, m, **kw:
                        build_structural_prediction(p, m, now_utc=NOW, **kw))
    stream = SimpleNamespace(subscription_epoch_id="a"*64, active_generation=1,
                             handoff_status="active", get_latest_pin=lambda symbol: None)
    monkeypatch.setattr(gex, "get_streamer", lambda: stream)
    monkeypatch.setattr(predict, "get_streamer", lambda: stream)
    journal = StructuralRegimeJournal(tmp_path / "structural.db")
    recorder = SimpleNamespace(record=lambda result: journal.record(result, now_utc=NOW))
    coordinator = lifecycle.WorkstationPredictionCoordinator(
        streamer=stream, shadow_research=SimpleNamespace(record=lambda p: None),
        capture_seconds=60, prediction_builder=lambda *a, **kw: model(),
        snapshot_saver=lambda *a, **kw: None,
        context_loader=lambda symbol: {}, adjustment_builder=lambda *a: (0, []),
        passport_writer=lambda *a, **kw: _passport(point_estimate=7719.55, interval_lower=7700, interval_upper=7750),
        structural_journal=recorder,
        lineage_verifier=lambda *a, **kw: {"valid": True, "reasons": []},
    )
    state = coordinator.process_payload(payload())
    yield state, stream, journal, coordinator
    workstation.workstation_state_store.reset()


def test_producer_journals_and_all_forecast_reads_retain_research_scope(published):
    state, stream, journal, coordinator = published
    assert state["status"] == "live"
    assert state["prediction"]["predicted_close"] == 7719.55
    structural = state["prediction"][FIELD]
    assert structural["predicted_close"] == pytest.approx(7728.6465)
    assert structural["journal_status"] == "RECORDED"
    sequence = workstation.workstation_state_store.sequence
    outputs = [
        asyncio.run(gex.get_structural_regime("SPX")),
        gex._symbol_dashboard_payload("SPX")["prediction"][FIELD],
        asyncio.run(gex.get_workstation_state("SPX"))["prediction"][FIELD],
        asyncio.run(predict.predict_ai_compatible("SPX"))[FIELD],
        asyncio.run(predict.predict_close("SPX")).structural_regime_prediction,
        asyncio.run(predict.get_close_overlay("SPX"))[FIELD],
    ]
    for result in outputs:
        assert result["prediction_id"] == structural["prediction_id"]
        assert result["decision_grade"] is False
        assert result["confidence_calibrated"] is False
    assert workstation.workstation_state_store.sequence == sequence
    with sqlite3.connect(journal.path) as connection:
        assert connection.execute("SELECT COUNT(*) FROM observations").fetchone()[0] == 1


def _advance_structural_clock(monkeypatch, seconds):
    now = NOW + timedelta(seconds=seconds)
    class AdvancedDatetime(datetime):
        @classmethod
        def now(cls, tz=None):
            return now if tz is not None else now.replace(tzinfo=None)
    monkeypatch.setattr(workstation, "datetime", AdvancedDatetime)
    monkeypatch.setattr(lifecycle, "build_structural_prediction", lambda p, m, **kw:
                        build_structural_prediction(p, m, now_utc=now, **kw))
    return now


def test_transient_update_preserves_exact_fresh_committed_structural_result(published, monkeypatch):
    state, stream, journal, coordinator = published
    original = state["prediction"][FIELD]
    now = _advance_structural_clock(monkeypatch, 10)
    transient = payload(calculation_id=None, timestamp=now.isoformat(), price=7740)
    result = coordinator.process_payload(transient)
    assert result["pin_payload"].get("calculation_id") is None
    assert result["prediction"][FIELD] == original
    assert result["prediction"][FIELD]["inputs"]["spot"] == 7735.76
    assert result["prediction"][FIELD]["as_of_utc"] == NOW.isoformat()
    assert asyncio.run(gex.get_structural_regime("SPX"))["prediction_id"] == original["prediction_id"]
    with sqlite3.connect(journal.path) as connection:
        assert connection.execute("SELECT COUNT(*) FROM observations").fetchone()[0] == 1


def test_committed_structural_expires_even_while_parent_is_fresh(published, monkeypatch):
    state, stream, journal, coordinator = published
    now = _advance_structural_clock(monkeypatch, 10)
    coordinator.process_payload(payload(calculation_id=None, timestamp=now.isoformat()))
    _advance_structural_clock(monkeypatch, 31)
    result = asyncio.run(gex.get_structural_regime("SPX"))
    assert result["status"] == "ABSTAIN"
    assert result["predicted_close"] is None
    assert result["reasons"] == ["STRUCTURAL_COMMITTED_SOURCE_STALE_MISSING_OR_FUTURE"]


@pytest.mark.parametrize("failure", ["generation", "epoch", "stale", "invalid", "model"])
def test_committed_structural_cannot_bypass_new_revision_gates(published, monkeypatch, failure):
    state, stream, journal, coordinator = published
    now = _advance_structural_clock(monkeypatch, 31 if failure == "stale" else 10)
    current = payload(calculation_id=None, timestamp=now.isoformat())
    if failure == "generation":
        stream.active_generation = current["subscription_generation"] = 2
    elif failure == "epoch":
        stream.subscription_epoch_id = current["subscription_epoch_id"] = "b" * 64
    elif failure == "invalid":
        current["validation_is_valid"] = False
    elif failure == "model":
        coordinator.prediction_builder = lambda *a, **kw: {**model(), "usable": False}
    result = coordinator.process_payload(current)
    structural = (result.get("prediction") or {}).get(FIELD) or {}
    assert structural.get("status") != "AVAILABLE"
    assert structural.get("predicted_close") is None


def test_committed_result_content_cannot_change_without_a_new_identity(published):
    state, stream, journal, coordinator = published
    state["prediction"][FIELD]["inputs"]["spot"] += 1
    result = workstation.sanitize_workstation_state_pin_payload(state)["prediction"][FIELD]
    assert result["status"] == "ABSTAIN"
    assert result["reasons"] == ["STRUCTURAL_COMMITTED_CONTENT_MISMATCH"]


@pytest.mark.parametrize("failure", ["generation", "epoch", "handoff", "stale"])
def test_runtime_failure_removes_nested_numeric_estimate(published, failure):
    state, stream, journal, coordinator = published
    if failure == "generation":
        stream.active_generation = 2
    elif failure == "epoch":
        stream.subscription_epoch_id = "b"*64
    elif failure == "handoff":
        stream.handoff_status = "pending"
    else:
        state["pin_payload"]["timestamp"] = (NOW-timedelta(hours=1)).isoformat()
        workstation.workstation_state_store.publish(state)
    result = asyncio.run(gex.get_structural_regime("SPX"))
    assert result["status"] == "ABSTAIN"
    assert result["predicted_close"] is None
    raw = asyncio.run(predict.predict_ai_compatible("SPX"))
    assert FIELD not in raw or raw[FIELD]["predicted_close"] is None


def test_invalid_source_and_missing_passport_never_journal(published):
    state, stream, journal, coordinator = published
    invalid = coordinator.process_payload(payload(validation_is_valid=False, calculation_id="invalid"))
    assert invalid["status"] == "invalid"
    coordinator.passport_writer = lambda *a, **kw: None
    no_passport = coordinator.process_payload(payload(calculation_id="no-passport", timestamp=(NOW-timedelta(seconds=1)).isoformat()))
    assert no_passport["status"] == "invalid"
    with sqlite3.connect(journal.path) as connection:
        assert connection.execute("SELECT COUNT(*) FROM observations").fetchone()[0] == 1


def test_persistence_error_visible_without_breaking_parent_estimate(published):
    state, stream, journal, coordinator = published
    def fail(_result):
        raise OSError("disk unavailable")
    coordinator.structural_journal = SimpleNamespace(record=fail)
    result = coordinator.process_payload(payload(calculation_id="failed-journal"))
    assert result["status"] == "live"
    assert result["prediction"]["predicted_close"] == 7719.55
    assert result["prediction"][FIELD]["journal_status"] == "FAILED"


def test_session_expiry_withholds_structural_number(published, monkeypatch):
    state, stream, journal, coordinator = published
    class ClosedDatetime(datetime):
        @classmethod
        def now(cls, tz=None):
            return NOW + timedelta(hours=2)
    monkeypatch.setattr(workstation, "datetime", ClosedDatetime)
    result = workstation.sanitize_workstation_state_pin_payload(state)["prediction"][FIELD]
    assert result["status"] == "ABSTAIN"
    assert result["predicted_close"] is result["inputs"] is None


@pytest.mark.parametrize("key,value", [
    ("symbol", "NDX"), ("subscription_generation", 999),
    ("calculation_id", "another-observation"), ("as_of_utc", "2026-09-25T17:59:59Z"),
])
def test_structural_result_must_match_its_parent_observation(published, key, value):
    state, stream, journal, coordinator = published
    state["prediction"][FIELD][key] = value
    result = workstation.sanitize_workstation_state_pin_payload(state)["prediction"][FIELD]
    assert result["status"] == "ABSTAIN"
    assert result["predicted_close"] is result["inputs"] is None


def _render(structural):
    from app.services.structural_regime_view import render_structural_regime
    render_structural_regime(structural)


def test_real_streamlit_panel_displays_formula_and_honest_score():
    result = build_structural_prediction(payload(), model(), now_utc=NOW)
    app = AppTest.from_function(_render, args=(result,)).run()
    assert not app.exception
    assert app.metric[0].value == "7,728.65"
    assert any("not decision-grade" in element.value for element in app.warning)
    assert any("0.680 / 1 (medium); not a probability" in element.value for element in app.caption)
    assert any("7,700.00–7,750.00" in element.value for element in app.caption)
    assert any("compressed upward band between pain and pin" in element.value for element in app.markdown)


def test_real_streamlit_panel_has_no_numbers_for_abstention():
    app = AppTest.from_function(_render, args=({"status": "ABSTAIN", "reasons": ["SAME_DAY_EXPIRATION_NOT_CONFIRMED"]},)).run()
    assert not app.exception
    assert not app.metric
    assert "SAME_DAY_EXPIRATION_NOT_CONFIRMED" in app.info[0].value


def test_frontend_adapter_passes_structural_without_replacing_main_forecast(published, monkeypatch):
    from app.services import live_data_client
    from app.services.live_data_client import build_databento_prediction_from_payloads
    monkeypatch.setattr(live_data_client, "datetime", FrozenDatetime)
    state = gex._symbol_dashboard_payload("SPX")
    result, warning, info = build_databento_prediction_from_payloads(
        symbol="SPX", timeframe="0DTE", dashboard_payload=state, stale_after_seconds=30,
    )
    assert warning is None
    assert result["predicted_price"] == 7719.55
    assert result[FIELD]["predicted_close"] == pytest.approx(7728.6465)


def test_validated_close_import_automatically_appends_comparison(tmp_path, monkeypatch):
    from tools import ingest_verified_closes as importer
    from tests.test_verified_close_import import _spx_artifact, _ndx_artifact

    yesterday = NOW - timedelta(days=1)
    journal = StructuralRegimeJournal(tmp_path / "data" / "structural_regime.db")
    prediction = build_structural_prediction(
        payload(timestamp=yesterday.isoformat(), primary_expiration="2026-09-24"),
        model(), now_utc=yesterday,
    )
    journal.record(prediction, now_utc=yesterday)
    spx, spx_hash = _spx_artifact(tmp_path, trading_date="September 24, 2026", close="7,720.00")
    ndx, ndx_hash = _ndx_artifact(tmp_path, trading_date="2026-09-24", close="27000.00")
    rows = [
        importer.VerifiedCloseInput("SPX", yesterday.date(), 7720, "sp-global-official",
            "https://www.spglobal.com/official", spx_hash, spx, yesterday+timedelta(hours=3), None),
        importer.VerifiedCloseInput("NDX", yesterday.date(), 27000, "nasdaq-official",
            "https://www.nasdaq.com/official", ndx_hash, ndx, yesterday+timedelta(hours=3), None),
    ]
    events = []
    database = SimpleNamespace(
        init_db=lambda: None,
        score_prediction_snapshots=lambda *a, **kw: pytest.fail("No legacy score selection"),
        upsert_verified_eod_close_bundle=lambda values: (
            events.append("committed"), [SimpleNamespace(official_close=item["official_close"]) for item in values]
        )[1],
    )
    monkeypatch.setenv("DATABASE_URL", importer.canonical_database_url(tmp_path))
    monkeypatch.setattr(importer, "_require_configured_database_bindings", lambda path: database)
    monkeypatch.setattr(importer, "reconcile_governed_prediction_outcomes",
                        lambda root, days, **kw: {day.isoformat(): {"scored": 0} for day in days})
    results = importer.ingest(rows, project_root=tmp_path, bundle_profile="core")
    assert events == ["committed"]
    assert results[0]["structural_regime_comparison"]["recorded"] == 1
    assert results[1]["structural_regime_comparison"]["recorded"] == 0
    with sqlite3.connect(journal.path) as connection:
        receipt = json.loads(connection.execute("SELECT outcome_json FROM outcomes").fetchone()[0])
        assert receipt["prediction_id"] == prediction["prediction_id"]
        assert receipt["actual_official_close"] == 7720
        assert receipt["source_artifact_sha256"] == spx_hash
