"""Raw capture metadata must not promote formula evidence into eligible input."""
from __future__ import annotations

import copy
import json
from datetime import datetime, timezone

import pytest

from backend.databento_streamer import DatabentoGammaStreamer, current_market_date


@pytest.fixture
def capture(tmp_path, monkeypatch):
    import backend.database as backend_database
    import database as root_database

    streamer = DatabentoGammaStreamer(["SPX"])
    streamer.exports_dir = tmp_path / "exports"
    streamer.audit_dir = tmp_path / "audit"
    streamer.snapshot_interval = 0
    streamer.active_generation = 3
    streamer.handoff_status = "active"
    audited = []
    monkeypatch.setattr(root_database, "save_audit_snapshot_to_db", audited.append)
    monkeypatch.setattr(root_database, "save_gamma_snapshot", lambda **_kwargs: None)
    monkeypatch.setattr(backend_database, "save_market_snapshot", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        backend_database, "save_gamma_calculation_inputs",
        lambda run, _inputs: {"calculation_id": run["calculation_id"], "payload_sha256": "f" * 64},
    )
    source = {
        "symbol": "SPX",
        "timestamp": datetime.now(timezone.utc),
        "price": 7500.0,
        "gamma_pin": 7510.0,
        "likely_close": 7505.0,
        "gross_gex": 10.0,
        "net_gex": 2.0,
        "validation_is_valid": True,
        "gamma_excluded_from_model": False,
        "subscription_epoch_id": streamer.subscription_epoch_id,
        "subscription_generation": 3,
        "calculation_id": "capture-one",
        "universe_provenance": {
            "is_fallback": False,
            "source_date": current_market_date().isoformat(),
            "trading_date": current_market_date().isoformat(),
        },
        "oi_analytics_provenance": {"is_fallback": False},
        "_calculation_inputs": {
            "rejection_counts": {},
            "parameters": {"quote_freshness_seconds": 10.0},
        },
    }
    return streamer, source, audited


@pytest.mark.parametrize("field", ["universe_provenance", "oi_analytics_provenance", "is_fallback", "historical_context_only"])
def test_fallback_capture_is_diagnostic_only(capture, field, monkeypatch):
    streamer, source, audited = capture
    source[field] = {"is_fallback": True} if field.endswith("provenance") else True
    before = copy.deepcopy(source)

    import backend.database as backend_database
    import database as root_database
    writes = []
    monkeypatch.setattr(backend_database, "save_gamma_calculation_inputs", lambda *_args: writes.append("inputs"))
    monkeypatch.setattr(backend_database, "save_market_snapshot", lambda *_args, **_kwargs: writes.append("market"))
    monkeypatch.setattr(root_database, "save_gamma_snapshot", lambda **_kwargs: writes.append("gamma"))
    assert streamer._write_snapshot(source) is False

    assert not writes
    assert not list(streamer.exports_dir.rglob("*.ndjson"))
    exported = json.loads(next(streamer.audit_dir.rglob("*.json")).read_text())
    assert exported["usable_for_prediction"] is False
    assert exported["capture_block_reason"] == "UNIVERSE_CAPTURE_NOT_CURRENT_SESSION"
    assert "FALLBACK_PROVENANCE" in exported["validation_failure_reasons"]
    assert "CALCULATION_INPUT_PERSISTENCE_FAILED" not in exported["validation_failure_reasons"]
    assert exported["likely_close"] == 7505.0
    assert exported["primary_gamma_pin_strike"] == 7510.0
    assert not exported.get("calculation_id")
    assert "NON_PRODUCTION_FALLBACK" in exported["prediction_eligibility_reasons"]
    assert json.loads(json.dumps(audited[0], default=str)) == exported
    assert source == before


@pytest.mark.parametrize("provenance", [
    None, {}, {"is_fallback": False},
    {"is_fallback": False, "trading_date": "2026-09-23", "source_date": "2026-09-23"},
    {"is_fallback": False, "trading_date": "2026-09-24", "source_date": "2026-09-23"},
    {"is_fallback": False, "trading_date": "2026-09-24", "source_date": "2026-09-25"},
])
def test_universe_date_must_match_capture_session(capture, monkeypatch, provenance):
    from datetime import date
    import backend.database as backend_database
    import database as root_database
    streamer, source, audited = capture
    monkeypatch.setattr("backend.databento_streamer.current_market_date", lambda: date(2026, 9, 24))
    source["universe_provenance"] = provenance
    writes = []
    monkeypatch.setattr(backend_database, "save_gamma_calculation_inputs", lambda *_args: writes.append("inputs"))
    monkeypatch.setattr(backend_database, "save_market_snapshot", lambda *_args, **_kwargs: writes.append("market"))
    monkeypatch.setattr(root_database, "save_gamma_snapshot", lambda **_kwargs: writes.append("gamma"))
    assert streamer._write_snapshot(source) is False
    assert writes == []
    assert audited[0]["capture_block_reason"] == "UNIVERSE_CAPTURE_NOT_CURRENT_SESSION"
    assert "UNIVERSE_CAPTURE_NOT_CURRENT_SESSION" in audited[0]["validation_failure_reasons"]
    assert "CALCULATION_INPUT_PERSISTENCE_FAILED" not in audited[0]["validation_failure_reasons"]
    assert audited[0]["usable_for_prediction"] is False
    assert not list(streamer.exports_dir.rglob("*.ndjson"))


@pytest.mark.parametrize("failure", ["missing_capture", "generation", "epoch", "handoff", "explicit_false", "invalid", "model_excluded"])
def test_unproven_or_invalid_capture_never_claims_prediction_eligibility(capture, failure):
    streamer, source, audited = capture
    if failure == "missing_capture":
        source.pop("calculation_id")
        source.pop("_calculation_inputs")
    elif failure == "generation":
        source["subscription_generation"] = 2
    elif failure == "epoch":
        source["subscription_epoch_id"] = "a" * 64
    elif failure == "handoff":
        streamer.handoff_status = "warming"
    elif failure == "explicit_false":
        source["usable_for_prediction"] = False
    elif failure == "invalid":
        source["validation_is_valid"] = False
    else:
        source["gamma_excluded_from_model"] = True

    streamer._write_snapshot(source)

    assert audited[0]["usable_for_prediction"] is False
    assert audited[0]["likely_close"] == 7505.0


def test_matching_persisted_capture_can_remain_eligible(capture):
    streamer, source, audited = capture
    assert streamer._write_snapshot(source) is True
    assert audited[0]["usable_for_prediction"] is True
    assert audited[0]["calculation_inputs_persisted"] is True
    assert audited[0]["prediction_eligibility_reasons"] == []
    assert audited[0]["prediction_eligibility_scope"] == "capture_input_at_write_requires_lifecycle_revalidation"
    assert audited[0]["quote_freshness_limit_seconds"] == 10.0
    assert audited[0]["transport_counter_scope"] == "shared_databento_stream"
    assert audited[0]["transport_counter_semantics"] == (
        "cumulative_process_totals_not_symbol_counts"
    )
    assert audited[0]["replay_evidence_complete"] is False
    assert audited[0]["replay_missing_evidence"] == ["provider_statistics_end"]
    assert audited[0]["timestamp"] == audited[0]["timestamp_utc"]
    assert audited[0]["producer_timestamp_legacy"] is not None


def test_exact_oi_cutoff_and_freshness_make_replay_evidence_complete(capture):
    streamer, source, audited = capture
    source["oi_analytics_provenance"]["provider_statistics_end"] = (
        "2026-09-16T12:00:00+00:00"
    )

    assert streamer._write_snapshot(source) is True

    assert audited[0]["replay_evidence_complete"] is True
    assert audited[0]["replay_missing_evidence"] == []
    assert audited[0]["open_interest_provider_statistics_end_utc"] == (
        "2026-09-16T12:00:00+00:00"
    )


def test_generation_changed_during_persistence_is_not_restamped(capture, monkeypatch):
    import backend.database as backend_database

    streamer, source, audited = capture

    def persist_then_handoff(run, _inputs):
        streamer.active_generation = 4
        return {"calculation_id": run["calculation_id"], "payload_sha256": "f" * 64}

    monkeypatch.setattr(backend_database, "save_gamma_calculation_inputs", persist_then_handoff)
    assert streamer._write_snapshot(source) is True
    assert audited[0]["subscription_generation"] == 3
    assert audited[0]["usable_for_prediction"] is False
    assert "SUBSCRIPTION_GENERATION_MISMATCH_OR_MISSING" in audited[0]["prediction_eligibility_reasons"]


def test_uncommitted_capture_remains_diagnostic(capture, monkeypatch):
    import backend.database as backend_database

    streamer, source, audited = capture
    monkeypatch.setattr(backend_database, "save_gamma_calculation_inputs", lambda *_args: None)
    assert streamer._write_snapshot(source) is False
    assert audited[0]["usable_for_prediction"] is False
    assert not list(streamer.exports_dir.rglob("*.ndjson"))


@pytest.mark.parametrize("generation", [True, 0, -1, "3", 3.5])
def test_export_does_not_accept_coerced_source_generation(capture, generation):
    streamer, source, audited = capture
    source["subscription_generation"] = generation
    streamer._write_snapshot(source)
    assert audited[0]["usable_for_prediction"] is False
    assert "SUBSCRIPTION_GENERATION_MISMATCH_OR_MISSING" in audited[0]["prediction_eligibility_reasons"]


@pytest.mark.parametrize("estimate", [None, 0, -1, float("nan"), float("inf"), True])
def test_export_invalid_point_estimate_cannot_be_eligible(capture, estimate):
    streamer, source, audited = capture
    source["likely_close"] = estimate
    streamer._write_snapshot(source)
    assert audited[0]["usable_for_prediction"] is False
    assert "INVALID_POINT_ESTIMATE" in audited[0]["prediction_eligibility_reasons"]
