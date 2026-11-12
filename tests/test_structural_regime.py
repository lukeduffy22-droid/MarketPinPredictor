from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
import hashlib
import json
import sqlite3
from types import SimpleNamespace

import pytest

from backend.structural_regime import (
    EngineConfig, Snapshot, classify_regime, compute_band_width, compute_dist_to_pain,
    compute_dist_to_pin, compute_dist_to_zero_gamma, predict_close, score_confidence,
)
from backend.structural_regime_live import FIELD, build_structural_prediction
from backend.structural_regime_journal import StructuralRegimeJournal

NOW = datetime(2026, 9, 25, 18, 0, tzinfo=timezone.utc)


def snapshot(**changes):
    return replace(Snapshot(7735.76, 7750, 7700, 7508.83, 68134.68, 7719.55,
                            0.6, "MIDDAY", False, "RESEARCH_ONLY"), **changes)


def payload(**changes):
    return {
        "symbol": "SPX", "timestamp": NOW.isoformat(), "price": 7735.76,
        "gamma_pin": 7750, "max_pain": 7700, "zero_gamma": 7508.83, "net_gex": 68134.68,
        "subscription_epoch_id": "a" * 64, "subscription_generation": 1,
        "calculation_id": "calculation-1", "validation_is_valid": True,
        "gamma_excluded_from_model": False, "primary_expiration": "2026-09-25",
        "same_day_authority": True, **changes,
    }


def model():
    return {"usable": True, "predicted_close": 7719.55, "model_version": "test-model"}


def test_example_uses_formula_rather_than_copying_model_lean():
    s = snapshot()
    result = predict_close(s)
    assert result.predicted_close == pytest.approx(7728.6465)
    assert (result.band_low, result.band_high) == (7700, 7750)
    assert result.confidence == pytest.approx(0.68)
    assert result.regime.structural_label == "compressed_upward_band_between_pain_and_pin"
    assert compute_dist_to_pin(s) == pytest.approx(14.24)
    assert compute_dist_to_pain(s) == pytest.approx(35.76)
    assert compute_dist_to_zero_gamma(s) == pytest.approx(226.93)
    assert compute_band_width(s) == 50
    assert json.loads(json.dumps(result.to_dict()))["inputs"]["capture_state"] == "RESEARCH_ONLY"


@pytest.mark.parametrize("distance,expected", [
    (-25.01, "negative"), (-25, "neutral"), (0, "neutral"), (0.01, "neutral"),
    (25, "neutral"), (25.01, "positive_shallow"), (50, "positive_shallow"),
    (50.01, "positive_deep"),
])
def test_nonoverlapping_gamma_boundaries(distance, expected):
    assert classify_regime(snapshot(zero_gamma=7735.76-distance)).gamma_regime == expected


@pytest.mark.parametrize("changes,expected,band", [
    ({"spot": 7800, "model_lean": 7900}, 7825, (7700, 7762.5)),
    ({"spot": 7725, "zero_gamma": 7725, "model_lean": 7900}, 7821.25, (7687.5, 7762.5)),
    ({"spot": 7725, "zero_gamma": 7800, "model_lean": 7900}, 7850, (7675, 7775)),
    ({"spot": 7725, "zero_gamma": 7685, "model_lean": 7900}, 7803.75, (7700, 7750)),
    ({"spot": 7800, "zero_gamma": 7800, "model_lean": 7900}, 7835, (7687.5, 7762.5)),
    ({"spot": 7600, "zero_gamma": 7500, "model_lean": 7900}, 7752.5, (7687.5, 7750)),
])
def test_all_formula_branches_and_optional_clamping(changes, expected, band):
    result = predict_close(snapshot(**changes))
    assert result.predicted_close == pytest.approx(expected)
    assert (result.band_low, result.band_high) == band


@pytest.mark.parametrize("lean,expected", [(1, 7700), (10000, 7750)])
def test_only_compressed_deep_positive_requires_clamp(lean, expected):
    assert predict_close(snapshot(model_lean=lean)).predicted_close == expected


@pytest.mark.parametrize("pin,pain", [(7700, 7750), (7725, 7725)])
@pytest.mark.parametrize("zero", [7500, 7700, 7730, 7800])
def test_reversed_or_zero_width_never_inverts_band(pin, pain, zero):
    result = predict_close(snapshot(gamma_pin=pin, max_pain=pain, zero_gamma=zero))
    assert result.band_low <= result.band_high
    assert result.regime.band_width == pin-pain


def test_equal_relations_and_bad_regime():
    s = snapshot(spot=7750, max_pain=7750)
    assert classify_regime(s).pin_relation == "above_pin"
    assert classify_regime(s).pain_relation == "below_pain"
    with pytest.raises(ValueError, match="regime_does_not_match"):
        predict_close(s, classify_regime(snapshot()))


@pytest.mark.parametrize("key,value", [
    ("spot", None), ("spot", True), ("spot", 0), ("gamma_pin", -1),
    ("zero_gamma", float("nan")), ("model_lean", float("inf")), ("net_gex", "200"),
    ("vol_valid", "false"), ("time_of_day", 1.1), ("session_state", "CLOSED"),
])
def test_bad_snapshot_rejected(key, value):
    with pytest.raises(ValueError):
        snapshot(**{key: value})


@pytest.mark.parametrize("gex,expected", [(0, 0.53), (24999, 0.53), (25000, 0.62), (-50000, 0.68)])
def test_confidence_thresholds_are_in_native_units(gex, expected):
    assert predict_close(snapshot(net_gex=gex)).confidence == pytest.approx(expected)


def test_confidence_vol_time_and_alignment_components():
    s = snapshot(vol_valid=True, session_state="CLOSING")
    r = classify_regime(s)
    assert score_confidence(s, r, 7725, 7700, 7750) == pytest.approx(0.855)
    assert score_confidence(s, r, 7800, 7700, 7750) == pytest.approx(0.755)
    with pytest.raises(ValueError):
        EngineConfig(medium_gex_threshold=50000, high_gex_threshold=1)


def test_live_adaptation_provenance_and_input_immutability():
    p, m = payload(), model()
    original = json.dumps([p, m], sort_keys=True)
    result = build_structural_prediction(p, m, now_utc=NOW)
    assert result["status"] == "AVAILABLE"
    assert result["predicted_close"] == pytest.approx(7728.6465)
    assert result["inputs"]["vol_valid"] is False
    assert result["confidence_calibrated"] is False
    assert result["decision_grade"] is False
    assert result["capture_state"] == "RESEARCH_ONLY"
    assert result["calculation_id"] == "calculation-1"
    assert result["inputs"]["time_of_day"] == pytest.approx(270/390)
    assert json.dumps([p, m], sort_keys=True) == original
    assert build_structural_prediction(p, m, now_utc=NOW)["prediction_id"] == result["prediction_id"]


@pytest.mark.parametrize("changes,reason", [
    ({"validation_is_valid": False}, "SOURCE_VALIDATION_NOT_CONFIRMED"),
    ({"universe_provenance": {"is_fallback": True}}, "NON_PRODUCTION_FALLBACK"),
    ({"timestamp": "2026-09-25T18:00:01Z"}, "SOURCE_TIMESTAMP_STALE_MISSING_OR_FUTURE"),
    ({"timestamp": "2026-09-25T15:00:00Z"}, "SOURCE_TIMESTAMP_STALE_MISSING_OR_FUTURE"),
    ({"timestamp": "2026-09-25T18:00:00"}, "SOURCE_TIMESTAMP_STALE_MISSING_OR_FUTURE"),
    ({"primary_expiration": "2026-09-28"}, "SAME_DAY_EXPIRATION_NOT_CONFIRMED"),
    ({"same_day_authority": False}, "SAME_DAY_EXPIRATION_NOT_CONFIRMED"),
    ({"subscription_generation": True}, "SOURCE_RUNTIME_IDENTITY_MISSING"),
    ({"calculation_id": None}, "COMMITTED_CALCULATION_ID_MISSING"),
    ({"zero_gamma": None}, "zero_gamma_must_be_numeric"),
    ({"gex_unit": "dollars"}, "GEX_UNIT_UNSUPPORTED"),
    ({"symbol": "VIX"}, "UNSUPPORTED_CASH_INDEX"),
])
def test_live_abstains_without_synthesizing_inputs(changes, reason):
    result = build_structural_prediction(payload(**changes), model(), now_utc=NOW)
    assert result["status"] == "ABSTAIN"
    assert reason in result["reasons"]
    assert result["predicted_close"] is result["confidence"] is result["inputs"] is None


def test_model_lean_must_be_usable():
    assert build_structural_prediction(payload(), {**model(), "usable": False}, now_utc=NOW)["status"] == "ABSTAIN"


def test_volatility_requires_fresh_same_generation_valid_context():
    vix = payload(symbol="VIX", price=18)
    assert build_structural_prediction(payload(), model(), vix_payload=vix, now_utc=NOW)["inputs"]["vol_valid"] is True
    for changes in ({"subscription_generation": 2}, {"validation_is_valid": False},
                    {"timestamp": "2026-09-25T17:00:00Z"}, {"is_fallback": True}, {"price": 0}):
        result = build_structural_prediction(payload(), model(), vix_payload={**vix, **changes}, now_utc=NOW)
        assert result["status"] == "AVAILABLE"
        assert result["inputs"]["vol_valid"] is False


def test_early_close_session_and_postclose_abstention():
    for clock, expected in (("2026-11-27T14:45:00+00:00", "OPENING"),
                            ("2026-11-27T17:15:00+00:00", "CLOSING")):
        now = datetime.fromisoformat(clock)
        result = build_structural_prediction(payload(timestamp=clock, primary_expiration="2026-11-27"), model(), now_utc=now)
        assert result["inputs"]["session_state"] == expected
        assert result["target_close_utc"] == "2026-11-27T18:00:00+00:00"
    now = datetime(2026, 11, 27, 18, tzinfo=timezone.utc)
    result = build_structural_prediction(payload(timestamp=now.isoformat(), primary_expiration="2026-11-27"), model(), now_utc=now)
    assert "OUTSIDE_CASH_SESSION" in result["reasons"]


@pytest.mark.parametrize("day", ["2026-09-26", "2026-12-25", "2030-09-25"])
def test_closed_or_unreviewed_calendar_abstains(day):
    now = datetime.fromisoformat(day + "T18:00:00+00:00")
    result = build_structural_prediction(payload(timestamp=now.isoformat(), primary_expiration=day), model(), now_utc=now)
    assert result["status"] == "ABSTAIN"
    assert result["predicted_close"] is None


def test_journal_is_prospective_append_only_and_idempotent(tmp_path):
    journal = StructuralRegimeJournal(tmp_path / "research.db")
    result = build_structural_prediction(payload(), model(), now_utc=NOW)
    assert journal.record(result, now_utc=NOW)
    assert not journal.record(result, now_utc=NOW)
    with pytest.raises(ValueError, match="before_close"):
        journal.record(result, now_utc=NOW + timedelta(hours=3))
    with pytest.raises(ValueError, match="hash_mismatch"):
        journal.record({**result, "predicted_close": 9999}, now_utc=NOW)
    with sqlite3.connect(journal.path) as connection:
        assert connection.execute("SELECT COUNT(*) FROM observations").fetchone()[0] == 1
        for sql in ("DELETE FROM observations", "UPDATE observations SET symbol='NDX'"):
            with pytest.raises(sqlite3.IntegrityError, match="append_only"):
                connection.execute(sql)


def test_final_session_close_receipt_keeps_original_and_corrections(tmp_path):
    journal = StructuralRegimeJournal(tmp_path / "research.db")
    journal.record(build_structural_prediction(payload(), model(), now_utc=NOW), now_utc=NOW)
    later = NOW + timedelta(minutes=30)
    final = build_structural_prediction(payload(timestamp=later.isoformat(), calculation_id="final"), model(), now_utc=later)
    journal.record(final, now_utc=later)
    artifact = tmp_path / "official.html"
    artifact.write_text("SPX 2026-09-25 close 7720")
    row = SimpleNamespace(symbol="SPX", trading_date=date(2026, 9, 25), official_close=7720,
        source="sp-global-official", source_reference="https://www.spglobal.com/official",
        source_artifact_path=artifact, source_artifact_sha256=hashlib.sha256(artifact.read_bytes()).hexdigest(),
        observed_at_utc=NOW + timedelta(hours=3), correction_of_id=None)
    assert journal.record_verified_close(row, now_utc=NOW+timedelta(hours=4))["recorded"] == 1
    assert journal.record_verified_close(row, now_utc=NOW+timedelta(hours=4))["recorded"] == 0
    row.correction_of_id = 1
    row.official_close = 7721
    artifact.write_text("SPX 2026-09-25 corrected close 7721")
    row.source_artifact_sha256 = hashlib.sha256(artifact.read_bytes()).hexdigest()
    assert journal.record_verified_close(row, now_utc=NOW+timedelta(hours=4))["recorded"] == 1
    with sqlite3.connect(journal.path) as connection:
        receipts = [json.loads(item[0]) for item in connection.execute("SELECT outcome_json FROM outcomes")]
        assert len(receipts) == 2
        assert all(item["prediction_id"] == final["prediction_id"] for item in receipts)
        assert receipts[0]["actual_official_close"] == 7720
        assert receipts[0]["prediction"]["inputs"]["spot"] == 7735.76
        assert receipts[0]["absolute_error_points"] == pytest.approx(8.6465)
    row.source = "manual"
    with pytest.raises(ValueError, match="approved"):
        journal.record_verified_close(row, now_utc=NOW+timedelta(hours=4))
