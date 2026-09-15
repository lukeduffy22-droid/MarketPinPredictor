from app.services.live_panel_view import (
    _closing_tape_analysis_message,
    _closing_tape_status_message,
    _current_valid_pin_symbols,
    _evidence_progress_rows,
    _pipeline_status_message,
    _promoted_prediction_view,
    _readiness_inventory_warning,
    _tape_family_rows,
)


def test_pipeline_message_reports_all_symbols_ready():
    assert _pipeline_status_message(True, ["SPX", "NDX"]) == (
        "success",
        "Prediction pipeline status: ready",
    )


def test_pipeline_message_reports_partial_symbol_readiness():
    assert _pipeline_status_message(False, ["NDX", "SPX"]) == (
        "warning",
        "Prediction pipeline status: ABSTAIN (current runtime is not eligible)",
    )


def test_pipeline_message_reports_degraded_when_none_are_valid():
    assert _pipeline_status_message(False, []) == (
        "warning",
        "Prediction pipeline status: ABSTAIN (current runtime is not eligible)",
    )


def _pipeline(*, handoff_status: str = "active", generation_is_current: bool = True):
    epoch = "e" * 64
    return {
        "prediction_pipeline_ok": True,
        "handoff_status": handoff_status,
        "subscription_epoch_id": epoch,
        "subscription_epoch_valid": True,
        "epoch_mismatch_symbols": [],
        "generation_mismatch_symbols": [],
        "required_epoch_mismatch_symbols": [],
        "required_generation_mismatch_symbols": [],
        "symbol_status": {
            "SPX": {
                "subscription_epoch_id": epoch,
                "active_subscription_epoch_id": epoch,
                "epoch_is_current": True,
                "subscription_generation": 7 if generation_is_current else 6,
                "active_generation": 7,
                "generation_is_current": generation_is_current,
                "usable_for_prediction": True,
                "is_stale": False,
                "fresh_quote_count": 10,
            }
        },
    }


def test_current_valid_pin_symbols_filters_retained_generation():
    assert _current_valid_pin_symbols(_pipeline(), ["SPX"]) == ["SPX"]
    assert _current_valid_pin_symbols(
        _pipeline(generation_is_current=False), ["SPX"]
    ) == []


def test_current_valid_pin_symbols_requires_active_handoff():
    assert _current_valid_pin_symbols(
        _pipeline(handoff_status="stopped"), ["SPX"]
    ) == []


def test_closing_tape_message_never_calls_missing_evidence_healthy():
    assert _closing_tape_status_message(
        {"state": "unavailable", "reasons": ["catalog is missing"]}
    ) == ("warning", "Capture: unavailable (catalog is missing)")


def test_closing_tape_message_marks_running_as_unfinalized():
    assert _closing_tape_status_message({"state": "running"}) == (
        "info",
        "Capture: recording; final integrity is not yet known",
    )


def test_closing_tape_transport_failure_is_not_missing_evidence():
    assert _closing_tape_status_message(
        {
            "transport_ok": False,
            "transport_error": "timeout",
            "transport_message": "request timed out after 5.0s",
        }
    ) == ("error", "Capture status unavailable: request timed out after 5.0s")


def test_closing_tape_analysis_is_independent_from_complete_capture():
    assert _closing_tape_analysis_message(
        {
            "state": "complete",
            "usable_for_research": True,
            "session": {
                "status": "incomplete",
                "error": "close-minus-15 analysis failed: post-horizon trade event",
            },
        }
    ) == (
        "warning",
        "Close-minus-15 analysis: abstained (close-minus-15 analysis failed: post-horizon trade event)",
    )


def test_closing_tape_analysis_does_not_infer_failure_during_capture():
    assert _closing_tape_analysis_message(
        {"state": "running", "session": {"status": "running"}}
    ) == ("info", "Close-minus-15 analysis: pending while capture is running")


def test_closing_tape_analysis_recognizes_producer_decision_states():
    assert _closing_tape_analysis_message(
        {"analysis": {"decision_state": "VALIDATED_RESEARCH"}}
    ) == (
        "success",
        "Close-minus-15 analysis: eligible research estimate recorded",
    )
    assert _closing_tape_analysis_message(
        {"analysis": {"decision_state": "RESEARCH_ONLY"}}
    ) == ("info", "Close-minus-15 analysis: recorded as research only")
    assert _closing_tape_analysis_message(
        {"analysis": {"decision_state": "ABSTAIN"}}
    ) == (
        "warning",
        "Close-minus-15 analysis: abstained (no eligible estimate)",
    )


def test_evidence_progress_uses_independent_sessions_not_record_count():
    rows = _evidence_progress_rows(
        {
            "unique_verified_sources": 7,
            "verified_close_sessions": 3,
            "model_evidence_sessions": 3,
            "paper_forecast_sessions": 2,
            "capture_gate_sessions_required": 10,
            "model_sessions_required": 60,
            "paper_sessions_required": 20,
            "capture_gate_ready": False,
            "model_training_ready": False,
            "paper_evidence_ready": False,
        }
    )

    assert [(row["current"], row["required"]) for row in rows] == [
        (7, 10), (3, 60), (2, 20)
    ]
    assert not any(row["ready"] for row in rows)


def test_catalog_inventory_issue_disables_progress_instead_of_rendering_zero():
    message = _readiness_inventory_warning(
        {
            "catalogs": 0,
            "catalog_issues": ["configured catalog root is missing: G:\\history"],
        }
    )

    assert message is not None
    assert "readiness gates are disabled" in message
    assert "G:\\history" in message
    assert _readiness_inventory_warning({"catalog_issues": []}) is None


def test_tape_family_rows_keep_observed_facts_and_inference_shares_distinct():
    rows = _tape_family_rows(
        {
            "observed": {
                "by_family": [
                    {
                        "family_root": "SPX",
                        "trade_records": 4,
                        "volume": 10,
                        "valid_pretrade_nbbo_records": 3,
                        "data_quality_flagged_records": 1,
                        "last_minute_utc": "2026-08-25T19:45:00+00:00",
                    }
                ]
            },
            "inferred": {
                "by_family": [
                    {
                        "family_root": "SPX",
                        "at_ask_count": 1,
                        "at_bid_count": 1,
                        "inside_count": 1,
                        "unknown_count": 1,
                    }
                ]
            },
        }
    )

    assert rows == [
        {
            "Family": "SPX",
            "Observed trades": 4,
            "Observed volume": 10.0,
            "NBBO coverage": 0.75,
            "At-ask estimate share": 0.25,
            "At-bid estimate share": 0.25,
            "Inside/unknown share": 0.5,
            "At-ask minus bid estimate": 0.0,
            "Estimate alignment": 1.0,
            "Quality-flagged records": 1,
            "Latest minute (UTC)": "2026-08-25T19:45:00+00:00",
        }
    ]


def _promoted_rows():
    return [
        {
            "family_root": family, "reference_price": 100.0,
            "predicted_level": 101.0, "feature_available_at_utc": "2026-08-25T19:45:00+00:00",
            "prediction_lower": 99.0, "prediction_upper": 103.0,
            "interval_target_coverage": 0.9,
            "calibration_method": "family_absolute_residual_conformal_v1",
            "calibration_evidence_sha256": "c" * 64,
            "recorded_at_utc": "2026-08-25T19:46:00+00:00", "source_sha256": "a" * 64,
            "model_version": "v1", "artifact_sha256": "b" * 64,
            "execution_device": "cpu", "prediction_mode": "tcbbo_promoted",
            "is_estimate": True, "trading_date": "2026-08-25",
        }
        for family in ("SPX", "NDX", "RUT", "VIX", "SPY")
    ]


def test_promoted_prediction_view_requires_complete_provenanced_estimates():
    view = _promoted_prediction_view({"rows": _promoted_rows()})

    assert view["state"] == "available"
    assert len(view["rows"]) == 5
    assert view["source_sha256"] == "a" * 64
    assert all(row["Promoted estimate"] == 101.0 for row in view["rows"])
    assert all(row["Target coverage"] == 0.9 for row in view["rows"])
    assert view["calibration_evidence_sha256"] == "c" * 64


def test_promoted_prediction_view_hides_partial_or_mixed_batches():
    assert _promoted_prediction_view({"rows": _promoted_rows()[:-1]})["state"] == "invalid"
    mixed = _promoted_rows()
    mixed[-1]["artifact_sha256"] = "c" * 64
    assert _promoted_prediction_view({"rows": mixed})["state"] == "invalid"
    assert _promoted_prediction_view({"rows": []})["state"] == "empty"


def test_promoted_prediction_view_treats_verified_empty_batch_as_neutral():
    view = _promoted_prediction_view(
        {
            "available": False,
            "read_succeeded": True,
            "transport_ok": True,
            "rows": [],
            "reason": "production model is not promoted",
        }
    )

    assert view == {
        "state": "empty",
        "rows": [],
        "message": "production model is not promoted",
    }


def test_promoted_prediction_view_does_not_hide_evidence_read_failure():
    view = _promoted_prediction_view(
        {
            "available": False,
            "read_succeeded": False,
            "transport_ok": True,
            "rows": [],
            "reason": "promoted prediction evidence is unavailable: database read failed",
        }
    )

    assert view == {
        "state": "unavailable",
        "rows": [],
        "message": "promoted prediction evidence is unavailable: database read failed",
    }


def test_promoted_prediction_view_hides_invalid_calibrated_interval():
    rows = _promoted_rows()
    rows[0]["prediction_lower"] = rows[0]["predicted_level"] + 1.0

    assert _promoted_prediction_view({"rows": rows})["state"] == "invalid"


def test_promoted_prediction_view_hides_mixed_or_invalid_calibration_evidence():
    mixed = _promoted_rows()
    mixed[-1]["calibration_evidence_sha256"] = "d" * 64
    assert _promoted_prediction_view({"rows": mixed})["state"] == "invalid"

    invalid = _promoted_rows()
    for row in invalid:
        row["calibration_evidence_sha256"] = "invalid"
    assert _promoted_prediction_view({"rows": invalid})["state"] == "invalid"
