"""Retained partial windows expose separate observed segments without promotion."""
from copy import deepcopy
from datetime import date, datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

import app.services.orb_view as view
from backend.market_structure import MarketStructureJournal
from backend.quote_timing import CBBO_TIMING, OPTIONAL_LAST_SALE


OPEN = datetime(2026, 9, 14, 13, 30, tzinfo=timezone.utc)
EPOCH = "a" * 64


def row(seconds, price, **overrides):
    sample = OPEN + timedelta(seconds=seconds)
    value = {
        "sample_id": f"sample-{seconds}-{price}",
        "symbol": "SPX", "provider": "databento",
        "sample_timestamp_utc": sample,
        "source_timestamp_utc": sample + timedelta(milliseconds=100),
        "captured_at_utc": sample + timedelta(milliseconds=200),
        "progress_decided_at_utc": sample + timedelta(milliseconds=250),
        "reference_price": price, "reference_price_inputs_valid": True,
        "progress_eligible": True, "subscription_epoch_id": EPOCH,
        "subscription_generation": 1, "universe_sha256": "b" * 64,
        "primary_expiration": "2026-09-14", "same_day_profile_available": True,
        "spot_formula_version": "put-call-parity-v2-discounted-strike",
        "risk_free_rate": .0525, "symbol_mapping_version": "c" * 64,
    }
    value.update(overrides)
    return value


def snapshot(rows, *, seconds=3601):
    as_of = OPEN + timedelta(seconds=seconds)
    journal = MarketStructureJournal(
        loader=lambda *_args, **_kwargs: [],
        reference_loader=lambda *_args, **_kwargs: deepcopy(rows),
        now_utc=lambda: as_of, expected_cadence_seconds=5.,
    )
    return journal.snapshot(
        "SPX", trading_date=date(2026, 9, 14), as_of_utc=as_of,
        active_subscription_epoch_id=EPOCH,
        active_subscription_generation=1, active_handoff_status="active",
    )


def envelope(state):
    return {
        "schema_version": "marketpin-reference-orb.collection.v2",
        "runtime_binding_applied": True, "runtime_context_stable": True,
        "active_runtime_context": {"subscription_epoch_id": EPOCH, "subscription_generation": 1, "handoff_status": "active"},
        "symbols": {"SPX": state},
    }


def assert_context_only(segment):
    assert segment["authority"] == "DIAGNOSTIC_ONLY"
    assert segment["usage"] == "CONTEXT_ONLY"
    for field in ("directional_evidence_eligible", "model_eligible", "usable_for_prediction", "eligible_for_training", "orb_complete"):
        assert segment[field] is False


def test_all_windows_keep_a_b_a_segments_and_strict_partial_state():
    values = [row(15, 100), row(20, 102), row(25, 200, subscription_generation=2), row(30, 201, subscription_generation=2), row(35, 103), row(40, 104)]
    original = deepcopy(values)
    result = snapshot(values)
    for duration, expected in (("5m", 60), ("15m", 180), ("30m", 360), ("60m", 720)):
        window = result["opening_ranges"][duration]
        segments = window["observed_context_segments"]
        assert [s["identity"]["subscription_generation"] for s in segments] == [1, 2, 1]
        assert [(s["observed_low"], s["observed_high"]) for s in segments] == [(100, 102), (200, 201), (103, 104)]
        assert all(s["observed_bucket_count"] == 2 for s in segments)
        assert all(s["nominal_window_coverage_ratio"] == 2 / expected for s in segments)
        assert all(s["first_source_timestamp_utc"] != s["first_bucket_utc"] for s in segments)
        assert all(s["first_captured_at_utc"] != s["first_source_timestamp_utc"] for s in segments)
        for segment in segments: assert_context_only(segment)
        grid = window["observed_context_grid"]
        assert grid["nominal_expected_bucket_count"] == expected
        assert grid["elapsed_expected_bucket_count"] == expected
        assert grid["observed_bucket_count"] == 6
        assert grid["elapsed_missing_bucket_count"] == expected - 6
        assert grid["opening_bucket_missing"] is True
        assert window["capture_status"] == "partial"
        assert window["directional_evidence_eligible"] is False
        assert window["breakout_direction"] == "unavailable"
    assert values == original


@pytest.mark.parametrize("changed", [
    {"subscription_epoch_id": "d" * 64}, {"subscription_generation": 2},
    {"universe_sha256": "d" * 64}, {"primary_expiration": "2026-09-15", "same_day_profile_available": False},
    {"spot_formula_version": "separate-formula-v2"}, {"risk_free_rate": .06},
    {"symbol_mapping_version": "d" * 64},
    {"timestamp_contract_version": CBBO_TIMING, "quote_schema": "cbbo-1s", "last_sale_validation_version": OPTIONAL_LAST_SALE},
])
def test_complete_provenance_changes_never_pool_numeric_extrema(changed):
    state = snapshot([row(5, 100), row(10, 200, **changed), row(15, 101)])
    segments = state["opening_ranges"]["5m"]["observed_context_segments"]
    assert len(segments) == 3
    assert [s["observed_low"] for s in segments] == [100, 200, 101]


def test_same_bucket_overlap_and_exact_duplicate_do_not_multiply_coverage():
    first = row(5, 100)
    duplicate = deepcopy(first)
    second = row(5, 200, subscription_generation=2)
    state = snapshot([first, duplicate, second, row(10, 201, subscription_generation=2)])
    window = state["opening_ranges"]["5m"]
    assert [s["observed_bucket_count"] for s in window["observed_context_segments"]] == [1, 2]
    grid = window["observed_context_grid"]
    assert grid["observed_bucket_count"] == 2
    assert grid["overlapping_identity_bucket_count"] == 1
    assert grid["duplicate_identity_bucket_count"] == 1
    assert window["capture_status"] == "partial"


def test_forming_window_counts_only_elapsed_missing_buckets():
    state = snapshot([row(15, 100), row(20, 101)], seconds=20.5)
    grid = state["opening_ranges"]["5m"]["observed_context_grid"]
    assert grid["nominal_expected_bucket_count"] == 60
    assert grid["elapsed_expected_bucket_count"] == 5
    assert grid["future_bucket_count"] == 55
    assert grid["elapsed_missing_bucket_count"] == 3
    assert grid["nominal_window_coverage_ratio"] == 2 / 60
    assert grid["elapsed_capture_ratio"] == 2 / 5
    assert grid["elapsed_missing_spans"] == [{"first_bucket_utc": OPEN.isoformat().replace("+00:00", "Z"), "last_bucket_utc": (OPEN + timedelta(seconds=10)).isoformat().replace("+00:00", "Z"), "bucket_count": 3}]


def test_invalid_undecided_future_offgrid_and_incomplete_rows_never_enter_segments():
    values = [row(5, 100), row(10, float("nan")), row(15, 200, progress_eligible=False), row(20, 300, reference_price_inputs_valid=False), row(25, 400, symbol_mapping_version=None), row(26, 500), row(30, 600, captured_at_utc=OPEN + timedelta(seconds=100)), row(35, 700, progress_decided_at_utc=OPEN + timedelta(seconds=100))]
    state = snapshot(values, seconds=40)
    window = state["opening_ranges"]["5m"]
    segments = window["observed_context_segments"]
    assert len(segments) == 1
    assert segments[0]["observed_low"] == segments[0]["observed_high"] == 100
    assert window["observed_context_grid"]["excluded_context_sample_count"] == 4
    assert window["capture_evidence"]["excluded_invalid_price_sample_count"] == 1


def test_large_same_identity_gap_is_explicit_without_interpolation():
    window = snapshot([row(0, 100), row(5, 102), row(40, 101)])["opening_ranges"]["5m"]
    segment = window["observed_context_segments"][0]
    assert segment["maximum_interbucket_gap_seconds"] == 35
    assert segment["interior_missing_bucket_count"] == 6
    assert segment["observed_bucket_count"] == 3
    assert_context_only(segment)
    assert window["capture_status"] == "partial"


def test_context_addition_does_not_demote_or_promote_strict_complete_window():
    state = snapshot([row(i, 100 + i / 100) for i in range(0, 301, 5)], seconds=300.5)
    window = state["opening_ranges"]["5m"]
    assert window["capture_status"] == "complete"
    assert window["directional_evidence_eligible"] is True
    assert_context_only(window["observed_context_segments"][0])


def test_ui_shows_each_observed_identity_and_keeps_primary_summary_partial():
    payload = envelope(snapshot([row(15, 100), row(20, 102), row(25, 200, subscription_generation=2)]))
    strict = view.opening_range_rows(payload)
    context = view.observed_opening_context_rows(payload)
    assert len(context) == 8
    assert {r["Window"] for r in context} == {"5m", "15m", "30m", "60m"}
    assert {r["Generation"] for r in context} == {1, 2}
    assert {r["Use"] for r in context} == {"Context only"}
    assert all(r["ORB eligible"] is False and r["Prediction eligible"] is False and r["Training eligible"] is False for r in context)
    assert strict[0]["Capture"] == "partial" and strict[0]["ORB eligible"] is False
    assert strict[0]["5m ORB"] == "partial"


def test_ui_rejects_malformed_or_promoted_segment_envelopes():
    payload = envelope(snapshot([row(15, 100)]))
    bad = deepcopy(payload)
    for window in bad["symbols"]["SPX"]["opening_ranges"].values():
        window["observed_context_segments"][0]["model_eligible"] = True
    assert view.observed_opening_context_rows(bad) == []
    bad = deepcopy(payload)
    for window in bad["symbols"]["SPX"]["opening_ranges"].values():
        window["observed_context_segments"][0]["observed_high"] = float("nan")
    assert view.observed_opening_context_rows(bad) == []


def test_panel_renders_separate_context_table_without_changing_main_rows(monkeypatch):
    payload = envelope(snapshot([row(15, 100), row(20, 200, subscription_generation=2)]))
    frames, captions = [], []
    fake = SimpleNamespace(
        subheader=lambda *_: None, caption=lambda value: captions.append(value),
        warning=lambda *_: None, info=lambda *_: None, error=lambda *_: None, success=lambda *_: None,
        dataframe=lambda frame, **_kwargs: frames.append(frame),
        column_config=SimpleNamespace(ProgressColumn=lambda *_args, **_kwargs: None, NumberColumn=lambda *_args, **_kwargs: None),
    )
    monkeypatch.setattr(view, "st", fake)
    monkeypatch.setattr(view, "fetch_opening_ranges", lambda: payload)
    view.render_opening_range_panel()
    assert len(frames) == 2
    assert frames[0].iloc[0]["Capture"] == "partial"
    assert set(frames[1]["Use"]) == {"Context only"}
    assert any("Observed segments" in caption for caption in captions)
