"""Startup chain population is not established core quote loss.

The nearby retained Nov6 calculation had356/780NDX primary quotes. It is not
claimed to be the atomic counter snapshot at the22srollback decision.
"""
from copy import deepcopy
from threading import RLock

import pytest

from backend.databento_streamer import DatabentoGammaStreamer
from backend.optional_family_canary import RUTOptionalFamilyCanary


@pytest.fixture
def opening_canary(monkeypatch):
    # No constructor, directories, credentials, client, database or live records.
    s = DatabentoGammaStreamer.__new__(DatabentoGammaStreamer)
    s._runtime_context_lock = RLock()
    s._prediction_publication_lock = RLock()
    s._clock_evidence_lock = RLock()
    s._subscription_stage_lock = RLock()
    s._active_generation = 1
    s.subscription_epoch_id = "retained-nearby-nov6-g1"
    s.symbols = ["SPX", "NDX", "VIX", "RUT"]
    s._handoff_status = "active"
    s.optional_family_canary = RUTOptionalFamilyCanary(enabled=True, requested_families=tuple(s.symbols))
    s.optional_family_canary_warmup_seconds = 180.
    s._optional_family_canary_generation = 1
    s._optional_family_canary_active_since_monotonic = 1000.
    s.subscription_stage_state = "primary_active"
    s.provider_queue_full_warnings = 0
    s.provider_slow_client_warnings = 0
    s.provider_skipped_record_warnings = 0
    s.provider_skipped_records = 0
    values = {"clock": 1000., "backpressure": 0.}
    monkeypatch.setattr("backend.databento_streamer.time.monotonic", lambda: values["clock"])
    monkeypatch.setattr(s, "_compute_backpressure_remaining", lambda: values["backpressure"])
    monkeypatch.setattr(s, "_rut_orb_reference_progress", lambda at: {"advancing": True, "current_generation_bucket_count": 2})
    observations = {}
    for family, fresh, expected, lag, pairs, age in (
        ("SPX", 542, 646, 1.4722421, .9914529914529915, 5.354117),
        ("NDX", 356, 780, .2475, .7661290322580645, .541935),
        ("RUT", 256, 394, .1529785, .9318181818181818, .325738),
    ):
        observations[family] = {
            "subscription_epoch_id": s.subscription_epoch_id,
            "subscription_generation": 1,
            "validation_is_valid": True,
            "data_age_seconds": age,
            "receive_to_process_lag_p95_seconds": lag,
            "primary_pair_coverage_ratio": pairs,
            "fresh_quote_count": fresh,
            "expected_quote_count": expected,
        }
    monkeypatch.setattr(s, "_optional_family_canary_observations", lambda: deepcopy(observations))
    window = {"state": "regular_session", "observed_at_utc": "2026-11-06T14:30:22.000996+00:00", "cash_open_utc": "2026-11-06T14:30:00+00:00"}
    return s, observations, values, window


def test_initial_partial_population_honors_existing_warmup(opening_canary):
    s, observations, values, window = opening_canary
    assert s.optional_family_canary.preview(observations).reasons == ("CORE_FRESH_QUOTE_COVERAGE:NDX",)
    decision = s._optional_family_canary_decision(window)
    assert decision.rollback_required is False
    assert s.active_generation == 1
    assert "RUT" in s.symbols
    assert s.optional_family_canary.status().state == "armed"
    evidence = s.optional_family_canary_evaluation_evidence
    assert evidence["startup_quote_population_deferred_reasons"] == ["CORE_FRESH_QUOTE_COVERAGE:NDX"]
    assert evidence["warmup_remaining_seconds"] == 180.


def test_established_same_generation_coverage_loss_is_immediate(opening_canary):
    s, observations, values, window = opening_canary
    observations["NDX"]["fresh_quote_count"] = 400
    assert not s._optional_family_canary_decision(window).rollback_required
    observations["NDX"]["fresh_quote_count"] = 356
    decision = s._optional_family_canary_decision(window)
    assert decision.rollback_required
    assert decision.reasons == ("CORE_FRESH_QUOTE_COVERAGE:NDX",)
    assert s.optional_family_canary_evaluation_evidence["warmup_bypassed_for_measured_risk"] is True


@pytest.mark.parametrize("change", ["generation", "epoch"])
def test_population_baseline_does_not_cross_runtime_identity(opening_canary, change):
    s, observations, values, window = opening_canary
    observations["NDX"]["fresh_quote_count"] = 400
    s._optional_family_canary_decision(window)
    if change == "generation":
        s.active_generation = 2
        for row in observations.values(): row["subscription_generation"] = 2
    else:
        s.subscription_epoch_id = "new-epoch"
        for row in observations.values(): row["subscription_epoch_id"] = "new-epoch"
    observations["NDX"]["fresh_quote_count"] = 356
    assert not s._optional_family_canary_decision(window).rollback_required


def test_initial_partial_population_after_warmup_keeps_strict_rollback(opening_canary):
    s, observations, values, window = opening_canary
    values["clock"] += 181.
    assert s._optional_family_canary_decision(window).rollback_required


@pytest.mark.parametrize("failure", ["invalid", "age", "lag", "pairs", "zero", "compute", "slow", "skipped", "rut_lag"])
def test_population_grace_never_masks_measured_risk(opening_canary, failure):
    s, observations, values, window = opening_canary
    if failure == "invalid": observations["NDX"]["validation_is_valid"] = False
    elif failure == "age": observations["NDX"]["data_age_seconds"] = 16.
    elif failure == "lag": observations["NDX"]["receive_to_process_lag_p95_seconds"] = 3.
    elif failure == "pairs": observations["NDX"]["primary_pair_coverage_ratio"] = .01
    elif failure == "zero": observations["NDX"]["fresh_quote_count"] = 0
    elif failure == "compute": values["backpressure"] = 30.; s.provider_queue_full_warnings = 2
    elif failure == "slow": s.provider_slow_client_warnings = 1
    elif failure == "skipped": s.provider_skipped_record_warnings = 1
    elif failure == "rut_lag": observations["RUT"]["receive_to_process_lag_p95_seconds"] = 4.
    assert s._optional_family_canary_decision(window).rollback_required


@pytest.mark.parametrize("field,bad", [("fresh_quote_count", None), ("expected_quote_count", None), ("primary_pair_coverage_ratio", None), ("receive_to_process_lag_p95_seconds", None), ("data_age_seconds", None), ("fresh_quote_count", True), ("primary_pair_coverage_ratio", float("nan"))])
def test_missing_or_malformed_observations_cannot_establish_healthy_baseline(opening_canary, field, bad):
    s, observations, values, window = opening_canary
    observations["NDX"]["fresh_quote_count"] = 400
    observations["NDX"][field] = bad
    result = s._canary_core_population_readiness(observations, s.optional_family_canary.status().budgets)
    assert "NDX" not in result["established_core_families"]


def test_retired_observation_identity_cannot_establish_or_defer(opening_canary):
    s, observations, values, window = opening_canary
    observations["NDX"]["subscription_generation"] = 0
    result = s._canary_core_population_readiness(observations, s.optional_family_canary.status().budgets)
    assert "NDX" not in result["established_core_families"]
    assert result["startup_quote_population_reasons"] == []
