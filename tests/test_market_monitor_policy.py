from __future__ import annotations

import copy
import hashlib
import json
import subprocess
import sys
from pathlib import Path

from backend.monitor_policy import (
    INPUT_SCHEMA,
    OUTPUT_SCHEMA,
    _observation_id,
    evaluate_monitor_policy,
)


ROOT = Path(__file__).resolve().parents[1]


def _observation(
    observed_at: str,
    *,
    spot: float = 29_500.0,
    gamma_pin: float = 29_600.0,
    max_pain: float = 29_480.0,
    normalized_net_gex: float = 0.20,
    forecast_bias: str | None = None,
    expected_move_pct: float | None = None,
) -> dict:
    return {
        "observation_id": f"NDX:{observed_at}",
        "observed_at_utc": observed_at,
        "symbol": "NDX",
        "eligible": True,
        "provider": "databento",
        "subscription_generation": 7,
        "subscription_epoch_id": "e" * 64,
        "primary_expiration": "2026-09-08",
        "gex_formula_version": "databento-gex-v2-call-minus-put",
        "universe_sha256": "a" * 64,
        "universe_is_fallback": False,
        "validation_is_valid": True,
        "gamma_excluded_from_model": False,
        "spot": spot,
        "gamma_pin": gamma_pin,
        "pin_is_contested": False,
        "pin_lead_ratio": 0.25,
        "top_strikes_by_abs_gex": [
            {"strike": 29_500.0},
            {"strike": 29_525.0},
            {"strike": 29_550.0},
        ],
        "max_pain": max_pain,
        "max_pain_source": "full-oi-universe",
        "max_pain_formula_version": "full-oi-max-pain-v1",
        "max_pain_as_of": "2026-09-08",
        "zero_gamma": 29_200.0,
        "positive_gex_wall": 29_600.0,
        "negative_gex_wall": 29_450.0,
        "normalized_net_gex": normalized_net_gex,
        "forecast_bias": forecast_bias,
        "expected_move_pct": expected_move_pct,
    }


def _payload(
    *observations: dict,
    state: dict | None = None,
    durable_event_receipts: list[dict] | None = None,
    confirmation_cadence_seconds: int | None = None,
) -> dict:
    payload = {
        "schema_version": INPUT_SCHEMA,
        "observations": list(observations),
        "policy_state": copy.deepcopy(state or {}),
    }
    if durable_event_receipts is not None:
        payload["durable_event_receipts"] = copy.deepcopy(durable_event_receipts)
    if confirmation_cadence_seconds is not None:
        payload["confirmation_cadence_seconds"] = confirmation_cadence_seconds
    return payload


def _event(result: dict, event_type: str) -> dict | None:
    return next((row for row in result["events"] if row["type"] == event_type), None)


def _supported_price_change(
    observation: dict, *, baseline_time: str, baseline_spot: float
) -> dict:
    current_spot = float(observation["spot"])
    observation["observation_id"] = hashlib.sha256(
        str(observation["observed_at_utc"]).encode("utf-8")
    ).hexdigest()
    return {
        "15m": {
            "schema_version": "marketpin-monitor-supported-price-change.v1",
            "window_seconds": 900,
            "actual_interval_seconds": 900.0,
            "pct": (current_spot - baseline_spot) / baseline_spot * 100.0,
            "baseline_scan_event_id": "a" * 64,
            "baseline_observation_id": "b" * 64,
            "baseline_observed_at_utc": baseline_time,
            "baseline_spot": baseline_spot,
            "current_observation_id": observation["observation_id"],
            "current_observed_at_utc": observation["observed_at_utc"],
            "current_spot": current_spot,
        }
    }


def test_supported_15m_price_change_requires_self_consistent_receipt_context() -> None:
    first = _observation(
        "2026-09-08T14:30:00Z", spot=29_087.0, normalized_net_gex=-0.20
    )
    second = _observation(
        "2026-09-08T14:45:00Z", spot=29_174.261, normalized_net_gex=0.20
    )
    first["supported_price_changes"] = _supported_price_change(
        first, baseline_time="2026-09-08T14:15:00Z", baseline_spot=29_000.0
    )
    second["supported_price_changes"] = _supported_price_change(
        second, baseline_time="2026-09-08T14:30:00Z", baseline_spot=29_087.0
    )

    valid = evaluate_monitor_policy(_payload(first, second))
    assert valid["accepted"] is True
    assert {
        (row["type"], row["direction"])
        for row in valid["confirmations"]["price"]
    } == {("persistent_15m_price_move", "bullish")}
    assert _event(valid, "DIRECTIONAL_SHIFT") is not None

    tampered = copy.deepcopy(second)
    tampered["supported_price_changes"]["15m"]["pct"] = 9.0
    rejected_context = evaluate_monitor_policy(_payload(first, tampered))
    assert rejected_context["accepted"] is True
    assert rejected_context["confirmations"]["price"] == []
    assert _event(rejected_context, "DIRECTIONAL_SHIFT") is None


def test_requires_two_eligible_aligned_observations() -> None:
    first = _observation("2026-09-08T14:00:00Z")
    assert evaluate_monitor_policy(_payload(first))["accepted"] is False

    second = _observation("2026-09-08T14:05:00Z")
    second["subscription_generation"] = 8
    result = evaluate_monitor_policy(_payload(first, second))
    assert result["accepted"] is False
    assert "alignment_mismatch:subscription_generation" in result["issues"]
    assert result["events"] == []

    second["observed_at_utc"] = "not-a-timestamp"
    malformed = evaluate_monitor_policy(_payload(first, second))
    assert malformed["accepted"] is False
    assert any("observed_at_utc_missing_or_invalid" in issue for issue in malformed["issues"])

    first = _observation("2026-09-08T14:00:00Z")
    second = _observation("2026-09-08T14:05:00Z")
    second["subscription_epoch_id"] = "f" * 64
    epoch_mismatch = evaluate_monitor_policy(_payload(first, second))
    assert epoch_mismatch["accepted"] is False
    assert "alignment_mismatch:subscription_epoch_id" in epoch_mismatch["issues"]
    assert epoch_mismatch["events"] == []


def test_rejects_same_session_confirmation_pair_after_long_monitoring_gap() -> None:
    first = _observation(
        "2026-09-08T13:30:00Z",
        forecast_bias="bullish",
        expected_move_pct=0.20,
        normalized_net_gex=0.20,
    )
    second = _observation(
        "2026-09-08T19:30:00Z",
        forecast_bias="bearish",
        expected_move_pct=-0.20,
        normalized_net_gex=-0.20,
    )
    prior_state = {"wrapper_extension": {"preserve": True}}

    result = evaluate_monitor_policy(_payload(first, second, state=prior_state))

    assert result["accepted"] is False
    assert "confirmation_gap_exceeds_maximum_seconds" in result["issues"]
    assert result["events"] == []
    assert result["next_state"] == prior_state
    assert result["confirmation_cadence_seconds"] == 900
    assert result["confirmation_min_gap_seconds"] == 240
    assert result["confirmation_max_gap_seconds"] == 1_200
    assert result["confirmation_gap_seconds"] == 6 * 60 * 60


def test_confirmation_gap_matches_normal_and_elevated_monitor_cadence() -> None:
    normal_first = _observation("2026-09-08T14:00:00Z")
    normal_second = _observation("2026-09-08T14:15:00Z")
    normal = evaluate_monitor_policy(_payload(normal_first, normal_second))

    assert normal["accepted"] is True
    assert normal["confirmation_cadence_seconds"] == 900
    assert normal["confirmation_min_gap_seconds"] == 240
    assert normal["confirmation_max_gap_seconds"] == 1_200
    assert normal["confirmation_gap_seconds"] == 900

    elevated_first = _observation("2026-09-08T14:00:00Z")
    elevated_second = _observation("2026-09-08T14:05:00Z")
    elevated = evaluate_monitor_policy(
        _payload(
            elevated_first,
            elevated_second,
            confirmation_cadence_seconds=300,
        )
    )

    assert elevated["accepted"] is True
    assert elevated["confirmation_cadence_seconds"] == 300
    assert elevated["confirmation_min_gap_seconds"] == 240
    assert elevated["confirmation_max_gap_seconds"] == 600
    assert elevated["confirmation_gap_seconds"] == 300

    too_late = _observation("2026-09-08T14:10:01Z")
    rejected = evaluate_monitor_policy(
        _payload(
            elevated_first,
            too_late,
            confirmation_cadence_seconds=300,
        )
    )
    assert rejected["accepted"] is False
    assert "confirmation_gap_exceeds_maximum_seconds" in rejected["issues"]


def test_confirmation_gap_rejects_single_wake_pair_and_honors_boundaries() -> None:
    first = _observation(
        "2026-09-08T14:00:00Z",
        forecast_bias="bullish",
        expected_move_pct=0.20,
        normalized_net_gex=0.20,
    )
    one_second_later = _observation(
        "2026-09-08T14:00:01Z",
        forecast_bias="bearish",
        expected_move_pct=-0.20,
        normalized_net_gex=-0.20,
    )
    rejected = evaluate_monitor_policy(
        _payload(
            first,
            one_second_later,
            confirmation_cadence_seconds=300,
        )
    )

    assert rejected["accepted"] is False
    assert rejected["issues"] == ["confirmation_gap_below_minimum_seconds"]
    assert rejected["events"] == []
    assert rejected["confirmation_min_gap_seconds"] == 240
    assert rejected["confirmation_gap_seconds"] == 1

    for cadence, accepted_last, rejected_last in (
        (300, "2026-09-08T14:10:00Z", "2026-09-08T14:10:01Z"),
        (900, "2026-09-08T14:20:00Z", "2026-09-08T14:20:01Z"),
    ):
        minimum = evaluate_monitor_policy(
            _payload(
                first,
                _observation("2026-09-08T14:04:00Z"),
                confirmation_cadence_seconds=cadence,
            )
        )
        below_minimum = evaluate_monitor_policy(
            _payload(
                first,
                _observation("2026-09-08T14:03:59Z"),
                confirmation_cadence_seconds=cadence,
            )
        )
        maximum = evaluate_monitor_policy(
            _payload(
                first,
                _observation(accepted_last),
                confirmation_cadence_seconds=cadence,
            )
        )
        above_maximum = evaluate_monitor_policy(
            _payload(
                first,
                _observation(rejected_last),
                confirmation_cadence_seconds=cadence,
            )
        )
        assert minimum["accepted"] is True
        assert below_minimum["accepted"] is False
        assert "confirmation_gap_below_minimum_seconds" in below_minimum["issues"]
        assert maximum["accepted"] is True
        assert above_maximum["accepted"] is False
        assert "confirmation_gap_exceeds_maximum_seconds" in above_maximum["issues"]


def test_confirmation_cadence_rejects_unsupported_or_noninteger_values() -> None:
    first = _observation("2026-09-08T14:00:00Z")
    second = _observation("2026-09-08T14:05:00Z")

    for invalid in (True, 600, 900.0, "300"):
        payload = _payload(first, second)
        payload["confirmation_cadence_seconds"] = invalid
        result = evaluate_monitor_policy(payload)
        assert result["accepted"] is False
        assert (
            "confirmation_cadence_seconds_must_be_300_or_900" in result["issues"]
        )
        assert result["events"] == []


def test_rejected_evaluation_preserves_complete_prior_state_unchanged() -> None:
    first = _observation("2026-09-08T14:00:00Z")
    second = _observation("2026-09-08T14:05:00Z")
    second["subscription_generation"] = 8
    prior_state = {
        "session_date": "2026-09-04",
        "levels": {
            "gamma_pin": {
                "value": 29_600.0,
                "provenance": {"preserve": "exactly"},
            }
        },
        "directional_latch": {
            "active_direction": "bearish",
            "custom_evidence": ["keep", "me"],
        },
        "extension_field": {"nested": True},
    }

    result = evaluate_monitor_policy(_payload(first, second, state=prior_state))

    assert result["accepted"] is False
    assert "alignment_mismatch:subscription_generation" in result["issues"]
    assert result["events"] == []
    assert result["next_state"] == prior_state
    assert result["next_state"] is not prior_state
    assert result["next_state"]["session_date"] == "2026-09-04"


def test_normalized_identity_fields_must_be_exactly_canonical() -> None:
    cases = (
        ("symbol", "ndx", "symbol_must_be_spx_or_ndx"),
        ("provider", "Databento", "provider_must_be_databento"),
        (
            "subscription_generation",
            "7",
            "subscription_generation_must_be_positive",
        ),
        (
            "subscription_generation",
            7.0,
            "subscription_generation_must_be_positive",
        ),
        (
            "subscription_epoch_id",
            "E" * 64,
            "subscription_epoch_id_must_be_sha256",
        ),
        (
            "subscription_epoch_id",
            None,
            "subscription_epoch_id_must_be_sha256",
        ),
        ("universe_sha256", "A" * 64, "universe_sha256_must_be_sha256"),
        (
            "max_pain_source",
            "FULL-OI-UNIVERSE",
            "max_pain_source_must_be_canonical_full_oi",
        ),
    )
    for field, value, issue_suffix in cases:
        first = _observation("2026-09-08T14:00:00Z")
        second = _observation("2026-09-08T14:05:00Z")
        first[field] = value
        second[field] = value
        result = evaluate_monitor_policy(_payload(first, second))
        assert result["accepted"] is False
        assert any(issue.endswith(issue_suffix) for issue in result["issues"])
        assert result["events"] == []


def test_static_below_pin_is_not_structure_confirmation() -> None:
    first = _observation(
        "2026-09-08T14:00:00Z",
        spot=29_450.0,
        forecast_bias="bearish",
        expected_move_pct=-0.25,
    )
    second = _observation(
        "2026-09-08T14:05:00Z",
        spot=29_430.0,
        forecast_bias="bearish",
        expected_move_pct=-0.20,
    )
    first["spot_below_gamma_pin_confirmed"] = True
    second["spot_below_gamma_pin_confirmed"] = True
    result = evaluate_monitor_policy(_payload(first, second))
    assert result["accepted"] is True
    assert result["confirmations"]["price"] == []
    assert result["confirmations"]["structure"] == []
    assert _event(result, "DIRECTIONAL_SHIFT") is None
    assert "directional_missing_price_confirmation" in result["suppressions"]


def test_independent_price_and_structure_confirmations_emit_direction() -> None:
    first = _observation(
        "2026-09-08T14:00:00Z",
        forecast_bias="bullish",
        expected_move_pct=0.25,
        normalized_net_gex=0.20,
    )
    second = _observation(
        "2026-09-08T14:05:00Z",
        forecast_bias="bearish",
        expected_move_pct=-0.20,
        normalized_net_gex=-0.20,
    )
    result = evaluate_monitor_policy(_payload(first, second))
    event = _event(result, "DIRECTIONAL_SHIFT")
    assert event is not None
    assert event["direction"] == "bearish"
    assert event["analytical_estimate_only"] is True
    assert result["confirmations"]["price"][0]["type"] == "forecast_bias_flip"


def test_research_flow_requires_explicit_fresh_research_only_and_coverage() -> None:
    first = _observation(
        "2026-09-08T14:00:00Z",
        forecast_bias="bullish",
        expected_move_pct=0.25,
    )
    second = _observation(
        "2026-09-08T14:05:00Z",
        forecast_bias="bearish",
        expected_move_pct=-0.20,
    )
    first_flow = {
        "score": 0.0,
        "classified_share": 0.80,
        "minimum_classified_share": 0.50,
    }
    second_flow = {
        "score": -0.20,
        "classified_share": 0.80,
        "minimum_classified_share": 0.50,
    }

    ineligible_flag_pairs = (
        ({}, {}),
        ({"fresh": False, "research_only": True}, {"fresh": False, "research_only": True}),
        ({"fresh": True, "research_only": False}, {"fresh": True, "research_only": False}),
        ({"fresh": True, "research_only": True}, {"fresh": True}),
    )
    for previous_flags, current_flags in ineligible_flag_pairs:
        prior = copy.deepcopy(first)
        current = copy.deepcopy(second)
        prior["research_flow"] = {**first_flow, **previous_flags}
        current["research_flow"] = {**second_flow, **current_flags}
        rejected_flow = evaluate_monitor_policy(_payload(prior, current))
        assert rejected_flow["accepted"] is True
        assert not any(
            row["type"] == "classified_research_flow_threshold_cross"
            for row in rejected_flow["confirmations"]["structure"]
        )
        assert _event(rejected_flow, "DIRECTIONAL_SHIFT") is None

    low_coverage_first = copy.deepcopy(first)
    low_coverage_second = copy.deepcopy(second)
    low_coverage_first["research_flow"] = {
        **first_flow,
        "classified_share": 0.40,
        "fresh": True,
        "research_only": True,
    }
    low_coverage_second["research_flow"] = {
        **second_flow,
        "classified_share": 0.40,
        "fresh": True,
        "research_only": True,
    }
    low_coverage = evaluate_monitor_policy(
        _payload(low_coverage_first, low_coverage_second)
    )
    assert not any(
        row["type"] == "classified_research_flow_threshold_cross"
        for row in low_coverage["confirmations"]["structure"]
    )
    assert _event(low_coverage, "DIRECTIONAL_SHIFT") is None

    invalid_coverage_values = (
        {"classified_share": -0.01},
        {"classified_share": 1.01},
        {"classified_share": float("inf")},
        {"classified_share": True},
        {"classified_share": "0.80"},
        {"minimum_classified_share": 0.0},
        {"minimum_classified_share": 1.01},
        {"minimum_classified_share": float("nan")},
        {"minimum_classified_share": True},
        {"minimum_classified_share": "0.50"},
    )
    for invalid_values in invalid_coverage_values:
        invalid_first = copy.deepcopy(first)
        invalid_second = copy.deepcopy(second)
        invalid_first["research_flow"] = {
            **first_flow,
            **invalid_values,
            "fresh": True,
            "research_only": True,
        }
        invalid_second["research_flow"] = {
            **second_flow,
            **invalid_values,
            "fresh": True,
            "research_only": True,
        }
        invalid_coverage = evaluate_monitor_policy(
            _payload(invalid_first, invalid_second)
        )
        assert not any(
            row["type"] == "classified_research_flow_threshold_cross"
            for row in invalid_coverage["confirmations"]["structure"]
        )
        assert _event(invalid_coverage, "DIRECTIONAL_SHIFT") is None

    invalid_scores = ("-0.20", True, -1.01, 1.01, float("inf"), float("nan"))
    for invalid_score in invalid_scores:
        invalid_first = copy.deepcopy(first)
        invalid_second = copy.deepcopy(second)
        invalid_first["research_flow"] = {
            **first_flow,
            "fresh": True,
            "research_only": True,
        }
        invalid_second["research_flow"] = {
            **second_flow,
            "score": invalid_score,
            "fresh": True,
            "research_only": True,
        }
        invalid_result = evaluate_monitor_policy(
            _payload(invalid_first, invalid_second)
        )
        assert not any(
            row["type"] == "classified_research_flow_threshold_cross"
            for row in invalid_result["confirmations"]["structure"]
        )
        assert _event(invalid_result, "DIRECTIONAL_SHIFT") is None

    first["research_flow"] = {
        **first_flow,
        "fresh": True,
        "research_only": True,
    }
    second["research_flow"] = {
        **second_flow,
        "fresh": True,
        "research_only": True,
    }
    eligible_flow = evaluate_monitor_policy(_payload(first, second))
    flow_confirmation = next(
        row
        for row in eligible_flow["confirmations"]["structure"]
        if row["type"] == "classified_research_flow_threshold_cross"
    )
    assert flow_confirmation["direction"] == "bearish"
    assert _event(eligible_flow, "DIRECTIONAL_SHIFT") is not None


def test_same_direction_forecasts_at_session_start_do_not_create_a_flip() -> None:
    first = _observation(
        "2026-09-08T14:00:00Z",
        forecast_bias="bearish",
        expected_move_pct=-0.25,
        normalized_net_gex=0.20,
    )
    second = _observation(
        "2026-09-08T14:05:00Z",
        forecast_bias="bearish",
        expected_move_pct=-0.20,
        normalized_net_gex=-0.20,
    )
    result = evaluate_monitor_policy(_payload(first, second))
    assert result["accepted"] is True
    assert result["confirmations"]["price"] == []
    assert result["confirmations"]["structure"][0]["direction"] == "bearish"
    assert _event(result, "DIRECTIONAL_SHIFT") is None
    assert "directional_missing_price_confirmation" in result["suppressions"]


def test_event_ids_are_deterministic() -> None:
    first = _observation(
        "2026-09-08T14:00:00Z",
        forecast_bias="bullish",
        expected_move_pct=0.25,
        normalized_net_gex=0.20,
    )
    second = _observation(
        "2026-09-08T14:05:00Z",
        forecast_bias="bearish",
        expected_move_pct=-0.20,
        normalized_net_gex=-0.20,
    )
    payload = _payload(first, second)
    first_result = evaluate_monitor_policy(payload)
    second_result = evaluate_monitor_policy(payload)
    assert first_result["events"] == second_result["events"]
    assert len(_event(first_result, "DIRECTIONAL_SHIFT")["event_id"]) == 64


def test_semantic_event_ids_survive_shifted_pair_after_interrupted_state_write() -> None:
    level_provenance = {
        "symbol": "NDX",
        "provider": "databento",
        "subscription_generation": 7,
        "subscription_epoch_id": "e" * 64,
        "primary_expiration": "2026-09-08",
        "universe_sha256": "a" * 64,
        "universe_is_fallback": False,
        "formula_version": "databento-gex-v2-call-minus-put",
    }
    max_pain_provenance = {
        **level_provenance,
        "formula_version": "full-oi-max-pain-v1",
        "source": "full-oi-universe",
        "as_of": "2026-09-08",
    }
    stale_state = {
        "session_date": "2026-09-08",
        "levels": {
            "gamma_pin": {"value": 29_600.0, "provenance": level_provenance},
            "max_pain": {"value": 29_480.0, "provenance": max_pain_provenance},
        },
    }
    first = _observation(
        "2026-09-08T14:00:00Z", gamma_pin=29_700.0, max_pain=29_500.0
    )
    second = _observation(
        "2026-09-08T14:05:00Z", gamma_pin=29_700.0, max_pain=29_500.0
    )
    third = _observation(
        "2026-09-08T14:10:00Z", gamma_pin=29_700.0, max_pain=29_500.0
    )

    original_pair = evaluate_monitor_policy(_payload(first, second, state=stale_state))
    shifted_pair = evaluate_monitor_policy(_payload(second, third, state=stale_state))
    for event_type in ("MAX_PAIN_CHANGE", "GAMMA_PIN_SHIFT"):
        original = _event(original_pair, event_type)
        shifted = _event(shifted_pair, event_type)
        assert original is not None and shifted is not None
        assert original["event_id"] == shifted["event_id"]
        assert (
            original["confirmation_observation_ids"]
            != shifted["confirmation_observation_ids"]
        )

    first["price_change_15m_pct"] = -0.25
    second["price_change_15m_pct"] = -0.25
    third["price_change_15m_pct"] = -0.25
    first["normalized_net_gex"] = 0.20
    second["normalized_net_gex"] = -0.20
    third["normalized_net_gex"] = -0.50
    original_direction = evaluate_monitor_policy(_payload(first, second))
    shifted_direction = evaluate_monitor_policy(_payload(second, third))
    original_event = _event(original_direction, "DIRECTIONAL_SHIFT")
    shifted_event = _event(shifted_direction, "DIRECTIONAL_SHIFT")
    assert original_event is not None and shifted_event is not None
    assert original_event["event_id"] == shifted_event["event_id"]
    assert (
        original_event["confirmation_observation_ids"]
        != shifted_event["confirmation_observation_ids"]
    )


def test_genuine_repeated_level_cycle_gets_a_new_semantic_event_id() -> None:
    gamma_provenance = {
        "symbol": "NDX",
        "provider": "databento",
        "subscription_generation": 7,
        "subscription_epoch_id": "e" * 64,
        "primary_expiration": "2026-09-08",
        "universe_sha256": "a" * 64,
        "universe_is_fallback": False,
        "formula_version": "databento-gex-v2-call-minus-put",
    }
    max_pain_provenance = {
        **gamma_provenance,
        "formula_version": "full-oi-max-pain-v1",
        "source": "full-oi-universe",
        "as_of": "2026-09-08",
    }
    initial_state = {
        "session_date": "2026-09-08",
        "levels": {
            "gamma_pin": {"value": 29_600.0, "provenance": gamma_provenance},
            "max_pain": {"value": 29_480.0, "provenance": max_pain_provenance},
        },
    }
    forward = evaluate_monitor_policy(
        _payload(
            _observation(
                "2026-09-08T14:00:00Z", gamma_pin=29_700.0, max_pain=29_500.0
            ),
            _observation(
                "2026-09-08T14:05:00Z", gamma_pin=29_700.0, max_pain=29_500.0
            ),
            state=initial_state,
        )
    )
    reverse = evaluate_monitor_policy(
        _payload(
            _observation(
                "2026-09-08T14:10:00Z", gamma_pin=29_600.0, max_pain=29_480.0
            ),
            _observation(
                "2026-09-08T14:15:00Z", gamma_pin=29_600.0, max_pain=29_480.0
            ),
            state=forward["next_state"],
        )
    )
    repeated_forward = evaluate_monitor_policy(
        _payload(
            _observation(
                "2026-09-08T14:20:00Z", gamma_pin=29_700.0, max_pain=29_500.0
            ),
            _observation(
                "2026-09-08T14:25:00Z", gamma_pin=29_700.0, max_pain=29_500.0
            ),
            state=reverse["next_state"],
        )
    )
    for event_type in ("MAX_PAIN_CHANGE", "GAMMA_PIN_SHIFT"):
        first_event = _event(forward, event_type)
        repeated_event = _event(repeated_forward, event_type)
        assert first_event is not None and repeated_event is not None
        assert first_event["event_id"] != repeated_event["event_id"]


def test_gamma_silent_provenance_rebaseline_preserves_semantic_predecessor() -> None:
    provenance = {
        "symbol": "NDX",
        "provider": "databento",
        "subscription_generation": 7,
        "subscription_epoch_id": "e" * 64,
        "primary_expiration": "2026-09-08",
        "universe_sha256": "a" * 64,
        "universe_is_fallback": False,
        "formula_version": "databento-gex-v2-call-minus-put",
    }
    state = {
        "session_date": "2026-09-08",
        "last_accepted_observed_at_utc": "2026-09-08T13:55:00Z",
        "levels": {"gamma_pin": {"value": 29_600.0, "provenance": provenance}},
    }
    first_transition = evaluate_monitor_policy(
        _payload(
            _observation("2026-09-08T14:00:00Z", gamma_pin=29_700.0),
            _observation("2026-09-08T14:05:00Z", gamma_pin=29_700.0),
            state=state,
        )
    )
    first_event = _event(first_transition, "GAMMA_PIN_SHIFT")
    assert first_event is not None

    changed_provenance_rows = [
        _observation("2026-09-08T14:10:00Z", gamma_pin=29_600.0),
        _observation("2026-09-08T14:15:00Z", gamma_pin=29_600.0),
    ]
    for row in changed_provenance_rows:
        row["subscription_generation"] = 8
        row["universe_sha256"] = "b" * 64
    silent_rebaseline = evaluate_monitor_policy(
        _payload(*changed_provenance_rows, state=first_transition["next_state"])
    )
    assert _event(silent_rebaseline, "GAMMA_PIN_SHIFT") is None
    rebaseline_level = silent_rebaseline["next_state"]["levels"]["gamma_pin"]
    assert rebaseline_level["status"] == "baseline"
    assert rebaseline_level["last_event_id"] == first_event["event_id"]

    restored_provenance = evaluate_monitor_policy(
        _payload(
            _observation("2026-09-08T14:20:00Z", gamma_pin=29_600.0),
            _observation("2026-09-08T14:25:00Z", gamma_pin=29_600.0),
            state=silent_rebaseline["next_state"],
        )
    )
    restored_level = restored_provenance["next_state"]["levels"]["gamma_pin"]
    assert restored_level["last_event_id"] == first_event["event_id"]

    repeated_transition = evaluate_monitor_policy(
        _payload(
            _observation("2026-09-08T14:30:00Z", gamma_pin=29_700.0),
            _observation("2026-09-08T14:35:00Z", gamma_pin=29_700.0),
            state=restored_provenance["next_state"],
        )
    )
    repeated_event = _event(repeated_transition, "GAMMA_PIN_SHIFT")
    assert repeated_event is not None
    assert repeated_event["event_id"] != first_event["event_id"]


def test_rollover_drops_legacy_epochless_levels_and_builds_clean_baselines() -> None:
    friday_gamma_provenance = {
        "symbol": "NDX",
        "provider": "databento",
        "subscription_generation": 6,
        "primary_expiration": "2026-09-04",
        "universe_sha256": "b" * 64,
        "universe_is_fallback": False,
        "formula_version": "databento-gex-v2-call-minus-put",
    }
    friday_max_pain_provenance = {
        **friday_gamma_provenance,
        "formula_version": "full-oi-max-pain-v1",
        "source": "full-oi-universe",
        "as_of": "2026-09-04",
    }
    friday_state = {
        "session_date": "2026-09-04",
        "levels": {
            "gamma_pin": {
                "value": 29_600.0,
                "provenance": friday_gamma_provenance,
            },
            "max_pain": {
                "value": 29_480.0,
                "provenance": friday_max_pain_provenance,
            },
        },
    }
    first = _observation(
        "2026-09-08T14:00:00Z", gamma_pin=29_700.0, max_pain=29_500.0
    )
    second = _observation(
        "2026-09-08T14:05:00Z", gamma_pin=29_700.0, max_pain=29_500.0
    )
    third = _observation(
        "2026-09-08T14:10:00Z", gamma_pin=29_700.0, max_pain=29_500.0
    )
    original = evaluate_monitor_policy(_payload(first, second, state=friday_state))
    shifted = evaluate_monitor_policy(_payload(second, third, state=friday_state))

    assert _event(original, "MAX_PAIN_PROVENANCE_RESET") is None
    assert _event(shifted, "MAX_PAIN_PROVENANCE_RESET") is None
    assert _event(original, "GAMMA_PIN_SHIFT") is None
    assert "max_pain_baseline_initialized_without_alert" in original["suppressions"]
    assert "gamma_pin_baseline_initialized_without_alert" in original["suppressions"]
    gamma_baseline = original["next_state"]["levels"]["gamma_pin"]
    assert gamma_baseline["value"] == 29_700.0
    assert gamma_baseline["status"] == "baseline"
    assert gamma_baseline["provenance"]["primary_expiration"] == "2026-09-08"
    assert gamma_baseline["prior_provenance"] is None
    assert gamma_baseline["current_provenance"] == gamma_baseline["provenance"]
    assert gamma_baseline["confirmation_observation_ids"] == [
        first["observation_id"],
        second["observation_id"],
    ]
    assert gamma_baseline["confirmed_at_utc"] == "2026-09-08T14:05:00Z"
    assert gamma_baseline["confirmed_at_ct"] == "2026-09-08T09:05:00-05:00"

    later_shift = evaluate_monitor_policy(
        _payload(
            _observation(
                "2026-09-08T14:15:00Z", gamma_pin=29_800.0, max_pain=29_500.0
            ),
            _observation(
                "2026-09-08T14:20:00Z", gamma_pin=29_800.0, max_pain=29_500.0
            ),
            state=original["next_state"],
        )
    )
    gamma_event = _event(later_shift, "GAMMA_PIN_SHIFT")
    assert gamma_event is not None
    assert gamma_event["old_value"] == 29_700.0
    assert gamma_event["new_value"] == 29_800.0


def test_max_pain_change_and_provenance_reset_are_distinct() -> None:
    first = _observation("2026-09-08T14:00:00Z", max_pain=29_500.0)
    second = _observation("2026-09-08T14:05:00Z", max_pain=29_500.0)
    provenance = {
        "symbol": "NDX",
        "provider": "databento",
        "subscription_generation": 7,
        "subscription_epoch_id": "e" * 64,
        "primary_expiration": "2026-09-08",
        "universe_sha256": "a" * 64,
        "universe_is_fallback": False,
        "formula_version": "full-oi-max-pain-v1",
        "source": "full-oi-universe",
        "as_of": "2026-09-08",
    }
    same_state = {
        "session_date": "2026-09-08",
        "levels": {"max_pain": {"value": 29_480.0, "provenance": provenance}},
    }
    change = evaluate_monitor_policy(_payload(first, second, state=same_state))
    assert _event(change, "MAX_PAIN_CHANGE") is not None

    reset_state = copy.deepcopy(same_state)
    reset_state["levels"]["max_pain"]["provenance"]["primary_expiration"] = "2026-09-04"
    reset = evaluate_monitor_policy(_payload(first, second, state=reset_state))
    assert _event(reset, "MAX_PAIN_PROVENANCE_RESET") is not None
    assert _event(reset, "MAX_PAIN_PROVENANCE_RESET")["provenance_reset"] is True


def test_reused_generation_with_new_epoch_cannot_borrow_prior_pin_context() -> None:
    gamma_provenance = {
        "symbol": "NDX",
        "provider": "databento",
        "subscription_generation": 7,
        "subscription_epoch_id": "e" * 64,
        "primary_expiration": "2026-09-08",
        "universe_sha256": "a" * 64,
        "universe_is_fallback": False,
        "formula_version": "databento-gex-v2-call-minus-put",
    }
    max_pain_provenance = {
        **gamma_provenance,
        "formula_version": "full-oi-max-pain-v1",
        "source": "full-oi-universe",
        "as_of": "2026-09-08",
    }
    state = {
        "session_date": "2026-09-08",
        "levels": {
            "gamma_pin": {"value": 29_600.0, "provenance": gamma_provenance},
            "max_pain": {"value": 29_480.0, "provenance": max_pain_provenance},
        },
    }
    observations = [
        _observation(
            "2026-09-08T14:00:00Z", gamma_pin=29_700.0, max_pain=29_500.0
        ),
        _observation(
            "2026-09-08T14:05:00Z", gamma_pin=29_700.0, max_pain=29_500.0
        ),
    ]
    for observation in observations:
        observation["subscription_epoch_id"] = "f" * 64

    result = evaluate_monitor_policy(_payload(*observations, state=state))

    assert result["accepted"] is True
    assert _event(result, "GAMMA_PIN_SHIFT") is None
    reset = _event(result, "MAX_PAIN_PROVENANCE_RESET")
    assert reset is not None
    assert reset["current_provenance"]["subscription_epoch_id"] == "f" * 64
    gamma = result["next_state"]["levels"]["gamma_pin"]
    assert gamma["status"] == "baseline"
    assert gamma["value"] == 29_700.0
    assert gamma["provenance"]["subscription_epoch_id"] == "f" * 64


def test_derived_observation_identity_includes_subscription_epoch() -> None:
    first = _observation("2026-09-08T14:00:00Z")
    first.pop("observation_id")
    restarted = copy.deepcopy(first)
    restarted["subscription_epoch_id"] = "f" * 64

    assert _observation_id(first) != _observation_id(restarted)


def test_same_session_legacy_epochless_levels_cannot_emit_live_shifts() -> None:
    legacy_gamma = {
        "symbol": "NDX",
        "provider": "databento",
        "subscription_generation": 7,
        "primary_expiration": "2026-09-08",
        "universe_sha256": "a" * 64,
        "universe_is_fallback": False,
        "formula_version": "databento-gex-v2-call-minus-put",
    }
    legacy_max = {
        **legacy_gamma,
        "formula_version": "full-oi-max-pain-v1",
        "source": "full-oi-universe",
        "as_of": "2026-09-08",
    }
    state = {
        "session_date": "2026-09-08",
        "levels": {
            "gamma_pin": {"value": 29_600.0, "provenance": legacy_gamma},
            "max_pain": {"value": 29_480.0, "provenance": legacy_max},
        },
    }
    result = evaluate_monitor_policy(
        _payload(
            _observation(
                "2026-09-08T14:00:00Z", gamma_pin=29_700.0, max_pain=29_500.0
            ),
            _observation(
                "2026-09-08T14:05:00Z", gamma_pin=29_700.0, max_pain=29_500.0
            ),
            state=state,
        )
    )

    assert _event(result, "GAMMA_PIN_SHIFT") is None
    assert _event(result, "MAX_PAIN_PROVENANCE_RESET") is None
    assert "gamma_pin_prior_provenance_incomplete_rebaselined" in result[
        "suppressions"
    ]
    assert "max_pain_prior_provenance_incomplete_rebaselined" in result[
        "suppressions"
    ]


def test_alerted_levels_persist_full_confirmation_and_threshold_evidence() -> None:
    first = _observation(
        "2026-09-08T14:00:00Z", gamma_pin=29_700.0, max_pain=29_500.0
    )
    second = _observation(
        "2026-09-08T14:05:00Z", gamma_pin=29_700.0, max_pain=29_500.0
    )
    gamma_provenance = {
        "symbol": "NDX",
        "provider": "databento",
        "subscription_generation": 7,
        "subscription_epoch_id": "e" * 64,
        "primary_expiration": "2026-09-08",
        "universe_sha256": "a" * 64,
        "universe_is_fallback": False,
        "formula_version": "databento-gex-v2-call-minus-put",
    }
    max_pain_provenance = {
        **gamma_provenance,
        "formula_version": "full-oi-max-pain-v1",
        "source": "full-oi-universe",
        "as_of": "2026-09-08",
    }
    prior_state = {
        "session_date": "2026-09-08",
        "levels": {
            "gamma_pin": {
                "value": 29_600.0,
                "provenance": gamma_provenance,
            },
            "max_pain": {
                "value": 29_480.0,
                "provenance": max_pain_provenance,
            },
        },
    }

    result = evaluate_monitor_policy(_payload(first, second, state=prior_state))
    confirmation_ids = [first["observation_id"], second["observation_id"]]
    confirmation_utc = [
        "2026-09-08T14:00:00Z",
        "2026-09-08T14:05:00Z",
    ]
    confirmation_ct = [
        "2026-09-08T09:00:00-05:00",
        "2026-09-08T09:05:00-05:00",
    ]

    max_event = _event(result, "MAX_PAIN_CHANGE")
    max_state = result["next_state"]["levels"]["max_pain"]
    assert max_event is not None
    assert max_state["status"] == "alerted"
    assert max_state["last_event_id"] == max_event["event_id"]
    assert max_state["confirmation_observation_ids"] == confirmation_ids
    assert max_state["confirmation_observed_at_utc"] == confirmation_utc
    assert max_state["confirmation_observed_at_ct"] == confirmation_ct
    assert max_state["confirmed_at_utc"] == confirmation_utc[-1]
    assert max_state["confirmed_at_ct"] == confirmation_ct[-1]
    assert max_state["prior_provenance"] == max_pain_provenance
    assert max_state["current_provenance"] == max_event["current_provenance"]
    assert max_state["current_provenance"]["source"] == "full-oi-universe"
    assert max_state["current_provenance"]["as_of"] == "2026-09-08"
    assert max_state["current_provenance"]["symbol"] == "NDX"
    assert max_state["current_provenance"]["universe_is_fallback"] is False

    gamma_event = _event(result, "GAMMA_PIN_SHIFT")
    gamma_state = result["next_state"]["levels"]["gamma_pin"]
    assert gamma_event is not None
    assert gamma_state["status"] == "alerted"
    assert gamma_state["last_event_id"] == gamma_event["event_id"]
    assert gamma_state["confirmation_observation_ids"] == confirmation_ids
    assert gamma_state["confirmation_observed_at_utc"] == confirmation_utc
    assert gamma_state["confirmation_observed_at_ct"] == confirmation_ct
    assert gamma_state["confirmed_at_utc"] == confirmation_utc[-1]
    assert gamma_state["confirmed_at_ct"] == confirmation_ct[-1]
    assert gamma_state["prior_provenance"] == gamma_provenance
    assert gamma_state["current_provenance"] == gamma_event["current_provenance"]
    expected_threshold_evidence = {
        "calculation_version": "gamma-pin-significance-v1",
        "threshold_points": 50.0,
        "threshold_source": "spot_and_observed_top_strike_spacing",
        "spot": 29_500.0,
        "spot_by_observation": [29_500.0, 29_500.0],
        "spot_component_points": 44.25,
        "top_strikes_by_observation": [
            [29_500.0, 29_525.0, 29_550.0],
            [29_500.0, 29_525.0, 29_550.0],
        ],
        "median_top_strike_spacing": 25.0,
        "spacing_component_points": 50.0,
        "conservative_fallback_points": 75.0,
        "fallback_used": False,
        "components_points": [44.25, 50.0],
        "change_points": 100.0,
        "absolute_change_points": 100.0,
        "absolute_change_qualified": True,
        "old_pin_value": 29_600.0,
        "new_pin_value": 29_700.0,
        "zero_gamma": 29_200.0,
        "zero_gamma_by_observation": [29_200.0, 29_200.0],
        "crossed_spot": False,
        "crossed_zero_gamma": False,
        "pin_lead_ratios": [0.25, 0.25],
        "pin_is_contested_by_observation": [False, False],
        "pin_contest_threshold": 0.10,
        "pin_is_contested": False,
        "spot_crossed_new_pin": False,
        "significant": True,
    }
    assert gamma_event["threshold_evidence"] == expected_threshold_evidence
    assert gamma_state["threshold_evidence"] == expected_threshold_evidence


def test_durable_receipt_hydrates_original_event_evidence_after_state_write_crash() -> None:
    gamma_provenance = {
        "symbol": "NDX",
        "provider": "databento",
        "subscription_generation": 7,
        "subscription_epoch_id": "e" * 64,
        "primary_expiration": "2026-09-08",
        "universe_sha256": "a" * 64,
        "universe_is_fallback": False,
        "formula_version": "databento-gex-v2-call-minus-put",
    }
    stale_state = {
        "session_date": "2026-09-08",
        "last_accepted_observed_at_utc": "2026-09-08T13:55:00Z",
        "levels": {
            "gamma_pin": {"value": 29_600.0, "provenance": gamma_provenance}
        },
    }
    first = _observation("2026-09-08T14:00:00Z", gamma_pin=29_700.0)
    second = _observation("2026-09-08T14:05:00Z", gamma_pin=29_700.0)
    original = evaluate_monitor_policy(_payload(first, second, state=stale_state))
    durable_event = copy.deepcopy(_event(original, "GAMMA_PIN_SHIFT"))
    assert durable_event is not None
    assert durable_event["threshold_points"] == 50.0

    third = _observation("2026-09-08T14:10:00Z", gamma_pin=29_700.0)
    third["top_strikes_by_abs_gex"] = [
        {"strike": 29_500.0},
        {"strike": 29_540.0},
        {"strike": 29_580.0},
    ]
    shifted_without_receipt = evaluate_monitor_policy(
        _payload(second, third, state=stale_state)
    )
    shifted_event = _event(shifted_without_receipt, "GAMMA_PIN_SHIFT")
    assert shifted_event is not None
    assert shifted_event["event_id"] == durable_event["event_id"]
    assert shifted_event["threshold_points"] == 65.0
    assert shifted_event["confirmation_observation_ids"] == [
        second["observation_id"],
        third["observation_id"],
    ]

    hydrated = evaluate_monitor_policy(
        _payload(
            second,
            third,
            state=stale_state,
            durable_event_receipts=[durable_event],
        )
    )
    hydrated_event = _event(hydrated, "GAMMA_PIN_SHIFT")
    hydrated_level = hydrated["next_state"]["levels"]["gamma_pin"]
    assert hydrated["accepted"] is True
    assert hydrated_event is not None
    assert hydrated_event["durable_receipt_applied"] is True
    assert hydrated_event["confirmation_observation_ids"] == [
        first["observation_id"],
        second["observation_id"],
    ]
    assert hydrated_event["threshold_evidence"] == durable_event[
        "threshold_evidence"
    ]
    assert hydrated_level["confirmation_observation_ids"] == [
        first["observation_id"],
        second["observation_id"],
    ]
    assert hydrated_level["confirmed_at_utc"] == "2026-09-08T14:05:00Z"
    assert hydrated_level["confirmed_at_ct"] == "2026-09-08T09:05:00-05:00"
    assert hydrated_level["threshold_evidence"]["threshold_points"] == 50.0
    assert (
        hydrated["next_state"]["last_accepted_observed_at_utc"]
        == "2026-09-08T14:10:00Z"
    )


def test_durable_level_receipts_recover_before_new_pair_loses_gamma_candidate() -> None:
    gamma_provenance = {
        "symbol": "NDX",
        "provider": "databento",
        "subscription_generation": 7,
        "subscription_epoch_id": "e" * 64,
        "primary_expiration": "2026-09-08",
        "universe_sha256": "a" * 64,
        "universe_is_fallback": False,
        "formula_version": "databento-gex-v2-call-minus-put",
    }
    max_pain_provenance = {
        **gamma_provenance,
        "formula_version": "full-oi-max-pain-v1",
        "source": "full-oi-universe",
        "as_of": "2026-09-08",
    }
    stale_state = {
        "session_date": "2026-09-08",
        "last_accepted_observed_at_utc": "2026-09-08T13:55:00Z",
        "levels": {
            "gamma_pin": {"value": 29_600.0, "provenance": gamma_provenance},
            "max_pain": {"value": 29_480.0, "provenance": max_pain_provenance},
        },
    }
    original_first = _observation(
        "2026-09-08T14:00:00Z", gamma_pin=29_700.0, max_pain=29_500.0
    )
    original_second = _observation(
        "2026-09-08T14:05:00Z", gamma_pin=29_700.0, max_pain=29_500.0
    )
    original = evaluate_monitor_policy(
        _payload(original_first, original_second, state=stale_state)
    )
    gamma_receipt = copy.deepcopy(_event(original, "GAMMA_PIN_SHIFT"))
    max_receipt = copy.deepcopy(_event(original, "MAX_PAIN_CHANGE"))
    assert gamma_receipt is not None and max_receipt is not None

    current_first = _observation(
        "2026-09-08T14:10:00Z", gamma_pin=29_700.0, max_pain=29_500.0
    )
    current_second = _observation(
        "2026-09-08T14:15:00Z", gamma_pin=29_700.0, max_pain=29_500.0
    )
    for row in (current_first, current_second):
        row["top_strikes_by_abs_gex"] = [
            {"strike": 29_400.0},
            {"strike": 29_500.0},
            {"strike": 29_600.0},
        ]
    without_receipts = evaluate_monitor_policy(
        _payload(current_first, current_second, state=stale_state)
    )
    assert _event(without_receipts, "GAMMA_PIN_SHIFT") is None
    assert "gamma_pin_change_below_significant_threshold" in without_receipts[
        "suppressions"
    ]

    recovered = evaluate_monitor_policy(
        _payload(
            current_first,
            current_second,
            state=stale_state,
            durable_event_receipts=[gamma_receipt, max_receipt],
        )
    )
    assert recovered["accepted"] is True
    assert len(recovered["events"]) == 2
    recovered_gamma = _event(recovered, "GAMMA_PIN_SHIFT")
    recovered_max = _event(recovered, "MAX_PAIN_CHANGE")
    assert recovered_gamma is not None and recovered_max is not None
    assert recovered_gamma["durable_receipt_applied"] is True
    assert recovered_max["durable_receipt_applied"] is True
    assert recovered_gamma["event_id"] == gamma_receipt["event_id"]
    assert recovered_max["event_id"] == max_receipt["event_id"]
    assert recovered_gamma["confirmation_observation_ids"] == gamma_receipt[
        "confirmation_observation_ids"
    ]
    assert recovered_gamma["threshold_evidence"] == gamma_receipt[
        "threshold_evidence"
    ]
    assert recovered["next_state"]["levels"]["gamma_pin"]["last_event_id"] == (
        gamma_receipt["event_id"]
    )
    assert recovered["next_state"]["levels"]["max_pain"]["last_event_id"] == (
        max_receipt["event_id"]
    )
    assert (
        recovered["next_state"]["last_accepted_observed_at_utc"]
        == "2026-09-08T14:15:00Z"
    )


def test_durable_gamma_receipts_chain_in_confirmation_order_before_current_pair() -> None:
    gamma_provenance = {
        "symbol": "NDX",
        "provider": "databento",
        "subscription_generation": 7,
        "subscription_epoch_id": "e" * 64,
        "primary_expiration": "2026-09-08",
        "universe_sha256": "a" * 64,
        "universe_is_fallback": False,
        "formula_version": "databento-gex-v2-call-minus-put",
    }
    stale_state = {
        "session_date": "2026-09-08",
        "last_accepted_observed_at_utc": "2026-09-08T13:55:00Z",
        "levels": {
            "gamma_pin": {"value": 29_600.0, "provenance": gamma_provenance}
        },
        "extension": {"must": "survive"},
    }
    first = evaluate_monitor_policy(
        _payload(
            _observation("2026-09-08T14:00:00Z", gamma_pin=29_700.0),
            _observation("2026-09-08T14:05:00Z", gamma_pin=29_700.0),
            state=stale_state,
        )
    )
    first_receipt = copy.deepcopy(_event(first, "GAMMA_PIN_SHIFT"))
    assert first_receipt is not None

    second = evaluate_monitor_policy(
        _payload(
            _observation("2026-09-08T14:10:00Z", gamma_pin=29_800.0),
            _observation("2026-09-08T14:15:00Z", gamma_pin=29_800.0),
            state=first["next_state"],
        )
    )
    second_receipt = copy.deepcopy(_event(second, "GAMMA_PIN_SHIFT"))
    assert second_receipt is not None
    assert second_receipt["predecessor"] == {
        "kind": "event",
        "id": first_receipt["event_id"],
    }

    recovered = evaluate_monitor_policy(
        _payload(
            _observation("2026-09-08T14:20:00Z", gamma_pin=29_900.0),
            _observation("2026-09-08T14:25:00Z", gamma_pin=29_900.0),
            state=stale_state,
            # Deliberately reverse the input. Receipts are replayed by their
            # confirmed time, not by array position or event type.
            durable_event_receipts=[second_receipt, first_receipt],
        )
    )

    gamma_events = [
        event for event in recovered["events"] if event["type"] == "GAMMA_PIN_SHIFT"
    ]
    assert recovered["accepted"] is True
    assert [event["event_id"] for event in gamma_events[:2]] == [
        first_receipt["event_id"],
        second_receipt["event_id"],
    ]
    assert all(event["durable_receipt_applied"] is True for event in gamma_events[:2])
    assert gamma_events[0]["threshold_evidence"] == first_receipt[
        "threshold_evidence"
    ]
    assert gamma_events[1]["threshold_evidence"] == second_receipt[
        "threshold_evidence"
    ]
    assert len(gamma_events) == 3
    assert gamma_events[2]["old_value"] == 29_800.0
    assert gamma_events[2]["new_value"] == 29_900.0
    assert gamma_events[2]["predecessor"] == {
        "kind": "event",
        "id": second_receipt["event_id"],
    }
    assert "durable_receipt_applied" not in gamma_events[2]
    assert recovered["next_state"]["levels"]["gamma_pin"]["value"] == 29_900.0
    assert recovered["next_state"]["extension"] == {"must": "survive"}

    missing_first_edge = evaluate_monitor_policy(
        _payload(
            _observation("2026-09-08T14:20:00Z", gamma_pin=29_900.0),
            _observation("2026-09-08T14:25:00Z", gamma_pin=29_900.0),
            state=stale_state,
            durable_event_receipts=[second_receipt],
        )
    )
    assert missing_first_edge["accepted"] is False
    assert missing_first_edge["events"] == []
    assert missing_first_edge["next_state"] == stale_state
    assert any(
        issue.endswith((".old_value_mismatch", ".predecessor_mismatch"))
        for issue in missing_first_edge["issues"]
    )

    nonadvancing_second = copy.deepcopy(second_receipt)
    for field in (
        "confirmation_observation_ids",
        "confirmation_observed_at_utc",
        "confirmation_observed_at_ct",
        "confirmed_at_utc",
        "confirmed_at_ct",
    ):
        nonadvancing_second[field] = copy.deepcopy(first_receipt[field])
    nonadvancing = evaluate_monitor_policy(
        _payload(
            _observation("2026-09-08T14:20:00Z", gamma_pin=29_900.0),
            _observation("2026-09-08T14:25:00Z", gamma_pin=29_900.0),
            state=stale_state,
            durable_event_receipts=[first_receipt, nonadvancing_second],
        )
    )
    assert nonadvancing["accepted"] is False
    assert nonadvancing["events"] == []
    assert nonadvancing["next_state"] == stale_state
    assert (
        "durable_event_receipts.gamma_pin_confirmation_must_advance"
        in nonadvancing["issues"]
    )


def test_durable_max_pain_receipt_allows_newer_third_provenance() -> None:
    max_pain_provenance = {
        "symbol": "NDX",
        "provider": "databento",
        "subscription_generation": 7,
        "subscription_epoch_id": "e" * 64,
        "primary_expiration": "2026-09-08",
        "universe_sha256": "a" * 64,
        "universe_is_fallback": False,
        "formula_version": "full-oi-max-pain-v1",
        "source": "full-oi-universe",
        "as_of": "2026-09-08",
    }
    stale_state = {
        "session_date": "2026-09-08",
        "last_accepted_observed_at_utc": "2026-09-08T13:55:00Z",
        "levels": {
            "max_pain": {"value": 29_480.0, "provenance": max_pain_provenance}
        },
    }
    original = evaluate_monitor_policy(
        _payload(
            _observation("2026-09-08T14:00:00Z", max_pain=29_500.0),
            _observation("2026-09-08T14:05:00Z", max_pain=29_500.0),
            state=stale_state,
        )
    )
    receipt = copy.deepcopy(_event(original, "MAX_PAIN_CHANGE"))
    assert receipt is not None

    current_first = _observation(
        "2026-09-08T14:10:00Z", max_pain=29_525.0
    )
    current_second = _observation(
        "2026-09-08T14:15:00Z", max_pain=29_525.0
    )
    for observation in (current_first, current_second):
        observation["universe_sha256"] = "b" * 64

    recovered = evaluate_monitor_policy(
        _payload(
            current_first,
            current_second,
            state=stale_state,
            durable_event_receipts=[receipt],
        )
    )
    max_events = [
        event
        for event in recovered["events"]
        if event["type"].startswith("MAX_PAIN_")
    ]
    assert recovered["accepted"] is True
    assert [event["type"] for event in max_events] == [
        "MAX_PAIN_CHANGE",
        "MAX_PAIN_PROVENANCE_RESET",
    ]
    assert max_events[0]["durable_receipt_applied"] is True
    assert max_events[0]["event_id"] == receipt["event_id"]
    assert max_events[1]["old_value"] == 29_500.0
    assert max_events[1]["new_value"] == 29_525.0
    assert max_events[1]["predecessor"] == {
        "kind": "event",
        "id": receipt["event_id"],
    }
    assert max_events[1]["current_provenance"]["universe_sha256"] == "b" * 64
    assert recovered["next_state"]["levels"]["max_pain"]["value"] == 29_525.0


def test_durable_directional_receipt_recovers_before_neutral_current_pair() -> None:
    original_first = _observation(
        "2026-09-08T14:00:00Z", normalized_net_gex=0.20
    )
    original_second = _observation(
        "2026-09-08T14:05:00Z", normalized_net_gex=-0.20
    )
    for row in (original_first, original_second):
        row["price_change_15m_pct"] = -0.25
    original = evaluate_monitor_policy(_payload(original_first, original_second))
    directional_receipt = copy.deepcopy(_event(original, "DIRECTIONAL_SHIFT"))
    assert directional_receipt is not None

    neutral_first = _observation(
        "2026-09-08T14:10:00Z", normalized_net_gex=-0.19
    )
    neutral_second = _observation(
        "2026-09-08T14:15:00Z", normalized_net_gex=-0.18
    )
    recovered = evaluate_monitor_policy(
        _payload(
            neutral_first,
            neutral_second,
            durable_event_receipts=[directional_receipt],
        )
    )
    recovered_event = _event(recovered, "DIRECTIONAL_SHIFT")
    assert recovered["accepted"] is True
    assert len(recovered["events"]) == 1
    assert recovered_event is not None
    assert recovered_event["durable_receipt_applied"] is True
    assert recovered_event["price_confirmations"] == directional_receipt[
        "price_confirmations"
    ]
    assert recovered_event["structure_confirmations"] == directional_receipt[
        "structure_confirmations"
    ]
    latch = recovered["next_state"]["directional_latch"]
    assert latch["active_direction"] == "bearish"
    assert latch["last_event_id"] == directional_receipt["event_id"]
    assert latch["neutral_eligible_scan_count"] == 1
    assert latch["pending_rearm_observations"][0]["observation_id"] == neutral_second[
        "observation_id"
    ]


def test_durable_receipt_semantic_mismatches_reject_without_state_change() -> None:
    gamma_provenance = {
        "symbol": "NDX",
        "provider": "databento",
        "subscription_generation": 7,
        "subscription_epoch_id": "e" * 64,
        "primary_expiration": "2026-09-08",
        "universe_sha256": "a" * 64,
        "universe_is_fallback": False,
        "formula_version": "databento-gex-v2-call-minus-put",
    }
    stale_state = {
        "session_date": "2026-09-08",
        "last_accepted_observed_at_utc": "2026-09-08T13:55:00Z",
        "levels": {
            "gamma_pin": {"value": 29_600.0, "provenance": gamma_provenance}
        },
        "extension": {"must": "survive"},
    }
    first = _observation("2026-09-08T14:00:00Z", gamma_pin=29_700.0)
    second = _observation("2026-09-08T14:05:00Z", gamma_pin=29_700.0)
    original = evaluate_monitor_policy(_payload(first, second, state=stale_state))
    durable_event = copy.deepcopy(_event(original, "GAMMA_PIN_SHIFT"))
    assert durable_event is not None

    mismatches: list[dict] = []
    for field, value in (
        ("event_id", "f" * 64),
        ("type", "MAX_PAIN_CHANGE"),
        ("symbol", "SPX"),
        ("old_value", 29_599.0),
        ("new_value", 29_701.0),
    ):
        receipt = copy.deepcopy(durable_event)
        receipt[field] = value
        mismatches.append(receipt)
    for provenance_field in ("prior_provenance", "current_provenance"):
        receipt = copy.deepcopy(durable_event)
        receipt[provenance_field]["universe_sha256"] = "b" * 64
        mismatches.append(receipt)
        receipt = copy.deepcopy(durable_event)
        receipt[provenance_field]["subscription_epoch_id"] = "f" * 64
        mismatches.append(receipt)

    for mismatched_receipt in mismatches:
        result = evaluate_monitor_policy(
            _payload(
                first,
                second,
                state=stale_state,
                durable_event_receipts=[mismatched_receipt],
            )
        )
        assert result["accepted"] is False
        assert result["events"] == []
        assert any("durable_event_receipt:" in issue for issue in result["issues"])
        assert result["next_state"] == stale_state


def test_durable_receipt_rejects_tampered_or_temporally_incompatible_evidence() -> None:
    gamma_provenance = {
        "symbol": "NDX",
        "provider": "databento",
        "subscription_generation": 7,
        "subscription_epoch_id": "e" * 64,
        "primary_expiration": "2026-09-08",
        "universe_sha256": "a" * 64,
        "universe_is_fallback": False,
        "formula_version": "databento-gex-v2-call-minus-put",
    }
    stale_state = {
        "session_date": "2026-09-08",
        "last_accepted_observed_at_utc": "2026-09-08T13:55:00Z",
        "levels": {
            "gamma_pin": {"value": 29_600.0, "provenance": gamma_provenance}
        },
    }
    first = _observation("2026-09-08T14:00:00Z", gamma_pin=29_700.0)
    second = _observation("2026-09-08T14:05:00Z", gamma_pin=29_700.0)
    third = _observation("2026-09-08T14:10:00Z", gamma_pin=29_700.0)
    original = evaluate_monitor_policy(_payload(first, second, state=stale_state))
    durable_event = copy.deepcopy(_event(original, "GAMMA_PIN_SHIFT"))
    assert durable_event is not None

    tampered_threshold = copy.deepcopy(durable_event)
    tampered_threshold["threshold_evidence"]["threshold_points"] = 999_999.0

    future_evidence = copy.deepcopy(durable_event)
    future_evidence["confirmation_observation_ids"] = ["future-one", "future-two"]
    future_evidence["confirmation_observed_at_utc"] = [
        "2026-09-08T16:00:00Z",
        "2026-09-08T16:05:00Z",
    ]
    future_evidence["confirmation_observed_at_ct"] = [
        "2026-09-08T11:00:00-05:00",
        "2026-09-08T11:05:00-05:00",
    ]
    future_evidence["confirmed_at_utc"] = "2026-09-08T16:05:00Z"
    future_evidence["confirmed_at_ct"] = "2026-09-08T11:05:00-05:00"

    regressive_evidence = copy.deepcopy(durable_event)
    regressive_evidence["confirmation_observation_ids"] = ["old-one", "old-two"]
    regressive_evidence["confirmation_observed_at_utc"] = [
        "2026-09-08T13:45:00Z",
        "2026-09-08T13:50:00Z",
    ]
    regressive_evidence["confirmation_observed_at_ct"] = [
        "2026-09-08T08:45:00-05:00",
        "2026-09-08T08:50:00-05:00",
    ]
    regressive_evidence["confirmed_at_utc"] = "2026-09-08T13:50:00Z"
    regressive_evidence["confirmed_at_ct"] = "2026-09-08T08:50:00-05:00"

    malformed_time = copy.deepcopy(durable_event)
    malformed_time["confirmation_observed_at_utc"][0] = "not-a-timestamp"

    incompatible_id = copy.deepcopy(durable_event)
    incompatible_id["confirmation_observation_ids"][-1] = "wrong-id-at-14:05"

    for receipt in (
        tampered_threshold,
        future_evidence,
        regressive_evidence,
        malformed_time,
        incompatible_id,
    ):
        result = evaluate_monitor_policy(
            _payload(
                second,
                third,
                state=stale_state,
                durable_event_receipts=[receipt],
            )
        )
        assert result["accepted"] is False
        assert result["events"] == []
        assert any("durable_event_receipt:" in issue for issue in result["issues"])
        assert result["next_state"] == stale_state


def test_durable_level_receipt_rejects_epochless_prior_provenance() -> None:
    provenance = {
        "symbol": "NDX",
        "provider": "databento",
        "subscription_generation": 7,
        "subscription_epoch_id": "e" * 64,
        "primary_expiration": "2026-09-08",
        "universe_sha256": "a" * 64,
        "universe_is_fallback": False,
        "formula_version": "databento-gex-v2-call-minus-put",
    }
    state = {
        "session_date": "2026-09-08",
        "last_accepted_observed_at_utc": "2026-09-08T13:55:00Z",
        "levels": {"gamma_pin": {"value": 29_600.0, "provenance": provenance}},
    }
    first = _observation("2026-09-08T14:00:00Z", gamma_pin=29_700.0)
    second = _observation("2026-09-08T14:05:00Z", gamma_pin=29_700.0)
    emitted = evaluate_monitor_policy(_payload(first, second, state=state))
    receipt = copy.deepcopy(_event(emitted, "GAMMA_PIN_SHIFT"))
    assert receipt is not None

    legacy_state = copy.deepcopy(state)
    legacy_state["levels"]["gamma_pin"]["provenance"].pop(
        "subscription_epoch_id"
    )
    receipt["prior_provenance"].pop("subscription_epoch_id")
    third = _observation("2026-09-08T14:10:00Z", gamma_pin=29_700.0)
    rejected = evaluate_monitor_policy(
        _payload(
            second,
            third,
            state=legacy_state,
            durable_event_receipts=[receipt],
        )
    )

    assert rejected["accepted"] is False
    assert rejected["events"] == []
    assert any(
        issue.endswith("prior_provenance_fields_invalid")
        for issue in rejected["issues"]
    )
    assert rejected["next_state"] == legacy_state


def test_durable_directional_receipt_requires_complete_supporting_evidence() -> None:
    first = _observation("2026-09-08T14:00:00Z", normalized_net_gex=0.20)
    second = _observation("2026-09-08T14:05:00Z", normalized_net_gex=-0.20)
    third = _observation("2026-09-08T14:10:00Z", normalized_net_gex=-0.50)
    for row in (first, second, third):
        row["price_change_15m_pct"] = -0.25
    original = evaluate_monitor_policy(_payload(first, second))
    durable_event = copy.deepcopy(_event(original, "DIRECTIONAL_SHIFT"))
    assert durable_event is not None

    valid_retry = evaluate_monitor_policy(
        _payload(second, third, durable_event_receipts=[durable_event])
    )
    valid_event = _event(valid_retry, "DIRECTIONAL_SHIFT")
    assert valid_retry["accepted"] is True
    assert valid_event is not None
    assert valid_event["durable_receipt_applied"] is True

    cross_epoch_receipt = copy.deepcopy(durable_event)
    cross_epoch_receipt["alignment_provenance"]["subscription_epoch_id"] = (
        "f" * 64
    )
    cross_epoch = evaluate_monitor_policy(
        _payload(second, third, durable_event_receipts=[cross_epoch_receipt])
    )
    assert cross_epoch["accepted"] is False
    assert cross_epoch["events"] == []
    assert any(issue.endswith("event_id_mismatch") for issue in cross_epoch["issues"])

    invalid_gex_receipt = copy.deepcopy(durable_event)
    gex_confirmation = next(
        row
        for row in invalid_gex_receipt["structure_confirmations"]
        if row["type"] == "normalized_net_gex_regime_change"
    )
    gex_confirmation["previous"] = 0.20
    gex_confirmation["current"] = 0.10
    invalid_gex = evaluate_monitor_policy(
        _payload(second, third, durable_event_receipts=[invalid_gex_receipt])
    )
    assert invalid_gex["accepted"] is False
    assert invalid_gex["events"] == []
    assert any("structure_confirmations[0]_incomplete" in issue for issue in invalid_gex["issues"])

    durable_event["price_confirmations"] = []
    durable_event["structure_confirmations"] = []
    durable_event["analytical_estimate_only"] = False

    result = evaluate_monitor_policy(
        _payload(second, third, durable_event_receipts=[durable_event])
    )
    assert result["accepted"] is False
    assert result["events"] == []
    assert any("price_confirmations_must_be_nonempty" in issue for issue in result["issues"])
    assert any(
        "structure_confirmations_must_be_nonempty" in issue
        for issue in result["issues"]
    )


def test_max_pain_formula_and_as_of_are_required_for_baseline_or_alert() -> None:
    provenance = {
        "symbol": "NDX",
        "provider": "databento",
        "subscription_generation": 7,
        "subscription_epoch_id": "e" * 64,
        "primary_expiration": "2026-09-08",
        "universe_sha256": "a" * 64,
        "universe_is_fallback": False,
        "formula_version": "full-oi-max-pain-v1",
        "source": "full-oi-universe",
        "as_of": "2026-09-04",
    }
    state = {
        "session_date": "2026-09-08",
        "levels": {"max_pain": {"value": 29_480.0, "provenance": provenance}},
    }
    for missing_field in ("max_pain_formula_version", "max_pain_as_of"):
        first = _observation("2026-09-08T14:00:00Z", max_pain=29_500.0)
        second = _observation("2026-09-08T14:05:00Z", max_pain=29_500.0)
        first[missing_field] = None
        second[missing_field] = None
        result = evaluate_monitor_policy(_payload(first, second, state=state))
        assert result["accepted"] is True
        assert _event(result, "MAX_PAIN_CHANGE") is None
        assert _event(result, "MAX_PAIN_PROVENANCE_RESET") is None
        assert "max_pain_provenance_incomplete" in result["suppressions"]
        assert result["next_state"]["levels"]["max_pain"] == state["levels"]["max_pain"]


def test_gamma_pin_shift_requires_confirmation_and_contest_gate() -> None:
    first = _observation("2026-09-08T14:00:00Z", gamma_pin=29_700.0)
    second = _observation("2026-09-08T14:05:00Z", gamma_pin=29_700.0)
    provenance = {
        "symbol": "NDX",
        "provider": "databento",
        "subscription_generation": 7,
        "subscription_epoch_id": "e" * 64,
        "primary_expiration": "2026-09-08",
        "universe_sha256": "a" * 64,
        "universe_is_fallback": False,
        "formula_version": "databento-gex-v2-call-minus-put",
    }
    state = {
        "session_date": "2026-09-08",
        "levels": {"gamma_pin": {"value": 29_600.0, "provenance": provenance}},
    }
    result = evaluate_monitor_policy(_payload(first, second, state=state))
    event = _event(result, "GAMMA_PIN_SHIFT")
    assert event is not None
    assert event["threshold_points"] == 50.0

    first["pin_is_contested"] = True
    second["pin_is_contested"] = True
    contested = evaluate_monitor_policy(_payload(first, second, state=state))
    assert _event(contested, "GAMMA_PIN_SHIFT") is None
    assert "gamma_pin_shift_suppressed_contested_leadership" in contested["suppressions"]


def test_directional_latch_rearms_after_two_new_neutral_scans() -> None:
    first = _observation(
        "2026-09-08T14:00:00Z",
        forecast_bias="bullish",
        expected_move_pct=0.25,
        normalized_net_gex=0.20,
    )
    second = _observation(
        "2026-09-08T14:05:00Z",
        forecast_bias="bearish",
        expected_move_pct=-0.20,
        normalized_net_gex=-0.20,
    )
    initial = evaluate_monitor_policy(_payload(first, second))
    initial_event = _event(initial, "DIRECTIONAL_SHIFT")
    assert initial_event is not None
    initial_latch = initial["next_state"]["directional_latch"]
    assert initial_latch["last_alert_confirmation_observation_ids"] == [
        first["observation_id"],
        second["observation_id"],
    ]
    assert initial_latch["last_alert_confirmation_observed_at_utc"] == [
        "2026-09-08T14:00:00Z",
        "2026-09-08T14:05:00Z",
    ]
    assert initial_latch["last_alert_confirmation_observed_at_ct"] == [
        "2026-09-08T09:00:00-05:00",
        "2026-09-08T09:05:00-05:00",
    ]
    assert initial_latch["last_alert_confirmed_at_utc"] == "2026-09-08T14:05:00Z"
    assert initial_latch["last_alert_confirmed_at_ct"] == "2026-09-08T09:05:00-05:00"

    replay = evaluate_monitor_policy(_payload(first, second, state=initial["next_state"]))
    assert replay["accepted"] is False
    assert "latest_observation_not_after_policy_watermark" in replay["issues"]

    repeated_first = _observation(
        "2026-09-08T14:10:00Z",
        forecast_bias="bullish",
        expected_move_pct=0.25,
        normalized_net_gex=0.20,
    )
    repeated_second = _observation(
        "2026-09-08T14:15:00Z",
        forecast_bias="bearish",
        expected_move_pct=-0.20,
        normalized_net_gex=-0.20,
    )
    repeated = evaluate_monitor_policy(
        _payload(repeated_first, repeated_second, state=initial["next_state"])
    )
    assert repeated["accepted"] is True
    assert _event(repeated, "DIRECTIONAL_SHIFT") is None
    assert "directional_same_direction_not_rearmed" in repeated["suppressions"]

    neutral_one = _observation("2026-09-08T14:20:00Z", normalized_net_gex=-0.19)
    after_one = evaluate_monitor_policy(
        _payload(repeated_second, neutral_one, state=repeated["next_state"])
    )
    assert after_one["directional_rearmed"] is False
    after_one_latch = after_one["next_state"]["directional_latch"]
    pending = after_one_latch["pending_rearm_observations"]
    assert len(pending) == 1
    assert pending[0]["observation_id"] == neutral_one["observation_id"]
    assert pending[0]["observed_at_utc"] == "2026-09-08T14:20:00Z"
    assert pending[0]["observed_at_ct"] == "2026-09-08T09:20:00-05:00"
    assert pending[0]["alignment_provenance"] == initial_event[
        "alignment_provenance"
    ]

    neutral_two = _observation("2026-09-08T14:25:00Z", normalized_net_gex=-0.18)
    after_two = evaluate_monitor_policy(
        _payload(neutral_one, neutral_two, state=after_one["next_state"])
    )
    assert after_two["directional_rearmed"] is True
    rearmed_latch = after_two["next_state"]["directional_latch"]
    assert rearmed_latch["active_direction"] is None
    assert rearmed_latch["last_rearm_observation_ids"] == [
        neutral_one["observation_id"],
        neutral_two["observation_id"],
    ]
    assert rearmed_latch["last_rearm_observed_at_utc"] == [
        "2026-09-08T14:20:00Z",
        "2026-09-08T14:25:00Z",
    ]
    assert rearmed_latch["last_rearm_observed_at_ct"] == [
        "2026-09-08T09:20:00-05:00",
        "2026-09-08T09:25:00-05:00",
    ]
    assert rearmed_latch["last_rearmed_at_utc"] == "2026-09-08T14:25:00Z"
    assert rearmed_latch["last_rearmed_at_ct"] == "2026-09-08T09:25:00-05:00"

    bullish_before_flip = _observation(
        "2026-09-08T14:30:00Z",
        forecast_bias="bullish",
        expected_move_pct=0.25,
        normalized_net_gex=0.20,
    )
    bearish_two = _observation(
        "2026-09-08T14:35:00Z",
        forecast_bias="bearish",
        expected_move_pct=-0.20,
        normalized_net_gex=-0.20,
    )
    re_alert = evaluate_monitor_policy(
        _payload(bullish_before_flip, bearish_two, state=after_two["next_state"])
    )
    re_alert_event = _event(re_alert, "DIRECTIONAL_SHIFT")
    assert re_alert_event is not None
    assert re_alert_event["event_id"] != initial_event["event_id"]
    re_alert_latch = re_alert["next_state"]["directional_latch"]
    assert re_alert_latch["last_alert_confirmation_observation_ids"] == [
        bullish_before_flip["observation_id"],
        bearish_two["observation_id"],
    ]
    assert re_alert_latch["last_alert_confirmed_at_utc"] == "2026-09-08T14:35:00Z"
    assert re_alert_latch["last_alert_confirmed_at_ct"] == "2026-09-08T09:35:00-05:00"
    assert re_alert_latch["last_rearm_observation_ids"] == rearmed_latch[
        "last_rearm_observation_ids"
    ]


def test_directional_identity_changes_with_capture_epoch() -> None:
    def qualifying_pair(epoch: str) -> tuple[dict, dict]:
        first = _observation(
            "2026-09-08T14:00:00Z",
            forecast_bias="bullish",
            expected_move_pct=0.25,
            normalized_net_gex=0.20,
        )
        second = _observation(
            "2026-09-08T14:05:00Z",
            forecast_bias="bearish",
            expected_move_pct=-0.20,
            normalized_net_gex=-0.20,
        )
        for observation in (first, second):
            observation["subscription_epoch_id"] = epoch
        return first, second

    epoch_e = _event(
        evaluate_monitor_policy(_payload(*qualifying_pair("e" * 64))),
        "DIRECTIONAL_SHIFT",
    )
    epoch_f = _event(
        evaluate_monitor_policy(_payload(*qualifying_pair("f" * 64))),
        "DIRECTIONAL_SHIFT",
    )

    assert epoch_e is not None and epoch_f is not None
    assert epoch_e["event_id"] != epoch_f["event_id"]
    assert epoch_e["alignment_provenance"]["subscription_epoch_id"] == "e" * 64
    assert epoch_f["alignment_provenance"]["subscription_epoch_id"] == "f" * 64


def test_directional_rearm_never_combines_neutral_markers_across_epochs() -> None:
    first = _observation(
        "2026-09-08T14:00:00Z",
        forecast_bias="bullish",
        expected_move_pct=0.25,
        normalized_net_gex=0.20,
    )
    second = _observation(
        "2026-09-08T14:05:00Z",
        forecast_bias="bearish",
        expected_move_pct=-0.20,
        normalized_net_gex=-0.20,
    )
    initial = evaluate_monitor_policy(_payload(first, second))
    assert _event(initial, "DIRECTIONAL_SHIFT") is not None

    old_epoch_neutral = evaluate_monitor_policy(
        _payload(
            _observation("2026-09-08T14:10:00Z", normalized_net_gex=-0.19),
            _observation("2026-09-08T14:15:00Z", normalized_net_gex=-0.18),
            state=initial["next_state"],
        )
    )
    assert old_epoch_neutral["directional_rearmed"] is False
    assert len(
        old_epoch_neutral["next_state"]["directional_latch"][
            "pending_rearm_observations"
        ]
    ) == 1

    new_epoch_pair = [
        _observation("2026-09-08T14:20:00Z", normalized_net_gex=-0.17),
        _observation("2026-09-08T14:25:00Z", normalized_net_gex=-0.16),
    ]
    for observation in new_epoch_pair:
        observation["subscription_epoch_id"] = "f" * 64
    after_restart = evaluate_monitor_policy(
        _payload(*new_epoch_pair, state=old_epoch_neutral["next_state"])
    )
    assert after_restart["directional_rearmed"] is False
    assert "directional_alignment_provenance_rebaselined" in after_restart[
        "suppressions"
    ]
    pending = after_restart["next_state"]["directional_latch"][
        "pending_rearm_observations"
    ]
    assert len(pending) == 1
    assert pending[0]["alignment_provenance"]["subscription_epoch_id"] == "f" * 64

    next_new_epoch = _observation(
        "2026-09-08T14:30:00Z", normalized_net_gex=-0.15
    )
    next_new_epoch["subscription_epoch_id"] = "f" * 64
    rearmed = evaluate_monitor_policy(
        _payload(
            new_epoch_pair[-1],
            next_new_epoch,
            state=after_restart["next_state"],
        )
    )
    assert rearmed["directional_rearmed"] is True


def test_cross_session_observations_cannot_confirm_an_alert() -> None:
    friday = _observation(
        "2026-09-04T20:00:00Z",
        forecast_bias="bearish",
        expected_move_pct=-0.25,
        normalized_net_gex=0.20,
    )
    tuesday = _observation(
        "2026-09-08T14:00:00Z",
        forecast_bias="bearish",
        expected_move_pct=-0.20,
        normalized_net_gex=-0.20,
    )
    result = evaluate_monitor_policy(_payload(friday, tuesday))
    assert result["accepted"] is False
    assert "alignment_mismatch:session_date_ct" in result["issues"]
    assert result["events"] == []


def test_duplicate_observation_ids_and_non_sha_provenance_fail_closed() -> None:
    first = _observation("2026-09-08T14:00:00Z")
    second = _observation("2026-09-08T14:05:00Z")
    second["observation_id"] = first["observation_id"]
    first["universe_sha256"] = "z" * 64
    second["universe_sha256"] = "z" * 64
    result = evaluate_monitor_policy(_payload(first, second))
    assert result["accepted"] is False
    assert "observations_must_have_unique_ids" in result["issues"]
    assert any(
        issue.endswith("universe_sha256_must_be_sha256")
        for issue in result["issues"]
    )
    assert result["events"] == []


def test_policy_watermark_rejects_replay_and_is_inferred_from_rich_state() -> None:
    first = _observation("2026-09-08T14:00:00Z")
    second = _observation("2026-09-08T14:05:00Z")
    initial = evaluate_monitor_policy(_payload(first, second))
    assert initial["accepted"] is True
    assert (
        initial["next_state"]["last_accepted_observed_at_utc"]
        == "2026-09-08T14:05:00Z"
    )

    replay = evaluate_monitor_policy(_payload(first, second, state=initial["next_state"]))
    assert replay["accepted"] is False
    assert "latest_observation_not_after_policy_watermark" in replay["issues"]
    assert replay["next_state"] == initial["next_state"]

    inferred_state = copy.deepcopy(initial["next_state"])
    del inferred_state["last_accepted_observed_at_utc"]
    inferred_replay = evaluate_monitor_policy(
        _payload(first, second, state=inferred_state)
    )
    assert inferred_replay["accepted"] is False
    assert "latest_observation_not_after_policy_watermark" in inferred_replay["issues"]
    assert inferred_replay["next_state"] == inferred_state

    third = _observation("2026-09-08T14:10:00Z")
    advancing = evaluate_monitor_policy(
        _payload(second, third, state=initial["next_state"])
    )
    assert advancing["accepted"] is True
    assert (
        advancing["next_state"]["last_accepted_observed_at_utc"]
        == "2026-09-08T14:10:00Z"
    )


def test_session_rollover_only_moves_forward_and_rejects_invalid_watermark() -> None:
    future_state = {
        "session_date": "2026-09-08",
        "last_accepted_observed_at_utc": "2026-09-08T14:05:00Z",
        "levels": {},
        "directional_latch": {},
    }
    friday_first = _observation("2026-09-04T14:00:00Z")
    friday_second = _observation("2026-09-04T14:05:00Z")
    rollback = evaluate_monitor_policy(
        _payload(friday_first, friday_second, state=future_state)
    )
    assert rollback["accepted"] is False
    assert "policy_state_session_rollback_rejected" in rollback["issues"]
    assert rollback["policy_state_reset_for_session_rollover"] is False
    assert rollback["next_state"] == future_state

    invalid_watermark_state = copy.deepcopy(future_state)
    invalid_watermark_state["last_accepted_observed_at_utc"] = "not-a-timestamp"
    tuesday_first = _observation("2026-09-08T14:10:00Z")
    tuesday_second = _observation("2026-09-08T14:15:00Z")
    invalid_watermark = evaluate_monitor_policy(
        _payload(tuesday_first, tuesday_second, state=invalid_watermark_state)
    )
    assert invalid_watermark["accepted"] is False
    assert "policy_state.last_accepted_observed_at_utc_invalid" in invalid_watermark[
        "issues"
    ]
    assert invalid_watermark["next_state"] == invalid_watermark_state

    inconsistent_watermark_state = {
        "session_date": "2026-09-08",
        "last_accepted_observed_at_utc": "2026-09-08T14:00:00Z",
        "levels": {
            "gamma_pin": {
                "value": 29_600.0,
                "provenance": {"retained": True},
                "confirmed_at_utc": "2026-09-08T14:10:00Z",
                "confirmation_observed_at_utc": [
                    "2026-09-08T14:05:00Z",
                    "2026-09-08T14:10:00Z",
                ],
            }
        },
    }
    regressive = evaluate_monitor_policy(
        _payload(
            _observation("2026-09-08T14:00:00Z"),
            _observation("2026-09-08T14:05:00Z"),
            state=inconsistent_watermark_state,
        )
    )
    assert regressive["accepted"] is False
    assert "policy_state_watermark_precedes_persisted_evidence" in regressive["issues"]
    assert "latest_observation_not_after_policy_watermark" in regressive["issues"]
    assert regressive["next_state"] == inconsistent_watermark_state


def test_policy_session_date_and_watermarks_must_be_canonical_and_aligned() -> None:
    observations = (
        _observation("2026-09-08T14:10:00Z"),
        _observation("2026-09-08T14:15:00Z"),
    )
    noncanonical_date = {
        "session_date": "20260908",
        "last_accepted_observed_at_utc": "2026-09-08T14:05:00Z",
    }
    invalid_date = evaluate_monitor_policy(
        _payload(*observations, state=noncanonical_date)
    )
    assert invalid_date["accepted"] is False
    assert "policy_state.session_date_invalid" in invalid_date["issues"]
    assert invalid_date["next_state"] == noncanonical_date

    for missing_value in (None, ""):
        present_but_missing = {"session_date": missing_value}
        missing_date = evaluate_monitor_policy(
            _payload(*observations, state=present_but_missing)
        )
        assert missing_date["accepted"] is False
        assert "policy_state.session_date_invalid" in missing_date["issues"]
        assert missing_date["next_state"] == present_but_missing

    explicit_mismatch_state = {
        "session_date": "2026-09-08",
        "last_accepted_observed_at_utc": "2026-09-07T14:05:00Z",
    }
    explicit_mismatch = evaluate_monitor_policy(
        _payload(*observations, state=explicit_mismatch_state)
    )
    assert explicit_mismatch["accepted"] is False
    assert "policy_state_explicit_watermark_session_mismatch" in explicit_mismatch[
        "issues"
    ]
    assert explicit_mismatch["next_state"] == explicit_mismatch_state

    inferred_mismatch_state = {
        "session_date": "2026-09-08",
        "levels": {
            "gamma_pin": {
                "value": 29_600.0,
                "provenance": {"retained": True},
                "confirmed_at_utc": "2026-09-07T14:05:00Z",
                "confirmation_observed_at_utc": [
                    "2026-09-07T14:00:00Z",
                    "2026-09-07T14:05:00Z",
                ],
            }
        },
    }
    inferred_mismatch = evaluate_monitor_policy(
        _payload(*observations, state=inferred_mismatch_state)
    )
    assert inferred_mismatch["accepted"] is False
    assert "policy_state_inferred_watermark_session_mismatch" in inferred_mismatch[
        "issues"
    ]
    assert inferred_mismatch["next_state"] == inferred_mismatch_state

    historical_context_state = {
        "session_date": "2026-09-08",
        "last_accepted_observed_at_utc": "2026-09-08T14:05:00Z",
        "levels": {
            "gamma_pin": {
                "value": 29_600.0,
                "provenance": {"historical": True},
                "historical_context_only": True,
                "confirmed_at_utc": "2026-09-04T20:00:00Z",
            }
        },
    }
    historical_context = evaluate_monitor_policy(
        _payload(*observations, state=historical_context_state)
    )
    assert historical_context["accepted"] is True


def test_accepted_state_preserves_unknown_top_level_extensions() -> None:
    prior_state = {
        "session_date": "2026-09-08",
        "last_accepted_observed_at_utc": "2026-09-08T13:55:00Z",
        "levels": {},
        "directional_latch": {},
        "prior_session_reference": {
            "historical_context_only": True,
            "source_hash": "retain-me",
        },
        "wrapper_extension": {"cadence": "ELEVATED", "counter": 3},
    }
    result = evaluate_monitor_policy(
        _payload(
            _observation("2026-09-08T14:00:00Z"),
            _observation("2026-09-08T14:05:00Z"),
            state=prior_state,
        )
    )
    assert result["accepted"] is True
    assert result["next_state"]["prior_session_reference"] == prior_state[
        "prior_session_reference"
    ]
    assert result["next_state"]["wrapper_extension"] == prior_state[
        "wrapper_extension"
    ]
    assert (
        result["next_state"]["last_accepted_observed_at_utc"]
        == "2026-09-08T14:05:00Z"
    )


def test_prior_session_policy_state_is_reset_before_tuesday_baseline() -> None:
    first = _observation(
        "2026-09-08T14:00:00Z", gamma_pin=29_700.0, max_pain=29_500.0
    )
    second = _observation(
        "2026-09-08T14:05:00Z", gamma_pin=29_700.0, max_pain=29_500.0
    )
    stale_state = {
        "session_date": "2026-09-04",
        "levels": {
            "gamma_pin": {"value": 29_600.0, "provenance": {"stale": True}},
            "max_pain": {"value": 29_480.0, "provenance": {"stale": True}},
        },
        "directional_latch": {"active_direction": "bearish"},
    }
    result = evaluate_monitor_policy(_payload(first, second, state=stale_state))
    assert result["accepted"] is True
    assert result["session_date"] == "2026-09-08"
    assert result["prior_policy_session_date"] == "2026-09-04"
    assert result["policy_state_reset_for_session_rollover"] is True
    assert result["events"] == []
    assert result["next_state"]["session_date"] == "2026-09-08"
    for level in ("gamma_pin", "max_pain"):
        baseline = result["next_state"]["levels"][level]
        assert baseline["status"] == "baseline"
        assert baseline["confirmation_observation_ids"] == [
            first["observation_id"],
            second["observation_id"],
        ]
        assert baseline["confirmation_observed_at_utc"] == [
            "2026-09-08T14:00:00Z",
            "2026-09-08T14:05:00Z",
        ]
        assert baseline["confirmed_at_utc"] == "2026-09-08T14:05:00Z"
        assert baseline["confirmed_at_ct"] == "2026-09-08T09:05:00-05:00"
        assert baseline["current_provenance"] == baseline["provenance"]


def test_rollover_uses_two_tuesday_observations_for_clean_live_baseline() -> None:
    prior_provenance = {
        "symbol": "NDX",
        "provider": "databento",
        "subscription_generation": 6,
        "primary_expiration": "2026-09-04",
        "universe_sha256": "b" * 64,
        "universe_is_fallback": False,
        "formula_version": "full-oi-max-pain-v1",
        "source": "full-oi-universe",
        "as_of": "2026-09-04",
    }
    prior_state = {
        "session_date": "2026-09-04",
        "levels": {
            "max_pain": {
                "value": 29_480.0,
                "provenance": prior_provenance,
                "status": "alerted",
                "last_event_id": "friday-max-pain-event",
            }
        },
        "directional_latch": {"active_direction": "bearish"},
    }
    first = _observation("2026-09-08T14:00:00Z", max_pain=29_500.0)
    second = _observation("2026-09-08T14:05:00Z", max_pain=29_500.0)
    result = evaluate_monitor_policy(_payload(first, second, state=prior_state))
    assert result["policy_state_reset_for_session_rollover"] is True
    assert _event(result, "MAX_PAIN_PROVENANCE_RESET") is None
    assert "max_pain_baseline_initialized_without_alert" in result["suppressions"]
    baseline = result["next_state"]["levels"]["max_pain"]
    assert baseline["value"] == 29_500.0
    assert baseline["prior_provenance"] is None
    assert _event(result, "DIRECTIONAL_SHIFT") is None
    assert result["next_state"]["directional_latch"]["active_direction"] is None


def test_unchanged_legacy_rollover_value_still_builds_clean_baseline() -> None:
    prior_state = {
        "session_date": "2026-09-04",
        "levels": {
            "max_pain": {
                "value": 29_480.0,
                "provenance": {
                    "symbol": "NDX",
                    "provider": "databento",
                    "subscription_generation": 6,
                    "primary_expiration": "2026-09-04",
                    "universe_sha256": "b" * 64,
                    "universe_is_fallback": False,
                    "formula_version": "full-oi-max-pain-v1",
                    "source": "full-oi-universe",
                    "as_of": "2026-09-04",
                },
                "status": "alerted",
            }
        },
    }
    first = _observation("2026-09-08T14:00:00Z", max_pain=29_480.0)
    second = _observation("2026-09-08T14:05:00Z", max_pain=29_480.0)
    result = evaluate_monitor_policy(_payload(first, second, state=prior_state))
    assert _event(result, "MAX_PAIN_PROVENANCE_RESET") is None
    assert "max_pain_baseline_initialized_without_alert" in result["suppressions"]
    current = result["next_state"]["levels"]["max_pain"]
    assert current["value"] == 29_480.0
    assert current["provenance"]["primary_expiration"] == "2026-09-08"
    assert current["provenance"]["subscription_generation"] == 7
    assert current["status"] == "baseline"
    assert current["prior_provenance"] is None
    assert current["current_provenance"] == current["provenance"]
    assert current["confirmation_observation_ids"] == [
        first["observation_id"],
        second["observation_id"],
    ]
    assert current["confirmation_observed_at_utc"] == [
        "2026-09-08T14:00:00Z",
        "2026-09-08T14:05:00Z",
    ]
    assert current["confirmed_at_utc"] == "2026-09-08T14:05:00Z"
    assert current["confirmed_at_ct"] == "2026-09-08T09:05:00-05:00"
    assert "historical_context_only" not in current


def test_cli_reads_json_from_stdin_and_remains_read_only() -> None:
    first = _observation("2026-09-08T14:00:00Z")
    second = _observation("2026-09-08T14:05:00Z")
    completed = subprocess.run(
        [
            sys.executable,
            str(ROOT / "tools" / "evaluate_market_monitor_policy.py"),
            "--input",
            "-",
        ],
        input=json.dumps(_payload(first, second)),
        text=True,
        capture_output=True,
        check=False,
        cwd=ROOT,
    )
    assert completed.returncode == 0, completed.stderr
    result = json.loads(completed.stdout)
    assert result["schema_version"] == OUTPUT_SCHEMA
    assert result["accepted"] is True


def test_cli_malformed_json_fails_closed_with_machine_readable_result() -> None:
    completed = subprocess.run(
        [
            sys.executable,
            str(ROOT / "tools" / "evaluate_market_monitor_policy.py"),
            "--input",
            "-",
        ],
        input='{"schema_version":',
        text=True,
        capture_output=True,
        check=False,
        cwd=ROOT,
    )
    assert completed.returncode == 2
    result = json.loads(completed.stdout)
    assert result["accepted"] is False
    assert result["events"] == []
    assert result["issues"][0].startswith("input_error:JSONDecodeError:")


def test_cli_overflowing_generation_fails_closed_without_traceback() -> None:
    first = _observation("2026-09-08T14:00:00Z")
    second = _observation("2026-09-08T14:05:00Z")
    raw = json.dumps(_payload(first, second), separators=(",", ":")).replace(
        '"subscription_generation":7', '"subscription_generation":1e309'
    )
    completed = subprocess.run(
        [
            sys.executable,
            str(ROOT / "tools" / "evaluate_market_monitor_policy.py"),
            "--input",
            "-",
        ],
        input=raw,
        text=True,
        capture_output=True,
        check=False,
        cwd=ROOT,
    )
    assert completed.returncode == 1
    assert completed.stderr == ""
    result = json.loads(completed.stdout)
    assert result["accepted"] is False
    assert result["events"] == []
    assert any(
        issue.endswith("subscription_generation_must_be_positive")
        for issue in result["issues"]
    )
