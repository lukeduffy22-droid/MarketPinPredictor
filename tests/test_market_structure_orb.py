from __future__ import annotations

import asyncio
import hashlib
import json
import math
from datetime import date, datetime, timedelta, timezone

import pytest
from sqlalchemy import create_engine, event, inspect, text
from sqlalchemy.exc import DatabaseError
from sqlalchemy.orm import sessionmaker

import backend.database as database
from backend.app import app
from backend.market_structure import (
    MarketStructureJournal,
    run_market_structure_capture_loop,
)


def _payload(timestamp: datetime, price: float, **overrides):
    payload = {
        "symbol": "SPX",
        "provider": "databento",
        "validation_is_valid": True,
        "gamma_excluded_from_model": False,
        "subscription_epoch_id": "d" * 64,
        "subscription_generation": 7,
        "latest_ts_recv_utc": timestamp.isoformat(),
        "price": price,
        "gamma_pin": 6500.0,
        "max_pain": 6480.0,
        "zero_gamma": 6450.0,
        "pin_lead_ratio": 1.4,
        "pin_is_contested": False,
        "gross_gex": 100.0,
        "net_gex": 20.0,
        "primary_expiration": "2026-09-04",
        "same_day_profile_available": True,
        "universe_sha256": "a" * 64,
    }
    payload.update(overrides)
    return payload


def _reference_payload(
    payload,
    *,
    sample_timestamp=None,
    source_timestamp=None,
    captured_timestamp=None,
):
    sample = sample_timestamp or datetime.fromisoformat(
        str(payload["latest_ts_recv_utc"]).replace("Z", "+00:00")
    )
    source = source_timestamp or sample
    captured = captured_timestamp or sample
    primary_expiration = str(payload["primary_expiration"])
    primary_date = date.fromisoformat(primary_expiration)
    symbol = str(payload["symbol"])
    root = {"SPX": "SPXW", "NDX": "NDXP", "VIX": "VIXW"}.get(symbol, symbol)
    risk_free_rate = 0.0525
    years = 1.0 / 365.0
    discount = math.exp(-risk_free_rate * years)
    formula_pairs = []
    for offset in range(5):
        strike = round(float(payload["price"]) + offset - 2, 3)
        put_mid = 10.0
        call_mid = float(payload["price"]) - strike * discount + put_mid
        strike_code = f"{int(round(strike * 1000)):08d}"
        formula_pairs.append(
            {
                "pair_identity": [root, primary_expiration, strike, None, None],
                "strike": strike,
                "call_symbol": f"{root}  {primary_date:%y%m%d}C{strike_code}",
                "call_mid": call_mid,
                "call_mapping_version": "b" * 64,
                "put_symbol": f"{root}  {primary_date:%y%m%d}P{strike_code}",
                "put_mid": put_mid,
                "put_mapping_version": "b" * 64,
            }
        )
    pair_hash = hashlib.sha256(
        json.dumps(
            formula_pairs,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()
    source_ns = int(source.timestamp() * 1_000_000_000)
    source_age = (captured - source).total_seconds()
    return {
        "symbol": symbol,
        "provider": "databento",
        "sample_timestamp_utc": sample.isoformat(),
        "source_timestamp_utc": source.isoformat(),
        "captured_at_utc": captured.isoformat(),
        "subscription_epoch_id": payload["subscription_epoch_id"],
        "subscription_generation": payload["subscription_generation"],
        "active_generation": payload["subscription_generation"],
        "reference_price": payload["price"],
        "spot_source": "databento_opra_put_call_parity",
        "spot_formula_version": "put-call-parity-v2-discounted-strike",
        "risk_free_rate": risk_free_rate,
        "time_to_expiration_years": years,
        "primary_expiration": primary_expiration,
        "planned_primary_expiration": primary_expiration,
        "same_day_profile_available": primary_date == sample.astimezone().date()
        if payload.get("same_day_profile_available") is None
        else payload["same_day_profile_available"],
        "universe_sha256": payload["universe_sha256"],
        "universe_is_fallback": False,
        "universe_provenance": {"is_fallback": False},
        "paired_quote_count": 5,
        "minimum_paired_quote_count": 5,
        "contributing_pair_count": 5,
        "contributing_quote_count": 10,
        "earliest_ts_event_ns": source_ns - 1_000_000,
        "latest_ts_event_ns": source_ns - 1_000_000,
        "earliest_ts_recv_ns": source_ns,
        "latest_ts_recv_ns": source_ns,
        "observation_index_ns": source_ns + 1,
        "source_quote_age_seconds": source_age,
        "maximum_source_quote_age_seconds": source_age,
        "source_timestamp_span_seconds": 0.0,
        "quote_freshness_limit_seconds": 10.0,
        "pair_identity_sha256": pair_hash,
        "symbol_mapping_version": "c" * 64,
        "formula_inputs": {
            "formula_version": "put-call-parity-v2-discounted-strike",
            "risk_free_rate": risk_free_rate,
            "time_to_expiration_years": years,
            "pair_limit": 15,
            "pairs": formula_pairs,
        },
        "processing_clock_status": "synchronized",
        "timestamp_order_valid": True,
        "handoff_status": "active",
        "generation_state_unchanged": True,
        "universe_state_unchanged": True,
    }


def _memory_journal(*, now: datetime, cadence: float = 5.0):
    rows = []
    reference_rows = []

    def save(observation):
        existing = next(
            (
                row
                for row in rows
                if row["observation_id"] == observation["observation_id"]
            ),
            None,
        )
        if existing is not None:
            return existing
        rows.append(dict(observation))
        return observation

    def load(symbol, trading_date, *, as_of_utc=None):
        cutoff = as_of_utc or now
        return [
            dict(row)
            for row in rows
            if row["symbol"] == symbol
            and row["trading_date"] == trading_date
            and row["source_timestamp_utc"] <= cutoff
        ]

    def save_reference(sample):
        existing = next(
            (
                row
                for row in reference_rows
                if row["sample_id"] == sample["sample_id"]
            ),
            None,
        )
        if existing is not None:
            return existing if existing == sample else None
        reference_rows.append(dict(sample))
        return sample

    def load_reference(symbol, trading_date, *, as_of_utc=None):
        cutoff = as_of_utc or now
        return [
            dict(row)
            for row in reference_rows
            if row["symbol"] == symbol
            and row["trading_date"] == trading_date
            and row["sample_timestamp_utc"] <= cutoff
            and row["captured_at_utc"] <= cutoff
        ]

    return (
        MarketStructureJournal(
            saver=save,
            loader=load,
            reference_saver=save_reference,
            reference_loader=load_reference,
            now_utc=lambda: now,
            expected_cadence_seconds=cadence,
        ),
        rows,
    )


def _record_both(journal, payload, **reference_overrides):
    reference_result = journal.record_reference(
        _reference_payload(payload, **reference_overrides)
    )
    assert reference_result["recorded"] is True
    return journal.record(payload)


def test_record_rejects_fallback_invalid_and_off_hours_evidence():
    as_of = datetime(2026, 9, 4, 14, 0, tzinfo=timezone.utc)
    journal, rows = _memory_journal(now=as_of)

    assert journal.record(_payload(as_of, 6500, is_fallback=True))["reason"] == "FALLBACK_PROVENANCE"
    assert journal.record(
        _payload(as_of, 6500, universe_provenance={"is_fallback": True})
    )["reason"] == "FALLBACK_PROVENANCE"
    assert journal.record(
        _payload(as_of, 6500, oi_analytics_provenance={"is_fallback": True})
    )["reason"] == "FALLBACK_PROVENANCE"
    assert journal.record(_payload(as_of, 6500, validation_is_valid=False))["reason"] == "PAYLOAD_INVALID"
    missing_gamma_authority = _payload(as_of, 6500)
    missing_gamma_authority.pop("gamma_excluded_from_model")
    assert journal.record(missing_gamma_authority)["reason"] == (
        "GAMMA_MODEL_ELIGIBILITY_UNPROVEN"
    )
    assert journal.record(
        _payload(as_of, 6500, gamma_excluded_from_model=True)
    )["reason"] == "GAMMA_EXCLUDED_FROM_MODEL"
    assert journal.record(_payload(as_of, 6500, subscription_generation=0))["reason"] == "SUBSCRIPTION_GENERATION_INVALID"
    assert journal.record(_payload(as_of, 6500, universe_sha256=None))["reason"] == "UNIVERSE_PROVENANCE_INVALID"
    assert journal.record(_payload(as_of, 6500, provider="unknown"))["reason"] == "PROVIDER_NOT_DATABENTO"
    assert journal.record(_payload(datetime(2026, 9, 4, 12, 0, tzinfo=timezone.utc), 6500))["reason"] == "OUTSIDE_REGULAR_SESSION"
    assert rows == []


def test_reference_record_rejects_clock_fallback_pairs_generation_and_stale_source():
    sample_time = datetime(2026, 9, 4, 13, 30, 15, tzinfo=timezone.utc)
    journal, _rows = _memory_journal(now=sample_time)
    base = _reference_payload(_payload(sample_time, 6500))

    assert journal.record_reference(
        {**base, "processing_clock_status": "unsynchronized"}
    )["reason"] == "PROCESSING_CLOCK_NOT_SYNCHRONIZED"
    assert journal.record_reference(
        {**base, "universe_is_fallback": True}
    )["reason"] == "FALLBACK_PROVENANCE"
    assert journal.record_reference(
        {**base, "paired_quote_count": 4}
    )["reason"] == "COMPLETE_PAIR_MINIMUM_NOT_MET"
    assert journal.record_reference(
        {**base, "active_generation": 8}
    )["reason"] == "SUBSCRIPTION_GENERATION_INVALID"

    frozen_source = sample_time - timedelta(seconds=15)
    stale = _reference_payload(
        _payload(frozen_source, 6500),
        sample_timestamp=sample_time,
        source_timestamp=frozen_source,
    )
    assert journal.record_reference(stale)["reason"] == "SOURCE_TIMESTAMP_STALE"


def test_reference_record_binds_formula_pair_to_primary_series_expiry_and_strike():
    sample_time = datetime(2026, 9, 4, 13, 30, tzinfo=timezone.utc)
    journal, _rows = _memory_journal(now=sample_time)
    reference = _reference_payload(_payload(sample_time, 6500))
    formula_inputs = dict(reference["formula_inputs"])
    pairs = [dict(pair) for pair in formula_inputs["pairs"]]
    pairs[0]["pair_identity"] = list(pairs[0]["pair_identity"])
    pairs[0]["pair_identity"][1] = "2026-09-11"
    formula_inputs["pairs"] = pairs
    reference["formula_inputs"] = formula_inputs
    reference["pair_identity_sha256"] = hashlib.sha256(
        json.dumps(pairs, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()

    assert journal.record_reference(reference)["reason"] == "OPTION_SERIES_IDENTITY_MISSING"


def test_orb_forms_from_sampled_reference_prices_and_tracks_pin_drift():
    start = datetime(2026, 9, 4, 13, 30, tzinfo=timezone.utc)
    as_of = start + timedelta(minutes=15, seconds=30)
    journal, _rows = _memory_journal(now=as_of)

    _record_both(journal, _payload(start, 6500, gamma_pin=6500, max_pain=6480))
    _record_both(
        journal,
        _payload(start + timedelta(minutes=5), 6510, gamma_pin=6520, max_pain=6490),
    )
    _record_both(
        journal,
        _payload(start + timedelta(minutes=15), 6490, gamma_pin=6520, max_pain=6490),
    )

    state = journal.snapshot("SPX", as_of_utc=as_of)
    assert state["capture_status"] == "forming"
    assert state["opening_price"] == 6500
    assert state["orb_high"] == 6510
    assert state["orb_low"] == 6490
    assert state["breakout_direction"] == "forming"
    assert state["pin_behavior"]["gamma_pin_change_from_open"] == 20
    assert state["pin_behavior"]["gamma_pin_change_count"] == 1
    assert state["pin_behavior"]["max_pain_change_from_open"] == 10
    assert state["provenance"]["subscription_generations"] == [7]


def test_complete_orb_requires_boundary_and_gap_coverage_then_classifies_breakout():
    as_of = datetime(2026, 9, 4, 14, 31, tzinfo=timezone.utc)
    journal, _rows = _memory_journal(now=as_of)
    start = datetime(2026, 9, 4, 13, 30, tzinfo=timezone.utc)
    for index in range(720):
        price = 100.0 + (index % 20) / 10.0
        _record_both(journal, _payload(start + timedelta(seconds=index * 5), price))
    _record_both(journal, _payload(as_of, 102.5, gamma_pin=6530))

    state = journal.snapshot("SPX", as_of_utc=as_of)
    assert state["capture_status"] == "complete"
    assert state["orb_complete"] is True
    assert state["orb_high"] == pytest.approx(101.9)
    assert state["orb_low"] == 100.0
    assert state["breakout_direction"] == "bullish"
    assert state["capture_evidence"]["sample_count"] == 720
    assert state["capture_evidence"]["expected_sample_count"] == 720
    assert state["capture_evidence"]["max_gap_seconds"] == 5.0
    assert set(state["opening_ranges"]) == {"5m", "15m", "30m", "60m"}
    assert all(
        window["capture_status"] == "complete"
        for window in state["opening_ranges"].values()
    )
    assert state["opening_ranges"]["60m"]["orb_high"] == state["orb_high"]
    assert state["opening_ranges"]["60m"]["orb_low"] == state["orb_low"]
    assert state["opening_ranges"]["60m"]["breakout_direction"] == state[
        "breakout_direction"
    ]
    assert state["reference_semantics"]["kind"] == "same_day_index_option_parity"
    assert state["directional_evidence_eligible"] is True
    assert all(
        window["directional_evidence_eligible"] is True
        for window in state["opening_ranges"].values()
    )


def test_short_opening_windows_complete_before_the_hour_range():
    as_of = datetime(2026, 9, 4, 13, 40, tzinfo=timezone.utc)
    journal, _rows = _memory_journal(now=as_of)
    start = datetime(2026, 9, 4, 13, 30, tzinfo=timezone.utc)
    for index in range(120):
        price = 100.0 if index < 60 else 102.0
        _record_both(journal, _payload(start + timedelta(seconds=index * 5), price))

    state = journal.snapshot("SPX", as_of_utc=as_of)
    assert state["capture_status"] == "forming"
    assert state["opening_ranges"]["5m"]["capture_status"] == "complete"
    assert state["opening_ranges"]["5m"]["orb_low"] == 100.0
    assert state["opening_ranges"]["5m"]["orb_high"] == 100.0
    assert state["opening_ranges"]["5m"]["breakout_direction"] == "bullish"
    assert state["opening_ranges"]["15m"]["capture_status"] == "forming"
    assert state["opening_ranges"]["30m"]["capture_status"] == "forming"
    assert state["opening_ranges"]["60m"]["capture_status"] == "forming"


def test_sparse_thirty_second_samples_never_complete_a_five_second_orb():
    as_of = datetime(2026, 9, 4, 13, 35, tzinfo=timezone.utc)
    journal, _rows = _memory_journal(now=as_of)
    start = datetime(2026, 9, 4, 13, 30, tzinfo=timezone.utc)
    for index in range(10):
        _record_both(
            journal,
            _payload(start + timedelta(seconds=index * 30), 6500 + index),
        )

    five_minute = journal.snapshot("SPX", as_of_utc=as_of)["opening_ranges"]["5m"]
    assert five_minute["capture_status"] == "partial"
    assert five_minute["orb_complete"] is False
    assert five_minute["capture_evidence"]["capture_ratio"] < 0.2
    assert five_minute["directional_evidence_eligible"] is False


@pytest.mark.parametrize("first_sample_index", [1, 2, 3])
def test_missing_normalized_opening_bucket_never_completes_orb(first_sample_index):
    start = datetime(2026, 9, 4, 13, 30, tzinfo=timezone.utc)
    as_of = start + timedelta(minutes=5, seconds=1)
    journal, _rows = _memory_journal(now=as_of)

    for index in range(first_sample_index, 60):
        _record_both(
            journal,
            _payload(start + timedelta(seconds=index * 5), 6500 + index / 10),
        )
    _record_both(journal, _payload(start + timedelta(minutes=5), 6510))

    five_minute = journal.snapshot("SPX", as_of_utc=as_of)["opening_ranges"]["5m"]

    assert five_minute["capture_evidence"]["capture_ratio"] >= 0.95
    assert five_minute["capture_evidence"]["first_sample_lag_seconds"] == (
        first_sample_index * 5.0
    )
    assert five_minute["capture_evidence"]["opening_bucket_present"] is False
    assert five_minute["capture_status"] == "partial"
    assert five_minute["orb_complete"] is False
    assert five_minute["directional_evidence_eligible"] is False


def test_missing_normalized_opening_bucket_never_completes_top_level_hour_orb():
    start = datetime(2026, 9, 4, 13, 30, tzinfo=timezone.utc)
    as_of = start + timedelta(minutes=60, seconds=1)
    journal, _rows = _memory_journal(now=as_of)

    for index in range(1, 720):
        _record_both(
            journal,
            _payload(start + timedelta(seconds=index * 5), 6500 + index / 10),
        )
    _record_both(journal, _payload(start + timedelta(minutes=60), 6580))

    state = journal.snapshot("SPX", as_of_utc=as_of)
    hour = state["opening_ranges"]["60m"]

    assert state["capture_evidence"]["capture_ratio"] >= 0.95
    assert state["capture_evidence"]["opening_bucket_present"] is False
    assert state["capture_status"] == "partial"
    assert state["orb_complete"] is False
    assert hour["capture_evidence"]["opening_bucket_present"] is False
    assert hour["capture_status"] == "partial"
    assert hour["orb_complete"] is False


def test_reference_only_samples_form_orb_without_promoting_invalid_or_absent_gex():
    as_of = datetime(2026, 9, 4, 13, 35, tzinfo=timezone.utc)
    journal, structure_rows = _memory_journal(now=as_of)
    start = datetime(2026, 9, 4, 13, 30, tzinfo=timezone.utc)
    for index in range(60):
        result = journal.record_reference(
            _reference_payload(
                _payload(start + timedelta(seconds=index * 5), 6500 + index / 10)
            )
        )
        assert result["recorded"] is True

    state = journal.snapshot("SPX", as_of_utc=as_of)
    assert structure_rows == []
    assert state["opening_ranges"]["5m"]["capture_status"] == "complete"
    assert state["opening_ranges"]["5m"]["directional_evidence_eligible"] is True
    assert state["opening_ranges"]["5m"][
        "combined_structure_directional_evidence_eligible"
    ] is False
    assert state["opening_ranges"]["5m"]["provenance"][
        "structure_reference_status"
    ] == "unavailable"
    assert state["pin_behavior"]["gamma_pin"] is None
    assert state["pin_behavior"]["max_pain"] is None


def test_completed_orb_never_emits_direction_from_a_stale_current_reference():
    as_of = datetime(2026, 9, 4, 13, 36, tzinfo=timezone.utc)
    journal, _rows = _memory_journal(now=as_of)
    start = datetime(2026, 9, 4, 13, 30, tzinfo=timezone.utc)
    for index in range(60):
        _record_both(
            journal,
            _payload(start + timedelta(seconds=index * 5), 6500 + index / 10),
        )

    state = journal.snapshot("SPX", as_of_utc=as_of)
    five_minute = state["opening_ranges"]["5m"]
    assert five_minute["capture_status"] == "complete"
    assert five_minute["current_reference_fresh"] is False
    assert five_minute["breakout_direction"] == "unavailable"
    assert five_minute["directional_evidence_eligible"] is False
    assert five_minute[
        "combined_structure_directional_evidence_eligible"
    ] is False
    assert "CURRENT_REFERENCE_STALE" in five_minute["warnings"]


def test_structure_reference_generation_mismatch_blocks_combined_directional_use():
    as_of = datetime(2026, 9, 4, 13, 35, tzinfo=timezone.utc)
    journal, _rows = _memory_journal(now=as_of)
    start = datetime(2026, 9, 4, 13, 30, tzinfo=timezone.utc)
    for index in range(60):
        _record_both(
            journal,
            _payload(start + timedelta(seconds=index * 5), 6500 + index / 10),
        )
    journal.record(_payload(as_of, 6510, subscription_generation=8))

    five_minute = journal.snapshot("SPX", as_of_utc=as_of)["opening_ranges"]["5m"]
    assert five_minute["capture_status"] == "complete"
    assert five_minute["provenance"]["structure_vs_reference_aligned"] is False
    assert five_minute["directional_evidence_eligible"] is True
    assert five_minute[
        "combined_structure_directional_evidence_eligible"
    ] is False
    assert "STRUCTURE_REFERENCE_PROVENANCE_MISMATCH" in five_minute["warnings"]


def test_stale_same_provenance_structure_is_not_exposed_as_current_or_combined():
    as_of = datetime(2026, 9, 4, 13, 35, tzinfo=timezone.utc)
    journal, _rows = _memory_journal(now=as_of)
    start = datetime(2026, 9, 4, 13, 30, tzinfo=timezone.utc)
    _record_both(journal, _payload(start, 6500, gamma_pin=6510, max_pain=6490))
    for index in range(1, 60):
        payload = _payload(start + timedelta(seconds=index * 5), 6500 + index / 10)
        result = journal.record_reference(_reference_payload(payload))
        assert result["recorded"] is True

    state = journal.snapshot("SPX", as_of_utc=as_of)
    five_minute = state["opening_ranges"]["5m"]

    assert five_minute["capture_status"] == "complete"
    assert five_minute["directional_evidence_eligible"] is True
    assert five_minute[
        "combined_structure_directional_evidence_eligible"
    ] is False
    assert five_minute["provenance"]["structure_vs_reference_aligned"] is True
    assert five_minute["provenance"]["structure_reference_status"] == "stale"
    assert five_minute["provenance"]["structure_reference_fresh"] is False
    assert five_minute["provenance"]["structure_reference_age_seconds"] == 300.0
    assert "STRUCTURE_REFERENCE_STALE" in five_minute["warnings"]
    assert state["pin_behavior"]["gamma_pin"] is None
    assert state["pin_behavior"]["max_pain"] is None
    assert state["pin_behavior"]["gamma_pin_change_from_open"] is None
    assert state["last_known_structure"]["gamma_pin"] == 6510
    assert state["last_known_structure"]["max_pain"] == 6490
    assert state["last_known_structure"]["status"] == "stale"


def test_fresh_newest_null_structure_never_carries_prior_levels_forward():
    as_of = datetime(2026, 9, 4, 13, 35, tzinfo=timezone.utc)
    journal, _rows = _memory_journal(now=as_of)
    start = datetime(2026, 9, 4, 13, 30, tzinfo=timezone.utc)
    _record_both(
        journal,
        _payload(
            start,
            6500,
            gamma_pin=6510,
            max_pain=6490,
            zero_gamma=6480,
        ),
    )
    for index in range(1, 59):
        payload = _payload(start + timedelta(seconds=index * 5), 6500 + index / 10)
        result = journal.record_reference(_reference_payload(payload))
        assert result["recorded"] is True
    _record_both(
        journal,
        _payload(
            start + timedelta(seconds=295),
            6505.9,
            gamma_pin=None,
            max_pain=None,
            zero_gamma=None,
        ),
    )

    state = journal.snapshot("SPX", as_of_utc=as_of)
    five_minute = state["opening_ranges"]["5m"]

    assert five_minute["capture_status"] == "complete"
    assert five_minute["directional_evidence_eligible"] is True
    assert five_minute["provenance"]["structure_reference_status"] == "aligned"
    assert five_minute["provenance"]["structure_reference_fresh"] is True
    assert five_minute["provenance"]["current_pin_level_availability"] == (
        "unavailable"
    )
    assert five_minute[
        "combined_structure_directional_evidence_eligible"
    ] is False
    assert "CURRENT_PIN_LEVELS_UNAVAILABLE" in five_minute["warnings"]

    pin_behavior = state["pin_behavior"]
    assert pin_behavior["gamma_pin"] is None
    assert pin_behavior["max_pain"] is None
    assert pin_behavior["zero_gamma"] is None
    assert pin_behavior["previous_gamma_pin"] == 6510
    assert pin_behavior["previous_max_pain"] == 6490
    assert pin_behavior["gamma_pin_change_last"] is None
    assert pin_behavior["max_pain_change_last"] is None
    assert pin_behavior["gamma_pin_change_from_open"] is None
    assert pin_behavior["max_pain_change_from_open"] is None
    assert pin_behavior["level_availability_status"] == "unavailable"
    assert pin_behavior["current_level_policy"] == (
        "newest_structure_observation_only_no_carry_forward"
    )

    last_known = state["last_known_structure"]
    assert last_known["status"] == "aligned"
    assert last_known["level_availability_status"] == "unavailable"
    assert last_known["source_timestamp_utc"] == "2026-09-04T13:34:55Z"
    assert last_known["gamma_pin"] is None
    assert last_known["max_pain"] is None
    assert last_known["zero_gamma"] is None
    assert "CURRENT_PIN_LEVELS_UNAVAILABLE" in state["warnings"]


def test_calculation_bound_projection_keeps_newest_structure_policy():
    start = datetime(2026, 9, 4, 13, 30, tzinfo=timezone.utc)
    as_of = start + timedelta(seconds=20)
    journal, rows = _memory_journal(now=as_of)
    _record_both(
        journal,
        _payload(
            start + timedelta(seconds=10),
            6501,
            calculation_id="calculation-current",
            gamma_pin=6510,
            max_pain=6490,
        ),
    )
    rows[-1]["captured_at_utc"] = start + timedelta(seconds=11)
    _record_both(
        journal,
        _payload(
            start + timedelta(seconds=15),
            6502,
            calculation_id=None,
            gamma_pin=6520,
            max_pain=6495,
        ),
    )
    rows[-1]["captured_at_utc"] = start + timedelta(seconds=16)

    state = journal.snapshot(
        "SPX",
        as_of_utc=as_of,
        active_subscription_epoch_id="d" * 64,
        active_subscription_generation=7,
        active_handoff_status="active",
    )

    assert state["pin_behavior"]["gamma_pin"] == 6520
    assert state["pin_behavior"]["max_pain"] == 6495
    assert state["pin_behavior"]["current_level_policy"] == (
        "newest_structure_observation_only_no_carry_forward"
    )
    assert state["last_known_structure"]["source_timestamp_utc"] == (
        "2026-09-04T13:30:15Z"
    )
    assert state["last_known_structure"]["calculation_id"] is None

    bound = state["last_calculation_bound_structure"]
    assert bound == {
        "status": "aligned",
        "level_availability_status": "available",
        "source_timestamp_utc": "2026-09-04T13:30:10Z",
        "captured_at_utc": "2026-09-04T13:30:11Z",
        "freshness_timestamp_utc": "2026-09-04T13:30:10Z",
        "age_seconds": 10.0,
        "source_age_seconds": 10.0,
        "capture_age_seconds": 9.0,
        "maximum_current_age_seconds": 90.0,
        "reference_price": 6501.0,
        "gamma_pin": 6510.0,
        "max_pain": 6490.0,
        "zero_gamma": 6450.0,
        "calculation_id": "calculation-current",
        "provider": "databento",
        "subscription_epoch_id": "d" * 64,
        "subscription_generation": 7,
        "universe_sha256": "a" * 64,
        "primary_expiration": "2026-09-04",
        "same_day_profile_available": True,
        "current_provenance_aligned": True,
        "runtime_aligned": True,
        "evidence_eligible": True,
    }


def test_calculation_bound_projection_fails_closed_when_stale():
    start = datetime(2026, 9, 4, 13, 30, tzinfo=timezone.utc)
    as_of = start + timedelta(seconds=100)
    journal, rows = _memory_journal(now=as_of)
    _record_both(
        journal,
        _payload(
            start,
            6500,
            calculation_id="calculation-stale",
            gamma_pin=6510,
            max_pain=6490,
        ),
    )
    rows[-1]["captured_at_utc"] = start + timedelta(seconds=1)
    _record_both(
        journal,
        _payload(
            as_of,
            6502,
            calculation_id=None,
            gamma_pin=6520,
            max_pain=6495,
        ),
    )
    rows[-1]["captured_at_utc"] = as_of

    state = journal.snapshot(
        "SPX",
        as_of_utc=as_of,
        active_subscription_epoch_id="d" * 64,
        active_subscription_generation=7,
        active_handoff_status="active",
    )

    bound = state["last_calculation_bound_structure"]
    assert bound["status"] == "stale"
    assert bound["age_seconds"] == 100.0
    assert bound["calculation_id"] == "calculation-stale"
    assert bound["gamma_pin"] == 6510
    assert bound["max_pain"] == 6490
    assert bound["current_provenance_aligned"] is True
    assert bound["runtime_aligned"] is True
    assert bound["evidence_eligible"] is False
    assert state["last_known_structure"]["status"] == "aligned"
    assert state["last_known_structure"]["calculation_id"] is None


@pytest.mark.parametrize(
    "identity_override",
    [
        {"subscription_epoch_id": "e" * 64},
        {"subscription_generation": 8},
        {"universe_sha256": "b" * 64},
        {
            "primary_expiration": "2026-09-05",
            "same_day_profile_available": False,
        },
    ],
)
def test_calculation_bound_projection_rejects_other_provenance_identity(
    identity_override,
):
    start = datetime(2026, 9, 4, 13, 30, tzinfo=timezone.utc)
    as_of = start + timedelta(seconds=20)
    journal, rows = _memory_journal(now=as_of)
    _record_both(
        journal,
        _payload(
            start + timedelta(seconds=10),
            6501,
            calculation_id="calculation-other-identity",
            **identity_override,
        ),
    )
    rows[-1]["captured_at_utc"] = start + timedelta(seconds=11)
    _record_both(
        journal,
        _payload(
            start + timedelta(seconds=15),
            6502,
            calculation_id=None,
        ),
    )
    rows[-1]["captured_at_utc"] = start + timedelta(seconds=16)

    state = journal.snapshot(
        "SPX",
        as_of_utc=as_of,
        active_subscription_epoch_id="d" * 64,
        active_subscription_generation=7,
        active_handoff_status="active",
    )

    bound = state["last_calculation_bound_structure"]
    assert bound["status"] == "mismatch"
    assert bound["calculation_id"] is None
    assert bound["gamma_pin"] is None
    assert bound["max_pain"] is None
    assert bound["current_provenance_aligned"] is False
    assert bound["runtime_aligned"] is False
    assert bound["evidence_eligible"] is False
    assert state["last_known_structure"]["calculation_id"] is None


def test_calculation_bound_projection_fails_closed_on_runtime_mismatch():
    start = datetime(2026, 9, 4, 13, 30, tzinfo=timezone.utc)
    as_of = start + timedelta(seconds=20)
    journal, rows = _memory_journal(now=as_of)
    _record_both(
        journal,
        _payload(
            start + timedelta(seconds=10),
            6501,
            calculation_id="calculation-current",
        ),
    )
    rows[-1]["captured_at_utc"] = start + timedelta(seconds=11)
    _record_both(
        journal,
        _payload(start + timedelta(seconds=15), 6502, calculation_id=None),
    )
    rows[-1]["captured_at_utc"] = start + timedelta(seconds=16)

    state = journal.snapshot(
        "SPX",
        as_of_utc=as_of,
        active_subscription_epoch_id="e" * 64,
        active_subscription_generation=7,
        active_handoff_status="active",
    )

    bound = state["last_calculation_bound_structure"]
    assert bound["status"] == "mismatch"
    assert bound["calculation_id"] == "calculation-current"
    assert bound["current_provenance_aligned"] is True
    assert bound["runtime_aligned"] is False
    assert bound["evidence_eligible"] is False
    assert state["last_known_structure"]["calculation_id"] is None


def test_vix_orb_is_explicit_forward_context_and_never_directional_spot_evidence():
    as_of = datetime(2026, 9, 4, 13, 36, tzinfo=timezone.utc)
    journal, _rows = _memory_journal(now=as_of)
    start = datetime(2026, 9, 4, 13, 30, tzinfo=timezone.utc)
    for index in range(60):
        _record_both(
            journal,
            _payload(
                start + timedelta(seconds=index * 5),
                20.0 + (index % 5) / 10.0,
                symbol="VIX",
                primary_expiration="2026-09-09",
                same_day_profile_available=False,
            )
        )
    _record_both(
        journal,
        _payload(
            as_of - timedelta(seconds=5),
            21.0,
            symbol="VIX",
            primary_expiration="2026-09-09",
            same_day_profile_available=False,
        )
    )

    state = journal.snapshot("VIX", as_of_utc=as_of)
    five_minute = state["opening_ranges"]["5m"]
    assert five_minute["capture_status"] == "complete"
    assert five_minute["breakout_direction"] == "bullish"
    assert five_minute["directional_evidence_eligible"] is False
    assert state["reference_semantics"]["kind"] == "vix_option_forward_context"
    assert state["directional_evidence_eligible"] is False
    assert "VIX_OPTION_PARITY_FORWARD_CONTEXT_NOT_SPOT" in state["warnings"]


def test_missing_opening_evidence_fails_closed_instead_of_backfilling():
    as_of = datetime(2026, 9, 4, 15, 0, tzinfo=timezone.utc)
    journal, _rows = _memory_journal(now=as_of)
    _record_both(journal, _payload(as_of, 6550))

    state = journal.snapshot("SPX", as_of_utc=as_of)
    assert state["capture_status"] == "unavailable"
    assert state["orb_high"] is None
    assert state["breakout_direction"] == "unavailable"
    assert "OPENING_RANGE_NOT_CAPTURED" in state["warnings"]


def test_structure_recalculation_does_not_inflate_reference_bucket_coverage():
    as_of = datetime(2026, 9, 4, 13, 31, tzinfo=timezone.utc)
    journal, _rows = _memory_journal(now=as_of)
    source = datetime(2026, 9, 4, 13, 30, tzinfo=timezone.utc)
    _record_both(journal, _payload(source, 6500, gamma_pin=6500))
    journal.record(_payload(source, 6500, gamma_pin=6520))

    state = journal.snapshot("SPX", as_of_utc=as_of)
    assert state["capture_evidence"]["sample_count"] == 1
    assert state["capture_evidence"]["raw_observation_count"] == 1
    assert state["capture_evidence"]["duplicate_source_timestamp_count"] == 0


def test_still_fresh_provider_quote_can_fill_two_distinct_five_second_buckets():
    as_of = datetime(2026, 9, 4, 13, 30, 6, tzinfo=timezone.utc)
    journal, _rows = _memory_journal(now=as_of)
    source = datetime(2026, 9, 4, 13, 30, tzinfo=timezone.utc)
    first = _payload(source, 6500)
    second = _payload(source, 6500)

    _record_both(journal, first)
    reference_result = journal.record_reference(
        _reference_payload(
            second,
            sample_timestamp=source + timedelta(seconds=5),
            source_timestamp=source,
        )
    )

    assert reference_result["recorded"] is True
    state = journal.snapshot("SPX", as_of_utc=as_of)
    assert state["capture_evidence"]["sample_count"] == 2
    assert state["capture_evidence"]["raw_observation_count"] == 2
    assert state["capture_evidence"]["max_gap_seconds"] == 5.0


def test_reconnect_inside_opening_range_marks_range_partial():
    as_of = datetime(2026, 9, 4, 14, 0, tzinfo=timezone.utc)
    journal, _rows = _memory_journal(now=as_of)
    start = datetime(2026, 9, 4, 13, 30, tzinfo=timezone.utc)
    _record_both(journal, _payload(start, 6500, subscription_generation=7))
    _record_both(
        journal,
        _payload(start + timedelta(minutes=1), 6510, subscription_generation=8)
    )

    state = journal.snapshot("SPX", as_of_utc=as_of)
    assert state["capture_status"] == "partial"
    assert state["orb_complete"] is False
    assert state["provenance"]["range_provenance_aligned"] is False
    assert "OPENING_RANGE_PROVENANCE_MIXED" in state["warnings"]


def test_post_range_reconnect_preserves_orb_but_disables_cross_generation_breakout():
    as_of = datetime(2026, 9, 4, 14, 31, tzinfo=timezone.utc)
    journal, _rows = _memory_journal(now=as_of)
    start = datetime(2026, 9, 4, 13, 30, tzinfo=timezone.utc)
    for index in range(720):
        _record_both(
            journal,
            _payload(
                start + timedelta(seconds=index * 5),
                100.0 + (index % 20) / 10.0,
                subscription_generation=7,
            )
        )
    _record_both(
        journal,
        _payload(as_of, 102.5, subscription_generation=8, gamma_pin=6600)
    )

    state = journal.snapshot("SPX", as_of_utc=as_of)
    assert state["capture_status"] == "complete"
    assert state["orb_complete"] is True
    assert state["breakout_direction"] == "unavailable"
    assert state["provenance"]["range_provenance_aligned"] is True
    assert state["provenance"]["current_vs_range_aligned"] is False
    assert state["pin_behavior"]["gamma_pin_change_from_open"] is None
    assert state["pin_behavior"]["comparison_reset"] is True
    assert "CURRENT_PROVENANCE_DIFFERS_FROM_ORB" in state["warnings"]


def test_post_range_process_restart_never_borrows_same_generation_opening_pins():
    as_of = datetime(2026, 9, 4, 14, 31, tzinfo=timezone.utc)
    journal, _rows = _memory_journal(now=as_of)
    start = datetime(2026, 9, 4, 13, 30, tzinfo=timezone.utc)
    old_epoch = "d" * 64
    new_epoch = "e" * 64
    for index in range(720):
        _record_both(
            journal,
            _payload(
                start + timedelta(seconds=index * 5),
                100.0 + (index % 20) / 10.0,
                subscription_epoch_id=old_epoch,
                subscription_generation=7,
            ),
        )
    _record_both(
        journal,
        _payload(
            as_of,
            102.5,
            subscription_epoch_id=new_epoch,
            subscription_generation=7,
            gamma_pin=6600,
            max_pain=6575,
        ),
    )

    state = journal.snapshot(
        "SPX",
        as_of_utc=as_of,
        active_subscription_epoch_id=new_epoch,
        active_subscription_generation=7,
        active_handoff_status="active",
    )
    assert state["capture_status"] == "complete"
    assert state["current_price"] == 102.5
    assert state["provenance"]["active_runtime_epoch_aligned"] is True
    assert state["provenance"]["current_vs_range_aligned"] is False
    assert state["directional_evidence_eligible"] is False
    assert state["pin_behavior"]["gamma_pin"] == 6600
    assert state["pin_behavior"]["opening_gamma_pin"] is None
    assert state["pin_behavior"]["gamma_pin_change_from_open"] is None
    assert state["pin_behavior"]["previous_gamma_pin"] is None
    assert state["pin_behavior"]["comparison_reset"] is True


def test_market_structure_persistence_is_idempotent_and_immutable(monkeypatch):
    temp_engine = create_engine("sqlite:///:memory:")
    database.Base.metadata.create_all(bind=temp_engine)
    monkeypatch.setattr(database, "engine", temp_engine)
    monkeypatch.setattr(database, "SessionLocal", sessionmaker(bind=temp_engine))
    monkeypatch.setattr(database, "DATABASE_URL", "sqlite:///:memory:")
    database._ensure_market_structure_immutability()

    observed = datetime(2026, 9, 4, 13, 30, tzinfo=timezone.utc)
    payload = {
        "observation_id": "b" * 64,
        "symbol": "SPX",
        "trading_date": date(2026, 9, 4),
        "source_timestamp_utc": observed,
        "provider": "databento",
        "subscription_epoch_id": "d" * 64,
        "subscription_generation": 7,
        "reference_price": 6500.0,
        "gamma_pin": 6510.0,
        "universe_sha256": "a" * 64,
        "validation_status": "valid",
    }
    first = database.save_market_structure_observation(payload)
    second = database.save_market_structure_observation(payload)
    assert database.save_market_structure_observation(
        {**payload, "observation_id": "c" * 64, "provider": "unknown"}
    ) is None
    assert first is not None
    assert second is not None
    rows = database.load_market_structure_observations("SPX", date(2026, 9, 4))
    assert len(rows) == 1

    with temp_engine.begin() as connection:
        with pytest.raises(DatabaseError):
            connection.execute(
                text(
                    "UPDATE market_structure_observations "
                    "SET reference_price = 1 WHERE observation_id = :observation_id"
                ),
                {"observation_id": "b" * 64},
            )


def test_orb_reference_persistence_is_idempotent_conflict_safe_and_append_only(
    monkeypatch,
):
    temp_engine = create_engine("sqlite:///:memory:")
    database.Base.metadata.create_all(bind=temp_engine)
    monkeypatch.setattr(database, "engine", temp_engine)
    monkeypatch.setattr(database, "SessionLocal", sessionmaker(bind=temp_engine))
    monkeypatch.setattr(database, "DATABASE_URL", "sqlite:///:memory:")
    database._ensure_orb_reference_schema()
    database._ensure_orb_reference_immutability()
    database._ensure_orb_reference_decision_schema()
    database._ensure_orb_reference_decision_immutability()

    observed = datetime(2026, 9, 4, 13, 30, tzinfo=timezone.utc)
    journal = MarketStructureJournal(
        saver=lambda _row: None,
        loader=lambda *_args, **_kwargs: [],
        reference_saver=database.save_orb_reference_sample,
        reference_decision_saver=database.save_orb_reference_sample_decision,
        reference_loader=database.load_orb_reference_samples,
        now_utc=lambda: observed,
    )
    reference = _reference_payload(_payload(observed, 6500.0))
    first = journal.record_reference(reference)
    second = journal.record_reference(reference)
    assert first["recorded"] is True
    assert second["recorded"] is True
    assert first["subscription_generation"] == 7
    assert second["subscription_generation"] == 7

    rows = database.load_orb_reference_samples("SPX", date(2026, 9, 4))
    assert len(rows) == 1
    persisted = rows[0]
    assert database.save_orb_reference_sample(persisted) is not None
    assert database.save_orb_reference_sample(
        {**persisted, "reference_price": 6501.0}
    ) is None
    assert database.save_orb_reference_sample(
        {**persisted, "sample_id": "f" * 64}
    ) is None
    assert database.save_orb_reference_sample(
        {**persisted, "formula_inputs_json": "[]"}
    ) is None
    assert database.save_orb_reference_sample(
        {
            **persisted,
            "formula_inputs_json": {
                "formula_version": persisted["spot_formula_version"],
                "invalid": float("nan"),
            },
        }
    ) is None

    columns = [column.name for column in database.OrbReferenceSample.__table__.columns]
    select_columns = [
        "reference_price + 1" if column == "reference_price" else column
        for column in columns
    ]
    replace_sql = (
        f"INSERT OR REPLACE INTO orb_reference_samples ({','.join(columns)}) "
        f"SELECT {','.join(select_columns)} FROM orb_reference_samples "
        "WHERE sample_id = :sample_id"
    )
    alternate_id_select_columns = [
        f"'{('e' * 64)}'" if column == "sample_id" else column
        for column in columns
    ]
    alternate_id_replace_sql = (
        f"INSERT OR REPLACE INTO orb_reference_samples ({','.join(columns)}) "
        f"SELECT {','.join(alternate_id_select_columns)} FROM orb_reference_samples "
        "WHERE sample_id = :sample_id"
    )
    with temp_engine.begin() as connection:
        for statement in (
            "UPDATE orb_reference_samples SET reference_price = 1 WHERE sample_id = :sample_id",
            "DELETE FROM orb_reference_samples WHERE sample_id = :sample_id",
            replace_sql,
            alternate_id_replace_sql,
        ):
            with pytest.raises(DatabaseError):
                connection.execute(text(statement), {"sample_id": first["sample_id"]})

    rows_after = database.load_orb_reference_samples("SPX", date(2026, 9, 4))
    assert len(rows_after) == 1
    assert rows_after[0]["reference_price"] == 6500.0


def test_orb_reference_pending_and_rejected_decisions_fail_closed_and_are_immutable(
    monkeypatch,
):
    temp_engine = create_engine("sqlite:///:memory:")
    database.Base.metadata.create_all(bind=temp_engine)
    monkeypatch.setattr(database, "engine", temp_engine)
    monkeypatch.setattr(database, "SessionLocal", sessionmaker(bind=temp_engine))
    monkeypatch.setattr(database, "DATABASE_URL", "sqlite:///:memory:")
    database._ensure_orb_reference_schema()
    database._ensure_orb_reference_immutability()
    database._ensure_orb_reference_decision_schema()
    database._ensure_orb_reference_decision_immutability()

    observed = datetime(2026, 9, 4, 13, 30, tzinfo=timezone.utc)
    retained: dict[str, object] = {}

    def capture_reference(sample):
        retained.update(sample)
        return sample

    capture_journal = MarketStructureJournal(
        saver=lambda _row: None,
        loader=lambda *_args, **_kwargs: [],
        reference_saver=capture_reference,
        reference_loader=lambda *_args, **_kwargs: [],
        now_utc=lambda: observed,
    )
    assert capture_journal.record_reference(
        _reference_payload(_payload(observed, 6500.0))
    )["recorded"] is True
    sample_id = str(retained["sample_id"])
    assert database.save_orb_reference_sample(retained) is not None

    # Raw evidence alone is pending and cannot be projected by either loader.
    assert database.load_orb_reference_samples("SPX", observed.date()) == []
    assert database.load_orb_reference_snapshot_samples("SPX", observed.date()) == []

    # Direct SQL cannot manufacture an eligible decision for a different
    # intended bucket or a completion outside the five-second bucket.
    with temp_engine.begin() as connection:
        for intended, completed in (
            (observed + timedelta(seconds=5), observed + timedelta(seconds=5, milliseconds=100)),
            (observed, observed + timedelta(seconds=5, milliseconds=100)),
        ):
            with pytest.raises(DatabaseError):
                connection.execute(
                    text(
                        "INSERT INTO orb_reference_sample_decisions "
                        "(sample_id, sample_timestamp_utc, intended_bucket_utc, "
                        "attempt_completed_at_utc, progress_eligible, reason, "
                        "decision_status) VALUES (:sample_id, :sample_time, "
                        ":intended, :completed, 1, NULL, 'final')"
                    ),
                    {
                        "sample_id": sample_id,
                        "sample_time": observed.replace(tzinfo=None),
                        "intended": intended.replace(tzinfo=None),
                        "completed": completed.replace(tzinfo=None),
                    },
                )

    rejected = {
        "sample_id": sample_id,
        "sample_timestamp_utc": observed,
        "intended_bucket_utc": observed,
        "attempt_completed_at_utc": observed + timedelta(seconds=5, milliseconds=100),
        "progress_eligible": False,
        "reason": "REFERENCE_CAPTURE_COMPLETED_OUTSIDE_INTENDED_BUCKET",
        "decision_status": "final",
    }
    assert database.save_orb_reference_sample_decision(rejected) is not None
    assert database.save_orb_reference_sample_decision(rejected) is not None
    assert database.save_orb_reference_sample_decision(
        {**rejected, "reason": "REFERENCE_CONTEXT_CHANGED_AFTER_PERSIST"}
    ) is None
    assert database.load_orb_reference_samples("SPX", observed.date()) == []

    with temp_engine.begin() as connection:
        for statement in (
            "UPDATE orb_reference_sample_decisions SET progress_eligible=1 "
            "WHERE sample_id=:sample_id",
            "DELETE FROM orb_reference_sample_decisions WHERE sample_id=:sample_id",
            "INSERT OR REPLACE INTO orb_reference_sample_decisions "
            "SELECT * FROM orb_reference_sample_decisions WHERE sample_id=:sample_id",
        ):
            with pytest.raises(DatabaseError):
                connection.execute(text(statement), {"sample_id": sample_id})


def test_orb_reference_reconciliation_is_append_only_fail_closed_and_idempotent(
    monkeypatch,
):
    temp_engine = create_engine("sqlite:///:memory:")
    database.Base.metadata.create_all(bind=temp_engine)
    monkeypatch.setattr(database, "engine", temp_engine)
    monkeypatch.setattr(database, "SessionLocal", sessionmaker(bind=temp_engine))
    monkeypatch.setattr(database, "DATABASE_URL", "sqlite:///:memory:")
    database._ensure_orb_reference_schema()
    database._ensure_orb_reference_immutability()
    database._ensure_orb_reference_decision_schema()
    database._ensure_orb_reference_decision_immutability()

    opening = datetime(2026, 9, 4, 13, 30, tzinfo=timezone.utc)
    retained_samples = []

    def retain_without_decision(sample):
        retained_samples.append(dict(sample))
        return sample

    capture_journal = MarketStructureJournal(
        saver=lambda _row: None,
        loader=lambda *_args, **_kwargs: [],
        reference_saver=retain_without_decision,
        reference_loader=lambda *_args, **_kwargs: [],
        now_utc=lambda: opening,
    )
    for index in range(2):
        sample_time = opening + timedelta(seconds=index * 5)
        result = capture_journal.record_reference(
            _reference_payload(
                _payload(sample_time, 6500.0 + index),
                sample_timestamp=sample_time,
                source_timestamp=sample_time,
                captured_timestamp=sample_time + timedelta(seconds=1),
            )
        )
        assert result["recorded"] is True
        assert database.save_orb_reference_sample(retained_samples[-1]) is not None

    # A raw row can still receive its live decision until its bucket closes;
    # startup recovery must not preempt that decision.
    assert database.reconcile_orb_reference_sample_decisions(
        as_of_utc=opening + timedelta(seconds=4)
    ) == {
        "reconciled_count": 0,
        "remaining_expired_pending_count": 0,
        "failed_count": 0,
    }
    with temp_engine.connect() as connection:
        assert connection.execute(
            text("SELECT COUNT(*) FROM orb_reference_sample_decisions")
        ).scalar_one() == 0

    # Only the first bucket has expired at this point. Recovery appends a final
    # rejection; it cannot promote the raw point or rewrite its evidence.
    first = database.reconcile_orb_reference_sample_decisions(
        as_of_utc=opening + timedelta(seconds=7)
    )
    assert first == {
        "reconciled_count": 1,
        "remaining_expired_pending_count": 0,
        "failed_count": 0,
    }
    assert database.load_orb_reference_samples("SPX", opening.date()) == []
    with temp_engine.connect() as connection:
        samples = connection.execute(
            text(
                "SELECT sample_id, reference_price FROM orb_reference_samples "
                "ORDER BY sample_timestamp_utc"
            )
        ).all()
        decisions = connection.execute(
            text(
                "SELECT sample_id, intended_bucket_utc, "
                "attempt_completed_at_utc, progress_eligible, reason "
                "FROM orb_reference_sample_decisions"
            )
        ).all()
    assert [(row.sample_id, row.reference_price) for row in samples] == [
        (str(retained_samples[0]["sample_id"]), 6500.0),
        (str(retained_samples[1]["sample_id"]), 6501.0),
    ]
    assert len(decisions) == 1
    assert decisions[0].sample_id == retained_samples[0]["sample_id"]
    assert decisions[0].progress_eligible == 0
    assert decisions[0].reason == (
        "REFERENCE_PROGRESS_DECISION_MISSING_AT_BUCKET_CLOSE"
    )
    assert datetime.fromisoformat(str(decisions[0].intended_bucket_utc)) == (
        opening.replace(tzinfo=None)
    )
    assert datetime.fromisoformat(str(decisions[0].attempt_completed_at_utc)) == (
        opening + timedelta(seconds=5)
    ).replace(tzinfo=None)

    # Replaying the same recovery is a no-op. Once the second bucket expires it
    # receives its own immutable rejection without touching the first one.
    assert database.reconcile_orb_reference_sample_decisions(
        as_of_utc=opening + timedelta(seconds=7)
    ) == {
        "reconciled_count": 0,
        "remaining_expired_pending_count": 0,
        "failed_count": 0,
    }
    assert database.reconcile_orb_reference_sample_decisions(
        as_of_utc=opening + timedelta(seconds=12)
    ) == {
        "reconciled_count": 1,
        "remaining_expired_pending_count": 0,
        "failed_count": 0,
    }
    with temp_engine.connect() as connection:
        assert connection.execute(
            text("SELECT COUNT(*) FROM orb_reference_samples")
        ).scalar_one() == 2
        assert connection.execute(
            text("SELECT COUNT(*) FROM orb_reference_sample_decisions")
        ).scalar_one() == 2


def test_orb_reference_reconciliation_limit_leaves_expired_remainder_fail_closed(
    monkeypatch,
):
    temp_engine = create_engine("sqlite:///:memory:")
    database.Base.metadata.create_all(bind=temp_engine)
    monkeypatch.setattr(database, "engine", temp_engine)
    monkeypatch.setattr(database, "SessionLocal", sessionmaker(bind=temp_engine))
    monkeypatch.setattr(database, "DATABASE_URL", "sqlite:///:memory:")
    database._ensure_orb_reference_schema()
    database._ensure_orb_reference_immutability()
    database._ensure_orb_reference_decision_schema()
    database._ensure_orb_reference_decision_immutability()

    opening = datetime(2026, 9, 4, 13, 30, tzinfo=timezone.utc)
    retained_samples = []

    def retain_without_decision(sample):
        retained_samples.append(dict(sample))
        return sample

    capture_journal = MarketStructureJournal(
        saver=lambda _row: None,
        loader=lambda *_args, **_kwargs: [],
        reference_saver=retain_without_decision,
        reference_loader=lambda *_args, **_kwargs: [],
        now_utc=lambda: opening,
    )
    for index in range(3):
        sample_time = opening + timedelta(seconds=index * 5)
        assert capture_journal.record_reference(
            _reference_payload(
                _payload(sample_time, 6500.0 + index),
                sample_timestamp=sample_time,
                source_timestamp=sample_time,
                captured_timestamp=sample_time + timedelta(seconds=1),
            )
        )["recorded"] is True
        assert database.save_orb_reference_sample(retained_samples[-1]) is not None

    result = database.reconcile_orb_reference_sample_decisions(
        as_of_utc=opening + timedelta(seconds=20),
        maximum_rows=1,
    )
    assert result == {
        "reconciled_count": 1,
        "remaining_expired_pending_count": 2,
        "failed_count": 0,
    }
    assert database.load_orb_reference_samples("SPX", opening.date()) == []
    with temp_engine.connect() as connection:
        assert connection.execute(
            text("SELECT COUNT(*) FROM orb_reference_samples")
        ).scalar_one() == 3
        assert connection.execute(
            text("SELECT COUNT(*) FROM orb_reference_sample_decisions")
        ).scalar_one() == 1


def test_orb_reference_loaders_reject_off_grid_eligible_decision_when_named_guard_is_weakened(
    monkeypatch,
):
    temp_engine = create_engine("sqlite:///:memory:")
    database.Base.metadata.create_all(bind=temp_engine)
    monkeypatch.setattr(database, "engine", temp_engine)
    monkeypatch.setattr(database, "SessionLocal", sessionmaker(bind=temp_engine))
    monkeypatch.setattr(database, "DATABASE_URL", "sqlite:///:memory:")
    database._ensure_orb_reference_schema()
    database._ensure_orb_reference_immutability()
    database._ensure_orb_reference_decision_schema()
    database._ensure_orb_reference_decision_immutability()

    observed = datetime(2026, 9, 4, 13, 30, tzinfo=timezone.utc)
    retained: dict[str, object] = {}

    def capture_reference(sample):
        retained.update(sample)
        return sample

    capture_journal = MarketStructureJournal(
        saver=lambda _row: None,
        loader=lambda *_args, **_kwargs: [],
        reference_saver=capture_reference,
        reference_loader=lambda *_args, **_kwargs: [],
        now_utc=lambda: observed,
    )
    assert capture_journal.record_reference(
        _reference_payload(_payload(observed, 6500.0))
    )["recorded"] is True
    assert database.save_orb_reference_sample(retained) is not None

    off_grid = (observed + timedelta(seconds=1)).replace(tzinfo=None)
    sample_id = str(retained["sample_id"])
    with temp_engine.begin() as connection:
        connection.execute(
            text("DROP TRIGGER IF EXISTS orb_reference_samples_no_update")
        )
        connection.execute(
            text(
                "CREATE TRIGGER orb_reference_samples_no_update "
                "BEFORE UPDATE ON orb_reference_samples WHEN 0 "
                "BEGIN SELECT RAISE(ABORT, 'weakened guard'); END"
            )
        )
        connection.execute(
            text(
                "UPDATE orb_reference_samples SET sample_timestamp_utc=:off_grid, "
                "source_timestamp_utc=:off_grid, captured_at_utc=:off_grid "
                "WHERE sample_id=:sample_id"
            ),
            {"off_grid": off_grid, "sample_id": sample_id},
        )
        connection.execute(
            text(
                "DROP TRIGGER IF EXISTS "
                "orb_reference_sample_decisions_insert_guard"
            )
        )
        connection.execute(
            text(
                "CREATE TRIGGER orb_reference_sample_decisions_insert_guard "
                "BEFORE INSERT ON orb_reference_sample_decisions WHEN 0 "
                "BEGIN SELECT RAISE(ABORT, 'weakened guard'); END"
            )
        )
        connection.execute(
            text(
                "INSERT INTO orb_reference_sample_decisions "
                "(sample_id, sample_timestamp_utc, intended_bucket_utc, "
                "attempt_completed_at_utc, progress_eligible, reason, "
                "decision_status) VALUES (:sample_id, :sample_time, "
                ":sample_time, :completed, 1, NULL, 'final')"
            ),
            {
                "sample_id": sample_id,
                "sample_time": off_grid,
                "completed": off_grid + timedelta(seconds=1),
            },
        )

    assert database.load_orb_reference_samples("SPX", observed.date()) == []
    assert database.load_orb_reference_snapshot_samples("SPX", observed.date()) == []


def test_orb_reference_same_bucket_and_generation_is_distinct_by_process_epoch(
    monkeypatch,
):
    temp_engine = create_engine("sqlite:///:memory:")
    database.Base.metadata.create_all(bind=temp_engine)
    monkeypatch.setattr(database, "engine", temp_engine)
    monkeypatch.setattr(database, "SessionLocal", sessionmaker(bind=temp_engine))
    monkeypatch.setattr(database, "DATABASE_URL", "sqlite:///:memory:")
    database._ensure_orb_reference_schema()
    database._ensure_orb_reference_immutability()
    database._ensure_orb_reference_decision_schema()
    database._ensure_orb_reference_decision_immutability()

    observed = datetime(2026, 9, 4, 13, 30, tzinfo=timezone.utc)
    journal = MarketStructureJournal(
        saver=lambda _row: None,
        loader=lambda *_args, **_kwargs: [],
        reference_saver=database.save_orb_reference_sample,
        reference_decision_saver=database.save_orb_reference_sample_decision,
        reference_loader=database.load_orb_reference_samples,
        now_utc=lambda: observed,
    )
    first = journal.record_reference(
        _reference_payload(
            _payload(observed, 6500.0, subscription_epoch_id="d" * 64)
        )
    )
    restarted = journal.record_reference(
        _reference_payload(
            _payload(observed, 6500.0, subscription_epoch_id="e" * 64)
        )
    )

    assert first["recorded"] is True
    assert restarted["recorded"] is True
    assert restarted["sample_id"] != first["sample_id"]
    rows = database.load_orb_reference_samples("SPX", date(2026, 9, 4))
    assert {row["subscription_epoch_id"] for row in rows} == {"d" * 64, "e" * 64}
    index = next(
        item
        for item in inspect(temp_engine).get_indexes("orb_reference_samples")
        if item["name"] == "uix_orb_reference_logical_sample"
    )
    assert index["column_names"][:4] == [
        "symbol",
        "sample_timestamp_utc",
        "subscription_epoch_id",
        "subscription_generation",
    ]


def test_orb_reference_schema_verifier_fails_fast_without_touching_legacy_rows(
    monkeypatch,
):
    temp_engine = create_engine("sqlite:///:memory:")
    with temp_engine.begin() as connection:
        connection.execute(text("CREATE TABLE orb_reference_samples (sample_id TEXT PRIMARY KEY)"))
        connection.execute(text("CREATE TABLE legacy_keep (value TEXT NOT NULL)"))
        connection.execute(text("INSERT INTO legacy_keep(value) VALUES ('preserve-me')"))
    monkeypatch.setattr(database, "engine", temp_engine)

    with pytest.raises(RuntimeError, match="schema is incompatible"):
        database._ensure_orb_reference_schema()

    with temp_engine.connect() as connection:
        assert connection.execute(text("SELECT value FROM legacy_keep")).scalar_one() == "preserve-me"


def test_orb_reference_loader_never_exposes_sample_before_capture_time(monkeypatch):
    temp_engine = create_engine("sqlite:///:memory:")
    database.Base.metadata.create_all(bind=temp_engine)
    monkeypatch.setattr(database, "engine", temp_engine)
    monkeypatch.setattr(database, "SessionLocal", sessionmaker(bind=temp_engine))
    monkeypatch.setattr(database, "DATABASE_URL", "sqlite:///:memory:")
    database._ensure_orb_reference_schema()
    database._ensure_orb_reference_immutability()
    database._ensure_orb_reference_decision_schema()
    database._ensure_orb_reference_decision_immutability()

    bucket = datetime(2026, 9, 4, 13, 30, tzinfo=timezone.utc)
    captured = bucket + timedelta(seconds=4)
    source = captured - timedelta(milliseconds=100)
    journal = MarketStructureJournal(
        saver=lambda _row: None,
        loader=lambda *_args, **_kwargs: [],
        reference_saver=database.save_orb_reference_sample,
        reference_decision_saver=database.save_orb_reference_sample_decision,
        reference_loader=database.load_orb_reference_samples,
        now_utc=lambda: captured,
    )
    result = journal.record_reference(
        _reference_payload(
            _payload(bucket, 6500.0),
            sample_timestamp=bucket,
            source_timestamp=source,
            captured_timestamp=captured,
        )
    )
    assert result["recorded"] is True
    assert database.load_orb_reference_samples(
        "SPX", date(2026, 9, 4), as_of_utc=bucket + timedelta(seconds=1)
    ) == []
    assert len(
        database.load_orb_reference_samples(
            "SPX", date(2026, 9, 4), as_of_utc=captured
        )
    ) == 1


def test_orb_snapshot_loader_preserves_projection_without_hydrating_audit_payload(
    monkeypatch,
):
    temp_engine = create_engine("sqlite:///:memory:")
    database.Base.metadata.create_all(bind=temp_engine)
    monkeypatch.setattr(database, "engine", temp_engine)
    monkeypatch.setattr(database, "SessionLocal", sessionmaker(bind=temp_engine))
    monkeypatch.setattr(database, "DATABASE_URL", "sqlite:///:memory:")
    database._ensure_orb_reference_schema()
    database._ensure_orb_reference_immutability()
    database._ensure_orb_reference_decision_schema()
    database._ensure_orb_reference_decision_immutability()

    observed = datetime(2026, 9, 4, 13, 30, tzinfo=timezone.utc)
    journal = MarketStructureJournal(
        saver=lambda _row: None,
        loader=lambda *_args, **_kwargs: [],
        reference_saver=database.save_orb_reference_sample,
        reference_decision_saver=database.save_orb_reference_sample_decision,
        reference_loader=database.load_orb_reference_samples,
        now_utc=lambda: observed,
    )
    retained_points = (
        (observed, 6500.0),
        (observed + timedelta(hours=1, minutes=1), 6510.0),
        (observed + timedelta(hours=5, minutes=30), 6520.0),
    )
    for timestamp, price in retained_points:
        captured = timestamp + timedelta(seconds=1)
        point_journal = MarketStructureJournal(
            saver=lambda _row: None,
            loader=lambda *_args, **_kwargs: [],
            reference_saver=database.save_orb_reference_sample,
            reference_decision_saver=database.save_orb_reference_sample_decision,
            reference_loader=database.load_orb_reference_samples,
            now_utc=lambda captured=captured: captured,
        )
        assert point_journal.record_reference(
            _reference_payload(
                _payload(timestamp, price),
                sample_timestamp=timestamp,
                source_timestamp=timestamp,
                captured_timestamp=captured,
            )
        )["recorded"] is True

    full = database.load_orb_reference_samples("SPX", date(2026, 9, 4))
    projected = database.load_orb_reference_snapshot_samples(
        "SPX", date(2026, 9, 4)
    )
    expected_fields = {
        "sample_id",
        "sample_timestamp_utc",
        "captured_at_utc",
        "provider",
        "subscription_epoch_id",
        "subscription_generation",
        "reference_price",
        "spot_source",
        "spot_formula_version",
        "risk_free_rate",
        "primary_expiration",
        "same_day_profile_available",
        "universe_sha256",
        "symbol_mapping_version",
        "progress_eligible",
        "progress_decision_status",
        "progress_decision_reason",
        "progress_decided_at_utc",
    }
    assert len(full) == 3
    assert len(projected) == 2
    assert [row["reference_price"] for row in projected] == [6500.0, 6520.0]
    assert set(projected[0]) == expected_fields
    assert "formula_inputs_json" in full[0]
    assert all(projected[0][field] == full[0][field] for field in expected_fields)

    projected_journal = MarketStructureJournal(
        saver=lambda _row: None,
        loader=lambda *_args, **_kwargs: [],
        reference_saver=lambda _row: None,
        reference_loader=database.load_orb_reference_snapshot_samples,
        now_utc=lambda: retained_points[-1][0] + timedelta(seconds=1),
    )
    full_journal = MarketStructureJournal(
        saver=lambda _row: None,
        loader=lambda *_args, **_kwargs: [],
        reference_saver=lambda _row: None,
        reference_loader=database.load_orb_reference_samples,
        now_utc=lambda: retained_points[-1][0] + timedelta(seconds=1),
    )
    assert projected_journal.snapshot("SPX") == full_journal.snapshot("SPX")


def test_orb_snapshot_loader_bounds_full_day_growth_without_changing_projection(
    monkeypatch,
):
    temp_engine = create_engine("sqlite:///:memory:")
    database.Base.metadata.create_all(bind=temp_engine)
    monkeypatch.setattr(database, "engine", temp_engine)
    monkeypatch.setattr(database, "SessionLocal", sessionmaker(bind=temp_engine))
    monkeypatch.setattr(database, "DATABASE_URL", "sqlite:///:memory:")
    database._ensure_orb_reference_schema()
    database._ensure_orb_reference_immutability()
    database._ensure_orb_reference_decision_schema()
    database._ensure_orb_reference_decision_immutability()

    opening_bucket = datetime(2026, 9, 4, 13, 30, tzinfo=timezone.utc)
    seed_journal = MarketStructureJournal(
        saver=lambda _row: None,
        loader=lambda *_args, **_kwargs: [],
        reference_saver=database.save_orb_reference_sample,
        reference_decision_saver=database.save_orb_reference_sample_decision,
        reference_loader=database.load_orb_reference_samples,
        now_utc=lambda: opening_bucket + timedelta(seconds=1),
    )
    assert seed_journal.record_reference(
        _reference_payload(
            _payload(opening_bucket, 6500.0),
            sample_timestamp=opening_bucket,
            source_timestamp=opening_bucket,
            captured_timestamp=opening_bucket + timedelta(seconds=1),
        )
    )["recorded"] is True

    seed = database.load_orb_reference_samples("SPX", opening_bucket.date())[0]
    sample_fields = tuple(
        column.name for column in database.OrbReferenceSample.__table__.columns
    )
    base_sample = {field: seed[field] for field in sample_fields}
    samples = []
    decisions = []
    post_open_count = 1_200
    for index in range(post_open_count):
        sample_time = opening_bucket + timedelta(minutes=61, seconds=index * 5)
        sample_time_naive = sample_time.replace(tzinfo=None)
        captured_at = sample_time_naive + timedelta(seconds=1)
        sample_id = hashlib.sha256(f"post-open-{index}".encode()).hexdigest()
        samples.append(
            {
                **base_sample,
                "sample_id": sample_id,
                "sample_timestamp_utc": sample_time_naive,
                "source_timestamp_utc": sample_time_naive,
                "captured_at_utc": captured_at,
            }
        )
        decisions.append(
            {
                "sample_id": sample_id,
                "sample_timestamp_utc": sample_time_naive,
                "intended_bucket_utc": sample_time_naive,
                "attempt_completed_at_utc": captured_at,
                "progress_eligible": True,
                "reason": None,
                "decision_status": "final",
            }
        )
    with temp_engine.begin() as connection:
        connection.execute(database.OrbReferenceSample.__table__.insert(), samples)
        connection.execute(
            database.OrbReferenceSampleDecision.__table__.insert(), decisions
        )

    as_of = opening_bucket + timedelta(minutes=61, seconds=post_open_count * 5)
    full = database.load_orb_reference_samples(
        "SPX", opening_bucket.date(), as_of_utc=as_of
    )
    projection_reads = []

    def record_projection_read(_connection, _cursor, statement, parameters, _context, _many):
        projection_reads.append((statement.lower(), parameters))

    event.listen(temp_engine, "before_cursor_execute", record_projection_read)
    try:
        projected = database.load_orb_reference_snapshot_samples(
            "SPX", opening_bucket.date(), as_of_utc=as_of
        )
    finally:
        event.remove(temp_engine, "before_cursor_execute", record_projection_read)
    # Runtime reads must not sort a second full-session provenance window or
    # hydrate the growing post-open audit payload merely to publish two rows.
    assert len(projection_reads) == 2
    with temp_engine.connect() as connection:
        metadata_sql, metadata_parameters = projection_reads[0]
        plan = connection.exec_driver_sql(
            "EXPLAIN QUERY PLAN " + metadata_sql, metadata_parameters
        ).fetchall()
    assert any(
        "COVERING INDEX idx_orb_reference_snapshot_metadata" in row[3]
        for row in plan
    )
    assert not any("TEMP B-TREE" in row[3] for row in plan)
    assert all("row_number" not in sql and "formula_inputs_json" not in sql
               for sql, _parameters in projection_reads)
    assert len(projection_reads[-1][1]) == 2
    assert len(full) == post_open_count + 1
    assert len(projected) == 2
    assert [row["sample_id"] for row in projected] == [
        full[0]["sample_id"],
        full[-1]["sample_id"],
    ]
    assert all("formula_inputs_json" not in row for row in projected)

    projected_journal = MarketStructureJournal(
        saver=lambda _row: None,
        loader=lambda *_args, **_kwargs: [],
        reference_saver=lambda _row: None,
        reference_loader=database.load_orb_reference_snapshot_samples,
        now_utc=lambda: as_of,
    )
    full_journal = MarketStructureJournal(
        saver=lambda _row: None,
        loader=lambda *_args, **_kwargs: [],
        reference_saver=lambda _row: None,
        reference_loader=database.load_orb_reference_samples,
        now_utc=lambda: as_of,
    )
    assert projected_journal.snapshot("SPX") == full_journal.snapshot("SPX")


def test_orb_snapshot_loader_retains_post_open_provenance_identities(monkeypatch):
    temp_engine = create_engine("sqlite:///:memory:")
    database.Base.metadata.create_all(bind=temp_engine)
    monkeypatch.setattr(database, "engine", temp_engine)
    monkeypatch.setattr(database, "SessionLocal", sessionmaker(bind=temp_engine))
    monkeypatch.setattr(database, "DATABASE_URL", "sqlite:///:memory:")
    database._ensure_orb_reference_schema()
    database._ensure_orb_reference_immutability()
    database._ensure_orb_reference_decision_schema()
    database._ensure_orb_reference_decision_immutability()

    opening_bucket = datetime(2026, 9, 4, 13, 30, tzinfo=timezone.utc)
    seed_journal = MarketStructureJournal(
        saver=lambda _row: None,
        loader=lambda *_args, **_kwargs: [],
        reference_saver=database.save_orb_reference_sample,
        reference_decision_saver=database.save_orb_reference_sample_decision,
        reference_loader=database.load_orb_reference_samples,
        now_utc=lambda: opening_bucket + timedelta(seconds=1),
    )
    assert seed_journal.record_reference(
        _reference_payload(
            _payload(opening_bucket, 6500.0),
            sample_timestamp=opening_bucket,
            source_timestamp=opening_bucket,
            captured_timestamp=opening_bucket + timedelta(seconds=1),
        )
    )["recorded"] is True

    seed = database.load_orb_reference_samples("SPX", opening_bucket.date())[0]
    sample_fields = tuple(
        column.name for column in database.OrbReferenceSample.__table__.columns
    )
    base_sample = {field: seed[field] for field in sample_fields}
    identity_specs = (
        ("e" * 64, 8, "b" * 64),
        ("e" * 64, 8, "b" * 64),
        ("f" * 64, 9, "c" * 64),
        ("f" * 64, 9, "c" * 64),
    )
    samples = []
    decisions = []
    for index, (epoch_id, generation, universe_sha256) in enumerate(
        identity_specs
    ):
        sample_time = opening_bucket + timedelta(minutes=61, seconds=index * 5)
        sample_time_naive = sample_time.replace(tzinfo=None)
        captured_at = sample_time_naive + timedelta(seconds=1)
        sample_id = hashlib.sha256(
            f"post-open-identity-{index}".encode()
        ).hexdigest()
        samples.append(
            {
                **base_sample,
                "sample_id": sample_id,
                "sample_timestamp_utc": sample_time_naive,
                "source_timestamp_utc": sample_time_naive,
                "captured_at_utc": captured_at,
                "subscription_epoch_id": epoch_id,
                "subscription_generation": generation,
                "universe_sha256": universe_sha256,
            }
        )
        decisions.append(
            {
                "sample_id": sample_id,
                "sample_timestamp_utc": sample_time_naive,
                "intended_bucket_utc": sample_time_naive,
                "attempt_completed_at_utc": captured_at,
                "progress_eligible": True,
                "reason": None,
                "decision_status": "final",
            }
        )
    with temp_engine.begin() as connection:
        connection.execute(database.OrbReferenceSample.__table__.insert(), samples)
        connection.execute(
            database.OrbReferenceSampleDecision.__table__.insert(), decisions
        )

    as_of = opening_bucket + timedelta(minutes=62)
    full = database.load_orb_reference_samples(
        "SPX", opening_bucket.date(), as_of_utc=as_of
    )
    projected = database.load_orb_reference_snapshot_samples(
        "SPX", opening_bucket.date(), as_of_utc=as_of
    )

    def provenance_identity(row):
        return (
            row["provider"],
            row["subscription_epoch_id"],
            row["subscription_generation"],
            row["universe_sha256"],
        )

    expected_identities = {provenance_identity(row) for row in full}
    projected_identities = {provenance_identity(row) for row in projected}
    assert len(expected_identities) == 3
    assert projected_identities == expected_identities
    assert len(projected) == 4
    assert len(projected) < len(full)

    projected_journal = MarketStructureJournal(
        saver=lambda _row: None,
        loader=lambda *_args, **_kwargs: [],
        reference_saver=lambda _row: None,
        reference_loader=database.load_orb_reference_snapshot_samples,
        now_utc=lambda: as_of,
    )
    full_journal = MarketStructureJournal(
        saver=lambda _row: None,
        loader=lambda *_args, **_kwargs: [],
        reference_saver=lambda _row: None,
        reference_loader=database.load_orb_reference_samples,
        now_utc=lambda: as_of,
    )
    assert projected_journal.snapshot("SPX") == full_journal.snapshot("SPX")


def test_structure_loader_never_exposes_observation_before_capture_time(monkeypatch):
    temp_engine = create_engine("sqlite:///:memory:")
    database.Base.metadata.create_all(bind=temp_engine)
    temp_session = sessionmaker(bind=temp_engine)
    monkeypatch.setattr(database, "engine", temp_engine)
    monkeypatch.setattr(database, "SessionLocal", temp_session)

    source = datetime(2026, 9, 4, 13, 30, tzinfo=timezone.utc)
    captured = source + timedelta(seconds=4)
    session = temp_session()
    try:
        session.add(
            database.MarketStructureObservation(
                observation_id="e" * 64,
                symbol="SPX",
                trading_date=date(2026, 9, 4),
                source_timestamp_utc=source.replace(tzinfo=None),
                captured_at_utc=captured.replace(tzinfo=None),
                provider="databento",
                subscription_epoch_id="f" * 64,
                subscription_generation=7,
                calculation_id="0f610e22-4976-45ee-a744-9ff28b743786",
                reference_price=6500.0,
                spot_source="databento_opra_put_call_parity",
                gamma_pin=6510.0,
                max_pain=6490.0,
                zero_gamma=6475.0,
                pin_lead_ratio=1.25,
                pin_is_contested=False,
                gross_gex=125.0,
                net_gex=-25.0,
                primary_expiration="2026-09-04",
                same_day_profile_available=True,
                universe_sha256="a" * 64,
                validation_status="valid",
            )
        )
        session.commit()
    finally:
        session.close()

    assert database.load_market_structure_observations(
        "SPX", date(2026, 9, 4), as_of_utc=source + timedelta(seconds=1)
    ) == []
    assert database.load_market_structure_observations(
        "SPX", date(2026, 9, 4), as_of_utc=captured
    ) == [
        {
            "observation_id": "e" * 64,
            "symbol": "SPX",
            "trading_date": date(2026, 9, 4),
            "source_timestamp_utc": source.replace(tzinfo=None),
            "captured_at_utc": captured.replace(tzinfo=None),
            "provider": "databento",
            "subscription_epoch_id": "f" * 64,
            "subscription_generation": 7,
            "calculation_id": "0f610e22-4976-45ee-a744-9ff28b743786",
            "reference_price": 6500.0,
            "spot_source": "databento_opra_put_call_parity",
            "gamma_pin": 6510.0,
            "max_pain": 6490.0,
            "zero_gamma": 6475.0,
            "pin_lead_ratio": 1.25,
            "pin_is_contested": False,
            "gross_gex": 125.0,
            "net_gex": -25.0,
            "primary_expiration": "2026-09-04",
            "same_day_profile_available": True,
            "universe_sha256": "a" * 64,
            "validation_status": "valid",
        }
    ]


def test_structure_loader_closes_core_connection_and_fails_empty(monkeypatch, caplog):
    class BrokenConnection:
        closed = False

        def __enter__(self):
            return self

        def execute(self, _statement):
            raise RuntimeError("synthetic read failure")

        def __exit__(self, *_args):
            self.closed = True

    connection = BrokenConnection()

    class BrokenEngine:
        def connect(self):
            return connection

    monkeypatch.setattr(database, "engine", BrokenEngine())
    with caplog.at_level("WARNING"):
        assert database.load_market_structure_observations(
            "SPX", date(2026, 9, 4)
        ) == []
    assert connection.closed is True
    assert "Failed to load market structure observations for SPX" in caplog.text


def test_live_capture_loop_records_new_revisions_without_http_requests():
    timestamp = datetime(2026, 9, 4, 13, 30, tzinfo=timezone.utc)
    journal, rows = _memory_journal(now=timestamp)

    class Streamer:
        def get_all_latest(self):
            return {"SPX": _payload(timestamp, 6500)}

    async def exercise():
        task = asyncio.create_task(
            run_market_structure_capture_loop(Streamer(), journal, poll_seconds=0.01)
        )
        await asyncio.sleep(0.08)
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)

    asyncio.run(exercise())
    assert len(rows) == 1


def test_active_fastapi_app_registers_orb_routes():
    paths = {route.path for route in app.routes}
    assert "/orb" in paths
    assert "/orb/{symbol}" in paths
    assert "/v1/orb" in paths
    assert "/v1/orb/{symbol}" in paths
