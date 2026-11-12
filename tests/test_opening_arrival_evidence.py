"""Failed opening receipts retain observed generation-local arrival evidence."""
from copy import deepcopy
from datetime import timedelta
import json

import pytest

import backend.databento_streamer as stream_module
from tests.test_opening_transition_stress import september_14_open
from tests.test_orb_cbbo_capture import _record


def _seed_arrivals(streamer, opening, monkeypatch):
    observed_ns = int(opening.timestamp()) * 10**9 + 200_000_000
    monkeypatch.setattr(stream_module.time, "time_ns", lambda: observed_ns + 100)
    arrivals = streamer._quote_arrivals
    arrivals.received("SPX", observed_ns, observed_ns - 100, False)
    arrivals.admitted("SPX", False)
    # A received, rejected NDX quote is not an admitted quote or active handoff.
    arrivals.received("NDX", observed_ns + 10, observed_ns - 90, False)
    arrivals.rejected("NDX", "INVALID_QUOTE_PRICE")
    arrivals.received(None, observed_ns + 20, observed_ns - 80, False)
    arrivals.rejected(None, "SYMBOL_MAPPING_UNAVAILABLE")
    streamer.messages_received = 12345
    streamer.quote_records_seen = 23456
    streamer.unmapped_quote_records = 12
    return arrivals


@pytest.mark.parametrize("clock_ready", [False, True])
def test_opening_failure_records_arrival_stages_without_admitting_quotes(
    september_14_open, monkeypatch, clock_ready,
):
    streamer, journal, _engine, opening = september_14_open
    _seed_arrivals(streamer, opening, monkeypatch)
    if clock_ready:
        recv = int(opening.timestamp()) * 10**9 + 1_000_000_000
        for instrument in range(stream_module.CLOCK_SYNC_MIN_SAMPLES):
            assert streamer._record_processing_clock_sample(
                _record(instrument + 1, recv),
                processed_at_ns=recv + 20_000_000, generation=7,
            )
    result = streamer._capture_opening_reference_for_bucket(
        "NDX", opening, now_utc=lambda: opening + timedelta(seconds=2), journal=journal,
    )
    assert result["recorded"] is False
    assert result.get("progress_eligible", False) is False
    assert result["reason"] == (
        "HANDOFF_NOT_ACTIVE" if clock_ready else "PROCESSING_CLOCK_NOT_SYNCHRONIZED"
    )
    evidence = result["opening_gate_evidence"]
    arrivals = evidence["quote_arrival_diagnostics"]
    assert arrivals["scope"] == "subscription_generation"
    assert arrivals["subscription_epoch_id"] == streamer.subscription_epoch_id
    assert arrivals["subscription_generation"] == 7
    assert arrivals["families"]["SPX"]["first_admitted_at_utc"] is not None
    ndx = arrivals["families"]["NDX"]
    assert ndx["first_received_at_utc"] is not None
    assert ndx["first_rejection_reason"] == "INVALID_QUOTE_PRICE"
    assert ndx["first_admitted_at_utc"] is None
    unknown = arrivals["families"]["UNKNOWN"]
    assert unknown["first_received_at_utc"] is not None
    assert unknown["first_mapped_at_utc"] is None
    assert unknown["first_admitted_at_utc"] is None
    counters = evidence["transport_counters"]
    assert counters["scope"] == "process_shared_cumulative"
    assert counters["messages_received"] == 12345
    assert counters["quote_records_seen"] == 23456
    assert counters["unmapped_quote_records"] == 12
    assert evidence["pair_evaluation"] == "not_evaluated"
    assert streamer.handoff_status == "warming"
    assert streamer._processing_clock_telemetry()["minimum_samples"] == 50


def test_failure_result_and_immutable_receipt_survive_generation_reset(
    september_14_open, monkeypatch,
):
    streamer, journal, _engine, opening = september_14_open
    retired_arrivals = _seed_arrivals(streamer, opening, monkeypatch)
    result = streamer._capture_opening_reference_for_bucket(
        "SPX", opening, now_utc=lambda: opening + timedelta(seconds=1), journal=journal,
    )
    before = deepcopy(result["opening_gate_evidence"])
    streamer._record_opening_reference_failed_attempt(
        market="SPX", subscription_epoch_id=streamer.subscription_epoch_id,
        subscription_generation=7, intended_bucket_utc=opening, result=result,
    )
    receipt = streamer.audit_dir.parent / "opening_reference_attempts" / "2026-09-14.ndjson"
    receipt_bytes = receipt.read_bytes()
    persisted = json.loads(receipt_bytes.splitlines()[-1])
    assert persisted["opening_gate_evidence"] == before
    assert persisted["usable_for_prediction"] is False
    assert persisted["validation_is_valid"] is False
    assert persisted["recorded"] is False

    # The next connection replaces live telemetry, but cannot alter the old receipt.
    streamer.active_generation = 8
    retired_arrivals.admitted("NDX", False)
    assert result["opening_gate_evidence"] == before
    assert receipt.read_bytes() == receipt_bytes
    next_result = streamer._capture_opening_reference_for_bucket(
        "SPX", opening, now_utc=lambda: opening + timedelta(seconds=2), journal=journal,
    )
    assert next_result["recorded"] is False
    next_arrivals = next_result["opening_gate_evidence"]["quote_arrival_diagnostics"]
    assert next_arrivals["subscription_generation"] == 8
    assert next_arrivals["families"]["SPX"]["first_received_at_utc"] is None
    assert next_arrivals["families"]["NDX"]["first_admitted_at_utc"] is None
    assert before["quote_arrival_diagnostics"]["subscription_generation"] == 7
    assert before["quote_arrival_diagnostics"]["families"]["NDX"]["first_admitted_at_utc"] is None


@pytest.mark.parametrize("mismatch", ["epoch", "generation"])
def test_unbound_arrival_diagnostics_are_explicitly_unavailable(
    september_14_open, mismatch,
):
    streamer, journal, _engine, opening = september_14_open
    streamer._quote_arrivals = stream_module._QuoteArrivalTelemetry(
        "f" * 64 if mismatch == "epoch" else streamer.subscription_epoch_id,
        6 if mismatch == "generation" else 7,
        streamer.symbols,
    )
    result = streamer._capture_opening_reference_for_bucket(
        "NDX", opening, now_utc=lambda: opening + timedelta(seconds=1), journal=journal,
    )
    assert result["recorded"] is False
    assert result["reason"] == "PROCESSING_CLOCK_NOT_SYNCHRONIZED"
    diagnostic = result["opening_gate_evidence"]["quote_arrival_diagnostics"]
    assert diagnostic["available"] is False
    assert diagnostic["reason"] == "ARRIVAL_DIAGNOSTICS_IDENTITY_MISMATCH"
    assert "families" not in diagnostic


def test_retained_replay_milestones_cannot_supply_live_clock_authority(
    september_14_open, monkeypatch,
):
    streamer, journal, _engine, opening = september_14_open
    received = int(opening.timestamp()) * 10**9
    monkeypatch.setattr(stream_module.time, "time_ns", lambda: received + 100)
    streamer._quote_arrivals.received("NDX", received, received, True)
    streamer._quote_arrivals.admitted("NDX", True)
    streamer._clock_replay_pending = True
    for instrument in range(stream_module.CLOCK_SYNC_MIN_SAMPLES):
        assert not streamer._record_processing_clock_sample(
            _record(instrument + 1, received),
            processed_at_ns=received + 100, generation=7,
        )
    result = streamer._capture_opening_reference_for_bucket(
        "NDX", opening, now_utc=lambda: opening + timedelta(seconds=1), journal=journal,
    )
    assert result["recorded"] is False
    assert result["reason"] == "PROCESSING_CLOCK_NOT_SYNCHRONIZED"
    evidence = result["opening_gate_evidence"]
    assert evidence["processing_clock"]["sample_count"] == 0
    arrival = evidence["quote_arrival_diagnostics"]["families"]["NDX"]
    assert arrival["first_received_replay_pending"] is True
    assert arrival["first_admitted_replay_pending"] is True
    assert evidence["handoff"]["status"] == "warming"
