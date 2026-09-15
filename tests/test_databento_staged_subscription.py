from __future__ import annotations

import hashlib
from datetime import date, datetime, timedelta, timezone
from types import SimpleNamespace

import pandas as pd
import pytest

from backend.databento_streamer import DatabentoGammaStreamer, raw_option_symbol


TRADING_DATE = date(2026, 9, 9)
FAMILIES = ("SPX", "NDX", "VIX", "RUT")
ROOT_AND_STRIKE = {
    "SPX": ("SPXW", 7600.0),
    "NDX": ("NDXP", 25000.0),
    "VIX": ("VIXW", 20.0),
    "RUT": ("RUTW", 2400.0),
}


def _build_staged_streamer(
    monkeypatch,
    *,
    optional_primary_pair_counts: dict[str, int] | None = None,
) -> DatabentoGammaStreamer:
    monkeypatch.setattr(
        "backend.databento_streamer.current_market_date", lambda: TRADING_DATE
    )
    monkeypatch.setattr("backend.databento_streamer.MIN_PRIMARY_STRIKE_PAIRS", 1)
    monkeypatch.setattr("backend.databento_streamer.MIN_NEXT_LISTED_STRIKE_PAIRS", 1)
    monkeypatch.setattr("backend.databento_streamer.MIN_PAIRED_QUOTES", 5)
    monkeypatch.setattr("backend.databento_streamer.MAX_SUBSCRIPTION_CONTRACTS", 100)
    requested_optional_counts = optional_primary_pair_counts or {}
    rows = []
    for family, (root, strike) in ROOT_AND_STRIKE.items():
        primary = TRADING_DATE + timedelta(days=1) if family == "VIX" else TRADING_DATE
        pair_count = (
            requested_optional_counts.get(family, 5)
            if family in {"VIX", "RUT"}
            else 1
        )
        for expiration in (primary, primary + timedelta(days=1)):
            for pair_index in range(pair_count):
                pair_strike = strike + pair_index
                for option_type in ("C", "P"):
                    rows.append(
                        {
                            "market": family,
                            "symbol": raw_option_symbol(
                                root, expiration, option_type, pair_strike
                            ),
                            "strike": pair_strike,
                            "option_type": option_type,
                            "expiration_date": expiration,
                            "open_interest": 1_000.0,
                        }
                    )
    streamer = DatabentoGammaStreamer(list(FAMILIES))
    streamer._apply_subscription_profile(
        pd.DataFrame(rows),
        as_of=TRADING_DATE,
        provenance={
            "mode": "test",
            "trading_date": TRADING_DATE.isoformat(),
            "source_date": TRADING_DATE.isoformat(),
            "source_sha256": "f" * 64,
            "is_fallback": False,
        },
    )
    return streamer


def _complete_orb_evidence() -> dict[str, object]:
    return {
        family: {
            "exact_complete": True,
            "capture_status": "complete",
            "orb_complete": True,
        }
        for family in FAMILIES
    }


def _prime_clean_family_metrics(
    streamer: DatabentoGammaStreamer,
    *,
    now_utc: datetime,
    now_monotonic: float,
) -> None:
    streamer.handoff_status = "active"
    streamer._last_progress_monotonic = now_monotonic
    streamer.latest_invalid = {
        family: {
            "receive_to_process_lag_p95_seconds": 0.25,
            "primary_pair_coverage_ratio": 0.75,
        }
        for family in FAMILIES
    }
    streamer.last_diagnostic_time = {
        family: now_utc.replace(tzinfo=None) for family in FAMILIES
    }
    streamer._fresh_quote_counts = lambda: dict(  # type: ignore[method-assign]
        streamer.subscription_stage_primary_counts
    )


def test_initial_live_request_contains_all_four_primary_expirations_only(
    monkeypatch,
):
    streamer = _build_staged_streamer(monkeypatch)
    full_symbols = list(streamer.live_symbols)
    full_hash = streamer.subscription_metadata["selected_universe_sha256"]
    created = []

    class PrimaryStageClient:
        def __init__(self, **kwargs):
            self.kwargs = kwargs
            self.subscribe_calls = []
            created.append(self)

        def subscribe(self, **kwargs):
            self.subscribe_calls.append(kwargs)
            return 0

        def __iter__(self):
            streamer.is_running = False
            return iter(())

        def stop(self):
            return None

        def block_for_close(self, timeout):
            assert timeout == 5.0

    streamer.api_key = "test-key"
    streamer.replay_minutes = 0
    streamer.is_running = True
    monkeypatch.setattr(
        streamer,
        "_subscription_window",
        lambda: {"state": "regular_session", "subscription_allowed": True},
    )
    monkeypatch.setattr(streamer, "_build_universe", lambda **_kwargs: None)
    monkeypatch.setattr(
        "backend.databento_streamer.db.Live", PrimaryStageClient
    )

    streamer._run_live()

    assert len(created) == 1
    subscribed = created[0].subscribe_calls[0]["symbols"]
    assert len(subscribed) == sum(
        streamer.subscription_stage_primary_counts.values()
    )
    assert {
        streamer._market_for_raw_symbol(symbol) for symbol in subscribed
    } == set(FAMILIES)
    assert all(
        next(
            row.expiration_date
            for row in streamer.universe.itertuples()
            if row.symbol == symbol
        )
        == date.fromisoformat(
            next(
                entry["expiration"]
                for entry in streamer.subscription_metadata["markets"][
                    streamer._market_for_raw_symbol(symbol)
                ]["selected_expirations"]
                if entry["role"] == "primary"
            )
        )
        for symbol in subscribed
    )
    assert streamer.subscription_metadata["markets"]["VIX"][
        "primary_expiration_authority"
    ] == "vix_forward_expiration_context_only"
    assert streamer.live_symbols == full_symbols
    assert streamer.subscription_metadata["selected_universe_sha256"] == full_hash
    assert streamer.active_generation == 1
    assert streamer.subscription_stage_generation == 1
    assert streamer.subscription_stage_state == "primary_active"


def test_all_primary_minimum_cap_failure_occurs_before_live_client(monkeypatch):
    streamer = _build_staged_streamer(monkeypatch)
    monkeypatch.setattr("backend.databento_streamer.MAX_SUBSCRIPTION_CONTRACTS", 6)
    created = []

    class ForbiddenClient:
        def __init__(self, **_kwargs):
            created.append(True)

    streamer.api_key = "test-key"
    streamer.replay_minutes = 0
    streamer.is_running = True
    monkeypatch.setattr(
        streamer,
        "_subscription_window",
        lambda: {"state": "regular_session", "subscription_allowed": True},
    )
    monkeypatch.setattr(streamer, "_build_universe", lambda **_kwargs: None)
    monkeypatch.setattr("backend.databento_streamer.db.Live", ForbiddenClient)

    def stop_after_backoff(_seconds):
        streamer.is_running = False

    monkeypatch.setattr("backend.databento_streamer.time.sleep", stop_after_backoff)

    streamer._run_live()

    assert created == []
    assert streamer.subscription_stage_state == "blocked"
    assert any(
        reason.startswith("ALL_PRIMARY_MINIMA_EXCEED_SUBSCRIPTION_CAP")
        for reason in streamer.subscription_stage_promotion_reasons
    )


@pytest.mark.parametrize("sparse_family", ["VIX", "RUT"])
def test_direct_stage_rejects_optional_family_below_orb_pair_floor(
    monkeypatch,
    sparse_family,
):
    streamer = _build_staged_streamer(
        monkeypatch,
        optional_primary_pair_counts={sparse_family: 1},
    )
    admission = streamer.subscription_metadata["markets"][sparse_family][
        "primary_pair_admission"
    ]

    assert admission["minimum_pair_count"] == 5
    assert admission["complete_pair_count"] == 1
    assert admission["passes"] is False
    with pytest.raises(
        RuntimeError,
        match=rf"PRIMARY_PAIR_MINIMUM_NOT_MET:{sparse_family}:1<5",
    ):
        streamer._prepare_live_subscription_stages()
    assert streamer.subscription_stage_state == "blocked"


def test_promotion_requires_continuous_clean_transport_and_exact_four_orbs(
    monkeypatch,
):
    streamer = _build_staged_streamer(monkeypatch)
    streamer._prepare_live_subscription_stages()
    streamer.active_generation = 4
    streamer.messages_received = 100
    streamer._activate_initial_subscription_stage(4)
    started_utc = datetime(2026, 9, 9, 15, 31, tzinfo=timezone.utc)
    _prime_clean_family_metrics(
        streamer,
        now_utc=started_utc,
        now_monotonic=100.0,
    )
    streamer.messages_received = 101

    assert not streamer._evaluate_subscription_stage_promotion(
        observed_monotonic=100.0,
        observed_at_utc=started_utc,
        orb_evidence=_complete_orb_evidence(),
    )
    assert "CLEAN_TRANSPORT_WINDOW_INCOMPLETE" in (
        streamer.subscription_stage_promotion_reasons
    )

    later_utc = started_utc + timedelta(seconds=60)
    streamer.messages_received = 200
    streamer._last_progress_monotonic = 160.0
    streamer.last_diagnostic_time = {
        family: later_utc.replace(tzinfo=None) for family in FAMILIES
    }
    missing_rut = _complete_orb_evidence()
    missing_rut["RUT"] = {"exact_complete": False, "capture_status": "partial"}
    assert not streamer._evaluate_subscription_stage_promotion(
        observed_monotonic=160.0,
        observed_at_utc=later_utc,
        orb_evidence=missing_rut,
    )
    assert "ORB_60M_NOT_EXACT_COMPLETE:RUT" in (
        streamer.subscription_stage_promotion_reasons
    )

    streamer.messages_received = 201
    streamer._last_progress_monotonic = 161.0
    final_utc = later_utc + timedelta(seconds=1)
    streamer.last_diagnostic_time = {
        family: final_utc.replace(tzinfo=None) for family in FAMILIES
    }
    assert streamer._evaluate_subscription_stage_promotion(
        observed_monotonic=161.0,
        observed_at_utc=final_utc,
        orb_evidence=_complete_orb_evidence(),
    )
    assert streamer.subscription_stage_promotion_reasons == []


def test_queue_freezes_but_slow_or_skipped_cancels_deferred_stage(monkeypatch):
    streamer = _build_staged_streamer(monkeypatch)
    streamer._prepare_live_subscription_stages()
    streamer._activate_initial_subscription_stage(1)

    streamer.record_provider_warning("record queue is full with 600 pending records")

    assert streamer.subscription_stage_state == "frozen"
    assert "QUEUE_FULL_DELTA_FREEZES_PROMOTION" in (
        streamer.subscription_stage_promotion_reasons
    )

    streamer.record_provider_warning("slow client detected; skipped 12 records")

    assert streamer.subscription_stage_state == "canceled"
    assert "SLOW_OR_SKIPPED_RECORD_DELTA_CANCELS_PROMOTION" in (
        streamer.subscription_stage_promotion_reasons
    )


def test_reconnect_delta_cancels_deferred_stage(monkeypatch):
    streamer = _build_staged_streamer(monkeypatch)
    streamer._prepare_live_subscription_stages()
    streamer._activate_initial_subscription_stage(1)
    streamer.reconnect_attempts += 1

    assert not streamer._evaluate_subscription_stage_promotion(
        observed_monotonic=100.0,
        observed_at_utc=datetime(2026, 9, 9, 15, 31, tzinfo=timezone.utc),
        orb_evidence=_complete_orb_evidence(),
    )
    assert streamer.subscription_stage_state == "canceled"
    assert any(
        reason.endswith("reconnect")
        for reason in streamer.subscription_stage_promotion_reasons
    )


def test_canary_cannot_drop_rut_during_all_index_primary_stage(monkeypatch):
    streamer = _build_staged_streamer(monkeypatch)
    initial_symbols = streamer._prepare_live_subscription_stages()
    streamer._activate_initial_subscription_stage(2)
    stopped = []
    streamer.client = SimpleNamespace(stop=lambda: stopped.append(True))

    removed = streamer._apply_optional_family_canary_rollback(
        SimpleNamespace(rollback_required=True, reasons=("CANARY_INVALID:RUT",))
    )

    assert removed is False
    assert streamer.symbols == list(FAMILIES)
    assert streamer.active_live_symbols == initial_symbols
    assert any(
        streamer._market_for_raw_symbol(symbol) == "RUT"
        for symbol in streamer.active_live_symbols
    )
    assert stopped == []
    assert streamer.optional_family_canary_evaluation_evidence[
        "mutation_suppressed_for_all_index_orb"
    ] is True


def test_additive_promotion_accepts_none_id_and_preserves_epoch_generation(
    monkeypatch,
):
    streamer = _build_staged_streamer(monkeypatch)
    initial_symbols = streamer._prepare_live_subscription_stages()
    full_symbols = list(streamer.live_symbols)
    full_hash = streamer.subscription_metadata["selected_universe_sha256"]
    streamer.active_generation = 9
    streamer.handoff_status = "active"
    streamer._activate_initial_subscription_stage(9)
    calls = []

    class AdditiveClient:
        def subscribe(self, **kwargs):
            calls.append(kwargs)
            return None

    streamer.client = AdditiveClient()

    def eligible(**_kwargs):
        streamer.subscription_stage_promotion_eligible = True
        streamer.subscription_stage_promotion_reasons = []
        return True

    monkeypatch.setattr(streamer, "_evaluate_subscription_stage_promotion", eligible)

    epoch_before = streamer.subscription_epoch_id
    assert streamer._maybe_promote_deferred_subscription_stage()

    assert len(calls) == 1
    assert calls[0]["symbols"] == [
        symbol for symbol in full_symbols if symbol not in set(initial_symbols)
    ]
    assert "start" not in calls[0]
    assert streamer.subscription_epoch_id == epoch_before
    assert streamer.active_generation == 9
    assert streamer.subscription_stage_generation == 9
    assert streamer.subscription_metadata["selected_universe_sha256"] == full_hash
    assert streamer.subscription_stage_state == "full_active"
    assert streamer.active_live_symbols == full_symbols
    assert streamer.deferred_live_symbols == []
    assert streamer.subscription_stage_additive_request_sent is True
    assert streamer.subscription_stage_additive_subscription_id is None


def test_orb_snapshot_requires_only_primary_mappings_while_hash_stays_full(
    monkeypatch,
):
    streamer = _build_staged_streamer(monkeypatch)
    initial_symbols = streamer._prepare_live_subscription_stages()
    streamer.active_generation = 3
    streamer.handoff_status = "active"
    streamer.subscription_cutoff_monotonic = 1.0
    instrument_id = 100
    for symbol in initial_symbols:
        mapping_version = hashlib.sha256(symbol.encode("utf-8")).hexdigest()
        streamer.symbol_mappings[instrument_id] = {
            "raw_symbol": symbol,
            "mapping_version": mapping_version,
        }
        instrument_id += 1

    snapshot, reason = streamer._snapshot_opening_reference_inputs(
        "SPX",
        trading_date=TRADING_DATE,
        captured_at_utc=datetime(2026, 9, 9, 13, 30, tzinfo=timezone.utc),
    )

    assert reason is None
    assert snapshot is not None
    assert len(snapshot["records"]) == 2
    assert snapshot["selected_universe_sha256"] == (
        streamer.subscription_metadata["selected_universe_sha256"]
    )
    assert len(streamer.live_symbols) == 48


def test_orb_snapshot_context_stays_current_after_shadow_mappings_arrive(
    monkeypatch,
):
    streamer = _build_staged_streamer(monkeypatch)
    initial_symbols = streamer._prepare_live_subscription_stages()
    streamer.active_generation = 3
    streamer.handoff_status = "active"
    streamer.subscription_cutoff_monotonic = 1.0
    with streamer._fresh_quote_lock:
        for instrument_id, symbol in enumerate(streamer.live_symbols, start=100):
            streamer.symbol_mappings[instrument_id] = {
                "raw_symbol": symbol,
                "mapping_version": hashlib.sha256(symbol.encode("utf-8")).hexdigest(),
            }

    snapshot, reason = streamer._snapshot_opening_reference_inputs(
        "SPX",
        trading_date=TRADING_DATE,
        captured_at_utc=datetime(2026, 9, 9, 13, 30, tzinfo=timezone.utc),
    )

    assert reason is None
    assert snapshot is not None
    assert len(streamer.live_symbols) > len(initial_symbols)
    assert len(snapshot["mapping_versions_by_symbol"]) == 2
    assert streamer._opening_reference_context_is_current(snapshot)


def test_subscription_stage_health_exposes_active_deferred_and_gate_details(
    monkeypatch,
):
    streamer = _build_staged_streamer(monkeypatch)
    streamer._prepare_live_subscription_stages()
    streamer._activate_initial_subscription_stage(5)

    health = streamer._subscription_stage_health()

    assert health["active_stage"] == "primary"
    assert health["deferred_stage"] == "shadow"
    assert health["active_contract_count"] == len(streamer.active_live_symbols)
    assert health["deferred_contract_count"] == len(streamer.deferred_live_symbols)
    assert health["full_selected_contract_count"] == len(streamer.live_symbols)
    assert health["requested_orb_families"] == list(FAMILIES)
    assert health["promotion_eligible"] is False
    assert health["promotion_reasons"]
    assert health["promotion_thresholds"] == {
        "clean_transport_seconds": 60.0,
        "maximum_data_age_seconds": 15.0,
        "maximum_p95_lag_seconds": 2.0,
        "minimum_primary_pair_coverage_ratio": 0.1,
        "minimum_fresh_coverage_ratio": 0.5,
        "requires_all_60m_orbs_exact_complete": True,
    }
    public_health = streamer.get_health()
    assert public_health["symbols_subscribed"] == len(streamer.active_live_symbols)
    assert public_health["symbols_selected"] == len(streamer.live_symbols)
    assert public_health["subscription_staging"]["state"] == "primary_active"
    assert public_health["market_subscription_status"]["RUT"][
        "active_contract_count"
    ] == 10
    assert public_health["market_subscription_status"]["RUT"][
        "deferred_contract_count"
    ] == 10
