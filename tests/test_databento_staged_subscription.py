from __future__ import annotations

import hashlib
import json
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


def _ready_evaluation(monkeypatch, *, now=None):
    streamer = _build_staged_streamer(monkeypatch)
    streamer._prepare_live_subscription_stages()
    streamer.active_generation = 4
    streamer._activate_initial_subscription_stage(4)
    now = now or datetime(2026, 9, 9, 15, 31, tzinfo=timezone.utc)
    _prime_clean_family_metrics(streamer, now_utc=now, now_monotonic=160.0)
    streamer.subscription_stage_clean_started_monotonic = 100.0
    streamer.messages_received = 10
    return streamer, now


@pytest.mark.parametrize("clock,reason", [
    (datetime(2026, 9, 9, 14, 29, 59, tzinfo=timezone.utc), "OPENING_HOUR_NOT_ELAPSED"),
    (datetime(2026, 9, 9, 20, tzinfo=timezone.utc), "OUTSIDE_VERIFIED_REGULAR_SESSION"),
    (datetime(2026, 9, 12, 15, tzinfo=timezone.utc), "OUTSIDE_VERIFIED_REGULAR_SESSION"),
])
def test_segment_never_bypasses_session_even_with_complete_orbs(monkeypatch, clock, reason):
    streamer, now = _ready_evaluation(monkeypatch, now=clock)
    assert not streamer._evaluate_subscription_stage_promotion(
        observed_monotonic=160.0, observed_at_utc=now, orb_evidence=_complete_orb_evidence())
    assert reason in streamer.subscription_stage_promotion_reasons


@pytest.mark.parametrize("failure,reason", [
    ("clock", "PROCESSING_CLOCK_NOT_SYNCHRONIZED"),
    ("prior_universe", "CURRENT_SESSION_UNIVERSE_NOT_ESTABLISHED"),
    ("fallback", "CURRENT_SESSION_UNIVERSE_NOT_ESTABLISHED"),
    ("generation", "SUBSCRIPTION_GENERATION_CHANGED"),
    ("epoch", "SUBSCRIPTION_EPOCH_CHANGED"),
    ("payload_epoch", "DIAGNOSTIC_EPOCH_NOT_CURRENT:SPX"),
    ("payload_generation", "DIAGNOSTIC_GENERATION_NOT_CURRENT:SPX"),
    ("coverage", "FRESH_COVERAGE_LOW:SPX"),
    ("pairs", "PRIMARY_PAIR_COVERAGE_LOW:SPX"),
    ("lag", "P95_LAG_EXCEEDED_OR_INVALID:SPX"),
    ("age", "DATA_AGE_EXCEEDED:SPX"),
])
def test_partial_segment_keeps_live_gates(monkeypatch, failure, reason):
    streamer, now = _ready_evaluation(monkeypatch)
    if failure == "clock":monkeypatch.setattr(streamer, "_processing_clock_telemetry", lambda: {"status":"unknown"})
    if failure == "prior_universe":streamer.subscription_metadata['universe_provenance']['source_date']='2026-09-08'
    if failure == "fallback":streamer.subscription_metadata['universe_provenance']['is_fallback']=True
    if failure == "generation":streamer.active_generation += 1
    if failure == "epoch":streamer.subscription_epoch_id='new-epoch'
    if failure == "payload_epoch":streamer.latest_invalid['SPX']['subscription_epoch_id']='old-epoch'
    if failure == "payload_generation":streamer.latest_invalid['SPX']['subscription_generation']=3
    if failure == "coverage":streamer._fresh_quote_counts=lambda: {}
    if failure == "pairs":streamer.latest_invalid['SPX']['primary_pair_coverage_ratio']=0.09
    if failure == "lag":streamer.latest_invalid['SPX']['receive_to_process_lag_p95_seconds']=2.01
    if failure == "age":streamer.last_diagnostic_time['SPX'] -= timedelta(seconds=16)
    assert not streamer._evaluate_subscription_stage_promotion(
        observed_monotonic=160.0, observed_at_utc=now, orb_evidence={})
    assert reason in streamer.subscription_stage_promotion_reasons


def test_newer_diagnostic_cannot_be_hidden_by_old_valid_payload(monkeypatch):
    streamer, now = _ready_evaluation(monkeypatch)
    streamer.latest_pins['SPX']=dict(streamer.latest_invalid['SPX'])
    streamer.last_message_time['SPX']=now.replace(tzinfo=None)-timedelta(seconds=300)
    assert streamer._evaluate_subscription_stage_promotion(
        observed_monotonic=160.0, observed_at_utc=now, orb_evidence={})
    assert streamer.subscription_stage_metrics['families']['SPX']['data_age_seconds']==0
    streamer.latest_invalid['SPX']['primary_pair_coverage_ratio']=0
    streamer.messages_received+=1
    assert not streamer._evaluate_subscription_stage_promotion(
        observed_monotonic=161.0, observed_at_utc=now, orb_evidence={})
    assert 'PRIMARY_PAIR_COVERAGE_LOW:SPX' in streamer.subscription_stage_promotion_reasons


@pytest.mark.parametrize("failure", ["intent_write", "outcome_write", "subscribe", "overload", "changed_plan", "expired_check"])
def test_segment_request_failures_preserve_evidence_and_never_retry(monkeypatch, tmp_path, failure):
    streamer, _ = _ready_evaluation(monkeypatch)
    calls=[]
    def subscribe(**kwargs):
        calls.append(kwargs)
        if failure=='subscribe':raise RuntimeError('test failure')
        return 10
    streamer.client=SimpleNamespace(subscribe=subscribe)
    def eligible(**kwargs):
        streamer.subscription_stage_promotion_eligible=True
        streamer.subscription_stage_last_evaluated_utc=datetime.now(timezone.utc)-timedelta(seconds=6 if failure=='expired_check' else 0)
        if failure=='changed_plan':streamer.live_symbols.append('unexpected')
        return True
    monkeypatch.setattr(streamer,'_evaluate_subscription_stage_promotion',eligible)
    monkeypatch.setattr('backend.databento_streamer.live_subscription_window',lambda _now: {'state':'regular_session'})
    monkeypatch.setattr('backend.databento_streamer.DATA_DIR',tmp_path)
    from backend.subscription_segment import write_segment_receipt
    def receipt(evidence):
        if (failure=='intent_write' or (failure=='outcome_write' and evidence['request_state']!='intent_only')):
            raise OSError('test disk failure')
        result=write_segment_receipt(tmp_path,evidence)
        if failure=='overload':streamer.provider_queue_full_warnings+=1
        return result
    monkeypatch.setattr(streamer,'_write_subscription_segment_receipt',receipt)
    requested=streamer._maybe_promote_deferred_subscription_stage()
    assert requested is (failure=='outcome_write')
    assert len(calls)==(1 if failure in {'subscribe','outcome_write'} else 0)
    assert not streamer._maybe_promote_deferred_subscription_stage()
    assert len(calls)==(1 if failure in {'subscribe','outcome_write'} else 0)
    for path in tmp_path.glob('*.json'):
        assert path.stem==hashlib.sha256(path.read_bytes()).hexdigest()
        evidence=json.loads(path.read_text())
        assert evidence['prediction_authority'] is False
        assert evidence['historical_opening_coverage_modified'] is False
    if failure=='subscribe':assert streamer.subscription_stage_segment_evidence['request_state']=='request_failed_delivery_unconfirmed'
    if failure=='outcome_write':assert streamer.subscription_stage_state=='full_active_integrity_warning'


def test_segment_receipt_is_idempotent_and_refuses_conflicting_bytes(tmp_path):
    from backend.subscription_segment import write_segment_receipt
    evidence={'request_state':'intent_only'}
    first=write_segment_receipt(tmp_path,evidence)
    assert write_segment_receipt(tmp_path,evidence)==first
    path=tmp_path/(first['sha256']+'.json')
    path.write_text('conflict')
    with pytest.raises(ValueError,match='conflict'):write_segment_receipt(tmp_path,evidence)


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
    monkeypatch.setattr(streamer, "_processing_clock_telemetry", lambda: {"status": "synchronized"})
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
            "subscription_epoch_id": streamer.subscription_epoch_id,
            "subscription_generation": streamer.active_generation,
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


def test_promotion_requires_clean_transport_but_preserves_partial_opening_evidence(
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
    assert streamer._evaluate_subscription_stage_promotion(
        observed_monotonic=160.0,
        observed_at_utc=later_utc,
        orb_evidence=missing_rut,
    )
    assert "ORB_60M_NOT_EXACT_COMPLETE:RUT" in streamer.subscription_stage_metrics["opening_capture_limitations"]
    assert streamer.subscription_stage_orb_evidence["RUT"]["capture_status"] == "partial"

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


def _queue_recovery_window(monkeypatch, tmp_path):
    from backend.subscription_segment import write_segment_receipt
    streamer, now = _ready_evaluation(monkeypatch)
    monkeypatch.setattr(streamer, "_write_subscription_segment_receipt",
                        lambda evidence: write_segment_receipt(tmp_path, evidence))
    streamer.record_provider_warning("record queue is full with 600 pending records")
    assert not streamer._evaluate_subscription_stage_promotion(
        observed_monotonic=160, observed_at_utc=now, orb_evidence={})
    def evaluate(elapsed):
        tick = now + timedelta(seconds=elapsed)
        _prime_clean_family_metrics(streamer, now_utc=tick, now_monotonic=160 + elapsed)
        streamer.messages_received = 20 + elapsed
        return streamer._evaluate_subscription_stage_promotion(
            observed_monotonic=160 + elapsed, observed_at_utc=tick, orb_evidence={})
    assert not evaluate(1)
    return streamer, evaluate


def test_queue_only_freeze_recovers_once_after_existing_clean_window(monkeypatch, tmp_path):
    streamer, evaluate = _queue_recovery_window(monkeypatch, tmp_path)
    assert not evaluate(59)
    assert evaluate(62)
    assert streamer.subscription_stage_state == "primary_active"
    receipt = streamer.subscription_stage_queue_recovery
    assert receipt["baseline_counters"]["queue_full"] == 0
    assert receipt["observed_counters"]["queue_full"] == 1
    assert receipt["prediction_authority"] is False
    assert receipt["historical_opening_coverage_modified"] is False
    assert len(list(tmp_path.glob("*.json"))) == 1
    streamer.record_provider_warning("record queue is full with 600 pending records")
    assert not evaluate(130)
    assert streamer.subscription_stage_state == "frozen"
    assert len(list(tmp_path.glob("*.json"))) == 1


@pytest.mark.parametrize("failure", ["reconnect", "clock", "epoch", "generation", "loss", "receipt"])
def test_queue_recovery_cannot_bypass_guards(monkeypatch, tmp_path, failure):
    streamer, evaluate = _queue_recovery_window(monkeypatch, tmp_path)
    if failure == "reconnect": streamer.reconnect_attempts += 1
    if failure == "clock": monkeypatch.setattr(streamer, "_processing_clock_telemetry", lambda: {"status": "unknown"})
    if failure == "epoch": streamer.subscription_epoch_id = "c" * 64
    if failure == "generation": streamer.active_generation += 1
    if failure == "loss": streamer.record_provider_warning("slow client detected; skipped 12 records")
    if failure == "receipt":
        def forbidden(_evidence): raise OSError("disk full")
        monkeypatch.setattr(streamer, "_write_subscription_segment_receipt", forbidden)
    assert not evaluate(62)
    assert not streamer.subscription_stage_queue_recovery
    assert streamer.subscription_stage_state in {"frozen", "canceled"}


def test_new_queue_pressure_restarts_the_clean_window(monkeypatch, tmp_path):
    streamer, evaluate = _queue_recovery_window(monkeypatch, tmp_path)
    streamer.record_provider_warning("record queue is full with 600 pending records")
    assert not evaluate(62)
    assert not evaluate(63)
    assert not evaluate(120)
    assert evaluate(124)


def test_warning_during_recovery_receipt_cannot_rearm_promotion(monkeypatch, tmp_path):
    streamer, evaluate = _queue_recovery_window(monkeypatch, tmp_path)
    original = streamer._write_subscription_segment_receipt
    def racing_warning(evidence):
        receipt = original(evidence)
        streamer.record_provider_warning("record queue is full with 600 pending records")
        return receipt
    monkeypatch.setattr(streamer, "_write_subscription_segment_receipt", racing_warning)
    assert not evaluate(62)
    assert not streamer.subscription_stage_queue_recovery
    assert streamer.subscription_stage_state == "frozen"


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


def test_core_pair_budget_breach_overrides_all_index_orb_canary_protection(
    monkeypatch,
):
    monkeypatch.setenv("DATABENTO_RUT_CANARY_ENABLED", "1")
    streamer = _build_staged_streamer(monkeypatch)
    streamer._prepare_live_subscription_stages()
    streamer._activate_initial_subscription_stage(2)
    streamer.handoff_status = "active"
    stopped = []
    streamer.client = SimpleNamespace(stop=lambda: stopped.append(True))

    def observation(*, pair_coverage=1.0):
        return {
            "validation_is_valid": True,
            "data_age_seconds": 1.0,
            "receive_to_process_lag_p95_seconds": 0.1,
            "primary_pair_coverage_ratio": pair_coverage,
            "fresh_quote_count": 10,
            "expected_quote_count": 10,
        }

    monkeypatch.setattr(
        streamer,
        "_optional_family_canary_observations",
        lambda: {
            "SPX": observation(),
            "NDX": observation(pair_coverage=0.05),
            "RUT": observation(),
        },
    )

    decision = streamer._optional_family_canary_decision(
        {
            "state": "regular_session",
            "observed_at_utc": "2026-09-09T13:31:00+00:00",
            "cash_open_utc": "2026-09-09T13:30:00+00:00",
        }
    )

    assert streamer.subscription_stage_state == "primary_active"
    assert decision.state == "rolled_back"
    assert decision.rollback_required is True
    assert "CORE_PRIMARY_PAIR_COVERAGE:NDX" in decision.reasons
    assert streamer.optional_family_canary_evaluation_evidence[
        "opening_orb_protection_overridden_by_core_degradation"
    ] is True
    assert streamer._apply_optional_family_canary_rollback(decision) is True
    assert streamer.symbols == ["SPX", "NDX", "VIX"]
    assert all(
        streamer._market_for_raw_symbol(symbol) != "RUT"
        for symbol in streamer.live_symbols
    )
    assert stopped == [True]


def test_rut_only_thinness_remains_deferred_during_all_index_orb_stage(
    monkeypatch,
):
    monkeypatch.setenv("DATABENTO_RUT_CANARY_ENABLED", "1")
    monkeypatch.setenv("DATABENTO_RUT_CANARY_WARMUP_SECONDS", "0")
    streamer = _build_staged_streamer(monkeypatch)
    initial_symbols = streamer._prepare_live_subscription_stages()
    streamer._activate_initial_subscription_stage(2)
    streamer.handoff_status = "active"
    stopped = []
    streamer.client = SimpleNamespace(stop=lambda: stopped.append(True))

    healthy_core = {
        family: {
            "validation_is_valid": True,
            "data_age_seconds": 1.0,
            "receive_to_process_lag_p95_seconds": 0.1,
            "primary_pair_coverage_ratio": 1.0,
            "fresh_quote_count": 10,
            "expected_quote_count": 10,
        }
        for family in ("SPX", "NDX")
    }
    monkeypatch.setattr(
        streamer,
        "_optional_family_canary_observations",
        lambda: healthy_core,
    )

    decision = streamer._optional_family_canary_decision(
        {
            "state": "regular_session",
            "observed_at_utc": "2026-09-09T13:31:00+00:00",
            "cash_open_utc": "2026-09-09T13:30:00+00:00",
        }
    )

    assert decision.state == "protected_all_index_orb"
    assert decision.rollback_required is False
    assert decision.reasons == ("CANARY_MISSING:RUT",)
    assert streamer._apply_optional_family_canary_rollback(decision) is False
    assert streamer.symbols == list(FAMILIES)
    assert streamer.active_live_symbols == initial_symbols
    assert stopped == []


def test_additive_promotion_accepts_none_id_and_preserves_epoch_generation(
    monkeypatch, tmp_path,
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
        streamer.subscription_stage_last_evaluated_utc = datetime.now(timezone.utc)
        return True

    monkeypatch.setattr(streamer, "_evaluate_subscription_stage_promotion", eligible)
    monkeypatch.setattr("backend.databento_streamer.live_subscription_window", lambda _now=None: {"state": "regular_session"})
    monkeypatch.setattr("backend.databento_streamer.DATA_DIR", tmp_path)

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
    assert not streamer._maybe_promote_deferred_subscription_stage()
    assert len(calls) == 1
    assert len(list((tmp_path / "subscription_segments").glob("*.json"))) == 2
    assert streamer._subscription_stage_health()["capture_segment"]["prediction_authority"] is False
    context = streamer._subscription_segment_context()
    assert context['outcome_receipt']['sha256']
    assert context['request_state']=='sent_unverified_quote_coverage'
    assert context['historical_opening_coverage_modified'] is False
    streamer.active_generation+=1
    assert streamer._subscription_segment_context()=={}


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
        "requires_all_60m_orbs_exact_complete": False,
        "requires_opening_hour_elapsed": True,
        "requires_current_session_universe": True,
        "requires_synchronized_clock": True,
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
