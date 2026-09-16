from __future__ import annotations

import hashlib
import json
import math
import time
from datetime import date, datetime, timedelta, timezone

import pandas as pd
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, event, text
from sqlalchemy.exc import DatabaseError
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

import backend.database as database
from backend.api.routers import orb as orb_router
from backend.databento_streamer import (
    DatabentoGammaStreamer,
    _configured_risk_free_rate,
    raw_option_symbol,
    years_to_expiration,
)
from backend.market_structure import MarketStructureJournal


TRADING_DATE = date(2026, 9, 8)
OPEN_UTC = datetime(2026, 9, 8, 13, 30, tzinfo=timezone.utc)
PRIMARY_EXPIRATIONS = {
    "SPX": TRADING_DATE,
    "NDX": TRADING_DATE,
    "VIX": date(2026, 9, 9),
    # The optional RUT family remains useful as an ORB context even when its
    # selected primary series is the next listed expiry rather than same-day.
    "RUT": date(2026, 9, 9),
}
MARKET_SPECS = {
    "SPX": ("SPXW", 6500.0),
    "NDX": ("NDXP", 29500.0),
    "VIX": ("VIXW", 20.0),
    "RUT": ("RUTW", 2200.0),
}


@pytest.fixture
def persistent_orb_journal(monkeypatch):
    """Use a real isolated SQLite store; never touch the runtime database."""

    temp_engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    temp_session = sessionmaker(
        autocommit=False,
        autoflush=False,
        bind=temp_engine,
    )
    monkeypatch.setattr(database, "engine", temp_engine)
    monkeypatch.setattr(database, "SessionLocal", temp_session)
    monkeypatch.setattr(database, "DATABASE_URL", "sqlite://")
    database.Base.metadata.create_all(bind=temp_engine)
    database._ensure_market_structure_schema()
    database._ensure_market_structure_immutability()
    database._ensure_orb_reference_schema()
    database._ensure_orb_reference_immutability()
    database._ensure_orb_reference_decision_schema()
    database._ensure_orb_reference_decision_immutability()

    def _stamp_simulated_structure_capture_time(_mapper, _connection, target):
        # Structure rows in this rehearsal are immutable, so establish their
        # simulated knowledge time before INSERT.  This remains deterministic
        # even when an earlier test has populated SQLAlchemy's INSERT cache.
        target.captured_at_utc = target.source_timestamp_utc

    event.listen(
        database.MarketStructureObservation,
        "before_insert",
        _stamp_simulated_structure_capture_time,
    )

    clock = {"now": OPEN_UTC}
    journal = MarketStructureJournal(
        saver=database.save_market_structure_observation,
        loader=database.load_market_structure_observations,
        reference_saver=database.save_orb_reference_sample,
        reference_decision_saver=database.save_orb_reference_sample_decision,
        reference_loader=database.load_orb_reference_samples,
        now_utc=lambda: clock["now"],
        expected_cadence_seconds=5.0,
    )
    try:
        yield journal, clock, temp_engine
    finally:
        event.remove(
            database.MarketStructureObservation,
            "before_insert",
            _stamp_simulated_structure_capture_time,
        )
        temp_engine.dispose()


def _build_streamer(monkeypatch, *, epoch: str) -> DatabentoGammaStreamer:
    monkeypatch.setenv("DATABENTO_RUT_CANARY_ENABLED", "1")
    monkeypatch.setenv("DATABENTO_RUT_CANARY_WARMUP_SECONDS", "0")
    streamer = DatabentoGammaStreamer(
        ["SPX", "NDX", "VIX", "RUT"],
        subscription_epoch_id=epoch,
    )
    generation = 7
    rows: list[dict[str, object]] = []
    mapping_versions: dict[str, str] = {}
    instrument_id = 10_000
    for market, (root, center) in MARKET_SPECS.items():
        expiration = PRIMARY_EXPIRATIONS[market]
        for offset in range(-2, 3):
            strike = center + float(offset)
            for option_type in ("C", "P"):
                symbol = raw_option_symbol(root, expiration, option_type, strike)
                rows.append(
                    {
                        "market": market,
                        "symbol": symbol,
                        "strike": strike,
                        "option_type": option_type,
                        "expiration_date": expiration,
                        "open_interest": 1_000.0,
                    }
                )
                mapping_version = hashlib.sha256(symbol.encode("utf-8")).hexdigest()
                mapping_versions[symbol] = mapping_version
                streamer.symbol_mappings[instrument_id] = {
                    "raw_symbol": symbol,
                    "mapping_version": mapping_version,
                }
                instrument_id += 1

    streamer.universe = pd.DataFrame(rows)
    streamer.full_universe = streamer.universe.copy()
    streamer._index_universe_metadata()
    streamer.active_generation = generation
    streamer.subscription_cutoff_monotonic = time.monotonic() - 60.0
    streamer.handoff_status = "active"
    streamer.subscription_metadata = {
        "selected_universe_sha256": "a" * 64,
        "universe_provenance": {
            "trading_date": TRADING_DATE.isoformat(),
            "is_fallback": False,
        },
        "markets": {
            market: {
                "selected_contract_count": 10,
                "selected_expirations": [
                    {
                        "expiration": PRIMARY_EXPIRATIONS[market].isoformat(),
                        "role": "primary",
                        "stage": 0,
                        "selected_strike_pairs": 5,
                    }
                ],
            }
            for market in MARKET_SPECS
        },
    }
    streamer._rehearsal_mapping_versions = mapping_versions
    monkeypatch.setattr(
        streamer,
        "_processing_clock_telemetry",
        lambda: {"status": "synchronized"},
    )
    return streamer


def _refresh_market_quotes(
    streamer: DatabentoGammaStreamer,
    market: str,
    observed_at: datetime,
    reference_price: float,
) -> None:
    expiration = PRIMARY_EXPIRATIONS[market]
    risk_free_rate = _configured_risk_free_rate()
    years = years_to_expiration(expiration, now=observed_at)
    discount = math.exp(-risk_free_rate * years)
    source_time = observed_at - timedelta(milliseconds=50)
    source_ns = int(source_time.timestamp() * 1_000_000_000)
    received_monotonic = time.monotonic()
    mapping_versions = streamer._rehearsal_mapping_versions

    for record in streamer._universe_records_by_market[market]:
        symbol = str(record["symbol"])
        strike = float(record["strike"])
        put_mid = 100.0
        mid = (
            reference_price - strike * discount + put_mid
            if record["option_type"] == "C"
            else put_mid
        )
        assert mid > 0.1
        streamer.quotes[symbol] = {
            "bid": mid - 0.05,
            "ask": mid + 0.05,
            "mid": mid,
            "received_monotonic": received_monotonic,
            "generation": streamer.active_generation,
            "ts_event_ns": source_ns - 1_000_000,
            "ts_recv_ns": source_ns,
            "ts_index_ns": source_ns + 1,
            "provider_timestamp_order_valid": True,
            "mapping_version": mapping_versions[symbol],
        }


def _capture(
    streamer: DatabentoGammaStreamer,
    journal: MarketStructureJournal,
    market: str,
    observed_at: datetime,
    reference_price: float,
) -> dict[str, object]:
    _refresh_market_quotes(streamer, market, observed_at, reference_price)
    return dict(
        streamer._capture_opening_reference_once(
            market,
            observed_at_utc=observed_at,
            journal=journal,
        )
    )


def _api_client(monkeypatch, journal, active_streamer) -> TestClient:
    class RehearsalDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            observed = journal.now_utc()
            return observed.astimezone(tz) if tz is not None else observed.replace(tzinfo=None)

    # The concurrent projection now binds one timestamp in the router. Keep
    # that clock on the same simulated session as its temporary journal.
    monkeypatch.setattr(orb_router, "datetime", RehearsalDateTime)
    monkeypatch.setattr(orb_router, "get_market_structure_journal", lambda: journal)
    monkeypatch.setattr(orb_router, "get_streamer", lambda: active_streamer["value"])
    api = FastAPI()
    api.include_router(orb_router.router)
    return TestClient(api, client=("127.0.0.1", 50_000))


def _structure_payload(
    streamer: DatabentoGammaStreamer,
    market: str,
    observed_at: datetime,
    price: float,
) -> dict[str, object]:
    return {
        "symbol": market,
        "provider": "databento",
        "validation_is_valid": True,
        "gamma_excluded_from_model": False,
        "subscription_epoch_id": streamer.subscription_epoch_id,
        "subscription_generation": streamer.active_generation,
        "latest_ts_recv_utc": (observed_at - timedelta(milliseconds=50)).isoformat(),
        "price": price,
        "gamma_pin": price + 10.0,
        "max_pain": price - 10.0,
        "zero_gamma": price - 20.0,
        "pin_lead_ratio": 1.5,
        "pin_is_contested": False,
        "gross_gex": 100.0,
        "net_gex": 20.0,
        "primary_expiration": PRIMARY_EXPIRATIONS[market].isoformat(),
        "same_day_profile_available": PRIMARY_EXPIRATIONS[market] == TRADING_DATE,
        "universe_sha256": "a" * 64,
    }


def test_four_index_sampler_to_sqlite_to_live_api_rehearsal(
    persistent_orb_journal,
    monkeypatch,
):
    journal, clock, temp_engine = persistent_orb_journal
    streamer = _build_streamer(monkeypatch, epoch="1" * 64)
    active_streamer = {"value": streamer}
    client = _api_client(monkeypatch, journal, active_streamer)

    for sample_index in range(720):
        observed_at = OPEN_UTC + timedelta(
            seconds=sample_index * 5,
            milliseconds=100,
        )
        for market, (_root, center) in MARKET_SPECS.items():
            scale = 0.01 if market == "VIX" else 0.1
            price = center + ((sample_index % 20) - 10) * scale
            result = _capture(streamer, journal, market, observed_at, price)
            assert result["recorded"] is True
            assert result["progress_eligible"] is True
            assert result["subscription_epoch_id"] == "1" * 64

    current_at = OPEN_UTC + timedelta(hours=1, seconds=5, milliseconds=100)
    current_prices = {
        "SPX": 6505.0,
        "NDX": 29505.0,
        "VIX": 21.0,
        "RUT": 2205.0,
    }
    for market, price in current_prices.items():
        assert _capture(streamer, journal, market, current_at, price)["recorded"]

    clock["now"] = current_at + timedelta(milliseconds=100)
    response = client.get("/v1/orb")
    assert response.status_code == 200
    payload = response.json()
    assert payload["schema_version"] == "marketpin-reference-orb.collection.v2"
    assert payload["configured_symbols"] == ["SPX", "NDX", "VIX", "RUT"]
    assert payload["requested_symbols"] == ["SPX", "NDX", "VIX", "RUT"]
    assert payload["runtime_binding_applied"] is True
    assert payload["runtime_context_stable"] is True
    assert payload["active_runtime_context"] == {
        "subscription_epoch_id": "1" * 64,
        "subscription_generation": 7,
        "handoff_status": "active",
    }
    assert set(payload["symbols"]) == {"SPX", "NDX", "VIX", "RUT"}

    for market, state in payload["symbols"].items():
        assert state["schema_version"] == "marketpin-reference-orb.v2"
        assert set(state["opening_ranges"]) == {"5m", "15m", "30m", "60m"}
        assert {
            window: state["opening_ranges"][window]["capture_evidence"][
                "sample_count"
            ]
            for window in ("5m", "15m", "30m", "60m")
        } == {"5m": 60, "15m": 180, "30m": 360, "60m": 720}
        assert all(
            window["capture_status"] == "complete"
            for window in state["opening_ranges"].values()
        )
        assert state["pin_behavior"]["gamma_pin"] is None
        assert state["combined_structure_directional_evidence_eligible"] is False
        assert state["provenance"]["runtime_binding_applied"] is True
        assert state["provenance"]["active_runtime_epoch_aligned"] is True
        assert state["last_known_reference"]["runtime_aligned"] is True
        assert len(database.load_orb_reference_samples(market, TRADING_DATE)) == 721

    assert payload["symbols"]["SPX"]["directional_evidence_eligible"] is True
    assert payload["symbols"]["NDX"]["directional_evidence_eligible"] is True
    vix = payload["symbols"]["VIX"]
    assert vix["breakout_direction"] == "bullish"
    assert vix["directional_evidence_eligible"] is False
    assert vix["reference_semantics"]["kind"] == "vix_option_forward_context"
    assert "VIX_OPTION_PARITY_FORWARD_CONTEXT_NOT_SPOT" in vix["warnings"]
    vix_formula_inputs = database.load_orb_reference_samples(
        "VIX", TRADING_DATE
    )[-1]["formula_inputs_json"]
    if isinstance(vix_formula_inputs, str):
        vix_formula_inputs = json.loads(vix_formula_inputs)
    assert vix_formula_inputs["primary_expiration_authority"] == (
        "vix_forward_expiration_context_only"
    )
    assert vix_formula_inputs["primary_expiration_context_only"] is True
    assert vix_formula_inputs["same_day_authority"] is False
    rut = payload["symbols"]["RUT"]
    assert rut["breakout_direction"] == "bullish"
    assert rut["directional_evidence_eligible"] is False
    assert rut["reference_semantics"]["kind"] == (
        "non_same_day_index_option_forward_context"
    )
    assert "NON_SAME_DAY_PRIMARY_EXPIRATION_CONTEXT_ONLY" in rut["warnings"]

    for market, price in current_prices.items():
        assert journal.record(
            _structure_payload(streamer, market, current_at, price)
        )["recorded"] is True

    aligned = client.get("/v1/orb").json()["symbols"]
    for market in ("SPX", "NDX"):
        assert aligned[market]["pin_behavior"]["gamma_pin"] == pytest.approx(
            current_prices[market] + 10.0
        )
        assert aligned[market][
            "combined_structure_directional_evidence_eligible"
        ] is True
        assert aligned[market]["provenance"]["structure_reference_status"] == (
            "aligned"
        )
    assert aligned["VIX"]["combined_structure_directional_evidence_eligible"] is False
    assert aligned["RUT"]["pin_behavior"]["gamma_pin"] == pytest.approx(
        current_prices["RUT"] + 10.0
    )
    assert aligned["RUT"]["combined_structure_directional_evidence_eligible"] is False

    # Missing optional RUT evidence must not resubscribe during the cash
    # session and invalidate the complete, same-generation SPX/NDX ranges.
    monkeypatch.setattr(
        streamer,
        "_optional_family_canary_observations",
        lambda: {
            market: {
                "validation_is_valid": True,
                "data_age_seconds": 1.0,
                "receive_to_process_lag_p95_seconds": 0.1,
                "primary_pair_coverage_ratio": 1.0,
                "fresh_quote_count": 10,
                "expected_quote_count": 10,
            }
            for market in ("SPX", "NDX")
        },
    )
    generation_before = streamer.active_generation
    protected = streamer._optional_family_canary_decision(
        {
            "state": "regular_session",
            "observed_at_utc": current_at.isoformat(),
            "cash_open_utc": OPEN_UTC.isoformat(),
        }
    )
    assert protected.state == "protected_cash_session"
    assert protected.rollback_required is False
    assert "CANARY_MISSING:RUT" in protected.reasons
    assert streamer._rut_orb_reference_progress(current_at)["advancing"] is False
    assert streamer._apply_optional_family_canary_rollback(protected) is False
    assert streamer.active_generation == generation_before
    still_complete = client.get("/v1/orb").json()["symbols"]
    assert all(
        window["capture_status"] == "complete"
        for market in ("SPX", "NDX")
        for window in still_complete[market]["opening_ranges"].values()
    )
    for market in ("SPX", "NDX"):
        assert still_complete[market]["directional_evidence_eligible"] is True
        assert still_complete[market][
            "combined_structure_directional_evidence_eligible"
        ] is True
        assert still_complete[market]["provenance"][
            "current_vs_range_aligned"
        ] is True

    # A restarted backend may reuse generation 7, but a new epoch must suppress
    # live current/directional claims before its first new sample arrives.
    restarted = _build_streamer(monkeypatch, epoch="2" * 64)
    restarted._orb_reference_recent_buckets_by_market["RUT"].extend(
        [
            (OPEN_UTC + timedelta(minutes=2, seconds=55), 7, "1" * 64),
            (OPEN_UTC + timedelta(minutes=3), 7, "1" * 64),
        ]
    )
    prior_epoch_progress = restarted._rut_orb_reference_progress(
        OPEN_UTC + timedelta(minutes=3)
    )
    assert prior_epoch_progress["advancing"] is False
    assert prior_epoch_progress["current_generation_bucket_count"] == 0
    active_streamer["value"] = restarted
    just_restarted = client.get("/v1/orb").json()["symbols"]
    for state in just_restarted.values():
        assert state["current_price"] is None
        assert state["current_reference_fresh"] is False
        assert state["directional_evidence_eligible"] is False
        assert state["combined_structure_directional_evidence_eligible"] is False
        assert state["pin_behavior"]["gamma_pin"] is None
        assert state["provenance"]["active_runtime_epoch_aligned"] is False
        assert state["last_known_reference"]["reference_price"] is not None
        assert state["last_known_reference"]["subscription_epoch_id"] == "1" * 64
        assert state["last_known_reference"]["runtime_aligned"] is False

    next_sample = current_at + timedelta(seconds=5)
    assert _capture(
        restarted,
        journal,
        "NDX",
        next_sample,
        current_prices["NDX"] + 1.0,
    )["recorded"] is True
    clock["now"] = next_sample + timedelta(milliseconds=100)
    restarted_ndx = client.get("/v1/orb/NDX").json()
    assert restarted_ndx["capture_status"] == "complete"
    assert restarted_ndx["provenance"]["range_provenance_aligned"] is True
    assert restarted_ndx["provenance"]["current_vs_range_aligned"] is False
    assert restarted_ndx["directional_evidence_eligible"] is False
    assert "CURRENT_PROVENANCE_DIFFERS_FROM_ORB" in restarted_ndx["warnings"]

    # Explicit historical replay remains available and is never mislabelled as
    # the current process's evidence.
    historical = client.get(
        "/v1/orb/SPX",
        params={
            "trading_date": TRADING_DATE.isoformat(),
            "as_of_utc": current_at.isoformat(),
        },
    ).json()
    assert historical["directional_evidence_eligible"] is True
    assert historical["provenance"]["runtime_binding_applied"] is False

    clock["now"] = next_sample + timedelta(minutes=1)
    stale = client.get("/v1/orb").json()["symbols"]
    assert all(state["current_reference_fresh"] is False for state in stale.values())
    assert all(state["directional_evidence_eligible"] is False for state in stale.values())

    with temp_engine.connect() as connection:
        assert connection.execute(
            text("SELECT COUNT(*) FROM orb_reference_samples")
        ).scalar_one() == 2_885
    with temp_engine.begin() as connection:
        with pytest.raises(DatabaseError):
            connection.execute(
                text(
                    "UPDATE orb_reference_samples SET reference_price = 1 "
                    "WHERE symbol = 'SPX'"
                )
            )


def test_tuesday_open_boundary_delayed_start_and_gap_fail_closed(
    persistent_orb_journal,
    monkeypatch,
):
    journal, clock, _temp_engine = persistent_orb_journal
    streamer = _build_streamer(monkeypatch, epoch="3" * 64)
    active_streamer = {"value": streamer}
    client = _api_client(monkeypatch, journal, active_streamer)

    before_open = OPEN_UTC - timedelta(milliseconds=100)
    _refresh_market_quotes(streamer, "VIX", before_open, 20.0)
    assert streamer._capture_opening_reference_once(
        "VIX",
        observed_at_utc=before_open,
        journal=journal,
    ) == {"recorded": False, "reason": "OUTSIDE_REGULAR_SESSION"}

    exact_open = OPEN_UTC + timedelta(milliseconds=100)
    opened = _capture(streamer, journal, "VIX", exact_open, 20.0)
    assert opened["recorded"] is True
    assert opened["sample_timestamp_utc"] == "2026-09-08T13:30:00Z"

    for sample_index in range(7, 60):
        observed_at = OPEN_UTC + timedelta(
            seconds=sample_index * 5,
            milliseconds=100,
        )
        assert _capture(
            streamer,
            journal,
            "SPX",
            observed_at,
            6500.0 + sample_index / 100.0,
        )["recorded"] is True

    for sample_index in range(60):
        if 20 <= sample_index <= 26:
            continue
        observed_at = OPEN_UTC + timedelta(
            seconds=sample_index * 5,
            milliseconds=100,
        )
        assert _capture(
            streamer,
            journal,
            "NDX",
            observed_at,
            29500.0 + sample_index / 100.0,
        )["recorded"] is True

    clock["now"] = OPEN_UTC + timedelta(minutes=5, milliseconds=200)
    states = client.get("/v1/orb").json()["symbols"]
    delayed = states["SPX"]["opening_ranges"]["5m"]
    assert delayed["capture_status"] == "partial"
    assert delayed["capture_evidence"]["first_sample_lag_seconds"] == 35.0
    assert delayed["directional_evidence_eligible"] is False

    gapped = states["NDX"]["opening_ranges"]["5m"]
    assert gapped["capture_status"] == "partial"
    assert gapped["capture_evidence"]["max_gap_seconds"] == 40.0
    assert gapped["directional_evidence_eligible"] is False

    close = datetime(2026, 9, 8, 20, 0, tzinfo=timezone.utc)
    _refresh_market_quotes(streamer, "RUT", close, 2200.0)
    assert streamer._capture_opening_reference_once(
        "RUT",
        observed_at_utc=close,
        journal=journal,
    ) == {"recorded": False, "reason": "OUTSIDE_REGULAR_SESSION"}


def test_post_persist_cross_bucket_decision_is_retained_but_excluded_end_to_end(
    persistent_orb_journal,
    monkeypatch,
):
    journal, clock, temp_engine = persistent_orb_journal
    streamer = _build_streamer(monkeypatch, epoch="4" * 64)
    streamer.is_running = True
    observed_at = OPEN_UTC + timedelta(milliseconds=100)
    _refresh_market_quotes(streamer, "SPX", observed_at, 6500.0)
    clock["now"] = observed_at

    original_record = journal.record_reference

    def persist_then_cross_bucket(payload):
        result = original_record(payload)
        clock["now"] = OPEN_UTC + timedelta(seconds=5, milliseconds=100)
        return result

    monkeypatch.setattr(journal, "record_reference", persist_then_cross_bucket)
    result = streamer._capture_opening_reference_for_bucket(
        "SPX",
        OPEN_UTC,
        now_utc=lambda: clock["now"],
        journal=journal,
    )

    assert result["recorded"] is True
    assert result["progress_eligible"] is False
    assert result["progress_decision_recorded"] is True
    assert result["progress_decision_pending"] is False
    assert result["reason"] == "REFERENCE_CAPTURE_COMPLETED_OUTSIDE_INTENDED_BUCKET"
    with temp_engine.connect() as connection:
        assert connection.execute(
            text("SELECT COUNT(*) FROM orb_reference_samples")
        ).scalar_one() == 1
        decision = connection.execute(
            text(
                "SELECT progress_eligible, reason "
                "FROM orb_reference_sample_decisions"
            )
        ).one()
        assert decision.progress_eligible == 0
        assert decision.reason == (
            "REFERENCE_CAPTURE_COMPLETED_OUTSIDE_INTENDED_BUCKET"
        )

    # Both database loaders, the journal projection, and the API omit the raw
    # row because its immutable final decision rejected progress eligibility.
    assert database.load_orb_reference_samples("SPX", TRADING_DATE) == []
    assert database.load_orb_reference_snapshot_samples("SPX", TRADING_DATE) == []
    snapshot = journal.snapshot(
        "SPX",
        trading_date=TRADING_DATE,
        as_of_utc=clock["now"],
        active_subscription_epoch_id=streamer.subscription_epoch_id,
        active_subscription_generation=streamer.active_generation,
        active_handoff_status="active",
    )
    assert snapshot["capture_evidence"]["sample_count"] == 0
    assert snapshot["last_known_reference"]["sample_id"] is None
    assert snapshot["directional_evidence_eligible"] is False

    client = _api_client(monkeypatch, journal, {"value": streamer})
    api_state = client.get("/v1/orb/SPX").json()
    assert api_state["capture_evidence"]["sample_count"] == 0
    assert api_state["last_known_reference"]["sample_id"] is None
    assert api_state["directional_evidence_eligible"] is False
