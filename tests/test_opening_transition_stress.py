from __future__ import annotations

import json
import math
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta, timezone
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pandas as pd
from sqlalchemy import create_engine, event, text
from sqlalchemy.orm import sessionmaker

import backend.database as database
import backend.databento_streamer as streamer_module
from backend.databento_streamer import (
    DatabentoGammaStreamer,
    _configured_risk_free_rate,
    raw_option_symbol,
    years_to_expiration,
)
from backend.market_structure import MarketStructureJournal


EASTERN = ZoneInfo("America/New_York")
TRADING_DATE = date(2026, 9, 8)
OPEN_ET = datetime(2026, 9, 8, 9, 30, tzinfo=EASTERN)
OPEN_UTC = OPEN_ET.astimezone(timezone.utc)
CAPTURE_UTC = OPEN_UTC + timedelta(milliseconds=250)
PRIMARY_EXPIRATIONS = {
    "SPX": TRADING_DATE,
    "NDX": TRADING_DATE,
    "VIX": date(2026, 9, 9),
    "RUT": date(2026, 9, 9),
}
MARKET_SPECS = {
    "SPX": ("SPXW", 6500.0),
    "NDX": ("NDXP", 29500.0),
    "VIX": ("VIXW", 20.0),
    "RUT": ("RUTW", 2200.0),
}
# Retained Sep. 9 opening-stage evidence contained this many primary pairs.
# Exercising the same 1,362 mapping/quote rows prevents a five-pair fixture from
# hiding lock contention that appears only during the real OPRA opening burst.
REPRESENTATIVE_OPENING_PAIR_COUNTS = {
    "SPX": 235,
    "NDX": 300,
    "VIX": 48,
    "RUT": 98,
}


def _temp_wal_journal(tmp_path, monkeypatch):
    db_path = tmp_path / "opening-transition-stress.sqlite3"
    database_url = f"sqlite:///{db_path.as_posix()}"
    temp_engine = create_engine(
        database_url,
        connect_args={"check_same_thread": False, "timeout": 10.0},
        pool_pre_ping=True,
    )

    @event.listens_for(temp_engine, "connect")
    def _configure_connection(dbapi_connection, _connection_record):
        cursor = dbapi_connection.cursor()
        try:
            cursor.execute("PRAGMA busy_timeout=10000")
            cursor.execute("PRAGMA foreign_keys=ON")
            cursor.execute("PRAGMA synchronous=NORMAL")
        finally:
            cursor.close()

    temp_session = sessionmaker(
        autocommit=False,
        autoflush=False,
        bind=temp_engine,
    )
    monkeypatch.setattr(database, "engine", temp_engine)
    monkeypatch.setattr(database, "SessionLocal", temp_session)
    monkeypatch.setattr(database, "DATABASE_URL", database_url)

    with temp_engine.connect() as connection:
        assert connection.exec_driver_sql("PRAGMA journal_mode=WAL").scalar_one().lower() == "wal"
    database.Base.metadata.create_all(bind=temp_engine)
    database._ensure_orb_reference_schema()
    database._ensure_orb_reference_immutability()
    database._ensure_orb_reference_decision_schema()
    database._ensure_orb_reference_decision_immutability()

    persisted_threads: set[int] = set()
    persisted_threads_lock = threading.Lock()

    def _record_reference(sample):
        with persisted_threads_lock:
            persisted_threads.add(threading.get_ident())
        return database.save_orb_reference_sample(sample)

    journal = MarketStructureJournal(
        saver=database.save_market_structure_observation,
        loader=database.load_market_structure_observations,
        reference_saver=_record_reference,
        reference_decision_saver=database.save_orb_reference_sample_decision,
        reference_loader=database.load_orb_reference_samples,
        now_utc=lambda: CAPTURE_UTC,
        expected_cadence_seconds=5.0,
    )
    return journal, temp_engine, db_path, persisted_threads


def _build_opening_streamer(monkeypatch) -> DatabentoGammaStreamer:
    monkeypatch.setattr(
        streamer_module,
        "DATABENTO_REQUIRED_SYMBOLS",
        ("SPX", "NDX"),
    )
    streamer = DatabentoGammaStreamer(
        ["SPX", "NDX", "VIX", "RUT"],
        subscription_epoch_id="1" * 64,
    )
    rows: list[dict[str, object]] = []
    for market, (root, center) in MARKET_SPECS.items():
        pair_count = max(
            int(streamer_module.MIN_PAIRED_QUOTES),
            REPRESENTATIVE_OPENING_PAIR_COUNTS[market],
        )
        expiration = PRIMARY_EXPIRATIONS[market]
        start_offset = -(pair_count // 2)
        strike_step = 0.5 if market == "VIX" else 1.0
        for pair_index in range(pair_count):
            strike = center + float(start_offset + pair_index) * strike_step
            for option_type in ("C", "P"):
                rows.append(
                    {
                        "market": market,
                        "symbol": raw_option_symbol(
                            root,
                            expiration,
                            option_type,
                            strike,
                        ),
                        "strike": strike,
                        "option_type": option_type,
                        "expiration_date": expiration,
                        "open_interest": 1_000.0,
                    }
                )

    streamer.universe = pd.DataFrame(rows)
    streamer.full_universe = streamer.universe.copy()
    streamer._index_universe_metadata()
    streamer.active_generation = 7
    streamer.subscription_cutoff_monotonic = time.monotonic() - 1.0
    streamer.handoff_status = "warming"
    streamer.handoff_reason = None
    streamer.is_running = True
    streamer.subscription_metadata = {
        "selected_universe_sha256": "a" * 64,
        "universe_provenance": {
            "trading_date": TRADING_DATE.isoformat(),
            "is_fallback": False,
        },
        "markets": {
            market: {
                "selected_contract_count": (
                    max(
                        int(streamer_module.MIN_PAIRED_QUOTES),
                        REPRESENTATIVE_OPENING_PAIR_COUNTS[market],
                    )
                    * 2
                ),
                "selected_expirations": [
                    {
                        "expiration": PRIMARY_EXPIRATIONS[market].isoformat(),
                        "role": "primary",
                        "stage": 0,
                        "selected_strike_pairs": max(
                            int(streamer_module.MIN_PAIRED_QUOTES),
                            REPRESENTATIVE_OPENING_PAIR_COUNTS[market],
                        ),
                    }
                ],
            }
            for market in MARKET_SPECS
        },
    }
    monkeypatch.setattr(
        streamer,
        "_processing_clock_telemetry",
        lambda: {"status": "synchronized"},
    )
    return streamer


def _publish_mappings_and_quotes(
    streamer: DatabentoGammaStreamer,
    market: str,
    *,
    first_instrument_id: int,
) -> float:
    source_at = OPEN_UTC + timedelta(milliseconds=100)
    source_ns = int(source_at.timestamp() * 1_000_000_000)
    received_monotonic = time.monotonic()
    expiration = PRIMARY_EXPIRATIONS[market]
    years = years_to_expiration(expiration, now=CAPTURE_UTC)
    discount = math.exp(-_configured_risk_free_rate() * years)
    reference_price = MARKET_SPECS[market][1]

    for offset, record in enumerate(streamer._universe_records_by_market[market]):
        instrument_id = first_instrument_id + offset
        symbol = str(record["symbol"])
        mapping_message = SimpleNamespace(
            instrument_id=instrument_id,
            stype_in_symbol=symbol,
            stype_out_symbol=symbol,
            ts_event=source_ns - 2_000_000,
            ts_index=source_ns - 1_000_000,
            start_ts=source_ns - 10_000_000,
            end_ts=source_ns + 86_400_000_000_000,
        )
        mapping = streamer._mapping_record(mapping_message, symbol)
        put_mid = 1_000.0
        strike = float(record["strike"])
        mid = (
            reference_price - strike * discount + put_mid
            if record["option_type"] == "C"
            else put_mid
        )
        assert mid > 0.1
        quote = {
            "bid": mid - 0.05,
            "ask": mid + 0.05,
            "mid": mid,
            "received_monotonic": received_monotonic,
            "generation": streamer.active_generation,
            "instrument_id": instrument_id,
            "ts_event_ns": source_ns - 1_000_000,
            "ts_recv_ns": source_ns,
            "ts_index_ns": source_ns + 1,
            "provider_timestamp_order_valid": True,
            "mapping_version": mapping["mapping_version"],
        }
        with streamer._fresh_quote_lock:
            streamer.id_to_symbol[instrument_id] = symbol
            streamer.symbol_mappings[instrument_id] = mapping
            streamer._store_quote(symbol, quote)
    return received_monotonic


def test_exact_open_four_family_handoff_and_concurrent_temp_sqlite_stress(
    tmp_path,
    monkeypatch,
):
    assert OPEN_ET.isoformat() == "2026-09-08T09:30:00-04:00"
    assert OPEN_UTC.isoformat() == "2026-09-08T13:30:00+00:00"
    journal, temp_engine, db_path, persisted_threads = _temp_wal_journal(
        tmp_path,
        monkeypatch,
    )
    streamer = _build_opening_streamer(monkeypatch)

    scenario_started = time.perf_counter()
    arrivals: dict[str, float] = {}
    next_instrument_id = 10_000
    for market in ("VIX", "RUT", "SPX"):
        arrivals[market] = _publish_mappings_and_quotes(
            streamer,
            market,
            first_instrument_id=next_instrument_id,
        )
        next_instrument_id += len(streamer._universe_records_by_market[market])
        assert (
            streamer._record_fresh_market_and_maybe_activate(
                market,
                generation=streamer.active_generation,
                received_monotonic=arrivals[market],
            )
            is False
        )
        assert streamer.handoff_status == "warming"

    warming_rejection = streamer._capture_opening_reference_for_bucket(
        "SPX",
        OPEN_UTC,
        now_utc=lambda: CAPTURE_UTC,
        journal=journal,
    )
    assert warming_rejection["recorded"] is False
    assert warming_rejection["reason"] == "HANDOFF_NOT_ACTIVE"

    arrivals["NDX"] = _publish_mappings_and_quotes(
        streamer,
        "NDX",
        first_instrument_id=next_instrument_id,
    )
    assert streamer._record_fresh_market_and_maybe_activate(
        "NDX",
        generation=streamer.active_generation,
        received_monotonic=arrivals["NDX"],
    ) is True
    assert streamer.handoff_status == "active"
    assert streamer.handoff_reason is None
    assert len(streamer.symbol_mappings) == len(streamer.universe)
    assert len(streamer.quotes) == len(streamer.universe)

    original_compute = streamer._compute_opening_reference_payload
    compute_barrier = threading.Barrier(len(MARKET_SPECS))
    compute_entries: list[tuple[str, int, float]] = []
    compute_entries_lock = threading.Lock()

    def _concurrent_compute(snapshot, **kwargs):
        with compute_entries_lock:
            compute_entries.append(
                (
                    str(snapshot["market"]),
                    threading.get_ident(),
                    time.perf_counter(),
                )
            )
        compute_barrier.wait(timeout=2.0)
        return original_compute(snapshot, **kwargs)

    monkeypatch.setattr(
        streamer,
        "_compute_opening_reference_payload",
        _concurrent_compute,
    )
    capture_started = time.perf_counter()
    with ThreadPoolExecutor(
        max_workers=len(MARKET_SPECS),
        thread_name_prefix="opening-transition-stress",
    ) as executor:
        futures = {
            market: executor.submit(
                streamer._capture_opening_reference_for_bucket,
                market,
                OPEN_UTC,
                now_utc=lambda: CAPTURE_UTC,
                journal=journal,
            )
            for market in MARKET_SPECS
        }
        results = {market: future.result(timeout=5.0) for market, future in futures.items()}
    capture_elapsed = time.perf_counter() - capture_started

    assert compute_barrier.n_waiting == 0
    assert {entry[0] for entry in compute_entries} == set(MARKET_SPECS)
    assert len({entry[1] for entry in compute_entries}) == len(MARKET_SPECS)
    assert capture_elapsed < 5.0
    for market, result in results.items():
        assert result["recorded"] is True, (market, result)
        assert result["progress_eligible"] is True, (market, result)
        assert result["progress_decision_recorded"] is True, (market, result)
        assert result["sample_timestamp_utc"] == "2026-09-08T13:30:00Z"
        assert result["intended_sample_timestamp_utc"] == OPEN_UTC.isoformat()
        assert result["subscription_epoch_id"] == "1" * 64
        assert result["subscription_generation"] == 7
        assert result["persist_lock_wait_seconds"] >= 0.0
        assert result["persist_lock_held_seconds"] >= 0.0

    expired_deadline = streamer._capture_opening_reference_for_bucket(
        "SPX",
        OPEN_UTC,
        now_utc=lambda: CAPTURE_UTC,
        journal=journal,
        attempt_deadline_utc=OPEN_UTC + timedelta(seconds=4),
        attempt_deadline_monotonic=time.monotonic() - 0.001,
    )
    assert expired_deadline["recorded"] is False
    assert expired_deadline["reason"] == "REFERENCE_ATTEMPT_DEADLINE_EXCEEDED"

    cross_bucket = streamer._capture_opening_reference_for_bucket(
        "NDX",
        OPEN_UTC,
        now_utc=lambda: OPEN_UTC + timedelta(seconds=5, milliseconds=1),
        journal=journal,
        attempt_deadline_utc=OPEN_UTC + timedelta(seconds=10),
        attempt_deadline_monotonic=time.monotonic() + 5.0,
    )
    assert cross_bucket["recorded"] is False
    assert cross_bucket["reason"] == "REFERENCE_CAPTURE_STARTED_OUTSIDE_INTENDED_BUCKET"

    with temp_engine.connect() as connection:
        counts = dict(
            connection.execute(
                text(
                    "SELECT symbol, COUNT(*) FROM orb_reference_samples "
                    "GROUP BY symbol ORDER BY symbol"
                )
            ).all()
        )
        decision_count = connection.execute(
            text("SELECT COUNT(*) FROM orb_reference_sample_decisions")
        ).scalar_one()
        eligible_decision_count = connection.execute(
            text(
                "SELECT COUNT(*) FROM orb_reference_sample_decisions "
                "WHERE progress_eligible = 1"
            )
        ).scalar_one()
        journal_mode = connection.exec_driver_sql("PRAGMA journal_mode").scalar_one()
        quick_check = connection.exec_driver_sql("PRAGMA quick_check").scalar_one()

    assert counts == {"NDX": 1, "RUT": 1, "SPX": 1, "VIX": 1}
    assert decision_count == 4
    assert eligible_decision_count == 4
    assert journal_mode.lower() == "wal"
    assert quick_check == "ok"
    assert len(persisted_threads) == len(MARKET_SPECS)
    assert db_path.parent == tmp_path
    assert db_path.exists()
    for market in MARKET_SPECS:
        persisted = database.load_orb_reference_samples(market, TRADING_DATE)
        assert len(persisted) == 1
        assert persisted[0]["sample_timestamp_utc"].replace(
            tzinfo=timezone.utc
        ) == OPEN_UTC

    compute_times = [entry[2] for entry in compute_entries]
    metrics = {
        "capture_elapsed_seconds": round(capture_elapsed, 6),
        "compute_entry_spread_seconds": round(max(compute_times) - min(compute_times), 6),
        "maximum_persist_lock_wait_seconds": round(
            max(float(result["persist_lock_wait_seconds"]) for result in results.values()),
            6,
        ),
        "maximum_persist_lock_held_seconds": round(
            max(float(result["persist_lock_held_seconds"]) for result in results.values()),
            6,
        ),
        "scenario_elapsed_seconds": round(time.perf_counter() - scenario_started, 6),
        "raw_rows": sum(counts.values()),
        "decision_rows": decision_count,
        "journal_mode": journal_mode,
        "quick_check": quick_check,
    }
    print("opening_transition_stress_metrics=" + json.dumps(metrics, sort_keys=True))

    temp_engine.dispose()


def test_production_scale_orb_snapshot_does_not_hold_live_quote_lock_while_parsing(
    monkeypatch,
):
    streamer = _build_opening_streamer(monkeypatch)
    next_instrument_id = 10_000
    for market in MARKET_SPECS:
        received = _publish_mappings_and_quotes(
            streamer,
            market,
            first_instrument_id=next_instrument_id,
        )
        next_instrument_id += len(streamer._universe_records_by_market[market])
        streamer._record_fresh_market_and_maybe_activate(
            market,
            generation=streamer.active_generation,
            received_monotonic=received,
        )

    assert streamer.handoff_status == "active"
    assert len(streamer.universe) == 2 * sum(
        REPRESENTATIVE_OPENING_PAIR_COUNTS.values()
    )
    mapping_parse_started = threading.Event()
    release_mapping_parse = threading.Event()
    quote_write_completed = threading.Event()
    snapshot_results = []
    errors = []

    class PausedMapping(dict):
        def get(self, key, default=None):
            if key == "raw_symbol" and not mapping_parse_started.is_set():
                mapping_parse_started.set()
                assert release_mapping_parse.wait(3.0)
            return super().get(key, default)

    with streamer._fresh_quote_lock:
        first_instrument_id = next(iter(streamer.symbol_mappings))
        streamer.symbol_mappings[first_instrument_id] = PausedMapping(
            streamer.symbol_mappings[first_instrument_id]
        )
        target_symbol = str(
            streamer._universe_records_by_market["SPX"][0]["symbol"]
        )
        updated_quote = dict(streamer.quotes[target_symbol])
        updated_quote["received_monotonic"] = time.monotonic()

    def take_snapshot():
        try:
            snapshot_results.append(
                streamer._snapshot_opening_reference_inputs(
                    "SPX",
                    trading_date=TRADING_DATE,
                    captured_at_utc=CAPTURE_UTC,
                )
            )
        except BaseException as exc:
            errors.append(exc)

    def publish_quote():
        try:
            streamer._store_quote(target_symbol, updated_quote)
            quote_write_completed.set()
        except BaseException as exc:
            errors.append(exc)

    snapshot_thread = threading.Thread(target=take_snapshot, daemon=True)
    writer_thread = threading.Thread(target=publish_quote, daemon=True)
    snapshot_thread.start()
    try:
        assert mapping_parse_started.wait(3.0)
        writer_thread.start()
        assert quote_write_completed.wait(0.5), (
            "ORB mapping parsing blocked the live quote writer"
        )
        # Mapping parsing now runs after the atomic state snapshot. A rollover
        # during that work must not splice a new epoch onto the old generation,
        # mappings, or quote references.
        with streamer._prediction_publication_lock:
            streamer.subscription_epoch_id = "2" * 64
    finally:
        release_mapping_parse.set()
        snapshot_thread.join(timeout=3.0)
        if writer_thread.ident is not None:
            writer_thread.join(timeout=3.0)

    assert not snapshot_thread.is_alive() and not writer_thread.is_alive()
    assert not errors
    assert snapshot_results
    snapshot, reason = snapshot_results[0]
    assert reason is None
    assert snapshot is not None
    assert snapshot["subscription_epoch_id"] == "1" * 64
    assert streamer.subscription_epoch_id == "2" * 64
    assert not streamer._opening_reference_context_is_current(snapshot)
    assert len(snapshot["records"]) == (
        2 * REPRESENTATIVE_OPENING_PAIR_COUNTS["SPX"]
    )
