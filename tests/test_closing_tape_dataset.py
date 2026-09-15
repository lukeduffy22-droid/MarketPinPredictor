from datetime import date, datetime, timezone
import hashlib
import sqlite3

import pandas as pd

from backend.closing_tape.catalog import TapeCatalog
from backend.closing_tape.config import build_session_config
from backend.closing_tape.contracts import (
    EVIDENCE_CONTRACT_VERSION,
    HISTORICAL_EVIDENCE_CONTRACT_VERSION,
    HISTORICAL_SOURCE_KIND,
    LIVE_SOURCE_KIND,
)
from backend.closing_tape.dataset import (
    attach_point_in_time_prices,
    attach_point_in_time_reference_prices,
    load_complete_contract_tape_features,
    load_complete_tape_features,
    load_marketpin_reference_prices,
    load_scored_marketpin_closes,
)


UTC = timezone.utc


def test_reference_price_loader_labels_calculated_spot_provenance(tmp_path):
    path = tmp_path / "market.sqlite"
    with sqlite3.connect(path) as connection:
        connection.execute(
            """
            CREATE TABLE prediction_snapshots (
                symbol TEXT, trading_date TEXT, timestamp_utc TEXT,
                quote_timestamp_utc TEXT, current_price REAL, provider TEXT,
                subscription_epoch_id TEXT, subscription_generation INTEGER, data_age_seconds REAL,
                quote_age_seconds REAL, predicted_close REAL, model_version TEXT,
                prediction_mode TEXT, source_payload_json TEXT, is_valid INTEGER,
                validation_status TEXT
            )
            """
        )
        connection.execute(
            "INSERT INTO prediction_snapshots VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                "SPX", "2026-08-25", "2026-08-25T15:00:55+00:00",
                "2026-08-25T15:00:54+00:00", 6500.0, "databento", "a" * 64, 1, 1.0, 1.0,
                6501.0, "test", "backend_periodic",
                '{"spot_formula_version":"put-call-parity-v2-discounted-strike",'
                '"underlying_validation_status":"valid"}',
                1, "valid",
            ),
        )

    prices = load_marketpin_reference_prices(path)

    assert len(prices) == 1
    assert prices.iloc[0]["reference_price_method"] == "put-call-parity-v2-discounted-strike"
    assert bool(prices.iloc[0]["reference_price_is_estimate"])
    assert prices.iloc[0]["reference_price_provider"] == "databento"
    assert prices.iloc[0]["reference_subscription_epoch_id"] == "a" * 64
    assert bool(prices.iloc[0]["reference_price_epoch_eligible"])


def test_reference_price_loader_keeps_legacy_schema_out_of_current_evidence(tmp_path):
    path = tmp_path / "legacy-market.sqlite"
    with sqlite3.connect(path) as connection:
        connection.execute(
            """
            CREATE TABLE prediction_snapshots (
                symbol TEXT, trading_date TEXT, timestamp_utc TEXT,
                quote_timestamp_utc TEXT, current_price REAL, provider TEXT,
                subscription_generation INTEGER, data_age_seconds REAL,
                quote_age_seconds REAL, predicted_close REAL, model_version TEXT,
                prediction_mode TEXT, source_payload_json TEXT, is_valid INTEGER,
                validation_status TEXT
            )
            """
        )

    assert load_marketpin_reference_prices(path).empty


def test_epoch_invalid_or_ambiguous_latest_price_blocks_older_primary(tmp_path):
    path = tmp_path / "market.sqlite"
    with sqlite3.connect(path) as connection:
        connection.execute(
            """
            CREATE TABLE prediction_snapshots (
                symbol TEXT, trading_date TEXT, timestamp_utc TEXT,
                quote_timestamp_utc TEXT, current_price REAL, provider TEXT,
                subscription_epoch_id TEXT, subscription_generation INTEGER,
                data_age_seconds REAL, quote_age_seconds REAL, predicted_close REAL,
                model_version TEXT, prediction_mode TEXT, source_payload_json TEXT,
                is_valid INTEGER, validation_status TEXT
            )
            """
        )
        base = (
            "SPX", "2026-08-25", "2026-08-25T15:00:50+00:00",
            "2026-08-25T15:00:49+00:00", 6500.0, "databento",
        )
        connection.executemany(
            "INSERT INTO prediction_snapshots VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            [
                (*base, "a" * 64, 1, 1.0, 1.0, 6501.0, "test", "backend_periodic", "{}", 1, "valid"),
                (*base, "b" * 64, 1, 1.0, 1.0, 6502.0, "test", "backend_periodic", "{}", 1, "valid"),
                (
                    "SPX", "2026-08-25", "2026-08-25T15:00:55+00:00",
                    "2026-08-25T15:00:54+00:00", 6503.0, "databento",
                    "BAD", 1, 1.0, 1.0, 6504.0, "test", "backend_periodic", "{}", 1, "valid",
                ),
            ],
        )
    prices = load_marketpin_reference_prices(path)
    assert not prices["reference_price_epoch_eligible"].any()
    assert set(prices["reference_price_epoch_status"]) == {
        "ambiguous_subscription_epoch_at_timestamp", "invalid_subscription_epoch"
    }
    features = pd.DataFrame([{
        "trading_date": "2026-08-25", "session_id": "s1", "family_root": "SPX",
        "minute_utc": datetime(2026, 8, 25, 15, 0, tzinfo=UTC),
    }])
    assert attach_point_in_time_prices(features, prices, max_price_age_seconds=30).empty


def test_point_in_time_price_join_uses_only_prices_known_by_minute_end():
    features = pd.DataFrame(
        [
            {
                "trading_date": "2026-08-25",
                "session_id": "s1",
                "family_root": "SPX",
                "minute_utc": datetime(2026, 8, 25, 14, 30, tzinfo=UTC),
            }
        ]
    )
    prices = pd.DataFrame(
        [
            {
                "trading_date": "2026-08-25", "family_root": "SPX",
                "timestamp_utc": datetime(2026, 8, 25, 14, 30, 55, tzinfo=UTC),
                "current_price": 6500.0,
            },
            {
                "trading_date": "2026-08-25", "family_root": "SPX",
                "timestamp_utc": datetime(2026, 8, 25, 14, 31, 5, tzinfo=UTC),
                "current_price": 9999.0,
            },
        ]
    )

    joined = attach_point_in_time_prices(features, prices, max_price_age_seconds=30)

    assert len(joined) == 1
    assert joined.iloc[0]["reference_price"] == 6500.0
    assert joined.iloc[0]["reference_price_age_seconds"] == 5.0


def test_stale_reference_price_fails_closed():
    features = pd.DataFrame(
        [{
            "trading_date": "2026-08-25", "session_id": "s1", "family_root": "SPX",
            "minute_utc": datetime(2026, 8, 25, 14, 30, tzinfo=UTC),
        }]
    )
    prices = pd.DataFrame(
        [{
            "trading_date": "2026-08-25", "family_root": "SPX",
            "timestamp_utc": datetime(2026, 8, 25, 14, 29, tzinfo=UTC),
            "current_price": 6500.0,
        }]
    )

    assert attach_point_in_time_prices(features, prices, max_price_age_seconds=30).empty


def test_reference_fallback_never_displaces_fresh_primary():
    features = pd.DataFrame(
        [
            {"trading_date": "2026-08-25", "session_id": "s1", "family_root": "SPX", "minute_utc": "2026-08-25T15:00:00Z"},
            {"trading_date": "2026-08-25", "session_id": "s1", "family_root": "RUT", "minute_utc": "2026-08-25T15:00:00Z"},
        ]
    )
    common = {
        "trading_date": "2026-08-25", "reference_price_is_estimate": True,
        "reference_price_provider": "databento", "reference_price_method": "test",
    }
    primary = pd.DataFrame(
        [{
            **common, "family_root": "SPX", "timestamp_utc": "2026-08-25T15:00:50Z",
            "current_price": 6500.0, "reference_subscription_epoch_id": "a" * 64,
            "reference_subscription_generation": 1,
            "reference_price_epoch_eligible": True,
            "reference_price_epoch_status": "eligible_current_epoch",
        }]
    )
    fallback = pd.DataFrame(
        [
            {**common, "family_root": "SPX", "timestamp_utc": "2026-08-25T15:00:59Z", "current_price": 9999.0},
            {**common, "family_root": "RUT", "timestamp_utc": "2026-08-25T15:00:59Z", "current_price": 2000.0},
        ]
    )

    joined = attach_point_in_time_reference_prices(features, primary, fallback, max_price_age_seconds=30)

    values = joined.set_index("family_root")
    assert values.loc["SPX", "reference_price"] == 6500.0
    assert values.loc["SPX", "reference_price_tier"] == "primary_marketpin_snapshot"
    assert values.loc["SPX", "reference_subscription_epoch_id"] == "a" * 64
    assert values.loc["RUT", "reference_price"] == 2000.0
    assert values.loc["RUT", "reference_price_tier"] == "tcbbo_parity_fallback_estimate"
    assert pd.isna(values.loc["RUT", "reference_subscription_epoch_id"])
    assert values.loc["RUT", "reference_price_epoch_status"] == "not_applicable_tcbbo_fallback"


def test_catalog_loader_requires_complete_hash_aligned_tape(tmp_path):
    config = build_session_config(
        tmp_path,
        trading_day=date(2026, 8, 25),
        now=datetime(2026, 8, 25, 15, 0, tzinfo=UTC),
        session_id="s1",
    )
    catalog = TapeCatalog(config.catalog_path)
    catalog.start_session(config)
    catalog.register_feed(config.session_id, config.feeds[0], config.output_dir / "raw.dbn")
    observed = {
        "session_id": "s1", "feed_name": "opra_options", "family_root": "SPX",
        "minute_utc": "2026-08-25T15:00:00+00:00", "asset_class": "options",
        "trade_count": 1, "volume": 2.0, "notional": 1000.0,
        "call_count": 1, "put_count": 0, "call_volume": 2.0, "put_volume": 0.0,
        "call_premium": 1000.0, "put_premium": 0.0, "first_price": 5.0,
        "high_price": 5.0, "low_price": 5.0, "last_price": 5.0,
        "price_volume_sum": 10.0, "largest_trade_size": 2.0,
        "largest_trade_notional": 1000.0, "updated_at_utc": "2026-08-25T15:01:00+00:00",
    }
    inferred = {
        "session_id": "s1", "feed_name": "opra_options", "family_root": "SPX",
        "minute_utc": "2026-08-25T15:00:00+00:00",
        "inference_method": "trade_price_vs_pretrade_nbbo", "inference_version": "1.0",
        "source_sha256": "a" * 64, "updated_at_utc": "2026-08-25T15:01:00+00:00",
    }
    for bucket in ("at_ask", "at_bid", "inside", "unknown"):
        inferred[f"{bucket}_count"] = 1 if bucket == "at_ask" else 0
        inferred[f"{bucket}_volume"] = 2.0 if bucket == "at_ask" else 0.0
        inferred[f"{bucket}_notional"] = 1000.0 if bucket == "at_ask" else 0.0
    for kind in ("call", "put"):
        for bucket in ("at_ask", "at_bid"):
            inferred[f"{kind}_{bucket}_notional"] = 1000.0 if (kind, bucket) == ("call", "at_ask") else 0.0
    catalog.upsert_observed_minutes([observed])
    catalog.upsert_inferred_minute_flow([inferred])
    raw_symbol = "SPX   260825C06500000"
    contract_observed = {
        "session_id": "s1", "feed_name": "opra_options", "family_root": "SPX",
        "raw_symbol": raw_symbol, "expiration": "2026-08-25", "option_type": "C",
        "strike": 6500.0, "minute_utc": "2026-08-25T15:00:00+00:00",
        "trade_count": 1, "volume": 2.0, "notional": 1000.0,
        "first_price": 5.0, "high_price": 5.0, "low_price": 5.0, "last_price": 5.0,
        "price_volume_sum": 10.0, "nbbo_valid_count": 1,
        "quoted_spread_sum": 0.2, "quoted_spread_bps_sum": 400.0,
        "trade_to_mid_abs_sum": 0.0, "trade_to_mid_signed_sum": 0.0,
        "bid_size_sum": 10.0, "ask_size_sum": 12.0,
        "receive_lag_ns_sum": 100, "receive_lag_ns_max": 100,
        "data_quality_flagged_count": 0, "updated_at_utc": "2026-08-25T15:01:00+00:00",
    }
    contract_inferred = {
        "session_id": "s1", "feed_name": "opra_options", "family_root": "SPX",
        "raw_symbol": raw_symbol, "expiration": "2026-08-25", "option_type": "C",
        "strike": 6500.0, "minute_utc": "2026-08-25T15:00:00+00:00",
        "inference_method": "trade_price_vs_pretrade_nbbo", "inference_version": "1.0",
        "source_sha256": "a" * 64, "updated_at_utc": "2026-08-25T15:01:00+00:00",
    }
    for bucket in ("at_ask", "at_bid", "inside", "unknown"):
        contract_inferred[f"{bucket}_count"] = 1 if bucket == "at_ask" else 0
        contract_inferred[f"{bucket}_volume"] = 2.0 if bucket == "at_ask" else 0.0
        contract_inferred[f"{bucket}_notional"] = 1000.0 if bucket == "at_ask" else 0.0
    catalog.upsert_observed_contract_minutes([contract_observed])
    catalog.upsert_inferred_contract_minute_flow([contract_inferred])
    catalog.upsert_open_interest(
        [
            {
                "session_id": "s1",
                "feed_name": "opra_options",
                "instrument_id": 1,
                "raw_symbol": raw_symbol,
                "family_root": "SPX",
                "asof_utc": "2026-08-25T00:00:00+00:00",
                # The mutable latest projection is deliberately not the value
                # that was knowable at the contract minute.
                "open_interest": 999.0,
            }
        ]
    )
    early_receive_ns = int(pd.Timestamp("2026-08-25T15:00:30Z").value)
    exact_cutoff_ns = int(pd.Timestamp("2026-08-25T15:01:00Z").value)
    future_receive_ns = int(pd.Timestamp("2026-08-25T15:01:30Z").value)
    reference_ns = int(pd.Timestamp("2026-08-25T00:00:00Z").value)
    catalog.append_open_interest_observations(
        [
            {
                "observation_key": f"{'a' * 64}:100",
                "session_id": "s1", "feed_name": "opra_options",
                "source_sha256": "a" * 64, "record_offset": 100,
                "instrument_id": 1, "raw_symbol": raw_symbol,
                "family_root": "SPX", "ts_event_ns": early_receive_ns - 1,
                "ts_recv_ns": early_receive_ns, "ts_ref_ns": reference_ns,
                "asof_utc": "2026-08-25T00:00:00+00:00", "sequence": 1,
                "channel_id": 1, "update_action": 1, "stat_flags": 0,
                "open_interest": 10.0,
                "ingested_at_utc": "2026-08-25T20:01:00+00:00",
            },
            {
                "observation_key": f"{'a' * 64}:200",
                "session_id": "s1", "feed_name": "opra_options",
                "source_sha256": "a" * 64, "record_offset": 200,
                "instrument_id": 1, "raw_symbol": raw_symbol,
                "family_root": "SPX", "ts_event_ns": future_receive_ns - 1,
                "ts_recv_ns": future_receive_ns, "ts_ref_ns": reference_ns,
                "asof_utc": "2026-08-25T00:00:00+00:00", "sequence": 2,
                "channel_id": 1, "update_action": 1, "stat_flags": 0,
                "open_interest": 99.0,
                "ingested_at_utc": "2026-08-25T20:01:00+00:00",
            },
            {
                "observation_key": f"{'a' * 64}:300",
                "session_id": "s1", "feed_name": "opra_options",
                "source_sha256": "a" * 64, "record_offset": 300,
                "instrument_id": 1, "raw_symbol": raw_symbol,
                "family_root": "SPX", "ts_event_ns": early_receive_ns,
                "ts_recv_ns": None, "ts_ref_ns": reference_ns,
                "asof_utc": "2026-08-25T00:00:00+00:00", "sequence": 99,
                "channel_id": 1, "update_action": 1, "stat_flags": 0,
                "open_interest": 777.0,
                "ingested_at_utc": "2026-08-25T20:01:00+00:00",
            },
            {
                "observation_key": f"{'a' * 64}:400",
                "session_id": "s1", "feed_name": "opra_options",
                "source_sha256": "a" * 64, "record_offset": 400,
                "instrument_id": 1, "raw_symbol": raw_symbol,
                "family_root": "SPX", "ts_event_ns": exact_cutoff_ns - 1,
                "ts_recv_ns": exact_cutoff_ns, "ts_ref_ns": reference_ns,
                "asof_utc": "2026-08-25T00:00:00+00:00", "sequence": 3,
                "channel_id": 1, "update_action": 1, "stat_flags": 0,
                "open_interest": 11.0,
                "ingested_at_utc": "2026-08-25T20:01:00+00:00",
            },
            {
                "observation_key": f"{'b' * 64}:500",
                "session_id": "s1", "feed_name": "opra_options",
                "source_sha256": "b" * 64, "record_offset": 500,
                "instrument_id": 1, "raw_symbol": raw_symbol,
                "family_root": "SPX", "ts_event_ns": exact_cutoff_ns - 2,
                "ts_recv_ns": exact_cutoff_ns - 1, "ts_ref_ns": reference_ns,
                "asof_utc": "2026-08-25T00:00:00+00:00", "sequence": 100,
                "channel_id": 1, "update_action": 1, "stat_flags": 0,
                "open_interest": 888.0,
                "ingested_at_utc": "2026-08-25T20:01:00+00:00",
            },
        ]
    )
    catalog.update_feed_status(
        "s1", "opra_options",
        {
            "status": "complete",
            "complete": 1,
            "sha256": "a" * 64,
            "tcbbo_records": 100,
            "tcbbo_timestamped_records": 100,
            "tcbbo_valid_nbbo_records": 99,
        },
    )
    catalog.record_finalization_run(
        {
            "run_key": "v3-test", "session_id": "s1", "feed_name": "opra_options",
            "source_sha256": "a" * 64,
            "evidence_contract_version": EVIDENCE_CONTRACT_VERSION,
            "attempted_at_utc": "2026-08-25T20:21:00+00:00", "complete": 1,
            "issues_json": "[]", "prior_status": None, "prior_error": None,
            "prior_gaps_json": "[]", "report_json": "{}",
        }
    )

    loaded = load_complete_tape_features([config.catalog_path])
    assert len(loaded) == 1
    assert loaded.iloc[0]["call_at_ask_notional"] == 1000.0
    contract_loaded = load_complete_contract_tape_features([config.catalog_path])
    assert len(contract_loaded) == 1
    assert contract_loaded.iloc[0]["strike"] == 6500.0
    assert contract_loaded.iloc[0]["open_interest"] == 11.0
    assert contract_loaded.iloc[0]["open_interest_available_at_utc"] == pd.Timestamp(
        "2026-08-25T15:01:00Z"
    )
    assert contract_loaded.iloc[0]["at_ask_notional"] == 1000.0

    delete_receive_ns = exact_cutoff_ns
    catalog.append_open_interest_observations(
        [
            {
                "observation_key": f"{'a' * 64}:600",
                "session_id": "s1", "feed_name": "opra_options",
                "source_sha256": "a" * 64, "record_offset": 600,
                "instrument_id": 1, "raw_symbol": raw_symbol,
                "family_root": "SPX", "ts_event_ns": delete_receive_ns - 1,
                "ts_recv_ns": delete_receive_ns, "ts_ref_ns": reference_ns,
                "asof_utc": "2026-08-25T00:00:00+00:00", "sequence": 4,
                "channel_id": 1, "update_action": 2, "stat_flags": 0,
                "open_interest": None,
                "ingested_at_utc": "2026-08-25T20:01:00+00:00",
            }
        ]
    )
    deleted_contract = load_complete_contract_tape_features([config.catalog_path])
    assert pd.isna(deleted_contract.iloc[0]["open_interest"])
    assert deleted_contract.iloc[0]["open_interest_update_action"] == 2

    catalog.update_feed_status(
        "s1",
        "opra_options",
        {
            "source_kind": HISTORICAL_SOURCE_KIND,
            "evidence_contract_version": HISTORICAL_EVIDENCE_CONTRACT_VERSION,
            "operational_counters_applicable": 0,
        },
    )
    catalog.record_finalization_run(
        {
            "run_key": "historical-dataset-test",
            "session_id": "s1",
            "feed_name": "opra_options",
            "source_sha256": "a" * 64,
            "evidence_contract_version": HISTORICAL_EVIDENCE_CONTRACT_VERSION,
            "attempted_at_utc": "2026-08-25T20:22:00+00:00",
            "complete": 1,
            "issues_json": "[]",
            "report_json": "{}",
        }
    )
    assert len(load_complete_tape_features([config.catalog_path])) == 1
    assert len(load_complete_contract_tape_features([config.catalog_path])) == 1

    catalog.update_feed_status(
        "s1",
        "opra_options",
        {"evidence_contract_version": EVIDENCE_CONTRACT_VERSION},
    )
    assert load_complete_tape_features([config.catalog_path]).empty
    assert load_complete_contract_tape_features([config.catalog_path]).empty
    catalog.update_feed_status(
        "s1",
        "opra_options",
        {
            "source_kind": LIVE_SOURCE_KIND,
            "evidence_contract_version": EVIDENCE_CONTRACT_VERSION,
            "operational_counters_applicable": 1,
        },
    )

    catalog.update_feed_status("s1", "opra_options", {"sha256": "b" * 64})
    assert load_complete_tape_features([config.catalog_path]).empty

    catalog.update_feed_status(
        "s1", "opra_options", {"sha256": "a" * 64, "tcbbo_valid_nbbo_records": 94}
    )
    assert load_complete_tape_features([config.catalog_path]).empty

    catalog.update_feed_status(
        "s1", "opra_options", {"tcbbo_valid_nbbo_records": 99}
    )
    with catalog.connect() as connection:
        connection.execute("DROP TABLE tape_open_interest_observations")
    legacy_contract = load_complete_contract_tape_features([config.catalog_path])
    assert len(legacy_contract) == 1
    assert pd.isna(legacy_contract.iloc[0]["open_interest"])
    assert pd.isna(legacy_contract.iloc[0]["open_interest_available_at_utc"])


def test_scored_close_loader_excludes_nonproduction_test_symbols(tmp_path):
    path = tmp_path / "market.db"
    vault = tmp_path / "verified_close_sources"
    artifact = b"verified SPX close fixture"
    artifact_hash = hashlib.sha256(artifact).hexdigest()
    artifact_path = vault / "2026-08-25" / "SPX" / f"{artifact_hash}.txt"
    artifact_path.parent.mkdir(parents=True)
    artifact_path.write_bytes(artifact)
    with sqlite3.connect(path) as connection:
        connection.execute(
            """
            CREATE TABLE eod_close_observations (
                id INTEGER PRIMARY KEY, symbol TEXT, trading_date TEXT,
                official_close REAL, source TEXT, source_reference TEXT,
                source_artifact_sha256 TEXT,
                source_verified INTEGER, observed_at_utc TEXT, correction_of_id INTEGER
            )
            """
        )
        connection.executemany(
            """
            INSERT INTO eod_close_observations (
                symbol, trading_date, official_close, source, source_reference,
                source_artifact_sha256, source_verified, observed_at_utc, correction_of_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    "SPX", "2026-08-25", 6500.0, "sp-global-official",
                    "https://www.spglobal.com/spx",
                    artifact_hash, 1, "2026-08-25T21:01:00+00:00", None,
                ),
                (
                    "TEST123", "2026-08-25", 101.0, "official-test", "ref-2",
                    "b" * 64, 1, "2026-08-25T21:01:00+00:00", None,
                ),
            ],
        )

    closes = load_scored_marketpin_closes(
        path, verified_artifact_root=vault
    )

    assert closes["family_root"].tolist() == ["SPX"]
    assert closes.iloc[0]["actual_close"] == 6500.0
    assert closes.iloc[0]["close_source_artifact_path"] == str(artifact_path.resolve())
