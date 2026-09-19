from datetime import date, datetime, timezone
import hashlib
import sqlite3

import pytest

import backend.closing_tape.readiness as readiness
from backend.closing_tape.catalog import TapeCatalog
from backend.closing_tape.config import build_session_config
from backend.closing_tape.contracts import (
    EVIDENCE_CONTRACT_VERSION,
    HISTORICAL_EVIDENCE_CONTRACT_VERSION,
    HISTORICAL_SOURCE_KIND,
)
from backend.closing_tape.readiness import (
    _source_observation_count,
    audit_training_readiness,
)


UTC = timezone.utc
FAMILIES = ("SPX", "NDX", "RUT", "VIX", "SPY")
SOURCES = {
    "SPX": "sp-global-official",
    "NDX": "nasdaq-official",
    "RUT": "ftse-russell-official",
    "VIX": "cboe-official",
    "SPY": "nyse-arca-official",
}
REFERENCES = {
    "SPX": "https://www.spglobal.com/spx",
    "NDX": "https://www.nasdaq.com/ndx",
    "RUT": "https://www.lseg.com/rut",
    "VIX": "https://www.cboe.com/vix",
    "SPY": "https://www.nyse.com/api/nyseservice/v1/quotes?symbol=SPY",
}


@pytest.fixture(autouse=True)
def _isolate_configured_market_database(monkeypatch, tmp_path):
    """Keep temporary readiness fixtures independent of a live DATABASE_URL."""
    database_path = (tmp_path / "data" / "market_data.db").as_posix()
    monkeypatch.setenv("DATABASE_URL", f"sqlite:///{database_path}")


def _write_close_artifact(tmp_path, trading_date, symbol, payload=None):
    content = payload or f"{trading_date}/{symbol}".encode()
    digest = hashlib.sha256(content).hexdigest()
    destination = (
        tmp_path / "data" / "verified_close_sources" / trading_date / symbol
        / f"{digest}.txt"
    )
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_bytes(content)
    return digest


def _complete_catalog(tmp_path, *, session_id="eligible", hash_character="a"):
    config = build_session_config(
        tmp_path,
        trading_day=date(2026, 8, 25),
        now=datetime(2026, 8, 25, 15, 0, tzinfo=UTC),
        session_id=session_id,
    )
    catalog = TapeCatalog(config.catalog_path)
    catalog.start_session(config)
    source = config.output_dir / f"complete-{session_id}.dbn"
    source.write_bytes(b"immutable fixture")
    feed = config.feeds[0]
    catalog.register_feed(config.session_id, feed, source)
    source_hash = hash_character * 64
    catalog.update_feed_status(
        config.session_id,
        feed.name,
        {
            "status": "complete",
            "complete": 1,
            "records_seen": 1000,
            "trade_records": 1000,
            "tcbbo_records": 1000,
            "tcbbo_timestamped_records": 1000,
            "tcbbo_valid_nbbo_records": 990,
            "statistics_records": 5,
            "definition_records": 5,
            "subscription_acks": 3,
            "expected_subscription_acks": 3,
            "replay_completed": 3,
            "sha256": source_hash,
        },
    )
    catalog.finish_session(config.session_id, "complete")
    catalog.record_finalization_run(
        {
            "run_key": ("c" if hash_character == "b" else "b") * 64,
            "session_id": config.session_id,
            "feed_name": feed.name,
            "source_sha256": source_hash,
            "evidence_contract_version": EVIDENCE_CONTRACT_VERSION,
            "attempted_at_utc": "2026-08-25T20:21:00+00:00",
            "complete": 1,
            "issues_json": "[]",
            "report_json": "{}",
        }
    )
    observed = []
    inferred = []
    observations = []
    definitions = []
    for instrument_id, family in enumerate(FAMILIES, start=1):
        key = {
            "session_id": config.session_id,
            "feed_name": feed.name,
            "family_root": family,
            "minute_utc": "2026-08-25T19:45:00+00:00",
        }
        observed.append(
            {
                **key,
                "asset_class": "options",
                "trade_count": 1,
                "volume": 1,
                "notional": 1,
                "call_count": 1,
                "put_count": 0,
                "call_volume": 1,
                "put_volume": 0,
                "call_premium": 1,
                "put_premium": 0,
                "first_price": 1,
                "high_price": 1,
                "low_price": 1,
                "last_price": 1,
                "price_volume_sum": 1,
                "largest_trade_size": 1,
                "largest_trade_notional": 1,
                "updated_at_utc": "2026-08-25T20:01:00+00:00",
            }
        )
        inferred.append(
            {
                **key,
                "inference_method": "trade_price_vs_pretrade_nbbo",
                "inference_version": "1.0",
                "source_sha256": source_hash,
                **{column: 0 for column in (
                    "at_ask_count", "at_bid_count", "inside_count", "unknown_count",
                    "at_ask_volume", "at_bid_volume", "inside_volume", "unknown_volume",
                    "at_ask_notional", "at_bid_notional", "inside_notional", "unknown_notional",
                    "call_at_ask_notional", "call_at_bid_notional", "put_at_ask_notional",
                    "put_at_bid_notional",
                )},
                "updated_at_utc": "2026-08-25T20:01:00+00:00",
            }
        )
        observations.append(
            {
                "observation_key": f"oi-{session_id}-{family}",
                "session_id": config.session_id,
                "feed_name": feed.name,
                "source_sha256": source_hash,
                "record_offset": instrument_id * 64,
                "instrument_id": instrument_id,
                "raw_symbol": f"{family} fixture",
                "family_root": family,
                "ts_event_ns": instrument_id,
                "ts_recv_ns": instrument_id,
                "ts_ref_ns": instrument_id,
                "asof_utc": "2026-08-25T00:00:00+00:00",
                "sequence": instrument_id,
                "channel_id": 1,
                "update_action": 1,
                "stat_flags": 0,
                "open_interest": 100,
                "ingested_at_utc": "2026-08-25T20:01:00+00:00",
            }
        )
        raw_definition = f"definition-{family}".encode()
        definitions.append(
            {
                "observation_key": f"definition-{session_id}-{family}",
                "session_id": config.session_id,
                "feed_name": feed.name,
                "source_sha256": source_hash,
                "record_offset": instrument_id * 520,
                "record_bytes": len(raw_definition),
                "instrument_id": instrument_id,
                "ts_event_ns": instrument_id,
                "ts_recv_ns": instrument_id,
                "raw_symbol": f"{family} fixture",
                "family_root": family,
                "option_type": "C",
                "expiration": "2026-08-25",
                "strike": 1.0,
                "security_update_action": "A",
                "raw_record_sha256": hashlib.sha256(raw_definition).hexdigest(),
                "raw_record": raw_definition,
                "ingested_at_utc": "2026-08-25T20:01:00+00:00",
            }
        )
    catalog.upsert_observed_minutes(observed)
    catalog.upsert_inferred_minute_flow(inferred)
    catalog.persist_open_interest_replay(
        session_id=config.session_id,
        feed_name=feed.name,
        observations=observations,
    )
    catalog.append_instrument_definition_observations(definitions)
    return config, catalog


def test_readiness_audit_accepts_only_full_hash_aligned_evidence(tmp_path):
    config, _catalog = _complete_catalog(tmp_path)

    report = audit_training_readiness(tmp_path)

    assert report.catalogs == 1
    assert report.catalog_issues == ()
    assert report.catalog_paths == (str(config.catalog_path.resolve()),)
    assert len(report.catalog_resolution_id) == 64
    assert report.sessions == 1
    assert report.eligible_sessions == 1
    assert report.unique_verified_sources == 1
    assert not report.capture_gate_ready
    assert report.sessions_detail[0].session_id == config.session_id
    assert report.sessions_detail[0].eligible
    assert len(report.label_work_queue) == 1
    assert report.label_work_queue[0].trading_date == "2026-08-25"
    assert report.label_work_queue[0].source_sha256 == "a" * 64
    assert report.label_work_queue[0].verified_families == ()
    assert report.label_work_queue[0].missing_families == tuple(sorted(FAMILIES))
    requirements = {
        item.family_root: item for item in report.label_work_queue[0].missing_requirements
    }
    assert requirements["SPX"].approved_sources == (
        "cboe-official",
        "sp-global-official",
    )
    assert requirements["SPX"].approved_reference_hosts == (
        "cboe.com",
        "spglobal.com",
    )
    assert requirements["RUT"].approved_sources == (
        "cboe-official",
        "ftse-russell-official",
    )
    assert requirements["RUT"].approved_reference_hosts == (
        "cboe.com",
        "ftserussell.com",
        "lseg.com",
    )


@pytest.mark.parametrize("artifact_case", ["duplicate", "missing", "valid"])
def test_close_reconciliation_upgrades_only_proven_completed_session(tmp_path, artifact_case):
    from backend.closing_tape.close_registry import reconcile_verified_close_artifact
    from backend.closing_tape.dataset import load_scored_marketpin_closes

    config, catalog = _complete_catalog(tmp_path)
    catalog.update_feed_status(config.session_id, "opra_options", {
        "source_kind": HISTORICAL_SOURCE_KIND,
        "evidence_contract_version": HISTORICAL_EVIDENCE_CONTRACT_VERSION,
        "operational_counters_applicable": 0,
        "source_manifest_path": str(config.output_dir / "complete-eligible.dbn"),
    })
    catalog.record_finalization_run({
        "run_key": "historical-registry-fixture", "session_id": config.session_id,
        "feed_name": "opra_options", "source_sha256": "a" * 64,
        "evidence_contract_version": HISTORICAL_EVIDENCE_CONTRACT_VERSION,
        "attempted_at_utc": "2026-08-25T20:22:00+00:00", "complete": 1,
        "issues_json": "[]", "report_json": "{}",
    })
    market = tmp_path / "data/market_data.db"
    vault = tmp_path / "data/verified_close_sources"
    with sqlite3.connect(market) as connection:
        connection.execute("""CREATE TABLE eod_close_observations (
            id INTEGER PRIMARY KEY, symbol TEXT, trading_date TEXT,
            official_close REAL, source TEXT, source_reference TEXT,
            source_verified INTEGER, observed_at_utc TEXT,
            source_artifact_sha256 TEXT, correction_of_id INTEGER)""")
        for family in FAMILIES:
            digest = _write_close_artifact(tmp_path, "2026-08-25", family)
            connection.execute("""INSERT INTO eod_close_observations
                (symbol, trading_date, official_close, source, source_reference,
                 source_verified, observed_at_utc, source_artifact_sha256)
                VALUES (?, '2026-08-25', 100, ?, ?, 1, '2026-08-25T21:00:00+00:00', ?)""",
                (family, SOURCES[family], REFERENCES[family], digest))
            if family == "SPX":
                target_hash = digest
                target = vault / "2026-08-25/SPX" / f"{digest}.txt"
    if artifact_case == "duplicate":
        target.with_suffix(".html").write_bytes(target.read_bytes())
    elif artifact_case == "missing":
        target.unlink()
    # SQLite read-only connections may create transient WAL/SHM coordination files.
    def tape_bytes():
        return {p: p.read_bytes() for p in config.output_dir.rglob("*")
                if p.is_file() and not p.name.endswith(("-shm", "-wal"))}
    tape_before = tape_bytes()
    market_before = market.read_bytes()
    before = audit_training_readiness(tmp_path)
    assert before.eligible_sessions == 1, before.sessions_detail[0].reasons
    assert before.model_evidence_sessions == (1 if artifact_case == "valid" else 0)
    identity = dict(trading_date="2026-08-25", symbol="SPX", source_artifact_sha256=target_hash)
    if artifact_case == "missing":
        with pytest.raises(ValueError):
            reconcile_verified_close_artifact(vault, **identity)
        with pytest.raises(ValueError, match="verified close artifact failed"):
            load_scored_marketpin_closes(market, verified_artifact_root=vault)
    else:
        reconcile_verified_close_artifact(vault, **identity)
        assert len(load_scored_marketpin_closes(market, verified_artifact_root=vault)) == 5
    after = audit_training_readiness(tmp_path)
    assert after.model_evidence_sessions == (0 if artifact_case == "missing" else 1)
    assert not after.model_training_ready
    assert market.read_bytes() == market_before
    assert tape_bytes() == tape_before


def test_immutable_observation_count_is_content_addressed(tmp_path):
    config, catalog = _complete_catalog(tmp_path)
    with catalog.connect() as connection:
        feed = connection.execute(
            "SELECT sha256 FROM tape_feed_status WHERE session_id=? AND feed_name='opra_options'",
            (config.session_id,),
        ).fetchone()
        assert _source_observation_count(
            connection,
            "tape_instrument_definition_observations",
            str(feed[0]),
        ) == 5
        assert _source_observation_count(
            connection,
            "tape_open_interest_observations",
            str(feed[0]),
        ) == 5

        with pytest.raises(ValueError, match="unsupported immutable observation table"):
            _source_observation_count(connection, "tape_sessions", str(feed[0]))


def test_readiness_audit_explains_feed_gap_and_rejects_session(tmp_path):
    config, catalog = _complete_catalog(tmp_path)
    catalog.update_feed_status(config.session_id, "opra_options", {"reconnect_count": 1})

    report = audit_training_readiness(tmp_path)

    assert report.eligible_sessions == 0
    assert "feed records reconnect gaps" in report.sessions_detail[0].reasons


def test_readiness_accepts_historical_contract_without_live_counter_claims(tmp_path):
    config, catalog = _complete_catalog(tmp_path)
    catalog.update_feed_status(
        config.session_id,
        "opra_options",
        {
            "source_kind": HISTORICAL_SOURCE_KIND,
            "evidence_contract_version": HISTORICAL_EVIDENCE_CONTRACT_VERSION,
            "operational_counters_applicable": 0,
            "source_manifest_path": str(config.output_dir / "complete-eligible.dbn"),
        },
    )
    catalog.record_finalization_run(
        {
            "run_key": "historical-readiness",
            "session_id": config.session_id,
            "feed_name": "opra_options",
            "source_sha256": "a" * 64,
            "evidence_contract_version": HISTORICAL_EVIDENCE_CONTRACT_VERSION,
            "attempted_at_utc": "2026-08-25T20:22:00+00:00",
            "complete": 1,
            "issues_json": "[]",
            "report_json": "{}",
        }
    )

    report = audit_training_readiness(tmp_path)

    assert report.eligible_sessions == 1
    assert report.sessions_detail[0].eligible

    catalog.update_feed_status(
        config.session_id,
        "opra_options",
        {"evidence_contract_version": EVIDENCE_CONTRACT_VERSION},
    )
    report = audit_training_readiness(tmp_path)
    assert report.eligible_sessions == 0
    assert any(
        "evidence contract does not match source kind" in reason
        for reason in report.sessions_detail[0].reasons
    )


def test_readiness_audit_handles_empty_project(tmp_path):
    report = audit_training_readiness(tmp_path)

    assert report.catalogs == 0
    assert report.catalog_issues == ()
    assert report.sessions == 0
    assert not report.model_training_ready


def test_readiness_discovers_eligible_external_catalog_root(tmp_path):
    primary_root = tmp_path / "primary"
    primary_root.mkdir()
    external_project = tmp_path / "external-project"
    config, _catalog = _complete_catalog(external_project)
    external_root = external_project / "data" / "closing_tape"

    report = audit_training_readiness(
        primary_root,
        catalog_roots=(external_root,),
    )

    assert report.catalogs == 1
    assert report.sessions == 1
    assert report.eligible_sessions == 1
    assert report.catalog_issues == ()
    assert report.catalog_paths == (str(config.catalog_path.resolve()),)
    assert report.sessions_detail[0].catalog_path == str(config.catalog_path.resolve())


def test_configured_catalog_gap_disables_otherwise_passing_capture_gate(
    monkeypatch,
    tmp_path,
):
    _complete_catalog(tmp_path)
    missing_root = tmp_path / "missing-archive"
    monkeypatch.setattr(readiness, "CAPTURE_GATE_SESSIONS_REQUIRED", 1)

    report = audit_training_readiness(
        tmp_path,
        catalog_roots=(missing_root,),
    )

    assert report.eligible_sessions == 1
    assert report.catalog_issues == (
        f"configured catalog root is missing: {missing_root.resolve()}",
    )
    assert report.capture_gate_ready is False
    assert not missing_root.exists()


def test_corrupt_discovered_catalog_is_reported_instead_of_escaping(tmp_path):
    catalog_path = (
        tmp_path
        / "data"
        / "closing_tape"
        / "2026-08-25"
        / "closing_tape.sqlite"
    )
    catalog_path.parent.mkdir(parents=True)
    catalog_path.write_bytes(b"not sqlite")

    report = audit_training_readiness(tmp_path)

    assert report.catalogs == 1
    assert report.sessions == 0
    assert len(report.catalog_issues) == 1
    assert "catalog audit failed" in report.catalog_issues[0]
    assert str(catalog_path.resolve()) in report.catalog_issues[0]
    assert report.capture_gate_ready is False


def test_verified_close_sessions_require_every_production_family(tmp_path):
    _complete_catalog(tmp_path)
    hashes = {
        family: _write_close_artifact(tmp_path, "2026-08-25", family)
        for family in FAMILIES
    }
    database_path = tmp_path / "data" / "market_data.db"
    database_path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(database_path) as connection:
        connection.execute(
            """
            CREATE TABLE eod_close_observations (
                id INTEGER PRIMARY KEY,
                symbol TEXT NOT NULL,
                trading_date TEXT NOT NULL,
                official_close REAL NOT NULL,
                source TEXT NOT NULL,
                source_reference TEXT NOT NULL,
                source_verified INTEGER NOT NULL,
                observed_at_utc TEXT NOT NULL,
                source_artifact_sha256 TEXT
            )
            """
        )
        connection.executemany(
            """
            INSERT INTO eod_close_observations
                (symbol, trading_date, official_close, source, source_reference,
                 source_verified,
                 observed_at_utc, source_artifact_sha256)
            VALUES (?, '2026-08-25', ?, ?, ?, 1,
                    '2026-08-25T21:00:00+00:00', ?)
            """,
            [
                (
                    family, float(index), SOURCES[family], REFERENCES[family],
                    hashes[family],
                )
                for index, family in enumerate(FAMILIES[:-1], start=1)
            ],
        )

    report = audit_training_readiness(tmp_path)
    assert report.verified_close_sessions == 0
    assert len(report.label_work_queue) == 1
    assert report.label_work_queue[0].verified_families == tuple(sorted(FAMILIES[:-1]))
    assert report.label_work_queue[0].missing_families == ("SPY",)
    assert report.label_work_queue[0].missing_requirements[0].family_root == "SPY"
    assert report.label_work_queue[0].missing_requirements[0].approved_sources == (
        "nyse-arca-official",
    )

    with sqlite3.connect(database_path) as connection:
        connection.execute(
            """
            INSERT INTO eod_close_observations
                (symbol, trading_date, official_close, source, source_reference,
                 source_verified,
                 observed_at_utc, source_artifact_sha256)
            VALUES ('SPY', '2026-08-25', 1.0, ?, ?, 1,
                    '2026-08-25T21:00:00+00:00', ?)
            """
            , (SOURCES["SPY"], REFERENCES["SPY"], hashes["SPY"])
        )

    report = audit_training_readiness(tmp_path)
    assert report.verified_close_sessions == 1
    assert report.label_work_queue == ()


def test_model_gate_requires_verified_closes_on_the_same_capture_dates(
    tmp_path, monkeypatch
):
    _complete_catalog(tmp_path)
    monkeypatch.setattr(
        "backend.closing_tape.readiness.MODEL_SESSIONS_REQUIRED", 1
    )
    database_path = tmp_path / "data" / "market_data.db"
    with sqlite3.connect(database_path) as connection:
        connection.execute(
            """
            CREATE TABLE eod_close_observations (
                id INTEGER PRIMARY KEY,
                symbol TEXT NOT NULL,
                trading_date TEXT NOT NULL,
                official_close REAL NOT NULL,
                source TEXT NOT NULL,
                source_reference TEXT NOT NULL,
                source_verified INTEGER NOT NULL,
                observed_at_utc TEXT NOT NULL,
                source_artifact_sha256 TEXT
            )
            """
        )
        for trading_date, hash_offset in (("2026-08-26", 0),):
            hashes = {
                family: _write_close_artifact(tmp_path, trading_date, family)
                for family in FAMILIES
            }
            connection.executemany(
                """
                INSERT INTO eod_close_observations
                    (symbol, trading_date, official_close, source, source_reference,
                     source_verified,
                     observed_at_utc, source_artifact_sha256)
                VALUES (?, ?, ?, ?, ?, 1, '2026-08-26T21:00:00+00:00', ?)
                """,
                [
                    (
                        family,
                        trading_date,
                        float(index),
                        SOURCES[family],
                        REFERENCES[family],
                        hashes[family],
                    )
                    for index, family in enumerate(FAMILIES, start=1)
                ],
            )

    report = audit_training_readiness(tmp_path)
    assert report.verified_close_sessions == 1
    assert report.model_evidence_sessions == 0
    assert report.model_evidence_sources == 0
    assert not report.model_training_ready

    with sqlite3.connect(database_path) as connection:
        hashes = {
            family: _write_close_artifact(tmp_path, "2026-08-25", family)
            for family in FAMILIES
        }
        connection.executemany(
            """
            INSERT INTO eod_close_observations
                (symbol, trading_date, official_close, source, source_reference,
                 source_verified,
                 observed_at_utc, source_artifact_sha256)
            VALUES (?, '2026-08-25', ?, ?, ?, 1,
                    '2026-08-25T21:00:00+00:00', ?)
            """,
            [
                (
                    family, float(index), SOURCES[family], REFERENCES[family],
                    hashes[family],
                )
                for index, family in enumerate(FAMILIES, start=1)
            ],
        )

    report = audit_training_readiness(tmp_path)
    assert report.verified_close_sessions == 2
    assert report.model_evidence_sessions == 1
    assert report.model_evidence_sources == 1
    assert report.model_training_ready


def test_readiness_downgrades_ledger_hash_without_retained_artifact(tmp_path):
    _complete_catalog(tmp_path)
    database_path = tmp_path / "data" / "market_data.db"
    with sqlite3.connect(database_path) as connection:
        connection.execute(
            """
            CREATE TABLE eod_close_observations (
                id INTEGER PRIMARY KEY, symbol TEXT NOT NULL,
                trading_date TEXT NOT NULL, official_close REAL NOT NULL,
                source TEXT NOT NULL, source_reference TEXT NOT NULL,
                source_verified INTEGER NOT NULL, observed_at_utc TEXT NOT NULL,
                source_artifact_sha256 TEXT
            )
            """
        )
        connection.execute(
            """
            INSERT INTO eod_close_observations
                (symbol, trading_date, official_close, source, source_reference,
                 source_verified,
                 observed_at_utc, source_artifact_sha256)
            VALUES ('SPX', '2026-08-25', 1.0, ?, ?, 1,
                    '2026-08-25T21:00:00+00:00', ?)
            """,
            (SOURCES["SPX"], REFERENCES["SPX"], "a" * 64),
        )

    report = audit_training_readiness(tmp_path)

    assert report.label_work_queue[0].verified_families == ()
    assert "SPX" in report.label_work_queue[0].missing_families
    assert len(report.verified_close_artifact_issues) == 1
    assert report.verified_close_artifact_issues[0].startswith("2026-08-25/SPX:")


def test_readiness_rejects_same_day_capture_inflation(tmp_path):
    _complete_catalog(tmp_path, session_id="first", hash_character="a")
    _complete_catalog(tmp_path, session_id="retry", hash_character="b")

    report = audit_training_readiness(tmp_path)

    assert report.sessions == 2
    assert report.eligible_sessions == 0
    assert report.unique_verified_sources == 0
    assert all(not item.eligible for item in report.sessions_detail)
    assert all(
        any("multiple eligible captures share this trading date" in reason for reason in item.reasons)
        for item in report.sessions_detail
    )
