from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Iterator, Mapping, Sequence

from .config import FeedSpec, SessionConfig
from .contracts import (
    EVIDENCE_CONTRACT_VERSION,
    HISTORICAL_SOURCE_KIND,
    LIVE_SOURCE_KIND,
    evidence_contract_for_source_kind,
)


UTC = timezone.utc


SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS tape_sessions (
    session_id TEXT PRIMARY KEY,
    trading_date TEXT NOT NULL,
    created_at_utc TEXT NOT NULL,
    cash_open_utc TEXT NOT NULL,
    analysis_due_utc TEXT NOT NULL,
    cash_close_utc TEXT NOT NULL,
    stop_due_utc TEXT NOT NULL,
    status TEXT NOT NULL,
    config_json TEXT NOT NULL,
    completed_at_utc TEXT,
    error TEXT
);

CREATE TABLE IF NOT EXISTS tape_feed_status (
    session_id TEXT NOT NULL,
    feed_name TEXT NOT NULL,
    dataset TEXT NOT NULL,
    schemas_json TEXT NOT NULL,
    symbols_json TEXT NOT NULL,
    dbn_path TEXT NOT NULL,
    source_kind TEXT NOT NULL DEFAULT 'databento_live',
    evidence_contract_version TEXT NOT NULL DEFAULT 'tcbbo-observed-v7',
    operational_counters_applicable INTEGER NOT NULL DEFAULT 1,
    source_manifest_path TEXT,
    source_components_json TEXT NOT NULL DEFAULT '{}',
    status TEXT NOT NULL,
    started_at_utc TEXT,
    ended_at_utc TEXT,
    records_seen INTEGER NOT NULL DEFAULT 0,
    trade_records INTEGER NOT NULL DEFAULT 0,
    provisional_tcbbo_records INTEGER NOT NULL DEFAULT 0,
    provisional_tcbbo_timestamped_records INTEGER NOT NULL DEFAULT 0,
    provisional_tcbbo_valid_nbbo_records INTEGER NOT NULL DEFAULT 0,
    tcbbo_records INTEGER NOT NULL DEFAULT 0,
    tcbbo_timestamped_records INTEGER NOT NULL DEFAULT 0,
    tcbbo_valid_nbbo_records INTEGER NOT NULL DEFAULT 0,
    tcbbo_flagged_records INTEGER NOT NULL DEFAULT 0,
    tcbbo_action_counts_json TEXT NOT NULL DEFAULT '{}',
    mapping_records INTEGER NOT NULL DEFAULT 0,
    statistics_records INTEGER NOT NULL DEFAULT 0,
    definition_records INTEGER NOT NULL DEFAULT 0,
    unmapped_trade_records INTEGER NOT NULL DEFAULT 0,
    first_event_ns INTEGER,
    last_event_ns INTEGER,
    last_receive_ns INTEGER,
    file_bytes INTEGER NOT NULL DEFAULT 0,
    reconnect_count INTEGER NOT NULL DEFAULT 0,
    slow_reader_warnings INTEGER NOT NULL DEFAULT 0,
    provider_error_count INTEGER NOT NULL DEFAULT 0,
    subscription_acks INTEGER NOT NULL DEFAULT 0,
    expected_subscription_acks INTEGER NOT NULL DEFAULT 0,
    replay_completed INTEGER NOT NULL DEFAULT 0,
    last_trade_event_ns INTEGER,
    root_trade_watermarks_json TEXT NOT NULL DEFAULT '{}',
    root_trade_counts_json TEXT NOT NULL DEFAULT '{}',
    callback_queue_depth INTEGER NOT NULL DEFAULT 0,
    gaps_json TEXT NOT NULL DEFAULT '[]',
    complete INTEGER NOT NULL DEFAULT 0,
    sha256 TEXT,
    error TEXT,
    PRIMARY KEY (session_id, feed_name)
);

CREATE TABLE IF NOT EXISTS tape_instruments (
    session_id TEXT NOT NULL,
    feed_name TEXT NOT NULL,
    instrument_id INTEGER NOT NULL,
    raw_symbol TEXT NOT NULL,
    family_root TEXT,
    option_type TEXT,
    expiration TEXT,
    strike REAL,
    first_seen_utc TEXT NOT NULL,
    PRIMARY KEY (session_id, feed_name, instrument_id)
);

CREATE TABLE IF NOT EXISTS tape_instrument_definition_observations (
    observation_key TEXT PRIMARY KEY,
    session_id TEXT NOT NULL,
    feed_name TEXT NOT NULL,
    source_sha256 TEXT NOT NULL,
    record_offset INTEGER NOT NULL,
    record_bytes INTEGER NOT NULL,
    instrument_id INTEGER NOT NULL,
    ts_event_ns INTEGER NOT NULL,
    ts_recv_ns INTEGER,
    raw_symbol TEXT NOT NULL,
    family_root TEXT,
    option_type TEXT,
    expiration TEXT,
    strike REAL,
    security_update_action TEXT NOT NULL,
    raw_record_sha256 TEXT NOT NULL,
    raw_record BLOB NOT NULL,
    ingested_at_utc TEXT NOT NULL,
    UNIQUE(source_sha256, record_offset)
);

CREATE TRIGGER IF NOT EXISTS tape_instrument_definition_observations_no_update
BEFORE UPDATE ON tape_instrument_definition_observations
BEGIN SELECT RAISE(ABORT, 'instrument definition observations are immutable'); END;

CREATE TRIGGER IF NOT EXISTS tape_instrument_definition_observations_no_delete
BEFORE DELETE ON tape_instrument_definition_observations
BEGIN SELECT RAISE(ABORT, 'instrument definition observations are immutable'); END;

CREATE TABLE IF NOT EXISTS tape_open_interest (
    session_id TEXT NOT NULL,
    feed_name TEXT NOT NULL,
    instrument_id INTEGER NOT NULL,
    raw_symbol TEXT,
    family_root TEXT,
    asof_utc TEXT NOT NULL,
    available_at_utc TEXT,
    open_interest REAL NOT NULL,
    PRIMARY KEY (session_id, feed_name, instrument_id)
);

CREATE TABLE IF NOT EXISTS tape_open_interest_observations (
    observation_key TEXT PRIMARY KEY,
    session_id TEXT NOT NULL,
    feed_name TEXT NOT NULL,
    source_sha256 TEXT NOT NULL,
    record_offset INTEGER NOT NULL,
    instrument_id INTEGER NOT NULL,
    raw_symbol TEXT,
    family_root TEXT,
    ts_event_ns INTEGER NOT NULL,
    ts_recv_ns INTEGER,
    ts_ref_ns INTEGER,
    asof_utc TEXT NOT NULL,
    sequence INTEGER NOT NULL,
    channel_id INTEGER NOT NULL,
    update_action INTEGER NOT NULL,
    stat_flags INTEGER NOT NULL,
    open_interest REAL,
    ingested_at_utc TEXT NOT NULL,
    UNIQUE(source_sha256, record_offset)
);

CREATE TABLE IF NOT EXISTS tape_observed_minute (
    session_id TEXT NOT NULL,
    feed_name TEXT NOT NULL,
    family_root TEXT NOT NULL,
    minute_utc TEXT NOT NULL,
    asset_class TEXT NOT NULL,
    trade_count INTEGER NOT NULL,
    volume REAL NOT NULL,
    notional REAL NOT NULL,
    call_count INTEGER NOT NULL,
    put_count INTEGER NOT NULL,
    call_volume REAL NOT NULL,
    put_volume REAL NOT NULL,
    call_premium REAL NOT NULL,
    put_premium REAL NOT NULL,
    first_price REAL,
    high_price REAL,
    low_price REAL,
    last_price REAL,
    price_volume_sum REAL NOT NULL,
    largest_trade_size REAL NOT NULL,
    largest_trade_notional REAL NOT NULL,
    nbbo_valid_count INTEGER NOT NULL DEFAULT 0,
    quoted_spread_sum REAL NOT NULL DEFAULT 0,
    quoted_spread_bps_sum REAL NOT NULL DEFAULT 0,
    trade_to_mid_abs_sum REAL NOT NULL DEFAULT 0,
    trade_to_mid_signed_sum REAL NOT NULL DEFAULT 0,
    bid_size_sum REAL NOT NULL DEFAULT 0,
    ask_size_sum REAL NOT NULL DEFAULT 0,
    receive_lag_ns_sum INTEGER NOT NULL DEFAULT 0,
    receive_lag_ns_max INTEGER,
    flag_last_count INTEGER NOT NULL DEFAULT 0,
    flag_tob_count INTEGER NOT NULL DEFAULT 0,
    flag_snapshot_count INTEGER NOT NULL DEFAULT 0,
    flag_mbp_count INTEGER NOT NULL DEFAULT 0,
    flag_bad_ts_recv_count INTEGER NOT NULL DEFAULT 0,
    flag_maybe_bad_book_count INTEGER NOT NULL DEFAULT 0,
    flag_publisher_specific_count INTEGER NOT NULL DEFAULT 0,
    data_quality_flagged_count INTEGER NOT NULL DEFAULT 0,
    updated_at_utc TEXT NOT NULL,
    PRIMARY KEY (session_id, feed_name, family_root, minute_utc)
);

CREATE TABLE IF NOT EXISTS tape_inferred_minute_flow (
    session_id TEXT NOT NULL,
    feed_name TEXT NOT NULL,
    family_root TEXT NOT NULL,
    minute_utc TEXT NOT NULL,
    inference_method TEXT NOT NULL,
    inference_version TEXT NOT NULL,
    source_sha256 TEXT NOT NULL,
    at_ask_count INTEGER NOT NULL,
    at_bid_count INTEGER NOT NULL,
    inside_count INTEGER NOT NULL,
    unknown_count INTEGER NOT NULL,
    at_ask_volume REAL NOT NULL,
    at_bid_volume REAL NOT NULL,
    inside_volume REAL NOT NULL,
    unknown_volume REAL NOT NULL,
    at_ask_notional REAL NOT NULL,
    at_bid_notional REAL NOT NULL,
    inside_notional REAL NOT NULL,
    unknown_notional REAL NOT NULL,
    call_at_ask_notional REAL NOT NULL,
    call_at_bid_notional REAL NOT NULL,
    put_at_ask_notional REAL NOT NULL,
    put_at_bid_notional REAL NOT NULL,
    updated_at_utc TEXT NOT NULL,
    PRIMARY KEY (
        session_id, feed_name, family_root, minute_utc,
        inference_method, inference_version
    )
);

CREATE TABLE IF NOT EXISTS tape_observed_contract_minute (
    session_id TEXT NOT NULL,
    feed_name TEXT NOT NULL,
    family_root TEXT NOT NULL,
    raw_symbol TEXT NOT NULL,
    expiration TEXT NOT NULL,
    option_type TEXT NOT NULL,
    strike REAL NOT NULL,
    minute_utc TEXT NOT NULL,
    trade_count INTEGER NOT NULL,
    volume REAL NOT NULL,
    notional REAL NOT NULL,
    first_price REAL,
    high_price REAL,
    low_price REAL,
    last_price REAL,
    price_volume_sum REAL NOT NULL,
    nbbo_valid_count INTEGER NOT NULL,
    quoted_spread_sum REAL NOT NULL,
    quoted_spread_bps_sum REAL NOT NULL,
    trade_to_mid_abs_sum REAL NOT NULL,
    trade_to_mid_signed_sum REAL NOT NULL,
    bid_size_sum REAL NOT NULL,
    ask_size_sum REAL NOT NULL,
    receive_lag_ns_sum INTEGER NOT NULL,
    receive_lag_ns_max INTEGER,
    data_quality_flagged_count INTEGER NOT NULL,
    last_pretrade_midpoint REAL,
    last_nbbo_event_ns INTEGER,
    updated_at_utc TEXT NOT NULL,
    PRIMARY KEY (session_id, feed_name, raw_symbol, minute_utc)
);

CREATE TABLE IF NOT EXISTS tape_inferred_contract_minute_flow (
    session_id TEXT NOT NULL,
    feed_name TEXT NOT NULL,
    family_root TEXT NOT NULL,
    raw_symbol TEXT NOT NULL,
    expiration TEXT NOT NULL,
    option_type TEXT NOT NULL,
    strike REAL NOT NULL,
    minute_utc TEXT NOT NULL,
    inference_method TEXT NOT NULL,
    inference_version TEXT NOT NULL,
    source_sha256 TEXT NOT NULL,
    at_ask_count INTEGER NOT NULL,
    at_bid_count INTEGER NOT NULL,
    inside_count INTEGER NOT NULL,
    unknown_count INTEGER NOT NULL,
    at_ask_volume REAL NOT NULL,
    at_bid_volume REAL NOT NULL,
    inside_volume REAL NOT NULL,
    unknown_volume REAL NOT NULL,
    at_ask_notional REAL NOT NULL,
    at_bid_notional REAL NOT NULL,
    inside_notional REAL NOT NULL,
    unknown_notional REAL NOT NULL,
    updated_at_utc TEXT NOT NULL,
    PRIMARY KEY (
        session_id, feed_name, raw_symbol, minute_utc,
        inference_method, inference_version
    )
);

CREATE TABLE IF NOT EXISTS tape_inferred_reference_minute (
    session_id TEXT NOT NULL,
    feed_name TEXT NOT NULL,
    family_root TEXT NOT NULL,
    minute_utc TEXT NOT NULL,
    reference_method TEXT NOT NULL,
    reference_version TEXT NOT NULL,
    source_sha256 TEXT NOT NULL,
    estimated_price REAL NOT NULL,
    available_at_utc TEXT NOT NULL,
    pair_count INTEGER NOT NULL,
    dispersion_bps REAL NOT NULL,
    parameters_json TEXT NOT NULL,
    updated_at_utc TEXT NOT NULL,
    PRIMARY KEY (
        session_id, feed_name, family_root, minute_utc,
        reference_method, reference_version
    )
);

CREATE INDEX IF NOT EXISTS ix_tape_observed_minute_session_time
    ON tape_observed_minute(session_id, minute_utc);
CREATE INDEX IF NOT EXISTS ix_tape_inferred_minute_session_time
    ON tape_inferred_minute_flow(session_id, minute_utc);
CREATE INDEX IF NOT EXISTS ix_tape_observed_contract_session_surface
    ON tape_observed_contract_minute(session_id, family_root, expiration, strike, minute_utc);
CREATE INDEX IF NOT EXISTS ix_tape_inferred_contract_session_surface
    ON tape_inferred_contract_minute_flow(session_id, family_root, expiration, strike, minute_utc);
CREATE INDEX IF NOT EXISTS ix_tape_inferred_reference_session_time
    ON tape_inferred_reference_minute(session_id, family_root, minute_utc);
CREATE INDEX IF NOT EXISTS ix_tape_oi_session_root
    ON tape_open_interest(session_id, family_root);
CREATE INDEX IF NOT EXISTS ix_tape_oi_observations_session_root
    ON tape_open_interest_observations(session_id, family_root, record_offset);
CREATE INDEX IF NOT EXISTS ix_tape_definition_observations_session_instrument
    ON tape_instrument_definition_observations(session_id, feed_name, instrument_id, record_offset);

CREATE TABLE IF NOT EXISTS closing_analysis_runs (
    session_id TEXT NOT NULL,
    horizon_id TEXT NOT NULL,
    trading_date TEXT NOT NULL,
    asof_utc TEXT NOT NULL,
    created_at_utc TEXT NOT NULL,
    decision_state TEXT NOT NULL,
    abstention_reasons_json TEXT NOT NULL,
    feature_hash TEXT NOT NULL,
    source_manifest_json TEXT NOT NULL,
    report_json TEXT NOT NULL,
    json_path TEXT,
    markdown_path TEXT,
    PRIMARY KEY (session_id, horizon_id)
);

CREATE TABLE IF NOT EXISTS tape_analysis_cutoffs (
    session_id TEXT NOT NULL,
    feed_name TEXT NOT NULL,
    horizon_id TEXT NOT NULL,
    captured_at_utc TEXT NOT NULL,
    event_cutoff_utc TEXT NOT NULL,
    cutoff_bytes INTEGER NOT NULL,
    record_sequence INTEGER NOT NULL DEFAULT 0,
    processed_sequence INTEGER NOT NULL DEFAULT 0,
    prefix_sha256 TEXT NOT NULL,
    last_trade_event_ns INTEGER,
    final_file_sha256 TEXT,
    finalized_at_utc TEXT,
    PRIMARY KEY (session_id, feed_name, horizon_id)
);

CREATE TABLE IF NOT EXISTS tape_finalization_runs (
    run_key TEXT PRIMARY KEY,
    session_id TEXT NOT NULL,
    feed_name TEXT NOT NULL,
    source_sha256 TEXT NOT NULL,
    evidence_contract_version TEXT NOT NULL,
    attempted_at_utc TEXT NOT NULL,
    complete INTEGER NOT NULL,
    issues_json TEXT NOT NULL,
    prior_status TEXT,
    prior_error TEXT,
    prior_gaps_json TEXT,
    report_json TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_tape_finalization_runs_session
    ON tape_finalization_runs(session_id, feed_name, attempted_at_utc);
"""


def _utcnow() -> str:
    return datetime.now(UTC).isoformat()


class TapeCatalog:
    def __init__(self, path: str | Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.initialize()

    @contextmanager
    def connect(self, *, read_only: bool = False) -> Iterator[sqlite3.Connection]:
        if read_only:
            connection = sqlite3.connect(f"file:{self.path}?mode=ro", uri=True, timeout=10.0)
        else:
            connection = sqlite3.connect(self.path, timeout=10.0)
        connection.row_factory = sqlite3.Row
        if not read_only:
            connection.execute("PRAGMA journal_mode=WAL")
            connection.execute("PRAGMA synchronous=NORMAL")
            connection.execute("PRAGMA busy_timeout=10000")
        try:
            yield connection
            if not read_only:
                connection.commit()
        finally:
            connection.close()

    def initialize(self) -> None:
        with self.connect() as connection:
            connection.executescript(SCHEMA_SQL)
            # Additive migration for catalogs created by an earlier development
            # build. The production path is append-only from this point forward.
            feed_columns = {
                str(row[1]) for row in connection.execute("PRAGMA table_info(tape_feed_status)")
            }
            additions = {
                "source_kind": "TEXT NOT NULL DEFAULT 'databento_live'",
                "evidence_contract_version": (
                    "TEXT NOT NULL DEFAULT 'tcbbo-observed-v7'"
                ),
                "operational_counters_applicable": "INTEGER NOT NULL DEFAULT 1",
                "source_manifest_path": "TEXT",
                "source_components_json": "TEXT NOT NULL DEFAULT '{}'",
                "subscription_acks": "INTEGER NOT NULL DEFAULT 0",
                "provider_error_count": "INTEGER NOT NULL DEFAULT 0",
                "expected_subscription_acks": "INTEGER NOT NULL DEFAULT 0",
                "replay_completed": "INTEGER NOT NULL DEFAULT 0",
                "last_trade_event_ns": "INTEGER",
                "root_trade_watermarks_json": "TEXT NOT NULL DEFAULT '{}'",
                "root_trade_counts_json": "TEXT NOT NULL DEFAULT '{}'",
                "callback_queue_depth": "INTEGER NOT NULL DEFAULT 0",
                "provisional_tcbbo_records": "INTEGER NOT NULL DEFAULT 0",
                "provisional_tcbbo_timestamped_records": "INTEGER NOT NULL DEFAULT 0",
                "provisional_tcbbo_valid_nbbo_records": "INTEGER NOT NULL DEFAULT 0",
                "tcbbo_records": "INTEGER NOT NULL DEFAULT 0",
                "tcbbo_timestamped_records": "INTEGER NOT NULL DEFAULT 0",
                "tcbbo_valid_nbbo_records": "INTEGER NOT NULL DEFAULT 0",
                "tcbbo_flagged_records": "INTEGER NOT NULL DEFAULT 0",
                "tcbbo_action_counts_json": "TEXT NOT NULL DEFAULT '{}'",
            }
            for column, definition in additions.items():
                if column not in feed_columns:
                    connection.execute(
                        f"ALTER TABLE tape_feed_status ADD COLUMN {column} {definition}"
                    )
            observed_columns = {
                str(row[1]) for row in connection.execute("PRAGMA table_info(tape_observed_minute)")
            }
            observed_additions = {
                "nbbo_valid_count": "INTEGER NOT NULL DEFAULT 0",
                "quoted_spread_sum": "REAL NOT NULL DEFAULT 0",
                "quoted_spread_bps_sum": "REAL NOT NULL DEFAULT 0",
                "trade_to_mid_abs_sum": "REAL NOT NULL DEFAULT 0",
                "trade_to_mid_signed_sum": "REAL NOT NULL DEFAULT 0",
                "bid_size_sum": "REAL NOT NULL DEFAULT 0",
                "ask_size_sum": "REAL NOT NULL DEFAULT 0",
                "receive_lag_ns_sum": "INTEGER NOT NULL DEFAULT 0",
                "receive_lag_ns_max": "INTEGER",
                "flag_last_count": "INTEGER NOT NULL DEFAULT 0",
                "flag_tob_count": "INTEGER NOT NULL DEFAULT 0",
                "flag_snapshot_count": "INTEGER NOT NULL DEFAULT 0",
                "flag_mbp_count": "INTEGER NOT NULL DEFAULT 0",
                "flag_bad_ts_recv_count": "INTEGER NOT NULL DEFAULT 0",
                "flag_maybe_bad_book_count": "INTEGER NOT NULL DEFAULT 0",
                "flag_publisher_specific_count": "INTEGER NOT NULL DEFAULT 0",
                "data_quality_flagged_count": "INTEGER NOT NULL DEFAULT 0",
            }
            for column, definition in observed_additions.items():
                if column not in observed_columns:
                    connection.execute(
                        f"ALTER TABLE tape_observed_minute ADD COLUMN {column} {definition}"
                    )
            contract_columns = {
                str(row[1])
                for row in connection.execute("PRAGMA table_info(tape_observed_contract_minute)")
            }
            contract_additions = {
                "last_pretrade_midpoint": "REAL",
                "last_nbbo_event_ns": "INTEGER",
            }
            for column, definition in contract_additions.items():
                if column not in contract_columns:
                    connection.execute(
                        f"ALTER TABLE tape_observed_contract_minute ADD COLUMN {column} {definition}"
                    )
            oi_columns = {
                str(row[1])
                for row in connection.execute("PRAGMA table_info(tape_open_interest)")
            }
            if "available_at_utc" not in oi_columns:
                connection.execute(
                    "ALTER TABLE tape_open_interest ADD COLUMN available_at_utc TEXT"
                )
            # Early builds keyed OI by (instrument, asof), which allowed the
            # analysis to sum repeated updates. Retain only each instrument's
            # latest state and enforce the canonical one-row key.
            connection.execute(
                """
                DELETE FROM tape_open_interest
                WHERE rowid IN (
                    SELECT rowid FROM (
                        SELECT rowid,
                               ROW_NUMBER() OVER (
                                   PARTITION BY session_id, feed_name, instrument_id
                                   ORDER BY asof_utc DESC, rowid DESC
                               ) AS duplicate_rank
                        FROM tape_open_interest
                    ) WHERE duplicate_rank > 1
                )
                """
            )
            connection.execute(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS ux_tape_oi_latest
                ON tape_open_interest(session_id, feed_name, instrument_id)
                """
            )
            cutoff_columns = {
                str(row[1]) for row in connection.execute("PRAGMA table_info(tape_analysis_cutoffs)")
            }
            if "record_sequence" not in cutoff_columns:
                connection.execute(
                    "ALTER TABLE tape_analysis_cutoffs ADD COLUMN record_sequence INTEGER NOT NULL DEFAULT 0"
                )
            if "processed_sequence" not in cutoff_columns:
                connection.execute(
                    "ALTER TABLE tape_analysis_cutoffs ADD COLUMN processed_sequence INTEGER NOT NULL DEFAULT 0"
                )

    def start_session(self, config: SessionConfig) -> None:
        with self.connect() as connection:
            connection.execute(
                """
                INSERT INTO tape_sessions (
                    session_id, trading_date, created_at_utc, cash_open_utc,
                    analysis_due_utc, cash_close_utc, stop_due_utc, status,
                    config_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, 'running', ?)
                """,
                (
                    config.session_id,
                    config.trading_date.isoformat(),
                    _utcnow(),
                    config.cash_open_utc.isoformat(),
                    config.analysis_due_utc.isoformat(),
                    config.cash_close_utc.isoformat(),
                    config.stop_due_utc.isoformat(),
                    config.to_json(),
                ),
            )

    def finish_session(self, session_id: str, status: str, error: str | None = None) -> None:
        with self.connect() as connection:
            connection.execute(
                """
                UPDATE tape_sessions
                SET status=?, completed_at_utc=?, error=?
                WHERE session_id=?
                """,
                (status, _utcnow(), error, session_id),
            )

    def abandon_running_sessions(
        self,
        *,
        recovery_session_id: str,
        reason: str | None = None,
    ) -> list[str]:
        """Close orphaned sessions after the exclusive process lock is held.

        Raw DBN files remain untouched. Only stale lifecycle projections are
        made truthful and explicitly incomplete.
        """
        observed_at = _utcnow()
        message = reason or f"orphaned recorder session recovered by {recovery_session_id}"
        with self.connect() as connection:
            orphaned = [
                str(row[0])
                for row in connection.execute(
                    """
                    SELECT session_id FROM tape_sessions
                    WHERE status='running' AND session_id<>?
                    ORDER BY created_at_utc
                    """,
                    (recovery_session_id,),
                ).fetchall()
            ]
            if not orphaned:
                return []
            placeholders = ",".join("?" for _ in orphaned)
            connection.execute(
                f"""
                UPDATE tape_sessions
                SET status='incomplete', completed_at_utc=?,
                    error=CASE
                        WHEN error IS NULL OR error='' THEN ?
                        ELSE error || '; ' || ?
                    END
                WHERE session_id IN ({placeholders}) AND status='running'
                """,
                (observed_at, message, message, *orphaned),
            )
            connection.execute(
                f"""
                UPDATE tape_feed_status
                SET status='incomplete', complete=0, ended_at_utc=?,
                    error=CASE
                        WHEN error IS NULL OR error='' THEN ?
                        ELSE error || '; ' || ?
                    END
                WHERE session_id IN ({placeholders})
                  AND status IN ('starting','running')
                """,
                (observed_at, message, message, *orphaned),
            )
            return orphaned

    def register_feed(
        self,
        session_id: str,
        feed: FeedSpec,
        dbn_path: Path,
        *,
        source_kind: str = LIVE_SOURCE_KIND,
        evidence_contract_version: str = EVIDENCE_CONTRACT_VERSION,
        operational_counters_applicable: bool = True,
        source_manifest_path: Path | None = None,
        source_components: Mapping[str, object] | None = None,
    ) -> None:
        normalized_source_kind = str(source_kind or "").strip().lower()
        expected_contract = evidence_contract_for_source_kind(normalized_source_kind)
        if str(evidence_contract_version) != expected_contract:
            raise ValueError("evidence_contract_version does not match source_kind")
        if (
            normalized_source_kind == HISTORICAL_SOURCE_KIND
            and operational_counters_applicable
        ):
            raise ValueError("live operational counters do not apply to historical sources")
        if normalized_source_kind == HISTORICAL_SOURCE_KIND and source_manifest_path is None:
            raise ValueError("historical sources require source_manifest_path")
        schemas = [subscription.schema for subscription in feed.subscriptions]
        symbols = sorted({symbol for subscription in feed.subscriptions for symbol in subscription.symbols})
        with self.connect() as connection:
            connection.execute(
                """
                INSERT OR IGNORE INTO tape_feed_status (
                    session_id, feed_name, dataset, schemas_json, symbols_json,
                    dbn_path, source_kind, evidence_contract_version,
                    operational_counters_applicable, source_manifest_path,
                    source_components_json, status, complete
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'starting', 0)
                """,
                (
                    session_id,
                    feed.name,
                    feed.dataset,
                    json.dumps(schemas, separators=(",", ":")),
                    json.dumps(symbols, separators=(",", ":")),
                    str(dbn_path),
                    normalized_source_kind,
                    str(evidence_contract_version),
                    int(bool(operational_counters_applicable)),
                    str(source_manifest_path) if source_manifest_path is not None else None,
                    json.dumps(
                        dict(source_components or {}),
                        sort_keys=True,
                        separators=(",", ":"),
                    ),
                ),
            )

    def update_feed_status(self, session_id: str, feed_name: str, values: Mapping[str, object]) -> None:
        allowed = {
            "source_kind",
            "evidence_contract_version",
            "operational_counters_applicable",
            "source_manifest_path",
            "source_components_json",
            "status",
            "started_at_utc",
            "ended_at_utc",
            "records_seen",
            "trade_records",
            "provisional_tcbbo_records",
            "provisional_tcbbo_timestamped_records",
            "provisional_tcbbo_valid_nbbo_records",
            "tcbbo_records",
            "tcbbo_timestamped_records",
            "tcbbo_valid_nbbo_records",
            "tcbbo_flagged_records",
            "tcbbo_action_counts_json",
            "mapping_records",
            "statistics_records",
            "definition_records",
            "unmapped_trade_records",
            "first_event_ns",
            "last_event_ns",
            "last_receive_ns",
            "file_bytes",
            "reconnect_count",
            "slow_reader_warnings",
            "provider_error_count",
            "subscription_acks",
            "expected_subscription_acks",
            "replay_completed",
            "last_trade_event_ns",
            "root_trade_watermarks_json",
            "root_trade_counts_json",
            "callback_queue_depth",
            "gaps_json",
            "complete",
            "sha256",
            "error",
        }
        clean = {key: value for key, value in values.items() if key in allowed}
        if not clean:
            return
        assignments = ", ".join(f"{key}=?" for key in clean)
        params = [clean[key] for key in clean] + [session_id, feed_name]
        with self.connect() as connection:
            connection.execute(
                f"UPDATE tape_feed_status SET {assignments} WHERE session_id=? AND feed_name=?",
                params,
            )

    def upsert_instruments(self, rows: Sequence[Mapping[str, object]]) -> None:
        if not rows:
            return
        values = [
            (
                row["session_id"], row["feed_name"], row["instrument_id"], row["raw_symbol"],
                row.get("family_root"), row.get("option_type"), row.get("expiration"),
                row.get("strike"), row.get("first_seen_utc") or _utcnow(),
            )
            for row in rows
        ]
        with self.connect() as connection:
            connection.executemany(
                """
                INSERT INTO tape_instruments (
                    session_id, feed_name, instrument_id, raw_symbol, family_root,
                    option_type, expiration, strike, first_seen_utc
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(session_id, feed_name, instrument_id) DO UPDATE SET
                    raw_symbol=excluded.raw_symbol,
                    family_root=excluded.family_root,
                    option_type=excluded.option_type,
                    expiration=excluded.expiration,
                    strike=excluded.strike
                """,
                values,
            )

    def rebuild_instruments_from_definition_observations(
        self,
        *,
        session_id: str,
        feed_name: str,
        source_sha256: str,
    ) -> int:
        """Rebuild the mutable instrument projection from immutable definitions."""

        with self.connect() as connection:
            connection.execute(
                "DELETE FROM tape_instruments WHERE session_id=? AND feed_name=?",
                (session_id, feed_name),
            )
            connection.execute(
                """
                INSERT INTO tape_instruments (
                    session_id, feed_name, instrument_id, raw_symbol, family_root,
                    option_type, expiration, strike, first_seen_utc
                )
                SELECT session_id, feed_name, instrument_id, raw_symbol, family_root,
                       option_type, expiration, strike, ingested_at_utc
                FROM (
                    SELECT observations.*,
                           ROW_NUMBER() OVER (
                               PARTITION BY session_id, feed_name, instrument_id
                               ORDER BY record_offset DESC
                           ) AS latest_rank
                    FROM tape_instrument_definition_observations observations
                    WHERE session_id=? AND feed_name=? AND source_sha256=?
                )
                WHERE latest_rank=1 AND family_root IS NOT NULL
                  AND security_update_action<>'D'
                """,
                (session_id, feed_name, source_sha256),
            )
            row = connection.execute(
                """
                SELECT COUNT(*) FROM tape_instruments
                WHERE session_id=? AND feed_name=?
                """,
                (session_id, feed_name),
            ).fetchone()
            return int(row[0] if row else 0)

    def upsert_open_interest(self, rows: Sequence[Mapping[str, object]]) -> None:
        if not rows:
            return
        values = [
            (
                row["session_id"], row["feed_name"], row["instrument_id"],
                row.get("raw_symbol"), row.get("family_root"), row["asof_utc"],
                row.get("available_at_utc"),
                row["open_interest"],
            )
            for row in rows
        ]
        with self.connect() as connection:
            connection.executemany(
                """
                INSERT INTO tape_open_interest (
                    session_id, feed_name, instrument_id, raw_symbol, family_root,
                    asof_utc, available_at_utc, open_interest
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(session_id, feed_name, instrument_id) DO UPDATE SET
                    raw_symbol=excluded.raw_symbol,
                    family_root=excluded.family_root,
                    asof_utc=excluded.asof_utc,
                    available_at_utc=excluded.available_at_utc,
                    open_interest=excluded.open_interest
                """,
                values,
            )

    def delete_open_interest(
        self,
        session_id: str,
        feed_name: str,
        instrument_ids: Sequence[int],
    ) -> None:
        if not instrument_ids:
            return
        with self.connect() as connection:
            connection.executemany(
                """
                DELETE FROM tape_open_interest
                WHERE session_id=? AND feed_name=? AND instrument_id=?
                """,
                [(session_id, feed_name, int(instrument_id)) for instrument_id in instrument_ids],
            )

    def persist_live_snapshot(
        self,
        *,
        instruments: Sequence[Mapping[str, object]],
        deleted_open_interest: Sequence[int],
        open_interest: Sequence[Mapping[str, object]],
        observed: Sequence[Mapping[str, object]],
        inferred: Sequence[Mapping[str, object]],
        session_id: str,
        feed_name: str,
    ) -> None:
        """Commit one internally consistent live-feature generation."""
        observed_columns = [
            "session_id", "feed_name", "family_root", "minute_utc", "asset_class",
            "trade_count", "volume", "notional", "call_count", "put_count",
            "call_volume", "put_volume", "call_premium", "put_premium",
            "first_price", "high_price", "low_price", "last_price",
            "price_volume_sum", "largest_trade_size", "largest_trade_notional", "updated_at_utc",
            "nbbo_valid_count", "quoted_spread_sum", "quoted_spread_bps_sum",
            "trade_to_mid_abs_sum", "trade_to_mid_signed_sum", "bid_size_sum",
            "ask_size_sum", "receive_lag_ns_sum", "receive_lag_ns_max",
            "flag_last_count", "flag_tob_count", "flag_snapshot_count", "flag_mbp_count",
            "flag_bad_ts_recv_count", "flag_maybe_bad_book_count",
            "flag_publisher_specific_count", "data_quality_flagged_count",
        ]
        inferred_columns = [
            "session_id", "feed_name", "family_root", "minute_utc",
            "inference_method", "inference_version", "source_sha256",
            "at_ask_count", "at_bid_count", "inside_count", "unknown_count",
            "at_ask_volume", "at_bid_volume", "inside_volume", "unknown_volume",
            "at_ask_notional", "at_bid_notional", "inside_notional", "unknown_notional",
            "call_at_ask_notional", "call_at_bid_notional", "put_at_ask_notional",
            "put_at_bid_notional", "updated_at_utc",
        ]
        with self.connect() as connection:
            if instruments:
                connection.executemany(
                    """
                    INSERT INTO tape_instruments (
                        session_id, feed_name, instrument_id, raw_symbol, family_root,
                        option_type, expiration, strike, first_seen_utc
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(session_id, feed_name, instrument_id) DO UPDATE SET
                        raw_symbol=excluded.raw_symbol,
                        family_root=excluded.family_root,
                        option_type=excluded.option_type,
                        expiration=excluded.expiration,
                        strike=excluded.strike
                    """,
                    [
                        (
                            row["session_id"], row["feed_name"], row["instrument_id"],
                            row["raw_symbol"], row.get("family_root"), row.get("option_type"),
                            row.get("expiration"), row.get("strike"),
                            row.get("first_seen_utc") or _utcnow(),
                        )
                        for row in instruments
                    ],
                )

            if deleted_open_interest:
                connection.executemany(
                    """
                    DELETE FROM tape_open_interest
                    WHERE session_id=? AND feed_name=? AND instrument_id=?
                    """,
                    [
                        (session_id, feed_name, int(instrument_id))
                        for instrument_id in deleted_open_interest
                    ],
                )
            if open_interest:
                connection.executemany(
                    """
                    INSERT INTO tape_open_interest (
                        session_id, feed_name, instrument_id, raw_symbol, family_root,
                        asof_utc, available_at_utc, open_interest
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(session_id, feed_name, instrument_id) DO UPDATE SET
                        raw_symbol=excluded.raw_symbol,
                        family_root=excluded.family_root,
                        asof_utc=excluded.asof_utc,
                        available_at_utc=excluded.available_at_utc,
                        open_interest=excluded.open_interest
                    """,
                    [
                        (
                            row["session_id"], row["feed_name"], row["instrument_id"],
                            row.get("raw_symbol"), row.get("family_root"), row["asof_utc"],
                            row.get("available_at_utc"),
                            row["open_interest"],
                        )
                        for row in open_interest
                    ],
                )
            if observed:
                placeholders = ",".join("?" for _ in observed_columns)
                updates = ",".join(
                    f"{column}=excluded.{column}"
                    for column in observed_columns
                    if column not in {"session_id", "feed_name", "family_root", "minute_utc"}
                )
                connection.executemany(
                    f"""
                    INSERT INTO tape_observed_minute ({','.join(observed_columns)})
                    VALUES ({placeholders})
                    ON CONFLICT(session_id, feed_name, family_root, minute_utc)
                    DO UPDATE SET {updates}
                    """,
                    [tuple(row.get(column) for column in observed_columns) for row in observed],
                )
            if inferred:
                placeholders = ",".join("?" for _ in inferred_columns)
                keys = {
                    "session_id", "feed_name", "family_root", "minute_utc",
                    "inference_method", "inference_version",
                }
                updates = ",".join(
                    f"{column}=excluded.{column}" for column in inferred_columns if column not in keys
                )
                connection.executemany(
                    f"""
                    INSERT INTO tape_inferred_minute_flow ({','.join(inferred_columns)})
                    VALUES ({placeholders})
                    ON CONFLICT(
                        session_id, feed_name, family_root, minute_utc,
                        inference_method, inference_version
                    ) DO UPDATE SET {updates}
                    """,
                    [tuple(row.get(column) for column in inferred_columns) for row in inferred],
                )

    def persist_open_interest_replay(
        self,
        *,
        session_id: str,
        feed_name: str,
        observations: Sequence[Mapping[str, object]],
    ) -> int:
        """Append immutable OI records and rebuild their latest projection."""
        inserted = self.append_open_interest_observations(observations)
        self.rebuild_open_interest_projection(session_id=session_id, feed_name=feed_name)
        return inserted

    def append_open_interest_observations(
        self,
        observations: Sequence[Mapping[str, object]],
    ) -> int:
        if not observations:
            return 0
        columns = (
            "observation_key", "session_id", "feed_name", "source_sha256",
            "record_offset", "instrument_id", "raw_symbol", "family_root",
            "ts_event_ns", "ts_recv_ns", "ts_ref_ns", "asof_utc", "sequence",
            "channel_id", "update_action", "stat_flags", "open_interest",
            "ingested_at_utc",
        )
        values = [tuple(row.get(column) for column in columns) for row in observations]
        with self.connect() as connection:
            connection.executemany(
                f"""
                INSERT OR IGNORE INTO tape_open_interest_observations
                    ({','.join(columns)})
                VALUES ({','.join('?' for _ in columns)})
                """,
                values,
            )
        return len(observations)

    def append_instrument_definition_observations(
        self,
        observations: Sequence[Mapping[str, object]],
    ) -> int:
        if not observations:
            return 0
        columns = (
            "observation_key", "session_id", "feed_name", "source_sha256",
            "record_offset", "record_bytes", "instrument_id", "ts_event_ns",
            "ts_recv_ns", "raw_symbol", "family_root", "option_type", "expiration",
            "strike", "security_update_action", "raw_record_sha256", "raw_record",
            "ingested_at_utc",
        )
        values = [tuple(row.get(column) for column in columns) for row in observations]
        with self.connect() as connection:
            before = connection.total_changes
            connection.executemany(
                f"""
                INSERT OR IGNORE INTO tape_instrument_definition_observations
                    ({','.join(columns)})
                VALUES ({','.join('?' for _ in columns)})
                """,
                values,
            )
            return int(connection.total_changes - before)

    def rebuild_open_interest_projection(self, *, session_id: str, feed_name: str) -> None:
        with self.connect() as connection:
            connection.execute(
                "DELETE FROM tape_open_interest WHERE session_id=? AND feed_name=?",
                (session_id, feed_name),
            )
            connection.execute(
                """
                INSERT INTO tape_open_interest (
                    session_id, feed_name, instrument_id, raw_symbol,
                    family_root, asof_utc, available_at_utc, open_interest
                )
                SELECT session_id, feed_name, instrument_id, raw_symbol,
                       family_root, asof_utc,
                       CASE
                         WHEN ts_recv_ns IS NOT NULL THEN
                           strftime(
                             '%Y-%m-%dT%H:%M:%f+00:00',
                             ts_recv_ns / 1000000000.0,
                             'unixepoch'
                           )
                       END,
                       open_interest
                FROM (
                    SELECT observations.*,
                           ROW_NUMBER() OVER (
                               PARTITION BY session_id, feed_name, instrument_id
                               ORDER BY record_offset DESC
                           ) AS latest_rank
                    FROM tape_open_interest_observations observations
                    WHERE session_id=? AND feed_name=?
                )
                WHERE latest_rank=1 AND update_action=1
                  AND open_interest IS NOT NULL AND open_interest>=0
                """,
                (session_id, feed_name),
            )

    def upsert_observed_minutes(self, rows: Sequence[Mapping[str, object]]) -> None:
        if not rows:
            return
        columns = [
            "session_id", "feed_name", "family_root", "minute_utc", "asset_class",
            "trade_count", "volume", "notional", "call_count", "put_count",
            "call_volume", "put_volume", "call_premium", "put_premium",
            "first_price", "high_price", "low_price", "last_price",
            "price_volume_sum", "largest_trade_size", "largest_trade_notional",
            "nbbo_valid_count", "quoted_spread_sum", "quoted_spread_bps_sum",
            "trade_to_mid_abs_sum", "trade_to_mid_signed_sum", "bid_size_sum",
            "ask_size_sum", "receive_lag_ns_sum", "receive_lag_ns_max",
            "flag_last_count", "flag_tob_count", "flag_snapshot_count", "flag_mbp_count",
            "flag_bad_ts_recv_count", "flag_maybe_bad_book_count",
            "flag_publisher_specific_count", "data_quality_flagged_count",
            "updated_at_utc",
        ]
        default_zero = set(columns) - {
            "session_id", "feed_name", "family_root", "minute_utc", "asset_class",
            "first_price", "high_price", "low_price", "last_price",
            "receive_lag_ns_max", "updated_at_utc",
        }
        values = [
            tuple(row.get(column, 0 if column in default_zero else None) for column in columns)
            for row in rows
        ]
        placeholders = ",".join("?" for _ in columns)
        updates = ",".join(
            f"{column}=excluded.{column}"
            for column in columns
            if column not in {"session_id", "feed_name", "family_root", "minute_utc"}
        )
        with self.connect() as connection:
            connection.executemany(
                f"""
                INSERT INTO tape_observed_minute ({','.join(columns)})
                VALUES ({placeholders})
                ON CONFLICT(session_id, feed_name, family_root, minute_utc)
                DO UPDATE SET {updates}
                """,
                values,
            )

    def upsert_inferred_minute_flow(self, rows: Sequence[Mapping[str, object]]) -> None:
        if not rows:
            return
        columns = [
            "session_id", "feed_name", "family_root", "minute_utc",
            "inference_method", "inference_version", "source_sha256",
            "at_ask_count", "at_bid_count", "inside_count", "unknown_count",
            "at_ask_volume", "at_bid_volume", "inside_volume", "unknown_volume",
            "at_ask_notional", "at_bid_notional", "inside_notional", "unknown_notional",
            "call_at_ask_notional", "call_at_bid_notional", "put_at_ask_notional",
            "put_at_bid_notional", "updated_at_utc",
        ]
        values = (tuple(row.get(column) for column in columns) for row in rows)
        placeholders = ",".join("?" for _ in columns)
        keys = {
            "session_id", "feed_name", "family_root", "minute_utc",
            "inference_method", "inference_version",
        }
        updates = ",".join(f"{column}=excluded.{column}" for column in columns if column not in keys)
        with self.connect() as connection:
            connection.executemany(
                f"""
                INSERT INTO tape_inferred_minute_flow ({','.join(columns)})
                VALUES ({placeholders})
                ON CONFLICT(
                    session_id, feed_name, family_root, minute_utc,
                    inference_method, inference_version
                ) DO UPDATE SET {updates}
                """,
                values,
            )

    def upsert_observed_contract_minutes(self, rows: Sequence[Mapping[str, object]]) -> None:
        if not rows:
            return
        columns = [
            "session_id", "feed_name", "family_root", "raw_symbol", "expiration",
            "option_type", "strike", "minute_utc", "trade_count", "volume", "notional",
            "first_price", "high_price", "low_price", "last_price", "price_volume_sum",
            "nbbo_valid_count", "quoted_spread_sum", "quoted_spread_bps_sum",
            "trade_to_mid_abs_sum", "trade_to_mid_signed_sum", "bid_size_sum", "ask_size_sum",
            "receive_lag_ns_sum", "receive_lag_ns_max", "data_quality_flagged_count",
            "last_pretrade_midpoint", "last_nbbo_event_ns",
            "updated_at_utc",
        ]
        values = (tuple(row.get(column) for column in columns) for row in rows)
        placeholders = ",".join("?" for _ in columns)
        keys = {"session_id", "feed_name", "raw_symbol", "minute_utc"}
        updates = ",".join(f"{column}=excluded.{column}" for column in columns if column not in keys)
        with self.connect() as connection:
            connection.executemany(
                f"""
                INSERT INTO tape_observed_contract_minute ({','.join(columns)})
                VALUES ({placeholders})
                ON CONFLICT(session_id, feed_name, raw_symbol, minute_utc)
                DO UPDATE SET {updates}
                """,
                values,
            )

    def upsert_inferred_contract_minute_flow(
        self, rows: Sequence[Mapping[str, object]]
    ) -> None:
        if not rows:
            return
        columns = [
            "session_id", "feed_name", "family_root", "raw_symbol", "expiration",
            "option_type", "strike", "minute_utc", "inference_method", "inference_version",
            "source_sha256", "at_ask_count", "at_bid_count", "inside_count", "unknown_count",
            "at_ask_volume", "at_bid_volume", "inside_volume", "unknown_volume",
            "at_ask_notional", "at_bid_notional", "inside_notional", "unknown_notional",
            "updated_at_utc",
        ]
        values = (tuple(row.get(column) for column in columns) for row in rows)
        placeholders = ",".join("?" for _ in columns)
        keys = {
            "session_id", "feed_name", "raw_symbol", "minute_utc",
            "inference_method", "inference_version",
        }
        updates = ",".join(f"{column}=excluded.{column}" for column in columns if column not in keys)
        with self.connect() as connection:
            connection.executemany(
                f"""
                INSERT INTO tape_inferred_contract_minute_flow ({','.join(columns)})
                VALUES ({placeholders})
                ON CONFLICT(
                    session_id, feed_name, raw_symbol, minute_utc,
                    inference_method, inference_version
                ) DO UPDATE SET {updates}
                """,
                values,
            )

    def upsert_inferred_reference_minutes(
        self, rows: Sequence[Mapping[str, object]]
    ) -> None:
        if not rows:
            return
        columns = [
            "session_id", "feed_name", "family_root", "minute_utc",
            "reference_method", "reference_version", "source_sha256",
            "estimated_price", "available_at_utc", "pair_count",
            "dispersion_bps", "parameters_json", "updated_at_utc",
        ]
        values = (tuple(row.get(column) for column in columns) for row in rows)
        placeholders = ",".join("?" for _ in columns)
        keys = {
            "session_id", "feed_name", "family_root", "minute_utc",
            "reference_method", "reference_version",
        }
        updates = ",".join(f"{column}=excluded.{column}" for column in columns if column not in keys)
        with self.connect() as connection:
            connection.executemany(
                f"""
                INSERT INTO tape_inferred_reference_minute ({','.join(columns)})
                VALUES ({placeholders})
                ON CONFLICT(
                    session_id, feed_name, family_root, minute_utc,
                    reference_method, reference_version
                ) DO UPDATE SET {updates}
                """,
                values,
            )

    def latest_session_id(self, trading_date: str) -> str | None:
        with self.connect(read_only=True) as connection:
            row = connection.execute(
                """
                SELECT session_id FROM tape_sessions
                WHERE trading_date=? ORDER BY created_at_utc DESC LIMIT 1
                """,
                (trading_date,),
            ).fetchone()
        return str(row["session_id"]) if row else None

    def record_analysis_cutoff(
        self,
        *,
        session_id: str,
        feed_name: str,
        horizon_id: str,
        captured_at_utc: str,
        event_cutoff_utc: str,
        cutoff_bytes: int,
        record_sequence: int,
        processed_sequence: int,
        prefix_sha256: str,
        last_trade_event_ns: int | None,
    ) -> None:
        with self.connect() as connection:
            connection.execute(
                """
                INSERT INTO tape_analysis_cutoffs (
                    session_id, feed_name, horizon_id, captured_at_utc,
                    event_cutoff_utc, cutoff_bytes, record_sequence,
                    processed_sequence, prefix_sha256, last_trade_event_ns
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(session_id, feed_name, horizon_id) DO NOTHING
                """,
                (
                    session_id,
                    feed_name,
                    horizon_id,
                    captured_at_utc,
                    event_cutoff_utc,
                    int(cutoff_bytes),
                    int(record_sequence),
                    int(processed_sequence),
                    prefix_sha256,
                    last_trade_event_ns,
                ),
            )

    def finalize_analysis_cutoff(
        self,
        *,
        session_id: str,
        feed_name: str,
        horizon_id: str,
        final_file_sha256: str,
    ) -> None:
        with self.connect() as connection:
            connection.execute(
                """
                UPDATE tape_analysis_cutoffs
                SET final_file_sha256=?, finalized_at_utc=?
                WHERE session_id=? AND feed_name=? AND horizon_id=?
                """,
                (
                    final_file_sha256,
                    _utcnow(),
                    session_id,
                    feed_name,
                    horizon_id,
                ),
            )

    def record_finalization_run(self, row: Mapping[str, object]) -> None:
        columns = (
            "run_key", "session_id", "feed_name", "source_sha256",
            "evidence_contract_version", "attempted_at_utc", "complete",
            "issues_json", "prior_status", "prior_error", "prior_gaps_json",
            "report_json",
        )
        with self.connect() as connection:
            connection.execute(
                f"""
                INSERT OR IGNORE INTO tape_finalization_runs ({','.join(columns)})
                VALUES ({','.join('?' for _ in columns)})
                """,
                tuple(row.get(column) for column in columns),
            )
