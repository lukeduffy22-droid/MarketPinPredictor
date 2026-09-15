"""High-performance local database with optimized schema."""
import hashlib
import math
import json
import logging
import os
import re
import uuid
import zlib
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence
from zoneinfo import ZoneInfo

from sqlalchemy import create_engine, Column, Integer, String, DateTime, Float, Boolean, Index, Date, Text, UniqueConstraint, CheckConstraint, inspect, text, LargeBinary, ForeignKey, event, func, select
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy.exc import IntegrityError

from backend.config import DATABASE_URL
from backend.closing_tape.close_evidence import (
    VERIFIED_CLOSE_SOURCES_BY_SYMBOL,
    validate_official_close_reference,
    validate_source_artifact_sha256,
    validate_verified_close_observed_at,
)

BASE_DIR = Path(__file__).resolve().parent.parent
PREDICTION_EXPORT_DIR = BASE_DIR / "exports" / "predictions"
DIAGNOSTIC_ACCURACY_SCOPE = "diagnostic_research_only"

logger = logging.getLogger(__name__)

# Create engine with performance optimizations
engine = create_engine(
    DATABASE_URL,
    echo=False,
    pool_pre_ping=True,
    pool_size=20,  # Handle concurrent requests
    max_overflow=40,
    connect_args={"check_same_thread": False} if DATABASE_URL.startswith("sqlite") else {}
)


if DATABASE_URL.startswith("sqlite"):
    @event.listens_for(engine, "connect")
    def _configure_sqlite(dbapi_connection, _connection_record):
        cursor = dbapi_connection.cursor()
        try:
            cursor.execute("PRAGMA journal_mode=WAL")
            cursor.execute("PRAGMA synchronous=NORMAL")
            cursor.execute("PRAGMA busy_timeout=10000")
            cursor.execute("PRAGMA foreign_keys=ON")
            cursor.execute("PRAGMA wal_autocheckpoint=1000")
        finally:
            cursor.close()

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


class MarketSnapshot(Base):
    """Real-time market data snapshot"""
    __tablename__ = "market_snapshots"

    id = Column(Integer, primary_key=True, index=True)
    symbol = Column(String(10), index=True, nullable=False)
    timestamp_utc = Column(DateTime, index=True, nullable=False)

    # Price data
    price = Column(Float, nullable=False)
    bid = Column(Float)
    ask = Column(Float)
    volume = Column(Float)

    # Derived metrics
    vwap = Column(Float)

    __table_args__ = (
        Index('idx_symbol_timestamp', 'symbol', 'timestamp_utc'),
    )


class GammaSnapshot(Base):
    """Gamma exposure snapshot"""
    __tablename__ = "gamma_snapshots"

    id = Column(Integer, primary_key=True, index=True)
    symbol = Column(String(10), index=True, nullable=False)
    timestamp_utc = Column(DateTime, index=True, nullable=False)
    trading_date = Column(DateTime, nullable=False)

    # Spot price
    spot_price = Column(Float, nullable=False)

    # Gamma metrics
    gamma_pin = Column(Float)
    distance_to_pin = Column(Float)
    gross_gex = Column(Float)
    net_gex = Column(Float)
    call_gex = Column(Float)
    put_gex = Column(Float)

    # Validation
    is_valid = Column(Boolean, default=True)
    confidence = Column(Float)

    __table_args__ = (
        Index('idx_gamma_symbol_timestamp', 'symbol', 'timestamp_utc'),
    )


class Prediction(Base):
    """Model predictions"""
    __tablename__ = "predictions"

    id = Column(Integer, primary_key=True, index=True)
    symbol = Column(String(10), index=True, nullable=False)
    timestamp_utc = Column(DateTime, index=True, nullable=False)

    # Input
    current_price = Column(Float, nullable=False)

    # Prediction
    predicted_close = Column(Float, nullable=False)
    confidence = Column(Float)

    # Model info
    model_version = Column(String(50))
    inference_time_ms = Column(Float)

    # Validation (filled after close)
    actual_close = Column(Float)
    error = Column(Float)

    __table_args__ = (
        Index('idx_pred_symbol_timestamp', 'symbol', 'timestamp_utc'),
    )


class PredictionSnapshot(Base):
    """Every live AI prediction, persisted for future scoring and backtests."""
    __tablename__ = "prediction_snapshots"

    id = Column(Integer, primary_key=True, index=True)
    symbol = Column(String(16), index=True, nullable=False)
    timestamp_utc = Column(DateTime, index=True, nullable=False)
    trading_date = Column(Date, index=True, nullable=False)

    provider = Column(String(50))
    model_version = Column(String(80))
    model_type = Column(String(80))
    prediction_mode = Column(String(40), default="balanced")
    is_valid = Column(Boolean, default=True)

    current_price = Column(Float, nullable=False)
    predicted_close = Column(Float, nullable=False)
    confidence = Column(Float)
    expected_move_points = Column(Float)
    expected_move_pct = Column(Float)
    net_bias = Column(String(20))

    feature_snapshot_json = Column(Text)
    feature_schema_version = Column(String(40))
    feature_hash = Column(String(64))
    signals_json = Column(Text)
    source_payload_json = Column(Text)
    data_age_seconds = Column(Float)
    quote_timestamp_utc = Column(DateTime)
    subscription_epoch_id = Column(String(64))
    subscription_generation = Column(Integer)
    quote_age_seconds = Column(Float)
    active_contract_count = Column(Integer)
    fresh_quote_count = Column(Integer)
    gamma_pin = Column(Float)
    max_pain = Column(Float)
    zero_gamma = Column(Float)
    gross_gex = Column(Float)
    net_gex = Column(Float)
    call_gex = Column(Float)
    put_gex = Column(Float)
    inference_device = Column(String(40))
    validation_status = Column(String(40))

    actual_close = Column(Float)
    error_points = Column(Float)
    error_pct = Column(Float)
    direction_hit = Column(Boolean)
    scored_at_utc = Column(DateTime)
    created_at_utc = Column(DateTime, default=datetime.utcnow, nullable=False)

    __table_args__ = (
        CheckConstraint(
            "subscription_epoch_id IS NULL OR length(subscription_epoch_id) = 64",
            name="ck_prediction_snapshot_epoch_hash_length",
        ),
        Index('idx_prediction_snapshot_symbol_time', 'symbol', 'timestamp_utc'),
        Index('idx_prediction_snapshot_symbol_date', 'symbol', 'trading_date'),
        Index(
            'uix_prediction_snapshot_source_quote',
            'symbol',
            'trading_date',
            'prediction_mode',
            'subscription_epoch_id',
            'subscription_generation',
            'quote_timestamp_utc',
            unique=True,
        ),
    )


class EODClose(Base):
    """Official or manually supplied end-of-day close used for scoring."""
    __tablename__ = "eod_closes"
    __table_args__ = (
        UniqueConstraint('symbol', 'trading_date', name='uix_eod_close_symbol_date'),
    )

    id = Column(Integer, primary_key=True, index=True)
    symbol = Column(String(16), index=True, nullable=False)
    trading_date = Column(Date, index=True, nullable=False)
    official_close = Column(Float, nullable=False)
    source = Column(String(80), default="manual")
    ingested_at_utc = Column(DateTime, default=datetime.utcnow, nullable=False)


class EODCloseObservation(Base):
    """Append-only close evidence; eod_closes is only the current projection."""
    __tablename__ = "eod_close_observations"

    id = Column(Integer, primary_key=True, index=True)
    observation_key = Column(String(64), unique=True, nullable=False)
    symbol = Column(String(16), index=True, nullable=False)
    trading_date = Column(Date, index=True, nullable=False)
    official_close = Column(Float, nullable=False)
    source = Column(String(80), nullable=False)
    source_reference = Column(String(500))
    source_artifact_sha256 = Column(String(64))
    source_verified = Column(Boolean, default=False, nullable=False)
    observed_at_utc = Column(DateTime, nullable=False)
    ingested_at_utc = Column(DateTime, default=datetime.utcnow, nullable=False)
    correction_of_id = Column(Integer, ForeignKey("eod_close_observations.id"))


class PredictionAccuracyObservation(Base):
    """Append-only diagnostic score evidence bound to one verified close."""
    __tablename__ = "prediction_accuracy_observations"

    id = Column(Integer, primary_key=True, index=True)
    score_key = Column(String(64), unique=True, nullable=False)
    prediction_id = Column(Integer, ForeignKey("prediction_snapshots.id", ondelete="RESTRICT"), nullable=False)
    close_observation_id = Column(Integer, ForeignKey("eod_close_observations.id", ondelete="RESTRICT"), nullable=False)
    symbol = Column(String(16), index=True, nullable=False)
    trading_date = Column(Date, index=True, nullable=False)
    prediction_timestamp_utc = Column(DateTime, nullable=False)
    predicted_close = Column(Float, nullable=False)
    actual_close = Column(Float, nullable=False)
    error_points = Column(Float, nullable=False)
    error_pct = Column(Float, nullable=False)
    direction_hit = Column(Boolean, nullable=False)
    model_version = Column(String(80))
    provider = Column(String(50))
    quote_age_seconds = Column(Float)
    fresh_quote_count = Column(Integer)
    active_contract_count = Column(Integer)
    close_source = Column(String(80), nullable=False)
    close_source_reference = Column(String(500), nullable=False)
    close_source_artifact_sha256 = Column(String(64), nullable=False)
    evidence_scope = Column(
        String(64),
        default=DIAGNOSTIC_ACCURACY_SCOPE,
        server_default=DIAGNOSTIC_ACCURACY_SCOPE,
        nullable=False,
    )
    training_eligible = Column(Boolean, default=False, server_default="0", nullable=False)
    selection_evidence_sha256 = Column(String(64))
    scored_at_utc = Column(DateTime, default=datetime.utcnow, nullable=False)

    __table_args__ = (
        UniqueConstraint("prediction_id", "close_observation_id", name="uix_prediction_accuracy_prediction_close_observation"),
        CheckConstraint(
            "evidence_scope = 'diagnostic_research_only'",
            name="ck_prediction_accuracy_diagnostic_scope",
        ),
        CheckConstraint(
            "training_eligible IS FALSE",
            name="ck_prediction_accuracy_not_training",
        ),
        Index("idx_prediction_accuracy_symbol_date", "symbol", "trading_date"),
    )


class GammaCalculationRun(Base):
    """Append-only metadata for a replayable live gamma calculation."""
    __tablename__ = "gamma_calculation_runs"

    id = Column(Integer, primary_key=True)
    calculation_id = Column(String(36), unique=True, nullable=False)
    symbol = Column(String(16), nullable=False)
    trading_date = Column(Date, nullable=False)
    calculated_at_utc = Column(DateTime, nullable=False)
    provider = Column(String(32), nullable=False)
    subscription_epoch_id = Column(String(64))
    subscription_generation = Column(Integer)
    status = Column(String(24), nullable=False)
    input_schema_version = Column(String(32), nullable=False)
    formula_version = Column(String(80), nullable=False)
    target_formula_version = Column(String(80))
    spot_formula_version = Column(String(80))
    universe_sha256 = Column(String(64))
    risk_free_rate = Column(Float)
    contract_multiplier = Column(Float)
    spot_price = Column(Float)
    gamma_pin = Column(Float)
    selected_target = Column(Float)
    primary_expiration_target = Column(Float)
    multi_expiration_target = Column(Float)
    zero_gamma = Column(Float)
    max_pain = Column(Float)
    call_gex = Column(Float)
    put_gex = Column(Float)
    gross_gex = Column(Float)
    net_gex = Column(Float)
    chain_row_count = Column(Integer, nullable=False)
    gex_row_count = Column(Integer, nullable=False)
    rejection_counts_json = Column(Text)
    created_at_utc = Column(DateTime, default=datetime.utcnow, nullable=False)

    __table_args__ = (
        CheckConstraint(
            "subscription_epoch_id IS NULL OR length(subscription_epoch_id) = 64",
            name="ck_gamma_calculation_epoch_hash_length",
        ),
        Index("idx_gamma_calc_symbol_date_time", "symbol", "trading_date", "calculated_at_utc"),
        Index("idx_gamma_calc_trading_date", "trading_date"),
    )


class GammaCalculationInputBlob(Base):
    """Compressed canonical full-chain inputs for one calculation run."""
    __tablename__ = "gamma_calculation_input_blobs"

    calculation_run_id = Column(Integer, ForeignKey("gamma_calculation_runs.id", ondelete="RESTRICT"), primary_key=True)
    encoding = Column(String(32), nullable=False)
    payload_sha256 = Column(String(64), nullable=False)
    uncompressed_bytes = Column(Integer, nullable=False)
    compressed_bytes = Column(Integer, nullable=False)
    payload = Column(LargeBinary, nullable=False)


class MarketStructureObservation(Base):
    """Append-only high-frequency level/price evidence for ORB and pin drift."""

    __tablename__ = "market_structure_observations"

    observation_id = Column(String(64), primary_key=True)
    symbol = Column(String(16), nullable=False)
    trading_date = Column(Date, nullable=False)
    source_timestamp_utc = Column(DateTime, nullable=False)
    captured_at_utc = Column(DateTime, default=datetime.utcnow, nullable=False)
    provider = Column(String(32), nullable=False)
    # Nullable only so append-only rows captured before the epoch contract stay
    # readable.  Every new row is required to carry a canonical epoch by the
    # writer and insert guard below.
    subscription_epoch_id = Column(String(64))
    subscription_generation = Column(Integer, nullable=False)
    calculation_id = Column(String(36))
    reference_price = Column(Float, nullable=False)
    spot_source = Column(String(80))
    gamma_pin = Column(Float)
    max_pain = Column(Float)
    zero_gamma = Column(Float)
    pin_lead_ratio = Column(Float)
    pin_is_contested = Column(Boolean)
    gross_gex = Column(Float)
    net_gex = Column(Float)
    primary_expiration = Column(String(10))
    same_day_profile_available = Column(Boolean)
    universe_sha256 = Column(String(64), nullable=False)
    validation_status = Column(String(24), nullable=False)

    __table_args__ = (
        CheckConstraint(
            "provider = 'databento'",
            name="ck_market_structure_databento_provider",
        ),
        CheckConstraint(
            "subscription_generation > 0",
            name="ck_market_structure_positive_generation",
        ),
        CheckConstraint(
            "subscription_epoch_id IS NULL OR length(subscription_epoch_id) = 64",
            name="ck_market_structure_epoch_hash_length",
        ),
        CheckConstraint(
            "reference_price > 0",
            name="ck_market_structure_positive_price",
        ),
        CheckConstraint(
            "length(universe_sha256) = 64",
            name="ck_market_structure_universe_hash_length",
        ),
        CheckConstraint(
            "validation_status = 'valid'",
            name="ck_market_structure_valid_only",
        ),
        Index(
            "idx_market_structure_symbol_date_time",
            "symbol",
            "trading_date",
            "source_timestamp_utc",
        ),
        Index("idx_market_structure_trading_date", "trading_date"),
    )


class OrbReferenceSample(Base):
    """Append-only 5-second option-parity reference evidence for ORB ranges."""

    __tablename__ = "orb_reference_samples"

    sample_id = Column(String(64), primary_key=True)
    symbol = Column(String(16), nullable=False)
    trading_date = Column(Date, nullable=False)
    sample_timestamp_utc = Column(DateTime, nullable=False)
    source_timestamp_utc = Column(DateTime, nullable=False)
    captured_at_utc = Column(DateTime, nullable=False)
    provider = Column(String(32), nullable=False)
    # Legacy rows predate process-epoch identity and remain nullable historical
    # evidence; new writes are rejected unless this is canonical 64-hex.
    subscription_epoch_id = Column(String(64))
    subscription_generation = Column(Integer, nullable=False)
    reference_price = Column(Float, nullable=False)
    spot_source = Column(String(80), nullable=False)
    spot_formula_version = Column(String(80), nullable=False)
    risk_free_rate = Column(Float, nullable=False)
    time_to_expiration_years = Column(Float, nullable=False)
    primary_expiration = Column(String(10), nullable=False)
    same_day_profile_available = Column(Boolean, nullable=False)
    universe_sha256 = Column(String(64), nullable=False)
    universe_is_fallback = Column(Boolean, nullable=False)
    paired_quote_count = Column(Integer, nullable=False)
    minimum_paired_quote_count = Column(Integer, nullable=False)
    contributing_pair_count = Column(Integer, nullable=False)
    contributing_quote_count = Column(Integer, nullable=False)
    earliest_ts_event_ns = Column(Integer, nullable=False)
    latest_ts_event_ns = Column(Integer, nullable=False)
    earliest_ts_recv_ns = Column(Integer, nullable=False)
    latest_ts_recv_ns = Column(Integer, nullable=False)
    observation_index_ns = Column(Integer, nullable=False)
    source_quote_age_seconds = Column(Float, nullable=False)
    maximum_source_quote_age_seconds = Column(Float, nullable=False)
    source_timestamp_span_seconds = Column(Float, nullable=False)
    quote_freshness_limit_seconds = Column(Float, nullable=False)
    pair_identity_sha256 = Column(String(64), nullable=False)
    symbol_mapping_version = Column(String(64), nullable=False)
    formula_inputs_json = Column(Text, nullable=False)
    processing_clock_status = Column(String(24), nullable=False)
    timestamp_order_valid = Column(Boolean, nullable=False)
    handoff_status = Column(String(24), nullable=False)
    generation_state_unchanged = Column(Boolean, nullable=False)
    universe_state_unchanged = Column(Boolean, nullable=False)
    validation_status = Column(String(24), nullable=False)

    __table_args__ = (
        CheckConstraint("provider = 'databento'", name="ck_orb_reference_databento_provider"),
        CheckConstraint("subscription_generation > 0", name="ck_orb_reference_positive_generation"),
        CheckConstraint(
            "subscription_epoch_id IS NULL OR length(subscription_epoch_id) = 64",
            name="ck_orb_reference_epoch_hash_length",
        ),
        CheckConstraint("reference_price > 0", name="ck_orb_reference_positive_price"),
        CheckConstraint("spot_source = 'databento_opra_put_call_parity'", name="ck_orb_reference_spot_source"),
        CheckConstraint("length(spot_formula_version) > 0", name="ck_orb_reference_formula_version"),
        CheckConstraint("time_to_expiration_years > 0", name="ck_orb_reference_positive_tte"),
        CheckConstraint("length(primary_expiration) = 10", name="ck_orb_reference_primary_expiration"),
        CheckConstraint("length(universe_sha256) = 64", name="ck_orb_reference_universe_hash_length"),
        CheckConstraint("universe_is_fallback = 0", name="ck_orb_reference_nonfallback"),
        CheckConstraint(
            "minimum_paired_quote_count > 0 AND paired_quote_count >= minimum_paired_quote_count",
            name="ck_orb_reference_complete_pairs",
        ),
        CheckConstraint(
            "contributing_pair_count > 0 "
            "AND contributing_pair_count <= paired_quote_count "
            "AND contributing_quote_count = contributing_pair_count * 2",
            name="ck_orb_reference_contributing_pairs",
        ),
        CheckConstraint(
            "earliest_ts_event_ns > 0 AND latest_ts_event_ns >= earliest_ts_event_ns "
            "AND earliest_ts_recv_ns > 0 AND latest_ts_recv_ns >= earliest_ts_recv_ns "
            "AND observation_index_ns > 0 AND latest_ts_event_ns <= latest_ts_recv_ns",
            name="ck_orb_reference_timestamp_order",
        ),
        CheckConstraint(
            "source_quote_age_seconds >= -0.05 "
            "AND maximum_source_quote_age_seconds >= source_quote_age_seconds "
            "AND source_timestamp_span_seconds >= 0 "
            "AND quote_freshness_limit_seconds > 0 "
            "AND maximum_source_quote_age_seconds <= quote_freshness_limit_seconds",
            name="ck_orb_reference_timestamp_evidence",
        ),
        CheckConstraint("length(pair_identity_sha256) = 64", name="ck_orb_reference_pair_hash"),
        CheckConstraint("length(symbol_mapping_version) = 64", name="ck_orb_reference_mapping_hash"),
        CheckConstraint("processing_clock_status = 'synchronized'", name="ck_orb_reference_clock_sync"),
        CheckConstraint("timestamp_order_valid = 1", name="ck_orb_reference_order_valid"),
        CheckConstraint("handoff_status = 'active'", name="ck_orb_reference_active_handoff"),
        CheckConstraint("generation_state_unchanged = 1", name="ck_orb_reference_generation_stable"),
        CheckConstraint("universe_state_unchanged = 1", name="ck_orb_reference_universe_stable"),
        CheckConstraint("validation_status = 'valid'", name="ck_orb_reference_valid_only"),
        Index(
            "idx_orb_reference_symbol_date_sample_time",
            "symbol",
            "trading_date",
            "sample_timestamp_utc",
        ),
        Index("idx_orb_reference_trading_date", "trading_date"),
        Index(
            "uix_orb_reference_logical_sample",
            "symbol",
            "sample_timestamp_utc",
            "subscription_epoch_id",
            "subscription_generation",
            "universe_sha256",
            "primary_expiration",
            "spot_formula_version",
            "risk_free_rate",
            "symbol_mapping_version",
            unique=True,
        ),
    )


class OrbReferenceSampleDecision(Base):
    """Immutable final progress decision for one retained ORB sample.

    Reference rows are written before the sampler can finish its post-persist
    bucket/context checks.  Keeping that final decision in a 1:1 sidecar
    preserves the raw evidence while making pending and rejected rows
    ineligible for every live/read-side projection.
    """

    __tablename__ = "orb_reference_sample_decisions"

    sample_id = Column(
        String(64),
        ForeignKey("orb_reference_samples.sample_id", ondelete="RESTRICT"),
        primary_key=True,
    )
    sample_timestamp_utc = Column(DateTime, nullable=False)
    intended_bucket_utc = Column(DateTime, nullable=False)
    attempt_completed_at_utc = Column(DateTime, nullable=False)
    progress_eligible = Column(Boolean, nullable=False)
    reason = Column(String(96))
    decision_status = Column(String(16), nullable=False)

    __table_args__ = (
        CheckConstraint(
            "decision_status = 'final'",
            name="ck_orb_reference_decision_final",
        ),
        CheckConstraint(
            "(progress_eligible = 1 AND reason IS NULL) OR "
            "(progress_eligible = 0 AND reason IS NOT NULL AND length(reason) > 0)",
            name="ck_orb_reference_decision_reason",
        ),
        CheckConstraint(
            "attempt_completed_at_utc >= intended_bucket_utc",
            name="ck_orb_reference_decision_completion_order",
        ),
        CheckConstraint(
            "progress_eligible = 0 OR sample_timestamp_utc = intended_bucket_utc",
            name="ck_orb_reference_decision_eligible_bucket_identity",
        ),
        Index(
            "uix_orb_reference_decision_sample_id",
            "sample_id",
            unique=True,
        ),
    )


class PredictionPassport(Base):
    """Immutable, provenance-complete sidecar for a forecast or abstention."""
    __tablename__ = "prediction_passports"

    passport_id = Column(String(64), primary_key=True)
    forecast_id = Column(String(64), unique=True, nullable=False)
    schema_version = Column(String(32), nullable=False)

    origin_kind = Column(String(40), nullable=False)
    origin_key = Column(String(128), nullable=False)
    prediction_snapshot_id = Column(
        Integer,
        ForeignKey("prediction_snapshots.id", ondelete="RESTRICT"),
        unique=True,
    )

    symbol = Column(String(16), nullable=False)
    prediction_mode = Column(String(40), nullable=False)
    state = Column(String(16), nullable=False)
    target_kind = Column(String(40), nullable=False)
    target_trading_date = Column(Date, nullable=False)
    prediction_timestamp_utc = Column(DateTime, nullable=False)
    target_timestamp_utc = Column(DateTime, nullable=False)
    horizon_seconds = Column(Integer, nullable=False)

    provider = Column(String(50))
    quote_timestamp_utc = Column(DateTime)
    subscription_epoch_id = Column(String(64))
    subscription_generation = Column(Integer)
    calculation_id = Column(String(36))
    calculation_input_sha256 = Column(String(64))
    source_payload_sha256 = Column(String(64))
    universe_sha256 = Column(String(64))

    model_version = Column(String(80))
    model_type = Column(String(80))
    model_artifact_sha256 = Column(String(64))
    feature_schema_version = Column(String(40))
    feature_hash = Column(String(64))
    formula_versions_json = Column(Text, nullable=False)
    inference_device = Column(String(40))

    reference_price = Column(Float)
    predicted_close = Column(Float)
    prediction_lower = Column(Float)
    prediction_upper = Column(Float)
    confidence_raw = Column(Float)
    confidence_scale = Column(String(16), nullable=False)
    confidence_kind = Column(String(64), nullable=False)
    interval_target_coverage = Column(Float)
    calibration_method = Column(String(80))
    calibration_evidence_sha256 = Column(String(64))

    validation_status = Column(String(40), nullable=False)
    data_age_seconds = Column(Float)
    quote_age_seconds = Column(Float)
    active_contract_count = Column(Integer)
    fresh_quote_count = Column(Integer)

    state_reasons_json = Column(Text, nullable=False)
    missing_evidence_json = Column(Text, nullable=False)
    provenance_json = Column(Text, nullable=False)
    canonical_payload_json = Column(Text, nullable=False)
    record_sha256 = Column(String(64), nullable=False)
    created_at_utc = Column(DateTime, default=datetime.utcnow, nullable=False)

    __table_args__ = (
        UniqueConstraint("origin_kind", "origin_key", name="uix_prediction_passport_origin"),
        CheckConstraint(
            "subscription_epoch_id IS NULL OR length(subscription_epoch_id) = 64",
            name="ck_prediction_passport_epoch_hash_length",
        ),
        CheckConstraint(
            "state IN ('valid','research','abstain','stale','unavailable')",
            name="ck_prediction_passport_state",
        ),
        CheckConstraint(
            "passport_id = forecast_id",
            name="ck_prediction_passport_identity",
        ),
        CheckConstraint(
            "(state NOT IN ('abstain','stale','unavailable')) OR "
            "(predicted_close IS NULL AND prediction_lower IS NULL AND prediction_upper IS NULL)",
            name="ck_prediction_passport_terminal_forecast_null",
        ),
        CheckConstraint(
            "(state != 'valid') OR "
            "(reference_price > 0 AND predicted_close > 0 AND prediction_lower > 0 "
            "AND prediction_lower <= predicted_close AND predicted_close <= prediction_upper)",
            name="ck_prediction_passport_valid_forecast",
        ),
        CheckConstraint("horizon_seconds >= 0", name="ck_prediction_passport_horizon"),
        Index(
            "idx_prediction_passport_symbol_target_time",
            "symbol",
            "target_trading_date",
            "prediction_timestamp_utc",
        ),
        Index(
            "idx_prediction_passport_model_state_date",
            "model_version",
            "state",
            "target_trading_date",
        ),
    )


def init_db():
    """Initialize database tables"""
    logger.info(f"Initializing database: {DATABASE_URL}")
    Base.metadata.create_all(bind=engine)
    _ensure_capture_epoch_columns()
    _ensure_prediction_snapshot_columns()
    _ensure_prediction_snapshot_provenance_immutability()
    _ensure_gamma_calculation_immutability()
    _ensure_eod_close_observation_columns()
    _ensure_prediction_accuracy_columns()
    _ensure_prediction_accuracy_immutability()
    _ensure_prediction_passport_immutability()
    _ensure_market_structure_schema()
    _ensure_market_structure_immutability()
    _ensure_orb_reference_schema()
    _ensure_orb_reference_immutability()
    _ensure_orb_reference_decision_schema()
    _ensure_orb_reference_decision_immutability()
    reconciliation = reconcile_orb_reference_sample_decisions()
    if reconciliation["reconciled_count"]:
        logger.warning(
            "Finalized %s interrupted ORB reference decisions as ineligible",
            reconciliation["reconciled_count"],
        )
    if reconciliation["remaining_expired_pending_count"]:
        logger.error(
            "%s expired ORB reference samples still lack a final decision",
            reconciliation["remaining_expired_pending_count"],
        )
    logger.info("Database initialized successfully")


def _ensure_capture_epoch_columns() -> None:
    """Add process-epoch identity without rewriting append-only legacy rows."""

    if not DATABASE_URL.startswith("sqlite"):
        return
    inspector = inspect(engine)
    for table_name in (
        "market_structure_observations",
        "orb_reference_samples",
        "prediction_snapshots",
        "gamma_calculation_runs",
        "prediction_passports",
    ):
        if not inspector.has_table(table_name):
            continue
        existing = {
            column["name"] for column in inspector.get_columns(table_name)
        }
        if "subscription_epoch_id" in existing:
            continue
        with engine.begin() as connection:
            connection.execute(
                text(
                    f"ALTER TABLE {table_name} "
                    "ADD COLUMN subscription_epoch_id VARCHAR(64)"
                )
            )

    inspector = inspect(engine)
    with engine.begin() as connection:
        if inspector.has_table("prediction_snapshots"):
            connection.execute(
                text("DROP TRIGGER IF EXISTS prediction_snapshots_epoch_guard")
            )
            connection.execute(text("""
                CREATE TRIGGER prediction_snapshots_epoch_guard
                BEFORE INSERT ON prediction_snapshots
                WHEN lower(COALESCE(NEW.provider, '')) = 'databento'
                  AND NEW.is_valid = 1
                  AND (
                      NEW.subscription_epoch_id IS NULL
                      OR length(NEW.subscription_epoch_id) != 64
                      OR NEW.subscription_epoch_id != lower(NEW.subscription_epoch_id)
                      OR NEW.subscription_epoch_id GLOB '*[^0-9a-f]*'
                  )
                BEGIN SELECT RAISE(ABORT, 'invalid prediction snapshot process epoch'); END
            """))
        if inspector.has_table("gamma_calculation_runs"):
            connection.execute(
                text("DROP TRIGGER IF EXISTS gamma_calculation_runs_epoch_guard")
            )
            connection.execute(text("""
                CREATE TRIGGER gamma_calculation_runs_epoch_guard
                BEFORE INSERT ON gamma_calculation_runs
                WHEN lower(COALESCE(NEW.provider, '')) = 'databento'
                  AND (
                      NEW.subscription_epoch_id IS NULL
                      OR length(NEW.subscription_epoch_id) != 64
                      OR NEW.subscription_epoch_id != lower(NEW.subscription_epoch_id)
                      OR NEW.subscription_epoch_id GLOB '*[^0-9a-f]*'
                      OR NEW.subscription_generation IS NULL
                      OR NEW.subscription_generation <= 0
                  )
                BEGIN SELECT RAISE(ABORT, 'invalid gamma calculation process identity'); END
            """))
        if inspector.has_table("prediction_passports"):
            connection.execute(
                text("DROP TRIGGER IF EXISTS prediction_passports_epoch_guard")
            )
            connection.execute(text("""
                CREATE TRIGGER prediction_passports_epoch_guard
                BEFORE INSERT ON prediction_passports
                WHEN lower(COALESCE(NEW.provider, '')) = 'databento'
                  AND NEW.state = 'valid'
                  AND (
                      NEW.subscription_epoch_id IS NULL
                      OR length(NEW.subscription_epoch_id) != 64
                      OR NEW.subscription_epoch_id != lower(NEW.subscription_epoch_id)
                      OR NEW.subscription_epoch_id GLOB '*[^0-9a-f]*'
                  )
                BEGIN SELECT RAISE(ABORT, 'invalid prediction passport process epoch'); END
            """))


def _ensure_prediction_snapshot_columns() -> None:
    """Add audit columns to existing SQLite databases without destructive migration."""
    if not DATABASE_URL.startswith("sqlite"):
        return
    existing = {column["name"] for column in inspect(engine).get_columns("prediction_snapshots")}
    additions = {
        "feature_schema_version": "VARCHAR(40)",
        "feature_hash": "VARCHAR(64)",
        "data_age_seconds": "REAL",
        "quote_timestamp_utc": "DATETIME",
        "subscription_epoch_id": "VARCHAR(64)",
        "subscription_generation": "INTEGER",
        "quote_age_seconds": "REAL",
        "active_contract_count": "INTEGER",
        "fresh_quote_count": "INTEGER",
        "gamma_pin": "REAL",
        "max_pain": "REAL",
        "zero_gamma": "REAL",
        "gross_gex": "REAL",
        "net_gex": "REAL",
        "call_gex": "REAL",
        "put_gex": "REAL",
        "inference_device": "VARCHAR(40)",
        "validation_status": "VARCHAR(40)",
    }
    with engine.begin() as connection:
        for name, sql_type in additions.items():
            if name not in existing:
                connection.execute(text(f"ALTER TABLE prediction_snapshots ADD COLUMN {name} {sql_type}"))
        duplicate = connection.execute(text("""
            SELECT 1
            FROM prediction_snapshots
            WHERE subscription_epoch_id IS NOT NULL
              AND subscription_generation IS NOT NULL
              AND quote_timestamp_utc IS NOT NULL
            GROUP BY symbol, trading_date, prediction_mode,
                     subscription_epoch_id, subscription_generation,
                     quote_timestamp_utc
            HAVING COUNT(*) > 1
            LIMIT 1
        """)).first()
        if duplicate is not None:
            raise RuntimeError(
                "Prediction snapshot process-identity duplicates prevent safe migration"
            )
        # Replace the pre-epoch uniqueness rule only after proving the stronger
        # key can be installed. Otherwise a restarted process reusing the same
        # generation and quote timestamp can be collapsed into the prior
        # process's prediction history.
        connection.execute(
            text("DROP INDEX IF EXISTS uix_prediction_snapshot_source_quote")
        )
        connection.execute(text("""
            CREATE UNIQUE INDEX uix_prediction_snapshot_source_quote
            ON prediction_snapshots (
                symbol,
                trading_date,
                prediction_mode,
                subscription_epoch_id,
                subscription_generation,
                quote_timestamp_utc
            )
        """))


def _ensure_prediction_snapshot_provenance_immutability() -> None:
    """Allow scoring updates while freezing the captured prediction evidence."""

    if not DATABASE_URL.startswith("sqlite"):
        return
    immutable_columns = (
        "id",
        "symbol",
        "timestamp_utc",
        "trading_date",
        "provider",
        "model_version",
        "model_type",
        "prediction_mode",
        "is_valid",
        "current_price",
        "predicted_close",
        "confidence",
        "expected_move_points",
        "expected_move_pct",
        "net_bias",
        "feature_snapshot_json",
        "feature_schema_version",
        "feature_hash",
        "signals_json",
        "source_payload_json",
        "data_age_seconds",
        "quote_timestamp_utc",
        "subscription_epoch_id",
        "subscription_generation",
        "quote_age_seconds",
        "active_contract_count",
        "fresh_quote_count",
        "gamma_pin",
        "max_pain",
        "zero_gamma",
        "gross_gex",
        "net_gex",
        "call_gex",
        "put_gex",
        "inference_device",
        "validation_status",
        "created_at_utc",
    )
    changed = "\n              OR ".join(
        f"NEW.{column} IS NOT OLD.{column}" for column in immutable_columns
    )
    with engine.begin() as connection:
        connection.execute(
            text("DROP TRIGGER IF EXISTS prediction_snapshots_provenance_no_update")
        )
        connection.execute(text(f"""
            CREATE TRIGGER prediction_snapshots_provenance_no_update
            BEFORE UPDATE ON prediction_snapshots
            WHEN {changed}
            BEGIN SELECT RAISE(
                ABORT,
                'prediction snapshot captured evidence is immutable'
            ); END
        """))


def _ensure_gamma_calculation_immutability() -> None:
    """Protect replay calculation metadata and compressed inputs from mutation."""

    if not DATABASE_URL.startswith("sqlite"):
        return
    with engine.begin() as connection:
        for table_name, label in (
            ("gamma_calculation_runs", "gamma calculation runs"),
            ("gamma_calculation_input_blobs", "gamma calculation input blobs"),
        ):
            connection.execute(
                text(f"DROP TRIGGER IF EXISTS {table_name}_no_update")
            )
            connection.execute(text(f"""
                CREATE TRIGGER {table_name}_no_update
                BEFORE UPDATE ON {table_name}
                BEGIN SELECT RAISE(ABORT, '{label} are immutable'); END
            """))
            connection.execute(
                text(f"DROP TRIGGER IF EXISTS {table_name}_no_delete")
            )
            connection.execute(text(f"""
                CREATE TRIGGER {table_name}_no_delete
                BEFORE DELETE ON {table_name}
                BEGIN SELECT RAISE(ABORT, '{label} are immutable'); END
            """))


def _ensure_eod_close_observation_columns() -> None:
    """Add immutable source-artifact identity to existing close ledgers."""
    if not DATABASE_URL.startswith("sqlite"):
        return
    inspector = inspect(engine)
    if "eod_close_observations" not in inspector.get_table_names():
        return
    existing = {column["name"] for column in inspector.get_columns("eod_close_observations")}
    if "source_artifact_sha256" not in existing:
        with engine.begin() as connection:
            connection.execute(text(
                "ALTER TABLE eod_close_observations "
                "ADD COLUMN source_artifact_sha256 VARCHAR(64)"
            ))


def _ensure_prediction_accuracy_columns() -> None:
    """Conservatively classify every legacy snapshot score as diagnostic-only."""
    if not DATABASE_URL.startswith("sqlite"):
        return
    inspector = inspect(engine)
    if "prediction_accuracy_observations" not in inspector.get_table_names():
        return
    existing = {
        column["name"] for column in inspector.get_columns("prediction_accuracy_observations")
    }
    additions = {
        "evidence_scope": (
            "VARCHAR(64) NOT NULL DEFAULT 'diagnostic_research_only'"
        ),
        "training_eligible": "BOOLEAN NOT NULL DEFAULT 0",
        "selection_evidence_sha256": "VARCHAR(64)",
    }
    with engine.begin() as connection:
        for name, sql_type in additions.items():
            if name not in existing:
                connection.execute(text(
                    f"ALTER TABLE prediction_accuracy_observations ADD COLUMN {name} {sql_type}"
                ))


def _ensure_prediction_accuracy_immutability() -> None:
    """Protect close/score evidence and keep the legacy scorer diagnostic-only."""
    if not DATABASE_URL.startswith("sqlite"):
        return
    with engine.begin() as connection:
        accuracy_columns = {
            str(row[1])
            for row in connection.execute(
                text("PRAGMA table_info(prediction_accuracy_observations)")
            )
        }
        connection.execute(text("""
            CREATE TRIGGER IF NOT EXISTS eod_close_observations_no_update
            BEFORE UPDATE ON eod_close_observations
            BEGIN SELECT RAISE(ABORT, 'eod close observations are immutable'); END
        """))
        connection.execute(text("""
            CREATE TRIGGER IF NOT EXISTS eod_close_observations_no_delete
            BEFORE DELETE ON eod_close_observations
            BEGIN SELECT RAISE(ABORT, 'eod close observations are immutable'); END
        """))
        connection.execute(text("""
            CREATE TRIGGER IF NOT EXISTS eod_close_observations_no_replace
            BEFORE INSERT ON eod_close_observations
            WHEN EXISTS (
                SELECT 1 FROM eod_close_observations
                WHERE id=NEW.id OR observation_key=NEW.observation_key
            )
            BEGIN SELECT RAISE(ABORT, 'eod close observations are immutable'); END
        """))
        connection.execute(text("""
            CREATE TRIGGER IF NOT EXISTS prediction_accuracy_observations_no_update
            BEFORE UPDATE ON prediction_accuracy_observations
            BEGIN SELECT RAISE(ABORT, 'prediction accuracy observations are immutable'); END
        """))
        connection.execute(text("""
            CREATE TRIGGER IF NOT EXISTS prediction_accuracy_observations_no_delete
            BEFORE DELETE ON prediction_accuracy_observations
            BEGIN SELECT RAISE(ABORT, 'prediction accuracy observations are immutable'); END
        """))
        accuracy_replace_identity = (
            "id=NEW.id OR score_key=NEW.score_key OR "
            "(prediction_id=NEW.prediction_id AND "
            "close_observation_id=NEW.close_observation_id)"
            if {
                "score_key",
                "prediction_id",
                "close_observation_id",
            }.issubset(accuracy_columns)
            else "id=NEW.id"
        )
        connection.execute(text(f"""
            CREATE TRIGGER IF NOT EXISTS prediction_accuracy_observations_no_replace
            BEFORE INSERT ON prediction_accuracy_observations
            WHEN EXISTS (
                SELECT 1 FROM prediction_accuracy_observations
                WHERE {accuracy_replace_identity}
            )
            BEGIN SELECT RAISE(ABORT, 'prediction accuracy observations are immutable'); END
        """))
        connection.execute(text("""
            CREATE TRIGGER IF NOT EXISTS prediction_accuracy_observations_scope_guard
            BEFORE INSERT ON prediction_accuracy_observations
            WHEN NEW.evidence_scope != 'diagnostic_research_only'
              OR NEW.training_eligible != 0
            BEGIN SELECT RAISE(
                ABORT,
                'legacy prediction accuracy observations must remain diagnostic-only'
            ); END
        """))


def _ensure_prediction_passport_immutability() -> None:
    """Protect issued PredictionPassports from SQLite mutation."""
    if not DATABASE_URL.startswith("sqlite"):
        return
    with engine.begin() as connection:
        connection.execute(text("""
            CREATE TRIGGER IF NOT EXISTS prediction_passports_no_update
            BEFORE UPDATE ON prediction_passports
            BEGIN SELECT RAISE(ABORT, 'prediction passports are immutable'); END
        """))
        connection.execute(text("""
            CREATE TRIGGER IF NOT EXISTS prediction_passports_no_delete
            BEFORE DELETE ON prediction_passports
            BEGIN SELECT RAISE(ABORT, 'prediction passports are immutable'); END
        """))
        # SQLite's INSERT OR REPLACE conflict action can delete and reinsert a
        # row without invoking BEFORE DELETE triggers when recursive_triggers
        # is disabled (the default). Reject every insert that collides with an
        # immutable passport identity before conflict resolution runs.
        connection.execute(text("DROP TRIGGER IF EXISTS prediction_passports_no_replace"))
        connection.execute(text("""
            CREATE TRIGGER prediction_passports_no_replace
            BEFORE INSERT ON prediction_passports
            WHEN EXISTS (
                SELECT 1
                FROM prediction_passports AS existing
                WHERE existing.passport_id = NEW.passport_id
                   OR existing.forecast_id = NEW.forecast_id
                   OR (
                       existing.origin_kind = NEW.origin_kind
                       AND existing.origin_key = NEW.origin_key
                   )
                   OR (
                       NEW.prediction_snapshot_id IS NOT NULL
                       AND existing.prediction_snapshot_id = NEW.prediction_snapshot_id
                   )
            )
            BEGIN SELECT RAISE(ABORT, 'prediction passports are immutable'); END
        """))


def _ensure_market_structure_schema() -> None:
    """Fail fast if retained structure rows cannot honor the live contract."""

    inspector = inspect(engine)
    if not inspector.has_table("market_structure_observations"):
        raise RuntimeError("market_structure_observations was not created")
    actual = {
        column["name"]
        for column in inspector.get_columns("market_structure_observations")
    }
    required = {
        column.name for column in MarketStructureObservation.__table__.columns
    }
    missing = sorted(required - actual)
    if missing:
        raise RuntimeError(
            "market_structure_observations schema is incompatible; missing columns: "
            + ", ".join(missing)
        )


def _ensure_market_structure_immutability() -> None:
    """Protect retained intraday structure observations from mutation."""
    if not DATABASE_URL.startswith("sqlite"):
        return
    with engine.begin() as connection:
        connection.execute(text("""
            CREATE TRIGGER IF NOT EXISTS market_structure_observations_no_update
            BEFORE UPDATE ON market_structure_observations
            BEGIN SELECT RAISE(ABORT, 'market structure observations are immutable'); END
        """))
        connection.execute(text("""
            CREATE TRIGGER IF NOT EXISTS market_structure_observations_no_delete
            BEFORE DELETE ON market_structure_observations
            BEGIN SELECT RAISE(ABORT, 'market structure observations are immutable'); END
        """))
        connection.execute(text("""
            CREATE TRIGGER IF NOT EXISTS market_structure_observations_insert_guard
            BEFORE INSERT ON market_structure_observations
            WHEN NEW.provider != 'databento'
              OR NEW.subscription_generation IS NULL
              OR NEW.subscription_generation <= 0
              OR NEW.reference_price IS NULL
              OR NEW.reference_price <= 0
              OR NEW.universe_sha256 IS NULL
              OR length(NEW.universe_sha256) != 64
              OR NEW.validation_status != 'valid'
            BEGIN SELECT RAISE(ABORT, 'invalid market structure provenance'); END
        """))
        connection.execute(text("""
            CREATE TRIGGER IF NOT EXISTS market_structure_observations_epoch_guard
            BEFORE INSERT ON market_structure_observations
            WHEN NEW.subscription_epoch_id IS NULL
              OR length(NEW.subscription_epoch_id) != 64
              OR NEW.subscription_epoch_id != lower(NEW.subscription_epoch_id)
              OR NEW.subscription_epoch_id GLOB '*[^0-9a-f]*'
            BEGIN SELECT RAISE(ABORT, 'invalid market structure process epoch'); END
        """))


def _ensure_orb_reference_immutability() -> None:
    """Protect retained ORB reference samples and their provenance."""
    if not DATABASE_URL.startswith("sqlite"):
        return
    with engine.begin() as connection:
        # The pre-epoch index and duplicate guard cannot distinguish two
        # backend processes that both start at subscription generation 1.
        connection.execute(text("DROP INDEX IF EXISTS uix_orb_reference_logical_sample"))
        connection.execute(text("""
            CREATE UNIQUE INDEX uix_orb_reference_logical_sample
            ON orb_reference_samples (
                symbol,
                sample_timestamp_utc,
                subscription_epoch_id,
                subscription_generation,
                universe_sha256,
                primary_expiration,
                spot_formula_version,
                risk_free_rate,
                symbol_mapping_version
            )
        """))
        connection.execute(
            text("DROP TRIGGER IF EXISTS orb_reference_samples_existing_guard")
        )
        connection.execute(text("""
            CREATE TRIGGER orb_reference_samples_existing_guard
            BEFORE INSERT ON orb_reference_samples
            WHEN EXISTS (
                SELECT 1 FROM orb_reference_samples AS existing
                WHERE existing.sample_id = NEW.sample_id
                   OR (
                       existing.symbol = NEW.symbol
                       AND existing.sample_timestamp_utc = NEW.sample_timestamp_utc
                       AND existing.subscription_epoch_id = NEW.subscription_epoch_id
                       AND existing.subscription_generation = NEW.subscription_generation
                       AND existing.universe_sha256 = NEW.universe_sha256
                       AND existing.primary_expiration = NEW.primary_expiration
                       AND existing.spot_formula_version = NEW.spot_formula_version
                       AND existing.risk_free_rate = NEW.risk_free_rate
                       AND existing.symbol_mapping_version = NEW.symbol_mapping_version
                   )
            )
            BEGIN SELECT RAISE(ABORT, 'ORB reference sample identity already exists'); END
        """))
        connection.execute(text("""
            CREATE TRIGGER IF NOT EXISTS orb_reference_samples_epoch_guard
            BEFORE INSERT ON orb_reference_samples
            WHEN NEW.subscription_epoch_id IS NULL
              OR length(NEW.subscription_epoch_id) != 64
              OR NEW.subscription_epoch_id != lower(NEW.subscription_epoch_id)
              OR NEW.subscription_epoch_id GLOB '*[^0-9a-f]*'
            BEGIN SELECT RAISE(ABORT, 'invalid ORB reference process epoch'); END
        """))
        connection.execute(text("""
            CREATE TRIGGER IF NOT EXISTS orb_reference_samples_no_update
            BEFORE UPDATE ON orb_reference_samples
            BEGIN SELECT RAISE(ABORT, 'ORB reference samples are immutable'); END
        """))
        connection.execute(text("""
            CREATE TRIGGER IF NOT EXISTS orb_reference_samples_no_delete
            BEFORE DELETE ON orb_reference_samples
            BEGIN SELECT RAISE(ABORT, 'ORB reference samples are immutable'); END
        """))
        connection.execute(text("""
            CREATE TRIGGER IF NOT EXISTS orb_reference_samples_insert_guard
            BEFORE INSERT ON orb_reference_samples
            WHEN NEW.provider != 'databento'
              OR NEW.subscription_generation IS NULL
              OR NEW.subscription_generation <= 0
              OR NEW.reference_price IS NULL
              OR NEW.reference_price <= 0
              OR NEW.spot_source != 'databento_opra_put_call_parity'
              OR NEW.spot_formula_version IS NULL
              OR length(NEW.spot_formula_version) = 0
              OR NEW.time_to_expiration_years IS NULL
              OR NEW.time_to_expiration_years <= 0
              OR NEW.primary_expiration IS NULL
              OR length(NEW.primary_expiration) != 10
              OR NEW.universe_sha256 IS NULL
              OR length(NEW.universe_sha256) != 64
              OR NEW.universe_is_fallback != 0
              OR NEW.minimum_paired_quote_count IS NULL
              OR NEW.minimum_paired_quote_count <= 0
              OR NEW.paired_quote_count < NEW.minimum_paired_quote_count
              OR NEW.contributing_pair_count IS NULL
              OR NEW.contributing_pair_count <= 0
              OR NEW.contributing_pair_count > NEW.paired_quote_count
              OR NEW.contributing_quote_count != NEW.contributing_pair_count * 2
              OR NEW.earliest_ts_event_ns IS NULL
              OR NEW.earliest_ts_event_ns <= 0
              OR NEW.latest_ts_event_ns IS NULL
              OR NEW.latest_ts_event_ns <= 0
              OR NEW.latest_ts_event_ns < NEW.earliest_ts_event_ns
              OR NEW.earliest_ts_recv_ns IS NULL
              OR NEW.earliest_ts_recv_ns <= 0
              OR NEW.latest_ts_recv_ns IS NULL
              OR NEW.latest_ts_recv_ns <= 0
              OR NEW.latest_ts_recv_ns < NEW.earliest_ts_recv_ns
              OR NEW.observation_index_ns IS NULL
              OR NEW.observation_index_ns <= 0
              OR NEW.latest_ts_event_ns > NEW.latest_ts_recv_ns
              OR NEW.source_quote_age_seconds < -0.05
              OR NEW.maximum_source_quote_age_seconds < NEW.source_quote_age_seconds
              OR NEW.source_timestamp_span_seconds < 0
              OR NEW.quote_freshness_limit_seconds <= 0
              OR NEW.maximum_source_quote_age_seconds > NEW.quote_freshness_limit_seconds
              OR NEW.pair_identity_sha256 IS NULL
              OR length(NEW.pair_identity_sha256) != 64
              OR NEW.symbol_mapping_version IS NULL
              OR length(NEW.symbol_mapping_version) != 64
              OR NEW.formula_inputs_json IS NULL
              OR length(NEW.formula_inputs_json) = 0
              OR NEW.processing_clock_status != 'synchronized'
              OR NEW.timestamp_order_valid != 1
              OR NEW.handoff_status != 'active'
              OR NEW.generation_state_unchanged != 1
              OR NEW.universe_state_unchanged != 1
              OR NEW.validation_status != 'valid'
            BEGIN SELECT RAISE(ABORT, 'invalid ORB reference provenance'); END
        """))


def _ensure_orb_reference_schema() -> None:
    """Fail fast if a legacy/partial table cannot honor the sampler contract."""
    inspector = inspect(engine)
    if not inspector.has_table("orb_reference_samples"):
        raise RuntimeError("orb_reference_samples was not created")
    actual = {
        column["name"]
        for column in inspector.get_columns("orb_reference_samples")
    }
    required = {column.name for column in OrbReferenceSample.__table__.columns}
    missing = sorted(required - actual)
    if missing:
        raise RuntimeError(
            "orb_reference_samples schema is incompatible; missing columns: "
            + ", ".join(missing)
        )


def _ensure_orb_reference_decision_schema() -> None:
    """Fail fast unless final ORB progress decisions can be retained."""
    inspector = inspect(engine)
    table_name = "orb_reference_sample_decisions"
    if not inspector.has_table(table_name):
        raise RuntimeError(f"{table_name} was not created")
    actual = {
        column["name"]
        for column in inspector.get_columns(table_name)
    }
    required = {
        column.name
        for column in OrbReferenceSampleDecision.__table__.columns
    }
    missing = sorted(required - actual)
    if missing:
        raise RuntimeError(
            f"{table_name} schema is incompatible; missing columns: "
            + ", ".join(missing)
        )


def _ensure_orb_reference_decision_immutability() -> None:
    """Protect the one final progress decision attached to each raw sample."""
    if not DATABASE_URL.startswith("sqlite"):
        return
    cadence_seconds = max(
        1,
        int(os.getenv("DATABENTO_ORB_REFERENCE_INTERVAL_SECONDS", "5")),
    )
    with engine.begin() as connection:
        connection.execute(text("""
            CREATE UNIQUE INDEX IF NOT EXISTS uix_orb_reference_decision_sample_id
            ON orb_reference_sample_decisions (sample_id)
        """))
        connection.execute(text("""
            CREATE TRIGGER IF NOT EXISTS orb_reference_sample_decisions_existing_guard
            BEFORE INSERT ON orb_reference_sample_decisions
            WHEN EXISTS (
                SELECT 1 FROM orb_reference_sample_decisions AS existing
                WHERE existing.sample_id = NEW.sample_id
            )
            BEGIN SELECT RAISE(ABORT, 'ORB reference sample decision already exists'); END
        """))
        connection.execute(
            text("DROP TRIGGER IF EXISTS orb_reference_sample_decisions_insert_guard")
        )
        connection.execute(text(f"""
            CREATE TRIGGER orb_reference_sample_decisions_insert_guard
            BEFORE INSERT ON orb_reference_sample_decisions
            WHEN NEW.decision_status != 'final'
              OR NEW.progress_eligible NOT IN (0, 1)
              OR (
                  NEW.progress_eligible = 1
                  AND NEW.reason IS NOT NULL
              )
              OR (
                  NEW.progress_eligible = 0
                  AND (NEW.reason IS NULL OR length(NEW.reason) = 0)
              )
              OR NOT EXISTS (
                  SELECT 1 FROM orb_reference_samples AS sample
                  WHERE sample.sample_id = NEW.sample_id
                    AND sample.sample_timestamp_utc = NEW.sample_timestamp_utc
                    AND julianday(NEW.attempt_completed_at_utc)
                        >= julianday(sample.captured_at_utc)
              )
              OR (
                  NEW.progress_eligible = 1
                  AND (
                      NEW.intended_bucket_utc != NEW.sample_timestamp_utc
                      OR CAST(strftime('%s', NEW.intended_bucket_utc) AS INTEGER)
                          % {cadence_seconds} != 0
                      OR julianday(NEW.attempt_completed_at_utc)
                          < julianday(NEW.intended_bucket_utc)
                      OR julianday(NEW.attempt_completed_at_utc)
                          >= julianday(
                              NEW.intended_bucket_utc,
                              '+{cadence_seconds} seconds'
                          )
                  )
              )
            BEGIN SELECT RAISE(ABORT, 'invalid ORB reference sample decision'); END
        """))
        connection.execute(text("""
            CREATE TRIGGER IF NOT EXISTS orb_reference_sample_decisions_no_update
            BEFORE UPDATE ON orb_reference_sample_decisions
            BEGIN SELECT RAISE(ABORT, 'ORB reference sample decisions are immutable'); END
        """))
        connection.execute(text("""
            CREATE TRIGGER IF NOT EXISTS orb_reference_sample_decisions_no_delete
            BEFORE DELETE ON orb_reference_sample_decisions
            BEGIN SELECT RAISE(ABORT, 'ORB reference sample decisions are immutable'); END
        """))


def get_db():
    """Dependency for FastAPI endpoints"""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def _to_py(value: Any) -> Any:
    """Convert numpy/pandas/datetime values into SQLite/JSON-safe primitives."""
    if value is None:
        return None
    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:
            pass
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(key): _to_py(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_to_py(item) for item in value]
    return value


def _json_dumps(value: Any) -> str:
    return json.dumps(_to_py(value), separators=(",", ":"), default=str)


def _canonical_json_bytes(value: Any) -> bytes:
    """Serialize replay inputs deterministically and reject non-finite JSON."""
    return json.dumps(
        _to_py(value),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")


def _naive_utc(value: Any = None) -> datetime:
    if isinstance(value, str):
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    elif isinstance(value, datetime):
        parsed = value
    else:
        parsed = datetime.now(ZoneInfo("UTC"))
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(ZoneInfo("UTC")).replace(tzinfo=None)
    return parsed


def _coerce_float(value: Any) -> float | None:
    """Return a finite float if possible, else ``None``."""
    if value is None:
        return None
    try:
        converted = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(converted):
        return None
    return converted


def _coerce_timestamp(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
    elif isinstance(value, (int, float)):
        converted = _coerce_float(value)
        if converted is None:
            return None
        parsed = datetime.fromtimestamp(converted, tz=ZoneInfo("UTC"))
    elif isinstance(value, date):
        parsed = datetime.combine(value, datetime.min.time())
    else:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=ZoneInfo("UTC"))
    return parsed.astimezone(ZoneInfo("UTC")).replace(tzinfo=None)


def _first_finite_float(*values: Any) -> float | None:
    """Return the first finite numeric value, preserving legitimate zeroes."""
    for value in values:
        if value is None:
            continue
        converted = _coerce_float(value)
        if converted is not None:
            return converted
    return None


def _first_int(*values: Any) -> int | None:
    """Return the first integer-like value, preserving legitimate zeroes."""
    value = _first_finite_float(*values)
    return int(value) if value is not None else None


def _canonical_subscription_epoch(*values: Any) -> str | None:
    for value in values:
        candidate = str(value or "").strip()
        if re.fullmatch(r"[0-9a-f]{64}", candidate) is not None:
            return candidate
    return None


def save_market_snapshot(
    symbol: str,
    timestamp_utc: Any,
    price: float | int | None,
    bid: float | int | None = None,
    ask: float | int | None = None,
    volume: float | int | None = None,
    vwap: float | int | None = None,
) -> MarketSnapshot | None:
    """Persist the underlying spot snapshot used by each gamma calculation."""
    symbol = str(symbol or "").upper().strip()
    if not symbol:
        return None

    spot_price = _coerce_float(price)
    if spot_price is None or spot_price <= 0.0:
        return None

    payload: dict[str, Any] = {
        "symbol": symbol,
        "timestamp_utc": _coerce_timestamp(timestamp_utc) or _naive_utc(),
        "price": spot_price,
        "bid": _coerce_float(bid),
        "ask": _coerce_float(ask),
        "volume": _coerce_float(volume),
        "vwap": _coerce_float(vwap),
    }

    session = SessionLocal()
    try:
        row = MarketSnapshot(**payload)
        session.add(row)
        session.commit()
        session.refresh(row)
        return row
    except Exception as exc:
        session.rollback()
        logger.warning("Failed to persist market snapshot for %s: %s", symbol, exc)
        return None
    finally:
        session.close()


def _market_structure_row_matches(
    row: MarketStructureObservation | None,
    payload: Mapping[str, Any],
) -> bool:
    """Return true only for an exact replay of an immutable observation."""

    if row is None:
        return False
    return all(getattr(row, key) == value for key, value in payload.items())


def save_market_structure_observation(
    observation: Mapping[str, Any],
) -> MarketStructureObservation | None:
    """Append one validated high-frequency structure observation idempotently."""
    observation_id = str(observation.get("observation_id") or "").strip()
    symbol = str(observation.get("symbol") or "").upper().strip()
    source_timestamp_utc = _coerce_timestamp(observation.get("source_timestamp_utc"))
    reference_price = _coerce_float(observation.get("reference_price"))
    validation_status = str(observation.get("validation_status") or "").lower().strip()
    provider = str(observation.get("provider") or "").lower().strip()
    subscription_epoch_id = str(
        observation.get("subscription_epoch_id") or ""
    ).strip()
    subscription_generation = _first_int(observation.get("subscription_generation"))
    universe_sha256 = str(observation.get("universe_sha256") or "").lower().strip()
    if (
        not observation_id
        or not symbol
        or source_timestamp_utc is None
        or reference_price is None
        or reference_price <= 0.0
        or validation_status != "valid"
        or provider != "databento"
        or re.fullmatch(r"[0-9a-f]{64}", subscription_epoch_id) is None
        or subscription_generation is None
        or subscription_generation <= 0
        or re.fullmatch(r"[0-9a-f]{64}", universe_sha256) is None
    ):
        return None

    raw_trading_date = observation.get("trading_date")
    if isinstance(raw_trading_date, datetime):
        trading_date = raw_trading_date.date()
    elif isinstance(raw_trading_date, date):
        trading_date = raw_trading_date
    elif isinstance(raw_trading_date, str):
        try:
            trading_date = date.fromisoformat(raw_trading_date[:10])
        except ValueError:
            return None
    else:
        trading_date = (
            source_timestamp_utc.replace(tzinfo=ZoneInfo("UTC"))
            .astimezone(ZoneInfo("America/New_York"))
            .date()
        )

    payload = {
        "observation_id": observation_id,
        "symbol": symbol,
        "trading_date": trading_date,
        "source_timestamp_utc": source_timestamp_utc,
        "provider": provider,
        "subscription_epoch_id": subscription_epoch_id,
        "subscription_generation": subscription_generation,
        "calculation_id": observation.get("calculation_id"),
        "reference_price": reference_price,
        "spot_source": observation.get("spot_source"),
        "gamma_pin": _coerce_float(observation.get("gamma_pin")),
        "max_pain": _coerce_float(observation.get("max_pain")),
        "zero_gamma": _coerce_float(observation.get("zero_gamma")),
        "pin_lead_ratio": _coerce_float(observation.get("pin_lead_ratio")),
        "pin_is_contested": (
            bool(observation.get("pin_is_contested"))
            if observation.get("pin_is_contested") is not None
            else None
        ),
        "gross_gex": _coerce_float(observation.get("gross_gex")),
        "net_gex": _coerce_float(observation.get("net_gex")),
        "primary_expiration": observation.get("primary_expiration"),
        "same_day_profile_available": (
            bool(observation.get("same_day_profile_available"))
            if observation.get("same_day_profile_available") is not None
            else None
        ),
        "universe_sha256": universe_sha256,
        "validation_status": validation_status,
    }

    session = SessionLocal()
    try:
        existing = session.get(MarketStructureObservation, observation_id)
        if existing is not None:
            if _market_structure_row_matches(existing, payload):
                return existing
            logger.warning(
                "Rejected conflicting market structure observation id %s",
                observation_id,
            )
            return None
        row = MarketStructureObservation(**payload)
        session.add(row)
        session.commit()
        session.refresh(row)
        return row
    except IntegrityError:
        session.rollback()
        existing = session.get(MarketStructureObservation, observation_id)
        if _market_structure_row_matches(existing, payload):
            return existing
        logger.warning(
            "Rejected conflicting concurrent market structure observation id %s",
            observation_id,
        )
        return None
    except Exception as exc:
        session.rollback()
        logger.warning(
            "Failed to persist market structure observation for %s: %s", symbol, exc
        )
        return None
    finally:
        session.close()


def load_market_structure_observations(
    symbol: str,
    trading_date: date,
    *,
    as_of_utc: Any = None,
) -> list[dict[str, Any]]:
    """Load append-only structure evidence for one symbol and trading date."""
    normalized = str(symbol or "").upper().strip()
    if not normalized:
        return []
    cutoff = _coerce_timestamp(as_of_utc) if as_of_utc is not None else None
    # Snapshot projections consume only these scalar fields.  Selecting the
    # complete ORM entity used to hydrate thousands of unnecessary Python
    # objects per symbol, which could make the four-index /v1/orb collection
    # exceed its bounded one-second read budget even though the indexed SQLite
    # queries themselves completed quickly.
    fields = (
        "observation_id",
        "symbol",
        "trading_date",
        "source_timestamp_utc",
        "captured_at_utc",
        "provider",
        "subscription_epoch_id",
        "subscription_generation",
        "calculation_id",
        "reference_price",
        "spot_source",
        "gamma_pin",
        "max_pain",
        "zero_gamma",
        "pin_lead_ratio",
        "pin_is_contested",
        "gross_gex",
        "net_gex",
        "primary_expiration",
        "same_day_profile_available",
        "universe_sha256",
        "validation_status",
    )
    try:
        statement = select(
            *(
                getattr(MarketStructureObservation, field).label(field)
                for field in fields
            )
        ).where(
            MarketStructureObservation.symbol == normalized,
            MarketStructureObservation.trading_date == trading_date,
        )
        if cutoff is not None:
            statement = statement.where(
                MarketStructureObservation.source_timestamp_utc <= cutoff,
                MarketStructureObservation.captured_at_utc <= cutoff,
            )
        statement = statement.order_by(
            MarketStructureObservation.source_timestamp_utc.asc(),
            MarketStructureObservation.observation_id.asc(),
        )
        with engine.connect() as connection:
            return [
                dict(row)
                for row in connection.execute(statement).mappings()
            ]
    except Exception as exc:
        logger.warning(
            "Failed to load market structure observations for %s: %s", normalized, exc
        )
        return []


def _orb_reference_row_matches(
    row: OrbReferenceSample | None,
    payload: Mapping[str, Any],
) -> bool:
    """Return true only for an exact idempotent replay of an immutable sample."""
    if row is None:
        return False
    return all(getattr(row, key) == value for key, value in payload.items())


def save_orb_reference_sample(
    sample: Mapping[str, Any],
) -> OrbReferenceSample | None:
    """Append one provenance-complete ORB reference sample idempotently.

    A reused sample id is accepted only when every persisted value is identical.
    Conflicting content fails closed instead of silently returning the old row.
    """
    sample_id = str(sample.get("sample_id") or "").lower().strip()
    symbol = str(sample.get("symbol") or "").upper().strip()
    sample_timestamp_utc = _coerce_timestamp(sample.get("sample_timestamp_utc"))
    source_timestamp_utc = _coerce_timestamp(sample.get("source_timestamp_utc"))
    captured_at_utc = _coerce_timestamp(sample.get("captured_at_utc"))
    provider = str(sample.get("provider") or "").lower().strip()
    subscription_epoch_id = str(
        sample.get("subscription_epoch_id") or ""
    ).strip()
    generation = _first_int(sample.get("subscription_generation"))
    reference_price = _coerce_float(sample.get("reference_price"))
    spot_source = str(sample.get("spot_source") or "").lower().strip()
    spot_formula_version = str(sample.get("spot_formula_version") or "").strip()
    risk_free_rate = _coerce_float(sample.get("risk_free_rate"))
    time_to_expiration_years = _coerce_float(
        sample.get("time_to_expiration_years")
    )
    primary_expiration = str(sample.get("primary_expiration") or "").strip()
    universe_sha256 = str(sample.get("universe_sha256") or "").lower().strip()
    paired_quote_count = _first_int(sample.get("paired_quote_count"))
    minimum_pair_count = _first_int(sample.get("minimum_paired_quote_count"))
    contributing_pair_count = _first_int(sample.get("contributing_pair_count"))
    contributing_quote_count = _first_int(sample.get("contributing_quote_count"))
    earliest_ts_event_ns = _first_int(sample.get("earliest_ts_event_ns"))
    latest_ts_event_ns = _first_int(sample.get("latest_ts_event_ns"))
    earliest_ts_recv_ns = _first_int(sample.get("earliest_ts_recv_ns"))
    latest_ts_recv_ns = _first_int(sample.get("latest_ts_recv_ns"))
    observation_index_ns = _first_int(sample.get("observation_index_ns"))
    source_quote_age_seconds = _coerce_float(sample.get("source_quote_age_seconds"))
    maximum_source_quote_age_seconds = _coerce_float(
        sample.get("maximum_source_quote_age_seconds")
    )
    source_timestamp_span_seconds = _coerce_float(
        sample.get("source_timestamp_span_seconds")
    )
    quote_freshness_limit_seconds = _coerce_float(
        sample.get("quote_freshness_limit_seconds")
    )
    pair_identity_sha256 = str(sample.get("pair_identity_sha256") or "").lower().strip()
    symbol_mapping_version = str(sample.get("symbol_mapping_version") or "").lower().strip()
    raw_formula_inputs = sample.get("formula_inputs_json")
    if isinstance(raw_formula_inputs, str):
        try:
            parsed_formula_inputs = json.loads(raw_formula_inputs)
        except json.JSONDecodeError:
            parsed_formula_inputs = None
        formula_inputs = (
            dict(parsed_formula_inputs)
            if isinstance(parsed_formula_inputs, Mapping)
            else None
        )
    elif isinstance(raw_formula_inputs, Mapping):
        formula_inputs = dict(raw_formula_inputs)
    else:
        formula_inputs = None
    try:
        formula_inputs_json = (
            json.dumps(
                formula_inputs,
                sort_keys=True,
                separators=(",", ":"),
                allow_nan=False,
            )
            if formula_inputs is not None
            else None
        )
    except (TypeError, ValueError):
        formula_inputs_json = None
    validation_status = str(sample.get("validation_status") or "").lower().strip()
    configured_quote_freshness_seconds = max(
        0.1,
        float(os.getenv("DATABENTO_QUOTE_FRESHNESS_SECONDS", "10")),
    )
    explicit_bools = (
        sample.get("same_day_profile_available"),
        sample.get("universe_is_fallback"),
        sample.get("timestamp_order_valid"),
        sample.get("generation_state_unchanged"),
        sample.get("universe_state_unchanged"),
    )
    try:
        parsed_primary_expiration = date.fromisoformat(primary_expiration)
    except ValueError:
        parsed_primary_expiration = None
    if (
        re.fullmatch(r"[0-9a-f]{64}", sample_id) is None
        or not symbol
        or sample_timestamp_utc is None
        or source_timestamp_utc is None
        or captured_at_utc is None
        or provider != "databento"
        or re.fullmatch(r"[0-9a-f]{64}", subscription_epoch_id) is None
        or generation is None
        or generation <= 0
        or reference_price is None
        or reference_price <= 0.0
        or spot_source != "databento_opra_put_call_parity"
        or not spot_formula_version
        or risk_free_rate is None
        or time_to_expiration_years is None
        or time_to_expiration_years <= 0.0
        or parsed_primary_expiration is None
        or re.fullmatch(r"[0-9a-f]{64}", universe_sha256) is None
        or paired_quote_count is None
        or minimum_pair_count is None
        or minimum_pair_count <= 0
        or paired_quote_count < minimum_pair_count
        or contributing_pair_count is None
        or contributing_pair_count <= 0
        or contributing_pair_count > paired_quote_count
        or contributing_quote_count != contributing_pair_count * 2
        or earliest_ts_event_ns is None
        or earliest_ts_event_ns <= 0
        or latest_ts_event_ns is None
        or latest_ts_event_ns <= 0
        or latest_ts_event_ns < earliest_ts_event_ns
        or earliest_ts_recv_ns is None
        or earliest_ts_recv_ns <= 0
        or latest_ts_recv_ns is None
        or latest_ts_recv_ns <= 0
        or latest_ts_recv_ns < earliest_ts_recv_ns
        or observation_index_ns is None
        or observation_index_ns <= 0
        or latest_ts_event_ns > latest_ts_recv_ns
        or source_quote_age_seconds is None
        or source_quote_age_seconds < -0.05
        or maximum_source_quote_age_seconds is None
        or maximum_source_quote_age_seconds < source_quote_age_seconds
        or source_timestamp_span_seconds is None
        or source_timestamp_span_seconds < 0.0
        or quote_freshness_limit_seconds is None
        or quote_freshness_limit_seconds <= 0.0
        or not math.isclose(
            quote_freshness_limit_seconds,
            configured_quote_freshness_seconds,
            rel_tol=0.0,
            abs_tol=1e-9,
        )
        or maximum_source_quote_age_seconds > configured_quote_freshness_seconds
        or re.fullmatch(r"[0-9a-f]{64}", pair_identity_sha256) is None
        or re.fullmatch(r"[0-9a-f]{64}", symbol_mapping_version) is None
        or formula_inputs_json is None
        or formula_inputs.get("formula_version") != spot_formula_version
        or str(sample.get("processing_clock_status") or "") != "synchronized"
        or str(sample.get("handoff_status") or "") != "active"
        or any(not isinstance(value, bool) for value in explicit_bools)
        or sample.get("universe_is_fallback") is not False
        or sample.get("timestamp_order_valid") is not True
        or sample.get("generation_state_unchanged") is not True
        or sample.get("universe_state_unchanged") is not True
        or validation_status != "valid"
    ):
        return None

    raw_trading_date = sample.get("trading_date")
    if isinstance(raw_trading_date, datetime):
        trading_date = raw_trading_date.date()
    elif isinstance(raw_trading_date, date):
        trading_date = raw_trading_date
    elif isinstance(raw_trading_date, str):
        try:
            trading_date = date.fromisoformat(raw_trading_date[:10])
        except ValueError:
            return None
    else:
        return None

    cadence_seconds = max(
        1,
        int(os.getenv("DATABENTO_ORB_REFERENCE_INTERVAL_SECONDS", "5")),
    )
    sample_aware = sample_timestamp_utc.replace(tzinfo=ZoneInfo("UTC"))
    source_aware = source_timestamp_utc.replace(tzinfo=ZoneInfo("UTC"))
    captured_aware = captured_at_utc.replace(tzinfo=ZoneInfo("UTC"))
    sample_et = sample_aware.astimezone(ZoneInfo("America/New_York"))
    source_et = source_aware.astimezone(ZoneInfo("America/New_York"))
    capture_offset = (captured_aware - sample_aware).total_seconds()
    source_age = (captured_aware - source_aware).total_seconds()
    expected_maximum_age = (
        captured_aware.timestamp() - earliest_ts_recv_ns / 1_000_000_000
    )
    expected_span = (latest_ts_recv_ns - earliest_ts_recv_ns) / 1_000_000_000
    recv_time = datetime.fromtimestamp(
        latest_ts_recv_ns / 1_000_000_000,
        tz=ZoneInfo("UTC"),
    )
    if (
        trading_date != sample_et.date()
        or source_et.date() != trading_date
        or sample.get("same_day_profile_available")
        is not (parsed_primary_expiration == trading_date)
        or sample_timestamp_utc.microsecond != 0
        or int(sample_aware.timestamp()) % cadence_seconds != 0
        or not (9 <= sample_et.hour < 16)
        or (sample_et.hour == 9 and sample_et.minute < 30)
        or not (9 <= source_et.hour < 16)
        or (source_et.hour == 9 and source_et.minute < 30)
        or not 0.0 <= capture_offset < cadence_seconds
        or source_age < -0.05
        or source_age > configured_quote_freshness_seconds
        or abs((recv_time - source_aware).total_seconds()) > 0.000002
        or not math.isclose(
            source_quote_age_seconds,
            source_age,
            rel_tol=0.0,
            abs_tol=0.000002,
        )
        or not math.isclose(
            maximum_source_quote_age_seconds,
            expected_maximum_age,
            rel_tol=0.0,
            abs_tol=0.000002,
        )
        or not math.isclose(
            source_timestamp_span_seconds,
            expected_span,
            rel_tol=0.0,
            abs_tol=0.000002,
        )
    ):
        return None

    canonical_identity = json.dumps(
        {
            "symbol": symbol,
            "sample_timestamp_utc": sample_aware.isoformat().replace("+00:00", "Z"),
            "subscription_epoch_id": subscription_epoch_id,
            "subscription_generation": generation,
            "universe_sha256": universe_sha256,
            "primary_expiration": parsed_primary_expiration.isoformat(),
            "spot_formula_version": spot_formula_version,
            "risk_free_rate": risk_free_rate,
            "symbol_mapping_version": symbol_mapping_version,
        },
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    if hashlib.sha256(canonical_identity).hexdigest() != sample_id:
        return None

    payload = {
        "sample_id": sample_id,
        "symbol": symbol,
        "trading_date": trading_date,
        "sample_timestamp_utc": sample_timestamp_utc,
        "source_timestamp_utc": source_timestamp_utc,
        "captured_at_utc": captured_at_utc,
        "provider": provider,
        "subscription_epoch_id": subscription_epoch_id,
        "subscription_generation": generation,
        "reference_price": reference_price,
        "spot_source": spot_source,
        "spot_formula_version": spot_formula_version,
        "risk_free_rate": risk_free_rate,
        "time_to_expiration_years": time_to_expiration_years,
        "primary_expiration": parsed_primary_expiration.isoformat(),
        "same_day_profile_available": sample["same_day_profile_available"],
        "universe_sha256": universe_sha256,
        "universe_is_fallback": sample["universe_is_fallback"],
        "paired_quote_count": paired_quote_count,
        "minimum_paired_quote_count": minimum_pair_count,
        "contributing_pair_count": contributing_pair_count,
        "contributing_quote_count": contributing_quote_count,
        "earliest_ts_event_ns": earliest_ts_event_ns,
        "latest_ts_event_ns": latest_ts_event_ns,
        "earliest_ts_recv_ns": earliest_ts_recv_ns,
        "latest_ts_recv_ns": latest_ts_recv_ns,
        "observation_index_ns": observation_index_ns,
        "source_quote_age_seconds": source_quote_age_seconds,
        "maximum_source_quote_age_seconds": maximum_source_quote_age_seconds,
        "source_timestamp_span_seconds": source_timestamp_span_seconds,
        "quote_freshness_limit_seconds": quote_freshness_limit_seconds,
        "pair_identity_sha256": pair_identity_sha256,
        "symbol_mapping_version": symbol_mapping_version,
        "formula_inputs_json": formula_inputs_json,
        "processing_clock_status": "synchronized",
        "timestamp_order_valid": True,
        "handoff_status": "active",
        "generation_state_unchanged": True,
        "universe_state_unchanged": True,
        "validation_status": "valid",
    }

    session = SessionLocal()
    try:
        existing = session.get(OrbReferenceSample, sample_id)
        if existing is not None:
            if _orb_reference_row_matches(existing, payload):
                return existing
            logger.warning("Rejected conflicting ORB reference sample id %s", sample_id)
            return None
        row = OrbReferenceSample(**payload)
        session.add(row)
        session.commit()
        session.refresh(row)
        return row
    except IntegrityError:
        session.rollback()
        existing = session.get(OrbReferenceSample, sample_id)
        if _orb_reference_row_matches(existing, payload):
            return existing
        logger.warning("Rejected conflicting concurrent ORB reference sample id %s", sample_id)
        return None
    except Exception as exc:
        session.rollback()
        logger.warning("Failed to persist ORB reference sample for %s: %s", symbol, exc)
        return None
    finally:
        session.close()


def _orb_reference_decision_row_matches(
    row: OrbReferenceSampleDecision | None,
    payload: Mapping[str, Any],
) -> bool:
    """Return true only for an exact replay of one immutable final decision."""
    if row is None:
        return False
    return all(getattr(row, key) == value for key, value in payload.items())


def save_orb_reference_sample_decision(
    decision: Mapping[str, Any],
) -> OrbReferenceSampleDecision | None:
    """Append the one final progress decision for a retained ORB sample.

    A raw reference row without this sidecar is deliberately pending and is
    never returned by the eligible-only loaders below.  Conflicting duplicate
    decisions fail closed; only an exact replay is idempotent.
    """
    sample_id = str(decision.get("sample_id") or "").lower().strip()
    sample_timestamp = _coerce_timestamp(decision.get("sample_timestamp_utc"))
    intended = _coerce_timestamp(decision.get("intended_bucket_utc"))
    completed = _coerce_timestamp(decision.get("attempt_completed_at_utc"))
    eligible = decision.get("progress_eligible")
    raw_reason = decision.get("reason")
    reason = str(raw_reason).strip() if raw_reason is not None else None
    status = str(decision.get("decision_status") or "final").lower().strip()
    cadence_seconds = max(
        1,
        int(os.getenv("DATABENTO_ORB_REFERENCE_INTERVAL_SECONDS", "5")),
    )
    if (
        re.fullmatch(r"[0-9a-f]{64}", sample_id) is None
        or sample_timestamp is None
        or intended is None
        or completed is None
        or not isinstance(eligible, bool)
        or status != "final"
        or (eligible and reason is not None)
        or (
            not eligible
            and (
                reason is None
                or re.fullmatch(r"[A-Z][A-Z0-9_]{0,95}", reason) is None
            )
        )
        or completed < intended
    ):
        return None
    if eligible:
        intended_aware = intended.replace(tzinfo=ZoneInfo("UTC"))
        completion_offset = (completed - intended).total_seconds()
        if (
            sample_timestamp != intended
            or intended.microsecond != 0
            or int(intended_aware.timestamp()) % cadence_seconds != 0
            or not 0.0 <= completion_offset < cadence_seconds
        ):
            return None

    payload = {
        "sample_id": sample_id,
        "sample_timestamp_utc": sample_timestamp,
        "intended_bucket_utc": intended,
        "attempt_completed_at_utc": completed,
        "progress_eligible": eligible,
        "reason": reason,
        "decision_status": "final",
    }
    session = SessionLocal()
    try:
        raw_sample = session.get(OrbReferenceSample, sample_id)
        if (
            raw_sample is None
            or raw_sample.sample_timestamp_utc != sample_timestamp
            or completed < raw_sample.captured_at_utc
        ):
            return None
        existing = session.get(OrbReferenceSampleDecision, sample_id)
        if existing is not None:
            if _orb_reference_decision_row_matches(existing, payload):
                return existing
            logger.warning(
                "Rejected conflicting ORB reference decision for sample %s",
                sample_id,
            )
            return None
        row = OrbReferenceSampleDecision(**payload)
        session.add(row)
        session.commit()
        session.refresh(row)
        return row
    except IntegrityError:
        session.rollback()
        existing = session.get(OrbReferenceSampleDecision, sample_id)
        if _orb_reference_decision_row_matches(existing, payload):
            return existing
        logger.warning(
            "Rejected conflicting concurrent ORB reference decision for sample %s",
            sample_id,
        )
        return None
    except Exception as exc:
        session.rollback()
        logger.warning(
            "Failed to persist ORB reference decision for sample %s: %s",
            sample_id,
            exc,
        )
        return None
    finally:
        session.close()


def reconcile_orb_reference_sample_decisions(
    *,
    as_of_utc: Any = None,
    maximum_rows: int | None = None,
) -> dict[str, int]:
    """Fail closed on expired raw ORB samples left without a final decision.

    The live sampler must retain the raw reference before it can perform its
    post-persist runtime/deadline checks.  A hard process interruption between
    those two appends therefore leaves a raw row with no immutable sidecar.
    Such a row can never be proven progress-eligible after restart.  Once its
    cadence bucket has closed, append a deterministic ineligible decision so
    startup is idempotent and later valid samples are not blocked forever.

    Raw samples are never updated or deleted, and this recovery path never
    promotes a sample.  A nonzero remaining count continues to fail closed in
    the eligible-only loaders and opening-acceptance gate.
    """
    observed_at = _coerce_timestamp(as_of_utc) or _naive_utc()
    cadence_seconds = max(
        1,
        int(os.getenv("DATABENTO_ORB_REFERENCE_INTERVAL_SECONDS", "5")),
    )
    if maximum_rows is None:
        try:
            maximum_rows = int(
                os.getenv("DATABENTO_ORB_DECISION_RECONCILE_MAX_ROWS", "20000")
            )
        except (TypeError, ValueError):
            maximum_rows = 20_000
    row_limit = min(100_000, max(1, int(maximum_rows)))
    expired_cutoff = observed_at - timedelta(seconds=cadence_seconds)

    def pending_query(session):
        return (
            session.query(OrbReferenceSample)
            .outerjoin(
                OrbReferenceSampleDecision,
                OrbReferenceSampleDecision.sample_id
                == OrbReferenceSample.sample_id,
            )
            .filter(
                OrbReferenceSampleDecision.sample_id.is_(None),
                OrbReferenceSample.sample_timestamp_utc <= expired_cutoff,
                OrbReferenceSample.captured_at_utc <= observed_at,
            )
        )

    reconciled_count = 0
    failed_count = 0
    # A second pass handles the narrow case where another initializing process
    # wins the same immutable insert after this process reads the pending set.
    for attempt in range(2):
        session = SessionLocal()
        try:
            pending_rows = (
                pending_query(session)
                .order_by(
                    OrbReferenceSample.sample_timestamp_utc.asc(),
                    OrbReferenceSample.sample_id.asc(),
                )
                .limit(row_limit)
                .all()
            )
            if not pending_rows:
                break
            for sample in pending_rows:
                intended = sample.sample_timestamp_utc
                completed = max(
                    sample.captured_at_utc,
                    intended + timedelta(seconds=cadence_seconds),
                )
                session.add(
                    OrbReferenceSampleDecision(
                        sample_id=sample.sample_id,
                        sample_timestamp_utc=sample.sample_timestamp_utc,
                        intended_bucket_utc=intended,
                        attempt_completed_at_utc=completed,
                        progress_eligible=False,
                        reason=(
                            "REFERENCE_PROGRESS_DECISION_MISSING_AT_BUCKET_CLOSE"
                        ),
                        decision_status="final",
                    )
                )
            session.commit()
            reconciled_count += len(pending_rows)
            break
        except IntegrityError as exc:
            session.rollback()
            if attempt == 1:
                failed_count = len(pending_rows)
                logger.warning(
                    "Failed to reconcile interrupted ORB reference decisions: %s",
                    exc,
                )
        except Exception as exc:
            session.rollback()
            failed_count = len(locals().get("pending_rows", ())) or 1
            logger.warning(
                "Failed to reconcile interrupted ORB reference decisions: %s",
                exc,
            )
            break
        finally:
            session.close()

    verification_session = SessionLocal()
    try:
        remaining = pending_query(verification_session).count()
    except Exception as exc:
        logger.warning(
            "Failed to verify interrupted ORB reference reconciliation: %s",
            exc,
        )
        remaining = max(1, failed_count)
    finally:
        verification_session.close()
    return {
        "reconciled_count": reconciled_count,
        "remaining_expired_pending_count": int(remaining),
        "failed_count": failed_count,
    }


def _orb_reference_decision_is_read_eligible(
    *,
    sample_timestamp_utc: datetime,
    captured_at_utc: datetime,
    decision_sample_timestamp_utc: datetime,
    intended_bucket_utc: datetime,
    attempt_completed_at_utc: datetime,
    progress_eligible: Any,
    decision_status: Any,
) -> bool:
    """Defensively revalidate final timing even if SQL guards were bypassed."""
    cadence_seconds = max(
        1,
        int(os.getenv("DATABENTO_ORB_REFERENCE_INTERVAL_SECONDS", "5")),
    )
    intended_aware = (
        intended_bucket_utc.replace(tzinfo=ZoneInfo("UTC"))
        if intended_bucket_utc.tzinfo is None
        else intended_bucket_utc.astimezone(ZoneInfo("UTC"))
    )
    return bool(
        progress_eligible is True
        and decision_status == "final"
        and decision_sample_timestamp_utc == sample_timestamp_utc
        and intended_bucket_utc == sample_timestamp_utc
        and intended_aware.microsecond == 0
        and int(intended_aware.timestamp()) % cadence_seconds == 0
        and attempt_completed_at_utc >= captured_at_utc
        and 0.0
        <= (attempt_completed_at_utc - intended_bucket_utc).total_seconds()
        < cadence_seconds
    )


def load_orb_reference_samples(
    symbol: str,
    trading_date: date,
    *,
    as_of_utc: Any = None,
) -> list[dict[str, Any]]:
    """Load immutable 5-second reference samples for one market session."""
    normalized = str(symbol or "").upper().strip()
    if not normalized:
        return []
    cutoff = _coerce_timestamp(as_of_utc) if as_of_utc is not None else None
    session = SessionLocal()
    try:
        query = session.query(
            OrbReferenceSample,
            OrbReferenceSampleDecision,
        ).join(
            OrbReferenceSampleDecision,
            OrbReferenceSampleDecision.sample_id == OrbReferenceSample.sample_id,
        ).filter(
            OrbReferenceSample.symbol == normalized,
            OrbReferenceSample.trading_date == trading_date,
            OrbReferenceSampleDecision.decision_status == "final",
            OrbReferenceSampleDecision.progress_eligible.is_(True),
        )
        if cutoff is not None:
            query = query.filter(
                OrbReferenceSample.sample_timestamp_utc <= cutoff,
                OrbReferenceSample.captured_at_utc <= cutoff,
            )
        rows = query.order_by(
            OrbReferenceSample.sample_timestamp_utc.asc(),
            OrbReferenceSample.sample_id.asc(),
        ).all()
        fields = (
            "sample_id",
            "symbol",
            "trading_date",
            "sample_timestamp_utc",
            "source_timestamp_utc",
            "captured_at_utc",
            "provider",
            "subscription_epoch_id",
            "subscription_generation",
            "reference_price",
            "spot_source",
            "spot_formula_version",
            "risk_free_rate",
            "time_to_expiration_years",
            "primary_expiration",
            "same_day_profile_available",
            "universe_sha256",
            "universe_is_fallback",
            "paired_quote_count",
            "minimum_paired_quote_count",
            "contributing_pair_count",
            "contributing_quote_count",
            "earliest_ts_event_ns",
            "latest_ts_event_ns",
            "earliest_ts_recv_ns",
            "latest_ts_recv_ns",
            "observation_index_ns",
            "source_quote_age_seconds",
            "maximum_source_quote_age_seconds",
            "source_timestamp_span_seconds",
            "quote_freshness_limit_seconds",
            "pair_identity_sha256",
            "symbol_mapping_version",
            "formula_inputs_json",
            "processing_clock_status",
            "timestamp_order_valid",
            "handoff_status",
            "generation_state_unchanged",
            "universe_state_unchanged",
            "validation_status",
        )
        return [
            {
                **{field: getattr(sample, field) for field in fields},
                "progress_eligible": True,
                "progress_decision_status": decision.decision_status,
                "progress_decision_reason": decision.reason,
                "progress_decided_at_utc": decision.attempt_completed_at_utc,
            }
            for sample, decision in rows
            if _orb_reference_decision_is_read_eligible(
                sample_timestamp_utc=sample.sample_timestamp_utc,
                captured_at_utc=sample.captured_at_utc,
                decision_sample_timestamp_utc=decision.sample_timestamp_utc,
                intended_bucket_utc=decision.intended_bucket_utc,
                attempt_completed_at_utc=decision.attempt_completed_at_utc,
                progress_eligible=decision.progress_eligible,
                decision_status=decision.decision_status,
            )
        ]
    except Exception as exc:
        logger.warning("Failed to load ORB reference samples for %s: %s", normalized, exc)
        return []
    finally:
        session.close()


def load_orb_reference_snapshot_samples(
    symbol: str,
    trading_date: date,
    *,
    as_of_utc: Any = None,
) -> list[dict[str, Any]]:
    """Load only the immutable fields needed to project an ORB snapshot.

    The full loader above intentionally retains the formula-input audit payload
    for replay and verification.  Live ORB reads do not consume that payload,
    so hydrating it on every request makes endpoint cost grow needlessly over
    the trading day. Select retained IDs from one ordered metadata scan, then
    load only the opening rows, latest row, and earliest row per provenance
    identity. This avoids both the repeated ROW_NUMBER sort and hydrating every
    post-opening scalar row while retaining the exact evidence selection.
    """
    normalized = str(symbol or "").upper().strip()
    if not normalized:
        return []
    cutoff = _coerce_timestamp(as_of_utc) if as_of_utc is not None else None
    fields = (
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
    )
    session = SessionLocal()
    try:
        detail_query = session.query(
            *(getattr(OrbReferenceSample, field) for field in fields),
            OrbReferenceSampleDecision.decision_status,
            OrbReferenceSampleDecision.reason,
            OrbReferenceSampleDecision.attempt_completed_at_utc,
            OrbReferenceSampleDecision.sample_timestamp_utc.label(
                "decision_sample_timestamp_utc"
            ),
            OrbReferenceSampleDecision.intended_bucket_utc,
            OrbReferenceSampleDecision.progress_eligible.label(
                "decision_progress_eligible"
            ),
        ).join(
            OrbReferenceSampleDecision,
            OrbReferenceSampleDecision.sample_id == OrbReferenceSample.sample_id,
        )
        query = detail_query.filter(
            OrbReferenceSample.symbol == normalized,
            OrbReferenceSample.trading_date == trading_date,
            OrbReferenceSampleDecision.decision_status == "final",
            OrbReferenceSampleDecision.progress_eligible.is_(True),
        )
        if cutoff is not None:
            query = query.filter(
                OrbReferenceSample.sample_timestamp_utc <= cutoff,
                OrbReferenceSample.captured_at_utc <= cutoff,
            )
        opening_start_utc = datetime(
            trading_date.year,
            trading_date.month,
            trading_date.day,
            9,
            30,
            tzinfo=ZoneInfo("America/New_York"),
        ).astimezone(ZoneInfo("UTC")).replace(tzinfo=None)
        opening_end_utc = opening_start_utc + timedelta(hours=1)
        ordered_rows = query.with_entities(
            OrbReferenceSample.sample_id,
            OrbReferenceSample.sample_timestamp_utc,
            OrbReferenceSample.provider,
            OrbReferenceSample.subscription_epoch_id,
            OrbReferenceSample.subscription_generation,
            OrbReferenceSample.universe_sha256,
        ).order_by(
            OrbReferenceSample.sample_timestamp_utc.asc(),
            OrbReferenceSample.sample_id.asc(),
        ).yield_per(256)
        retained_ids = set()
        seen_identities = set()
        latest_row = None
        for row in ordered_rows:
            identity = (
                row.provider, row.subscription_epoch_id,
                row.subscription_generation, row.universe_sha256,
            )
            if (opening_start_utc <= row.sample_timestamp_utc < opening_end_utc
                    or identity not in seen_identities):
                retained_ids.add(row.sample_id)
            seen_identities.add(identity)
            latest_row = row
        if latest_row is not None:
            retained_ids.add(latest_row.sample_id)
        rows = detail_query.filter(
            OrbReferenceSample.sample_id.in_(retained_ids)
        ).order_by(
            OrbReferenceSample.sample_timestamp_utc.asc(),
            OrbReferenceSample.sample_id.asc(),
        ).all() if retained_ids else []
        return [
            {
                **{
                    field: getattr(row, field)
                    for field in fields
                },
                "progress_eligible": True,
                "progress_decision_status": row.decision_status,
                "progress_decision_reason": row.reason,
                "progress_decided_at_utc": row.attempt_completed_at_utc,
            }
            for row in rows
            if _orb_reference_decision_is_read_eligible(
                sample_timestamp_utc=row.sample_timestamp_utc,
                captured_at_utc=row.captured_at_utc,
                decision_sample_timestamp_utc=row.decision_sample_timestamp_utc,
                intended_bucket_utc=row.intended_bucket_utc,
                attempt_completed_at_utc=row.attempt_completed_at_utc,
                progress_eligible=row.decision_progress_eligible,
                decision_status=row.decision_status,
            )
        ]
    except Exception as exc:
        logger.warning(
            "Failed to load ORB reference snapshot samples for %s: %s",
            normalized,
            exc,
        )
        return []
    finally:
        session.close()


def save_gamma_calculation_inputs(run: dict[str, Any], input_payload: dict[str, Any]) -> dict[str, Any] | None:
    """Atomically append one calculation run and its compressed full input set."""
    calculation_id = str(run.get("calculation_id") or uuid.uuid4())
    calculated_at_utc = _naive_utc(run.get("calculated_at_utc"))
    trading_date = calculated_at_utc.replace(tzinfo=ZoneInfo("UTC")).astimezone(ZoneInfo("America/New_York")).date()
    provider = str(run.get("provider") or "unknown").lower().strip()
    run_epoch_id = _canonical_subscription_epoch(run.get("subscription_epoch_id"))
    input_epoch_id = _canonical_subscription_epoch(
        input_payload.get("subscription_epoch_id")
    )
    run_generation = _first_int(run.get("subscription_generation"))
    input_generation = _first_int(input_payload.get("subscription_generation"))
    if provider == "databento" and (
        run_epoch_id is None
        or input_epoch_id is None
        or run_epoch_id != input_epoch_id
        or run_generation is None
        or input_generation is None
        or run_generation <= 0
        or input_generation <= 0
        or run_generation != input_generation
    ):
        logger.warning(
            "Rejected Databento gamma inputs %s without matching process identity",
            calculation_id,
        )
        return None
    if (
        run_generation is not None
        and input_generation is not None
        and run_generation != input_generation
    ):
        logger.warning(
            "Rejected gamma inputs %s with conflicting subscription generation",
            calculation_id,
        )
        return None
    subscription_epoch_id = run_epoch_id or input_epoch_id
    subscription_generation = (
        run_generation if run_generation is not None else input_generation
    )
    try:
        canonical = _canonical_json_bytes(input_payload)
        digest = hashlib.sha256(canonical).hexdigest()
        compressed = zlib.compress(canonical, level=6)
    except (TypeError, ValueError) as exc:
        logger.warning("Rejected non-canonical gamma inputs %s: %s", calculation_id, exc)
        return None
    session = SessionLocal()
    try:
        row = GammaCalculationRun(
            calculation_id=calculation_id,
            symbol=str(run.get("symbol") or "").upper(),
            trading_date=trading_date,
            calculated_at_utc=calculated_at_utc,
            provider=provider,
            subscription_epoch_id=subscription_epoch_id,
            subscription_generation=subscription_generation,
            status=str(run.get("status") or "invalid"),
            input_schema_version=str(input_payload.get("input_schema_version") or "gamma-inputs-v1"),
            formula_version=str(run.get("formula_version") or "unknown"),
            target_formula_version=run.get("target_formula_version"),
            spot_formula_version=run.get("spot_formula_version"),
            universe_sha256=run.get("universe_sha256"),
            risk_free_rate=run.get("risk_free_rate"),
            contract_multiplier=run.get("contract_multiplier"),
            spot_price=run.get("spot_price"),
            gamma_pin=run.get("gamma_pin"),
            selected_target=run.get("selected_target"),
            primary_expiration_target=run.get("primary_expiration_target"),
            multi_expiration_target=run.get("multi_expiration_target"),
            zero_gamma=run.get("zero_gamma"),
            max_pain=run.get("max_pain"),
            call_gex=run.get("call_gex"),
            put_gex=run.get("put_gex"),
            gross_gex=run.get("gross_gex"),
            net_gex=run.get("net_gex"),
            chain_row_count=int(run.get("chain_row_count") or 0),
            gex_row_count=int(run.get("gex_row_count") or 0),
            rejection_counts_json=_json_dumps(run.get("rejection_counts") or {}),
        )
        with session.begin():
            session.add(row)
            session.flush()
            session.add(GammaCalculationInputBlob(
                calculation_run_id=row.id,
                encoding="canonical-json+zlib-v1",
                payload_sha256=digest,
                uncompressed_bytes=len(canonical),
                compressed_bytes=len(compressed),
                payload=compressed,
            ))
        return {
            "calculation_id": calculation_id,
            "subscription_epoch_id": subscription_epoch_id,
            "subscription_generation": subscription_generation,
            "payload_sha256": digest,
            "uncompressed_bytes": len(canonical),
            "compressed_bytes": len(compressed),
        }
    except IntegrityError:
        session.rollback()
        logger.warning("Duplicate gamma calculation_id rejected: %s", calculation_id)
        return None
    except Exception as exc:
        session.rollback()
        logger.warning("Failed to save gamma calculation inputs %s: %s", calculation_id, exc)
        return None
    finally:
        session.close()


def load_gamma_calculation_inputs(calculation_id: str) -> dict[str, Any] | None:
    """Load and verify a replay input capture; corruption fails closed."""
    session = SessionLocal()
    try:
        run = session.query(GammaCalculationRun).filter(GammaCalculationRun.calculation_id == calculation_id).first()
        if run is None:
            return None
        blob = session.query(GammaCalculationInputBlob).filter(GammaCalculationInputBlob.calculation_run_id == run.id).first()
        if blob is None:
            raise ValueError(f"Missing input blob for calculation {calculation_id}")
        canonical = zlib.decompress(blob.payload)
        if len(canonical) != blob.uncompressed_bytes:
            raise ValueError(f"Input size mismatch for calculation {calculation_id}")
        if hashlib.sha256(canonical).hexdigest() != blob.payload_sha256:
            raise ValueError(f"Input hash mismatch for calculation {calculation_id}")
        return json.loads(canonical.decode("utf-8"))
    finally:
        session.close()


def _trading_date(timestamp: datetime | None = None) -> date:
    value = timestamp or datetime.now(ZoneInfo("UTC"))
    if value.tzinfo is None:
        value = value.replace(tzinfo=ZoneInfo("UTC"))
    return value.astimezone(ZoneInfo("America/New_York")).date()


def _append_prediction_jsonl(payload: dict[str, Any]) -> None:
    PREDICTION_EXPORT_DIR.mkdir(parents=True, exist_ok=True)
    trading_date = payload.get("trading_date") or _trading_date().isoformat()
    path = PREDICTION_EXPORT_DIR / f"{trading_date}.ndjson"
    with open(path, "a", encoding="utf-8") as handle:
        handle.write(_json_dumps(payload) + "\n")


def save_prediction_snapshot(
    prediction: dict[str, Any],
    prediction_mode: str = "balanced",
    *,
    publication_guard: Callable[[], bool] | None = None,
) -> PredictionSnapshot | None:
    """Persist a live AI prediction to SQLite and NDJSON fallback."""
    def publication_allowed() -> bool:
        if publication_guard is None:
            return True
        try:
            return publication_guard() is True
        except Exception:
            logger.exception("Prediction publication guard failed")
            return False

    if not publication_allowed():
        return None
    try:
        timestamp_utc = _coerce_timestamp(prediction.get("timestamp")) or _naive_utc()

        symbol = str(prediction.get("symbol", "")).upper()
        current_price = _first_finite_float(prediction.get("current_price")) or 0.0
        predicted_close = _first_finite_float(prediction.get("predicted_close"), current_price)
        if predicted_close is None:
            predicted_close = current_price
        expected_move_points = _first_finite_float(prediction.get("expected_move_points"))
        if expected_move_points is None:
            expected_move_points = predicted_close - current_price
        expected_move_pct = _first_finite_float(prediction.get("expected_move_pct"))
        if expected_move_pct is None:
            expected_move_pct = (expected_move_points / current_price * 100.0) if current_price else 0.0
        trading_day = _trading_date(timestamp_utc)
        source_payload = prediction.get("pin_payload") or {}
        if not isinstance(source_payload, dict):
            source_payload = {}
        provider = str(
            prediction.get("provider") or source_payload.get("provider") or ""
        ).lower().strip() or None
        raw_epoch_values = [
            value
            for value in (
                prediction.get("subscription_epoch_id"),
                source_payload.get("subscription_epoch_id"),
            )
            if value is not None
        ]
        raw_epoch_id = (
            str(raw_epoch_values[0]).strip() if raw_epoch_values else None
        )
        epoch_conflict = any(
            str(value).strip() != raw_epoch_id for value in raw_epoch_values[1:]
        )
        subscription_epoch_id = (
            raw_epoch_id
            if raw_epoch_id is not None
            and not epoch_conflict
            and re.fullmatch(r"[0-9a-f]{64}", raw_epoch_id) is not None
            else None
        )
        quote_timestamp = _coerce_timestamp(
            prediction.get("quote_timestamp_utc")
            if prediction.get("quote_timestamp_utc") is not None
            else source_payload.get("latest_ts_recv_utc")
            or source_payload.get("observation_index_utc")
            or source_payload.get("timestamp")
        )
        subscription_generation = _first_int(
            prediction.get("subscription_generation"),
            source_payload.get("subscription_generation"),
        )
        quote_age_seconds = _first_finite_float(
            prediction.get("quote_age_seconds"),
            source_payload.get("quote_age_seconds"),
            prediction.get("data_age_seconds"),
        )
        data_age_seconds = _first_finite_float(prediction.get("data_age_seconds"))
        active_contract_count = _first_int(
            prediction.get("active_contract_count"),
            source_payload.get("contracts_count"),
        )
        fresh_quote_count = _first_int(
            prediction.get("fresh_quote_count"),
            source_payload.get("fresh_quote_count"),
        )
        gamma_pin = _first_finite_float(prediction.get("gamma_pin"), source_payload.get("gamma_pin"))
        max_pain = _first_finite_float(prediction.get("max_pain"), source_payload.get("max_pain"))
        zero_gamma = _first_finite_float(prediction.get("zero_gamma"), source_payload.get("zero_gamma"))
        gross_gex = _first_finite_float(prediction.get("gross_gex"), source_payload.get("gross_gex"))
        net_gex = _first_finite_float(prediction.get("net_gex"), source_payload.get("net_gex"))
        call_gex = _first_finite_float(prediction.get("call_gex"), source_payload.get("call_gex_total"))
        put_gex = _first_finite_float(prediction.get("put_gex"), source_payload.get("put_gex_total"))
        is_valid = prediction.get("usable") is True and current_price > 0.0 and predicted_close > 0.0
        if provider == "databento" and is_valid and subscription_epoch_id is None:
            logger.warning(
                "Rejected valid Databento prediction snapshot without one canonical process epoch"
            )
            return None
        validation_status = str(
            prediction.get("validation_status")
            or ("valid" if is_valid else "invalid")
        )
        existing = None
        if (
            quote_timestamp is not None
            and subscription_epoch_id is not None
            and subscription_generation is not None
        ):
            db = SessionLocal()
            try:
                existing = db.query(PredictionSnapshot).filter(
                    PredictionSnapshot.symbol == symbol,
                    PredictionSnapshot.trading_date == trading_day,
                    PredictionSnapshot.prediction_mode == prediction_mode,
                    PredictionSnapshot.subscription_epoch_id
                    == subscription_epoch_id,
                    PredictionSnapshot.subscription_generation == int(subscription_generation),
                    PredictionSnapshot.quote_timestamp_utc == quote_timestamp,
                ).first()
            finally:
                db.close()
        if existing is not None:
            return existing if publication_allowed() else None

        row_payload = {
            "symbol": symbol,
            "timestamp_utc": timestamp_utc.isoformat(),
            "trading_date": trading_day.isoformat(),
            "provider": provider,
            "model_version": prediction.get("model_version"),
            "model_type": prediction.get("model_type"),
            "prediction_mode": prediction_mode,
            "is_valid": is_valid,
            "current_price": current_price,
            "predicted_close": predicted_close,
            "confidence": prediction.get("confidence"),
            "expected_move_points": expected_move_points,
            "expected_move_pct": expected_move_pct,
            "net_bias": prediction.get("net_bias"),
            "feature_schema_version": prediction.get("feature_schema_version"),
            "feature_hash": prediction.get("feature_hash"),
            "feature_snapshot": prediction.get("feature_snapshot"),
            "signals": prediction.get("signals"),
            "source_payload": prediction.get("pin_payload"),
            "data_age_seconds": data_age_seconds,
            "quote_timestamp_utc": quote_timestamp.isoformat() if isinstance(quote_timestamp, datetime) else None,
            "subscription_epoch_id": subscription_epoch_id,
            "subscription_generation": subscription_generation,
            "quote_age_seconds": quote_age_seconds,
            "active_contract_count": active_contract_count,
            "fresh_quote_count": fresh_quote_count,
            "gamma_pin": gamma_pin,
            "max_pain": max_pain,
            "zero_gamma": zero_gamma,
            "gross_gex": gross_gex,
            "net_gex": net_gex,
            "call_gex": call_gex,
            "put_gex": put_gex,
            "inference_device": prediction.get("inference_device"),
            "validation_status": validation_status,
        }

        db = SessionLocal()
        snapshot = PredictionSnapshot(
            symbol=symbol,
            timestamp_utc=timestamp_utc,
            trading_date=trading_day,
            provider=provider,
            model_version=prediction.get("model_version"),
            model_type=prediction.get("model_type"),
            prediction_mode=prediction_mode,
            is_valid=is_valid,
            current_price=current_price,
            predicted_close=predicted_close,
            confidence=prediction.get("confidence"),
            expected_move_points=expected_move_points,
            expected_move_pct=expected_move_pct,
            net_bias=prediction.get("net_bias"),
            feature_snapshot_json=_json_dumps(prediction.get("feature_snapshot")),
            feature_schema_version=prediction.get("feature_schema_version"),
            feature_hash=prediction.get("feature_hash"),
            signals_json=_json_dumps(prediction.get("signals")),
            source_payload_json=_json_dumps(prediction.get("pin_payload")),
            data_age_seconds=data_age_seconds,
            quote_timestamp_utc=quote_timestamp,
            subscription_epoch_id=subscription_epoch_id,
            subscription_generation=subscription_generation,
            quote_age_seconds=quote_age_seconds,
            active_contract_count=active_contract_count,
            fresh_quote_count=fresh_quote_count,
            gamma_pin=gamma_pin,
            max_pain=max_pain,
            zero_gamma=zero_gamma,
            gross_gex=gross_gex,
            net_gex=net_gex,
            call_gex=call_gex,
            put_gex=put_gex,
            inference_device=prediction.get("inference_device"),
            validation_status=validation_status,
        )
        if not publication_allowed():
            db.rollback()
            return None
        db.add(snapshot)
        db.flush()
        if not publication_allowed():
            db.rollback()
            return None
        db.commit()
        db.refresh(snapshot)
        try:
            _append_prediction_jsonl(row_payload)
        except Exception as export_exc:
            logger.warning(
                "Prediction snapshot %s was saved to SQLite but NDJSON export failed: %s",
                snapshot.id,
                export_exc,
            )
        return snapshot
    except Exception as exc:
        logger.warning("Failed to save prediction snapshot: %s", exc)
        try:
            if "row_payload" in locals():
                _append_prediction_jsonl(row_payload)
        except Exception as export_exc:
            logger.error("Prediction snapshot SQLite and NDJSON persistence both failed: %s", export_exc)
        return None
    finally:
        try:
            db.close()
        except Exception:
            pass


def _verified_close_chain_tip(
    observations: Sequence[EODCloseObservation], *, parent_overrides: Mapping[int, int] | None = None,
) -> EODCloseObservation | None:
    """Return one causal verified-close leaf, rejecting roots, gaps, and forks."""

    verified = [item for item in observations if bool(item.source_verified)]
    if not verified:
        return None
    by_id = {int(item.id): item for item in verified}
    roots: list[EODCloseObservation] = []
    children: dict[int, EODCloseObservation] = {}
    for item in verified:
        parent_id = (parent_overrides or {}).get(int(item.id), item.correction_of_id)
        if parent_id is None:
            roots.append(item)
            continue
        parent = by_id.get(int(parent_id))
        if (
            parent is None
            or int(parent_id) >= int(item.id)
            or parent.symbol != item.symbol
            or parent.trading_date != item.trading_date
        ):
            raise ValueError("verified close correction lineage is missing, mismatched, or non-causal")
        if int(parent_id) in children:
            raise ValueError("verified close correction lineage forks")
        children[int(parent_id)] = item
    if len(roots) != 1:
        raise ValueError("verified close observations require exactly one causal root")
    current = roots[0]
    visited = {int(current.id)}
    while int(current.id) in children:
        current = children[int(current.id)]
        if int(current.id) in visited:
            raise ValueError("verified close correction lineage cycles")
        visited.add(int(current.id))
    if visited != set(by_id):
        raise ValueError("verified close correction lineage is disconnected")
    return current


def _upsert_eod_close_in_session(
    db,
    symbol: str,
    trading_date: date,
    official_close: float,
    source: str = "manual-unverified",
    *,
    source_reference: str | None = None,
    source_artifact_sha256: str | None = None,
    observed_at_utc: datetime | None = None,
    correction_of_id: int | None = None,
    source_verified: bool = False,
) -> EODClose | None:
    """Append one observation through an existing transaction."""
    symbol = symbol.upper()
    close_value = float(official_close)
    if not math.isfinite(close_value) or close_value <= 0:
        raise ValueError("official_close must be finite and positive")
    source_value = str(source or "manual-unverified").strip()[:80]
    reference_value = str(source_reference).strip()[:500] if source_reference else None
    artifact_hash = (
        validate_source_artifact_sha256(source_artifact_sha256)
        if source_artifact_sha256 is not None else None
    )
    observed = observed_at_utc or datetime.utcnow()
    if source_verified:
        if observed_at_utc is None:
            raise ValueError("observed_at_utc is required for verified close evidence")
        observed = validate_verified_close_observed_at(trading_date, observed_at_utc)
    if observed.tzinfo is not None:
        observed = observed.astimezone(ZoneInfo("UTC")).replace(tzinfo=None)
    key_payload = {
        "symbol": symbol, "trading_date": trading_date.isoformat(),
        "official_close": close_value, "source": source_value,
        "source_reference": reference_value, "source_artifact_sha256": artifact_hash,
    }
    observation_key = hashlib.sha256(
        json.dumps(key_payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    row = db.query(EODClose).filter(
        EODClose.symbol == symbol, EODClose.trading_date == trading_date
    ).first()
    observation = db.query(EODCloseObservation).filter(
        EODCloseObservation.observation_key == observation_key
    ).first()
    if observation is None:
        lineage_id = correction_of_id
        if (
            not source_verified
            and lineage_id is None
            and row is not None
            and not math.isclose(
                float(row.official_close), close_value, rel_tol=0.0, abs_tol=1e-12
            )
        ):
            prior = db.query(EODCloseObservation).filter(
                EODCloseObservation.symbol == symbol,
                EODCloseObservation.trading_date == trading_date,
            ).order_by(EODCloseObservation.id.desc()).first()
            lineage_id = prior.id if prior is not None else None
        verified_observations = db.query(EODCloseObservation).filter(
            EODCloseObservation.symbol == symbol,
            EODCloseObservation.trading_date == trading_date,
            EODCloseObservation.source_verified == True,
        ).order_by(EODCloseObservation.id).all()
        from backend.closing_tape.close_reconciliation import load_verified_close_parent_overrides
        verified_tip = _verified_close_chain_tip(
            verified_observations,
            parent_overrides=load_verified_close_parent_overrides(db.connection()),
        )
        if source_verified and verified_tip is not None:
            if lineage_id is None:
                raise ValueError(
                    "a subsequent verified close requires explicit correction_of_id lineage"
                )
            if int(lineage_id) != int(verified_tip.id):
                raise ValueError(
                    "correction_of_id must reference the current authoritative verified close"
                )
        elif source_verified and lineage_id is not None:
            raise ValueError(
                "correction_of_id is invalid because no prior verified close exists"
            )
        if lineage_id is not None:
            prior = db.query(EODCloseObservation).filter(
                EODCloseObservation.id == lineage_id
            ).first()
            if prior is None or prior.symbol != symbol or prior.trading_date != trading_date:
                raise ValueError("correction_of_id must reference the same symbol and trading date")
        db.add(EODCloseObservation(
            observation_key=observation_key, symbol=symbol, trading_date=trading_date,
            official_close=close_value, source=source_value,
            source_reference=reference_value, source_artifact_sha256=artifact_hash,
            source_verified=bool(source_verified), observed_at_utc=observed,
            correction_of_id=lineage_id,
        ))
    elif (
        observation.correction_of_id != correction_of_id
        or bool(observation.source_verified) != bool(source_verified)
    ):
        raise ValueError(
            "idempotent close evidence conflicts with its immutable lineage or verification state"
        )
    else:
        if row is None:
            raise RuntimeError(
                "immutable close observation exists without its current projection"
            )
        return row
    if row is None:
        row = EODClose(
            symbol=symbol, trading_date=trading_date,
            official_close=close_value, source=source_value,
        )
        db.add(row)
    else:
        row.official_close = close_value
        row.source = source_value
        row.ingested_at_utc = datetime.utcnow()
    return row


def upsert_eod_close(
    symbol: str,
    trading_date: date,
    official_close: float,
    source: str = "manual-unverified",
    *,
    source_reference: str | None = None,
    source_artifact_sha256: str | None = None,
    observed_at_utc: datetime | None = None,
    correction_of_id: int | None = None,
    source_verified: bool = False,
) -> EODClose | None:
    """Append immutable evidence and update the current close projection."""
    db = SessionLocal()
    try:
        row = _upsert_eod_close_in_session(
            db, symbol, trading_date, official_close, source,
            source_reference=source_reference,
            source_artifact_sha256=source_artifact_sha256,
            observed_at_utc=observed_at_utc,
            correction_of_id=correction_of_id,
            source_verified=source_verified,
        )
        db.commit()
        db.refresh(row)
        return row
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


def _normalize_verified_close_payload(payload: Mapping[str, Any]) -> dict[str, Any]:
    normalized = str(payload["symbol"]).strip().upper()
    normalized_source = str(payload["source"]).strip().lower()
    trading_day = payload["trading_date"]
    if not isinstance(trading_day, date):
        raise ValueError("trading_date must be a date")
    source_reference = validate_official_close_reference(
        normalized, normalized_source, str(payload.get("source_reference") or "")
    )
    source_artifact_sha256 = validate_source_artifact_sha256(
        str(payload.get("source_artifact_sha256") or "")
    )
    observed_at_utc = payload.get("observed_at_utc")
    if observed_at_utc is None:
        raise ValueError("observed_at_utc is required for verified close evidence")
    if not isinstance(observed_at_utc, datetime):
        raise ValueError("observed_at_utc must be a datetime")
    if observed_at_utc.tzinfo is None:
        raise ValueError("observed_at_utc must include a timezone")
    observed_at_utc = validate_verified_close_observed_at(
        trading_day,
        observed_at_utc,
    )
    return {
        "symbol": normalized,
        "trading_date": trading_day,
        "official_close": payload["official_close"],
        "source": normalized_source,
        "source_reference": source_reference,
        "source_artifact_sha256": source_artifact_sha256,
        "observed_at_utc": observed_at_utc,
        "correction_of_id": payload.get("correction_of_id"),
    }


def upsert_verified_eod_close_bundle(
    payloads: Sequence[Mapping[str, Any]],
) -> list[EODClose]:
    """Atomically append a prevalidated verified-close bundle."""
    normalized = [_normalize_verified_close_payload(payload) for payload in payloads]
    if not normalized:
        raise ValueError("verified close bundle contains no rows")
    keys = [(item["trading_date"], item["symbol"]) for item in normalized]
    if len(keys) != len(set(keys)):
        raise ValueError("verified close bundle contains duplicate symbol/trading-date rows")
    db = SessionLocal()
    try:
        rows = [
            _upsert_eod_close_in_session(db, source_verified=True, **item)
            for item in normalized
        ]
        db.commit()
        for row in rows:
            db.refresh(row)
        return rows
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


def upsert_verified_eod_close(
    symbol: str,
    trading_date: date,
    official_close: float,
    source: str,
    *,
    source_reference: str,
    source_artifact_sha256: str,
    observed_at_utc: datetime,
    correction_of_id: int | None = None,
) -> EODClose | None:
    """Internal single-row verified path; intentionally not exposed publicly."""
    return upsert_verified_eod_close_bundle([{
        "symbol": symbol, "trading_date": trading_date,
        "official_close": official_close, "source": source,
        "source_reference": source_reference,
        "source_artifact_sha256": source_artifact_sha256,
        "observed_at_utc": observed_at_utc,
        "correction_of_id": correction_of_id,
    }])[0]


def score_prediction_snapshots(
    symbol: str,
    trading_date: date,
    *,
    prediction_ids: Sequence[int] | None = None,
    selection_evidence_sha256: str | None = None,
) -> dict[str, Any]:
    """Score explicitly selected legacy snapshots for diagnostic research only.

    Promotion-grade evaluation is intentionally handled by the closing-tape
    governance ledger. Rows written here can never become training evidence or
    headline model-accuracy claims.
    """
    selected_ids = sorted({int(value) for value in (prediction_ids or ())})
    if not selected_ids:
        return {
            "symbol": symbol.upper(),
            "trading_date": trading_date.isoformat(),
            "scored": 0,
            "reason": "explicit_prediction_selection_required",
        }
    if any(value <= 0 for value in selected_ids):
        raise ValueError("prediction_ids must contain only positive integers")
    normalized_selection_hash = None
    if selection_evidence_sha256 is not None:
        normalized_selection_hash = str(selection_evidence_sha256).strip().lower()
        if (
            len(normalized_selection_hash) != 64
            or any(character not in "0123456789abcdef" for character in normalized_selection_hash)
        ):
            raise ValueError(
                "selection_evidence_sha256 must be a 64-character hexadecimal SHA-256"
            )
    db = SessionLocal()
    try:
        symbol = symbol.upper()
        close = db.query(EODCloseObservation).filter(
            EODCloseObservation.symbol == symbol,
            EODCloseObservation.trading_date == trading_date,
            EODCloseObservation.source_verified == True,
        ).order_by(EODCloseObservation.id.desc()).first()
        if close is None:
            return {"symbol": symbol, "trading_date": trading_date.isoformat(), "scored": 0, "reason": "missing_verified_eod_close"}
        try:
            artifact_hash = validate_source_artifact_sha256(close.source_artifact_sha256)
            validate_official_close_reference(symbol, close.source, close.source_reference)
        except ValueError as exc:
            return {
                "symbol": symbol, "trading_date": trading_date.isoformat(),
                "scored": 0, "reason": "invalid_verified_eod_close_evidence", "detail": str(exc),
            }

        selected_rows = db.query(PredictionSnapshot).filter(
            PredictionSnapshot.id.in_(selected_ids),
        ).all()
        rows_by_id = {row.id: row for row in selected_rows}
        invalid_selection: list[int] = []
        ineligible_reasons: dict[int, str] = {}
        for prediction_id in selected_ids:
            row = rows_by_id.get(prediction_id)
            if row is None:
                invalid_selection.append(prediction_id)
                ineligible_reasons[prediction_id] = "prediction_not_found"
                continue
            if row.symbol != symbol or row.trading_date != trading_date:
                invalid_selection.append(prediction_id)
                ineligible_reasons[prediction_id] = "symbol_or_trading_date_mismatch"
                continue
            if row.is_valid is not True:
                invalid_selection.append(prediction_id)
                ineligible_reasons[prediction_id] = "prediction_not_valid"
                continue
            try:
                source_payload = json.loads(row.source_payload_json or "{}")
            except json.JSONDecodeError:
                source_payload = {}
            if not isinstance(source_payload, dict):
                source_payload = {}
            calculation_id = str(source_payload.get("calculation_id") or "").strip()
            if not calculation_id:
                invalid_selection.append(prediction_id)
                ineligible_reasons[prediction_id] = "missing_calculation_id"
                continue
            calculation = db.query(GammaCalculationRun).join(
                GammaCalculationInputBlob,
                GammaCalculationInputBlob.calculation_run_id == GammaCalculationRun.id,
            ).filter(
                GammaCalculationRun.calculation_id == calculation_id,
                GammaCalculationRun.symbol == symbol,
                GammaCalculationRun.trading_date == trading_date,
                GammaCalculationRun.status == "valid",
            ).first()
            if calculation is None:
                invalid_selection.append(prediction_id)
                ineligible_reasons[prediction_id] = "missing_valid_calculation_and_input_blob"
        if invalid_selection:
            return {
                "symbol": symbol,
                "trading_date": trading_date.isoformat(),
                "scored": 0,
                "reason": "prediction_selection_not_eligible",
                "prediction_ids": selected_ids,
                "ineligible_prediction_ids": invalid_selection,
                "ineligible_reasons": ineligible_reasons,
            }
        rows = [rows_by_id[prediction_id] for prediction_id in selected_ids]
        score_keys = {
            row.id: hashlib.sha256(f"{row.id}:{close.id}".encode("utf-8")).hexdigest()
            for row in rows
        }
        existing_accuracy_rows = db.query(PredictionAccuracyObservation).filter(
            PredictionAccuracyObservation.score_key.in_(list(score_keys.values()))
        ).all()
        existing_by_key = {row.score_key: row for row in existing_accuracy_rows}
        scope_conflicts = [
            row.prediction_id
            for row in existing_accuracy_rows
            if row.evidence_scope != DIAGNOSTIC_ACCURACY_SCOPE
            or row.training_eligible is True
        ]
        if scope_conflicts:
            return {
                "symbol": symbol,
                "trading_date": trading_date.isoformat(),
                "scored": 0,
                "reason": "existing_score_scope_invalid",
                "ineligible_prediction_ids": sorted(scope_conflicts),
            }
        selection_conflicts = [
            row.prediction_id
            for row in existing_accuracy_rows
            if normalized_selection_hash is not None
            and row.selection_evidence_sha256 != normalized_selection_hash
        ]
        if selection_conflicts:
            return {
                "symbol": symbol,
                "trading_date": trading_date.isoformat(),
                "scored": 0,
                "reason": "selection_evidence_conflict",
                "ineligible_prediction_ids": sorted(selection_conflicts),
                "detail": (
                    "the immutable score is already bound to different or missing "
                    "selection evidence"
                ),
            }
        scored_at = datetime.utcnow()
        accuracy_rows: list[PredictionAccuracyObservation] = []
        for row in rows:
            row.actual_close = close.official_close
            row.error_points = row.predicted_close - close.official_close
            row.error_pct = (row.error_points / close.official_close * 100.0) if close.official_close else None
            predicted_direction = 1 if row.predicted_close > row.current_price else -1 if row.predicted_close < row.current_price else 0
            actual_direction = 1 if close.official_close > row.current_price else -1 if close.official_close < row.current_price else 0
            row.direction_hit = predicted_direction == actual_direction
            row.scored_at_utc = scored_at
            score_key = score_keys[row.id]
            existing = existing_by_key.get(score_key)
            if existing is None:
                existing = PredictionAccuracyObservation(
                    score_key=score_key, prediction_id=row.id, close_observation_id=close.id,
                    symbol=row.symbol, trading_date=trading_date,
                    prediction_timestamp_utc=row.timestamp_utc,
                    predicted_close=row.predicted_close, actual_close=close.official_close,
                    error_points=row.error_points, error_pct=row.error_pct,
                    direction_hit=row.direction_hit, model_version=row.model_version,
                    provider=row.provider, quote_age_seconds=row.quote_age_seconds,
                    fresh_quote_count=row.fresh_quote_count,
                    active_contract_count=row.active_contract_count,
                    close_source=close.source,
                    close_source_reference=close.source_reference,
                    close_source_artifact_sha256=artifact_hash,
                    evidence_scope=DIAGNOSTIC_ACCURACY_SCOPE,
                    training_eligible=False,
                    selection_evidence_sha256=normalized_selection_hash,
                    scored_at_utc=scored_at,
                )
                db.add(existing)
            accuracy_rows.append(existing)
        db.commit()
        persisted_selection_hashes = sorted({
            row.selection_evidence_sha256
            for row in accuracy_rows
            if row.selection_evidence_sha256 is not None
        })
        selection_evidence_bound = bool(accuracy_rows) and all(
            row.selection_evidence_sha256 is not None for row in accuracy_rows
        ) and len(persisted_selection_hashes) == 1
        persisted_selection_hash = (
            persisted_selection_hashes[0] if selection_evidence_bound else None
        )
        try:
            _rebuild_accuracy_ledger(trading_date)
            export_status = "current"
        except Exception as exc:
            logger.warning("Accuracy SQLite commit succeeded but NDJSON projection failed: %s", exc)
            export_status = "stale"
        return {
            "symbol": symbol, "trading_date": trading_date.isoformat(),
            "actual_close": close.official_close, "scored": len(rows),
            "close_observation_id": close.id, "accuracy_export": export_status,
            "evidence_scope": DIAGNOSTIC_ACCURACY_SCOPE,
            "training_eligible": False,
            "selection_evidence_sha256": persisted_selection_hash,
            "selection_evidence_bound": selection_evidence_bound,
        }
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


def _accuracy_payload(row: PredictionAccuracyObservation) -> dict[str, Any]:
    return {
        "score_key": row.score_key, "prediction_id": row.prediction_id,
        "close_observation_id": row.close_observation_id, "symbol": row.symbol,
        "trading_date": row.trading_date.isoformat(),
        "prediction_timestamp_utc": row.prediction_timestamp_utc.isoformat(),
        "predicted_close": row.predicted_close, "actual_close": row.actual_close,
        "absolute_error_points": abs(row.error_points),
        "absolute_error_percent": abs(row.error_pct),
        "direction_correct": row.direction_hit, "model_version": row.model_version,
        "source": row.provider, "quote_age_seconds": row.quote_age_seconds,
        "fresh_quote_count": row.fresh_quote_count,
        "active_contract_count": row.active_contract_count,
        "close_source": row.close_source,
        "close_source_reference": row.close_source_reference,
        "close_source_artifact_sha256": row.close_source_artifact_sha256,
        "evidence_scope": row.evidence_scope,
        "training_eligible": bool(row.training_eligible),
        "selection_evidence_sha256": row.selection_evidence_sha256,
        "performance_claim_eligible": False,
        "scored_at_utc": row.scored_at_utc.isoformat(),
    }


def _rebuild_accuracy_ledger(trading_date: date) -> None:
    """Atomically project authoritative SQLite score evidence to daily NDJSON."""
    db = SessionLocal()
    try:
        rows = db.query(PredictionAccuracyObservation).filter(
            PredictionAccuracyObservation.trading_date == trading_date
        ).order_by(
            PredictionAccuracyObservation.symbol,
            PredictionAccuracyObservation.prediction_id,
            PredictionAccuracyObservation.close_observation_id,
        ).all()
        payload = "".join(_json_dumps(_accuracy_payload(row)) + "\n" for row in rows)
    finally:
        db.close()
    ledger_dir = PREDICTION_EXPORT_DIR / "accuracy_ledger"
    ledger_dir.mkdir(parents=True, exist_ok=True)
    path = ledger_dir / f"{trading_date.isoformat()}.ndjson"
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    try:
        with open(temporary, "w", encoding="utf-8", newline="\n") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _prediction_score_metrics(rows: Sequence[PredictionAccuracyObservation]) -> dict[str, Any]:
    if not rows:
        return {"count": 0, "mae": None, "rmse": None, "direction_hit_rate": None}
    errors = [abs(row.error_points or 0.0) for row in rows]
    squared = [(row.error_points or 0.0) ** 2 for row in rows]
    hits = [row.direction_hit for row in rows if row.direction_hit is not None]
    return {
        "count": len(rows),
        "mae": sum(errors) / len(errors),
        "rmse": (sum(squared) / len(squared)) ** 0.5,
        "direction_hit_rate": (
            sum(1 for hit in hits if hit) / len(hits)
        ) if hits else None,
    }


def get_prediction_score_summary(symbol: str | None = None, trading_date: date | None = None) -> dict[str, Any]:
    """Return claim-safe headline metrics plus separately labeled diagnostics."""
    db = SessionLocal()
    try:
        query = db.query(PredictionAccuracyObservation)
        if symbol:
            query = query.filter(PredictionAccuracyObservation.symbol == symbol.upper())
        if trading_date:
            query = query.filter(PredictionAccuracyObservation.trading_date == trading_date)
        rows = query.all()
        latest = {}
        for row in rows:
            prior = latest.get(row.prediction_id)
            if prior is None or row.close_observation_id > prior.close_observation_id:
                latest[row.prediction_id] = row
        rows = list(latest.values())
        diagnostic_rows = [
            row
            for row in rows
            if row.evidence_scope == DIAGNOSTIC_ACCURACY_SCOPE
            and row.training_eligible is not True
        ]
        unexpected_rows = [row for row in rows if row not in diagnostic_rows]
        return {
            "metric_scope": "performance_claim_eligible_only",
            "status": "no_claim_eligible_scores",
            **_prediction_score_metrics([]),
            "excluded_non_claim_eligible_count": len(rows),
            "diagnostic_research_only": _prediction_score_metrics(diagnostic_rows),
            "unclassified_or_invalid_scope_count": len(unexpected_rows),
            "authoritative_scorecard": "/closing-tape/scorecard",
            "note": (
                "Legacy snapshot scores are diagnostic research only and are excluded "
                "from model-accuracy and training claims."
            ),
        }
    finally:
        db.close()


if __name__ == "__main__":
    init_db()
    print("✅ Database initialized")
