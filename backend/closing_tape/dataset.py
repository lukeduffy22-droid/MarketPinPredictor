from __future__ import annotations

import json
import re
import sqlite3
from numbers import Integral
from pathlib import Path
from typing import Iterable

import pandas as pd

from .close_evidence import (
    resolve_verified_close_artifact,
    validate_official_close_reference,
)
from .contracts import (
    EVIDENCE_CONTRACT_VERSION,
    HISTORICAL_EVIDENCE_CONTRACT_VERSION,
    HISTORICAL_SOURCE_KIND,
    LIVE_SOURCE_KIND,
)
from .sqlite_io import sqlite_read_only_uri


OBSERVED_COLUMNS = (
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
)
INFERRED_COLUMNS = (
    "at_ask_count", "at_bid_count", "inside_count", "unknown_count",
    "at_ask_volume", "at_bid_volume", "inside_volume", "unknown_volume",
    "at_ask_notional", "at_bid_notional", "inside_notional", "unknown_notional",
    "call_at_ask_notional", "call_at_bid_notional", "put_at_ask_notional",
    "put_at_bid_notional",
)
PRODUCTION_FAMILIES = ("SPX", "NDX", "RUT", "VIX", "SPY")
CONTRACT_OBSERVED_COLUMNS = (
    "trade_count", "volume", "notional", "first_price", "high_price", "low_price",
    "last_price", "price_volume_sum", "nbbo_valid_count", "quoted_spread_sum",
    "quoted_spread_bps_sum", "trade_to_mid_abs_sum", "trade_to_mid_signed_sum",
    "bid_size_sum", "ask_size_sum", "receive_lag_ns_sum", "receive_lag_ns_max",
    "data_quality_flagged_count",
    "last_pretrade_midpoint", "last_nbbo_event_ns",
)
CONTRACT_INFERRED_COLUMNS = tuple(
    f"{bucket}_{metric}"
    for bucket in ("at_ask", "at_bid", "inside", "unknown")
    for metric in ("count", "volume", "notional")
)
SUBSCRIPTION_EPOCH_PATTERN = re.compile(r"[0-9a-f]{64}")


def _read_only(path: Path) -> sqlite3.Connection:
    if not path.is_file():
        raise FileNotFoundError(path)
    connection = sqlite3.connect(
        sqlite_read_only_uri(path),
        uri=True,
        timeout=10.0,
    )
    connection.row_factory = sqlite3.Row
    return connection


def _empty_marketpin_reference_prices() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "family_root", "trading_date", "timestamp_utc", "quote_timestamp_utc",
            "current_price", "provider", "reference_subscription_epoch_id",
            "reference_subscription_generation", "reference_price_epoch_eligible",
            "reference_price_epoch_status", "data_age_seconds", "quote_age_seconds",
            "predicted_close", "model_version", "prediction_mode",
            "reference_price_method", "reference_price_is_estimate",
            "reference_price_provider", "reference_underlying_validation_status",
        ]
    )


def _canonical_subscription_epoch(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    return value if SUBSCRIPTION_EPOCH_PATTERN.fullmatch(value) else None


def _positive_subscription_generation(value: object) -> int | None:
    if isinstance(value, bool) or not isinstance(value, Integral) or value <= 0:
        return None
    return int(value)


def _source_contract_sql(
    feed_columns: set[str],
) -> tuple[str, str, tuple[str, ...], tuple[str, ...]]:
    """Build a backward-compatible, source-kind-bound audit predicate."""

    if {"source_kind", "evidence_contract_version"} <= feed_columns:
        source_clause = """
          AND (
                (feeds.source_kind=? AND feeds.evidence_contract_version=?)
             OR (feeds.source_kind=? AND feeds.evidence_contract_version=?)
          )
        """
        source_params = (
            LIVE_SOURCE_KIND,
            EVIDENCE_CONTRACT_VERSION,
            HISTORICAL_SOURCE_KIND,
            HISTORICAL_EVIDENCE_CONTRACT_VERSION,
        )
        return "feeds.evidence_contract_version", source_clause, source_params, ()
    return "?", "", (), (EVIDENCE_CONTRACT_VERSION,)


def _attach_point_in_time_open_interest(
    connection: sqlite3.Connection,
    contracts: pd.DataFrame,
) -> pd.DataFrame:
    """Attach only OI records received by each contract minute's end.

    The immutable observation ledger is intentionally joined in memory. Older
    catalogs may not yet carry the supporting SQLite index, and a correlated
    range lookup for every contract minute is prohibitively expensive.
    """
    output_columns = {
        "open_interest": pd.NA,
        "open_interest_asof_utc": pd.NaT,
        "open_interest_available_at_utc": pd.NaT,
        "open_interest_event_ns": pd.NA,
        "open_interest_receive_ns": pd.NA,
        "open_interest_reference_ns": pd.NA,
        "open_interest_update_action": pd.NA,
    }
    if contracts.empty:
        return contracts.assign(**output_columns)
    tables = {
        str(row[0])
        for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        )
    }
    if "tape_open_interest_observations" not in tables:
        return contracts.assign(**output_columns)
    session_ids = tuple(sorted(contracts["session_id"].dropna().astype(str).unique()))
    if not session_ids:
        return contracts.assign(**output_columns)
    placeholders = ",".join("?" for _ in session_ids)
    rows = connection.execute(
        f"""
        SELECT observations.session_id, observations.feed_name,
               observations.raw_symbol, observations.source_sha256,
               observations.ts_event_ns AS open_interest_event_ns,
               observations.ts_recv_ns AS open_interest_receive_ns,
               observations.ts_ref_ns AS open_interest_reference_ns,
               observations.asof_utc AS open_interest_asof_utc,
               observations.sequence, observations.record_offset,
               observations.update_action AS open_interest_update_action,
               observations.open_interest AS open_interest_value
        FROM tape_open_interest_observations observations
        WHERE observations.session_id IN ({placeholders})
          AND observations.raw_symbol IS NOT NULL
          AND observations.ts_recv_ns IS NOT NULL
        """,
        session_ids,
    ).fetchall()
    if not rows:
        return contracts.assign(**output_columns)

    observations = pd.DataFrame([dict(row) for row in rows])
    group_keys = ["session_id", "feed_name", "raw_symbol", "source_sha256"]
    observations = observations.merge(
        contracts[group_keys].drop_duplicates(),
        on=group_keys,
        how="inner",
        validate="many_to_one",
    )
    if observations.empty:
        return contracts.assign(**output_columns)
    observations["open_interest_receive_ns"] = pd.to_numeric(
        observations["open_interest_receive_ns"], errors="coerce"
    )
    observations = observations.dropna(subset=["open_interest_receive_ns"])
    if observations.empty:
        return contracts.assign(**output_columns)
    observations["open_interest_receive_ns"] = observations[
        "open_interest_receive_ns"
    ].astype("int64")
    observations["open_interest_available_at_utc"] = pd.to_datetime(
        observations["open_interest_receive_ns"], unit="ns", utc=True
    )
    for column in (
        "open_interest_event_ns",
        "open_interest_reference_ns",
        "open_interest_update_action",
    ):
        observations[column] = pd.to_numeric(
            observations[column], errors="coerce"
        ).astype("Int64")
    observations["sequence"] = pd.to_numeric(
        observations["sequence"], errors="coerce"
    ).fillna(-1).astype("int64")
    observations["record_offset"] = pd.to_numeric(
        observations["record_offset"], errors="coerce"
    ).fillna(-1).astype("int64")

    left = contracts.copy()
    left["_feature_available_ns"] = (
        pd.to_datetime(left["minute_utc"], utc=True).astype("int64")
        + 60_000_000_000
    )
    left["_original_order"] = range(len(left))
    left = left.sort_values(["_feature_available_ns", *group_keys])
    observations = observations.sort_values(
        ["open_interest_receive_ns", *group_keys, "sequence", "record_offset"]
    )
    joined = pd.merge_asof(
        left,
        observations,
        left_on="_feature_available_ns",
        right_on="open_interest_receive_ns",
        by=group_keys,
        direction="backward",
        allow_exact_matches=True,
    )
    valid_new = (
        pd.to_numeric(joined["open_interest_update_action"], errors="coerce").eq(1)
        & pd.to_numeric(joined["open_interest_value"], errors="coerce").ge(0)
    )
    joined["open_interest"] = pd.to_numeric(
        joined["open_interest_value"], errors="coerce"
    ).where(valid_new)
    joined["open_interest_available_at_utc"] = pd.to_datetime(
        joined["open_interest_available_at_utc"], utc=True, errors="coerce"
    )
    joined["open_interest_receive_ns"] = pd.Series(
        pd.array(
            [
                value.value if pd.notna(value) else pd.NA
                for value in joined["open_interest_available_at_utc"]
            ],
            dtype="Int64",
        ),
        index=joined.index,
    )
    return (
        joined.sort_values("_original_order")
        .drop(
            columns=[
                "_feature_available_ns", "_original_order", "sequence",
                "record_offset", "open_interest_value",
            ]
        )
        .reset_index(drop=True)
    )


def load_complete_tape_features(
    catalog_paths: Iterable[str | Path],
    *,
    inference_method: str = "trade_price_vs_pretrade_nbbo",
    inference_version: str = "1.0",
) -> pd.DataFrame:
    """Load only raw-complete, hash-aligned observed/inferred feature rows."""
    observed_sql = ",".join(f"observed.{column} AS {column}" for column in OBSERVED_COLUMNS)
    inferred_sql = ",".join(f"inferred.{column} AS {column}" for column in INFERRED_COLUMNS)
    query_template = f"""
        SELECT sessions.trading_date, sessions.cash_open_utc, sessions.cash_close_utc,
               observed.session_id, observed.feed_name,
               observed.family_root, observed.minute_utc,
               {observed_sql}, {inferred_sql},
               inferred.inference_method, inferred.inference_version,
               inferred.source_sha256
        FROM tape_observed_minute observed
        JOIN tape_inferred_minute_flow inferred
          ON inferred.session_id=observed.session_id
         AND inferred.feed_name=observed.feed_name
         AND inferred.family_root=observed.family_root
         AND inferred.minute_utc=observed.minute_utc
        JOIN tape_sessions sessions ON sessions.session_id=observed.session_id
        JOIN tape_feed_status feeds
          ON feeds.session_id=observed.session_id
         AND feeds.feed_name=observed.feed_name
        WHERE feeds.complete=1
          AND feeds.status='complete'
          AND feeds.reconnect_count=0
          AND feeds.slow_reader_warnings=0
          AND feeds.tcbbo_records>0
          AND feeds.tcbbo_timestamped_records=feeds.tcbbo_records
          AND (1.0 * feeds.tcbbo_valid_nbbo_records / feeds.tcbbo_records)>=0.95
          AND feeds.sha256 IS NOT NULL
          AND inferred.source_sha256=feeds.sha256
          AND inferred.inference_method=?
          AND inferred.inference_version=?
          __SOURCE_CONTRACT_CLAUSE__
          AND EXISTS (
              SELECT 1 FROM tape_finalization_runs finalization
              WHERE finalization.session_id=observed.session_id
                AND finalization.feed_name=observed.feed_name
                AND finalization.source_sha256=feeds.sha256
                AND finalization.evidence_contract_version=__EVIDENCE_CONTRACT__
                AND finalization.complete=1
          )
          AND EXISTS (
              SELECT 1 FROM tape_open_interest oi
              WHERE oi.session_id=observed.session_id
                AND oi.feed_name=observed.feed_name
                AND oi.family_root=observed.family_root
                AND oi.open_interest>=0
          )
    """
    frames: list[pd.DataFrame] = []
    for raw_path in catalog_paths:
        path = Path(raw_path)
        with _read_only(path) as connection:
            feed_columns = {
                str(row[1])
                for row in connection.execute("PRAGMA table_info(tape_feed_status)")
            }
            required_evidence_columns = {
                "tcbbo_records",
                "tcbbo_timestamped_records",
                "tcbbo_valid_nbbo_records",
            }
            if not required_evidence_columns <= feed_columns:
                # Pre-evidence catalogs cannot be silently grandfathered into
                # model research merely because they once carried complete=1.
                continue
            tables = {
                str(row[0])
                for row in connection.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                )
            }
            if "tape_finalization_runs" not in tables:
                continue
            contract_sql, source_clause, source_params, contract_params = (
                _source_contract_sql(feed_columns)
            )
            query = query_template.replace(
                "__SOURCE_CONTRACT_CLAUSE__", source_clause
            ).replace("__EVIDENCE_CONTRACT__", contract_sql)
            rows = connection.execute(
                query,
                (
                    inference_method,
                    inference_version,
                    *source_params,
                    *contract_params,
                ),
            ).fetchall()
        if rows:
            frames.append(pd.DataFrame([dict(row) for row in rows]))
    if not frames:
        return pd.DataFrame(
            columns=[
                "trading_date", "session_id", "feed_name", "family_root", "minute_utc",
                *OBSERVED_COLUMNS, *INFERRED_COLUMNS,
                "inference_method", "inference_version", "source_sha256",
                "capture_integrity_verified",
            ]
        )
    result = pd.concat(frames, ignore_index=True)
    result["capture_integrity_verified"] = True
    result["minute_utc"] = pd.to_datetime(result["minute_utc"], utc=True)
    if result.duplicated(["session_id", "feed_name", "family_root", "minute_utc"]).any():
        raise ValueError("duplicate complete tape feature rows across catalogs")
    return result.sort_values(["minute_utc", "session_id", "family_root"]).reset_index(drop=True)


def load_complete_contract_tape_features(
    catalog_paths: Iterable[str | Path],
    *,
    inference_method: str = "trade_price_vs_pretrade_nbbo",
    inference_version: str = "1.0",
) -> pd.DataFrame:
    """Load hash-aligned strike/expiry rows from passing current tape audits only."""
    observed_sql = ",".join(
        f"observed.{column} AS {column}" for column in CONTRACT_OBSERVED_COLUMNS
    )
    inferred_sql = ",".join(
        f"inferred.{column} AS {column}" for column in CONTRACT_INFERRED_COLUMNS
    )
    query_template = f"""
        SELECT sessions.trading_date, sessions.cash_open_utc, sessions.cash_close_utc,
               observed.session_id, observed.feed_name,
               observed.family_root, observed.raw_symbol, observed.expiration,
               observed.option_type, observed.strike, observed.minute_utc,
               {observed_sql}, {inferred_sql},
               inferred.inference_method, inferred.inference_version,
               inferred.source_sha256
        FROM tape_observed_contract_minute observed
        JOIN tape_inferred_contract_minute_flow inferred
          ON inferred.session_id=observed.session_id
         AND inferred.feed_name=observed.feed_name
         AND inferred.raw_symbol=observed.raw_symbol
         AND inferred.minute_utc=observed.minute_utc
        JOIN tape_sessions sessions ON sessions.session_id=observed.session_id
        JOIN tape_feed_status feeds
          ON feeds.session_id=observed.session_id
         AND feeds.feed_name=observed.feed_name
        WHERE feeds.complete=1 AND feeds.status='complete'
          AND feeds.reconnect_count=0 AND feeds.slow_reader_warnings=0
          AND feeds.tcbbo_records>0
          AND feeds.tcbbo_timestamped_records=feeds.tcbbo_records
          AND (1.0 * feeds.tcbbo_valid_nbbo_records / feeds.tcbbo_records)>=0.95
          AND feeds.sha256 IS NOT NULL AND inferred.source_sha256=feeds.sha256
          AND inferred.inference_method=? AND inferred.inference_version=?
          __SOURCE_CONTRACT_CLAUSE__
          AND EXISTS (
              SELECT 1 FROM tape_finalization_runs finalization
              WHERE finalization.session_id=observed.session_id
                AND finalization.feed_name=observed.feed_name
                AND finalization.source_sha256=feeds.sha256
                AND finalization.evidence_contract_version=__EVIDENCE_CONTRACT__
                AND finalization.complete=1
          )
    """
    identity = [
        "trading_date", "cash_open_utc", "cash_close_utc",
        "session_id", "feed_name", "family_root", "raw_symbol",
        "expiration", "option_type", "strike", "minute_utc",
    ]
    frames: list[pd.DataFrame] = []
    for raw_path in catalog_paths:
        path = Path(raw_path)
        with _read_only(path) as connection:
            tables = {
                str(row[0]) for row in connection.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                )
            }
            if not {
                "tape_observed_contract_minute", "tape_inferred_contract_minute_flow",
                "tape_finalization_runs",
            } <= tables:
                continue
            feed_columns = {
                str(row[1])
                for row in connection.execute("PRAGMA table_info(tape_feed_status)")
            }
            contract_sql, source_clause, source_params, contract_params = (
                _source_contract_sql(feed_columns)
            )
            query = query_template.replace(
                "__SOURCE_CONTRACT_CLAUSE__", source_clause
            ).replace("__EVIDENCE_CONTRACT__", contract_sql)
            rows = connection.execute(
                query,
                (
                    inference_method,
                    inference_version,
                    *source_params,
                    *contract_params,
                ),
            ).fetchall()
            if rows:
                contracts = pd.DataFrame([dict(row) for row in rows])
                frames.append(
                    _attach_point_in_time_open_interest(connection, contracts)
                )
    columns = [
        *identity, *CONTRACT_OBSERVED_COLUMNS, *CONTRACT_INFERRED_COLUMNS,
        "inference_method", "inference_version", "source_sha256",
        "open_interest", "open_interest_asof_utc",
        "open_interest_available_at_utc", "open_interest_event_ns",
        "open_interest_receive_ns", "open_interest_reference_ns",
        "open_interest_update_action", "capture_integrity_verified",
    ]
    if not frames:
        return pd.DataFrame(columns=columns)
    result = pd.concat(frames, ignore_index=True)
    result["capture_integrity_verified"] = True
    result["minute_utc"] = pd.to_datetime(result["minute_utc"], utc=True)
    result["expiration"] = pd.to_datetime(result["expiration"]).dt.date
    result["open_interest_asof_utc"] = pd.to_datetime(
        result["open_interest_asof_utc"], utc=True, errors="coerce"
    )
    result["open_interest_available_at_utc"] = pd.to_datetime(
        result["open_interest_available_at_utc"], utc=True, errors="coerce"
    )
    feature_available_at = result["minute_utc"] + pd.Timedelta(minutes=1)
    oi_present = result["open_interest"].notna()
    invalid_oi_availability = oi_present & (
        result["open_interest_available_at_utc"].isna()
        | (result["open_interest_available_at_utc"] > feature_available_at)
    )
    if invalid_oi_availability.any():
        raise ValueError(
            "contract tape contains open interest unavailable at feature time"
        )
    identity_key = ["session_id", "feed_name", "raw_symbol", "minute_utc"]
    if result.duplicated(identity_key).any():
        raise ValueError("duplicate complete contract tape feature rows across catalogs")
    return result.sort_values(
        ["minute_utc", "session_id", "family_root", "expiration", "strike", "option_type"]
    ).reset_index(drop=True)


def load_marketpin_reference_prices(
    market_db_path: str | Path,
    *,
    max_data_age_seconds: float = 30.0,
) -> pd.DataFrame:
    """Load point-in-time valid MarketPin prices without stale/invalid fallbacks."""
    path = Path(market_db_path)
    with _read_only(path) as connection:
        tables = {
            str(row[0])
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
        }
        if "prediction_snapshots" not in tables:
            return _empty_marketpin_reference_prices()
        columns = {
            str(row[1])
            for row in connection.execute("PRAGMA table_info(prediction_snapshots)")
        }
        # Legacy databases remain readable historical context, but without a
        # process epoch they cannot supply current research evidence.
        if not {"subscription_epoch_id", "subscription_generation"} <= columns:
            return _empty_marketpin_reference_prices()
        query = """
            SELECT symbol AS family_root, trading_date, timestamp_utc,
                   quote_timestamp_utc, current_price, provider,
                   subscription_epoch_id AS reference_subscription_epoch_id,
                   subscription_generation AS reference_subscription_generation,
                   data_age_seconds, quote_age_seconds, predicted_close,
                   model_version, prediction_mode, source_payload_json
            FROM prediction_snapshots
            WHERE is_valid=1
              AND lower(COALESCE(validation_status, ''))='valid'
              AND current_price>0
              AND data_age_seconds IS NOT NULL
              AND data_age_seconds<=?
        """
        rows = connection.execute(query, (max_data_age_seconds,)).fetchall()
    result = pd.DataFrame([dict(row) for row in rows])
    if result.empty:
        return _empty_marketpin_reference_prices()
    result["timestamp_utc"] = pd.to_datetime(result["timestamp_utc"], utc=True)
    result["quote_timestamp_utc"] = pd.to_datetime(result["quote_timestamp_utc"], utc=True)
    epochs = result["reference_subscription_epoch_id"].map(_canonical_subscription_epoch)
    generations = result["reference_subscription_generation"].map(
        _positive_subscription_generation
    )
    result["reference_price_epoch_eligible"] = epochs.notna() & generations.notna()
    result["reference_price_epoch_status"] = [
        "eligible_current_epoch"
        if epoch is not None and generation is not None
        else "missing_subscription_epoch"
        if raw_epoch is None or str(raw_epoch).strip() == ""
        else "invalid_subscription_epoch"
        if epoch is None
        else "invalid_subscription_generation"
        for raw_epoch, epoch, generation in zip(
            result["reference_subscription_epoch_id"], epochs, generations
        )
    ]
    # A timestamp shared by more than one process identity has no deterministic
    # winner for an as-of join. Keep the rows as diagnostic context while
    # making the whole timestamp grain ineligible.
    identity = ["family_root", "trading_date", "timestamp_utc"]
    raw_pairs = (
        result["reference_subscription_epoch_id"].astype("string").fillna("<missing>")
        + "|"
        + result["reference_subscription_generation"].astype("string").fillna("<missing>")
    )
    pair_counts = raw_pairs.groupby(
        [result[column] for column in identity], dropna=False
    ).transform("nunique")
    ambiguous = pair_counts > 1
    result.loc[ambiguous, "reference_price_epoch_eligible"] = False
    result.loc[ambiguous, "reference_price_epoch_status"] = (
        "ambiguous_subscription_epoch_at_timestamp"
    )
    def _metadata(raw: object) -> tuple[str, bool, str | None]:
        try:
            payload = json.loads(str(raw or "{}"))
        except (TypeError, ValueError, json.JSONDecodeError):
            payload = {}
        method = str(payload.get("spot_formula_version") or "unspecified-calculated-price")
        status = payload.get("underlying_validation_status")
        # The persisted current_price is generated by the named spot formula.
        # Unknown lineage is conservatively treated as estimated, never observed.
        return method, True, str(status) if status is not None else None

    metadata = result["source_payload_json"].map(_metadata)
    result["reference_price_method"] = metadata.map(lambda item: item[0])
    result["reference_price_is_estimate"] = metadata.map(lambda item: item[1])
    result["reference_underlying_validation_status"] = metadata.map(lambda item: item[2])
    result["reference_price_provider"] = result["provider"]
    result = result.drop(columns=["source_payload_json"])
    return result.sort_values(
        [
            "timestamp_utc", "family_root", "reference_subscription_epoch_id",
            "reference_subscription_generation",
        ],
        na_position="last",
    ).reset_index(drop=True)


def attach_point_in_time_prices(
    features: pd.DataFrame,
    prices: pd.DataFrame,
    *,
    max_price_age_seconds: float = 90.0,
) -> pd.DataFrame:
    """As-of join prices known by minute end; discard missing or stale joins."""
    if max_price_age_seconds <= 0:
        raise ValueError("max_price_age_seconds must be positive")
    required_features = {"trading_date", "family_root", "minute_utc"}
    required_prices = {"trading_date", "family_root", "timestamp_utc", "current_price"}
    if missing := sorted(required_features - set(features.columns)):
        raise ValueError(f"feature columns missing: {', '.join(missing)}")
    if missing := sorted(required_prices - set(prices.columns)):
        raise ValueError(f"price columns missing: {', '.join(missing)}")
    if features.empty or prices.empty:
        result = features.iloc[0:0].copy()
        result["reference_price"] = pd.Series(dtype=float)
        result["reference_price_timestamp_utc"] = pd.Series(dtype="datetime64[ns, UTC]")
        result["reference_price_age_seconds"] = pd.Series(dtype=float)
        return result

    left = features.copy()
    left["minute_utc"] = pd.to_datetime(left["minute_utc"], utc=True)
    left["feature_available_at_utc"] = left["minute_utc"] + pd.Timedelta(minutes=1)
    right = prices.copy()
    right["timestamp_utc"] = pd.to_datetime(right["timestamp_utc"], utc=True)
    group_keys = ["trading_date", "family_root"]
    joined_parts: list[pd.DataFrame] = []
    for keys, feature_group in left.groupby(group_keys, sort=False):
        trading_date, family_root = keys
        price_group = right[
            (right["trading_date"] == trading_date) & (right["family_root"] == family_root)
        ].sort_values("timestamp_utc")
        if price_group.empty:
            continue
        joined_parts.append(
            pd.merge_asof(
                feature_group.sort_values("feature_available_at_utc"),
                price_group,
                left_on="feature_available_at_utc",
                right_on="timestamp_utc",
                direction="backward",
                tolerance=pd.Timedelta(seconds=max_price_age_seconds),
                suffixes=("", "_price"),
            )
        )
    if not joined_parts:
        return attach_point_in_time_prices(features.iloc[0:0], prices.iloc[0:0])
    result = pd.concat(joined_parts, ignore_index=True)
    result = result[result["current_price"].notna()].copy()
    if "reference_price_epoch_eligible" in result.columns:
        result = result[
            result["reference_price_epoch_eligible"].map(
                lambda value: isinstance(value, bool) and value
            )
        ].copy()
    result["reference_price"] = pd.to_numeric(result["current_price"], errors="coerce")
    result["reference_price_timestamp_utc"] = result["timestamp_utc"]
    result["reference_price_age_seconds"] = (
        result["feature_available_at_utc"] - result["reference_price_timestamp_utc"]
    ).dt.total_seconds()
    result = result[
        (result["reference_price"] > 0)
        & (result["reference_price_age_seconds"] >= 0)
        & (result["reference_price_age_seconds"] <= max_price_age_seconds)
    ]
    return result.sort_values(["feature_available_at_utc", "session_id", "family_root"]).reset_index(drop=True)


def attach_point_in_time_reference_prices(
    features: pd.DataFrame,
    primary_prices: pd.DataFrame,
    fallback_prices: pd.DataFrame,
    *,
    max_price_age_seconds: float = 90.0,
) -> pd.DataFrame:
    """Use a fresh primary reference first, then a labeled estimate fallback.

    Selection is performed per feature row, so a newer fallback estimate can
    never displace an eligible primary snapshot merely because of timestamp.
    """
    marker = "_reference_join_row_id"
    if marker in features.columns:
        raise ValueError(f"reserved feature column is present: {marker}")
    work = features.copy().reset_index(drop=True)
    work[marker] = range(len(work))
    primary_prices = primary_prices.copy()
    primary_defaults: dict[str, object] = {
        "reference_subscription_epoch_id": pd.NA,
        "reference_subscription_generation": pd.NA,
        "reference_price_epoch_eligible": False,
        "reference_price_epoch_status": "missing_subscription_epoch",
    }
    for column, default in primary_defaults.items():
        if column not in primary_prices.columns:
            primary_prices[column] = default
    primary = attach_point_in_time_prices(
        work, primary_prices, max_price_age_seconds=max_price_age_seconds
    )
    if not primary.empty:
        primary["reference_price_tier"] = "primary_marketpin_snapshot"
        matched = set(primary[marker].astype(int))
    else:
        matched = set()
    remaining = work[~work[marker].isin(matched)].copy()
    fallback = attach_point_in_time_prices(
        remaining, fallback_prices, max_price_age_seconds=max_price_age_seconds
    )
    if not fallback.empty:
        fallback["reference_price_tier"] = "tcbbo_parity_fallback_estimate"
        fallback["reference_subscription_epoch_id"] = pd.NA
        fallback["reference_subscription_generation"] = pd.NA
        fallback["reference_price_epoch_eligible"] = False
        fallback["reference_price_epoch_status"] = "not_applicable_tcbbo_fallback"
    parts = [part for part in (primary, fallback) if not part.empty]
    if not parts:
        empty = work.iloc[0:0].drop(columns=[marker])
        empty["reference_price"] = pd.Series(dtype=float)
        empty["reference_price_tier"] = pd.Series(dtype=str)
        empty["reference_subscription_epoch_id"] = pd.Series(dtype=str)
        empty["reference_subscription_generation"] = pd.Series(dtype="Int64")
        empty["reference_price_epoch_eligible"] = pd.Series(dtype=bool)
        empty["reference_price_epoch_status"] = pd.Series(dtype=str)
        return empty
    result = pd.concat(parts, ignore_index=True)
    if result[marker].duplicated().any():
        raise ValueError("reference fallback produced duplicate feature matches")
    return result.drop(columns=[marker]).sort_values(
        ["feature_available_at_utc", "session_id", "family_root"]
    ).reset_index(drop=True)


def load_scored_marketpin_closes(
    market_db_path: str | Path,
    *,
    verified_artifact_root: str | Path,
    allowed_families: Iterable[str] = PRODUCTION_FAMILIES,
) -> pd.DataFrame:
    """Load scored closes, rejecting conflicts and unattested source artifacts."""
    path = Path(market_db_path)
    with _read_only(path) as connection:
        exists = connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='eod_close_observations'"
        ).fetchone()
        if not exists:
            rows = []
        else:
            columns = {
                str(row[1]) for row in connection.execute(
                    "PRAGMA table_info(eod_close_observations)"
                )
            }
            if "source_artifact_sha256" not in columns:
                rows = []
            else:
                rows = connection.execute(
                """
                SELECT id, symbol AS family_root, trading_date,
                       official_close AS actual_close, source AS close_source,
                       source_reference AS close_source_reference,
                       source_artifact_sha256 AS close_source_artifact_sha256,
                       observed_at_utc AS close_label_available_at_utc,
                       correction_of_id
                FROM eod_close_observations
                WHERE source_verified=1 AND official_close>0
                  AND observed_at_utc IS NOT NULL
                  AND length(source_artifact_sha256)=64
                  AND source_artifact_sha256 NOT GLOB '*[^0-9a-f]*'
                ORDER BY trading_date, symbol, observed_at_utc, id
                """
                ).fetchall()
    frame = pd.DataFrame([dict(row) for row in rows])
    if frame.empty:
        return pd.DataFrame(
            columns=[
                "family_root", "trading_date", "actual_close", "close_source",
                "close_source_reference", "close_source_artifact_sha256",
                "close_label_available_at_utc", "close_source_artifact_path",
            ]
        )
    allowed = {str(item).upper() for item in allowed_families}
    frame = frame[frame["family_root"].astype(str).str.upper().isin(allowed)].copy()
    if frame.empty:
        return pd.DataFrame(
            columns=[
                "family_root", "trading_date", "actual_close", "close_source",
                "close_source_reference", "close_source_artifact_sha256",
                "close_label_available_at_utc", "close_source_artifact_path",
            ]
        )
    frame["actual_close"] = pd.to_numeric(frame["actual_close"], errors="coerce")
    frame["close_label_available_at_utc"] = pd.to_datetime(
        frame["close_label_available_at_utc"], utc=True
    )
    grouped = frame.groupby(["family_root", "trading_date"], sort=True)
    invalid_corrections = []
    for keys, group in grouped:
        if group["actual_close"].nunique(dropna=True) > 1:
            later = group.iloc[1:]
            if later["correction_of_id"].isna().any():
                invalid_corrections.append(f"{keys[0]}/{keys[1]}")
    if invalid_corrections:
        raise ValueError(
            f"conflicting verified closes without correction lineage: {', '.join(invalid_corrections)}"
        )
    result = grouped.tail(1).reset_index(drop=True)
    result = result[
        result["actual_close"].notna() & (result["actual_close"] > 0)
    ].reset_index(drop=True)
    artifact_paths: list[str] = []
    for row in result.to_dict(orient="records"):
        try:
            validate_official_close_reference(
                str(row["family_root"]),
                str(row["close_source"]),
                str(row["close_source_reference"]),
            )
            artifact = resolve_verified_close_artifact(
                verified_artifact_root,
                trading_date=str(row["trading_date"]),
                symbol=str(row["family_root"]),
                source_artifact_sha256=str(row["close_source_artifact_sha256"]),
            )
        except (OSError, ValueError) as exc:
            raise ValueError(
                "verified close artifact failed for "
                f"{row['trading_date']}/{row['family_root']}: {exc}"
            ) from exc
        artifact_paths.append(str(artifact))
    result["close_source_artifact_path"] = artifact_paths
    return result
