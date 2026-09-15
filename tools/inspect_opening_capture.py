"""Read-only opening-capture acceptance inspector.

This tool never imports or starts the Databento streamer. It reads existing
HTTP health/ORB projections, Windows clock telemetry, and SQLite evidence.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import ntpath
import os
import re
import sqlite3
import statistics
import subprocess
import sys
import xml.etree.ElementTree as ET
import zlib
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.utils.market_time import is_early_close_day, is_holiday, is_weekend
from backend.monitor_opening_acceptance import (
    MonitorOpeningAcceptanceError,
    _is_gamma_scope_excluded_issue,
    _is_orb_scope_excluded_issue,
    _subscription_staging_issues,
    _validate_loaded_code_fingerprint,
)


CT = ZoneInfo("America/Chicago")
UTC = timezone.utc
CORE_SYMBOLS = ("SPX", "NDX")
OPENING_SYMBOLS = ("SPX", "NDX", "VIX", "RUT")
ACCEPTANCE_SCOPES = (
    "startup",
    "first_eligible_gamma_capture",
    "complete_5m_orb",
    "complete_60m_orb",
)
OPTION_ROOT_ALIASES = {
    "SPX": ("SPXW", "SPX", "XSP"),
    "NDX": ("NDXP", "NDX", "XND"),
    "VIX": ("VIXW", "VIX"),
    "RUT": ("RUTW", "RUT", "MRUT"),
}
RUT_CANARY_MAX_SUBSCRIPTION_CONTRACTS = 3_200
AUTOSTART_TASK_NAME = "MarketPinPredictor_AutoStart"
WATCHDOG_TASK_NAME = "MarketPinPredictor_Watchdog"
TASK_XML_NAMESPACE = {"task": "http://schemas.microsoft.com/windows/2004/02/mit/task"}
WEEKDAY_TRIGGER_NAMES = {"Monday", "Tuesday", "Wednesday", "Thursday", "Friday"}
EXPECTED_MARKET_STRUCTURE_TRIGGERS = {
    "market_structure_observations_insert_guard",
    "market_structure_observations_epoch_guard",
    "market_structure_observations_no_update",
    "market_structure_observations_no_delete",
}
EXPECTED_MARKET_STRUCTURE_COLUMNS = {
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
    "gamma_pin",
    "max_pain",
    "primary_expiration",
    "same_day_profile_available",
    "universe_sha256",
    "validation_status",
}
ORB_REFERENCE_CADENCE_SECONDS = 5
ORB_REFERENCE_MIN_CAPTURE_RATIO = 0.95
ORB_REFERENCE_MAX_CURRENT_AGE_SECONDS = 15.0
ORB_REFERENCE_MAX_OPENING_GAP_SECONDS = 30.0
MARKET_STRUCTURE_MAX_AGE_SECONDS = 90.0
GAMMA_INPUT_SCHEMA_VERSION = "gamma-inputs-v2-point-in-time"
STARTUP_ACCEPTANCE_GRACE_END_CT = time(7, 55)
EXPECTED_ORB_REFERENCE_TRIGGERS = {
    "orb_reference_samples_existing_guard",
    "orb_reference_samples_epoch_guard",
    "orb_reference_samples_insert_guard",
    "orb_reference_samples_no_update",
    "orb_reference_samples_no_delete",
}
EXPECTED_ORB_REFERENCE_INDEXES = {"uix_orb_reference_logical_sample"}
EXPECTED_ORB_REFERENCE_DECISION_TRIGGERS = {
    "orb_reference_sample_decisions_existing_guard",
    "orb_reference_sample_decisions_insert_guard",
    "orb_reference_sample_decisions_no_update",
    "orb_reference_sample_decisions_no_delete",
}
EXPECTED_ORB_REFERENCE_DECISION_INDEXES = {
    "uix_orb_reference_decision_sample_id"
}
EXPECTED_ORB_REFERENCE_DECISION_COLUMNS = {
    "sample_id",
    "sample_timestamp_utc",
    "intended_bucket_utc",
    "attempt_completed_at_utc",
    "progress_eligible",
    "reason",
    "decision_status",
}
EXPECTED_ORB_REFERENCE_COLUMNS = {
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
}
OFFSET_PATTERN = re.compile(r"[,\s]([+-]\d+(?:\.\d+)?)s\s*$", re.MULTILINE)


def _parse_utc(value: object) -> datetime | None:
    if value in (None, ""):
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _utc_iso(value: object) -> str | None:
    parsed = _parse_utc(value)
    if parsed is None:
        return None
    return parsed.isoformat().replace("+00:00", "Z")


def _canonical_subscription_epoch(value: object) -> str | None:
    candidate = str(value or "").strip()
    if re.fullmatch(r"[0-9a-f]{64}", candidate) is None:
        return None
    return candidate


def _canonical_subscription_generation(value: object) -> int | None:
    """Return a positive JSON/SQLite integer generation, never a coercion."""
    if type(value) is not int or value <= 0:
        return None
    return value


def _positive_finite(value: object) -> float | None:
    """Return a positive finite level without accepting booleans."""
    if isinstance(value, bool):
        return None
    try:
        candidate = float(value)
    except (TypeError, ValueError):
        return None
    return candidate if math.isfinite(candidate) and candidate > 0.0 else None


def _same_positive_finite(left: object, right: object) -> bool:
    """Compare two persisted positive levels without truthy coercion."""

    left_value = _positive_finite(left)
    right_value = _positive_finite(right)
    return (
        left_value is not None
        and right_value is not None
        and left_value == right_value
    )


def _canonical_json_bytes(value: object) -> bytes:
    """Serialize decoded evidence exactly as the gamma-input writer does."""

    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")


def _is_canonical_v2_gamma_input_payload(payload: object) -> bool:
    """Require the full-input v2 container, not a summary-only facsimile."""

    if not isinstance(payload, dict):
        return False
    raw_rows = payload.get("raw_fresh_chain_rows")
    calculated_rows = payload.get("calculated_gex_rows")
    return bool(
        payload.get("input_schema_version") == GAMMA_INPUT_SCHEMA_VERSION
        and isinstance(payload.get("calculated_at_utc"), str)
        and isinstance(raw_rows, list)
        and all(isinstance(row, dict) for row in raw_rows)
        and isinstance(calculated_rows, list)
        and all(isinstance(row, dict) for row in calculated_rows)
        and isinstance(payload.get("parameters"), dict)
        and isinstance(payload.get("rejection_counts"), dict)
        and isinstance(payload.get("output_summary"), dict)
    )


def _calculation_lineage_evidence(
    connection: sqlite3.Connection,
    *,
    table_names: set[str],
    structure: sqlite3.Row,
    symbol: str,
    trading_day: date,
    observed_utc: datetime,
    maximum_run_age_seconds: float | None = MARKET_STRUCTURE_MAX_AGE_SECONDS,
) -> dict[str, Any]:
    """Verify one structure row against its immutable full-input calculation."""

    calculation_id = str(structure["calculation_id"] or "").strip()
    base: dict[str, Any] = {
        "status": "invalid",
        "reason": "CALCULATION_ID_MISSING",
        "calculation_id": calculation_id or None,
        "gamma_run_present": False,
        "input_blob_present": False,
        "payload_integrity_verified": False,
        "payload_sha256": None,
        "run_calculated_at_utc": None,
        "run_age_seconds": None,
    }
    if not calculation_id:
        return base
    if "gamma_calculation_runs" not in table_names:
        base["reason"] = "GAMMA_CALCULATION_RUN_TABLE_MISSING"
        return base
    if "gamma_calculation_input_blobs" not in table_names:
        base["reason"] = "GAMMA_CALCULATION_INPUT_BLOB_TABLE_MISSING"
        return base
    try:
        run = connection.execute(
            "SELECT id, calculation_id, symbol, trading_date, calculated_at_utc, "
            "provider, subscription_epoch_id, subscription_generation, status, "
            "input_schema_version, universe_sha256, spot_price, gamma_pin, max_pain "
            "FROM gamma_calculation_runs "
            "WHERE calculation_id=? AND symbol=? AND trading_date=? LIMIT 1",
            (calculation_id, symbol, trading_day.isoformat()),
        ).fetchone()
    except sqlite3.Error:
        base["reason"] = "GAMMA_CALCULATION_RUN_QUERY_FAILED"
        return base
    if run is None:
        base["reason"] = "GAMMA_CALCULATION_RUN_MISSING"
        return base
    base["gamma_run_present"] = True
    calculated_at = _parse_utc(run["calculated_at_utc"])
    base["run_calculated_at_utc"] = _utc_iso(run["calculated_at_utc"])
    base["run_age_seconds"] = (
        (observed_utc - calculated_at).total_seconds()
        if calculated_at is not None
        else None
    )
    if (
        str(run["calculation_id"] or "").strip() != calculation_id
        or str(run["symbol"] or "").upper().strip() != symbol
        or str(run["trading_date"] or "") != trading_day.isoformat()
        or str(run["provider"] or "").lower().strip() != "databento"
        or str(run["status"] or "").lower().strip() != "valid"
        or _canonical_subscription_epoch(run["subscription_epoch_id"])
        != _canonical_subscription_epoch(structure["subscription_epoch_id"])
        or _canonical_subscription_generation(run["subscription_generation"])
        != _canonical_subscription_generation(structure["subscription_generation"])
        or not _same_positive_finite(run["spot_price"], structure["reference_price"])
        or not _same_positive_finite(run["gamma_pin"], structure["gamma_pin"])
        or not _same_positive_finite(run["max_pain"], structure["max_pain"])
        or calculated_at is None
        or base["run_age_seconds"] is None
        or float(base["run_age_seconds"]) < 0.0
        or (
            maximum_run_age_seconds is not None
            and float(base["run_age_seconds"]) > maximum_run_age_seconds
        )
    ):
        base["reason"] = "GAMMA_CALCULATION_RUN_MISMATCH"
        return base
    try:
        blob = connection.execute(
            "SELECT encoding, payload_sha256, uncompressed_bytes, "
            "compressed_bytes, payload FROM gamma_calculation_input_blobs "
            "WHERE calculation_run_id=? LIMIT 1",
            (run["id"],),
        ).fetchone()
    except sqlite3.Error:
        base["reason"] = "GAMMA_CALCULATION_INPUT_BLOB_QUERY_FAILED"
        return base
    if blob is None:
        base["reason"] = "GAMMA_CALCULATION_INPUT_BLOB_MISSING"
        return base
    base["input_blob_present"] = True
    payload_sha256 = str(blob["payload_sha256"] or "").strip()
    base["payload_sha256"] = payload_sha256 or None
    try:
        compressed = bytes(blob["payload"])
        decompressor = zlib.decompressobj()
        canonical = decompressor.decompress(compressed)
        canonical += decompressor.flush()
        compressed_stream_complete = bool(
            decompressor.eof
            and not decompressor.unused_data
            and not decompressor.unconsumed_tail
        )
        payload = json.loads(canonical.decode("utf-8"))
        canonical_payload_matches = _canonical_json_bytes(payload) == canonical
    except (TypeError, ValueError, UnicodeDecodeError, zlib.error, json.JSONDecodeError):
        base["reason"] = "GAMMA_CALCULATION_INPUT_BLOB_INVALID"
        return base
    if (
        str(blob["encoding"] or "") != "canonical-json+zlib-v1"
        or type(blob["uncompressed_bytes"]) is not int
        or type(blob["compressed_bytes"]) is not int
        or int(blob["uncompressed_bytes"]) != len(canonical)
        or int(blob["compressed_bytes"]) != len(compressed)
        or re.fullmatch(r"[0-9a-f]{64}", payload_sha256) is None
        or hashlib.sha256(canonical).hexdigest() != payload_sha256
        or not compressed_stream_complete
        or not isinstance(payload, dict)
        or not canonical_payload_matches
    ):
        base["reason"] = "GAMMA_CALCULATION_INPUT_INTEGRITY_INVALID"
        return base
    if not _is_canonical_v2_gamma_input_payload(payload):
        base["reason"] = "GAMMA_CALCULATION_INPUT_SCHEMA_INVALID"
        return base
    payload_calculated_at = _parse_utc(payload.get("calculated_at_utc"))
    structure_captured_at = _parse_utc(structure["captured_at_utc"])
    summary = payload.get("output_summary")
    summary = summary if isinstance(summary, dict) else {}
    provenance = summary.get("universe_provenance")
    provenance = provenance if isinstance(provenance, dict) else {}
    run_source_universe = _canonical_subscription_epoch(run["universe_sha256"])
    summary_source_universe = _canonical_subscription_epoch(
        summary.get("universe_sha256")
    )
    structure_selected_universe = _canonical_subscription_epoch(
        structure["universe_sha256"]
    )
    summary_selected_universe = _canonical_subscription_epoch(
        summary.get("selected_universe_sha256")
    )
    if (
        str(run["input_schema_version"] or "") != GAMMA_INPUT_SCHEMA_VERSION
        or payload_calculated_at is None
        or payload_calculated_at != calculated_at
        or structure_captured_at is None
        or calculated_at > structure_captured_at
        or str(payload.get("calculation_id") or "").strip() != calculation_id
        or str(payload.get("symbol") or "").upper().strip() != symbol
        or _canonical_subscription_epoch(payload.get("subscription_epoch_id"))
        != _canonical_subscription_epoch(structure["subscription_epoch_id"])
        or _canonical_subscription_generation(payload.get("subscription_generation"))
        != _canonical_subscription_generation(structure["subscription_generation"])
        or str(summary.get("calculation_id") or "").strip() != calculation_id
        or str(summary.get("symbol") or "").upper().strip() != symbol
        or _canonical_subscription_epoch(summary.get("subscription_epoch_id"))
        != _canonical_subscription_epoch(structure["subscription_epoch_id"])
        or _canonical_subscription_generation(summary.get("subscription_generation"))
        != _canonical_subscription_generation(structure["subscription_generation"])
        or summary.get("validation_is_valid") is not True
        or summary.get("gamma_excluded_from_model") is not False
        or not _same_positive_finite(summary.get("price"), structure["reference_price"])
        or not _same_positive_finite(summary.get("gamma_pin"), structure["gamma_pin"])
        or not _same_positive_finite(summary.get("max_pain"), structure["max_pain"])
        or str(summary.get("primary_expiration") or "")
        != str(structure["primary_expiration"] or "")
        or summary.get("same_day_profile_available") is not True
        or structure["same_day_profile_available"] != 1
        or structure_selected_universe is None
        or summary_selected_universe != structure_selected_universe
        or run_source_universe is None
        or summary_source_universe != run_source_universe
        or _canonical_subscription_epoch(provenance.get("source_sha256"))
        != run_source_universe
        or provenance.get("is_fallback") is not False
        or str(provenance.get("trading_date") or "") != trading_day.isoformat()
        or str(provenance.get("source_date") or "") != trading_day.isoformat()
    ):
        base["reason"] = "GAMMA_CALCULATION_INPUT_MISMATCH"
        return base
    base.update(
        status="verified",
        reason=None,
        payload_integrity_verified=True,
    )
    return base


def _first_eligible_calculation_bound_pair_evidence(
    connection: sqlite3.Connection,
    *,
    table_names: set[str],
    structure_columns: set[str],
    trading_day: date,
    observed_utc: datetime,
) -> dict[str, dict[str, Any]]:
    """Select the first fully verified SPX/NDX pair across same-day identities."""

    def unavailable(reason: str) -> dict[str, Any]:
        return {
            "candidate_count": 0,
            "selection_status": "unavailable",
            "selection_reason": reason,
            "lineage": {
                "status": "invalid",
                "reason": reason,
                "calculation_id": None,
                "gamma_run_present": False,
                "input_blob_present": False,
                "payload_integrity_verified": False,
                "payload_sha256": None,
                "run_calculated_at_utc": None,
            },
        }

    def unavailable_pair(reason: str) -> dict[str, dict[str, Any]]:
        return {symbol: unavailable(reason) for symbol in CORE_SYMBOLS}

    required_tables = {
        "market_structure_observations",
        "gamma_calculation_runs",
        "gamma_calculation_input_blobs",
    }
    if not required_tables.issubset(table_names):
        return unavailable_pair("FIRST_ELIGIBLE_LINEAGE_TABLE_MISSING")

    identity_columns = {
        "observation_id",
        "trading_date",
        "provider",
        "subscription_epoch_id",
        "subscription_generation",
        "calculation_id",
        "primary_expiration",
        "same_day_profile_available",
        "universe_sha256",
        "validation_status",
    }
    if not identity_columns.issubset(structure_columns):
        return unavailable_pair(
            "FIRST_ELIGIBLE_STRUCTURE_SCHEMA_INCOMPATIBLE"
        )

    evidence_fields = (
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
        "gamma_pin",
        "max_pain",
        "primary_expiration",
        "same_day_profile_available",
        "universe_sha256",
        "validation_status",
    )
    select_fields = ", ".join(
        f"m.{field} AS {field}"
        if field in structure_columns
        else f"NULL AS {field}"
        for field in evidence_fields
    )
    try:
        raw_candidates = connection.execute(
            f"SELECT {select_fields}, "
            "r.calculated_at_utc AS candidate_run_calculated_at_utc "
            "FROM market_structure_observations AS m "
            "JOIN gamma_calculation_runs AS r "
            "ON r.calculation_id=m.calculation_id "
            "AND r.symbol=m.symbol AND r.trading_date=m.trading_date "
            "WHERE m.symbol IN (?, ?) AND m.trading_date=? "
            "AND m.calculation_id IS NOT NULL AND TRIM(m.calculation_id) <> '' "
            "AND lower(m.provider)='databento' AND m.validation_status='valid' "
            "AND lower(r.provider)='databento' AND lower(r.status)='valid'",
            (
                *CORE_SYMBOLS,
                trading_day.isoformat(),
            ),
        ).fetchall()
    except sqlite3.Error:
        return unavailable_pair("FIRST_ELIGIBLE_CALCULATION_QUERY_FAILED")

    cash_open_utc = datetime.combine(trading_day, time(8, 30), tzinfo=CT).astimezone(
        UTC
    )
    cash_close = time(12, 0) if is_early_close_day(trading_day) else time(15, 0)
    cash_close_utc = datetime.combine(trading_day, cash_close, tzinfo=CT).astimezone(
        UTC
    )
    identity_groups: dict[
        tuple[str, str, int, str, str, str, bool],
        dict[
            str,
            list[tuple[datetime, datetime, datetime, str, dict[str, Any]]],
        ],
    ] = {}
    for candidate in raw_candidates:
        symbol = str(candidate["symbol"] or "").upper().strip()
        provider = str(candidate["provider"] or "").lower().strip()
        epoch = _canonical_subscription_epoch(candidate["subscription_epoch_id"])
        generation = _canonical_subscription_generation(
            candidate["subscription_generation"]
        )
        universe = _canonical_subscription_epoch(candidate["universe_sha256"])
        candidate_trading_date = str(candidate["trading_date"] or "")
        primary_expiration = str(candidate["primary_expiration"] or "")
        same_day = candidate["same_day_profile_available"] == 1
        run_at = _parse_utc(candidate["candidate_run_calculated_at_utc"])
        source_at = _parse_utc(candidate["source_timestamp_utc"])
        captured_at = _parse_utc(candidate["captured_at_utc"])
        if (
            symbol not in CORE_SYMBOLS
            or provider != "databento"
            or epoch is None
            or generation is None
            or universe is None
            or candidate_trading_date != trading_day.isoformat()
            or primary_expiration != trading_day.isoformat()
            or not same_day
            or _positive_finite(candidate["reference_price"]) is None
            or _positive_finite(candidate["gamma_pin"]) is None
            or _positive_finite(candidate["max_pain"]) is None
            or run_at is None
            or source_at is None
            or captured_at is None
            or not cash_open_utc
            <= source_at
            <= run_at
            <= captured_at
            <= min(observed_utc, cash_close_utc)
        ):
            continue
        lineage = _calculation_lineage_evidence(
            connection,
            table_names=table_names,
            structure=candidate,
            symbol=symbol,
            trading_day=trading_day,
            observed_utc=observed_utc,
            maximum_run_age_seconds=None,
        )
        if lineage.get("status") != "verified":
            continue
        immutable_lineage = {
            key: value
            for key, value in lineage.items()
            if key != "run_age_seconds"
        }
        immutable_lineage["source_to_run_seconds"] = (
            run_at - source_at
        ).total_seconds()
        immutable_lineage["run_to_capture_seconds"] = (
            captured_at - run_at
        ).total_seconds()
        evidence = {
            "selection_status": "verified",
            "selection_reason": None,
            "observation_id": candidate["observation_id"],
            "trading_date": candidate_trading_date,
            "source_timestamp_utc": _utc_iso(candidate["source_timestamp_utc"]),
            "captured_at_utc": _utc_iso(candidate["captured_at_utc"]),
            "provider": provider,
            "subscription_epoch_id": epoch,
            "subscription_generation": generation,
            "calculation_id": candidate["calculation_id"],
            "reference_price": candidate["reference_price"],
            "gamma_pin": candidate["gamma_pin"],
            "max_pain": candidate["max_pain"],
            "primary_expiration": candidate["primary_expiration"],
            "same_day_profile_available": (
                bool(candidate["same_day_profile_available"])
                if candidate["same_day_profile_available"] is not None
                else None
            ),
            "universe_sha256": universe,
            "validation_status": candidate["validation_status"],
            "lineage": immutable_lineage,
        }
        identity = (
            provider,
            epoch,
            generation,
            universe,
            candidate_trading_date,
            primary_expiration,
            same_day,
        )
        identity_groups.setdefault(identity, {}).setdefault(symbol, []).append(
            (
                run_at,
                captured_at,
                source_at,
                str(candidate["observation_id"] or ""),
                evidence,
            )
        )

    coherent_groups: list[
        tuple[
            datetime,
            datetime,
            tuple[str, str, int, str, str, str, bool],
            dict[str, dict[str, Any]],
        ]
    ] = []
    for identity, grouped in identity_groups.items():
        if any(not grouped.get(symbol) for symbol in CORE_SYMBOLS):
            continue
        selected: dict[str, dict[str, Any]] = {}
        selected_rows: dict[
            str, tuple[datetime, datetime, datetime, str, dict[str, Any]]
        ] = {}
        for symbol in CORE_SYMBOLS:
            first = min(
                grouped[symbol],
                key=lambda item: (
                    item[1],
                    item[0],
                    item[2],
                    item[3],
                    str(item[4].get("calculation_id") or ""),
                ),
            )
            selected_rows[symbol] = first
            selected[symbol] = first[4]
            selected[symbol]["candidate_count"] = len(grouped[symbol])
        pair_completed_at = max(row[1] for row in selected_rows.values())
        pair_latest_run_at = max(row[0] for row in selected_rows.values())
        coherent_groups.append(
            (pair_completed_at, pair_latest_run_at, identity, selected)
        )

    if not coherent_groups:
        return unavailable_pair("COHERENT_FIRST_ELIGIBLE_PAIR_NOT_FOUND")
    pair_completed_at, _pair_latest_run_at, _identity, selected_pair = min(
        coherent_groups,
        key=lambda item: (item[0], item[1], item[2]),
    )
    pair_completed_at_utc = _utc_iso(pair_completed_at)
    for evidence in selected_pair.values():
        evidence["pair_completed_at_utc"] = pair_completed_at_utc
    return selected_pair


def _orb_reference_evidence(
    rows: list[sqlite3.Row],
    *,
    trading_day: date,
    observed_at_ct: datetime,
) -> dict[str, dict[str, Any]]:
    observed_ct = observed_at_ct.astimezone(CT)
    observed_utc = observed_ct.astimezone(UTC)
    open_ct = datetime.combine(trading_day, time(8, 30), tzinfo=CT)
    opening_end_ct = datetime.combine(trading_day, time(9, 30), tzinfo=CT)
    elapsed_end_ct = min(max(observed_ct, open_ct), opening_end_ct)
    expected_samples = max(
        0,
        int((elapsed_end_ct - open_ct).total_seconds())
        // ORB_REFERENCE_CADENCE_SECONDS,
    )
    counted_end_utc = (
        open_ct.astimezone(UTC)
        + expected_samples * timedelta(seconds=ORB_REFERENCE_CADENCE_SECONDS)
    )
    grouped: dict[
        str,
        list[
            tuple[
                datetime,
                datetime,
                datetime,
                str | None,
                int | None,
                str,
                str,
            ]
        ],
    ] = {}
    classification_candidates: dict[
        str,
        list[tuple[datetime, datetime, datetime, str | None, bool | None]],
    ] = {}
    invalid_timestamp_rows: dict[str, int] = {}
    for row in rows:
        symbol = str(row["symbol"] or "").upper().strip()
        sample = _parse_utc(row["sample_timestamp_utc"])
        source = _parse_utc(row["source_timestamp_utc"])
        captured = _parse_utc(row["captured_at_utc"])
        epoch_id = _canonical_subscription_epoch(row["subscription_epoch_id"])
        generation = _canonical_subscription_generation(
            row["subscription_generation"]
        )
        provider = str(row["provider"] or "").lower().strip()
        validation_status = str(row["validation_status"] or "").lower().strip()
        row_keys = set(row.keys())
        primary_expiration = str(
            row["primary_expiration"] if "primary_expiration" in row_keys else ""
        ).strip() or None
        raw_same_day = (
            row["same_day_profile_available"]
            if "same_day_profile_available" in row_keys
            else None
        )
        same_day_profile_available = (
            True
            if raw_same_day == 1
            else False
            if raw_same_day == 0
            else None
        )
        if not symbol or sample is None or source is None or captured is None:
            invalid_timestamp_rows[symbol] = invalid_timestamp_rows.get(symbol, 0) + 1
            continue
        grouped.setdefault(symbol, []).append(
            (
                sample,
                source,
                captured,
                epoch_id,
                generation,
                provider,
                validation_status,
            )
        )
        classification_candidates.setdefault(symbol, []).append(
            (
                sample,
                source,
                captured,
                primary_expiration,
                same_day_profile_available,
            )
        )

    evidence: dict[str, dict[str, Any]] = {}
    opening_start_utc = open_ct.astimezone(UTC)
    regular_close_utc = datetime.combine(
        trading_day,
        time(12, 0) if is_early_close_day(trading_day) else time(15, 0),
        tzinfo=CT,
    ).astimezone(UTC)
    provider_advancement_window = timedelta(
        seconds=(
            ORB_REFERENCE_MAX_CURRENT_AGE_SECONDS
            + ORB_REFERENCE_CADENCE_SECONDS * 2
        )
    )
    for symbol, samples in grouped.items():
        samples.sort(key=lambda item: (item[0], item[2], item[1]))
        opening_buckets = {
            int(sample.timestamp()) // ORB_REFERENCE_CADENCE_SECONDS
            for (
                sample,
                _source,
                _captured,
                _epoch_id,
                _generation,
                _provider,
                _validation_status,
            ) in samples
            if opening_start_utc <= sample < counted_end_utc
        }
        opening_bucket = int(opening_start_utc.timestamp()) // ORB_REFERENCE_CADENCE_SECONDS
        opening_bucket_present = opening_bucket in opening_buckets
        ordered_buckets = sorted(opening_buckets)
        opening_gap_seconds: list[float] = []
        if ordered_buckets:
            first_sample = datetime.fromtimestamp(
                ordered_buckets[0] * ORB_REFERENCE_CADENCE_SECONDS,
                tz=UTC,
            )
            last_sample = datetime.fromtimestamp(
                ordered_buckets[-1] * ORB_REFERENCE_CADENCE_SECONDS,
                tz=UTC,
            )
            opening_gap_seconds.extend(
                [
                    (first_sample - opening_start_utc).total_seconds(),
                    (counted_end_utc - last_sample).total_seconds(),
                ]
            )
            opening_gap_seconds.extend(
                (right - left) * ORB_REFERENCE_CADENCE_SECONDS
                for left, right in zip(ordered_buckets, ordered_buckets[1:])
            )
        (
            latest_sample,
            latest_source,
            latest_capture,
            latest_epoch_id,
            latest_generation,
            latest_provider,
            latest_validation_status,
        ) = max(
            samples, key=lambda item: (item[2], item[0], item[1])
        )
        latest_classification = max(
            classification_candidates[symbol],
            key=lambda item: (item[2], item[0], item[1]),
        )
        epoch_ids = sorted(
            {
                epoch_id
                for (
                    _sample,
                    _source,
                    _captured,
                    epoch_id,
                    _generation,
                    _provider,
                    _validation_status,
                ) in samples
                if epoch_id
            }
        )
        generations = sorted(
            {
                generation
                for (
                    _sample,
                    _source,
                    _captured,
                    _epoch_id,
                    generation,
                    _provider,
                    _validation_status,
                ) in samples
                if generation is not None
            }
        )
        invalid_epoch_count = sum(
            1
            for (
                _sample,
                _source,
                _captured,
                epoch_id,
                _generation,
                _provider,
                _validation_status,
            ) in samples
            if epoch_id is None
        )
        invalid_generation_count = sum(
            1
            for (
                _sample,
                _source,
                _captured,
                _epoch_id,
                generation,
                _provider,
                _validation_status,
            ) in samples
            if generation is None
        )
        capture_ratio = (
            min(1.0, len(opening_buckets) / expected_samples)
            if expected_samples
            else None
        )
        sample_age = (observed_utc - latest_sample).total_seconds()
        source_age = (observed_utc - latest_source).total_seconds()
        capture_age = (observed_utc - latest_capture).total_seconds()
        off_cadence = sum(
            1
            for (
                sample,
                _source,
                _captured,
                _epoch_id,
                _generation,
                _provider,
                _validation_status,
            ) in samples
            if int(sample.timestamp()) % ORB_REFERENCE_CADENCE_SECONDS
            or sample.microsecond
        )
        eligible_provider_samples = [
            item
            for item in samples
            if item[5] == "databento"
            and item[6] == "valid"
            and opening_start_utc <= item[1] < regular_close_utc
            and 0.0
            <= (item[2] - item[0]).total_seconds()
            < ORB_REFERENCE_CADENCE_SECONDS
            and (item[2] - item[1]).total_seconds() >= -0.05
        ]
        recent_provider_samples = sorted(
            (
                item
                for item in eligible_provider_samples
                if observed_utc - provider_advancement_window
                <= item[2]
                <= observed_utc + timedelta(seconds=0.05)
            ),
            key=lambda item: (item[2], item[0], item[1]),
        )
        recent_provider_sources = [item[1] for item in recent_provider_samples]
        distinct_recent_provider_sources = sorted(set(recent_provider_sources))
        provider_source_timestamp_advancement_count = sum(
            right > left
            for left, right in zip(
                recent_provider_sources,
                recent_provider_sources[1:],
            )
        )
        provider_source_timestamp_regression_count = sum(
            right < left
            for left, right in zip(
                recent_provider_sources,
                recent_provider_sources[1:],
            )
        )
        provider_source_timestamps_advancing = bool(
            len(distinct_recent_provider_sources) >= 2
            and recent_provider_sources[-1] > recent_provider_sources[0]
            and provider_source_timestamp_advancement_count > 0
            and provider_source_timestamp_regression_count == 0
        )
        invalid_provider_source_row_count = len(samples) - len(
            eligible_provider_samples
        )
        current = bool(
            sample_age >= 0.0
            and capture_age >= 0.0
            and source_age >= 0.0
            and sample_age <= ORB_REFERENCE_MAX_CURRENT_AGE_SECONDS
            and capture_age <= ORB_REFERENCE_MAX_CURRENT_AGE_SECONDS
            and source_age
            <= ORB_REFERENCE_MAX_CURRENT_AGE_SECONDS
            + ORB_REFERENCE_CADENCE_SECONDS * 2
        )
        maximum_opening_gap_seconds = (
            max(opening_gap_seconds) if opening_gap_seconds else None
        )
        evidence[symbol] = {
            "row_count": len(samples),
            "invalid_timestamp_row_count": invalid_timestamp_rows.get(symbol, 0),
            "cadence_seconds": ORB_REFERENCE_CADENCE_SECONDS,
            "expected_opening_sample_count": expected_samples,
            "distinct_opening_bucket_count": len(opening_buckets),
            "opening_bucket_present": opening_bucket_present,
            "opening_capture_ratio": capture_ratio,
            "minimum_capture_ratio": ORB_REFERENCE_MIN_CAPTURE_RATIO,
            "maximum_opening_gap_seconds": (
                maximum_opening_gap_seconds
            ),
            "maximum_allowed_opening_gap_seconds": (
                ORB_REFERENCE_MAX_OPENING_GAP_SECONDS
            ),
            "off_cadence_row_count": off_cadence,
            "eligible_provider_source_row_count": len(eligible_provider_samples),
            "invalid_provider_source_row_count": invalid_provider_source_row_count,
            "recent_provider_source_row_count": len(recent_provider_samples),
            "distinct_recent_provider_source_timestamp_count": len(
                distinct_recent_provider_sources
            ),
            "provider_source_timestamp_advancement_count": (
                provider_source_timestamp_advancement_count
            ),
            "provider_source_timestamp_regression_count": (
                provider_source_timestamp_regression_count
            ),
            "earliest_recent_provider_source_timestamp_utc": (
                recent_provider_sources[0].isoformat()
                if recent_provider_sources
                else None
            ),
            "latest_recent_provider_source_timestamp_utc": (
                recent_provider_sources[-1].isoformat()
                if recent_provider_sources
                else None
            ),
            "provider_source_timestamps_advancing": (
                provider_source_timestamps_advancing
            ),
            "latest_sample_timestamp_utc": latest_sample.isoformat(),
            "latest_source_timestamp_utc": latest_source.isoformat(),
            "latest_captured_at_utc": latest_capture.isoformat(),
            "latest_subscription_epoch_id": latest_epoch_id,
            "subscription_epoch_ids": epoch_ids,
            "invalid_subscription_epoch_row_count": invalid_epoch_count,
            "mixed_subscription_epoch_rows": len(epoch_ids) > 1,
            "latest_subscription_generation": latest_generation,
            "subscription_generations": generations,
            "invalid_subscription_generation_row_count": invalid_generation_count,
            "mixed_subscription_generation_rows": len(generations) > 1,
            "latest_provider": latest_provider,
            "latest_validation_status": latest_validation_status,
            "latest_primary_expiration": latest_classification[3],
            "latest_same_day_profile_available": latest_classification[4],
            "latest_sample_age_seconds": sample_age,
            "latest_source_age_seconds": source_age,
            "latest_capture_age_seconds": capture_age,
            "current_reference_evidence": current,
            "advancing_5s_evidence": bool(
                current
                and off_cadence == 0
                and invalid_epoch_count == 0
                and len(epoch_ids) == 1
                and invalid_generation_count == 0
                and len(generations) == 1
                and invalid_provider_source_row_count == 0
                and provider_source_timestamps_advancing
                and maximum_opening_gap_seconds is not None
                and opening_bucket_present
                and maximum_opening_gap_seconds
                <= ORB_REFERENCE_MAX_OPENING_GAP_SECONDS
                and (
                    capture_ratio is None
                    or capture_ratio >= ORB_REFERENCE_MIN_CAPTURE_RATIO
                )
            ),
        }
    return evidence


def _fetch(url: str, timeout: float, *, json_body: bool = True) -> dict[str, Any]:
    try:
        with urlopen(
            Request(url, headers={"Accept": "application/json"}),
            timeout=timeout,
        ) as response:
            body = response.read().decode("utf-8", errors="replace")
            return {
                "ok": 200 <= response.status < 300,
                "status_code": response.status,
                "payload": json.loads(body) if json_body else body.strip()[:200],
            }
    except (HTTPError, URLError, TimeoutError, json.JSONDecodeError) as exc:
        return {"ok": False, "error": f"{type(exc).__name__}: {exc}"}


def parse_clock_outputs(status_text: str, stripchart_text: str) -> dict[str, Any]:
    offsets = [float(value) for value in OFFSET_PATTERN.findall(stripchart_text)]
    leap_match = re.search(r"Leap Indicator:\s*(\d+)", status_text, re.IGNORECASE)
    leap_indicator = int(leap_match.group(1)) if leap_match else None
    median_offset = statistics.median(offsets) if offsets else None
    spread = max(offsets) - min(offsets) if len(offsets) > 1 else 0.0 if offsets else None
    windows_time_synchronized = bool(
        leap_indicator is not None and leap_indicator != 3
    )
    external_offset_verified = median_offset is not None
    if not windows_time_synchronized:
        synchronized: bool | None = False
        status = "unsynchronized"
    elif not external_offset_verified:
        synchronized = None
        status = "windows_time_synchronized_external_offset_unavailable"
    else:
        synchronized = abs(median_offset) <= 0.25
        status = "synchronized" if synchronized else "external_offset_exceeded"
    return {
        "status": status,
        "synchronized": synchronized,
        "windows_time_synchronized": windows_time_synchronized,
        "external_offset_verified": external_offset_verified,
        "leap_indicator": leap_indicator,
        "offset_samples_seconds": offsets,
        "median_offset_seconds": median_offset,
        "sample_spread_seconds": spread,
        "maximum_allowed_absolute_offset_seconds": 0.25,
    }


def inspect_windows_clock(samples: int = 3) -> dict[str, Any]:
    if sys.platform != "win32":
        return {"applicable": False, "synchronized": None}
    try:
        status = subprocess.run(
            ["w32tm", "/query", "/status"],
            check=False,
            capture_output=True,
            text=True,
            timeout=10,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        return {
            "applicable": True,
            "synchronized": False,
            "windows_time_synchronized": False,
            "external_offset_verified": False,
            "error": f"{type(exc).__name__}: {exc}",
        }
    stripchart_error: str | None = None
    try:
        stripchart = subprocess.run(
            [
                "w32tm",
                "/stripchart",
                "/computer:time.windows.com",
                "/dataonly",
                f"/samples:{max(1, min(int(samples), 5))}",
            ],
            check=False,
            capture_output=True,
            text=True,
            timeout=20,
        )
        stripchart_text = stripchart.stdout + stripchart.stderr
        stripchart_exit_code: int | None = stripchart.returncode
    except (OSError, subprocess.TimeoutExpired) as exc:
        stripchart_text = ""
        stripchart_exit_code = None
        stripchart_error = f"{type(exc).__name__}: {exc}"
    parsed = parse_clock_outputs(
        status.stdout + status.stderr,
        stripchart_text,
    )
    result = {
        "applicable": True,
        "status_exit_code": status.returncode,
        "stripchart_exit_code": stripchart_exit_code,
        **parsed,
    }
    if stripchart_error:
        result["stripchart_error"] = stripchart_error
    return result


def _task_argument_tokens(arguments: str) -> list[str] | None:
    """Tokenize the deliberately constrained scheduled-task argument grammar."""

    tokens: list[str] = []
    token: list[str] = []
    inside_quotes = False
    token_started = False
    for character in arguments:
        if character == '"':
            inside_quotes = not inside_quotes
            token_started = True
            continue
        if character.isspace() and not inside_quotes:
            if token_started:
                tokens.append("".join(token))
                token = []
                token_started = False
            continue
        token.append(character)
        token_started = True
    if inside_quotes:
        return None
    if token_started:
        tokens.append("".join(token))
    return tokens


def _canonical_windows_path(value: object) -> str:
    raw = str(value or "").strip()
    if not raw or not ntpath.isabs(raw):
        return ""
    return ntpath.normcase(ntpath.normpath(raw)).rstrip("\\")


def _xml_text(parent: ET.Element | None, path: str) -> str:
    if parent is None:
        return ""
    node = parent.find(path, TASK_XML_NAMESPACE)
    return str(node.text or "").strip() if node is not None else ""


def _xml_bool(parent: ET.Element | None, path: str, *, default: bool = False) -> bool:
    value = _xml_text(parent, path)
    if not value:
        return default
    return value.casefold() == "true"


def _xml_local_name(node: ET.Element) -> str:
    return node.tag.rsplit("}", 1)[-1]


def _weekly_trigger_contract(
    trigger: ET.Element,
    *,
    expected_clock: tuple[int, int, int],
    expected_repetition: tuple[str, str, bool] | None,
) -> bool:
    expected_children = {"StartBoundary", "ScheduleByWeek"}
    if expected_repetition is not None:
        expected_children.add("Repetition")
    trigger_children = [_xml_local_name(node) for node in list(trigger)]
    if len(trigger_children) != len(expected_children) or set(trigger_children) != expected_children:
        return False
    try:
        boundary = datetime.fromisoformat(
            _xml_text(trigger, "task:StartBoundary").replace("Z", "+00:00")
        )
        actual_clock = (boundary.hour, boundary.minute, boundary.second)
    except ValueError:
        return False
    schedule = trigger.find("task:ScheduleByWeek", TASK_XML_NAMESPACE)
    days_node = (
        schedule.find("task:DaysOfWeek", TASK_XML_NAMESPACE)
        if schedule is not None
        else None
    )
    schedule_children = [_xml_local_name(node) for node in list(schedule or [])]
    day_nodes = list(days_node or [])
    days = {_xml_local_name(node) for node in day_nodes}
    weeks_interval = _xml_text(schedule, "task:WeeksInterval")
    repetition = trigger.find("task:Repetition", TASK_XML_NAMESPACE)
    if expected_repetition is None:
        repetition_valid = repetition is None
    else:
        interval, duration, stop_at_end = expected_repetition
        repetition_children = [
            _xml_local_name(node) for node in list(repetition or [])
        ]
        repetition_valid = bool(
            repetition is not None
            and len(repetition_children) == 3
            and set(repetition_children)
            == {"Interval", "Duration", "StopAtDurationEnd"}
            and _xml_text(repetition, "task:Interval") == interval
            and _xml_text(repetition, "task:Duration") == duration
            and _xml_bool(repetition, "task:StopAtDurationEnd") is stop_at_end
        )
    return bool(
        actual_clock == expected_clock
        and weeks_interval == "1"
        and len(schedule_children) == 2
        and set(schedule_children) == {"WeeksInterval", "DaysOfWeek"}
        and len(day_nodes) == 5
        and days == WEEKDAY_TRIGGER_NAMES
        and repetition_valid
    )


def _normalize_scheduled_task_contract(
    row: dict[str, Any], project_root: Path
) -> dict[str, Any]:
    name = str(row.get("name") or "")
    normalized: dict[str, Any] = {
        "present": row.get("present") is True,
        "state": str(row.get("state") or ""),
        "enabled": row.get("enabled") is True,
        "run_level": str(row.get("run_level") or ""),
        "last_run_time": row.get("last_run_time"),
        "last_result": row.get("last_result"),
        "discovered_task_paths": list(row.get("discovered_task_paths") or []),
    }
    if not normalized["present"]:
        return normalized

    reasons: list[str] = []
    try:
        root = ET.fromstring(str(row.get("xml") or ""))
    except ET.ParseError:
        normalized.update(
            {
                "task_path_valid": False,
                "principal_system_account": False,
                "logon_type_service_account": False,
                "action_count_valid": False,
                "action_shape_valid": False,
                "executable_matches_system32_powershell": False,
                "working_directory_matches_project": False,
                "arguments_match_contract": False,
                "trigger_contract_valid": False,
                "multiple_instances_ignore_new": False,
                "execution_limit_valid": False,
                "battery_policy_valid": False,
                "restart_policy_valid": False,
                "wake_to_run": False,
                "start_when_available": False,
                "contract_reasons": ["task_xml_invalid"],
            }
        )
        return normalized

    expected_uri = f"\\{name}"
    discovered_paths = list(row.get("discovered_task_paths") or [])
    task_path_valid = bool(
        str(row.get("task_path") or "") == "\\"
        and _xml_text(root, "task:RegistrationInfo/task:URI") == expected_uri
        and discovered_paths == ["\\"]
    )

    principal_nodes = root.findall("task:Principals/task:Principal", TASK_XML_NAMESPACE)
    principal = principal_nodes[0] if len(principal_nodes) == 1 else None
    expected_principal_sid = "S-1-5-18"
    reported_principal_sid = str(row.get("principal_sid") or "")
    principal_system_account = bool(
        principal is not None
        and _xml_text(principal, "task:UserId") == expected_principal_sid
        and reported_principal_sid == expected_principal_sid
    )
    xml_logon_type = _xml_text(principal, "task:LogonType")
    logon_type_service_account = bool(
        principal is not None
        and str(row.get("principal_logon_type") or "") == "ServiceAccount"
        # Windows omits LogonType from exported XML for LocalSystem even
        # though the ScheduledTasks CIM readback reports ServiceAccount.
        and xml_logon_type in {"", "ServiceAccount"}
    )

    actions = root.find("task:Actions", TASK_XML_NAMESPACE)
    action_nodes = list(actions or [])
    action_count_valid = bool(
        len(action_nodes) == 1 and _xml_local_name(action_nodes[0]) == "Exec"
    )
    action = action_nodes[0] if action_count_valid else None
    action_children = [_xml_local_name(node) for node in list(action or [])]
    action_shape_valid = bool(
        action is not None
        and len(action_children) == 3
        and set(action_children) == {"Command", "Arguments", "WorkingDirectory"}
        and actions is not None
        and principal is not None
        and str(actions.attrib.get("Context") or "")
        == str(principal.attrib.get("id") or "")
    )
    expected_powershell = Path(os.environ.get("SystemRoot", r"C:\Windows")) / (
        r"System32\WindowsPowerShell\v1.0\powershell.exe"
    )
    executable_matches = bool(
        action is not None
        and _canonical_windows_path(_xml_text(action, "task:Command"))
        == _canonical_windows_path(expected_powershell)
    )
    expected_root = _canonical_windows_path(project_root.resolve())
    working_directory_matches = bool(
        action is not None
        and _canonical_windows_path(_xml_text(action, "task:WorkingDirectory"))
        == expected_root
    )

    expected_script = _canonical_windows_path(
        (project_root / "start_market_day.ps1").resolve()
    )
    actual_tokens = (
        _task_argument_tokens(_xml_text(action, "task:Arguments"))
        if action is not None
        else None
    )
    expected_tail = ["-EnableRutCanary"]
    if name == WATCHDOG_TASK_NAME:
        expected_tail.append("-SkipClockSync")
    arguments_match = bool(actual_tokens is not None and len(actual_tokens) == 6 + len(expected_tail))
    if arguments_match and actual_tokens is not None:
        arguments_match = bool(
            [value.casefold() for value in actual_tokens[:5]]
            == [
                "-noprofile",
                "-noninteractive",
                "-executionpolicy",
                "bypass",
                "-file",
            ]
            and _canonical_windows_path(actual_tokens[5]) == expected_script
            and [value.casefold() for value in actual_tokens[6:]]
            == [value.casefold() for value in expected_tail]
        )

    triggers = root.find("task:Triggers", TASK_XML_NAMESPACE)
    trigger_nodes = list(triggers or [])
    current_sid = str(row.get("current_user_sid") or "")
    current_account = str(row.get("current_user_account") or "")
    if name == AUTOSTART_TASK_NAME:
        calendar_triggers = [
            node for node in trigger_nodes if _xml_local_name(node) == "CalendarTrigger"
        ]
        logon_triggers = [
            node for node in trigger_nodes if _xml_local_name(node) == "LogonTrigger"
        ]
        boot_triggers = [
            node for node in trigger_nodes if _xml_local_name(node) == "BootTrigger"
        ]
        actual_clocks: list[tuple[int, int, int]] = []
        calendars_valid = len(calendar_triggers) == 4
        for trigger in calendar_triggers:
            try:
                boundary = datetime.fromisoformat(
                    _xml_text(trigger, "task:StartBoundary").replace("Z", "+00:00")
                )
                clock = (boundary.hour, boundary.minute, boundary.second)
            except ValueError:
                clock = (-1, -1, -1)
            actual_clocks.append(clock)
            calendars_valid = calendars_valid and _weekly_trigger_contract(
                trigger,
                expected_clock=clock,
                expected_repetition=None,
            )
        expected_clocks = {(7, 0, 0), (7, 15, 0), (7, 45, 0), (8, 15, 0)}
        logon_user = (
            _xml_text(logon_triggers[0], "task:UserId")
            if len(logon_triggers) == 1
            else ""
        )
        logon_children = (
            [_xml_local_name(node) for node in list(logon_triggers[0])]
            if len(logon_triggers) == 1
            else []
        )
        logon_user_valid = bool(
            logon_user
            and logon_children == ["UserId"]
            and logon_user.casefold()
            in {current_sid.casefold(), current_account.casefold()}
        )
        boot_children = (
            [_xml_local_name(node) for node in list(boot_triggers[0])]
            if len(boot_triggers) == 1
            else []
        )
        boot_trigger_valid = bool(
            len(boot_triggers) == 1
            and len(boot_children) == len(set(boot_children))
            and set(boot_children).issubset({"Enabled"})
            and _xml_bool(boot_triggers[0], "task:Enabled", default=True)
        )
        trigger_contract_valid = bool(
            len(trigger_nodes) == 6
            and calendars_valid
            and set(actual_clocks) == expected_clocks
            and len(set(actual_clocks)) == 4
            and boot_trigger_valid
            and len(logon_triggers) == 1
            and logon_user_valid
        )
    elif name == WATCHDOG_TASK_NAME:
        trigger_contract_valid = bool(
            len(trigger_nodes) == 1
            and _xml_local_name(trigger_nodes[0]) == "CalendarTrigger"
            and _weekly_trigger_contract(
                trigger_nodes[0],
                expected_clock=(7, 50, 0),
                expected_repetition=("PT5M", "PT10H10M", True),
            )
        )
    else:
        trigger_contract_valid = False

    settings = root.find("task:Settings", TASK_XML_NAMESPACE)
    wake_to_run = _xml_bool(settings, "task:WakeToRun")
    start_when_available = _xml_bool(settings, "task:StartWhenAvailable")
    multiple_instances_ignore_new = (
        _xml_text(settings, "task:MultipleInstancesPolicy") == "IgnoreNew"
    )
    execution_limit_valid = (
        _xml_text(settings, "task:ExecutionTimeLimit") == "PT15M"
    )
    battery_policy_valid = bool(
        not _xml_bool(settings, "task:DisallowStartIfOnBatteries", default=True)
        and not _xml_bool(settings, "task:StopIfGoingOnBatteries", default=True)
    )
    restart = (
        settings.find("task:RestartOnFailure", TASK_XML_NAMESPACE)
        if settings is not None
        else None
    )
    if name == AUTOSTART_TASK_NAME:
        restart_policy_valid = bool(
            restart is not None
            and _xml_text(restart, "task:Count") == "3"
            and _xml_text(restart, "task:Interval") == "PT1M"
            and start_when_available
        )
    else:
        restart_policy_valid = bool(restart is None and not start_when_available)

    checks = {
        "task_path_valid": task_path_valid,
        "principal_system_account": principal_system_account,
        "logon_type_service_account": logon_type_service_account,
        "action_count_valid": action_count_valid,
        "action_shape_valid": action_shape_valid,
        "executable_matches_system32_powershell": executable_matches,
        "working_directory_matches_project": working_directory_matches,
        "arguments_match_contract": arguments_match,
        "trigger_contract_valid": trigger_contract_valid,
        "multiple_instances_ignore_new": multiple_instances_ignore_new,
        "execution_limit_valid": execution_limit_valid,
        "battery_policy_valid": battery_policy_valid,
        "restart_policy_valid": restart_policy_valid,
        "wake_to_run": wake_to_run,
        "start_when_available_policy_valid": (
            start_when_available
            if name == AUTOSTART_TASK_NAME
            else not start_when_available
        ),
    }
    for field, value in checks.items():
        if not value:
            reasons.append(field)
    normalized.update(checks)
    normalized["start_when_available"] = start_when_available
    normalized["launch_script_matches_project"] = arguments_match
    normalized["rut_canary_requested"] = arguments_match
    normalized["clock_sync_skipped"] = bool(
        arguments_match and name == WATCHDOG_TASK_NAME
    )
    normalized["contract_reasons"] = reasons
    return normalized


def inspect_scheduled_tasks(project_root: Path) -> dict[str, Any]:
    if sys.platform != "win32":
        return {"applicable": False, "tasks": {}}
    script = r"""
$ErrorActionPreference = 'Stop'
$identity = [Security.Principal.WindowsIdentity]::GetCurrent()
$rows = @()
foreach ($name in @('MarketPinPredictor_AutoStart','MarketPinPredictor_Watchdog')) {
    $matches = @(Get-ScheduledTask -TaskName $name -ErrorAction SilentlyContinue)
    $rootMatches = @($matches | Where-Object { [string]$_.TaskPath -ceq '\' })
    if ($rootMatches.Count -ne 1) {
        $rows += [pscustomobject]@{
            name = $name
            present = $false
            discovered_task_paths = @($matches | ForEach-Object { [string]$_.TaskPath })
        }
        continue
    }
    $task = $rootMatches[0]
    $info = Get-ScheduledTaskInfo -TaskName $name -TaskPath '\' -ErrorAction Stop
    $principalSid = try {
        if ([string]$task.Principal.UserId -match '^S-\d-(?:\d+-)+\d+$') {
            ([Security.Principal.SecurityIdentifier]::new([string]$task.Principal.UserId)).Value
        }
        else {
            ([Security.Principal.NTAccount]::new([string]$task.Principal.UserId)).Translate(
                [Security.Principal.SecurityIdentifier]
            ).Value
        }
    }
    catch { '' }
    $rows += [pscustomobject]@{
        name = $name
        present = $true
        task_path = [string]$task.TaskPath
        state = [string]$task.State
        enabled = [bool]$task.Settings.Enabled
        run_level = [string]$task.Principal.RunLevel
        principal_user_id = [string]$task.Principal.UserId
        principal_sid = [string]$principalSid
        principal_logon_type = [string]$task.Principal.LogonType
        last_run_time = $info.LastRunTime.ToString('o')
        last_result = [int64]$info.LastTaskResult
        current_user_sid = [string]$identity.User.Value
        current_user_account = [string]$identity.Name
        discovered_task_paths = @($matches | ForEach-Object { [string]$_.TaskPath })
        xml = [string](Export-ScheduledTask -TaskName $name -TaskPath '\' -ErrorAction Stop)
    }
}
$rows | ConvertTo-Json -Depth 6 -Compress
"""
    try:
        result = subprocess.run(
            ["powershell.exe", "-NoProfile", "-NonInteractive", "-Command", script],
            check=False,
            capture_output=True,
            text=True,
            timeout=15,
        )
        if result.returncode != 0:
            raise RuntimeError(result.stderr.strip() or f"exit_code={result.returncode}")
        raw = json.loads(result.stdout)
    except (OSError, subprocess.TimeoutExpired, json.JSONDecodeError, RuntimeError) as exc:
        return {
            "applicable": True,
            "error": f"{type(exc).__name__}: {exc}",
            "tasks": {},
        }
    rows = raw if isinstance(raw, list) else [raw]
    tasks: dict[str, dict[str, Any]] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        name = str(row.get("name") or "")
        tasks[name] = _normalize_scheduled_task_contract(row, project_root)
    return {
        "applicable": True,
        "query_exit_code": result.returncode,
        "tasks": tasks,
    }


def inspect_database(
    path: Path,
    trading_day: date,
    *,
    observed_at_ct: datetime | None = None,
) -> dict[str, Any]:
    if not path.is_file():
        return {"present": False, "path": str(path)}
    connection: sqlite3.Connection | None = None
    try:
        connection = sqlite3.connect(
            f"file:{path.resolve().as_posix()}?mode=ro",
            uri=True,
            timeout=5.0,
        )
        connection.row_factory = sqlite3.Row
        with connection:
            journal_mode = str(connection.execute("PRAGMA journal_mode").fetchone()[0])
            quick_check = str(connection.execute("PRAGMA quick_check").fetchone()[0])
            table_names = {
                str(row[0])
                for row in connection.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                )
            }
            structure_columns: set[str] = set()
            if "market_structure_observations" in table_names:
                structure_columns = {
                    str(row[1])
                    for row in connection.execute(
                        "PRAGMA table_info(market_structure_observations)"
                    )
                }
            structure_trigger_names = {
                str(row[0])
                for row in connection.execute(
                    "SELECT name FROM sqlite_master "
                    "WHERE type='trigger' AND tbl_name='market_structure_observations'"
                )
            }
            orb_trigger_names = {
                str(row[0])
                for row in connection.execute(
                    "SELECT name FROM sqlite_master "
                    "WHERE type='trigger' AND tbl_name='orb_reference_samples'"
                )
            }
            orb_index_names = {
                str(row[0])
                for row in connection.execute(
                    "SELECT name FROM sqlite_master "
                    "WHERE type='index' AND tbl_name='orb_reference_samples'"
                )
            }
            orb_decision_trigger_names = {
                str(row[0])
                for row in connection.execute(
                    "SELECT name FROM sqlite_master "
                    "WHERE type='trigger' "
                    "AND tbl_name='orb_reference_sample_decisions'"
                )
            }
            orb_decision_index_names = {
                str(row[0])
                for row in connection.execute(
                    "SELECT name FROM sqlite_master "
                    "WHERE type='index' "
                    "AND tbl_name='orb_reference_sample_decisions'"
                )
            }
            observed = observed_at_ct or datetime.now(CT)
            observed_utc = observed.astimezone(UTC)
            first_eligible_pair = (
                _first_eligible_calculation_bound_pair_evidence(
                    connection,
                    table_names=table_names,
                    structure_columns=structure_columns,
                    trading_day=trading_day,
                    observed_utc=observed_utc,
                )
                if "market_structure_observations" in table_names
                else {}
            )
            rows: dict[str, dict[str, Any]] = {}
            if "market_structure_observations" in table_names:
                for row in connection.execute(
                    "SELECT symbol, COUNT(*) AS row_count, "
                    "MAX(source_timestamp_utc) AS latest_source_timestamp_utc "
                    "FROM market_structure_observations WHERE trading_date=? "
                    "GROUP BY symbol ORDER BY symbol",
                    (trading_day.isoformat(),),
                ):
                    latest_source_timestamp = row["latest_source_timestamp_utc"]
                    latest_source = _parse_utc(latest_source_timestamp)
                    latest_fields = (
                        "source_timestamp_utc",
                        "captured_at_utc",
                        "provider",
                        "subscription_epoch_id",
                        "subscription_generation",
                        "calculation_id",
                        "reference_price",
                        "gamma_pin",
                        "max_pain",
                        "primary_expiration",
                        "same_day_profile_available",
                        "universe_sha256",
                        "validation_status",
                    )
                    select_fields = ", ".join(
                        field
                        if field in structure_columns
                        else f"NULL AS {field}"
                        for field in latest_fields
                    )
                    order_fields = ["source_timestamp_utc DESC"]
                    if "captured_at_utc" in structure_columns:
                        order_fields.append("captured_at_utc DESC")
                    if "observation_id" in structure_columns:
                        order_fields.append("observation_id DESC")
                    latest = connection.execute(
                        f"SELECT {select_fields} "
                        "FROM market_structure_observations "
                        "WHERE symbol=? AND trading_date=? "
                        f"ORDER BY {', '.join(order_fields)} LIMIT 1",
                        (str(row["symbol"]), trading_day.isoformat()),
                    ).fetchone()
                    calculation_bound = None
                    calculation_bound_count = 0
                    bound_identity_columns = {
                        "provider",
                        "subscription_epoch_id",
                        "subscription_generation",
                        "calculation_id",
                        "primary_expiration",
                        "same_day_profile_available",
                        "universe_sha256",
                        "validation_status",
                    }
                    if (
                        latest is not None
                        and bound_identity_columns.issubset(structure_columns)
                    ):
                        identity_predicate = (
                            "provider IS ? AND subscription_epoch_id IS ? AND "
                            "subscription_generation IS ? AND universe_sha256 IS ? AND "
                            "primary_expiration IS ? AND same_day_profile_available IS ?"
                        )
                        identity_parameters = (
                            latest["provider"],
                            latest["subscription_epoch_id"],
                            latest["subscription_generation"],
                            latest["universe_sha256"],
                            latest["primary_expiration"],
                            latest["same_day_profile_available"],
                        )
                        calculation_bound_count = int(
                            connection.execute(
                                "SELECT COUNT(*) FROM market_structure_observations "
                                "WHERE symbol=? AND trading_date=? AND "
                                "calculation_id IS NOT NULL AND TRIM(calculation_id) <> '' "
                                "AND validation_status='valid' AND "
                                + identity_predicate,
                                (
                                    str(row["symbol"]),
                                    trading_day.isoformat(),
                                    *identity_parameters,
                                ),
                            ).fetchone()[0]
                        )
                        calculation_bound = connection.execute(
                            f"SELECT {select_fields} "
                            "FROM market_structure_observations "
                            "WHERE symbol=? AND trading_date=? AND "
                            "calculation_id IS NOT NULL AND TRIM(calculation_id) <> '' "
                            "AND validation_status='valid' AND "
                            + identity_predicate
                            + f" ORDER BY {', '.join(order_fields)} LIMIT 1",
                            (
                                str(row["symbol"]),
                                trading_day.isoformat(),
                                *identity_parameters,
                            ),
                        ).fetchone()
                    latest_capture = _parse_utc(
                        latest["captured_at_utc"] if latest is not None else None
                    )
                    bound_source = _parse_utc(
                        calculation_bound["source_timestamp_utc"]
                        if calculation_bound is not None
                        else None
                    )
                    bound_capture = _parse_utc(
                        calculation_bound["captured_at_utc"]
                        if calculation_bound is not None
                        else None
                    )
                    calculation_bound_evidence = (
                        {
                            "row_count": calculation_bound_count,
                            "source_timestamp_utc": _utc_iso(
                                calculation_bound["source_timestamp_utc"]
                            ),
                            "source_age_seconds": (
                                (observed_utc - bound_source).total_seconds()
                                if bound_source is not None
                                else None
                            ),
                            "captured_at_utc": _utc_iso(
                                calculation_bound["captured_at_utc"]
                            ),
                            "capture_age_seconds": (
                                (observed_utc - bound_capture).total_seconds()
                                if bound_capture is not None
                                else None
                            ),
                            "lag_from_latest_source_seconds": (
                                (latest_source - bound_source).total_seconds()
                                if latest_source is not None and bound_source is not None
                                else None
                            ),
                            "provider": calculation_bound["provider"],
                            "subscription_epoch_id": _canonical_subscription_epoch(
                                calculation_bound["subscription_epoch_id"]
                            ),
                            "subscription_generation": calculation_bound[
                                "subscription_generation"
                            ],
                            "calculation_id": calculation_bound["calculation_id"],
                            "reference_price": calculation_bound["reference_price"],
                            "gamma_pin": calculation_bound["gamma_pin"],
                            "max_pain": calculation_bound["max_pain"],
                            "primary_expiration": calculation_bound[
                                "primary_expiration"
                            ],
                            "same_day_profile_available": (
                                bool(
                                    calculation_bound[
                                        "same_day_profile_available"
                                    ]
                                )
                                if calculation_bound[
                                    "same_day_profile_available"
                                ]
                                is not None
                                else None
                            ),
                            "universe_sha256": calculation_bound["universe_sha256"],
                            "validation_status": calculation_bound[
                                "validation_status"
                            ],
                            "lineage": (
                                _calculation_lineage_evidence(
                                    connection,
                                    table_names=table_names,
                                    structure=calculation_bound,
                                    symbol=str(row["symbol"]),
                                    trading_day=trading_day,
                                    observed_utc=observed_utc,
                                )
                                if str(row["symbol"]) in CORE_SYMBOLS
                                else {
                                    "status": "not_required",
                                    "reason": None,
                                    "calculation_id": calculation_bound[
                                        "calculation_id"
                                    ],
                                }
                            ),
                        }
                        if calculation_bound is not None
                        else {
                            "row_count": calculation_bound_count,
                            "lineage": {
                                "status": "invalid",
                                "reason": "CALCULATION_BOUND_STRUCTURE_MISSING",
                                "calculation_id": None,
                                "gamma_run_present": False,
                                "input_blob_present": False,
                                "payload_integrity_verified": False,
                                "payload_sha256": None,
                                "run_calculated_at_utc": None,
                                "run_age_seconds": None,
                            },
                        }
                    )
                    first_eligible_calculation_bound = (
                        first_eligible_pair.get(str(row["symbol"]))
                        if str(row["symbol"]) in CORE_SYMBOLS
                        else None
                    )
                    rows[str(row["symbol"])] = {
                        "row_count": int(row["row_count"]),
                        "latest_source_timestamp_utc": latest_source_timestamp,
                        "latest_source_age_seconds": (
                            (observed_utc - latest_source).total_seconds()
                            if latest_source is not None
                            else None
                        ),
                        "latest_captured_at_utc": (
                            latest["captured_at_utc"]
                            if latest is not None
                            else None
                        ),
                        "latest_capture_age_seconds": (
                            (observed_utc - latest_capture).total_seconds()
                            if latest_capture is not None
                            else None
                        ),
                        "latest_provider": (
                            latest["provider"] if latest is not None else None
                        ),
                        "latest_subscription_epoch_id": (
                            _canonical_subscription_epoch(
                                latest["subscription_epoch_id"]
                            )
                            if latest is not None
                            else None
                        ),
                        "latest_subscription_generation": (
                            latest["subscription_generation"]
                            if latest is not None
                            else None
                        ),
                        "latest_calculation_id": (
                            latest["calculation_id"] if latest is not None else None
                        ),
                        "latest_reference_price": (
                            latest["reference_price"] if latest is not None else None
                        ),
                        "latest_gamma_pin": (
                            latest["gamma_pin"] if latest is not None else None
                        ),
                        "latest_max_pain": (
                            latest["max_pain"] if latest is not None else None
                        ),
                        "latest_primary_expiration": (
                            latest["primary_expiration"] if latest is not None else None
                        ),
                        "latest_same_day_profile_available": (
                            bool(latest["same_day_profile_available"])
                            if latest is not None
                            and latest["same_day_profile_available"] is not None
                            else None
                        ),
                        "latest_universe_sha256": (
                            latest["universe_sha256"] if latest is not None else None
                        ),
                        "latest_validation_status": (
                            latest["validation_status"] if latest is not None else None
                        ),
                        "latest_calculation_bound": calculation_bound_evidence,
                        "first_eligible_calculation_bound": (
                            first_eligible_calculation_bound
                        ),
                    }
            orb_columns: set[str] = set()
            orb_decision_columns: set[str] = set()
            orb_rows: list[sqlite3.Row] = []
            orb_decision_rows: dict[str, dict[str, int]] = {}
            if "orb_reference_samples" in table_names:
                orb_columns = {
                    str(row[1])
                    for row in connection.execute(
                        "PRAGMA table_info(orb_reference_samples)"
                    )
                }
                if "orb_reference_sample_decisions" in table_names:
                    orb_decision_columns = {
                        str(row[1])
                        for row in connection.execute(
                            "PRAGMA table_info(orb_reference_sample_decisions)"
                        )
                    }
                decision_contract_ready = (
                    EXPECTED_ORB_REFERENCE_DECISION_COLUMNS.issubset(
                        orb_decision_columns
                    )
                )
                if decision_contract_ready:
                    for row in connection.execute(
                        "SELECT sample.symbol AS symbol, "
                        "COUNT(*) AS raw_count, "
                        "SUM(CASE WHEN decision.sample_id IS NULL THEN 1 ELSE 0 END) "
                        "AS pending_count, "
                        "SUM(CASE WHEN decision.progress_eligible = 0 THEN 1 ELSE 0 END) "
                        "AS ineligible_count, "
                        "SUM(CASE WHEN decision.progress_eligible = 1 THEN 1 ELSE 0 END) "
                        "AS eligible_count "
                        "FROM orb_reference_samples AS sample "
                        "LEFT JOIN orb_reference_sample_decisions AS decision "
                        "ON decision.sample_id = sample.sample_id "
                        "WHERE sample.trading_date=? GROUP BY sample.symbol",
                        (trading_day.isoformat(),),
                    ):
                        orb_decision_rows[str(row["symbol"])] = {
                            "raw_row_count": int(row["raw_count"] or 0),
                            "pending_decision_count": int(row["pending_count"] or 0),
                            "ineligible_decision_count": int(
                                row["ineligible_count"] or 0
                            ),
                            "eligible_decision_count": int(row["eligible_count"] or 0),
                        }
                if {
                    "symbol",
                    "trading_date",
                    "sample_timestamp_utc",
                    "source_timestamp_utc",
                    "captured_at_utc",
                }.issubset(orb_columns) and decision_contract_ready:
                    orb_rows = list(
                        connection.execute(
                            "SELECT sample.symbol, sample.sample_timestamp_utc, "
                            "sample.source_timestamp_utc, sample.captured_at_utc, "
                            + (
                                "sample.subscription_epoch_id "
                                if "subscription_epoch_id" in orb_columns
                                else "NULL AS subscription_epoch_id "
                            )
                            + ", "
                            + (
                                "sample.subscription_generation "
                                if "subscription_generation" in orb_columns
                                else "NULL AS subscription_generation "
                            )
                            + ", "
                            + (
                                "sample.provider "
                                if "provider" in orb_columns
                                else "NULL AS provider "
                            )
                            + ", "
                            + (
                                "sample.validation_status "
                                if "validation_status" in orb_columns
                                else "NULL AS validation_status "
                            )
                            + ", "
                            + (
                                "sample.primary_expiration "
                                if "primary_expiration" in orb_columns
                                else "NULL AS primary_expiration "
                            )
                            + ", "
                            + (
                                "sample.same_day_profile_available "
                                if "same_day_profile_available" in orb_columns
                                else "NULL AS same_day_profile_available "
                            )
                            + "FROM orb_reference_samples AS sample "
                            + "JOIN orb_reference_sample_decisions AS decision "
                            + "ON decision.sample_id = sample.sample_id "
                            + "WHERE sample.trading_date=? "
                            + "AND decision.decision_status='final' "
                            + "AND decision.progress_eligible=1 "
                            + "AND decision.sample_timestamp_utc="
                            + "sample.sample_timestamp_utc "
                            + "AND decision.intended_bucket_utc="
                            + "sample.sample_timestamp_utc "
                            + "AND CAST(strftime('%s', "
                            + "decision.intended_bucket_utc) AS INTEGER) "
                            + f"% {ORB_REFERENCE_CADENCE_SECONDS}=0 "
                            + "AND (instr(CAST(decision.intended_bucket_utc "
                            + "AS TEXT), '.')=0 OR CAST("
                            + "decision.intended_bucket_utc AS TEXT) "
                            + "NOT GLOB '*.[0-9]*[1-9]*') "
                            + "AND julianday(decision.attempt_completed_at_utc) "
                            + ">= julianday(sample.captured_at_utc) "
                            + "AND julianday(decision.attempt_completed_at_utc) "
                            + ">= julianday(decision.intended_bucket_utc) "
                            + "AND julianday(decision.attempt_completed_at_utc) "
                            + "< julianday(decision.intended_bucket_utc, "
                            + f"'+{ORB_REFERENCE_CADENCE_SECONDS} seconds') "
                            + "ORDER BY sample.symbol, sample.sample_timestamp_utc, "
                            + "sample.captured_at_utc",
                            (trading_day.isoformat(),),
                        )
                    )
            observed = observed_at_ct or datetime.now(CT)
            return {
                "present": True,
                "path": str(path.resolve()),
                "journal_mode": journal_mode,
                "quick_check": quick_check,
                "market_structure_table_present": (
                    "market_structure_observations" in table_names
                ),
                "market_structure_evidence_role": "gex_pin_max_pain_only",
                "market_structure_columns": sorted(structure_columns),
                "missing_market_structure_columns": sorted(
                    EXPECTED_MARKET_STRUCTURE_COLUMNS.difference(structure_columns)
                ),
                "market_structure_triggers": sorted(structure_trigger_names),
                "missing_market_structure_triggers": sorted(
                    EXPECTED_MARKET_STRUCTURE_TRIGGERS.difference(
                        structure_trigger_names
                    )
                ),
                "market_structure_rows": rows,
                "orb_reference_table_present": (
                    "orb_reference_samples" in table_names
                ),
                "orb_reference_evidence_role": "opening_ranges_only",
                "orb_reference_columns": sorted(orb_columns),
                "missing_orb_reference_columns": sorted(
                    EXPECTED_ORB_REFERENCE_COLUMNS.difference(orb_columns)
                ),
                "orb_reference_triggers": sorted(orb_trigger_names),
                "missing_orb_reference_triggers": sorted(
                    EXPECTED_ORB_REFERENCE_TRIGGERS.difference(
                        orb_trigger_names
                    )
                ),
                "orb_reference_indexes": sorted(orb_index_names),
                "missing_orb_reference_indexes": sorted(
                    EXPECTED_ORB_REFERENCE_INDEXES.difference(orb_index_names)
                ),
                "orb_reference_rows": _orb_reference_evidence(
                    orb_rows,
                    trading_day=trading_day,
                    observed_at_ct=observed,
                ),
                "orb_reference_decision_table_present": (
                    "orb_reference_sample_decisions" in table_names
                ),
                "orb_reference_decision_columns": sorted(orb_decision_columns),
                "missing_orb_reference_decision_columns": sorted(
                    EXPECTED_ORB_REFERENCE_DECISION_COLUMNS.difference(
                        orb_decision_columns
                    )
                ),
                "orb_reference_decision_triggers": sorted(
                    orb_decision_trigger_names
                ),
                "missing_orb_reference_decision_triggers": sorted(
                    EXPECTED_ORB_REFERENCE_DECISION_TRIGGERS.difference(
                        orb_decision_trigger_names
                    )
                ),
                "orb_reference_decision_indexes": sorted(
                    orb_decision_index_names
                ),
                "missing_orb_reference_decision_indexes": sorted(
                    EXPECTED_ORB_REFERENCE_DECISION_INDEXES.difference(
                        orb_decision_index_names
                    )
                ),
                "orb_reference_progress_decisions": orb_decision_rows,
            }
    except sqlite3.Error as exc:
        return {
            "present": True,
            "path": str(path.resolve()),
            "error": f"{type(exc).__name__}: {exc}",
        }
    finally:
        if connection is not None:
            connection.close()


def _due_windows(now_ct: datetime) -> tuple[str, ...]:
    local_time = now_ct.time()
    return tuple(
        name
        for boundary, name in (
            (time(8, 35), "5m"),
            (time(8, 45), "15m"),
            (time(9, 0), "30m"),
            (time(9, 30), "60m"),
        )
        if local_time >= boundary
    )


def _due_window(now_ct: datetime) -> str | None:
    """Compatibility alias for the latest already-due opening window."""
    due = _due_windows(now_ct)
    return due[-1] if due else None


def _task_ran_since(task: dict[str, Any], now_ct: datetime, boundary: time) -> bool:
    value = task.get("last_run_time")
    if not value:
        return False
    try:
        observed = datetime.fromisoformat(str(value))
    except ValueError:
        return False
    if observed.tzinfo is None:
        observed = observed.replace(tzinfo=CT)
    observed_ct = observed.astimezone(CT)
    return observed_ct.date() == now_ct.date() and observed_ct.time() >= boundary


def evaluate(
    *,
    now_ct: datetime,
    clock: dict[str, Any],
    health: dict[str, Any],
    live: dict[str, Any],
    dashboard: dict[str, Any],
    orb: dict[str, Any],
    database: dict[str, Any],
    scheduled_tasks: dict[str, Any] | None = None,
    project_root: Path = PROJECT_ROOT,
) -> tuple[str, list[str], list[str]]:
    issues: list[str] = []
    warnings: list[str] = []
    market_day = not is_weekend(now_ct.date()) and not is_holiday(now_ct.date())
    session_close = time(12, 0) if is_early_close_day(now_ct.date()) else time(15, 0)
    startup_warming = (
        market_day
        and time(7, 45) <= now_ct.time() < STARTUP_ACCEPTANCE_GRACE_END_CT
    )
    after_start = market_day and now_ct.time() >= STARTUP_ACCEPTANCE_GRACE_END_CT
    regular_session = market_day and time(8, 30) <= now_ct.time() < session_close
    health_payload = health.get("payload") or {}
    orb_payload = orb.get("payload") or {}
    requested = {
        str(symbol).strip().upper()
        for symbol in health_payload.get("symbols_requested") or []
        if str(symbol).strip()
    }
    configured = {
        str(symbol).strip().upper()
        for symbol in orb_payload.get("configured_symbols") or []
        if str(symbol).strip()
    }
    orb_requested = {
        str(symbol).strip().upper()
        for symbol in orb_payload.get("requested_symbols") or []
        if str(symbol).strip()
    }
    capture_symbols = sorted(
        set(CORE_SYMBOLS).union(requested, configured, orb_requested)
    )
    # Pre-open warming and post-close suppression are valid lifecycle states.
    # Exact live identity/handoff becomes mandatory only once cash trading begins.
    runtime_identity_required = regular_session
    active_generation: int | None = None

    if startup_warming:
        warnings.append("STARTUP_ACCEPTANCE_WARMING")

    task_report = scheduled_tasks or {"applicable": False, "tasks": {}}
    if task_report.get("applicable"):
        if task_report.get("error"):
            issues.append("SCHEDULED_TASK_INSPECTION_FAILED")
        tasks = task_report.get("tasks") or {}
        for name in (AUTOSTART_TASK_NAME, WATCHDOG_TASK_NAME):
            task = tasks.get(name) or {}
            if task.get("present") is not True:
                issues.append(f"SCHEDULED_TASK_MISSING:{name}")
                continue
            if task.get("enabled") is not True:
                issues.append(f"SCHEDULED_TASK_DISABLED:{name}")
            if task.get("wake_to_run") is not True:
                issues.append(f"SCHEDULED_TASK_WAKE_DISABLED:{name}")
            if task.get("task_path_valid") is not True:
                issues.append(f"SCHEDULED_TASK_PATH_INVALID:{name}")
            if task.get("principal_system_account") is not True:
                issues.append(f"SCHEDULED_TASK_PRINCIPAL_INVALID:{name}")
            if task.get("logon_type_service_account") is not True:
                issues.append(f"SCHEDULED_TASK_LOGON_INVALID:{name}")
            if task.get("action_count_valid") is not True:
                issues.append(f"SCHEDULED_TASK_ACTION_COUNT_INVALID:{name}")
            if task.get("action_shape_valid") is not True:
                issues.append(f"SCHEDULED_TASK_ACTION_SHAPE_INVALID:{name}")
            if task.get("executable_matches_system32_powershell") is not True:
                issues.append(f"SCHEDULED_TASK_EXECUTABLE_INVALID:{name}")
            if task.get("working_directory_matches_project") is not True:
                issues.append(f"SCHEDULED_TASK_WORKDIR_INVALID:{name}")
            if task.get("arguments_match_contract") is not True:
                issues.append(f"SCHEDULED_TASK_ARGUMENTS_INVALID:{name}")
            if task.get("trigger_contract_valid") is not True:
                issues.append(f"SCHEDULED_TASK_TRIGGER_INVALID:{name}")
            if task.get("multiple_instances_ignore_new") is not True:
                issues.append(f"SCHEDULED_TASK_MULTIPLE_INSTANCES_INVALID:{name}")
            if task.get("execution_limit_valid") is not True:
                issues.append(f"SCHEDULED_TASK_EXECUTION_LIMIT_INVALID:{name}")
            if task.get("battery_policy_valid") is not True:
                issues.append(f"SCHEDULED_TASK_BATTERY_POLICY_INVALID:{name}")
            if task.get("restart_policy_valid") is not True:
                issues.append(f"SCHEDULED_TASK_RETRY_POLICY_INVALID:{name}")
            if task.get("start_when_available_policy_valid") is not True:
                issues.append(f"SCHEDULED_TASK_START_AVAILABLE_INVALID:{name}")

        startup = tasks.get(AUTOSTART_TASK_NAME) or {}
        if startup.get("present") is True:
            if str(startup.get("run_level") or "").lower() != "highest":
                issues.append("AUTOSTART_RUN_LEVEL_NOT_HIGHEST")
            if startup.get("start_when_available") is not True:
                issues.append("AUTOSTART_START_WHEN_AVAILABLE_DISABLED")
            if startup.get("clock_sync_skipped") is True:
                issues.append("AUTOSTART_CLOCK_SYNC_SKIPPED")
            if now_ct.weekday() < 5 and now_ct.time() >= time(7, 47):
                startup_ran = _task_ran_since(startup, now_ct, time(7, 44))
                if not startup_ran:
                    issues.append("AUTOSTART_DID_NOT_RUN_TODAY")
                elif str(startup.get("state") or "").casefold() != "running":
                    try:
                        startup_result = int(startup.get("last_result"))
                    except (TypeError, ValueError):
                        startup_result = None
                    if startup_result != 0:
                        issues.append(
                            "SCHEDULED_TASK_LAST_RUN_FAILED:"
                            f"{AUTOSTART_TASK_NAME}:"
                            f"{startup_result if startup_result is not None else 'unknown'}"
                        )

        watchdog = tasks.get(WATCHDOG_TASK_NAME) or {}
        if watchdog.get("present") is True:
            if str(watchdog.get("run_level") or "").lower() != "highest":
                issues.append("WATCHDOG_RUN_LEVEL_NOT_HIGHEST")
            if watchdog.get("clock_sync_skipped") is not True:
                issues.append("WATCHDOG_CLOCK_SYNC_NOT_SKIPPED")
            if now_ct.weekday() < 5 and now_ct.time() >= time(7, 52):
                watchdog_ran = _task_ran_since(watchdog, now_ct, time(7, 49))
                if not watchdog_ran:
                    issues.append("WATCHDOG_DID_NOT_RUN_TODAY")
                elif str(watchdog.get("state") or "").casefold() != "running":
                    try:
                        watchdog_result = int(watchdog.get("last_result"))
                    except (TypeError, ValueError):
                        watchdog_result = None
                    if watchdog_result != 0:
                        issues.append(
                            "SCHEDULED_TASK_LAST_RUN_FAILED:"
                            f"{WATCHDOG_TASK_NAME}:"
                            f"{watchdog_result if watchdog_result is not None else 'unknown'}"
                        )

    if clock.get("applicable"):
        if clock.get("synchronized") is False:
            issues.append("PROCESSING_CLOCK_NOT_SYNCHRONIZED")
        elif clock.get("synchronized") is not True:
            warnings.append("PROCESSING_CLOCK_EXTERNAL_OFFSET_UNAVAILABLE")
    if not database.get("present") or database.get("error"):
        issues.append("LIVE_DATABASE_UNAVAILABLE")
    else:
        if str(database.get("journal_mode")).lower() != "wal":
            issues.append("LIVE_DATABASE_NOT_WAL")
        if str(database.get("quick_check")).lower() != "ok":
            issues.append("LIVE_DATABASE_QUICK_CHECK_FAILED")
        if database.get("market_structure_table_present") is not True:
            issues.append("MARKET_STRUCTURE_TABLE_MISSING")
        if database.get("missing_market_structure_columns"):
            issues.append("MARKET_STRUCTURE_SCHEMA_INCOMPATIBLE")
        if database.get("missing_market_structure_triggers"):
            issues.append("MARKET_STRUCTURE_IMMUTABILITY_TRIGGERS_MISSING")
        if database.get("orb_reference_table_present") is not True:
            issues.append("ORB_REFERENCE_TABLE_MISSING")
        if database.get("missing_orb_reference_columns"):
            issues.append("ORB_REFERENCE_SCHEMA_INCOMPATIBLE")
        if database.get("missing_orb_reference_triggers"):
            issues.append("ORB_REFERENCE_IMMUTABILITY_TRIGGERS_MISSING")
        if database.get("missing_orb_reference_indexes"):
            issues.append("ORB_REFERENCE_IDENTITY_INDEX_MISSING")
        if database.get("orb_reference_decision_table_present") is not True:
            issues.append("ORB_REFERENCE_DECISION_TABLE_MISSING")
        if database.get("missing_orb_reference_decision_columns"):
            issues.append("ORB_REFERENCE_DECISION_SCHEMA_INCOMPATIBLE")
        if database.get("missing_orb_reference_decision_triggers"):
            issues.append("ORB_REFERENCE_DECISION_IMMUTABILITY_TRIGGERS_MISSING")
        if database.get("missing_orb_reference_decision_indexes"):
            issues.append("ORB_REFERENCE_DECISION_IDENTITY_INDEX_MISSING")

    if after_start:
        if not health.get("ok"):
            issues.append("BACKEND_HEALTH_UNREACHABLE")
        else:
            try:
                _validate_loaded_code_fingerprint(
                    health_payload,
                    observed_utc=now_ct.astimezone(UTC),
                    session_date=now_ct.date().isoformat(),
                    project_root=project_root,
                )
            except MonitorOpeningAcceptanceError as exc:
                issues.append(f"LOADED_CODE_FINGERPRINT_INVALID:{exc}")
        if not live.get("ok"):
            issues.append("BACKEND_LIVE_HEALTH_UNREACHABLE")
        if not dashboard.get("ok"):
            issues.append("DASHBOARD_HEALTH_UNREACHABLE")
        active_epoch_id = _canonical_subscription_epoch(
            health_payload.get("subscription_epoch_id")
        )
        if active_epoch_id is None:
            issues.append("SUBSCRIPTION_EPOCH_INVALID_OR_MISSING")
        if runtime_identity_required:
            active_generation = _canonical_subscription_generation(
                health_payload.get("active_generation")
            )
            if active_generation is None:
                issues.append("ACTIVE_GENERATION_INVALID_OR_MISSING")
            if str(health_payload.get("handoff_status") or "").lower() != "active":
                issues.append("HEALTH_HANDOFF_NOT_ACTIVE")

            live_payload = live.get("payload") or {}
            live_generation = _canonical_subscription_generation(
                live_payload.get("subscription_generation")
            )
            live_active_generation = _canonical_subscription_generation(
                live_payload.get("active_generation")
            )
            if live_generation != active_generation:
                issues.append("LIVE_HEALTH_SUBSCRIPTION_GENERATION_MISMATCH")
            if live_active_generation != active_generation:
                issues.append("LIVE_HEALTH_ACTIVE_GENERATION_MISMATCH")
            if str(live_payload.get("handoff_status") or "").lower() != "active":
                issues.append("LIVE_HEALTH_HANDOFF_NOT_ACTIVE")
        issues.extend(_subscription_staging_issues(health_payload, requested))
        sleep = (health_payload.get("runtime_controls") or {}).get(
            "sleep_prevention"
        ) or {}
        if sleep.get("requested") is True and sleep.get("active") is not True:
            issues.append("BACKEND_SLEEP_PREVENTION_INACTIVE")
        sampler = health_payload.get("orb_reference_sampler") or {}
        if sampler.get("thread_alive") is not True:
            issues.append("ORB_REFERENCE_SAMPLER_THREAD_NOT_ALIVE")
        try:
            sampler_interval = int(sampler.get("interval_seconds"))
        except (TypeError, ValueError):
            sampler_interval = None
        if sampler_interval != ORB_REFERENCE_CADENCE_SECONDS:
            issues.append("ORB_REFERENCE_SAMPLER_CADENCE_INVALID")

        if "RUT" in requested:
            subscriptions_expected = bool(
                health_payload.get("subscription_allowed") is True
                or regular_session
            )
            bounds = health_payload.get("subscription_bounds") or {}
            try:
                max_contracts = int(bounds.get("max_subscription_contracts"))
            except (TypeError, ValueError):
                max_contracts = None
            if max_contracts != RUT_CANARY_MAX_SUBSCRIPTION_CONTRACTS:
                issues.append("RUT_CANARY_SUBSCRIPTION_CAP_INVALID")

            if subscriptions_expected:
                try:
                    subscribed_contracts = int(
                        health_payload.get("symbols_subscribed")
                    )
                except (TypeError, ValueError):
                    subscribed_contracts = None
                if subscribed_contracts is None or subscribed_contracts <= 0:
                    issues.append("RUT_CANARY_SUBSCRIPTION_COUNT_UNAVAILABLE")
                elif (
                    max_contracts is not None
                    and subscribed_contracts > max_contracts
                ):
                    issues.append("RUT_CANARY_SUBSCRIPTION_CAP_EXCEEDED")

                root_counts = health_payload.get("root_contract_counts") or {}
                family_status = health_payload.get("core_symbol_status") or {}
                for symbol in OPENING_SYMBOLS:
                    aliases = OPTION_ROOT_ALIASES.get(symbol, (symbol,))
                    raw_alias_counts: list[int] = []
                    for alias in aliases:
                        try:
                            raw_alias_counts.append(int(root_counts.get(alias) or 0))
                        except (TypeError, ValueError):
                            raw_alias_counts.append(0)
                    alias_count = sum(max(0, count) for count in raw_alias_counts)
                    try:
                        reported_family_count = int(
                            (family_status.get(symbol) or {}).get(
                                "contracts_subscribed"
                            )
                            or 0
                        )
                    except (TypeError, ValueError):
                        reported_family_count = 0
                    if max(alias_count, reported_family_count) <= 0:
                        issues.append(f"SUBSCRIPTION_ROOT_MISSING:{symbol}")

                metadata = health_payload.get("subscription_metadata") or {}
                try:
                    reservation_shortfall = int(
                        metadata.get("reservation_shortfall_pairs")
                    )
                except (TypeError, ValueError):
                    reservation_shortfall = None
                if reservation_shortfall is None:
                    issues.append("RUT_RESERVATION_SHORTFALL_UNREPORTED")
                elif reservation_shortfall != 0:
                    issues.append("RUT_RESERVATION_SHORTFALL_NONZERO")

            canary = health_payload.get("optional_family_canary") or {}
            canary_state = str(canary.get("state") or "unreported").lower()
            canary_evidence = canary.get("evaluation_evidence") or {}
            if canary_state == "deferred_preopen":
                if regular_session:
                    issues.append("RUT_CANARY_PREOPEN_DEFERRAL_DURING_SESSION")
                else:
                    warnings.append("RUT_CANARY_EVALUATION_DEFERRED_PREOPEN")
                if canary_evidence.get("evaluation_deferred") is not True:
                    issues.append("RUT_CANARY_DEFERRAL_EVIDENCE_MISSING")
            elif canary_state == "protected_opening_orb":
                issues.append("RUT_CANARY_OPENING_ORB_PROTECTION_ACTIVE")
                if canary_evidence.get("opening_orb_protection_active") is not True:
                    issues.append("RUT_CANARY_PROTECTION_EVIDENCE_MISSING")
                if (
                    canary_evidence.get("cash_session_reconnect_protection_active")
                    is not True
                ):
                    issues.append("RUT_CANARY_CASH_PROTECTION_EVIDENCE_MISSING")
            elif canary_state == "protected_cash_session":
                issues.append("RUT_CANARY_CASH_SESSION_PROTECTION_ACTIVE")
                if (
                    canary_evidence.get("cash_session_reconnect_protection_active")
                    is not True
                ):
                    issues.append("RUT_CANARY_CASH_PROTECTION_EVIDENCE_MISSING")
            elif canary_state == "deferred_off_hours" and not regular_session:
                pass
            elif canary_state not in {"armed", "observing"}:
                issues.append(f"RUT_CANARY_STATE_INVALID:{canary_state}")

    if regular_session:
        live_payload = live.get("payload") or {}
        active_epoch_id = _canonical_subscription_epoch(
            health_payload.get("subscription_epoch_id")
        )
        if live_payload.get("subscription_epoch_id") != active_epoch_id:
            issues.append("LIVE_HEALTH_SUBSCRIPTION_EPOCH_MISMATCH")
        processing_clock = health_payload.get("processing_clock_telemetry") or {}
        if processing_clock.get("status") != "synchronized":
            issues.append("BACKEND_PROCESSING_CLOCK_NOT_SYNCHRONIZED")
        for field in (
            "stream_connected",
            "stream_progressing",
            "collection_ready",
            "calculation_ready",
            "prediction_pipeline_ok",
        ):
            if live_payload.get(field) is not True:
                issues.append(f"LIVE_GATE_FAILED:{field}")
        structure_rows = database.get("market_structure_rows") or {}
        for symbol in capture_symbols:
            evidence = structure_rows.get(symbol) or {}
            is_core_structure = symbol in CORE_SYMBOLS

            def record_structure_problem(code: str) -> None:
                label = f"{code}:{symbol}"
                if is_core_structure:
                    issues.append(label)
                else:
                    warnings.append(f"OPTIONAL_{label}")

            if int(evidence.get("row_count") or 0) <= 0:
                record_structure_problem("VALID_GEX_STRUCTURE_NOT_ADVANCING")
                continue
            try:
                source_age = float(evidence.get("latest_source_age_seconds"))
            except (TypeError, ValueError):
                source_age = None
            if source_age is None:
                record_structure_problem("VALID_GEX_STRUCTURE_STALE")
            elif source_age < 0.0:
                record_structure_problem("GEX_STRUCTURE_SOURCE_TIMESTAMP_FUTURE")
            elif source_age > MARKET_STRUCTURE_MAX_AGE_SECONDS:
                record_structure_problem("VALID_GEX_STRUCTURE_STALE")
            try:
                capture_age = float(evidence.get("latest_capture_age_seconds"))
            except (TypeError, ValueError):
                capture_age = None
            if capture_age is None:
                record_structure_problem("GEX_STRUCTURE_CAPTURE_TIMESTAMP_MISSING")
            elif capture_age < 0.0:
                record_structure_problem("GEX_STRUCTURE_CAPTURE_TIMESTAMP_FUTURE")
            elif capture_age > MARKET_STRUCTURE_MAX_AGE_SECONDS:
                record_structure_problem("GEX_STRUCTURE_CAPTURE_STALE")
            if str(evidence.get("latest_provider") or "").lower() != "databento":
                record_structure_problem("GEX_STRUCTURE_PROVIDER_INVALID")
            if str(evidence.get("latest_validation_status") or "").lower() != "valid":
                record_structure_problem("GEX_STRUCTURE_VALIDATION_INVALID")
            if _positive_finite(evidence.get("latest_reference_price")) is None:
                record_structure_problem("GEX_STRUCTURE_REFERENCE_PRICE_INVALID")
            if _positive_finite(evidence.get("latest_gamma_pin")) is None:
                record_structure_problem("GAMMA_PIN_UNAVAILABLE")
            if _positive_finite(evidence.get("latest_max_pain")) is None:
                record_structure_problem("MAX_PAIN_UNAVAILABLE")
            if (
                _canonical_subscription_epoch(
                    evidence.get("latest_universe_sha256")
                )
                is None
            ):
                record_structure_problem("GEX_STRUCTURE_UNIVERSE_INVALID")
            if evidence.get("latest_primary_expiration") != now_ct.date().isoformat():
                record_structure_problem("GEX_STRUCTURE_PRIMARY_EXPIRATION_NOT_SAME_DAY")
            if evidence.get("latest_same_day_profile_available") is not True:
                record_structure_problem("GEX_STRUCTURE_SAME_DAY_PROFILE_UNAVAILABLE")
            if evidence.get("latest_subscription_epoch_id") != active_epoch_id:
                record_structure_problem("GEX_STRUCTURE_SUBSCRIPTION_EPOCH_MISMATCH")
            if (
                _canonical_subscription_generation(
                    evidence.get("latest_subscription_generation")
                )
                != active_generation
            ):
                record_structure_problem(
                    "GEX_STRUCTURE_SUBSCRIPTION_GENERATION_MISMATCH"
                )
            if not is_core_structure:
                continue
            calculation_bound = evidence.get("latest_calculation_bound")
            calculation_bound = (
                calculation_bound if isinstance(calculation_bound, dict) else {}
            )
            if int(calculation_bound.get("row_count") or 0) <= 0:
                record_structure_problem("GEX_CALCULATION_BOUND_STRUCTURE_MISSING")
                continue
            try:
                bound_source_age = float(
                    calculation_bound.get("source_age_seconds")
                )
                bound_capture_age = float(
                    calculation_bound.get("capture_age_seconds")
                )
                bound_latest_lag = float(
                    calculation_bound.get("lag_from_latest_source_seconds")
                )
            except (TypeError, ValueError):
                bound_source_age = None
                bound_capture_age = None
                bound_latest_lag = None
            if (
                bound_source_age is None
                or bound_capture_age is None
                or bound_latest_lag is None
                or not 0.0
                <= bound_source_age
                <= MARKET_STRUCTURE_MAX_AGE_SECONDS
                or not 0.0
                <= bound_capture_age
                <= MARKET_STRUCTURE_MAX_AGE_SECONDS
                or not 0.0
                <= bound_latest_lag
                <= MARKET_STRUCTURE_MAX_AGE_SECONDS
            ):
                record_structure_problem("GEX_CALCULATION_BOUND_STRUCTURE_STALE")
            if (
                str(calculation_bound.get("provider") or "").lower()
                != "databento"
                or str(calculation_bound.get("validation_status") or "").lower()
                != "valid"
                or not str(calculation_bound.get("calculation_id") or "").strip()
                or _positive_finite(calculation_bound.get("reference_price")) is None
                or _positive_finite(calculation_bound.get("gamma_pin")) is None
                or _positive_finite(calculation_bound.get("max_pain")) is None
            ):
                record_structure_problem(
                    "GEX_CALCULATION_BOUND_STRUCTURE_INVALID"
                )
            if (
                calculation_bound.get("subscription_epoch_id") != active_epoch_id
                or _canonical_subscription_generation(
                    calculation_bound.get("subscription_generation")
                )
                != active_generation
                or calculation_bound.get("universe_sha256")
                != evidence.get("latest_universe_sha256")
                or calculation_bound.get("primary_expiration")
                != now_ct.date().isoformat()
                or calculation_bound.get("same_day_profile_available") is not True
            ):
                record_structure_problem(
                    "GEX_CALCULATION_BOUND_IDENTITY_MISMATCH"
                )
            lineage = calculation_bound.get("lineage")
            lineage = lineage if isinstance(lineage, dict) else {}
            try:
                run_age = float(lineage.get("run_age_seconds"))
            except (TypeError, ValueError):
                run_age = None
            if (
                lineage.get("status") != "verified"
                or lineage.get("reason") is not None
                or lineage.get("calculation_id")
                != calculation_bound.get("calculation_id")
                or lineage.get("gamma_run_present") is not True
                or lineage.get("input_blob_present") is not True
                or lineage.get("payload_integrity_verified") is not True
                or _canonical_subscription_epoch(lineage.get("payload_sha256"))
                is None
                or run_age is None
                or not 0.0 <= run_age <= MARKET_STRUCTURE_MAX_AGE_SECONDS
            ):
                record_structure_problem("GEX_CALCULATION_LINEAGE_INVALID")

        reference_rows = database.get("orb_reference_rows") or {}
        reference_decisions = (
            database.get("orb_reference_progress_decisions") or {}
        )
        reference_due = now_ct.time() >= time(8, 30, 15)
        if reference_due:
            runtime_context = orb_payload.get("active_runtime_context") or {}
            if orb_payload.get("runtime_binding_applied") is not True:
                issues.append("ORB_RUNTIME_BINDING_MISSING")
            if orb_payload.get("runtime_context_stable") is not True:
                issues.append("ORB_RUNTIME_CONTEXT_UNSTABLE")
            if runtime_context.get("subscription_epoch_id") != active_epoch_id:
                issues.append("ORB_RUNTIME_SUBSCRIPTION_EPOCH_MISMATCH")
            for symbol in set(CORE_SYMBOLS).union(configured):
                evidence = reference_rows.get(symbol) or {}
                decision_evidence = reference_decisions.get(symbol) or {}
                if int(decision_evidence.get("pending_decision_count") or 0) > 0:
                    issues.append(f"ORB_REFERENCE_DECISION_PENDING:{symbol}")
                if int(evidence.get("row_count") or 0) <= 0:
                    issues.append(f"ORB_REFERENCE_NOT_ADVANCING:{symbol}")
                    continue
                try:
                    capture_ratio = float(evidence.get("opening_capture_ratio"))
                except (TypeError, ValueError):
                    capture_ratio = None
                if (
                    capture_ratio is None
                    or capture_ratio < ORB_REFERENCE_MIN_CAPTURE_RATIO
                ):
                    issues.append(f"ORB_REFERENCE_CAPTURE_RATIO_LOW:{symbol}")
                if evidence.get("advancing_5s_evidence") is not True:
                    issues.append(f"ORB_REFERENCE_5S_EVIDENCE_STALE:{symbol}")
                if evidence.get("opening_bucket_present") is not True:
                    issues.append(f"ORB_REFERENCE_OPENING_BUCKET_MISSING:{symbol}")
                if int(evidence.get("invalid_subscription_epoch_row_count") or 0):
                    issues.append(f"ORB_REFERENCE_EPOCH_INVALID:{symbol}")
                if evidence.get("mixed_subscription_epoch_rows") is True:
                    issues.append(f"ORB_REFERENCE_EPOCH_MIXED:{symbol}")
                if evidence.get("latest_subscription_epoch_id") != active_epoch_id:
                    issues.append(f"ORB_REFERENCE_SUBSCRIPTION_EPOCH_MISMATCH:{symbol}")
                if int(
                    evidence.get("invalid_subscription_generation_row_count") or 0
                ):
                    issues.append(f"ORB_REFERENCE_GENERATION_INVALID:{symbol}")
                if evidence.get("mixed_subscription_generation_rows") is True:
                    issues.append(f"ORB_REFERENCE_GENERATION_MIXED:{symbol}")
                generations = evidence.get("subscription_generations")
                latest_generation = _canonical_subscription_generation(
                    evidence.get("latest_subscription_generation")
                )
                if (
                    latest_generation != active_generation
                    or generations != [active_generation]
                ):
                    issues.append(
                        f"ORB_REFERENCE_SUBSCRIPTION_GENERATION_MISMATCH:{symbol}"
                    )
                state = (orb_payload.get("symbols") or {}).get(symbol) or {}
                semantics = state.get("reference_semantics") or {}
                if symbol == "RUT":
                    trading_date_iso = now_ct.date().isoformat()
                    persisted_same_day = bool(
                        evidence.get("latest_primary_expiration")
                        == trading_date_iso
                        and evidence.get("latest_same_day_profile_available") is True
                    )
                    projected_same_day = bool(
                        semantics.get("primary_expiration") == trading_date_iso
                        and semantics.get("same_day_profile_available") is True
                        and semantics.get("directional_base_eligible") is True
                    )
                    if not persisted_same_day or not projected_same_day:
                        issues.append("RUT_ORB_CONTEXT_ONLY")
                    if persisted_same_day != projected_same_day:
                        issues.append("RUT_ORB_CLASSIFICATION_MISMATCH")
                provenance = state.get("provenance") or {}
                if provenance.get("runtime_binding_applied") is not True:
                    issues.append(f"ORB_SYMBOL_RUNTIME_BINDING_MISSING:{symbol}")
                if provenance.get("active_runtime_epoch_aligned") is not True:
                    issues.append(f"ORB_SYMBOL_RUNTIME_EPOCH_MISMATCH:{symbol}")
                if (
                    _canonical_subscription_generation(
                        provenance.get("active_subscription_generation")
                    )
                    != active_generation
                ):
                    issues.append(f"ORB_SYMBOL_RUNTIME_GENERATION_MISMATCH:{symbol}")
                if provenance.get("subscription_generations") != [active_generation]:
                    issues.append(f"ORB_SYMBOL_RANGE_GENERATION_MISMATCH:{symbol}")
                if str(provenance.get("active_handoff_status") or "").lower() != "active":
                    issues.append(f"ORB_SYMBOL_HANDOFF_NOT_ACTIVE:{symbol}")

    if after_start:
        if not orb.get("ok"):
            issues.append("ORB_ENDPOINT_UNREACHABLE")
        else:
            if runtime_identity_required:
                runtime_context = orb_payload.get("active_runtime_context") or {}
                if runtime_context.get("subscription_epoch_id") != active_epoch_id:
                    issues.append("ORB_RUNTIME_SUBSCRIPTION_EPOCH_MISMATCH")
                if (
                    _canonical_subscription_generation(
                        runtime_context.get("subscription_generation")
                    )
                    != active_generation
                ):
                    issues.append("ORB_RUNTIME_SUBSCRIPTION_GENERATION_MISMATCH")
                if str(runtime_context.get("handoff_status") or "").lower() != "active":
                    issues.append("ORB_RUNTIME_HANDOFF_NOT_ACTIVE")
            symbols = orb_payload.get("symbols") or {}
            if regular_session:
                for symbol in CORE_SYMBOLS:
                    symbol_state = symbols.get(symbol) or {}
                    pin_behavior = symbol_state.get("pin_behavior") or {}
                    last_structure = symbol_state.get("last_known_structure") or {}
                    provenance = symbol_state.get("provenance") or {}
                    semantics = symbol_state.get("reference_semantics") or {}
                    if pin_behavior.get("level_availability_status") != "available":
                        issues.append(f"ORB_PIN_LEVELS_UNAVAILABLE:{symbol}")
                    if _positive_finite(pin_behavior.get("gamma_pin")) is None:
                        issues.append(f"ORB_GAMMA_PIN_UNAVAILABLE:{symbol}")
                    if _positive_finite(pin_behavior.get("max_pain")) is None:
                        issues.append(f"ORB_MAX_PAIN_UNAVAILABLE:{symbol}")
                    if last_structure.get("status") != "aligned":
                        issues.append(f"ORB_STRUCTURE_REFERENCE_NOT_ALIGNED:{symbol}")
                    if (
                        last_structure.get("level_availability_status")
                        != "available"
                    ):
                        issues.append(f"ORB_STRUCTURE_LEVELS_UNAVAILABLE:{symbol}")
                    if _positive_finite(last_structure.get("gamma_pin")) is None:
                        issues.append(f"ORB_STRUCTURE_GAMMA_PIN_UNAVAILABLE:{symbol}")
                    if _positive_finite(last_structure.get("max_pain")) is None:
                        issues.append(f"ORB_STRUCTURE_MAX_PAIN_UNAVAILABLE:{symbol}")
                    try:
                        structure_age = float(last_structure.get("age_seconds"))
                    except (TypeError, ValueError):
                        structure_age = None
                    if (
                        structure_age is None
                        or structure_age < 0.0
                        or structure_age > MARKET_STRUCTURE_MAX_AGE_SECONDS
                    ):
                        issues.append(f"ORB_STRUCTURE_REFERENCE_STALE:{symbol}")
                    if provenance.get("structure_reference_status") != "aligned":
                        issues.append(f"ORB_PROVENANCE_STRUCTURE_NOT_ALIGNED:{symbol}")
                    if provenance.get("structure_vs_reference_aligned") is not True:
                        issues.append(f"ORB_PROVENANCE_STRUCTURE_MISMATCH:{symbol}")
                    if provenance.get("structure_reference_fresh") is not True:
                        issues.append(f"ORB_PROVENANCE_STRUCTURE_STALE:{symbol}")
                    if provenance.get("current_pin_level_availability") != "available":
                        issues.append(f"ORB_PROVENANCE_PIN_LEVELS_UNAVAILABLE:{symbol}")
                    if semantics.get("primary_expiration") != now_ct.date().isoformat():
                        issues.append(f"ORB_PRIMARY_EXPIRATION_NOT_SAME_DAY:{symbol}")
                    if semantics.get("same_day_profile_available") is not True:
                        issues.append(f"ORB_SAME_DAY_PROFILE_UNAVAILABLE:{symbol}")
            if runtime_identity_required:
                for symbol in configured:
                    provenance = (symbols.get(symbol) or {}).get("provenance") or {}
                    if provenance.get("active_runtime_epoch_aligned") is not True:
                        issues.append(f"ORB_SYMBOL_RUNTIME_EPOCH_MISMATCH:{symbol}")
                    if (
                        _canonical_subscription_generation(
                            provenance.get("active_subscription_generation")
                        )
                        != active_generation
                    ):
                        issues.append(
                            f"ORB_SYMBOL_RUNTIME_GENERATION_MISMATCH:{symbol}"
                        )
                    if (
                        str(provenance.get("active_handoff_status") or "").lower()
                        != "active"
                    ):
                        issues.append(f"ORB_SYMBOL_HANDOFF_NOT_ACTIVE:{symbol}")
            for required_symbol in OPENING_SYMBOLS:
                if required_symbol in configured:
                    continue
                if required_symbol == "RUT" and now_ct.time() < time(8, 25):
                    warnings.append("RUT_PREOPEN_UPGRADE_PENDING")
                else:
                    issues.append(f"ORB_SYMBOL_NOT_CONFIGURED:{required_symbol}")
            if clock.get("synchronized") is True and now_ct.time() < time(8, 25):
                if "RUT" not in configured:
                    warnings.append("RUT_PREOPEN_UPGRADE_PENDING")
            due_windows = _due_windows(now_ct) if regular_session else ()
            for due_window in due_windows:
                for symbol in configured:
                    state = symbols.get(symbol) or {}
                    window = (state.get("opening_ranges") or {}).get(due_window) or {}
                    if window.get("capture_status") != "complete":
                        issues.append(f"ORB_{due_window}_INCOMPLETE:{symbol}")
                    try:
                        range_capture_ratio = float(
                            (window.get("capture_evidence") or {}).get(
                                "capture_ratio"
                            )
                        )
                    except (TypeError, ValueError):
                        range_capture_ratio = None
                    if (
                        range_capture_ratio is None
                        or range_capture_ratio < ORB_REFERENCE_MIN_CAPTURE_RATIO
                    ):
                        issues.append(
                            f"ORB_{due_window}_CAPTURE_RATIO_LOW:{symbol}"
                        )
                    if symbol == "VIX":
                        if window.get("directional_evidence_eligible") is not False:
                            issues.append("VIX_ORB_AUTHORITY_INVALID")
                    elif symbol in CORE_SYMBOLS and window.get(
                        "directional_evidence_eligible"
                    ) is not True:
                        issues.append(f"ORB_{due_window}_NOT_DIRECTIONAL_ELIGIBLE:{symbol}")
                    if symbol in CORE_SYMBOLS and window.get(
                        "combined_structure_directional_evidence_eligible"
                    ) is not True:
                        issues.append(
                            f"ORB_{due_window}_COMBINED_STRUCTURE_NOT_ELIGIBLE:{symbol}"
                        )
                    elif symbol == "RUT":
                        semantics = state.get("reference_semantics") or {}
                        rut_directional = bool(
                            semantics.get("primary_expiration")
                            == now_ct.date().isoformat()
                            and semantics.get("same_day_profile_available") is True
                            and semantics.get("directional_base_eligible") is True
                            and window.get("directional_evidence_eligible") is True
                        )
                        if not rut_directional:
                            issues.append("RUT_ORB_CONTEXT_ONLY")

    return (
        "degraded" if issues else "ready" if after_start else "preflight",
        list(dict.fromkeys(issues)),
        list(dict.fromkeys(warnings)),
    )


def scope_acceptance_evaluation(
    *, state: str, issues: list[str], acceptance_scope: str | None
) -> tuple[str, list[str], list[str]]:
    """Narrow only exact defects outside an explicitly bound milestone."""

    ordered_issues = list(dict.fromkeys(issues))
    if acceptance_scope == "first_eligible_gamma_capture":
        excluded_predicate = _is_gamma_scope_excluded_issue
    elif acceptance_scope in {"complete_5m_orb", "complete_60m_orb"}:
        excluded_predicate = _is_orb_scope_excluded_issue
    else:
        return state, ordered_issues, []
    excluded = [
        issue for issue in ordered_issues if excluded_predicate(issue)
    ]
    retained = [
        issue for issue in ordered_issues if not excluded_predicate(issue)
    ]
    scoped_state = (
        "preflight"
        if state == "preflight"
        else "degraded" if retained else "ready"
    )
    return scoped_state, retained, excluded


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--project-root", type=Path, default=Path.cwd())
    parser.add_argument("--backend-url", default="http://127.0.0.1:8000")
    parser.add_argument(
        "--dashboard-url", default="http://127.0.0.1:8501/_stcore/health"
    )
    parser.add_argument("--database", type=Path)
    parser.add_argument(
        "--timeout-seconds",
        type=float,
        default=10.0,
        help="Per-endpoint timeout; bounded to 0.2-10 seconds.",
    )
    parser.add_argument("--clock-samples", type=int, default=3)
    parser.add_argument(
        "--acceptance-scope",
        choices=ACCEPTANCE_SCOPES,
        help=(
            "Bind compact evidence to one opening milestone. Scopes only exclude "
            "exact issues proven independent of that milestone."
        ),
    )
    parser.add_argument(
        "--full",
        action="store_true",
        help="Include complete endpoint payloads for interactive diagnosis.",
    )
    return parser.parse_args(argv)


def _selected(source: object, fields: tuple[str, ...]) -> dict[str, Any]:
    if not isinstance(source, dict):
        return {}
    return {field: source.get(field) for field in fields if field in source}


def _endpoint_summary(
    endpoint: object,
    payload_fields: tuple[str, ...],
) -> dict[str, Any]:
    summary = _selected(
        endpoint,
        ("ok", "status_code", "error", "not_due"),
    )
    if not isinstance(endpoint, dict):
        return summary
    payload = endpoint.get("payload")
    if isinstance(payload, dict):
        summary["payload"] = _selected(payload, payload_fields)
    elif payload not in (None, ""):
        summary["payload"] = payload
    return summary


def _compact_orb(
    orb: object, *, due_windows: tuple[str, ...]
) -> dict[str, Any]:
    summary = _endpoint_summary(
        orb,
        (
            "schema_version",
            "configured_symbols",
            "requested_symbols",
            "runtime_binding_applied",
            "runtime_context_stable",
            "active_runtime_context",
        ),
    )
    if not isinstance(orb, dict):
        return summary
    payload = orb.get("payload")
    symbols = payload.get("symbols") if isinstance(payload, dict) else None
    if not isinstance(symbols, dict):
        return summary

    compact_symbols: dict[str, Any] = {}
    for symbol, raw_state in symbols.items():
        if not isinstance(raw_state, dict):
            compact_symbols[str(symbol)] = {"malformed": True}
            continue
        state = _selected(
            raw_state,
            (
                "configured",
                "trading_date",
                "as_of_utc",
                "clock_status",
                "capture_status",
                "orb_complete",
                "opening_price",
                "orb_high",
                "orb_low",
                "current_price",
                "current_reference_fresh",
                "breakout_direction",
                "directional_evidence_eligible",
                "combined_structure_directional_evidence_eligible",
                "warnings",
            ),
        )
        state["reference_semantics"] = _selected(
            raw_state.get("reference_semantics"),
            (
                "kind",
                "source",
                "authority",
                "primary_expiration",
                "same_day_profile_available",
                "directional_base_eligible",
                "limitation",
            ),
        )
        state["capture_evidence"] = _selected(
            raw_state.get("capture_evidence"),
            (
                "sample_count",
                "expected_sample_count",
                "capture_ratio",
                "opening_bucket_present",
                "first_sample_lag_seconds",
                "end_gap_seconds",
                "max_gap_seconds",
                "current_reference_age_seconds",
            ),
        )
        state["last_known_reference"] = _selected(
            raw_state.get("last_known_reference"),
            (
                "reference_price",
                "sample_id",
                "subscription_epoch_id",
                "subscription_generation",
                "runtime_aligned",
            ),
        )
        state["pin_behavior"] = _selected(
            raw_state.get("pin_behavior"),
            (
                "gamma_pin",
                "previous_gamma_pin",
                "gamma_pin_change_last",
                "opening_gamma_pin",
                "gamma_pin_change_from_open",
                "gamma_pin_change_count",
                "max_pain",
                "previous_max_pain",
                "max_pain_change_last",
                "opening_max_pain",
                "max_pain_change_from_open",
                "max_pain_change_count",
                "comparison_scope",
                "comparison_reset",
                "level_availability_status",
                "current_level_policy",
                "zero_gamma",
                "pin_lead_ratio",
                "pin_is_contested",
            ),
        )
        state["last_known_structure"] = _selected(
            raw_state.get("last_known_structure"),
            (
                "status",
                "level_availability_status",
                "source_timestamp_utc",
                "captured_at_utc",
                "freshness_timestamp_utc",
                "age_seconds",
                "maximum_current_age_seconds",
                "gamma_pin",
                "max_pain",
                "zero_gamma",
                "calculation_id",
            ),
        )
        state["last_calculation_bound_structure"] = _selected(
            raw_state.get("last_calculation_bound_structure"),
            (
                "status",
                "level_availability_status",
                "source_timestamp_utc",
                "captured_at_utc",
                "freshness_timestamp_utc",
                "age_seconds",
                "source_age_seconds",
                "capture_age_seconds",
                "maximum_current_age_seconds",
                "reference_price",
                "gamma_pin",
                "max_pain",
                "zero_gamma",
                "calculation_id",
                "provider",
                "subscription_epoch_id",
                "subscription_generation",
                "universe_sha256",
                "primary_expiration",
                "same_day_profile_available",
                "current_provenance_aligned",
                "runtime_aligned",
                "evidence_eligible",
            ),
        )
        state["provenance"] = _selected(
            raw_state.get("provenance"),
            (
                "providers",
                "spot_source",
                "subscription_epoch_ids",
                "subscription_generations",
                "universe_sha256_values",
                "runtime_binding_applied",
                "active_runtime_epoch_aligned",
                "active_subscription_epoch_id",
                "active_subscription_generation",
                "active_handoff_status",
                "range_provenance_aligned",
                "current_vs_range_aligned",
                "structure_vs_reference_aligned",
                "structure_reference_status",
                "structure_reference_fresh",
                "structure_reference_age_seconds",
                "structure_reference_max_age_seconds",
                "latest_source_timestamp_utc",
                "latest_reference_sample_id",
                "latest_reference_subscription_epoch_id",
                "latest_calculation_id",
                "primary_expiration",
                "same_day_profile_available",
                "structure_subscription_generation",
                "structure_subscription_epoch_id",
                "structure_universe_sha256",
            ),
        )
        ranges = raw_state.get("opening_ranges")
        compact_ranges: dict[str, Any] = {}
        if isinstance(ranges, dict):
            for window in due_windows:
                raw_window = ranges.get(window)
                if not isinstance(raw_window, dict):
                    continue
                compact_window = _selected(
                    raw_window,
                    (
                        "duration_minutes",
                        "range_start_utc",
                        "range_end_utc",
                        "clock_status",
                        "capture_status",
                        "orb_complete",
                        "opening_price",
                        "orb_high",
                        "orb_low",
                        "current_price",
                        "current_reference_fresh",
                        "breakout_direction",
                        "directional_evidence_eligible",
                        "combined_structure_directional_evidence_eligible",
                        "warnings",
                    ),
                )
                compact_window["capture_evidence"] = _selected(
                    raw_window.get("capture_evidence"),
                    (
                        "sample_count",
                        "expected_sample_count",
                        "capture_ratio",
                        "opening_bucket_present",
                        "first_sample_lag_seconds",
                        "end_gap_seconds",
                        "max_gap_seconds",
                        "current_reference_age_seconds",
                    ),
                )
                compact_window["provenance"] = _selected(
                    raw_window.get("provenance"),
                    (
                        "range_provenance_aligned",
                        "current_vs_range_aligned",
                        "structure_vs_reference_aligned",
                        "structure_reference_status",
                        "structure_reference_fresh",
                        "structure_reference_age_seconds",
                        "structure_reference_max_age_seconds",
                    ),
                )
                compact_ranges[window] = compact_window
        state["opening_ranges"] = compact_ranges
        compact_symbols[str(symbol)] = state
    summary.setdefault("payload", {})["symbols"] = compact_symbols
    return summary


def compact_report(report: dict[str, Any]) -> dict[str, Any]:
    """Return bounded acceptance evidence; keep full endpoint bodies opt-in."""
    observed_at_ct = report.get("observed_at_ct")
    try:
        parsed_observed_at_ct = datetime.fromisoformat(str(observed_at_ct))
    except ValueError:
        parsed_observed_at_ct = None
    due_windows = (
        _due_windows(parsed_observed_at_ct)
        if parsed_observed_at_ct is not None
        else ()
    )
    due_window = due_windows[-1] if due_windows else None
    health = report.get("backend_health")
    compact_health = _endpoint_summary(
        health,
        (
            "status",
            "provider",
            "websocket",
            "buffer_health",
            "subscription_session_state",
            "subscription_allowed",
            "subscription_suppressed",
            "subscription_window",
            "messages_received",
            "fresh_quote_counts",
            "subscription_epoch_id",
            "active_generation",
            "handoff_status",
            "subscription_epoch_id",
            "subscription_epoch_valid",
            "symbols_subscribed",
            "symbols_selected",
            "symbols_requested",
            "stream_progressing",
            "reconnect_attempts",
            "last_reconnect_utc",
            "last_reconnect_reason",
            "subscription_attempts",
            "provider_queue_full_warnings",
            "provider_slow_client_warnings",
            "provider_skipped_record_warnings",
            "provider_skipped_records",
            "last_error",
            "universe_fallback_active",
            "universe_provenance",
            "expected_current_cache_file",
            "subscription_bounds",
            "root_contract_counts",
            "core_symbol_status",
            "subscription_metadata",
            "optional_family_canary",
            "orb_reference_sampler",
            "runtime_controls",
            "processing_clock_telemetry",
            "loaded_code_fingerprint",
        ),
    )
    raw_health_payload = health.get("payload") if isinstance(health, dict) else None
    compact_health_payload = compact_health.get("payload")
    if isinstance(raw_health_payload, dict) and isinstance(compact_health_payload, dict):
        compact_health_payload["subscription_window"] = _selected(
            raw_health_payload.get("subscription_window"),
            (
                "state",
                "subscription_allowed",
                "observed_at_utc",
                "trading_date",
                "connect_from_utc",
                "cash_open_utc",
                "cash_close_utc",
            ),
        )
        compact_health_payload["subscription_bounds"] = _selected(
            raw_health_payload.get("subscription_bounds"),
            (
                "primary_max_strike_pairs",
                "shadow_max_strike_pairs",
                "minimum_primary_strike_pairs",
                "minimum_next_listed_strike_pairs",
                "max_contracts_per_market",
                "max_subscription_contracts",
                "pair_selection_method",
            ),
        )
        raw_metadata = raw_health_payload.get("subscription_metadata")
        compact_metadata = _selected(
            raw_metadata,
            (
                "profile",
                "full_contract_count",
                "selected_contract_count",
                "expired_contract_count",
                "selected_universe_sha256",
                "universe_provenance",
                "reservation_shortfall_pairs",
            ),
        )
        if isinstance(raw_metadata, dict) and isinstance(raw_metadata.get("markets"), dict):
            compact_metadata["markets"] = {
                str(symbol): _selected(
                    market,
                    (
                        "full_contract_count",
                        "selected_contract_count",
                        "market_reservation_shortfall_pairs",
                        "primary_reserved_pairs_retained",
                        "next_listed_reserved_pairs_retained",
                    ),
                )
                for symbol, market in raw_metadata["markets"].items()
            }
        compact_health_payload["subscription_metadata"] = compact_metadata
        if "subscription_staging" in raw_health_payload:
            raw_staging = raw_health_payload.get("subscription_staging")
            compact_health_payload["subscription_staging"] = _selected(
                raw_staging,
                (
                    "mode",
                    "state",
                    "active_stage",
                    "deferred_stage",
                    "full_selected_contract_count",
                    "active_contract_count",
                    "deferred_contract_count",
                    "requested_orb_families",
                    "primary_contract_counts",
                    "subscription_epoch_id",
                    "subscription_generation",
                    "full_selected_universe_sha256",
                    "same_client_additive_subscription",
                    "intraday_replay_for_deferred_stage",
                    "promotion_eligible",
                    "promotion_reasons",
                    "additive_request_sent",
                ),
            )
        if "market_subscription_status" in raw_health_payload:
            raw_market_status = raw_health_payload.get(
                "market_subscription_status"
            )
            compact_health_payload["market_subscription_status"] = {
                str(symbol): _selected(
                    status,
                    (
                        "requested",
                        "selected_contract_count",
                        "active_contract_count",
                        "deferred_contract_count",
                    ),
                )
                for symbol, status in (
                    raw_market_status.items()
                    if isinstance(raw_market_status, dict)
                    else ()
                )
            }
        compact_health_payload["optional_family_canary"] = _selected(
            raw_health_payload.get("optional_family_canary"),
            (
                "state",
                "family",
                "rollback_required",
                "reasons",
                "warmup_seconds",
                "warmup_remaining_seconds",
                "warmup_generation",
                "rollback_evidence",
                "evaluation_evidence",
            ),
        )
        compact_health_payload["orb_reference_sampler"] = _selected(
            raw_health_payload.get("orb_reference_sampler"),
            ("thread_alive", "interval_seconds", "last_bucket_utc", "markets"),
        )
        raw_controls = raw_health_payload.get("runtime_controls")
        compact_health_payload["runtime_controls"] = {
            "sleep_prevention": _selected(
                raw_controls.get("sleep_prevention")
                if isinstance(raw_controls, dict)
                else None,
                ("platform", "requested", "active", "mode", "limitation", "error"),
            )
        }
    live = _endpoint_summary(
        report.get("backend_live_health"),
        (
            "status",
            "subscription_epoch_id",
            "subscription_generation",
            "active_generation",
            "subscription_generation_valid",
            "stream_connected",
            "stream_progressing",
            "handoff_status",
            "transport_ready",
            "collection_ready",
            "calculation_ready",
            "prediction_pipeline_ok",
            "required_missing_symbols",
            "required_invalid_symbols",
            "required_epoch_mismatch_symbols",
            "required_generation_mismatch_symbols",
            "required_zero_fresh_quote_symbols",
            "optional_degraded_symbols",
            "last_tick_age_ms",
            "messages_received",
            "reconnect_attempts",
            "last_reconnect_utc",
            "last_reconnect_reason",
            "last_error",
        ),
    )
    database = report.get("database")
    compact_database = _selected(
        database,
        (
            "present",
            "path",
            "journal_mode",
            "quick_check",
            "market_structure_table_present",
            "market_structure_evidence_role",
            "missing_market_structure_columns",
            "market_structure_triggers",
            "missing_market_structure_triggers",
            "market_structure_rows",
            "orb_reference_table_present",
            "orb_reference_evidence_role",
            "missing_orb_reference_columns",
            "orb_reference_triggers",
            "missing_orb_reference_triggers",
            "orb_reference_indexes",
            "missing_orb_reference_indexes",
            "orb_reference_rows",
            "orb_reference_decision_table_present",
            "missing_orb_reference_decision_columns",
            "orb_reference_decision_triggers",
            "missing_orb_reference_decision_triggers",
            "orb_reference_decision_indexes",
            "missing_orb_reference_decision_indexes",
            "orb_reference_progress_decisions",
        ),
    )
    if isinstance(database, dict):
        compact_database["market_structure_column_count"] = len(
            database.get("market_structure_columns") or []
        )
        compact_database["expected_market_structure_column_count"] = len(
            EXPECTED_MARKET_STRUCTURE_COLUMNS
        )
        compact_database["orb_reference_column_count"] = len(
            database.get("orb_reference_columns") or []
        )
        compact_database["orb_reference_decision_column_count"] = len(
            database.get("orb_reference_decision_columns") or []
        )
        compact_database["expected_orb_reference_decision_column_count"] = len(
            EXPECTED_ORB_REFERENCE_DECISION_COLUMNS
        )
        compact_database["expected_orb_reference_column_count"] = len(
            EXPECTED_ORB_REFERENCE_COLUMNS
        )
    return {
        "schema_version": report.get("schema_version"),
        "output_mode": "compact",
        "state": report.get("state"),
        "observed_at_ct": report.get("observed_at_ct"),
        "observed_at_utc": report.get("observed_at_utc"),
        "issues": report.get("issues") or [],
        "acceptance_scope": report.get("acceptance_scope"),
        "out_of_scope_issues": report.get("out_of_scope_issues") or [],
        "warnings": report.get("warnings") or [],
        "due_orb_windows": list(due_windows),
        "due_orb_window": due_window,
        "clock": report.get("clock"),
        "backend_health": compact_health,
        "backend_live_health": live,
        "dashboard_health": _endpoint_summary(
            report.get("dashboard_health"),
            (),
        ),
        "orb": _compact_orb(report.get("orb"), due_windows=due_windows),
        "database": compact_database,
        "scheduled_tasks": report.get("scheduled_tasks"),
        "read_only": report.get("read_only") is True,
        "authorities": {
            "backend_health": "/health",
            "backend_live_health": "/health/live",
            "orb": "/v1/orb",
            "database": compact_database.get("path"),
        },
        "notes": report.get("notes") or [],
    }


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    root = args.project_root.resolve()
    now_ct = datetime.now(CT)
    database_path = args.database or root / "data" / "market_data.db"
    timeout = max(0.2, min(float(args.timeout_seconds), 10.0))
    clock = inspect_windows_clock(args.clock_samples)
    market_day = not is_weekend(now_ct.date()) and not is_holiday(now_ct.date())
    if not market_day or now_ct.time() < time(7, 45):
        health = live = orb = dashboard = {"ok": False, "not_due": True}
    else:
        health = _fetch(args.backend_url.rstrip("/") + "/health", timeout)
        live = _fetch(args.backend_url.rstrip("/") + "/health/live", timeout)
        orb = _fetch(args.backend_url.rstrip("/") + "/v1/orb", timeout)
        dashboard = _fetch(args.dashboard_url, timeout, json_body=False)
    database = inspect_database(
        database_path.resolve(),
        now_ct.date(),
    )
    scheduled_tasks = inspect_scheduled_tasks(root)
    # Evaluate against a timestamp taken after all bounded probes.  This keeps
    # newly persisted observations from appearing to originate in the future.
    now_ct = datetime.now(CT)
    state, issues, warnings = evaluate(
        now_ct=now_ct,
        clock=clock,
        health=health,
        live=live,
        dashboard=dashboard,
        orb=orb,
        database=database,
        scheduled_tasks=scheduled_tasks,
        project_root=root,
    )
    state, issues, out_of_scope_issues = scope_acceptance_evaluation(
        state=state,
        issues=issues,
        acceptance_scope=args.acceptance_scope,
    )
    report = {
        "schema_version": "marketpin-opening-readiness.v1",
        "state": state,
        "observed_at_ct": now_ct.isoformat(),
        "observed_at_utc": now_ct.astimezone(UTC).isoformat(),
        "issues": issues,
        "acceptance_scope": args.acceptance_scope,
        "out_of_scope_issues": out_of_scope_issues,
        "warnings": warnings,
        "clock": clock,
        "backend_health": health,
        "backend_live_health": live,
        "dashboard_health": dashboard,
        "orb": orb,
        "database": database,
        "scheduled_tasks": scheduled_tasks,
        "read_only": True,
        "notes": [
            "No process, service, database, file, or market-data connection was modified.",
            "ORB prices are MarketPin option-parity references, not official exchange OHLC.",
        ],
    }
    output = report if args.full else compact_report(report)
    if args.full:
        output["output_mode"] = "full"
    print(
        json.dumps(
            output,
            indent=2 if args.full else None,
            separators=None if args.full else (",", ":"),
            default=str,
        )
    )
    return 1 if state == "degraded" else 0


if __name__ == "__main__":
    raise SystemExit(main())
