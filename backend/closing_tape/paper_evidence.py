from __future__ import annotations

import hashlib
import json
import math
import os
import re
import sqlite3
import stat
import tempfile
import uuid
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

from .catalog_discovery import select_closing_tape_catalogs
from .config import _market_times
from .contracts import EVIDENCE_CONTRACT_VERSION, LIVE_SOURCE_KIND
from .candidate import (
    parse_candidate_package_receipt,
    parse_paper_candidate_activation_receipt,
)
from .close_evidence import resolve_verified_close_artifact
from .model_artifact import FrozenModelArtifact, load_frozen_model_artifact_bytes
from .dataset import PRODUCTION_FAMILIES
from .integrity import inspect_dbn
from .sqlite_io import sqlite_read_only_uri
from .surface import (
    MODEL_FEATURE_COLUMNS,
    MODEL_FEATURE_CONTRACT_HASH,
    select_decision_horizon_features,
)
from .surface_artifact import verify_retained_live_catalog_source


PAPER_FEATURE_PAYLOAD_CONTRACT_VERSION = "closing-tape-paper-feature-payload-v1"
LIVE_PREFIX_RECEIPT_CONTRACT_VERSION = "closing-tape-live-prefix-receipt-v1"
PAPER_PREFIX_REPLAY_CONTRACT_VERSION = "closing-tape-paper-prefix-replay-v2"
PAPER_CAMPAIGN_COVERAGE_CONTRACT_VERSION = (
    "closing-tape-paper-campaign-coverage-v1"
)
MAX_PAPER_FEATURE_PAYLOAD_BYTES = 256 * 1024
MAX_PAPER_PROMOTION_EVIDENCE_BYTES = 16 * 1024 * 1024
PAPER_FORECAST_MAX_LATENCY_SECONDS = 90.0
SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
UTC = timezone.utc


@dataclass(frozen=True)
class ResolvedPaperCandidate:
    activation_receipt_sha256: str
    candidate_package_sha256: str
    artifact_sha256: str
    artifact: FrozenModelArtifact
    activation_receipt: dict[str, object]
    candidate_package_receipt: dict[str, object]


@dataclass(frozen=True)
class ParsedPaperFeaturePayload:
    feature_payload_sha256: str
    forecast_identity: dict[str, object]
    live_prefix_receipt: dict[str, object]
    features: dict[str, float | None]


@dataclass(frozen=True)
class PaperPrefixReplayVerification:
    payload_receipt_sha256s: dict[str, str]
    receipt_sha256s: tuple[str, ...]
    final_source_sha256s: tuple[str, ...]
    receipt_artifacts: tuple[tuple[str, bytes], ...] = ()


@dataclass(frozen=True)
class PaperCampaignCoverage:
    payloads: tuple[ParsedPaperFeaturePayload, ...]
    recorded_at_by_payload: dict[str, str]
    opportunity_count: int
    receipt_sha256: str
    receipt_bytes: bytes = b""


def _canonical_json_bytes(value: object) -> bytes:
    try:
        return json.dumps(
            value, sort_keys=True, separators=(",", ":"), allow_nan=False
        ).encode("utf-8") + b"\n"
    except (TypeError, ValueError) as exc:
        raise ValueError("paper feature evidence must be strict canonical JSON") from exc


def _canonical_feature_value(value: object) -> float | None:
    try:
        numeric = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return numeric if math.isfinite(numeric) else None


def _canonical_live_prefix_receipt(
    value: Mapping[str, object],
    *,
    forecast_identity: Mapping[str, object],
) -> dict[str, object]:
    expected_fields = {
        "contract_version",
        "prefix_sha256",
        "cutoff_bytes",
        "session_id",
        "feed_name",
        "horizon_id",
        "trading_date",
        "feature_available_at_utc",
        "catalog_path",
        "source_path",
    }
    if set(value) != expected_fields:
        raise ValueError("paper live-prefix receipt fields are invalid")
    if value.get("contract_version") != LIVE_PREFIX_RECEIPT_CONTRACT_VERSION:
        raise ValueError("paper live-prefix receipt contract is unsupported")
    prefix_hash = str(value.get("prefix_sha256") or "").lower()
    try:
        cutoff_bytes = int(value.get("cutoff_bytes"))
        trading_date = date.fromisoformat(str(value.get("trading_date") or ""))
        available = datetime.fromisoformat(
            str(value.get("feature_available_at_utc") or "").replace("Z", "+00:00")
        )
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError("paper live-prefix receipt counters or times are invalid") from exc
    available_text = (
        available.astimezone(UTC).isoformat() if available.tzinfo is not None else ""
    )
    session_id = str(value.get("session_id") or "").strip()
    feed_name = str(value.get("feed_name") or "").strip()
    horizon_id = str(value.get("horizon_id") or "").strip()
    catalog_path = str(value.get("catalog_path") or "").strip()
    source_path = str(value.get("source_path") or "").strip()
    resolved_catalog_path = str(Path(catalog_path).resolve()) if catalog_path else ""
    resolved_source_path = str(Path(source_path).resolve()) if source_path else ""
    if (
        not SHA256_PATTERN.fullmatch(prefix_hash)
        or cutoff_bytes < 1
        or not session_id
        or feed_name != "opra_options"
        or horizon_id != "cash-close-minus-15m-v1"
        or int(forecast_identity.get("decision_horizon_minutes") or 0) != 15
        or not available_text
        or not catalog_path
        or not source_path
        or not Path(catalog_path).is_absolute()
        or not Path(source_path).is_absolute()
        or catalog_path != resolved_catalog_path
        or source_path != resolved_source_path
    ):
        raise ValueError("paper live-prefix receipt identity is invalid")
    canonical = {
        "contract_version": LIVE_PREFIX_RECEIPT_CONTRACT_VERSION,
        "prefix_sha256": prefix_hash,
        "cutoff_bytes": cutoff_bytes,
        "session_id": session_id,
        "feed_name": feed_name,
        "horizon_id": horizon_id,
        "trading_date": trading_date.isoformat(),
        "feature_available_at_utc": available_text,
        "catalog_path": resolved_catalog_path,
        "source_path": resolved_source_path,
    }
    if (
        canonical["prefix_sha256"] != forecast_identity.get("source_sha256")
        or canonical["session_id"] != forecast_identity.get("session_id")
        or canonical["trading_date"] != forecast_identity.get("trading_date")
        or canonical["feature_available_at_utc"]
        != forecast_identity.get("feature_available_at_utc")
    ):
        raise ValueError("paper live-prefix receipt does not bind the forecast identity")
    return canonical


def build_paper_feature_payload(
    features: Mapping[str, object],
    *,
    forecast_identity: Mapping[str, object],
    live_prefix_receipt: Mapping[str, object],
) -> tuple[str, str]:
    """Freeze the exact ordered model inputs used for one paper forecast."""

    missing = [column for column in MODEL_FEATURE_COLUMNS if column not in features]
    if missing:
        raise ValueError(
            "paper model feature payload is incomplete: " + ", ".join(missing)
        )
    identity = dict(forecast_identity)
    expected_identity_fields = {
        "forecast_key",
        "model_version",
        "artifact_sha256",
        "source_sha256",
        "session_id",
        "family_root",
        "trading_date",
        "decision_horizon_minutes",
        "feature_available_at_utc",
        "reference_price",
        "incumbent_predicted_close",
        "feature_contract_hash",
    }
    if set(identity) != expected_identity_fields:
        raise ValueError("paper feature forecast identity fields are invalid")
    payload = {
        "contract_version": PAPER_FEATURE_PAYLOAD_CONTRACT_VERSION,
        "feature_contract_hash": MODEL_FEATURE_CONTRACT_HASH,
        "forecast_identity": identity,
        "live_prefix_receipt": _canonical_live_prefix_receipt(
            live_prefix_receipt,
            forecast_identity=identity,
        ),
        "feature_columns": list(MODEL_FEATURE_COLUMNS),
        "feature_values": [
            _canonical_feature_value(features[column])
            for column in MODEL_FEATURE_COLUMNS
        ],
    }
    serialized = _canonical_json_bytes(payload)
    return serialized.decode("utf-8"), hashlib.sha256(serialized).hexdigest()


def parse_paper_feature_evidence(
    serialized: str,
    *,
    expected_sha256: str,
    expected_forecast_identity: Mapping[str, object],
) -> ParsedPaperFeaturePayload:
    """Validate one canonical payload while retaining its replay receipt."""

    raw = serialized.encode("utf-8")
    feature_hash = str(expected_sha256 or "").lower()
    if (
        not SHA256_PATTERN.fullmatch(feature_hash)
        or len(raw) <= 0
        or len(raw) > MAX_PAPER_FEATURE_PAYLOAD_BYTES
        or hashlib.sha256(raw).hexdigest() != feature_hash
    ):
        raise ValueError("paper feature payload SHA-256 or size is invalid")
    try:
        payload = json.loads(serialized)
    except json.JSONDecodeError as exc:
        raise ValueError("paper feature payload is not valid JSON") from exc
    expected_fields = {
        "contract_version",
        "feature_contract_hash",
        "forecast_identity",
        "live_prefix_receipt",
        "feature_columns",
        "feature_values",
    }
    if not isinstance(payload, dict) or set(payload) != expected_fields:
        raise ValueError("paper feature payload fields do not match its contract")
    if payload.get("contract_version") != PAPER_FEATURE_PAYLOAD_CONTRACT_VERSION:
        raise ValueError("paper feature payload contract is unsupported")
    if payload.get("feature_contract_hash") != MODEL_FEATURE_CONTRACT_HASH:
        raise ValueError("paper feature payload contract hash does not match")
    if payload.get("forecast_identity") != dict(expected_forecast_identity):
        raise ValueError("paper feature payload forecast identity does not match")
    prefix_receipt = payload.get("live_prefix_receipt")
    if not isinstance(prefix_receipt, dict) or prefix_receipt != _canonical_live_prefix_receipt(
        prefix_receipt,
        forecast_identity=expected_forecast_identity,
    ):
        raise ValueError("paper feature payload live-prefix receipt is invalid")
    if payload.get("feature_columns") != list(MODEL_FEATURE_COLUMNS):
        raise ValueError("paper feature payload columns or order do not match")
    values = payload.get("feature_values")
    if not isinstance(values, list) or len(values) != len(MODEL_FEATURE_COLUMNS):
        raise ValueError("paper feature payload values are incomplete")
    for value in values:
        if value is None:
            continue
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise ValueError("paper feature payload contains a non-numeric value")
        if not math.isfinite(float(value)):
            raise ValueError("paper feature payload contains a non-finite value")
    if raw != _canonical_json_bytes(payload):
        raise ValueError("paper feature payload is not canonical JSON")
    features = {
        column: (None if value is None else float(value))
        for column, value in zip(MODEL_FEATURE_COLUMNS, values)
    }
    return ParsedPaperFeaturePayload(
        feature_payload_sha256=feature_hash,
        forecast_identity=dict(expected_forecast_identity),
        live_prefix_receipt=dict(prefix_receipt),
        features=features,
    )


def parse_paper_feature_payload(
    serialized: str,
    *,
    expected_sha256: str,
    expected_forecast_identity: Mapping[str, object],
) -> dict[str, float | None]:
    """Validate canonical feature evidence and return the declared model inputs."""

    return parse_paper_feature_evidence(
        serialized,
        expected_sha256=expected_sha256,
        expected_forecast_identity=expected_forecast_identity,
    ).features


def _catalog_cutoff_matches(
    catalog_paths: tuple[Path, ...],
    receipt: Mapping[str, object],
) -> list[dict[str, object]]:
    matches: list[dict[str, object]] = []
    required_tables = {
        "tape_sessions",
        "tape_feed_status",
        "tape_analysis_cutoffs",
    }
    for catalog_path in catalog_paths:
        if catalog_path.parent.name != str(receipt["trading_date"]):
            continue
        connection = sqlite3.connect(
            sqlite_read_only_uri(catalog_path), uri=True, timeout=10.0
        )
        connection.row_factory = sqlite3.Row
        try:
            quick_check = connection.execute("PRAGMA quick_check").fetchone()
            if quick_check is None or str(quick_check[0]).lower() != "ok":
                raise ValueError(f"paper prefix catalog quick_check failed: {catalog_path}")
            tables = {
                str(row[0])
                for row in connection.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                )
            }
            if not required_tables <= tables:
                continue
            rows = connection.execute(
                """
                SELECT cutoffs.*, sessions.trading_date,
                       sessions.cash_open_utc, sessions.analysis_due_utc,
                       sessions.cash_close_utc,
                       sessions.status AS session_status,
                       feeds.dbn_path, feeds.sha256 AS feed_sha256,
                       feeds.status AS feed_status, feeds.complete AS feed_complete,
                       feeds.dataset, feeds.schemas_json, feeds.source_kind,
                       feeds.evidence_contract_version,
                       feeds.expected_subscription_acks,
                       feeds.reconnect_count, feeds.slow_reader_warnings,
                       feeds.provider_error_count, feeds.unmapped_trade_records
                FROM tape_analysis_cutoffs AS cutoffs
                JOIN tape_sessions AS sessions
                  ON sessions.session_id=cutoffs.session_id
                JOIN tape_feed_status AS feeds
                  ON feeds.session_id=cutoffs.session_id
                 AND feeds.feed_name=cutoffs.feed_name
                WHERE cutoffs.session_id=? AND cutoffs.feed_name=?
                  AND cutoffs.horizon_id=?
                """,
                (
                    str(receipt["session_id"]),
                    str(receipt["feed_name"]),
                    str(receipt["horizon_id"]),
                ),
            ).fetchall()
            for row in rows:
                payload = dict(row)
                payload["catalog_path"] = str(catalog_path.resolve())
                matches.append(payload)
        except sqlite3.DatabaseError as exc:
            raise ValueError(f"paper prefix catalog is unreadable: {catalog_path}") from exc
        finally:
            connection.close()
    return matches


def _resolve_live_prefix_source(
    catalog_paths: tuple[Path, ...],
    receipt: Mapping[str, object],
) -> tuple[Path, Path, dict[str, object]]:
    matches = _catalog_cutoff_matches(catalog_paths, receipt)
    if len(matches) != 1:
        raise ValueError(
            "paper live-prefix identity must resolve to exactly one retained "
            f"catalog/session/feed/horizon (matches={len(matches)})"
        )
    row = matches[0]
    catalog_path = Path(str(row["catalog_path"])).resolve()
    source_path = Path(str(row.get("dbn_path") or "")).resolve()
    try:
        cutoff_bytes = int(row.get("cutoff_bytes"))
        record_sequence = int(row.get("record_sequence"))
        processed_sequence = int(row.get("processed_sequence"))
        event_cutoff = datetime.fromisoformat(
            str(row.get("event_cutoff_utc") or "").replace("Z", "+00:00")
        )
        captured_at = datetime.fromisoformat(
            str(row.get("captured_at_utc") or "").replace("Z", "+00:00")
        )
        finalized_at = datetime.fromisoformat(
            str(row.get("finalized_at_utc") or "").replace("Z", "+00:00")
        )
        cash_open = datetime.fromisoformat(
            str(row.get("cash_open_utc") or "").replace("Z", "+00:00")
        )
        cash_close = datetime.fromisoformat(
            str(row.get("cash_close_utc") or "").replace("Z", "+00:00")
        )
        feature_at = datetime.fromisoformat(
            str(receipt["feature_available_at_utc"]).replace("Z", "+00:00")
        )
        analysis_due = datetime.fromisoformat(
            str(row.get("analysis_due_utc") or "").replace("Z", "+00:00")
        )
        expected_subscription_acks = int(row.get("expected_subscription_acks"))
        schemas = json.loads(str(row.get("schemas_json") or "[]"))
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError("paper live-prefix catalog counters or timestamps are invalid") from exc
    prefix_hash = str(row.get("prefix_sha256") or "").lower()
    final_hash = str(row.get("final_file_sha256") or "").lower()
    feed_hash = str(row.get("feed_sha256") or "").lower()
    last_trade_ns = row.get("last_trade_event_ns")
    event_cutoff_ns = int(event_cutoff.timestamp() * 1_000_000_000)
    if (
        catalog_path != Path(str(receipt["catalog_path"])).resolve()
        or source_path != Path(str(receipt["source_path"])).resolve()
        or str(row.get("trading_date") or "") != str(receipt["trading_date"])
        or catalog_path.parent.name != str(receipt["trading_date"])
        or str(row.get("session_status") or "") != "complete"
        or str(row.get("feed_status") or "") != "complete"
        or int(row.get("feed_complete") or 0) != 1
        or str(row.get("dataset") or "") != "OPRA.PILLAR"
        or str(row.get("source_kind") or "").lower() != LIVE_SOURCE_KIND
        or str(row.get("evidence_contract_version") or "")
        != EVIDENCE_CONTRACT_VERSION
        or not isinstance(schemas, list)
        or not {"tcbbo", "definition", "statistics"} <= set(map(str, schemas))
        or expected_subscription_acks < 1
        or any(
            int(row.get(field) or 0) != 0
            for field in (
                "reconnect_count", "slow_reader_warnings",
                "provider_error_count", "unmapped_trade_records",
            )
        )
        or prefix_hash != str(receipt["prefix_sha256"])
        or cutoff_bytes != int(receipt["cutoff_bytes"])
        or cutoff_bytes < 1
        or record_sequence < 1
        or processed_sequence != record_sequence
        or event_cutoff.tzinfo is None
        or analysis_due.tzinfo is None
        or event_cutoff.astimezone(UTC) != analysis_due.astimezone(UTC)
        or feature_at.tzinfo is None
        or event_cutoff.astimezone(UTC) != feature_at.astimezone(UTC)
        or captured_at.tzinfo is None
        or finalized_at.tzinfo is None
        or captured_at < event_cutoff
        or finalized_at < captured_at
        or cash_open.tzinfo is None
        or cash_close.tzinfo is None
        or not (cash_open < event_cutoff < cash_close)
        or (last_trade_ns is not None and int(last_trade_ns) > event_cutoff_ns)
        or not SHA256_PATTERN.fullmatch(final_hash)
        or feed_hash != final_hash
    ):
        raise ValueError("paper live-prefix receipt does not match finalized cutoff evidence")
    try:
        source_path.relative_to(catalog_path.parent)
    except ValueError as exc:
        raise ValueError("paper live-prefix DBN escapes its catalog trading-day directory") from exc
    if not source_path.is_file() or source_path.stat().st_size < cutoff_bytes:
        raise ValueError("paper live-prefix retained DBN is missing or shorter than cutoff")
    verified_source = verify_retained_live_catalog_source(
        catalog_path,
        trading_date=str(receipt["trading_date"]),
        session_id=str(receipt["session_id"]),
        feed_name=str(receipt["feed_name"]),
        source_sha256=final_hash,
    )
    if Path(str(verified_source.get("source_path") or "")).resolve() != source_path:
        raise ValueError("paper finalized source verifier resolved a different DBN")
    row["cash_open"] = cash_open.astimezone(UTC)
    row["cash_close"] = cash_close.astimezone(UTC)
    row["feature_at"] = feature_at.astimezone(UTC)
    row["captured_at"] = captured_at.astimezone(UTC)
    row["expected_subscription_acks"] = expected_subscription_acks
    row["final_file_sha256"] = final_hash
    row["final_source_bytes"] = int(verified_source["source_bytes"])
    row["last_trade_event_ns"] = (
        int(last_trade_ns) if last_trade_ns is not None else None
    )
    source_stat = source_path.stat()
    row["source_file_identity"] = (
        int(source_stat.st_dev),
        int(source_stat.st_ino),
        int(source_stat.st_size),
        int(source_stat.st_mtime_ns),
    )
    return catalog_path, source_path, row


def _assert_replayed_paper_group(
    surface: pd.DataFrame,
    payloads: list[ParsedPaperFeaturePayload],
    *,
    expected_families: tuple[str, ...],
) -> None:
    receipt = payloads[0].live_prefix_receipt
    selected = select_decision_horizon_features(surface, minutes_before_close=15)
    if selected.empty:
        raise ValueError("paper live-prefix replay produced no exact-horizon surface")
    family_rows = {
        str(row["family_root"]).upper(): row
        for row in selected.to_dict(orient="records")
    }
    if len(family_rows) != len(selected) or set(family_rows) != set(expected_families):
        raise ValueError(
            "paper live-prefix replay must produce exactly one row per expected family"
        )
    for payload in payloads:
        identity = payload.forecast_identity
        family = str(identity["family_root"]).upper()
        row = family_rows.get(family)
        if row is None:
            raise ValueError(f"paper live-prefix replay is missing {family}")
        actual_feature_time = pd.Timestamp(row["feature_available_at_utc"])
        if actual_feature_time.tzinfo is None:
            raise ValueError("paper live-prefix replay feature timestamp is naive")
        actual_features = {
            column: _canonical_feature_value(row.get(column))
            for column in MODEL_FEATURE_COLUMNS
        }
        if (
            str(row.get("session_id") or "") != str(receipt["session_id"])
            or str(row.get("feed_name") or "") != str(receipt["feed_name"])
            or str(row.get("trading_date") or "") != str(receipt["trading_date"])
            or actual_feature_time.tz_convert("UTC").isoformat()
            != str(receipt["feature_available_at_utc"])
            or str(row.get("source_sha256") or "").lower()
            != str(receipt["prefix_sha256"])
            or bool(row.get("capture_integrity_verified")) is not True
            or _canonical_feature_value(row.get("reference_price"))
            != _canonical_feature_value(identity["reference_price"])
            or _canonical_feature_value(row.get("predicted_close"))
            != _canonical_feature_value(identity["incumbent_predicted_close"])
            or actual_features != payload.features
        ):
            raise ValueError(
                f"paper live-prefix replay semantics do not match stored {family} payload"
            )


def replay_verify_paper_feature_groups(
    payloads: list[ParsedPaperFeaturePayload],
    *,
    project_root: str | Path,
    market_db_path: str | Path,
    recorded_at_by_payload: Mapping[str, str],
    expected_families: tuple[str, ...] = tuple(sorted(PRODUCTION_FAMILIES)),
) -> PaperPrefixReplayVerification:
    """Independently replay one raw prefix per paper session before scoring."""

    if not payloads:
        raise ValueError("paper feature replay evidence is empty")
    families = tuple(sorted({str(value).upper() for value in expected_families}))
    payload_hashes = {payload.feature_payload_sha256 for payload in payloads}
    if set(recorded_at_by_payload) != payload_hashes:
        raise ValueError("paper prefix replay recording-time evidence is incomplete")
    selection = select_closing_tape_catalogs(project_root)
    if selection.issues:
        raise ValueError(
            "paper prefix catalog selection failed: " + "; ".join(selection.issues)
        )
    if not selection.catalog_paths:
        raise ValueError("paper prefix replay found no retained catalogs")
    groups: dict[bytes, list[ParsedPaperFeaturePayload]] = {}
    payload_receipt_hashes: dict[str, str] = {}
    receipt_hashes: set[str] = set()
    final_source_hashes: set[str] = set()
    receipt_artifacts: dict[str, bytes] = {}
    for payload in payloads:
        key = _canonical_json_bytes(payload.live_prefix_receipt)
        groups.setdefault(key, []).append(payload)
    for group in groups.values():
        receipt = group[0].live_prefix_receipt
        claimed_catalog = Path(str(receipt["catalog_path"])).resolve()
        if claimed_catalog not in selection.catalog_paths:
            raise ValueError(
                "paper live-prefix receipt catalog is not in independent discovery"
            )
        group_families = [
            str(item.forecast_identity["family_root"]).upper() for item in group
        ]
        if (
            len(group_families) != len(families)
            or len(set(group_families)) != len(group_families)
            or set(group_families) != set(families)
        ):
            raise ValueError(
                "paper live-prefix group must contain exactly one payload per expected family"
            )
        catalog_path, source_path, cutoff = _resolve_live_prefix_source(
            selection.catalog_paths, receipt
        )
        for payload in group:
            try:
                recorded_at = datetime.fromisoformat(
                    str(
                        recorded_at_by_payload[payload.feature_payload_sha256]
                    ).replace("Z", "+00:00")
                )
            except (TypeError, ValueError) as exc:
                raise ValueError("paper prefix replay recorded timestamp is invalid") from exc
            if recorded_at.tzinfo is None or recorded_at < cutoff["captured_at"]:
                raise ValueError("paper forecast was recorded before its prefix cutoff")
        from .live_shadow import (
            _validate_live_prefix_integrity,
            build_surface_from_live_prefix,
            copy_verified_prefix,
        )

        with tempfile.TemporaryDirectory(prefix="marketpin-paper-prefix-") as raw_dir:
            prefix_path = Path(raw_dir) / "verified-prefix.dbn"
            copy_verified_prefix(
                source_path,
                prefix_path,
                cutoff_bytes=int(receipt["cutoff_bytes"]),
                expected_sha256=str(receipt["prefix_sha256"]),
            )
            prefix_integrity = inspect_dbn(
                prefix_path,
                require_tcbbo=True,
                expected_subscription_acks=int(
                    cutoff["expected_subscription_acks"]
                ),
            )
            _validate_live_prefix_integrity(
                prefix_integrity,
                expected_sha256=str(receipt["prefix_sha256"]),
                expected_subscription_acks=int(
                    cutoff["expected_subscription_acks"]
                ),
                event_cutoff_utc=cutoff["feature_at"],
            )
            if (
                prefix_integrity.last_trade_event_ns
                != cutoff["last_trade_event_ns"]
            ):
                raise ValueError(
                    "decoded prefix last trade does not match the cutoff ledger"
                )
            source_stat = source_path.stat()
            identity_before_hash = (
                int(source_stat.st_dev),
                int(source_stat.st_ino),
                int(source_stat.st_size),
                int(source_stat.st_mtime_ns),
            )
            with source_path.open("rb") as source_stream:
                current_full_hash = hashlib.file_digest(
                    source_stream, "sha256"
                ).hexdigest()
            source_stat = source_path.stat()
            identity_after_hash = (
                int(source_stat.st_dev),
                int(source_stat.st_ino),
                int(source_stat.st_size),
                int(source_stat.st_mtime_ns),
            )
            if (
                identity_before_hash != cutoff["source_file_identity"]
                or identity_after_hash != cutoff["source_file_identity"]
                or current_full_hash != cutoff["final_file_sha256"]
            ):
                raise ValueError(
                    "paper finalized DBN changed while its prefix was replayed"
                )
            surface = build_surface_from_live_prefix(
                prefix_path,
                prefix_sha256=str(receipt["prefix_sha256"]),
                catalog_path=catalog_path,
                market_db_path=market_db_path,
                session_id=str(receipt["session_id"]),
                feed_name=str(receipt["feed_name"]),
                trading_day=date.fromisoformat(str(receipt["trading_date"])),
                cash_open_utc=cutoff["cash_open"],
                cash_close_utc=cutoff["cash_close"],
                feature_available_at_utc=cutoff["feature_at"],
                derive_open_interest_from_prefix=True,
                expected_subscription_acks=int(
                    cutoff["expected_subscription_acks"]
                ),
                verified_integrity_report=prefix_integrity,
            )
        _assert_replayed_paper_group(
            surface, group, expected_families=families
        )
        replay_receipt = {
            "contract_version": PAPER_PREFIX_REPLAY_CONTRACT_VERSION,
            "model_version": str(group[0].forecast_identity["model_version"]),
            "artifact_sha256": str(
                group[0].forecast_identity["artifact_sha256"]
            ).lower(),
            "catalog_path": str(catalog_path),
            "source_path": str(source_path),
            "session_id": str(receipt["session_id"]),
            "feed_name": str(receipt["feed_name"]),
            "horizon_id": str(receipt["horizon_id"]),
            "trading_date": str(receipt["trading_date"]),
            "feature_available_at_utc": str(
                receipt["feature_available_at_utc"]
            ),
            "prefix_sha256": str(receipt["prefix_sha256"]),
            "cutoff_bytes": int(receipt["cutoff_bytes"]),
            "record_sequence": int(cutoff["record_sequence"]),
            "processed_sequence": int(cutoff["processed_sequence"]),
            "last_trade_event_ns": cutoff["last_trade_event_ns"],
            "final_source_sha256": str(cutoff["final_file_sha256"]),
            "final_source_bytes": int(cutoff["final_source_bytes"]),
            "expected_subscription_acks": int(
                cutoff["expected_subscription_acks"]
            ),
            "feature_payload_sha256s": sorted(
                item.feature_payload_sha256 for item in group
            ),
            "families": list(families),
            "exact_semantics_verified": True,
        }
        if any(
            str(item.forecast_identity["model_version"])
            != replay_receipt["model_version"]
            or str(item.forecast_identity["artifact_sha256"]).lower()
            != replay_receipt["artifact_sha256"]
            for item in group
        ):
            raise ValueError("paper live-prefix replay group mixes model identities")
        replay_bytes = _canonical_json_bytes(replay_receipt)
        replay_hash = hashlib.sha256(replay_bytes).hexdigest()
        receipt_hashes.add(replay_hash)
        receipt_artifacts[replay_hash] = replay_bytes
        final_source_hashes.add(str(cutoff["final_file_sha256"]))
        for payload in group:
            payload_receipt_hashes[payload.feature_payload_sha256] = replay_hash
    return PaperPrefixReplayVerification(
        payload_receipt_sha256s=payload_receipt_hashes,
        receipt_sha256s=tuple(sorted(receipt_hashes)),
        final_source_sha256s=tuple(sorted(final_source_hashes)),
        receipt_artifacts=tuple(sorted(receipt_artifacts.items())),
    )


def audit_paper_campaign_coverage(
    forecast_rows: list[Mapping[str, object]],
    *,
    project_root: str | Path,
    activated_at_utc: str,
    latest_counted_trading_date: str,
    expected_families: tuple[str, ...] = tuple(sorted(PRODUCTION_FAMILIES)),
) -> PaperCampaignCoverage:
    """Require a five-family batch for every clean cutoff since activation."""

    try:
        activated_at = datetime.fromisoformat(
            str(activated_at_utc).replace("Z", "+00:00")
        )
        latest_day = date.fromisoformat(str(latest_counted_trading_date))
    except ValueError as exc:
        raise ValueError("paper campaign audit boundary is invalid") from exc
    if activated_at.tzinfo is None:
        raise ValueError("paper campaign activation timestamp is naive")
    families = tuple(sorted({str(value).upper() for value in expected_families}))
    if set(families) != set(PRODUCTION_FAMILIES) or len(families) != 5:
        raise ValueError("paper campaign audit requires five production families")
    selection = select_closing_tape_catalogs(project_root)
    if selection.issues:
        raise ValueError(
            "paper campaign catalog selection failed: " + "; ".join(selection.issues)
        )
    opportunity_receipts: dict[
        tuple[str, str, str, str, str], dict[str, object]
    ] = {}
    for catalog_path in selection.catalog_paths:
        try:
            catalog_day = date.fromisoformat(catalog_path.parent.name)
        except ValueError:
            continue
        if catalog_day > latest_day:
            continue
        connection = sqlite3.connect(
            sqlite_read_only_uri(catalog_path), uri=True, timeout=10.0
        )
        connection.row_factory = sqlite3.Row
        try:
            tables = {
                str(row[0])
                for row in connection.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                )
            }
            if not {
                "tape_sessions", "tape_feed_status", "tape_analysis_cutoffs"
            } <= tables:
                continue
            rows = connection.execute(
                """
                SELECT cutoffs.*, sessions.trading_date,
                       sessions.status AS session_status,
                       feeds.dbn_path, feeds.dataset, feeds.source_kind,
                       feeds.evidence_contract_version,
                       feeds.status AS feed_status, feeds.complete AS feed_complete,
                       feeds.reconnect_count, feeds.slow_reader_warnings,
                       feeds.provider_error_count, feeds.unmapped_trade_records
                FROM tape_analysis_cutoffs AS cutoffs
                JOIN tape_sessions AS sessions
                  ON sessions.session_id=cutoffs.session_id
                JOIN tape_feed_status AS feeds
                  ON feeds.session_id=cutoffs.session_id
                 AND feeds.feed_name=cutoffs.feed_name
                WHERE cutoffs.horizon_id=?
                """,
                ("cash-close-minus-15m-v1",),
            ).fetchall()
        except sqlite3.DatabaseError as exc:
            raise ValueError(
                f"paper campaign catalog is unreadable: {catalog_path}"
            ) from exc
        finally:
            connection.close()
        for raw_row in rows:
            row = dict(raw_row)
            try:
                event_cutoff = datetime.fromisoformat(
                    str(row.get("event_cutoff_utc") or "").replace("Z", "+00:00")
                )
            except ValueError as exc:
                raise ValueError("paper campaign cutoff timestamp is invalid") from exc
            if event_cutoff.tzinfo is None or event_cutoff < activated_at:
                continue
            clean_claim = (
                str(row.get("session_status") or "") == "complete"
                and str(row.get("feed_status") or "") == "complete"
                and int(row.get("feed_complete") or 0) == 1
                and str(row.get("dataset") or "") == "OPRA.PILLAR"
                and str(row.get("source_kind") or "").lower() == LIVE_SOURCE_KIND
                and str(row.get("evidence_contract_version") or "")
                == EVIDENCE_CONTRACT_VERSION
                and not any(
                    int(row.get(field) or 0)
                    for field in (
                        "reconnect_count", "slow_reader_warnings",
                        "provider_error_count", "unmapped_trade_records",
                    )
                )
                and bool(str(row.get("finalized_at_utc") or ""))
            )
            if not clean_claim:
                continue
            receipt = {
                "contract_version": LIVE_PREFIX_RECEIPT_CONTRACT_VERSION,
                "prefix_sha256": str(row.get("prefix_sha256") or "").lower(),
                "cutoff_bytes": int(row.get("cutoff_bytes") or 0),
                "session_id": str(row.get("session_id") or ""),
                "feed_name": str(row.get("feed_name") or ""),
                "horizon_id": str(row.get("horizon_id") or ""),
                "trading_date": str(row.get("trading_date") or ""),
                "feature_available_at_utc": event_cutoff.astimezone(UTC).isoformat(),
                "catalog_path": str(catalog_path.resolve()),
                "source_path": str(Path(str(row.get("dbn_path") or "")).resolve()),
            }
            receipt = _canonical_live_prefix_receipt(
                receipt,
                forecast_identity={
                    "source_sha256": receipt["prefix_sha256"],
                    "session_id": receipt["session_id"],
                    "trading_date": receipt["trading_date"],
                    "feature_available_at_utc": receipt[
                        "feature_available_at_utc"
                    ],
                    "decision_horizon_minutes": 15,
                },
            )
            _resolve_live_prefix_source(selection.catalog_paths, receipt)
            key = (
                str(receipt["trading_date"]),
                str(receipt["session_id"]),
                str(receipt["feed_name"]),
                str(receipt["horizon_id"]),
                str(receipt["prefix_sha256"]),
            )
            if key in opportunity_receipts:
                raise ValueError("paper campaign has an ambiguous duplicate cutoff")
            opportunity_receipts[key] = receipt
    if not opportunity_receipts:
        raise ValueError("paper campaign has no clean finalized cutoff opportunities")

    parsed_by_key: dict[
        tuple[str, str, str, str, str], list[ParsedPaperFeaturePayload]
    ] = {}
    recorded_by_payload: dict[str, str] = {}
    for forecast in forecast_rows:
        try:
            feature_at = datetime.fromisoformat(
                str(forecast.get("feature_available_at_utc") or "").replace(
                    "Z", "+00:00"
                )
            )
            trading_day = date.fromisoformat(str(forecast.get("trading_date") or ""))
        except ValueError as exc:
            raise ValueError("paper campaign forecast timestamp is invalid") from exc
        if feature_at.tzinfo is None or feature_at < activated_at or trading_day > latest_day:
            continue
        if int(forecast.get("decision_horizon_minutes") or 0) != 15:
            raise ValueError("paper campaign contains a non-15-minute forecast")
        family = str(forecast.get("family_root") or "").upper()
        expected_identity = {
            "forecast_key": str(forecast.get("forecast_key") or ""),
            "model_version": str(forecast.get("model_version") or ""),
            "artifact_sha256": str(forecast.get("artifact_sha256") or "").lower(),
            "source_sha256": str(forecast.get("source_sha256") or "").lower(),
            "session_id": str(forecast.get("session_id") or ""),
            "family_root": family,
            "trading_date": trading_day.isoformat(),
            "decision_horizon_minutes": 15,
            "feature_available_at_utc": feature_at.astimezone(UTC).isoformat(),
            "reference_price": float(forecast.get("reference_price")),
            "incumbent_predicted_close": float(
                forecast.get("incumbent_predicted_close")
            ),
            "feature_contract_hash": str(
                forecast.get("feature_contract_hash") or ""
            ),
        }
        parsed = parse_paper_feature_evidence(
            str(forecast.get("feature_payload_json") or ""),
            expected_sha256=str(forecast.get("feature_payload_sha256") or ""),
            expected_forecast_identity=expected_identity,
        )
        receipt = parsed.live_prefix_receipt
        key = (
            str(receipt["trading_date"]),
            str(receipt["session_id"]),
            str(receipt["feed_name"]),
            str(receipt["horizon_id"]),
            str(receipt["prefix_sha256"]),
        )
        parsed_by_key.setdefault(key, []).append(parsed)
        recorded_by_payload[parsed.feature_payload_sha256] = str(
            forecast.get("recorded_at_utc") or ""
        )
    campaign_payloads: list[ParsedPaperFeaturePayload] = []
    opportunity_evidence: list[dict[str, object]] = []
    for key, receipt in sorted(opportunity_receipts.items()):
        batch = parsed_by_key.get(key, [])
        batch_families = [
            str(item.forecast_identity["family_root"]).upper() for item in batch
        ]
        if (
            len(batch) != 5
            or len(set(batch_families)) != 5
            or set(batch_families) != set(families)
        ):
            raise ValueError(
                "paper campaign omitted a complete five-family forecast batch for "
                f"{key[0]}/{key[1]}"
            )
        campaign_payloads.extend(batch)
        opportunity_evidence.append(
            {
                "trading_date": key[0],
                "session_id": key[1],
                "feed_name": key[2],
                "horizon_id": key[3],
                "prefix_sha256": key[4],
                "feature_payload_sha256s": sorted(
                    item.feature_payload_sha256 for item in batch
                ),
            }
        )
    coverage_receipt = {
        "contract_version": PAPER_CAMPAIGN_COVERAGE_CONTRACT_VERSION,
        "activated_at_utc": activated_at.astimezone(UTC).isoformat(),
        "latest_counted_trading_date": latest_day.isoformat(),
        "families": list(families),
        "opportunities": opportunity_evidence,
    }
    coverage_bytes = _canonical_json_bytes(coverage_receipt)
    return PaperCampaignCoverage(
        payloads=tuple(campaign_payloads),
        recorded_at_by_payload=recorded_by_payload,
        opportunity_count=len(opportunity_evidence),
        receipt_sha256=hashlib.sha256(coverage_bytes).hexdigest(),
        receipt_bytes=coverage_bytes,
    )


def _paper_promotion_evidence_path(
    project_root: str | Path,
    category: str,
    artifact_sha256: str,
) -> Path:
    artifact_hash = str(artifact_sha256 or "").lower()
    if category not in {"rows", "prefix_replays", "campaign_coverage"}:
        raise ValueError("paper promotion evidence category is invalid")
    if not SHA256_PATTERN.fullmatch(artifact_hash):
        raise ValueError("paper promotion evidence SHA-256 is invalid")
    evidence_root = (
        Path(project_root).resolve() / "models" / "promotion_evidence" / "paper"
    ).resolve()
    directory = (evidence_root / category).resolve()
    if directory.parent != evidence_root:
        raise ValueError("paper promotion evidence path escapes its canonical root")
    return directory / f"{artifact_hash}.json"


def _publish_content_addressed_bytes(path: Path, payload: bytes, expected_sha256: str) -> None:
    if (
        not payload
        or len(payload) > MAX_PAPER_PROMOTION_EVIDENCE_BYTES
        or hashlib.sha256(payload).hexdigest() != expected_sha256
    ):
        raise ValueError("paper promotion evidence bytes do not match their identity")
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        if path.read_bytes() != payload:
            raise ValueError("conflicting immutable paper promotion evidence")
        return
    temporary = path.with_name(f".{path.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp")
    try:
        with temporary.open("xb") as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        try:
            os.link(temporary, path)
        except FileExistsError:
            if path.read_bytes() != payload:
                raise ValueError("conflicting immutable paper promotion evidence") from None
    finally:
        if temporary.exists():
            temporary.unlink()


def retain_paper_promotion_evidence(
    project_root: str | Path,
    *,
    row_evidence_sha256: str,
    row_evidence_bytes: bytes,
    prefix_replay: PaperPrefixReplayVerification,
    campaign_coverage: PaperCampaignCoverage,
) -> None:
    """Publish the exact paper proof objects into immutable canonical paths."""

    row_hash = str(row_evidence_sha256 or "").lower()
    _publish_content_addressed_bytes(
        _paper_promotion_evidence_path(project_root, "rows", row_hash),
        row_evidence_bytes,
        row_hash,
    )
    replay_artifacts = dict(prefix_replay.receipt_artifacts)
    if set(replay_artifacts) != set(prefix_replay.receipt_sha256s):
        raise ValueError("paper prefix replay receipt artifacts are incomplete")
    for receipt_hash, receipt_bytes in replay_artifacts.items():
        _publish_content_addressed_bytes(
            _paper_promotion_evidence_path(
                project_root, "prefix_replays", receipt_hash
            ),
            receipt_bytes,
            receipt_hash,
        )
    coverage_hash = str(campaign_coverage.receipt_sha256 or "").lower()
    _publish_content_addressed_bytes(
        _paper_promotion_evidence_path(
            project_root, "campaign_coverage", coverage_hash
        ),
        campaign_coverage.receipt_bytes,
        coverage_hash,
    )


def _load_paper_promotion_json(
    project_root: str | Path,
    category: str,
    artifact_sha256: str,
) -> tuple[object, bytes]:
    artifact_hash = str(artifact_sha256 or "").lower()
    path = _paper_promotion_evidence_path(project_root, category, artifact_hash)
    if not path.is_file():
        raise ValueError(f"paper {category.replace('_', ' ')} evidence is missing")
    try:
        payload = path.read_bytes()
    except OSError as exc:
        raise ValueError(f"paper {category.replace('_', ' ')} evidence is unreadable") from exc
    if (
        not payload
        or len(payload) > MAX_PAPER_PROMOTION_EVIDENCE_BYTES
        or hashlib.sha256(payload).hexdigest() != artifact_hash
    ):
        raise ValueError(
            f"paper {category.replace('_', ' ')} evidence SHA-256 does not match"
        )
    try:
        return json.loads(payload.decode("utf-8")), payload
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError(
            f"paper {category.replace('_', ' ')} evidence is not valid UTF-8 JSON"
        ) from exc


def _hash_final_source_once(path: Path, *, cutoff_bytes: int) -> tuple[str, str, int]:
    try:
        before = path.stat()
    except OSError as exc:
        raise ValueError("paper final tape source is missing or unreadable") from exc
    if not stat.S_ISREG(before.st_mode) or before.st_size < cutoff_bytes or cutoff_bytes < 1:
        raise ValueError("paper final tape source size or type is invalid")
    full_digest = hashlib.sha256()
    prefix_digest = hashlib.sha256()
    prefix_remaining = cutoff_bytes
    try:
        with path.open("rb") as stream:
            for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                full_digest.update(chunk)
                if prefix_remaining:
                    prefix_chunk = chunk[:prefix_remaining]
                    prefix_digest.update(prefix_chunk)
                    prefix_remaining -= len(prefix_chunk)
    except OSError as exc:
        raise ValueError("paper final tape source could not be hashed") from exc
    try:
        after = path.stat()
    except OSError as exc:
        raise ValueError("paper final tape source changed while it was hashed") from exc
    identity_before = (
        int(before.st_dev), int(before.st_ino), int(before.st_size), int(before.st_mtime_ns)
    )
    identity_after = (
        int(after.st_dev), int(after.st_ino), int(after.st_size), int(after.st_mtime_ns)
    )
    if identity_before != identity_after or prefix_remaining:
        raise ValueError("paper final tape source changed while it was hashed")
    return full_digest.hexdigest(), prefix_digest.hexdigest(), int(after.st_size)


def validate_retained_paper_promotion_evidence(
    project_root: str | Path,
    *,
    model_version: str,
    artifact_sha256: str,
    activation_receipt_sha256: str,
    row_evidence_sha256: str,
    prefix_replay_receipt_sha256s: tuple[str, ...],
    final_tape_source_sha256s: tuple[str, ...],
    campaign_coverage_receipt_sha256: str,
    campaign_opportunities: int,
    close_source_artifact_sha256s: tuple[str, ...],
    paper_sessions: int,
    candidate_mae: float,
    incumbent_mae: float,
    family_metrics: Mapping[str, Mapping[str, object]],
) -> dict[str, object]:
    """Resolve and cross-check every retained object behind the paper gate."""

    version = str(model_version or "").strip()
    artifact_hash = str(artifact_sha256 or "").lower()
    activation_hash = str(activation_receipt_sha256 or "").lower()
    coverage_hash = str(campaign_coverage_receipt_sha256 or "").lower()
    expected_replay_hashes = tuple(sorted(prefix_replay_receipt_sha256s))
    expected_final_hashes = tuple(sorted(final_tape_source_sha256s))
    expected_close_hashes = tuple(sorted(close_source_artifact_sha256s))
    if (
        not version
        or any(
            not SHA256_PATTERN.fullmatch(value)
            for value in (
                artifact_hash,
                activation_hash,
                str(row_evidence_sha256 or "").lower(),
                coverage_hash,
                *expected_replay_hashes,
                *expected_final_hashes,
                *expected_close_hashes,
            )
        )
        or expected_replay_hashes != tuple(sorted(set(expected_replay_hashes)))
        or expected_final_hashes != tuple(sorted(set(expected_final_hashes)))
        or expected_close_hashes != tuple(sorted(set(expected_close_hashes)))
    ):
        raise ValueError("paper promotion evidence identities are invalid")

    candidate = resolve_paper_candidate(
        project_root,
        activation_receipt_sha256=activation_hash,
        candidate_package_sha256=None,
        model_version=version,
        artifact_sha256=artifact_hash,
        activated_at_utc=None,
    )
    activation = candidate.activation_receipt
    activated_at_text = str(activation["activated_at_utc"])
    candidate_package_hash = candidate.candidate_package_sha256
    activated_at = datetime.fromisoformat(activated_at_text.replace("Z", "+00:00"))

    raw_rows, row_bytes = _load_paper_promotion_json(
        project_root, "rows", row_evidence_sha256
    )
    if not isinstance(raw_rows, list) or not raw_rows:
        raise ValueError("paper row evidence must contain a non-empty list")
    canonical_rows = json.dumps(
        raw_rows, sort_keys=True, separators=(",", ":"), allow_nan=False
    ).encode("utf-8")
    if row_bytes != canonical_rows:
        raise ValueError("paper row evidence is not canonical JSON")

    expected_row_fields = {
        "forecast_key", "session_id", "trading_date", "family_root",
        "feature_available_at_utc", "recorded_at_utc",
        "decision_horizon_minutes", "feature_contract_hash", "source_sha256",
        "model_version", "artifact_sha256", "activation_receipt_sha256",
        "candidate_package_sha256", "activated_at_utc",
        "activation_registered_at_utc", "feature_payload_sha256",
        "feature_payload_json", "prefix_replay_receipt_sha256",
        "campaign_coverage_receipt_sha256", "close_source_artifact_sha256",
        "reference_price", "candidate_predicted_log_return",
        "candidate_predicted_close", "incumbent_predicted_close", "official_close",
    }
    rows: list[dict[str, object]] = []
    payload_to_row: dict[str, dict[str, object]] = {}
    vault_root = Path(project_root).resolve() / "data" / "verified_close_sources"
    resolved_close_keys: set[tuple[str, str, str]] = set()
    for raw in raw_rows:
        if not isinstance(raw, dict) or set(raw) != expected_row_fields:
            raise ValueError("paper row evidence fields do not match its contract")
        row = dict(raw)
        family = str(row["family_root"] or "").upper()
        try:
            trading_day = date.fromisoformat(str(row["trading_date"] or ""))
            feature_at = datetime.fromisoformat(
                str(row["feature_available_at_utc"] or "").replace("Z", "+00:00")
            )
            recorded_at = datetime.fromisoformat(
                str(row["recorded_at_utc"] or "").replace("Z", "+00:00")
            )
            activation_registered_at = datetime.fromisoformat(
                str(row["activation_registered_at_utc"] or "").replace(
                    "Z", "+00:00"
                )
            )
            numeric = {
                name: float(row[name])
                for name in (
                    "reference_price", "candidate_predicted_log_return",
                    "candidate_predicted_close", "incumbent_predicted_close",
                    "official_close",
                )
            }
        except (TypeError, ValueError, OverflowError) as exc:
            raise ValueError("paper row evidence counters, dates, or values are invalid") from exc
        feature_hash = str(row["feature_payload_sha256"] or "").lower()
        replay_hash = str(row["prefix_replay_receipt_sha256"] or "").lower()
        close_hash = str(row["close_source_artifact_sha256"] or "").lower()
        _cash_open, _analysis_due, cash_close, _stop = _market_times(trading_day)
        expected_feature_timestamp = cash_close.timestamp() - 15 * 60
        if (
            family not in PRODUCTION_FAMILIES
            or feature_at.tzinfo is None
            or recorded_at.tzinfo is None
            or activation_registered_at.tzinfo is None
            or activated_at > activation_registered_at
            or activated_at > feature_at
            or activation_registered_at > recorded_at
            or recorded_at < feature_at
            or (recorded_at - feature_at).total_seconds()
            > PAPER_FORECAST_MAX_LATENCY_SECONDS
            or abs(feature_at.timestamp() - expected_feature_timestamp) > 1.0
            or recorded_at > cash_close
            or int(row["decision_horizon_minutes"]) != 15
            or str(row["feature_contract_hash"]) != MODEL_FEATURE_CONTRACT_HASH
            or str(row["model_version"]) != version
            or str(row["artifact_sha256"]).lower() != artifact_hash
            or str(row["activation_receipt_sha256"]).lower() != activation_hash
            or str(row["candidate_package_sha256"]).lower()
            != candidate_package_hash
            or str(row["activated_at_utc"]) != activated_at_text
            or str(row["campaign_coverage_receipt_sha256"]).lower() != coverage_hash
            or replay_hash not in expected_replay_hashes
            or close_hash not in expected_close_hashes
            or not SHA256_PATTERN.fullmatch(feature_hash)
            or feature_hash in payload_to_row
            or any(not math.isfinite(value) for value in numeric.values())
            or any(
                numeric[name] <= 0
                for name in (
                    "reference_price", "candidate_predicted_close",
                    "incumbent_predicted_close", "official_close",
                )
            )
            or not math.isclose(
                numeric["candidate_predicted_close"],
                numeric["reference_price"]
                * math.exp(numeric["candidate_predicted_log_return"]),
                rel_tol=1e-12,
                abs_tol=1e-12,
            )
        ):
            raise ValueError("paper row evidence identity or prediction semantics are invalid")
        expected_identity = {
            "forecast_key": str(row["forecast_key"]),
            "model_version": version,
            "artifact_sha256": artifact_hash,
            "source_sha256": str(row["source_sha256"]).lower(),
            "session_id": str(row["session_id"]),
            "family_root": family,
            "trading_date": trading_day.isoformat(),
            "decision_horizon_minutes": 15,
            "feature_available_at_utc": feature_at.astimezone(UTC).isoformat(),
            "reference_price": numeric["reference_price"],
            "incumbent_predicted_close": numeric["incumbent_predicted_close"],
            "feature_contract_hash": MODEL_FEATURE_CONTRACT_HASH,
        }
        parsed = parse_paper_feature_evidence(
            str(row["feature_payload_json"]),
            expected_sha256=feature_hash,
            expected_forecast_identity=expected_identity,
        )
        row["_parsed_feature"] = parsed
        row["_numeric"] = numeric
        row["_family"] = family
        row["_trading_day"] = trading_day.isoformat()
        row["_replay_hash"] = replay_hash
        payload_to_row[feature_hash] = row
        rows.append(row)
        close_key = (trading_day.isoformat(), family, close_hash)
        if close_key not in resolved_close_keys:
            resolve_verified_close_artifact(
                vault_root,
                trading_date=trading_day,
                symbol=family,
                source_artifact_sha256=close_hash,
            )
            resolved_close_keys.add(close_key)
    if tuple(sorted({key[2] for key in resolved_close_keys})) != expected_close_hashes:
        raise ValueError("paper close artifacts do not match retained row evidence")

    replayed_predictions = recompute_paper_predictions(
        candidate,
        [row["_parsed_feature"].features for row in rows],
    )
    if len(replayed_predictions) != len(rows):
        raise ValueError("paper model replay returned an unexpected prediction count")
    for row, replayed_value in zip(rows, replayed_predictions):
        replayed_log_return = float(replayed_value)
        replayed_close = float(row["_numeric"]["reference_price"]) * math.exp(
            replayed_log_return
        )
        if (
            not math.isclose(
                float(row["_numeric"]["candidate_predicted_log_return"]),
                replayed_log_return,
                rel_tol=1e-12,
                abs_tol=1e-12,
            )
            or not math.isclose(
                float(row["_numeric"]["candidate_predicted_close"]),
                replayed_close,
                rel_tol=1e-12,
                abs_tol=1e-12,
            )
        ):
            raise ValueError("paper prediction does not reproduce from retained model inputs")

    selection = select_closing_tape_catalogs(project_root)
    if selection.issues:
        raise ValueError(
            "paper replay catalog selection failed: " + "; ".join(selection.issues)
        )
    selected_catalogs = set(selection.catalog_paths)
    replay_payload_hashes: set[str] = set()
    replay_by_hash: dict[str, dict[str, object]] = {}
    actual_final_hashes: set[str] = set()
    replay_fields = {
        "contract_version", "model_version", "artifact_sha256", "catalog_path",
        "source_path", "session_id", "feed_name", "horizon_id", "trading_date",
        "feature_available_at_utc", "prefix_sha256", "cutoff_bytes",
        "record_sequence", "processed_sequence", "last_trade_event_ns",
        "final_source_sha256", "final_source_bytes", "expected_subscription_acks",
        "feature_payload_sha256s", "families", "exact_semantics_verified",
    }
    opportunity_keys: dict[tuple[str, str, str, str, str], dict[str, object]] = {}
    for replay_hash in expected_replay_hashes:
        raw_receipt, receipt_bytes = _load_paper_promotion_json(
            project_root, "prefix_replays", replay_hash
        )
        if (
            not isinstance(raw_receipt, dict)
            or set(raw_receipt) != replay_fields
            or receipt_bytes != _canonical_json_bytes(raw_receipt)
            or raw_receipt.get("contract_version")
            != PAPER_PREFIX_REPLAY_CONTRACT_VERSION
            or raw_receipt.get("exact_semantics_verified") is not True
            or str(raw_receipt.get("model_version") or "") != version
            or str(raw_receipt.get("artifact_sha256") or "").lower()
            != artifact_hash
            or raw_receipt.get("families") != sorted(PRODUCTION_FAMILIES)
        ):
            raise ValueError("paper prefix replay receipt contract is invalid")
        try:
            trading_day = date.fromisoformat(str(raw_receipt["trading_date"]))
            available = datetime.fromisoformat(
                str(raw_receipt["feature_available_at_utc"]).replace("Z", "+00:00")
            )
            cutoff = int(raw_receipt["cutoff_bytes"])
            record_sequence = int(raw_receipt["record_sequence"])
            processed_sequence = int(raw_receipt["processed_sequence"])
            source_bytes = int(raw_receipt["final_source_bytes"])
            expected_acks = int(raw_receipt["expected_subscription_acks"])
        except (TypeError, ValueError, OverflowError) as exc:
            raise ValueError("paper prefix replay receipt counters are invalid") from exc
        catalog_path = Path(str(raw_receipt["catalog_path"])).resolve()
        source_path = Path(str(raw_receipt["source_path"])).resolve()
        payload_hashes = raw_receipt.get("feature_payload_sha256s")
        prefix_hash = str(raw_receipt["prefix_sha256"] or "").lower()
        final_hash = str(raw_receipt["final_source_sha256"] or "").lower()
        if (
            trading_day.isoformat() != str(raw_receipt["trading_date"])
            or available.tzinfo is None
            or catalog_path not in selected_catalogs
            or catalog_path.parent.name != trading_day.isoformat()
            or not source_path.is_relative_to(catalog_path.parent)
            or str(raw_receipt["feed_name"]) != "opra_options"
            or str(raw_receipt["horizon_id"]) != "cash-close-minus-15m-v1"
            or cutoff < 1
            or record_sequence < 1
            or processed_sequence != record_sequence
            or source_bytes < cutoff
            or expected_acks < 1
            or not SHA256_PATTERN.fullmatch(prefix_hash)
            or final_hash not in expected_final_hashes
            or not isinstance(payload_hashes, list)
            or len(payload_hashes) != len(PRODUCTION_FAMILIES)
            or payload_hashes != sorted(set(map(str, payload_hashes)))
            or any(not SHA256_PATTERN.fullmatch(str(value)) for value in payload_hashes)
            or replay_payload_hashes.intersection(map(str, payload_hashes))
        ):
            raise ValueError("paper prefix replay receipt identity is invalid")
        actual_full, actual_prefix, actual_bytes = _hash_final_source_once(
            source_path, cutoff_bytes=cutoff
        )
        if (
            actual_full != final_hash
            or actual_prefix != prefix_hash
            or actual_bytes != source_bytes
        ):
            raise ValueError("paper final tape bytes do not match replay receipt")
        replay_payload_hashes.update(map(str, payload_hashes))
        actual_final_hashes.add(final_hash)
        replay_by_hash[replay_hash] = dict(raw_receipt)
        key = (
            trading_day.isoformat(), str(raw_receipt["session_id"]),
            str(raw_receipt["feed_name"]), str(raw_receipt["horizon_id"]), prefix_hash,
        )
        if key in opportunity_keys:
            raise ValueError("paper prefix replay receipts duplicate one opportunity")
        opportunity_keys[key] = dict(raw_receipt)
    if tuple(sorted(actual_final_hashes)) != expected_final_hashes:
        raise ValueError("paper final tape hashes do not match retained replay receipts")

    for feature_hash, row in payload_to_row.items():
        replay = replay_by_hash[row["_replay_hash"]]
        live_receipt = row["_parsed_feature"].live_prefix_receipt
        if (
            feature_hash not in replay["feature_payload_sha256s"]
            or any(
                str(live_receipt[field]) != str(replay[field])
                for field in (
                    "catalog_path", "source_path", "session_id", "feed_name",
                    "horizon_id", "trading_date", "feature_available_at_utc",
                    "prefix_sha256", "cutoff_bytes",
                )
            )
        ):
            raise ValueError("paper row evidence does not bind its prefix replay receipt")

    raw_coverage, coverage_bytes = _load_paper_promotion_json(
        project_root, "campaign_coverage", coverage_hash
    )
    coverage_fields = {
        "contract_version", "activated_at_utc", "latest_counted_trading_date",
        "families", "opportunities",
    }
    if (
        not isinstance(raw_coverage, dict)
        or set(raw_coverage) != coverage_fields
        or coverage_bytes != _canonical_json_bytes(raw_coverage)
        or raw_coverage.get("contract_version")
        != PAPER_CAMPAIGN_COVERAGE_CONTRACT_VERSION
        or raw_coverage.get("families") != sorted(PRODUCTION_FAMILIES)
        or not isinstance(raw_coverage.get("opportunities"), list)
    ):
        raise ValueError("paper campaign coverage receipt contract is invalid")
    try:
        activated = datetime.fromisoformat(
            str(raw_coverage["activated_at_utc"]).replace("Z", "+00:00")
        )
        latest_day = date.fromisoformat(str(raw_coverage["latest_counted_trading_date"]))
    except ValueError as exc:
        raise ValueError("paper campaign coverage boundary is invalid") from exc
    if activated.tzinfo is None:
        raise ValueError("paper campaign coverage activation timestamp is naive")
    if activated.astimezone(UTC).isoformat() != activated_at.astimezone(UTC).isoformat():
        raise ValueError("paper campaign coverage is bound to another activation")
    coverage_keys: dict[tuple[str, str, str, str, str], tuple[str, ...]] = {}
    opportunity_fields = {
        "trading_date", "session_id", "feed_name", "horizon_id", "prefix_sha256",
        "feature_payload_sha256s",
    }
    for opportunity in raw_coverage["opportunities"]:
        if not isinstance(opportunity, dict) or set(opportunity) != opportunity_fields:
            raise ValueError("paper campaign opportunity fields are invalid")
        key = (
            str(opportunity["trading_date"]), str(opportunity["session_id"]),
            str(opportunity["feed_name"]), str(opportunity["horizon_id"]),
            str(opportunity["prefix_sha256"]),
        )
        hashes = opportunity["feature_payload_sha256s"]
        if (
            key in coverage_keys
            or key not in opportunity_keys
            or not isinstance(hashes, list)
            or tuple(hashes) != tuple(opportunity_keys[key]["feature_payload_sha256s"])
            or datetime.fromisoformat(
                str(opportunity_keys[key]["feature_available_at_utc"]).replace(
                    "Z", "+00:00"
                )
            ) < activated_at
        ):
            raise ValueError("paper campaign coverage does not bind replay opportunities")
        coverage_keys[key] = tuple(map(str, hashes))
    if (
        len(coverage_keys) != campaign_opportunities
        or campaign_opportunities != len(opportunity_keys)
        or set(coverage_keys) != set(opportunity_keys)
        or set().union(*(set(value) for value in coverage_keys.values()))
        != replay_payload_hashes
        or latest_day != max(date.fromisoformat(key[0]) for key in coverage_keys)
    ):
        raise ValueError("paper campaign coverage receipt is incomplete")

    rows_by_day: dict[str, list[dict[str, object]]] = {}
    for row in rows:
        rows_by_day.setdefault(str(row["_trading_day"]), []).append(row)
    if (
        len(rows_by_day) != paper_sessions
        or any(
            len(day_rows) != len(PRODUCTION_FAMILIES)
            or {str(row["_family"]) for row in day_rows} != set(PRODUCTION_FAMILIES)
            for day_rows in rows_by_day.values()
        )
    ):
        raise ValueError("paper metric rows do not contain complete five-family sessions")

    def _mae(selected: list[dict[str, object]], field: str) -> float:
        return sum(
            abs(float(row["_numeric"][field]) - float(row["_numeric"]["official_close"]))
            for row in selected
        ) / len(selected)

    if (
        not math.isclose(_mae(rows, "candidate_predicted_close"), candidate_mae, rel_tol=1e-12, abs_tol=1e-12)
        or not math.isclose(_mae(rows, "incumbent_predicted_close"), incumbent_mae, rel_tol=1e-12, abs_tol=1e-12)
        or set(family_metrics) != set(PRODUCTION_FAMILIES)
    ):
        raise ValueError("paper aggregate metrics do not reproduce from retained rows")
    for family in sorted(PRODUCTION_FAMILIES):
        selected = [row for row in rows if row["_family"] == family]
        metric = family_metrics[family]
        if (
            int(metric.get("rows") or 0) != len(selected)
            or int(metric.get("sessions") or 0)
            != len({str(row["_trading_day"]) for row in selected})
            or not math.isclose(
                float(metric.get("candidate_mae")),
                _mae(selected, "candidate_predicted_close"),
                rel_tol=1e-12,
                abs_tol=1e-12,
            )
            or not math.isclose(
                float(metric.get("incumbent_mae")),
                _mae(selected, "incumbent_predicted_close"),
                rel_tol=1e-12,
                abs_tol=1e-12,
            )
        ):
            raise ValueError(f"{family} paper metrics do not reproduce from retained rows")
    return {
        "rows": len(rows),
        "sessions": len(rows_by_day),
        "opportunities": len(opportunity_keys),
        "source_files": len(actual_final_hashes),
        "close_artifacts": len(resolved_close_keys),
    }


def _read_stable_content_addressed_file(
    path: Path,
    *,
    expected_sha256: str,
    label: str,
) -> bytes:
    if not path.is_file():
        raise ValueError(f"paper {label} is missing")
    try:
        before = path.read_bytes()
    except OSError as exc:
        raise ValueError(f"paper {label} is unreadable") from exc
    if hashlib.sha256(before).hexdigest() != expected_sha256:
        raise ValueError(f"paper {label} SHA-256 does not match")
    return before


def resolve_paper_candidate(
    project_root: str | Path,
    *,
    activation_receipt_sha256: str,
    candidate_package_sha256: str | None,
    model_version: str,
    artifact_sha256: str,
    activated_at_utc: str | None,
) -> ResolvedPaperCandidate:
    """Resolve and independently re-hash the complete paper candidate chain."""

    activation_hash = str(activation_receipt_sha256 or "").lower()
    package_hash = (
        str(candidate_package_sha256).lower()
        if candidate_package_sha256 is not None
        else None
    )
    artifact_hash = str(artifact_sha256 or "").lower()
    if any(
        not SHA256_PATTERN.fullmatch(value)
        for value in (activation_hash, artifact_hash)
    ):
        raise ValueError("paper candidate chain identities must be SHA-256 values")
    if package_hash is not None and not SHA256_PATTERN.fullmatch(package_hash):
        raise ValueError("paper candidate chain identities must be SHA-256 values")
    version = str(model_version or "").strip()
    if not version:
        raise ValueError("paper candidate chain model version is missing")

    models_dir = (Path(project_root).resolve() / "models").resolve()
    activation_path = (
        models_dir / "paper_activations" / f"{activation_hash}.json"
    ).resolve()
    if activation_path.parent != (models_dir / "paper_activations").resolve():
        raise ValueError("paper activation receipt path escapes its evidence directory")
    activation_bytes = _read_stable_content_addressed_file(
        activation_path,
        expected_sha256=activation_hash,
        label="activation receipt",
    )
    try:
        activation_payload = json.loads(activation_bytes.decode("utf-8"))
        activation = parse_paper_candidate_activation_receipt(activation_payload)
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
        raise ValueError("paper activation receipt is invalid") from exc
    if activation_bytes != _canonical_json_bytes(activation):
        raise ValueError("paper activation receipt is not canonical JSON")
    resolved_package_hash = str(activation["candidate_package_sha256"]).lower()
    resolved_activated_at = str(activation["activated_at_utc"])
    if (
        (package_hash is not None and resolved_package_hash != package_hash)
        or str(activation["candidate_package_path"])
        != f"candidate_packages/{resolved_package_hash}.json"
        or str(activation["model_version"]) != version
        or str(activation["artifact_sha256"]).lower() != artifact_hash
        or (
            activated_at_utc is not None
            and resolved_activated_at != str(activated_at_utc)
        )
    ):
        raise ValueError("paper activation receipt does not bind the ledger candidate")
    package_hash = resolved_package_hash

    package_path = (models_dir / "candidate_packages" / f"{package_hash}.json").resolve()
    if package_path.parent != (models_dir / "candidate_packages").resolve():
        raise ValueError("paper candidate package path escapes its evidence directory")
    package_bytes = _read_stable_content_addressed_file(
        package_path,
        expected_sha256=package_hash,
        label="candidate package",
    )
    try:
        package_payload = json.loads(package_bytes.decode("utf-8"))
        package = parse_candidate_package_receipt(package_payload)
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
        raise ValueError("paper candidate package is invalid") from exc
    if package_bytes != _canonical_json_bytes(package):
        raise ValueError("paper candidate package is not canonical JSON")
    if (
        str(package["version"]) != version
        or str(package["artifact_sha256"]).lower() != artifact_hash
    ):
        raise ValueError("paper candidate package does not bind the ledger artifact")

    artifact_path = (models_dir / str(package["artifact_path"])).resolve()
    if artifact_path.parent != models_dir:
        raise ValueError("paper model artifact escapes the models directory")
    artifact_bytes = _read_stable_content_addressed_file(
        artifact_path,
        expected_sha256=artifact_hash,
        label="model artifact",
    )
    try:
        artifact = load_frozen_model_artifact_bytes(artifact_bytes)
    except ValueError as exc:
        raise ValueError("paper model artifact is invalid") from exc
    if (
        artifact.training_rows != int(package["training_rows"])
        or artifact.training_epochs != int(package["training_epochs"])
        or artifact.training_device != str(package["training_device"])
        or artifact.source_sha256s != tuple(package["source_sha256s"])
        or artifact.label_source_artifact_sha256s
        != tuple(package["label_source_artifact_sha256s"])
        or artifact.surface_artifact_sha256
        != str(package["surface_artifact_sha256"])
        or artifact.surface_replay_receipt_sha256
        != str(package["surface_replay_receipt_sha256"])
    ):
        raise ValueError("paper model artifact provenance does not match its package")
    return ResolvedPaperCandidate(
        activation_receipt_sha256=activation_hash,
        candidate_package_sha256=package_hash,
        artifact_sha256=artifact_hash,
        artifact=artifact,
        activation_receipt=activation,
        candidate_package_receipt=package,
    )


def recompute_paper_predictions(
    candidate: ResolvedPaperCandidate,
    feature_rows: list[dict[str, float | None]],
) -> np.ndarray:
    if not feature_rows:
        raise ValueError("paper feature evidence is empty")
    frame = pd.DataFrame(feature_rows, columns=list(MODEL_FEATURE_COLUMNS))
    return candidate.artifact.predict_log_return(frame, device="cpu")
