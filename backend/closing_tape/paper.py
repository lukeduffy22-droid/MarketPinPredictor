from __future__ import annotations

import hashlib
import json
import math
import re
import sqlite3
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Iterable

import numpy as np

from .config import _market_times
from .dataset import PRODUCTION_FAMILIES, load_scored_marketpin_closes
from .paper_evidence import (
    audit_paper_campaign_coverage,
    build_paper_feature_payload,
    parse_paper_feature_evidence,
    recompute_paper_predictions,
    replay_verify_paper_feature_groups,
    retain_paper_promotion_evidence,
    resolve_paper_candidate,
)
from .surface import MODEL_FEATURE_CONTRACT_HASH


UTC = timezone.utc
SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
PAPER_FORECAST_MAX_LATENCY_SECONDS = 90.0


@dataclass(frozen=True)
class PaperFamilyMetric:
    family_root: str
    rows: int
    sessions: int
    candidate_mae: float
    incumbent_mae: float


@dataclass(frozen=True)
class PaperEvaluationReport:
    model_version: str
    artifact_sha256: str
    complete_sessions: int
    eligible_rows: int
    candidate_mae: float
    incumbent_mae: float
    evidence_sha256: str
    activation_receipt_sha256s: tuple[str, ...]
    prefix_replay_receipt_sha256s: tuple[str, ...]
    final_tape_source_sha256s: tuple[str, ...]
    campaign_opportunities: int
    campaign_coverage_receipt_sha256: str
    close_source_artifact_sha256s: tuple[str, ...]
    family_metrics: tuple[PaperFamilyMetric, ...]

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


def initialize_paper_forecast_ledger(path: str | Path) -> None:
    with sqlite3.connect(Path(path)) as connection:
        connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS paper_close_forecasts (
                id INTEGER PRIMARY KEY,
                forecast_key TEXT NOT NULL UNIQUE,
                model_version TEXT NOT NULL,
                artifact_sha256 TEXT NOT NULL,
                feature_contract_hash TEXT NOT NULL,
                source_sha256 TEXT NOT NULL,
                session_id TEXT NOT NULL,
                family_root TEXT NOT NULL,
                trading_date TEXT NOT NULL,
                decision_horizon_minutes INTEGER NOT NULL,
                feature_available_at_utc TEXT NOT NULL,
                recorded_at_utc TEXT NOT NULL,
                reference_price REAL NOT NULL,
                candidate_predicted_log_return REAL NOT NULL,
                candidate_predicted_close REAL NOT NULL,
                incumbent_predicted_close REAL NOT NULL,
                UNIQUE(model_version, family_root, trading_date, decision_horizon_minutes)
            );
            CREATE TRIGGER IF NOT EXISTS paper_close_forecasts_no_update
            BEFORE UPDATE ON paper_close_forecasts
            BEGIN SELECT RAISE(ABORT, 'paper_close_forecasts is append-only'); END;
            CREATE TRIGGER IF NOT EXISTS paper_close_forecasts_no_delete
            BEFORE DELETE ON paper_close_forecasts
            BEGIN SELECT RAISE(ABORT, 'paper_close_forecasts is append-only'); END;
            CREATE TABLE IF NOT EXISTS paper_candidate_activations (
                activation_receipt_sha256 TEXT PRIMARY KEY,
                model_version TEXT NOT NULL,
                artifact_sha256 TEXT NOT NULL,
                candidate_package_sha256 TEXT NOT NULL,
                activated_at_utc TEXT NOT NULL,
                registered_at_utc TEXT NOT NULL,
                UNIQUE(model_version, artifact_sha256)
            );
            CREATE UNIQUE INDEX IF NOT EXISTS
            paper_candidate_activations_one_artifact_per_version
            ON paper_candidate_activations(model_version);
            CREATE TRIGGER IF NOT EXISTS paper_candidate_activations_no_update
            BEFORE UPDATE ON paper_candidate_activations
            BEGIN SELECT RAISE(ABORT, 'paper_candidate_activations is append-only'); END;
            CREATE TRIGGER IF NOT EXISTS paper_candidate_activations_no_delete
            BEFORE DELETE ON paper_candidate_activations
            BEGIN SELECT RAISE(ABORT, 'paper_candidate_activations is append-only'); END;
            CREATE TABLE IF NOT EXISTS paper_forecast_feature_payloads (
                forecast_key TEXT PRIMARY KEY,
                feature_payload_sha256 TEXT NOT NULL UNIQUE,
                feature_payload_json TEXT NOT NULL
            );
            CREATE TRIGGER IF NOT EXISTS paper_forecast_feature_payloads_no_update
            BEFORE UPDATE ON paper_forecast_feature_payloads
            BEGIN SELECT RAISE(ABORT, 'paper_forecast_feature_payloads is append-only'); END;
            CREATE TRIGGER IF NOT EXISTS paper_forecast_feature_payloads_no_delete
            BEFORE DELETE ON paper_forecast_feature_payloads
            BEGIN SELECT RAISE(ABORT, 'paper_forecast_feature_payloads is append-only'); END;
            """
        )


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _utc_timestamp(value: datetime, *, field: str) -> str:
    if value.tzinfo is None:
        raise ValueError(f"{field} must be timezone-aware")
    return value.astimezone(UTC).isoformat()


def record_paper_candidate_activation(
    path: str | Path,
    *,
    activation_receipt_sha256: str,
    model_version: str,
    artifact_sha256: str,
    candidate_package_sha256: str,
    activated_at_utc: datetime,
) -> None:
    """Bind one immutable candidate activation to the paper ledger."""

    activation_hash = activation_receipt_sha256.lower()
    artifact_hash = artifact_sha256.lower()
    package_hash = candidate_package_sha256.lower()
    if any(
        not SHA256_PATTERN.fullmatch(value)
        for value in (activation_hash, artifact_hash, package_hash)
    ):
        raise ValueError("paper activation identities must be SHA-256 values")
    version = str(model_version or "").strip()
    if not version:
        raise ValueError("paper activation model version is required")
    activated = _utc_timestamp(activated_at_utc, field="paper activation timestamp")
    registered = _utc_timestamp(_utc_now(), field="paper activation registration")
    if datetime.fromisoformat(activated) > datetime.fromisoformat(registered):
        raise ValueError("paper activation cannot be registered before activation")
    values = (
        activation_hash,
        version,
        artifact_hash,
        package_hash,
        activated,
        registered,
    )
    initialize_paper_forecast_ledger(path)
    with sqlite3.connect(Path(path)) as connection:
        connection.row_factory = sqlite3.Row
        connection.execute(
            """
            INSERT OR IGNORE INTO paper_candidate_activations (
                activation_receipt_sha256, model_version, artifact_sha256,
                candidate_package_sha256, activated_at_utc, registered_at_utc
            ) VALUES (?,?,?,?,?,?)
            """,
            values,
        )
        existing = connection.execute(
            """
            SELECT * FROM paper_candidate_activations
            WHERE model_version=?
            """,
            (version,),
        ).fetchone()
        if existing is None:
            raise ValueError("conflicting paper candidate activation identity")
        proposed = dict(
            zip(
                (
                    "activation_receipt_sha256",
                    "model_version",
                    "artifact_sha256",
                    "candidate_package_sha256",
                    "activated_at_utc",
                ),
                values[:5],
            )
        )
        conflicts = [
            key for key, value in proposed.items() if existing[key] != value
        ]
        if conflicts:
            raise ValueError(
                "conflicting paper candidate activation: " + ", ".join(conflicts)
            )


def record_paper_forecast(
    path: str | Path,
    *,
    model_version: str,
    artifact_sha256: str,
    source_sha256: str,
    session_id: str,
    family_root: str,
    trading_day: date,
    decision_horizon_minutes: int,
    feature_available_at_utc: datetime,
    reference_price: float,
    candidate_predicted_log_return: float,
    incumbent_predicted_close: float,
    model_features: Mapping[str, object],
    live_prefix_receipt: Mapping[str, object],
) -> str:
    family = family_root.upper()
    if family not in PRODUCTION_FAMILIES:
        raise ValueError(f"unsupported paper family: {family}")
    if decision_horizon_minutes < 1:
        raise ValueError("decision horizon must be positive")
    artifact_hash = artifact_sha256.lower()
    source_hash = source_sha256.lower()
    if not SHA256_PATTERN.fullmatch(artifact_hash) or not SHA256_PATTERN.fullmatch(source_hash):
        raise ValueError("paper artifact and source hashes must be SHA-256 values")
    available = feature_available_at_utc
    recorded = _utc_now()
    if available.tzinfo is None or recorded.tzinfo is None:
        raise ValueError("paper timestamps must be timezone-aware")
    available = available.astimezone(UTC)
    recorded = recorded.astimezone(UTC)
    if recorded < available:
        raise ValueError("paper forecast cannot be recorded before features are available")
    if (recorded - available).total_seconds() > PAPER_FORECAST_MAX_LATENCY_SECONDS:
        raise ValueError(
            "paper forecast missed the bounded decision-horizon recording window"
        )
    _cash_open, _analysis_due, cash_close, _stop = _market_times(trading_day)
    expected_available = cash_close.timestamp() - decision_horizon_minutes * 60
    if abs(available.timestamp() - expected_available) > 1.0:
        raise ValueError("paper feature timestamp is not the exact decision-horizon availability time")
    if recorded > cash_close:
        raise ValueError("paper forecast must be recorded before the cash close")
    numeric = np.asarray(
        [reference_price, candidate_predicted_log_return, incumbent_predicted_close], dtype=float
    )
    if not np.isfinite(numeric).all() or reference_price <= 0 or incumbent_predicted_close <= 0:
        raise ValueError("paper forecast prices and return must be finite and prices positive")
    candidate_close = float(reference_price * np.exp(candidate_predicted_log_return))
    identity = {
        "model_version": model_version,
        "family_root": family,
        "trading_date": trading_day.isoformat(),
        "decision_horizon_minutes": decision_horizon_minutes,
    }
    forecast_key = hashlib.sha256(
        json.dumps(identity, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    feature_forecast_identity = {
        "forecast_key": forecast_key,
        "model_version": str(model_version),
        "artifact_sha256": artifact_hash,
        "source_sha256": source_hash,
        "session_id": str(session_id),
        "family_root": family,
        "trading_date": trading_day.isoformat(),
        "decision_horizon_minutes": int(decision_horizon_minutes),
        "feature_available_at_utc": available.isoformat(),
        "reference_price": float(reference_price),
        "incumbent_predicted_close": float(incumbent_predicted_close),
        "feature_contract_hash": MODEL_FEATURE_CONTRACT_HASH,
    }
    feature_payload_json, feature_payload_sha256 = build_paper_feature_payload(
        model_features,
        forecast_identity=feature_forecast_identity,
        live_prefix_receipt=live_prefix_receipt,
    )
    values = (
        forecast_key, model_version, artifact_hash, MODEL_FEATURE_CONTRACT_HASH,
        source_hash, session_id, family, trading_day.isoformat(),
        decision_horizon_minutes, available.isoformat(), recorded.isoformat(),
        float(reference_price), float(candidate_predicted_log_return), candidate_close,
        float(incumbent_predicted_close),
    )
    initialize_paper_forecast_ledger(path)
    with sqlite3.connect(Path(path)) as connection:
        connection.row_factory = sqlite3.Row
        activation = connection.execute(
            """
            SELECT * FROM paper_candidate_activations
            WHERE model_version=? AND artifact_sha256=?
            """,
            (model_version, artifact_hash),
        ).fetchone()
        if activation is None:
            raise ValueError(
                "paper forecast requires an immutable candidate activation"
            )
        activated = datetime.fromisoformat(str(activation["activated_at_utc"]))
        registered = datetime.fromisoformat(str(activation["registered_at_utc"]))
        if activated > available or registered > recorded:
            raise ValueError(
                "paper forecast predates its candidate activation campaign"
            )
        inserted = connection.execute(
            """
            INSERT OR IGNORE INTO paper_close_forecasts (
                forecast_key, model_version, artifact_sha256, feature_contract_hash,
                source_sha256, session_id, family_root, trading_date,
                decision_horizon_minutes, feature_available_at_utc, recorded_at_utc,
                reference_price, candidate_predicted_log_return,
                candidate_predicted_close, incumbent_predicted_close
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            values,
        ).rowcount == 1
        existing = connection.execute(
            "SELECT * FROM paper_close_forecasts WHERE forecast_key=?", (forecast_key,)
        ).fetchone()
        if existing is None:
            raise ValueError("conflicting paper forecast identity already exists")
        columns = [
            item[1]
            for item in connection.execute("PRAGMA table_info(paper_close_forecasts)")
        ]
        stored = dict(zip(columns, existing))
        proposed = dict(zip(columns[1:], values))
        conflicts = [
            key
            for key, value in proposed.items()
            if key != "recorded_at_utc" and stored.get(key) != value
        ]
        if conflicts:
            raise ValueError(
                f"conflicting paper forecast already exists: {', '.join(conflicts)}"
            )
        stored_feature = connection.execute(
            """
            SELECT * FROM paper_forecast_feature_payloads
            WHERE forecast_key=?
            """,
            (forecast_key,),
        ).fetchone()
        if stored_feature is None and not inserted:
            raise ValueError(
                "existing paper forecast has no immutable model feature payload"
            )
        connection.execute(
            """
            INSERT OR IGNORE INTO paper_forecast_feature_payloads (
                forecast_key, feature_payload_sha256, feature_payload_json
            ) VALUES (?,?,?)
            """,
            (forecast_key, feature_payload_sha256, feature_payload_json),
        )
        stored_feature = connection.execute(
            """
            SELECT * FROM paper_forecast_feature_payloads
            WHERE forecast_key=?
            """,
            (forecast_key,),
        ).fetchone()
        if stored_feature is None:
            raise ValueError("paper model feature payload was not recorded")
        feature_conflicts = [
            field
            for field, value in (
                ("feature_payload_sha256", feature_payload_sha256),
                ("feature_payload_json", feature_payload_json),
            )
            if stored_feature[field] != value
        ]
        if feature_conflicts:
            raise ValueError(
                "conflicting paper model feature payload already exists: "
                + ", ".join(feature_conflicts)
            )
    return forecast_key


def evaluate_paper_forecasts(
    path: str | Path,
    *,
    project_root: str | Path,
    model_version: str,
    verified_artifact_root: str | Path,
    expected_families: Iterable[str] = PRODUCTION_FAMILIES,
) -> PaperEvaluationReport:
    families = tuple(sorted({str(value).upper() for value in expected_families}))
    if set(families) != set(PRODUCTION_FAMILIES) or len(families) != 5:
        raise ValueError("paper evaluation requires exactly the five production families")
    with sqlite3.connect(Path(path)) as connection:
        connection.row_factory = sqlite3.Row
        activation_table = connection.execute(
            """
            SELECT 1 FROM sqlite_master
            WHERE type='table' AND name='paper_candidate_activations'
            """
        ).fetchone()
        if activation_table is None:
            raise ValueError("paper evidence has no immutable candidate activation")
        feature_table = connection.execute(
            """
            SELECT 1 FROM sqlite_master
            WHERE type='table' AND name='paper_forecast_feature_payloads'
            """
        ).fetchone()
        if feature_table is None:
            raise ValueError("paper evidence has no immutable model feature payloads")
        forecasts = connection.execute(
            """
            SELECT forecasts.*,
                   activations.activation_receipt_sha256 AS activation_receipt_sha256,
                   activations.candidate_package_sha256 AS candidate_package_sha256,
                   activations.activated_at_utc AS activated_at_utc,
                   activations.registered_at_utc AS activation_registered_at_utc,
                   features.feature_payload_sha256 AS feature_payload_sha256,
                   features.feature_payload_json AS feature_payload_json
            FROM paper_close_forecasts forecasts
            LEFT JOIN paper_candidate_activations activations
              ON activations.model_version=forecasts.model_version
             AND activations.artifact_sha256=forecasts.artifact_sha256
            LEFT JOIN paper_forecast_feature_payloads features
              ON features.forecast_key=forecasts.forecast_key
            WHERE forecasts.model_version=?
            ORDER BY forecasts.trading_date, forecasts.family_root
            """,
            (model_version,),
        ).fetchall()
    closes = load_scored_marketpin_closes(
        path,
        verified_artifact_root=verified_artifact_root,
        allowed_families=families,
    )
    close_by_key = {
        (str(row["family_root"]), str(row["trading_date"])): row
        for row in closes.to_dict(orient="records")
    }
    records = []
    for forecast in forecasts:
        row = dict(forecast)
        close = close_by_key.get(
            (str(row["family_root"]), str(row["trading_date"]))
        )
        if close is None:
            continue
        row.update(
            {
                "official_close": close["actual_close"],
                "close_source": close["close_source"],
                "close_source_reference": close["close_source_reference"],
                "close_source_artifact_sha256": close[
                    "close_source_artifact_sha256"
                ],
            }
        )
        records.append(row)
    if not records:
        raise ValueError("no paper forecasts have verified close outcomes")
    by_date: dict[str, list[str]] = {}
    for row in records:
        by_date.setdefault(str(row["trading_date"]), []).append(
            str(row["family_root"])
        )
    complete_dates = {
        day
        for day, roots in by_date.items()
        if len(roots) == 5 and set(roots) == set(families)
    }
    eligible = [row for row in records if str(row["trading_date"]) in complete_dates]
    if not eligible:
        raise ValueError("no paper session has forecasts and verified closes for every family")
    artifact_hashes = {str(row["artifact_sha256"]).lower() for row in eligible}
    if len(artifact_hashes) != 1:
        raise ValueError("paper evidence mixes artifact hashes for one model version")
    model_artifact_hash = next(iter(artifact_hashes))
    if not SHA256_PATTERN.fullmatch(model_artifact_hash):
        raise ValueError("paper evidence artifact SHA-256 is invalid")
    close_artifact_hashes: set[str] = set()
    activation_hashes: set[str] = set()
    candidate_package_hashes: set[str] = set()
    feature_rows: list[dict[str, float | None]] = []
    feature_payloads = []
    for row in eligible:
        family = str(row["family_root"]).upper()
        activation_hash = str(row["activation_receipt_sha256"]).lower()
        candidate_package_hash = str(row["candidate_package_sha256"]).lower()
        if (
            not SHA256_PATTERN.fullmatch(activation_hash)
            or not SHA256_PATTERN.fullmatch(candidate_package_hash)
        ):
            raise ValueError("paper activation receipt identity is invalid")
        activated_at = datetime.fromisoformat(str(row["activated_at_utc"]))
        registered_at = datetime.fromisoformat(
            str(row["activation_registered_at_utc"])
        )
        feature_at = datetime.fromisoformat(str(row["feature_available_at_utc"]))
        recorded_at = datetime.fromisoformat(str(row["recorded_at_utc"]))
        trading_day = date.fromisoformat(str(row["trading_date"]))
        decision_horizon = int(row["decision_horizon_minutes"])
        _cash_open, _analysis_due, cash_close, _stop = _market_times(trading_day)
        expected_feature_timestamp = (
            cash_close.timestamp() - decision_horizon * 60
        )
        if (
            activated_at > registered_at
            or activated_at > feature_at
            or registered_at > recorded_at
            or recorded_at < feature_at
            or (recorded_at - feature_at).total_seconds()
            > PAPER_FORECAST_MAX_LATENCY_SECONDS
            or decision_horizon != 15
            or abs(feature_at.timestamp() - expected_feature_timestamp) > 1.0
            or recorded_at > cash_close
        ):
            raise ValueError("paper forecast predates its activation campaign")
        if str(row["feature_contract_hash"]) != MODEL_FEATURE_CONTRACT_HASH:
            raise ValueError("paper forecast feature contract does not match")
        activation_hashes.add(activation_hash)
        candidate_package_hashes.add(candidate_package_hash)
        expected_feature_identity = {
            "forecast_key": str(row["forecast_key"]),
            "model_version": str(row["model_version"]),
            "artifact_sha256": str(row["artifact_sha256"]).lower(),
            "source_sha256": str(row["source_sha256"]).lower(),
            "session_id": str(row["session_id"]),
            "family_root": family,
            "trading_date": str(row["trading_date"]),
            "decision_horizon_minutes": int(row["decision_horizon_minutes"]),
            "feature_available_at_utc": str(row["feature_available_at_utc"]),
            "reference_price": float(row["reference_price"]),
            "incumbent_predicted_close": float(
                row["incumbent_predicted_close"]
            ),
            "feature_contract_hash": str(row["feature_contract_hash"]),
        }
        parsed_feature = parse_paper_feature_evidence(
            str(row["feature_payload_json"] or ""),
            expected_sha256=str(row["feature_payload_sha256"] or ""),
            expected_forecast_identity=expected_feature_identity,
        )
        feature_payloads.append(parsed_feature)
        feature_rows.append(parsed_feature.features)
        close_artifact_hash = str(row["close_source_artifact_sha256"]).lower()
        close_artifact_hashes.add(close_artifact_hash)

    if len(activation_hashes) != 1 or len(candidate_package_hashes) != 1:
        raise ValueError("paper evidence must bind exactly one activated candidate")
    activation_hash = next(iter(activation_hashes))
    candidate_package_hash = next(iter(candidate_package_hashes))
    campaign = audit_paper_campaign_coverage(
        [dict(row) for row in forecasts],
        project_root=project_root,
        activated_at_utc=str(eligible[0]["activated_at_utc"]),
        latest_counted_trading_date=max(complete_dates),
        expected_families=families,
    )
    prefix_replay = replay_verify_paper_feature_groups(
        list(campaign.payloads),
        project_root=project_root,
        market_db_path=path,
        recorded_at_by_payload=campaign.recorded_at_by_payload,
        expected_families=families,
    )
    candidate = resolve_paper_candidate(
        project_root,
        activation_receipt_sha256=activation_hash,
        candidate_package_sha256=candidate_package_hash,
        model_version=model_version,
        artifact_sha256=model_artifact_hash,
        activated_at_utc=str(eligible[0]["activated_at_utc"]),
    )
    recomputed_log_returns = recompute_paper_predictions(candidate, feature_rows)
    if len(recomputed_log_returns) != len(eligible):
        raise ValueError("paper model replay returned an unexpected prediction count")

    evidence_records: list[dict[str, object]] = []
    for row, recomputed_log_return in zip(eligible, recomputed_log_returns):
        reference_price = float(row["reference_price"])
        replayed_log_return = float(recomputed_log_return)
        replayed_close = float(reference_price * np.exp(replayed_log_return))
        claimed_log_return = float(row["candidate_predicted_log_return"])
        claimed_close = float(row["candidate_predicted_close"])
        if (
            not math.isclose(
                claimed_log_return,
                replayed_log_return,
                rel_tol=1e-12,
                abs_tol=1e-12,
            )
            or not math.isclose(
                claimed_close,
                replayed_close,
                rel_tol=1e-12,
                abs_tol=1e-12,
            )
        ):
            raise ValueError(
                "paper forecast does not match replay from its frozen artifact"
            )
        row["candidate_predicted_log_return"] = replayed_log_return
        row["candidate_predicted_close"] = replayed_close
        family = str(row["family_root"]).upper()
        close_artifact_hash = str(row["close_source_artifact_sha256"]).lower()
        evidence_records.append(
            {
                "forecast_key": str(row["forecast_key"]),
                "session_id": str(row["session_id"]),
                "trading_date": str(row["trading_date"]),
                "family_root": family,
                "feature_available_at_utc": str(row["feature_available_at_utc"]),
                "recorded_at_utc": str(row["recorded_at_utc"]),
                "decision_horizon_minutes": int(
                    row["decision_horizon_minutes"]
                ),
                "feature_contract_hash": str(row["feature_contract_hash"]),
                "source_sha256": str(row["source_sha256"]).lower(),
                "model_version": str(row["model_version"]),
                "artifact_sha256": str(row["artifact_sha256"]).lower(),
                "activation_receipt_sha256": str(
                    row["activation_receipt_sha256"]
                ).lower(),
                "candidate_package_sha256": str(
                    row["candidate_package_sha256"]
                ).lower(),
                "activated_at_utc": str(row["activated_at_utc"]),
                "activation_registered_at_utc": str(
                    row["activation_registered_at_utc"]
                ),
                "feature_payload_sha256": str(
                    row["feature_payload_sha256"]
                ).lower(),
                "feature_payload_json": str(row["feature_payload_json"]),
                "prefix_replay_receipt_sha256": (
                    prefix_replay.payload_receipt_sha256s[
                        str(row["feature_payload_sha256"]).lower()
                    ]
                ),
                "campaign_coverage_receipt_sha256": (
                    campaign.receipt_sha256
                ),
                "close_source_artifact_sha256": close_artifact_hash,
                "reference_price": reference_price,
                "candidate_predicted_log_return": replayed_log_return,
                "candidate_predicted_close": float(
                    row["candidate_predicted_close"]
                ),
                "incumbent_predicted_close": float(
                    row["incumbent_predicted_close"]
                ),
                "official_close": float(row["official_close"]),
            }
        )
    evidence_bytes = json.dumps(
        sorted(
            evidence_records,
            key=lambda item: (
                str(item["trading_date"]),
                str(item["family_root"]),
                str(item["session_id"]),
            ),
        ),
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    evidence_sha256 = hashlib.sha256(evidence_bytes).hexdigest()

    def mae(items: list[dict[str, object]], column: str) -> float:
        return float(np.mean([abs(float(row[column]) - float(row["official_close"])) for row in items]))

    metrics = []
    for family in families:
        selected = [row for row in eligible if row["family_root"] == family]
        metrics.append(
            PaperFamilyMetric(
                family_root=family, rows=len(selected), sessions=len(complete_dates),
                candidate_mae=mae(selected, "candidate_predicted_close"),
                incumbent_mae=mae(selected, "incumbent_predicted_close"),
            )
        )
    report = PaperEvaluationReport(
        model_version=model_version, artifact_sha256=model_artifact_hash,
        complete_sessions=len(complete_dates), eligible_rows=len(eligible),
        candidate_mae=mae(eligible, "candidate_predicted_close"),
        incumbent_mae=mae(eligible, "incumbent_predicted_close"),
        evidence_sha256=evidence_sha256,
        activation_receipt_sha256s=tuple(sorted(activation_hashes)),
        prefix_replay_receipt_sha256s=prefix_replay.receipt_sha256s,
        final_tape_source_sha256s=prefix_replay.final_source_sha256s,
        campaign_opportunities=campaign.opportunity_count,
        campaign_coverage_receipt_sha256=campaign.receipt_sha256,
        close_source_artifact_sha256s=tuple(sorted(close_artifact_hashes)),
        family_metrics=tuple(metrics),
    )
    retain_paper_promotion_evidence(
        project_root,
        row_evidence_sha256=evidence_sha256,
        row_evidence_bytes=evidence_bytes,
        prefix_replay=prefix_replay,
        campaign_coverage=campaign,
    )
    return report
