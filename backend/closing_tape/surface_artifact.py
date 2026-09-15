from __future__ import annotations

import hashlib
import io
import json
import math
import os
import re
import sqlite3
import stat
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable, Mapping

import pandas as pd

from .catalog_discovery import CatalogDiscovery, select_closing_tape_catalogs
from .contracts import (
    EVIDENCE_CONTRACT_VERSION,
    HISTORICAL_EVIDENCE_CONTRACT_VERSION,
    HISTORICAL_SOURCE_KIND,
    LIVE_SOURCE_KIND,
)
from .historical_import import verify_historical_bundle_manifest
from .integrity import inspect_dbn
from .pipeline import build_research_surface_dataset
from .sqlite_io import sqlite_read_only_uri
from .surface import MODEL_FEATURE_COLUMNS, MODEL_FEATURE_CONTRACT_HASH


SURFACE_ARTIFACT_FORMAT = "marketpin-closing-surface-parquet-v1"
SURFACE_REPLAY_VERIFICATION_CONTRACT = "marketpin-closing-surface-replay-v1"
MAX_MANIFEST_BYTES = 2 * 1024 * 1024
MAX_SURFACE_REPLAY_RECEIPT_BYTES = 16 * 1024 * 1024
SHA256_PATTERN = re.compile(r"[0-9a-f]{64}")
SURFACE_IDENTITY_COLUMNS = (
    "trading_date",
    "session_id",
    "feed_name",
    "family_root",
    "minute_utc",
    "feature_available_at_utc",
    "source_sha256",
    "capture_integrity_verified",
    "inference_method",
    "inference_version",
    "reference_price_tier",
    "reference_subscription_epoch_id",
    "reference_subscription_generation",
    "reference_price_epoch_eligible",
    "reference_price_epoch_status",
    "surface_feature_version",
    "feature_schema_hash",
)


@dataclass(frozen=True)
class FrozenResearchSurfaceArtifact:
    frame: pd.DataFrame
    manifest_path: Path
    parquet_path: Path
    artifact_sha256: str
    manifest: dict[str, object]


_REPLAY_VERIFIED_SURFACE_TOKEN = object()


@dataclass(frozen=True, init=False)
class ReplayVerifiedResearchSurfaceArtifact(FrozenResearchSurfaceArtifact):
    catalog_selection: CatalogDiscovery
    replay_verification: dict[str, object]
    replay_receipt_sha256: str
    manifest_sha256: str
    frame_semantic_sha256: str
    _verification_token: object = field(repr=False, compare=False)

    @property
    def surface_identity(self) -> ReplayVerifiedSurfaceIdentity:
        return ReplayVerifiedSurfaceIdentity(
            surface_artifact_sha256=self.artifact_sha256,
            surface_replay_receipt_sha256=self.replay_receipt_sha256,
        )


@dataclass(frozen=True)
class ReplayVerifiedSurfaceIdentity:
    surface_artifact_sha256: str
    surface_replay_receipt_sha256: str

    def to_dict(self) -> dict[str, str]:
        return {
            "surface_artifact_sha256": self.surface_artifact_sha256,
            "surface_replay_receipt_sha256": self.surface_replay_receipt_sha256,
        }


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _strict_json_bytes(payload: dict[str, object]) -> bytes:
    return (
        json.dumps(
            payload,
            sort_keys=True,
            indent=2,
            allow_nan=False,
            separators=(",", ": "),
        )
        + "\n"
    ).encode("utf-8")


def canonical_replay_receipt_sha256(payload: dict[str, object]) -> str:
    """Hash the strict replay receipt without trusting embedded status fields."""

    return hashlib.sha256(_strict_json_bytes(payload)).hexdigest()


def _publish_immutable_file(source: Path, destination: Path) -> None:
    """Copy one retained artifact without ever replacing a prior identity."""

    destination.parent.mkdir(parents=True, exist_ok=True)
    source_hash = _sha256_file(source)
    if destination.exists():
        if _sha256_file(destination) != source_hash:
            raise ValueError("conflicting immutable surface promotion evidence")
        return
    temporary = destination.with_name(
        f".{destination.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp"
    )
    try:
        digest = hashlib.sha256()
        with source.open("rb") as reader, temporary.open("xb") as writer:
            for chunk in iter(lambda: reader.read(1024 * 1024), b""):
                writer.write(chunk)
                digest.update(chunk)
            writer.flush()
            os.fsync(writer.fileno())
        if digest.hexdigest() != source_hash:
            raise ValueError("surface evidence changed while it was retained")
        try:
            os.link(temporary, destination)
        except FileExistsError:
            if _sha256_file(destination) != source_hash:
                raise ValueError("conflicting immutable surface promotion evidence") from None
    finally:
        if temporary.exists():
            temporary.unlink()


def retain_surface_promotion_evidence(
    artifact: ReplayVerifiedResearchSurfaceArtifact,
    *,
    models_dir: str | Path,
) -> None:
    """Retain the exact replay-authorized surface under canonical model paths."""

    identity = require_replay_verified_surface_identity(artifact)
    root = (Path(models_dir).resolve() / "promotion_evidence").resolve()
    surface_dir = (root / "surfaces").resolve()
    replay_dir = (root / "surface_replays").resolve()
    if surface_dir.parent != root or replay_dir.parent != root:
        raise ValueError("surface promotion evidence path escapes its canonical root")
    parquet_destination = surface_dir / f"{identity.surface_artifact_sha256}.parquet"
    manifest_destination = (
        surface_dir / f"{identity.surface_artifact_sha256}.manifest.json"
    )
    _publish_immutable_file(Path(artifact.parquet_path).resolve(), parquet_destination)
    _publish_immutable_file(Path(artifact.manifest_path).resolve(), manifest_destination)
    receipt_bytes = _strict_json_bytes(artifact.replay_verification)
    if hashlib.sha256(receipt_bytes).hexdigest() != identity.surface_replay_receipt_sha256:
        raise ValueError("surface replay receipt bytes do not match their identity")
    replay_destination = replay_dir / f"{identity.surface_replay_receipt_sha256}.json"
    replay_dir.mkdir(parents=True, exist_ok=True)
    if replay_destination.exists():
        if replay_destination.read_bytes() != receipt_bytes:
            raise ValueError("conflicting immutable surface replay receipt")
    else:
        temporary = replay_destination.with_name(
            f".{replay_destination.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp"
        )
        try:
            with temporary.open("xb") as stream:
                stream.write(receipt_bytes)
                stream.flush()
                os.fsync(stream.fileno())
            try:
                os.link(temporary, replay_destination)
            except FileExistsError:
                if replay_destination.read_bytes() != receipt_bytes:
                    raise ValueError("conflicting immutable surface replay receipt") from None
        finally:
            if temporary.exists():
                temporary.unlink()


def dataframe_semantic_sha256(frame: pd.DataFrame) -> str:
    """Hash one in-memory frame including schema, order, index, and values."""

    if not isinstance(frame, pd.DataFrame):
        raise ValueError("semantic frame digest requires a pandas DataFrame")
    metadata = {
        "columns": [str(column) for column in frame.columns],
        "dtypes": [str(dtype) for dtype in frame.dtypes],
        "index_names": [None if name is None else str(name) for name in frame.index.names],
        "index_dtype": str(frame.index.dtype),
        "rows": int(len(frame)),
    }
    digest = hashlib.sha256(_strict_json_bytes(metadata))
    try:
        row_hashes = pd.util.hash_pandas_object(
            frame,
            index=True,
            categorize=False,
        ).to_numpy(dtype="<u8", copy=False)
    except (TypeError, ValueError) as exc:
        raise ValueError("research frame cannot be hashed deterministically") from exc
    digest.update(row_hashes.tobytes(order="C"))
    return digest.hexdigest()


def _build_replay_verified_surface_artifact(
    *,
    frozen: FrozenResearchSurfaceArtifact,
    catalog_selection: CatalogDiscovery,
    replay_verification: dict[str, object],
    replay_receipt_sha256: str,
) -> ReplayVerifiedResearchSurfaceArtifact:
    """Create replay authority only inside the retained-source replay loader."""

    artifact = object.__new__(ReplayVerifiedResearchSurfaceArtifact)
    values = {
        "frame": frozen.frame,
        "manifest_path": frozen.manifest_path.resolve(),
        "parquet_path": frozen.parquet_path.resolve(),
        "artifact_sha256": frozen.artifact_sha256,
        "manifest": frozen.manifest,
        "catalog_selection": catalog_selection,
        "replay_verification": replay_verification,
        "replay_receipt_sha256": replay_receipt_sha256,
        "manifest_sha256": _sha256_file(frozen.manifest_path),
        "frame_semantic_sha256": dataframe_semantic_sha256(frozen.frame),
        "_verification_token": _REPLAY_VERIFIED_SURFACE_TOKEN,
    }
    for name, value in values.items():
        object.__setattr__(artifact, name, value)
    return artifact


def require_replay_verified_surface_identity(
    artifact: ReplayVerifiedResearchSurfaceArtifact,
) -> ReplayVerifiedSurfaceIdentity:
    """Rehash files and current semantics before accepting replay authority."""

    if (
        type(artifact) is not ReplayVerifiedResearchSurfaceArtifact
        or getattr(artifact, "_verification_token", None)
        is not _REPLAY_VERIFIED_SURFACE_TOKEN
    ):
        raise ValueError("a replay-verified research surface artifact is required")
    artifact_hash = str(artifact.artifact_sha256).lower()
    receipt_hash = str(artifact.replay_receipt_sha256).lower()
    receipt = artifact.replay_verification
    expected_receipt_fields = {
        "contract_version",
        "surface_artifact_sha256",
        "semantic_equality_verified",
        "max_price_age_seconds",
        "selected_sessions",
        "selected_catalog_paths",
        "source_evidence",
        "replay_surface_report",
    }
    if (
        not SHA256_PATTERN.fullmatch(artifact_hash)
        or not SHA256_PATTERN.fullmatch(receipt_hash)
        or not isinstance(receipt, dict)
        or set(receipt) != expected_receipt_fields
        or receipt.get("contract_version") != SURFACE_REPLAY_VERIFICATION_CONTRACT
        or receipt.get("semantic_equality_verified") is not True
        or str(receipt.get("surface_artifact_sha256") or "").lower()
        != artifact_hash
        or canonical_replay_receipt_sha256(receipt) != receipt_hash
    ):
        raise ValueError("research surface replay receipt identity is invalid")
    manifest_path = Path(artifact.manifest_path).resolve()
    parquet_path = Path(artifact.parquet_path).resolve()
    manifest_hash = str(artifact.manifest_sha256).lower()
    frame_hash = str(artifact.frame_semantic_sha256).lower()
    if (
        not SHA256_PATTERN.fullmatch(manifest_hash)
        or not SHA256_PATTERN.fullmatch(frame_hash)
        or not manifest_path.is_file()
        or _sha256_file(manifest_path) != manifest_hash
    ):
        raise ValueError("research surface manifest identity is invalid")
    reloaded = load_research_surface_artifact(manifest_path)
    if (
        reloaded.manifest_path.resolve() != manifest_path
        or reloaded.parquet_path.resolve() != parquet_path
        or reloaded.artifact_sha256 != artifact_hash
        or reloaded.manifest != artifact.manifest
        or dataframe_semantic_sha256(reloaded.frame) != frame_hash
        or dataframe_semantic_sha256(artifact.frame) != frame_hash
    ):
        raise ValueError("research surface files or current frame changed after replay verification")
    return ReplayVerifiedSurfaceIdentity(
        surface_artifact_sha256=artifact_hash,
        surface_replay_receipt_sha256=receipt_hash,
    )


def _surface_summary(frame: pd.DataFrame) -> dict[str, object]:
    if frame.empty:
        raise ValueError("research surface artifact cannot be empty")
    required = set(SURFACE_IDENTITY_COLUMNS) | set(MODEL_FEATURE_COLUMNS)
    missing = sorted(required - set(frame.columns))
    if missing:
        raise ValueError("research surface columns missing: " + ", ".join(missing))
    if not frame["capture_integrity_verified"].fillna(False).astype(bool).all():
        raise ValueError("research surface contains rows without verified capture integrity")
    source_hashes = tuple(sorted(frame["source_sha256"].astype(str).str.lower().unique()))
    if not source_hashes or any(not SHA256_PATTERN.fullmatch(value) for value in source_hashes):
        raise ValueError("research surface contains an invalid source SHA-256")
    schema_hashes = tuple(sorted(frame["feature_schema_hash"].astype(str).str.lower().unique()))
    if len(schema_hashes) != 1 or not SHA256_PATTERN.fullmatch(schema_hashes[0]):
        raise ValueError("research surface must contain one valid feature schema hash")
    identity = ["session_id", "feed_name", "family_root", "minute_utc"]
    if frame.duplicated(identity).any():
        raise ValueError("research surface contains duplicate session/family/minute rows")
    available = pd.to_datetime(frame["feature_available_at_utc"], utc=True, errors="coerce")
    if available.isna().any():
        raise ValueError("research surface contains an invalid feature availability timestamp")
    families = tuple(sorted(frame["family_root"].astype(str).str.upper().unique()))
    dates = tuple(sorted(frame["trading_date"].astype(str).unique()))
    subscription_identities = [
        {
            "subscription_epoch_id": str(epoch),
            "subscription_generation": int(generation),
        }
        for epoch, generation in sorted({
            (str(row.reference_subscription_epoch_id), int(row.reference_subscription_generation))
            for row in frame[
                frame["reference_price_epoch_eligible"].eq(True)
            ][
                ["reference_subscription_epoch_id", "reference_subscription_generation"]
            ].itertuples(index=False)
        })
    ]
    return {
        "row_count": int(len(frame)),
        "sessions": int(frame["session_id"].astype(str).nunique()),
        "trading_dates": dates,
        "family_roots": families,
        "source_sha256s": source_hashes,
        "reference_subscription_identities": subscription_identities,
        "feature_schema_hash": schema_hashes[0],
        "feature_available_min_utc": available.min().isoformat(),
        "feature_available_max_utc": available.max().isoformat(),
    }


def write_research_surface_artifact(
    frame: pd.DataFrame,
    *,
    artifact_dir: str | Path,
    report: Any,
    max_price_age_seconds: float,
) -> FrozenResearchSurfaceArtifact:
    """Write one immutable content-addressed Parquet surface and strict manifest."""
    if max_price_age_seconds <= 0:
        raise ValueError("max_price_age_seconds must be positive")
    summary = _surface_summary(frame)
    directory = Path(artifact_dir).resolve()
    directory.mkdir(parents=True, exist_ok=True)
    temporary = directory / f".surface.{uuid.uuid4().hex}.tmp.parquet"
    try:
        frame.to_parquet(temporary, index=False, engine="pyarrow", compression="zstd")
        with temporary.open("r+b") as stream:
            os.fsync(stream.fileno())
        artifact_hash = _sha256_file(temporary)
        parquet_path = directory / f"{artifact_hash}.parquet"
        if parquet_path.exists():
            if _sha256_file(parquet_path) != artifact_hash:
                raise ValueError("existing surface artifact conflicts with its content hash")
        else:
            os.replace(temporary, parquet_path)
        report_payload = report.to_dict() if hasattr(report, "to_dict") else dict(report)
        manifest: dict[str, object] = {
            "artifact_format": SURFACE_ARTIFACT_FORMAT,
            "artifact_sha256": artifact_hash,
            "artifact_bytes": parquet_path.stat().st_size,
            "parquet_file": parquet_path.name,
            "surface_columns": list(frame.columns),
            "model_feature_contract_hash": MODEL_FEATURE_CONTRACT_HASH,
            "model_feature_columns": list(MODEL_FEATURE_COLUMNS),
            "max_price_age_seconds": float(max_price_age_seconds),
            "surface_report": report_payload,
            **summary,
        }
        manifest_bytes = _strict_json_bytes(manifest)
        manifest_path = directory / f"{artifact_hash}.manifest.json"
        if manifest_path.exists():
            if manifest_path.read_bytes() != manifest_bytes:
                raise ValueError("existing surface manifest conflicts with the artifact identity")
        else:
            manifest_temporary = directory / f".{manifest_path.name}.{uuid.uuid4().hex}.tmp"
            try:
                with manifest_temporary.open("xb") as stream:
                    stream.write(manifest_bytes)
                    stream.flush()
                    os.fsync(stream.fileno())
                os.replace(manifest_temporary, manifest_path)
            finally:
                if manifest_temporary.exists():
                    manifest_temporary.unlink()
        return FrozenResearchSurfaceArtifact(
            frame=frame.copy(),
            manifest_path=manifest_path,
            parquet_path=parquet_path,
            artifact_sha256=artifact_hash,
            manifest=manifest,
        )
    finally:
        if temporary.exists():
            temporary.unlink()


def load_research_surface_artifact(
    manifest_path: str | Path,
) -> FrozenResearchSurfaceArtifact:
    """Rehash and validate an immutable surface before model evaluation."""
    manifest_file = Path(manifest_path).resolve()
    if not manifest_file.is_file():
        raise FileNotFoundError(manifest_file)
    manifest_size = manifest_file.stat().st_size
    if manifest_size <= 0 or manifest_size > MAX_MANIFEST_BYTES:
        raise ValueError("research surface manifest must be between 1 byte and 2 MiB")
    try:
        payload = json.loads(manifest_file.read_text(encoding="utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("research surface manifest must contain valid UTF-8 JSON") from exc
    if not isinstance(payload, dict):
        raise ValueError("research surface manifest must contain a top-level object")
    if payload.get("artifact_format") != SURFACE_ARTIFACT_FORMAT:
        raise ValueError("unsupported research surface artifact format")
    artifact_hash = str(payload.get("artifact_sha256") or "").strip().lower()
    if not SHA256_PATTERN.fullmatch(artifact_hash):
        raise ValueError("research surface artifact SHA-256 is invalid")
    if manifest_file.name != f"{artifact_hash}.manifest.json":
        raise ValueError("research surface manifest filename is not content-addressed")
    parquet_name = str(payload.get("parquet_file") or "").strip()
    if not parquet_name or Path(parquet_name).name != parquet_name:
        raise ValueError("research surface parquet path must be a local filename")
    if parquet_name != f"{artifact_hash}.parquet":
        raise ValueError("research surface parquet filename is not content-addressed")
    parquet_path = manifest_file.parent / parquet_name
    if not parquet_path.is_file():
        raise FileNotFoundError(parquet_path)
    try:
        parquet_bytes = parquet_path.read_bytes()
    except OSError as exc:
        raise ValueError("research surface artifact is unreadable") from exc
    if len(parquet_bytes) != int(payload.get("artifact_bytes") or -1):
        raise ValueError("research surface artifact byte count does not match the manifest")
    if hashlib.sha256(parquet_bytes).hexdigest() != artifact_hash:
        raise ValueError("research surface artifact bytes do not match the manifest SHA-256")
    if payload.get("model_feature_contract_hash") != MODEL_FEATURE_CONTRACT_HASH:
        raise ValueError("research surface model feature contract is not current")
    if tuple(payload.get("model_feature_columns") or ()) != MODEL_FEATURE_COLUMNS:
        raise ValueError("research surface model feature columns are not current")

    frame = pd.read_parquet(io.BytesIO(parquet_bytes), engine="pyarrow")
    if list(frame.columns) != list(payload.get("surface_columns") or ()):
        raise ValueError("research surface Parquet columns do not match the manifest")
    summary = _surface_summary(frame)
    for key, value in summary.items():
        manifest_value = payload.get(key)
        if isinstance(value, tuple):
            manifest_value = tuple(manifest_value or ())
        if manifest_value != value:
            raise ValueError(f"research surface {key} does not match the manifest")
    return FrozenResearchSurfaceArtifact(
        frame=frame,
        manifest_path=manifest_file,
        parquet_path=parquet_path,
        artifact_sha256=artifact_hash,
        manifest=payload,
    )


def _selected_session_claims(frame: pd.DataFrame) -> tuple[dict[str, str], ...]:
    columns = ("trading_date", "session_id", "feed_name", "source_sha256")
    claims = frame.loc[:, columns].copy()
    for column in columns:
        claims[column] = claims[column].astype(str)
    claims["source_sha256"] = claims["source_sha256"].str.lower()
    claims = claims.drop_duplicates().sort_values(list(columns)).reset_index(drop=True)
    if claims.empty:
        raise ValueError("research surface contains no selected session claims")
    session_identity = ["trading_date", "session_id", "feed_name"]
    if claims.duplicated(session_identity).any():
        raise ValueError("research surface session claim has multiple source SHA-256 values")
    if (claims["feed_name"] != "opra_options").any():
        raise ValueError("research surface contains a non-OPRA session claim")
    if not claims["source_sha256"].map(
        lambda value: bool(SHA256_PATTERN.fullmatch(value))
    ).all():
        raise ValueError("research surface session claim has an invalid source SHA-256")
    return tuple(
        {column: str(row[column]) for column in columns}
        for row in claims.to_dict(orient="records")
    )


def _catalog_tables(connection: sqlite3.Connection) -> set[str]:
    return {
        str(row[0])
        for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        )
    }


def _require_regular_single_link_file(path: Path, *, label: str) -> None:
    try:
        metadata = path.stat()
    except FileNotFoundError:
        raise FileNotFoundError(path) from None
    if not stat.S_ISREG(metadata.st_mode):
        raise ValueError(f"{label} is not a regular file: {path}")
    link_count = int(getattr(metadata, "st_nlink", 1) or 1)
    if link_count > 1:
        raise ValueError(
            f"{label} has multiple hard links and is not physically contained: "
            f"{path} (links={link_count})"
        )


def _load_catalog_session(
    catalog_path: Path,
    *,
    claim: Mapping[str, str],
) -> tuple[dict[str, object], dict[str, object]] | None:
    connection = sqlite3.connect(
        sqlite_read_only_uri(catalog_path),
        uri=True,
        timeout=10.0,
    )
    connection.row_factory = sqlite3.Row
    try:
        quick_check = connection.execute("PRAGMA quick_check").fetchone()
        if quick_check is None or str(quick_check[0]).lower() != "ok":
            raise ValueError(f"catalog quick_check failed: {catalog_path}")
        required_tables = {
            "tape_sessions",
            "tape_feed_status",
            "tape_finalization_runs",
        }
        missing_tables = sorted(required_tables - _catalog_tables(connection))
        if missing_tables:
            raise ValueError(
                "catalog is missing replay evidence tables "
                f"({catalog_path}): {', '.join(missing_tables)}"
            )
        session = connection.execute(
            """
            SELECT * FROM tape_sessions
            WHERE trading_date=? AND session_id=?
            """,
            (claim["trading_date"], claim["session_id"]),
        ).fetchone()
        if session is None:
            return None
        if str(dict(session).get("status") or "") != "complete":
            raise ValueError(
                "selected catalog session is not complete "
                f"({claim['trading_date']}/{claim['session_id']})"
            )
        feed = connection.execute(
            """
            SELECT * FROM tape_feed_status
            WHERE session_id=? AND feed_name=?
            """,
            (claim["session_id"], claim["feed_name"]),
        ).fetchone()
        if feed is None:
            return None
        feed_payload = dict(feed)
        required_feed_fields = {
            "dataset",
            "dbn_path",
            "source_kind",
            "evidence_contract_version",
            "operational_counters_applicable",
            "source_manifest_path",
            "source_components_json",
            "status",
            "complete",
            "sha256",
            "reconnect_count",
            "slow_reader_warnings",
            "provider_error_count",
            "unmapped_trade_records",
        }
        missing_feed_fields = sorted(required_feed_fields - set(feed_payload))
        if missing_feed_fields:
            raise ValueError(
                "selected catalog feed is missing source-verification fields: "
                + ", ".join(missing_feed_fields)
            )
        source_kind = str(feed_payload.get("source_kind") or "").strip().lower()
        expected_contract = {
            LIVE_SOURCE_KIND: EVIDENCE_CONTRACT_VERSION,
            HISTORICAL_SOURCE_KIND: HISTORICAL_EVIDENCE_CONTRACT_VERSION,
        }.get(source_kind)
        if expected_contract is None:
            raise ValueError(
                f"unsupported source kind for replay verification: {source_kind or '<missing>'}"
            )
        source_hash = str(feed_payload.get("sha256") or "").strip().lower()
        if source_hash != claim["source_sha256"]:
            raise ValueError(
                "surface session source claim does not match the selected catalog "
                f"({claim['trading_date']}/{claim['session_id']})"
            )
        if (
            str(feed_payload.get("dataset") or "") != "OPRA.PILLAR"
            or str(feed_payload.get("status") or "") != "complete"
            or int(feed_payload.get("complete") or 0) != 1
            or str(feed_payload.get("evidence_contract_version") or "")
            != expected_contract
        ):
            raise ValueError(
                "selected catalog feed is not complete current-contract OPRA evidence "
                f"({claim['trading_date']}/{claim['session_id']})"
            )
        operational_counters_applicable = bool(
            int(feed_payload.get("operational_counters_applicable") or 0)
        )
        if operational_counters_applicable != (source_kind == LIVE_SOURCE_KIND):
            raise ValueError("selected catalog source kind has inconsistent counter semantics")
        loss_counters = {
            "reconnect_count": int(feed_payload.get("reconnect_count") or 0),
            "slow_reader_warnings": int(
                feed_payload.get("slow_reader_warnings") or 0
            ),
            "provider_error_count": int(feed_payload.get("provider_error_count") or 0),
            "unmapped_trade_records": int(
                feed_payload.get("unmapped_trade_records") or 0
            ),
        }
        if any(loss_counters.values()):
            raise ValueError(
                "selected catalog feed records loss or unmapped evidence: "
                + ", ".join(
                    f"{field}={value}"
                    for field, value in loss_counters.items()
                    if value
                )
            )
        finalization = connection.execute(
            """
            SELECT * FROM tape_finalization_runs
            WHERE session_id=? AND feed_name=? AND source_sha256=?
              AND evidence_contract_version=? AND complete=1
            ORDER BY attempted_at_utc DESC LIMIT 1
            """,
            (
                claim["session_id"],
                claim["feed_name"],
                source_hash,
                expected_contract,
            ),
        ).fetchone()
        if finalization is None:
            raise ValueError(
                "selected catalog session has no passing immutable finalization audit "
                f"({claim['trading_date']}/{claim['session_id']})"
            )
        try:
            report = json.loads(str(finalization["report_json"]))
        except (KeyError, TypeError, json.JSONDecodeError) as exc:
            raise ValueError("selected catalog finalization report is invalid") from exc
        if not isinstance(report, dict):
            raise ValueError("selected catalog finalization report is not an object")
        if (
            str(report.get("source_sha256") or "").lower() != source_hash
            or ("complete" in report and not bool(report["complete"]))
        ):
            raise ValueError("selected catalog finalization report identity is inconsistent")
        return feed_payload, report
    finally:
        connection.close()


def _verify_live_source(
    catalog_path: Path,
    *,
    claim: Mapping[str, str],
    feed: Mapping[str, object],
    finalization_report: Mapping[str, object],
) -> dict[str, object]:
    source = Path(str(feed.get("dbn_path") or "")).resolve()
    day_directory = catalog_path.parent.resolve()
    try:
        source.relative_to(day_directory)
    except ValueError as exc:
        raise ValueError("live DBN source escapes its catalog trading-day directory") from exc
    _require_regular_single_link_file(source, label="live DBN source")
    expected_acks = int(feed.get("expected_subscription_acks") or 0)
    if expected_acks <= 0:
        raise ValueError("live DBN source has no expected subscription identity")
    integrity = inspect_dbn(
        source,
        require_trades=True,
        require_tcbbo=True,
        expected_subscription_acks=expected_acks,
    )
    if not integrity.local_file_intact:
        raise ValueError(
            "live DBN source failed replay integrity: "
            + ", ".join(integrity.incomplete_reasons)
        )
    if integrity.sha256 != claim["source_sha256"]:
        raise ValueError("live DBN bytes do not match the catalog source SHA-256")
    expected_counters = {
        "records_seen": integrity.records_seen,
        "trade_records": integrity.trade_records,
        "tcbbo_records": integrity.tcbbo_records,
        "tcbbo_timestamped_records": integrity.tcbbo_timestamped_records,
        "tcbbo_valid_nbbo_records": integrity.tcbbo_valid_nbbo_records,
        "definition_records": integrity.definition_records,
        "statistics_records": integrity.statistics_records,
        "subscription_acks": integrity.subscription_acks,
        "replay_completed": integrity.replay_completed,
        "file_bytes": integrity.file_bytes,
        "last_trade_event_ns": integrity.last_trade_event_ns or 0,
    }
    mismatches = [
        field
        for field, actual in expected_counters.items()
        if int(feed.get(field) or 0) != int(actual)
    ]
    if mismatches:
        raise ValueError(
            "live DBN replay counters do not match the selected catalog: "
            + ", ".join(mismatches)
        )
    report_integrity = finalization_report.get("integrity")
    if not isinstance(report_integrity, Mapping):
        raise ValueError("live finalization report has no integrity receipt")
    if (
        str(report_integrity.get("sha256") or "").lower() != integrity.sha256
        or int(report_integrity.get("file_bytes") or 0) != integrity.file_bytes
    ):
        raise ValueError("live finalization integrity receipt does not match retained bytes")
    return {
        "trading_date": claim["trading_date"],
        "session_id": claim["session_id"],
        "feed_name": claim["feed_name"],
        "source_kind": LIVE_SOURCE_KIND,
        "source_sha256": integrity.sha256,
        "source_path": str(source),
        "source_bytes": integrity.file_bytes,
        "records_seen": integrity.records_seen,
        "tcbbo_records": integrity.tcbbo_records,
    }


def verify_retained_live_catalog_source(
    catalog_path: str | Path,
    *,
    trading_date: str,
    session_id: str,
    feed_name: str,
    source_sha256: str,
) -> dict[str, object]:
    """Re-hash one finalized live source selected by immutable catalog identity."""

    catalog = Path(catalog_path).resolve()
    day = str(trading_date).strip()
    claim = {
        "trading_date": day,
        "session_id": str(session_id).strip(),
        "feed_name": str(feed_name).strip(),
        "source_sha256": str(source_sha256).lower(),
    }
    if (
        catalog.parent.name != day
        or not claim["session_id"]
        or not claim["feed_name"]
        or not SHA256_PATTERN.fullmatch(claim["source_sha256"])
    ):
        raise ValueError("live catalog source claim identity is invalid")
    loaded = _load_catalog_session(catalog, claim=claim)
    if loaded is None:
        raise ValueError("live catalog source claim does not resolve to a session/feed")
    feed, finalization_report = loaded
    if str(feed.get("source_kind") or "").strip().lower() != LIVE_SOURCE_KIND:
        raise ValueError("paper prefix replay requires finalized live DBN evidence")
    return _verify_live_source(
        catalog,
        claim=claim,
        feed=feed,
        finalization_report=finalization_report,
    )


def _verify_historical_source(
    *,
    claim: Mapping[str, str],
    feed: Mapping[str, object],
    finalization_report: Mapping[str, object],
) -> dict[str, object]:
    raw_manifest = str(feed.get("source_manifest_path") or feed.get("dbn_path") or "")
    if not raw_manifest:
        raise ValueError("historical catalog feed has no retained source manifest")
    manifest_path = Path(raw_manifest).resolve()
    _require_regular_single_link_file(manifest_path, label="historical source manifest")
    bundle = verify_historical_bundle_manifest(manifest_path)
    if (
        bundle.trading_date.isoformat() != claim["trading_date"]
        or bundle.bundle_sha256 != claim["source_sha256"]
    ):
        raise ValueError("historical bundle identity does not match the selected catalog")
    dbn_path = Path(str(feed.get("dbn_path") or "")).resolve()
    source_manifest_path = Path(str(feed.get("source_manifest_path") or "")).resolve()
    if dbn_path != bundle.manifest_path or source_manifest_path != bundle.manifest_path:
        raise ValueError("historical catalog source paths do not match the verified manifest")
    try:
        catalog_components = json.loads(str(feed.get("source_components_json") or "{}"))
    except json.JSONDecodeError as exc:
        raise ValueError("historical catalog component identity is invalid JSON") from exc
    verified_components = {
        schema: component.source_identity()
        for schema, component in sorted(bundle.components.items())
    }
    if catalog_components != verified_components:
        raise ValueError("historical catalog component identity does not match retained bytes")
    if (
        str(finalization_report.get("manifest_sha256") or "").lower()
        != bundle.manifest_sha256
        or str(finalization_report.get("bundle_sha256") or claim["source_sha256"]).lower()
        != claim["source_sha256"]
    ):
        raise ValueError("historical finalization receipt does not match retained manifest")
    return {
        "trading_date": claim["trading_date"],
        "session_id": claim["session_id"],
        "feed_name": claim["feed_name"],
        "source_kind": HISTORICAL_SOURCE_KIND,
        "source_sha256": bundle.bundle_sha256,
        "source_manifest_path": str(bundle.manifest_path),
        "source_manifest_sha256": bundle.manifest_sha256,
        "component_sha256s": {
            schema: component.file_sha256
            for schema, component in sorted(bundle.components.items())
        },
    }


def _resolve_and_verify_selected_sessions(
    catalog_paths: Iterable[Path],
    *,
    claims: tuple[dict[str, str], ...],
) -> tuple[tuple[Path, ...], tuple[dict[str, object], ...]]:
    catalog_paths = tuple(Path(path).resolve() for path in catalog_paths)
    matched_catalogs: set[Path] = set()
    evidence: list[dict[str, object]] = []
    for claim in claims:
        matches: list[tuple[Path, dict[str, object], dict[str, object]]] = []
        for catalog_path in catalog_paths:
            if catalog_path.parent.name != claim["trading_date"]:
                continue
            loaded = _load_catalog_session(catalog_path, claim=claim)
            if loaded is not None:
                matches.append((catalog_path, *loaded))
        if len(matches) != 1:
            raise ValueError(
                "surface session claim must resolve to exactly one selected catalog "
                f"({claim['trading_date']}/{claim['session_id']}; matches={len(matches)})"
            )
        catalog_path, feed, finalization_report = matches[0]
        source_kind = str(feed.get("source_kind") or "").strip().lower()
        if source_kind == LIVE_SOURCE_KIND:
            source_evidence = _verify_live_source(
                catalog_path,
                claim=claim,
                feed=feed,
                finalization_report=finalization_report,
            )
        elif source_kind == HISTORICAL_SOURCE_KIND:
            source_evidence = _verify_historical_source(
                claim=claim,
                feed=feed,
                finalization_report=finalization_report,
            )
        else:  # guarded in _load_catalog_session; retain a fail-closed branch.
            raise ValueError(f"unsupported source kind: {source_kind or '<missing>'}")
        source_evidence["catalog_path"] = str(catalog_path)
        evidence.append(source_evidence)
        matched_catalogs.add(catalog_path)
    return (
        tuple(sorted(matched_catalogs, key=lambda path: str(path).casefold())),
        tuple(evidence),
    )


def _filter_selected_sessions(
    frame: pd.DataFrame,
    claims: tuple[dict[str, str], ...],
) -> pd.DataFrame:
    if frame.empty:
        return frame.copy()
    keys = {
        (claim["trading_date"], claim["session_id"], claim["feed_name"])
        for claim in claims
    }
    observed = list(
        zip(
            frame["trading_date"].astype(str),
            frame["session_id"].astype(str),
            frame["feed_name"].astype(str),
        )
    )
    mask = pd.Series((key in keys for key in observed), index=frame.index)
    return frame.loc[mask].copy().reset_index(drop=True)


def _assert_exact_surface_semantics(
    claimed: pd.DataFrame,
    replayed: pd.DataFrame,
) -> None:
    if list(claimed.columns) != list(replayed.columns):
        raise ValueError("replayed research surface columns do not match the artifact")
    identity = ["session_id", "feed_name", "family_root", "minute_utc"]
    claimed_ordered = claimed.sort_values(identity).reset_index(drop=True)
    replayed_ordered = replayed.sort_values(identity).reset_index(drop=True)
    try:
        pd.testing.assert_frame_equal(
            claimed_ordered,
            replayed_ordered,
            check_dtype=False,
            check_exact=True,
            check_like=False,
            check_categorical=True,
        )
    except AssertionError as exc:
        first_line = str(exc).strip().splitlines()[0] if str(exc).strip() else "value mismatch"
        raise ValueError(
            "replayed research surface is not semantically identical to the artifact: "
            + first_line
        ) from exc


def load_replay_verified_research_surface_artifact(
    manifest_path: str | Path,
    *,
    project_root: str | Path,
    market_db_path: str | Path,
    explicit_catalogs: Iterable[str | Path] | None = None,
) -> ReplayVerifiedResearchSurfaceArtifact:
    """Verify retained evidence and exactly replay a frozen surface.

    The Parquet manifest is only a selection claim. Catalogs are resolved from
    the project configuration (or an explicit complete override), each selected
    source is independently rehashed/attested, and the selected sessions are
    rebuilt from read-only catalogs plus the point-in-time market database.
    """

    frozen = load_research_surface_artifact(manifest_path)
    raw_age = frozen.manifest.get("max_price_age_seconds")
    try:
        max_price_age_seconds = float(raw_age)
    except (TypeError, ValueError) as exc:
        raise ValueError("research surface max_price_age_seconds is invalid") from exc
    if not math.isfinite(max_price_age_seconds) or max_price_age_seconds <= 0:
        raise ValueError("research surface max_price_age_seconds must be finite and positive")

    selection = select_closing_tape_catalogs(
        project_root,
        explicit_catalogs=explicit_catalogs,
    )
    if selection.issues:
        raise ValueError(
            "catalog selection failed during surface replay verification: "
            + "; ".join(selection.issues)
        )
    claims = _selected_session_claims(frozen.frame)
    replay_catalogs, source_evidence = _resolve_and_verify_selected_sessions(
        selection.catalog_paths,
        claims=claims,
    )
    replayed, replay_report = build_research_surface_dataset(
        replay_catalogs,
        Path(market_db_path).resolve(),
        max_price_age_seconds=max_price_age_seconds,
    )
    selected_replay = _filter_selected_sessions(replayed, claims)
    _assert_exact_surface_semantics(frozen.frame, selected_replay)
    verification: dict[str, object] = {
        "contract_version": SURFACE_REPLAY_VERIFICATION_CONTRACT,
        "surface_artifact_sha256": frozen.artifact_sha256,
        "semantic_equality_verified": True,
        "max_price_age_seconds": max_price_age_seconds,
        "selected_sessions": [dict(claim) for claim in claims],
        "selected_catalog_paths": [str(path) for path in replay_catalogs],
        "source_evidence": [dict(item) for item in source_evidence],
        "replay_surface_report": replay_report.to_dict(),
    }
    replay_receipt_sha256 = canonical_replay_receipt_sha256(verification)
    return _build_replay_verified_surface_artifact(
        frozen=frozen,
        catalog_selection=selection,
        replay_verification=verification,
        replay_receipt_sha256=replay_receipt_sha256,
    )


def _stable_file_sha256(path: Path) -> tuple[str, int]:
    try:
        before = path.stat()
    except OSError as exc:
        raise ValueError(f"retained source is missing or unreadable: {path}") from exc
    _require_regular_single_link_file(path, label="retained source")
    digest = hashlib.sha256()
    try:
        with path.open("rb") as stream:
            for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                digest.update(chunk)
        after = path.stat()
    except OSError as exc:
        raise ValueError(f"retained source changed while it was hashed: {path}") from exc
    identity_before = (
        int(before.st_dev), int(before.st_ino), int(before.st_size), int(before.st_mtime_ns)
    )
    identity_after = (
        int(after.st_dev), int(after.st_ino), int(after.st_size), int(after.st_mtime_ns)
    )
    if identity_before != identity_after:
        raise ValueError(f"retained source changed while it was hashed: {path}")
    return digest.hexdigest(), int(after.st_size)


def validate_retained_surface_promotion_evidence(
    project_root: str | Path,
    *,
    surface_artifact_sha256: str,
    surface_replay_receipt_sha256: str,
    source_sha256s: tuple[str, ...],
    market_db_path: str | Path,
) -> dict[str, object]:
    """Resolve and re-hash the frozen surface, replay receipt, and raw sources."""

    artifact_hash = str(surface_artifact_sha256 or "").lower()
    receipt_hash = str(surface_replay_receipt_sha256 or "").lower()
    expected_sources = tuple(sorted(source_sha256s))
    if (
        not SHA256_PATTERN.fullmatch(artifact_hash)
        or not SHA256_PATTERN.fullmatch(receipt_hash)
        or not expected_sources
        or expected_sources != tuple(sorted(set(expected_sources)))
        or any(not SHA256_PATTERN.fullmatch(value) for value in expected_sources)
    ):
        raise ValueError("surface promotion evidence identity is invalid")
    root = (
        Path(project_root).resolve() / "models" / "promotion_evidence"
    ).resolve()
    surface_dir = (root / "surfaces").resolve()
    replay_dir = (root / "surface_replays").resolve()
    manifest_path = surface_dir / f"{artifact_hash}.manifest.json"
    replay_path = replay_dir / f"{receipt_hash}.json"
    if not manifest_path.is_file():
        raise ValueError("retained research surface manifest is missing")
    frozen = load_research_surface_artifact(manifest_path)
    manifest_sources = tuple(
        sorted(str(value).lower() for value in frozen.manifest.get("source_sha256s", ()))
    )
    if frozen.artifact_sha256 != artifact_hash or manifest_sources != expected_sources:
        raise ValueError("retained research surface provenance does not match the model")
    if not replay_path.is_file():
        raise ValueError("retained surface replay receipt is missing")
    try:
        receipt_bytes = replay_path.read_bytes()
    except OSError as exc:
        raise ValueError("retained surface replay receipt is unreadable") from exc
    if (
        not receipt_bytes
        or len(receipt_bytes) > MAX_SURFACE_REPLAY_RECEIPT_BYTES
        or hashlib.sha256(receipt_bytes).hexdigest() != receipt_hash
    ):
        raise ValueError("retained surface replay receipt SHA-256 does not match")
    try:
        receipt = json.loads(receipt_bytes.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("retained surface replay receipt is not valid UTF-8 JSON") from exc
    expected_receipt_fields = {
        "contract_version", "surface_artifact_sha256", "semantic_equality_verified",
        "max_price_age_seconds", "selected_sessions", "selected_catalog_paths",
        "source_evidence", "replay_surface_report",
    }
    if (
        not isinstance(receipt, dict)
        or set(receipt) != expected_receipt_fields
        or receipt_bytes != _strict_json_bytes(receipt)
        or receipt.get("contract_version") != SURFACE_REPLAY_VERIFICATION_CONTRACT
        or receipt.get("semantic_equality_verified") is not True
        or str(receipt.get("surface_artifact_sha256") or "").lower() != artifact_hash
    ):
        raise ValueError("retained surface replay receipt contract is invalid")

    selection = select_closing_tape_catalogs(project_root)
    if selection.issues:
        raise ValueError(
            "catalog selection failed while resolving retained sources: "
            + "; ".join(selection.issues)
        )
    raw_selected_catalogs = receipt["selected_catalog_paths"]
    if not isinstance(raw_selected_catalogs, list):
        raise ValueError("surface replay catalogs are outside canonical retained roots")
    selected_catalogs = [Path(str(value)).resolve() for value in raw_selected_catalogs]
    if (
        selected_catalogs != sorted(set(selected_catalogs), key=lambda path: str(path).casefold())
        or any(path not in selection.catalog_paths for path in selected_catalogs)
    ):
        raise ValueError("surface replay catalogs are outside canonical retained roots")
    sessions = receipt.get("selected_sessions")
    evidence = receipt.get("source_evidence")
    if not isinstance(sessions, list) or not isinstance(evidence, list) or len(sessions) != len(evidence):
        raise ValueError("surface replay source evidence is incomplete")
    session_fields = {"trading_date", "session_id", "feed_name", "source_sha256"}
    session_by_key: dict[tuple[str, str, str], dict[str, str]] = {}
    normalized_sessions: list[dict[str, str]] = []
    for session in sessions:
        if not isinstance(session, dict) or set(session) != session_fields:
            raise ValueError("surface replay session claim fields are invalid")
        key = (
            str(session["trading_date"]), str(session["session_id"]),
            str(session["feed_name"]),
        )
        source_hash = str(session["source_sha256"] or "").lower()
        if key in session_by_key or source_hash not in expected_sources:
            raise ValueError("surface replay session claim identity is invalid")
        normalized = {
            "trading_date": key[0],
            "session_id": key[1],
            "feed_name": key[2],
            "source_sha256": source_hash,
        }
        session_by_key[key] = normalized
        normalized_sessions.append(normalized)
    artifact_claims = _selected_session_claims(frozen.frame)
    if tuple(normalized_sessions) != artifact_claims:
        raise ValueError("surface replay session claims do not match the retained surface")

    resolved_catalogs, resolved_evidence = _resolve_and_verify_selected_sessions(
        selection.catalog_paths,
        claims=tuple(normalized_sessions),
    )
    if tuple(selected_catalogs) != resolved_catalogs:
        raise ValueError("surface replay catalogs do not exactly match resolved sessions")
    if [dict(item) for item in resolved_evidence] != evidence:
        raise ValueError(
            "surface replay source evidence does not reproduce from retained catalogs"
        )
    observed_sources = {
        str(item["source_sha256"]).lower() for item in resolved_evidence
    }
    if tuple(sorted(observed_sources)) != expected_sources:
        raise ValueError("surface replay sources do not cover the promoted training evidence")
    try:
        max_price_age_seconds = float(receipt["max_price_age_seconds"])
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError("surface replay max price age is invalid") from exc
    if (
        not math.isfinite(max_price_age_seconds)
        or max_price_age_seconds <= 0
        or max_price_age_seconds
        != float(frozen.manifest.get("max_price_age_seconds") or 0)
    ):
        raise ValueError("surface replay max price age does not match the artifact")
    replayed, replay_report = build_research_surface_dataset(
        resolved_catalogs,
        Path(market_db_path).resolve(),
        max_price_age_seconds=max_price_age_seconds,
    )
    selected_replay = _filter_selected_sessions(replayed, artifact_claims)
    _assert_exact_surface_semantics(frozen.frame, selected_replay)
    if replay_report.to_dict() != receipt["replay_surface_report"]:
        raise ValueError("surface replay report does not reproduce from retained catalogs")
    return {
        "artifact_sha256": artifact_hash,
        "replay_receipt_sha256": receipt_hash,
        "source_files": len(resolved_evidence),
    }
