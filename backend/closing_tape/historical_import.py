from __future__ import annotations

import hashlib
import json
import re
import shutil
import sqlite3
import stat
import tempfile
from collections.abc import Sequence as SequenceABC
from dataclasses import asdict, dataclass, replace
from datetime import date, datetime, time, timedelta
from pathlib import Path
from typing import Mapping, Sequence

from .catalog import TapeCatalog, _utcnow
from .compute_guard import find_active_capture_sessions, require_compute_window
from .config import DEFAULT_OPTION_PARENTS, UTC, _market_times, build_session_config
from .contracts import (
    HISTORICAL_EVIDENCE_CONTRACT_VERSION,
    HISTORICAL_SOURCE_KIND,
)
from .definition_replay import replay_instrument_definitions
from .integrity import TapeIntegrityReport, inspect_dbn
from .oi_replay import replay_open_interest
from .replay import (
    build_minute_rows,
    persist_contract_minute_rows,
    persist_minute_rows,
)


HISTORICAL_BUNDLE_VERSION = "marketpin-databento-historical-bundle-v2"
HISTORICAL_REQUEST_VERSION = "marketpin-databento-historical-request-v1"
DATASET = "OPRA.PILLAR"
SUPPORTED_SCHEMAS = ("definition", "statistics", "tcbbo")
PRODUCTION_FAMILIES = ("SPX", "NDX", "RUT", "VIX", "SPY")
SHA256_RE = re.compile(r"[0-9a-f]{64}")
IMPORT_REPORT_VERSION = "marketpin-historical-import-v1"
_DBN_RECORD_BYTES = {"definition": 520, "statistics": 80, "tcbbo": 80}
_WORKING_SPACE_BUFFER_BYTES = 512 * 1024**2
_CATALOG_STORAGE_AMPLIFICATION = 4
_MINIMUM_WAL_HEADROOM_BYTES = 512 * 1024**2


@dataclass(frozen=True)
class HistoricalComponent:
    schema: str
    path: Path
    file_bytes: int
    file_sha256: str
    requested_start_utc: str
    requested_end_utc: str
    validation: Mapping[str, object]

    def source_identity(self) -> dict[str, object]:
        return {
            "file": str(self.path),
            "file_bytes": self.file_bytes,
            "file_sha256": self.file_sha256,
            "requested_start_utc": self.requested_start_utc,
            "requested_end_utc": self.requested_end_utc,
        }


@dataclass(frozen=True)
class VerifiedHistoricalBundle:
    manifest_path: Path
    manifest_sha256: str
    trading_date: date
    bundle_sha256: str
    request_sha256: str
    provenance_sha256: str
    attestation_sha256: str
    components: Mapping[str, HistoricalComponent]
    payload: Mapping[str, object]

    def to_dict(self) -> dict[str, object]:
        return {
            "manifest_path": str(self.manifest_path),
            "manifest_sha256": self.manifest_sha256,
            "trading_date": self.trading_date.isoformat(),
            "bundle_sha256": self.bundle_sha256,
            "request_sha256": self.request_sha256,
            "provenance_sha256": self.provenance_sha256,
            "attestation_sha256": self.attestation_sha256,
            "components": {
                schema: component.source_identity()
                for schema, component in sorted(self.components.items())
            },
        }


@dataclass(frozen=True)
class HistoricalImportResult:
    status: str
    session_id: str
    trading_date: str
    catalog_path: str
    source_sha256: str
    manifest_sha256: str
    feature_hash: str | None
    finalization_run_key: str | None
    observed_minute_rows: int
    inferred_minute_rows: int
    observed_contract_minute_rows: int
    inferred_contract_minute_rows: int
    open_interest_observations: int
    definition_observations: int

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


def _estimated_working_bytes(bundle: VerifiedHistoricalBundle) -> int:
    raw_records = sum(
        int(component.validation["record_count"]) * _DBN_RECORD_BYTES[schema]
        for schema, component in bundle.components.items()
    )
    compressed = sum(component.file_bytes for component in bundle.components.values())
    return raw_records + compressed + _WORKING_SPACE_BUFFER_BYTES


def _estimated_retained_catalog_bytes(bundle: VerifiedHistoricalBundle) -> int:
    """Conservatively estimate retained SQLite rows and indexes from source counts."""

    raw_records = sum(
        int(component.validation["record_count"]) * _DBN_RECORD_BYTES[schema]
        for schema, component in bundle.components.items()
    )
    compressed = sum(component.file_bytes for component in bundle.components.values())
    # The catalog retains immutable definition/OI observations plus separate
    # observed and inferred minute ledgers and their indexes. Four times the
    # decoded record footprint is deliberately above the largest retained-day
    # amplification observed during the historical replay pilot.
    return raw_records * _CATALOG_STORAGE_AMPLIFICATION + compressed


def _estimated_wal_headroom_bytes(estimated_catalog_growth_bytes: int) -> int:
    return max(
        _MINIMUM_WAL_HEADROOM_BYTES,
        int(estimated_catalog_growth_bytes) // 4,
    )


def _disk_usage_probe_path(target_directory: str | Path) -> Path:
    """Resolve the nearest existing ancestor without creating the destination."""

    try:
        candidate = Path(target_directory).resolve()
        while True:
            try:
                candidate.stat()
                return candidate
            except FileNotFoundError:
                parent = candidate.parent
                if parent == candidate:
                    raise RuntimeError(
                        "historical import destination volume is unavailable: "
                        f"{target_directory}"
                    )
                candidate = parent
    except RuntimeError:
        raise
    except OSError as exc:
        raise RuntimeError(
            "historical import cannot inspect destination path "
            f"{target_directory}: {type(exc).__name__}: {exc}"
        ) from exc


def _free_bytes_for_target(target_directory: str | Path) -> tuple[Path, int]:
    probe = _disk_usage_probe_path(target_directory)
    try:
        free_bytes = int(shutil.disk_usage(probe).free)
    except OSError as exc:
        raise RuntimeError(
            f"historical import cannot inspect destination volume at {probe}: "
            f"{type(exc).__name__}: {exc}"
        ) from exc
    return probe, free_bytes


def _same_storage_volume(first: Path, second: Path) -> bool:
    try:
        return int(first.stat().st_dev) == int(second.stat().st_dev)
    except OSError:
        # Capacity safety wins over an optimistic separate-volume assumption.
        return True


def _catalog_has_nonempty_wal(catalog_path: str | Path) -> bool:
    try:
        wal_path = Path(f"{Path(catalog_path).resolve()}-wal")
        return wal_path.is_file() and wal_path.stat().st_size > 0
    except OSError:
        return True


def _catalog_capacity_requirement(
    *,
    minimum_reserve_bytes: int,
    estimated_working_bytes: int,
    estimated_catalog_growth_bytes: int,
    estimated_wal_headroom_bytes: int,
    catalog_probe: Path,
    temporary_probe: Path,
    incremental_import_required: bool = True,
) -> dict[str, int | bool]:
    shared_volume = _same_storage_volume(catalog_probe, temporary_probe)
    estimated_catalog_growth = (
        int(estimated_catalog_growth_bytes) if incremental_import_required else 0
    )
    estimated_wal_headroom = (
        int(estimated_wal_headroom_bytes) if incremental_import_required else 0
    )
    shared_temporary_bytes = (
        int(estimated_working_bytes)
        if incremental_import_required and shared_volume
        else 0
    )
    required_free = 0
    if incremental_import_required:
        required_free = (
            int(minimum_reserve_bytes)
            + estimated_catalog_growth
            + estimated_wal_headroom
            + shared_temporary_bytes
        )
    return {
        "minimum_reserve_bytes": int(minimum_reserve_bytes),
        "estimated_catalog_growth_bytes": estimated_catalog_growth,
        "estimated_wal_headroom_bytes": estimated_wal_headroom,
        "shared_volume_temporary_bytes": shared_temporary_bytes,
        "catalog_and_temporary_share_volume": shared_volume,
        "required_free_bytes": required_free,
    }


def _canonical_hash(payload: Mapping[str, object]) -> str:
    encoded = json.dumps(
        dict(payload),
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _sha256(path: Path, chunk_size: int = 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(chunk_size), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _require_sha256(value: object, field: str) -> str:
    normalized = str(value or "").lower()
    if not SHA256_RE.fullmatch(normalized):
        raise ValueError(f"{field} is not a lowercase SHA-256")
    return normalized


def _iso_utc(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _metadata_iso(value: object, field: str) -> str:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            raise ValueError(f"DBN metadata {field} is timezone-naive")
        return _iso_utc(value)
    try:
        timestamp_ns = int(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError(f"DBN metadata {field} is invalid") from exc
    if timestamp_ns < 0:
        raise ValueError(f"DBN metadata {field} is negative")
    seconds, nanoseconds = divmod(timestamp_ns, 1_000_000_000)
    observed = datetime.fromtimestamp(seconds, tz=UTC).replace(
        microsecond=nanoseconds // 1_000
    )
    return _iso_utc(observed)


def _mapping_summary(
    mappings: Mapping[object, object], trading_day: date
) -> tuple[str, dict[int, str]]:
    digest = hashlib.sha256()
    active_roots: dict[int, str] = {}
    string_keys = {str(key): key for key in mappings}
    for raw_symbol in sorted(string_keys):
        segments = mappings[string_keys[raw_symbol]]
        if not isinstance(segments, SequenceABC) or isinstance(segments, (str, bytes)):
            raise ValueError(f"DBN mapping entry is malformed for {raw_symbol}")
        normalized: list[tuple[str, str, str]] = []
        for segment in segments:
            if not isinstance(segment, Mapping):
                raise ValueError(f"DBN mapping segment is malformed for {raw_symbol}")
            try:
                mapped_symbol = str(segment["symbol"])
                start_date = date.fromisoformat(str(segment["start_date"]))
                end_date = date.fromisoformat(str(segment["end_date"]))
                instrument_id = int(mapped_symbol)
            except (KeyError, TypeError, ValueError) as exc:
                raise ValueError(f"DBN mapping segment is invalid for {raw_symbol}") from exc
            if end_date <= start_date:
                raise ValueError(f"DBN mapping interval is invalid for {raw_symbol}")
            normalized.append(
                (start_date.isoformat(), end_date.isoformat(), mapped_symbol)
            )
            if start_date <= trading_day < end_date:
                active_roots[instrument_id] = raw_symbol[:6].strip().upper()
        digest.update(raw_symbol.encode("utf-8"))
        digest.update(b"\0")
        for values in sorted(normalized):
            digest.update("|".join(values).encode("utf-8"))
            digest.update(b"\0")
    return digest.hexdigest(), active_roots


def _component_identity(component: Mapping[str, object]) -> dict[str, object]:
    validation = component.get("validation")
    if not isinstance(validation, Mapping):
        raise ValueError("historical component contains no validation summary")
    return {
        "schema": str(component["schema"]),
        "requested_start_utc": str(component["requested_start_utc"]),
        "requested_end_utc": str(component["requested_end_utc"]),
        "file": str(component["file"]),
        "file_bytes": int(component["file_bytes"]),
        "file_sha256": str(component["file_sha256"]),
        "validation": dict(validation),
    }


def _bundle_identity(payload: Mapping[str, object]) -> dict[str, object]:
    components = payload.get("components")
    if not isinstance(components, list):
        raise ValueError("historical bundle components are malformed")
    identity: dict[str, object] = {
        "version": HISTORICAL_BUNDLE_VERSION,
        "dataset": DATASET,
        "trading_date": str(payload.get("trading_date") or ""),
        "request_sha256": str(payload.get("request_sha256") or ""),
        "components": [
            _component_identity(item)
            for item in sorted(components, key=lambda value: str(value["schema"]))
        ],
    }
    legacy = payload.get("legacy_evidence")
    if legacy is not None:
        if not isinstance(legacy, Mapping):
            raise ValueError("historical legacy evidence link is malformed")
        identity["legacy_evidence"] = dict(legacy)
    return identity


def _provenance_identity(payload: Mapping[str, object]) -> dict[str, object]:
    planning = payload.get("planning")
    acquisition = payload.get("acquisition")
    if not isinstance(planning, Mapping) or not isinstance(acquisition, Mapping):
        raise ValueError("historical provenance blocks are malformed")
    return {
        "source_kind": payload.get("source_kind"),
        "provider_condition": payload.get("provider_condition"),
        "provider_condition_last_modified_date": payload.get(
            "provider_condition_last_modified_date"
        ),
        "provider_condition_at_attestation": payload.get(
            "provider_condition_at_attestation"
        ),
        "provider_condition_last_modified_date_at_attestation": payload.get(
            "provider_condition_last_modified_date_at_attestation"
        ),
        "planning": dict(planning),
        "acquisition": dict(acquisition),
    }


def _canonical_windows(trading_day: date) -> list[dict[str, str]]:
    cash_open, _analysis_due, _cash_close, stop_due = _market_times(trading_day)
    midnight = datetime.combine(trading_day, time.min, tzinfo=UTC)
    next_midnight = midnight + timedelta(days=1)
    return [
        {
            "schema": "definition",
            "start_utc": _iso_utc(midnight),
            "end_utc": _iso_utc(next_midnight),
        },
        {
            "schema": "statistics",
            "start_utc": _iso_utc(midnight),
            "end_utc": _iso_utc(next_midnight),
        },
        {
            "schema": "tcbbo",
            "start_utc": _iso_utc(cash_open),
            "end_utc": _iso_utc(stop_due),
        },
    ]


def _resolve_component(day_dir: Path, raw_name: object) -> Path:
    path = (day_dir / str(raw_name or "")).resolve()
    try:
        path.relative_to(day_dir)
    except ValueError as exc:
        raise ValueError("historical component path escapes its day directory") from exc
    return path


def _validate_stored_component(
    *,
    schema: str,
    item: Mapping[str, object],
    parents: Sequence[str],
) -> None:
    validation = item.get("validation")
    if not isinstance(validation, Mapping):
        raise ValueError(f"historical {schema} component has no validation summary")
    required_identity = {
        "dbn_dataset": DATASET,
        "dbn_schema": schema,
        "dbn_compression": "zstd",
        "dbn_stype_in": "parent",
        "dbn_stype_out": "instrument_id",
        "dbn_start_utc": str(item["requested_start_utc"]),
        "dbn_end_utc": str(item["requested_end_utc"]),
    }
    for field, expected in required_identity.items():
        if validation.get(field) != expected:
            raise ValueError(f"historical {schema} validation {field} mismatch")
    if tuple(validation.get("dbn_symbols") or ()) != tuple(parents):
        raise ValueError(f"historical {schema} validation symbols mismatch")
    _require_sha256(validation.get("dbn_mapping_sha256"), f"{schema} mapping hash")
    if int(validation.get("record_count") or 0) <= 0:
        raise ValueError(f"historical {schema} component has no records")
    if validation.get("family_coverage_complete") is not True:
        raise ValueError(f"historical {schema} family coverage is incomplete")
    if list(validation.get("missing_record_families") or ()):
        raise ValueError(f"historical {schema} component is missing option families")
    if schema == "definition":
        if int(validation.get("option_definition_count") or 0) <= 0:
            raise ValueError("historical definition component has no option definitions")
    elif schema == "statistics":
        if list(validation.get("missing_open_interest_families") or ()):
            raise ValueError("historical statistics component has incomplete open interest")
        if int(validation.get("undefined_open_interest_record_count") or 0) != 0:
            raise ValueError("historical statistics component has undefined open interest")
        if not dict(validation.get("open_interest_record_counts") or {}):
            raise ValueError("historical statistics component has no open-interest records")
    elif schema == "tcbbo":
        if validation.get("two_sided_tcbbo_coverage_pass") is not True:
            raise ValueError("historical TCBBO two-sided NBBO coverage failed")
        coverage = float(validation.get("two_sided_tcbbo_coverage") or 0.0)
        if coverage < 0.95:
            raise ValueError("historical TCBBO two-sided NBBO coverage is below 95 percent")


def verify_historical_bundle_manifest(
    manifest_path: str | Path,
) -> VerifiedHistoricalBundle:
    """Verify a v2 historical bundle without importing or mutating a catalog."""

    path = Path(manifest_path).resolve()
    if not path.is_file():
        raise FileNotFoundError(path)
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, Mapping):
        raise ValueError("historical bundle manifest root must be an object")
    if payload.get("version") != HISTORICAL_BUNDLE_VERSION:
        raise ValueError("historical bundle version is unsupported")
    if payload.get("dataset") != DATASET or payload.get("status") != "complete":
        raise ValueError("historical bundle is not complete OPRA.PILLAR evidence")
    if payload.get("source_kind") != HISTORICAL_SOURCE_KIND:
        raise ValueError("historical bundle source kind is unsupported")

    try:
        trading_day = date.fromisoformat(str(payload["trading_date"]))
    except (KeyError, ValueError) as exc:
        raise ValueError("historical bundle trading date is invalid") from exc
    request = payload.get("request")
    if not isinstance(request, Mapping):
        raise ValueError("historical bundle contains no request identity")
    if (
        request.get("version") != HISTORICAL_REQUEST_VERSION
        or request.get("dataset") != DATASET
        or request.get("stype_in") != "parent"
        or request.get("stype_out") != "instrument_id"
        or request.get("range_filter") != "ts_recv"
        or request.get("encoding") != "dbn"
        or request.get("compression") != "zstd"
    ):
        raise ValueError("historical request contract is unsupported")
    parents = request.get("parents")
    if tuple(parents or ()) != tuple(DEFAULT_OPTION_PARENTS):
        raise ValueError("historical bundle does not contain the canonical nine parents")
    request_sha256 = _require_sha256(payload.get("request_sha256"), "request_sha256")
    if _canonical_hash(request) != request_sha256:
        raise ValueError("historical request identity hash mismatch")

    components = payload.get("components")
    if not isinstance(components, list) or len(components) != len(SUPPORTED_SCHEMAS):
        raise ValueError("historical bundle must contain exactly three components")
    day_dir = path.parent.resolve()
    verified_components: dict[str, HistoricalComponent] = {}
    mapping_hashes: set[str] = set()
    component_windows: list[dict[str, str]] = []
    for raw_item in components:
        if not isinstance(raw_item, Mapping):
            raise ValueError("historical component must be an object")
        schema = str(raw_item.get("schema") or "")
        if schema not in SUPPORTED_SCHEMAS or schema in verified_components:
            raise ValueError(f"invalid or duplicate historical component schema: {schema}")
        source = _resolve_component(day_dir, raw_item.get("file"))
        if not source.is_file():
            raise FileNotFoundError(source)
        try:
            expected_bytes = int(raw_item["file_bytes"])
        except (KeyError, TypeError, ValueError) as exc:
            raise ValueError(f"historical {schema} component size is malformed") from exc
        expected_hash = _require_sha256(
            raw_item.get("file_sha256"), f"{schema} file_sha256"
        )
        if expected_bytes <= 0 or source.stat().st_size != expected_bytes:
            raise ValueError(f"historical component size mismatch: {schema}")
        if _sha256(source) != expected_hash:
            raise ValueError(f"historical component hash mismatch: {schema}")
        _validate_stored_component(schema=schema, item=raw_item, parents=tuple(parents))
        validation = dict(raw_item["validation"])
        mapping_hashes.add(str(validation["dbn_mapping_sha256"]))
        start = str(raw_item.get("requested_start_utc") or "")
        end = str(raw_item.get("requested_end_utc") or "")
        component_windows.append({"schema": schema, "start_utc": start, "end_utc": end})
        verified_components[schema] = HistoricalComponent(
            schema=schema,
            path=source,
            file_bytes=expected_bytes,
            file_sha256=expected_hash,
            requested_start_utc=start,
            requested_end_utc=end,
            validation=validation,
        )
    if set(verified_components) != set(SUPPORTED_SCHEMAS):
        raise ValueError("historical bundle component set is incomplete")
    if len(mapping_hashes) != 1:
        raise ValueError("historical components do not share one mapping identity")
    component_windows.sort(key=lambda item: item["schema"])
    canonical_windows = sorted(_canonical_windows(trading_day), key=lambda item: item["schema"])
    if component_windows != canonical_windows:
        raise ValueError("historical component windows are not canonical for the session")
    if request.get("components") != canonical_windows:
        raise ValueError("historical request windows do not match component evidence")

    planning = payload.get("planning")
    acquisition = payload.get("acquisition")
    if not isinstance(planning, Mapping) or not isinstance(acquisition, Mapping):
        raise ValueError("historical provenance blocks are malformed")
    legacy = payload.get("legacy_evidence")
    if legacy is None:
        if payload.get("provider_condition") != "available":
            raise ValueError("historical acquisition was not provider-available")
        revision = str(payload.get("provider_condition_last_modified_date") or "")
        try:
            date.fromisoformat(revision)
        except ValueError as exc:
            raise ValueError("historical acquisition provider revision is invalid") from exc
        plan_sha256 = _require_sha256(planning.get("plan_sha256"), "plan_sha256")
        estimates = planning.get("component_estimates")
        if not isinstance(estimates, list) or not all(
            isinstance(item, Mapping) for item in estimates
        ):
            raise ValueError("historical planning estimates are malformed")
        normalized_estimates = sorted(
            [dict(item) for item in estimates],
            key=lambda item: str(item.get("schema") or ""),
        )
        expected_plan_sha256 = _canonical_hash(
            {
                "request_sha256": request_sha256,
                "provider_condition": payload.get("provider_condition"),
                "provider_condition_last_modified_date": revision,
                "component_estimates": normalized_estimates,
            }
        )
        if plan_sha256 != expected_plan_sha256:
            raise ValueError("historical planning identity hash mismatch")
    else:
        if payload.get("provider_condition_at_attestation") != "available":
            raise ValueError("legacy bundle was not provider-available when attested")
        revision = str(
            payload.get("provider_condition_last_modified_date_at_attestation")
            or ""
        )
        try:
            date.fromisoformat(revision)
        except ValueError as exc:
            raise ValueError("legacy attestation provider revision is invalid") from exc

    provenance_sha256 = _require_sha256(
        payload.get("provenance_sha256"), "provenance_sha256"
    )
    if _canonical_hash(_provenance_identity(payload)) != provenance_sha256:
        raise ValueError("historical provenance attestation hash mismatch")
    bundle_sha256 = _require_sha256(payload.get("bundle_sha256"), "bundle_sha256")
    if _canonical_hash(_bundle_identity(payload)) != bundle_sha256:
        raise ValueError("historical bundle identity hash mismatch")
    attestation_sha256 = _require_sha256(
        payload.get("attestation_sha256"), "attestation_sha256"
    )
    if _canonical_hash(
        {
            "bundle_sha256": bundle_sha256,
            "provenance_sha256": provenance_sha256,
        }
    ) != attestation_sha256:
        raise ValueError("historical root attestation hash mismatch")

    if legacy is not None:
        if not isinstance(legacy, Mapping):
            raise ValueError("historical legacy evidence link is malformed")
        legacy_path = _resolve_component(day_dir, legacy.get("manifest_file"))
        legacy_hash = _require_sha256(
            legacy.get("manifest_sha256"), "legacy manifest_sha256"
        )
        legacy_bundle_hash = _require_sha256(
            legacy.get("bundle_sha256"), "legacy bundle_sha256"
        )
        if not legacy_path.is_file() or _sha256(legacy_path) != legacy_hash:
            raise ValueError("historical legacy manifest hash mismatch")
        legacy_payload = json.loads(legacy_path.read_text(encoding="utf-8"))
        if not isinstance(legacy_payload, Mapping):
            raise ValueError("historical legacy manifest root is malformed")
        if str(legacy_payload.get("bundle_sha256") or "") != legacy_bundle_hash:
            raise ValueError("historical legacy bundle identity mismatch")

    return VerifiedHistoricalBundle(
        manifest_path=path,
        manifest_sha256=_sha256(path),
        trading_date=trading_day,
        bundle_sha256=bundle_sha256,
        request_sha256=request_sha256,
        provenance_sha256=provenance_sha256,
        attestation_sha256=attestation_sha256,
        components=verified_components,
        payload=dict(payload),
    )


def describe_historical_import(
    project_root: str | Path,
    manifest_path: str | Path,
    *,
    catalog_path: str | Path | None = None,
) -> dict[str, object]:
    """Return a read-only import plan after verifying immutable source bytes."""

    root = Path(project_root).resolve()
    bundle = verify_historical_bundle_manifest(manifest_path)
    active = find_active_capture_sessions(root)
    session_id = (
        f"{bundle.trading_date.isoformat()}-historical-{bundle.bundle_sha256[:12]}"
    )
    config = build_session_config(
        root,
        trading_day=bundle.trading_date,
        now=_market_times(bundle.trading_date)[3] + timedelta(seconds=1),
        session_id=session_id,
    )
    if catalog_path is not None:
        config = replace(config, catalog_path=Path(catalog_path).resolve())
    catalog_has_nonempty_wal = _catalog_has_nonempty_wal(config.catalog_path)
    existing_result = _existing_import_result(
        config.catalog_path,
        session_id=session_id,
        bundle=bundle,
    )
    incremental_import_required: bool | None = existing_result is None
    if catalog_has_nonempty_wal and existing_result is None:
        incremental_import_required = None
    capacity_assumes_incremental_import = existing_result is None
    required_working_bytes = (
        _estimated_working_bytes(bundle)
        if capacity_assumes_incremental_import
        else 0
    )
    estimated_catalog_growth = (
        _estimated_retained_catalog_bytes(bundle)
        if capacity_assumes_incremental_import
        else 0
    )
    estimated_wal_headroom = (
        _estimated_wal_headroom_bytes(estimated_catalog_growth)
        if capacity_assumes_incremental_import
        else 0
    )
    temporary_probe = _disk_usage_probe_path(Path(tempfile.gettempdir()))
    temporary_free = shutil.disk_usage(temporary_probe).free
    catalog_volume_probe, catalog_volume_free = _free_bytes_for_target(
        config.catalog_path.parent
    )
    capacity = _catalog_capacity_requirement(
        minimum_reserve_bytes=int(config.min_free_bytes),
        estimated_working_bytes=required_working_bytes,
        estimated_catalog_growth_bytes=estimated_catalog_growth,
        estimated_wal_headroom_bytes=estimated_wal_headroom,
        catalog_probe=catalog_volume_probe,
        temporary_probe=temporary_probe,
        incremental_import_required=capacity_assumes_incremental_import,
    )
    required_catalog_bytes = int(capacity["required_free_bytes"])
    blocked_reasons = []
    if active:
        blocked_reasons.append("closing-tape capture is active")
    if (
        capacity_assumes_incremental_import
        and temporary_free < required_working_bytes
    ):
        blocked_reasons.append("temporary volume has insufficient free space")
    if (
        capacity_assumes_incremental_import
        and catalog_volume_free < required_catalog_bytes
    ):
        blocked_reasons.append("catalog volume has insufficient free space")
    existing_sessions: list[dict[str, object]] = []
    if config.catalog_path.is_file() and not catalog_has_nonempty_wal:
        connection = sqlite3.connect(
            f"file:{config.catalog_path.resolve()}?mode=ro&immutable=1",
            uri=True,
            timeout=10.0,
        )
        connection.row_factory = sqlite3.Row
        try:
            has_sessions = connection.execute(
                """
                SELECT 1 FROM sqlite_master
                WHERE type='table' AND name='tape_sessions'
                """
            ).fetchone()
            if has_sessions:
                existing_sessions = [
                    dict(row)
                    for row in connection.execute(
                        """
                        SELECT session_id, status, created_at_utc, completed_at_utc
                        FROM tape_sessions ORDER BY created_at_utc
                        """
                    )
                ]
        finally:
            connection.close()
    return {
        "mode": "dry-run",
        "executable": not blocked_reasons,
        "blocked_reasons": blocked_reasons,
        "session_id": session_id,
        "catalog_path": str(config.catalog_path),
        "catalog_exists": config.catalog_path.is_file(),
        "catalog_has_nonempty_wal": catalog_has_nonempty_wal,
        "catalog_inspection_deferred": catalog_has_nonempty_wal,
        "existing_sessions": existing_sessions,
        "incremental_import_required": incremental_import_required,
        "capacity_assumes_incremental_import": (
            capacity_assumes_incremental_import
        ),
        "existing_import_result": (
            existing_result.to_dict() if existing_result is not None else None
        ),
        "evidence_contract_version": HISTORICAL_EVIDENCE_CONTRACT_VERSION,
        "live_operational_counters_applicable": False,
        "estimated_temporary_working_bytes": required_working_bytes,
        "temporary_free_bytes": temporary_free,
        "temporary_volume_probe_path": str(temporary_probe),
        "catalog_volume_probe_path": str(catalog_volume_probe),
        "catalog_volume_free_bytes": catalog_volume_free,
        "minimum_catalog_reserve_bytes": int(capacity["minimum_reserve_bytes"]),
        "estimated_catalog_growth_bytes": int(
            capacity["estimated_catalog_growth_bytes"]
        ),
        "estimated_wal_headroom_bytes": int(
            capacity["estimated_wal_headroom_bytes"]
        ),
        "shared_volume_temporary_bytes": int(
            capacity["shared_volume_temporary_bytes"]
        ),
        "catalog_and_temporary_share_volume": bool(
            capacity["catalog_and_temporary_share_volume"]
        ),
        "predicted_catalog_volume_free_after_import_bytes": (
            catalog_volume_free
            - int(capacity["estimated_catalog_growth_bytes"])
            - int(capacity["estimated_wal_headroom_bytes"])
        ),
        "predicted_catalog_volume_free_at_peak_bytes": (
            catalog_volume_free
            - int(capacity["estimated_catalog_growth_bytes"])
            - int(capacity["estimated_wal_headroom_bytes"])
            - int(capacity["shared_volume_temporary_bytes"])
        ),
        "required_catalog_free_bytes": required_catalog_bytes,
        "bundle": bundle.to_dict(),
        "active_capture_sessions": [item.to_dict() for item in active],
    }


def _materialize_component(
    component: HistoricalComponent,
    destination: Path,
    *,
    trading_day: date,
) -> tuple[Path, TapeIntegrityReport]:
    import databento as db

    store = db.DBNStore.from_file(component.path)
    metadata = store.metadata
    actual_schema = str(getattr(metadata, "schema", "") or "").lower()
    if actual_schema.startswith("schema."):
        actual_schema = actual_schema.split(".", 1)[1]
    if str(getattr(metadata, "dataset", "") or "") != DATASET:
        raise ValueError(f"historical {component.schema} DBN dataset mismatch")
    if actual_schema != component.schema:
        raise ValueError(f"historical {component.schema} DBN schema mismatch")
    if str(getattr(metadata, "stype_in", "") or "").lower() != "parent":
        raise ValueError(f"historical {component.schema} DBN input symbology mismatch")
    if str(getattr(metadata, "stype_out", "") or "").lower() != "instrument_id":
        raise ValueError(f"historical {component.schema} DBN output symbology mismatch")
    if str(getattr(store, "compression", "") or "").lower() != "zstd":
        raise ValueError(f"historical {component.schema} DBN compression mismatch")
    if _metadata_iso(getattr(metadata, "start", None), "start") != (
        component.requested_start_utc
    ):
        raise ValueError(f"historical {component.schema} DBN start mismatch")
    if _metadata_iso(getattr(metadata, "end", None), "end") != (
        component.requested_end_utc
    ):
        raise ValueError(f"historical {component.schema} DBN end mismatch")
    if tuple(str(value).upper() for value in getattr(metadata, "symbols", ())) != tuple(
        DEFAULT_OPTION_PARENTS
    ):
        raise ValueError(f"historical {component.schema} DBN symbols mismatch")
    if tuple(getattr(metadata, "not_found", ()) or ()):
        raise ValueError(f"historical {component.schema} DBN has unresolved symbols")
    if tuple(getattr(metadata, "partial", ()) or ()):
        raise ValueError(f"historical {component.schema} DBN has partial symbols")
    mappings = getattr(metadata, "mappings", None)
    if not isinstance(mappings, Mapping) or not mappings:
        raise ValueError(f"historical {component.schema} DBN has no mappings")
    if len(mappings) != int(component.validation["dbn_mapping_count"]):
        raise ValueError(f"historical {component.schema} DBN mapping count mismatch")
    mapping_sha256, active_roots = _mapping_summary(mappings, trading_day)
    if mapping_sha256 != str(component.validation["dbn_mapping_sha256"]):
        raise ValueError(f"historical {component.schema} DBN mapping hash mismatch")
    expected_raw_roots = {
        parent.removesuffix(".OPT") for parent in DEFAULT_OPTION_PARENTS
    }
    missing_roots = expected_raw_roots - set(active_roots.values())
    if missing_roots:
        raise ValueError(
            f"historical {component.schema} DBN mappings omit: "
            + ", ".join(sorted(missing_roots))
        )
    raw_path = destination / f"{component.schema}.dbn"
    store.to_file(raw_path, mode="x", compression="none")
    report = inspect_dbn(
        raw_path,
        require_trades=component.schema == "tcbbo",
        require_tcbbo=component.schema == "tcbbo",
        expected_subscription_acks=0,
    )
    if not report.local_file_intact:
        raise ValueError(
            f"historical {component.schema} DBN failed framing validation: "
            + ", ".join(report.incomplete_reasons)
        )
    validation = component.validation
    expected_records = int(validation.get("record_count") or 0)
    if report.records_seen != expected_records:
        raise ValueError(
            f"historical {component.schema} record count mismatch "
            f"({report.records_seen}/{expected_records})"
        )
    expected_event_first = int(validation.get("first_ts_event_ns") or 0)
    expected_event_last = int(validation.get("last_ts_event_ns") or 0)
    if (
        report.first_event_ns != expected_event_first
        or report.last_event_ns != expected_event_last
    ):
        raise ValueError(f"historical {component.schema} event-time range mismatch")
    if component.schema == "definition":
        if report.definition_records != expected_records:
            raise ValueError("historical definition DBN contains unexpected record types")
    elif component.schema == "statistics":
        if report.statistics_records != expected_records:
            raise ValueError("historical statistics DBN contains unexpected record types")
    else:
        if report.tcbbo_records != expected_records:
            raise ValueError("historical TCBBO DBN contains unexpected record types")
        if report.tcbbo_timestamped_records != expected_records:
            raise ValueError("historical TCBBO DBN contains undefined timestamps")
        if report.tcbbo_valid_nbbo_records / report.tcbbo_records < 0.95:
            raise ValueError("historical decoded TCBBO valid NBBO coverage is below 95 percent")
    return raw_path, report


def _existing_import_result(
    catalog_path: str | Path,
    *,
    session_id: str,
    bundle: VerifiedHistoricalBundle,
    immutable: bool = True,
) -> HistoricalImportResult | None:
    try:
        path = Path(catalog_path).resolve()
        path_metadata = path.stat()
    except FileNotFoundError:
        return None
    except OSError as exc:
        raise RuntimeError(
            "historical import cannot inspect catalog path "
            f"{catalog_path}: {type(exc).__name__}: {exc}"
        ) from exc
    if not stat.S_ISREG(path_metadata.st_mode):
        return None
    if immutable and _catalog_has_nonempty_wal(path):
        return None
    immutable_parameter = "&immutable=1" if immutable else ""
    connection = sqlite3.connect(
        f"file:{path}?mode=ro{immutable_parameter}",
        uri=True,
        timeout=10.0,
    )
    connection.row_factory = sqlite3.Row
    try:
        required_tables = {
            "tape_sessions",
            "tape_feed_status",
            "tape_finalization_runs",
        }
        present_tables = {
            str(row[0])
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
        }
        if not required_tables.issubset(present_tables):
            return None
        session = connection.execute(
            "SELECT status FROM tape_sessions WHERE session_id=?", (session_id,)
        ).fetchone()
        if session is None:
            return None
        feed = connection.execute(
            """
            SELECT * FROM tape_feed_status
            WHERE session_id=? AND feed_name='opra_options'
            """,
            (session_id,),
        ).fetchone()
        if feed is None:
            return None
        if (
            str(feed["source_kind"]) != HISTORICAL_SOURCE_KIND
            or str(feed["sha256"] or "") not in {"", bundle.bundle_sha256}
            or Path(str(feed["source_manifest_path"] or "")).resolve()
            != bundle.manifest_path
        ):
            raise ValueError("historical import session conflicts with a different source")
        if str(session["status"]) != "complete" or str(feed["status"]) != "complete":
            return None
        counts = {
            table: int(
                connection.execute(
                    f"SELECT COUNT(*) FROM {table} WHERE session_id=?",
                    (session_id,),
                ).fetchone()[0]
            )
            for table in (
                "tape_observed_minute",
                "tape_inferred_minute_flow",
                "tape_observed_contract_minute",
                "tape_inferred_contract_minute_flow",
                "tape_open_interest_observations",
                "tape_instrument_definition_observations",
            )
        }
        finalization = connection.execute(
            """
            SELECT run_key, report_json FROM tape_finalization_runs
            WHERE session_id=? AND feed_name='opra_options' AND source_sha256=?
              AND evidence_contract_version=? AND complete=1
            ORDER BY attempted_at_utc DESC LIMIT 1
            """,
            (
                session_id,
                bundle.bundle_sha256,
                HISTORICAL_EVIDENCE_CONTRACT_VERSION,
            ),
        ).fetchone()
        if finalization is None:
            raise ValueError("completed historical import has no passing finalization audit")
        report_payload = json.loads(str(finalization["report_json"]))
        return HistoricalImportResult(
            status="already_complete",
            session_id=session_id,
            trading_date=bundle.trading_date.isoformat(),
            catalog_path=str(path),
            source_sha256=bundle.bundle_sha256,
            manifest_sha256=bundle.manifest_sha256,
            feature_hash=str(report_payload.get("feature_hash") or "") or None,
            finalization_run_key=str(finalization["run_key"]),
            observed_minute_rows=counts["tape_observed_minute"],
            inferred_minute_rows=counts["tape_inferred_minute_flow"],
            observed_contract_minute_rows=counts["tape_observed_contract_minute"],
            inferred_contract_minute_rows=counts["tape_inferred_contract_minute_flow"],
            open_interest_observations=counts["tape_open_interest_observations"],
            definition_observations=counts[
                "tape_instrument_definition_observations"
            ],
        )
    finally:
        connection.close()


def _register_or_resume(
    catalog: TapeCatalog,
    *,
    config,
    bundle: VerifiedHistoricalBundle,
) -> dict[str, object | None]:
    components = {
        schema: component.source_identity()
        for schema, component in sorted(bundle.components.items())
    }
    with catalog.connect(read_only=True) as connection:
        exists = connection.execute(
            "SELECT 1 FROM tape_sessions WHERE session_id=?", (config.session_id,)
        ).fetchone()
    if exists is None:
        catalog.start_session(config)
    with catalog.connect(read_only=True) as connection:
        prior_feed = connection.execute(
            """
            SELECT status, error, gaps_json FROM tape_feed_status
            WHERE session_id=? AND feed_name='opra_options'
            """,
            (config.session_id,),
        ).fetchone()
    if prior_feed is None:
        catalog.register_feed(
            config.session_id,
            config.feeds[0],
            bundle.manifest_path,
            source_kind=HISTORICAL_SOURCE_KIND,
            evidence_contract_version=HISTORICAL_EVIDENCE_CONTRACT_VERSION,
            operational_counters_applicable=False,
            source_manifest_path=bundle.manifest_path,
            source_components=components,
        )
        prior_evidence: dict[str, object | None] = {
            "status": "starting",
            "error": None,
            "gaps_json": "[]",
        }
    else:
        prior_evidence = {
            "status": prior_feed["status"],
            "error": prior_feed["error"],
            "gaps_json": prior_feed["gaps_json"],
        }
    if exists is not None:
        with catalog.connect() as connection:
            connection.execute(
                """
                UPDATE tape_sessions
                SET status='running', completed_at_utc=NULL, error=NULL
                WHERE session_id=?
                """,
                (config.session_id,),
            )
    catalog.update_feed_status(
        config.session_id,
        "opra_options",
        {
            "status": "importing",
            "complete": 0,
            "sha256": bundle.bundle_sha256,
            "source_kind": HISTORICAL_SOURCE_KIND,
            "evidence_contract_version": HISTORICAL_EVIDENCE_CONTRACT_VERSION,
            "operational_counters_applicable": 0,
            "source_manifest_path": str(bundle.manifest_path),
            "source_components_json": json.dumps(
                components, sort_keys=True, separators=(",", ":")
            ),
            "error": None,
        },
    )
    return prior_evidence


def _verify_persisted_import(
    catalog: TapeCatalog,
    *,
    session_id: str,
    source_sha256: str,
    expected_tcbbo_records: int,
    expected_definition_records: int,
) -> dict[str, object]:
    with catalog.connect(read_only=True) as connection:
        def families(table: str) -> set[str]:
            return {
                str(row[0])
                for row in connection.execute(
                    f"""
                    SELECT DISTINCT family_root FROM {table}
                    WHERE session_id=? AND feed_name='opra_options'
                      AND family_root IS NOT NULL
                    """,
                    (session_id,),
                )
            }

        counts = {
            table: int(
                connection.execute(
                    f"SELECT COUNT(*) FROM {table} WHERE session_id=?",
                    (session_id,),
                ).fetchone()[0]
            )
            for table in (
                "tape_observed_minute",
                "tape_inferred_minute_flow",
                "tape_observed_contract_minute",
                "tape_inferred_contract_minute_flow",
                "tape_open_interest_observations",
                "tape_instrument_definition_observations",
            )
        }
        observed_trades = int(
            connection.execute(
                """
                SELECT COALESCE(SUM(trade_count),0) FROM tape_observed_minute
                WHERE session_id=? AND feed_name='opra_options'
                """,
                (session_id,),
            ).fetchone()[0]
        )
        inferred_trades = int(
            connection.execute(
                """
                SELECT COALESCE(SUM(
                    at_ask_count+at_bid_count+inside_count+unknown_count
                ),0) FROM tape_inferred_minute_flow
                WHERE session_id=? AND feed_name='opra_options'
                """,
                (session_id,),
            ).fetchone()[0]
        )
        mismatched_sources = int(
            connection.execute(
                """
                SELECT COUNT(*) FROM tape_inferred_minute_flow
                WHERE session_id=? AND source_sha256<>?
                """,
                (session_id, source_sha256),
            ).fetchone()[0]
        ) + int(
            connection.execute(
                """
                SELECT COUNT(*) FROM tape_inferred_contract_minute_flow
                WHERE session_id=? AND source_sha256<>?
                """,
                (session_id, source_sha256),
            ).fetchone()[0]
        )
        observed_families = families("tape_observed_minute")
        inferred_families = families("tape_inferred_minute_flow")
        oi_families = families("tape_open_interest")
    expected_families = set(PRODUCTION_FAMILIES)
    if observed_trades != expected_tcbbo_records or inferred_trades != expected_tcbbo_records:
        raise ValueError(
            "historical persisted trade totals do not match immutable TCBBO records "
            f"({observed_trades}/{inferred_trades}/{expected_tcbbo_records})"
        )
    if mismatched_sources:
        raise ValueError("historical inferred rows are not bundle-hash aligned")
    if counts["tape_instrument_definition_observations"] != expected_definition_records:
        raise ValueError("historical immutable definition ledger is incomplete")
    for label, observed_values in (
        ("observed", observed_families),
        ("inferred", inferred_families),
        ("open-interest", oi_families),
    ):
        if observed_values != expected_families:
            raise ValueError(
                f"historical {label} family coverage mismatch: "
                + ", ".join(sorted(expected_families - observed_values))
            )
    if counts["tape_observed_contract_minute"] <= 0 or (
        counts["tape_observed_contract_minute"]
        != counts["tape_inferred_contract_minute_flow"]
    ):
        raise ValueError("historical contract-minute evidence is incomplete")
    if counts["tape_open_interest_observations"] <= 0:
        raise ValueError("historical immutable open-interest ledger is empty")
    return {
        "counts": counts,
        "observed_trade_records": observed_trades,
        "inferred_classification_records": inferred_trades,
        "families": sorted(observed_families),
    }


def import_historical_bundle(
    project_root: str | Path,
    manifest_path: str | Path,
    *,
    catalog_path: str | Path | None = None,
) -> HistoricalImportResult:
    """Import a verified historical bundle into observed and inferred storage.

    This intentionally has no active-capture override. It decompresses and
    aggregates millions of records, so a running recorder is an unconditional
    compute-window blocker.
    """

    root = Path(project_root).resolve()
    require_compute_window(
        root,
        operation="historical closing-tape import",
        override_supported=False,
    )
    bundle = verify_historical_bundle_manifest(manifest_path)
    session_id = (
        f"{bundle.trading_date.isoformat()}-historical-{bundle.bundle_sha256[:12]}"
    )
    config = build_session_config(
        root,
        trading_day=bundle.trading_date,
        now=_market_times(bundle.trading_date)[3] + timedelta(seconds=1),
        session_id=session_id,
    )
    if catalog_path is not None:
        config = replace(config, catalog_path=Path(catalog_path).resolve())
    # Execute mode may attach read-only to an existing WAL to prove that the
    # requested bundle is already complete before applying new-growth gates.
    # Dry-run remains immutable and reports this state as unknown instead.
    catalog_has_nonempty_wal = _catalog_has_nonempty_wal(config.catalog_path)
    existing = _existing_import_result(
        config.catalog_path,
        session_id=session_id,
        bundle=bundle,
        immutable=not catalog_has_nonempty_wal,
    )
    if existing is not None:
        return existing
    required_working_bytes = _estimated_working_bytes(bundle)
    estimated_catalog_growth = _estimated_retained_catalog_bytes(bundle)
    estimated_wal_headroom = _estimated_wal_headroom_bytes(
        estimated_catalog_growth
    )
    temporary_root = _disk_usage_probe_path(Path(tempfile.gettempdir()))
    temporary_free = shutil.disk_usage(temporary_root).free
    if temporary_free < required_working_bytes:
        raise RuntimeError(
            "historical import has insufficient temporary free space "
            f"({temporary_free}/{required_working_bytes} bytes)"
        )
    catalog_volume_probe, catalog_volume_free = _free_bytes_for_target(
        config.catalog_path.parent
    )
    capacity = _catalog_capacity_requirement(
        minimum_reserve_bytes=int(config.min_free_bytes),
        estimated_working_bytes=required_working_bytes,
        estimated_catalog_growth_bytes=estimated_catalog_growth,
        estimated_wal_headroom_bytes=estimated_wal_headroom,
        catalog_probe=catalog_volume_probe,
        temporary_probe=temporary_root,
    )
    required_catalog_bytes = int(capacity["required_free_bytes"])
    if catalog_volume_free < required_catalog_bytes:
        raise RuntimeError(
            "historical import has insufficient catalog-volume free space "
            f"at {catalog_volume_probe} "
            f"({catalog_volume_free}/{required_catalog_bytes} bytes)"
        )
    catalog = TapeCatalog(config.catalog_path)

    registered = False
    attempted_at = _utcnow()
    prior_evidence: dict[str, object | None] = {
        "status": None,
        "error": None,
        "gaps_json": None,
    }
    try:
        with tempfile.TemporaryDirectory(
            prefix="marketpin-historical-import-",
            dir=temporary_root,
        ) as raw_dir:
            raw_root = Path(raw_dir)
            raw_paths: dict[str, Path] = {}
            integrity: dict[str, TapeIntegrityReport] = {}
            for schema in SUPPORTED_SCHEMAS:
                raw_path, report = _materialize_component(
                    bundle.components[schema],
                    raw_root,
                    trading_day=bundle.trading_date,
                )
                raw_paths[schema] = raw_path
                integrity[schema] = report

            registered = True
            prior_evidence = _register_or_resume(
                catalog,
                config=config,
                bundle=bundle,
            )
            definition_replay = replay_instrument_definitions(
                raw_paths["definition"],
                catalog=catalog,
                session_id=session_id,
                feed_name="opra_options",
                source_sha256=bundle.bundle_sha256,
            )
            expected_definitions = int(
                bundle.components["definition"].validation["record_count"]
            )
            if (
                int(definition_replay["records"]) != expected_definitions
                or int(definition_replay["parsed_contracts"]) != expected_definitions
            ):
                raise ValueError("historical definition replay is incomplete")
            projected_instruments = catalog.rebuild_instruments_from_definition_observations(
                session_id=session_id,
                feed_name="opra_options",
                source_sha256=bundle.bundle_sha256,
            )
            if projected_instruments <= 0:
                raise ValueError("historical instrument projection is empty")

            oi_replay = replay_open_interest(
                raw_paths["statistics"],
                catalog=catalog,
                session_id=session_id,
                feed_name="opra_options",
                source_sha256=bundle.bundle_sha256,
            )
            expected_oi_records = sum(
                int(value)
                for value in dict(
                    bundle.components["statistics"].validation[
                        "open_interest_record_counts"
                    ]
                ).values()
            )
            if int(oi_replay["records"]) != expected_oi_records:
                raise ValueError("historical open-interest replay count mismatch")
            if int(oi_replay["unmapped_records"]) != 0:
                raise ValueError("historical open-interest replay contains unmapped records")

            tcbbo_report = integrity["tcbbo"]
            (
                observed,
                inferred,
                contract_observed,
                contract_inferred,
                feature_hash,
            ) = build_minute_rows(
                raw_paths["tcbbo"],
                session_id=session_id,
                feed_name="opra_options",
                verified_source_sha256=tcbbo_report.sha256,
                verified_integrity_report=tcbbo_report,
                evidence_source_sha256=bundle.bundle_sha256,
                include_contract_rows=True,
            )
            persist_minute_rows(catalog, observed, inferred)
            persist_contract_minute_rows(
                catalog, contract_observed, contract_inferred
            )
            persisted = _verify_persisted_import(
                catalog,
                session_id=session_id,
                source_sha256=bundle.bundle_sha256,
                expected_tcbbo_records=tcbbo_report.tcbbo_records,
                expected_definition_records=expected_definitions,
            )

            component_evidence = {
                schema: {
                    **bundle.components[schema].source_identity(),
                    "raw_dbn_sha256": integrity[schema].sha256,
                    "raw_dbn_bytes": integrity[schema].file_bytes,
                    "raw_dbn_materialization": "temporary_verified_then_removed",
                    "raw_integrity": {
                        key: value
                        for key, value in integrity[schema].to_dict().items()
                        if key != "path"
                    },
                }
                for schema in SUPPORTED_SCHEMAS
            }
            report_payload = {
                "version": IMPORT_REPORT_VERSION,
                "session_id": session_id,
                "trading_date": bundle.trading_date.isoformat(),
                "source_kind": HISTORICAL_SOURCE_KIND,
                "source_sha256": bundle.bundle_sha256,
                "evidence_contract_version": HISTORICAL_EVIDENCE_CONTRACT_VERSION,
                "manifest_path": str(bundle.manifest_path),
                "manifest_sha256": bundle.manifest_sha256,
                "request_sha256": bundle.request_sha256,
                "provenance_sha256": bundle.provenance_sha256,
                "attestation_sha256": bundle.attestation_sha256,
                "live_operational_counters_applicable": False,
                "prior_feed_evidence": prior_evidence,
                "feature_hash": feature_hash,
                "definition_replay": dict(definition_replay),
                "projected_instruments": projected_instruments,
                "open_interest_replay": dict(oi_replay),
                "persisted": persisted,
                "components": component_evidence,
            }
            run_key = _canonical_hash(
                {
                    "version": IMPORT_REPORT_VERSION,
                    "session_id": session_id,
                    "source_sha256": bundle.bundle_sha256,
                    "report": report_payload,
                }
            )
            catalog.record_finalization_run(
                {
                    "run_key": run_key,
                    "session_id": session_id,
                    "feed_name": "opra_options",
                    "source_sha256": bundle.bundle_sha256,
                    "evidence_contract_version": (
                        HISTORICAL_EVIDENCE_CONTRACT_VERSION
                    ),
                    "attempted_at_utc": attempted_at,
                    "complete": 1,
                    "issues_json": "[]",
                    "prior_status": prior_evidence["status"],
                    "prior_error": prior_evidence["error"],
                    "prior_gaps_json": prior_evidence["gaps_json"],
                    "report_json": json.dumps(
                        report_payload, sort_keys=True, separators=(",", ":")
                    ),
                }
            )

            family_counts: dict[str, int] = {}
            for raw_family, value in dict(
                bundle.components["tcbbo"].validation["family_record_counts"]
            ).items():
                family = {
                    "SPXW": "SPX",
                    "NDXP": "NDX",
                    "RUTW": "RUT",
                    "VIXW": "VIX",
                }.get(str(raw_family), str(raw_family))
                family_counts[family] = family_counts.get(family, 0) + int(value)
            catalog.update_feed_status(
                session_id,
                "opra_options",
                {
                    "status": "complete",
                    "started_at_utc": attempted_at,
                    "ended_at_utc": _utcnow(),
                    "records_seen": sum(
                        report.records_seen for report in integrity.values()
                    ),
                    "trade_records": tcbbo_report.trade_records,
                    "tcbbo_records": tcbbo_report.tcbbo_records,
                    "tcbbo_timestamped_records": (
                        tcbbo_report.tcbbo_timestamped_records
                    ),
                    "tcbbo_valid_nbbo_records": (
                        tcbbo_report.tcbbo_valid_nbbo_records
                    ),
                    "tcbbo_flagged_records": tcbbo_report.tcbbo_flagged_records,
                    "tcbbo_action_counts_json": json.dumps(
                        dict(tcbbo_report.tcbbo_action_counts),
                        sort_keys=True,
                        separators=(",", ":"),
                    ),
                    "statistics_records": integrity["statistics"].statistics_records,
                    "definition_records": integrity["definition"].definition_records,
                    "unmapped_trade_records": 0,
                    "first_event_ns": min(
                        int(report.first_event_ns)
                        for report in integrity.values()
                        if report.first_event_ns is not None
                    ),
                    "last_event_ns": max(
                        int(report.last_event_ns)
                        for report in integrity.values()
                        if report.last_event_ns is not None
                    ),
                    "last_receive_ns": tcbbo_report.last_receive_ns,
                    "file_bytes": sum(
                        component.file_bytes for component in bundle.components.values()
                    ),
                    "last_trade_event_ns": tcbbo_report.last_event_ns,
                    "root_trade_counts_json": json.dumps(
                        family_counts, sort_keys=True, separators=(",", ":")
                    ),
                    "gaps_json": "[]",
                    "complete": 1,
                    "sha256": bundle.bundle_sha256,
                    "source_components_json": json.dumps(
                        component_evidence, sort_keys=True, separators=(",", ":")
                    ),
                    "error": None,
                },
            )
            catalog.finish_session(session_id, "complete")
            counts = dict(persisted["counts"])
            return HistoricalImportResult(
                status="complete",
                session_id=session_id,
                trading_date=bundle.trading_date.isoformat(),
                catalog_path=str(catalog.path.resolve()),
                source_sha256=bundle.bundle_sha256,
                manifest_sha256=bundle.manifest_sha256,
                feature_hash=feature_hash,
                finalization_run_key=run_key,
                observed_minute_rows=int(counts["tape_observed_minute"]),
                inferred_minute_rows=int(counts["tape_inferred_minute_flow"]),
                observed_contract_minute_rows=int(
                    counts["tape_observed_contract_minute"]
                ),
                inferred_contract_minute_rows=int(
                    counts["tape_inferred_contract_minute_flow"]
                ),
                open_interest_observations=int(
                    counts["tape_open_interest_observations"]
                ),
                definition_observations=int(
                    counts["tape_instrument_definition_observations"]
                ),
            )
    except Exception as exc:
        if registered:
            message = f"{type(exc).__name__}: {str(exc)[:1000]}"
            catalog.update_feed_status(
                session_id,
                "opra_options",
                {
                    "status": "incomplete",
                    "complete": 0,
                    "ended_at_utc": _utcnow(),
                    "error": message,
                    "gaps_json": json.dumps(
                        [{"historical_import_error": message}],
                        separators=(",", ":"),
                    ),
                },
            )
            catalog.finish_session(session_id, "incomplete", message)
            with catalog.connect(read_only=True) as connection:
                feed_exists = connection.execute(
                    """
                    SELECT 1 FROM tape_feed_status
                    WHERE session_id=? AND feed_name='opra_options'
                    """,
                    (session_id,),
                ).fetchone()
            if feed_exists is not None:
                failure_report = {
                    "version": IMPORT_REPORT_VERSION,
                    "session_id": session_id,
                    "trading_date": bundle.trading_date.isoformat(),
                    "source_kind": HISTORICAL_SOURCE_KIND,
                    "source_sha256": bundle.bundle_sha256,
                    "evidence_contract_version": (
                        HISTORICAL_EVIDENCE_CONTRACT_VERSION
                    ),
                    "manifest_path": str(bundle.manifest_path),
                    "manifest_sha256": bundle.manifest_sha256,
                    "attempted_at_utc": attempted_at,
                    "complete": False,
                    "issues": [message],
                    "prior_feed_evidence": prior_evidence,
                }
                failure_key = _canonical_hash(
                    {
                        "version": IMPORT_REPORT_VERSION,
                        "session_id": session_id,
                        "source_sha256": bundle.bundle_sha256,
                        "failure": failure_report,
                    }
                )
                catalog.record_finalization_run(
                    {
                        "run_key": failure_key,
                        "session_id": session_id,
                        "feed_name": "opra_options",
                        "source_sha256": bundle.bundle_sha256,
                        "evidence_contract_version": (
                            HISTORICAL_EVIDENCE_CONTRACT_VERSION
                        ),
                        "attempted_at_utc": attempted_at,
                        "complete": 0,
                        "issues_json": json.dumps([message], separators=(",", ":")),
                        "prior_status": prior_evidence["status"],
                        "prior_error": prior_evidence["error"],
                        "prior_gaps_json": prior_evidence["gaps_json"],
                        "report_json": json.dumps(
                            failure_report,
                            sort_keys=True,
                            separators=(",", ":"),
                        ),
                    }
                )
        raise
