"""Read UTC-partitioned snapshot exports as browser-local calendar days."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
import json
from numbers import Integral
from pathlib import Path
from typing import Any, Iterable, Iterator

from app.utils.display_time import DisplayTimezone, UTC, parse_utc_timestamp
from backend.workstation import payload_has_fallback_provenance
from backend.validation_method import validation_labels


# Legacy import compatibility only; no stored-record classifier returns this.
CURRENT_LIVE_EPOCH_VERIFIED = "current_live_epoch_verified"
RESEARCH_RECORDED_SUBSCRIPTION_IDENTITY = "research_recorded_subscription_identity"
RESEARCH_FALLBACK_PROVENANCE = "research_fallback_provenance"
HISTORICAL_UNVERIFIED_SUBSCRIPTION_IDENTITY = (
    "historical_unverified_subscription_identity"
)
DIAGNOSTIC_INVALID_SNAPSHOT = "diagnostic_invalid_snapshot"


@dataclass(frozen=True)
class SnapshotSelection:
    """Records selected for one local day plus transparent read diagnostics."""

    records: tuple[dict[str, Any], ...]
    source_files: tuple[str, ...]
    malformed_timestamp_records: int


@dataclass(frozen=True)
class SnapshotEvidence:
    """Separate identity-bearing research, legacy context, and retained failures.

    ``usable_records`` is retained for caller compatibility; it means research
    records with valid calculations, never current prediction authority.
    """

    usable_records: tuple[dict[str, Any], ...]
    historical_records: tuple[dict[str, Any], ...]
    diagnostic_records: tuple[dict[str, Any], ...]


def canonical_subscription_epoch_id(value: object) -> str | None:
    """Return the process epoch only when it is canonical lowercase SHA-256."""

    if not isinstance(value, str) or len(value) != 64:
        return None
    return value if all(character in "0123456789abcdef" for character in value) else None


def positive_subscription_generation(value: object) -> int | None:
    """Return a positive integral generation without coercing legacy values."""

    if isinstance(value, bool) or not isinstance(value, Integral) or value <= 0:
        return None
    return int(value)


def gamma_snapshot_provenance_status(record: dict[str, Any]) -> str:
    """Classify stored research provenance without asserting current authority."""

    source_valid = record.get("validation_is_valid") is True and record.get("gamma_excluded_from_model") is False
    if not source_valid:
        return DIAGNOSTIC_INVALID_SNAPSHOT
    if payload_has_fallback_provenance(record):
        return RESEARCH_FALLBACK_PROVENANCE
    epoch_id = canonical_subscription_epoch_id(record.get("subscription_epoch_id"))
    generation = positive_subscription_generation(record.get("subscription_generation"))
    if epoch_id is not None and generation is not None:
        return RESEARCH_RECORDED_SUBSCRIPTION_IDENTITY
    return HISTORICAL_UNVERIFIED_SUBSCRIPTION_IDENTITY


def is_usable_gamma_snapshot(record: dict[str, Any]) -> bool:
    """Return calculation-valid research availability, not live eligibility."""

    return (
        gamma_snapshot_provenance_status(record)
        != DIAGNOSTIC_INVALID_SNAPSHOT
    )


def snapshot_research_export_fields(record: dict[str, Any]) -> dict[str, Any]:
    """Add explicit export authority without overwriting stored producer fields."""

    from app.services.validation_review import validation_group
    return {
        **validation_labels(record),
        "review_validation_group": validation_group(record),
        "export_provenance_status": gamma_snapshot_provenance_status(record),
        "current_live_eligible": False,
        "research_only": True,
        "fallback_provenance": payload_has_fallback_provenance(record),
    }


def snapshot_research_export_record(record: dict[str, Any]) -> dict[str, Any]:
    """Build a sanitized derived copy while preserving content identities."""

    redacted_paths = 0

    def sanitize(value: Any) -> Any:
        nonlocal redacted_paths
        if isinstance(value, dict):
            result = {}
            for key, nested in value.items():
                if key == "source_path" and nested:
                    result[key] = None
                    redacted_paths += 1
                else:
                    result[key] = sanitize(nested)
            return result
        if isinstance(value, list):
            return [sanitize(item) for item in value]
        return value

    sanitized = sanitize(record)
    return {
        **sanitized,
        **snapshot_research_export_fields(record),
        "local_source_paths_redacted": redacted_paths,
    }


def snapshot_coverage_manifest(
    evidence_by_symbol: dict[str, SnapshotEvidence],
    *,
    expected_symbols: Iterable[str],
    trading_date: str,
) -> dict[str, Any]:
    """Describe complete export coverage without fabricating missing rows."""

    expected = tuple(
        dict.fromkeys(str(symbol).strip().upper() for symbol in expected_symbols if str(symbol).strip())
    )
    symbols = {}
    observed_symbols = []
    for symbol in sorted(set(expected) | set(evidence_by_symbol)):
        evidence = evidence_by_symbol.get(symbol)
        usable = len(evidence.usable_records) if evidence else 0
        historical = len(evidence.historical_records) if evidence else 0
        diagnostics = len(evidence.diagnostic_records) if evidence else 0
        total = usable + historical + diagnostics
        if total:
            observed_symbols.append(symbol)
        symbols[symbol] = {
            "producer_pass_records": usable,
            "historical_unverified_records": historical,
            "failed_or_unproven_records": diagnostics,
            "total_records": total,
            "missing_evidence_reason": (
                "NO_RETAINED_SNAPSHOT_RECORDS" if total == 0 else None
            ),
        }
    observed = tuple(sorted(observed_symbols))
    return {
        "schema_version": "marketpin-snapshot-export-coverage.v1",
        "trading_date": trading_date,
        "expected_symbols": list(expected),
        "observed_symbols": list(observed),
        "missing_expected_symbols": sorted(set(expected) - set(observed)),
        "symbols": symbols,
        "research_only": True,
        "accuracy_established": False,
    }


def partition_snapshot_evidence(
    records: Iterable[dict[str, Any]],
) -> SnapshotEvidence:
    """Partition records without promoting legacy identity or invalid GEX data."""

    usable: list[dict[str, Any]] = []
    historical: list[dict[str, Any]] = []
    diagnostics: list[dict[str, Any]] = []
    for record in records:
        status = gamma_snapshot_provenance_status(record)
        if status in {
            RESEARCH_RECORDED_SUBSCRIPTION_IDENTITY,
            RESEARCH_FALLBACK_PROVENANCE,
        }:
            usable.append(record)
        elif status == HISTORICAL_UNVERIFIED_SUBSCRIPTION_IDENTITY:
            historical.append(record)
        else:
            diagnostics.append(record)
    return SnapshotEvidence(tuple(usable), tuple(historical), tuple(diagnostics))


def current_local_date(display_timezone: DisplayTimezone) -> str:
    """Return today's date in the viewer's resolved display timezone."""

    return datetime.now(UTC).astimezone(display_timezone.tzinfo).date().isoformat()


def local_day_utc_bounds(
    local_date: str | date,
    display_timezone: DisplayTimezone,
) -> tuple[datetime, datetime]:
    """Return the half-open UTC interval for a viewer-local calendar day."""

    day = date.fromisoformat(local_date) if isinstance(local_date, str) else local_date
    start_local = datetime.combine(day, time.min, tzinfo=display_timezone.tzinfo)
    end_local = datetime.combine(day + timedelta(days=1), time.min, tzinfo=display_timezone.tzinfo)
    return start_local.astimezone(UTC), end_local.astimezone(UTC)


def utc_partition_dates_for_local_day(
    local_date: str | date,
    display_timezone: DisplayTimezone,
    *,
    include_adjacent: bool = True,
) -> tuple[str, ...]:
    """List UTC filename dates that can contain records from one local day."""

    start_utc, end_utc = local_day_utc_bounds(local_date, display_timezone)
    first_day = start_utc.date()
    last_day = (end_utc - timedelta(microseconds=1)).date()
    if include_adjacent:
        first_day -= timedelta(days=1)
        last_day += timedelta(days=1)

    result: list[str] = []
    current = first_day
    while current <= last_day:
        result.append(current.isoformat())
        current += timedelta(days=1)
    return tuple(result)


def _snapshot_timestamp(record: dict[str, Any]) -> datetime | None:
    generated = parse_utc_timestamp(record.get("generated_at_utc"))
    if generated is not None:
        return generated
    return parse_utc_timestamp(record.get("timestamp_utc"))


def _records_from_file(path: Path) -> Iterator[tuple[dict[str, Any], datetime | None]]:
    try:
        with path.open("r", encoding="utf-8") as handle:
            for raw_line in handle:
                line = raw_line.strip()
                if not line:
                    continue
                try:
                    record = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if isinstance(record, dict):
                    yield record, _snapshot_timestamp(record)
    except OSError:
        return


def _reverse_lines(path: Path, chunk_size: int = 64 * 1024) -> Iterator[str]:
    """Yield non-empty UTF-8 lines from the end without loading large files."""

    try:
        with path.open("rb") as handle:
            handle.seek(0, 2)
            position = handle.tell()
            buffer = b""
            while position > 0:
                read_size = min(chunk_size, position)
                position -= read_size
                handle.seek(position)
                buffer = handle.read(read_size) + buffer
                lines = buffer.split(b"\n")
                buffer = lines[0]
                for raw_line in reversed(lines[1:]):
                    if raw_line.strip():
                        yield raw_line.decode("utf-8", errors="replace")
            if buffer.strip():
                yield buffer.decode("utf-8", errors="replace")
    except OSError:
        return


def _timestamp_from_line(raw_line: str) -> datetime | None:
    try:
        record = json.loads(raw_line)
    except json.JSONDecodeError:
        return None
    return _snapshot_timestamp(record) if isinstance(record, dict) else None


def _file_timestamp_bounds(path: Path) -> tuple[datetime | None, datetime | None]:
    first_timestamp = None
    for _record, timestamp in _records_from_file(path):
        if timestamp is not None:
            first_timestamp = timestamp
            break

    last_timestamp = None
    for raw_line in _reverse_lines(path):
        timestamp = _timestamp_from_line(raw_line)
        if timestamp is not None:
            last_timestamp = timestamp
            break
    return first_timestamp, last_timestamp


def load_snapshots_for_local_day(
    exports_root: str | Path,
    symbol: str,
    local_date: str | date,
    display_timezone: DisplayTimezone,
) -> SnapshotSelection:
    """Merge adjacent UTC partitions and filter them to one browser-local day."""

    start_utc, end_utc = local_day_utc_bounds(local_date, display_timezone)
    symbol_dir = Path(exports_root) / symbol
    source_files: list[str] = []
    selected: list[tuple[datetime, int, dict[str, Any]]] = []
    malformed_count = 0
    sequence = 0

    for partition_date in utc_partition_dates_for_local_day(
        local_date,
        display_timezone,
    ):
        path = symbol_dir / f"{partition_date}.ndjson"
        if not path.is_file():
            continue
        source_files.append(str(path))
        for record, timestamp_utc in _records_from_file(path):
            if timestamp_utc is None:
                malformed_count += 1
                continue
            if start_utc <= timestamp_utc < end_utc:
                selected.append((timestamp_utc, sequence, record))
                sequence += 1

    selected.sort(key=lambda item: (item[0], item[1]))
    return SnapshotSelection(
        records=tuple(item[2] for item in selected),
        source_files=tuple(source_files),
        malformed_timestamp_records=malformed_count,
    )


def available_local_dates(
    partition_dates: Iterable[str],
    display_timezone: DisplayTimezone,
) -> list[str]:
    """Map UTC filename dates to possible viewer-local dates without file scans."""

    local_dates: set[str] = set()
    for value in partition_dates:
        try:
            utc_day = date.fromisoformat(str(value))
        except ValueError:
            continue
        start_utc = datetime.combine(utc_day, time.min, tzinfo=UTC)
        end_utc = datetime.combine(utc_day + timedelta(days=1), time.min, tzinfo=UTC)
        local_dates.add(start_utc.astimezone(display_timezone.tzinfo).date().isoformat())
        local_dates.add(
            (end_utc - timedelta(microseconds=1))
            .astimezone(display_timezone.tzinfo)
            .date()
            .isoformat()
        )
    return sorted(local_dates, reverse=True)


def available_local_dates_for_symbol(
    exports_root: str | Path,
    symbol: str,
    display_timezone: DisplayTimezone,
) -> list[str]:
    """Catalog actual local dates from each partition's first/last records."""

    symbol_dir = Path(exports_root) / symbol
    if not symbol_dir.is_dir():
        return []

    local_dates: set[str] = set()
    for path in symbol_dir.glob("*.ndjson"):
        first_timestamp, last_timestamp = _file_timestamp_bounds(path)
        if first_timestamp is None or last_timestamp is None:
            continue
        first_day = first_timestamp.astimezone(display_timezone.tzinfo).date()
        last_day = last_timestamp.astimezone(display_timezone.tzinfo).date()
        current = min(first_day, last_day)
        end = max(first_day, last_day)
        while current <= end:
            local_dates.add(current.isoformat())
            current += timedelta(days=1)
    return sorted(local_dates, reverse=True)
