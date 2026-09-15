from __future__ import annotations

import hashlib
import json
import math
import os
import shutil
import uuid
import warnings
from collections import Counter
from dataclasses import asdict, dataclass
from datetime import date, datetime, time, timedelta
from importlib.metadata import PackageNotFoundError, version as package_version
from pathlib import Path
from typing import Callable, Iterable, Mapping, Sequence

from .config import DEFAULT_OPTION_PARENTS, UTC, _market_times


DATASET = "OPRA.PILLAR"
HISTORICAL_BUNDLE_VERSION = "marketpin-databento-historical-bundle-v2"
LEGACY_BUNDLE_VERSION = "marketpin-databento-historical-bundle-v1"
REQUEST_CONTRACT_VERSION = "marketpin-databento-historical-request-v1"
SUPPORTED_SCHEMAS = ("tcbbo", "statistics", "definition")
DEFAULT_OUTPUT_DIRECTORY = Path("data") / "databento_history"
STYPE_IN = "parent"
STYPE_OUT = "instrument_id"
RANGE_FILTER = "ts_recv"
ENCODING = "dbn"
COMPRESSION = "zstd"

# MarketPin's checked-in cash-market calendar is currently maintained only for
# these years. Fail closed outside it rather than silently assigning an ordinary
# 16:00 ET close to a historical holiday or early-close session.
SUPPORTED_CALENDAR_START = date(2024, 1, 1)
SUPPORTED_CALENDAR_END = date(2026, 12, 31)

_EXPECTED_RTYPE = {"tcbbo": 194, "statistics": 24, "definition": 19}
_OPEN_INTEREST_STAT_TYPE = 9
_UNDEF_TIMESTAMP = 2**64 - 1
_UNDEF_STAT_QUANTITY = 2**63 - 1
_UNDEF_PRICE = 2**63 - 1
_OPTION_INSTRUMENT_CLASSES = {67, 80}  # ASCII C/P in DBN.
_MIN_TWO_SIDED_TCBBO_COVERAGE = 0.95


def _iso_utc(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _parse_utc(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError(f"timestamp must be timezone-aware: {value}")
    return parsed.astimezone(UTC)


def _datetime_ns(value: datetime) -> int:
    utc_value = value.astimezone(UTC)
    seconds = int(utc_value.timestamp())
    return seconds * 1_000_000_000 + utc_value.microsecond * 1_000


def _metadata_ns(value: object, field: str) -> int:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            raise ValueError(f"DBN metadata {field} is timezone-naive")
        return _datetime_ns(value)
    try:
        result = int(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError(f"DBN metadata {field} is invalid: {value!r}") from exc
    if result < 0:
        raise ValueError(f"DBN metadata {field} is negative")
    return result


def _ns_iso(value: int) -> str:
    seconds, nanoseconds = divmod(int(value), 1_000_000_000)
    base = datetime.fromtimestamp(seconds, tz=UTC).replace(
        microsecond=nanoseconds // 1_000
    )
    return _iso_utc(base)


def _sha256(path: Path, chunk_size: int = 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(chunk_size), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _canonical_hash(payload: Mapping[str, object]) -> str:
    encoded = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _atomic_json(path: Path, payload: Mapping[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp")
    try:
        with temporary.open("x", encoding="utf-8", newline="\n") as stream:
            json.dump(payload, stream, indent=2, sort_keys=True, allow_nan=False)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _finite_nonnegative(value: object, field: str) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError(f"{field} must be a finite nonnegative number") from exc
    if not math.isfinite(parsed) or parsed < 0:
        raise ValueError(f"{field} must be a finite nonnegative number")
    return parsed


def _positive_bytes(value: object, field: str) -> int:
    if isinstance(value, bool):
        raise ValueError(f"{field} must be a positive integer")
    try:
        parsed = int(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError(f"{field} must be a positive integer") from exc
    if parsed <= 0:
        raise ValueError(f"{field} must be a positive integer")
    return parsed


def _sdk_versions() -> dict[str, object]:
    values: dict[str, object] = {}
    for distribution in ("databento", "databento-dbn"):
        try:
            values[distribution] = package_version(distribution)
        except PackageNotFoundError:
            values[distribution] = "unknown"
    return values


class HistoricalDayLock:
    """Non-blocking single-writer lock for one historical trading day."""

    def __init__(self, path: Path):
        self.path = path
        self.handle = None

    def __enter__(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        handle = self.path.open("a+b")
        if self.path.stat().st_size == 0:
            handle.write(b"0")
            handle.flush()
        handle.seek(0)
        try:
            if os.name == "nt":
                import msvcrt

                msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl

                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            handle.close()
            raise RuntimeError(
                f"historical backfill is already running for this day: {self.path.parent}"
            ) from exc
        self.handle = handle
        return self

    def __exit__(self, exc_type, exc, tb):
        if self.handle is None:
            return
        try:
            self.handle.seek(0)
            if os.name == "nt":
                import msvcrt

                msvcrt.locking(self.handle.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl

                fcntl.flock(self.handle.fileno(), fcntl.LOCK_UN)
        finally:
            self.handle.close()


@dataclass(frozen=True)
class HistoricalSchemaPlan:
    schema: str
    start_utc: str
    end_utc: str
    estimated_cost_usd: float
    estimated_billable_bytes: int

    def request_kwargs(self, parents: Sequence[str]) -> dict[str, object]:
        return {
            "dataset": DATASET,
            "schema": self.schema,
            "symbols": list(parents),
            "stype_in": STYPE_IN,
            "stype_out": STYPE_OUT,
            "start": self.start_utc,
            "end": self.end_utc,
        }

    def request_identity(self) -> dict[str, object]:
        return {
            "schema": self.schema,
            "start_utc": self.start_utc,
            "end_utc": self.end_utc,
        }


@dataclass(frozen=True)
class HistoricalDayPlan:
    trading_date: str
    provider_condition: str | None
    parents: tuple[str, ...]
    schemas: tuple[HistoricalSchemaPlan, ...]
    provider_condition_last_modified_date: str | None = None
    already_eligible: bool = False
    existing_bundle: str | None = None
    blocked_reasons: tuple[str, ...] = ()

    @property
    def estimated_cost_usd(self) -> float:
        return float(sum(item.estimated_cost_usd for item in self.schemas))

    @property
    def estimated_billable_bytes(self) -> int:
        return int(sum(item.estimated_billable_bytes for item in self.schemas))

    @property
    def executable(self) -> bool:
        return not self.blocked_reasons and not self.already_eligible and not self.existing_bundle

    @property
    def request(self) -> dict[str, object]:
        return {
            "version": REQUEST_CONTRACT_VERSION,
            "dataset": DATASET,
            "parents": list(self.parents),
            "stype_in": STYPE_IN,
            "stype_out": STYPE_OUT,
            "range_filter": RANGE_FILTER,
            "encoding": ENCODING,
            "compression": COMPRESSION,
            "components": [
                item.request_identity()
                for item in sorted(self.schemas, key=lambda value: value.schema)
            ],
        }

    @property
    def request_sha256(self) -> str:
        return _canonical_hash(self.request)

    @property
    def plan_sha256(self) -> str:
        return _canonical_hash(
            {
                "request_sha256": self.request_sha256,
                "provider_condition": self.provider_condition,
                "provider_condition_last_modified_date": (
                    self.provider_condition_last_modified_date
                ),
                "component_estimates": [
                    {
                        "schema": item.schema,
                        "estimated_cost_usd": item.estimated_cost_usd,
                        "estimated_billable_bytes": item.estimated_billable_bytes,
                    }
                    for item in sorted(self.schemas, key=lambda value: value.schema)
                ],
            }
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "trading_date": self.trading_date,
            "provider_condition": self.provider_condition,
            "provider_condition_last_modified_date": (
                self.provider_condition_last_modified_date
            ),
            "parents": list(self.parents),
            "schemas": [asdict(item) for item in self.schemas],
            "request": self.request,
            "request_sha256": self.request_sha256,
            "plan_sha256": self.plan_sha256,
            "estimated_cost_usd": self.estimated_cost_usd,
            "estimated_billable_bytes": self.estimated_billable_bytes,
            "estimate_enforcement_scope": "client_preflight_only",
            "already_eligible": self.already_eligible,
            "existing_bundle": self.existing_bundle,
            "blocked_reasons": list(self.blocked_reasons),
            "executable": self.executable,
        }


def request_windows(
    trading_day: date,
    schemas: Iterable[str] = SUPPORTED_SCHEMAS,
) -> tuple[tuple[str, datetime, datetime], ...]:
    requested = tuple(dict.fromkeys(str(item).lower() for item in schemas))
    unsupported = sorted(set(requested) - set(SUPPORTED_SCHEMAS))
    if unsupported:
        raise ValueError(f"unsupported historical schemas: {', '.join(unsupported)}")
    if set(requested) != set(SUPPORTED_SCHEMAS):
        raise ValueError(
            "a complete acquisition bundle requires TCBBO, statistics, and definition"
        )
    if not SUPPORTED_CALENDAR_START <= trading_day <= SUPPORTED_CALENDAR_END:
        raise ValueError(
            "historical acquisition calendar is verified only from "
            f"{SUPPORTED_CALENDAR_START.isoformat()} through "
            f"{SUPPORTED_CALENDAR_END.isoformat()}"
        )
    cash_open, _, _, stop_due = _market_times(trading_day)
    midnight = datetime.combine(trading_day, time.min, tzinfo=UTC)
    next_midnight = midnight + timedelta(days=1)
    windows = {
        "tcbbo": (cash_open, stop_due),
        "statistics": (midnight, next_midnight),
        "definition": (midnight, next_midnight),
    }
    return tuple((schema, *windows[schema]) for schema in requested)


def eligible_capture_dates(project_root: str | Path) -> set[str]:
    from .readiness import audit_training_readiness

    report = audit_training_readiness(Path(project_root).resolve())
    return {item.trading_date for item in report.sessions_detail if item.eligible}


def _condition_for_day(
    client: object, trading_day: date
) -> tuple[str | None, str | None, str | None]:
    try:
        rows = client.metadata.get_dataset_condition(
            dataset=DATASET,
            start_date=trading_day.isoformat(),
            end_date=trading_day.isoformat(),
        )
    except Exception as exc:
        return None, None, f"provider condition lookup failed: {type(exc).__name__}: {exc}"
    match = next(
        (
            row
            for row in rows
            if isinstance(row, Mapping)
            and str(row.get("date")) == trading_day.isoformat()
        ),
        None,
    )
    if match is None:
        return None, None, "provider returned no condition for the requested date"
    condition = str(match.get("condition") or "").lower() or None
    last_modified_date = str(match.get("last_modified_date") or "") or None
    if last_modified_date is not None:
        try:
            date.fromisoformat(last_modified_date)
        except ValueError:
            return condition, last_modified_date, "provider condition revision date is invalid"
    elif condition == "available":
        return condition, None, "provider condition revision date is missing"
    if condition != "available":
        return (
            condition,
            last_modified_date,
            f"provider condition is {condition or 'unknown'}",
        )
    return condition, last_modified_date, None


def _assert_provider_plan_current(client: object, plan: HistoricalDayPlan) -> None:
    try:
        trading_day = date.fromisoformat(plan.trading_date)
    except ValueError as exc:
        raise ValueError("historical plan trading date is invalid") from exc
    condition, last_modified_date, issue = _condition_for_day(client, trading_day)
    if issue:
        raise ValueError(f"provider revision recheck failed: {issue}")
    if (
        condition != plan.provider_condition
        or last_modified_date != plan.provider_condition_last_modified_date
    ):
        raise ValueError(
            "provider condition/revision changed after planning; generate and approve a new plan"
        )


def _schema_range_issue(
    dataset_range: Mapping[str, object],
    *,
    schema: str,
    start_utc: datetime,
    end_utc: datetime,
) -> str | None:
    schema_ranges = dataset_range.get("schema")
    if not isinstance(schema_ranges, Mapping):
        return "provider dataset range contains no per-schema ranges"
    values = schema_ranges.get(schema)
    if not isinstance(values, Mapping):
        return f"provider does not expose the {schema} schema"
    try:
        available_start = _parse_utc(str(values["start"]))
        available_end = _parse_utc(str(values["end"]))
    except (KeyError, TypeError, ValueError) as exc:
        return f"provider returned an invalid {schema} availability range: {exc}"
    if start_utc < available_start:
        return f"{schema} begins at {_iso_utc(available_start)}"
    if end_utc > available_end:
        return f"{schema} is available only through {_iso_utc(available_end)}"
    return None


def plan_historical_day(
    client: object,
    *,
    trading_day: date,
    parents: Iterable[str] = DEFAULT_OPTION_PARENTS,
    schemas: Iterable[str] = SUPPORTED_SCHEMAS,
    already_eligible: bool = False,
    existing_bundle: str | None = None,
    dataset_range: Mapping[str, object] | None = None,
) -> HistoricalDayPlan:
    normalized_parents = tuple(dict.fromkeys(str(item).upper() for item in parents))
    if not normalized_parents:
        raise ValueError("at least one option parent is required")
    if normalized_parents != tuple(DEFAULT_OPTION_PARENTS):
        raise ValueError("historical bundle requires the canonical nine option parents")
    blocked: list[str] = []
    try:
        windows = request_windows(trading_day, schemas)
    except ValueError as exc:
        return HistoricalDayPlan(
            trading_date=trading_day.isoformat(),
            provider_condition=None,
            provider_condition_last_modified_date=None,
            parents=normalized_parents,
            schemas=(),
            already_eligible=already_eligible,
            existing_bundle=existing_bundle,
            blocked_reasons=(str(exc),),
        )

    condition, condition_last_modified_date, condition_issue = _condition_for_day(
        client, trading_day
    )
    if condition_issue:
        blocked.append(condition_issue)
    if dataset_range is None:
        try:
            dataset_range = client.metadata.get_dataset_range(dataset=DATASET)
        except Exception as exc:
            blocked.append(f"dataset range lookup failed: {type(exc).__name__}: {exc}")
            dataset_range = {}

    planned_schemas: list[HistoricalSchemaPlan] = []
    for schema, start_utc, end_utc in windows:
        if issue := _schema_range_issue(
            dataset_range,
            schema=schema,
            start_utc=start_utc,
            end_utc=end_utc,
        ):
            blocked.append(issue)
            continue
        request = {
            "dataset": DATASET,
            "schema": schema,
            "symbols": list(normalized_parents),
            "stype_in": STYPE_IN,
            "start": _iso_utc(start_utc),
            "end": _iso_utc(end_utc),
        }
        try:
            cost = _finite_nonnegative(
                client.metadata.get_cost(**request), f"{schema} estimated cost"
            )
            billable_bytes = int(client.metadata.get_billable_size(**request))
            if billable_bytes < 0:
                raise ValueError(f"{schema} estimated billable bytes must be nonnegative")
        except Exception as exc:
            blocked.append(f"{schema} estimate failed: {type(exc).__name__}: {exc}")
            continue
        if schema == "tcbbo" and billable_bytes == 0:
            blocked.append("TCBBO estimate contains zero bytes")
        planned_schemas.append(
            HistoricalSchemaPlan(
                schema=schema,
                start_utc=_iso_utc(start_utc),
                end_utc=_iso_utc(end_utc),
                estimated_cost_usd=cost,
                estimated_billable_bytes=billable_bytes,
            )
        )

    if planned_schemas and not math.isfinite(
        sum(item.estimated_cost_usd for item in planned_schemas)
    ):
        blocked.append("total estimated cost is not finite")

    return HistoricalDayPlan(
        trading_date=trading_day.isoformat(),
        provider_condition=condition,
        provider_condition_last_modified_date=condition_last_modified_date,
        parents=normalized_parents,
        schemas=tuple(planned_schemas),
        already_eligible=already_eligible,
        existing_bundle=existing_bundle,
        blocked_reasons=tuple(dict.fromkeys(blocked)),
    )


def _option_root(raw_symbol: object) -> str:
    return str(raw_symbol or "")[:6].strip().upper()


def _mapping_summary(
    mappings: Mapping[object, object], trading_day: date
) -> tuple[str, dict[int, str]]:
    digest = hashlib.sha256()
    active_instrument_roots: dict[int, str] = {}
    string_keys = {str(key): key for key in mappings}
    for raw_symbol in sorted(string_keys):
        segments = mappings[string_keys[raw_symbol]]
        if not isinstance(segments, Sequence) or isinstance(segments, (str, bytes)):
            raise ValueError(f"DBN mapping entry is malformed for {raw_symbol}")
        normalized_segments: list[tuple[str, str, str]] = []
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
            normalized_segments.append(
                (start_date.isoformat(), end_date.isoformat(), mapped_symbol)
            )
            if start_date <= trading_day < end_date:
                active_instrument_roots[instrument_id] = _option_root(raw_symbol)
        digest.update(raw_symbol.encode("utf-8"))
        digest.update(b"\0")
        for values in sorted(normalized_segments):
            digest.update("|".join(values).encode("utf-8"))
            digest.update(b"\0")
    return digest.hexdigest(), active_instrument_roots


def validate_dbn_component(
    path: Path,
    schema_plan: HistoricalSchemaPlan,
    parents: Sequence[str],
    *,
    store_factory: Callable[[Path], object] | None = None,
) -> dict[str, object]:
    """Decode and validate one compressed DBN component without a DataFrame."""

    bento_warning: type[Warning] = Warning
    if store_factory is None:
        import databento as db
        from databento.common.error import BentoWarning

        store_factory = db.DBNStore.from_file
        bento_warning = BentoWarning
    store = store_factory(path)
    metadata = store.metadata
    schema = schema_plan.schema

    actual_dataset = str(getattr(metadata, "dataset", "") or "")
    if actual_dataset != DATASET:
        raise ValueError(f"unexpected DBN dataset: {actual_dataset or '<missing>'}")
    actual_schema = str(getattr(metadata, "schema", "") or "").lower()
    if actual_schema.startswith("schema."):
        actual_schema = actual_schema.split(".", 1)[1]
    if actual_schema != schema:
        raise ValueError(f"unexpected DBN schema: {actual_schema or '<missing>'} != {schema}")
    actual_stype_in = str(getattr(metadata, "stype_in", "") or "").lower()
    actual_stype_out = str(getattr(metadata, "stype_out", "") or "").lower()
    if actual_stype_in != STYPE_IN or actual_stype_out != STYPE_OUT:
        raise ValueError(
            "unexpected DBN symbology: "
            f"{actual_stype_in or '<missing>'}->{actual_stype_out or '<missing>'}"
        )
    actual_compression = str(getattr(store, "compression", "") or "").lower()
    if actual_compression != COMPRESSION:
        raise ValueError(
            f"unexpected DBN compression: {actual_compression or '<missing>'}"
        )

    expected_start_ns = _datetime_ns(_parse_utc(schema_plan.start_utc))
    expected_end_ns = _datetime_ns(_parse_utc(schema_plan.end_utc))
    actual_start_ns = _metadata_ns(getattr(metadata, "start", None), "start")
    actual_end_ns = _metadata_ns(getattr(metadata, "end", None), "end")
    if actual_start_ns != expected_start_ns or actual_end_ns != expected_end_ns:
        raise ValueError(
            "DBN request window mismatch: "
            f"{_ns_iso(actual_start_ns)}..{_ns_iso(actual_end_ns)} != "
            f"{schema_plan.start_utc}..{schema_plan.end_utc}"
        )

    actual_symbols = tuple(str(item).upper() for item in getattr(metadata, "symbols", ()))
    expected_symbols = tuple(str(item).upper() for item in parents)
    if actual_symbols != expected_symbols:
        raise ValueError("DBN requested parent symbols do not match the historical plan")
    if tuple(getattr(metadata, "not_found", ()) or ()):
        raise ValueError("DBN metadata reports unresolved requested symbols")
    if tuple(getattr(metadata, "partial", ()) or ()):
        raise ValueError("DBN metadata reports partially resolved requested symbols")

    try:
        trading_day = date.fromisoformat(schema_plan.start_utc[:10])
    except ValueError as exc:
        raise ValueError("historical component start date is invalid") from exc
    mappings = getattr(metadata, "mappings", None)
    if not isinstance(mappings, Mapping) or not mappings:
        raise ValueError("DBN metadata contains no instrument mappings")
    mapping_sha256, active_instrument_roots = _mapping_summary(mappings, trading_day)
    required_roots = tuple(item.removesuffix(".OPT") for item in expected_symbols)
    mapped_roots = set(active_instrument_roots.values())
    missing_mappings = sorted(set(required_roots) - mapped_roots)
    if missing_mappings:
        raise ValueError(
            "DBN mappings omit requested option families: " + ", ".join(missing_mappings)
        )

    record_count = 0
    first_ts_recv_ns: int | None = None
    last_ts_recv_ns: int | None = None
    first_ts_event_ns: int | None = None
    last_ts_event_ns: int | None = None
    record_types: Counter[int] = Counter()
    family_record_counts: Counter[str] = Counter()
    open_interest_counts: Counter[str] = Counter()
    undefined_open_interest_count = 0
    option_definition_count = 0
    two_sided_tcbbo_count = 0
    expected_rtype = _EXPECTED_RTYPE[schema]
    with warnings.catch_warnings():
        warnings.simplefilter("error", bento_warning)
        for record in store:
            try:
                rtype = int(record.rtype)
                ts_recv = int(record.ts_recv)
                ts_event = int(record.ts_event)
                instrument_id = int(record.instrument_id)
            except (AttributeError, TypeError, ValueError, OverflowError) as exc:
                raise ValueError(f"malformed {schema} DBN record") from exc
            if ts_recv == _UNDEF_TIMESTAMP or ts_event == _UNDEF_TIMESTAMP:
                raise ValueError(f"{schema} record contains an undefined timestamp")
            if not expected_start_ns <= ts_recv < expected_end_ns:
                raise ValueError(f"{schema} record falls outside its ts_recv request window")
            mapped_family = active_instrument_roots.get(instrument_id, "")
            if not mapped_family:
                raise ValueError(f"{schema} record has no active point-in-time mapping")
            if schema == "definition":
                family = _option_root(getattr(record, "raw_symbol", ""))
                if family != mapped_family:
                    raise ValueError("definition raw symbol conflicts with its DBN mapping")
            else:
                family = mapped_family
            record_count += 1
            record_types[rtype] += 1
            family_record_counts[family] += 1
            first_ts_recv_ns = ts_recv if first_ts_recv_ns is None else min(first_ts_recv_ns, ts_recv)
            last_ts_recv_ns = ts_recv if last_ts_recv_ns is None else max(last_ts_recv_ns, ts_recv)
            first_ts_event_ns = ts_event if first_ts_event_ns is None else min(first_ts_event_ns, ts_event)
            last_ts_event_ns = ts_event if last_ts_event_ns is None else max(last_ts_event_ns, ts_event)
            if schema == "statistics":
                try:
                    if int(record.stat_type) == _OPEN_INTEREST_STAT_TYPE:
                        if int(record.quantity) == _UNDEF_STAT_QUANTITY:
                            undefined_open_interest_count += 1
                        else:
                            open_interest_counts[family] += 1
                except (AttributeError, TypeError, ValueError, OverflowError) as exc:
                    raise ValueError("malformed statistics DBN record") from exc
            elif schema == "definition":
                try:
                    instrument_class = int(record.instrument_class)
                    expiration = int(record.expiration)
                    strike_price = int(record.strike_price)
                except (AttributeError, TypeError, ValueError, OverflowError) as exc:
                    raise ValueError("malformed option definition record") from exc
                if instrument_class not in _OPTION_INSTRUMENT_CLASSES:
                    raise ValueError("definition contains a non-call/non-put instrument")
                if expiration == _UNDEF_TIMESTAMP:
                    raise ValueError("definition contains an undefined expiration")
                if strike_price == _UNDEF_PRICE:
                    raise ValueError("definition contains an undefined strike")
                option_definition_count += 1
            elif schema == "tcbbo":
                try:
                    level = record.levels[0]
                    trade_price = int(record.price)
                    bid_price = int(level.bid_px)
                    ask_price = int(level.ask_px)
                    bid_size = int(level.bid_sz)
                    ask_size = int(level.ask_sz)
                except (AttributeError, IndexError, TypeError, ValueError, OverflowError) as exc:
                    raise ValueError("malformed TCBBO trade/BBO record") from exc
                if trade_price == _UNDEF_PRICE:
                    raise ValueError("TCBBO record contains an undefined trade price")
                if (
                    bid_price != _UNDEF_PRICE
                    and ask_price != _UNDEF_PRICE
                    and bid_size > 0
                    and ask_size > 0
                ):
                    two_sided_tcbbo_count += 1

    if record_count == 0:
        raise ValueError(f"{schema} DBN component contains zero records")
    if set(record_types) != {expected_rtype}:
        raise ValueError(
            f"{schema} DBN component contains unexpected record types: "
            + ", ".join(str(item) for item in sorted(record_types))
        )
    missing_records = sorted(set(required_roots) - set(family_record_counts))
    missing_open_interest: list[str] = []
    if schema == "statistics":
        missing_open_interest = sorted(set(required_roots) - set(open_interest_counts))
        if not open_interest_counts:
            raise ValueError("statistics contains no usable open-interest records")
    if schema == "definition" and option_definition_count != record_count:
        raise ValueError("definition component is not entirely call/put option evidence")

    return {
        "dbn_format_version": int(getattr(metadata, "version", 0) or 0),
        "dbn_dataset": actual_dataset,
        "dbn_schema": actual_schema,
        "dbn_start_utc": _ns_iso(actual_start_ns),
        "dbn_end_utc": _ns_iso(actual_end_ns),
        "dbn_stype_in": actual_stype_in,
        "dbn_stype_out": actual_stype_out,
        "dbn_compression": actual_compression,
        "dbn_symbols": list(actual_symbols),
        "dbn_mapping_count": len(mappings),
        "dbn_mapping_sha256": mapping_sha256,
        "record_count": record_count,
        "record_types": {str(key): value for key, value in sorted(record_types.items())},
        "first_ts_recv_ns": first_ts_recv_ns,
        "last_ts_recv_ns": last_ts_recv_ns,
        "first_ts_event_ns": first_ts_event_ns,
        "last_ts_event_ns": last_ts_event_ns,
        "family_record_counts": {
            key: family_record_counts[key] for key in sorted(required_roots)
        },
        "open_interest_record_counts": {
            key: open_interest_counts[key] for key in sorted(required_roots)
        }
        if schema == "statistics"
        else {},
        "undefined_open_interest_record_count": undefined_open_interest_count,
        "option_definition_count": option_definition_count,
        "two_sided_tcbbo_count": two_sided_tcbbo_count,
        "two_sided_tcbbo_coverage": (
            two_sided_tcbbo_count / record_count if schema == "tcbbo" else None
        ),
        "missing_record_families": missing_records,
        "missing_open_interest_families": missing_open_interest,
        "family_coverage_complete": not missing_records
        and not missing_open_interest
        and undefined_open_interest_count == 0,
        "two_sided_tcbbo_coverage_pass": (
            two_sided_tcbbo_count / record_count >= _MIN_TWO_SIDED_TCBBO_COVERAGE
            if schema == "tcbbo"
            else None
        ),
    }


ComponentValidator = Callable[
    [Path, HistoricalSchemaPlan, Sequence[str]], Mapping[str, object]
]


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


def _bundle_identity(
    *,
    trading_date: str,
    request_sha256: str,
    components: Sequence[Mapping[str, object]],
    legacy_evidence: Mapping[str, object] | None = None,
) -> dict[str, object]:
    identity: dict[str, object] = {
        "version": HISTORICAL_BUNDLE_VERSION,
        "dataset": DATASET,
        "trading_date": trading_date,
        "request_sha256": request_sha256,
        "components": [
            _component_identity(item)
            for item in sorted(components, key=lambda value: str(value["schema"]))
        ],
    }
    if legacy_evidence is not None:
        identity["legacy_evidence"] = dict(legacy_evidence)
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


def _read_manifest(path: Path) -> Mapping[str, object]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, Mapping):
        raise ValueError("historical bundle manifest root must be an object")
    return payload


def _resolve_component(day_dir: Path, component: Mapping[str, object]) -> Path:
    source = (day_dir / str(component["file"])).resolve()
    try:
        source.relative_to(day_dir)
    except ValueError as exc:
        raise ValueError("historical component path escapes its day directory") from exc
    return source


def _verify_component_file(
    *, source: Path, component: Mapping[str, object], schema: str
) -> None:
    if not source.is_file():
        raise FileNotFoundError(source)
    try:
        expected_bytes = int(component["file_bytes"])
        expected_sha256 = str(component["file_sha256"])
    except (KeyError, TypeError, ValueError) as exc:
        raise ValueError(f"historical {schema} component metadata is malformed") from exc
    if expected_bytes <= 0 or source.stat().st_size != expected_bytes:
        raise ValueError(f"historical component size mismatch: {schema}")
    if _sha256(source) != expected_sha256:
        raise ValueError(f"historical component hash mismatch: {schema}")


def _schema_plan_from_component(component: Mapping[str, object]) -> HistoricalSchemaPlan:
    try:
        return HistoricalSchemaPlan(
            schema=str(component["schema"]),
            start_utc=str(component["requested_start_utc"]),
            end_utc=str(component["requested_end_utc"]),
            estimated_cost_usd=float(component.get("estimated_cost_usd", 0.0)),
            estimated_billable_bytes=int(component.get("estimated_billable_bytes", 0)),
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise ValueError("historical component request metadata is malformed") from exc


def _verify_v2_manifest(
    path: Path,
    payload: Mapping[str, object],
    validate_component: ComponentValidator,
) -> dict[str, object]:
    if payload.get("dataset") != DATASET or payload.get("status") != "complete":
        raise ValueError("historical bundle manifest is not complete OPRA.PILLAR evidence")
    if payload.get("source_kind") != "databento_historical":
        raise ValueError("historical bundle manifest has an unexpected source kind")
    request = payload.get("request")
    if not isinstance(request, Mapping):
        raise ValueError("historical bundle manifest contains no request identity")
    request_sha256 = str(payload.get("request_sha256") or "")
    if _canonical_hash(request) != request_sha256:
        raise ValueError("historical request identity hash mismatch")
    if (
        request.get("version") != REQUEST_CONTRACT_VERSION
        or request.get("dataset") != DATASET
        or request.get("stype_in") != STYPE_IN
        or request.get("stype_out") != STYPE_OUT
        or request.get("range_filter") != RANGE_FILTER
        or request.get("encoding") != ENCODING
        or request.get("compression") != COMPRESSION
    ):
        raise ValueError("historical request contract is unsupported")
    parents = request.get("parents")
    if not isinstance(parents, list) or not parents or not all(isinstance(x, str) for x in parents):
        raise ValueError("historical request parent list is malformed")
    if tuple(parents) != tuple(DEFAULT_OPTION_PARENTS):
        raise ValueError("standard MarketPin bundle requires the canonical nine parents")
    components = payload.get("components")
    if not isinstance(components, list) or not components:
        raise ValueError("historical bundle manifest contains no components")

    day_dir = path.parent.resolve()
    seen: set[str] = set()
    mapping_hashes: set[str] = set()
    for item in components:
        if not isinstance(item, Mapping):
            raise ValueError("historical bundle component must be an object")
        schema = str(item.get("schema") or "")
        if schema in seen or schema not in SUPPORTED_SCHEMAS:
            raise ValueError(f"invalid or duplicate historical component schema: {schema}")
        seen.add(schema)
        source = _resolve_component(day_dir, item)
        _verify_component_file(source=source, component=item, schema=schema)
        schema_plan = _schema_plan_from_component(item)
        observed_validation = dict(validate_component(source, schema_plan, tuple(parents)))
        stored_validation = item.get("validation")
        if not isinstance(stored_validation, Mapping):
            raise ValueError(f"historical {schema} component has no validation summary")
        if observed_validation != dict(stored_validation):
            raise ValueError(f"historical {schema} decoded validation summary mismatch")
        mapping_hashes.add(str(observed_validation.get("dbn_mapping_sha256") or ""))
    missing = sorted(set(SUPPORTED_SCHEMAS) - seen)
    if missing:
        raise ValueError(f"historical bundle is missing components: {', '.join(missing)}")
    if len(mapping_hashes) != 1 or "" in mapping_hashes:
        raise ValueError("historical bundle components do not share one DBN mapping identity")

    request_components = request.get("components")
    expected_request_components = sorted(
        [
            {
                "schema": str(item["schema"]),
                "start_utc": str(item["requested_start_utc"]),
                "end_utc": str(item["requested_end_utc"]),
            }
            for item in components
        ],
        key=lambda item: item["schema"],
    )
    if request_components != expected_request_components:
        raise ValueError("historical request windows do not match component evidence")
    trading_date = str(payload.get("trading_date") or "")
    if not trading_date or any(
        str(item["start_utc"])[:10] != trading_date
        for item in expected_request_components
    ):
        raise ValueError("historical component windows do not match the trading date")
    try:
        canonical_components = sorted(
            [
                {
                    "schema": schema,
                    "start_utc": _iso_utc(start_utc),
                    "end_utc": _iso_utc(end_utc),
                }
                for schema, start_utc, end_utc in request_windows(
                    date.fromisoformat(trading_date)
                )
            ],
            key=lambda item: item["schema"],
        )
    except ValueError as exc:
        raise ValueError("historical trading date has no verified canonical window") from exc
    if expected_request_components != canonical_components:
        raise ValueError("historical component windows are not canonical for the session")

    planning = payload.get("planning")
    if not isinstance(planning, Mapping):
        raise ValueError("historical bundle contains no planning evidence")
    legacy_evidence = payload.get("legacy_evidence")
    is_legacy_attestation = legacy_evidence is not None
    plan_sha256 = str(planning.get("plan_sha256") or "")
    if not is_legacy_attestation:
        if payload.get("provider_condition") != "available":
            raise ValueError("native historical acquisition was not provider-available")
        revision = str(payload.get("provider_condition_last_modified_date") or "")
        try:
            date.fromisoformat(revision)
        except ValueError as exc:
            raise ValueError("native historical acquisition revision is invalid") from exc
        if not plan_sha256:
            raise ValueError("native historical acquisition has no reviewed plan identity")
    else:
        if payload.get("provider_condition_at_attestation") != "available":
            raise ValueError("legacy bundle was not provider-available when attested")
        revision_at_attestation = str(
            payload.get("provider_condition_last_modified_date_at_attestation") or ""
        )
        try:
            date.fromisoformat(revision_at_attestation)
        except ValueError as exc:
            raise ValueError("legacy attestation provider revision is invalid") from exc
    if plan_sha256:
        component_estimates = planning.get("component_estimates")
        if not isinstance(component_estimates, list):
            raise ValueError("historical planning estimates are missing")
        normalized_estimates = sorted(
            [dict(item) for item in component_estimates if isinstance(item, Mapping)],
            key=lambda item: str(item.get("schema") or ""),
        )
        if len(normalized_estimates) != len(component_estimates):
            raise ValueError("historical planning estimates are malformed")
        expected_plan_sha256 = _canonical_hash(
            {
                "request_sha256": request_sha256,
                "provider_condition": payload.get("provider_condition"),
                "provider_condition_last_modified_date": payload.get(
                    "provider_condition_last_modified_date"
                ),
                "component_estimates": normalized_estimates,
            }
        )
        if expected_plan_sha256 != plan_sha256:
            raise ValueError("historical planning identity hash mismatch")
    provenance_sha256 = str(payload.get("provenance_sha256") or "")
    if _canonical_hash(_provenance_identity(payload)) != provenance_sha256:
        raise ValueError("historical provenance attestation hash mismatch")
    attestation_sha256 = _canonical_hash(
        {
            "bundle_sha256": str(payload.get("bundle_sha256") or ""),
            "provenance_sha256": provenance_sha256,
        }
    )
    if attestation_sha256 != str(payload.get("attestation_sha256") or ""):
        raise ValueError("historical root attestation hash mismatch")

    if legacy_evidence is not None:
        if not isinstance(legacy_evidence, Mapping):
            raise ValueError("historical legacy evidence link is malformed")
        try:
            legacy_name = str(legacy_evidence["manifest_file"])
            legacy_sha256 = str(legacy_evidence["manifest_sha256"])
            legacy_bundle_sha256 = str(legacy_evidence["bundle_sha256"])
        except KeyError as exc:
            raise ValueError("historical legacy evidence link is incomplete") from exc
        legacy_path = (day_dir / legacy_name).resolve()
        try:
            legacy_path.relative_to(day_dir)
        except ValueError as exc:
            raise ValueError("legacy manifest path escapes its day directory") from exc
        if legacy_path == path:
            raise ValueError("v2 attestation cannot reference itself as legacy evidence")
        if not legacy_path.is_file() or _sha256(legacy_path) != legacy_sha256:
            raise ValueError("referenced legacy manifest hash mismatch")
        legacy_payload = _read_manifest(legacy_path)
        if legacy_payload.get("version") != LEGACY_BUNDLE_VERSION:
            raise ValueError("referenced legacy manifest has the wrong version")
        verified_legacy = _verify_legacy_v1_manifest(
            legacy_path, legacy_payload, validate_component
        )
        if str(verified_legacy.get("bundle_sha256") or "") != legacy_bundle_sha256:
            raise ValueError("referenced legacy bundle identity mismatch")

    identity = _bundle_identity(
        trading_date=trading_date,
        request_sha256=request_sha256,
        components=components,
        legacy_evidence=legacy_evidence if isinstance(legacy_evidence, Mapping) else None,
    )
    if _canonical_hash(identity) != str(payload.get("bundle_sha256") or ""):
        raise ValueError("historical bundle identity hash mismatch")
    return dict(payload)


def _verify_legacy_v1_manifest(
    path: Path,
    payload: Mapping[str, object],
    validate_component: ComponentValidator,
) -> dict[str, object]:
    if payload.get("dataset") != DATASET or payload.get("status") != "complete":
        raise ValueError("legacy historical bundle is not complete OPRA.PILLAR evidence")
    parents = payload.get("parents")
    components = payload.get("components")
    if not isinstance(parents, list) or not all(isinstance(x, str) for x in parents):
        raise ValueError("legacy historical bundle parents are malformed")
    if tuple(parents) != tuple(DEFAULT_OPTION_PARENTS):
        raise ValueError("legacy MarketPin bundle does not contain the canonical nine parents")
    if not isinstance(components, list) or not components:
        raise ValueError("legacy historical bundle contains no components")
    day_dir = path.parent.resolve()
    seen: set[str] = set()
    mapping_hashes: set[str] = set()
    legacy_identity_components: list[dict[str, object]] = []
    for item in components:
        if not isinstance(item, Mapping):
            raise ValueError("legacy historical component must be an object")
        schema = str(item.get("schema") or "")
        if schema in seen or schema not in SUPPORTED_SCHEMAS:
            raise ValueError(f"invalid or duplicate historical component schema: {schema}")
        seen.add(schema)
        source = _resolve_component(day_dir, item)
        _verify_component_file(source=source, component=item, schema=schema)
        try:
            schema_plan = HistoricalSchemaPlan(
                schema=schema,
                start_utc=str(item["start_utc"]),
                end_utc=str(item["end_utc"]),
                estimated_cost_usd=float(item.get("estimated_cost_usd", 0.0)),
                estimated_billable_bytes=int(item.get("estimated_billable_bytes", 0)),
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise ValueError("legacy historical component metadata is malformed") from exc
        validation = dict(validate_component(source, schema_plan, tuple(parents)))
        mapping_hashes.add(str(validation.get("dbn_mapping_sha256") or ""))
        legacy_identity_components.append(
            {
                "schema": schema,
                "start_utc": schema_plan.start_utc,
                "end_utc": schema_plan.end_utc,
                "file": str(item["file"]),
                "file_bytes": int(item["file_bytes"]),
                "file_sha256": str(item["file_sha256"]),
            }
        )
    if set(seen) != set(SUPPORTED_SCHEMAS):
        raise ValueError("legacy historical bundle is missing required components")
    if len(mapping_hashes) != 1 or "" in mapping_hashes:
        raise ValueError("legacy historical components do not share one DBN mapping identity")
    legacy_identity = {
        "version": LEGACY_BUNDLE_VERSION,
        "dataset": DATASET,
        "trading_date": str(payload["trading_date"]),
        "parents": list(parents),
        "components": sorted(legacy_identity_components, key=lambda item: item["schema"]),
    }
    if _canonical_hash(legacy_identity) != str(payload.get("bundle_sha256") or ""):
        raise ValueError("legacy historical bundle identity hash mismatch")
    canonical_components = sorted(
        [
            {
                "schema": schema,
                "start_utc": _iso_utc(start_utc),
                "end_utc": _iso_utc(end_utc),
            }
            for schema, start_utc, end_utc in request_windows(
                date.fromisoformat(str(payload["trading_date"]))
            )
        ],
        key=lambda item: item["schema"],
    )
    observed_components = sorted(
        [
            {
                "schema": item["schema"],
                "start_utc": item["start_utc"],
                "end_utc": item["end_utc"],
            }
            for item in legacy_identity_components
        ],
        key=lambda item: item["schema"],
    )
    if observed_components != canonical_components:
        raise ValueError("legacy historical component windows are not canonical")
    return dict(payload)


def verify_bundle_manifest(
    manifest_path: str | Path,
    *,
    validate_component: ComponentValidator = validate_dbn_component,
) -> dict[str, object]:
    path = Path(manifest_path).resolve()
    payload = _read_manifest(path)
    if payload.get("version") == HISTORICAL_BUNDLE_VERSION:
        return _verify_v2_manifest(path, payload, validate_component)
    if payload.get("version") == LEGACY_BUNDLE_VERSION:
        return _verify_legacy_v1_manifest(path, payload, validate_component)
    raise ValueError("unsupported historical bundle manifest version")


def _request_from_legacy(payload: Mapping[str, object]) -> dict[str, object]:
    components = payload.get("components")
    parents = payload.get("parents")
    if not isinstance(components, list) or not isinstance(parents, list):
        raise ValueError("legacy historical bundle request identity is malformed")
    normalized_components: list[dict[str, object]] = []
    for item in components:
        if not isinstance(item, Mapping):
            raise ValueError("legacy historical bundle component is malformed")
        normalized_components.append(
            {
                "schema": str(item["schema"]),
                "start_utc": str(item["start_utc"]),
                "end_utc": str(item["end_utc"]),
            }
        )
    return {
        "version": REQUEST_CONTRACT_VERSION,
        "dataset": DATASET,
        "parents": list(parents),
        "stype_in": STYPE_IN,
        "stype_out": STYPE_OUT,
        "range_filter": RANGE_FILTER,
        "encoding": ENCODING,
        "compression": COMPRESSION,
        "components": sorted(normalized_components, key=lambda item: item["schema"]),
    }


def _assert_manifest_matches_plan(
    payload: Mapping[str, object], plan: HistoricalDayPlan
) -> None:
    request = (
        payload.get("request")
        if payload.get("version") == HISTORICAL_BUNDLE_VERSION
        else _request_from_legacy(payload)
    )
    if not isinstance(request, Mapping) or dict(request) != plan.request:
        raise ValueError(
            "existing historical bundle request does not match the requested parents/windows"
        )
    if str(payload.get("trading_date") or "") != plan.trading_date:
        raise ValueError("existing historical bundle trading date does not match the plan")
    if payload.get("version") != HISTORICAL_BUNDLE_VERSION:
        raise ValueError(
            "legacy v1 bundle has no provider-revision attestation; create a v2 sidecar"
        )
    if payload.get("legacy_evidence") is not None:
        bundle_condition = payload.get("provider_condition_at_attestation")
        bundle_revision = payload.get(
            "provider_condition_last_modified_date_at_attestation"
        )
    else:
        bundle_condition = payload.get("provider_condition")
        bundle_revision = payload.get("provider_condition_last_modified_date")
    if (
        bundle_condition != plan.provider_condition
        or bundle_revision != plan.provider_condition_last_modified_date
    ):
        raise ValueError(
            "existing historical bundle provider revision differs from the current plan"
        )


def discover_existing_bundle(
    output_root: str | Path,
    trading_day: date,
    *,
    expected_plan: HistoricalDayPlan | None = None,
    validate_component: ComponentValidator = validate_dbn_component,
) -> str | None:
    day_dir = Path(output_root).resolve() / trading_day.isoformat()
    candidates = (day_dir / "manifest.v2.json", day_dir / "manifest.json")
    manifest = next((item for item in candidates if item.is_file()), None)
    if manifest is None:
        return None
    payload = verify_bundle_manifest(manifest, validate_component=validate_component)
    if expected_plan is not None:
        _assert_manifest_matches_plan(payload, expected_plan)
    return str(manifest)


def _component_from_existing_file(
    *,
    path: Path,
    sidecar_path: Path,
    plan: HistoricalDayPlan,
    schema_plan: HistoricalSchemaPlan,
    validate_component: ComponentValidator,
) -> dict[str, object] | None:
    if not path.is_file():
        return None
    if not sidecar_path.is_file():
        raise ValueError(
            f"orphan historical component has no acquisition sidecar: {path}"
        )
    sidecar = _read_manifest(sidecar_path)
    if (
        sidecar.get("request_sha256") != plan.request_sha256
        or sidecar.get("plan_sha256") != plan.plan_sha256
        or sidecar.get("provider_condition_last_modified_date")
        != plan.provider_condition_last_modified_date
        or sidecar.get("schema") != schema_plan.schema
        or sidecar.get("start_utc") != schema_plan.start_utc
        or sidecar.get("end_utc") != schema_plan.end_utc
    ):
        raise ValueError(f"orphan component acquisition identity mismatch: {path}")
    if int(sidecar.get("file_bytes", -1)) != path.stat().st_size:
        raise ValueError(f"orphan component size mismatch: {path}")
    file_sha256 = _sha256(path)
    if str(sidecar.get("file_sha256") or "") != file_sha256:
        raise ValueError(f"orphan component hash mismatch: {path}")
    validation = dict(validate_component(path, schema_plan, plan.parents))
    if validation != dict(sidecar.get("validation") or {}):
        raise ValueError(f"orphan component decoded validation mismatch: {path}")
    return {
        "schema": schema_plan.schema,
        "requested_start_utc": schema_plan.start_utc,
        "requested_end_utc": schema_plan.end_utc,
        "file": path.name,
        "file_bytes": path.stat().st_size,
        "file_sha256": file_sha256,
        "estimated_billable_bytes": schema_plan.estimated_billable_bytes,
        "estimated_cost_usd": schema_plan.estimated_cost_usd,
        "validation": validation,
    }


def _load_staged_component(
    *,
    stage_dir: Path,
    plan: HistoricalDayPlan,
    schema_plan: HistoricalSchemaPlan,
    validate_component: ComponentValidator,
) -> tuple[Path, dict[str, object]] | None:
    filename = f"opra-pillar.{schema_plan.schema}.verified.dbn.zst"
    verified_path = stage_dir / filename
    sidecar_path = stage_dir / f"{filename}.json"
    if not verified_path.exists() and not sidecar_path.exists():
        return None
    if sidecar_path.exists() and not verified_path.is_file():
        raise ValueError(
            f"incomplete staged {schema_plan.schema} component requires inspection: {stage_dir}"
        )
    if verified_path.is_file() and not sidecar_path.exists():
        validation = dict(
            validate_component(verified_path, schema_plan, plan.parents)
        )
        file_sha256 = _sha256(verified_path)
        _atomic_json(
            sidecar_path,
            {
                "request_sha256": plan.request_sha256,
                "plan_sha256": plan.plan_sha256,
                "provider_condition_last_modified_date": (
                    plan.provider_condition_last_modified_date
                ),
                "schema": schema_plan.schema,
                "start_utc": schema_plan.start_utc,
                "end_utc": schema_plan.end_utc,
                "file_bytes": verified_path.stat().st_size,
                "file_sha256": file_sha256,
                "validation": validation,
                "state": "verified_recovered",
            },
        )
    try:
        sidecar = _read_manifest(sidecar_path)
        if sidecar.get("request_sha256") != plan.request_sha256:
            raise ValueError("staged request hash mismatch")
        if sidecar.get("plan_sha256") != plan.plan_sha256:
            raise ValueError("staged plan hash mismatch")
        if (
            sidecar.get("provider_condition_last_modified_date")
            != plan.provider_condition_last_modified_date
        ):
            raise ValueError("staged provider revision mismatch")
        if sidecar.get("schema") != schema_plan.schema:
            raise ValueError("staged schema mismatch")
        if sidecar.get("start_utc") != schema_plan.start_utc:
            raise ValueError("staged start window mismatch")
        if sidecar.get("end_utc") != schema_plan.end_utc:
            raise ValueError("staged end window mismatch")
        if int(sidecar.get("file_bytes", -1)) != verified_path.stat().st_size:
            raise ValueError("staged component size mismatch")
        file_sha256 = _sha256(verified_path)
        if str(sidecar.get("file_sha256") or "") != file_sha256:
            raise ValueError("staged component hash mismatch")
        validation = dict(validate_component(verified_path, schema_plan, plan.parents))
        stored_validation = sidecar.get("validation")
        if not isinstance(stored_validation, Mapping) or validation != dict(stored_validation):
            raise ValueError("staged component validation mismatch")
    except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
        raise ValueError(
            f"invalid staged {schema_plan.schema} component requires inspection: {stage_dir}"
        ) from exc
    component = {
        "schema": schema_plan.schema,
        "requested_start_utc": schema_plan.start_utc,
        "requested_end_utc": schema_plan.end_utc,
        "file": "",
        "file_bytes": verified_path.stat().st_size,
        "file_sha256": file_sha256,
        "estimated_billable_bytes": schema_plan.estimated_billable_bytes,
        "estimated_cost_usd": schema_plan.estimated_cost_usd,
        "validation": validation,
    }
    return verified_path, component


def _download_to_stage(
    *,
    client: object,
    stage_dir: Path,
    plan: HistoricalDayPlan,
    schema_plan: HistoricalSchemaPlan,
    validate_component: ComponentValidator,
) -> tuple[Path, dict[str, object]]:
    filename = f"opra-pillar.{schema_plan.schema}.verified.dbn.zst"
    verified_path = stage_dir / filename
    if verified_path.exists():
        raise ValueError(f"refusing to overwrite staged component: {verified_path}")
    temporary = stage_dir / (
        f".{schema_plan.schema}.{os.getpid()}.{uuid.uuid4().hex}.partial.dbn.zst"
    )
    try:
        _assert_provider_plan_current(client, plan)
        client.timeseries.get_range(
            **schema_plan.request_kwargs(plan.parents), path=str(temporary)
        )
        _assert_provider_plan_current(client, plan)
        if not temporary.is_file() or temporary.stat().st_size <= 0:
            raise ValueError(f"provider returned an empty {schema_plan.schema} component")
        validation = dict(validate_component(temporary, schema_plan, plan.parents))
        with temporary.open("r+b") as stream:
            os.fsync(stream.fileno())
        os.replace(temporary, verified_path)
    except Exception as exc:
        if temporary.is_file():
            failed_path = stage_dir / (
                f"opra-pillar.{schema_plan.schema}.{uuid.uuid4().hex}.failed.dbn.zst"
            )
            os.replace(temporary, failed_path)
            _atomic_json(
                failed_path.with_name(f"{failed_path.name}.json"),
                {
                    "request_sha256": plan.request_sha256,
                    "plan_sha256": plan.plan_sha256,
                    "provider_condition_last_modified_date": (
                        plan.provider_condition_last_modified_date
                    ),
                    "schema": schema_plan.schema,
                    "state": "failed_diagnostic_only",
                    "error_type": type(exc).__name__,
                    "file_bytes": failed_path.stat().st_size,
                    "file_sha256": _sha256(failed_path),
                },
            )
        raise
    file_sha256 = _sha256(verified_path)
    component = {
        "schema": schema_plan.schema,
        "requested_start_utc": schema_plan.start_utc,
        "requested_end_utc": schema_plan.end_utc,
        "file": "",
        "file_bytes": verified_path.stat().st_size,
        "file_sha256": file_sha256,
        "estimated_billable_bytes": schema_plan.estimated_billable_bytes,
        "estimated_cost_usd": schema_plan.estimated_cost_usd,
        "validation": validation,
    }
    _atomic_json(
        stage_dir / f"{filename}.json",
        {
            "request_sha256": plan.request_sha256,
            "plan_sha256": plan.plan_sha256,
            "provider_condition_last_modified_date": (
                plan.provider_condition_last_modified_date
            ),
            "schema": schema_plan.schema,
            "start_utc": schema_plan.start_utc,
            "end_utc": schema_plan.end_utc,
            "file_bytes": component["file_bytes"],
            "file_sha256": file_sha256,
            "validation": validation,
            "state": "verified",
        },
    )
    return verified_path, component


def download_historical_day(
    client: object,
    *,
    plan: HistoricalDayPlan,
    output_root: str | Path,
    max_cost_usd: float,
    max_billable_bytes: int,
    approved_plan_sha256: str | None = None,
    validate_component: ComponentValidator = validate_dbn_component,
    observed_at: datetime | None = None,
) -> dict[str, object]:
    if plan.already_eligible:
        raise ValueError(f"{plan.trading_date} already has an eligible live capture")
    if plan.blocked_reasons:
        raise ValueError("historical plan is blocked: " + "; ".join(plan.blocked_reasons))
    max_estimated_cost = _finite_nonnegative(max_cost_usd, "estimated cost ceiling")
    max_estimated_bytes = _positive_bytes(
        max_billable_bytes, "estimated billable-byte ceiling"
    )
    planned_cost = _finite_nonnegative(plan.estimated_cost_usd, "planned estimated cost")
    if planned_cost > max_estimated_cost + 1e-9:
        raise ValueError(
            f"estimated cost ${planned_cost:.6f} exceeds client preflight ceiling "
            f"${max_estimated_cost:.6f}"
        )
    if plan.estimated_billable_bytes > max_estimated_bytes:
        raise ValueError(
            f"estimated billable bytes {plan.estimated_billable_bytes} exceed client "
            f"preflight ceiling {max_estimated_bytes}"
        )
    if {item.schema for item in plan.schemas} != set(SUPPORTED_SCHEMAS):
        raise ValueError("a complete historical bundle requires TCBBO, statistics, and definition")

    root = Path(output_root).resolve()
    day_dir = (root / plan.trading_date).resolve()
    try:
        day_dir.relative_to(root)
    except ValueError as exc:
        raise ValueError("historical day directory escapes the configured output root") from exc

    if plan.existing_bundle:
        _assert_provider_plan_current(client, plan)
        payload = verify_bundle_manifest(
            plan.existing_bundle, validate_component=validate_component
        )
        _assert_manifest_matches_plan(payload, plan)
        return payload
    if approved_plan_sha256 != plan.plan_sha256:
        raise ValueError(
            "execution requires the exact plan_sha256 emitted by the reviewed dry run"
        )
    _assert_provider_plan_current(client, plan)

    root.mkdir(parents=True, exist_ok=True)
    day_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = day_dir / "manifest.json"
    with HistoricalDayLock(day_dir / ".backfill.lock"):
        existing_manifest = next(
            (
                item
                for item in (day_dir / "manifest.v2.json", manifest_path)
                if item.is_file()
            ),
            None,
        )
        if existing_manifest is not None:
            payload = verify_bundle_manifest(
                existing_manifest, validate_component=validate_component
            )
            _assert_manifest_matches_plan(payload, plan)
            return payload

        free_bytes = shutil.disk_usage(root).free
        if free_bytes < plan.estimated_billable_bytes + 1024**3:
            raise ValueError(
                "insufficient free disk for the estimated historical request and 1 GiB reserve"
            )

        stage_dir = day_dir / ".staging" / plan.plan_sha256
        stage_dir.mkdir(parents=True, exist_ok=True)
        components: list[dict[str, object]] = []
        for schema_plan in sorted(plan.schemas, key=lambda item: item.schema):
            final_name = f"opra-pillar.{plan.trading_date}.{schema_plan.schema}.dbn.zst"
            final_path = day_dir / final_name
            component = _component_from_existing_file(
                path=final_path,
                sidecar_path=stage_dir
                / f"opra-pillar.{schema_plan.schema}.verified.dbn.zst.json",
                plan=plan,
                schema_plan=schema_plan,
                validate_component=validate_component,
            )
            if component is None:
                staged = _load_staged_component(
                    stage_dir=stage_dir,
                    plan=plan,
                    schema_plan=schema_plan,
                    validate_component=validate_component,
                )
                if staged is None:
                    staged = _download_to_stage(
                        client=client,
                        stage_dir=stage_dir,
                        plan=plan,
                        schema_plan=schema_plan,
                        validate_component=validate_component,
                    )
                staged_path, component = staged
                if final_path.exists():
                    raise ValueError(f"refusing to overwrite existing component: {final_path}")
                os.replace(staged_path, final_path)
            component["file"] = final_name
            components.append(component)

        _assert_provider_plan_current(client, plan)

        mapping_hashes = {
            str(dict(item["validation"]).get("dbn_mapping_sha256") or "")
            for item in components
        }
        if len(mapping_hashes) != 1 or "" in mapping_hashes:
            raise ValueError("historical components do not share one DBN mapping identity")

        identity = _bundle_identity(
            trading_date=plan.trading_date,
            request_sha256=plan.request_sha256,
            components=components,
        )
        timestamp = observed_at or datetime.now(UTC)
        if timestamp.tzinfo is None:
            raise ValueError("observed_at must be timezone-aware")
        component_estimates = [
            {
                "schema": item.schema,
                "estimated_cost_usd": item.estimated_cost_usd,
                "estimated_billable_bytes": item.estimated_billable_bytes,
            }
            for item in sorted(plan.schemas, key=lambda value: value.schema)
        ]
        manifest: dict[str, object] = {
            **identity,
            "source_kind": "databento_historical",
            "status": "complete",
            "request": plan.request,
            "provider_condition": plan.provider_condition,
            "provider_condition_last_modified_date": (
                plan.provider_condition_last_modified_date
            ),
            "planning": {
                "plan_sha256": plan.plan_sha256,
                "component_estimates": component_estimates,
                "estimated_cost_usd": plan.estimated_cost_usd,
                "estimated_billable_bytes": plan.estimated_billable_bytes,
                "estimate_currency": "USD",
                "estimate_enforcement_scope": "client_preflight_only",
                "approved_estimate_ceilings": {
                    "cost_usd": max_estimated_cost,
                    "billable_bytes": max_estimated_bytes,
                },
            },
            "acquisition": {
                "completed_at_utc": _iso_utc(timestamp),
                "sdk_versions": _sdk_versions(),
            },
            "bundle_sha256": _canonical_hash(identity),
        }
        manifest["provenance_sha256"] = _canonical_hash(
            _provenance_identity(manifest)
        )
        manifest["attestation_sha256"] = _canonical_hash(
            {
                "bundle_sha256": manifest["bundle_sha256"],
                "provenance_sha256": manifest["provenance_sha256"],
            }
        )
        _atomic_json(manifest_path, manifest)
        return verify_bundle_manifest(
            manifest_path, validate_component=validate_component
        )


def write_v2_attestation_for_legacy_bundle(
    manifest_path: str | Path,
    *,
    provider_condition_at_attestation: str,
    provider_condition_last_modified_date_at_attestation: str,
    validate_component: ComponentValidator = validate_dbn_component,
    observed_at: datetime | None = None,
) -> dict[str, object]:
    """Write a v2 sidecar while preserving the original v1 acquisition manifest."""

    legacy_path = Path(manifest_path).resolve()
    payload = _read_manifest(legacy_path)
    if payload.get("version") != LEGACY_BUNDLE_VERSION:
        raise ValueError("v2 attestation requires an original v1 manifest")
    verified = _verify_legacy_v1_manifest(legacy_path, payload, validate_component)
    if provider_condition_at_attestation != "available":
        raise ValueError("legacy attestation requires provider condition available")
    try:
        date.fromisoformat(provider_condition_last_modified_date_at_attestation)
    except ValueError as exc:
        raise ValueError("legacy attestation provider revision date is invalid") from exc
    request = _request_from_legacy(verified)
    parents = tuple(str(item) for item in request["parents"])
    components: list[dict[str, object]] = []
    for item in verified["components"]:
        if not isinstance(item, Mapping):
            raise ValueError("legacy component is malformed")
        schema_plan = HistoricalSchemaPlan(
            schema=str(item["schema"]),
            start_utc=str(item["start_utc"]),
            end_utc=str(item["end_utc"]),
            estimated_cost_usd=float(item.get("estimated_cost_usd", 0.0)),
            estimated_billable_bytes=int(item.get("estimated_billable_bytes", 0)),
        )
        source = _resolve_component(legacy_path.parent.resolve(), item)
        components.append(
            {
                "schema": schema_plan.schema,
                "requested_start_utc": schema_plan.start_utc,
                "requested_end_utc": schema_plan.end_utc,
                "file": source.name,
                "file_bytes": source.stat().st_size,
                "file_sha256": _sha256(source),
                "estimated_billable_bytes": schema_plan.estimated_billable_bytes,
                "estimated_cost_usd": schema_plan.estimated_cost_usd,
                "validation": dict(validate_component(source, schema_plan, parents)),
            }
        )
    request_sha256 = _canonical_hash(request)
    legacy_evidence = {
        "manifest_file": legacy_path.name,
        "manifest_sha256": _sha256(legacy_path),
        "bundle_sha256": verified["bundle_sha256"],
    }
    identity = _bundle_identity(
        trading_date=str(verified["trading_date"]),
        request_sha256=request_sha256,
        components=components,
        legacy_evidence=legacy_evidence,
    )
    timestamp = observed_at or datetime.now(UTC)
    if timestamp.tzinfo is None:
        raise ValueError("observed_at must be timezone-aware")
    attestation: dict[str, object] = {
        **identity,
        "source_kind": "databento_historical",
        "status": "complete",
        "request": request,
        "provider_condition": verified.get("provider_condition"),
        "provider_condition_last_modified_date": None,
        "provider_condition_at_attestation": provider_condition_at_attestation,
        "provider_condition_last_modified_date_at_attestation": (
            provider_condition_last_modified_date_at_attestation
        ),
        "legacy_evidence": legacy_evidence,
        "planning": {
            "estimated_cost_usd": float(verified.get("estimated_cost_usd", 0.0)),
            "estimated_billable_bytes": int(verified.get("estimated_billable_bytes", 0)),
            "estimate_currency": "USD",
            "estimate_enforcement_scope": "client_preflight_only",
            "legacy_manifest_version": LEGACY_BUNDLE_VERSION,
        },
        "acquisition": {
            "original_completed_at_utc": verified.get("completed_at_utc"),
            "attested_at_utc": _iso_utc(timestamp),
            "sdk_versions": _sdk_versions(),
        },
        "bundle_sha256": _canonical_hash(identity),
    }
    attestation["provenance_sha256"] = _canonical_hash(
        _provenance_identity(attestation)
    )
    attestation["attestation_sha256"] = _canonical_hash(
        {
            "bundle_sha256": attestation["bundle_sha256"],
            "provenance_sha256": attestation["provenance_sha256"],
        }
    )
    sidecar = legacy_path.with_name("manifest.v2.json")
    if sidecar.exists():
        existing = verify_bundle_manifest(sidecar, validate_component=validate_component)
        if existing["bundle_sha256"] != attestation["bundle_sha256"]:
            raise ValueError("existing v2 attestation conflicts with legacy bundle")
        return existing
    _atomic_json(sidecar, attestation)
    return verify_bundle_manifest(sidecar, validate_component=validate_component)


def inclusive_dates(start: date, end: date) -> tuple[date, ...]:
    if end < start:
        raise ValueError("end date must be on or after start date")
    values: list[date] = []
    cursor = start
    while cursor <= end:
        if cursor.weekday() < 5:
            values.append(cursor)
        cursor += timedelta(days=1)
    return tuple(values)
