from __future__ import annotations

import hashlib
import json
import math
import os
import uuid
from dataclasses import asdict, dataclass, replace
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Callable, Iterable, Mapping, Sequence

from .compute_guard import require_compute_window
from .historical_backfill import (
    DEFAULT_OUTPUT_DIRECTORY,
    HistoricalDayLock,
    HistoricalDayPlan,
    discover_existing_bundle,
    download_historical_day,
    plan_historical_day,
    verify_bundle_manifest,
)
from .historical_import import import_historical_bundle
from .readiness import audit_training_readiness


UTC = timezone.utc
BULK_PLAN_VERSION = "marketpin-closing-tape-historical-bulk-plan-v1"
BULK_JOURNAL_VERSION = "marketpin-closing-tape-historical-bulk-journal-v1"
DEFAULT_JOURNAL_DIRECTORY = Path("data") / "backtest_pipeline" / "historical_bulk"
MAX_JOURNAL_BYTES = 32 * 1024 * 1024


def _canonical_hash(payload: Mapping[str, object]) -> str:
    encoded = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
        default=str,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _sha256_file(path: Path, chunk_size: int = 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(chunk_size), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _iso_utc(value: datetime) -> str:
    if value.tzinfo is None:
        raise ValueError("journal timestamps must be timezone-aware")
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _finite_nonnegative(value: object, field: str) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError(f"{field} must be a finite nonnegative number") from exc
    if not math.isfinite(parsed) or parsed < 0:
        raise ValueError(f"{field} must be a finite nonnegative number")
    return parsed


def _positive_integer(value: object, field: str) -> int:
    if isinstance(value, bool):
        raise ValueError(f"{field} must be a positive integer")
    try:
        parsed = int(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError(f"{field} must be a positive integer") from exc
    if parsed <= 0:
        raise ValueError(f"{field} must be a positive integer")
    return parsed


def _unique_dates(values: Iterable[date]) -> tuple[date, ...]:
    normalized: list[date] = []
    seen: set[date] = set()
    for value in values:
        if not isinstance(value, date):
            raise TypeError("historical bulk dates must be datetime.date values")
        if value not in seen:
            normalized.append(value)
            seen.add(value)
    if not normalized:
        raise ValueError("historical bulk requires at least one trading date")
    return tuple(normalized)


def _readiness_payload(report: object) -> dict[str, object]:
    if hasattr(report, "to_dict"):
        payload = report.to_dict()
    elif isinstance(report, Mapping):
        payload = dict(report)
    else:
        payload = asdict(report)  # type: ignore[arg-type]
    if not isinstance(payload, dict):
        raise TypeError("readiness report must serialize to an object")
    return payload


def _session_values(report: object) -> Sequence[object]:
    values = getattr(report, "sessions_detail", ())
    return tuple(values or ())


def _session_value(item: object, field: str, default: object = None) -> object:
    if isinstance(item, Mapping):
        return item.get(field, default)
    return getattr(item, field, default)


def _eligible_and_ambiguous_dates(report: object) -> tuple[set[str], set[str]]:
    eligible: set[str] = set()
    ambiguous: set[str] = set()
    for item in _session_values(report):
        trading_date = str(_session_value(item, "trading_date", "") or "")
        reasons = tuple(_session_value(item, "reasons", ()) or ())
        if bool(_session_value(item, "eligible", False)) and trading_date:
            eligible.add(trading_date)
        if trading_date and any(
            "multiple eligible captures share this trading date" in str(reason)
            for reason in reasons
        ):
            ambiguous.add(trading_date)
    return eligible, ambiguous


@dataclass(frozen=True)
class HistoricalBulkPlan:
    project_root: Path
    output_root: Path
    requested_dates: tuple[date, ...]
    day_plans: tuple[HistoricalDayPlan, ...]
    batch_plan_sha256: str
    estimated_cost_usd: float
    estimated_billable_bytes: int
    readiness_before: Mapping[str, object]

    @property
    def blocked_dates(self) -> tuple[str, ...]:
        return tuple(
            plan.trading_date for plan in self.day_plans if plan.blocked_reasons
        )

    @property
    def executable_dates(self) -> tuple[str, ...]:
        return tuple(plan.trading_date for plan in self.day_plans if plan.executable)

    @property
    def ready_for_execute(self) -> bool:
        return not self.blocked_dates

    def identity(self) -> dict[str, object]:
        # Runtime state such as an already-downloaded manifest is deliberately
        # excluded. A resumed batch keeps the reviewed identity, while a changed
        # provider revision, request window, or estimate changes each day hash.
        return {
            "version": BULK_PLAN_VERSION,
            "project_root": str(self.project_root),
            "output_root": str(self.output_root),
            "days": [
                {
                    "trading_date": plan.trading_date,
                    "plan_sha256": plan.plan_sha256,
                }
                for plan in self.day_plans
            ],
        }

    def to_dict(self) -> dict[str, object]:
        return {
            "version": BULK_PLAN_VERSION,
            "project_root": str(self.project_root),
            "output_root": str(self.output_root),
            "requested_dates": [value.isoformat() for value in self.requested_dates],
            "batch_plan_sha256": self.batch_plan_sha256,
            "estimated_cost_usd": self.estimated_cost_usd,
            "estimated_billable_bytes": self.estimated_billable_bytes,
            "estimate_enforcement_scope": "client_preflight_only",
            "billing_notice": (
                "Aggregate ceilings are checked locally before any get_range call; "
                "they are not provider-enforced spending limits."
            ),
            "ready_for_execute": self.ready_for_execute,
            "blocked_dates": list(self.blocked_dates),
            "executable_dates": list(self.executable_dates),
            "plans": [plan.to_dict() for plan in self.day_plans],
            "readiness_before": dict(self.readiness_before),
        }


def plan_historical_bulk(
    client: object,
    *,
    project_root: str | Path,
    trading_dates: Iterable[date],
    output_root: str | Path | None = None,
    readiness_fn: Callable[[str | Path], object] = audit_training_readiness,
    plan_day_fn: Callable[..., HistoricalDayPlan] = plan_historical_day,
    discover_bundle_fn: Callable[..., str | None] = discover_existing_bundle,
) -> HistoricalBulkPlan:
    """Build one metadata/cost-only plan without writing pipeline state.

    The injected provider client is used only by ``plan_day_fn`` for condition,
    range, cost, and billable-size metadata. This function never calls the
    provider time-series ``get_range`` method.
    """

    root = Path(project_root).resolve()
    dates = _unique_dates(trading_dates)
    destination = (
        Path(output_root).resolve()
        if output_root is not None
        else (root / DEFAULT_OUTPUT_DIRECTORY).resolve()
    )
    if destination.exists() and not destination.is_dir():
        raise ValueError("historical bulk output root is not a directory")

    readiness = readiness_fn(root)
    eligible_dates, ambiguous_dates = _eligible_and_ambiguous_dates(readiness)
    dataset_range = client.metadata.get_dataset_range(dataset="OPRA.PILLAR")
    plans: list[HistoricalDayPlan] = []
    for trading_day in dates:
        day_text = trading_day.isoformat()
        plan = plan_day_fn(
            client,
            trading_day=trading_day,
            already_eligible=day_text in eligible_dates,
            dataset_range=dataset_range,
        )
        if day_text in ambiguous_dates:
            plan = replace(
                plan,
                already_eligible=False,
                blocked_reasons=tuple(
                    dict.fromkeys(
                        (
                            *plan.blocked_reasons,
                            "multiple retained captures share this trading date; "
                            "historical acquisition would increase duplicate-date ambiguity",
                        )
                    )
                ),
            )
        try:
            existing = discover_bundle_fn(
                destination,
                trading_day,
                expected_plan=plan,
            )
        except (FileNotFoundError, KeyError, OSError, TypeError, ValueError, json.JSONDecodeError) as exc:
            plan = replace(
                plan,
                blocked_reasons=tuple(
                    dict.fromkeys(
                        (*plan.blocked_reasons, f"existing bundle conflict: {exc}")
                    )
                ),
            )
        else:
            if existing:
                plan = replace(plan, existing_bundle=str(Path(existing).resolve()))
        plans.append(plan)

    provisional_identity = {
        "version": BULK_PLAN_VERSION,
        "project_root": str(root),
        "output_root": str(destination),
        "days": [
            {
                "trading_date": plan.trading_date,
                "plan_sha256": plan.plan_sha256,
            }
            for plan in plans
        ],
    }
    executable = [plan for plan in plans if plan.executable]
    return HistoricalBulkPlan(
        project_root=root,
        output_root=destination,
        requested_dates=dates,
        day_plans=tuple(plans),
        batch_plan_sha256=_canonical_hash(provisional_identity),
        estimated_cost_usd=float(
            sum(plan.estimated_cost_usd for plan in executable)
        ),
        estimated_billable_bytes=int(
            sum(plan.estimated_billable_bytes for plan in executable)
        ),
        readiness_before=_readiness_payload(readiness),
    )


def _resolve_journal_root(project_root: Path, raw: str | Path | None) -> Path:
    candidate = (
        Path(raw).resolve()
        if raw is not None
        else (project_root / DEFAULT_JOURNAL_DIRECTORY).resolve()
    )
    try:
        candidate.relative_to(project_root)
    except ValueError as exc:
        raise ValueError("historical bulk journal root must remain inside project_root") from exc
    if candidate.exists() and not candidate.is_dir():
        raise ValueError("historical bulk journal root is not a directory")
    return candidate


def _resolve_manifest_for_day(
    output_root: Path,
    trading_date: str,
    raw_path: str | Path,
) -> Path:
    manifest = Path(raw_path).resolve()
    expected_day_root = (output_root / trading_date).resolve()
    try:
        manifest.relative_to(expected_day_root)
    except ValueError as exc:
        raise ValueError(
            "historical bundle manifest escapes its configured trading-day directory"
        ) from exc
    if not manifest.is_file():
        raise FileNotFoundError(manifest)
    return manifest


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


class _BulkJournal:
    def __init__(
        self,
        path: Path,
        *,
        plan: HistoricalBulkPlan,
        perform_import: bool,
        now_fn: Callable[[], datetime],
    ):
        self.path = path
        self.plan = plan
        self.perform_import = perform_import
        self.now_fn = now_fn
        request = {
            "batch_plan_sha256": plan.batch_plan_sha256,
            "perform_import": perform_import,
        }
        self.request_sha256 = _canonical_hash(request)
        self.payload = self._load_or_initialize(request)

    def _load_or_initialize(self, request: Mapping[str, object]) -> dict[str, object]:
        if not self.path.exists():
            return {
                "version": BULK_JOURNAL_VERSION,
                "batch_plan_sha256": self.plan.batch_plan_sha256,
                "request": dict(request),
                "request_sha256": self.request_sha256,
                "created_at_utc": _iso_utc(self.now_fn()),
                "events": [],
            }
        size = self.path.stat().st_size
        if size <= 0 or size > MAX_JOURNAL_BYTES:
            raise ValueError("historical bulk journal has an invalid size")
        payload = json.loads(self.path.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            raise ValueError("historical bulk journal root must be an object")
        if payload.get("version") != BULK_JOURNAL_VERSION:
            raise ValueError("historical bulk journal version is unsupported")
        if payload.get("batch_plan_sha256") != self.plan.batch_plan_sha256:
            raise ValueError("historical bulk journal belongs to a different batch plan")
        if payload.get("request_sha256") != self.request_sha256:
            raise ValueError("historical bulk journal execution request changed")
        if _canonical_hash(dict(payload.get("request") or {})) != self.request_sha256:
            raise ValueError("historical bulk journal request identity is corrupt")
        if not isinstance(payload.get("events"), list):
            raise ValueError("historical bulk journal events are malformed")
        for event in payload["events"]:
            if not isinstance(event, Mapping):
                raise ValueError("historical bulk journal event is malformed")
            identity = {
                "trading_date": event.get("trading_date"),
                "stage": event.get("stage"),
                "status": event.get("status"),
                "payload": event.get("payload"),
            }
            if event.get("event_key") != _canonical_hash(identity):
                raise ValueError("historical bulk journal event identity is corrupt")
        return payload

    def record(
        self,
        *,
        trading_date: str | None,
        stage: str,
        status: str,
        payload: Mapping[str, object],
    ) -> None:
        identity = {
            "trading_date": trading_date,
            "stage": stage,
            "status": status,
            "payload": dict(payload),
        }
        event_key = _canonical_hash(identity)
        events = self.payload["events"]
        assert isinstance(events, list)
        if any(
            isinstance(event, Mapping) and event.get("event_key") == event_key
            for event in events
        ):
            return
        events.append(
            {
                "event_key": event_key,
                "observed_at_utc": _iso_utc(self.now_fn()),
                **identity,
            }
        )
        _atomic_json(self.path, self.payload)


def _result_dict(value: object) -> dict[str, object]:
    if hasattr(value, "to_dict"):
        result = value.to_dict()
    elif isinstance(value, Mapping):
        result = dict(value)
    else:
        result = asdict(value)  # type: ignore[arg-type]
    if not isinstance(result, dict):
        raise TypeError("pipeline stage result must serialize to an object")
    return result


def _label_work_by_date(readiness_payload: Mapping[str, object]) -> dict[str, object]:
    work: dict[str, object] = {}
    raw_queue = readiness_payload.get("label_work_queue") or ()
    if not isinstance(raw_queue, (list, tuple)):
        return work
    for item in raw_queue:
        if isinstance(item, Mapping):
            trading_date = str(item.get("trading_date") or "")
            if trading_date:
                work[trading_date] = dict(item)
    return work


def execute_historical_bulk(
    client: object,
    *,
    plan: HistoricalBulkPlan,
    approved_batch_plan_sha256: str,
    max_estimated_cost_usd: float,
    max_estimated_billable_bytes: int,
    journal_root: str | Path | None = None,
    perform_import: bool = True,
    download_day_fn: Callable[..., Mapping[str, object]] = download_historical_day,
    discover_bundle_fn: Callable[..., str | None] = discover_existing_bundle,
    verify_bundle_fn: Callable[..., Mapping[str, object]] = verify_bundle_manifest,
    import_bundle_fn: Callable[..., object] = import_historical_bundle,
    readiness_fn: Callable[[str | Path], object] = audit_training_readiness,
    compute_guard_fn: Callable[..., object] = require_compute_window,
    now_fn: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> dict[str, object]:
    """Acquire, verify, import, and readiness-audit a reviewed batch.

    Every aggregate authorization check occurs before the first download. Each
    completed stage is checkpointed atomically, but resumability never trusts the
    journal alone: existing manifests and completed imports are reverified by the
    underlying content-addressed APIs on every run.
    """

    if approved_batch_plan_sha256 != plan.batch_plan_sha256:
        raise ValueError(
            "execution requires the exact batch_plan_sha256 emitted by the reviewed dry run"
        )
    if not plan.ready_for_execute:
        raise ValueError(
            "historical bulk plan is blocked for: " + ", ".join(plan.blocked_dates)
        )
    cost_ceiling = _finite_nonnegative(
        max_estimated_cost_usd, "aggregate estimated cost ceiling"
    )
    byte_ceiling = _positive_integer(
        max_estimated_billable_bytes, "aggregate estimated billable-byte ceiling"
    )
    if plan.estimated_cost_usd > cost_ceiling + 1e-9:
        raise ValueError(
            f"aggregate estimated cost ${plan.estimated_cost_usd:.6f} exceeds "
            f"client preflight ceiling ${cost_ceiling:.6f}"
        )
    if plan.estimated_billable_bytes > byte_ceiling:
        raise ValueError(
            f"aggregate estimated billable bytes {plan.estimated_billable_bytes} "
            f"exceed client preflight ceiling {byte_ceiling}"
        )

    # Bulk automation has no live-capture override. Recheck again before each
    # import because a recorder can start after acquisition begins.
    compute_guard_fn(
        plan.project_root,
        operation="historical bulk acquisition/import",
        override_supported=False,
    )
    state_root = _resolve_journal_root(plan.project_root, journal_root)
    state_root.mkdir(parents=True, exist_ok=True)
    journal_path = state_root / f"{plan.batch_plan_sha256}.journal.json"
    lock_path = (plan.project_root / DEFAULT_JOURNAL_DIRECTORY / ".historical_bulk.lock").resolve()
    date_results: dict[str, dict[str, object]] = {
        item.trading_date: {
            "trading_date": item.trading_date,
            "plan_sha256": item.plan_sha256,
            "acquisition": {"status": "pending"},
            "import": {"status": "pending" if perform_import else "not_requested"},
        }
        for item in plan.day_plans
    }
    manifests: dict[str, Path] = {}
    failures: list[dict[str, str]] = []

    with HistoricalDayLock(lock_path):
        journal = _BulkJournal(
            journal_path,
            plan=plan,
            perform_import=perform_import,
            now_fn=now_fn,
        )
        journal.record(
            trading_date=None,
            stage="authorization",
            status="approved",
            payload={
                "batch_plan_sha256": plan.batch_plan_sha256,
                "max_estimated_cost_usd": cost_ceiling,
                "max_estimated_billable_bytes": byte_ceiling,
            },
        )

        for day_plan in plan.day_plans:
            day = day_plan.trading_date
            result = date_results[day]
            if day_plan.already_eligible:
                acquisition = {
                    "status": "skipped_eligible_capture",
                    "reason": "one retained capture already passes immutable readiness",
                }
                result["acquisition"] = acquisition
                result["import"] = {"status": "skipped_eligible_capture"}
                journal.record(
                    trading_date=day,
                    stage="acquisition",
                    status="skipped_eligible_capture",
                    payload=acquisition,
                )
                continue
            try:
                if day_plan.existing_bundle:
                    manifest_path_raw = discover_bundle_fn(
                        plan.output_root,
                        date.fromisoformat(day),
                        expected_plan=day_plan,
                    )
                    if not manifest_path_raw:
                        raise FileNotFoundError(
                            "planned existing bundle is no longer discoverable"
                        )
                    manifest_path = _resolve_manifest_for_day(
                        plan.output_root, day, manifest_path_raw
                    )
                    manifest = verify_bundle_fn(manifest_path)
                    acquisition_status = "reused_verified_bundle"
                else:
                    manifest = download_day_fn(
                        client,
                        plan=day_plan,
                        output_root=plan.output_root,
                        max_cost_usd=cost_ceiling,
                        max_billable_bytes=byte_ceiling,
                        approved_plan_sha256=day_plan.plan_sha256,
                    )
                    manifest_path_raw = discover_bundle_fn(
                        plan.output_root,
                        date.fromisoformat(day),
                        expected_plan=day_plan,
                    )
                    if not manifest_path_raw:
                        raise FileNotFoundError(
                            "download completed without a discoverable verified manifest"
                        )
                    manifest_path = _resolve_manifest_for_day(
                        plan.output_root, day, manifest_path_raw
                    )
                    # Rehash the published evidence rather than trusting the
                    # downloader return value or journal projection.
                    manifest = verify_bundle_fn(manifest_path)
                    acquisition_status = "downloaded_verified_bundle"
                bundle_hash = str(manifest.get("bundle_sha256") or "")
                if len(bundle_hash) != 64:
                    raise ValueError("verified historical manifest has no bundle SHA-256")
                acquisition = {
                    "status": acquisition_status,
                    "manifest": str(manifest_path),
                    "manifest_sha256": _sha256_file(manifest_path),
                    "bundle_sha256": bundle_hash,
                }
                result["acquisition"] = acquisition
                manifests[day] = manifest_path
                journal.record(
                    trading_date=day,
                    stage="acquisition",
                    status=acquisition_status,
                    payload=acquisition,
                )
            except Exception as exc:
                message = f"{type(exc).__name__}: {str(exc)[:1000]}"
                failure = {"trading_date": day, "stage": "acquisition", "error": message}
                failures.append(failure)
                result["acquisition"] = {"status": "failed", "error": message}
                result["import"] = {"status": "blocked_by_acquisition"}
                journal.record(
                    trading_date=day,
                    stage="acquisition",
                    status="failed",
                    payload={"error": message},
                )

        if perform_import:
            for day_plan in plan.day_plans:
                day = day_plan.trading_date
                manifest_path = manifests.get(day)
                if manifest_path is None:
                    continue
                try:
                    compute_guard_fn(
                        plan.project_root,
                        operation=f"historical bulk import {day}",
                        override_supported=False,
                    )
                    imported = import_bundle_fn(plan.project_root, manifest_path)
                    import_payload = _result_dict(imported)
                    import_status = str(import_payload.get("status") or "complete")
                    if import_status not in {"complete", "already_complete"}:
                        raise ValueError(
                            f"historical importer returned nonterminal status {import_status!r}"
                        )
                    date_results[day]["import"] = import_payload
                    journal.record(
                        trading_date=day,
                        stage="import",
                        status=import_status,
                        payload=import_payload,
                    )
                except Exception as exc:
                    message = f"{type(exc).__name__}: {str(exc)[:1000]}"
                    failure = {"trading_date": day, "stage": "import", "error": message}
                    failures.append(failure)
                    date_results[day]["import"] = {"status": "failed", "error": message}
                    journal.record(
                        trading_date=day,
                        stage="import",
                        status="failed",
                        payload={"error": message},
                    )

        readiness_after_object = readiness_fn(plan.project_root)
        readiness_after = _readiness_payload(readiness_after_object)
        journal.record(
            trading_date=None,
            stage="readiness",
            status="audited",
            payload=readiness_after,
        )

    eligible_after, ambiguous_after = _eligible_and_ambiguous_dates(
        readiness_after_object
    )
    work_by_date = _label_work_by_date(readiness_after)
    sessions_by_date: dict[str, list[dict[str, object]]] = {}
    for item in _session_values(readiness_after_object):
        day = str(_session_value(item, "trading_date", "") or "")
        if not day:
            continue
        if isinstance(item, Mapping):
            serialized = dict(item)
        else:
            serialized = asdict(item)  # type: ignore[arg-type]
        sessions_by_date.setdefault(day, []).append(serialized)
    for day, result in date_results.items():
        result["sessions"] = sessions_by_date.get(day, [])
        if day in ambiguous_after:
            result["readiness"] = {
                "status": "excluded_duplicate_date",
                "eligible": False,
            }
            result["labels"] = {"status": "blocked_by_capture_ambiguity"}
        elif day not in eligible_after:
            reasons = [
                reason
                for session in sessions_by_date.get(day, [])
                for reason in session.get("reasons", [])
            ]
            result["readiness"] = {
                "status": "capture_not_eligible",
                "eligible": False,
                "reasons": list(dict.fromkeys(str(value) for value in reasons)),
            }
            result["labels"] = {"status": "blocked_by_capture_readiness"}
        elif day in work_by_date:
            result["readiness"] = {"status": "capture_eligible", "eligible": True}
            result["labels"] = {
                "status": "full_five_family_bundle_pending",
                "work_item": work_by_date[day],
            }
        else:
            result["readiness"] = {"status": "capture_eligible", "eligible": True}
            result["labels"] = {"status": "full_five_family_bundle_verified"}

    return {
        "version": BULK_JOURNAL_VERSION,
        "mode": "execute",
        "status": "partial" if failures else "complete",
        "batch_plan_sha256": plan.batch_plan_sha256,
        "journal_path": str(journal_path),
        "perform_import": perform_import,
        "dates": [date_results[plan.trading_date] for plan in plan.day_plans],
        "failures": failures,
        "readiness_after": readiness_after,
        "surface": {
            "status": "not_requested",
            "reason": (
                "bulk acquisition stops at immutable import, verified-close work queue, "
                "and readiness; content-addressed surface/evaluation remains separately gated"
            ),
        },
    }
