"""Durable, fail-closed MarketPin monitor session rollover.

The heartbeat remains responsible for collecting market evidence.  This module
only advances its small monitor journal/state boundary after the reviewed cash
calendar proves that the requested Central-time date is an open session.
"""

from __future__ import annotations

import copy
import hashlib
import json
import os
import tempfile
from contextlib import contextmanager
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Iterator, Mapping
from zoneinfo import ZoneInfo

from app.utils.market_calendar import CALENDAR_PATH, market_calendar_status
from backend.monitor_data_quality_outbox import (
    MonitorDataQualityOutboxError,
    armed_outbox,
    legacy_pre_activation_outbox,
    validate_data_quality_notification_outbox_receipt_replay,
)
from backend.monitor_scan_ledger import (
    MonitorScanLedgerError,
    NOTIFICATION_ACK_EVENT_TYPE,
    _journal_index as _scan_journal_index,
    _validated_ledger_state as _validated_scan_ledger_state,
)


RESULT_SCHEMA = "marketpin-monitor-session-rollover.result.v1"
ROLLOVER_INTENT_SCHEMA = "marketpin-monitor-session-rollover-intent.v1"
EVENT_SCHEMA_VERSION = 2
CT = ZoneInfo("America/Chicago")
# This exact five-flag shape is committed into every v2 rollover event ID.
# Later opening-owner outbox fields are initialized by its first milestone and
# must not retroactively change reconstruction of an already-durable receipt.
_OPENING_RESET_BY_SCHEMA_VERSION = {
    2: {
        "startup_milestone_notified": False,
        "first_eligible_gamma_capture_notified": False,
        "first_complete_5m_orb_notified": False,
        "final_complete_60m_orb_notified": False,
        "temporary_acceptance_checks_complete": False,
    }
}
_OPENING_MILESTONE_EVENT_TYPE = "opening_acceptance_milestone"
_OPENING_ACK_EVENT_TYPE = "opening_acceptance_notification_ack"
_OPENING_MILESTONE_SPECS = (
    ("startup", "startup_milestone_notified", "startup_milestone"),
    (
        "first_eligible_gamma_capture",
        "first_eligible_gamma_capture_notified",
        "first_eligible_gamma_capture",
    ),
    ("complete_5m_orb", "first_complete_5m_orb_notified", "first_complete_5m_orb"),
    (
        "complete_60m_orb",
        "final_complete_60m_orb_notified",
        "final_complete_60m_orb",
    ),
)
_OPENING_MILESTONE_DEPENDENCIES = {
    "startup": (),
    "first_eligible_gamma_capture": (),
    "complete_5m_orb": ("startup",),
    "complete_60m_orb": ("startup", "complete_5m_orb"),
}
_ROLLOVER_CADENCE = {
    "mode": "NORMAL",
    "substantive": True,
    "reason": "verified_open_session_rollover",
}
_ROLLOVER_DIRECTIONAL_INTERPRETATION = (
    "ABSTAIN_NEW_SESSION_BASELINES_REQUIRED"
)
_WRAPPER_SESSION_FIELDS = (
    "mode",
    "mode_reason",
    "elevated_since_ct",
    "elevated_minimum_until_ct",
    "stable_elevated_scan_count",
    "comparison_baselines_eligibility",
    "comparison_baselines",
    "pending_confirmations",
    "pending_zero_gamma_confirmation",
    "pending_pin_contest",
    "pending_directional_confirmation",
    "last_alerted_levels",
    "preflight_alerts",
    "runtime_alerts",
    "last_alert",
    "current_data_quality",
    "last_scan_ct",
    "last_scan_utc",
    "last_eligible_scan_ct",
    "last_scan_result",
    "monitor_scan_ledger",
    "data_quality_notification_outbox",
)


class MonitorSessionRolloverError(RuntimeError):
    """A fail-closed validation or persistence error."""


def _canonical_json_bytes(value: Any, *, pretty: bool = False) -> bytes:
    return (
        json.dumps(
            value,
            sort_keys=not pretty,
            indent=2 if pretty else None,
            separators=None if pretty else (",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        )
        + "\n"
    ).encode("utf-8")


def _value_sha256(value: Any) -> str:
    return hashlib.sha256(_canonical_json_bytes(value)).hexdigest()


def _file_sha256(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _canonical_date(value: object) -> date | None:
    if not isinstance(value, str) or len(value) != 10:
        return None
    try:
        parsed = date.fromisoformat(value)
    except ValueError:
        return None
    return parsed if parsed.isoformat() == value else None


def _normalized_observed_at(value: datetime | None) -> datetime:
    observed = value or datetime.now(timezone.utc)
    if observed.tzinfo is None or observed.utcoffset() is None:
        raise MonitorSessionRolloverError("observed_at_utc_must_be_timezone_aware")
    return observed.astimezone(timezone.utc)


def _utc_iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _load_state(path: Path) -> tuple[dict[str, Any], bytes, str]:
    try:
        raw = path.read_bytes()
    except OSError as exc:
        raise MonitorSessionRolloverError(
            f"state_read_failed:{type(exc).__name__}"
        ) from exc
    try:
        payload = json.loads(raw.decode("utf-8"))
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise MonitorSessionRolloverError(
            f"state_json_invalid:{type(exc).__name__}"
        ) from exc
    if not isinstance(payload, dict):
        raise MonitorSessionRolloverError("state_root_must_be_object")
    if payload.get("schema_version") != 2:
        raise MonitorSessionRolloverError("state_schema_version_must_equal_2")
    if _canonical_date(payload.get("session_date")) is None:
        raise MonitorSessionRolloverError("state_session_date_invalid")
    if not isinstance(payload.get("opening_acceptance"), Mapping):
        raise MonitorSessionRolloverError("state_opening_acceptance_missing")
    if not isinstance(payload.get("policy_evaluator"), Mapping):
        raise MonitorSessionRolloverError("state_policy_evaluator_missing")
    prior_reference = payload.get("prior_session_reference")
    if prior_reference is not None and not isinstance(prior_reference, Mapping):
        raise MonitorSessionRolloverError("state_prior_session_reference_invalid")
    return payload, raw, _file_sha256(raw)


def _read_journal_snapshot(path: Path) -> tuple[list[dict[str, Any]], bytes]:
    if not path.exists():
        return [], b""
    try:
        raw = path.read_bytes()
    except OSError as exc:
        raise MonitorSessionRolloverError(
            f"journal_read_failed:{type(exc).__name__}"
        ) from exc
    if raw and not raw.endswith(b"\n"):
        raise MonitorSessionRolloverError("journal_missing_terminal_newline")
    try:
        text = raw.decode("utf-8")
    except UnicodeError as exc:
        raise MonitorSessionRolloverError("journal_utf8_invalid") from exc
    records: list[dict[str, Any]] = []
    for line_number, line in enumerate(text.splitlines(), 1):
        if not line.strip():
            raise MonitorSessionRolloverError(
                f"journal_blank_line:{line_number}"
            )
        try:
            record = json.loads(line)
        except json.JSONDecodeError as exc:
            raise MonitorSessionRolloverError(
                f"journal_json_invalid:{line_number}"
            ) from exc
        if not isinstance(record, dict):
            raise MonitorSessionRolloverError(
                f"journal_record_must_be_object:{line_number}"
            )
        records.append(record)
    return records, raw


def _read_journal(path: Path) -> list[dict[str, Any]]:
    return _read_journal_snapshot(path)[0]


def _read_target_journal_prefix(
    path: Path,
) -> tuple[list[dict[str, Any]], bytes, bytes, bytes]:
    """Read complete JSONL records plus a possible hard-interruption tail."""

    if not path.exists():
        return [], b"", b"", b""
    try:
        raw = path.read_bytes()
    except OSError as exc:
        raise MonitorSessionRolloverError(
            f"journal_read_failed:{type(exc).__name__}"
        ) from exc
    if not raw or raw.endswith(b"\n"):
        records, validated_raw = _read_journal_snapshot(path)
        return records, validated_raw, b"", validated_raw
    boundary = raw.rfind(b"\n") + 1
    complete_raw = raw[:boundary]
    tail = raw[boundary:]
    try:
        text = complete_raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise MonitorSessionRolloverError("journal_utf8_invalid") from exc
    records: list[dict[str, Any]] = []
    for line_number, line in enumerate(text.splitlines(), 1):
        if not line.strip():
            raise MonitorSessionRolloverError(
                f"journal_blank_line:{line_number}"
            )
        try:
            record = json.loads(line)
        except json.JSONDecodeError as exc:
            raise MonitorSessionRolloverError(
                f"journal_json_invalid:{line_number}"
            ) from exc
        if not isinstance(record, dict):
            raise MonitorSessionRolloverError(
                f"journal_record_must_be_object:{line_number}"
            )
        records.append(record)
    return records, complete_raw, tail, raw


def _pre_rollover_record_allowed(record: Mapping[str, Any], target: str) -> bool:
    return bool(
        record.get("schema_version") == 2
        and record.get("session_date") == target
        and record.get("phase") == "clock_preflight"
        and record.get("event_type") in {"clock_preflight", "data_quality"}
    )


def _append_jsonl_durable(path: Path, record: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    encoded = _canonical_json_bytes(record)
    descriptor = os.open(
        path,
        os.O_APPEND | os.O_CREAT | os.O_WRONLY | getattr(os, "O_BINARY", 0),
        0o600,
    )
    original_size = os.fstat(descriptor).st_size
    bytes_written = 0
    try:
        view = memoryview(encoded)
        while view:
            written = os.write(descriptor, view)
            if written <= 0:
                raise OSError("journal append made no progress")
            bytes_written += written
            view = view[written:]
        os.fsync(descriptor)
    except BaseException:
        # Roll back only bytes provably written by this append.  If the file is
        # larger than our expected partial end, another writer may have raced
        # us and truncation would be unsafe; leave it fail-closed instead.
        try:
            if os.fstat(descriptor).st_size == original_size + bytes_written:
                os.ftruncate(descriptor, original_size)
                os.fsync(descriptor)
        except OSError:
            pass
        raise
    finally:
        os.close(descriptor)


def _rollback_exact_owned_suffix(
    path: Path,
    *,
    original_prefix: bytes,
    observed_raw: bytes,
    owned_suffix: bytes,
) -> bool:
    """Remove only our exact terminal append after a detected interleave."""

    if (
        not owned_suffix
        or not observed_raw.startswith(original_prefix)
        or not observed_raw.endswith(owned_suffix)
    ):
        return False
    retained = observed_raw[: -len(owned_suffix)]
    try:
        descriptor = os.open(
            path,
            os.O_RDWR | getattr(os, "O_BINARY", 0),
            0o600,
        )
    except OSError:
        return False
    try:
        size = os.fstat(descriptor).st_size
        if size != len(observed_raw):
            return False
        os.lseek(descriptor, 0, os.SEEK_SET)
        chunks: list[bytes] = []
        remaining = size
        while remaining:
            chunk = os.read(descriptor, min(remaining, 1024 * 1024))
            if not chunk:
                return False
            chunks.append(chunk)
            remaining -= len(chunk)
        if b"".join(chunks) != observed_raw or os.fstat(descriptor).st_size != size:
            return False
        os.ftruncate(descriptor, len(retained))
        os.fsync(descriptor)
        return True
    except OSError:
        return False
    finally:
        os.close(descriptor)


def _atomic_write_json_impl(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        dir=path.parent,
        prefix=f".{path.name}.",
        suffix=".tmp",
    )
    temporary = Path(temporary_name)
    try:
        encoded = _canonical_json_bytes(payload, pretty=True)
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(encoded)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        if os.name != "nt":
            directory = os.open(path.parent, os.O_RDONLY)
            try:
                os.fsync(directory)
            finally:
                os.close(directory)
    finally:
        if temporary.exists():
            temporary.unlink()


def _atomic_write_json(path: Path, payload: Mapping[str, Any]) -> None:
    """State replacement seam retained for failure-injection tests."""

    _atomic_write_json_impl(path, payload)


def _atomic_write_rollover_intent(
    path: Path, payload: Mapping[str, Any]
) -> None:
    _atomic_write_json_impl(path, payload)


@contextmanager
def _exclusive_lock(path: Path) -> Iterator[None]:
    path.parent.mkdir(parents=True, exist_ok=True)
    handle = path.open("a+b")
    locked = False
    try:
        if path.stat().st_size == 0:
            handle.write(b"0")
            handle.flush()
            os.fsync(handle.fileno())
        handle.seek(0)
        if os.name == "nt":
            import msvcrt

            msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
        else:
            import fcntl

            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        locked = True
        yield
    except (OSError, BlockingIOError) as exc:
        raise MonitorSessionRolloverError("rollover_lock_unavailable") from exc
    finally:
        if locked:
            handle.seek(0)
            if os.name == "nt":
                import msvcrt

                msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl

                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        handle.close()


def _verify_no_intervening_open_session(prior: date, target: date) -> None:
    cursor = prior + timedelta(days=1)
    while cursor < target:
        status = market_calendar_status(cursor)
        if status.get("supported") is not True:
            raise MonitorSessionRolloverError(
                f"intervening_calendar_unverified:{cursor.isoformat()}"
            )
        if status.get("market_open") is True:
            raise MonitorSessionRolloverError(
                f"intervening_open_session_unprocessed:{cursor.isoformat()}"
            )
        cursor += timedelta(days=1)


def _opening_reset(
    target: str, *, schema_version: int = EVENT_SCHEMA_VERSION
) -> dict[str, Any]:
    reset = _OPENING_RESET_BY_SCHEMA_VERSION.get(schema_version)
    if reset is None:
        raise MonitorSessionRolloverError(
            f"opening_reset_schema_version_unsupported:{schema_version}"
        )
    return {"session_date": target, **copy.deepcopy(reset)}


def _is_sha256(value: object) -> bool:
    return bool(
        isinstance(value, str)
        and len(value) == 64
        and all(character in "0123456789abcdef" for character in value)
    )


def _archived_opening_acceptance(
    state: Mapping[str, Any], *, prior: str
) -> dict[str, Any]:
    old_opening = copy.deepcopy(dict(state["opening_acceptance"]))
    nested_session = old_opening.get("session_date")
    if nested_session not in (None, prior):
        raise MonitorSessionRolloverError(
            "opening_acceptance_prior_session_mismatch"
        )
    old_opening["session_date"] = prior
    old_opening["historical_context_only"] = True
    return old_opening


def _archived_wrapper_session_state(state: Mapping[str, Any]) -> dict[str, Any]:
    archived = {
        field: copy.deepcopy(state.get(field))
        for field in _WRAPPER_SESSION_FIELDS
        if field in state
    }
    if "data_quality_notification_outbox" not in archived:
        # September 8 and earlier production state predates the dedicated DQ
        # delivery owner.  Preserve that fact explicitly instead of deriving
        # retroactive notifications from historical cadence receipts.
        archived["data_quality_notification_outbox"] = (
            legacy_pre_activation_outbox(str(state.get("session_date") or ""))
        )
    archived["historical_context_only"] = True
    return archived


def _validate_prior_monitor_scan_ledger(
    state: Mapping[str, Any],
    records: list[dict[str, Any]],
    journal_raw: bytes,
    *,
    rollover_observed_at_utc: datetime | None = None,
) -> None:
    ledger = state.get("monitor_scan_ledger")
    session_date = state.get("session_date")

    def validate_legacy_friday_boundary() -> None:
        """Allow the seeded Friday history, but never a modern scan suffix.

        The 2026-09-04 production state predates ``monitor_scan_ledger``.  That
        one-time compatibility boundary must not also exempt a scan transaction
        appended after the state snapshot (for example, a hard interruption
        after the commit receipt but before state replacement).  Modern scan,
        helper, cadence, and acknowledgement records all use canonical SHA-256
        identities; the explicitly retained Friday seed/correction/recap rows
        do not.
        """

        modern_event_types = {
            "substantive_scan",
            "substantive_scan_commit",
            "policy_notification_ack",
            "cadence_change",
        }
        for record in records:
            if (
                record.get("event_type") in modern_event_types
                or isinstance(record.get("helper_event"), Mapping)
                or _is_sha256(record.get("event_id"))
            ):
                raise MonitorSessionRolloverError(
                    "legacy_friday_contains_unreflected_modern_transaction"
                )

    if ledger is None:
        # Compatibility boundary for the pre-ledger Friday seed.  Its complete
        # journal is still byte-sealed by the rollover receipt below, but first
        # prove it does not contain an unreflected modern transaction.
        if session_date == "2026-09-04":
            validate_legacy_friday_boundary()
            return
        raise MonitorSessionRolloverError(
            "prior_session_monitor_scan_ledger_missing"
        )
    if not isinstance(ledger, Mapping):
        raise MonitorSessionRolloverError(
            "prior_session_monitor_scan_ledger_must_be_an_object"
        )
    if not ledger:
        if session_date == "2026-09-04":
            validate_legacy_friday_boundary()
            return
        try:
            by_id, _receipts = _scan_journal_index(records)
        except MonitorScanLedgerError as exc:
            raise MonitorSessionRolloverError(
                f"prior_session_empty_scan_ledger_invalid:{exc}"
            ) from exc
        if any(
            record.get("event_type") == "substantive_scan"
            or record.get("event_type") == "substantive_scan_commit"
            or record.get("event_type") == NOTIFICATION_ACK_EVENT_TYPE
            or isinstance(record.get("helper_event"), Mapping)
            for record in by_id.values()
        ):
            raise MonitorSessionRolloverError(
                "prior_session_empty_scan_ledger_has_scan_receipts"
            )
        prior_reference = state.get("prior_session_reference")
        archived_policy = (
            prior_reference.get("policy_evaluator_at_rollover")
            if isinstance(prior_reference, Mapping)
            else None
        )
        if (
            state.get("last_scan_utc") is not None
            or not isinstance(archived_policy, Mapping)
            or archived_policy != state.get("policy_evaluator")
        ):
            raise MonitorSessionRolloverError(
                "prior_session_empty_scan_ledger_state_invalid"
            )
        try:
            validate_data_quality_notification_outbox_receipt_replay(
                state,
                records,
                journal_raw,
                require_notifications_acknowledged=True,
                rollover_observed_at_utc=rollover_observed_at_utc,
            )
        except MonitorDataQualityOutboxError as exc:
            raise MonitorSessionRolloverError(
                f"prior_session_data_quality_outbox_invalid:{exc}"
            ) from exc
        return
    try:
        by_id, _receipts = _scan_journal_index(records)
        validated_ledger = _validated_scan_ledger_state(
            ledger, state, by_id, journal_raw
        )
        # The state ledger anchors only the durably reflected transaction
        # prefix.  Refuse to advance the session while any scan transaction is
        # still present solely in the journal (for example, a crash after the
        # scan or commit-receipt append but before state replacement).  Such a
        # suffix must be completed through the exact scan retry path first;
        # otherwise rollover would seal and abandon evidence that never became
        # authoritative policy state.
        journal_scan_ids = [
            str(record.get("event_id"))
            for record in records
            if record.get("event_type") == "substantive_scan"
        ]
        journal_policy_ids = [
            str(record.get("event_id"))
            for record in records
            if isinstance(record.get("helper_event"), Mapping)
        ]
        journal_commit_ids = [
            str(record.get("event_id"))
            for record in records
            if record.get("event_type") == "substantive_scan_commit"
        ]
        journal_ack_ids = [
            str(record.get("event_id"))
            for record in records
            if record.get("event_type") == NOTIFICATION_ACK_EVENT_TYPE
        ]
        committed_ack_ids = validated_ledger.get(
            "committed_notification_ack_event_ids"
        )
        if journal_ack_ids != committed_ack_ids:
            unreflected_ack_ids = [
                event_id
                for event_id in journal_ack_ids
                if not isinstance(committed_ack_ids, list)
                or event_id not in committed_ack_ids
            ]
            raise MonitorScanLedgerError(
                "prior_session_unreflected_notification_ack:"
                + ",".join(unreflected_ack_ids or journal_ack_ids)
            )
        if rollover_observed_at_utc is not None:
            if (
                rollover_observed_at_utc.tzinfo is None
                or rollover_observed_at_utc.utcoffset() is None
            ):
                raise MonitorScanLedgerError(
                    "rollover_observed_at_utc_must_be_timezone_aware"
                )
            rollover_utc = rollover_observed_at_utc.astimezone(timezone.utc)
            for ack_event_id in journal_ack_ids:
                ack = by_id[ack_event_id]
                ack_observed = datetime.fromisoformat(
                    str(ack.get("observed_at_utc") or "").replace("Z", "+00:00")
                ).astimezone(timezone.utc)
                if ack_observed >= rollover_utc:
                    raise MonitorScanLedgerError(
                        "prior_session_notification_ack_not_before_rollover:"
                        + ack_event_id
                    )
        pending_notification_ids = validated_ledger.get(
            "pending_notification_event_ids"
        )
        if isinstance(pending_notification_ids, list) and pending_notification_ids:
            raise MonitorScanLedgerError(
                "prior_session_pending_notifications:"
                + ",".join(str(event_id) for event_id in pending_notification_ids)
            )
        if (
            journal_scan_ids
            != validated_ledger.get("committed_scan_event_ids")
            or journal_policy_ids
            != validated_ledger.get("committed_policy_event_ids")
            or journal_commit_ids
            != validated_ledger.get("committed_receipt_event_ids")
        ):
            raise MonitorScanLedgerError(
                "unreflected_prior_session_scan_transaction_requires_exact_retry"
            )
        try:
            validate_data_quality_notification_outbox_receipt_replay(
                state,
                records,
                journal_raw,
                require_notifications_acknowledged=True,
                rollover_observed_at_utc=rollover_observed_at_utc,
            )
        except MonitorDataQualityOutboxError as exc:
            raise MonitorScanLedgerError(
                f"data_quality_outbox_invalid:{exc}"
            ) from exc
    except MonitorScanLedgerError as exc:
        raise MonitorSessionRolloverError(
            f"prior_session_monitor_scan_ledger_invalid:{exc}"
        ) from exc


def _build_prior_session_reference(
    state: Mapping[str, Any],
    *,
    prior: str,
    state_sha256: str,
    prior_journal_raw: bytes,
) -> dict[str, Any]:
    opening = _archived_opening_acceptance(state, prior=prior)
    wrapper = _archived_wrapper_session_state(state)
    policy = copy.deepcopy(dict(state["policy_evaluator"]))
    return {
        "session_date": prior,
        "journal": f"exports/market_monitor/{prior}.jsonl",
        "historical_context_only": True,
        "eligible_for_live_confirmation": False,
        "opening_acceptance": opening,
        "wrapper_session_state": wrapper,
        "policy_evaluator_at_rollover": policy,
        "prior_state_sha256": state_sha256,
        "prior_opening_acceptance_sha256": _value_sha256(opening),
        "prior_wrapper_session_state_sha256": _value_sha256(wrapper),
        "policy_evaluator_sha256": _value_sha256(policy),
        "prior_journal_size_bytes": len(prior_journal_raw),
        "prior_journal_sha256": _file_sha256(prior_journal_raw),
    }


def _build_event(
    *,
    target: str,
    prior: str,
    observed_utc: datetime,
    calendar: Mapping[str, Any],
    state_sha256: str,
    opening_sha256: str,
    wrapper_sha256: str,
    policy_sha256: str,
    prior_journal_size: int,
    prior_journal_sha256: str,
) -> dict[str, Any]:
    observed_ct = observed_utc.astimezone(CT)
    return {
        "schema_version": EVENT_SCHEMA_VERSION,
        "event_id": f"{target}:session:session_rollover:v1",
        "event_type": "session_rollover",
        "observed_at_ct": observed_ct.isoformat(),
        "observed_at_utc": _utc_iso(observed_utc),
        "session_date": target,
        "phase": "preopen_acceptance",
        "cadence": copy.deepcopy(_ROLLOVER_CADENCE),
        "evidence": [
            f"calendar:{CALENDAR_PATH.name}",
            f"prior_state_sha256:{state_sha256}",
            f"prior_journal_sha256:{prior_journal_sha256}",
        ],
        "symbols": {},
        "alerts": [],
        "directional_interpretation": _ROLLOVER_DIRECTIONAL_INTERPRETATION,
        "research_hypotheses": [],
        "prior_session_date": prior,
        "market_calendar": copy.deepcopy(dict(calendar)),
        "prior_state_sha256": state_sha256,
        "prior_opening_acceptance_sha256": opening_sha256,
        "prior_wrapper_session_state_sha256": wrapper_sha256,
        "policy_evaluator_sha256": policy_sha256,
        "prior_journal_size_bytes": prior_journal_size,
        "prior_journal_sha256": prior_journal_sha256,
        "opening_acceptance_reset": _opening_reset(target),
        "append_before_state_required": True,
        "policy_evaluator_preserved_for_helper_rollover": True,
    }


def _build_rollover_intent(event: Mapping[str, Any]) -> dict[str, Any]:
    copied = copy.deepcopy(dict(event))
    return {
        "schema_version": ROLLOVER_INTENT_SCHEMA,
        "session_date": copied.get("session_date"),
        "event_id": copied.get("event_id"),
        "event_sha256": _value_sha256(copied),
        "event": copied,
    }


def _load_rollover_intent(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text("utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise MonitorSessionRolloverError(
            "rollover_intent_invalid"
        ) from exc
    if not isinstance(payload, dict) or set(payload) != {
        "schema_version",
        "session_date",
        "event_id",
        "event_sha256",
        "event",
    }:
        raise MonitorSessionRolloverError("rollover_intent_invalid")
    event = payload.get("event")
    if (
        payload.get("schema_version") != ROLLOVER_INTENT_SCHEMA
        or not isinstance(event, Mapping)
        or payload.get("session_date") != event.get("session_date")
        or payload.get("event_id") != event.get("event_id")
        or not _is_sha256(payload.get("event_sha256"))
        or payload.get("event_sha256") != _value_sha256(event)
    ):
        raise MonitorSessionRolloverError("rollover_intent_invalid")
    return copy.deepcopy(payload)


def _validate_event_receipt(
    event: Mapping[str, Any], expected: Mapping[str, Any]
) -> None:
    observed_utc = event.get("observed_at_utc")
    try:
        parsed_utc = datetime.fromisoformat(str(observed_utc).replace("Z", "+00:00"))
    except ValueError as exc:
        raise MonitorSessionRolloverError(
            "rollover_event_timestamp_invalid"
        ) from exc
    if parsed_utc.tzinfo is None or parsed_utc.utcoffset() is None:
        raise MonitorSessionRolloverError("rollover_event_timestamp_invalid")
    if parsed_utc.astimezone(CT).date().isoformat() != expected["session_date"]:
        raise MonitorSessionRolloverError("rollover_event_timestamp_misaligned")
    expected_for_receipt = _build_event(
        target=str(expected["session_date"]),
        prior=str(expected["prior_session_date"]),
        observed_utc=parsed_utc.astimezone(timezone.utc),
        calendar=dict(expected["market_calendar"]),
        state_sha256=str(expected["prior_state_sha256"]),
        opening_sha256=str(expected["prior_opening_acceptance_sha256"]),
        wrapper_sha256=str(expected["prior_wrapper_session_state_sha256"]),
        policy_sha256=str(expected["policy_evaluator_sha256"]),
        prior_journal_size=int(expected["prior_journal_size_bytes"]),
        prior_journal_sha256=str(expected["prior_journal_sha256"]),
    )
    if dict(event) != expected_for_receipt:
        raise MonitorSessionRolloverError("rollover_event_receipt_mismatch")


def _validate_current_event_receipt(
    event: Mapping[str, Any],
    *,
    state: Mapping[str, Any],
    journal_dir: Path,
    target: str,
    event_id: str,
) -> None:
    current = _canonical_date(target)
    reference = state.get("prior_session_reference")
    if not isinstance(reference, Mapping):
        raise MonitorSessionRolloverError("current_session_rollover_receipt_invalid")
    prior = _canonical_date(reference.get("session_date"))
    if (
        prior is None
        or current is None
        or prior >= current
        or event.get("prior_session_date") != prior.isoformat()
        or reference.get("journal")
        != f"exports/market_monitor/{prior.isoformat()}.jsonl"
        or reference.get("historical_context_only") is not True
        or reference.get("eligible_for_live_confirmation") is not False
    ):
        raise MonitorSessionRolloverError("current_session_rollover_receipt_invalid")

    opening = reference.get("opening_acceptance")
    wrapper = reference.get("wrapper_session_state")
    policy = reference.get("policy_evaluator_at_rollover")
    if not all(isinstance(value, Mapping) for value in (opening, wrapper, policy)):
        raise MonitorSessionRolloverError(
            "current_session_prior_reference_invalid"
        )
    assert isinstance(opening, Mapping)
    assert isinstance(wrapper, Mapping)
    assert isinstance(policy, Mapping)
    if (
        opening.get("session_date") != prior.isoformat()
        or opening.get("historical_context_only") is not True
        or wrapper.get("historical_context_only") is not True
    ):
        raise MonitorSessionRolloverError(
            "current_session_prior_reference_invalid"
        )

    reference_hashes = {
        "prior_opening_acceptance_sha256": _value_sha256(opening),
        "prior_wrapper_session_state_sha256": _value_sha256(wrapper),
        "policy_evaluator_sha256": _value_sha256(policy),
    }
    for field, expected_hash in reference_hashes.items():
        if not _is_sha256(reference.get(field)) or reference.get(field) != expected_hash:
            raise MonitorSessionRolloverError(
                "current_session_prior_reference_hash_mismatch"
            )
    if not _is_sha256(reference.get("prior_state_sha256")):
        raise MonitorSessionRolloverError(
            "current_session_prior_reference_invalid"
        )

    prior_journal_size = reference.get("prior_journal_size_bytes")
    prior_journal_sha256 = reference.get("prior_journal_sha256")
    if (
        type(prior_journal_size) is not int
        or prior_journal_size < 0
        or not _is_sha256(prior_journal_sha256)
    ):
        raise MonitorSessionRolloverError(
            "current_session_prior_reference_invalid"
        )
    prior_journal_path = journal_dir / f"{prior.isoformat()}.jsonl"
    prior_records, prior_journal_raw = _read_journal_snapshot(prior_journal_path)
    if (
        len(prior_journal_raw) != prior_journal_size
        or _file_sha256(prior_journal_raw) != prior_journal_sha256
    ):
        raise MonitorSessionRolloverError(
            "current_session_prior_journal_seal_mismatch"
        )

    archived_state = copy.deepcopy(dict(state))
    archived_state.update(copy.deepcopy(dict(wrapper)))
    archived_state["session_date"] = prior.isoformat()
    archived_state["opening_acceptance"] = copy.deepcopy(dict(opening))
    archived_state["policy_evaluator"] = copy.deepcopy(dict(policy))
    archived_state["monitor_scan_ledger"] = copy.deepcopy(
        wrapper.get("monitor_scan_ledger")
    )
    try:
        parsed_utc = datetime.fromisoformat(
            str(event.get("observed_at_utc")).replace("Z", "+00:00")
        )
    except ValueError as exc:
        raise MonitorSessionRolloverError(
            "current_session_rollover_receipt_invalid"
        ) from exc
    if parsed_utc.tzinfo is None or parsed_utc.utcoffset() is None:
        raise MonitorSessionRolloverError(
            "current_session_rollover_receipt_invalid"
        )
    _validate_prior_monitor_scan_ledger(
        archived_state,
        prior_records,
        prior_journal_raw,
        rollover_observed_at_utc=parsed_utc.astimezone(timezone.utc),
    )
    calendar = market_calendar_status(current)
    expected = _build_event(
        target=target,
        prior=prior.isoformat(),
        observed_utc=parsed_utc.astimezone(timezone.utc),
        calendar=calendar,
        state_sha256=str(reference["prior_state_sha256"]),
        opening_sha256=reference_hashes["prior_opening_acceptance_sha256"],
        wrapper_sha256=reference_hashes["prior_wrapper_session_state_sha256"],
        policy_sha256=reference_hashes["policy_evaluator_sha256"],
        prior_journal_size=prior_journal_size,
        prior_journal_sha256=str(prior_journal_sha256),
    )
    if (
        event_id != expected["event_id"]
        or dict(event) != expected
    ):
        raise MonitorSessionRolloverError(
            "current_session_rollover_receipt_invalid"
        )


def _validate_opening_acceptance_receipts(
    state: Mapping[str, Any],
    records: list[dict[str, Any]],
    *,
    require_notifications_acknowledged: bool,
) -> None:
    """Reject orphan/tampered opening receipts before their namespace is reset."""

    session_date = str(state.get("session_date") or "")
    opening = state.get("opening_acceptance")
    if not isinstance(opening, Mapping) or opening.get("session_date") != session_date:
        raise MonitorSessionRolloverError(
            "opening_acceptance_prior_session_mismatch"
        )
    rollover_id = state.get("session_rollover_event_id")
    rollover_positions = [
        index
        for index, record in enumerate(records)
        if record.get("event_id") == rollover_id
    ]
    if len(rollover_positions) != 1:
        raise MonitorSessionRolloverError(
            "prior_session_rollover_receipt_missing_or_duplicate"
        )
    relevant = [
        (index, record)
        for index, record in enumerate(records)
        if record.get("event_type")
        in {_OPENING_MILESTONE_EVENT_TYPE, _OPENING_ACK_EVENT_TYPE}
    ]
    if any(index <= rollover_positions[0] for index, _record in relevant):
        raise MonitorSessionRolloverError(
            "opening_receipt_must_follow_session_rollover"
        )
    try:
        # Lazy import avoids the owner's dependency on rollover receipt
        # validation while keeping one authoritative semantic replay.
        from backend.monitor_opening_acceptance import (
            MonitorOpeningAcceptanceError,
            validate_opening_acceptance_receipt_replay,
        )

        validate_opening_acceptance_receipt_replay(
            records=records,
            state_opening=opening,
            session_date=session_date,
            require_notifications_acknowledged=(
                require_notifications_acknowledged
            ),
        )
    except MonitorOpeningAcceptanceError as exc:
        if str(exc).startswith("prior_session_"):
            raise MonitorSessionRolloverError(str(exc)) from exc
        raise MonitorSessionRolloverError(
            "prior_session_opening_semantic_replay_failed:" + str(exc)
        ) from exc
    latest_observed: datetime | None = None
    for record in records:
        if record.get("session_date") != session_date:
            continue
        raw_observed = record.get("observed_at_utc")
        if raw_observed is None:
            continue
        try:
            observed = datetime.fromisoformat(
                str(raw_observed).replace("Z", "+00:00")
            )
        except ValueError as exc:
            raise MonitorSessionRolloverError(
                "prior_session_record_observed_at_invalid"
            ) from exc
        if observed.tzinfo is None or observed.utcoffset() is None:
            raise MonitorSessionRolloverError(
                "prior_session_record_observed_at_invalid"
            )
        observed = observed.astimezone(timezone.utc)
        if (
            record.get("event_type")
            in {_OPENING_MILESTONE_EVENT_TYPE, _OPENING_ACK_EVENT_TYPE}
            and latest_observed is not None
            and observed <= latest_observed
        ):
            raise MonitorSessionRolloverError(
                "prior_session_opening_receipt_not_after_latest_journal_record"
            )
        if latest_observed is None or observed > latest_observed:
            latest_observed = observed

    milestone_rows = [
        record
        for _index, record in relevant
        if record.get("event_type") == _OPENING_MILESTONE_EVENT_TYPE
    ]
    if len(milestone_rows) > len(_OPENING_MILESTONE_SPECS):
        raise MonitorSessionRolloverError(
            "too_many_opening_milestone_receipts"
        )
    milestone_by_name: dict[str, dict[str, Any]] = {}
    committed_names: set[str] = set()
    for row in milestone_rows:
        milestone_name = str(row.get("milestone") or "")
        if milestone_name not in _OPENING_MILESTONE_DEPENDENCIES:
            raise MonitorSessionRolloverError(
                "prior_session_opening_milestone_invalid"
            )
        if milestone_name in milestone_by_name:
            raise MonitorSessionRolloverError(
                "prior_session_opening_milestone_duplicate"
            )
        missing_dependencies = [
            dependency
            for dependency in _OPENING_MILESTONE_DEPENDENCIES[milestone_name]
            if dependency not in committed_names
        ]
        if missing_dependencies:
            raise MonitorSessionRolloverError(
                "prior_session_opening_milestone_prerequisite_missing:"
                + missing_dependencies[0]
            )
        expected_event_id = _value_sha256(
            {
                "event_schema": row.get("event_schema"),
                "event_type": _OPENING_MILESTONE_EVENT_TYPE,
                "session_date": session_date,
                "milestone": milestone_name,
            }
        )
        if (
            row.get("schema_version") != 2
            or row.get("event_schema")
            != "marketpin-monitor-opening-acceptance-milestone.v1"
            or row.get("session_date") != session_date
            or not _is_sha256(row.get("event_id"))
            or row.get("event_id") != expected_event_id
        ):
            raise MonitorSessionRolloverError(
                "prior_session_opening_milestone_invalid"
            )
        milestone_by_name[milestone_name] = row
        committed_names.add(milestone_name)

    milestone_by_id = {str(row["event_id"]): row for row in milestone_rows}
    simulated_pending: list[str] = []
    acked_by: dict[str, str] = {}
    validated_ack_ids: list[str] = []
    for _index, row in relevant:
        if row.get("event_type") == _OPENING_MILESTONE_EVENT_TYPE:
            simulated_pending.append(str(row["event_id"]))
            continue
        material = copy.deepcopy(dict(row))
        material.pop("event_id", None)
        expected_ack_id = _value_sha256(
            {
                "event_schema": (
                    "marketpin-monitor-opening-acceptance-notification-ack.v1"
                ),
                "ack": material,
            }
        )
        commitments = row.get("acked_milestone_events")
        if not isinstance(commitments, list) or not commitments:
            raise MonitorSessionRolloverError(
                "prior_session_opening_notification_ack_invalid"
            )
        acked_ids: list[str] = []
        for commitment in commitments:
            if not isinstance(commitment, Mapping) or set(commitment) != {
                "event_id",
                "record_sha256",
            }:
                raise MonitorSessionRolloverError(
                    "prior_session_opening_notification_ack_invalid"
                )
            acked_id = str(commitment.get("event_id") or "")
            milestone = milestone_by_id.get(acked_id)
            if (
                milestone is None
                or commitment.get("record_sha256") != _value_sha256(milestone)
            ):
                raise MonitorSessionRolloverError(
                    "prior_session_opening_notification_ack_commitment_invalid"
                )
            acked_ids.append(acked_id)
        proof = row.get("delivery_proof")
        if (
            row.get("schema_version") != 2
            or row.get("event_schema")
            != "marketpin-monitor-opening-acceptance-notification-ack.v1"
            or row.get("session_date") != session_date
            or row.get("event_id") != expected_ack_id
            or row.get("pending_notification_event_ids_before")
            != simulated_pending
            or acked_ids != simulated_pending[: len(acked_ids)]
            or row.get("pending_notification_event_ids_after")
            != simulated_pending[len(acked_ids) :]
            or not isinstance(proof, Mapping)
            or set(proof)
            != {
                "proof_type",
                "conversation_history_sha256",
                "prior_final_delivered_at_utc",
            }
            or proof.get("proof_type") != "prior_heartbeat_final_delivered"
            or not _is_sha256(proof.get("conversation_history_sha256"))
            or row.get("evidence")
            != {"delivery_proof_sha256": _value_sha256(proof)}
        ):
            raise MonitorSessionRolloverError(
                "prior_session_opening_notification_ack_invalid"
            )
        ack_id = str(row["event_id"])
        for acked_id in acked_ids:
            acked_by[acked_id] = ack_id
        simulated_pending = simulated_pending[len(acked_ids) :]
        validated_ack_ids.append(ack_id)

    reflected_ids: set[str] = set()
    pending_by_id: set[str] = set()
    for name, flag, detail_name in _OPENING_MILESTONE_SPECS:
        detail = opening.get(detail_name)
        flag_value = opening.get(flag)
        row = milestone_by_name.get(name)
        if type(flag_value) is not bool:
            raise MonitorSessionRolloverError(
                f"opening_flag_invalid:{flag}"
            )
        if detail is None:
            if row is not None:
                raise MonitorSessionRolloverError(
                    "prior_session_unreflected_opening_milestone:"
                    + str(row.get("event_id") or "")
                )
            if flag_value is not False:
                raise MonitorSessionRolloverError(
                    f"opening_flag_without_receipt:{flag}"
                )
            continue
        if not isinstance(detail, Mapping) or row is None:
            raise MonitorSessionRolloverError(
                f"opening_state_receipt_without_journal:{detail_name}"
            )
        event_id = str(row["event_id"])
        if (
            detail.get("event_id") != event_id
            or detail.get("event_sha256") != _value_sha256(row)
            or detail.get("committed") is not True
            or detail.get("notified") is not flag_value
            or detail.get("notification_status")
            != ("acknowledged" if flag_value else "pending")
            or (
                flag_value
                and detail.get("notification_ack_event_id")
                != acked_by.get(event_id)
            )
            or (
                not flag_value
                and detail.get("notification_ack_event_id") is not None
            )
        ):
            raise MonitorSessionRolloverError(
                f"opening_state_receipt_mismatch:{detail_name}"
            )
        reflected_ids.add(event_id)
        if not flag_value:
            pending_by_id.add(event_id)
    if len(reflected_ids) != len(milestone_rows):
        orphan = next(
            row
            for row in milestone_rows
            if str(row.get("event_id") or "") not in reflected_ids
        )
        raise MonitorSessionRolloverError(
            "prior_session_unreflected_opening_milestone:"
            + str(orphan.get("event_id") or "")
        )

    pending_ids = [
        str(row["event_id"])
        for row in milestone_rows
        if str(row["event_id"]) in pending_by_id
    ]

    raw_pending = opening.get("pending_notification_event_ids", [])
    if raw_pending != pending_ids or raw_pending != simulated_pending:
        raise MonitorSessionRolloverError(
            "prior_session_opening_pending_notification_mismatch"
        )
    ack_rows = [
        record
        for _index, record in relevant
        if record.get("event_type") == _OPENING_ACK_EVENT_TYPE
    ]
    ack_ids = [str(record.get("event_id") or "") for record in ack_rows]
    if ack_ids != validated_ack_ids:
        raise MonitorSessionRolloverError(
            "prior_session_opening_notification_ack_invalid"
        )
    state_ack_ids = opening.get("committed_notification_ack_event_ids", [])
    if ack_ids != state_ack_ids:
        orphan_ids = [event_id for event_id in ack_ids if event_id not in state_ack_ids]
        if orphan_ids:
            raise MonitorSessionRolloverError(
                "prior_session_unreflected_opening_notification_ack:"
                + orphan_ids[0]
            )
        raise MonitorSessionRolloverError(
            "prior_session_opening_notification_ack_mismatch"
        )
    if any(not _is_sha256(event_id) for event_id in ack_ids):
        raise MonitorSessionRolloverError(
            "prior_session_opening_notification_ack_invalid"
        )
    if ack_rows:
        if (
            opening.get("last_notification_ack_event_id") != ack_ids[-1]
            or opening.get("last_notification_ack_sha256")
            != _value_sha256(ack_rows[-1])
        ):
            raise MonitorSessionRolloverError(
                "prior_session_opening_notification_ack_anchor_invalid"
            )
    elif (
        opening.get("last_notification_ack_event_id") is not None
        or opening.get("last_notification_ack_sha256") is not None
    ):
        raise MonitorSessionRolloverError(
            "prior_session_opening_notification_ack_anchor_invalid"
        )
    if require_notifications_acknowledged and pending_ids:
        raise MonitorSessionRolloverError(
            "prior_session_opening_notification_pending:" + pending_ids[0]
        )


def _validate_prior_session_rollover_boundary(
    state: Mapping[str, Any],
    records: list[dict[str, Any]],
    *,
    journal_dir: Path,
) -> None:
    """Verify the live prior session's own rollover before advancing again."""

    session_date = state.get("session_date")
    if session_date == "2026-09-04":
        # Explicit compatibility boundary for the seeded pre-rollover Friday.
        return
    event_id = state.get("session_rollover_event_id")
    if not isinstance(event_id, str) or not event_id:
        raise MonitorSessionRolloverError(
            "prior_session_rollover_event_id_missing"
        )
    matching = [record for record in records if record.get("event_id") == event_id]
    if len(matching) != 1:
        raise MonitorSessionRolloverError(
            "prior_session_rollover_receipt_missing_or_duplicate"
        )
    event = matching[0]
    if event.get("event_type") != "session_rollover":
        raise MonitorSessionRolloverError(
            "prior_session_rollover_event_type_invalid"
        )
    _validate_current_event_receipt(
        event,
        state=state,
        journal_dir=journal_dir,
        target=str(session_date or ""),
        event_id=event_id,
    )
    _validate_opening_acceptance_receipts(
        state, records, require_notifications_acknowledged=True
    )


def _build_next_state(
    state: Mapping[str, Any],
    *,
    target: str,
    observed_utc: datetime,
    event_id: str,
    prior_reference: Mapping[str, Any],
) -> dict[str, Any]:
    policy_evaluator = copy.deepcopy(dict(state["policy_evaluator"]))
    result = copy.deepcopy(dict(state))
    result.update(
        {
            "schema_version": 2,
            "session_date": target,
            "updated_at_ct": observed_utc.astimezone(CT).isoformat(),
            "updated_at_utc": _utc_iso(observed_utc),
            "session_rollover_event_id": event_id,
            "mode": "NORMAL",
            "mode_reason": "session_rollover",
            "elevated_since_ct": None,
            "elevated_minimum_until_ct": None,
            "stable_elevated_scan_count": 0,
            "opening_acceptance": _opening_reset(target),
            # Constructed from only the immediate prior session.  Never carry
            # arbitrary extensions from an older prior_session_reference.
            "prior_session_reference": copy.deepcopy(dict(prior_reference)),
            "comparison_baselines_eligibility": "new_session_uninitialized",
            "comparison_baselines": {},
            "pending_confirmations": {},
            "pending_zero_gamma_confirmation": {},
            "pending_pin_contest": {},
            "pending_directional_confirmation": {},
            "last_alerted_levels": {},
            "preflight_alerts": {},
            "runtime_alerts": {},
            "last_alert": None,
            "current_data_quality": {},
            "last_scan_ct": None,
            "last_scan_utc": None,
            "last_eligible_scan_ct": None,
            "last_scan_result": "session_rollover_pending_first_scan",
            # The ledger anchor is session-journal scoped. Preserve the prior
            # anchor only in wrapper_session_state and require the first scan
            # of the new session to establish a fresh journal/state boundary.
            "monitor_scan_ledger": {},
            # The dedicated data-quality delivery owner activates only at a
            # durable target-session rollover.  Historical source receipts in
            # a namespace that lacked this marker stay raw evidence and are
            # never retroactively wrapped.
            "data_quality_notification_outbox": (
                armed_outbox(target, event_id)
                if target >= "2026-09-09"
                else legacy_pre_activation_outbox(target)
            ),
            "policy_evaluator": policy_evaluator,
        }
    )
    if result["policy_evaluator"] != state["policy_evaluator"]:
        raise MonitorSessionRolloverError("policy_evaluator_preservation_failed")
    return result


def _base_result(
    *,
    state_path: Path,
    journal_path: Path,
    target: str,
    observed_utc: datetime,
    calendar: Mapping[str, Any],
    dry_run: bool,
) -> dict[str, Any]:
    return {
        "schema_version": RESULT_SCHEMA,
        "output_mode": "compact",
        "accepted": False,
        "action": "abstain",
        "session_date": target,
        "observed_at_ct": observed_utc.astimezone(CT).isoformat(),
        "observed_at_utc": _utc_iso(observed_utc),
        "market_calendar": copy.deepcopy(dict(calendar)),
        "state_path": str(state_path),
        "journal_path": str(journal_path),
        "intent_path": str(
            journal_path.with_name(
                f"{target}.session_rollover_intent.json"
            )
        ),
        "dry_run": dry_run,
        "journal_event_appended": False,
        "journal_tail_recovered": False,
        "intent_created": False,
        "intent_reused": False,
        "durable_event_reused": False,
        "state_updated": False,
        "commit_phase": "not_started",
        "event_id": f"{target}:session:session_rollover:v1",
        "issues": [],
    }


def prepare_monitor_session(
    *,
    state_path: Path,
    journal_dir: Path,
    observed_at_utc: datetime | None = None,
    session_date: str | None = None,
    dry_run: bool = False,
    failpoint: Callable[[str], None] | None = None,
) -> dict[str, Any]:
    """Prepare exactly one verified open-session monitor namespace."""

    try:
        observed_utc = _normalized_observed_at(observed_at_utc)
    except MonitorSessionRolloverError as exc:
        return {
            "schema_version": RESULT_SCHEMA,
            "output_mode": "compact",
            "accepted": False,
            "action": "abstain",
            "issues": [str(exc)],
        }
    observed_date = observed_utc.astimezone(CT).date()
    requested_date = _canonical_date(session_date) if session_date is not None else observed_date
    if requested_date is None:
        calendar: dict[str, Any] = {
            "date": session_date,
            "supported": False,
            "market_open": False,
            "reason": "requested_session_date_invalid",
        }
        target = str(session_date or "")
    else:
        target = requested_date.isoformat()
        calendar = market_calendar_status(requested_date)
    journal_path = journal_dir / f"{target}.jsonl"
    result = _base_result(
        state_path=state_path,
        journal_path=journal_path,
        target=target,
        observed_utc=observed_utc,
        calendar=calendar,
        dry_run=dry_run,
    )

    if requested_date is None:
        result["issues"] = ["session_date_invalid"]
        return result
    if requested_date != observed_date:
        result["issues"] = ["session_date_does_not_match_observed_ct_date"]
        return result
    if calendar.get("supported") is not True:
        result["issues"] = ["market_calendar_unverified"]
        return result
    if calendar.get("market_open") is not True:
        result.update(
            {
                "accepted": True,
                "action": "non_open_session_noop",
                "issues": [],
            }
        )
        return result

    def execute_locked() -> dict[str, Any]:
        state, _raw, state_sha256 = _load_state(state_path)
        prior_date = _canonical_date(state["session_date"])
        assert prior_date is not None
        if requested_date < prior_date:
            raise MonitorSessionRolloverError("session_date_rollback_rejected")

        event_id = str(result["event_id"])
        intent_path = Path(str(result["intent_path"]))
        (
            records,
            target_journal_raw,
            target_partial_tail,
            target_observed_raw,
        ) = _read_target_journal_prefix(journal_path)
        matching = [record for record in records if record.get("event_id") == event_id]
        if len(matching) > 1:
            raise MonitorSessionRolloverError("duplicate_session_rollover_event")

        if requested_date == prior_date:
            opening = state.get("opening_acceptance") or {}
            if opening.get("session_date") != target:
                raise MonitorSessionRolloverError(
                    "current_session_opening_acceptance_not_armed"
                )
            if state.get("session_rollover_event_id") != event_id or len(matching) != 1:
                raise MonitorSessionRolloverError(
                    "current_session_rollover_receipt_missing"
                )
            _validate_current_event_receipt(
                matching[0],
                state=state,
                journal_dir=journal_dir,
                target=target,
                event_id=event_id,
            )
            _validate_opening_acceptance_receipts(
                state,
                records,
                require_notifications_acknowledged=False,
            )
            if any(
                not _pre_rollover_record_allowed(record, target)
                for record in records[: records.index(matching[0])]
            ):
                raise MonitorSessionRolloverError(
                    "signal_record_precedes_session_rollover"
                )
            result.update(
                {
                    "accepted": True,
                    "action": "already_current",
                    "durable_event_reused": True,
                    "commit_phase": "complete",
                    "issues": [],
                }
            )
            return result

        _verify_no_intervening_open_session(prior_date, requested_date)
        prior_journal_path = journal_dir / f"{prior_date.isoformat()}.jsonl"
        prior_records, prior_journal_raw = _read_journal_snapshot(
            prior_journal_path
        )
        _validate_prior_session_rollover_boundary(
            state,
            prior_records,
            journal_dir=journal_dir,
        )
        prior_reference = _build_prior_session_reference(
            state,
            prior=prior_date.isoformat(),
            state_sha256=state_sha256,
            prior_journal_raw=prior_journal_raw,
        )
        candidate_event = _build_event(
            target=target,
            prior=prior_date.isoformat(),
            observed_utc=observed_utc,
            calendar=calendar,
            state_sha256=state_sha256,
            opening_sha256=str(
                prior_reference["prior_opening_acceptance_sha256"]
            ),
            wrapper_sha256=str(
                prior_reference["prior_wrapper_session_state_sha256"]
            ),
            policy_sha256=str(prior_reference["policy_evaluator_sha256"]),
            prior_journal_size=int(
                prior_reference["prior_journal_size_bytes"]
            ),
            prior_journal_sha256=str(
                prior_reference["prior_journal_sha256"]
            ),
        )
        durable_intent = _load_rollover_intent(intent_path)
        effective_observed_utc = observed_utc
        if matching:
            try:
                durable_observed = datetime.fromisoformat(
                    str(matching[0].get("observed_at_utc")).replace(
                        "Z", "+00:00"
                    )
                )
            except ValueError as exc:
                raise MonitorSessionRolloverError(
                    "rollover_event_timestamp_invalid"
                ) from exc
            if (
                durable_observed.tzinfo is None
                or durable_observed.utcoffset() is None
            ):
                raise MonitorSessionRolloverError(
                    "rollover_event_timestamp_invalid"
                )
            effective_observed_utc = durable_observed.astimezone(timezone.utc)
            result["observed_at_utc"] = _utc_iso(effective_observed_utc)
            result["observed_at_ct"] = effective_observed_utc.astimezone(
                CT
            ).isoformat()
            expected_event = _build_event(
                target=target,
                prior=prior_date.isoformat(),
                observed_utc=effective_observed_utc,
                calendar=calendar,
                state_sha256=state_sha256,
                opening_sha256=str(
                    prior_reference["prior_opening_acceptance_sha256"]
                ),
                wrapper_sha256=str(
                    prior_reference["prior_wrapper_session_state_sha256"]
                ),
                policy_sha256=str(
                    prior_reference["policy_evaluator_sha256"]
                ),
                prior_journal_size=int(
                    prior_reference["prior_journal_size_bytes"]
                ),
                prior_journal_sha256=str(
                    prior_reference["prior_journal_sha256"]
                ),
            )
        elif durable_intent is not None:
            intent_event = durable_intent["event"]
            assert isinstance(intent_event, Mapping)
            _validate_event_receipt(intent_event, candidate_event)
            expected_event = copy.deepcopy(dict(intent_event))
            parsed_intent_time = datetime.fromisoformat(
                str(expected_event["observed_at_utc"]).replace(
                    "Z", "+00:00"
                )
            )
            effective_observed_utc = parsed_intent_time.astimezone(timezone.utc)
            result["observed_at_utc"] = _utc_iso(effective_observed_utc)
            result["observed_at_ct"] = effective_observed_utc.astimezone(
                CT
            ).isoformat()
            result["intent_reused"] = True
        else:
            expected_event = candidate_event
        _validate_prior_monitor_scan_ledger(
            state,
            prior_records,
            prior_journal_raw,
            rollover_observed_at_utc=effective_observed_utc,
        )
        next_state = _build_next_state(
            state,
            target=target,
            observed_utc=effective_observed_utc,
            event_id=event_id,
            prior_reference=prior_reference,
        )
        result["prior_state_sha256"] = state_sha256
        result["policy_evaluator_sha256"] = prior_reference[
            "policy_evaluator_sha256"
        ]
        result["prior_journal_size_bytes"] = prior_reference[
            "prior_journal_size_bytes"
        ]
        result["prior_journal_sha256"] = prior_reference[
            "prior_journal_sha256"
        ]
        result["next_state_sha256"] = _value_sha256(next_state)
        if not matching and durable_intent is None and not dry_run:
            try:
                _atomic_write_rollover_intent(
                    intent_path, _build_rollover_intent(expected_event)
                )
            except OSError as exc:
                raise MonitorSessionRolloverError(
                    f"rollover_intent_write_failed:{type(exc).__name__}"
                ) from exc
            persisted_intent = _load_rollover_intent(intent_path)
            if persisted_intent != _build_rollover_intent(expected_event):
                raise MonitorSessionRolloverError(
                    "rollover_intent_postcondition_failed"
                )
            result["intent_created"] = True
        if target_partial_tail:
            expected_bytes = _canonical_json_bytes(expected_event)
            if (
                matching
                or any(
                    not _pre_rollover_record_allowed(record, target)
                    for record in records
                )
                or not expected_bytes.startswith(target_partial_tail)
            ):
                raise MonitorSessionRolloverError(
                    "target_journal_partial_tail_unrecognized"
                )
            if dry_run:
                raise MonitorSessionRolloverError(
                    "target_journal_partial_tail_requires_live_recovery"
                )
            recovered = _rollback_exact_owned_suffix(
                journal_path,
                original_prefix=target_journal_raw,
                observed_raw=target_observed_raw,
                owned_suffix=target_partial_tail,
            )
            if not recovered:
                result["commit_phase"] = "integrity_conflict"
                raise MonitorSessionRolloverError(
                    "target_journal_partial_tail_recovery_conflict"
                )
            result["journal_tail_recovered"] = True
        if matching:
            event_index = records.index(matching[0])
            if any(
                not _pre_rollover_record_allowed(record, target)
                for record in records[:event_index]
            ):
                raise MonitorSessionRolloverError(
                    "signal_record_precedes_session_rollover"
                )
            _validate_event_receipt(matching[0], expected_event)
            result["durable_event_reused"] = True
            result["commit_phase"] = "journal_durable"
        else:
            if any(not _pre_rollover_record_allowed(record, target) for record in records):
                raise MonitorSessionRolloverError(
                    "signal_record_precedes_session_rollover"
                )
            if not dry_run:
                try:
                    _append_jsonl_durable(journal_path, expected_event)
                except OSError as exc:
                    raise MonitorSessionRolloverError(
                        f"journal_append_failed:{type(exc).__name__}"
                    ) from exc
                result["journal_event_appended"] = True
                result["commit_phase"] = "journal_durable"
        if dry_run:
            result.update(
                {
                    "accepted": True,
                    "action": "would_roll_over",
                    "issues": [],
                }
            )
            return result

        expected_target_journal_raw = (
            target_journal_raw
            if matching
            else target_journal_raw + _canonical_json_bytes(expected_event)
        )
        try:
            current_target_journal_raw = (
                journal_path.read_bytes() if journal_path.exists() else b""
            )
        except OSError as exc:
            raise MonitorSessionRolloverError(
                f"target_journal_revalidation_failed:{type(exc).__name__}"
            ) from exc
        if current_target_journal_raw != expected_target_journal_raw:
            if not matching and result["journal_event_appended"]:
                owned_suffix = _canonical_json_bytes(expected_event)
                rolled_back = _rollback_exact_owned_suffix(
                    journal_path,
                    original_prefix=target_journal_raw,
                    observed_raw=current_target_journal_raw,
                    owned_suffix=owned_suffix,
                )
                if rolled_back:
                    result["journal_event_appended"] = False
                    result["commit_phase"] = "not_started"
                else:
                    result["commit_phase"] = "integrity_conflict"
            else:
                result["commit_phase"] = "integrity_conflict"
            raise MonitorSessionRolloverError(
                "target_session_journal_changed_during_rollover"
            )

        try:
            current_raw = state_path.read_bytes()
        except OSError as exc:
            raise MonitorSessionRolloverError(
                f"state_revalidation_failed:{type(exc).__name__}"
            ) from exc
        if _file_sha256(current_raw) != state_sha256:
            raise MonitorSessionRolloverError("state_changed_during_rollover")
        try:
            current_prior_journal_raw = (
                prior_journal_path.read_bytes()
                if prior_journal_path.exists()
                else b""
            )
        except OSError as exc:
            raise MonitorSessionRolloverError(
                f"prior_journal_revalidation_failed:{type(exc).__name__}"
            ) from exc
        if current_prior_journal_raw != prior_journal_raw:
            raise MonitorSessionRolloverError(
                "prior_session_journal_changed_during_rollover"
            )
        try:
            _atomic_write_json(state_path, next_state)
        except OSError as exc:
            try:
                persisted_after_error, _raw_after_error, _hash_after_error = _load_state(
                    state_path
                )
                if persisted_after_error == next_state:
                    result["state_updated"] = True
                    result["commit_phase"] = "post_state_replace_uncertain"
            except (MonitorSessionRolloverError, OSError):
                pass
            raise MonitorSessionRolloverError(
                f"state_atomic_replace_failed:{type(exc).__name__}"
            ) from exc
        # From this point onward the durable state file has changed.  Any
        # readback or postcondition failure must report that truth and require
        # an exact retry instead of claiming a pre-commit abstention.
        result["state_updated"] = True
        result["commit_phase"] = "post_state_replace_uncertain"
        if failpoint is not None:
            try:
                failpoint("after_state_replace")
            except Exception as exc:
                raise MonitorSessionRolloverError(
                    f"post_state_replace_verification_failed:{type(exc).__name__}"
                ) from exc

        persisted, _persisted_raw, persisted_sha256 = _load_state(state_path)
        if (
            persisted.get("session_date") != target
            or (persisted.get("opening_acceptance") or {}).get("session_date")
            != target
            or persisted.get("policy_evaluator") != state.get("policy_evaluator")
            or persisted_sha256 != _file_sha256(_canonical_json_bytes(next_state, pretty=True))
        ):
            raise MonitorSessionRolloverError("state_rollover_postcondition_failed")
        result.update(
            {
                "accepted": True,
                "action": (
                    "recovered_from_durable_rollover_event"
                    if result["durable_event_reused"]
                    else "rolled_over"
                ),
                "state_updated": True,
                "commit_phase": "complete",
                "persisted_state_sha256": persisted_sha256,
                "issues": [],
            }
        )
        return result

    try:
        if dry_run:
            return execute_locked()
        lock_path = state_path.with_name(f"{state_path.name}.rollover.lock")
        with _exclusive_lock(lock_path):
            return execute_locked()
    except (MonitorSessionRolloverError, ValueError, TypeError, OverflowError) as exc:
        post_replace = result.get("state_updated") is True
        journal_durable = result.get("commit_phase") == "journal_durable"
        result.update(
            {
                "accepted": False,
                "action": (
                    "retry_required" if post_replace or journal_durable else "abstain"
                ),
                "issues": [str(exc)],
            }
        )
        if post_replace:
            result["commit_phase"] = "post_state_replace_uncertain"
        return result
