"""Durable, rollover-independent owner for the 07:00 universe pre-stage gate.

The pre-stage is inspected before the main market-monitor session rolls over.
Its tiny per-session JSONL journal is therefore both the immutable receipt and
the notification outbox.  No application database or main monitor state is
read or written here.
"""

from __future__ import annotations

import copy
import hashlib
import json
import os
from datetime import date, datetime, time, timezone
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence
from zoneinfo import ZoneInfo

from app.utils.market_calendar import market_calendar_status
from backend.monitor_scan_ledger import (
    _exclusive_lock,
    _validate_notification_delivery_proof,
)


INSPECTION_SCHEMA = "marketpin-universe-prestage-inspection.v1"
EVENT_SCHEMA = "marketpin-monitor-universe-prestage-event.v1"
ACK_EVENT_SCHEMA = "marketpin-monitor-universe-prestage-ack.v1"
COMMIT_RESULT_SCHEMA = "marketpin-monitor-universe-prestage-commit.result.v1"
ACK_RESULT_SCHEMA = "marketpin-monitor-universe-prestage-ack.result.v1"
EVENT_TYPE = "universe_prestage_postcondition"
ACK_EVENT_TYPE = "universe_prestage_notification_ack"
CHECKPOINT = "07:15_CT"

_CT = ZoneInfo("America/Chicago")
_UTC = timezone.utc
_MAX_REPORT_BYTES = 128 * 1024
_ALERT_TYPES = {
    "CURRENT_DAY_UNIVERSE_PRESTAGE_FAILED",
    "CURRENT_DAY_UNIVERSE_PRESTAGE_MISSING",
    "CURRENT_DAY_UNIVERSE_PRESTAGE_UNVERIFIABLE",
}
_ALERT_REASONS = {
    "EXPLICIT_FAILURE",
    "MISSING_POSTCONDITION",
    "UNVERIFIABLE_POSTCONDITION",
}
_EXPECTED_REPORT_FIELDS = {
    "schema_version",
    "output_mode",
    "read_only",
    "session_date",
    "observed_at_utc",
    "observed_at_ct",
    "checkpoint",
    "status",
    "alert_type",
    "reason",
    "notification_required",
    "evidence",
    "issues",
}


class MonitorUniversePrestageError(RuntimeError):
    """A fail-closed validation or persistence error."""


def _canonical_json_bytes(value: Any, *, newline: bool = False) -> bytes:
    payload = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("utf-8")
    return payload + (b"\n" if newline else b"")


def _canonical_hash(value: Any) -> str:
    return hashlib.sha256(_canonical_json_bytes(value)).hexdigest()


def _is_sha256(value: Any) -> bool:
    return bool(
        isinstance(value, str)
        and len(value) == 64
        and all(character in "0123456789abcdef" for character in value)
    )


def _is_windows_powershell_action(value: Any) -> bool:
    return bool(
        isinstance(value, str)
        and value.replace("/", "\\").lower()
        == r"c:\windows\system32\windowspowershell\v1.0\powershell.exe"
    )


def _canonical_session_date(value: Any) -> str:
    if not isinstance(value, str):
        raise MonitorUniversePrestageError("session_date_invalid")
    try:
        parsed = date.fromisoformat(value)
    except ValueError as exc:
        raise MonitorUniversePrestageError("session_date_invalid") from exc
    if parsed.isoformat() != value:
        raise MonitorUniversePrestageError("session_date_not_canonical")
    return value


def _parsed_aware(value: Any, field: str) -> datetime:
    if not isinstance(value, str) or not value:
        raise MonitorUniversePrestageError(f"{field}_invalid")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise MonitorUniversePrestageError(f"{field}_invalid") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise MonitorUniversePrestageError(f"{field}_must_be_timezone_aware")
    return parsed


def _validate_ready_evidence(evidence: Mapping[str, Any]) -> None:
    task = evidence.get("task_history")
    watchdog = evidence.get("watchdog_log")
    if not isinstance(task, Mapping) or not isinstance(watchdog, Mapping):
        raise MonitorUniversePrestageError("ready_evidence_missing")
    if (
        task.get("source_status") != "OK"
        or not isinstance(task.get("trigger_instance_id"), str)
        or not task.get("trigger_instance_id")
        or task.get("trigger_event_name") != "TimeTriggerEvent"
        or not isinstance(task.get("result_code"), int)
        or isinstance(task.get("result_code"), bool)
        or task.get("result_code") != 0
        or not _is_windows_powershell_action(task.get("action_name"))
        or task.get("contradictions") not in ([], ())
    ):
        raise MonitorUniversePrestageError("ready_task_postcondition_invalid")
    ready = watchdog.get("ready")
    if not isinstance(ready, Mapping):
        raise MonitorUniversePrestageError("ready_watchdog_postcondition_missing")
    if (
        ready.get("trading_date") != evidence.get("session_date")
        or ready.get("component") != "universe"
        or ready.get("action") != "return_without_component_change"
        or ready.get("symbols") != ["SPX", "NDX", "VIX", "RUT"]
        or not isinstance(ready.get("selected_contract_count"), int)
        or isinstance(ready.get("selected_contract_count"), bool)
        or not (1 <= ready["selected_contract_count"] <= 3200)
        or not _is_sha256(ready.get("selected_universe_sha256"))
        or not isinstance(ready.get("invocation_id"), str)
        or not ready.get("invocation_id")
        or not isinstance(ready.get("powershell_pid"), int)
        or isinstance(ready.get("powershell_pid"), bool)
        or ready.get("powershell_pid") <= 0
        or watchdog.get("matched_invocation_ids") != [ready.get("invocation_id")]
    ):
        raise MonitorUniversePrestageError("ready_watchdog_postcondition_invalid")
    if watchdog.get("failure_events") not in ([], ()):
        raise MonitorUniversePrestageError("ready_evidence_contains_failure")
    if watchdog.get("contradictions") not in ([], ()):
        raise MonitorUniversePrestageError("ready_evidence_contradictory")


def _validate_evidence_shape(
    evidence: Mapping[str, Any], *, session_date: str, observed_utc: datetime
) -> None:
    task = evidence.get("task_history")
    watchdog = evidence.get("watchdog_log")
    if not isinstance(task, Mapping) or set(task) != {
        "source_status",
        "trigger_instance_id",
        "trigger_event_name",
        "trigger_time_utc",
        "task_start_time_utc",
        "task_start_user_context",
        "completion_time_utc",
        "result_code",
        "action_name",
        "matched_event_record_ids",
        "later_task_instances_ignored",
        "contradictions",
        "events_sha256",
    }:
        raise MonitorUniversePrestageError("task_history_evidence_invalid")
    if task.get("source_status") not in {"OK", "UNAVAILABLE"}:
        raise MonitorUniversePrestageError("task_history_source_status_invalid")
    instance = task.get("trigger_instance_id")
    if instance is not None and (not isinstance(instance, str) or not instance):
        raise MonitorUniversePrestageError("task_history_instance_invalid")
    result_code = task.get("result_code")
    if result_code is not None and (
        not isinstance(result_code, int) or isinstance(result_code, bool)
    ):
        raise MonitorUniversePrestageError("task_history_result_code_invalid")
    task_contradictions = task.get("contradictions")
    if (
        not isinstance(task_contradictions, list)
        or any(
            not isinstance(item, str) or not item
            for item in task_contradictions
        )
        or len(set(task_contradictions)) != len(task_contradictions)
    ):
        raise MonitorUniversePrestageError("task_history_contradictions_invalid")
    record_ids = task.get("matched_event_record_ids")
    if (
        not isinstance(record_ids, list)
        or any(not isinstance(item, int) or isinstance(item, bool) or item < 0 for item in record_ids)
        or len(set(record_ids)) != len(record_ids)
    ):
        raise MonitorUniversePrestageError("task_history_record_ids_invalid")
    ignored = task.get("later_task_instances_ignored")
    if not isinstance(ignored, int) or isinstance(ignored, bool) or ignored < 0:
        raise MonitorUniversePrestageError("task_history_ignored_count_invalid")
    events_sha = task.get("events_sha256")
    if task.get("source_status") == "OK":
        if not _is_sha256(events_sha):
            raise MonitorUniversePrestageError("task_history_hash_invalid")
    elif (
        events_sha is not None
        or instance is not None
        or result_code is not None
        or task.get("trigger_event_name") is not None
        or task.get("task_start_time_utc") is not None
        or task.get("task_start_user_context") is not None
        or task.get("action_name") is not None
        or record_ids
        or ignored != 0
        or task_contradictions
        or task.get("trigger_time_utc") is not None
        or task.get("completion_time_utc") is not None
    ):
        raise MonitorUniversePrestageError("task_history_unavailable_evidence_invalid")

    trigger_text = task.get("trigger_time_utc")
    task_start_text = task.get("task_start_time_utc")
    completion_text = task.get("completion_time_utc")
    trigger = _parsed_aware(trigger_text, "task.trigger_time_utc") if trigger_text else None
    task_start = (
        _parsed_aware(task_start_text, "task.task_start_time_utc")
        if task_start_text
        else None
    )
    completion = (
        _parsed_aware(completion_text, "task.completion_time_utc")
        if completion_text
        else None
    )
    for value, field in (
        (trigger, "trigger"),
        (task_start, "start"),
        (completion, "completion"),
    ):
        if value is not None and value.utcoffset() != _UTC.utcoffset(value):
            raise MonitorUniversePrestageError(f"task_{field}_time_must_be_utc")
    if instance is None:
        if (
            trigger is not None
            or completion is not None
            or task_start is not None
            or result_code is not None
            or task.get("trigger_event_name") is not None
            or task.get("task_start_user_context") is not None
            or task.get("action_name") is not None
            or record_ids
        ):
            raise MonitorUniversePrestageError("task_history_orphan_result")
    else:
        if (
            trigger is None
            or task.get("trigger_event_name") != "TimeTriggerEvent"
            or not record_ids
        ):
            raise MonitorUniversePrestageError("task_history_trigger_evidence_missing")
        local_trigger = trigger.astimezone(_CT)
        if (
            local_trigger.date().isoformat() != session_date
            or not (time(7, 0) <= local_trigger.time().replace(tzinfo=None) < time(7, 40))
        ):
            raise MonitorUniversePrestageError("task_history_trigger_window_invalid")
        start_valid = bool(
            task_start is not None
            and task_start >= trigger
            and task_start <= observed_utc
            and task.get("task_start_user_context") == r"NT AUTHORITY\SYSTEM"
        )
        start_contradiction = any(
            item.startswith("invalid_task_start_count:")
            or item == "task_start_user_context_mismatch"
            for item in task_contradictions
        )
        if not start_valid and not start_contradiction:
            raise MonitorUniversePrestageError("task_history_start_evidence_invalid")
        if completion is not None and (completion < trigger or completion > observed_utc):
            raise MonitorUniversePrestageError("task_history_completion_time_invalid")
        if (
            completion is not None
            and task_start is not None
            and completion < task_start
        ):
            raise MonitorUniversePrestageError("task_history_completion_precedes_start")
        if (completion is None) != (result_code is None):
            raise MonitorUniversePrestageError("task_history_completion_result_mismatch")
        if completion is None:
            if task.get("action_name") is not None:
                raise MonitorUniversePrestageError("task_history_orphan_action")
        elif (
            not _is_windows_powershell_action(task.get("action_name"))
            and "unexpected_action_name" not in task_contradictions
        ):
            raise MonitorUniversePrestageError("task_history_action_invalid")

    if not isinstance(watchdog, Mapping) or set(watchdog) != {
        "path",
        "tail_sha256",
        "tail_truncated",
        "matched_invocation_ids",
        "ready",
        "failure_events",
        "contradictions",
    }:
        raise MonitorUniversePrestageError("watchdog_evidence_invalid")
    path = watchdog.get("path")
    if (
        not isinstance(path, str)
        or not path
        or not path.replace("\\", "/").lower().endswith("/logs/runtime/watchdog.log")
        or not _is_sha256(watchdog.get("tail_sha256"))
        or type(watchdog.get("tail_truncated")) is not bool
    ):
        raise MonitorUniversePrestageError("watchdog_evidence_identity_invalid")
    invocation_ids = watchdog.get("matched_invocation_ids")
    contradictions = watchdog.get("contradictions")
    failures = watchdog.get("failure_events")
    if (
        not isinstance(invocation_ids, list)
        or any(not isinstance(item, str) or not item for item in invocation_ids)
        or len(set(invocation_ids)) != len(invocation_ids)
        or not isinstance(contradictions, list)
        or any(not isinstance(item, str) or not item for item in contradictions)
        or not isinstance(failures, list)
    ):
        raise MonitorUniversePrestageError("watchdog_evidence_collections_invalid")
    for failure in failures:
        if not isinstance(failure, Mapping) or set(failure) != {
            "timestamp_utc",
            "invocation_started_at_utc",
            "invocation_id",
            "powershell_pid",
            "event",
        }:
            raise MonitorUniversePrestageError("watchdog_failure_evidence_invalid")
        failure_time = _parsed_aware(failure.get("timestamp_utc"), "watchdog.failure_time")
        failure_started = _parsed_aware(
            failure.get("invocation_started_at_utc"),
            "watchdog.failure_started_at_utc",
        )
        if (
            failure_time.utcoffset() != _UTC.utcoffset(failure_time)
            or failure_started.utcoffset() != _UTC.utcoffset(failure_started)
            or failure_started > failure_time
            or failure_time > observed_utc
            or failure.get("invocation_id") not in invocation_ids
            or not isinstance(failure.get("powershell_pid"), int)
            or isinstance(failure.get("powershell_pid"), bool)
            or failure.get("powershell_pid") <= 0
            or failure.get("event")
            not in {"universe_current_day_prestage_failed", "invocation_failed"}
        ):
            raise MonitorUniversePrestageError("watchdog_failure_evidence_invalid")
        if trigger is not None and failure_started < trigger:
            raise MonitorUniversePrestageError("watchdog_failure_precedes_task")
        if completion is not None and failure_time > completion:
            raise MonitorUniversePrestageError("watchdog_failure_follows_task")
    ready = watchdog.get("ready")
    if ready is not None:
        if not isinstance(ready, Mapping) or set(ready) != {
            "timestamp_utc",
            "invocation_started_at_utc",
            "invocation_id",
            "powershell_pid",
            "component",
            "trading_date",
            "symbols",
            "selected_contract_count",
            "selected_universe_sha256",
            "action",
        }:
            raise MonitorUniversePrestageError("watchdog_ready_evidence_invalid")
        ready_time = _parsed_aware(ready.get("timestamp_utc"), "watchdog.ready_time")
        ready_started = _parsed_aware(
            ready.get("invocation_started_at_utc"),
            "watchdog.ready_started_at_utc",
        )
        if (
            ready_time.utcoffset() != _UTC.utcoffset(ready_time)
            or ready_started.utcoffset() != _UTC.utcoffset(ready_started)
            or ready_started > ready_time
            or ready_time > observed_utc
            or ready.get("invocation_id") not in invocation_ids
            or not isinstance(ready.get("powershell_pid"), int)
            or isinstance(ready.get("powershell_pid"), bool)
            or ready.get("powershell_pid") <= 0
            or ready.get("component") != "universe"
            or ready.get("action") != "return_without_component_change"
        ):
            raise MonitorUniversePrestageError("watchdog_ready_evidence_invalid")
        if trigger is not None and ready_started < trigger:
            raise MonitorUniversePrestageError("watchdog_ready_precedes_task")
        if completion is not None and ready_time > completion:
            raise MonitorUniversePrestageError("watchdog_ready_follows_task")


def validate_inspector_report(report: Mapping[str, Any]) -> dict[str, Any]:
    """Validate one compact read-only inspector report."""

    if not isinstance(report, Mapping):
        raise MonitorUniversePrestageError("inspector_report_must_be_an_object")
    normalized = copy.deepcopy(dict(report))
    try:
        encoded = _canonical_json_bytes(normalized)
    except (TypeError, ValueError, OverflowError) as exc:
        raise MonitorUniversePrestageError("inspector_report_not_canonical_json") from exc
    if len(encoded) > _MAX_REPORT_BYTES:
        raise MonitorUniversePrestageError("inspector_report_exceeds_size_limit")
    if set(normalized) != _EXPECTED_REPORT_FIELDS:
        raise MonitorUniversePrestageError("inspector_report_fields_invalid")
    if (
        normalized.get("schema_version") != INSPECTION_SCHEMA
        or normalized.get("output_mode") != "compact"
        or normalized.get("read_only") is not True
        or normalized.get("checkpoint") != CHECKPOINT
    ):
        raise MonitorUniversePrestageError("inspector_report_contract_invalid")

    session_date = _canonical_session_date(normalized.get("session_date"))
    calendar = market_calendar_status(date.fromisoformat(session_date))
    if calendar.get("supported") is not True:
        raise MonitorUniversePrestageError("session_market_calendar_unavailable")
    if calendar.get("market_open") is not True:
        raise MonitorUniversePrestageError("session_not_open")
    observed_utc = _parsed_aware(
        normalized.get("observed_at_utc"), "observed_at_utc"
    )
    observed_ct = _parsed_aware(normalized.get("observed_at_ct"), "observed_at_ct")
    if observed_utc.utcoffset() != _UTC.utcoffset(observed_utc):
        raise MonitorUniversePrestageError("observed_at_utc_must_be_utc")
    chicago_observed = observed_utc.astimezone(_CT)
    if (
        observed_ct.astimezone(_UTC) != observed_utc
        or observed_ct.utcoffset() != chicago_observed.utcoffset()
        or chicago_observed.date().isoformat() != session_date
    ):
        raise MonitorUniversePrestageError("inspector_timestamp_mismatch")
    if not (time(7, 15) <= chicago_observed.time().replace(tzinfo=None) < time(7, 45)):
        raise MonitorUniversePrestageError("inspector_outside_commit_window")

    evidence = normalized.get("evidence")
    issues = normalized.get("issues")
    if not isinstance(evidence, Mapping) or set(evidence) != {
        "session_date",
        "task_history",
        "watchdog_log",
    }:
        raise MonitorUniversePrestageError("inspector_evidence_invalid")
    if evidence.get("session_date") != session_date:
        raise MonitorUniversePrestageError("inspector_evidence_session_mismatch")
    _validate_evidence_shape(
        evidence,
        session_date=session_date,
        observed_utc=observed_utc,
    )
    if (
        not isinstance(issues, list)
        or any(not isinstance(issue, str) or not issue for issue in issues)
        or len(set(issues)) != len(issues)
    ):
        raise MonitorUniversePrestageError("inspector_issues_invalid")

    status = normalized.get("status")
    if status == "READY":
        if (
            normalized.get("alert_type") is not None
            or normalized.get("reason") != "EXACT_CURRENT_DAY_PRESTAGE_READY"
            or normalized.get("notification_required") is not False
            or issues
        ):
            raise MonitorUniversePrestageError("ready_inspector_result_invalid")
        _validate_ready_evidence(evidence)
    elif status == "ALERT":
        if (
            normalized.get("alert_type") not in _ALERT_TYPES
            or normalized.get("reason") not in _ALERT_REASONS
            or normalized.get("notification_required") is not True
            or not issues
        ):
            raise MonitorUniversePrestageError("alert_inspector_result_invalid")
        expected_alert = {
            "EXPLICIT_FAILURE": "CURRENT_DAY_UNIVERSE_PRESTAGE_FAILED",
            "MISSING_POSTCONDITION": "CURRENT_DAY_UNIVERSE_PRESTAGE_MISSING",
            "UNVERIFIABLE_POSTCONDITION": "CURRENT_DAY_UNIVERSE_PRESTAGE_UNVERIFIABLE",
        }[str(normalized["reason"])]
        if normalized.get("alert_type") != expected_alert:
            raise MonitorUniversePrestageError("alert_type_reason_mismatch")
        task = evidence["task_history"]
        watchdog = evidence["watchdog_log"]
        if normalized["reason"] == "EXPLICIT_FAILURE" and not (
            (
                isinstance(task.get("result_code"), int)
                and not isinstance(task.get("result_code"), bool)
                and task.get("result_code") != 0
            )
            or watchdog.get("failure_events")
        ):
            raise MonitorUniversePrestageError("explicit_failure_evidence_missing")
        if normalized["reason"] == "MISSING_POSTCONDITION" and (
            task.get("trigger_instance_id") is not None
            and task.get("result_code") == 0
            and watchdog.get("ready") is not None
        ):
            raise MonitorUniversePrestageError("missing_postcondition_evidence_invalid")
        if normalized["reason"] == "UNVERIFIABLE_POSTCONDITION" and not (
            task.get("source_status") == "UNAVAILABLE"
            or task.get("contradictions")
            or watchdog.get("contradictions")
            or watchdog.get("tail_truncated")
        ):
            raise MonitorUniversePrestageError("unverifiable_evidence_missing")
    else:
        raise MonitorUniversePrestageError("inspector_status_not_committable")
    return normalized


def prestage_event_id(session_date: str) -> str:
    session = _canonical_session_date(session_date)
    return _canonical_hash(
        {
            "schema_version": EVENT_SCHEMA,
            "event_type": EVENT_TYPE,
            "session_date": session,
            "checkpoint": CHECKPOINT,
        }
    )


def _build_event(report: Mapping[str, Any]) -> dict[str, Any]:
    normalized = validate_inspector_report(report)
    session_date = str(normalized["session_date"])
    report_sha256 = _canonical_hash(normalized)
    return {
        "schema_version": EVENT_SCHEMA,
        "event_id": prestage_event_id(session_date),
        "event_type": EVENT_TYPE,
        "session_date": session_date,
        "checkpoint": CHECKPOINT,
        # Sorted canonical JSON places this exact-request commitment near the
        # beginning of the record. Recovery is allowed only after the complete
        # marker is already durable, so a short common prefix can never be
        # reinterpreted as a different READY/ALERT report.
        "commitment_sha256": report_sha256,
        "observed_at_utc": normalized["observed_at_utc"],
        "observed_at_ct": normalized["observed_at_ct"],
        "outcome": normalized["status"],
        "alert_type": normalized["alert_type"],
        "reason": normalized["reason"],
        "notification_required": normalized["notification_required"],
        "inspector_report_sha256": report_sha256,
        "inspector_report": normalized,
    }


def _validate_event(record: Mapping[str, Any], *, session_date: str) -> dict[str, Any]:
    if not isinstance(record, Mapping):
        raise MonitorUniversePrestageError("prestage_event_invalid")
    normalized = copy.deepcopy(dict(record))
    expected_fields = {
        "schema_version",
        "event_id",
        "event_type",
        "session_date",
        "checkpoint",
        "commitment_sha256",
        "observed_at_utc",
        "observed_at_ct",
        "outcome",
        "alert_type",
        "reason",
        "notification_required",
        "inspector_report_sha256",
        "inspector_report",
    }
    if set(normalized) != expected_fields:
        raise MonitorUniversePrestageError("prestage_event_fields_invalid")
    report = validate_inspector_report(normalized.get("inspector_report"))
    expected = _build_event(report)
    if normalized != expected or normalized.get("session_date") != session_date:
        raise MonitorUniversePrestageError("prestage_event_receipt_mismatch")
    return normalized


def _ack_event_id(material: Mapping[str, Any]) -> str:
    return _canonical_hash(
        {
            "schema_version": ACK_EVENT_SCHEMA,
            "event_type": ACK_EVENT_TYPE,
            "ack": material,
        }
    )


def _build_ack(
    *,
    event: Mapping[str, Any],
    observed_at_utc: datetime,
    delivery_proof: Mapping[str, Any],
) -> dict[str, Any]:
    event_id = str(event["event_id"])
    proof = _validate_notification_delivery_proof(delivery_proof)
    observed = observed_at_utc.astimezone(_UTC)
    observed_text = observed.isoformat().replace("+00:00", "Z")
    material = {
        "session_date": event["session_date"],
        "event_ids": [event_id],
        "observed_at_utc": observed_text,
        "observed_at_ct": observed.astimezone(_CT).isoformat(),
        "delivery_proof": proof,
        "event_commitment": {
            "event_id": event_id,
            "record_sha256": _canonical_hash(event),
        },
    }
    return {
        "schema_version": ACK_EVENT_SCHEMA,
        "event_id": _ack_event_id(material),
        "event_type": ACK_EVENT_TYPE,
        **material,
    }


def _validate_ack(
    record: Mapping[str, Any], *, event: Mapping[str, Any]
) -> dict[str, Any]:
    if not isinstance(record, Mapping):
        raise MonitorUniversePrestageError("prestage_ack_invalid")
    normalized = copy.deepcopy(dict(record))
    expected_fields = {
        "schema_version",
        "event_id",
        "event_type",
        "session_date",
        "event_ids",
        "observed_at_utc",
        "observed_at_ct",
        "delivery_proof",
        "event_commitment",
    }
    if set(normalized) != expected_fields:
        raise MonitorUniversePrestageError("prestage_ack_fields_invalid")
    if (
        normalized.get("schema_version") != ACK_EVENT_SCHEMA
        or normalized.get("event_type") != ACK_EVENT_TYPE
        or normalized.get("session_date") != event.get("session_date")
        or normalized.get("event_ids") != [event.get("event_id")]
        or normalized.get("event_commitment")
        != {
            "event_id": event.get("event_id"),
            "record_sha256": _canonical_hash(event),
        }
    ):
        raise MonitorUniversePrestageError("prestage_ack_commitment_invalid")
    proof = _validate_notification_delivery_proof(normalized.get("delivery_proof"))
    observed = _parsed_aware(normalized.get("observed_at_utc"), "ack.observed_at_utc")
    observed_ct = _parsed_aware(normalized.get("observed_at_ct"), "ack.observed_at_ct")
    if observed.utcoffset() != _UTC.utcoffset(observed):
        raise MonitorUniversePrestageError("ack_observed_at_utc_must_be_utc")
    if observed_ct.astimezone(_UTC) != observed:
        raise MonitorUniversePrestageError("ack_timestamp_mismatch")
    if observed_ct.utcoffset() != observed.astimezone(_CT).utcoffset():
        raise MonitorUniversePrestageError("ack_observed_at_ct_offset_invalid")
    delivered = _parsed_aware(
        proof["prior_final_delivered_at_utc"], "ack.prior_final_delivered_at_utc"
    ).astimezone(_UTC)
    event_observed = _parsed_aware(
        event.get("observed_at_utc"), "event.observed_at_utc"
    ).astimezone(_UTC)
    if delivered < event_observed:
        raise MonitorUniversePrestageError("ack_delivery_precedes_event")
    if observed <= delivered:
        raise MonitorUniversePrestageError("acknowledgement_precedes_delivery")
    material = {key: normalized[key] for key in expected_fields - {"schema_version", "event_id", "event_type"}}
    if normalized.get("event_id") != _ack_event_id(material):
        raise MonitorUniversePrestageError("prestage_ack_event_id_mismatch")
    return normalized


def _read_journal_snapshot(path: Path) -> tuple[list[dict[str, Any]], bytes, bytes, bytes]:
    if not path.exists():
        return [], b"", b"", b""
    raw = path.read_bytes()
    last_newline = raw.rfind(b"\n")
    complete_size = last_newline + 1 if last_newline >= 0 else 0
    complete = raw[:complete_size]
    tail = raw[complete_size:]
    records: list[dict[str, Any]] = []
    for line_number, line in enumerate(complete.splitlines(), start=1):
        if not line.strip():
            raise MonitorUniversePrestageError(
                f"prestage_journal_blank_line:{line_number}"
            )
        try:
            record = json.loads(line)
        except json.JSONDecodeError as exc:
            raise MonitorUniversePrestageError(
                f"prestage_journal_json_invalid:{line_number}"
            ) from exc
        if not isinstance(record, dict):
            raise MonitorUniversePrestageError(
                f"prestage_journal_record_invalid:{line_number}"
            )
        records.append(record)
    return records, complete, tail, raw


def _replay(
    records: Sequence[Mapping[str, Any]], *, session_date: str
) -> tuple[dict[str, Any] | None, dict[str, Any] | None, list[str]]:
    if len(records) > 2:
        raise MonitorUniversePrestageError("prestage_journal_record_count_invalid")
    event: dict[str, Any] | None = None
    ack: dict[str, Any] | None = None
    if records:
        event = _validate_event(records[0], session_date=session_date)
    if len(records) == 2:
        if event is None:
            raise MonitorUniversePrestageError("prestage_ack_without_event")
        ack = _validate_ack(records[1], event=event)
    pending = (
        [str(event["event_id"])]
        if event is not None and event["notification_required"] and ack is None
        else []
    )
    return event, ack, pending


def inspect_universe_prestage_outbox(
    *, session_date: str, journal_dir: Path
) -> dict[str, Any]:
    """Read and validate the complete tiny per-session outbox."""

    session = _canonical_session_date(session_date)
    path = Path(journal_dir).resolve() / f"{session}.jsonl"
    records, _complete, tail, _raw = _read_journal_snapshot(path)
    if tail:
        raise MonitorUniversePrestageError("prestage_journal_partial_tail")
    event, ack, pending = _replay(records, session_date=session)
    return {
        "schema_version": "marketpin-monitor-universe-prestage-outbox.v1",
        "read_only": True,
        "status": "OK",
        "session_date": session,
        "journal_path": str(path),
        "event": event,
        "ack": ack,
        "pending_notification_event_ids": pending,
        "issues": [],
    }


def _append_jsonl_durable(path: Path, record: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    encoded = _canonical_json_bytes(record, newline=True)
    descriptor = os.open(
        path,
        os.O_APPEND | os.O_CREAT | os.O_WRONLY | getattr(os, "O_BINARY", 0),
        0o600,
    )
    original_size = os.fstat(descriptor).st_size
    written = 0
    try:
        view = memoryview(encoded)
        while view:
            count = os.write(descriptor, view)
            if count <= 0:
                raise OSError("prestage journal write made no progress")
            written += count
            view = view[count:]
        os.fsync(descriptor)
    except BaseException:
        try:
            if os.fstat(descriptor).st_size == original_size + written:
                os.ftruncate(descriptor, original_size)
                os.fsync(descriptor)
        finally:
            os.close(descriptor)
        raise
    os.close(descriptor)


def _truncate_exact_tail(path: Path, *, expected_raw: bytes, complete_size: int) -> None:
    descriptor = os.open(path, os.O_RDWR | getattr(os, "O_BINARY", 0))
    try:
        current = bytearray()
        size = os.fstat(descriptor).st_size
        while len(current) < size:
            chunk = os.read(descriptor, size - len(current))
            if not chunk:
                break
            current.extend(chunk)
        if bytes(current) != expected_raw:
            raise MonitorUniversePrestageError("prestage_journal_changed_during_recovery")
        os.ftruncate(descriptor, complete_size)
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _partial_prefix_contains_marker(
    tail: bytes, *, expected: bytes, marker: bytes
) -> bool:
    """Return true only for an exact prefix that already binds the request."""

    marker_end = expected.find(marker)
    if marker_end < 0:
        raise MonitorUniversePrestageError("prestage_recovery_marker_missing")
    marker_end += len(marker)
    return len(tail) >= marker_end and expected.startswith(tail)


def _commit_base(report: Any = None) -> dict[str, Any]:
    return {
        "schema_version": COMMIT_RESULT_SCHEMA,
        "accepted": False,
        "action": "abstain",
        "session_date": report.get("session_date") if isinstance(report, Mapping) else None,
        "event_id": None,
        "event_appended": False,
        "partial_journal_tail_recovered": False,
        "pending_notification_event_ids": None,
        "notification_required": False,
        "commit_phase": "not_started",
        "issues": [],
    }


def commit_universe_prestage(
    *,
    inspector_report: Mapping[str, Any],
    journal_dir: Path,
    failpoint: Callable[[str], None] | None = None,
) -> dict[str, Any]:
    """Append one immutable checkpoint and return its replayed outbox."""

    result = _commit_base(inspector_report)
    try:
        expected = _build_event(inspector_report)
        session = str(expected["session_date"])
        result.update(
            {
                "session_date": session,
                "event_id": expected["event_id"],
                "notification_required": bool(expected["notification_required"]),
            }
        )
        root = Path(journal_dir).resolve()
        path = root / f"{session}.jsonl"
        result["journal_path"] = str(path)
        with _exclusive_lock(root / f"{session}.lock"):
            records, complete, tail, full_raw = _read_journal_snapshot(path)
            if tail:
                if records:
                    raise MonitorUniversePrestageError("prestage_journal_partial_tail")
                expected_bytes = _canonical_json_bytes(expected, newline=True)
                recovery_marker = (
                    f'"commitment_sha256":"{expected["commitment_sha256"]}"'
                ).encode("ascii")
                if not _partial_prefix_contains_marker(
                    tail,
                    expected=expected_bytes,
                    marker=recovery_marker,
                ):
                    raise MonitorUniversePrestageError(
                        "prestage_journal_unrecognized_partial_tail"
                    )
                _truncate_exact_tail(
                    path, expected_raw=full_raw, complete_size=len(complete)
                )
                result["partial_journal_tail_recovered"] = True
                records = []
            event, _ack, pending = _replay(records, session_date=session)
            if event is not None:
                result.update(
                    {
                        "accepted": True,
                        "action": (
                            "notification_pending" if pending else "already_committed"
                        ),
                        "event_id": event["event_id"],
                        "notification_required": bool(event["notification_required"]),
                        "pending_notification_event_ids": pending,
                        "commit_phase": "complete",
                        "issues": [],
                    }
                )
                return result

            _append_jsonl_durable(path, expected)
            result["event_appended"] = True
            result["commit_phase"] = "journal_durable"
            if failpoint is not None:
                failpoint("after_event_append")
            confirmed_records, _complete, confirmed_tail, _raw = _read_journal_snapshot(path)
            if confirmed_tail:
                raise MonitorUniversePrestageError("prestage_journal_partial_tail_after_append")
            confirmed_event, _confirmed_ack, confirmed_pending = _replay(
                confirmed_records, session_date=session
            )
            if confirmed_event != expected:
                raise MonitorUniversePrestageError("prestage_event_postcondition_failed")
            result.update(
                {
                    "accepted": True,
                    "action": "committed",
                    "pending_notification_event_ids": confirmed_pending,
                    "commit_phase": "complete",
                    "issues": [],
                }
            )
            return result
    except Exception as exc:
        if isinstance(exc, (KeyboardInterrupt, SystemExit)):
            raise
        result["issues"] = [str(exc)]
        if result.get("commit_phase") == "journal_durable":
            result["action"] = "retry_required"
        elif "journal" in str(exc) or "receipt" in str(exc):
            result["action"] = "integrity_conflict"
            result["commit_phase"] = "integrity_conflict"
        return result


def _ack_base(session_date: Any = None, event_ids: Any = None) -> dict[str, Any]:
    return {
        "schema_version": ACK_RESULT_SCHEMA,
        "accepted": False,
        "action": "abstain",
        "session_date": session_date,
        "ack_event_id": None,
        "ack_appended": False,
        "partial_journal_tail_recovered": False,
        "requested_event_ids": list(event_ids) if isinstance(event_ids, list) else [],
        "event_ids_acknowledged": [],
        "pending_notification_event_ids": None,
        "commit_phase": "not_started",
        "issues": [],
    }


def ack_universe_prestage_notifications(
    *,
    session_date: str,
    event_ids: Sequence[str],
    observed_at_utc: datetime,
    delivery_proof: Mapping[str, Any],
    journal_dir: Path,
    failpoint: Callable[[str], None] | None = None,
) -> dict[str, Any]:
    """Append a causal prior-turn delivery ACK for the exact pending event."""

    result = _ack_base(
        session_date,
        list(event_ids) if isinstance(event_ids, Sequence) and not isinstance(event_ids, (str, bytes)) else [],
    )
    try:
        session = _canonical_session_date(session_date)
        if (
            isinstance(event_ids, (str, bytes))
            or not isinstance(event_ids, Sequence)
            or len(event_ids) != 1
            or not _is_sha256(event_ids[0])
        ):
            raise MonitorUniversePrestageError("event_ids_invalid")
        if (
            not isinstance(observed_at_utc, datetime)
            or observed_at_utc.tzinfo is None
            or observed_at_utc.utcoffset() is None
        ):
            raise MonitorUniversePrestageError("observed_at_utc_invalid")
        proof = _validate_notification_delivery_proof(delivery_proof)
        root = Path(journal_dir).resolve()
        path = root / f"{session}.jsonl"
        result["journal_path"] = str(path)
        with _exclusive_lock(root / f"{session}.lock"):
            records, complete, tail, full_raw = _read_journal_snapshot(path)
            if not records:
                raise MonitorUniversePrestageError("prestage_event_missing")
            event, existing_ack, pending = _replay(records, session_date=session)
            if event is None:
                raise MonitorUniversePrestageError("prestage_event_missing")
            expected_ack = _build_ack(
                event=event,
                observed_at_utc=observed_at_utc,
                delivery_proof=proof,
            )
            if tail:
                if existing_ack is not None:
                    raise MonitorUniversePrestageError("prestage_journal_partial_tail")
                expected_bytes = _canonical_json_bytes(expected_ack, newline=True)
                recovery_marker = (
                    f'"event_id":"{expected_ack["event_id"]}"'
                ).encode("ascii")
                if not _partial_prefix_contains_marker(
                    tail,
                    expected=expected_bytes,
                    marker=recovery_marker,
                ):
                    raise MonitorUniversePrestageError(
                        "prestage_journal_unrecognized_partial_tail"
                    )
                _truncate_exact_tail(
                    path, expected_raw=full_raw, complete_size=len(complete)
                )
                result["partial_journal_tail_recovered"] = True
                records = records[:1]
                existing_ack = None
                pending = [str(event["event_id"])] if event["notification_required"] else []
            if existing_ack is not None:
                if existing_ack != expected_ack:
                    raise MonitorUniversePrestageError(
                        "prestage_ack_already_committed_with_different_request"
                    )
                result.update(
                    {
                        "accepted": True,
                        "action": "already_acknowledged",
                        "ack_event_id": existing_ack["event_id"],
                        "event_ids_acknowledged": list(event_ids),
                        "pending_notification_event_ids": [],
                        "commit_phase": "complete",
                        "issues": [],
                    }
                )
                return result
            if pending != list(event_ids):
                raise MonitorUniversePrestageError("requested_event_ids_not_pending")

            _validate_ack(expected_ack, event=event)
            _append_jsonl_durable(path, expected_ack)
            result.update(
                {
                    "ack_event_id": expected_ack["event_id"],
                    "ack_appended": True,
                    "commit_phase": "journal_durable",
                }
            )
            if failpoint is not None:
                failpoint("after_ack_append")
            confirmed_records, _complete, confirmed_tail, _raw = _read_journal_snapshot(path)
            if confirmed_tail:
                raise MonitorUniversePrestageError("prestage_journal_partial_tail_after_ack")
            _event, confirmed_ack, confirmed_pending = _replay(
                confirmed_records, session_date=session
            )
            if confirmed_ack != expected_ack or confirmed_pending:
                raise MonitorUniversePrestageError("prestage_ack_postcondition_failed")
            result.update(
                {
                    "accepted": True,
                    "action": "acknowledged",
                    "event_ids_acknowledged": list(event_ids),
                    "pending_notification_event_ids": [],
                    "commit_phase": "complete",
                    "issues": [],
                }
            )
            return result
    except Exception as exc:
        if isinstance(exc, (KeyboardInterrupt, SystemExit)):
            raise
        result["issues"] = [str(exc)]
        if result.get("commit_phase") == "journal_durable":
            result["action"] = "retry_required"
        elif "journal" in str(exc) or "receipt" in str(exc):
            result["action"] = "integrity_conflict"
            result["commit_phase"] = "integrity_conflict"
        return result


__all__ = [
    "ACK_EVENT_SCHEMA",
    "ACK_RESULT_SCHEMA",
    "COMMIT_RESULT_SCHEMA",
    "EVENT_SCHEMA",
    "EVENT_TYPE",
    "INSPECTION_SCHEMA",
    "MonitorUniversePrestageError",
    "ack_universe_prestage_notifications",
    "commit_universe_prestage",
    "inspect_universe_prestage_outbox",
    "prestage_event_id",
    "validate_inspector_report",
]
