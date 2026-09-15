"""Read-only 07:00 current-day universe pre-stage postcondition inspector."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import subprocess
import sys
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence
from uuid import UUID
from zoneinfo import ZoneInfo


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.utils.market_calendar import market_calendar_status  # noqa: E402
from backend.monitor_universe_prestage import (  # noqa: E402
    CHECKPOINT,
    INSPECTION_SCHEMA,
)


CT = ZoneInfo("America/Chicago")
UTC = timezone.utc
TASK_NAME = r"\MarketPinPredictor_AutoStart"
EXPECTED_SYMBOLS = ["SPX", "NDX", "VIX", "RUT"]
MAX_LOG_TAIL_BYTES = 4 * 1024 * 1024
_LOG_PATTERN = re.compile(
    r"^(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} [+-]\d{2}:\d{2}) "
    r"invocation=(?P<invocation>[A-Za-z0-9_.:@-]+) "
    r"caller=(?P<caller>[A-Za-z0-9_.:@-]+) "
    r"powershell_pid=(?P<pid>\d+) event=(?P<event>[A-Za-z0-9_.:@-]+)"
    r"(?: (?P<details>.*))?$"
)


def _canonical_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("utf-8")


def _canonical_date(value: str) -> date:
    parsed = date.fromisoformat(value)
    if parsed.isoformat() != value:
        raise ValueError("session date must be canonical YYYY-MM-DD")
    return parsed


def _aware_utc(value: str | None) -> datetime:
    if value is None:
        return datetime.now(UTC)
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("observed-at-utc must include an offset")
    return parsed.astimezone(UTC)


def _task_event_time(event: Mapping[str, Any]) -> datetime | None:
    value = event.get("time_created_utc")
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return None
    return parsed.astimezone(UTC)


def _normalized_instance(value: Any) -> str | None:
    if not isinstance(value, str) or not value.strip():
        return None
    raw = value.strip().strip("{}").lower()
    try:
        parsed = str(UUID(raw))
    except ValueError:
        return None
    return parsed if raw == parsed else None


def _load_task_events_windows(
    *, start_utc: datetime, end_utc: datetime
) -> list[dict[str, Any]]:
    if os.name != "nt":
        raise OSError("Task Scheduler Operational history is Windows-only")
    # Get-WinEvent interprets FilterHashtable DateTime ticks as local wall time
    # even when the object carries Utc Kind.  Supply explicit Chicago wall
    # times with Unspecified Kind, then normalize returned records to UTC.
    start_text = start_utc.astimezone(CT).strftime("%Y-%m-%dT%H:%M:%S.%f")
    end_text = end_utc.astimezone(CT).strftime("%Y-%m-%dT%H:%M:%S.%f")
    script = rf"""
$ErrorActionPreference = 'Stop'
$start = [datetime]::SpecifyKind([datetime]::Parse('{start_text}'), [DateTimeKind]::Unspecified)
$end = [datetime]::SpecifyKind([datetime]::Parse('{end_text}'), [DateTimeKind]::Unspecified)
$records = @(Get-WinEvent -FilterHashtable @{{
    LogName='Microsoft-Windows-TaskScheduler/Operational'
    StartTime=$start
    EndTime=$end
    Id=100,107,201
}} -ErrorAction Stop | ForEach-Object {{
    [xml]$xml = $_.ToXml()
    $data = @{{}}
    foreach ($node in @($xml.Event.EventData.Data)) {{
        $data[[string]$node.Name] = [string]$node.'#text'
    }}
    [pscustomobject]@{{
        event_id = [int]$_.Id
        event_data_name = [string]$xml.Event.EventData.Name
        record_id = [long]$_.RecordId
        time_created_utc = $_.TimeCreated.ToUniversalTime().ToString('o')
        activity_id = [string]$xml.Event.System.Correlation.ActivityID
        data = $data
    }}
}})
ConvertTo-Json -InputObject $records -Depth 6 -Compress
"""
    completed = subprocess.run(
        [
            os.path.join(
                os.environ.get("SystemRoot", r"C:\Windows"),
                "System32",
                "WindowsPowerShell",
                "v1.0",
                "powershell.exe",
            ),
            "-NoProfile",
            "-NonInteractive",
            "-ExecutionPolicy",
            "Bypass",
            "-Command",
            script,
        ],
        check=False,
        capture_output=True,
        text=True,
        timeout=8,
    )
    if completed.returncode != 0:
        message = " ".join((completed.stderr or completed.stdout).split())[:300]
        raise OSError(f"Task Scheduler history query failed: {message}")
    decoded = json.loads(completed.stdout or "[]")
    if not isinstance(decoded, list):
        raise ValueError("Task Scheduler history output was not an array")
    return [dict(event) for event in decoded if isinstance(event, Mapping)]


def _inspect_task_history(
    *,
    session_day: date,
    observed_utc: datetime,
    events: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    # A time trigger may arrive late when StartWhenAvailable catches up after a
    # wake, but it must never precede the configured 07:00 wall-clock trigger.
    session_start_ct = datetime.combine(session_day, time(7, 0), CT)
    trigger_before_ct = datetime.combine(session_day, time(7, 40), CT)
    selected_events = []
    for event in events:
        data = event.get("data")
        event_time = _task_event_time(event)
        if not isinstance(data, Mapping) or event_time is None:
            continue
        if data.get("TaskName") != TASK_NAME or event_time > observed_utc:
            continue
        selected_events.append(dict(event))
    selected_events.sort(
        key=lambda event: (
            _task_event_time(event) or datetime.min.replace(tzinfo=UTC),
            int(event.get("record_id") or 0),
        )
    )

    trigger_candidates = []
    task_contradictions: list[str] = []
    later_task_instances: set[str] = set()
    for event in selected_events:
        event_time = _task_event_time(event)
        data = event.get("data") or {}
        if (
            int(event.get("event_id") or 0) == 107
            and event.get("event_data_name") == "TimeTriggerEvent"
        ):
            local_time = event_time.astimezone(CT)
            instance = _normalized_instance(data.get("InstanceId"))
            activity = _normalized_instance(event.get("activity_id"))
            if instance is None or activity != instance:
                if session_start_ct <= local_time < trigger_before_ct:
                    task_contradictions.append("invalid_time_trigger_identity")
                continue
            if session_start_ct <= local_time < trigger_before_ct:
                trigger_candidates.append((event_time, instance, event))
            elif local_time.date() == session_day and local_time >= trigger_before_ct:
                later_task_instances.add(instance)
    trigger_candidates.sort(key=lambda item: item[0])
    if not trigger_candidates:
        return {
            "source_status": "OK",
            "trigger_instance_id": None,
            "trigger_event_name": None,
            "trigger_time_utc": None,
            "completion_time_utc": None,
            "result_code": None,
            "action_name": None,
            "matched_event_record_ids": [],
            "later_task_instances_ignored": len(later_task_instances),
            "task_start_time_utc": None,
            "task_start_user_context": None,
            "contradictions": sorted(set(task_contradictions)),
            "events_sha256": hashlib.sha256(_canonical_bytes(selected_events)).hexdigest(),
        }

    trigger_time, instance, trigger = trigger_candidates[0]
    retry_boundary_ct = datetime.combine(session_day, time(7, 15), CT)
    primary_local_time = trigger_time.astimezone(CT)
    if primary_local_time >= retry_boundary_ct:
        # Event 107 does not identify which registered calendar trigger fired.
        # Once the 07:15 retry window begins, a lone/earliest event cannot prove
        # that the authoritative 07:00 attempt occurred, so fail closed.
        task_contradictions.append("autostart_0700_trigger_not_proven")
    expected_retry = None
    if len(trigger_candidates) == 2:
        retry_time, retry_instance, _retry_trigger = trigger_candidates[1]
        retry_local_time = retry_time.astimezone(CT)
        if (
            primary_local_time < retry_boundary_ct <= retry_local_time
            and retry_instance != instance
        ):
            # Infer the bounded 07:15 retry window from a distinct Event 107;
            # exact registered-trigger integrity is validated separately. Keep
            # the pre-07:15 instance authoritative while retaining the retry in
            # the source hash and ignored-instance count. A later retry must
            # never mask an explicit result from the first attempt.
            expected_retry = retry_instance
            later_task_instances.add(retry_instance)
    if len(trigger_candidates) > 2 or (
        len(trigger_candidates) == 2 and expected_retry is None
    ):
        task_contradictions.append(
            f"multiple_0700_time_trigger_instances:{len(trigger_candidates)}"
        )
    task_starts = []
    for event in selected_events:
        if (
            int(event.get("event_id") or 0) != 100
            or event.get("event_data_name") != "TaskStartEvent"
        ):
            continue
        data = event.get("data") or {}
        if _normalized_instance(data.get("InstanceId")) != instance:
            continue
        event_time = _task_event_time(event)
        if _normalized_instance(event.get("activity_id")) != instance:
            task_contradictions.append("task_start_activity_mismatch")
            continue
        if event_time is not None and event_time >= trigger_time:
            task_starts.append((event_time, event))
    task_starts.sort(key=lambda item: item[0])
    if len(task_starts) != 1:
        task_contradictions.append(f"invalid_task_start_count:{len(task_starts)}")
    task_start_time: datetime | None = None
    task_start_user_context: str | None = None
    task_start_record_ids: list[int] = []
    if task_starts:
        task_start_time, task_start = task_starts[0]
        task_start_user_context = (task_start.get("data") or {}).get("UserContext")
        task_start_record_ids = [
            int(item[1].get("record_id") or 0) for item in task_starts
        ]
        if task_start_user_context != r"NT AUTHORITY\SYSTEM":
            task_contradictions.append("task_start_user_context_mismatch")

    completions = []
    for event in selected_events:
        if (
            int(event.get("event_id") or 0) != 201
            or event.get("event_data_name") != "ActionSuccess"
        ):
            continue
        data = event.get("data") or {}
        if _normalized_instance(data.get("TaskInstanceId")) == instance:
            event_time = _task_event_time(event)
            if _normalized_instance(event.get("activity_id")) != instance:
                task_contradictions.append("task_completion_activity_mismatch")
                continue
            if event_time is not None and event_time >= trigger_time:
                completions.append((event_time, event))
    completions.sort(key=lambda item: item[0])
    completion_time: datetime | None = None
    result_code: int | None = None
    action_name: str | None = None
    completion_record_ids: list[int] = []
    if completions:
        parsed_completions: list[tuple[datetime, Mapping[str, Any], int | None]] = []
        for candidate_time, candidate in completions:
            completion_record_ids.append(int(candidate.get("record_id") or 0))
            try:
                candidate_code = int(
                    (candidate.get("data") or {}).get("ResultCode")
                )
            except (TypeError, ValueError):
                candidate_code = None
            parsed_completions.append((candidate_time, candidate, candidate_code))
        # Never let a later zero result mask an earlier failure tied to the
        # same scheduled-task instance. Missing/malformed results likewise do
        # not get promoted to success by a later completion.
        selected_completion = next(
            (item for item in parsed_completions if item[2] not in (None, 0)),
            next(
                (item for item in parsed_completions if item[2] is None),
                parsed_completions[0],
            ),
        )
        completion_time, completion, result_code = selected_completion
        raw_action = (completion.get("data") or {}).get("ActionName")
        action_name = raw_action if isinstance(raw_action, str) else None
        normalized_action = (action_name or "").replace("/", "\\").lower()
        if normalized_action != (
            r"c:\windows\system32\windowspowershell\v1.0\powershell.exe"
        ):
            task_contradictions.append("unexpected_action_name")
    matched_ids = [int(trigger.get("record_id") or 0)]
    matched_ids.extend(task_start_record_ids)
    matched_ids.extend(completion_record_ids)
    return {
        "source_status": "OK",
        "trigger_instance_id": instance,
        "trigger_event_name": "TimeTriggerEvent",
        "trigger_time_utc": trigger_time.isoformat().replace("+00:00", "Z"),
        "task_start_time_utc": (
            task_start_time.isoformat().replace("+00:00", "Z")
            if task_start_time is not None
            else None
        ),
        "task_start_user_context": task_start_user_context,
        "completion_time_utc": (
            completion_time.isoformat().replace("+00:00", "Z")
            if completion_time is not None
            else None
        ),
        "result_code": result_code,
        "action_name": action_name,
        "matched_event_record_ids": matched_ids,
        "later_task_instances_ignored": len(later_task_instances),
        "contradictions": sorted(set(task_contradictions)),
        "events_sha256": hashlib.sha256(_canonical_bytes(selected_events)).hexdigest(),
    }


def _read_log_tail(path: Path) -> tuple[bytes, bool]:
    if not path.is_file():
        return b"", False
    size = path.stat().st_size
    with path.open("rb") as handle:
        if size > MAX_LOG_TAIL_BYTES:
            handle.seek(size - MAX_LOG_TAIL_BYTES)
            raw = handle.read()
            newline = raw.find(b"\n")
            return (raw[newline + 1 :] if newline >= 0 else b""), True
        return handle.read(), False


def _detail_tokens(value: str | None) -> dict[str, str]:
    tokens: dict[str, str] = {}
    for token in (value or "").split():
        if "=" not in token:
            continue
        key, item = token.split("=", 1)
        if key and key not in tokens:
            tokens[key] = item
    return tokens


def _inspect_watchdog_log(
    *,
    path: Path,
    session_day: date,
    task: Mapping[str, Any],
    observed_utc: datetime,
) -> dict[str, Any]:
    raw, truncated = _read_log_tail(path)
    tail_sha256 = hashlib.sha256(raw).hexdigest()
    trigger_value = task.get("trigger_time_utc")
    completion_value = task.get("completion_time_utc")
    trigger_time = (
        datetime.fromisoformat(str(trigger_value).replace("Z", "+00:00"))
        if trigger_value
        else datetime.combine(session_day, time(7, 0), CT).astimezone(UTC)
    )
    end_time = min(
        observed_utc,
        (
            datetime.fromisoformat(str(completion_value).replace("Z", "+00:00"))
            if completion_value
            else trigger_time + timedelta(minutes=10)
        )
        + timedelta(seconds=2),
    )
    groups: dict[str, list[dict[str, Any]]] = {}
    malformed_relevant = 0
    for raw_line in raw.decode("utf-8", errors="replace").splitlines():
        if "caller=ScheduledTaskUniversePrestage" not in raw_line:
            continue
        if not raw_line.startswith(session_day.isoformat()):
            continue
        match = _LOG_PATTERN.match(raw_line.strip())
        if match is None:
            malformed_relevant += 1
            continue
        if match.group("caller") != "ScheduledTaskUniversePrestage":
            continue
        try:
            source_timestamp = datetime.strptime(
                match.group("timestamp"), "%Y-%m-%d %H:%M:%S %z"
            )
        except ValueError:
            malformed_relevant += 1
            continue
        expected_offset = datetime.combine(session_day, time(7, 0), CT).utcoffset()
        if source_timestamp.utcoffset() != expected_offset:
            malformed_relevant += 1
            continue
        timestamp = source_timestamp.astimezone(UTC)
        if timestamp < trigger_time - timedelta(seconds=2) or timestamp > end_time:
            continue
        record = {
            "timestamp_utc": timestamp.isoformat().replace("+00:00", "Z"),
            "invocation_id": match.group("invocation"),
            "powershell_pid": int(match.group("pid")),
            "event": match.group("event"),
            "details": _detail_tokens(match.group("details")),
        }
        groups.setdefault(match.group("invocation"), []).append(record)

    ready_records: list[dict[str, Any]] = []
    failure_records: list[dict[str, Any]] = []
    contradictions: list[str] = []
    matched_ids: list[str] = []
    for invocation, records in sorted(groups.items()):
        starts = [
            record
            for record in records
            if record["event"] == "invocation_started"
            and record["details"].get("prepare_universe_only") == "True"
        ]
        ready = [
            record
            for record in records
            if record["event"] == "universe_current_day_prestage_ready"
        ]
        failures = [
            record
            for record in records
            if record["event"]
            in {"universe_current_day_prestage_failed", "invocation_failed"}
        ]
        if len(starts) != 1:
            if ready or failures or len(starts) > 1:
                contradictions.append(
                    f"invalid_start_count:{invocation}:{len(starts)}"
                )
            continue
        start = starts[0]
        started = True
        if started:
            matched_ids.append(invocation)
        if ready and failures:
            contradictions.append(f"ready_and_failed_same_invocation:{invocation}")
        for record in failures:
            if (
                record["powershell_pid"] != start["powershell_pid"]
                or record["timestamp_utc"] < start["timestamp_utc"]
            ):
                contradictions.append(f"invalid_failure_causality:{invocation}")
                continue
            failure_records.append(
                {
                    "timestamp_utc": record["timestamp_utc"],
                    "invocation_started_at_utc": start["timestamp_utc"],
                    "invocation_id": invocation,
                    "powershell_pid": record["powershell_pid"],
                    "event": record["event"],
                }
            )
        for record in ready:
            details = record["details"]
            try:
                count = int(details.get("selected_contract_count", ""))
            except ValueError:
                count = 0
            symbols = details.get("symbols", "").split(",")
            selected_hash = details.get("selected_hash")
            valid = bool(
                started
                and record["powershell_pid"] == start["powershell_pid"]
                and record["timestamp_utc"] >= start["timestamp_utc"]
                and details.get("component") == "universe"
                and details.get("trading_date") == session_day.isoformat()
                and symbols == EXPECTED_SYMBOLS
                and 1 <= count <= 3200
                and isinstance(selected_hash, str)
                and re.fullmatch(r"[0-9a-f]{64}", selected_hash)
                and details.get("action") == "return_without_component_change"
            )
            if not valid:
                contradictions.append(f"invalid_ready_receipt:{invocation}")
                continue
            ready_records.append(
                {
                    "timestamp_utc": record["timestamp_utc"],
                    "invocation_started_at_utc": start["timestamp_utc"],
                    "invocation_id": invocation,
                    "powershell_pid": record["powershell_pid"],
                    "component": "universe",
                    "trading_date": session_day.isoformat(),
                    "symbols": symbols,
                    "selected_contract_count": count,
                    "selected_universe_sha256": selected_hash,
                    "action": "return_without_component_change",
                }
            )
    if len(ready_records) > 1:
        contradictions.append("multiple_valid_ready_receipts")
    if malformed_relevant:
        contradictions.append(f"malformed_relevant_log_lines:{malformed_relevant}")
    if len(matched_ids) > 1:
        contradictions.append(f"multiple_started_invocations:{len(matched_ids)}")
    return {
        "path": str(path.resolve()),
        "tail_sha256": tail_sha256,
        "tail_truncated": truncated,
        "matched_invocation_ids": matched_ids,
        "ready": ready_records[0] if len(ready_records) == 1 else None,
        "failure_events": failure_records,
        "contradictions": contradictions,
    }


def inspect_universe_prestage(
    *,
    project_root: Path,
    session_date: str,
    observed_at_utc: datetime | None = None,
    task_events_loader: Callable[..., Sequence[Mapping[str, Any]]] | None = None,
) -> dict[str, Any]:
    """Return a compact provider-free postcondition report."""

    session_day = _canonical_date(session_date)
    if observed_at_utc is not None and (
        observed_at_utc.tzinfo is None or observed_at_utc.utcoffset() is None
    ):
        raise ValueError("observed_at_utc must be timezone-aware")
    observed = (observed_at_utc or datetime.now(UTC)).astimezone(UTC)
    observed_ct = observed.astimezone(CT)
    calendar = market_calendar_status(session_day)
    base = {
        "schema_version": INSPECTION_SCHEMA,
        "output_mode": "compact",
        "read_only": True,
        "session_date": session_day.isoformat(),
        "observed_at_utc": observed.isoformat().replace("+00:00", "Z"),
        "observed_at_ct": observed_ct.isoformat(),
        "checkpoint": CHECKPOINT,
        "status": "NOT_DUE",
        "alert_type": None,
        "reason": "CHECKPOINT_NOT_DUE",
        "notification_required": False,
        "evidence": {
            "session_date": session_day.isoformat(),
            "task_history": {},
            "watchdog_log": {},
        },
        "issues": [],
    }
    if calendar.get("supported") is not True:
        base.update(
            {
                "status": "ALERT",
                "alert_type": "CURRENT_DAY_UNIVERSE_PRESTAGE_UNVERIFIABLE",
                "reason": "UNVERIFIABLE_POSTCONDITION",
                "notification_required": True,
                "issues": ["MARKET_CALENDAR_UNAVAILABLE"],
            }
        )
        return base
    if calendar.get("market_open") is not True:
        base.update({"status": "NOT_APPLICABLE", "reason": "NON_OPEN_SESSION"})
        return base
    if observed_ct.date() != session_day:
        raise ValueError("observed_at_utc does not match session_date in Chicago")
    if observed_ct.time().replace(tzinfo=None) < time(7, 15):
        return base

    start_utc = datetime.combine(session_day, time(6, 55), CT).astimezone(UTC)
    loader = task_events_loader or _load_task_events_windows
    task_error: str | None = None
    try:
        loaded = loader(start_utc=start_utc, end_utc=observed)
        task = _inspect_task_history(
            session_day=session_day,
            observed_utc=observed,
            events=loaded,
        )
    except Exception as exc:
        task_error = f"{type(exc).__name__}:{' '.join(str(exc).split())[:240]}"
        task = {
            "source_status": "UNAVAILABLE",
            "trigger_instance_id": None,
            "trigger_event_name": None,
            "trigger_time_utc": None,
            "task_start_time_utc": None,
            "task_start_user_context": None,
            "completion_time_utc": None,
            "result_code": None,
            "action_name": None,
            "matched_event_record_ids": [],
            "later_task_instances_ignored": 0,
            "contradictions": [],
            "events_sha256": None,
        }
    watchdog = _inspect_watchdog_log(
        path=Path(project_root).resolve() / "logs" / "runtime" / "watchdog.log",
        session_day=session_day,
        task=task,
        observed_utc=observed,
    )
    base["evidence"] = {
        "session_date": session_day.isoformat(),
        "task_history": task,
        "watchdog_log": watchdog,
    }

    issues: list[str] = []
    if task_error is not None:
        issues.append("TASK_HISTORY_UNAVAILABLE")
    if watchdog["tail_truncated"] and not watchdog["matched_invocation_ids"]:
        issues.append("WATCHDOG_LOG_REQUIRED_WINDOW_TRUNCATED")
    if watchdog["contradictions"]:
        issues.append("WATCHDOG_PRESTAGE_EVIDENCE_CONTRADICTORY")
    if task.get("contradictions"):
        issues.append("TASK_HISTORY_EVIDENCE_CONTRADICTORY")
    if task.get("result_code") not in (None, 0) or watchdog["failure_events"]:
        issues.append("UNIVERSE_PRESTAGE_EXPLICIT_FAILURE")
    if task.get("trigger_instance_id") is None:
        issues.append("AUTOSTART_0700_TASK_INSTANCE_MISSING")
    elif task.get("result_code") is None:
        issues.append("AUTOSTART_0700_TASK_COMPLETION_MISSING")
    if watchdog["ready"] is None:
        issues.append("UNIVERSE_PRESTAGE_READY_RECEIPT_MISSING")

    if (
        not issues
        and task.get("source_status") == "OK"
        and task.get("result_code") == 0
        and watchdog["ready"] is not None
    ):
        base.update(
            {
                "status": "READY",
                "reason": "EXACT_CURRENT_DAY_PRESTAGE_READY",
                "issues": [],
            }
        )
        return base

    if "UNIVERSE_PRESTAGE_EXPLICIT_FAILURE" in issues:
        alert_type = "CURRENT_DAY_UNIVERSE_PRESTAGE_FAILED"
        reason = "EXPLICIT_FAILURE"
    elif (
        task_error is not None
        or task.get("contradictions")
        or watchdog["contradictions"]
    ):
        alert_type = "CURRENT_DAY_UNIVERSE_PRESTAGE_UNVERIFIABLE"
        reason = "UNVERIFIABLE_POSTCONDITION"
    else:
        alert_type = "CURRENT_DAY_UNIVERSE_PRESTAGE_MISSING"
        reason = "MISSING_POSTCONDITION"
    base.update(
        {
            "status": "ALERT",
            "alert_type": alert_type,
            "reason": reason,
            "notification_required": True,
            "issues": sorted(set(issues)),
        }
    )
    return base


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("--project-root", type=Path, default=PROJECT_ROOT)
    parser.add_argument("--session-date", required=True)
    parser.add_argument("--observed-at-utc")
    parser.add_argument("--pretty", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        report = inspect_universe_prestage(
            project_root=args.project_root,
            session_date=args.session_date,
            observed_at_utc=_aware_utc(args.observed_at_utc),
        )
    except (OSError, ValueError, TypeError, OverflowError) as exc:
        report = {
            "schema_version": INSPECTION_SCHEMA,
            "output_mode": "compact",
            "read_only": True,
            "session_date": args.session_date,
            "status": "ERROR",
            "notification_required": False,
            "issues": [f"input_error:{type(exc).__name__}"],
        }
    print(
        json.dumps(
            report,
            sort_keys=True,
            indent=2 if args.pretty else None,
            separators=None if args.pretty else (",", ":"),
        )
    )
    return 0 if report.get("status") in {"READY", "ALERT", "NOT_DUE", "NOT_APPLICABLE"} else 2


if __name__ == "__main__":
    raise SystemExit(main())
