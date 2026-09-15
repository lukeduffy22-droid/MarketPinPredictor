from __future__ import annotations

import json
import importlib
from datetime import datetime, timezone
from pathlib import Path

import pytest

import backend.monitor_universe_prestage as owner
from backend.monitor_universe_prestage import (
    ack_universe_prestage_notifications,
    commit_universe_prestage,
    inspect_universe_prestage_outbox,
    prestage_event_id,
)
from tools.inspect_universe_prestage import inspect_universe_prestage


inspector_module = importlib.import_module("tools.inspect_universe_prestage")


UTC = timezone.utc
SESSION = "2026-09-09"
OBSERVED = datetime.fromisoformat("2026-09-09T12:15:00+00:00")
INSTANCE = "11111111-2222-3333-4444-555555555555"
RETRY_INSTANCE = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
UNIVERSE_HASH = "a" * 64


def _event(
    event_id: int,
    timestamp: str,
    *,
    instance: str = INSTANCE,
    result_code: int | None = None,
    record_id: int,
) -> dict:
    data = {
        "TaskName": r"\MarketPinPredictor_AutoStart",
        ("TaskInstanceId" if event_id == 201 else "InstanceId"): "{" + instance + "}",
    }
    if result_code is not None:
        data["ResultCode"] = str(result_code)
    if event_id == 201:
        data["ActionName"] = (
            r"C:\WINDOWS\system32\WindowsPowerShell\v1.0\powershell.exe"
        )
    if event_id == 100:
        data["UserContext"] = r"NT AUTHORITY\SYSTEM"
    return {
        "event_id": event_id,
        "event_data_name": {
            100: "TaskStartEvent",
            107: "TimeTriggerEvent",
            201: "ActionSuccess",
        }[event_id],
        "record_id": record_id,
        "time_created_utc": timestamp,
        "activity_id": "{" + instance + "}",
        "data": data,
    }


def _events(result_code: int = 0) -> list[dict]:
    return [
        _event(107, "2026-09-09T12:00:00Z", record_id=10),
        _event(100, "2026-09-09T12:00:01Z", record_id=11),
        _event(
            201,
            "2026-09-09T12:00:07Z",
            result_code=result_code,
            record_id=12,
        ),
    ]


def _retry_events(result_code: int = 0) -> list[dict]:
    return [
        _event(
            107,
            "2026-09-09T12:15:00Z",
            instance=RETRY_INSTANCE,
            record_id=20,
        ),
        _event(
            100,
            "2026-09-09T12:15:01Z",
            instance=RETRY_INSTANCE,
            record_id=21,
        ),
        _event(
            201,
            "2026-09-09T12:15:07Z",
            instance=RETRY_INSTANCE,
            result_code=result_code,
            record_id=22,
        ),
    ]


def _write_log(root: Path, lines: list[str]) -> None:
    path = root / "logs" / "runtime" / "watchdog.log"
    path.parent.mkdir(parents=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _ready_lines(*, include_failure: bool = False) -> list[str]:
    lines = [
        "2026-09-09 07:00:01 -05:00 invocation=abc123 "
        "caller=ScheduledTaskUniversePrestage powershell_pid=100 "
        "event=invocation_started restart_backend=False prepare_universe_only=True",
        "2026-09-09 07:00:05 -05:00 invocation=abc123 "
        "caller=ScheduledTaskUniversePrestage powershell_pid=100 "
        "event=universe_current_day_prestage_ready component=universe "
        "trading_date=2026-09-09 symbols=SPX,NDX,VIX,RUT "
        f"selected_contract_count=2400 selected_hash={UNIVERSE_HASH} "
        "action=return_without_component_change",
    ]
    if include_failure:
        lines.append(
            "2026-09-09 07:00:06 -05:00 invocation=abc123 "
            "caller=ScheduledTaskUniversePrestage powershell_pid=100 "
            "event=universe_current_day_prestage_failed component=universe"
        )
    return lines


def _retry_ready_lines() -> list[str]:
    return [
        "2026-09-09 07:15:01 -05:00 invocation=retryonly "
        "caller=ScheduledTaskUniversePrestage powershell_pid=102 "
        "event=invocation_started restart_backend=False prepare_universe_only=True",
        "2026-09-09 07:15:05 -05:00 invocation=retryonly "
        "caller=ScheduledTaskUniversePrestage powershell_pid=102 "
        "event=universe_current_day_prestage_ready component=universe "
        "trading_date=2026-09-09 symbols=SPX,NDX,VIX,RUT "
        f"selected_contract_count=2400 selected_hash={UNIVERSE_HASH} "
        "action=return_without_component_change",
    ]


def _loader(events):
    def load(**_kwargs):
        return events

    return load


def _ready_report(tmp_path: Path) -> dict:
    _write_log(tmp_path, _ready_lines())
    report = inspect_universe_prestage(
        project_root=tmp_path,
        session_date=SESSION,
        observed_at_utc=OBSERVED,
        task_events_loader=_loader(_events()),
    )
    assert report["status"] == "READY"
    return report


def _alert_report(tmp_path: Path) -> dict:
    report = inspect_universe_prestage(
        project_root=tmp_path,
        session_date=SESSION,
        observed_at_utc=OBSERVED,
        task_events_loader=_loader(_events()),
    )
    assert report["status"] == "ALERT"
    return report


def _proof(delivered_at: str = "2026-09-09T12:16:00Z") -> dict:
    return {
        "proof_type": "prior_heartbeat_final_delivered",
        "conversation_history_sha256": "b" * 64,
        "prior_final_delivered_at_utc": delivered_at,
    }


def test_inspector_requires_task_and_same_invocation_ready_receipt(tmp_path):
    report = _ready_report(tmp_path)

    assert report["notification_required"] is False
    assert report["issues"] == []
    assert report["evidence"]["task_history"]["result_code"] == 0
    assert report["evidence"]["watchdog_log"]["ready"][
        "selected_universe_sha256"
    ] == UNIVERSE_HASH


def test_inspector_treats_configured_0715_retry_as_expected_not_contradictory(
    tmp_path,
):
    _write_log(tmp_path, _ready_lines())

    report = inspect_universe_prestage(
        project_root=tmp_path,
        session_date=SESSION,
        observed_at_utc=datetime.fromisoformat("2026-09-09T12:30:00+00:00"),
        task_events_loader=_loader(_events() + _retry_events()),
    )

    task = report["evidence"]["task_history"]
    assert report["status"] == "READY"
    assert task["trigger_instance_id"] == INSTANCE
    assert task["result_code"] == 0
    assert task["later_task_instances_ignored"] == 1
    assert task["contradictions"] == []


def test_inspector_preserves_0700_failure_before_valid_0715_retry(tmp_path):
    _write_log(
        tmp_path,
        [
            "2026-09-09 07:00:01 -05:00 invocation=failed0700 "
            "caller=ScheduledTaskUniversePrestage powershell_pid=101 "
            "event=invocation_started prepare_universe_only=True",
            "2026-09-09 07:00:06 -05:00 invocation=failed0700 "
            "caller=ScheduledTaskUniversePrestage powershell_pid=101 "
            "event=universe_current_day_prestage_failed component=universe",
            "2026-09-09 07:15:01 -05:00 invocation=ready0715 "
            "caller=ScheduledTaskUniversePrestage powershell_pid=102 "
            "event=invocation_started prepare_universe_only=True",
            "2026-09-09 07:15:05 -05:00 invocation=ready0715 "
            "caller=ScheduledTaskUniversePrestage powershell_pid=102 "
            "event=universe_current_day_prestage_ready component=universe "
            "trading_date=2026-09-09 symbols=SPX,NDX,VIX,RUT "
            f"selected_contract_count=2400 selected_hash={UNIVERSE_HASH} "
            "action=return_without_component_change",
        ],
    )

    report = inspect_universe_prestage(
        project_root=tmp_path,
        session_date=SESSION,
        observed_at_utc=datetime.fromisoformat("2026-09-09T12:30:00+00:00"),
        task_events_loader=_loader(_events(result_code=1) + _retry_events()),
    )

    task = report["evidence"]["task_history"]
    assert report["status"] == "ALERT"
    assert report["alert_type"] == "CURRENT_DAY_UNIVERSE_PRESTAGE_FAILED"
    assert task["trigger_instance_id"] == INSTANCE
    assert task["result_code"] == 1
    assert task["later_task_instances_ignored"] == 1
    assert task["contradictions"] == []


def test_inspector_rejects_lone_0715_instance_as_unproven_0700(tmp_path):
    _write_log(tmp_path, _retry_ready_lines())

    report = inspect_universe_prestage(
        project_root=tmp_path,
        session_date=SESSION,
        observed_at_utc=datetime.fromisoformat("2026-09-09T12:30:00+00:00"),
        task_events_loader=_loader(_retry_events()),
    )

    task = report["evidence"]["task_history"]
    assert report["status"] == "ALERT"
    assert report["alert_type"] == "CURRENT_DAY_UNIVERSE_PRESTAGE_UNVERIFIABLE"
    assert "autostart_0700_trigger_not_proven" in task["contradictions"]


def test_inspector_rejects_0715_retry_reusing_primary_instance(tmp_path):
    _write_log(tmp_path, _ready_lines())
    duplicate_trigger = _event(
        107,
        "2026-09-09T12:15:00Z",
        instance=INSTANCE,
        record_id=20,
    )

    report = inspect_universe_prestage(
        project_root=tmp_path,
        session_date=SESSION,
        observed_at_utc=datetime.fromisoformat("2026-09-09T12:30:00+00:00"),
        task_events_loader=_loader(_events() + [duplicate_trigger]),
    )

    task = report["evidence"]["task_history"]
    assert report["alert_type"] == "CURRENT_DAY_UNIVERSE_PRESTAGE_UNVERIFIABLE"
    assert task["later_task_instances_ignored"] == 0
    assert "multiple_0700_time_trigger_instances:2" in task["contradictions"]


def test_inspector_rejects_two_candidates_entirely_in_retry_window(tmp_path):
    _write_log(tmp_path, _retry_ready_lines())
    later_retry = _event(
        107,
        "2026-09-09T12:39:00Z",
        instance="bbbbbbbb-cccc-dddd-eeee-ffffffffffff",
        record_id=30,
    )

    report = inspect_universe_prestage(
        project_root=tmp_path,
        session_date=SESSION,
        observed_at_utc=datetime.fromisoformat("2026-09-09T12:40:00+00:00"),
        task_events_loader=_loader(_retry_events() + [later_retry]),
    )

    task = report["evidence"]["task_history"]
    assert report["alert_type"] == "CURRENT_DAY_UNIVERSE_PRESTAGE_UNVERIFIABLE"
    assert "autostart_0700_trigger_not_proven" in task["contradictions"]
    assert "multiple_0700_time_trigger_instances:2" in task["contradictions"]


def test_inspector_rejects_second_candidate_before_retry_boundary(tmp_path):
    _write_log(tmp_path, _ready_lines())
    early_second = _event(
        107,
        "2026-09-09T12:14:59Z",
        instance=RETRY_INSTANCE,
        record_id=20,
    )

    report = inspect_universe_prestage(
        project_root=tmp_path,
        session_date=SESSION,
        observed_at_utc=datetime.fromisoformat("2026-09-09T12:30:00+00:00"),
        task_events_loader=_loader(_events() + [early_second]),
    )

    task = report["evidence"]["task_history"]
    assert report["alert_type"] == "CURRENT_DAY_UNIVERSE_PRESTAGE_UNVERIFIABLE"
    assert task["later_task_instances_ignored"] == 0
    assert "multiple_0700_time_trigger_instances:2" in task["contradictions"]


def test_inspector_accepts_distinct_retry_at_fractional_end_of_window(tmp_path):
    _write_log(tmp_path, _ready_lines())
    delayed_retry = _event(
        107,
        "2026-09-09T12:39:59.999999Z",
        instance=RETRY_INSTANCE,
        record_id=20,
    )

    report = inspect_universe_prestage(
        project_root=tmp_path,
        session_date=SESSION,
        observed_at_utc=datetime.fromisoformat("2026-09-09T12:40:00+00:00"),
        task_events_loader=_loader(_events() + [delayed_retry]),
    )

    task = report["evidence"]["task_history"]
    assert report["status"] == "READY"
    assert task["later_task_instances_ignored"] == 1
    assert task["contradictions"] == []


def test_inspector_treats_exact_0740_trigger_as_later_instance(tmp_path):
    _write_log(tmp_path, _ready_lines())
    after_window = _event(
        107,
        "2026-09-09T12:40:00Z",
        instance=RETRY_INSTANCE,
        record_id=20,
    )

    report = inspect_universe_prestage(
        project_root=tmp_path,
        session_date=SESSION,
        observed_at_utc=datetime.fromisoformat("2026-09-09T12:40:01+00:00"),
        task_events_loader=_loader(_events() + [after_window]),
    )

    task = report["evidence"]["task_history"]
    assert report["status"] == "READY"
    assert task["later_task_instances_ignored"] == 1
    assert task["contradictions"] == []


def test_inspector_rejects_zero_task_result_without_ready_receipt(tmp_path):
    report = _alert_report(tmp_path)

    assert report["alert_type"] == "CURRENT_DAY_UNIVERSE_PRESTAGE_MISSING"
    assert "UNIVERSE_PRESTAGE_READY_RECEIPT_MISSING" in report["issues"]


def test_inspector_failure_precedence_ignores_later_success(tmp_path):
    later_instance = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
    events = _events(result_code=1) + [
        _event(
            107,
            "2026-09-09T12:45:00Z",
            instance=later_instance,
            record_id=20,
        ),
        _event(
            201,
            "2026-09-09T12:45:05Z",
            instance=later_instance,
            result_code=0,
            record_id=21,
        ),
    ]
    _write_log(
        tmp_path,
        [
            "2026-09-09 07:00:03 -05:00 invocation=failed0700 "
            "caller=ScheduledTaskUniversePrestage powershell_pid=101 "
            "event=invocation_started prepare_universe_only=True",
            "2026-09-09 07:00:06 -05:00 invocation=failed0700 "
            "caller=ScheduledTaskUniversePrestage powershell_pid=101 "
            "event=universe_current_day_prestage_failed component=universe",
            *_ready_lines(),
        ],
    )

    report = inspect_universe_prestage(
        project_root=tmp_path,
        session_date=SESSION,
        observed_at_utc=datetime.fromisoformat("2026-09-09T13:30:00+00:00"),
        task_events_loader=_loader(events),
    )

    assert report["status"] == "ALERT"
    assert report["alert_type"] == "CURRENT_DAY_UNIVERSE_PRESTAGE_FAILED"
    assert report["evidence"]["task_history"]["result_code"] == 1
    assert report["evidence"]["task_history"]["later_task_instances_ignored"] == 1


def test_inspector_contradictory_same_invocation_fails_closed(tmp_path):
    _write_log(tmp_path, _ready_lines(include_failure=True))

    report = inspect_universe_prestage(
        project_root=tmp_path,
        session_date=SESSION,
        observed_at_utc=OBSERVED,
        task_events_loader=_loader(_events()),
    )

    assert report["status"] == "ALERT"
    assert "WATCHDOG_PRESTAGE_EVIDENCE_CONTRADICTORY" in report["issues"]
    assert "UNIVERSE_PRESTAGE_EXPLICIT_FAILURE" in report["issues"]


def test_inspector_requires_exact_prestage_caller_token(tmp_path):
    _write_log(
        tmp_path,
        [
            line.replace(
                "caller=ScheduledTaskUniversePrestage ",
                "caller=ScheduledTaskUniversePrestageEvil ",
            )
            for line in _ready_lines()
        ],
    )

    report = inspect_universe_prestage(
        project_root=tmp_path,
        session_date=SESSION,
        observed_at_utc=OBSERVED,
        task_events_loader=_loader(_events()),
    )

    assert report["status"] == "ALERT"
    assert report["alert_type"] == "CURRENT_DAY_UNIVERSE_PRESTAGE_MISSING"
    assert report["evidence"]["watchdog_log"]["matched_invocation_ids"] == []
    assert report["evidence"]["watchdog_log"]["ready"] is None


def test_inspector_same_instance_failure_cannot_be_masked_by_later_zero(tmp_path):
    events = _events(result_code=1) + [
        _event(
            201,
            "2026-09-09T12:00:08Z",
            result_code=0,
            record_id=13,
        )
    ]
    _write_log(tmp_path, _ready_lines())

    report = inspect_universe_prestage(
        project_root=tmp_path,
        session_date=SESSION,
        observed_at_utc=OBSERVED,
        task_events_loader=_loader(events),
    )

    assert report["status"] == "ALERT"
    assert report["alert_type"] == "CURRENT_DAY_UNIVERSE_PRESTAGE_FAILED"
    assert report["evidence"]["task_history"]["result_code"] == 1
    assert report["evidence"]["task_history"]["matched_event_record_ids"] == [
        10,
        11,
        12,
        13,
    ]


def test_inspector_non_0700_trigger_cannot_mask_0700_failure(tmp_path):
    early_instance = "99999999-2222-3333-4444-555555555555"
    events = [
        _event(
            107,
            "2026-09-09T11:55:00Z",
            instance=early_instance,
            record_id=1,
        ),
        _event(
            201,
            "2026-09-09T11:55:05Z",
            instance=early_instance,
            result_code=0,
            record_id=2,
        ),
        *_events(result_code=1),
    ]
    _write_log(tmp_path, _ready_lines())

    report = inspect_universe_prestage(
        project_root=tmp_path,
        session_date=SESSION,
        observed_at_utc=OBSERVED,
        task_events_loader=_loader(events),
    )

    assert report["status"] == "ALERT"
    assert report["alert_type"] == "CURRENT_DAY_UNIVERSE_PRESTAGE_FAILED"
    assert report["evidence"]["task_history"]["trigger_instance_id"] == INSTANCE
    assert report["evidence"]["task_history"]["result_code"] == 1


@pytest.mark.parametrize(
    "defect",
    [
        "non_guid_trigger",
        "trigger_activity_mismatch",
        "task_start_missing",
        "completion_activity_mismatch",
        "wrong_action_path",
    ],
)
def test_inspector_rejects_incoherent_task_instance_chain(tmp_path, defect):
    events = _events()
    if defect == "non_guid_trigger":
        events[0]["data"]["InstanceId"] = "{not-a-guid}"
        events[0]["activity_id"] = "{not-a-guid}"
    elif defect == "trigger_activity_mismatch":
        events[0]["activity_id"] = "{aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee}"
    elif defect == "task_start_missing":
        events.pop(1)
    elif defect == "completion_activity_mismatch":
        events[-1]["activity_id"] = "{aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee}"
    elif defect == "wrong_action_path":
        events[-1]["data"]["ActionName"] = (
            r"C:\evil\WindowsPowerShell\v1.0\powershell.exe"
        )
    _write_log(tmp_path, _ready_lines())

    report = inspect_universe_prestage(
        project_root=tmp_path,
        session_date=SESSION,
        observed_at_utc=OBSERVED,
        task_events_loader=_loader(events),
    )

    assert report["status"] == "ALERT"
    assert report["alert_type"] == "CURRENT_DAY_UNIVERSE_PRESTAGE_UNVERIFIABLE"
    assert "TASK_HISTORY_EVIDENCE_CONTRADICTORY" in report["issues"]
    result = commit_universe_prestage(
        inspector_report=report,
        journal_dir=tmp_path / "outbox",
    )
    assert result["accepted"] is True
    assert result["pending_notification_event_ids"] == [prestage_event_id(SESSION)]


@pytest.mark.parametrize(
    "lines",
    [
        [
            "2026-09-09 07:00:05 -05:00 invocation=abc123 "
            "caller=ScheduledTaskUniversePrestage powershell_pid=100 "
            "event=invocation_started restart_backend=False "
            "prepare_universe_only=True",
            "2026-09-09 07:00:01 -05:00 invocation=abc123 "
            "caller=ScheduledTaskUniversePrestage powershell_pid=100 "
            "event=universe_current_day_prestage_ready component=universe "
            "trading_date=2026-09-09 symbols=SPX,NDX,VIX,RUT "
            f"selected_contract_count=2400 selected_hash={UNIVERSE_HASH} "
            "action=return_without_component_change",
        ],
        [
            "2026-09-09 07:00:01 -05:00 invocation=abc123 "
            "caller=ScheduledTaskUniversePrestage powershell_pid=100 "
            "event=invocation_started restart_backend=False "
            "prepare_universe_only=True",
            "2026-09-09 07:00:05 -05:00 invocation=abc123 "
            "caller=ScheduledTaskUniversePrestage powershell_pid=999 "
            "event=universe_current_day_prestage_ready component=universe "
            "trading_date=2026-09-09 symbols=SPX,NDX,VIX,RUT "
            f"selected_contract_count=2400 selected_hash={UNIVERSE_HASH} "
            "action=return_without_component_change",
        ],
    ],
)
def test_inspector_rejects_noncausal_or_cross_process_ready(tmp_path, lines):
    _write_log(tmp_path, lines)

    report = inspect_universe_prestage(
        project_root=tmp_path,
        session_date=SESSION,
        observed_at_utc=OBSERVED,
        task_events_loader=_loader(_events()),
    )

    assert report["status"] == "ALERT"
    assert report["alert_type"] == "CURRENT_DAY_UNIVERSE_PRESTAGE_UNVERIFIABLE"
    assert "WATCHDOG_PRESTAGE_EVIDENCE_CONTRADICTORY" in report["issues"]
    assert report["evidence"]["watchdog_log"]["ready"] is None


def test_inspector_rejects_non_chicago_log_offset(tmp_path):
    _write_log(
        tmp_path,
        [line.replace(" -05:00 ", " +10:00 ") for line in _ready_lines()],
    )

    report = inspect_universe_prestage(
        project_root=tmp_path,
        session_date=SESSION,
        observed_at_utc=OBSERVED,
        task_events_loader=_loader(_events()),
    )

    assert report["status"] == "ALERT"
    assert report["alert_type"] == "CURRENT_DAY_UNIVERSE_PRESTAGE_UNVERIFIABLE"
    assert "WATCHDOG_PRESTAGE_EVIDENCE_CONTRADICTORY" in report["issues"]


def test_inspector_rejects_naive_or_wrong_session_observation(tmp_path):
    with pytest.raises(ValueError, match="timezone-aware"):
        inspect_universe_prestage(
            project_root=tmp_path,
            session_date=SESSION,
            observed_at_utc=datetime(2026, 9, 9, 12, 15),
            task_events_loader=_loader(_events()),
        )
    with pytest.raises(ValueError, match="does not match session_date"):
        inspect_universe_prestage(
            project_root=tmp_path,
            session_date=SESSION,
            observed_at_utc=datetime.fromisoformat("2026-09-10T12:15:00+00:00"),
            task_events_loader=_loader(_events()),
        )


def test_inspector_does_not_trust_truthy_calendar_flags(tmp_path, monkeypatch):
    monkeypatch.setattr(
        inspector_module,
        "market_calendar_status",
        lambda _day: {"supported": "true", "market_open": "true"},
    )

    report = inspect_universe_prestage(
        project_root=tmp_path,
        session_date=SESSION,
        observed_at_utc=OBSERVED,
        task_events_loader=lambda **_kwargs: pytest.fail("loader must not run"),
    )

    assert report["status"] == "ALERT"
    assert report["alert_type"] == "CURRENT_DAY_UNIVERSE_PRESTAGE_UNVERIFIABLE"
    assert report["issues"] == ["MARKET_CALENDAR_UNAVAILABLE"]


def test_inspector_is_quiet_before_checkpoint(tmp_path):
    report = inspect_universe_prestage(
        project_root=tmp_path,
        session_date=SESSION,
        observed_at_utc=datetime.fromisoformat("2026-09-09T12:14:59+00:00"),
        task_events_loader=lambda **_kwargs: pytest.fail("loader must not run"),
    )

    assert report["status"] == "NOT_DUE"
    assert report["notification_required"] is False


def test_inspector_turns_unavailable_task_history_into_committable_alert(tmp_path):
    def unavailable(**_kwargs):
        raise OSError("history disabled")

    report = inspect_universe_prestage(
        project_root=tmp_path,
        session_date=SESSION,
        observed_at_utc=OBSERVED,
        task_events_loader=unavailable,
    )
    result = commit_universe_prestage(
        inspector_report=report,
        journal_dir=tmp_path / "outbox",
    )

    assert report["alert_type"] == "CURRENT_DAY_UNIVERSE_PRESTAGE_UNVERIFIABLE"
    assert result["accepted"] is True
    assert result["pending_notification_event_ids"] == [prestage_event_id(SESSION)]


def test_alert_commit_is_durable_pending_and_idempotent(tmp_path):
    report = _alert_report(tmp_path)
    journal_dir = tmp_path / "outbox"

    first = commit_universe_prestage(
        inspector_report=report,
        journal_dir=journal_dir,
    )
    second = commit_universe_prestage(
        inspector_report=report,
        journal_dir=journal_dir,
    )
    replay = inspect_universe_prestage_outbox(
        session_date=SESSION,
        journal_dir=journal_dir,
    )

    assert replay["read_only"] is True
    assert replay["status"] == "OK"
    assert replay["issues"] == []
    assert first["accepted"] is True
    assert first["action"] == "committed"
    assert first["pending_notification_event_ids"] == [prestage_event_id(SESSION)]
    assert second["accepted"] is True
    assert second["action"] == "notification_pending"
    assert replay["pending_notification_event_ids"] == [prestage_event_id(SESSION)]
    assert len((journal_dir / f"{SESSION}.jsonl").read_text("utf-8").splitlines()) == 1


def test_ready_commit_is_durable_without_notification(tmp_path):
    report = _ready_report(tmp_path)
    journal_dir = tmp_path / "outbox"

    result = commit_universe_prestage(
        inspector_report=report,
        journal_dir=journal_dir,
    )

    assert result["accepted"] is True
    assert result["notification_required"] is False
    assert result["pending_notification_event_ids"] == []


def test_owner_rejects_boolean_task_result_as_forged_ready_evidence(tmp_path):
    report = _ready_report(tmp_path)
    report["evidence"]["task_history"]["result_code"] = False

    result = commit_universe_prestage(
        inspector_report=report,
        journal_dir=tmp_path / "outbox",
    )

    assert result["accepted"] is False
    assert "task_history_result_code_invalid" in result["issues"]


def test_owner_rejects_fully_shifted_weekend_ready_report(tmp_path):
    report = _ready_report(tmp_path)
    report["session_date"] = "2026-09-12"
    report["observed_at_utc"] = "2026-09-12T12:15:00Z"
    report["observed_at_ct"] = "2026-09-12T07:15:00-05:00"
    evidence = report["evidence"]
    evidence["session_date"] = "2026-09-12"
    task = evidence["task_history"]
    task["trigger_time_utc"] = "2026-09-12T12:00:00Z"
    task["completion_time_utc"] = "2026-09-12T12:00:07Z"
    ready = evidence["watchdog_log"]["ready"]
    ready["timestamp_utc"] = "2026-09-12T12:00:05Z"
    ready["trading_date"] = "2026-09-12"

    result = commit_universe_prestage(
        inspector_report=report,
        journal_dir=tmp_path / "outbox",
    )

    assert result["accepted"] is False
    assert result["issues"] == ["session_not_open"]
    assert not (tmp_path / "outbox" / "2026-09-12.jsonl").exists()


def test_owner_rejects_ready_evidence_that_precedes_task_trigger(tmp_path):
    report = _ready_report(tmp_path)
    ready = report["evidence"]["watchdog_log"]["ready"]
    ready["invocation_started_at_utc"] = "2026-09-09T10:59:59Z"
    ready["timestamp_utc"] = "2026-09-09T11:00:00Z"

    result = commit_universe_prestage(
        inspector_report=report,
        journal_dir=tmp_path / "outbox",
    )

    assert result["accepted"] is False
    assert result["issues"] == ["watchdog_ready_precedes_task"]


def test_first_checkpoint_receipt_is_immutable_across_later_observation(tmp_path):
    first_report = _alert_report(tmp_path)
    journal_dir = tmp_path / "outbox"
    first = commit_universe_prestage(
        inspector_report=first_report,
        journal_dir=journal_dir,
    )
    before = (journal_dir / f"{SESSION}.jsonl").read_bytes()
    _write_log(tmp_path, _ready_lines())
    later_ready = inspect_universe_prestage(
        project_root=tmp_path,
        session_date=SESSION,
        observed_at_utc=datetime.fromisoformat("2026-09-09T12:30:00+00:00"),
        task_events_loader=_loader(_events()),
    )

    repeated = commit_universe_prestage(
        inspector_report=later_ready,
        journal_dir=journal_dir,
    )

    assert first["notification_required"] is True
    assert later_ready["status"] == "READY"
    assert repeated["action"] == "notification_pending"
    assert repeated["notification_required"] is True
    assert (journal_dir / f"{SESSION}.jsonl").read_bytes() == before


def test_event_append_failpoint_recovers_by_replay(tmp_path):
    report = _alert_report(tmp_path)
    journal_dir = tmp_path / "outbox"

    def crash(_name):
        raise RuntimeError("simulated crash")

    interrupted = commit_universe_prestage(
        inspector_report=report,
        journal_dir=journal_dir,
        failpoint=crash,
    )
    recovered = commit_universe_prestage(
        inspector_report=report,
        journal_dir=journal_dir,
    )

    assert interrupted["accepted"] is False
    assert interrupted["action"] == "retry_required"
    assert recovered["accepted"] is True
    assert recovered["action"] == "notification_pending"
    assert recovered["pending_notification_event_ids"] == [prestage_event_id(SESSION)]


def test_exact_partial_event_prefix_is_recovered(tmp_path):
    report = _alert_report(tmp_path)
    journal_dir = tmp_path / "outbox"
    journal_dir.mkdir()
    expected = owner._build_event(report)
    encoded = owner._canonical_json_bytes(expected, newline=True)
    (journal_dir / f"{SESSION}.jsonl").write_bytes(encoded[: len(encoded) // 2])

    result = commit_universe_prestage(
        inspector_report=report,
        journal_dir=journal_dir,
    )

    assert result["accepted"] is True
    assert result["partial_journal_tail_recovered"] is True
    assert (journal_dir / f"{SESSION}.jsonl").read_bytes() == encoded


def test_short_torn_alert_prefix_cannot_be_replaced_by_ready(tmp_path):
    alert_report = _alert_report(tmp_path)
    journal_dir = tmp_path / "outbox"
    journal_dir.mkdir()
    alert_encoded = owner._canonical_json_bytes(
        owner._build_event(alert_report),
        newline=True,
    )
    path = journal_dir / f"{SESSION}.jsonl"
    torn = alert_encoded[:14]
    path.write_bytes(torn)
    _write_log(tmp_path, _ready_lines())
    ready_report = inspect_universe_prestage(
        project_root=tmp_path,
        session_date=SESSION,
        observed_at_utc=OBSERVED,
        task_events_loader=_loader(_events()),
    )

    result = commit_universe_prestage(
        inspector_report=ready_report,
        journal_dir=journal_dir,
    )

    assert result["accepted"] is False
    assert result["action"] == "integrity_conflict"
    assert result["issues"] == ["prestage_journal_unrecognized_partial_tail"]
    assert path.read_bytes() == torn


def test_unrecognized_partial_tail_is_never_truncated(tmp_path):
    report = _alert_report(tmp_path)
    journal_dir = tmp_path / "outbox"
    journal_dir.mkdir()
    path = journal_dir / f"{SESSION}.jsonl"
    original = b'{"unowned":true'
    path.write_bytes(original)

    result = commit_universe_prestage(
        inspector_report=report,
        journal_dir=journal_dir,
    )

    assert result["accepted"] is False
    assert result["action"] == "integrity_conflict"
    assert path.read_bytes() == original


def test_pending_outbox_is_independent_of_main_monitor_rollover(tmp_path):
    report = _alert_report(tmp_path)
    journal_dir = tmp_path / "outbox"
    commit_universe_prestage(inspector_report=report, journal_dir=journal_dir)
    main_state = tmp_path / "state.json"
    main_state.write_text(json.dumps({"session_date": "2026-09-08"}), encoding="utf-8")
    main_state.write_text(json.dumps({"session_date": SESSION}), encoding="utf-8")

    replay = inspect_universe_prestage_outbox(
        session_date=SESSION,
        journal_dir=journal_dir,
    )

    assert replay["pending_notification_event_ids"] == [prestage_event_id(SESSION)]


def test_ack_requires_causal_prior_turn_delivery_and_is_idempotent(tmp_path):
    report = _alert_report(tmp_path)
    journal_dir = tmp_path / "outbox"
    commit_universe_prestage(inspector_report=report, journal_dir=journal_dir)
    event_id = prestage_event_id(SESSION)

    rejected = ack_universe_prestage_notifications(
        session_date=SESSION,
        event_ids=[event_id],
        observed_at_utc=datetime.fromisoformat("2026-09-09T12:17:00+00:00"),
        delivery_proof=_proof("2026-09-09T12:14:59Z"),
        journal_dir=journal_dir,
    )
    accepted = ack_universe_prestage_notifications(
        session_date=SESSION,
        event_ids=[event_id],
        observed_at_utc=datetime.fromisoformat("2026-09-09T12:17:00+00:00"),
        delivery_proof=_proof(),
        journal_dir=journal_dir,
    )
    repeated = ack_universe_prestage_notifications(
        session_date=SESSION,
        event_ids=[event_id],
        observed_at_utc=datetime.fromisoformat("2026-09-09T12:17:00+00:00"),
        delivery_proof=_proof(),
        journal_dir=journal_dir,
    )

    assert rejected["accepted"] is False
    assert rejected["pending_notification_event_ids"] is None
    assert accepted["accepted"] is True
    assert accepted["action"] == "acknowledged"
    assert accepted["pending_notification_event_ids"] == []
    assert repeated["accepted"] is True
    assert repeated["action"] == "already_acknowledged"


def test_ack_append_failpoint_recovers_exact_receipt(tmp_path):
    report = _alert_report(tmp_path)
    journal_dir = tmp_path / "outbox"
    commit_universe_prestage(inspector_report=report, journal_dir=journal_dir)
    event_id = prestage_event_id(SESSION)

    def crash(_name):
        raise RuntimeError("simulated ack crash")

    request = {
        "session_date": SESSION,
        "event_ids": [event_id],
        "observed_at_utc": datetime.fromisoformat("2026-09-09T12:17:00+00:00"),
        "delivery_proof": _proof(),
        "journal_dir": journal_dir,
    }
    interrupted = ack_universe_prestage_notifications(**request, failpoint=crash)
    recovered = ack_universe_prestage_notifications(**request)

    assert interrupted["accepted"] is False
    assert interrupted["action"] == "retry_required"
    assert recovered["accepted"] is True
    assert recovered["action"] == "already_acknowledged"
    assert recovered["pending_notification_event_ids"] == []


def test_tampered_event_blocks_outbox_replay(tmp_path):
    report = _alert_report(tmp_path)
    journal_dir = tmp_path / "outbox"
    commit_universe_prestage(inspector_report=report, journal_dir=journal_dir)
    path = journal_dir / f"{SESSION}.jsonl"
    event = json.loads(path.read_text("utf-8"))
    event["reason"] = "EXACT_CURRENT_DAY_PRESTAGE_READY"
    path.write_text(json.dumps(event) + "\n", encoding="utf-8")

    with pytest.raises(owner.MonitorUniversePrestageError, match="receipt_mismatch"):
        inspect_universe_prestage_outbox(
            session_date=SESSION,
            journal_dir=journal_dir,
        )
