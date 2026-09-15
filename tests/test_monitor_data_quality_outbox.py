from __future__ import annotations

import copy
import hashlib
import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

import backend.monitor_session_rollover as session_rollover
from backend.monitor_cadence import (
    CADENCE_EVIDENCE_SCHEMA,
    CADENCE_TRIGGER_CALCULATION_VERSION,
)
from backend.monitor_completed_final_delivery import (
    COMPLETED_FINAL_SCHEMA,
    MonitorCompletedFinalDeliveryError,
    build_data_quality_ack_request,
)
from backend.monitor_data_quality_outbox import (
    ACK_EVENT_TYPE,
    ARMED,
    LEGACY_PRE_ACTIVATION,
    NOTIFICATION_EVENT_TYPE,
    ack_data_quality_notifications,
    sync_data_quality_notifications,
)
from backend.monitor_scan_ledger import commit_monitor_scan, with_scan_event_id


ROOT = Path(__file__).resolve().parents[1]
CT = ZoneInfo("America/Chicago")


def _write_friday_and_roll(
    root: Path, *, target: str = "2026-09-08"
) -> tuple[Path, Path]:
    state_path = root / "state.json"
    root.mkdir(parents=True, exist_ok=True)
    state_path.write_text(
        json.dumps(
            {
                "schema_version": 2,
                "session_date": "2026-09-04",
                "mode": "ELEVATED",
                "opening_acceptance": {"session_date": "2026-09-04"},
                "policy_evaluator": {"SPX": {}, "NDX": {}},
            }
        ),
        encoding="utf-8",
    )
    observed = datetime.fromisoformat(f"{target}T12:45:00+00:00")
    result = session_rollover.prepare_monitor_session(
        state_path=state_path,
        journal_dir=root,
        observed_at_utc=observed,
        session_date=target,
    )
    assert result["accepted"] is True, result
    return state_path, root / f"{target}.jsonl"


def _flags() -> dict[str, dict[str, bool]]:
    fields = (
        "half_threshold_gamma_pin_move",
        "contested_pin_leadership",
        "spot_near_or_crossed_gamma_pin",
        "spot_near_or_crossed_zero_gamma",
        "spot_near_or_crossed_gex_wall",
        "normalized_net_gex_near_or_crossed_zero",
        "forecast_bias_awaiting_confirmation",
        "high_volatility_regime",
    )
    return {
        symbol: {field: False for field in fields}
        for symbol in ("SPX", "NDX")
    }


def _dq_id(session_date: str, symbol: str, issue: str) -> str:
    material = json.dumps(
        {
            "schema_version": "marketpin-monitor-data-quality-condition.v1",
            "session_date": session_date,
            "symbol": symbol,
            "issues": [issue],
        },
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(material).hexdigest()


def _scan(
    session_date: str,
    observed_at_utc: str,
    *,
    mode: str,
    source_id: str,
    issue: str,
) -> dict:
    observed = datetime.fromisoformat(observed_at_utc.replace("Z", "+00:00"))
    descriptor = {
        "event_id": source_id,
        "symbol": "SPX",
        "issues": [issue],
    }
    adaptive = {
        "schema_version": CADENCE_EVIDENCE_SCHEMA,
        "trigger_calculation_version": CADENCE_TRIGGER_CALCULATION_VERSION,
        "active_data_quality_event_ids": [source_id],
        "new_data_quality_event_ids": [source_id],
        "symbols": _flags(),
    }
    return with_scan_event_id(
        {
            "schema_version": 2,
            "event_type": "substantive_scan",
            "observed_at_ct": observed.astimezone(CT).isoformat(),
            "observed_at_utc": observed.astimezone(timezone.utc)
            .isoformat()
            .replace("+00:00", "Z"),
            "session_date": session_date,
            "phase": "regular-session",
            "cadence": {
                "mode": mode,
                "substantive": True,
                "adaptive_evidence": adaptive,
            },
            "evidence": {"data_quality_events": [descriptor]},
            "symbols": {
                symbol: {
                    "eligible": False,
                    "eligibility_reasons": [issue],
                }
                for symbol in ("SPX", "NDX")
            },
            "alerts": [],
            "directional_interpretation": "ABSTAIN",
            "research_hypotheses": [],
        }
    )


def _no_dq_scan(session_date: str, observed_at_utc: str, *, mode: str) -> dict:
    observed = datetime.fromisoformat(observed_at_utc.replace("Z", "+00:00"))
    adaptive = {
        "schema_version": CADENCE_EVIDENCE_SCHEMA,
        "trigger_calculation_version": CADENCE_TRIGGER_CALCULATION_VERSION,
        "active_data_quality_event_ids": [],
        "new_data_quality_event_ids": [],
        "symbols": _flags(),
    }
    return with_scan_event_id(
        {
            "schema_version": 2,
            "event_type": "substantive_scan",
            "observed_at_ct": observed.astimezone(CT).isoformat(),
            "observed_at_utc": observed.astimezone(timezone.utc)
            .isoformat()
            .replace("+00:00", "Z"),
            "session_date": session_date,
            "phase": "regular-session",
            "cadence": {
                "mode": mode,
                "substantive": True,
                "adaptive_evidence": adaptive,
            },
            "evidence": {"data_quality_events": []},
            "symbols": {
                symbol: {
                    "eligible": False,
                    "eligibility_reasons": ["confirmation_history_seeding"],
                    "candidate_policy_observation": {
                        "observation_id": f"{symbol}:{observed_at_utc}",
                        "observed_at_utc": observed_at_utc,
                        "symbol": symbol,
                        "eligible": True,
                        "provider": "databento",
                        "subscription_generation": 1,
                        "subscription_epoch_id": "e" * 64,
                        "primary_expiration": session_date,
                        "gex_formula_version": "databento-gex-v2-call-minus-put",
                        "universe_sha256": "a" * 64,
                        "universe_is_fallback": False,
                        "validation_is_valid": True,
                        "gamma_excluded_from_model": False,
                        "spot": 7000.0 if symbol == "SPX" else 29500.0,
                        "gamma_pin": 7050.0 if symbol == "SPX" else 29600.0,
                        "pin_is_contested": False,
                        "pin_lead_ratio": 0.25,
                        "top_strikes_by_abs_gex": [
                            {"strike": 7000.0 if symbol == "SPX" else 29500.0}
                        ],
                        "max_pain": 6980.0 if symbol == "SPX" else 29480.0,
                        "max_pain_source": "full-oi-universe",
                        "max_pain_formula_version": "full-oi-max-pain-v1",
                        "max_pain_as_of": session_date,
                        "zero_gamma": 6900.0 if symbol == "SPX" else 29200.0,
                        "positive_gex_wall": 7050.0 if symbol == "SPX" else 29600.0,
                        "negative_gex_wall": 6950.0 if symbol == "SPX" else 29450.0,
                        "normalized_net_gex": 0.2,
                    },
                }
                for symbol in ("SPX", "NDX")
            },
            "alerts": [],
            "directional_interpretation": "ABSTAIN",
            "research_hypotheses": [],
        }
    )


def _commit_dq(
    state_path: Path,
    journal_dir: Path,
    *,
    session_date: str,
    observed_at_utc: str,
    issue: str = "HEALTH_MESSAGES_NOT_ADVANCING",
) -> tuple[dict, dict]:
    state = json.loads(state_path.read_text("utf-8"))
    source_id = _dq_id(session_date, "SPX", issue)
    scan = _scan(
        session_date,
        observed_at_utc,
        mode=state["mode"],
        source_id=source_id,
        issue=issue,
    )
    result = commit_monitor_scan(
        scan=scan,
        policy_inputs={},
        state_path=state_path,
        journal_dir=journal_dir,
    )
    assert result["accepted"] is True, result
    return scan, result


def _proof(delivered_at: str = "2026-09-09T14:16:00Z") -> dict:
    return {
        "proof_type": "prior_heartbeat_final_delivered",
        "conversation_history_sha256": "b" * 64,
        "prior_final_delivered_at_utc": delivered_at,
    }


def _rows(path: Path) -> list[dict]:
    return [json.loads(line) for line in path.read_text("utf-8").splitlines()]


def _write_and_arm_september_9(root: Path) -> tuple[Path, Path]:
    state_path, _sep8_journal = _write_friday_and_roll(root)
    rolled = session_rollover.prepare_monitor_session(
        state_path=state_path,
        journal_dir=root,
        observed_at_utc=datetime(2026, 9, 9, 12, 45, tzinfo=timezone.utc),
        session_date="2026-09-09",
    )
    assert rolled["accepted"] is True, rolled
    return state_path, root / "2026-09-09.jsonl"


def _armed_with_source(root: Path) -> tuple[Path, Path, dict]:
    state_path, journal_path = _write_and_arm_september_9(root)
    scan, _committed = _commit_dq(
        state_path,
        root,
        session_date="2026-09-09",
        observed_at_utc="2026-09-09T14:15:00Z",
    )
    return state_path, journal_path, scan


def test_sync_projects_only_receipt_backed_new_ids_and_returns_full_pending(
    tmp_path: Path,
) -> None:
    state_path, journal_path, scan = _armed_with_source(tmp_path)

    synced = sync_data_quality_notifications(
        session_date="2026-09-09",
        state_path=state_path,
        journal_dir=tmp_path,
    )
    assert synced["accepted"] is True
    assert synced["action"] == "synchronized"
    assert synced["event_ids_pending_notification"] == synced["event_ids_appended"]
    assert len(synced["event_ids_pending_notification"]) == 1

    wrapper = next(
        row for row in _rows(journal_path) if row["event_type"] == NOTIFICATION_EVENT_TYPE
    )
    assert wrapper["schema_version"] == 2
    assert wrapper["alerts"] == ["DATA_QUALITY"]
    assert wrapper["directional_interpretation"] == "DATA_QUALITY_ONLY"
    assert "helper_event" not in wrapper
    assert not any(key.startswith("policy_") for key in wrapper)
    assert wrapper["parent_scan_event_id"] == scan["event_id"]
    assert wrapper["data_quality_descriptor"] == scan["evidence"][
        "data_quality_events"
    ][0]
    assert wrapper["data_quality_descriptor_sha256"] == hashlib.sha256(
        (json.dumps(
            wrapper["data_quality_descriptor"],
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
        ) + "\n").encode("utf-8")
    ).hexdigest()

    replay = sync_data_quality_notifications(
        session_date="2026-09-09",
        state_path=state_path,
        journal_dir=tmp_path,
    )
    assert replay["accepted"] is True
    assert replay["action"] == "already_synchronized"
    assert replay["event_ids_pending_notification"] == synced[
        "event_ids_pending_notification"
    ]
    assert replay["event_ids_appended"] == []


def test_sync_recovers_only_exact_journal_ahead_prefix(tmp_path: Path) -> None:
    state_path, journal_path, _scan_row = _armed_with_source(tmp_path / "exact")

    def crash(stage: str) -> None:
        if stage == "after_notification_append":
            raise OSError("simulated hard stop")

    failed = sync_data_quality_notifications(
        session_date="2026-09-09",
        state_path=state_path,
        journal_dir=state_path.parent,
        failpoint=crash,
    )
    assert failed["accepted"] is False
    assert failed["action"] == "retry_required"
    assert failed["event_ids_pending_notification"] == failed["event_ids_appended"]
    state_after_crash = json.loads(state_path.read_text("utf-8"))
    assert state_after_crash["data_quality_notification_outbox"][
        "pending_notification_event_ids"
    ] == []

    recovered = sync_data_quality_notifications(
        session_date="2026-09-09",
        state_path=state_path,
        journal_dir=state_path.parent,
    )
    assert recovered["accepted"] is True
    assert recovered["action"] == "recovered_and_synchronized"
    assert recovered["event_ids_recovered"] == failed["event_ids_appended"]
    assert recovered["event_ids_pending_notification"] == failed[
        "event_ids_pending_notification"
    ]

    conflict_state, conflict_journal, _ = _armed_with_source(tmp_path / "conflict")
    with conflict_journal.open("ab") as handle:
        handle.write(b'{"schema_version":2,"event_type":"foreign')
    before = conflict_journal.read_bytes()
    rejected = sync_data_quality_notifications(
        session_date="2026-09-09",
        state_path=conflict_state,
        journal_dir=conflict_state.parent,
    )
    assert rejected["accepted"] is False
    assert rejected["issues"] == [
        "data_quality_notification_partial_tail_not_exact_prefix"
    ]
    assert conflict_journal.read_bytes() == before


def test_processed_commit_anchor_detects_pre_sync_crash_and_advances_on_no_dq(
    tmp_path: Path,
) -> None:
    state_path, _journal_path = _write_and_arm_september_9(tmp_path)
    state = json.loads(state_path.read_text("utf-8"))
    scan = _no_dq_scan(
        "2026-09-09", "2026-09-09T14:15:00Z", mode=state["mode"]
    )
    committed = commit_monitor_scan(
        scan=scan,
        policy_inputs={},
        state_path=state_path,
        journal_dir=tmp_path,
    )
    assert committed["accepted"] is True, committed
    after_commit = json.loads(state_path.read_text("utf-8"))
    receipt_id = after_commit["monitor_scan_ledger"][
        "last_commit_receipt_event_id"
    ]
    assert after_commit["data_quality_notification_outbox"][
        "last_processed_commit_receipt_event_id"
    ] is None

    synced = sync_data_quality_notifications(
        session_date="2026-09-09",
        state_path=state_path,
        journal_dir=tmp_path,
    )
    assert synced["accepted"] is True
    assert synced["state_updated"] is True
    assert synced["event_ids_pending_notification"] == []
    assert synced["last_processed_commit_receipt_event_id"] == receipt_id
    persisted = json.loads(state_path.read_text("utf-8"))
    assert persisted["data_quality_notification_outbox"][
        "last_processed_commit_receipt_event_id"
    ] == receipt_id


def test_processed_commit_anchor_tamper_fails_closed(tmp_path: Path) -> None:
    state_path, journal_path = _write_and_arm_september_9(tmp_path)
    state = json.loads(state_path.read_text("utf-8"))
    committed = commit_monitor_scan(
        scan=_no_dq_scan(
            "2026-09-09", "2026-09-09T14:15:00Z", mode=state["mode"]
        ),
        policy_inputs={},
        state_path=state_path,
        journal_dir=tmp_path,
    )
    assert committed["accepted"] is True
    assert sync_data_quality_notifications(
        session_date="2026-09-09", state_path=state_path, journal_dir=tmp_path
    )["accepted"] is True
    tampered = json.loads(state_path.read_text("utf-8"))
    tampered["data_quality_notification_outbox"][
        "last_processed_commit_receipt_sha256"
    ] = "0" * 64
    state_path.write_text(json.dumps(tampered), encoding="utf-8")
    journal_before = journal_path.read_bytes()

    rejected = sync_data_quality_notifications(
        session_date="2026-09-09", state_path=state_path, journal_dir=tmp_path
    )
    assert rejected["accepted"] is False
    assert rejected["issues"] == [
        "data_quality_outbox_processed_commit_anchor_tampered"
    ]
    assert journal_path.read_bytes() == journal_before


def test_notification_physically_preceding_commit_receipt_is_rejected(
    tmp_path: Path,
) -> None:
    state_path, journal_path, _scan_row = _armed_with_source(tmp_path)
    synced = sync_data_quality_notifications(
        session_date="2026-09-09", state_path=state_path, journal_dir=tmp_path
    )
    assert synced["accepted"] is True
    rows = _rows(journal_path)
    notification_index = next(
        index
        for index, row in enumerate(rows)
        if row.get("event_type") == NOTIFICATION_EVENT_TYPE
    )
    notification = rows.pop(notification_index)
    receipt_index = next(
        index
        for index, row in enumerate(rows)
        if row.get("event_type") == "substantive_scan_commit"
    )
    rows.insert(receipt_index, notification)
    reordered_raw = b"".join(
        (json.dumps(row, sort_keys=True, separators=(",", ":")) + "\n").encode(
            "utf-8"
        )
        for row in rows
    )
    journal_path.write_bytes(reordered_raw)
    state = json.loads(state_path.read_text("utf-8"))
    state["monitor_scan_ledger"]["committed_journal_size"] = len(reordered_raw)
    state["monitor_scan_ledger"]["committed_journal_sha256"] = hashlib.sha256(
        reordered_raw
    ).hexdigest()
    state_path.write_text(json.dumps(state), encoding="utf-8")

    rejected = sync_data_quality_notifications(
        session_date="2026-09-09", state_path=state_path, journal_dir=tmp_path
    )
    assert rejected["accepted"] is False
    assert rejected["issues"][0].startswith(
        "data_quality_notification_must_follow_parent_and_commit_receipt:"
    )


def test_ack_requires_prior_final_proof_and_appends_before_state_removal(
    tmp_path: Path,
) -> None:
    state_path, journal_path, _scan_row = _armed_with_source(tmp_path)
    synced = sync_data_quality_notifications(
        session_date="2026-09-09", state_path=state_path, journal_dir=tmp_path
    )
    assert synced["accepted"] is True, synced
    pending = synced["event_ids_pending_notification"]

    too_early = ack_data_quality_notifications(
        session_date="2026-09-09",
        event_ids=pending,
        observed_at_utc=datetime.fromisoformat("2026-09-09T14:17:00+00:00"),
        delivery_proof=_proof("2026-09-09T14:14:59Z"),
        state_path=state_path,
        journal_dir=tmp_path,
    )
    assert too_early["accepted"] is False
    assert too_early["event_ids_acknowledged"] == []
    assert too_early["event_ids_pending_notification"] == pending

    observed_pending: list[list[str]] = []

    def inspect_after_append(stage: str) -> None:
        if stage == "after_ack_append":
            durable = _rows(journal_path)[-1]
            assert durable["event_type"] == ACK_EVENT_TYPE
            current = json.loads(state_path.read_text("utf-8"))
            observed_pending.append(
                current["data_quality_notification_outbox"][
                    "pending_notification_event_ids"
                ]
            )

    acknowledged = ack_data_quality_notifications(
        session_date="2026-09-09",
        event_ids=pending,
        observed_at_utc=datetime.fromisoformat("2026-09-09T14:17:00+00:00"),
        delivery_proof=_proof(),
        state_path=state_path,
        journal_dir=tmp_path,
        failpoint=inspect_after_append,
    )
    assert acknowledged["accepted"] is True
    assert acknowledged["action"] == "acknowledged"
    assert observed_pending == [pending]
    assert acknowledged["event_ids_pending_notification"] == []


def test_orphan_ack_requires_exact_request_recovery(tmp_path: Path) -> None:
    state_path, _journal_path, _scan_row = _armed_with_source(tmp_path)
    synced = sync_data_quality_notifications(
        session_date="2026-09-09", state_path=state_path, journal_dir=tmp_path
    )
    pending = synced["event_ids_pending_notification"]

    def crash(stage: str) -> None:
        if stage == "after_ack_append":
            raise OSError("simulated orphan ack")

    failed = ack_data_quality_notifications(
        session_date="2026-09-09",
        event_ids=pending,
        observed_at_utc=datetime.fromisoformat("2026-09-09T14:17:00+00:00"),
        delivery_proof=_proof(),
        state_path=state_path,
        journal_dir=tmp_path,
        failpoint=crash,
    )
    assert failed["accepted"] is False
    assert failed["action"] == "retry_required"

    mismatched = ack_data_quality_notifications(
        session_date="2026-09-09",
        event_ids=pending,
        observed_at_utc=datetime.fromisoformat("2026-09-09T14:18:00+00:00"),
        delivery_proof=_proof(),
        state_path=state_path,
        journal_dir=tmp_path,
    )
    assert mismatched["accepted"] is False
    assert mismatched["issues"][0].startswith(
        "unreflected_data_quality_notification_ack_requires_exact_recovery:"
    )
    assert json.loads(state_path.read_text("utf-8"))[
        "data_quality_notification_outbox"
    ]["pending_notification_event_ids"] == pending

    recovered = ack_data_quality_notifications(
        session_date="2026-09-09",
        event_ids=pending,
        observed_at_utc=datetime.fromisoformat("2026-09-09T14:17:00+00:00"),
        delivery_proof=_proof(),
        state_path=state_path,
        journal_dir=tmp_path,
    )
    assert recovered["accepted"] is True
    assert recovered["action"] == "recovered_and_acknowledged"
    assert recovered["ack_appended"] is False
    assert recovered["event_ids_pending_notification"] == []


def test_september_8_pre_activation_history_is_not_retroactively_wrapped(
    tmp_path: Path,
) -> None:
    state_path, sep8_journal = _write_friday_and_roll(tmp_path)
    _commit_dq(
        state_path,
        tmp_path,
        session_date="2026-09-08",
        observed_at_utc="2026-09-08T14:15:00Z",
    )
    state = json.loads(state_path.read_text("utf-8"))
    state.pop("data_quality_notification_outbox")
    state_path.write_text(json.dumps(state), encoding="utf-8")

    legacy_sync = sync_data_quality_notifications(
        session_date="2026-09-08", state_path=state_path, journal_dir=tmp_path
    )
    assert legacy_sync["accepted"] is True
    assert legacy_sync["action"] == "legacy_pre_activation_noop"
    assert not any(
        row.get("event_type") == NOTIFICATION_EVENT_TYPE
        for row in _rows(sep8_journal)
    )

    rolled = session_rollover.prepare_monitor_session(
        state_path=state_path,
        journal_dir=tmp_path,
        observed_at_utc=datetime(2026, 9, 9, 12, 45, tzinfo=timezone.utc),
        session_date="2026-09-09",
    )
    assert rolled["accepted"] is True, rolled
    next_state = json.loads(state_path.read_text("utf-8"))
    assert next_state["data_quality_notification_outbox"][
        "activation_status"
    ] == ARMED
    assert next_state["data_quality_notification_outbox"][
        "pending_notification_event_ids"
    ] == []
    assert next_state["prior_session_reference"]["wrapper_session_state"][
        "data_quality_notification_outbox"
    ]["activation_status"] == LEGACY_PRE_ACTIVATION


@pytest.mark.parametrize("failure_kind", ["pending", "unreflected_wrapper", "unreflected_ack"])
def test_armed_session_blocks_rollover_while_dq_delivery_is_incomplete(
    tmp_path: Path, failure_kind: str
) -> None:
    # The helper rolls through legacy September 8 into armed September 9 and
    # commits exactly one September 9 source scan before testing delivery.
    state_path, _sep9_journal, _scan_row = _armed_with_source(tmp_path)

    if failure_kind == "unreflected_wrapper":
        def stop_after_wrapper(stage: str) -> None:
            if stage == "after_notification_append":
                raise OSError("simulated wrapper orphan")

        failed = sync_data_quality_notifications(
            session_date="2026-09-09",
            state_path=state_path,
            journal_dir=tmp_path,
            failpoint=stop_after_wrapper,
        )
        assert failed["accepted"] is False
    else:
        synced = sync_data_quality_notifications(
            session_date="2026-09-09",
            state_path=state_path,
            journal_dir=tmp_path,
        )
        assert synced["accepted"] is True
        if failure_kind == "unreflected_ack":
            def stop_after_ack(stage: str) -> None:
                if stage == "after_ack_append":
                    raise OSError("simulated ack orphan")

            failed = ack_data_quality_notifications(
                session_date="2026-09-09",
                event_ids=synced["event_ids_pending_notification"],
                observed_at_utc=datetime.fromisoformat(
                    "2026-09-09T14:17:00+00:00"
                ),
                delivery_proof=_proof("2026-09-09T14:16:00Z"),
                state_path=state_path,
                journal_dir=tmp_path,
                failpoint=stop_after_ack,
            )
            assert failed["accepted"] is False

    blocked = session_rollover.prepare_monitor_session(
        state_path=state_path,
        journal_dir=tmp_path,
        observed_at_utc=datetime(2026, 9, 10, 12, 45, tzinfo=timezone.utc),
        session_date="2026-09-10",
    )
    assert blocked["accepted"] is False
    issue = blocked["issues"][0]
    if failure_kind == "pending":
        assert "prior_session_pending_data_quality_notifications:" in issue
    elif failure_kind == "unreflected_wrapper":
        assert "prior_session_unreflected_data_quality_notification:" in issue
    else:
        assert "prior_session_unreflected_data_quality_notification_ack" in issue


def test_cli_contracts_are_machine_readable(tmp_path: Path) -> None:
    state_path, _journal_path, _scan_row = _armed_with_source(tmp_path)
    sync = subprocess.run(
        [
            sys.executable,
            str(ROOT / "tools" / "sync_market_monitor_data_quality_notifications.py"),
            "--state-path",
            str(state_path),
            "--journal-dir",
            str(tmp_path),
        ],
        input=json.dumps({"session_date": "2026-09-09"}),
        text=True,
        capture_output=True,
        cwd=ROOT,
        check=False,
    )
    assert sync.returncode == 0, sync.stderr
    sync_result = json.loads(sync.stdout)
    pending = sync_result["event_ids_pending_notification"]
    assert pending

    ack = subprocess.run(
        [
            sys.executable,
            str(ROOT / "tools" / "ack_market_monitor_data_quality_notifications.py"),
            "--state-path",
            str(state_path),
            "--journal-dir",
            str(tmp_path),
        ],
        input=json.dumps(
            {
                "session_date": "2026-09-09",
                "event_ids": pending,
                "observed_at_utc": "2026-09-09T14:17:00Z",
                "delivery_proof": _proof(),
            }
        ),
        text=True,
        capture_output=True,
        cwd=ROOT,
        check=False,
    )
    assert ack.returncode == 0, ack.stderr
    assert json.loads(ack.stdout)["event_ids_pending_notification"] == []


def test_completed_final_exact_utf8_proof_builds_request_and_cli_acknowledges(
    tmp_path: Path,
) -> None:
    state_path, journal_path, _scan_row = _armed_with_source(tmp_path)
    synced = sync_data_quality_notifications(
        session_date="2026-09-09", state_path=state_path, journal_dir=tmp_path
    )
    assert synced["accepted"] is True, synced
    pending = synced["event_ids_pending_notification"]
    marker = "2026-09-09T14:16:00.125000Z"
    final_text = (
        "MARKETPIN DATA QUALITY — prior-turn delivery\r\n"
        f"Audit delivery marker: `{marker}`. Pending DATA_QUALITY_ONLY events "
        "surfaced but not acknowledged:\r\n"
        + "\r\n".join(pending)
        + "\r\nExact-byte sentinel: Δ queue pressure.\r\n"
    )
    final_bytes = final_text.encode("utf-8")
    completed_final = {
        "schema_version": COMPLETED_FINAL_SCHEMA,
        "turn_id": "01a087b2-cf19-7632-9f12-9203df334d3c",
        "role": "assistant",
        "status": "completed",
        "completed_at_utc": "2026-09-09T14:16:30Z",
        "final_text": final_text,
    }

    builder_input = {
        "session_date": "2026-09-09",
        "event_ids": pending,
        "observed_at_utc": "2026-09-09T14:17:00Z",
        "completed_final": completed_final,
    }
    built = subprocess.run(
        [
            sys.executable,
            str(
                ROOT
                / "tools"
                / "build_market_monitor_data_quality_ack_request.py"
            ),
            "--input",
            "-",
        ],
        input=json.dumps(builder_input, ensure_ascii=False),
        encoding="utf-8",
        capture_output=True,
        cwd=ROOT,
        check=False,
    )
    assert built.returncode == 0
    assert built.stderr == ""
    assert built.stdout.endswith("\n")
    assert built.stdout.count("\n") == 1
    request = json.loads(built.stdout)

    assert request == {
        "session_date": "2026-09-09",
        "event_ids": pending,
        "observed_at_utc": "2026-09-09T14:17:00Z",
        "delivery_proof": {
            "proof_type": "prior_heartbeat_final_delivered",
            "conversation_history_sha256": hashlib.sha256(final_bytes).hexdigest(),
            "prior_final_delivered_at_utc": marker,
        },
    }
    lf_only = final_text.replace("\r\n", "\n").encode("utf-8")
    assert hashlib.sha256(lf_only).hexdigest() != request["delivery_proof"][
        "conversation_history_sha256"
    ]

    ack = subprocess.run(
        [
            sys.executable,
            str(ROOT / "tools" / "ack_market_monitor_data_quality_notifications.py"),
            "--state-path",
            str(state_path),
            "--journal-dir",
            str(tmp_path),
        ],
        input=json.dumps(request, ensure_ascii=False),
        text=True,
        capture_output=True,
        cwd=ROOT,
        check=False,
    )
    assert ack.returncode == 0, ack.stderr
    result = json.loads(ack.stdout)
    assert result["accepted"] is True
    assert result["event_ids_acknowledged"] == pending
    assert result["event_ids_pending_notification"] == []
    durable_ack = next(
        row for row in _rows(journal_path) if row["event_type"] == ACK_EVENT_TYPE
    )
    assert durable_ack["delivery_proof"] == request["delivery_proof"]


def test_completed_final_builder_cli_failure_is_one_json_and_has_no_side_effect(
    tmp_path: Path,
) -> None:
    sentinel = tmp_path / "sentinel.txt"
    sentinel.write_text("unchanged", encoding="utf-8")
    malformed = {
        "session_date": "2026-09-09",
        "event_ids": ["a" * 64],
        "observed_at_utc": "2026-09-09T14:17:00Z",
        "completed_final": {
            "schema_version": COMPLETED_FINAL_SCHEMA,
            "turn_id": "still-running-turn",
            "role": "assistant",
            "status": "running",
            "completed_at_utc": "2026-09-09T14:16:30Z",
            "final_text": (
                "Audit delivery marker: 2026-09-09T14:16:00Z.\n"
                + "a" * 64
            ),
        },
    }

    built = subprocess.run(
        [
            sys.executable,
            str(
                ROOT
                / "tools"
                / "build_market_monitor_data_quality_ack_request.py"
            ),
        ],
        input=json.dumps(malformed),
        encoding="utf-8",
        capture_output=True,
        cwd=tmp_path,
        check=False,
    )

    assert built.returncode == 1
    assert built.stderr == ""
    assert built.stdout.endswith("\n")
    assert built.stdout.count("\n") == 1
    failure = json.loads(built.stdout)
    assert failure["accepted"] is False
    assert failure["schema_version"] == (
        "marketpin-monitor-data-quality-ack-request-build.error.v1"
    )
    assert failure["issues"] == [
        "input_error:MonitorCompletedFinalDeliveryError:"
        "completed_final_status_invalid"
    ]
    assert sentinel.read_text("utf-8") == "unchanged"
    assert list(tmp_path.iterdir()) == [sentinel]


@pytest.mark.parametrize(
    "rendered_marker",
    [
        "Audit delivery marker: 2026-09-09T14:16:00Z.",
        "Audit delivery marker: `2026-09-09T14:16:00Z`.",
    ],
)
def test_completed_final_marker_accepts_bare_or_single_backtick_inline_code(
    rendered_marker: str,
) -> None:
    event_id = "a" * 64
    final_text = f"{rendered_marker}\n{event_id}"

    request = build_data_quality_ack_request(
        session_date="2026-09-09",
        event_ids=[event_id],
        observed_at_utc="2026-09-09T14:17:00Z",
        completed_final={
            "schema_version": COMPLETED_FINAL_SCHEMA,
            "turn_id": "prior-turn",
            "role": "assistant",
            "status": "completed",
            "completed_at_utc": "2026-09-09T14:16:30Z",
            "final_text": final_text,
        },
    )

    assert request["delivery_proof"] == {
        "proof_type": "prior_heartbeat_final_delivered",
        "conversation_history_sha256": hashlib.sha256(
            final_text.encode("utf-8")
        ).hexdigest(),
        "prior_final_delivered_at_utc": "2026-09-09T14:16:00Z",
    }


@pytest.mark.parametrize(
    "rendered_marker",
    [
        "Audit delivery marker: `2026-09-09T14:16:00Z.",
        "Audit delivery marker: 2026-09-09T14:16:00Z`.",
        "Audit delivery marker: ``2026-09-09T14:16:00Z``.",
        "Audit delivery marker: `2026-09-09T14:16:00Z`",
    ],
)
def test_completed_final_marker_rejects_mismatched_inline_code(
    rendered_marker: str,
) -> None:
    event_id = "a" * 64

    with pytest.raises(
        MonitorCompletedFinalDeliveryError,
        match="completed_final_delivery_marker_format_invalid",
    ):
        build_data_quality_ack_request(
            session_date="2026-09-09",
            event_ids=[event_id],
            observed_at_utc="2026-09-09T14:17:00Z",
            completed_final={
                "schema_version": COMPLETED_FINAL_SCHEMA,
                "turn_id": "prior-turn",
                "role": "assistant",
                "status": "completed",
                "completed_at_utc": "2026-09-09T14:16:30Z",
                "final_text": f"{rendered_marker}\n{event_id}",
            },
        )


def test_completed_final_marker_rejects_mixed_bare_and_inline_duplicates() -> None:
    event_id = "a" * 64
    final_text = (
        "Audit delivery marker: 2026-09-09T14:16:00Z.\n"
        "Audit delivery marker: `2026-09-09T14:16:01Z`.\n"
        f"{event_id}"
    )

    with pytest.raises(
        MonitorCompletedFinalDeliveryError,
        match="completed_final_delivery_marker_must_appear_exactly_once",
    ):
        build_data_quality_ack_request(
            session_date="2026-09-09",
            event_ids=[event_id],
            observed_at_utc="2026-09-09T14:17:00Z",
            completed_final={
                "schema_version": COMPLETED_FINAL_SCHEMA,
                "turn_id": "prior-turn",
                "role": "assistant",
                "status": "completed",
                "completed_at_utc": "2026-09-09T14:16:30Z",
                "final_text": final_text,
            },
        )


@pytest.mark.parametrize(
    ("mutation", "issue"),
    [
        (
            lambda record, event_ids: record.update(status="running"),
            "completed_final_status_invalid",
        ),
        (
            lambda record, event_ids: record.update(
                completed_at_utc="2026-09-09T14:15:59Z"
            ),
            "completed_final_delivery_marker_after_completion",
        ),
        (
            lambda record, event_ids: record.update(
                final_text=record["final_text"].replace(event_ids[0], "f" * 64)
            ),
            "completed_final_event_id_occurrence_invalid:",
        ),
        (
            lambda record, event_ids: record.update(
                final_text=(
                    record["final_text"]
                    + "Audit delivery marker: 2026-09-09T14:16:00Z."
                )
            ),
            "completed_final_delivery_marker_must_appear_exactly_once",
        ),
    ],
)
def test_completed_final_proof_builder_fails_closed(
    mutation,
    issue: str,
) -> None:
    event_ids = ["a" * 64, "b" * 64]
    completed_final = {
        "schema_version": COMPLETED_FINAL_SCHEMA,
        "turn_id": "prior-turn",
        "role": "assistant",
        "status": "completed",
        "completed_at_utc": "2026-09-09T14:16:30Z",
        "final_text": (
            "Audit delivery marker: 2026-09-09T14:16:00Z.\n"
            + "\n".join(event_ids)
        ),
    }
    mutation(completed_final, event_ids)

    with pytest.raises(MonitorCompletedFinalDeliveryError) as caught:
        build_data_quality_ack_request(
            session_date="2026-09-09",
            event_ids=event_ids,
            observed_at_utc="2026-09-09T14:17:00Z",
            completed_final=completed_final,
        )
    assert str(caught.value).startswith(issue)


def test_completed_final_proof_builder_requires_a_later_turn_and_pending_order() -> None:
    event_ids = ["a" * 64, "b" * 64]
    completed_final = {
        "schema_version": COMPLETED_FINAL_SCHEMA,
        "turn_id": "prior-turn",
        "role": "assistant",
        "status": "completed",
        "completed_at_utc": "2026-09-09T14:16:30Z",
        "final_text": (
            "Audit delivery marker: 2026-09-09T14:16:00Z.\n"
            + "\n".join(reversed(event_ids))
        ),
    }

    with pytest.raises(
        MonitorCompletedFinalDeliveryError,
        match="completed_final_event_ids_not_in_pending_order",
    ):
        build_data_quality_ack_request(
            session_date="2026-09-09",
            event_ids=event_ids,
            observed_at_utc="2026-09-09T14:17:00Z",
            completed_final=completed_final,
        )

    completed_final["final_text"] = (
        "Audit delivery marker: 2026-09-09T14:16:00Z.\n" + "\n".join(event_ids)
    )
    with pytest.raises(
        MonitorCompletedFinalDeliveryError,
        match="acknowledgement_must_strictly_follow_completed_final",
    ):
        build_data_quality_ack_request(
            session_date="2026-09-09",
            event_ids=event_ids,
            observed_at_utc=completed_final["completed_at_utc"],
            completed_final=completed_final,
        )
