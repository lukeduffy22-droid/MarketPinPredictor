from __future__ import annotations

import copy
import json
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

import backend.monitor_scan_ledger as scan_ledger
import backend.monitor_session_rollover as session_rollover
from backend.monitor_cadence import (
    CADENCE_EVIDENCE_SCHEMA,
    CADENCE_TRIGGER_CALCULATION_VERSION,
)
from backend.monitor_notification_outbox import ack_monitor_notifications
from backend.monitor_policy import INPUT_SCHEMA, evaluate_monitor_policy
from backend.monitor_scan_ledger import (
    commit_monitor_scan,
    policy_input_sha256,
    with_scan_event_id,
)


ROOT = Path(__file__).resolve().parents[1]


def _observation(
    observed_at: str,
    *,
    gamma_pin: float = 29_600.0,
    max_pain: float = 29_480.0,
    session_date: str = "2026-09-08",
) -> dict:
    return {
        "observation_id": f"NDX:{observed_at}",
        "observed_at_utc": observed_at,
        "symbol": "NDX",
        "eligible": True,
        "provider": "databento",
        "subscription_generation": 7,
        "subscription_epoch_id": "e" * 64,
        "primary_expiration": session_date,
        "gex_formula_version": "databento-gex-v2-call-minus-put",
        "universe_sha256": "a" * 64,
        "universe_is_fallback": False,
        "validation_is_valid": True,
        "gamma_excluded_from_model": False,
        "spot": 29_500.0,
        "gamma_pin": gamma_pin,
        "pin_is_contested": False,
        "pin_lead_ratio": 0.25,
        "top_strikes_by_abs_gex": [
            {"strike": 29_500.0},
            {"strike": 29_525.0},
            {"strike": 29_550.0},
        ],
        "max_pain": max_pain,
        "max_pain_source": "full-oi-universe",
        "max_pain_formula_version": "full-oi-max-pain-v1",
        "max_pain_as_of": session_date,
        "zero_gamma": 29_200.0,
        "positive_gex_wall": 29_600.0,
        "negative_gex_wall": 29_450.0,
        "normalized_net_gex": 0.20,
    }


def _policy_input(*observations: dict, cadence_seconds: int = 300) -> dict:
    return {
        "schema_version": INPUT_SCHEMA,
        "observations": list(observations),
        "confirmation_cadence_seconds": cadence_seconds,
    }


def _baseline_state(session_date: str = "2026-09-08") -> dict:
    evaluated = evaluate_monitor_policy(
        {
            **_policy_input(
                _observation(
                    f"{session_date}T14:00:00Z", session_date=session_date
                ),
                _observation(
                    f"{session_date}T14:05:00Z", session_date=session_date
                ),
            ),
            "policy_state": {},
        }
    )
    assert evaluated["accepted"] is True
    return evaluated["next_state"]


def _write_state(path: Path, *, session_date: str = "2026-09-08") -> dict:
    state = {
        "schema_version": 2,
        "session_date": session_date,
        "mode": "ELEVATED",
        "opening_acceptance": {"session_date": session_date},
        "policy_evaluator": {"SPX": {}, "NDX": _baseline_state(session_date)},
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state), encoding="utf-8")
    return state


def _scan(
    observed_at_utc: str,
    *,
    policy_payload: dict | None = None,
    latest: dict | None = None,
    candidate: dict | None = None,
    prior_confirmation_scan_event_id: str | None = None,
    session_date: str = "2026-09-08",
    cadence_mode: str = "ELEVATED",
    cadence_evidence: dict | None = None,
) -> dict:
    observed_at_ct = (
        datetime.fromisoformat(observed_at_utc.replace("Z", "+00:00"))
        .astimezone(ZoneInfo("America/Chicago"))
        .isoformat()
    )
    eligible = policy_payload is not None
    ndx = {
        "eligible": eligible,
        "eligibility_reasons": [] if eligible else ["SOURCE_INVALID"],
    }
    if eligible:
        ndx["policy_observation"] = copy.deepcopy(latest)
        ndx["policy_input_sha256"] = policy_input_sha256(policy_payload)
    if candidate is not None:
        ndx["candidate_policy_observation"] = copy.deepcopy(candidate)
        if not eligible:
            ndx["eligibility_reasons"] = ["confirmation_history_seeding"]
    if prior_confirmation_scan_event_id is not None:
        ndx["prior_confirmation_scan_event_id"] = prior_confirmation_scan_event_id
    cadence = {"mode": cadence_mode, "substantive": True}
    if cadence_evidence is not None:
        cadence["adaptive_evidence"] = copy.deepcopy(cadence_evidence)
    return with_scan_event_id(
        {
            "schema_version": 2,
            "event_type": "substantive_scan",
            "observed_at_ct": observed_at_ct,
            "observed_at_utc": observed_at_utc,
            "session_date": session_date,
            "phase": "regular-session",
            "cadence": cadence,
            "evidence": ["fixture:outbox"],
            "symbols": {
                "SPX": {"eligible": False, "eligibility_reasons": ["SOURCE_INVALID"]},
                "NDX": ndx,
            },
            "alerts": [],
            "directional_interpretation": (
                "POLICY_CONTROLLED" if eligible else "ABSTAIN"
            ),
            "research_hypotheses": [],
        }
    )


def _cadence_evidence() -> dict:
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
        "schema_version": CADENCE_EVIDENCE_SCHEMA,
        "trigger_calculation_version": CADENCE_TRIGGER_CALCULATION_VERSION,
        "active_data_quality_event_ids": [],
        "new_data_quality_event_ids": [],
        "symbols": {
            symbol: {field: False for field in fields}
            for symbol in ("SPX", "NDX")
        },
    }


def _rows(path: Path) -> list[dict]:
    return [json.loads(line) for line in path.read_text("utf-8").splitlines()]


def _proof(delivered_at: str = "2026-09-08T14:20:30Z") -> dict:
    return {
        "proof_type": "prior_heartbeat_final_delivered",
        "conversation_history_sha256": "b" * 64,
        "prior_final_delivered_at_utc": delivered_at,
    }


def _commit_alert(
    state_path: Path,
    journal_dir: Path,
    *,
    session_date: str = "2026-09-08",
) -> tuple[dict, dict]:
    state = json.loads(state_path.read_text("utf-8"))
    rolled_session = state.get("session_rollover_event_id") is not None
    first_time = "14:00:00Z" if rolled_session else "14:10:00Z"
    first = _observation(
        f"{session_date}T{first_time}",
        gamma_pin=29_700.0,
        max_pain=29_500.0,
        session_date=session_date,
    )
    latest = _observation(
        f"{session_date}T14:15:00Z",
        gamma_pin=29_700.0,
        max_pain=29_500.0,
        session_date=session_date,
    )
    cadence_mode = "NORMAL" if rolled_session else "ELEVATED"
    prior_scan_event_id = None
    if rolled_session:
        seed_scan = _scan(
            f"{session_date}T{first_time}",
            candidate=first,
            session_date=session_date,
            cadence_mode=cadence_mode,
            cadence_evidence=_cadence_evidence(),
        )
        seeded = commit_monitor_scan(
            scan=seed_scan,
            policy_inputs={},
            state_path=state_path,
            journal_dir=journal_dir,
        )
        assert seeded["accepted"] is True, seeded
        prior_scan_event_id = seed_scan["event_id"]
    payload = _policy_input(
        first,
        latest,
        cadence_seconds=900 if rolled_session else 300,
    )
    if prior_scan_event_id is not None:
        payload["prior_scan_event_id"] = prior_scan_event_id
    scan = _scan(
        f"{session_date}T14:15:00Z",
        policy_payload=payload,
        latest=latest,
        candidate=latest if rolled_session else None,
        prior_confirmation_scan_event_id=prior_scan_event_id,
        session_date=session_date,
        cadence_mode=cadence_mode,
        cadence_evidence=_cadence_evidence() if rolled_session else None,
    )
    result = commit_monitor_scan(
        scan=scan,
        policy_inputs={"NDX": payload},
        state_path=state_path,
        journal_dir=journal_dir,
    )
    assert result["accepted"] is True, result
    return scan, result


def _write_rolled_tuesday_state(state_path: Path, journal_dir: Path) -> None:
    friday = _write_state(state_path, session_date="2026-09-04")
    # The fixture deliberately seeds Tuesday policy evidence before rollover,
    # mirroring the production preserved evaluator boundary used by first scan.
    friday["policy_evaluator"]["NDX"] = _baseline_state("2026-09-08")
    state_path.write_text(json.dumps(friday), encoding="utf-8")
    rolled = session_rollover.prepare_monitor_session(
        state_path=state_path,
        journal_dir=journal_dir,
        observed_at_utc=datetime(
            2026, 9, 8, 12, 45, tzinfo=ZoneInfo("UTC")
        ),
        session_date="2026-09-08",
    )
    assert rolled["accepted"] is True, rolled


def test_committed_and_already_committed_results_expose_pending_outbox(
    tmp_path: Path,
) -> None:
    state_path = tmp_path / "state.json"
    _write_state(state_path)
    scan, committed = _commit_alert(state_path, tmp_path)
    assert committed["event_ids_pending_notification"] == committed[
        "event_ids_appended"
    ]
    assert committed["event_ids_pending_notification"]

    replayed = commit_monitor_scan(
        scan=scan,
        policy_inputs={},
        state_path=state_path,
        journal_dir=tmp_path,
    )
    assert replayed["accepted"] is True
    assert replayed["action"] == "already_committed"
    assert replayed["event_ids_pending_notification"] == committed[
        "event_ids_pending_notification"
    ]

    diagnostic = _scan("2026-09-08T14:20:00Z")
    next_result = commit_monitor_scan(
        scan=diagnostic,
        policy_inputs={},
        state_path=state_path,
        journal_dir=tmp_path,
    )
    assert next_result["accepted"] is True
    assert next_result["event_ids_appended"] == []
    assert next_result["event_ids_pending_notification"] == committed[
        "event_ids_pending_notification"
    ]


def test_already_acknowledged_rejects_unknown_partial_journal_tail(
    tmp_path: Path,
) -> None:
    state_path = tmp_path / "state.json"
    _write_state(state_path)
    _scan_row, committed = _commit_alert(state_path, tmp_path)
    pending = committed["event_ids_pending_notification"]
    acknowledged = ack_monitor_notifications(
        session_date="2026-09-08",
        event_ids=pending,
        observed_at_utc=datetime.fromisoformat("2026-09-08T14:21:00+00:00"),
        delivery_proof=_proof(),
        state_path=state_path,
        journal_dir=tmp_path,
    )
    assert acknowledged["accepted"] is True, acknowledged
    journal_path = tmp_path / "2026-09-08.jsonl"
    journal_path.write_bytes(journal_path.read_bytes() + b'{"foreign_partial":')
    before = journal_path.read_bytes()

    retried = ack_monitor_notifications(
        session_date="2026-09-08",
        event_ids=pending,
        observed_at_utc=datetime.fromisoformat("2026-09-08T14:21:00+00:00"),
        delivery_proof=_proof(),
        state_path=state_path,
        journal_dir=tmp_path,
    )

    assert retried["accepted"] is False
    assert retried["requested_event_ids"] == pending
    assert retried["event_ids_acknowledged"] == []
    assert retried["event_ids_pending_notification"] == []
    assert retried["issues"] == [
        "partial_journal_tail_after_committed_notification_ack"
    ]
    assert journal_path.read_bytes() == before


def test_rejected_ack_never_reports_requested_ids_as_acknowledged(
    tmp_path: Path,
) -> None:
    state_path = tmp_path / "state.json"
    _write_state(state_path)
    requested = ["f" * 64]

    rejected = ack_monitor_notifications(
        session_date="2026-09-08",
        event_ids=requested,
        observed_at_utc=datetime.fromisoformat("2026-09-08T14:21:00+00:00"),
        delivery_proof=_proof(),
        state_path=state_path,
        journal_dir=tmp_path,
    )

    assert rejected["accepted"] is False
    assert rejected["requested_event_ids"] == requested
    assert rejected["event_ids_acknowledged"] == []
    assert rejected["event_ids_pending_notification"] is None


def test_partial_ack_recovery_allows_valid_complete_nonpolicy_suffix(
    tmp_path: Path,
) -> None:
    control_dir = tmp_path / "control"
    target_dir = tmp_path / "target"
    control_state = control_dir / "state.json"
    target_state = target_dir / "state.json"
    _write_state(control_state)
    _write_state(target_state)
    _scan_row, control_commit = _commit_alert(control_state, control_dir)
    _scan_row, target_commit = _commit_alert(target_state, target_dir)
    pending = target_commit["event_ids_pending_notification"]
    assert pending == control_commit["event_ids_pending_notification"]
    nonpolicy = {
        "schema_version": 2,
        "event_id": "2026-09-08:data-quality:between-scan-and-ack",
        "event_type": "data_quality",
        "session_date": "2026-09-08",
        "alerts": [],
    }
    for directory in (control_dir, target_dir):
        with (directory / "2026-09-08.jsonl").open("ab") as handle:
            handle.write(scan_ledger._canonical_json_bytes(nonpolicy))

    observed_at = datetime.fromisoformat("2026-09-08T14:21:00+00:00")
    control_ack = ack_monitor_notifications(
        session_date="2026-09-08",
        event_ids=pending,
        observed_at_utc=observed_at,
        delivery_proof=_proof(),
        state_path=control_state,
        journal_dir=control_dir,
    )
    assert control_ack["accepted"] is True, control_ack
    expected_ack_raw = scan_ledger._canonical_json_bytes(
        _rows(control_dir / "2026-09-08.jsonl")[-1]
    )
    target_journal = target_dir / "2026-09-08.jsonl"
    complete_raw = target_journal.read_bytes()
    target_journal.write_bytes(
        complete_raw + expected_ack_raw[: len(expected_ack_raw) // 2]
    )

    recovered = ack_monitor_notifications(
        session_date="2026-09-08",
        event_ids=pending,
        observed_at_utc=observed_at,
        delivery_proof=_proof(),
        state_path=target_state,
        journal_dir=target_dir,
    )

    assert recovered["accepted"] is True, recovered
    assert recovered["action"] == "recovered_and_acknowledged"
    assert recovered["partial_journal_tail_recovered"] is True
    assert recovered["event_ids_acknowledged"] == pending
    assert recovered["event_ids_pending_notification"] == []


def test_failed_scan_recovery_puts_existing_and_new_wrappers_in_outbox(
    tmp_path: Path,
) -> None:
    state_path = tmp_path / "state.json"
    original = _write_state(state_path)
    first = _observation(
        "2026-09-08T14:10:00Z", gamma_pin=29_700.0, max_pain=29_500.0
    )
    latest = _observation(
        "2026-09-08T14:15:00Z", gamma_pin=29_700.0, max_pain=29_500.0
    )
    payload = _policy_input(first, latest)
    scan = _scan("2026-09-08T14:15:00Z", policy_payload=payload, latest=latest)
    appended = 0

    def crash(stage: str) -> None:
        nonlocal appended
        if stage == "after_event_append":
            appended += 1
            if appended == 1:
                raise OSError("simulated helper crash")

    failed = commit_monitor_scan(
        scan=scan,
        policy_inputs={"NDX": payload},
        state_path=state_path,
        journal_dir=tmp_path,
        failpoint=crash,
    )
    assert failed["accepted"] is False
    assert json.loads(state_path.read_text("utf-8")) == original
    recovered = commit_monitor_scan(
        scan=scan,
        policy_inputs={"NDX": payload},
        state_path=state_path,
        journal_dir=tmp_path,
    )
    helper_ids = [
        row["event_id"]
        for row in _rows(tmp_path / "2026-09-08.jsonl")
        if "helper_event" in row
    ]
    assert recovered["accepted"] is True
    assert recovered["action"] == "recovered_and_committed"
    assert set(recovered["event_ids_recovered"]) < set(helper_ids)
    assert recovered["event_ids_pending_notification"] == helper_ids


def test_ack_journals_before_state_and_exactly_clears_pending(tmp_path: Path) -> None:
    state_path = tmp_path / "state.json"
    _write_state(state_path)
    _scan_row, committed = _commit_alert(state_path, tmp_path)
    pending = committed["event_ids_pending_notification"]

    result = ack_monitor_notifications(
        session_date="2026-09-08",
        event_ids=pending,
        observed_at_utc=datetime.fromisoformat("2026-09-08T14:21:00+00:00"),
        delivery_proof=_proof(),
        state_path=state_path,
        journal_dir=tmp_path,
    )

    assert result["accepted"] is True
    assert result["action"] == "acknowledged"
    assert result["event_ids_pending_notification"] == []
    rows = _rows(tmp_path / "2026-09-08.jsonl")
    ack = rows[-1]
    assert ack["event_type"] == "policy_notification_ack"
    assert ack["schema_version"] == 2
    assert ack["alerts"] == []
    assert [row["event_id"] for row in ack["acked_policy_events"]] == pending
    state = json.loads(state_path.read_text("utf-8"))
    ledger = state["monitor_scan_ledger"]
    assert ledger["pending_notification_event_ids"] == []
    assert ledger["last_notification_ack_event_id"] == ack["event_id"]


def test_ack_append_crash_blocks_scan_until_exact_ack_recovery(tmp_path: Path) -> None:
    state_path = tmp_path / "state.json"
    _write_state(state_path)
    _scan_row, committed = _commit_alert(state_path, tmp_path)
    pending = committed["event_ids_pending_notification"]
    original_state = json.loads(state_path.read_text("utf-8"))

    def crash(stage: str) -> None:
        if stage == "after_ack_append":
            raise OSError("simulated ack crash")

    failed = ack_monitor_notifications(
        session_date="2026-09-08",
        event_ids=pending,
        observed_at_utc=datetime.fromisoformat("2026-09-08T14:21:00+00:00"),
        delivery_proof=_proof(),
        state_path=state_path,
        journal_dir=tmp_path,
        failpoint=crash,
    )
    assert failed["accepted"] is False
    assert json.loads(state_path.read_text("utf-8")) == original_state
    journal_path = tmp_path / "2026-09-08.jsonl"
    before_recovery = journal_path.read_bytes()

    scan_blocked = commit_monitor_scan(
        scan=_scan("2026-09-08T14:25:00Z"),
        policy_inputs={},
        state_path=state_path,
        journal_dir=tmp_path,
    )
    assert scan_blocked["accepted"] is False
    assert scan_blocked["issues"][0].startswith(
        "unreflected_notification_ack_requires_exact_recovery:"
    )
    wrong = ack_monitor_notifications(
        session_date="2026-09-08",
        event_ids=pending,
        observed_at_utc=datetime.fromisoformat("2026-09-08T14:22:00+00:00"),
        delivery_proof=_proof(),
        state_path=state_path,
        journal_dir=tmp_path,
    )
    assert wrong["accepted"] is False
    assert wrong["issues"][0].startswith(
        "unreflected_notification_ack_requires_exact_recovery:"
    )

    recovered = ack_monitor_notifications(
        session_date="2026-09-08",
        event_ids=pending,
        observed_at_utc=datetime.fromisoformat("2026-09-08T14:21:00+00:00"),
        delivery_proof=_proof(),
        state_path=state_path,
        journal_dir=tmp_path,
    )
    assert recovered["accepted"] is True
    assert recovered["action"] == "recovered_and_acknowledged"
    assert journal_path.read_bytes() == before_recovery
    assert recovered["event_ids_pending_notification"] == []


def test_exact_ack_recovery_rejects_unreceipted_mode_tamper(
    tmp_path: Path,
) -> None:
    state_path = tmp_path / "state.json"
    _write_state(state_path)
    _scan_row, committed = _commit_alert(state_path, tmp_path)
    pending = committed["event_ids_pending_notification"]

    def interrupt_after_append(stage: str) -> None:
        if stage == "after_ack_append":
            raise OSError("simulated acknowledgement interruption")

    failed = ack_monitor_notifications(
        session_date="2026-09-08",
        event_ids=pending,
        observed_at_utc=datetime.fromisoformat("2026-09-08T14:21:00+00:00"),
        delivery_proof=_proof(),
        state_path=state_path,
        journal_dir=tmp_path,
        failpoint=interrupt_after_append,
    )
    assert failed["accepted"] is False
    state = json.loads(state_path.read_text("utf-8"))
    state["mode"] = "NORMAL"
    state_path.write_text(json.dumps(state), encoding="utf-8")

    recovered = ack_monitor_notifications(
        session_date="2026-09-08",
        event_ids=pending,
        observed_at_utc=datetime.fromisoformat("2026-09-08T14:21:00+00:00"),
        delivery_proof=_proof(),
        state_path=state_path,
        journal_dir=tmp_path,
    )

    assert recovered["accepted"] is False, recovered
    assert recovered["action"] == "abstain"
    assert recovered["issues"] == [
        "monitor_scan_ledger_cadence_state_hash_mismatch"
    ]


def test_exact_retry_recovers_authenticated_partial_ack_tail(tmp_path: Path) -> None:
    control_dir = tmp_path / "control"
    target_dir = tmp_path / "target"
    control_state = control_dir / "state.json"
    target_state = target_dir / "state.json"
    _write_state(control_state)
    _write_state(target_state)
    _scan_row, control_commit = _commit_alert(control_state, control_dir)
    _scan_row, target_commit = _commit_alert(target_state, target_dir)
    assert control_commit["event_ids_pending_notification"] == target_commit[
        "event_ids_pending_notification"
    ]
    pending = target_commit["event_ids_pending_notification"]
    observed_at = datetime.fromisoformat("2026-09-08T14:21:00+00:00")

    control_ack = ack_monitor_notifications(
        session_date="2026-09-08",
        event_ids=pending,
        observed_at_utc=observed_at,
        delivery_proof=_proof(),
        state_path=control_state,
        journal_dir=control_dir,
    )
    assert control_ack["accepted"] is True
    control_journal = control_dir / "2026-09-08.jsonl"
    expected_full_raw = control_journal.read_bytes()
    expected_ack_raw = scan_ledger._canonical_json_bytes(
        _rows(control_journal)[-1]
    )

    target_journal = target_dir / "2026-09-08.jsonl"
    committed_raw = target_journal.read_bytes()
    partial_ack = expected_ack_raw[: len(expected_ack_raw) // 2]
    target_journal.write_bytes(committed_raw + partial_ack)

    recovered = ack_monitor_notifications(
        session_date="2026-09-08",
        event_ids=pending,
        observed_at_utc=observed_at,
        delivery_proof=_proof(),
        state_path=target_state,
        journal_dir=target_dir,
    )

    assert recovered["accepted"] is True, recovered
    assert recovered["action"] == "recovered_and_acknowledged"
    assert recovered["partial_journal_tail_recovered"] is True
    assert recovered["ack_appended"] is True
    assert recovered["event_ids_pending_notification"] == []
    assert target_journal.read_bytes() == expected_full_raw


def test_mismatched_partial_ack_tail_is_never_truncated(tmp_path: Path) -> None:
    state_path = tmp_path / "state.json"
    _write_state(state_path)
    _scan_row, committed = _commit_alert(state_path, tmp_path)
    pending = committed["event_ids_pending_notification"]
    journal_path = tmp_path / "2026-09-08.jsonl"
    forged_tail = b'{"acked_policy_events":[{"event_id":"forged"}'
    journal_path.write_bytes(journal_path.read_bytes() + forged_tail)
    before_journal = journal_path.read_bytes()
    before_state = state_path.read_bytes()

    rejected = ack_monitor_notifications(
        session_date="2026-09-08",
        event_ids=pending,
        observed_at_utc=datetime.fromisoformat("2026-09-08T14:21:00+00:00"),
        delivery_proof=_proof(),
        state_path=state_path,
        journal_dir=tmp_path,
    )

    assert rejected["accepted"] is False
    assert rejected["issues"] == [
        "partial_notification_ack_tail_not_exact_prefix"
    ]
    assert journal_path.read_bytes() == before_journal
    assert state_path.read_bytes() == before_state


def test_post_state_ack_failure_is_idempotently_recoverable(tmp_path: Path) -> None:
    state_path = tmp_path / "state.json"
    _write_state(state_path)
    _scan_row, committed = _commit_alert(state_path, tmp_path)
    pending = committed["event_ids_pending_notification"]

    def fail_after_state(stage: str) -> None:
        if stage == "after_state_replace":
            raise OSError("simulated post-state failure")

    uncertain = ack_monitor_notifications(
        session_date="2026-09-08",
        event_ids=pending,
        observed_at_utc=datetime.fromisoformat("2026-09-08T14:21:00+00:00"),
        delivery_proof=_proof(),
        state_path=state_path,
        journal_dir=tmp_path,
        failpoint=fail_after_state,
    )
    assert uncertain["accepted"] is False
    assert uncertain["action"] == "retry_required"
    assert uncertain["state_updated"] is True
    assert uncertain["commit_phase"] == "post_state_replace_uncertain"

    retry = ack_monitor_notifications(
        session_date="2026-09-08",
        event_ids=pending,
        observed_at_utc=datetime.fromisoformat("2026-09-08T14:21:00+00:00"),
        delivery_proof=_proof(),
        state_path=state_path,
        journal_dir=tmp_path,
    )
    assert retry["accepted"] is True
    assert retry["action"] == "already_acknowledged"
    assert retry["event_ids_pending_notification"] == []


def test_ack_rejects_nonpending_and_forged_duplicate_receipt(tmp_path: Path) -> None:
    state_path = tmp_path / "state.json"
    _write_state(state_path)
    _scan_row, committed = _commit_alert(state_path, tmp_path)
    pending = committed["event_ids_pending_notification"]
    nonpending = ack_monitor_notifications(
        session_date="2026-09-08",
        event_ids=["f" * 64],
        observed_at_utc=datetime.fromisoformat("2026-09-08T14:21:00+00:00"),
        delivery_proof=_proof(),
        state_path=state_path,
        journal_dir=tmp_path,
    )
    assert nonpending["accepted"] is False
    assert nonpending["issues"] == ["notification_ack_event_id_not_pending"]

    acknowledged = ack_monitor_notifications(
        session_date="2026-09-08",
        event_ids=pending,
        observed_at_utc=datetime.fromisoformat("2026-09-08T14:21:00+00:00"),
        delivery_proof=_proof(),
        state_path=state_path,
        journal_dir=tmp_path,
    )
    assert acknowledged["accepted"] is True
    journal_path = tmp_path / "2026-09-08.jsonl"
    rows = _rows(journal_path)
    duplicate = copy.deepcopy(rows[-1])
    with journal_path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(duplicate) + "\n")
    rejected = commit_monitor_scan(
        scan=_scan("2026-09-08T14:25:00Z"),
        policy_inputs={},
        state_path=state_path,
        journal_dir=tmp_path,
    )
    assert rejected["accepted"] is False
    assert rejected["issues"] == [
        "journal_duplicate_event_id:" + duplicate["event_id"]
    ]

    forged_rows = rows[:-1]
    forged = copy.deepcopy(rows[-1])
    forged["acked_policy_events"][0]["record_sha256"] = "f" * 64
    forged["event_id"] = scan_ledger._notification_ack_event_id(forged)
    with pytest.raises(
        scan_ledger.MonitorScanLedgerError,
        match="notification_ack_policy_commitment_mismatch",
    ):
        scan_ledger._journal_index([*forged_rows, forged])


def test_missing_later_turn_delivery_proof_leaves_pending_unchanged(
    tmp_path: Path,
) -> None:
    state_path = tmp_path / "state.json"
    _write_state(state_path)
    _scan_row, committed = _commit_alert(state_path, tmp_path)
    pending = committed["event_ids_pending_notification"]
    journal_path = tmp_path / "2026-09-08.jsonl"
    original_journal = journal_path.read_bytes()
    original_state = json.loads(state_path.read_text("utf-8"))

    rejected = ack_monitor_notifications(
        session_date="2026-09-08",
        event_ids=pending,
        observed_at_utc=datetime.fromisoformat("2026-09-08T14:21:00+00:00"),
        delivery_proof=_proof("2026-09-08T14:21:00Z"),
        state_path=state_path,
        journal_dir=tmp_path,
    )

    assert rejected["accepted"] is False
    assert rejected["issues"] == [
        "acknowledgement_must_follow_prior_final_delivery"
    ]
    assert journal_path.read_bytes() == original_journal
    assert json.loads(state_path.read_text("utf-8")) == original_state
    assert original_state["monitor_scan_ledger"][
        "pending_notification_event_ids"
    ] == pending


def test_delivery_marker_before_helper_confirmation_leaves_pending_unchanged(
    tmp_path: Path,
) -> None:
    state_path = tmp_path / "state.json"
    _write_state(state_path)
    _scan_row, committed = _commit_alert(state_path, tmp_path)
    pending = committed["event_ids_pending_notification"]
    journal_path = tmp_path / "2026-09-08.jsonl"
    original_journal = journal_path.read_bytes()
    original_state = json.loads(state_path.read_text("utf-8"))

    rejected = ack_monitor_notifications(
        session_date="2026-09-08",
        event_ids=pending,
        observed_at_utc=datetime.fromisoformat("2026-09-08T14:16:00+00:00"),
        delivery_proof=_proof("2026-09-08T14:14:00Z"),
        state_path=state_path,
        journal_dir=tmp_path,
    )

    assert rejected["accepted"] is False
    assert rejected["issues"] == [
        "notification_ack_delivery_precedes_policy_wrapper:" + pending[0]
    ]
    assert journal_path.read_bytes() == original_journal
    assert json.loads(state_path.read_text("utf-8")) == original_state


def test_delivery_marker_before_parent_scan_leaves_pending_unchanged(
    tmp_path: Path,
) -> None:
    state_path = tmp_path / "state.json"
    _write_state(state_path)
    first = _observation(
        "2026-09-08T14:10:00Z",
        gamma_pin=29_700.0,
        max_pain=29_500.0,
    )
    latest = _observation(
        "2026-09-08T14:15:00Z",
        gamma_pin=29_700.0,
        max_pain=29_500.0,
    )
    payload = _policy_input(first, latest)
    committed = commit_monitor_scan(
        scan=_scan(
            "2026-09-08T14:19:00Z",
            policy_payload=payload,
            latest=latest,
        ),
        policy_inputs={"NDX": payload},
        state_path=state_path,
        journal_dir=tmp_path,
    )
    assert committed["accepted"] is True, committed
    pending = committed["event_ids_pending_notification"]
    journal_path = tmp_path / "2026-09-08.jsonl"
    original_journal = journal_path.read_bytes()
    original_state = json.loads(state_path.read_text("utf-8"))

    rejected = ack_monitor_notifications(
        session_date="2026-09-08",
        event_ids=pending,
        observed_at_utc=datetime.fromisoformat("2026-09-08T14:20:00+00:00"),
        delivery_proof=_proof("2026-09-08T14:16:00Z"),
        state_path=state_path,
        journal_dir=tmp_path,
    )

    assert rejected["accepted"] is False
    assert rejected["issues"] == [
        "notification_ack_delivery_precedes_parent_scan:" + pending[0]
    ]
    assert journal_path.read_bytes() == original_journal
    assert json.loads(state_path.read_text("utf-8")) == original_state


def test_journal_replay_rejects_causally_backdated_delivery_marker(
    tmp_path: Path,
) -> None:
    state_path = tmp_path / "state.json"
    _write_state(state_path)
    first = _observation(
        "2026-09-08T14:10:00Z",
        gamma_pin=29_700.0,
        max_pain=29_500.0,
    )
    latest = _observation(
        "2026-09-08T14:15:00Z",
        gamma_pin=29_700.0,
        max_pain=29_500.0,
    )
    payload = _policy_input(first, latest)
    committed = commit_monitor_scan(
        scan=_scan(
            "2026-09-08T14:19:00Z",
            policy_payload=payload,
            latest=latest,
        ),
        policy_inputs={"NDX": payload},
        state_path=state_path,
        journal_dir=tmp_path,
    )
    pending = committed["event_ids_pending_notification"]
    acknowledged = ack_monitor_notifications(
        session_date="2026-09-08",
        event_ids=pending,
        observed_at_utc=datetime.fromisoformat("2026-09-08T14:21:00+00:00"),
        delivery_proof=_proof("2026-09-08T14:20:00Z"),
        state_path=state_path,
        journal_dir=tmp_path,
    )
    assert acknowledged["accepted"] is True, acknowledged
    rows = _rows(tmp_path / "2026-09-08.jsonl")
    forged = copy.deepcopy(rows[-1])
    forged["delivery_proof"][
        "prior_final_delivered_at_utc"
    ] = "2026-09-08T14:16:00Z"
    forged["evidence"]["delivery_proof_sha256"] = scan_ledger._canonical_hash(
        forged["delivery_proof"]
    )
    forged["event_id"] = scan_ledger._notification_ack_event_id(forged)

    with pytest.raises(
        scan_ledger.MonitorScanLedgerError,
        match="notification_ack_delivery_precedes_parent_scan:" + pending[0],
    ):
        scan_ledger._journal_index([*rows[:-1], forged])


def test_ack_before_latest_scan_anchor_leaves_pending_unchanged(
    tmp_path: Path,
) -> None:
    state_path = tmp_path / "state.json"
    _write_state(state_path)
    _scan_row, committed = _commit_alert(state_path, tmp_path)
    pending = committed["event_ids_pending_notification"]
    diagnostic = commit_monitor_scan(
        scan=_scan("2026-09-08T14:20:00Z"),
        policy_inputs={},
        state_path=state_path,
        journal_dir=tmp_path,
    )
    assert diagnostic["accepted"] is True, diagnostic
    journal_path = tmp_path / "2026-09-08.jsonl"
    original_journal = journal_path.read_bytes()
    original_state = json.loads(state_path.read_text("utf-8"))

    rejected = ack_monitor_notifications(
        session_date="2026-09-08",
        event_ids=pending,
        observed_at_utc=datetime.fromisoformat("2026-09-08T14:19:00+00:00"),
        delivery_proof=_proof("2026-09-08T14:16:00Z"),
        state_path=state_path,
        journal_dir=tmp_path,
    )

    assert rejected["accepted"] is False
    assert rejected["issues"] == [
        "notification_ack_must_follow_scan_commit_anchor"
    ]
    assert journal_path.read_bytes() == original_journal
    assert json.loads(state_path.read_text("utf-8")) == original_state


def test_ack_cli_requires_exact_request_and_commits(tmp_path: Path) -> None:
    state_path = tmp_path / "state.json"
    _write_state(state_path)
    _scan_row, committed = _commit_alert(state_path, tmp_path)
    request_path = tmp_path / "ack.json"
    request_path.write_text(
        json.dumps(
            {
                "session_date": "2026-09-08",
                "event_ids": committed["event_ids_pending_notification"],
                "observed_at_utc": "2026-09-08T14:21:00Z",
                "delivery_proof": _proof(),
            }
        ),
        encoding="utf-8",
    )
    completed = subprocess.run(
        [
            sys.executable,
            str(ROOT / "tools" / "ack_market_monitor_notifications.py"),
            "--input",
            str(request_path),
            "--state-path",
            str(state_path),
            "--journal-dir",
            str(tmp_path),
        ],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=False,
    )
    assert completed.returncode == 0, completed.stderr
    result = json.loads(completed.stdout)
    assert result["accepted"] is True
    assert result["event_ids_pending_notification"] == []


def test_rollover_blocks_valid_pending_notifications_until_ack(tmp_path: Path) -> None:
    state_path = tmp_path / "state.json"
    _write_rolled_tuesday_state(state_path, tmp_path)
    _scan_row, committed = _commit_alert(state_path, tmp_path)
    pending = committed["event_ids_pending_notification"]

    blocked = session_rollover.prepare_monitor_session(
        state_path=state_path,
        journal_dir=tmp_path,
        observed_at_utc=datetime(
            2026, 9, 9, 12, 45, tzinfo=ZoneInfo("UTC")
        ),
        session_date="2026-09-09",
    )

    assert blocked["accepted"] is False
    assert blocked["issues"] == [
        "prior_session_monitor_scan_ledger_invalid:"
        "prior_session_pending_notifications:"
        + ",".join(pending)
    ]


def test_rollover_distinguishes_orphan_ack_and_unblocks_after_exact_recovery(
    tmp_path: Path,
) -> None:
    state_path = tmp_path / "state.json"
    _write_rolled_tuesday_state(state_path, tmp_path)
    _scan_row, committed = _commit_alert(state_path, tmp_path)
    pending = committed["event_ids_pending_notification"]

    def crash(stage: str) -> None:
        if stage == "after_ack_append":
            raise OSError("simulated orphan ack")

    failed = ack_monitor_notifications(
        session_date="2026-09-08",
        event_ids=pending,
        observed_at_utc=datetime.fromisoformat("2026-09-08T14:21:00+00:00"),
        delivery_proof=_proof(),
        state_path=state_path,
        journal_dir=tmp_path,
        failpoint=crash,
    )
    orphan_ack_id = failed["ack_event_id"]
    blocked = session_rollover.prepare_monitor_session(
        state_path=state_path,
        journal_dir=tmp_path,
        observed_at_utc=datetime(
            2026, 9, 9, 12, 45, tzinfo=ZoneInfo("UTC")
        ),
        session_date="2026-09-09",
    )
    assert blocked["accepted"] is False
    assert blocked["issues"] == [
        "prior_session_monitor_scan_ledger_invalid:"
        "prior_session_unreflected_notification_ack:"
        + orphan_ack_id
    ]

    recovered = ack_monitor_notifications(
        session_date="2026-09-08",
        event_ids=pending,
        observed_at_utc=datetime.fromisoformat("2026-09-08T14:21:00+00:00"),
        delivery_proof=_proof(),
        state_path=state_path,
        journal_dir=tmp_path,
    )
    assert recovered["accepted"] is True
    rolled = session_rollover.prepare_monitor_session(
        state_path=state_path,
        journal_dir=tmp_path,
        observed_at_utc=datetime(
            2026, 9, 9, 12, 45, tzinfo=ZoneInfo("UTC")
        ),
        session_date="2026-09-09",
    )
    assert rolled["accepted"] is True, rolled


def test_friday_pending_can_be_acknowledged_tuesday_without_backdating(
    tmp_path: Path,
) -> None:
    state_path = tmp_path / "state.json"
    _write_state(state_path, session_date="2026-09-04")
    _scan_row, committed = _commit_alert(
        state_path, tmp_path, session_date="2026-09-04"
    )
    pending = committed["event_ids_pending_notification"]

    acknowledged = ack_monitor_notifications(
        session_date="2026-09-04",
        event_ids=pending,
        observed_at_utc=datetime.fromisoformat("2026-09-08T12:47:00+00:00"),
        delivery_proof=_proof("2026-09-08T12:46:00Z"),
        state_path=state_path,
        journal_dir=tmp_path,
    )

    assert acknowledged["accepted"] is True, acknowledged
    friday_rows = _rows(tmp_path / "2026-09-04.jsonl")
    ack = friday_rows[-1]
    assert ack["session_date"] == "2026-09-04"
    assert ack["observed_at_utc"] == "2026-09-08T12:47:00Z"
    assert ack["observed_at_ct"].startswith("2026-09-08T07:47:00")
    rolled = session_rollover.prepare_monitor_session(
        state_path=state_path,
        journal_dir=tmp_path,
        observed_at_utc=datetime(
            2026, 9, 8, 13, 0, tzinfo=ZoneInfo("UTC")
        ),
        session_date="2026-09-08",
    )
    assert rolled["accepted"] is True, rolled


def test_rollover_rejects_prior_session_ack_at_or_after_rollover_time(
    tmp_path: Path,
) -> None:
    state_path = tmp_path / "state.json"
    _write_state(state_path, session_date="2026-09-04")
    _scan_row, committed = _commit_alert(
        state_path, tmp_path, session_date="2026-09-04"
    )
    pending = committed["event_ids_pending_notification"]
    acknowledged = ack_monitor_notifications(
        session_date="2026-09-04",
        event_ids=pending,
        observed_at_utc=datetime.fromisoformat("2026-09-09T12:47:00+00:00"),
        delivery_proof=_proof("2026-09-09T12:46:00Z"),
        state_path=state_path,
        journal_dir=tmp_path,
    )
    assert acknowledged["accepted"] is True, acknowledged
    original_state = state_path.read_bytes()
    friday_journal = (tmp_path / "2026-09-04.jsonl").read_bytes()

    rejected = session_rollover.prepare_monitor_session(
        state_path=state_path,
        journal_dir=tmp_path,
        observed_at_utc=datetime(
            2026, 9, 8, 13, 0, tzinfo=ZoneInfo("UTC")
        ),
        session_date="2026-09-08",
    )

    assert rejected["accepted"] is False
    assert rejected["issues"] == [
        "prior_session_monitor_scan_ledger_invalid:"
        "prior_session_notification_ack_not_before_rollover:"
        + str(acknowledged["ack_event_id"])
    ]
    assert state_path.read_bytes() == original_state
    assert (tmp_path / "2026-09-04.jsonl").read_bytes() == friday_journal
    assert not (tmp_path / "2026-09-08.jsonl").exists()
    assert not (tmp_path / "2026-09-08.session_rollover_intent.json").exists()
