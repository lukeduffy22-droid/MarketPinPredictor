from __future__ import annotations

import copy
import hashlib
import json
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

import backend.monitor_session_rollover as rollover
from backend.monitor_policy import evaluate_monitor_policy
from backend.monitor_scan_ledger import commit_monitor_scan, with_scan_event_id


ROOT = Path(__file__).resolve().parents[1]
TOOL = ROOT / "tools" / "prepare_market_monitor_session.py"
TUESDAY_UTC = datetime(2026, 9, 8, 12, 45, tzinfo=timezone.utc)


def _legacy_level(
    symbol: str,
    *,
    value: float,
    formula: str,
    observed_at: str,
    observation_ids: list[str],
    max_pain: bool = False,
) -> dict:
    provenance = {
        "symbol": symbol,
        "provider": "databento",
        "subscription_generation": 1,
        "primary_expiration": "2026-09-04",
        "universe_sha256": (
            "e0b6d26158efe8bfd2c45be1ffd6e0e8b453ba8152dc2713b24fc17e79b897e6"
        ),
        "universe_is_fallback": False,
        "formula_version": formula,
    }
    if max_pain:
        provenance.update(
            {
                "source": "full-oi-universe",
                "as_of": "2026-09-04",
            }
        )
    prior = datetime.fromisoformat(observed_at.replace("Z", "+00:00"))
    earlier = prior.replace(second=prior.second - 5)
    return {
        "value": value,
        "provenance": provenance,
        "prior_provenance": None,
        "current_provenance": copy.deepcopy(provenance),
        "status": "baseline",
        "confirmation_observation_ids": observation_ids,
        "confirmation_observed_at_utc": [
            earlier.isoformat().replace("+00:00", "Z"),
            observed_at,
        ],
        "confirmed_at_utc": observed_at,
    }


def _friday_state() -> dict:
    """Reproduce the authoritative late-Friday v2 seed relevant to rollover."""

    spx_ids = [
        "73c24bcd1a0ff313edffaa371b63dff51a6e3ff39793affe8952d04fc99090ee",
        "ac3fa5451fcc6f12efc1301653ab7349b4ea8a531be03be1e12bdae60ed298bb",
    ]
    ndx_ids = [
        "298789fb8ce2b7d30321be0b470c3c26194d6863b2e87e383437a72d9f7bbd7d",
        "7a983c35ebc91f4d5ac9739d3d616ac6fb5f43122e7b11b4e7670b8e70926e5a",
    ]
    return {
        "schema_version": 2,
        "session_date": "2026-09-04",
        "updated_at_ct": "2026-09-04T15:05:15-05:00",
        "updated_at_utc": "2026-09-04T20:05:15Z",
        "mode": "ELEVATED",
        "mode_reason": (
            "Clock and startup repaired; post-close no-message timeout forced "
            "generation 3 and live signals remain fail-closed"
        ),
        "elevated_since_ct": "2026-09-04T07:01:34-05:00",
        "elevated_minimum_until_ct": "2026-09-04T15:17:53-05:00",
        "stable_elevated_scan_count": 0,
        "comparison_baselines_eligibility": "ineligible_after_generation_change",
        "comparison_baselines": {"SPX": {"eligible": False}},
        "pending_confirmations": {"SPX": {"candidate": "old"}},
        "pending_zero_gamma_confirmation": {},
        "pending_pin_contest": {"NDX": {"candidate": "old"}},
        "pending_directional_confirmation": {},
        "last_alerted_levels": {
            "SPX": {"gamma_pin": 7700, "directional_alert": "bearish"},
            "NDX": {"max_pain": 29480},
        },
        "preflight_alerts": {"2026-09-04:clock": {"notified": True}},
        "runtime_alerts": {"2026-09-04:reconnect": {"notified": True}},
        "last_alert": {"type": "DATA_QUALITY_FORCED_RECONNECT_AT_CLOSE"},
        "current_data_quality": {"core_stream": "post_close_stale"},
        "last_scan_ct": "2026-09-04T15:05:15-05:00",
        "last_scan_utc": "2026-09-04T20:05:15Z",
        "last_eligible_scan_ct": "2026-09-04T14:54:25-05:00",
        "last_scan_result": "post_close_generation_invalid",
        "opening_acceptance": {
            "startup_milestone": {
                "dedupe_key": "2026-09-04:opening-startup-milestone",
                "notified": True,
            },
            "first_eligible_gamma_capture_notified": True,
            "first_complete_5m_orb_notified": False,
            "final_complete_60m_orb_notified": False,
            "temporary_acceptance_checks_complete": True,
            "temporary_acceptance_outcome": "completed_with_60m_orb_failure",
        },
        "policy_evaluator": {
            "SPX": {
                "session_date": "2026-09-04",
                "last_accepted_observed_at_utc": "2026-09-04T19:51:24Z",
                "levels": {
                    "gamma_pin": _legacy_level(
                        "SPX",
                        value=7720,
                        formula="databento-gex-v2-call-minus-put",
                        observed_at="2026-09-04T19:51:24Z",
                        observation_ids=spx_ids,
                    ),
                    "max_pain": _legacy_level(
                        "SPX",
                        value=7700,
                        formula="full-oi-max-pain-v1",
                        observed_at="2026-09-04T19:51:24Z",
                        observation_ids=spx_ids,
                        max_pain=True,
                    ),
                },
                "directional_latch": {},
                "seed_evidence": {
                    "event_id": "2026-09-04:session:policy_evaluator_seed:v2",
                    "historical_context_only": True,
                    "eligible_for_tuesday_live_confirmation": False,
                },
            },
            "NDX": {
                "session_date": "2026-09-04",
                "last_accepted_observed_at_utc": "2026-09-04T19:55:50Z",
                "levels": {
                    "gamma_pin": _legacy_level(
                        "NDX",
                        value=29570,
                        formula="databento-gex-v2-call-minus-put",
                        observed_at="2026-09-04T19:55:50Z",
                        observation_ids=ndx_ids,
                    ),
                    "max_pain": _legacy_level(
                        "NDX",
                        value=29480,
                        formula="full-oi-max-pain-v1",
                        observed_at="2026-09-04T19:55:50Z",
                        observation_ids=ndx_ids,
                        max_pain=True,
                    ),
                },
                "directional_latch": {},
                "seed_evidence": {
                    "event_id": "2026-09-04:session:policy_evaluator_seed:v2",
                    "historical_context_only": True,
                    "eligible_for_tuesday_live_confirmation": False,
                },
            },
        },
        "prior_session_reference": {
            "session_date": "2026-09-04",
            "journal": "exports/market_monitor/2026-09-04.jsonl",
            "historical_context_only": True,
            "eligible_for_tuesday_live_confirmation": False,
        },
    }


def _write_state(path: Path, state: dict) -> bytes:
    raw = (json.dumps(state, indent=2) + "\n").encode("utf-8")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(raw)
    return raw


def _records(path: Path) -> list[dict]:
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text("utf-8").splitlines()]


def _diagnostic_scan(
    *, session_date: str, observed_at_utc: str, cadence_mode: str = "ELEVATED"
) -> dict:
    observed_at_ct = (
        datetime.fromisoformat(observed_at_utc.replace("Z", "+00:00"))
        .astimezone(ZoneInfo("America/Chicago"))
        .isoformat()
    )
    return with_scan_event_id(
        {
            "schema_version": 2,
            "event_type": "substantive_scan",
            "observed_at_ct": observed_at_ct,
            "observed_at_utc": observed_at_utc,
            "session_date": session_date,
            "phase": "regular-session",
            "cadence": {"mode": cadence_mode, "substantive": True},
            "evidence": ["fixture:prior-session-ledger"],
            "symbols": {
                "SPX": {
                    "eligible": False,
                    "eligibility_reasons": ["SOURCE_INVALID"],
                },
                "NDX": {
                    "eligible": False,
                    "eligibility_reasons": ["SOURCE_INVALID"],
                },
            },
            "alerts": [],
            "directional_interpretation": "ABSTAIN",
            "research_hypotheses": [],
        }
    )


def _write_valid_friday_scan_ledger(
    state_path: Path, journal_dir: Path
) -> tuple[dict, Path]:
    _write_state(state_path, _friday_state())
    scan = _diagnostic_scan(
        session_date="2026-09-04",
        observed_at_utc="2026-09-04T20:10:15Z",
    )
    committed = commit_monitor_scan(
        scan=scan,
        policy_inputs={},
        state_path=state_path,
        journal_dir=journal_dir,
    )
    assert committed["accepted"] is True
    assert committed["state_updated"] is True
    return json.loads(state_path.read_text("utf-8")), journal_dir / "2026-09-04.jsonl"


def _observation(symbol: str, observed_at: str) -> dict:
    is_spx = symbol == "SPX"
    return {
        "observation_id": f"{symbol}:{observed_at}",
        "observed_at_utc": observed_at,
        "symbol": symbol,
        "eligible": True,
        "provider": "databento",
        "subscription_generation": 1,
        "subscription_epoch_id": "e" * 64,
        "primary_expiration": "2026-09-08",
        "gex_formula_version": "databento-gex-v2-call-minus-put",
        "universe_sha256": "a" * 64,
        "universe_is_fallback": False,
        "validation_is_valid": True,
        "gamma_excluded_from_model": False,
        "spot": 7800.0 if is_spx else 30000.0,
        "gamma_pin": 7810.0 if is_spx else 30050.0,
        "pin_is_contested": False,
        "pin_lead_ratio": 0.25,
        "top_strikes_by_abs_gex": [
            {"strike": 7790.0 if is_spx else 29950.0},
            {"strike": 7800.0 if is_spx else 30000.0},
            {"strike": 7810.0 if is_spx else 30050.0},
        ],
        "max_pain": 7780.0 if is_spx else 29950.0,
        "max_pain_source": "full-oi-universe",
        "max_pain_formula_version": "full-oi-max-pain-v1",
        "max_pain_as_of": "2026-09-08",
        "zero_gamma": 7760.0 if is_spx else 29800.0,
        "positive_gex_wall": 7820.0 if is_spx else 30100.0,
        "negative_gex_wall": 7750.0 if is_spx else 29900.0,
        "normalized_net_gex": 0.2,
    }


def test_exact_seeded_friday_state_rolls_to_tuesday_before_helper_baseline(
    tmp_path: Path,
) -> None:
    state_path = tmp_path / "market_monitor" / "state.json"
    journal_dir = state_path.parent
    friday = _friday_state()
    friday_policy = copy.deepcopy(friday["policy_evaluator"])
    friday_opening = copy.deepcopy(friday["opening_acceptance"])
    _write_state(state_path, friday)

    result = rollover.prepare_monitor_session(
        state_path=state_path,
        journal_dir=journal_dir,
        observed_at_utc=TUESDAY_UTC,
        session_date="2026-09-08",
    )

    assert result["accepted"] is True
    assert result["action"] == "rolled_over"
    assert result["journal_event_appended"] is True
    assert result["state_updated"] is True
    records = _records(journal_dir / "2026-09-08.jsonl")
    assert len(records) == 1
    event = records[0]
    assert event["schema_version"] == 2
    assert event["event_type"] == "session_rollover"
    assert event["alerts"] == []
    assert event["cadence"] == {
        "mode": "NORMAL",
        "substantive": True,
        "reason": "verified_open_session_rollover",
    }
    assert event["prior_session_date"] == "2026-09-04"
    assert event["policy_evaluator_preserved_for_helper_rollover"] is True

    tuesday = json.loads(state_path.read_text("utf-8"))
    assert tuesday["session_date"] == "2026-09-08"
    assert tuesday["session_rollover_event_id"] == event["event_id"]
    assert tuesday["policy_evaluator"] == friday_policy
    assert tuesday["mode"] == "NORMAL"
    assert tuesday["mode_reason"] == "session_rollover"
    assert tuesday["elevated_since_ct"] is None
    assert tuesday["elevated_minimum_until_ct"] is None
    assert tuesday["stable_elevated_scan_count"] == 0
    assert tuesday["opening_acceptance"] == {
        "session_date": "2026-09-08",
        "startup_milestone_notified": False,
        "first_eligible_gamma_capture_notified": False,
        "first_complete_5m_orb_notified": False,
        "final_complete_60m_orb_notified": False,
        "temporary_acceptance_checks_complete": False,
    }
    archived = tuesday["prior_session_reference"]["opening_acceptance"]
    assert archived["session_date"] == "2026-09-04"
    assert archived["historical_context_only"] is True
    for key, value in friday_opening.items():
        assert archived[key] == value
    archived_wrapper = tuesday["prior_session_reference"]["wrapper_session_state"]
    for key in (
        "mode",
        "mode_reason",
        "elevated_since_ct",
        "elevated_minimum_until_ct",
        "stable_elevated_scan_count",
    ):
        assert archived_wrapper[key] == friday[key]
    assert archived_wrapper["historical_context_only"] is True
    assert "monitor_scan_ledger" not in archived_wrapper
    prior_reference = tuesday["prior_session_reference"]
    assert prior_reference["policy_evaluator_at_rollover"] == friday_policy
    assert prior_reference["prior_state_sha256"] == event["prior_state_sha256"]
    opening_hash = rollover._value_sha256(archived)
    wrapper_hash = rollover._value_sha256(archived_wrapper)
    policy_hash = rollover._value_sha256(friday_policy)
    assert prior_reference["prior_opening_acceptance_sha256"] == opening_hash
    assert event["prior_opening_acceptance_sha256"] == opening_hash
    assert prior_reference["prior_wrapper_session_state_sha256"] == wrapper_hash
    assert event["prior_wrapper_session_state_sha256"] == wrapper_hash
    assert prior_reference["policy_evaluator_sha256"] == policy_hash
    assert event["policy_evaluator_sha256"] == policy_hash
    assert prior_reference["prior_journal_size_bytes"] == 0
    assert event["prior_journal_size_bytes"] == 0
    empty_journal_hash = hashlib.sha256(b"").hexdigest()
    assert prior_reference["prior_journal_sha256"] == empty_journal_hash
    assert event["prior_journal_sha256"] == empty_journal_hash
    assert tuesday["monitor_scan_ledger"] == {}
    assert tuesday["pending_confirmations"] == {}
    assert tuesday["pending_pin_contest"] == {}
    assert tuesday["comparison_baselines"] == {}
    assert tuesday["last_alerted_levels"] == {}

    for symbol in ("SPX", "NDX"):
        helper_result = evaluate_monitor_policy(
            {
                "schema_version": "marketpin-monitor-policy.input.v2",
                "observations": [
                    _observation(symbol, "2026-09-08T14:00:00Z"),
                    _observation(symbol, "2026-09-08T14:05:00Z"),
                ],
                "policy_state": tuesday["policy_evaluator"][symbol],
                "confirmation_cadence_seconds": 300,
            }
        )
        assert helper_result["accepted"] is True
        assert helper_result["policy_state_reset_for_session_rollover"] is True
        assert helper_result["events"] == []
        assert helper_result["next_state"]["session_date"] == "2026-09-08"


def test_v2_legacy_five_field_opening_reset_replays_for_next_session(
    tmp_path: Path,
) -> None:
    state_path = tmp_path / "monitor" / "state.json"
    journal_dir = state_path.parent
    _write_state(state_path, _friday_state())
    rolled = rollover.prepare_monitor_session(
        state_path=state_path,
        journal_dir=journal_dir,
        observed_at_utc=TUESDAY_UTC,
        session_date="2026-09-08",
    )
    assert rolled["accepted"] is True, rolled
    tuesday_journal = journal_dir / "2026-09-08.jsonl"
    event = _records(tuesday_journal)[0]
    assert event["opening_acceptance_reset"] == {
        "session_date": "2026-09-08",
        "startup_milestone_notified": False,
        "first_eligible_gamma_capture_notified": False,
        "first_complete_5m_orb_notified": False,
        "final_complete_60m_orb_notified": False,
        "temporary_acceptance_checks_complete": False,
    }

    state_before = state_path.read_bytes()
    journal_before = tuesday_journal.read_bytes()
    checked = rollover.prepare_monitor_session(
        state_path=state_path,
        journal_dir=journal_dir,
        observed_at_utc=datetime(2026, 9, 9, 12, 45, tzinfo=timezone.utc),
        session_date="2026-09-09",
        dry_run=True,
    )
    assert checked["accepted"] is True, checked
    assert checked["action"] == "would_roll_over"
    assert state_path.read_bytes() == state_before
    assert tuesday_journal.read_bytes() == journal_before
    assert not (journal_dir / "2026-09-09.jsonl").exists()


def test_weekend_and_labor_day_do_not_consume_opening_reset(tmp_path: Path) -> None:
    for observed in (
        datetime(2026, 9, 5, 14, 0, tzinfo=timezone.utc),
        datetime(2026, 9, 7, 14, 0, tzinfo=timezone.utc),
    ):
        case_dir = tmp_path / observed.date().isoformat()
        state_path = case_dir / "state.json"
        original = _write_state(state_path, _friday_state())

        result = rollover.prepare_monitor_session(
            state_path=state_path,
            journal_dir=case_dir,
            observed_at_utc=observed,
        )

        assert result["accepted"] is True
        assert result["action"] == "non_open_session_noop"
        assert result["state_updated"] is False
        assert result["journal_event_appended"] is False
        assert state_path.read_bytes() == original
        assert not (case_dir / f"{observed.date().isoformat()}.jsonl").exists()


def test_rollover_resets_nonzero_elevated_cadence_lifecycle(
    tmp_path: Path,
) -> None:
    state_path = tmp_path / "monitor" / "state.json"
    friday = _friday_state()
    friday["stable_elevated_scan_count"] = 3
    _write_state(state_path, friday)

    result = rollover.prepare_monitor_session(
        state_path=state_path,
        journal_dir=state_path.parent,
        observed_at_utc=TUESDAY_UTC,
    )

    assert result["accepted"] is True
    tuesday = json.loads(state_path.read_text("utf-8"))
    assert tuesday["mode"] == "NORMAL"
    assert tuesday["mode_reason"] == "session_rollover"
    assert tuesday["elevated_since_ct"] is None
    assert tuesday["elevated_minimum_until_ct"] is None
    assert tuesday["stable_elevated_scan_count"] == 0
    archived = tuesday["prior_session_reference"]["wrapper_session_state"]
    assert archived["mode"] == "ELEVATED"
    assert archived["stable_elevated_scan_count"] == 3


def test_journal_append_precedes_atomic_state_and_retry_uses_receipt(
    tmp_path: Path, monkeypatch
) -> None:
    state_path = tmp_path / "monitor" / "state.json"
    journal_dir = state_path.parent
    original = _write_state(state_path, _friday_state())
    real_atomic_write = rollover._atomic_write_json
    observed_order: list[str] = []

    def fail_after_observing_journal(path: Path, payload: dict) -> None:
        records = _records(journal_dir / "2026-09-08.jsonl")
        assert len(records) == 1
        assert records[0]["event_type"] == "session_rollover"
        observed_order.append("journal_before_state")
        raise OSError("injected atomic state failure")

    monkeypatch.setattr(rollover, "_atomic_write_json", fail_after_observing_journal)
    failed = rollover.prepare_monitor_session(
        state_path=state_path,
        journal_dir=journal_dir,
        observed_at_utc=TUESDAY_UTC,
    )

    assert failed["accepted"] is False
    assert failed["journal_event_appended"] is True
    assert failed["state_updated"] is False
    assert failed["action"] == "retry_required"
    assert failed["commit_phase"] == "journal_durable"
    assert failed["issues"] == ["state_atomic_replace_failed:OSError"]
    assert observed_order == ["journal_before_state"]
    assert state_path.read_bytes() == original
    assert len(_records(journal_dir / "2026-09-08.jsonl")) == 1

    monkeypatch.setattr(rollover, "_atomic_write_json", real_atomic_write)
    recovered = rollover.prepare_monitor_session(
        state_path=state_path,
        journal_dir=journal_dir,
        observed_at_utc=TUESDAY_UTC + timedelta(minutes=5),
    )
    assert recovered["accepted"] is True
    assert recovered["action"] == "recovered_from_durable_rollover_event"
    assert recovered["durable_event_reused"] is True
    assert recovered["journal_event_appended"] is False
    assert recovered["commit_phase"] == "complete"
    assert len(_records(journal_dir / "2026-09-08.jsonl")) == 1


def test_rollover_recovery_uses_durable_event_time_not_earlier_retry_time(
    tmp_path: Path, monkeypatch
) -> None:
    state_path = tmp_path / "monitor" / "state.json"
    journal_dir = state_path.parent
    _write_state(state_path, _friday_state())
    real_atomic_write = rollover._atomic_write_json

    def fail_state_write(path: Path, payload: dict) -> None:
        raise OSError("simulated state replacement failure")

    monkeypatch.setattr(rollover, "_atomic_write_json", fail_state_write)
    durable_time = TUESDAY_UTC + timedelta(minutes=5)
    failed = rollover.prepare_monitor_session(
        state_path=state_path,
        journal_dir=journal_dir,
        observed_at_utc=durable_time,
    )
    assert failed["accepted"] is False
    assert failed["commit_phase"] == "journal_durable"

    monkeypatch.setattr(rollover, "_atomic_write_json", real_atomic_write)
    recovered = rollover.prepare_monitor_session(
        state_path=state_path,
        journal_dir=journal_dir,
        observed_at_utc=TUESDAY_UTC,
    )
    assert recovered["accepted"] is True
    assert recovered["action"] == "recovered_from_durable_rollover_event"
    assert recovered["observed_at_utc"] == "2026-09-08T12:50:00Z"
    persisted = json.loads(state_path.read_text("utf-8"))
    assert persisted["updated_at_utc"] == "2026-09-08T12:50:00Z"
    assert persisted["updated_at_ct"] == "2026-09-08T07:50:00-05:00"


def test_append_failure_leaves_state_unchanged_and_fails_closed(
    tmp_path: Path, monkeypatch
) -> None:
    state_path = tmp_path / "monitor" / "state.json"
    journal_dir = state_path.parent
    original = _write_state(state_path, _friday_state())

    def fail_append(path: Path, record: dict) -> None:
        raise OSError("injected journal failure")

    monkeypatch.setattr(rollover, "_append_jsonl_durable", fail_append)
    result = rollover.prepare_monitor_session(
        state_path=state_path,
        journal_dir=journal_dir,
        observed_at_utc=TUESDAY_UTC,
    )

    assert result["accepted"] is False
    assert result["issues"] == ["journal_append_failed:OSError"]
    assert result["journal_event_appended"] is False
    assert result["state_updated"] is False
    assert state_path.read_bytes() == original
    assert _records(journal_dir / "2026-09-08.jsonl") == []


def test_short_append_then_error_rolls_back_own_partial_record(
    tmp_path: Path, monkeypatch
) -> None:
    state_path = tmp_path / "monitor" / "state.json"
    journal_dir = state_path.parent
    original = _write_state(state_path, _friday_state())
    real_write = rollover.os.write
    calls = 0

    def short_then_fail(descriptor: int, payload) -> int:
        nonlocal calls
        calls += 1
        if calls == 1:
            return real_write(descriptor, bytes(payload[:13]))
        raise OSError("injected append interruption")

    monkeypatch.setattr(rollover.os, "write", short_then_fail)
    failed = rollover.prepare_monitor_session(
        state_path=state_path,
        journal_dir=journal_dir,
        observed_at_utc=TUESDAY_UTC,
    )

    assert failed["accepted"] is False
    assert failed["action"] == "abstain"
    assert failed["commit_phase"] == "not_started"
    assert failed["journal_event_appended"] is False
    assert failed["state_updated"] is False
    assert state_path.read_bytes() == original
    journal_path = journal_dir / "2026-09-08.jsonl"
    assert journal_path.read_bytes() == b""

    monkeypatch.setattr(rollover.os, "write", real_write)
    retry = rollover.prepare_monitor_session(
        state_path=state_path,
        journal_dir=journal_dir,
        observed_at_utc=TUESDAY_UTC + timedelta(minutes=5),
    )
    assert retry["accepted"] is True
    assert retry["action"] == "rolled_over"


def test_hard_interruption_partial_rollover_tail_is_exactly_recovered(
    tmp_path: Path, monkeypatch
) -> None:
    state_path = tmp_path / "monitor" / "state.json"
    journal_dir = state_path.parent
    original_state = _write_state(state_path, _friday_state())
    real_append = rollover._append_jsonl_durable

    def hard_interrupt(path: Path, record: dict) -> None:
        encoded = rollover._canonical_json_bytes(record)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(encoded[: len(encoded) // 2])
        raise KeyboardInterrupt("simulated hard interruption")

    monkeypatch.setattr(rollover, "_append_jsonl_durable", hard_interrupt)
    durable_time = TUESDAY_UTC + timedelta(minutes=5)
    with pytest.raises(KeyboardInterrupt, match="hard interruption"):
        rollover.prepare_monitor_session(
            state_path=state_path,
            journal_dir=journal_dir,
            observed_at_utc=durable_time,
        )
    assert state_path.read_bytes() == original_state
    partial = (journal_dir / "2026-09-08.jsonl").read_bytes()
    assert partial and not partial.endswith(b"\n")

    monkeypatch.setattr(rollover, "_append_jsonl_durable", real_append)
    recovered = rollover.prepare_monitor_session(
        state_path=state_path,
        journal_dir=journal_dir,
        observed_at_utc=TUESDAY_UTC,
    )
    assert recovered["accepted"] is True
    assert recovered["action"] == "rolled_over"
    assert recovered["journal_tail_recovered"] is True
    assert recovered["intent_reused"] is True
    assert recovered["observed_at_utc"] == "2026-09-08T12:50:00Z"
    records = _records(journal_dir / "2026-09-08.jsonl")
    assert len(records) == 1
    assert records[0]["event_type"] == "session_rollover"
    persisted = json.loads(state_path.read_text("utf-8"))
    assert persisted["updated_at_utc"] == "2026-09-08T12:50:00Z"


def test_unknown_partial_rollover_tail_is_never_truncated(
    tmp_path: Path,
) -> None:
    state_path = tmp_path / "monitor" / "state.json"
    journal_dir = state_path.parent
    _write_state(state_path, _friday_state())
    journal_path = journal_dir / "2026-09-08.jsonl"
    journal_path.parent.mkdir(parents=True, exist_ok=True)
    unknown = b'{"schema_version":2,"event_type":"unknown"'
    journal_path.write_bytes(unknown)

    rejected = rollover.prepare_monitor_session(
        state_path=state_path,
        journal_dir=journal_dir,
        observed_at_utc=TUESDAY_UTC,
    )
    assert rejected["accepted"] is False
    assert rejected["action"] == "abstain"
    assert rejected["commit_phase"] == "not_started"
    assert rejected["issues"] == [
        "target_journal_partial_tail_unrecognized"
    ]
    assert journal_path.read_bytes() == unknown


def test_target_journal_interleave_is_detected_before_state_replace(
    tmp_path: Path, monkeypatch
) -> None:
    state_path = tmp_path / "monitor" / "state.json"
    journal_dir = state_path.parent
    original = _write_state(state_path, _friday_state())
    real_append = rollover._append_jsonl_durable
    injected = {
        "schema_version": 2,
        "event_id": "2026-09-08:injected:regular-session-data-quality",
        "event_type": "data_quality",
        "observed_at_ct": "2026-09-08T07:45:00-05:00",
        "observed_at_utc": "2026-09-08T12:45:00Z",
        "session_date": "2026-09-08",
        "phase": "regular_session",
        "cadence": {"mode": "NORMAL", "substantive": False},
        "evidence": ["injected-race"],
        "symbols": {},
        "alerts": [],
        "directional_interpretation": "ABSTAIN",
        "research_hypotheses": [],
    }

    def interleave(path: Path, record: dict) -> None:
        if record.get("event_type") == "session_rollover":
            real_append(path, injected)
        real_append(path, record)

    monkeypatch.setattr(rollover, "_append_jsonl_durable", interleave)
    failed = rollover.prepare_monitor_session(
        state_path=state_path,
        journal_dir=journal_dir,
        observed_at_utc=TUESDAY_UTC,
    )

    assert failed["accepted"] is False
    assert failed["action"] == "abstain"
    assert failed["commit_phase"] == "not_started"
    assert failed["journal_event_appended"] is False
    assert failed["state_updated"] is False
    assert failed["issues"] == ["target_session_journal_changed_during_rollover"]
    assert state_path.read_bytes() == original
    assert [row["event_type"] for row in _records(journal_dir / "2026-09-08.jsonl")] == [
        "data_quality",
    ]

    still_rejected = rollover.prepare_monitor_session(
        state_path=state_path,
        journal_dir=journal_dir,
        observed_at_utc=TUESDAY_UTC + timedelta(minutes=5),
    )
    assert still_rejected["accepted"] is False
    assert still_rejected["action"] == "abstain"
    assert still_rejected["commit_phase"] == "not_started"
    assert still_rejected["issues"] == ["signal_record_precedes_session_rollover"]


def test_post_replace_failure_truthfully_requires_retry(
    tmp_path: Path,
) -> None:
    state_path = tmp_path / "monitor" / "state.json"
    journal_dir = state_path.parent
    _write_state(state_path, _friday_state())

    def fail_after_replace(stage: str) -> None:
        assert stage == "after_state_replace"
        raise RuntimeError("injected readback boundary failure")

    failed = rollover.prepare_monitor_session(
        state_path=state_path,
        journal_dir=journal_dir,
        observed_at_utc=TUESDAY_UTC,
        failpoint=fail_after_replace,
    )

    assert failed["accepted"] is False
    assert failed["action"] == "retry_required"
    assert failed["state_updated"] is True
    assert failed["commit_phase"] == "post_state_replace_uncertain"
    assert failed["issues"] == [
        "post_state_replace_verification_failed:RuntimeError"
    ]
    persisted = json.loads(state_path.read_text("utf-8"))
    assert persisted["session_date"] == "2026-09-08"
    assert persisted["session_rollover_event_id"] == failed["event_id"]
    assert len(_records(journal_dir / "2026-09-08.jsonl")) == 1

    recovered = rollover.prepare_monitor_session(
        state_path=state_path,
        journal_dir=journal_dir,
        observed_at_utc=TUESDAY_UTC,
    )
    assert recovered["accepted"] is True
    assert recovered["action"] == "already_current"
    assert recovered["state_updated"] is False
    assert len(_records(journal_dir / "2026-09-08.jsonl")) == 1


def test_atomic_writer_error_after_replace_reports_truthful_uncertainty(
    tmp_path: Path, monkeypatch
) -> None:
    state_path = tmp_path / "monitor" / "state.json"
    journal_dir = state_path.parent
    _write_state(state_path, _friday_state())
    real_atomic_write = rollover._atomic_write_json

    def replace_then_raise(path: Path, payload: dict) -> None:
        real_atomic_write(path, payload)
        raise OSError("injected error after durable replace")

    monkeypatch.setattr(rollover, "_atomic_write_json", replace_then_raise)
    failed = rollover.prepare_monitor_session(
        state_path=state_path,
        journal_dir=journal_dir,
        observed_at_utc=TUESDAY_UTC,
    )

    assert failed["accepted"] is False
    assert failed["action"] == "retry_required"
    assert failed["state_updated"] is True
    assert failed["commit_phase"] == "post_state_replace_uncertain"
    assert failed["issues"] == ["state_atomic_replace_failed:OSError"]
    assert json.loads(state_path.read_text("utf-8"))["session_date"] == "2026-09-08"

    monkeypatch.setattr(rollover, "_atomic_write_json", real_atomic_write)
    retry = rollover.prepare_monitor_session(
        state_path=state_path,
        journal_dir=journal_dir,
        observed_at_utc=TUESDAY_UTC + timedelta(minutes=5),
    )
    assert retry["accepted"] is True
    assert retry["action"] == "already_current"


@pytest.mark.parametrize(
    ("field", "tampered_value"),
    [
        (
            "market_calendar",
            {
                "date": "2026-09-08",
                "supported": True,
                "market_open": True,
                "reason": "tampered",
            },
        ),
        (
            "cadence",
            {
                "mode": "ELEVATED",
                "substantive": True,
                "reason": "verified_open_session_rollover",
            },
        ),
        ("directional_interpretation", "BULLISH"),
        ("alerts", [{"type": "TAMPERED"}]),
        ("symbols", {"SPX": {"eligible": True}}),
        ("evidence", ["tampered"]),
    ],
)
def test_crash_recovery_rejects_tampered_stable_event_semantics(
    tmp_path: Path,
    monkeypatch,
    field: str,
    tampered_value: object,
) -> None:
    state_path = tmp_path / "monitor" / "state.json"
    journal_dir = state_path.parent
    original = _write_state(state_path, _friday_state())
    real_atomic_write = rollover._atomic_write_json

    def fail_state_write(path: Path, payload: dict) -> None:
        raise OSError("injected atomic state failure")

    monkeypatch.setattr(rollover, "_atomic_write_json", fail_state_write)
    failed = rollover.prepare_monitor_session(
        state_path=state_path,
        journal_dir=journal_dir,
        observed_at_utc=TUESDAY_UTC,
    )
    assert failed["journal_event_appended"] is True
    assert state_path.read_bytes() == original

    journal_path = journal_dir / "2026-09-08.jsonl"
    tampered = _records(journal_path)[0]
    tampered[field] = tampered_value
    journal_path.write_text(
        json.dumps(tampered, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(rollover, "_atomic_write_json", real_atomic_write)

    recovered = rollover.prepare_monitor_session(
        state_path=state_path,
        journal_dir=journal_dir,
        observed_at_utc=TUESDAY_UTC,
    )
    assert recovered["accepted"] is False
    assert recovered["issues"] == ["rollover_event_receipt_mismatch"]
    assert state_path.read_bytes() == original


def test_rollover_is_idempotent_and_rejects_skipped_open_session(
    tmp_path: Path,
) -> None:
    state_path = tmp_path / "monitor" / "state.json"
    journal_dir = state_path.parent
    _write_state(state_path, _friday_state())
    first = rollover.prepare_monitor_session(
        state_path=state_path,
        journal_dir=journal_dir,
        observed_at_utc=TUESDAY_UTC,
    )
    progressed = json.loads(state_path.read_text("utf-8"))
    progressed["policy_evaluator"]["SPX"] = {
        "session_date": "2026-09-08",
        "helper_progressed_after_rollover": True,
    }
    _write_state(state_path, progressed)
    second = rollover.prepare_monitor_session(
        state_path=state_path,
        journal_dir=journal_dir,
        observed_at_utc=TUESDAY_UTC,
    )
    assert first["accepted"] is True
    assert second["accepted"] is True
    assert second["action"] == "already_current"
    assert len(_records(journal_dir / "2026-09-08.jsonl")) == 1

    skipped_state = tmp_path / "skipped" / "state.json"
    _write_state(skipped_state, _friday_state())
    skipped = rollover.prepare_monitor_session(
        state_path=skipped_state,
        journal_dir=skipped_state.parent,
        observed_at_utc=datetime(2026, 9, 9, 12, 45, tzinfo=timezone.utc),
    )
    assert skipped["accepted"] is False
    assert skipped["issues"] == [
        "intervening_open_session_unprocessed:2026-09-08"
    ]
    assert json.loads(skipped_state.read_text("utf-8"))["session_date"] == "2026-09-04"


def test_same_session_rejects_self_consistent_fake_prior_receipt(
    tmp_path: Path,
) -> None:
    state_path = tmp_path / "monitor" / "state.json"
    journal_dir = state_path.parent
    _write_state(state_path, _friday_state())
    rolled = rollover.prepare_monitor_session(
        state_path=state_path,
        journal_dir=journal_dir,
        observed_at_utc=TUESDAY_UTC,
    )
    assert rolled["accepted"] is True

    target_journal = journal_dir / "2026-09-08.jsonl"
    original_event = _records(target_journal)[0]
    fake = rollover._build_event(
        target="2026-09-08",
        prior="2026-09-03",
        observed_utc=TUESDAY_UTC,
        calendar=original_event["market_calendar"],
        state_sha256="0" * 64,
        opening_sha256="1" * 64,
        wrapper_sha256="2" * 64,
        policy_sha256="3" * 64,
        prior_journal_size=17,
        prior_journal_sha256="4" * 64,
    )
    target_journal.write_text(
        json.dumps(fake, sort_keys=True) + "\n", encoding="utf-8"
    )

    retried = rollover.prepare_monitor_session(
        state_path=state_path,
        journal_dir=journal_dir,
        observed_at_utc=TUESDAY_UTC,
    )
    assert retried["accepted"] is False
    assert retried["issues"] == ["current_session_rollover_receipt_invalid"]


@pytest.mark.parametrize("archive_name", ["opening_acceptance", "policy_evaluator"])
def test_same_session_rejects_tampered_archived_snapshot(
    tmp_path: Path, archive_name: str
) -> None:
    state_path = tmp_path / archive_name / "state.json"
    _write_state(state_path, _friday_state())
    rolled = rollover.prepare_monitor_session(
        state_path=state_path,
        journal_dir=state_path.parent,
        observed_at_utc=TUESDAY_UTC,
    )
    assert rolled["accepted"] is True
    state = json.loads(state_path.read_text("utf-8"))
    reference = state["prior_session_reference"]
    if archive_name == "opening_acceptance":
        reference["opening_acceptance"]["tampered"] = True
    else:
        reference["policy_evaluator_at_rollover"]["SPX"]["tampered"] = True
    _write_state(state_path, state)

    retried = rollover.prepare_monitor_session(
        state_path=state_path,
        journal_dir=state_path.parent,
        observed_at_utc=TUESDAY_UTC,
    )
    assert retried["accepted"] is False
    assert retried["issues"] == [
        "current_session_prior_reference_hash_mismatch"
    ]


def test_valid_prior_scan_ledger_is_verified_archived_and_cleared(
    tmp_path: Path,
) -> None:
    state_path = tmp_path / "monitor" / "state.json"
    journal_dir = state_path.parent
    friday, prior_journal = _write_valid_friday_scan_ledger(
        state_path, journal_dir
    )
    prior_raw = prior_journal.read_bytes()

    result = rollover.prepare_monitor_session(
        state_path=state_path,
        journal_dir=journal_dir,
        observed_at_utc=TUESDAY_UTC,
    )

    assert result["accepted"] is True
    tuesday = json.loads(state_path.read_text("utf-8"))
    reference = tuesday["prior_session_reference"]
    archived_ledger = reference["wrapper_session_state"][
        "monitor_scan_ledger"
    ]
    assert archived_ledger == friday["monitor_scan_ledger"]
    assert reference["policy_evaluator_at_rollover"] == friday[
        "policy_evaluator"
    ]
    assert reference["prior_journal_size_bytes"] == len(prior_raw)
    assert reference["prior_journal_sha256"] == hashlib.sha256(
        prior_raw
    ).hexdigest()
    assert tuesday["monitor_scan_ledger"] == {}


@pytest.mark.parametrize(
    "crash_stage", ("after_scan_append", "after_commit_receipt_append")
)
def test_rollover_rejects_unreflected_prior_scan_until_exact_retry_commits(
    tmp_path: Path, crash_stage: str,
) -> None:
    state_path = tmp_path / "monitor" / "state.json"
    journal_dir = state_path.parent
    _write_valid_friday_scan_ledger(state_path, journal_dir)
    orphan = _diagnostic_scan(
        session_date="2026-09-04",
        observed_at_utc="2026-09-04T20:15:15Z",
    )

    def crash_after_scan(stage: str) -> None:
        if stage == crash_stage:
            raise OSError("simulated prior-session orphan scan")

    failed_commit = commit_monitor_scan(
        scan=orphan,
        policy_inputs={},
        state_path=state_path,
        journal_dir=journal_dir,
        failpoint=crash_after_scan,
    )
    assert failed_commit["accepted"] is False
    assert failed_commit["commit_phase"] == "journal_durable"

    rejected_rollover = rollover.prepare_monitor_session(
        state_path=state_path,
        journal_dir=journal_dir,
        observed_at_utc=TUESDAY_UTC,
    )
    assert rejected_rollover["accepted"] is False
    assert rejected_rollover["issues"] == [
        "prior_session_monitor_scan_ledger_invalid:"
        "unreflected_prior_session_scan_transaction_requires_exact_retry"
    ]
    assert json.loads(state_path.read_text("utf-8"))["session_date"] == (
        "2026-09-04"
    )

    recovered = commit_monitor_scan(
        scan=orphan,
        policy_inputs={},
        state_path=state_path,
        journal_dir=journal_dir,
    )
    assert recovered["accepted"] is True
    assert recovered["action"] == "recovered_and_committed"

    rolled = rollover.prepare_monitor_session(
        state_path=state_path,
        journal_dir=journal_dir,
        observed_at_utc=TUESDAY_UTC,
    )
    assert rolled["accepted"] is True
    assert rolled["action"] == "rolled_over"


def test_prior_scan_ledger_rejects_policy_or_journal_tamper_before_rollover(
    tmp_path: Path,
) -> None:
    for tamper in ("policy", "journal"):
        case = tmp_path / tamper
        state_path = case / "state.json"
        friday, prior_journal = _write_valid_friday_scan_ledger(
            state_path, case
        )
        if tamper == "policy":
            friday["policy_evaluator"]["SPX"]["tampered"] = True
            _write_state(state_path, friday)
        else:
            record = _records(prior_journal)[0]
            record["evidence"] = ["tampered"]
            prior_journal.write_text(
                json.dumps(record, sort_keys=True) + "\n", encoding="utf-8"
            )

        result = rollover.prepare_monitor_session(
            state_path=state_path,
            journal_dir=case,
            observed_at_utc=TUESDAY_UTC,
        )
        assert result["accepted"] is False
        assert result["issues"][0].startswith(
            "prior_session_monitor_scan_ledger_invalid:"
        )
        assert json.loads(state_path.read_text("utf-8"))["session_date"] == (
            "2026-09-04"
        )


def test_archived_scan_ledger_is_semantically_revalidated_on_retry(
    tmp_path: Path,
) -> None:
    state_path = tmp_path / "monitor" / "state.json"
    journal_dir = state_path.parent
    _write_valid_friday_scan_ledger(state_path, journal_dir)
    rolled = rollover.prepare_monitor_session(
        state_path=state_path,
        journal_dir=journal_dir,
        observed_at_utc=TUESDAY_UTC,
    )
    assert rolled["accepted"] is True

    state = json.loads(state_path.read_text("utf-8"))
    reference = state["prior_session_reference"]
    wrapper = reference["wrapper_session_state"]
    wrapper["monitor_scan_ledger"]["last_committed_scan_event_id"] = "0" * 64
    rebound_wrapper_hash = rollover._value_sha256(wrapper)
    reference["prior_wrapper_session_state_sha256"] = rebound_wrapper_hash
    _write_state(state_path, state)
    target_journal = journal_dir / "2026-09-08.jsonl"
    event = _records(target_journal)[0]
    event["prior_wrapper_session_state_sha256"] = rebound_wrapper_hash
    target_journal.write_text(
        json.dumps(event, sort_keys=True) + "\n", encoding="utf-8"
    )

    retried = rollover.prepare_monitor_session(
        state_path=state_path,
        journal_dir=journal_dir,
        observed_at_utc=TUESDAY_UTC,
    )
    assert retried["accepted"] is False
    assert retried["issues"][0].startswith(
        "prior_session_monitor_scan_ledger_invalid:"
    )


def test_consecutive_rollover_archives_only_immediate_prior_session(
    tmp_path: Path,
) -> None:
    state_path = tmp_path / "monitor" / "state.json"
    journal_dir = state_path.parent
    _write_state(state_path, _friday_state())
    tuesday_result = rollover.prepare_monitor_session(
        state_path=state_path,
        journal_dir=journal_dir,
        observed_at_utc=TUESDAY_UTC,
    )
    assert tuesday_result["accepted"] is True
    tuesday = json.loads(state_path.read_text("utf-8"))
    tuesday["prior_session_reference"].update(
        {
            "seed_event_id": "friday-only",
            "final_valid_structure": {"session_date": "2026-09-04"},
            "last_full_monitor_scan": {"session_date": "2026-09-04"},
            "eligible_for_tuesday_live_confirmation": False,
        }
    )
    tuesday_policy = copy.deepcopy(tuesday["policy_evaluator"])
    _write_state(state_path, tuesday)
    tuesday_journal_raw = (journal_dir / "2026-09-08.jsonl").read_bytes()

    wednesday = rollover.prepare_monitor_session(
        state_path=state_path,
        journal_dir=journal_dir,
        observed_at_utc=datetime(2026, 9, 9, 12, 45, tzinfo=timezone.utc),
    )

    assert wednesday["accepted"] is True
    persisted = json.loads(state_path.read_text("utf-8"))
    reference = persisted["prior_session_reference"]
    assert reference["session_date"] == "2026-09-08"
    assert reference["policy_evaluator_at_rollover"] == tuesday_policy
    assert reference["prior_journal_size_bytes"] == len(tuesday_journal_raw)
    assert reference["prior_journal_sha256"] == hashlib.sha256(
        tuesday_journal_raw
    ).hexdigest()
    for stale_key in (
        "seed_event_id",
        "final_valid_structure",
        "last_full_monitor_scan",
        "eligible_for_tuesday_live_confirmation",
    ):
        assert stale_key not in reference


def test_missing_rollover_pointer_blocks_first_scan_and_next_rollover(
    tmp_path: Path,
) -> None:
    state_path = tmp_path / "monitor" / "state.json"
    journal_dir = state_path.parent
    _write_state(state_path, _friday_state())
    rolled = rollover.prepare_monitor_session(
        state_path=state_path,
        journal_dir=journal_dir,
        observed_at_utc=TUESDAY_UTC,
    )
    assert rolled["accepted"] is True

    state = json.loads(state_path.read_text("utf-8"))
    state.pop("session_rollover_event_id")
    _write_state(state_path, state)

    first_scan = commit_monitor_scan(
        scan=_diagnostic_scan(
            session_date="2026-09-08",
            observed_at_utc="2026-09-08T14:15:00Z",
            cadence_mode="NORMAL",
        ),
        policy_inputs={},
        state_path=state_path,
        journal_dir=journal_dir,
    )
    assert first_scan["accepted"] is False
    assert first_scan["issues"] == [
        "first_scan_session_rollover_event_id_missing"
    ]

    next_rollover = rollover.prepare_monitor_session(
        state_path=state_path,
        journal_dir=journal_dir,
        observed_at_utc=datetime(2026, 9, 9, 12, 45, tzinfo=timezone.utc),
    )
    assert next_rollover["accepted"] is False
    assert next_rollover["issues"] == [
        "prior_session_rollover_event_id_missing"
    ]
    assert json.loads(state_path.read_text("utf-8"))["session_date"] == (
        "2026-09-08"
    )


def test_missing_scan_ledger_compatibility_is_limited_to_legacy_friday(
    tmp_path: Path,
) -> None:
    state_path = tmp_path / "monitor" / "state.json"
    journal_dir = state_path.parent
    _write_state(state_path, _friday_state())
    tuesday_result = rollover.prepare_monitor_session(
        state_path=state_path,
        journal_dir=journal_dir,
        observed_at_utc=TUESDAY_UTC,
    )
    assert tuesday_result["accepted"] is True
    tuesday = json.loads(state_path.read_text("utf-8"))
    assert tuesday.pop("monitor_scan_ledger") == {}
    _write_state(state_path, tuesday)

    wednesday = rollover.prepare_monitor_session(
        state_path=state_path,
        journal_dir=journal_dir,
        observed_at_utc=datetime(2026, 9, 9, 12, 45, tzinfo=timezone.utc),
    )

    assert wednesday["accepted"] is False
    assert wednesday["issues"] == [
        "prior_session_monitor_scan_ledger_missing"
    ]
    assert json.loads(state_path.read_text("utf-8"))["session_date"] == (
        "2026-09-08"
    )


@pytest.mark.parametrize("tamper", ["content", "truncate"])
def test_same_session_retry_rejects_prior_journal_tamper_or_truncation(
    tmp_path: Path, tamper: str
) -> None:
    state_path = tmp_path / tamper / "state.json"
    journal_dir = state_path.parent
    _write_state(state_path, _friday_state())
    prior_journal = journal_dir / "2026-09-04.jsonl"
    prior_journal.parent.mkdir(parents=True, exist_ok=True)
    original = b'{"legacy":1}\n{"legacy":2}\n'
    prior_journal.write_bytes(original)
    rolled = rollover.prepare_monitor_session(
        state_path=state_path,
        journal_dir=journal_dir,
        observed_at_utc=TUESDAY_UTC,
    )
    assert rolled["accepted"] is True
    if tamper == "content":
        prior_journal.write_bytes(original.replace(b"1", b"9", 1))
    else:
        prior_journal.write_bytes(original[:-1])

    retried = rollover.prepare_monitor_session(
        state_path=state_path,
        journal_dir=journal_dir,
        observed_at_utc=TUESDAY_UTC,
    )
    assert retried["accepted"] is False
    if tamper == "content":
        assert retried["issues"] == [
            "current_session_prior_journal_seal_mismatch"
        ]
    else:
        assert retried["issues"] == ["journal_missing_terminal_newline"]


def test_legacy_missing_ledger_is_allowed_but_entire_journal_is_sealed(
    tmp_path: Path,
) -> None:
    state_path = tmp_path / "monitor" / "state.json"
    journal_dir = state_path.parent
    friday = _friday_state()
    assert "monitor_scan_ledger" not in friday
    _write_state(state_path, friday)
    prior_journal = journal_dir / "2026-09-04.jsonl"
    prior_raw = b'{ "legacy": true, "sequence": 1 }\n{"sequence":2}\n'
    prior_journal.write_bytes(prior_raw)

    result = rollover.prepare_monitor_session(
        state_path=state_path,
        journal_dir=journal_dir,
        observed_at_utc=TUESDAY_UTC,
    )

    assert result["accepted"] is True
    event = _records(journal_dir / "2026-09-08.jsonl")[0]
    persisted = json.loads(state_path.read_text("utf-8"))
    reference = persisted["prior_session_reference"]
    expected_hash = hashlib.sha256(prior_raw).hexdigest()
    assert event["prior_journal_size_bytes"] == len(prior_raw)
    assert reference["prior_journal_size_bytes"] == len(prior_raw)
    assert event["prior_journal_sha256"] == expected_hash
    assert reference["prior_journal_sha256"] == expected_hash


@pytest.mark.parametrize("ledger_value", [None, {}])
def test_legacy_friday_boundary_rejects_unreflected_modern_transaction(
    tmp_path: Path, ledger_value: dict | None,
) -> None:
    state_path = tmp_path / "monitor" / "state.json"
    journal_dir = state_path.parent
    friday = _friday_state()
    if ledger_value is not None:
        friday["monitor_scan_ledger"] = ledger_value
    _write_state(state_path, friday)
    orphan = _diagnostic_scan(
        session_date="2026-09-04",
        observed_at_utc="2026-09-04T20:10:15Z",
    )

    def interrupt_after_receipt(stage: str) -> None:
        if stage == "after_commit_receipt_append":
            raise OSError("simulated legacy Friday interruption")

    failed = commit_monitor_scan(
        scan=orphan,
        policy_inputs={},
        state_path=state_path,
        journal_dir=journal_dir,
        failpoint=interrupt_after_receipt,
    )
    assert failed["accepted"] is False
    assert failed["commit_phase"] == "journal_durable"
    persisted = json.loads(state_path.read_text("utf-8"))
    if ledger_value is None:
        assert "monitor_scan_ledger" not in persisted
    else:
        assert persisted["monitor_scan_ledger"] == {}

    result = rollover.prepare_monitor_session(
        state_path=state_path,
        journal_dir=journal_dir,
        observed_at_utc=TUESDAY_UTC,
    )

    assert result["accepted"] is False
    assert result["issues"] == [
        "legacy_friday_contains_unreflected_modern_transaction"
    ]
    assert json.loads(state_path.read_text("utf-8"))["session_date"] == (
        "2026-09-04"
    )


def test_bounded_cli_rolls_only_temporary_paths(tmp_path: Path) -> None:
    state_path = tmp_path / "monitor" / "state.json"
    journal_dir = state_path.parent
    _write_state(state_path, _friday_state())
    completed = subprocess.run(
        [
            sys.executable,
            str(TOOL),
            "--state-path",
            str(state_path),
            "--journal-dir",
            str(journal_dir),
            "--session-date",
            "2026-09-08",
            "--observed-at-utc",
            "2026-09-08T12:45:00Z",
        ],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=False,
    )
    assert completed.returncode == 0, completed.stderr
    result = json.loads(completed.stdout)
    assert result["schema_version"] == rollover.RESULT_SCHEMA
    assert result["output_mode"] == "compact"
    assert result["accepted"] is True
    assert result["action"] == "rolled_over"
    assert json.loads(state_path.read_text("utf-8"))["session_date"] == "2026-09-08"
    assert len(_records(journal_dir / "2026-09-08.jsonl")) == 1
