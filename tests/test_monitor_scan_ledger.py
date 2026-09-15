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

import backend.monitor_scan_ledger as scan_ledger
import backend.monitor_session_rollover as session_rollover
from backend.monitor_cadence import (
    CADENCE_EVIDENCE_SCHEMA,
    CADENCE_TRIGGER_CALCULATION_VERSION,
)
from backend.monitor_policy import INPUT_SCHEMA, evaluate_monitor_policy
from backend.monitor_scan_ledger import (
    RESULT_SCHEMA,
    commit_monitor_scan,
    policy_input_sha256,
    with_scan_event_id,
)


ROOT = Path(__file__).resolve().parents[1]


def _observation(
    observed_at: str,
    *,
    symbol: str = "NDX",
    spot: float = 29_500.0,
    gamma_pin: float = 29_600.0,
    max_pain: float = 29_480.0,
) -> dict:
    return {
        "observation_id": f"{symbol}:{observed_at}",
        "observed_at_utc": observed_at,
        "symbol": symbol,
        "eligible": True,
        "provider": "databento",
        "subscription_generation": 7,
        "subscription_epoch_id": "e" * 64,
        "primary_expiration": "2026-09-08",
        "gex_formula_version": "databento-gex-v2-call-minus-put",
        "universe_sha256": "a" * 64,
        "universe_is_fallback": False,
        "validation_is_valid": True,
        "gamma_excluded_from_model": False,
        "spot": spot,
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
        "max_pain_as_of": "2026-09-08",
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


def _cadence_evidence(
    *,
    active_data_quality_event_ids: list[str] | None = None,
    new_data_quality_event_ids: list[str] | None = None,
    symbol_flags: dict[str, dict[str, bool]] | None = None,
) -> dict:
    trigger_fields = (
        "half_threshold_gamma_pin_move",
        "contested_pin_leadership",
        "spot_near_or_crossed_gamma_pin",
        "spot_near_or_crossed_zero_gamma",
        "spot_near_or_crossed_gex_wall",
        "normalized_net_gex_near_or_crossed_zero",
        "forecast_bias_awaiting_confirmation",
        "high_volatility_regime",
    )
    symbol_flags = symbol_flags or {}
    return {
        "schema_version": CADENCE_EVIDENCE_SCHEMA,
        "trigger_calculation_version": CADENCE_TRIGGER_CALCULATION_VERSION,
        "active_data_quality_event_ids": sorted(
            active_data_quality_event_ids or []
        ),
        "new_data_quality_event_ids": sorted(new_data_quality_event_ids or []),
        "symbols": {
            symbol: {
                field: bool(symbol_flags.get(symbol, {}).get(field, False))
                for field in trigger_fields
            }
            for symbol in ("SPX", "NDX")
        },
    }


def _scan(
    observed_at_utc: str = "2026-09-08T14:15:00Z",
    *,
    ndx_eligible: bool,
    spx_eligible: bool = False,
    ndx_policy_observation: dict | None = None,
    spx_policy_observation: dict | None = None,
    ndx_candidate_policy_observation: dict | None = None,
    spx_candidate_policy_observation: dict | None = None,
    ndx_prior_confirmation_scan_event_id: str | None = None,
    spx_prior_confirmation_scan_event_id: str | None = None,
    ndx_policy_payload: dict | None = None,
    spx_policy_payload: dict | None = None,
    cadence_mode: str = "ELEVATED",
    cadence_evidence: dict | None = None,
) -> dict:
    observed_at_ct = (
        datetime.fromisoformat(observed_at_utc.replace("Z", "+00:00"))
        .astimezone(ZoneInfo("America/Chicago"))
        .isoformat()
    )
    spx_payload = {
        "eligible": spx_eligible,
        "eligibility_reasons": [] if spx_eligible else ["SOURCE_INVALID"],
    }
    if spx_eligible:
        spx_payload["policy_observation"] = copy.deepcopy(
            spx_policy_observation
            or _observation(observed_at_utc, symbol="SPX", spot=7_000.0)
        )
        spx_payload["policy_input_sha256"] = (
            policy_input_sha256(spx_policy_payload)
            if spx_policy_payload is not None
            else "0" * 64
        )
    if spx_candidate_policy_observation is not None:
        spx_payload["candidate_policy_observation"] = copy.deepcopy(
            spx_candidate_policy_observation
        )
        if not spx_eligible:
            spx_payload["eligibility_reasons"] = ["confirmation_history_seeding"]
    if spx_prior_confirmation_scan_event_id is not None:
        spx_payload["prior_confirmation_scan_event_id"] = (
            spx_prior_confirmation_scan_event_id
        )
    ndx_payload = {
        "eligible": ndx_eligible,
        "eligibility_reasons": [] if ndx_eligible else ["SOURCE_INVALID"],
        "primary_expiration": "2026-09-08",
        "future_expiration_profiles": [
            {
                "expiration": "2026-09-09",
                "pin": 29_700.0,
                "role": "future_expiration_context_only",
            }
        ],
    }
    if ndx_eligible:
        ndx_payload["policy_observation"] = copy.deepcopy(
            ndx_policy_observation or _observation(observed_at_utc)
        )
        ndx_payload["policy_input_sha256"] = (
            policy_input_sha256(ndx_policy_payload)
            if ndx_policy_payload is not None
            else "0" * 64
        )
    if ndx_candidate_policy_observation is not None:
        ndx_payload["candidate_policy_observation"] = copy.deepcopy(
            ndx_candidate_policy_observation
        )
        if not ndx_eligible:
            ndx_payload["eligibility_reasons"] = ["confirmation_history_seeding"]
    if ndx_prior_confirmation_scan_event_id is not None:
        ndx_payload["prior_confirmation_scan_event_id"] = (
            ndx_prior_confirmation_scan_event_id
        )
    cadence = {"mode": cadence_mode, "substantive": True}
    if cadence_evidence is not None:
        cadence["adaptive_evidence"] = copy.deepcopy(cadence_evidence)
    return with_scan_event_id(
        {
            "schema_version": 2,
            "event_type": "substantive_scan",
            "observed_at_ct": observed_at_ct,
            "observed_at_utc": observed_at_utc,
            "session_date": "2026-09-08",
            "phase": "regular-session",
            "cadence": cadence,
            "evidence": ["fixture:same-bucket-authorities"],
            "symbols": {
                "SPX": spx_payload,
                "NDX": ndx_payload,
            },
            "alerts": [],
            "directional_interpretation": (
                "POLICY_CONTROLLED" if ndx_eligible or spx_eligible else "ABSTAIN"
            ),
            "research_hypotheses": [],
        }
    )


def _write_state(path: Path, *, policy_state: dict | None = None) -> dict:
    state = {
        "schema_version": 2,
        "session_date": "2026-09-08",
        "mode": "ELEVATED",
        "policy_evaluator": {"NDX": copy.deepcopy(policy_state or {}), "SPX": {}},
        "wrapper_extension": {"preserve": True},
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state, indent=2), encoding="utf-8")
    return state


def _journal_rows(path: Path) -> list[dict]:
    return [json.loads(line) for line in path.read_text("utf-8").splitlines() if line]


def _baseline_state() -> dict:
    first = _observation("2026-09-08T14:00:00Z")
    second = _observation("2026-09-08T14:05:00Z")
    result = evaluate_monitor_policy(
        {
            "schema_version": INPUT_SCHEMA,
            "observations": [first, second],
            "policy_state": {},
            "confirmation_cadence_seconds": 300,
        }
    )
    assert result["accepted"] is True
    return result["next_state"]


def _rollover_friday_to_tuesday(tmp_path: Path) -> tuple[Path, Path, dict]:
    state_path = tmp_path / "state.json"
    tmp_path.mkdir(parents=True, exist_ok=True)
    friday = {
        "schema_version": 2,
        "session_date": "2026-09-04",
        "mode": "ELEVATED",
        "opening_acceptance": {"session_date": "2026-09-04"},
        "policy_evaluator": {"SPX": {}, "NDX": {}},
    }
    state_path.write_text(json.dumps(friday), encoding="utf-8")
    rolled = session_rollover.prepare_monitor_session(
        state_path=state_path,
        journal_dir=tmp_path,
        observed_at_utc=datetime(2026, 9, 8, 12, 45, tzinfo=timezone.utc),
        session_date="2026-09-08",
    )
    assert rolled["accepted"] is True, rolled
    journal_path = tmp_path / "2026-09-08.jsonl"
    return state_path, journal_path, json.loads(state_path.read_text("utf-8"))


def _dual_symbol_explicit_scan(
    observed_at_utc: str,
    prior_observed_at_utc: str,
    *,
    prior_scan_event_id: str,
    cadence_mode: str,
    cadence_seconds: int,
    cadence_evidence: dict,
) -> tuple[dict, dict[str, dict]]:
    ndx_first = _observation(prior_observed_at_utc)
    ndx_latest = _observation(observed_at_utc)
    spx_first = _observation(
        prior_observed_at_utc,
        symbol="SPX",
        spot=7_000.0,
        gamma_pin=7_050.0,
        max_pain=6_980.0,
    )
    spx_latest = _observation(
        observed_at_utc,
        symbol="SPX",
        spot=7_000.0,
        gamma_pin=7_050.0,
        max_pain=6_980.0,
    )
    ndx_policy = _policy_input(
        ndx_first, ndx_latest, cadence_seconds=cadence_seconds
    )
    spx_policy = _policy_input(
        spx_first, spx_latest, cadence_seconds=cadence_seconds
    )
    ndx_policy["prior_scan_event_id"] = prior_scan_event_id
    spx_policy["prior_scan_event_id"] = prior_scan_event_id
    scan = _scan(
        observed_at_utc,
        ndx_eligible=True,
        spx_eligible=True,
        ndx_policy_observation=ndx_latest,
        spx_policy_observation=spx_latest,
        ndx_candidate_policy_observation=ndx_latest,
        spx_candidate_policy_observation=spx_latest,
        ndx_prior_confirmation_scan_event_id=prior_scan_event_id,
        spx_prior_confirmation_scan_event_id=prior_scan_event_id,
        ndx_policy_payload=ndx_policy,
        spx_policy_payload=spx_policy,
        cadence_mode=cadence_mode,
        cadence_evidence=cadence_evidence,
    )
    return scan, {"NDX": ndx_policy, "SPX": spx_policy}


def _dual_symbol_seed_scan(observed_at_utc: str, *, cadence_mode: str) -> dict:
    return _scan(
        observed_at_utc,
        ndx_eligible=False,
        spx_eligible=False,
        ndx_candidate_policy_observation=_observation(observed_at_utc),
        spx_candidate_policy_observation=_observation(
            observed_at_utc,
            symbol="SPX",
            spot=7_000.0,
            gamma_pin=7_050.0,
            max_pain=6_980.0,
        ),
        cadence_mode=cadence_mode,
        cadence_evidence=_cadence_evidence(),
    )


def test_diagnostic_scan_is_durable_without_invoking_policy(tmp_path: Path) -> None:
    state_path = tmp_path / "state.json"
    original = _write_state(state_path, policy_state={"keep": "unchanged"})
    scan = _scan(ndx_eligible=False)

    def forbidden(_payload):
        raise AssertionError("policy must not run for an ineligible diagnostic")

    result = commit_monitor_scan(
        scan=scan,
        policy_inputs={"NDX": {"malformed": True}},
        state_path=state_path,
        journal_dir=tmp_path,
        evaluator=forbidden,
    )

    assert result["accepted"] is True
    assert result["diagnostic_symbols_retained"] == ["SPX", "NDX"]
    assert result["policy_symbols_evaluated"] == []
    rows = _journal_rows(tmp_path / "2026-09-08.jsonl")
    assert rows[0] == scan
    assert rows[1]["event_type"] == "substantive_scan_commit"
    assert rows[1]["ordered_policy_event_ids"] == []
    assert result["event_ids_appended"] == []
    assert result["commit_receipt_event_id"] == rows[1]["event_id"]
    assert result["commit_receipt_appended"] is True
    persisted = json.loads(state_path.read_text("utf-8"))
    assert persisted["policy_evaluator"] == original["policy_evaluator"]
    assert persisted["wrapper_extension"] == {"preserve": True}
    assert persisted["last_scan_result"] == "diagnostic_unavailable"


def test_rollover_first_valid_observation_seeds_then_next_scan_anchors_policy(
    tmp_path: Path,
) -> None:
    state_path, journal_path, _rolled_state = _rollover_friday_to_tuesday(tmp_path)
    seed_observation = _observation("2026-09-08T14:00:00Z")
    seed_scan = _scan(
        "2026-09-08T14:00:00Z",
        ndx_eligible=False,
        ndx_candidate_policy_observation=seed_observation,
        cadence_mode="NORMAL",
        cadence_evidence=_cadence_evidence(),
    )

    seeded = commit_monitor_scan(
        scan=seed_scan,
        policy_inputs={},
        state_path=state_path,
        journal_dir=tmp_path,
    )

    assert seeded["accepted"] is True
    assert seeded["event_ids_appended"] == []
    assert seeded["policy_symbols_evaluated"] == []
    assert json.loads(state_path.read_text("utf-8"))["policy_evaluator"]["NDX"] == {}

    latest = _observation("2026-09-08T14:15:00Z")
    policy_payload = _policy_input(seed_observation, latest, cadence_seconds=900)
    policy_payload["prior_scan_event_id"] = seed_scan["event_id"]
    scan = _scan(
        "2026-09-08T14:15:00Z",
        ndx_eligible=True,
        ndx_policy_observation=latest,
        ndx_candidate_policy_observation=latest,
        ndx_prior_confirmation_scan_event_id=seed_scan["event_id"],
        ndx_policy_payload=policy_payload,
        cadence_mode="NORMAL",
        cadence_evidence=_cadence_evidence(),
    )
    committed = commit_monitor_scan(
        scan=scan,
        policy_inputs={"NDX": policy_payload},
        state_path=state_path,
        journal_dir=tmp_path,
    )

    assert committed["accepted"] is True, committed
    assert committed["policy_symbols_evaluated"] == ["NDX"]
    assert committed["event_ids_appended"] == []
    rows = _journal_rows(journal_path)
    assert rows[-2]["event_type"] == "substantive_scan"
    assert rows[-2]["symbols"]["NDX"]["prior_confirmation_scan_event_id"] == (
        seed_scan["event_id"]
    )
    persisted = json.loads(state_path.read_text("utf-8"))
    assert (
        persisted["policy_evaluator"]["NDX"]["last_accepted_observed_at_utc"]
        == "2026-09-08T14:15:00Z"
    )


def test_confirmation_anchor_rejects_single_wake_and_fabricated_prior_without_write(
    tmp_path: Path,
) -> None:
    state_path, journal_path, _rolled_state = _rollover_friday_to_tuesday(tmp_path)
    seed_observation = _observation("2026-09-08T14:00:00Z")
    seed_scan = _scan(
        "2026-09-08T14:00:00Z",
        ndx_eligible=False,
        ndx_candidate_policy_observation=seed_observation,
        cadence_mode="NORMAL",
        cadence_evidence=_cadence_evidence(),
    )
    seeded = commit_monitor_scan(
        scan=seed_scan,
        policy_inputs={},
        state_path=state_path,
        journal_dir=tmp_path,
    )
    assert seeded["accepted"] is True

    state_before = state_path.read_bytes()
    journal_before = journal_path.read_bytes()
    too_soon = _observation("2026-09-08T14:00:01Z")
    too_soon_policy = _policy_input(
        seed_observation, too_soon, cadence_seconds=900
    )
    too_soon_policy["prior_scan_event_id"] = seed_scan["event_id"]
    too_soon_scan = _scan(
        "2026-09-08T14:00:01Z",
        ndx_eligible=True,
        ndx_policy_observation=too_soon,
        ndx_candidate_policy_observation=too_soon,
        ndx_prior_confirmation_scan_event_id=seed_scan["event_id"],
        ndx_policy_payload=too_soon_policy,
        cadence_mode="NORMAL",
        cadence_evidence=_cadence_evidence(),
    )
    rejected_gap = commit_monitor_scan(
        scan=too_soon_scan,
        policy_inputs={"NDX": too_soon_policy},
        state_path=state_path,
        journal_dir=tmp_path,
    )
    assert rejected_gap["accepted"] is False
    assert rejected_gap["issues"] == [
        "symbols.NDX.confirmation_gap_below_minimum_seconds"
    ]
    assert state_path.read_bytes() == state_before
    assert journal_path.read_bytes() == journal_before

    latest = _observation("2026-09-08T14:15:00Z")
    fabricated_prior = copy.deepcopy(seed_observation)
    fabricated_prior["spot"] = float(fabricated_prior["spot"]) + 1.0
    fabricated_policy = _policy_input(
        fabricated_prior, latest, cadence_seconds=900
    )
    fabricated_policy["prior_scan_event_id"] = seed_scan["event_id"]
    fabricated_scan = _scan(
        "2026-09-08T14:15:00Z",
        ndx_eligible=True,
        ndx_policy_observation=latest,
        ndx_candidate_policy_observation=latest,
        ndx_prior_confirmation_scan_event_id=seed_scan["event_id"],
        ndx_policy_payload=fabricated_policy,
        cadence_mode="NORMAL",
        cadence_evidence=_cadence_evidence(),
    )
    rejected_fabrication = commit_monitor_scan(
        scan=fabricated_scan,
        policy_inputs={"NDX": fabricated_policy},
        state_path=state_path,
        journal_dir=tmp_path,
    )
    assert rejected_fabrication["accepted"] is False
    assert rejected_fabrication["issues"] == [
        "policy_inputs.NDX.prior_observation_not_anchored_to_previous_committed_scan"
    ]
    assert state_path.read_bytes() == state_before
    assert journal_path.read_bytes() == journal_before


def test_scan_then_full_helper_events_are_durable_before_state(tmp_path: Path) -> None:
    state_path = tmp_path / "state.json"
    _write_state(state_path, policy_state=_baseline_state())
    first = _observation(
        "2026-09-08T14:10:00Z", gamma_pin=29_700.0, max_pain=29_500.0
    )
    second = _observation(
        "2026-09-08T14:15:00Z", gamma_pin=29_700.0, max_pain=29_500.0
    )
    scan = _scan(
        ndx_eligible=True,
        ndx_policy_observation=second,
        ndx_policy_payload=_policy_input(first, second),
    )

    result = commit_monitor_scan(
        scan=scan,
        policy_inputs={"NDX": _policy_input(first, second)},
        state_path=state_path,
        journal_dir=tmp_path,
    )

    assert result["accepted"] is True
    assert result["action"] == "committed"
    rows = _journal_rows(tmp_path / "2026-09-08.jsonl")
    assert rows[0] == scan
    helper_rows = [row for row in rows if "helper_event" in row]
    assert {row["event_type"] for row in helper_rows} == {
        "MAX_PAIN_CHANGE",
        "GAMMA_PIN_SHIFT",
    }
    for row in helper_rows:
        assert row["event_id"] == row["helper_event"]["event_id"]
        assert row["alerts"] == [row["helper_event"]]
        assert row["parent_scan_event_id"] == scan["event_id"]
    receipt = rows[-1]
    assert receipt["event_type"] == "substantive_scan_commit"
    assert receipt["ordered_policy_event_ids"] == [
        row["event_id"] for row in helper_rows
    ]
    state = json.loads(state_path.read_text("utf-8"))
    assert state["monitor_scan_ledger"]["last_committed_scan_event_id"] == scan[
        "event_id"
    ]
    assert state["policy_evaluator"]["NDX"]["levels"]["gamma_pin"]["value"] == 29_700.0


def test_receipt_first_recovery_dedupes_after_state_write_crash(tmp_path: Path) -> None:
    state_path = tmp_path / "state.json"
    original_state = _write_state(state_path, policy_state=_baseline_state())
    first = _observation(
        "2026-09-08T14:10:00Z", gamma_pin=29_700.0, max_pain=29_500.0
    )
    second = _observation(
        "2026-09-08T14:15:00Z", gamma_pin=29_700.0, max_pain=29_500.0
    )
    scan = _scan(
        ndx_eligible=True,
        ndx_policy_observation=second,
        ndx_policy_payload=_policy_input(first, second),
    )

    def crash(stage: str) -> None:
        if stage == "before_state_replace":
            raise OSError("simulated crash")

    failed = commit_monitor_scan(
        scan=scan,
        policy_inputs={"NDX": _policy_input(first, second)},
        state_path=state_path,
        journal_dir=tmp_path,
        failpoint=crash,
    )
    assert failed["accepted"] is False
    assert json.loads(state_path.read_text("utf-8")) == original_state
    durable_before_retry = _journal_rows(tmp_path / "2026-09-08.jsonl")
    assert len(durable_before_retry) == 4

    recovered = commit_monitor_scan(
        scan=scan,
        policy_inputs={"NDX": _policy_input(first, second)},
        state_path=state_path,
        journal_dir=tmp_path,
    )

    assert recovered["accepted"] is True
    assert recovered["action"] == "recovered_and_committed"
    assert len(recovered["event_ids_recovered"]) == 2
    assert _journal_rows(tmp_path / "2026-09-08.jsonl") == durable_before_retry
    state = json.loads(state_path.read_text("utf-8"))
    assert state["policy_evaluator"]["NDX"]["levels"]["max_pain"]["value"] == 29_500.0


def test_partial_event_append_recovers_and_appends_only_missing_event(
    tmp_path: Path,
) -> None:
    state_path = tmp_path / "state.json"
    original_state = _write_state(state_path, policy_state=_baseline_state())
    first = _observation(
        "2026-09-08T14:10:00Z", gamma_pin=29_700.0, max_pain=29_500.0
    )
    second = _observation(
        "2026-09-08T14:15:00Z", gamma_pin=29_700.0, max_pain=29_500.0
    )
    scan = _scan(
        ndx_eligible=True,
        ndx_policy_observation=second,
        ndx_policy_payload=_policy_input(first, second),
    )
    appended_events = 0

    def crash_after_first_event(stage: str) -> None:
        nonlocal appended_events
        if stage == "after_event_append":
            appended_events += 1
            if appended_events == 1:
                raise OSError("simulated partial append crash")

    failed = commit_monitor_scan(
        scan=scan,
        policy_inputs={"NDX": _policy_input(first, second)},
        state_path=state_path,
        journal_dir=tmp_path,
        failpoint=crash_after_first_event,
    )
    assert failed["accepted"] is False
    assert json.loads(state_path.read_text("utf-8")) == original_state
    journal_path = tmp_path / "2026-09-08.jsonl"
    partial = _journal_rows(journal_path)
    assert len(partial) == 2

    recovered = commit_monitor_scan(
        scan=scan,
        policy_inputs={"NDX": _policy_input(first, second)},
        state_path=state_path,
        journal_dir=tmp_path,
    )
    final_rows = _journal_rows(journal_path)
    assert recovered["accepted"] is True
    assert recovered["action"] == "recovered_and_committed"
    assert len(recovered["event_ids_recovered"]) == 1
    assert len(final_rows) == 4
    assert len({row["event_id"] for row in final_rows}) == 4


def test_orphan_helper_outer_wrapper_mutation_cannot_be_laundered_by_retry(
    tmp_path: Path,
) -> None:
    state_path = tmp_path / "state.json"
    original_state = _write_state(state_path, policy_state=_baseline_state())
    first = _observation(
        "2026-09-08T14:10:00Z", gamma_pin=29_700.0, max_pain=29_500.0
    )
    latest = _observation(
        "2026-09-08T14:15:00Z", gamma_pin=29_700.0, max_pain=29_500.0
    )
    payload = _policy_input(first, latest)
    scan = _scan(
        ndx_eligible=True,
        ndx_policy_observation=latest,
        ndx_policy_payload=payload,
    )
    appended_helpers = 0

    def crash(stage: str) -> None:
        nonlocal appended_helpers
        if stage == "after_event_append":
            appended_helpers += 1
            if appended_helpers == 1:
                raise OSError("simulated orphan helper")

    failed = commit_monitor_scan(
        scan=scan,
        policy_inputs={"NDX": payload},
        state_path=state_path,
        journal_dir=tmp_path,
        failpoint=crash,
    )
    assert failed["accepted"] is False
    journal_path = tmp_path / "2026-09-08.jsonl"
    rows = _journal_rows(journal_path)
    assert len(rows) == 2
    orphan_wrapper = rows[1]
    orphan_wrapper["directional_interpretation"] = "FORGED_OUTER_VALUE"
    mutated = "".join(json.dumps(row) + "\n" for row in rows)
    journal_path.write_text(mutated, encoding="utf-8")

    rejected = commit_monitor_scan(
        scan=scan,
        policy_inputs={"NDX": payload},
        state_path=state_path,
        journal_dir=tmp_path,
    )

    assert rejected["accepted"] is False
    assert rejected["issues"] == [
        "existing_policy_wrapper_payload_mismatch:" + orphan_wrapper["event_id"]
    ]
    assert journal_path.read_text("utf-8") == mutated
    assert json.loads(state_path.read_text("utf-8")) == original_state


def test_tampered_helper_wrapper_fails_closed_before_diagnostic_append(
    tmp_path: Path,
) -> None:
    state_path = tmp_path / "state.json"
    original_state = _write_state(state_path)
    policy_result = evaluate_monitor_policy(
        {
            "schema_version": INPUT_SCHEMA,
            "observations": [
                _observation("2026-09-08T14:10:00Z", gamma_pin=29_700.0),
                _observation("2026-09-08T14:15:00Z", gamma_pin=29_700.0),
            ],
            "policy_state": _baseline_state(),
            "confirmation_cadence_seconds": 300,
        }
    )
    helper = next(
        event
        for event in policy_result["events"]
        if event["type"] == "GAMMA_PIN_SHIFT"
    )
    tampered = {
        "schema_version": 2,
        "event_id": "c" * 64,
        "event_type": "GAMMA_PIN_SHIFT",
        "observed_at_ct": helper["confirmed_at_ct"],
        "observed_at_utc": helper["confirmed_at_utc"],
        "session_date": "2026-09-08",
        "phase": "regular-session",
        "cadence": {"mode": "ELEVATED"},
        "evidence": [],
        "symbols": {"NDX": {"eligible": True}},
        "alerts": [helper],
        "directional_interpretation": "GAMMA_PIN_SHIFT",
        "research_hypotheses": [],
        "helper_event": helper,
    }
    journal_path = tmp_path / "2026-09-08.jsonl"
    journal_path.write_text(json.dumps(tampered) + "\n", encoding="utf-8")
    scan = _scan(ndx_eligible=False)

    result = commit_monitor_scan(
        scan=scan,
        policy_inputs={},
        state_path=state_path,
        journal_dir=tmp_path,
    )

    assert result["accepted"] is False
    assert result["issues"] == ["policy_alert_wrapper_event_id_mismatch"]
    assert _journal_rows(journal_path) == [tampered]
    assert json.loads(state_path.read_text("utf-8")) == original_state


def test_semantically_tampered_helper_event_id_fails_closed(tmp_path: Path) -> None:
    state_path = tmp_path / "state.json"
    original_state = _write_state(state_path, policy_state=_baseline_state())
    first = _observation(
        "2026-09-08T14:10:00Z", gamma_pin=29_700.0, max_pain=29_500.0
    )
    second = _observation(
        "2026-09-08T14:15:00Z", gamma_pin=29_700.0, max_pain=29_500.0
    )
    scan = _scan(
        ndx_eligible=True,
        ndx_policy_observation=second,
        ndx_policy_payload=_policy_input(first, second),
    )

    def crash(stage: str) -> None:
        if stage == "before_state_replace":
            raise OSError("simulated crash")

    failed = commit_monitor_scan(
        scan=scan,
        policy_inputs={"NDX": _policy_input(first, second)},
        state_path=state_path,
        journal_dir=tmp_path,
        failpoint=crash,
    )
    assert failed["accepted"] is False
    journal_path = tmp_path / "2026-09-08.jsonl"
    rows = _journal_rows(journal_path)
    gamma_wrapper = next(
        row for row in rows if row["event_type"] == "GAMMA_PIN_SHIFT"
    )
    gamma_wrapper["helper_event"]["new_value"] += 25.0
    gamma_wrapper["alerts"] = [copy.deepcopy(gamma_wrapper["helper_event"])]
    journal_path.write_text(
        "".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8"
    )

    rejected = commit_monitor_scan(
        scan=scan,
        policy_inputs={"NDX": _policy_input(first, second)},
        state_path=state_path,
        journal_dir=tmp_path,
    )
    assert rejected["accepted"] is False
    assert rejected["issues"] == ["helper_event_semantic_event_id_mismatch"]
    assert json.loads(state_path.read_text("utf-8")) == original_state


def test_malformed_journal_and_scan_id_mismatch_fail_closed(tmp_path: Path) -> None:
    state_path = tmp_path / "state.json"
    original_state = _write_state(state_path)
    bad_scan = _scan(ndx_eligible=False)
    bad_scan["event_id"] = "0" * 64
    mismatch = commit_monitor_scan(
        scan=bad_scan,
        policy_inputs={},
        state_path=state_path,
        journal_dir=tmp_path,
    )
    assert mismatch["accepted"] is False
    assert mismatch["issues"] == ["scan_event_id_mismatch"]

    journal_path = tmp_path / "2026-09-08.jsonl"
    journal_path.write_text(json.dumps(_scan(ndx_eligible=False)), encoding="utf-8")
    unterminated = commit_monitor_scan(
        scan=_scan(ndx_eligible=False),
        policy_inputs={},
        state_path=state_path,
        journal_dir=tmp_path,
    )
    assert unterminated["accepted"] is False
    assert unterminated["issues"] == [
        "partial_journal_tail_not_exact_transaction_prefix"
    ]

    journal_path.write_text("{not-json\n", encoding="utf-8")
    malformed = commit_monitor_scan(
        scan=_scan(ndx_eligible=False),
        policy_inputs={},
        state_path=state_path,
        journal_dir=tmp_path,
    )
    assert malformed["accepted"] is False
    assert malformed["issues"] == ["journal_line_1_malformed"]
    assert json.loads(state_path.read_text("utf-8")) == original_state


def test_idempotent_committed_scan_does_not_duplicate_or_reinvoke_policy(
    tmp_path: Path,
) -> None:
    state_path = tmp_path / "state.json"
    _write_state(state_path)
    scan = _scan(ndx_eligible=False)
    first = commit_monitor_scan(
        scan=scan,
        policy_inputs={},
        state_path=state_path,
        journal_dir=tmp_path,
    )
    assert first["accepted"] is True

    def forbidden(_payload):
        raise AssertionError("idempotent replay must not invoke policy")

    second = commit_monitor_scan(
        scan=scan,
        policy_inputs={"NDX": {"malformed": True}},
        state_path=state_path,
        journal_dir=tmp_path,
        evaluator=forbidden,
    )
    assert second["accepted"] is True
    assert second["action"] == "already_committed"
    assert len(_journal_rows(tmp_path / "2026-09-08.jsonl")) == 2


def test_idempotent_state_rejects_missing_committed_helper_receipt(
    tmp_path: Path,
) -> None:
    state_path = tmp_path / "state.json"
    _write_state(state_path, policy_state=_baseline_state())
    first = _observation(
        "2026-09-08T14:10:00Z", gamma_pin=29_700.0, max_pain=29_500.0
    )
    second = _observation(
        "2026-09-08T14:15:00Z", gamma_pin=29_700.0, max_pain=29_500.0
    )
    scan = _scan(
        ndx_eligible=True,
        ndx_policy_observation=second,
        ndx_policy_payload=_policy_input(first, second),
    )
    committed = commit_monitor_scan(
        scan=scan,
        policy_inputs={"NDX": _policy_input(first, second)},
        state_path=state_path,
        journal_dir=tmp_path,
    )
    assert committed["accepted"] is True
    journal_path = tmp_path / "2026-09-08.jsonl"
    journal_path.write_text(json.dumps(scan) + "\n", encoding="utf-8")

    retry = commit_monitor_scan(
        scan=scan,
        policy_inputs={"NDX": _policy_input(first, second)},
        state_path=state_path,
        journal_dir=tmp_path,
    )
    assert retry["accepted"] is False
    assert retry["issues"] == ["monitor_scan_ledger_commit_receipt_missing"]


def test_scan_uses_exclusive_shared_rollover_lock(tmp_path: Path) -> None:
    state_path = tmp_path / "state.json"
    original_state = _write_state(state_path)
    shared_lock = state_path.with_name(f"{state_path.name}.rollover.lock")

    with scan_ledger._exclusive_lock(shared_lock):
        result = commit_monitor_scan(
            scan=_scan(ndx_eligible=False),
            policy_inputs={},
            state_path=state_path,
            journal_dir=tmp_path,
        )

    assert result["accepted"] is False
    assert result["issues"] == ["monitor_scan_lock_unavailable"]
    assert not (tmp_path / "2026-09-08.jsonl").exists()
    assert json.loads(state_path.read_text("utf-8")) == original_state


def test_mixed_scan_preflight_failure_writes_nothing(
    tmp_path: Path,
) -> None:
    state_path = tmp_path / "state.json"
    original_state = _write_state(state_path, policy_state=_baseline_state())
    latest = _observation("2026-09-08T14:15:00Z")
    scan = _scan(
        ndx_eligible=True,
        ndx_policy_observation=latest,
        ndx_policy_payload=_policy_input(
            _observation("2026-09-08T14:10:00Z"), latest
        ),
    )

    result = commit_monitor_scan(
        scan=scan,
        policy_inputs={},
        state_path=state_path,
        journal_dir=tmp_path,
    )

    assert result["accepted"] is False
    assert result["issues"] == ["policy_inputs.NDX_required_for_eligible_symbol"]
    assert result["diagnostic_symbols_retained"] == []
    assert not (tmp_path / "2026-09-08.jsonl").exists()
    assert json.loads(state_path.read_text("utf-8")) == original_state


def test_malformed_policy_preflight_writes_nothing_then_corrected_scan_commits(
    tmp_path: Path,
) -> None:
    state_path = tmp_path / "state.json"
    original_state = _write_state(state_path, policy_state=_baseline_state())
    latest = _observation("2026-09-08T14:15:00Z")
    malformed_payload = _policy_input(latest)
    malformed_scan = _scan(
        ndx_eligible=True,
        ndx_policy_observation=latest,
        ndx_policy_payload=malformed_payload,
    )

    rejected = commit_monitor_scan(
        scan=malformed_scan,
        policy_inputs={"NDX": malformed_payload},
        state_path=state_path,
        journal_dir=tmp_path,
    )

    assert rejected["accepted"] is False
    assert rejected["issues"] == [
        "policy_inputs.NDX.observations_must_have_exactly_two"
    ]
    assert not (tmp_path / "2026-09-08.jsonl").exists()
    assert json.loads(state_path.read_text("utf-8")) == original_state

    first = _observation("2026-09-08T14:10:00Z")
    corrected_payload = _policy_input(first, latest)
    corrected_scan = _scan(
        ndx_eligible=True,
        ndx_policy_observation=latest,
        ndx_policy_payload=corrected_payload,
    )
    corrected = commit_monitor_scan(
        scan=corrected_scan,
        policy_inputs={"NDX": corrected_payload},
        state_path=state_path,
        journal_dir=tmp_path,
    )
    assert corrected["accepted"] is True


def test_second_symbol_preflight_rejection_makes_zero_writes(tmp_path: Path) -> None:
    state_path = tmp_path / "state.json"
    original_state = _write_state(state_path, policy_state=_baseline_state())
    spx_first = _observation(
        "2026-09-08T14:10:00Z",
        symbol="SPX",
        spot=7_000.0,
        gamma_pin=7_025.0,
        max_pain=6_990.0,
    )
    spx_latest = _observation(
        "2026-09-08T14:15:00Z",
        symbol="SPX",
        spot=7_000.0,
        gamma_pin=7_025.0,
        max_pain=6_990.0,
    )
    spx_payload = _policy_input(spx_first, spx_latest)
    ndx_latest = _observation("2026-09-08T14:15:00Z")
    invalid_ndx_payload = _policy_input(ndx_latest)
    scan = _scan(
        ndx_eligible=True,
        spx_eligible=True,
        ndx_policy_observation=ndx_latest,
        spx_policy_observation=spx_latest,
        ndx_policy_payload=invalid_ndx_payload,
        spx_policy_payload=spx_payload,
    )

    result = commit_monitor_scan(
        scan=scan,
        policy_inputs={"SPX": spx_payload, "NDX": invalid_ndx_payload},
        state_path=state_path,
        journal_dir=tmp_path,
    )

    assert result["accepted"] is False
    assert result["issues"] == [
        "policy_inputs.NDX.observations_must_have_exactly_two"
    ]
    assert result["policy_symbols_evaluated"] == ["SPX"]
    assert not (tmp_path / "2026-09-08.jsonl").exists()
    assert json.loads(state_path.read_text("utf-8")) == original_state


def test_evaluator_rejection_preflight_makes_zero_writes(tmp_path: Path) -> None:
    state_path = tmp_path / "state.json"
    original_state = _write_state(state_path, policy_state=_baseline_state())
    first = _observation("2026-09-08T14:10:00Z")
    latest = _observation("2026-09-08T14:15:00Z")
    policy_payload = _policy_input(first, latest)
    scan = _scan(
        ndx_eligible=True,
        ndx_policy_observation=latest,
        ndx_policy_payload=policy_payload,
    )

    def reject(_payload):
        return {
            "schema_version": scan_ledger.POLICY_OUTPUT_SCHEMA,
            "accepted": False,
            "issues": ["simulated_rejection"],
        }

    result = commit_monitor_scan(
        scan=scan,
        policy_inputs={"NDX": policy_payload},
        state_path=state_path,
        journal_dir=tmp_path,
        evaluator=reject,
    )

    assert result["accepted"] is False
    assert result["issues"] == [
        "policy_result.NDX_rejected:simulated_rejection"
    ]
    assert not (tmp_path / "2026-09-08.jsonl").exists()
    assert json.loads(state_path.read_text("utf-8")) == original_state


def test_scan_and_policy_latest_observation_are_cross_bound(tmp_path: Path) -> None:
    state_path = tmp_path / "state.json"
    original_state = _write_state(state_path)
    first = _observation("2026-09-08T14:10:00Z")
    scan_latest = _observation("2026-09-08T14:15:00Z", gamma_pin=29_700.0)
    detached_latest = _observation("2026-09-08T14:15:00Z", gamma_pin=29_725.0)
    scan = _scan(
        ndx_eligible=True,
        ndx_policy_observation=scan_latest,
        ndx_policy_payload=_policy_input(first, scan_latest),
    )

    result = commit_monitor_scan(
        scan=scan,
        policy_inputs={"NDX": _policy_input(first, detached_latest)},
        state_path=state_path,
        journal_dir=tmp_path,
    )

    assert result["accepted"] is False
    assert result["issues"] == ["policy_inputs.NDX.sha256_scan_payload_mismatch"]
    assert not (tmp_path / "2026-09-08.jsonl").exists()
    assert json.loads(state_path.read_text("utf-8")) == original_state


def test_scan_top_level_levels_cannot_disagree_with_embedded_observation(
    tmp_path: Path,
) -> None:
    state_path = tmp_path / "state.json"
    original_state = _write_state(state_path)
    latest = _observation("2026-09-08T14:15:00Z", gamma_pin=29_700.0)
    scan = _scan(
        ndx_eligible=True,
        ndx_policy_observation=latest,
        ndx_policy_payload=_policy_input(
            _observation("2026-09-08T14:10:00Z"), latest
        ),
    )
    scan["symbols"]["NDX"]["gamma_pin"] = 1.0
    scan.pop("event_id")
    scan = with_scan_event_id(scan)

    result = commit_monitor_scan(
        scan=scan,
        policy_inputs={"NDX": _policy_input(_observation("2026-09-08T14:10:00Z"), latest)},
        state_path=state_path,
        journal_dir=tmp_path,
    )

    assert result["accepted"] is False
    assert result["issues"] == [
        "symbols.NDX.gamma_pin_policy_observation_mismatch"
    ]
    assert not (tmp_path / "2026-09-08.jsonl").exists()
    assert json.loads(state_path.read_text("utf-8")) == original_state


def test_policy_cadence_must_match_scan_mode(tmp_path: Path) -> None:
    state_path = tmp_path / "state.json"
    original_state = _write_state(state_path)
    first = _observation("2026-09-08T14:00:00Z")
    latest = _observation("2026-09-08T14:15:00Z")
    scan = _scan(
        ndx_eligible=True,
        ndx_policy_observation=latest,
        ndx_policy_payload=_policy_input(first, latest, cadence_seconds=900),
    )

    result = commit_monitor_scan(
        scan=scan,
        policy_inputs={"NDX": _policy_input(first, latest, cadence_seconds=900)},
        state_path=state_path,
        journal_dir=tmp_path,
    )

    assert result["accepted"] is False
    assert result["issues"] == [
        "policy_inputs.NDX.confirmation_cadence_scan_mode_mismatch"
    ]
    assert not (tmp_path / "2026-09-08.jsonl").exists()
    assert json.loads(state_path.read_text("utf-8")) == original_state


def test_stale_latest_observation_cannot_drive_alert(tmp_path: Path) -> None:
    state_path = tmp_path / "state.json"
    original_state = _write_state(state_path)
    first = _observation("2026-09-08T14:09:00Z")
    latest = _observation("2026-09-08T14:14:00Z")
    scan = _scan(
        "2026-09-08T14:20:00Z",
        ndx_eligible=True,
        ndx_policy_observation=latest,
        ndx_policy_payload=_policy_input(first, latest),
    )

    result = commit_monitor_scan(
        scan=scan,
        policy_inputs={"NDX": _policy_input(first, latest)},
        state_path=state_path,
        journal_dir=tmp_path,
    )

    assert result["accepted"] is False
    assert result["issues"] == [
        "policy_inputs.NDX.latest_observation_stale_for_scan_cadence"
    ]
    assert not (tmp_path / "2026-09-08.jsonl").exists()
    assert json.loads(state_path.read_text("utf-8")) == original_state


def test_older_retry_fails_closed_and_cannot_regress_state(
    tmp_path: Path,
) -> None:
    state_path = tmp_path / "state.json"
    _write_state(state_path)
    older = _scan(ndx_eligible=False)
    newer = _scan("2026-09-08T14:20:00Z", ndx_eligible=False)
    assert commit_monitor_scan(
        scan=older, policy_inputs={}, state_path=state_path, journal_dir=tmp_path
    )["accepted"] is True
    assert commit_monitor_scan(
        scan=newer, policy_inputs={}, state_path=state_path, journal_dir=tmp_path
    )["accepted"] is True
    state_after_newer = json.loads(state_path.read_text("utf-8"))

    retry = commit_monitor_scan(
        scan=older, policy_inputs={}, state_path=state_path, journal_dir=tmp_path
    )

    assert retry["accepted"] is False
    assert retry["action"] == "abstain"
    assert retry["issues"] == ["scan_observed_at_not_after_state_last_scan"]
    assert json.loads(state_path.read_text("utf-8")) == state_after_newer
    assert len(_journal_rows(tmp_path / "2026-09-08.jsonl")) == 4


def test_scan_cadence_must_match_valid_state_mode_before_append(
    tmp_path: Path,
) -> None:
    state_path = tmp_path / "state.json"
    state = _write_state(state_path)
    state["mode"] = "NORMAL"
    state_path.write_text(json.dumps(state), encoding="utf-8")

    mismatch = commit_monitor_scan(
        scan=_scan(ndx_eligible=False, cadence_mode="ELEVATED"),
        policy_inputs={},
        state_path=state_path,
        journal_dir=tmp_path,
    )

    assert mismatch["accepted"] is False
    assert mismatch["issues"] == ["scan_cadence_state_mode_mismatch"]
    assert not (tmp_path / "2026-09-08.jsonl").exists()

    state["mode"] = "PAUSED"
    state_path.write_text(json.dumps(state), encoding="utf-8")
    invalid = commit_monitor_scan(
        scan=_scan(ndx_eligible=False, cadence_mode="ELEVATED"),
        policy_inputs={},
        state_path=state_path,
        journal_dir=tmp_path,
    )

    assert invalid["accepted"] is False
    assert invalid["issues"] == ["state_mode_must_be_NORMAL_or_ELEVATED"]
    assert not (tmp_path / "2026-09-08.jsonl").exists()


def test_arbitrary_event_id_cannot_suppress_required_helper_wrapper(
    tmp_path: Path,
) -> None:
    state_path = tmp_path / "state.json"
    baseline = _baseline_state()
    original_state = _write_state(state_path, policy_state=baseline)
    first = _observation(
        "2026-09-08T14:10:00Z", gamma_pin=29_700.0, max_pain=29_500.0
    )
    latest = _observation(
        "2026-09-08T14:15:00Z", gamma_pin=29_700.0, max_pain=29_500.0
    )
    policy_result = evaluate_monitor_policy(
        {
            **_policy_input(first, latest),
            "policy_state": baseline,
        }
    )
    event_id = next(
        event["event_id"]
        for event in policy_result["events"]
        if event["type"] == "GAMMA_PIN_SHIFT"
    )
    junk = {"schema_version": 1, "event_id": event_id, "legacy": "junk"}
    journal_path = tmp_path / "2026-09-08.jsonl"
    journal_path.write_text(json.dumps(junk) + "\n", encoding="utf-8")
    scan = _scan(
        ndx_eligible=True,
        ndx_policy_observation=latest,
        ndx_policy_payload=_policy_input(first, latest),
    )

    result = commit_monitor_scan(
        scan=scan,
        policy_inputs={"NDX": _policy_input(first, latest)},
        state_path=state_path,
        journal_dir=tmp_path,
    )

    assert result["accepted"] is False
    assert result["issues"] == [
        f"policy_event_id_collision_with_nonreceipt:{event_id}"
    ]
    assert _journal_rows(journal_path) == [junk]
    assert json.loads(state_path.read_text("utf-8")) == original_state


def test_idempotent_retry_detects_full_wrapper_and_ledger_hash_tampering(
    tmp_path: Path,
) -> None:
    state_path = tmp_path / "state.json"
    _write_state(state_path, policy_state=_baseline_state())
    first = _observation(
        "2026-09-08T14:10:00Z", gamma_pin=29_700.0, max_pain=29_500.0
    )
    latest = _observation(
        "2026-09-08T14:15:00Z", gamma_pin=29_700.0, max_pain=29_500.0
    )
    scan = _scan(
        ndx_eligible=True,
        ndx_policy_observation=latest,
        ndx_policy_payload=_policy_input(first, latest),
    )
    assert commit_monitor_scan(
        scan=scan,
        policy_inputs={"NDX": _policy_input(first, latest)},
        state_path=state_path,
        journal_dir=tmp_path,
    )["accepted"] is True

    journal_path = tmp_path / "2026-09-08.jsonl"
    rows = _journal_rows(journal_path)
    gamma = next(row for row in rows if row["event_type"] == "GAMMA_PIN_SHIFT")
    gamma["evidence"] = ["tampered-after-commit"]
    journal_path.write_text(
        "".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8"
    )
    wrapper_tamper = commit_monitor_scan(
        scan=scan,
        policy_inputs={"NDX": _policy_input(first, latest)},
        state_path=state_path,
        journal_dir=tmp_path,
    )
    assert wrapper_tamper["accepted"] is False
    assert wrapper_tamper["issues"] == [
        "policy_alert_wrapper_parent_evidence_mismatch"
    ]

    # Restore the journal, then prove changing only the ledger's stored hash is
    # also rejected on the idempotent path.
    gamma["evidence"] = copy.deepcopy(scan["evidence"])
    journal_path.write_text(
        "".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8"
    )
    state = json.loads(state_path.read_text("utf-8"))
    state["monitor_scan_ledger"]["last_committed_policy_record_sha256"][
        gamma["event_id"]
    ] = "0" * 64
    state_path.write_text(json.dumps(state), encoding="utf-8")
    state_tamper = commit_monitor_scan(
        scan=scan,
        policy_inputs={"NDX": _policy_input(first, latest)},
        state_path=state_path,
        journal_dir=tmp_path,
    )
    assert state_tamper["accepted"] is False
    assert state_tamper["issues"] == ["committed_scan_policy_receipt_tampered"]


def test_journal_mutation_during_commit_prevents_state_replace(tmp_path: Path) -> None:
    state_path = tmp_path / "state.json"
    original_state = _write_state(state_path)
    scan = _scan(ndx_eligible=False)
    journal_path = tmp_path / "2026-09-08.jsonl"

    def mutate_journal(stage: str) -> None:
        if stage == "before_state_replace":
            with journal_path.open("a", encoding="utf-8") as handle:
                handle.write(json.dumps({"schema_version": 1, "race": True}) + "\n")

    result = commit_monitor_scan(
        scan=scan,
        policy_inputs={},
        state_path=state_path,
        journal_dir=tmp_path,
        failpoint=mutate_journal,
    )

    assert result["accepted"] is False
    assert result["issues"] == ["journal_changed_during_scan_commit"]
    assert json.loads(state_path.read_text("utf-8")) == original_state


def test_orphan_scan_blocks_overtake_until_exact_retry_completes(
    tmp_path: Path,
) -> None:
    state_path = tmp_path / "state.json"
    original_state = _write_state(state_path, policy_state=_baseline_state())
    first = _observation(
        "2026-09-08T14:10:00Z", gamma_pin=29_700.0, max_pain=29_500.0
    )
    latest = _observation(
        "2026-09-08T14:15:00Z", gamma_pin=29_700.0, max_pain=29_500.0
    )
    orphan = _scan(
        ndx_eligible=True,
        ndx_policy_observation=latest,
        ndx_policy_payload=_policy_input(first, latest),
    )

    def crash_after_scan(stage: str) -> None:
        if stage == "after_scan_append":
            raise OSError("simulated orphan scan crash")

    failed = commit_monitor_scan(
        scan=orphan,
        policy_inputs={"NDX": _policy_input(first, latest)},
        state_path=state_path,
        journal_dir=tmp_path,
        failpoint=crash_after_scan,
    )
    assert failed["accepted"] is False
    assert json.loads(state_path.read_text("utf-8")) == original_state
    journal_path = tmp_path / "2026-09-08.jsonl"
    assert _journal_rows(journal_path) == [orphan]

    newer = _scan("2026-09-08T14:20:00Z", ndx_eligible=False)
    overtaking = commit_monitor_scan(
        scan=newer,
        policy_inputs={},
        state_path=state_path,
        journal_dir=tmp_path,
    )
    assert overtaking["accepted"] is False
    assert overtaking["issues"] == [
        "unreflected_substantive_scan_requires_exact_retry:" + orphan["event_id"]
    ]
    assert _journal_rows(journal_path) == [orphan]
    assert json.loads(state_path.read_text("utf-8")) == original_state

    altered_first = copy.deepcopy(first)
    altered_first["spot"] += 25.0
    altered_retry = commit_monitor_scan(
        scan=orphan,
        policy_inputs={"NDX": _policy_input(altered_first, latest)},
        state_path=state_path,
        journal_dir=tmp_path,
    )
    assert altered_retry["accepted"] is False
    assert altered_retry["issues"] == [
        "policy_inputs.NDX.sha256_scan_payload_mismatch"
    ]
    assert _journal_rows(journal_path) == [orphan]
    assert json.loads(state_path.read_text("utf-8")) == original_state

    recovered = commit_monitor_scan(
        scan=orphan,
        policy_inputs={"NDX": _policy_input(first, latest)},
        state_path=state_path,
        journal_dir=tmp_path,
    )
    assert recovered["accepted"] is True
    assert recovered["action"] == "recovered_and_committed"
    recovered_state = json.loads(state_path.read_text("utf-8"))
    assert recovered_state["monitor_scan_ledger"]["committed_scan_event_ids"] == [
        orphan["event_id"]
    ]

    after_recovery = commit_monitor_scan(
        scan=newer,
        policy_inputs={},
        state_path=state_path,
        journal_dir=tmp_path,
    )
    assert after_recovery["accepted"] is True


def test_forged_directional_orphan_receipt_cannot_steer_clean_retry(
    tmp_path: Path,
) -> None:
    state_path = tmp_path / "state.json"
    baseline = _baseline_state()
    original_state = _write_state(state_path, policy_state=baseline)
    neutral_first = _observation(
        "2026-09-08T14:10:00Z", gamma_pin=29_600.0, max_pain=29_480.0
    )
    neutral_latest = _observation(
        "2026-09-08T14:15:00Z", gamma_pin=29_600.0, max_pain=29_480.0
    )
    neutral_payload = _policy_input(neutral_first, neutral_latest)
    orphan_scan = _scan(
        ndx_eligible=True,
        ndx_policy_observation=neutral_latest,
        ndx_policy_payload=neutral_payload,
    )

    def crash(stage: str) -> None:
        if stage == "after_scan_append":
            raise OSError("simulated orphan scan")

    failed = commit_monitor_scan(
        scan=orphan_scan,
        policy_inputs={"NDX": neutral_payload},
        state_path=state_path,
        journal_dir=tmp_path,
        failpoint=crash,
    )
    assert failed["accepted"] is False

    forged_first = copy.deepcopy(neutral_first)
    forged_latest = copy.deepcopy(neutral_latest)
    forged_first["normalized_net_gex"] = 0.20
    forged_latest["normalized_net_gex"] = -0.20
    forged_first["price_change_15m_pct"] = -0.25
    forged_latest["price_change_15m_pct"] = -0.25
    forged_result = evaluate_monitor_policy(
        {
            **_policy_input(forged_first, forged_latest),
            "policy_state": copy.deepcopy(baseline),
        }
    )
    forged_directional = next(
        event
        for event in forged_result["events"]
        if event["type"] == "DIRECTIONAL_SHIFT"
    )
    forged_wrapper = scan_ledger._build_alert_wrapper(
        orphan_scan, forged_directional
    )
    journal_path = tmp_path / "2026-09-08.jsonl"
    scan_ledger._append_jsonl_durable(journal_path, forged_wrapper)

    rejected = commit_monitor_scan(
        scan=orphan_scan,
        policy_inputs={"NDX": neutral_payload},
        state_path=state_path,
        journal_dir=tmp_path,
    )

    assert rejected["accepted"] is False
    assert rejected["issues"] == [
        "unreflected_policy_wrapper_not_causally_recovered"
    ]
    persisted = json.loads(state_path.read_text("utf-8"))
    assert persisted == original_state
    assert persisted["policy_evaluator"]["NDX"]["directional_latch"][
        "active_direction"
    ] is None
    assert not persisted["policy_evaluator"]["NDX"]["directional_latch"].get(
        "last_event_id"
    )
    assert "monitor_scan_ledger" not in persisted


def test_multiple_or_out_of_order_orphan_scans_fail_closed(tmp_path: Path) -> None:
    first_dir = tmp_path / "multiple"
    first_state = first_dir / "state.json"
    _write_state(first_state)
    older = _scan(ndx_eligible=False)
    newer = _scan("2026-09-08T14:20:00Z", ndx_eligible=False)
    journal_path = first_dir / "2026-09-08.jsonl"
    journal_path.write_text(
        "".join(json.dumps(row) + "\n" for row in (older, newer)),
        encoding="utf-8",
    )
    multiple = commit_monitor_scan(
        scan=older,
        policy_inputs={},
        state_path=first_state,
        journal_dir=first_dir,
    )
    assert multiple["accepted"] is False
    assert multiple["issues"] == ["multiple_unreflected_substantive_scans"]

    second_dir = tmp_path / "out-of-order"
    second_state = second_dir / "state.json"
    _write_state(second_state)
    second_journal = second_dir / "2026-09-08.jsonl"
    second_journal.write_text(
        "".join(json.dumps(row) + "\n" for row in (newer, older)),
        encoding="utf-8",
    )
    out_of_order = commit_monitor_scan(
        scan=older,
        policy_inputs={},
        state_path=second_state,
        journal_dir=second_dir,
    )
    assert out_of_order["accepted"] is False
    assert out_of_order["issues"] == [
        "journal_substantive_scans_out_of_order"
    ]


def test_unreflected_policy_wrapper_cannot_be_skipped_and_anchored(
    tmp_path: Path,
) -> None:
    state_path = tmp_path / "state.json"
    baseline = _baseline_state()
    _write_state(state_path, policy_state=baseline)
    first = _observation(
        "2026-09-08T14:10:00Z", gamma_pin=29_700.0, max_pain=29_500.0
    )
    latest = _observation(
        "2026-09-08T14:15:00Z", gamma_pin=29_700.0, max_pain=29_500.0
    )
    committed_scan = _scan(
        ndx_eligible=True,
        ndx_policy_observation=latest,
        ndx_policy_payload=_policy_input(first, latest),
    )
    assert commit_monitor_scan(
        scan=committed_scan,
        policy_inputs={"NDX": _policy_input(first, latest)},
        state_path=state_path,
        journal_dir=tmp_path,
    )["accepted"] is True

    orphan_latest = _observation(
        "2026-09-08T14:20:00Z", gamma_pin=29_700.0, max_pain=29_500.0
    )
    orphan_scan = _scan(
        "2026-09-08T14:20:00Z",
        ndx_eligible=True,
        ndx_policy_observation=orphan_latest,
        ndx_policy_payload=_policy_input(latest, orphan_latest),
    )
    stale_result = evaluate_monitor_policy(
        {
            **_policy_input(
                _observation("2026-09-08T14:06:00Z", gamma_pin=29_800.0),
                _observation("2026-09-08T14:10:00Z", gamma_pin=29_800.0),
            ),
            "policy_state": baseline,
        }
    )
    stale_event = next(
        event
        for event in stale_result["events"]
        if event["type"] == "GAMMA_PIN_SHIFT"
    )
    stale_wrapper = scan_ledger._build_alert_wrapper(orphan_scan, stale_event)
    journal_path = tmp_path / "2026-09-08.jsonl"
    scan_ledger._append_jsonl_durable(journal_path, orphan_scan)
    scan_ledger._append_jsonl_durable(journal_path, stale_wrapper)

    rejected = commit_monitor_scan(
        scan=orphan_scan,
        policy_inputs={"NDX": _policy_input(latest, orphan_latest)},
        state_path=state_path,
        journal_dir=tmp_path,
    )

    assert rejected["accepted"] is False
    assert rejected["issues"] == [
        "unreflected_policy_wrapper_not_causally_recovered"
    ]


def test_policy_state_hash_rejects_state_only_tampering(tmp_path: Path) -> None:
    state_path = tmp_path / "state.json"
    _write_state(state_path)
    scan = _scan(ndx_eligible=False)
    assert commit_monitor_scan(
        scan=scan,
        policy_inputs={},
        state_path=state_path,
        journal_dir=tmp_path,
    )["accepted"] is True
    state = json.loads(state_path.read_text("utf-8"))
    state["policy_evaluator"]["NDX"] = {"tampered": True}
    state_path.write_text(json.dumps(state), encoding="utf-8")

    rejected = commit_monitor_scan(
        scan=scan,
        policy_inputs={},
        state_path=state_path,
        journal_dir=tmp_path,
    )

    assert rejected["accepted"] is False
    assert rejected["issues"] == [
        "monitor_scan_ledger_policy_state_hash_mismatch"
    ]


def test_first_scan_validates_policy_state_against_rollover_archive(
    tmp_path: Path,
) -> None:
    state_path = tmp_path / "state.json"
    state = _write_state(state_path, policy_state=_baseline_state())
    archived = copy.deepcopy(state["policy_evaluator"])
    state["prior_session_reference"] = {
        "policy_evaluator_at_rollover": archived,
        "policy_evaluator_sha256": scan_ledger._canonical_hash(archived),
    }
    state["policy_evaluator"]["NDX"] = {"tampered_after_rollover": True}
    state_path.write_text(json.dumps(state), encoding="utf-8")

    result = commit_monitor_scan(
        scan=_scan(ndx_eligible=False),
        policy_inputs={},
        state_path=state_path,
        journal_dir=tmp_path,
    )

    assert result["accepted"] is False
    assert result["issues"] == [
        "first_scan_policy_evaluator_differs_from_rollover_archive"
    ]
    assert not (tmp_path / "2026-09-08.jsonl").exists()


def test_commit_receipt_is_authoritative_across_crash_before_state(
    tmp_path: Path,
) -> None:
    state_path = tmp_path / "state.json"
    original_state = _write_state(state_path, policy_state=_baseline_state())
    first = _observation(
        "2026-09-08T14:10:00Z", gamma_pin=29_700.0, max_pain=29_500.0
    )
    latest = _observation(
        "2026-09-08T14:15:00Z", gamma_pin=29_700.0, max_pain=29_500.0
    )
    policy_payload = _policy_input(first, latest)
    scan = _scan(
        ndx_eligible=True,
        ndx_policy_observation=latest,
        ndx_policy_payload=policy_payload,
    )

    def crash(stage: str) -> None:
        if stage == "after_commit_receipt_append":
            raise OSError("simulated receipt/state gap")

    failed = commit_monitor_scan(
        scan=scan,
        policy_inputs={"NDX": policy_payload},
        state_path=state_path,
        journal_dir=tmp_path,
        failpoint=crash,
    )

    assert failed["accepted"] is False
    assert failed["commit_phase"] == "journal_durable"
    assert failed["commit_receipt_appended"] is True
    assert json.loads(state_path.read_text("utf-8")) == original_state
    journal_path = tmp_path / "2026-09-08.jsonl"
    before_retry = journal_path.read_bytes()
    rows = _journal_rows(journal_path)
    receipt = rows[-1]
    assert receipt["event_type"] == "substantive_scan_commit"
    assert receipt["parent_scan_event_id"] == scan["event_id"]
    assert receipt["ordered_policy_event_ids"] == [
        row["event_id"] for row in rows if "helper_event" in row
    ]
    assert receipt["alerts"] == []
    assert receipt["symbols"] == {}
    assert receipt["directional_interpretation"] == "POLICY_COMMIT_RECEIPT"

    recovered = commit_monitor_scan(
        scan=scan,
        policy_inputs={"NDX": policy_payload},
        state_path=state_path,
        journal_dir=tmp_path,
    )

    assert recovered["accepted"] is True
    assert recovered["action"] == "recovered_and_committed"
    assert recovered["records_appended"] == 0
    assert recovered["commit_receipt_appended"] is False
    assert journal_path.read_bytes() == before_retry
    persisted = json.loads(state_path.read_text("utf-8"))
    ledger = persisted["monitor_scan_ledger"]
    assert ledger["last_commit_receipt_event_id"] == receipt["event_id"]
    assert ledger["last_commit_receipt_sha256"] == scan_ledger._canonical_hash(
        receipt
    )
    assert receipt["next_policy_evaluator_sha256"] == scan_ledger._canonical_hash(
        persisted["policy_evaluator"]
    )


def test_tampered_commit_receipt_or_state_anchor_fails_closed(
    tmp_path: Path,
) -> None:
    journal_case = tmp_path / "journal"
    state_path = journal_case / "state.json"
    original_state = _write_state(state_path)
    scan = _scan(ndx_eligible=False)

    def crash(stage: str) -> None:
        if stage == "after_commit_receipt_append":
            raise OSError("simulated crash")

    assert commit_monitor_scan(
        scan=scan,
        policy_inputs={},
        state_path=state_path,
        journal_dir=journal_case,
        failpoint=crash,
    )["accepted"] is False
    journal_path = journal_case / "2026-09-08.jsonl"
    rows = _journal_rows(journal_path)
    rows[-1]["next_policy_evaluator_sha256"] = "0" * 64
    journal_path.write_text(
        "".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8"
    )
    rejected = commit_monitor_scan(
        scan=scan,
        policy_inputs={},
        state_path=state_path,
        journal_dir=journal_case,
    )
    assert rejected["accepted"] is False
    assert rejected["issues"] == ["commit_receipt_event_id_mismatch"]
    assert json.loads(state_path.read_text("utf-8")) == original_state

    state_case = tmp_path / "state"
    anchored_state_path = state_case / "state.json"
    _write_state(anchored_state_path)
    anchored_scan = _scan(ndx_eligible=False)
    assert commit_monitor_scan(
        scan=anchored_scan,
        policy_inputs={},
        state_path=anchored_state_path,
        journal_dir=state_case,
    )["accepted"] is True
    anchored_state = json.loads(anchored_state_path.read_text("utf-8"))
    anchored_state["monitor_scan_ledger"]["last_commit_receipt_sha256"] = "f" * 64
    anchored_state_path.write_text(json.dumps(anchored_state), encoding="utf-8")
    anchor_rejected = commit_monitor_scan(
        scan=anchored_scan,
        policy_inputs={},
        state_path=anchored_state_path,
        journal_dir=state_case,
    )
    assert anchor_rejected["accepted"] is False
    assert anchor_rejected["issues"] == [
        "monitor_scan_ledger_commit_receipt_tampered"
    ]


def test_older_policy_wrapper_hash_cannot_be_hidden_by_reanchoring_state(
    tmp_path: Path,
) -> None:
    state_path = tmp_path / "state.json"
    _write_state(state_path, policy_state=_baseline_state())
    first = _observation(
        "2026-09-08T14:10:00Z", gamma_pin=29_700.0, max_pain=29_500.0
    )
    latest = _observation(
        "2026-09-08T14:15:00Z", gamma_pin=29_700.0, max_pain=29_500.0
    )
    payload = _policy_input(first, latest)
    alert_scan = _scan(
        ndx_eligible=True,
        ndx_policy_observation=latest,
        ndx_policy_payload=payload,
    )
    assert commit_monitor_scan(
        scan=alert_scan,
        policy_inputs={"NDX": payload},
        state_path=state_path,
        journal_dir=tmp_path,
    )["accepted"] is True
    diagnostic_scan = _scan("2026-09-08T14:20:00Z", ndx_eligible=False)
    assert commit_monitor_scan(
        scan=diagnostic_scan,
        policy_inputs={},
        state_path=state_path,
        journal_dir=tmp_path,
    )["accepted"] is True

    journal_path = tmp_path / "2026-09-08.jsonl"
    rows = _journal_rows(journal_path)
    gamma_wrapper = next(
        row
        for row in rows
        if row.get("event_type") == "GAMMA_PIN_SHIFT"
        and row.get("parent_scan_event_id") == alert_scan["event_id"]
    )
    assert "threshold_points" in gamma_wrapper["helper_event"]
    gamma_wrapper["helper_event"]["threshold_points"] += 1.0
    gamma_wrapper["alerts"] = [copy.deepcopy(gamma_wrapper["helper_event"])]
    mutated_raw = "".join(json.dumps(row) + "\n" for row in rows).encode(
        "utf-8"
    )
    journal_path.write_bytes(mutated_raw)

    state = json.loads(state_path.read_text("utf-8"))
    state["monitor_scan_ledger"]["committed_journal_size"] = len(mutated_raw)
    state["monitor_scan_ledger"]["committed_journal_sha256"] = hashlib.sha256(
        mutated_raw
    ).hexdigest()
    state_path.write_text(json.dumps(state), encoding="utf-8")
    next_scan = _scan("2026-09-08T14:25:00Z", ndx_eligible=False)
    rejected = commit_monitor_scan(
        scan=next_scan,
        policy_inputs={},
        state_path=state_path,
        journal_dir=tmp_path,
    )

    assert rejected["accepted"] is False
    assert rejected["issues"] == [
        "commit_receipt_policy_wrapper_hash_mismatch:"
        + gamma_wrapper["event_id"]
    ]


def test_journal_enforces_parent_helper_and_commit_receipt_physical_order(
    tmp_path: Path,
) -> None:
    state_path = tmp_path / "state.json"
    _write_state(state_path, policy_state=_baseline_state())
    first = _observation(
        "2026-09-08T14:10:00Z", gamma_pin=29_700.0, max_pain=29_500.0
    )
    latest = _observation(
        "2026-09-08T14:15:00Z", gamma_pin=29_700.0, max_pain=29_500.0
    )
    payload = _policy_input(first, latest)
    scan = _scan(
        ndx_eligible=True,
        ndx_policy_observation=latest,
        ndx_policy_payload=payload,
    )
    assert commit_monitor_scan(
        scan=scan,
        policy_inputs={"NDX": payload},
        state_path=state_path,
        journal_dir=tmp_path,
    )["accepted"] is True
    rows = _journal_rows(tmp_path / "2026-09-08.jsonl")
    helper_rows = [row for row in rows if "helper_event" in row]
    receipt = rows[-1]

    with pytest.raises(
        scan_ledger.MonitorScanLedgerError,
        match="policy_alert_wrapper_precedes_parent_scan",
    ):
        scan_ledger._journal_index([helper_rows[0], rows[0], *helper_rows[1:], receipt])

    with pytest.raises(
        scan_ledger.MonitorScanLedgerError,
        match="commit_receipt_policy_wrapper_order_invalid",
    ):
        scan_ledger._journal_index([rows[0], receipt, *helper_rows])


def test_unreceipted_mode_tamper_rejects_exact_and_orphan_retries(
    tmp_path: Path,
) -> None:
    committed_dir = tmp_path / "committed"
    committed_state_path = committed_dir / "state.json"
    _write_state(committed_state_path, policy_state=_baseline_state())
    first = _observation("2026-09-08T14:10:00Z")
    latest = _observation("2026-09-08T14:15:00Z")
    payload = _policy_input(first, latest)
    scan = _scan(
        ndx_eligible=True,
        ndx_policy_observation=latest,
        ndx_policy_payload=payload,
    )
    assert commit_monitor_scan(
        scan=scan,
        policy_inputs={"NDX": payload},
        state_path=committed_state_path,
        journal_dir=committed_dir,
    )["accepted"] is True
    transitioned = json.loads(committed_state_path.read_text("utf-8"))
    transitioned["mode"] = "NORMAL"
    committed_state_path.write_text(json.dumps(transitioned), encoding="utf-8")

    altered = copy.deepcopy(payload)
    altered["observations"][0]["spot"] += 1.0
    wrong_retry = commit_monitor_scan(
        scan=scan,
        policy_inputs={"NDX": altered},
        state_path=committed_state_path,
        journal_dir=committed_dir,
    )
    assert wrong_retry["accepted"] is False
    assert wrong_retry["issues"] == [
        "policy_inputs.NDX.sha256_scan_payload_mismatch"
    ]
    exact_retry = commit_monitor_scan(
        scan=scan,
        policy_inputs={"NDX": payload},
        state_path=committed_state_path,
        journal_dir=committed_dir,
    )
    assert exact_retry["accepted"] is False
    assert exact_retry["issues"] == [
        "monitor_scan_ledger_cadence_state_hash_mismatch"
    ]

    orphan_dir = tmp_path / "orphan"
    orphan_state_path = orphan_dir / "state.json"
    _write_state(orphan_state_path)
    orphan = _scan(ndx_eligible=False)

    def crash(stage: str) -> None:
        if stage == "after_scan_append":
            raise OSError("simulated orphan")

    assert commit_monitor_scan(
        scan=orphan,
        policy_inputs={},
        state_path=orphan_state_path,
        journal_dir=orphan_dir,
        failpoint=crash,
    )["accepted"] is False
    orphan_state = json.loads(orphan_state_path.read_text("utf-8"))
    orphan_state["mode"] = "NORMAL"
    orphan_state_path.write_text(json.dumps(orphan_state), encoding="utf-8")
    recovered = commit_monitor_scan(
        scan=orphan,
        policy_inputs={},
        state_path=orphan_state_path,
        journal_dir=orphan_dir,
    )
    assert recovered["accepted"] is False
    assert recovered["issues"] == ["scan_cadence_state_mode_mismatch"]

    orphan_state["mode"] = "ELEVATED"
    orphan_state_path.write_text(json.dumps(orphan_state), encoding="utf-8")
    recovered = commit_monitor_scan(
        scan=orphan,
        policy_inputs={},
        state_path=orphan_state_path,
        journal_dir=orphan_dir,
    )
    assert recovered["accepted"] is True
    assert recovered["action"] == "recovered_and_committed"

    brand_new = _scan(
        "2026-09-08T14:20:00Z", ndx_eligible=False, cadence_mode="NORMAL"
    )
    mismatch = commit_monitor_scan(
        scan=brand_new,
        policy_inputs={},
        state_path=orphan_state_path,
        journal_dir=orphan_dir,
    )
    assert mismatch["accepted"] is False
    assert mismatch["issues"] == ["scan_cadence_state_mode_mismatch"]


def test_first_scan_binds_full_rollover_event_and_matches_pre_rollover_predicate(
    tmp_path: Path,
) -> None:
    tamper_dir = tmp_path / "tamper"
    state_path, journal_path, rolled_state = _rollover_friday_to_tuesday(
        tamper_dir
    )
    rows = _journal_rows(journal_path)
    rows[0]["prior_journal_sha256"] = "0" * 64
    journal_path.write_text(json.dumps(rows[0]) + "\n", encoding="utf-8")
    rejected = commit_monitor_scan(
        scan=_scan(
            ndx_eligible=False,
            cadence_mode="NORMAL",
            cadence_evidence=_cadence_evidence(),
        ),
        policy_inputs={},
        state_path=state_path,
        journal_dir=tamper_dir,
    )
    assert rejected["accepted"] is False
    assert rejected["issues"][0].startswith(
        "first_scan_session_rollover_receipt_invalid:"
    )
    assert json.loads(state_path.read_text("utf-8")) == rolled_state
    assert len(_journal_rows(journal_path)) == 1

    ordering_dir = tmp_path / "ordering"
    ordered_state_path, ordered_journal_path, ordered_state = (
        _rollover_friday_to_tuesday(ordering_dir)
    )
    rollover_event = _journal_rows(ordered_journal_path)[0]
    pre_rollover_data_quality = {
        "schema_version": 2,
        "event_id": "2026-09-08:clock:data_quality:probe",
        "event_type": "data_quality",
        "session_date": "2026-09-08",
        "phase": "clock_preflight",
        "alerts": [],
    }
    ordered_journal_path.write_text(
        json.dumps(pre_rollover_data_quality)
        + "\n"
        + json.dumps(rollover_event)
        + "\n",
        encoding="utf-8",
    )
    ordering_accepted = commit_monitor_scan(
        scan=_scan(
            ndx_eligible=False,
            cadence_mode="NORMAL",
            cadence_evidence=_cadence_evidence(),
        ),
        policy_inputs={},
        state_path=ordered_state_path,
        journal_dir=ordering_dir,
    )
    assert ordering_accepted["accepted"] is True

    signal_dir = tmp_path / "signal"
    signal_state_path, signal_journal_path, signal_state = (
        _rollover_friday_to_tuesday(signal_dir)
    )
    signal_rollover = _journal_rows(signal_journal_path)[0]
    pre_rollover_signal = {
        "schema_version": 2,
        "event_id": "2026-09-08:probe:cadence_change:v1",
        "event_type": "cadence_change",
        "session_date": "2026-09-08",
        "phase": "regular-session",
        "alerts": [],
    }
    signal_journal_path.write_text(
        json.dumps(pre_rollover_signal)
        + "\n"
        + json.dumps(signal_rollover)
        + "\n",
        encoding="utf-8",
    )
    signal_rejected = commit_monitor_scan(
        scan=_scan(
            ndx_eligible=False,
            cadence_mode="NORMAL",
            cadence_evidence=_cadence_evidence(),
        ),
        policy_inputs={},
        state_path=signal_state_path,
        journal_dir=signal_dir,
    )
    assert signal_rejected["accepted"] is False
    assert signal_rejected["issues"] == [
        "first_scan_signal_record_precedes_session_rollover"
    ]
    assert json.loads(signal_state_path.read_text("utf-8")) == signal_state


def test_short_write_failure_rolls_back_owned_jsonl_tail(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    journal_path = tmp_path / "journal.jsonl"
    original = b'{"existing":true}\n'
    journal_path.write_bytes(original)
    real_write = scan_ledger.os.write
    call_count = 0

    def short_then_fail(descriptor: int, data) -> int:
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            chunk = bytes(data[:7])
            return real_write(descriptor, chunk)
        raise OSError("simulated write failure")

    monkeypatch.setattr(scan_ledger.os, "write", short_then_fail)
    with pytest.raises(OSError, match="simulated write failure"):
        scan_ledger._append_jsonl_durable(journal_path, {"new": True})
    assert journal_path.read_bytes() == original


@pytest.mark.parametrize("partial_stage", ["scan", "helper", "commit_receipt"])
def test_exact_retry_authenticates_and_recovers_hard_kill_partial_tail(
    tmp_path: Path, partial_stage: str
) -> None:
    first = _observation(
        "2026-09-08T14:10:00Z", gamma_pin=29_700.0, max_pain=29_500.0
    )
    latest = _observation(
        "2026-09-08T14:15:00Z", gamma_pin=29_700.0, max_pain=29_500.0
    )
    payload = _policy_input(first, latest)
    scan = _scan(
        ndx_eligible=True,
        ndx_policy_observation=latest,
        ndx_policy_payload=payload,
    )

    control_dir = tmp_path / "control"
    control_state = control_dir / "state.json"
    _write_state(control_state, policy_state=_baseline_state())
    assert commit_monitor_scan(
        scan=scan,
        policy_inputs={"NDX": payload},
        state_path=control_state,
        journal_dir=control_dir,
    )["accepted"] is True
    expected_rows = _journal_rows(control_dir / "2026-09-08.jsonl")
    helper_indexes = [
        index for index, row in enumerate(expected_rows) if "helper_event" in row
    ]
    next_index = {
        "scan": 0,
        "helper": helper_indexes[0],
        "commit_receipt": len(expected_rows) - 1,
    }[partial_stage]

    target_dir = tmp_path / "target"
    target_state = target_dir / "state.json"
    _write_state(target_state, policy_state=_baseline_state())
    journal_path = target_dir / "2026-09-08.jsonl"
    complete = b"".join(
        scan_ledger._canonical_json_bytes(row) for row in expected_rows[:next_index]
    )
    next_bytes = scan_ledger._canonical_json_bytes(expected_rows[next_index])
    partial = next_bytes[: max(1, len(next_bytes) // 2)]
    journal_path.write_bytes(complete + partial)

    recovered = commit_monitor_scan(
        scan=scan,
        policy_inputs={"NDX": payload},
        state_path=target_state,
        journal_dir=target_dir,
    )

    assert recovered["accepted"] is True, recovered
    assert recovered["partial_journal_tail_recovered"] is True
    assert _journal_rows(journal_path) == expected_rows
    assert recovered["event_ids_pending_notification"] == [
        row["event_id"] for row in expected_rows if "helper_event" in row
    ]


def test_exact_retry_recovers_partial_scan_after_complete_non_policy_suffix(
    tmp_path: Path,
) -> None:
    target_dir = tmp_path / "target"
    target_state = target_dir / "state.json"
    _write_state(target_state)
    first_scan = _scan("2026-09-08T14:15:00Z", ndx_eligible=False)
    first_result = commit_monitor_scan(
        scan=first_scan,
        policy_inputs={},
        state_path=target_state,
        journal_dir=target_dir,
    )
    assert first_result["accepted"] is True, first_result

    journal_path = target_dir / "2026-09-08.jsonl"
    data_quality_record = {
        "schema_version": 2,
        "event_id": "2026-09-08:regular-session:data-quality:probe",
        "event_type": "data_quality",
        "session_date": "2026-09-08",
        "phase": "regular-session",
        "alerts": [],
    }
    with journal_path.open("ab") as handle:
        handle.write(scan_ledger._canonical_json_bytes(data_quality_record))

    control_dir = tmp_path / "control"
    control_dir.mkdir()
    control_state = control_dir / "state.json"
    control_state.write_bytes(target_state.read_bytes())
    control_journal = control_dir / "2026-09-08.jsonl"
    control_journal.write_bytes(journal_path.read_bytes())
    next_scan = _scan("2026-09-08T14:20:00Z", ndx_eligible=False)
    control_result = commit_monitor_scan(
        scan=next_scan,
        policy_inputs={},
        state_path=control_state,
        journal_dir=control_dir,
    )
    assert control_result["accepted"] is True, control_result
    expected_rows = _journal_rows(control_journal)
    next_scan_bytes = scan_ledger._canonical_json_bytes(expected_rows[-2])
    with journal_path.open("ab") as handle:
        handle.write(next_scan_bytes[: max(1, len(next_scan_bytes) // 2)])

    recovered = commit_monitor_scan(
        scan=next_scan,
        policy_inputs={},
        state_path=target_state,
        journal_dir=target_dir,
    )

    assert recovered["accepted"] is True, recovered
    assert recovered["action"] == "recovered_and_committed"
    assert recovered["partial_journal_tail_recovered"] is True
    assert _journal_rows(journal_path) == expected_rows
    assert _journal_rows(journal_path)[2] == data_quality_record


@pytest.mark.parametrize(
    "reserved_event_type",
    [
        "substantive_scan",
        "substantive_scan_commit",
        "policy_notification_ack",
        "MAX_PAIN_CHANGE",
        "MAX_PAIN_PROVENANCE_RESET",
        "GAMMA_PIN_SHIFT",
        "DIRECTIONAL_SHIFT",
    ],
)
def test_partial_recovery_never_skips_wrong_schema_reserved_record(
    tmp_path: Path, reserved_event_type: str
) -> None:
    state_path = tmp_path / "state.json"
    _write_state(state_path)
    first_result = commit_monitor_scan(
        scan=_scan("2026-09-08T14:15:00Z", ndx_eligible=False),
        policy_inputs={},
        state_path=state_path,
        journal_dir=tmp_path,
    )
    assert first_result["accepted"] is True, first_result

    journal_path = tmp_path / "2026-09-08.jsonl"
    suspicious_record = {
        "schema_version": 1,
        "event_type": reserved_event_type,
        "session_date": "2026-09-08",
    }
    if reserved_event_type in scan_ledger._POLICY_EVENT_TYPES:
        suspicious_record["event_id"] = "f" * 64
    next_scan = _scan("2026-09-08T14:20:00Z", ndx_eligible=False)
    next_scan_bytes = scan_ledger._canonical_json_bytes(next_scan)
    with journal_path.open("ab") as handle:
        handle.write(scan_ledger._canonical_json_bytes(suspicious_record))
        handle.write(next_scan_bytes[: max(1, len(next_scan_bytes) // 2)])
    journal_before = journal_path.read_bytes()
    state_before = state_path.read_bytes()

    rejected = commit_monitor_scan(
        scan=next_scan,
        policy_inputs={},
        state_path=state_path,
        journal_dir=tmp_path,
    )

    assert rejected["accepted"] is False
    assert rejected["issues"] == [
        f"journal_transaction_schema_invalid:{reserved_event_type}"
    ]
    assert journal_path.read_bytes() == journal_before
    assert state_path.read_bytes() == state_before


def test_mismatched_partial_tail_is_never_truncated(tmp_path: Path) -> None:
    state_path = tmp_path / "state.json"
    original_state = _write_state(state_path)
    journal_path = tmp_path / "2026-09-08.jsonl"
    forged_tail = b'{"forged_unowned_tail":'
    journal_path.write_bytes(forged_tail)

    result = commit_monitor_scan(
        scan=_scan(ndx_eligible=False),
        policy_inputs={},
        state_path=state_path,
        journal_dir=tmp_path,
    )

    assert result["accepted"] is False
    assert result["issues"] == [
        "partial_journal_tail_not_exact_transaction_prefix"
    ]
    assert journal_path.read_bytes() == forged_tail
    assert json.loads(state_path.read_text("utf-8")) == original_state


def test_post_state_failure_reports_retry_required_without_false_untouched_claim(
    tmp_path: Path,
) -> None:
    state_path = tmp_path / "state.json"
    _write_state(state_path)
    scan = _scan(ndx_eligible=False)

    def fail_after_replace(stage: str) -> None:
        if stage == "after_state_replace":
            raise OSError("simulated postcondition failure")

    uncertain = commit_monitor_scan(
        scan=scan,
        policy_inputs={},
        state_path=state_path,
        journal_dir=tmp_path,
        failpoint=fail_after_replace,
    )

    assert uncertain["accepted"] is False
    assert uncertain["action"] == "retry_required"
    assert uncertain["state_updated"] is True
    assert uncertain["commit_phase"] == "post_state_replace_uncertain"
    state = json.loads(state_path.read_text("utf-8"))
    assert state["monitor_scan_ledger"]["last_committed_scan_event_id"] == scan[
        "event_id"
    ]

    retry = commit_monitor_scan(
        scan=scan,
        policy_inputs={},
        state_path=state_path,
        journal_dir=tmp_path,
    )
    assert retry["accepted"] is True
    assert retry["action"] == "already_committed"


def test_known_non_policy_schema_v2_records_remain_compatible() -> None:
    event_types = {
        "cadence_change",
        "data_quality",
        "session_rollover",
        "policy_evaluator_seed",
        "policy_state_seed",
        "historical_correction",
        "post_close_recap",
    }
    records = [
        {
            "schema_version": 2,
            "event_id": f"2026-09-08:session:{event_type}:v1",
            "event_type": event_type,
            "session_date": "2026-09-08",
            "alerts": [],
        }
        for event_type in sorted(event_types)
    ]

    by_id, receipts = scan_ledger._journal_index(records)

    assert set(by_id) == {record["event_id"] for record in records}
    assert receipts == {"SPX": [], "NDX": []}


def test_rollover_preserved_prior_session_ledger_does_not_block_new_session(
    tmp_path: Path,
) -> None:
    state_path = tmp_path / "state.json"
    state = _write_state(state_path)
    state["monitor_scan_ledger"] = {
        "schema_version": RESULT_SCHEMA,
        "session_date": "2026-09-04",
        "last_committed_scan_event_id": "f" * 64,
    }
    state_path.write_text(json.dumps(state), encoding="utf-8")

    result = commit_monitor_scan(
        scan=_scan(ndx_eligible=False),
        policy_inputs={},
        state_path=state_path,
        journal_dir=tmp_path,
    )

    assert result["accepted"] is True
    persisted = json.loads(state_path.read_text("utf-8"))
    assert persisted["monitor_scan_ledger"]["session_date"] == "2026-09-08"


def test_substantive_scan_cannot_embed_alert_or_directional_diagnostic() -> None:
    with_alert = _scan(ndx_eligible=False)
    with_alert["alerts"] = [{"event_type": "GAMMA_PIN_SHIFT"}]
    with_alert.pop("event_id")
    with_alert = with_scan_event_id(with_alert)
    assert commit_monitor_scan(
        scan=with_alert,
        policy_inputs={},
        state_path=Path("unused-state"),
        journal_dir=Path("unused-journal"),
    )["issues"] == ["scan_alerts_must_be_empty"]

    directional = _scan(ndx_eligible=False)
    directional["directional_interpretation"] = "BULLISH"
    directional.pop("event_id")
    directional = with_scan_event_id(directional)
    assert commit_monitor_scan(
        scan=directional,
        policy_inputs={},
        state_path=Path("unused-state"),
        journal_dir=Path("unused-journal"),
    )["issues"] == [
        "diagnostic_scan_directional_interpretation_must_equal_ABSTAIN"
    ]

    first = _observation("2026-09-08T14:10:00Z")
    latest = _observation("2026-09-08T14:15:00Z")
    eligible_directional = _scan(
        ndx_eligible=True,
        ndx_policy_observation=latest,
        ndx_policy_payload=_policy_input(first, latest),
    )
    eligible_directional["directional_interpretation"] = "BULLISH"
    eligible_directional.pop("event_id")
    eligible_directional = with_scan_event_id(eligible_directional)
    assert commit_monitor_scan(
        scan=eligible_directional,
        policy_inputs={"NDX": _policy_input(first, latest)},
        state_path=Path("unused-state"),
        journal_dir=Path("unused-journal"),
    )["issues"] == [
        "eligible_scan_directional_interpretation_must_equal_POLICY_CONTROLLED"
    ]


def test_cli_assigns_scan_id_and_commits_diagnostic(tmp_path: Path) -> None:
    state_path, _journal_path, _rolled_state = _rollover_friday_to_tuesday(tmp_path)
    scan = _scan(
        ndx_eligible=False,
        cadence_mode="NORMAL",
        cadence_evidence=_cadence_evidence(),
    )
    scan.pop("event_id")
    request_path = tmp_path / "request.json"
    request_path.write_text(
        json.dumps({"scan": scan, "policy_inputs": {}}), encoding="utf-8"
    )

    completed = subprocess.run(
        [
            sys.executable,
            str(ROOT / "tools" / "commit_market_monitor_scan.py"),
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
    assert result["schema_version"] == RESULT_SCHEMA
    assert result["accepted"] is True
    rows = _journal_rows(tmp_path / "2026-09-08.jsonl")
    assert len(rows) == 3
    assert rows[1]["event_id"] == result["scan_event_id"]


def test_cli_commits_full_policy_input_hash_before_scan_identity(
    tmp_path: Path,
) -> None:
    state_path, _journal_path, _rolled_state = _rollover_friday_to_tuesday(tmp_path)
    first = _observation(
        "2026-09-08T14:10:00Z", gamma_pin=29_700.0, max_pain=29_500.0
    )
    latest = _observation(
        "2026-09-08T14:15:00Z", gamma_pin=29_700.0, max_pain=29_500.0
    )
    seed_scan = _scan(
        "2026-09-08T14:10:00Z",
        ndx_eligible=False,
        ndx_candidate_policy_observation=first,
        cadence_mode="NORMAL",
        cadence_evidence=_cadence_evidence(),
    )
    seeded = commit_monitor_scan(
        scan=seed_scan,
        policy_inputs={},
        state_path=state_path,
        journal_dir=tmp_path,
    )
    assert seeded["accepted"] is True, seeded
    policy_payload = _policy_input(first, latest, cadence_seconds=900)
    policy_payload["prior_scan_event_id"] = seed_scan["event_id"]
    scan = _scan(
        ndx_eligible=True,
        ndx_policy_observation=latest,
        ndx_candidate_policy_observation=latest,
        ndx_prior_confirmation_scan_event_id=seed_scan["event_id"],
        ndx_policy_payload=policy_payload,
        cadence_mode="NORMAL",
        cadence_evidence=_cadence_evidence(),
    )
    scan.pop("event_id")
    scan["symbols"]["NDX"].pop("policy_input_sha256")
    request_path = tmp_path / "eligible-request.json"
    request_path.write_text(
        json.dumps({"scan": scan, "policy_inputs": {"NDX": policy_payload}}),
        encoding="utf-8",
    )

    completed = subprocess.run(
        [
            sys.executable,
            str(ROOT / "tools" / "commit_market_monitor_scan.py"),
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
    committed_scan = _journal_rows(tmp_path / "2026-09-08.jsonl")[-2]
    assert committed_scan["symbols"]["NDX"]["policy_input_sha256"] == (
        policy_input_sha256(policy_payload)
    )
    assert committed_scan["event_id"] == result["scan_event_id"]


def test_explicit_cadence_transition_recovers_receipt_before_state_crash(
    tmp_path: Path,
) -> None:
    state_path, journal_path, _rolled_state = _rollover_friday_to_tuesday(tmp_path)
    seed_scan = _dual_symbol_seed_scan(
        "2026-09-08T13:45:00Z", cadence_mode="NORMAL"
    )
    seeded = commit_monitor_scan(
        scan=seed_scan,
        policy_inputs={},
        state_path=state_path,
        journal_dir=tmp_path,
    )
    assert seeded["accepted"] is True, seeded
    state_after_seed = json.loads(state_path.read_text("utf-8"))
    scan, policy_inputs = _dual_symbol_explicit_scan(
        "2026-09-08T14:00:00Z",
        "2026-09-08T13:45:00Z",
        prior_scan_event_id=seed_scan["event_id"],
        cadence_mode="NORMAL",
        cadence_seconds=900,
        cadence_evidence=_cadence_evidence(
            symbol_flags={"NDX": {"high_volatility_regime": True}}
        ),
    )

    def crash(stage: str) -> None:
        if stage == "after_commit_receipt_append":
            raise OSError("simulated receipt-before-state interruption")

    failed = commit_monitor_scan(
        scan=scan,
        policy_inputs=policy_inputs,
        state_path=state_path,
        journal_dir=tmp_path,
        failpoint=crash,
    )
    assert failed["accepted"] is False
    assert failed["commit_receipt_appended"] is True
    assert failed["cadence_transition_newly_durable"] is False
    assert json.loads(state_path.read_text("utf-8")) == state_after_seed
    durable_before_retry = journal_path.read_bytes()

    recovered = commit_monitor_scan(
        scan=scan,
        policy_inputs=policy_inputs,
        state_path=state_path,
        journal_dir=tmp_path,
    )

    assert recovered["accepted"] is True, recovered
    assert recovered["action"] == "recovered_and_committed"
    assert recovered["cadence_decision"]["transition"] == "ENTER_ELEVATED"
    assert recovered["commit_receipt_appended"] is False
    assert recovered["cadence_transition_newly_durable"] is False
    assert recovered["event_ids_pending_notification"] == []
    assert journal_path.read_bytes() == durable_before_retry
    persisted = json.loads(state_path.read_text("utf-8"))
    assert persisted["mode"] == "ELEVATED"
    assert persisted["elevated_minimum_until_ct"] == (
        "2026-09-08T09:30:00-05:00"
    )


def test_third_durable_stable_scan_demotes_and_exact_retry_is_idempotent(
    tmp_path: Path,
) -> None:
    state_path, _journal_path, _rolled_state = _rollover_friday_to_tuesday(tmp_path)
    seed_scan = _dual_symbol_seed_scan(
        "2026-09-08T13:45:00Z", cadence_mode="NORMAL"
    )
    seeded = commit_monitor_scan(
        scan=seed_scan,
        policy_inputs={},
        state_path=state_path,
        journal_dir=tmp_path,
    )
    assert seeded["accepted"] is True, seeded
    trigger_scan, trigger_inputs = _dual_symbol_explicit_scan(
        "2026-09-08T14:00:00Z",
        "2026-09-08T13:45:00Z",
        prior_scan_event_id=seed_scan["event_id"],
        cadence_mode="NORMAL",
        cadence_seconds=900,
        cadence_evidence=_cadence_evidence(
            symbol_flags={"NDX": {"high_volatility_regime": True}}
        ),
    )
    entered = commit_monitor_scan(
        scan=trigger_scan,
        policy_inputs=trigger_inputs,
        state_path=state_path,
        journal_dir=tmp_path,
    )
    assert entered["accepted"] is True, entered
    assert entered["cadence_decision"]["transition"] == "ENTER_ELEVATED"

    prior_scan = trigger_scan
    for observed_at, prior_at in (
        ("2026-09-08T14:05:00Z", "2026-09-08T14:00:00Z"),
        ("2026-09-08T14:10:00Z", "2026-09-08T14:05:00Z"),
        ("2026-09-08T14:15:00Z", "2026-09-08T14:10:00Z"),
        ("2026-09-08T14:20:00Z", "2026-09-08T14:15:00Z"),
    ):
        warmup_scan, warmup_inputs = _dual_symbol_explicit_scan(
            observed_at,
            prior_at,
            prior_scan_event_id=prior_scan["event_id"],
            cadence_mode="ELEVATED",
            cadence_seconds=300,
            cadence_evidence=_cadence_evidence(),
        )
        warmed = commit_monitor_scan(
            scan=warmup_scan,
            policy_inputs=warmup_inputs,
            state_path=state_path,
            journal_dir=tmp_path,
        )
        assert warmed["accepted"] is True, warmed
        assert warmed["cadence_decision"]["transition"] == "HOLD_ELEVATED_MINIMUM"
        prior_scan = warmup_scan

    requests = (
        ("2026-09-08T14:25:00Z", "2026-09-08T14:20:00Z"),
        ("2026-09-08T14:30:00Z", "2026-09-08T14:25:00Z"),
        ("2026-09-08T14:35:00Z", "2026-09-08T14:30:00Z"),
        ("2026-09-08T14:40:00Z", "2026-09-08T14:35:00Z"),
    )
    transitions: list[str] = []
    final_scan: dict | None = None
    final_inputs: dict[str, dict] | None = None
    final_result: dict | None = None
    for observed_at, prior_at in requests:
        scan, policy_inputs = _dual_symbol_explicit_scan(
            observed_at,
            prior_at,
            prior_scan_event_id=prior_scan["event_id"],
            cadence_mode="ELEVATED",
            cadence_seconds=300,
            cadence_evidence=_cadence_evidence(),
        )
        committed = commit_monitor_scan(
            scan=scan,
            policy_inputs=policy_inputs,
            state_path=state_path,
            journal_dir=tmp_path,
        )
        assert committed["accepted"] is True, committed
        transitions.append(committed["cadence_decision"]["transition"])
        final_scan, final_inputs, final_result = scan, policy_inputs, committed
        prior_scan = scan

    assert transitions == [
        "HOLD_ELEVATED_MINIMUM",
        "COUNT_STABLE_ELEVATED_SCAN",
        "COUNT_STABLE_ELEVATED_SCAN",
        "RETURN_NORMAL",
    ]
    assert final_result is not None
    assert final_result["cadence_transition_newly_durable"] is True
    assert final_result["event_ids_appended"] == []
    assert final_result["event_ids_pending_notification"] == []
    persisted = json.loads(state_path.read_text("utf-8"))
    assert persisted["mode"] == "NORMAL"
    assert persisted["stable_elevated_scan_count"] == 0

    assert final_scan is not None and final_inputs is not None
    exact_retry = commit_monitor_scan(
        scan=final_scan,
        policy_inputs=final_inputs,
        state_path=state_path,
        journal_dir=tmp_path,
    )
    assert exact_retry["accepted"] is True, exact_retry
    assert exact_retry["action"] == "already_committed"
    assert exact_retry["cadence_decision"]["transition"] == "RETURN_NORMAL"
    assert exact_retry["cadence_transition_newly_durable"] is False
