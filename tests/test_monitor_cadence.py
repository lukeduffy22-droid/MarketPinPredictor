from __future__ import annotations

import copy
from datetime import datetime
from zoneinfo import ZoneInfo

import pytest

from backend.monitor_cadence import (
    CADENCE_EVIDENCE_SCHEMA,
    CADENCE_TRIGGER_CALCULATION_VERSION,
    MonitorCadenceError,
    apply_cadence_decision,
    evaluate_cadence_transition,
)


_TRIGGER_FIELDS = (
    "half_threshold_gamma_pin_move",
    "contested_pin_leadership",
    "spot_near_or_crossed_gamma_pin",
    "spot_near_or_crossed_zero_gamma",
    "spot_near_or_crossed_gex_wall",
    "normalized_net_gex_near_or_crossed_zero",
    "forecast_bias_awaiting_confirmation",
    "high_volatility_regime",
)


def _state(
    *,
    mode: str = "NORMAL",
    last_scan_utc: str | None = None,
    stable_count: int = 0,
) -> dict:
    state = {
        "session_date": "2026-09-08",
        "mode": mode,
        "mode_reason": "session_rollover" if mode == "NORMAL" else "fixture_trigger",
        "elevated_since_ct": None,
        "elevated_minimum_until_ct": None,
        "stable_elevated_scan_count": stable_count,
    }
    if mode == "ELEVATED":
        state["elevated_since_ct"] = "2026-09-08T09:00:00-05:00"
        state["elevated_minimum_until_ct"] = "2026-09-08T09:30:00-05:00"
    if last_scan_utc is not None:
        state["last_scan_utc"] = last_scan_utc
    return state


def _evidence(
    *,
    symbol: str | None = None,
    trigger: str | None = None,
    active_ids: list[str] | None = None,
    new_ids: list[str] | None = None,
) -> dict:
    flags = {
        item: {field: False for field in _TRIGGER_FIELDS}
        for item in ("SPX", "NDX")
    }
    if symbol is not None and trigger is not None:
        flags[symbol][trigger] = True
    return {
        "schema_version": CADENCE_EVIDENCE_SCHEMA,
        "trigger_calculation_version": CADENCE_TRIGGER_CALCULATION_VERSION,
        "active_data_quality_event_ids": sorted(active_ids or []),
        "new_data_quality_event_ids": sorted(new_ids or []),
        "symbols": flags,
    }


def _scan(
    observed_at_utc: str,
    *,
    mode: str,
    evidence: dict | None,
    spx_eligible: bool = True,
    ndx_eligible: bool = True,
    contested_symbol: str | None = None,
) -> dict:
    observed_at_ct = (
        datetime.fromisoformat(observed_at_utc.replace("Z", "+00:00"))
        .astimezone(ZoneInfo("America/Chicago"))
        .isoformat()
    )

    def symbol_payload(symbol: str, eligible: bool) -> dict:
        payload = {"eligible": eligible}
        if eligible:
            payload["policy_observation"] = {
                "pin_is_contested": symbol == contested_symbol,
                "pin_lead_ratio": 0.05 if symbol == contested_symbol else 0.25,
            }
        return payload

    cadence = {"mode": mode, "substantive": True}
    if evidence is not None:
        cadence["adaptive_evidence"] = copy.deepcopy(evidence)
    return {
        "observed_at_ct": observed_at_ct,
        "observed_at_utc": observed_at_utc,
        "session_date": "2026-09-08",
        "cadence": cadence,
        "symbols": {
            "SPX": symbol_payload("SPX", spx_eligible),
            "NDX": symbol_payload("NDX", ndx_eligible),
        },
    }


@pytest.mark.parametrize("trigger", _TRIGGER_FIELDS)
def test_each_explicit_trigger_enters_elevated_for_thirty_minutes(
    trigger: str,
) -> None:
    decision = evaluate_cadence_transition(
        state=_state(),
        scan=_scan(
            "2026-09-08T14:15:00Z",
            mode="NORMAL",
            evidence=_evidence(symbol="NDX", trigger=trigger),
        ),
        policy_events=[],
        require_explicit_evidence=True,
    )

    assert decision["transition"] == "ENTER_ELEVATED"
    assert decision["trigger_reasons"] == [f"NDX:{trigger.upper()}"]
    assert decision["next_state"]["mode"] == "ELEVATED"
    assert decision["next_state"]["elevated_since_ct"] == (
        "2026-09-08T09:15:00-05:00"
    )
    assert decision["next_state"]["elevated_minimum_until_ct"] == (
        "2026-09-08T09:45:00-05:00"
    )


def test_generated_policy_alert_is_automatic_cadence_trigger() -> None:
    event = {
        "event_id": "policy-event",
        "type": "GAMMA_PIN_SHIFT",
        "symbol": "SPX",
    }
    decision = evaluate_cadence_transition(
        state=_state(),
        scan=_scan(
            "2026-09-08T14:15:00Z",
            mode="NORMAL",
            evidence=_evidence(),
        ),
        policy_events=[event],
        require_explicit_evidence=True,
    )

    assert decision["transition"] == "ENTER_ELEVATED"
    assert decision["trigger_reasons"] == [
        "POLICY_ALERT:GAMMA_PIN_SHIFT:SPX:policy-event"
    ]


def test_trigger_extends_existing_minimum_and_resets_stable_count() -> None:
    decision = evaluate_cadence_transition(
        state=_state(
            mode="ELEVATED",
            last_scan_utc="2026-09-08T14:10:00Z",
            stable_count=2,
        ),
        scan=_scan(
            "2026-09-08T14:15:00Z",
            mode="ELEVATED",
            evidence=_evidence(symbol="SPX", trigger="high_volatility_regime"),
        ),
        policy_events=[],
        require_explicit_evidence=True,
    )

    assert decision["transition"] == "EXTEND_ELEVATED"
    assert decision["next_state"]["elevated_since_ct"] == (
        "2026-09-08T09:00:00-05:00"
    )
    assert decision["next_state"]["elevated_minimum_until_ct"] == (
        "2026-09-08T09:45:00-05:00"
    )
    assert decision["next_state"]["stable_elevated_scan_count"] == 0


def test_third_eligible_trigger_free_five_minute_scan_after_minimum_demotes() -> None:
    state = _state(
        mode="ELEVATED",
        last_scan_utc="2026-09-08T14:30:00Z",
    )
    transitions = []
    for observed_at in (
        "2026-09-08T14:35:00Z",
        "2026-09-08T14:40:00Z",
        "2026-09-08T14:45:00Z",
    ):
        decision = evaluate_cadence_transition(
            state=state,
            scan=_scan(
                observed_at,
                mode="ELEVATED",
                evidence=_evidence(),
            ),
            policy_events=[],
            require_explicit_evidence=True,
        )
        transitions.append(decision["transition"])
        state = apply_cadence_decision(state, decision)
        state["last_scan_utc"] = observed_at

    assert transitions == [
        "COUNT_STABLE_ELEVATED_SCAN",
        "COUNT_STABLE_ELEVATED_SCAN",
        "RETURN_NORMAL",
    ]
    assert state["mode"] == "NORMAL"
    assert state["elevated_since_ct"] is None
    assert state["elevated_minimum_until_ct"] is None
    assert state["stable_elevated_scan_count"] == 0


@pytest.mark.parametrize(
    ("observed_at", "last_scan", "spx_eligible", "active_ids", "transition"),
    [
        (
            "2026-09-08T14:25:00Z",
            "2026-09-08T14:20:00Z",
            True,
            [],
            "HOLD_ELEVATED_MINIMUM",
        ),
        (
            "2026-09-08T14:35:00Z",
            "2026-09-08T14:30:00Z",
            False,
            [],
            "HOLD_ELEVATED_INELIGIBLE",
        ),
        (
            "2026-09-08T14:35:00Z",
            "2026-09-08T14:30:00Z",
            True,
            ["dq-active"],
            "HOLD_ELEVATED_DATA_QUALITY",
        ),
        (
            "2026-09-08T14:35:00Z",
            "2026-09-08T14:32:00Z",
            True,
            [],
            "HOLD_ELEVATED_NONCONSECUTIVE",
        ),
        (
            "2026-09-08T14:35:00Z",
            "2026-09-08T14:25:00Z",
            True,
            [],
            "HOLD_ELEVATED_NONCONSECUTIVE",
        ),
    ],
)
def test_nonqualifying_scan_resets_stable_count(
    observed_at: str,
    last_scan: str,
    spx_eligible: bool,
    active_ids: list[str],
    transition: str,
) -> None:
    decision = evaluate_cadence_transition(
        state=_state(mode="ELEVATED", last_scan_utc=last_scan, stable_count=2),
        scan=_scan(
            observed_at,
            mode="ELEVATED",
            evidence=_evidence(active_ids=active_ids),
            spx_eligible=spx_eligible,
        ),
        policy_events=[],
        require_explicit_evidence=True,
        previously_seen_data_quality_event_ids=active_ids,
    )

    assert decision["transition"] == transition
    assert decision["next_state"]["stable_elevated_scan_count"] == 0


def test_new_data_quality_id_triggers_once_and_must_remain_active() -> None:
    with pytest.raises(
        MonitorCadenceError,
        match="cadence_new_data_quality_event_ids_not_derived_from_active",
    ):
        evaluate_cadence_transition(
            state=_state(),
            scan=_scan(
                "2026-09-08T14:15:00Z",
                mode="NORMAL",
                evidence=_evidence(active_ids=["dq-1"]),
            ),
            policy_events=[],
            require_explicit_evidence=True,
        )

    decision = evaluate_cadence_transition(
        state=_state(),
        scan=_scan(
            "2026-09-08T14:15:00Z",
            mode="NORMAL",
            evidence=_evidence(active_ids=["dq-1"], new_ids=["dq-1"]),
        ),
        policy_events=[],
        require_explicit_evidence=True,
    )
    assert decision["transition"] == "ENTER_ELEVATED"
    assert decision["new_data_quality_event_ids"] == ["dq-1"]

    with pytest.raises(
        MonitorCadenceError,
        match="cadence_new_data_quality_event_already_seen:dq-1",
    ):
        evaluate_cadence_transition(
            state=apply_cadence_decision(_state(), decision),
            scan=_scan(
                "2026-09-08T14:20:00Z",
                mode="ELEVATED",
                evidence=_evidence(active_ids=["dq-1"], new_ids=["dq-1"]),
            ),
            policy_events=[],
            require_explicit_evidence=True,
            previously_seen_data_quality_event_ids=["dq-1"],
        )


def test_contested_observation_cannot_omit_contested_trigger() -> None:
    with pytest.raises(
        MonitorCadenceError,
        match="cadence_evidence.NDX.contested_pin_trigger_omitted",
    ):
        evaluate_cadence_transition(
            state=_state(),
            scan=_scan(
                "2026-09-08T14:15:00Z",
                mode="NORMAL",
                evidence=_evidence(),
                contested_symbol="NDX",
            ),
            policy_events=[],
            require_explicit_evidence=True,
        )


def test_legacy_compatibility_preserves_incomplete_fixture_state() -> None:
    state = {"session_date": "2026-09-08", "mode": "ELEVATED", "extension": 1}
    decision = evaluate_cadence_transition(
        state=state,
        scan=_scan(
            "2026-09-08T14:15:00Z",
            mode="ELEVATED",
            evidence=None,
            spx_eligible=False,
            ndx_eligible=False,
        ),
        policy_events=[],
        require_explicit_evidence=False,
    )

    assert decision["transition"] == "LEGACY_PRESERVE"
    assert apply_cadence_decision(state, decision) == state


def test_production_lane_rejects_missing_adaptive_evidence() -> None:
    with pytest.raises(
        MonitorCadenceError,
        match="scan_cadence_adaptive_evidence_required",
    ):
        evaluate_cadence_transition(
            state=_state(),
            scan=_scan(
                "2026-09-08T14:15:00Z",
                mode="NORMAL",
                evidence=None,
            ),
            policy_events=[],
            require_explicit_evidence=True,
        )
