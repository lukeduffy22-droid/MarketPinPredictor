import asyncio
import hashlib
import json
from datetime import datetime, timezone

import pytest

from backend.api.routers import predict as predict_router
from backend.workstation import build_workstation_state, workstation_state_store

EPOCH_ID = "e" * 64


class _ActiveRuntime:
    subscription_epoch_id = EPOCH_ID
    active_generation = 1
    handoff_status = "active"


@pytest.fixture(autouse=True)
def _active_runtime(monkeypatch):
    runtime = _ActiveRuntime()
    monkeypatch.setattr(predict_router, "get_streamer", lambda: runtime)
    return runtime


def _payload() -> dict[str, object]:
    return {
        "symbol": "SPX",
        "provider": "databento",
        "subscription_epoch_id": EPOCH_ID,
        "subscription_generation": 1,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "price": 7500.0,
        "likely_close": 7510.0,
        "gamma_pin": 7505.0,
        "validation_is_valid": True,
        "gamma_excluded_from_model": False,
        "validation_failure_reasons": [],
    }


def _prediction() -> dict[str, object]:
    return {
        "symbol": "SPX",
        "usable": True,
        "predicted_close": 7511.0,
        "confidence": 72.0,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def _passport(
    label: str,
    *,
    state: str = "RESEARCH_ONLY",
    decision_grade: bool = False,
    prediction_mode: str = "backend_lifecycle",
) -> dict[str, object]:
    origin = {
        "schema_version": "prediction-passport-v1",
        "origin_kind": "test",
        "origin_key": label,
    }
    forecast_id = hashlib.sha256(
        json.dumps(origin, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    passport = {
        "forecast_id": forecast_id,
        "symbol": "SPX",
        "origin": origin,
        "prediction_mode": prediction_mode,
        "state": state,
        "decision_grade": decision_grade,
        "provenance": {},
        "model": {},
        "prediction": {
            "point_estimate": 7511.0,
            "interval_lower": 7500.0,
            "interval_upper": 7520.0,
        },
        "quality": {
            "validation_status": "valid",
            "state_reasons": [],
            "missing_evidence": [],
        },
    }
    passport["record_sha256"] = hashlib.sha256(
        json.dumps(passport, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    return passport


def test_typed_and_raw_prediction_responses_share_research_authority():
    workstation_state_store.reset()
    try:
        workstation_state_store.publish(
            build_workstation_state(
                symbol="SPX",
                payload=_payload(),
                prediction=_prediction(),
                passport=_passport("research"),
            )
        )

        typed = asyncio.run(predict_router._predict_close_impl("SPX"))
        raw = asyncio.run(predict_router._predict_close_impl("SPX", raw=True))

        assert typed.prediction_mode == "backend_lifecycle"
        assert typed.is_estimate is True
        assert typed.decision_grade is False
        assert typed.prediction_authority["authority_state"] == "research_only"
        assert typed.prediction_authority["tcbbo_promoted"] is False
        assert raw["prediction_authority"] == typed.prediction_authority
        assert raw["prediction_authority"]["source_label"].endswith(
            "not TCBBO promoted"
        )
    finally:
        workstation_state_store.reset()


def test_close_overlay_carries_authority_and_cannot_become_decision_grade():
    workstation_state_store.reset()
    try:
        workstation_state_store.publish(
            build_workstation_state(
                symbol="SPX",
                payload=_payload(),
                prediction=_prediction(),
                passport=_passport(
                    "legacy-passport",
                    state="VALID",
                    decision_grade=True,
                ),
            )
        )

        overlay = asyncio.run(predict_router.get_close_overlay("SPX"))

        assert overlay["prediction_mode"] == "backend_lifecycle"
        assert overlay["is_estimate"] is True
        assert overlay["decision_grade"] is False
        assert overlay["prediction_authority"]["authority_state"] == "research_only"
    finally:
        workstation_state_store.reset()


def test_close_overlay_rejects_malformed_persisted_interval():
    workstation_state_store.reset()
    try:
        passport = _passport("malformed-interval")
        passport["prediction"] = {
            "point_estimate": 7511.0,
            "interval_lower": 7520.0,
            "interval_upper": 7500.0,
        }
        passport.pop("record_sha256")
        passport["record_sha256"] = hashlib.sha256(
            json.dumps(passport, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
        workstation_state_store.publish(
            build_workstation_state(
                symbol="SPX",
                payload=_payload(),
                prediction=_prediction(),
                passport=passport,
            )
        )

        with pytest.raises(predict_router.HTTPException) as exc_info:
            asyncio.run(predict_router.get_close_overlay("SPX"))

        assert exc_info.value.status_code == 503
        assert "malformed" in str(exc_info.value.detail).lower()
    finally:
        workstation_state_store.reset()
