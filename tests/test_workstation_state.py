import asyncio
import hashlib
import json
import threading
from contextlib import contextmanager, suppress
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from backend.api.lifecycle import (
    WorkstationPredictionCoordinator,
    run_workstation_state_loop,
)
from backend.api.schemas import WorkstationEventV1, WorkstationStateV1
from backend.workstation import (
    WorkstationStateStore,
    build_workstation_state,
    legacy_dashboard_payload,
    unavailable_workstation_state,
    workstation_state_store,
)


def _payload(symbol: str = "SPX", **overrides):
    payload = {
        "symbol": symbol,
        "provider": "databento",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "price": 7500.0,
        "likely_close": 7510.0,
        "gamma_pin": 7505.0,
        "zero_gamma": None,
        "max_pain": 7520.0,
        "gross_gex": 100.0,
        "net_gex": -5.0,
        "contracts": 50,
        "fresh_quote_count": 25,
        "subscription_epoch_id": "e" * 64,
        "subscription_generation": 3,
        "validation_is_valid": True,
        "gamma_excluded_from_model": False,
        "validation_failure_reasons": [],
    }
    payload.update(overrides)
    return payload


def _prediction(symbol: str = "SPX"):
    return {
        "symbol": symbol,
        "usable": True,
        "predicted_close": 7511.0,
        "current_price": 7500.0,
        "confidence": 72.0,
        "model_type": "Databento Quant Ensemble",
        "model_version": "test-v1",
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def _passport(
    label: str = "research",
    *,
    state: str = "RESEARCH_ONLY",
    point_estimate: float | None = 7511.0,
    decision_grade: bool = False,
    prediction_mode: str = "backend_lifecycle",
    quality: dict | None = None,
    interval_lower: float | None = 7500.0,
    interval_upper: float | None = 7520.0,
    symbol: str = "SPX",
    prediction_snapshot_id: int | None = None,
):
    origin = {
        "schema_version": "prediction-passport-v1",
        "origin_kind": "test",
        "origin_key": label,
    }
    identity = {
        "schema_version": origin["schema_version"],
        "origin_kind": origin["origin_kind"],
        "origin_key": origin["origin_key"],
    }
    forecast_id = hashlib.sha256(
        json.dumps(identity, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    passport_origin = {
        "schema_version": "prediction-passport-v1",
        "origin_kind": "test",
        "origin_key": label,
    }
    if prediction_snapshot_id is not None:
        passport_origin["prediction_snapshot_id"] = prediction_snapshot_id
    passport = {
        "forecast_id": forecast_id,
        "symbol": symbol,
        "origin": passport_origin,
        "prediction_mode": prediction_mode,
        "state": state,
        "decision_grade": decision_grade,
        "provenance": {},
        "model": {},
        "prediction": {
            "point_estimate": point_estimate,
            "interval_lower": interval_lower,
            "interval_upper": interval_upper,
        },
        "quality": quality or {
            "validation_status": "valid",
            "state_reasons": [],
            "missing_evidence": [],
        },
    }
    passport["record_sha256"] = hashlib.sha256(
        json.dumps(passport, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    return passport


class _ActiveStreamer:
    subscription_epoch_id = "e" * 64
    active_generation = 3
    handoff_status = "active"

    @staticmethod
    def get_latest_pin(_symbol):
        return None


def test_reads_do_not_advance_sequence_or_state_revision():
    store = WorkstationStateStore()
    published = store.publish(
        build_workstation_state(
            symbol="SPX", payload=_payload(), prediction=_prediction()
        )
    )

    first = store.get("SPX")
    second = store.get("SPX")
    snapshot = store.snapshot_event()

    assert store.sequence == published["sequence"] == 1
    assert first["state_revision"] == second["state_revision"] == 1
    assert first["event_id"] == second["event_id"] == "1"
    assert snapshot["sequence"] == 1


def test_passport_backed_legacy_prediction_is_research_only_everywhere():
    state = build_workstation_state(
        symbol="SPX",
        payload=_payload(),
        prediction=_prediction(),
        passport=_passport(),
    )
    dashboard = legacy_dashboard_payload(state)

    assert state["prediction_authority"]["authority_state"] == "research_only"
    assert state["prediction_authority"]["tcbbo_promoted"] is False
    assert state["decision_grade"] is False
    assert state["prediction"]["prediction_authority"] == state["prediction_authority"]
    assert dashboard["prediction"]["source_label"].endswith(
        "not TCBBO promoted"
    )
    assert dashboard["market_data_source_label"] == "Databento Live"


def test_numeric_prediction_without_persisted_passport_fails_closed():
    state = build_workstation_state(
        symbol="SPX", payload=_payload(), prediction=_prediction()
    )

    assert state["status"] == "invalid"
    assert state["forecast_id"] is None
    assert state["forecast_state"] == "ABSTAIN"
    assert state["prediction"]["predicted_close"] is None


@pytest.mark.parametrize(
    ("passport", "snapshot_id", "expected_reason"),
    [
        (_passport("wrong-symbol", symbol="NDX"), None, "PASSPORT_SYMBOL_MISMATCH"),
        (
            _passport("wrong-snapshot", prediction_snapshot_id=18),
            17,
            "PASSPORT_SNAPSHOT_ID_MISMATCH",
        ),
    ],
)
def test_workstation_rejects_cross_bound_complete_passport(
    passport, snapshot_id, expected_reason
):
    state = build_workstation_state(
        symbol="SPX",
        payload=_payload(),
        prediction=_prediction(),
        passport=passport,
        prediction_snapshot_id=snapshot_id,
    )

    assert state["status"] == "invalid"
    assert state["forecast_id"] is None
    assert state["prediction"]["predicted_close"] is None
    assert expected_reason in state["health"]["validation_failure_reasons"]
    assert "PASSPORT_INCOMPLETE_OR_UNVERIFIED" in state["health"][
        "validation_failure_reasons"
    ]


def test_generic_workstation_cannot_self_attest_tcbbo_promotion():
    state = build_workstation_state(
        symbol="SPX",
        payload=_payload(),
        prediction=_prediction(),
        passport=_passport(
            "untrusted-tcbbo-claim",
            state="VALID",
            decision_grade=True,
            prediction_mode="tcbbo_promoted",
        ),
    )

    assert state["prediction_authority"]["authority_state"] == "research_only"
    assert state["prediction_authority"]["promotion_verified"] is False
    assert state["decision_grade"] is False


def test_event_history_requires_full_resync_after_a_gap():
    store = WorkstationStateStore(history_size=2)
    for index in range(3):
        store.publish(
            build_workstation_state(
                symbol="SPX",
                payload=_payload(
                    timestamp=(datetime.now(timezone.utc) + timedelta(seconds=index)).isoformat()
                ),
                prediction=_prediction(),
            )
        )

    [event] = store.events_after(0)

    assert event["event_type"] == "resync_required"
    assert event["requires_resync"] is True
    assert event["sequence"] == 3
    assert len(event["states"]) == 1
    WorkstationEventV1.model_validate(event)


def test_future_event_cursor_requires_resync_without_advancing_sequence():
    store = WorkstationStateStore()
    published = store.publish(
        build_workstation_state(
            symbol="SPX", payload=_payload(), prediction=_prediction()
        )
    )

    [event] = store.events_after(99)

    assert event["event_type"] == "resync_required"
    assert event["requested_after_sequence"] == 99
    assert event["latest_sequence"] == published["sequence"] == 1
    assert store.sequence == 1
    WorkstationEventV1.model_validate(event)


def test_state_contract_preserves_missing_levels_and_marks_stale_nonusable():
    stale_payload = _payload(
        timestamp=(datetime.now(timezone.utc) - timedelta(minutes=5)).isoformat(),
        quote_age_seconds=300.0,
        gamma_pin=None,
        zero_gamma=None,
        max_pain=None,
    )
    state = build_workstation_state(
        symbol="SPX", payload=stale_payload, prediction=None
    )

    validated = WorkstationStateV1.model_validate(state)

    assert validated.status == "stale"
    assert validated.forecast_id is None
    assert validated.prediction is not None
    assert validated.prediction["usable"] is False
    assert validated.prediction["gamma_pin"] is None
    assert validated.prediction["zero_gamma"] is None
    assert validated.prediction["max_pain"] is None
    assert "LIVE_DATA_STALE" in validated.health["validation_failure_reasons"]


def test_fallback_is_numeric_free_and_never_decision_grade():
    state = build_workstation_state(
        symbol="SPX",
        payload=_payload(
            provider="historical-fallback",
            historical_context_only=True,
            validation_is_valid=False,
        ),
        prediction={**_prediction(), "predicted_close": 7511.0},
        passport={
            "forecast_id": "fallback-passport",
            "state": "ABSTAIN",
            "decision_grade": False,
            "quality": {"state_reasons": ["NON_PRODUCTION_FALLBACK"]},
        },
    )

    assert state["status"] == "closed_context"
    assert state["forecast_state"] == "ABSTAIN"
    assert state["decision_grade"] is False
    assert state["prediction"]["usable"] is False
    assert state["prediction"]["predicted_close"] is None
    assert "NON_PRODUCTION_FALLBACK" in state["health"][
        "validation_failure_reasons"
    ]


@pytest.mark.parametrize(
    "provenance_field",
    ["universe_provenance", "oi_analytics_provenance"],
)
def test_nested_fallback_provenance_is_numeric_free_and_never_decision_grade(
    provenance_field,
):
    state = build_workstation_state(
        symbol="SPX",
        payload=_payload(**{provenance_field: {"is_fallback": True}}),
        prediction=_prediction(),
    )

    assert state["status"] == "closed_context"
    assert state["forecast_state"] == "ABSTAIN"
    assert state["decision_grade"] is False
    assert state["prediction"]["usable"] is False
    assert state["prediction"]["predicted_close"] is None
    assert "NON_PRODUCTION_FALLBACK" in state["health"][
        "validation_failure_reasons"
    ]


def test_unpublished_state_is_stable_and_does_not_claim_a_revision():
    first = unavailable_workstation_state("SPX")
    second = unavailable_workstation_state("SPX")

    assert first == second
    assert first["sequence"] == first["state_revision"] == 0
    assert first["generated_at_utc"] is None
    assert first["prediction_authority"]["authority_state"] == "unavailable"
    WorkstationStateV1.model_validate(first)


def test_coordinator_computes_on_publish_and_persists_only_when_due():
    workstation_state_store.reset()
    saves = []
    shadow_rows = []
    clock_values = iter([100.0, 101.0])

    class _Streamer:
        subscription_epoch_id = "e" * 64
        active_generation = 3
        handoff_status = "active"

        @staticmethod
        def get_latest_pin(symbol):
            return None

    class _Shadow:
        @staticmethod
        def record(payload):
            shadow_rows.append(payload)
            return SimpleNamespace(error=None)

    def _save(prediction, prediction_mode):
        saves.append((prediction, prediction_mode))
        return SimpleNamespace(id=17)

    coordinator = WorkstationPredictionCoordinator(
        streamer=_Streamer(),
        shadow_research=_Shadow(),
        capture_seconds=60.0,
        prediction_builder=lambda symbol, payload, **_: _prediction(symbol),
        snapshot_saver=_save,
        context_loader=lambda _symbol: {},
        adjustment_builder=lambda *_args: (0.0, []),
        monotonic_clock=lambda: next(clock_values),
    )

    first = coordinator.process_payload(_payload(calculation_id="committed-first"))
    second = coordinator.process_payload(
        _payload(timestamp=(datetime.now(timezone.utc) + timedelta(seconds=1)).isoformat())
    )

    assert first["sequence"] == 1
    assert second["sequence"] == 2
    assert len(saves) == 1
    assert saves[0][1] == "backend_periodic"
    assert len(shadow_rows) == 1
    assert first["prediction"]["prediction_snapshot_id"] == 17
    assert second["prediction"]["prediction_snapshot_id"] is None
    assert first["forecast_id"] is None
    assert first["status"] == "invalid"
    assert first["prediction"]["predicted_close"] is None
    assert "PASSPORT_ISSUANCE_UNAVAILABLE" in first["health"][
        "validation_failure_reasons"
    ]


def test_invalid_payload_never_calls_prediction_or_persistence():
    workstation_state_store.reset()

    def _forbidden(*_args, **_kwargs):
        raise AssertionError("invalid state must not invoke prediction or persistence")

    coordinator = WorkstationPredictionCoordinator(
        streamer=_ActiveStreamer(),
        shadow_research=object(),
        capture_seconds=60.0,
        prediction_builder=_forbidden,
        snapshot_saver=_forbidden,
        context_loader=_forbidden,
        adjustment_builder=_forbidden,
    )

    state = coordinator.process_payload(
        _payload(
            validation_is_valid=False,
            validation_failure_reasons=["CHAIN_TOO_THIN"],
        )
    )

    assert state["status"] == "invalid"
    assert state["health"]["usable_for_prediction"] is False
    assert state["health"]["validation_failure_reasons"] == [
        "CHAIN_TOO_THIN",
        "SOURCE_VALIDATION_NOT_CONFIRMED",
    ]


@pytest.mark.parametrize(
    "provenance_field",
    ["universe_provenance", "oi_analytics_provenance"],
)
def test_nested_fallback_never_calls_prediction_context_or_persistence(
    provenance_field,
):
    workstation_state_store.reset()

    def _forbidden(*_args, **_kwargs):
        raise AssertionError("fallback state must not invoke inference or persistence")

    coordinator = WorkstationPredictionCoordinator(
        streamer=_ActiveStreamer(),
        shadow_research=object(),
        capture_seconds=60.0,
        prediction_builder=_forbidden,
        snapshot_saver=_forbidden,
        context_loader=_forbidden,
        adjustment_builder=_forbidden,
    )

    state = coordinator.process_payload(
        _payload(**{provenance_field: {"is_fallback": True}})
    )

    assert state["status"] == "closed_context"
    assert state["health"]["usable_for_prediction"] is False
    assert state["prediction"]["predicted_close"] is None
    assert "NON_PRODUCTION_FALLBACK" in state["health"][
        "validation_failure_reasons"
    ]


def test_passport_writer_is_lifecycle_owned_and_optional():
    workstation_state_store.reset()
    passport_calls = []

    def _passport_writer(prediction, **kwargs):
        passport_calls.append((prediction, kwargs))
        return _passport(
            "forecast-abstain-1",
            state="ABSTAIN",
            point_estimate=None,
            interval_lower=None,
            interval_upper=None,
            quality={
                "validation_status": "invalid",
                "state_reasons": ["CHAIN_TOO_THIN"],
                "missing_evidence": ["calculation_input_sha256"],
            },
        )

    coordinator = WorkstationPredictionCoordinator(
        streamer=_ActiveStreamer(),
        shadow_research=object(),
        capture_seconds=60.0,
        prediction_builder=lambda *_args, **_kwargs: pytest.fail(
            "invalid state must not run inference"
        ),
        snapshot_saver=lambda *_args, **_kwargs: pytest.fail(
            "invalid state must not write a prediction snapshot"
        ),
        context_loader=lambda *_args, **_kwargs: pytest.fail(
            "invalid state must not load inference context"
        ),
        adjustment_builder=lambda *_args, **_kwargs: pytest.fail(
            "invalid state must not compute an adjustment"
        ),
        passport_writer=_passport_writer,
    )

    state = coordinator.process_payload(
        _payload(
            validation_is_valid=False,
            validation_failure_reasons=["CHAIN_TOO_THIN"],
        )
    )

    assert len(passport_calls) == 1
    assert passport_calls[0][1]["requested_state"] == "abstain"
    assert state["forecast_id"] == _passport(
        "forecast-abstain-1",
        state="ABSTAIN",
        point_estimate=None,
        interval_lower=None,
        interval_upper=None,
        quality={
            "validation_status": "invalid",
            "state_reasons": ["CHAIN_TOO_THIN"],
            "missing_evidence": ["calculation_input_sha256"],
        },
    )["forecast_id"]
    assert state["decision_grade"] is False
    assert state["health"]["missing_evidence"] == ["calculation_input_sha256"]


def test_guarded_passport_origin_is_content_addressed_across_freshness_transition(
    monkeypatch,
):
    workstation_state_store.reset()
    passport_calls = []
    ages = iter((1.0, 31.0))

    def _passport_writer(prediction, **kwargs):
        passport_calls.append((prediction, kwargs))
        return _passport(
            f"guarded-{len(passport_calls)}",
            state="ABSTAIN",
            point_estimate=None,
            interval_lower=None,
            interval_upper=None,
            quality={
                "validation_status": "invalid",
                "state_reasons": prediction["validation_failure_reasons"],
                "missing_evidence": [],
            },
        )

    monkeypatch.setattr(
        "backend.api.lifecycle.payload_age_seconds", lambda _payload: next(ages)
    )
    coordinator = WorkstationPredictionCoordinator(
        streamer=_ActiveStreamer(),
        shadow_research=object(),
        capture_seconds=60.0,
        prediction_builder=lambda *_args, **_kwargs: pytest.fail(
            "invalid state must not run inference"
        ),
        passport_writer=_passport_writer,
    )
    payload = _payload(
        validation_is_valid=False,
        validation_failure_reasons=["PROCESSING_CLOCK_NOT_SYNCHRONIZED"],
    )

    coordinator.process_payload(payload)
    coordinator.process_payload(payload)

    assert [call[0]["quote_age_seconds"] for call in passport_calls] == [1.0, 31.0]
    assert passport_calls[0][1]["origin_key"] != passport_calls[1][1]["origin_key"]


@pytest.mark.parametrize(
    ("payload_overrides", "expected_reason"),
    [
        ({"subscription_generation": 2}, "SUBSCRIPTION_GENERATION_MISMATCH"),
        ({"subscription_epoch_id": "f" * 64}, "SUBSCRIPTION_EPOCH_MISMATCH"),
        ({}, "HANDOFF_NOT_ACTIVE:warming"),
    ],
)
def test_stream_generation_and_handoff_gates_abstain_before_inference(
    payload_overrides, expected_reason
):
    workstation_state_store.reset()
    passport_calls = []

    class _Streamer:
        subscription_epoch_id = "e" * 64
        active_generation = 3
        handoff_status = "warming" if not payload_overrides else "active"

    def _passport_writer(prediction, **kwargs):
        passport_calls.append((prediction, kwargs))
        return _passport(
            "guarded-passport",
            state="ABSTAIN",
            point_estimate=None,
            interval_lower=None,
            interval_upper=None,
            quality={
                "validation_status": "invalid",
                "state_reasons": prediction["validation_failure_reasons"],
                "missing_evidence": [],
            },
        )

    coordinator = WorkstationPredictionCoordinator(
        streamer=_Streamer(),
        shadow_research=object(),
        capture_seconds=60.0,
        prediction_builder=lambda *_args, **_kwargs: pytest.fail(
            "guarded state must not run inference"
        ),
        snapshot_saver=lambda *_args, **_kwargs: pytest.fail(
            "guarded state must not persist a prediction"
        ),
        context_loader=lambda *_args, **_kwargs: pytest.fail(
            "guarded state must not load inference context"
        ),
        adjustment_builder=lambda *_args, **_kwargs: pytest.fail(
            "guarded state must not compute an adjustment"
        ),
        passport_writer=_passport_writer,
    )

    state = coordinator.process_payload(_payload(**payload_overrides))

    assert passport_calls[0][1]["requested_state"] == "abstain"
    assert state["status"] == "invalid"
    assert state["forecast_state"] == "ABSTAIN"
    assert state["prediction"]["predicted_close"] is None
    assert expected_reason in state["health"]["validation_failure_reasons"]


def test_lifecycle_guard_fails_closed_without_active_process_epoch():
    coordinator = WorkstationPredictionCoordinator(
        streamer=object(),
        shadow_research=object(),
        capture_seconds=60.0,
        prediction_builder=lambda *_args, **_kwargs: None,
    )

    guard = coordinator._guard_snapshot(_payload())

    assert guard["requested_state"] == "abstain"
    assert "SUBSCRIPTION_EPOCH_MISMATCH" in guard["validation_failure_reasons"]


@pytest.mark.parametrize(
    "streamer",
    [
        SimpleNamespace(
            subscription_epoch_id="e" * 64,
            active_generation=None,
            handoff_status="active",
        ),
        SimpleNamespace(
            subscription_epoch_id="e" * 64,
            active_generation=0,
            handoff_status="active",
        ),
        SimpleNamespace(
            subscription_epoch_id="e" * 64,
            active_generation=3,
        ),
    ],
)
def test_lifecycle_guard_requires_explicit_active_generation_and_handoff(streamer):
    coordinator = WorkstationPredictionCoordinator(
        streamer=streamer,
        shadow_research=object(),
        capture_seconds=60.0,
    )

    guard = coordinator._guard_snapshot(_payload())

    assert guard["requested_state"] == "abstain"
    assert any(
        reason == "SUBSCRIPTION_GENERATION_MISMATCH"
        or reason.startswith("HANDOFF_NOT_ACTIVE:")
        for reason in guard["validation_failure_reasons"]
    )


def test_lifecycle_guard_requires_explicit_source_validation_proof():
    coordinator = WorkstationPredictionCoordinator(
        streamer=_ActiveStreamer(),
        shadow_research=object(),
        capture_seconds=60.0,
    )
    payload = _payload()
    payload.pop("gamma_excluded_from_model")

    guard = coordinator._guard_snapshot(payload)

    assert guard["requested_state"] == "abstain"
    assert "SOURCE_VALIDATION_NOT_CONFIRMED" in guard[
        "validation_failure_reasons"
    ]


def test_malformed_passport_result_cannot_publish_numeric_forecast():
    workstation_state_store.reset()
    coordinator = WorkstationPredictionCoordinator(
        streamer=_ActiveStreamer(),
        shadow_research=object(),
        capture_seconds=1_000_000.0,
        prediction_builder=lambda symbol, _payload_value, **_kwargs: _prediction(symbol),
        context_loader=lambda _symbol: {},
        adjustment_builder=lambda *_args: (0.0, []),
        monotonic_clock=lambda: 1.0,
        passport_writer=lambda *_args, **_kwargs: {
            "forecast_id": "not-a-persisted-passport",
            "state": "RESEARCH_ONLY",
        },
    )

    state = coordinator.process_payload(_payload())

    assert state["status"] == "invalid"
    assert state["forecast_id"] is None
    assert state["prediction"]["predicted_close"] is None
    assert "PASSPORT_ISSUANCE_UNAVAILABLE" in state["health"][
        "validation_failure_reasons"
    ]


def test_inference_failure_publishes_numeric_free_unavailable_state():
    workstation_state_store.reset()
    passport_calls = []

    def _passport_writer(_prediction_value, **kwargs):
        passport_calls.append(kwargs)
        return _passport(
            "inference-unavailable",
            state="UNAVAILABLE",
            point_estimate=None,
            interval_lower=None,
            interval_upper=None,
            quality={
                "validation_status": "invalid",
                "state_reasons": ["INFERENCE_PIPELINE_UNAVAILABLE"],
                "missing_evidence": [],
            },
        )

    coordinator = WorkstationPredictionCoordinator(
        streamer=_ActiveStreamer(),
        shadow_research=object(),
        capture_seconds=0.0,
        prediction_builder=lambda *_args, **_kwargs: (_ for _ in ()).throw(
            RuntimeError("test failure")
        ),
        snapshot_saver=lambda *_args, **_kwargs: pytest.fail(
            "failed inference must not persist"
        ),
        context_loader=lambda _symbol: {},
        adjustment_builder=lambda *_args: (0.0, []),
        passport_writer=_passport_writer,
    )

    state = coordinator.process_payload(_payload())

    assert passport_calls[0]["requested_state"] == "unavailable"
    assert state["status"] == "unavailable"
    assert state["forecast_state"] == "UNAVAILABLE"
    assert state["prediction"] is None
    assert "INFERENCE_PIPELINE_UNAVAILABLE" in state["health"][
        "validation_failure_reasons"
    ]


def test_fresh_to_stale_transition_retains_forecast_identity(monkeypatch):
    from backend.api import lifecycle as lifecycle_module
    from backend import workstation as workstation_module

    workstation_state_store.reset()
    age = {"seconds": 1.0}
    monkeypatch.setattr(
        lifecycle_module,
        "payload_age_seconds",
        lambda _payload_value: age["seconds"],
    )
    monkeypatch.setattr(
        workstation_module,
        "payload_age_seconds",
        lambda _payload_value: age["seconds"],
    )
    passport_calls = []

    def _passport_writer(_prediction_value, **_kwargs):
        passport_calls.append(True)
        return _passport("retained-revision")

    coordinator = WorkstationPredictionCoordinator(
        streamer=_ActiveStreamer(),
        shadow_research=object(),
        capture_seconds=1_000_000.0,
        prediction_builder=lambda symbol, _payload_value, **_kwargs: _prediction(symbol),
        context_loader=lambda _symbol: {},
        adjustment_builder=lambda *_args: (0.0, []),
        monotonic_clock=lambda: 1.0,
        passport_writer=_passport_writer,
    )
    payload = _payload()

    fresh = coordinator.process_payload(payload)
    age["seconds"] = lifecycle_module.LIVE_DATA_STALE_AFTER_SECONDS + 1.0
    stale = coordinator.process_payload(payload)

    assert len(passport_calls) == 1
    assert fresh["status"] == "live"
    assert fresh["prediction"]["live_inference_feature_schema_version"] == (
        "databento-gex-structure-v1"
    )
    assert fresh["prediction"]["live_inference_features"]["price"] == pytest.approx(
        7500.0
    )
    assert fresh["prediction"]["live_inference_features"][
        "zero_gamma_distance"
    ] is None
    assert stale["status"] == "stale"
    assert stale["forecast_id"] == fresh["forecast_id"]
    assert stale["forecast_state"] == "RESEARCH_ONLY"
    assert stale["prediction"]["predicted_close"] is None
    assert "LIVE_DATA_STALE" in stale["health"]["validation_failure_reasons"]
    [event] = workstation_state_store.events_after(fresh["sequence"])
    assert event["states"][0]["forecast_id"] == fresh["forecast_id"]


def test_stale_payload_issues_numeric_free_stale_passport():
    workstation_state_store.reset()
    passport_calls = []

    def _passport_writer(prediction, **kwargs):
        passport_calls.append((prediction, kwargs))
        return _passport(
            "stale-passport",
            state="STALE",
            point_estimate=None,
            interval_lower=None,
            interval_upper=None,
            quality={
                "validation_status": "stale",
                "state_reasons": ["SOURCE_STALE"],
                "missing_evidence": [],
            },
        )

    coordinator = WorkstationPredictionCoordinator(
        streamer=_ActiveStreamer(),
        shadow_research=object(),
        capture_seconds=60.0,
        prediction_builder=lambda *_args, **_kwargs: pytest.fail(
            "stale state must not run inference"
        ),
        snapshot_saver=lambda *_args, **_kwargs: pytest.fail(
            "stale state must not persist a prediction"
        ),
        context_loader=lambda *_args, **_kwargs: pytest.fail(
            "stale state must not load inference context"
        ),
        adjustment_builder=lambda *_args, **_kwargs: pytest.fail(
            "stale state must not compute an adjustment"
        ),
        passport_writer=_passport_writer,
    )

    state = coordinator.process_payload(
        _payload(timestamp=(datetime.now(timezone.utc) - timedelta(minutes=5)).isoformat())
    )

    assert passport_calls[0][1]["requested_state"] == "stale"
    assert state["status"] == "stale"
    assert state["forecast_state"] == "STALE"
    assert state["prediction"]["usable"] is False
    assert state["prediction"]["predicted_close"] is None


def test_missing_price_takes_unavailable_precedence_over_staleness():
    workstation_state_store.reset()
    passport_calls = []

    def _passport_writer(prediction, **kwargs):
        passport_calls.append((prediction, kwargs))
        return _passport(
            "unavailable-passport",
            state="UNAVAILABLE",
            point_estimate=None,
            interval_lower=None,
            interval_upper=None,
            quality={
                "validation_status": "invalid",
                "state_reasons": ["STATE_UNAVAILABLE"],
                "missing_evidence": [],
            },
        )

    coordinator = WorkstationPredictionCoordinator(
        streamer=_ActiveStreamer(),
        shadow_research=object(),
        capture_seconds=60.0,
        prediction_builder=lambda *_args, **_kwargs: pytest.fail(
            "unavailable state must not run inference"
        ),
        passport_writer=_passport_writer,
    )

    state = coordinator.process_payload(_payload(timestamp=None, price=None))

    assert passport_calls[0][1]["requested_state"] == "unavailable"
    assert state["status"] == "unavailable"
    assert state["forecast_state"] == "UNAVAILABLE"


def test_generation_change_during_inference_cannot_persist_or_publish_live():
    workstation_state_store.reset()
    saves = []

    class _Streamer:
        subscription_epoch_id = "e" * 64
        active_generation = 3
        handoff_status = "active"

        @staticmethod
        def get_latest_pin(_symbol):
            return None

    streamer = _Streamer()

    def _prediction_builder(symbol, _payload_value, **_kwargs):
        streamer.active_generation = 4
        streamer.handoff_status = "warming"
        return _prediction(symbol)

    def _passport_writer(prediction, **kwargs):
        state = "ABSTAIN" if kwargs.get("requested_state") == "abstain" else "RESEARCH_ONLY"
        return _passport(
            "post-inference-guard",
            state=state,
            point_estimate=None if state == "ABSTAIN" else 7511.0,
            interval_lower=None if state == "ABSTAIN" else 7500.0,
            interval_upper=None if state == "ABSTAIN" else 7520.0,
            quality={
                "validation_status": "invalid" if state == "ABSTAIN" else "valid",
                "state_reasons": prediction.get("validation_failure_reasons") or [],
                "missing_evidence": [],
            },
        )

    coordinator = WorkstationPredictionCoordinator(
        streamer=streamer,
        shadow_research=object(),
        capture_seconds=0.0,
        prediction_builder=_prediction_builder,
        snapshot_saver=lambda *_args, **_kwargs: saves.append(True),
        context_loader=lambda _symbol: {},
        adjustment_builder=lambda *_args: (0.0, []),
        passport_writer=_passport_writer,
    )

    state = coordinator.process_payload(_payload())

    assert saves == []
    assert state["status"] == "invalid"
    assert state["forecast_state"] == "ABSTAIN"
    assert state["prediction"]["usable"] is False
    assert "SUBSCRIPTION_GENERATION_MISMATCH" in state["health"][
        "validation_failure_reasons"
    ]
    assert "HANDOFF_NOT_ACTIVE:warming" in state["health"][
        "validation_failure_reasons"
    ]


def test_future_source_timestamp_abstains_before_inference_or_persistence():
    def _forbidden(*_args, **_kwargs):
        raise AssertionError("future-dated source evidence must fail before inference")

    coordinator = WorkstationPredictionCoordinator(
        streamer=_ActiveStreamer(),
        shadow_research=object(),
        capture_seconds=0.0,
        prediction_builder=_forbidden,
        snapshot_saver=_forbidden,
        context_loader=_forbidden,
        adjustment_builder=_forbidden,
    )

    state = coordinator.process_payload(
        _payload(timestamp=(datetime.now(timezone.utc) + timedelta(minutes=5)).isoformat())
    )

    assert state["status"] == "stale"
    assert state["forecast_state"] == "STALE"
    assert state["prediction"]["predicted_close"] is None
    assert "LIVE_DATA_STALE" in state["health"]["validation_failure_reasons"]


def test_missing_usable_abstains_without_numeric_persistence():
    passport_calls = []

    def _prediction_without_usable(symbol, _payload_value, **_kwargs):
        result = _prediction(symbol)
        result.pop("usable")
        return result

    def _passport_writer(_prediction_value, **kwargs):
        passport_calls.append(kwargs)
        return _passport(
            "missing-usable",
            state="ABSTAIN",
            point_estimate=None,
            interval_lower=None,
            interval_upper=None,
        )

    coordinator = WorkstationPredictionCoordinator(
        streamer=_ActiveStreamer(),
        shadow_research=object(),
        capture_seconds=0.0,
        prediction_builder=_prediction_without_usable,
        snapshot_saver=lambda *_args, **_kwargs: pytest.fail(
            "missing usable must not persist"
        ),
        context_loader=lambda _symbol: {},
        adjustment_builder=lambda *_args: (0.0, []),
        passport_writer=_passport_writer,
    )

    state = coordinator.process_payload(_payload())

    assert passport_calls[0]["requested_state"] == "abstain"
    assert state["forecast_state"] == "ABSTAIN"
    assert state["prediction"]["predicted_close"] is None
    assert "PREDICTION_USABLE_NOT_CONFIRMED" in state["health"][
        "validation_failure_reasons"
    ]


def test_generation_change_during_snapshot_commit_leaves_no_numeric_artifact():
    durable_snapshots = []
    numeric_passports = []

    class _Streamer(_ActiveStreamer):
        @contextmanager
        def prediction_publication_guard(
            self, *, subscription_epoch_id, subscription_generation
        ):
            yield bool(
                subscription_epoch_id == self.subscription_epoch_id
                and subscription_generation == self.active_generation
                and self.handoff_status == "active"
            )

    streamer = _Streamer()

    def _save(_prediction_value, *, publication_guard, **_kwargs):
        streamer.active_generation = 4
        streamer.handoff_status = "warming"
        if publication_guard():
            durable_snapshots.append(True)
            return SimpleNamespace(id=99)
        return None

    def _passport_writer(prediction, **kwargs):
        if kwargs.get("requested_state") is None:
            numeric_passports.append(prediction["predicted_close"])
            return _passport("obsolete-numeric")
        return _passport(
            "generation-abstain",
            state="ABSTAIN",
            point_estimate=None,
            interval_lower=None,
            interval_upper=None,
        )

    coordinator = WorkstationPredictionCoordinator(
        streamer=streamer,
        shadow_research=object(),
        capture_seconds=0.0,
        prediction_builder=lambda symbol, _payload_value, **_kwargs: _prediction(symbol),
        snapshot_saver=_save,
        context_loader=lambda _symbol: {},
        adjustment_builder=lambda *_args: (0.0, []),
        passport_writer=_passport_writer,
    )

    state = coordinator.process_payload(_payload())

    assert durable_snapshots == []
    assert numeric_passports == []
    assert state["forecast_state"] == "ABSTAIN"
    assert state["prediction"]["predicted_close"] is None
    assert "SUBSCRIPTION_GENERATION_MISMATCH" in state["health"][
        "validation_failure_reasons"
    ]


def test_final_live_publication_holds_generation_transition_barrier(monkeypatch):
    transition_attempted = threading.Event()
    transition_completed = threading.Event()
    transition_threads = []

    class _LockedStreamer(_ActiveStreamer):
        def __init__(self):
            self._publication_lock = threading.RLock()
            self.subscription_epoch_id = "e" * 64
            self.active_generation = 3
            self.handoff_status = "active"

        @contextmanager
        def prediction_publication_guard(
            self, *, subscription_epoch_id, subscription_generation
        ):
            with self._publication_lock:
                yield bool(
                    subscription_epoch_id == self.subscription_epoch_id
                    and subscription_generation == self.active_generation
                    and self.handoff_status == "active"
                )

        def transition(self):
            transition_attempted.set()
            with self._publication_lock:
                self.active_generation = 4
                self.handoff_status = "warming"
            transition_completed.set()

    streamer = _LockedStreamer()
    original_publish = workstation_state_store.publish

    def _publish_while_transition_attempts(state):
        thread = threading.Thread(target=streamer.transition)
        transition_threads.append(thread)
        thread.start()
        assert transition_attempted.wait(1.0)
        assert not transition_completed.wait(0.05)
        return original_publish(state)

    monkeypatch.setattr(workstation_state_store, "publish", _publish_while_transition_attempts)
    coordinator = WorkstationPredictionCoordinator(
        streamer=streamer,
        shadow_research=object(),
        capture_seconds=1_000_000.0,
        prediction_builder=lambda symbol, _payload_value, **_kwargs: _prediction(symbol),
        context_loader=lambda _symbol: {},
        adjustment_builder=lambda *_args: (0.0, []),
        monotonic_clock=lambda: 1.0,
        passport_writer=lambda *_args, **_kwargs: _passport("locked-publication"),
    )

    state = coordinator.process_payload(_payload())
    for thread in transition_threads:
        thread.join(timeout=1.0)

    assert state["status"] == "live"
    assert transition_completed.is_set()


def test_published_state_and_events_are_json_serializable():
    store = WorkstationStateStore()
    state = build_workstation_state(
        symbol="SPX",
        payload=_payload(timestamp=datetime.now(timezone.utc)),
        prediction=_prediction(),
    )

    published = store.publish(state)

    json.dumps(published, allow_nan=False)
    json.dumps(store.snapshot_event(), allow_nan=False)


def test_lifecycle_loop_deduplicates_an_unchanged_backend_revision():
    payload = _payload()

    class _Streamer:
        @staticmethod
        def get_all_latest():
            return {"SPX": payload}

    class _Coordinator:
        def __init__(self):
            self.calls = 0

        def process_payload(self, _payload_value):
            self.calls += 1

    coordinator = _Coordinator()

    async def _run_briefly():
        task = asyncio.create_task(
            run_workstation_state_loop(_Streamer(), coordinator, poll_seconds=0.05)
        )
        await asyncio.sleep(0.14)
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task

    asyncio.run(_run_briefly())

    assert coordinator.calls == 1


def test_lifecycle_loop_republishes_stream_generation_and_handoff_changes():
    payload = _payload()

    class _Streamer:
        active_generation = 3
        handoff_status = "active"

        @staticmethod
        def get_all_latest():
            return {"SPX": payload}

    streamer = _Streamer()

    class _Coordinator:
        def __init__(self):
            self.calls = 0

        def process_payload(self, _payload_value):
            self.calls += 1
            if self.calls == 1:
                streamer.active_generation = 4
                streamer.handoff_status = "warming"

    coordinator = _Coordinator()

    async def _run_briefly():
        task = asyncio.create_task(
            run_workstation_state_loop(streamer, coordinator, poll_seconds=0.05)
        )
        await asyncio.sleep(0.14)
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task

    asyncio.run(_run_briefly())

    assert coordinator.calls == 2


def test_lifecycle_loop_republishes_active_epoch_change():
    payload = _payload()

    class _Streamer:
        subscription_epoch_id = "e" * 64
        active_generation = 3
        handoff_status = "active"

        @staticmethod
        def get_all_latest():
            return {"SPX": payload}

    streamer = _Streamer()

    class _Coordinator:
        def __init__(self):
            self.calls = 0

        def process_payload(self, _payload_value):
            self.calls += 1
            if self.calls == 1:
                streamer.subscription_epoch_id = "f" * 64

    coordinator = _Coordinator()

    async def _run_briefly():
        task = asyncio.create_task(
            run_workstation_state_loop(streamer, coordinator, poll_seconds=0.05)
        )
        await asyncio.sleep(0.14)
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task

    asyncio.run(_run_briefly())

    assert coordinator.calls == 2


def test_lifecycle_loop_publishes_one_stale_transition(monkeypatch):
    from backend.api import lifecycle as lifecycle_module

    monkeypatch.setattr(lifecycle_module, "LIVE_DATA_STALE_AFTER_SECONDS", 0.05)
    payload = _payload(timestamp=datetime.now(timezone.utc).isoformat())

    class _Streamer:
        @staticmethod
        def get_all_latest():
            return {"SPX": payload}

    class _Coordinator:
        def __init__(self):
            self.calls = 0

        def process_payload(self, _payload_value):
            self.calls += 1

    coordinator = _Coordinator()

    async def _run_briefly():
        task = asyncio.create_task(
            run_workstation_state_loop(_Streamer(), coordinator, poll_seconds=0.02)
        )
        await asyncio.sleep(0.14)
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task

    asyncio.run(_run_briefly())

    assert coordinator.calls == 2


class _ShadowCaptureHarness:
    def __init__(self, *, streamer=None, record=None):
        workstation_state_store.reset()
        self.streamer = streamer or _ActiveStreamer()
        self.now = 100.0
        self.saves = []
        self.shadow_rows = []

        def save(prediction, **_kwargs):
            self.saves.append(prediction)
            return SimpleNamespace(id=len(self.saves))

        def capture(payload):
            self.shadow_rows.append(dict(payload))
            return SimpleNamespace(error=None, saved_count=2)

        self.coordinator = WorkstationPredictionCoordinator(
            streamer=self.streamer,
            shadow_research=SimpleNamespace(record=record or capture),
            capture_seconds=60.0,
            prediction_builder=lambda symbol, _payload_value, **_kwargs: _prediction(symbol),
            snapshot_saver=save,
            context_loader=lambda _symbol: {},
            adjustment_builder=lambda *_args: (0.0, []),
            monotonic_clock=lambda: self.now,
        )

    def process(self, **overrides):
        return self.coordinator.process_payload(_payload(**overrides))


def test_shadow_capture_follows_committed_inputs_off_prediction_timer_phase():
    harness = _ShadowCaptureHarness()
    harness.process()  # Prediction timer fires before any input-capture revision.
    harness.now = 159.0
    harness.process(calculation_id="committed-1")  # Not due for prediction save.
    harness.now = 160.0
    harness.process()  # Prediction save is due, but this revision has no input ID.
    harness.now = 219.0
    harness.process(calculation_id="committed-2")
    harness.process(calculation_id="committed-2")  # Same committed identity.

    assert len(harness.saves) == 2
    assert [row["calculation_id"] for row in harness.shadow_rows] == [
        "committed-1", "committed-2"
    ]
    assert all("calculation_id" not in row["pin_payload"] for row in harness.saves)


@pytest.mark.parametrize("calculation_id", [None, "", "  ", 17, True, {}])
def test_shadow_capture_does_not_invent_or_coerce_missing_calculation_identity(calculation_id):
    harness = _ShadowCaptureHarness()
    harness.process(calculation_id=calculation_id)

    assert len(harness.saves) == 1
    assert harness.shadow_rows == []
    assert harness.coordinator.last_shadow_capture == {}


def test_shadow_capture_identity_includes_epoch_generation_and_symbol():
    harness = _ShadowCaptureHarness()
    harness.process(calculation_id="committed")
    harness.process(symbol="NDX", calculation_id="committed")
    harness.streamer.active_generation = 4
    harness.process(calculation_id="committed", subscription_generation=4)
    harness.streamer.subscription_epoch_id = "f" * 64
    harness.process(
        calculation_id="committed", subscription_generation=4,
        subscription_epoch_id="f" * 64,
    )
    harness.process(
        calculation_id="committed", subscription_generation=4,
        subscription_epoch_id="f" * 64,
    )

    assert len(harness.shadow_rows) == 4
    assert len(harness.coordinator.last_shadow_capture) == 2


@pytest.mark.parametrize(
    "overrides",
    [
        {"validation_is_valid": False},
        {"gamma_excluded_from_model": True},
        {"is_fallback": True},
        {"subscription_generation": 2},
        {"subscription_epoch_id": "f" * 64},
        {"timestamp": "2000-01-01T00:00:00+00:00"},
    ],
)
def test_shadow_capture_does_not_record_blocked_or_stale_payloads(overrides):
    harness = _ShadowCaptureHarness()
    harness.process(calculation_id="committed", **overrides)

    assert harness.shadow_rows == []
    assert harness.saves == []


@pytest.mark.parametrize(
    "attribute,value",
    [
        ("active_generation", None),
        ("active_generation", True),
        ("active_generation", 3.5),
        ("active_generation", float("nan")),
        ("active_generation", float("inf")),
        ("active_generation", "3.0"),
        ("subscription_epoch_id", "malformed"),
        ("handoff_status", "warming"),
    ],
)
def test_shadow_capture_rejects_malformed_or_unready_runtime(attribute, value):
    harness = _ShadowCaptureHarness()
    setattr(harness.streamer, attribute, value)
    harness.process(calculation_id="committed")

    assert harness.shadow_rows == []
    assert harness.saves == []


def test_shadow_capture_rechecks_generation_after_prediction_work():
    harness = _ShadowCaptureHarness()

    def predict(symbol, _payload_value, **_kwargs):
        harness.streamer.active_generation = 4
        return _prediction(symbol)

    harness.coordinator.prediction_builder = predict
    harness.process(calculation_id="committed")

    assert harness.shadow_rows == []
    assert harness.saves == []


def test_shadow_capture_requires_publication_lease():
    class RejectedStreamer(_ActiveStreamer):
        @contextmanager
        def prediction_publication_guard(self, **_kwargs):
            yield False

    harness = _ShadowCaptureHarness(streamer=RejectedStreamer())
    harness.process(calculation_id="committed")

    assert harness.shadow_rows == []
    assert harness.saves == []


@pytest.mark.parametrize("failure", ["error", "exception", "skipped", "no_result"])
def test_shadow_capture_failed_attempt_remains_retryable(failure):
    attempts = []

    def capture(payload):
        attempts.append(dict(payload))
        if len(attempts) == 1:
            if failure == "exception":
                raise RuntimeError("temporary journal contention")
            if failure == "no_result":
                return None
            return SimpleNamespace(
                error="temporary journal contention" if failure == "error" else None,
                skipped_reason="SHADOW_RESEARCH_DISABLED" if failure == "skipped" else None,
            )
        return SimpleNamespace(error=None, saved_count=2)

    harness = _ShadowCaptureHarness(record=capture)
    payload = _payload(calculation_id="committed-retry")
    harness.coordinator.process_payload(payload)
    assert harness.coordinator.last_shadow_capture == {}
    harness.coordinator.process_payload(payload)
    harness.coordinator.process_payload(payload)

    assert len(attempts) == 2
    assert len(harness.saves) == 1
    assert len(harness.coordinator.last_shadow_capture) == 1


def test_shadow_capture_holds_generation_barrier_during_recording():
    transition_attempted = threading.Event()
    transition_completed = threading.Event()
    transition_threads = []

    class LockedStreamer(_ActiveStreamer):
        def __init__(self):
            self.lock = threading.RLock()

        @contextmanager
        def prediction_publication_guard(self, **_kwargs):
            with self.lock:
                yield self.handoff_status == "active"

        def transition(self):
            transition_attempted.set()
            with self.lock:
                self.active_generation = 4
                self.handoff_status = "warming"
            transition_completed.set()

    streamer = LockedStreamer()

    def capture(_payload_value):
        transition = threading.Thread(target=streamer.transition)
        transition_threads.append(transition)
        transition.start()
        assert transition_attempted.wait(timeout=1.0)
        assert not transition_completed.is_set()
        assert streamer.active_generation == 3
        return SimpleNamespace(error=None, saved_count=2)

    harness = _ShadowCaptureHarness(streamer=streamer, record=capture)
    harness.process(calculation_id="committed")
    for thread in transition_threads:
        thread.join(timeout=1.0)
    assert transition_completed.is_set()
    assert len(harness.coordinator.last_shadow_capture) == 1



def test_shadow_journal_delay_cannot_publish_a_stale_revision_as_live(monkeypatch):
    from backend.api import lifecycle as lifecycle_module

    age = [0.0]
    monkeypatch.setattr(lifecycle_module, "payload_age_seconds", lambda _data: age[0])
    monkeypatch.setattr("backend.workstation.payload_age_seconds", lambda _data, **_kwargs: age[0])

    def capture(_payload_value):
        age[0] = lifecycle_module.LIVE_DATA_STALE_AFTER_SECONDS + 1.0
        return SimpleNamespace(error=None, saved_count=2)

    harness = _ShadowCaptureHarness(record=capture)
    state = harness.process(calculation_id="committed")

    assert state["status"] == "stale"
    assert state["prediction"]["predicted_close"] is None
    assert state["health"]["usable_for_prediction"] is False
