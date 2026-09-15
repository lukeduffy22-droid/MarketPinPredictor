from __future__ import annotations

from datetime import datetime, timedelta, timezone
import importlib.util
from pathlib import Path
import sys

import pytest
import requests

from app.services.sidebar_live_state import (
    SidebarStateBatch,
    SidebarSymbolState,
    fetch_sidebar_symbol_states,
    retained_prediction_matches_state,
    summarize_cache_reload,
)
from app.services.live_data_client import build_databento_predictions_from_state_batch


class _Response:
    def __init__(self, status_code: int, payload=None, text: str = ""):
        self.status_code = status_code
        self._payload = payload
        self.text = text

    def json(self):
        if isinstance(self._payload, Exception):
            raise self._payload
        return self._payload


NOW = datetime(2026, 9, 3, 18, 0, tzinfo=timezone.utc)
CURRENT_EPOCH = "e" * 64
OLD_EPOCH = "f" * 64


def test_sidebar_state_dataclasses_survive_streamlit_module_eviction():
    """Model the watcher race that removes a module while it is executing."""

    module_path = (
        Path(__file__).resolve().parents[1]
        / "app"
        / "services"
        / "sidebar_live_state.py"
    )
    module_name = "_sidebar_live_state_hot_reload_probe"
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)

    sys.modules.pop(module_name, None)
    spec.loader.exec_module(module)

    assert module_name not in sys.modules
    assert module.SidebarStateBatch.__module__ == module_name
    assert module.SidebarSymbolState.__module__ == module_name


def _state(symbol: str = "SPX", **overrides):
    payload = {
        "contract_version": "workstation-state.v1",
        "symbol": symbol,
        "status": "ready",
        "generated_at_utc": (NOW - timedelta(seconds=1)).isoformat(),
        "source_as_of_utc": (NOW - timedelta(seconds=5)).isoformat(),
        "provider": "databento",
        "subscription_epoch_id": CURRENT_EPOCH,
        "subscription_generation": 7,
        "state_revision": 12,
        "event_id": "12",
        "forecast_id": "forecast-12",
        "prediction_snapshot_id": 44,
        "current_price": 6500.0,
        "market_data_source_label": "Databento Live",
        "prediction_authority": {
            "is_estimate": True,
            "tcbbo_promoted": False,
            "source_label": "Research GEX estimate",
        },
        "prediction": {
            "usable": True,
            "predicted_close": 6510.0,
            "is_estimate": True,
        },
        "pin_payload": {
            "gamma_pin": 6505.0,
            "max_pain": 6490.0,
            "positive_gex_wall": 6520.0,
            "negative_gex_wall": 6475.0,
        },
        "health": {
            "validation_is_valid": True,
            "validation_failure_reasons": [],
            "is_stale": False,
            "data_age_seconds": 5.0,
            "stale_after_seconds": 30.0,
            "usable_for_prediction": True,
        },
    }
    payload.update(overrides)
    return payload


def _health(
    *symbols: str,
    active_generation: int = 7,
    active_epoch: str = CURRENT_EPOCH,
    epoch_is_current: bool = True,
    handoff_status: str = "active",
):
    return {
        "subscription_epoch_id": active_epoch,
        "handoff_status": handoff_status,
        "symbol_status": {
            symbol: {
                "active_subscription_epoch_id": active_epoch,
                "epoch_is_current": epoch_is_current,
                "active_generation": active_generation,
                "generation_is_current": True,
            }
            for symbol in symbols
        }
    }


def _fetch(states, health, *, now=NOW):
    def get(url, timeout):
        assert timeout == 2.0
        if url.endswith("/health/live"):
            return _Response(200, health)
        symbol = url.rsplit("/", 1)[-1]
        value = states[symbol]
        if isinstance(value, Exception):
            raise value
        if isinstance(value, tuple):
            return _Response(value[0], value[1])
        return _Response(200, value)

    return fetch_sidebar_symbol_states(
        list(states),
        get=get,
        checked_at=now,
        timeout_seconds=2.0,
    )


def test_ready_state_releases_only_lifecycle_verified_numbers_and_timestamps():
    batch = _fetch({"SPX": _state()}, _health("SPX"))

    result = batch.results[0]
    assert result.state == "ready"
    assert result.usable is True
    assert result.current_price == 6500.0
    assert result.predicted_close == 6510.0
    assert result.prediction_usable is True
    assert result.gamma_pin == 6505.0
    assert result.source_as_of_utc == (NOW - timedelta(seconds=5)).isoformat()
    assert result.checked_at_utc == "2026-09-03T18:00:00Z"
    assert result.subscription_epoch_id == CURRENT_EPOCH
    assert result.active_subscription_epoch_id == CURRENT_EPOCH
    assert result.epoch_is_current is True
    assert result.active_handoff_status == "active"
    assert result.subscription_generation == result.active_generation == 7
    assert result.state_revision == 12
    assert result.forecast_id == "forecast-12"


def test_market_can_remain_usable_while_unusable_prediction_is_redacted():
    payload = _state(
        prediction={"usable": False, "predicted_close": 6510.0},
        health={
            "validation_is_valid": True,
            "validation_failure_reasons": [],
            "is_stale": False,
            "data_age_seconds": 5.0,
            "stale_after_seconds": 30.0,
            "usable_for_prediction": False,
        },
    )

    result = _fetch({"SPX": payload}, _health("SPX")).results[0]

    assert result.usable is True
    assert result.current_price == 6500.0
    assert result.prediction_usable is False
    assert result.predicted_close is None


def test_retained_prediction_requires_generation_timestamp_revision_and_forecast_identity():
    state = _fetch({"SPX": _state()}, _health("SPX")).results[0]
    retained = {
        "ticker": "SPX",
        "predicted_price": 6510.0,
        "subscription_epoch_id": CURRENT_EPOCH,
        "subscription_generation": 7,
        "state_revision": 12,
        "forecast_id": "forecast-12",
        "source_as_of_utc": (NOW - timedelta(seconds=5)).isoformat(),
        "tcbbo_promoted": False,
    }

    assert retained_prediction_matches_state(retained, state)[0] is True

    stale_revision = {**retained, "state_revision": 11}
    matches, reason = retained_prediction_matches_state(stale_revision, state)
    assert matches is False
    assert "revision" in reason

    stale_generation = {**retained, "subscription_generation": 6}
    matches, reason = retained_prediction_matches_state(stale_generation, state)
    assert matches is False
    assert "generation" in reason

    stale_epoch = {**retained, "subscription_epoch_id": OLD_EPOCH}
    matches, reason = retained_prediction_matches_state(stale_epoch, state)
    assert matches is False
    assert "epoch" in reason

    stale_timestamp = {
        **retained,
        "source_as_of_utc": (NOW - timedelta(seconds=6)).isoformat(),
    }
    matches, reason = retained_prediction_matches_state(stale_timestamp, state)
    assert matches is False
    assert "timestamp" in reason


def _forecast_state(revision=12, predicted_close=6510.0, *, source_age=5):
    source = (NOW - timedelta(seconds=source_age)).isoformat()
    forecast_id = f"forecast-{revision}"
    return _state(
        state_revision=revision,
        event_id=str(revision),
        forecast_id=forecast_id,
        source_as_of_utc=source,
        prediction={
            "usable": True,
            "predicted_close": predicted_close,
            "forecast_id": forecast_id,
        },
        pin_payload={
            "symbol": "SPX",
            "price": 6500.0,
            "timestamp": source,
            "subscription_epoch_id": CURRENT_EPOCH,
            "subscription_generation": 7,
            "validation_is_valid": True,
            "gamma_excluded_from_model": False,
            "gamma_pin": 6505.0,
            "top_strikes": [],
        },
    )


def test_single_batch_forecast_rebuild_moves_values_and_identity_together(monkeypatch):
    monkeypatch.setattr("app.services.live_data_client._payload_age_seconds", lambda _payload: 5.0)

    def no_refetch(*_args, **_kwargs):
        raise AssertionError("Candidate conversion must not request another state revision")

    monkeypatch.setattr("app.services.live_data_client.requests.get", no_refetch)
    old_batch = _fetch({"SPX": _forecast_state()}, _health("SPX"))
    old_predictions, _ = build_databento_predictions_from_state_batch(
        old_batch, timeframe="1-day", stale_after_seconds=30.0,
    )
    new_batch = _fetch(
        {"SPX": _forecast_state(13, 6520.0, source_age=3)}, _health("SPX"),
    )
    assert retained_prediction_matches_state(old_predictions["SPX"], new_batch.results[0])[0] is False

    predictions, messages = build_databento_predictions_from_state_batch(
        new_batch, timeframe="1-day", stale_after_seconds=30.0,
    )

    assert messages == ()
    assert predictions["SPX"]["predicted_price"] == 6520.0
    assert predictions["SPX"]["state_revision"] == 13
    assert predictions["SPX"]["forecast_id"] == "forecast-13"
    assert predictions["SPX"]["source_as_of_utc"] == (NOW - timedelta(seconds=3)).isoformat()
    assert retained_prediction_matches_state(predictions["SPX"], new_batch.results[0])[0] is True


@pytest.mark.parametrize("failure", ["invalid", "stale", "epoch_changed", "generation_changed"])
def test_single_batch_forecast_rebuild_withholds_new_invalid_or_obsolete_source(monkeypatch, failure):
    monkeypatch.setattr("app.services.live_data_client._payload_age_seconds", lambda _payload: 5.0)
    payload = _forecast_state(13, 6520.0)
    health = _health("SPX")
    if failure == "invalid":
        payload["health"]["validation_is_valid"] = False
    elif failure == "stale":
        payload["source_as_of_utc"] = (NOW - timedelta(seconds=31)).isoformat()
        payload["pin_payload"]["timestamp"] = payload["source_as_of_utc"]
    elif failure == "epoch_changed":
        health = _health("SPX", active_epoch=OLD_EPOCH)
    else:
        health = _health("SPX", active_generation=8)
    batch = _fetch({"SPX": payload}, health)

    predictions, _messages = build_databento_predictions_from_state_batch(
        batch, timeframe="1-day", stale_after_seconds=30.0,
    )

    assert batch.results[0].usable is False
    assert predictions == {}


def test_invalid_state_preserves_exact_reasons_and_redacts_all_numbers():
    invalid = _state(
        status="invalid",
        pin_payload={
            "price": 6498.25,
            "gamma_pin": 6505.0,
            "max_pain": 6490.0,
            "zero_gamma": 6485.0,
            "gross_gex": 1200.0,
            "net_gex": -200.0,
            "contracts": 42,
            "fresh_quote_count": 88,
            "paired_quote_count": 22,
            "expected_primary_pair_count": 50,
            "paired_primary_pair_count": 4,
            "primary_pair_coverage_ratio": 0.08,
            "primary_expiration": "2026-09-03",
            "max_pain_source": "full-oi-universe",
            "validation_is_valid": False,
        },
        health={
            "validation_is_valid": False,
            "validation_failure_reasons": [
                "PRIMARY_PAIR_COVERAGE_LOW: 0.0449 < 0.1",
                "CHAIN_TOO_THIN",
            ],
            "is_stale": False,
            "data_age_seconds": 5.0,
            "stale_after_seconds": 30.0,
        },
    )

    result = _fetch({"SPX": invalid}, _health("SPX")).results[0]

    assert result.state == "invalid"
    assert result.reason == "PRIMARY_PAIR_COVERAGE_LOW: 0.0449 < 0.1; CHAIN_TOO_THIN"
    assert result.failure_reasons == (
        "PRIMARY_PAIR_COVERAGE_LOW: 0.0449 < 0.1",
        "CHAIN_TOO_THIN",
    )
    assert result.usable is False
    assert result.current_price is None
    assert result.predicted_close is None
    assert result.gamma_pin is None
    assert result.diagnostic_evidence is not None
    assert result.diagnostic_evidence.scope == "partial_live_calculation"
    assert result.diagnostic_evidence.spot == 6498.25
    assert result.diagnostic_evidence.gamma_pin == 6505.0
    assert result.diagnostic_evidence.gross_gex == 1200.0
    assert result.diagnostic_evidence.primary_pair_coverage_ratio == 0.08


def test_invalid_zero_shell_hides_placeholders_but_preserves_oi_context():
    invalid = _state(
        status="invalid",
        current_price=0.0,
        prediction=None,
        pin_payload={
            "price": 0.0,
            "spot_last": 0.0,
            "gamma_pin": 0.0,
            "primary_gamma_pin_strike": 0.0,
            "max_pain": 6500.0,
            "max_pain_source": "full-oi-universe",
            "gross_gex": 0.0,
            "net_gex": 0.0,
            "contracts_count": 0,
            "fresh_quote_count": 700,
            "paired_quote_count": 3,
            "primary_expiration": "2026-09-03",
            "validation_is_valid": False,
        },
        health={
            "validation_is_valid": False,
            "validation_failure_reasons": [
                "PSEUDO_PARITY_TOO_FEW_PAIRED_QUOTES: 3 pairs < 5"
            ],
            "is_stale": False,
            "data_age_seconds": 5.0,
            "stale_after_seconds": 30.0,
        },
    )

    result = _fetch({"SPX": invalid}, _health("SPX")).results[0]

    assert result.usable is False
    assert result.current_price is None
    assert result.gamma_pin is None
    assert result.diagnostic_evidence is not None
    assert result.diagnostic_evidence.scope == "open_interest_context_only"
    assert result.diagnostic_evidence.spot is None
    assert result.diagnostic_evidence.gamma_pin is None
    assert result.diagnostic_evidence.gross_gex is None
    assert result.diagnostic_evidence.net_gex is None
    assert result.diagnostic_evidence.max_pain == 6500.0
    assert result.diagnostic_evidence.paired_quote_count == 3


def test_closed_market_fallback_is_labeled_as_historical_context_not_live_data():
    closed_context = _state(
        status="closed_context",
        provider="historical-fallback",
        current_price=6500.0,
        prediction=None,
        pin_payload={
            "provider": "historical-fallback",
            "price": 6500.0,
            "historical_context_only": True,
            "is_fallback": True,
            "validation_is_valid": False,
        },
        health={
            "validation_is_valid": False,
            "validation_failure_reasons": [
                "CLOSED_MARKET_HISTORICAL_CONTEXT_ONLY"
            ],
            "is_stale": True,
            "data_age_seconds": 3600.0,
            "stale_after_seconds": 30.0,
            "usable_for_prediction": False,
        },
    )

    result = _fetch(
        {"SPX": closed_context},
        _health("SPX", handoff_status="off_hours"),
    ).results[0]

    assert result.state == "closed_context"
    assert result.usable is False
    assert result.current_price is None
    assert result.diagnostic_evidence is not None
    assert result.diagnostic_evidence.scope == "historical_context_only"
    assert result.diagnostic_evidence.spot == 6500.0


def test_dynamic_freshness_check_rejects_a_stale_materialized_state():
    old = _state(source_as_of_utc=(NOW - timedelta(minutes=2)).isoformat())

    result = _fetch({"SPX": old}, _health("SPX")).results[0]

    assert result.state == "stale"
    assert result.usable is False
    assert result.data_age_seconds == 120.0
    assert result.current_price is None
    assert result.reason.startswith("LIVE_DATA_STALE:")
    assert result.diagnostic_evidence is not None
    assert result.diagnostic_evidence.gamma_pin == 6505.0


def test_generation_mismatch_is_distinct_and_redacts_values():
    health = _health("SPX", active_generation=8)
    health["symbol_status"]["SPX"]["generation_is_current"] = False

    result = _fetch({"SPX": _state()}, health).results[0]

    assert result.state == "generation_mismatch"
    assert result.usable is False
    assert result.subscription_generation == 7
    assert result.active_generation == 8
    assert result.current_price is None
    assert "SUBSCRIPTION_GENERATION_MISMATCH" in result.reason
    assert result.diagnostic_evidence is not None
    assert result.diagnostic_evidence.gamma_pin == 6505.0


def test_reused_generation_from_old_process_epoch_is_redacted_immediately():
    health = _health(
        "SPX",
        active_generation=7,
        active_epoch=CURRENT_EPOCH,
        epoch_is_current=False,
    )
    old_process_state = _state(subscription_epoch_id=OLD_EPOCH)

    result = _fetch({"SPX": old_process_state}, health).results[0]

    assert result.state == "epoch_mismatch"
    assert result.usable is False
    assert result.subscription_epoch_id == OLD_EPOCH
    assert result.active_subscription_epoch_id == CURRENT_EPOCH
    assert result.epoch_is_current is False
    assert result.subscription_generation == result.active_generation == 7
    assert result.current_price is None
    assert result.predicted_close is None
    assert result.gamma_pin is None
    assert "SUBSCRIPTION_EPOCH_MISMATCH" in result.reason


def test_ready_state_requires_canonical_epoch_and_explicit_current_flag():
    uppercase_epoch = "A" * 64
    result = _fetch(
        {"SPX": _state(subscription_epoch_id=uppercase_epoch)},
        _health("SPX", active_epoch=uppercase_epoch),
    ).results[0]

    assert result.state == "epoch_unverified"
    assert result.usable is False
    assert result.subscription_epoch_id is None
    assert result.active_subscription_epoch_id is None
    assert result.current_price is None

    health = _health("SPX")
    health["symbol_status"]["SPX"].pop("epoch_is_current")
    result = _fetch({"SPX": _state()}, health).results[0]
    assert result.state == "epoch_unverified"
    assert result.usable is False
    assert result.current_price is None


@pytest.mark.parametrize("handoff_status", ["stopped", "off_hours", "unknown"])
def test_unready_handoff_redacts_fresh_same_epoch_and_generation_state(
    handoff_status,
):
    result = _fetch(
        {"SPX": _state()},
        _health("SPX", handoff_status=handoff_status),
    ).results[0]

    assert result.state == "handoff_unready"
    assert result.usable is False
    assert result.subscription_epoch_id == CURRENT_EPOCH
    assert result.active_subscription_epoch_id == CURRENT_EPOCH
    assert result.epoch_is_current is True
    assert result.subscription_generation == result.active_generation == 7
    assert result.active_handoff_status == handoff_status
    assert result.current_price is None
    assert result.predicted_close is None
    assert result.gamma_pin is None
    assert "handoff_status" in result.reason


def test_transport_and_http_failures_are_not_mislabeled_backend_offline():
    batch = _fetch(
        {
            "SPX": requests.exceptions.Timeout("two-second deadline"),
            "NDX": requests.exceptions.ConnectionError("connection refused"),
            "VIX": (503, {"detail": "event loop busy"}),
        },
        _health("SPX", "NDX", "VIX"),
    )

    assert [result.state for result in batch.results] == [
        "timeout",
        "connection_error",
        "http_error",
    ]
    assert "two-second deadline" in batch.results[0].reason
    assert "connection refused" in batch.results[1].reason
    assert batch.results[2].reason == "HTTP 503: event loop busy"
    assert batch.results[2].optional is True


def test_health_failure_withholds_otherwise_ready_numbers_when_generation_is_unknown():
    def get(url, timeout):
        if url.endswith("/health/live"):
            raise requests.exceptions.Timeout("health deadline")
        return _Response(200, _state())

    result = fetch_sidebar_symbol_states(
        ["SPX"],
        get=get,
        checked_at=NOW,
        timeout_seconds=2.0,
    ).results[0]

    assert result.state == "generation_unverified"
    assert result.usable is False
    assert result.current_price is None
    assert "health timeout" in result.reason


def _summary_state(symbol: str, *, usable: bool, optional: bool = False):
    return SidebarSymbolState(
        symbol=symbol,
        state="ready" if usable else "invalid",
        lifecycle_status="ready" if usable else "invalid",
        usable=usable,
        reason="ok" if usable else "CHAIN_TOO_THIN",
        failure_reasons=() if usable else ("CHAIN_TOO_THIN",),
        checked_at_utc="2026-09-03T18:00:00Z",
        current_price=6500.0 if usable else None,
        optional=optional,
    )


def test_reload_summary_never_claims_success_when_all_required_symbols_fail():
    batch = SidebarStateBatch(
        results=(
            _summary_state("SPX", usable=False),
            _summary_state("NDX", usable=False),
        ),
        checked_at_utc="2026-09-03T18:00:00Z",
        health_state="ok",
        health_reason=None,
    )

    summary = summarize_cache_reload(batch)
    assert summary.level == "error"
    assert summary.message.startswith("No required symbol reloaded")


def test_reload_summary_reports_partial_required_results():
    batch = SidebarStateBatch(
        results=(
            _summary_state("SPX", usable=True),
            _summary_state("NDX", usable=False),
        ),
        checked_at_utc="2026-09-03T18:00:00Z",
        health_state="ok",
        health_reason=None,
    )

    summary = summarize_cache_reload(batch)
    assert summary.level == "warning"
    assert summary.message == "Partial reload: 1/2 required symbols are current"


def test_optional_vix_failure_does_not_fail_a_complete_required_reload():
    batch = SidebarStateBatch(
        results=(
            _summary_state("SPX", usable=True),
            _summary_state("NDX", usable=True),
            _summary_state("VIX", usable=False, optional=True),
        ),
        checked_at_utc="2026-09-03T18:00:00Z",
        health_state="ok",
        health_reason=None,
    )

    summary = summarize_cache_reload(batch)
    assert summary.level == "success"
    assert "optional VIX unavailable (invalid)" in summary.message
