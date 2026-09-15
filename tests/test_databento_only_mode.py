import asyncio
import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest
from fastapi import HTTPException

from backend.api.routers import health as health_router
from backend.api.routers import predict as predict_router
from backend.workstation import workstation_state_store

TEST_SUBSCRIPTION_EPOCH = "e" * 64


class _DummyStreamer:
    def __init__(
        self,
        is_running=True,
        latest=None,
        *,
        epoch_id=TEST_SUBSCRIPTION_EPOCH,
        generation=3,
        handoff_status="active",
    ):
        self.is_running = is_running
        self._latest = latest or {}
        self.subscription_epoch_id = epoch_id
        self.active_generation = generation
        self.handoff_status = handoff_status

    def get_all_latest(self):
        return self._latest

    def get_latest_data(self, symbol: str, n: int = 1):
        return []

    def get_subscription_context(self):
        return {
            "subscription_epoch_id": self.subscription_epoch_id,
            "subscription_generation": self.active_generation,
            "handoff_status": self.handoff_status,
        }


def test_settings_fail_fast_without_databento_key():
    repo_root = Path(__file__).resolve().parents[1]
    env = os.environ.copy()
    env["DATABENTO_API_KEY"] = ""
    env["PYTHONPATH"] = str(repo_root)

    cmd = [
        sys.executable,
        "-c",
        "import app.utils.settings",  # noqa: F401
    ]
    result = subprocess.run(cmd, cwd=str(repo_root), env=env, capture_output=True, text=True)

    assert result.returncode != 0
    combined = f"{result.stdout}\n{result.stderr}"
    assert "Databento-only mode" in combined or "DATABENTO_API_KEY" in combined


def test_health_status_reports_databento_provider(monkeypatch):
    dummy_streamer = _DummyStreamer(is_running=True, latest={"SPX": {"price": 7500.0}})

    monkeypatch.setattr(health_router, "get_streamer", lambda: dummy_streamer)
    monkeypatch.setattr(
        health_router,
        "streamer_health",
        lambda _streamer: {
            "provider": "databento",
            "stream_progressing": True,
            "websocket": "active",
            "messages_received": 1,
        },
    )

    payload = asyncio.run(health_router.health_status())

    assert payload["market_data_provider"] == "databento"


def test_health_and_sse_omit_full_contract_lists_by_default(monkeypatch):
    contracts = [f"SPXW  260826C{7000000 + index:08d}" for index in range(2_800)]
    dummy_streamer = _DummyStreamer(is_running=True, latest={})
    raw_health = {
        "provider": "databento",
        "websocket": "active",
        "stream_progressing": True,
        "handoff_status": "active",
        "messages_received": 10,
        "symbols_requested": ["SPX", "NDX", "VIX"],
        "symbols_subscribed": len(contracts),
        "subscribed_symbols": contracts,
        "root_contract_counts": {"SPXW": len(contracts)},
    }
    monkeypatch.setattr(health_router, "get_streamer", lambda: dummy_streamer)
    monkeypatch.setattr(health_router, "streamer_health", lambda _streamer: raw_health)

    compact = asyncio.run(health_router.health_status())
    detailed = asyncio.run(health_router.health_status(include_symbols=True))
    compact_universe = health_router._universe_snapshot(raw_health, include_symbols=False)
    detailed_universe = health_router._universe_snapshot(raw_health, include_symbols=True)

    assert compact["symbols_subscribed"] == len(contracts)
    assert compact["subscribed_symbols_included"] is False
    assert compact["subscribed_symbols_diagnostics_url"] == "/databento/universe"
    assert "subscribed_symbols" not in compact
    assert detailed["subscribed_symbols"] == contracts
    assert detailed["subscribed_symbols_included"] is True
    assert "subscribed_symbols" not in compact_universe
    assert detailed_universe["subscribed_symbols"] == contracts
    # Startup provenance is shared fixed metadata whose source set can grow;
    # compare the streaming contract payload rather than that independent audit.
    compact_without_fingerprint = {key: value for key, value in compact.items() if key != "loaded_code_fingerprint"}
    detailed_without_fingerprint = {key: value for key, value in detailed.items() if key != "loaded_code_fingerprint"}
    assert len(json.dumps(detailed_without_fingerprint)) > len(json.dumps(compact_without_fingerprint)) * 20


def _live_payload(symbol: str, *, valid: bool, generation: int = 3) -> dict:
    return {
        "symbol": symbol,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "price": 7500.0,
        "likely_close": 7510.0,
        "validation_is_valid": valid,
        "gamma_excluded_from_model": not valid,
        "validation_failure_reasons": [] if valid else ["NO_CURRENT_QUOTES"],
        "subscription_epoch_id": TEST_SUBSCRIPTION_EPOCH,
        "subscription_generation": generation,
    }


def test_health_live_rejects_fresh_invalid_payload(monkeypatch):
    dummy_streamer = _DummyStreamer(
        is_running=True,
        latest={"SPX": _live_payload("SPX", valid=False)},
    )
    monkeypatch.setattr(health_router, "get_streamer", lambda: dummy_streamer)
    monkeypatch.setattr(
        health_router,
        "streamer_health",
        lambda _streamer: {
            "provider": "databento",
            "symbols_requested": ["SPX"],
            "websocket": "active",
            "stream_progressing": True,
            "handoff_status": "active",
            "subscription_epoch_id": TEST_SUBSCRIPTION_EPOCH,
            "active_generation": 3,
            "fresh_quote_counts": {"SPX": 10},
        },
    )

    payload = asyncio.run(health_router.health_live_status())

    assert payload["status"] == "degraded"
    assert payload["calculation_ready"] is False
    assert payload["prediction_pipeline_ok"] is False
    assert payload["invalid_symbols"] == ["SPX"]
    assert payload["symbol_status"]["SPX"]["status"] == "invalid"


def test_health_live_fails_closed_without_canonical_active_process_epoch(
    monkeypatch,
):
    dummy_streamer = _DummyStreamer(
        is_running=True,
        latest={"SPX": _live_payload("SPX", valid=True)},
    )
    monkeypatch.setattr(health_router, "get_streamer", lambda: dummy_streamer)
    monkeypatch.setattr(
        health_router,
        "streamer_health",
        lambda _streamer: {
            "provider": "databento",
            "symbols_requested": ["SPX"],
            "websocket": "active",
            "stream_progressing": True,
            "handoff_status": "active",
            "active_generation": 3,
            "fresh_quote_counts": {"SPX": 10},
        },
    )

    payload = asyncio.run(health_router.health_live_status())

    assert payload["subscription_epoch_valid"] is False
    assert payload["calculation_ready"] is False
    assert payload["prediction_pipeline_ok"] is False
    assert payload["required_epoch_mismatch_symbols"] == ["SPX"]


@pytest.mark.parametrize("active_generation", [None, 0, -1, True, "invalid"])
def test_health_live_fails_closed_without_positive_active_generation(
    monkeypatch,
    active_generation,
):
    dummy_streamer = _DummyStreamer(
        is_running=True,
        latest={"SPX": _live_payload("SPX", valid=True)},
    )
    monkeypatch.setattr(health_router, "get_streamer", lambda: dummy_streamer)
    monkeypatch.setattr(
        health_router,
        "streamer_health",
        lambda _streamer: {
            "provider": "databento",
            "symbols_requested": ["SPX"],
            "websocket": "active",
            "stream_progressing": True,
            "handoff_status": "active",
            "subscription_epoch_id": TEST_SUBSCRIPTION_EPOCH,
            "active_generation": active_generation,
            "fresh_quote_counts": {"SPX": 10},
        },
    )

    payload = asyncio.run(health_router.health_live_status())

    assert payload["subscription_generation_valid"] is False
    assert payload["calculation_ready"] is False
    assert payload["prediction_pipeline_ok"] is False
    assert payload["required_generation_mismatch_symbols"] == ["SPX"]
    assert payload["symbol_status"]["SPX"]["generation_is_current"] is False


@pytest.mark.parametrize("handoff_status", [None, "unknown", "warming", "stopped"])
def test_health_live_requires_exact_active_handoff(monkeypatch, handoff_status):
    dummy_streamer = _DummyStreamer(
        is_running=True,
        latest={"SPX": _live_payload("SPX", valid=True)},
    )
    monkeypatch.setattr(health_router, "get_streamer", lambda: dummy_streamer)
    monkeypatch.setattr(
        health_router,
        "streamer_health",
        lambda _streamer: {
            "provider": "databento",
            "symbols_requested": ["SPX"],
            "websocket": "active",
            "stream_progressing": True,
            "handoff_status": handoff_status,
            "subscription_epoch_id": TEST_SUBSCRIPTION_EPOCH,
            "active_generation": 3,
            "fresh_quote_counts": {"SPX": 10},
        },
    )

    payload = asyncio.run(health_router.health_live_status())

    assert payload["handoff_status"] == (handoff_status or "unknown")
    assert payload["collection_ready"] is False
    assert payload["prediction_pipeline_ok"] is False


def test_health_live_requires_every_required_symbol_and_reports_optional_gaps(monkeypatch):
    dummy_streamer = _DummyStreamer(
        is_running=True,
        latest={
            "SPX": _live_payload("SPX", valid=True, generation=3),
            "NDX": _live_payload("NDX", valid=True, generation=2),
        },
    )
    monkeypatch.setattr(health_router, "get_streamer", lambda: dummy_streamer)
    monkeypatch.setattr(
        health_router,
        "streamer_health",
        lambda _streamer: {
            "provider": "databento",
            "symbols_requested": ["SPX", "NDX", "VIX"],
            "websocket": "active",
            "stream_progressing": True,
            "handoff_status": "active",
            "subscription_epoch_id": TEST_SUBSCRIPTION_EPOCH,
            "active_generation": 3,
            "fresh_quote_counts": {"SPX": 10, "NDX": 10, "VIX": 0},
        },
    )

    payload = asyncio.run(health_router.health_live_status())

    assert payload["status"] == "degraded"
    assert payload["missing_symbols"] == ["VIX"]
    assert payload["generation_mismatch_symbols"] == ["NDX"]
    assert payload["zero_fresh_quote_symbols"] == ["VIX"]
    assert payload["required_symbols"] == ["NDX", "SPX"]
    assert payload["optional_symbols"] == ["VIX"]
    assert payload["required_generation_mismatch_symbols"] == ["NDX"]
    assert payload["required_zero_fresh_quote_symbols"] == []
    assert payload["optional_degraded_symbols"] == ["VIX"]
    assert payload["prediction_pipeline_ok"] is False


def test_health_live_is_healthy_only_when_all_layers_are_ready(monkeypatch):
    dummy_streamer = _DummyStreamer(
        is_running=True,
        latest={
            "SPX": _live_payload("SPX", valid=True),
            "NDX": _live_payload("NDX", valid=True),
        },
    )
    monkeypatch.setattr(health_router, "get_streamer", lambda: dummy_streamer)
    monkeypatch.setattr(
        health_router,
        "streamer_health",
        lambda _streamer: {
            "provider": "databento",
            "symbols_requested": ["SPX", "NDX"],
            "websocket": "active",
            "stream_progressing": True,
            "handoff_status": "active",
            "subscription_epoch_id": TEST_SUBSCRIPTION_EPOCH,
            "active_generation": 3,
            "fresh_quote_counts": {"SPX": 10, "NDX": 10},
        },
    )

    payload = asyncio.run(health_router.health_live_status())

    assert payload["status"] == "healthy"
    assert payload["transport_ready"] is True
    assert payload["collection_ready"] is True
    assert payload["calculation_ready"] is True
    assert payload["prediction_pipeline_ok"] is True


def test_health_live_fails_closed_when_runtime_context_changes_during_read(
    monkeypatch,
):
    class _ChangingRuntime(_DummyStreamer):
        def __init__(self):
            super().__init__(
                latest={"SPX": _live_payload("SPX", valid=True)}
            )
            self.context_reads = 0

        def get_subscription_context(self):
            self.context_reads += 1
            generation = 3 if self.context_reads == 1 else 4
            return {
                "subscription_epoch_id": TEST_SUBSCRIPTION_EPOCH,
                "subscription_generation": generation,
                "handoff_status": "active",
            }

    runtime = _ChangingRuntime()
    monkeypatch.setattr(health_router, "get_streamer", lambda: runtime)
    monkeypatch.setattr(
        health_router,
        "streamer_health",
        lambda _streamer: {
            "provider": "databento",
            "symbols_requested": ["SPX"],
            "websocket": "active",
            "stream_progressing": True,
            "handoff_status": "active",
            "subscription_epoch_id": TEST_SUBSCRIPTION_EPOCH,
            "active_generation": 3,
            "fresh_quote_counts": {"SPX": 10},
        },
    )

    payload = asyncio.run(health_router.health_live_status())

    assert payload["runtime_context_stable"] is False
    assert payload["collection_ready"] is False
    assert payload["calculation_ready"] is False
    assert payload["prediction_pipeline_ok"] is False


def test_health_live_rejects_reused_generation_from_an_old_process(monkeypatch):
    old_payload = _live_payload("SPX", valid=True, generation=3)
    old_payload["subscription_epoch_id"] = "f" * 64
    dummy_streamer = _DummyStreamer(is_running=True, latest={"SPX": old_payload})
    monkeypatch.setattr(health_router, "get_streamer", lambda: dummy_streamer)
    monkeypatch.setattr(
        health_router,
        "streamer_health",
        lambda _streamer: {
            "provider": "databento",
            "symbols_requested": ["SPX"],
            "websocket": "active",
            "stream_progressing": True,
            "handoff_status": "active",
            "subscription_epoch_id": TEST_SUBSCRIPTION_EPOCH,
            "active_generation": 3,
            "fresh_quote_counts": {"SPX": 10},
        },
    )

    payload = asyncio.run(health_router.health_live_status())

    assert payload["status"] == "degraded"
    assert payload["calculation_ready"] is False
    assert payload["prediction_pipeline_ok"] is False
    assert payload["epoch_mismatch_symbols"] == ["SPX"]
    assert payload["required_epoch_mismatch_symbols"] == ["SPX"]
    assert payload["symbol_status"]["SPX"]["epoch_is_current"] is False


def test_optional_vix_failure_does_not_veto_spx_ndx_readiness(monkeypatch):
    dummy_streamer = _DummyStreamer(
        is_running=True,
        latest={
            "SPX": _live_payload("SPX", valid=True),
            "NDX": _live_payload("NDX", valid=True),
            "VIX": _live_payload("VIX", valid=False),
        },
    )
    monkeypatch.setattr(health_router, "get_streamer", lambda: dummy_streamer)
    monkeypatch.setattr(
        health_router,
        "streamer_health",
        lambda _streamer: {
            "provider": "databento",
            "symbols_requested": ["SPX", "NDX", "VIX"],
            "websocket": "active",
            "stream_progressing": True,
            "handoff_status": "active",
            "subscription_epoch_id": TEST_SUBSCRIPTION_EPOCH,
            "active_generation": 3,
            "fresh_quote_counts": {"SPX": 10, "NDX": 10, "VIX": 10},
        },
    )

    payload = asyncio.run(health_router.health_live_status())

    assert payload["status"] == "healthy"
    assert payload["calculation_ready"] is True
    assert payload["prediction_pipeline_ok"] is True
    assert payload["required_invalid_symbols"] == []
    assert payload["invalid_symbols"] == ["VIX"]
    assert payload["optional_context_ready"] is False
    assert payload["optional_degraded_symbols"] == ["VIX"]
    assert payload["all_configured_prediction_pipeline_ok"] is False


def test_predict_close_returns_404_when_no_market_data(monkeypatch):
    workstation_state_store.reset()
    monkeypatch.setattr(predict_router, "get_streamer", lambda: _DummyStreamer(is_running=True, latest={}))

    with pytest.raises(HTTPException) as exc:
        asyncio.run(predict_router._predict_close_impl("SPX", db=None))

    assert exc.value.status_code == 404
    assert "No market data" in str(exc.value.detail)
