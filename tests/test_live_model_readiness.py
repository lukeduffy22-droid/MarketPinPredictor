"""Tests for fail-closed live model readiness and anomaly recording."""

import asyncio
import json
import time
from datetime import date
from types import SimpleNamespace

from app.api import state as api_state
from app.ingest import options_websocket_stream
from app.state.oi_cache import OICache
from app.state.ring_buffers import FLOW_RINGS


class _FakeFlowRing:
    def __init__(self, timestamp: float) -> None:
        self.timestamp = timestamp

    def latest(self):
        return (self.timestamp, object())


def test_readiness_requires_live_gamma_flow_and_coefficients(monkeypatch):
    """A complete live signal set should be marked ready."""
    now_ts = time.time()
    monkeypatch.setitem(
        api_state.coefficients_cache,
        "SPX",
        {"beta_gamma": 0.3, "beta_flow": 0.5},
    )
    monkeypatch.setitem(
        api_state.model_metadata_cache,
        "SPX",
        {
            "coefficients_source": "database",
            "coefficients_sample_size": 100,
        },
    )
    monkeypatch.setattr(
        "app.state.oi_cache.oi_cache.get_status",
        lambda symbol, max_age_minutes: {
            "fresh": True,
            "simulated": False,
            "strike_count": 12,
            "last_refresh": "2026-08-25T09:30:00-04:00",
        },
    )
    monkeypatch.setattr(
        options_websocket_stream,
        "get_options_data_provider",
        lambda: "polygon",
    )
    monkeypatch.setattr(
        options_websocket_stream,
        "is_options_websocket_active",
        lambda: True,
    )
    monkeypatch.setitem(
        FLOW_RINGS,
        "SPX",
        _FakeFlowRing(now_ts - 5),
    )

    readiness = api_state.get_live_model_readiness("SPX", now_ts=now_ts)

    assert readiness["ready"] is True
    assert readiness["issues"] == []


def test_readiness_rejects_vwap_only_fallback(monkeypatch):
    """Fallback coefficients must never qualify as a full model."""
    monkeypatch.setitem(
        api_state.coefficients_cache,
        "SPX",
        {"beta_gamma": 0.0, "beta_flow": 0.0},
    )
    monkeypatch.setitem(
        api_state.model_metadata_cache,
        "SPX",
        {
            "coefficients_source": "fallback-defaults",
            "coefficients_sample_size": 0,
        },
    )

    readiness = api_state.get_live_model_readiness("SPX")

    assert readiness["ready"] is False
    assert "calibrated_coefficients_unavailable" in readiness["issues"]
    assert "gamma_coefficient_inactive" in readiness["issues"]
    assert "flow_coefficient_inactive" in readiness["issues"]


def test_anomaly_log_is_append_only_and_deduplicated(monkeypatch, tmp_path):
    """Repeated identical failures should not flood the append-only log."""
    monkeypatch.setattr(
        api_state.settings,
        "live_anomaly_log_dir",
        str(tmp_path),
    )
    api_state._last_anomaly_state.clear()
    readiness = {"ready": False, "issues": ["live_oi_unavailable"]}

    api_state.record_runtime_anomaly("SPX", readiness)
    api_state.record_runtime_anomaly("SPX", readiness)

    log_files = list(tmp_path.glob("*.ndjson"))
    assert len(log_files) == 1
    records = [
        json.loads(line)
        for line in log_files[0].read_text(encoding="utf-8").splitlines()
    ]
    assert len(records) == 1
    assert records[0]["event_type"] == "live_model_anomaly"


def test_live_oi_loader_uses_same_day_expiry():
    """The OI cache should aggregate live call and put data by strike."""
    def contract(expiry: str, kind: str, oi: int, iv: float):
        return SimpleNamespace(
            details=SimpleNamespace(
                expiration_date=expiry,
                strike_price=5500,
                contract_type=kind,
            ),
            open_interest=oi,
            implied_volatility=iv,
        )

    today = date.today()
    later = date(today.year + 1, 1, 1)
    client = SimpleNamespace(
        list_snapshot_options_chain=lambda root: [
            contract(today.isoformat(), "call", 100, 0.2),
            contract(today.isoformat(), "put", 80, 0.25),
            contract(later.isoformat(), "call", 999, 0.3),
            contract(later.isoformat(), "put", 999, 0.3),
        ]
    )

    snapshots = OICache._load_live_chain("SPX", client)

    assert snapshots[5500].oi_call == 100
    assert snapshots[5500].oi_put == 80
    assert snapshots[5500].exp == today


def test_live_oi_loader_rejects_future_only_and_dia_proxy():
    """OI must fail closed without same-day, index-scaled contracts."""
    tomorrow = date.fromordinal(date.today().toordinal() + 1)
    contract = SimpleNamespace(
        details=SimpleNamespace(
            expiration_date=tomorrow.isoformat(),
            strike_price=5500,
            contract_type="call",
        ),
        open_interest=100,
        implied_volatility=0.2,
    )
    client = SimpleNamespace(list_snapshot_options_chain=lambda root: [contract])

    assert OICache._load_live_chain("SPX", client) == {}
    assert OICache._load_live_chain("DJI", client) == {}


def test_options_trade_populates_flow_ring(monkeypatch):
    """Dedicated options ingestion must feed the model's flow ring."""
    stream = options_websocket_stream.OptionsWebSocketStream()
    ring = type(FLOW_RINGS["SPX"])()
    monkeypatch.setitem(FLOW_RINGS, "SPX", ring)
    timestamp_ns = 1_800_000_000_000_000_000

    asyncio.run(
        stream._process_options_trade(
            {
                "sym": "O:SPXW260825C05500000",
                "p": 10.0,
                "s": 2,
                "t": timestamp_ns,
            }
        )
    )

    timestamp, trade = ring.latest()
    assert timestamp == 1_800_000_000
    assert trade.root == "SPX"
    assert trade.K == 5500
    assert trade.notional == 2000.0
    assert trade.aggressor == 0


def test_weekly_option_roots_populate_index_flow_rings(monkeypatch):
    """NDXP and RUTW trades must map to their index flow rings."""
    stream = options_websocket_stream.OptionsWebSocketStream()
    ndx_ring = type(FLOW_RINGS["NDX"])()
    rut_ring = type(FLOW_RINGS["RUT"])()
    monkeypatch.setitem(FLOW_RINGS, "NDX", ndx_ring)
    monkeypatch.setitem(FLOW_RINGS, "RUT", rut_ring)

    asyncio.run(stream._process_options_trade({
        "sym": "O:NDXP260902C24000000", "p": 10.0, "s": 1, "t": 1_800_000_000_000_000_000,
    }))
    asyncio.run(stream._process_options_trade({
        "sym": "O:RUTW260902P02200000", "p": 5.0, "s": 1, "t": 1_800_000_000_000_000_000,
    }))

    assert ndx_ring.latest()[1].root == "NDX"
    assert rut_ring.latest()[1].root == "RUT"


def test_options_subscription_state_includes_trade_diagnostics(monkeypatch):
    """Running-stream diagnostics must include processed trade counters."""
    stream = options_websocket_stream.OptionsWebSocketStream()
    monkeypatch.setattr(options_websocket_stream, "_options_stream", stream)
    monkeypatch.setattr(options_websocket_stream._gamma_tracker, "trade_counts", {"SPX": 4})
    monkeypatch.setattr(options_websocket_stream._gamma_tracker, "last_update_ts", {"SPX": 123.0})

    state = options_websocket_stream.get_options_subscription_state()

    assert state["trade_counts"] == {"SPX": 4}
    assert state["last_update_ts"] == {"SPX": 123.0}


def test_anomaly_write_failure_is_best_effort(monkeypatch, tmp_path):
    """A log filesystem failure must not escape the readiness path."""
    monkeypatch.setattr(api_state.settings, "live_anomaly_log_dir", str(tmp_path))
    monkeypatch.setattr(api_state.os, "makedirs", lambda *_args, **_kwargs: (_ for _ in ()).throw(OSError("full")))
    api_state._last_anomaly_state.clear()

    api_state.record_runtime_anomaly("SPX", {"ready": False, "issues": ["live_oi_unavailable"]})


def test_options_subscription_state_handles_unstarted_stream(monkeypatch):
    """Diagnostics should remain available before the options stream starts."""
    monkeypatch.setattr(options_websocket_stream, "_options_stream", None)

    state = options_websocket_stream.get_options_subscription_state()

    assert state["running"] is False
    assert state["confirmed_count"] == 0
