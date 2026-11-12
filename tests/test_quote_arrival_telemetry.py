"""First-quote diagnostics preserve feed/admission separation without gates."""
import asyncio
from copy import deepcopy
import time
from types import SimpleNamespace

import pytest

import backend.databento_streamer as stream_module
from backend.databento_streamer import _QuoteArrivalTelemetry

EPOCH = "a" * 64
NS = 1_791_465_600_000_000_000
SYMBOL = "SPXW  261008C07700000"


@pytest.fixture(autouse=True)
def isolated_environment(monkeypatch, tmp_path):
    monkeypatch.setenv("DATABASE_URL", "sqlite:///" + (tmp_path / "unused.sqlite").as_posix())
    monkeypatch.setattr(stream_module, "DATA_DIR", tmp_path)


def test_first_milestones_are_immutable_and_completed_bursts_do_not_allocate(monkeypatch):
    ticks = iter([NS+10, NS+20, NS+30])
    monkeypatch.setattr(stream_module.time, "time_ns", lambda: next(ticks))
    telemetry = _QuoteArrivalTelemetry(EPOCH, 1, ["SPX", "NDX"])
    telemetry.received("SPX", NS, NS-100, True)
    telemetry.rejected("SPX", "INVALID_QUOTE_PRICE")
    initial = telemetry.snapshot()
    assert initial["families"]["SPX"]["first_admitted_at_utc"] is None
    telemetry.admitted("SPX", False)
    row = telemetry._rows["SPX"]
    assert row.received_at_ns == NS and row.provider_ts_recv_ns == NS-100
    assert row.mapped_at_ns == NS+10 and row.admitted_at_ns == NS+30
    assert row.first_rejection_at_ns == NS+20
    assert row.received_replay_pending is True and row.admitted_replay_pending is False
    for _ in range(1000):
        telemetry.received("SPX", NS+100, NS+90)
        telemetry.admitted("SPX")
        telemetry.rejected("SPX", "CROSSED_QUOTE")
    assert telemetry._rows["SPX"] is row  # no per-record history/row replacement
    assert initial["families"]["SPX"]["first_admitted_at_utc"] is None
    assert telemetry.snapshot()["families"]["NDX"]["first_received_at_utc"] is None


def test_unmapped_receipt_is_never_reassigned_when_a_later_quote_maps(monkeypatch):
    monkeypatch.setattr(stream_module.time, "time_ns", lambda: NS+20)
    telemetry = _QuoteArrivalTelemetry(EPOCH, 1, ["SPX"])
    telemetry.received(None, NS, NS-10)
    telemetry.rejected(None, "SYMBOL_MAPPING_UNAVAILABLE")
    unknown = telemetry._rows["UNKNOWN"]
    telemetry.received("SPX", NS+10, NS)
    telemetry.admitted("SPX")
    assert telemetry._rows["UNKNOWN"] is unknown
    assert unknown.mapped_at_ns is None and unknown.admitted_at_ns is None
    assert telemetry._rows["SPX"].received_at_ns == NS+10


def test_generation_and_epoch_reset_cannot_receive_retiring_reader_updates():
    streamer = stream_module.DatabentoGammaStreamer(["SPX"], subscription_epoch_id=EPOCH)
    streamer.active_generation = 1
    retired = streamer._quote_arrivals
    retired.received("SPX", NS, NS)
    streamer.active_generation = 2
    current = streamer._quote_arrivals
    retired.admitted("SPX")
    assert current.generation == 2 and current._rows["SPX"].received_at_ns is None
    streamer.active_generation = 2
    assert streamer._quote_arrivals is current
    streamer.subscription_epoch_id = "b" * 64
    streamer.active_generation = 2
    assert streamer._quote_arrivals is not current
    assert streamer._quote_arrivals.epoch == "b" * 64


def test_actual_reader_distinguishes_unmapped_rejected_and_admitted_quotes(monkeypatch):
    monkeypatch.setattr(stream_module, "DATABENTO_REQUIRED_SYMBOLS", ["SPX"])
    streamer = stream_module.DatabentoGammaStreamer(["SPX"], subscription_epoch_id=EPOCH)
    streamer.api_key = "offline-test-only"
    streamer.replay_minutes = 0
    streamer.live_symbols = [SYMBOL]
    streamer.is_running = True
    monkeypatch.setattr(streamer, "_subscription_window", lambda: {
        "state": "regular_session", "subscription_allowed": True})
    monkeypatch.setattr(streamer, "_build_universe", lambda **kwargs: None)
    monkeypatch.setattr(streamer, "_stream_stalled", lambda: False)
    captures = []

    def quote(bid=1_000_000_000):
        stamp = time.time_ns() // 1_000_000_000 * 1_000_000_000
        return SimpleNamespace(instrument_id=1, rtype=192, ts_recv=stamp, ts_index=stamp,
                               ts_event=stamp, bid_px_00=bid, ask_px_00=2_000_000_000)

    class FakeLocalClient:
        def __init__(self, **kwargs): pass
        def subscribe(self, **kwargs): pass
        def stop(self): pass
        def block_for_close(self, **kwargs): pass
        def __iter__(self):
            yield quote()
            captures.append(streamer._quote_arrivals.snapshot())
            yield type("SymbolMappingMsg", (), {"instrument_id": 1, "stype_out_symbol": SYMBOL})()
            yield quote(bid=None)
            captures.append(streamer._quote_arrivals.snapshot())
            yield quote()
            captures.append(streamer._quote_arrivals.snapshot())
            streamer.is_running = False

    monkeypatch.setattr(stream_module.db, "Live", FakeLocalClient)
    streamer._run_live()
    unknown = captures[0]["families"]["UNKNOWN"]
    assert unknown["first_received_at_utc"] is not None
    assert unknown["first_rejection_reason"] == "SYMBOL_MAPPING_UNAVAILABLE"
    assert captures[0]["families"]["SPX"]["first_received_at_utc"] is None
    rejected = captures[1]["families"]["SPX"]
    assert rejected["first_mapped_at_utc"] is not None
    assert rejected["first_admitted_at_utc"] is None
    assert rejected["first_rejection_reason"] == "INVALID_QUOTE_PRICE"
    admitted = captures[2]["families"]["SPX"]
    assert admitted["first_admitted_at_utc"] is not None
    assert admitted["first_received_replay_pending"] is False
    assert admitted["first_admitted_replay_pending"] is False
    assert admitted["first_received_at_utc"] == rejected["first_received_at_utc"]
    assert captures[2]["families"]["UNKNOWN"] == unknown
    assert streamer.messages_received == 1 and streamer.invalid_quote_records == 1
    assert streamer.unmapped_quote_records == 1 and streamer.reconnect_attempts == 0
    assert set(admitted) == {"first_received_at_utc", "first_provider_ts_recv_utc", "first_mapped_at_utc",
                            "first_admitted_at_utc", "first_rejection_at_utc", "first_rejection_reason",
                            "first_received_replay_pending", "first_admitted_replay_pending"}


def test_health_diagnostics_never_grant_prediction_authority_or_cross_identity(monkeypatch):
    from backend.api.routers import health
    _live_pipeline_snapshot = health._live_pipeline_snapshot
    streamer = stream_module.DatabentoGammaStreamer(["SPX"], subscription_epoch_id=EPOCH)
    streamer.active_generation = 1
    streamer._quote_arrivals.received("SPX", NS, NS)
    streamer._quote_arrivals.admitted("SPX")
    observed = streamer.get_health()
    snapshot = _live_pipeline_snapshot(streamer, observed)
    assert snapshot["prediction_pipeline_ok"] is False
    assert snapshot["symbol_status"]["SPX"]["quote_arrival_diagnostics"]["first_admitted_at_utc"] is not None
    async def sample(*_args):
        return {"health": observed, "pipeline": snapshot, "quant_inference_device": "cpu",
                "runtime_controls": {}, "sampled_at_utc": "2026-10-08T13:30:01+00:00"}
    monkeypatch.setattr(health, "_read_or_503", sample)
    live = asyncio.run(health.health_live_status())
    assert live["quote_arrival_diagnostics"]["authority"] == "DIAGNOSTIC_ONLY"
    assert "UNKNOWN" in live["quote_arrival_diagnostics"]["families"]
    assert live["prediction_pipeline_ok"] is False
    previous = deepcopy(observed)
    observed["quote_arrival_diagnostics"]["subscription_generation"] = 2
    assert "quote_arrival_diagnostics" not in _live_pipeline_snapshot(streamer, observed)["symbol_status"]["SPX"]
    assert previous["quote_arrival_diagnostics"]["subscription_generation"] == 1
