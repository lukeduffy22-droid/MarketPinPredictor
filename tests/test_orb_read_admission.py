import asyncio
from datetime import date
import threading

from backend.api.routers import orb


def test_different_orb_requests_cannot_run_projections_concurrently(monkeypatch):
    first_entered = threading.Event()
    release = threading.Event()
    calls = []

    class Journal:
        def snapshot(self, symbol, **kwargs):
            calls.append(symbol)
            if symbol == "SPX":
                first_entered.set()
                assert release.wait(2)
            return {"symbol": symbol, "usable_for_prediction": False}

    monkeypatch.setattr(orb, "get_market_structure_journal", Journal)
    monkeypatch.setattr(orb, "get_streamer", lambda: object())

    async def exercise():
        first = asyncio.create_task(orb.get_orb("SPX", trading_date=date(2026, 9, 16)))
        try:
            async with asyncio.timeout(1):
                while not first_entered.is_set():
                    await asyncio.sleep(0.001)
            second = asyncio.create_task(orb.get_orb("NDX", trading_date=date(2026, 9, 16)))
            await asyncio.sleep(0.03)
            assert calls == ["SPX"]
            release.set()
            results = await asyncio.gather(first, second)
            assert [result["symbol"] for result in results] == ["SPX", "NDX"]
            assert all(result["usable_for_prediction"] is False for result in results)
        finally:
            release.set()
            await first

    asyncio.run(exercise())


def test_multi_symbol_projection_uses_one_worker_and_one_runtime_binding():
    worker_threads = []
    bindings = []

    class Streamer:
        def get_subscription_context(self):
            return {"subscription_epoch_id": "a" * 64, "subscription_generation": 2, "handoff_status": "active"}

    class Journal:
        def snapshot(self, symbol, **kwargs):
            worker_threads.append(threading.get_ident())
            bindings.append(kwargs)
            return {"symbol": symbol}

    result, context, stable = orb._live_snapshots(Journal(), Streamer(), ("SPX", "NDX"), configured=("SPX", "NDX"))
    assert list(result) == ["SPX", "NDX"]
    assert worker_threads == [threading.get_ident()] * 2
    assert bindings[0] == bindings[1]
    assert context["subscription_generation"] == 2
    assert stable is True
