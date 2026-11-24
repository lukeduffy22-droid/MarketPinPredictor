"""Read-only catalyst dashboard. Polling and quote sampling run outside the API request."""
from __future__ import annotations

import asyncio
import logging
import os
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path

from fastapi import APIRouter, Request

from backend.market_event_estimates import enrich_earnings, fetch_upcoming_macro_consensus
from backend.market_event_quotes import build_market_context, fetch_cash_index_context, fetch_daily_index_context
from backend.market_event_sources import collect_market_events
from backend.market_issuer_feeds import collect_issuer_earnings, merge_issuer_earnings
from backend.market_event_proxies import ETFReactionBackfill
from backend.market_events import EventArchive, QuoteWindow, assess, instant
from backend.streamer import get_streamer

logger = logging.getLogger(__name__)
router = APIRouter(tags=["market-data"])
POLL_SECONDS = 120
SAMPLE_SECONDS = 15
UTC = timezone.utc


class MarketEventMonitor:
    def __init__(self, path: Path):
        self.archive = EventArchive(path)
        self.window = QuoteWindow()
        self.proxies = ETFReactionBackfill()
        self.archive_lock = asyncio.Lock()
        self.events = []
        self.indices = []
        self.sources = {}
        self.daily = {}
        self.cash = {}
        self.fetched_at = None
        self.indices_checked_at = None
        self.status = "initializing"
        self.error = None

    def snapshot(self, now: datetime) -> dict:
        sampled = instant(self.indices_checked_at)
        indices = self.indices
        if sampled and now - sampled > timedelta(seconds=3 * SAMPLE_SECONDS):
            indices = [dict(row, status="stale", detail="Local quote sampler has stopped advancing.")
                       if row.get("status") in {"current", "delayed"} else row for row in indices]
        last_poll = instant(self.fetched_at)
        status = self.status
        if last_poll and now - last_poll > timedelta(seconds=POLL_SECONDS * 3):
            status = "stale"
        return {"schema_version": "marketpin-market-events.v1", "events": self.events,
                "sources": list(self.sources.values()), "indices": indices, "poll_status": status,
                "poll_interval_seconds": POLL_SECONDS, "fetched_at": self.fetched_at,
                "indices_checked_at": self.indices_checked_at, "last_error": self.error}

    async def restore(self):
        self.events = [assess(event) for event in await asyncio.to_thread(self.archive.restore)]
        if self.events:
            self.status = "stale"

    async def poll(self):
        self.status = "refreshing"
        official, (issuer_events, issuer_sources), (estimates, estimate_source) = await asyncio.gather(
            asyncio.to_thread(collect_market_events, sec_user_agent=os.getenv("SEC_USER_AGENT")),
            asyncio.to_thread(collect_issuer_earnings), asyncio.to_thread(fetch_upcoming_macro_consensus))
        events = merge_issuer_earnings(official["events"], issuer_events)
        events, earnings_source = await asyncio.to_thread(enrich_earnings, events)
        primary_sources = [*official["sources"], *issuer_sources]
        sources = [*primary_sources, estimate_source, earnings_source]
        self.sources.update({row["id"]: row for row in sources})
        now = datetime.now(UTC)
        async with self.archive_lock:
            records = await asyncio.to_thread(self.archive.record, events, estimates, now)
            self.events = [assess(event) for event in records]
        self.fetched_at = now.isoformat()
        if any(row["status"] in {"ok", "partial"} for row in primary_sources):
            self.status, self.error = "ready", None
        else:
            self.status = "error"
            self.error = "No official release feed succeeded on this poll. Previously observed events are retained."

    async def poll_prices(self, feed: str):
        fetch = fetch_daily_index_context if feed == "daily" else fetch_cash_index_context
        response = await asyncio.to_thread(fetch)
        # Retain the last known observations through source outages. The builder checks their own ages.
        getattr(self, feed).update(response["rows"])
        source = response["source"]
        self.sources[source["id"]] = source

    async def sample(self):
        now = datetime.now(UTC)
        daily, cash = self.daily.copy(), self.cash.copy()
        rows = await asyncio.to_thread(lambda: build_market_context(get_streamer(), daily=daily, cash=cash, now=now))
        self.indices, self.indices_checked_at = rows, now.isoformat()
        self.window.observe(rows, now)
        async with self.archive_lock:
            await self.update_windows(now)

    async def sample_proxies(self):
        updates, health = await asyncio.to_thread(self.proxies.collect, self.events)
        self.sources[health["id"]] = health
        if updates:
            async with self.archive_lock:
                await self.update_windows(datetime.now(UTC), proxies=updates)

    async def update_windows(self, now, proxies=None):
        windows = {}
        updated = []
        for event in self.events:
            published = instant(event["published_at"])
            if published and timedelta(minutes=5) <= now - published <= timedelta(hours=2):
                previous = {(row["symbol"], row["basis"]): row for row in event["event_window"]}
                measured = self.window.reaction(published)
                previous.update({(row["symbol"], row["basis"]): row for row in measured})
                proxy = (proxies or {}).get(event["id"])
                if proxy and instant(proxy["published_at"]) == published:
                    previous.update({(row["symbol"], row["basis"]): row for row in proxy["rows"]})
                merged = [previous[key] for key in sorted(previous)]
                if merged != event["event_window"]:
                    windows[event["id"]] = (published.isoformat(), merged)
                    event = assess(dict(event, event_window=merged))
            updated.append(event)
        if windows:
            await asyncio.to_thread(self.archive.retain_windows, windows)
            self.events = updated

    async def repeat(self, operation, seconds: int, *args):
        while True:
            try:
                await operation(*args)
            except Exception as exc:
                # Network exception text can include API credentials in URLs.
                logger.warning("Market event %s failed (%s)", operation.__name__, type(exc).__name__)
                if operation == self.poll:
                    self.status = "error"
                    self.error = f"Release refresh failed ({type(exc).__name__}). Previous evidence retained."
            await asyncio.sleep(seconds)


@asynccontextmanager
async def market_event_lifespan(app):
    monitor = MarketEventMonitor(Path("data/market_events/events.db"))
    app.state.market_events = monitor
    if os.getenv("MARKETPIN_MARKET_EVENTS", "1").strip().lower() in {"0", "false", "off"}:
        monitor.status = "disabled"
        yield
        return

    async def releases():
        try:
            await monitor.restore()
        except Exception as exc:
            logger.warning("Market event archive restore failed (%s)", type(exc).__name__)
        await monitor.repeat(monitor.poll, POLL_SECONDS)

    tasks = [asyncio.create_task(coro) for coro in (
        releases(), monitor.repeat(monitor.sample, SAMPLE_SECONDS),
        monitor.repeat(monitor.poll_prices, 1800, "daily"), monitor.repeat(monitor.poll_prices, 60, "cash"),
        monitor.repeat(monitor.sample_proxies, 60))]
    try:
        yield
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


@router.get("/market-events", summary="Read observed releases, consensus and timestamped index context")
async def market_events_snapshot(request: Request) -> dict:
    return request.app.state.market_events.snapshot(datetime.now(UTC))
