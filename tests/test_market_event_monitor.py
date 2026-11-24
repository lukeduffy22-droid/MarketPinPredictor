import asyncio
from datetime import datetime, timedelta, timezone

from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.api.routers import market_events as api
from backend.market_events import EventArchive, QuoteWindow, assess

RELEASE = datetime(2026, 11, 10, 13, 30, tzinfo=timezone.utc)


def release():
    return {"id": "release-1", "category": "cpi", "period": "2026-10", "source": "BLS",
            "headline": "Consumer Price Index", "published_at": RELEASE.isoformat(),
            "figures": [{"metric": "Core CPI MoM", "actual": 0.3, "unit": "%", "expected": None}]}


def survey(minutes, **updates):
    return {"category": "cpi", "period": "2026-10", "metric": "Core CPI MoM", "unit": "%",
            "expected": 0.2, "expected_source": "Survey", "expected_url": "https://example.com/survey",
            "expected_as_of": (RELEASE + timedelta(minutes=minutes)).isoformat(), **updates}


def quote(seconds, level, **updates):
    return {"status": "current", "source": "Databento OPRA", "symbol": "RUT",
            "basis": "option_implied_reference", "price": level,
            "observed_at": (RELEASE + timedelta(seconds=seconds)).isoformat(),
            "subscription_epoch_id": "epoch", "active_generation": 3,
            "primary_expiration": "2026-11-10", **updates}


def test_archive_matches_only_latest_prepublication_comparable_survey(tmp_path):
    path = tmp_path / "archive.db"
    db = EventArchive(path)
    surveys = [survey(-60, expected=-1), survey(-10), survey(0, expected=7), survey(1, expected=8),
               survey(-2, period="2026-09"), survey(-1, unit="index"), survey(-1, metric="Core CPI YoY"),
               survey(-1, expected=float("nan"))]
    now = RELEASE + timedelta(minutes=2)
    first = db.record([release(), dict(release(), id="future", published_at=(now + timedelta(days=1)).isoformat())], surveys, now)
    assert len(first) == 1
    figure = first[0]["figures"][0]
    assert (figure["expected"], figure["surprise"]) == (0.2, 0.1)
    assert "percentage points" in assess(first[0])["assessment"]
    db.record([release()], [], now + timedelta(minutes=1))
    assert EventArchive(path).restore() == first


def test_window_requires_same_instrument_source_basis_generation_expiry_and_timely_ticks():
    window = QuoteWindow()
    now = RELEASE + timedelta(minutes=7)
    window.observe([quote(-1, 100)], now)
    for change in ({"active_generation":4}, {"subscription_epoch_id":"new"},
                   {"primary_expiration":"2026-11-11"}, {"source":"other"}, {"status":"delayed"},
                   {"basis":"daily_index_close"}, {"symbol":"IWM"}):
        window.observe([quote(301, 1000, **change)], now)
    window.observe([quote(299, 102), quote(399, 103)], now)
    assert window.reaction(RELEASE) == []
    window.observe([quote(301, 101, basis="cash_index", source="Massive")], now)
    assert window.reaction(RELEASE) == []
    complete = QuoteWindow()
    complete.observe([quote(-10, 100), quote(302, 101)], now)
    assert complete.reaction(RELEASE)[0]["change_pct"] == 1.0
    complete.observe([quote(-1, 200, basis="cash_index", source="Massive"),
                      quote(303, 201, basis="cash_index", source="Massive")], now)
    assert complete.reaction(RELEASE)[0]["basis"] == "cash_index"
    assert complete.reaction(RELEASE)[0]["change_pct"] == 0.5
    complete.observe([], now + timedelta(hours=3))
    assert complete.reaction(RELEASE) == []


def test_stale_last_known_snapshot_is_never_reported_current(tmp_path):
    monitor = api.MarketEventMonitor(tmp_path / "a.db")
    monitor.indices = [quote(0, 100)]
    monitor.indices_checked_at = monitor.fetched_at = RELEASE.isoformat()
    monitor.status = "ready"
    app = FastAPI()
    app.state.market_events = monitor
    app.include_router(api.router)
    snapshot = monitor.snapshot(RELEASE + timedelta(hours=1))
    assert (snapshot["poll_status"], snapshot["indices"][0]["status"]) == ("stale", "stale")
    assert monitor.indices[0]["status"] == "current"
    assert TestClient(app).get("/market-events").json()["schema_version"] == "marketpin-market-events.v1"
    assert not monitor.archive.path.exists()


def test_poll_failures_retain_last_verified_release_and_timestamp(tmp_path, monkeypatch):
    monitor = api.MarketEventMonitor(tmp_path / "archive.db")
    now = datetime.now(timezone.utc)
    event = dict(release(), published_at=(now-timedelta(hours=1)).isoformat())
    health = {"id":"official", "status":"ok"}
    official = {"events":[event], "sources":[health]}
    monkeypatch.setattr(api, "collect_market_events", lambda **kw: official)
    monkeypatch.setattr(api, "collect_issuer_earnings", lambda: ([], []))
    monkeypatch.setattr(api, "fetch_upcoming_macro_consensus", lambda: ([], {"id":"survey","status":"unconfigured"}))
    monkeypatch.setattr(api, "enrich_earnings", lambda events: (events, {"id":"earn","status":"unconfigured"}))
    asyncio.run(monitor.poll())
    initial = monitor.events[0]
    official.update(events=[], sources=[dict(health, status="error")])
    asyncio.run(monitor.poll())
    assert monitor.events == [initial]
    assert monitor.status == "error" and monitor.error
    restored = api.MarketEventMonitor(monitor.archive.path)
    asyncio.run(restored.restore())
    assert restored.events == [initial] and restored.status == "stale"


def test_superseded_filing_stays_deduplicated_and_signed_provider_figures_survive_outage(tmp_path):
    db = EventArchive(tmp_path / "e.db")
    later = RELEASE + timedelta(minutes=5)
    sec = dict(release(), id="sec", category="earnings", source="SEC EDGAR", published_at=later.isoformat())
    evidence = {"metric":"Adjusted EPS", "unit":"USD/share", "actual":2, "expected":1.9,
                "source":"Finnhub", "as_of":later.isoformat()}
    ir = dict(sec, id="issuer", source="Issuer", published_at=RELEASE.isoformat(), figures=[evidence],
              source_links=[{"source_url":"https://www.sec.gov/a","source":"SEC EDGAR"}], supersedes_ids=["sec"])
    assert [r["id"] for r in db.record([sec], [], later)] == ["sec"]
    assert [r["id"] for r in db.record([ir], [], later)] == ["issuer"]
    restart = EventArchive(db.path)
    reduced = dict(ir, figures=[], source_links=[], supersedes_ids=[])
    row, = restart.record([reduced, sec], [], later)
    assert row["source_links"] == ir["source_links"] and row["figures"] == [evidence]


def test_vol_context_and_unconfirmed_filing_are_labeled():
    filing = dict(release(), category="earnings", source="SEC EDGAR", form="8-K/A", period=None)
    assert assess(filing)["significance"] == "watch"
    speech = dict(filing, category="fed_commentary", source="Federal Reserve Board", event_window=[{
        "symbol":"VIX", "basis":"cash_index", "before_level":15.25, "after_level":16., "change_pct":4.92}])
    assert assess(speech)["significance"] == "material_release"
    assert "+0.75 points" in assess(speech)["assessment"]
