from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from streamlit.testing.v1 import AppTest

from app.services import market_events_view as view


def fixture():
    published = "2026-11-10T13:30:00Z"
    return {
        "schema_version": "marketpin-market-events.v1", "poll_status": "ready", "fetched_at": published,
        "poll_interval_seconds": 900, "sources": [
            {"name": "BLS", "status": "ok", "checked_at": published},
            {"name": "SEC", "status": "unconfigured", "detail": "Contact required"}],
        "indices": [{"name": "Russell 2000", "symbol": "RUT", "basis": "option_implied_reference", "status": "current",
                     "price": 2300, "observed_at": published, "source": "Databento"},
                    {"name": "Dow", "symbol": "DJI", "basis": "daily_index_close", "status": "daily_close",
                     "price": 40000, "session_date": "2026-11-09", "change_pct": -1.5, "change_basis": "close to close"},
                    {"name": "Nasdaq-100", "symbol": "NDX", "basis": "unavailable", "status": "unavailable", "price": 1}],
        "events": [
            {"id": "inflation", "headline": "Consumer Price Index", "category": "cpi", "facts": "Headline CPI was unchanged.",
             "source": "BLS", "source_url": "https://www.bls.gov/", "published_at": published, "observed_at": published,
             "figures": [{"metric": "Monthly CPI", "actual": 0.0, "unit": "%", "expected": None, "surprise": 3}],
             "uncertainty": "No verified consensus.", "assessment": "Attribution is uncertain.", "significance": "material_release",
             "event_window": [{"symbol": "RUT", "basis": "option_implied_reference", "before_at": "2026-11-10T13:29:00Z",
                               "after_at": "2026-11-10T13:31:00Z", "change_pct": 0.3}]},
            {"id": "remarks", "headline": "Governor remarks", "category": "fed_commentary", "source": "Federal Reserve",
             "published_at": "2026-11-09T12:00:00Z", "facts": "The governor spoke.", "significance": "watch"},
        ],
    }


def test_active_fragment_shows_partial_coverage_sources_reactions_and_filters(monkeypatch):
    data = fixture()
    calls = []
    def read():
        calls.append(True)
        return data
    monkeypatch.setattr(view, "fetch_market_events", read)
    ui = AppTest.from_string(
        "from app.services.market_view_navigation import select_market_view\n"
        "from app.services.market_events_view import render_market_events\n"
        "if select_market_view() == 'Market Events': render_market_events()"
    ).run()
    assert not ui.exception and not calls
    ui.radio[0].set_value("Market Events").run()
    assert not ui.exception and len(calls) == 1
    assert any("Sources awaiting configuration · SEC" in x.value for x in ui.caption)
    assert any("07:30:00 AM CST" in x.value for x in ui.caption)
    assert ui.subheader[-1].value == "Consumer Price Index"
    assert [item.value for item in ui.markdown] == ["**Confirmed facts**", "**Market window around publication**", "**Interpretation**", "**Uncertainty**"]
    indices = ui.dataframe[1].value.set_index("Symbol")
    assert indices.loc["DJI", "As of"] == "2026-11-09"
    assert indices.loc["RUT", "Price basis"] == "option implied reference"
    assert indices["Level"].isna().loc["NDX"]
    assert ui.dataframe[2].value["Actual"].iloc[0] == 0
    assert ui.dataframe[2].value["Surprise"].isna().all()
    assert ui.dataframe[3].value.iloc[0]["Change (%)"] == 0.3
    ui.selectbox[0].set_value("fed_commentary").run()
    assert not ui.exception and ui.subheader[-1].value == "Governor remarks"
    assert any("No comparable observations" in x.value for x in ui.caption)
    ui.checkbox[0].set_value(True).run()
    assert any("No retained releases" in x.value for x in ui.info)
    ui.radio[0].set_value("Advisor").run()
    assert len(calls) == 3 and not ui.dataframe


def test_transport_outage_and_invalid_schema_withhold_old_results(monkeypatch):
    class Response:
        def raise_for_status(self):
            pass
        def json(self):
            return {"schema_version": "future"}
    monkeypatch.setattr(view.requests, "get", lambda *args, **kwargs: Response())
    ui = AppTest.from_string("from app.services.market_events_view import render_market_events\nrender_market_events()").run()
    assert not ui.exception and "service unavailable" in ui.warning[0].value and not ui.dataframe
    def offline(*args, **kwargs):
        raise view.requests.ConnectionError()
    monkeypatch.setattr(view.requests, "get", offline)
    ui.run()
    assert not ui.exception and "service unavailable" in ui.warning[0].value and not ui.dataframe


def test_earnings_source_provenance_and_expected_zero():
    rows = view._figure_rows({"figures": [{"metric": "Adjusted EPS", "actual": 0.12, "expected": 0,
        "unit": "USD/share", "surprise": 0.12, "period": "FY2026 Q3", "source": "Issuer",
        "expected_source": "Finnhub", "expected_source_url": "https://finnhub.io/", "estimate_vintage": "unverified",
        "source_url": "javascript:invalid", "note": "Estimate vintage unverified"}]})
    assert rows[0]["Expected"] == 0 and rows[0]["Surprise"] == 0.12
    assert rows[0]["Estimate as of"] == "unverified" and rows[0]["Estimate link"] == "https://finnhub.io/"
    assert rows[0]["Actual link"] is None


def test_disabled_monitor_is_distinct_from_outage(monkeypatch):
    data = fixture()
    data["poll_status"] = "disabled"
    monkeypatch.setattr(view, "fetch_market_events", lambda: data)
    ui = AppTest.from_string("from app.services.market_events_view import render_market_events\nrender_market_events()").run()
    assert not ui.exception and not ui.warning and not ui.dataframe
    assert "MARKETPIN_MARKET_EVENTS" in ui.info[0].value


def test_browser_alerts_ignore_history_and_repeat_but_notice_new_or_reclassified_material():
    now = datetime.now(timezone.utc)
    toasts = []
    st = SimpleNamespace(session_state={}, toast=toasts.append)
    old = {"id": "old", "category": "cpi", "published_at": (now - timedelta(days=2)).isoformat(), "significance": "material_release"}
    watched = dict(old, id="speech", category="fed_commentary", significance="watch")
    view._notify_new_material(st, [old, watched], now)
    assert toasts == []
    new = dict(old, id="new", published_at=(now + timedelta(seconds=5)).isoformat())
    backfill = dict(old, id="another old item")
    material = [new, old, backfill, dict(watched, significance="material_release")]
    view._notify_new_material(st, material, now + timedelta(seconds=30))
    assert len(toasts) == 2
    view._notify_new_material(st, material, now + timedelta(seconds=55))
    merged = dict(new, id="issuer", supersedes_ids=["new"])
    view._notify_new_material(st, [old, merged], now + timedelta(seconds=80))
    view._notify_new_material(st, [*material, dict(new, id="away")], now + timedelta(hours=2))
    assert len(toasts) == 2
