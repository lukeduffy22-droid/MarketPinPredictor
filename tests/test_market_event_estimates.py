from datetime import datetime, timezone

import pytest
import requests

from backend import market_event_estimates as estimates

NOW = datetime(2026, 11, 20, 14, tzinfo=timezone.utc)
EVENT = {"category": "earnings", "ticker": "NVDA", "form": "8-K", "period": "2026-10-25", "fiscal_year_end": "0131",
         "published_at": "2026-11-19T00:10:03Z", "source": "SEC EDGAR",
         "figures": [{"metric": "GAAP diluted EPS", "actual": 2.0, "expected": None}]}
RESULT = {"date": "2026-11-18", "symbol": "NVDA", "year": 2027, "quarter": 3,
          "epsActual": 2.1, "epsEstimate": 2.0, "revenueActual": 8.0e10, "revenueEstimate": 7.9e10}
SURVEY = {"Country": "United States", "Event": "Core PCE Price Index MoM",
          "Date": "2026-11-25T13:30:00", "DateSpan": "0", "ReferenceDate": "2026-10-31T00:00:00",
          "Actual": "", "Forecast": "0.3%", "TEForecast": "0.7%", "Unit": "%",
          "URL": "/united-states/core-pce-price-index-mom"}


def reply(monkeypatch, payload):
    monkeypatch.setattr(estimates, "_get_json", lambda *args, **kwargs: payload)


def test_earnings_matches_us_date_and_keeps_issuer_and_vendor_basis(monkeypatch):
    def calendar(url, *, headers, params):
        assert headers == {"X-Finnhub-Token": "test-key"}
        assert params == {"from": "2026-11-18", "to": "2026-11-18"}
        return {"earningsCalendar": [RESULT]}
    monkeypatch.setattr(estimates, "_get_json", calendar)
    events, status = estimates.enrich_earnings([EVENT], api_key="test-key", now=NOW)
    assert status["status"] == "ok" and "test-key" not in str(status)
    original, eps, rev = events[0]["figures"]
    assert original == EVENT["figures"][0] and len(EVENT["figures"]) == 1
    assert (eps["actual"], eps["expected"], eps["unit"], eps["period"]) == (2.1, 2.0, "USD/share", "FY2027 Q3")
    assert eps["expected_as_of"] is None and eps["estimate_vintage"] == "unverified"
    assert eps["source_url"].startswith("https://finnhub.io/")
    assert "proprietary" in rev["note"]
    again, _ = estimates.enrich_earnings(events, api_key="test-key", now=NOW)
    assert again == events


@pytest.mark.parametrize("rows", [[RESULT, RESULT], [dict(RESULT, symbol="AMD")],
                                  [dict(RESULT, date="2026-11-19")], [dict(RESULT, quarter=0)], [dict(RESULT, quarter=2)], [dict(RESULT, year=2026)],
                                  [dict(RESULT, epsActual=None, revenueActual=None)]])
def test_unmatched_earnings_unchanged(monkeypatch, rows):
    reply(monkeypatch, {"earningsCalendar": rows})
    result, _ = estimates.enrich_earnings([EVENT], api_key="test", now=NOW)
    assert result == [EVENT]


def test_ignore_amendment_duplicate_filing_and_future_or_stale_events(monkeypatch):
    reply(monkeypatch, {"earningsCalendar": [RESULT]})
    for events in ([EVENT, EVENT], [dict(EVENT, form="8-K/A")], [dict(EVENT, period=None)],
                   [dict(EVENT, fiscal_year_end=None)], [dict(EVENT, period="2026-11-08")], [dict(EVENT, published_at="2026-11-21T01:00:00Z")],
                   [dict(EVENT, published_at="2026-09-21T01:00:00Z")]):
        assert estimates.enrich_earnings(events, api_key="test", now=NOW)[0] == events


def test_missing_estimate_retains_actual_and_zero_estimate_survives(monkeypatch):
    reply(monkeypatch, {"earningsCalendar": [dict(RESULT, epsEstimate="NaN", revenueEstimate=0)]})
    updated, _ = estimates.enrich_earnings([EVENT], api_key="test", now=NOW)
    eps, revenue = updated[0]["figures"][1:]
    assert eps["actual"] == 2.1 and eps["expected"] is None
    assert revenue["expected"] == 0


def test_macro_snapshot_has_true_receipt_time_and_only_survey(monkeypatch):
    def calendar(url, *, headers, params):
        assert headers == {"Authorization": "test-key"} and params == {"f": "json"}
        assert url.endswith("2026-11-20/2026-12-25")
        return [SURVEY, dict(SURVEY, Event="Inflation Rate YoY", Forecast=0, URL="//bad.example")]
    monkeypatch.setattr(estimates, "_get_json", calendar)
    records, status = estimates.fetch_upcoming_macro_consensus(api_key="test-key", now=NOW)
    assert status["status"] == "ok" and "test-key" not in str(records)
    assert [(r["category"], r["metric"], r["expected"]) for r in records] == [
        ("pce", "Core PCE MoM", .3), ("cpi", "Headline CPI YoY", 0)]
    for record in records:
        assert record["period"] == "2026-10" and record["unit"] == "%"
        assert record["expected_as_of"] == NOW.isoformat()
        assert record["expected_url"].startswith("https://tradingeconomics.com/")


@pytest.mark.parametrize("change", [
    {"Actual": "0.1%"}, {"Actual": 0}, {"Date": "2026-11-20T14:00:00"},
    {"DateSpan": "1"}, {"Event": "Core PCE Prices QoQ"}, {"Event": "Core PCE Price Index"},
    {"ReferenceDate": None}, {"ReferenceDate": "2027-01-31T00:00:00"}, {"Forecast": ""},
    {"Forecast": "0.3", "Unit": "index"}, {"Country": "Mexico"}])
def test_skip_noncomparable_post_release_or_missing_survey(monkeypatch, change):
    reply(monkeypatch, [dict(SURVEY, **change)])
    assert estimates.fetch_upcoming_macro_consensus(api_key="test", now=NOW)[0] == []


def test_skip_ambiguous_simultaneous_survey_records(monkeypatch):
    reply(monkeypatch, [SURVEY, dict(SURVEY, Forecast="0.4%")])
    assert estimates.fetch_upcoming_macro_consensus(api_key="test", now=NOW)[0] == []


def test_missing_key_has_no_network_and_errors_have_no_token(monkeypatch):
    def offline(*args, **kwargs):
        raise requests.Timeout("secret-value")
    monkeypatch.setattr(estimates, "_get_json", offline)
    for key, state in (("", "unconfigured"), ("secret-value", "error")):
        for data, status in (estimates.fetch_upcoming_macro_consensus(api_key=key, now=NOW),
                             estimates.enrich_earnings([EVENT], api_key=key, now=NOW)):
            assert status["status"] == state and "secret-value" not in str(status)
            assert data in ([], [EVENT])


def test_fed_survey_compares_only_upper_bound_for_same_eastern_meeting_date(monkeypatch):
    from backend.market_events import EventArchive
    from tempfile import TemporaryDirectory
    from pathlib import Path

    # Distinct rows guard against confusing projections or an alternate indicator
    # with the provider's meeting decision. UTC date rolls over in this fixture.
    row = dict(SURVEY, Event="Fed Interest Rate Decision", Date="2026-12-10T00:15:00",
               ReferenceDate=None, Forecast="4.25%", URL="/united-states/interest-rate")
    reply(monkeypatch, [row, dict(row, Event="FOMC Economic Projections"),
                        dict(row, URL="/united-states/effective-federal-funds-rate")])
    surveys, status = estimates.fetch_upcoming_macro_consensus(api_key="test", now=NOW)
    assert status["status"] == "ok" and len(surveys) == 1
    assert surveys[0]["period"] == "2026-12-09" and "upper bound" in surveys[0]["expected_source"]
    published_at = datetime.fromisoformat("2026-12-10T00:15:00+00:00")
    event = {"id": "statement", "category": "fed_policy", "period": "2026-12-09",
             "published_at": published_at.isoformat(),
             "figures": [{"metric": "Fed funds target lower", "unit": "%", "actual": 4.0},
                         {"metric": "Fed funds target upper", "unit": "%", "actual": 4.25}]}
    with TemporaryDirectory() as path:
        retained = EventArchive(Path(path) / "archive.db").record([event], surveys, published_at)
    lower, upper = retained[0]["figures"]
    assert lower.get("expected") is None and upper["expected"] == 4.25 and upper["surprise"] == 0.0
