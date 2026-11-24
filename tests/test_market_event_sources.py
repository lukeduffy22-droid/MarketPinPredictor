"""Offline contracts for the publishers' release and filing formats."""
import json
from datetime import datetime, timezone

import pytest

from backend import market_event_sources as source


NOW = datetime(2026, 11, 20, 16, tzinfo=timezone.utc)
CPI = """
<pre>Transmission of material in this release is embargoed until
8:30 a.m. (ET) Tuesday, November 10, 2026
CONSUMER PRICE INDEX - OCTOBER 2026</pre>
<table><tr><th></th><th>Seasonally adjusted changes from preceding month</th>
<th>Un-<br>adjusted 12-mos. ended Oct. 2026</th></tr>
<tr><th>Sep. 2026</th><th>Oct. 2026</th></tr>
<tr><th>All items</th><td>0.6</td><td>0.4</td><td>-</td></tr>
<tr><th>All items less food and energy</th><td>0.2</td><td>0.3</td><td>-</td></tr></table>
"""
PCE_LINK = "/news/2026/personal-income-and-outlays-september-2026"
PCE_RELEASE = """
EMBARGOED UNTIL RELEASE AT 8:30 a.m. EDT, Thursday, October 29, 2026
<h1>Personal Income and Outlays, September 2026</h1>
<p>Personal consumption expenditures increased 9.8 percent.</p>
<p>From the preceding month, <a>the PCE price index for September increased 0.5 percent</a>.
Excluding food and energy, the PCE price index declined 0.3 percent.</p>
<p>From the same month one year ago, the PCE price index for September increased 3.6 percent.
Excluding food and energy, the PCE price index increased 3.1 percent from one year ago.</p>
Next release: November 25, 2026, at 8:30 a.m. EST
"""
STATEMENT = "https://www.federalreserve.gov/newsevents/pressreleases/monetary20261028a.htm"
SPEECH = "https://www.federalreserve.gov/newsevents/speech/waller20261110a.htm"


def rss(title, url, date="Wed, 28 Oct 2026 18:00:00 GMT"):
    return (f"<rss><channel><item><title>{title}</title><link>{url}</link>"
            f"<pubDate>{date}</pubDate><description>Official communication</description></item></channel></rss>")


def public_feeds():
    return {
        source.CPI_URL: CPI,
        "https://www.bls.gov/news.release/archives/cpi_11102026.htm": CPI,
        SPEECH: "<p>I believe that inflation risks warrant keeping policy restrictive.</p>",
        source.BEA_URL: f'<a href="{PCE_LINK}">Personal Income and Outlays, September 2026</a>',
        f"https://www.bea.gov{PCE_LINK}": PCE_RELEASE,
        source.FED_POLICY: rss("Federal Reserve issues FOMC statement", STATEMENT),
        source.FED_COMMENTARY: rss("Economic outlook", SPEECH, "Tue, 10 Nov 2026 17:30:00 GMT"),
        STATEMENT: "<p>The Committee decided to lower the target range for the federal funds rate "
                   "by 1/4 percentage point to 3-3/4 to 4 percent.</p>",
    }


def test_release_times_periods_bases_and_null_expectations(monkeypatch):
    monkeypatch.delenv("SEC_USER_AGENT", raising=False)
    feeds = public_feeds()
    result = source.collect_market_events(now=NOW, fetch=lambda url, agent: feeds[url])
    by_category = {event["category"]: event for event in result["events"]}
    cpi, pce, policy = (by_category[name] for name in ("cpi", "pce", "fed_policy"))
    assert (cpi["period"], cpi["published_at"]) == ("2026-10", "2026-11-10T13:30:00Z")
    assert (pce["period"], pce["published_at"]) == ("2026-09", "2026-10-29T12:30:00Z")
    assert {f["metric"]: f["actual"] for f in cpi["figures"]} == {"Headline CPI MoM": .4, "Core CPI MoM": .3}
    assert {f["metric"]: f["actual"] for f in pce["figures"]} == {
        "Headline PCE MoM": .5, "Core PCE MoM": -.3, "Headline PCE YoY": 3.6, "Core PCE YoY": 3.1,
    }
    assert [(f["actual"], f["expected"]) for f in policy["figures"]] == [(3.75, None), (4.0, None)]
    assert "lowered" in policy["facts"]
    assert policy["period"] == "2026-10-28" and policy["published_at"] == "2026-10-28T18:00:00Z"
    assert cpi["source_url"].endswith("cpi_11102026.htm")
    assert by_category["fed_commentary"]["figures"] == []
    assert "keeping policy restrictive" in by_category["fed_commentary"]["facts"]
    assert result["sources"][-1]["status"] == "unconfigured"


def test_release_time_unknown_or_in_future_and_source_failure_are_isolated(monkeypatch):
    monkeypatch.delenv("SEC_USER_AGENT", raising=False)
    feeds = public_feeds()
    feeds[source.FED_POLICY] = rss("Pending", STATEMENT, "Wed, 25 Nov 2026 17:00:00 GMT")
    feeds[source.FED_COMMENTARY] = rss("Speech", SPEECH, "Tue, 10 Nov 2026 12:00:00")
    feeds[source.BEA_URL] = '<a href="https://untrusted.example/news/a">Personal Income and Outlays, May 2026</a>'
    result = source.collect_market_events(now=NOW, fetch=lambda url, agent: feeds[url])
    assert [e["category"] for e in result["events"]] == ["cpi"]
    rows = {s["id"]: s["status"] for s in result["sources"]}
    assert rows["bea_pce"] == rows["fed_commentary"] == "error" and rows["bls_cpi"] == "ok"
    feeds[source.CPI_URL] = CPI.replace("8:30 a.m. (ET)", "")
    with pytest.raises(ValueError):
        source._cpi(feeds.__getitem__)


def test_sec_filing_and_amendment_keep_their_identity_and_fiscal_period(monkeypatch):
    monkeypatch.setattr(source, "TECH_CIKS", {"NVDA": 1045810})
    feeds = public_feeds()
    accessions = ["0001045810-26-000091", "0001045810-26-000092", "0001045810-26-000093"]
    feeds[source.SEC_API + "CIK0001045810.json"] = json.dumps({
        "cik": 1045810, "fiscalYearEnd": "0131", "filings": {"recent": {
            "form": ["8-K/A", "8-K", "8-K"], "items": ["2.02,9.01", "9.01", "2.02, 9.01"],
            "acceptanceDateTime": ["2026-11-19T21:01:02Z", "2026-11-18T21:00:00Z", "2026-11-17T21:01:02Z"],
            "accessionNumber": accessions,
        }}})
    for acc in accessions:
        path = f"https://www.sec.gov/Archives/edgar/data/1045810/{acc.replace('-', '')}/"
        feeds[path + acc + "-index.html"] = (
            '<table><tr><td>2</td><td>News</td><td><a href="q3pr.htm">q3pr.htm</a></td>'
            '<td>EX-99.1</td><td>12345</td></tr></table>')
        feeds[path + "q3pr.htm"] = (
            "Company results for the third quarter ended October 25, 2026. Record quarterly revenue of $10.1 billion. "
            "For the quarter, GAAP earnings per diluted share was $0.95. Non-GAAP earnings per diluted share was $1.20."
            " For fiscal 2026, revenue was $100 billion and GAAP earnings per diluted share was $10.11.")
    calls = []
    def get(url, ua):
        if "sec.gov" in url:
            assert ua == "Example monitor test@example.com"
        calls.append(url)
        return feeds[url]

    result = source.collect_market_events(sec_user_agent="Example monitor test@example.com", now=NOW, fetch=get)
    events = [e for e in result["events"] if e["category"] == "earnings"]
    assert len(events) == 2 and len({e["id"] for e in events}) == 2
    assert all(e["ticker"] == "NVDA" and e["period"] == "2026-10-25" for e in events)
    assert all(e["fiscal_year_end"] == "0131" for e in events)
    assert [e["form"] for e in events] == ["8-K/A", "8-K"]
    assert all([f["actual"] for f in e["figures"]] == [10_100_000_000.0, .95] for e in events)
    assert all(e["figures"][0]["source_url"] == e["exhibit_url"] for e in events)
    assert all(accessions[1] not in url for url in calls)



def test_flat_price_is_a_verified_zero_and_less_than_is_an_unknown_amount():
    feeds = public_feeds()
    url = "https://www.bea.gov" + PCE_LINK
    feeds[url] = feeds[url].replace("for September increased 0.5 percent", "for September was unchanged")
    feeds[url] = feeds[url].replace("declined 0.3 percent", "declined less than 0.1 percent")
    values = {f["metric"]: f["actual"] for f in source._pce(feeds.__getitem__)[0]["figures"]}
    assert values["Headline PCE MoM"] == 0.0 and "Core PCE MoM" not in values
