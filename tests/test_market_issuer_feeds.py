from datetime import datetime, timezone

import pytest

from backend.market_issuer_feeds import FEEDS, collect_issuer_earnings, merge_issuer_earnings

NOW = datetime(2026, 11, 20, 12, tzinfo=timezone.utc)
APPLE = "https://www.apple.com/newsroom/2026/11/apple-reports-fourth-quarter-results/"
NVIDIA = "https://nvidianews.nvidia.com/news/nvidia-announces-financial-results-for-third-quarter-fiscal-2027"
APPLE_TITLE = "Apple reports fourth quarter results"
NV_TITLE = "NVIDIA Announces Financial Results for Third Quarter Fiscal 2027"
APPLE_TIME = "2026-11-02T20:30:00Z"
NV_TIME = "Tue, 17 Nov 2026 21:20:00 GMT"
APPLE_TEXT = ("Apple announced results for its fiscal fourth quarter ended September 26, 2026. "
              "The Company posted quarterly revenue of $100.1 billion, up 5 percent. "
              "Diluted earnings per share was $2.34.")
NV_TEXT = ("NVIDIA today reported revenue for the third quarter ended October 25, 2026, of $90.1 billion. "
           "For the quarter, GAAP and non-GAAP earnings per diluted share were $1.22 and $1.13, respectively.")


def public_fetch(sources):
    sources.setdefault(FEEDS["AMD"][1], "<rss><channel/></rss>")
    return sources.__getitem__


def apple_feed(title=APPLE_TITLE, url=APPLE, timestamp="", extra=""):
    return (f'<feed xmlns="http://www.w3.org/2005/Atom"><entry><title>{title}</title>'
            f'<link rel="alternate" href="{url}"/>{timestamp}</entry>{extra}</feed>')


def nvidia_feed(title=NV_TITLE, url=NVIDIA, stamp=NV_TIME):
    return (f"<rss><channel><item><title>{title}</title><link>{url}</link>"
            f"<pubDate>{stamp}</pubDate></item></channel></rss>")


def test_extracts_quarterly_figures_and_distinguishes_timestamps():
    sources = {FEEDS["AAPL"][1]: apple_feed(timestamp="<updated>2026-11-03T17:00:00Z</updated>"),
               FEEDS["NVDA"][1]: nvidia_feed(), NVIDIA: NV_TEXT,
               APPLE: f'<meta property="article:published_time" content="{APPLE_TIME}">{APPLE_TEXT}'}
    events, statuses = collect_issuer_earnings(now=NOW, fetch=public_fetch(sources))
    apple, nvidia = events
    assert apple["issuer_release"] is True
    assert apple["published_at"] == APPLE_TIME
    assert nvidia["published_at"] == "2026-11-17T21:20:00Z"
    assert [e["period"] for e in events] == ["2026-09-26", "2026-10-25"]
    assert [f["actual"] for f in apple["figures"]] == [100_100_000_000, 2.34]
    assert [f["actual"] for f in nvidia["figures"]] == [90_100_000_000, 1.22]
    assert all(f["source_url"] == e["source_url"] for e in events for f in e["figures"])
    assert all(row["status"] == "ok" for row in statuses)


@pytest.mark.parametrize("apple_time", [
    '<updated>2026-11-02T23:00:00Z</updated>', '<published>2026-11-02</published>',
    '<published>2026-11-02T15:00:00</published>',
])
def test_rejects_date_only_updated_or_unzoned_and_skips_earnings_previews(apple_time):
    sources = {FEEDS["AAPL"][1]: apple_feed(timestamp=apple_time),
               FEEDS["NVDA"][1]: nvidia_feed(title="NVIDIA Sets Conference Call for Third-Quarter Financial Results"),
               APPLE: '<meta property="article:modified_time" content="2026-11-02T21:30:00Z">'}
    events, statuses = collect_issuer_earnings(now=NOW, fetch=public_fetch(sources))
    assert events == []
    assert [r["status"] for r in statuses] == ["partial", "ok", "ok"]


def test_rejects_untrusted_links_and_future_releases_without_fetching():
    urls = {FEEDS["AAPL"][1]: apple_feed(url="https://www.apple.com@attacker.invalid/newsroom/2026/11/results/"),
            FEEDS["NVDA"][1]: nvidia_feed(stamp="Fri, 20 Nov 2026 20:30:00 GMT")}
    events, statuses = collect_issuer_earnings(now=NOW, fetch=public_fetch(urls))
    assert not events
    assert [r["status"] for r in statuses] == ["partial", "ok", "ok"]


def test_source_failure_isolated_and_facts_survive_unrecognized_figures():
    urls = {FEEDS["AAPL"][1]: "<html>", FEEDS["NVDA"][1]: nvidia_feed(), NVIDIA: "Partial release"}
    events, statuses = collect_issuer_earnings(now=NOW, fetch=public_fetch(urls))
    assert [r["status"] for r in statuses] == ["error", "ok", "ok"]
    assert events[0]["figures"] == [] and events[0]["period"] is None


def test_dedup_preserves_both_publication_times_and_distinguishes_amendments_and_periods():
    urls = {FEEDS["AAPL"][1]: apple_feed(timestamp=f"<published>{APPLE_TIME}</published>"),
            FEEDS["NVDA"][1]: "<rss/>", APPLE: APPLE_TEXT}
    issuer = collect_issuer_earnings(now=NOW, fetch=public_fetch(urls))[0][0]
    filing = dict(issuer, source="SEC EDGAR", id="sec-id", source_url="https://www.sec.gov/Archives/report",
                  form="8-K", published_at="2026-11-02T20:35:00Z", figures=[], accession="000", fiscal_year_end="0930")
    amendment = dict(filing, id="amendment", form="8-K/A")
    result = merge_issuer_earnings([filing, amendment], [issuer])
    assert [e["id"] for e in result] == ["amendment", issuer["id"]]
    event = result[1]
    assert event["published_at"] == APPLE_TIME and event["accession"] == "000"
    assert event["source_links"][1]["published_at"] == filing["published_at"]
    assert event["source_links"][1]["source_url"] == filing["source_url"]
    assert event["fiscal_year_end"] == "0930"
    assert event["supersedes_ids"] == ["sec-id"]
    for distinct in (dict(filing, period=None), dict(filing, period="2026-08-30"),
                     dict(filing, published_at="2026-11-03T16:35:00Z")):
        assert len(merge_issuer_earnings([distinct], [issuer])) == 2
    assert len(merge_issuer_earnings([filing, dict(filing, id="other")], [issuer])) == 3


def test_amd_covers_only_reported_gaap_quarter_with_explicit_table_basis():
    url = "https://ir.amd.com/news-events/press-releases/detail/123/amd-reports-third-quarter"
    sources = {FEEDS["AAPL"][1]: "<feed/>", FEEDS["NVDA"][1]: "<rss/>",
               FEEDS["AMD"][1]: nvidia_feed(title="AMD Reports Third Quarter 2026 Financial Results",
                                            url=url, stamp="Tue, 03 Nov 26 16:15:00 -0500"),
               url: ("AMD announced financial results for the third quarter of 2026. Third quarter "
                     "revenue was $10.3 billion, operating income was $2.8 billion and diluted earnings "
                     "per share was $1.04. On a non-GAAP basis diluted earnings per share was $2.12."
                     "<table><tr><th>Three Months Ended</th></tr><tr><td>September 26, 2026</td>"
                     "<td>June 27, 2026</td></tr></table>")}
    events, _ = collect_issuer_earnings(now=NOW, fetch=public_fetch(sources))
    assert len(events) == 1
    event = events[0]
    assert event["period"] == "2026-09-26" and event["published_at"] == "2026-11-03T21:15:00Z"
    assert [f["actual"] for f in event["figures"]] == [10_300_000_000, 1.04]
