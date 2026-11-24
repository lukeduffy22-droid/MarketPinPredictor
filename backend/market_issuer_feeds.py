"""No-key issuer releases from publisher-operated RSS and Atom feeds."""

from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
import json
import re
from urllib.parse import urlsplit
from xml.etree import ElementTree

from backend.market_event_sources import (
    EASTERN, UTC, _DATE, _Page, _allowed, _event, _fetch, _figure, _iso, _timestamp,
)

# NVIDIA publishes its press-release feed in https://nvidianews.nvidia.com/rss.
FEEDS = {
    "AAPL": ("Apple Newsroom", "https://www.apple.com/newsroom/rss-feed.rss"),
    "NVDA": ("NVIDIA Newsroom", "https://nvidianews.nvidia.com/cats/press_release.xml"),
    "AMD": ("AMD Investor Relations", "https://ir.amd.com/news-events/press-releases/rss"),
}
QUARTER = r"(?:first|second|third|fourth)"
TITLES = {
    "AMD": rf"AMD Reports {QUARTER} Quarter(?: and Full Year)? \d{{4}} Financial Results",
    "AAPL": rf"Apple reports {QUARTER} quarter results",
    "NVDA": rf"NVIDIA Announces Financial Results for {QUARTER} Quarter(?: and Fiscal Year)? Fiscal \d{{4}}"
            rf"|NVIDIA Announces Financial Results for {QUARTER} Quarter and Fiscal \d{{4}}",
}
RELEASE_PATHS = {
    "AAPL": r"/newsroom/\d{4}/\d{2}/[a-z0-9-]+/?",
    "AMD": r"/news-events/press-releases/detail/\d+/[a-z0-9-]+/?",
    "NVDA": r"/news/[a-z0-9-]+/?",
}
MAX_AGE = timedelta(days=100)


class _Article(_Page):
    """Read publication metadata, independently from last-modified time."""
    def __init__(self, markup):
        self.published = []
        self._ld = False
        super().__init__(markup)

    def handle_starttag(self, tag, attrs):
        values = dict(attrs)
        if tag == "meta" and (values.get("property") == "article:published_time"
                              or values.get("itemprop") == "datePublished"):
            self.published.append(values.get("content") or "")
        if tag == "script":
            self._ld = values.get("type", "").lower() == "application/ld+json"
        super().handle_starttag(tag, attrs)

    def handle_data(self, value):
        if self._ld:
            try:
                data = json.loads(value)
            except ValueError:
                data = {}
            nodes = data if isinstance(data, list) else [data]
            for node in nodes:
                if isinstance(node, dict) and isinstance(node.get("@graph"), list):
                    nodes_to_read = node["@graph"]
                else:
                    nodes_to_read = [node]
                for article in nodes_to_read:
                    if isinstance(article, dict) and article.get("@type") in ("NewsArticle", "Article"):
                        self.published.append(article.get("datePublished"))
        super().handle_data(value)

    def handle_endtag(self, tag):
        if tag == "script":
            self._ld = False
        super().handle_endtag(tag)


def _time(value):
    if not isinstance(value, str) or not re.search(r"\d{1,2}:\d{2}", value):
        return None
    try:
        return _timestamp(value)
    except (ValueError, TypeError, OverflowError):
        return None


def _amd_numbers(article, url):
    # AMD opens with a current-quarter GAAP sentence followed by an explicitly
    # labeled non-GAAP sentence. Annual results and guidance come later.
    match = re.search(rf"announced financial results for the ({QUARTER}) quarter of (\d{{4}})\. "
                      rf"\1 quarter revenue was \$([\d,.]+) (billion|million), (?:[^.]|\.(?=\d)){{0,400}}?"
                      r"diluted earnings per share was \$(\d+\.\d+)", article.text[:6000], re.I)
    if not match:
        return None, []
    period = None
    for i, row in enumerate(article.rows[:-1]):
        if [c.lower() for c in row if c] != ["three months ended"]:
            continue
        first_date = next((cell for cell in article.rows[i + 1] if cell), "")
        if re.fullmatch(_DATE, first_date) and first_date.endswith(match[2]):
            period = datetime.strptime(first_date, "%B %d, %Y").date().isoformat()
            break
    scale = 1_000_000_000 if match[4].lower() == "billion" else 1_000_000
    figures = [_figure("Quarterly revenue", float(match[3].replace(",", "")) * scale, "USD"),
               _figure("Quarterly diluted EPS (GAAP)", float(match[5]), "USD/share")]
    return period, [dict(figure, source_url=url) for figure in figures]


def _quarter_numbers(ticker, article, url):
    if ticker == "AMD":
        return _amd_numbers(article, url)
    text = article.text
    period_match = re.search(rf"\bquarter ended\s+({_DATE})\b", text, re.I)
    if not period_match:
        return None, []
    period = datetime.strptime(period_match[1].title(), "%B %d, %Y").date().isoformat()
    # Limit the scope to the issuer's current-quarter opening statement, before
    # tables, annual results, forward guidance, or segment-specific commentary.
    paragraph = text[period_match.start():period_match.end() + 750]
    dollars = r"\$\s*([\d,]+(?:\.\d+)?)\s+(million|billion)\b"
    revenue = (re.search(r"\b(?:total company |quarterly )revenue of\s*" + dollars, paragraph, re.I)
               if ticker == "AAPL" else re.match(rf"quarter ended\s+{_DATE},?\s+of\s*" + dollars, paragraph, re.I))
    figures = []
    if revenue:
        units = {"million": 1_000_000, "billion": 1_000_000_000}
        figures.append(_figure("Quarterly revenue", float(revenue[1].replace(",", "")) * units[revenue[2].lower()], "USD"))
    eps = (re.search(r"(?:quarterly )?(?:diluted earnings per share|diluted EPS)\s+(?:was|were|of)\s*\$([\d]+\.\d+)", paragraph, re.I)
           if ticker == "AAPL" else re.search(r"\bGAAP and non-GAAP earnings per diluted share were \$([\d]+\.\d+) and \$[\d]+\.\d+, respectively", paragraph, re.I))
    if eps:
        figures.append(_figure("Quarterly diluted EPS (GAAP)", float(eps[1]), "USD/share"))
    for figure in figures:
        figure["source_url"] = url
    return period, figures


class _MissingReleaseTime(ValueError):
    def __init__(self, title, url):
        self.title = title
        self.url = url


def _issuer(ticker, now, get):
    name, feed = FEEDS[ticker]
    host = urlsplit(feed).hostname
    root = ElementTree.fromstring(get(feed, host))
    protocol = root.tag.split("}")[-1].lower()
    if protocol not in ("feed", "rss", "rdf"):
        raise ValueError("Unrecognized feed")
    tag = "entry" if protocol == "feed" else "item"
    skipped = 0
    for entry in root.findall(".//{*}" + tag)[:60]:
        title = " ".join((entry.findtext("{*}title") or "").split())
        if not re.fullmatch(TITLES[ticker], title, re.I):
            continue
        if protocol == "feed":
            url = next((link.get("href") for link in entry.findall("{*}link")
                        if link.get("rel", "alternate") == "alternate"), "")
            stamp = _time(entry.findtext("{*}published"))
        else:
            url = entry.findtext("{*}link") or ""
            stamp = _time(entry.findtext("{*}pubDate") or entry.findtext("{*}date"))
        if not url or not _allowed(url, {host}):
            skipped += 1
            continue
        path = urlsplit(url).path
        if not re.fullmatch(RELEASE_PATHS[ticker], path):
            skipped += 1
            continue
        if stamp and not now - MAX_AGE <= stamp <= now:
            continue
        url = f"https://{host}{path}"
        article = _Article(get(url, host))
        # Feeds may include only updated times. Never use an update or HTTP
        # Last-Modified timestamp in a window advertised as a release reaction.
        stamps = {candidate for raw in article.published if (candidate := _time(raw))}
        if stamp is None and len(stamps) == 1:
            stamp = stamps.pop()
        if not stamp:
            raise _MissingReleaseTime(title, url)
        if not now - MAX_AGE <= stamp <= now:
            break
        period, figures = _quarter_numbers(ticker, article, url)
        facts = f"{name} published its quarterly results."
        if period:
            facts += f" The reported quarter ended {period}."
        event = _event("earnings", name, url, stamp, title, facts, ticker=ticker, period=period,
                       figures=figures, identity=url.rstrip("/"),
                       uncertainty="Time is the issuer's published release time. Unverified figures are omitted.")
        event["issuer_release"] = True
        event["source_links"] = [_source_link(event)]
        return [event], skipped
    return [], skipped


def collect_issuer_earnings(*, now=None, fetch=None):
    """Read the issuer feeds in parallel and at most one eligible release per feed."""
    now = now or datetime.now(UTC)
    if now.utcoffset() is None:
        raise ValueError("A timezone-aware timestamp is required")

    def get(url, host):
        if fetch is not None:
            return fetch(url)
        return _fetch(url, "MarketPinPredictor/1.0", allowed_hosts={host})

    def collect(ticker):
        name, feed = FEEDS[ticker]
        row = {"id": f"issuer_{ticker}", "name": name, "source_url": feed, "checked_at": _iso(now)}
        try:
            events, skipped = _issuer(ticker, now, get)
            row.update(status="partial" if skipped else "ok",
                       detail=("A matching item lacked a verified publication time or source URL." if skipped else
                               f"{len(events)} recent quarterly results releases in the current feed."))
        except _MissingReleaseTime as exc:
            events = []
            row.update(status="partial", feed_url=feed, source_url=exc.url,
                       detail=f"{exc.title}. The issuer supplied no verified intraday publication time.")
        except (OSError, ValueError, TypeError, ElementTree.ParseError) as exc:
            events = []
            row.update(status="error", detail=f"Issuer feed or release unavailable ({type(exc).__name__}).")
        return events, row

    with ThreadPoolExecutor(max_workers=len(FEEDS)) as executor:
        batches = list(executor.map(collect, FEEDS))
    return [event for batch, _ in batches for event in batch], [row for _, row in batches]


def _source_link(event):
    return {key: event[key] for key in ("source", "source_url", "published_at")}


def _report_key(event):
    if event.get("category") != "earnings" or event.get("ticker") not in FEEDS:
        return None
    period = event.get("period") or ""
    stamp = _time(event.get("published_at"))
    if not stamp or not re.fullmatch(r"\d{4}-\d\d-\d\d", period):
        return None
    return event["ticker"], period, stamp.astimezone(EASTERN).date()


def merge_issuer_earnings(sec_events, issuer_events):
    """Link a unique same-day original filing only for the identical reported period."""
    filings, releases = defaultdict(list), defaultdict(list)
    for event in sec_events:
        key = _report_key(event)
        if key and event.get("source") == "SEC EDGAR" and event.get("form") == "8-K":
            filings[key].append(event)
    for event in issuer_events:
        key = _report_key(event)
        if key:
            releases[key].append(event)
    skipped, merged = set(), []
    for issuer in issuer_events:
        key = _report_key(issuer)
        if not key or len(filings[key]) != 1 or len(releases[key]) != 1:
            merged.append(issuer)
            continue
        filing = filings[key][0]
        skipped.add(filing["id"])
        metrics = {(f["metric"], f["unit"]) for f in issuer["figures"]}
        extra = [f for f in filing["figures"] if (f["metric"], f["unit"]) not in metrics]
        metadata = {k: filing[k] for k in ("accession", "form", "exhibit_url", "fiscal_year_end") if k in filing}
        merged.append(dict(issuer, **metadata, figures=issuer["figures"] + extra,
                           facts=issuer["facts"] + " EDGAR also accepted an Item 2.02 filing for this quarter.",
                           source_links=[_source_link(issuer), _source_link(filing)],
                           supersedes_ids=[filing["id"]]))
    return [event for event in sec_events if event["id"] not in skipped] + merged
