"""Timestamped public releases for the read-only market events panel."""

from __future__ import annotations

import hashlib
import json
import os
import re
import time
from threading import Lock
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from fractions import Fraction
from functools import lru_cache
from html.parser import HTMLParser
from http.client import HTTPException
from urllib.parse import urljoin, urlsplit
from urllib.request import HTTPRedirectHandler, Request, build_opener
from xml.etree import ElementTree
from zoneinfo import ZoneInfo


EASTERN = ZoneInfo("America/New_York")
UTC = timezone.utc
CPI_URL = "https://www.bls.gov/news.release/cpi.nr0.htm"
BEA_URL = "https://www.bea.gov/news/current-releases"
FED_POLICY = "https://www.federalreserve.gov/feeds/press_monetary.xml"
FED_COMMENTARY = "https://www.federalreserve.gov/feeds/speeches_and_testimony.xml"
SEC_API = "https://data.sec.gov/submissions/"
TECH_CIKS = {
    "AAPL": 320193, "MSFT": 789019, "NVDA": 1045810, "GOOGL": 1652044,
    "AMZN": 1018724, "META": 1326801, "TSLA": 1318605, "AVGO": 1730168,
    "AMD": 2488, "ORCL": 1341439, "NFLX": 1065280, "INTC": 50863,
}
_MONTH = r"(?:January|February|March|April|May|June|July|August|September|October|November|December)"
_DATE = rf"{_MONTH}\s+\d{{1,2}},\s+\d{{4}}"
_OFFICIAL_HOSTS = {
    "www.bls.gov", "www.bea.gov", "bea.gov", "www.federalreserve.gov", "federalreserve.gov",
    "www.sec.gov", "data.sec.gov",
}


def _iso(when):
    return when.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _allowed(url, hosts):
    try:
        parts = urlsplit(url)
        return (parts.scheme == "https" and parts.hostname in hosts and not parts.username
                and not parts.password and parts.port in (None, 443))
    except ValueError:
        return False


class _OfficialRedirects(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        if not _allowed(newurl, {urlsplit(req.full_url).hostname}):
            raise ValueError("Unrecognized source redirect")
        return super().redirect_request(req, fp, code, msg, headers, newurl)


def _fetch(url, user_agent, *, allowed_hosts=None):
    if not _allowed(url, _OFFICIAL_HOSTS if allowed_hosts is None else allowed_hosts):
        raise ValueError("Unrecognized release host")
    request = Request(url, headers={"User-Agent": user_agent, "Accept": "*/*"})
    payload = bytearray()
    deadline = time.monotonic() + 12
    with build_opener(_OfficialRedirects()).open(request, timeout=5) as response:
        while chunk := response.read1(65_536):
            if time.monotonic() > deadline or len(payload) + len(chunk) > 4_000_000:
                raise ValueError("Release exceeded collection limits")
            payload.extend(chunk)
    return payload.decode("utf-8-sig", errors="replace")


def _space(value):
    return " ".join(value.split())


class _Page(HTMLParser):
    """Release text, links, and table cells with their source order intact."""
    def __init__(self, markup):
        super().__init__(convert_charrefs=True)
        self._parts = []
        self.links = []
        self.paragraphs = []
        self._paragraph = None
        self.rows = []
        self._row = self._cell = self._anchor = None
        self._hidden = 0
        self.feed(markup)
        self.close()
        self.text = _space(" ".join(self._parts))

    def handle_starttag(self, tag, attrs):
        if tag in ("script", "style"):
            self._hidden += 1
        if self._hidden:
            return
        if tag == "p":
            self._paragraph = []
        if tag == "tr":
            self._row = []
        if tag in ("th", "td") and self._row is not None:
            self._cell = []
        if tag == "a":
            self._anchor = (dict(attrs).get("href", ""), [])

    def handle_endtag(self, tag):
        if tag in ("script", "style"):
            self._hidden = max(0, self._hidden - 1)
        if self._hidden:
            return
        if tag == "p" and self._paragraph is not None:
            self.paragraphs.append(_space(" ".join(self._paragraph)))
            self._paragraph = None
        if tag == "a" and self._anchor is not None:
            url, parts = self._anchor
            self.links.append((url, _space(" ".join(parts))))
            self._anchor = None
        if tag in ("th", "td") and self._cell is not None:
            self._row.append(_space(" ".join(self._cell)))
            self._cell = None
        if tag == "tr" and self._row is not None:
            self.rows.append(self._row)
            self._row = self._cell = None

    def handle_data(self, value):
        if self._hidden:
            return
        self._parts.append(value)
        if self._paragraph is not None:
            self._paragraph.append(value)
        if self._anchor is not None:
            self._anchor[1].append(value)
        if self._cell is not None:
            self._cell.append(value)


def _embargo(text):
    # Both publishers explicitly label their embargo in Eastern time.
    match = re.search(
        rf"embargoed until\s+(?:release at\s+)?(\d{{1,2}}:\d\d)\s*([ap])\.?m\.?\s*"
        rf"\(?(?:ET|EDT|EST)\)?,?\s*(?:[A-Za-z]+,\s*)?({_DATE})", text, re.I)
    if not match:
        raise ValueError("Official publication timestamp unavailable")
    clock, ampm, date = match.groups()
    return datetime.strptime(f"{date} {clock} {ampm}M", "%B %d, %Y %I:%M %p").replace(tzinfo=EASTERN)


def _figure(metric, value, unit="%"):
    return {"metric": metric, "actual": value, "unit": unit, "expected": None}


def _event(category, source, url, published, headline, facts, *, period=None, ticker=None,
           figures=None, uncertainty="", identity=None):
    key = identity or f"{url}|{_iso(published)}"
    return {
        "id": f"{category}-{hashlib.sha256(key.encode()).hexdigest()[:20]}",
        "category": category, "headline": headline, "source": source, "source_url": url,
        "published_at": _iso(published), "facts": facts, "figures": figures or [],
        "uncertainty": uncertainty, "ticker": ticker, "period": period,
    }


def _cpi(get):
    page = _Page(get(CPI_URL))
    published = _embargo(page.text)
    match = re.search(rf"CONSUMER PRICE INDEX\s*[-–—]\s*({_MONTH}\s+\d{{4}})", page.text, re.I)
    if not match:
        raise ValueError("CPI reference month unavailable")
    period = datetime.strptime(match[1], "%B %Y")
    # Table A always labels its two bases. Refuse other tables or layout changes.
    latest_month = period.strftime("%b").lower()
    figures = []
    for i, header in enumerate(page.rows):
        header_text = re.sub(r"[\s-]", "", " ".join(header).lower())
        if "seasonallyadjustedchangesfromprecedingmonth" not in header_text or "12mos" not in header_text:
            continue
        months = [cell for cell in page.rows[i + 1] if cell.strip()] if i + 1 < len(page.rows) else []
        if not months or latest_month not in months[-1].lower() or str(period.year) not in months[-1]:
            continue
        for row in page.rows[i + 2:]:
            if len(row) != len(months) + 2:
                break
            label = {"all items": "Headline", "all items less food and energy": "Core"}.get(row[0].lower())
            if label:
                for basis, cell in zip(("MoM", "YoY"), row[-2:]):
                    if re.fullmatch(r"[-+]?\d+(?:\.\d+)?", cell.strip()):
                        figures.append(_figure(f"{label} CPI {basis}", float(cell)))
            if label == "Core":
                break
        break
    # A publisher can suppress individual Table A columns. Narrow prose fallbacks
    # accept only current-month, explicitly identified changes.
    movement = r"(increased|rose|advanced|decreased|declined|fell)\s+(\d+(?:\.\d+)?)\s+percent"
    readable = {f["metric"] for f in figures}
    for label, phrase in (
            ("Headline", rf"Consumer Price Index for All Urban Consumers \(CPI-U\) {movement} on a seasonally adjusted basis in {period:%B}"),
            ("Core", rf"index for all items less food and energy {movement} in {period:%B}")):
        metric = f"{label} CPI MoM"
        number = re.search(phrase, page.text, re.I)
        if metric not in readable and number:
            sign = -1 if number[1].lower() in {"decreased", "declined", "fell"} else 1
            figures.append(_figure(metric, sign * float(number[2])))
    archive = f"https://www.bls.gov/news.release/archives/cpi_{published:%m%d%Y}.htm"
    try:
        # The embargo notice and reference month precede the historical tables,
        # which can exceed a megabyte in a single release.
        archived = _Page(get(archive)[:160_000])
        has_copy = (_embargo(archived.text) == published and match[0].lower() in archived.text.lower())
    except (OSError, ValueError, HTTPException):
        has_copy = False
    url = archive if has_copy else CPI_URL
    uncertainty = "Unreported or unrecognized figures are omitted."
    if "cannot publish 12-month percent changes" in page.text.lower():
        uncertainty += " BLS reported missing year-earlier observations for some 12-month changes."
    if not has_copy:
        uncertainty += " The date-specific archive is unverified. The BLS summary URL rolls to the latest release."
    return [_event("cpi", "BLS", url, published, f"Consumer Price Index · {period:%B %Y}",
                   "BLS published the national CPI-U release. Monthly changes are seasonally adjusted; annual changes are unadjusted.",
                   period=period.strftime("%Y-%m"), figures=figures,
                   uncertainty=uncertainty, identity=f"CPI-U|{period:%Y-%m}|{_iso(published)}")]


def _pce(get):
    listing = _Page(get(BEA_URL))
    for href, title in listing.links:
        match = re.fullmatch(rf"Personal Income and Outlays,?\s+({_MONTH}\s+\d{{4}})", title, re.I)
        url = urljoin(BEA_URL, href)
        if match and _allowed(url, {"www.bea.gov", "bea.gov"}) and urlsplit(url).path.startswith("/news/"):
            break
    else:
        raise ValueError("Monthly BEA release link unavailable")
    period = datetime.strptime(match[1], "%B %Y")
    page = _Page(get(url))
    if not re.search(rf"Personal Income and Outlays,?\s+{re.escape(match[1])}\b", page.text, re.I):
        raise ValueError("BEA reference period mismatch")
    figures = []
    # CPI/PCE growth is a percentage, not an index level. A rounded value
    # described as "less than" is never converted to a point estimate.
    movement = (r"(?:(increased|rose|advanced|decreased|declined|fell)\s+(\d+(?:\.\d+)?)\s+percent"
                r"|(was unchanged|remained unchanged|showed no change))")
    for basis, prefix in (("MoM", "From the preceding month"), ("YoY", "From the same month one year ago")):
        first = re.search(rf"{prefix},?\s+the PCE price index(?: for {period:%B})?\s+{movement}\b",
                          page.text, re.I)
        if first:
            values = [("Headline", first)]
            core = re.match(rf"\s*\.\s*Excluding food and energy,?\s+the PCE price index\s+{movement}\b",
                            page.text[first.end():], re.I)
            if core:
                values.append(("Core", core))
            for label, item in values:
                sign = -1 if (item[1] or "").lower() in ("decreased", "declined", "fell") else 1
                figures.append(_figure(f"{label} PCE {basis}", sign * float(item[2] or "0")))
    return [_event("pce", "BEA", url, _embargo(page.text), f"PCE price index · {period:%B %Y}",
                   "BEA published Personal Income and Outlays. The figures below measure prices, excluding changes in spending volume.",
                   period=period.strftime("%Y-%m"), figures=figures,
                   uncertainty="BEA can revise prior estimates. Missing figures could not be verified from the release.")]


def _timestamp(value):
    if not value:
        raise ValueError("Missing publication time")
    try:
        result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        result = parsedate_to_datetime(value)
    if result is None or result.utcoffset() is None:
        raise ValueError("Source time zone unavailable")
    return result.astimezone(UTC)


def _rate(token):
    parts = re.split(r"[- ]", token)
    return float(sum(Fraction(part) for part in parts))


def _policy_decision(markup):
    text = _Page(markup).text.replace("‑", "-").replace("–", "-")
    rate = r"(\d+(?:\.\d+|(?:[- ]\d+)?/\d+)?)"
    match = re.search(
        r"the Committee decided to (lower|raise|maintain|keep) the target range for the federal funds rate "
        rf"(?:by [^.]+? percentage points? )?(?:at|to) {rate} to {rate} percent", text, re.I)
    if not match:
        return None, []
    lower, upper = map(_rate, match.groups()[1:])
    if not 0 <= lower <= upper <= 100:
        return None, []
    action = {"lower": "lowered", "raise": "raised", "maintain": "maintained", "keep": "kept"}[match[1].lower()]
    preposition = "to" if action in ("lowered", "raised") else "at"
    summary = f"The FOMC {action} its federal funds target range {preposition} {lower:g}% to {upper:g}%."
    return summary, [_figure("Fed funds target lower", lower), _figure("Fed funds target upper", upper)]


def _speech_quote(markup):
    # Choose a complete sentence, preserving any qualifying clause and avoiding
    # other speakers' quotations, citations and footnotes.
    fallback = None
    for paragraph in _Page(markup).paragraphs:
        if re.search(r"Return to text|^\d+\.|”|“|\"", paragraph):
            continue
        for sentence in re.split(r"(?<=[.!?])\s+(?=[A-Z])", paragraph):
            if not 6 <= len(sentence.split()) <= 25 or not sentence.endswith((".", "!")):
                continue
            if not re.search(r"\b(?:policy|rates?|inflation|tightening|easing)\b", sentence, re.I):
                continue
            if re.search(r"\bI (?:believe|favor|expect|support(?:ed)?|voted|would|judge|think)\b", sentence, re.I):
                return sentence
            if not fallback and re.search(r"^(?:If|Should) .+\b(?:warranted|appropriate|necessary|prudent)\b", sentence):
                fallback = sentence
    return fallback


def _fed(get, feed, category, now):
    xml = ElementTree.fromstring(get(feed))
    if xml.tag.lower().split("}")[-1] not in ("rdf", "rss"):
        raise ValueError("Unrecognized Federal Reserve feed")
    items = xml.findall(".//{*}item")
    if not items:
        raise ValueError("Federal Reserve feed contained no release items")
    events = []
    details_read = 0
    has_valid_item = False
    for item in items[:30]:
        fields = {node.tag.split("}")[-1]: (node.text or "").strip() for node in item}
        title, url = fields.get("title", ""), fields.get("link", "")
        if not title or not _allowed(url, {"www.federalreserve.gov", "federalreserve.gov"}):
            continue
        try:
            published = _timestamp(fields.get("pubDate") or fields.get("date"))
        except (ValueError, TypeError, OverflowError):
            continue
        has_valid_item = True
        if not now - timedelta(days=100) <= published <= now:
            continue
        description = _Page(fields.get("description", "")).text
        figures = []
        uncertainty = "Publication confirms the item. Its effect on index prices is uncertain."
        if (category == "fed_commentary" and details_read < 3 and published >= now - timedelta(days=14)
                and urlsplit(url).path.startswith(("/newsevents/speech/", "/newsevents/testimony/"))):
            details_read += 1
            try:
                quote = _speech_quote(get(url))
            except (OSError, ValueError, HTTPException):
                quote = None
            if quote:
                description = f'{description or title}. Published remarks: “{quote}”'
                uncertainty = "The quotation is one speaker's view. RSS time can differ from delivery time."
            else:
                uncertainty = "No verified policy quotation was extracted. RSS time can differ from delivery time."
        if category == "fed_policy" and "issues fomc statement" in title.lower():
            try:
                decision, figures = _policy_decision(get(url))
                if decision:
                    description = decision
            except (OSError, ValueError, HTTPException):
                pass
            if not figures:
                uncertainty = "The policy statement is published. Its target range could not be verified automatically."
        events.append(_event(category, "Federal Reserve Board", url, published, title,
                             description[:600] or title, period=published.astimezone(EASTERN).date().isoformat(),
                             figures=figures, uncertainty=uncertainty, identity=url))
    if not has_valid_item:
        raise ValueError("No trusted Federal Reserve links with verified publication timestamps")
    return events


def _sec_submissions(get, ticker, cik, now):
    data = json.loads(get(f"{SEC_API}CIK{cik:010d}.json"))
    if int(data["cik"]) != cik:
        raise ValueError("SEC issuer mismatch")
    recent = data["filings"]["recent"]
    events = []
    for form, items, accession, accepted in zip(*(recent[key] for key in
            ("form", "items", "accessionNumber", "acceptanceDateTime")), strict=True):
        if form not in ("8-K", "8-K/A") or "2.02" not in re.findall(r"\b\d\.\d{2}\b", items or ""):
            continue
        published = _timestamp(accepted)
        if not now - timedelta(days=100) <= published <= now:
            continue
        if not re.fullmatch(r"\d{10}-\d{2}-\d{6}", accession):
            continue
        folder = f"https://www.sec.gov/Archives/edgar/data/{cik}/{accession.replace('-', '')}/"
        url = f"{folder}{accession}-index.html"
        event = _event("earnings", "SEC EDGAR", url, published,
                       f"{ticker} filed {form} results disclosure", f"EDGAR accepted a {form} under Item 2.02 (results of operations and financial condition).",
                       ticker=ticker, identity=accession,
                       uncertainty="Time is SEC acceptance. The issuer may have announced earlier. The filing alone does not establish a quarterly beat or miss.")
        event["accession"] = accession
        event["form"] = form
        fiscal_year_end = data.get("fiscalYearEnd")
        if isinstance(fiscal_year_end, str) and re.fullmatch(r"\d{4}", fiscal_year_end):
            try:
                datetime.strptime(f"2000{fiscal_year_end}", "%Y%m%d")
                event["fiscal_year_end"] = fiscal_year_end
            except ValueError:
                pass
        events.append(event)
    return events


_SEC_CLOCK = Lock()
_sec_next_request = 0.0


def _sec_fetch(url, agent):
    # Pace all local collectors, including parallel issuer/detail reads.
    global _sec_next_request
    with _SEC_CLOCK:
        delay = _sec_next_request - time.monotonic()
        if delay > 0:
            time.sleep(delay)
        _sec_next_request = time.monotonic() + 0.18
    return _fetch(url, agent)


@lru_cache(maxsize=128)
def _filed_details(url, agent):
    # EDGAR accessions encode immutable versions. Cache the extracted evidence,
    # not megabytes of HTML per issuer.
    return _filing_details(url, lambda address: _sec_fetch(address, agent))


def _filing_details(url, get):
    index = _Page(get(url))
    for row in index.rows:
        if len(row) < 4 or row[3].upper() != "EX-99.1":
            continue
        href = next((link for link, label in index.links if label == row[2] and label), None)
        if not href:
            continue
        exhibit = urljoin(url, href)
        if not _allowed(exhibit, {"www.sec.gov"}) or not exhibit.startswith(url.rsplit("/", 1)[0] + "/"):
            continue
        if not urlsplit(exhibit).path.lower().endswith((".htm", ".html")):
            continue
        page = _Page(get(exhibit))
        details = {"exhibit_url": exhibit}
        # Only extract explicitly quarterly issuer figures. Do not infer a fiscal
        # period from the filing acceptance date or annual/YTD financial tables.
        ended = re.search(rf"\bquarter ended\s+({_DATE})\b", page.text[:5000], re.I)
        if not ended:
            return details
        details.update(period=datetime.strptime(ended[1], "%B %d, %Y").date().isoformat(), figures=[])
        # These labels cover explicitly scoped AAPL/NVDA release prose; other
        # quarterly metrics are left to authenticated, period-matched providers.
        revenue = re.search(r"\bquarterly revenue of\s*\$\s*([\d,.]+)\s+(million|billion)\b",
                            page.text[:2200], re.I)
        if revenue:
            scale = 1_000_000 if revenue[2].lower() == "million" else 1_000_000_000
            details["figures"].append(_figure("Quarterly revenue", float(revenue[1].replace(",", "")) * scale, "USD"))
        eps = re.search(r"\bFor the quarter, GAAP earnings per diluted share (?:were|was)\s*\$\s*(\d+\.\d+)\b",
                        page.text[:3500], re.I)
        if eps:
            details["figures"].append(_figure("Quarterly diluted EPS (GAAP)", float(eps[1]), "USD/share"))
        for figure in details["figures"]:
            figure["source_url"] = exhibit
        return details
    return {}


@lru_cache(maxsize=12)
def _dated_publication(url, half_hour):
    return _fetch(url, "MarketPinPredictor/1.0")


def collect_market_events(*, sec_user_agent=None, now=None, fetch=None):
    """Poll official sources independently. No credentials, samples or stale prices are fabricated.

    `fetch(url, user_agent) -> str` is injectable for offline verification.
    The caller owns scheduling and cross-poll deduplication.
    """
    now = now or datetime.now(UTC)
    if now.utcoffset() is None:
        raise ValueError("now must include a time zone")
    sec_agent = (sec_user_agent or os.getenv("SEC_USER_AGENT", "")).strip()
    transport = fetch or _fetch
    def get(url):
        if fetch or url in (CPI_URL, BEA_URL, FED_POLICY, FED_COMMENTARY):
            return transport(url, "MarketPinPredictor/1.0")
        return _dated_publication(url, int(now.timestamp()) // 1800)

    tasks = [
        ("bls_cpi", "BLS CPI", CPI_URL, lambda: _cpi(get)),
        ("bea_pce", "BEA PCE", BEA_URL, lambda: _pce(get)),
        ("fed_policy", "Federal Reserve monetary policy", FED_POLICY, lambda: _fed(get, FED_POLICY, "fed_policy", now)),
        ("fed_commentary", "Federal Reserve speeches and testimony", FED_COMMENTARY,
         lambda: _fed(get, FED_COMMENTARY, "fed_commentary", now)),
    ]

    def collect(task):
        key, name, url, read = task
        row = {"id": key, "name": name, "source_url": url, "checked_at": _iso(now),
               "status": "ok", "detail": "Checked public source"}
        try:
            events = [e for e in read() if _timestamp(e["published_at"]) <= now]
            if key in ("bls_cpi", "bea_pce") and events and not events[0]["figures"]:
                row.update(status="partial", detail="Release verified; price figures unavailable")
        except (OSError, HTTPException, ValueError, TypeError, KeyError, IndexError, ElementTree.ParseError) as exc:
            # HTTP errors can embed URLs or contact data. Report only the failure class/code.
            code = getattr(exc, "code", None)
            row.update(status="error", detail=f"Source unavailable or changed format ({'HTTP ' + str(code) if code else type(exc).__name__})")
            events = []
        return events, row

    configured = bool(re.search(r"[^\s@]+@[^\s@]+\.[^\s@]+", sec_agent)
                      and "\n" not in sec_agent and "\r" not in sec_agent)
    if configured:
        def issuer(ticker, cik):
            events = _sec_submissions(lambda url: (fetch or _sec_fetch)(url, sec_agent), ticker, cik, now)
            for event in events:
                try:
                    url = event["source_url"]
                    details = (_filing_details(url, lambda u: fetch(u, sec_agent))
                               if fetch else _filed_details(url, sec_agent))
                    event.update(details)
                    if details.get("period"):
                        event["facts"] += f" Exhibit 99.1 identifies the quarter ended {details['period']}."
                        event["headline"] = f"{ticker} {event['form']} results disclosure · quarter ended {details['period']}"
                except (OSError, HTTPException, ValueError, TypeError, IndexError):
                    event["exhibit_status"] = "error"
                    event["uncertainty"] += " Exhibit retrieval failed."
            return events

        for ticker, cik in TECH_CIKS.items():
            tasks.append((f"sec_{ticker}", f"SEC EDGAR · {ticker}", f"{SEC_API}CIK{cik:010d}.json",
                          lambda t=ticker, c=cik: issuer(t, c)))
    with ThreadPoolExecutor(max_workers=6) as executor:
        results = list(executor.map(collect, tasks))
    for batch, row in results:
        if any(e.get("exhibit_status") == "error" for e in batch):
            row.update(status="partial", detail="Filing verified; exhibit unavailable")
    if not configured:
        results.append(([], {"id": "sec", "name": "SEC EDGAR earnings", "source_url": SEC_API,
                             "checked_at": _iso(now), "status": "unconfigured",
                             "detail": "Set SEC_USER_AGENT to an application name and contact email."}))
    events = [e for batch, _ in results for e in batch]
    events.sort(key=lambda e: (e["published_at"], e["id"]), reverse=True)
    return {"events": events, "sources": [row for _, row in results], "fetched_at": _iso(now)}
