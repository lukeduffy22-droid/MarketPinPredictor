"""Read the local event monitor without opening upstream connections from Streamlit."""
from __future__ import annotations

import math
from datetime import datetime, timedelta, timezone
from typing import Any
from urllib.parse import urlsplit

import requests

from app.services.forecast_research_view import central_timestamp
from app.utils.display_time import parse_utc_timestamp
from backend.market_universe import MARKET_UNIVERSE


ENDPOINT = "http://127.0.0.1:8000/market-events"
CATEGORIES = {
    "cpi": "CPI", "pce": "PCE", "fed_policy": "Fed decisions",
    "fed_commentary": "Fed commentary", "earnings": "Tech earnings",
}
POLL_STATES = {"ready", "refreshing", "initializing", "stale", "error", "disabled"}


def fetch_market_events() -> dict[str, Any]:
    response = requests.get(ENDPOINT, timeout=2)
    response.raise_for_status()
    data = response.json()
    if (not isinstance(data, dict) or data.get("schema_version") != "marketpin-market-events.v1"
            or data.get("poll_status") not in POLL_STATES):
        raise ValueError("Unsupported market-events snapshot")
    for field in ("indices", "sources", "events"):
        if not isinstance(data.get(field), list) or not all(isinstance(row, dict) for row in data[field]):
            raise ValueError(f"Invalid {field} collection")
    return data


def _link(value: Any) -> str | None:
    try:
        url = urlsplit(value) if isinstance(value, str) else None
        if url and url.scheme == "https" and url.hostname and not (url.username or url.password):
            return value
    except ValueError:
        pass
    return None


def _number(value: Any) -> int | float | None:
    if type(value) in (float, int) and math.isfinite(value):
        return value
    return None


def index_rows(records: list[dict]) -> list[dict]:
    rows = []
    for record in records:
        status = record.get("status") or "unavailable"
        basis = record.get("basis") or "unavailable"
        measured = status in {"current", "delayed", "daily_close", "stale"} and basis != "unavailable"
        stamp = central_timestamp(record.get("observed_at")) if record.get("observed_at") else record.get("session_date")
        rows.append({
            "Symbol": record.get("symbol"), "Market": record.get("name"), "Instrument": record.get("kind"), "Status": status,
            "Price basis": basis.replace("_", " "), "Level": _number(record.get("price")) if measured else None,
            "Change (%)": _number(record.get("change_pct")) if measured else None,
            "Comparison": record.get("change_basis") if measured else None, "As of": stamp or "Unavailable",
            "Source": record.get("source"), "Link": _link(record.get("source_url")), "Coverage": record.get("detail"),
        })
    return rows


def _figure_rows(event: dict) -> list[dict]:
    rows = []
    for figure in event.get("figures") or []:
        actual, expected = _number(figure.get("actual")), _number(figure.get("expected"))
        estimate_time = figure.get("expected_as_of")
        rows.append({
            "Metric": figure.get("metric"), "Period": figure.get("period") or event.get("period"),
            "Unit": figure.get("unit"), "Actual": actual, "Expected": expected,
            "Surprise": _number(figure.get("surprise")) if actual is not None and expected is not None else None,
            "Figure source": figure.get("source") or event.get("source"),
            "Actual link": _link(figure.get("source_url") or event.get("source_url")),
            "Estimate source": figure.get("expected_source"),
            "Estimate link": _link(figure.get("expected_url") or figure.get("expected_source_url")),
            "Estimate as of": central_timestamp(estimate_time) if estimate_time else figure.get("estimate_vintage"),
            "Figures retrieved": central_timestamp(figure["as_of"]) if figure.get("as_of") else None,
            "Notes": figure.get("note"),
        })
    return rows


def _table(st: Any, rows: list[dict], *links: str) -> None:
    st.dataframe(rows, hide_index=True, width="stretch", column_config={
        key: st.column_config.LinkColumn(key, display_text="Open source") for key in links
    })


def _render_release(st: Any, event: dict) -> None:
    st.subheader(event["headline"])
    sec_filing = event.get("source") == "SEC EDGAR"
    time_label = "SEC accepted" if sec_filing else "Published"
    st.caption(f"{time_label} {central_timestamp(event.get('published_at'))} · First observed "
               f"{central_timestamp(event.get('observed_at'))} · {event.get('source') or 'Source unavailable'}")
    link = _link(event.get("source_url"))
    if link:
        st.link_button("Open SEC filing" if sec_filing else "Open primary release", link)
    related = [row for row in event.get("source_links") or [] if _link(row.get("source_url")) and row["source_url"] != link]
    if related:
        _table(st, [{"Additional source": row.get("source"),
                     "Timestamp basis": "SEC acceptance" if row.get("source") == "SEC EDGAR" else "Publication",
                     "Time": central_timestamp(row.get("published_at")), "Link": _link(row["source_url"])} for row in related], "Link")
    st.markdown("**Confirmed facts**")
    st.text(event.get("facts") or "Release details unavailable.")
    figures = _figure_rows(event)
    if figures:
        _table(st, figures, "Actual link", "Estimate link")
        st.caption("Surprise is actual minus expected. Differences between percentages are percentage points. Blank cells indicate unavailable evidence.")
    st.markdown("**Market window around filing acceptance**" if sec_filing else "**Market window around publication**")
    if sec_filing:
        st.caption("The issuer may have announced earnings before SEC acceptance.")
    windows = event.get("event_window") or []
    if windows:
        rows = []
        for row in windows:
            minute_bar = row.get("basis") == "etf_minute_close"
            instrument = MARKET_UNIVERSE.get(row.get("symbol"))
            etf = minute_bar or (instrument is not None and instrument.kind == "etf")
            before = _number(row.get("before_price" if minute_bar else "before_level"))
            after = _number(row.get("after_price" if minute_bar else "after_level"))
            rows.append({"Symbol": row.get("symbol"), "Basis": row.get("basis"), "Change (%)": _number(row.get("change_pct")),
                         "Before": before, "After": after, "Unit": "USD/share" if etf else "index points",
                         "Level change": round(after - before, 4) if before is not None and after is not None else None,
                         "Source": row.get("source"), "Link": _link(row.get("source_url")), "Coverage": row.get("detail"),
                         "From": central_timestamp(row.get("before_at")), "To": central_timestamp(row.get("after_at"))})
        _table(st, rows, "Link")
        if any(row.get("basis") == "etf_minute_close" for row in windows):
            st.caption("ETF rows show the named fund’s minute-bar return around publication. Extended-hours ETF trading can precede the cash index open.")
    else:
        st.caption("No comparable observations bracket this release.")
    st.markdown("**Interpretation**")
    st.text(event.get("assessment") or "An assessment is unavailable.")
    st.markdown("**Uncertainty**")
    st.text(event.get("uncertainty") or "The release’s independent market impact is unknown.")
    if event.get("alert_reason"):
        level = "Material release" if event.get("significance") == "material_release" else "Watch"
        st.caption(f"{level} · {event['alert_reason']}")


def _notify_new_material(st: Any, events: list[dict], now: datetime) -> None:
    # Baseline the first open panel and re-opened sessions without replaying old releases.
    state_key = "market_event_notifications"
    prior = st.session_state.get(state_key)
    material = {row["id"]: row for row in events if row.get("id") and row.get("significance") == "material_release"}
    since = now
    if prior and now - prior["checked"] <= timedelta(minutes=2):
        since = prior["since"]
        for event_id in material.keys() - prior["material"]:
            event = material[event_id]
            if prior["material"].intersection(event.get("supersedes_ids") or []):
                continue
            published = parse_utc_timestamp(event.get("published_at"))
            if published is not None and (since <= published <= now or event_id in prior["watch"]):
                subject = event.get("ticker") or CATEGORIES.get(event.get("category"), "Market release")
                st.toast(f"Material update · {subject} · {central_timestamp(event.get('published_at'))}")
    st.session_state[state_key] = {"since": since, "checked": now, "material": set(material),
                                   "watch": {row.get("id") for row in events if row.get("significance") == "watch"}}


def _render_market_events() -> None:
    import streamlit as st

    st.header("Market events")
    try:
        data = fetch_market_events()
    except (requests.RequestException, ValueError, TypeError):
        st.warning("Market-events service unavailable. Event coverage and current index levels are unknown.")
        return
    poll = data["poll_status"]
    if poll == "disabled":
        st.info("Collection is disabled by MARKETPIN_MARKET_EVENTS on this workstation.")
        return
    stamp = central_timestamp(data.get("fetched_at"))
    st.caption(f"Source poll {poll} · Last collection {stamp} · Dashboard checked "
               f"{central_timestamp(datetime.now(timezone.utc).isoformat())}")
    if poll in {"stale", "error"}:
        st.warning(data.get("last_error") or "Source collection is overdue. Displayed releases may be incomplete.")
    elif poll == "initializing":
        st.info("Waiting for the first source collection.")
    sources = data["sources"]
    unconfigured, gaps = [], []
    for row in sources:
        name = row.get("name") or row.get("id") or "Unknown source"
        if row.get("status") in {"unconfigured", "not_configured", "disabled"}:
            unconfigured.append(name)
        elif row.get("status") != "ok":
            gaps.append(name)
    if gaps:
        st.warning("Incomplete source coverage · " + ", ".join(gaps))
    if unconfigured:
        st.caption("Sources awaiting configuration · " + ", ".join(unconfigured))
    with st.expander("Source coverage"):
        if sources:
            _table(st, [{"Source": row.get("name"), "Status": row.get("status"), "Last checked": central_timestamp(row.get("checked_at")),
                         "Detail": row.get("detail"), "Link": _link(row.get("source_url"))} for row in sources], "Link")
        else:
            st.caption("No source checks are available.")
        interval = data.get("poll_interval_seconds")
        if interval:
            st.caption(f"Collector interval {interval} seconds. This view reads its local snapshot every 30 seconds while open.")
    if data["indices"]:
        st.subheader("Index levels and market context")
        st.caption(f"Quotes last checked {central_timestamp(data.get('indices_checked_at'))}")
        _table(st, index_rows(data["indices"]), "Link")
        st.caption("An option-implied reference is inferred from option prices. Daily index closes use the stated session date. "
                   "ETF levels use the fund’s own units. Each change uses the comparison shown in its row.")
    else:
        st.info("Index observations are unavailable.")
    if poll in {"ready", "refreshing"} and data.get("fetched_at"):
        _notify_new_material(st, data["events"], datetime.now(timezone.utc))
    left, right = st.columns(2)
    category = left.selectbox("Event type", ["all", *CATEGORIES], format_func=lambda item: CATEGORIES.get(item, "All events"))
    material_only = right.checkbox("Material releases only", value=False)
    events = [row for row in data["events"] if row.get("id") and row.get("headline")
              and (category == "all" or row.get("category") == category)
              and (not material_only or row.get("significance") == "material_release")]
    if not events:
        st.info("No retained releases match these filters.")
        return
    events.sort(key=lambda row: row.get("published_at") or "", reverse=True)
    by_id = {row["id"]: row for row in events}
    def label(event_id: str | None) -> str:
        if event_id is None:
            return "Latest matching release"
        release = by_id[event_id]
        return f"{central_timestamp(release.get('published_at'))} · {release['headline']}"
    selection = st.selectbox("Release", [None, *by_id], format_func=label)
    _render_release(st, by_id[selection] if selection else events[0])


def render_market_events() -> None:
    import streamlit as st

    st.fragment(run_every="30s")(_render_market_events)()
