"""Retain official releases, point-in-time survey observations and bounded event windows."""
from __future__ import annotations

import json
import math
import sqlite3
from bisect import bisect_right
from collections import defaultdict
from contextlib import closing
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable

UTC = timezone.utc


def instant(value: str | None) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed.astimezone(UTC) if parsed.tzinfo else None


def number(value: object) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)


class EventArchive:
    def __init__(self, path: Path):
        self.path = path

    def _connect(self) -> sqlite3.Connection:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        db = sqlite3.connect(self.path, timeout=5)
        db.execute("""CREATE TABLE IF NOT EXISTS events (
            id TEXT PRIMARY KEY, published_at TEXT NOT NULL, observed_at TEXT NOT NULL,
            payload TEXT NOT NULL, windows TEXT NOT NULL DEFAULT '[]')""")
        db.execute("""CREATE TABLE IF NOT EXISTS event_aliases (
            alias TEXT PRIMARY KEY, event_id TEXT NOT NULL)""")
        db.execute("""CREATE TABLE IF NOT EXISTS surveys (
            category TEXT, period TEXT, metric TEXT, unit TEXT, captured TEXT, source TEXT,
            payload TEXT, PRIMARY KEY (category, period, metric, unit, captured, source))""")
        return db

    def recent(self, db: sqlite3.Connection) -> list[dict]:
        rows = db.execute("""SELECT payload, observed_at, windows FROM events
                             ORDER BY published_at DESC, id LIMIT 100""")
        return [dict(json.loads(payload), observed_at=first, event_window=json.loads(windows))
                for payload, first, windows in rows]

    def restore(self) -> list[dict]:
        with closing(self._connect()) as db:
            return self.recent(db)

    def record(self, events: Iterable[dict], surveys: Iterable[dict], now: datetime) -> list[dict]:
        with closing(self._connect()) as db, db:
            # Normalize timestamps to one UTC representation before SQLite comparisons.
            for record in surveys:
                at = instant(record.get("expected_as_of"))
                if not at or at > now or not number(record.get("expected")):
                    continue
                fields = [record.get(k) for k in ("category", "period", "metric", "unit")]
                if not all(fields) or not record.get("expected_url") or not record.get("expected_source"):
                    continue
                db.execute("INSERT OR IGNORE INTO surveys VALUES (?, ?, ?, ?, ?, ?, ?)",
                           (*fields, at.isoformat(), record["expected_source"], json.dumps(record)))
            for event in events:
                published = instant(event.get("published_at"))
                if not event.get("id") or not published or published > now:
                    continue
                if db.execute("SELECT 1 FROM event_aliases WHERE alias=?", (event["id"],)).fetchone():
                    continue
                event = dict(event, figures=[dict(f) for f in event.get("figures", [])])
                # An independently verified issuer release can supersede a later filing for the same results.
                for alias in event.get("supersedes_ids", []):
                    if alias != event["id"]:
                        db.execute("INSERT OR REPLACE INTO event_aliases VALUES (?,?)", (alias, event["id"]))
                        db.execute("DELETE FROM events WHERE id=?", (alias,))
                old = db.execute("SELECT payload FROM events WHERE id=?", (event["id"],)).fetchone()
                if old:
                    previous = json.loads(old[0])
                    links = {r["source_url"]: r for r in previous.get("source_links", [])}
                    links.update({r["source_url"]: r for r in event.get("source_links", [])})
                    if links:
                        event["source_links"] = list(links.values())
                    # A missing optional provider or exhibit must not erase a sourced, dated observation.
                    seen = {(r.get("source") or event["source"], r["metric"], r["unit"]) for r in event["figures"]}
                    for prior in previous.get("figures", []):
                        ident = (prior.get("source") or previous["source"], prior["metric"], prior["unit"])
                        if ident not in seen and (prior.get("source_url") or prior.get("as_of")):
                            event["figures"].append(prior)
                    if not event.get("period") and previous.get("period"):
                        for field in ("period", "headline", "facts", "exhibit_url", "fiscal_year_end"):
                            if previous.get(field):
                                event[field] = previous[field]
                for figure in event["figures"]:
                    if not number(figure.get("actual")) or figure.get("expected") is not None:
                        continue
                    row = db.execute("""SELECT payload FROM surveys
                        WHERE category=? AND period=? AND metric=? AND unit=? AND captured < ?
                        ORDER BY captured DESC LIMIT 1""",
                        (event["category"], event.get("period"), figure["metric"], figure["unit"],
                         published.isoformat())).fetchone()
                    if row:
                        survey = json.loads(row[0])
                        figure.update({k: survey[k] for k in
                                       ("expected", "expected_as_of", "expected_source", "expected_url")})
                        figure["surprise"] = round(figure["actual"] - figure["expected"], 6)
                key = (event["id"], published.isoformat(), now.isoformat(), json.dumps(event))
                db.execute("""INSERT INTO events (id,published_at,observed_at,payload) VALUES (?,?,?,?)
                    ON CONFLICT(id) DO UPDATE SET payload=excluded.payload,
                    windows=CASE WHEN events.published_at=excluded.published_at THEN events.windows ELSE '[]' END,
                    published_at=excluded.published_at""", key)
            db.execute("DELETE FROM surveys WHERE captured < ?", ((now - timedelta(days=100)).isoformat(),))
            db.execute("DELETE FROM events WHERE published_at < ?", ((now - timedelta(days=100)).isoformat(),))
            db.execute("DELETE FROM event_aliases WHERE event_id NOT IN (SELECT id FROM events)")
            return self.recent(db)

    def retain_windows(self, windows: dict[str, tuple[str, list[dict]]]) -> None:
        if not windows:
            return
        with closing(self._connect()) as db, db:
            for event_id, (published, observations) in windows.items():
                db.execute("UPDATE events SET windows=? WHERE id=? AND published_at=?",
                           (json.dumps(observations), event_id, published))


class QuoteWindow:
    """Sample validated, same-generation references. A daily close cannot enter a window."""
    def __init__(self):
        self.history: dict[tuple, list] = defaultdict(list)

    def observe(self, rows: list[dict], now: datetime) -> None:
        cutoff = now - timedelta(hours=2)
        for key in list(self.history):
            items = self.history[key]
            del items[:bisect_right(items, cutoff, key=lambda item: item[0])]
            if not items:
                del self.history[key]
        for row in rows:
            when = instant(row.get("observed_at"))
            if (row.get("status") != "current" or not when or not cutoff <= when <= now
                or not number(row.get("price")) or row["price"] <= 0):
                continue
            if row.get("basis") == "cash_index":
                identity = ("cash", None, None)
            elif row.get("basis") == "option_implied_reference":
                identity = (row.get("subscription_epoch_id"), row.get("active_generation"),
                            row.get("primary_expiration"))
                if not identity[0] or identity[1] is None or not identity[2]:
                    continue
            else:
                continue
            key = (row["symbol"], row["basis"], row.get("source"), row.get("source_url"), *identity)
            items = self.history[key]
            if not items or items[-1][0] < when:
                items.append((when, row["price"]))

    def reaction(self, published: datetime) -> list[dict]:
        result = {}
        for (symbol, basis, source, source_url, *_identity), items in self.history.items():
            before = bisect_right(items, published - timedelta(microseconds=1), key=lambda x: x[0]) - 1
            after = bisect_right(items, published + timedelta(minutes=5) - timedelta(microseconds=1),
                                 key=lambda x: x[0])
            if before < 0 or after == len(items):
                continue
            t0, p0 = items[before]
            t1, p1 = items[after]
            if published - t0 > timedelta(seconds=90) or t1 - published > timedelta(minutes=6, seconds=30):
                continue
            if symbol in result and result[symbol]["basis"] == "cash_index":
                continue
            result[symbol] = {"symbol": symbol, "basis": basis, "source": source, "source_url": source_url,
                              "before_at": t0.isoformat(), "after_at": t1.isoformat(),
                              "before_level": p0, "after_level": p1,
                              "change_pct": round(100 * (p1 / p0 - 1), 4)}
        return sorted(result.values(), key=lambda row: row["symbol"])


def assess(event: dict) -> dict:
    category = event["category"]
    windows = event.get("event_window") or []
    moves = [r for r in windows if r["symbol"] in {"SPX", "NDX", "RUT", "DJI"}]
    largest = max(moves, key=lambda r: abs(r["change_pct"]), default=None)
    matched = [f for f in event.get("figures", []) if number(f.get("surprise")) and
               instant(f.get("expected_as_of")) and instant(event["published_at"]) and
               instant(f.get("expected_as_of")) < instant(event["published_at"])]
    volatility = next((r for r in windows if r["symbol"] == "VIX" and r["basis"] == "cash_index"
                       and number(r.get("before_level")) and number(r.get("after_level"))), None)
    vol_move = volatility["after_level"] - volatility["before_level"] if volatility else None
    material = category in {"cpi", "pce"}
    reason = {"cpi": "Monthly inflation release", "pce": "Monthly PCE price index release"}.get(
        category, "Policy communication")
    if category == "earnings":
        material = bool(event.get("period") or event.get("issuer_release")) and event.get("form") != "8-K/A"
        reason = ("Major issuer results release" if material else
                  "Results-related filing. Quarterly results or a material change remain unverified.")
    if category == "fed_policy":
        material = any(term in event["headline"].lower() for term in (
            "fomc statement", "minutes", "policy decision", "interest rate decision", "implementation note", "projections"))
    if category.startswith("fed") and ((largest and abs(largest["change_pct"]) >= 0.35) or
                                       (vol_move is not None and abs(vol_move) >= 0.5)):
        material, reason = True, "Policy communication coincided with a measured equity move of at least 0.35% or a VIX move of at least 0.5 points."
    assessment = []
    if matched:
        figure = matched[0]
        unit = "percentage points" if figure["unit"] == "%" else figure["unit"]
        assessment.append(f"{figure['metric']} differed from the recorded pre-release estimate by {figure['surprise']:+g} {unit}.")
    anchor = "filing acceptance" if event.get("source") == "SEC EDGAR" else "publication"
    if largest:
        assessment.append(f"{largest['symbol']} had the largest measured equity-benchmark move across {anchor} "
                          f"({largest['change_pct']:+.2f}%, {largest['basis'].replace('_', ' ')}).")
    if vol_move is not None:
        assessment.append(f"The VIX cash index moved {vol_move:+.2f} points across {anchor}.")
    proxies = [r for r in windows if r.get("basis") == "etf_minute_close" and number(r.get("change_pct"))]
    if proxies:
        observed = ", ".join(f"{r['symbol']} {r['change_pct']:+.2f}%" for r in proxies)
        assessment.append(f"Extended-hours ETF trades across {anchor}: {observed}. These are fund returns.")
    if largest or volatility or proxies:
        assessment.append("The observations bracket the event and target five minutes afterward. Other news may have contributed.")
    else:
        assessment.append("No qualifying event window is available to establish an equity-index or VIX response.")
    return dict(event, assessment=" ".join(assessment),
                significance="material_release" if material else "watch", alert_reason=reason)
