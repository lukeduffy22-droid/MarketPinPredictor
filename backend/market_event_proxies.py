"""Measure extended-hours ETF moves using small, unadjusted trade-bar windows."""
from __future__ import annotations

import json
from datetime import datetime, time, timedelta, timezone
from email.utils import parsedate_to_datetime
from urllib.error import HTTPError

from backend.forecast_calendar import ET, session_bounds
from backend.market_event_quotes import _now, _number, _read, _source, event_massive_key
from backend.market_events import instant
from backend.market_universe import MARKET_UNIVERSE

DOCS = 'https://massive.com/docs/rest/stocks/aggregates/custom-bars'
ETFS = ('SPY', 'QQQ', 'IWM')
MINUTE = timedelta(minutes=1)
BEFORE_LIMIT = timedelta(seconds=90)
AFTER_TARGET = timedelta(minutes=5)
AFTER_LIMIT = AFTER_TARGET + BEFORE_LIMIT
DELAYED_RETRY = timedelta(minutes=23)
MAX_EVENT_AGE = timedelta(hours=2)


def _extended_session(published):
    local = published.astimezone(ET)
    try:
        opened, closed = session_bounds(local.date())
    except ValueError:
        return None
    if not time(4) <= local.time() < time(20) or opened < published < closed - AFTER_TARGET:
        return None
    return opened, closed


def _session(start, bounds):
    if start < bounds[0]:
        return 'premarket'
    if start >= bounds[1]:
        return 'after-hours'
    return 'regular'


def _reaction(document, symbol, published, available, bounds):
    if (not isinstance(document, dict) or document.get('status') not in ('OK', 'DELAYED')
            or document.get('ticker') != symbol or document.get('adjusted') is not False):
        return None
    bars = {}
    for bar in document.get('results') or []:
        if not isinstance(bar, dict):
            continue
        ms, price = bar.get('t'), _number(bar.get('c'))
        volume = _number(bar.get('v'))
        if (type(ms) is not int or ms % 60000 or not 0 < ms < 10 ** 13
                or price is None or price <= 0 or volume is None or volume <= 0):
            continue
        start = datetime.fromtimestamp(ms / 1000, timezone.utc)
        end = start + MINUTE
        local = start.astimezone(ET)
        if (local.date() != published.astimezone(ET).date() or not time(4) <= local.time() < time(20)
                or end > available or not published - BEFORE_LIMIT <= end <= published + AFTER_LIMIT):
            continue
        if end in bars and bars[end] != price:
            return None
        bars[end] = price
    # A bar timestamp denotes its START. Its close bounds the last eligible
    # trade in [start, end); a bar ending at the release is safely pre-release.
    before = max((end for end in bars if end <= published), default=None)
    after = min((end for end in bars if end >= published + AFTER_TARGET), default=None)
    if before is None or after is None:
        return None
    left, right = _session(before - MINUTE, bounds), _session(after - MINUTE, bounds)
    session = left if left == right else f'{left} to {right}'
    return {'symbol': symbol, 'name': MARKET_UNIVERSE[symbol].name, 'kind': 'etf',
            'basis': 'etf_minute_close', 'source': 'Massive equities', 'source_url': DOCS,
            'before_at': before.isoformat(), 'after_at': after.isoformat(),
            'before_price': bars[before], 'after_price': bars[after],
            'change_pct': round((bars[after] / bars[before] - 1) * 100, 4),
            'session': session, 'available_at': available.isoformat(),
            'detail': f'{session.capitalize()} ETF trades. Times are minute-bar ends. Historical retrieval may be delayed by the data plan.'}


class ETFReactionBackfill:
    """A serial background worker; at most three short requests per eligible run."""
    def __init__(self):
        self.slots = {}
        self.next_request = None
        self.last_source = None

    def collect(self, events, *, now=None):
        at = _now(now)
        def source(status, detail):
            return _source('massive_etf_windows', 'Massive extended-hours ETF bars', DOCS, status, detail, _now(now))
        key = event_massive_key()
        if not key:
            return {}, source('not_configured', 'No Massive equities key configured.')

        self.slots = {t: slot for t, slot in self.slots.items() if at - t <= MAX_EVENT_AGE}
        eligible = {}
        for event in events:
            published = instant(event.get('published_at'))
            if (not published or not event.get('id') or not AFTER_LIMIT <= at - published <= MAX_EVENT_AGE
                    or not _extended_session(published)):
                continue
            already = {row.get('symbol') for row in event.get('event_window') or [] if row.get('basis') == 'etf_minute_close'}
            missing = set(ETFS) - already
            if missing:
                eligible.setdefault(published, []).append((event['id'], missing))

        updates = {}
        for published in sorted(eligible, reverse=True):
            slot = self.slots.setdefault(published, {'stage': 0, 'rows': {}})
            missing = set.union(*(symbols for _, symbols in eligible[published])) - slot['rows'].keys()
            stage = 2 if at - published >= DELAYED_RETRY else 1
            if (missing and slot['stage'] < stage and
                    (self.next_request is None or at >= self.next_request)):
                self.next_request = at + MINUTE
                slot['stage'] = stage
                start_ms = int((published - BEFORE_LIMIT - MINUTE).timestamp()) * 1000
                end_ms = int((published + AFTER_LIMIT).timestamp()) * 1000
                errors = []
                for symbol in sorted(missing):
                    url = (f'https://api.massive.com/v2/aggs/ticker/{symbol}/range/1/minute/{start_ms}/{end_ms}'
                           '?adjusted=false&sort=asc&limit=15')
                    try:
                        body = _read(url, bearer=key)
                        row = _reaction(json.loads(body), symbol, published, _now(now), _extended_session(published))
                        if row:
                            slot['rows'][symbol] = row
                    except (OSError, ValueError) as exc:
                        errors.append(f'HTTP {exc.code}' if isinstance(exc, HTTPError) else type(exc).__name__)
                        if isinstance(exc, HTTPError) and exc.code == 429:
                            retry = (exc.headers or {}).get('Retry-After', '')
                            try:
                                until = (at + timedelta(seconds=int(retry)) if retry.isdigit()
                                         else parsedate_to_datetime(retry).astimezone(timezone.utc))
                            except (TypeError, ValueError, OverflowError):
                                until = at + DELAYED_RETRY
                            self.next_request = max(self.next_request, until)
                            break
                        if isinstance(exc, HTTPError) and exc.code == 401:
                            break
                found = len(slot['rows'])
                status = 'ok' if found == len(ETFS) else 'partial' if found else 'unavailable'
                detail = f'{found} of {len(ETFS)} ETF windows measured for {published.isoformat()}.'
                if errors:
                    detail += ' Source error: ' + ', '.join(sorted(set(errors))) + '.'
                self.last_source = source(status, detail)
            for event_id, absent in eligible[published]:
                rows = [slot['rows'][s] for s in ETFS if s in absent and s in slot['rows']]
                if rows:
                    updates[event_id] = {'published_at': published.isoformat(), 'rows': rows}
        return updates, self.last_source or source('idle', 'No eligible extended-hours release window yet.')
