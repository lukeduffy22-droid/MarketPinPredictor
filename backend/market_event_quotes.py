"""Source-timed index context. External fetches are for the background poller only."""
from __future__ import annotations

import csv
from datetime import date, datetime, timedelta, timezone
from io import BytesIO, StringIO
import json
import math
import os
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from zipfile import BadZipFile, ZipFile

from backend.config import DATABENTO_SUPPORTED_SYMBOLS, DATABENTO_SYMBOLS, LIVE_DATA_STALE_AFTER_SECONDS
from backend.forecast_calendar import ET, session_bounds, shift_session
from backend.market_universe import MARKET_UNIVERSE

FRED_SERIES = {
    'SPX': ('SP500', 'S&P Dow Jones Indices'), 'NDX': ('NASDAQ100', 'Nasdaq'),
    'DJI': ('DJIA', 'S&P Dow Jones Indices'), 'VIX': ('VIXCLS', 'Cboe'),
    'SOX': ('NASDAQSOX', 'Nasdaq'), 'RVX': ('RVXCLS', 'Cboe'), 'VXN': ('VXNCLS', 'Cboe'),
}
EXTRA_NAMES = {'SOX': 'PHLX Semiconductor Index', 'RVX': 'Cboe Russell 2000 Volatility Index',
               'VXN': 'Cboe Nasdaq-100 Volatility Index'}
OPTIONAL_INDEX_NAMES = {
    'XSP': 'Mini-SPX Index', 'XND': 'Nasdaq-100 Micro Index', 'MRUT': 'Mini-Russell 2000 Index',
    'OEX': 'S&P 100', 'DJX': 'Dow Jones Industrial Average (1/100)', 'RUI': 'Russell 1000',
    'XAU': 'PHLX Gold/Silver Sector', 'HGX': 'PHLX Housing Sector',
    'OSX': 'PHLX Oil Service Sector', 'UTY': 'PHLX Utility Sector',
}
MASSIVE_DOCS = 'https://massive.com/docs/rest/indices/snapshots/indices-snapshot'
FRED_URL = 'https://fred.stlouisfed.org/graph/fredgraph.csv'
DATABENTO_URL = 'https://databento.com/datasets/OPRA.PILLAR'
MAX_RESPONSE_BYTES = 2_000_000


def _now(value):
    value = value or datetime.now(timezone.utc)
    if value.tzinfo is None:
        raise ValueError('A timezone-aware reference time is required')
    return value.astimezone(timezone.utc)


def _number(value):
    if isinstance(value, bool):
        return None
    try:
        result = float(value)
        return result if math.isfinite(result) else None
    except (TypeError, ValueError, OverflowError):
        return None


def event_massive_key():
    """Resolve the event feed credential independently of the capture provider."""
    for name in ('MASSIVE_API_KEY', 'Massive_API', 'POLYGON_API_KEY'):
        candidate = os.getenv(name, '').strip()
        if (candidate and not candidate.lower().startswith(('your_', 'db-'))
                and not any(char.isspace() for char in candidate)):
            return candidate
    return None


def _read(url, *, bearer=None):
    request = Request(url, headers={'User-Agent': 'MarketPinPredictor/1.0'})
    if bearer:
        request.add_unredirected_header('Authorization', 'Bearer ' + bearer)
    with urlopen(request, timeout=6) as response:
        body = response.read(MAX_RESPONSE_BYTES + 1)
    if len(body) > MAX_RESPONSE_BYTES:
        raise ValueError('response_size_limit')
    return body


def _source(source_id, name, url, status, detail, now):
    return {'id': source_id, 'name': name, 'source_url': url, 'status': status,
            'detail': detail, 'checked_at': now.isoformat()}


def _empty(symbol):
    instrument = MARKET_UNIVERSE.get(symbol)
    return {'symbol': symbol, 'name': instrument.name if instrument else EXTRA_NAMES.get(symbol, OPTIONAL_INDEX_NAMES.get(symbol, symbol)),
            'kind': instrument.kind if instrument else 'index', 'status': 'unavailable',
            'price': None, 'observed_at': None, 'session_date': None, 'basis': 'unavailable',
            'source': None, 'source_url': None, 'change_pct': None, 'change_basis': None,
            'detail': 'No verified observation from the configured sources.'}


def _last_completed_session(now):
    today = now.astimezone(ET).date()
    for offset in range(10):
        day = today - timedelta(days=offset)
        try:
            if session_bounds(day)[1] <= now:
                return day
        except ValueError:
            continue
    return None


def _fred_csv_documents(body):
    # FRED groups series with different frequencies (e.g. Daily vs Daily, Close)
    # into separate CSV members in a ZIP even when fredgraph.csv is requested.
    if not body.startswith(b'PK'):
        return [body]
    with ZipFile(BytesIO(body)) as archive:
        members = [member for member in archive.infolist() if member.filename.lower().endswith('.csv')]
        if not members or sum(member.file_size for member in members) > MAX_RESPONSE_BYTES:
            raise ValueError('invalid_csv_bundle')
        return [archive.read(member) for member in members]


def _daily_rows(body, now):
    latest_session = _last_completed_session(now)
    if latest_session is None:
        return {}
    cutoff = latest_session - timedelta(days=21)
    samples = {series: {} for series, _ in FRED_SERIES.values()}
    for document in _fred_csv_documents(body):
        reader = csv.DictReader(StringIO(document.decode('utf-8-sig')))
        recognized = set(reader.fieldnames or ()) & samples.keys()
        for row in reader:
            try:
                day = date.fromisoformat(row.get('observation_date') or row.get('DATE') or '')
                if not cutoff <= day <= latest_session:
                    continue
                session_bounds(day)  # Exclude repeated holiday observations.
            except ValueError:
                continue
            for series in recognized:
                price = _number(row[series])
                if price is not None and price > 0:
                    samples[series][day] = price
    result = {}
    for symbol, (series, publisher) in FRED_SERIES.items():
        points = samples[series]
        if not points:
            continue
        day = max(points)
        previous = points.get(shift_session(day, -1))
        price = points[day]
        result[symbol] = {**_empty(symbol), 'price': price, 'basis': 'daily_index_close',
                          'status': 'daily_close' if day == latest_session else 'stale',
                          'session_date': day.isoformat(), 'source': f'{publisher} via FRED',
                          'source_url': f'https://fred.stlouisfed.org/series/{series}',
                          'change_pct': (price / previous - 1) * 100 if previous else None,
                          'change_basis': 'previous session close to this close' if previous else None,
                          'detail': 'Daily close with session-date precision. Intraday movement is unavailable.'}
    return result


def fetch_daily_index_context(*, now=None):
    started = _now(now)
    series = [series for series, _ in FRED_SERIES.values()]
    query = urlencode({'id': ','.join(series),
                       'cosd': ','.join([(started.date() - timedelta(days=21)).isoformat()] * len(series))})
    try:
        body = _read(f'{FRED_URL}?{query}')
        rows = _daily_rows(body, _now(now))
        status = 'ok' if len(rows) == len(series) else 'partial' if rows else 'unavailable'
        detail = f'{len(rows)} of {len(series)} dated series available.'
    except (OSError, ValueError, BadZipFile, csv.Error, EOFError) as exc:
        rows, status, detail = {}, 'unavailable', f'Daily index source unavailable ({type(exc).__name__}).'
    return {'rows': rows, 'source': _source('fred_indices', 'FRED daily indices',
            'https://fred.stlouisfed.org/', status, detail, _now(now))}


def _cash_rows(document, requested, now):
    rows = {}
    if not isinstance(document, dict) or document.get('status') not in ('OK', 'DELAYED'):
        return rows
    for item in document.get('results') or []:
        if not isinstance(item, dict) or item.get('error') or item.get('type') != 'indices':
            continue
        ticker = item.get('ticker')
        if ticker not in requested:
            continue
        ns, price, timeframe = item.get('last_updated'), _number(item.get('value')), item.get('timeframe')
        if type(ns) is not int or not 0 < ns < 2 ** 63 or price is None or price <= 0:
            continue
        observed = datetime.fromtimestamp(ns / 1_000_000_000, timezone.utc)
        if observed > now or timeframe not in ('REAL-TIME', 'DELAYED'):
            continue
        if document['status'] == 'DELAYED':
            timeframe = 'DELAYED'
        symbol = ticker[2:]
        session = item.get('session') or {}
        previous = _number(session.get('previous_close')) if isinstance(session, dict) else None
        rows[symbol] = {**_empty(symbol), 'price': price, 'basis': 'cash_index',
                        'source': 'Massive', 'source_url': MASSIVE_DOCS, 'timeframe': timeframe,
                        'observed_at': observed.isoformat(), 'session_date': observed.astimezone(ET).date().isoformat(),
                        'market_status': str(item.get('market_status') or 'unknown').lower(),
                        'change_pct': (price / previous - 1) * 100 if previous and previous > 0 else None,
                        'change_basis': 'index value versus previous session close' if previous and previous > 0 else None}
        _age_cash(rows[symbol], now)
    return rows


def fetch_cash_index_context(*, now=None):
    rows = {}
    key = event_massive_key()
    if not key:
        status, detail = 'not_configured', 'No Massive indices key configured.'
    else:
        index_symbols = (set(DATABENTO_SUPPORTED_SYMBOLS) | set(FRED_SERIES)) - {
            symbol for symbol, instrument in MARKET_UNIVERSE.items() if instrument.kind == 'etf'}
        requested = {'I:' + symbol for symbol in index_symbols}
        query = urlencode({'ticker.any_of': ','.join(sorted(requested)), 'limit': len(requested)})
        try:
            body = _read('https://api.massive.com/v3/snapshot/indices?' + query, bearer=key)
            rows = _cash_rows(json.loads(body), requested, _now(now))
            status = 'ok' if len(rows) == len(requested) else 'partial' if rows else 'unavailable'
            detail = f'{len(rows)} of {len(requested)} requested index snapshots available; coverage depends on entitlements.'
        except (HTTPError, URLError, OSError, ValueError) as exc:
            status = 'unavailable'
            code = getattr(exc, 'code', None)
            detail = 'Indices request failed (' + (f'HTTP {code}' if code else type(exc).__name__) + ').'
    return {'rows': rows, 'source': _source('massive_indices', 'Massive index snapshots', MASSIVE_DOCS, status, detail, _now(now))}


def _age_cash(row, now):
    observed = datetime.fromisoformat(row['observed_at'])
    age = (now - observed).total_seconds()
    # Cash snapshots are sampled once per minute. A delayed entitlement remains
    # delayed even if the provider timestamp happens to be just a few seconds old.
    status = 'stale'
    if row['market_status'] == 'open' and age >= 0:
        if row['timeframe'] == 'REAL-TIME' and age <= 90:
            status = 'current'
        elif row['timeframe'] == 'DELAYED' and age <= 17 * 60:
            status = 'delayed'
    row.update(status=status, detail=f"Provider feed: {row['timeframe']}. Market: {row['market_status']}. Observation age: {int(age)}s.")


def _reference(streamer, symbol, now):
    from backend.api.helpers import current_buffered_payload
    from backend.structural_regime_live import parse_utc
    from backend.workstation import payload_has_fallback_provenance

    payload, _ = current_buffered_payload(streamer, symbol)
    if (not payload or payload.get('symbol') != symbol
            or payload.get('spot_source') != 'databento_opra_put_call_parity'
            or payload.get('is_etf_proxy') is not False or payload_has_fallback_provenance(payload)
            or payload.get('validation_is_valid') is not True or payload.get('gamma_excluded_from_model') is not False):
        return None
    observed = parse_utc(payload.get('observation_index_utc') or payload.get('latest_ts_recv_utc'))
    price = _number(payload.get('price'))
    if price is None or price <= 0 or observed is None or observed > now:
        return None
    age = (now - observed).total_seconds()
    return {**_empty(symbol), 'price': price, 'basis': 'option_implied_reference', 'source': 'Databento OPRA',
            'source_url': DATABENTO_URL, 'status': 'current' if age <= LIVE_DATA_STALE_AFTER_SECONDS else 'stale',
            'observed_at': observed.isoformat(), 'session_date': observed.astimezone(ET).date().isoformat(),
            'subscription_epoch_id': payload['subscription_epoch_id'], 'active_generation': payload['subscription_generation'],
            'primary_expiration': payload.get('primary_expiration'),
            'detail': f'Put-call parity estimate from option quotes. Latest provider observation age: {int(age)}s.'}


def build_market_context(streamer, *, now=None, daily=None, cash=None):
    """Project cached provider reads. Never start a subscription or fetch a quote."""
    now = _now(now)
    daily, cash = daily or {}, cash or {}
    selected = tuple(getattr(streamer, 'symbols', ()))
    symbols = dict.fromkeys([*(symbol for symbol, item in MARKET_UNIVERSE.items() if item.kind == 'index'),
                             *DATABENTO_SYMBOLS, *(s for s in selected if s in DATABENTO_SUPPORTED_SYMBOLS), *EXTRA_NAMES])
    latest_session = _last_completed_session(now)
    result = []
    for symbol in symbols:
        candidates = []
        if symbol in cash:
            cash_row = cash[symbol].copy()
            _age_cash(cash_row, now)
            candidates.append(cash_row)
        if streamer is not None and symbol in selected:
            try:
                reference = _reference(streamer, symbol, now)
            except (AttributeError, RuntimeError, KeyError, ValueError, TypeError, OSError):
                reference = None
            if reference:
                candidates.append(reference)
        if symbol in daily:
            daily_row = daily[symbol].copy()
            daily_row['status'] = 'daily_close' if latest_session and daily_row['session_date'] == latest_session.isoformat() else 'stale'
            candidates.append(daily_row)
        if not candidates:
            result.append(_empty(symbol))
            continue
        # Source precision is preserved: never compare an option estimate to an official close.
        rank = {'current': 3, 'delayed': 2, 'daily_close': 1, 'stale': 0}
        result.append(max(candidates, key=lambda c: (rank[c['status']], c['session_date'],
                          c['basis'] == 'cash_index', c['basis'] == 'daily_index_close', c['observed_at'] or '')))
    return result
