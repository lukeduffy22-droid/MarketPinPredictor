from datetime import datetime, timedelta, timezone
import json

import pytest
from backend import market_event_proxies as proxies


# Synthetic 08:30 Eastern release.
EVENT = datetime(2026, 11, 19, 13, 30, tzinfo=timezone.utc)


def bar(start, price):
    return {'t': int(start.timestamp()) * 1000, 'c': price, 'v': 10}


def document(symbol, release=EVENT):
    return {'ticker': symbol, 'adjusted': False, 'status': 'OK', 'results': [
        bar(release - timedelta(minutes=1), 100), bar(release, 700),
        bar(release + timedelta(minutes=4), 102), bar(release + timedelta(minutes=5), 101)]}


def test_measure_complete_etf_bars_and_dedup_coincident_releases(monkeypatch):
    calls = []
    def fetch(url, *, bearer):
        assert bearer == 'sample' and 'sample' not in url and 'adjusted=false' in url
        symbol = url.split('/ticker/')[1].split('/')[0]
        calls.append(symbol)
        return json.dumps(document(symbol)).encode()
    monkeypatch.setattr(proxies, 'event_massive_key', lambda: 'sample')
    monkeypatch.setattr(proxies, '_read', fetch)
    worker = proxies.ETFReactionBackfill()
    events = [{'id': id_, 'published_at': EVENT.isoformat()} for id_ in ('a', 'b')]
    assert not worker.collect(events, now=EVENT + timedelta(minutes=5))[0]
    updates, source = worker.collect(events, now=EVENT + timedelta(minutes=7))
    assert sorted(calls) == ['IWM', 'QQQ', 'SPY'] and source['status'] == 'ok'
    assert updates['a'] == updates['b']
    row = updates['a']['rows'][0]
    assert row['symbol'] == 'SPY' and row['kind'] == 'etf' and row['basis'] == 'etf_minute_close'
    assert row['before_at'] == EVENT.isoformat() and row['after_at'] == (EVENT + timedelta(minutes=5)).isoformat()
    assert row['session'] == 'premarket' and row['change_pct'] == pytest.approx(2.)
    assert row['available_at'] == (EVENT + timedelta(minutes=7)).isoformat()
    for event in events:
        event['event_window'] = updates[event['id']]['rows']
    assert worker.collect(events, now=EVENT + timedelta(minutes=25))[0] == {} and len(calls) == 3


def test_non_minute_release_excludes_containing_bar_and_never_bridges_missing_minutes():
    published = EVENT + timedelta(seconds=37)
    result = document('QQQ')
    row = proxies._reaction(result, 'QQQ', published, published + timedelta(minutes=8), proxies._extended_session(published))
    assert row['before_price'] == 100 and row['after_price'] == 101
    result['results'] = result['results'][1:]
    assert proxies._reaction(result, 'QQQ', published, published + timedelta(minutes=8), proxies._extended_session(published)) is None


def test_keyless_and_holiday_and_regular_hours_do_not_fetch(monkeypatch):
    monkeypatch.setattr(proxies, '_read', lambda *a, **kw: pytest.fail('unexpected provider request'))
    worker = proxies.ETFReactionBackfill()
    monkeypatch.setattr(proxies, 'event_massive_key', lambda: None)
    e = {'id': 'abc', 'published_at': EVENT.isoformat()}
    assert worker.collect([e], now=EVENT + timedelta(minutes=8))[1]['status'] == 'not_configured'
    monkeypatch.setattr(proxies, 'event_massive_key', lambda: 'sample')
    for hour in (1, 15):
        t = EVENT.replace(hour=hour)
        assert not worker.collect([dict(e, published_at=t.isoformat())], now=t + timedelta(minutes=8))[0]
    thanksgiving = EVENT.replace(day=26)
    assert not worker.collect([dict(e, published_at=thanksgiving.isoformat())], now=thanksgiving + timedelta(minutes=8))[0]


def test_rate_bound_and_delayed_second_pass(monkeypatch):
    calls = []
    def fetch(url, *, bearer):
        symbol = url.split('/ticker/')[1].split('/')[0]
        calls.append(symbol)
        if len(calls) <= 3:
            return b'{"status":"OK"}'
        return json.dumps(document(symbol)).encode()
    monkeypatch.setattr(proxies, '_read', fetch)
    monkeypatch.setattr(proxies, 'event_massive_key', lambda: 'sample')
    worker = proxies.ETFReactionBackfill()
    event = {'id': 'a', 'published_at': EVENT.isoformat()}
    assert not worker.collect([event], now=EVENT + timedelta(minutes=8))[0]
    other = {'id': 'b', 'published_at': (EVENT - timedelta(minutes=1)).isoformat()}
    assert not worker.collect([event, other], now=EVENT + timedelta(minutes=8, seconds=5))[0]
    assert not worker.collect([event], now=EVENT + timedelta(minutes=22))[0]
    assert worker.collect([event], now=EVENT + timedelta(minutes=23))[0] and len(calls) == 6
    assert not worker.collect([event], now=EVENT + timedelta(hours=3))[0] and len(calls) == 6


def test_after_hours_session_boundary_and_conflicting_or_unavailable_data():
    pub = EVENT.replace(hour=21, minute=0)
    bounds = proxies._extended_session(pub)
    available = pub + timedelta(minutes=8)
    result = document('IWM', pub)
    assert proxies._reaction(result, 'IWM', pub, available, bounds)['session'] == 'regular to after-hours'
    assert proxies._reaction(result, 'SPY', pub, available, bounds) is None
    assert proxies._reaction(result, 'IWM', pub, pub + timedelta(minutes=4), bounds) is None
    assert proxies._reaction(dict(result, adjusted=True), 'IWM', pub, available, bounds) is None
    result['results'].append(bar(pub - timedelta(minutes=1), 777))
    assert proxies._reaction(result, 'IWM', pub, available, bounds) is None


def test_provider_retry_after_applies_across_events(monkeypatch):
    from urllib.error import HTTPError
    calls = []
    def fetch(url, *, bearer):
        calls.append(url)
        raise HTTPError(url, 429, 'Too many requests', {'Retry-After': '1800'}, None)
    monkeypatch.setattr(proxies, 'event_massive_key', lambda: 'sample')
    monkeypatch.setattr(proxies, '_read', fetch)
    worker = proxies.ETFReactionBackfill()
    event = {'id': 'a', 'published_at': EVENT.isoformat()}
    at = EVENT + timedelta(minutes=7)
    worker.collect([event], now=at)
    assert len(calls) == 1 and worker.next_request == at + timedelta(minutes=30)
    worker.collect([event, dict(event, published_at=(EVENT - timedelta(minutes=1)).isoformat())], now=at + timedelta(minutes=25))
    assert len(calls) == 1
