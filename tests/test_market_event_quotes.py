from datetime import datetime, timedelta, timezone
from io import BytesIO
import json
from zipfile import ZipFile

import pytest
from backend import market_event_quotes as quotes


NOW = datetime(2026, 11, 19, 16, tzinfo=timezone.utc)
EPOCH = 'a' * 64


class Stream:
    symbols = ('RUT', 'XSP', 'IWM')

    def __init__(self, **changes):
        self.data = {'symbol': 'RUT', 'spot_source': 'databento_opra_put_call_parity',
                     'price': 2200, 'is_etf_proxy': False, 'gamma_excluded_from_model': False,
                     'validation_is_valid': True, 'subscription_epoch_id': EPOCH, 'subscription_generation': 2,
                     'primary_expiration': '2026-11-20', 'observation_index_utc': NOW.isoformat(), **changes}

    def get_subscription_context(self):
        return dict(subscription_epoch_id=EPOCH, subscription_generation=2, handoff_status='active')

    def get_latest_data(self, symbol, n):
        return [self.data] if symbol == 'RUT' else []


def lookup(stream=None, now=NOW, **rows):
    return {row['symbol']: row for row in quotes.build_market_context(stream, now=now, **rows)}


def test_fred_mixed_frequency_archive_preserves_date_and_prior_session_only(monkeypatch):
    buf = BytesIO()
    with ZipFile(buf, 'w') as bundle:
        bundle.writestr('daily,_close.csv', 'observation_date,NASDAQ100,VIXCLS\n2026-11-16,90,20\n2026-11-17,100,.\n2026-11-18,110,25\n2026-11-19,999,799')
        bundle.writestr('daily.csv', 'observation_date,NASDAQSOX\n2026-11-17,50\n2026-11-18,49\n')
    monkeypatch.setattr(quotes, '_read', lambda *args: buf.getvalue())
    result = quotes.fetch_daily_index_context(now=NOW)
    assert result['source']['status'] == 'partial'
    rows = lookup(daily=result['rows'])
    assert set(rows) >= {'SPX', 'NDX', 'RUT', 'DJI', 'VIX', 'VXN', 'RVX', 'SOX'}
    ndx = rows['NDX']
    assert (ndx['price'], ndx['observed_at'], ndx['session_date'], ndx['status']) == (110, None, '2026-11-18', 'daily_close')
    assert ndx['change_pct'] == pytest.approx(10)
    assert rows['VIX']['change_pct'] is None
    assert rows['SOX']['change_pct'] == pytest.approx(-2)
    assert lookup(now=NOW + timedelta(hours=6), daily=result['rows'])['NDX']['status'] == 'stale'
    assert rows['RUT']['price'] is None


def test_reference_preserves_instrument_and_binding():
    rows = lookup(Stream())
    assert (rows['RUT']['basis'], rows['RUT']['status'], rows['RUT']['source'], rows['RUT']['observed_at']) == (
        'option_implied_reference', 'current', 'Databento OPRA', NOW.isoformat())
    assert rows['RUT']['subscription_epoch_id'] == EPOCH
    assert rows['RUT']['active_generation'] == 2
    assert rows['RUT']['primary_expiration'] == '2026-11-20'
    assert rows['XSP']['kind'] == 'index' and rows['IWM']['kind'] == 'etf'
    assert lookup(Stream(), now=NOW + timedelta(minutes=10))['RUT']['status'] == 'stale'


@pytest.mark.parametrize('changes', [dict(subscription_generation=1), dict(symbol='IWM'), dict(is_etf_proxy=True),
                                    dict(universe_provenance={'is_fallback': True}), dict(validation_is_valid=False),
                                    dict(observation_index_utc=None, generated_at_utc=NOW.isoformat()),
                                    dict(observation_index_utc=(NOW + timedelta(seconds=1)).isoformat())])
def test_rejects_unverified_references(changes):
    assert lookup(Stream(**changes))['RUT']['price'] is None


def test_cash_precedence_entitlements_and_cache_aging(monkeypatch):
    item = dict(type='indices', ticker='I:RUT', timeframe='DELAYED', value=2025., market_status='open',
                last_updated=int(NOW.timestamp()) * 10 ** 9, session={'previous_close': 2000.})
    monkeypatch.setattr(quotes, 'event_massive_key', lambda: 'sample')
    def fetch(url, *, bearer):
        assert url.startswith('https://api.massive.com/') and 'sample' not in url and bearer == 'sample'
        return json.dumps({'status': 'OK', 'results': [item, {'error': 'NOT_ENTITLED', 'ticker': 'I:VIX'}]}).encode()
    monkeypatch.setattr(quotes, '_read', fetch)
    result = quotes.fetch_cash_index_context(now=NOW)
    assert result['source']['status'] == 'partial' and lookup(cash=result['rows'])['RUT']['status'] == 'delayed'
    assert lookup(Stream(), cash=result['rows'])['RUT']['basis'] == 'option_implied_reference'
    item['timeframe'] = 'REAL-TIME'
    result = quotes.fetch_cash_index_context(now=NOW)
    rut = lookup(Stream(), cash=result['rows'])['RUT']
    assert rut['source'] == 'Massive' and rut['price'] == 2025. and rut['change_pct'] == pytest.approx(1.25)
    assert lookup(cash=result['rows'], now=NOW + timedelta(minutes=2))['RUT']['status'] == 'stale'
    item['last_updated'] += 10**9
    assert not quotes.fetch_cash_index_context(now=NOW)['rows']


def test_missing_key_never_contacts_massive(monkeypatch):
    monkeypatch.setattr(quotes, 'event_massive_key', lambda: None)
    monkeypatch.setattr(quotes, '_read', lambda *args, **kwargs: pytest.fail('unexpected request'))
    assert quotes.fetch_cash_index_context(now=NOW)['source']['status'] == 'not_configured'


def test_dedicated_massive_key_aliases_and_placeholders(monkeypatch):
    for name in ('MASSIVE_API_KEY', 'Massive_API', 'POLYGON_API_KEY'):
        monkeypatch.setenv(name, 'YOUR_POLYGON_API_KEY')
    assert quotes.event_massive_key() is None
    monkeypatch.setenv('Massive_API', 'legacy')
    assert quotes.event_massive_key() == 'legacy'
    monkeypatch.setenv('MASSIVE_API_KEY', 'standard')
    assert quotes.event_massive_key() == 'standard'
    monkeypatch.setenv('MASSIVE_API_KEY', 'db-wrong-service')
    monkeypatch.setenv('Massive_API', 'YOUR_MASSIVE_API_KEY')
    monkeypatch.setenv('POLYGON_API_KEY', 'earlier-provider-name')
    assert quotes.event_massive_key() == 'earlier-provider-name'


def test_delayed_response_cannot_be_promoted_to_current():
    item = dict(ticker='I:RUT', type='indices', timeframe='REAL-TIME', market_status='open',
                value=2000, last_updated=int(NOW.timestamp()) * 10 ** 9)
    result = quotes._cash_rows({'status': 'DELAYED', 'results': [item]}, {'I:RUT'}, NOW)
    assert result['RUT']['status'] == 'delayed'
