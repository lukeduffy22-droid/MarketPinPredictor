"""Retained bars -> read-only API -> indicators -> rendered ETF context."""
from copy import deepcopy
from datetime import datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace
from io import StringIO

import pandas as pd
import pytest
import requests
from fastapi.testclient import TestClient
from streamlit.testing.v1 import AppTest

from backend.equity_context import EquityContextTracker
from backend.indicator_bars import read_indicator_bars, validate_bars
from backend.research_api import create_research_app
from app.services.observed_indicators import attach_indicators, fetch_indicator_bars, indicator_surface

NOW = datetime(2026, 10, 1, 14, 30, tzinfo=timezone.utc)
OPEN = NOW - timedelta(hours=1)


@pytest.fixture
def retained(tmp_path):
    path = tmp_path / 'equity_context.db'
    def forbidden(**kwargs):
        raise AssertionError('Indicator reads must not contact a provider')
    tracker = EquityContextTracker(symbols=('SPY', 'QQQ'), db_path=path, fetcher=forbidden)
    rows = [{'symbol': sym, 'ts_event': OPEN + timedelta(minutes=i), 'open': 100+i,
             'high': 101+i, 'low': 99+i, 'close': 100+i, 'volume': 10}
            for sym in ('SPY', 'QQQ') for i in range(60)]
    assert tracker.ingest(rows, observed_at=NOW) == 120
    return path


def test_retained_api_to_client_calculations_and_no_writes(retained, monkeypatch):
    before = hashlib.sha256(retained.read_bytes()).hexdigest()
    original = read_indicator_bars
    monkeypatch.setattr('backend.indicator_bars.read_indicator_bars',
                        lambda path, symbol: original(path, symbol, now=NOW))
    service = SimpleNamespace(source_database=retained.parent/'market_data.db', symbols=('SPX', 'NDX', 'SPY'))
    client = TestClient(create_research_app(service, manage_lifecycle=False))
    def request(url, *, params, timeout):
        assert timeout == 2
        assert url.endswith('/v1/research/indicator-bars')
        return client.get('/v1/research/indicator-bars', params=params)
    evidence = fetch_indicator_bars('SPX', requester=request)
    prediction = attach_indicators({'ticker': 'SPX', 'predicted_price': 7700, 'current_price': 7690}, evidence, now=NOW)
    frame = prediction['df']; p = prediction['indicator_provenance']
    assert prediction['indicators_available'] and p['bar_symbol'] == 'SPY' and p['is_proxy_context']
    assert prediction['predicted_price'] == 7700 and prediction['current_price'] == 7690
    latest = frame.iloc[-1]
    assert latest['close'] == 159 and latest['SMA_20'] == 149.5
    assert latest['RSI'] == 100 and latest['Momentum'] == 10 and latest['VWAP'] == 129.5
    # Independent scalar EMA recurrence; compare MACD and its signal warm-up.
    fast = slow = 100.; signal = None
    for i in range(1, 60):
        fast += (100+i-fast) * 2/13
        slow += (100+i-slow) * 2/27
        if i >= 25:
            signal = fast-slow if signal is None else signal + ((fast-slow)-signal)*2/10
    assert latest['MACD'] == pytest.approx(fast-slow)
    assert latest['Signal_Line'] == pytest.approx(signal)
    assert hashlib.sha256(retained.read_bytes()).hexdigest() == before
    assert client.get('/v1/research/indicator-bars', params={'symbol': 'UNKNOWN'}).status_code == 422


@pytest.mark.parametrize('change,code', [
    (lambda r: r.pop(10), 'BAR_HISTORY_GAPPED_OR_DUPLICATED'),
    (lambda r: r.append(deepcopy(r[-1])), 'BAR_HISTORY_GAPPED_OR_DUPLICATED'),
    (lambda r: r.pop(0), 'BAR_SESSION_OPEN_MISSING'),
    (lambda r: r[-1].update(symbol='SPX'), 'BAR_INSTRUMENT_MISMATCH'),
    (lambda r: r[-1].update(source_kind='option_implied_reference'), 'BAR_PROVENANCE_INVALID'),
    (lambda r: r[-1].update(source_revision='0'*64), 'BAR_REVISION_MISMATCH'),
    (lambda r: r[-1].update(close=float('nan')), 'BAR_VALUES_INVALID'),
    (lambda r: r[-1].update(volume=-1), 'BAR_VALUES_INVALID'),
    (lambda r: r[-1].update(observed_at_utc=(NOW+timedelta(seconds=1)).isoformat()), 'BAR_INCOMPLETE_OR_WRONG_SESSION'),
    (lambda r: r[-1].update(timestamp_utc=NOW.isoformat()), 'BAR_INCOMPLETE_OR_WRONG_SESSION'),
])
def test_bad_bars_fail_closed(retained, change, code):
    evidence = read_indicator_bars(retained, 'SPX', now=NOW)
    change(evidence['bars'])
    frame, provenance = indicator_surface('SPX', evidence, now=NOW)
    assert frame.empty and provenance['status'] == 'unavailable'
    assert provenance['reason_code'] == code


def test_stale_short_wrong_session_and_flat_prices(retained):
    evidence = read_indicator_bars(retained, 'SPX', now=NOW)
    with pytest.raises(ValueError, match='OBSERVED_BARS_STALE'):
        validate_bars('SPX', evidence['bars'], now=NOW+timedelta(minutes=4))
    with pytest.raises(ValueError, match='INDICATOR_LOOKBACK_INSUFFICIENT'):
        short = deepcopy(evidence['bars'][:20])
        for row in short:
            row['observed_at_utc'] = (OPEN+timedelta(minutes=20)).isoformat()
        validate_bars('SPX', short, now=OPEN+timedelta(minutes=20))
    # Availability times are preserved, so this earlier replay must also fail.
    with pytest.raises(ValueError):
        validate_bars('SPX', evidence['bars'], now=NOW-timedelta(days=1))


def test_missing_database_and_sqlite_failure_are_read_only(tmp_path):
    missing = tmp_path/'missing.db'
    assert read_indicator_bars(missing, 'SPX', now=NOW)['reason_code'] == 'RETAINED_BAR_DATABASE_MISSING'
    assert not missing.exists()
    missing.write_bytes(b'not sqlite')
    assert read_indicator_bars(missing, 'SPX', now=NOW)['reason_code'] == 'RETAINED_BAR_READ_UNAVAILABLE'
    assert missing.read_bytes() == b'not sqlite'


@pytest.mark.parametrize('failure,code', [(requests.Timeout('secret'), 'BAR_API_TIMEOUT'),
                                        (requests.ConnectionError('secret'), 'BAR_API_UNAVAILABLE')])
def test_transport_is_precise_and_sanitized(failure, code):
    def request(*args, **kwargs):
        raise failure
    evidence = fetch_indicator_bars('SPX', requester=request)
    assert evidence['reason_code'] == code and 'secret' not in json.dumps(evidence)


def test_denied_source_and_cross_symbol_response_never_enable_indicators(retained):
    evidence = read_indicator_bars(retained, 'SPX', now=NOW)
    denied = {**evidence, 'status': 'unavailable', 'reason_code': 'ENTITLEMENT_UNAVAILABLE'}
    frame, provenance = indicator_surface('SPX', denied, now=NOW)
    assert frame.empty and 'license' in provenance['reason']
    frame, provenance = indicator_surface('NDX', evidence, now=NOW)
    assert frame.empty and provenance['reason_code'] == 'BAR_RESPONSE_INVALID_OR_STALE'


def test_stored_metadata_mismatch_and_newer_bad_revision_are_not_hidden(retained):
    with sqlite3.connect(retained) as c:
        c.execute("UPDATE equity_context_bars SET source_revision=? WHERE symbol='SPY' AND timestamp_utc=?",
                  ('0'*64, OPEN.isoformat()))
    result = read_indicator_bars(retained, 'SPX', now=NOW)
    assert result['reason_code'] == 'BAR_STORED_METADATA_MISMATCH' and not result['bars']


def test_actual_batch_path_attaches_bars_without_changing_forecast_binding(retained, monkeypatch):
    from tests.test_sidebar_live_state import _fetch, _forecast_state, _health
    from app.services.live_data_client import build_databento_predictions_from_state_batch
    class FixedClock(datetime):
        @classmethod
        def now(cls, tz=None):
            return NOW
    monkeypatch.setattr('app.services.observed_indicators.datetime', FixedClock)
    monkeypatch.setattr('app.services.live_data_client._payload_age_seconds', lambda _: 5.)
    batch = _fetch({'SPX': _forecast_state()}, _health('SPX'))
    before, _ = build_databento_predictions_from_state_batch(batch, timeframe='1-day', stale_after_seconds=30.)
    calls = []
    def loader(symbol):
        calls.append(symbol)
        return read_indicator_bars(retained, symbol, now=NOW)
    after, _ = build_databento_predictions_from_state_batch(batch, timeframe='1-day', stale_after_seconds=30., indicator_loader=loader)
    assert calls == ['SPX'] and after['SPX']['indicators_available']
    for key in ('forecast_id', 'state_revision', 'predicted_price', 'source_as_of_utc', 'pin_payload'):
        assert after['SPX'][key] == before['SPX'][key]


def test_export_keeps_bar_identity_and_units_separate_from_index_forecast(retained):
    from tests.test_app_prediction_export import _load_export_to_csv
    evidence = read_indicator_bars(retained, 'SPX', now=NOW)
    prediction = attach_indicators({'ticker': 'SPX', 'current_price': 7690., 'predicted_price': 7700.}, evidence, now=NOW)
    exported = pd.read_csv(StringIO(_load_export_to_csv()({'S&P 500': prediction}, include_indicators=True)))
    assert set(exported['indicator_source_symbol']) == {'SPY'}
    assert set(exported['indicator_context_for_symbol']) == {'SPX'}
    assert set(exported['subscription_epoch_id']) == {evidence['bars'][0]['subscription_epoch_id']}
    assert set(exported['record_type']) == {'derived_indicators_from_observed_bars'}
    assert exported['predicted_price'].isna().all()


def test_streamlit_renders_etf_values_without_index_target_and_expires(retained, tmp_path):
    def script():
        import streamlit as st
        from app.services.observed_indicators import attach_indicators, render_observed_indicators
        p = attach_indicators({'ticker':'SPX', 'predicted_price':7700}, st.session_state.evidence, now=st.session_state.captured)
        render_observed_indicators(p, now=st.session_state.now)
    app = AppTest.from_function(script)
    app.session_state.evidence = read_indicator_bars(retained, 'SPX', now=NOW)
    app.session_state.captured = NOW
    app.session_state.now = NOW
    app.run(timeout=15)
    assert not app.exception
    assert [m.value for m in app.metric] == ['100.00', '6.87', '149.50', '10.00']
    assert any('SPY ETF context for SPX' in c.value for c in app.caption)
    assert not app.get('plotly_chart')  # No index forecast overlay.
    app.session_state.now = NOW+timedelta(minutes=4)
    app.run(timeout=15)
    assert not app.exception and not app.metric
    assert any('stale or invalid' in i.value for i in app.info)


def test_independent_indicator_panel_requires_explicit_load_and_expires(retained):
    def script():
        import streamlit as st
        from app.services.observed_indicators import render_indicator_panel
        def loader(symbol):
            st.session_state.calls.append(symbol)
            return st.session_state.evidence[symbol]
        render_indicator_panel(loader=loader, now=st.session_state.now)
    app = AppTest.from_function(script)
    app.session_state.evidence = {'SPX': read_indicator_bars(retained, 'SPX', now=NOW)}
    app.session_state.calls = []
    app.session_state.now = NOW
    app.run(timeout=15)
    assert not app.exception and app.session_state.calls == [] and not app.metric
    app.multiselect[0].set_value(['SPX'])
    app.button[0].click().run(timeout=15)
    assert not app.exception and len(app.metric) == 4 and app.session_state.calls == ['SPX']
    assert any('Adaptive moving average' in c.value for c in app.caption)
    app.run(timeout=15)
    assert app.session_state.calls == ['SPX']  # Rerender must never contact the provider.
    app.session_state.now = NOW+timedelta(minutes=4)
    app.run(timeout=15)
    assert not app.exception and not app.metric


def test_independent_indicator_panel_displays_license_failure_without_forecast(retained):
    def script():
        import streamlit as st
        from app.services.observed_indicators import render_indicator_panel
        render_indicator_panel(loader=lambda symbol: st.session_state.denied, now=st.session_state.now)
    app = AppTest.from_function(script)
    app.session_state.now = NOW
    app.session_state.denied = {**read_indicator_bars(retained, 'SPX', now=NOW),
                               'status': 'unavailable', 'reason_code': 'ENTITLEMENT_UNAVAILABLE'}
    app.run(timeout=15)
    app.multiselect[0].set_value(['SPX'])
    app.button[0].click().run(timeout=15)
    assert not app.exception and not app.metric
    assert any('license gate' in i.value for i in app.info)
