from datetime import datetime
import json

import pytest

from backend import databento_streamer as module
from backend.post_close_quotes import append_quote_sample


def test_capture_excludes_preclose_stale_future_and_wrong_generation(tmp_path):
    window = module.live_subscription_window(datetime.fromisoformat('2026-09-16T20:05:00+00:00'))
    now = int(datetime.fromisoformat(window['observed_at_utc']).timestamp() * 1e9)
    good = {'generation': 2, 'ts_event_ns': now-1_000_000_000,
            'ts_recv_ns': now-500_000_000, 'bid': 1., 'ask': 2.}
    quotes = {'good': good, 'old': {**good, 'ts_event_ns': now-600_000_000_000},
              'stale': {**good, 'ts_event_ns': now-40_000_000_000, 'ts_recv_ns': now-40_000_000_000},
              'future': {**good, 'ts_recv_ns': now+1},
              'generation': {**good, 'generation': 1}}
    for _ in range(2):
        result = append_quote_sample(tmp_path, window, quotes, epoch='a'*64, generation=2)
        assert result['quote_count'] == 1
        assert result['excluded_quote_count'] == 4
        assert result['usable_for_prediction'] is False
    assert len((tmp_path/'2026-09-16.ndjson').read_text().splitlines()) == 2
    assert json.loads((tmp_path/'2026-09-16.ndjson').read_text().splitlines()[0])['quotes'][0]['symbol'] == 'good'


def test_post_close_never_calculates_or_publishes(monkeypatch):
    streamer = module.DatabentoGammaStreamer(['SPX'])
    calls = []
    monkeypatch.setattr(module, 'market_is_closed', lambda: True)
    monkeypatch.setattr(streamer, '_capture_post_close_quotes', lambda: calls.append('research'))
    monkeypatch.setattr(streamer, '_calculate_pin', lambda *a, **k: pytest.fail('post-close production calculation'))
    streamer._compute_and_publish()
    assert calls == ['research']


def test_collection_does_not_extend_cash_prediction_window():
    window = module.live_subscription_window(datetime.fromisoformat('2026-09-16T20:05:00+00:00'))
    assert window['subscription_allowed'] is True
    assert window['prediction_session_allowed'] is False
    assert window['cash_close_utc'] == '2026-09-16T20:00:00+00:00'
    assert window['collection_close_utc'] == '2026-09-16T20:15:00+00:00'


def test_closed_window_cannot_write(tmp_path):
    window = module.live_subscription_window(datetime.fromisoformat('2026-09-16T20:15:00+00:00'))
    with pytest.raises(ValueError):
        append_quote_sample(tmp_path, window, {}, epoch='a'*64, generation=1)
    assert not list(tmp_path.iterdir())
