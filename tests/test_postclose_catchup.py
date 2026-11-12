from datetime import date, datetime, timezone
import json
from pathlib import Path

import pytest
import tools.run_verified_close_postclose as runner


def fixtures(root, monkeypatch, days, complete=()):
    for day in days:
        (root/'data'/'prediction_score_selections'/day).mkdir(parents=True)
    monkeypatch.setattr(runner, 'load_prediction_score_selection', lambda root, day: {'receipt_sha256':day.isoformat()})
    def status(root, day):
        if day.isoformat() not in complete:
            return {'settlement':{'status':'PENDING'}}
        path = root/(day.isoformat()+'.json')
        path.write_text(json.dumps({'trading_date':day.isoformat(),'selection_receipt_sha256':day.isoformat()}))
        return {'settlement':{'status':'COMPLETE','path':str(path)}}
    monkeypatch.setattr(runner, 'latest_pipeline_status', status)
    calls = []
    monkeypatch.setattr(runner, 'run_settlement', lambda root, day, **kw: calls.append(day) or {'settled':day.isoformat()})
    return calls


def test_morning_retry_uses_previous_frozen_selection_and_never_creates_one(tmp_path, monkeypatch):
    calls = fixtures(tmp_path, monkeypatch, ['2026-10-01', '2026-09-30'])
    now = datetime(2026,10,2,11,30,tzinfo=timezone.utc)
    assert runner.run_pending_settlement(tmp_path,date(2026,10,2),observed_at_utc=now)['settled']=='2026-10-01'
    assert calls == [date(2026,10,1)]
    assert not (tmp_path/'data'/'prediction_score_selections'/'2026-10-02').exists()


def test_evening_prefers_today_and_respects_cutoff(tmp_path, monkeypatch):
    calls=fixtures(tmp_path,monkeypatch,['2026-10-01','2026-10-02'])
    runner.run_pending_settlement(tmp_path,date(2026,10,2),observed_at_utc=datetime(2026,10,2,21,tzinfo=timezone.utc))
    assert calls == [date(2026,10,1)]
    runner.run_pending_settlement(tmp_path,date(2026,10,2),observed_at_utc=datetime(2026,10,2,23,tzinfo=timezone.utc))
    assert calls[-1] == date(2026,10,2)


def test_completed_date_skipped_and_lookback_is_bounded(tmp_path,monkeypatch):
    calls=fixtures(tmp_path,monkeypatch,['2026-10-01','2026-09-20'],complete=['2026-10-01'])
    report=runner.run_pending_settlement(tmp_path,date(2026,10,2),observed_at_utc=datetime(2026,10,2,11,tzinfo=timezone.utc))
    assert not calls and report['status']=='NO_PENDING_PUBLISHED_SELECTION'


def test_corrupt_receipt_is_not_silently_retried(tmp_path,monkeypatch):
    calls=fixtures(tmp_path,monkeypatch,['2026-10-01'])
    monkeypatch.setattr(runner,'latest_pipeline_status',lambda *a:{'settlement':{'status':'INVALID'}})
    with pytest.raises(RuntimeError,match='Invalid existing'):
        runner.run_pending_settlement(tmp_path,date(2026,10,2),observed_at_utc=datetime(2026,10,2,11,tzinfo=timezone.utc))
    assert not calls


@pytest.mark.parametrize('field,value', [
    ('trading_date', '2026-09-30'),
    ('selection_receipt_sha256', 'different-selection'),
])
def test_completed_receipt_must_match_date_and_frozen_selection(tmp_path, monkeypatch, field, value):
    calls = fixtures(tmp_path, monkeypatch, ['2026-10-01'])
    payload = {'trading_date': '2026-10-01', 'selection_receipt_sha256': '2026-10-01'}
    payload[field] = value
    path = tmp_path / 'mismatched.json'
    path.write_text(json.dumps(payload))
    monkeypatch.setattr(runner, 'latest_pipeline_status', lambda *a: {
        'settlement': {'status': 'COMPLETE', 'path': str(path)},
    })
    with pytest.raises(RuntimeError, match='selection binding mismatch'):
        runner.run_pending_settlement(
            tmp_path, date(2026, 10, 2),
            observed_at_utc=datetime(2026, 10, 2, 11, tzinfo=timezone.utc),
        )
    assert not calls
