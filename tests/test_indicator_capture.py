from datetime import datetime, timedelta, timezone
import json
import sqlite3
import threading

import pytest
from fastapi.testclient import TestClient

from backend.equity_context import EquityContextTracker
from backend.indicator_capture import IndicatorCapture, load_capture_config
from backend.indicator_bars import read_indicator_bars
from backend.market_research_service import MarketResearchService
from backend.research_api import create_research_app
from tools.run_forecast_research import build_service
from app.services.observed_indicators import indicator_surface

OPEN = datetime(2026, 10, 1, 13, 30, tzinfo=timezone.utc)
NOW = OPEN + timedelta(minutes=60)


def bars(start=OPEN, end=NOW, symbols=("SPY", "QQQ")):
    return [dict(symbol=s, ts_event=start+timedelta(minutes=i), open=100., high=102.,
                 low=99., close=100.+(i%10)/10, volume=10.)
            for i in range(int((end-start).total_seconds()/60)) for s in symbols]


def test_denied_then_recovered_request_includes_open_and_uses_retrieval_time(tmp_path):
    calls=[]
    complete=NOW+timedelta(minutes=30, seconds=8)
    def fetch(**kw):
        calls.append(kw)
        if len(calls)==1:
            raise RuntimeError("403 license_not_found_unauthorized credential-must-not-appear")
        return bars(end=complete.replace(second=0))
    tracker=EquityContextTracker(symbols=("SPY","QQQ"), enabled=True,
        db_path=tmp_path/'equity_context.db', fetcher=fetch, clock=lambda:complete)
    assert tracker.poll_once(NOW)==0
    assert tracker.poll_once(NOW+timedelta(minutes=1))==0
    assert len(calls)==1
    assert "credential" not in json.dumps(tracker.snapshot(NOW))
    assert tracker.poll_once(NOW+timedelta(minutes=30))==180
    assert calls[-1]['start']==OPEN.isoformat()
    assert tracker.history('SPY')[0]['observed_at_utc']==complete.isoformat()
    result=read_indicator_bars(tracker.db_path,'SPX',now=complete)
    frame,provenance=indicator_surface('SPX',result,now=complete)
    assert provenance['status']=='available' and len(frame)==90
    assert all(frame.iloc[-1][name]==frame.iloc[-1][name] for name in
        ('RSI','MACD','Signal_Line','SMA_20','Momentum','VWAP','AMA'))


def test_storage_failure_does_not_publish_or_skip_retry(tmp_path,monkeypatch):
    tracker=EquityContextTracker(symbols=['SPY'],db_path=tmp_path/'bars.db')
    persist=tracker._persist
    def fail(rows):raise sqlite3.OperationalError('locked')
    monkeypatch.setattr(tracker,'_persist',fail)
    with pytest.raises(sqlite3.OperationalError):tracker.ingest(bars(symbols=['SPY']),observed_at=NOW)
    assert tracker.history('SPY')==[] and tracker.snapshot(NOW)['accepted_bar_count']==0
    monkeypatch.setattr(tracker,'_persist',persist)
    assert tracker.ingest(bars(symbols=['SPY']),observed_at=NOW)==60
    assert read_indicator_bars(tracker.db_path,'SPX',now=NOW)['status']=='available'


def test_mid_session_gap_and_next_day_are_requested_from_first_missing_minute(tmp_path):
    calls=[]
    tracker=EquityContextTracker(symbols=['SPY'],enabled=True,db_path=tmp_path/'bars.db',
        fetcher=lambda **kw:calls.append(kw) or [],clock=lambda:NOW)
    rows=bars(symbols=['SPY'])
    tracker.ingest(rows[:10]+rows[11:],observed_at=NOW)
    tracker.poll_once(NOW)
    assert calls[0]['start']==(OPEN+timedelta(minutes=10)).isoformat()
    tracker.poll_once(NOW+timedelta(days=1))
    assert calls[1]['start']==(OPEN+timedelta(days=1)).isoformat()


def test_config_and_preflight_construct_without_capture_or_database_creation(tmp_path):
    config=tmp_path/'runtime.json'
    assert not load_capture_config(config)['enabled']
    config.write_text(json.dumps({'indicator_capture':{'enabled':True,'symbols':['SPY','QQQ']}}))
    source=tmp_path/'source.db';source.touch()
    service=build_service(source,tmp_path/'journal.db',indicator_config=load_capture_config(config))
    assert service.indicator_tracker.enabled
    assert service.equity_tracker is None
    assert not service.indicator_tracker.snapshot(NOW)['running']
    assert not (tmp_path/'equity_context.db').exists()
    assert not list(tmp_path.glob('*.lock'))


@pytest.mark.parametrize('value',[{'enabled':'true'},{'enabled':True,'symbols':['SPX']},
    {'enabled':True,'symbols':['SPY','SPY']},{'enabled':True,'symbols':[]}])
def test_config_rejects_invalid_capture_scope(tmp_path,value):
    path=tmp_path/'runtime.json';path.write_text(json.dumps({'indicator_capture':value}))
    with pytest.raises(ValueError):load_capture_config(path)


def test_second_capture_cannot_write_until_first_worker_has_exited(tmp_path):
    class WaitingCapture(IndicatorCapture):
        def poll_once(self,now=None):self._stop.wait(.1)
    first=WaitingCapture(symbols=['SPY'],enabled=True,db_path=tmp_path/'bars.db')
    second=WaitingCapture(symbols=['SPY'],enabled=True,db_path=tmp_path/'bars.db')
    try:
        assert first.start()
        assert not second.start()
        assert second.snapshot(NOW)['status']=='INDICATOR_CAPTURE_WRITER_UNAVAILABLE'
        first.stop()
        assert second.start()
        assert second.snapshot(NOW)['status'] == 'COLLECTING'
    finally:first.stop();second.stop()


def test_lifespan_owns_display_collector_and_gets_never_poll(tmp_path,monkeypatch):
    class Spy:
        def __init__(self):self.starts=0;self.stops=0
        def start(self):self.starts+=1
        def stop(self):self.stops+=1
        def snapshot(self,now=None):return {'enabled':True,'status':'ENTITLEMENT_UNAVAILABLE','running':True}
        def poll_once(self,*a,**kw):raise AssertionError('GET must not contact provider')
        def forecast_history(self,*a,**kw):raise AssertionError('Display bars must not enter forecast histories')
    spy=Spy()
    service=MarketResearchService(tmp_path/'source.db',tmp_path/'journal.db',indicator_tracker=spy)
    monkeypatch.setattr(service,'_run',lambda:service._stop.wait(5))
    with TestClient(create_research_app(service)) as client:
        assert spy.starts==1 and service.equity_tracker is None
        result=client.get('/v1/research/indicator-bars?symbol=SPX').json()
        assert result['reason_code']=='ENTITLEMENT_UNAVAILABLE'
        assert result['bar_reason_code']=='RETAINED_BAR_DATABASE_MISSING'
        assert client.get('/health').json()['indicator_capture']['status']=='ENTITLEMENT_UNAVAILABLE'
        assert not (tmp_path/'equity_context.db').exists()
    assert spy.stops==1
