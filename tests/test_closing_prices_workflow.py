from datetime import date, datetime, timezone
import hashlib
import json
from pathlib import Path
from app.services.closing_prices import collect_closing_prices, latest_close_report


def test_prepublication_saves_pending_without_network(tmp_path):
    def no_fetch(**kwargs):
        raise AssertionError('premature fetch')
    report,path = collect_closing_prices(['SPX','NDX','QQQ'],date(2026,9,14),tmp_path,
        now=datetime(2026,9,14,20,30,tzinfo=timezone.utc),fetch=no_fetch)
    assert [r['status'] for r in report['records']] == ['AWAITING_PUBLICATION','AWAITING_PUBLICATION','UNSUPPORTED_SOURCE']
    assert path.is_file() and not report['complete']
    assert latest_close_report('2026-09-14',tmp_path)==report


def fake_fetch(root):
    def fetch(**kwargs):
        if kwargs['symbol'] != 'SPX':
            raise RuntimeError('source unavailable')
        raw=b'DATE,SPX\n09/11/2026,6500.25\n'
        digest=hashlib.sha256(raw).hexdigest()
        p=root/'data/verified_close_sources/2026-09-11/SPX'/f'{digest}.csv'
        p.parent.mkdir(parents=True,exist_ok=True)
        p.write_bytes(raw)
        return {'source_artifact_path':str(p),'source_reference':kwargs['source_reference'],
                'source_artifact_sha256':digest,'retrieved_at_utc':'2026-09-12T00:00:00+00:00'}
    return fetch


def test_partial_collection_and_tamper_detection(tmp_path):
    report,path=collect_closing_prices(['NDX','SPX'],date(2026,9,11),tmp_path,
        now=datetime(2026,9,12,tzinfo=timezone.utc),fetch=fake_fetch(tmp_path))
    assert [r['status'] for r in report['records']]==['SOURCE_UNAVAILABLE','OFFICIAL_ARTIFACT_SAVED']
    assert report['records'][1]['official_close']==6500.25
    assert not report['ledger_ingested'] and not report['scoring_performed']
    assert latest_close_report('2026-09-11',tmp_path)['records'][1]['status']=='OFFICIAL_ARTIFACT_SAVED'
    Path(report['records'][1]['source_artifact_path']).write_text('altered')
    reread=latest_close_report('2026-09-11',tmp_path)
    assert reread['records'][1]['status']=='ARTIFACT_INVALID'
    assert reread['records'][1]['official_close'] is None
    assert json.loads(path.read_text())==report  # original receipt remains immutable


def test_retries_append_and_do_not_replace(tmp_path):
    kw=dict(symbols=['SPX'],trading_day=date(2026,9,11),root=tmp_path,
            now=datetime(2026,9,12,tzinfo=timezone.utc),fetch=fake_fetch(tmp_path))
    first,p1=collect_closing_prices(**kw)
    second,p2=collect_closing_prices(**kw)
    assert p1!=p2 and p1.is_file() and p2.is_file()
    assert first['complete'] and second['complete']


def test_weekend_is_not_a_close_session(tmp_path):
    result,_=collect_closing_prices(['SPX'],date(2026,9,12),tmp_path,
                                    now=datetime(2026,9,13,tzinfo=timezone.utc))
    assert result['records'][0]['status']=='NO_VERIFIED_SESSION'
