"""Synthetic hash-bound passports; no provider access or market-artifact mutations."""
from copy import deepcopy
import hashlib
import json

import pytest
import requests

from app.services.diagnostic_forecast_binding import (
    collect_bindings, fetch_passport, verify_binding, verified_forecast_claims, MAX_PASSPORT_BYTES,
)
from app.services.diagnostic_findings import derive_findings, render_selection

STAMP = '2026-09-25T17:00:02+00:00'


@pytest.fixture(autouse=True)
def isolate_governance_config(tmp_path,monkeypatch):
    from backend import governance_credentials
    monkeypatch.setattr(governance_credentials,'ROOT',tmp_path)


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()).hexdigest()


def seal(p):
    p['record_sha256'] = digest({k:v for k,v in p.items() if k not in ('record_sha256','created_at_utc','outcome')})
    return p


def fixture():
    origin = {'schema_version': 'prediction-passport-v1', 'origin_kind': 'prediction_snapshot', 'origin_key': '42'}
    p = {'schema_version': 'prediction-passport-v1', 'forecast_id': digest(origin),
         'origin': {**origin, 'prediction_snapshot_id': 42}, 'symbol': 'SPX',
         'state': 'RESEARCH_ONLY', 'decision_grade': False,
         'target': {'kind':'official_cash_close','trading_date':'2026-09-25',
                    'prediction_timestamp_utc':'2026-09-25T17:00:01+00:00',
                    'target_timestamp_utc':'2026-09-25T20:00:00+00:00','horizon_seconds':10799},
         'provenance': {'calculation_id':'calc42','subscription_epoch_id':'a'*64,'subscription_generation':1,
                        'quote_timestamp_utc':'2026-09-25T17:00:00+00:00'},
         'model': {'model_version':'v1','model_type':'research','model_artifact_sha256':'b'*64},
         'prediction': {'point_estimate':7750.5,'reference_price':7740.0},
         'quality': {'source_validation_is_valid':True,'data_age_seconds':1,'quote_age_seconds':1},
         'created_at_utc': STAMP}
    seal(p)
    d = {'symbol':'SPX','forecast_id':p['forecast_id'],'prediction_snapshot_id':42,
         'subscription_epoch_id':'a'*64,'subscription_generation':1,'current_price':7740.0,
         'source_as_of_utc':'2026-09-25T17:00:00Z','forecast_state':'RESEARCH_ONLY',
         'pin_payload':{'calculation_id':'calc42','spot_source':'databento_opra_put_call_parity'},
         'prediction':{'model_version':'v1','predicted_close':7750.5,'timestamp':'2026-09-25T17:00:01Z'},
         'health':{'is_stale':False,'stale_after_seconds':10}}
    return d,p


def test_matching_identity_and_numeric_claims():
    d,p = fixture()
    b = verify_binding('SPX',d,p,STAMP)
    assert b['status'] == 'BOUND'
    assert b['source_age_seconds'] == 2
    assert b['current_use'] == 'WITHIN_REPORTED_FRESHNESS_LIMIT'
    assert b['calibration_status'] == 'UNAVAILABLE'
    claim = next(c for c in verified_forecast_claims(p) if c['claim_code']=='POINT_VS_REFERENCE')
    assert '7750.5' in claim['statement'] and '7740' in claim['statement']
    assert claim['evidence_paths'] == ['prediction.point_estimate','prediction.reference_price']


@pytest.mark.parametrize('path,value', [
    (('forecast_id',),'c'*64),(('symbol',),'NDX'),(('prediction_snapshot_id',),43),
    (('pin_payload','calculation_id'),'other'),(('prediction','model_version'),'v2'),
    (('subscription_generation',),2),(('subscription_epoch_id',),'c'*64),
    (('prediction','predicted_close'),9999),(('current_price',),9000),
    (('source_as_of_utc',),'2026-09-25T16:00:00Z'),
    (('prediction','timestamp'),'2026-09-25T16:00:00Z'),
    (('prediction','target_timestamp_utc'),'2026-09-25T19:00:00Z'),
    (('prediction','forecast_id'),'c'*64),
    (('prediction','current_price'),9999), (('prediction','horizon_seconds'),300),
    (('pin_payload','subscription_generation'),2),
])
def test_mismatch_never_renders_numbers(path,value):
    d,p=fixture()
    target=d
    for k in path[:-1]: target=target[k]
    target[path[-1]]=value
    assert verify_binding('SPX',d,p,STAMP)['status']=='IDENTITY_MISMATCH'


def test_missing_identity_tampering_and_wrong_horizon():
    d,p=fixture()
    del d['pin_payload']['calculation_id']
    assert verify_binding('SPX',d,p,STAMP)['status']=='BINDING_EVIDENCE_MISSING'
    d,p=fixture()
    p['prediction']['point_estimate']=9999
    assert verify_binding('SPX',d,p,STAMP)['status']=='MALFORMED_PASSPORT'
    d,p=fixture()
    p['target']['horizon_seconds']=300
    seal(p)
    assert verify_binding('SPX',d,p,STAMP)['status']=='IDENTITY_MISMATCH'


def test_bound_stale_is_not_identity_mismatch_or_fresh():
    d,p=fixture()
    b=verify_binding('SPX',d,p,'2026-09-25T17:02:00Z')
    assert b['status']=='BOUND' and b['current_use']=='STALE'
    del d['health']['stale_after_seconds']
    assert verify_binding('SPX',d,p,STAMP)['current_use']=='UNVERIFIED'


class Response:
    def __init__(self, code=200, body=None):
        self.status_code=code
        self.raw=json.dumps(body or {}).encode()
        self.closed=False
    def iter_content(self, chunk_size):
        yield self.raw
    def close(self): self.closed=True


@pytest.mark.parametrize('code,status',[(401,'AUTHORIZATION_DENIED'),(403,'AUTHORIZATION_DENIED'),
    (404,'PASSPORT_NOT_FOUND'),(503,'PASSPORT_SERVICE_UNAVAILABLE'),(302,'PASSPORT_HTTP_FAILURE')])
def test_http_failures_safe_and_no_redirect(code,status):
    response=Response(code,{'error':'private backend error'})
    def get(url,**kwargs):
        assert kwargs['allow_redirects'] is False and kwargs['stream'] is True
        return response
    r,p=fetch_passport('a'*64,get=get,token='secret-test-token-123')
    assert r['outcome']==status and p is None and response.closed
    assert 'secret' not in json.dumps(r) and 'private' not in json.dumps(r)


def test_missing_auth_timeout_size_and_malformed():
    assert fetch_passport('a'*64,token='')[0]['outcome']=='AUTHORIZATION_MISSING'
    def timeout(*a,**k): raise requests.Timeout('secret')
    assert fetch_passport('a'*64,get=timeout,token='secret-test-token-123')[0]['outcome']=='PASSPORT_TIMEOUT'
    response=Response(body={'huge':'x'*MAX_PASSPORT_BYTES})
    assert fetch_passport('a'*64,get=lambda *a,**k:response,token='secret-test-token-123')[0]['outcome']=='PASSPORT_SIZE_LIMIT'
    response=Response(body={'not_passport':1})
    assert fetch_passport('a'*64,get=lambda *a,**k:response,token='secret-test-token-123')[0]['outcome']=='MALFORMED_PASSPORT'


def test_exact_id_read_and_dedup_no_latest_fallback():
    d,p=fixture()
    calls=[]
    def fetch(fid):
        calls.append(fid)
        return {'outcome':'RETRIEVED'},p
    result=collect_bindings({'SPX':d,'NDX':{**d,'symbol':'NDX'}},STAMP,fetch=fetch)
    assert calls==[p['forecast_id']]
    assert result['SPX']['status']=='BOUND'
    assert result['NDX']['status']=='IDENTITY_MISMATCH'
    newer={**d,'forecast_id':'c'*64}
    assert collect_bindings({'SPX':newer},STAMP,fetch=fetch)['SPX']['status']=='IDENTITY_MISMATCH'


def packet(d,p):
    return {'session_date_ct':'2026-09-25','evidence':{
        'runtime':{},'live_observations':{'samples':[{'observed_at_utc':STAMP,'dashboards':{'SPX':d}}]},
        'forecast_bindings':{'SPX':{'status':'BOUND','passport':p}}}}


def test_revalidates_packet_binding_and_keeps_operational_review():
    d,p=fixture()
    value=packet(d,p)
    facts=derive_findings(value)
    rendered=render_selection({'finding_ids':[],'investigation_ids':[]},facts)
    assert any('7750.5' in c['finding'] for c in rendered['forecasts'])
    assert any('unavailable' in c['finding'] for c in rendered['forecasts'])
    p['prediction']['point_estimate']=9999
    facts=derive_findings(value)
    assert 'FORECAST_UNBOUND:SPX' in facts and 'RUNTIME_UNAVAILABLE' in facts
    assert not any('9999' in c['finding'] for c in facts.values())
    value['evidence']['forecast_bindings']['SPX']['passport']=None
    assert 'FORECAST_UNBOUND:SPX' in derive_findings(value)


def test_selected_session_mismatch_is_unbound():
    d,p=fixture()
    value=packet(d,p)
    value['session_date_ct']='2026-09-24'
    assert 'FORECAST_UNBOUND:SPX' in derive_findings(value)


def test_v3_ui_renders_verified_paths():
    from streamlit.testing.v1 import AppTest
    d,p=fixture()
    body=render_selection({'finding_ids':[],'investigation_ids':[]},derive_findings(packet(d,p)))
    value={'schema_version':'diagnostic-review-v3','review':body}
    app=AppTest.from_string('from app.services.diagnostic_agent import render_review_content\n'
         +f'render_review_content({value!r})').run()
    assert not app.exception
    assert any('Verified paths:' in v.value for v in app.caption)


def test_authenticated_api_envelope_round_trip(monkeypatch):
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    from backend.api.routers import passports as router
    from backend.api.schemas import ForecastPassportV1
    d,p=fixture()
    p.update(prediction_mode='backend_lifecycle',drivers=[],feature_snapshot={})
    p['model']['formula_versions']={}
    p['prediction'].update(confidence_scale='unit_interval',confidence_kind='heuristic',
                           baseline={'name':'persistence','value':7740.0})
    p['quality'].update(validation_status='valid',regime='unknown',state_reasons=[],missing_evidence=[])
    core=ForecastPassportV1.model_validate({k:v for k,v in p.items() if k not in ('record_sha256','created_at_utc')}).model_dump()
    p=seal({**core,'created_at_utc':STAMP})
    monkeypatch.setenv('MARKETPIN_GOVERNANCE_ACCESS_TOKEN','synthetic-governance-credential')
    monkeypatch.setattr(router,'read_prediction_passport',lambda fid: deepcopy(p))
    api=FastAPI()
    api.include_router(router.router)
    client=TestClient(api,client=('127.0.0.1',50000))
    def get(url,**kwargs):
        result=client.get('/v1/workstation/passports/'+p['forecast_id'],headers=kwargs['headers'])
        return Response(result.status_code,result.json())
    receipt,retained=fetch_passport(p['forecast_id'],get=get)
    assert receipt['outcome']=='RETRIEVED'
    assert retained==p
    assert verify_binding('SPX',d,retained,STAMP)['status']=='BOUND'


def test_successful_v3_review_retains_exact_binding_and_receipt(tmp_path):
    from types import SimpleNamespace
    from app.services.diagnostic_agent import diagnose
    from app.services.diagnostic_history import load_reviews
    d,p=fixture()
    value=packet(d,p)
    raw=json.dumps({'finding_ids':['PASSPORT_POINT_VS_REFERENCE:SPX'],'investigation_ids':[]})
    response=SimpleNamespace(status='completed',model='test-model',output=[],output_text=raw)
    submitted=[]
    def create(**kwargs):
        submitted.append(kwargs)
        return response
    client=SimpleNamespace(responses=SimpleNamespace(create=create))
    review=diagnose('Explain this exact forecast',value,client=client,root=tmp_path)
    assert len(submitted)==1
    assert json.loads(submitted[0]['input'])['packet']==value
    assert review['schema_version']=='diagnostic-review-v3'
    assert load_reviews(tmp_path,'2026-09-25')==[review]
    assert review['response_output_text']==raw
    receipts=[json.loads(path.read_text()) for path in (tmp_path/'data/diagnostic_receipts/2026-09-25').glob('*.json')]
    saved=next(r for r in receipts if r['outcome']=='SAVED')
    assert saved['binding_version']=='diagnostic-binding-v3' and saved['review_path']


def test_lifecycle_revision_can_bind_without_snapshot_but_preserves_identity_checks():
    d,p=fixture()
    p['origin'].update(origin_kind='lifecycle_revision',origin_key='d'*64,prediction_snapshot_id=None)
    p['forecast_id']=digest({k:p['origin'][k] for k in ('schema_version','origin_kind','origin_key')})
    d.update(forecast_id=p['forecast_id'],prediction_snapshot_id=None)
    seal(p)
    assert verify_binding('SPX',d,p,STAMP)['status']=='BOUND'
    d['prediction']['prediction_snapshot_id']=42
    assert verify_binding('SPX',d,p,STAMP)['status']=='IDENTITY_MISMATCH'
    d['prediction'].pop('prediction_snapshot_id')
    d['subscription_generation']=2
    assert verify_binding('SPX',d,p,STAMP)['status']=='IDENTITY_MISMATCH'


def test_independent_source_hash_binds_lifecycle_without_calculation_id():
    d,p=fixture()
    p['origin'].update(origin_kind='lifecycle_revision',origin_key='d'*64,prediction_snapshot_id=None)
    p['forecast_id']=digest({k:p['origin'][k] for k in ('schema_version','origin_kind','origin_key')})
    d.update(forecast_id=p['forecast_id'],prediction_snapshot_id=None,forecast_source_payload_sha256='f'*64)
    d['pin_payload'].pop('calculation_id')
    p['provenance'].update(calculation_id=None,source_payload_sha256='f'*64)
    seal(p)
    assert verify_binding('SPX',d,p,STAMP)['status']=='BOUND'
    d['forecast_source_payload_sha256']='e'*64
    assert verify_binding('SPX',d,p,STAMP)['status']=='IDENTITY_MISMATCH'
    d.pop('forecast_source_payload_sha256')
    assert verify_binding('SPX',d,p,STAMP)['status']=='BINDING_EVIDENCE_MISSING'


@pytest.mark.parametrize('kind,key,snapshot,expected',[
    ('prediction_snapshot','42',None,'BINDING_EVIDENCE_MISSING'),
    ('lifecycle_revision','bad',None,'MALFORMED_PASSPORT'),
    ('lifecycle_revision','d'*64,42,'IDENTITY_MISMATCH'),
    ('unknown','d'*64,None,'UNSUPPORTED_ORIGIN'),
])
def test_origin_contracts_do_not_allow_blanket_missing_ids(kind,key,snapshot,expected):
    d,p=fixture()
    p['origin'].update(origin_kind=kind,origin_key=key,prediction_snapshot_id=snapshot)
    p['forecast_id']=digest({k:p['origin'][k] for k in ('schema_version','origin_kind','origin_key')})
    d.update(forecast_id=p['forecast_id'],prediction_snapshot_id=snapshot)
    seal(p)
    assert verify_binding('SPX',d,p,STAMP)['status']==expected


def test_missing_credential_reason_visible_and_secret_payload_rejected():
    d,p=fixture()
    value=packet(d,p)
    value['evidence']['forecast_bindings']['SPX']={'status':'AUTHORIZATION_MISSING'}
    assert 'no configured passport access credential' in derive_findings(value)['FORECAST_UNBOUND:SPX']['finding']
    response=Response(body={'passport':{'secret':'synthetic-governance-credential'}})
    r,p=fetch_passport('a'*64,get=lambda *a,**k:response,token='synthetic-governance-credential')
    assert r['outcome']=='UNSAFE_PASSPORT_CONTENT' and p is None
    assert 'synthetic-governance-credential' not in json.dumps(r)
