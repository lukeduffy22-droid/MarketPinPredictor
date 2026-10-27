import json
from types import SimpleNamespace as NS

import pytest
import requests

from app.services.diagnostic_agent import build_packet, diagnose
from app.services.diagnostic_history import load_reviews
from app.services.diagnostic_receipts import DiagnosticError
import app.services.diagnostic_runtime as runtime


def output():
    return {'finding_ids': ['RUNTIME_UNAVAILABLE'], 'investigation_ids': []}


def client(**overrides):
    response = dict(status='completed', model='actual-model', id='response-id',
                    output=[], output_text=json.dumps(output()))
    response.update(overrides)
    return NS(responses=NS(create=lambda **kwargs: NS(**response)))


@pytest.mark.parametrize('overrides,category', [
    ({'status': 'incomplete', 'output_text': '{', 'incomplete_details': NS(reason='max_output_tokens')}, 'RESPONSE_INCOMPLETE'),
    ({'status': 'failed'}, 'RESPONSE_NOT_COMPLETED'),
    ({'output': [NS(content=[NS(type='refusal')])], 'output_text': ''}, 'RESPONSE_REFUSED'),
    ({'output_text': ''}, 'EMPTY_OUTPUT'),
    ({'output_text': '{'}, 'INVALID_JSON'),
    ({'output_text': '{}'}, 'INVALID_SCHEMA'),
    ({'output_text': json.dumps({'finding_ids': ['invented'], 'investigation_ids': []})}, 'UNSUPPORTED_FACT_SELECTION'),
])
def test_failure_receipt_without_saved_review(tmp_path, overrides, category):
    packet = build_packet({}, {}, tmp_path)
    with pytest.raises(DiagnosticError) as error:
        diagnose('Review', packet, client(**overrides), root=tmp_path)
    assert error.value.category == category
    assert not load_reviews(tmp_path, packet['session_date_ct'])
    receipts = [json.loads(p.read_text()) for p in (tmp_path / 'data/diagnostic_receipts').glob('*/*.json')]
    assert {r['outcome'] for r in receipts} == {'STARTED', category}
    assert all(r['review_path'] is None for r in receipts)


def test_real_outcome_metadata_and_exact_inputs_saved(tmp_path):
    packet = build_packet({}, {}, tmp_path)
    review = diagnose('Review', packet, client(), model='requested-model', root=tmp_path)
    assert review['model'] == 'actual-model'
    assert load_reviews(tmp_path, packet['session_date_ct']) == [review]
    assert review['packet'] == packet
    assert review['response_output_text'] == json.dumps(output())
    receipts = [json.loads(p.read_text()) for p in (tmp_path / 'data/diagnostic_receipts').glob('*/*.json')]
    saved = next(r for r in receipts if r['outcome'] == 'SAVED')
    assert saved['requested_model'] == 'requested-model'
    assert saved['actual_model'] == 'actual-model'
    assert saved['review_path']
    from app.services.diagnostic_receipts import verified_saved_receipt
    retained = json.loads(__import__('pathlib').Path(saved['review_path']).read_text())
    assert verified_saved_receipt(retained, tmp_path) == saved
    assert retained['request_receipt']['outcome'] == 'VALIDATED'
    assert retained['request_receipt']['review_path'] is None
    __import__('pathlib').Path(saved['review_path']).write_text('{}')
    assert verified_saved_receipt(retained, tmp_path) is None


def test_provider_error_does_not_leak_body(tmp_path):
    class Denied(Exception):
        status_code = 403
    def create(**kwargs):
        raise Denied('sk-secret-private provider error body')
    with pytest.raises(DiagnosticError, match='PERMISSION_DENIED'):
        diagnose('Review', build_packet({}, {}, tmp_path), NS(responses=NS(create=create)), root=tmp_path)
    assert all('sk-secret' not in p.read_text() for p in tmp_path.rglob('*.json'))


def test_live_collection_failure_never_reuses_first_runtime(tmp_path, monkeypatch):
    calls = []
    def poll(path):
        calls.append(path)
        if len(calls) == 1:
            return {'status': 'OBSERVED', 'completed_at_utc': '2026-09-21T19:43:00+00:00'}, {
                'handoff_status': 'active', 'messages_received': 100, 'subscription_epoch_id': 'x', 'subscription_generation': 1}
        return {'status': 'UNAVAILABLE', 'error_kind': 'Timeout', 'path': path}, {}
    monkeypatch.setattr(runtime, 'poll_endpoint', poll)
    monkeypatch.setattr(runtime, 'persistence_observation', lambda root: {'status': 'UNAVAILABLE'})
    packet = runtime.collect_live_packet(['SPX'], {}, tmp_path, '2026-09-21', sleep=lambda _: None)
    assert packet['evidence']['runtime']['observed_at_utc'] is None
    assert 'handoff_status' not in packet['evidence']['runtime']
    assert packet['evidence']['live_observations']['message_delta'] is None
    assert len(packet['evidence']['collection_errors']) == 3


def test_endpoint_failure_and_allowlist():
    def fail(*args, **kwargs):
        raise requests.Timeout('secret-url')
    receipt, data = runtime.poll_endpoint('/health/live', get=fail)
    assert data == {} and receipt['error_kind'] == 'Timeout'
    assert 'secret-url' not in json.dumps(receipt)
    safe = runtime.safe_fields({'api_key': 'private', 'prediction': {'confidence': 0.5, 'secret': 'hidden'},
                                'validation_failure_reasons': ['sk-private failure']})
    assert safe == {'prediction': {'confidence': 0.5}, 'validation_failure_reasons': ['[REDACTED] failure']}


def test_persistence_is_read_only_and_reports_row_time(tmp_path, monkeypatch):
    import sqlite3
    monkeypatch.delenv('DATABASE_URL', raising=False)
    path = tmp_path / 'data/market_data.db'
    path.parent.mkdir()
    with sqlite3.connect(path) as c:
        c.execute('CREATE TABLE prediction_snapshots(symbol TEXT, timestamp_utc TEXT)')
        c.execute("INSERT INTO prediction_snapshots VALUES ('SPX', '2026-09-21T19:59:00')")
    before = path.read_bytes()
    result = runtime.persistence_observation(tmp_path)
    assert result['status'] == 'PARTIAL'
    assert result['tables']['prediction_snapshots']['SPX'] == '2026-09-21T19:59:00'
    assert before == path.read_bytes()


def test_save_failure_is_not_reported_as_success(tmp_path, monkeypatch):
    import app.services.diagnostic_agent as agent
    def fail(*args):
        raise OSError('private location')
    monkeypatch.setattr(agent, 'save_review', fail)
    with pytest.raises(DiagnosticError, match='REVIEW_SAVE_FAILED'):
        diagnose('Review', build_packet({}, {}, tmp_path), client(), root=tmp_path)
    receipts = [json.loads(p.read_text()) for p in (tmp_path / 'data/diagnostic_receipts').glob('*/*.json')]
    assert 'SAVED' not in {r['outcome'] for r in receipts}


def test_schema_binds_citations_before_generation(tmp_path):
    packet = build_packet({}, {}, tmp_path)
    def create(**kwargs):
        schema = kwargs['text']['format']['schema']
        from app.services.diagnostic_findings import derive_findings
        facts = derive_findings(packet)
        assert schema['properties']['finding_ids']['items']['enum'] == list(facts)
        assert schema['additionalProperties'] is False
        assert 'summary' not in schema['properties']
        return NS(status='completed', model='actual', output=[], output_text=json.dumps(output()))
    diagnose('Review', packet, NS(responses=NS(create=create)), root=tmp_path)


def test_generation_change_prevents_message_delta(tmp_path, monkeypatch):
    calls = []
    def poll(path):
        calls.append(path)
        generation = 1 if len(calls) <= 2 else 2
        return {'status': 'OBSERVED', 'completed_at_utc': '2026-09-21T19:43:00+00:00'}, {
            'messages_received': generation * 100, 'subscription_epoch_id': 'epoch',
            'subscription_generation': generation}
    monkeypatch.setattr(runtime, 'poll_endpoint', poll)
    monkeypatch.setattr(runtime, 'persistence_observation', lambda root: {})
    result = runtime.collect_live_packet(['SPX'], {}, tmp_path, '2026-09-21', sleep=lambda _: None)
    assert result['evidence']['live_observations']['runtime_comparable'] is False
    assert result['evidence']['live_observations']['message_delta'] is None


@pytest.mark.parametrize('hour,minute,expected', [(19, 43, 'NOT_STARTED'), (19, 48, 'IN_PROGRESS'), (20, 5, 'ELAPSED_COVERAGE_NOT_VERIFIED')])
def test_closing_window_labels_follow_packet_time(tmp_path, monkeypatch, hour, minute, expected):
    from datetime import datetime, timezone
    import app.services.diagnostic_agent as agent
    class FixedDatetime(datetime):
        @classmethod
        def now(cls, tz=None):
            return datetime(2026, 9, 21, hour, minute, tzinfo=timezone.utc)
    monkeypatch.setattr(agent, 'datetime', FixedDatetime)
    packet = build_packet({}, {}, tmp_path, '2026-09-21')
    assert packet['evidence']['coverage_limits']['closing_window'] == expected
    assert packet['evidence']['coverage_limits']['opening_bucket'].startswith('NOT_YET_VERIFIED')


def test_forecast_question_leads_with_verified_answer_or_blocker(monkeypatch):
    import sys
    import app.services.diagnostic_agent as agent
    rendered = []
    fake = NS(**{name: lambda value: rendered.append(value) for name in ('write', 'caption', 'markdown')})
    monkeypatch.setitem(sys.modules, 'streamlit', fake)
    estimate = {'code': 'PASSPORT_POINT_VS_REFERENCE', 'finding': 'SPX verified research estimate',
                'evidence_ids': ['forecast_bindings']}
    blocker = {'code': 'FORECAST_UNBOUND', 'finding': 'NDX source identity missing',
               'evidence_ids': ['forecast_bindings']}
    agent.render_review_content({'schema_version': 'diagnostic-review-v3',
        'question': 'What are SPX and NDX most likely to close at?',
        'review': {'summary': 'Operational summary', 'forecasts': [estimate], 'unverified': [blocker]}})
    assert rendered.index(estimate['finding']) < rendered.index('Operational summary')
    assert rendered.index(blocker['finding']) < rendered.index('Operational summary')
