import json
from datetime import datetime, timezone
from types import SimpleNamespace
import pytest
from backend.capture_attempts import record_blocked_capture
from app.services.capture_review import load_capture_attempts
from app.services.diagnostic_agent import build_packet, diagnose
from app.utils.display_time import resolve_display_timezone
from app.utils.market_time import market_is_open


@pytest.mark.parametrize('utc', ['2026-09-14T19:45:00+00:00', '2026-09-14T19:59:59+00:00',
                                  '2026-11-27T17:59:59+00:00', '2026-09-14T20:00:00+00:00'])
def test_capture_session_remains_open_until_actual_close(utc):
    assert market_is_open(datetime.fromisoformat(utc))


@pytest.mark.parametrize('utc', ['2026-09-14T20:00:00.000001+00:00', '2026-11-27T18:00:00.000001+00:00'])
def test_regular_and_early_close_boundaries(utc):
    assert not market_is_open(datetime.fromisoformat(utc))


def test_blocked_receipt_throttled_and_never_price_evidence(tmp_path):
    streamer = SimpleNamespace(audit_dir=tmp_path/'logs/audit', snapshot_interval=60,
                               symbols=['SPX'], active_generation=1,
                               subscription_epoch_id='test', handoff_status='warming')
    record_blocked_capture(streamer, 'HANDOFF_NOT_ACTIVE', 100)
    record_blocked_capture(streamer, 'HANDOFF_NOT_ACTIVE', 101)
    record_blocked_capture(streamer, 'COMPUTE_SUSPENDED', 160)
    path = next((tmp_path/'logs/capture_attempts').glob('*.ndjson'))
    rows = [json.loads(line) for line in path.read_text().splitlines()]
    assert len(rows) == 2
    assert all(r['usable_for_prediction'] is False and 'price' not in r for r in rows)


def test_failed_late_attempt_visible_without_promoting_snapshot(tmp_path):
    directory = tmp_path/'logs/audit/SPX'
    directory.mkdir(parents=True)
    row = {'generated_at_utc': '2026-09-14T19:59:48+00:00', 'validation_is_valid': False,
           'validation_failure_reasons': ['TOO_FEW_PAIRS']}
    (directory/'20260914-195948.json').write_text(json.dumps(row))
    result = load_capture_attempts(tmp_path, 'SPX', '2026-09-14', resolve_display_timezone('America/Chicago'))
    assert result == [row]


def test_packet_excludes_unknown_fields_and_secrets(tmp_path):
    packet = build_packet({'timestamp_utc':'old', 'pipeline': {'api_key':'secret', 'handoff_status':'warming'},
                           'dashboards':{'SPX':{'secret':'secret'}}, 'requested_symbols':['SPX']}, {}, tmp_path)
    assert 'secret' not in json.dumps(packet)
    assert packet['evidence']['runtime']['observed_at_utc'] == 'old'


def response_client(review):
    def create(**kwargs):
        assert kwargs['store'] is False
        assert 'tools' not in kwargs
        assert kwargs['text']['format']['strict'] is True
        return SimpleNamespace(output_text=json.dumps(review))
    return SimpleNamespace(responses=SimpleNamespace(create=create))


@pytest.mark.parametrize('ref,accepted', [('runtime', True), ('invented', False)])
def test_agent_rejects_fabricated_evidence(tmp_path, ref, accepted):
    packet = build_packet({}, {}, tmp_path)
    review = {'summary':'Missing live proof', 'uncertainties':['No observation'],
              'change_requests':[{'title':'Check capture','reason':'Missing proof',
                                  'proposed_change':'Inspect', 'verification':'Compare timestamps',
                                  'evidence_ids':[ref]}]}
    if accepted:
        result = diagnose('Review readiness', packet, response_client(review))
        assert result['execution_status'] == 'PROPOSED_ONLY'
    else:
        with pytest.raises(ValueError, match='unsupported'):
            diagnose('Review readiness', packet, response_client(review))


def test_agent_refusal_cannot_become_a_change_request(tmp_path):
    with pytest.raises(Exception):
        diagnose('Review', build_packet({}, {}, tmp_path), response_client({'summary':'refused'}))


def test_agent_ui_proposed_bundle_and_rerun(monkeypatch):
    from streamlit.testing.v1 import AppTest
    import app.services.diagnostic_agent as agent
    monkeypatch.setattr(agent, 'build_packet', lambda *args: {'evidence': {}})
    monkeypatch.setattr(agent, 'diagnose', lambda *args: {
        'generated_at_utc': '2026-09-14T20:00:00+00:00', 'execution_status': 'PROPOSED_ONLY',
        'review': {'summary': 'Test diagnosis', 'uncertainties': ['No live evidence'], 'change_requests': []}})
    app = AppTest.from_string('from app.services.diagnostic_agent import render_diagnostic_agent\nrender_diagnostic_agent({}, {})').run()
    app.text_area[0].set_value('Review capture')
    app.button[0].click().run()
    assert not app.exception
    assert app.session_state['diagnostic_result']['execution_status'] == 'PROPOSED_ONLY'
    app.run()
    assert app.session_state['diagnostic_result']['review']['summary'] == 'Test diagnosis'
