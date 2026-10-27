"""Synthetic reproduction of reported September 25 conditions, not the original packet."""
from copy import deepcopy
from datetime import datetime

import pytest

from app.services.diagnostic_findings import derive_findings, render_selection
from app.services.diagnostic_history import session_context, review_status_lines, save_review, load_reviews
from app.services.diagnostic_runtime import safe_fields


def packet():
    timestamp = '2026-09-25T17:42:00+00:00'
    timing = session_context(timestamp, '2026-09-25', datetime.fromisoformat(timestamp),
        collection={'mode': 'REQUEST_TIME', 'started_at_utc': timestamp, 'completed_at_utc': timestamp})
    runtime = {'observed_at_utc': timestamp, 'handoff_status': 'active',
               'required_symbols': ['SPX', 'NDX'], 'optional_symbols': ['VIX', 'RUT'],
               'invalid_symbols': ['VIX', 'RUT'], 'subscription_epoch_id': 'a' * 64,
               'subscription_generation': 1, 'messages_received': 9006301}
    evidence = {'runtime': runtime, 'session_context': timing,
        'symbol:SPX': {'validation_is_valid': True, 'is_stale': False, 'usable_for_prediction': True},
        'symbol:VIX': {'validation_is_valid': False}, 'symbol:RUT': {'validation_is_valid': False},
        'coverage_limits': {'opening_bucket': 'NOT_YET_VERIFIED', 'closing_window': 'NOT_STARTED'},
        'capture:SPX': {'session_date_ct': '2026-09-25', 'retained_attempts': 243, 'valid_attempts': 242},
        'source': {'meaning': 'Disk only'},
        'live_observations': {'samples': [{
            'runtime': runtime, 'dashboards': {'SPX': {'forecast_state': 'RESEARCH_ONLY',
                'decision_grade': False, 'current_price': 7740.4596, 'forecast_id': 'b' * 64,
                'pin_payload': {'spot_source': 'databento_opra_put_call_parity'}}},
            'endpoints': [{'status': 'OBSERVED'}],
            'captures': {'capture:SPX': {'session_date_ct': '2026-09-25', 'retained_attempts': 243}},
        }]}}
    last = deepcopy(evidence['live_observations']['samples'][0])
    last['runtime']['messages_received'] = 9007896
    last['dashboards']['SPX']['current_price'] = 7740.2147
    evidence['live_observations']['samples'].append(last)
    return {'session_date_ct': '2026-09-25', 'evidence': evidence}


def test_retained_preview_age_is_measured_at_creation_not_today():
    p = packet()
    timing = session_context('2026-09-25T20:17:33.442617+00:00', '2026-09-25',
                             datetime.fromisoformat('2026-09-25T20:35:42.697906+00:00'))
    p['evidence']['session_context'] = timing
    p['evidence']['runtime']['observed_at_utc'] = timing['runtime_observed_at_utc']
    facts = derive_findings(p)
    assert '1089.3 seconds old' in facts['RETAINED_OBSERVATION']['finding']
    assert 'COLLECTED_FOR_REQUEST' not in facts


@pytest.mark.parametrize('observed,end', [
    ('2026-09-25T20:17:33+00:00', '2026-09-25T20:33:28+00:00'),
    ('2026-09-25T20:33:30+00:00', '2026-09-25T20:33:28+00:00'),
    ('2026-09-25T20:33:27+00:00', '2026-09-25T20:40:00+00:00'),
])
def test_request_time_label_requires_observation_inside_completed_collection(observed, end):
    timing = session_context(observed, '2026-09-25',
        datetime.fromisoformat('2026-09-25T20:35:00+00:00'),
        collection={'mode': 'REQUEST_TIME', 'started_at_utc': '2026-09-25T20:33:20+00:00',
                    'completed_at_utc': end})
    assert timing['collection_mode'] == 'RETAINED_OR_UNKNOWN'


def test_retained_attempt_and_old_database_rows_are_visible_without_health_claim():
    p = packet()
    p['evidence']['capture:SPX']['last_valid_attempt_utc'] = '2026-09-25T19:52:13.423495+00:00'
    p['evidence']['live_observations']['samples'][-1]['persistence'] = {
        'status': 'OBSERVED', 'tables': {'prediction_snapshots': {'VIX': '2026-08-25 20:16:08.879219'}}}
    facts = derive_findings(p)
    assert '19:52:13.423495' in facts['LAST_VALID_ATTEMPT:SPX']['finding']
    old = facts['LOCAL_PERSISTENCE_TIMES:VIX']
    assert old['required'] and old['section'] == 'unverified'
    assert '2026-08-25' in old['finding'] and 'not commit times' in old['finding']
    review = render_selection({'finding_ids': [], 'investigation_ids': []}, facts)
    assert any(f['code'] == 'LOCAL_PERSISTENCE_TIMES' for f in review['unverified'])
    p['evidence']['capture:SPX']['session_date_ct'] = '2026-09-24'
    assert 'LAST_VALID_ATTEMPT:SPX' not in derive_findings(p)


def test_september_25_wording_cannot_reclassify_limitations_as_failures():
    facts = derive_findings(packet())
    review = render_selection({'finding_ids': ['MESSAGE_CHANGE', 'INPUT_VALID_AT_CHECK:SPX'], 'investigation_ids': []}, facts)
    assert review['failing'] == []
    assert facts['RESEARCH_AUTHORITY:SPX']['section'] == 'limitations'
    assert facts['OPTIONAL_SYMBOL_EXCLUDED:VIX']['section'] == 'limitations'
    assert facts['OPTIONAL_SYMBOL_EXCLUDED:RUT']['section'] == 'limitations'
    assert facts['WINDOW_NOT_DUE']['section'] == 'not_due'
    assert 'not a failure' in review['summary']
    assert 'collected for this request' in review['summary']
    assert '1595' in review['changed'][0]['finding']
    assert facts['DERIVED_REFERENCE:SPX']['required'] is True
    assert 'not an observed cash-index quote' in facts['DERIVED_REFERENCE:SPX']['finding']
    assert 'forecast accuracy' in review['summary']


def test_model_cannot_write_summary_numbers_or_categories():
    import jsonschema
    facts = derive_findings(packet())
    with pytest.raises(jsonschema.ValidationError):
        render_selection({'finding_ids': [], 'investigation_ids': [],
                          'summary': 'SPX will close at 9999; VIX blocks SPX'}, facts)
    with pytest.raises(ValueError, match='UNSUPPORTED_FACT'):
        render_selection({'finding_ids': ['invented'], 'investigation_ids': []}, facts)
    with pytest.raises(ValueError, match='UNSUPPORTED_INVESTIGATION'):
        render_selection({'finding_ids': [], 'investigation_ids': ['WINDOW_NOT_DUE']}, facts)


def test_critical_facts_cannot_be_hidden_by_empty_selection():
    facts = derive_findings(packet())
    review = render_selection({'finding_ids': [], 'investigation_ids': []}, facts)
    shown = {v['fact_id'] for values in (review[k] for k in ('working', 'failing', 'changed', 'limitations', 'not_due', 'unverified')) for v in values}
    assert {k for k, v in facts.items() if v['required']} <= shown


def test_poll_origin_does_not_override_stale_quote():
    value = packet()
    value['evidence']['symbol:SPX']['is_stale'] = True
    facts = derive_findings(value)
    assert facts['COLLECTED_FOR_REQUEST']['section'] == 'working'
    assert facts['STALE_INPUT:SPX']['section'] == 'failing'


def test_postclose_staleness_and_different_session_are_not_intraday_outages():
    value = packet()
    value['evidence']['session_context']['runtime_phase'] = 'POST_CLOSE'
    value['evidence']['symbol:SPX']['is_stale'] = True
    facts = derive_findings(value)
    assert facts['STALE_INPUT:SPX']['section'] == 'limitations'
    value['evidence']['session_context'].update(runtime_phase='REGULAR_SESSION', runtime_matches_selected_session=False)
    facts = derive_findings(value)
    assert facts['STALE_INPUT:SPX']['section'] == 'limitations'
    assert facts['SESSION_MISMATCH']['section'] == 'unverified'


@pytest.mark.parametrize('window,code,section', [('IN_PROGRESS', 'WINDOW_IN_PROGRESS', 'unverified'),
    ('ELAPSED_COVERAGE_NOT_VERIFIED', 'CLOSING_COVERAGE_UNVERIFIED', 'unverified')])
def test_partial_or_elapsed_window_is_not_automatically_failure(window, code, section):
    value = packet()
    value['evidence']['coverage_limits']['closing_window'] = window
    assert derive_findings(value)[code]['section'] == section


def test_missing_provenance_and_role_are_not_inferred():
    value = packet()
    del value['evidence']['runtime']['optional_symbols']
    value['evidence']['live_observations']['samples'][-1]['dashboards']['SPX']['pin_payload'] = {'provider': 'databento'}
    facts = derive_findings(value)
    assert 'OPTIONAL_SYMBOL_EXCLUDED:VIX' not in facts
    assert facts['SYMBOL_INVALID_ROLE_UNKNOWN:VIX']['section'] == 'failing'
    assert 'REFERENCE_SOURCE_UNKNOWN:SPX' in facts


def test_generation_reset_suppresses_message_claim_even_if_precomputed_delta_present():
    value = packet()
    value['evidence']['live_observations']['samples'][-1]['runtime']['subscription_generation'] = 2
    value['evidence']['live_observations']['message_delta'] = 1595
    assert 'MESSAGE_CHANGE' not in derive_findings(value)


def test_projection_keeps_binding_and_provenance_without_arbitrary_fields():
    raw = {'forecast_id': 'f', 'prediction_snapshot_id': 1, 'health': {'usable_for_prediction': True},
           'pin_payload': {'spot_source': 'databento_opra_put_call_parity', 'secret': 'hidden'}}
    projected = safe_fields(raw)
    assert projected['forecast_id'] == 'f'
    assert projected['health']['usable_for_prediction'] is True
    assert projected['pin_payload'] == {'spot_source': 'databento_opra_put_call_parity'}


def test_failed_job_and_later_saved_review_remain_independent(tmp_path):
    old = {'schema_version': 'diagnostic-review-v1', 'research_only': True,
           'generated_at_utc': '2026-09-25T18:00:00+00:00', 'packet': packet(),
           'review': {'summary': 'Legacy wording', 'uncertainties': [], 'change_requests': []}}
    path = save_review(old, tmp_path)
    raw = path.read_bytes()
    new = {**old, 'schema_version': 'diagnostic-review-v2', 'generated_at_utc': '2026-09-25T18:05:00+00:00'}
    save_review(new, tmp_path)
    job = {'status': 'FAILED', 'attempts': 1, 'updated_at_utc': '2026-09-25T17:50:00+00:00'}
    a, b = review_status_lines(job, load_reviews(tmp_path, '2026-09-25'))
    assert 'FAILED' in a and 'that job only' in a
    assert '18:05:00' in b and 'does not change' in b
    assert path.read_bytes() == raw
    assert job['status'] == 'FAILED'


def test_unknown_collection_and_backend_timestamp_are_separate():
    stamp = '2026-09-25T17:42:00+00:00'
    result = session_context(stamp, '2026-09-25', datetime.fromisoformat(stamp))
    assert result['collection_mode'] == 'RETAINED_OR_UNKNOWN'
    assert 'not a fresh poll' not in result['meaning']
    assert result['report_created_at_utc'] == stamp


def test_postclose_preserves_actual_validation_and_reports_window_counts():
    value=packet()
    value['evidence']['session_context']['runtime_phase']='POST_CLOSE'
    value['evidence']['coverage_limits']['closing_window']='ELAPSED_COVERAGE_NOT_VERIFIED'
    value['evidence']['symbol:SPX'].update(validation_is_valid=False,is_stale=True,
        validation_failure_reasons=['PSEUDO_PARITY_TOO_FEW_PAIRED_QUOTES: 4 pairs < 5'])
    value['evidence']['capture:SPX'].update(final_15_minutes_attempts=17,final_15_minutes_valid_attempts=6)
    facts=derive_findings(value)
    assert facts['REQUIRED_SYMBOL_INVALID:SPX']['section']=='failing'
    assert 'PSEUDO_PARITY_TOO_FEW_PAIRED_QUOTES' in facts['REQUIRED_SYMBOL_INVALID:SPX']['finding']
    assert 'does not locate when' in facts['REQUIRED_SYMBOL_INVALID:SPX']['finding']
    assert facts['STALE_INPUT:SPX']['section']=='limitations'
    assert '6 valid of 17' in facts['CLOSING_ATTEMPTS:SPX']['finding']


def test_legacy_ui_preserves_original_labels_and_marks_them():
    from streamlit.testing.v1 import AppTest
    value = {'schema_version': 'diagnostic-review-v1', 'review': {'summary': 'Old summary',
             'failing': [{'finding': 'Original wording', 'evidence_ids': ['runtime']}]}}
    app = AppTest.from_string('from app.services.diagnostic_agent import render_review_content\n'
                             f'render_review_content({value!r})').run()
    assert not app.exception
    assert any('predates' in v.value for v in app.caption)
    assert any('original classification' in v.value for v in app.markdown)


def test_v2_ui_keeps_limitations_separate_and_qualifies_empty_issues():
    from streamlit.testing.v1 import AppTest
    body = render_selection({'finding_ids': [], 'investigation_ids': []}, derive_findings(packet()))
    value = {'schema_version': 'diagnostic-review-v2', 'review': body}
    app = AppTest.from_string('from app.services.diagnostic_agent import render_review_content\n'
                             f'render_review_content({value!r})').run()
    assert not app.exception
    words = '\n'.join(v.value for v in app.markdown)
    assert 'Forecast limitations' in words and 'Not due yet' in words
    assert 'No data/application issue was established' in words
    assert 'not a general health guarantee' in words


def test_early_close_uses_calendar_before_classifying_coverage(tmp_path, monkeypatch):
    from app.services import diagnostic_agent as agent
    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return datetime.fromisoformat('2026-11-27T17:50:00+00:00')
    monkeypatch.setattr(agent, 'datetime', Clock)
    value = agent.build_packet({'requested_symbols': [], 'pipeline': {}}, {}, tmp_path, '2026-11-27')
    assert value['evidence']['coverage_limits']['cash_close_utc'].startswith('2026-11-27T18:00:00')
    assert value['evidence']['coverage_limits']['closing_window'] == 'IN_PROGRESS'
    assert derive_findings(value)['WINDOW_IN_PROGRESS']['section'] == 'unverified'
