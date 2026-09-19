from datetime import date, datetime, timezone
import json

import pytest

from app.services.diagnostic_agent import build_packet
from app.services.diagnostic_history import load_reviews, save_review, session_context


def test_packet_separates_retained_runtime_selected_capture_and_old_documents(tmp_path):
    directory = tmp_path / 'logs/audit/SPX'
    directory.mkdir(parents=True)
    for stamp, valid in [('2026-09-17T19:46:00+00:00', True),
                         ('2026-09-17T19:59:00+00:00', False),
                         ('2026-09-16T19:59:00+00:00', False)]:
        (directory / (stamp[:10].replace('-', '') + '-' + str(valid) + '.json')).write_text(json.dumps({
            'generated_at_utc': stamp, 'validation_is_valid': valid,
            'validation_failure_reasons': [] if valid else ['TOO_FEW_PAIRS'],
        }))
    doc = tmp_path / 'docs/FAILURE_AND_CORRECTION_REGISTER.md'
    doc.parent.mkdir()
    doc.write_text('Older incident remains open')
    context = {'timestamp_utc': '2026-09-16T20:22:00+00:00', 'requested_symbols': ['SPX', 'NDX']}
    packet = build_packet(context, {}, tmp_path, date(2026, 9, 17))
    assert packet['session_date_ct'] == '2026-09-17'
    timing = packet['evidence']['session_context']
    assert timing['runtime_session_date_ct'] == '2026-09-16'
    assert timing['runtime_matches_selected_session'] is False
    summary = packet['evidence']['capture:SPX']
    assert summary['retained_attempts'] == 2
    assert summary['valid_attempts'] == summary['invalid_attempts'] == 1
    assert summary['final_15_minutes_valid_attempts'] == 1
    assert summary['validation_is_valid'] is False  # latest attempt only
    assert summary['last_valid_attempt_utc'] == '2026-09-17T19:46:00+00:00'
    assert len(summary['attempt_records_sha256']) == 64
    assert packet['evidence']['capture:NDX']['availability'] == 'NO_RETAINED_ATTEMPTS'
    assert packet['evidence']['docs/FAILURE_AND_CORRECTION_REGISTER.md']['evidence_kind'].startswith('HISTORICAL')
    # Default caller must stay on the runtime observation day, not silently use today.
    assert build_packet(context, {}, tmp_path)['session_date_ct'] == '2026-09-16'


def test_postclose_clock_and_unknown_runtime_are_explicit():
    now = datetime(2026, 9, 17, 20, 30, tzinfo=timezone.utc)
    timing = session_context('2026-09-17T20:22:57+00:00', '2026-09-17', now)
    assert timing['runtime_phase'] == 'POST_CLOSE'
    assert timing['runtime_age_seconds'] == 423
    assert session_context('bad', '2026-09-17', now)['runtime_phase'] == 'UNKNOWN'


def test_available_dates_use_chicago_dates_and_keep_failed_only_sessions(tmp_path):
    from app.services.diagnostic_history import available_capture_dates
    audit = tmp_path / 'logs/audit/SPX'
    audit.mkdir(parents=True)
    (audit / '20260917-020000.json').write_text('{}')  # September 16 Chicago
    (audit / '20260917-190000.json').write_text('{}')
    (audit / 'unrecognized-name.json').write_text('{}')
    failed = tmp_path / 'logs/capture_attempts'
    failed.mkdir()
    (failed / '2026-09-15.ndjson').write_text(json.dumps({
        'generated_at_utc': '2026-09-15T19:59:00+00:00', 'validation_is_valid': False,
    }) + '\ninvalid-json\n')
    assert available_capture_dates(tmp_path) == ['2026-09-17', '2026-09-16', '2026-09-15']


def _review(day='2026-09-17'):
    return {'schema_version': 'diagnostic-review-v1', 'research_only': True,
            'generated_at_utc': day + 'T20:30:00+00:00', 'execution_status': 'PROPOSED_ONLY',
            'packet': {'session_date_ct': day},
            'review': {'summary': 'Inspect capture', 'change_requests': [], 'uncertainties': []}}


def test_review_archive_is_idempotent_and_does_not_replace_prior_sessions(tmp_path):
    old = _review('2026-09-16')
    old_path = save_review(old, tmp_path)
    before = old_path.read_bytes(), old_path.stat().st_mtime_ns
    assert save_review(old, tmp_path) == old_path
    save_review(_review(), tmp_path)
    assert (old_path.read_bytes(), old_path.stat().st_mtime_ns) == before
    assert load_reviews(tmp_path, '2026-09-16') == [old]
    assert load_reviews(tmp_path, '2026-09-18') == []
    old_path.write_bytes(b'tampered')
    with pytest.raises(ValueError, match='hash'):
        load_reviews(tmp_path, '2026-09-16')
    with pytest.raises(ValueError, match='conflicts'):
        save_review(old, tmp_path)
    assert old_path.read_bytes() == b'tampered'


def test_ui_saves_dated_review_and_labels_it_when_date_changes(tmp_path, monkeypatch):
    from streamlit.testing.v1 import AppTest
    import app.services.diagnostic_agent as agent
    monkeypatch.setattr(agent, 'ROOT', tmp_path)
    monkeypatch.setattr(agent, 'diagnose', lambda question, packet: {
        **_review(packet['session_date_ct']), 'packet': packet})
    app = AppTest.from_string(
        'from app.services.diagnostic_agent import render_diagnostic_agent\n'
        "render_diagnostic_agent({'timestamp_utc': '2026-09-17T20:22:57+00:00'}, {})"
    ).run()
    app.date_input(key='diagnostic_session_date').set_value(date(2026, 9, 17)).run()
    app.text_area[0].set_value('Review this session')
    next(b for b in app.button if b.label == 'Ask AI to review this evidence').click().run()
    next(b for b in app.button if b.label == 'Save dated review to local history').click().run()
    assert not app.exception
    assert len(load_reviews(tmp_path, '2026-09-17')) == 1
    app.date_input(key='diagnostic_session_date').set_value(date(2026, 9, 18)).run()
    assert not app.exception
    assert any('Retained AI review belongs to 2026-09-17' in w.value for w in app.warning)
