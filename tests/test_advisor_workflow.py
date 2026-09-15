import copy
import json
from pathlib import Path

import pytest
from streamlit.testing.v1 import AppTest
from app.services.advisor_workflow import (
    inspect_capabilities, contextual_proposals, save_selection, load_selection, make_selection,
)
from app.services.live_advisor import build_advisor_report
from app.services.advisor_view import symbol_diagnostic


def context():
    epoch = 'a' * 64
    return {'timestamp_utc': '2026-09-14T13:30:00+00:00', 'requested_symbols': ['SPX'],
            'health': {'websocket': 'active'},
            'pipeline': {'prediction_pipeline_ok': True, 'handoff_status': 'active',
                         'subscription_epoch_id': epoch, 'subscription_epoch_valid': True,
                         'symbol_status': {'SPX': {
                             'subscription_epoch_id': epoch, 'active_subscription_epoch_id': epoch,
                             'epoch_is_current': True, 'generation_is_current': True,
                             'subscription_generation': 1, 'active_generation': 1,
                             'usable_for_prediction': True, 'is_stale': False, 'fresh_quote_count': 8}}}}


def test_badges_never_promote_invalid_or_stale_or_identity_mismatch():
    c = context()
    assert symbol_diagnostic(c, 'SPX')[0] == 'READY AT CHECK'
    s = c['pipeline']['symbol_status']['SPX']
    s['is_stale'] = True
    assert symbol_diagnostic(c, 'SPX')[0] == 'STALE'
    s['is_stale'] = False
    s['epoch_is_current'] = False
    assert symbol_diagnostic(c, 'SPX')[0] == 'INVALID'
    assert symbol_diagnostic(c, 'NDX')[0] == 'UNAVAILABLE'


def test_source_capabilities_suppress_completed_work_and_track_changes(tmp_path):
    cap = inspect_capabilities()
    assert all(cap['implemented'].values())
    assert not contextual_proposals(context(), cap)
    root = Path(__file__).resolve().parents[1]
    for relative in cap['source_hashes']:
        target = tmp_path / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes((root / relative).read_bytes())
    original = inspect_capabilities(tmp_path)
    p = tmp_path / 'app.py'
    p.write_text(p.read_text(encoding='utf-8').replace('render_advisor(advisor_symbols)', 'print(advisor_symbols)'), encoding='utf-8')
    changed = inspect_capabilities(tmp_path)
    assert changed['fingerprint'] != original['fingerprint']
    assert not any(changed['implemented'].values())
    assert len(contextual_proposals(context(), changed)) == 3


def test_report_generates_evidence_bound_change_review():
    old = inspect_capabilities()
    old['source_hashes']['app.py'] = 'old'
    old['fingerprint'] = 'old'
    report = build_advisor_report(context(), old)
    item = report['safe_quick_wins'][-1]
    assert item['target_files'] == ['app.py']
    assert 'changed' in item['evidence']
    assert report['medium_risk_improvements'] == report['high_impact_optional'] == []


def test_disk_selection_roundtrip_and_failed_write_preserves_previous(tmp_path, monkeypatch):
    c = context(); c['pipeline']['stale_symbols'] = ['SPX']
    report = build_advisor_report(c)
    item = report['safe_quick_wins'][0]
    saved = make_selection(report, [item['id']])
    path = tmp_path / 'prefs.json'
    save_selection(saved, path)
    assert load_selection(path) == saved
    import app.services.advisor_workflow as workflow
    def fail(*args):
        raise OSError('disk unavailable')
    monkeypatch.setattr(workflow.os, 'replace', fail)
    with pytest.raises(OSError):
        save_selection(make_selection(report, []), path)
    assert load_selection(path) == saved
    assert list(tmp_path.iterdir()) == [path]
    path.write_text('{broken', encoding='utf-8')
    with pytest.raises(ValueError):
        load_selection(path)


def test_real_ui_save_restore_download_and_retained_labels(tmp_path):
    c = context(); c['pipeline']['stale_symbols'] = ['SPX']
    path = tmp_path / 'prefs.json'
    script = f'''
from app.services import advisor_view as view
view.fetch_advisor_context = lambda symbols: {c!r}
view.render_advisor(['SPX'], preferences_path={str(path)!r})
'''
    ui = AppTest.from_string(script).run()
    assert not ui.exception
    ui.button[0].click().run()
    assert not ui.exception
    assert any('Retained observation' in x.value for x in ui.caption)
    assert any('STALE' in x.value for x in ui.markdown)
    ui.checkbox[0].check().run()
    next(b for b in ui.button if b.label == 'Record Approved Proposals').click().run()
    assert not ui.exception
    saved = load_selection(path)
    assert len(saved['proposals']) == 1
    assert saved['research_only'] and saved['decision_grade'] is False
    fresh = AppTest.from_string(script).run()
    assert not fresh.exception
    assert fresh.get('download_button')
    fresh.button[0].click().run()
    assert fresh.checkbox[0].value is True
    # A previously saved selection from different source is retained but not checked.
    saved['source_fingerprint'] = 'older-source'
    save_selection(saved, path)
    changed = AppTest.from_string(script).run()
    changed.button[0].click().run()
    assert changed.checkbox[0].value is False
    assert any('older source version' in x.value for x in changed.warning)
