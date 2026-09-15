"""Local proposal review workflow; never changes analytical policy or models."""
from __future__ import annotations

import ast
import hashlib
import json
import os
from pathlib import Path
import tempfile
from datetime import datetime, timezone

ROOT = Path(__file__).resolve().parents[2]
PREFERENCES = ROOT / 'data' / 'ui_preferences' / 'advisor_selection.json'
TIERS = ('safe_quick_wins', 'medium_risk_improvements', 'high_impact_optional')


def identity(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':')).encode()).hexdigest()


def inspect_capabilities(root=ROOT):
    """Bounded source inspection. Presence is implementation evidence, not runtime proof."""
    paths = sorted({'app.py', 'app/services/live_advisor.py', 'app/services/live_panel_view.py',
                    'app/services/advisor_view.py', 'app/services/advisor_workflow.py'} |
                   {p.relative_to(Path(root)).as_posix()
                    for p in (Path(root) / 'app/services').glob('*.py')})
    hashes, calls, functions, issues = {}, {}, {}, []
    for relative in paths:
        try:
            raw = (Path(root) / relative).read_bytes()
            hashes[relative] = hashlib.sha256(raw).hexdigest()
            tree = ast.parse(raw)
            functions[relative] = {n.name for n in ast.walk(tree) if isinstance(n, ast.FunctionDef)}
            calls[relative] = {ast.unparse(n.func) for n in ast.walk(tree) if isinstance(n, ast.Call)}
        except (OSError, SyntaxError) as exc:
            hashes[relative] = None
            issues.append(f'{relative}: {type(exc).__name__}')
    wired = 'render_advisor' in calls.get('app.py', set())
    view_calls = calls.get('app/services/advisor_view.py', set())
    supported = {
        'symbol_badges': wired and 'render_symbol_cards' in view_calls and
                         'render_symbol_cards' in functions.get('app/services/advisor_view.py', set()),
        'saved_selections': wired and {'save_selection', 'load_selection', 'st.download_button'} <= view_calls and
                            {'save_selection', 'load_selection'} <= functions.get('app/services/advisor_workflow.py', set()),
        'degradation_header': wired and 'render_degradation_header' in view_calls and
                              'render_degradation_header' in functions.get('app/services/advisor_view.py', set()),
    }
    return {'fingerprint': identity(hashes), 'source_hashes': hashes,
            'implemented': supported, 'inspection_issues': issues}


def proposal(title, summary, evidence, files, level='Low'):
    item = {'title': title, 'change_summary': summary, 'evidence': evidence,
            'target_files': files, 'risk_level': level,
            'expected_impact': 'Proposed benefit requires verification; no accuracy claim.'}
    item['id'] = identity(item)
    return item


def contextual_proposals(context, capabilities):
    """Evidence-triggered checklist, explicitly not unconstrained AI code generation."""
    items = []
    for key, title, summary in (
        ('symbol_badges', 'Show per-symbol diagnostic cards', 'Display backend eligibility and explicit failure states.'),
        ('saved_selections', 'Persist and export advisor selections', 'Save report-bound selections to local UI preferences.'),
        ('degradation_header', 'Surface degradation in the advisor header', 'Show abstention reasons before proposals.'),
    ):
        if not capabilities['implemented'].get(key):
            items.append(proposal(title, summary, f'Source capability not detected: {key}',
                                  ['app.py', 'app/services/advisor_view.py']))
    pipeline = context.get('pipeline') or {}
    for field, title in (
        ('stale_symbols', 'Investigate stale symbol coverage'),
        ('epoch_mismatch_symbols', 'Investigate subscription epoch mismatch'),
        ('generation_mismatch_symbols', 'Investigate subscription generation mismatch'),
    ):
        if pipeline.get(field):
            items.append(proposal(title, 'Trace source timestamps and runtime identity; preserve existing validity gates.',
                                  f'{field}: {pipeline[field]}', ['app/services/live_data_client.py']))
    if pipeline.get('handoff_status') != 'active':
        items.append(proposal('Inspect inactive stream handoff',
                              'Review transport and subscription diagnostics before considering recovery.',
                              f"handoff_status: {pipeline.get('handoff_status', 'unavailable')}",
                              ['app/services/live_data_client.py']))
    if capabilities['inspection_issues']:
        items.append(proposal('Resolve incomplete advisor source inspection',
                              'Review missing or unparsable source before treating capabilities as implemented.',
                              '; '.join(capabilities['inspection_issues']), ['app/services/advisor_workflow.py']))
    return items


def make_selection(report, selected_ids):
    items = [p for tier in TIERS for p in report.get(tier, []) if p['id'] in selected_ids]
    return {'schema_version': 1, 'recorded_at': datetime.now(timezone.utc).isoformat(),
            'report_id': report['report_id'], 'source_fingerprint': report['capabilities']['fingerprint'],
            'source_hashes': report['capabilities']['source_hashes'],
            'context_timestamp': report.get('context_timestamp'), 'proposals': items,
            'research_only': True, 'decision_grade': False, 'action': 'implementation_review_only'}


def save_selection(selection, path=PREFERENCES):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    name = None
    try:
        with tempfile.NamedTemporaryFile(mode='w', encoding='utf-8', dir=path.parent, delete=False) as handle:
            name = handle.name
            json.dump(selection, handle, indent=2, allow_nan=False)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(name, path)
    finally:
        if name and os.path.exists(name):
            os.unlink(name)


def load_selection(path=PREFERENCES):
    try:
        value = json.loads(Path(path).read_text(encoding='utf-8'))
    except FileNotFoundError:
        return None
    if (not isinstance(value, dict) or value.get('schema_version') != 1 or
            not isinstance(value.get('proposals'), list) or
            value.get('action') != 'implementation_review_only' or
            value.get('research_only') is not True or value.get('decision_grade') is not False):
        raise ValueError('Invalid advisor preferences; existing file was not changed')
    return value
