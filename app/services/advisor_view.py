"""Optional advisor UI, with report-scoped choices and durable local export."""
import json
import streamlit as st
from app.services.live_advisor import fetch_advisor_context, build_advisor_report
from app.services.live_data_client import live_pipeline_symbol_is_current
from app.services.advisor_workflow import (
    TIERS, PREFERENCES, inspect_capabilities, make_selection, save_selection, load_selection,
)


def symbol_diagnostic(context, symbol):
    pipeline = context.get('pipeline') or {}
    state = (pipeline.get('symbol_status') or {}).get(symbol) or {}
    invalid = (context.get('health') or {}).get('invalid_symbols') or []
    if symbol in invalid or state.get('validation_is_valid') is False:
        return 'INVALID', 'Backend reports invalid evidence.'
    if state.get('is_stale') is True or symbol in (pipeline.get('stale_symbols') or []):
        return 'STALE', 'Backend reports stale data.'
    if live_pipeline_symbol_is_current(pipeline, symbol):
        return 'READY AT CHECK', 'Runtime identity eligible at observation; not a model-accuracy claim.'
    if state.get('epoch_is_current') is False or state.get('generation_is_current') is False:
        return 'INVALID', 'Subscription identity mismatch.'
    if state and pipeline.get('handoff_status') == 'active':
        return 'WARMING / INELIGIBLE', 'Current eligibility is not established; inspect backend diagnostics.'
    return 'UNAVAILABLE', 'No current eligible runtime evidence.'


def render_symbol_cards(context):
    for symbol in context.get('requested_symbols', []):
        status, reason = symbol_diagnostic(context, symbol)
        with st.container(border=True):
            st.markdown(f'**{symbol} — {status}**')
            st.caption(reason)
            with st.expander(f'{symbol} diagnostics'):
                st.json(((context.get('pipeline') or {}).get('symbol_status') or {}).get(symbol, {}))


def render_degradation_header(report):
    if not report.get('live_state_eligible'):
        st.warning('ABSTAIN at last check — ' + '; '.join(report.get('live_state_abstention_reasons', [])))
        st.caption('Inspect transport, quote freshness, and subscription identity. A rerun does not repair a feed; do not loosen gates to clear this warning.')
    st.caption('Retained observation only. Select Analyze Live State to refresh; these cards are not a continuous health monitor.')


def render_advisor(symbols, preferences_path=PREFERENCES):
    st.markdown('**Application Advisor — evidence-based implementation review**')
    st.caption('Same day first, then up to one week. Suggestions use source capability checks and observed diagnostics; they are not newly generated AI findings.')
    legacy = st.session_state.get('live_advisor_approved')
    if legacy:
        with st.expander('Earlier session selections — review before reuse'):
            st.caption('These selections predate source-version binding. They are preserved here, without automatically checking new proposals.')
            st.json(legacy)
            st.download_button('Download earlier session selections', json.dumps(legacy, indent=2),
                               file_name='advisor-earlier-selection.json', mime='application/json')
    capabilities = inspect_capabilities()
    if 'advisor_saved' not in st.session_state:
        try:
            st.session_state.advisor_saved = load_selection(preferences_path)
        except (OSError, ValueError) as exc:
            st.warning(f'Could not restore selections: {exc}')
            st.session_state.advisor_saved = None
    if st.button('Analyze Live State', type='primary'):
        st.session_state.live_advisor_context = fetch_advisor_context(symbols)
        previous = st.session_state.get('live_advisor_report', {}).get('capabilities')
        if previous is None and st.session_state.advisor_saved:
            saved = st.session_state.advisor_saved
            previous = {'fingerprint': saved.get('source_fingerprint'),
                        'source_hashes': saved.get('source_hashes', {})}
        st.session_state.live_advisor_report = build_advisor_report(st.session_state.live_advisor_context, previous)
    report = st.session_state.get('live_advisor_report')
    if report:
        context = st.session_state.live_advisor_context
        # Reconcile proposal inventory against current source without querying the feed.
        if report.get('capabilities', {}).get('fingerprint') != capabilities['fingerprint']:
            report = build_advisor_report(context, report.get('capabilities'))
            st.session_state.live_advisor_report = report
            st.info('Application source changed. Suggestions were refreshed against the retained observation; selections require review.')
        st.caption(f"Observed at {context.get('timestamp_utc', 'unknown')} · Source {capabilities['fingerprint'][:12]}")
        if list(symbols) != context.get('requested_symbols', []):
            st.warning('Selected symbols changed. Analyze Live State again to inspect the new selection.')
        render_degradation_header(report)
        render_symbol_cards(context)
        selected = []
        saved = st.session_state.advisor_saved or {}
        restored = {p.get('id') for p in saved.get('proposals', []) if isinstance(p, dict)} if saved.get('source_fingerprint') == capabilities['fingerprint'] else set()
        for tier in TIERS:
            for item in report.get(tier, []):
                with st.expander(item['title']):
                    st.write(item['change_summary'])
                    st.caption('Evidence: ' + item['evidence'])
                    st.caption('Target files: ' + ', '.join(item['target_files']))
                    st.caption('Risk: ' + item['risk_level'])
                    if st.checkbox('Select for implementation review', value=item['id'] in restored,
                                   key=f"advisor_{report['report_id']}_{item['id']}"):
                        selected.append(item['id'])
        if not any(report.get(tier) for tier in TIERS):
            st.info('No new changes are justified by the checks currently implemented. This is not a full code review or proof of predictive validity.')
        st.caption('Implemented source capabilities: ' + ', '.join(k for k, v in capabilities['implemented'].items() if v))
        st.caption('Freshness policy, fallback confidence, calibration and active model features remain gated. No automated tuning or code execution.')
        if st.button('Record Approved Proposals'):
            selection = make_selection(report, selected)
            try:
                save_selection(selection, preferences_path)
            except (OSError, ValueError) as exc:
                st.error(f'Save failed; previous saved selection retained: {exc}')
            else:
                st.session_state.advisor_saved = selection
                st.success('Selection saved to local UI preferences. No code or model changes executed.')
    else:
        st.caption('Select Analyze Live State to collect diagnostics. No live requests run automatically.')
    from app.services.diagnostic_agent import render_diagnostic_agent
    render_diagnostic_agent(st.session_state.get('live_advisor_context') or
                            {'requested_symbols': list(symbols)}, capabilities)
    saved = st.session_state.advisor_saved
    if saved:
        if saved.get('source_fingerprint') != capabilities['fingerprint']:
            st.warning('Saved selections refer to an older source version. They remain available for download but are not automatically approved for this version.')
        with st.expander('Approved Proposals (Recorded)'):
            st.json(saved)
        st.download_button('Download saved proposals (JSON)', json.dumps(saved, indent=2),
                           file_name='advisor-proposals.json', mime='application/json')
