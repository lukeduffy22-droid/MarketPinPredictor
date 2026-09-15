"""Explicit official-close collection; never substitutes estimates or scores models."""
from datetime import date, datetime, timezone
import json
import os
from pathlib import Path
import uuid

ROOT = Path(__file__).resolve().parents[2]


def validate_saved_close(record, root):
    from backend.closing_tape.close_evidence import (
        resolve_verified_close_artifact, validate_official_close_reference,
        validate_verified_close_observed_at)
    from tools.ingest_verified_closes import VerifiedCloseInput, validate_official_artifact_semantics
    path = resolve_verified_close_artifact(Path(root)/'data'/'verified_close_sources',
        trading_date=record['trading_date'], symbol=record['symbol'],
        source_artifact_sha256=record['source_artifact_sha256'])
    reference = validate_official_close_reference(record['symbol'],record['source'],record['source_reference'])
    observed = validate_verified_close_observed_at(record['trading_date'],record['observed_at_utc'])
    validate_official_artifact_semantics(VerifiedCloseInput(
        symbol=record['symbol'],trading_date=date.fromisoformat(record['trading_date']),
        official_close=record['official_close'],source=record['source'],source_reference=reference,
        source_artifact_sha256=record['source_artifact_sha256'],source_artifact_path=path,
        observed_at_utc=observed,correction_of_id=None))


def collect_closing_prices(symbols, trading_day, root=ROOT, *, now=None, fetch=None):
    from tools.fetch_verified_close_bundle import (
        OFFICIAL_CLOSE_ENDPOINTS, _publication_cutoff_utc, extract_official_close)
    from tools.fetch_verified_close_artifact import fetch_official_artifact
    from tools.ingest_verified_closes import VerifiedCloseInput, validate_official_artifact_semantics
    from app.utils.market_calendar import market_calendar_status
    fetch = fetch or fetch_official_artifact
    now = now or datetime.now(timezone.utc)
    if now.tzinfo is None:
        raise ValueError('Observation time must include a timezone.')
    trading_day = date.fromisoformat(str(trading_day))
    root = Path(root).resolve()
    endpoints = {e.symbol: e for e in OFFICIAL_CLOSE_ENDPOINTS}
    symbols = list(dict.fromkeys(str(s).strip().upper() for s in symbols))
    if not symbols or len(symbols) > 50 or any(not s.isalnum() or len(s)>16 for s in symbols):
        raise ValueError('Provide between 1 and 50 supported symbol identifiers.')
    calendar = market_calendar_status(trading_day)
    session_open = calendar['market_open']
    records = []
    for symbol in symbols:
        record = {'symbol':symbol, 'trading_date':str(trading_day), 'official_close':None,
                  'status':'AWAITING_PUBLICATION'}
        records.append(record)
        if not session_open:
            record['status'] = 'NO_VERIFIED_SESSION'
            continue
        if symbol not in endpoints:
            record['status'] = 'UNSUPPORTED_SOURCE'
            continue
        if now < _publication_cutoff_utc(trading_day):
            record['retry_after_utc'] = _publication_cutoff_utc(trading_day).isoformat()
            continue
        endpoint = endpoints[symbol]
        try:
            artifact = fetch(symbol=symbol, source=endpoint.source,
                             source_reference=endpoint.url(trading_day),
                             trading_date=trading_day, project_root=root)
            row = VerifiedCloseInput(symbol=symbol, trading_date=trading_day,
                official_close=extract_official_close(artifact['source_artifact_path'], symbol=symbol, trading_day=trading_day),
                source=endpoint.source, source_reference=artifact['source_reference'],
                source_artifact_sha256=artifact['source_artifact_sha256'],
                source_artifact_path=Path(artifact['source_artifact_path']),
                observed_at_utc=datetime.fromisoformat(artifact['retrieved_at_utc'].replace('Z','+00:00')),
                correction_of_id=None)
            validate_official_artifact_semantics(row)
            record.update(status='OFFICIAL_ARTIFACT_SAVED', official_close=row.official_close,
                          source=row.source, source_reference=row.source_reference,
                          source_artifact_sha256=row.source_artifact_sha256,
                          source_artifact_path=str(row.source_artifact_path),
                          observed_at_utc=row.observed_at_utc.isoformat())
            validate_saved_close(record, root)
        except Exception as exc:
            # Keep each symbol visible and continue the remaining symbols.
            record.update(status='SOURCE_UNAVAILABLE', official_close=None, error_type=type(exc).__name__)
    report = {'schema_version':'tracked-closing-prices-v1', 'trading_date':str(trading_day),
              'created_at_utc':now.isoformat(), 'records':records,
              'complete':all(r['status']=='OFFICIAL_ARTIFACT_SAVED' for r in records),
              'ledger_ingested':False, 'scoring_performed':False,
              'note':'Saved source evidence only. Existing complete-bundle ledger ingestion and outcome validation remain required for scoring.'}
    directory = root/'data'/'closing_prices'/str(trading_day)
    directory.mkdir(parents=True, exist_ok=True)
    name = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%f')+'-'+uuid.uuid4().hex+'.json'
    path = directory/name
    temporary = directory/(name+'.tmp')
    with temporary.open('x',encoding='utf-8') as handle:
        json.dump(report,handle,indent=2,allow_nan=False)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary,path)
    return report, path


def latest_close_report(trading_day, root=ROOT):
    day = date.fromisoformat(str(trading_day))
    directory = Path(root)/'data'/'closing_prices'/str(day)
    for path in sorted(directory.glob('*.json'), reverse=True):
        try:
            report = json.loads(path.read_text(encoding='utf-8'))
            if report.get('schema_version') == 'tracked-closing-prices-v1' and report.get('trading_date') == str(day):
                for row in report['records']:
                    if row['status'] == 'OFFICIAL_ARTIFACT_SAVED':
                        try:
                            validate_saved_close(row,root)
                        except (OSError,ValueError,KeyError,TypeError):
                            row.update(status='ARTIFACT_INVALID',official_close=None)
                report['complete'] = all(r['status']=='OFFICIAL_ARTIFACT_SAVED' for r in report['records'])
                return report
        except (OSError,ValueError,KeyError,TypeError):
            continue
    return None


def tracked_symbols_from_backend(selected):
    import requests
    response = requests.get('http://127.0.0.1:8000/health',timeout=3)
    response.raise_for_status()
    tracked = response.json().get('symbols_requested')
    if not isinstance(tracked,list) or not tracked:
        raise ValueError('Backend tracked-symbol inventory unavailable.')
    return sorted(set(selected) | {str(s).strip().upper() for s in tracked})


def render_closing_prices(symbols):
    import streamlit as st
    from zoneinfo import ZoneInfo
    with st.expander('Official closing prices — save and review'):
        st.caption('Fetches official-source artifacts for every selected tracked symbol. No AI is needed. The existing publication gate allows requests from 5:00 PM Central; unavailable sources remain pending. Nothing is scored or trained here.')
        day = st.date_input('Closing-price session', datetime.now(ZoneInfo('America/Chicago')).date(), key='official_close_day')
        st.write('Selected symbols: '+', '.join(symbols))
        st.caption('Collection also includes every symbol reported as tracked by the backend. If that inventory cannot be read, collection fails rather than silently omitting symbols.')
        if st.button('Fetch and save official closes'):
            with st.spinner('Collecting official close evidence…'):
                try:
                    report, path = collect_closing_prices(tracked_symbols_from_backend(symbols),day)
                    st.success('Collection report saved: '+str(path))
                except Exception:
                    st.error('Could not complete collection. Check the backend tracked-symbol inventory, session date and local storage.')
        report = latest_close_report(day)
        if report:
            st.caption('Saved at '+report['created_at_utc']+'; retained source evidence, not a live quote.')
            st.dataframe(report['records'],hide_index=True)
            missing = sorted(set(symbols)-{r['symbol'] for r in report['records']})
            if missing:
                st.warning('Not included in this saved collection: '+', '.join(missing))
            if not report['complete']:
                st.warning('Closing-price coverage is incomplete. Review the per-symbol statuses and retry after publication.')
            st.download_button('Download saved closing-price report',json.dumps(report,indent=2),
                               file_name=f'closing-prices-{day}.json',mime='application/json')
        else:
            st.info('No saved collection report for this session yet.')
