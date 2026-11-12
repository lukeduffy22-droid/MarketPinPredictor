"""Bounded read-only observations for AI reviews; no retained-runtime fallback."""
from datetime import datetime, timezone
import math
import re
import sqlite3
import time

import requests


def utc_now():
    return datetime.now(timezone.utc).isoformat()


# Explicit fields only: never send environment, arbitrary logs, or exception bodies.
FIELDS = set('''status provider sampled_at_utc generated_at_utc source_as_of_utc
last_update_utc handoff_status messages_received subscription_epoch_id
subscription_generation prediction_pipeline_ok stream_connected stream_progressing
stale_symbols invalid_symbols missing_symbols last_tick_age_ms stale_after_seconds
symbol_status validation_is_valid validation_failure_reasons is_stale epoch_is_current
generation_is_current age_seconds fresh_quote_count current_price symbol
prediction_authority prediction forecast_state decision_grade predicted_close
confidence model_version model_type prediction_mode pin_payload gamma_pin max_pain
zero_gamma net_gex calculation_id calculated_at_utc validation health
is_valid failure_reasons abstention_reasons source timestamp_utc
required_symbols optional_symbols required_invalid_symbols optional_degraded_symbols
runtime_context_stable reconnect_attempts last_reconnect_utc
forecast_id prediction_snapshot_id spot_source usable_for_prediction
quote_timestamp_utc source_timestamp_utc prediction_timestamp_utc target_timestamp_utc
timestamp model_artifact_sha256 data_age_seconds stale_after_seconds horizon_seconds
forecast_source_payload_sha256 primary_expiration primary_expiration_authority
primary_expiration_context_only selected_primary_pair_count expected_primary_pair_count
paired_primary_pair_count primary_pair_coverage_ratio paired_quote_count call_quote_count
put_quote_count strike_count nonzero_strike_count top_strike_concentration quote_age_seconds'''.split())


def safe_fields(value, depth=0):
    if depth > 5:
        return None
    if isinstance(value, dict):
        return {k: safe_fields(v, depth + 1) for k, v in value.items()
                if k in FIELDS or k in ('SPX', 'NDX', 'RUT', 'VIX')}
    if isinstance(value, list):
        return [safe_fields(v, depth + 1) for v in value[:20]]
    if isinstance(value, str):
        return re.sub(r'(?i)(?:sk-[\w-]+|bearer\s+\S+)', '[REDACTED]', value[:500])
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return value if value is None or isinstance(value, (bool, int, float)) else None


def poll_endpoint(path, get=None):
    started = time.monotonic()
    result = {'path': path, 'started_at_utc': utc_now(), 'status': 'UNAVAILABLE'}
    payload = {}
    try:
        response = (get or requests.get)('http://127.0.0.1:8000' + path, timeout=3)
        result['http_status'] = response.status_code
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, dict):
            raise ValueError('Expected object')
        reported_error = payload.get('last_error')
        payload = safe_fields(payload)
        if reported_error:
            # Fixed categories only, never raw exception/log text.
            text = str(reported_error).lower()
            payload['reported_error_summary'] = {
                'present': True,
                'matched_terms': [term for term in ('timeout', 'locked', 'disconnect', 'subscription', 'permission') if term in text],
                'meaning': 'Bounded matches in backend-reported error text; not a root-cause conclusion.'}
        result['status'] = 'OBSERVED'
    except (requests.RequestException, ValueError) as exc:
        result['error_kind'] = type(exc).__name__
        payload = {}
    result.update(completed_at_utc=utc_now(), elapsed_ms=round((time.monotonic() - started) * 1000))
    return result, payload


def persistence_observation(root):
    """Local configured DB evidence; does not assert the backend uses this same DB."""
    result = {'observed_at_utc': utc_now(), 'status': 'UNAVAILABLE', 'tables': {},
              'meaning': 'Read-only configured local database. Runtime database identity not verified; timestamps are row event times, not commit times.'}
    try:
        from backend.database_target import configured_market_database_path
        path = configured_market_database_path(root)
        if not path.is_file():
            result['error_kind'] = 'DATABASE_MISSING'
            return result
        connection = sqlite3.connect(path.as_uri() + '?mode=ro', uri=True, timeout=1)
        deadline = time.monotonic() + 2
        connection.set_progress_handler(lambda: int(time.monotonic() > deadline), 1000)
        try:
            for table, column in (('gamma_calculation_runs', 'calculated_at_utc'),
                                  ('prediction_snapshots', 'timestamp_utc')):
                try:
                    rows = connection.execute(
                        f'SELECT symbol, MAX({column}) FROM {table} GROUP BY symbol').fetchall()
                    result['tables'][table] = {s: t for s, t in rows if s in ('SPX', 'NDX', 'RUT', 'VIX')}
                except sqlite3.Error as exc:
                    result['tables'][table] = {'error_kind': type(exc).__name__}
            result['status'] = 'OBSERVED' if all('error_kind' not in v for v in result['tables'].values()) else 'PARTIAL'
        finally:
            connection.close()
    except (OSError, RuntimeError, sqlite3.Error) as exc:
        result['error_kind'] = type(exc).__name__
    return result


def collect_live_packet(symbols, capabilities, root, session_date, *, sample_seconds=5, sleep=time.sleep):
    from app.services.diagnostic_agent import build_packet
    symbols = [s for s in dict.fromkeys(symbols) if s in ('SPX', 'NDX', 'RUT', 'VIX')]
    observations = []
    packets = []
    for index in range(2):
        if index:
            sleep(sample_seconds)
        collection_start = utc_now()
        receipt, health = poll_endpoint('/health/live')
        endpoints = [receipt]
        dashboards = {}
        for symbol in symbols:
            item, payload = poll_endpoint('/dashboard/symbol/' + symbol)
            endpoints.append(item)
            dashboards[symbol] = payload
        context = {'timestamp_utc': receipt['completed_at_utc'] if receipt['status'] == 'OBSERVED' else None,
                   'requested_symbols': symbols, 'pipeline': health,
                   '_collection': {'mode': 'REQUEST_TIME', 'started_at_utc': collection_start,
                                   'completed_at_utc': utc_now(), 'backend_sampled_at_utc': health.get('sampled_at_utc')}}
        packet = build_packet(context, capabilities, root, session_date)
        observations.append({'observed_at_utc': utc_now(), 'endpoints': endpoints,
                             'runtime': packet['evidence']['runtime'], 'dashboards': dashboards,
                             'reported_error_summary': health.get('reported_error_summary', {'present': False}),
                             'persistence': persistence_observation(root),
                             'captures': {k: v for k, v in packet['evidence'].items() if k.startswith('capture:')}})
        packets.append(packet)
    first, last = observations
    a, b = first['runtime'], last['runtime']
    comparable = bool(a.get('subscription_epoch_id')) and all(
        a.get(k) == b.get(k) for k in ('subscription_epoch_id', 'subscription_generation'))
    comparable = comparable and all(isinstance(v.get('messages_received'), int) for v in (a, b))
    delta = b['messages_received'] - a['messages_received'] if comparable else None
    evidence = packets[-1]['evidence']
    evidence['live_observations'] = {'samples': observations,
        'message_delta': delta, 'runtime_comparable': comparable,
        'capture_attempt_deltas': {k: last['captures'][k]['retained_attempts'] - v['retained_attempts']
                                   for k, v in first['captures'].items()},
        'meaning': 'Two bounded observations. Zero change is not proof of an outage; the interval can be shorter than the persistence cadence. Compare session phase and source times.'}
    evidence['collection_errors'] = [r for o in observations for r in o['endpoints'] if r['status'] != 'OBSERVED'][:12]
    from app.services.diagnostic_forecast_binding import collect_bindings
    evidence['forecast_bindings'] = collect_bindings(last['dashboards'], last['observed_at_utc'])
    # Keep current findings before historical excerpts in the submitted packet.
    packets[-1]['evidence'] = dict(sorted(evidence.items(), key=lambda item: item[0].startswith('docs/')))
    return packets[-1]
