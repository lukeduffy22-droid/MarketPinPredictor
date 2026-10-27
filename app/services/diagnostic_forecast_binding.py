"""Bounded authenticated reads and pure binding of observed forecasts to passports."""
from datetime import date, datetime, timezone
import hashlib
import json
import math
import os
import re
import time

import requests

from app.services.diagnostic_history import observation_time

BINDING_VERSION = 'diagnostic-binding-v3'
MAX_PASSPORT_BYTES = 24576
MAX_PASSPORTS = 4
SYMBOLS = ('SPX', 'NDX', 'RUT', 'VIX')
STATUS_MESSAGES = {
    'FORECAST_ID_MISSING': 'No immutable forecast ID was supplied.',
    'INVALID_FORECAST_ID': 'The supplied forecast ID is invalid.',
    'AUTHORIZATION_MISSING': 'The diagnostic process has no configured passport access credential.',
    'AUTHORIZATION_CONFIGURATION_INVALID': 'Passport access configuration conflicts or is invalid.',
    'AUTHORIZATION_DENIED': 'The passport API rejected the access credential.',
    'PASSPORT_NOT_FOUND': 'The exact forecast passport was not found.',
    'PASSPORT_SERVICE_UNAVAILABLE': 'The passport service or its access configuration is unavailable.',
    'PASSPORT_TIMEOUT': 'The bounded passport read timed out.',
    'PASSPORT_CONNECTION_FAILED': 'The passport API connection failed.',
    'PASSPORT_HTTP_FAILURE': 'The passport API returned an unsuccessful response.',
    'PASSPORT_SIZE_LIMIT': 'The passport exceeded the bounded evidence size limit.',
    'UNSAFE_PASSPORT_CONTENT': 'The passport contained credential-like content and was excluded.',
    'MALFORMED_PASSPORT': 'The passport structure or immutable integrity could not be verified.',
    'BINDING_EVIDENCE_MISSING': 'Required forecast identity or timestamp evidence is missing.',
    'IDENTITY_MISMATCH': 'The passport does not match the observed forecast identity, values or target.',
    'SESSION_MISMATCH': 'The forecast belongs to a different selected session.',
    'RETRIEVAL_LIMIT': 'The bounded passport retrieval limit was reached.',
    'UNSUPPORTED_ORIGIN': 'The passport origin is not supported by this verifier.',
}


def _now():
    return datetime.now(timezone.utc).isoformat()


def _finite(value):
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)


def fetch_passport(forecast_id, *, get=None, token=None):
    """Never follow redirects or retain credential/error bodies. Cap each response."""
    receipt = {'started_at_utc': _now(), 'outcome': 'INVALID_FORECAST_ID'}
    started = time.monotonic()
    passport = None
    if isinstance(forecast_id, str) and re.fullmatch('[0-9a-f]{64}', forecast_id):
        receipt['forecast_id'] = forecast_id
        try:
            from backend.governance_credentials import governance_token
            credential = token if token is not None else governance_token()
        except ValueError:
            receipt.update(outcome='AUTHORIZATION_CONFIGURATION_INVALID', completed_at_utc=_now(), elapsed_ms=0)
            return receipt, None
        if len(credential) < 16:
            receipt['outcome'] = 'AUTHORIZATION_MISSING'
        else:
            response = None
            try:
                response = (get or requests.get)(
                    'http://127.0.0.1:8000/v1/workstation/passports/' + forecast_id,
                    headers={'X-MarketPin-Governance-Token': credential}, timeout=(2, 3),
                    allow_redirects=False, stream=True)
                receipt['http_status'] = response.status_code
                if response.status_code != 200:
                    receipt['outcome'] = {401: 'AUTHORIZATION_DENIED', 403: 'AUTHORIZATION_DENIED',
                        404: 'PASSPORT_NOT_FOUND', 503: 'PASSPORT_SERVICE_UNAVAILABLE'}.get(
                            response.status_code, 'PASSPORT_HTTP_FAILURE')
                else:
                    raw = bytearray()
                    for chunk in response.iter_content(chunk_size=4096):
                        raw.extend(chunk)
                        if len(raw) > MAX_PASSPORT_BYTES:
                            raise OverflowError
                        if time.monotonic() - started > 5:
                            raise requests.Timeout
                    receipt['response_bytes'] = len(raw)
                    envelope = json.loads(raw)
                    # Reject rather than mutate hash-bound evidence containing secrets.
                    text = json.dumps(envelope)
                    if credential in text or re.search(r'(?i)sk-[\w-]{10,}|bearer\s+\S+', text):
                        receipt['outcome'] = 'UNSAFE_PASSPORT_CONTENT'
                    elif not isinstance(envelope, dict) or not isinstance(envelope.get('passport'), dict):
                        receipt['outcome'] = 'MALFORMED_PASSPORT'
                    else:
                        passport = {**envelope['passport'], 'record_sha256': envelope.get('record_sha256'),
                                    'created_at_utc': envelope.get('created_at_utc')}
                        # Outcome is a mutable projection and is outside Stage 2's claims.
                        receipt['response_sha256'] = hashlib.sha256(raw).hexdigest()
                        receipt['outcome'] = 'RETRIEVED'
            except OverflowError:
                receipt['outcome'] = 'PASSPORT_SIZE_LIMIT'
            except requests.Timeout:
                receipt['outcome'] = 'PASSPORT_TIMEOUT'
            except requests.RequestException:
                receipt['outcome'] = 'PASSPORT_CONNECTION_FAILED'
            except (ValueError, TypeError):
                receipt['outcome'] = 'MALFORMED_PASSPORT'
            finally:
                if response is not None:
                    response.close()
    receipt.update(completed_at_utc=_now(), elapsed_ms=round((time.monotonic() - started) * 1000))
    return receipt, passport


def verify_binding(symbol, dashboard, passport, observed_at):
    """Exact observed identity first; freshness is a separate, current-use result."""
    from backend.workstation import passport_contract_is_complete
    from backend.forecast_calendar import session_bounds
    result = {'version': BINDING_VERSION, 'status': 'MALFORMED_PASSPORT', 'checked_fields': []}
    try:
        if not passport_contract_is_complete(passport):
            return result
        origin, provenance, model, target, price = (passport[k] for k in
            ('origin', 'provenance', 'model', 'target', 'prediction'))
        prediction = dashboard.get('prediction') or {}
        pin = dashboard.get('pin_payload') or {}
        kind = origin.get('origin_kind')
        snapshot = origin.get('prediction_snapshot_id')
        observed_snapshot = dashboard.get('prediction_snapshot_id')
        if kind == 'prediction_snapshot':
            if (not isinstance(snapshot, int) or isinstance(snapshot, bool) or snapshot <= 0
                    or origin.get('origin_key') != str(snapshot) or observed_snapshot is None):
                return {**result, 'status': 'BINDING_EVIDENCE_MISSING', 'field': 'snapshot_id'}
        elif kind == 'lifecycle_revision':
            if not re.fullmatch('[0-9a-f]{64}', str(origin.get('origin_key') or '')):
                return {**result, 'status': 'MALFORMED_PASSPORT', 'field': 'origin_key'}
            if snapshot is not None or observed_snapshot is not None:
                return {**result, 'status': 'IDENTITY_MISMATCH', 'field': 'snapshot_id'}
        else:
            return {**result, 'status': 'UNSUPPORTED_ORIGIN'}
        result['checked_fields'].append('origin_kind_and_key')
        pairs = [
            ('symbol', symbol, passport.get('symbol')),
            ('dashboard_symbol', dashboard.get('symbol'), passport.get('symbol')),
            ('forecast_id', dashboard.get('forecast_id'), passport.get('forecast_id')),
            ('model_version', prediction.get('model_version'), model.get('model_version')),
            ('epoch', dashboard.get('subscription_epoch_id'), provenance.get('subscription_epoch_id')),
            ('generation', dashboard.get('subscription_generation'), provenance.get('subscription_generation')),
            ('point_estimate', prediction.get('predicted_close'), price.get('point_estimate')),
            ('reference_price', dashboard.get('current_price'), price.get('reference_price')),
            ('forecast_state', dashboard.get('forecast_state'), passport.get('state')),
        ]
        if kind == 'prediction_snapshot':
            pairs.append(('snapshot_id', observed_snapshot, snapshot))
        if pin.get('calculation_id') is not None or provenance.get('calculation_id') is not None:
            pairs.append(('calculation_id', pin.get('calculation_id'), provenance.get('calculation_id')))
        elif kind == 'lifecycle_revision':
            source_hash = dashboard.get('forecast_source_payload_sha256')
            if not isinstance(source_hash, str) or not re.fullmatch('[0-9a-f]{64}', source_hash):
                return {**result, 'status': 'BINDING_EVIDENCE_MISSING', 'field': 'source_payload_sha256'}
            pairs.append(('source_payload_sha256', source_hash, provenance.get('source_payload_sha256')))
        else:
            return {**result, 'status': 'BINDING_EVIDENCE_MISSING', 'field': 'calculation_id'}
        for field, actual, expected in pairs:
            if actual is None or expected is None:
                return {**result, 'status': 'BINDING_EVIDENCE_MISSING', 'field': field}
            if type(actual) is bool or type(expected) is bool or actual != expected:
                return {**result, 'status': 'IDENTITY_MISMATCH', 'field': field}
            result['checked_fields'].append(field)
        issued = observation_time(target.get('prediction_timestamp_utc'))
        due = observation_time(target.get('target_timestamp_utc'))
        quote = observation_time(provenance.get('quote_timestamp_utc'))
        source = observation_time(dashboard.get('source_as_of_utc'))
        issue_observed = observation_time(prediction.get('prediction_timestamp_utc') or prediction.get('timestamp'))
        asof = observation_time(observed_at)
        if not all((issued, due, quote, source, issue_observed, asof)):
            return {**result, 'status': 'BINDING_EVIDENCE_MISSING', 'field': 'timestamps'}
        _, closing = session_bounds(date.fromisoformat(target['trading_date']))
        if (quote != source or issued != issue_observed or not quote <= issued < due
                or due != closing or target.get('kind') != 'official_cash_close'
                or target.get('horizon_seconds') != int((due - issued).total_seconds())
                or issued > asof or quote > asof):
            return {**result, 'status': 'IDENTITY_MISMATCH', 'field': 'timestamps_or_horizon'}
        for key in ('target_timestamp_utc', 'prediction_timestamp_utc'):
            expected = observation_time(target[key])
            for container in (dashboard, prediction):
                if container.get(key) is not None and observation_time(container[key]) != expected:
                    return {**result, 'status': 'IDENTITY_MISMATCH', 'field': key}
        for key, expected in (('forecast_id', passport['forecast_id']),
                              ('prediction_snapshot_id', origin['prediction_snapshot_id']),
                              ('subscription_epoch_id', provenance['subscription_epoch_id']),
                              ('subscription_generation', provenance['subscription_generation']),
                              ('current_price', price['reference_price']),
                              ('horizon_seconds', target['horizon_seconds']),
                              ('model_type', model.get('model_type')),
                              ('model_artifact_sha256', model.get('model_artifact_sha256'))):
            if prediction.get(key) is not None and prediction[key] != expected:
                return {**result, 'status': 'IDENTITY_MISMATCH', 'field': key}
        for key in ('subscription_epoch_id', 'subscription_generation', 'calculation_id'):
            if pin.get(key) is not None and pin[key] != provenance.get(key):
                return {**result, 'status': 'IDENTITY_MISMATCH', 'field': key}
        if not all(_finite(price.get(k)) and price[k] > 0 for k in ('point_estimate', 'reference_price')):
            return result
        result.update(status='BOUND', forecast_id=passport['forecast_id'],
                      source_age_seconds=(asof - quote).total_seconds(),
                      issued_at_utc=issued.isoformat(), target_at_utc=due.isoformat(),
                      reference_source_kind=('OPRA_DERIVED_REFERENCE' if pin.get('spot_source') ==
                          'databento_opra_put_call_parity' else 'UNKNOWN'),
                      calibration_status='UNVERIFIED', current_use='UNVERIFIED')
        health = dashboard.get('health') or {}
        threshold = health.get('stale_after_seconds')
        if health.get('is_stale') is True or due <= asof or (
                _finite(threshold) and threshold >= 0 and result['source_age_seconds'] > threshold):
            result['current_use'] = 'STALE'
        elif _finite(threshold) and threshold >= 0 and health.get('is_stale') is False:
            result['current_use'] = 'WITHIN_REPORTED_FRESHNESS_LIMIT'
        # Presence of calibration metadata alone is not empirical calibration proof.
        if not model.get('calibration_evidence'):
            result['calibration_status'] = 'UNAVAILABLE'
        result['checked_fields'].extend(['source_time', 'issue_time', 'official_close_target', 'horizon'])
        return result
    except (AttributeError, KeyError, TypeError, ValueError, OverflowError):
        return {**result, 'status': 'MALFORMED_PASSPORT'}


def collect_bindings(dashboards, observed_at, *, fetch=fetch_passport):
    """Freeze the last observed IDs; never fetch a floating latest forecast."""
    cache, results = {}, {}
    for symbol in SYMBOLS:
        dashboard = dashboards.get(symbol)
        if not dashboard:
            continue
        forecast_id = dashboard.get('forecast_id')
        if not forecast_id:
            results[symbol] = {'version': BINDING_VERSION, 'status': 'FORECAST_ID_MISSING'}
            continue
        if not isinstance(forecast_id, str) or not re.fullmatch('[0-9a-f]{64}', forecast_id):
            results[symbol] = {'version': BINDING_VERSION, 'status': 'INVALID_FORECAST_ID'}
            continue
        if forecast_id not in cache:
            if len(cache) >= MAX_PASSPORTS:
                results[symbol] = {'version': BINDING_VERSION, 'status': 'RETRIEVAL_LIMIT'}
                continue
            cache[forecast_id] = fetch(forecast_id)
        receipt, passport = cache[forecast_id]
        binding = (verify_binding(symbol, dashboard, passport, observed_at) if passport is not None else
                   {'version': BINDING_VERSION, 'status': receipt['outcome']})
        results[symbol] = {**binding, 'retrieval_receipt': receipt, 'passport': passport}
    return results


def verified_forecast_claims(passport):
    """Reuse the existing pure analyst verifier; never call a second model."""
    from backend.passport_analyst import CLAIM_EVIDENCE_PATHS, AnalystEvidenceError, verify_analyst_answer
    claims = []
    for code in ('STATE_AND_AUTHORITY', 'TARGET_WINDOW', 'POINT_VS_REFERENCE', 'SOURCE_FRESHNESS',
                 'MODEL_PROVENANCE', 'FORECAST_INTERVAL'):
        try:
            answer = verify_analyst_answer({'response_code': 'ANSWERED', 'claims': [
                {'claim_code': code, 'evidence_paths': list(CLAIM_EVIDENCE_PATHS[code])}]}, passport)
            claims.append(answer['findings'][0])
        except AnalystEvidenceError:
            continue
    return claims
