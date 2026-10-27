"""Typed operational facts. The model prioritizes facts; it cannot rewrite them."""
from copy import deepcopy
import re
from app.services.diagnostic_history import observation_time

RULE_VERSION = 'diagnostic-facts-v5'
SECTION_LABELS = (
    ('working', 'Working'), ('failing', 'Data/application issues'),
    ('changed', 'Changes observed'), ('limitations', 'Forecast limitations'),
    ('not_due', 'Not due yet'), ('unverified', 'Not yet verified'),
    ('forecasts', 'Verified forecast explanations'),
)
SYMBOLS = ('SPX', 'NDX', 'RUT', 'VIX')


def _count(value):
    return isinstance(value, int) and not isinstance(value, bool) and value >= 0


def derive_findings(packet):
    """No I/O, model calls or inference of missing evidence. Each fact cites input keys."""
    evidence = packet.get('evidence') or {}
    timing = evidence.get('session_context') or {}
    runtime = evidence.get('runtime') or {}
    coverage = evidence.get('coverage_limits') or {}
    observations = evidence.get('live_observations') or {}
    samples = observations.get('samples') or []
    dashboards = (samples[-1].get('dashboards') or {}) if samples else {}
    facts = {}

    def add(code, section, text, refs, *, symbol=None, required=False, action=None):
        refs = [r for r in refs if r in evidence]
        if not refs:
            return
        identity = code + (':' + symbol if symbol else '')
        facts[identity] = {'fact_id': identity, 'code': code, 'section': section,
                           'finding': text, 'evidence_ids': refs, 'required': required}
        if action:
            facts[identity]['investigation'] = {
                'title': action[0], 'reason': text, 'proposed_change': action[1],
                'verification': action[2], 'evidence_ids': refs}

    mode = timing.get('collection_mode', 'RETAINED_OR_UNKNOWN')
    observed = observation_time(runtime.get('observed_at_utc'))
    if mode == 'REQUEST_TIME' and observed:
        add('COLLECTED_FOR_REQUEST', 'working',
            f"Runtime was collected for this request at {observed.isoformat()}. This saved report does not update afterward; quote age remains separate.",
            ['runtime', 'session_context'], required=True)
    elif not observed:
        add('RUNTIME_UNAVAILABLE', 'unverified', 'No successful runtime observation is supplied.',
            ['runtime'], required=True)
    else:
        created = observation_time(packet.get('created_at_utc') or timing.get('report_created_at_utc'))
        age = (created - observed).total_seconds() if created else None
        age_text = (f' It was {age:.1f} seconds old when this packet was created.'
                    if age is not None and age >= 0 else '')
        add('RETAINED_OBSERVATION', 'unverified',
            f"Runtime is a retained observation from {observed.isoformat()}; request-time collection is not established.{age_text}",
            ['runtime', 'session_context'], required=True)
    if timing.get('runtime_matches_selected_session') is False:
        add('SESSION_MISMATCH', 'unverified',
            'The runtime observation is missing or belongs to another session; it cannot describe live health for the selected capture session.',
            ['session_context'], required=True)
    if timing.get('runtime_phase') == 'POST_CLOSE':
        add('POST_CLOSE', 'not_due', 'The runtime observation is post-close. Quote inactivity alone does not establish a regular-session outage.',
            ['session_context'], required=True)
    window = coverage.get('closing_window')
    if window == 'NOT_STARTED':
        add('WINDOW_NOT_DUE', 'not_due',
            'The final-15-minute window has not started. Zero attempts in that future window are not a capture failure.',
            ['coverage_limits'], required=True)
    elif window == 'IN_PROGRESS':
        add('WINDOW_IN_PROGRESS', 'unverified', 'The closing window is in progress; complete closing-window coverage cannot yet be assessed.',
            ['coverage_limits'], required=True)
    elif window == 'ELAPSED_COVERAGE_NOT_VERIFIED':
        add('CLOSING_COVERAGE_UNVERIFIED', 'unverified',
            'The closing window has elapsed, but complete coverage and an official close are not established by elapsed time or attempt counts.',
            ['coverage_limits'], required=True)
    if coverage.get('opening_bucket', '').startswith('NOT_YET_VERIFIED'):
        add('OPENING_UNVERIFIED', 'unverified', 'The exact opening bucket is not verified by this packet. This does not prove the bucket was absent.',
            ['coverage_limits'], required=True,
            action=('Inspect the exact opening receipt', 'Read the existing first-bucket receipt and provenance without reconstructing missing observations.',
                    'Match the exact opening bucket, symbol, session identity and eligibility; keep missing evidence unverified.'))
    errors = evidence.get('collection_errors') or []
    if errors:
        add('COLLECTION_FAILED', 'failing', f'{len(errors)} endpoint reads failed during collection; this does not by itself prove market-data loss.',
            ['collection_errors'], required=True,
            action=('Investigate endpoint collection failures', 'Inspect the recorded endpoint status, latency and bounded error category.',
                    'A new bounded read succeeds; feed progression is checked separately.'))
    elif samples and all(o.get('endpoints') and all(r.get('status') == 'OBSERVED' for r in o['endpoints']) for o in samples):
        add('ENDPOINTS_OBSERVED', 'working', 'All recorded endpoint reads succeeded. Endpoint availability alone does not establish forecast readiness.', ['live_observations'])
    if runtime.get('handoff_status') == 'active':
        add('HANDOFF_ACTIVE', 'working', 'Handoff was active at the runtime observation; this is a transport fact, not a forecast-accuracy claim.', ['runtime'])

    required = set(runtime.get('required_symbols') or [])
    optional = set(runtime.get('optional_symbols') or [])
    for symbol in SYMBOLS:
        state = evidence.get('symbol:' + symbol)
        dash = dashboards.get(symbol) or {}
        if state is None and not dash:
            continue
        state = state or {}
        refs = ['symbol:' + symbol, 'runtime']
        invalid = state.get('validation_is_valid') is False or symbol in (runtime.get('invalid_symbols') or [])
        if invalid:
            if symbol in optional and symbol not in required:
                code, section = 'OPTIONAL_SYMBOL_EXCLUDED', 'limitations'
                text = f'{symbol} is invalid and excluded as optional pipeline context. This alone does not veto a required symbol; individual forecast dependencies are not verified here.'
            elif symbol in required and symbol not in optional:
                code, section = 'REQUIRED_SYMBOL_INVALID', 'failing'
                text = f'{symbol} is a required pipeline symbol and its inputs were invalid at the observation. This does not establish that every session attempt failed.'
            else:
                code, section = 'SYMBOL_INVALID_ROLE_UNKNOWN', 'failing'
                text = f'{symbol} inputs were invalid at the observation. Its required/optional role is unknown or conflicting; effects on other forecasts are not established.'
            add(code, section, text, refs, symbol=symbol, required=True,
                action=(f'Inspect {symbol} validation evidence', 'Trace reported failures to the contemporaneous quotes, selected contracts and calculation identity; preserve validity thresholds.',
                        'Verify the specific input failure against retained records; do not treat another symbol as repaired.'))
            reasons = state.get('validation_failure_reasons') or []
            codes = sorted({str(v).split(':', 1)[0] for v in reasons
                            if re.fullmatch(r'[A-Z][A-Z0-9_]{1,79}', str(v).split(':', 1)[0])})[:8]
            if codes:
                facts[code + ':' + symbol]['finding'] += (
                    f" Reported validation reasons: {', '.join(codes)}. These are source-reported checks, not a proven root cause."
                    + (' The observation is post-close; it does not locate when these failures began.'
                       if timing.get('runtime_phase') == 'POST_CLOSE' else ''))
        elif state.get('validation_is_valid') is True:
            add('INPUT_VALID_AT_CHECK', 'working', f'{symbol} input validation passed at the observation. This establishes input validity only, not forecast authority or accuracy.', refs, symbol=symbol)
        if state.get('is_stale') is True or symbol in (runtime.get('stale_symbols') or []):
            live_session = timing.get('runtime_matches_selected_session') is True and timing.get('runtime_phase') == 'REGULAR_SESSION'
            add('STALE_INPUT', 'failing' if live_session else 'limitations',
                f'{symbol} inputs were stale at the observation. A new poll does not make old quotes fresh.', refs, symbol=symbol, required=True)
        research = dash.get('forecast_state') == 'RESEARCH_ONLY'
        if research or dash.get('decision_grade') is False:
            add('RESEARCH_AUTHORITY' if research else 'NON_DECISION_GRADE', 'limitations',
                f'{symbol} displays a research-only estimate.' if research else f'{symbol} output is not decision-grade.',
                ['live_observations'], symbol=symbol, required=True)
        if dash:
            source = dash.get('spot_source') or (dash.get('pin_payload') or {}).get('spot_source')
            if source == 'databento_opra_put_call_parity':
                add('DERIVED_REFERENCE', 'limitations', f'{symbol} reference is derived from OPRA put/call parity; it is not an observed cash-index quote.',
                    ['live_observations'], symbol=symbol, required=True)
            else:
                add('REFERENCE_SOURCE_UNKNOWN', 'unverified', f'{symbol} reference-price source kind is not established by the allowlisted fields; do not label it cash spot.',
                    ['live_observations'], symbol=symbol, required=True)
        capture = evidence.get('capture:' + symbol) or {}
        last_valid = observation_time(capture.get('last_valid_attempt_utc'))
        if last_valid and capture.get('session_date_ct') == packet.get('session_date_ct'):
            add('LAST_VALID_ATTEMPT', 'unverified',
                f'{symbol}: Last valid retained attempt for the selected session was {last_valid.isoformat()}. '
                'This is input-validation evidence, not an official close or proof of forecast accuracy.',
                ['capture:' + symbol], symbol=symbol, required=True)
        n, valid = capture.get('retained_attempts'), capture.get('valid_attempts')
        if _count(n) and _count(valid) and valid <= n:
            add('RETAINED_ATTEMPTS', 'working' if valid else 'unverified',
                f'{symbol}: {valid} valid of {n} retained attempts. These counts do not establish unique events, continuous coverage or predictive accuracy.',
                ['capture:' + symbol], symbol=symbol)
        late, late_valid = capture.get('final_15_minutes_attempts'), capture.get('final_15_minutes_valid_attempts')
        if _count(late) and _count(late_valid) and late_valid <= late and window == 'ELAPSED_COVERAGE_NOT_VERIFIED':
            add('CLOSING_ATTEMPTS', 'unverified',
                f'{symbol}: {late_valid} valid of {late} retained attempts in the final 15 minutes. This measures retained validation results, not complete coverage, an official close, or forecast accuracy.',
                ['capture:' + symbol, 'coverage_limits'], symbol=symbol, required=True)

        persistence = (samples[-1].get('persistence') or {}) if samples else {}
        tables = persistence.get('tables') or {}
        # Preserve the stored timestamp verbatim; legacy SQLite values may have no timezone.
        row_times = []
        for table in ('gamma_calculation_runs', 'prediction_snapshots'):
            value = (tables.get(table) or {}).get(symbol)
            if isinstance(value, str) and re.fullmatch(r'\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})?', value):
                row_times.append(f'{table}: {value}')
        if row_times:
            add('LOCAL_PERSISTENCE_TIMES', 'unverified',
                f"{symbol}: Latest row timestamps in the configured local database — {'; '.join(row_times)}. "
                'These are stored event timestamps, not commit times or proof of the running backend database. '
                'A timestamp without an offset does not establish its timezone.',
                ['live_observations'], symbol=symbol, required=True)

    if len(samples) == 2:
        a, b = (o.get('runtime') or {} for o in samples)
        comparable = bool(a.get('subscription_epoch_id')) and all(a.get(k) == b.get(k) and a.get(k) is not None
            for k in ('subscription_epoch_id', 'subscription_generation'))
        if comparable and _count(a.get('messages_received')) and _count(b.get('messages_received')):
            delta = b['messages_received'] - a['messages_received']
            if delta >= 0:
                add('MESSAGE_CHANGE', 'changed', f'Message count changed by {delta} between the two same-identity observations. This short interval does not establish continuous coverage or an outage.', ['live_observations'])
        for symbol in SYMBOLS:
            ca, cb = [(o.get('captures') or {}).get('capture:' + symbol) or {} for o in samples]
            x, y = ca.get('retained_attempts'), cb.get('retained_attempts')
            if _count(x) and _count(y) and y >= x and ca.get('session_date_ct') == cb.get('session_date_ct') == packet.get('session_date_ct'):
                add('CAPTURE_COUNT_CHANGE', 'changed', f'{symbol} retained-attempt count increased by {y - x} between observations. No increase over this short interval is not proof of an outage.', ['live_observations'], symbol=symbol)
    if 'source' in evidence:
        add('DEPLOYMENT_UNVERIFIED', 'unverified', 'Disk source hashes do not establish loaded deployment identity. Local database row timestamps do not prove runtime database identity or commit times.',
            ['source', 'live_observations'])
    bindings = evidence.get('forecast_bindings') or {}
    if bindings and samples:
        from app.services.diagnostic_forecast_binding import STATUS_MESSAGES, verify_binding, verified_forecast_claims
        for symbol in SYMBOLS:
            if symbol not in bindings:
                continue
            binding = bindings[symbol]
            passport = binding.get('passport')
            checked = (verify_binding(symbol, dashboards.get(symbol) or {}, passport,
                                      samples[-1].get('observed_at_utc')) if passport else
                       {'status': binding.get('status') if binding.get('status') in STATUS_MESSAGES else 'UNBOUND'})
            if checked.get('status') == 'BOUND' and passport['target']['trading_date'] != packet.get('session_date_ct'):
                checked = {'status': 'SESSION_MISMATCH'}
            if checked.get('status') != 'BOUND':
                reason = STATUS_MESSAGES.get(checked.get('status'), 'A matching immutable passport was not verified.')
                add('FORECAST_UNBOUND', 'unverified',
                    f'{symbol}: Unbound research estimate. {reason} Numeric forecast explanations are unavailable.',
                    ['forecast_bindings'], symbol=symbol, required=True)
                continue
            age = checked['source_age_seconds']
            source = ('OPRA-derived reference, not an observed cash-index quote' if
                      checked['reference_source_kind'] == 'OPRA_DERIVED_REFERENCE' else 'unknown reference-price source kind')
            freshness = {'STALE': 'stale for current use', 'UNVERIFIED': 'freshness threshold unverified',
                         'WITHIN_REPORTED_FRESHNESS_LIMIT': 'within the reported freshness limit'}[checked['current_use']]
            add('FORECAST_BOUND', 'forecasts',
                f"{symbol}: Exact forecast {checked['forecast_id']} is bound. Issued {checked['issued_at_utc']}; target {checked['target_at_utc']}. Source age at observation: {age:.3f} seconds ({freshness}); {source}. This binding does not promote forecast authority.",
                ['forecast_bindings', 'live_observations'], symbol=symbol, required=True)
            add('CALIBRATION_LIMIT', 'forecasts',
                f"{symbol}: Calibration is {'unavailable' if checked['calibration_status'] == 'UNAVAILABLE' else 'not verified by this review'}. Raw or heuristic confidence is not a calibrated hit probability.",
                ['forecast_bindings'], symbol=symbol, required=True)
            for claim in verified_forecast_claims(passport):
                code = 'PASSPORT_' + claim['claim_code']
                add(code, 'forecasts', symbol + ': ' + claim['statement'],
                    ['forecast_bindings'], symbol=symbol, required=True)
                facts[code + ':' + symbol]['evidence_paths'] = [
                    'forecast_bindings.' + symbol + '.passport.' + path for path in claim['evidence_paths']]
    return facts


SELECTION_SCHEMA = {'type': 'object', 'additionalProperties': False,
    'properties': {k: {'type': 'array', 'items': {'type': 'string'}, 'maxItems': limit}
                   for k, limit in (('finding_ids', 12), ('investigation_ids', 5))},
    'required': ['finding_ids', 'investigation_ids']}


def selection_schema(facts):
    schema = deepcopy(SELECTION_SCHEMA)
    schema['properties']['finding_ids']['items']['enum'] = list(facts)
    actions = [key for key, item in facts.items() if item.get('investigation')]
    if actions:
        schema['properties']['investigation_ids']['items']['enum'] = actions
    else:
        schema['properties']['investigation_ids']['maxItems'] = 0
    return schema


def render_selection(selection, facts):
    """Validate again locally, then render all words/numbers from trusted templates."""
    import jsonschema
    jsonschema.validate(selection, SELECTION_SCHEMA)
    for key in ('finding_ids', 'investigation_ids'):
        values = selection[key]
        if len(set(values)) != len(values) or any(v not in facts for v in values):
            raise ValueError('UNSUPPORTED_FACT_SELECTION')
    if any(not facts[k].get('investigation') for k in selection['investigation_ids']):
        raise ValueError('UNSUPPORTED_INVESTIGATION_SELECTION')
    ordered = list(dict.fromkeys([*selection['finding_ids'], *(k for k, v in facts.items() if v['required'])]))
    result = {key: [] for key, _ in SECTION_LABELS}
    for key in ordered:
        fact = facts[key]
        result[fact['section']].append({k: fact[k] for k in ('fact_id', 'code', 'finding', 'evidence_ids', 'evidence_paths') if k in fact})
    collection = next((v['finding'] for k, v in facts.items() if k in ('COLLECTED_FOR_REQUEST', 'RETAINED_OBSERVATION', 'RUNTIME_UNAVAILABLE')), '')
    summary = [collection] if collection else []
    summary.append(f"Data/application issues established by these checks: {len(result['failing'])}. Forecast limitations: {len(result['limitations'])}. Unchecked behavior remains unverified.")
    if 'WINDOW_NOT_DUE' in facts:
        summary.append('The closing window is not due yet, which is not a failure.')
    if any(v['code'] == 'RESEARCH_AUTHORITY' for v in facts.values()):
        summary.append('Research-only estimates remain research-only; input validation does not establish forecast accuracy.')
    result.update(summary=' '.join(summary), uncertainties=[],
                  change_requests=[facts[k]['investigation'] for k in selection['investigation_ids']])
    return result
