"""Read-only chronological evaluation of frozen five-minute research forecasts.

No fitting, coefficient selection, model promotion or journal writes. Outcomes
are retained OPRA parity observations, not official cash-index prices. The
60-second outcome tolerance below is an explicit evaluation filter, not a
change to any live gate. Each test interval is disjoint plus a five-minute
embargo, and only earlier, already-scored sessions supply prior evidence.
"""
from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path
import sqlite3
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from tools.evaluate_shadow_formulas import (
    BASELINE_ID, CANDIDATE_ID, CENTRAL, _finite, _metrics, _parse_timestamp,
    _process_identity, _safe_json,
)
from backend.research.shadow_formula import NAIVE_LAST_PRICE_V1, PIN_CONTEXT_LINEAR_V1

DEFINITIONS = {d.formula_id: d for d in (NAIVE_LAST_PRICE_V1, PIN_CONTEXT_LINEAR_V1)}
EMBARGO = timedelta(seconds=300)
OUTCOME_TOLERANCE = timedelta(seconds=60)


def eligible(row, now):
    definition = DEFINITIONS.get(row.get('formula_id'))
    if not definition or row.get('symbol') not in ('SPX', 'NDX'):
        return 'OUTSIDE_FIXED_COMPARISON'
    if row.get('abstained') != 0 or row.get('mode') != 'shadow' or row.get('production_signal_replaced') != 0:
        return 'NOT_ELIGIBLE_SHADOW'
    if (row.get('formula_version') != definition.version
            or row.get('equation') != definition.equation
            or _safe_json(row.get('coefficients_json'), None) != dict(definition.coefficients)):
        return 'FORMULA_DEFINITION_MISMATCH'
    provenance = _safe_json(row.get('provenance_json'), {})
    if _process_identity(row) is None or provenance.get('is_fallback') is not False:
        return 'IDENTITY_OR_SOURCE_UNVERIFIED'
    names = ('ts_event_utc', 'ts_recv_utc', 'observation_index_utc', 'processed_at_utc',
             'prediction_timestamp_utc', 'created_at_utc', 'target_timestamp_utc',
             'realized_timestamp_utc', 'scored_at_utc')
    times = [_parse_timestamp(row.get(name)) for name in names]
    if any(t is None for t in times):
        return 'MISSING_TIME_EVIDENCE'
    event, receive, index, processed, predicted, created, target, realized, scored = times
    if not event <= receive <= index <= processed <= predicted <= created < target <= realized <= scored <= now:
        return 'INVALID_TIME_ORDER'
    if row.get('horizon_seconds') != 300 or target - predicted != EMBARGO:
        return 'HORIZON_MISMATCH'
    if realized - target > OUTCOME_TOLERANCE:
        return 'OUTCOME_MORE_THAN_60_SECONDS_LATE'
    features = _safe_json(row.get('feature_snapshot_json'), {})
    asofs = features.get('feature_asof_utc', {})
    if not isinstance(asofs, dict) or any(
        (at := _parse_timestamp(asofs.get(name))) is None or at > index
        for name in definition.required_raw_features
    ):
        return 'FEATURE_TIME_UNVERIFIED'
    if any((value := _finite(row.get(name))) is None or value <= 0
           for name in ('spot', 'predicted_price', 'realized_price')):
        return 'INVALID_PRICE'
    return None


def evaluate_rows(rows, *, now):
    exclusions = Counter()
    groups = defaultdict(list)
    for row in rows:
        reason = eligible(row, now)
        if reason:
            exclusions[reason] += 1
            continue
        key = (row['symbol'], row['prediction_timestamp_utc'], *_process_identity(row))
        groups[key].append(row)
    paired = defaultdict(list)
    for key, group in groups.items():
        if len(group) != 2 or {r['formula_id'] for r in group} != set(DEFINITIONS):
            exclusions['MISSING_OR_DUPLICATE_PAIR'] += len(group)
            continue
        by_id = {r['formula_id']: r for r in group}
        a, b = by_id[BASELINE_ID], by_id[CANDIDATE_ID]
        if any(a[k] != b[k] for k in ('spot', 'target_timestamp_utc', 'realized_price', 'realized_timestamp_utc')):
            exclusions['PAIR_OUTCOME_MISMATCH'] += 2
            continue
        paired[key[0]].append((a, b))
    reports = []
    for symbol, pairs in sorted(paired.items()):
        pairs.sort(key=lambda pair: _parse_timestamp(pair[0]['prediction_timestamp_utc']))
        sessions = sorted({_parse_timestamp(a['prediction_timestamp_utc']).astimezone(CENTRAL).date() for a, b in pairs})
        folds = []
        all_baselines, all_candidates = [], []
        for day in sessions:
            boundary = datetime.combine(day, datetime.min.time(), CENTRAL).astimezone(timezone.utc)
            prior = [(a, b) for a, b in pairs if max(_parse_timestamp(r['scored_at_utc']) for r in (a, b)) + EMBARGO < boundary]
            prior_days = {_parse_timestamp(a['prediction_timestamp_utc']).astimezone(CENTRAL).date() for a, b in prior}
            if len(prior_days) < 5:
                continue
            selected, excluded_overlap = [], 0
            next_start = boundary
            for a, b in pairs:
                start = _parse_timestamp(a['prediction_timestamp_utc'])
                if start.astimezone(CENTRAL).date() != day:
                    continue
                if start < next_start:
                    excluded_overlap += 1
                    continue
                selected.append((a, b))
                next_start = _parse_timestamp(a['realized_timestamp_utc']) + EMBARGO
            if not selected:
                continue
            baselines, candidates = [a for a, b in selected], [b for a, b in selected]
            all_baselines.extend(baselines)
            all_candidates.extend(candidates)
            folds.append({'test_session': day.isoformat(), 'prior_sessions': len(prior_days),
                          'latest_prior_score_utc': max(_parse_timestamp(r['scored_at_utc']) for pair in prior for r in pair).isoformat(),
                          'overlap_or_embargo_excluded': excluded_overlap,
                          'prediction_ids': [[a['prediction_id'], b['prediction_id']] for a, b in selected],
                          'baseline': _metrics(baselines), 'candidate': _metrics(candidates)})
        baseline, candidate = _metrics(all_baselines), _metrics(all_candidates)
        reports.append({'symbol': symbol, 'test_sessions': len(folds), 'folds': folds,
                        'baseline': baseline, 'candidate': candidate,
                        'candidate_mae_improvement_points': (baseline['mae_points'] - candidate['mae_points']) if all_baselines else None})
    return {'schema_version': 'marketpin-fixed-shadow-walk-forward.v1', 'as_of_utc': now.isoformat(),
            'input_rows': len(rows), 'excluded_rows_by_reason': dict(exclusions), 'reports': reports,
            'policy': {'prior_sessions_minimum': 5, 'embargo_seconds': 300, 'outcome_tolerance_seconds': 60,
                       'coefficients_fitted': False, 'selection_uses_error_magnitude': False},
            'outcome_kind': 'retained_opra_parity_research_observation_not_official_index_price',
            'promotion_supported': False, 'accuracy_improvement_verified': False,
            'logical_input_sha256': hashlib.sha256(json.dumps(rows, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()).hexdigest()}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--journal', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists():
        parser.error('Refusing to replace an existing report')
    with sqlite3.connect(args.journal.resolve().as_uri()+'?mode=ro', uri=True, timeout=2) as conn:
        conn.execute('PRAGMA query_only=ON')
        conn.row_factory = sqlite3.Row
        rows = [dict(r) for r in conn.execute('SELECT * FROM shadow_prediction_journal ORDER BY prediction_timestamp_utc, prediction_id')]
    report = evaluate_rows(rows, now=datetime.now(timezone.utc))
    with args.output.open('x', encoding='utf-8') as handle:
        json.dump(report, handle, indent=2, allow_nan=False)
    print(json.dumps({'report': str(args.output), 'symbols': [{k: r[k] for k in ('symbol', 'test_sessions', 'baseline', 'candidate')} for r in report['reports']]}))


if __name__ == '__main__':
    main()
