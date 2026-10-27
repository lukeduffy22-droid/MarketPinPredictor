from copy import deepcopy
from datetime import datetime, timedelta, timezone
import json

from tools.evaluate_shadow_walk_forward import DEFINITIONS, eligible, evaluate_rows

NOW = datetime(2026, 10, 20, tzinfo=timezone.utc)
START = datetime(2026, 10, 5, 14, tzinfo=timezone.utc)


def pair(at):
    result = []
    for name, definition in DEFINITIONS.items():
        result.append(dict(
            prediction_id=f'{at.isoformat()}:{name}', symbol='SPX', formula_id=name,
            formula_version=definition.version, equation=definition.equation,
            coefficients_json=json.dumps(dict(definition.coefficients)),
            abstained=0, mode='shadow', production_signal_replaced=0,
            provenance_json=json.dumps({'is_fallback': False, 'subscription_epoch_id': 'a'*64, 'subscription_generation': 1}),
            ts_event_utc=(at-timedelta(seconds=2)).isoformat(),
            ts_recv_utc=(at-timedelta(seconds=1)).isoformat(),
            observation_index_utc=(at-timedelta(seconds=1)).isoformat(),
            processed_at_utc=at.isoformat(), prediction_timestamp_utc=at.isoformat(),
            created_at_utc=(at+timedelta(milliseconds=100)).isoformat(),
            target_timestamp_utc=(at+timedelta(minutes=5)).isoformat(),
            realized_timestamp_utc=(at+timedelta(minutes=5, seconds=1)).isoformat(),
            scored_at_utc=(at+timedelta(minutes=5, seconds=2)).isoformat(),
            horizon_seconds=300, spot=100., predicted_price=100., realized_price=101.,
            feature_snapshot_json=json.dumps({'feature_asof_utc': {key:(at-timedelta(seconds=1)).isoformat() for key in definition.required_raw_features}}),
        ))
    return result


def test_chronological_prior_sessions_disjoint_tests_and_embargo():
    rows = [r for day in (0, 1, 2, 3, 4, 7) for minute in (0, 1, 2, 12)
            for r in pair(START+timedelta(days=day, minutes=minute))]
    result = evaluate_rows(rows, now=NOW)
    report = result['reports'][0]
    assert report['test_sessions'] == 1 and report['baseline']['n'] == 2
    assert report['folds'][0]['prior_sessions'] == 5
    assert report['folds'][0]['overlap_or_embargo_excluded'] == 2
    assert not result['promotion_supported'] and not result['policy']['coefficients_fitted']
    saved = deepcopy(rows)
    evaluate_rows(rows, now=NOW)
    assert rows == saved


def test_delayed_future_or_lookahead_outcomes_are_excluded():
    original = pair(START)[0]
    assert eligible(original, NOW) is None
    for field, value, expected in (
        ('created_at_utc', (START+timedelta(minutes=6)).isoformat(), 'INVALID_TIME_ORDER'),
        ('scored_at_utc', (NOW+timedelta(days=1)).isoformat(), 'INVALID_TIME_ORDER'),
        ('horizon_seconds', 60, 'HORIZON_MISMATCH'),
    ):
        assert eligible({**original, field:value}, NOW) == expected
    late = {**original, 'realized_timestamp_utc':(START+timedelta(minutes=7)).isoformat(),
            'scored_at_utc':(START+timedelta(minutes=7, seconds=1)).isoformat()}
    assert eligible(late, NOW) == 'OUTCOME_MORE_THAN_60_SECONDS_LATE'
    features = json.loads(original['feature_snapshot_json'])
    features['feature_asof_utc']['spot'] = (START+timedelta(minutes=1)).isoformat()
    assert eligible({**original, 'feature_snapshot_json':json.dumps(features)}, NOW) == 'FEATURE_TIME_UNVERIFIED'


def test_duplicate_and_mismatched_pairs_cannot_be_selected():
    rows = pair(START)
    duplicate = evaluate_rows(rows + [rows[0]], now=NOW)
    assert duplicate['reports'] == []
    assert duplicate['excluded_rows_by_reason']['MISSING_OR_DUPLICATE_PAIR'] == 3
    rows[1]['realized_price'] = 102.
    mismatch = evaluate_rows(rows, now=NOW)
    assert mismatch['reports'] == []
    assert mismatch['excluded_rows_by_reason']['PAIR_OUTCOME_MISMATCH'] == 2


def test_prior_outcomes_must_have_been_available_before_test_day():
    rows = [r for day in (0, 1, 2, 3, 4, 7) for r in pair(START+timedelta(days=day))]
    for row in rows[:2]:
        row['scored_at_utc'] = (START+timedelta(days=7)).isoformat()
    result = evaluate_rows(rows, now=NOW)
    assert result['reports'][0]['test_sessions'] == 0
