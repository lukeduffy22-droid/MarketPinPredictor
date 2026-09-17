"""Immutable local operational reviews, separate from prediction authority."""
from collections import Counter
from datetime import date, datetime, timedelta, timezone
import hashlib
import json
import os
from pathlib import Path
from zoneinfo import ZoneInfo

CT = ZoneInfo('America/Chicago')


def available_capture_dates(root):
    """Find retained session candidates without loading every audit payload."""
    dates = set()
    for path in (Path(root) / 'logs/audit').glob('*/*.json'):
        try:
            stamp = datetime.strptime(path.stem, '%Y%m%d-%H%M%S').replace(tzinfo=timezone.utc)
        except ValueError:
            continue
        dates.add(stamp.astimezone(CT).date().isoformat())
    from app.utils.snapshot_history import _snapshot_timestamp
    for path in (Path(root) / 'logs/capture_attempts').glob('*.ndjson'):
        with path.open(encoding='utf-8') as stream:
            for line in stream:
                try:
                    stamp = _snapshot_timestamp(json.loads(line))
                except (ValueError, AttributeError):
                    continue
                if stamp:
                    dates.add(stamp.astimezone(CT).date().isoformat())
    return sorted(dates, reverse=True)


def observation_time(value):
    try:
        parsed = datetime.fromisoformat(str(value).replace('Z', '+00:00'))
        return parsed.astimezone(timezone.utc) if parsed.tzinfo else None
    except ValueError:
        return None


def session_context(observed_at, day, now):
    from backend.forecast_calendar import session_bounds
    observed = observation_time(observed_at)
    result = {'selected_session_date_ct': day, 'runtime_observed_at_utc': observed_at,
              'runtime_session_date_ct': observed.astimezone(CT).date().isoformat() if observed else None,
              'runtime_age_seconds': (now - observed).total_seconds() if observed else None,
              'runtime_phase': 'UNKNOWN', 'calendar_status': 'UNKNOWN'}
    result['runtime_matches_selected_session'] = result['runtime_session_date_ct'] == day
    try:
        opening, closing = session_bounds(date.fromisoformat(day))
        result.update(calendar_status='TRADING_SESSION', cash_open_utc=opening.isoformat(),
                      cash_close_utc=closing.isoformat())
        if observed and result['runtime_matches_selected_session']:
            result['runtime_phase'] = ('PREMARKET' if observed < opening else
                                       'REGULAR_SESSION' if observed < closing else 'POST_CLOSE')
    except ValueError as exc:
        result['calendar_status'] = str(exc)
    result['meaning'] = ('Runtime is a timestamped retained observation, not a fresh poll. '
                         'Post-close inactivity alone is not a regular-session outage. '
                         'Different-session runtime must not characterize the selected session.')
    return result


def summarize_attempts(rows, timing):
    from app.utils.snapshot_history import _snapshot_timestamp
    valid = [row for row in rows if row.get('validation_is_valid') is True]
    reasons = Counter(str(reason) for row in rows
                      for reason in (row.get('validation_failure_reasons') or []))
    opening = observation_time(timing.get('cash_open_utc'))
    closing = observation_time(timing.get('cash_close_utc'))
    regular = [row for row in rows if opening and closing and
               _snapshot_timestamp(row) is not None and opening <= _snapshot_timestamp(row) < closing]
    late = [row for row in regular if _snapshot_timestamp(row) >= closing - timedelta(minutes=15)]
    return {
        'retained_attempts': len(rows), 'valid_attempts': len(valid),
        'invalid_attempts': sum(row.get('validation_is_valid') is False for row in rows),
        'unknown_validity_attempts': sum(row.get('validation_is_valid') not in (True, False) for row in rows),
        'regular_session_attempts': len(regular),
        'regular_session_valid_attempts': sum(row.get('validation_is_valid') is True for row in regular),
        'final_15_minutes_attempts': len(late),
        'final_15_minutes_valid_attempts': sum(row.get('validation_is_valid') is True for row in late),
        'attempt_records_sha256': hashlib.sha256(
            json.dumps(rows, sort_keys=True, separators=(',', ':'), default=str).encode()).hexdigest(),
        'first_attempt_utc': _snapshot_timestamp(rows[0]).isoformat() if rows else None,
        'last_valid_attempt_utc': _snapshot_timestamp(valid[-1]).isoformat() if valid else None,
        'failure_reason_counts': dict(sorted(reasons.items())),
        'meaning': 'Retained attempt records; counts are not unique market events or proof of continuous capture. '
                   'Last-attempt validity is not whole-session validity. Missing attempts are not reconstructed.',
    }


def save_review(review, root):
    """Create an exclusive content-addressed file; retries never overwrite evidence."""
    if review.get('schema_version') != 'diagnostic-review-v1' or review.get('research_only') is not True:
        raise ValueError('Unsupported diagnostic review')
    day = review.get('packet', {}).get('session_date_ct')
    day = date.fromisoformat(day).isoformat()
    raw = (json.dumps(review, sort_keys=True, indent=2, allow_nan=False) + '\n').encode()
    digest = hashlib.sha256(raw).hexdigest()
    directory = Path(root) / 'data' / 'diagnostic_reviews' / day
    directory.mkdir(parents=True, exist_ok=True)
    destination = directory / f'{digest}.json'
    try:
        with destination.open('xb') as stream:
            stream.write(raw)
            stream.flush()
            os.fsync(stream.fileno())
    except FileExistsError:
        if destination.read_bytes() != raw:
            raise ValueError('Existing review conflicts with its content identity')
    return destination


def load_reviews(root, day):
    day = date.fromisoformat(day).isoformat()
    results = []
    for path in sorted((Path(root) / 'data' / 'diagnostic_reviews' / day).glob('*.json')):
        raw = path.read_bytes()
        if hashlib.sha256(raw).hexdigest() != path.stem:
            raise ValueError('Saved review failed its content hash check')
        value = json.loads(raw)
        if value.get('packet', {}).get('session_date_ct') != day:
            raise ValueError('Saved review session mismatch')
        results.append(value)
    return sorted(results, key=lambda item: item['generated_at_utc'])
