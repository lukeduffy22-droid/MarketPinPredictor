"""Read retained attempts separately from valid gamma exports and official closes."""
import json
from pathlib import Path
from app.utils.snapshot_history import _snapshot_timestamp, local_day_utc_bounds


def load_capture_attempts(root, symbol, local_date, display_timezone):
    start, end = local_day_utc_bounds(local_date, display_timezone)
    rows = []
    root = Path(root)
    # Only inspect the UTC dates overlapping the requested local day.
    for day in {start.date().strftime('%Y%m%d'), end.date().strftime('%Y%m%d')}:
        for path in (root / 'logs' / 'audit' / symbol).glob(f'{day}-*.json'):
            try:
                row = json.loads(path.read_text(encoding='utf-8'))
            except (OSError, ValueError):
                continue
            timestamp = _snapshot_timestamp(row)
            if timestamp is not None and start <= timestamp < end:
                rows.append(row)
    for day in {start.date(), end.date()}:
        path = root / 'logs' / 'capture_attempts' / f'{day}.ndjson'
        if not path.is_file():
            continue
        with path.open(encoding='utf-8') as handle:
            for line in handle:
                try:
                    row = json.loads(line)
                    timestamp = _snapshot_timestamp(row)
                except (ValueError, AttributeError):
                    continue
                if row.get('symbol') == symbol and timestamp and start <= timestamp < end:
                    rows.append(row)
    return sorted(rows, key=lambda r: _snapshot_timestamp(r))


def render_capture_review(symbol, local_date, display_timezone, root):
    import streamlit as st
    from app.utils.display_time import format_display_timestamp
    rows = load_capture_attempts(root, symbol, local_date, display_timezone)
    st.caption('Recording runs throughout the regular session. Valid gamma snapshots, failed attempts, and verified official closes are separate evidence.')
    if rows:
        last = rows[-1]
        st.metric('Last retained recording attempt', format_display_timestamp(
            _snapshot_timestamp(last), display_timezone, format_string='%I:%M:%S %p %Z'))
        if last.get('validation_is_valid') is not True:
            st.warning('Latest recording attempt failed: ' + '; '.join(last.get('validation_failure_reasons') or ['Unknown reason']))
        with st.expander('Recording attempts and failure evidence'):
            st.dataframe([{'UTC': str(_snapshot_timestamp(r)),
                           'Calculation valid': r.get('validation_is_valid') is True,
                           'Reasons': '; '.join(r.get('validation_failure_reasons') or [])}
                          for r in rows], hide_index=True)
            st.download_button('Download recording-attempt evidence',
                               '\n'.join(json.dumps(r) for r in rows),
                               file_name=f'{symbol}-{local_date}-attempts.ndjson',
                               mime='application/x-ndjson')
    else:
        st.warning('No retained recording attempts found for this symbol and day.')
    st.info('Official close is not established by this snapshot export. Latest gamma pin and last observed price must not be used as verified close outcomes.')
