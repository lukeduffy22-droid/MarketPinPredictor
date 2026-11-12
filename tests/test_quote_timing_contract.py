from datetime import datetime, timezone

import pytest

from backend.quote_timing import (
    CBBO_TIMING, UNDEF_TIMESTAMP, cbbo_timestamps_valid, timestamp_bounds_valid,
)

OPEN = int(datetime(2026, 9, 29, 13, 30, 1, tzinfo=timezone.utc).timestamp()) * 10**9


@pytest.mark.parametrize("event", [None, UNDEF_TIMESTAMP, OPEN - 1, OPEN - 60 * 10**9])
def test_interval_does_not_require_a_trade_in_the_cash_opening_bucket(event):
    assert cbbo_timestamps_valid("cbbo-1s", event, OPEN, OPEN)


@pytest.mark.parametrize("event,recv,index", [
    (0, OPEN, OPEN), (True, OPEN, OPEN), (OPEN + 1, OPEN, OPEN),
    (OPEN - 86400 * 10**9, OPEN, OPEN), (None, OPEN + 1, OPEN + 1),
    (None, OPEN, OPEN - 1), (None, UNDEF_TIMESTAMP, UNDEF_TIMESTAMP),
])
def test_bad_timestamp_evidence_is_rejected(event, recv, index):
    assert not cbbo_timestamps_valid("cbbo-1s", event, recv, index)


def test_missing_event_is_allowed_only_by_explicit_cbbo_contract():
    payload = dict(earliest_ts_event_ns=None, latest_ts_event_ns=None,
                   earliest_ts_recv_ns=OPEN, latest_ts_recv_ns=OPEN,
                   observation_index_ns=OPEN, trade_timestamp_count=0,
                   contributing_quote_count=10)
    assert not timestamp_bounds_valid(payload)
    payload.update(timestamp_contract_version=CBBO_TIMING, quote_schema="cbbo-1s")
    assert timestamp_bounds_valid(payload)
    payload["timestamp_contract_version"] = "unknown"
    assert not timestamp_bounds_valid(payload)

def test_native_cbbo_schema_must_match_record_type():
    import databento_dbn as dbn
    from backend.quote_timing import cbbo_record_timing_valid
    record = dbn.CBBOMsg(rtype=dbn.RType.CBBO_1S, publisher_id=1, instrument_id=1,
                        ts_event=UNDEF_TIMESTAMP, price=dbn.UNDEF_PRICE, size=0,
                        side=dbn.Side.NONE, ts_recv=OPEN)
    assert cbbo_record_timing_valid('cbbo-1s', record)
    assert not cbbo_record_timing_valid('cbbo-1m', record)
    assert not cbbo_record_timing_valid('mbp-1', record)
