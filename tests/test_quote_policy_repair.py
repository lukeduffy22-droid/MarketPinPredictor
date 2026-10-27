"""Offline retained-semantics regression; never opens a provider connection."""
from datetime import timedelta
import json

import pytest

from backend import database
from backend.market_structure import _orb_reference_provenance_identity
from backend.quote_timing import (
    OPTIONAL_LAST_SALE, SAME_DAY_LAST_SALE, cbbo_timestamps_valid,
    cbbo_formula_timing_valid, last_sale_policy,
)
from tests.test_orb_cbbo_capture import cbbo_store, _quotes
from tests.test_orb_e2e_rehearsal import OPEN_UTC, TRADING_DATE, persistent_orb_journal


def _new_quotes(streamer, at):
    event = int((at - timedelta(days=2)).timestamp()) * 10**9
    _quotes(streamer, at, event)
    for quote in streamer.quotes.values():
        quote['last_sale_validation_version'] = OPTIONAL_LAST_SALE
    return event


def test_prior_day_last_sale_is_metadata_only_under_explicit_new_policy(cbbo_store):
    streamer, journal, clock, engine = cbbo_store
    at = OPEN_UTC + timedelta(seconds=1, milliseconds=100)
    event = _new_quotes(streamer, at)
    result = streamer._capture_opening_reference_for_bucket('SPX', OPEN_UTC, now_utc=lambda: at, journal=journal)
    assert result['recorded'] and result['progress_eligible'], result
    full = database.load_orb_reference_samples('SPX', TRADING_DATE, as_of_utc=at)[0]
    projected = database.load_orb_reference_snapshot_samples('SPX', TRADING_DATE, as_of_utc=at)[0]
    assert full['earliest_ts_event_ns'] == event
    assert last_sale_policy(full) == last_sale_policy(projected) == OPTIONAL_LAST_SALE
    assert _orb_reference_provenance_identity(full) == _orb_reference_provenance_identity(projected)
    inputs = json.loads(full['formula_inputs_json'])
    assert cbbo_formula_timing_valid(full, inputs['pairs'])
    assert inputs['pairs'][0]['call_raw_ts_event_ns'] == event
    assert inputs['pairs'][0]['call_last_sale_validation_version'] == OPTIONAL_LAST_SALE
    assert not cbbo_formula_timing_valid({**full, 'last_sale_validation_version': SAME_DAY_LAST_SALE}, inputs['pairs'])
    assert not cbbo_formula_timing_valid({**full, 'formula_inputs_json': {}}, inputs['pairs'])
    inputs['pairs'][0]['put_last_sale_validation_version'] = 'unknown'
    assert not cbbo_formula_timing_valid(full, inputs['pairs'])


@pytest.mark.parametrize('bad', ['stale', 'future_interval', 'future_trade', 'wrong_generation', 'wrong_mapping', 'unknown_policy', 'missing_pair'])
def test_metadata_policy_does_not_relax_quote_admission(cbbo_store, bad):
    streamer, journal, clock, engine = cbbo_store
    at = OPEN_UTC + timedelta(seconds=1, milliseconds=100)
    _new_quotes(streamer, at)
    for symbol, quote in streamer.quotes.items():
        if bad in ('stale', 'future_interval'):
            quote['ts_recv_ns'] += (-86400 if bad == 'stale' else 5) * 10**9
            quote['ts_index_ns'] = quote['ts_recv_ns']
        elif bad == 'future_trade':
            quote['ts_event_ns'] = quote['raw_ts_event_ns'] = quote['ts_recv_ns'] + 1
        elif bad == 'wrong_generation':
            quote['generation'] -= 1
        elif bad == 'wrong_mapping':
            quote['mapping_version'] = 'f' * 64
        elif bad == 'unknown_policy':
            quote['last_sale_validation_version'] = 'unrecognized'
        elif bad == 'missing_pair' and 'P' in symbol:
            quote['generation'] -= 1
    result = streamer._capture_opening_reference_for_bucket('SPX', OPEN_UTC, now_utc=lambda: at, journal=journal)
    assert not result['recorded'], result
    assert database.load_orb_reference_samples('SPX', TRADING_DATE, as_of_utc=at) == []


@pytest.mark.parametrize('event', [0, True, 1.5, -1, 1 << 63])
def test_malformed_last_sale_still_rejected(event):
    recv = int(OPEN_UTC.timestamp()) * 10**9
    assert not cbbo_timestamps_valid('cbbo-1s', event, recv, recv, policy=OPTIONAL_LAST_SALE)


def test_old_evidence_and_unknown_policy_remain_fail_closed():
    recv = int(OPEN_UTC.timestamp()) * 10**9
    event = recv - 86400 * 10**9
    assert not cbbo_timestamps_valid('cbbo-1s', event, recv, recv)
    assert cbbo_timestamps_valid('cbbo-1s', event, recv, recv, policy=OPTIONAL_LAST_SALE)
    assert not cbbo_timestamps_valid('cbbo-1s', event, recv, recv, policy='unknown')


def test_old_and_new_policies_have_distinct_ids_and_do_not_merge_ranges(cbbo_store):
    streamer, journal, clock, engine = cbbo_store
    at = OPEN_UTC + timedelta(seconds=1, milliseconds=100)
    _quotes(streamer, at)
    old = streamer._capture_opening_reference_for_bucket('SPX', OPEN_UTC, now_utc=lambda: at, journal=journal)
    assert old['progress_eligible']
    at += timedelta(seconds=1)
    _new_quotes(streamer, at)
    conflict = streamer._capture_opening_reference_for_bucket('SPX', OPEN_UTC, now_utc=lambda: at, journal=journal)
    assert not conflict['progress_eligible']  # immutable bucket cannot be replaced
    at = OPEN_UTC + timedelta(seconds=6, milliseconds=100)
    _new_quotes(streamer, at)
    new = streamer._capture_opening_reference_for_bucket('SPX', OPEN_UTC + timedelta(seconds=5), now_utc=lambda: at, journal=journal)
    assert new['progress_eligible'], new
    rows = database.load_orb_reference_samples('SPX', TRADING_DATE, as_of_utc=at)
    assert len(rows) == 2 and len({row['sample_id'] for row in rows}) == 2
    assert len({_orb_reference_provenance_identity(row) for row in rows}) == 2
    state = journal.snapshot('SPX', as_of_utc=OPEN_UTC + timedelta(minutes=5))
    assert state['opening_ranges']['5m']['capture_status'] != 'complete'

