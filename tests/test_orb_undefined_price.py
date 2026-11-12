import copy
import hashlib
import json
import math
from datetime import datetime, timedelta, timezone

import databento_dbn as dbn
import pytest
from sqlalchemy import select, text

from backend import database
from backend.databento_streamer import normalize_price
from backend.market_structure import MarketStructureJournal
from tests.test_orb_cbbo_capture import cbbo_store, _quotes
from tests.test_orb_e2e_rehearsal import persistent_orb_journal, OPEN_UTC, TRADING_DATE
from tests.test_market_structure_orb import _memory_journal, _payload, _reference_payload

UNDEFINED = dbn.UNDEF_PRICE / dbn.FIXED_PRICE_SCALE


@pytest.mark.parametrize('value', [dbn.UNDEF_PRICE, float(dbn.UNDEF_PRICE), UNDEFINED, float('inf')])
def test_decoder_rejects_undefined_price_before_scaling(value):
    assert normalize_price(value) is None


def test_reference_writer_rejects_cancelling_undefined_pair_prices():
    opening = datetime(2026, 9, 4, 13, 30, tzinfo=timezone.utc)
    at = opening + timedelta(seconds=1)
    journal, *_ = _memory_journal(now=at)
    payload = _reference_payload(_payload(opening, 6500.0), sample_timestamp=opening)
    pairs = payload['formula_inputs']['pairs']
    for pair in pairs:
        pair['call_mid'] = pair['put_mid'] = UNDEFINED
    payload['pair_identity_sha256'] = hashlib.sha256(json.dumps(pairs, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
    values = sorted(pair['strike'] * math.exp(-payload['risk_free_rate'] * payload['time_to_expiration_years']) for pair in pairs)
    payload['reference_price'] = values[len(values) // 2]
    assert journal.record_reference(payload) == {'recorded': False, 'reason': 'REFERENCE_OPTION_PRICE_INVALID'}


def test_latest_invalid_price_does_not_promote_an_earlier_reference():
    opening = datetime(2026, 9, 4, 13, 30, tzinfo=timezone.utc)
    journal, _ = _memory_journal(now=opening + timedelta(seconds=7))
    for offset in (0, 5):
        at = opening + timedelta(seconds=offset)
        assert journal.record_reference(_reference_payload(_payload(at, 6500.0), sample_timestamp=at))['recorded']
    rows = journal.reference_loader('SPX', opening.date())
    rows[-1]['reference_price_inputs_valid'] = False
    journal.reference_loader = lambda *args, **kwargs: rows
    state = journal.snapshot('SPX')
    assert state['current_price'] is None
    assert state['current_reference_fresh'] is False
    assert state['directional_evidence_eligible'] is False
    assert 'LATEST_REFERENCE_OPTION_PRICE_INVALID' in state['warnings']
    assert all(window['current_price'] is None for window in state['opening_ranges'].values())


def test_retained_undefined_prices_are_excluded_without_mutating_raw_records(cbbo_store, monkeypatch):
    streamer, journal, clock, engine = cbbo_store
    original_saver = journal.reference_saver
    stored_bad = []

    def legacy_saver(sample):
        # Reproduce a row accepted by the old decoder, bypassing today's writer.
        legacy = copy.deepcopy(sample)
        for pair in legacy['formula_inputs_json']['pairs']:
            pair['call_mid'] = pair['put_mid'] = UNDEFINED
        legacy['reference_price'] = 26996.116717640016
        legacy['formula_inputs_json'] = json.dumps(legacy['formula_inputs_json'])
        columns = {column.name for column in database.OrbCbboReferenceSample.__table__.columns}
        with database.SessionLocal() as session:
            session.add(database.OrbCbboReferenceSample(**{k: v for k, v in legacy.items() if k in columns}))
            session.commit()
        stored_bad.append(legacy)
        return legacy

    monkeypatch.setattr(journal, 'reference_saver', legacy_saver)
    observed = OPEN_UTC + timedelta(seconds=1, milliseconds=100)
    _quotes(streamer, observed)
    assert streamer._capture_opening_reference_for_bucket('SPX', OPEN_UTC, now_utc=lambda: observed, journal=journal)['recorded']
    monkeypatch.setattr(journal, 'reference_saver', original_saver)
    observed += timedelta(seconds=5)
    _quotes(streamer, observed)
    assert streamer._capture_opening_reference_for_bucket('SPX', OPEN_UTC + timedelta(seconds=5), now_utc=lambda: observed, journal=journal)['recorded']

    for loader in (database.load_orb_reference_samples, database.load_orb_reference_snapshot_samples):
        rows = loader('SPX', TRADING_DATE, as_of_utc=observed)
        assert len(rows) == 2  # Audit visibility is preserved.
        assert rows[0]['reference_price_inputs_valid'] is False
        assert rows[1]['reference_price_inputs_valid'] is True
        view = MarketStructureJournal(loader=lambda *args, **kwargs: [], reference_loader=loader, now_utc=lambda: observed)
        state = view.snapshot('SPX')
        assert state['orb_low'] > 6000 and state['orb_high'] < 7000
        assert state['capture_evidence']['sample_count'] == 1
        assert state['capture_evidence']['excluded_invalid_price_sample_count'] == 1
        assert state['capture_evidence']['opening_bucket_present'] is False
        assert 'INVALID_OPTION_PRICE_SAMPLES_EXCLUDED' in state['warnings']
        assert state['directional_evidence_eligible'] is False

    with engine.connect() as connection:
        retained = connection.execute(select(database.OrbCbboReferenceSample.formula_inputs_json).where(database.OrbCbboReferenceSample.sample_id == stored_bad[0]['sample_id'])).scalar_one()
        assert retained == stored_bad[0]['formula_inputs_json']
        assert connection.execute(text('SELECT count(*) FROM orb_cbbo_reference_samples')).scalar_one() == 2
    assert database.save_orb_reference_sample(stored_bad[0]) is None
