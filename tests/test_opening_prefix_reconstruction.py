from datetime import datetime, timedelta, timezone

import pytest

from tools.reconstruct_opening_prefix import coverage, evaluate, run


OPEN = datetime(2026, 10, 5, 13, 30, tzinfo=timezone.utc)
NS = int(OPEN.timestamp()*1e9)
POLICY = {"freshness_seconds":30, "minimum_pairs":5, "pair_limit":15,
          "risk_free_rate":0.0525, "minimum_tte_years":1/365,
          "expiry_hour_et":16, "expiry_minute_et":0}


def fixtures(offset=4):
    book, contracts = {}, {}
    for strike in range(100, 105):
        for side, mid in (("C", 10), ("P", 10)):
            symbol = f"S{strike}{side}"
            contracts[symbol] = ("SPXW", "2026-10-05", side, float(strike))
            book[symbol] = (NS+offset*10**9, mid-1, mid+1, None, len(book))
    return book, contracts


def compute(book, contracts, second=4):
    return evaluate(book, contracts, as_of_ns=NS+second*10**9, opening_ns=NS,
                    expiration="2026-10-05", policy=POLICY)


def test_exact_first_bucket_can_use_contemporaneous_four_second_quote():
    result = compute(*fixtures())
    assert result["status"] == "reconstructed"
    assert result["paired_quotes"] == 5
    assert result["classification"] == "INFERRED"
    assert result["reference_price"] == pytest.approx(101.98533, abs=.00001)


@pytest.mark.parametrize("offset,second", [(7,4), (0,4), (-1,4), (4,35)])
def test_future_preopen_and_stale_quotes_cannot_fill_first_bucket(offset, second):
    assert compute(*fixtures(offset), second=second)["status"] == "unavailable"


@pytest.mark.parametrize("invalid", ["nan", "crossed", "future_trade", "undefined"])
def test_invalid_leg_cannot_supply_required_pair(invalid):
    book, contracts = fixtures()
    recv, bid, ask, event, iid = book["S100C"]
    if invalid == "nan": bid = float("nan")
    if invalid == "crossed": bid = ask+1
    if invalid == "future_trade": event = recv+1
    if invalid == "undefined": ask = (2**63-1)/1e9
    book["S100C"] = recv,bid,ask,event,iid
    assert compute(book, contracts)["paired_quotes"] == 4


def test_duplicate_series_leg_is_not_resolved_by_arrival_order():
    book, contracts = fixtures()
    book["duplicate"] = book["S100C"]
    contracts["duplicate"] = contracts["S100C"]
    assert compute(book, contracts)["status"] == "unavailable"


def live_rows():
    return [{"sample_timestamp_utc":(OPEN+timedelta(seconds=i*5)).isoformat(), "reference_price":100} for i in range(4,60)]


def test_complete_reconstruction_never_becomes_live_prediction_authority():
    reconstructed = {i:{"status":"reconstructed", "reference_price":101} for i in range(4)}
    r = coverage(live_rows(), reconstructed, OPEN, 5, OPEN+timedelta(minutes=6))
    assert r["status"] == "reconstructed_complete"
    assert r["covered_buckets"] == 60
    assert r["recovered_buckets"] == 4
    assert r["live_capture_complete"] is False
    assert r["usable_for_prediction"] is False


def test_absent_first_bucket_stays_partial_despite_high_coverage():
    reconstructed = {i:{"status":"reconstructed", "reference_price":101} for i in range(1,4)}
    r = coverage(live_rows(), reconstructed, OPEN, 5, OPEN+timedelta(minutes=6))
    assert r["status"] == "partial"
    assert r["missing_bucket_offsets_seconds"] == [0]


def test_later_history_does_not_overwrite_live_or_fill_earlier_bucket():
    r = coverage(live_rows(), {4:{"status":"reconstructed", "reference_price":999}}, OPEN, 5, OPEN+timedelta(minutes=6))
    assert r["recovered_buckets"] == 0
    assert r["sampled_high"] == 100
    assert r["missing_bucket_offsets_seconds"] == [0,5,10,15]


def test_unfinished_window_stays_forming():
    r = coverage(live_rows(), {}, OPEN, 60, OPEN+timedelta(minutes=50))
    assert r["status"] == "forming"
    assert r["not_yet_elapsed_buckets"] == 120
    assert max(r["missing_elapsed_bucket_offsets_seconds"]) < 3000


def test_quote_exactly_on_second_five_cannot_complete_first_bucket():
    result = compute(*fixtures(5), second=5)
    assert result["status"] == "reconstructed"
    r = coverage(live_rows(), {1:result}, OPEN, 5, OPEN+timedelta(minutes=6))
    assert r["first_bucket_available"] is False
    assert 0 in r["missing_bucket_offsets_seconds"]


def test_modified_raw_artifact_rejected_before_database_or_output(tmp_path):
    import json
    raw = tmp_path/'raw.dbn.zst'
    raw.write_bytes(b'changed')
    receipt = tmp_path/'receipt.json'
    receipt.write_text(json.dumps({'path':str(raw), 'status':'downloaded', 'sha256':'0'*64}))
    output = tmp_path/'output'
    with pytest.raises(ValueError, match='hash mismatch'):
        run(receipt, tmp_path/'absent.db', tmp_path/'absent.csv', output)
    assert not output.exists()
    assert not (tmp_path/'absent.db').exists()
