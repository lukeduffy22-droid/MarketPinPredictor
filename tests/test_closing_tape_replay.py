from datetime import date
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

from backend.closing_tape.integrity import inspect_dbn
from backend.closing_tape.replay import (
    build_minute_rows,
    canonicalize_tcbbo_order,
    parse_occ_symbol,
)
import backend.closing_tape.replay as replay_module


def test_occ_symbol_parser_preserves_family_and_contract_fields():
    parsed = parse_occ_symbol("SPXW  260930P06900000")

    assert parsed == {
        "family_root": "SPX",
        "expiration": "2026-09-30",
        "option_type": "P",
        "strike": 6900.0,
    }


def test_canonical_tcbbo_order_is_independent_of_capture_arrival_order():
    rows = [
        {
            "ts_event": pd.Timestamp("2026-08-28T18:00:00.000000300Z"),
            "ts_recv": pd.Timestamp("2026-08-28T18:00:00.000000330Z"),
            "instrument_id": 3, "price": 3.0, "size": 1, "bid_px_00": 2.9,
            "ask_px_00": 3.1, "bid_sz_00": 4, "ask_sz_00": 5, "flags": 0,
            "symbol": "SPXW  260828C07000000",
        },
        {
            "ts_event": pd.Timestamp("2026-08-28T18:00:00.000000100Z"),
            "ts_recv": pd.Timestamp("2026-08-28T18:00:00.000000130Z"),
            "instrument_id": 1, "price": 1.0, "size": 1, "bid_px_00": 0.9,
            "ask_px_00": 1.1, "bid_sz_00": 2, "ask_sz_00": 3, "flags": 0,
            "symbol": "SPXW  260828C07000000",
        },
        {
            "ts_event": pd.Timestamp("2026-08-28T18:00:00.000000200Z"),
            "ts_recv": pd.Timestamp("2026-08-28T18:00:00.000000230Z"),
            "instrument_id": 2, "price": 2.0, "size": 1, "bid_px_00": 1.9,
            "ask_px_00": 2.1, "bid_sz_00": 3, "ask_sz_00": 4, "flags": 0,
            "symbol": "SPXW  260828C07000000",
        },
    ]

    forward = canonicalize_tcbbo_order(pd.DataFrame(rows))
    reverse = canonicalize_tcbbo_order(pd.DataFrame(list(reversed(rows))))

    assert forward.equals(reverse)
    assert forward["price"].tolist() == [1.0, 2.0, 3.0]


def test_metadata_symbol_mappings_resolve_only_the_active_session_interval():
    mappings = {
        "RUTW  260827C03000000": [
            {
                "start_date": date(2026, 8, 26),
                "end_date": date(2026, 8, 27),
                "symbol": "122",
            },
            {
                "start_date": date(2026, 8, 27),
                "end_date": date(2026, 8, 28),
                "symbol": "123",
            },
        ]
    }

    assert replay_module._active_metadata_symbol_mappings(
        mappings,
        date(2026, 8, 27),
    ) == {123: "RUTW  260827C03000000"}


def test_raw_tcbbo_decoder_falls_back_to_dbn_metadata_mappings(monkeypatch, tmp_path):
    record = bytearray(80)
    record[0] = 20
    record[1] = replay_module.TCBBO_RTYPE
    replay_module._TCBBO_FIELDS.pack_into(
        record,
        4,
        123,
        1_787_653_800_613_943_845,
        2_860_000_000,
        7,
        192,
        1_787_653_800_614_146_367,
        2_850_000_000,
        2_870_000_000,
        11,
        13,
    )
    source = tmp_path / "historical-tcbbo.dbn"
    source.write_bytes(b"DBN\x01\x00\x00\x00\x00" + record)
    monkeypatch.setattr(
        replay_module,
        "_metadata_symbol_mappings",
        lambda _source, _day: {123: "RUTW  260827C03000000"},
    )

    frame = replay_module._decode_tcbbo_frame(source, 1)

    assert frame.loc[0, "instrument_id"] == 123
    assert frame.loc[0, "symbol"] == "RUTW  260827C03000000"
    assert frame.loc[0, "price"] == 2.86
    assert frame.loc[0, "size"] == 7
    assert frame.loc[0, "bid_px_00"] == 2.85
    assert frame.loc[0, "ask_px_00"] == 2.87
    assert frame.loc[0, "bid_sz_00"] == 11
    assert frame.loc[0, "ask_sz_00"] == 13


def test_replay_extracts_contract_fields_after_canonical_sort(monkeypatch, tmp_path):
    source = tmp_path / "out-of-order.dbn"
    source.write_bytes(b"test")
    rows = [
        {
            "ts_event": pd.Timestamp("2026-08-28T18:00:00.000000200Z"),
            "ts_recv": pd.Timestamp("2026-08-28T18:00:00.000000230Z"),
            "instrument_id": 1, "price": 7.0, "size": 1, "bid_px_00": 6.9,
            "ask_px_00": 7.1, "bid_sz_00": 2, "ask_sz_00": 3, "flags": 0,
            "symbol": "SPXW  260828C07000000",
        },
        {
            "ts_event": pd.Timestamp("2026-08-28T18:00:00.000000100Z"),
            "ts_recv": pd.Timestamp("2026-08-28T18:00:00.000000130Z"),
            "instrument_id": 2, "price": 8.0, "size": 1, "bid_px_00": 7.9,
            "ask_px_00": 8.1, "bid_sz_00": 2, "ask_sz_00": 3, "flags": 0,
            "symbol": "SPXW  260828C07100000",
        },
    ]
    report = SimpleNamespace(
        path=str(source.resolve()), local_file_intact=True, incomplete_reasons=(),
        sha256="a" * 64, tcbbo_records=len(rows),
    )
    monkeypatch.setattr(
        replay_module, "_decode_tcbbo_frame",
        lambda _source, _expected: pd.DataFrame(rows),
    )

    _, _, contract_observed, _, _ = build_minute_rows(
        source,
        session_id="alignment",
        verified_source_sha256=report.sha256,
        verified_integrity_report=report,
        include_contract_rows=True,
    )

    contract_by_symbol = {row["raw_symbol"]: row for row in contract_observed}
    assert contract_by_symbol["SPXW  260828C07000000"]["strike"] == 7000.0
    assert contract_by_symbol["SPXW  260828C07100000"]["strike"] == 7100.0
    assert contract_by_symbol["SPXW  260828C07000000"]["last_price"] == 7.0
    assert contract_by_symbol["SPXW  260828C07100000"]["last_price"] == 8.0


def test_probe_replay_is_deterministic_and_keeps_estimates_separate():
    sample = Path(__file__).parents[1] / ".codex_tmp" / "opra_probe_5d4993deeb574657a5908a74fe5fa7bd.dbn"
    if not sample.exists():
        pytest.skip("local OPRA probe is unavailable")

    verified_hash = inspect_dbn(sample).sha256
    observed, inferred, first_hash = build_minute_rows(
        sample, session_id="probe", verified_source_sha256=verified_hash
    )
    observed_again, inferred_again, second_hash = build_minute_rows(
        sample, session_id="probe", verified_source_sha256=verified_hash
    )

    assert first_hash == second_hash
    assert sum(row["trade_count"] for row in observed) == 1250
    assert sum(
        row["at_ask_count"] + row["at_bid_count"] + row["inside_count"] + row["unknown_count"]
        for row in inferred
    ) == 1250
    assert all("at_ask_count" not in row for row in observed)
    assert sum(row["flag_last_count"] for row in observed) == 1250
    assert sum(row["flag_tob_count"] for row in observed) == 1250
    assert sum(row["data_quality_flagged_count"] for row in observed) == 0
    assert all(row["inference_method"] == "trade_price_vs_pretrade_nbbo" for row in inferred)
    assert all(row["inference_version"] == "1.0" for row in inferred)
    assert [row["updated_at_utc"] for row in observed] != []
    assert len(observed_again) == len(observed)
    assert len(inferred_again) == len(inferred)

    (
        _, _, contract_observed, contract_inferred, contract_hash,
    ) = build_minute_rows(
        sample,
        session_id="probe",
        verified_source_sha256=verified_hash,
        include_contract_rows=True,
    )
    assert contract_hash != first_hash
    assert sum(row["trade_count"] for row in contract_observed) == 1250
    assert sum(
        row["at_ask_count"] + row["at_bid_count"] + row["inside_count"] + row["unknown_count"]
        for row in contract_inferred
    ) == 1250
    assert all({"raw_symbol", "expiration", "option_type", "strike"} <= row.keys() for row in contract_observed)
    assert all("last_pretrade_midpoint" in row and "last_nbbo_event_ns" in row for row in contract_observed)
    assert all("at_ask_count" not in row for row in contract_observed)
    assert all("quoted_spread_sum" not in row for row in contract_inferred)


def test_replay_reuses_a_matching_verified_integrity_report(monkeypatch):
    sample = Path(__file__).parents[1] / ".codex_tmp" / "opra_probe_5d4993deeb574657a5908a74fe5fa7bd.dbn"
    if not sample.exists():
        pytest.skip("local OPRA probe is unavailable")
    integrity = inspect_dbn(sample)

    def unexpected_rescan(_path):
        raise AssertionError("verified DBN must not be rescanned")

    monkeypatch.setattr(replay_module, "inspect_dbn", unexpected_rescan)
    observed, inferred, _feature_hash = build_minute_rows(
        sample,
        session_id="probe",
        verified_source_sha256=integrity.sha256,
        verified_integrity_report=integrity,
    )

    assert sum(row["trade_count"] for row in observed) == 1250
    assert sum(row["at_ask_count"] + row["at_bid_count"] + row["inside_count"] + row["unknown_count"] for row in inferred) == 1250


def test_replay_can_bind_historical_rows_to_a_verified_bundle_hash(monkeypatch, tmp_path):
    source = tmp_path / "historical-tcbbo.dbn"
    source.write_bytes(b"verified raw component")
    frame = pd.DataFrame(
        [
            {
                "ts_event": pd.Timestamp("2026-08-27T15:00:00.000000100Z"),
                "ts_recv": pd.Timestamp("2026-08-27T15:00:00.000000200Z"),
                "instrument_id": 1,
                "price": 2.0,
                "size": 3,
                "bid_px_00": 1.9,
                "ask_px_00": 2.0,
                "bid_sz_00": 4,
                "ask_sz_00": 5,
                "flags": 0,
                "symbol": "SPXW  260827C07000000",
            }
        ]
    )
    raw_hash = "a" * 64
    bundle_hash = "b" * 64
    report = SimpleNamespace(
        path=str(source.resolve()),
        local_file_intact=True,
        incomplete_reasons=(),
        sha256=raw_hash,
        tcbbo_records=1,
    )
    monkeypatch.setattr(replay_module, "_decode_tcbbo_frame", lambda *_args: frame)

    _, inferred, _, contract_inferred, _ = build_minute_rows(
        source,
        session_id="historical",
        verified_source_sha256=raw_hash,
        verified_integrity_report=report,
        evidence_source_sha256=bundle_hash,
        include_contract_rows=True,
    )

    assert {row["source_sha256"] for row in inferred} == {bundle_hash}
    assert {row["source_sha256"] for row in contract_inferred} == {bundle_hash}
