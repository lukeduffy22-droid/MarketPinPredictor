import hashlib
import sqlite3
from datetime import datetime, timezone

from backend.closing_tape.catalog import TapeCatalog
from backend.closing_tape.live_shadow import _replay_open_interest
from backend.closing_tape.oi_replay import (
    decode_open_interest_record,
    replay_open_interest,
    replay_open_interest_state_from_prefix,
)


UTC = timezone.utc


def _stat_record(*, action=1, quantity=1288, flags=4):
    raw = bytearray(64)
    raw[0] = 16
    raw[1] = 24
    raw[4:8] = (123).to_bytes(4, "little")
    raw[8:16] = (1_787_653_800_613_943_845).to_bytes(8, "little")
    raw[16:24] = (1_787_653_800_614_146_367).to_bytes(8, "little")
    raw[24:32] = (2**64 - 1).to_bytes(8, "little")
    raw[32:40] = (2**63 - 1).to_bytes(8, "little", signed=True)
    raw[40:48] = quantity.to_bytes(8, "little")
    raw[48:52] = (7).to_bytes(4, "little")
    raw[56:58] = (9).to_bytes(2, "little")
    raw[58:60] = (83).to_bytes(2, "little")
    raw[60] = action
    raw[61] = flags
    return raw


def _stat_record_v1(*, action=1, quantity=1288, flags=4):
    raw = bytearray(64)
    raw[0] = 16
    raw[1] = 24
    raw[4:8] = (123).to_bytes(4, "little")
    raw[8:16] = (1_787_653_800_613_943_845).to_bytes(8, "little")
    raw[16:24] = (1_787_653_800_614_146_367).to_bytes(8, "little")
    raw[24:32] = (2**64 - 1).to_bytes(8, "little")
    raw[32:40] = (2**63 - 1).to_bytes(8, "little", signed=True)
    raw[40:44] = quantity.to_bytes(4, "little")
    raw[44:48] = (7).to_bytes(4, "little")
    raw[52:54] = (9).to_bytes(2, "little")
    raw[54:56] = (83).to_bytes(2, "little")
    raw[56] = action
    raw[57] = flags
    return raw


def _observation(key, offset, action, quantity):
    return {
        "observation_key": key,
        "session_id": "s1",
        "feed_name": "opra_options",
        "source_sha256": "a" * 64,
        "record_offset": offset,
        "instrument_id": 123,
        "raw_symbol": "SPX   260825C06500000",
        "family_root": "SPX",
        "ts_event_ns": 1_787_653_800_613_943_845 + offset,
        "ts_recv_ns": 1_787_653_800_614_146_367 + offset,
        "ts_ref_ns": None,
        "asof_utc": "2026-08-25T10:30:00+00:00",
        "sequence": offset,
        "channel_id": 83,
        "update_action": action,
        "stat_flags": 0,
        "open_interest": quantity,
        "ingested_at_utc": "2026-08-25T16:00:00+00:00",
    }


def _definition_record(raw_symbol="SPX   260825C06500000"):
    raw = bytearray(496)
    raw[0] = 124
    raw[1] = 19
    raw[4:8] = (123).to_bytes(4, "little")
    symbol = raw_symbol.encode("ascii")
    raw[238 : 238 + len(symbol)] = symbol
    raw[493] = ord("A")
    return raw


def _prefix_stat_record(
    *,
    action=1,
    quantity=1288,
    received_at=datetime(2026, 8, 25, 19, 44, tzinfo=UTC),
):
    event_ns = int(received_at.timestamp() * 1_000_000_000) - 1_000
    receive_ns = int(received_at.timestamp() * 1_000_000_000)
    raw = _stat_record(action=action, quantity=quantity, flags=0)
    raw[8:16] = event_ns.to_bytes(8, "little")
    raw[16:24] = receive_ns.to_bytes(8, "little")
    raw[24:32] = event_ns.to_bytes(8, "little")
    return raw


def test_binary_oi_decoder_preserves_timestamps_action_flags_and_quantity():
    decoded = decode_open_interest_record(_stat_record(), 0, 64)

    assert decoded == {
        "instrument_id": 123,
        "ts_event_ns": 1_787_653_800_613_943_845,
        "ts_recv_ns": 1_787_653_800_614_146_367,
        "ts_ref_ns": None,
        "sequence": 7,
        "channel_id": 83,
        "update_action": 1,
        "stat_flags": 4,
        "open_interest": 1288.0,
    }

    deleted = decode_open_interest_record(_stat_record(action=2), 0, 64)
    assert deleted["update_action"] == 2
    assert deleted["open_interest"] is None


def test_oi_replay_accepts_v1_layout_and_preserves_attested_fields(tmp_path):
    record = _stat_record_v1(quantity=1259, flags=3)
    source = tmp_path / "statistics-v1.dbn"
    source.write_bytes(b"DBN\x01\x00\x00\x00\x00" + record)
    source_sha256 = hashlib.sha256(source.read_bytes()).hexdigest()
    catalog = TapeCatalog(tmp_path / "catalog-v1.sqlite")
    catalog.upsert_instruments(
        [
            {
                "session_id": "session-v1",
                "feed_name": "opra_options",
                "instrument_id": 123,
                "raw_symbol": "RUTW  260930C03000000",
                "family_root": "RUT",
            }
        ]
    )

    result = replay_open_interest(
        source,
        catalog=catalog,
        session_id="session-v1",
        feed_name="opra_options",
        source_sha256=source_sha256,
    )

    assert result["records"] == 1
    assert result["unmapped_records"] == 0
    assert result["families"] == ["RUT"]
    with catalog.connect(read_only=True) as connection:
        row = connection.execute(
            "SELECT * FROM tape_open_interest_observations"
        ).fetchone()
    assert row["instrument_id"] == 123
    assert row["raw_symbol"] == "RUTW  260930C03000000"
    assert row["family_root"] == "RUT"
    assert row["sequence"] == 7
    assert row["channel_id"] == 83
    assert row["update_action"] == 1
    assert row["stat_flags"] == 3
    assert row["open_interest"] == 1259.0


def test_immutable_oi_observations_rebuild_latest_projection_idempotently(tmp_path):
    catalog = TapeCatalog(tmp_path / "catalog.sqlite")
    created = _observation("a" * 64 + ":100", 100, 1, 1288.0)
    deleted = _observation("a" * 64 + ":200", 200, 2, None)

    catalog.persist_open_interest_replay(
        session_id="s1",
        feed_name="opra_options",
        observations=[created, deleted],
    )
    catalog.persist_open_interest_replay(
        session_id="s1",
        feed_name="opra_options",
        observations=[created, deleted],
    )

    with sqlite3.connect(catalog.path) as connection:
        assert connection.execute(
            "SELECT COUNT(*) FROM tape_open_interest_observations"
        ).fetchone()[0] == 2
        assert connection.execute("SELECT COUNT(*) FROM tape_open_interest").fetchone()[0] == 0

    restored = _observation("a" * 64 + ":300", 300, 1, 1300.0)
    catalog.persist_open_interest_replay(
        session_id="s1",
        feed_name="opra_options",
        observations=[restored],
    )
    with sqlite3.connect(catalog.path) as connection:
        row = connection.execute(
            "SELECT open_interest, family_root FROM tape_open_interest"
        ).fetchone()
    assert row == (1300.0, "SPX")


def test_prefix_oi_replay_derives_symbol_and_horizon_state_only_from_dbn(tmp_path):
    source = tmp_path / "prefix.dbn"
    source.write_bytes(
        b"DBN\x03\x00\x00\x00\x00"
        + _definition_record()
        + _prefix_stat_record(quantity=1288)
        + _prefix_stat_record(
            quantity=9999,
            received_at=datetime(2026, 8, 25, 19, 46, tzinfo=UTC),
        )
    )

    rows = replay_open_interest_state_from_prefix(
        source,
        available_before_utc=datetime(2026, 8, 25, 19, 45, tzinfo=UTC),
    )

    assert len(rows) == 1
    assert rows[0]["raw_symbol"] == "SPX   260825C06500000"
    assert rows[0]["family_root"] == "SPX"
    assert rows[0]["open_interest"] == 1288.0


def test_paper_prefix_oi_replay_ignores_tampered_catalog_ledger(tmp_path):
    source = tmp_path / "prefix.dbn"
    source.write_bytes(
        b"DBN\x03\x00\x00\x00\x00"
        + _definition_record()
        + _prefix_stat_record(quantity=1288)
    )
    catalog = TapeCatalog(tmp_path / "catalog.sqlite")
    catalog.upsert_instruments(
        [
            {
                "session_id": "s1",
                "feed_name": "opra_options",
                "instrument_id": 123,
                "raw_symbol": "SPX   260825C06500000",
                "family_root": "SPX",
            }
        ]
    )
    catalog.persist_open_interest_replay(
        session_id="s1",
        feed_name="opra_options",
        observations=[_observation("a" * 64 + ":100", 100, 1, 999999.0)],
    )

    frame = _replay_open_interest(
        source,
        available_before_utc=datetime(2026, 8, 25, 19, 45, tzinfo=UTC),
    )

    assert frame.iloc[0]["open_interest"] == 1288.0
    with sqlite3.connect(catalog.path) as connection:
        assert connection.execute(
            "SELECT open_interest FROM tape_open_interest"
        ).fetchone()[0] == 999999.0
