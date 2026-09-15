import hashlib
import sqlite3

import pytest

from backend.closing_tape.catalog import TapeCatalog
from backend.closing_tape.definition_replay import replay_instrument_definitions


def _definition_record(*, instrument_id: int, symbol: str, action: str, ts_event: int) -> bytes:
    payload = bytearray(520)
    payload[0] = 130
    payload[1] = 19
    payload[4:8] = instrument_id.to_bytes(4, "little")
    payload[8:16] = ts_event.to_bytes(8, "little")
    payload[16:24] = (ts_event + 1_000).to_bytes(8, "little")
    encoded = symbol.encode("ascii")
    payload[238 : 238 + len(encoded)] = encoded
    payload[493] = ord(action)
    return bytes(payload)


def _definition_record_v1(
    *, instrument_id: int, symbol: str, action: str, ts_event: int
) -> bytes:
    payload = bytearray(360)
    payload[0] = 90
    payload[1] = 19
    payload[4:8] = instrument_id.to_bytes(4, "little")
    payload[8:16] = ts_event.to_bytes(8, "little")
    payload[16:24] = (ts_event + 1_000).to_bytes(8, "little")
    encoded = symbol.encode("ascii")
    payload[200 : 200 + len(encoded)] = encoded
    payload[349] = ord(action)
    return bytes(payload)


def test_definition_replay_accepts_v1_layout_and_preserves_source_bytes(
    tmp_path,
):
    definition = _definition_record_v1(
        instrument_id=43,
        symbol="RUTW  260930C03000000",
        action="A",
        ts_event=1_800_000_002_000_000_000,
    )
    source = tmp_path / "definitions-v1.dbn"
    source.write_bytes(b"DBN\x01\x00\x00\x00\x00" + definition)
    source_sha256 = hashlib.sha256(source.read_bytes()).hexdigest()
    catalog = TapeCatalog(tmp_path / "catalog-v1.sqlite")

    result = replay_instrument_definitions(
        source,
        catalog=catalog,
        session_id="session-v1",
        feed_name="opra_options",
        source_sha256=source_sha256,
    )

    assert result["records"] == 1
    assert result["parsed_contracts"] == 1
    with catalog.connect(read_only=True) as connection:
        row = connection.execute(
            "SELECT * FROM tape_instrument_definition_observations"
        ).fetchone()
    assert row["instrument_id"] == 43
    assert row["raw_symbol"] == "RUTW  260930C03000000"
    assert row["family_root"] == "RUT"
    assert row["security_update_action"] == "A"
    assert row["record_bytes"] == 360
    assert row["raw_record"] == definition
    assert row["raw_record_sha256"] == hashlib.sha256(definition).hexdigest()


def test_definition_replay_is_immutable_complete_and_idempotent(tmp_path):
    first = _definition_record(
        instrument_id=42, symbol="SPXW  260930P06900000", action="A", ts_event=1_800_000_000_000_000_000,
    )
    correction = _definition_record(
        instrument_id=42, symbol="SPXW  260930P06900000", action="M", ts_event=1_800_000_001_000_000_000,
    )
    source = tmp_path / "definitions.dbn"
    source.write_bytes(b"DBN\x00\x00\x00\x00\x00" + first + correction)
    source_sha256 = hashlib.sha256(source.read_bytes()).hexdigest()
    catalog = TapeCatalog(tmp_path / "catalog.sqlite")

    result = replay_instrument_definitions(
        source, catalog=catalog, session_id="session", feed_name="opra_options",
        source_sha256=source_sha256, batch_size=1,
    )
    repeated = replay_instrument_definitions(
        source, catalog=catalog, session_id="session", feed_name="opra_options",
        source_sha256=source_sha256, batch_size=2,
    )

    assert result == {
        "records": 2, "inserted": 2, "parsed_contracts": 2,
        "source_sha256": source_sha256,
    }
    assert repeated["records"] == 2
    assert repeated["inserted"] == 0
    with catalog.connect(read_only=True) as connection:
        rows = connection.execute(
            "SELECT * FROM tape_instrument_definition_observations ORDER BY record_offset"
        ).fetchall()
    assert len(rows) == 2
    assert [row["security_update_action"] for row in rows] == ["A", "M"]
    assert rows[0]["family_root"] == "SPX"
    assert rows[0]["option_type"] == "P"
    assert rows[0]["expiration"] == "2026-09-30"
    assert rows[0]["strike"] == 6900.0
    assert rows[0]["raw_record"] == first
    assert rows[0]["raw_record_sha256"] == hashlib.sha256(first).hexdigest()
    with catalog.connect() as connection:
        with pytest.raises(sqlite3.IntegrityError, match="immutable"):
            connection.execute(
                "UPDATE tape_instrument_definition_observations SET raw_symbol='changed'"
            )
    with catalog.connect() as connection:
        with pytest.raises(sqlite3.IntegrityError, match="immutable"):
            connection.execute("DELETE FROM tape_instrument_definition_observations")
