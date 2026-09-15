from __future__ import annotations

import mmap
from datetime import datetime, timezone
from pathlib import Path

from .catalog import TapeCatalog, _utcnow
from .config import UTC
from .replay import parse_occ_symbol


STAT_RTYPE = 24
OPEN_INTEREST_STAT_TYPE = 9
STAT_ACTION_NEW = 1
STAT_ACTION_DELETE = 2
UNDEFINED_U64 = 2**64 - 1
STAT_RECORD_MIN_BYTES = 64
DEFINITION_RTYPE = 19
DEFINITION_MIN_BYTES = 494


def _timestamp_iso(timestamp_ns: int) -> str:
    return datetime.fromtimestamp(timestamp_ns / 1e9, tz=UTC).isoformat()


def decode_open_interest_record(
    raw: bytes | bytearray | memoryview | mmap.mmap,
    offset: int,
    record_bytes: int,
) -> dict[str, int | float | None] | None:
    """Decode the stable DBN StatMsg fields needed for immutable OI replay."""
    if record_bytes < STAT_RECORD_MIN_BYTES or int(raw[offset + 1]) != STAT_RTYPE:
        return None
    stat_type = int.from_bytes(raw[offset + 56 : offset + 58], "little")
    if stat_type != OPEN_INTEREST_STAT_TYPE:
        return None
    event_ns = int.from_bytes(raw[offset + 8 : offset + 16], "little")
    if event_ns <= 0 or event_ns == UNDEFINED_U64:
        raise ValueError(f"open-interest record at {offset} has no valid event timestamp")
    receive_ns = int.from_bytes(raw[offset + 16 : offset + 24], "little")
    reference_ns = int.from_bytes(raw[offset + 24 : offset + 32], "little")
    quantity = int.from_bytes(raw[offset + 40 : offset + 48], "little")
    action = int(raw[offset + 60])
    if action not in {STAT_ACTION_NEW, STAT_ACTION_DELETE}:
        raise ValueError(f"open-interest record at {offset} has unknown update action {action}")
    return {
        "instrument_id": int.from_bytes(raw[offset + 4 : offset + 8], "little"),
        "ts_event_ns": event_ns,
        "ts_recv_ns": receive_ns if 0 < receive_ns < UNDEFINED_U64 else None,
        "ts_ref_ns": reference_ns if 0 < reference_ns < UNDEFINED_U64 else None,
        "sequence": int.from_bytes(raw[offset + 48 : offset + 52], "little"),
        "channel_id": int.from_bytes(raw[offset + 58 : offset + 60], "little"),
        "update_action": action,
        "stat_flags": int(raw[offset + 61]),
        "open_interest": (
            float(quantity)
            if action == STAT_ACTION_NEW and quantity < UNDEFINED_U64
            else None
        ),
    }


def _decode_sdk_open_interest_record(record: object) -> dict[str, int | float | None] | None:
    """Normalize an SDK-decoded StatMsg after any required DBN upgrade."""
    if int(getattr(record, "stat_type")) != OPEN_INTEREST_STAT_TYPE:
        return None
    event_ns = int(getattr(record, "ts_event"))
    if event_ns <= 0 or event_ns == UNDEFINED_U64:
        raise ValueError("open-interest record has no valid event timestamp")
    receive_ns = int(getattr(record, "ts_recv"))
    reference_ns = int(getattr(record, "ts_ref"))
    quantity = int(getattr(record, "quantity"))
    action = int(getattr(record, "update_action"))
    if action not in {STAT_ACTION_NEW, STAT_ACTION_DELETE}:
        raise ValueError(f"open-interest record has unknown update action {action}")
    return {
        "instrument_id": int(getattr(record, "instrument_id")),
        "ts_event_ns": event_ns,
        "ts_recv_ns": receive_ns if 0 < receive_ns < UNDEFINED_U64 else None,
        "ts_ref_ns": reference_ns if 0 < reference_ns < UNDEFINED_U64 else None,
        "sequence": int(getattr(record, "sequence")),
        "channel_id": int(getattr(record, "channel_id")),
        "update_action": action,
        "stat_flags": int(getattr(record, "stat_flags")),
        "open_interest": (
            float(quantity)
            if action == STAT_ACTION_NEW and quantity < UNDEFINED_U64
            else None
        ),
    }


def replay_open_interest_state_from_prefix(
    dbn_path: str | Path,
    *,
    available_before_utc: datetime,
) -> list[dict[str, object]]:
    """Derive horizon OI and symbol identity only from the copied DBN prefix."""

    if available_before_utc.tzinfo is None:
        raise ValueError("open-interest replay horizon must be timezone-aware")
    available_ns = int(
        available_before_utc.astimezone(timezone.utc).timestamp() * 1_000_000_000
    )
    source = Path(dbn_path).resolve()
    mappings: dict[int, tuple[str, str | None]] = {}
    latest: dict[int, dict[str, object]] = {}
    with source.open("rb") as stream:
        raw = mmap.mmap(stream.fileno(), 0, access=mmap.ACCESS_READ)
        try:
            if len(raw) < 8 or raw[:3] != b"DBN":
                raise ValueError("invalid DBN header")
            format_version = int(raw[3])
            stat_decoder = definition_decoder = None
            if format_version in {1, 2}:
                import databento_dbn as dbn

                stat_decoder = dbn.DBNDecoder(
                    has_metadata=False,
                    input_version=format_version,
                    upgrade_policy=dbn.VersionUpgradePolicy.UPGRADE_TO_V3,
                )
                definition_decoder = dbn.DBNDecoder(
                    has_metadata=False,
                    input_version=format_version,
                    upgrade_policy=dbn.VersionUpgradePolicy.AS_IS,
                )
            offset = 8 + int.from_bytes(raw[4:8], "little")
            while offset < len(raw):
                record_bytes = int(raw[offset]) * 4
                if record_bytes < 16 or offset + record_bytes > len(raw):
                    raise ValueError(f"invalid DBN record boundary at {offset}")
                rtype = int(raw[offset + 1])
                if rtype == DEFINITION_RTYPE:
                    if definition_decoder is not None:
                        decoded_records = definition_decoder.write_and_decode(
                            bytes(raw[offset : offset + record_bytes])
                        )
                        if len(decoded_records) != 1:
                            raise ValueError(
                                f"could not decode instrument definition at {offset}"
                            )
                        definition = decoded_records[0]
                        instrument_id = int(definition.instrument_id)
                        raw_symbol = str(definition.raw_symbol)
                        action = str(definition.security_update_action)
                    else:
                        if record_bytes < DEFINITION_MIN_BYTES:
                            raise ValueError(f"truncated instrument definition at {offset}")
                        instrument_id = int.from_bytes(
                            raw[offset + 4 : offset + 8], "little"
                        )
                        raw_symbol = bytes(
                            raw[offset + 238 : offset + 309]
                        ).split(b"\0", 1)[0].decode("utf-8")
                        action = bytes(
                            raw[offset + 493 : offset + 494]
                        ).decode("ascii", errors="strict")
                    parsed = parse_occ_symbol(raw_symbol)
                    if action.upper() == "D":
                        mappings.pop(instrument_id, None)
                    elif parsed is not None:
                        mappings[instrument_id] = (
                            raw_symbol,
                            str(parsed["family_root"]),
                        )
                elif rtype == STAT_RTYPE:
                    if stat_decoder is not None:
                        decoded_records = stat_decoder.write_and_decode(
                            bytes(raw[offset : offset + record_bytes])
                        )
                        if len(decoded_records) != 1:
                            raise ValueError(
                                f"could not decode statistics record at {offset}"
                            )
                        decoded = _decode_sdk_open_interest_record(decoded_records[0])
                    else:
                        decoded = decode_open_interest_record(raw, offset, record_bytes)
                    if decoded is not None:
                        receive_ns = decoded.get("ts_recv_ns")
                        if receive_ns is None or int(receive_ns) > available_ns:
                            offset += record_bytes
                            continue
                        instrument_id = int(decoded["instrument_id"])
                        raw_symbol, family_root = mappings.get(
                            instrument_id, (None, None)
                        )
                        latest[instrument_id] = {
                            **decoded,
                            "record_offset": offset,
                            "raw_symbol": raw_symbol,
                            "family_root": family_root,
                        }
                offset += record_bytes
        finally:
            raw.close()
    result: list[dict[str, object]] = []
    for state in latest.values():
        receive_ns = state.get("ts_recv_ns")
        if int(state["update_action"]) != STAT_ACTION_NEW:
            continue
        raw_symbol = state.get("raw_symbol")
        family_root = state.get("family_root")
        if not raw_symbol or not family_root:
            raise ValueError(
                "open-interest prefix state has no prior in-prefix instrument definition"
            )
        if state.get("open_interest") is None:
            raise ValueError(
                "open-interest prefix state has undefined NEW quantity"
            )
        asof_ns = int(state.get("ts_ref_ns") or state["ts_event_ns"])
        result.append(
            {
                "raw_symbol": str(raw_symbol),
                "family_root": str(family_root),
                "open_interest": float(state["open_interest"]),
                "open_interest_asof_utc": _timestamp_iso(asof_ns),
                "open_interest_available_at_utc": _timestamp_iso(int(receive_ns)),
                "record_offset": int(state["record_offset"]),
            }
        )
    return sorted(result, key=lambda item: str(item["raw_symbol"]))


def replay_open_interest(
    dbn_path: str | Path,
    *,
    catalog: TapeCatalog,
    session_id: str,
    feed_name: str,
    source_sha256: str,
    batch_size: int = 10_000,
) -> dict[str, object]:
    """Persist every observed OI update and rebuild the latest-state projection."""
    if len(source_sha256) != 64:
        raise ValueError("source_sha256 must be the finalized DBN SHA-256")
    if batch_size <= 0:
        raise ValueError("batch_size must be positive")
    source = Path(dbn_path).resolve()
    with catalog.connect(read_only=True) as connection:
        mappings = {
            int(row["instrument_id"]): (row["raw_symbol"], row["family_root"])
            for row in connection.execute(
                """
                SELECT instrument_id, raw_symbol, family_root
                FROM tape_instruments WHERE session_id=? AND feed_name=?
                """,
                (session_id, feed_name),
            )
        }
    ingested_at = _utcnow()
    batch: list[dict[str, object]] = []
    records = 0
    unmapped = 0
    roots: set[str] = set()
    timestamp_cache: dict[int, str] = {}
    with source.open("rb") as stream:
        raw = mmap.mmap(stream.fileno(), 0, access=mmap.ACCESS_READ)
        try:
            if len(raw) < 8 or raw[:3] != b"DBN":
                raise ValueError("invalid DBN header")
            format_version = int(raw[3])
            legacy_decoder = None
            if format_version in {1, 2}:
                import databento_dbn as dbn

                legacy_decoder = dbn.DBNDecoder(
                    has_metadata=False,
                    input_version=format_version,
                    upgrade_policy=dbn.VersionUpgradePolicy.UPGRADE_TO_V3,
                )
            offset = 8 + int.from_bytes(raw[4:8], "little")
            while offset < len(raw):
                record_bytes = int(raw[offset]) * 4
                if record_bytes < 16 or offset + record_bytes > len(raw):
                    raise ValueError(f"invalid DBN record boundary at {offset}")
                if legacy_decoder is not None and int(raw[offset + 1]) == STAT_RTYPE:
                    payload = bytes(raw[offset : offset + record_bytes])
                    decoded_records = legacy_decoder.write_and_decode(payload)
                    if len(decoded_records) != 1:
                        raise ValueError(
                            f"could not decode DBN v{format_version} statistics record "
                            f"at {offset}"
                        )
                    decoded = _decode_sdk_open_interest_record(decoded_records[0])
                else:
                    decoded = decode_open_interest_record(raw, offset, record_bytes)
                if decoded is not None:
                    instrument_id = int(decoded["instrument_id"])
                    raw_symbol, family_root = mappings.get(instrument_id, (None, None))
                    if family_root is None:
                        unmapped += 1
                    else:
                        roots.add(str(family_root))
                    asof_ns = int(decoded["ts_ref_ns"] or decoded["ts_event_ns"])
                    asof_utc = timestamp_cache.get(asof_ns)
                    if asof_utc is None:
                        asof_utc = _timestamp_iso(asof_ns)
                        timestamp_cache[asof_ns] = asof_utc
                    batch.append(
                        {
                            "observation_key": f"{source_sha256}:{offset}",
                            "session_id": session_id,
                            "feed_name": feed_name,
                            "source_sha256": source_sha256,
                            "record_offset": offset,
                            "raw_symbol": raw_symbol,
                            "family_root": family_root,
                            "asof_utc": asof_utc,
                            "ingested_at_utc": ingested_at,
                            **decoded,
                        }
                    )
                    records += 1
                    if len(batch) >= batch_size:
                        catalog.append_open_interest_observations(batch)
                        batch.clear()
                offset += record_bytes
        finally:
            raw.close()
    if batch:
        catalog.append_open_interest_observations(batch)
    catalog.rebuild_open_interest_projection(session_id=session_id, feed_name=feed_name)
    return {
        "records": records,
        "unmapped_records": unmapped,
        "families": sorted(roots),
        "source_sha256": source_sha256,
    }
