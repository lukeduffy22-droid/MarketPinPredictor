from __future__ import annotations

import hashlib
import mmap
from pathlib import Path

from .catalog import TapeCatalog, _utcnow
from .replay import parse_occ_symbol


DEFINITION_RTYPE = 19
DEFINITION_MIN_BYTES = 494
UNDEFINED_U64 = 2**64 - 1


def _optional_timestamp(raw: mmap.mmap, start: int) -> int | None:
    value = int.from_bytes(raw[start : start + 8], "little")
    return value if 0 < value < UNDEFINED_U64 else None


def replay_instrument_definitions(
    dbn_path: str | Path,
    *,
    catalog: TapeCatalog,
    session_id: str,
    feed_name: str,
    source_sha256: str,
    batch_size: int = 5_000,
) -> dict[str, object]:
    """Append exact DBN instrument-definition records to the observed ledger."""
    if len(source_sha256) != 64:
        raise ValueError("source_sha256 must be the finalized DBN SHA-256")
    if batch_size <= 0:
        raise ValueError("batch_size must be positive")
    source = Path(dbn_path).resolve()
    batch: list[dict[str, object]] = []
    records = 0
    parsed_contracts = 0
    inserted = 0
    ingested_at = _utcnow()
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
                    upgrade_policy=dbn.VersionUpgradePolicy.AS_IS,
                )
            offset = 8 + int.from_bytes(raw[4:8], "little")
            while offset < len(raw):
                record_bytes = int(raw[offset]) * 4
                if record_bytes < 16 or offset + record_bytes > len(raw):
                    raise ValueError(f"invalid DBN record boundary at {offset}")
                if int(raw[offset + 1]) == DEFINITION_RTYPE:
                    payload = bytes(raw[offset : offset + record_bytes])
                    if legacy_decoder is not None:
                        decoded = legacy_decoder.write_and_decode(payload)
                        if len(decoded) != 1:
                            raise ValueError(
                                f"could not decode DBN v{format_version} instrument definition "
                                f"at {offset}"
                            )
                        record = decoded[0]
                        if int(record.record_size()) != record_bytes:
                            raise ValueError(
                                f"DBN v{format_version} definition size mismatch at {offset}"
                            )
                        raw_symbol = str(record.raw_symbol)
                        action = str(record.security_update_action)
                        instrument_id = int(record.instrument_id)
                        ts_event_ns = int(record.ts_event)
                        ts_recv_value = int(record.ts_recv)
                        ts_recv_ns = (
                            ts_recv_value
                            if 0 < ts_recv_value < UNDEFINED_U64
                            else None
                        )
                    else:
                        if record_bytes < DEFINITION_MIN_BYTES:
                            raise ValueError(
                                f"truncated instrument definition at {offset}"
                            )
                        raw_symbol = bytes(
                            raw[offset + 238 : offset + 309]
                        ).split(b"\0", 1)[0].decode("utf-8")
                        action = bytes(
                            raw[offset + 493 : offset + 494]
                        ).decode("ascii", errors="strict")
                        instrument_id = int.from_bytes(
                            raw[offset + 4 : offset + 8], "little"
                        )
                        ts_event_ns = int.from_bytes(
                            raw[offset + 8 : offset + 16], "little"
                        )
                        ts_recv_ns = _optional_timestamp(raw, offset + 16)
                    parsed = parse_occ_symbol(raw_symbol)
                    if parsed is not None:
                        parsed_contracts += 1
                    batch.append({
                        "observation_key": f"{source_sha256}:{offset}",
                        "session_id": session_id,
                        "feed_name": feed_name,
                        "source_sha256": source_sha256,
                        "record_offset": offset,
                        "record_bytes": record_bytes,
                        "instrument_id": instrument_id,
                        "ts_event_ns": ts_event_ns,
                        "ts_recv_ns": ts_recv_ns,
                        "raw_symbol": raw_symbol,
                        "family_root": parsed.get("family_root") if parsed else None,
                        "option_type": parsed.get("option_type") if parsed else None,
                        "expiration": parsed.get("expiration") if parsed else None,
                        "strike": parsed.get("strike") if parsed else None,
                        "security_update_action": action,
                        "raw_record_sha256": hashlib.sha256(payload).hexdigest(),
                        "raw_record": payload,
                        "ingested_at_utc": ingested_at,
                    })
                    records += 1
                    if len(batch) >= batch_size:
                        inserted += catalog.append_instrument_definition_observations(batch)
                        batch.clear()
                offset += record_bytes
        finally:
            raw.close()
    if batch:
        inserted += catalog.append_instrument_definition_observations(batch)
    return {
        "records": records,
        "inserted": inserted,
        "parsed_contracts": parsed_contracts,
        "source_sha256": source_sha256,
    }
