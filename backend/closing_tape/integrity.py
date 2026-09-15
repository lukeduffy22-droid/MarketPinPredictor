from __future__ import annotations

import hashlib
import json
import mmap
from dataclasses import asdict, dataclass
from pathlib import Path


@dataclass(frozen=True)
class TapeIntegrityReport:
    path: str
    dataset: str | None
    sha256: str
    file_bytes: int
    records_seen: int
    trade_records: int
    tcbbo_records: int
    tcbbo_timestamped_records: int
    tcbbo_valid_nbbo_records: int
    tcbbo_flagged_records: int
    tcbbo_action_counts: tuple[tuple[str, int], ...]
    mapping_records: int
    statistics_records: int
    definition_records: int
    system_records: int
    first_event_ns: int | None
    last_event_ns: int | None
    last_trade_event_ns: int | None
    first_receive_ns: int | None
    last_receive_ns: int | None
    subscription_acks: int
    replay_completed: int
    provider_errors: tuple[str, ...]
    slow_reader_warnings: int
    local_file_intact: bool
    session_complete: bool | None
    complete: bool
    incomplete_reasons: tuple[str, ...]

    def to_dict(self) -> dict[str, object]:
        return asdict(self)

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), sort_keys=True, separators=(",", ":"))


def _sha256(path: Path, chunk_size: int = 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(chunk_size), b""):
            digest.update(chunk)
    return digest.hexdigest()


def inspect_dbn(
    path: str | Path,
    *,
    require_trades: bool = True,
    require_tcbbo: bool = False,
    expected_subscription_acks: int = 1,
) -> TapeIntegrityReport:
    """Decode a raw DBN file and produce a conservative completeness report.

    ``local_file_intact`` and the backward-compatible ``complete`` alias only
    prove local framing/provider-message integrity. ``session_complete`` is
    intentionally ``None`` because time/root/schema scope needs the session
    catalog and cannot be inferred from an arbitrary DBN file alone.
    """
    import databento as db

    source = Path(path).resolve()
    if not source.is_file():
        raise FileNotFoundError(source)

    # Metadata decoding is native and validates the DBN header. Record-boundary
    # scanning below avoids materializing hundreds of thousands of Python SDK
    # objects at close while still proving that the file ends on a full record.
    store = db.DBNStore.from_file(source)
    metadata = store.metadata
    counts = {
        "records_seen": 0,
        "trade_records": 0,
        "tcbbo_records": 0,
        "tcbbo_timestamped_records": 0,
        "tcbbo_valid_nbbo_records": 0,
        "tcbbo_flagged_records": 0,
        "mapping_records": 0,
        "statistics_records": 0,
        "definition_records": 0,
        "system_records": 0,
    }
    first_event = last_event = first_receive = last_receive = None
    last_trade_event = None
    subscription_acks = 0
    replay_completed = 0
    provider_errors: list[str] = []
    slow_reader_warnings = 0
    tcbbo_action_counts: dict[str, int] = {}
    boundary_error: str | None = None
    with source.open("rb") as stream:
        raw = mmap.mmap(stream.fileno(), 0, access=mmap.ACCESS_READ)
        try:
            if len(raw) < 8 or raw[:3] != b"DBN":
                boundary_error = "invalid_dbn_header"
            else:
                offset = 8 + int.from_bytes(raw[4:8], "little")
                if offset > len(raw):
                    boundary_error = "truncated_dbn_metadata"
                while boundary_error is None and offset < len(raw):
                    record_bytes = int(raw[offset]) * 4
                    if record_bytes < 16 or offset + record_bytes > len(raw):
                        boundary_error = f"truncated_or_invalid_record_at:{offset}"
                        break
                    rtype_number = int(raw[offset + 1])
                    event_ns = int.from_bytes(raw[offset + 8 : offset + 16], "little")
                    if 0 < event_ns < 2**64 - 1:
                        first_event = event_ns if first_event is None else min(first_event, event_ns)
                        last_event = event_ns if last_event is None else max(last_event, event_ns)
                    counts["records_seen"] += 1
                    if rtype_number == 22:
                        counts["mapping_records"] += 1
                    elif rtype_number == 24:
                        counts["statistics_records"] += 1
                    elif rtype_number == 19:
                        counts["definition_records"] += 1
                    elif rtype_number in {0, 1, 194}:
                        counts["trade_records"] += 1
                        if rtype_number == 194 and record_bytes >= 40:
                            counts["tcbbo_records"] += 1
                            if 0 < event_ns < 2**64 - 1:
                                last_trade_event = (
                                    event_ns
                                    if last_trade_event is None
                                    else max(last_trade_event, event_ns)
                                )
                            receive_ns = int.from_bytes(raw[offset + 32 : offset + 40], "little")
                            if 0 < receive_ns < 2**64 - 1:
                                first_receive = (
                                    receive_ns if first_receive is None else min(first_receive, receive_ns)
                                )
                                last_receive = (
                                    receive_ns if last_receive is None else max(last_receive, receive_ns)
                                )
                            if (
                                0 < event_ns < 2**64 - 1
                                and 0 < receive_ns < 2**64 - 1
                            ):
                                counts["tcbbo_timestamped_records"] += 1
                            if record_bytes >= 72:
                                action_value = int(raw[offset + 28])
                                action = (
                                    chr(action_value)
                                    if 32 <= action_value <= 126
                                    else f"0x{action_value:02x}"
                                )
                                tcbbo_action_counts[action] = tcbbo_action_counts.get(action, 0) + 1
                                if int(raw[offset + 30]) != 0:
                                    counts["tcbbo_flagged_records"] += 1
                                bid = int.from_bytes(
                                    raw[offset + 48 : offset + 56], "little", signed=True
                                )
                                ask = int.from_bytes(
                                    raw[offset + 56 : offset + 64], "little", signed=True
                                )
                                undefined_price = 2**63 - 1
                                if (
                                    bid != undefined_price
                                    and ask != undefined_price
                                    and bid >= 0
                                    and ask > 0
                                    and bid <= ask
                                ):
                                    counts["tcbbo_valid_nbbo_records"] += 1
                    elif rtype_number in {21, 23}:
                        message = bytes(raw[offset + 16 : offset + record_bytes]).split(b"\0", 1)[0].decode(
                            "utf-8", errors="replace"
                        )
                        lowered = message.lower()
                        if rtype_number == 21:
                            provider_errors.append(message[:1000])
                        else:
                            counts["system_records"] += 1
                            if "succeeded" in lowered and "subscription" in lowered:
                                subscription_acks += 1
                            if "replay completed" in lowered or ("finished" in lowered and "replay" in lowered):
                                replay_completed += 1
                            if "slow" in lowered or "skip" in lowered or "queue is full" in lowered:
                                slow_reader_warnings += 1
                            if any(token in lowered for token in ("error", "failed", "terminated", "skip")):
                                provider_errors.append(message[:1000])
                    offset += record_bytes
                if boundary_error is None and offset != len(raw):
                    boundary_error = f"invalid_final_record_boundary:{offset}/{len(raw)}"
        finally:
            raw.close()

    reasons: list[str] = []
    if counts["records_seen"] == 0:
        reasons.append("empty_file")
    if require_trades and counts["trade_records"] == 0:
        reasons.append("no_trade_records")
    if require_tcbbo and counts["tcbbo_records"] == 0:
        reasons.append("no_tcbbo_records")
    if subscription_acks < max(0, int(expected_subscription_acks)):
        reasons.append(
            f"missing_subscription_ack:{subscription_acks}/{max(0, int(expected_subscription_acks))}"
        )
    if provider_errors:
        reasons.append("provider_error_message")
    if slow_reader_warnings:
        reasons.append("slow_reader_or_skipped_records")
    if boundary_error:
        reasons.append(boundary_error)

    local_file_intact = not reasons
    return TapeIntegrityReport(
        path=str(source),
        dataset=str(getattr(metadata, "dataset", "")) or None,
        sha256=_sha256(source),
        file_bytes=source.stat().st_size,
        first_event_ns=first_event,
        last_event_ns=last_event,
        last_trade_event_ns=last_trade_event,
        first_receive_ns=first_receive,
        last_receive_ns=last_receive,
        subscription_acks=subscription_acks,
        replay_completed=replay_completed,
        provider_errors=tuple(provider_errors),
        slow_reader_warnings=slow_reader_warnings,
        tcbbo_action_counts=tuple(sorted(tcbbo_action_counts.items())),
        local_file_intact=local_file_intact,
        session_complete=None,
        complete=local_file_intact,
        incomplete_reasons=tuple(reasons),
        **counts,
    )
