from __future__ import annotations

import hashlib
import json
import mmap
import re
import struct
from datetime import date, datetime, timezone
from itertools import chain
from pathlib import Path
from typing import Mapping

import pandas as pd
import numpy as np

from .integrity import inspect_dbn


UTC = timezone.utc
OCC_RE = re.compile(r"^(?P<root>.{1,6})(?P<date>\d{6})(?P<kind>[CP])(?P<strike>\d{8})$")
FAMILY_ROOTS = {"SPXW": "SPX", "NDXP": "NDX", "RUTW": "RUT", "VIXW": "VIX"}
INFERENCE_METHOD = "trade_price_vs_pretrade_nbbo"
INFERENCE_VERSION = "1.0"
REPLAY_DECODER_VERSION = "mixed-dbn-raw-tcbbo-v4-metadata-symbol-alignment"
RECORD_FLAG_BITS = {
    "last": 128, "tob": 64, "snapshot": 32, "mbp": 16,
    "bad_ts_recv": 8, "maybe_bad_book": 4, "publisher_specific": 2,
}
DATA_QUALITY_FLAG_MASK = 8 | 4 | 2
TCBBO_RTYPE = 194
SYMBOL_MAPPING_RTYPE = 22
UNDEFINED_I64 = 2**63 - 1
_TCBBO_FIELDS = struct.Struct("<IQqI2xBxQ8xqqII")
CANONICAL_TCBBO_ORDER_COLUMNS = (
    "ts_event", "ts_recv", "instrument_id", "price", "size",
    "bid_px_00", "ask_px_00", "bid_sz_00", "ask_sz_00", "flags", "symbol",
)


def canonicalize_tcbbo_order(frame: pd.DataFrame) -> pd.DataFrame:
    """Order immutable trades independently of live/replay callback arrival."""
    missing = sorted(set(CANONICAL_TCBBO_ORDER_COLUMNS) - set(frame.columns))
    if missing:
        raise ValueError(
            "TCBBO canonical ordering columns missing: " + ", ".join(missing)
        )
    return frame.sort_values(
        list(CANONICAL_TCBBO_ORDER_COLUMNS),
        kind="mergesort",
        na_position="last",
    ).reset_index(drop=True)


def _active_metadata_symbol_mappings(
    metadata_mappings: Mapping[object, object],
    trading_day: date,
) -> dict[int, str]:
    """Resolve immutable DBN metadata mappings active on one UTC session day."""
    active: dict[int, str] = {}
    for raw_symbol_value, intervals_value in metadata_mappings.items():
        raw_symbol = str(raw_symbol_value)
        if not isinstance(intervals_value, (list, tuple)):
            raise ValueError("DBN metadata symbol mapping intervals are malformed")
        for interval in intervals_value:
            if isinstance(interval, Mapping):
                start_date = interval.get("start_date")
                end_date = interval.get("end_date")
                instrument_value = interval.get("symbol")
            else:
                start_date = getattr(interval, "start_date", None)
                end_date = getattr(interval, "end_date", None)
                instrument_value = getattr(interval, "symbol", None)
            if not isinstance(start_date, date) or not isinstance(end_date, date):
                raise ValueError("DBN metadata symbol mapping dates are malformed")
            if not start_date <= trading_day < end_date:
                continue
            try:
                instrument_id = int(instrument_value)
            except (TypeError, ValueError, OverflowError) as exc:
                raise ValueError("DBN metadata instrument mapping is malformed") from exc
            prior = active.get(instrument_id)
            if prior is not None and prior != raw_symbol:
                raise ValueError(
                    f"instrument {instrument_id} has conflicting DBN metadata symbols"
                )
            active[instrument_id] = raw_symbol
    return active


def _metadata_symbol_mappings(source: Path, trading_day: date) -> dict[int, str]:
    """Read point-in-time mappings embedded in a historical DBN metadata header."""
    import databento as db

    metadata = db.DBNStore.from_file(source).metadata
    metadata_mappings = getattr(metadata, "mappings", None)
    if not isinstance(metadata_mappings, Mapping):
        return {}
    return _active_metadata_symbol_mappings(metadata_mappings, trading_day)


def _decode_tcbbo_frame(source: Path, expected_records: int) -> pd.DataFrame:
    """Decode only the stable TCBBO and symbology fields from a mixed DBN."""
    if expected_records <= 0:
        raise ValueError("verified integrity report has no TCBBO records")
    mappings: dict[int, str] = {}
    with source.open("rb") as stream:
        raw = mmap.mmap(stream.fileno(), 0, access=mmap.ACCESS_READ)
        try:
            if len(raw) < 8 or raw[:3] != b"DBN":
                raise ValueError("invalid DBN header")
            first_record = 8 + int.from_bytes(raw[4:8], "little")
            offset = first_record
            while offset < len(raw):
                record_bytes = int(raw[offset]) * 4
                if record_bytes < 16 or offset + record_bytes > len(raw):
                    raise ValueError(f"invalid DBN record boundary at {offset}")
                if int(raw[offset + 1]) == SYMBOL_MAPPING_RTYPE:
                    if record_bytes < 176:
                        raise ValueError(f"truncated symbol mapping record at {offset}")
                    instrument_id = int.from_bytes(raw[offset + 4 : offset + 8], "little")
                    symbol = bytes(raw[offset + 89 : offset + 160]).split(b"\0", 1)[0].decode("utf-8")
                    prior = mappings.get(instrument_id)
                    if prior is not None and prior != symbol:
                        raise ValueError(
                            f"instrument {instrument_id} has conflicting point-in-time symbols"
                        )
                    mappings[instrument_id] = symbol
                offset += record_bytes

            instrument_id = np.empty(expected_records, dtype=np.uint32)
            ts_event = np.empty(expected_records, dtype=np.uint64)
            raw_price = np.empty(expected_records, dtype=np.int64)
            size = np.empty(expected_records, dtype=np.uint32)
            flags = np.empty(expected_records, dtype=np.uint8)
            ts_recv = np.empty(expected_records, dtype=np.uint64)
            raw_bid = np.empty(expected_records, dtype=np.int64)
            raw_ask = np.empty(expected_records, dtype=np.int64)
            bid_size = np.empty(expected_records, dtype=np.uint32)
            ask_size = np.empty(expected_records, dtype=np.uint32)
            offset = first_record
            index = 0
            while offset < len(raw):
                record_bytes = int(raw[offset]) * 4
                if int(raw[offset + 1]) == TCBBO_RTYPE:
                    if record_bytes < 72 or index >= expected_records:
                        raise ValueError(f"invalid TCBBO record at {offset}")
                    values = _TCBBO_FIELDS.unpack_from(raw, offset + 4)
                    (
                        instrument_id[index], ts_event[index], raw_price[index], size[index],
                        flags[index], ts_recv[index], raw_bid[index], raw_ask[index],
                        bid_size[index], ask_size[index],
                    ) = values
                    index += 1
                offset += record_bytes
        finally:
            raw.close()
    if index != expected_records:
        raise ValueError(f"TCBBO decode count mismatch ({index}/{expected_records})")
    missing_instrument_ids = set(np.unique(instrument_id)) - set(mappings)
    if missing_instrument_ids:
        first_day = datetime.fromtimestamp(
            int(ts_event.min()) // 1_000_000_000,
            tz=UTC,
        ).date()
        last_day = datetime.fromtimestamp(
            int(ts_event.max()) // 1_000_000_000,
            tz=UTC,
        ).date()
        if first_day != last_day:
            raise ValueError(
                "DBN metadata symbol fallback requires a single UTC session day"
            )
        for mapped_id, symbol in _metadata_symbol_mappings(source, first_day).items():
            prior = mappings.get(mapped_id)
            if prior is not None and prior != symbol:
                raise ValueError(
                    f"instrument {mapped_id} conflicts between DBN record and metadata symbols"
                )
            mappings[mapped_id] = symbol
    symbols = pd.Series(instrument_id, copy=False).map(mappings)
    missing = int(symbols.isna().sum())
    if missing:
        raise ValueError(f"{missing} TCBBO records have no point-in-time symbol mapping")

    def prices(values: np.ndarray) -> np.ndarray:
        result = values.astype(np.float64)
        result[values == UNDEFINED_I64] = np.nan
        result[values != UNDEFINED_I64] /= 1_000_000_000.0
        return result

    return pd.DataFrame({
        "instrument_id": instrument_id,
        "ts_event": pd.to_datetime(ts_event, unit="ns", utc=True),
        "price": prices(raw_price),
        "size": size,
        "flags": flags,
        "ts_recv": pd.to_datetime(ts_recv, unit="ns", utc=True),
        "bid_px_00": prices(raw_bid),
        "ask_px_00": prices(raw_ask),
        "bid_sz_00": bid_size,
        "ask_sz_00": ask_size,
        "symbol": symbols.to_numpy(),
    })


def parse_occ_symbol(raw_symbol: str) -> dict[str, object] | None:
    match = OCC_RE.match(raw_symbol)
    if not match:
        return None
    return {
        "family_root": FAMILY_ROOTS.get(match.group("root").strip(), match.group("root").strip()),
        "expiration": datetime.strptime(match.group("date"), "%y%m%d").date().isoformat(),
        "option_type": match.group("kind"),
        "strike": int(match.group("strike")) / 1000.0,
    }


def _price(record: object, pretty_name: str, raw_name: str) -> float | None:
    pretty = getattr(record, pretty_name, None)
    if isinstance(pretty, (int, float)):
        return float(pretty)
    raw = getattr(record, raw_name, None)
    if isinstance(raw, int) and raw < 2**63 - 1:
        return raw / 1_000_000_000.0
    return None


def _bucket(price: float | None, bid: float | None, ask: float | None) -> str:
    if price is None or bid is None or ask is None or bid < 0 or ask <= 0 or bid > ask:
        return "unknown"
    if price >= ask:
        return "at_ask"
    if price <= bid:
        return "at_bid"
    return "inside"


def _base_observed(session_id: str, feed_name: str, root: str, minute: str) -> dict[str, object]:
    return {
        "session_id": session_id, "feed_name": feed_name, "family_root": root,
        "minute_utc": minute, "asset_class": "options", "trade_count": 0,
        "volume": 0.0, "notional": 0.0, "call_count": 0, "put_count": 0,
        "call_volume": 0.0, "put_volume": 0.0, "call_premium": 0.0,
        "put_premium": 0.0, "first_price": None, "high_price": None,
        "low_price": None, "last_price": None, "price_volume_sum": 0.0,
        "largest_trade_size": 0.0, "largest_trade_notional": 0.0,
        "nbbo_valid_count": 0, "quoted_spread_sum": 0.0,
        "quoted_spread_bps_sum": 0.0, "trade_to_mid_abs_sum": 0.0,
        "trade_to_mid_signed_sum": 0.0, "bid_size_sum": 0.0,
        "ask_size_sum": 0.0, "receive_lag_ns_sum": 0,
        "receive_lag_ns_max": None, "flag_last_count": 0, "flag_tob_count": 0,
        "flag_snapshot_count": 0, "flag_mbp_count": 0, "flag_bad_ts_recv_count": 0,
        "flag_maybe_bad_book_count": 0, "flag_publisher_specific_count": 0,
        "data_quality_flagged_count": 0,
    }


def _base_inferred(session_id: str, feed_name: str, root: str, minute: str, sha256: str) -> dict[str, object]:
    row: dict[str, object] = {
        "session_id": session_id, "feed_name": feed_name, "family_root": root,
        "minute_utc": minute, "inference_method": INFERENCE_METHOD,
        "inference_version": INFERENCE_VERSION, "source_sha256": sha256,
    }
    for bucket in ("at_ask", "at_bid", "inside", "unknown"):
        row[f"{bucket}_count"] = 0
        row[f"{bucket}_volume"] = 0.0
        row[f"{bucket}_notional"] = 0.0
    for kind in ("call", "put"):
        for bucket in ("at_ask", "at_bid"):
            row[f"{kind}_{bucket}_notional"] = 0.0
    return row


def build_minute_rows(
    path: str | Path,
    *,
    session_id: str,
    feed_name: str = "opra_options",
    verified_source_sha256: str | None = None,
    verified_integrity_report: object | None = None,
    evidence_source_sha256: str | None = None,
    include_contract_rows: bool = False,
) -> tuple:
    """Rebuild rows from a verified DBN and bind estimates to its evidence source.

    Live files use the same SHA-256 for both values. Historical imports verify
    the decompressed TCBBO component separately, while binding derived rows to
    the content-addressed three-component bundle SHA-256.
    """
    source = Path(path).resolve()
    report = verified_integrity_report or inspect_dbn(source)
    if Path(str(getattr(report, "path", ""))).resolve() != source:
        raise ValueError("verified_integrity_report does not describe the requested DBN file")
    if not report.local_file_intact:
        raise ValueError(f"refusing incomplete tape: {', '.join(report.incomplete_reasons)}")
    if not verified_source_sha256:
        raise ValueError("verified_source_sha256 from a completed tape catalog is required")
    if verified_source_sha256.lower() != report.sha256.lower():
        raise ValueError("verified_source_sha256 does not match the raw DBN file")
    evidence_sha256 = str(evidence_source_sha256 or report.sha256).lower()
    if not re.fullmatch(r"[0-9a-f]{64}", evidence_sha256):
        raise ValueError("evidence_source_sha256 must be a lowercase SHA-256")

    frame = _decode_tcbbo_frame(source, int(report.tcbbo_records))
    if frame.empty:
        raise ValueError("refusing tape with no TCBBO rows")
    if "ts_recv" not in frame.columns and frame.index.name == "ts_recv":
        frame = frame.reset_index()
    # Canonical ordering resets the index. Extract OCC fields only after that
    # reset so pandas cannot align symbol metadata from the pre-sort row index
    # onto a different immutable trade.
    frame = canonicalize_tcbbo_order(frame.copy())
    extracted = frame["symbol"].astype(str).str.extract(OCC_RE)
    missing_symbols = int(extracted["root"].isna().sum())
    if missing_symbols:
        raise ValueError(f"{missing_symbols} TCBBO records have no parseable point-in-time symbol mapping")

    frame["family_root"] = extracted["root"].str.strip().replace(FAMILY_ROOTS)
    frame["option_type"] = extracted["kind"]
    frame["raw_symbol"] = frame["symbol"].astype(str)
    frame["expiration"] = pd.to_datetime(extracted["date"], format="%y%m%d").dt.date.astype(str)
    frame["strike"] = extracted["strike"].astype("int64") / 1000.0
    frame["minute_utc"] = frame["ts_event"].dt.floor("min")
    frame["notional"] = frame["price"] * frame["size"] * 100.0
    frame["price_volume"] = frame["price"] * frame["size"]
    valid_nbbo = (
        frame["price"].notna() & frame["bid_px_00"].notna() & frame["ask_px_00"].notna()
        & (frame["bid_px_00"] >= 0) & (frame["ask_px_00"] > 0)
        & (frame["bid_px_00"] <= frame["ask_px_00"])
    )
    midpoint = (frame["bid_px_00"] + frame["ask_px_00"]) / 2.0
    spread = frame["ask_px_00"] - frame["bid_px_00"]
    frame["nbbo_valid_count"] = valid_nbbo.astype("int64")
    frame["quoted_spread"] = spread.where(valid_nbbo, 0.0)
    frame["quoted_spread_bps"] = (spread / midpoint * 10_000.0).where(
        valid_nbbo & (midpoint > 0), 0.0
    )
    frame["trade_to_mid_abs"] = (frame["price"] - midpoint).abs().where(valid_nbbo, 0.0)
    frame["trade_to_mid_signed"] = (frame["price"] - midpoint).where(valid_nbbo, 0.0)
    frame["pretrade_midpoint"] = midpoint.where(valid_nbbo)
    event_ns = frame["ts_event"].astype("int64")
    frame["valid_nbbo_event_ns"] = event_ns.where(valid_nbbo)
    frame["observed_bid_size"] = frame["bid_sz_00"].where(valid_nbbo, 0.0)
    frame["observed_ask_size"] = frame["ask_sz_00"].where(valid_nbbo, 0.0)
    receive_lag = (frame["ts_recv"] - frame["ts_event"]).dt.total_seconds() * 1_000_000_000
    valid_lag = receive_lag.notna() & (receive_lag >= 0)
    frame["receive_lag_ns"] = receive_lag.where(valid_lag, 0).astype("int64")
    frame["valid_receive_lag_ns"] = receive_lag.where(valid_lag)
    raw_flags = frame["flags"].fillna(0).astype("int64")
    for flag_name, bit in RECORD_FLAG_BITS.items():
        frame[f"flag_{flag_name}_count"] = ((raw_flags & bit) != 0).astype("int64")
    frame["data_quality_flagged_count"] = (
        (raw_flags & DATA_QUALITY_FLAG_MASK) != 0
    ).astype("int64")
    tolerance = ((frame["ask_px_00"] - frame["bid_px_00"]) * 0.02).clip(lower=0.005)
    at_ask = valid_nbbo & (frame["price"] >= frame["ask_px_00"] - tolerance)
    at_bid = valid_nbbo & ~at_ask & (frame["price"] <= frame["bid_px_00"] + tolerance)
    frame["bucket"] = "unknown"
    frame.loc[at_ask, "bucket"] = "at_ask"
    frame.loc[at_bid, "bucket"] = "at_bid"
    frame.loc[
        valid_nbbo & ~at_ask & ~at_bid & (frame["price"] > frame["bid_px_00"])
        & (frame["price"] < frame["ask_px_00"]),
        "bucket",
    ] = "inside"

    for kind, prefix in (("C", "call"), ("P", "put")):
        mask = frame["option_type"] == kind
        frame[f"{prefix}_count"] = mask.astype("int64")
        frame[f"{prefix}_volume"] = frame["size"].where(mask, 0.0)
        frame[f"{prefix}_premium"] = frame["notional"].where(mask, 0.0)
    for bucket in ("at_ask", "at_bid", "inside", "unknown"):
        mask = frame["bucket"] == bucket
        frame[f"{bucket}_count"] = mask.astype("int64")
        frame[f"{bucket}_volume"] = frame["size"].where(mask, 0.0)
        frame[f"{bucket}_notional"] = frame["notional"].where(mask, 0.0)
    for kind, prefix in (("C", "call"), ("P", "put")):
        for bucket in ("at_ask", "at_bid"):
            mask = (frame["option_type"] == kind) & (frame["bucket"] == bucket)
            frame[f"{prefix}_{bucket}_notional"] = frame["notional"].where(mask, 0.0)

    keys = ["family_root", "minute_utc"]
    observed_aggregations = dict(
        trade_count=("size", "count"), volume=("size", "sum"), notional=("notional", "sum"),
        call_count=("call_count", "sum"), put_count=("put_count", "sum"),
        call_volume=("call_volume", "sum"), put_volume=("put_volume", "sum"),
        call_premium=("call_premium", "sum"), put_premium=("put_premium", "sum"),
        first_price=("price", "first"), high_price=("price", "max"),
        low_price=("price", "min"), last_price=("price", "last"),
        price_volume_sum=("price_volume", "sum"), largest_trade_size=("size", "max"),
        largest_trade_notional=("notional", "max"),
        nbbo_valid_count=("nbbo_valid_count", "sum"),
        quoted_spread_sum=("quoted_spread", "sum"),
        quoted_spread_bps_sum=("quoted_spread_bps", "sum"),
        trade_to_mid_abs_sum=("trade_to_mid_abs", "sum"),
        trade_to_mid_signed_sum=("trade_to_mid_signed", "sum"),
        bid_size_sum=("observed_bid_size", "sum"), ask_size_sum=("observed_ask_size", "sum"),
        receive_lag_ns_sum=("receive_lag_ns", "sum"),
        receive_lag_ns_max=("valid_receive_lag_ns", "max"),
        flag_last_count=("flag_last_count", "sum"), flag_tob_count=("flag_tob_count", "sum"),
        flag_snapshot_count=("flag_snapshot_count", "sum"), flag_mbp_count=("flag_mbp_count", "sum"),
        flag_bad_ts_recv_count=("flag_bad_ts_recv_count", "sum"),
        flag_maybe_bad_book_count=("flag_maybe_bad_book_count", "sum"),
        flag_publisher_specific_count=("flag_publisher_specific_count", "sum"),
        data_quality_flagged_count=("data_quality_flagged_count", "sum"),
    )
    inferred_columns = {
        f"{bucket}_{metric}": (f"{bucket}_{metric}", "sum")
        for bucket in ("at_ask", "at_bid", "inside", "unknown")
        for metric in ("count", "volume", "notional")
    }
    inferred_columns.update({
        f"{kind}_{bucket}_notional": (f"{kind}_{bucket}_notional", "sum")
        for kind in ("call", "put") for bucket in ("at_ask", "at_bid")
    })
    family_aggregated = frame.groupby(keys, sort=True, observed=True).agg(
        **observed_aggregations, **inferred_columns
    ).reset_index()
    observed_frame = family_aggregated[
        keys + list(observed_aggregations)
    ].copy()
    inferred_frame = family_aggregated[keys + list(inferred_columns)].copy()

    contract_observed_frame = None
    contract_inferred_frame = None
    if include_contract_rows:
        contract_keys = [
            "family_root", "raw_symbol", "expiration", "option_type", "strike", "minute_utc"
        ]
        contract_observed_aggregations = dict(
            trade_count=("size", "count"), volume=("size", "sum"),
            notional=("notional", "sum"), first_price=("price", "first"),
            high_price=("price", "max"), low_price=("price", "min"),
            last_price=("price", "last"), price_volume_sum=("price_volume", "sum"),
            nbbo_valid_count=("nbbo_valid_count", "sum"),
            quoted_spread_sum=("quoted_spread", "sum"),
            quoted_spread_bps_sum=("quoted_spread_bps", "sum"),
            trade_to_mid_abs_sum=("trade_to_mid_abs", "sum"),
            trade_to_mid_signed_sum=("trade_to_mid_signed", "sum"),
            bid_size_sum=("observed_bid_size", "sum"),
            ask_size_sum=("observed_ask_size", "sum"),
            receive_lag_ns_sum=("receive_lag_ns", "sum"),
            receive_lag_ns_max=("valid_receive_lag_ns", "max"),
            data_quality_flagged_count=("data_quality_flagged_count", "sum"),
            last_pretrade_midpoint=("pretrade_midpoint", "last"),
            last_nbbo_event_ns=("valid_nbbo_event_ns", "max"),
        )
        contract_inferred_aggregations = {
            f"{bucket}_{metric}": (f"{bucket}_{metric}", "sum")
            for bucket in ("at_ask", "at_bid", "inside", "unknown")
            for metric in ("count", "volume", "notional")
        }
        contract_aggregated = frame.groupby(
            contract_keys, sort=True, observed=True
        ).agg(
            **contract_observed_aggregations, **contract_inferred_aggregations
        ).reset_index()
        contract_observed_frame = contract_aggregated[
            contract_keys + list(contract_observed_aggregations)
        ].copy()
        contract_inferred_frame = contract_aggregated[
            contract_keys + list(contract_inferred_aggregations)
        ].copy()

    def _records(dataframe) -> list[dict[str, object]]:
        rows: list[dict[str, object]] = []
        columns = list(dataframe.columns)
        for values in dataframe.itertuples(index=False, name=None):
            row = {
                key: (None if pd.isna(value) else value)
                for key, value in zip(columns, values)
            }
            row["minute_utc"] = row["minute_utc"].isoformat()
            rows.append(row)
        return rows

    observed_rows = _records(observed_frame)
    inferred_rows = _records(inferred_frame)
    contract_observed_rows = _records(contract_observed_frame) if contract_observed_frame is not None else []
    contract_inferred_rows = _records(contract_inferred_frame) if contract_inferred_frame is not None else []
    for row in observed_rows:
        row.update({"session_id": session_id, "feed_name": feed_name, "asset_class": "options"})
    for row in inferred_rows:
        row.update({
            "session_id": session_id, "feed_name": feed_name,
            "inference_method": INFERENCE_METHOD, "inference_version": INFERENCE_VERSION,
            "source_sha256": evidence_sha256,
        })
    for row in contract_observed_rows:
        row.update({"session_id": session_id, "feed_name": feed_name})
    for row in contract_inferred_rows:
        row.update({
            "session_id": session_id, "feed_name": feed_name,
            "inference_method": INFERENCE_METHOD, "inference_version": INFERENCE_VERSION,
            "source_sha256": evidence_sha256,
        })

    timestamp = datetime.now(UTC).isoformat()
    canonical_hasher = hashlib.sha256()
    encoder = json.JSONEncoder(
        sort_keys=True, separators=(",", ":"), allow_nan=False,
    )
    for chunk in encoder.iterencode(
        {
            "observed": observed_rows, "inferred": inferred_rows,
            "contract_observed": contract_observed_rows,
            "contract_inferred": contract_inferred_rows,
        }
    ):
        canonical_hasher.update(chunk.encode())
    feature_hash = canonical_hasher.hexdigest()
    for row in chain(
        observed_rows, inferred_rows, contract_observed_rows, contract_inferred_rows
    ):
        row["updated_at_utc"] = timestamp
    if include_contract_rows:
        return (
            observed_rows, inferred_rows, contract_observed_rows,
            contract_inferred_rows, feature_hash,
        )
    return observed_rows, inferred_rows, feature_hash


def persist_minute_rows(catalog, observed: list[Mapping[str, object]], inferred: list[Mapping[str, object]]) -> None:
    catalog.upsert_observed_minutes(observed)
    catalog.upsert_inferred_minute_flow(inferred)


def persist_contract_minute_rows(
    catalog,
    observed: list[Mapping[str, object]],
    inferred: list[Mapping[str, object]],
) -> None:
    catalog.upsert_observed_contract_minutes(observed)
    catalog.upsert_inferred_contract_minute_flow(inferred)
