from __future__ import annotations

import argparse
import hashlib
import json
import logging
import math
import os
import queue
import re
import shutil
import signal
import sqlite3
import sys
import threading
import time
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Callable

import databento as db
import databento_dbn as dbn
from dotenv import load_dotenv

from .catalog import TapeCatalog
from .config import FeedSpec, SessionConfig, UTC, build_session_config
from .integrity import inspect_dbn
from backend.runtime_controls import ES_CONTINUOUS, ES_SYSTEM_REQUIRED, SystemSleepGuard


LOGGER = logging.getLogger("marketpin.closing_tape")
RAW_OPTION_RE = re.compile(
    r"^(?P<root>[A-Z0-9]+)\s+(?P<yymmdd>\d{6})(?P<cp>[CP])(?P<strike>\d{8})$"
)
FAMILY_ROOTS = {"SPXW": "SPX", "NDXP": "NDX", "RUTW": "RUT", "VIXW": "VIX"}
INFERENCE_METHOD = "trade_price_vs_pretrade_nbbo"
INFERENCE_VERSION = "1.0"
ANALYSIS_HORIZON_ID = "cash-close-minus-15m-v1"
MAX_UNMAPPED_TRADE_RATIO = 0.005
RECORD_FLAG_BITS = {
    "last": 128, "tob": 64, "snapshot": 32, "mbp": 16,
    "bad_ts_recv": 8, "maybe_bad_book": 4, "publisher_specific": 2,
}
DATA_QUALITY_FLAG_MASK = 8 | 4 | 2
TAPE_LIVE_CLIENT_CLOSE_TIMEOUT_SECONDS = 5.0
NORMAL_PRIORITY_CLASS = 0x00000020
WINDOWS_PRIORITY_NAMES = {
    0x00000040: "idle",
    0x00004000: "below_normal",
    NORMAL_PRIORITY_CLASS: "normal",
    0x00008000: "above_normal",
    0x00000080: "high",
    0x00000100: "realtime",
}


def utcnow() -> datetime:
    return datetime.now(UTC)


def monitor_pause_reason(
    elapsed_seconds: float,
    expected_interval_seconds: float,
) -> str | None:
    """Describe a monitor pause large enough to invalidate continuous capture."""
    threshold = max(30.0, float(expected_interval_seconds) * 3.0)
    if float(elapsed_seconds) <= threshold:
        return None
    return (
        f"recorder monitor paused for {float(elapsed_seconds):.1f} seconds "
        f"(expected no more than {threshold:.1f}); "
        "system sleep or a severe scheduler stall is likely"
    )


def retry_sqlite_busy(
    operation: Callable[[], object],
    *,
    attempts: int = 3,
    delay_seconds: float = 0.25,
) -> object:
    """Retry bounded catalog contention without hiding persistent failure."""
    limit = max(1, int(attempts))
    for attempt in range(limit):
        try:
            return operation()
        except sqlite3.OperationalError as exc:
            message = str(exc).lower()
            if not any(token in message for token in ("locked", "busy")):
                raise
            if attempt + 1 >= limit:
                raise
            time.sleep(max(0.0, float(delay_seconds)))
    raise AssertionError("unreachable SQLite retry state")


class ProcessPriorityControl:
    """Normalize Task Scheduler's inherited below-normal priority on Windows."""

    def __init__(
        self,
        *,
        platform_name: str | None = None,
        current_process: Callable[[], object] | None = None,
        getter: Callable[[object], object] | None = None,
        setter: Callable[[object, int], object] | None = None,
    ):
        self.platform_name = platform_name or os.name
        self.requested = self.platform_name == "nt"
        self.active = False
        self.before_code: int | None = None
        self.after_code: int | None = None
        self.error: str | None = None
        self._current_process = current_process
        self._getter = getter
        self._setter = setter

    def _resolve_api(self) -> tuple[
        Callable[[], object],
        Callable[[object], object],
        Callable[[object, int], object],
    ]:
        if self._current_process is None or self._getter is None or self._setter is None:
            import ctypes

            kernel32 = ctypes.windll.kernel32
            kernel32.GetCurrentProcess.restype = ctypes.c_void_p
            kernel32.GetPriorityClass.argtypes = (ctypes.c_void_p,)
            kernel32.GetPriorityClass.restype = ctypes.c_uint32
            kernel32.SetPriorityClass.argtypes = (ctypes.c_void_p, ctypes.c_uint32)
            kernel32.SetPriorityClass.restype = ctypes.c_int
            self._current_process = kernel32.GetCurrentProcess
            self._getter = kernel32.GetPriorityClass
            self._setter = kernel32.SetPriorityClass
        return self._current_process, self._getter, self._setter

    def normalize(self) -> bool:
        if not self.requested:
            return False
        try:
            current_process, getter, setter = self._resolve_api()
            handle = current_process()
            self.before_code = int(getter(handle) or 0)
            if self.before_code == 0:
                self.error = "GetPriorityClass returned 0 before normalization"
                return False
            if self.before_code != NORMAL_PRIORITY_CLASS:
                if int(setter(handle, NORMAL_PRIORITY_CLASS) or 0) == 0:
                    self.error = "SetPriorityClass returned 0"
                    return False
            self.after_code = int(getter(handle) or 0)
        except Exception as exc:
            self.error = f"{type(exc).__name__}: {str(exc)[:300]}"
            return False
        if self.after_code != NORMAL_PRIORITY_CLASS:
            self.error = (
                "process priority did not normalize to normal "
                f"(observed={self.after_code})"
            )
            return False
        self.active = True
        self.error = None
        return True

    def to_dict(self) -> dict[str, object]:
        return {
            "platform": self.platform_name,
            "requested": self.requested,
            "active": self.active,
            "mode": "normal_priority_class" if self.requested else "not_applicable",
            "before": WINDOWS_PRIORITY_NAMES.get(self.before_code),
            "before_code": self.before_code,
            "after": WINDOWS_PRIORITY_NAMES.get(self.after_code),
            "after_code": self.after_code,
            "error": self.error,
        }


def sha256_prefix(path: Path, byte_count: int, chunk_size: int = 1024 * 1024) -> str:
    remaining = int(byte_count)
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while remaining > 0:
            chunk = stream.read(min(chunk_size, remaining))
            if not chunk:
                raise OSError(f"raw tape ended before analysis cutoff ({remaining} bytes missing)")
            digest.update(chunk)
            remaining -= len(chunk)
    return digest.hexdigest()


def record_timestamp_ns(record: object, field: str) -> int | None:
    try:
        value = int(getattr(record, field))
    except (AttributeError, TypeError, ValueError, OverflowError):
        return None
    return value if 0 < value < 10**19 else None


def normalize_price(value: object, *, allow_zero: bool = False) -> float | None:
    try:
        raw_integer = int(value)
    except (TypeError, ValueError, OverflowError):
        raw_integer = None
    if raw_integer is not None and raw_integer >= int(dbn.UNDEF_PRICE):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    if not math.isfinite(number) or number < 0 or (number == 0 and not allow_zero):
        return None
    if number > 1_000_000:
        number /= 1_000_000_000.0
    return number if math.isfinite(number) and (number > 0 or allow_zero) else None


def parse_raw_symbol(raw_symbol: str) -> dict[str, object]:
    symbol = str(raw_symbol or "").strip()
    match = RAW_OPTION_RE.match(symbol)
    if not match:
        return {
            "raw_symbol": symbol,
            "family_root": FAMILY_ROOTS.get(symbol, symbol),
            "option_type": None,
            "expiration": None,
            "strike": None,
        }
    root = match.group("root")
    return {
        "raw_symbol": symbol,
        "family_root": FAMILY_ROOTS.get(root, root),
        "option_type": match.group("cp"),
        "expiration": datetime.strptime(match.group("yymmdd"), "%y%m%d").date().isoformat(),
        "strike": int(match.group("strike")) / 1000.0,
    }


def infer_execution_location(price: float, bid: float | None, ask: float | None) -> str:
    """Classify trade location without claiming a provider-supplied side."""
    if bid is None or ask is None or ask < bid:
        return "unknown"
    tolerance = max(0.005, (ask - bid) * 0.02)
    if price >= ask - tolerance:
        return "at_ask"
    if price <= bid + tolerance:
        return "at_bid"
    if bid < price < ask:
        return "inside"
    return "unknown"


def tcbbo_integrity_issues(integrity: object) -> list[str]:
    records = int(getattr(integrity, "tcbbo_records", 0) or 0)
    timestamped = int(getattr(integrity, "tcbbo_timestamped_records", 0) or 0)
    valid_nbbo = int(getattr(integrity, "tcbbo_valid_nbbo_records", 0) or 0)
    if records <= 0:
        return ["TCBBO subscription produced no TCBBO records"]
    issues = []
    if timestamped != records:
        issues.append("not every TCBBO record preserved event and receive timestamps")
    if valid_nbbo / records < 0.95:
        issues.append("fewer than 95 percent of TCBBO records contain a valid pre-trade NBBO")
    return issues


def provisional_tcbbo_integrity_issues(
    status: dict[str, object], integrity: object
) -> list[str]:
    """Reconcile non-authoritative callback counters with terminal DBN truth."""
    comparisons = (
        (
            "provisional_tcbbo_records",
            "tcbbo_records",
            "TCBBO record",
        ),
        (
            "provisional_tcbbo_timestamped_records",
            "tcbbo_timestamped_records",
            "timestamped TCBBO record",
        ),
        (
            "provisional_tcbbo_valid_nbbo_records",
            "tcbbo_valid_nbbo_records",
            "valid pre-trade NBBO record",
        ),
    )
    issues = []
    for provisional_field, terminal_field, label in comparisons:
        provisional = int(status.get(provisional_field) or 0)
        terminal = int(getattr(integrity, terminal_field, 0) or 0)
        if provisional != terminal:
            issues.append(
                f"provisional {label} count does not match terminal DBN scan "
                f"({provisional}/{terminal})"
            )
    return issues


def excessive_unmapped_trade_ratio(unmapped: int, trades: int) -> bool:
    return trades > 0 and unmapped / trades > MAX_UNMAPPED_TRADE_RATIO


@dataclass
class MinuteBucket:
    session_id: str
    feed_name: str
    family_root: str
    minute_utc: str
    asset_class: str
    trade_count: int = 0
    volume: float = 0.0
    notional: float = 0.0
    call_count: int = 0
    put_count: int = 0
    call_volume: float = 0.0
    put_volume: float = 0.0
    call_premium: float = 0.0
    put_premium: float = 0.0
    first_price: float | None = None
    high_price: float | None = None
    low_price: float | None = None
    last_price: float | None = None
    price_volume_sum: float = 0.0
    largest_trade_size: float = 0.0
    largest_trade_notional: float = 0.0
    nbbo_valid_count: int = 0
    quoted_spread_sum: float = 0.0
    quoted_spread_bps_sum: float = 0.0
    trade_to_mid_abs_sum: float = 0.0
    trade_to_mid_signed_sum: float = 0.0
    bid_size_sum: float = 0.0
    ask_size_sum: float = 0.0
    receive_lag_ns_sum: int = 0
    receive_lag_ns_max: int | None = None
    flag_last_count: int = 0
    flag_tob_count: int = 0
    flag_snapshot_count: int = 0
    flag_mbp_count: int = 0
    flag_bad_ts_recv_count: int = 0
    flag_maybe_bad_book_count: int = 0
    flag_publisher_specific_count: int = 0
    data_quality_flagged_count: int = 0
    at_ask_count: int = 0
    at_bid_count: int = 0
    inside_count: int = 0
    unknown_count: int = 0
    at_ask_volume: float = 0.0
    at_bid_volume: float = 0.0
    inside_volume: float = 0.0
    unknown_volume: float = 0.0
    at_ask_notional: float = 0.0
    at_bid_notional: float = 0.0
    inside_notional: float = 0.0
    unknown_notional: float = 0.0
    call_at_ask_notional: float = 0.0
    call_at_bid_notional: float = 0.0
    put_at_ask_notional: float = 0.0
    put_at_bid_notional: float = 0.0
    _first_trade_order: tuple[object, ...] | None = field(
        default=None, repr=False, compare=False
    )
    _last_trade_order: tuple[object, ...] | None = field(
        default=None, repr=False, compare=False
    )

    def add(
        self,
        *,
        price: float,
        size: float,
        notional: float,
        event_ns: int,
        receive_ns: int | None,
        instrument_id: int,
        option_type: str | None,
        location: str,
        bid: float | None = None,
        ask: float | None = None,
        bid_size: float | None = None,
        ask_size: float | None = None,
        receive_lag_ns: int | None = None,
        flags: int = 0,
    ) -> None:
        # Provider callbacks can arrive in a different order during a replay.
        # Define first/last by immutable provider chronology plus stable record
        # fields, never by local callback arrival order.
        trade_order = (
            int(event_ns),
            int(receive_ns if receive_ns is not None else event_ns),
            int(instrument_id),
            float(price),
            float(size),
            float(bid if bid is not None else -math.inf),
            float(ask if ask is not None else -math.inf),
            float(bid_size if bid_size is not None else 0.0),
            float(ask_size if ask_size is not None else 0.0),
            int(flags),
        )
        self.trade_count += 1
        self.volume += size
        self.notional += notional
        self.price_volume_sum += price * size
        if self._first_trade_order is None or trade_order < self._first_trade_order:
            self._first_trade_order = trade_order
            self.first_price = price
        self.high_price = price if self.high_price is None else max(self.high_price, price)
        self.low_price = price if self.low_price is None else min(self.low_price, price)
        if self._last_trade_order is None or trade_order > self._last_trade_order:
            self._last_trade_order = trade_order
            self.last_price = price
        self.largest_trade_size = max(self.largest_trade_size, size)
        self.largest_trade_notional = max(self.largest_trade_notional, notional)
        if bid is not None and ask is not None and 0 <= bid <= ask and ask > 0:
            midpoint = (bid + ask) / 2.0
            spread = ask - bid
            self.nbbo_valid_count += 1
            self.quoted_spread_sum += spread
            if midpoint > 0:
                self.quoted_spread_bps_sum += spread / midpoint * 10_000.0
            self.trade_to_mid_abs_sum += abs(price - midpoint)
            self.trade_to_mid_signed_sum += price - midpoint
            self.bid_size_sum += max(0.0, bid_size or 0.0)
            self.ask_size_sum += max(0.0, ask_size or 0.0)
        if receive_lag_ns is not None and receive_lag_ns >= 0:
            self.receive_lag_ns_sum += receive_lag_ns
            self.receive_lag_ns_max = (
                receive_lag_ns if self.receive_lag_ns_max is None
                else max(self.receive_lag_ns_max, receive_lag_ns)
            )
        for flag_name, bit in RECORD_FLAG_BITS.items():
            field = f"flag_{flag_name}_count"
            setattr(self, field, getattr(self, field) + int((flags & bit) != 0))
        self.data_quality_flagged_count += int((flags & DATA_QUALITY_FLAG_MASK) != 0)
        if option_type == "C":
            self.call_count += 1
            self.call_volume += size
            self.call_premium += notional
        elif option_type == "P":
            self.put_count += 1
            self.put_volume += size
            self.put_premium += notional
        setattr(self, f"{location}_count", getattr(self, f"{location}_count") + 1)
        setattr(self, f"{location}_volume", getattr(self, f"{location}_volume") + size)
        setattr(self, f"{location}_notional", getattr(self, f"{location}_notional") + notional)
        if location in {"at_ask", "at_bid"} and option_type in {"C", "P"}:
            prefix = "call" if option_type == "C" else "put"
            field = f"{prefix}_{location}_notional"
            setattr(self, field, getattr(self, field) + notional)

    def observed_row(self) -> dict[str, object]:
        fields = (
            "trade_count", "volume", "notional", "call_count", "put_count",
            "call_volume", "put_volume", "call_premium", "put_premium",
            "first_price", "high_price", "low_price", "last_price",
            "price_volume_sum", "largest_trade_size", "largest_trade_notional",
            "nbbo_valid_count", "quoted_spread_sum", "quoted_spread_bps_sum",
            "trade_to_mid_abs_sum", "trade_to_mid_signed_sum", "bid_size_sum",
            "ask_size_sum", "receive_lag_ns_sum", "receive_lag_ns_max",
            "flag_last_count", "flag_tob_count", "flag_snapshot_count", "flag_mbp_count",
            "flag_bad_ts_recv_count", "flag_maybe_bad_book_count",
            "flag_publisher_specific_count", "data_quality_flagged_count",
        )
        result = {
            "session_id": self.session_id,
            "feed_name": self.feed_name,
            "family_root": self.family_root,
            "minute_utc": self.minute_utc,
            "asset_class": self.asset_class,
            "updated_at_utc": utcnow().isoformat(),
        }
        result.update({field: getattr(self, field) for field in fields})
        return result

    def inferred_row(self, source_sha256: str = "pending-live-file") -> dict[str, object]:
        fields = (
            "at_ask_count", "at_bid_count", "inside_count", "unknown_count",
            "at_ask_volume", "at_bid_volume", "inside_volume", "unknown_volume",
            "at_ask_notional", "at_bid_notional", "inside_notional", "unknown_notional",
            "call_at_ask_notional", "call_at_bid_notional", "put_at_ask_notional",
            "put_at_bid_notional",
        )
        result = {
            "session_id": self.session_id,
            "feed_name": self.feed_name,
            "family_root": self.family_root,
            "minute_utc": self.minute_utc,
            "inference_method": INFERENCE_METHOD,
            "inference_version": INFERENCE_VERSION,
            "source_sha256": source_sha256,
            "updated_at_utc": utcnow().isoformat(),
        }
        result.update({field: getattr(self, field) for field in fields})
        return result


class LiveAggregator:
    def __init__(self, session_id: str, feed: FeedSpec, catalog: TapeCatalog):
        self.session_id = session_id
        self.feed = feed
        self.catalog = catalog
        self._lock = threading.RLock()
        self._flush_lock = threading.Lock()
        self.instruments: dict[int, dict[str, object]] = {}
        self.dirty_instruments: set[int] = set()
        self.open_interest: dict[int, dict[str, object]] = {}
        self.dirty_open_interest: set[int] = set()
        self.deleted_open_interest: set[int] = set()
        self.minutes: dict[tuple[str, str], MinuteBucket] = {}
        self.records_seen = 0
        self.trade_records = 0
        self.provisional_tcbbo_records = 0
        self.provisional_tcbbo_timestamped_records = 0
        self.provisional_tcbbo_valid_nbbo_records = 0
        self.mapping_records = 0
        self.statistics_records = 0
        self.definition_records = 0
        self.unmapped_trade_records = 0
        self.first_event_ns: int | None = None
        self.last_event_ns: int | None = None
        self.last_receive_ns: int | None = None
        self.last_trade_event_ns: int | None = None
        self.root_trade_watermarks: dict[str, int] = {}
        self.root_trade_counts: dict[str, int] = {}
        self.subscription_acks = 0
        self.replay_completed = 0
        self.reconnect_gaps: list[dict[str, str]] = []
        self.derived_gaps: list[dict[str, str]] = []
        self.operational_warnings: list[dict[str, str]] = []
        self.slow_warnings = 0
        self.provider_error_count = 0
        self.callback_drop_records = 0
        self.errors: list[str] = []

    def _mapping(self, record: object) -> None:
        symbol = getattr(record, "stype_out_symbol", None) or getattr(record, "stype_in_symbol", None)
        if not symbol:
            return
        instrument_id = int(getattr(record, "instrument_id"))
        existing = self.instruments.get(instrument_id)
        if existing is not None and existing.get("raw_symbol") == str(symbol):
            return
        item = parse_raw_symbol(str(symbol))
        item.update(
            session_id=self.session_id,
            feed_name=self.feed.name,
            instrument_id=instrument_id,
            first_seen_utc=utcnow().isoformat(),
        )
        self.instruments[instrument_id] = item
        self.dirty_instruments.add(instrument_id)
        if instrument_id in self.open_interest:
            self.open_interest[instrument_id].update(
                raw_symbol=item.get("raw_symbol"),
                family_root=item.get("family_root"),
            )
            self.dirty_open_interest.add(instrument_id)

    def _statistic(self, record: object) -> None:
        try:
            stat_type = int(getattr(record, "stat_type"))
            instrument_id = int(getattr(record, "instrument_id"))
        except (AttributeError, TypeError, ValueError, OverflowError):
            return
        if stat_type != int(dbn.StatType.OPEN_INTEREST):
            return
        try:
            action = int(getattr(record, "update_action"))
        except (AttributeError, TypeError, ValueError, OverflowError):
            action = int(dbn.StatUpdateAction.NEW)
        if action == int(dbn.StatUpdateAction.DELETE):
            self.open_interest.pop(instrument_id, None)
            self.dirty_open_interest.discard(instrument_id)
            self.deleted_open_interest.add(instrument_id)
            return
        if action != int(dbn.StatUpdateAction.NEW):
            return
        try:
            quantity = float(getattr(record, "quantity"))
        except (AttributeError, TypeError, ValueError, OverflowError):
            return
        if (
            not math.isfinite(quantity)
            or quantity < 0
            or quantity >= int(dbn.UNDEF_STAT_QUANTITY)
        ):
            return
        # OPRA's live open-interest snapshots commonly leave ts_ref undefined
        # (u64 max). In that case, ts_event is the point-in-time publication
        # timestamp at which this daily baseline became observable.
        reference_ns = record_timestamp_ns(record, "ts_ref") or record_timestamp_ns(
            record, "ts_event"
        )
        receive_ns = record_timestamp_ns(record, "ts_recv")
        if reference_ns is None or receive_ns is None:
            return
        asof = datetime.fromtimestamp(reference_ns / 1e9, tz=UTC).isoformat()
        available_at = datetime.fromtimestamp(receive_ns / 1e9, tz=UTC).isoformat()
        mapped = self.instruments.get(instrument_id, {})
        self.open_interest[instrument_id] = {
            "session_id": self.session_id,
            "feed_name": self.feed.name,
            "instrument_id": instrument_id,
            "raw_symbol": mapped.get("raw_symbol"),
            "family_root": mapped.get("family_root"),
            "asof_utc": asof,
            "available_at_utc": available_at,
            "open_interest": quantity,
        }
        self.deleted_open_interest.discard(instrument_id)
        self.dirty_open_interest.add(instrument_id)

    def __call__(self, record: object) -> None:
        name = type(record).__name__
        event_ns = record_timestamp_ns(record, "ts_event")
        receive_ns = record_timestamp_ns(record, "ts_recv")
        with self._lock:
            self.records_seen += 1
            if event_ns is not None:
                self.first_event_ns = event_ns if self.first_event_ns is None else min(self.first_event_ns, event_ns)
                self.last_event_ns = event_ns if self.last_event_ns is None else max(self.last_event_ns, event_ns)
            if receive_ns is not None:
                self.last_receive_ns = receive_ns if self.last_receive_ns is None else max(self.last_receive_ns, receive_ns)
            if name == "SystemMsg":
                message = str(getattr(record, "msg", ""))
                lowered_message = message.lower()
                try:
                    code = int(getattr(record, "code"))
                except (AttributeError, TypeError, ValueError, OverflowError):
                    code = None
                if code == int(dbn.SystemCode.SUBSCRIPTION_ACK) or (
                    "subscription" in lowered_message and "succeeded" in lowered_message
                ):
                    self.subscription_acks += 1
                if code == int(dbn.SystemCode.REPLAY_COMPLETED) or "replay completed" in lowered_message:
                    self.replay_completed += 1
                if code == int(dbn.SystemCode.SLOW_READER_WARNING) or any(
                    token in lowered_message for token in ("slow reader", "queue is full", "skipped")
                ):
                    self.slow_warnings += 1
                    self.errors.append(f"provider slow-reader warning: {message[:400]}")
                return
            if name == "SymbolMappingMsg":
                self.mapping_records += 1
                self._mapping(record)
                return
            lowered = name.lower()
            if "definition" in lowered or "instrumentdef" in lowered:
                self.definition_records += 1
                return
            if hasattr(record, "stat_type"):
                self.statistics_records += 1
                self._statistic(record)
                return
            if name == "ErrorMsg":
                message = str(getattr(record, "err", "")) or str(record)
                self.provider_error_count += 1
                self.errors.append(message[:500])
                self.derived_gaps.append({"provider_error": message[:400]})
                if any(token in message.lower() for token in ("slow", "skip", "queue")):
                    self.slow_warnings += 1
                return
            bid = None
            ask = None
            is_tcbbo = hasattr(record, "bid_px_00") and hasattr(record, "ask_px_00")
            if is_tcbbo:
                self.provisional_tcbbo_records += 1
                if event_ns is not None and receive_ns is not None:
                    self.provisional_tcbbo_timestamped_records += 1
                bid = normalize_price(
                    getattr(record, "bid_px_00", None), allow_zero=True
                )
                ask = normalize_price(getattr(record, "ask_px_00", None))
                if bid is not None and ask is not None and bid <= ask:
                    self.provisional_tcbbo_valid_nbbo_records += 1
            if not (hasattr(record, "price") and hasattr(record, "size")):
                return
            price = normalize_price(getattr(record, "price", None))
            try:
                size = float(getattr(record, "size"))
                instrument_id = int(getattr(record, "instrument_id"))
            except (AttributeError, TypeError, ValueError, OverflowError):
                return
            if price is None or not math.isfinite(size) or size <= 0 or event_ns is None:
                return
            self.trade_records += 1
            self.last_trade_event_ns = (
                event_ns if self.last_trade_event_ns is None else max(self.last_trade_event_ns, event_ns)
            )
            mapped = self.instruments.get(instrument_id)
            if not mapped:
                self.unmapped_trade_records += 1
                return
            root = str(mapped.get("family_root") or "UNKNOWN")
            self.root_trade_watermarks[root] = max(self.root_trade_watermarks.get(root, 0), event_ns)
            self.root_trade_counts[root] = self.root_trade_counts.get(root, 0) + 1
            minute = datetime.fromtimestamp(event_ns / 1e9, tz=UTC).replace(second=0, microsecond=0).isoformat()
            key = (root, minute)
            bucket = self.minutes.get(key)
            if bucket is None:
                bucket = MinuteBucket(
                    self.session_id,
                    self.feed.name,
                    root,
                    minute,
                    self.feed.asset_class,
                )
                self.minutes[key] = bucket
            location = infer_execution_location(price, bid, ask)
            receive_lag_ns = receive_ns - event_ns if receive_ns is not None else None
            bucket.add(
                price=price,
                size=size,
                notional=price * size * self.feed.contract_multiplier,
                event_ns=event_ns,
                receive_ns=receive_ns,
                instrument_id=instrument_id,
                option_type=str(mapped.get("option_type")) if mapped.get("option_type") else None,
                location=location,
                bid=bid,
                ask=ask,
                bid_size=float(getattr(record, "bid_sz_00", 0) or 0),
                ask_size=float(getattr(record, "ask_sz_00", 0) or 0),
                receive_lag_ns=receive_lag_ns,
                flags=int(getattr(record, "flags", 0) or 0),
            )

    def reconnect_gap(self, start: object, end: object) -> None:
        with self._lock:
            self.reconnect_gaps.append({"reconnect_start": str(start), "reconnect_end": str(end)})

    def exception(self, exc: Exception) -> None:
        with self._lock:
            self.errors.append(f"{type(exc).__name__}: {str(exc)[:400]}")

    def callback_drop(self) -> None:
        """Record loss in the derived-feature queue without blocking DBN capture."""
        with self._lock:
            self.callback_drop_records += 1
            if self.callback_drop_records == 1:
                self.slow_warnings += 1
                self.errors.append(
                    "derived aggregation queue overflowed; raw DBN remains canonical but live features are incomplete"
                )

    def mark_incomplete(self, reason: str) -> None:
        with self._lock:
            self.slow_warnings += 1
            self.derived_gaps.append({"derived_incomplete": reason[:400]})
            self.errors.append(reason[:500])

    def record_operational_warning(self, reason: str) -> None:
        """Retain scheduler/monitor evidence without asserting raw data loss."""
        with self._lock:
            self.operational_warnings.append({"operational_warning": reason[:400]})

    def flush(self) -> None:
        with self._flush_lock:
            with self._lock:
                instrument_keys = set(self.dirty_instruments)
                interest_keys = set(self.dirty_open_interest)
                deleted_interest = set(self.deleted_open_interest)
                instruments = [self.instruments[key].copy() for key in instrument_keys]
                interest = [self.open_interest[key].copy() for key in interest_keys]
                observed = [bucket.observed_row() for bucket in self.minutes.values()]
                inferred = [bucket.inferred_row() for bucket in self.minutes.values()]
                # New callback updates can re-add keys while persistence runs.
                self.dirty_instruments.difference_update(instrument_keys)
                self.dirty_open_interest.difference_update(interest_keys)
                self.deleted_open_interest.difference_update(deleted_interest)
            try:
                self.catalog.persist_live_snapshot(
                    instruments=instruments,
                    deleted_open_interest=list(deleted_interest),
                    open_interest=interest,
                    observed=observed,
                    inferred=inferred,
                    session_id=self.session_id,
                    feed_name=self.feed.name,
                )
            except Exception:
                with self._lock:
                    self.dirty_instruments.update(instrument_keys)
                    self.dirty_open_interest.update(
                        key
                        for key in interest_keys
                        if key in self.open_interest and key not in self.deleted_open_interest
                    )
                    self.deleted_open_interest.update(
                        key for key in deleted_interest if key not in self.open_interest
                    )
                raise

    def status(self) -> dict[str, object]:
        with self._lock:
            return {
                "records_seen": self.records_seen,
                "trade_records": self.trade_records,
                "provisional_tcbbo_records": self.provisional_tcbbo_records,
                "provisional_tcbbo_timestamped_records": (
                    self.provisional_tcbbo_timestamped_records
                ),
                "provisional_tcbbo_valid_nbbo_records": (
                    self.provisional_tcbbo_valid_nbbo_records
                ),
                "mapping_records": self.mapping_records,
                "statistics_records": self.statistics_records,
                "definition_records": self.definition_records,
                "unmapped_trade_records": self.unmapped_trade_records,
                "first_event_ns": self.first_event_ns,
                "last_event_ns": self.last_event_ns,
                "last_receive_ns": self.last_receive_ns,
                "last_trade_event_ns": self.last_trade_event_ns,
                "root_trade_watermarks_json": json.dumps(self.root_trade_watermarks, separators=(",", ":")),
                "root_trade_counts_json": json.dumps(self.root_trade_counts, separators=(",", ":")),
                "subscription_acks": self.subscription_acks,
                "replay_completed": self.replay_completed,
                "reconnect_count": len(self.reconnect_gaps),
                "slow_reader_warnings": self.slow_warnings,
                "provider_error_count": self.provider_error_count,
                "gaps_json": json.dumps(
                    [
                        *self.reconnect_gaps,
                        *self.derived_gaps,
                        *self.operational_warnings,
                        *(
                        [{"derived_callback_drop_records": str(self.callback_drop_records)}]
                        if self.callback_drop_records else []
                        ),
                    ],
                    separators=(",", ":"),
                ),
                "error": self.errors[-1] if self.errors else None,
            }


class ProcessFileLock:
    def __init__(self, path: Path):
        self.path = path
        self.handle = None

    def __enter__(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        handle = self.path.open("a+b")
        if self.path.stat().st_size == 0:
            handle.write(b"0")
            handle.flush()
        handle.seek(0)
        try:
            if os.name == "nt":
                import msvcrt

                msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl

                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            handle.close()
            raise RuntimeError(f"closing-tape recorder is already running: {self.path}") from exc
        self.handle = handle
        return self

    def __exit__(self, exc_type, exc, tb):
        if self.handle is None:
            return
        try:
            self.handle.seek(0)
            if os.name == "nt":
                import msvcrt

                msvcrt.locking(self.handle.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl

                fcntl.flock(self.handle.fileno(), fcntl.LOCK_UN)
        finally:
            self.handle.close()


class FeedThread(threading.Thread):
    def __init__(
        self,
        config: SessionConfig,
        feed: FeedSpec,
        catalog: TapeCatalog,
        status_callback: Callable[[], None],
        *,
        live_factory: Callable[..., object] = db.Live,
        parquet: bool = True,
    ):
        super().__init__(name=f"closing-tape-{feed.name}", daemon=False)
        self.config = config
        self.feed = feed
        self.catalog = catalog
        self.status_callback = status_callback
        self.live_factory = live_factory
        self.parquet = parquet
        self.path = config.output_dir / f"{feed.name}.{config.session_id}.dbn"
        self.aggregator = LiveAggregator(config.session_id, feed, catalog)
        self.client = None
        self.stream_handle = None
        self.failure: str | None = None
        self.monitor_stop = threading.Event()
        # Databento dispatches Python callbacks on its network event loop.  The
        # callback therefore does only a non-blocking enqueue; symbol parsing,
        # aggregation, and SQLite work happen off that loop. Raw DBN is written
        # before callbacks are dispatched and remains the source of truth.
        self.record_queue: queue.Queue[tuple[int, int, object]] = queue.Queue(maxsize=250_000)
        self.worker_stop = threading.Event()
        self.sequence_condition = threading.Condition()
        self.enqueued_sequence = 0
        self.processed_sequence = 0
        self.enqueued_record_bytes = 0
        self.stream_metadata_bytes: int | None = None
        self.analysis_pause_target: int | None = None
        self.analysis_deadline_sequence: int | None = None
        self.analysis_deadline_bytes: int | None = None
        self.worker_paused = False

    def _enqueue_record(self, record: object) -> None:
        callback_observed_at = utcnow()
        try:
            record_bytes = int(record.record_size())
        except (AttributeError, TypeError, ValueError, OverflowError):
            try:
                record_bytes = len(bytes(record))
            except (TypeError, ValueError):
                record_bytes = 0
        with self.sequence_condition:
            if self.stream_metadata_bytes is None:
                # SDK metadata is written lazily when the session starts. The
                # callback runs immediately after the current record's raw
                # write, so this difference is the exact DBN header length.
                self.stream_metadata_bytes = self.path.stat().st_size - record_bytes
                if self.stream_metadata_bytes < 8:
                    self.aggregator.mark_incomplete("could not establish DBN metadata byte length")
                    self.stream_metadata_bytes = max(0, self.stream_metadata_bytes)
            self.enqueued_record_bytes += record_bytes
            next_sequence = self.enqueued_sequence + 1
            raw_end_bytes = int(self.stream_metadata_bytes) + self.enqueued_record_bytes
            if (
                self.analysis_deadline_sequence is None
                and self.config.analysis_due_utc <= callback_observed_at < self.config.stop_due_utc
            ):
                # The SDK writes the raw record before dispatching this callback.
                # Freeze the prefix at the preceding callback so an orchestration
                # thread that wakes after the deadline cannot admit future-local
                # information into the close-minus-15 snapshot.
                self.analysis_deadline_sequence = self.enqueued_sequence
                self.analysis_deadline_bytes = raw_end_bytes - record_bytes
                if self.analysis_pause_target is None:
                    self.analysis_pause_target = self.analysis_deadline_sequence
            try:
                self.record_queue.put_nowait((next_sequence, raw_end_bytes, record))
            except queue.Full:
                self.aggregator.callback_drop()
                self.sequence_condition.notify_all()
                return
            self.enqueued_sequence = next_sequence
            self.sequence_condition.notify_all()

    def _expected_trade_roots(self) -> set[str]:
        roots: set[str] = set()
        for subscription in self.feed.subscriptions:
            if subscription.schema not in {"tcbbo", "tbbo", "trades"}:
                continue
            for symbol in subscription.symbols:
                parent = str(symbol).upper().split(".", 1)[0]
                roots.add(FAMILY_ROOTS.get(parent, parent))
        return roots

    def _status_snapshot(self) -> dict[str, object]:
        values = self.aggregator.status()
        values.update(
            expected_subscription_acks=len(self.feed.subscriptions),
            callback_queue_depth=self.record_queue.qsize(),
        )
        return values

    def _fatal_stream_exception(self, exc: Exception) -> None:
        reason = f"raw DBN stream write failed: {type(exc).__name__}: {str(exc)[:400]}"
        self.failure = self.failure or reason
        self.aggregator.exception(exc)
        self.stop(reason)

    def wait_for_analysis_ready(self, timeout_seconds: float = 30.0) -> bool:
        """Wait briefly for replay and the off-loop aggregate queue to catch up."""
        deadline = time.monotonic() + max(0.0, timeout_seconds)
        expected_acks = len(self.feed.subscriptions)
        expected_roots = self._expected_trade_roots()
        last_reasons: list[str] = []
        while time.monotonic() <= deadline:
            status = self._status_snapshot()
            now_ns = time.time_ns()
            watermarks = json.loads(str(status.get("root_trade_watermarks_json") or "{}"))
            last_trade_ns = int(status.get("last_trade_event_ns") or 0)
            last_reasons = []
            if int(status.get("subscription_acks") or 0) < expected_acks:
                last_reasons.append("subscription acknowledgements are incomplete")
            if int(status.get("replay_completed") or 0) < expected_acks:
                last_reasons.append("historical replay has not completed for every schema")
            if int(status.get("provider_error_count") or 0) > 0:
                last_reasons.append("provider emitted an ErrorMsg during capture")
            if last_trade_ns <= 0 or (now_ns - last_trade_ns) / 1e9 > 90:
                last_reasons.append("latest OPRA trade is more than 90 seconds old")
            for root in expected_roots:
                watermark = int(watermarks.get(root) or 0)
                allowed_age = 900 if root == "VIX" else 300
                if watermark <= 0 or (now_ns - watermark) / 1e9 > allowed_age:
                    last_reasons.append(f"{root} trade watermark is more than {allowed_age} seconds old")
            if not last_reasons:
                self.aggregator.flush()
                values = self._status_snapshot()
                values.update(status="running", file_bytes=self.path.stat().st_size if self.path.exists() else 0)
                self.catalog.update_feed_status(self.config.session_id, self.feed.name, values)
                return True
            time.sleep(0.25)
        reason = "close-minus-15 input did not catch up: " + "; ".join(last_reasons)
        self.aggregator.mark_incomplete(reason)
        values = self._status_snapshot()
        values.update(status="running", file_bytes=self.path.stat().st_size if self.path.exists() else 0)
        self.catalog.update_feed_status(self.config.session_id, self.feed.name, values)
        return False

    def capture_analysis_cutoff(
        self,
        *,
        record_sequence: int,
        cutoff_bytes: int,
    ) -> dict[str, object]:
        with self.sequence_condition:
            processed_sequence = self.processed_sequence
            barrier_complete = (
                self.analysis_pause_target == record_sequence
                and self.worker_paused
                and processed_sequence == record_sequence
            )
        if not barrier_complete:
            raise RuntimeError(
                "analysis cutoff requested without an exact processed-sequence barrier "
                f"({processed_sequence}/{record_sequence})"
            )
        if self.path.stat().st_size < cutoff_bytes:
            raise OSError("raw DBN file is shorter than the frozen callback sequence cutoff")
        status = self._status_snapshot()
        last_trade_event_ns = int(status.get("last_trade_event_ns") or 0)
        event_cutoff_ns = int(self.config.analysis_due_utc.timestamp() * 1_000_000_000)
        if last_trade_event_ns > event_cutoff_ns:
            raise RuntimeError(
                "analysis prefix contains a post-horizon trade event "
                f"({last_trade_event_ns}>{event_cutoff_ns})"
            )
        prefix = sha256_prefix(self.path, cutoff_bytes)
        captured = utcnow()
        status.update(status="running", file_bytes=self.path.stat().st_size)
        self.catalog.update_feed_status(self.config.session_id, self.feed.name, status)
        self.catalog.record_analysis_cutoff(
            session_id=self.config.session_id,
            feed_name=self.feed.name,
            horizon_id=ANALYSIS_HORIZON_ID,
            captured_at_utc=captured.isoformat(),
            event_cutoff_utc=self.config.analysis_due_utc.isoformat(),
            cutoff_bytes=cutoff_bytes,
            record_sequence=record_sequence,
            processed_sequence=processed_sequence,
            prefix_sha256=prefix,
            last_trade_event_ns=last_trade_event_ns or None,
        )
        return {
            "feed_name": self.feed.name,
            "captured_at_utc": captured.isoformat(),
            "event_cutoff_utc": self.config.analysis_due_utc.isoformat(),
            "cutoff_bytes": cutoff_bytes,
            "record_sequence": record_sequence,
            "processed_sequence": processed_sequence,
            "prefix_sha256": prefix,
        }

    def _aggregate_worker(self) -> None:
        while not self.worker_stop.is_set() or not self.record_queue.empty():
            with self.sequence_condition:
                while (
                    self.analysis_pause_target is not None
                    and self.processed_sequence >= self.analysis_pause_target
                    and not self.worker_stop.is_set()
                ):
                    self.worker_paused = True
                    self.sequence_condition.notify_all()
                    self.sequence_condition.wait(timeout=0.25)
                self.worker_paused = False
            try:
                sequence, raw_end_bytes, record = self.record_queue.get(timeout=0.25)
            except queue.Empty:
                continue
            with self.sequence_condition:
                while (
                    self.analysis_pause_target is not None
                    and sequence > self.analysis_pause_target
                    and not self.worker_stop.is_set()
                ):
                    self.worker_paused = True
                    self.sequence_condition.notify_all()
                    self.sequence_condition.wait(timeout=0.25)
                self.worker_paused = False
            try:
                self.aggregator(record)
            except Exception as exc:
                self.aggregator.exception(exc)
                self.aggregator.mark_incomplete(
                    f"derived record aggregation failed: {type(exc).__name__}: {exc}"
                )
            finally:
                with self.sequence_condition:
                    self.processed_sequence = max(self.processed_sequence, sequence)
                    self.sequence_condition.notify_all()
                self.record_queue.task_done()

    def freeze_analysis_snapshot(self, timeout_seconds: float = 30.0) -> tuple[int, int] | None:
        deadline = time.monotonic() + max(0.0, timeout_seconds)
        with self.sequence_condition:
            if self.analysis_deadline_sequence is not None:
                target_sequence = self.analysis_deadline_sequence
                cutoff_bytes = int(self.analysis_deadline_bytes or 0)
                if self.analysis_pause_target not in {None, target_sequence}:
                    raise RuntimeError("analysis snapshot is already frozen at another sequence")
                self.analysis_pause_target = target_sequence
            else:
                if self.analysis_pause_target is not None:
                    raise RuntimeError("analysis snapshot is already frozen")
                target_sequence = self.enqueued_sequence
                metadata_bytes = (
                    int(self.stream_metadata_bytes)
                    if self.stream_metadata_bytes is not None
                    else (self.path.stat().st_size if self.path.exists() else 0)
                )
                cutoff_bytes = metadata_bytes + self.enqueued_record_bytes
                self.analysis_pause_target = target_sequence
            self.sequence_condition.notify_all()
            while not (
                self.worker_paused and self.processed_sequence >= target_sequence
            ):
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    self.analysis_pause_target = None
                    self.sequence_condition.notify_all()
                    self.aggregator.mark_incomplete(
                        "analysis sequence barrier did not drain before timeout"
                    )
                    return None
                self.sequence_condition.wait(timeout=min(0.25, remaining))
            return target_sequence, cutoff_bytes

    def release_analysis_snapshot(self) -> None:
        with self.sequence_condition:
            self.analysis_pause_target = None
            self.worker_paused = False
            self.sequence_condition.notify_all()

    def stop(self, reason: str | None = None) -> None:
        if reason:
            self.failure = reason
        if self.client is not None:
            try:
                self.client.stop()
            except Exception:
                LOGGER.debug("Clean stop failed for %s", self.feed.name, exc_info=True)

    def _cleanup_unclosed_live_client(self) -> None:
        """Stop this feed's exact client and wait on its bounded close barrier."""
        client = self.client
        if client is None:
            return
        cleanup_errors: list[str] = []
        try:
            client.stop()
        except Exception as exc:
            cleanup_errors.append(f"stop {type(exc).__name__}: {str(exc)[:300]}")
        try:
            client.block_for_close(timeout=TAPE_LIVE_CLIENT_CLOSE_TIMEOUT_SECONDS)
        except Exception as exc:
            cleanup_errors.append(
                f"close barrier {type(exc).__name__}: {str(exc)[:300]}"
            )
        if not cleanup_errors:
            return
        cleanup_failure = "live client cleanup failed: " + "; ".join(cleanup_errors)
        # Preserve the provider/subscribe/start exception that initiated
        # cleanup. It is the causal failure and must not be masked by a
        # secondary close-barrier problem.
        if self.failure is None:
            self.failure = cleanup_failure
        LOGGER.warning(
            "Tape feed %s cleanup issue after failure=%r: %s",
            self.feed.name,
            self.failure,
            cleanup_failure,
        )

    def _monitor(self) -> None:
        last_tick = time.monotonic()
        while not self.monitor_stop.wait(self.config.flush_seconds):
            try:
                current_tick = time.monotonic()
                pause_reason = monitor_pause_reason(
                    current_tick - last_tick,
                    self.config.flush_seconds,
                )
                if pause_reason:
                    self.aggregator.record_operational_warning(pause_reason)

                def persist_monitor_state() -> int:
                    self.aggregator.flush()
                    file_size = self.path.stat().st_size if self.path.exists() else 0
                    values = self._status_snapshot()
                    values.update(status="running", file_bytes=file_size)
                    self.catalog.update_feed_status(
                        self.config.session_id,
                        self.feed.name,
                        values,
                    )
                    return file_size

                file_bytes = int(retry_sqlite_busy(persist_monitor_state))
                try:
                    # Status publication is advisory and may open its own
                    # catalog reader. Request it only after monitor persistence
                    # has released its write transaction; it must never turn a
                    # healthy required feed incomplete.
                    self.status_callback()
                except Exception as exc:
                    LOGGER.warning(
                        "Tape status refresh request failed for %s: %s",
                        self.feed.name,
                        exc,
                    )
                free = shutil.disk_usage(self.config.output_dir).free
                if file_bytes > self.config.max_feed_file_bytes:
                    self.stop(f"file safety cap exceeded: {file_bytes}")
                elif free < self.config.min_free_bytes:
                    self.stop(f"disk safety floor breached: {free}")
            except Exception as exc:
                self.aggregator.exception(exc)
                self.aggregator.mark_incomplete(f"live aggregate persistence failed: {type(exc).__name__}: {exc}")
                self.failure = self.failure or f"live aggregate persistence failed: {type(exc).__name__}: {str(exc)[:400]}"
                LOGGER.exception("Tape monitor failed for %s", self.feed.name)
            finally:
                # Persistence time is not a scheduler pause. Measure the next
                # wait from the end of this tick so known SQLite contention
                # cannot manufacture a sleep/stall warning.
                last_tick = time.monotonic()

    def _parquet(self) -> None:
        if not self.parquet or not self.path.exists():
            return
        store = db.DBNStore.from_file(self.path)
        for subscription in self.feed.subscriptions:
            target = self.path.with_suffix(f".{subscription.schema}.parquet")
            if target.exists():
                continue
            try:
                store.to_parquet(target, schema=subscription.schema, mode="x")
            except Exception as exc:
                LOGGER.warning("Parquet conversion failed for %s/%s: %s", self.feed.name, subscription.schema, exc)

    def run(self) -> None:
        try:
            self._run_impl()
        except BaseException as exc:
            self.failure = self.failure or f"terminal feed failure: {type(exc).__name__}: {str(exc)[:500]}"
            self.aggregator.exception(exc if isinstance(exc, Exception) else RuntimeError(str(exc)))
            try:
                if self.client is not None:
                    self.client.stop()
            except Exception:
                pass
            try:
                self.catalog.register_feed(self.config.session_id, self.feed, self.path)
                self.catalog.update_feed_status(
                    self.config.session_id,
                    self.feed.name,
                    {
                        **self._status_snapshot(),
                        "status": "incomplete",
                        "ended_at_utc": utcnow().isoformat(),
                        "file_bytes": self.path.stat().st_size if self.path.exists() else 0,
                        "complete": 0,
                        "error": self.failure,
                    },
                )
            except Exception:
                LOGGER.exception("Could not persist terminal failure for %s", self.feed.name)
            try:
                self.status_callback()
            except Exception:
                pass
            LOGGER.exception("Unhandled terminal failure in tape feed %s", self.feed.name)

    def _run_impl(self) -> None:
        self.catalog.register_feed(self.config.session_id, self.feed, self.path)
        started = utcnow()
        self.catalog.update_feed_status(
            self.config.session_id,
            self.feed.name,
            {
                "status": "connecting",
                "started_at_utc": started.isoformat(),
                "expected_subscription_acks": len(self.feed.subscriptions),
            },
        )
        monitor = threading.Thread(target=self._monitor, name=f"{self.name}-monitor", daemon=True)
        worker = threading.Thread(target=self._aggregate_worker, name=f"{self.name}-aggregate", daemon=False)
        timer = None
        close_barrier_returned = False
        try:
            client = self.live_factory(
                heartbeat_interval_s=5,
                # This optional secondary must never reclaim a provider slot
                # after a disconnect while the primary gamma/ORB client owns
                # the session. A failed tape remains explicitly incomplete.
                reconnect_policy="none",
                slow_reader_behavior="warn",
                compression=db.Compression.ZSTD,
            )
            self.client = client
            for subscription in self.feed.subscriptions:
                client.subscribe(
                    dataset=self.feed.dataset,
                    schema=subscription.schema,
                    symbols=list(subscription.symbols),
                    stype_in=subscription.stype_in,
                    start=subscription.start_utc.isoformat(),
                )
            self.stream_handle = self.path.open("xb", buffering=0)
            client.add_stream(self.stream_handle, exception_callback=self._fatal_stream_exception)
            client.add_callback(self._enqueue_record, exception_callback=self.aggregator.exception)
            client.add_reconnect_callback(self.aggregator.reconnect_gap, exception_callback=self.aggregator.exception)
            worker.start()
            client.start()
            self.catalog.update_feed_status(
                self.config.session_id,
                self.feed.name,
                {
                    "status": "running",
                    "started_at_utc": started.isoformat(),
                    "expected_subscription_acks": len(self.feed.subscriptions),
                },
            )
            monitor.start()
            remaining = max(0.1, (self.config.stop_due_utc - utcnow()).total_seconds())
            timer = threading.Timer(remaining, self.stop)
            timer.daemon = True
            timer.start()
            client.block_for_close()
            close_barrier_returned = True
        except Exception as exc:
            self.failure = f"{type(exc).__name__}: {str(exc)[:500]}"
            self.aggregator.exception(exc)
            LOGGER.exception("Tape feed %s failed", self.feed.name)
        finally:
            if timer is not None:
                timer.cancel()
            # Subscribe/start failures can bypass the ordinary blocking close
            # call. Release the provider slot before hashing, aggregation drain,
            # or other potentially lengthy finalization work begins.
            if not close_barrier_returned:
                self._cleanup_unclosed_live_client()
            self.monitor_stop.set()
            if monitor.is_alive():
                monitor.join(timeout=max(5.0, self.config.flush_seconds * 2))
            self.worker_stop.set()
            self.release_analysis_snapshot()
            if worker.is_alive():
                worker.join(timeout=120.0)
            if worker.is_alive() or not self.record_queue.empty():
                remaining = self.record_queue.qsize()
                self.aggregator.callback_drop()
                self.failure = self.failure or f"derived aggregation queue did not drain ({remaining} records remain)"
            if self.stream_handle is not None and not self.stream_handle.closed:
                try:
                    self.stream_handle.flush()
                    self.stream_handle.close()
                except Exception as exc:
                    self.failure = self.failure or f"raw DBN stream close failed: {exc}"
            try:
                self.aggregator.flush()
            except Exception as exc:
                self.failure = self.failure or f"final aggregate flush failed: {exc}"
            status = self._status_snapshot()
            file_bytes = self.path.stat().st_size if self.path.exists() else 0
            complete = False
            sha256 = None
            if file_bytes:
                try:
                    expects_tcbbo = any(
                        subscription.schema == "tcbbo"
                        for subscription in self.feed.subscriptions
                    )
                    integrity = inspect_dbn(
                        self.path,
                        require_trades=self.feed.required,
                        require_tcbbo=expects_tcbbo,
                        expected_subscription_acks=len(self.feed.subscriptions),
                    )
                    derived_slow_warnings = int(status.get("slow_reader_warnings") or 0)
                    reconnect_count = int(status.get("reconnect_count") or 0)
                    provider_error_count = int(status.get("provider_error_count") or 0)
                    completion_issues: list[str] = []
                    expected_roots = self._expected_trade_roots()
                    root_counts = json.loads(str(status.get("root_trade_counts_json") or "{}"))
                    if int(status.get("subscription_acks") or 0) < len(self.feed.subscriptions):
                        completion_issues.append("not every subscription was acknowledged")
                    if int(status.get("replay_completed") or 0) < len(self.feed.subscriptions):
                        completion_issues.append("not every replayed schema reached replay-completed")
                    unmapped_trades = int(status.get("unmapped_trade_records") or 0)
                    derived_trades = int(status.get("trade_records") or 0)
                    if excessive_unmapped_trade_ratio(unmapped_trades, derived_trades):
                        completion_issues.append(
                            "unmapped trade ratio exceeds 0.5 percent "
                            f"({unmapped_trades}/{derived_trades})"
                        )
                    for subscription in self.feed.subscriptions:
                        if subscription.schema == "tcbbo":
                            completion_issues.extend(tcbbo_integrity_issues(integrity))
                            completion_issues.extend(
                                provisional_tcbbo_integrity_issues(status, integrity)
                            )
                        if subscription.schema == "statistics" and integrity.statistics_records <= 0:
                            completion_issues.append("statistics subscription produced no records")
                        if subscription.schema == "definition" and integrity.definition_records <= 0:
                            completion_issues.append("definition subscription produced no records")
                    for root in expected_roots:
                        if int(root_counts.get(root) or 0) <= 0:
                            completion_issues.append(f"{root} has no captured trade records")
                    if any(
                        subscription.schema == "statistics"
                        for subscription in self.feed.subscriptions
                    ):
                        from .oi_replay import replay_open_interest

                        oi_replay = replay_open_interest(
                            self.path,
                            catalog=self.catalog,
                            session_id=self.config.session_id,
                            feed_name=self.feed.name,
                            source_sha256=integrity.sha256,
                        )
                        if int(oi_replay["records"] or 0) <= 0:
                            completion_issues.append(
                                "statistics subscription produced no replayable open-interest observations"
                            )
                        if int(oi_replay["unmapped_records"] or 0) > 0:
                            completion_issues.append(
                                "open-interest replay contains unmapped instrument observations "
                                f"({oi_replay['unmapped_records']}/{oi_replay['records']})"
                            )
                        with self.catalog.connect(read_only=True) as connection:
                            oi_roots = {
                                str(row[0])
                                for row in connection.execute(
                                    """
                                    SELECT DISTINCT family_root
                                    FROM tape_open_interest
                                    WHERE session_id=? AND feed_name=?
                                      AND open_interest>=0 AND family_root IS NOT NULL
                                    """,
                                    (self.config.session_id, self.feed.name),
                                ).fetchall()
                            }
                        for root in expected_roots:
                            if root not in oi_roots:
                                completion_issues.append(
                                    f"{root} has no persisted daily open-interest observations"
                                )
                    final_trade_ns = int(status.get("last_trade_event_ns") or 0)
                    final_cutoff_ns = int((self.config.cash_close_utc - timedelta(minutes=5)).timestamp() * 1e9)
                    if utcnow() >= self.config.cash_close_utc and final_trade_ns < final_cutoff_ns:
                        completion_issues.append("trade tape did not advance to the cash-close window")
                    complete = (
                        integrity.local_file_intact
                        and not self.failure
                        and derived_slow_warnings == 0
                        and reconnect_count == 0
                        and provider_error_count == 0
                        and not completion_issues
                    )
                    sha256 = integrity.sha256
                    live_gaps = json.loads(str(status.get("gaps_json") or "[]"))
                    integrity_gaps = [
                        {"integrity_reason": reason} for reason in integrity.incomplete_reasons
                    ]
                    completion_gaps = [
                        {"completion_issue": reason} for reason in completion_issues
                    ]
                    status.update(
                        records_seen=integrity.records_seen,
                        trade_records=integrity.trade_records,
                        tcbbo_records=integrity.tcbbo_records,
                        tcbbo_timestamped_records=integrity.tcbbo_timestamped_records,
                        tcbbo_valid_nbbo_records=integrity.tcbbo_valid_nbbo_records,
                        tcbbo_flagged_records=integrity.tcbbo_flagged_records,
                        tcbbo_action_counts_json=json.dumps(
                            dict(integrity.tcbbo_action_counts), separators=(",", ":")
                        ),
                        mapping_records=integrity.mapping_records,
                        statistics_records=integrity.statistics_records,
                        definition_records=integrity.definition_records,
                        first_event_ns=integrity.first_event_ns,
                        last_event_ns=integrity.last_event_ns,
                        last_receive_ns=integrity.last_receive_ns,
                        slow_reader_warnings=derived_slow_warnings + integrity.slow_reader_warnings,
                        gaps_json=json.dumps(
                            [*live_gaps, *integrity_gaps, *completion_gaps],
                            separators=(",", ":"),
                        ),
                    )
                    if completion_issues and not self.failure:
                        self.failure = "; ".join(completion_issues)
                except Exception as exc:
                    self.failure = self.failure or f"DBN integrity check failed: {exc}"
            status.update(
                status="complete" if complete else "incomplete",
                ended_at_utc=utcnow().isoformat(),
                file_bytes=file_bytes,
                complete=int(complete),
                sha256=sha256,
                error=self.failure or status.get("error"),
            )
            self.catalog.update_feed_status(self.config.session_id, self.feed.name, status)
            if sha256:
                with self.catalog.connect() as connection:
                    connection.execute(
                        """
                        UPDATE tape_inferred_minute_flow SET source_sha256=?
                        WHERE session_id=? AND feed_name=?
                        """,
                        (sha256, self.config.session_id, self.feed.name),
                    )
                self.catalog.finalize_analysis_cutoff(
                    session_id=self.config.session_id,
                    feed_name=self.feed.name,
                    horizon_id=ANALYSIS_HORIZON_ID,
                    final_file_sha256=sha256,
                )
            self.status_callback()
            self._parquet()


class StreamingSessionRecorder:
    def __init__(
        self,
        config: SessionConfig,
        *,
        run_analysis: bool = True,
        parquet: bool = True,
        live_factory: Callable[..., object] = db.Live,
        process_priority: ProcessPriorityControl | None = None,
    ):
        self.config = config
        # Opening TapeCatalog performs schema migrations. Defer it until run()
        # owns the process lock so a duplicate launch cannot alter the active
        # recorder's database before being rejected.
        self.catalog: TapeCatalog | None = None
        self.run_analysis = run_analysis
        self.parquet = parquet
        self.live_factory = live_factory
        self.feeds: list[FeedThread] = []
        self.status_lock = threading.Lock()
        self.status_heartbeat_requested = threading.Event()
        self.shutdown = threading.Event()
        self.shutdown_reason: str | None = None
        self.analysis_error: str | None = None
        self.paper_shadow_result: dict[str, object] | None = None
        self.analysis_thread: threading.Thread | None = None
        self.sleep_guard = SystemSleepGuard()
        self.process_priority = process_priority or ProcessPriorityControl()

    def write_status(self) -> None:
        if self.catalog is None:
            raise RuntimeError("closing-tape catalog is not initialized")
        with self.status_lock:
            with self.catalog.connect(read_only=True) as connection:
                session = connection.execute(
                    "SELECT * FROM tape_sessions WHERE session_id=?", (self.config.session_id,)
                ).fetchone()
                feeds = connection.execute(
                    "SELECT * FROM tape_feed_status WHERE session_id=? ORDER BY feed_name",
                    (self.config.session_id,),
                ).fetchall()
            payload = {
                "observed_at_utc": utcnow().isoformat(),
                "pid": os.getpid(),
                "runtime": {
                    "python_executable": sys.executable,
                    "python_prefix": sys.prefix,
                    "python_base_prefix": sys.base_prefix,
                    "venv_active": sys.prefix != sys.base_prefix,
                },
                "process_priority": self.process_priority.to_dict(),
                "sleep_prevention": self.sleep_guard.to_dict(),
                "session": dict(session) if session else None,
                "feeds": [dict(row) for row in feeds],
                "analysis_error": self.analysis_error,
                "paper_shadow": self.paper_shadow_result,
            }
            # Use a writer-unique temporary name. A stale/overlapping process or
            # an external status reader must never contend for one shared tmp
            # path and terminate the immutable DBN capture on Windows.
            temporary = self.config.status_path.with_name(
                f"{self.config.status_path.name}.{os.getpid()}.{threading.get_ident()}.tmp"
            )
            temporary.parent.mkdir(parents=True, exist_ok=True)
            temporary.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
            try:
                for attempt in range(20):
                    try:
                        os.replace(temporary, self.config.status_path)
                        return
                    except PermissionError:
                        if attempt == 19:
                            LOGGER.warning(
                                "Could not replace status file after Windows reader-lock retries: %s",
                                self.config.status_path,
                            )
                            return
                        time.sleep(0.05)
            finally:
                temporary.unlink(missing_ok=True)

    def _write_status_heartbeat(self) -> bool:
        """Publish non-terminal status without letting transient contention stop capture."""
        # Clear before the attempt so a request arriving during I/O remains
        # pending. Persistent contention re-arms the request for the next
        # ordinary recorder-loop heartbeat.
        self.status_heartbeat_requested.clear()
        try:
            retry_sqlite_busy(self.write_status)
        except sqlite3.OperationalError as exc:
            message = str(exc).lower()
            if not any(token in message for token in ("locked", "busy")):
                raise
            self.status_heartbeat_requested.set()
            LOGGER.warning(
                "Closing-tape status heartbeat deferred after SQLite contention: %s",
                exc,
            )
            return False
        return True

    def _request_status_heartbeat(self) -> None:
        """Schedule status publication without doing catalog I/O on a feed thread."""
        self.status_heartbeat_requested.set()

    def stop(self, reason: str | None = None) -> None:
        normalized_reason = reason or "operator shutdown requested"
        self.shutdown_reason = self.shutdown_reason or normalized_reason
        self.shutdown.set()
        for feed in self.feeds:
            feed.stop(normalized_reason)

    def _stop_feeds_at_absolute_deadline(self, feeds: list[FeedThread]) -> bool:
        """Enforce the UTC deadline even when a relative timer paused during sleep."""
        if utcnow() < self.config.stop_due_utc:
            return False
        for feed in feeds:
            if feed.is_alive():
                feed.stop()
        return True

    def _run_paper_shadow(self, feed: FeedThread, cutoff: dict[str, object]) -> None:
        """Run optional paper evidence without endangering canonical analysis."""
        try:
            from .live_shadow import run_live_prefix_paper_shadow

            shadow = run_live_prefix_paper_shadow(
                project_root=self.config.project_root,
                source_path=feed.path,
                cutoff_bytes=int(cutoff["cutoff_bytes"]),
                prefix_sha256=str(cutoff["prefix_sha256"]),
                catalog_path=self.config.catalog_path,
                market_db_path=self.config.market_db_path,
                session_id=self.config.session_id,
                feed_name=feed.feed.name,
                trading_day=self.config.trading_date,
                cash_open_utc=self.config.cash_open_utc,
                cash_close_utc=self.config.cash_close_utc,
                feature_available_at_utc=self.config.analysis_due_utc,
            )
            self.paper_shadow_result = shadow.to_dict()
        except Exception as exc:
            self.paper_shadow_result = {
                "configured": True,
                "recorded": 0,
                "reasons": [f"{type(exc).__name__}: {str(exc)[:500]}"],
            }
            LOGGER.exception("Paper shadow failed")

    def _analysis(self) -> None:
        frozen_feeds: list[FeedThread] = []
        captured_cutoffs: list[tuple[FeedThread, dict[str, object]]] = []
        try:
            if utcnow() > self.config.analysis_due_utc + timedelta(minutes=2):
                for feed in self.feeds:
                    if feed.feed.required:
                        feed.aggregator.mark_incomplete(
                            "close-minus-15 analysis horizon was missed by more than two minutes"
                        )
            for feed in self.feeds:
                if feed.feed.required:
                    feed.wait_for_analysis_ready(timeout_seconds=30.0)
                else:
                    feed.aggregator.flush()
            for feed in self.feeds:
                barrier = feed.freeze_analysis_snapshot(timeout_seconds=30.0)
                if barrier is None:
                    continue
                frozen_feeds.append(feed)
                record_sequence, cutoff_bytes = barrier
                feed.aggregator.flush()
                cutoff = feed.capture_analysis_cutoff(
                    record_sequence=record_sequence,
                    cutoff_bytes=cutoff_bytes,
                )
                captured_cutoffs.append((feed, cutoff))
            # The cutoff ledger and raw prefix are now immutable. Resume the
            # aggregation worker before any report/replay work so live callback
            # records cannot accumulate behind analysis.
            for feed in frozen_feeds:
                feed.release_analysis_snapshot()
            frozen_feeds.clear()
            from backend.closing_analysis import run_closing_analysis

            run_closing_analysis(
                tape_catalog_path=self.config.catalog_path,
                market_db_path=self.config.market_db_path,
                output_root=self.config.project_root / "exports" / "decision_support",
                session_id=self.config.session_id,
                trading_day=self.config.trading_date,
                asof_utc=self.config.analysis_due_utc,
            )
            required = next(
                ((feed, cutoff) for feed, cutoff in captured_cutoffs if feed.feed.required),
                None,
            )
            if required is not None:
                feed, cutoff = required
                self._run_paper_shadow(feed, cutoff)
        except Exception as exc:
            self.analysis_error = f"{type(exc).__name__}: {str(exc)[:500]}"
            LOGGER.exception("Close-minus-15 analysis failed")
        finally:
            for feed in frozen_feeds:
                feed.release_analysis_snapshot()
            try:
                self.write_status()
            except Exception:
                LOGGER.exception("Could not publish analysis status")

    def run(self) -> int:
        self.config.output_dir.mkdir(parents=True, exist_ok=True)
        with ProcessFileLock(self.config.lock_path), self.sleep_guard:
            self.catalog = TapeCatalog(self.config.catalog_path)
            orphaned = self.catalog.abandon_running_sessions(
                recovery_session_id=self.config.session_id
            )
            if orphaned:
                LOGGER.warning(
                    "Marked orphaned recorder sessions incomplete after exclusive-lock recovery: %s",
                    ",".join(orphaned),
                )
            self.catalog.start_session(self.config)
            analysis_started = False
            parent_failure: str | None = None
            started_feeds: list[FeedThread] = []
            try:
                self.feeds = [
                    FeedThread(
                        self.config,
                        feed,
                        self.catalog,
                        self._request_status_heartbeat,
                        live_factory=self.live_factory,
                        parquet=self.parquet,
                    )
                    for feed in self.config.feeds
                ]
                for feed in self.feeds:
                    if self.shutdown.is_set():
                        break
                    feed.start()
                    started_feeds.append(feed)
                self._write_status_heartbeat()
                while any(feed.is_alive() for feed in started_feeds):
                    self._stop_feeds_at_absolute_deadline(started_feeds)
                    if self.shutdown.wait(2.0):
                        break
                    if self.run_analysis and not analysis_started and utcnow() >= self.config.analysis_due_utc:
                        analysis_started = True
                        self.analysis_thread = threading.Thread(
                            target=self._analysis,
                            name="closing-analysis",
                            daemon=False,
                        )
                        self.analysis_thread.start()
                    self._write_status_heartbeat()
            except BaseException as exc:
                parent_failure = f"recorder orchestration failed: {type(exc).__name__}: {str(exc)[:500]}"
                LOGGER.exception("Closing-tape session orchestration failed")
                self.stop(parent_failure)
            finally:
                if self.shutdown.is_set() or parent_failure:
                    stop_reason = parent_failure or self.shutdown_reason or "operator shutdown requested"
                    for feed in started_feeds:
                        if feed.is_alive():
                            feed.stop(stop_reason)
                for feed in started_feeds:
                    # Keep the process lock until hashing/finalization is truly
                    # finished; otherwise a second recorder could overlap.
                    feed.join()
                if self.analysis_thread is not None:
                    self.analysis_thread.join()

            failures = [parent_failure] if parent_failure else []
            failures.extend(
                feed.failure for feed in self.feeds if feed.feed.required and feed.failure
            )
            with self.catalog.connect(read_only=True) as connection:
                final_rows = {
                    str(row["feed_name"]): row
                    for row in connection.execute(
                        "SELECT * FROM tape_feed_status WHERE session_id=?",
                        (self.config.session_id,),
                    )
                }
            for feed in self.config.feeds:
                if not feed.required:
                    continue
                row = final_rows.get(feed.name)
                if row is None:
                    failures.append(f"required feed {feed.name} has no terminal status")
                elif int(row["complete"] or 0) != 1 or str(row["status"]) != "complete":
                    failures.append(f"required feed {feed.name} failed completeness gates")
            if self.run_analysis and not analysis_started:
                failures.append("close-minus-15 analysis did not run")
            if self.run_analysis and self.analysis_error:
                failures.append(f"close-minus-15 analysis failed: {self.analysis_error}")
            failures = list(dict.fromkeys(str(item) for item in failures if item))
            self.catalog.finish_session(
                self.config.session_id,
                "complete" if not failures else "incomplete",
                "; ".join(failures) if failures else None,
            )
            try:
                self.write_status()
            except Exception:
                LOGGER.exception("Could not publish final tape status")
                return 2
            return 0 if not failures else 2


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description="Capture an immutable MarketPin OPRA closing tape")
    result.add_argument("--project-root", default=str(Path(__file__).resolve().parents[2]))
    result.add_argument("--trading-date")
    result.add_argument("--session-id")
    result.add_argument("--include-equities", action="store_true")
    result.add_argument("--no-analysis", action="store_true")
    result.add_argument("--no-parquet", action="store_true")
    result.add_argument("--flush-seconds", type=float)
    result.add_argument("--stop-after-seconds", type=float)
    result.add_argument("--log-level", default="INFO")
    return result


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    logging.basicConfig(
        level=getattr(logging, str(args.log_level).upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    if os.getenv("DATABENTO_DEBUG_LOGS", "0") != "1":
        # The SDK emits one INFO line per replayed symbol mapping. Full OPRA
        # parent subscriptions can contain hundreds of thousands of mappings;
        # logging them adds disk I/O to the capture path without health value.
        logging.getLogger("databento").setLevel(logging.WARNING)
    root = Path(args.project_root).resolve()
    load_dotenv(root / ".env")
    if not os.getenv("DATABENTO_API_KEY"):
        raise SystemExit("DATABENTO_API_KEY is not configured")
    process_priority = ProcessPriorityControl()
    if not process_priority.normalize() and process_priority.error:
        LOGGER.warning(
            "Could not normalize recorder process priority: %s",
            process_priority.error,
        )
    now = utcnow()
    stop = now + timedelta(seconds=max(args.stop_after_seconds, 1.0)) if args.stop_after_seconds else None
    config = build_session_config(
        root,
        trading_day=date.fromisoformat(args.trading_date) if args.trading_date else None,
        now=now,
        include_equities=bool(args.include_equities),
        session_id=args.session_id,
        flush_seconds=args.flush_seconds,
        stop_due_utc=stop,
    )
    recorder = StreamingSessionRecorder(
        config,
        run_analysis=not args.no_analysis,
        parquet=not args.no_parquet,
        process_priority=process_priority,
    )

    def shutdown(_signum, _frame):
        recorder.stop("signal shutdown requested")

    for signum in (signal.SIGINT, signal.SIGTERM):
        try:
            signal.signal(signum, shutdown)
        except (ValueError, OSError):
            pass
    return recorder.run()


if __name__ == "__main__":
    raise SystemExit(main())
