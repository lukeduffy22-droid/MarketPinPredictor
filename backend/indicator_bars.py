"""Bounded read-only access to retained, completed ETF minute bars.

No provider clients, collection, database creation, or index price conversion.
The existing equity collector remains the sole writer.
"""
from __future__ import annotations

from contextlib import closing
from datetime import datetime, timedelta, timezone
import hashlib
import json
import math
from pathlib import Path
import re
import sqlite3
import time

from backend.forecast_calendar import ET, session_bounds
from backend.market_universe import EQUITY_CONTEXT_SYMBOLS

SCHEMA = "marketpin-indicator-bars.v1"
MIN_BARS = 35  # MACD 26 + signal 9 warm-up, also covers RSI 14 and SMA 20.
MAX_AGE_SECONDS = 180
ETF_CONTEXT = {"SPX": "SPY", "NDX": "QQQ", "RUT": "IWM", "DJI": "DIA"}
FIELDS = ("open", "high", "low", "close", "volume")


def utc(value):
    stamp = value if isinstance(value, datetime) else datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if stamp.tzinfo is None:
        raise ValueError("BAR_TIMESTAMP_INVALID")
    return stamp.astimezone(timezone.utc)


def bar_symbol(symbol):
    symbol = str(symbol).upper()
    target = ETF_CONTEXT.get(symbol, symbol)
    return target if target in EQUITY_CONTEXT_SYMBOLS else None


def unavailable(symbol, reason, *, now=None):
    return {"schema_version": SCHEMA, "status": "unavailable", "reason_code": reason,
            "requested_symbol": symbol, "bar_symbol": bar_symbol(symbol),
            "as_of_utc": utc(now or datetime.now(timezone.utc)).isoformat(), "bars": []}


def validate_bars(symbol, rows, *, now):
    """Verify exact instrument, source, availability, session and full continuity."""
    now = utc(now)
    target = bar_symbol(symbol)
    if not target:
        raise ValueError("OBSERVED_BAR_SOURCE_UNSUPPORTED")
    opened, closed = session_bounds(now.astimezone(ET).date())
    if not opened <= now < closed:
        raise ValueError("OUTSIDE_CASH_SESSION")
    if not isinstance(rows, list) or not rows:
        raise ValueError("OBSERVED_INTRADAY_BARS_UNAVAILABLE")
    if len(rows) > 500:
        raise ValueError("BAR_READ_LIMIT_EXCEEDED")
    stamps = []
    for row in rows:
        if not isinstance(row, dict) or row.get("symbol") != target:
            raise ValueError("BAR_INSTRUMENT_MISMATCH")
        if (row.get("dataset") != "EQUS.MINI" or row.get("schema") != "ohlcv-1m"
                or row.get("source") != "databento_eq_us_mini"
                or row.get("source_kind") != "observed_etf_bar"
                or row.get("consolidated_sip") is not False
                or row.get("decision_grade") is not False):
            raise ValueError("BAR_PROVENANCE_INVALID")
        if (not re.fullmatch(r"[0-9a-f]{64}", str(row.get("subscription_epoch_id", "")))
                or not re.fullmatch(r"[0-9a-f]{64}", str(row.get("universe_hash", "")))
                or type(row.get("subscription_generation")) is not int
                or row["subscription_generation"] <= 0):
            raise ValueError("BAR_PROVENANCE_INVALID")
        stamp, observed = utc(row.get("timestamp_utc")), utc(row.get("observed_at_utc"))
        if (stamp.second or stamp.microsecond or not opened <= stamp < closed
                or not stamp + timedelta(minutes=1) <= observed <= now):
            raise ValueError("BAR_INCOMPLETE_OR_WRONG_SESSION")
        if any(type(row.get(key)) not in (float, int) or not math.isfinite(row[key]) for key in FIELDS):
            raise ValueError("BAR_VALUES_INVALID")
        values = {key: float(row[key]) for key in FIELDS}
        if (min(values[k] for k in FIELDS[:-1]) <= 0 or values["volume"] < 0
                or values["low"] > min(values["open"], values["close"])
                or values["high"] < max(values["open"], values["close"])):
            raise ValueError("BAR_VALUES_INVALID")
        revision = hashlib.sha256(json.dumps([target, stamp.isoformat(), values], sort_keys=True).encode()).hexdigest()
        if revision != row.get("source_revision"):
            raise ValueError("BAR_REVISION_MISMATCH")
        stamps.append(stamp)
    if stamps != sorted(set(stamps)) or any(b - a != timedelta(minutes=1) for a, b in zip(stamps, stamps[1:])):
        raise ValueError("BAR_HISTORY_GAPPED_OR_DUPLICATED")
    if stamps[0] != opened:
        raise ValueError("BAR_SESSION_OPEN_MISSING")
    if (now - stamps[-1]).total_seconds() > MAX_AGE_SECONDS:
        raise ValueError("OBSERVED_BARS_STALE")
    if len(rows) < MIN_BARS:
        raise ValueError("INDICATOR_LOOKBACK_INSUFFICIENT")
    if sum(row["volume"] for row in rows) <= 0:
        raise ValueError("BAR_VOLUME_UNAVAILABLE")
    return rows


def read_indicator_bars(database, symbol, *, now=None):
    now = utc(now or datetime.now(timezone.utc))
    symbol = str(symbol).upper()
    target = bar_symbol(symbol)
    if not target:
        return unavailable(symbol, "OBSERVED_BAR_SOURCE_UNSUPPORTED", now=now)
    path = Path(database)
    if not path.is_file():
        return unavailable(symbol, "RETAINED_BAR_DATABASE_MISSING", now=now)
    try:
        opened, closed = session_bounds(now.astimezone(ET).date())
        with closing(sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True, timeout=0.25)) as conn:
            conn.execute("PRAGMA query_only=ON")
            deadline = time.monotonic() + 1.0
            conn.set_progress_handler(lambda: time.monotonic() > deadline, 1000)
            retained = conn.execute("""SELECT symbol, timestamp_utc, observed_at_utc, source_revision, payload_json
                FROM equity_context_bars WHERE symbol=? AND timestamp_utc>=? AND timestamp_utc<?
                ORDER BY timestamp_utc, observed_at_utc DESC, rowid DESC LIMIT 2001""",
                (target, opened.isoformat(), closed.isoformat())).fetchall()
        if len(retained) > 2000:
            raise ValueError("BAR_READ_LIMIT_EXCEEDED")
        selected = {}
        for instrument, stamp, observed, revision, raw in retained:
            row = json.loads(raw)
            if any(row.get(k) != v for k, v in (("symbol", instrument), ("timestamp_utc", stamp),
                                               ("observed_at_utc", observed), ("source_revision", revision))):
                raise ValueError("BAR_STORED_METADATA_MISMATCH")
            # A future-dated revision must not fall back silently to an older revision.
            selected.setdefault(stamp, row)
        rows = validate_bars(symbol, list(selected.values()), now=now)
    except sqlite3.Error:
        return unavailable(symbol, "RETAINED_BAR_READ_UNAVAILABLE", now=now)
    except (ValueError, TypeError, KeyError, OSError, OverflowError) as exc:
        code = str(exc) if str(exc).isupper() and len(str(exc)) < 80 else "BAR_EVIDENCE_INVALID"
        return unavailable(symbol, code, now=now)
    return {"schema_version": SCHEMA, "status": "available", "requested_symbol": symbol,
            "bar_symbol": target, "relation": "same_instrument" if target == symbol else "etf_context",
            "as_of_utc": now.isoformat(), "dataset": "EQUS.MINI", "bar_schema": "ohlcv-1m",
            "bars": rows}
