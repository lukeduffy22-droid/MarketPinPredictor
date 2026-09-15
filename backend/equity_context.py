"""Bounded, opt-in EQUS minute-bar research capture, independent of OPRA.

GET consumers only read retained state. A single background worker performs at
most one basket request per minute. Entitlement failure opens a 30-minute circuit
breaker. No ETF observation can be relabelled as a cash-index price or a SIP bar.
"""
from __future__ import annotations

import hashlib
import json
import math
import os
import sqlite3
import threading
import uuid
from collections import deque
from contextlib import closing
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Mapping
from zoneinfo import ZoneInfo

from backend.market_universe import resolve_equity_context_symbols

DATASET = "EQUS.MINI"
SCHEMA = "ohlcv-1m"
UTC = timezone.utc
ET = ZoneInfo("America/New_York")
FRESH_SECONDS = 180.0


def _utc(value) -> datetime:
    if hasattr(value, "to_pydatetime"):
        value = value.to_pydatetime()
    if not isinstance(value, datetime):
        value = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if value.tzinfo is None:
        raise ValueError("Timestamp must include timezone")
    return value.astimezone(UTC)


def _regular_minute(timestamp: datetime) -> bool:
    from app.utils.market_time import is_holiday
    from app.utils.time_et import close_time_et, open_time_et
    local = timestamp.astimezone(ET)
    day = local.date()
    return (
        day.weekday() < 5 and not is_holiday(day)
        and open_time_et(local).astimezone(UTC) <= timestamp
        < close_time_et(local).astimezone(UTC)
    )


class EquityContextTracker:
    def __init__(self, *, symbols, enabled: bool = False, db_path: Path | None = None,
                 fetcher: Callable[..., Any] | None = None, poll_seconds: float = 60.0,
                 max_bars_per_symbol: int = 20000):
        self.symbols = resolve_equity_context_symbols(symbols)
        self.enabled = bool(enabled)
        self.subscription_epoch_id = hashlib.sha256(uuid.uuid4().bytes).hexdigest()
        self.universe_hash = hashlib.sha256(
            json.dumps([DATASET, SCHEMA, sorted(self.symbols)]).encode()
        ).hexdigest()
        self.db_path = Path(db_path) if db_path is not None else None
        self.poll_seconds = max(60.0, float(poll_seconds))
        self._fetcher = fetcher or self._fetch
        self._bars = {symbol: deque(maxlen=max(2, min(20000, int(max_bars_per_symbol))))
                      for symbol in self.symbols}
        self._lock = threading.Lock()
        self._poll_lock = threading.Lock()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._last_attempt: datetime | None = None
        self._retry_after: datetime | None = None
        self._failure: str | None = None
        self._accepted = 0
        self._rejected = 0

    @staticmethod
    def _fetch(**kwargs):
        import databento as db
        from backend.underlying_validator import _available_end_from_error
        client = db.Historical(os.getenv("DATABENTO_API_KEY", ""))
        try:
            result = client.timeseries.get_range(**kwargs)
        except Exception as exc:
            available_end = _available_end_from_error(exc, str(kwargs["end"]))
            complete_end = _utc(available_end).replace(second=0, microsecond=0) if available_end else None
            if complete_end is None or complete_end <= _utc(kwargs["start"]):
                raise
            result = client.timeseries.get_range(**{**kwargs, "end": complete_end.isoformat()})
        frame = result.to_df()
        return [] if frame is None or frame.empty else frame.reset_index().to_dict("records")

    def start(self) -> bool:
        self.restore()
        if not self.enabled or not self.symbols:
            return False
        with self._lock:
            if self._thread is not None and self._thread.is_alive():
                return False
            self._stop.clear()
            self._thread = threading.Thread(target=self._run, daemon=True, name="equity-context")
            self._thread.start()
        return True

    def stop(self) -> None:
        self._stop.set()
        thread = self._thread
        if thread is not None:
            thread.join(timeout=1.0)

    def _run(self) -> None:
        while not self._stop.is_set():
            self.poll_once()
            self._stop.wait(self.poll_seconds)

    def restore(self) -> int:
        """Load retained observations read-only; never rewrite their availability."""
        if self.db_path is None or not self.db_path.is_file():
            return 0
        restored = 0
        try:
            with closing(sqlite3.connect(self.db_path.resolve().as_uri() + "?mode=ro", uri=True, timeout=1.0)) as conn:
                for symbol in self.symbols:
                    rows = conn.execute("""SELECT payload_json FROM (
                        SELECT payload_json, timestamp_utc,
                        ROW_NUMBER() OVER (PARTITION BY timestamp_utc ORDER BY observed_at_utc DESC, rowid DESC) AS revision_rank
                        FROM equity_context_bars WHERE symbol = ?)
                        WHERE revision_rank = 1 ORDER BY timestamp_utc DESC LIMIT ?""",
                        (symbol, self._bars[symbol].maxlen)).fetchall()
                    values = [json.loads(row[0]) for row in reversed(rows)]
                    with self._lock:
                        if not self._bars[symbol]:
                            self._bars[symbol].extend(values)
                            restored += len(values)
        except (sqlite3.Error, ValueError, OSError):
            with self._lock:
                self._failure = "EQUITY_CONTEXT_RESTORE_FAILED"
        return restored

    def poll_once(self, now: datetime | None = None) -> int:
        now = _utc(now or datetime.now(UTC))
        if not self.enabled or not self.symbols or not _regular_minute(now):
            return 0
        if not self._poll_lock.acquire(blocking=False):
            return 0
        try:
            with self._lock:
                if self._retry_after and now < self._retry_after:
                    return 0
                if self._last_attempt and (now - self._last_attempt).total_seconds() < self.poll_seconds:
                    return 0
                first_attempt = self._last_attempt is None
                self._last_attempt = now
            # Initial session catch-up is at most one regular session. Later
            # polls overlap three minutes to capture delayed completed bars.
            from app.utils.time_et import open_time_et
            session_open = open_time_et(now.astimezone(ET)).astimezone(UTC)
            start = session_open if first_attempt else max(session_open, now - timedelta(minutes=3))
            if start >= now.replace(second=0, microsecond=0):
                return 0
            rows = self._fetcher(dataset=DATASET, schema=SCHEMA, symbols=list(self.symbols),
                                 start=start.isoformat(), end=now.replace(second=0, microsecond=0).isoformat(),
                                 limit=500 * len(self.symbols))
            if self._stop.is_set():
                return 0
            accepted = self.ingest(rows, observed_at=now)
            with self._lock:
                self._failure = None
                self._retry_after = None
            return accepted
        except Exception as exc:
            text = str(exc).casefold()
            denied = any(token in text for token in ("403", "license_not_found", "unauthorized", "live data license"))
            # Provider error text may carry credentials or URLs; expose a
            # stable diagnosis only, with no provider exception serialization.
            with self._lock:
                self._failure = "ENTITLEMENT_UNAVAILABLE" if denied else "EQUITY_CONTEXT_FETCH_FAILED"
                self._retry_after = now + timedelta(seconds=1800 if denied else self.poll_seconds)
            return 0
        finally:
            self._poll_lock.release()

    def ingest(self, rows, *, observed_at: datetime | None = None) -> int:
        """Validate observed complete RTH bars, preserving observation time."""
        observed = _utc(observed_at or datetime.now(UTC))
        pending = []
        rejected = 0
        for row in rows:
            try:
                symbol = str(row.get("symbol", "")).upper()
                if symbol not in self._bars:
                    raise ValueError("Symbol outside requested basket")
                stamp = _utc(row.get("ts_event", row.get("timestamp_utc")))
                if stamp.second or stamp.microsecond or stamp + timedelta(minutes=1) > observed:
                    raise ValueError("Incomplete or unaligned minute")
                if not _regular_minute(stamp):
                    raise ValueError("Outside regular session")
                values = {key: float(row[key]) for key in ("open", "high", "low", "close", "volume")}
                if not all(math.isfinite(value) for value in values.values()):
                    raise ValueError("Non-finite bar")
                if min(values[key] for key in ("open", "high", "low", "close")) <= 0 or values["volume"] < 0:
                    raise ValueError("Invalid bar value")
                if values["low"] > min(values["open"], values["close"]) or values["high"] < max(values["open"], values["close"]):
                    raise ValueError("Inconsistent OHLC")
                payload = {"symbol": symbol, "timestamp_utc": stamp.isoformat(), **values,
                           "dataset": DATASET, "schema": SCHEMA,
                           "source": "databento_eq_us_mini", "source_kind": "observed_etf_bar",
                           "consolidated_sip": False, "decision_grade": False,
                           "observed_at_utc": observed.isoformat(),
                           "subscription_epoch_id": self.subscription_epoch_id,
                           "subscription_generation": 1, "universe_hash": self.universe_hash}
                payload["source_revision"] = hashlib.sha256(json.dumps(
                    [symbol, stamp.isoformat(), values], sort_keys=True).encode()).hexdigest()
                pending.append(payload)
            except (ValueError, TypeError, KeyError, OverflowError):
                rejected += 1
        accepted = []
        with self._lock:
            for payload in sorted(pending, key=lambda item: item["timestamp_utc"]):
                bucket = self._bars[payload["symbol"]]
                matching = next((item for item in reversed(bucket) if item["timestamp_utc"] == payload["timestamp_utc"]), None)
                if matching and matching["source_revision"] == payload["source_revision"]:
                    continue
                retained = [item for item in bucket if item["timestamp_utc"] != payload["timestamp_utc"]]
                retained.append(payload)
                bucket.clear()
                bucket.extend(sorted(retained, key=lambda item: item["timestamp_utc"])[-bucket.maxlen:])
                accepted.append(payload)
            self._accepted += len(accepted)
            self._rejected += rejected
        if accepted and self.db_path is not None:
            self._persist(accepted)
        return len(accepted)

    def _persist(self, rows) -> None:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        with closing(sqlite3.connect(self.db_path, timeout=1.0)) as conn:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("""CREATE TABLE IF NOT EXISTS equity_context_bars (
                symbol TEXT NOT NULL, timestamp_utc TEXT NOT NULL,
                observed_at_utc TEXT NOT NULL, source_revision TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                PRIMARY KEY(symbol, timestamp_utc, observed_at_utc, source_revision))""")
            conn.executemany("INSERT OR IGNORE INTO equity_context_bars VALUES (?, ?, ?, ?, ?)",
                             [(r["symbol"], r["timestamp_utc"], r["observed_at_utc"], r["source_revision"],
                               json.dumps(r, sort_keys=True)) for r in rows])
            conn.commit()

    def history(self, symbol: str, limit: int = 20000) -> list[dict]:
        with self._lock:
            return [dict(row) for row in list(self._bars.get(str(symbol).upper(), ()))[-max(1, min(20000, int(limit))):]]

    def forecast_history(self, symbol: str, limit: int = 20000) -> list[dict]:
        """Adapt completed observed ETF bars without inventing price or VWAP."""
        return [{
            **row, "source_bar_start_utc": row["timestamp_utc"],
            "timestamp_utc": (_utc(row["timestamp_utc"]) + timedelta(minutes=1)).isoformat(),
            "source_timestamp_utc": (_utc(row["timestamp_utc"]) + timedelta(minutes=1)).isoformat(),
            "available_at_utc": row["observed_at_utc"],
            "price": row["close"], "spot": row["close"],
            "valid": True, "source_verified": True,
            "provider": "databento", "active_generation": row["subscription_generation"],
            "universe_sha256": row["universe_hash"],
            "volume_kind": "minute_bar_aggregate",
        } for row in self.history(symbol, limit)]

    def snapshot(self, now: datetime | None = None) -> dict:
        now = _utc(now or datetime.now(UTC))
        with self._lock:
            statuses = {}
            for symbol, rows in self._bars.items():
                latest = dict(rows[-1]) if rows else None
                age = (now - _utc(latest["timestamp_utc"])).total_seconds() if latest else None
                fresh = bool(age is not None and 60 <= age <= FRESH_SECONDS and _regular_minute(now))
                statuses[symbol] = {"status": "OBSERVED" if fresh else "STALE" if latest else "NOT_YET_OBSERVED",
                                    "fresh": fresh, "bar_count": len(rows), "age_seconds": age,
                                    "latest_bar": latest, "failure_reason": self._failure}
            return {"schema_version": "marketpin-equity-context.v1", "enabled": self.enabled,
                    "running": bool(self._thread and self._thread.is_alive()),
                    "dataset": DATASET, "schema": SCHEMA, "collection_mode": "historical_poll",
                    "observed_at_utc": now.isoformat(), "decision_grade": False,
                    "status": self._failure or (
                        "COLLECTING" if self.enabled and self._thread and self._thread.is_alive()
                        else "CONFIGURED" if self.enabled else "STAGED_DISABLED"
                    ),
                    "poll_seconds": self.poll_seconds, "max_symbols": 20,
                    "retry_after_utc": self._retry_after.isoformat() if self._retry_after else None,
                    "last_attempt_utc": self._last_attempt.isoformat() if self._last_attempt else None,
                    "accepted_bar_count": self._accepted, "rejected_bar_count": self._rejected,
                    "symbols": statuses}


_tracker: EquityContextTracker | None = None
_tracker_lock = threading.Lock()


def get_equity_context_tracker() -> EquityContextTracker:
    global _tracker
    with _tracker_lock:
        if _tracker is None:
            from backend.config import DATA_DIR, DATABENTO_EQUITY_CONTEXT_ENABLED, DATABENTO_EQUITY_CONTEXT_SYMBOLS
            _tracker = EquityContextTracker(symbols=DATABENTO_EQUITY_CONTEXT_SYMBOLS,
                                            enabled=DATABENTO_EQUITY_CONTEXT_ENABLED,
                                            db_path=DATA_DIR / "equity_context.db")
        return _tracker
