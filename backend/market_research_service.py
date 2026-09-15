"""Independent, bounded research capture alongside the production feed.

The production SQLite database is opened query-only. New candidate forecasts
are journaled in a separate database and never become production passports.
"""
from __future__ import annotations

import copy
from contextlib import closing
from datetime import datetime, timezone
import hashlib
import json
import logging
from pathlib import Path
import sqlite3
import threading
import urllib.request

from backend.forecast_repository import load_research_history, utc

log = logging.getLogger(__name__)
SCHEMA = "marketpin-market-research.v1"
DEFAULT_SYMBOLS = ("SPX", "NDX", "RUT", "DJI", "SPY", "QQQ", "DIA", "IWM", "VIX")


def read_runtime_context():
    with urllib.request.urlopen("http://127.0.0.1:8000/health/live", timeout=2.0) as response:
        payload = json.load(response)
    sampled = utc(payload.get("sampled_at_utc"))
    age = (datetime.now(timezone.utc) - sampled).total_seconds()
    if not 0 <= age <= 15:
        raise ValueError("RUNTIME_HEALTH_SAMPLE_STALE")
    return payload


class MarketResearchService:
    def __init__(self, source_database, journal_database, *, context_reader=read_runtime_context,
                 symbols=DEFAULT_SYMBOLS, interval_seconds=60.0, equity_tracker=None):
        self.source_database = Path(source_database).resolve()
        self.journal_database = Path(journal_database).resolve()
        if self.source_database == self.journal_database:
            raise ValueError("Research journal must be separate from source database")
        self.context_reader = context_reader
        self.symbols = tuple(dict.fromkeys(symbols))
        if not 1 <= len(self.symbols) <= 40:
            raise ValueError("Research symbol count must be between 1 and 40")
        self.interval_seconds = max(60.0, float(interval_seconds))
        self.equity_tracker = equity_tracker
        self.code_fingerprint = hashlib.sha256(b"".join(
            (Path(__file__).parent / name).read_bytes()
            for name in ("market_research_service.py", "forecast_repository.py", "forecast_horizons.py",
                         "forecast_calendar.py", "directional_shift.py", "equity_context.py", "market_universe.py")
        )).hexdigest()
        self._stop = threading.Event()
        self._lock = threading.Lock()
        self._thread = None
        self._state = {"schema_version": SCHEMA, "status": "WARMING", "symbols": {}, "reasons": []}

    def initialize_journal(self):
        self.journal_database.parent.mkdir(parents=True, exist_ok=True)
        with closing(sqlite3.connect(self.journal_database, timeout=0.5)) as conn:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("CREATE TABLE IF NOT EXISTS research_forecasts ("
                         "forecast_id TEXT PRIMARY KEY, symbol TEXT NOT NULL, "
                         "as_of_utc TEXT NOT NULL, horizon_sessions INTEGER NOT NULL, "
                         "target_session_date TEXT, payload_json TEXT NOT NULL)")
            conn.execute("CREATE INDEX IF NOT EXISTS research_forecasts_symbol_time "
                         "ON research_forecasts(symbol, as_of_utc)")
            conn.commit()

    def _persist(self, state):
        rows = []
        for symbol, item in state["symbols"].items():
            for horizon, forecast in item["forecasts"].items():
                if forecast.get("status") != "RESEARCH_ONLY":
                    continue
                payload = {"forecast": forecast, "directional_shift": item["directional_shift"],
                           "evidence_sha256": item["evidence_sha256"],
                           "captured_at_utc": state["as_of_utc"]}
                canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"), allow_nan=False)
                key = hashlib.sha256(canonical.encode()).hexdigest()
                rows.append((key, symbol, state["as_of_utc"], int(horizon),
                             forecast.get("target_session_date"), canonical))
        if rows:
            with closing(sqlite3.connect(self.journal_database, timeout=0.5)) as conn:
                conn.executemany("INSERT OR IGNORE INTO research_forecasts VALUES (?,?,?,?,?,?)", rows)
                conn.commit()

    def collect_once(self, *, now=None):
        from backend.forecast_horizons import evaluate_forecast
        from backend.directional_shift import evaluate_directional_shift

        as_of = utc(now or datetime.now(timezone.utc))
        reasons = []
        try:
            context = self.context_reader()
        except Exception as exc:
            context = {}
            reasons.append("RUNTIME_CONTEXT_UNAVAILABLE:" + type(exc).__name__)
        try:
            histories, diagnostics = load_research_history(
                self.source_database, self.symbols, as_of_utc=as_of, runtime_context=context)
            reasons.extend(diagnostics)
            context_after = self.context_reader() if context else {}
            identity_keys = ("subscription_epoch_id", "subscription_generation", "handoff_status")
            context_invalid = (
                any(context.get(k) != context_after.get(k) for k in identity_keys)
                or context.get("runtime_context_stable") is not True
                or context_after.get("runtime_context_stable") is not True
                or context_after.get("handoff_status") != "active"
                or not context_after.get("subscription_epoch_id")
            )
            if context_invalid:
                for rows in histories.values():
                    for row in rows:
                        if row.get("kind") != "daily_close":
                            row.update(valid=False, validation_status="runtime_unverified")
                reasons.append("RUNTIME_CONTEXT_CHANGED_OR_UNVERIFIED")
            else:
                # Health can turn ineligible without reconnecting. Both ends
                # of the bounded history read must approve each exact symbol.
                for symbol, rows in histories.items():
                    before_status = (context.get("symbol_status") or {}).get(symbol, {})
                    after_status = (context_after.get("symbol_status") or {}).get(symbol, {})
                    if before_status.get("usable_for_prediction") is not True or after_status.get("usable_for_prediction") is not True:
                        affected = [row for row in rows if row.get("kind") != "daily_close"]
                        for row in affected:
                            row.update(valid=False, validation_status="runtime_unverified")
                        if affected:
                            reasons.append(f"{symbol}:LIVE_SYMBOL_EVIDENCE_UNVERIFIED")
        except (sqlite3.Error, OSError, ValueError) as exc:
            histories = {symbol: [] for symbol in self.symbols}
            reasons.append("RETAINED_EVIDENCE_UNAVAILABLE:" + type(exc).__name__)
        equity_status = {"status": "DISABLED", "reason": "EQUITY_CONTEXT_NOT_ENABLED"}
        if self.equity_tracker is not None:
            equity_status = self.equity_tracker.snapshot(now=as_of)
            # The equity collector supplies independent observed ETF bars. Do
            # not substitute an ETF record into its related cash index history.
            for symbol in self.symbols:
                coverage = (equity_status.get("symbols") or {}).get(symbol, {})
                if not (
                    equity_status.get("enabled") is True
                    and coverage.get("status") == "OBSERVED"
                    and coverage.get("fresh") is True
                    and not coverage.get("failure_reason")
                ):
                    # Retained stale/disabled equity context must not displace
                    # an independently valid option-implied ETF reference.
                    continue
                equity_rows = self.equity_tracker.forecast_history(symbol, limit=500)
                equity_rows = [row for row in equity_rows if (
                    row.get("symbol") == symbol and row.get("valid") is True
                    and row.get("source_verified") is True
                    and utc(row["timestamp_utc"]) <= utc(row["available_at_utc"]) <= as_of
                )]
                if equity_rows:
                    histories[symbol] = [r for r in histories[symbol] if r.get("kind") == "daily_close"] + equity_rows
        intraday = {symbol: [row for row in rows if row.get("kind") != "daily_close"]
                    for symbol, rows in histories.items()}
        state = {"schema_version": SCHEMA, "status": "RESEARCH_ONLY", "decision_grade": False,
                 "as_of_utc": as_of.isoformat(), "reasons": reasons, "symbols": {},
                 "equity_context": equity_status,
                 "runtime_context": {k: context.get(k) for k in
                    ("subscription_epoch_id", "subscription_generation", "handoff_status")},
                 "source_database_access": "query_only", "capture_interval_seconds": self.interval_seconds,
                 "source_database": str(self.source_database),
                 "loaded_source_sha256": self.code_fingerprint,
                 "accuracy_claim": "Unvalidated candidates; improvement requires matured out-of-sample outcomes."}
        for symbol, rows in histories.items():
            current = intraday[symbol][-1] if intraday[symbol] else None
            forecasts = {str(horizon): evaluate_forecast(symbol, rows, as_of_utc=as_of, horizon_sessions=horizon,
                                                        current_snapshot=current)
                         for horizon in range(11)}
            direction = evaluate_directional_shift(symbol, intraday[symbol], as_of_utc=as_of,
                                                  cross_market_history=intraday)
            state["symbols"][symbol] = {"forecasts": forecasts, "directional_shift": direction,
                "retained_intraday_rows": len(intraday[symbol]),
                "verified_daily_rows": sum(r.get("kind") == "daily_close" for r in rows),
                "evidence_sha256": hashlib.sha256(json.dumps(rows, sort_keys=True, default=str).encode()).hexdigest()}
        self._persist(state)
        with self._lock:
            self._state = state
        return copy.deepcopy(state)

    def snapshot(self):
        with self._lock:
            state = copy.deepcopy(self._state)
        now = datetime.now(timezone.utc)
        state["read_at_utc"] = now.isoformat()
        state["capture_age_seconds"] = ((now - utc(state["as_of_utc"])).total_seconds()
                                         if state.get("as_of_utc") else None)
        # These are explicitly timestamped retained research observations. A
        # stopped collector must not keep presenting eligible numerical targets.
        if state["capture_age_seconds"] is not None and not 0 <= state["capture_age_seconds"] <= 120:
            future_capture = state["capture_age_seconds"] < 0
            reason = "RESEARCH_CAPTURE_TIMESTAMP_IN_FUTURE" if future_capture else "RESEARCH_CAPTURE_STALE"
            state["status"] = "UNAVAILABLE" if future_capture else "STALE"
            state["reasons"].append(reason)
            for item in state["symbols"].values():
                for forecast in item["forecasts"].values():
                    forecast.update(status="ABSTAIN", predicted_close=None)
                    forecast["candidates"] = []
                    forecast.setdefault("reasons", []).append(reason)
                item["directional_shift"].update(status="ABSTAIN", direction=None)
                item["directional_shift"].setdefault("reason_codes", []).append(reason)
        return state

    def _run(self):
        while not self._stop.is_set():
            try:
                self.collect_once()
            except Exception as exc:
                log.exception("Research capture failed")
                with self._lock:
                    self._state = {"schema_version": SCHEMA, "status": "UNAVAILABLE", "symbols": {},
                                   "reasons": ["RESEARCH_CAPTURE_FAILED:" + type(exc).__name__]}
            self._stop.wait(self.interval_seconds)

    def start(self):
        if self._thread and self._thread.is_alive():
            return
        self.initialize_journal()
        if self.equity_tracker is not None:
            self.equity_tracker.start()
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, name="market-research", daemon=True)
        self._thread.start()

    def stop(self):
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=5)
        if self.equity_tracker is not None:
            self.equity_tracker.stop()
