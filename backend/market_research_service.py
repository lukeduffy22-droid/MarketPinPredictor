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
import math
import os
from pathlib import Path
import sqlite3
import threading
import time
import urllib.request
import uuid

from backend.forecast_repository import load_research_history, utc

log = logging.getLogger(__name__)
SCHEMA = "marketpin-market-research.v1"
DEFAULT_SYMBOLS = ("SPX", "NDX", "RUT", "DJI", "SPY", "QQQ", "DIA", "IWM", "VIX")
HEALTH_SCHEMA = "marketpin-market-research-health.v1"


def source_identity(source_database, journal_database, comparison_journal=None):
    """Stable path identity, without publishing filesystem paths in health."""
    paths = [os.path.normcase(str(Path(p).resolve())).replace("\\", "/")
             for p in (source_database, journal_database)]
    if comparison_journal is not None:
        paths.append(os.path.normcase(str(Path(comparison_journal).resolve())).replace("\\", "/"))
    return hashlib.sha256("\n".join(paths).encode("utf-8")).hexdigest()


def research_code_fingerprint():
    root = Path(__file__).resolve().parents[1]
    return hashlib.sha256(b"".join((root / name).read_bytes() for name in (
        "backend/market_research_service.py", "backend/research_api.py",
        "backend/forecast_repository.py", "backend/forecast_horizons.py",
        "backend/forecast_calendar.py", "backend/directional_shift.py",
        "backend/market_universe.py", "tools/run_forecast_research.py",
        "backend/indicator_bars.py", "backend/equity_context.py", "backend/indicator_capture.py",
        "backend/eod_forecast_learning.py",
        "backend/eod_journal_index.py",
        "backend/research/evaluation_statistics.py",
        "backend/forecast_publication_gate.py", "backend/forecast_attempt_journal.py",
    ))).hexdigest()


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
                 symbols=DEFAULT_SYMBOLS, interval_seconds=60.0, equity_tracker=None,
                 indicator_tracker=None, comparison_journal=None):
        self.source_database = Path(source_database).resolve()
        self.journal_database = Path(journal_database).resolve()
        self.comparison_journal = Path(comparison_journal).resolve() if comparison_journal else None
        if self.source_database == self.journal_database or (
            self.source_database.exists() and self.journal_database.exists()
            and self.source_database.samefile(self.journal_database)
        ):
            raise ValueError("Research journal must be separate from source database")
        self.context_reader = context_reader
        self.symbols = tuple(dict.fromkeys(symbols))
        if not 1 <= len(self.symbols) <= 40:
            raise ValueError("Research symbol count must be between 1 and 40")
        if not math.isfinite(float(interval_seconds)) or float(interval_seconds) < 60:
            raise ValueError("Research interval must be finite and at least 60 seconds")
        self.interval_seconds = float(interval_seconds)
        self.equity_tracker = equity_tracker
        # Indicator bars never enter _collect_once's forecast histories.
        self.indicator_tracker = indicator_tracker
        self.code_fingerprint = research_code_fingerprint()
        self.source_identity = source_identity(self.source_database, self.journal_database, self.comparison_journal)
        self.instance_id = uuid.uuid4().hex
        self.started_at = datetime.now(timezone.utc).isoformat()
        self._stop = threading.Event()
        self._lock = threading.Lock()
        self._thread = None
        self._eod_frozen_cache = None
        self._eod_calendar_cache = None
        self._worker = {"attempt_count": 0, "completed_capture_count": 0,
                        "last_attempt_at_utc": None, "last_completed_at_utc": None,
                        "collecting": False, "last_error_code": None}
        self._state = {"schema_version": SCHEMA, "status": "WARMING", "symbols": {}, "reasons": []}

    def initialize_journal(self):
        from backend.forecast_attempt_journal import initialize_attempt_journal
        from backend.eod_journal_index import initialize_eod_index
        self.journal_database.parent.mkdir(parents=True, exist_ok=True)
        with closing(sqlite3.connect(self.journal_database, timeout=0.5)) as conn:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("CREATE TABLE IF NOT EXISTS research_forecasts ("
                         "forecast_id TEXT PRIMARY KEY, symbol TEXT NOT NULL, "
                         "as_of_utc TEXT NOT NULL, horizon_sessions INTEGER NOT NULL, "
                         "target_session_date TEXT, payload_json TEXT NOT NULL)")
            conn.execute("CREATE INDEX IF NOT EXISTS research_forecasts_symbol_time "
                         "ON research_forecasts(symbol, as_of_utc)")
            conn.execute("CREATE TABLE IF NOT EXISTS research_forecast_receipts ("
                         "forecast_id TEXT PRIMARY KEY, committed_at_utc TEXT NOT NULL, receipt_sha256 TEXT NOT NULL)")
            initialize_attempt_journal(conn)
            initialize_eod_index(conn)
            conn.commit()

    def _persist(self, state):
        rows = []
        prepared_at = datetime.now(timezone.utc).isoformat()
        for symbol, item in state["symbols"].items():
            for horizon, forecast in item["forecasts"].items():
                if forecast.get("status") != "RESEARCH_ONLY":
                    continue
                payload = {"forecast": forecast, "directional_shift": item["directional_shift"],
                           "evidence_sha256": item["evidence_sha256"],
                           "captured_at_utc": state["as_of_utc"], "prepared_at_utc": prepared_at}
                if int(horizon) == 0:
                    payload["eod_learning"] = item.get("eod_learning")
                    payload["loaded_source_sha256"] = self.code_fingerprint
                canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"), allow_nan=False)
                key = hashlib.sha256(canonical.encode()).hexdigest()
                rows.append((key, symbol, state["as_of_utc"], int(horizon),
                             forecast.get("target_session_date"), canonical))
        if rows:
            with closing(sqlite3.connect(self.journal_database, timeout=0.5)) as conn:
                conn.executemany("INSERT OR IGNORE INTO research_forecasts VALUES (?,?,?,?,?,?)", rows)
                conn.commit()
                # Record completion AFTER the forecast transaction commits.
                # A crash before this second append leaves issuance unverified.
                committed = datetime.now(timezone.utc).isoformat()
                pending = [row for row in rows if conn.execute(
                    "SELECT 1 FROM research_forecast_receipts WHERE forecast_id=?", (row[0],)).fetchone() is None]
                receipts = [(row[0], committed, hashlib.sha256(
                    (row[0] + "\n" + committed).encode()).hexdigest()) for row in pending]
                conn.executemany("INSERT OR IGNORE INTO research_forecast_receipts VALUES (?,?,?)", receipts)
                from backend.eod_journal_index import append_eod_index
                append_eod_index(conn, pending, committed)
                conn.commit()

    def collect_once(self, *, now=None):
        from backend.forecast_attempt_journal import append_attempt
        as_of = utc(now or datetime.now(timezone.utc))
        with self._lock:
            self._worker.update(collecting=True, last_attempt_at_utc=as_of.isoformat())
            self._worker["attempt_count"] += 1
            sequence = self._worker["attempt_count"]
        def record_attempt(state=None, error_code=None):
            return append_attempt(self.journal_database, instance_id=self.instance_id, sequence=sequence,
                attempted_at_utc=as_of.isoformat(), symbols=self.symbols, state=state,
                error_code=error_code, loaded_source_sha256=self.code_fingerprint)
        try:
            state = self._collect_once(as_of=as_of)
        except Exception:
            audit = record_attempt(error_code="RESEARCH_CAPTURE_FAILED")
            with self._lock:
                self._worker.update(collecting=False, last_error_code="RESEARCH_CAPTURE_FAILED", attempt_journal=audit)
            raise
        state["attempt_journal"] = record_attempt(state=state)
        with self._lock:
            self._state = state
            self._worker.update(collecting=False, last_completed_at_utc=as_of.isoformat(), last_error_code=None,
                                attempt_journal=state["attempt_journal"])
            self._worker["completed_capture_count"] += 1
        return copy.deepcopy(state)

    def _collect_once(self, *, as_of):
        from backend.forecast_horizons import evaluate_forecast
        from backend.forecast_horizons import inventory_daily_history
        from backend.directional_shift import evaluate_directional_shift
        from backend.eod_forecast_learning import analyze_eod_forecast, read_frozen_eod
        from backend.forecast_calendar import resolve_target_session, ET

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
            reasons.append("RETAINED_DATABASE_LOCKED" if isinstance(exc, sqlite3.OperationalError)
                           and "locked" in str(exc).lower() else "RETAINED_EVIDENCE_UNAVAILABLE")
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
        # Previous sessions are immutable. Refresh the bounded read every ten
        # minutes to notice separately appended receipts; current quotes and
        # verified close availability are rechecked every capture cycle.
        cache = self._eod_frozen_cache
        try:
            calendar_day = as_of.astimezone(ET).date()
            if self._eod_calendar_cache is None or self._eod_calendar_cache[0] != calendar_day:
                self._eod_calendar_cache = (calendar_day, resolve_target_session(as_of, 0))
            calendar = self._eod_calendar_cache[1]
            lead_bucket = math.ceil((utc(calendar["target_close_utc"]) - as_of).total_seconds() / 900)
        except (ValueError, TypeError, KeyError):
            lead_bucket = 0
        cache_scope = (as_of.date(), lead_bucket)
        # A transient lock/timeout is not immutable history. Retry it at the
        # next regular collection; successful reads retain the ten-minute cache.
        if (cache is None or cache[0] != cache_scope or cache[3]["status"] == "UNAVAILABLE"
                or time.monotonic() - cache[1] >= 600):
            if 1 <= lead_bucket <= 26:
                frozen, audit = read_frozen_eod(self.journal_database, self.symbols, as_of_utc=as_of, lead_bucket=lead_bucket)
            else:
                frozen, audit = [], {"status": "READ_ONLY", "frozen_forecasts": 0, "scope": "outside_current_session"}
            if self.comparison_journal and self.comparison_journal != self.journal_database and 1 <= lead_bucket <= 26:
                reference, reference_audit = read_frozen_eod(self.comparison_journal, self.symbols, as_of_utc=as_of, lead_bucket=lead_bucket)
                frozen += reference
                audit = {"status": "UNAVAILABLE" if "UNAVAILABLE" in (audit["status"], reference_audit["status"]) else "READ_ONLY",
                         "current_journal": audit, "comparison_journal": reference_audit}
            self._eod_frozen_cache = (cache_scope, time.monotonic(), frozen, audit)
        _, _, frozen, evaluation_audit = self._eod_frozen_cache
        for symbol, rows in histories.items():
            current = intraday[symbol][-1] if intraday[symbol] else None
            forecasts = {str(horizon): evaluate_forecast(symbol, rows, as_of_utc=as_of, horizon_sessions=horizon,
                                                        current_snapshot=current)
                         for horizon in range(11)}
            direction = evaluate_directional_shift(symbol, intraday[symbol], as_of_utc=as_of,
                                                  cross_market_history=intraday)
            inventory = inventory_daily_history(symbol, rows, as_of_utc=as_of)
            read_errors = [r for r in reasons if r in (
                "RETAINED_DATABASE_LOCKED", "RETAINED_EVIDENCE_UNAVAILABLE", "RETAINED_DATABASE_MISSING")]
            if read_errors:
                for availability in inventory.values():
                    availability.update(status="UNKNOWN", qualified_return_samples=None,
                                        verified_daily_close_count=None, reasons=read_errors)
            state["symbols"][symbol] = {"forecasts": forecasts, "directional_shift": direction,
                "history_by_horizon": inventory,
                "retained_intraday_rows": len(intraday[symbol]),
                "verified_daily_rows": sum(r.get("kind") == "daily_close" for r in rows),
                "evidence_sha256": hashlib.sha256(json.dumps(rows, sort_keys=True, default=str).encode()).hexdigest()}
            learning = analyze_eod_forecast(forecasts["0"], frozen,
                [row for row in rows if row.get("kind") == "daily_close"], as_of_utc=as_of)
            learning["journal_read"] = evaluation_audit
            if evaluation_audit["status"] == "UNAVAILABLE":
                learning["adaptive"].update(status="ABSTAIN", predicted_close=None, historical_error_band=None)
                learning["adaptive"].setdefault("reasons", []).append("EVALUATION_JOURNAL_READ_INCOMPLETE")
            state["symbols"][symbol]["eod_learning"] = learning
        self._persist(state)
        return state

    def snapshot(self, *, now=None):
        with self._lock:
            state = copy.deepcopy(self._state)
            worker = copy.deepcopy(self._worker)
        now = utc(now or datetime.now(timezone.utc))
        alive = bool(self._thread and self._thread.is_alive())
        last_progress = worker["last_completed_at_utc"] or worker["last_attempt_at_utc"]
        progress_age = (now - utc(last_progress)).total_seconds() if last_progress else None
        if self._stop.is_set():
            worker_status = "STOPPED"
        elif worker["last_error_code"]:
            worker_status = "ERROR"
        elif progress_age is not None and (progress_age < 0 or progress_age > max(120, 2 * self.interval_seconds)):
            worker_status = "STALLED"
        elif alive:
            worker_status = "COLLECTING" if worker["collecting"] else "WAITING"
        elif self._thread is not None:
            worker_status = "STOPPED"
        else:
            worker_status = "NOT_STARTED" if not worker["attempt_count"] else "IDLE"
        state.update(health_schema_version=HEALTH_SCHEMA, component="forecast_research",
                     process_alive=True, process_id=os.getpid(), instance_id=self.instance_id,
                     started_at_utc=self.started_at, loaded_source_sha256=self.code_fingerprint,
                     source_identity_sha256=self.source_identity, source_database_access="query_only",
                     comparison_journal_access="query_only" if self.comparison_journal else None,
                     capture_interval_seconds=self.interval_seconds,
                     worker={**worker, "status": worker_status, "thread_alive": alive,
                             "progress_age_seconds": progress_age})
        state["read_at_utc"] = now.isoformat()
        state["indicator_capture"] = (self.indicator_tracker.snapshot(now=now)
            if self.indicator_tracker is not None else
            {"enabled": False, "status": "STAGED_DISABLED", "scope": "technical_indicator_display_only"})
        state["capture_age_seconds"] = ((now - utc(state["as_of_utc"])).total_seconds()
                                         if state.get("as_of_utc") else None)
        # These are explicitly timestamped retained research observations. A
        # stopped collector must not keep presenting eligible numerical targets.
        reason = None
        if state["capture_age_seconds"] is not None and not 0 <= state["capture_age_seconds"] <= 120:
            future_capture = state["capture_age_seconds"] < 0
            reason = "RESEARCH_CAPTURE_TIMESTAMP_IN_FUTURE" if future_capture else "RESEARCH_CAPTURE_STALE"
            state["status"] = "UNAVAILABLE" if future_capture else "STALE"
        elif worker_status in ("STOPPED", "ERROR", "STALLED"):
            state["status"] = "UNAVAILABLE"
            reason = worker["last_error_code"] or "RESEARCH_WORKER_" + worker_status
        if reason:
            state["reasons"].append(reason)
            for item in state["symbols"].values():
                item["retained_forecasts"] = copy.deepcopy(item["forecasts"])
                for forecast in item["forecasts"].values():
                    forecast.update(status="ABSTAIN", predicted_close=None)
                    forecast["candidates"] = []
                    forecast.setdefault("reasons", []).append(reason)
                item["directional_shift"].update(status="ABSTAIN", direction=None)
                item["directional_shift"].setdefault("reason_codes", []).append(reason)
                learning = item.get("eod_learning")
                if learning:
                    learning.update(status="ABSTAIN", disagreement=None)
                    learning.setdefault("reasons", []).append(reason)
                    for comparison in learning.get("comparisons", []):
                        comparison["current_estimate"] = None
                    learning["adaptive"].update(status="ABSTAIN", predicted_close=None, historical_error_band=None)
        return state

    def _run(self):
        while not self._stop.is_set():
            try:
                self.collect_once()
            except Exception as exc:
                log.exception("Research capture failed")
                with self._lock:
                    self._worker["last_error_code"] = "RESEARCH_CAPTURE_FAILED"
            self._stop.wait(self.interval_seconds)

    def start(self):
        if self._thread and self._thread.is_alive():
            return
        self.initialize_journal()
        if self.equity_tracker is not None:
            self.equity_tracker.start()
        if self.indicator_tracker is not None:
            self.indicator_tracker.start()
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, name="market-research", daemon=True)
        self._thread.start()

    def stop(self):
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=5)
        if self.equity_tracker is not None:
            self.equity_tracker.stop()
        if self.indicator_tracker is not None:
            self.indicator_tracker.stop()
