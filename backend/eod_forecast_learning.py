"""Causal EOD research comparisons; no production model or source-store writes.

One frozen forecast per session and 15-minute time-to-close bucket is used.
The adaptive challenger uses only outcomes available before each test origin.
Repeated intraday forecasts cannot masquerade as independent trading sessions.
"""
from __future__ import annotations

from collections import Counter, defaultdict
from contextlib import closing
from datetime import datetime, timezone
import hashlib
import json
import math
from pathlib import Path
import sqlite3
from statistics import fmean, stdev
import time

from backend.forecast_calendar import resolve_target_session
from backend.forecast_horizons import SCHEMA_VERSION, _forecast_digest, _normalize, _positive, _utc

SCHEMA = "marketpin-eod-learning.v1"
METHOD = "eod_prior_error_blend_v1"
BASELINE = "last_price_v1"
COMPONENTS = (BASELINE, "eod_damped_momentum_v1", "eod_sample_mean_reversion_v1",
              "eod_positive_gex_pin_context_v1")
# Research eligibility only. Existing production thresholds are untouched.
MIN_TRAIN_SESSIONS = 20
MIN_TEST_SESSIONS = 20
MIN_INTERVAL_SESSIONS = 30
MAX_TRAIN_SESSIONS = 120
BUCKET_MINUTES = 15


def read_frozen_eod(journal_path, symbols, *, as_of_utc, limit=50000, include_current_session=False):
    """Bounded, query-only scan with outer receipt and column binding checks."""
    path = Path(journal_path).resolve()
    if not path.is_file():
        return [], {"status": "EMPTY", "reasons": ["NO_FROZEN_JOURNAL"]}
    started = time.monotonic()
    forecasts, rejected = [], Counter()
    try:
        with closing(sqlite3.connect(path.as_uri() + "?mode=ro", uri=True, timeout=0.25)) as conn:
            conn.execute("PRAGMA query_only=ON")
            conn.set_progress_handler(lambda: int(time.monotonic() - started > 3), 2000)
            receipts = conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='research_forecast_receipts'").fetchone()
            receipt_columns = ",r.committed_at_utc,r.receipt_sha256" if receipts else ",NULL,NULL"
            receipt_join = " LEFT JOIN research_forecast_receipts r ON r.forecast_id=f.forecast_id" if receipts else ""
            placeholders = ",".join("?" for _ in symbols)
            date_operator = "<=" if include_current_session else "<"
            rows = conn.execute(
                "SELECT f.forecast_id,f.symbol,f.as_of_utc,f.target_session_date,f.payload_json" + receipt_columns +
                " FROM research_forecasts f" + receipt_join + " WHERE f.horizon_sessions=0 AND f.symbol IN (" + placeholders + ") "
                "AND f.target_session_date" + date_operator + "? LIMIT ?",
                (*symbols, _utc(as_of_utc).date().isoformat(), limit + 1)).fetchall()
        if len(rows) > limit:
            return [], {"status": "UNAVAILABLE", "reasons": ["EVALUATION_SCAN_LIMIT_REACHED"]}
        # Every matching row is required or the read fails closed above. Sort
        # references only after that check: SQLite otherwise copies the large
        # receipt payloads into a temporary sort, consuming the read budget.
        rows.sort(key=lambda row: row[2] if isinstance(row[2], str) else "", reverse=True)
        for key, symbol, origin, target_day, raw, committed, receipt_hash in rows:
            try:
                origin_utc = _utc(origin)
                if origin_utc is None:
                    raise ValueError("invalid receipt origin")
                payload = json.loads(raw)
                canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"), allow_nan=False)
                forecast = payload["forecast"]
                if (hashlib.sha256(canonical.encode()).hexdigest() != key
                        or forecast["symbol"] != symbol or forecast["horizon_sessions"] != 0
                        or _utc(forecast["as_of_utc"]) != origin_utc
                        or forecast["target_session_date"] != target_day
                        or _utc(payload["captured_at_utc"]) != origin_utc):
                    raise ValueError("receipt mismatch")
                forecast = dict(forecast)
                # Older receipts only recorded a declared analysis time. They
                # can be compared, but cannot prove pre-close issuance for fitting.
                verified = bool(committed and receipt_hash == hashlib.sha256((key + "\n" + committed).encode()).hexdigest())
                forecast["journal_receipt"] = {"receipt_sha256": receipt_hash, "binding_verified": verified,
                    "persisted_at_utc": committed if verified else None}
                forecasts.append(forecast)
            except (ValueError, TypeError, KeyError, AttributeError):
                rejected["JOURNAL_RECEIPT_INVALID"] += 1
    except (sqlite3.Error, OSError) as exc:
        # Keep the original failure code for clients, with a bounded cause
        # instead of swallowing timeouts/locks or exposing SQL and file paths.
        code = getattr(exc, "sqlite_errorcode", None)
        primary_code = code & 0xff if isinstance(code, int) else None
        cause = ("EVALUATION_JOURNAL_READ_TIMEOUT" if primary_code == sqlite3.SQLITE_INTERRUPT else
                 "EVALUATION_JOURNAL_LOCKED" if primary_code in (sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED) else
                 "EVALUATION_JOURNAL_IO_ERROR" if isinstance(exc, OSError) else
                 "EVALUATION_JOURNAL_SQLITE_ERROR")
        return [], {"status": "UNAVAILABLE", "reasons": ["EVALUATION_JOURNAL_READ_FAILED", cause]}
    return forecasts, {"status": "READ_ONLY", "frozen_forecasts": len(forecasts), "rejected": dict(rejected)}


def _issued(forecast):
    try:
        if (forecast.get("schema_version") != SCHEMA_VERSION
                or forecast.get("status") != "RESEARCH_ONLY"
                or type(forecast.get("horizon_sessions")) is not int
                or forecast["horizon_sessions"] != 0
                or forecast.get("forecast_id") != _forecast_digest(forecast)):
            return None
        origin, target = _utc(forecast["as_of_utc"]), _utc(forecast["target_close_utc"])
        evidence = forecast["evidence"]
        event, available = _utc(evidence["source_timestamp_utc"]), _utc(evidence["available_at_utc"])
        spot = _positive(evidence["spot"])
        if not spot or None in (origin, target, event, available) or not event <= available <= origin < target:
            return None
        if (origin - event).total_seconds() > 120:
            return None
        calendar = resolve_target_session(origin, 0)
        if (_utc(calendar["target_close_utc"]) != target
                or calendar["target_session_date"] != forecast["target_session_date"]
                or not _utc(calendar["session_open_utc"]) <= origin < _utc(calendar["session_close_utc"])):
            return None
        candidates = {}
        signatures = {}
        for candidate in forecast["candidates"]:
            value, method = _positive(candidate.get("predicted_close")), candidate.get("method_id")
            if value is None or candidate.get("status") != "RESEARCH_ONLY" or not isinstance(method, str):
                continue
            if method in candidates:
                return None
            candidates[method] = value
            signatures[method] = tuple(sorted(candidate.get("components") or []))
        if candidates.get(BASELINE) != spot:
            return None
        receipt = forecast.get("journal_receipt") or {}
        persisted = _utc(receipt.get("persisted_at_utc"))
        issuance_verified = receipt.get("binding_verified") is True and persisted is not None and origin <= persisted < target
        return {"origin": origin, "target": target, "spot": spot, "predictions": candidates,
                "signatures": signatures, "session": forecast["target_session_date"],
                "forecast_id": forecast["forecast_id"], "symbol": forecast["symbol"],
                "issuance_verified": issuance_verified,
                "bucket": math.ceil((target - origin).total_seconds() / (BUCKET_MINUTES * 60))}
    except (KeyError, TypeError, ValueError, OverflowError, AttributeError):
        return None


def _weights(training, methods):
    # Scale errors by the reference price before comparing different sessions.
    inverse = {method: 1 / max(1e-6, fmean(abs(r["predictions"][method] - r["actual"]) / r["spot"]
                                           for r in training)) for method in methods}
    total = sum(inverse.values())
    weights = {method: 0.5 * inverse[method] / total for method in methods}
    weights[BASELINE] += 0.5
    return weights


def _summary(errors, baseline_errors):
    mae, baseline_mae = fmean(map(abs, errors)), fmean(map(abs, baseline_errors))
    return {"sessions": len(errors), "mae_points": mae, "bias_points": fmean(errors),
            "baseline_mae_points": baseline_mae,
            "skill_vs_last_price": 1 - mae / baseline_mae if baseline_mae else None}


def analyze_eod_forecast(current, frozen_forecasts, official_outcomes, *, as_of_utc=None):
    """Return a separately labeled challenger and point-in-time replay evidence.

    Historical error bands describe replay errors, not guaranteed coverage.
    No result changes the selected live formula or grants production authority.
    """
    now = _utc(as_of_utc or datetime.now(timezone.utc))
    if now is None:
        raise ValueError("as_of_utc_must_be_timezone_aware")
    issued = _issued(current)
    result = {"schema_version": SCHEMA, "as_of_utc": now.isoformat(),
              "source_forecast_id": current.get("forecast_id"), "status": "ABSTAIN",
              "decision_grade": False, "production_promotion_allowed": False,
              "accuracy_improvement_verified": False, "comparisons": [], "scored_sessions": [],
              "adaptive": {"method_id": METHOD, "status": "ABSTAIN", "predicted_close": None,
                           "minimum_training_sessions": MIN_TRAIN_SESSIONS,
                           "minimum_test_sessions": MIN_TEST_SESSIONS, "historical_error_band": None},
              "reasons": []}
    if not issued or not 0 <= (now - issued["origin"]).total_seconds() <= 120 or now >= issued["target"]:
        result["reasons"] = ["CURRENT_FORECAST_NOT_ELIGIBLE"]
        return result
    cutoff = issued["origin"]
    prices = list(issued["predictions"].values())
    result.update(status="RESEARCH_ONLY", lead_bucket_minutes={
        "lower_exclusive": (issued["bucket"] - 1) * BUCKET_MINUTES,
        "upper_inclusive": issued["bucket"] * BUCKET_MINUTES},
        disagreement={"low": min(prices), "high": max(prices), "width_points": max(prices) - min(prices),
                      "kind": "candidate_spread_not_prediction_interval"})
    excluded = Counter()
    labels, conflicting = {}, set()
    for raw in official_outcomes:
        row, reason = _normalize(raw, issued["symbol"], cutoff)
        if row is None or not row.daily:
            excluded[reason or "not_official_close"] += 1
            continue
        day = row.day.isoformat()
        if day in labels and labels[day][0].price != row.price:
            conflicting.add(day)
        # Even identical duplicates use the latest availability, conservatively.
        if day not in labels or row.available > labels[day][0].available:
            labels[day] = (row, raw.get("source_sha256"))
    # Prospective challengers have no comparable frozen history yet. Their
    # absence in old receipts must not erase the established shared cohort or
    # admit their unvalidated values into adaptive weights.
    prospective = {row["method_id"] for row in current["candidates"]
                   if row.get("prospective_only") is True and row.get("method_id") in issued["predictions"]}
    methods = tuple(method for method in issued["predictions"] if method not in prospective)
    result["prospective_methods_excluded_from_shared_cohort"] = sorted(prospective)
    by_day = defaultdict(list)
    for forecast in frozen_forecasts:
        old = _issued(forecast)
        if old is None:
            excluded["invalid_frozen_forecast"] += 1
            continue
        if old["symbol"] != issued["symbol"] or old["bucket"] != issued["bucket"] or old["target"] >= cutoff:
            continue
        if old["session"] not in labels or old["session"] in conflicting:
            excluded["missing_or_conflicting_verified_close"] += 1
            continue
        if not all(m in old["predictions"] and old["signatures"][m] == issued["signatures"][m] for m in methods):
            excluded["different_candidate_family"] += 1
            continue
        outcome, artifact = labels[old["session"]]
        by_day[old["session"]].append({**old, "actual": outcome.price,
                                     "outcome_available": outcome.available, "outcome_sha256": artifact})
    selected = []
    for day, rows in sorted(by_day.items()):
        rows.sort(key=lambda r: r["origin"])
        earliest = [r for r in rows if r["origin"] == rows[0]["origin"]]
        if len({r["forecast_id"] for r in earliest}) != 1:
            excluded["conflicting_forecast_at_same_origin"] += 1
            continue
        selected.append(rows[0])
    baseline_errors = [r["spot"] - r["actual"] for r in selected]
    for method in methods:
        summary = (_summary([r["predictions"][method] - r["actual"] for r in selected], baseline_errors)
                   if selected else {"sessions": 0, "mae_points": None, "bias_points": None,
                                     "baseline_mae_points": None, "skill_vs_last_price": None})
        result["comparisons"].append({"method_id": method, "current_estimate": issued["predictions"][method], **summary})
    result["scored_sessions"] = [{"session": r["session"], "forecast_id": r["forecast_id"],
        "forecast_at_utc": r["origin"].isoformat(), "official_close": r["actual"],
        "close_available_at_utc": r["outcome_available"].isoformat(), "source_sha256": r["outcome_sha256"],
        "preclose_issuance_verified": r["issuance_verified"]}
        for r in selected]
    result["excluded"] = dict(excluded)
    result["independent_sessions"] = len(selected)
    verified_issued = [r for r in selected if r["issuance_verified"]]
    result["verified_issuance_sessions"] = len(verified_issued)
    pool = tuple(m for m in COMPONENTS if m in methods)
    tests = []
    for row in verified_issued:
        train = [r for r in verified_issued if r["target"] < row["origin"]
                 and r["outcome_available"] <= row["origin"]][-MAX_TRAIN_SESSIONS:]
        if len(train) < MIN_TRAIN_SESSIONS or len(pool) < 2:
            continue
        weights = _weights(train, pool)
        prediction = sum(weights[m] * row["predictions"][m] for m in pool)
        tests.append({"session": row["session"], "forecast_at_utc": row["origin"].isoformat(),
                      "training_sessions": len(train), "predicted_close": prediction,
                      "actual_close": row["actual"], "baseline": row["spot"], "weights": weights,
                      "error": prediction - row["actual"], "baseline_error": row["spot"] - row["actual"]})
    adaptive = result["adaptive"]
    adaptive.update(training_sessions=min(len(verified_issued), MAX_TRAIN_SESSIONS), test_sessions=len(tests),
                    evaluation_kind="causal_replay_of_frozen_candidates_not_issued_adaptive_forecasts",
                    replay=tests, reasons=[])
    if tests:
        adaptive["evaluation"] = _summary([r["error"] for r in tests], [r["baseline_error"] for r in tests])
    if len(verified_issued) < len(selected):
        adaptive["legacy_sessions_without_verified_issuance"] = len(selected) - len(verified_issued)
    if len(verified_issued) < MIN_TRAIN_SESSIONS or len(pool) < 2:
        adaptive["reasons"].append("INSUFFICIENT_MATCHED_TRAINING_SESSIONS")
    if len(tests) < MIN_TEST_SESSIONS:
        adaptive["reasons"].append("INSUFFICIENT_WALK_FORWARD_TEST_SESSIONS")
    else:
        gains = [(abs(r["baseline_error"]) - abs(r["error"])) / r["baseline"] for r in tests]
        # Conservative engineering screen, not an inferential significance claim.
        screen = fmean(gains) - 2 * stdev(gains) / math.sqrt(len(gains))
        adaptive["paired_improvement_screen"] = screen
        if screen <= 0 or (adaptive["evaluation"]["skill_vs_last_price"] or 0) < 0.05:
            adaptive["reasons"].append("NO_STABLE_IMPROVEMENT_OVER_LAST_PRICE")
    if not adaptive["reasons"]:
        weights = _weights(verified_issued[-MAX_TRAIN_SESSIONS:], pool)
        prediction = sum(weights[m] * issued["predictions"][m] for m in pool)
        adaptive.update(status="RESEARCH_ONLY", predicted_close=prediction, weights=weights)
        if len(tests) >= MIN_INTERVAL_SESSIONS:
            residuals = sorted(abs(r["error"]) / r["baseline"] for r in tests)
            radius = residuals[min(len(residuals), math.ceil(0.9 * (len(residuals) + 1))) - 1] * issued["spot"]
            adaptive["historical_error_band"] = {"lower": max(0.0, prediction - radius), "upper": prediction + radius,
                "nominal_level": 0.9, "sessions": len(residuals), "coverage_verified": False,
                "kind": "historical_replay_absolute_error_quantile_not_guaranteed_coverage"}
    result["reasons"] = ["RESEARCH_COMPARISON_ONLY", "LIVE_ACCURACY_NOT_ESTABLISHED"]
    return result
