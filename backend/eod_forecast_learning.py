"""Causal EOD research comparisons; no production model or source-store writes.

One frozen forecast per session and 15-minute time-to-close bucket is used.
The adaptive challenger uses only outcomes available before each test origin.
Repeated intraday forecasts cannot masquerade as independent trading sessions.
"""
from __future__ import annotations

from collections import Counter, defaultdict, OrderedDict
from copy import deepcopy
from contextlib import closing
from datetime import datetime, timezone
import hashlib
import json
import math
from pathlib import Path
import sqlite3
from statistics import fmean, stdev
import time
import sys

from backend.forecast_calendar import resolve_target_session, session_bounds
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


# Only journals with enforced append-only source/receipt tables may reuse verified
# content. Cache entries bind row identity, receipt, projection hash and schema.
_READ_CACHES = OrderedDict()
MAX_CACHE_ENTRIES = 50000
MAX_CACHE_BYTES = 32 * 1024 * 1024
MAX_FORECAST_BYTES = 256 * 1024
READ_BUDGET_SECONDS = 3.0


def _cache_cost(value):
    """Count nested Python objects, including metadata, rather than raw payload size."""
    seen = set()
    def size(item):
        if id(item) in seen:
            return 0
        seen.add(id(item))
        total = sys.getsizeof(item)
        if isinstance(item, dict):
            total += sum(size(k) + size(v) for k, v in item.items())
        elif isinstance(item, (tuple, list)):
            total += sum(map(size, item))
        return total
    return size(value) + 256  # OrderedDict bookkeeping allowance per entry.


class _VerifiedCache(OrderedDict):
    def __init__(self):
        super().__init__()
        self.costs = {}
        self.byte_size = 0

    def remember(self, key, value):
        cost = _cache_cost((key, value))
        if cost > MAX_CACHE_BYTES:
            return cost
        while self and (self.byte_size + cost > MAX_CACHE_BYTES or len(self) >= MAX_CACHE_ENTRIES):
            old, _ = self.popitem(last=False)
            self.byte_size -= self.costs.pop(old)
        self[key] = deepcopy(value)
        self.costs[key] = cost
        self.byte_size += cost
        return cost


class _CohortCapacityError(Exception):
    pass


def read_frozen_eod(journal_path, symbols, *, as_of_utc, limit=50000, include_current_session=False, lead_bucket=None):
    """Bounded metadata-first read; cold-cache work resumes after a timeout.

    Partial cohorts are never returned. A read-only legacy/comparison database
    lacking append-only guards is verified afresh on every call.
    """
    from backend.eod_journal_index import binding, validate_selection, guards_verified
    if lead_bucket is not None and (type(lead_bucket) is not int or not 1 <= lead_bucket <= 26):
        raise ValueError("lead bucket must be 1..26")
    path = Path(journal_path).resolve()
    if not path.is_file():
        return [], {"status": "EMPTY", "reasons": ["NO_FROZEN_JOURNAL"]}
    started = time.monotonic()
    forecasts, rejected = [], Counter()
    cached_count = 0
    cohort_bytes = 0
    def budget():
        if time.monotonic() - started > READ_BUDGET_SECONDS:
            raise TimeoutError("evaluation budget exceeded")
    try:
        with closing(sqlite3.connect(path.as_uri() + "?mode=ro", uri=True, timeout=0.25)) as conn:
            conn.execute("PRAGMA query_only=ON")
            conn.set_progress_handler(lambda: int(time.monotonic() - started > READ_BUDGET_SECONDS), 2000)
            objects = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type IN ('table','trigger')")}
            receipts = "research_forecast_receipts" in objects
            compact = "research_eod_index" in objects
            selection = "research_eod_fixed_lead_selections" in objects
            guarded = guards_verified(conn)
            stat = path.stat()
            identity = (str(path), stat.st_dev, stat.st_ino, conn.execute("PRAGMA schema_version").fetchone()[0])
            if guarded:
                if identity not in _READ_CACHES:
                    if len(_READ_CACHES) >= 4:
                        _READ_CACHES.popitem(last=False)
                    _READ_CACHES[identity] = _VerifiedCache()
                _READ_CACHES.move_to_end(identity)
                cache = _READ_CACHES[identity]
            else:
                cache = {}
            receipt_columns = ",r.committed_at_utc,r.receipt_sha256" if receipts else ",NULL,NULL"
            receipt_join = " LEFT JOIN research_forecast_receipts r ON r.forecast_id=f.forecast_id" if receipts else ""
            compact_columns = ",c.forecast_sha256,c.binding_sha256" if compact else ",NULL,NULL"
            compact_join = " LEFT JOIN research_eod_index c ON c.forecast_id=f.forecast_id" if compact else ""
            placeholders = ",".join("?" for _ in symbols)
            date_operator = "<=" if include_current_session else "<"
            rows = conn.execute(
                "SELECT f.rowid,f.forecast_id,f.symbol,f.as_of_utc,f.target_session_date" + receipt_columns + compact_columns +
                " FROM research_forecasts f" + receipt_join + compact_join +
                " WHERE f.horizon_sessions=0 AND f.symbol IN (" + placeholders + ") "
                "AND f.target_session_date" + date_operator + "? LIMIT ?",
                (*symbols, _utc(as_of_utc).date().isoformat(), limit + 1)).fetchall()
            budget()
            if len(rows) > limit:
                return [], {"status": "UNAVAILABLE", "reasons": ["EVALUATION_SCAN_LIMIT_REACHED"]}
            if lead_bucket is not None:
                scoped = []
                closes = {}
                for row in rows:
                    budget()
                    try:
                        if row[4] not in closes:
                            closes[row[4]] = session_bounds(datetime.fromisoformat(row[4]).date())[1]
                        bucket = math.ceil((closes[row[4]] - _utc(row[3])).total_seconds() / (60 * BUCKET_MINUTES))
                        if bucket == lead_bucket:
                            scoped.append(row)
                    except (TypeError, ValueError, OverflowError):
                        rejected["JOURNAL_RECEIPT_INVALID"] += 1
                rows = scoped
            # Only small metadata is sorted; large retained payloads never enter
            # a SQLite temporary sort or an unbounded fetchall.
            rows.sort(key=lambda r: r[3] if isinstance(r[3], str) else "", reverse=True)
            for metadata in rows:
                budget()
                rowid, key, symbol, origin, target_day, committed, receipt_hash, compact_sha, compact_binding = metadata
                try:
                    if metadata in cache:
                        forecast = deepcopy(cache[metadata])
                        cache.move_to_end(metadata)
                        cohort_bytes += cache.costs[metadata]
                        cached_count += 1
                    else:
                        origin_utc = _utc(origin)
                        if origin_utc is None:
                            raise ValueError("invalid receipt origin")
                        # A compact projection is not an authenticated Merkle proof.
                        # Verify its original content-addressed envelope once before
                        # cache reuse; a copied/tampered index cannot invent lineage.
                        raw = conn.execute("SELECT CASE WHEN length(CAST(payload_json AS BLOB))<=? "
                            "THEN payload_json END FROM research_forecasts WHERE rowid=?",
                            (4 * 1024 * 1024, rowid)).fetchone()[0]
                        if not isinstance(raw, str):
                            raise ValueError("oversize or missing receipt")
                        payload = json.loads(raw)
                        canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"), allow_nan=False)
                        if hashlib.sha256(canonical.encode()).hexdigest() != key or _utc(payload["captured_at_utc"]) != origin_utc:
                            raise ValueError("outer receipt mismatch")
                        forecast = payload["forecast"]
                        if compact_sha:
                            projection = conn.execute("SELECT CASE WHEN length(CAST(forecast_json AS BLOB))<=? "
                                "THEN forecast_json END FROM research_eod_index WHERE forecast_id=?",
                                (MAX_FORECAST_BYTES, key)).fetchone()[0]
                            expected = json.dumps(forecast, sort_keys=True, separators=(",", ":"), allow_nan=False)
                            if (projection != expected or hashlib.sha256(expected.encode()).hexdigest() != compact_sha
                                    or binding(key, compact_sha, committed) != compact_binding):
                                raise ValueError("compact receipt mismatch")
                        budget()
                        if (forecast["symbol"] != symbol or forecast["horizon_sessions"] != 0
                                or _utc(forecast["as_of_utc"]) != origin_utc or forecast["target_session_date"] != target_day):
                            raise ValueError("receipt column mismatch")
                        forecast = dict(forecast)
                        verified = bool(committed and receipt_hash == hashlib.sha256((key + "\n" + committed).encode()).hexdigest())
                        forecast["journal_receipt"] = {"receipt_sha256": receipt_hash, "binding_verified": verified,
                            "persisted_at_utc": committed if verified else None}
                        if guarded:
                            cohort_bytes += cache.remember(metadata, forecast)
                        else:
                            cohort_bytes += _cache_cost((metadata, forecast))
                    if cohort_bytes > MAX_CACHE_BYTES:
                        raise _CohortCapacityError()
                    # Selection is checked on every read, even when payload parsing
                    # was cached; a newly appended receipt must be visible.
                    selected = conn.execute("SELECT protocol_id,lead_minutes,selected_at_utc,receipt_sha256 "
                        "FROM research_eod_fixed_lead_selections WHERE forecast_id=?", (key,)).fetchone() if selection else None
                    forecast = dict(forecast)
                    forecast["prospective_selection"] = validate_selection(selected, key=key, symbol=symbol,
                        day=target_day, origin=_utc(origin), target=_utc(forecast.get("target_close_utc")), committed=committed)
                    forecasts.append(forecast)
                except (ValueError, TypeError, KeyError, AttributeError):
                    rejected["JOURNAL_RECEIPT_INVALID"] += 1
            budget()
    except _CohortCapacityError:
        return [], {"status": "UNAVAILABLE", "reasons": ["EVALUATION_COHORT_MEMORY_LIMIT"],
                    "maximum_bytes": MAX_CACHE_BYTES, "complete_cohort": False}
    except (sqlite3.Error, OSError, TimeoutError) as exc:
        code = getattr(exc, "sqlite_errorcode", None)
        primary_code = code & 0xff if isinstance(code, int) else None
        cause = ("EVALUATION_JOURNAL_READ_TIMEOUT" if isinstance(exc, TimeoutError) or primary_code == sqlite3.SQLITE_INTERRUPT else
                 "EVALUATION_JOURNAL_LOCKED" if primary_code in (sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED) else
                 "EVALUATION_JOURNAL_IO_ERROR" if isinstance(exc, OSError) else "EVALUATION_JOURNAL_SQLITE_ERROR")
        return [], {"status": "UNAVAILABLE", "reasons": ["EVALUATION_JOURNAL_READ_FAILED", cause],
                    "verified_cache_entries": len(cache) if "cache" in locals() else 0}
    return forecasts, {"status": "READ_ONLY", "frozen_forecasts": len(forecasts), "rejected": dict(rejected),
                       "verified_cache_hits": cached_count, "complete_cohort": True,
                       "lead_bucket_minutes": lead_bucket * BUCKET_MINUTES if lead_bucket else None}


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
        prospective = forecast.get("prospective_selection")
        prospective = prospective if issuance_verified and isinstance(prospective, dict) and prospective.get("binding_verified") is True else None
        return {"origin": origin, "target": target, "spot": spot, "predictions": candidates,
                "signatures": signatures, "session": forecast["target_session_date"],
                "forecast_id": forecast["forecast_id"], "symbol": forecast["symbol"],
                "issuance_verified": issuance_verified,
                "prospective_selection": prospective,
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
    result["prospective_fixed_lead_sessions"] = sum(bool(r["prospective_selection"]) for r in selected)
    result["selection_scope"] = "diagnostic earliest-in-bucket; prospective fixed-lead cohort reported separately after completion"
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
