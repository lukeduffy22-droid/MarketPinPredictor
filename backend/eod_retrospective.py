"""After-close scoring of frozen candidates; never issues or promotes a forecast."""
from collections import Counter, defaultdict
from datetime import datetime, timezone

from backend.eod_forecast_learning import BASELINE, BUCKET_MINUTES, _issued, _summary
from backend.forecast_horizons import _normalize, _utc


def score_completed_eod(forecasts, outcomes, *, symbols, as_of_utc=None):
    now = _utc(as_of_utc or datetime.now(timezone.utc))
    if now is None:
        raise ValueError("as_of_utc_must_be_timezone_aware")
    excluded = Counter()
    labels, conflicts = {}, set()
    for symbol in symbols:
        for raw in outcomes.get(symbol, []):
            row, reason = _normalize(raw, symbol, now)
            if row is None or not row.daily:
                excluded[reason or "not_official_close"] += 1
                continue
            key = (symbol, row.day.isoformat())
            if key in labels and labels[key].price != row.price:
                conflicts.add(key)
            if key not in labels or row.available > labels[key].available:
                labels[key] = row
    grouped = defaultdict(list)
    for forecast in forecasts:
        row = _issued(forecast)
        if row is None:
            excluded["invalid_frozen_forecast"] += 1
            continue
        if row["symbol"] not in symbols or row["target"] >= now:
            excluded["session_not_completed_or_out_of_scope"] += 1
            continue
        key = (row["symbol"], row["session"])
        if key not in labels or key in conflicts:
            excluded["missing_or_conflicting_verified_close"] += 1
            continue
        # Keep legacy comparisons separate from proven pre-close issuance.
        cohort = ("prospective_fixed_lead" if row["prospective_selection"] else
                  "verified_preclose" if row["issuance_verified"] else "unverified_issuance_diagnostic")
        grouped[(row["symbol"], row["session"], row["bucket"], cohort)].append(row)
    scored, comparisons = [], defaultdict(list)
    for (symbol, session, bucket, cohort), rows in sorted(grouped.items()):
        earliest = min(r["origin"] for r in rows)
        selected = [r for r in rows if r["origin"] == earliest]
        if len({r["forecast_id"] for r in selected}) != 1:
            excluded["conflicting_forecast_at_same_origin"] += 1
            continue
        row = selected[0]
        label = labels[(symbol, session)]
        errors = {m: v - label.price for m, v in row["predictions"].items()}
        scored.append({"symbol": symbol, "session": session, "lead_bucket_minutes": bucket * BUCKET_MINUTES,
            "cohort": cohort, "forecast_id": row["forecast_id"], "forecast_at_utc": earliest.isoformat(),
            "prospective_selection": row["prospective_selection"],
            "official_close": label.price, "close_available_at_utc": label.available.isoformat(),
            "source_sha256": label.source_sha256, "predictions": row["predictions"],
            "error_points": errors, "absolute_error_points": {m: abs(e) for m, e in errors.items()}})
        for method, error in errors.items():
            # Composition changes get distinct rows; candidate and baseline share the exact same origins.
            comparisons[(symbol, bucket, cohort, method, row["signatures"][method])].append(
                (session, error, errors[BASELINE]))
    return {"schema_version": "marketpin-completed-eod-scores.v1", "as_of_utc": now.isoformat(),
        "status": "RESEARCH_COMPARISON_ONLY", "decision_grade": False,
        "accuracy_improvement_verified": False, "production_promotion_allowed": False,
        "selection_rule": "prospective fixed-lead receipts kept separate; legacy earliest eligible per symbol/session/15-minute bucket/issuance cohort",
        "excluded": dict(excluded), "scored_forecasts": scored,
        "independent_sessions": {s: len({r["session"] for r in scored if r["symbol"] == s}) for s in symbols},
        "comparisons": [{"symbol": s, "lead_bucket_minutes": b * BUCKET_MINUTES,
            "cohort": c, "method_id": m, "components": list(signature),
            **_summary([r[1] for r in rows], [r[2] for r in rows])}
            for (s, b, c, m, signature), rows in sorted(comparisons.items())]}
