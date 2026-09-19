"""Evaluate the append-only live shadow formula journal without fitting a model.

This is a reproducible audit companion, not a training or promotion tool.  It
reads an existing SQLite journal in read-only mode, recomputes metrics from the
stored predictions/outcomes, checks point-in-time ordering, and emits JSON and
Markdown evidence.  A single live session is never represented as out-of-sample
validation.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import sqlite3
from collections import Counter, defaultdict
from datetime import datetime, time, timezone
from pathlib import Path
from statistics import mean
from typing import Any, Iterable
from zoneinfo import ZoneInfo


UTC = timezone.utc
CENTRAL = ZoneInfo("America/Chicago")
TABLE = "shadow_prediction_journal"
BASELINE_ID = "shadow-naive-last-price"
CANDIDATE_ID = "shadow-pin-context-linear"
CORE_SYMBOLS = ("SPX", "NDX")
WALK_FORWARD_EMBARGO_SECONDS = 300
WALK_FORWARD_MIN_TRAIN_SESSIONS = 5
WALK_FORWARD_MIN_INDEPENDENT_SESSIONS = 10
WALK_FORWARD_MIN_SESSIONS_PER_SYMBOL = 5
WALK_FORWARD_MIN_PURGED_FOLDS = 5
PROMOTION_MIN_HELD_OUT_SESSIONS = 60
V2_CANDIDATE_ID = "shadow-pin-context-no-zero-gamma"
V2_CANDIDATE_VERSION = "0.2.0-preregistered"


def _parse_timestamp(value: object) -> datetime | None:
    if not value:
        return None
    text = str(value).strip().replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return None
    return parsed.astimezone(UTC)


def _finite(value: object) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _safe_json(value: object, default: Any) -> Any:
    try:
        return json.loads(str(value))
    except (TypeError, ValueError, json.JSONDecodeError):
        return default


def _process_identity(row: dict[str, Any]) -> tuple[str, int] | None:
    provenance = _safe_json(row.get("provenance_json"), {})
    if not isinstance(provenance, dict):
        return None
    epoch_id = provenance.get("subscription_epoch_id")
    generation_value = provenance.get("subscription_generation")
    if not isinstance(epoch_id, str) or len(epoch_id) != 64:
        return None
    if any(character not in "0123456789abcdef" for character in epoch_id):
        return None
    if isinstance(generation_value, bool):
        return None
    try:
        generation = int(generation_value)
    except (TypeError, ValueError, OverflowError):
        return None
    return (epoch_id, generation) if generation > 0 else None


def _round(value: float | None, digits: int = 6) -> float | None:
    return round(value, digits) if value is not None and math.isfinite(value) else None


def _file_sha256(path: Path) -> str | None:
    if not path.is_file():
        return None
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _metrics(rows: Iterable[dict[str, Any]]) -> dict[str, object]:
    selected = list(rows)
    errors: list[float] = []
    hits: list[int] = []
    for row in selected:
        predicted = _finite(row.get("predicted_price"))
        realized = _finite(row.get("realized_price"))
        if predicted is None or realized is None:
            continue
        errors.append(predicted - realized)
        if row.get("direction_hit") is not None:
            hits.append(int(row["direction_hit"]))
    if not errors:
        return {
            "n": 0,
            "mae_points": None,
            "rmse_points": None,
            "bias_points": None,
            "directional_accuracy": None,
        }
    return {
        "n": len(errors),
        "mae_points": _round(mean(abs(error) for error in errors)),
        "rmse_points": _round(math.sqrt(mean(error * error for error in errors))),
        "bias_points": _round(mean(errors)),
        "directional_accuracy": _round(mean(hits)) if hits else None,
    }


def _time_bucket(timestamp: datetime | None) -> str:
    if timestamp is None:
        return "invalid_timestamp"
    local = timestamp.astimezone(CENTRAL).time()
    if local < time(10, 30):
        return "open_0830_1029_CT"
    if local < time(13, 30):
        return "midday_1030_1329_CT"
    return "close_1330_1500_CT"


def _gex_regime(row: dict[str, Any]) -> str:
    snapshot = _safe_json(row.get("feature_snapshot_json"), {})
    net_gex = _finite(snapshot.get("net_gex")) if isinstance(snapshot, dict) else None
    if net_gex is None:
        return "missing_net_gex"
    if net_gex > 0:
        return "positive_net_gex"
    if net_gex < 0:
        return "negative_net_gex"
    return "zero_net_gex"


def _group_metrics(rows: list[dict[str, Any]], key_function) -> dict[str, object]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[str(key_function(row))].append(row)
    return {key: _metrics(value) for key, value in sorted(grouped.items())}


def _formula_summary(rows: list[dict[str, Any]]) -> list[dict[str, object]]:
    groups: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        groups[(str(row["symbol"]), str(row["formula_id"]), str(row["formula_version"]))].append(row)
    summaries: list[dict[str, object]] = []
    for (symbol, formula_id, version), group in sorted(groups.items()):
        provenance_eligible = [
            row for row in group if _process_identity(row) is not None
        ]
        eligible = [
            row for row in provenance_eligible
            if int(row["abstained"] or 0) == 0 and _finite(row.get("predicted_price")) is not None
        ]
        scored = [row for row in eligible if _finite(row.get("realized_price")) is not None]
        item: dict[str, object] = {
            "symbol": symbol,
            "formula_id": formula_id,
            "formula_version": version,
            "total_rows": len(group),
            "provenance_eligible_rows": len(provenance_eligible),
            "eligible_rows": len(eligible),
            "scored_rows": len(scored),
            "eligibility_coverage": _round(len(eligible) / len(group)) if group else None,
            "scoring_coverage_of_eligible": _round(len(scored) / len(eligible)) if eligible else None,
            "metrics": _metrics(scored),
            "metrics_by_time_of_day": _group_metrics(
                scored,
                lambda row: _time_bucket(_parse_timestamp(row.get("prediction_timestamp_utc"))),
            ),
            "metrics_by_gex_regime": _group_metrics(scored, _gex_regime),
        }
        summaries.append(item)
    return summaries


def _paired_comparison(rows: list[dict[str, Any]]) -> list[dict[str, object]]:
    scored = [
        row for row in rows
        if _process_identity(row) is not None
        and int(row["abstained"] or 0) == 0
        and _finite(row.get("predicted_price")) is not None
        and _finite(row.get("realized_price")) is not None
    ]
    keyed: dict[
        tuple[str, str, int, str, int], dict[str, dict[str, Any]]
    ] = defaultdict(dict)
    for row in scored:
        identity = _process_identity(row)
        assert identity is not None
        key = (
            str(row["symbol"]),
            str(row["prediction_timestamp_utc"]),
            int(row["horizon_seconds"]),
            identity[0],
            identity[1],
        )
        keyed[key][str(row["formula_id"])] = row
    by_symbol: dict[str, list[tuple[dict[str, Any], dict[str, Any]]]] = defaultdict(list)
    for (symbol, _timestamp, _horizon, _epoch, _generation), formulas in keyed.items():
        if BASELINE_ID in formulas and CANDIDATE_ID in formulas:
            by_symbol[symbol].append((formulas[BASELINE_ID], formulas[CANDIDATE_ID]))
    output: list[dict[str, object]] = []
    for symbol, pairs in sorted(by_symbol.items()):
        baseline_rows = [pair[0] for pair in pairs]
        candidate_rows = [pair[1] for pair in pairs]
        baseline_metrics = _metrics(baseline_rows)
        candidate_metrics = _metrics(candidate_rows)
        baseline_errors = [
            abs(float(row["predicted_price"]) - float(row["realized_price"]))
            for row in baseline_rows
        ]
        candidate_errors = [
            abs(float(row["predicted_price"]) - float(row["realized_price"]))
            for row in candidate_rows
        ]
        wins = sum(candidate < baseline for candidate, baseline in zip(candidate_errors, baseline_errors))
        ties = sum(candidate == baseline for candidate, baseline in zip(candidate_errors, baseline_errors))
        mae_improvement = None
        if baseline_metrics["mae_points"] is not None and candidate_metrics["mae_points"] is not None:
            mae_improvement = float(baseline_metrics["mae_points"]) - float(candidate_metrics["mae_points"])
        paired_sessions = sorted({
            timestamp.astimezone(CENTRAL).date().isoformat()
            for baseline, _candidate in pairs
            if (timestamp := _parse_timestamp(baseline.get("prediction_timestamp_utc"))) is not None
        })
        output.append({
            "symbol": symbol,
            "common_scored_timestamps": len(pairs),
            "baseline": baseline_metrics,
            "candidate": candidate_metrics,
            "candidate_mae_improvement_points": _round(mae_improvement),
            "candidate_win_rate": _round(wins / len(pairs)) if pairs else None,
            "ties": ties,
            "independent_trading_sessions": len(paired_sessions),
            "promotion_supported": False,
            "promotion_reason": (
                f"Descriptive paired evidence spans {len(paired_sessions)} independent CT trading "
                "session(s), but this report does not execute purged walk-forward validation or "
                "fit coefficients; keep the candidate in shadow mode."
            ),
        })
    return output


def _walk_forward_status(
    rows: list[dict[str, Any]],
    trading_dates: list[str],
) -> dict[str, object]:
    """Describe the no-fit walk-forward evidence gate from actual journal rows."""
    scored = [
        row for row in rows
        if _process_identity(row) is not None
        and int(row.get("abstained") or 0) == 0
        and _finite(row.get("predicted_price")) is not None
        and _finite(row.get("realized_price")) is not None
    ]
    keyed: dict[tuple[str, str, int, str, int], set[str]] = defaultdict(set)
    for row in scored:
        identity = _process_identity(row)
        assert identity is not None
        keyed[
            (
                str(row.get("symbol") or "").upper(),
                str(row.get("prediction_timestamp_utc") or ""),
                int(row.get("horizon_seconds") or 0),
                identity[0],
                identity[1],
            )
        ].add(str(row.get("formula_id") or ""))

    paired_dates_by_symbol: dict[str, set[str]] = defaultdict(set)
    for (
        symbol,
        timestamp_text,
        _horizon,
        _epoch,
        _generation,
    ), formula_ids in keyed.items():
        if BASELINE_ID not in formula_ids or CANDIDATE_ID not in formula_ids:
            continue
        timestamp = _parse_timestamp(timestamp_text)
        if timestamp is not None:
            paired_dates_by_symbol[symbol].add(
                timestamp.astimezone(CENTRAL).date().isoformat()
            )

    paired_dates = set().union(*paired_dates_by_symbol.values()) if paired_dates_by_symbol else set()
    symbol_counts = {
        symbol: len(paired_dates_by_symbol.get(symbol, set()))
        for symbol in CORE_SYMBOLS
    }
    reasons: list[str] = []
    if len(trading_dates) < WALK_FORWARD_MIN_INDEPENDENT_SESSIONS:
        reasons.append(
            f"journal has {len(trading_dates)} independent CT trading sessions; "
            f"{WALK_FORWARD_MIN_INDEPENDENT_SESSIONS} required"
        )
    if len(paired_dates) < WALK_FORWARD_MIN_INDEPENDENT_SESSIONS:
        reasons.append(
            f"paired scored baseline/candidate evidence has {len(paired_dates)} independent "
            f"sessions; {WALK_FORWARD_MIN_INDEPENDENT_SESSIONS} required"
        )
    for symbol in CORE_SYMBOLS:
        count = symbol_counts[symbol]
        if count < WALK_FORWARD_MIN_SESSIONS_PER_SYMBOL:
            reasons.append(
                f"{symbol} has {count} paired scored sessions; "
                f"{WALK_FORWARD_MIN_SESSIONS_PER_SYMBOL} required"
            )

    evidence_ready = not reasons
    if evidence_ready:
        reason = (
            "Evidence-count prerequisites are met. This descriptive no-fit report still does not "
            "run the explicit purged/embargoed evaluator; current coefficients remain unchanged "
            "and promotion remains disabled."
        )
        status = "ready_for_explicit_purged_evaluator"
    else:
        reason = "Walk-forward evidence gate blocked: " + "; ".join(reasons) + "."
        status = "insufficient_evidence"
    return {
        "status": status,
        "executed": False,
        "evidence_gate_passed": evidence_ready,
        "fit_performed": False,
        "promotion_supported": False,
        "independent_session_count": len(trading_dates),
        "paired_scored_independent_session_count": len(paired_dates),
        "paired_scored_sessions_by_symbol": symbol_counts,
        "required_symbols": list(CORE_SYMBOLS),
        "minimum_independent_sessions": WALK_FORWARD_MIN_INDEPENDENT_SESSIONS,
        "minimum_sessions_per_symbol": WALK_FORWARD_MIN_SESSIONS_PER_SYMBOL,
        "minimum_train_sessions": WALK_FORWARD_MIN_TRAIN_SESSIONS,
        "minimum_purged_folds": WALK_FORWARD_MIN_PURGED_FOLDS,
        "purged_folds": 0,
        "embargo_seconds": WALK_FORWARD_EMBARGO_SECONDS,
        "gate_reasons": reasons,
        "reason": reason,
    }


def _integrity_checks(rows: list[dict[str, Any]]) -> dict[str, object]:
    duplicate_keys = Counter(
        (
            str(row["symbol"]), str(row["formula_id"]), str(row["formula_version"]),
            str(row["prediction_timestamp_utc"]), int(row["horizon_seconds"]),
            _process_identity(row),
        )
        for row in rows
    )
    counts = Counter()
    for row in rows:
        event = _parse_timestamp(row.get("ts_event_utc"))
        received = _parse_timestamp(row.get("ts_recv_utc"))
        index = _parse_timestamp(row.get("observation_index_utc"))
        processed = _parse_timestamp(row.get("processed_at_utc"))
        target = _parse_timestamp(row.get("target_timestamp_utc"))
        realized = _parse_timestamp(row.get("realized_timestamp_utc"))
        if event and received and event > received:
            counts["event_after_receive"] += 1
        if received and index and received > index:
            counts["receive_after_index"] += 1
        if index and processed and index > processed:
            counts["index_after_processing"] += 1
        if realized and target and realized < target:
            counts["same_bar_or_early_outcome"] += 1
        snapshot = _safe_json(row.get("feature_snapshot_json"), {})
        asofs = snapshot.get("feature_asof_utc", {}) if isinstance(snapshot, dict) else {}
        if isinstance(asofs, dict) and index:
            for value in asofs.values():
                feature_time = _parse_timestamp(value)
                if feature_time and feature_time > index:
                    counts["lookahead_feature_timestamps"] += 1
        for field in ("spot", "predicted_price", "realized_price", "error_points"):
            value = row.get(field)
            if value is not None and _finite(value) is None:
                counts["nonfinite_numeric_values"] += 1
    return {
        "sqlite_rows": len(rows),
        "missing_or_invalid_process_identity_rows": sum(
            _process_identity(row) is None for row in rows
        ),
        "production_signal_replaced_rows": sum(int(row["production_signal_replaced"] or 0) for row in rows),
        "duplicate_formula_timestamp_keys": sum(count - 1 for count in duplicate_keys.values() if count > 1),
        "event_after_receive_rows": counts["event_after_receive"],
        "receive_after_index_rows": counts["receive_after_index"],
        "index_after_processing_rows": counts["index_after_processing"],
        "same_bar_or_early_outcome_rows": counts["same_bar_or_early_outcome"],
        "lookahead_feature_timestamp_instances": counts["lookahead_feature_timestamps"],
        "nonfinite_numeric_instances": counts["nonfinite_numeric_values"],
    }


def _abstention_counts(rows: list[dict[str, Any]]) -> dict[str, int]:
    reasons: Counter[str] = Counter()
    for row in rows:
        if not int(row["abstained"] or 0):
            continue
        raw = _safe_json(row.get("abstention_reasons_json"), [])
        if isinstance(raw, list):
            reasons.update(str(reason) for reason in raw)
    return dict(reasons.most_common())


def _observation_key(row: dict[str, Any]) -> tuple[str, str, int, str, int] | None:
    identity = _process_identity(row)
    if identity is None:
        return None
    return (
        str(row.get("symbol") or "").upper(),
        str(row.get("prediction_timestamp_utc") or ""),
        int(row.get("horizon_seconds") or 0),
        identity[0],
        identity[1],
    )


def _context_features_from_snapshot(row: dict[str, Any]) -> dict[str, float] | None:
    snapshot = _safe_json(row.get("feature_snapshot_json"), {})
    if not isinstance(snapshot, dict):
        return None
    values = {
        name: _finite(snapshot.get(name))
        for name in (
            "spot", "gamma_pin", "zero_gamma", "gross_gex", "net_gex",
            "top_strike_share", "spot_return_5m",
        )
    }
    if any(value is None for value in values.values()):
        return None
    spot = float(values["spot"])
    gross = float(values["gross_gex"])
    if spot <= 0 or gross <= 0:
        return None
    pin_gap = max(-200.0, min(200.0, 10_000.0 * (float(values["gamma_pin"]) - spot) / spot))
    zero_gap = max(-200.0, min(200.0, 10_000.0 * (float(values["zero_gamma"]) - spot) / spot))
    momentum = max(-100.0, min(100.0, 10_000.0 * float(values["spot_return_5m"])))
    return {
        "spot": spot,
        "pin_gap_bps": pin_gap,
        "zero_gamma_gap_bps": zero_gap,
        "momentum_5m_bps": momentum,
        "pin_concentration_bps": pin_gap * float(values["top_strike_share"]),
        "gex_balance_pin_bps": pin_gap * abs(float(values["net_gex"]) / gross),
    }


def _counterfactual_price(
    features: dict[str, float],
    *,
    omit: frozenset[str] = frozenset(),
) -> float:
    coefficients = {
        "pin_gap_bps": 0.10,
        "zero_gamma_gap_bps": 0.03,
        "momentum_5m_bps": 0.12,
        "pin_concentration_bps": 0.08,
        "gex_balance_pin_bps": 0.05,
    }
    delta_bps = sum(
        coefficient * features[name]
        for name, coefficient in coefficients.items()
        if name not in omit
    )
    delta_bps = max(-75.0, min(75.0, delta_bps))
    return features["spot"] * (1.0 + delta_bps / 10_000.0)


def _chronological_ablation(rows: list[dict[str, Any]]) -> dict[str, object]:
    """Evaluate fixed equations by held-out CT session without fitting.

    The first five sessions are a temporal warm-up. Every later session is a
    separate test fold. Session boundaries provide more than the 300-second
    embargo, and no coefficient is selected or fitted from a held-out fold.
    """
    by_key: dict[tuple[str, str, int, str, int], dict[str, dict[str, Any]]] = defaultdict(dict)
    for row in rows:
        key = _observation_key(row)
        if key is not None:
            by_key[key][str(row.get("formula_id") or "")] = row

    examples: list[dict[str, Any]] = []
    contested: list[dict[str, Any]] = []
    for key, formulas in by_key.items():
        baseline = formulas.get(BASELINE_ID)
        frozen = formulas.get(CANDIDATE_ID)
        if baseline is None or frozen is None:
            continue
        realized = _finite(baseline.get("realized_price"))
        baseline_prediction = _finite(baseline.get("predicted_price"))
        timestamp = _parse_timestamp(baseline.get("prediction_timestamp_utc"))
        features = _context_features_from_snapshot(frozen)
        if realized is None or baseline_prediction is None or timestamp is None or features is None:
            continue
        reasons = _safe_json(frozen.get("abstention_reasons_json"), [])
        reasons = [str(reason) for reason in reasons] if isinstance(reasons, list) else []
        stored_frozen_prediction = _finite(frozen.get("predicted_price"))
        item = {
            "symbol": key[0],
            "session": timestamp.astimezone(CENTRAL).date().isoformat(),
            "realized_price": realized,
            "baseline_price": baseline_prediction,
            "frozen_v1_price": (
                stored_frozen_prediction
                if stored_frozen_prediction is not None
                else _counterfactual_price(features)
            ),
            "v2_no_zero_gamma_price": _counterfactual_price(
                features, omit=frozenset({"zero_gamma_gap_bps"})
            ),
            "no_pin_concentration_price": _counterfactual_price(
                features, omit=frozenset({"pin_concentration_bps"})
            ),
        }
        if int(frozen.get("abstained") or 0) == 0:
            examples.append(item)
        elif reasons and all(reason.startswith("PIN_CONTESTED") for reason in reasons):
            contested.append(item)

    sessions = sorted({str(item["session"]) for item in examples})
    held_out_sessions = sessions[WALK_FORWARD_MIN_TRAIN_SESSIONS:]
    held_out = [item for item in examples if item["session"] in set(held_out_sessions)]

    def metrics(field: str, selected: list[dict[str, Any]]) -> dict[str, object]:
        synthetic = [
            {"predicted_price": item[field], "realized_price": item["realized_price"]}
            for item in selected
        ]
        return _metrics(synthetic)

    folds = []
    for test_session in held_out_sessions:
        selected = [item for item in held_out if item["session"] == test_session]
        folds.append({
            "test_session": test_session,
            "train_sessions": [session for session in sessions if session < test_session],
            "rows": len(selected),
            "baseline": metrics("baseline_price", selected),
            "frozen_v1": metrics("frozen_v1_price", selected),
            "v2_no_zero_gamma": metrics("v2_no_zero_gamma_price", selected),
        })

    status = "insufficient_held_out_sessions"
    if len(held_out_sessions) >= PROMOTION_MIN_HELD_OUT_SESSIONS:
        status = "research_evidence_available_no_automatic_promotion"
    return {
        "executed": True,
        "fit_performed": False,
        "split_unit": "America/Chicago trading session",
        "embargo_seconds": WALK_FORWARD_EMBARGO_SECONDS,
        "warmup_sessions": sessions[:WALK_FORWARD_MIN_TRAIN_SESSIONS],
        "held_out_sessions": held_out_sessions,
        "held_out_session_count": len(held_out_sessions),
        "minimum_held_out_sessions_for_promotion_review": PROMOTION_MIN_HELD_OUT_SESSIONS,
        "status": status,
        "promotion_supported": False,
        "candidate": {
            "formula_id": V2_CANDIDATE_ID,
            "formula_version": V2_CANDIDATE_VERSION,
            "fitted": False,
            "promotion_allowed": False,
        },
        "held_out_metrics": {
            "baseline": metrics("baseline_price", held_out),
            "frozen_v1": metrics("frozen_v1_price", held_out),
            "v2_no_zero_gamma": metrics("v2_no_zero_gamma_price", held_out),
            "no_pin_concentration_ablation": metrics(
                "no_pin_concentration_price", held_out
            ),
        },
        "folds": folds,
        "contested_pin_counterfactual": {
            "live_guardrail_changed": False,
            "rows": len(contested),
            "frozen_v1": metrics("frozen_v1_price", contested),
            "v2_no_zero_gamma": metrics("v2_no_zero_gamma_price", contested),
            "warning": (
                "Offline diagnostic only; contested-pin rows remain live abstentions."
            ),
        },
    }


def evaluate(journal_path: Path) -> dict[str, object]:
    resolved = journal_path.resolve()
    if not resolved.is_file():
        raise FileNotFoundError(resolved)
    uri = f"file:{resolved.as_posix()}?mode=ro"
    with sqlite3.connect(uri, uri=True) as connection:
        connection.row_factory = sqlite3.Row
        quick_check = str(connection.execute("PRAGMA quick_check").fetchone()[0])
        tables = {str(row[0]) for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        if TABLE not in tables:
            raise ValueError(f"missing required table: {TABLE}")
        rows = [dict(row) for row in connection.execute(f"SELECT * FROM {TABLE} ORDER BY prediction_timestamp_utc, formula_id")]
    if not rows:
        raise ValueError("shadow journal is empty")

    formulas: dict[tuple[str, str], dict[str, object]] = {}
    for row in rows:
        key = (str(row["formula_id"]), str(row["formula_version"]))
        formulas[key] = {
            "formula_id": key[0],
            "formula_version": key[1],
            "equation": str(row["equation"]),
            "coefficients": _safe_json(row["coefficients_json"], {}),
            "horizon_seconds": int(row["horizon_seconds"]),
            "mode": str(row["mode"]),
        }
    trading_dates = sorted({
        timestamp.astimezone(CENTRAL).date().isoformat()
        for row in rows
        if _process_identity(row) is not None
        and (timestamp := _parse_timestamp(row.get("prediction_timestamp_utc"))) is not None
    })
    walk_forward_status = _walk_forward_status(rows, trading_dates)
    abstention_reasons = _abstention_counts(rows)
    missing_calculation_id_rows = sum(
        count
        for reason, count in abstention_reasons.items()
        if reason == "MISSING_PROVENANCE:calculation_id"
    )
    insufficient_pair_rows = sum(
        count
        for reason, count in abstention_reasons.items()
        if reason == "INSUFFICIENT_PAIRED_QUOTES"
        or reason.startswith("INSUFFICIENT_PRIMARY_PAIR_COVERAGE")
    )
    backlog_rows = abstention_reasons.get("RECEIVE_TO_PROCESSING_BACKLOG", 0)
    invalid_identity_rows = sum(_process_identity(row) is None for row in rows)
    selection_and_regime_limits = [
        (
            "Only rows with canonical process epoch/generation identity that passed "
            "live guardrails can become evidence; abstentions and legacy epochless "
            f"rows are retained and counted ({invalid_identity_rows} identity-ineligible row(s))."
        ),
        (
            f"The journal contains {len(trading_dates)} independent CT trading session(s); "
            "this report is descriptive and never fits coefficients."
        ),
        "Confidence is a data-quality heuristic, not calibrated forecast probability.",
    ]
    if walk_forward_status["paired_scored_sessions_by_symbol"]["SPX"] == 0:
        selection_and_regime_limits.append(
            "No SPX baseline/candidate pair has a scored outcome in this journal."
        )
    result: dict[str, object] = {
        "artifact_schema_version": "marketpin-shadow-evaluation-v3",
        "generated_at_utc": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "journal_path": str(resolved),
        "journal_physical_sha256": _file_sha256(resolved),
        "journal_wal_sha256": _file_sha256(Path(f"{resolved}-wal")),
        "logical_snapshot_sha256": hashlib.sha256(
            json.dumps(rows, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")
        ).hexdigest(),
        "sqlite_quick_check": quick_check,
        "trading_dates": trading_dates,
        "formulas": list(formulas.values()),
        "integrity_checks": _integrity_checks(rows),
        "formula_symbol_results": _formula_summary(rows),
        "paired_candidate_vs_baseline": _paired_comparison(rows),
        "abstention_reasons": abstention_reasons,
        "walk_forward_validation": walk_forward_status,
        "chronological_candidate_evaluation": _chronological_ablation(rows),
        "provenance_identifier_audit": {
            "historical_rows_missing_calculation_id": missing_calculation_id_rows,
            "backfill_performed": False,
            "current_ingestion_contract": (
                "Lifecycle recording requires a nonblank committed calculation_id and a "
                "positive subscription generation; immutable historical rows are not rewritten."
            ),
        },
        "operational_priorities": [
            {
                "priority": "P1",
                "issue": "quote_pairing",
                "historical_abstention_instances": insufficient_pair_rows,
            },
            {
                "priority": "P1",
                "issue": "receive_to_processing_backlog",
                "historical_abstention_instances": backlog_rows,
            },
        ],
        "selection_and_regime_limits": selection_and_regime_limits,
        "decision": {
            "state": "SHADOW_ONLY",
            "promotion_supported": False,
            "frozen_benchmark": (
                "shadow-pin-context-linear:0.1.0-preregistered"
            ),
            "frozen_benchmark_disposition": "REJECTED_PROMOTION_CANDIDATE",
            "recommendation": (
                "Keep 0.1.0 frozen, repair P1 quote-pairing/backlog coverage, and collect "
                "session-level held-out evidence for the separately versioned 0.2.0 ablation."
            ),
        },
    }
    return result


def _markdown(report: dict[str, object]) -> str:
    lines = [
        "# MarketPin live shadow-formula evaluation",
        "",
        f"Generated: `{report['generated_at_utc']}`",
        "",
        "Decision: **SHADOW ONLY — promotion is not supported.**",
        "",
        "## Formula results",
        "",
        "| Symbol | Formula | Rows | Eligible | Scored | Coverage | MAE | RMSE | Direction |",
        "|---|---|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for item in report["formula_symbol_results"]:
        metrics = item["metrics"]
        coverage = item["eligibility_coverage"]
        coverage_text = f"{coverage:.1%}" if isinstance(coverage, float) else "n/a"
        lines.append(
            f"| {item['symbol']} | `{item['formula_id']}:{item['formula_version']}` | "
            f"{item['total_rows']} | {item['eligible_rows']} | {item['scored_rows']} | "
            f"{coverage_text} | "
        )
        lines[-1] += (
            f"{metrics['mae_points'] if metrics['mae_points'] is not None else 'n/a'} | "
            f"{metrics['rmse_points'] if metrics['rmse_points'] is not None else 'n/a'} | "
            f"{metrics['directional_accuracy'] if metrics['directional_accuracy'] is not None else 'n/a'} |"
        )
    lines.extend(["", "## Paired candidate comparison", ""])
    paired = report["paired_candidate_vs_baseline"]
    if not paired:
        lines.append("No candidate/baseline timestamps were jointly scoreable.")
    for item in paired:
        lines.extend([
            f"- {item['symbol']}: {item['common_scored_timestamps']} common timestamps; "
            f"baseline MAE {item['baseline']['mae_points']}, candidate MAE "
            f"{item['candidate']['mae_points']}, improvement "
            f"{item['candidate_mae_improvement_points']} points, candidate win rate "
            f"{item['candidate_win_rate']:.1%}.",
            f"  {item['promotion_reason']}",
        ])
    checks = report["integrity_checks"]
    lines.extend([
        "",
        "## Point-in-time and persistence checks",
        "",
        f"- SQLite quick check: `{report['sqlite_quick_check']}`",
        f"- Production replacements: {checks['production_signal_replaced_rows']}",
        f"- Duplicate formula/timestamp keys: {checks['duplicate_formula_timestamp_keys']}",
        f"- Event/receive/index/processing ordering failures: "
        f"{checks['event_after_receive_rows'] + checks['receive_after_index_rows'] + checks['index_after_processing_rows']}",
        f"- Feature look-ahead timestamp instances: {checks['lookahead_feature_timestamp_instances']}",
        f"- Realized outcomes earlier than their target horizon: {checks['same_bar_or_early_outcome_rows']}",
        "",
        "## Walk-forward status",
        "",
        report["walk_forward_validation"]["reason"],
        "",
        "## Recommendation",
        "",
        report["decision"]["recommendation"],
        "",
    ])
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--journal", required=True, type=Path)
    parser.add_argument("--output-json", type=Path)
    parser.add_argument("--output-markdown", type=Path)
    args = parser.parse_args(argv)
    report = evaluate(args.journal)
    payload = json.dumps(report, indent=2, sort_keys=True, allow_nan=False)
    if args.output_json:
        args.output_json.parent.mkdir(parents=True, exist_ok=True)
        args.output_json.write_text(payload + "\n", encoding="utf-8")
    if args.output_markdown:
        args.output_markdown.parent.mkdir(parents=True, exist_ok=True)
        args.output_markdown.write_text(_markdown(report), encoding="utf-8")
    print(payload)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
