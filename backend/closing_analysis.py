from __future__ import annotations

import argparse
import hashlib
import json
import math
import sqlite3
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable, Mapping, Sequence


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


UTC = timezone.utc
HORIZON_ID = "cash-close-minus-15m-v1"
FEATURE_SCHEMA_VERSION = "closing-tape-features-1.0"
REQUIRED_TAPE_ROOTS = ("SPX", "NDX", "RUT", "VIX", "SPY")
SCORE_ROOT_WEIGHTS = {"SPX": 0.30, "NDX": 0.25, "RUT": 0.15, "SPY": 0.30}
OBSERVED_FEATURES = (
    "trade_count", "volume", "notional", "call_count", "put_count",
    "call_volume", "put_volume", "call_premium", "put_premium",
)
INFERRED_FEATURES = (
    "at_ask_count", "at_bid_count", "inside_count", "unknown_count",
    "at_ask_volume", "at_bid_volume", "inside_volume", "unknown_volume",
    "at_ask_notional", "at_bid_notional", "inside_notional", "unknown_notional",
    "call_at_ask_notional", "call_at_bid_notional", "put_at_ask_notional",
    "put_at_bid_notional", "classified_notional", "inferred_directional_score",
    "classified_share", "put_call_premium_ratio", "minute_buckets",
)


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _iso(value: datetime) -> str:
    return _utc(value).isoformat()


def _json_canonical(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def _hash(value: object) -> str:
    return hashlib.sha256(_json_canonical(value).encode("utf-8")).hexdigest()


def _finite(value: object) -> float | None:
    try:
        result = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return result if math.isfinite(result) else None


def _parse_ns(value: object) -> datetime | None:
    try:
        number = int(value)
    except (TypeError, ValueError, OverflowError):
        return None
    if not (0 < number < 10**19):
        return None
    return datetime.fromtimestamp(number / 1_000_000_000, tz=UTC)


def _connect(path: Path, *, read_only: bool) -> sqlite3.Connection:
    if read_only:
        connection = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=10.0)
    else:
        connection = sqlite3.connect(path, timeout=10.0)
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("PRAGMA synchronous=NORMAL")
        connection.execute("PRAGMA busy_timeout=10000")
    connection.row_factory = sqlite3.Row
    return connection


def _existing_report(connection: sqlite3.Connection, session_id: str) -> dict[str, object] | None:
    row = connection.execute(
        """
        SELECT report_json FROM closing_analysis_runs
        WHERE session_id=? AND horizon_id=?
        """,
        (session_id, HORIZON_ID),
    ).fetchone()
    if not row:
        return None
    return json.loads(row["report_json"])


def _latest_prediction(
    connection: sqlite3.Connection,
    symbol: str,
    trading_day: date,
    asof_utc: datetime,
) -> dict[str, object] | None:
    asof_naive = _utc(asof_utc).replace(tzinfo=None).isoformat(sep=" ")
    row = connection.execute(
        """
        SELECT * FROM prediction_snapshots
        WHERE symbol=? AND trading_date=? AND timestamp_utc<=?
        ORDER BY timestamp_utc DESC, id DESC LIMIT 1
        """,
        (symbol, trading_day.isoformat(), asof_naive),
    ).fetchone()
    if not row:
        return None
    item = dict(row)
    # Keep only audited fields required by this report.  Raw provider payloads
    # remain in the canonical database and are not copied into an AI prompt.
    return {
        "symbol": symbol,
        "timestamp_utc": item.get("timestamp_utc"),
        "quote_timestamp_utc": item.get("quote_timestamp_utc"),
        "is_valid": bool(item.get("is_valid")),
        "validation_status": item.get("validation_status"),
        "provider": item.get("provider"),
        "model_version": item.get("model_version"),
        "current_price": _finite(item.get("current_price")),
        "predicted_close": _finite(item.get("predicted_close")),
        "confidence": _finite(item.get("confidence")),
        "gamma_pin_strike": _finite(item.get("gamma_pin")),
        "max_pain_strike": _finite(item.get("max_pain")),
        "zero_gamma_modeled_level": _finite(item.get("zero_gamma")),
        "gross_gex": _finite(item.get("gross_gex")),
        "net_gex": _finite(item.get("net_gex")),
        "quote_age_seconds": _finite(item.get("quote_age_seconds")),
        "data_age_seconds": _finite(item.get("data_age_seconds")),
        "subscription_epoch_id": item.get("subscription_epoch_id"),
        "subscription_generation": item.get("subscription_generation"),
        "active_contract_count": item.get("active_contract_count"),
        "fresh_quote_count": item.get("fresh_quote_count"),
    }


def _prediction_age_seconds(prediction: Mapping[str, object], asof_utc: datetime) -> float | None:
    value = prediction.get("timestamp_utc")
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return (_utc(asof_utc) - _utc(parsed)).total_seconds()


def _prediction_process_identity(
    prediction: Mapping[str, object],
) -> tuple[str, int] | None:
    epoch_id = prediction.get("subscription_epoch_id")
    generation_value = prediction.get("subscription_generation")
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


def _sum_features(rows: Sequence[sqlite3.Row]) -> dict[str, float]:
    numeric = (
        "trade_count",
        "volume",
        "notional",
        "call_count",
        "put_count",
        "call_volume",
        "put_volume",
        "call_premium",
        "put_premium",
        "at_ask_count",
        "at_bid_count",
        "inside_count",
        "unknown_count",
        "at_ask_volume",
        "at_bid_volume",
        "inside_volume",
        "unknown_volume",
        "at_ask_notional",
        "at_bid_notional",
        "inside_notional",
        "unknown_notional",
        "call_at_ask_notional",
        "call_at_bid_notional",
        "put_at_ask_notional",
        "put_at_bid_notional",
    )
    result = {column: 0.0 for column in numeric}
    for row in rows:
        for column in numeric:
            result[column] += float(row[column] or 0.0)
    result["classified_notional"] = (
        result["call_at_ask_notional"]
        + result["call_at_bid_notional"]
        + result["put_at_ask_notional"]
        + result["put_at_bid_notional"]
    )
    # Positive means option-flow prints are heuristically aligned with upward
    # underlying exposure: calls at ask / puts at bid.  OPRA does not publish
    # aggressor side, so this remains an inference, not a participant position.
    directional = (
        result["call_at_ask_notional"]
        - result["call_at_bid_notional"]
        - result["put_at_ask_notional"]
        + result["put_at_bid_notional"]
    )
    denominator = result["classified_notional"]
    result["inferred_directional_score"] = directional / denominator if denominator > 0 else 0.0
    result["classified_share"] = denominator / result["notional"] if result["notional"] > 0 else 0.0
    result["put_call_premium_ratio"] = (
        result["put_premium"] / result["call_premium"] if result["call_premium"] > 0 else 0.0
    )
    return result


def _root_window(
    connection: sqlite3.Connection,
    session_id: str,
    root: str,
    start_utc: datetime,
    asof_utc: datetime,
) -> dict[str, float]:
    rows = connection.execute(
        """
        SELECT
            observed.*,
            COALESCE(inferred.at_ask_count, 0) AS at_ask_count,
            COALESCE(inferred.at_bid_count, 0) AS at_bid_count,
            COALESCE(inferred.inside_count, 0) AS inside_count,
            COALESCE(inferred.unknown_count, 0) AS unknown_count,
            COALESCE(inferred.at_ask_volume, 0) AS at_ask_volume,
            COALESCE(inferred.at_bid_volume, 0) AS at_bid_volume,
            COALESCE(inferred.inside_volume, 0) AS inside_volume,
            COALESCE(inferred.unknown_volume, 0) AS unknown_volume,
            COALESCE(inferred.at_ask_notional, 0) AS at_ask_notional,
            COALESCE(inferred.at_bid_notional, 0) AS at_bid_notional,
            COALESCE(inferred.inside_notional, 0) AS inside_notional,
            COALESCE(inferred.unknown_notional, 0) AS unknown_notional,
            COALESCE(inferred.call_at_ask_notional, 0) AS call_at_ask_notional,
            COALESCE(inferred.call_at_bid_notional, 0) AS call_at_bid_notional,
            COALESCE(inferred.put_at_ask_notional, 0) AS put_at_ask_notional,
            COALESCE(inferred.put_at_bid_notional, 0) AS put_at_bid_notional
        FROM tape_observed_minute AS observed
        LEFT JOIN tape_inferred_minute_flow AS inferred
          ON inferred.session_id=observed.session_id
         AND inferred.feed_name=observed.feed_name
         AND inferred.family_root=observed.family_root
         AND inferred.minute_utc=observed.minute_utc
         AND inferred.inference_method='trade_price_vs_pretrade_nbbo'
         AND inferred.inference_version='1.0'
        WHERE observed.session_id=?
          AND observed.feed_name='opra_options'
          AND observed.family_root=?
          AND observed.minute_utc>=?
          AND observed.minute_utc<=?
        ORDER BY observed.minute_utc
        """,
        (
            session_id,
            root,
            _utc(start_utc).replace(second=0, microsecond=0).isoformat(),
            _utc(asof_utc).replace(second=0, microsecond=0).isoformat(),
        ),
    ).fetchall()
    result = _sum_features(rows)
    result["minute_buckets"] = float(len(rows))
    return result


def _open_interest_by_root(connection: sqlite3.Connection, session_id: str) -> dict[str, float]:
    rows = connection.execute(
        """
        SELECT COALESCE(oi.family_root, instruments.family_root) AS family_root,
               SUM(oi.open_interest) AS open_interest
        FROM tape_open_interest oi
        LEFT JOIN tape_instruments instruments
          ON instruments.session_id=oi.session_id
         AND instruments.feed_name=oi.feed_name
         AND instruments.instrument_id=oi.instrument_id
        WHERE oi.session_id=?
        GROUP BY COALESCE(oi.family_root, instruments.family_root)
        """,
        (session_id,),
    ).fetchall()
    return {str(row["family_root"]): float(row["open_interest"] or 0.0) for row in rows if row["family_root"]}


def _load_model_gate(project_root: Path) -> dict[str, object]:
    # Use the same fail-closed verifier as the API/dashboard.  A weaker local
    # interpretation would let post-close reports bypass artifact, family,
    # calibration, regime, or incumbent-baseline evidence.
    from backend.closing_tape.status import _model_gate

    return _model_gate(project_root)


def _market_context(
    market_db_path: Path,
    trading_day: date,
    asof_utc: datetime,
) -> tuple[dict[str, object], dict[str, object], list[str]]:
    reasons: list[str] = []
    context: dict[str, object] = {}
    identities: dict[str, tuple[str, int]] = {}
    if not market_db_path.exists():
        return (
            {},
            {"aligned": False, "identities": {}},
            ["canonical MarketPin database is missing"],
        )
    connection = _connect(market_db_path, read_only=True)
    try:
        for symbol in ("SPX", "NDX"):
            prediction = _latest_prediction(connection, symbol, trading_day, asof_utc)
            if prediction is None:
                reasons.append(f"{symbol} has no point-in-time prediction at or before the as-of time")
                continue
            context[symbol] = prediction
            age = _prediction_age_seconds(prediction, asof_utc)
            if age is None or age > 120:
                reasons.append(f"{symbol} prediction is stale ({age!r} seconds)")
            if not prediction.get("is_valid") or str(prediction.get("validation_status") or "").lower() != "valid":
                reasons.append(f"{symbol} MarketPin prediction is invalid")
            data_age = _finite(prediction.get("data_age_seconds"))
            if data_age is None or data_age > 30:
                reasons.append(f"{symbol} source data age exceeds 30 seconds ({data_age!r})")
            identity = _prediction_process_identity(prediction)
            if identity is None:
                reasons.append(f"{symbol} MarketPin process identity is missing or invalid")
            else:
                identities[symbol] = identity
    finally:
        connection.close()
    aligned = bool(
        len(identities) == 2 and len(set(identities.values())) == 1
    )
    if len(context) == 2 and not aligned:
        reasons.append("SPX/NDX MarketPin process identities are not aligned")
    common_identity = next(iter(identities.values())) if aligned else None
    alignment = {
        "aligned": aligned,
        "subscription_epoch_id": common_identity[0] if common_identity else None,
        "subscription_generation": common_identity[1] if common_identity else None,
        "identities": {
            symbol: {
                "subscription_epoch_id": identity[0],
                "subscription_generation": identity[1],
            }
            for symbol, identity in identities.items()
        },
    }
    return context, alignment, reasons


def _markdown(report: Mapping[str, object]) -> str:
    lines = [
        f"# Closing tape decision support - {report['trading_date']}",
        "",
        f"As of: {report['asof_utc']}",
        f"State: {report['decision_state']}",
        "",
        "This is an audited research artifact, not an individualized recommendation or an executable order.",
        "",
        "## Data quality",
        "",
    ]
    reasons = list(report.get("abstention_reasons") or [])
    if reasons:
        lines.extend(f"- {reason}" for reason in reasons)
    else:
        lines.append("- No tape-integrity gate failed at the as-of time.")
    lines.extend(
        [
            "",
            "## Research-only flow readout",
            "",
            f"- Heuristic label: {report['research_readout']['label']}",
            f"- Last-15-minute inferred score: {report['research_readout']['score_15m']:.4f}",
            f"- Session inferred score: {report['research_readout']['score_session']:.4f}",
            "- Trade-side classifications are inferred from trade price versus pre-trade NBBO; OPRA does not publish aggressor side.",
            "- Zero gamma is an interpolated modeled regime level, not a listed strike and not a close target in this report.",
            "",
            "## Promotion gate",
            "",
            f"- {report['model_gate'].get('reason') or 'Frozen-model evidence gate passed.'}",
            "",
        ]
    )
    return "\n".join(lines)


def run_closing_analysis(
    *,
    tape_catalog_path: str | Path,
    market_db_path: str | Path,
    output_root: str | Path,
    session_id: str,
    trading_day: date,
    asof_utc: datetime,
) -> dict[str, object]:
    tape_path = Path(tape_catalog_path).resolve()
    market_path = Path(market_db_path).resolve()
    output_base = Path(output_root).resolve()
    asof = _utc(asof_utc)
    connection = _connect(tape_path, read_only=False)
    try:
        existing = _existing_report(connection, session_id)
        if existing is not None:
            return existing

        session = connection.execute(
            "SELECT * FROM tape_sessions WHERE session_id=?",
            (session_id,),
        ).fetchone()
        if session is None:
            raise ValueError(f"unknown closing-tape session {session_id}")
        feed_rows = connection.execute(
            "SELECT * FROM tape_feed_status WHERE session_id=? ORDER BY feed_name",
            (session_id,),
        ).fetchall()
        feed_manifest = [dict(row) for row in feed_rows]
        cutoff_rows = connection.execute(
            """
            SELECT * FROM tape_analysis_cutoffs
            WHERE session_id=? AND horizon_id=? ORDER BY feed_name
            """,
            (session_id, HORIZON_ID),
        ).fetchall()
        cutoff_manifest = [dict(row) for row in cutoff_rows]
        reasons: list[str] = []
        required_feed = next((row for row in feed_rows if row["feed_name"] == "opra_options"), None)
        if required_feed is None:
            reasons.append("required OPRA options tape feed is missing")
        else:
            if required_feed["status"] not in {"running", "complete"}:
                reasons.append(f"OPRA tape status is {required_feed['status']}")
            if int(required_feed["trade_records"] or 0) <= 0:
                reasons.append("OPRA tape has no trade records")
            if int(required_feed["reconnect_count"] or 0) > 0:
                reasons.append("OPRA tape has a recorded reconnect gap")
            if int(required_feed["slow_reader_warnings"] or 0) > 0:
                reasons.append("OPRA tape reported slow-reader or skipped-record risk")
            if int(required_feed["provider_error_count"] or 0) > 0:
                reasons.append("OPRA provider emitted an ErrorMsg during capture")
            expected_acks = int(required_feed["expected_subscription_acks"] or 0)
            required_cutoff = next(
                (row for row in cutoff_rows if row["feed_name"] == "opra_options"),
                None,
            )
            if expected_acks > 0 and required_cutoff is None:
                reasons.append("immutable OPRA analysis byte cutoff is missing")
            elif required_cutoff is not None:
                if int(required_cutoff["cutoff_bytes"] or 0) <= 0:
                    reasons.append("immutable OPRA analysis byte cutoff is empty")
                if len(str(required_cutoff["prefix_sha256"] or "")) != 64:
                    reasons.append("immutable OPRA analysis prefix hash is invalid")
                record_sequence = int(required_cutoff["record_sequence"] or 0)
                processed_sequence = int(required_cutoff["processed_sequence"] or 0)
                if record_sequence <= 0 or processed_sequence != record_sequence:
                    reasons.append(
                        "OPRA analysis sequence barrier is missing or incomplete "
                        f"({processed_sequence}/{record_sequence})"
                    )
                try:
                    cutoff_event = datetime.fromisoformat(
                        str(required_cutoff["event_cutoff_utc"]).replace("Z", "+00:00")
                    )
                except (TypeError, ValueError):
                    cutoff_event = None
                    reasons.append("OPRA analysis event cutoff timestamp is invalid")
                try:
                    cutoff_last_trade_ns = int(required_cutoff["last_trade_event_ns"] or 0)
                except (TypeError, ValueError, OverflowError):
                    cutoff_last_trade_ns = 0
                if (
                    cutoff_event is not None
                    and cutoff_last_trade_ns > int(
                        _utc(cutoff_event).timestamp() * 1_000_000_000
                    )
                ):
                    reasons.append("OPRA analysis prefix contains a post-horizon trade event")
            if expected_acks > 0 and int(required_feed["subscription_acks"] or 0) < expected_acks:
                reasons.append("OPRA subscription acknowledgements are incomplete")
            if expected_acks > 0 and int(required_feed["replay_completed"] or 0) < expected_acks:
                reasons.append("OPRA replay has not completed for every requested schema")
            # Before a cutoff exists, a non-empty queue means derived rows lag
            # raw capture. Once the immutable cutoff exists, its exact sequence
            # barrier—not the live post-cutoff queue—is the relevant gate.
            if required_cutoff is None and int(required_feed["callback_queue_depth"] or 0) > 0:
                reasons.append("OPRA derived aggregation queue is not empty")
            trades = int(required_feed["trade_records"] or 0)
            unmapped = int(required_feed["unmapped_trade_records"] or 0)
            if trades > 0 and unmapped / trades > 0.005:
                reasons.append(f"OPRA unmapped trade ratio exceeds 0.5 percent ({unmapped}/{trades})")
            last_trade = _parse_ns(
                required_feed["last_trade_event_ns"] or required_feed["last_event_ns"]
            )
            if last_trade is None or (asof - last_trade).total_seconds() > 120:
                reasons.append("OPRA trade tape has not advanced to within 120 seconds of the as-of time")
            if expected_acks > 0:
                try:
                    root_watermarks = json.loads(required_feed["root_trade_watermarks_json"] or "{}")
                except (TypeError, ValueError, json.JSONDecodeError):
                    root_watermarks = {}
                for root in REQUIRED_TAPE_ROOTS:
                    watermark = _parse_ns(root_watermarks.get(root))
                    allowed_age = 900 if root == "VIX" else 300
                    if watermark is None or (asof - watermark).total_seconds() > allowed_age:
                        reasons.append(f"{root} trade watermark is older than {allowed_age} seconds")

        session_start = datetime.fromisoformat(str(session["cash_open_utc"]).replace("Z", "+00:00"))
        event_cutoff_minute = asof.replace(second=0, microsecond=0)
        final_complete_minute = event_cutoff_minute - timedelta(minutes=1)
        recent_start = event_cutoff_minute - timedelta(minutes=15)
        windows: dict[str, object] = {}
        for root in REQUIRED_TAPE_ROOTS:
            full = _root_window(connection, session_id, root, session_start, final_complete_minute)
            recent = _root_window(connection, session_id, root, recent_start, final_complete_minute)
            windows[root] = {"session": full, "last_15m": recent}
            if full["trade_count"] <= 0:
                reasons.append(f"{root} option trade coverage is missing")

        open_interest = _open_interest_by_root(connection, session_id)
        for root in REQUIRED_TAPE_ROOTS:
            if open_interest.get(root, 0.0) <= 0:
                reasons.append(f"{root} open-interest baseline is missing")

        market_context, market_context_alignment, market_reasons = _market_context(
            market_path, trading_day, asof
        )
        reasons.extend(market_reasons)
        reasons = list(dict.fromkeys(reasons))

        def combined_score(window: str) -> float:
            weighted = 0.0
            present = 0.0
            for root, weight in SCORE_ROOT_WEIGHTS.items():
                root_data = windows[root][window]
                if float(root_data["classified_notional"]) <= 0:
                    continue
                weighted += weight * float(root_data["inferred_directional_score"])
                present += weight
            return weighted / present if present else 0.0

        score_15m = combined_score("last_15m")
        score_session = combined_score("session")
        if score_15m >= 0.15:
            label = "positive-flow tilt"
        elif score_15m <= -0.15:
            label = "negative-flow tilt"
        else:
            label = "mixed or neutral flow"

        project_root = market_path.parent.parent
        model_gate = _load_model_gate(project_root)
        decision_state = "ABSTAIN" if reasons else ("VALIDATED_RESEARCH" if model_gate["passed"] else "RESEARCH_ONLY")
        source_manifest = {
            "session_id": session_id,
            "tape_catalog": str(tape_path),
            "market_database": str(market_path),
            "feeds": feed_manifest,
            "analysis_cutoffs": cutoff_manifest,
        }
        feature_packet = {
            "schema_version": FEATURE_SCHEMA_VERSION,
            "asof_utc": _iso(asof),
            "event_window_end_exclusive_utc": _iso(event_cutoff_minute),
            "windows": windows,
            "open_interest": open_interest,
            "market_context": market_context,
            "market_context_alignment": market_context_alignment,
        }
        feature_hash = _hash(feature_packet)
        observed_windows = {
            root: {
                window_name: {field: values[field] for field in OBSERVED_FEATURES}
                for window_name, values in root_windows.items()
            }
            for root, root_windows in windows.items()
        }
        inferred_windows = {
            root: {
                window_name: {field: values[field] for field in INFERRED_FEATURES}
                for window_name, values in root_windows.items()
            }
            for root, root_windows in windows.items()
        }
        report: dict[str, object] = {
            "horizon_id": HORIZON_ID,
            "feature_schema_version": FEATURE_SCHEMA_VERSION,
            "session_id": session_id,
            "trading_date": trading_day.isoformat(),
            "asof_utc": _iso(asof),
            "created_at_utc": datetime.now(UTC).isoformat(),
            "decision_state": decision_state,
            "use_gate": "NOT_VALIDATED_FOR_LIVE_TRADE_DECISIONS" if not model_gate["passed"] else "DECISION_SUPPORT_ONLY",
            "abstention_reasons": reasons,
            "research_readout": {
                "label": label,
                "score_15m": score_15m,
                "score_session": score_session,
                "confidence_cap": 0.25 if not model_gate["passed"] else 0.60,
                "semantics": "heuristic option-flow estimate; not a participant-position record",
            },
            "observed_trade_features": observed_windows,
            "inferred_positioning": {
                "open_interest_is_daily_published_baseline": True,
                "open_interest_by_root": open_interest,
                "trade_side_is_inferred_from_pre_trade_nbbo": True,
                "flow_by_window": inferred_windows,
            },
            "marketpin_context": market_context,
            "marketpin_context_alignment": market_context_alignment,
            "level_semantics": {
                "gamma_pin_strike": "listed strike selected by the active gamma calculation",
                "max_pain_strike": "listed strike from the options-payout construct",
                "zero_gamma_modeled_level": "continuous linear-interpolation regime crossing; not necessarily a listed strike and not used as a close target here",
            },
            "model_gate": model_gate,
            "feature_hash": feature_hash,
            "source_manifest_hash": _hash(source_manifest),
            "source_manifest": source_manifest,
            "limitations": [
                "Cash SPX, NDX, RUT, and VIX are calculated indexes and do not themselves print trades.",
                "OPRA does not publish aggressor side or participant holdings.",
                "Actual dealer inventory is not observable; positioning fields are explicitly inferred.",
                "No same-day fitting or LLM-generated numeric target is used.",
                "This artifact contains no broker connection or executable order construction.",
            ],
        }

        day_dir = output_base / trading_day.isoformat()
        day_dir.mkdir(parents=True, exist_ok=True)
        stem = f"closing_analysis_{session_id}_{HORIZON_ID}"
        json_path = day_dir / f"{stem}.json"
        markdown_path = day_dir / f"{stem}.md"
        json_path.write_text(json.dumps(report, indent=2, sort_keys=True, default=str), encoding="utf-8")
        markdown_path.write_text(_markdown(report), encoding="utf-8")
        connection.execute(
            """
            INSERT INTO closing_analysis_runs (
                session_id, horizon_id, trading_date, asof_utc, created_at_utc,
                decision_state, abstention_reasons_json, feature_hash,
                source_manifest_json, report_json, json_path, markdown_path
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                session_id,
                HORIZON_ID,
                trading_day.isoformat(),
                _iso(asof),
                str(report["created_at_utc"]),
                decision_state,
                _json_canonical(reasons),
                feature_hash,
                _json_canonical(source_manifest),
                _json_canonical(report),
                str(json_path),
                str(markdown_path),
            ),
        )
        connection.commit()
        return report
    finally:
        connection.close()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Build an audited close-minus-15 MarketPin decision-support report")
    parser.add_argument("--project-root", default=str(Path(__file__).resolve().parents[1]))
    parser.add_argument("--trading-date", required=True)
    parser.add_argument("--session-id")
    parser.add_argument("--asof")
    args = parser.parse_args(argv)
    project_root = Path(args.project_root).resolve()
    trading_day = date.fromisoformat(args.trading_date)
    tape_path = project_root / "data" / "closing_tape" / trading_day.isoformat() / "closing_tape.sqlite"
    connection = _connect(tape_path, read_only=True)
    try:
        session_id = args.session_id
        if not session_id:
            row = connection.execute(
                "SELECT session_id FROM tape_sessions WHERE trading_date=? ORDER BY created_at_utc DESC LIMIT 1",
                (trading_day.isoformat(),),
            ).fetchone()
            if row is None:
                raise SystemExit("No closing-tape session exists for the requested date")
            session_id = str(row["session_id"])
    finally:
        connection.close()
    asof = datetime.fromisoformat(args.asof.replace("Z", "+00:00")) if args.asof else datetime.now(UTC)
    report = run_closing_analysis(
        tape_catalog_path=tape_path,
        market_db_path=project_root / "data" / "market_data.db",
        output_root=project_root / "exports" / "decision_support",
        session_id=session_id,
        trading_day=trading_day,
        asof_utc=asof,
    )
    print(json.dumps({
        "session_id": session_id,
        "decision_state": report["decision_state"],
        "asof_utc": report["asof_utc"],
        "abstention_reasons": report["abstention_reasons"],
    }, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
