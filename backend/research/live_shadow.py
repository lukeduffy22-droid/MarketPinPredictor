"""Shadow-only live recorder fed by the existing Databento calculation callback.

This module never opens a market-data connection and never writes to the
production prediction tables.  It adapts the already-computed point-in-time
payload, evaluates immutable research formulas, and journals either a shadow
prediction or an explicit abstention in a separate SQLite database.
"""

from __future__ import annotations

import logging
import os
import threading
from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

from backend.optional_family_canary import CORE_PRIMARY_PAIR_COVERAGE_RATIO
from backend.research.shadow_formula import (
    NAIVE_LAST_PRICE_V1,
    PIN_CONTEXT_LINEAR_V1,
    PIN_CONTEXT_NO_ZERO_GAMMA_V2,
    FormulaDefinition,
    ShadowFormulaEngine,
    ShadowGuardrails,
    ShadowPrediction,
    ShadowPredictionJournal,
    observation_from_databento_payload,
)


UTC = timezone.utc
logger = logging.getLogger(__name__)
DEFAULT_JOURNAL_PATH = Path(__file__).resolve().parents[2] / "data" / "shadow_research.db"
MIN_PRIMARY_PAIR_COVERAGE_RATIO = CORE_PRIMARY_PAIR_COVERAGE_RATIO


def _env_enabled(name: str, default: bool = True) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _parse_utc(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str) and value.strip():
        try:
            parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
        except ValueError:
            return None
    else:
        return None
    if parsed.tzinfo is None:
        return None
    return parsed.astimezone(UTC)


def _positive_float(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if number > 0 else None


def _optional_int(value: Any) -> int | None:
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _process_identity(payload: Mapping[str, Any]) -> tuple[str, int] | None:
    epoch_id = str(payload.get("subscription_epoch_id") or "").strip()
    raw_generation = payload.get("subscription_generation")
    if isinstance(raw_generation, bool):
        generation = None
    elif isinstance(raw_generation, int):
        generation = raw_generation
    elif isinstance(raw_generation, str) and raw_generation.strip().isdigit():
        generation = int(raw_generation.strip())
    else:
        generation = None
    calculation_id = payload.get("calculation_id")
    if (
        not isinstance(calculation_id, str)
        or not calculation_id.strip()
        or len(epoch_id) != 64
        or any(character not in "0123456789abcdef" for character in epoch_id)
        or generation is None
        or generation <= 0
    ):
        return None
    return epoch_id, generation


@dataclass(frozen=True)
class LiveShadowRecordResult:
    predictions: tuple[ShadowPrediction, ...] = ()
    saved_count: int = 0
    scored_count: int = 0
    skipped_reason: str | None = None
    error: str | None = None


class LiveShadowResearchRecorder:
    """Evaluate and persist isolated five-minute shadow research records."""

    def __init__(
        self,
        *,
        journal_path: str | Path | None = None,
        enabled: bool | None = None,
        formulas: Sequence[FormulaDefinition] = (
            NAIVE_LAST_PRICE_V1,
            PIN_CONTEXT_LINEAR_V1,
            PIN_CONTEXT_NO_ZERO_GAMMA_V2,
        ),
        max_quote_age_seconds: float = 30.0,
    ) -> None:
        configured_path = journal_path or os.getenv("SHADOW_RESEARCH_DB_PATH") or DEFAULT_JOURNAL_PATH
        self.enabled = _env_enabled("SHADOW_RESEARCH_ENABLED", True) if enabled is None else bool(enabled)
        self.formulas = tuple(formulas)
        self.engine = ShadowFormulaEngine(
            ShadowGuardrails(max_quote_age_seconds=float(max_quote_age_seconds))
        )
        self.journal = ShadowPredictionJournal(configured_path)
        self._history: dict[
            tuple[str, str, int], deque[tuple[datetime, float]]
        ] = defaultdict(
            lambda: deque(maxlen=120)
        )
        self._lock = threading.RLock()

    def record(
        self,
        payload: Mapping[str, Any],
        *,
        processed_at_utc: datetime | None = None,
        forced_abstention_reasons: Sequence[str] = (),
    ) -> LiveShadowRecordResult:
        """Journal formulas or abstentions without propagating research errors."""
        if not self.enabled:
            return LiveShadowRecordResult(skipped_reason="SHADOW_RESEARCH_DISABLED")
        try:
            with self._lock:
                return self._record_locked(
                    payload,
                    processed_at_utc=processed_at_utc,
                    forced_abstention_reasons=forced_abstention_reasons,
                )
        except Exception as exc:  # production callback must never fail on research work
            logger.warning("Shadow research recorder failed closed: %s", exc)
            return LiveShadowRecordResult(error=f"{type(exc).__name__}: {exc}")

    def _record_locked(
        self,
        payload: Mapping[str, Any],
        *,
        processed_at_utc: datetime | None,
        forced_abstention_reasons: Sequence[str],
    ) -> LiveShadowRecordResult:
        symbol = str(payload.get("symbol") or "").strip().upper()
        event_at = _parse_utc(payload.get("latest_ts_event_utc"))
        recv_at = _parse_utc(payload.get("latest_ts_recv_utc"))
        index_at = _parse_utc(payload.get("observation_index_utc"))
        processed_at = _parse_utc(processed_at_utc or datetime.now(UTC))
        spot = _positive_float(
            payload.get("price") if payload.get("price") is not None else payload.get("spot_last")
        )
        process_identity = _process_identity(payload)
        if process_identity is None:
            return LiveShadowRecordResult(
                skipped_reason="MISSING_COMMITTED_PROVENANCE_IDENTITY"
            )

        momentum = self._spot_return(symbol, process_identity, index_at, spot)
        required_features = {
            feature
            for formula in self.formulas
            for feature in formula.required_raw_features
        }
        feature_asof = {feature: index_at for feature in required_features}

        reasons = list(forced_abstention_reasons)
        if _optional_int(payload.get("provider_timestamp_order_errors")) not in (None, 0):
            reasons.append("SOURCE_PROVIDER_TIMESTAMP_ORDER_ERRORS")
        if (_optional_int(payload.get("mapping_version_missing_count")) or 0) > 0:
            reasons.append("INCOMPLETE_POINT_IN_TIME_SYMBOL_MAPPINGS")
        pair_coverage = payload.get("primary_pair_coverage_ratio")
        if pair_coverage is not None:
            try:
                coverage_value = float(pair_coverage)
            except (TypeError, ValueError):
                coverage_value = -1.0
            if coverage_value < MIN_PRIMARY_PAIR_COVERAGE_RATIO:
                reasons.append(
                    "INSUFFICIENT_PRIMARY_PAIR_COVERAGE: "
                    f"{coverage_value:.4f} < {MIN_PRIMARY_PAIR_COVERAGE_RATIO:.4f}"
                )
        else:
            reasons.append("PRIMARY_PAIR_COVERAGE_UNKNOWN")
        clock_status = str(
            (payload.get("processing_clock_telemetry") or {}).get("status") or ""
        ).strip().lower()
        if clock_status != "synchronized":
            reasons.append(
                "PROCESSING_CLOCK_UNSYNCHRONIZED"
                if clock_status == "unsynchronized"
                else "PROCESSING_CLOCK_UNKNOWN"
            )

        observation = observation_from_databento_payload(
            payload,
            ts_event_utc=event_at,
            ts_recv_utc=recv_at,
            observation_index_utc=index_at,
            processed_at_utc=processed_at,
            feature_asof_utc=feature_asof,
            spot_return_5m=momentum,
            # Use the maximum age of every row that actually contributed to
            # the calculation, not the stream's latest-message age.
            trusted_quote_age_seconds=payload.get("quote_age_max_seconds"),
            fresh_quote_count=_optional_int(payload.get("fresh_quote_count")),
            paired_quote_count=_optional_int(payload.get("paired_quote_count")),
            crossed_quote_count=_optional_int(payload.get("contributing_crossed_quote_count")),
            expected_quote_count=_optional_int(payload.get("expected_quote_count")),
            instrument_definition_version=(
                str(payload.get("instrument_definition_version"))
                if payload.get("instrument_definition_version") else None
            ),
            symbol_mapping_version=(
                str(payload.get("symbol_mapping_version"))
                if payload.get("symbol_mapping_version") else None
            ),
        )

        if (
            index_at is not None
            and process_identity is not None
            and self.journal.has_observation_index(
                symbol,
                index_at,
                subscription_epoch_id=process_identity[0],
                subscription_generation=process_identity[1],
            )
        ):
            reasons.append("DUPLICATE_SYMBOL_INDEX_TIMESTAMP")

        scored_count = self._score_due(
            symbol,
            index_at,
            spot,
            process_identity=process_identity,
        )
        predictions_list: list[ShadowPrediction] = []
        for formula in self.formulas:
            formula_reasons = list(reasons)
            if bool(payload.get("pin_is_contested")) and "gamma_pin" in formula.required_raw_features:
                formula_reasons.append(
                    str(payload.get("pin_competition_reason") or "PIN_CONTESTED")
                )
            predictions_list.append(
                self.engine.evaluate(
                    observation,
                    formula,
                    forced_abstention_reasons=tuple(formula_reasons),
                )
            )
        predictions = tuple(predictions_list)
        saved_count = sum(1 for prediction in predictions if self.journal.save(prediction))

        if index_at is not None and spot is not None and process_identity is not None:
            self._history[(symbol, *process_identity)].append((index_at, spot))
        return LiveShadowRecordResult(
            predictions=predictions,
            saved_count=saved_count,
            scored_count=scored_count,
        )

    def _spot_return(
        self,
        symbol: str,
        process_identity: tuple[str, int] | None,
        index_at: datetime | None,
        spot: float | None,
    ) -> float | None:
        if index_at is None or spot is None or process_identity is None:
            return None
        cutoff = index_at - timedelta(seconds=PIN_CONTEXT_LINEAR_V1.prediction_horizon_seconds)
        eligible = [
            point
            for point in self._history[(symbol, *process_identity)]
            if point[0] <= cutoff
        ]
        if not eligible:
            return None
        prior_spot = eligible[-1][1]
        return (spot / prior_spot) - 1.0 if prior_spot > 0 else None

    def _score_due(
        self,
        symbol: str,
        realized_at: datetime | None,
        realized_price: float | None,
        *,
        process_identity: tuple[str, int] | None,
    ) -> int:
        if realized_at is None or realized_price is None or process_identity is None:
            return 0
        scored = 0
        for row in self.journal.fetch_due_unscored(
            symbol,
            realized_at,
            subscription_epoch_id=process_identity[0],
            subscription_generation=process_identity[1],
        ):
            try:
                self.journal.score_outcome(
                    str(row["prediction_id"]),
                    realized_price=realized_price,
                    realized_timestamp_utc=realized_at,
                    scored_at_utc=datetime.now(UTC),
                )
                scored += 1
            except (KeyError, ValueError) as exc:
                logger.warning("Shadow outcome scoring skipped for %s: %s", row.get("prediction_id"), exc)
        return scored
