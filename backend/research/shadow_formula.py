"""Point-in-time-safe shadow formulas for MarketPin research.

This module is deliberately isolated from the live Databento callback and the
production prediction path.  It does not connect to Databento, read an API key,
place orders, fit itself, or promote a formula.  A caller may pass an already
computed Databento payload into :func:`observation_from_databento_payload`, then
evaluate one of the immutable formula definitions below.

The first candidate is *pre-registered*, not fitted and not production-ready.
Its purpose is to establish an explicit, replayable contract for future
walk-forward validation.
"""

from __future__ import annotations

import hashlib
import json
import math
import sqlite3
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import MappingProxyType
from typing import Any, Iterable, Mapping, Sequence


UTC = timezone.utc
SHADOW_MODE = "shadow"
PERSISTENCE_SCHEMA_VERSION = "shadow-predictions-v1"


def _finite(value: Any) -> float | None:
    if value is None:
        return None
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None


def _utc(value: datetime | None) -> datetime | None:
    """Return an aware UTC timestamp; naive timestamps are intentionally invalid."""
    if value is None or not isinstance(value, datetime) or value.tzinfo is None:
        return None
    return value.astimezone(UTC)


def _iso(value: datetime | None) -> str | None:
    normalized = _utc(value)
    return normalized.isoformat().replace("+00:00", "Z") if normalized else None


def _canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _clip(value: float, lower: float, upper: float) -> float:
    return max(lower, min(upper, value))


def _canonical_subscription_epoch(value: Any) -> str | None:
    if not isinstance(value, str) or len(value) != 64:
        return None
    return value if all(character in "0123456789abcdef" for character in value) else None


@dataclass(frozen=True, kw_only=True)
class FormulaDefinition:
    """An immutable, reviewable formula contract."""

    formula_id: str
    version: str
    description: str
    equation: str
    prediction_horizon_seconds: int
    required_raw_features: tuple[str, ...]
    feature_units: Mapping[str, str]
    transformations: Mapping[str, str]
    coefficients: Mapping[str, float]
    intercept_bps: float = 0.0
    delta_floor_bps: float = -75.0
    delta_ceiling_bps: float = 75.0
    fitted: bool = False
    promotion_allowed: bool = False

    def __post_init__(self) -> None:
        # ``frozen=True`` does not freeze a caller-supplied dict.  Copy and wrap
        # the mappings so a running research process cannot mutate a registered
        # equation after predictions have already been journaled.
        object.__setattr__(self, "feature_units", MappingProxyType(dict(self.feature_units)))
        object.__setattr__(self, "transformations", MappingProxyType(dict(self.transformations)))
        object.__setattr__(self, "coefficients", MappingProxyType(dict(self.coefficients)))

    @property
    def identity(self) -> str:
        return f"{self.formula_id}:{self.version}"


NAIVE_LAST_PRICE_V1 = FormulaDefinition(
    formula_id="shadow-naive-last-price",
    version="1.0.0",
    description="No-change baseline: the horizon price equals the observed spot.",
    equation="predicted_price(t+h) = spot_t; delta_bps = 0",
    prediction_horizon_seconds=300,
    required_raw_features=("spot",),
    feature_units={"spot": "index points"},
    transformations={},
    coefficients={},
    delta_floor_bps=0.0,
    delta_ceiling_bps=0.0,
)


PIN_CONTEXT_LINEAR_V1 = FormulaDefinition(
    formula_id="shadow-pin-context-linear",
    version="0.1.0-preregistered",
    description=(
        "Interpretable five-minute candidate using pin/zero-gamma distances, "
        "five-minute momentum, strike concentration, and the absolute GEX balance."
    ),
    equation=(
        "delta_bps = clip[-75,75](0.00 + 0.10*pin_gap_bps "
        "+ 0.03*zero_gamma_gap_bps + 0.12*momentum_5m_bps "
        "+ 0.08*pin_concentration_bps + 0.05*gex_balance_pin_bps); "
        "predicted_price(t+300s) = spot_t*(1 + delta_bps/10000)"
    ),
    prediction_horizon_seconds=300,
    required_raw_features=(
        "spot",
        "gamma_pin",
        "zero_gamma",
        "gross_gex",
        "net_gex",
        "top_strike_share",
        "spot_return_5m",
    ),
    feature_units={
        "spot": "index points",
        "gamma_pin": "index points",
        "zero_gamma": "index points",
        "gross_gex": "raw gamma*open_interest*100",
        "net_gex": "raw signed gamma*open_interest*100",
        "top_strike_share": "ratio [0,1]",
        "spot_return_5m": "decimal return; 0.001 = 10 bps",
        "pin_gap_bps": "basis points relative to spot",
        "zero_gamma_gap_bps": "basis points relative to spot",
        "momentum_5m_bps": "basis points",
        "pin_concentration_bps": "pin_gap_bps*top_strike_share",
        "gex_balance_pin_bps": "pin_gap_bps*abs(net_gex/gross_gex)",
    },
    transformations={
        "pin_gap_bps": "clip(10000*(gamma_pin-spot)/spot, -200, 200)",
        "zero_gamma_gap_bps": "clip(10000*(zero_gamma-spot)/spot, -200, 200)",
        "momentum_5m_bps": "clip(10000*spot_return_5m, -100, 100)",
        "pin_concentration_bps": "pin_gap_bps*top_strike_share",
        "gex_balance_pin_bps": "pin_gap_bps*abs(net_gex/gross_gex)",
    },
    coefficients={
        "pin_gap_bps": 0.10,
        "zero_gamma_gap_bps": 0.03,
        "momentum_5m_bps": 0.12,
        "pin_concentration_bps": 0.08,
        "gex_balance_pin_bps": 0.05,
    },
)


@dataclass(frozen=True, kw_only=True)
class ShadowGuardrails:
    """Fail-closed thresholds for research scoring.

    Thresholds are research defaults, not claims that a stream is healthy.  A
    later live integration should configure them from measured OPRA throughput.
    """

    allowed_symbols: tuple[str, ...] = ("SPX", "NDX")
    max_quote_age_seconds: float = 10.0
    max_receive_to_process_lag_seconds: float = 5.0
    min_quote_coverage: float = 0.10
    min_fresh_quote_count: int = 50
    min_paired_quote_count: int = 25
    max_bid_ask_spread_bps: float = 250.0
    maximum_event_clock_skew_seconds: float = 0.001


@dataclass(frozen=True, kw_only=True)
class PointInTimeObservation:
    """One feature snapshot indexed by an explicit point-in-time cutoff."""

    symbol: str
    ts_recv_utc: datetime | None
    observation_index_utc: datetime | None
    processed_at_utc: datetime | None
    feature_asof_utc: Mapping[str, datetime | None]
    calculation_id: str | None
    universe_sha256: str | None
    selected_universe_sha256: str | None
    subscription_epoch_id: str | None
    subscription_generation: int | None
    instrument_definition_version: str | None
    symbol_mapping_version: str | None
    ts_event_utc: datetime | None = None
    provider: str = "databento"
    spot: float | None = None
    gamma_pin: float | None = None
    zero_gamma: float | None = None
    gross_gex: float | None = None
    net_gex: float | None = None
    top_strike_share: float | None = None
    spot_return_5m: float | None = None
    coherent_bid: float | None = None
    coherent_ask: float | None = None
    crossed_quote_count: int | None = None
    fresh_quote_count: int | None = None
    expected_quote_count: int | None = None
    paired_quote_count: int | None = None
    quote_age_seconds: float | None = None
    source_confidence: float | None = None
    expiration_days: float | None = None
    validation_is_valid: bool = False
    validation_failure_reasons: tuple[str, ...] = ()
    provenance: Mapping[str, Any] = field(default_factory=dict)

    @property
    def normalized_symbol(self) -> str:
        return str(self.symbol or "").strip().upper()


@dataclass(frozen=True, kw_only=True)
class ShadowPrediction:
    """An auditable result.  Abstentions intentionally have no prediction value."""

    prediction_id: str
    formula: FormulaDefinition
    symbol: str
    prediction_timestamp_utc: datetime
    target_timestamp_utc: datetime
    observation_index_utc: datetime | None
    ts_event_utc: datetime | None
    ts_recv_utc: datetime | None
    processed_at_utc: datetime | None
    receive_to_processing_lag_seconds: float | None
    spot: float | None
    predicted_price: float | None
    predicted_delta_points: float | None
    predicted_delta_bps: float | None
    uncertainty_bps: float | None
    confidence: float
    confidence_kind: str
    abstained: bool
    abstention_reasons: tuple[str, ...]
    raw_feature_snapshot: Mapping[str, Any]
    transformed_features: Mapping[str, float]
    provenance: Mapping[str, Any]
    mode: str = SHADOW_MODE
    production_signal_replaced: bool = False

    def as_dict(self) -> dict[str, Any]:
        return {
            "persistence_schema_version": PERSISTENCE_SCHEMA_VERSION,
            "prediction_id": self.prediction_id,
            "mode": self.mode,
            "production_signal_replaced": self.production_signal_replaced,
            "formula_id": self.formula.formula_id,
            "formula_version": self.formula.version,
            "formula_identity": self.formula.identity,
            "formula_description": self.formula.description,
            "equation": self.formula.equation,
            "coefficients": dict(self.formula.coefficients),
            "transformations": dict(self.formula.transformations),
            "feature_units": dict(self.formula.feature_units),
            "fitted": self.formula.fitted,
            "promotion_allowed": self.formula.promotion_allowed,
            "symbol": self.symbol,
            "prediction_timestamp_utc": _iso(self.prediction_timestamp_utc),
            "prediction_horizon_seconds": self.formula.prediction_horizon_seconds,
            "target_timestamp_utc": _iso(self.target_timestamp_utc),
            "observation_index_utc": _iso(self.observation_index_utc),
            "ts_event_utc": _iso(self.ts_event_utc),
            "ts_recv_utc": _iso(self.ts_recv_utc),
            "processed_at_utc": _iso(self.processed_at_utc),
            "receive_to_processing_lag_seconds": self.receive_to_processing_lag_seconds,
            "spot": self.spot,
            "predicted_price": self.predicted_price,
            "predicted_delta_points": self.predicted_delta_points,
            "predicted_delta_bps": self.predicted_delta_bps,
            "uncertainty_bps": self.uncertainty_bps,
            "confidence": self.confidence,
            "confidence_kind": self.confidence_kind,
            "abstained": self.abstained,
            "abstention_reasons": list(self.abstention_reasons),
            "raw_feature_snapshot": dict(self.raw_feature_snapshot),
            "transformed_features": dict(self.transformed_features),
            "provenance": dict(self.provenance),
            "realized_outcome": None,
        }


class ShadowFormulaEngine:
    """Evaluate immutable formulas without altering any production state."""

    def __init__(self, guardrails: ShadowGuardrails | None = None):
        self.guardrails = guardrails or ShadowGuardrails()

    def evaluate(
        self,
        observation: PointInTimeObservation,
        formula: FormulaDefinition = PIN_CONTEXT_LINEAR_V1,
        *,
        forced_abstention_reasons: Sequence[str] = (),
    ) -> ShadowPrediction:
        reasons = list(forced_abstention_reasons)
        timing = self._validate_timing(observation, reasons)
        self._validate_common_inputs(observation, formula, reasons)

        raw_features = self._raw_feature_snapshot(observation)
        transformed: dict[str, float] = {}
        delta_bps: float | None = None
        predicted_price: float | None = None
        predicted_delta_points: float | None = None
        uncertainty_bps: float | None = None

        if not reasons:
            if formula.identity == NAIVE_LAST_PRICE_V1.identity:
                delta_bps = 0.0
                transformed = {}
            elif formula.identity == PIN_CONTEXT_LINEAR_V1.identity:
                transformed = self._pin_context_features(observation)
                delta_bps = formula.intercept_bps + sum(
                    formula.coefficients[name] * transformed[name]
                    for name in formula.coefficients
                )
                delta_bps = _clip(delta_bps, formula.delta_floor_bps, formula.delta_ceiling_bps)
            else:
                reasons.append(f"UNSUPPORTED_FORMULA:{formula.identity}")

        spot = _finite(observation.spot)
        if not reasons and spot is not None and delta_bps is not None:
            predicted_price = spot * (1.0 + delta_bps / 10_000.0)
            predicted_delta_points = predicted_price - spot
            uncertainty_bps = self._heuristic_uncertainty_bps(observation)

        # De-duplicate while preserving diagnostic order.
        reasons = list(dict.fromkeys(reasons))
        abstained = bool(reasons)
        if abstained:
            predicted_price = None
            predicted_delta_points = None
            delta_bps = None
            uncertainty_bps = None

        # Evaluation time is allowed to exist even when a malformed source
        # snapshot omitted its processing time.  Keep the source value ``None``
        # in the audit record instead of silently replacing it with "now".
        prediction_timestamp = timing["processed"] or datetime.now(UTC)
        target = prediction_timestamp + timedelta(seconds=formula.prediction_horizon_seconds)
        prediction_id = self._prediction_id(observation, formula, prediction_timestamp)
        return ShadowPrediction(
            prediction_id=prediction_id,
            formula=formula,
            symbol=observation.normalized_symbol,
            prediction_timestamp_utc=prediction_timestamp,
            target_timestamp_utc=target,
            observation_index_utc=timing["index"],
            ts_event_utc=timing["event"],
            ts_recv_utc=timing["recv"],
            processed_at_utc=timing["processed"],
            receive_to_processing_lag_seconds=timing["lag"],
            spot=spot,
            predicted_price=predicted_price,
            predicted_delta_points=predicted_delta_points,
            predicted_delta_bps=delta_bps,
            uncertainty_bps=uncertainty_bps,
            confidence=0.0 if abstained else self._data_quality_confidence(observation),
            confidence_kind="data-quality heuristic; not forecast calibration",
            abstained=abstained,
            abstention_reasons=tuple(reasons),
            raw_feature_snapshot=raw_features,
            transformed_features=transformed,
            provenance=self._provenance(observation),
        )

    def evaluate_many(
        self,
        observations: Iterable[PointInTimeObservation],
        formulas: Sequence[FormulaDefinition] = (NAIVE_LAST_PRICE_V1, PIN_CONTEXT_LINEAR_V1),
    ) -> list[ShadowPrediction]:
        """Sort by receive/index time and mark duplicate symbol/index observations.

        Sorting is stable.  An invalid or missing index sorts last and abstains.
        Only the first observation at a symbol/index grain is eligible; later
        duplicates remain in the audit trail as explicit abstentions.
        """
        indexed = list(enumerate(observations))
        far_future = datetime.max.replace(tzinfo=UTC)
        indexed.sort(
            key=lambda pair: (
                _utc(pair[1].observation_index_utc)
                or _utc(pair[1].ts_recv_utc)
                or far_future,
                pair[0],
            )
        )
        seen: set[tuple[str, str | None, int | None, str | None]] = set()
        predictions: list[ShadowPrediction] = []
        for _, observation in indexed:
            index_key = _iso(observation.observation_index_utc) or _iso(observation.ts_recv_utc)
            key = (
                observation.normalized_symbol,
                observation.subscription_epoch_id,
                observation.subscription_generation,
                index_key,
            )
            forced: tuple[str, ...] = ()
            if key in seen:
                forced = ("DUPLICATE_SYMBOL_INDEX_TIMESTAMP",)
            else:
                seen.add(key)
            for formula in formulas:
                predictions.append(
                    self.evaluate(
                        observation,
                        formula,
                        forced_abstention_reasons=forced,
                    )
                )
        return predictions

    def _validate_timing(
        self,
        observation: PointInTimeObservation,
        reasons: list[str],
    ) -> dict[str, datetime | float | None]:
        event = _utc(observation.ts_event_utc)
        recv = _utc(observation.ts_recv_utc)
        index = _utc(observation.observation_index_utc)
        processed = _utc(observation.processed_at_utc)

        if observation.ts_event_utc is not None and event is None:
            reasons.append("NAIVE_OR_INVALID_TS_EVENT")
        if recv is None:
            reasons.append("MISSING_OR_NAIVE_TS_RECV")
        if index is None:
            reasons.append("MISSING_OR_NAIVE_OBSERVATION_INDEX")
        if processed is None:
            reasons.append("MISSING_OR_NAIVE_PROCESSED_AT")
        if event and recv and event > recv + timedelta(seconds=self.guardrails.maximum_event_clock_skew_seconds):
            reasons.append("TS_EVENT_AFTER_TS_RECV")
        if recv and index and recv > index:
            reasons.append("TS_RECV_AFTER_OBSERVATION_INDEX")
        if index and processed and index > processed:
            reasons.append("OBSERVATION_INDEX_AFTER_PREDICTION")

        lag: float | None = None
        if recv and processed:
            lag = (processed - recv).total_seconds()
            if lag < 0:
                reasons.append("NEGATIVE_RECEIVE_TO_PROCESSING_LAG")
            elif lag > self.guardrails.max_receive_to_process_lag_seconds:
                reasons.append("RECEIVE_TO_PROCESSING_BACKLOG")

        if index:
            for feature_name, raw_asof in observation.feature_asof_utc.items():
                asof = _utc(raw_asof)
                if asof is None:
                    reasons.append(f"MISSING_OR_NAIVE_FEATURE_ASOF:{feature_name}")
                elif asof > index:
                    reasons.append(f"LOOKAHEAD_FEATURE:{feature_name}")
        return {"event": event, "recv": recv, "index": index, "processed": processed, "lag": lag}

    def _validate_common_inputs(
        self,
        observation: PointInTimeObservation,
        formula: FormulaDefinition,
        reasons: list[str],
    ) -> None:
        symbol = observation.normalized_symbol
        if symbol not in self.guardrails.allowed_symbols:
            reasons.append(f"UNSUPPORTED_SYMBOL:{symbol or 'EMPTY'}")
        if str(observation.provider or "").lower() != "databento":
            reasons.append("NON_DATABENTO_SOURCE")
        if not observation.validation_is_valid or observation.validation_failure_reasons:
            reasons.append("SOURCE_VALIDATION_FAILED")
            reasons.extend(f"SOURCE:{reason}" for reason in observation.validation_failure_reasons)

        raw = self._raw_values(observation)
        for name in formula.required_raw_features:
            if _finite(raw.get(name)) is None:
                reasons.append(f"MISSING_OR_NONFINITE_FEATURE:{name}")
            if name not in observation.feature_asof_utc:
                reasons.append(f"MISSING_FEATURE_ASOF:{name}")

        spot = _finite(observation.spot)
        if spot is not None and spot <= 0:
            reasons.append("NONPOSITIVE_SPOT")
        crossed = observation.crossed_quote_count
        if crossed is None:
            reasons.append("MISSING_CROSSED_QUOTE_TELEMETRY")
        elif int(crossed) < 0:
            reasons.append("INVALID_CROSSED_QUOTE_COUNT")
        elif int(crossed) > 0:
            reasons.append("CROSSED_QUOTES_PRESENT")

        # A coherent bid/ask is optional for a parity-derived index spot.  When
        # supplied, it must refer to the same instrument and timestamp.  The
        # current backend's ``market_bid``/``market_ask`` are chain medians and
        # must never be passed here as if they were an underlying NBBO.
        bid = _finite(observation.coherent_bid)
        ask = _finite(observation.coherent_ask)
        if (bid is None) != (ask is None):
            reasons.append("INCOMPLETE_COHERENT_BID_ASK")
        elif bid is not None and ask is not None:
            if bid <= 0 or ask <= 0:
                reasons.append("NONPOSITIVE_COHERENT_BID_ASK")
            elif bid > ask:
                reasons.append("CROSSED_COHERENT_BID_ASK")
            elif spot and ((ask - bid) / spot * 10_000.0) > self.guardrails.max_bid_ask_spread_bps:
                reasons.append("EXCESSIVE_COHERENT_BID_ASK_SPREAD")

        quote_age = _finite(observation.quote_age_seconds)
        if quote_age is None:
            reasons.append("MISSING_QUOTE_AGE")
        elif quote_age < 0:
            reasons.append("NEGATIVE_QUOTE_AGE")
        elif quote_age > self.guardrails.max_quote_age_seconds:
            reasons.append("STALE_QUOTES")

        fresh = observation.fresh_quote_count
        expected = observation.expected_quote_count
        paired = observation.paired_quote_count
        if fresh is None or int(fresh) < self.guardrails.min_fresh_quote_count:
            reasons.append("INSUFFICIENT_FRESH_QUOTES")
        if expected is None or int(expected) <= 0:
            reasons.append("MISSING_EXPECTED_QUOTE_COUNT")
        elif fresh is not None:
            if int(fresh) > int(expected):
                reasons.append("FRESH_QUOTE_COUNT_EXCEEDS_EXPECTED")
            elif int(fresh) / int(expected) < self.guardrails.min_quote_coverage:
                reasons.append("INSUFFICIENT_QUOTE_COVERAGE")
        if paired is None or int(paired) < self.guardrails.min_paired_quote_count:
            reasons.append("INSUFFICIENT_PAIRED_QUOTES")

        gross = _finite(observation.gross_gex)
        net = _finite(observation.net_gex)
        if gross is not None:
            if gross <= 0:
                reasons.append("NONPOSITIVE_GROSS_GEX")
            if net is not None and abs(net) > gross + max(1e-9, gross * 1e-9):
                reasons.append("GEX_INVARIANT_FAILED")
        share = _finite(observation.top_strike_share)
        if share is not None and not 0.0 <= share <= 1.0:
            reasons.append("INVALID_TOP_STRIKE_SHARE")
        dte = _finite(observation.expiration_days)
        if dte is None or dte < 0:
            reasons.append("MISSING_OR_INVALID_EXPIRATION_DAYS")

        required_provenance = {
            "calculation_id": observation.calculation_id,
            "universe_sha256": observation.universe_sha256,
            "selected_universe_sha256": observation.selected_universe_sha256,
            "subscription_epoch_id": observation.subscription_epoch_id,
            "subscription_generation": observation.subscription_generation,
            "instrument_definition_version": observation.instrument_definition_version,
            "symbol_mapping_version": observation.symbol_mapping_version,
        }
        for name, value in required_provenance.items():
            if value is None or (isinstance(value, str) and not value.strip()):
                reasons.append(f"MISSING_PROVENANCE:{name}")
        if (
            observation.subscription_epoch_id is not None
            and _canonical_subscription_epoch(observation.subscription_epoch_id) is None
        ):
            reasons.append("INVALID_PROVENANCE:subscription_epoch_id")

    @staticmethod
    def _raw_values(observation: PointInTimeObservation) -> dict[str, Any]:
        return {
            "spot": observation.spot,
            "gamma_pin": observation.gamma_pin,
            "zero_gamma": observation.zero_gamma,
            "gross_gex": observation.gross_gex,
            "net_gex": observation.net_gex,
            "top_strike_share": observation.top_strike_share,
            "spot_return_5m": observation.spot_return_5m,
        }

    def _raw_feature_snapshot(self, observation: PointInTimeObservation) -> dict[str, Any]:
        snapshot = self._raw_values(observation)
        snapshot.update({
            "coherent_bid": observation.coherent_bid,
            "coherent_ask": observation.coherent_ask,
            "crossed_quote_count": observation.crossed_quote_count,
            "fresh_quote_count": observation.fresh_quote_count,
            "expected_quote_count": observation.expected_quote_count,
            "paired_quote_count": observation.paired_quote_count,
            "quote_age_seconds": observation.quote_age_seconds,
            "source_confidence": observation.source_confidence,
            "expiration_days": observation.expiration_days,
            "feature_asof_utc": {
                name: _iso(value) for name, value in observation.feature_asof_utc.items()
            },
        })
        return snapshot

    @staticmethod
    def _pin_context_features(observation: PointInTimeObservation) -> dict[str, float]:
        spot = float(observation.spot)  # validated before this method is called
        pin_gap = _clip(10_000.0 * (float(observation.gamma_pin) - spot) / spot, -200.0, 200.0)
        zero_gap = _clip(10_000.0 * (float(observation.zero_gamma) - spot) / spot, -200.0, 200.0)
        momentum = _clip(10_000.0 * float(observation.spot_return_5m), -100.0, 100.0)
        share = float(observation.top_strike_share)
        balance = abs(float(observation.net_gex) / float(observation.gross_gex))
        return {
            "pin_gap_bps": pin_gap,
            "zero_gamma_gap_bps": zero_gap,
            "momentum_5m_bps": momentum,
            "pin_concentration_bps": pin_gap * share,
            "gex_balance_pin_bps": pin_gap * balance,
        }

    def _data_quality_confidence(self, observation: PointInTimeObservation) -> float:
        fresh = float(observation.fresh_quote_count or 0)
        expected = float(observation.expected_quote_count or 1)
        coverage = _clip(fresh / expected, 0.0, 1.0)
        source = _clip(_finite(observation.source_confidence) or 0.0, 0.0, 1.0)
        age_ratio = _clip(float(observation.quote_age_seconds or 0.0) / self.guardrails.max_quote_age_seconds, 0.0, 1.0)
        recv = _utc(observation.ts_recv_utc)
        processed = _utc(observation.processed_at_utc)
        lag = max(0.0, (processed - recv).total_seconds()) if recv and processed else self.guardrails.max_receive_to_process_lag_seconds
        lag_ratio = _clip(lag / self.guardrails.max_receive_to_process_lag_seconds, 0.0, 1.0)
        # Intentionally capped below 0.70 because the candidate is unvalidated.
        return round(_clip(0.15 + 0.25 * coverage + 0.15 * source - 0.10 * age_ratio - 0.05 * lag_ratio, 0.0, 0.65), 4)

    def _heuristic_uncertainty_bps(self, observation: PointInTimeObservation) -> float:
        fresh = float(observation.fresh_quote_count or 0)
        expected = float(observation.expected_quote_count or 1)
        coverage = _clip(fresh / expected, 0.0, 1.0)
        age_ratio = _clip(float(observation.quote_age_seconds or 0.0) / self.guardrails.max_quote_age_seconds, 0.0, 1.0)
        recv = _utc(observation.ts_recv_utc)
        processed = _utc(observation.processed_at_utc)
        lag = max(0.0, (processed - recv).total_seconds()) if recv and processed else self.guardrails.max_receive_to_process_lag_seconds
        lag_ratio = _clip(lag / self.guardrails.max_receive_to_process_lag_seconds, 0.0, 1.0)
        # This is an explicit operational uncertainty heuristic, not a calibrated interval.
        return round(12.0 + 20.0 * (1.0 - coverage) + 8.0 * age_ratio + 4.0 * lag_ratio, 4)

    @staticmethod
    def _provenance(observation: PointInTimeObservation) -> dict[str, Any]:
        return {
            "provider": observation.provider,
            "calculation_id": observation.calculation_id,
            "universe_sha256": observation.universe_sha256,
            "selected_universe_sha256": observation.selected_universe_sha256,
            "subscription_epoch_id": observation.subscription_epoch_id,
            "subscription_generation": observation.subscription_generation,
            "instrument_definition_version": observation.instrument_definition_version,
            "symbol_mapping_version": observation.symbol_mapping_version,
            **dict(observation.provenance),
        }

    @staticmethod
    def _prediction_id(
        observation: PointInTimeObservation,
        formula: FormulaDefinition,
        processed: datetime,
    ) -> str:
        identity = "|".join((
            formula.identity,
            observation.normalized_symbol,
            _iso(processed) or "missing-time",
            str(formula.prediction_horizon_seconds),
            str(observation.calculation_id or "missing-calculation"),
            str(observation.subscription_epoch_id or "missing-epoch"),
            str(observation.subscription_generation or "missing-generation"),
        ))
        return hashlib.sha256(identity.encode("utf-8")).hexdigest()


def observation_from_databento_payload(
    payload: Mapping[str, Any],
    *,
    ts_recv_utc: datetime | None,
    observation_index_utc: datetime | None,
    processed_at_utc: datetime | None,
    feature_asof_utc: Mapping[str, datetime | None],
    spot_return_5m: float | None,
    trusted_quote_age_seconds: float | None,
    fresh_quote_count: int | None,
    paired_quote_count: int | None,
    crossed_quote_count: int | None,
    expected_quote_count: int | None,
    instrument_definition_version: str | None,
    symbol_mapping_version: str | None,
    ts_event_utc: datetime | None = None,
    coherent_bid: float | None = None,
    coherent_ask: float | None = None,
) -> PointInTimeObservation:
    """Adapt an existing backend payload without opening another data stream.

    Timing, mapping/definition versions, momentum, coverage counts, and quote
    age and crossed-record telemetry must be supplied by the caller rather than
    guessed.  In particular, the
    current aggregate backend payload can report a hard-coded zero quote age;
    this adapter intentionally ignores it.  A replay caller should derive the
    maximum contributing-row age, while a future live caller should use the
    latest contributing receive timestamp.  If either is unavailable, passing
    ``None`` preserves that fact and the engine abstains.
    """
    provenance = payload.get("universe_provenance")
    if not isinstance(provenance, Mapping):
        provenance = {}
    return PointInTimeObservation(
        symbol=str(payload.get("symbol") or ""),
        provider=str(payload.get("provider") or "databento"),
        ts_event_utc=ts_event_utc,
        ts_recv_utc=ts_recv_utc,
        observation_index_utc=observation_index_utc,
        processed_at_utc=processed_at_utc,
        feature_asof_utc=feature_asof_utc,
        calculation_id=str(payload.get("calculation_id")) if payload.get("calculation_id") else None,
        universe_sha256=str(payload.get("universe_sha256")) if payload.get("universe_sha256") else None,
        selected_universe_sha256=(
            str(payload.get("selected_universe_sha256"))
            if payload.get("selected_universe_sha256") else None
        ),
        subscription_epoch_id=(
            str(payload.get("subscription_epoch_id"))
            if payload.get("subscription_epoch_id") else None
        ),
        subscription_generation=(
            int(payload["subscription_generation"])
            if payload.get("subscription_generation") is not None else None
        ),
        instrument_definition_version=instrument_definition_version,
        symbol_mapping_version=symbol_mapping_version,
        spot=_finite(payload.get("price") if payload.get("price") is not None else payload.get("spot_last")),
        gamma_pin=_finite(payload.get("gamma_pin") if payload.get("gamma_pin") is not None else payload.get("gamma_pin_strike")),
        zero_gamma=_finite(payload.get("zero_gamma") if payload.get("zero_gamma") is not None else payload.get("zero_gamma_level")),
        gross_gex=_finite(payload.get("gross_gex")),
        net_gex=_finite(payload.get("net_gex")),
        top_strike_share=_finite(payload.get("top_strike_share")),
        spot_return_5m=_finite(spot_return_5m),
        # Never adapt payload ``market_bid``/``market_ask``: those fields are
        # medians across option rows, not a coherent underlying NBBO.
        coherent_bid=_finite(coherent_bid),
        coherent_ask=_finite(coherent_ask),
        crossed_quote_count=(
            int(crossed_quote_count) if crossed_quote_count is not None else None
        ),
        fresh_quote_count=int(fresh_quote_count) if fresh_quote_count is not None else None,
        expected_quote_count=int(expected_quote_count) if expected_quote_count is not None else None,
        paired_quote_count=int(paired_quote_count) if paired_quote_count is not None else None,
        quote_age_seconds=_finite(trusted_quote_age_seconds),
        source_confidence=_finite(payload.get("confidence")),
        expiration_days=_finite(payload.get("expirations_min_days")),
        validation_is_valid=bool(payload.get("validation_is_valid", False)),
        validation_failure_reasons=tuple(str(reason) for reason in (payload.get("validation_failure_reasons") or ())),
        provenance=dict(provenance),
    )


class ShadowPredictionJournal:
    """Additive SQLite journal isolated from the production ``market.db``.

    Constructing this class does not touch the database.  The schema is created
    only by :meth:`save`.  Callers should point it at ``data/shadow_research.db``
    or another explicitly research-only path.
    """

    def __init__(self, path: str | Path):
        self.path = Path(path)

    def _connect(self) -> sqlite3.Connection:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(self.path, timeout=10.0)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("PRAGMA synchronous=NORMAL")
        connection.execute("PRAGMA busy_timeout=10000")
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS shadow_prediction_journal (
                prediction_id TEXT PRIMARY KEY,
                schema_version TEXT NOT NULL,
                mode TEXT NOT NULL CHECK (mode = 'shadow'),
                production_signal_replaced INTEGER NOT NULL CHECK (production_signal_replaced = 0),
                formula_id TEXT NOT NULL,
                formula_version TEXT NOT NULL,
                symbol TEXT NOT NULL,
                prediction_timestamp_utc TEXT NOT NULL,
                horizon_seconds INTEGER NOT NULL,
                target_timestamp_utc TEXT NOT NULL,
                observation_index_utc TEXT,
                ts_event_utc TEXT,
                ts_recv_utc TEXT,
                processed_at_utc TEXT,
                receive_to_processing_lag_seconds REAL,
                spot REAL,
                predicted_price REAL,
                predicted_delta_points REAL,
                predicted_delta_bps REAL,
                uncertainty_bps REAL,
                confidence REAL NOT NULL,
                confidence_kind TEXT NOT NULL,
                abstained INTEGER NOT NULL,
                abstention_reasons_json TEXT NOT NULL,
                equation TEXT NOT NULL,
                coefficients_json TEXT NOT NULL,
                feature_snapshot_json TEXT NOT NULL,
                transformed_features_json TEXT NOT NULL,
                provenance_json TEXT NOT NULL,
                record_sha256 TEXT NOT NULL,
                realized_price REAL,
                realized_timestamp_utc TEXT,
                error_points REAL,
                absolute_error_points REAL,
                squared_error_points REAL,
                direction_hit INTEGER,
                scored_at_utc TEXT,
                created_at_utc TEXT NOT NULL
            )
            """
        )
        connection.execute(
            "CREATE INDEX IF NOT EXISTS idx_shadow_formula_symbol_time "
            "ON shadow_prediction_journal(formula_id, formula_version, symbol, prediction_timestamp_utc)"
        )
        return connection

    def save(self, prediction: ShadowPrediction) -> bool:
        payload = prediction.as_dict()
        record_sha256 = hashlib.sha256(_canonical_json(payload).encode("utf-8")).hexdigest()
        now = _iso(datetime.now(UTC))
        values = (
            prediction.prediction_id,
            PERSISTENCE_SCHEMA_VERSION,
            prediction.mode,
            int(prediction.production_signal_replaced),
            prediction.formula.formula_id,
            prediction.formula.version,
            prediction.symbol,
            _iso(prediction.prediction_timestamp_utc),
            prediction.formula.prediction_horizon_seconds,
            _iso(prediction.target_timestamp_utc),
            _iso(prediction.observation_index_utc),
            _iso(prediction.ts_event_utc),
            _iso(prediction.ts_recv_utc),
            _iso(prediction.processed_at_utc),
            prediction.receive_to_processing_lag_seconds,
            prediction.spot,
            prediction.predicted_price,
            prediction.predicted_delta_points,
            prediction.predicted_delta_bps,
            prediction.uncertainty_bps,
            prediction.confidence,
            prediction.confidence_kind,
            int(prediction.abstained),
            _canonical_json(list(prediction.abstention_reasons)),
            prediction.formula.equation,
            _canonical_json(dict(prediction.formula.coefficients)),
            _canonical_json(dict(prediction.raw_feature_snapshot)),
            _canonical_json(dict(prediction.transformed_features)),
            _canonical_json(dict(prediction.provenance)),
            record_sha256,
            now,
        )
        with self._connect() as connection:
            cursor = connection.execute(
                """
                INSERT OR IGNORE INTO shadow_prediction_journal (
                    prediction_id, schema_version, mode, production_signal_replaced,
                    formula_id, formula_version, symbol, prediction_timestamp_utc,
                    horizon_seconds, target_timestamp_utc, observation_index_utc,
                    ts_event_utc, ts_recv_utc, processed_at_utc,
                    receive_to_processing_lag_seconds, spot, predicted_price,
                    predicted_delta_points, predicted_delta_bps, uncertainty_bps,
                    confidence, confidence_kind, abstained, abstention_reasons_json,
                    equation, coefficients_json, feature_snapshot_json,
                    transformed_features_json, provenance_json, record_sha256,
                    created_at_utc
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                values,
            )
            return cursor.rowcount == 1

    def score_outcome(
        self,
        prediction_id: str,
        *,
        realized_price: float,
        realized_timestamp_utc: datetime,
        scored_at_utc: datetime | None = None,
    ) -> dict[str, Any]:
        """Attach an outcome only at/after the registered horizon cutoff."""
        price = _finite(realized_price)
        realized_at = _utc(realized_timestamp_utc)
        scored_at = _utc(scored_at_utc or datetime.now(UTC))
        if price is None or price <= 0:
            raise ValueError("realized_price must be finite and positive")
        if realized_at is None:
            raise ValueError("realized_timestamp_utc must be timezone-aware")
        if scored_at is None:
            raise ValueError("scored_at_utc must be timezone-aware")

        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM shadow_prediction_journal WHERE prediction_id = ?",
                (prediction_id,),
            ).fetchone()
            if row is None:
                raise KeyError(prediction_id)
            if row["realized_price"] is not None:
                raise ValueError("realized outcome is already recorded and immutable")
            target_at = datetime.fromisoformat(str(row["target_timestamp_utc"]).replace("Z", "+00:00"))
            if realized_at < target_at:
                raise ValueError("realized outcome precedes the registered target timestamp")
            if int(row["abstained"]):
                raise ValueError("an abstained prediction has no forecast to score")

            predicted = float(row["predicted_price"])
            spot = float(row["spot"])
            error = predicted - price
            predicted_direction = math.copysign(1.0, predicted - spot) if predicted != spot else 0.0
            realized_direction = math.copysign(1.0, price - spot) if price != spot else 0.0
            direction_hit = predicted_direction == realized_direction
            connection.execute(
                """
                UPDATE shadow_prediction_journal
                SET realized_price = ?, realized_timestamp_utc = ?,
                    error_points = ?, absolute_error_points = ?, squared_error_points = ?,
                    direction_hit = ?, scored_at_utc = ?
                WHERE prediction_id = ?
                """,
                (
                    price,
                    _iso(realized_at),
                    error,
                    abs(error),
                    error * error,
                    int(direction_hit),
                    _iso(scored_at),
                    prediction_id,
                ),
            )
            return {
                "prediction_id": prediction_id,
                "realized_price": price,
                "realized_timestamp_utc": _iso(realized_at),
                "error_points": error,
                "absolute_error_points": abs(error),
                "squared_error_points": error * error,
                "direction_hit": direction_hit,
            }

    def fetch(self, prediction_id: str) -> dict[str, Any] | None:
        if not self.path.exists():
            return None
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM shadow_prediction_journal WHERE prediction_id = ?",
                (prediction_id,),
            ).fetchone()
            return dict(row) if row else None

    def has_observation_index(
        self,
        symbol: str,
        observation_index_utc: datetime,
        *,
        subscription_epoch_id: str,
        subscription_generation: int,
    ) -> bool:
        """Return whether this exact process already captured the symbol/index."""
        if not self.path.exists():
            return False
        index_value = _iso(observation_index_utc)
        epoch_id = _canonical_subscription_epoch(subscription_epoch_id)
        try:
            generation = int(subscription_generation)
        except (TypeError, ValueError, OverflowError):
            generation = 0
        if index_value is None or epoch_id is None or generation <= 0:
            return False
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT 1
                FROM shadow_prediction_journal
                WHERE symbol = ? AND observation_index_utc = ?
                  AND json_extract(provenance_json, '$.subscription_epoch_id') = ?
                  AND json_extract(provenance_json, '$.subscription_generation') = ?
                LIMIT 1
                """,
                (
                    str(symbol or "").strip().upper(),
                    index_value,
                    epoch_id,
                    generation,
                ),
            ).fetchone()
            return row is not None

    def fetch_due_unscored(
        self,
        symbol: str,
        realized_timestamp_utc: datetime,
        *,
        subscription_epoch_id: str,
        subscription_generation: int,
    ) -> list[dict[str, Any]]:
        """Return due predictions from the same process identity only."""
        if not self.path.exists():
            return []
        realized_value = _iso(realized_timestamp_utc)
        epoch_id = _canonical_subscription_epoch(subscription_epoch_id)
        try:
            generation = int(subscription_generation)
        except (TypeError, ValueError, OverflowError):
            generation = 0
        if realized_value is None or epoch_id is None or generation <= 0:
            return []
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT *
                FROM shadow_prediction_journal
                WHERE symbol = ?
                  AND abstained = 0
                  AND realized_price IS NULL
                  AND target_timestamp_utc <= ?
                  AND json_extract(provenance_json, '$.subscription_epoch_id') = ?
                  AND json_extract(provenance_json, '$.subscription_generation') = ?
                ORDER BY target_timestamp_utc, prediction_id
                """,
                (
                    str(symbol or "").strip().upper(),
                    realized_value,
                    epoch_id,
                    generation,
                ),
            ).fetchall()
            return [dict(row) for row in rows]
