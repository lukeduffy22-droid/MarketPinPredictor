"""Pure, versioned 0DTE structural heuristic. Scores are not probabilities."""
from __future__ import annotations

from dataclasses import asdict, dataclass
import math
from numbers import Real
from typing import Literal

MODEL_VERSION = "structural-regime:1.0.0"
GEX_UNIT = "raw_gamma_x_open_interest_x_100"


def finite_number(value: object, name: str, *, positive: bool = False) -> float:
    if isinstance(value, bool) or not isinstance(value, Real):
        raise ValueError(f"{name}_must_be_numeric")
    number = float(value)
    if not math.isfinite(number) or (positive and number <= 0):
        raise ValueError(f"{name}_must_be_finite" + ("_and_positive" if positive else ""))
    return number


@dataclass(frozen=True)
class EngineConfig:
    # Initial research choices in native GEX units; not historical quantiles.
    medium_gex_threshold: float = 25_000.0
    high_gex_threshold: float = 50_000.0
    neutral_distance: float = 25.0
    deep_positive_distance: float = 50.0

    def __post_init__(self):
        for name, value in asdict(self).items():
            finite_number(value, name, positive=True)
        if self.medium_gex_threshold >= self.high_gex_threshold:
            raise ValueError("gex_thresholds_must_be_ordered")
        if self.neutral_distance >= self.deep_positive_distance:
            raise ValueError("gamma_thresholds_must_be_ordered")


DEFAULT_CONFIG = EngineConfig()


@dataclass(frozen=True)
class Snapshot:
    spot: float
    gamma_pin: float
    max_pain: float
    zero_gamma: float
    net_gex: float
    model_lean: float
    time_of_day: float  # Fraction of this cash session, including early closes.
    session_state: Literal["OPENING", "MIDDAY", "CLOSING"]
    vol_valid: bool
    capture_state: Literal["RESEARCH_ONLY", "DECISION_GRADE"]

    def __post_init__(self):
        for name in ("spot", "gamma_pin", "max_pain", "zero_gamma", "model_lean"):
            finite_number(getattr(self, name), name, positive=True)
        finite_number(self.net_gex, "net_gex")
        if not 0 <= finite_number(self.time_of_day, "time_of_day") <= 1:
            raise ValueError("time_of_day_must_be_session_fraction")
        if self.session_state not in {"OPENING", "MIDDAY", "CLOSING"}:
            raise ValueError("invalid_session_state")
        if type(self.vol_valid) is not bool:
            raise ValueError("vol_valid_must_be_boolean")
        if self.capture_state not in {"RESEARCH_ONLY", "DECISION_GRADE"}:
            raise ValueError("invalid_capture_state")


@dataclass(frozen=True)
class Regime:
    gamma_regime: str
    pin_relation: str
    pain_relation: str
    band_width: float
    structural_label: str


@dataclass(frozen=True)
class Prediction:
    predicted_close: float
    band_low: float
    band_high: float
    confidence: float
    regime: Regime
    inputs: Snapshot

    def to_dict(self) -> dict:
        return asdict(self)


def compute_dist_to_pin(snapshot: Snapshot) -> float:
    return snapshot.gamma_pin - snapshot.spot


def compute_dist_to_pain(snapshot: Snapshot) -> float:
    return snapshot.spot - snapshot.max_pain


def compute_dist_to_zero_gamma(snapshot: Snapshot) -> float:
    return snapshot.spot - snapshot.zero_gamma


def compute_band_width(snapshot: Snapshot) -> float:
    return snapshot.gamma_pin - snapshot.max_pain


def classify_regime(snapshot: Snapshot, config: EngineConfig = DEFAULT_CONFIG) -> Regime:
    distance = compute_dist_to_zero_gamma(snapshot)
    # Resolve the spec's overlapping (0, 25] ranges in favor of neutral.
    if distance > config.deep_positive_distance:
        gamma = "positive_deep"
    elif distance > config.neutral_distance:
        gamma = "positive_shallow"
    elif distance >= -config.neutral_distance:
        gamma = "neutral"
    else:
        gamma = "negative"
    pin = "below_pin" if snapshot.spot < snapshot.gamma_pin else "above_pin"
    pain = "above_pain" if snapshot.spot > snapshot.max_pain else "below_pain"
    if gamma == "positive_deep" and pin == "below_pin" and pain == "above_pain":
        label = "compressed_upward_band_between_pain_and_pin"
    elif gamma == "negative" and pin == "above_pin" and pain == "below_pain":
        label = "trend_down_regime_below_pain"
    elif gamma == "neutral" and pin == "below_pin" and pain == "above_pain":
        label = "fragile_band_with_weak_pin_magnet"
    else:
        label = f"{gamma}_{pin}_{pain}"
    return Regime(gamma, pin, pain, compute_band_width(snapshot), label)


def score_confidence(
    snapshot: Snapshot, regime: Regime, predicted_close: float,
    band_low: float, band_high: float, config: EngineConfig = DEFAULT_CONFIG,
) -> float:
    """Return the specified uncalibrated 0..1 heuristic score."""
    for name, value in (("predicted_close", predicted_close), ("band_low", band_low), ("band_high", band_high)):
        finite_number(value, name)
    if band_low > band_high:
        raise ValueError("band_must_be_ordered")
    magnitude = abs(snapshot.net_gex)
    strength = 0.9 if magnitude >= config.high_gex_threshold else 0.7 if magnitude >= config.medium_gex_threshold else 0.4
    vol = 0.8 if snapshot.vol_valid else 0.3
    time_factor = {"OPENING": 0.4, "MIDDAY": 0.6, "CLOSING": 0.9}.get(snapshot.session_state, 0.5)
    alignment = 0.8 if band_low <= predicted_close <= band_high else 0.4
    return min(1.0, max(0.0, 0.30 * strength + 0.20 * vol + 0.25 * time_factor + 0.25 * alignment))


def predict_close(
    snapshot: Snapshot, regime: Regime | None = None, config: EngineConfig = DEFAULT_CONFIG,
) -> Prediction:
    expected = classify_regime(snapshot, config)
    if regime is not None and regime != expected:
        raise ValueError("regime_does_not_match_snapshot")
    regime = expected
    s, m, p, x = snapshot.spot, snapshot.model_lean, snapshot.gamma_pin, snapshot.max_pain
    # Keep signed width in derived features; use its magnitude for expansion so
    # reversed pin/pain ordering can never create an inverted range.
    width = abs(regime.band_width)
    low, high = min(x, p), max(x, p)
    between = regime.pin_relation == "below_pin" and regime.pain_relation == "above_pain"
    if regime.gamma_regime == "negative":
        low, high = low - 0.5 * width, high + 0.5 * width
        target = 0.20 * s + 0.70 * m + 0.10 * p
    elif regime.gamma_regime == "positive_deep" and between:
        target = min(high, max(low, 0.40 * s + 0.35 * m + 0.15 * p + 0.10 * x))
    elif regime.gamma_regime == "positive_deep" and regime.pain_relation == "above_pain":
        high += 0.25 * width
        target = 0.30 * s + 0.40 * m + 0.30 * p
    elif regime.gamma_regime == "neutral":
        low, high = low - 0.25 * width, high + 0.25 * width
        target = (0.25 * s + 0.55 * m + 0.10 * p + 0.10 * x) if between else (0.30 * s + 0.50 * m + 0.10 * p + 0.10 * x)
    elif regime.gamma_regime == "positive_shallow":
        if not between:
            low, high = low - 0.25 * width, high + 0.25 * width
        target = 0.35 * s + 0.45 * m + 0.10 * p + 0.10 * x
    else:
        low -= 0.25 * width
        target = 0.35 * s + 0.40 * m + 0.15 * p + 0.10 * x
    score = score_confidence(snapshot, regime, target, low, high, config)
    return Prediction(target, low, high, score, regime, snapshot)
