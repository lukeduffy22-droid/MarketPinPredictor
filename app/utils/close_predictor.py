"""
Close Predictor Overlay Module

This module provides a checklist-based overlay on top of the gamma model
to improve end-of-day close predictions. It analyzes:
- Pin migration throughout the trading day
- Spot vs pin deviation thresholds
- Call vs Put GEX imbalance
- Pull strength decay
- Final-hour drift factors
- Holiday/thin liquidity adjustments

The overlay runs independently and merges with the gamma model output.
"""

from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any
from datetime import datetime, timedelta
import pytz


@dataclass
class CloseSignal:
    """A single signal from the close predictor checklist"""
    emoji: str
    category: str
    message: str
    bias: str  # 'bullish', 'bearish', 'neutral'
    strength: float  # 0-1 indicating signal strength


@dataclass
class ClosePrediction:
    """Unified close prediction result"""
    index: str
    pin_strike: float
    spot_price: float
    expected_close: float
    close_range_low: float
    close_range_high: float
    signals: List[CloseSignal] = field(default_factory=list)
    net_bias: str = 'neutral'  # 'bullish', 'bearish', 'neutral'
    confidence: float = 0.5
    drift_adjustment: float = 0.0
    summary: str = ""


def analyze_pin_migration(pin_history: List[Dict]) -> Optional[CloseSignal]:
    """
    Check if pin strike has migrated during the session.
    Pin migration indicates shifting dealer positioning.
    """
    if not pin_history or len(pin_history) < 2:
        return None
    
    unique_pins = set(p.get('pin_strike') for p in pin_history if p.get('pin_strike'))
    
    if len(unique_pins) > 1:
        pins_sorted = sorted(unique_pins)
        migration_range = pins_sorted[-1] - pins_sorted[0]
        latest_pin = pin_history[-1].get('pin_strike', 0)
        first_pin = pin_history[0].get('pin_strike', 0)
        
        if latest_pin > first_pin:
            return CloseSignal(
                emoji="⚠️",
                category="pin_migration",
                message=f"Pin migrated UP intraday ({first_pin:.0f} → {latest_pin:.0f}) — reset close expectation higher",
                bias="bullish",
                strength=min(1.0, migration_range / first_pin * 100) if first_pin else 0.5
            )
        elif latest_pin < first_pin:
            return CloseSignal(
                emoji="⚠️",
                category="pin_migration",
                message=f"Pin migrated DOWN intraday ({first_pin:.0f} → {latest_pin:.0f}) — reset close expectation lower",
                bias="bearish",
                strength=min(1.0, migration_range / first_pin * 100) if first_pin else 0.5
            )
    
    return None


def analyze_spot_deviation(
    spot: float,
    pin: float,
    deviation_history: List[float],
    threshold_pct: float = 0.02,
    min_duration_samples: int = 4
) -> Optional[CloseSignal]:
    """
    Check if spot has been consistently above/below pin.
    Sustained deviation suggests directional pressure.
    
    Args:
        spot: Current spot price
        pin: Current pin strike
        deviation_history: List of (spot-pin)/pin values over time
        threshold_pct: Deviation threshold (0.02 = 0.02%)
        min_duration_samples: Minimum samples above threshold
    """
    if not deviation_history:
        return None
    
    current_dev = (spot - pin) / pin * 100 if pin else 0
    
    above_threshold = sum(1 for d in deviation_history[-min_duration_samples:] if d > threshold_pct)
    below_threshold = sum(1 for d in deviation_history[-min_duration_samples:] if d < -threshold_pct)
    
    if above_threshold >= min_duration_samples:
        return CloseSignal(
            emoji="⬆️",
            category="spot_deviation",
            message=f"Spot consistently above pin (+{current_dev:.2f}%) for {above_threshold}+ samples → upward bias",
            bias="bullish",
            strength=min(1.0, current_dev / 0.1)
        )
    elif below_threshold >= min_duration_samples:
        return CloseSignal(
            emoji="⬇️",
            category="spot_deviation",
            message=f"Spot consistently below pin ({current_dev:.2f}%) for {below_threshold}+ samples → downward bias",
            bias="bearish",
            strength=min(1.0, abs(current_dev) / 0.1)
        )
    
    return None


def analyze_gex_balance(
    call_gex: float,
    put_gex: float,
    imbalance_threshold: float = 3.0
) -> Optional[CloseSignal]:
    """
    Check call vs put GEX imbalance.
    Strong call dominance suggests upside drift; put dominance suggests downside.
    """
    if put_gex == 0 and call_gex == 0:
        return None
    
    if put_gex == 0:
        ratio = float('inf')
    elif call_gex == 0:
        ratio = 0
    else:
        ratio = call_gex / put_gex
    
    if ratio > imbalance_threshold:
        return CloseSignal(
            emoji="📈",
            category="gex_balance",
            message=f"Call GEX dominant ({ratio:.1f}x puts) → upside drift risk",
            bias="bullish",
            strength=min(1.0, (ratio - imbalance_threshold) / 5.0)
        )
    elif ratio < 1 / imbalance_threshold:
        inverse_ratio = 1 / ratio if ratio > 0 else float('inf')
        return CloseSignal(
            emoji="📉",
            category="gex_balance",
            message=f"Put GEX dominant ({inverse_ratio:.1f}x calls) → downside drift risk",
            bias="bearish",
            strength=min(1.0, (inverse_ratio - imbalance_threshold) / 5.0)
        )
    
    return None


def analyze_pull_strength(
    pull_strength: float,
    strong_threshold: float = 0.5,
    weak_threshold: float = 1.5
) -> Optional[CloseSignal]:
    """
    Analyze pin magnet strength.
    Strong pull = expect reversion to pin.
    Weak pull = expect drift away from pin.
    
    Note: pull_strength is % distance from spot to pin.
    Lower = closer = stronger magnet.
    """
    if pull_strength < strong_threshold:
        return CloseSignal(
            emoji="🧲",
            category="pull_strength",
            message=f"Strong pin magnet ({pull_strength:.2f}% distance) → expect close near pin",
            bias="neutral",
            strength=0.8
        )
    elif pull_strength > weak_threshold:
        return CloseSignal(
            emoji="⚠️",
            category="pull_strength",
            message=f"Weak pin magnet ({pull_strength:.2f}% distance) → expect drift from pin",
            bias="neutral",
            strength=0.6
        )
    
    return None


def analyze_final_hour_drift(
    spot: float,
    pin: float,
    call_gex: float,
    put_gex: float,
    minutes_to_close: int
) -> Optional[CloseSignal]:
    """
    Final hour overlay: combine deviation + GEX for directional bias.
    In thin liquidity, moves face less resistance.
    """
    if minutes_to_close > 60:
        return None
    
    deviation_pct = (spot - pin) / pin * 100 if pin else 0
    call_dominant = call_gex > put_gex if (call_gex and put_gex) else False
    put_dominant = put_gex > call_gex if (call_gex and put_gex) else False
    
    liquidity_factor = 1.0 + (60 - minutes_to_close) / 60 * 0.5
    
    if deviation_pct > 0.02 and call_dominant:
        return CloseSignal(
            emoji="🎯",
            category="final_hour_drift",
            message=f"Final hour: spot above pin + call GEX dominant → higher close probability (thin liquidity factor: {liquidity_factor:.2f}x)",
            bias="bullish",
            strength=min(1.0, 0.7 * liquidity_factor)
        )
    elif deviation_pct < -0.02 and put_dominant:
        return CloseSignal(
            emoji="🎯",
            category="final_hour_drift",
            message=f"Final hour: spot below pin + put GEX dominant → lower close probability (thin liquidity factor: {liquidity_factor:.2f}x)",
            bias="bearish",
            strength=min(1.0, 0.7 * liquidity_factor)
        )
    
    return None


def is_holiday_adjacent() -> bool:
    """Check if today is adjacent to a market holiday (thin liquidity expected)"""
    et_tz = pytz.timezone('US/Eastern')
    today = datetime.now(et_tz).date()
    
    holidays_2024_2025 = [
        (12, 24), (12, 25), (12, 31),  # Christmas/NYE
        (1, 1), (1, 20),  # New Year, MLK Day
        (2, 17),  # Presidents Day
        (4, 18),  # Good Friday
        (5, 26),  # Memorial Day
        (7, 4),   # Independence Day
        (9, 1),   # Labor Day
        (11, 27), # Thanksgiving
    ]
    
    for month, day in holidays_2024_2025:
        try:
            holiday = today.replace(month=month, day=day)
            if abs((today - holiday).days) <= 1:
                return True
        except ValueError:
            continue
    
    return False


def analyze_holiday_liquidity() -> Optional[CloseSignal]:
    """
    Holiday-adjacent sessions often have thin liquidity.
    This reduces resistance to directional moves.
    """
    if is_holiday_adjacent():
        return CloseSignal(
            emoji="🎄",
            category="holiday_liquidity",
            message="Holiday-adjacent session → thin liquidity, less resistance to directional moves",
            bias="neutral",
            strength=0.5
        )
    return None


def calculate_drift_adjustment(signals: List[CloseSignal]) -> float:
    """
    Calculate net drift adjustment from signals.
    Returns a points adjustment (+/- from pin).
    """
    bullish_weight = sum(s.strength for s in signals if s.bias == 'bullish')
    bearish_weight = sum(s.strength for s in signals if s.bias == 'bearish')
    
    net_weight = bullish_weight - bearish_weight
    
    max_drift_pts = 10
    drift = net_weight * max_drift_pts / 3
    
    return drift


def determine_net_bias(signals: List[CloseSignal]) -> str:
    """Determine overall bias from signals"""
    bullish = sum(1 for s in signals if s.bias == 'bullish')
    bearish = sum(1 for s in signals if s.bias == 'bearish')
    
    if bullish > bearish:
        return 'bullish'
    elif bearish > bullish:
        return 'bearish'
    return 'neutral'


def generate_summary(prediction: ClosePrediction) -> str:
    """Generate human-readable summary"""
    bias_emoji = {
        'bullish': '🟢',
        'bearish': '🔴',
        'neutral': '⚪'
    }
    
    drift_direction = "↑" if prediction.drift_adjustment > 0 else "↓" if prediction.drift_adjustment < 0 else "↔"
    
    return (
        f"{bias_emoji.get(prediction.net_bias, '⚪')} {prediction.index}: "
        f"Pin ${prediction.pin_strike:,.0f}, "
        f"Expected Close ${prediction.expected_close:,.0f} "
        f"(range ${prediction.close_range_low:,.0f}-${prediction.close_range_high:,.0f}) "
        f"{drift_direction}"
    )


def run_close_predictor(
    index: str,
    spot: float,
    pin: float,
    call_gex: float = 0,
    put_gex: float = 0,
    pull_strength: float = 0,
    pin_history: Optional[List[Dict]] = None,
    deviation_history: Optional[List[float]] = None,
    minutes_to_close: int = 390
) -> ClosePrediction:
    """
    Main entry point for the close predictor overlay.
    
    Args:
        index: Index symbol (SPX, NDX, RUT)
        spot: Current spot price
        pin: Current gamma pin strike
        call_gex: Total call gamma exposure
        put_gex: Total put gamma exposure
        pull_strength: % distance from spot to pin
        pin_history: List of historical pin snapshots
        deviation_history: List of (spot-pin)/pin values
        minutes_to_close: Minutes until market close
    
    Returns:
        ClosePrediction with signals and adjusted close estimate
    """
    signals: List[CloseSignal] = []
    
    if pin_history:
        signal = analyze_pin_migration(pin_history)
        if signal:
            signals.append(signal)
    
    if deviation_history:
        signal = analyze_spot_deviation(spot, pin, deviation_history)
        if signal:
            signals.append(signal)
    
    signal = analyze_gex_balance(call_gex, put_gex)
    if signal:
        signals.append(signal)
    
    signal = analyze_pull_strength(pull_strength)
    if signal:
        signals.append(signal)
    
    signal = analyze_final_hour_drift(spot, pin, call_gex, put_gex, minutes_to_close)
    if signal:
        signals.append(signal)
    
    signal = analyze_holiday_liquidity()
    if signal:
        signals.append(signal)
    
    drift_adjustment = calculate_drift_adjustment(signals)
    net_bias = determine_net_bias(signals)
    
    expected_close = pin + drift_adjustment
    
    base_range = 5  # Base ±5 points
    volatility_factor = 1.0 + (pull_strength / 2)  # Wider range if far from pin
    range_pts = base_range * volatility_factor
    
    close_range_low = expected_close - range_pts
    close_range_high = expected_close + range_pts
    
    bullish_signals = sum(1 for s in signals if s.bias == 'bullish')
    bearish_signals = sum(1 for s in signals if s.bias == 'bearish')
    total_signals = len(signals)
    
    if total_signals > 0:
        agreement = max(bullish_signals, bearish_signals) / total_signals
        confidence = 0.5 + agreement * 0.3
    else:
        confidence = 0.5
    
    prediction = ClosePrediction(
        index=index,
        pin_strike=pin,
        spot_price=spot,
        expected_close=expected_close,
        close_range_low=close_range_low,
        close_range_high=close_range_high,
        signals=signals,
        net_bias=net_bias,
        confidence=confidence,
        drift_adjustment=drift_adjustment
    )
    
    prediction.summary = generate_summary(prediction)
    
    return prediction


def format_signals_for_ui(signals: List[CloseSignal]) -> List[Dict[str, Any]]:
    """Format signals for UI display"""
    return [
        {
            'emoji': s.emoji,
            'category': s.category,
            'message': s.message,
            'bias': s.bias,
            'strength': s.strength
        }
        for s in signals
    ]
