# app/core/audit_snapshot.py
"""
Audit Snapshot Builder.

This module provides the `build_audit_snapshot()` function which captures a complete,
deterministic snapshot of the system state at a given moment.

CRITICAL DESIGN RULES:
1. This function does NOT compute gamma. It only records what already exists.
2. Given the same inputs, output is deterministic and diffable.
3. No derived values are recomputed inside this builder.
4. This function must not depend on UI state.
"""
from __future__ import annotations

from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any
import json


@dataclass
class StrikeGexData:
    """Per-strike GEX data for audit trail."""
    strike: float
    expiration_days: int
    call_open_interest: int
    put_open_interest: int
    call_gamma: float
    put_gamma: float
    call_gex: float
    put_gex: float
    net_gex: float  # call_gex - put_gex
    abs_gex: float  # abs(net_gex)


@dataclass
class SpotState:
    """Current spot price state."""
    price: float
    timestamp_utc: str  # ISO 8601 format
    source: str  # 'websocket', 'rest', 'cache'


@dataclass
class ChainIdentity:
    """Options chain identity and metadata."""
    chain_symbol_used: str  # Exact string passed to Polygon (e.g., DIA for DJI)
    underlying_reported: Optional[str]  # From options payload if present (e.g., DJI)
    is_etf_proxy: bool  # True if using ETF proxy (e.g., DJI→DIA)
    expirations_included: List[int]  # List of days to expiry, or [min, max]
    contracts_count: int
    expiration_scope: str  # '0DTE' or 'ALL<=90D'


@dataclass
class DerivedGammaMetrics:
    """Derived gamma metrics from the gamma surface."""
    primary_gamma_pin_strike: float
    primary_gamma_pin_abs_gex: float
    zero_gamma_level: Optional[float]
    zero_gamma_method: Optional[str]  # 'cumulative', 'local', 'smoothed', None
    total_gex_abs: float  # sum(abs(net_gex_per_strike)) - CANONICAL DEFINITION
    total_gex_net: float  # sum(net_gex_per_strike) - CANONICAL DEFINITION
    total_gex_abs_definition: str  # Explicit definition string
    total_gex_net_definition: str  # Explicit definition string


@dataclass
class ValidationResult:
    """Result of sanity validation (populated later by sanity_checks.py)."""
    is_valid: bool
    failure_reasons: List[str] = field(default_factory=list)
    gamma_excluded_from_model: bool = False


# ============================================================================
# DIAGNOSTIC METRIC FUNCTIONS (observational only - do NOT modify gamma math)
# ============================================================================

def compute_skew_metrics(contracts: List[Dict[str, Any]], spot: float) -> Optional[Dict[str, float]]:
    """
    Compute ATM call vs put IV spread.
    DIAGNOSTIC ONLY - do NOT use to adjust gamma.
    
    Args:
        contracts: List of contract dicts with 'strike', 'iv', 'contract_type' keys
        spot: Current spot price
    
    Returns:
        Dict with call_iv_mean, put_iv_mean, skew or None if insufficient data
    """
    atm_window = [c for c in contracts if abs(c.get('strike', 0) - spot) / max(spot, 1) < 0.01]
    
    call_ivs = [c.get('iv', 0) for c in atm_window if c.get('contract_type', '').upper() == 'CALL' and c.get('iv')]
    put_ivs = [c.get('iv', 0) for c in atm_window if c.get('contract_type', '').upper() == 'PUT' and c.get('iv')]
    
    if not call_ivs or not put_ivs:
        return None
    
    call_iv_mean = sum(call_ivs) / len(call_ivs)
    put_iv_mean = sum(put_ivs) / len(put_ivs)
    
    return {
        "call_iv_mean": round(call_iv_mean, 4),
        "put_iv_mean": round(put_iv_mean, 4),
        "skew": round(put_iv_mean - call_iv_mean, 4)
    }


def strike_distance_bucket(strike: float, spot: float) -> str:
    """
    Classify strike by distance from spot.
    
    Args:
        strike: Strike price
        spot: Spot price
    
    Returns:
        Bucket name: ATM, NEAR, MID, or FAR
    """
    if spot <= 0:
        return "FAR"
    pct = abs(strike - spot) / spot
    if pct < 0.002:
        return "ATM"
    if pct < 0.01:
        return "NEAR"
    if pct < 0.03:
        return "MID"
    return "FAR"


def compute_gamma_by_distance(strikes_data: List[Dict[str, Any]], spot: float) -> Dict[str, float]:
    """
    Aggregate gamma contribution per strike-distance bucket.
    DIAGNOSTIC ONLY - do NOT use to adjust gamma.
    
    Args:
        strikes_data: List of strike dicts with 'strike' and 'abs_gex' or 'net_gex'
        spot: Current spot price
    
    Returns:
        Dict with ATM, NEAR, MID, FAR gamma contributions as percentages
    """
    buckets = {"ATM": 0.0, "NEAR": 0.0, "MID": 0.0, "FAR": 0.0}
    
    for s in strikes_data:
        strike = s.get('strike', 0)
        abs_gex = abs(s.get('net_gex', 0))
        bucket = strike_distance_bucket(strike, spot)
        buckets[bucket] += abs_gex
    
    total = sum(buckets.values())
    if total > 0:
        return {k: round(v / total * 100, 1) for k, v in buckets.items()}
    return buckets


def classify_vol_regime(iv: float) -> str:
    """
    Classify volatility regime based on IV.
    
    Args:
        iv: Implied volatility (decimal, e.g., 0.20 for 20%)
    
    Returns:
        Regime: LOW, MEDIUM, or HIGH
    """
    if iv < 0.15:
        return "LOW"
    if iv < 0.25:
        return "MEDIUM"
    return "HIGH"


def compute_truncation_metrics(
    contracts_used: int,
    contracts_available: int,
    strikes_used: List[float],
    strikes_available: List[float]
) -> Dict[str, Any]:
    """
    Compute truncation bias metrics.
    DIAGNOSTIC ONLY - quantifies bias from contract caps.
    
    Args:
        contracts_used: Number of contracts actually used
        contracts_available: Total contracts available
        strikes_used: List of strikes used in calculation
        strikes_available: List of all available strikes
    
    Returns:
        Dict with truncation metrics
    """
    if not strikes_used or not strikes_available:
        return {
            "contracts_used": contracts_used,
            "contracts_available": contracts_available,
            "excluded_above": 0,
            "excluded_below": 0,
            "truncation_pct": 0.0
        }
    
    max_used = max(strikes_used)
    min_used = min(strikes_used)
    max_available = max(strikes_available)
    min_available = min(strikes_available)
    
    excluded_above = max_available - max_used if max_available > max_used else 0
    excluded_below = min_used - min_available if min_used > min_available else 0
    
    truncation_pct = 0.0
    if contracts_available > 0:
        truncation_pct = round((1 - contracts_used / contracts_available) * 100, 1)
    
    return {
        "contracts_used": contracts_used,
        "contracts_available": contracts_available,
        "excluded_above": round(excluded_above, 2),
        "excluded_below": round(excluded_below, 2),
        "truncation_pct": truncation_pct
    }


def compute_confidence_score(
    contracts_used: int,
    validation_failed: bool,
    vol_regime: str,
    truncation_pct: float = 0.0
) -> tuple:
    """
    Compute self-diagnostic confidence score.
    DIAGNOSTIC ONLY - do NOT use for trading decisions.
    
    Args:
        contracts_used: Number of contracts in snapshot
        validation_failed: Whether sanity validation failed
        vol_regime: Volatility regime (LOW, MEDIUM, HIGH)
        truncation_pct: Percentage of contracts truncated
    
    Returns:
        Tuple of (confidence score 0.0-1.0, list of factors)
    """
    confidence = 1.0
    factors = []
    
    if contracts_used < 400:
        confidence -= 0.2
        factors.append(f"Low contract count ({contracts_used} < 400)")
    
    if validation_failed:
        confidence -= 0.4
        factors.append("Sanity validation failed")
    
    if vol_regime == "HIGH":
        confidence -= 0.1
        factors.append("High volatility regime")
    
    if truncation_pct > 20:
        confidence -= 0.15
        factors.append(f"High truncation ({truncation_pct:.1f}% excluded)")
    
    confidence = max(confidence, 0.0)
    
    if not factors:
        factors.append("No confidence degradation factors")
    
    return round(confidence, 2), factors


def compute_dispersion_ratio(gamma_by_distance: Dict[str, float]) -> float:
    """
    Compute dispersion ratio (FAR/ATM gamma ratio).
    DIAGNOSTIC ONLY - tracks structural noise.
    
    Higher dispersion indicates more gamma spread to far strikes,
    which may indicate noisier pin predictions.
    
    Args:
        gamma_by_distance: Dict with ATM, NEAR, MID, FAR percentages
    
    Returns:
        Dispersion ratio (FAR/ATM or 0 if ATM is zero)
    """
    atm = gamma_by_distance.get("ATM", 0)
    far = gamma_by_distance.get("FAR", 0)
    if atm < 0.001:
        return far  # If ATM is effectively zero, return FAR as ratio
    return round(far / atm, 3)


@dataclass
class AuditSnapshot:
    """Complete audit snapshot for a symbol at a point in time."""
    # Metadata
    snapshot_version: str = "1.0"
    generated_at_utc: str = ""
    
    # A. Core market state
    symbol: str = ""
    timestamp_utc: str = ""
    spot_last: float = 0.0
    spot_timestamp: str = ""
    spot_source: str = ""
    
    # B. Options chain identity
    chain_symbol_used: str = ""  # Actual symbol used for options chain (e.g., DIA for DJI)
    underlying_reported: Optional[str] = None  # Original index symbol (e.g., DJI)
    is_etf_proxy: bool = False  # True if using ETF proxy (e.g., DJI→DIA)
    expirations_min_days: int = 0
    expirations_max_days: int = 0
    contracts_count: int = 0
    expiration_scope: str = ""  # '0DTE' or 'ALL<=90D'
    
    # C. Top 15 strikes by abs GEX
    top_strikes_by_abs_gex: List[Dict[str, Any]] = field(default_factory=list)
    
    # D. Derived gamma metrics
    primary_gamma_pin_strike: float = 0.0
    primary_gamma_pin_abs_gex: float = 0.0
    zero_gamma_level: Optional[float] = None
    zero_gamma_method: Optional[str] = None
    
    # D.1 Corrected GEX fields (signed call/put separation)
    call_gex_total: float = 0.0  # Aggregate call gamma exposure
    put_gex_total: float = 0.0   # Aggregate put gamma exposure
    gross_gex: float = 0.0       # call_gex_total + put_gex_total (total magnitude)
    net_gex: float = 0.0         # call_gex_total - put_gex_total (signed directional)
    gross_gex_definition: str = "sum(call_gex) + sum(put_gex)"
    net_gex_definition: str = "sum(call_gex) - sum(put_gex)"
    
    # D.2 Legacy fields (kept for backward compatibility)
    total_gex_abs: float = 0.0  # Legacy: same as gross_gex
    total_gex_net: float = 0.0  # Legacy: same as net_gex
    total_gex_abs_definition: str = "sum(call_gex) + sum(put_gex) [CORRECTED]"
    total_gex_net_definition: str = "sum(call_gex) - sum(put_gex) [CORRECTED]"
    
    # D.3 Pin Drift Tracking (intraday gamma pin migration)
    pin_drift_points_per_hour: Optional[float] = None  # (pin_now - pin_prev) / hours_elapsed
    pin_change_points: Optional[float] = None  # Simple difference: pin_now - pin_prev
    prev_pin_strike: Optional[float] = None  # Previous snapshot's pin strike
    prev_snapshot_timestamp: Optional[str] = None  # Previous snapshot's timestamp
    
    # E. Validation result (populated by sanity_checks.py)
    validation_is_valid: bool = True
    validation_failure_reasons: List[str] = field(default_factory=list)
    gamma_excluded_from_model: bool = False
    
    # F. Diagnostic Metrics (observational only - do NOT use to adjust gamma math)
    # F.1 Skew Metrics - ATM call vs put IV spread
    skew_metrics: Optional[Dict[str, float]] = None  # call_iv_mean, put_iv_mean, skew
    
    # F.2 Gamma by Distance - contribution per strike-distance bucket
    gamma_by_distance: Optional[Dict[str, float]] = None  # ATM, NEAR, MID, FAR
    
    # F.3 Volatility Regime - LOW, MEDIUM, HIGH
    vol_regime: Optional[str] = None
    vol_regime_iv: Optional[float] = None  # IV value used for classification
    
    # F.4 Truncation Metrics - bias from contract caps
    truncation: Optional[Dict[str, Any]] = None  # contracts_used, contracts_available, excluded_above, excluded_below
    
    # F.5 Confidence Score - self-diagnostic 0.0-1.0
    confidence: Optional[float] = None
    confidence_factors: Optional[List[str]] = None  # reasons for confidence adjustments
    
    # F.6 Dispersion Ratio - structural noise metric (FAR/ATM gamma ratio)
    dispersion_ratio: Optional[float] = None
    
    # G. Pre-Gate Explanation Fields (for sanity check transparency)
    # These fields explain WHY a snapshot might fail sanity checks
    strike_count: Optional[int] = None  # Total number of strikes in snapshot
    nonzero_strike_count: Optional[int] = None  # Strikes with non-zero GEX
    top_strike_share: Optional[float] = None  # Percentage of total GEX in top strike
    pregate_reason: Optional[str] = None  # Explanation if chain is thin/concentrated
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return asdict(self)
    
    def to_json(self, indent: int = 2) -> str:
        """Convert to JSON string."""
        return json.dumps(self.to_dict(), indent=indent)


def build_audit_snapshot(
    symbol: str,
    spot_state: Dict[str, Any],
    chain_snapshot: Dict[str, Any],
    gamma_surface: Dict[str, Any],
    raw_contracts: Optional[List[Dict[str, Any]]] = None,
    contracts_available: Optional[int] = None,
    strikes_available: Optional[List[float]] = None,
    prev_snapshot: Optional['AuditSnapshot'] = None,
) -> AuditSnapshot:
    """
    Build a complete audit snapshot from existing computed data.
    
    CRITICAL: This function does NOT compute gamma.
    It only records what already exists.
    
    Args:
        symbol: Index symbol (SPX, NDX, RUT, DJI)
        spot_state: Dict with keys: price, timestamp_utc, source
        chain_snapshot: Dict with keys: chain_symbol_used, underlying_reported,
                       expirations (list of ints), contracts_count, expiration_scope
        gamma_surface: Dict with keys: strikes (list of strike dicts), 
                      pin_strike, pin_abs_gex, zero_gamma_level, zero_gamma_method,
                      call_gex_total, put_gex_total, gross_gex, net_gex
        raw_contracts: Optional list of raw contract dicts for skew calculation
        contracts_available: Optional total contracts available before truncation
        strikes_available: Optional list of all available strikes before truncation
        prev_snapshot: Optional previous AuditSnapshot for pin drift calculation
    
    Returns:
        AuditSnapshot: Complete, deterministic snapshot
    """
    now_utc = datetime.now(timezone.utc).isoformat()
    spot_price = float(spot_state.get('price', 0.0))
    contracts_count = int(chain_snapshot.get('contracts_count', 0))
    
    # Extract GEX values from gamma_surface
    call_gex_total = float(gamma_surface.get('call_gex_total', 0.0))
    put_gex_total = float(gamma_surface.get('put_gex_total', 0.0))
    gross_gex = float(gamma_surface.get('gross_gex', call_gex_total + put_gex_total))
    net_gex = float(gamma_surface.get('net_gex', call_gex_total - put_gex_total))
    
    # Legacy fields (for backward compatibility)
    total_gex_abs = gross_gex
    total_gex_net = net_gex
    
    pin_strike = float(gamma_surface.get('pin_strike', 0.0))
    
    snapshot = AuditSnapshot(
        snapshot_version="1.2",  # Bumped for GEX correction and pin drift
        generated_at_utc=now_utc,
        
        # A. Core market state
        symbol=symbol,
        timestamp_utc=now_utc,
        spot_last=spot_price,
        spot_timestamp=str(spot_state.get('timestamp_utc', now_utc)),
        spot_source=str(spot_state.get('source', 'unknown')),
        
        # B. Options chain identity
        chain_symbol_used=str(chain_snapshot.get('chain_symbol_used', symbol)),
        underlying_reported=chain_snapshot.get('underlying_reported'),
        is_etf_proxy=bool(chain_snapshot.get('is_etf_proxy', False)),
        expirations_min_days=int(min(chain_snapshot.get('expirations', [0])) if chain_snapshot.get('expirations') else 0),
        expirations_max_days=int(max(chain_snapshot.get('expirations', [0])) if chain_snapshot.get('expirations') else 0),
        contracts_count=contracts_count,
        expiration_scope=str(chain_snapshot.get('expiration_scope', 'UNKNOWN')),
        
        # D. Derived gamma metrics
        primary_gamma_pin_strike=pin_strike,
        primary_gamma_pin_abs_gex=float(gamma_surface.get('pin_abs_gex', 0.0)),
        zero_gamma_level=gamma_surface.get('zero_gamma_level'),
        zero_gamma_method=gamma_surface.get('zero_gamma_method'),
        
        # D.1 Corrected GEX fields
        call_gex_total=call_gex_total,
        put_gex_total=put_gex_total,
        gross_gex=gross_gex,
        net_gex=net_gex,
        
        # D.2 Legacy fields
        total_gex_abs=total_gex_abs,
        total_gex_net=total_gex_net,
    )
    
    # D.3 Pin Drift Calculation
    if prev_snapshot is not None and prev_snapshot.primary_gamma_pin_strike > 0:
        prev_pin = prev_snapshot.primary_gamma_pin_strike
        prev_timestamp_str = prev_snapshot.generated_at_utc
        
        snapshot.prev_pin_strike = prev_pin
        snapshot.prev_snapshot_timestamp = prev_timestamp_str
        snapshot.pin_change_points = pin_strike - prev_pin
        
        # Calculate hours elapsed for drift per hour
        try:
            # Parse timestamps
            if prev_timestamp_str:
                prev_dt = datetime.fromisoformat(prev_timestamp_str.replace('Z', '+00:00'))
                now_dt = datetime.fromisoformat(now_utc.replace('Z', '+00:00'))
                hours_elapsed = (now_dt - prev_dt).total_seconds() / 3600.0
                
                if hours_elapsed > 0.01:  # At least ~36 seconds elapsed
                    snapshot.pin_drift_points_per_hour = round((pin_strike - prev_pin) / hours_elapsed, 2)
                else:
                    snapshot.pin_drift_points_per_hour = 0.0
        except Exception:
            # If timestamp parsing fails, just record the point change
            snapshot.pin_drift_points_per_hour = None
    
    # C. Top 15 strikes by abs GEX
    strikes = gamma_surface.get('strikes', [])
    if strikes:
        # Sort by absolute GEX descending, take top 15
        sorted_strikes = sorted(
            strikes, 
            key=lambda s: abs(s.get('net_gex', 0)), 
            reverse=True
        )[:15]
        
        snapshot.top_strikes_by_abs_gex = [
            {
                'strike': float(s.get('strike', 0)),
                'expiration_days': int(s.get('expiration_days', 0)),
                'call_open_interest': int(s.get('call_open_interest', 0)),
                'put_open_interest': int(s.get('put_open_interest', 0)),
                'call_gamma': float(s.get('call_gamma', 0)),
                'put_gamma': float(s.get('put_gamma', 0)),
                'call_gex': float(s.get('call_gex', 0)),
                'put_gex': float(s.get('put_gex', 0)),
                'net_gex': float(s.get('net_gex', 0)),
                'abs_gex': abs(float(s.get('net_gex', 0))),
            }
            for s in sorted_strikes
        ]
    
    # =========================================================================
    # F. DIAGNOSTIC METRICS (observational only - do NOT use to adjust gamma)
    # =========================================================================
    
    # F.1 Skew Metrics - ATM call vs put IV spread
    if raw_contracts and spot_price > 0:
        snapshot.skew_metrics = compute_skew_metrics(raw_contracts, spot_price)
    
    # F.2 Gamma by Distance - contribution per strike-distance bucket
    if strikes and spot_price > 0:
        snapshot.gamma_by_distance = compute_gamma_by_distance(strikes, spot_price)
    
    # F.3 Volatility Regime - classify based on average IV from contracts
    avg_iv = gamma_surface.get('avg_iv', None)
    if avg_iv is None and raw_contracts:
        ivs = [c.get('iv', 0) for c in raw_contracts if c.get('iv')]
        if ivs:
            avg_iv = sum(ivs) / len(ivs)
    if avg_iv is not None:
        snapshot.vol_regime = classify_vol_regime(avg_iv)
        snapshot.vol_regime_iv = round(avg_iv, 4)
    
    # F.4 Truncation Metrics - bias from contract caps
    strikes_used = [s.get('strike', 0) for s in strikes] if strikes else []
    if contracts_available is not None or strikes_available is not None:
        snapshot.truncation = compute_truncation_metrics(
            contracts_used=contracts_count,
            contracts_available=contracts_available or contracts_count,
            strikes_used=strikes_used,
            strikes_available=strikes_available or strikes_used
        )
    else:
        # Default truncation with no exclusion info
        snapshot.truncation = {
            "contracts_used": contracts_count,
            "contracts_available": contracts_count,
            "excluded_above": 0,
            "excluded_below": 0,
            "truncation_pct": 0.0
        }
    
    # F.5 Confidence Score - self-diagnostic
    truncation_pct = snapshot.truncation.get('truncation_pct', 0.0) if snapshot.truncation else 0.0
    vol_regime = snapshot.vol_regime or "MEDIUM"
    confidence, factors = compute_confidence_score(
        contracts_used=contracts_count,
        validation_failed=False,  # Will be updated by sanity_checks.py
        vol_regime=vol_regime,
        truncation_pct=truncation_pct
    )
    snapshot.confidence = confidence
    snapshot.confidence_factors = factors
    
    # F.6 Dispersion Ratio - structural noise metric
    if snapshot.gamma_by_distance:
        snapshot.dispersion_ratio = compute_dispersion_ratio(snapshot.gamma_by_distance)
    
    # G. Pre-Gate Explanation Fields
    # These help explain WHY sanity checks might fail
    snapshot.strike_count = len(strikes) if strikes else 0
    
    # Count strikes with non-zero GEX
    nonzero_strikes = [s for s in strikes if abs(s.get('net_gex', 0)) > 1e-10] if strikes else []
    snapshot.nonzero_strike_count = len(nonzero_strikes)
    
    # Calculate top strike's share of total GEX
    if snapshot.top_strikes_by_abs_gex and gross_gex > 0:
        top_strike_gex = abs(snapshot.top_strikes_by_abs_gex[0].get('net_gex', 0))
        snapshot.top_strike_share = round(top_strike_gex / gross_gex, 4)
    else:
        snapshot.top_strike_share = 0.0
    
    # Determine pregate_reason if chain appears problematic
    if snapshot.strike_count < 30:
        snapshot.pregate_reason = f"CHAIN_TOO_THIN: Only {snapshot.strike_count} strikes (min: 30)"
    elif snapshot.nonzero_strike_count < 10:
        snapshot.pregate_reason = f"TOO_FEW_ACTIVE_STRIKES: Only {snapshot.nonzero_strike_count} strikes with non-zero GEX"
    elif snapshot.top_strike_share > 0.9:
        snapshot.pregate_reason = f"EXTREME_CONCENTRATION: Top strike has {snapshot.top_strike_share*100:.1f}% of total GEX"
    else:
        snapshot.pregate_reason = None
    
    return snapshot


def snapshot_from_gamma_exposure_result(
    symbol: str,
    spot_price: float,
    spot_timestamp: str,
    spot_source: str,
    chain_symbol: str,
    gamma_result: Dict[str, Any],
    prev_snapshot: Optional['AuditSnapshot'] = None,
) -> AuditSnapshot:
    """
    Convenience function to build audit snapshot from calculate_gamma_exposure result.
    
    Args:
        symbol: Index symbol
        spot_price: Current spot price
        spot_timestamp: When spot was fetched
        spot_source: Source of spot data
        chain_symbol: Symbol used to fetch options chain
        gamma_result: Result from calculate_gamma_exposure()
        prev_snapshot: Optional previous snapshot for pin drift calculation
    
    Returns:
        AuditSnapshot
    """
    from app.core.gex import compute_aggregate_gex
    
    # Extract strikes data from gamma_result if available
    strikes_data = gamma_result.get('gex_by_strike', [])
    
    # Get expiration days from strikes
    exp_days = list(set(s.get('expiration_days', 0) for s in strikes_data)) if strikes_data else [0]
    
    # Determine expiration scope
    if len(exp_days) == 1 and exp_days[0] == 0:
        expiration_scope = '0DTE'
    else:
        expiration_scope = f'ALL<={max(exp_days) if exp_days else 90}D'
    
    # Compute aggregate GEX using canonical function (with call/put separation)
    agg_result = compute_aggregate_gex(strikes_data)
    
    spot_state = {
        'price': spot_price,
        'timestamp_utc': spot_timestamp,
        'source': spot_source,
    }
    
    chain_snapshot = {
        'chain_symbol_used': chain_symbol,
        'underlying_reported': gamma_result.get('underlying_symbol'),
        'expirations': exp_days,
        'contracts_count': gamma_result.get('contracts_count', len(strikes_data)),
        'expiration_scope': expiration_scope,
    }
    
    gamma_surface = {
        'strikes': strikes_data,
        'pin_strike': gamma_result.get('pin_strike', 0),
        'pin_abs_gex': abs(gamma_result.get('net_gex', 0)),
        'zero_gamma_level': gamma_result.get('zero_gamma_level'),
        'zero_gamma_method': gamma_result.get('zero_gamma_method'),
        # Corrected GEX fields
        'call_gex_total': agg_result.call_gex_total,
        'put_gex_total': agg_result.put_gex_total,
        'gross_gex': agg_result.gross_gex,
        'net_gex': agg_result.net_gex,
        # Legacy fields
        'total_gex_abs': agg_result.gross_gex,
        'total_gex_net': agg_result.net_gex,
    }
    
    return build_audit_snapshot(symbol, spot_state, chain_snapshot, gamma_surface, prev_snapshot=prev_snapshot)
