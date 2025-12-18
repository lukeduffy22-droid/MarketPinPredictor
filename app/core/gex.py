# app/core/gex.py
"""
Canonical GEX (Gamma Exposure) computation module.
This is the SINGLE SOURCE OF TRUTH for ALL GEX logic.
All other files MUST import from this module. No inline definitions anywhere.

=== GLOBAL GEX DEFINITIONS (NON-NEGOTIABLE) ===

Per-Strike:
    net_gex_per_strike = call_gex - put_gex
    
Aggregate (across ALL strikes):
    TOTAL_GEX_ABS = sum(abs(net_gex_per_strike))   # Gross exposure magnitude
    TOTAL_GEX_NET = sum(net_gex_per_strike)        # Net directional exposure

These definitions must be used everywhere. No exceptions.
UI labels must map 1:1 to these definitions.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class GexResult:
    """Result of GEX computation with all components."""
    net_gex: float
    total_gex: float
    call_gex: float
    put_gex: float


def compute_gex(
    gamma_call: float,
    oi_call: float,
    gamma_put: float,
    oi_put: float,
    multiplier: float = 100.0,
) -> GexResult:
    """
    Canonical GEX computation.

    Rules:
    - Gamma inputs must be unsigned magnitudes (>= 0).
    - Exposure is computed separately for calls and puts.
    - Sign convention applied after exposure:
        call_gex = gamma_call * oi_call * multiplier (positive)
        put_gex = gamma_put * oi_put * multiplier (positive magnitude)
        net_gex = +call_gex - put_gex
        total_gex = |call_gex| + |put_gex|
    - Invariant: total_gex >= abs(net_gex)
    
    Args:
        gamma_call: Unsigned gamma magnitude for calls (>= 0)
        oi_call: Open interest for calls (>= 0)
        gamma_put: Unsigned gamma magnitude for puts (>= 0)
        oi_put: Open interest for puts (>= 0)
        multiplier: Contract multiplier (default 100 for options)
    
    Returns:
        GexResult with net_gex, total_gex, call_gex, put_gex
    
    Raises:
        ValueError: If inputs are None, negative, or invalid
        TypeError: If inputs are not numeric
        AssertionError: If invariant is violated (should never happen)
    """
    # Defensive checks (fail closed; do not silently proceed on bad inputs)
    for name, v in (
        ("gamma_call", gamma_call),
        ("gamma_put", gamma_put),
        ("oi_call", oi_call),
        ("oi_put", oi_put),
        ("multiplier", multiplier),
    ):
        if v is None:
            raise ValueError(f"{name} is None")
        if not isinstance(v, (int, float)):
            raise TypeError(f"{name} must be numeric, got {type(v)}")

    if gamma_call < 0 or gamma_put < 0:
        raise ValueError("Gamma inputs must be unsigned magnitudes (>= 0).")
    if oi_call < 0 or oi_put < 0:
        raise ValueError("Open interest must be >= 0.")
    if multiplier <= 0:
        raise ValueError("Multiplier must be > 0.")

    # Compute exposure separately for calls and puts
    call_gex = float(gamma_call) * float(oi_call) * float(multiplier)
    put_gex = float(gamma_put) * float(oi_put) * float(multiplier)

    # Apply sign convention: calls positive, puts negative in net
    net_gex = call_gex - put_gex
    total_gex = abs(call_gex) + abs(put_gex)

    # Hard invariant check
    if total_gex + 1e-12 < abs(net_gex):
        raise AssertionError("Invariant violated: total_gex must be >= abs(net_gex)")

    return GexResult(
        net_gex=net_gex,
        total_gex=total_gex,
        call_gex=call_gex,
        put_gex=put_gex
    )


def validate_gex_invariant(net_gex: float, total_gex: float) -> bool:
    """
    Validate the GEX invariant: total_gex >= abs(net_gex).
    
    Args:
        net_gex: Net gamma exposure
        total_gex: Total gamma exposure
    
    Returns:
        True if invariant holds, False otherwise
    """
    return total_gex >= abs(net_gex) - 1e-9


def assert_gex_invariant(net_gex: float, total_gex: float, context: str = ""):
    """
    Assert the GEX invariant, raising AssertionError if violated.
    
    Args:
        net_gex: Net gamma exposure
        total_gex: Total gamma exposure
        context: Optional context string for error message
    
    Raises:
        AssertionError: If invariant is violated
    """
    if not validate_gex_invariant(net_gex, total_gex):
        raise AssertionError(
            f"GEX invariant violated{f' ({context})' if context else ''}: "
            f"total_gex ({total_gex}) must be >= abs(net_gex) ({abs(net_gex)})"
        )


# =============================================================================
# AGGREGATE GEX DEFINITIONS (across all strikes)
# These are the ONLY functions that compute aggregate GEX. No exceptions.
# =============================================================================

from typing import List, Dict, Any


@dataclass(frozen=True)
class AggregateGexResult:
    """Result of aggregate GEX computation across all strikes.
    
    CORRECTED DEFINITIONS:
    - gross_gex = sum(call_gex) + sum(put_gex)  # Total gamma magnitude (always positive)
    - net_gex = sum(call_gex) - sum(put_gex)    # Net directional exposure (signed)
    - call_gex_total = sum(call_gex)            # Aggregate call gamma
    - put_gex_total = sum(put_gex)              # Aggregate put gamma
    
    Legacy fields (kept for compatibility):
    - total_gex_abs = gross_gex (same)
    - total_gex_net = net_gex (same)
    """
    gross_gex: float       # sum(call_gex) + sum(put_gex) - total gamma magnitude
    net_gex: float         # sum(call_gex) - sum(put_gex) - signed net exposure
    call_gex_total: float  # aggregate call gamma exposure
    put_gex_total: float   # aggregate put gamma exposure
    strike_count: int      # number of strikes aggregated
    
    # Legacy aliases for backward compatibility
    @property
    def total_gex_abs(self) -> float:
        """Legacy alias for gross_gex."""
        return self.gross_gex
    
    @property
    def total_gex_net(self) -> float:
        """Legacy alias for net_gex."""
        return self.net_gex


def compute_aggregate_gex(strikes_data: List[Dict[str, Any]]) -> AggregateGexResult:
    """
    Compute aggregate GEX across all strikes.
    
    CORRECTED DEFINITIONS:
        gross_gex = sum(call_gex) + sum(put_gex)  # Total gamma magnitude
        net_gex = sum(call_gex) - sum(put_gex)    # Signed net exposure
        call_gex_total = sum(call_gex)
        put_gex_total = sum(put_gex)
    
    Args:
        strikes_data: List of dicts with 'call_gex' and 'put_gex' keys.
                      Falls back to 'net_gex' for backward compatibility.
                      Expected format: [{'strike': 100, 'call_gex': 0.5, 'put_gex': 0.3}, ...]
    
    Returns:
        AggregateGexResult with gross_gex, net_gex, call_gex_total, put_gex_total
    
    Raises:
        ValueError: If input is invalid
    """
    if not isinstance(strikes_data, list):
        raise ValueError("strikes_data must be a list")
    
    if len(strikes_data) == 0:
        return AggregateGexResult(
            gross_gex=0.0,
            net_gex=0.0,
            call_gex_total=0.0,
            put_gex_total=0.0,
            strike_count=0
        )
    
    call_gex_total = 0.0
    put_gex_total = 0.0
    
    for strike_entry in strikes_data:
        if not isinstance(strike_entry, dict):
            raise ValueError(f"Each strike entry must be a dict, got {type(strike_entry)}")
        
        # Use call_gex/put_gex if available, otherwise fall back to net_gex
        if 'call_gex' in strike_entry and 'put_gex' in strike_entry:
            call_gex_total += float(strike_entry['call_gex'])
            put_gex_total += float(strike_entry['put_gex'])
        elif 'net_gex' in strike_entry:
            # Backward compatibility: if only net_gex provided
            # Treat positive net_gex as call-dominated, negative as put-dominated
            net_gex = float(strike_entry['net_gex'])
            if net_gex >= 0:
                call_gex_total += net_gex
            else:
                put_gex_total += abs(net_gex)
        else:
            raise ValueError(f"Strike entry missing 'call_gex'/'put_gex' or 'net_gex' keys: {strike_entry}")
    
    gross_gex = call_gex_total + put_gex_total
    net_gex = call_gex_total - put_gex_total
    
    return AggregateGexResult(
        gross_gex=gross_gex,
        net_gex=net_gex,
        call_gex_total=call_gex_total,
        put_gex_total=put_gex_total,
        strike_count=len(strikes_data)
    )


def compute_aggregate_gex_from_arrays(
    net_gex_per_strike: List[float],
    call_gex_per_strike: Optional[List[float]] = None,
    put_gex_per_strike: Optional[List[float]] = None
) -> AggregateGexResult:
    """
    Compute aggregate GEX from arrays.
    
    CORRECTED DEFINITIONS:
        gross_gex = sum(call_gex) + sum(put_gex)  # Total gamma magnitude
        net_gex = sum(call_gex) - sum(put_gex)    # Signed net exposure
    
    Args:
        net_gex_per_strike: List of net GEX values (for backward compatibility)
        call_gex_per_strike: Optional list of call GEX values
        put_gex_per_strike: Optional list of put GEX values
    
    Returns:
        AggregateGexResult with gross_gex, net_gex, call_gex_total, put_gex_total
    """
    if not isinstance(net_gex_per_strike, (list, tuple)):
        raise ValueError("net_gex_per_strike must be a list or tuple")
    
    if len(net_gex_per_strike) == 0:
        return AggregateGexResult(
            gross_gex=0.0,
            net_gex=0.0,
            call_gex_total=0.0,
            put_gex_total=0.0,
            strike_count=0
        )
    
    # If call/put arrays provided, use them for accurate computation
    if call_gex_per_strike is not None and put_gex_per_strike is not None:
        call_gex_total = sum(float(x) for x in call_gex_per_strike)
        put_gex_total = sum(float(x) for x in put_gex_per_strike)
        gross_gex = call_gex_total + put_gex_total
        net_gex = call_gex_total - put_gex_total
    else:
        # Backward compatibility: estimate from net_gex
        # Positive net_gex -> call-dominated, negative -> put-dominated
        call_gex_total = sum(float(x) for x in net_gex_per_strike if x >= 0)
        put_gex_total = sum(abs(float(x)) for x in net_gex_per_strike if x < 0)
        gross_gex = call_gex_total + put_gex_total
        net_gex = sum(float(x) for x in net_gex_per_strike)
    
    return AggregateGexResult(
        gross_gex=gross_gex,
        net_gex=net_gex,
        call_gex_total=call_gex_total,
        put_gex_total=put_gex_total,
        strike_count=len(net_gex_per_strike)
    )
