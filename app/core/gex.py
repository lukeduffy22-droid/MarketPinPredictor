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
    """Result of aggregate GEX computation across all strikes."""
    total_gex_abs: float   # sum(abs(net_gex_per_strike)) - gross magnitude
    total_gex_net: float   # sum(net_gex_per_strike) - net directional
    strike_count: int      # number of strikes aggregated


def compute_aggregate_gex(strikes_data: List[Dict[str, Any]]) -> AggregateGexResult:
    """
    Compute aggregate GEX across all strikes.
    
    DEFINITIONS (NON-NEGOTIABLE):
        TOTAL_GEX_ABS = sum(abs(net_gex_per_strike))
        TOTAL_GEX_NET = sum(net_gex_per_strike)
    
    Args:
        strikes_data: List of dicts, each with 'net_gex' key (net GEX for that strike)
                      Expected format: [{'strike': 100, 'net_gex': 0.5}, ...]
    
    Returns:
        AggregateGexResult with total_gex_abs and total_gex_net
    
    Raises:
        ValueError: If input is invalid
    """
    if not isinstance(strikes_data, list):
        raise ValueError("strikes_data must be a list")
    
    if len(strikes_data) == 0:
        return AggregateGexResult(
            total_gex_abs=0.0,
            total_gex_net=0.0,
            strike_count=0
        )
    
    total_gex_abs = 0.0
    total_gex_net = 0.0
    
    for strike_entry in strikes_data:
        if not isinstance(strike_entry, dict):
            raise ValueError(f"Each strike entry must be a dict, got {type(strike_entry)}")
        
        if 'net_gex' not in strike_entry:
            raise ValueError(f"Strike entry missing 'net_gex' key: {strike_entry}")
        
        net_gex = float(strike_entry['net_gex'])
        total_gex_abs += abs(net_gex)
        total_gex_net += net_gex
    
    return AggregateGexResult(
        total_gex_abs=total_gex_abs,
        total_gex_net=total_gex_net,
        strike_count=len(strikes_data)
    )


def compute_aggregate_gex_from_arrays(net_gex_per_strike: List[float]) -> AggregateGexResult:
    """
    Compute aggregate GEX from a simple list of net GEX values.
    
    DEFINITIONS (NON-NEGOTIABLE):
        TOTAL_GEX_ABS = sum(abs(net_gex_per_strike))
        TOTAL_GEX_NET = sum(net_gex_per_strike)
    
    Args:
        net_gex_per_strike: List of net GEX values, one per strike
    
    Returns:
        AggregateGexResult with total_gex_abs and total_gex_net
    """
    if not isinstance(net_gex_per_strike, (list, tuple)):
        raise ValueError("net_gex_per_strike must be a list or tuple")
    
    if len(net_gex_per_strike) == 0:
        return AggregateGexResult(
            total_gex_abs=0.0,
            total_gex_net=0.0,
            strike_count=0
        )
    
    total_gex_abs = sum(abs(float(x)) for x in net_gex_per_strike)
    total_gex_net = sum(float(x) for x in net_gex_per_strike)
    
    return AggregateGexResult(
        total_gex_abs=total_gex_abs,
        total_gex_net=total_gex_net,
        strike_count=len(net_gex_per_strike)
    )
