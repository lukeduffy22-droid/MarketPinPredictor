# app/core/gex.py
"""
Canonical GEX (Gamma Exposure) computation module.
This is the SINGLE SOURCE OF TRUTH for net/total GEX logic.
All other files must import from this module.
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
