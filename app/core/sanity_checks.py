# app/core/sanity_checks.py
"""
Sanity Gates for Gamma Data Validation.

These are HARD FAIL conditions. If any gate fails:
1. Gamma features are EXCLUDED from the price model
2. Snapshot still persists with INVALID marker
3. Failure reason is explicitly recorded

These are not warnings - they are gates that prevent corrupted data from entering the model.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import List, Tuple, Optional

from app.core.audit_snapshot import AuditSnapshot

log = logging.getLogger("sanity_checks")


@dataclass
class ValidationResult:
    """Result of gamma snapshot validation."""
    is_valid: bool
    failure_reasons: List[str] = field(default_factory=list)
    gamma_excluded_from_model: bool = False  # Set True when validation fails
    
    def add_failure(self, reason: str):
        """Add a failure reason and mark as invalid."""
        self.is_valid = False
        self.failure_reasons.append(reason)
        self.gamma_excluded_from_model = True


# Symbol-specific thresholds (can be tuned per index)
SYMBOL_THRESHOLDS = {
    'SPX': {
        'max_pin_distance_pct': 0.10,  # 10% max distance from spot to pin
        'max_gex_concentration_pct': 0.90,  # No single strike > 90% of total
        'min_contracts': 50,  # Minimum contracts for valid gamma
        'max_contracts': 50000,  # Maximum reasonable contracts
        'spot_range': (4000, 10000),  # Order of magnitude check
    },
    'NDX': {
        'max_pin_distance_pct': 0.10,
        'max_gex_concentration_pct': 0.90,
        'min_contracts': 30,
        'max_contracts': 30000,
        'spot_range': (12000, 35000),
    },
    'RUT': {
        'max_pin_distance_pct': 0.10,
        'max_gex_concentration_pct': 0.90,
        'min_contracts': 20,
        'max_contracts': 20000,
        'spot_range': (1500, 4000),
    },
    'DJI': {
        'max_pin_distance_pct': 0.10,
        'max_gex_concentration_pct': 0.90,
        'min_contracts': 10,
        'max_contracts': 10000,
        'spot_range': (30000, 60000),
    },
}

DEFAULT_THRESHOLDS = {
    'max_pin_distance_pct': 0.10,
    'max_gex_concentration_pct': 0.90,
    'min_contracts': 10,
    'max_contracts': 50000,
    'spot_range': (100, 100000),
}


def get_thresholds(symbol: str) -> dict:
    """Get thresholds for a symbol, with defaults as fallback."""
    return SYMBOL_THRESHOLDS.get(symbol.upper(), DEFAULT_THRESHOLDS)


def validate_gamma_snapshot(snapshot: AuditSnapshot) -> ValidationResult:
    """
    Validate a gamma snapshot against sanity gates.
    
    HARD FAIL conditions (if ANY are true, gamma is INVALID):
    1. abs(primary_pin - spot) / spot > max_pin_distance_pct
    2. top strike abs_gex > max_gex_concentration_pct of total_gex_abs
    3. strike domain scale incompatible with spot (order-of-magnitude check)
    4. contracts_count outside min/max bounds
    
    Args:
        snapshot: The AuditSnapshot to validate
    
    Returns:
        ValidationResult with is_valid, failure_reasons, and gamma_excluded_from_model
    """
    result = ValidationResult(is_valid=True)
    
    symbol = snapshot.symbol.upper()
    thresholds = get_thresholds(symbol)
    
    spot = snapshot.spot_last
    pin = snapshot.primary_gamma_pin_strike
    contracts = snapshot.contracts_count
    total_gex_abs = snapshot.total_gex_abs
    
    # === GATE 1: Pin distance check ===
    if spot > 0 and pin > 0:
        pin_distance_pct = abs(pin - spot) / spot
        max_distance = thresholds['max_pin_distance_pct']
        
        if pin_distance_pct > max_distance:
            result.add_failure(
                f"GATE1_PIN_DISTANCE: Pin strike ${pin:.2f} is {pin_distance_pct*100:.1f}% "
                f"from spot ${spot:.2f} (max allowed: {max_distance*100:.0f}%)"
            )
    elif spot <= 0:
        result.add_failure("GATE1_PIN_DISTANCE: Invalid spot price <= 0")
    
    # === GATE 2: GEX concentration check ===
    if snapshot.top_strikes_by_abs_gex and total_gex_abs > 0:
        top_strike_gex = snapshot.top_strikes_by_abs_gex[0].get('abs_gex', 0)
        concentration_pct = top_strike_gex / total_gex_abs
        max_concentration = thresholds['max_gex_concentration_pct']
        
        if concentration_pct > max_concentration:
            result.add_failure(
                f"GATE2_GEX_CONCENTRATION: Top strike has {concentration_pct*100:.1f}% of total GEX "
                f"(max allowed: {max_concentration*100:.0f}%)"
            )
    
    # === GATE 3: Strike domain scale check ===
    if snapshot.top_strikes_by_abs_gex and spot > 0:
        strikes = [s.get('strike', 0) for s in snapshot.top_strikes_by_abs_gex]
        if strikes:
            min_strike = min(strikes)
            max_strike = max(strikes)
            
            spot_order_of_magnitude = len(str(int(spot)))
            
            # Check if strikes are in the same order of magnitude as spot
            for strike in [min_strike, max_strike]:
                if strike > 0:
                    strike_oom = len(str(int(strike)))
                    if abs(strike_oom - spot_order_of_magnitude) > 1:
                        result.add_failure(
                            f"GATE3_STRIKE_DOMAIN: Strike ${strike:.2f} is order-of-magnitude "
                            f"different from spot ${spot:.2f}"
                        )
                        break
    
    # === GATE 4: Contracts count check ===
    min_contracts = thresholds['min_contracts']
    max_contracts = thresholds['max_contracts']
    
    if contracts < min_contracts:
        result.add_failure(
            f"GATE4_CONTRACT_COUNT: Only {contracts} contracts "
            f"(minimum required: {min_contracts})"
        )
    elif contracts > max_contracts:
        result.add_failure(
            f"GATE4_CONTRACT_COUNT: {contracts} contracts exceeds maximum "
            f"({max_contracts})"
        )
    
    # === GATE 5: Spot price range check ===
    spot_min, spot_max = thresholds['spot_range']
    if spot < spot_min or spot > spot_max:
        result.add_failure(
            f"GATE5_SPOT_RANGE: Spot ${spot:.2f} outside expected range "
            f"[${spot_min:.2f}, ${spot_max:.2f}] for {symbol}"
        )
    
    # === GATE 6: Pre-gate checks (from build_audit_snapshot) ===
    # These cover chain thinness, active strikes count, and extreme concentration
    MIN_NONZERO_STRIKES = 10
    if snapshot.pregate_reason:
        result.add_failure(f"GATE6_PREGATE: {snapshot.pregate_reason}")
    elif snapshot.nonzero_strike_count is not None and snapshot.nonzero_strike_count < MIN_NONZERO_STRIKES:
        result.add_failure(
            f"GATE6_PREGATE: Only {snapshot.nonzero_strike_count} strikes with non-zero GEX "
            f"(minimum required: {MIN_NONZERO_STRIKES})"
        )
    
    # === GATE 7: GEX Invariants (from build_audit_snapshot) ===
    # These are locked invariants: gross_gex >= abs(net_gex) and gross_gex == call + put
    if total_gex_abs > 0:
        call_total = snapshot.call_gex_total or 0
        put_total = snapshot.put_gex_total or 0
        expected_gross = call_total + put_total
        
        # Invariant 1: gross >= abs(net)
        if snapshot.gross_gex is not None and snapshot.net_gex is not None:
            if snapshot.gross_gex < abs(snapshot.net_gex) - 1e-9:
                result.add_failure(
                    f"GATE7_INVARIANT: gross_gex ({snapshot.gross_gex:.4f}) < abs(net_gex) ({abs(snapshot.net_gex):.4f})"
                )
        
        # Invariant 2: gross == call + put
        if snapshot.gross_gex is not None and abs(snapshot.gross_gex - expected_gross) > 1e-9:
            result.add_failure(
                f"GATE7_INVARIANT: gross_gex ({snapshot.gross_gex:.4f}) != call_gex + put_gex ({expected_gross:.4f})"
            )
    
    # Log validation result
    if result.is_valid:
        log.debug(f"Gamma validation PASSED for {symbol}")
    else:
        log.warning(f"Gamma validation FAILED for {symbol}: {result.failure_reasons}")
    
    return result


def apply_validation_to_snapshot(snapshot: AuditSnapshot) -> AuditSnapshot:
    """
    Apply validation and update snapshot with results.
    
    Args:
        snapshot: The AuditSnapshot to validate
    
    Returns:
        Updated AuditSnapshot with validation results embedded
    """
    result = validate_gamma_snapshot(snapshot)
    
    # Update snapshot with validation results
    snapshot.validation_is_valid = result.is_valid
    snapshot.validation_failure_reasons = result.failure_reasons
    snapshot.gamma_excluded_from_model = result.gamma_excluded_from_model
    
    return snapshot


def should_use_gamma_in_model(snapshot: AuditSnapshot) -> bool:
    """
    Determine if gamma data should be used in the prediction model.
    
    Returns False if validation failed, True otherwise.
    """
    return snapshot.validation_is_valid and not snapshot.gamma_excluded_from_model


def format_validation_summary(result: ValidationResult) -> str:
    """Format validation result as a human-readable summary."""
    if result.is_valid:
        return "VALID: All sanity gates passed"
    
    lines = [f"INVALID: {len(result.failure_reasons)} gate(s) failed"]
    for reason in result.failure_reasons:
        lines.append(f"  - {reason}")
    return "\n".join(lines)
