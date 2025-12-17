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
    chain_symbol_used: str  # Exact string passed to Polygon
    underlying_reported: Optional[str]  # From options payload if present
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
    chain_symbol_used: str = ""
    underlying_reported: Optional[str] = None
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
    total_gex_abs: float = 0.0  # sum(abs(net_gex_per_strike))
    total_gex_net: float = 0.0  # sum(net_gex_per_strike)
    total_gex_abs_definition: str = "sum(abs(net_gex_per_strike))"
    total_gex_net_definition: str = "sum(net_gex_per_strike)"
    
    # E. Validation result (populated by sanity_checks.py)
    validation_is_valid: bool = True
    validation_failure_reasons: List[str] = field(default_factory=list)
    gamma_excluded_from_model: bool = False
    
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
                      total_gex_abs, total_gex_net
    
    Returns:
        AuditSnapshot: Complete, deterministic snapshot
    """
    now_utc = datetime.now(timezone.utc).isoformat()
    
    snapshot = AuditSnapshot(
        snapshot_version="1.0",
        generated_at_utc=now_utc,
        
        # A. Core market state
        symbol=symbol,
        timestamp_utc=now_utc,
        spot_last=float(spot_state.get('price', 0.0)),
        spot_timestamp=str(spot_state.get('timestamp_utc', now_utc)),
        spot_source=str(spot_state.get('source', 'unknown')),
        
        # B. Options chain identity
        chain_symbol_used=str(chain_snapshot.get('chain_symbol_used', symbol)),
        underlying_reported=chain_snapshot.get('underlying_reported'),
        expirations_min_days=int(min(chain_snapshot.get('expirations', [0])) if chain_snapshot.get('expirations') else 0),
        expirations_max_days=int(max(chain_snapshot.get('expirations', [0])) if chain_snapshot.get('expirations') else 0),
        contracts_count=int(chain_snapshot.get('contracts_count', 0)),
        expiration_scope=str(chain_snapshot.get('expiration_scope', 'UNKNOWN')),
        
        # D. Derived gamma metrics
        primary_gamma_pin_strike=float(gamma_surface.get('pin_strike', 0.0)),
        primary_gamma_pin_abs_gex=float(gamma_surface.get('pin_abs_gex', 0.0)),
        zero_gamma_level=gamma_surface.get('zero_gamma_level'),
        zero_gamma_method=gamma_surface.get('zero_gamma_method'),
        total_gex_abs=float(gamma_surface.get('total_gex_abs', 0.0)),
        total_gex_net=float(gamma_surface.get('total_gex_net', 0.0)),
    )
    
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
    
    return snapshot


def snapshot_from_gamma_exposure_result(
    symbol: str,
    spot_price: float,
    spot_timestamp: str,
    spot_source: str,
    chain_symbol: str,
    gamma_result: Dict[str, Any],
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
    
    Returns:
        AuditSnapshot
    """
    from app.core.gex import compute_aggregate_gex_from_arrays
    
    # Extract strikes data from gamma_result if available
    strikes_data = gamma_result.get('gex_by_strike', [])
    
    # Get expiration days from strikes
    exp_days = list(set(s.get('expiration_days', 0) for s in strikes_data)) if strikes_data else [0]
    
    # Determine expiration scope
    if len(exp_days) == 1 and exp_days[0] == 0:
        expiration_scope = '0DTE'
    else:
        expiration_scope = f'ALL<={max(exp_days) if exp_days else 90}D'
    
    # Compute aggregate GEX using canonical function
    net_gex_values = [float(s.get('net_gex', 0)) for s in strikes_data]
    agg_result = compute_aggregate_gex_from_arrays(net_gex_values)
    
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
        'total_gex_abs': agg_result.total_gex_abs,
        'total_gex_net': agg_result.total_gex_net,
    }
    
    return build_audit_snapshot(symbol, spot_state, chain_snapshot, gamma_surface)
