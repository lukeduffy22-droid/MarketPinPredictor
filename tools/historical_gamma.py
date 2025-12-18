#!/usr/bin/env python3
"""
Historical Gamma Builder - Audit and Validation Infrastructure

This module builds historical gamma snapshots from Polygon data for model validation.
It uses the canonical GEX functions from app/core/gex.py.

=== IMPORTANT DISCLAIMERS ===
This is MODEL VALIDATION infrastructure, NOT a backtest for PnL.

=== HARD RULES ===
1. Do NOT invent missing historical OI - if OI data is unavailable, return None
2. Do NOT imply this recreates dealer positioning - we track magnitude only

Assumptions (explicit, logged):
- OI is from live snapshot only (NOT reconstructed for past dates)
- Dealer net positioning sign is UNKNOWN (track magnitude only)
- Gamma decay approximated from time-to-expiry
- Historical IV from end-of-day snapshots when available

What this validates:
- Does the live pin consistently overshoot?
- Is RUT structurally noisier than SPX?
- Do errors cluster on high-vol days?
- Does pin accuracy degrade with skew?

What this does NOT do:
- Does NOT reconstruct true dealer inventory or positioning
- Does NOT invent or estimate historical OI data
- Does NOT capture intraday OI changes
- Does NOT imply tradable historical signal
"""
import os
import math
import logging
from datetime import datetime, date, timedelta
from typing import Optional, List, Dict, Any
from dataclasses import dataclass, field, asdict
import json

log = logging.getLogger("historical_gamma")

HISTORICAL_SNAPSHOT_DIR = "./logs/historical"


@dataclass
class HistoricalGammaSnapshot:
    """Historical gamma snapshot for validation."""
    symbol: str
    date: str
    spot: float
    pin_strike: Optional[float]
    contracts_count: int
    gamma_rows: List[Dict[str, Any]] = field(default_factory=list)
    total_gex_abs: float = 0.0
    total_gex_net: float = 0.0
    assumptions: Dict[str, str] = field(default_factory=dict)
    historical: bool = True
    generated_at_utc: str = ""
    
    # Diagnostic metrics (observational only - do NOT use to adjust gamma math)
    skew_metrics: Optional[Dict[str, float]] = None
    gamma_by_distance: Optional[Dict[str, float]] = None
    vol_regime: Optional[str] = None
    vol_regime_iv: Optional[float] = None
    truncation: Optional[Dict[str, Any]] = None
    confidence: Optional[float] = None
    confidence_factors: Optional[List[str]] = None
    dispersion_ratio: Optional[float] = None
    
    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)
    
    def to_json(self) -> str:
        return json.dumps(self.to_dict(), indent=2)


def compute_black_scholes_gamma(
    spot: float,
    strike: float,
    time_to_expiry: float,
    iv: float,
    r: float = 0.05
) -> float:
    """
    Compute Black-Scholes gamma for an option.
    
    Args:
        spot: Current underlying price
        strike: Option strike price
        time_to_expiry: Time to expiry in years (e.g., 1/252 for 1 day)
        iv: Implied volatility (annualized, e.g., 0.20 for 20%)
        r: Risk-free rate (default 5%)
    
    Returns:
        Gamma value (unsigned magnitude)
    """
    if time_to_expiry <= 0 or iv <= 0 or spot <= 0 or strike <= 0:
        return 0.0
    
    try:
        sqrt_t = math.sqrt(time_to_expiry)
        d1 = (math.log(spot / strike) + (r + 0.5 * iv * iv) * time_to_expiry) / (iv * sqrt_t)
        
        pdf_d1 = math.exp(-0.5 * d1 * d1) / math.sqrt(2 * math.pi)
        gamma = pdf_d1 / (spot * iv * sqrt_t)
        
        return max(0.0, gamma)
    except (ValueError, ZeroDivisionError, OverflowError):
        return 0.0


def compute_contract_gamma(
    spot: float,
    strike: float,
    iv: float,
    time_to_expiry: float,
    open_interest: int,
    is_call: bool = True,
    multiplier: float = 100.0
) -> float:
    """
    Compute gamma exposure for a single contract.
    
    Uses Black-Scholes gamma and applies open interest.
    
    Args:
        spot: Current underlying price
        strike: Option strike price
        iv: Implied volatility (annualized)
        time_to_expiry: Time to expiry in years
        open_interest: Open interest for this contract
        is_call: True for call, False for put
        multiplier: Contract multiplier (default 100)
    
    Returns:
        Gamma exposure (always positive magnitude)
    """
    gamma = compute_black_scholes_gamma(spot, strike, time_to_expiry, iv)
    exposure = gamma * open_interest * multiplier
    return exposure


def find_gamma_pin_strike(
    spot: float,
    gamma_rows: List[Dict[str, Any]]
) -> Optional[float]:
    """
    Find the gamma pin strike (strike with highest absolute GEX).
    
    Args:
        spot: Current spot price
        gamma_rows: List of dicts with 'strike' and 'abs_gex' keys
    
    Returns:
        Pin strike, or None if no valid strikes
    """
    if not gamma_rows:
        return None
    
    valid_rows = [r for r in gamma_rows if abs(r.get('strike', 0) - spot) / spot < 0.15]
    
    if not valid_rows:
        return None
    
    max_row = max(valid_rows, key=lambda r: r.get('abs_gex', 0))
    return max_row.get('strike')


def aggregate_gamma(spot: float, gamma_rows: List[Dict[str, Any]]) -> Optional[float]:
    """
    Find the gamma pin using weighted average approach.
    
    Args:
        spot: Current spot price
        gamma_rows: List of dicts with 'strike' and 'abs_gex' keys
    
    Returns:
        Weighted gamma pin strike
    """
    if not gamma_rows:
        return None
    
    total_weight = sum(r.get('abs_gex', 0) for r in gamma_rows)
    if total_weight == 0:
        return find_gamma_pin_strike(spot, gamma_rows)
    
    weighted_sum = sum(r.get('strike', 0) * r.get('abs_gex', 0) for r in gamma_rows)
    return weighted_sum / total_weight


def get_polygon_client():
    """Get Polygon REST client."""
    from polygon import RESTClient
    api_key = os.getenv("POLYGON_API_KEY")
    if not api_key:
        raise ValueError("POLYGON_API_KEY not set")
    return RESTClient(api_key)


def get_underlying_close(client, symbol: str, target_date: date) -> Optional[float]:
    """
    Get the closing price for an index on a given date.
    
    Args:
        client: Polygon REST client
        symbol: Index symbol (SPX, NDX, DJI, RUT)
        target_date: The date to fetch
    
    Returns:
        Closing price or None
    """
    try:
        ticker = f"I:{symbol}"
        aggs = list(client.get_aggs(
            ticker,
            1,
            "day",
            target_date.strftime("%Y-%m-%d"),
            target_date.strftime("%Y-%m-%d"),
            limit=1
        ))
        
        if aggs and len(aggs) > 0:
            return float(aggs[0].close)
        
        return None
        
    except Exception as e:
        log.warning(f"Failed to get close for {symbol} on {target_date}: {e}")
        return None


def get_options_chain_snapshot(
    client,
    symbol: str,
    max_dte: int = 7
) -> List[Dict[str, Any]]:
    """
    Get current options chain snapshot from Polygon with OI data.
    
    Uses list_snapshot_options_chain for real-time OI data.
    This only works for current date (live snapshot).
    
    Args:
        client: Polygon REST client
        symbol: Index symbol (SPX, NDX, DJI, RUT)
        max_dte: Maximum DTE to include (default 7)
    
    Returns:
        List of contract data dicts with OI
    """
    contracts = []
    today = date.today()
    
    try:
        snapshot = client.list_snapshot_options_chain(symbol)
        
        for result in snapshot:
            try:
                details = getattr(result, 'details', None)
                if not details:
                    continue
                
                exp_date = getattr(details, 'expiration_date', None)
                if not exp_date:
                    continue
                
                if isinstance(exp_date, str):
                    exp_date = datetime.strptime(exp_date, "%Y-%m-%d").date()
                
                dte = (exp_date - today).days
                if dte < 0 or dte > max_dte:
                    continue
                
                open_interest = getattr(result, 'open_interest', 0) or 0
                if open_interest == 0:
                    continue
                
                contracts.append({
                    'ticker': getattr(details, 'ticker', ''),
                    'strike': float(getattr(details, 'strike_price', 0)),
                    'contract_type': getattr(details, 'contract_type', 'call'),
                    'open_interest': int(open_interest),
                    'expiration_date': str(exp_date),
                    'dte': dte,
                    'iv': getattr(result, 'implied_volatility', None),
                })
                
            except Exception as e:
                log.debug(f"Skipping contract: {e}")
                continue
        
        log.info(f"Found {len(contracts)} contracts for {symbol} with 0-{max_dte} DTE")
        return contracts
        
    except Exception as e:
        log.error(f"Failed to get options snapshot for {symbol}: {e}")
        return []


def get_options_chain_historical(
    client,
    symbol: str,
    target_date: date,
    expiration_date: date
) -> List[Dict[str, Any]]:
    """
    Get options contracts for a specific expiration date.
    
    NOTE: This uses list_options_contracts which does NOT include OI.
    For OI data, use get_options_chain_snapshot for current date only.
    
    Args:
        client: Polygon REST client
        symbol: Root symbol (SPX, SPXW for S&P, etc.)
        target_date: The date for which to fetch data
        expiration_date: Options expiration date
    
    Returns:
        List of contract data dicts (may not have OI)
    """
    contracts = []
    
    try:
        options = client.list_options_contracts(
            underlying_ticker=f"I:{symbol}",
            expiration_date=expiration_date.strftime("%Y-%m-%d"),
            limit=1000
        )
        
        for opt in options:
            try:
                contract_ticker = opt.ticker
                
                oi = getattr(opt, 'open_interest', None) or 0
                
                contracts.append({
                    'ticker': contract_ticker,
                    'strike': float(opt.strike_price),
                    'contract_type': opt.contract_type,
                    'open_interest': int(oi),
                    'expiration_date': str(opt.expiration_date),
                })
                
            except Exception as e:
                log.debug(f"Skipping contract: {e}")
                continue
        
        log.info(f"Found {len(contracts)} contracts for {symbol} expiring {expiration_date}")
        return contracts
        
    except Exception as e:
        log.error(f"Failed to get options chain for {symbol}: {e}")
        return []


def get_historical_iv(
    client,
    contract_ticker: str,
    target_date: date
) -> Optional[float]:
    """
    Get historical implied volatility for a contract.
    
    Note: Polygon may not have historical IV for all contracts.
    Falls back to a reasonable default if unavailable.
    
    Args:
        client: Polygon REST client
        contract_ticker: Options contract ticker
        target_date: The date for which to fetch IV
    
    Returns:
        IV as decimal (e.g., 0.20 for 20%), or None
    """
    try:
        snapshot = client.get_snapshot_option(contract_ticker, target_date.strftime("%Y-%m-%d"))
        
        if snapshot and hasattr(snapshot, 'implied_volatility'):
            return float(snapshot.implied_volatility)
        
        return None
        
    except Exception:
        return None


def build_today_snapshot(
    symbol: str,
    default_iv: float = 0.20,
    max_dte: int = 7
) -> Optional[HistoricalGammaSnapshot]:
    """
    Build a gamma snapshot for TODAY using current options chain.
    
    This uses list_snapshot_options_chain which includes OI data.
    Save these snapshots daily to build a validation dataset.
    
    Args:
        symbol: Index symbol (SPX, NDX, DJI, RUT)
        default_iv: Default IV to use when unavailable
        max_dte: Maximum DTE to include (default 7)
    
    Returns:
        HistoricalGammaSnapshot or None on failure
    """
    today = date.today()
    log.info(f"Building today's snapshot for {symbol} on {today}")
    
    try:
        client = get_polygon_client()
        
        spot = get_underlying_close(client, symbol, today)
        if spot is None:
            log.warning(f"No close yet for {symbol} on {today}, using previous")
            yesterday = today - timedelta(days=1)
            spot = get_underlying_close(client, symbol, yesterday)
            if spot is None:
                log.error(f"Could not get spot price for {symbol}")
                return None
        
        log.info(f"{symbol} spot: {spot}")
        
        all_contracts = get_options_chain_snapshot(client, symbol, max_dte)
        
        if not all_contracts:
            log.warning(f"No contracts found for {symbol}")
            return None
        
        log.info(f"Total contracts: {len(all_contracts)}")
        
        gamma_rows = []
        strike_data = {}
        
        for c in all_contracts:
            strike = c['strike']
            oi = c['open_interest']
            is_call = c['contract_type'].upper() == 'CALL'
            dte = c.get('dte', 0)
            
            time_to_expiry = max(dte, 1) / 252.0
            
            iv = c.get('iv') or default_iv
            if iv and iv > 1:
                iv = iv / 100.0
            
            gamma_exposure = compute_contract_gamma(
                spot=spot,
                strike=strike,
                iv=iv,
                time_to_expiry=time_to_expiry,
                open_interest=oi,
                is_call=is_call
            )
            
            if strike not in strike_data:
                strike_data[strike] = {'call_gex': 0.0, 'put_gex': 0.0, 'oi': 0}
            
            if is_call:
                strike_data[strike]['call_gex'] += gamma_exposure
            else:
                strike_data[strike]['put_gex'] += gamma_exposure
            strike_data[strike]['oi'] += oi
        
        for strike, data in strike_data.items():
            net_gex = data['call_gex'] - data['put_gex']
            abs_gex = abs(net_gex)
            
            gamma_rows.append({
                'strike': strike,
                'net_gex': net_gex,
                'abs_gex': abs_gex,
                'call_gex': data['call_gex'],
                'put_gex': data['put_gex'],
                'oi': data['oi']
            })
        
        gamma_rows.sort(key=lambda r: r['abs_gex'], reverse=True)
        
        pin_strike = find_gamma_pin_strike(spot, gamma_rows)
        
        from app.core.gex import compute_aggregate_gex
        agg_result = compute_aggregate_gex(gamma_rows)
        
        # Compute diagnostic metrics (observational only - do NOT adjust gamma)
        from app.core.audit_snapshot import (
            compute_skew_metrics,
            compute_gamma_by_distance,
            classify_vol_regime,
            compute_confidence_score,
            compute_dispersion_ratio
        )
        
        # F.1 Skew metrics
        skew_metrics = compute_skew_metrics(all_contracts, spot)
        
        # F.2 Gamma by distance
        gamma_by_distance = compute_gamma_by_distance(gamma_rows, spot)
        
        # F.3 Volatility regime
        ivs = [float(iv) for iv in [c.get('iv') for c in all_contracts] if iv is not None]
        avg_iv = sum(ivs) / len(ivs) if ivs else None
        vol_regime = classify_vol_regime(avg_iv) if avg_iv else None
        
        # F.4 Truncation metrics (no truncation for live snapshots)
        truncation = {
            "contracts_used": len(all_contracts),
            "contracts_available": len(all_contracts),
            "excluded_above": 0,
            "excluded_below": 0,
            "truncation_pct": 0.0
        }
        
        # F.5 Confidence score
        confidence, confidence_factors = compute_confidence_score(
            contracts_used=len(all_contracts),
            validation_failed=False,
            vol_regime=vol_regime or "MEDIUM",
            truncation_pct=0.0
        )
        
        # F.6 Dispersion ratio
        dispersion_ratio = compute_dispersion_ratio(gamma_by_distance) if gamma_by_distance else None
        
        snapshot = HistoricalGammaSnapshot(
            symbol=symbol,
            date=today.strftime("%Y-%m-%d"),
            spot=spot,
            pin_strike=pin_strike,
            contracts_count=len(all_contracts),
            gamma_rows=gamma_rows[:15],
            total_gex_abs=agg_result.total_gex_abs,
            total_gex_net=agg_result.total_gex_net,
            assumptions={
                "oi_source": "Live snapshot from Polygon list_snapshot_options_chain",
                "dealer_positioning": "Dealer net sign unknown, tracking magnitude only",
                "gamma_decay": f"0-{max_dte} DTE contracts included",
                "iv_source": "Live IV from snapshot, or default if unavailable",
                "not_for_trading": "Model validation only, not a tradable signal"
            },
            historical=True,
            generated_at_utc=datetime.utcnow().isoformat(),
            skew_metrics=skew_metrics,
            gamma_by_distance=gamma_by_distance,
            vol_regime=vol_regime,
            vol_regime_iv=round(avg_iv, 4) if avg_iv else None,
            truncation=truncation,
            confidence=confidence,
            confidence_factors=confidence_factors,
            dispersion_ratio=dispersion_ratio
        )
        
        log.info(f"Today's snapshot built: pin={pin_strike}, contracts={len(all_contracts)}, total_gex_abs={agg_result.total_gex_abs:.2f}, confidence={confidence}")
        
        return snapshot
        
    except Exception as e:
        log.error(f"Failed to build today's snapshot for {symbol}: {e}")
        import traceback
        traceback.print_exc()
        return None


def build_historical_snapshot(
    symbol: str,
    target_date: date,
    default_iv: float = 0.20,
    max_dte: int = 7
) -> Optional[HistoricalGammaSnapshot]:
    """
    Build a gamma snapshot for a historical date.
    
    NOTE: For historical dates (not today), OI data is NOT available from Polygon
    standard endpoints. This function will:
    - For today: Delegate to build_today_snapshot (uses live OI)
    - For past dates: Attempt to load from stored snapshots
    
    To build a historical validation dataset:
    1. Run build_today_snapshot daily during market hours
    2. Save snapshots using save_historical_snapshot
    3. Validate using stored snapshots
    
    Args:
        symbol: Index symbol (SPX, NDX, DJI, RUT)
        target_date: The trading date to analyze
        default_iv: Default IV to use when historical IV unavailable
        max_dte: Maximum DTE to include (default 7)
    
    Returns:
        HistoricalGammaSnapshot or None on failure
    """
    today = date.today()
    
    if target_date == today:
        return build_today_snapshot(symbol, default_iv, max_dte)
    
    log.info(f"Building historical snapshot for {symbol} on {target_date}")
    
    stored = load_historical_snapshot(symbol, target_date)
    if stored:
        log.info(f"Loaded stored snapshot for {symbol} on {target_date}")
        return stored
    
    log.warning(
        f"Historical OI data not available from Polygon for {target_date}. "
        f"Snapshots must be pre-built during market hours using build_today_snapshot. "
        f"Refusing to build snapshot with missing/invented OI data."
    )
    return None


def save_historical_snapshot(snapshot: HistoricalGammaSnapshot) -> Optional[str]:
    """
    Save a historical snapshot to disk.
    
    Args:
        snapshot: The snapshot to save
    
    Returns:
        Path to saved file or None
    """
    try:
        from pathlib import Path
        
        snapshot_dir = Path(HISTORICAL_SNAPSHOT_DIR) / snapshot.symbol
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        
        filename = f"{snapshot.date}.json"
        filepath = snapshot_dir / filename
        
        with open(filepath, 'w') as f:
            f.write(snapshot.to_json())
        
        log.info(f"Historical snapshot saved: {filepath}")
        return str(filepath)
        
    except Exception as e:
        log.error(f"Failed to save historical snapshot: {e}")
        return None


def load_historical_snapshot(symbol: str, target_date: date) -> Optional[HistoricalGammaSnapshot]:
    """
    Load a historical snapshot from disk.
    
    Args:
        symbol: Index symbol
        target_date: The date
    
    Returns:
        HistoricalGammaSnapshot or None
    """
    try:
        from pathlib import Path
        
        filepath = Path(HISTORICAL_SNAPSHOT_DIR) / symbol / f"{target_date.strftime('%Y-%m-%d')}.json"
        
        if not filepath.exists():
            return None
        
        with open(filepath, 'r') as f:
            data = json.load(f)
        
        return HistoricalGammaSnapshot(**data)
        
    except Exception as e:
        log.error(f"Failed to load historical snapshot: {e}")
        return None


def get_trading_days(start_date: date, end_date: date) -> List[date]:
    """
    Get list of trading days between two dates.
    Excludes weekends. Does not exclude holidays (caller should handle).
    
    Args:
        start_date: Start date
        end_date: End date
    
    Returns:
        List of trading dates
    """
    trading_days = []
    current = start_date
    
    while current <= end_date:
        if current.weekday() < 5:
            trading_days.append(current)
        current += timedelta(days=1)
    
    return trading_days


def build_batch_historical(
    symbol: str,
    days: int = 30,
    end_date: Optional[date] = None
) -> List[HistoricalGammaSnapshot]:
    """
    Build historical snapshots for the last N trading days.
    
    Args:
        symbol: Index symbol
        days: Number of trading days to fetch
        end_date: End date (default: yesterday)
    
    Returns:
        List of successfully built snapshots
    """
    if end_date is None:
        end_date = date.today() - timedelta(days=1)
    
    start_date = end_date - timedelta(days=int(days * 1.5))
    
    trading_days = get_trading_days(start_date, end_date)
    trading_days = trading_days[-days:]
    
    log.info(f"Building {len(trading_days)} historical snapshots for {symbol}")
    
    snapshots = []
    for target_date in trading_days:
        try:
            existing = load_historical_snapshot(symbol, target_date)
            if existing:
                log.info(f"Using cached snapshot for {symbol} on {target_date}")
                snapshots.append(existing)
                continue
            
            snapshot = build_historical_snapshot(symbol, target_date)
            if snapshot:
                save_historical_snapshot(snapshot)
                snapshots.append(snapshot)
            
        except Exception as e:
            log.warning(f"Failed to build snapshot for {target_date}: {e}")
            continue
    
    log.info(f"Built {len(snapshots)}/{len(trading_days)} snapshots for {symbol}")
    return snapshots


if __name__ == "__main__":
    import argparse
    
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    parser = argparse.ArgumentParser(description="Build historical gamma snapshots")
    parser.add_argument("symbol", help="Index symbol (SPX, NDX, DJI, RUT)")
    parser.add_argument("--date", help="Target date (YYYY-MM-DD)", default=None)
    parser.add_argument("--days", type=int, default=30, help="Number of days for batch mode")
    parser.add_argument("--batch", action="store_true", help="Build batch of last N days")
    
    args = parser.parse_args()
    
    symbol = args.symbol.upper()
    
    if args.batch:
        snapshots = build_batch_historical(symbol, args.days)
        print(f"\nBuilt {len(snapshots)} snapshots for {symbol}")
        for s in snapshots:
            print(f"  {s.date}: pin={s.pin_strike}, spot={s.spot}")
    else:
        if args.date:
            target_date = datetime.strptime(args.date, "%Y-%m-%d").date()
        else:
            target_date = date.today() - timedelta(days=1)
        
        snapshot = build_historical_snapshot(symbol, target_date)
        if snapshot:
            save_historical_snapshot(snapshot)
            print(f"\nSnapshot for {symbol} on {target_date}:")
            print(f"  Spot: {snapshot.spot}")
            print(f"  Pin Strike: {snapshot.pin_strike}")
            print(f"  Contracts: {snapshot.contracts_count}")
            print(f"  Total GEX (abs): {snapshot.total_gex_abs:.2f}")
            print(f"  Total GEX (net): {snapshot.total_gex_net:.2f}")
