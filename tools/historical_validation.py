#!/usr/bin/env python3
"""
Historical Validation Pipeline

Compares historical gamma pin predictions against actual closes.
Feeds results into the accuracy ledger for analysis.

=== PURPOSE ===
This module validates the measurement system by:
1. Loading historical gamma snapshots
2. Comparing pin predictions to actual closes
3. Computing error metrics (MAE, bias, distribution)
4. Feeding results into accuracy ledger
5. Generating validation reports

=== WHAT THIS VALIDATES ===
- Does the live pin consistently overshoot?
- Is RUT structurally noisier than SPX?
- Do errors cluster on high-vol days?
- Does pin accuracy degrade with skew?

=== HARD RULES ===
1. Do NOT invent missing historical OI - validation requires pre-built snapshots
2. Do NOT imply this recreates dealer positioning - we track magnitude only

=== WHAT THIS DOES NOT DO ===
- Does NOT reconstruct true dealer inventory or positioning
- Does NOT invent or estimate historical OI data
- Does NOT capture intraday OI changes
- Does NOT imply tradable historical signal
"""
import os
import logging
from datetime import date, timedelta
from typing import Optional, List, Dict, Any
from dataclasses import dataclass
import json
import statistics

log = logging.getLogger("historical_validation")


@dataclass
class ValidationResult:
    """Result of validating a single historical snapshot."""
    symbol: str
    date: str
    pin_strike: Optional[float]
    actual_close: float
    spot_at_snapshot: float
    error_points: float
    error_pct: float
    direction_correct: bool
    contracts_count: int
    total_gex_abs: float


@dataclass
class ValidationSummary:
    """Summary statistics from validation."""
    symbol: str
    days_validated: int
    days_with_pin: int
    mae_points: float
    mae_pct: float
    bias_points: float
    direction_accuracy: float
    median_error: float
    max_error: float
    min_error: float
    error_std: float
    errors_by_date: List[Dict[str, Any]]


def validate_snapshot(
    snapshot_dict: Dict[str, Any],
    actual_close: float
) -> Optional[ValidationResult]:
    """
    Validate a single historical snapshot against actual close.
    
    Args:
        snapshot_dict: Historical snapshot data
        actual_close: The actual closing price for that date
    
    Returns:
        ValidationResult or None if pin is missing
    """
    symbol = snapshot_dict.get('symbol', '')
    date_str = snapshot_dict.get('date', '')
    pin_strike = snapshot_dict.get('pin_strike')
    spot = snapshot_dict.get('spot', 0)
    contracts_count = snapshot_dict.get('contracts_count', 0)
    total_gex_abs = snapshot_dict.get('total_gex_abs', 0)
    
    if pin_strike is None:
        log.warning(f"No pin strike for {symbol} on {date_str}")
        return None
    
    error_points = actual_close - pin_strike
    error_pct = (error_points / actual_close) * 100 if actual_close > 0 else 0
    
    spot_to_close_direction = actual_close > spot
    pin_to_spot_direction = pin_strike > spot
    direction_correct = spot_to_close_direction == pin_to_spot_direction
    
    return ValidationResult(
        symbol=symbol,
        date=date_str,
        pin_strike=pin_strike,
        actual_close=actual_close,
        spot_at_snapshot=spot,
        error_points=error_points,
        error_pct=error_pct,
        direction_correct=direction_correct,
        contracts_count=contracts_count,
        total_gex_abs=total_gex_abs
    )


def get_actual_close(symbol: str, target_date: date) -> Optional[float]:
    """
    Get the actual closing price for a date.
    
    Args:
        symbol: Index symbol
        target_date: The date
    
    Returns:
        Closing price or None
    """
    try:
        from polygon.rest import RESTClient
        api_key = os.getenv("Massive_API") or os.getenv("POLYGON_API_KEY")
        if not api_key:
            raise ValueError("Massive_API not set")
        
        client = RESTClient(api_key)
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
            close_price = getattr(aggs[0], 'close', None)
            if close_price is not None:
                return float(close_price)
        
        return None
        
    except Exception as e:
        log.warning(f"Failed to get close for {symbol} on {target_date}: {e}")
        return None


def validate_historical_snapshots(
    symbol: str,
    days: int = 30
) -> ValidationSummary:
    """
    Validate historical snapshots for a symbol.
    
    Args:
        symbol: Index symbol
        days: Number of days to validate
    
    Returns:
        ValidationSummary with aggregate statistics
    """
    from tools.historical_gamma import load_historical_snapshot, get_trading_days
    from datetime import datetime
    
    end_date = date.today() - timedelta(days=1)
    start_date = end_date - timedelta(days=int(days * 1.5))
    trading_days = get_trading_days(start_date, end_date)[-days:]
    
    results = []
    errors_by_date = []
    
    for target_date in trading_days:
        try:
            snapshot = load_historical_snapshot(symbol, target_date)
            if not snapshot:
                log.debug(f"No snapshot for {symbol} on {target_date}")
                continue
            
            actual_close = get_actual_close(symbol, target_date)
            if actual_close is None:
                log.debug(f"No actual close for {symbol} on {target_date}")
                continue
            
            result = validate_snapshot(snapshot.to_dict(), actual_close)
            if result:
                results.append(result)
                errors_by_date.append({
                    'date': result.date,
                    'pin': result.pin_strike,
                    'close': result.actual_close,
                    'error_points': result.error_points,
                    'error_pct': result.error_pct,
                    'direction_correct': result.direction_correct
                })
                
        except Exception as e:
            log.warning(f"Validation failed for {target_date}: {e}")
            continue
    
    if not results:
        return ValidationSummary(
            symbol=symbol,
            days_validated=0,
            days_with_pin=0,
            mae_points=0,
            mae_pct=0,
            bias_points=0,
            direction_accuracy=0,
            median_error=0,
            max_error=0,
            min_error=0,
            error_std=0,
            errors_by_date=[]
        )
    
    errors = [r.error_points for r in results]
    abs_errors = [abs(e) for e in errors]
    
    mae_points = statistics.mean(abs_errors)
    bias_points = statistics.mean(errors)
    direction_correct_count = sum(1 for r in results if r.direction_correct)
    direction_accuracy = direction_correct_count / len(results) if results else 0
    
    mae_pct = statistics.mean([abs(r.error_pct) for r in results])
    median_error = statistics.median(errors)
    max_error = max(abs_errors)
    min_error = min(abs_errors)
    error_std = statistics.stdev(errors) if len(errors) > 1 else 0
    
    return ValidationSummary(
        symbol=symbol,
        days_validated=len(trading_days),
        days_with_pin=len(results),
        mae_points=mae_points,
        mae_pct=mae_pct,
        bias_points=bias_points,
        direction_accuracy=direction_accuracy,
        median_error=median_error,
        max_error=max_error,
        min_error=min_error,
        error_std=error_std,
        errors_by_date=errors_by_date
    )


def feed_to_accuracy_ledger(results: List[ValidationResult]) -> int:
    """
    Feed validation results into the accuracy ledger.
    
    Args:
        results: List of ValidationResult objects
    
    Returns:
        Number of records added to ledger
    """
    try:
        from app.core.accuracy_ledger import record_accuracy
        
        count = 0
        for r in results:
            if r.pin_strike is None:
                continue
            
            snapshot_dict = {
                'timestamp_utc': f"{r.date}T16:00:00Z",
                'primary_gamma_pin_strike': r.pin_strike,
                'spot_last': r.spot_at_snapshot,
                'contracts_count': r.contracts_count,
                'validation_is_valid': True,
                'total_gex_abs': r.total_gex_abs,
            }
            
            record = record_accuracy(
                symbol=r.symbol,
                snapshot=snapshot_dict,
                official_close=r.actual_close,
                freeze_enforced=False,
            )
            
            if record:
                count += 1
        
        log.info(f"Added {count} records to accuracy ledger")
        return count
        
    except Exception as e:
        log.error(f"Failed to feed to accuracy ledger: {e}")
        return 0


def generate_validation_report(summary: ValidationSummary) -> str:
    """
    Generate a human-readable validation report.
    
    Args:
        summary: ValidationSummary object
    
    Returns:
        Formatted report string
    """
    report = f"""
===============================================================================
HISTORICAL GAMMA VALIDATION REPORT
===============================================================================
Symbol: {summary.symbol}
Days Analyzed: {summary.days_validated}
Days with Valid Pin: {summary.days_with_pin}

=== ERROR METRICS ===
Mean Absolute Error (Points): {summary.mae_points:.2f}
Mean Absolute Error (%):      {summary.mae_pct:.3f}%
Bias (Points):                {summary.bias_points:+.2f}
Median Error:                 {summary.median_error:+.2f}
Std Dev of Error:             {summary.error_std:.2f}
Max Absolute Error:           {summary.max_error:.2f}
Min Absolute Error:           {summary.min_error:.2f}

=== DIRECTIONAL ACCURACY ===
Direction Correct:            {summary.direction_accuracy*100:.1f}%

=== INTERPRETATION ===
"""
    
    if summary.bias_points > 0:
        report += f"- Gamma pin tends to UNDERESTIMATE close by {summary.bias_points:.2f} points\n"
    elif summary.bias_points < 0:
        report += f"- Gamma pin tends to OVERESTIMATE close by {abs(summary.bias_points):.2f} points\n"
    else:
        report += "- Gamma pin is unbiased\n"
    
    if summary.mae_pct < 0.5:
        report += "- Error magnitude is LOW (< 0.5%)\n"
    elif summary.mae_pct < 1.0:
        report += "- Error magnitude is MODERATE (0.5% - 1.0%)\n"
    else:
        report += "- Error magnitude is HIGH (> 1.0%)\n"
    
    if summary.direction_accuracy > 0.6:
        report += "- Directional accuracy is GOOD (> 60%)\n"
    elif summary.direction_accuracy > 0.5:
        report += "- Directional accuracy is MARGINAL (50-60%)\n"
    else:
        report += "- Directional accuracy is POOR (< 50%)\n"
    
    report += """
=== ASSUMPTIONS (logged for audit) ===
- OI is static intraday (end-of-day snapshot)
- Dealer net positioning sign unknown (tracking magnitude only)
- Gamma decay approximated from time-to-expiry
- This is model VALIDATION, not a tradable backtest
===============================================================================
"""
    
    return report


def run_full_validation(
    symbols: Optional[List[str]] = None,
    days: int = 30,
    build_missing: bool = True,
    feed_ledger: bool = False
) -> Dict[str, ValidationSummary]:
    """
    Run full validation pipeline for multiple symbols.
    
    Args:
        symbols: List of symbols to validate (default: all)
        days: Number of days to validate
        build_missing: Whether to build missing historical snapshots
        feed_ledger: Whether to feed results to accuracy ledger
    
    Returns:
        Dict mapping symbol to ValidationSummary
    """
    if symbols is None:
        symbols = ["SPX", "NDX", "DJI", "RUT"]
    
    results = {}
    
    for symbol in symbols:
        log.info(f"Validating {symbol}...")
        
        if build_missing:
            from tools.historical_gamma import build_batch_historical
            build_batch_historical(symbol, days)
        
        summary = validate_historical_snapshots(symbol, days)
        results[symbol] = summary
        
        print(generate_validation_report(summary))
    
    return results


if __name__ == "__main__":
    import argparse
    
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    parser = argparse.ArgumentParser(description="Validate historical gamma predictions")
    parser.add_argument("--symbol", help="Symbol to validate (default: all)", default=None)
    parser.add_argument("--days", type=int, default=30, help="Number of days to validate")
    parser.add_argument("--build", action="store_true", help="Build missing historical snapshots")
    parser.add_argument("--feed-ledger", action="store_true", help="Feed results to accuracy ledger")
    
    args = parser.parse_args()
    
    symbols = [args.symbol.upper()] if args.symbol else None
    
    run_full_validation(
        symbols=symbols,
        days=args.days,
        build_missing=args.build,
        feed_ledger=args.feed_ledger
    )
