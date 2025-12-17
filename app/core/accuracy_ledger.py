"""
Accuracy Ledger - Post-Close Prediction Accuracy Tracking

Records prediction accuracy after market close for auditability:
- Pin strike vs official close
- Error in points and percentage
- Freeze enforcement status
- Confidence score at time of prediction

Schema: logs/accuracy_ledger.csv
date,symbol,snapshot_time,pin,spot_at_snapshot,official_close,error_points,error_pct,freeze_enforced,confidence
"""
from __future__ import annotations

import csv
import os
import logging
from datetime import datetime, date
from pathlib import Path
from typing import Optional, Dict, Any, List
from dataclasses import dataclass

log = logging.getLogger("accuracy_ledger")

LEDGER_PATH = Path("logs/accuracy_ledger.csv")

LEDGER_HEADERS = [
    "date",
    "symbol", 
    "snapshot_time",
    "pin",
    "spot_at_snapshot",
    "official_close",
    "error_points",
    "error_pct",
    "freeze_enforced",
    "confidence",
    "contracts_count",
    "expiration_scope",
    "validation_is_valid",
]


@dataclass
class AccuracyRecord:
    """Single accuracy record for the ledger."""
    date: str
    symbol: str
    snapshot_time: str
    pin: float
    spot_at_snapshot: float
    official_close: float
    error_points: float
    error_pct: float
    freeze_enforced: bool
    confidence: float
    contracts_count: int
    expiration_scope: str
    validation_is_valid: bool
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "date": self.date,
            "symbol": self.symbol,
            "snapshot_time": self.snapshot_time,
            "pin": self.pin,
            "spot_at_snapshot": self.spot_at_snapshot,
            "official_close": self.official_close,
            "error_points": self.error_points,
            "error_pct": self.error_pct,
            "freeze_enforced": self.freeze_enforced,
            "confidence": self.confidence,
            "contracts_count": self.contracts_count,
            "expiration_scope": self.expiration_scope,
            "validation_is_valid": self.validation_is_valid,
        }


def ensure_ledger_exists() -> None:
    """Create the ledger file with headers if it doesn't exist."""
    LEDGER_PATH.parent.mkdir(parents=True, exist_ok=True)
    
    if not LEDGER_PATH.exists():
        with open(LEDGER_PATH, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(LEDGER_HEADERS)
        log.info(f"Created accuracy ledger: {LEDGER_PATH}")


def record_accuracy(
    symbol: str,
    snapshot: Dict[str, Any],
    official_close: float,
    freeze_enforced: bool = True,
) -> Optional[AccuracyRecord]:
    """
    Record prediction accuracy after market close.
    
    This function should be called AFTER market close with the official
    closing price to calculate and log prediction accuracy.
    
    Args:
        symbol: Index symbol (SPX, NDX, DJI, RUT)
        snapshot: Audit snapshot dictionary or AuditSnapshot object
        official_close: Official closing price from data provider
        freeze_enforced: Whether the freeze was enforced (should always be True)
    
    Returns:
        AccuracyRecord if successful, None otherwise
    """
    try:
        ensure_ledger_exists()
        
        if hasattr(snapshot, 'to_dict'):
            snapshot = snapshot.__dict__
        
        snapshot_date = snapshot.get('timestamp_utc', '')[:10]
        if not snapshot_date:
            snapshot_date = str(date.today())
        
        pin = float(snapshot.get('primary_gamma_pin_strike', 0))
        spot = float(snapshot.get('spot_last', 0))
        snapshot_time = snapshot.get('timestamp_utc', '')
        
        error_points = official_close - pin
        error_pct = (error_points / official_close * 100) if official_close else 0
        
        confidence = compute_confidence_score(snapshot)
        
        record = AccuracyRecord(
            date=snapshot_date,
            symbol=symbol,
            snapshot_time=snapshot_time,
            pin=pin,
            spot_at_snapshot=spot,
            official_close=official_close,
            error_points=error_points,
            error_pct=error_pct,
            freeze_enforced=freeze_enforced,
            confidence=confidence,
            contracts_count=int(snapshot.get('contracts_count', 0)),
            expiration_scope=str(snapshot.get('expiration_scope', '')),
            validation_is_valid=bool(snapshot.get('validation_is_valid', True)),
        )
        
        with open(LEDGER_PATH, 'a', newline='') as f:
            writer = csv.writer(f)
            writer.writerow([
                record.date,
                record.symbol,
                record.snapshot_time,
                f"{record.pin:.2f}",
                f"{record.spot_at_snapshot:.2f}",
                f"{record.official_close:.2f}",
                f"{record.error_points:.2f}",
                f"{record.error_pct:.4f}",
                record.freeze_enforced,
                f"{record.confidence:.3f}",
                record.contracts_count,
                record.expiration_scope,
                record.validation_is_valid,
            ])
        
        log.info(f"Recorded accuracy for {symbol}: pin={pin:.2f}, close={official_close:.2f}, error={error_points:+.2f} pts ({error_pct:+.2f}%)")
        
        return record
        
    except Exception as e:
        log.error(f"Failed to record accuracy for {symbol}: {e}")
        return None


def compute_confidence_score(snapshot: Dict[str, Any]) -> float:
    """
    Compute confidence score based on sanity check factors.
    
    Score ranges from 0.0 (no confidence) to 1.0 (full confidence).
    
    Deductions:
    - contracts_count < 400: -0.2
    - validation failed: -0.4
    - top strike dominates (>90% of total): -0.3 (sign of thin liquidity)
    """
    confidence = 1.0
    
    contracts = int(snapshot.get('contracts_count', 0))
    if contracts < 400:
        confidence -= 0.2
    if contracts < 100:
        confidence -= 0.2
    
    if not snapshot.get('validation_is_valid', True):
        confidence -= 0.4
    
    if snapshot.get('gamma_excluded_from_model', False):
        confidence -= 0.3
    
    top_strikes = snapshot.get('top_strikes_by_abs_gex', [])
    if top_strikes:
        top_gex = abs(float(top_strikes[0].get('net_gex', 0)))
        total_gex = float(snapshot.get('total_gex_abs', 1))
        if total_gex > 0 and (top_gex / total_gex) > 0.9:
            confidence -= 0.3
    
    return max(0.0, min(1.0, confidence))


def get_ledger_records(symbol: Optional[str] = None, days: int = 30) -> List[Dict[str, Any]]:
    """
    Read accuracy ledger records.
    
    Args:
        symbol: Filter by symbol (None for all)
        days: Number of days to look back
    
    Returns:
        List of record dictionaries
    """
    try:
        if not LEDGER_PATH.exists():
            return []
        
        records = []
        with open(LEDGER_PATH, 'r', newline='') as f:
            reader = csv.DictReader(f)
            for row in reader:
                if symbol and row.get('symbol') != symbol:
                    continue
                records.append(row)
        
        return records[-days * 4:] if len(records) > days * 4 else records
        
    except Exception as e:
        log.error(f"Failed to read ledger: {e}")
        return []


def get_accuracy_stats(symbol: str, days: int = 30) -> Dict[str, Any]:
    """
    Get accuracy statistics for a symbol.
    
    Returns:
        Dictionary with MAE, bias, hit rate, etc.
    """
    records = get_ledger_records(symbol, days)
    
    if not records:
        return {"error": "No records found", "count": 0}
    
    errors = []
    for r in records:
        try:
            errors.append(float(r.get('error_points', 0)))
        except:
            continue
    
    if not errors:
        return {"error": "No valid error data", "count": 0}
    
    import statistics
    
    mae = sum(abs(e) for e in errors) / len(errors)
    bias = sum(errors) / len(errors)
    
    within_05pct = sum(1 for r in records if abs(float(r.get('error_pct', 100))) < 0.5) / len(records)
    within_1pct = sum(1 for r in records if abs(float(r.get('error_pct', 100))) < 1.0) / len(records)
    
    return {
        "symbol": symbol,
        "count": len(records),
        "mae_points": round(mae, 2),
        "bias_points": round(bias, 2),
        "within_05pct": round(within_05pct * 100, 1),
        "within_1pct": round(within_1pct * 100, 1),
        "std_dev": round(statistics.stdev(errors), 2) if len(errors) > 1 else 0,
    }
