# app/core/audit_persistence.py
"""
Audit Snapshot Persistence Layer.

Writes audit snapshots to disk with:
- No overwrites (each snapshot is unique)
- FIFO cleanup (keep last 200 per symbol)
- Survives app restarts
- File naming sortable by time without parsing JSON
"""
from __future__ import annotations

import os
import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, List

from app.core.audit_snapshot import AuditSnapshot

log = logging.getLogger("audit_persistence")

# Configuration
AUDIT_LOG_DIR = Path("./logs/audit")
VALIDATED_AUDIT_LOG_DIR = Path("./logs/audit_validated")
MAX_SNAPSHOTS_PER_SYMBOL = 200


def get_audit_dir(symbol: str) -> Path:
    """Get the audit directory for a symbol, creating if needed."""
    audit_dir = AUDIT_LOG_DIR / symbol.upper()
    audit_dir.mkdir(parents=True, exist_ok=True)
    return audit_dir


def get_validated_audit_dir(symbol: str) -> Path:
    """Get the validated audit directory for a symbol, creating if needed."""
    audit_dir = VALIDATED_AUDIT_LOG_DIR / symbol.upper()
    audit_dir.mkdir(parents=True, exist_ok=True)
    return audit_dir


def generate_filename(timestamp_utc: Optional[datetime] = None) -> str:
    """
    Generate a sortable filename based on timestamp.
    Format: YYYYMMDD-HHMMSS.json
    
    This format is sortable without parsing JSON.
    """
    if timestamp_utc is None:
        timestamp_utc = datetime.now(timezone.utc)
    return timestamp_utc.strftime("%Y%m%d-%H%M%S") + ".json"


def persist_audit_snapshot(snapshot: AuditSnapshot, force_write: bool = False) -> Optional[Path]:
    """
    Persist an audit snapshot to disk.
    
    Rules:
    1. Never overwrites existing files (adds suffix if collision)
    2. Maintains FIFO cleanup (keeps last MAX_SNAPSHOTS_PER_SYMBOL)
    3. Files are named to be sortable by time
    4. CRITICAL: Raises RuntimeError if market is closed (unless force_write=True)
    
    Args:
        snapshot: The AuditSnapshot to persist
        force_write: If True, bypass market close check (USE ONLY FOR TESTING)
    
    Returns:
        Path to the written file, or None on failure
    
    Raises:
        RuntimeError: If market is closed and force_write is False
    """
    try:
        from app.utils.market_time import market_is_closed, get_freeze_status
        
        if market_is_closed() and not force_write:
            is_frozen, reason = get_freeze_status()
            error_msg = f"WRITE BLOCKED: {reason} — Snapshot write rejected for {snapshot.symbol}"
            log.error(error_msg)
            raise RuntimeError(error_msg)
        
        audit_dir = get_audit_dir(snapshot.symbol)
        
        # Generate base filename
        base_filename = generate_filename()
        filepath = audit_dir / base_filename
        
        # Never overwrite - add suffix if file exists
        counter = 1
        while filepath.exists():
            name_without_ext = base_filename.rsplit('.', 1)[0]
            filepath = audit_dir / f"{name_without_ext}_{counter:03d}.json"
            counter += 1
            if counter > 999:
                log.error(f"Too many snapshot collisions for {snapshot.symbol}")
                return None
        
        # Write snapshot
        with open(filepath, 'w') as f:
            f.write(snapshot.to_json())
        
        log.info(f"Audit snapshot saved: {filepath}")
        
        # FIFO cleanup
        cleanup_old_snapshots(snapshot.symbol)
        
        return filepath
        
    except Exception as e:
        log.error(f"Failed to persist audit snapshot for {snapshot.symbol}: {e}")
        return None


def cleanup_old_snapshots(symbol: str) -> int:
    """
    Remove oldest snapshots beyond MAX_SNAPSHOTS_PER_SYMBOL.
    
    Returns:
        Number of files deleted
    """
    try:
        audit_dir = get_audit_dir(symbol)
        
        # List all JSON files, sorted by name (which is time-sortable)
        files = sorted(audit_dir.glob("*.json"))
        
        if len(files) <= MAX_SNAPSHOTS_PER_SYMBOL:
            return 0
        
        # Delete oldest files (files are sorted oldest first)
        files_to_delete = files[:-MAX_SNAPSHOTS_PER_SYMBOL]
        
        deleted = 0
        for f in files_to_delete:
            try:
                f.unlink()
                deleted += 1
            except Exception as e:
                log.warning(f"Failed to delete old snapshot {f}: {e}")
        
        if deleted > 0:
            log.info(f"Cleaned up {deleted} old snapshots for {symbol}")
        
        return deleted
        
    except Exception as e:
        log.error(f"Failed to cleanup old snapshots for {symbol}: {e}")
        return 0


def cleanup_old_validated_snapshots(symbol: str) -> int:
    """
    Remove oldest validated snapshots beyond MAX_SNAPSHOTS_PER_SYMBOL.

    Returns:
        Number of files deleted
    """
    try:
        audit_dir = get_validated_audit_dir(symbol)
        files = sorted(audit_dir.glob("*.json"))

        if len(files) <= MAX_SNAPSHOTS_PER_SYMBOL:
            return 0

        files_to_delete = files[:-MAX_SNAPSHOTS_PER_SYMBOL]

        deleted = 0
        for f in files_to_delete:
            try:
                f.unlink()
                deleted += 1
            except Exception as e:
                log.warning(f"Failed to delete old validated snapshot {f}: {e}")

        if deleted > 0:
            log.info(f"Cleaned up {deleted} old validated snapshots for {symbol}")

        return deleted
    except Exception as e:
        log.error(f"Failed to cleanup old validated snapshots for {symbol}: {e}")
        return 0


def persist_validated_audit_snapshot(snapshot: AuditSnapshot) -> Optional[Path]:
    """
    Persist only validated snapshots to a separate, read-optimized store.

    Returns:
        Path to the written file, or None when snapshot is invalid or write fails.
    """
    if not snapshot.validation_is_valid:
        return None

    try:
        audit_dir = get_validated_audit_dir(snapshot.symbol)
        base_filename = generate_filename()
        filepath = audit_dir / base_filename

        counter = 1
        while filepath.exists():
            name_without_ext = base_filename.rsplit('.', 1)[0]
            filepath = audit_dir / f"{name_without_ext}_{counter:03d}.json"
            counter += 1
            if counter > 999:
                log.error(f"Too many validated snapshot collisions for {snapshot.symbol}")
                return None

        with open(filepath, 'w') as f:
            f.write(snapshot.to_json())

        cleanup_old_validated_snapshots(snapshot.symbol)
        log.info(f"Validated audit snapshot saved: {filepath}")
        return filepath
    except Exception as e:
        log.error(f"Failed to persist validated snapshot for {snapshot.symbol}: {e}")
        return None


def get_latest_validated_snapshot(symbol: str) -> Optional[AuditSnapshot]:
    """Get the most recent validated audit snapshot for a symbol."""
    try:
        audit_dir = get_validated_audit_dir(symbol)
        files = sorted(audit_dir.glob("*.json"))
        if not files:
            return None

        with open(files[-1], 'r') as f:
            data = json.load(f)
        return dict_to_audit_snapshot(data)
    except Exception as e:
        log.error(f"Failed to get latest validated snapshot for {symbol}: {e}")
        return None


def get_latest_snapshot(symbol: str) -> Optional[AuditSnapshot]:
    """
    Get the most recent audit snapshot for a symbol.
    
    Returns:
        AuditSnapshot if found, None otherwise
    """
    try:
        audit_dir = get_audit_dir(symbol)
        
        # List all JSON files, sorted by name (most recent last)
        files = sorted(audit_dir.glob("*.json"))
        
        if not files:
            return None
        
        latest_file = files[-1]
        
        with open(latest_file, 'r') as f:
            data = json.load(f)
        
        # Reconstruct AuditSnapshot from dict
        return dict_to_audit_snapshot(data)
        
    except Exception as e:
        log.error(f"Failed to get latest snapshot for {symbol}: {e}")
        return None


def get_recent_snapshots(symbol: str, count: int = 10) -> List[AuditSnapshot]:
    """
    Get the N most recent audit snapshots for a symbol.
    
    Args:
        symbol: Index symbol
        count: Number of recent snapshots to return
    
    Returns:
        List of AuditSnapshots, most recent first
    """
    try:
        audit_dir = get_audit_dir(symbol)
        
        # List all JSON files, sorted by name
        files = sorted(audit_dir.glob("*.json"), reverse=True)[:count]
        
        snapshots = []
        for f in files:
            try:
                with open(f, 'r') as fp:
                    data = json.load(fp)
                snapshots.append(dict_to_audit_snapshot(data))
            except Exception as e:
                log.warning(f"Failed to read snapshot {f}: {e}")
        
        return snapshots
        
    except Exception as e:
        log.error(f"Failed to get recent snapshots for {symbol}: {e}")
        return []


def dict_to_audit_snapshot(data: dict) -> AuditSnapshot:
    """Convert a dictionary to an AuditSnapshot."""
    return AuditSnapshot(
        snapshot_version=data.get('snapshot_version', '1.0'),
        generated_at_utc=data.get('generated_at_utc', ''),
        symbol=data.get('symbol', ''),
        timestamp_utc=data.get('timestamp_utc', ''),
        spot_last=float(data.get('spot_last', 0)),
        spot_timestamp=data.get('spot_timestamp', ''),
        spot_source=data.get('spot_source', ''),
        chain_symbol_used=data.get('chain_symbol_used', ''),
        underlying_reported=data.get('underlying_reported'),
        expirations_min_days=int(data.get('expirations_min_days', 0)),
        expirations_max_days=int(data.get('expirations_max_days', 0)),
        contracts_count=int(data.get('contracts_count', 0)),
        expiration_scope=data.get('expiration_scope', ''),
        top_strikes_by_abs_gex=data.get('top_strikes_by_abs_gex', []),
        primary_gamma_pin_strike=float(data.get('primary_gamma_pin_strike', 0)),
        primary_gamma_pin_abs_gex=float(data.get('primary_gamma_pin_abs_gex', 0)),
        zero_gamma_level=data.get('zero_gamma_level'),
        zero_gamma_method=data.get('zero_gamma_method'),
        # NEW: Corrected GEX fields with call/put separation
        call_gex_total=float(data.get('call_gex_total', 0)),
        put_gex_total=float(data.get('put_gex_total', 0)),
        gross_gex=float(data.get('gross_gex', 0)),
        net_gex=float(data.get('net_gex', 0)),
        # Legacy fields for backward compatibility
        total_gex_abs=float(data.get('total_gex_abs', 0)),
        total_gex_net=float(data.get('total_gex_net', 0)),
        total_gex_abs_definition=data.get('total_gex_abs_definition', 'sum(abs(net_gex_per_strike))'),
        total_gex_net_definition=data.get('total_gex_net_definition', 'sum(net_gex_per_strike)'),
        validation_is_valid=data.get('validation_is_valid', True),
        validation_failure_reasons=data.get('validation_failure_reasons', []),
        gamma_excluded_from_model=data.get('gamma_excluded_from_model', False),
        # Pin drift fields
        pin_drift_points_per_hour=data.get('pin_drift_points_per_hour'),
        pin_change_points=data.get('pin_change_points'),
        prev_pin_strike=data.get('prev_pin_strike'),
        prev_snapshot_timestamp=data.get('prev_snapshot_timestamp'),
        # Diagnostic fields
        truncation=data.get('truncation'),
        gamma_by_distance=data.get('gamma_by_distance'),
        confidence=data.get('confidence'),
        confidence_factors=data.get('confidence_factors'),
        dispersion_ratio=data.get('dispersion_ratio'),
        skew_metrics=data.get('skew_metrics'),
        vol_regime=data.get('vol_regime'),
        vol_regime_iv=data.get('vol_regime_iv'),
        # Pre-gate explanation fields
        strike_count=data.get('strike_count'),
        nonzero_strike_count=data.get('nonzero_strike_count'),
        top_strike_share=data.get('top_strike_share'),
        pregate_reason=data.get('pregate_reason'),
    )


def list_snapshot_files(symbol: str) -> List[Path]:
    """List all snapshot files for a symbol, sorted oldest to newest."""
    audit_dir = get_audit_dir(symbol)
    return sorted(audit_dir.glob("*.json"))


def get_snapshot_count(symbol: str) -> int:
    """Get the number of snapshots for a symbol."""
    return len(list_snapshot_files(symbol))


def load_last_valid_snapshot(symbol: str) -> Optional[AuditSnapshot]:
    """
    Load the most recent VALID audit snapshot for a symbol.
    
    This function is used during market close freeze to provide
    the last known good gamma state without any live data fetching.
    
    CRITICAL: Only returns snapshots where validation_is_valid=True
    
    Returns:
        Valid AuditSnapshot if found, None otherwise
    """
    try:
        audit_dir = get_audit_dir(symbol)
        
        files = sorted(audit_dir.glob("*.json"), reverse=True)
        
        for f in files:
            try:
                with open(f, 'r') as fp:
                    data = json.load(fp)
                
                if data.get('validation_is_valid', False):
                    snapshot = dict_to_audit_snapshot(data)
                    log.info(f"Loaded last valid snapshot for {symbol}: {f.name}")
                    return snapshot
            except Exception as e:
                log.warning(f"Failed to read snapshot {f}: {e}")
                continue
        
        log.warning(f"No valid snapshots found for {symbol}")
        return None
        
    except Exception as e:
        log.error(f"Failed to load last valid snapshot for {symbol}: {e}")
        return None


def get_snapshot_by_file(filepath: str) -> Optional[AuditSnapshot]:
    """
    Load a specific snapshot file for offline analysis.
    
    Args:
        filepath: Path to the snapshot JSON file
    
    Returns:
        AuditSnapshot if valid, None otherwise
    """
    try:
        with open(filepath, 'r') as f:
            data = json.load(f)
        return dict_to_audit_snapshot(data)
    except Exception as e:
        log.error(f"Failed to load snapshot from {filepath}: {e}")
        return None
