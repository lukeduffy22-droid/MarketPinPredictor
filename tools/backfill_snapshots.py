#!/usr/bin/env python3
"""
Backfill Gamma Snapshots from NDJSON files to Database.

Usage:
    python tools/backfill_snapshots.py --date 2025-12-24
    python tools/backfill_snapshots.py --date 2025-12-24 --symbol SPX
    python tools/backfill_snapshots.py --all-dates
"""

import argparse
import json
import os
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytz
from database import save_gamma_snapshot, SessionLocal, GammaPinSnapshot

SYMBOLS = ['SPX', 'NDX', 'DJI', 'RUT']
EXPORTS_DIR = Path('exports')


def load_ndjson_file(filepath):
    """Load all snapshots from an NDJSON file."""
    snapshots = []
    try:
        with open(filepath, 'r') as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        snapshots.append(json.loads(line))
                    except json.JSONDecodeError as e:
                        print(f"  Warning: Skipping invalid JSON line: {e}")
    except FileNotFoundError:
        print(f"  File not found: {filepath}")
    return snapshots


def backfill_symbol_date(symbol, date_str, dry_run=False):
    """Backfill snapshots for a specific symbol and date."""
    filepath = EXPORTS_DIR / symbol / f"{date_str}.ndjson"
    
    if not filepath.exists():
        print(f"  No NDJSON file for {symbol} on {date_str}")
        return 0, 0
    
    snapshots = load_ndjson_file(filepath)
    print(f"  Found {len(snapshots)} snapshots in {filepath}")
    
    saved = 0
    skipped = 0
    
    et_tz = pytz.timezone('US/Eastern')
    
    for snap in snapshots:
        try:
            timestamp_str = snap.get('generated_at_utc') or snap.get('timestamp_utc')
            if not timestamp_str:
                print(f"    Skipping snapshot without timestamp")
                skipped += 1
                continue
            
            if isinstance(timestamp_str, str):
                timestamp = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
            else:
                timestamp = timestamp_str
            
            timestamp_et = timestamp.astimezone(et_tz) if timestamp.tzinfo else et_tz.localize(timestamp)
            
            pin_strike = snap.get('primary_gamma_pin_strike', 0)
            spot_price = snap.get('spot_last', 0)
            gross_gex = snap.get('gross_gex', snap.get('total_gex_abs', 0))
            net_gex = snap.get('net_gex', snap.get('total_gex_net', 0))
            is_valid = snap.get('validation_is_valid', True)
            validation_reasons = snap.get('validation_failure_reasons', [])
            
            # Calculate pull_strength from available data
            # Pull strength = pin_abs_gex / gross_gex (ratio of gamma concentration at pin)
            pin_abs_gex = snap.get('primary_gamma_pin_abs_gex', 0)
            if gross_gex and gross_gex > 0:
                pull_strength = min(1.0, abs(pin_abs_gex) / gross_gex)
            else:
                pull_strength = 0.5  # Default if no GEX data
            
            if isinstance(validation_reasons, list):
                validation_reasons = ', '.join(validation_reasons) if validation_reasons else None
            
            if dry_run:
                validity_str = "VALID" if is_valid else "INVALID"
                print(f"    [DRY RUN] Would save: {symbol} @ {timestamp_et.strftime('%H:%M')} pin=${pin_strike:.2f} spot=${spot_price:.2f} {validity_str}")
                saved += 1
            else:
                result = save_gamma_snapshot(
                    ticker=symbol,
                    interval_timestamp=timestamp_et,
                    pin_strike=pin_strike,
                    pull_strength=pull_strength,
                    spot_price=spot_price,
                    total_gex=gross_gex,
                    net_gex=net_gex,
                    is_mock_data=False,
                    is_valid=is_valid,
                    validation_reasons=validation_reasons
                )
                
                if result:
                    saved += 1
                else:
                    skipped += 1
                    
        except Exception as e:
            print(f"    Error processing snapshot: {e}")
            skipped += 1
    
    return saved, skipped


def get_available_dates(symbol=None):
    """Get all available dates from NDJSON files."""
    dates = set()
    
    symbols_to_check = [symbol] if symbol else SYMBOLS
    
    for sym in symbols_to_check:
        symbol_dir = EXPORTS_DIR / sym
        if symbol_dir.exists():
            for file in symbol_dir.glob("*.ndjson"):
                date_str = file.stem
                if len(date_str) == 10 and date_str[4] == '-':
                    dates.add(date_str)
    
    return sorted(dates)


def get_db_count_for_date(date_str):
    """Get count of snapshots already in database for a date."""
    try:
        db = SessionLocal()
        date_obj = datetime.strptime(date_str, '%Y-%m-%d').date()
        count = db.query(GammaPinSnapshot).filter(
            GammaPinSnapshot.trading_date == date_obj
        ).count()
        db.close()
        return count
    except Exception as e:
        print(f"Error checking DB count: {e}")
        return 0


def main():
    parser = argparse.ArgumentParser(description='Backfill gamma snapshots from NDJSON to database')
    parser.add_argument('--date', help='Date to backfill (YYYY-MM-DD)')
    parser.add_argument('--symbol', help='Specific symbol to backfill (SPX, NDX, DJI, RUT)')
    parser.add_argument('--all-dates', action='store_true', help='Backfill all available dates')
    parser.add_argument('--dry-run', action='store_true', help='Show what would be done without saving')
    parser.add_argument('--list-dates', action='store_true', help='List available dates')
    
    args = parser.parse_args()
    
    if args.list_dates:
        dates = get_available_dates(args.symbol)
        print(f"Available dates in NDJSON exports:")
        for d in dates:
            db_count = get_db_count_for_date(d)
            print(f"  {d} (DB has {db_count} snapshots)")
        return
    
    if not args.date and not args.all_dates:
        parser.print_help()
        print("\nExample: python tools/backfill_snapshots.py --date 2025-12-24")
        return
    
    dates_to_process = []
    if args.all_dates:
        dates_to_process = get_available_dates(args.symbol)
    else:
        dates_to_process = [args.date]
    
    symbols_to_process = [args.symbol.upper()] if args.symbol else SYMBOLS
    
    total_saved = 0
    total_skipped = 0
    
    for date_str in dates_to_process:
        print(f"\n{'='*60}")
        print(f"Processing date: {date_str}")
        print(f"{'='*60}")
        
        db_before = get_db_count_for_date(date_str)
        print(f"Database has {db_before} snapshots for {date_str} before backfill")
        
        for symbol in symbols_to_process:
            print(f"\nBackfilling {symbol}...")
            saved, skipped = backfill_symbol_date(symbol, date_str, dry_run=args.dry_run)
            total_saved += saved
            total_skipped += skipped
            print(f"  Result: {saved} saved, {skipped} skipped")
        
        if not args.dry_run:
            db_after = get_db_count_for_date(date_str)
            print(f"\nDatabase now has {db_after} snapshots for {date_str} (+{db_after - db_before})")
    
    print(f"\n{'='*60}")
    print(f"TOTAL: {total_saved} saved, {total_skipped} skipped")
    print(f"{'='*60}")


if __name__ == '__main__':
    main()
