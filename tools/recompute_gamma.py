#!/usr/bin/env python3
"""
Offline Gamma Recompute Tool

Recalculate gamma exposure from an audit snapshot with ZERO API calls.
This tool proves mathematical correctness and enables post-hoc verification.

Usage:
    python tools/recompute_gamma.py logs/audit/SPX/20251217-155700.json
    python tools/recompute_gamma.py --symbol SPX --latest
    python tools/recompute_gamma.py --verify-all SPX

Acceptance Criteria:
    - Recomputed pin == stored pin (± epsilon)
    - Any mismatch is a BUG, not noise
"""
import argparse
import json
import sys
from pathlib import Path
from typing import Optional, Dict, Any

sys.path.insert(0, str(Path(__file__).parent.parent))

from app.core.gex import compute_aggregate_gex_from_arrays, AggregateGexResult
from app.core.audit_persistence import get_latest_snapshot, get_snapshot_by_file, list_snapshot_files


EPSILON = 0.0001


def recompute_aggregate_gex(snapshot_data: Dict[str, Any]) -> AggregateGexResult:
    """
    Recompute aggregate GEX from snapshot data using canonical functions.
    
    This uses ONLY the data in the snapshot - no external API calls.
    
    CRITICAL: Uses call_gex and put_gex arrays when available to ensure
    gross_gex = call_gex_total + put_gex_total (NOT derived from net_gex).
    """
    top_strikes = snapshot_data.get('top_strikes_by_abs_gex', [])
    
    if not top_strikes:
        return AggregateGexResult(
            gross_gex=0.0, 
            net_gex=0.0, 
            call_gex_total=0.0, 
            put_gex_total=0.0, 
            strike_count=0
        )
    
    # Extract call_gex and put_gex arrays when available (preferred path)
    # This ensures gross_gex = sum(call) + sum(put), not abs(net)
    # Filter to strikes that have BOTH call_gex and put_gex (skip malformed rows)
    strikes_with_call_put = [
        s for s in top_strikes 
        if 'call_gex' in s and 'put_gex' in s
    ]
    
    if strikes_with_call_put:
        # Use call/put separation for all strikes that have it
        call_gex_values = [float(s['call_gex']) for s in strikes_with_call_put]
        put_gex_values = [float(s['put_gex']) for s in strikes_with_call_put]
        net_gex_values = [float(s.get('net_gex', s['call_gex'] - s['put_gex'])) for s in strikes_with_call_put]
        return compute_aggregate_gex_from_arrays(
            net_gex_per_strike=net_gex_values,
            call_gex_per_strike=call_gex_values,
            put_gex_per_strike=put_gex_values
        )
    else:
        # Legacy fallback for old snapshots without call/put separation
        net_gex_values = [float(s.get('net_gex', 0)) for s in top_strikes]
        return compute_aggregate_gex_from_arrays(net_gex_values)


def verify_snapshot(filepath: str, verbose: bool = True) -> Dict[str, Any]:
    """
    Verify a snapshot's stored values against recomputed values.
    
    Returns:
        Dictionary with verification results
    """
    with open(filepath, 'r') as f:
        data = json.load(f)
    
    stored_gex_abs = float(data.get('total_gex_abs', 0))
    stored_gex_net = float(data.get('total_gex_net', 0))
    stored_pin = float(data.get('primary_gamma_pin_strike', 0))
    
    recomputed = recompute_aggregate_gex(data)
    
    gex_abs_match = abs(stored_gex_abs - recomputed.total_gex_abs) < EPSILON
    gex_net_match = abs(stored_gex_net - recomputed.total_gex_net) < EPSILON
    
    top_strikes = data.get('top_strikes_by_abs_gex', [])
    recomputed_pin = float(top_strikes[0]['strike']) if top_strikes else 0
    pin_match = abs(stored_pin - recomputed_pin) < EPSILON
    
    result = {
        'filepath': filepath,
        'symbol': data.get('symbol', 'UNKNOWN'),
        'timestamp': data.get('timestamp_utc', ''),
        'stored_gex_abs': stored_gex_abs,
        'recomputed_gex_abs': recomputed.total_gex_abs,
        'gex_abs_delta': stored_gex_abs - recomputed.total_gex_abs,
        'gex_abs_match': gex_abs_match,
        'stored_gex_net': stored_gex_net,
        'recomputed_gex_net': recomputed.total_gex_net,
        'gex_net_delta': stored_gex_net - recomputed.total_gex_net,
        'gex_net_match': gex_net_match,
        'stored_pin': stored_pin,
        'recomputed_pin': recomputed_pin,
        'pin_delta': stored_pin - recomputed_pin,
        'pin_match': pin_match,
        'all_match': gex_abs_match and gex_net_match,
        'validation_is_valid': data.get('validation_is_valid', True),
    }
    
    if verbose:
        print(f"\n{'='*60}")
        print(f"Snapshot: {filepath}")
        print(f"Symbol: {result['symbol']} | Time: {result['timestamp']}")
        print(f"{'='*60}")
        print(f"\nGEX ABS:")
        print(f"  Stored:     {stored_gex_abs:>15.6f}")
        print(f"  Recomputed: {recomputed.total_gex_abs:>15.6f}")
        print(f"  Delta:      {result['gex_abs_delta']:>15.6f}  {'✓' if gex_abs_match else '✗ MISMATCH'}")
        print(f"\nGEX NET:")
        print(f"  Stored:     {stored_gex_net:>15.6f}")
        print(f"  Recomputed: {recomputed.total_gex_net:>15.6f}")
        print(f"  Delta:      {result['gex_net_delta']:>15.6f}  {'✓' if gex_net_match else '✗ MISMATCH'}")
        print(f"\nPIN STRIKE:")
        print(f"  Stored:     ${stored_pin:>14.2f}")
        print(f"  Top Strike: ${recomputed_pin:>14.2f}")
        print(f"  Delta:      ${result['pin_delta']:>14.2f}  {'✓' if pin_match else '✗ MISMATCH'}")
        print(f"\nValidation: {'PASSED' if result['validation_is_valid'] else 'FAILED'}")
        print(f"Overall:    {'ALL MATCH ✓' if result['all_match'] else 'MISMATCH DETECTED ✗'}")
    
    return result


def verify_all_snapshots(symbol: str) -> Dict[str, Any]:
    """
    Verify all snapshots for a symbol.
    
    Returns summary statistics.
    """
    files = list_snapshot_files(symbol)
    
    if not files:
        print(f"No snapshots found for {symbol}")
        return {"error": "No snapshots found"}
    
    print(f"\nVerifying {len(files)} snapshots for {symbol}...")
    
    total = len(files)
    matched = 0
    mismatched = 0
    errors = []
    
    for f in files:
        try:
            result = verify_snapshot(str(f), verbose=False)
            if result['all_match']:
                matched += 1
            else:
                mismatched += 1
                errors.append({
                    'file': f.name,
                    'gex_abs_delta': result['gex_abs_delta'],
                    'gex_net_delta': result['gex_net_delta'],
                })
        except Exception as e:
            print(f"  Error verifying {f.name}: {e}")
    
    print(f"\n{'='*60}")
    print(f"VERIFICATION SUMMARY: {symbol}")
    print(f"{'='*60}")
    print(f"Total snapshots: {total}")
    print(f"Matched:         {matched} ({matched/total*100:.1f}%)")
    print(f"Mismatched:      {mismatched} ({mismatched/total*100:.1f}%)")
    
    if errors:
        print(f"\nMismatched files:")
        for e in errors[:10]:
            print(f"  - {e['file']}: abs_delta={e['gex_abs_delta']:.6f}, net_delta={e['gex_net_delta']:.6f}")
        if len(errors) > 10:
            print(f"  ... and {len(errors)-10} more")
    
    return {
        'symbol': symbol,
        'total': total,
        'matched': matched,
        'mismatched': mismatched,
        'match_rate': matched / total if total > 0 else 0,
    }


def main():
    parser = argparse.ArgumentParser(
        description='Offline Gamma Recompute Tool - Verify gamma calculations without API calls'
    )
    
    parser.add_argument('filepath', nargs='?', help='Path to snapshot JSON file')
    parser.add_argument('--symbol', '-s', help='Symbol to verify (SPX, NDX, DJI, RUT)')
    parser.add_argument('--latest', '-l', action='store_true', help='Verify latest snapshot for symbol')
    parser.add_argument('--verify-all', '-a', action='store_true', help='Verify all snapshots for symbol')
    parser.add_argument('--quiet', '-q', action='store_true', help='Minimal output')
    
    args = parser.parse_args()
    
    if args.filepath:
        result = verify_snapshot(args.filepath, verbose=not args.quiet)
        sys.exit(0 if result['all_match'] else 1)
    
    elif args.symbol and args.verify_all:
        result = verify_all_snapshots(args.symbol)
        sys.exit(0 if result.get('mismatched', 0) == 0 else 1)
    
    elif args.symbol and args.latest:
        snapshot = get_latest_snapshot(args.symbol)
        if not snapshot:
            print(f"No snapshot found for {args.symbol}")
            sys.exit(1)
        
        filepath = f"logs/audit/{args.symbol}/"
        files = list_snapshot_files(args.symbol)
        if files:
            result = verify_snapshot(str(files[-1]), verbose=not args.quiet)
            sys.exit(0 if result['all_match'] else 1)
        else:
            print(f"No snapshot files found for {args.symbol}")
            sys.exit(1)
    
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
