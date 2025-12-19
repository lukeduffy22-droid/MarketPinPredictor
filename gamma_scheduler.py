"""
Background Scheduler for Automatic Gamma Pin Sampling
Runs with adaptive intervals during market hours to track gamma evolution:
- Default: 15 minutes
- Last hour before close: 5 minutes  
- Last 15 minutes before close: 2 minutes (highest precision when it matters most)

Respects early close days (1 PM ET) like Black Friday and Christmas Eve.
"""
import threading
import time
import os
from datetime import datetime, time as dt_time, timedelta
import pytz
from database import save_gamma_snapshot
from websocket_streaming import get_snapshot_data
from options_gamma import fetch_options_chain, calculate_gamma_exposure

# Market hours in Eastern Time
MARKET_OPEN_TIME = dt_time(9, 30)  # 9:30 AM ET
MARKET_CLOSE_REGULAR = dt_time(16, 0)  # 4:00 PM ET (regular close)
MARKET_CLOSE_EARLY = dt_time(13, 0)   # 1:00 PM ET (early close)

TRACKED_SYMBOLS = ['SPX', 'NDX', 'DJI', 'RUT']

# Global flag to control scheduler
_scheduler_running = False
_scheduler_thread = None

def get_market_close_time():
    """Get today's market close time, accounting for early close days"""
    try:
        from app.utils.time_et import is_early_close, close_time_et
        if is_early_close():
            return MARKET_CLOSE_EARLY
        return MARKET_CLOSE_REGULAR
    except:
        return MARKET_CLOSE_REGULAR

def get_minutes_until_close():
    """Get minutes until today's market close"""
    try:
        from app.utils.time_et import minutes_to_close_et
        from datetime import datetime, timezone
        return minutes_to_close_et(datetime.now(timezone.utc))
    except:
        # Fallback calculation
        et_tz = pytz.timezone('US/Eastern')
        now_et = datetime.now(et_tz)
        close_time = get_market_close_time()
        close_dt = now_et.replace(hour=close_time.hour, minute=close_time.minute, second=0)
        if now_et >= close_dt:
            return 0
        return int((close_dt - now_et).total_seconds() // 60)

def get_adaptive_sample_interval():
    """
    Get the sampling interval in minutes based on time until close.
    More frequent sampling as market close approaches.
    
    Returns: (interval_minutes, interval_name)
    """
    minutes_left = get_minutes_until_close()
    
    if minutes_left <= 0:
        return (15, "market_closed")
    elif minutes_left <= 15:
        return (2, "final_15min")  # Highest precision in last 15 minutes
    elif minutes_left <= 30:
        return (3, "final_30min")  # High precision in last 30 minutes
    elif minutes_left <= 60:
        return (5, "final_hour")   # Increased precision in last hour
    else:
        return (15, "regular")     # Standard interval

def is_market_hours():
    """Check if current time is during regular trading hours, respecting early close days"""
    try:
        from app.utils.market_time import market_is_open
        return market_is_open()
    except ImportError:
        try:
            from app.utils.time_et import is_regular_hours, is_market_holiday
            from datetime import datetime, timezone
            
            if is_market_holiday():
                return False
            
            return is_regular_hours(datetime.now(timezone.utc))
        except:
            et_tz = pytz.timezone('US/Eastern')
            now_et = datetime.now(et_tz)
            current_time = now_et.time()
            
            if now_et.weekday() >= 5:
                return False
            
            close_time = get_market_close_time()
            if MARKET_OPEN_TIME <= current_time < close_time:
                return True
            
            return False


def is_freeze_active():
    """Check if post-close data freeze is active. When True, no live sampling allowed."""
    try:
        from app.utils.market_time import is_freeze_enforced, get_freeze_status
        return is_freeze_enforced()
    except ImportError:
        return not is_market_hours()


def get_frozen_snapshot(symbol: str):
    """
    Get the last valid snapshot for a symbol during freeze mode.
    
    CRITICAL: This is the ONLY way to get gamma data after market close.
    No live API calls are allowed during freeze.
    
    Returns:
        dict with gamma data if available, None otherwise
    """
    try:
        from app.core.audit_persistence import load_last_valid_snapshot
        
        snapshot = load_last_valid_snapshot(symbol)
        if snapshot:
            return {
                'pin_strike': snapshot.primary_gamma_pin_strike,
                'total_gex': snapshot.total_gex_abs,
                'net_gex': snapshot.total_gex_net,
                'spot_price': snapshot.spot_last,
                'is_frozen': True,
                'snapshot_time': snapshot.timestamp_utc,
            }
        return None
    except Exception as e:
        print(f"Error loading frozen snapshot for {symbol}: {e}")
        return None

def fetch_and_save_gamma_snapshot(api_key, symbol):
    """
    Fetch current gamma data for a symbol, validate, create audit snapshot, and save.
    
    FREEZE GUARD (mandatory):
    If market is closed, returns last valid frozen snapshot instead of live data.
    No API calls are made during freeze mode.
    
    AUDIT PIPELINE (mandatory):
    1. Fetch spot price and options chain
    2. Calculate gamma exposure
    3. Build audit snapshot (pure function, no recomputation)
    4. Apply sanity validation gates (hard fail)
    5. Persist audit snapshot to disk (ALWAYS, even if invalid)
    6. Save to database only if sanity checks pass
    
    Returns True if successful, False otherwise
    """
    from datetime import timezone
    
    if is_freeze_active():
        try:
            from app.utils.market_time import get_freeze_status
            is_frozen, reason = get_freeze_status()
            print(f"  🔒 FREEZE ACTIVE for {symbol}: {reason}")
            print(f"  🔒 Using last valid snapshot (no live API calls)")
            
            frozen_data = get_frozen_snapshot(symbol)
            if frozen_data:
                print(f"  ✓ Frozen snapshot loaded: pin=${frozen_data['pin_strike']:.2f}, time={frozen_data['snapshot_time']}")
                return True
            else:
                print(f"  ⚠️ No frozen snapshot available for {symbol}")
                return False
        except Exception as e:
            print(f"  Error in freeze mode for {symbol}: {e}")
            return False
    
    try:
        from app.core.audit_persistence import persist_audit_snapshot
        from app.core.sanity_checks import apply_validation_to_snapshot, should_use_gamma_in_model
        
        # Get current price using snapshot API
        polygon_ticker = f"I:{symbol}"
        print(f"  Fetching snapshot for {polygon_ticker}...")
        snapshot = get_snapshot_data(api_key, [polygon_ticker])
        
        if not snapshot:
            print(f"  Warning: Snapshot API returned None for {symbol}")
            return False
        
        if polygon_ticker not in snapshot:
            print(f"  Warning: Ticker {polygon_ticker} not in snapshot response. Keys: {list(snapshot.keys())}")
            return False
        
        current_price = snapshot[polygon_ticker].get('price')
        if not current_price:
            print(f"  Warning: No price in snapshot for {symbol}. Data: {snapshot[polygon_ticker]}")
            return False
        
        # Fetch options chain and calculate gamma exposure
        options_df, is_mock = fetch_options_chain(api_key, symbol, current_price)
        
        if options_df is None or len(options_df) == 0:
            print(f"Warning: No options data for {symbol}, skipping gamma sample")
            return False
        
        # Calculate gamma exposure
        gex_analysis = calculate_gamma_exposure(options_df, current_price)
        
        if not gex_analysis or 'pin_strike' not in gex_analysis:
            print(f"Warning: Could not calculate gamma exposure for {symbol}, skipping gamma sample")
            return False
        
        # === AUDIT PIPELINE START ===
        # CRITICAL: Use canonical values from gex_analysis - NO recomputation
        
        et_tz = pytz.timezone('US/Eastern')
        now_et = datetime.now(et_tz)
        
        # Use CANONICAL aggregate GEX values directly from calculate_gamma_exposure
        # These are already computed using app/core/gex.py definitions
        aggregate_total_gex = gex_analysis.get('aggregate_total_gex', 0)
        aggregate_net_gex = gex_analysis.get('aggregate_net_gex', 0)
        expiration_scope = gex_analysis.get('expiration_scope', 'UNKNOWN')
        contracts_count = gex_analysis.get('contracts_count', len(options_df))
        
        # Convert gex_by_strike DataFrame to list for audit (read-only, no recomputation)
        gex_by_strike = gex_analysis.get('gex_by_strike')
        strikes_data = []
        if gex_by_strike is not None and not gex_by_strike.empty:
            for _, row in gex_by_strike.iterrows():
                strikes_data.append({
                    'strike': float(row.get('strike', 0)),
                    'expiration_days': int(row.get('days_to_expiry', 0)),
                    'call_gex': float(row.get('call_gex', 0)),
                    'put_gex': float(row.get('put_gex', 0)),
                    'net_gex': float(row.get('net_gex', 0)),
                    'total_gex': float(row.get('total_gex', 0)),
                })
        
        # Extract NEW call/put aggregate GEX from gex_analysis
        call_gex_total = gex_analysis.get('call_gex_total', 0.0)
        put_gex_total = gex_analysis.get('put_gex_total', 0.0)
        gross_gex = gex_analysis.get('gross_gex', call_gex_total + put_gex_total)
        net_gex_total = gex_analysis.get('net_gex_total', call_gex_total - put_gex_total)
        
        # Build audit snapshot using CANONICAL values (no recomputation)
        from app.core.audit_snapshot import AuditSnapshot
        
        # Sort strikes by abs(net_gex) descending for top 15
        sorted_strikes = sorted(strikes_data, key=lambda s: abs(s.get('net_gex', 0)), reverse=True)[:15]
        
        # Get expiration days for scope metadata
        exp_days = list(set(s.get('expiration_days', 0) for s in strikes_data)) if strikes_data else [0]
        
        audit_snapshot = AuditSnapshot(
            symbol=symbol,
            timestamp_utc=now_et.isoformat(),
            spot_last=current_price,
            spot_timestamp=now_et.isoformat(),
            spot_source='polygon_rest',
            chain_symbol_used=symbol,
            expirations_min_days=min(exp_days) if exp_days else 0,
            expirations_max_days=max(exp_days) if exp_days else 0,
            contracts_count=contracts_count,
            expiration_scope=expiration_scope,  # Use canonical value from gamma_result
            top_strikes_by_abs_gex=[
                {
                    'strike': s['strike'],
                    'expiration_days': s.get('expiration_days', 0),
                    'call_gex': s.get('call_gex', 0),
                    'put_gex': s.get('put_gex', 0),
                    'net_gex': s['net_gex'],
                    'abs_gex': abs(s['net_gex']),
                }
                for s in sorted_strikes
            ],
            primary_gamma_pin_strike=gex_analysis.get('pin_strike', 0),
            primary_gamma_pin_abs_gex=abs(gex_analysis.get('net_gex', 0)),
            zero_gamma_level=gex_analysis.get('zero_gamma'),
            zero_gamma_method='cumulative',
            # NEW: Corrected GEX fields with call/put separation
            call_gex_total=call_gex_total,
            put_gex_total=put_gex_total,
            gross_gex=gross_gex,
            net_gex=net_gex_total,
            # Legacy fields for backward compatibility
            total_gex_abs=aggregate_total_gex,
            total_gex_net=aggregate_net_gex,
        )
        
        # Step 1: Apply sanity validation BEFORE any persistence
        audit_snapshot = apply_validation_to_snapshot(audit_snapshot)
        
        # Step 2: Persist audit snapshot to disk (ALWAYS, even if invalid)
        audit_file = persist_audit_snapshot(audit_snapshot)
        if audit_file:
            print(f"  📝 Audit snapshot saved: {audit_file}")
        
        # Step 3: Check if gamma should be used in the model
        # If validation fails, gamma is EXCLUDED from model but audit is persisted
        if not should_use_gamma_in_model(audit_snapshot):
            print(f"  ⚠️ Gamma INVALID for {symbol}: {audit_snapshot.validation_failure_reasons}")
            print(f"  ⚠️ Gamma excluded from model, audit snapshot persisted (no DB save)")
            # Return True - audit was successful, just gamma excluded from model
            # Do NOT save to DB - this prevents invalid gamma from entering predictions
            return True
        
        # === AUDIT PIPELINE END ===
        
        # Save to database ONLY if validation passed
        snapshot_result = save_gamma_snapshot(
            ticker=symbol,
            interval_timestamp=now_et,
            pin_strike=gex_analysis['pin_strike'],
            pull_strength=gex_analysis.get('pull_strength', 0.5),
            spot_price=current_price,
            total_gex=aggregate_total_gex,  # Use canonical aggregate
            net_gex=aggregate_net_gex,      # Use canonical aggregate
            is_mock_data=is_mock
        )
        
        if snapshot_result:
            print(f"✓ Gamma snapshot saved for {symbol}: pin=${gex_analysis['pin_strike']:.2f}, spot=${current_price:.2f}, scope={expiration_scope}, mock={is_mock}")
            return True
        else:
            print(f"✗ Failed to save gamma snapshot for {symbol}")
            return False
            
    except Exception as e:
        import traceback
        print(f"Error fetching gamma snapshot for {symbol}: {str(e)}")
        traceback.print_exc()
        return False

def gamma_sampling_loop():
    """
    Main loop with adaptive sampling intervals based on time to close.
    - Regular session: 15-minute intervals
    - Last hour: 5-minute intervals
    - Last 30 minutes: 3-minute intervals
    - Last 15 minutes: 2-minute intervals (highest precision when it matters most)
    """
    global _scheduler_running
    
    api_key = os.getenv('Massive_API') or os.getenv('POLYGON_API_KEY')
    if not api_key:
        print("ERROR: Massive_API not found in environment, gamma sampling disabled")
        return
    
    print("🚀 Gamma sampling scheduler started (adaptive intervals)")
    
    # Track consecutive failures for backoff
    consecutive_failures = 0
    max_consecutive_failures = 3
    
    # Track last sample time to avoid duplicates
    last_sample_time = None
    last_interval = None
    
    while _scheduler_running:
        try:
            # Check market hours
            if is_market_hours():
                et_tz = pytz.timezone('US/Eastern')
                current_time_et = datetime.now(et_tz)
                
                # Get adaptive interval based on time to close
                interval_minutes, interval_mode = get_adaptive_sample_interval()
                
                # Log interval change
                if last_interval != interval_mode:
                    minutes_left = get_minutes_until_close()
                    close_time = get_market_close_time()
                    print(f"\n🔄 Sampling interval adjusted: {interval_minutes} min ({interval_mode})")
                    print(f"   Market closes at {close_time.hour}:{close_time.minute:02d} PM ET, {minutes_left} minutes left")
                    last_interval = interval_mode
                
                # Calculate which interval boundary we're in
                current_minute = current_time_et.minute
                current_boundary = (current_minute // interval_minutes) * interval_minutes
                
                # Create a timestamp for this boundary
                boundary_time = current_time_et.replace(minute=current_boundary, second=0, microsecond=0)
                
                # Sample if we haven't sampled this boundary yet
                if last_sample_time is None or boundary_time > last_sample_time:
                    minutes_left = get_minutes_until_close()
                    print(f"\n📊 Gamma sampling at {current_time_et.strftime('%I:%M %p ET')} [{interval_mode}] ({minutes_left} min to close)")
                    
                    # Sample all tracked symbols
                    success_count = 0
                    failed_count = 0
                    
                    for symbol in TRACKED_SYMBOLS:
                        if fetch_and_save_gamma_snapshot(api_key, symbol):
                            success_count += 1
                        else:
                            failed_count += 1
                    
                    print(f"✓ Gamma sampling complete ({success_count}/{len(TRACKED_SYMBOLS)} successful)\n")
                    
                    # Update last sample time
                    last_sample_time = boundary_time
                    
                    # Track failures for backoff
                    if failed_count == len(TRACKED_SYMBOLS):
                        consecutive_failures += 1
                        if consecutive_failures >= max_consecutive_failures:
                            print(f"⚠️ WARNING: {consecutive_failures} consecutive failures. Possible API rate limit.")
                            print("   Scheduler will continue but check Polygon API quota if this persists.")
                    else:
                        consecutive_failures = 0  # Reset on any success
            else:
                # Market closed, don't sample
                consecutive_failures = 0  # Reset failures when market closed
                last_sample_time = None  # Reset for next day
                last_interval = None  # Reset interval mode
            
            # Check every 30 seconds for faster response to interval changes
            # This allows quick adaptation when transitioning to higher frequency modes
            for _ in range(3):  # 3 * 10 seconds = 30 seconds
                if not _scheduler_running:
                    break
                time.sleep(10)
                
        except Exception as e:
            print(f"Error in gamma sampling loop: {str(e)}")
            time.sleep(30)  # Wait 30 seconds before retry on error
    
    print("🛑 Gamma sampling scheduler stopped")

def start_gamma_scheduler():
    """Start the background gamma sampling scheduler"""
    global _scheduler_running, _scheduler_thread
    
    if _scheduler_running:
        print("Gamma scheduler already running")
        return False
    
    _scheduler_running = True
    _scheduler_thread = threading.Thread(target=gamma_sampling_loop, daemon=True)
    _scheduler_thread.start()
    
    return True

def stop_gamma_scheduler():
    """Stop the background gamma sampling scheduler"""
    global _scheduler_running
    
    if not _scheduler_running:
        return False
    
    _scheduler_running = False
    
    # Wait for thread to finish (with timeout)
    if _scheduler_thread:
        _scheduler_thread.join(timeout=5)
    
    return True

def is_scheduler_running():
    """Check if gamma scheduler is currently running"""
    return _scheduler_running
