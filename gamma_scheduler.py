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
        from app.utils.time_et import is_regular_hours, is_market_holiday
        from datetime import datetime, timezone
        
        if is_market_holiday():
            return False
        
        return is_regular_hours(datetime.now(timezone.utc))
    except:
        # Fallback to simple check
        et_tz = pytz.timezone('US/Eastern')
        now_et = datetime.now(et_tz)
        current_time = now_et.time()
        
        if now_et.weekday() >= 5:
            return False
        
        close_time = get_market_close_time()
        if MARKET_OPEN_TIME <= current_time < close_time:
            return True
        
        return False

def fetch_and_save_gamma_snapshot(api_key, symbol):
    """
    Fetch current gamma data for a symbol and save to database
    
    Returns True if successful, False otherwise
    """
    try:
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
        
        # Save to database (timestamp will be auto-rounded to 15-min boundary)
        et_tz = pytz.timezone('US/Eastern')
        snapshot_result = save_gamma_snapshot(
            ticker=symbol,
            interval_timestamp=datetime.now(et_tz),
            pin_strike=gex_analysis['pin_strike'],
            pull_strength=gex_analysis.get('pull_strength', 0.5),
            spot_price=current_price,
            total_gex=gex_analysis.get('total_gex', 0),
            net_gex=gex_analysis.get('net_gex', 0),
            is_mock_data=is_mock
        )
        
        if snapshot_result:
            print(f"✓ Gamma snapshot saved for {symbol}: pin=${gex_analysis['pin_strike']:.2f}, spot=${current_price:.2f}, mock={is_mock}")
            return True
        else:
            print(f"✗ Failed to save gamma snapshot for {symbol}")
            return False
            
    except Exception as e:
        print(f"Error fetching gamma snapshot for {symbol}: {str(e)}")
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
    
    api_key = os.getenv('POLYGON_API_KEY')
    if not api_key:
        print("ERROR: POLYGON_API_KEY not found in environment, gamma sampling disabled")
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
