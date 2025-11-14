"""
Background Scheduler for Automatic Gamma Pin Sampling
Runs every 15 minutes during market hours to track gamma evolution
"""
import threading
import time
import os
from datetime import datetime, time as dt_time
import pytz
from database import save_gamma_snapshot
from websocket_streaming import get_snapshot_data
from options_gamma import fetch_options_chain, calculate_gamma_exposure

# Market hours in Eastern Time
MARKET_OPEN_TIME = dt_time(9, 30)  # 9:30 AM ET
MARKET_CLOSE_TIME = dt_time(16, 0)  # 4:00 PM ET
SAMPLE_INTERVAL_MINUTES = 15
TRACKED_SYMBOLS = ['SPX', 'NDX', 'DJI', 'RUT']

# Global flag to control scheduler
_scheduler_running = False
_scheduler_thread = None

def is_market_hours():
    """Check if current time is during regular trading hours (9:30 AM - 4:00 PM ET)"""
    try:
        et_tz = pytz.timezone('US/Eastern')
        now_et = datetime.now(et_tz)
        current_time = now_et.time()
        
        # Check if weekday (Monday = 0, Sunday = 6)
        if now_et.weekday() >= 5:  # Saturday or Sunday
            return False
        
        # Check if within market hours
        if MARKET_OPEN_TIME <= current_time < MARKET_CLOSE_TIME:
            return True
        
        return False
    except Exception as e:
        print(f"Error checking market hours: {str(e)}")
        return False

def fetch_and_save_gamma_snapshot(api_key, symbol):
    """
    Fetch current gamma data for a symbol and save to database
    
    Returns True if successful, False otherwise
    """
    try:
        # Get current price using snapshot API
        polygon_ticker = f"I:{symbol}"
        snapshot = get_snapshot_data(api_key, [polygon_ticker])
        
        if not snapshot or polygon_ticker not in snapshot:
            print(f"Warning: Could not fetch snapshot data for {symbol}, skipping gamma sample")
            return False
        
        current_price = snapshot[polygon_ticker].get('price')
        if not current_price:
            print(f"Warning: No price in snapshot for {symbol}, skipping gamma sample")
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
        snapshot_result = save_gamma_snapshot(
            ticker=symbol,
            interval_timestamp=datetime.now(),
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
    """Main loop that runs every 15 minutes during market hours"""
    global _scheduler_running
    
    api_key = os.getenv('POLYGON_API_KEY')
    if not api_key:
        print("ERROR: POLYGON_API_KEY not found in environment, gamma sampling disabled")
        return
    
    print("🚀 Gamma sampling scheduler started")
    
    # Track consecutive failures for backoff
    consecutive_failures = 0
    max_consecutive_failures = 3
    
    while _scheduler_running:
        try:
            # Check market hours
            if is_market_hours():
                et_tz = pytz.timezone('US/Eastern')
                current_time_et = datetime.now(et_tz)
                
                # Only sample on 15-minute boundaries (9:30, 9:45, 10:00, etc.)
                current_minute = current_time_et.minute
                if current_minute % 15 == 0:  # On a 15-minute boundary
                    print(f"\n📊 Running gamma sampling at {current_time_et.strftime('%I:%M %p ET')}")
                    
                    # Sample all tracked symbols
                    success_count = 0
                    failed_count = 0
                    
                    for symbol in TRACKED_SYMBOLS:
                        if fetch_and_save_gamma_snapshot(api_key, symbol):
                            success_count += 1
                        else:
                            failed_count += 1
                    
                    print(f"✓ Gamma sampling cycle complete ({success_count}/{len(TRACKED_SYMBOLS)} successful)\n")
                    
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
            
            # Sleep for 15 minutes before next sample
            # Check every 30 seconds if scheduler should stop (for graceful shutdown)
            for _ in range(30):  # 30 * 30 seconds = 15 minutes
                if not _scheduler_running:
                    break
                time.sleep(30)
                
        except Exception as e:
            print(f"Error in gamma sampling loop: {str(e)}")
            time.sleep(60)  # Wait 1 minute before retry on error
    
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
