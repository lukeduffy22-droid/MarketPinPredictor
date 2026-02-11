#!/usr/bin/env python3
"""
Demo script showing gamma scheduler functionality
Tests scheduler start/stop and configuration without requiring live API key
"""
import time
from datetime import datetime
import pytz

def demo_scheduler_lifecycle():
    """Demonstrate scheduler lifecycle"""
    print("=" * 60)
    print("DEMO: Gamma Scheduler Lifecycle")
    print("=" * 60)
    
    from gamma_scheduler import (
        start_gamma_scheduler, 
        stop_gamma_scheduler,
        is_scheduler_running,
        TRACKED_SYMBOLS,
        EXPORT_SNAPSHOTS
    )
    
    print(f"\n✓ Scheduler module loaded successfully")
    print(f"✓ Tracked symbols: {', '.join(TRACKED_SYMBOLS)}")
    print(f"✓ NDJSON export enabled: {EXPORT_SNAPSHOTS}")
    
    # Check initial state
    print(f"\nInitial state:")
    print(f"  Scheduler running: {is_scheduler_running()}")
    
    # Start scheduler
    print(f"\nStarting scheduler...")
    result = start_gamma_scheduler()
    print(f"  Start result: {result}")
    print(f"  Scheduler running: {is_scheduler_running()}")
    
    # Let it run for a few seconds
    print(f"\nScheduler is now running in background thread")
    print(f"  (During market hours, it would be collecting data)")
    print(f"  Letting it run for 3 seconds...")
    time.sleep(3)
    
    # Stop scheduler
    print(f"\nStopping scheduler...")
    result = stop_gamma_scheduler()
    print(f"  Stop result: {result}")
    print(f"  Scheduler running: {is_scheduler_running()}")
    
    print("\n✓ Scheduler lifecycle demo complete")


def demo_market_time_checks():
    """Demonstrate market time utilities"""
    print("\n" + "=" * 60)
    print("DEMO: Market Time Utilities")
    print("=" * 60)
    
    from gamma_scheduler import (
        is_market_hours,
        get_adaptive_sample_interval,
        get_minutes_until_close,
        get_market_close_time,
        MARKET_OPEN_TIME
    )
    
    et_tz = pytz.timezone('US/Eastern')
    current_time_et = datetime.now(et_tz)
    
    print(f"\nCurrent time (ET): {current_time_et.strftime('%Y-%m-%d %I:%M:%S %p %Z')}")
    print(f"Day of week: {current_time_et.strftime('%A')}")
    
    print(f"\nMarket hours configuration:")
    print(f"  Open time: {MARKET_OPEN_TIME.strftime('%I:%M %p')}")
    close_time = get_market_close_time()
    print(f"  Close time: {close_time.strftime('%I:%M %p')}")
    
    is_open = is_market_hours()
    print(f"\nMarket status:")
    print(f"  Market is open: {is_open}")
    
    if is_open:
        minutes_left = get_minutes_until_close()
        print(f"  Minutes until close: {minutes_left}")
    
    interval_min, interval_name = get_adaptive_sample_interval()
    print(f"\nSampling configuration:")
    print(f"  Current interval: {interval_min} minutes")
    print(f"  Interval mode: {interval_name}")
    
    # Explain the adaptive intervals
    print(f"\nAdaptive sampling intervals:")
    print(f"  • Regular hours (>60 min to close): 5 minutes")
    print(f"  • Last hour (31-60 min): 5 minutes")
    print(f"  • Last 30 minutes (16-30 min): 3 minutes")
    print(f"  • Last 15 minutes (0-15 min): 2 minutes (highest precision)")
    
    print("\n✓ Market time utilities demo complete")


def demo_data_storage():
    """Demonstrate data storage configuration"""
    print("\n" + "=" * 60)
    print("DEMO: Data Storage Configuration")
    print("=" * 60)
    
    import os
    from datetime import date
    
    # Show exports directory structure
    exports_dir = './exports'
    print(f"\nNDJSON storage location: {os.path.abspath(exports_dir)}")
    
    from gamma_scheduler import TRACKED_SYMBOLS
    for symbol in TRACKED_SYMBOLS:
        symbol_dir = os.path.join(exports_dir, symbol)
        today_file = os.path.join(symbol_dir, f"{date.today()}.ndjson")
        print(f"  {symbol}: {today_file}")
        if os.path.exists(today_file):
            lines = sum(1 for _ in open(today_file))
            print(f"    └─ {lines} snapshots collected")
        else:
            print(f"    └─ No snapshots yet today")
    
    # Show database info
    print(f"\nDatabase storage:")
    print(f"  File: market_predictor.db (SQLite)")
    print(f"  Tables:")
    print(f"    • gamma_pin_snapshots (simplified snapshots)")
    print(f"    • gamma_audit_snapshots (comprehensive audit data)")
    
    # Check database
    from database import SessionLocal, GammaPinSnapshot
    session = SessionLocal()
    try:
        today = date.today()
        count = session.query(GammaPinSnapshot).filter(
            GammaPinSnapshot.trading_date == today
        ).count()
        print(f"  Snapshots today in database: {count}")
    finally:
        session.close()
    
    print("\n✓ Data storage demo complete")


def main():
    """Run all demos"""
    print("\n" + "🎬 " + "=" * 58)
    print("   GAMMA SNAPSHOT AUTOMATION DEMO")
    print("=" * 60 + "\n")
    print("This demo shows the gamma scheduler functionality")
    print("without requiring a live API key or market data.\n")
    
    try:
        # Demo 1: Scheduler lifecycle
        demo_scheduler_lifecycle()
        
        # Demo 2: Market time utilities
        demo_market_time_checks()
        
        # Demo 3: Data storage
        demo_data_storage()
        
        # Summary
        print("\n" + "=" * 60)
        print("✅ ALL DEMOS COMPLETED SUCCESSFULLY")
        print("=" * 60)
        print("\nThe gamma scheduler is properly configured and ready to use.")
        print("\nTo collect live data:")
        print("  1. Set environment variable: export Massive_API='your_api_key'")
        print("  2. Start the FastAPI server: python server.py")
        print("  3. The scheduler will automatically start and collect data")
        print("     during market hours (Mon-Fri, 9:30 AM - 4:00 PM ET)")
        print("\nFor more information, see GAMMA_SETUP.md")
        print("=" * 60 + "\n")
        
    except Exception as e:
        print(f"\n❌ Demo failed with error: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(main())
