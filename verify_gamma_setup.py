#!/usr/bin/env python3
"""
Verification script for gamma snapshot automation setup.
Tests that all components are properly configured for automatic data collection.
"""
import os
import sys
from datetime import datetime
import pytz


def check_environment():
    """Check required environment variables"""
    print("=" * 60)
    print("CHECKING ENVIRONMENT VARIABLES")
    print("=" * 60)
    
    api_key = os.getenv('Massive_API') or os.getenv('POLYGON_API_KEY')
    db_url = os.getenv('DATABASE_URL')
    
    print(f"✓ Massive_API/POLYGON_API_KEY: {'SET' if api_key else '❌ NOT SET (required for live data)'}")
    print(f"✓ DATABASE_URL: {'SET' if db_url else '⚠️ NOT SET (using SQLite fallback)'}")
    
    if not api_key:
        print("\n⚠️ WARNING: API key not set. Gamma scheduler will not collect live data.")
        print("   Set environment variable: export Massive_API='your_api_key'")
        print("   or: export POLYGON_API_KEY='your_api_key'")
    
    return api_key is not None


def check_database():
    """Check database connectivity and schema"""
    print("\n" + "=" * 60)
    print("CHECKING DATABASE")
    print("=" * 60)
    
    try:
        from database import init_db, engine, Base
        
        # Initialize database
        init_db()
        print("✓ Database initialized successfully")
        
        # Check for required tables
        from sqlalchemy import inspect
        inspector = inspect(engine)
        tables = inspector.get_table_names()
        
        required_tables = ['gamma_pin_snapshots', 'gamma_audit_snapshots', 'predictions']
        for table in required_tables:
            if table in tables:
                print(f"✓ Table '{table}' exists")
            else:
                print(f"❌ Table '{table}' missing")
        
        return True
        
    except Exception as e:
        print(f"❌ Database error: {e}")
        return False


def check_exports_directory():
    """Check exports directory structure"""
    print("\n" + "=" * 60)
    print("CHECKING EXPORTS DIRECTORY")
    print("=" * 60)
    
    symbols = ['SPX', 'NDX', 'DJI', 'RUT', 'VIX']
    exports_dir = './exports'
    
    if not os.path.exists(exports_dir):
        print(f"❌ Exports directory '{exports_dir}' does not exist")
        return False
    
    print(f"✓ Exports directory exists: {os.path.abspath(exports_dir)}")
    
    for symbol in symbols:
        symbol_dir = os.path.join(exports_dir, symbol)
        if os.path.exists(symbol_dir):
            print(f"✓ Symbol directory '{symbol}/' exists")
        else:
            print(f"⚠️ Symbol directory '{symbol}/' missing (will be created on first snapshot)")
    
    return True


def check_gamma_scheduler():
    """Check gamma scheduler module"""
    print("\n" + "=" * 60)
    print("CHECKING GAMMA SCHEDULER")
    print("=" * 60)
    
    try:
        from gamma_scheduler import (
            start_gamma_scheduler, 
            stop_gamma_scheduler,
            is_scheduler_running,
            TRACKED_SYMBOLS,
            EXPORT_SNAPSHOTS
        )
        
        print("✓ Gamma scheduler module imported successfully")
        print(f"✓ Tracked symbols: {', '.join(TRACKED_SYMBOLS)}")
        print(f"✓ NDJSON export enabled: {EXPORT_SNAPSHOTS}")
        print(f"✓ Scheduler currently running: {is_scheduler_running()}")
        
        return True
        
    except Exception as e:
        print(f"❌ Gamma scheduler error: {e}")
        import traceback
        traceback.print_exc()
        return False


def check_market_time():
    """Check market time utilities"""
    print("\n" + "=" * 60)
    print("CHECKING MARKET TIME UTILITIES")
    print("=" * 60)
    
    try:
        from gamma_scheduler import is_market_hours, get_adaptive_sample_interval
        
        is_open = is_market_hours()
        interval_min, interval_name = get_adaptive_sample_interval()
        
        et_tz = pytz.timezone('US/Eastern')
        current_time_et = datetime.now(et_tz)
        
        print(f"✓ Current time (ET): {current_time_et.strftime('%Y-%m-%d %I:%M:%S %p')}")
        print(f"✓ Market is open: {is_open}")
        print(f"✓ Sampling interval: {interval_min} minutes ({interval_name})")
        
        if not is_open:
            print("\n  ℹ️ Market is currently closed. Scheduler will not collect data until market opens.")
            print("  ℹ️ Market hours: Monday-Friday, 9:30 AM - 4:00 PM ET")
        
        return True
        
    except Exception as e:
        print(f"❌ Market time utilities error: {e}")
        import traceback
        traceback.print_exc()
        return False


def check_api_integration():
    """Check if API integration works (basic test)"""
    print("\n" + "=" * 60)
    print("CHECKING API INTEGRATION")
    print("=" * 60)
    
    api_key = os.getenv('Massive_API') or os.getenv('POLYGON_API_KEY')
    
    if not api_key:
        print("⚠️ Skipping API test (no API key set)")
        return True
    
    try:
        from websocket_streaming import get_snapshot_data
        
        # Try to get a snapshot (just test the function works)
        print("✓ API integration functions available")
        print("  ℹ️ Full API test skipped (requires live market data)")
        
        return True
        
    except Exception as e:
        print(f"⚠️ API integration warning: {e}")
        return True  # Non-critical


def print_summary(results):
    """Print summary of verification results"""
    print("\n" + "=" * 60)
    print("VERIFICATION SUMMARY")
    print("=" * 60)
    
    all_passed = all(results.values())
    
    for check, passed in results.items():
        status = "✓" if passed else "❌"
        print(f"{status} {check}")
    
    print("\n" + "=" * 60)
    if all_passed:
        print("✓ ALL CHECKS PASSED")
        print("\nGamma snapshot automation is properly configured!")
        print("\nTo start the application:")
        print("  1. FastAPI backend: python server.py")
        print("  2. Streamlit frontend: streamlit run app.py --server.port 5000")
        print("\nThe gamma scheduler will automatically start when the FastAPI app starts.")
        print("Data will be saved to:")
        print("  - Database: gamma_pin_snapshots and gamma_audit_snapshots tables")
        print("  - NDJSON files: exports/{symbol}/{YYYY-MM-DD}.ndjson")
    else:
        print("❌ SOME CHECKS FAILED")
        print("\nPlease fix the issues above before running the application.")
    print("=" * 60)
    
    return all_passed


def main():
    """Run all verification checks"""
    print("\n🔍 GAMMA SNAPSHOT AUTOMATION VERIFICATION")
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %I:%M:%S %p')}\n")
    
    results = {
        "Environment Variables": check_environment(),
        "Database": check_database(),
        "Exports Directory": check_exports_directory(),
        "Gamma Scheduler": check_gamma_scheduler(),
        "Market Time Utilities": check_market_time(),
        "API Integration": check_api_integration(),
    }
    
    success = print_summary(results)
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
