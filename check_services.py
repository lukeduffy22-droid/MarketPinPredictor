"""
Quick service status checker for MarketPinPredictor
Checks if backend API and data collection services are running
"""
import requests
import sys
from datetime import datetime

def check_backend():
    """Check if FastAPI backend is running"""
    try:
        response = requests.get("http://localhost:8000/health", timeout=2)
        if response.status_code == 200:
            data = response.json()
            print("✅ Backend API: RUNNING")
            print(f"   Status: {data.get('status', 'unknown')}")

            provider = data.get("market_data_provider") or data.get("provider")
            if provider:
                print(f"   Provider: {provider}")
            streaming = data.get("streaming_active")
            if streaming is not None:
                print(f"   Streaming: {'active' if streaming else 'stopped'}")

            return True
        else:
            print(f"⚠️  Backend API: Responding but unhealthy (status {response.status_code})")
            return False
    except requests.exceptions.ConnectionError:
        print("❌ Backend API: NOT RUNNING")
        print("   Recover the full stack with: powershell -File .\\start_databento_app.ps1")
        return False
    except Exception as e:
        print(f"❌ Backend API: ERROR - {str(e)}")
        return False

def check_streamlit():
    """Check if Streamlit frontend is accessible"""
    try:
        response = requests.get("http://localhost:8501", timeout=2)
        if response.status_code == 200:
            print("✅ Streamlit Frontend: RUNNING")
            print("   Access at: http://localhost:8501")
            return True
        else:
            print(f"⚠️  Streamlit Frontend: Responding but unhealthy (status {response.status_code})")
            return False
    except requests.exceptions.ConnectionError:
        print("❌ Streamlit Frontend: NOT RUNNING")
        print("   Recover the full stack with: powershell -File .\\start_databento_app.ps1")
        return False
    except Exception as e:
        print(f"❌ Streamlit Frontend: ERROR - {str(e)}")
        return False

def check_gamma_snapshots():
    """Check if gamma snapshots are being recorded"""
    import os

    snapshot_file = "gamma_snapshots.csv"
    if os.path.exists(snapshot_file):
        size = os.path.getsize(snapshot_file)
        modified = datetime.fromtimestamp(os.path.getmtime(snapshot_file))
        age_minutes = (datetime.now() - modified).total_seconds() / 60

        print(f"✅ Gamma Snapshots: FILE EXISTS")
        print(f"   File: {snapshot_file}")
        print(f"   Size: {size:,} bytes")
        print(f"   Last modified: {modified.strftime('%Y-%m-%d %H:%M:%S')} ({age_minutes:.1f} min ago)")

        if age_minutes > 5:
            print("   ⚠️  Warning: File hasn't been updated in >5 minutes")
            return False
        return True
    else:
        print(f"❌ Gamma Snapshots: FILE NOT FOUND")
        print(f"   Expected: {snapshot_file}")
        print("   Gamma capture is owned by the guarded full-stack launcher")
        return False

def check_ndjson_exports():
    """Check if NDJSON exports exist"""
    import os

    export_dir = "exports"
    if os.path.exists(export_dir):
        symbols = [d for d in os.listdir(export_dir) if os.path.isdir(os.path.join(export_dir, d))]
        if symbols:
            print(f"✅ NDJSON Exports: {len(symbols)} symbols")
            for symbol in symbols:
                symbol_dir = os.path.join(export_dir, symbol)
                files = [f for f in os.listdir(symbol_dir) if f.endswith('.ndjson')]
                if files:
                    latest = max(files)
                    file_path = os.path.join(symbol_dir, latest)
                    size = os.path.getsize(file_path)
                    print(f"   {symbol}: {latest} ({size:,} bytes)")
            return True
        else:
            print("⚠️  NDJSON Exports: Directory exists but empty")
            return False
    else:
        print("❌ NDJSON Exports: Directory not found")
        print("   Exports will be created during market hours")
        return False

def main():
    print("=" * 60)
    print("MarketPinPredictor - Service Status Check")
    print("=" * 60)
    print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()

    # Check all services
    backend_ok = check_backend()
    print()

    streamlit_ok = check_streamlit()
    print()

    snapshots_ok = check_gamma_snapshots()
    print()

    exports_ok = check_ndjson_exports()
    print()

    # Summary
    print("=" * 60)
    print("Summary:")
    print("=" * 60)

    all_ok = backend_ok and streamlit_ok

    if all_ok:
        print("✅ All core services are running!")
        if snapshots_ok:
            print("✅ Data collection is active")
        else:
            print("⚠️  Data collection may not be active")
    else:
        print("❌ Some services are not running")
        print()
        print("To recover the verified backend/dashboard pair:")
        if not (backend_ok and streamlit_ok):
            print("  powershell -File .\\start_databento_app.ps1")
        if not snapshots_ok:
            print("  Gamma capture remains unavailable until the guarded stack is healthy")

    print()
    return 0 if all_ok else 1

if __name__ == "__main__":
    sys.exit(main())
