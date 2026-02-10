# Gamma Snapshot Automation Setup

## Overview

This application automatically collects and saves gamma snapshot data for market indices during trading hours. The gamma scheduler runs in the background and adapts its sampling frequency based on time to market close.

## ✅ Setup Complete

The following components are properly configured:

### 1. Database
- **Location**: SQLite database (`market_predictor.db`) or PostgreSQL (if `DATABASE_URL` is set)
- **Tables**: 
  - `gamma_pin_snapshots` - Simplified gamma snapshots
  - `gamma_audit_snapshots` - Comprehensive audit snapshots
  - `predictions` - Prediction history
- **Auto-initialization**: Database tables are created automatically on first startup

### 2. Gamma Scheduler
- **File**: `gamma_scheduler.py`
- **Auto-start**: Scheduler starts automatically when FastAPI app starts (see `app/api/main.py` line 181-182)
- **Tracked symbols**: SPX, NDX, DJI, RUT
- **Sampling intervals** (adaptive based on time to close):
  - Regular hours: 5 minutes
  - Last hour: 5 minutes  
  - Last 30 minutes: 3 minutes
  - Last 15 minutes: 2 minutes (highest precision)

### 3. Data Storage
Gamma snapshots are saved in two formats:

#### Database Storage
- Table: `gamma_pin_snapshots` and `gamma_audit_snapshots`
- Contains all snapshots with validity flags
- Used for historical queries and model training

#### NDJSON File Storage
- Location: `exports/{symbol}/{YYYY-MM-DD}.ndjson`
- One line per snapshot (newline-delimited JSON)
- Used for full-day observability and analysis
- Example: `exports/SPX/2026-02-10.ndjson`

### 4. Export Directories
Pre-created directories for all tracked symbols:
```
exports/
├── SPX/
├── NDX/
├── DJI/
├── RUT/
└── VIX/
```

## Running the Application

### Start the FastAPI Backend
```bash
python server.py
```

The server will:
- Initialize the database
- Start the gamma scheduler automatically
- Listen on http://0.0.0.0:8000

### Start the Streamlit Frontend (Optional)
```bash
streamlit run app.py --server.port 5000
```

### Environment Variables

#### Required for Live Data Collection
- `Massive_API` or `POLYGON_API_KEY` - Polygon.io API key for live market data
  - Without this, the scheduler will not collect live data
  - To set: `export Massive_API='your_api_key_here'`

#### Optional
- `DATABASE_URL` - PostgreSQL connection string
  - If not set, uses SQLite fallback
  - Example: `postgresql://user:password@host:port/database`

## Verification

Run the verification script to check all components:

```bash
python verify_gamma_setup.py
```

This will check:
- ✓ Environment variables
- ✓ Database connectivity and schema
- ✓ Exports directory structure
- ✓ Gamma scheduler module
- ✓ Market time utilities
- ✓ API integration

## Testing the Gamma Scheduler

### Manual Test
You can test the scheduler functionality manually:

```python
from gamma_scheduler import start_gamma_scheduler, is_scheduler_running, stop_gamma_scheduler

# Start the scheduler
start_gamma_scheduler()
print(f"Scheduler running: {is_scheduler_running()}")

# Wait for some snapshots to be collected...
# (Only collects during market hours)

# Stop the scheduler
stop_gamma_scheduler()
```

### Check Market Hours
```python
from gamma_scheduler import is_market_hours, get_adaptive_sample_interval

print(f"Market is open: {is_market_hours()}")
interval_min, interval_name = get_adaptive_sample_interval()
print(f"Current sampling interval: {interval_min} minutes ({interval_name})")
```

## Data Collection Behavior

### During Market Hours (9:30 AM - 4:00 PM ET, Mon-Fri)
- Scheduler automatically samples all tracked symbols
- Saves to database and NDJSON files
- Adapts sampling frequency based on time to close
- More frequent sampling as market close approaches

### Outside Market Hours
- Scheduler remains running but does not collect data
- No API calls are made
- Returns frozen snapshots from last valid market close

### Early Close Days
- Automatically detects early close days (1:00 PM ET)
- Adjusts sampling intervals accordingly
- Examples: Black Friday, Christmas Eve

## Checking Collected Data

### View Database Snapshots
```python
from database import SessionLocal, GammaPinSnapshot
from datetime import date

session = SessionLocal()
today = date.today()

# Get all snapshots for SPX today
snapshots = session.query(GammaPinSnapshot).filter(
    GammaPinSnapshot.ticker == 'SPX',
    GammaPinSnapshot.trading_date == today
).all()

print(f"SPX snapshots today: {len(snapshots)}")
for snap in snapshots[:5]:
    print(f"  {snap.interval_timestamp}: pin=${snap.pin_strike:.2f}, spot=${snap.spot_price:.2f}")
```

### View NDJSON Files
```bash
# List snapshots for today
ls -lh exports/SPX/$(date +%Y-%m-%d).ndjson

# View the first few snapshots
head -n 5 exports/SPX/$(date +%Y-%m-%d).ndjson | jq .

# Count snapshots
wc -l exports/SPX/$(date +%Y-%m-%d).ndjson
```

## API Endpoints

Once the FastAPI server is running, the following endpoints are available:

- `GET /health` - Health check with data freshness status
- `GET /api/status` - API capabilities and market status
- `GET /api/predictions` - Get predictions for all indices
- `GET /api/prediction/{symbol}` - Get detailed prediction for one index
- `GET /gamma/multi-expiry` - Multi-expiry gamma analysis

## Troubleshooting

### Scheduler Not Collecting Data
1. Check if API key is set: `echo $Massive_API`
2. Verify market is open: Market hours are 9:30 AM - 4:00 PM ET, Mon-Fri
3. Check server logs for errors
4. Run verification script: `python verify_gamma_setup.py`

### Database Errors
1. Check if database file exists: `ls -la market_predictor.db`
2. Try reinitializing: `python -c "from database import init_db; init_db()"`
3. Check for permission issues on database file

### Missing NDJSON Files
1. Verify exports directory exists: `ls -la exports/`
2. Check if market was open during expected collection time
3. Verify scheduler is running: Check server logs for "Gamma pin scheduler started"

### Server Won't Start
1. Install missing dependencies: `pip install fastapi uvicorn streamlit pandas numpy sqlalchemy`
2. Check for syntax errors in modified files
3. Verify Python version: 3.9+ required

## Next Steps

### Production Deployment
For production deployment, set the following:

1. **PostgreSQL Database**:
   ```bash
   export DATABASE_URL='postgresql://user:password@host:port/database'
   ```

2. **Polygon API Key**:
   ```bash
   export Massive_API='your_polygon_api_key'
   ```

3. **Run with proper process management**:
   - Use systemd, supervisor, or Docker
   - Ensure server restarts on failure
   - Set up log rotation

### Monitoring
- Monitor `exports/` directory for daily NDJSON files
- Check database growth and query performance
- Set up alerts for scheduler failures
- Monitor API rate limits (Polygon.io)

## Support

For issues or questions:
1. Run the verification script: `python verify_gamma_setup.py`
2. Check server logs for detailed error messages
3. Review this documentation for common issues
4. Check Polygon.io API status and rate limits
