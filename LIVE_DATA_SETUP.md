# Live Data Collection Setup Guide

## Overview
The system now automatically collects live market data from two sources:

1. **Local API Data** - collected from your backend endpoints
2. **Massive/Polygon API Data** - live index and options data (requires API key)

## Setting Up Massive API Live Data Collection

### Step 1: Get Your Massive API Key
If you already have live data access to the top index funds (SPX, NDX, RUT, VIX, DJI), you have a Massive/Polygon API key with live data tier access.

### Step 2: Set Environment Variable

#### Option A: Local Development
```bash
export Massive_API="your_api_key_here"
```

#### Option B: Docker Container
When running the Docker container, pass the environment variable:
```bash
docker run -e Massive_API="your_api_key_here" your_image:tag
```

#### Option C: Docker Compose
Add to your `docker-compose.yml`:
```yaml
environment:
  - Massive_API=your_api_key_here
```

#### Option D: Kubernetes / Production
Set the `Massive_API` environment variable in your deployment configuration.

### Step 3: Start Collection

The data collection starts automatically when the container runs. All live data is collected to:
```
/app/exports/massive/
```

### What Gets Collected

**Indices (every 10 seconds by default):**
- SPX (S&P 500 Index)
- NDX (NASDAQ-100 Index)
- RUT (Russell 2000 Index)
- VIX (Volatility Index)
- DJI (Dow Jones Index)

**Options Data:**
- SPX options chain (top 100 active contracts)
- NDX options chain
- RUT options chain

**Data Format:**
- Raw JSON: `exports/massive/indices/{YYYY-MM-DD}/{SYMBOL}_{TIMESTAMP}.json`
- Parquet: `exports/massive/indices/{YYYY-MM-DD}/{SYMBOL}_{TIMESTAMP}.parquet`
- Options: `exports/massive/options/{YYYY-MM-DD}/{SYMBOL}_chain_{TIMESTAMP}.parquet`

## Configuration

### Environment Variables
- `Massive_API` - Your API key (required for live collection)
- `MASSIVE_INTERVAL` - Seconds between collections (default: 10)
- `EXPORTS_DIR` - Output directory (default: /app/exports)
- `COLLECTOR_INTERVAL` - Local API collection interval (default: 10)
- `QC_INTERVAL` - Quality check cycle interval (default: 10)

### Example: Faster Collection
```bash
export Massive_API="your_api_key_here"
export MASSIVE_INTERVAL=30  # Collect every 30 seconds instead of 10
```

## Monitoring

Check the collector logs to verify live data is being collected:
```bash
# Watch collection activity
tail -f /app/logs/collector.log

# Check collected data
ls -lh /app/exports/massive/
```

## Data Processing Pipeline

After collection, the pipeline automatically:

1. **Combines snapshots** - Merges all collected data
2. **Quality checks** - Validates data integrity with `add_quality_flags.py`
3. **Exports to Parquet** - Creates optimized columnar format for analysis

## Verification

To verify live data collection is working:

```python
import pandas as pd
from pathlib import Path

# Check latest collected indices
indices_dir = Path("/app/exports/massive/indices")
for json_file in sorted(indices_dir.glob("**/*.json"))[-5:]:
    print(f"✓ {json_file}")

# Read parquet data
df = pd.read_parquet("/app/exports/massive/indices/*/SPX*.parquet")
print(f"SPX samples collected: {len(df)}")
print(df.head())
```

## Troubleshooting

### No live data being collected?

1. **Check if Massive_API is set:**
   ```bash
   echo $Massive_API
   ```

2. **Verify API key is valid:**
   - Test with a simple Polygon API call
   - Check if your account has live data tier access

3. **Check collector logs:**
   ```bash
   docker logs <container_id> | grep -i massive
   ```

### High latency in collection?

- Increase `MASSIVE_INTERVAL` to reduce frequency
- Check API rate limits (live tier typically 120 req/min)
- Verify network connectivity

### Storage growing too fast?

- Reduce collection frequency: `export MASSIVE_INTERVAL=300` (5 min)
- Archive older data to S3 or other storage
- Adjust retention policy in exports pipeline

## Additional Resources

- Massive/Polygon API Docs: https://polygon.io/docs/
- Live Data Tier: Includes real-time index quotes and options data
- Support: Check your Massive/Polygon account dashboard
