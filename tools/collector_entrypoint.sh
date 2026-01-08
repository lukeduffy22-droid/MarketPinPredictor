#!/usr/bin/env bash
set -euo pipefail

# Collector+QC entrypoint
# - Starts local API collector in background (writes into /app/exports)
# - Starts Massive live data collector in background for index funds
# - Periodically runs exports_to_parquet.py to create combined snapshots
# - Runs add_quality_flags.py to produce QC'd parquet

BASE_DIR=${BASE_DIR:-/app}
EXPORTS_DIR=${EXPORTS_DIR:-$BASE_DIR/exports}
COLLECTOR_INTERVAL=${COLLECTOR_INTERVAL:-60}
MASSIVE_INTERVAL=${MASSIVE_INTERVAL:-60}
QC_INTERVAL=${QC_INTERVAL:-60}
ENDPOINTS=${ENDPOINTS:-"/health /gamma/multi-expiry /orb /predict/eod"}

echo "Starting collector entrypoint"
echo "EXPORTS_DIR=$EXPORTS_DIR"
echo "COLLECTOR_INTERVAL=$COLLECTOR_INTERVAL (local API)"
echo "MASSIVE_INTERVAL=$MASSIVE_INTERVAL (live data from Massive API)"
echo "QC_INTERVAL=$QC_INTERVAL"

# ensure exports dir exists
mkdir -p "$EXPORTS_DIR"

# Check if Massive API key is available
if [ -z "${Massive_API:-}" ]; then
  echo "WARNING: Massive_API env var not set - will collect local API data only"
  echo "To enable live data collection from Massive, set: export Massive_API=your_api_key"
else
  echo "Massive API key detected - will collect live index data"
fi

# Start the local API collector in background (split endpoints into array)
IFS=' ' read -r -a ENDPOINT_ARR <<< "$ENDPOINTS"
python tools/save_market_data.py --endpoints "${ENDPOINT_ARR[@]}" --out-dir "$EXPORTS_DIR" --interval $COLLECTOR_INTERVAL &
COL_PID=$!

echo "Collector started (pid=$COL_PID)"

# Start Massive live data collector if API key is available
if [ -n "${Massive_API:-}" ]; then
  python tools/save_massive_live_data.py --api-key "$Massive_API" --out-dir "$EXPORTS_DIR" --interval $MASSIVE_INTERVAL &
  MASSIVE_PID=$!
  echo "Massive live data collector started (pid=$MASSIVE_PID)"
else
  MASSIVE_PID=""
  echo "Skipping Massive live data collector (no API key)"
fi

# Run an initial exports+QC pass immediately
python tools/exports_to_parquet.py --exports-dir "$EXPORTS_DIR" --out "$EXPORTS_DIR/collected/parquet" || echo "exports_to_parquet failed"
# wait for the combined snapshots file to appear (avoid race)
COMBINED="$EXPORTS_DIR/collected/parquet/combined_snapshots.parquet"
WAIT_SECS=10
i=0
while [ ! -f "$COMBINED" ] && [ $i -lt $WAIT_SECS ]; do
  sleep 1
  i=$((i+1))
done

if [ -f "$COMBINED" ]; then
  python tools/add_quality_flags.py --inp "$COMBINED" --out-dir "$EXPORTS_DIR/collected/parquet" || echo "add_quality_flags failed"
  # Write QC status and optionally push to S3
  QC_PARQUET="$EXPORTS_DIR/collected/parquet/combined_snapshots_qc.parquet"
  QC_STATUS_JSON="$EXPORTS_DIR/collected/parquet/qc_status.json"
  if [ -f "$QC_PARQUET" ]; then
    python tools/qc_status.py --qc-parquet "$QC_PARQUET" --status-path "$QC_STATUS_JSON" || echo "qc_status.py failed"
  else
    echo "QC Parquet not found, skipping qc_status.py"
  fi
else
  echo "Combined snapshots file not found after ${WAIT_SECS}s, skipping QC"
fi

# Periodically run exports -> QC
while true; do
  sleep "$QC_INTERVAL"
  echo "Running exports_to_parquet and QC at $(date -u)"
  python tools/exports_to_parquet.py --exports-dir "$EXPORTS_DIR" --out "$EXPORTS_DIR/collected/parquet" || echo "exports_to_parquet failed"
  # wait for the combined snapshots file to appear (avoid race)
  COMBINED="$EXPORTS_DIR/collected/parquet/combined_snapshots.parquet"
  WAIT_SECS=10
  i=0
  while [ ! -f "$COMBINED" ] && [ $i -lt $WAIT_SECS ]; do
    sleep 1
    i=$((i+1))
  done
  if [ -f "$COMBINED" ]; then
    python tools/add_quality_flags.py --inp "$COMBINED" --out-dir "$EXPORTS_DIR/collected/parquet" || echo "add_quality_flags failed"
    # Write QC status and optionally push to S3
    QC_PARQUET="$EXPORTS_DIR/collected/parquet/combined_snapshots_qc.parquet"
    QC_STATUS_JSON="$EXPORTS_DIR/collected/parquet/qc_status.json"
    if [ -f "$QC_PARQUET" ]; then
      python tools/qc_status.py --qc-parquet "$QC_PARQUET" --status-path "$QC_STATUS_JSON" || echo "qc_status.py failed"
    else
      echo "QC Parquet not found, skipping qc_status.py"
    fi
  else
    echo "Combined snapshots file not found after ${WAIT_SECS}s, skipping QC"
  fi
done

