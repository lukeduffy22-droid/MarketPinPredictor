#!/usr/bin/env bash
set -euo pipefail

# Collector+QC entrypoint
# - Starts the collector in background (writes into /app/exports)
# - Periodically runs exports_to_parquet.py to create combined snapshots
# - Runs add_quality_flags.py to produce QC'd parquet

BASE_DIR=${BASE_DIR:-/app}
EXPORTS_DIR=${EXPORTS_DIR:-$BASE_DIR/exports}
COLLECTOR_INTERVAL=${COLLECTOR_INTERVAL:-60}
QC_INTERVAL=${QC_INTERVAL:-60}
ENDPOINTS=${ENDPOINTS:-"/health /gamma/multi-expiry /orb /predict/eod"}

echo "Starting collector entrypoint"
echo "EXPORTS_DIR=$EXPORTS_DIR, COLLECTOR_INTERVAL=$COLLECTOR_INTERVAL, QC_INTERVAL=$QC_INTERVAL"

# ensure exports dir exists
mkdir -p "$EXPORTS_DIR"


# Start the collector in background (split endpoints into array)
IFS=' ' read -r -a ENDPOINT_ARR <<< "$ENDPOINTS"
python tools/save_market_data.py --endpoints "${ENDPOINT_ARR[@]}" --out-dir "$EXPORTS_DIR" --interval $COLLECTOR_INTERVAL &
COL_PID=$!

echo "Collector started (pid=$COL_PID)"

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

