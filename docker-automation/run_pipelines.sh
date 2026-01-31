#!/bin/bash

# Pipeline Orchestrator
# Sequentially executes the full suite of ETL processes.
# Exits immediately on failure (Fail-Fast).

# Exit immediately if a command exits with a non-zero status
set -e

# Define a logging helper function for consistent timestamps
log() {
    echo "[$(date +'%Y-%m-%dT%H:%M:%S%z')] [INFO] $1"
}

error_handler() {
    echo "[$(date +'%Y-%m-%dT%H:%M:%S%z')] [CRITICAL] Pipeline failed on line $1"
    exit 1
}

# Trap errors and pass the line number to the handler
trap 'error_handler $LINENO' ERR

# --- Start Execution ---

log "Starting Data Acquisition Sequence..."

# 1. Universe Generation
# Must run first to define the asset list for historical backfills.
log "Step 1/5: Generating Historical Universe..."
python pipelines/universe/universe_generation_pipeline.py

# 2. Historical Research Data (The 'Bronze' Layer)
# Fetches full history for backtesting.
log "Step 2/5: Running Historical ETL (Research Layer)..."
python pipelines/historical_data/historical_data_etl.py

# 3. Production Dashboard Data
# Fetches live/current data for the user-facing app.
log "Step 3/5: Updating Production Dashboard Data..."
python pipelines/live_data/live_data_pipeline.py

# 4. Derivatives Snapshot
# Daily capture of Open Interest and Funding Rates.
log "Step 4/5: Capturing Derivatives Market Snapshot..."
python pipelines/derivatives/derivatives_snapshot_etl.py

# 5. Coinbase Universe
# Specialized ingest for exchange-specific analysis.
log "Step 5/5: Ingesting Coinbase Asset Universe..."
python pipelines/coinbase/coinbase_universe_pipeline.py

log "All pipelines completed successfully."