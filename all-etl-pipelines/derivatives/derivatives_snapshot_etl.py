"""
Daily Derivatives Snapshot ETL

This script executes a daily extraction of the global cryptocurrency derivatives
market (Futures, Perpetuals, Options). It iterates through all major exchanges
supported by the data provider, capturing a snapshot of critical metrics
including Funding Rates, Open Interest, and Trading Volume.

The data is persisted to a Data Lake (Parquet format) to enable longitudinal
analysis of market sentiment and leverage.

Author: John Swindell
"""

import os
import sys
import time
from datetime import datetime, timezone
import pandas as pd
from dotenv import load_dotenv
from pycoingecko import CoinGeckoAPI

# --- Configuration & Paths ---
load_dotenv()

# Infrastructure Config
GCP_PROJECT_ID = os.getenv('GCP_PROJECT_ID')
GCP_BUCKET_NAME = os.getenv('GCP_BUCKET_NAME')
CG_API_KEY = os.getenv("COINGECKO_PRO_API_KEY")

# Directory Structure
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(os.path.dirname(SCRIPT_DIR))
DATA_DIR = os.path.join(PROJECT_ROOT, 'data')
CACHE_DIR = os.path.join(DATA_DIR, 'cache')
BRONZE_DIR = os.path.join(DATA_DIR, 'bronze')

# Import Shared Modules
sys.path.append(os.path.dirname(SCRIPT_DIR))
from pipeline_helpers import GCSCachingManager


def fetch_daily_snapshot(api_client, cacher):
    """
    Orchestrates the retrieval of the daily derivatives snapshot.
    Checks the cloud cache first to prevent duplicate processing for the same day.
    """
    today_iso = datetime.now(timezone.utc).strftime('%Y-%m-%d')
    cache_key = f"derivatives/daily_snapshots/{today_iso}.parquet"

    # 1. Idempotency Check
    # If we already have data for today in the cloud, do not re-fetch.
    cached_df = cacher.get(cache_key)
    if cached_df is not None:
        print(f"Info: Snapshot for {today_iso} already exists. Skipping ingestion.")
        return cached_df

    print(f"Starting ingestion for {today_iso}...")
    all_contracts = []

    try:
        # 2. Discovery
        # Get list of all available derivatives exchanges
        exchanges = api_client.get_derivatives_exchanges()
        print(f"Targeting {len(exchanges)} exchanges.")

        # 3. Extraction
        for exchange in exchanges:
            exchange_id = exchange['id']
            try:
                # Fetch all tickers for this specific exchange
                data = api_client.get_derivatives_exchanges_by_id(
                    id=exchange_id,
                    include_tickers='all'
                )

                # Normalize Data
                for ticker in data.get('tickers', []):
                    all_contracts.append({
                        'snapshot_date': pd.to_datetime(today_iso),
                        'exchange': exchange_id,
                        'symbol': ticker.get('symbol'),
                        'base_asset': ticker.get('base'),
                        'target_asset': ticker.get('target'),
                        'contract_type': ticker.get('contract_type'),
                        'last_price': pd.to_numeric(ticker.get('last'), errors='coerce'),
                        'volume_24h': ticker.get('converted_volume', {}).get('usd'),
                        'funding_rate': ticker.get('funding_rate'),
                        'open_interest': ticker.get('open_interest_usd')
                    })

                # Rate Limiting
                time.sleep(1.5)

            except Exception as e:
                print(f"Warning: Failed to fetch data for exchange '{exchange_id}': {e}")
                continue

        # 4. Persistence
        if not all_contracts:
            print("Error: No contracts found.")
            return pd.DataFrame()

        final_df = pd.DataFrame(all_contracts)

        # Persist to Data Lake via Cacher
        cacher.set(cache_key, final_df)

        return final_df

    except Exception as e:
        print(f"Critical Error during extraction: {e}")
        return pd.DataFrame()


def main():
    print("--- Derivatives Market Snapshot ETL ---")

    # 1. Setup
    if not CG_API_KEY:
        sys.exit("Error: COINGECKO_PRO_API_KEY not found in environment.")

    os.makedirs(CACHE_DIR, exist_ok=True)

    # 2. Initialization
    try:
        cg = CoinGeckoAPI(api_key=CG_API_KEY)
        cacher = GCSCachingManager(project_id=GCP_PROJECT_ID, bucket_name=GCP_BUCKET_NAME)
    except Exception as e:
        sys.exit(f"Initialization failed: {e}")

    # 3. Execution
    df = fetch_daily_snapshot(cg, cacher)

    # 4. Validation & Local Backup
    if not df.empty:
        today_str = datetime.now(timezone.utc).strftime('%Y-%m-%d')
        local_output = os.path.join(CACHE_DIR, f"derivatives_{today_str}.parquet")

        df.to_parquet(local_output)

        print(f"Success: Process complete.")
        print(f"Stats: {len(df)} contracts captured across {df['exchange'].nunique()} exchanges.")
        print(f"Local backup saved to: {local_output}")
    else:
        sys.exit("Pipeline failed to acquire data.")


if __name__ == "__main__":
    main()