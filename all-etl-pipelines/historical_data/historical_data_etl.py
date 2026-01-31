"""
Historical Market Data ETL Pipeline

This script orchestrates the extraction, transformation, and loading (ETL) of
historical cryptocurrency market data. It integrates multiple data sources
(Market, On-Chain, Social) to build a robust, point-in-time dataset for
quantitative analysis and backtesting.

Features:
- Two-level caching (Local + Cloud Object Storage)
- Survivorship-bias-free universe reconstruction
- Canonical asset aggregation

Author: John Swindell
"""

import os
import sys
import json
import time
import pandas as pd
import numpy as np
from tqdm import tqdm
from dotenv import load_dotenv
from pycoingecko import CoinGeckoAPI

# --- Configuration & Path Setup ---
load_dotenv()

# Infrastructure Config
GCP_PROJECT_ID = os.getenv('GCP_PROJECT_ID')
GCP_BUCKET_NAME = os.getenv('GCP_BUCKET_NAME')
CG_API_KEY = os.getenv("COINGECKO_PRO_API_KEY")
DL_API_KEY = os.getenv("DEFILLAMA_PRO_API_KEY")
LC_API_KEY = os.getenv("LUNARCRUSH_PRO_API_KEY")

# Directory Structure
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(os.path.dirname(SCRIPT_DIR))
DATA_DIR = os.path.join(PROJECT_ROOT, 'data')
CACHE_DIR = os.path.join(DATA_DIR, 'cache')
BRONZE_DIR = os.path.join(DATA_DIR, 'bronze')
OUTPUT_FILE = os.path.join(BRONZE_DIR, 'historical_data.parquet')

# Pipeline Parameters
START_DATE = '2022-01-01'
CANDIDATE_POOL_SIZE = 500
UNIVERSE_CACHE_FILE = os.path.join(CACHE_DIR, 'universe_definition.json')

# Import Shared Modules
sys.path.append(os.path.dirname(SCRIPT_DIR))
from pipeline_helpers import (
    GCSCachingManager,
    fetch_and_cache_coin_history,
    create_defillama_protocol_map,
    get_all_chains_map
)

def aggregate_canonical_data(group):
    """
    Aggregates data for a canonical asset group (e.g., merging Wrapped + Native).
    Prioritizes the native asset for pricing but sums volume across all versions.
    """
    # 1. Identify native asset
    canonical_id = group['canonical_id'].iloc[0]
    native_asset_row = group[group['coin_id'] == canonical_id]

    # 2. Set Base Data (Price/Market Cap)
    if not native_asset_row.empty:
        output = native_asset_row.iloc[0].copy()
    else:
        # Fallback to the largest asset in the group if native is missing
        output = group.sort_values('market_cap', ascending=False).iloc[0].copy()

    # 3. Aggregate Volume
    output['volume'] = group['volume'].sum()

    # 4. Backfill Gaps
    # If the native asset has missing OHLC data for this day, try to fill from wrappers
    critical_cols = ['open', 'high', 'low', 'close', 'market_cap']
    for col in critical_cols:
        if pd.isna(output[col]):
            valid_vals = group[col].dropna()
            if not valid_vals.empty:
                output[col] = valid_vals.iloc[0]

    return output

def main():
    print("--- Starting Historical Data ETL ---")

    # 1. Validation & Initialization
    if not all([CG_API_KEY, DL_API_KEY, LC_API_KEY]):
        sys.exit("Error: Missing required API keys in environment.")

    os.makedirs(BRONZE_DIR, exist_ok=True)
    os.makedirs(CACHE_DIR, exist_ok=True)

    cg = CoinGeckoAPI(api_key=CG_API_KEY)

    # Initialize Cloud Cache
    try:
        cacher = GCSCachingManager(project_id=GCP_PROJECT_ID, bucket_name=GCP_BUCKET_NAME)
    except Exception as e:
        sys.exit(f"Failed to initialize Cache Manager: {e}")

    # 2. Load Universe Definition
    # This file defines which assets were 'top assets' at specific historical points.
    print("Loading point-in-time universe definition...")
    try:
        with open(UNIVERSE_CACHE_FILE, 'r') as f:
            point_in_time_universe = json.load(f)
    except FileNotFoundError:
        sys.exit(f"Error: Universe definition not found at {UNIVERSE_CACHE_FILE}. Run universe generation first.")

    # 3. Load External Mappings
    print("Hydrating external data mappings...")
    # Fetch a candidate list just to build the ticker map
    candidate_markets = cg.get_coins_markets(vs_currency='usd', per_page=CANDIDATE_POOL_SIZE, page=1)
    ticker_map = {coin['id']: coin['symbol'].upper() for coin in candidate_markets}

    all_headers = {
        'llama': {'api-key': DL_API_KEY},
        'lunarcrush': {'Authorization': f'Bearer {LC_API_KEY}'}
    }

    llama_protocol_map = create_defillama_protocol_map(all_headers['llama'], cacher)
    llama_chain_map = get_all_chains_map(all_headers['llama'], cacher)

    # 4. Ingestion (Stage 1)
    # Fetch full history for every unique asset that appears in our universe at any point.
    all_unique_coins = sorted(list(set(c for coins in point_in_time_universe.values() for c in coins)))
    print(f"Ingesting history for {len(all_unique_coins)} unique assets...")

    full_history_cache = {}
    for coin_id in tqdm(all_unique_coins, desc="Processing Assets"):
        try:
            df = fetch_and_cache_coin_history(
                coin_id=coin_id,
                cg_client=cg,
                ticker_map=ticker_map,
                llama_protocol_map=llama_protocol_map,
                llama_chain_map=llama_chain_map,
                all_headers=all_headers,
                cacher=cacher
            )
            if not df.empty:
                full_history_cache[coin_id] = df
        except Exception as e:
            print(f"Warning: Failed to fetch {coin_id}: {e}")

        time.sleep(0.05) # Micro-sleep for stability

    # 5. Assembly (Stage 2)
    # Reconstruct the dataset as it looked historically (Point-in-Time).
    print("Assembling point-in-time slices...")
    all_period_data = []

    for period_str, coin_list in tqdm(point_in_time_universe.items(), desc="Building Slices"):
        period_date = pd.to_datetime(period_str)

        for coin_id in coin_list:
            if coin_id in full_history_cache:
                history = full_history_cache[coin_id]
                # Only include data known *up to* this period (prevents lookahead bias in some contexts,
                # though here we mainly filter to ensure we only include the asset during its relevant active periods)
                slice_data = history[history['date'] <= period_date].copy()
                all_period_data.append(slice_data)

    # 6. Transformation & Loading
    if all_period_data:
        print("Transforming and aggregating...")
        master_df = pd.concat(all_period_data, ignore_index=True)

        # Filter Logic
        master_df = master_df[master_df['date'] >= pd.to_datetime(START_DATE)].copy()

        # Canonicalization
        canonical_map = {
            'wrapped-bitcoin': 'bitcoin',
            'coinbase-wrapped-btc': 'bitcoin',
            'bitcoin-avalanche-bridged-btc-b': 'bitcoin',
            'wrapped-steth': 'staked-ether',
            'binance-peg-weth': 'weth'
        }
        master_df['canonical_id'] = master_df['coin_id'].map(canonical_map).fillna(master_df['coin_id'])

        # Aggregation
        final_df = (master_df.groupby(['canonical_id', 'date'])
                    .apply(aggregate_canonical_data, include_groups=False)
                    .reset_index())

        # Persistence
        final_df.to_parquet(OUTPUT_FILE)
        print(f"Success: ETL complete. Data saved to '{OUTPUT_FILE}'.")
        print(f"Stats: {len(final_df)} rows, {final_df['canonical_id'].nunique()} unique assets.")

    else:
        sys.exit("Error: No data produced.")

if __name__ == "__main__":
    main()