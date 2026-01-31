"""
Production Live Data Pipeline

This script orchestrates the creation of the primary dataset for the user-facing
analytics dashboard. It fetches the CURRENT Top 200 cryptocurrencies by market cap
and aggregates their full historical market, on-chain, and social data.

It leverages a shared Data Lake architecture (GCS) to ensure efficient execution
and consistency with research datasets.

Author: John Swindell
"""

import os
import sys
import time
import pandas as pd
import numpy as np
from tqdm import tqdm
from dotenv import load_dotenv
from pycoingecko import CoinGeckoAPI

# --- Configuration & Paths ---
load_dotenv()

# Infrastructure Config
GCP_PROJECT_ID = os.getenv('GCP_PROJECT_ID')
GCP_BUCKET_NAME = os.getenv('GCP_BUCKET_NAME')
CG_API_KEY = os.getenv("COINGECKO_PRO_API_KEY")
DL_API_KEY = os.getenv("DEFILLAMA_PRO_API_KEY")
LC_API_KEY = os.getenv("LUNARCRUSH_PRO_API_KEY")

# Path Handling
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(os.path.dirname(SCRIPT_DIR))
DATA_DIR = os.path.join(PROJECT_ROOT, 'data')
CACHE_DIR = os.path.join(DATA_DIR, 'cache')
BRONZE_DIR = os.path.join(DATA_DIR, 'bronze')
OUTPUT_FILE = os.path.join(BRONZE_DIR, 'user_facing_data.parquet')

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
    Intelligently aggregates data for a canonical asset group (e.g., BTC + WBTC).
    Prioritizes the native asset for pricing/OHLC but sums volume across all versions.
    """
    # 1. Identify the 'native' asset row (where coin_id matches the canonical_id)
    canonical_id = group['canonical_id'].iloc[0]
    native_asset_row = group[group['coin_id'] == canonical_id]

    # 2. Establish the base row for price/market_cap data
    if not native_asset_row.empty:
        output = native_asset_row.iloc[0].copy()
    else:
        # Fallback to the largest market cap asset if native isn't present
        output = group.sort_values('market_cap', ascending=False).iloc[0].copy()

    # 3. Sum Volume across all versions
    output['volume'] = group['volume'].sum()

    # 4. Backfill missing data from other versions if the native row has gaps
    # (Common in early history or specific outage days)
    critical_cols = ['open', 'high', 'low', 'close', 'market_cap']
    for col in critical_cols:
        if pd.isna(output[col]):
            valid_values = group[col].dropna()
            if not valid_values.empty:
                output[col] = valid_values.iloc[0]

    return output

def main():
    print("--- Starting Production Dashboard Pipeline ---")

    # 1. Initialization
    if not all([CG_API_KEY, DL_API_KEY, LC_API_KEY]):
        sys.exit("Error: Missing API keys in environment variables.")

    try:
        cacher = GCSCachingManager(project_id=GCP_PROJECT_ID, bucket_name=GCP_BUCKET_NAME)
        cg = CoinGeckoAPI(api_key=CG_API_KEY)
        os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    except Exception as e:
        sys.exit(f"Initialization Failed: {e}")

    # Prepare Headers
    all_headers = {
        'llama': {'api-key': DL_API_KEY},
        'lunarcrush': {'Authorization': f'Bearer {LC_API_KEY}'}
    }

    # 2. Define Asset Universe
    print("Defining target universe (Top 200 by Market Cap)...")
    try:
        top_200_markets = cg.get_coins_markets(
            vs_currency='usd', order='market_cap_desc', per_page=200, page=1
        )
        universe_ids = [coin['id'] for coin in top_200_markets]
        ticker_map = {coin['id']: coin['symbol'].upper() for coin in top_200_markets}
        print(f"Targeting {len(universe_ids)} assets.")
    except Exception as e:
        sys.exit(f"Error fetching universe definition: {e}")

    # 3. Load Mappings
    print("Loading external data mappings...")
    llama_protocol_map = create_defillama_protocol_map(all_headers['llama'], cacher)
    llama_chain_map = get_all_chains_map(all_headers['llama'], cacher)

    # 4. Data Ingestion
    print(f"Ingesting historical data...")
    all_coin_data = []

    for coin_id in tqdm(universe_ids, desc="Processing Assets"):
        try:
            coin_df = fetch_and_cache_coin_history(
                coin_id=coin_id,
                cg_client=cg,
                ticker_map=ticker_map,
                llama_protocol_map=llama_protocol_map,
                llama_chain_map=llama_chain_map,
                all_headers=all_headers,
                cacher=cacher
            )
            if not coin_df.empty:
                all_coin_data.append(coin_df)
        except Exception as e:
            print(f"Warning: Failed to process {coin_id}: {e}")

        time.sleep(0.1)

    # 5. Aggregation & Persistence
    if all_coin_data:
        print("Consolidating dataset...")
        master_df = pd.concat(all_coin_data, ignore_index=True)

        # Canonicalization Map (Expand as needed)
        canonical_map = {
            'wrapped-bitcoin': 'bitcoin',
            'coinbase-wrapped-btc': 'bitcoin',
            'bitcoin-avalanche-bridged-btc-b': 'bitcoin',
            'wrapped-steth': 'staked-ether',
            'binance-peg-weth': 'weth'
        }
        master_df['canonical_id'] = master_df['coin_id'].map(canonical_map).fillna(master_df['coin_id'])

        print("Applying smart aggregation logic...")
        # Group by Canonical ID + Date -> Apply custom aggregation
        final_df = master_df.groupby(['canonical_id', 'date']).apply(aggregate_canonical_data).reset_index(drop=True)

        # Save
        final_df.to_parquet(OUTPUT_FILE)
        print(f"Success: Dashboard data saved to '{OUTPUT_FILE}'.")
        print(f"Stats: {len(final_df)} rows, {final_df['canonical_id'].nunique()} unique assets.")

    else:
        sys.exit("Error: Pipeline failed to produce data.")

if __name__ == '__main__':
    main()