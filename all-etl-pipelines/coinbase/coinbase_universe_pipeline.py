"""
Coinbase Universe Data Pipeline

This script generates a comprehensive, historical dataset for all assets
currently tradable on Coinbase. It fetches the live asset list from the
Exchange API and orchestrates the retrieval of full historical market,
on-chain, and social data using a shared data lake architecture.

Author: John Swindell
"""

import os
import sys
import time
import requests
import pandas as pd
from dotenv import load_dotenv
from pycoingecko import CoinGeckoAPI
from tqdm import tqdm

# --- Configuration & Paths ---
load_dotenv()

GCP_PROJECT_ID = os.getenv('GCP_PROJECT_ID')
GCP_BUCKET_NAME = os.getenv('GCP_BUCKET_NAME')
COINBASE_API_URL = 'https://api.exchange.coinbase.com/products'

# Get absolute paths to ensure reliable execution in any environment
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(os.path.dirname(SCRIPT_DIR))

# Data Lake Directory Structure
DATA_DIR = os.path.join(PROJECT_ROOT, 'data')
BRONZE_DIR = os.path.join(DATA_DIR, 'bronze') # Raw / Ingest
OUTPUT_FILE = os.path.join(BRONZE_DIR, 'coinbase_universe_data.parquet')

# Import shared ETL modules
sys.path.append(os.path.dirname(SCRIPT_DIR))
from pipeline_helpers import (
    GCSCachingManager,
    fetch_and_cache_coin_history,
    create_defillama_protocol_map,
    get_all_chains_map
)

def main():
    """Main execution entry point."""

    # --- 1. Validation & Setup ---
    output_dir = os.path.dirname(OUTPUT_FILE)
    try:
        os.makedirs(output_dir, exist_ok=True)
    except OSError as e:
        sys.exit(f"FATAL: Could not create output directory '{output_dir}'. Error: {e}")

    print("--- Initializing Data Pipeline ---")

    # Load Credentials
    cg_api_key = os.getenv("COINGECKO_PRO_API_KEY")
    dl_api_key = os.getenv("DEFILLAMA_PRO_API_KEY")
    lc_api_key = os.getenv("LUNARCRUSH_PRO_API_KEY")

    # Initialize Clients
    cg = CoinGeckoAPI(api_key=cg_api_key)
    cacher = GCSCachingManager(
        project_id=GCP_PROJECT_ID,
        bucket_name=GCP_BUCKET_NAME
    )

    all_headers = {
        'llama': {'api-key': dl_api_key},
        'lunarcrush': {'Authorization': f'Bearer {lc_api_key}'}
    }

    # --- 2. Build Asset Universe ---
    print("\n--- Fetching Coinbase Asset Universe ---")
    try:
        response = requests.get(COINBASE_API_URL)
        response.raise_for_status()
        products = response.json()

        if not isinstance(products, list):
            raise ValueError("Unexpected API response format")

        # Extract unique base currencies (e.g., 'BTC', 'ETH')
        coinbase_tickers = {prod['base_currency'].upper() for prod in products}
        print(f"-> Found {len(coinbase_tickers)} tradable assets on Coinbase.")

        # Map Tickers to CoinGecko IDs (e.g., 'BTC' -> 'bitcoin')
        # This normalization is crucial for joining disparate datasets.
        all_coins_list = cg.get_coins_list()
        ticker_to_id_map = {coin['symbol'].upper(): coin['id'] for coin in all_coins_list}

        universe_ids = [ticker_to_id_map[t] for t in coinbase_tickers if t in ticker_to_id_map]
        print(f"-> Successfully mapped {len(universe_ids)} assets to canonical IDs.")

    except Exception as e:
        sys.exit(f"FATAL: Failed to build asset universe. Error: {e}")

    # --- 3. Pre-fetch Mappings ---
    # Invert map for reverse lookups
    id_to_ticker_map = {v: k for k, v in ticker_to_id_map.items()}

    # Fetch/Cache external mappings (DeFiLlama)
    llama_protocol_map = create_defillama_protocol_map(headers=all_headers['llama'], cacher=cacher)
    llama_chain_map = get_all_chains_map(headers=all_headers['llama'], cacher=cacher)

    # --- 4. Orchestrate Data Acquisition ---
    print(f"\n--- Ingesting History for {len(universe_ids)} Assets ---")
    all_coin_data = []

    for coin_id in tqdm(universe_ids, desc="Processing Assets"):
        try:
            coin_df = fetch_and_cache_coin_history(
                coin_id=coin_id,
                cg_client=cg,
                ticker_map=id_to_ticker_map,
                llama_protocol_map=llama_protocol_map,
                llama_chain_map=llama_chain_map,
                all_headers=all_headers,
                cacher=cacher
            )
            if not coin_df.empty:
                all_coin_data.append(coin_df)
        except Exception as e:
            print(f"Warning: Failed to fetch data for {coin_id}: {e}")

        # Respect API rate limits
        time.sleep(0.1)

    # --- 5. Aggregation & Persistence ---
    if all_coin_data:
        print("\n--- Consolidating Dataset ---")
        master_df = pd.concat(all_coin_data, ignore_index=True)

        # Canonicalization: Merge wrapped/bridged tokens into their underlying asset
        print("Canonicalizing asset IDs (e.g., WBTC -> BTC)...")
        canonical_map = {
            'wrapped-bitcoin': 'bitcoin',
            'coinbase-wrapped-btc': 'bitcoin',
            'bitcoin-avalanche-bridged-btc-b': 'bitcoin',
            'wrapped-steth': 'staked-ether',
            'binance-peg-weth': 'weth'
        }
        master_df['canonical_id'] = master_df['coin_id'].map(canonical_map).fillna(master_df['coin_id'])

        # Aggregation Logic: Sum volumes, take last price
        print("Applying aggregation rules...")
        aggregation_rules = {
            'volume': 'sum',
            'open': 'last', 'high': 'last', 'low': 'last', 'close': 'last',
            'market_cap': 'last', 'chain_tvl': 'last', 'protocol_tvl': 'last',
            'dex_volume': 'last', 'social_score': 'last', 'social_rank': 'last',
            'sentiment': 'last', 'ticker': 'last'
        }

        # Handle missing columns gracefully during aggregation
        valid_rules = {k: v for k, v in aggregation_rules.items() if k in master_df.columns}
        final_df = master_df.groupby(['canonical_id', 'date']).agg(valid_rules).reset_index()

        # Save to Data Lake (Bronze Layer)
        final_df.to_parquet(OUTPUT_FILE)
        print(f"\nSUCCESS: Pipeline complete. Data saved to '{OUTPUT_FILE}'.")
        print(f"Rows: {len(final_df)}")
        print(f"Unique Assets: {final_df['canonical_id'].nunique()}")
    else:
        sys.exit("\nFATAL: Pipeline failed. No data acquired.")

if __name__ == '__main__':
    main()