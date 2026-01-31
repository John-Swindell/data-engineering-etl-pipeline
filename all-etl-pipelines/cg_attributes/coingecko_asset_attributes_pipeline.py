"""
CoinGecko Asset Attributes Pipeline

This script acquires fundamental snapshot data for a defined universe of assets.
It fetches current market, developer, and community metrics from CoinGecko and
applies a standardized taxonomy to classify assets into internal business categories
(e.g., Infrastructure, DeFi, Consumer).

Author: John Swindell
"""

import os
import sys
import time
import json
import pandas as pd
from dotenv import load_dotenv
from pycoingecko import CoinGeckoAPI

# --- Configuration & Paths ---
load_dotenv()

# Project Paths
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(os.path.dirname(SCRIPT_DIR))
DATA_DIR = os.path.join(PROJECT_ROOT, 'data')
CACHE_DIR = os.path.join(DATA_DIR, 'cache')

# Infrastructure Config
GCP_PROJECT_ID = os.getenv('GCP_PROJECT_ID')
GCP_BUCKET_NAME = os.getenv('GCP_BUCKET_NAME')
CG_API_KEY = os.getenv("COINGECKO_PRO_API_KEY")

# Import Shared Modules
sys.path.append(os.path.dirname(SCRIPT_DIR))
from pipeline_helpers import GCSCachingManager, fetch_and_cache_coin_attributes

# --- Taxonomy Definitions ---

def define_category_map():
    """
    Defines the mapping from CoinGecko's raw category tags to standardized
    internal sectors. The dictionary order defines the precedence (top to bottom).
    """
    return {
        # Tier 1: Core Infrastructure
        'Infrastructure Chains': [
            'layer-1', 'layer-2', 'smart-contract-platform', 'rollup', 'sidechain',
            'zero-knowledge', 'data-availability', 'layer-0'
        ],
        'Platform Protocols': [
            'cosmos-ecosystem', 'polkadot-ecosystem', 'aptos-ecosystem', 'sui-ecosystem',
            'modular-blockchain', 'interoperability', 'cross-chain'
        ],
        'AI & Data Infrastructure': [
            'artificial-intelligence', 'storage', 'depin', 'oracle', 'analytics',
            'infrastructure', 'decentralized-identifier'
        ],

        # Tier 2: Financial Applications
        'DeFi & Financial Apps': [
            'decentralized-finance-defi', 'lending', 'dex', 'derivatives',
            'perpetuals', 'yield-farming', 'insurance', 'launchpad'
        ],
        'Stable Assets': [
            'stablecoins', 'real-world-assets', 'rwa', 'bridged-usdc', 'bridged-usdt',
            'tokenized-btc', 'wrapped-tokens', 'liquid-staking-tokens'
        ],

        # Tier 3: Consumer & Speculative
        'Consumer & Culture': [
            'gaming', 'nft', 'metaverse', 'social', 'payment-solutions', 'wallets', 'education'
        ],
        'Speculative Assets': [
            'meme', 'dog-themed', '4chan-themed', 'elon-musk-inspired'
        ],

        # Fallback
        'Blue Chips & Institutional': ['top-100', 'exchange-based-tokens']
    }

def assign_standard_category(row, category_map):
    """
    Applies taxonomy logic to a DataFrame row.
    Iterates through the map; the first matching keyword determines the category.
    """
    raw_categories = row.get('coingecko_categories', [])
    coin_id = row.get('coin_id')

    # Hardcoded Overrides
    if coin_id == 'bitcoin':
        return 'Blue Chips & Institutional'
    if coin_id == 'ethereum':
        return 'Infrastructure Chains'

    # Precedence-based Keyword Matching
    for standard_category, keywords in category_map.items():
        for keyword in keywords:
            # Check if keyword exists in any of the raw category strings
            if any(keyword in cat.lower() for cat in raw_categories):
                return standard_category

    return 'Uncategorized'

# --- Main Execution ---

def main():
    print("--- Starting CoinGecko attributes Pipeline ---")

    # 1. Setup
    cg = CoinGeckoAPI(api_key=CG_API_KEY)
    cacher = GCSCachingManager(project_id=GCP_PROJECT_ID, bucket_name=GCP_BUCKET_NAME)

    universe_cache_path = os.path.join(CACHE_DIR, 'universe_cache.json')
    output_file = os.path.join(CACHE_DIR, 'coingecko_attributes_data.parquet')

    # 2. Load Universe
    try:
        with open(universe_cache_path, 'r') as f:
            point_in_time_universe = json.load(f)
        # Flatten dictionary values into a unique list of coin IDs
        all_unique_coins = sorted(list(set(coin for coin_list in point_in_time_universe.values() for coin in coin_list)))
        print(f"Loaded {len(all_unique_coins)} unique assets from universe cache.")
    except FileNotFoundError:
        sys.exit(f"Error: Universe cache file not found at {universe_cache_path}.")

    # 3. Fetch Data
    all_attributes_data = []
    print("Fetching asset attributes...")

    for coin_id in all_unique_coins:
        try:
            coin_attributes_df = fetch_and_cache_coin_attributes(coin_id, cg, cacher)
            if not coin_attributes_df.empty:
                all_attributes_data.append(coin_attributes_df)
        except Exception as e:
            print(f"Warning: Failed to process {coin_id}: {e}")

        time.sleep(0.1) # Rate limit buffer

    # 4. Transform & Save
    if all_attributes_data:
        master_df = pd.concat(all_attributes_data, ignore_index=True)

        print("Applying standardized taxonomy...")
        category_map = define_category_map()
        master_df['standard_category'] = master_df.apply(
            lambda row: assign_standard_category(row, category_map), axis=1
        )

        master_df.to_parquet(output_file)

        print(f"Success: attributes data saved to '{output_file}'.")
        print("Category Distribution:")
        print(master_df['standard_category'].value_counts())

    else:
        sys.exit("Error: Pipeline failed to acquire data.")

if __name__ == "__main__":
    main()