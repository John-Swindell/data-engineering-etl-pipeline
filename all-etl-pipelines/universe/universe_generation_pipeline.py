"""
Survivorship-Free Universe Generator

This script constructs a point-in-time historical universe of cryptocurrency assets.
It creates a survivorship-bias-free dataset by identifying the top N assets by
market capitalization for each month in a given historical period, rather than
relying solely on currently active assets.

The output is a JSON cache file used to drive realistic backtests.

Author: John Swindell
"""

import os
import sys
import json
import time
import math
import pandas as pd
from dotenv import load_dotenv
from pathlib import Path
from pycoingecko import CoinGeckoAPI

# --- Configuration & Constants ---
load_dotenv()

# Data Engineering Parameters
# adjusted for demonstration purposes
START_DATE = '2021-01-01'
TARGET_CANDIDATE_POOL = 1000  # Total assets to scan
FINAL_UNIVERSE_SIZE = 100     # Top N assets to select per month

# Infrastructure Config
CG_API_KEY = os.getenv("COINGECKO_PRO_API_KEY")
MAX_RETRIES = 3
RATE_LIMIT_WAIT = 65  # Seconds

# Path Handling
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent.parent # Adjust levels based on your actual repo structure
DATA_DIR = PROJECT_ROOT / 'data'
CACHE_DIR = DATA_DIR / 'cache'
OUTPUT_FILE = CACHE_DIR / 'universe_definition.json'

# --- API Client Setup ---
if not CG_API_KEY:
    print("Warning: COINGECKO_PRO_API_KEY not found in environment. Rate limits may apply.")
cg = CoinGeckoAPI(api_key=CG_API_KEY)


# --- Helper Functions ---

def fetch_market_chart(coin_id: str) -> dict | None:
    """
    Fetches full daily market chart data with robust error handling and
    automatic backoff for rate limits.
    """
    for attempt in range(MAX_RETRIES):
        try:
            return cg.get_coin_market_chart_by_id(
                id=coin_id,
                vs_currency='usd',
                days='max',
                interval='daily'
            )
        except Exception as e:
            error_msg = str(e).lower()
            if '429' in error_msg:
                print(f"[WARN] Rate limit hit for {coin_id}. Pausing for {RATE_LIMIT_WAIT}s...")
                time.sleep(RATE_LIMIT_WAIT)
            else:
                print(f"[INFO] Skipping {coin_id}: {e}")
                return None

    print(f"[ERR] Failed to fetch {coin_id} after {MAX_RETRIES} attempts.")
    return None


def normalize_market_data(raw_data: dict, coin_id: str) -> pd.DataFrame:
    """
    Parses raw API response into a normalized DataFrame.
    """
    if not raw_data or not raw_data.get('market_caps'):
        return pd.DataFrame()

    df = pd.DataFrame(raw_data['market_caps'], columns=['timestamp', 'market_cap'])
    df['date'] = pd.to_datetime(df['timestamp'], unit='ms').dt.normalize()
    df['coin_id'] = coin_id

    return df[['date', 'coin_id', 'market_cap']]


def main():
    print("--- Starting Universe Generation Pipeline ---")

    # 1. Fetch Candidate Pool
    # We fetch a wide net of assets (e.g., Top 1000) to ensure we capture
    # assets that were significant in the past but have since dropped in rank.
    print(f"Step 1: Fetching top {TARGET_CANDIDATE_POOL} candidates...")

    candidate_ids = []
    items_per_page = 250
    pages = math.ceil(TARGET_CANDIDATE_POOL / items_per_page)

    try:
        for page_num in range(1, pages + 1):
            print(f"Fetching page {page_num} of {pages}...")
            markets_page = cg.get_coins_markets(
                vs_currency='usd',
                order='market_cap_desc',
                per_page=items_per_page,
                page=page_num
            )
            candidate_ids.extend([coin['id'] for coin in markets_page])
            time.sleep(1.5) # Gentle rate limiting

        print(f"Successfully identified {len(candidate_ids)} candidate assets.")

    except Exception as e:
        sys.exit(f"Critical Error: Failed to fetch candidate pool. {e}")


    # 2. Acquire Historical Data
    print("Step 2: Hydrating historical market cap data...")
    all_histories = []

    for i, coin_id in enumerate(candidate_ids):
        # Progress logging
        if i % 10 == 0:
            print(f"Processing asset {i + 1}/{len(candidate_ids)}...")

        raw_data = fetch_market_chart(coin_id)
        if raw_data:
            df = normalize_market_data(raw_data, coin_id)
            if not df.empty:
                all_histories.append(df)

        time.sleep(0.5)

    if not all_histories:
        sys.exit("Critical Error: No historical data acquired.")


    # 3. Construct Point-in-Time Universe
    print("Step 3: calculating monthly top assets...")
    master_df = pd.concat(all_histories, ignore_index=True)

    # Filter for requested date range
    master_df = master_df[master_df['date'] >= pd.to_datetime(START_DATE)]
    master_df.dropna(subset=['market_cap'], inplace=True)

    # Resample to monthly granularity to determine rank
    master_df['month'] = master_df['date'].dt.to_period('M')

    # Calculate average market cap for the month to smooth volatility
    monthly_metrics = master_df.groupby(['month', 'coin_id'])['market_cap'].mean().reset_index()

    # Rank assets per month
    monthly_metrics['rank'] = monthly_metrics.groupby('month')['market_cap'].rank(
        method='first', ascending=False
    )

    # Select Top N
    universe_df = monthly_metrics[monthly_metrics['rank'] <= FINAL_UNIVERSE_SIZE]

    # Format output dictionary
    universe_map = {}
    for month, group in universe_df.groupby('month'):
        # Format key as YYYY-MM-01
        month_str = month.to_timestamp().strftime('%Y-%m-01')
        universe_map[month_str] = group['coin_id'].tolist()


    # 4. Persistence
    print(f"Step 4: Saving universe definition to {OUTPUT_FILE}...")
    try:
        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        with open(OUTPUT_FILE, 'w') as f:
            json.dump(universe_map, f, indent=4)
        print("Success: Universe generation complete.")

    except IOError as e:
        print(f"Error saving output file: {e}")

if __name__ == "__main__":
    main()