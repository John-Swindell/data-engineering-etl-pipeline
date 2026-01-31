"""
Multi-Source Cryptocurrency Data ETL Pipeline

This module orchestrates the acquisition, normalization, and persistence of
cryptocurrency market data. It aggregates disparate data points—historical
OHLCV, on-chain TVL, and social sentiment—from major aggregators (CoinGecko,
DeFiLlama, LunarCrush) into a unified analytical dataset.

Features:
- Two-tier caching architecture (Local Disk + Google Cloud Storage).
- Rate-limit aware fetching strategies.
- robust error handling and data normalization.

Author: John Swindell
"""

import os
import json
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional, Union, Any

import pandas as pd
import numpy as np
import requests
from google.cloud import storage
from pycoingecko import CoinGeckoAPI

# --- Configuration & Constants ---
GCP_PROJECT_ID = os.getenv('GCP_PROJECT_ID')
GCP_BUCKET_NAME = os.getenv('GCP_BUCKET_NAME')
CACHE_DIR = os.getenv('LOCAL_CACHE_DIR', 'cache')

class DataPersistenceLayer:
    """
    Manages a two-tier caching system (Local Disk + Cloud Storage)
    to minimize redundant API calls and accelerate pipeline execution.
    """

    def __init__(self, project_id: str, bucket_name: str, local_cache_dir: str = CACHE_DIR, gcs_client: Optional[storage.Client] = None):
        if not project_id or not bucket_name:
             # Graceful fallback for local-only testing if credentials aren't present
            print("Warning: GCS credentials missing. Running in local-only mode.")
            self.bucket = None
        else:
            try:
                self.client = storage.Client(project=project_id) if gcs_client is None else gcs_client
                self.bucket = self.client.bucket(bucket_name)
            except Exception as e:
                raise ConnectionError(f"Failed to initialize Cloud Storage client: {e}")

        self.local_cache_dir = local_cache_dir
        os.makedirs(self.local_cache_dir, exist_ok=True)

    def get(self, file_name: str) -> Optional[Union[pd.DataFrame, Dict, List]]:
        """Retrieves data from local cache if available, otherwise attempts to download from Cloud."""
        local_path = os.path.join(self.local_cache_dir, file_name)

        if os.path.exists(local_path):
            return self._load_from_local(local_path)

        if self.bucket:
            blob = self.bucket.blob(file_name)
            if blob.exists():
                local_dir = os.path.dirname(local_path)
                os.makedirs(local_dir, exist_ok=True)
                blob.download_to_filename(local_path)
                return self._load_from_local(local_path)

        return None

    def set(self, file_name: str, data: Union[pd.DataFrame, Dict, List]) -> None:
        """Persists data to both local disk and Cloud Storage."""
        local_path = os.path.join(self.local_cache_dir, file_name)

        try:
            local_dir = os.path.dirname(local_path)
            os.makedirs(local_dir, exist_ok=True)

            if isinstance(data, pd.DataFrame):
                data.to_parquet(local_path)
            else:
                with open(local_path, 'w') as f:
                    json.dump(data, f)

            if self.bucket:
                blob = self.bucket.blob(file_name)
                blob.upload_from_filename(local_path)

        except Exception as e:
            print(f"Warning: Cache upload failed for '{file_name}': {e}")

    def _load_from_local(self, local_path: str) -> Optional[Union[pd.DataFrame, Dict, List]]:
        try:
            if local_path.endswith('.parquet'):
                return pd.read_parquet(local_path)
            elif local_path.endswith('.json'):
                with open(local_path, 'r') as f:
                    return json.load(f)
        except Exception as e:
            print(f"Warning: Corrupt local cache file '{local_path}': {e}")
        return None


# --- Parsers & Normalizers ---

def _extract_dev_activity(dev_data: Dict) -> Dict:
    """Normalizes development activity metrics from source data."""
    if not dev_data:
        return {}
    return {
        'forks': dev_data.get('forks'),
        'stars': dev_data.get('stars'),
        'subscribers': dev_data.get('subscribers'),
        'total_issues': dev_data.get('total_issues'),
        'closed_issues': dev_data.get('closed_issues'),
        'pr_contributors': dev_data.get('pull_request_contributors'),
        'commit_count_4w': dev_data.get('commit_count_4_weeks')
    }

def _extract_market_metrics(mkt_data: Dict) -> Dict:
    """Extracts core market health and supply metrics."""
    if not mkt_data:
        return {}
    return {
        'tvl': mkt_data.get('total_value_locked'),
        'mcap_tvl_ratio': mkt_data.get('mcap_to_tvl_ratio'),
        'fdv_tvl_ratio': mkt_data.get('fdv_to_tvl_ratio'),
        'ath_change_pct': mkt_data.get('ath_change_percentage', {}).get('usd'),
        'circulating_supply': mkt_data.get('circulating_supply'),
        'price_change_7d': mkt_data.get('price_change_percentage_7d'),
        'price_change_30d': mkt_data.get('price_change_percentage_30d'),
        'price_change_1y': mkt_data.get('price_change_percentage_1y')
    }

def parse_asset_metadata(raw_json: Dict) -> pd.DataFrame:
    """
    Transforms raw JSON response into a structured DataFrame row.
    """
    flat_data = {
        'coin_id': raw_json.get('id'),
        'last_updated': pd.to_datetime(raw_json.get('last_updated')).normalize(),
        'rank': raw_json.get('market_cap_rank'),
        'genesis_date': pd.to_datetime(raw_json.get('genesis_date')).normalize() if raw_json.get('genesis_date') else None,
        'sentiment_up_pct': raw_json.get('sentiment_votes_up_percentage'),
        'categories': raw_json.get('categories', [])
    }

    flat_data.update(_extract_dev_activity(raw_json.get('developer_data', {})))
    flat_data.update(_extract_market_metrics(raw_json.get('market_data', {})))

    # Extract links if available
    links = raw_json.get('links', {})
    flat_data['homepage'] = links.get('homepage', [None])[0] if links.get('homepage') else None

    return pd.DataFrame([flat_data])


# --- Core Fetch Logic ---

def fetch_asset_metadata(coin_id: str, api_client: Any, persistence: DataPersistenceLayer) -> pd.DataFrame:
    """Retrieves detailed asset metadata, utilizing the persistence layer to minimize API overhead."""
    cache_key = f"metadata/{coin_id}.parquet"
    cached_df = persistence.get(cache_key)

    if cached_df is not None:
        return cached_df

    try:
        # Note: Assuming api_client is a CoinGeckoAPI instance or similar wrapper
        raw_data = api_client.get_coin_by_id(
            id=coin_id,
            localization=False,
            tickers=False,
            market_data=True,
            community_data=True,
            developer_data=True
        )
        if not raw_data:
            return pd.DataFrame()

        details_df = parse_asset_metadata(raw_data)

        if not details_df.empty:
            persistence.set(cache_key, details_df)

        return details_df

    except Exception as e:
        print(f"Error fetching metadata for '{coin_id}': {e}")
        return pd.DataFrame()


def fetch_historical_market_data(coin_id: str, api_client: Any, ticker_map: Dict) -> pd.DataFrame:
    """
    Fetches full historical OHLCV.
    Stitches together daily market chart with granular OHLC frames where available.
    """
    try:
        # Fetch base daily data (Prices, Market Caps, Volumes)
        market_chart_data = api_client.get_coin_market_chart_by_id(
            id=coin_id, vs_currency='usd', days='max', interval='daily'
        )

        if not market_chart_data or not market_chart_data.get('prices'):
            return pd.DataFrame()

        df_base = pd.DataFrame(market_chart_data['prices'], columns=['timestamp', 'close'])
        df_vol = pd.DataFrame(market_chart_data['total_volumes'], columns=['timestamp', 'volume'])
        df_mcap = pd.DataFrame(market_chart_data['market_caps'], columns=['timestamp', 'market_cap'])

        df = pd.merge(pd.merge(df_base, df_vol, on='timestamp', how='left'), df_mcap, on='timestamp', how='left')
        df['date'] = pd.to_datetime(df['timestamp'], unit='ms').dt.normalize()

    except Exception as e:
        print(f"Error fetching base market chart for '{coin_id}': {e}")
        return pd.DataFrame()

    # OHLC Backfilling Strategy
    # APIs often paginate or chunk deep history; this loop ensures complete coverage.
    try:
        all_ohlc_data = []
        from_timestamp = int(datetime(2018, 1, 1, tzinfo=timezone.utc).timestamp())
        to_timestamp_final = int(datetime.now().timestamp())
        current_from = from_timestamp

        while current_from < to_timestamp_final:
            # Fetch in ~6 month chunks to respect API limits
            current_to = min(current_from + (179 * 24 * 60 * 60), to_timestamp_final)

            ohlc_chunk = api_client.get_coin_ohlc_by_id_range(
                id=coin_id, vs_currency='usd', from_timestamp=str(current_from), to_timestamp=str(current_to),
                interval='daily'
            )
            if ohlc_chunk:
                all_ohlc_data.extend(ohlc_chunk)

            current_from = current_to + (24 * 60 * 60)
            time.sleep(1.0) # Rate limit courtesy

        if all_ohlc_data:
            df_ohlc = pd.DataFrame(all_ohlc_data, columns=['timestamp', 'open', 'high', 'low', 'close_ohlc'])
            df_ohlc['date'] = pd.to_datetime(df_ohlc['timestamp'], unit='ms').dt.normalize()
            df = pd.merge(df, df_ohlc[['date', 'open', 'high', 'low']], on='date', how='left')

    except Exception as e:
        print(f"Warning: OHLC backfill incomplete for '{coin_id}'. {e}")

    df['coin_id'] = coin_id
    df['ticker'] = df['coin_id'].map(ticker_map)
    final_cols = ['date', 'coin_id', 'ticker', 'open', 'high', 'low', 'close', 'volume', 'market_cap']

    return df.drop(columns=['timestamp'], errors='ignore')[[col for col in final_cols if col in df.columns]]


def fetch_onchain_metrics(slug: str, coin_id: str, ticker_map: Dict) -> pd.DataFrame:
    """
    Fetches Protocol TVL and DEX Volume from on-chain aggregators.
    Returns a merged DataFrame of TVL and Volume.
    """
    tvl_df, dex_df = pd.DataFrame(), pd.DataFrame()
    headers = {'User-Agent': 'Portfolio-Pipeline/1.0'}

    try:
        res_tvl = requests.get(f"https://api.llama.fi/protocol/{slug}", headers=headers)
        if res_tvl.ok:
            data = res_tvl.json()
            # Handle standard vs chain-specific TVL structures
            tvl_data = data.get('tvl', [])
            if tvl_data:
                tvl_df = pd.DataFrame(tvl_data)
                tvl_df.rename(columns={'totalLiquidityUSD': 'protocol_tvl'}, inplace=True)
                tvl_df['date'] = pd.to_datetime(pd.to_numeric(tvl_df['date'], errors='coerce'), unit='s').dt.normalize()
                tvl_df = tvl_df[['date', 'protocol_tvl']]
    except Exception as e:
        print(f"Warning: TVL fetch failed for {slug}: {e}")

    try:
        res_dex = requests.get(f"https://api.llama.fi/summary/dexs/{slug}", headers=headers)
        if res_dex.ok:
            chart = res_dex.json().get('totalDataChart')
            if chart:
                dex_df = pd.DataFrame(chart, columns=['date', 'dex_volume'])
                dex_df['date'] = pd.to_datetime(dex_df['date'], unit='s', errors='coerce').dt.normalize()
    except Exception as e:
        print(f"Warning: DEX volume fetch failed for {slug}: {e}")

    if tvl_df.empty and dex_df.empty:
        return pd.DataFrame()

    merged_df = pd.merge(tvl_df, dex_df, on='date', how='outer') if not tvl_df.empty and not dex_df.empty else (tvl_df if not tvl_df.empty else dex_df)

    merged_df['coin_id'] = coin_id
    return merged_df


def fetch_social_metrics(ticker: str, coin_id: str, api_key: str) -> pd.DataFrame:
    """Fetches historical social dominance and sentiment scores."""
    if not ticker or not api_key:
        return pd.DataFrame()

    try:
        headers = {'Authorization': f'Bearer {api_key}'}
        url = f"https://lunarcrush.com/api4/public/coins/{ticker}/time-series/v2?bucket=day&interval=3650d"
        res = requests.get(url, headers=headers)

        if not res.ok:
            return pd.DataFrame()

        time_series_data = res.json().get('data', [])
        if not time_series_data:
            return pd.DataFrame()

        df = pd.DataFrame(time_series_data)
        df.rename(columns={
            'time': 'date',
            'galaxy_score': 'social_score',
            'alt_rank': 'social_rank',
            'sentiment': 'sentiment_score'
        }, inplace=True)

        df['date'] = pd.to_datetime(df['date'], unit='s').dt.normalize()
        df['coin_id'] = coin_id

        required_cols = ['date', 'coin_id', 'social_score', 'social_rank', 'sentiment_score']
        return df[[col for col in required_cols if col in df.columns]]

    except Exception as e:
        print(f"Warning: Social metrics processing failed for {ticker}: {e}")
        return pd.DataFrame()


def run_pipeline(coin_id: str, ticker: str, api_keys: Dict, persistence: DataPersistenceLayer) -> pd.DataFrame:
    """
    Main Orchestrator:
    1. Check Cache
    2. Fetch Market Data (CoinGecko)
    3. Fetch On-Chain Data (DeFiLlama)
    4. Fetch Social Data (LunarCrush)
    5. Merge & Persist
    """
    # 0. Check Cache
    cache_key = f"consolidated_history/{coin_id}.parquet"
    if cached_df := persistence.get(cache_key):
         return cached_df # type: ignore

    # 1. Market Data
    cg_client = CoinGeckoAPI() # In production, inject this dependency
    ticker_map = {coin_id: ticker}
    market_df = fetch_historical_market_data(coin_id, cg_client, ticker_map)

    if market_df.empty:
        print(f"Critical: No market data found for {coin_id}. Aborting.")
        return pd.DataFrame()

    # 2. On-Chain Data (Simplified mapping logic for demo)
    # Note: In a full prod env, we would maintain a map of CoinGeckoID -> ProtocolSlugs
    onchain_df = fetch_onchain_metrics(coin_id, coin_id, ticker_map)

    # 3. Social Data
    social_df = fetch_social_metrics(ticker, coin_id, api_keys.get('lunarcrush'))

    # 4. Merge
    final_df = market_df
    if not onchain_df.empty:
        final_df = pd.merge(final_df, onchain_df, on=['date', 'coin_id'], how='left')

    if not social_df.empty:
        final_df = pd.merge(final_df, social_df, on=['date', 'coin_id'], how='left')

    # 5. Persist
    if not final_df.empty:
        persistence.set(cache_key, final_df)

    return final_df