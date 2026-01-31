# Coinbase Universe ETL Pipeline

## 1. Purpose

This pipeline automates the generation of a comprehensive financial dataset for all assets currently tradable on the Coinbase exchange. 

It is designed to support institutional-grade market analysis by filtering the broader crypto market down to the specific investable universe available on major US-regulated exchanges.

## 2. Architecture & Workflow

The pipeline follows a modern ETL (Extract, Transform, Load) pattern, leveraging a shared Bronze/Silver data lake structure:

1.  **Universe Discovery (Extract)**: 
    - Queries the live Coinbase Exchange API (`/products`) to identify all currently tradable tickers.
2.  **Identity Resolution (Transform)**: 
    - Maps exchange tickers (e.g., `BTC`) to standardized internal IDs (`bitcoin`) using CoinGecko as the source of truth.
3.  **Historical Acquisition (Extract)**: 
    - Iterates through the resolved universe and calls the shared `fetch_and_cache_coin_history` orchestrator.
    - Utilizes a **hybrid caching strategy** (Local + Google Cloud Storage) to minimize API costs and reduce runtime for subsequent backtests.
4.  **Canonicalization (Transform)**: 
    - Merges fragmented asset liquidity. For example, `Wrapped Bitcoin` (WBTC) and native `Bitcoin` are aggregated into a single `canonical_id` to provide an accurate view of total asset volume and market cap.
5.  **Persistence (Load)**: 
    - The cleaned, aggregated dataset is stored as a Parquet file in the `data/bronze` directory, ready for downstream modeling.

## 3. Key Features

### Asset Canonicalization
To ensure accurate liquidity modeling, the pipeline treats wrapped and bridged versions of an asset as a single entity.
* **Logic:** Groups data by `canonical_id` and `date`.
* **Aggregation:** * **Volume:** Summed across all versions (e.g., Total Vol = BTC Vol + WBTC Vol).
    * **Price:** Taken from the primary asset (using `last` logic).

### Shared Infrastructure
This script demonstrates modular engineering by importing shared utilities for:
* **Cloud Caching:** `GCSCachingManager` handles abstract read/writes to GCS buckets.
* **Rate Limiting:** Intelligent sleeping and retries to handle strict 3rd-party API limits.

## 4. Usage

**Environment Setup:**
Ensure your `.env` file is configured with the necessary API keys and GCP credentials:
```bash
GCP_PROJECT_ID=your-project-id
GCP_BUCKET_NAME=your-data-lake-bucket
COINGECKO_PRO_API_KEY=...
```

**Execution:**
```bash
python coinbase_universe_pipeline.py
```

**Output:**
* `data/bronze/coinbase_universe_data.parquet`: A multi-index DataFrame containing OHLCV, TVL, and social metrics for all Coinbase assets.