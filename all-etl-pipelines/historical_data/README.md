# Historical Market Data ETL

## 1. Objective
This pipeline constructs a rigorous, point-in-time historical dataset for quantitative research and backtesting. 

Its primary goal is to provide a **single source of truth** for machine learning models, ensuring that strategies are evaluated on a consistent, high-fidelity data foundation. The pipeline is explicitly designed to combat **survivorship bias**—a common error where failed assets are excluded from history, leading to inflated performance results.

## 2. Key Features

* **Multi-Source Integration**: Aggregates data from three professional-grade sources:
    * **Market**: Price, Volume, Market Cap (CoinGecko)
    * **On-Chain**: Total Value Locked (TVL), DEX Volume (DeFi Llama)
    * **Social**: Sentiment, Social Dominance (LunarCrush)
* **Survivorship-Bias Free**: Implements a "historian" mode that reconstructs the asset universe as it existed historically (e.g., including assets that were Top 10 in 2021 but have since delisted), rather than just backfilling today's current winners.
* **Collaborative Caching**: Uses a shared Google Cloud Storage (GCS) layer to cache API responses. This prevents redundant fetching across the team, reducing API costs and execution time by orders of magnitude.
* **Canonical Aggregation**: Automatically detects and merges liquidity from bridged/wrapped versions of assets (e.g., WBTC + BTC) into a unified entity.

## 3. Architecture

The pipeline follows a standard cloud-native ETL pattern:

1.  **Extract**: 
    * Reads a `universe_definition.json` file to determine which assets were active in which historical periods.
    * Fetches raw data from APIs, checking the GCS Cache first.
2.  **Transform**: 
    * Filters data to match the specific "active" periods for each asset.
    * Cleanses anomalies and backfills missing OHLC data using secondary sources (e.g., filling native BTC gaps with Wrapped BTC data).
    * Aggregates fragmented liquidity into canonical IDs.
3.  **Load**: 
    * Writes the final dataset to a compressed, typed Parquet file in the Bronze data layer.

## 4. Configuration

**Environment Variables:**
The pipeline relies on a `.env` file for secure credential management:
```bash
GCP_PROJECT_ID=your-project-id
GCP_BUCKET_NAME=your-data-lake-bucket
COINGECKO_PRO_API_KEY=...
DEFILLAMA_PRO_API_KEY=...
LUNARCRUSH_PRO_API_KEY=...

```

## 5. Execution

**Run Full Pipeline:**

```bash
python historical_data_etl.py

```

**Run Unit Tests:**
To verify the caching logic without connecting to the live cloud bucket (offline mode):

```bash
python -m unittest test_caching_infrastructure.py

```

## 6. Data Schema

The output `historical_data.parquet` contains the following schema:

| Column | Type | Description |
| --- | --- | --- |
| `date` | datetime | UTC timestamp. |
| `canonical_id` | string | Unified asset identifier. |
| `close` | float | Closing price (USD). |
| `volume` | float | Aggregated Volume (Native + Wrappers). |
| `market_cap` | float | Circulating Market Cap. |
| `chain_tvl` | float | Total Value Locked (L1/L2). |
| `lc_sentiment` | float | Social sentiment score. |
| ... | ... | ... |

```