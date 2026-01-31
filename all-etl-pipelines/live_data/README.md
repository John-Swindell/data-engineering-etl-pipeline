# Production Analytics Pipeline

## 1. Objective
This pipeline generates the primary dataset for the user-facing analytics dashboard. It is designed to provide comprehensive, up-to-date historical data for the **current Top 200 cryptocurrencies** by market capitalization.

Unlike the research backtesting pipelines (which control for survivorship bias), this pipeline focuses on relevance. It ensures that the dashboard always displays rich data for the assets that users are actively trading and analyzing today.

## 2. Methodology

The pipeline employs a high-efficiency workflow utilizing a generic Data Lake architecture:

1.  **Universe Definition (Live)**:
    * Queries the CoinGecko API for the current Top 200 assets. This dynamic definition ensures the dashboard automatically adapts to market shifts (e.g., new entrants entering the Top 200 are automatically picked up).

2.  **Multi-Source Ingestion**:
    * **Market Data**: OHLCV (Open, High, Low, Close, Volume) from CoinGecko.
    * **On-Chain Fundamentals**: TVL (Total Value Locked) and DEX Volume from DeFi Llama.
    * **Social Sentiment**: Galaxy Score and Social Dominance from LunarCrush.

3.  **Smart Caching**:
    * Utilizes a shared `GCSCachingManager`. Before fetching data, it checks the cloud bucket. If valid data for an asset exists (from a previous run or a different pipeline), it is reused. This reduces API costs and execution time by >90% on subsequent runs.

4.  **Canonical Aggregation**:
    * Resolves fragmented liquidity (e.g., merging `Wrapped Bitcoin` volume into `Bitcoin`).
    * Prioritizes native asset pricing while summing volume across all bridged versions to present a unified view of the asset's economy.

## 3. Setup

**Prerequisites:**
* Python 3.10+
* Google Cloud Credentials (if writing to remote bucket)
* API Keys (CoinGecko Pro, DeFi Llama, LunarCrush)

**Configuration:**
Create a `.env` file in the project root:
```bash
GCP_PROJECT_ID=...
GCP_BUCKET_NAME=...
COINGECKO_PRO_API_KEY=...
# ... other keys

```

## 4. Execution

To refresh the user facing dataset:

```bash
python live_data_pipeline.py

```

## 5. Data Schema

The output file `user_facing_data.parquet` contains the following consolidated metrics:

| Column | Type | Description |
| --- | --- | --- |
| `date` | datetime | UTC timestamp. |
| `canonical_id` | string | The unified asset identifier (e.g., 'bitcoin'). |
| `close` | float | Closing price (USD). |
| `volume` | float | Aggregated 24h trading volume (USD). |
| `market_cap` | float | Circulating Market Cap. |
| `chain_tvl` | float | Total Value Locked (if Layer 1/2). |
| `protocol_tvl` | float | Total Value Locked (if DeFi Protocol). |
| `dex_volume` | float | On-chain DEX volume. |
| `lc_galaxy_score` | float | Composite social/market health score. |
| `lc_sentiment` | float | Weighted social sentiment percentage. |

```
