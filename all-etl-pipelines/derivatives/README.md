# Derivatives Market Snapshot ETL

## 1. Objective
This pipeline captures a daily "state-of-the-world" snapshot of the global cryptocurrency derivatives market. 

While the Historical Market Data pipeline focuses on spot prices, this tool aggregates data on Futures, Perpetuals, and Options. It is critical for analyzing:
* **Market Sentiment**: Via Funding Rates (bullish/bearish bias).
* **Leverage**: Via Open Interest (total capital at risk).
* **Liquidity**: Via 24-hour Contract Volume.

## 2. Methodology

The script runs daily (typically via cron or Airflow) and follows this workflow:

1.  **Exchange Discovery**: Queries the API to identify all currently supported derivatives exchanges (e.g., Binance Futures, Deribit, Bybit).
2.  **Ticker Extraction**: Iterates through every exchange to fetch data for every active contract.
3.  **Idempotency Check**: Before fetching, it checks the Data Lake (GCS) to see if a snapshot for the current date already exists. If it does, the script terminates early to prevent redundant API usage and data duplication.
4.  **Normalization**: Standardizes fields across different exchanges (e.g., standardizing how "Funding Rate" is reported).
5.  **Persistence**: Saves the aggregated dataset as a partitioned Parquet file in the cloud bucket.

## 3. Configuration

**Environment Variables:**
```bash
GCP_PROJECT_ID=your-project-id
GCP_BUCKET_NAME=your-data-lake-bucket
COINGECKO_PRO_API_KEY=...

```

## 4. Usage

**Manual Execution:**

```bash
python derivatives_snapshot_etl.py

```

**Output Location:**

* Cloud: `gs://[bucket-name]/derivatives/daily_snapshots/YYYY-MM-DD.parquet`
* Local: `data/cache/derivatives_YYYY-MM-DD.parquet`

## 5. Data Schema

The resulting dataset provides granular data at the contract level:

| Column | Type | Description |
| --- | --- | --- |
| `snapshot_date` | datetime | The date the snapshot was taken. |
| `exchange` | string | The venue (e.g., 'binance_futures'). |
| `symbol` | string | The ticker symbol (e.g., 'BTCUSDT'). |
| `contract_type` | string | E.g., 'perpetual', 'futures'. |
| `last_price` | float | Most recent trade price. |
| `funding_rate` | float | The periodic payment exchanged between longs/shorts. |
| `open_interest` | float | Total value of outstanding contracts (USD). |
| `volume_24h` | float | 24-hour trading volume (USD). |

```
