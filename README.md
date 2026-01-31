# Cryptocurrency Market Data Engineering Platform

## Overview

This repository houses the foundational data engineering architecture for a quantitative market analysis platform. It consists of a suite of automated ETL (Extract, Transform, Load) pipelines designed to ingest, validate, and process cryptocurrency market data from disparate sources.

The system was originally architected to serve as the backend for an internal enterprise analytics dashboard, providing staff with the ability to filter, visualize, and run calculations on high-fidelity market data. This repository focuses on the data acquisition, quality assurance, and feature engineering layers that powered those downstream applications.

## Architecture

The platform follows a modern "Lakehouse" pattern with strict data quality gates:

### 1. Bronze Layer (Raw Ingestion)
Automated pipelines fetch raw data from external APIs. The system handles rate limiting, pagination, and multi-source join logic (e.g., mapping on-chain TVL to off-chain price data).

* **Historical ETL:** Reconstructs survivorship-bias-free market history for backtesting.
* **Live Data ETL:** Fetches real-time snapshots for production monitoring.
* **Derivatives ETL:** Captures daily open interest and funding rates.
* **Exchange Universe:** Maps assets specific to regulated exchanges (e.g., Coinbase).

### 2. Quality Gate (Validation)
Between the Bronze and Silver layers, a strict **Data Quality Gate** acts as a circuit breaker. It enforces schema validation, standardizes numerical precision, and filters logical inconsistencies (e.g., OHLC integrity violations) before data is allowed downstream.

### 3. Silver Layer (Feature Engineering)
Cleaned data is enriched with derived features. This includes calculating time-series momentum indicators, volatility metrics, and cross-sectional market structure factors.

## Pipeline Inventory

| Pipeline | Layer | Script Name | Description |
| :--- | :--- | :--- | :--- |
| **Universe Generation** | Foundation | `universe_generation_pipeline.py` | Defines the point-in-time universe of assets to eliminate survivorship bias. |
| **Historical Data** | Bronze | `historical_data_etl.py` | Ingests full OHLCV, Social, and On-Chain history for research backtesting. |
| **Live Data** | Bronze | `live_data_pipeline.py` | Fetches the current state of the Top 200 assets for real-time analytics. |
| **Derivatives** | Bronze | `derivatives_snapshot_etl.py` | Captures daily snapshots of the global futures and options market. |
| **Coinbase Universe** | Bronze | `coinbase_universe_pipeline.py` | Specialized ingestion for assets listed on Coinbase. |
| **Quality Gate** | QA | `data_quality_gate.py` | Validates schema and integrity; halts pipeline if data loss exceeds thresholds. |
| **Feature Engineering** | Silver | `feature_engineering_pipeline.py` | Generates technical indicators and market factors (Gold Layer ready). |

## Getting Started

### Prerequisites

* **Docker** (Recommended for orchestration)
* **Python 3.11+** (For local development)
* **API Keys**: CoinGecko Pro, DeFiLlama, LunarCrush

### Installation

1.  **Clone the repository**
    ```bash
    git clone [https://github.com/your-username/crypto-data-engineering.git](https://github.com/your-username/crypto-data-engineering.git)
    cd crypto-data-engineering
    ```

2.  **Configure Environment**
    Create a `.env` file in the root directory:
    ```bash
    COINGECKO_PRO_API_KEY=your_key
    DEFILLAMA_PRO_API_KEY=your_key
    LUNARCRUSH_PRO_API_KEY=your_key
    GCP_PROJECT_ID=your_gcp_project
    GCP_BUCKET_NAME=your_gcs_bucket
    ```

### Execution

#### Option A: Docker (Production-Like Orchestration)
The entire ETL sequence is containerized. This ensures reproducible execution across environments.

```bash
# Build the container
docker build -t crypto-etl .

# Run the full pipeline sequence
docker run --env-file .env crypto-etl

```

#### Option B: Local Execution

You can run individual pipelines manually for development or debugging.

```bash
# Install dependencies
pip install -r requirements.txt

# Run a specific pipeline (e.g., Live Data)
python pipelines/live_data/live_data_pipeline.py

```

## Data Sources

The platform aggregates data from three primary professional-grade sources:

* **CoinGecko**: Core market data (Price, Volume, Market Cap).
* **DeFiLlama**: On-chain fundamentals (Total Value Locked, DEX Volume).
* **LunarCrush**: Social sentiment and community engagement metrics.

## Key Features

* **Survivorship Bias Mitigation**: The "Historical" pipeline reconstructs the asset universe as it existed in the past, rather than backfilling current winners.
* **Point-in-Time Correctness**: Feature engineering respects publication lags to prevent lookahead bias in modeling.
* **Hybrid Caching**: Implements a custom `GCSCachingManager` that utilizes both local disk and Google Cloud Storage to minimize API costs and latency.
* **Fail-Fast Architecture**: Pipelines are designed to exit immediately upon critical data quality failures to prevent downstream pollution.

```