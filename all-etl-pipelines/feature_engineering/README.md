# Feature Engineering Pipeline

## 1. Overview
This pipeline transforms cleaned historical data (Silver Layer) into a rich feature matrix (Gold Layer) suitable for training machine learning models. 

It handles the complex task of generating two distinct types of features:
1.  **Time-Series Indicators:** Metrics derived from an asset's own history (e.g., RSI, Volatility).
2.  **Cross-Sectional Factors:** Metrics derived by comparing an asset to the broader market on a specific day (e.g., Market Dominance, Rank).

## 2. Architecture
The pipeline is optimized for performance using `pandas` vectorization and `Groupby-Apply` patterns to avoid slow iterative loops.

* **Input:** `data/silver/historical_data_clean.parquet` (Sanitized historical data)
* **Output:** `data/gold/features.parquet` (Model-ready feature set)

## 3. Key Features Generated

### Technical Indicators
Leverages `TA-Lib` to generate industry-standard signals efficiently.
* **Momentum:** RSI (14D), MACD (Signal/Hist), Bollinger Band Width.
* **Volatility:** Average True Range (ATR), Rolling Standard Deviation.
* **Volume:** Volume Z-Scores (to normalize volume spikes across assets with vastly different liquidities).

### Market Structure Factors
Generates features that require a "whole-market" view.
* **Dominance:** Asset market cap relative to the total daily universe.
* **Rankings:** Daily rank by Market Cap and Volume.

## 4. Usage

```bash
python feature_engineering_pipeline.py \
  --input_file "data/silver/historical_data_clean.parquet" \
  --output_file "data/gold/features.parquet"