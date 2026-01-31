# CoinGecko Asset Attributes Pipeline

## 1. Purpose
This pipeline acquires fundamental point-in-time data for every asset in the historical research universe. Unlike the time-series pipelines that fetch daily price history, this script captures a detailed snapshot of an asset's current health, development activity, and community engagement.

The output is a Bronze Layer dataset (`coingecko_asset_attributes_data.parquet`) containing one row per asset. This file is designed to be merged with historical price data to create a multi-factor dataset for advanced modeling.

## 2. Architecture
The script executes a sequential workflow to gather and process the data:

1.  **Load Universe**: The script reads `universe_cache.json` to identify the master list of coin IDs to process.
2.  **Fetch & Cache**: For each ID, it queries the CoinGecko `/coins/{id}` endpoint. It utilizes the shared `GCSCachingManager` to check for existing data in the cloud bucket before making a live API call, minimizing rate limit usage.
3.  **Parse Data**: Modular helper functions flatten the nested JSON response into a normalized DataFrame row.
4.  **Categorize**: It applies internal business logic to map raw CoinGecko tags into a standardized taxonomy (e.g., Infrastructure Chains, DeFi).
5.  **Persistence**: The consolidated data is saved locally as a Parquet file.

## 3. Data Dictionary
The pipeline extracts the following fundamental metrics:

### Market & Tokenomics
* **market_cap_rank**: The asset's rank by capitalization.
* **ath_change_percentage_usd**: The percentage drawdown from the all-time high.
* **total_supply**: The total amount of tokens currently in existence.
* **circulating_supply**: The amount of tokens circulating in the market.
* **mcap_to_tvl_ratio**: A valuation metric measuring capital efficiency relative to the on-chain economy.
* **genesis_date**: The project launch date.

### Developer Activity
* **forks / stars**: GitHub repository metrics indicating developer interest.
* **commit_count_4_weeks**: The number of code commits in the last month (proxy for development velocity).
* **pull_request_contributors**: The count of unique contributors.

### Community Engagement
* **reddit_subscribers**: Follower count on the official subreddit.
* **telegram_channel_user_count**: User count in the official Telegram channel.
* **sentiment_votes_up_percentage**: Community sentiment score.

## 4. Taxonomy and Categorization
A key feature of this pipeline is the translation of raw category tags into a clean, standardized taxonomy. This ensures consistent analysis across the ecosystem.

### Logic
The categorization logic is precedence-based. The system iterates through the `define_category_map` dictionary, and the first match determines the classification.

1.  **Override Check**: Checks for hardcoded exceptions (e.g., Bitcoin is always labeled *Blue Chips & Institutional*).
2.  **Keyword Search**: Iterates through the standard categories in the defined order.
3.  **First Match**: If a keyword (e.g., "layer-1") is found in the asset's raw tags, the category is assigned immediately, and the search stops.
4.  **Default**: If no matches are found, the asset is labeled *Uncategorized*.

This hierarchical design ensures that an asset with multiple tags (e.g., "smart-contract-platform" and "cosmos-ecosystem") is classified by its most fundamental property (in this case, *Infrastructure Chains*).

## 5. Usage
This script is designed to be run periodically (e.g., weekly) to refresh the fundamental data snapshot.

**Execution:**
```bash
python coingecko_asset_attributes_pipeline.py