# Historical Universe Generator

## Overview

This script solves the problem of **survivorship bias** in financial backtesting. 

Standard backtests often rely on the *current* top assets to test historical strategies. This methodology is flawed because it ignores assets that were market leaders in the past but have since failed or declined (e.g., Luna, FTX Token).

This tool reconstructs the true investment universe as it existed at each point in history. It identifies the top N assets by market capitalization for every month in the target period, creating a realistic, dynamic list of tradable assets for any given date.

## Methodology

1.  **Candidate Selection**: Fetches a broad pool of assets (default: Top 1000) to ensure coverage of fallen angels.
2.  **Data Hydration**: Retrieves full daily market capitalization history for every candidate.
3.  **Monthly Ranking**: 
    * Aggregates data into monthly periods.
    * Calculates the average market cap for each asset per month (smoothing out daily volatility).
    * Ranks assets against their peers for that specific month.
4.  **Universe Definition**: Selects the top N assets (default: 100) for each month and saves the mapping to disk.

## Configuration

Parameters can be adjusted in the `Configuration` section of the script:

* `START_DATE`: The beginning of the historical period (Default: `2021-01-01`).
* `TARGET_CANDIDATE_POOL`: The number of assets to scan from the API (Default: `1000`).
* `FINAL_UNIVERSE_SIZE`: The number of assets to include in the final monthly universe (Default: `100`).

## Usage

**Prerequisites:**
* Python 3.10+
* CoinGecko API Key (Pro plan recommended for large history requests).

**Environment Setup:**
Ensure your `.env` file contains your credentials:
```bash
COINGECKO_PRO_API_KEY=your_key_here

```

**Execution:**

```bash
python universe_generation_pipeline.py

```

## Output

The script generates a JSON file at `data/cache/universe_definition.json`.

**Structure:**

```json
{
    "2021-01-01": [
        "bitcoin",
        "ethereum",
        "tether",
        "... (Top 100 assets for Jan 2021)"
    ],
    "2021-02-01": [
        "bitcoin",
        "ethereum",
        "cardano",
        "... (Top 100 assets for Feb 2021)"
    ]
}
```
