"""
Data Quality Gate & Sanitation Pipeline

This script serves as a quality assurance barrier for incoming raw data.
It ingests raw Parquet files, validates schema conformity, standardizes
numerical precision, and filters logical inconsistencies (e.g., High < Close).

It implements a Circuit Breaker pattern: if the data quality is too poor
(exceeding the defined data loss threshold), the pipeline halts to prevent
downstream pollution.

Author: John Swindell
"""

import sys
import os
import argparse
import pandas as pd
import numpy as np

# --- Configuration ---
# Standard Financial Data Schema
REQUIRED_COLUMNS = {'canonical_id', 'date', 'open', 'high', 'low', 'close'}

# Standardization Rules
PRICE_DECIMALS = 16
OHLC_TOLERANCE = 1e-6

# Circuit Breaker Threshold (Halts pipeline if > 5% of data is invalid)
MAX_DATA_LOSS_PCT = 5.0

def validate_schema(df: pd.DataFrame) -> None:
    """Ensures the dataframe contains all required columns."""
    missing = REQUIRED_COLUMNS - set(df.columns)
    if missing:
        raise ValueError(f"Schema Mismatch. Missing columns: {sorted(list(missing))}")

def standardize_precision(df: pd.DataFrame) -> pd.DataFrame:
    """Standardizes floating point precision for price columns."""
    price_cols = ['open', 'high', 'low', 'close']
    for col in price_cols:
        if col in df.columns:
            df[col] = df[col].round(PRICE_DECIMALS)
    return df

def filter_ohlc_violations(df: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    """
    Removes rows where Close price is logically inconsistent with High/Low.
    Returns: (Cleaned DataFrame, Count of removed rows)
    """
    # Logic: Close cannot be higher than High or lower than Low (within tolerance)
    is_invalid = (
        (df['close'] < df['low'] - OHLC_TOLERANCE) |
        (df['close'] > df['high'] + OHLC_TOLERANCE)
    )

    invalid_count = is_invalid.sum()
    if invalid_count > 0:
        return df[~is_invalid].copy(), invalid_count

    return df, 0

def main(input_path: str, output_path: str, threshold: float = MAX_DATA_LOSS_PCT):
    print("--- Starting Data Quality Gate ---")

    # 1. Input Validation
    if not os.path.exists(input_path):
        sys.exit(f"Error: Input file not found at '{input_path}'")

    try:
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
    except OSError as e:
        sys.exit(f"Error: Could not create output directory. {e}")

    # 2. Ingestion
    print(f"Loading raw data: {input_path}")
    try:
        df = pd.read_parquet(input_path)
    except Exception as e:
        sys.exit(f"Error reading Parquet file: {e}")

    initial_count = len(df)
    print(f"Initial Rows: {initial_count}")

    try:
        # 3. Validation (Fail Strategy)
        print("Validating schema...")
        validate_schema(df)

        # 4. Standardization (Fix Strategy)
        print("Standardizing precision...")
        df = standardize_precision(df)

        # 5. Sanitation (Filter Strategy)
        print("Checking OHLC integrity...")
        df, removed_count = filter_ohlc_violations(df)

        if removed_count > 0:
            print(f" -> Removed {removed_count} rows with OHLC violations.")

        # 6. Circuit Breaker
        loss_pct = (removed_count / initial_count) * 100 if initial_count > 0 else 0
        print(f"\nData Quality Report:")
        print(f" - Rows Removed: {removed_count}")
        print(f" - Data Loss: {loss_pct:.2f}%")

        if loss_pct > threshold:
            print(f"\nCRITICAL: Data loss ({loss_pct:.2f}%) exceeds threshold ({threshold}%).")
            print("Aborting to protect downstream systems.")
            sys.exit(1)

        # 7. Persistence
        df.to_parquet(output_path, index=False)
        print(f"\nSuccess: Cleaned data saved to '{output_path}'")

    except Exception as e:
        sys.exit(f"Pipeline Failed: {e}")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Data Quality Gate")
    parser.add_argument("--input_file", required=True, help="Path to raw input file")
    parser.add_argument("--output_file", required=True, help="Path to save cleaned file")
    parser.add_argument("--threshold", type=float, default=MAX_DATA_LOSS_PCT, help="Max allowed data loss %")

    args = parser.parse_args()
    main(args.input_file, args.output_file, args.threshold)