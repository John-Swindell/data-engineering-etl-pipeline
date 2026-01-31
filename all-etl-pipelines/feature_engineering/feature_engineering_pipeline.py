"""
Feature Engineering Pipeline

This script transforms cleaned historical data into a set of
predictive features (Silver Layer) for downstream machine learning models.

It applies a suite of transformations including:
- Time-series momentum indicators (RSI, MACD, Bollinger Bands)
- Volatility estimators (ATR, Rolling Std Dev)
- Cross-sectional market factors (Momentum, Value, Size)

Author: John Swindell
"""

import sys
import os
import argparse
import pandas as pd

# --- Path Configuration ---
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(SCRIPT_DIR)

# Import helper module
from feature_calculation_functions import (
    create_return_features,
    create_momentum_features,
    create_volatility_features,
    create_volume_features,
    create_market_structure_factors
)


def main(input_path: str, output_path: str):
    print("--- Starting Feature Engineering Pipeline ---")

    # 1. Output Validation
    try:
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
    except OSError as e:
        sys.exit(f"Error creating output directory: {e}")

    # 2. Ingestion
    print(f"Loading cleaned dataset: {input_path}")
    try:
        df = pd.read_parquet(input_path)
        # Ensure distinct index for grouping
        df.reset_index(drop=True, inplace=True)
    except Exception as e:
        sys.exit(f"Failed to load input file: {e}")

    initial_shape = df.shape
    print(f"Input Shape: {initial_shape}")

    # 3. Feature Generation
    # We apply transformations sequentially. Each function returns a dataframe
    # with new columns appended.
    print("\n[1/3] Generating Time-Series Indicators...")
    df = create_return_features(df)
    df = create_momentum_features(df)  # RSI, MACD, Bollinger
    df = create_volatility_features(df)  # ATR, StdDev
    df = create_volume_features(df)  # OBV, Volume Z-Score

    print("\n[2/3] Generating Cross-Sectional Factors...")
    # These calculations rely on comparing assets against the daily universe
    df = create_market_structure_factors(df)

    # 4. Cleaning & Validation
    print("\n[3/3] Finalizing Dataset...")

    # Optional: Drop initial rows where rolling windows result in NaNs
    # strict_mode = False
    # if strict_mode:
    #     df.dropna(inplace=True)

    print(f"Final Shape: {df.shape}")
    print(f"Generated {df.shape[1] - initial_shape[1]} new features.")

    # 5. Persistence
    try:
        df.to_parquet(output_path, index=False)
        print(f"\nSuccess: Feature matrix saved to '{output_path}'")
    except Exception as e:
        sys.exit(f"Error saving output: {e}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Feature Engineering ETL")
    parser.add_argument("--input_file", required=True, help="Path to cleaned Silver parquet file")
    parser.add_argument("--output_file", required=True, help="Path to save Gold parquet file")

    args = parser.parse_args()
    main(args.input_file, args.output_file)