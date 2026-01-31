"""
Data Integrity & Validation Test Suite

This module contains a comprehensive suite of unit tests designed to validate
financial time-series data. It specifically targets common quantitative
errors including lookahead bias, backfill bias, repainting, and structural
breaks in precision.

These tests are intended to be run via pytest as part of the CI/CD pipeline
before data is promoted to the Silver/Gold layers.

Author: John Swindell
"""

import os
import pytest
import pandas as pd
import numpy as np
from scipy.stats import ttest_ind

# =============================================================================
# Configuration & Constants
# =============================================================================

# Paths - Assuming standard project structure relative to test execution
DATA_DIR = os.path.join("data", "bronze")
CLEAN_DIR = os.path.join("data", "silver")

# File Artifacts
CURRENT_FILE = os.path.join(DATA_DIR, "historical_data.parquet")
PREVIOUS_FILE = os.path.join(DATA_DIR, "historical_data_prev.parquet") # Snapshot from previous run
SILVER_FILE = os.path.join(CLEAN_DIR, "historical_data_clean.parquet")

# Column Definitions
ASSET_ID = 'canonical_id'
DATE_COL = 'date'
TARGET_COL = 'future_return_1d'

# Schema Definitions
SCHEMA_BRONZE = {'canonical_id', 'date', 'open', 'high', 'low', 'close'}
SCHEMA_SILVER = SCHEMA_BRONZE.union({'returns_1d', 'market_cap'})

# Statistical Thresholds
P_VALUE_THRESHOLD = 0.001
MIN_SAMPLES = 50
SHARPE_THRESHOLD = 1.0
PREDICTION_HORIZON = 1
FLOAT_TOLERANCE = 1e-9

# =============================================================================
# Helper Logic
# =============================================================================

def identify_backfill_bias(df_old: pd.DataFrame, df_new: pd.DataFrame) -> pd.DataFrame:
    """
    Detects assets where historical data was retroactively added (backfilled).
    Compares the earliest available date for each asset between two dataset versions.
    """
    start_old = df_old.groupby(ASSET_ID)[DATE_COL].min()
    start_new = df_new.groupby(ASSET_ID)[DATE_COL].min()

    comparison = pd.DataFrame({
        'start_date_old': start_old,
        'start_date_new': start_new
    }).dropna()

    # Bias exists if the new start date is older than the previous start date
    return comparison[comparison['start_date_new'] < comparison['start_date_old']]

def detect_ohlc_violations(df: pd.DataFrame, tolerance: float = 1e-6) -> pd.DataFrame:
    """
    Identifies rows where Close price is logically inconsistent (outside High/Low range).
    """
    invalid_mask = (df['close'] < df['low'] - tolerance) | (df['close'] > df['high'] + tolerance)
    return df[invalid_mask]

def detect_precision_break(series: pd.Series) -> tuple[float, float]:
    """
    Uses a T-Test to detect structural breaks in floating point precision
    (e.g., an exchange switching from 2 to 8 decimal places).
    """
    if len(series) < MIN_SAMPLES:
        return 0.0, 1.0

    # extract length of decimal component
    decimals = series.astype(str).str.split('.').str[1].str.len().fillna(0)

    mid = len(decimals) // 2
    pre, post = decimals.iloc[:mid], decimals.iloc[mid:]

    if pre.empty or post.empty:
        return 0.0, 1.0

    return ttest_ind(pre, post, equal_var=False)

def calculate_hml_pit(df: pd.DataFrame) -> pd.Series:
    """
    Recalculates a High-Minus-Low (HML) value factor using strictly Point-in-Time
    data logic (lagging fundamentals by 1 day to account for publication delay).
    """
    df_pit = df.copy()

    # Enforce 1-day lag on fundamentals
    df_pit['mcap_lag'] = df_pit.groupby(ASSET_ID)['market_cap'].shift(1)
    df_pit['tvl_lag'] = df_pit.groupby(ASSET_ID)['protocol_tvl'].shift(1)

    df_pit.dropna(subset=['mcap_lag', 'tvl_lag', 'returns_1d'], inplace=True)

    if df_pit.empty:
        return pd.Series(dtype=float)

    # Calculate Value Score
    df_pit['value_score'] = df_pit['mcap_lag'] / df_pit['tvl_lag']
    df_pit.replace([np.inf, -np.inf], np.nan, inplace=True)

    # Calculate daily HML return
    def get_hml(g):
        if len(g) < 10: return np.nan
        longs = g.nlargest(int(len(g) * 0.3), 'value_score')['returns_1d'].mean()
        shorts = g.nsmallest(int(len(g) * 0.3), 'value_score')['returns_1d'].mean()
        return longs - shorts

    return df_pit.groupby(DATE_COL).apply(get_hml)

def detect_repainting(df_old: pd.DataFrame, df_new: pd.DataFrame) -> pd.DataFrame:
    """
    Compares two dataset versions to identify historical values that changed
    (repainting), violating immutability.
    """
    # Align Indexes
    df_old = df_old.set_index([ASSET_ID, DATE_COL]).sort_index()
    df_new = df_new.set_index([ASSET_ID, DATE_COL]).sort_index()

    common_idx = df_old.index.intersection(df_new.index)
    if common_idx.empty:
        return pd.DataFrame()

    cols = df_old.columns.intersection(df_new.columns)

    # Calculate Deltas
    diff = (df_old.loc[common_idx, cols] - df_new.loc[common_idx, cols]).abs()
    mask = diff > FLOAT_TOLERANCE

    if not mask.any().any():
        return pd.DataFrame()

    # Format Report
    deltas = mask.stack()
    deltas = deltas[deltas] # Filter True only

    report = []
    for (asset, date), col in deltas.index:
        report.append({
            'asset': asset,
            'date': date,
            'feature': col,
            'old': df_old.loc[(asset, date), col],
            'new': df_new.loc[(asset, date), col]
        })

    return pd.DataFrame(report)

# =============================================================================
# Test Definitions
# =============================================================================

@pytest.mark.data_validation
def test_backfill_bias():
    """
    Verifies that no historical data was retroactively added for existing assets,
    which would simulate data availability that did not exist in real-time.
    """
    try:
        df_old = pd.read_parquet(PREVIOUS_FILE)
        df_new = pd.read_parquet(CURRENT_FILE)
    except FileNotFoundError:
        pytest.skip("Previous dataset snapshot not available for comparison.")

    violations = identify_backfill_bias(df_old, df_new)

    assert violations.empty, (
        f"Backfill Bias Detected: {len(violations)} assets have earlier start dates "
        f"in the new dataset than in the historical record."
    )

@pytest.mark.data_validation
def test_price_integrity_and_leakage():
    """
    Verifies that High/Low prices bound the Close price.
    Violations often indicate 'Adjusted Close' logic was applied historically,
    which destroys the ability to backtest execution logic correctly.
    """
    try:
        df = pd.read_parquet(CURRENT_FILE)
    except FileNotFoundError:
        pytest.fail("Current data file missing.")

    violations = detect_ohlc_violations(df)

    assert violations.empty, (
        f"Integrity Violation: Found {len(violations)} rows where Close is outside [High, Low]. "
        "This suggests adjusted pricing logic is corrupting raw market data."
    )

@pytest.mark.data_validation
def test_precision_stability():
    """
    Checks for structural breaks in data precision (e.g. 2 decimals -> 8 decimals).
    Drastic precision changes can introduce noise in volatility calculations.
    """
    try:
        df = pd.read_parquet(CURRENT_FILE)
    except FileNotFoundError:
        pytest.fail("Current data file missing.")

    failures = []
    for asset in df[ASSET_ID].unique():
        prices = df.loc[df[ASSET_ID] == asset, 'close'].sort_values()
        _, p_val = detect_precision_break(prices)

        if p_val < P_VALUE_THRESHOLD:
            failures.append(asset)

    assert not failures, (
        f"Precision Instability: {len(failures)} assets show statistically significant "
        "changes in decimal precision over time."
    )

@pytest.mark.data_validation
def test_predictability_of_errors():
    """
    'Trade the Bug' Test: Ensures that data errors (OHLC violations) are random.
    If errors are predictive of future returns, the model will learn to exploit
    the data bug rather than market dynamics.
    """
    try:
        df = pd.read_parquet(CURRENT_FILE).sort_values(by=[ASSET_ID, DATE_COL])
    except FileNotFoundError:
        pytest.fail("Current data file missing.")

    # Generate Returns
    df[TARGET_COL] = df.groupby(ASSET_ID)['close'].pct_change(PREDICTION_HORIZON).shift(-PREDICTION_HORIZON)

    # Isolate Error Population
    bad_idx = detect_ohlc_violations(df).index
    df_bad = df[df.index.isin(bad_idx)].dropna(subset=[TARGET_COL])
    df_good = df[~df.index.isin(bad_idx)].dropna(subset=[TARGET_COL])

    if df_bad.empty:
        pytest.skip("No data errors found to test.")

    # Statistical Significance Test
    _, p_val = ttest_ind(df_bad[TARGET_COL], df_good[TARGET_COL], equal_var=False)

    # Economic Significance Test (Sharpe Ratio)
    daily_ret = df_bad.groupby(DATE_COL)[TARGET_COL].mean()
    sharpe = (daily_ret.mean() / daily_ret.std()) * np.sqrt(365) if daily_ret.std() > 0 else 0

    is_leakage = (p_val < P_VALUE_THRESHOLD) and (sharpe > SHARPE_THRESHOLD)

    assert not is_leakage, (
        f"Predictive Error Leakage: Trading on data errors yields Sharpe {sharpe:.2f}. "
        "The model is likely overfitting to data quality issues."
    )

@pytest.mark.data_validation
def test_publication_lag_compliance():
    """
    Verifies that fundamental factors respect publication delays.
    Compares the HML factor in the dataset against a strictly recalculated
    Point-in-Time version.
    """
    try:
        df = pd.read_parquet(SILVER_FILE).sort_values(by=[ASSET_ID, DATE_COL])
    except FileNotFoundError:
        pytest.skip("Silver data not generated yet.")

    required = ['hml_factor', 'market_cap', 'protocol_tvl', 'returns_1d']
    if not all(col in df.columns for col in required):
        pytest.skip("Silver dataset missing columns required for HML test.")

    # Re-derive factor with strict lags
    hml_pit = calculate_hml_pit(df)

    if hml_pit.empty:
        pytest.skip("Insufficient data for HML recalculation.")

    # Compare
    merged = df.merge(hml_pit.rename('hml_pit'), on=DATE_COL, how='inner')
    diff = (merged['hml_factor'] - merged['hml_pit']).abs()

    violations = diff > FLOAT_TOLERANCE

    assert not violations.any(), (
        f"Lookahead Bias Detected: {violations.sum()} days show discrepancies between "
        "stored HML factor and Point-in-Time recalculated factor."
    )

@pytest.mark.data_validation
def test_historical_immutability():
    """
    Ensures historical data values have not changed since the last run (Repainting).
    """
    try:
        df_old = pd.read_parquet(PREVIOUS_FILE)
        df_new = pd.read_parquet(CURRENT_FILE)
    except FileNotFoundError:
        pytest.skip("Previous snapshot missing for immutability test.")

    repainting = detect_repainting(df_old, df_new)

    assert repainting.empty, (
        f"Immutability Violation: {len(repainting)} data points have changed value "
        "retroactively."
    )

@pytest.mark.data_validation
def test_schema_validity():
    """
    Validates that Bronze/Silver artifacts conform to expected schema.
    """
    for fname, schema in [(CURRENT_FILE, SCHEMA_BRONZE), (SILVER_FILE, SCHEMA_SILVER)]:
        if not os.path.exists(fname):
            continue

        df = pd.read_parquet(fname)
        missing = schema - set(df.columns)

        assert not missing, f"Schema Violation in {fname}: Missing {missing}"