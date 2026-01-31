"""
Feature Calculation Library

Contains modular functions for generating financial features.
Optimized for pandas vectorization and GroupBy operations.

Author: John Swindell
"""

import pandas as pd
import numpy as np
import talib

# --- Utility Functions ---

def rolling_zscore(series: pd.Series, window: int = 30) -> pd.Series:
    """Calculates rolling z-score with handling for division by zero."""
    mean = series.rolling(window).mean()
    std = series.rolling(window).std()
    z_score = (series - mean) / std
    return z_score.replace([np.inf, -np.inf], np.nan)

# --- Time Series Indicators ---

def create_return_features(df: pd.DataFrame) -> pd.DataFrame:
    """Generates rolling returns and z-scores."""
    df = df.copy()

    # Calculate returns for various lookback windows
    windows = [1, 3, 7, 14, 30]
    for w in windows:
        col_name = f'ret_{w}d'
        # GroupBy is essential here to prevent data leakage across different assets
        df[col_name] = df.groupby('canonical_id')['close'].pct_change(w)

        # Add Z-Score normalization for the return signal
        if w in [3, 7]:
            df[f'{col_name}_z'] = df.groupby('canonical_id')[col_name].transform(
                lambda x: rolling_zscore(x, window=30)
            )

    return df

def create_momentum_features(df: pd.DataFrame) -> pd.DataFrame:
    """Generates standard momentum indicators (RSI, MACD, Bollinger Bands)."""
    df = df.copy()

    # 1. RSI (Relative Strength Index)
    df['rsi_14'] = df.groupby('canonical_id')['close'].transform(
        lambda x: talib.RSI(x, timeperiod=14)
    )

    # 2. MACD (Moving Average Convergence Divergence)
    def _calc_macd(g):
        macd, signal, hist = talib.MACD(g['close'])
        return pd.DataFrame({'macd': macd, 'macd_sig': signal, 'macd_hist': hist}, index=g.index)

    # Apply complex multi-column output function
    macd_df = df.groupby('canonical_id').apply(_calc_macd, include_groups=False).reset_index(level=0, drop=True)
    df = pd.concat([df, macd_df], axis=1)

    # 3. Bollinger Bands
    def _calc_bbands(g):
        upper, middle, lower = talib.BBANDS(g['close'], timeperiod=20)
        # Normalized bandwidth feature
        width = (upper - lower) / middle
        return pd.DataFrame({'bb_upper': upper, 'bb_lower': lower, 'bb_width': width}, index=g.index)

    bb_df = df.groupby('canonical_id').apply(_calc_bbands, include_groups=False).reset_index(level=0, drop=True)
    df = pd.concat([df, bb_df], axis=1)

    return df

def create_volatility_features(df: pd.DataFrame) -> pd.DataFrame:
    """Generates ATR and Rolling Volatility metrics."""
    df = df.copy()

    # Rolling Standard Deviation of Returns (Volatility)
    if 'ret_1d' not in df.columns:
        df['ret_1d'] = df.groupby('canonical_id')['close'].pct_change()

    df['volatility_30d'] = df.groupby('canonical_id')['ret_1d'].transform(
        lambda x: x.rolling(30).std()
    )

    # Average True Range (ATR)
    def _calc_atr(g):
        return talib.ATR(g['high'], g['low'], g['close'], timeperiod=14)

    df['atr_14'] = df.groupby('canonical_id').apply(
        lambda g: _calc_atr(g), include_groups=False
    ).reset_index(level=0, drop=True)

    return df

def create_volume_features(df: pd.DataFrame) -> pd.DataFrame:
    """Generates volume-based indicators."""
    df = df.copy()

    # Volume Z-Score (detects unusual volume spikes)
    df['vol_z30'] = df.groupby('canonical_id')['volume'].transform(
        lambda x: rolling_zscore(x, window=30)
    )

    return df

# --- Cross-Sectional Factors ---

def create_market_structure_factors(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculates cross-sectional factors based on daily universe comparisons.

    Note: Implementation details for Alpha Factors (HML, SMB, Momentum Rank)
    have been redacted for this public portfolio version.
    """
    df = df.copy()

    # Example placeholder for Market Cap Rank
    # Real implementation would calculate Fama-French style factors here.
    df['mcap_rank_daily'] = df.groupby('date')['market_cap'].rank(ascending=False)

    # Example placeholder for Universe Dominance
    daily_total_cap = df.groupby('date')['market_cap'].transform('sum')
    df['dominance_pct'] = df['market_cap'] / daily_total_cap

    return df