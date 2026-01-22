"""
Technical indicator calculation service.
Pure feature engineering logic separated from data management.
"""
import logging

import pandas as pd

logger = logging.getLogger("TechnicalIndicatorCalculator")


class TechnicalIndicatorCalculator:
    """
    Calculates technical indicators for stock price prediction.
    All methods are stateless - they take a DataFrame and return enhanced DataFrame.
    """

    def __init__(self):
        """Initialize the technical indicator calculator."""
        logger.info("TechnicalIndicatorCalculator initialized")

    def calculate_all(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate all technical indicators for the given DataFrame.

        Parameters:
        df (pd.DataFrame): Raw stock data with columns: timestamp, symbol, Open, High, Low, Close, Volume

        Returns:
        pd.DataFrame: DataFrame with 38 engineered features
        """
        if len(df) < 2:
            return df

        # Create a copy to avoid modifying original
        df = df.copy()

        # Create new dataframe for features
        df_new = pd.DataFrame(index=df.index)
        df_new["timestamp"] = df["timestamp"]
        df_new["symbol"] = df["symbol"]

        # Apply all feature engineering functions
        self._add_original_features(df, df_new)
        self._add_price_moving_averages(df, df_new)
        self._add_volume_moving_averages(df, df_new)
        self._add_price_volatility(df, df_new)
        self._add_volume_volatility(df, df_new)
        self._add_return_features(df, df_new)

        return df_new

    def _add_original_features(self, df: pd.DataFrame, df_new: pd.DataFrame) -> None:
        """Add lagged features from original OHLCV data."""
        df_new["open"] = df["Open"]
        df_new["high"] = df["High"]
        df_new["low"] = df["Low"]
        df_new["close"] = df["Close"]
        df_new["volume"] = df["Volume"]
        df_new["open_1"] = df["Open"].shift(1)
        df_new["close_1"] = df["Close"].shift(1)
        df_new["high_1"] = df["High"].shift(1)
        df_new["low_1"] = df["Low"].shift(1)
        df_new["volume_1"] = df["Volume"].shift(1)

    def _add_price_moving_averages(self, df: pd.DataFrame, df_new: pd.DataFrame) -> None:
        """
        Add moving averages of price and their cross-period ratios.

        Features:
        - avg_price_5: 5-day moving average
        - avg_price_30: 21-day (monthly) moving average
        - avg_price_365: 252-day (yearly) moving average
        - Ratios between different timeframes
        """
        df_new["avg_price_5"] = df["Close"].rolling(5).mean().shift(1)
        df_new["avg_price_30"] = df["Close"].rolling(21).mean().shift(1)
        df_new["avg_price_365"] = df["Close"].rolling(252).mean().shift(1)

        df_new["ratio_avg_price_5_30"] = df_new["avg_price_5"] / df_new["avg_price_30"]
        df_new["ratio_avg_price_5_365"] = df_new["avg_price_5"] / df_new["avg_price_365"]
        df_new["ratio_avg_price_30_365"] = df_new["avg_price_30"] / df_new["avg_price_365"]

    def _add_volume_moving_averages(self, df: pd.DataFrame, df_new: pd.DataFrame) -> None:
        """
        Add moving averages of volume and their cross-period ratios.

        Features indicate trading activity changes across timeframes.
        """
        df_new["avg_volume_5"] = df["Volume"].rolling(5).mean().shift(1)
        df_new["avg_volume_30"] = df["Volume"].rolling(21).mean().shift(1)
        df_new["avg_volume_365"] = df["Volume"].rolling(252).mean().shift(1)

        df_new["ratio_avg_volume_5_30"] = df_new["avg_volume_5"] / df_new["avg_volume_30"]
        df_new["ratio_avg_volume_5_365"] = df_new["avg_volume_5"] / df_new["avg_volume_365"]
        df_new["ratio_avg_volume_30_365"] = df_new["avg_volume_30"] / df_new["avg_volume_365"]

    def _add_price_volatility(self, df: pd.DataFrame, df_new: pd.DataFrame) -> None:
        """
        Add standard deviations (volatility) of price and their ratios.

        Features detect regime shifts in market volatility.
        """
        df_new["std_price_5"] = df["Close"].rolling(5).std().shift(1)
        df_new["std_price_30"] = df["Close"].rolling(21).std().shift(1)
        df_new["std_price_365"] = df["Close"].rolling(252).std().shift(1)

        df_new["ratio_std_price_5_30"] = df_new["std_price_5"] / df_new["std_price_30"]
        df_new["ratio_std_price_5_365"] = df_new["std_price_5"] / df_new["std_price_365"]
        df_new["ratio_std_price_30_365"] = df_new["std_price_30"] / df_new["std_price_365"]

    def _add_volume_volatility(self, df: pd.DataFrame, df_new: pd.DataFrame) -> None:
        """
        Add standard deviations of volume and their ratios.

        Features identify unusual trading activity patterns.
        """
        df_new["std_volume_5"] = df["Volume"].rolling(5).std().shift(1)
        df_new["std_volume_30"] = df["Volume"].rolling(21).std().shift(1)
        df_new["std_volume_365"] = df["Volume"].rolling(252).std().shift(1)

        df_new["ratio_std_volume_5_30"] = df_new["std_volume_5"] / df_new["std_volume_30"]
        df_new["ratio_std_volume_5_365"] = df_new["std_volume_5"] / df_new["std_volume_365"]
        df_new["ratio_std_volume_30_365"] = df_new["std_volume_30"] / df_new["std_volume_365"]

    def _add_return_features(self, df: pd.DataFrame, df_new: pd.DataFrame) -> None:
        """
        Add percentage return features and momentum indicators.

        Features:
        - return_N: N-period returns
        - moving_avg_N: N-period moving average of daily returns (momentum)
        """
        # Calculate returns over different periods
        df_new["return_1"] = (
            (df["Close"] - df["Close"].shift(1)) / df["Close"].shift(1)
        ).shift(1)
        df_new["return_5"] = (
            (df["Close"] - df["Close"].shift(5)) / df["Close"].shift(5)
        ).shift(1)
        df_new["return_30"] = (
            (df["Close"] - df["Close"].shift(21)) / df["Close"].shift(21)
        ).shift(1)
        df_new["return_365"] = (
            (df["Close"] - df["Close"].shift(252)) / df["Close"].shift(252)
        ).shift(1)

        # Moving averages of daily returns (momentum indicators)
        df_new["moving_avg_5"] = df_new["return_1"].rolling(5).mean().shift(1)
        df_new["moving_avg_30"] = df_new["return_1"].rolling(21).mean().shift(1)
        df_new["moving_avg_365"] = df_new["return_1"].rolling(252).mean().shift(1)
