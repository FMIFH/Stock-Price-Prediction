"""
Buffer recovery service for loading historical data on startup.
Handles recovery from feature store to populate data buffer.
"""
import logging

import pandas as pd

from services.feature_store import FeatureStore

logger = logging.getLogger("BufferRecoveryService")


class BufferRecoveryService:
    """
    Handles recovery of historical stock data from feature store.
    Converts stored features back to raw record format for buffer population.
    """

    def __init__(self, feature_store: FeatureStore):
        """
        Initialize buffer recovery service.

        Parameters:
        feature_store (FeatureStore): Feature store instance for data retrieval
        """
        self.feature_store = feature_store
        logger.info("BufferRecoveryService initialized")

    def recover(self, symbol: str, window_size: int) -> list[dict]:
        """
        Recover historical records from feature store.

        Parameters:
        symbol (str): Stock symbol to recover data for
        window_size (int): Number of records to recover

        Returns:
        list[dict]: list of stock records in Kafka message format
        """
        logger.info(f"Attempting to recover {window_size} records for {symbol}...")

        try:
            # Load historical features from PostgreSQL
            df = self.feature_store.get_historical_features(
                symbol=symbol,
                limit=window_size
            )

            if df.empty:
                logger.warning(f"No historical data found in feature store for {symbol}")
                return []

            # Convert feature store format back to raw record format
            records = self._convert_features_to_records(df)

            logger.info(f"✓ Recovered {len(records)} records from feature store")
            if records:
                logger.info(f"Data range: {records[0]['Timestamp']} to {records[-1]['Timestamp']}")

            return records

        except Exception as e:
            logger.error(f"Failed to recover buffer from feature store: {str(e)}")
            logger.warning("Consumer will start with empty buffer")
            return []

    def _convert_features_to_records(self, df: pd.DataFrame) -> list[dict]:
        """
        Convert feature store DataFrame to list of raw records.

        Parameters:
        df (pd.DataFrame): Feature store data

        Returns:
        list[dict]: list of records in Kafka message format
        """
        # Sort by timestamp (oldest first) to maintain chronological order
        df = df.sort_values('timestamp')

        records = []
        for _, row in df.iterrows():
            # Reconstruct the original Kafka message format
            record = {
                'Symbol': row['symbol'],
                'Timestamp': row['timestamp'],
                'Open': row.get('open', 0),
                'High': row.get('high', 0),
                'Low': row.get('low', 0),
                'Close': row.get('close', 0),
                'Volume': int(row.get('volume', 0)) if pd.notna(row.get('volume')) else 0
            }
            records.append(record)

        return records
