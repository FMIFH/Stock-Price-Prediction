"""
Data buffer service for managing in-memory stock data.
Handles buffering and conversion to DataFrames.
"""
import logging
from collections import deque
from typing import Dict, List

import pandas as pd

logger = logging.getLogger("DataBuffer")


class DataBuffer:
    """
    Manages in-memory buffer of stock data with fixed window size.
    Provides conversion to pandas DataFrame for feature calculation.
    """

    def __init__(self, window_size: int = 100):
        """
        Initialize data buffer.

        Parameters:
        window_size (int): Maximum number of records to keep in memory
        """
        self.window_size = window_size
        self.buffer = deque(maxlen=window_size)
        logger.info(f"DataBuffer initialized with window size: {window_size}")

    def add(self, record: Dict) -> None:
        """
        Add a new record to the buffer.

        Parameters:
        record (dict): Stock data record with keys: Symbol, Timestamp, Open, High, Low, Close, Volume
        """
        self.buffer.append(record)

    def get_dataframe(self) -> pd.DataFrame:
        """
        Convert buffer to pandas DataFrame with proper types and sorting.

        Returns:
        pd.DataFrame: DataFrame with columns: timestamp, symbol, Open, High, Low, Close, Volume
        """
        if not self.buffer:
            return pd.DataFrame()

        df = pd.DataFrame(list(self.buffer))

        # Normalize column names for consistency
        column_mapping = {
            'Timestamp': 'timestamp',
            'Symbol': 'symbol'
        }
        df = df.rename(columns=column_mapping)

        # Convert timestamp to datetime
        df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True)

        # Sort by timestamp
        df = df.sort_values('timestamp')

        return df

    def size(self) -> int:
        """
        Get current buffer size.

        Returns:
        int: Number of records in buffer
        """
        return len(self.buffer)

    def clear(self) -> None:
        """Clear all records from the buffer."""
        self.buffer.clear()
        logger.info("Buffer cleared")

    def is_empty(self) -> bool:
        """
        Check if buffer is empty.

        Returns:
        bool: True if buffer is empty, False otherwise
        """
        return len(self.buffer) == 0

    def load_records(self, records: List[Dict]) -> None:
        """
        Load multiple records into the buffer (for recovery).

        Parameters:
        records (List[Dict]): List of stock data records
        """
        for record in records:
            self.buffer.append(record)
        logger.info(f"Loaded {len(records)} records into buffer")
