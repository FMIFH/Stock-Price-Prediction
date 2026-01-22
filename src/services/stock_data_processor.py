import logging
import threading
import time
from typing import Dict, Optional

import pandas as pd

from services.buffer_recovery import BufferRecoveryService
from services.data_buffer import DataBuffer
from services.feature_store import DatabaseSessionService, FeatureStore, RedisService
from services.technical_indicators import TechnicalIndicatorCalculator

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("StockDataProcessor")


class StockDataProcessor:
    """
    Lightweight orchestrator for stock data processing.
    Delegates responsibilities to specialized services:
    - DataBuffer: In-memory data management
    - TechnicalIndicatorCalculator: Feature engineering
    - FeatureStore: Persistent storage
    - BufferRecoveryService: Startup recovery
    """

    def __init__(
        self,
        window_size: int = 400,
        symbol: Optional[str] = None,
        recover_on_startup: bool = True,
        batch_write_threshold: int = 100,  # Write to DB every N records
        auto_flush_timeout: int = 10  # Auto-flush if no messages for N seconds
    ):
        """
        Initialize the stock data processor.

        Parameters:
        window_size (int): Number of recent records to keep for feature engineering
        symbol (str): Stock symbol for data recovery
        use_feature_store (bool): Whether to use the feature store for storage
        recover_on_startup (bool): Whether to recover buffer from feature store on startup
        batch_write_threshold (int): Number of new records before writing to database
        auto_flush_timeout (int): Seconds of inactivity before auto-flushing pending records
        """
        self.window_size = window_size
        self.symbol = symbol
        self.total_records_processed = 0  # Track total records processed
        self.batch_write_threshold = batch_write_threshold
        self.pending_records = 0  # Track records pending database write
        self.auto_flush_timeout = auto_flush_timeout
        self.last_record_time = time.time()  # Track time of last record
        self._stop_flush_thread = threading.Event()
        self._flush_thread = None

        # Initialize services
        self.buffer = DataBuffer(window_size=window_size)
        self.calculator = TechnicalIndicatorCalculator()

        # Initialize feature store
        try:
            db_service = DatabaseSessionService()
            redis_service = RedisService()
            self.feature_store = FeatureStore(
                database_session_service=db_service,
                redis_service=redis_service
            )
            logger.info("Feature store initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize feature store: {str(e)}")
            self.feature_store = None

        # Recover buffer on startup if feature store is available
        if recover_on_startup and symbol and self.feature_store:
            self._recover_buffer(symbol)

        # Start auto-flush thread if timeout is configured
        self._start_auto_flush_thread()

    def _recover_buffer(self, symbol: str) -> None:
        """
        Recover buffer state from feature store on startup.

        Parameters:
        symbol (str): Stock symbol to recover data for
        """
        recovery_service = BufferRecoveryService(self.feature_store)
        records = recovery_service.recover(symbol, self.window_size)

        if records:
            self.buffer.load_records(records)

    def add_record(self, record: Dict) -> None:
        """
        Add a new record to the buffer.

        Parameters:
        record (dict): Stock data record
        """
        self.buffer.add(record)
        self.total_records_processed += 1
        self.pending_records += 1
        self.last_record_time = time.time()  # Update activity timestamp

    def preprocess_for_ml(self, store_features: bool = True) -> pd.DataFrame:
        """
        Preprocess data for ML model training/prediction.

        Parameters:
        store_features (bool): Whether to store features in feature store

        Returns:
        pd.DataFrame: Preprocessed DataFrame ready for ML
        """
        # Get data from buffer
        df = self.buffer.get_dataframe()

        if df.empty:
            logger.warning("No data available for preprocessing")
            return df

        # Calculate technical indicators
        df = self.calculator.calculate_all(df)

        # Store in feature store
        if store_features and self.feature_store:
            self._store_features(df)

        # Log preprocessing summary
        self._log_preprocessing_summary(df)

        return df

    def _store_features(self, df: pd.DataFrame) -> None:
        """
        Store features in feature store (Redis + PostgreSQL).
        Uses batch writing to reduce database load.

        Parameters:
        df (pd.DataFrame): DataFrame with calculated features
        """
        try:
            if not df.empty:
                symbol = df["symbol"].iloc[-1]
                latest_features = df.iloc[-1].to_dict()
                # Always update Redis for real-time access (lightweight)
                self.feature_store.store_latest_features(symbol, latest_features)

                # Store only NEW historical features in PostgreSQL
                # Use batch writing to reduce database load
                # Only write to database when batch threshold is reached
                if self.pending_records >= self.batch_write_threshold:
                    # Get the most recent pending records from the buffer
                    # Since buffer is a rolling window, we take the last N records
                    records_to_write = min(self.pending_records, len(df))
                    new_records_df = df.iloc[-records_to_write:]
                    self.feature_store.store_historical_features(new_records_df)
                    logger.info(f"Batch wrote {records_to_write} records to PostgreSQL")
                    self.pending_records = 0
        except Exception as e:
            logger.error(f"Error storing features: {str(e)}")

    def _log_preprocessing_summary(self, df: pd.DataFrame) -> None:
        """
        Log summary of preprocessing results.

        Parameters:
        df (pd.DataFrame): Preprocessed DataFrame
        """
        logger.info(f"Preprocessed {len(df)} records with {len(df.columns)} features")

        if not df.empty and "open" in df.columns:
            logger.info(f"Latest open price: ${df['open'].iloc[-1]:.2f}")

            if "return_1" in df.columns and pd.notna(df["return_1"].iloc[-1]):
                latest_return = df["return_1"].iloc[-1] * 100
                logger.info(f"Latest 1-day return: {latest_return:.2f}%")

    def get_latest_features(self) -> Optional[Dict]:
        """
        Get the latest record with all features for real-time prediction.

        Returns:
        dict: Latest record with all features or None if no data
        """
        df = self.preprocess_for_ml()

        if df.empty:
            return None

        return df.iloc[-1].to_dict()

    def _start_auto_flush_thread(self) -> None:
        """Start background thread for automatic timeout-based flushing."""
        if self.auto_flush_timeout > 0:
            self._flush_thread = threading.Thread(
                target=self._auto_flush_worker,
                daemon=True,
                name=f"AutoFlush-{self.symbol}"
            )
            self._flush_thread.start()
            logger.info(f"Auto-flush thread started (timeout={self.auto_flush_timeout}s)")

    def _auto_flush_worker(self) -> None:
        """Background worker that checks for inactivity and flushes pending records."""
        check_interval = min(5, self.auto_flush_timeout / 2)  # Check at least every 5 seconds

        while not self._stop_flush_thread.is_set():
            try:
                # Wait for check interval or stop signal
                if self._stop_flush_thread.wait(timeout=check_interval):
                    break

                # Check if we have pending records and if timeout has elapsed
                if self.pending_records > 0:
                    time_since_last_record = time.time() - self.last_record_time

                    if time_since_last_record >= self.auto_flush_timeout:
                        logger.info(
                            f"No messages for {time_since_last_record:.1f}s, "
                            f"auto-flushing {self.pending_records} pending records"
                        )
                        self.flush_pending_records()

            except Exception as e:
                logger.error(f"Error in auto-flush worker: {str(e)}")

    def flush_pending_records(self) -> None:
        """Force write any pending records to database."""
        if self.pending_records > 0 and self.feature_store:
            try:
                df = self.buffer.get_dataframe()
                if not df.empty:
                    df = self.calculator.calculate_all(df)
                    # Get the most recent pending records from the buffer
                    records_to_write = min(self.pending_records, len(df))
                    new_records_df = df.iloc[-records_to_write:]
                    self.feature_store.store_historical_features(new_records_df)
                    logger.info(f"Flushed {records_to_write} pending records to PostgreSQL")
                    self.pending_records = 0
            except Exception as e:
                logger.error(f"Error flushing pending records: {str(e)}")

    def cleanup(self) -> None:
        """Close feature store connections."""
        # Stop auto-flush thread
        if self._flush_thread and self._flush_thread.is_alive():
            logger.info("Stopping auto-flush thread...")
            self._stop_flush_thread.set()
            self._flush_thread.join(timeout=5)

        # Flush any pending records before cleanup
        self.flush_pending_records()
        if self.feature_store:
            self.feature_store.close()
            logger.info("Feature store connections closed")

