import json
import logging
from datetime import datetime
from typing import Dict, Optional

import numpy as np
import pandas as pd
from sqlalchemy import insert

from models import StockPrice
from services.database_service import DatabaseSessionService
from services.redis_service import RedisService

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("FeatureStore")


class FeatureStore:
    """
    Feature store for managing real-time and historical stock features.
    Uses Redis for real-time features and PostgreSQL for historical storage.
    """

    def __init__(
        self,
        database_session_service: DatabaseSessionService,
        redis_service: RedisService,
    ):
        """Initialize connections to Redis and PostgreSQL."""
        # PostgreSQL configuration from settings
        self.database_session_service = database_session_service
        self.redis_service = redis_service
        self.redis_client = self.redis_service.redis_client

    def store_latest_features(self, symbol: str, features: Dict):
        """
        Store latest features in Redis for real-time access.

        Parameters:
        symbol (str): Stock symbol
        features (dict): Feature dictionary
        """
        if not self.redis_client:
            logger.warning("Redis not available, skipping real-time feature storage")
            return

        try:
            # Convert timestamp to string if it's a Pandas Timestamp
            timestamp = features.get("timestamp", datetime.now().isoformat())
            if isinstance(timestamp, pd.Timestamp):
                timestamp_str = timestamp.isoformat()
                timestamp_float = timestamp.timestamp()
            else:
                timestamp_str = str(timestamp)
                timestamp_float = pd.Timestamp(timestamp).timestamp()

            # Store as JSON with expiration (24 hours)
            key = f"features:latest:{symbol}"
            self.redis_client.setex(
                key,
                86400,  # 24 hours TTL
                json.dumps(features, default=str),
            )

            # Also maintain a sorted set of timestamps for this symbol
            timestamp_key = f"features:timestamps:{symbol}"
            self.redis_client.zadd(timestamp_key, {timestamp_str: timestamp_float})

            # Keep only last 1000 timestamps
            self.redis_client.zremrangebyrank(timestamp_key, 0, -1001)

            logger.debug(f"Stored latest features for {symbol} in Redis")
        except Exception as e:
            logger.error(f"Error storing features in Redis: {str(e)}")

    def get_latest_features(self, symbol: str) -> Optional[Dict]:
        """
        Get latest features from Redis.

        Parameters:
        symbol (str): Stock symbol

        Returns:
        dict: Latest features or None
        """
        if not self.redis_client:
            return None

        try:
            key = f"features:latest:{symbol}"
            data = self.redis_client.get(key)
            if data:
                return json.loads(data)
            return None
        except Exception as e:
            logger.error(f"Error getting features from Redis: {str(e)}")
            return None

    def store_historical_features(self, features_df: pd.DataFrame):
        """Store historical features in PostgreSQL.

        Parameters:
        features_df (pd.DataFrame): DataFrame with features
        """
        try:
            clean_df = features_df.astype(object)
            clean_df = clean_df.where(pd.notna(clean_df), None)
            clean_df = clean_df.replace([np.inf, -np.inf], None)
            objects_to_insert = clean_df.to_dict(orient="records")

            with self.database_session_service.get_session() as session:
                session.execute(insert(StockPrice), objects_to_insert)
                logger.info(
                    f"Batch inserted {len(objects_to_insert)} historical features in PostgreSQL"
                )
        except Exception as e:
            logger.error(f"Error storing historical features in PostgreSQL: {str(e)}")

    def get_historical_features(
        self,
        symbol: str,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        limit: int = 1000,
    ) -> pd.DataFrame:
        """
        Retrieve historical features from PostgreSQL.

        Parameters:
        symbol (str): Stock symbol
        start_time (datetime): Start timestamp
        end_time (datetime): End timestamp
        limit (int): Maximum number of records

        Returns:
        pd.DataFrame: Historical features
        """

        try:
            with self.database_session_service.get_session() as session:
                # Build query using ORM
                query = session.query(StockPrice).filter(StockPrice.symbol == symbol)

                if start_time:
                    query = query.filter(StockPrice.timestamp >= start_time.isoformat())

                if end_time:
                    query = query.filter(StockPrice.timestamp <= end_time.isoformat())

                query = query.order_by(StockPrice.timestamp.desc()).limit(limit)

                # Execute query and convert to DataFrame
                results = query.all()

                if not results:
                    return pd.DataFrame()

                # Convert ORM objects to dict and then to DataFrame
                data = []
                for result in results:
                    data.append(
                        {
                            "id": result.id,
                            "symbol": result.symbol,
                            "timestamp": result.timestamp,
                            "open": result.open,
                            "high": result.high,
                            "low": result.low,
                            "close": result.close,
                            "volume": result.volume,
                            "open_1": result.open_1d,
                            "high_1": result.high_1d,
                            "low_1": result.low_1d,
                            "close_1": result.close_1d,
                            "volume_1": result.volume_1d,
                            "avg_price_5": result.avg_price_5d,
                            "avg_price_30": result.avg_price_30d,
                            "avg_price_365": result.avg_price_365d,
                            "ratio_avg_volume_5_30": result.ratio_avg_volume_5_30,
                            "ratio_avg_volume_5_365": result.ratio_avg_volume_5_365,
                            "ratio_avg_volume_30_365": result.ratio_avg_volume_30_365,
                            "std_price_5": result.std_price_5,
                            "std_price_30": result.std_price_30,
                            "std_price_365": result.std_price_365,
                            "ratio_std_price_5_30": result.ratio_std_price_5_30,
                            "ratio_std_price_5_365": result.ratio_std_price_5_365,
                            "ratio_std_price_30_365": result.ratio_std_price_30_365,
                            "std_volume_5": result.std_volume_5,
                            "std_volume_30": result.std_volume_30,
                            "std_volume_365": result.std_volume_365,
                            "ratio_std_volume_5_30": result.ratio_std_volume_5_30,
                            "ratio_std_volume_5_365": result.ratio_std_volume_5_365,
                            "ratio_std_volume_30_365": result.ratio_std_volume_30_365,
                            "return_1": result.return_1,
                            "return_5": result.return_5,
                            "return_30": result.return_30,
                            "return_365": result.return_365,
                            "moving_avg_5": result.moving_avg_5,
                            "moving_avg_30": result.moving_avg_30,
                            "moving_avg_365": result.moving_avg_365,
                        }
                    )

                df = pd.DataFrame(data)
                logger.info(f"Retrieved {len(df)} historical features for {symbol}")
                return df
        except Exception as e:
            logger.error(f"Error retrieving historical features: {str(e)}")
            return pd.DataFrame()

    def get_feature_stats(self, symbol: str) -> Dict:
        """
        Get statistics about stored features.

        Parameters:
        symbol (str): Stock symbol

        Returns:
        dict: Feature statistics
        """
        stats = {
            "symbol": symbol,
            "redis_available": self.redis_client is not None,
            "postgres_available": self.Session is not None,
            "latest_features": None,
            "historical_count": 0,
        }

        # Check Redis
        if self.redis_client:
            try:
                latest = self.get_latest_features(symbol)
                if latest:
                    stats["latest_features"] = latest.get("timestamp")
            except Exception as e:
                logger.error(f"Error getting Redis stats: {str(e)}")

            try:
                with self.database_session_service.get_session() as session:
                    count = (
                        session.query(StockPrice)
                        .filter(StockPrice.symbol == symbol)
                        .count()
                    )
                    stats["historical_count"] = count
            except Exception as e:
                logger.error(f"Error getting PostgreSQL stats: {str(e)}")

        return stats

    def close(self):
        """Close all connections."""
        if self.redis_service:
            self.redis_service.close()
        if self.database_session_service:
            self.database_session_service.close()
