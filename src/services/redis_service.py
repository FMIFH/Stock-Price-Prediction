import logging

import redis

from models import redis_settings

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("RedisService")


class RedisService:
    """
    Service to manage Redis connection.
    """

    def __init__(self):
        self._init_redis()

    def _init_redis(self):
        """Initialize Redis connection."""
        try:
            self.redis_client = redis.Redis(
                host=redis_settings.REDIS_HOST,
                port=redis_settings.REDIS_PORT,
                decode_responses=True,
                socket_connect_timeout=5,
            )
            # Test connection
            self.redis_client.ping()
            logger.info(
                f"Connected to Redis at {redis_settings.REDIS_HOST}:{redis_settings.REDIS_PORT}"
            )
        except Exception as e:
            logger.error(f"Failed to connect to Redis: {str(e)}")
            self.redis_client = None

    def close(self):
        """Close Redis connection."""
        if self.redis_client:
            self.redis_client.close()
            logger.info("Closed Redis connection")
