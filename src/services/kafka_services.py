import json
import logging
import time

from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable

from models.settings import kafka_settings

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_kafka_consumer(max_retries=10, retry_interval=5):
    """
    Create a Kafka consumer with retry logic.

    Parameters:
    max_retries (int): Maximum number of connection attempts
    retry_interval (int): Seconds to wait between retries

    Returns:
    KafkaConsumer: Configured Kafka consumer instance
    """
    for attempt in range(1, max_retries + 1):
        try:
            logger.info(
                f"Attempting to connect to Kafka at {kafka_settings.KAFKA_BROKER} (attempt {attempt}/{max_retries})..."
            )
            consumer = KafkaConsumer(
                kafka_settings.KAFKA_TOPIC,
                bootstrap_servers=[kafka_settings.KAFKA_BROKER],
                group_id=kafka_settings.KAFKA_GROUP_ID,
                value_deserializer=lambda x: json.loads(x.decode("utf-8")),
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                api_version_auto_timeout_ms=10000,
            )
            logger.info("Successfully connected to Kafka broker!")
            return consumer
        except NoBrokersAvailable:
            if attempt < max_retries:
                logger.warning(
                    f"Kafka broker not available. Retrying in {retry_interval} seconds..."
                )
                time.sleep(retry_interval)
            else:
                logger.error(f"Failed to connect to Kafka after {max_retries} attempts")
                raise
        except Exception as e:
            logger.error(f"Unexpected error connecting to Kafka: {str(e)}")
            if attempt < max_retries:
                time.sleep(retry_interval)
            else:
                raise


def create_kafka_producer(max_retries=10, retry_interval=5):
    """
    Create a Kafka producer with retry logic.

    Parameters:
    max_retries (int): Maximum number of connection attempts
    retry_interval (int): Seconds to wait between retries

    Returns:
    KafkaProducer: Configured Kafka producer instance
    """
    for attempt in range(1, max_retries + 1):
        try:
            logger.info(
                f"Attempting to connect to Kafka at {kafka_settings.KAFKA_BROKER} (attempt {attempt}/{max_retries})..."
            )
            producer = KafkaProducer(
                bootstrap_servers=[kafka_settings.KAFKA_BROKER],
                value_serializer=lambda x: json.dumps(x).encode("utf-8"),
                key_serializer=lambda x: x.encode("utf-8"),
                api_version_auto_timeout_ms=10000,
            )
            logger.info("Successfully connected to Kafka broker!")
            return producer
        except NoBrokersAvailable:
            if attempt < max_retries:
                logger.warning(
                    f"Kafka broker not available. Retrying in {retry_interval} seconds..."
                )
                time.sleep(retry_interval)
            else:
                logger.error(f"Failed to connect to Kafka after {max_retries} attempts")
                raise
        except Exception as e:
            logger.error(f"Unexpected error connecting to Kafka: {str(e)}")
            if attempt < max_retries:
                time.sleep(retry_interval)
            else:
                raise
