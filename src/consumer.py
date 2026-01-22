import logging

from services.kafka_services import create_kafka_consumer
from services.stock_data_processor import StockDataProcessor

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Consumer")

def consume_stock_data():
    """
    Main consumer loop that processes stock data from Kafka.
    Handles multiple stock symbols with separate processors for each.
    """
    consumer = create_kafka_consumer()

    # Dictionary to hold separate processors for each symbol
    processors = {}

    message_count = 0

    try:
        for message in consumer:
            try:
                data = message.value
                symbol = data['Symbol']

                logger.info(
                    f"Received: [{symbol}] Open: ${data['Open']:.2f} | Close: ${data['Close']:.2f} | "
                    f"High: ${data['High']:.2f} | Low: ${data['Low']:.2f} | "
                    f"Volume: {data['Volume']:,} | Time: {data['Timestamp']}"
                )

                # Create processor for symbol if it doesn't exist
                if symbol not in processors:
                    logger.info(f"Creating new processor for symbol: {symbol}")
                    processors[symbol] = StockDataProcessor(
                        symbol=symbol,
                        recover_on_startup=True
                    )

                # Add record to the appropriate processor
                processors[symbol].add_record(data)
                message_count += 1

                # Preprocess and log features periodically
                if message_count % 10 == 0:
                    # Process features for all symbols
                    for sym, proc in processors.items():
                        latest_features = proc.get_latest_features()
                        if latest_features:
                            logger.info(
                                f"[{sym}] Latest features calculated - Ready for ML prediction"
                            )
                    logger.info(f"Processed {message_count} messages so far")

            except Exception as e:
                logger.error(f"Error processing message: {str(e)}")
                continue

    except KeyboardInterrupt:
        logger.info("Consumer interrupted by user")
    except Exception as e:
        logger.error(f"Consumer error: {str(e)}")
    finally:
        # Cleanup all processors
        for proc in processors.values():
            proc.cleanup()
        consumer.close()
        logger.info("Consumer closed")


if __name__ == "__main__":
    consume_stock_data()
