import logging
import time

import pandas as pd
import yfinance as yf

from models.settings import kafka_settings, stocks_settings
from services.kafka_services import create_kafka_producer

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


SYMBOL = stocks_settings.STOCK_SYMBOL
stock = yf.Ticker(SYMBOL)


producer = create_kafka_producer()


def fetch_stock_data(ticker_symbol: str, period="2y", interval="1d") -> pd.DataFrame:
    """
    Fetch historical stock data for a given ticker symbol.

    Parameters:
    ticker_symbol (str): The stock ticker symbol.
    period (str): The period over which to fetch data (default is '2y' for 2 years).
    interval (str): The data interval (default is '1d' for daily data).

    Returns:
    pd.DataFrame: A pandas DataFrame containing the historical stock data.
    """
    global stock
    hist = stock.history(period=period, interval=interval)
    return hist


def stream_stock_data(ticker_symbol: str):
    """
    Generator that yields historical stock data rows one at a time.

    Parameters:
    ticker_symbol (str): The stock ticker symbol.

    Yields:
    pd.Series: A row of historical stock data.
    """
    data = fetch_stock_data(ticker_symbol)
    while True:
        try:
            logger.info(f"Fetched {len(data)} rows of data for {ticker_symbol}")
            for _, row in data.iterrows():
                payload = {
                    "Symbol": ticker_symbol,
                    "Timestamp": row.name.isoformat(),
                    "Open": row["Open"],
                    "High": row["High"],
                    "Low": row["Low"],
                    "Close": row["Close"],
                    "Volume": row["Volume"],
                }
                producer.send(
                    kafka_settings.KAFKA_TOPIC, key=ticker_symbol, value=payload
                )
                logger.info(f"[{ticker_symbol}] Sent data: {payload['Close']}")
                time.sleep(0.1)
            time.sleep(24 * 60 * 60)  # Daily data - send one record per day
            data = fetch_stock_data(ticker_symbol, period="1d", interval="1d")
        except Exception as e:
            logger.error(f"Error in {SYMBOL} producer: {str(e)}")
            time.sleep(10)


if __name__ == "__main__":
    stream_stock_data(SYMBOL)
