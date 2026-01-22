from datetime import datetime, timezone

from sqlalchemy import BigInteger, Column, DateTime, Float, Index, Integer, String
from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    pass


class StockPrice(Base):
    __tablename__ = "stock_prices"

    __table_args__ = (
        Index("idx_stock_features_symbol_timestamp", "symbol", "timestamp", unique=True),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    symbol = Column(String, nullable=False)
    timestamp = Column(
        DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc)
    )
    open = Column(Float, nullable=False)
    high = Column(Float, nullable=False)
    low = Column(Float, nullable=False)
    close = Column(Float, nullable=False)
    volume = Column(BigInteger, nullable=False)
    open_1 = Column(Float, nullable=True)
    high_1 = Column(Float, nullable=True)
    low_1 = Column(Float, nullable=True)
    close_1 = Column(Float, nullable=True)
    volume_1 = Column(BigInteger, nullable=True)
    avg_price_5 = Column(Float, nullable=True)
    avg_price_30 = Column(Float, nullable=True)
    avg_price_365 = Column(Float, nullable=True)
    ratio_avg_price_5_30 = Column(Float, nullable=True)
    ratio_avg_price_5_365 = Column(Float, nullable=True)
    ratio_avg_price_30_365 = Column(Float, nullable=True)
    avg_volume_5 = Column(Float, nullable=True)
    avg_volume_30 = Column(Float, nullable=True)
    avg_volume_365 = Column(Float, nullable=True)
    ratio_avg_volume_5_30 = Column(Float, nullable=True)
    ratio_avg_volume_5_365 = Column(Float, nullable=True)
    ratio_avg_volume_30_365 = Column(Float, nullable=True)
    std_price_5 = Column(Float, nullable=True)
    std_price_30 = Column(Float, nullable=True)
    std_price_365 = Column(Float, nullable=True)
    ratio_std_price_5_30 = Column(Float, nullable=True)
    ratio_std_price_5_365 = Column(Float, nullable=True)
    ratio_std_price_30_365 = Column(Float, nullable=True)
    std_volume_5 = Column(Float, nullable=True)
    std_volume_30 = Column(Float, nullable=True)
    std_volume_365 = Column(Float, nullable=True)
    ratio_std_volume_5_30 = Column(Float, nullable=True)
    ratio_std_volume_5_365 = Column(Float, nullable=True)
    ratio_std_volume_30_365 = Column(Float, nullable=True)
    return_1 = Column(Float, nullable=True)
    return_5 = Column(Float, nullable=True)
    return_30 = Column(Float, nullable=True)
    return_365 = Column(Float, nullable=True)
    moving_avg_5 = Column(Float, nullable=True)
    moving_avg_30 = Column(Float, nullable=True)
    moving_avg_365 = Column(Float, nullable=True)
