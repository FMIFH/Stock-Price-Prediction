from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class PostgeSQLSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env", env_file_encoding="utf-8", extra="ignore", validate_by_name=True
    )

    POSTGRES_HOST: str = Field("localhost")
    POSTGRES_PORT: int = Field(5432)
    POSTGRES_USER: str = Field("featurestore")
    POSTGRES_PASSWORD: str = Field("featurestore123")
    POSTGRES_DB: str = Field("features")


class RedisSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env", env_file_encoding="utf-8", extra="ignore"
    )

    REDIS_HOST: str = Field("localhost")
    REDIS_PORT: int = Field(6379)


class KafkaSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env", env_file_encoding="utf-8", extra="ignore"
    )

    KAFKA_BROKER: str = Field("localhost:9092")
    KAFKA_TOPIC: str = Field("stock_data")
    KAFKA_GROUP_ID: str = Field("feature_store_group")

class StocksSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env", env_file_encoding="utf-8", extra="ignore"
    )

    STOCK_SYMBOL: str = Field("AAPL")


postgresql_settings = PostgeSQLSettings()
redis_settings = RedisSettings()
kafka_settings = KafkaSettings()
stocks_settings = StocksSettings()
