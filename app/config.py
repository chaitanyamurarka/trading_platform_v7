# In app/config.py
import os
from pydantic_settings import BaseSettings
from dotenv import load_dotenv
from typing import Optional
load_dotenv()

class Settings(BaseSettings):

    # URL for the Redis instance, used for caching and as a Celery message broker/result backend.
    REDIS_URL: str = os.getenv("REDIS_URL", "redis://localhost:6379/0")

    # Celery configuration, defaulting to the same Redis instance.
    CELERY_BROKER_URL: str = os.getenv("CELERY_BROKER_URL", "redis://localhost:6379/0")
    CELERY_RESULT_BACKEND: str = os.getenv("CELERY_RESULT_BACKEND", "redis://localhost:6379/0")

    # DTN IQFeed Credentials for market data.
    # These are optional as they might be configured directly in the IQConnect client.
    DTN_PRODUCT_ID: Optional[str] = os.getenv("DTN_PRODUCT_ID")
    DTN_LOGIN: Optional[str] = os.getenv("DTN_LOGIN")
    DTN_PASSWORD: Optional[str] = os.getenv("DTN_PASSWORD")


    # InfluxDB v2 Configuration
    INFLUX_URL: str = os.getenv("INFLUX_URL")
    INFLUX_TOKEN: str = os.getenv("INFLUX_TOKEN")
    INFLUX_ORG: str = os.getenv("INFLUX_ORG")
    INFLUX_BUCKET: str = "trading_data"

    class Config:
        env_file = ".env"

settings = Settings()