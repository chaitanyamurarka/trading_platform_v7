"""
config.py

This module centralizes application configuration management.

It uses pydantic-settings to load settings from environment variables or a .env file,
providing a single, type-hinted source of truth for configuration values like
database URLs, API keys, and other service credentials.
"""

import os
from pathlib import Path
from pydantic_settings import BaseSettings
from typing import Optional
from dotenv import load_dotenv

# --- MODIFICATION START ---
# Build a path to the .env file.
# Path(__file__) is the path to this config.py file.
# .parent is the directory it's in (ingestion_service).
# .parent.parent is the project root (trading_platform_v5).
# Then we append the .env filename.
# This makes the path absolute and independent of where the script is run.
env_path = Path(__file__).parent.parent / ".env"

# Load the .env file from the calculated path if it exists.
if env_path.exists():
    load_dotenv(dotenv_path=env_path)
    print(f"Loaded environment variables from: {env_path}")
else:
    print(f"Warning: .env file not found at {env_path}. Using default settings or environment variables.")
# --- MODIFICATION END ---


class Settings(BaseSettings):
    """
    Defines the application's configuration settings.
    Pydantic automatically reads these from environment variables (case-insensitive)
    which were loaded by load_dotenv().
    """
    # URL for the Redis instance, used for caching and as a Celery message broker/result backend.
    REDIS_URL: str = os.getenv("REDIS_URL", "redis://localhost:6379/0")

    # Celery configuration, defaulting to the same Redis instance.
    CELERY_BROKER_URL: str = os.getenv("CELERY_BROKER_URL", "redis://localhost:6379/0")
    CELERY_RESULT_BACKEND: str = os.getenv("CELERY_RESULT_BACKEND", "redis://localhost:6379/0")

    # DTN IQFeed Credentials for market data.
    DTN_PRODUCT_ID: Optional[str] = os.getenv("DTN_PRODUCT_ID")
    DTN_LOGIN: Optional[str] = os.getenv("DTN_LOGIN")
    DTN_PASSWORD: Optional[str] = os.getenv("DTN_PASSWORD")

    INFLUX_URL: Optional[str] = os.getenv("INFLUX_URL")
    INFLUX_TOKEN: Optional[str] = os.getenv("INFLUX_TOKEN")
    INFLUX_ORG: Optional[str] = os.getenv("INFLUX_ORG")
    INFLUX_BUCKET: Optional[str] = os.getenv("INFLUX_BUCKET")


    class Config:
        # Pydantic can also look for an env file, but we are loading it explicitly above
        # to ensure the correct path is used.
        env_file = ".env"
        env_file_encoding = 'utf-8'

# Create a single, globally-accessible instance of the settings.
settings = Settings()