# app/core/cache.py
import redis
import json
from typing import Optional, List, Any
from .config import settings # Your application settings
from . import schemas # Your Pydantic schemas

# Redis connection (URL from environment or defaults)
REDIS_URL = settings.REDIS_URL
redis_client = redis.Redis.from_url(REDIS_URL)

# Define a cache expiration time for user-specific data (e.g., 35 minutes)
CACHE_EXPIRATION_SECONDS = 60 * 35 # 35 minutes as a default

def get_cached_ohlc_data(cache_key: str) -> Optional[List[schemas.Candle]]:
    """Attempts to retrieve and deserialize OHLC data from Redis cache."""
    cached_data = redis_client.get(cache_key)
    if cached_data:
        try:
            # Assuming data is stored as a JSON string of a list of candle dicts
            deserialized_data = json.loads(cached_data)
            # Convert list of dicts back to list of Pydantic models
            return [schemas.Candle(**item) for item in deserialized_data]
        except (json.JSONDecodeError, TypeError) as e:
            print(f"Error deserializing cached data for key {cache_key}: {e}")
            return None
    return None

def set_cached_ohlc_data(cache_key: str, data: List[schemas.Candle], expiration: int = CACHE_EXPIRATION_SECONDS):
    """Serializes and stores OHLC data in Redis cache with an expiration."""
    try:
        # Convert list of Pydantic models to list of dicts for JSON serialization
        serializable_data = [item.model_dump(mode='json') for item in data] # Pydantic v2
        # serializable_data = [item.dict() for item in data] # Pydantic v1
        redis_client.set(cache_key, json.dumps(serializable_data), ex=expiration)
    except TypeError as e:
        print(f"Error serializing data for cache key {cache_key}: {e}")

def build_ohlc_cache_key(
    exchange: str,
    token: str,
    interval: str,
    start_time_iso: str,
    end_time_iso: str,
    timezone: str,
    session_token: Optional[str] = None
) -> str:
    """
    Builds a consistent cache key for OHLC data queries.
    This now serves as the single source of truth for key generation.
    It includes the timezone to ensure cache uniqueness per display setting.
    """
    # Create a unique but readable key by combining the core query parameters.
    # The session_token is included to keep user data separate.
    base_key = f"ohlc:{exchange}:{token}:{interval}:{start_time_iso}:{end_time_iso}:{timezone}"
    
    if session_token:
        # User-specific cache key
        return f"user:{session_token}:{base_key}"
    else:
        # Generic, shared cache key (if applicable in the future)
        return f"shared:{base_key}"
    
def get_cached_heikin_ashi_data(cache_key: str) -> Optional[List[schemas.HeikinAshiCandle]]:
    """Attempts to retrieve and deserialize Heikin Ashi data from Redis cache."""
    cached_data = redis_client.get(cache_key)
    if cached_data:
        try:
            # Assuming data is stored as a JSON string of a list of HA candle dicts
            deserialized_data = json.loads(cached_data)
            # Convert list of dicts back to list of Heikin Ashi Pydantic models
            return [schemas.HeikinAshiCandle(**item) for item in deserialized_data]
        except (json.JSONDecodeError, TypeError) as e:
            print(f"Error deserializing cached Heikin Ashi data for key {cache_key}: {e}")
            return None
    return None

def set_cached_heikin_ashi_data(cache_key: str, data: List[schemas.HeikinAshiCandle], expiration: int = CACHE_EXPIRATION_SECONDS):
    """Serializes and stores Heikin Ashi data in Redis cache with an expiration."""
    try:
        # Convert list of Pydantic models to list of dicts for JSON serialization
        serializable_data = [item.model_dump(mode='json') for item in data] # Pydantic v2
        redis_client.set(cache_key, json.dumps(serializable_data), ex=expiration)
    except TypeError as e:
        print(f"Error serializing Heikin Ashi data for cache key {cache_key}: {e}")

def build_heikin_ashi_cache_key(
    exchange: str,
    token: str,
    interval: str,
    start_time_iso: str,
    end_time_iso: str,
    timezone: str,
    session_token: Optional[str] = None
) -> str:
    """
    Builds a consistent cache key for Heikin Ashi data queries.
    Similar to OHLC cache key but with 'heikin_ashi' prefix to distinguish from regular candles.
    """
    base_key = f"heikin_ashi:{exchange}:{token}:{interval}:{start_time_iso}:{end_time_iso}:{timezone}"
    
    if session_token:
        # User-specific cache key
        return f"user:{session_token}:{base_key}"
    else:
        # Generic, shared cache key
        return f"shared:{base_key}"
