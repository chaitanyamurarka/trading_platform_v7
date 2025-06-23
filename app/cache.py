import redis
import json
from typing import Optional, List, Any
from .config import settings
from . import schemas
from datetime import datetime, timedelta
import hashlib


# Redis connection (URL from environment or defaults)
REDIS_URL = settings.REDIS_URL
redis_client = redis.Redis.from_url(REDIS_URL)

# Define a cache expiration time for user-specific data (e.g., 35 minutes)
CACHE_EXPIRATION_SECONDS = 60 * 35 # 35 minutes as a default

def parse_interval_to_seconds(interval_str: str) -> int:
    """Converts an interval string like '1m', '5s', '1h' to total seconds."""
    unit = interval_str[-1]
    value = int(interval_str[:-1])
    if unit == 's':
        return value
    if unit == 'm':
        return value * 60
    if unit == 'h':
        return value * 3600
    if unit == 'd':
        return value * 86400 # 24 hours in seconds
    raise ValueError(f"Invalid interval format: {interval_str}")

def get_cached_ohlc_data(cache_key: str) -> Optional[List[schemas.Candle]]:
    """Attempts to retrieve and deserialize OHLC data from Redis cache."""
    cached_data = redis_client.get(cache_key)
    if cached_data:
        try:
            deserialized_data = json.loads(cached_data)
            return [schemas.Candle(**item) for item in deserialized_data]
        except (json.JSONDecodeError, TypeError) as e:
            print(f"Error deserializing cached data for key {cache_key}: {e}")
            return None
    return None

def set_cached_ohlc_data(cache_key: str, data: List[schemas.Candle], expiration: int = CACHE_EXPIRATION_SECONDS):
    """Serializes and stores OHLC data in Redis cache with an expiration."""
    try:
        serializable_data = [item.model_dump(mode='json') for item in data]
        redis_client.set(cache_key, json.dumps(serializable_data), ex=expiration)
    except TypeError as e:
        print(f"Error serializing data for cache key {cache_key}: {e}")

def build_ohlc_cache_key(
    exchange: str,
    token: str,
    interval: str,
    start_time: datetime,
    end_time: datetime,
    timezone: str,
    session_token: str,
    candle_type: str = "regular"
) -> str:
    """
    Builds a unique cache key for an OHLC data request.
    Handles both time-based and tick-based intervals.
    """
    # --- MODIFICATION START ---
    # Check if the interval is tick-based. If so, use the string itself.
    # Otherwise, parse it to seconds for time-based intervals.
    if "tick" in interval:
        interval_identifier = interval
    else:
        try:
            interval_identifier = str(parse_interval_to_seconds(interval))
        except (ValueError, TypeError):
            # Fallback for any unexpected format
            interval_identifier = interval
    # --- MODIFICATION END ---

    start_str = start_time.strftime('%Y%m%d%H%M%S')
    end_str = end_time.strftime('%Y%m%d%H%M%S')

    key_string = (
        f"{candle_type}:{exchange}:{token}:{interval_identifier}:"
        f"{start_str}:{end_str}:{timezone}:{session_token}"
    )
    
    # Use SHA256 for a consistent and safe key format
    return hashlib.sha256(key_string.encode()).hexdigest()

def get_cached_heikin_ashi_data(cache_key: str) -> Optional[List[schemas.HeikinAshiCandle]]:
    """Attempts to retrieve and deserialize Heikin Ashi data from Redis cache."""
    cached_data = redis_client.get(cache_key)
    if cached_data:
        try:
            deserialized_data = json.loads(cached_data)
            return [schemas.HeikinAshiCandle(**item) for item in deserialized_data]
        except (json.JSONDecodeError, TypeError) as e:
            print(f"Error deserializing cached Heikin Ashi data for key {cache_key}: {e}")
            return None
    return None

def set_cached_heikin_ashi_data(cache_key: str, data: List[schemas.HeikinAshiCandle], expiration: int = CACHE_EXPIRATION_SECONDS):
    """Serializes and stores Heikin Ashi data in Redis cache with an expiration."""
    try:
        serializable_data = [item.model_dump(mode='json') for item in data]
        redis_client.set(cache_key, json.dumps(serializable_data), ex=expiration)
    except TypeError as e:
        print(f"Error serializing Heikin Ashi data for cache key {cache_key}: {e}")

def build_heikin_ashi_cache_key(
    exchange: str,
    token: str,
    interval: str,
    start_time: datetime, # Changed from start_time_iso: str
    end_time: datetime,   # Changed from end_time_iso: str
    timezone: str,
    session_token: Optional[str] = None
) -> str:
    """
    Builds a consistent cache key for Heikin Ashi data queries, aligning timestamps.
    """
    interval_seconds = parse_interval_to_seconds(interval)

    # Ensure datetimes are timezone-aware
    if start_time.tzinfo is None:
        start_time = start_time.replace(tzinfo=datetime.now().astimezone().tzinfo)
    if end_time.tzinfo is None:
        end_time = end_time.replace(tzinfo=datetime.now().astimezone().tzinfo)

    start_timestamp = int(start_time.timestamp())
    end_timestamp = int(end_time.timestamp())
    
    # Align to interval boundaries
    aligned_start = (start_timestamp // interval_seconds) * interval_seconds
    aligned_end = (end_timestamp // interval_seconds) * interval_seconds
    
    base_key = f"heikin_ashi:{exchange}:{token}:{interval}:{aligned_start}:{aligned_end}:{timezone}"
    
    if session_token:
        return f"user:{session_token}:{base_key}"
    else:
        return f"shared:{base_key}"