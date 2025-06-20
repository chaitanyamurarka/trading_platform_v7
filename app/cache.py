# app/cache.py
import hashlib
import json
import logging
from datetime import datetime, timezone
import redis
from typing import List, Optional,Union

# Assuming settings is configured correctly
from .config import settings
from .schemas import Candle, Interval # Ensure Interval enum is imported
from . import schemas
# Initialize Redis client
try:
    REDIS_URL = settings.REDIS_URL
    redis_client = redis.Redis.from_url(REDIS_URL)
    # Ping to check connection
    redis_client.ping()
    logging.info("Successfully connected to Redis for caching.")
except redis.exceptions.ConnectionError as e:
    logging.error(f"Could not connect to Redis: {e}")
    redis_client = None

CACHE_EXPIRATION_SECONDS = 60 * 35 # 35 minutes cache for historical data

def parse_interval_to_seconds(interval_str: str) -> Union[int, str]: # Changed return type to Union[int, str]
    """
    Converts interval string (e.g., '1s', '1m', '1h', '1d', '1t') to seconds or returns string for tick.
    """
    if interval_str.endswith('s'):
        return int(interval_str[:-1])
    elif interval_str.endswith('m'):
        return int(interval_str[:-1]) * 60
    elif interval_str.endswith('h'):
        return int(interval_str[:-1]) * 3600
    elif interval_str.endswith('d'):
        return int(interval_str[:-1]) * 24 * 3600
    elif interval_str.endswith('t'): # Handle tick intervals
        return interval_str # Return the string directly for tick intervals
    else:
        raise ValueError(f"Invalid interval format: {interval_str}")

def build_ohlc_cache_key(
    exchange: str,
    token: str,
    interval: str, # Changed type hint to str, as Interval is str Enum
    start_time: datetime,
    end_time: datetime,
    timezone: str,
    session_token: str
) -> str:
    """
    Generates a unique cache key for OHLC data requests.
    Handles both time-based and tick-based intervals.
    """
    interval_key_part = parse_interval_to_seconds(interval)

    # Ensure timestamps are UTC for consistency
    start_ts = int(start_time.timestamp())
    end_ts = int(end_time.timestamp())

    # Conditionally apply timestamp alignment for time-based intervals only
    if isinstance(interval_key_part, int): # It's a time-based interval in seconds
        # Align start and end timestamps to the beginning of the interval for consistent caching
        # This prevents separate cache entries for requests that cover the same bars
        aligned_start = (start_ts // interval_key_part) * interval_key_part
        # For the end, we align it to the end of the last full interval it covers
        # This ensures that if a request ends mid-interval, it still uses a key for the whole interval
        # If end_ts is perfectly aligned, this will be end_ts itself.
        # Otherwise, it aligns to the beginning of the last interval that contains end_ts
        aligned_end = ((end_ts + interval_key_part -1) // interval_key_part) * interval_key_part
        # Using a slight adjustment for end_ts to ensure we capture the full last bar
        # that could be requested if the end_time falls within it.
        # However, for simplicity and typical charting requests, just using the raw timestamp
        # might be sufficient if interval_seconds is used only for `start_ts` alignment.
        # For cache key, let's just use start and end for time ranges.
        # A simpler approach for cache keys for time-based data is to still align the start but use raw end.
        
        # Reverting to simpler, effective cache key logic for time-based:
        # The alignment for historical data is less critical for the cache key itself
        # than for ensuring the database queries are efficient.
        # Let's use raw start/end timestamps for key, but ensure consistency in interval part.
        
        # A hash of all parts is safer and shorter for Redis keys
        # Use the raw start/end timestamps from the datetime objects, converted to UTC int
        key_components = f"{session_token}:{exchange}:{token}:{interval_key_part}:{start_ts}:{end_ts}:{timezone}"
    else: # It's a tick-based interval string (e.g., "1t")
        key_components = f"{session_token}:{exchange}:{token}:{interval_key_part}:{start_ts}:{end_ts}:{timezone}"

    # Use SHA256 hash to create a concise and unique key
    hashed_key = hashlib.sha256(key_components.encode('utf-8')).hexdigest()
    return f"ohlc:{hashed_key}" # Prefix with "ohlc" for clarity in Redis

def get_cached_ohlc_data(key: str) -> Optional[List[Candle]]:
    """Retrieves OHLC data from Redis cache."""
    if redis_client:
        try:
            cached_data = redis_client.get(key)
            if cached_data:
                decoded_data = json.loads(cached_data)
                candles = [Candle(**item) for item in decoded_data]
                logging.info(f"Cache HIT for key: {key}")
                return candles
        except Exception as e:
            logging.error(f"Error retrieving or deserializing data from Redis for key {key}: {e}")
    return None

def set_cached_ohlc_data(key: str, data: List[Candle], expiration: int = CACHE_EXPIRATION_SECONDS):
    """Stores OHLC data in Redis cache."""
    if redis_client:
        try:
            serialized_data = json.dumps([c.model_dump() for c in data]) # Use model_dump() for Pydantic v2
            redis_client.setex(key, expiration, serialized_data)
            logging.info(f"Data set in cache for key: {key}")
        except Exception as e:
            logging.error(f"Error serializing or storing data in Redis for key {key}: {e}")

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