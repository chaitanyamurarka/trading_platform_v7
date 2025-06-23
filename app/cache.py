import redis
import json
from typing import Optional, List, Any
from .config import settings
from . import schemas
import hashlib
import logging

REDIS_URL = settings.REDIS_URL
redis_client = redis.Redis.from_url(REDIS_URL, decode_responses=True)
CACHE_EXPIRATION_SECONDS = 60 * 35 # 35 minutes

def build_cache_key(data_type: schemas.DataType, params: dict) -> str:
    """Builds a unique and consistent cache key for any data request."""
    key_parts = [
        data_type.value,
        params.get('exchange'),
        params.get('token'),
        params.get('interval'),
        params.get('start_time').strftime('%Y%m%d%H%M%S'),
        params.get('end_time').strftime('%Y%m%d%H%M%S'),
        params.get('timezone'),
        params.get('session_token')
    ]
    key_string = ":".join(filter(None, key_parts))
    return f"data:{hashlib.sha256(key_string.encode()).hexdigest()}"

async def get_cached_data(cache_key: str) -> Optional[List[Any]]:
    """Retrieves and deserializes data from Redis cache."""
    cached_data = redis_client.get(cache_key)
    if not cached_data:
        return None
    try:
        deserialized = json.loads(cached_data)
        # No need to validate with Pydantic here, assume it was valid on set
        return deserialized
    except (json.JSONDecodeError, TypeError):
        return None

async def set_cached_data(cache_key: str, data: List[Any], expiration: int = CACHE_EXPIRATION_SECONDS):
    """Serializes and stores data in Redis cache."""
    try:
        # Pydantic models need to be converted to dicts for JSON serialization
        serializable_data = [item.model_dump(mode='json') if hasattr(item, 'model_dump') else item for item in data]
        redis_client.set(cache_key, json.dumps(serializable_data), ex=expiration)
    except TypeError as e:
        logging.error(f"Error serializing data for cache key {cache_key}: {e}")