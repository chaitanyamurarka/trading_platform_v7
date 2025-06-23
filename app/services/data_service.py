import logging
import json
import base64
import uuid
import time
from datetime import datetime, timezone as dt_timezone
from typing import List
from fastapi import HTTPException, WebSocket
from zoneinfo import ZoneInfo

from .query_service import query_service
from .heikin_ashi_calculator import calculate_historical_heikin_ashi
from .. import schemas
from ..config import settings
from ..cache import redis_client, build_cache_key, get_cached_data, set_cached_data

logger = logging.getLogger(__name__)
INITIAL_FETCH_LIMIT = 5000

# --- Session Management ---
def initiate_session():
    """Generates and stores a new session token."""
    session_token = str(uuid.uuid4())
    redis_client.set(f"session:{session_token}", int(time.time()), ex=60 * 45)
    return schemas.SessionInfo(session_token=session_token)

# --- Historical Data Logic ---

async def get_historical_data(
    session_token: str, exchange: str, token: str, interval: str,
    data_type: schemas.DataType, start_time: datetime, end_time: datetime, timezone: str
) -> schemas.UnifiedDataResponse:
    """
    Main entry point for all historical data. Fetches, processes, and caches data.
    """
    request_params = locals()
    cache_key = build_cache_key(data_type, request_params)
    
    full_data = await get_cached_data(cache_key)
    
    if not full_data:
        logger.info(f"Cache MISS for {data_type.value} data. Key: {cache_key[:50]}...")
        full_data = await _fetch_and_process_data_from_source(request_params)
        if full_data:
            await set_cached_data(cache_key, full_data)
    else:
        logger.info(f"Cache HIT for {data_type.value} data. Key: {cache_key[:50]}...")

    if not full_data:
        return schemas.UnifiedDataResponse(candles=[], message="No data available for this range.", is_partial=False)

    total_available = len(full_data)
    initial_offset = max(0, total_available - INITIAL_FETCH_LIMIT)
    candles_to_send = full_data[initial_offset:]
    
    is_partial = total_available > len(candles_to_send)
    next_cursor = None
    if is_partial:
        cursor_data = {"cache_key": cache_key, "offset": initial_offset}
        next_cursor = base64.urlsafe_b64encode(json.dumps(cursor_data).encode()).decode()

    return schemas.UnifiedDataResponse(
        request_id=next_cursor,
        candles=candles_to_send,
        is_partial=is_partial,
        message=f"Loaded last {len(candles_to_send)} of {total_available} {data_type.value} candles."
    )

async def _fetch_and_process_data_from_source(params: dict) -> List[schemas.Candle | schemas.HeikinAshiCandle]:
    """Helper to fetch data from InfluxDB and perform necessary transformations."""
    if params['data_type'] == schemas.DataType.HEIKIN_ASHI:
        # For HA, first fetch regular candles
        regular_candles = await _fetch_regular_candles(params)
        return calculate_historical_heikin_ashi(regular_candles)
    else:
        # For regular or tick, just fetch them
        return await _fetch_regular_candles(params)

async def _fetch_regular_candles(params: dict) -> List[schemas.Candle]:
    """Fetches regular or tick candles from InfluxDB."""
    try:
        client_tz = ZoneInfo(params['timezone'])
    except Exception:
        client_tz = ZoneInfo("UTC")

    start_utc = params['start_time'].astimezone(dt_timezone.utc)
    end_utc = params['end_time'].astimezone(dt_timezone.utc)

    measurement = f"ohlc_{params['interval']}"
    if "tick" in params['interval']:
         measurement = f"ohlc_{params['interval']}"

    flux_query = f"""
        from(bucket: "{settings.INFLUX_BUCKET}")
          |> range(start: {start_utc.isoformat()}, stop: {end_utc.isoformat()})
          |> filter(fn: (r) => r._measurement == "{measurement}")
          |> filter(fn: (r) => r.symbol == "{params['token']}")
          |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
          |> sort(columns: ["_time"])
    """
    return await query_service.query_candles(flux_query, params['timezone'])

async def get_historical_chunk(request_id: str, limit: int) -> schemas.UnifiedDataChunkResponse:
    """Retrieves a subsequent chunk of data using a cursor."""
    try:
        cursor_data = json.loads(base64.urlsafe_b64decode(request_id).decode())
        cache_key = cursor_data["cache_key"]
        current_offset = cursor_data["offset"]
    except (json.JSONDecodeError, KeyError, TypeError):
        raise HTTPException(status_code=400, detail="Invalid or malformed request_id (cursor).")

    full_data = await get_cached_data(cache_key)
    if full_data is None:
        raise HTTPException(status_code=404, detail="Data for this request not found or has expired.")

    if current_offset <= 0:
        return schemas.UnifiedDataChunkResponse(candles=[], is_partial=False)

    new_offset = max(0, current_offset - limit)
    chunk = full_data[new_offset:current_offset]
    
    is_partial = new_offset > 0
    next_cursor = None
    if is_partial:
        new_cursor_data = {"cache_key": cache_key, "offset": new_offset}
        next_cursor = base64.urlsafe_b64encode(json.dumps(new_cursor_data).encode()).decode()

    return schemas.UnifiedDataChunkResponse(
        request_id=next_cursor,
        candles=chunk,
        is_partial=is_partial
    )

# --- Live Data Logic ---
async def send_initial_live_data(websocket: WebSocket, symbol: str, interval: str, timezone: str, data_type: schemas.DataType):
    """Sends the initial backfill of data when a client connects to the live feed."""
    logger.info(f"[{symbol}] Getting cached intraday bars for live backfill.")
    try:
        # Fetch 1-second bars from Redis cache
        cached_bars_str = await redis_client.lrange(f"intraday_bars:{symbol}", 0, -1)
        if not cached_bars_str:
            return

        one_sec_bars = [schemas.Candle(**json.loads(b)) for b in cached_bars_str]
        
        # Resample the 1s bars to the client's desired interval
        from ..websocket_manager import connection_manager # Avoid circular import
        resampler = connection_manager._create_resampler(interval, timezone, data_type)
        
        resampled_bars = []
        for bar in one_sec_bars:
            tick_like_data = {"price": bar.close, "volume": bar.volume, "timestamp": bar.unix_timestamp}
            completed_bar = resampler.add_bar(tick_like_data) # This function now exists in both resamplers
            if completed_bar:
                resampled_bars.append(completed_bar)
        
        if resampler.current_bar:
            resampled_bars.append(resampler.current_bar)
            
        if resampled_bars:
            await websocket.send_json([bar.model_dump() for bar in resampled_bars])
            logger.info(f"Sent {len(resampled_bars)} backfilled bars to client for {symbol}")

    except Exception as e:
        logger.error(f"Error during live data backfill for {symbol}: {e}", exc_info=True)