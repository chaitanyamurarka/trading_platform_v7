# app/services/historical_service.py

import logging
import json
import base64
import time
from datetime import datetime, timezone as dt_timezone
from typing import List, Optional, Union
from fastapi import HTTPException
from zoneinfo import ZoneInfo

from .. import schemas
from ..config import settings
from ..cache import (
    redis_client,
    get_cached_ohlc_data,
    set_cached_ohlc_data,
    build_ohlc_cache_key,
    get_cached_heikin_ashi_data,
    set_cached_heikin_ashi_data,
    build_heikin_ashi_cache_key,
    CACHE_EXPIRATION_SECONDS
)
from influxdb_client import InfluxDBClient

# --- InfluxDB Client Setup ---
influx_client = InfluxDBClient(
    url=settings.INFLUX_URL,
    token=settings.INFLUX_TOKEN,
    org=settings.INFLUX_ORG,
    timeout=30_000
)
query_api = influx_client.query_api()
INITIAL_FETCH_LIMIT = 5000

# --- Internal Helper Functions ---

def _query_and_process_influx_data(flux_query: str, timezone_str: str) -> List[schemas.Candle]:
    """Helper to run a Flux query and convert results to Candle schemas."""
    try:
        target_tz = ZoneInfo(timezone_str)
    except Exception:
        target_tz = ZoneInfo("UTC")

    start_time = time.time()
    tables = query_api.query(query=flux_query)
    query_time = time.time() - start_time
    
    process_start = time.time()
    candles = []
    record_count = 0
    
    for table in tables:
        for record in table.records:
            record_count += 1
            utc_dt = record.get_time()
            local_dt = utc_dt.astimezone(target_tz)
            
            fake_utc_dt = datetime(
                local_dt.year, local_dt.month, local_dt.day,
                local_dt.hour, local_dt.minute, local_dt.second,
                microsecond=local_dt.microsecond,
                tzinfo=dt_timezone.utc
            )
            unix_timestamp_for_chart = fake_utc_dt.timestamp()

            ## FIX: Changed back from .get('field') to ['field'] for FluxRecord objects.
            candles.append(schemas.Candle(
                timestamp=utc_dt,
                open=record['open'],
                high=record['high'],
                low=record['low'],
                close=record['close'],
                volume=record['volume'],
                unix_timestamp=unix_timestamp_for_chart
            ))
    
    process_time = time.time() - process_start
    total_time = time.time() - start_time
    
    logging.info(f"InfluxDB Query Performance: Query execution: {query_time:.3f}s, Data processing: {process_time:.3f}s, Total: {total_time:.3f}s, Records: {record_count}")
    
    return candles

def _calculate_heikin_ashi(regular_candles: List[schemas.Candle]) -> List[schemas.HeikinAshiCandle]:
    """Calculates a full set of Heikin Ashi candles from regular candles."""
    if not regular_candles:
        return []

    ha_candles = []
    # Initial HA candle calculation
    first_candle = regular_candles[0]
    ha_close = (first_candle.open + first_candle.high + first_candle.low + first_candle.close) / 4
    ha_open = (first_candle.open + first_candle.close) / 2
    ha_high = max(first_candle.high, ha_open, ha_close)
    ha_low = min(first_candle.low, ha_open, ha_close)

    ha_candles.append(schemas.HeikinAshiCandle(
        open=ha_open, high=ha_high, low=ha_low, close=ha_close,
        volume=first_candle.volume, unix_timestamp=first_candle.unix_timestamp,
        regular_open=first_candle.open, regular_close=first_candle.close
    ))
    prev_ha_open, prev_ha_close = ha_open, ha_close

    # Subsequent candles
    for candle in regular_candles[1:]:
        ha_close = (candle.open + candle.high + candle.low + candle.close) / 4
        ha_open = (prev_ha_open + prev_ha_close) / 2
        ha_high = max(candle.high, ha_open, ha_close)
        ha_low = min(candle.low, ha_open, ha_close)

        ha_candles.append(schemas.HeikinAshiCandle(
            open=ha_open, high=ha_high, low=ha_low, close=ha_close,
            volume=candle.volume, unix_timestamp=candle.unix_timestamp,
            regular_open=candle.open, regular_close=candle.close
        ))
        prev_ha_open, prev_ha_close = ha_open, ha_close

    return ha_candles

# --- Main Service Functions ---

def get_historical_data(
    session_token: str, exchange: str, token: str, interval_val: str,
    start_time: datetime, end_time: datetime, timezone: str, data_type: schemas.DataType
) -> Union[schemas.HistoricalDataResponse, schemas.TickDataResponse, schemas.HeikinAshiDataResponse]:
    """Unified function to get all types of historical data."""
    
    ## CHANGE: Route REGULAR data to the new cursor-based method for efficiency.
    if data_type in [schemas.DataType.TICK, schemas.DataType.REGULAR]:
        return _get_initial_cursor_based_data(
            token=token, interval_val=interval_val, start_time=start_time,
            end_time=end_time, timezone=timezone, data_type=data_type
        )

    if data_type == schemas.DataType.HEIKIN_ASHI:
        return _get_initial_heikin_ashi_data(
            session_token=session_token, exchange=exchange, token=token, interval_val=interval_val,
            start_time=start_time, end_time=end_time, timezone=timezone
        )

    raise HTTPException(status_code=400, detail=f"Unsupported data_type: {data_type}")


def get_historical_chunk(
    request_id: str,
    offset: Optional[int], # ## NOTE: Offset is now only used for Heikin Ashi.
    limit: int,
    data_type: schemas.DataType
) -> Union[schemas.HistoricalDataChunkResponse, schemas.TickDataChunkResponse, schemas.HeikinAshiDataChunkResponse]:
    """Unified function to get chunks for all data types."""

    ## CHANGE: Route REGULAR data to the cursor-based chunk method.
    if data_type in [schemas.DataType.TICK, schemas.DataType.REGULAR]:
        # For tick/regular data, 'request_id' is a cursor and 'offset' is ignored.
        return _get_cursor_based_chunk(request_id, limit)

    if data_type == schemas.DataType.HEIKIN_ASHI:
        if offset is None:
            raise HTTPException(status_code=400, detail="Offset is required for Heikin Ashi data type.")
        return _get_heikin_ashi_chunk(request_id, offset, limit)

    raise HTTPException(status_code=400, detail=f"Unsupported data_type: {data_type}")

# --- Internal Logic for Specific Data Types ---

## CHANGE: New function to fetch the FULL dataset, used ONLY for Heikin Ashi calculation.
def _fetch_and_cache_full_ohlc(
    request_id: str, exchange: str, token: str, interval_val: str,
    start_time: datetime, end_time: datetime, timezone: str
) -> List[schemas.Candle]:
    """
    Fetches the complete OHLC dataset for a given range and caches it.
    This is an expensive operation and should only be used when the full dataset is
    required, such as for calculating Heikin Ashi candles.
    """
    full_data = get_cached_ohlc_data(request_id)
    if full_data:
        logging.info(f"Cache HIT for FULL OHLC data: {request_id}")
        return full_data

    logging.info(f"Cache MISS for FULL OHLC data: {request_id}. Querying InfluxDB.")
    start_utc = start_time.astimezone(dt_timezone.utc)
    end_utc = end_time.astimezone(dt_timezone.utc)

    # This query scans the entire time range.
    flux_query = f"""
        from(bucket: "{settings.INFLUX_BUCKET}")
          |> range(start: {start_utc.isoformat()}, stop: {end_utc.isoformat()})
          |> filter(fn: (r) => r._measurement == "ohlc_{interval_val}")
          |> filter(fn: (r) => r.symbol == "{token}")
          |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
          |> sort(columns: ["_time"])
    """
    
    fetch_start = time.time()
    full_data = _query_and_process_influx_data(flux_query, timezone)
    fetch_time = time.time() - fetch_start
    logging.info(f"InfluxDB full fetch completed in {fetch_time:.3f}s for {len(full_data)} records")

    if full_data:
        set_cached_ohlc_data(request_id, full_data, expiration=CACHE_EXPIRATION_SECONDS)
        logging.info(f"Cache SET for FULL OHLC data: {request_id} with {len(full_data)} records.")

    return full_data


## CHANGE: Renamed and generalized from _get_initial_tick_data. Handles both TICK and REGULAR.
def _get_initial_cursor_based_data(
    token: str, interval_val: str, start_time: datetime, end_time: datetime, timezone: str, data_type: schemas.DataType
) -> schemas.TickDataResponse:
    """
    Handles fetching the initial, most recent page of data for cursor-based pagination.
    This is highly efficient as it uses `limit()` in the database.
    """
    start_utc = start_time.astimezone(dt_timezone.utc)
    end_utc = end_time.astimezone(dt_timezone.utc)
    measurement = f"ohlc_{interval_val}"

    ## CHANGE: The query is now optimized to only get the last N records.
    flux_query = f"""
        from(bucket: "{settings.INFLUX_BUCKET}")
          |> range(start: {start_utc.isoformat()}, stop: {end_utc.isoformat()})
          |> filter(fn: (r) => r._measurement == "{measurement}" and r.symbol == "{token}")
          |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
          |> sort(columns: ["_time"], desc: true)
          |> limit(n: {INITIAL_FETCH_LIMIT})
          |> sort(columns: ["_time"])
    """
    
    candles_to_send = _query_and_process_influx_data(flux_query, timezone)

    if not candles_to_send:
        return schemas.TickDataResponse(candles=[], is_partial=False, message="No data available for this range.", request_id=None)

    next_cursor, is_partial = None, False
    if len(candles_to_send) == INITIAL_FETCH_LIMIT:
        earliest_time = candles_to_send[0].timestamp
        if earliest_time > start_utc:
            is_partial = True
            cursor_data = {
                "token": token, "interval": interval_val,
                "start_time_iso": start_utc.isoformat(), "cursor_iso": earliest_time.isoformat(),
                "timezone": timezone
            }
            next_cursor = base64.urlsafe_b64encode(json.dumps(cursor_data).encode()).decode()

    # We can reuse the TickDataResponse schema for any cursor-based data.
    return schemas.TickDataResponse(
        request_id=next_cursor,
        candles=candles_to_send,
        is_partial=is_partial,
        message=f"Loaded last {len(candles_to_send)} {data_type.value} bars."
    )

## CHANGE: Renamed and generalized from _get_tick_data_chunk.
def _get_cursor_based_chunk(request_id: str, limit: int) -> schemas.TickDataChunkResponse:
    """Handles fetching subsequent pages for cursor-based data (Tick and Regular)."""
    try:
        cursor_data = json.loads(base64.urlsafe_b64decode(request_id).decode())
        original_start_utc = datetime.fromisoformat(cursor_data['start_time_iso'])
        # The new end time for the query is the timestamp from the cursor.
        end_utc = datetime.fromisoformat(cursor_data['cursor_iso'])
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid cursor.")

    measurement = f"ohlc_{cursor_data['interval']}"
    flux_query = f"""
        from(bucket: "{settings.INFLUX_BUCKET}")
          |> range(start: {original_start_utc.isoformat()}, stop: {end_utc.isoformat()})
          |> filter(fn: (r) => r._measurement == "{measurement}" and r.symbol == "{cursor_data['token']}")
          |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
          |> sort(columns: ["_time"], desc: true)
          |> limit(n: {limit})
          |> sort(columns: ["_time"])
    """
    
    candles_to_send = _query_and_process_influx_data(flux_query, cursor_data['timezone'])

    if not candles_to_send:
        return schemas.TickDataChunkResponse(candles=[], is_partial=False, request_id=None)

    next_cursor, is_partial = None, False
    if len(candles_to_send) == limit:
        earliest_time = candles_to_send[0].timestamp
        if earliest_time > original_start_utc:
            is_partial = True
            new_cursor_data = {**cursor_data, "cursor_iso": earliest_time.isoformat()}
            next_cursor = base64.urlsafe_b64encode(json.dumps(new_cursor_data).encode()).decode()
    
    # We can reuse the TickDataChunkResponse schema.
    return schemas.TickDataChunkResponse(request_id=next_cursor, candles=candles_to_send, is_partial=is_partial)


## CHANGE: This is now the initial fetch function specifically for Heikin Ashi.
def _get_initial_heikin_ashi_data(
    session_token: str, exchange: str, token: str, interval_val: str, start_time: datetime, end_time: datetime, timezone: str
) -> schemas.HeikinAshiDataResponse:
    """Calculates and caches Heikin Ashi data, then returns the last N records."""
    
    # Build a cache key for the full underlying regular OHLC data.
    ohlc_request_id = build_ohlc_cache_key(
        candle_type="regular_full", session_token=session_token, exchange=exchange, token=token, 
        interval=interval_val, start_time=start_time, end_time=end_time, timezone=timezone
    )
    # Build a cache key for the calculated Heikin Ashi data.
    ha_request_id = build_heikin_ashi_cache_key(
        session_token=session_token, exchange=exchange, token=token, interval=interval_val, 
        start_time=start_time, end_time=end_time, timezone=timezone
    )

    full_ha_data = get_cached_heikin_ashi_data(ha_request_id)
    if not full_ha_data:
        logging.info(f"Cache MISS for HEIKIN ASHI data: {ha_request_id}. Calculating...")
        
        # This is the key change: we call the function that performs the full, expensive scan.
        full_regular_data = _fetch_and_cache_full_ohlc(
            request_id=ohlc_request_id, exchange=exchange, token=token, interval_val=interval_val,
            start_time=start_time, end_time=end_time, timezone=timezone
        )

        if not full_regular_data:
            return schemas.HeikinAshiDataResponse(candles=[], total_available=0, is_partial=False, message="No underlying data available to calculate Heikin Ashi.", request_id=None, offset=None)

        full_ha_data = _calculate_heikin_ashi(full_regular_data)
        set_cached_heikin_ashi_data(ha_request_id, full_ha_data, expiration=CACHE_EXPIRATION_SECONDS)
        logging.info(f"Cache SET for HEIKIN ASHI data: {ha_request_id} with {len(full_ha_data)} candles.")

    total_available = len(full_ha_data)
    initial_offset = max(0, total_available - INITIAL_FETCH_LIMIT)
    candles_to_send = full_ha_data[initial_offset:]

    return schemas.HeikinAshiDataResponse(
        request_id=ha_request_id,
        candles=candles_to_send,
        offset=initial_offset,
        total_available=total_available,
        is_partial=(total_available > len(candles_to_send)),
        message=f"Heikin Ashi data loaded. Displaying last {len(candles_to_send)} of {total_available} candles."
    )

## CHANGE: This function now exclusively serves chunks for Heikin Ashi from the cache.
def _get_heikin_ashi_chunk(request_id: str, offset: int, limit: int) -> schemas.HeikinAshiDataChunkResponse:
    """Fetches a chunk of Heikin Ashi data from the cache using offset and limit."""
    full_ha_data = get_cached_heikin_ashi_data(request_id)
    if not full_ha_data:
        raise HTTPException(status_code=404, detail="Heikin Ashi data for this request not found or has expired.")

    total = len(full_ha_data)
    # Ensure offset is within bounds
    start_index = max(0, offset)
    end_index = min(start_index + limit, total)
    chunk = full_ha_data[start_index:end_index]

    return schemas.HeikinAshiDataChunkResponse(candles=chunk, offset=offset, limit=limit, total_available=total)