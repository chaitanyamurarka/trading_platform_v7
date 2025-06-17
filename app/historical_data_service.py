# In app/historical_data_service.py

import logging
from datetime import datetime, timezone as dt_timezone
from typing import List
from fastapi import HTTPException
# Use zoneinfo for timezone conversions (available in Python 3.9+, backports for older versions)
try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo

from . import schemas
# Use the new centralized cache key builder
from .cache import get_cached_ohlc_data, set_cached_ohlc_data, build_ohlc_cache_key, CACHE_EXPIRATION_SECONDS
from .config import settings
from influxdb_client import InfluxDBClient

# --- InfluxDB Client Setup ---
influx_client = InfluxDBClient(
    url=settings.INFLUX_URL,
    token=settings.INFLUX_TOKEN,
    org=settings.INFLUX_ORG,
    timeout=20_000 # 20 seconds
)
query_api = influx_client.query_api()

INITIAL_FETCH_LIMIT = 5000

def get_initial_historical_data(
    session_token: str,
    exchange: str,
    token: str,
    interval_val: str,
    start_time: datetime,
    end_time: datetime,
    timezone: str,
) -> schemas.HistoricalDataResponse:
    """
    Main entry point for fetching historical data. It now correctly handles timezones for queries.
    """
    # =================================================================
    # --- NEW FIX: Make naive datetimes from the request timezone-aware ---
    # =================================================================
    try:
        # The timezone provided by the client (e.g., 'America/New_York')
        client_tz = ZoneInfo(timezone)
    except Exception:
        logging.warning(f"Invalid timezone '{timezone}' provided by client. Defaulting to UTC.")
        client_tz = ZoneInfo("UTC")

    # The incoming start_time and end_time from FastAPI are naive.
    # We must first make them "aware" of the client's timezone, then convert
    # them to UTC for the InfluxDB query.
    start_time_aware = start_time.replace(tzinfo=client_tz)
    end_time_aware = end_time.replace(tzinfo=client_tz)

    start_time_utc = start_time_aware.astimezone(dt_timezone.utc)
    end_time_utc = end_time_aware.astimezone(dt_timezone.utc)
    # --- END FIX ---


    # Use the centralized function to build the cache key
    request_id = build_ohlc_cache_key(
        exchange=exchange,
        token=token,
        interval=interval_val,
        start_time_iso=start_time.isoformat(),
        end_time_iso=end_time.isoformat(),
        timezone=timezone,
        session_token=session_token
    )
    
    full_data = get_cached_ohlc_data(request_id)
    
    if not full_data:
        logging.info(f"Cache MISS for {request_id}. Querying InfluxDB...")
        try:
            # Use the corrected UTC-aware datetimes in the query, formatting to ISO 8601 with 'Z'
            flux_query = f"""
                from(bucket: "{settings.INFLUX_BUCKET}")
                  |> range(start: {start_time_utc.isoformat().replace('+00:00', 'Z')}, stop: {end_time_utc.isoformat().replace('+00:00', 'Z')})
                  |> filter(fn: (r) => r._measurement == "ohlc_{interval_val}")
                  |> filter(fn: (r) => r.symbol == "{token}")
                  |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
                  |> sort(columns: ["_time"])
            """
            
            tables = query_api.query(query=flux_query)
            
            full_data = []
            
            # This logic for display timezone conversion is correct and remains.
            try:
                target_tz = ZoneInfo(timezone)
            except Exception:
                logging.warning(f"Invalid timezone '{timezone}' provided for display. Defaulting to UTC.")
                target_tz = ZoneInfo("UTC")

            for table in tables:
                for record in table.records:
                    utc_dt = record.get_time()
                    local_dt = utc_dt.astimezone(target_tz)
                    fake_utc_dt = datetime(
                        local_dt.year, local_dt.month, local_dt.day,
                        local_dt.hour, local_dt.minute, local_dt.second,
                        tzinfo=dt_timezone.utc
                    )
                    unix_timestamp_for_chart = fake_utc_dt.timestamp()

                    full_data.append(schemas.Candle(
                        timestamp=utc_dt,
                        open=record['open'],
                        high=record['high'],
                        low=record['low'],
                        close=record['close'],
                        volume=record['volume'],
                        unix_timestamp=unix_timestamp_for_chart
                    ))

            if not full_data:
                return schemas.HistoricalDataResponse(candles=[], total_available=0, is_partial=False, message="No data available in InfluxDB for this range.", request_id=None, offset=None)

            set_cached_ohlc_data(request_id, full_data, expiration=CACHE_EXPIRATION_SECONDS)
            logging.info(f"Cache SET for {request_id} with {len(full_data)} records for {CACHE_EXPIRATION_SECONDS}s.")

        except Exception as e:
            logging.error(f"Error querying InfluxDB or processing data: {e}", exc_info=True)
            raise HTTPException(status_code=500, detail="Failed to retrieve data from database.")

    total_available = len(full_data)
    initial_offset = max(0, total_available - INITIAL_FETCH_LIMIT)
    candles_to_send = full_data[initial_offset:]
    
    return schemas.HistoricalDataResponse(
        request_id=request_id,
        candles=candles_to_send,
        offset=initial_offset,
        total_available=total_available,
        is_partial=(total_available > len(candles_to_send)),
        message=f"Initial data loaded. Displaying last {len(candles_to_send)} of {total_available} candles."
    )

def get_historical_data_chunk(
    request_id: str,
    offset: int,
    limit: int = 5000
) -> schemas.HistoricalDataChunkResponse:
    """
    Retrieves a subsequent chunk of historical data from the cache using the request_id.
    """
    full_data = get_cached_ohlc_data(request_id)
    if full_data is None:
        raise HTTPException(status_code=404, detail="Data for this request not found or has expired.")

    total_available = len(full_data)
    if offset < 0 or offset >= total_available:
        return schemas.HistoricalDataChunkResponse(candles=[], offset=offset, limit=limit, total_available=total_available)
        
    chunk = full_data[offset: offset + limit]
    
    return schemas.HistoricalDataChunkResponse(
        candles=chunk,
        offset=offset,
        limit=limit,
        total_available=total_available
    )