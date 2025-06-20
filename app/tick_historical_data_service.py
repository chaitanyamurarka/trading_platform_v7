import logging
from datetime import datetime, timezone as dt_timezone
from typing import List, Optional
from fastapi import HTTPException
import json
import base64

try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo

from . import schemas
from .config import settings
from .historical_data_service import query_api, INITIAL_FETCH_LIMIT

def _query_and_process_ticks(flux_query: str, timezone_str: str) -> List[schemas.Candle]:
    """Helper function to execute a Flux query and process the results into Candle objects."""
    tables = query_api.query(query=flux_query)
    candles = []
    target_tz = ZoneInfo(timezone_str)

    for table in tables:
        for record in table.records:
            utc_dt = record.get_time()
            local_dt = utc_dt.astimezone(target_tz)
            fake_utc_dt = datetime(
                local_dt.year, local_dt.month, local_dt.day,
                local_dt.hour, local_dt.minute, local_dt.second,
                microsecond=local_dt.microsecond,
                tzinfo=dt_timezone.utc
            )
            unix_timestamp_for_chart = fake_utc_dt.timestamp()

            candles.append(schemas.Candle(
                timestamp=utc_dt,
                open=record['open'],
                high=record['high'],
                low=record['low'],
                close=record['close'],
                volume=record['volume'],
                unix_timestamp=unix_timestamp_for_chart
            ))
    return candles

def get_initial_tick_data(
    session_token: str,
    exchange: str,
    token: str,
    interval_val: str,
    start_time: datetime,
    end_time: datetime,
    timezone: str,
) -> schemas.TickDataResponse:
    """
    Fetches the latest chunk of aggregated tick data using cursor-based pagination.
    """
    logging.info(f"[{token}] Performing initial InfluxDB query for latest {INITIAL_FETCH_LIMIT} tick bars.")
    
    try:
        client_tz = ZoneInfo(timezone)
    except Exception:
        client_tz = ZoneInfo("UTC")

    start_time_utc = start_time.astimezone(dt_timezone.utc) if start_time.tzinfo else start_time.replace(tzinfo=client_tz).astimezone(dt_timezone.utc)
    end_time_utc = end_time.astimezone(dt_timezone.utc) if end_time.tzinfo else end_time.replace(tzinfo=client_tz).astimezone(dt_timezone.utc)

    measurement = f"ohlc_{interval_val}"
    
    flux_query = f"""
        from(bucket: "{settings.INFLUX_BUCKET}")
          |> range(start: {start_time_utc.isoformat().replace('+00:00', 'Z')}, stop: {end_time_utc.isoformat().replace('+00:00', 'Z')})
          |> filter(fn: (r) => r._measurement == "{measurement}")
          |> filter(fn: (r) => r.symbol == "{token}")
          |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
          |> sort(columns: ["_time"], desc: true)
          |> limit(n: {INITIAL_FETCH_LIMIT})
          |> sort(columns: ["_time"], desc: false)
    """
    
    try:
        candles_to_send = _query_and_process_ticks(flux_query, timezone)

        if not candles_to_send:
            return schemas.TickDataResponse(candles=[], is_partial=False, message="No data available for this range.", request_id=None)

        next_cursor = None
        is_partial = False
        
        # If we got a full chunk, there might be more data
        if len(candles_to_send) == INITIAL_FETCH_LIMIT:
            earliest_candle_time = candles_to_send[0].timestamp
            # If the earliest data is still after the user's requested start time, then there is more data to fetch.
            if earliest_candle_time > start_time_utc:
                is_partial = True
                cursor_data = {
                    "exchange": exchange,
                    "token": token,
                    "interval": interval_val,
                    "start_time_iso": start_time_utc.isoformat(),
                    "cursor_iso": earliest_candle_time.isoformat(),
                    "timezone": timezone,
                }
                # The request_id is now our cursor for the next page
                next_cursor = base64.urlsafe_b64encode(json.dumps(cursor_data).encode()).decode()

        return schemas.TickDataResponse(
            request_id=next_cursor,
            candles=candles_to_send,
            is_partial=is_partial,
            message=f"Loaded last {len(candles_to_send)} tick bars."
        )

    except Exception as e:
        logging.error(f"Error querying InfluxDB for initial tick data: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to retrieve tick data from database.")


def get_tick_data_chunk(
    request_id: str,
    limit: int = 5000
) -> schemas.TickDataChunkResponse:
    """
    Retrieves a subsequent chunk of tick data using a cursor.
    """
    if not request_id:
        raise HTTPException(status_code=400, detail="Invalid request_id (cursor) for tick data chunk.")
        
    try:
        cursor_data = json.loads(base64.urlsafe_b64decode(request_id).decode())
        
        token = cursor_data["token"]
        measurement = f"ohlc_{cursor_data['interval']}"
        original_start_time_utc = datetime.fromisoformat(cursor_data['start_time_iso'])
        # The new end_time for the query is the cursor from the previous request
        end_time_utc = datetime.fromisoformat(cursor_data['cursor_iso'])
        timezone = cursor_data["timezone"]

    except (json.JSONDecodeError, KeyError, TypeError):
        raise HTTPException(status_code=400, detail="Invalid or malformed request_id (cursor).")

    flux_query = f"""
        from(bucket: "{settings.INFLUX_BUCKET}")
          |> range(start: {original_start_time_utc.isoformat().replace('+00:00', 'Z')}, stop: {end_time_utc.isoformat().replace('+00:00', 'Z')})
          |> filter(fn: (r) => r._measurement == "{measurement}")
          |> filter(fn: (r) => r.symbol == "{token}")
          |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
          |> sort(columns: ["_time"], desc: true)
          |> limit(n: {limit})
          |> sort(columns: ["_time"], desc: false)
    """
    
    try:
        candles_to_send = _query_and_process_ticks(flux_query, timezone)
        
        if not candles_to_send:
            return schemas.TickDataChunkResponse(candles=[], is_partial=False, request_id=None)
        
        next_cursor = None
        is_partial = False
        
        if len(candles_to_send) == limit:
            earliest_candle_time = candles_to_send[0].timestamp
            if earliest_candle_time > original_start_time_utc:
                is_partial = True
                new_cursor_data = {**cursor_data, "cursor_iso": earliest_candle_time.isoformat()}
                next_cursor = base64.urlsafe_b64encode(json.dumps(new_cursor_data).encode()).decode()

        return schemas.TickDataChunkResponse(
            request_id=next_cursor,
            candles=candles_to_send,
            is_partial=is_partial
        )
    except Exception as e:
        logging.error(f"Error querying InfluxDB for tick data chunk: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to retrieve tick data chunk.")