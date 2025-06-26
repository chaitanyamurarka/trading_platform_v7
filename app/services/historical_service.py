import logging
import json
import base64
import time
import pandas as pd
import re
from datetime import datetime, timezone as dt_timezone, timedelta
from typing import List, Optional, Union, Dict, Any
from fastapi import HTTPException
from zoneinfo import ZoneInfo

from .. import schemas
from ..config import settings
from influxdb_client import InfluxDBClient

# --- InfluxDB Client Setup ---
influx_client = InfluxDBClient(url=settings.INFLUX_URL, token=settings.INFLUX_TOKEN, org=settings.INFLUX_ORG, timeout=60_000)
query_api = influx_client.query_api()
INITIAL_FETCH_LIMIT = 5000

# --- Internal Helper Functions ---

def _query_and_process_influx_data(flux_query: str, timezone_str: str) -> List[schemas.Candle]:
    """Helper to run a Flux query and convert results to Candle schemas."""
    logging.info(f"Executing Flux Query:\n{flux_query}")
    try:
        target_tz = ZoneInfo(timezone_str)
    except Exception:
        target_tz = ZoneInfo("UTC")

    tables = query_api.query(query=flux_query)
    candles = []
    
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
                volume=int(record['volume']),
                unix_timestamp=unix_timestamp_for_chart
            ))

    candles.reverse()
    return candles

def _calculate_heikin_ashi_chunk(regular_candles: List[schemas.Candle], prev_ha_candle: Optional[schemas.HeikinAshiCandle] = None) -> List[schemas.HeikinAshiCandle]:
    if not regular_candles: return []
    ha_candles = []
    if prev_ha_candle:
        prev_ha_open, prev_ha_close = prev_ha_candle.open, prev_ha_candle.close
    else:
        first_candle = regular_candles[0]
        prev_ha_open = (first_candle.open + first_candle.close) / 2
        prev_ha_close = (first_candle.open + first_candle.high + first_candle.low + first_candle.close) / 4
    for candle in regular_candles:
        ha_close = (candle.open + candle.high + candle.low + candle.close) / 4
        ha_open = (prev_ha_open + prev_ha_close) / 2
        ha_high = max(candle.high, ha_open, ha_close)
        ha_low = min(candle.low, ha_open, ha_close)
        ha_candles.append(schemas.HeikinAshiCandle(open=ha_open, high=ha_high, low=ha_low, close=ha_close, volume=candle.volume, unix_timestamp=candle.unix_timestamp, regular_open=candle.open, regular_close=candle.close))
        prev_ha_open, prev_ha_close = ha_open, ha_close
    return ha_candles

def _fetch_data_full_range(token: str, interval_val: str, start_utc: datetime, end_utc: datetime, timezone: str, limit: int) -> tuple[List[schemas.Candle], Optional[datetime]]:
    """Fetches data by querying all measurements in the date range at once."""
    logging.info("Using full-range fetch strategy for low-frequency data.")
    et_zone = ZoneInfo("America/New_York")
    start_et, end_et = start_utc.astimezone(et_zone), end_utc.astimezone(et_zone)
    date_range = pd.date_range(start=start_et.date(), end=end_et.date(), freq='D')
    date_regex_part = "|".join([day.strftime('%Y%m%d') for day in date_range])
    if not date_regex_part: return [], None
    
    sanitized_token = re.escape(token)
    measurement_regex = f"^ohlc_{sanitized_token}_({date_regex_part})_{interval_val}$"

    flux_query = f"""
        from(bucket: "{settings.INFLUX_BUCKET}")
          |> range(start: {start_utc.isoformat()}, stop: {end_utc.isoformat()})
          |> filter(fn: (r) => r._measurement =~ /{measurement_regex}/ and r.symbol == "{token}")
          |> drop(columns: ["_measurement", "_start", "_stop"])
          |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
          |> sort(columns: ["_time"], desc: true)
          |> limit(n: {limit})
    """
    
    candles = _query_and_process_influx_data(flux_query, timezone)
    
    # FIXED: For the next chunk, we need to start from just before the oldest candle we found
    next_cursor_timestamp = None
    if len(candles) >= limit and candles[0].timestamp > start_utc:
        # Set next cursor to be just before the oldest candle (1 microsecond earlier)
        next_cursor_timestamp = candles[0].timestamp - timedelta(microseconds=1)
    
    return candles, next_cursor_timestamp

def _fetch_data_day_by_day(token: str, interval_val: str, start_utc: datetime, end_utc: datetime, timezone_str: str, limit: int) -> tuple[List[schemas.Candle], Optional[datetime]]:
    """
    Correctly fetches data by querying day-by-day, newest to oldest, until the limit is reached.
    """
    logging.info("Using day-by-day fetch strategy for high-frequency data.")
    all_candles = []
    
    et_zone = ZoneInfo("America/New_York")
    start_et = start_utc.astimezone(et_zone)
    end_et = end_utc.astimezone(et_zone)
    
    # Create a range of dates to iterate through, from newest to oldest.
    date_range = pd.date_range(start=start_et.date(), end=end_et.date(), freq='D').sort_values(ascending=False)
    
    oldest_timestamp_found = None

    for day in date_range:
        remaining_limit = limit - len(all_candles)
        
        # Define the time range for this specific day's query
        day_start_et = datetime.combine(day, datetime.min.time(), tzinfo=et_zone)
        day_end_et = day_start_et + timedelta(days=1)
        
        # Make sure we don't query beyond our actual boundaries
        query_start = max(day_start_et.astimezone(dt_timezone.utc), start_utc)
        query_end = min(day_end_et.astimezone(dt_timezone.utc), end_utc)
        
        measurement_name = f"ohlc_{token}_{day.strftime('%Y%m%d')}_{interval_val}"
        
        flux_query = f"""
            from(bucket: "{settings.INFLUX_BUCKET}")
              |> range(start: {query_start.isoformat()}, stop: {query_end.isoformat()})
              |> filter(fn: (r) => r._measurement == "{measurement_name}" and r.symbol == "{token}")
              |> drop(columns: ["_measurement", "_start", "_stop"])
              |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
              |> sort(columns: ["_time"], desc: true)
              |> limit(n: {remaining_limit})
        """
        
        daily_candles = _query_and_process_influx_data(flux_query, timezone_str)
        if daily_candles:
            # Track the oldest timestamp we've found
            if oldest_timestamp_found is None or daily_candles[0].timestamp < oldest_timestamp_found:
                oldest_timestamp_found = daily_candles[0].timestamp
            
            # Prepend the daily candles to keep the list roughly sorted
            all_candles = daily_candles + all_candles
        
        if len(all_candles) >= limit:
            logging.info(f"Limit of {limit} reached. Stopping day-by-day fetch.")
            break
            
    # Final sort to ensure perfect ascending order
    all_candles.sort(key=lambda c: c.timestamp)

    # FIXED: For the next chunk, we need to start from just before the oldest candle we found
    next_cursor_timestamp = None
    if len(all_candles) >= limit and oldest_timestamp_found and oldest_timestamp_found > start_utc:
        # Set next cursor to be just before the oldest candle (1 microsecond earlier)
        next_cursor_timestamp = oldest_timestamp_found - timedelta(microseconds=1)

    return all_candles, next_cursor_timestamp

def _create_cursor(original_start_iso: str, original_end_iso: str, next_start_iso: Optional[str], data_type: schemas.DataType, token: str, interval: str, timezone: str, last_ha_candle: Optional[schemas.HeikinAshiCandle] = None) -> Optional[str]:
    """
    FIXED: Now includes both original boundaries AND the next start point for pagination
    """
    if next_start_iso is None:
        return None
        
    cursor_data: Dict[str, Any] = {
        "original_start_iso": original_start_iso, 
        "original_end_iso": original_end_iso,
        "next_start_iso": next_start_iso,
        "data_type": data_type.value, 
        "token": token, 
        "interval": interval, 
        "timezone": timezone
    }
    if last_ha_candle:
        cursor_data['last_ha_candle'] = last_ha_candle.model_dump_json()
    return base64.urlsafe_b64encode(json.dumps(cursor_data).encode()).decode()

def get_historical_data(session_token: str, exchange: str, token: str, interval_val: str, start_time: datetime, end_time: datetime, timezone: str, data_type: schemas.DataType) -> Union[schemas.HistoricalDataResponse, schemas.HeikinAshiDataResponse]:
    start_utc, end_utc = start_time.astimezone(dt_timezone.utc), end_time.astimezone(dt_timezone.utc)
    
    is_high_frequency = interval_val.endswith('s') or interval_val.endswith('tick')
    if is_high_frequency:
        regular_candles, next_cursor_timestamp = _fetch_data_day_by_day(token, interval_val, start_utc, end_utc, timezone, INITIAL_FETCH_LIMIT)
    else:
        regular_candles, next_cursor_timestamp = _fetch_data_full_range(token, interval_val, start_utc, end_utc, timezone, INITIAL_FETCH_LIMIT)

    if not regular_candles:
        return schemas.HistoricalDataResponse(candles=[], is_partial=False, message="No data available for this range.", request_id=None)
    
    is_partial = next_cursor_timestamp is not None
    if data_type == schemas.DataType.HEIKIN_ASHI:
        ha_candles = _calculate_heikin_ashi_chunk(regular_candles)
        next_cursor = _create_cursor(
            start_utc.isoformat(), 
            end_utc.isoformat(), 
            next_cursor_timestamp.isoformat() if next_cursor_timestamp else None,
            data_type, 
            token, 
            interval_val, 
            timezone, 
            ha_candles[-1] if ha_candles else None
        )
        return schemas.HeikinAshiDataResponse(request_id=next_cursor, candles=ha_candles, is_partial=is_partial, message=f"Loaded initial {len(ha_candles)} bars.")
    else:
        next_cursor = _create_cursor(
            start_utc.isoformat(), 
            end_utc.isoformat(),
            next_cursor_timestamp.isoformat() if next_cursor_timestamp else None,
            data_type, 
            token, 
            interval_val, 
            timezone
        )
        return schemas.HistoricalDataResponse(request_id=next_cursor, candles=regular_candles, is_partial=is_partial, message=f"Loaded initial {len(regular_candles)} bars.")

def get_historical_chunk(request_id: str, offset: Optional[int], limit: int, data_type: schemas.DataType) -> Union[schemas.HistoricalDataChunkResponse, schemas.HeikinAshiDataChunkResponse]:
    try:
        cursor_data = json.loads(base64.urlsafe_b64decode(request_id).decode())
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid request_id cursor.")
    
    # FIXED: Now we use the next_start as our new end boundary, and original_start remains the same
    original_start_utc = datetime.fromisoformat(cursor_data['original_start_iso'])
    next_start_utc = datetime.fromisoformat(cursor_data['next_start_iso'])
    interval_val = cursor_data['interval']

    # The next chunk queries from original start to the new boundary (exclusive of already fetched data)
    is_high_frequency = interval_val.endswith('s') or interval_val.endswith('tick')
    if is_high_frequency:
        regular_candles, next_cursor_timestamp = _fetch_data_day_by_day(
            cursor_data['token'], 
            interval_val, 
            original_start_utc, 
            next_start_utc,  # FIXED: Use next_start as the end boundary
            cursor_data['timezone'], 
            limit
        )
    else:
        regular_candles, next_cursor_timestamp = _fetch_data_full_range(
            cursor_data['token'], 
            interval_val, 
            original_start_utc, 
            next_start_utc,  # FIXED: Use next_start as the end boundary
            cursor_data['timezone'], 
            limit
        )
        
    if not regular_candles:
        if data_type == schemas.DataType.HEIKIN_ASHI:
            return schemas.HeikinAshiDataChunkResponse(candles=[], request_id=None, is_partial=False, limit=limit)
        else:
            return schemas.HistoricalDataChunkResponse(candles=[], request_id=None, is_partial=False, limit=limit)

    is_partial = next_cursor_timestamp is not None
    if data_type == schemas.DataType.HEIKIN_ASHI:
        prev_ha_candle = schemas.HeikinAshiCandle.model_validate_json(cursor_data.get('last_ha_candle')) if 'last_ha_candle' in cursor_data else None
        ha_candles = _calculate_heikin_ashi_chunk(regular_candles, prev_ha_candle)
        next_cursor = _create_cursor(
            cursor_data['original_start_iso'], 
            cursor_data['original_end_iso'],
            next_cursor_timestamp.isoformat() if next_cursor_timestamp else None,
            data_type, 
            cursor_data['token'], 
            interval_val, 
            cursor_data['timezone'], 
            ha_candles[-1] if ha_candles else None
        )
        return schemas.HeikinAshiDataChunkResponse(candles=ha_candles, request_id=next_cursor, is_partial=is_partial, limit=limit)
    else:
        next_cursor = _create_cursor(
            cursor_data['original_start_iso'], 
            cursor_data['original_end_iso'],
            next_cursor_timestamp.isoformat() if next_cursor_timestamp else None,
            data_type, 
            cursor_data['token'], 
            interval_val, 
            cursor_data['timezone']
        )
        return schemas.HistoricalDataChunkResponse(candles=regular_candles, request_id=next_cursor, is_partial=is_partial, limit=limit)