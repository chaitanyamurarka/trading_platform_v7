import logging
from datetime import datetime, timezone as dt_timezone
from typing import List
from fastapi import HTTPException
try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo

from . import schemas
from .config import settings
from influxdb_client import InfluxDBClient

# Re-use the existing InfluxDB client, query_api, and fetch limit constant
from .historical_data_service import influx_client, query_api, INITIAL_FETCH_LIMIT

def get_initial_tick_data(
    session_token: str,
    exchange: str,
    token: str,
    interval_val: str, # e.g., "1tick", "10tick"
    start_time: datetime,
    end_time: datetime,
    timezone: str,
) -> schemas.HistoricalDataResponse:
    """
    Fetches the latest chunk of aggregated tick data directly from InfluxDB,
    bypassing the Redis cache.
    """
    logging.info(f"[{token}] Performing direct InfluxDB query for latest {INITIAL_FETCH_LIMIT} tick bars.")
    
    try:
        client_tz = ZoneInfo(timezone)
    except Exception:
        logging.warning(f"Invalid timezone '{timezone}' provided. Defaulting to UTC.")
        client_tz = ZoneInfo("UTC")

    start_time_utc = start_time.replace(tzinfo=client_tz).astimezone(dt_timezone.utc)
    end_time_utc = end_time.replace(tzinfo=client_tz).astimezone(dt_timezone.utc)

    try:
        # Dynamically determine the measurement based on the interval
        measurement = f"ohlc_{interval_val}"
        logging.info(f"Querying measurement: {measurement}")
        
        # --- MODIFIED FLUX QUERY ---
        # This query now sorts descending, takes the most recent N records,
        # and then sorts again ascending to return them in the correct order for charting.
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
        
        tables = query_api.query(query=flux_query)
        
        candles_to_send = []
        target_tz = ZoneInfo(timezone) # Re-get target_tz for safety

        for table in tables:
            for record in table.records:
                utc_dt = record.get_time()
                local_dt = utc_dt.astimezone(target_tz)
                # Create a "fake" UTC datetime to make the charting library display local time correctly
                fake_utc_dt = datetime(
                    local_dt.year, local_dt.month, local_dt.day,
                    local_dt.hour, local_dt.minute, local_dt.second,
                    microsecond=local_dt.microsecond,
                    tzinfo=dt_timezone.utc
                )
                unix_timestamp_for_chart = fake_utc_dt.timestamp()

                candles_to_send.append(schemas.Candle(
                    open=record['open'],
                    high=record['high'],
                    low=record['low'],
                    close=record['close'],
                    volume=record['volume'],
                    unix_timestamp=unix_timestamp_for_chart
                ))

        if not candles_to_send:
            return schemas.HistoricalDataResponse(candles=[], total_available=0, is_partial=False, message=f"No data available in InfluxDB for {measurement}.", request_id=None, offset=None)
        
        total_available = len(candles_to_send)
        
        # --- MODIFIED RESPONSE ---
        # Since we are not caching or planning to paginate, we return the data as a complete set.
        return schemas.HistoricalDataResponse(
            request_id=None,  # No request_id as there's no cache
            candles=candles_to_send,
            offset=0,
            total_available=total_available,
            is_partial=False,  # Signal to the frontend that this is all the data
            message=f"Direct query loaded last {total_available} tick bars."
        )

    except Exception as e:
        logging.error(f"Error querying InfluxDB for tick data: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to retrieve tick data from database.")