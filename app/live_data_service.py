import asyncio
import json
import logging
from datetime import datetime, timedelta, timezone
from typing import AsyncGenerator, Dict, Optional, List
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
from starlette.websockets import WebSocket, WebSocketDisconnect
from websockets.exceptions import ConnectionClosed

import pandas as pd
import redis.asyncio as aioredis
from fastapi import WebSocket

from . import schemas
from .config import settings

logger = logging.getLogger(__name__)

# Connect to Redis
redis_client = aioredis.from_url(settings.REDIS_URL, decode_responses=True)

class BarResampler:
    """
    Aggregates raw ticks from Redis into OHLCV bars of a specified interval.
    This class is now stateful and designed to be used per client subscription.
    """

    def __init__(self, interval_str: str, timezone_str: str):
        self.interval_str = interval_str
        self.interval_td = self._parse_interval(interval_str)
        self.current_bar: Optional[schemas.Candle] = None
        
        try:
            self.tz = ZoneInfo(timezone_str)
            logger.info(f"BarResampler initialized for interval {interval_str}, timezone: {timezone_str}")
        except ZoneInfoNotFoundError:
            logger.warning(f"Invalid timezone '{timezone_str}', falling back to UTC.")
            self.tz = timezone.utc

    def _parse_interval(self, interval_str: str) -> timedelta:
        """Converts an interval string like '1m', '5s', '1h' to a timedelta."""
        unit = interval_str[-1]
        value = int(interval_str[:-1])
        if unit == 's':
            return timedelta(seconds=value)
        if unit == 'm':
            return timedelta(minutes=value)
        if unit == 'h':
            return timedelta(hours=value)
        raise ValueError(f"Invalid interval format: {interval_str}")

    def _get_bar_start_time(self, dt_utc: datetime) -> datetime:
        """
        Calculates the start time for a bar in the local timezone, aligned
        to the interval, and returns it as a naive datetime object.
        """
        # Convert the UTC tick time to the user's selected timezone
        local_dt = dt_utc.astimezone(self.tz)
        
        interval_seconds = self.interval_td.total_seconds()
        
        # Calculate how many seconds have passed since the beginning of the day in the local timezone
        seconds_past_midnight = local_dt.hour * 3600 + local_dt.minute * 60 + local_dt.second
        
        # Find the start of the current interval bucket
        seconds_into_current_interval = seconds_past_midnight % interval_seconds
        
        bar_start_local_dt = local_dt - timedelta(
            seconds=seconds_into_current_interval,
            microseconds=local_dt.microsecond
        )
        
        # Return as a naive datetime, which the charting library expects
        return bar_start_local_dt.replace(tzinfo=None)

    def add_bar(self, tick_data: Dict) -> Optional[schemas.Candle]:
        """
        Adds a new tick to the resampler. If a bar is completed, it's returned.
        This method was previously named `add_bar`, but now it processes a `tick`.
        The name is kept for compatibility with consumers.
        """
        completed_bar = None
        
        price = float(tick_data['price'])
        volume = int(tick_data['volume'])
        
        # Create a timezone-aware datetime object in UTC from the tick's timestamp
        timestamp_utc = datetime.fromtimestamp(tick_data['timestamp'], tz=timezone.utc)

        # Get the "fake" UTC timestamp for the start of the bar, which the charting library needs
        bar_start_naive = self._get_bar_start_time(timestamp_utc)
        bar_start_unix = bar_start_naive.replace(tzinfo=timezone.utc).timestamp()
        
        if not self.current_bar:
            # This is the first tick for a new bar
            self.current_bar = schemas.Candle(
                open=price, high=price, low=price, close=price, volume=volume,
                unix_timestamp=bar_start_unix
            )
        else:
            # Check if the tick belongs to the current bar or a new one
            if bar_start_unix > self.current_bar.unix_timestamp:
                # The previous bar is now complete
                completed_bar = self.current_bar
                # Start a new bar with the current tick's data
                self.current_bar = schemas.Candle(
                    open=price, high=price, low=price, close=price, volume=volume,
                    unix_timestamp=bar_start_unix
                )
            else:
                # This tick updates the current bar
                self.current_bar.high = max(self.current_bar.high, price)
                self.current_bar.low = min(self.current_bar.low, price)
                self.current_bar.close = price
                self.current_bar.volume += volume

        return completed_bar


async def get_cached_intraday_bars(symbol: str, interval: str, timezone_str: str) -> List[schemas.Candle]:
    """
    Fetches 1-second bars from the Redis cache (populated by BarConn_live_data_ingestor),
    resamples them to the client's desired interval, and returns them.
    This logic remains valid as it provides the historical backfill for a new client connection.
    """
    cache_key = f"intraday_bars:{symbol}"
    try:
        # Fetch all bars from the list
        cached_bars_str = await redis_client.lrange(cache_key, 0, -1)
        if not cached_bars_str:
            logger.info(f"No cached intraday bars found for {symbol}.")
            return []

        # Deserialize the 1-second bars from cache
        one_sec_bars = [json.loads(b) for b in cached_bars_str]
        logger.info(f"Fetched {len(one_sec_bars)} 1-sec bars from cache for {symbol} for resampling.")

        # Resample the 1-sec bars into the desired interval
        resampler = BarResampler(interval, timezone_str)
        resampled_bars = []
        for bar in one_sec_bars:
            # The `add_bar` method now takes a tick, but we can adapt the 1s bar to look like a tick
            # for the purpose of resampling the historical data.
            # We'll treat the 'close' price of the 1s bar as the 'tick' price.
            tick_like_data = {
                "price": bar['close'],
                "volume": bar['volume'],
                "timestamp": bar['timestamp']
            }
            completed_bar = resampler.add_bar(tick_like_data)
            if completed_bar:
                resampled_bars.append(completed_bar)
        
        # Append the final, in-progress bar from the resampler
        if resampler.current_bar:
            resampled_bars.append(resampler.current_bar)
        
        logger.info(f"Resampled into {len(resampled_bars)} bars for {symbol} with interval {interval}.")
        return resampled_bars

    except Exception as e:
        logger.error(f"Error fetching/resampling cached intraday bars for {symbol}: {e}", exc_info=True)
        return []
    
# The rest of the file (live_data_stream, stream_redis_data) is no longer used by the new
# websocket_manager and can be removed or ignored if desired. The websocket_manager
# now contains the primary logic for handling live data.