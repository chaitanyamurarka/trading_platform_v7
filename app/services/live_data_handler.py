# chaitanyamurarka/trading_platform_v7/trading_platform_v7-e2d358352a61cb7a1309edf91f97d1e2f22f6d7b/app/services/live_data_handler.py
import logging
import asyncio
from datetime import datetime, timedelta, timezone as dt_timezone
from typing import Dict, Optional, List, Union
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from .. import schemas

# --- Get Logger ---
logger = logging.getLogger(__name__)



class TickBarResampler:
    """
    Aggregates raw ticks into bars of a specified tick-count.
    This class is now timezone-aware and ensures monotonically increasing timestamps.
    """
    def __init__(self, interval_str: str, timezone_str: str):
        try:
            self.ticks_per_bar = int(interval_str.replace('tick', ''))
        except ValueError:
            logger.error(f"Invalid tick interval format: {interval_str}. Defaulting to 1000.")
            self.ticks_per_bar = 1000
            
        self.current_bar: Optional[schemas.Candle] = None
        self.tick_count = 0
        try:
            self.tz = ZoneInfo(timezone_str)
        except ZoneInfoNotFoundError:
            logger.warning(f"Timezone '{timezone_str}' not found. Defaulting to UTC.")
            self.tz = dt_timezone.utc

        self.last_completed_bar_timestamp: Optional[float] = None

    def add_bar(self, tick_data: Dict) -> Optional[schemas.Candle]:
        """
        Processes a single raw tick. If a bar is completed, it returns the completed bar.
        --- MODIFIED: Logic corrected to avoid 1-tick lag. ---
        """
        if not all(k in tick_data for k in ['price', 'volume', 'timestamp']):
            logger.warning(f"Malformed tick data received: {tick_data}")
            return None

        price, volume, ts_float = float(tick_data['price']), int(tick_data['volume']), tick_data['timestamp']
        
        # Create a timezone-aware "fake UTC" timestamp for the frontend
        ts_utc = datetime.fromtimestamp(ts_float, tz=dt_timezone.utc)
        local_dt = ts_utc.astimezone(self.tz)
        fake_utc_dt = datetime(
            local_dt.year, local_dt.month, local_dt.day,
            local_dt.hour, local_dt.minute, local_dt.second,
            microsecond=local_dt.microsecond,
            tzinfo=dt_timezone.utc
        )
        fake_unix_timestamp = fake_utc_dt.timestamp()

        # Ensure timestamps are always unique and increasing
        if self.last_completed_bar_timestamp is not None and fake_unix_timestamp <= self.last_completed_bar_timestamp:
            fake_unix_timestamp = self.last_completed_bar_timestamp + 0.000001

        # If there's no bar, start a new one
        if self.current_bar is None:
            self.current_bar = schemas.Candle(open=price, high=price, low=price, close=price, volume=0, unix_timestamp=fake_unix_timestamp)
        
        # Add the current tick's data to the bar
        self.current_bar.high = max(self.current_bar.high, price)
        self.current_bar.low = min(self.current_bar.low, price)
        self.current_bar.close = price
        self.current_bar.volume += volume
        self.tick_count += 1
        
        # Check if the bar is now complete
        if self.tick_count >= self.ticks_per_bar:
            completed_bar = self.current_bar
            self.last_completed_bar_timestamp = completed_bar.unix_timestamp
            
            # Reset for the next bar
            self.current_bar = None
            self.tick_count = 0
            
            return completed_bar
        
        # Bar is still in-progress, return nothing
        return None

# The rest of the file remains unchanged...
# (BarResampler class and resample_ticks_to_bars function)
# ...
class BarResampler:
    """
    Aggregates raw ticks into time-based OHLCV bars (e.g., 1-minute, 5-minute).
    This class is stateful and timezone-aware.
    """
    def __init__(self, interval_str: str, timezone_str: str):
        self.interval_td = self._parse_interval(interval_str)
        self.current_bar: Optional[schemas.Candle] = None
        try:
            self.tz = ZoneInfo(timezone_str)
        except ZoneInfoNotFoundError:
            logger.warning(f"Timezone '{timezone_str}' not found. Defaulting to UTC.")
            self.tz = dt_timezone.utc

    def _parse_interval(self, s: str) -> timedelta:
        unit, value = s[-1], int(s[:-1])
        if unit == 's': return timedelta(seconds=value)
        if unit == 'm': return timedelta(minutes=value)
        if unit == 'h': return timedelta(hours=value)
        raise ValueError(f"Invalid time-based interval: {s}")

    def add_bar(self, tick_data: Dict) -> Optional[schemas.Candle]:
        """
        Processes a single raw tick. If a new time interval begins, it returns the
        previously completed bar with a frontend-compatible 'fake UTC' timestamp.
        """
        if not all(k in tick_data for k in ['price', 'volume', 'timestamp']):
            logger.warning(f"Malformed tick data received: {tick_data}")
            return None

        price, volume = float(tick_data['price']), int(tick_data['volume'])
        ts_utc = datetime.fromtimestamp(tick_data['timestamp'], tz=dt_timezone.utc)
        
        local_dt = ts_utc.astimezone(self.tz)
        
        interval_seconds = self.interval_td.total_seconds()
        local_ts = local_dt.timestamp()
        bar_start_local_ts_float = local_ts - (local_ts % interval_seconds)
        
        bar_start_local_dt = datetime.fromtimestamp(bar_start_local_ts_float, self.tz)

        fake_utc_dt = datetime(
            bar_start_local_dt.year, bar_start_local_dt.month, bar_start_local_dt.day,
            bar_start_local_dt.hour, bar_start_local_dt.minute, bar_start_local_dt.second,
            tzinfo=dt_timezone.utc
        )
        bar_start_unix = fake_utc_dt.timestamp()
        
        if not self.current_bar:
            self.current_bar = schemas.Candle(open=price, high=price, low=price, close=price, volume=volume, unix_timestamp=bar_start_unix)
        elif bar_start_unix > self.current_bar.unix_timestamp:
            completed_bar = self.current_bar
            self.current_bar = schemas.Candle(open=price, high=price, low=price, close=price, volume=volume, unix_timestamp=bar_start_unix)
            return completed_bar
        else:
            self.current_bar.high = max(self.current_bar.high, price)
            self.current_bar.low = min(self.current_bar.low, price)
            self.current_bar.close = price
            self.current_bar.volume += volume
            
        return None

async def resample_ticks_to_bars(
    ticks: List[Dict],
    target_interval_str: str,
    target_timezone_str: str,
    chunk_size: int = 25000
) -> List[schemas.Candle]:
    """
    Asynchronously resamples a list of raw tick data into OHLC bars,
    yielding control to the event loop periodically to prevent blocking.
    """
    if not ticks:
        return []

    logger.info(f"Asynchronously resampling {len(ticks)} ticks into {target_interval_str} bars.")

    is_tick_based = 'tick' in target_interval_str
    resampler = TickBarResampler(target_interval_str, target_timezone_str) if is_tick_based else BarResampler(target_interval_str, target_timezone_str)
    
    completed_bars: List[schemas.Candle] = []
    for i in range(0, len(ticks), chunk_size):
        chunk = ticks[i:i + chunk_size]
        for tick in chunk:
            completed_bar = resampler.add_bar(tick)
            if completed_bar:
                completed_bars.append(completed_bar)
        
        # Yield control to the event loop to prevent blocking
        await asyncio.sleep(0)
            
    # Add the final, in-progress bar
    if resampler.current_bar:
        completed_bars.append(resampler.current_bar)
        
    logger.info(f"Resampling complete. Produced {len(completed_bars)} bars.")
    return completed_bars