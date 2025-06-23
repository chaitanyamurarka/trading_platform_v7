# chaitanyamurarka/trading_platform_v7/trading_platform_v7-453e29a60b38870dc8c9a94acffec1826c839dee/app/services/live_data_handler.py
import logging
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
        Processes a single raw tick. If a bar is completed, it returns the completed bar
        and immediately starts the next bar, ensuring there is always a current_bar.
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
        
        # --- NEW, CORRECTED LOGIC ---

        # If this is the very first tick the resampler has ever seen, create a bar and exit.
        if self.current_bar is None:
            self.current_bar = schemas.Candle(open=price, high=price, low=price, close=price, volume=volume, unix_timestamp=fake_unix_timestamp)
            self.tick_count = 1
            return None

        # If we are here, a bar is already in progress. Check if it's full.
        if self.tick_count >= self.ticks_per_bar:
            # The bar was filled by the PREVIOUS tick. This new tick starts the NEXT bar.
            completed_bar = self.current_bar
            self.last_completed_bar_timestamp = completed_bar.unix_timestamp
            
            # Start the new bar with the current tick's data
            self.current_bar = schemas.Candle(open=price, high=price, low=price, close=price, volume=volume, unix_timestamp=fake_unix_timestamp)
            self.tick_count = 1
            
            # Return the bar we just completed
            return completed_bar
        else:
            # The current bar is not full yet. Add the current tick's data to it.
            self.current_bar.high = max(self.current_bar.high, price)
            self.current_bar.low = min(self.current_bar.low, price)
            self.current_bar.close = price
            self.current_bar.volume += volume
            self.current_bar.unix_timestamp = fake_unix_timestamp
            self.tick_count += 1
            
            # The bar is still in-progress, return nothing.
            return None
        
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

def resample_ticks_to_bars(
    ticks: List[Dict],
    target_interval_str: str,
    target_timezone_str: str
) -> List[schemas.Candle]:
    """
    Resamples a list of raw tick data into OHLC bars of a specified interval.
    """
    if not ticks:
        return []

    logger.info(f"Resampling {len(ticks)} ticks into {target_interval_str} bars.")

    is_tick_based = 'tick' in target_interval_str
    resampler = TickBarResampler(target_interval_str, target_timezone_str) if is_tick_based else BarResampler(target_interval_str, target_timezone_str)
    
    completed_bars: List[schemas.Candle] = []
    for tick in ticks:
        completed_bar = resampler.add_bar(tick)
        if completed_bar:
            completed_bars.append(completed_bar)
            
    if resampler.current_bar:
        completed_bars.append(resampler.current_bar)
        
    logger.info(f"Resampling complete. Produced {len(completed_bars)} bars.")
    return completed_bars