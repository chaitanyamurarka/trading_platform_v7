import logging
from datetime import datetime, timedelta, timezone as dt_timezone
from typing import Dict, Optional, List, Union
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from .. import schemas

# --- Get Logger ---
# Standard practice to get a logger instance for the current module
logger = logging.getLogger(__name__)

class TickBarResampler:
    """
    Aggregates raw ticks into bars of a specified tick-count.
    
    This class is stateful and designed to be used per interval. For example, if a
    client wants 100-tick bars, this class will collect 100 raw ticks, assemble them
    into a single OHLC bar, and then reset.
    """
    def __init__(self, interval_str: str):
        """
        Initializes the resampler with the number of ticks required for each bar.
        
        Args:
            interval_str (str): The interval string, e.g., "100tick".
        """
        try:
            self.ticks_per_bar = int(interval_str.replace('tick', ''))
        except ValueError:
            logger.error(f"Invalid tick interval format: {interval_str}. Defaulting to 1000.")
            self.ticks_per_bar = 1000
            
        self.current_bar: Optional[schemas.Candle] = None
        self.tick_count = 0

    def add_bar(self, tick_data: Dict) -> Optional[schemas.Candle]:
        """
        Processes a single raw tick. If a bar is completed, it returns the bar.
        
        Args:
            tick_data (Dict): A dictionary containing 'price', 'volume', and 'timestamp'.
            
        Returns:
            Optional[schemas.Candle]: The completed OHLC candle, or None if the bar is still in progress.
        """
        # --- Input validation ---
        if not all(k in tick_data for k in ['price', 'volume', 'timestamp']):
            logger.warning(f"Malformed tick data received: {tick_data}")
            return None

        price, volume, ts = float(tick_data['price']), int(tick_data['volume']), tick_data['timestamp']
        
        if not self.current_bar:
            # --- Start of a new bar ---
            self.current_bar = schemas.Candle(open=price, high=price, low=price, close=price, volume=volume, unix_timestamp=ts)
            self.tick_count = 1
        else:
            # --- Update the in-progress bar ---
            self.current_bar.high = max(self.current_bar.high, price)
            self.current_bar.low = min(self.current_bar.low, price)
            self.current_bar.close = price
            self.current_bar.volume += volume
            self.current_bar.unix_timestamp = ts  # Always update to the latest tick's timestamp
            self.tick_count += 1
        
        # --- Check if the bar is complete ---
        if self.tick_count >= self.ticks_per_bar:
            completed_bar = self.current_bar
            self.current_bar = None  # Reset for the next bar
            self.tick_count = 0
            return completed_bar
            
        return None

class BarResampler:
    """
    Aggregates raw ticks into time-based OHLCV bars (e.g., 1-minute, 5-minute).
    
    This class is stateful and timezone-aware. It correctly buckets ticks into
    time windows based on the client's specified timezone.
    """
    def __init__(self, interval_str: str, timezone_str: str):
        """
        Initializes the resampler with a time interval and a client timezone.
        
        Args:
            interval_str (str): The time interval string, e.g., "1m", "5s".
            timezone_str (str): The IANA timezone name, e.g., "America/New_York".
        """
        self.interval_td = self._parse_interval(interval_str)
        self.current_bar: Optional[schemas.Candle] = None
        try:
            self.tz = ZoneInfo(timezone_str)
        except ZoneInfoNotFoundError:
            logger.warning(f"Timezone '{timezone_str}' not found. Defaulting to UTC.")
            self.tz = dt_timezone.utc

    def _parse_interval(self, s: str) -> timedelta:
        """Helper to convert an interval string into a timedelta object."""
        unit, value = s[-1], int(s[:-1])
        if unit == 's': return timedelta(seconds=value)
        if unit == 'm': return timedelta(minutes=value)
        if unit == 'h': return timedelta(hours=value)
        raise ValueError(f"Invalid time-based interval: {s}")

    def _get_bar_start_time(self, dt_utc: datetime) -> float:
        """
        Calculates the start timestamp for the bar that a given tick belongs to.
        This correctly "snaps" the tick to the beginning of its interval window.
        
        For example, a tick at 10:01:23 for a '1m' interval will be snapped to 10:01:00.
        
        Args:
            dt_utc (datetime): The UTC timestamp of the raw tick.
            
        Returns:
            float: The UNIX timestamp for the start of the bar.
        """
        local_dt = dt_utc.astimezone(self.tz)
        interval_seconds = self.interval_td.total_seconds()
        ts = local_dt.timestamp()
        bar_start_local_ts = ts - (ts % interval_seconds)
        return bar_start_local_ts

    def add_bar(self, tick_data: Dict) -> Optional[schemas.Candle]:
        """
        Processes a single raw tick. If a new time interval begins, it returns the previously completed bar.
        
        Args:
            tick_data (Dict): A dictionary containing 'price', 'volume', and 'timestamp'.
            
        Returns:
            Optional[schemas.Candle]: The completed OHLC candle, or None if the bar is still in progress.
        """
        if not all(k in tick_data for k in ['price', 'volume', 'timestamp']):
            logger.warning(f"Malformed tick data received: {tick_data}")
            return None

        price, volume = float(tick_data['price']), int(tick_data['volume'])
        ts_utc = datetime.fromtimestamp(tick_data['timestamp'], tz=dt_timezone.utc)
        bar_start_unix = self._get_bar_start_time(ts_utc)
        
        if not self.current_bar:
            # --- Start the very first bar ---
            self.current_bar = schemas.Candle(open=price, high=price, low=price, close=price, volume=volume, unix_timestamp=bar_start_unix)
        elif bar_start_unix > self.current_bar.unix_timestamp:
            # --- A new time interval has started, so the previous bar is complete ---
            completed_bar = self.current_bar
            # Start the new bar with the current tick's data
            self.current_bar = schemas.Candle(open=price, high=price, low=price, close=price, volume=volume, unix_timestamp=bar_start_unix)
            return completed_bar
        else:
            # --- The tick belongs to the current bar, so update it ---
            self.current_bar.high = max(self.current_bar.high, price)
            self.current_bar.low = min(self.current_bar.low, price)
            self.current_bar.close = price
            self.current_bar.volume += volume
            
        return None

# NEW FUNCTION: Add this to the end of the file
def resample_bars_from_bars(
    one_sec_bars: List[schemas.Candle],
    target_interval_str: str,
    target_timezone_str: str
) -> List[schemas.Candle]:
    """
    Resamples a list of 1-second OHLC bars into a new, larger time interval.

    Args:
        one_sec_bars: A list of 1-second candle objects, sorted chronologically.
        target_interval_str: The desired output interval (e.g., "5m", "1h").
        target_timezone_str: The IANA timezone for alignment.

    Returns:
        A new list of resampled OHLC candle objects.
    """
    if not one_sec_bars:
        return []

    # This method is not suitable for creating tick-based bars from time-based bars.
    if 'tick' in target_interval_str:
        logger.warning(f"Cannot resample 1s bars into tick-based interval '{target_interval_str}'.")
        return []

    logger.info(f"Resampling {len(one_sec_bars)} 1-second bars into {target_interval_str} bars.")

    # Use a temporary resampler instance for this one-off task.
    resampler = BarResampler(interval_str=target_interval_str, timezone_str=target_timezone_str)
    completed_bars: List[schemas.Candle] = []

    for bar in one_sec_bars:
        # To resample, we can treat each 1-second bar as if it were a single "tick".
        # The BarResampler will then correctly bucket these based on their timestamps.
        tick_like_data = {
            "price": bar.close,
            "volume": bar.volume,
            "timestamp": bar.unix_timestamp # The resampler works with UNIX timestamps
        }
        completed_bar = resampler.add_bar(tick_like_data)
        if completed_bar:
            completed_bars.append(completed_bar)

    # After the loop, the resampler might have a final, in-progress bar.
    # For backfilling, we consider this bar "complete" for the data we have.
    if resampler.current_bar:
        completed_bars.append(resampler.current_bar)
        
    logger.info(f"Resampling complete. Produced {len(completed_bars)} bars.")
    return completed_bars