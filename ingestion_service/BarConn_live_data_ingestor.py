# ingestion_service/BarConn_live_data_ingestor.py

import logging
import json
import time
from datetime import datetime, timezone
import numpy as np
import pyiqfeed as iq
import redis
from zoneinfo import ZoneInfo
from influxdb_client import InfluxDBClient

# Local imports
from dtn_iq_client import get_iqfeed_bar_conn, launch_iqfeed_service_if_needed
from config import settings
from pyiqfeed.field_readers import date_us_to_datetime

# --- Configuration ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(funcName)s - %(message)s')
REDIS_URL = settings.REDIS_URL
redis_client = redis.Redis.from_url(REDIS_URL)

# InfluxDB Configuration
INFLUX_URL = settings.INFLUX_URL
INFLUX_TOKEN = settings.INFLUX_TOKEN
INFLUX_ORG = settings.INFLUX_ORG
INFLUX_BUCKET = settings.INFLUX_BUCKET

# InfluxDB Client
influx_client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG, timeout=20_000)
query_api = influx_client.query_api()


def get_latest_timestamp_from_influx(symbol: str, measurement: str = "ohlc_1s") -> datetime | None:
    """
    Queries InfluxDB for the latest timestamp for a given symbol and measurement.
    Returns the datetime in UTC timezone.
    """
    flux_query = f'''
        from(bucket: "{INFLUX_BUCKET}")
          |> range(start: 0)
          |> filter(fn: (r) => r._measurement == "{measurement}" and r.symbol == "{symbol}")
          |> last()
          |> keep(columns: ["_time"])
    '''
    try:
        tables = query_api.query(query=flux_query)
        if not tables or not tables[0].records:
            logging.info(f"No existing data found for '{symbol}' in measurement '{measurement}'.")
            return None
        
        latest_time = tables[0].records[0].get_time()
        
        if latest_time.tzinfo is None:
            latest_time = latest_time.replace(tzinfo=timezone.utc)
            
        logging.info(f"Latest timestamp for '{symbol}' in '{measurement}' is {latest_time}.")
        return latest_time
    except Exception as e:
        logging.error(f"Error querying latest timestamp for {symbol} in {measurement}: {e}", exc_info=True)
        return None


class LiveBarListener(iq.SilentBarListener):
    """
    A listener that processes 1-second bars from BarConn, logs a summary when
    backfill is complete, and silently processes live data.
    """

    def __init__(self, name="LiveBarListener"):
        super().__init__(name)
        self.redis_client = redis.Redis.from_url(REDIS_URL)
        self.source_timezone = ZoneInfo("America/New_York")
        self.completed_lookbacks = set()

    def _prepare_bar_data(self, bar_data: np.ndarray) -> dict:
        """
        Converts a numpy bar record from IQFeed into a dictionary
        with a standard UTC Unix timestamp.
        """
        bar = bar_data[0]
        naive_bar_datetime = date_us_to_datetime(bar['date'], bar['time'])
        aware_bar_datetime = naive_bar_datetime.replace(tzinfo=self.source_timezone)
        utc_timestamp = int(aware_bar_datetime.timestamp())

        return {
            "timestamp": utc_timestamp,
            "open": float(bar['open_p']),
            "high": float(bar['high_p']),
            "low": float(bar['low_p']),
            "close": float(bar['close_p']),
            "volume": int(bar['prd_vlm']),
        }

    def _check_and_log_lookback_completion(self, symbol: str):
        """Checks if lookback is complete for a symbol and logs it exactly once."""
        if symbol not in self.completed_lookbacks:
            logging.info(f"Lookback complete for {symbol}. Now processing live data.")
            self.completed_lookbacks.add(symbol)

    def process_history_bar(self, bar_data: np.ndarray) -> None:
        """
        Handles historical bars silently, caching them in Redis.
        The verbose super() call is removed to prevent console noise.
        """
        try:
            if bar_data[0]['high_p'] == 0 and bar_data[0]['low_p'] == 0:
                return

            bar_dict = self._prepare_bar_data(bar_data)
            symbol = bar_data[0]['symbol'].decode('utf-8')
            cache_key = f"intraday_bars:{symbol}"
            
            self.redis_client.rpush(cache_key, json.dumps(bar_dict))
            self.redis_client.expire(cache_key, 86400)
            
        except Exception as e:
            logging.error(f"Error processing HISTORY bar: {e}. Data: {bar_data}", exc_info=True)

    def process_latest_bar_update(self, bar_data: np.ndarray) -> None:
        """
        This is the first message received after the history stream ends.
        We use it as a signal to log that the lookback is complete.
        """
        super().process_latest_bar_update(bar_data) # Keep one verbose print as a signal
        try:
            symbol = bar_data[0]['symbol'].decode('utf-8')
            self._check_and_log_lookback_completion(symbol)
        except Exception as e:
            logging.error(f"Error in process_latest_bar_update: {e}", exc_info=True)

    def process_live_bar(self, bar_data: np.ndarray) -> None:
        """
        Handles a newly completed live bar silently.
        """
        try:
            bar_dict = self._prepare_bar_data(bar_data)
            symbol = bar_data[0]['symbol'].decode('utf-8')

            # Ensure the completion message is logged if it hasn't been already
            self._check_and_log_lookback_completion(symbol)
            
            # Publish to live subscribers
            channel = f"live_bars:{symbol}"
            self.redis_client.publish(channel, json.dumps(bar_dict))

            # Cache this live bar
            cache_key = f"intraday_bars:{symbol}"
            self.redis_client.rpush(cache_key, json.dumps(bar_dict))
            self.redis_client.expire(cache_key, 86400)
            
        except Exception as e:
            logging.error(f"Error processing LIVE bar: {e}. Data: {bar_data}", exc_info=True)


def clear_symbol_cache(symbols: list):
    """Deletes the Redis cache for the given symbols before starting."""
    for symbol in symbols:
        cache_key = f"intraday_bars:{symbol}"
        logging.info(f"Clearing cache for {symbol} (key: {cache_key}).")
        redis_client.delete(cache_key)


def get_bgn_bars_datetime(symbol: str) -> datetime | None:
    """
    Gets the starting datetime for fetching bars from the last available data in InfluxDB.
    If no data exists in InfluxDB, returns None to fetch default amount of historical data.
    """
    try:
        # Get the latest timestamp from InfluxDB for 1-second bars
        latest_timestamp = get_latest_timestamp_from_influx(symbol, "ohlc_1s")
        
        if latest_timestamp:
            # Convert UTC to Eastern Time for IQFeed
            et_timezone = ZoneInfo("America/New_York")
            bgn_bars_et = latest_timestamp.astimezone(et_timezone)
            logging.info(f"Found last available data for {symbol} at {latest_timestamp} UTC, starting from {bgn_bars_et} ET")
            return bgn_bars_et.replace(tzinfo=None)  # IQFeed expects naive datetime
        else:
            logging.info(f"No existing data found for {symbol} in InfluxDB, will fetch default historical data")
            return None
            
    except Exception as e:
        logging.error(f"Error getting bgn_bars datetime for {symbol}: {e}", exc_info=True)
        return None


def main():
    """Main function to start listening to live bar data."""
    launch_iqfeed_service_if_needed()
    symbols = ["AAPL", "AMZN", "TSLA", "@NQM25"]
    
    clear_symbol_cache(symbols)
    
    bar_conn = get_iqfeed_bar_conn()
    if not bar_conn:
        logging.error("Could not get IQFeed BarConn. Exiting.")
        return

    listener = LiveBarListener()
    bar_conn.add_listener(listener)

    # bar_listener = iq.VerboseBarListener("Bar Listener")
    # bar_conn.add_listener(bar_listener)
    
    with iq.ConnConnector([bar_conn]):
        for symbol in symbols:
            # Get the starting datetime from the last available data in InfluxDB
            bgn_bars_datetime = get_bgn_bars_datetime(symbol)
            
            if bgn_bars_datetime:
                # Use bgn_bars to start from the last available data
                bar_conn.watch(
                    symbol=symbol, 
                    interval_len=1, 
                    interval_type='s', 
                    update=1,
                    bgn_bars=bgn_bars_datetime
                )
                logging.info(f"Watching {symbol} for 1-second bars starting from {bgn_bars_datetime}")
            else:
                # If no data in InfluxDB, start with minimal lookback for efficiency
                # This ensures we get some recent data but not too much historical data
                bar_conn.watch(
                    symbol=symbol, 
                    interval_len=1, 
                    interval_type='s', 
                    update=1,
                    lookback_bars=10  # Minimal lookback as fallback
                )
                logging.info(f"Watching {symbol} for 1-second bars with minimal lookback (no InfluxDB data found)")
        
        try:
            logging.info("Ingestion service is running with BarConn. Press Ctrl+C to stop.")
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            logging.info("Stopping live data ingestion.")
            for symbol in symbols:
                bar_conn.unwatch(symbol)


if __name__ == "__main__":
    main()