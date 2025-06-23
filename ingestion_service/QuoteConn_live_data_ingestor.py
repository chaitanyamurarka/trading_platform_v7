# In ingestion_service/QuoteConn_live_data_ingestor.py

import logging
import json
import time
from datetime import datetime, timezone
import numpy as np
import pyiqfeed as iq
import redis
from zoneinfo import ZoneInfo

# Local imports
from dtn_iq_client import get_iqfeed_quote_conn, get_iqfeed_history_conn, launch_iqfeed_service_if_needed
from config import settings

# --- Configuration ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(funcName)s - %(message)s')
REDIS_URL = settings.REDIS_URL
redis_client = redis.Redis.from_url(REDIS_URL)


class LiveTickListener(iq.SilentQuoteListener):
    """
    A listener that processes live data from QuoteConn and publishes
    raw ticks directly to Redis for the application server to process.
    """

    def __init__(self, name="LiveTickListener"):
        super().__init__(name)
        self.redis_client = redis_client
        self.source_timezone = ZoneInfo("America/New_York")
        # Removed self.current_bars as aggregation is no longer done here.

    def backfill_intraday_data(self, symbol: str, hist_conn: iq.HistoryConn):
        """On startup, fetch today's data from IQFeed to populate the cache."""
        logging.info(f"Backfilling intraday data for {symbol}...")
        try:
            # This part remains unchanged, as it's for historical backfill.
            # It populates a cache that the app can use for initial chart loads.
            today_data = hist_conn.request_bars_for_days(
                ticker=symbol, interval_len=1, interval_type='s', days=1, ascend=True)

            if today_data is not None and len(today_data) > 0:
                cache_key = f"intraday_bars:{symbol}"
                self.redis_client.delete(cache_key)
                
                for bar in today_data:
                    naive_bar_datetime = (bar['date'] + bar['time']).astype(datetime)
                    aware_bar_datetime = naive_bar_datetime.replace(tzinfo=self.source_timezone)
                    utc_timestamp = int(aware_bar_datetime.timestamp())

                    bar_data = {
                        "unix_timestamp": utc_timestamp,
                        "open": float(bar['open_p']), "high": float(bar['high_p']),
                        "low": float(bar['low_p']), "close": float(bar['close_p']),
                        "volume": int(bar['prd_vlm']),
                    }
                    self.redis_client.rpush(cache_key, json.dumps(bar_data))
                    self.redis_client.expire(cache_key, 86400)

                
                logging.info(f"Successfully backfilled {len(today_data)} bars for {symbol}.")
            else:
                logging.info(f"No intraday data found to backfill for {symbol}.")
        except iq.NoDataError:
            logging.warning(f"No intraday data available to backfill for {symbol}.")
        except Exception as e:
            logging.error(f"Error during intraday backfill for {symbol}: {e}", exc_info=True)
    
    def _publish_tick(self, symbol: str, price: float, volume: int):
        """
        Publishes a single raw tick to Redis.
        """
        # Create a precise UTC timestamp for the tick
        utc_timestamp = datetime.now(timezone.utc).timestamp()
        
        tick_data = {
            "symbol": symbol,
            "price": price,
            "volume": volume,
            "timestamp": utc_timestamp
        }
        
        # Publish to the new 'live_ticks' channel
        channel = f"live_ticks:{symbol}"
        self.redis_client.publish(channel, json.dumps(tick_data))


    def process_summary(self, summary_data: np.ndarray) -> None:
        """Handles summary messages, treating them as ticks."""
        try:
            for summary in summary_data:
                symbol = summary['Symbol'].decode('utf-8')
                price = float(summary['Most Recent Trade'])
                # Using 'Total Volume' and tracking its change might be complex.
                # For simplicity, we'll use a volume of 0 for non-trade updates.
                volume = 0 
                
                if price > 0:
                    self._publish_tick(symbol, price, volume)
        except Exception as e:
            logging.error(f"Error processing SUMMARY data: {e}. Data: {summary_data}", exc_info=True)

    def process_update(self, update_data: np.ndarray) -> None:
        """Handles trade update messages and publishes them as raw ticks."""
        try:
            for trade in update_data:
                price = float(trade['Most Recent Trade'])
                volume = int(trade['Most Recent Trade Size'])
                
                # Ignore ticks with no price or volume
                if price <= 0 or volume <= 0:
                    continue 

                symbol = trade['Symbol'].decode('utf-8')
                self._publish_tick(symbol, price, volume)
        except Exception as e:
            logging.error(f"Error processing TRADE data: {e}. Data: {update_data}", exc_info=True)
    
    # The _update_bar and publish_and_cache_bar methods are no longer needed
    # and have been removed.

def main():
    """Main function to start listening to live data."""
    launch_iqfeed_service_if_needed()
    symbols = ["AAPL", "AMZN", "TSLA", "@NQ#"]
    
    quote_conn = get_iqfeed_quote_conn()
    hist_conn = get_iqfeed_history_conn()

    if not quote_conn or not hist_conn:
        logging.error("Could not get IQFeed connections. Exiting.")
        return

    listener = LiveTickListener()
    quote_conn.add_listener(listener)
    
    with iq.ConnConnector([quote_conn, hist_conn]):
        for symbol in symbols:
            # The backfill logic remains, it helps populate the chart on initial load
            listener.backfill_intraday_data(symbol, hist_conn)
            # This subscribes to live trade updates from IQFeed
            quote_conn.trades_watch(symbol)
            logging.info(f"Watching {symbol} for live tick updates.")
        
        try:
            logging.info("Ingestion service is running. Press Ctrl+C to stop.")
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            logging.info("Stopping live data ingestion.")
            for symbol in symbols:
                quote_conn.unwatch(symbol)

if __name__ == "__main__":
    main()