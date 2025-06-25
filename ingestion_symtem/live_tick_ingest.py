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
    A listener that processes live data from QuoteConn, backfills historical ticks,
    and publishes raw ticks to Redis for the application server to process.
    """

    def __init__(self, name="LiveTickListener"):
        super().__init__(name)
        self.redis_client = redis_client
        self.source_timezone = ZoneInfo("America/New_York")

    def backfill_intraday_data(self, symbol: str, hist_conn: iq.HistoryConn):
        """On startup, fetch today's raw ticks from IQFeed to populate the cache."""
        logging.info(f"Backfilling intraday raw ticks for {symbol}...")
        try:
            # Fetch raw ticks for the current day instead of 1-second bars.
            today_ticks = hist_conn.request_ticks_for_days(
                ticker=symbol, num_days=1, ascend=True
            )

            if today_ticks is not None and len(today_ticks) > 0:
                cache_key = f"intraday_ticks:{symbol}"
                self.redis_client.delete(cache_key)
                
                # Use a pipeline for efficient bulk insertion
                pipeline = self.redis_client.pipeline()
                
                for tick in today_ticks:
                    # Convert IQFeed's timestamp parts to a UTC Unix timestamp
                    naive_dt = iq.date_us_to_datetime(tick['date'], tick['time'])
                    aware_dt = naive_dt.replace(tzinfo=self.source_timezone)
                    utc_timestamp = aware_dt.timestamp()

                    tick_data = {
                        "timestamp": utc_timestamp,
                        "price": float(tick['last']),
                        "volume": int(tick['last_sz']),
                    }
                    pipeline.rpush(cache_key, json.dumps(tick_data))
                
                pipeline.expire(cache_key, 86400) # Expire after 24 hours
                pipeline.execute()
                
                logging.info(f"Successfully backfilled {len(today_ticks)} raw ticks for {symbol}.")
            else:
                logging.info(f"No intraday tick data found to backfill for {symbol}.")
        except iq.NoDataError:
            logging.warning(f"No intraday tick data available to backfill for {symbol}.")
        except Exception as e:
            logging.error(f"Error during intraday tick backfill for {symbol}: {e}", exc_info=True)
    
    def _publish_tick(self, symbol: str, price: float, volume: int):
        """
        Publishes a single raw tick to a Redis channel for live streaming and
        adds it to a capped list for backfilling recent activity.
        """
        utc_timestamp = datetime.now(timezone.utc).timestamp()
        
        tick_data = {
            # "symbol": symbol,
            "price": price,
            "volume": volume,
            "timestamp": utc_timestamp
        }
        
        # Publish to the 'live_ticks' channel for real-time subscribers
        channel = f"live_ticks:{symbol}"
        self.redis_client.publish(channel, json.dumps(tick_data))

        # Add the tick to a capped list for new clients to backfill recent data
        cache_key = f"intraday_ticks:{symbol}"
        pipeline = self.redis_client.pipeline()
        pipeline.rpush(cache_key, json.dumps(tick_data))
        pipeline.execute()

    def process_summary(self, summary_data: np.ndarray) -> None:
        """Handles summary messages, treating them as ticks with zero volume."""
        try:
            for summary in summary_data:
                symbol = summary['Symbol'].decode('utf-8')
                price = float(summary['Most Recent Trade'])
                # Summary messages are not trades, so volume is 0.
                if price > 0:
                    self._publish_tick(symbol, price, 0)
        except Exception as e:
            logging.error(f"Error processing SUMMARY data: {e}. Data: {summary_data}", exc_info=True)

    def process_update(self, update_data: np.ndarray) -> None:
        """Handles trade update messages and publishes them as raw ticks."""
        try:
            for trade in update_data:
                price = float(trade['Most Recent Trade'])
                volume = int(trade['Most Recent Trade Size'])
                
                if price <= 0 or volume <= 0:
                    continue 

                symbol = trade['Symbol'].decode('utf-8')
                self._publish_tick(symbol, price, volume)
        except Exception as e:
            logging.error(f"Error processing TRADE data: {e}. Data: {update_data}", exc_info=True)

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
            listener.backfill_intraday_data(symbol, hist_conn)
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