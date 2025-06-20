# In ingestion_service/live_data_ingestor.py

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
    A listener that processes live data from QuoteConn, aggregates it into
    1-second bars, and publishes completed bars to Redis.
    """

    def __init__(self, name="LiveTickListener"):
        super().__init__(name)
        self.redis_client = redis_client
        self.source_timezone = ZoneInfo("America/New_York")
        self.current_bars = {}

    def backfill_intraday_data(self, symbol: str, hist_conn: iq.HistoryConn):
        """On startup, fetch today's data from IQFeed to populate the cache."""
        logging.info(f"Backfilling intraday data for {symbol}...")
        try:
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
                        "timestamp": utc_timestamp,
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

    def process_summary(self, summary_data: np.ndarray) -> None:
        """Handles summary messages, which are common for futures/indices."""
        try:
            for summary in summary_data:
                symbol = summary['Symbol'].decode('utf-8')
                price = float(summary['Most Recent Trade'])
                # Using 'Total Volume' from summary as it's more reliable here
                volume = int(summary['Total Volume'])
                
                if price > 0:
                    self._update_bar(symbol, price, volume, is_trade=False)
        except Exception as e:
            logging.error(f"Error processing SUMMARY data: {e}. Data: {summary_data}", exc_info=True)

    def process_update(self, update_data: np.ndarray) -> None:
        """Handles trade update messages."""
        try:
            for trade in update_data:
                price = float(trade['Most Recent Trade'])
                # 'Most Recent Trade Size' is the volume for this specific trade
                volume = int(trade['Most Recent Trade Size'])
                
                if price <= 0 or volume <= 0:
                    continue 

                symbol = trade['Symbol'].decode('utf-8')
                self._update_bar(symbol, price, volume, is_trade=True)
        except Exception as e:
            logging.error(f"Error processing TRADE data: {e}. Data: {update_data}", exc_info=True)

    def _update_bar(self, symbol: str, price: float, volume: int, is_trade: bool):
        """A centralized method to create or update the current bar for a symbol."""
        aware_dt = datetime.now(self.source_timezone)
        utc_dt = aware_dt.astimezone(timezone.utc)
        bar_timestamp = int(utc_dt.replace(microsecond=0).timestamp())

        if symbol not in self.current_bars or self.current_bars[symbol]['timestamp'] != bar_timestamp:
            if symbol in self.current_bars:
                self.publish_and_cache_bar(self.current_bars[symbol])

            self.current_bars[symbol] = {
                "timestamp": bar_timestamp, "symbol": symbol,
                "open": price, "high": price, "low": price, "close": price,
                # For a new bar, the volume is the volume of the first tick
                "volume": volume if is_trade else 0
            }
        else:
            bar = self.current_bars[symbol]
            bar['high'] = max(bar['high'], price)
            bar['low'] = min(bar['low'], price)
            bar['close'] = price
            # Only accumulate volume if it's from a trade update
            if is_trade:
                bar['volume'] += volume

    def publish_and_cache_bar(self, bar_data):
        """Publishes a completed bar to Redis pub/sub and caches it."""
        message_to_publish = bar_data.copy()
        symbol = message_to_publish.pop("symbol")
        
        channel = f"live_bars:{symbol}"
        self.redis_client.publish(channel, json.dumps(message_to_publish))

        cache_key = f"intraday_bars:{symbol}"
        self.redis_client.rpush(cache_key, json.dumps(message_to_publish))
        self.redis_client.expire(cache_key, 86400)


def main():
    """Main function to start listening to live data."""
    launch_iqfeed_service_if_needed()
    symbols = ["AAPL", "AMZN", "TSLA", "@NQM25"]
    
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
            logging.info(f"Watching {symbol} for live updates.")
        
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