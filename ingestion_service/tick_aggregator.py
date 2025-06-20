import logging
import time
import numpy as np
from datetime import datetime, timedelta, timezone as dt_timezone
from typing import List, Optional, Dict
from concurrent.futures import ThreadPoolExecutor
import multiprocessing as mp

# Local imports
from dtn_iq_client import get_iqfeed_history_conn
from iqfeed_ingestor import get_latest_timestamp, get_last_completed_session_end_time_utc
from config import settings
import pyiqfeed as iq
from influxdb_client import Point
from influxdb_client.client.write_api import WriteOptions
from influxdb_client import InfluxDBClient, Point, WriteOptions, WritePrecision

# --- Configuration ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(module)s - %(message)s')

# --- InfluxDB Setup ---
INFLUX_URL = settings.INFLUX_URL
INFLUX_TOKEN = settings.INFLUX_TOKEN
INFLUX_ORG = settings.INFLUX_ORG
INFLUX_BUCKET = settings.INFLUX_BUCKET

# --- PERFORMANCE CONFIGURATION ---
# Adjust these based on your system capabilities
MAX_WORKERS = min(8, mp.cpu_count())  # CPU cores for parallel processing
TICK_BATCH_SIZE = 50000  # Larger batches for better throughput
WRITE_BATCH_SIZE = 20000  # Optimized for InfluxDB performance
QUEUE_SIZE = 100000  # Buffer size for async processing
CONCURRENT_WRITES = 4  # Number of concurrent InfluxDB writers


# --- Optimized InfluxDB Connection Pool ---
class InfluxDBPool:
    """Connection pool for InfluxDB with async writers"""
    
    def __init__(self, pool_size=CONCURRENT_WRITES):
        self.pool_size = pool_size
        self.clients = []
        self.write_apis = []
        self._create_pool()
        
    def _create_pool(self):
        """Create a pool of InfluxDB connections"""
        for i in range(self.pool_size):
            client = InfluxDBClient(
                url=INFLUX_URL, 
                token=INFLUX_TOKEN, 
                org=INFLUX_ORG, 
                timeout=30_000
            )
            
            # Optimized write options for high throughput
            write_api = client.write_api(write_options=WriteOptions(
                batch_size=WRITE_BATCH_SIZE,
                flush_interval=2_000,  # Faster flush
                jitter_interval=500,   # Reduced jitter
                retry_interval=1_000,  # Faster retry
                max_retries=3,
                exponential_base=1.1
            ))
            
            self.clients.append(client)
            self.write_apis.append(write_api)
            
    def get_write_api(self, index=0):
        """Get a write API from the pool"""
        return self.write_apis[index % self.pool_size]
    
    def close_all(self):
        """Close all connections in the pool"""
        for client in self.clients:
            client.close()

influx_pool = InfluxDBPool()
write_api = influx_pool.get_write_api(0)

# --- Aggregation Configuration ---
AGGREGATION_LEVELS = [1, 10, 1000] # e.g., 1-tick, 10-tick, 1000-tick bars
TICK_FETCH_BATCH_SIZE = 2_000_000 # Number of ticks to fetch from IQFeed at once

def aggregate_ticks_to_bars(ticks: np.ndarray, ticks_per_bar: int) -> List[Dict]:
    """
    Aggregates an array of raw ticks into OHLC bars.

    Args:
        ticks: A numpy array of tick data from pyiqfeed.
        ticks_per_bar: The number of ticks to aggregate into a single bar.

    Returns:
        A list of dictionaries, where each dictionary represents an OHLC bar.
    """
    if len(ticks) == 0:
        return []

    bars = []
    for i in range(0, len(ticks), ticks_per_bar):
        chunk = ticks[i:i + ticks_per_bar]
        if len(chunk) == 0:
            continue

        # Get timestamp from the last tick in the chunk
        last_tick = chunk[-1]
        naive_dt = iq.date_us_to_datetime(last_tick['date'], last_tick['time'])
        aware_dt = naive_dt.replace(tzinfo=dt_timezone.utc)
        
        # Aggregate data for the bar
        open_price = float(chunk[0]['last'])
        high_price = float(np.max(chunk['last']))
        low_price = float(np.min(chunk['last']))
        close_price = float(last_tick['last'])
        total_volume = int(np.sum(chunk['last_sz']))

        bars.append({
            "time": aware_dt,
            "open": open_price,
            "high": high_price,
            "low": low_price,
            "close": close_price,
            "volume": total_volume
        })
    return bars

def convert_bars_to_influx_points(bars: List[Dict], symbol: str, exchange: str, measurement: str) -> List[Point]:
    """Converts aggregated bars into InfluxDB points."""
    points = []
    for bar in bars:
        point = (
            Point(measurement)
            .tag("symbol", symbol)
            .tag("exchange", exchange)
            .field("open", bar["open"])
            .field("high", bar["high"])
            .field("low", bar["low"])
            .field("close", bar["close"])
            .field("volume", bar["volume"])
            .time(bar["time"])
        )
        points.append(point)
    return points

def fetch_and_process_ticks(symbol: str, exchange: str, hist_conn: iq.HistoryConn, start_date: datetime, end_date: datetime):
    """Fetches ticks for a given period and processes them for all aggregation levels."""
    logging.info(f"[{symbol}] Fetching raw tick data from {start_date} to {end_date}.")
    try:
        raw_ticks = hist_conn.request_ticks_in_period(
            ticker=symbol,
            bgn_prd=start_date,
            end_prd=end_date,
            ascend=True
        )
        if raw_ticks is None or len(raw_ticks) == 0:
            logging.warning(f"[{symbol}] No raw tick data returned for the specified period.")
            return

        logging.info(f"[{symbol}] Fetched {len(raw_ticks)} ticks. Starting aggregation...")

        # Use a thread pool to process different aggregation levels in parallel
        with ThreadPoolExecutor() as executor:
            futures = []
            for level in AGGREGATION_LEVELS:
                future = executor.submit(process_aggregation_level, raw_ticks, symbol, exchange, level)
                futures.append(future)
            # Wait for all aggregation tasks to complete
            for future in futures:
                future.result()

    except iq.NoDataError:
        logging.warning(f"[{symbol}] IQFeed reported no tick data for the requested period.")
    except Exception as e:
        logging.error(f"[{symbol}] An error occurred during tick fetching and processing: {e}", exc_info=True)

def process_aggregation_level(raw_ticks: np.ndarray, symbol: str, exchange: str, level: int):
    """Processes a single aggregation level for a set of ticks."""
    measurement_name = f"ohlc_{level}tick"
    logging.info(f"[{symbol}] Aggregating for {measurement_name}...")
    
    aggregated_bars = aggregate_ticks_to_bars(raw_ticks, level)
    if not aggregated_bars:
        logging.info(f"[{symbol}] No bars were created for {measurement_name}.")
        return

    influx_points = convert_bars_to_influx_points(aggregated_bars, symbol, exchange, measurement_name)
    if not influx_points:
        logging.info(f"[{symbol}] No InfluxDB points to write for {measurement_name}.")
        return

    try:
        write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=influx_points)
        logging.info(f"[{symbol}] Successfully wrote {len(influx_points)} points to {measurement_name}.")
    except Exception as e:
        logging.error(f"[{symbol}] Failed to write to InfluxDB for {measurement_name}: {e}")

def run_tick_ingestion(symbols: List[str], exchange: str, days_to_backfill: int = 7):
    """Main function to run the tick ingestion and aggregation process."""
    logging.info("--- Starting Tick Data Aggregation Service ---")
    hist_conn = get_iqfeed_history_conn()
    if not hist_conn:
        logging.error("Failed to get IQFeed history connection. Aborting.")
        return

    with iq.ConnConnector([hist_conn]):
        end_date = get_last_completed_session_end_time_utc()
        start_date = end_date - timedelta(days=days_to_backfill)

        for symbol in symbols:
            logging.info(f"--- Processing symbol: {symbol} ---")
            # Here, you could add logic to check the latest timestamp in your tick-bar measurements
            # and adjust `start_date` accordingly to prevent reprocessing.
            fetch_and_process_ticks(symbol, exchange, hist_conn, start_date, end_date)

    logging.info("--- Tick Data Aggregation Service Finished ---")
    influx_pool.close_all()

if __name__ == "__main__":
    symbols_to_process = ["AAPL", "AMZN", "TSLA", "@NQM25"]
    exchange_name = "NASDAQ"
    run_tick_ingestion(symbols_to_process, exchange_name)