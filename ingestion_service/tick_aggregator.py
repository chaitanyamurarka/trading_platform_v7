# ingestion_service/tick_aggregator.py

import logging
import time
import numpy as np
import pandas as pd
from datetime import datetime, timedelta, timezone as dt_timezone
from typing import List, Optional, Dict
from concurrent.futures import ThreadPoolExecutor
import multiprocessing as mp
# NEW IMPORTS
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
import pytz
from zoneinfo import ZoneInfo
import signal 

# Local imports
from dtn_iq_client import get_iqfeed_history_conn
# MODIFIED IMPORT
from iqfeed_ingestor import get_latest_timestamp, get_last_completed_session_end_time_utc, is_nasdaq_trading_hours
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


# --- Aggregation Configuration ---
AGGREGATION_LEVELS = [1, 10, 1000] # e.g., 1-tick, 10-tick, 1000-tick bars
TICK_FETCH_BATCH_SIZE = 2_000_000 # Number of ticks to fetch from IQFeed at once

def aggregate_ticks_to_bars(ticks: np.ndarray, ticks_per_bar: int) -> List[Dict]:
    """
    Aggregates an array of raw ticks into OHLC bars.
    Correctly handles timestamps from IQFeed (assumed to be US/Eastern).
    """
    if len(ticks) == 0:
        return []

    logging.info(f"Aggregating {len(ticks)} ticks into bars of {ticks_per_bar} ticks.")
    # Log first and last tick for diagnostics
    if len(ticks) > 0:
        logging.debug(f"First raw tick received: {ticks[0]}")
        logging.debug(f"Last raw tick received: {ticks[-1]}")

    bars = []
    source_timezone = ZoneInfo("America/New_York") # IQFeed data is in ET

    for i in range(0, len(ticks), ticks_per_bar):
        chunk = ticks[i:i + ticks_per_bar]
        if len(chunk) == 0:
            continue

        # Get timestamp from the last tick in the chunk
        last_tick = chunk[-1]
        
        # --- FIX: Correct timezone handling ---
        # 1. Create a naive datetime object from the tick's date and time parts.
        naive_dt = iq.date_us_to_datetime(last_tick['date'], last_tick['time'])
        # 2. Localize the naive datetime to its source timezone (US/Eastern).
        aware_et_dt = naive_dt.replace(tzinfo=source_timezone)
        # 3. Convert the Eastern Time datetime to UTC for storage.
        utc_dt = aware_et_dt.astimezone(dt_timezone.utc)

        # Aggregate data for the bar
        open_price = float(chunk[0]['last'])
        high_price = float(np.max(chunk['last']))
        low_price = float(np.min(chunk['last']))
        close_price = float(last_tick['last'])
        total_volume = int(np.sum(chunk['last_sz']))

        bars.append({
            "time": utc_dt,
            "open": open_price,
            "high": high_price,
            "low": low_price,
            "close": close_price,
            "volume": total_volume
        })
    
    logging.info(f"Created {len(bars)} aggregated bars.")
    if len(bars) > 0:
        logging.debug(f"First aggregated bar: {bars[0]}")
        logging.debug(f"Last aggregated bar: {bars[-1]}")
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

# NEW: High-performance function to log raw ticks using a DataFrame
def log_raw_ticks_to_influx_df(raw_ticks: np.ndarray, symbol: str):
    """
    Takes raw tick data, converts it to a Pandas DataFrame, and writes it
    efficiently to a dedicated InfluxDB measurement.
    """
    measurement_name = "raw_ticks"
    logging.info(f"[{symbol}] ==> Vectorized Pre-processing: Preparing {len(raw_ticks)} raw ticks for InfluxDB DataFrame write...")

    if len(raw_ticks) == 0:
        logging.info(f"[{symbol}] No raw ticks to log.")
        return "No raw ticks to log"

    try:
        # 1. Convert numpy array to DataFrame
        df = pd.DataFrame(raw_ticks)

        # 2. Vectorized timestamp conversion
        timestamps_ns = df['date'].values.astype('datetime64[ns]') + df['time'].values.astype('timedelta64[us]')
        
        # 3. Localize to source timezone (ET) and convert to UTC. This becomes our timestamp column.
        df['time'] = pd.to_datetime(timestamps_ns, utc=False).tz_localize('America/New_York').tz_convert('UTC')

        # 4. Prepare DataFrame for InfluxDB write by renaming columns for clarity
        df.rename(columns={
            'last_sz': 'last_size',
            'tot_vlm': 'total_volume',
            'tick_id': 'tick_id',
            'last_type': 'basis_for_last',
            'mkt_ctr': 'trade_market_center'
        }, inplace=True)

        # Add the symbol column which will be used as a tag
        df['symbol'] = symbol

        # --- THE FIX ---
        # No longer manually selecting columns. We will pass the whole DataFrame.
        # The InfluxDB client will correctly use 'time' as the timestamp, 'symbol' as a tag,
        # and all other columns as fields.
        
        # 5. Write the entire DataFrame in one go
        write_api = influx_pool.get_write_api(0)
        write_api.write(
            bucket=INFLUX_BUCKET,
            org=INFLUX_ORG,
            record=df,  # Pass the entire, corrected DataFrame
            data_frame_measurement_name=measurement_name,
            data_frame_tag_columns=['symbol'],
            data_frame_timestamp_column='time' # Tell the client which column is the timestamp
        )
        
        logging.info(f"[{symbol}] ==> Successfully initiated DataFrame write of {len(df)} points to '{measurement_name}'.")
        return f"Logged {len(df)} points to {measurement_name}"

    except Exception as e:
        logging.error(f"[{symbol}] ==> Failed to log raw ticks via DataFrame for {measurement_name}: {e}", exc_info=True)
        return f"Error logging to {measurement_name}"
    
# NEW: High-performance function to process aggregations using a DataFrame
def process_aggregation_level_df(raw_ticks: np.ndarray, symbol: str, exchange: str, level: int):
    """Processes a single aggregation level and writes to InfluxDB using DataFrames."""
    measurement_name = f"ohlc_{level}tick"
    logging.info(f"[{symbol}] ==> Starting aggregation for {measurement_name}...")

    # The aggregation logic itself is already fast
    aggregated_bars = aggregate_ticks_to_bars(raw_ticks, level)
    
    if not aggregated_bars:
        logging.info(f"[{symbol}] No bars were created for {measurement_name}.")
        return "No bars created"

    try:
        # Convert list of dicts to a DataFrame
        df = pd.DataFrame(aggregated_bars)
        
        # The timestamp is already a UTC-aware datetime object, set it as the index for the write
        df.set_index('time', inplace=True)
        
        # Add tag columns
        df['symbol'] = symbol
        df['exchange'] = exchange
        
        # Write the entire DataFrame in one go
        writer_index = level % CONCURRENT_WRITES
        write_api = influx_pool.get_write_api(writer_index)
        
        write_api.write(
            bucket=INFLUX_BUCKET,
            org=INFLUX_ORG,
            record=df,
            data_frame_measurement_name=measurement_name,
            data_frame_tag_columns=['symbol', 'exchange']
        )
        
        logging.info(f"[{symbol}] ==> Successfully initiated DataFrame write of {len(df)} points to {measurement_name}.")
        return f"Wrote {len(df)} points to {measurement_name}"

    except Exception as e:
        logging.error(f"[{symbol}] ==> Failed to write to InfluxDB via DataFrame for {measurement_name}: {e}", exc_info=True)
        return f"Error writing to {measurement_name}"


# MODIFIED: Update fetch_and_process_ticks to call the new DataFrame functions
def fetch_and_process_ticks(symbol: str, exchange: str, hist_conn: iq.HistoryConn, start_date: datetime, end_date: datetime):
    """Fetches ticks, logs them, and processes them using high-performance DataFrame operations."""
    logging.info(f"[{symbol}] Fetching raw tick data from {start_date.isoformat()} to {end_date.isoformat()}.")
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

        logging.info(f"[{symbol}] Fetched {len(raw_ticks)} ticks. Starting parallel DataFrame logging and aggregation...")

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            # Task 1: Log the raw ticks using the new DataFrame method
            log_future = executor.submit(log_raw_ticks_to_influx_df, raw_ticks, symbol)
            
            # Tasks 2...N: Process each aggregation level using the new DataFrame method
            agg_futures = []
            for level in AGGREGATION_LEVELS:
                future = executor.submit(process_aggregation_level_df, raw_ticks, symbol, exchange, level)
                agg_futures.append(future)
            
            # Wait for all tasks to complete and check for any errors
            try:
                log_result = log_future.result()
                logging.info(f"[{symbol}] Raw tick logging task completed. Result: {log_result}")
            except Exception as exc:
                logging.error(f"[{symbol}] Raw tick logging task generated an exception: {exc}", exc_info=True)

            for future in agg_futures:
                try:
                    agg_result = future.result()
                    logging.info(f"[{symbol}] Aggregation task completed successfully. Result: {agg_result}")
                except Exception as exc:
                    logging.error(f"[{symbol}] An aggregation task generated an exception: {exc}", exc_info=True)

    except iq.NoDataError:
        logging.warning(f"[{symbol}] IQFeed reported NoDataError for the requested period.")
    except Exception as e:
        logging.error(f"[{symbol}] An error occurred during tick fetching and processing: {e}", exc_info=True)

def process_aggregation_level(raw_ticks: np.ndarray, symbol: str, exchange: str, level: int):
    """Processes a single aggregation level for a set of ticks."""
    measurement_name = f"ohlc_{level}tick"
    logging.info(f"[{symbol}] ==> Starting aggregation for {measurement_name}...")

    aggregated_bars = aggregate_ticks_to_bars(raw_ticks, level)
    if not aggregated_bars:
        logging.info(f"[{symbol}] No bars were created for {measurement_name}.")
        return f"No bars created for {measurement_name}"

    influx_points = convert_bars_to_influx_points(aggregated_bars, symbol, exchange, measurement_name)
    if not influx_points:
        logging.info(f"[{symbol}] No InfluxDB points to write for {measurement_name}.")
        return f"No points to write for {measurement_name}"

    try:
        # Using a round-robin approach to distribute write load
        writer_index = level % CONCURRENT_WRITES
        write_api = influx_pool.get_write_api(writer_index)
        
        write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=influx_points)
        logging.info(f"[{symbol}] ==> Successfully wrote {len(influx_points)} points to {measurement_name} using writer {writer_index}.")
        return f"Wrote {len(influx_points)} points to {measurement_name}"
    except Exception as e:
        logging.error(f"[{symbol}] ==> Failed to write to InfluxDB for {measurement_name}: {e}", exc_info=True)
        return f"Error writing to {measurement_name}"

def run_tick_ingestion(symbols: List[str], exchange: str, days_to_backfill: int = 7):
    """
    Main function to run the tick ingestion and aggregation process.
    - Removes trading hours check to run at any time.
    - Fetches data up to the current time.
    """
    logging.info("--- Starting Tick Data Aggregation Service ---")
    
    # REMOVED: is_nasdaq_trading_hours check to allow running anytime
    # if is_nasdaq_trading_hours():
    #     logging.warning("Aborting tick aggregation: operation is not permitted during trading hours.")
    #     return

    hist_conn = get_iqfeed_history_conn()
    if not hist_conn:
        logging.error("Failed to get IQFeed history connection. Aborting.")
        return

    with iq.ConnConnector([hist_conn]):
        # --- FIX: Fetch data up to the current time to include after-hours activity ---
        end_date = datetime.now(dt_timezone.utc)
        logging.info(f"Setting data fetch end time to current UTC time: {end_date.isoformat()}")

        for symbol in symbols:
            logging.info(f"--- Processing symbol: {symbol} ---")

            latest_timestamps = []
            for level in AGGREGATION_LEVELS:
                measurement = f"ohlc_{level}tick"
                # Use a more specific logger
                logging.info(f"[{symbol}] Checking latest timestamp for {measurement}...")
                latest_ts = get_latest_timestamp(symbol, measurement)
                if latest_ts:
                    logging.info(f"[{symbol}] Latest timestamp for {measurement} is {latest_ts.isoformat()}")
                    latest_timestamps.append(latest_ts)

            start_date = None
            if latest_timestamps:
                # Start fetching from the earliest last known point across all tick levels
                start_date = min(latest_timestamps)
                logging.info(f"[{symbol}] Found existing data. Setting start date to earliest known timestamp: {start_date.isoformat()}")
            else:
                # No data exists for any tick level, so backfill
                start_date = end_date - timedelta(days=days_to_backfill)
                logging.info(f"[{symbol}] No existing data found. Backfilling for {days_to_backfill} days. Start date: {start_date.isoformat()}")

            if start_date >= end_date:
                logging.info(f"[{symbol}] All tick data is already up-to-date (start_date >= end_date). Skipping.")
                continue

            fetch_and_process_ticks(symbol, exchange, hist_conn, start_date, end_date)

    logging.info("--- Tick Data Aggregation Service Finished ---")


def scheduled_tick_update():
    """Wrapper function for the scheduler."""
    logging.info("--- Triggering Scheduled Tick Update ---")
    symbols_to_process = ["AAPL", "AMZN", "TSLA", "@NQ#"]
    exchange_name = "NASDAQ"
    run_tick_ingestion(symbols_to_process, exchange_name)

if __name__ == "__main__":
    logging.info("Initializing tick aggregation scheduler...")
    # Make sure to initialize the pool at the start of the main block
    influx_pool = InfluxDBPool()

    scheduler = BlockingScheduler(timezone="America/New_York")

    # Add the scheduled job
    scheduler.add_job(
        scheduled_tick_update,
        trigger=CronTrigger(
            hour=20,
            minute=30,
            second=0,
            timezone=pytz.timezone('America/New_York')
        ),
        name="Daily Tick Data Aggregation"
    )

    def shutdown_handler(signum, frame):
        """Handle shutdown signals (like Ctrl+C) gracefully."""
        logging.warning(f"Received shutdown signal: {signum}. Shutting down scheduler...")
        if scheduler.running:
            scheduler.shutdown(wait=False) # Use wait=False to prevent blocking
        logging.info("Scheduler shutdown initiated.")

    # Register the handler for interrupt (Ctrl+C) and termination signals
    signal.signal(signal.SIGINT, shutdown_handler)
    signal.signal(signal.SIGTERM, shutdown_handler)

    try:
        logging.info("Running initial tick data update on startup...")
        scheduled_tick_update()

        logging.info("Scheduler started. Waiting for jobs to run...")
        logging.info("Press Ctrl+C or send a shutdown signal to exit.")
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logging.info("Shutdown signal received, proceeding with cleanup.")
    except Exception as e:
        logging.error(f"An unexpected error occurred with the scheduler: {e}", exc_info=True)
    finally:
        # This cleanup block will now run after the scheduler has been shut down
        if scheduler.running:
             logging.info("Final scheduler shutdown check.")
             scheduler.shutdown()

        if influx_pool:
            influx_pool.close_all()
            logging.info("InfluxDB connection pool closed. Exiting.")