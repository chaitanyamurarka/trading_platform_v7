# ingestion_service/tick_aggregator.py

import logging
import time
import numpy as np
import pandas as pd
from datetime import datetime, timedelta, timezone as dt_timezone
from typing import List, Optional
from concurrent.futures import ThreadPoolExecutor
import multiprocessing as mp
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
import pytz
from zoneinfo import ZoneInfo
import signal

# Local imports
from dtn_iq_client import get_iqfeed_history_conn
from iqfeed_ingestor import get_latest_timestamp
from config import settings
import pyiqfeed as iq
from influxdb_client import Point, InfluxDBClient, WriteOptions

# --- Configuration ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(processName)s - %(module)s - %(message)s')

# --- InfluxDB Setup ---
INFLUX_URL = settings.INFLUX_URL
INFLUX_TOKEN = settings.INFLUX_TOKEN
INFLUX_ORG = settings.INFLUX_ORG
INFLUX_BUCKET = settings.INFLUX_BUCKET

# --- PERFORMANCE CONFIGURATION ---
# OPTIMIZATION: MAX_PROCESSES defines how many symbols to process in parallel.
# Set this based on your CPU cores and IQFeed's ability to handle concurrent requests.
MAX_PROCESSES = max(1, mp.cpu_count() // 2) # Use half the CPU cores to be safe
# OPTIMIZATION: MAX_THREADS_PER_PROCESS defines threads for I/O tasks within each process.
MAX_THREADS_PER_PROCESS = 4 # For concurrent DB writes within a single symbol's process
WRITE_BATCH_SIZE = 50000  # Optimized for InfluxDB performance
CONCURRENT_WRITES = 4  # Number of concurrent InfluxDB writers per pool

# --- Optimized InfluxDB Connection Pool ---
class InfluxDBPool:
    """Connection pool for InfluxDB with async writers, to be used within a single process."""
    def __init__(self, pool_size=CONCURRENT_WRITES):
        self.pool_size = pool_size
        self.clients = []
        self.write_apis = []
        self._create_pool()

    def _create_pool(self):
        for i in range(self.pool_size):
            client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG, timeout=60_000)
            write_api = client.write_api(write_options=WriteOptions(
                batch_size=WRITE_BATCH_SIZE,
                flush_interval=5_000,
                jitter_interval=1_000,
                retry_interval=2_000,
                max_retries=3
            ))
            self.clients.append(client)
            self.write_apis.append(write_api)

    def get_write_api(self, index=0):
        return self.write_apis[index % self.pool_size]

    def close_all(self):
        for write_api in self.write_apis:
            write_api.close()
        for client in self.clients:
            client.close()

# --- Aggregation Configuration ---
AGGREGATION_LEVELS = [1, 10, 1000]

# --- OPTIMIZATION: New Vectorized Aggregation ---
def vectorized_aggregate_ticks_to_bars(ticks: np.ndarray, ticks_per_bar: int, symbol: str) -> Optional[pd.DataFrame]:
    """
    Vectorized aggregation of raw ticks into OHLCV bars using pandas.
    This is significantly faster than looping through ticks.
    """
    if len(ticks) == 0:
        return None

    logging.info(f"[{symbol}] Vectorized Aggregation: Processing {len(ticks)} ticks for {ticks_per_bar}-tick bars.")
    df = pd.DataFrame(ticks)
    
    # Create a grouper key. Each group of 'ticks_per_bar' will have the same key.
    grouper = np.arange(len(df)) // ticks_per_bar
    
    # Perform the aggregation using groupby().agg(). This is the core optimization.
    agg_df = df.groupby(grouper).agg(
        open=('last', 'first'),
        high=('last', 'max'),
        low=('last', 'min'),
        close=('last', 'last'),
        volume=('last_sz', 'sum'),
        # Get date and time from the last tick in the group for the timestamp
        date=('date', 'last'),
        time=('time', 'last')
    ).astype({'volume': 'int64'})

    # Vectorized Timestamp Conversion
    timestamps_ns = agg_df['date'].values.astype('datetime64[D]') + agg_df['time'].values.astype('timedelta64[us]')
    agg_df['timestamp'] = pd.to_datetime(timestamps_ns, utc=False).tz_localize('America/New_York').tz_convert('UTC')
    agg_df.drop(columns=['date', 'time'], inplace=True)
    agg_df.set_index('timestamp', inplace=True)

    logging.info(f"[{symbol}] Vectorized Aggregation: Created {len(agg_df)} bars.")
    return agg_df

# --- OPTIMIZATION: DataFrame-based InfluxDB Writers ---
def log_raw_ticks_to_influx(raw_ticks: np.ndarray, symbol: str, write_api):
    """Writes raw ticks to InfluxDB from a DataFrame."""
    measurement_name = "raw_ticks"
    if len(raw_ticks) == 0:
        return "No raw ticks to log"
    try:
        df = pd.DataFrame(raw_ticks)
        timestamps_ns = df['date'].values.astype('datetime64[ns]') + df['time'].values.astype('timedelta64[us]')
        df['time'] = pd.to_datetime(timestamps_ns, utc=False).tz_localize('America/New_York').tz_convert('UTC')
        df.rename(columns={'last_sz': 'last_size', 'tot_vlm': 'total_volume'}, inplace=True)
        df['symbol'] = symbol

        write_api.write(
            bucket=INFLUX_BUCKET,
            record=df,
            data_frame_measurement_name=measurement_name,
            data_frame_tag_columns=['symbol'],
            data_frame_timestamp_column='time'
        )
        logging.info(f"[{symbol}] ==> Initiated write of {len(df)} raw tick points.")
        return f"Logged {len(df)} points to {measurement_name}"
    except Exception as e:
        logging.error(f"[{symbol}] ==> Failed to log raw ticks: {e}", exc_info=True)
        return "Error logging raw ticks"

def process_aggregation_level_vectorized(raw_ticks: np.ndarray, symbol: str, exchange: str, level: int, write_api):
    """Processes a single aggregation level using the vectorized function and writes to InfluxDB."""
    aggregated_df = vectorized_aggregate_ticks_to_bars(raw_ticks, level, symbol)
    if aggregated_df is None or aggregated_df.empty:
        return "No bars created"
    try:
        aggregated_df['_measurement'] = aggregated_df.index.strftime(f'ohlc_{symbol}_%Y%m%d_{level}tick')
        aggregated_df['symbol'] = symbol
        aggregated_df['exchange'] = exchange

        # FIX: Group by the '_measurement' column and write each group separately
        grouped_by_measurement = aggregated_df.groupby('_measurement')
        logging.info(f"Writing {len(aggregated_df)} aggregated points to {len(grouped_by_measurement)} measurements for level {level}tick...")

        for name, group_df in grouped_by_measurement:
            write_api.write(
                bucket=INFLUX_BUCKET,
                record=group_df,
                data_frame_measurement_name=name, # Use the actual measurement name for each group
                data_frame_tag_columns=['symbol', 'exchange']
            )
        
        logging.info(f"Write complete for level {level}tick.")
        return f"Wrote {len(aggregated_df)} points for level {level}tick"

    except Exception as e:
        logging.error(f"[{symbol}] ==> Failed to write for level {level}tick: {e}", exc_info=True)
        return f"Error writing for level {level}tick"
    
# --- OPTIMIZATION: Central Data Fetching & Processing Logic ---
def fetch_and_process_data(symbol: str, exchange: str, hist_conn: iq.HistoryConn, start_date: datetime, end_date: datetime, influx_pool: InfluxDBPool):
    """Fetches ticks and processes them using vectorized functions and a thread pool for I/O."""
    logging.info(f"[{symbol}] Fetching raw tick data from {start_date.isoformat()} to {end_date.isoformat()}.")
    try:
        raw_ticks = hist_conn.request_ticks_in_period(ticker=symbol, bgn_prd=start_date, end_prd=end_date)
        if raw_ticks is None or len(raw_ticks) == 0:
            logging.warning(f"[{symbol}] No raw tick data returned for the period.")
            return

        logging.info(f"[{symbol}] Fetched {len(raw_ticks)} ticks. Starting parallel logging and aggregation...")
        
        with ThreadPoolExecutor(max_workers=MAX_THREADS_PER_PROCESS) as executor:
            # Task 1: Log raw ticks
            log_future = executor.submit(log_raw_ticks_to_influx, raw_ticks, symbol, influx_pool.get_write_api(0))
            
            # Tasks 2...N: Process aggregations
            agg_futures = {
                executor.submit(
                    process_aggregation_level_vectorized,
                    raw_ticks, symbol, exchange, level, influx_pool.get_write_api(i + 1)
                ): level for i, level in enumerate(AGGREGATION_LEVELS)
            }
            
            # Check results
            logging.info(f"[{symbol}] Raw tick logging result: {log_future.result()}")
            for future in agg_futures:
                logging.info(f"[{symbol}] Aggregation result for level {agg_futures[future]}: {future.result()}")

    except iq.NoDataError:
        logging.warning(f"[{symbol}] IQFeed reported NoDataError for the requested period.")
    except Exception as e:
        logging.error(f"[{symbol}] An error occurred during data processing: {e}", exc_info=True)

# --- OPTIMIZATION: Worker Function for a Single Symbol (for Multiprocessing) ---
def process_symbol_worker(args: tuple):
    """
    Worker function to process a single symbol. Establishes its own connections.
    """
    symbol, exchange, days_to_backfill = args
    logging.info(f"[{symbol}] WORKER START: Processing symbol.")
    
    influx_pool = None
    hist_conn = None
    
    try:
        # Each process gets its own connection pool and IQFeed connection
        influx_pool = InfluxDBPool()
        hist_conn = get_iqfeed_history_conn()
        if not hist_conn:
            logging.error(f"[{symbol}] WORKER ERROR: Failed to get IQFeed connection.")
            return

        with iq.ConnConnector([hist_conn]):
            end_date = datetime.now(dt_timezone.utc)
            
            # Determine start date based on data in InfluxDB
            latest_timestamps = [get_latest_timestamp(symbol, f"{level}tick") for level in AGGREGATION_LEVELS]
            valid_timestamps = [ts for ts in latest_timestamps if ts is not None]
            
            start_date = min(valid_timestamps) if valid_timestamps else (end_date - timedelta(days=days_to_backfill))

            if start_date >= end_date:
                logging.info(f"[{symbol}] Data is up-to-date. Skipping.")
                return
            
            logging.info(f"[{symbol}] Determined fetch range. Start: {start_date}, End: {end_date}")
            fetch_and_process_data(symbol, exchange, hist_conn, start_date, end_date, influx_pool)

    except Exception as e:
        logging.error(f"[{symbol}] WORKER FAILED: An unhandled exception occurred: {e}", exc_info=True)
    finally:
        # Clean up resources for this worker process
        if influx_pool:
            influx_pool.close_all()
        logging.info(f"[{symbol}] WORKER END: Resources closed.")

# --- OPTIMIZATION: Main Orchestrator for Parallel Ingestion ---
def run_parallel_ingestion(symbols: List[str], exchange: str, days_to_backfill: int = 7):
    """Runs the ingestion process in parallel for multiple symbols using a process pool."""
    logging.info(f"--- Starting PARALLEL Tick Data Aggregation Service for {len(symbols)} symbols ---")
    
    # Prepare arguments for each worker process
    symbol_args = [(s, exchange, days_to_backfill) for s in symbols]
    
    # Use a multiprocessing Pool to run workers in parallel
    # The 'spawn' start method is safer and more compatible across platforms
    ctx = mp.get_context('spawn')
    with ctx.Pool(processes=min(len(symbols), MAX_PROCESSES)) as pool:
        pool.map(process_symbol_worker, symbol_args)

    logging.info("--- PARALLEL Tick Data Aggregation Service Finished ---")

def scheduled_tick_update():
    """Wrapper function for the scheduler to call the parallel ingestion."""
    logging.info("--- Triggering Scheduled Tick Update ---")
    symbols_to_process = ["AAPL", "AMZN", "TSLA", "@NQ#"]
    exchange_name = "NASDAQ"
    # Call the new parallel function
    run_parallel_ingestion(symbols_to_process, exchange_name)

if __name__ == "__main__":
    logging.info("Initializing tick aggregation scheduler...")
    scheduler = BlockingScheduler(timezone="America/New_York")

    scheduler.add_job(
        scheduled_tick_update,
        trigger=CronTrigger(hour=20, minute=30, timezone=pytz.timezone('America/New_York')),
        name="Daily Tick Data Aggregation"
    )

    def shutdown_handler(signum, frame):
        logging.warning(f"Shutdown signal {signum} received. Shutting down scheduler...")
        if scheduler.running:
            scheduler.shutdown(wait=False)
        logging.info("Scheduler shutdown initiated.")

    signal.signal(signal.SIGINT, shutdown_handler)
    signal.signal(signal.SIGTERM, shutdown_handler)

    try:
        logging.info("Running initial parallel tick data update on startup...")
        scheduled_tick_update()

        logging.info("Scheduler started. Press Ctrl+C to exit.")
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logging.info("Shutdown signal received, proceeding with cleanup.")
    except Exception as e:
        logging.error(f"An unexpected error occurred with the scheduler: {e}", exc_info=True)
    finally:
        if scheduler.running:
            scheduler.shutdown()
        logging.info("Scheduler shut down. Exiting.")