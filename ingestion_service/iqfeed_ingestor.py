import os
import logging
import time
import asyncio
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from datetime import datetime as dt, timezone, time as dt_time, timedelta
import pytz
# For timezone-aware datetime objects
try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo

import numpy as np
from dotenv import load_dotenv
from influxdb_client import InfluxDBClient, Point, WriteOptions, WritePrecision
from influxdb_client.client.write_api import ASYNCHRONOUS
import queue
import threading
from typing import List, Optional, Tuple
import multiprocessing as mp

# NEW IMPORTS FOR SCHEDULER
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger

# Local imports from your project structure
import pyiqfeed as iq
from dtn_iq_client import get_iqfeed_history_conn
from config import settings

# --- Configuration ---
load_dotenv()
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# InfluxDB Configuration
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

# Global connection pool
influx_pool = InfluxDBPool()
query_api = influx_pool.clients[0].query_api()


def chunk_array(arr: np.ndarray, chunk_size: int) -> List[np.ndarray]:
    """Split numpy array into chunks for parallel processing"""
    chunks = []
    for i in range(0, len(arr), chunk_size):
        chunks.append(arr[i:i + chunk_size])
    return chunks


def format_tick_chunk_for_influx(
    tick_chunk: np.ndarray,
    symbol: str,
    exchange: str,
    measurement: str,
    source_timezone: ZoneInfo,
    end_time_utc_cutoff: Optional[dt] = None
) -> List[Point]:
    """
    Optimized function to format a chunk of tick data for InfluxDB.
    Designed for parallel processing.
    """
    points = []
    
    try:
        for tick in tick_chunk:
            # Convert the tick timestamp
            naive_timestamp_dt = iq.date_us_to_datetime(tick['date'], tick['time'])
            aware_timestamp_dt = naive_timestamp_dt.replace(tzinfo=source_timezone)

            if end_time_utc_cutoff:
                timestamp_utc = aware_timestamp_dt.astimezone(timezone.utc)
                if timestamp_utc > end_time_utc_cutoff:
                    continue

            unix_timestamp_microseconds = int(aware_timestamp_dt.timestamp() * 1_000_000)

            # Create InfluxDB point for tick data
            point = (
                Point(measurement)
                .tag("symbol", symbol)
                .tag("exchange", exchange)
                .tag("market_center", str(tick['mkt_ctr']))
                .tag("last_type", tick['last_type'].decode('utf-8') if isinstance(tick['last_type'], bytes) else str(tick['last_type']))
                .field("last_price", float(tick['last']))
                .field("last_size", int(tick['last_sz']))
                .field("total_volume", int(tick['tot_vlm']))
                .field("bid", float(tick['bid']))
                .field("ask", float(tick['ask']))
                .field("tick_id", int(tick['tick_id']))
                .field("condition_1", int(tick['cond1']))
                .field("condition_2", int(tick['cond2']))
                .field("condition_3", int(tick['cond3']))
                .field("condition_4", int(tick['cond4']))
                .time(unix_timestamp_microseconds, write_precision=WritePrecision.US)
            )
            points.append(point)
    except Exception as e:
        logging.error(f"Error processing tick chunk: {e}")
    
    return points


class AsyncTickWriter:
    """Asynchronous tick data writer with queue-based processing"""
    
    def __init__(self):
        self.write_queue = queue.Queue(maxsize=QUEUE_SIZE)
        self.running = False
        self.writer_threads = []
        
    def start_writers(self):
        """Start multiple writer threads"""
        self.running = True
        
        for i in range(CONCURRENT_WRITES):
            writer_thread = threading.Thread(
                target=self._writer_worker,
                args=(i,),
                name=f"TickWriter-{i}"
            )
            writer_thread.daemon = True
            writer_thread.start()
            self.writer_threads.append(writer_thread)
            
        logging.info(f"Started {CONCURRENT_WRITES} async tick writers")
    
    def _writer_worker(self, worker_id: int):
        """Worker thread for writing data to InfluxDB"""
        write_api = influx_pool.get_write_api(worker_id)
        
        while self.running:
            try:
                # Get batch from queue with timeout
                batch = self.write_queue.get(timeout=1.0)
                
                if batch is None:  # Shutdown signal
                    break
                    
                # Write to InfluxDB
                write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=batch)
                self.write_queue.task_done()
                
            except queue.Empty:
                continue
            except Exception as e:
                logging.error(f"Writer {worker_id} error: {e}")
                
    def add_batch(self, batch: List[Point], timeout=5.0):
        """Add a batch to the write queue"""
        try:
            self.write_queue.put(batch, timeout=timeout)
        except queue.Full:
            logging.warning("Write queue is full, dropping batch")
    
    def stop_writers(self):
        """Stop all writer threads"""
        self.running = False
        
        # Send shutdown signals
        for _ in range(CONCURRENT_WRITES):
            try:
                self.write_queue.put(None, timeout=1.0)
            except queue.Full:
                pass
        
        # Wait for threads to finish
        for thread in self.writer_threads:
            thread.join(timeout=5.0)
            
        logging.info("Stopped async tick writers")


def fetch_and_store_tick_data_optimized(symbol: str, exchange: str, hist_conn: iq.HistoryConn):
    """
    High-performance tick data fetching and storage with parallel processing.
    """
    start_time = time.time()
    logging.info(f"Starting OPTIMIZED tick data ingestion for {symbol}...")
    
    measurement = "ticks"
    latest_timestamp = get_latest_timestamp(symbol, measurement)
    last_session_end_utc = get_last_completed_session_end_time_utc()
    
    try:
        # Determine the time range for tick data
        if latest_timestamp:
            start_dt_for_request = latest_timestamp
        else:
            # If no existing tick data, start from 7 days ago (typical tick data retention)
            start_dt_for_request = last_session_end_utc - timedelta(days=7)
        
        end_dt_for_request = last_session_end_utc
        
        if start_dt_for_request >= end_dt_for_request:
            logging.info(f"Tick data for {symbol} is already up-to-date. Skipping fetch.")
            return
        
        logging.info(f"Requesting tick data for {symbol} from {start_dt_for_request} to {end_dt_for_request}")
        
        # Convert to Eastern Time for IQFeed request
        et_timezone = ZoneInfo("America/New_York")
        start_et = start_dt_for_request.astimezone(et_timezone).replace(tzinfo=None)
        end_et = end_dt_for_request.astimezone(et_timezone).replace(tzinfo=None)
        
        # === STEP 1: FETCH DATA (Optimized) ===
        fetch_start = time.time()
        logging.info(f"Fetching tick data from DTN...")
        
        tick_data = hist_conn.request_ticks_in_period(
            ticker=symbol,
            bgn_prd=start_et,
            end_prd=end_et,
            ascend=True,
            max_ticks=None,  # Get all available ticks
            timeout=600  # 10 minutes timeout for very large requests
        )
        
        fetch_time = time.time() - fetch_start
        
        if tick_data is None or len(tick_data) == 0:
            logging.warning(f"No tick data returned for {symbol}.")
            return
            
        logging.info(f"Fetched {len(tick_data)} tick records for {symbol} in {fetch_time:.2f}s")
        
        # === STEP 2: PARALLEL PROCESSING ===
        process_start = time.time()
        logging.info(f"Starting parallel processing of {len(tick_data)} ticks...")
        
        # Start async writers
        async_writer = AsyncTickWriter()
        async_writer.start_writers()
        
        try:
            # Split data into chunks for parallel processing
            tick_chunks = chunk_array(tick_data, TICK_BATCH_SIZE)
            logging.info(f"Split data into {len(tick_chunks)} chunks of ~{TICK_BATCH_SIZE} ticks each")
            
            # Process chunks in parallel using ThreadPoolExecutor
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                futures = []
                
                for i, chunk in enumerate(tick_chunks):
                    future = executor.submit(
                        format_tick_chunk_for_influx,
                        chunk,
                        symbol,
                        exchange,
                        measurement,
                        et_timezone,
                        last_session_end_utc
                    )
                    futures.append((i, future))
                
                # Process results as they complete
                total_points = 0
                for chunk_idx, future in futures:
                    try:
                        points = future.result(timeout=120)  # 2 minutes per chunk
                        
                        if points:
                            # Add to async write queue
                            async_writer.add_batch(points)
                            total_points += len(points)
                            
                            if chunk_idx % 10 == 0:  # Progress update every 10 chunks
                                logging.info(f"Processed chunk {chunk_idx + 1}/{len(tick_chunks)}, "
                                           f"total points: {total_points}")
                                
                    except Exception as e:
                        logging.error(f"Error processing chunk {chunk_idx}: {e}")
            
            # Wait for all writes to complete
            logging.info("Waiting for all writes to complete...")
            async_writer.write_queue.join()
            
            process_time = time.time() - process_start
            total_time = time.time() - start_time
            
            logging.info(f"Successfully processed {total_points} tick points for {symbol}")
            logging.info(f"Performance metrics:")
            logging.info(f"  - Fetch time: {fetch_time:.2f}s")
            logging.info(f"  - Processing time: {process_time:.2f}s") 
            logging.info(f"  - Total time: {total_time:.2f}s")
            logging.info(f"  - Throughput: {total_points/total_time:.0f} points/second")
            
        finally:
            # Clean up async writers
            async_writer.stop_writers()
            
    except iq.NoDataError:
        logging.warning(f"IQFeed reported NoDataError for tick data on {symbol}.")
    except Exception as e:
        logging.error(f"An error occurred while fetching tick data for {symbol}: {e}", exc_info=True)


def get_latest_timestamp(symbol: str, measurement: str) -> dt | None:
    """
    Queries InfluxDB for the latest timestamp for a given symbol and measurement.
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


def get_last_completed_session_end_time_utc() -> dt:
    """
    Determines the timestamp of the end of the last fully completed trading session (8 PM ET).
    """
    et_zone = ZoneInfo("America/New_York")
    now_et = dt.now(et_zone)
    
    target_date_et = now_et.date()
    
    if now_et.time() < dt_time(20, 0):
        target_date_et -= timedelta(days=1)
        
    session_end_et = dt.combine(target_date_et, dt_time(20, 0), tzinfo=et_zone)
    
    return session_end_et.astimezone(timezone.utc)


def is_nasdaq_trading_hours(check_time_utc: dt | None = None) -> bool:
    """
    Checks if a given UTC time falls within NASDAQ trading hours (9:30 AM to 4:00 PM ET)
    on a weekday.
    """
    et_zone = ZoneInfo("America/New_York")

    if check_time_utc is None:
        check_time_utc = dt.now(timezone.utc)

    et_time = check_time_utc.astimezone(et_zone)

    if et_time.weekday() >= 5:
        logging.info("Skipping operation: It's the weekend, NASDAQ is closed.")
        return False

    trading_start = dt_time(9, 30)
    trading_end = dt_time(16, 0)

    if trading_start <= et_time.time() <= trading_end:
        logging.warning(
            f"Current time {et_time.time()} is within NASDAQ trading hours (9:30 AM - 4:00 PM ET). Deferring historical operations.")
        return True

    logging.info(f"Current time {et_time.time()} is outside NASDAQ trading hours. Safe to proceed.")
    return False


# === EXISTING FUNCTIONS (keep your original OHLC functions) ===

def format_data_for_influx(
    dtn_data: np.ndarray,
    symbol: str,
    exchange: str,
    measurement: str,
    end_time_utc_cutoff: dt | None = None
) -> list[Point]:
    """
    Converts NumPy array from pyiqfeed to InfluxDB Points, filtering out records
    after the specified UTC cutoff time as a safeguard.
    """
    points = []
    has_time_field = 'time' in dtn_data.dtype.names
    has_prd_vlm = 'prd_vlm' in dtn_data.dtype.names
    has_tot_vlm = 'tot_vlm' in dtn_data.dtype.names

    source_timezone = ZoneInfo("America/New_York")

    for rec in dtn_data:
        if has_time_field:
            naive_timestamp_dt = iq.date_us_to_datetime(rec['date'], rec['time'])
        else:
            daily_date = iq.datetime64_to_date(rec['date'])
            naive_timestamp_dt = dt.combine(daily_date, dt.min.time())

        aware_timestamp_dt = naive_timestamp_dt.replace(tzinfo=source_timezone)

        if end_time_utc_cutoff:
            timestamp_utc = aware_timestamp_dt.astimezone(timezone.utc)
            if timestamp_utc > end_time_utc_cutoff:
                continue

        unix_timestamp_microseconds = int(aware_timestamp_dt.timestamp() * 1_000_000)

        volume = 0
        if has_prd_vlm:
            volume = int(rec['prd_vlm'])
        elif has_tot_vlm:
            volume = int(rec['tot_vlm'])

        point = (
            Point(measurement)
            .tag("symbol", symbol)
            .tag("exchange", exchange)
            .field("open", float(rec['open_p']))
            .field("high", float(rec['high_p']))
            .field("low", float(rec['low_p']))
            .field("close", float(rec['close_p']))
            .field("volume", volume)
            .time(unix_timestamp_microseconds, write_precision=WritePrecision.US)
        )
        points.append(point)
        
    return points


def fetch_and_store_history(symbol: str, exchange: str, hist_conn: iq.HistoryConn):
    """
    Fetches history and filters it to ensure only data from complete trading
    sessions is stored.
    """
    logging.info(f"Starting historical data ingestion for {symbol}...")

    timeframes_to_fetch = {
        "1s":   {"interval": 1,    "type": "s", "days": 7},
        "5s":   {"interval": 5,    "type": "s", "days": 7},
        "10s":  {"interval": 10,   "type": "s", "days": 7},
        "15s":  {"interval": 15,   "type": "s", "days": 7},
        "30s":  {"interval": 30,   "type": "s", "days": 7},
        "45s":  {"interval": 45,   "type": "s", "days": 7},
        "1m":   {"interval": 60,   "type": "s", "days": 180},
        "5m":   {"interval": 300,  "type": "s", "days": 180},
        "10m":  {"interval": 600,  "type": "s", "days": 180},
        "15m":  {"interval": 900,  "type": "s", "days": 180},
        "30m":  {"interval": 1800, "type": "s", "days": 180},
        "45m":  {"interval": 2700, "type": "s", "days": 180},
        "1h":   {"interval": 3600, "type": "s", "days": 180},
        "1d":   {"interval": 1,    "type": "d", "days": 10000}
    }

    last_session_end_utc = get_last_completed_session_end_time_utc()
    logging.info(f"Data will be filtered to on or before last session end: {last_session_end_utc}")

    for tf_name, params in timeframes_to_fetch.items():
        try:
            measurement = f"ohlc_{tf_name}"
            latest_timestamp = get_latest_timestamp(symbol, measurement)
            
            dtn_data = None
            
            if params['type'] != 'd':  # For all intraday intervals
                
                start_dt_for_request = None
                if latest_timestamp:
                    start_dt_for_request = latest_timestamp
                else:
                    start_dt_for_request = last_session_end_utc - timedelta(days=params['days'])
                
                end_dt_for_request = last_session_end_utc

                if start_dt_for_request >= end_dt_for_request:
                    logging.info(f"Data for {tf_name} is already up-to-date. Skipping fetch.")
                    continue
                
                logging.info(f"Requesting {tf_name} data from {start_dt_for_request} to {end_dt_for_request}")

                dtn_data = hist_conn.request_bars_in_period(
                    ticker=symbol,
                    interval_len=params['interval'],
                    interval_type=params['type'],
                    bgn_prd=start_dt_for_request,
                    end_prd=end_dt_for_request,
                    ascend=True
                 )
            else:  # For '1d' daily data
                days_to_fetch = params['days']
                if latest_timestamp:
                    days_to_fetch = (dt.now(timezone.utc) - latest_timestamp).days + 1

                if days_to_fetch <= 0:
                    logging.info(f"Daily data for {symbol} is up-to-date. Skipping.")
                    continue
                
                dtn_data = hist_conn.request_daily_data(ticker=symbol, num_days=days_to_fetch, ascend=True)

            if dtn_data is not None and len(dtn_data) > 0:
                logging.info(f"Fetched {len(dtn_data)} records for {tf_name}.")
                
                influx_points = format_data_for_influx(
                    dtn_data, symbol, exchange, measurement,
                    end_time_utc_cutoff=last_session_end_utc
                )
                
                if not influx_points:
                    logging.warning(f"No new {tf_name} data for {symbol} on or before the last session end time.")
                    continue

                logging.info(f"Writing {len(influx_points)} filtered points to InfluxDB for '{measurement}'.")
                write_api = influx_pool.get_write_api(0)
                write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=influx_points)
            else:
                logging.warning(f"No new {tf_name} data returned for {symbol}.")

        except iq.NoDataError:
            logging.warning(f"IQFeed reported NoDataError for {symbol} on {tf_name}.")
        except Exception as e:
            logging.error(f"An error occurred while fetching {tf_name} for {symbol}: {e}", exc_info=True)
        time.sleep(1)


def daily_update(symbols_to_update: list, exchange: str, include_ticks: bool = True):
    """
    Performs the daily update, respecting trading hours.
    Now uses optimized tick data processing.
    """
    logging.info("--- Checking conditions for Daily Update Process ---")
    if is_nasdaq_trading_hours():
        logging.warning("Aborting daily update: operation is not permitted during trading hours.")
        return

    logging.info("--- Starting Daily Update Process ---")
    hist_conn = get_iqfeed_history_conn()
    if hist_conn is None:
        logging.error("Could not get IQFeed connection. Aborting daily update.")
        return

    with iq.ConnConnector([hist_conn]):
        for symbol in symbols_to_update:
            try:
                # Fetch OHLC bar data
                fetch_and_store_history(symbol, exchange, hist_conn)
                
                # Fetch tick data if requested (OPTIMIZED)
                if include_ticks:
                    fetch_and_store_tick_data_optimized(symbol, exchange, hist_conn)
                    
            except Exception as e:
                logging.error(f"Error processing symbol {symbol}: {e}", exc_info=True)
                continue

    logging.info("--- Daily Update Process Finished ---")


def scheduled_daily_update():
    """
    Wrapper function for the scheduler. Defines the symbols and exchange to update.
    """
    logging.info("--- Triggering Scheduled Daily Update ---")
    symbols_to_update = ["AAPL", "AMZN", "TSLA", "@NQM25"]
    exchange = "NASDAQ"
    
    # Include tick data in the update
    include_tick_data = True
    
    daily_update(symbols_to_update, exchange, include_ticks=include_tick_data)


def manual_tick_data_backfill_optimized(symbols: list, exchange: str, days_back: int = 7):
    """
    Optimized manual function to backfill tick data for specified symbols.
    """
    logging.info(f"--- Starting OPTIMIZED Manual Tick Data Backfill for {days_back} days ---")
    
    if is_nasdaq_trading_hours():
        logging.warning("Running during trading hours - this may impact performance.")
    
    hist_conn = get_iqfeed_history_conn()
    if hist_conn is None:
        logging.error("Could not get IQFeed connection. Aborting tick data backfill.")
        return

    with iq.ConnConnector([hist_conn]):
        for symbol in symbols:
            try:
                logging.info(f"Processing OPTIMIZED tick data for {symbol}...")
                fetch_and_store_tick_data_optimized(symbol, exchange, hist_conn)
            except Exception as e:
                logging.error(f"Error processing tick data for {symbol}: {e}", exc_info=True)
                continue

    logging.info("--- Manual Tick Data Backfill Finished ---")


# === CLEANUP FUNCTION ===
def cleanup_resources():
    """Clean up resources on exit"""
    try:
        influx_pool.close_all()
        logging.info("InfluxDB connection pool closed.")
    except Exception as e:
        logging.error(f"Error during cleanup: {e}")


if __name__ == '__main__':

    try:
        logging.info("Starting optimized data ingestion...")
        
        # For testing the optimized tick ingestion
        # manual_tick_data_backfill_optimized(["AAPL"], "NASDAQ", days_back=3)
        
        # Regular scheduled update
        scheduled_daily_update()    

        # --- RUN AS A PERSISTENT SCHEDULER SERVICE ---
        logging.info("Initializing historical ingestion scheduler...")
        
        scheduler = BlockingScheduler(timezone="America/New_York")

        scheduler.add_job(
            scheduled_daily_update,
            trigger=CronTrigger(
                hour=20, 
                minute=1, 
                second=0,
                timezone=pytz.timezone('America/New_York')
            ),
            name="Daily Historical Market Data Ingestion"
        )
        
        logging.info("Scheduler started. Waiting for jobs to run...")
        logging.info("Press Ctrl+C to exit.")

        scheduler.start()
        
    except (KeyboardInterrupt, SystemExit):
        logging.info("Scheduler stopped. Shutting down...")
    finally:
        cleanup_resources()