import os
import logging
import time
from datetime import datetime as dt, timezone, time as dt_time, timedelta
import pytz
import re

# For timezone-aware datetime objects. ZoneInfo is in the standard library for Python 3.9+.
# If using an older version, you might need 'pip install backports.zoneinfo' or use 'pytz'.
try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
from dotenv import load_dotenv
from influxdb_client import InfluxDBClient, Point, WriteOptions, WritePrecision
# MODIFICATION: Import SYNCHRONOUS
from influxdb_client.client.write_api import SYNCHRONOUS
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

# --- InfluxDB Connection ---
influx_client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG, timeout=90_000)
# MODIFICATION: Using WriteOptions with smaller batch sizes and retry on failure
write_options = WriteOptions(
    batch_size=5000,  # Reduced from default to avoid connection timeouts
    flush_interval=10_000,  # 10 seconds
    jitter_interval=2_000,  # 2 seconds
    retry_interval=5_000,  # 5 seconds
    max_retries=3,
    max_retry_delay=30_000,
    exponential_base=2
)
write_api = influx_client.write_api(write_options=write_options)
query_api = influx_client.query_api()


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

def get_latest_timestamp(symbol: str, measurement_suffix: str) -> dt | None:
    sanitized_symbol = re.escape(symbol)
    measurement_regex = f"^ohlc_{sanitized_symbol}_\\d{{8}}_{measurement_suffix}$"
    flux_query = f'''
        from(bucket: "{INFLUX_BUCKET}")
          |> range(start: 0)
          |> filter(fn: (r) => r._measurement =~ /{measurement_regex}/ and r.symbol == "{symbol}")
          |> last() |> keep(columns: ["_time"])
    '''
    try:
        tables = query_api.query(query=flux_query)
        if not tables or not tables[0].records: return None
        latest_time = tables[0].records[0].get_time()
        return latest_time.replace(tzinfo=timezone.utc) if latest_time.tzinfo is None else latest_time
    except Exception: return None

def format_data_for_influx(dtn_data: np.ndarray, symbol: str, exchange: str, tf_name: str, end_time_utc_cutoff: dt | None = None) -> pd.DataFrame | None:
    """
    Converts NumPy array from pyiqfeed to a Pandas DataFrame ready for InfluxDB,
    with a '_measurement' column based on the timestamp of each record.
    """
    if len(dtn_data) == 0:
        return None

    df = pd.DataFrame(dtn_data)
    
    has_time_field = 'time' in dtn_data.dtype.names
    
    if has_time_field:
        # For intraday data
        timestamps_ns = df['date'].values.astype('datetime64[D]') + df['time'].values.astype('timedelta64[us]')
        df['timestamp'] = pd.to_datetime(timestamps_ns, utc=False).tz_localize('America/New_York')
    else:
        # For daily data
        df['timestamp'] = pd.to_datetime(df['date']).dt.tz_localize('America/New_York')

    if end_time_utc_cutoff:
        df = df[df['timestamp'].dt.tz_convert('UTC') <= end_time_utc_cutoff]

    if df.empty:
        return None
        
    # Create the dynamic measurement name for each row
    df['_measurement'] = df['timestamp'].dt.strftime(f'ohlc_{symbol}_%Y%m%d_{tf_name}')

    df.rename(columns={
        'open_p': 'open', 'high_p': 'high', 'low_p': 'low', 'close_p': 'close',
        'prd_vlm': 'volume', 'tot_vlm': 'total_volume' # Handle different volume fields
    }, inplace=True)

    # Ensure volume exists and is integer
    if 'volume' not in df.columns and 'total_volume' in df.columns:
        df['volume'] = df['total_volume']
    if 'volume' not in df.columns:
        df['volume'] = 0 # Default volume if none exists
        
    df['volume'] = df['volume'].astype('int64')

    # Add tags
    df['symbol'] = symbol
    df['exchange'] = exchange

    # Set timestamp as index for the writer
    df.set_index('timestamp', inplace=True)
    
    # Select and reorder columns for clarity before writing
    final_cols = ['open', 'high', 'low', 'close', 'volume', 'symbol', 'exchange', '_measurement']
    return df[[col for col in final_cols if col in df.columns]]

def write_data_in_chunks(influx_df: pd.DataFrame, chunk_size: int = 1000, max_retries: int = 3):
    """
    Write data to InfluxDB in smaller chunks to avoid connection timeouts.
    Includes retry logic for failed chunks.
    """
    grouped_by_measurement = influx_df.groupby('_measurement')
    total_measurements = len(grouped_by_measurement)
    total_points = len(influx_df)
    
    logging.info(f"Writing {total_points} points to {total_measurements} measurements...")
    
    successful_writes = 0
    failed_measurements = []
    
    for name, group_df in grouped_by_measurement:
        # For large groups, write in chunks
        if len(group_df) > chunk_size:
            chunks = [group_df[i:i+chunk_size] for i in range(0, len(group_df), chunk_size)]
            logging.debug(f"Splitting measurement {name} into {len(chunks)} chunks")
            
            for i, chunk in enumerate(chunks):
                retry_count = 0
                while retry_count < max_retries:
                    try:
                        write_api.write(
                            bucket=INFLUX_BUCKET,
                            record=chunk,
                            data_frame_measurement_name=name,
                            data_frame_tag_columns=['symbol', 'exchange']
                        )
                        successful_writes += len(chunk)
                        break
                    except Exception as e:
                        retry_count += 1
                        if retry_count >= max_retries:
                            logging.error(f"Failed to write chunk {i+1}/{len(chunks)} of {name} after {max_retries} retries: {e}")
                            failed_measurements.append((name, len(chunk)))
                        else:
                            logging.warning(f"Retry {retry_count}/{max_retries} for chunk {i+1}/{len(chunks)} of {name}")
                            time.sleep(2 ** retry_count)  # Exponential backoff
        else:
            # For smaller groups, write as single batch
            retry_count = 0
            while retry_count < max_retries:
                try:
                    write_api.write(
                        bucket=INFLUX_BUCKET,
                        record=group_df,
                        data_frame_measurement_name=name,
                        data_frame_tag_columns=['symbol', 'exchange']
                    )
                    successful_writes += len(group_df)
                    break
                except Exception as e:
                    retry_count += 1
                    if retry_count >= max_retries:
                        logging.error(f"Failed to write {name} after {max_retries} retries: {e}")
                        failed_measurements.append((name, len(group_df)))
                    else:
                        logging.warning(f"Retry {retry_count}/{max_retries} for {name}")
                        time.sleep(2 ** retry_count)
    
    logging.info(f"Write complete. Successfully wrote {successful_writes}/{total_points} points.")
    
    if failed_measurements:
        logging.error(f"Failed to write {len(failed_measurements)} measurements:")
        for measurement, count in failed_measurements:
            logging.error(f"  - {measurement}: {count} points")
    
    return successful_writes, failed_measurements

def fetch_and_store_history(symbol: str, exchange: str, hist_conn: iq.HistoryConn):
    timeframes_to_fetch = {
        "1s":   {"interval": 1,    "type": "s", "days": 10000},
        "5s":   {"interval": 5,    "type": "s", "days": 10000},
        "10s":  {"interval": 10,   "type": "s", "days": 10000},
        "15s":  {"interval": 15,   "type": "s", "days": 10000},
        "30s":  {"interval": 30,   "type": "s", "days": 10000},
        "45s":  {"interval": 45,   "type": "s", "days": 10000},
        "1m":   {"interval": 60,   "type": "s", "days": 10000},
        "5m":   {"interval": 300,  "type": "s", "days": 10000},
        "10m":  {"interval": 600,  "type": "s", "days": 10000},
        "15m":  {"interval": 900,  "type": "s", "days": 10000},
        "30m":  {"interval": 1800, "type": "s", "days": 10000},
        "45m":  {"interval": 2700, "type": "s", "days": 10000},
        "1h":   {"interval": 3600, "type": "s", "days": 10000},
        "1d":   {"interval": 1,    "type": "d", "days": 10000}
    }
    
    last_session_end_utc = get_last_completed_session_end_time_utc()

    for tf_name, params_raw in timeframes_to_fetch.items():
        try:
            params = {"interval": int(re.sub(r'\D', '', tf_name)), "type": re.sub(r'\d', '', tf_name), **params_raw}
            latest_timestamp = get_latest_timestamp(symbol, tf_name)
            dtn_data = None
            
            if params['type'] != 'd':
                start_dt = latest_timestamp or (last_session_end_utc - timedelta(days=params['days']))
                if start_dt >= last_session_end_utc: continue
                dtn_data = hist_conn.request_bars_in_period(ticker=symbol, interval_len=params['interval'], interval_type=params['type'], bgn_prd=start_dt, end_prd=last_session_end_utc, ascend=True)
            else:
                days = params['days'] if not latest_timestamp else (dt.now(timezone.utc) - latest_timestamp).days + 1
                if days <= 0: continue
                dtn_data = hist_conn.request_daily_data(ticker=symbol, num_days=days, ascend=True)

            if dtn_data is not None and len(dtn_data) > 0:
                influx_df = format_data_for_influx(dtn_data, symbol, exchange, tf_name, last_session_end_utc)
                if influx_df is not None and not influx_df.empty:
                    # Use chunked writing with retry logic
                    write_data_in_chunks(influx_df, chunk_size=1000 if tf_name == '1d' else 5000)
                    
        except Exception as e:
            logging.error(f"Error fetching {tf_name} for {symbol}: {e}", exc_info=True)

def daily_update(symbols_to_update: list, exchange: str):
    """
    Performs the daily update, respecting trading hours.
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

    try:
        with iq.ConnConnector([hist_conn]):
            for symbol in symbols_to_update:
                try:
                    logging.info(f"Processing symbol: {symbol}")
                    fetch_and_store_history(symbol, exchange, hist_conn)
                except Exception as e:
                    logging.error(f"Failed to process symbol {symbol}: {e}", exc_info=True)
                    # Continue with next symbol even if one fails
                    continue
    finally:
        # Ensure write_api is flushed before closing
        try:
            write_api.flush()
        except Exception as e:
            logging.error(f"Error flushing write API: {e}")

    logging.info("--- Daily Update Process Finished ---")

def scheduled_daily_update():
    """
    Wrapper function for the scheduler. Defines the symbols and exchange to update.
    """
    logging.info("--- Triggering Scheduled Daily Update ---")
    symbols_to_update = ["AAPL", "AMZN", "TSLA", "@NQ#"]
    exchange = "NASDAQ"
    daily_update(symbols_to_update, exchange)


if __name__ == '__main__':
    logging.info("Updating Data on Script Initial Starting")
    scheduled_daily_update()    

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

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logging.info("Scheduler stopped. Shutting down...")
    finally:
        try:
            write_api.close()
        except Exception as e:
            logging.error(f"Error closing write API: {e}")
        
        if influx_client:
            influx_client.close()
            logging.info("InfluxDB client closed.")