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
# MODIFICATION: Change the write_api to be SYNCHRONOUS for debugging.
# This will make writes slower but will raise an error immediately if they fail.
write_api = influx_client.write_api(write_options=SYNCHRONOUS)
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
    if len(dtn_data) == 0: return None
    df = pd.DataFrame(dtn_data)
    if 'time' in dtn_data.dtype.names:
        df['timestamp'] = pd.to_datetime(df['date'].values.astype('datetime64[D]') + df['time'].values.astype('timedelta64[us]')).tz_localize('America/New_York')
    else:
        df['timestamp'] = pd.to_datetime(df['date']).dt.tz_localize('America/New_York')
    if end_time_utc_cutoff: df = df[df['timestamp'].dt.tz_convert('UTC') <= end_time_utc_cutoff]
    if df.empty: return None
    df['_measurement'] = df['timestamp'].dt.strftime(f'ohlc_{symbol}_%Y%m%d_{tf_name}')
    df.rename(columns={'open_p': 'open', 'high_p': 'high', 'low_p': 'low', 'close_p': 'close', 'prd_vlm': 'volume', 'tot_vlm': 'total_volume'}, inplace=True)
    if 'volume' not in df.columns and 'total_volume' in df.columns: df['volume'] = df['total_volume']
    if 'volume' not in df.columns: df['volume'] = 0
    df['volume'] = df['volume'].astype('int64'); df['symbol'] = symbol; df['exchange'] = exchange
    df.set_index('timestamp', inplace=True)
    final_cols = ['open', 'high', 'low', 'close', 'volume', 'symbol', 'exchange', '_measurement']
    return df[[col for col in final_cols if col in df.columns]]

def fetch_and_store_history(symbol: str, exchange: str, hist_conn: iq.HistoryConn):
    timeframes = {"1s":{"days":7},"5s":{"days":7},"10s":{"days":7},"15s":{"days":7},"30s":{"days":7},"45s":{"days":7},"1m":{"days":180},"5m":{"days":180},"10m":{"days":180},"15m":{"days":180},"30m":{"days":180},"45m":{"days":180},"1h":{"days":180},"1d":{"days":10000}}
    last_session_end_utc = get_last_completed_session_end_time_utc()

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
                    # FIX: Group by the '_measurement' column and write each group separately
                    grouped_by_measurement = influx_df.groupby('_measurement')
                    logging.info(f"Writing {len(influx_df)} points to {len(grouped_by_measurement)} measurements for timeframe '{tf_name}'...")
                    for name, group_df in grouped_by_measurement:
                        write_api.write(
                            bucket=INFLUX_BUCKET,
                            record=group_df,
                            data_frame_measurement_name=name, # Use the actual measurement name for each group
                            data_frame_tag_columns=['symbol', 'exchange']
                        )
                    logging.info("Write complete.")
        except Exception as e:
            logging.error(f"Error fetching {tf_name} for {symbol}: {e}", exc_info=True)


def format_data_for_influx(
    dtn_data: np.ndarray,
    symbol: str,
    exchange: str,
    tf_name: str,
    end_time_utc_cutoff: dt | None = None
) -> pd.DataFrame | None:
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

    with iq.ConnConnector([hist_conn]):
        for symbol in symbols_to_update:
            fetch_and_store_history(symbol, exchange, hist_conn)

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
        if influx_client:
            influx_client.close()
            logging.info("InfluxDB client closed.")