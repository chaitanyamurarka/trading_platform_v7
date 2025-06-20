import os
import logging
import time
from datetime import datetime as dt, timezone, time as dt_time, timedelta
import pytz
# For timezone-aware datetime objects. ZoneInfo is in the standard library for Python 3.9+.
# If using an older version, you might need 'pip install backports.zoneinfo' or use 'pytz'.
try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo

import numpy as np
from dotenv import load_dotenv
from influxdb_client import InfluxDBClient, Point, WriteOptions, WritePrecision
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
write_api = influx_client.write_api(write_options=WriteOptions(batch_size=5000, flush_interval=10_000, jitter_interval=2_000))
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
        
    # if target_date_et.weekday() == 5:
    #     target_date_et -= timedelta(days=1)
    # elif target_date_et.weekday() == 6:
    #     target_date_et -= timedelta(days=2)
        
    session_end_et = dt.combine(target_date_et, dt_time(20, 0), tzinfo=et_zone)
    
    return session_end_et.astimezone(timezone.utc)


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
            
            # =================================================================
            # --- NEW: Using request_bars_in_period for precise intraday data ---
            # =================================================================
            if params['type'] != 'd':  # For all intraday intervals
                
                start_dt_for_request = None
                if latest_timestamp:
                    start_dt_for_request = latest_timestamp
                else:
                    # If no data exists, backfill for the number of days specified in the config
                    start_dt_for_request = last_session_end_utc - timedelta(days=params['days'])
                
                # The end of our request period is the end of the last completed session
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
            else:  # For '1d' daily data, the old method is still best
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
                    logging.warning(f"No new {tf_name} data for {symbol} on or before the last session end time (all fetched data may have been filtered).")
                    continue

                logging.info(f"Writing {len(influx_points)} filtered points to InfluxDB for '{measurement}'.")
                write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=influx_points)
            else:
                logging.warning(f"No new {tf_name} data returned for {symbol}.")

        except iq.NoDataError:
            logging.warning(f"IQFeed reported NoDataError for {symbol} on {tf_name}.")
        except Exception as e:
            logging.error(f"An error occurred while fetching {tf_name} for {symbol}: {e}", exc_info=True)
        time.sleep(1)

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
    symbols_to_update = ["AAPL", "AMZN", "TSLA", "@NQM25"]
    exchange = "NASDAQ"
    daily_update(symbols_to_update, exchange)


if __name__ == '__main__':


    logging.info("Updating Data on Script Initial Starting")
    scheduled_daily_update()    

    # --- NEW: RUN AS A PERSISTENT SCHEDULER SERVICE ---
    logging.info("Initializing historical ingestion scheduler...")
    
    # Using BlockingScheduler because this script's only purpose is to run the scheduler.
    # It will block the process from exiting.
    scheduler = BlockingScheduler(timezone="America/New_York")

    scheduler.add_job(
        scheduled_daily_update,
        trigger=CronTrigger(
            hour=20, 
            minute=1, 
            second=0, 
            # day_of_week='mon-fri', # Only run on weekdays
            timezone=pytz.timezone('America/New_York')
        ),
        name="Daily Historical Market Data Ingestion"
    )
    
    logging.info("Scheduler started. Waiting for jobs to run...")
    logging.info("Press Ctrl+C to exit.")

    try:
        # This will start the scheduler and block until interrupted
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        # Handle graceful shutdown
        logging.info("Scheduler stopped. Shutting down...")
    finally:
        # Ensure the InfluxDB client is closed properly on exit
        if influx_client:
            influx_client.close()
            logging.info("InfluxDB client closed.")