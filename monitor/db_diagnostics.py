import os
import argparse
import logging
from datetime import datetime
from dotenv import load_dotenv
from influxdb_client import InfluxDBClient
from pathlib import Path


# --- Configuration ---
# Basic Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Load environment variables from .env file
dotenv_path = Path(__file__).parent.parent / ".env"

if os.path.exists(dotenv_path):
    load_dotenv(dotenv_path)
    logging.info("Loaded environment variables from .env file.")
else:
    logging.error(".env file not found. Please ensure it exists in the same directory as this script.")
    exit()

# --- InfluxDB Connection ---
try:
    INFLUX_URL = os.getenv("INFLUX_URL")
    INFLUX_TOKEN = os.getenv("INFLUX_TOKEN")
    INFLUX_ORG = os.getenv("INFLUX_ORG")
    INFLUX_BUCKET = os.getenv("INFLUX_BUCKET")

    if not all([INFLUX_URL, INFLUX_TOKEN, INFLUX_ORG, INFLUX_BUCKET]):
        raise ValueError("One or more InfluxDB environment variables are not set.")

    influx_client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG, timeout=30_000)
    query_api = influx_client.query_api()
    logging.info(f"Successfully connected to InfluxDB at {INFLUX_URL}")

except Exception as e:
    logging.error(f"Failed to connect to InfluxDB. Please check your .env settings. Error: {e}")
    exit()


def list_all_measurements():
    """Fetches and prints all measurement names in the bucket."""
    logging.info("--- [Check 1] Listing all measurements in bucket '{}' ---".format(INFLUX_BUCKET))
    flux_query = f'''
        import "influxdata/influxdb/schema"
        schema.measurements(bucket: "{INFLUX_BUCKET}")
    '''
    try:
        tables = query_api.query(query=flux_query)
        measurements = sorted([row.get_value() for table in tables for row in table.records])
        
        if not measurements:
            logging.warning("No measurements found in the bucket.")
        else:
            logging.info(f"Found {len(measurements)} measurements:")
            for m in measurements:
                print(f"  - {m}")
    except Exception as e:
        logging.error(f"Failed to list measurements. Error: {e}", exc_info=True)

def check_specific_measurement(symbol: str, date_str: str, interval: str):
    """Fetches a few raw records from a specific, constructed measurement name."""
    measurement_name = f"ohlc_{symbol}_{date_str}_{interval}"
    logging.info(f"--- [Check 2] Checking specific measurement: '{measurement_name}' ---")
    
    flux_query = f'''
        from(bucket: "{INFLUX_BUCKET}")
          |> range(start: 0)
          |> filter(fn: (r) => r._measurement == "{measurement_name}")
          |> limit(n: 10)
    '''
    try:
        tables = query_api.query(query=flux_query)
        if not tables:
            logging.warning("Query returned no tables. The measurement may be empty or does not exist.")
            return

        records_found = 0
        for table in tables:
            for record in table.records:
                records_found += 1
                logging.info(f"  > Found record: {record.values}")
        
        if records_found == 0:
            logging.warning("No records found in this measurement.")
        else:
            logging.info(f"Successfully found {records_found} records.")

    except Exception as e:
        logging.error(f"Failed to query specific measurement. Error: {e}", exc_info=True)

def run_app_query(symbol: str, interval: str):
    """Runs a query similar to the one used in the application to count matching records."""
    logging.info(f"--- [Check 3] Running app-style regex query for {symbol} ({interval}) ---")
    start_utc_iso = "2024-01-01T00:00:00Z"
    end_utc_iso = "2026-01-01T00:00:00Z"
    
    measurement_regex = f"^ohlc_{symbol}_\\d{{8}}_{interval}$"

    flux_query = f'''
        from(bucket: "{INFLUX_BUCKET}")
          |> range(start: {start_utc_iso}, stop: {end_utc_iso})
          |> filter(fn: (r) => r._measurement =~ /{measurement_regex}/ and r.symbol == "{symbol}")
          |> group() 
          |> count()
    '''
    try:
        tables = query_api.query(query=flux_query)
        if not tables or not tables[0].records:
            logging.warning("App-style query returned 0 records.")
        else:
            count = tables[0].records[0].get_value()
            logging.info(f"App-style query found a total of {count} matching raw records (unpivoted).")

    except Exception as e:
        logging.error(f"Failed to run app-style query. Error: {e}", exc_info=True)


def main():
    parser = argparse.ArgumentParser(
        description="InfluxDB Diagnostics Script for Trading Platform.",
        formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument(
        "--list-measurements", 
        action="store_true", 
        help="[Check 1] List all measurements in the bucket."
    )
    parser.add_argument(
        "--check-measurement", 
        nargs=3, 
        metavar=("SYMBOL", "YYYYMMDD", "INTERVAL"), 
        help="[Check 2] Check a specific measurement for records.\nExample: --check-measurement AAPL 20250624 1m"
    )
    parser.add_argument(
        "--app-query", 
        nargs=2, 
        metavar=("SYMBOL", "INTERVAL"), 
        help="[Check 3] Run a regex query similar to the app.\nExample: --app-query AAPL 1m"
    )
    
    args = parser.parse_args()

    if not any(vars(args).values()):
        parser.print_help()
        return

    if args.list_measurements:
        list_all_measurements()
        print("-" * 50)
    
    if args.check_measurement:
        symbol, date_str, interval = args.check_measurement
        check_specific_measurement(symbol, date_str, interval)
        print("-" * 50)

    if args.app_query:
        symbol, interval = args.app_query
        run_app_query(symbol, interval)
        print("-" * 50)

if __name__ == "__main__":
    main()