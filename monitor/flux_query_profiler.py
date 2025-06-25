import os
import argparse
import logging
import re
import pandas as pd
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
from influxdb_client import InfluxDBClient
from influxdb_client.client.query_api import QueryOptions

from zoneinfo import ZoneInfo
from pathlib import Path

# --- Configuration ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Load Environment Variables ---
dotenv_path = Path(__file__).parent.parent / ".env"
if os.path.exists(dotenv_path):
    load_dotenv(dotenv_path)
    logging.info("Loaded environment variables from .env file.")
else:
    logging.error(".env file not found. Please ensure it exists in the project root.")
    exit()

# --- InfluxDB Connection ---
try:
    INFLUX_URL = os.getenv("INFLUX_URL")
    INFLUX_TOKEN = os.getenv("INFLUX_TOKEN")
    INFLUX_ORG = os.getenv("INFLUX_ORG")
    INFLUX_BUCKET = os.getenv("INFLUX_BUCKET")

    if not all([INFLUX_URL, INFLUX_TOKEN, INFLUX_ORG, INFLUX_BUCKET]):
        raise ValueError("One or more InfluxDB environment variables are not set.")

    # Note: We enable profilers here using QueryOptions
    query_options = QueryOptions(profilers=["query", "operator"])
    
    influx_client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG, timeout=60_000)
    query_api = influx_client.query_api(query_options=query_options)
    
    logging.info(f"Successfully connected to InfluxDB at {INFLUX_URL}")
    logging.info("Flux 'query' and 'operator' profilers have been enabled for this session.")

except Exception as e:
    logging.error(f"Failed to connect to InfluxDB. Please check your .env settings. Error: {e}")
    exit()


def profile_query(symbol: str, interval: str, days_back: int):
    """
    Builds and executes a profiled Flux query, then prints the results.
    """
    logging.info(f"--- Profiling query for Symbol: {symbol}, Interval: {interval}, Days: {days_back} ---")

    # --- 1. Build the exact same query as the main application ---
    end_utc = datetime.now(timezone.utc)
    start_utc = end_utc - timedelta(days=days_back)
    
    et_zone = ZoneInfo("America/New_York")
    start_et, end_et = start_utc.astimezone(et_zone), end_utc.astimezone(et_zone)
    date_range = pd.date_range(start=start_et.date(), end=end_et.date(), freq='D')
    date_regex_part = "|".join([day.strftime('%Y%m%d') for day in date_range])
    
    if not date_regex_part:
        logging.error("Could not generate a date range for the query.")
        return
    
    sanitized_token = re.escape(symbol)
    measurement_regex = f"^ohlc_{sanitized_token}_({date_regex_part})_{interval}$"

    flux_query = f"""
        from(bucket: "{INFLUX_BUCKET}")
          |> range(start: {start_utc.isoformat()}, stop: {end_utc.isoformat()})
          |> filter(fn: (r) => r._measurement =~ /{measurement_regex}/ and r.symbol == "{symbol}")
          |> drop(columns: ["_measurement", "_start", "_stop"])
          |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
          |> sort(columns: ["_time"], desc: true)
          |> limit(n: 5000)
    """
    
    logging.info(f"Executing profiled query:\n{flux_query}")

    # --- 2. Execute the query and process the results ---
    try:
        tables = query_api.query(query=flux_query)
        
        data_record_count = 0
        
        print("\n" + "="*25 + " PROFILER RESULTS " + "="*25)

        for i, table in enumerate(tables):
            # The first table with records is usually the main data result
            if table.records and data_record_count == 0:
                data_record_count = len(table.records)

            # Check if this is a profiler table and print it
            if table.records and 'TotalDuration' in table.records[0].values:
                print("\n--- Query Profiler Stats ---")
                for r in table.records:
                    for k, v in r.values.items():
                        print(f"  {k}: {v}")

            elif table.records and 'Type' in table.records[0].values and 'DurationSum' in table.records[0].values:
                print("\n--- Operator Profiler Stats ---")
                # Create a DataFrame for better formatting
                df = pd.DataFrame([r.values for r in table.records])
                print(df.to_string())
        
        print("\n" + "="*22 + " END PROFILER RESULTS " + "="*22)
        logging.info(f"\nMain data query returned {data_record_count} candle records.")

    except Exception as e:
        logging.error(f"Query execution failed. Error: {e}", exc_info=True)


def main():
    parser = argparse.ArgumentParser(
        description="InfluxDB Flux Query Profiler.",
        formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument("--symbol", required=True, help="The stock symbol to query (e.g., AAPL).")
    parser.add_argument("--interval", required=True, help="The interval to query (e.g., 1m, 5s, 1000tick).")
    parser.add_argument("--days", type=int, default=30, help="How many days back from now to query (default: 30).")
    
    args = parser.parse_args()
    
    profile_query(args.symbol, args.interval, args.days)

if __name__ == "__main__":
    main()