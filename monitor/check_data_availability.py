import os
import re
from datetime import datetime
from dotenv import load_dotenv
from influxdb_client import InfluxDBClient
import pandas as pd

# Load environment variables from .env file
load_dotenv()

# --- InfluxDB Configuration ---
INFLUX_URL = os.getenv("INFLUX_URL")
INFLUX_TOKEN = os.getenv("INFLUX_TOKEN")
INFLUX_ORG = os.getenv("INFLUX_ORG")
INFLUX_BUCKET = os.getenv("INFLUX_BUCKET")

def get_influxdb_client():
    """Initializes and returns an InfluxDB client."""
    if not all([INFLUX_URL, INFLUX_TOKEN, INFLUX_ORG, INFLUX_BUCKET]):
        raise ValueError("InfluxDB credentials not found in .env file.")
    client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
    return client

def get_data_range(client: InfluxDBClient, symbol: str, interval: str):
    """
    Queries InfluxDB for the first and last timestamp for a given symbol and interval.
    """
    query_api = client.query_api()
    
    # Regex to match all measurements for the symbol and interval across all dates
    measurement_regex = f"^ohlc_{re.escape(symbol)}_\\d{{8}}_{interval}$"

    # Flux query to get the first and last timestamps
    flux_query = f'''
    first_time = from(bucket: "{INFLUX_BUCKET}")
      |> range(start: 0)
      |> filter(fn: (r) => r._measurement =~ /{measurement_regex}/ and r.symbol == "{symbol}")
      |> first()
      |> keep(columns: ["_time"])
      |> yield(name: "first")

    last_time = from(bucket: "{INFLUX_BUCKET}")
      |> range(start: 0)
      |> filter(fn: (r) => r._measurement =~ /{measurement_regex}/ and r.symbol == "{symbol}")
      |> last()
      |> keep(columns: ["_time"])
      |> yield(name: "last")
    '''

    try:
        tables = query_api.query(flux_query)
        
        first_timestamp = None
        last_timestamp = None

        for table in tables:
            if not table.records:
                continue
            
            if table.get_yield_name() == "first":
                first_timestamp = table.records[0].get_time()
            elif table.get_yield_name() == "last":
                last_timestamp = table.records[0].get_time()

        return first_timestamp, last_timestamp

    except Exception as e:
        print(f"An error occurred while querying for {symbol} with interval {interval}: {e}")
        return None, None

if __name__ == "__main__":
    symbols_to_check = ["AAPL", "AMZN", "TSLA"]
    intervals_to_check = ["1s", "1m", "1h", "1d"]

    influx_client = get_influxdb_client()

    print("Checking available data ranges in InfluxDB...")
    for symbol in symbols_to_check:
        print(f"\n--- Symbol: {symbol} ---")
        for interval in intervals_to_check:
            start_date, end_date = get_data_range(influx_client, symbol, interval)
            
            if start_date and end_date:
                print(f"  Interval: {interval}")
                print(f"    From: {start_date.strftime('%Y-%m-%d %H:%M:%S')}")
                print(f"    To:   {end_date.strftime('%Y-%m-%d %H:%M:%S')}")
            else:
                print(f"  Interval: {interval} -> No data found")

    influx_client.close()