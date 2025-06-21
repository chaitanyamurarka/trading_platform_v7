import argparse
import os
import pandas as pd
from dotenv import load_dotenv
from influxdb_client import InfluxDBClient
from datetime import datetime, timedelta, timezone

# Load environment variables from .env file
load_dotenv()

# --- InfluxDB Configuration ---
INFLUX_URL = os.getenv("INFLUX_URL")
INFLUX_TOKEN = os.getenv("INFLUX_TOKEN")
INFLUX_ORG = os.getenv("INFLUX_ORG")
INFLUX_BUCKET = os.getenv("INFLUX_BUCKET")


def get_influxdb_client():
    """Initializes and returns an InfluxDB client."""
    if not all([INFLUX_URL, INFLUX_TOKEN, INFLUX_ORG]):
        raise ValueError(
            "InfluxDB credentials not found. Please ensure a .env file exists."
        )
    client = InfluxDBClient(
        url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG, timeout=90_000
    )
    return client


def find_first_tick_time_by_price(
    client: InfluxDBClient, symbol: str, price: float, day_start: str, day_end: str
):
    """Finds the timestamp of the first tick matching a specific price on a given day."""
    query_api = client.query_api()
    
    flux_query = f"""
    from(bucket: "{INFLUX_BUCKET}")
    |> range(start: {day_start}, stop: {day_end})
    |> filter(fn: (r) => r["_measurement"] == "raw_ticks" and r["symbol"] == "{symbol}")
    |> filter(fn: (r) => r["_field"] == "last" and r["_value"] == {price})
    |> first()
    |> keep(columns: ["_time"])
    """
    
    print(f"Searching for first tick with price {price}...\n")
    
    try:
        tables = query_api.query(flux_query)
        if not tables or not tables[0].records:
            return None
        return tables[0].records[0].get_time()
    except Exception as e:
        print(f"An error occurred while searching for the price: {e}")
        return None


def fetch_raw_ticks(client: InfluxDBClient, symbol: str, start_time: str, end_time: str):
    """Fetches raw tick data from InfluxDB for a given symbol and time range."""
    query_api = client.query_api()
    flux_query = f"""
    from(bucket: "{INFLUX_BUCKET}")
    |> range(start: {start_time}, stop: {end_time})
    |> filter(fn: (r) => r["_measurement"] == "raw_ticks" and r["symbol"] == "{symbol}")
    |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
    """

    print("Executing Flux query to fetch tick window:\n" + flux_query + "\n")

    try:
        result = query_api.query_data_frame(query=flux_query)
        return result
    except Exception as e:
        print(f"An error occurred while querying InfluxDB: {e}")
        return pd.DataFrame()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Fetch raw tick data surrounding a specific price on a given day."
    )
    parser.add_argument("symbol", type=str, help="The trading symbol to query (e.g., AAPL).")
    parser.add_argument("--price", required=True, type=float, help="The price to search for (e.g., 183.09).")
    parser.add_argument("--date", required=True, type=str, help="The date to search within in YYYY-MM-DD format (New York Time).")
    parser.add_argument("--window", type=int, default=30, help="Number of seconds for the context window around the found tick (default: 30).")

    args = parser.parse_args()

    influx_client = None
    try:
        # Convert the user-provided date into a full day range in UTC
        ny_tz = timezone(timedelta(hours=-4), 'America/New_York') # Eastern Daylight Time (EDT)
        day_start_ny = datetime.strptime(args.date, "%Y-%m-%d").replace(tzinfo=ny_tz)
        day_end_ny = day_start_ny + timedelta(days=1, seconds=-1)
        
        day_start_utc = day_start_ny.astimezone(timezone.utc).isoformat().replace('+00:00', 'Z')
        day_end_utc = day_end_ny.astimezone(timezone.utc).isoformat().replace('+00:00', 'Z')
        
        influx_client = get_influxdb_client()

        if influx_client.ping():
            print("Successfully connected to InfluxDB.")
            
            # 1. Find the time of the target tick
            target_time = find_first_tick_time_by_price(
                influx_client, args.symbol, args.price, day_start_utc, day_end_utc
            )

            if target_time:
                print(f"Found tick with price {args.price} at {target_time.isoformat()}. Fetching surrounding ticks...")
                
                # 2. Define the window around that time
                window_delta = timedelta(seconds=args.window)
                window_start = (target_time - window_delta).isoformat()
                window_end = (target_time + window_delta).isoformat()

                # 3. Fetch all ticks in that window
                raw_ticks_df = fetch_raw_ticks(influx_client, args.symbol, window_start, window_end)
                
                if isinstance(raw_ticks_df, list):
                    all_dfs = [df for df in raw_ticks_df if not df.empty]
                    if not all_dfs:
                         print("No tick data found in the context window.")
                    else:
                        for df in all_dfs:
                            print(df.to_string())
                elif raw_ticks_df.empty:
                    print("No tick data found in the context window.")
                else:
                    print(raw_ticks_df.to_string())
            else:
                print(f"No tick with price {args.price} found for {args.symbol} on {args.date}.")

    except ValueError as e:
        print(e)
    except Exception as e:
        print(f"An unexpected error occurred: {e}", exc_info=True)
    finally:
        if influx_client:
            influx_client.close()