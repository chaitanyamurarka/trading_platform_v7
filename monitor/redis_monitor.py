import redis
import datetime
import json

def get_human_readable_size(size_in_bytes):
    """Converts bytes to a human-readable format (KB, MB, GB)."""
    if size_in_bytes is None:
        return "N/A"
    power = 2**10
    n = 0
    power_labels = {0: '', 1: 'K', 2: 'M', 3: 'G', 4: 'T'}
    while size_in_bytes >= power and n < len(power_labels):
        size_in_bytes /= power
        n += 1
    return f"{size_in_bytes:.2f} {power_labels[n]}B"

def format_ttl(ttl_in_seconds):
    """Converts TTL in seconds to a human-readable string."""
    if ttl_in_seconds == -1:
        return "No Expiration"
    if ttl_in_seconds < 0:
        return "N/A"  # Should not happen for existing keys

    return str(datetime.timedelta(seconds=ttl_in_seconds))

def check_redis_stats(redis_url):
    """
    Connects to a Redis instance, checks cached keys with their TTL and type-specific
    details (count, start/end dates), and displays memory usage statistics.
    """
    try:
        r = redis.from_url(redis_url, decode_responses=True)
        r.ping()
        print("Successfully connected to Redis! ✅\n")
    except redis.exceptions.ConnectionError as e:
        print(f"Error connecting to Redis: {e} ❌")
        return

    print("--- Cached Keys Analysis ---")
    try:
        all_keys = list(r.scan_iter("*"))
        if not all_keys:
            print("No keys found in the cache.")
        else:
            print(f"Found {len(all_keys)} keys. Analyzing details...")

            table_data = []
            max_key_len = 0

            for key in all_keys:
                if len(key) > max_key_len:
                    max_key_len = len(key)
                
                key_type = r.type(key)
                ttl = format_ttl(r.ttl(key))
                count_size = "N/A"
                start_date = "N/A"
                end_date = "N/A"

                try:
                    if key_type == 'list':
                        list_len = r.llen(key)
                        count_size = f"{list_len} items"
                        if list_len > 0:
                            first_item_json = r.lindex(key, 0)
                            last_item_json = r.lindex(key, -1)
                            if first_item_json:
                                first_item = json.loads(first_item_json)
                                start_date = datetime.datetime.fromtimestamp(first_item['timestamp'], tz=datetime.timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
                            if last_item_json:
                                last_item = json.loads(last_item_json)
                                end_date = datetime.datetime.fromtimestamp(last_item['timestamp'], tz=datetime.timezone.utc).strftime('%Y-%m-%d %H:%M:%S')

                    elif key_type == 'string':
                        value_bytes = r.get(key)
                        count_size = get_human_readable_size(len(value_bytes))
                        
                        if key.startswith('session:'):
                            session_timestamp = float(value_bytes)
                            start_date = datetime.datetime.fromtimestamp(session_timestamp, tz=datetime.timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
                            end_date = "(Last Heartbeat)"
                        else:
                            try:
                                data_list = json.loads(value_bytes)
                                if isinstance(data_list, list) and len(data_list) > 0:
                                    count_size = f"{len(data_list)} items"
                                    # Assuming the list contains candle-like objects with 'unix_timestamp'
                                    if 'unix_timestamp' in data_list[0]:
                                        start_date = datetime.datetime.fromtimestamp(data_list[0]['unix_timestamp'], tz=datetime.timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
                                        end_date = datetime.datetime.fromtimestamp(data_list[-1]['unix_timestamp'], tz=datetime.timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
                            except (json.JSONDecodeError, TypeError, IndexError):
                                # Not a JSON list of candles, size is enough info
                                pass
                
                except Exception as e:
                    # If analysis for a specific key fails, note it but don't crash
                    start_date = f"Error: {e}"

                table_data.append([key, key_type, ttl, count_size, start_date, end_date])

            # Print the formatted table
            if max_key_len < 40: max_key_len = 40
            header = f"{'KEY':<{max_key_len}} | {'TYPE':<7} | {'TTL':<20} | {'COUNT/SIZE':<12} | {'START DATE (UTC)':<25} | {'END DATE (UTC)':<25}"
            print("\n" + header)
            print("-" * (len(header) + 5))
            for row in table_data:
                print(f"{row[0]:<{max_key_len}} | {row[1]:<7} | {row[2]:<20} | {row[3]:<12} | {row[4]:<25} | {row[5]:<25}")

    except redis.exceptions.RedisError as e:
        print(f"Could not retrieve keys: {e}")

    # Memory statistics part remains the same
    print("\n--- Memory Statistics ---")
    try:
        info = r.info('memory')
        used_memory = info.get('used_memory')
        used_memory_human = get_human_readable_size(used_memory)
        print(f"Used Memory: {used_memory_human}")

        max_memory = info.get('maxmemory')
        max_memory_human = get_human_readable_size(max_memory)
        print(f"Max Memory: {max_memory_human}")

        if max_memory and max_memory > 0:
            remaining_memory = max_memory - used_memory
            remaining_memory_human = get_human_readable_size(remaining_memory)
            used_percentage = (used_memory / max_memory) * 100
            print(f"Remaining Memory: {remaining_memory_human}")
            print(f"Memory Usage: {used_percentage:.2f}%")
        else:
            print("Remaining Memory: Not applicable (no max memory limit set)")

    except redis.exceptions.RedisError as e:
        print(f"Could not retrieve memory information: {e}")


def flush_all_dbs(redis_url):
    """
    Connects to Redis and flushes all keys from all databases (FLUSHALL).
    """
    try:
        r = redis.from_url(redis_url, decode_responses=True)
        r.ping()
        print("Successfully connected to Redis. ✅")
    except redis.exceptions.ConnectionError as e:
        print(f"Error connecting to Redis: {e} ❌")
        return
    
    # Safety check
    confirm = input("Are you sure you want to delete ALL keys from ALL databases? (yes/no): ")
    if confirm.lower() == 'yes':
        try:
            r.flushall()
            print(f"\nSuccessfully flushed all databases. ✨")
            print("Cache is now empty.")
        except redis.exceptions.RedisError as e:
            print(f"An error occurred while flushing the database: {e} ❌")
    else:
        print("\nFlush operation cancelled. Your data is safe. 👍")


if __name__ == "__main__":
    # --- IMPORTANT ---
    # Replace this with your actual Redis URL
    # Format: redis://[password@]host:port/db
    REDIS_URL = "redis://localhost:6379/0"

    # --- CHOOSE ACTION ---
    print("What would you like to do?")
    print("1. Check Redis Stats (Detailed)")
    print("2. Flush All Redis Databases (FLUSHALL)")
    
    choice = input("Enter your choice (1 or 2): ")
    
    if choice == '1':
        print("\nRunning Redis detailed stats check...")
        check_redis_stats(REDIS_URL)
    elif choice == '2':
        print("\nInitiating flush operation...")
        flush_all_dbs(REDIS_URL)
    else:
        print("\nInvalid choice. Please run the script again and enter 1 or 2.")