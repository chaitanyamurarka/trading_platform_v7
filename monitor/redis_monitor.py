import redis
import datetime

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
        return "N/A" # Should not happen for existing keys
    
    return str(datetime.timedelta(seconds=ttl_in_seconds))

def check_redis_stats(redis_url):
    """
    Connects to a Redis instance via a URL, checks cached keys with their TTL,
    and displays memory usage statistics.
    """
    try:
        r = redis.from_url(redis_url, decode_responses=True)
        r.ping()
        print("Successfully connected to Redis! ✅\n")
    except redis.exceptions.ConnectionError as e:
        print(f"Error connecting to Redis: {e} ❌")
        return

    print("--- Cached Keys (with Time To Live) ---")
    keys_with_ttl = []
    max_key_len = 0 # For formatting the output nicely
    try:
        for key in r.scan_iter("*"):
            ttl = r.ttl(key)
            keys_with_ttl.append((key, format_ttl(ttl)))
            if len(key) > max_key_len:
                max_key_len = len(key)
        
        if keys_with_ttl:
            print(f"{'KEY':<{max_key_len}}  |  {'TIME TO LIVE (TTL)':<25}")
            print(f"{'-' * max_key_len}  |  {'-' * 25}")
            
            for key, ttl_str in keys_with_ttl:
                print(f"{key:<{max_key_len}}  |  {ttl_str}")
        else:
            print("No keys found in the cache.")
        
        print(f"\nTotal number of keys: {len(keys_with_ttl)}")

    except redis.exceptions.RedisError as e:
        print(f"Could not retrieve keys: {e}")
    
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

# --- NEW FUNCTION TO FLUSH ALL DATABASES ---
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
            key_count = r.dbsize() # Get number of keys before flushing (for current DB)
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
    print("1. Check Redis Stats")
    print("2. Flush All Redis Databases (FLUSHALL)")
    
    choice = input("Enter your choice (1 or 2): ")
    
    if choice == '1':
        print("\nRunning Redis stats check...")
        check_redis_stats(REDIS_URL)
    elif choice == '2':
        print("\nInitiating flush operation...")
        flush_all_dbs(REDIS_URL)
    else:
        print("\nInvalid choice. Please run the script again and enter 1 or 2.")