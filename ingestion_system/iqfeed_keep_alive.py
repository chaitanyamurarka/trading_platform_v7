#! /usr/bin/env python3
# coding=utf-8

"""
iqfeed_keep_alive.py

This script acts as a persistent watchdog for the DTN IQFeed client (IQConnect.exe).

Purpose:
The IQFeed client application can sometimes disconnect or shut down. This script's
sole responsibility is to run continuously in the background, ensure that the
IQFeed client is always running and connected, and re-launch it if necessary.
This provides a stable foundation upon which the main trading application can rely
for its data feed.

How it works:
- It uses the `pyiqfeed` library to launch and check the status of IQConnect.exe.
- It runs in an infinite loop, periodically checking the connection status.
- It implements robust error handling to catch various connection issues (e.g.,
  ConnectionRefused, ConnectionReset, timeouts) and attempts to recover.
- It can be shut down gracefully by creating a specific "control file" (e.g.,
  /tmp/stop_iqfeed.ctrl), which the script checks for in each loop.
"""

import os
import time
import argparse
import socket
import logging
from typing import Optional
from config import settings

# It's assumed that the 'app' package is in the Python path.
# If running this script standalone, you might need to adjust the path.
# Example: export PYTHONPATH=/path/to/your/project/trading_backend
try:
    import pyiqfeed as iq
except ImportError:
    print("Error: Could not import the 'app.pyiqfeed' module.")
    print("Please ensure the project's root directory is in your PYTHONPATH.")
    exit(1)


# --- Basic Logging Configuration ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Load Credentials from Environment Variables ---
# This script requires the same DTN credentials as the main application.
dtn_product_id = settings.DTN_PRODUCT_ID
dtn_login = settings.DTN_LOGIN
dtn_password = settings.DTN_PASSWORD

def main():
    """Main function to run the keep-alive logic."""
    parser = argparse.ArgumentParser(description="Launch and maintain the IQFeed connection.")
    parser.add_argument('--nohup', action='store_true', default=False, help="Don't kill IQConnect.exe when this script exits.")
    parser.add_argument('--headless', action="store_true", default=False, help="Launch IQFeed in a headless XServer (for Linux).")
    parser.add_argument('--control-file', default="/tmp/stop_iqfeed.ctrl", help='A file path that, if it exists, will cause the script to stop.')
    args = parser.parse_args()

    if not all([dtn_product_id, dtn_login, dtn_password]):
        logging.error("DTN credentials (DTN_PRODUCT_ID, DTN_LOGIN, DTN_PASSWORD) must be set as environment variables.")
        exit(1)

    # Initialize the FeedService object, which is used to launch IQConnect.exe
    try:
        logging.info("Initializing IQFeed Service object...")
        feed_service = iq.FeedService(
            product=dtn_product_id,
            version="IQFEED_KEEPALIVE_V1",
            login=dtn_login,
            password=dtn_password
        )
    except Exception as e:
        logging.error(f"Failed to initialize IQFeed Service object: {e}", exc_info=True)
        exit(1)

    # --- Main Loop ---
    # This loop continuously tries to keep the IQFeed connection alive.
    # It will exit only when the control file is detected.
    while not os.path.isfile(args.control_file):
        try:
            # Step 1: Ensure IQConnect.exe is running before attempting to connect.
            # The `launch` method is idempotent; it won't start a new process if one is running.
            logging.info(f"Ensuring IQFeed is running (headless={args.headless}, nohup={args.nohup})...")
            feed_service.launch(
                timeout=60,  # Generous timeout for initial launch
                check_conn=True,
                headless=args.headless,
                nohup=args.nohup
            )
            logging.info("IQFeed launch/check command issued. IQConnect should be running.")

            # Step 2: Establish an Admin connection to monitor the feed's health.
            logging.info("Attempting to establish Admin connection to IQFeed...")
            admin_conn = iq.AdminConn(name="KeepAliveAdmin")
            # The ConnConnector context manager ensures connect() and disconnect() are called properly.
            with iq.ConnConnector([admin_conn]):
                logging.info("Admin connection established. Monitoring for control file and connection health.")
                # Turn on client stats to get periodic health updates.
                admin_conn.client_stats_on()

                # Inner loop: keep checking for the control file while connected.
                while not os.path.isfile(args.control_file):
                    if not admin_conn.reader_running():
                        logging.warning("AdminConn reader thread is not running. Connection might be lost.")
                        raise ConnectionResetError("AdminConn reader thread terminated unexpectedly.")
                    time.sleep(10)  # Check for the control file every 10 seconds.
            
            # If the control file was found, break the outer loop to exit.
            if os.path.isfile(args.control_file):
                logging.info(f"Control file '{args.control_file}' found. Initiating shutdown.")
                break

        # --- Exception Handling and Retry Logic ---
        except (ConnectionRefusedError, ConnectionResetError, socket.timeout, socket.error) as e:
            logging.warning(f"A connection error occurred: {e}. IQFeed may have disconnected. Retrying in 15s.")
        except RuntimeError as e:
            logging.error(f"A runtime error occurred, possibly from pyiqfeed: {e}. Retrying in 15s.")
        except Exception as e:
            logging.error(f"An unexpected error occurred in the main loop: {e}", exc_info=True)
            logging.info("Attempting to recover. Retrying in 15s.")
        
        # If the control file is found at any point, break out of the main loop.
        if os.path.isfile(args.control_file):
            break

        # Wait for 15 seconds before the next connection attempt, but check for the
        # control file every second during the wait to allow for a fast exit.
        logging.info("Waiting 15 seconds before next connection attempt...")
        for _ in range(15):
            if os.path.isfile(args.control_file):
                logging.info(f"Control file detected during wait. Exiting retry loop.")
                break
            time.sleep(1)
        if os.path.isfile(args.control_file):
            break

    # --- Cleanup ---
    # Remove the control file on exit so the script can be restarted easily.
    if os.path.exists(args.control_file):
        try:
            logging.info(f"Removing control file: {args.control_file}")
            os.remove(args.control_file)
        except OSError as e:
            logging.error(f"Error removing control file '{args.control_file}': {e}")
    
    logging.info("IQFeed Keep Alive script finished.")


if __name__ == "__main__":
    main()