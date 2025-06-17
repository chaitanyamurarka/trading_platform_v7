# In ingestion_service/diagnose_symbol.py

import logging
import time
import pyiqfeed as iq
import numpy as np

# --- Local Imports ---
from dtn_iq_client import launch_iqfeed_service_if_needed, get_iqfeed_history_conn, get_iqfeed_quote_conn, get_iqfeed_look_conn
from config import settings

# --- Configuration ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(funcName)s - %(message)s')
SYMBOL_TO_TEST = "@NQM25"
SEARCH_TERM = "E-mini NASDAQ 100"

class VerboseListener(iq.SilentQuoteListener):
    """
    A listener that prints every piece of data it receives and, crucially,
    logs the column names of the data arrays.
    """
    def __init__(self, name="VerboseListener"):
        super().__init__(name)
        self.data_received = False
        self.logged_update_fields = False
        self.logged_summary_fields = False

    def process_update(self, update_data: np.ndarray) -> None:
        """Processes a trade update message."""
        self.data_received = True
        # --- MODIFICATION: Log the column names (fields) of the data, but only once ---
        if not self.logged_update_fields:
            logging.info(f"****************************************************************")
            logging.info(f"TRADE UPDATE Column Names: {update_data.dtype.names}")
            logging.info(f"****************************************************************")
            self.logged_update_fields = True
        logging.info(f"Received TRADE UPDATE data:\n{update_data}")

    def process_summary(self, summary_data: np.ndarray) -> None:
        """Processes a summary message."""
        self.data_received = True
        # --- MODIFICATION: Log the column names (fields) of the data, but only once ---
        if not self.logged_summary_fields:
            logging.info(f"****************************************************************")
            logging.info(f"SUMMARY Column Names: {summary_data.dtype.names}")
            logging.info(f"****************************************************************")
            self.logged_summary_fields = True
        logging.info(f"Received SUMMARY data:\n{summary_data}")

    def process_system_message(self, message: bytes) -> None:
        self.data_received = True
        logging.warning(f"Received SYSTEM message: {message.decode(errors='ignore').strip()}")

    def process_error_message(self, message: bytes) -> None:
        self.data_received = True
        logging.error(f"Received ERROR message: {message.decode(errors='ignore').strip()}")

def main_diagnostic():
    logging.info("--- Starting Symbol Diagnostic Script (v2) ---")
    launch_iqfeed_service_if_needed()

    hist_conn = get_iqfeed_history_conn()
    quote_conn = get_iqfeed_quote_conn()
    look_conn = get_iqfeed_look_conn()

    if not hist_conn or not quote_conn:
        logging.error("Could not get IQFeed connections. Exiting.")
        return

    listener = VerboseListener()
    quote_conn.add_listener(listener)

    with iq.ConnConnector([hist_conn, quote_conn, look_conn]):
        # --- Test 1: Symbol Lookup (Corrected) ---
        logging.info(f"\n--- Running Test 1: Looking up symbols for '{SEARCH_TERM}' ---")
        try:
            # Using a more standard lookup method
            lookup_data = look_conn.request_symbols_by_filter(search_term=SEARCH_TERM)
           # --- FIX: Change the condition to check the length of the array ---
            if lookup_data is not None and len(lookup_data) > 0:
                logging.info(f"Found symbols for '{SEARCH_TERM}':")
                # Limiting output to first 10 results for brevity

                try:
                    logging.info("Trying to print column names")
                    logging.info(lookup_data[0].dtype.names)
                except Exception as e:
                    logging(f"Error while checking columnn names {e}")

                for symbol_info in lookup_data[:50]:
                    # --- FIX: Print the object directly instead of decoding it ---
                    logging.info(f"  - {symbol_info}")
            else:
                logging.warning(f"No symbols found for description '{SEARCH_TERM}'.")
        except Exception as e:
            logging.error(f"Error during symbol lookup: {e}", exc_info=True)
        
        time.sleep(2)

        # # --- Test 2: Historical Data Download ---
        # logging.info(f"\n--- Running Test 2: Requesting recent historical bar for '{SYMBOL_TO_TEST}' ---")
        # try:
        #     bar_data = hist_conn.request_bars(ticker=SYMBOL_TO_TEST, interval_len=60, interval_type='s', max_bars=1)
        #     if bar_data is not None and len(bar_data) > 0:
        #         logging.info(f"Successfully downloaded historical bar for {SYMBOL_TO_TEST}:\n{bar_data[0]}")
        #     else:
        #         logging.warning(f"No historical bar data returned for {SYMBOL_TO_TEST}.")
        # except iq.NoDataError:
        #     logging.error(f"NoDataError for {SYMBOL_TO_TEST}. Check symbol and subscription.")
        # except Exception as e:
        #     logging.error(f"Error requesting historical data for {SYMBOL_TO_TEST}: {e}", exc_info=True)
        
        # time.sleep(2)

        # --- Test 3: Live Data Watch ---
        logging.info(f"\n--- Running Test 3: Watching '{SYMBOL_TO_TEST}' for 30 seconds ---")
        logging.info("Column names and data will be printed below.")
        quote_conn.trades_watch(SYMBOL_TO_TEST)
        
        try:
            time.sleep(30)
        except KeyboardInterrupt:
            logging.info("Stopping diagnostic test.")

        quote_conn.unwatch(SYMBOL_TO_TEST)

        if not listener.data_received:
            logging.warning(f"--- Test 3 Result: NO live data was received for {SYMBOL_TO_TEST}. ---")
        else:
            logging.info(f"--- Test 3 Result: Successfully received live data for {SYMBOL_TO_TEST}. ---")

    logging.info("\n--- Diagnostic Script Finished ---")


if __name__ == "__main__":
    main_diagnostic()