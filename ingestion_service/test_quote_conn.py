# In ingestion_service/test_quote_conn.py

import logging
import time
import pyiqfeed as iq
from dtn_iq_client import launch_iqfeed_service_if_needed

# --- Configuration ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(funcName)s - %(message)s')

def test_level1_quotes(symbols_to_watch: list[str], duration_seconds: int = 60):
    """
    Establishes a QuoteConn to listen for Level 1 data (trades and quotes).
    """
    try:
        # Make sure the IQFeed client is running
        launch_iqfeed_service_if_needed()

        # Set up a connection to the Quote port
        quote_conn = iq.QuoteConn(name="QuoteConnectionTester")
        
        # Using VerboseQuoteListener will automatically print all messages received
        # which is perfect for diagnostics.
        listener = iq.VerboseQuoteListener("VerboseQuoteTestListener")
        quote_conn.add_listener(listener)
        
        # The ConnConnector manages the connection lifecycle
        with iq.ConnConnector([quote_conn]):
            logging.info(f"Connection successful. Listening for Level 1 data for {duration_seconds} seconds...")
            
            # Watch the symbols for Level 1 updates
            for symbol in symbols_to_watch:
                quote_conn.watch(symbol)
                logging.info(f"Sent Level 1 watch request for: {symbol}")
            
            # Wait for data to come in
            time.sleep(duration_seconds)

            # Unwatch the symbols before closing
            for symbol in symbols_to_watch:
                quote_conn.unwatch(symbol)
            logging.info("Stopped watching symbols.")

    except Exception as e:
        logging.error(f"An error occurred: {e}", exc_info=True)

if __name__ == "__main__":
    # Using the same symbols for a consistent test
    SYMBOLS = ["AAPL", "AMZN", "TSLA", "@ES#"]
    logging.info("--- Starting Level 1 Quote Connection Test ---")
    test_level1_quotes(SYMBOLS)
    logging.info("--- Level 1 Quote Connection Test Finished ---")