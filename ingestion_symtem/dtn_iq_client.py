"""
dtn_iq_client.py

This module manages the connection to the DTN IQFeed client (IQConnect.exe).

It provides a centralized and robust mechanism to:
1. Launch the IQConnect.exe process if it's not already running.
2. Continuously check for a stable connection to the client's admin port.
3. Handle connection errors and re-launch attempts gracefully.
4. Provide other parts of the application with valid connection objects:
   - `HistoryConn` for historical data requests.
   - `QuoteConn` for live streaming data.

This abstraction is critical because IQFeed client can be unstable or may need
to be started manually, and this module automates that process.
"""

import logging
import time

import pyiqfeed as iq
from config import settings

# --- Module-level State ---
# These global variables track the status of the IQFeed service connection.
# This prevents redundant launch attempts and provides a quick status check.
is_iqfeed_service_launched = False
iqfeed_launch_error = None


def _check_admin_port_connectivity() -> tuple[bool, str | None]:
    """
    Performs a quick, live check to see if the IQFeed admin port is responsive.

    Returns:
        A tuple containing:
        - bool: True if the connection is successful, False otherwise.
        - str | None: An error message if the connection failed, otherwise None.
    """
    try:
        # Attempt to establish a connection to the admin port (default: 9300).
        conn_check = iq.AdminConn(name="AdminPortHealthCheck")
        conn_check.connect()
        time.sleep(0.2)  # A brief pause to allow the connection to establish.

        # The 'connected' attribute in pyiqfeed confirms connection to DTN servers.
        if conn_check.connected:
            conn_check.disconnect()
            return True, None
        else:
            conn_check.disconnect()
            logging.warning("Admin port check: connect() succeeded but IQFeed is not connected to DTN servers.")
            return False, "IQFeed service is running but not connected to backend servers."
    except ConnectionRefusedError:
        return False, "Connection refused. IQFeed service is likely not running."
    except Exception as e:
        logging.error(f"Admin port check: An unexpected error occurred: {e}", exc_info=True)
        return False, f"An unexpected error occurred during connection check: {e}"


def launch_iqfeed_service_if_needed():
    """
    Ensures the IQFeed client is running and connected.

    This function checks for connectivity and, if needed, attempts to launch
    IQConnect.exe using the credentials from the application settings.
    It updates the global state variables based on the outcome.
    """
    global is_iqfeed_service_launched, iqfeed_launch_error

    # If already marked as launched, do a quick verification.
    if is_iqfeed_service_launched:
        is_connected_now, _ = _check_admin_port_connectivity()
        if is_connected_now:
            return  # Still good, no action needed.
        else:
            logging.warning("Previously launched IQFeed service is no longer responsive. Attempting re-launch.")
            is_iqfeed_service_launched = False # Force a re-launch attempt.

    # Check for required DTN credentials in settings.
    if not all([settings.DTN_PRODUCT_ID, settings.DTN_LOGIN, settings.DTN_PASSWORD]):
        iqfeed_launch_error = "DTN IQFeed credentials are not fully configured in settings."
        logging.error(iqfeed_launch_error)
        is_iqfeed_service_launched = False
        return

    logging.info("Attempting to launch or ensure IQFeed client is running...")
    try:
        # Use the pyiqfeed FeedService to launch the client.
        svc = iq.FeedService(
            product=settings.DTN_PRODUCT_ID,
            version="1.0",
            login=settings.DTN_LOGIN,
            password=settings.DTN_PASSWORD
        )
        # The `launch` command will start IQConnect.exe if it's not running.
        svc.launch(headless=True) # Use headless=True for server environments.
        time.sleep(2) # Allow time for the client to initialize and log in.

        # After attempting launch, verify connectivity again.
        is_connected_after_launch, conn_error_msg = _check_admin_port_connectivity()
        if is_connected_after_launch:
            logging.info("Successfully connected to IQFeed admin port after launch.")
            is_iqfeed_service_launched = True
            iqfeed_launch_error = None
        else:
            iqfeed_launch_error = (f"Failed to connect after launch attempt. Error: {conn_error_msg}. "
                                   "Please ensure IQConnect.exe can start and log in correctly.")
            logging.error(iqfeed_launch_error)
            is_iqfeed_service_launched = False

    except Exception as e:
        iqfeed_launch_error = f"An exception occurred during FeedService.launch(): {e}"
        logging.error(iqfeed_launch_error, exc_info=True)
        is_iqfeed_service_launched = False


def get_iqfeed_history_conn() -> iq.HistoryConn | None:
    """
    Provides a HistoryConn object for fetching historical data.

    This is the primary function that other services should call. It ensures the
    IQFeed service is running before returning a connection object.

    Returns:
        An instance of `iq.HistoryConn` if the service is live, otherwise `None`.
    """
    # Always perform a live check before returning a connection.
    launch_iqfeed_service_if_needed()

    if is_iqfeed_service_launched:
        try:
            hist_conn = iq.HistoryConn(name="TradingAppHistoryConnection")
            logging.debug("IQFeed HistoryConn instance created.")
            return hist_conn
        except Exception as e:
            logging.error(f"Failed to create IQFeed HistoryConn instance: {e}", exc_info=True)
            return None
    else:
        logging.error(f"Cannot create HistoryConn: IQFeed service is not running. Last error: {iqfeed_launch_error}")
        return None

def get_iqfeed_streaming_conn() -> iq.QuoteConn | None:
    """
    Provides a QuoteConn object for fetching streaming live data.

    This function ensures the IQFeed service is running before returning a
    connection object for live tick data.

    Returns:
        An instance of `iq.QuoteConn` if the service is live, otherwise `None`.
    """
    # Always perform a live check before returning a connection.
    launch_iqfeed_service_if_needed()

    if is_iqfeed_service_launched:
        try:
            # CORRECTED: Use QuoteConn for streaming summary data
            stream_conn = iq.QuoteConn(name="TradingAppStreamConnection")
            logging.debug("IQFeed QuoteConn instance created.")
            return stream_conn
        except Exception as e:
            logging.error(f"Failed to create IQFeed QuoteConn instance: {e}", exc_info=True)
            return None
    else:
        logging.error(f"Cannot create QuoteConn: IQFeed service is not running. Last error: {iqfeed_launch_error}")
        return None
    
def get_iqfeed_bar_conn() -> iq.BarConn | None:
    """
    Provides a BarConn object for fetching streaming bar data.

    This function ensures the IQFeed service is running before returning a
    connection object for live, interval-based data.

    Returns:
        An instance of `iq.BarConn` if the service is live, otherwise `None`.
    """
    # Always perform a live check before returning a connection.
    launch_iqfeed_service_if_needed()

    if is_iqfeed_service_launched:
        try:
            bar_conn = iq.BarConn(name="TradingAppBarConnection")
            logging.debug("IQFeed BarConn instance created.")
            return bar_conn
        except Exception as e:
            logging.error(f"Failed to create IQFeed BarConn instance: {e}", exc_info=True)
            return None
    else:
        logging.error(f"Cannot create BarConn: IQFeed service is not running. Last error: {iqfeed_launch_error}")
        return None
    
def get_iqfeed_quote_conn() -> iq.QuoteConn | None:
    """
    Provides a QuoteConn object for fetching streaming bar data.

    This function ensures the IQFeed service is running before returning a
    connection object for live, interval-based data.

    Returns:
        An instance of `iq.QuoteConn` if the service is live, otherwise `None`.
    """
    # Always perform a live check before returning a connection.
    launch_iqfeed_service_if_needed()

    if is_iqfeed_service_launched:
        try:
            quote_conn = iq.QuoteConn(name="TradingAppQuoteConnection")
            logging.debug("IQFeed QuoteConn instance created.")
            return quote_conn
        except Exception as e:
            logging.error(f"Failed to create IQFeed QuoteConn instance: {e}", exc_info=True)
            return None
    else:
        logging.error(f"Cannot create QuoteConn: IQFeed service is not running. Last error: {iqfeed_launch_error}")
        return None
    
def get_iqfeed_look_conn() -> iq.LookupConn | None:
    """
    Provides a LookupConn object for fetching streaming bar data.

    This function ensures the IQFeed service is running before returning a
    connection object for live, interval-based data.

    Returns:
        An instance of `iq.LookupConn` if the service is live, otherwise `None`.
    """
    # Always perform a live check before returning a connection.
    launch_iqfeed_service_if_needed()

    if is_iqfeed_service_launched:
        try:
            Lookup_conn = iq.LookupConn(name="TradingAppLookupConnection")
            logging.debug("IQFeed LookupConn instance created.")
            return Lookup_conn
        except Exception as e:
            logging.error(f"Failed to create IQFeed LookupConn instance: {e}", exc_info=True)
            return None
    else:
        logging.error(f"Cannot create LookupConn: IQFeed service is not running. Last error: {iqfeed_launch_error}")
        return None