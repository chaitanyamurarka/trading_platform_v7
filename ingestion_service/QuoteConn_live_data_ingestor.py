# ingestion_service/QuoteConn_live_data_ingestor.py

import logging
import json
import time
import threading
from datetime import datetime, timezone
from collections import defaultdict, deque
from typing import Dict, Optional, List
import numpy as np
import pyiqfeed as iq
import redis
from zoneinfo import ZoneInfo
from influxdb_client import InfluxDBClient
from dataclasses import dataclass, field

# Local imports
from dtn_iq_client import get_iqfeed_quote_conn, launch_iqfeed_service_if_needed
from config import settings
from pyiqfeed.field_readers import date_us_to_datetime

# --- Configuration ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(funcName)s - %(message)s')
REDIS_URL = settings.REDIS_URL
redis_client = redis.Redis.from_url(REDIS_URL)

# InfluxDB Configuration
INFLUX_URL = settings.INFLUX_URL
INFLUX_TOKEN = settings.INFLUX_TOKEN
INFLUX_ORG = settings.INFLUX_ORG
INFLUX_BUCKET = settings.INFLUX_BUCKET

# InfluxDB Client
influx_client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG, timeout=20_000)
query_api = influx_client.query_api()

@dataclass
class TradeData:
    """Represents a single trade/quote update"""
    symbol: str
    timestamp: datetime
    last_price: float
    last_size: int
    volume: int
    bid: float
    ask: float
    high: float = 0.0
    low: float = 0.0
    open: float = 0.0
    is_trade: bool = True

@dataclass
class CandleAccumulator:
    """Accumulates trades into 1-second candles"""
    symbol: str
    start_time: datetime
    open: float = 0.0
    high: float = 0.0
    low: float = float('inf')
    close: float = 0.0
    volume: int = 0
    trade_count: int = 0
    last_trade_time: Optional[datetime] = None
    
    def add_trade(self, trade: TradeData):
        """Add a trade to this candle"""
        if self.open == 0.0:
            self.open = trade.last_price
            
        self.high = max(self.high, trade.last_price)
        self.low = min(self.low, trade.last_price)
        self.close = trade.last_price
        self.volume += trade.last_size
        self.trade_count += 1
        self.last_trade_time = trade.timestamp
    
    def to_dict(self) -> dict:
        """Convert to dictionary for Redis/JSON serialization"""
        return {
            "timestamp": int(self.start_time.timestamp()),
            "open": float(self.open) if self.open else 0.0,
            "high": float(self.high) if self.high else 0.0,
            "low": float(self.low) if self.low != float('inf') else 0.0,
            "close": float(self.close) if self.close else 0.0,
            "volume": int(self.volume),
            "trade_count": int(self.trade_count)
        }

class DataQualityMonitor:
    """Monitors data quality and detects anomalies"""
    
    def __init__(self, symbol: str):
        self.symbol = symbol
        self.recent_prices = deque(maxlen=100)
        self.recent_volumes = deque(maxlen=100)
        self.last_price = None
        self.total_trades = 0
        self.bad_data_count = 0
        
    def check_trade_quality(self, trade: TradeData) -> tuple[bool, List[str]]:
        """
        Check if trade data is valid
        Returns: (is_valid, list_of_issues)
        """
        issues = []
        
        # Basic validation
        if trade.last_price <= 0:
            issues.append(f"Invalid price: {trade.last_price}")
            
        if trade.last_size <= 0:
            issues.append(f"Invalid size: {trade.last_size}")
            
        if trade.volume < 0:
            issues.append(f"Invalid volume: {trade.volume}")
        
        # Price spike detection
        if self.last_price and abs(trade.last_price - self.last_price) / self.last_price > 0.1:  # 10% price change
            issues.append(f"Large price movement: {self.last_price} -> {trade.last_price}")
        
        # Volume spike detection
        if len(self.recent_volumes) > 10:
            avg_volume = sum(self.recent_volumes) / len(self.recent_volumes)
            if trade.last_size > avg_volume * 10:  # 10x average volume
                issues.append(f"Large volume spike: {trade.last_size} vs avg {avg_volume:.0f}")
        
        # Bid-Ask validation
        if trade.bid > 0 and trade.ask > 0 and trade.bid >= trade.ask:
            issues.append(f"Invalid bid-ask spread: bid={trade.bid}, ask={trade.ask}")
        
        # Update tracking data
        if not issues:  # Only update with good data
            self.recent_prices.append(trade.last_price)
            self.recent_volumes.append(trade.last_size)
            self.last_price = trade.last_price
        else:
            self.bad_data_count += 1
            
        self.total_trades += 1
        
        return len(issues) == 0, issues
    
    def get_quality_stats(self) -> dict:
        """Get data quality statistics"""
        return {
            "symbol": self.symbol,
            "total_trades": self.total_trades,
            "bad_data_count": self.bad_data_count,
            "quality_ratio": 1 - (self.bad_data_count / max(1, self.total_trades)),
            "recent_price_range": {
                "min": min(self.recent_prices) if self.recent_prices else 0,
                "max": max(self.recent_prices) if self.recent_prices else 0,
                "avg": sum(self.recent_prices) / len(self.recent_prices) if self.recent_prices else 0
            }
        }

class LiveQuoteListener(iq.SilentQuoteListener):
    """
    Enhanced listener that processes real-time quotes and trades from QuoteConn,
    aggregates them into 1-second candles, and publishes to Redis.
    """

    def __init__(self, name="LiveQuoteListener"):
        super().__init__(name)
        self.redis_client = redis.Redis.from_url(REDIS_URL)
        self.source_timezone = ZoneInfo("America/New_York")
        
        # Data management
        self.candle_accumulators: Dict[str, Dict[int, CandleAccumulator]] = defaultdict(dict)
        self.quality_monitors: Dict[str, DataQualityMonitor] = {}
        self.symbol_states: Dict[str, dict] = defaultdict(dict)
        
        # Logging
        self.trade_logger = self._setup_trade_logger()
        self.quality_logger = self._setup_quality_logger()
        
        # Timing
        self.last_candle_publish = {}
        self.candle_publish_lock = threading.Lock()
        
        # Start background tasks
        self._start_background_tasks()
        
    def _setup_trade_logger(self) -> logging.Logger:
        """Setup logger for raw trade data"""
        logger = logging.getLogger('trade_data')
        logger.setLevel(logging.INFO)
        
        # File handler for trade data
        trade_handler = logging.FileHandler('logs/trade_data.log')
        trade_handler.setFormatter(logging.Formatter(
            '%(asctime)s - %(message)s'
        ))
        logger.addHandler(trade_handler)
        
        return logger
    
    def _setup_quality_logger(self) -> logging.Logger:
        """Setup logger for data quality issues"""
        logger = logging.getLogger('data_quality')
        logger.setLevel(logging.WARNING)
        
        # File handler for quality issues
        quality_handler = logging.FileHandler('logs/data_quality.log')
        quality_handler.setFormatter(logging.Formatter(
            '%(asctime)s - %(levelname)s - %(message)s'
        ))
        logger.addHandler(quality_handler)
        
        return logger
    
    def _start_background_tasks(self):
        """Start background tasks for candle publishing and quality monitoring"""
        
        def candle_publisher():
            """Background task to publish completed candles every second"""
            while True:
                try:
                    current_second = int(time.time())
                    self._publish_completed_candles(current_second - 1)  # Publish previous second
                    time.sleep(0.1)  # Check every 100ms
                except Exception as e:
                    logging.error(f"Error in candle publisher: {e}", exc_info=True)
        
        def quality_reporter():
            """Background task to report data quality statistics"""
            while True:
                try:
                    time.sleep(60)  # Report every minute
                    self._report_quality_stats()
                except Exception as e:
                    logging.error(f"Error in quality reporter: {e}", exc_info=True)
        
        # Start threads
        threading.Thread(target=candle_publisher, daemon=True).start()
        threading.Thread(target=quality_reporter, daemon=True).start()
        
    def _get_candle_second(self, timestamp: datetime) -> int:
        """Get the second timestamp for candle grouping"""
        return int(timestamp.timestamp())
    
    def _parse_trade_from_update(self, update_data: np.ndarray) -> Optional[TradeData]:
        """Parse trade data from quote update"""
        try:
            symbol = update_data['Symbol'][0].decode('utf-8') if isinstance(update_data['Symbol'][0], bytes) else str(update_data['Symbol'][0])
            
            # Get price data
            last_price = float(update_data['Most Recent Trade'][0]) if 'Most Recent Trade' in update_data.dtype.names else 0.0
            last_size = int(update_data['Most Recent Trade Size'][0]) if 'Most Recent Trade Size' in update_data.dtype.names else 0
            total_volume = int(update_data['Total Volume'][0]) if 'Total Volume' in update_data.dtype.names else 0
            
            # Get bid/ask
            bid = float(update_data['Bid'][0]) if 'Bid' in update_data.dtype.names else 0.0
            ask = float(update_data['Ask'][0]) if 'Ask' in update_data.dtype.names else 0.0
            
            # Get OHLC if available
            high = float(update_data['High'][0]) if 'High' in update_data.dtype.names else last_price
            low = float(update_data['Low'][0]) if 'Low' in update_data.dtype.names else last_price
            open_price = float(update_data['Open'][0]) if 'Open' in update_data.dtype.names else last_price
            
            # Create timestamp (assuming data is in ET timezone)
            now = datetime.now(self.source_timezone)
            
            return TradeData(
                symbol=symbol,
                timestamp=now,
                last_price=last_price,
                last_size=last_size,
                volume=total_volume,
                bid=bid,
                ask=ask,
                high=high,
                low=low,
                open=open_price,
                is_trade=last_price > 0 and last_size > 0
            )
            
        except Exception as e:
            logging.error(f"Error parsing trade data: {e}, data: {update_data}")
            return None
    
    def _process_trade(self, trade: TradeData):
        """Process a single trade/quote update"""
        
        # Initialize quality monitor if needed
        if trade.symbol not in self.quality_monitors:
            self.quality_monitors[trade.symbol] = DataQualityMonitor(trade.symbol)
        
        # Check data quality
        is_valid, issues = self.quality_monitors[trade.symbol].check_trade_quality(trade)
        
        if not is_valid:
            self.quality_logger.warning(f"Data quality issues for {trade.symbol}: {', '.join(issues)}")
            # Decide whether to skip bad data or process anyway
            # For now, we'll process it but log the issues
        
        # Log raw trade data
        self.trade_logger.info(json.dumps({
            "symbol": trade.symbol,
            "timestamp": trade.timestamp.isoformat(),
            "price": trade.last_price,
            "size": trade.last_size,
            "volume": trade.volume,
            "bid": trade.bid,
            "ask": trade.ask,
            "is_valid": is_valid,
            "issues": issues if not is_valid else []
        }))
        
        # Get candle second for grouping
        candle_second = self._get_candle_second(trade.timestamp)
        candle_start_time = datetime.fromtimestamp(candle_second, tz=timezone.utc)
        
        # Get or create candle accumulator
        if candle_second not in self.candle_accumulators[trade.symbol]:
            self.candle_accumulators[trade.symbol][candle_second] = CandleAccumulator(
                symbol=trade.symbol,
                start_time=candle_start_time
            )
        
        # Add trade to candle
        self.candle_accumulators[trade.symbol][candle_second].add_trade(trade)
        
    def _publish_completed_candles(self, target_second: int):
        """Publish completed candles for the given second"""
        with self.candle_publish_lock:
            for symbol in list(self.candle_accumulators.keys()):
                if target_second in self.candle_accumulators[symbol]:
                    candle = self.candle_accumulators[symbol][target_second]
                    
                    # Only publish if we have actual trade data
                    if candle.trade_count > 0:
                        self._publish_candle_to_redis(candle)
                    
                    # Remove the processed candle
                    del self.candle_accumulators[symbol][target_second]
                    
                    # Clean up old candles (older than 10 seconds)
                    cutoff_second = target_second - 10
                    keys_to_remove = [k for k in self.candle_accumulators[symbol].keys() if k < cutoff_second]
                    for key in keys_to_remove:
                        del self.candle_accumulators[symbol][key]
    
    def _publish_candle_to_redis(self, candle: CandleAccumulator):
        """Publish a completed candle to Redis"""
        try:
            # Prepare candle data
            candle_dict = candle.to_dict()
            
            # Publish to live subscribers
            channel = f"live_bars:{candle.symbol}"
            self.redis_client.publish(channel, json.dumps(candle_dict))
            
            # Cache for historical retrieval
            cache_key = f"intraday_bars:{candle.symbol}"
            self.redis_client.rpush(cache_key, json.dumps(candle_dict))
            self.redis_client.expire(cache_key, 86400)  # 24 hour expiry
            
            logging.debug(f"Published 1s candle for {candle.symbol}: {candle_dict}")
            
        except Exception as e:
            logging.error(f"Error publishing candle for {candle.symbol}: {e}", exc_info=True)
    
    def _report_quality_stats(self):
        """Report data quality statistics"""
        for symbol, monitor in self.quality_monitors.items():
            stats = monitor.get_quality_stats()
            logging.info(f"Quality stats for {symbol}: {json.dumps(stats)}")
    
    # Override QuoteListener methods
    def process_update(self, update_data: np.array) -> None:
        """Process real-time quote/trade updates"""
        try:
            trade = self._parse_trade_from_update(update_data)
            if trade and trade.is_trade:  # Only process actual trades
                self._process_trade(trade)
        except Exception as e:
            logging.error(f"Error processing update: {e}", exc_info=True)
    
    def process_summary(self, summary_data: np.array) -> None:
        """Process initial summary data when subscribing to a symbol"""
        try:
            # Process summary as a trade for initial state
            trade = self._parse_trade_from_update(summary_data)
            if trade:
                symbol = trade.symbol
                logging.info(f"Received summary for {symbol}: price={trade.last_price}, volume={trade.volume}")
                
                # Store initial state
                self.symbol_states[symbol] = {
                    'last_price': trade.last_price,
                    'total_volume': trade.volume,
                    'summary_received': True,
                    'summary_time': datetime.now()
                }
                
        except Exception as e:
            logging.error(f"Error processing summary: {e}", exc_info=True)
    
    def process_invalid_symbol(self, bad_symbol: str) -> None:
        """Handle invalid symbol notifications"""
        logging.error(f"Invalid symbol: {bad_symbol}")
    
    def feed_is_stale(self) -> None:
        """Handle feed disconnection"""
        logging.warning("IQFeed connection is stale - data may be delayed")
    
    def feed_is_fresh(self) -> None:
        """Handle feed reconnection"""
        logging.info("IQFeed connection is fresh - receiving live data")


def get_latest_timestamp_from_influx(symbol: str, measurement: str = "ohlc_1s") -> datetime | None:
    """
    Queries InfluxDB for the latest timestamp for a given symbol and measurement.
    Returns the datetime in UTC timezone.
    """
    flux_query = f'''
        from(bucket: "{INFLUX_BUCKET}")
          |> range(start: 0)
          |> filter(fn: (r) => r._measurement == "{measurement}" and r.symbol == "{symbol}")
          |> last()
          |> keep(columns: ["_time"])
    '''
    try:
        tables = query_api.query(query=flux_query)
        if not tables or not tables[0].records:
            logging.info(f"No existing data found for '{symbol}' in measurement '{measurement}'.")
            return None
        
        latest_time = tables[0].records[0].get_time()
        
        if latest_time.tzinfo is None:
            latest_time = latest_time.replace(tzinfo=timezone.utc)
            
        logging.info(f"Latest timestamp for '{symbol}' in '{measurement}' is {latest_time}.")
        return latest_time
    except Exception as e:
        logging.error(f"Error querying latest timestamp for {symbol} in {measurement}: {e}", exc_info=True)
        return None


def clear_symbol_cache(symbols: list):
    """Deletes the Redis cache for the given symbols before starting."""
    for symbol in symbols:
        cache_key = f"intraday_bars:{symbol}"
        logging.info(f"Clearing cache for {symbol} (key: {cache_key}).")
        redis_client.delete(cache_key)


def setup_logging_directories():
    """Create logging directories if they don't exist"""
    import os
    os.makedirs('logs', exist_ok=True)


def main():
    """Main function to start listening to live quote data."""
    setup_logging_directories()
    launch_iqfeed_service_if_needed()
    
    symbols = ["AAPL", "AMZN", "TSLA", "@NQM25"]
    
    # Optional: Clear cache for fresh start
    # clear_symbol_cache(symbols)
    
    quote_conn = get_iqfeed_quote_conn()
    if not quote_conn:
        logging.error("Could not get IQFeed QuoteConn. Exiting.")
        return

    listener = LiveQuoteListener()
    quote_conn.add_listener(listener)
    
    with iq.ConnConnector([quote_conn]):
        # Now we can configure the connection since it's connected
        
        # Optional: Select specific fields for updates to reduce data volume
        # This focuses on trade-related fields
        trade_fields = [
            "Symbol",
            "Most Recent Trade",
            "Most Recent Trade Size", 
            "Most Recent Trade Time",
            "Total Volume",
            "Bid",
            "Ask",
            "High",
            "Low",
            "Open",
            "Close"
        ]
        
        try:
            quote_conn.select_update_fieldnames(trade_fields)
            logging.info("Selected update fields for QuoteConn")
        except Exception as e:
            logging.warning(f"Could not select update fields: {e}. Using default fields.")
        
        # Subscribe to symbols for real-time updates
        for symbol in symbols:
            try:
                # Check for latest data in InfluxDB for backfill context
                latest_timestamp = get_latest_timestamp_from_influx(symbol)
                if latest_timestamp:
                    logging.info(f"Latest data for {symbol} in InfluxDB: {latest_timestamp}")
                else:
                    logging.info(f"No historical data found for {symbol} in InfluxDB")
                
                # Subscribe to real-time quotes and trades
                quote_conn.watch(symbol)
                logging.info(f"Subscribed to real-time data for {symbol}")
                
                # Small delay between subscriptions to avoid overwhelming the feed
                time.sleep(0.1)
                
            except Exception as e:
                logging.error(f"Error subscribing to {symbol}: {e}")
        
        try:
            logging.info("QuoteConn live data ingestion service is running. Press Ctrl+C to stop.")
            logging.info(f"Monitoring {len(symbols)} symbols: {', '.join(symbols)}")
            logging.info("Publishing 1-second candles to Redis and logging raw trades.")
            logging.info("Data will appear as trades occur during market hours.")
            
            while True:
                time.sleep(1)
                
        except KeyboardInterrupt:
            logging.info("Stopping live data ingestion.")
            try:
                for symbol in symbols:
                    quote_conn.unwatch(symbol)
                logging.info("Unsubscribed from all symbols.")
            except Exception as e:
                logging.error(f"Error during cleanup: {e}")


if __name__ == "__main__":
    main()