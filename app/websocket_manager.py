# app/websocket_manager.py
# FIXED: Proper Redis subscription and message forwarding, and parallel WebSocket processing

import asyncio
import json
import logging
from typing import Dict, Set, Optional, Any
from dataclasses import dataclass, field
from datetime import datetime, timedelta
import redis.asyncio as aioredis
from starlette.websockets import WebSocket, WebSocketDisconnect
from websockets.exceptions import ConnectionClosed

from .config import settings
from . import schemas
from .live_data_service import BarResampler # Ensure BarResampler is available

logger = logging.getLogger(__name__)

@dataclass
class ConnectionInfo:
    """Information about a WebSocket connection"""
    websocket: WebSocket
    symbol: str
    interval: str
    timezone: str
    connected_at: datetime = field(default_factory=datetime.now)
    last_activity: datetime = field(default_factory=datetime.now)

@dataclass
class SubscriptionGroup:
    """Manages a group of connections for the same Redis channel"""
    channel: str
    symbol: str
    interval: str # This interval is the *source* interval from the Redis channel
    connections: Set[WebSocket] = field(default_factory=set)
    redis_subscription: Optional[Any] = None
    last_message: Optional[dict] = None
    created_at: datetime = field(default_factory=datetime.now)
    message_task: Optional[asyncio.Task] = None

class ConnectionManager:
    """
    Manages WebSocket connections with Redis subscription pooling.

    Key optimizations:
    - Shared Redis subscriptions for same symbol (regardless of interval for clients)
    - Automatic cleanup of unused subscriptions
    - Connection health monitoring
    - Exponential backoff for Redis reconnections
    - Parallel processing of messages to connected clients within a group.
    """

    def __init__(self):
        # Core data structures
        self.connections: Dict[WebSocket, ConnectionInfo] = {}
        # Stores groups by symbol (as the Redis channel is per symbol)
        self.subscription_groups: Dict[str, SubscriptionGroup] = {}

        # Redis connection with pooling
        self.redis_client = aioredis.from_url(
            settings.REDIS_URL,
            decode_responses=True,
            max_connections=20,  # Connection pool size
            retry_on_timeout=True
        )

        # Background tasks
        self._cleanup_task: Optional[asyncio.Task] = None
        self._redis_reconnect_delay = 1.0  # Start with 1 second
        self._max_reconnect_delay = 60.0  # Max 60 seconds

        # Metrics for monitoring
        self.metrics = {
            'total_connections': 0,
            'active_subscriptions': 0,
            'redis_reconnects': 0,
            'messages_sent': 0,
            'cleanup_cycles': 0
        }

    async def start(self):
        """Start background tasks"""
        if not self._cleanup_task:
            self._cleanup_task = asyncio.create_task(self._cleanup_loop())
            logger.info("WebSocket connection manager started")

    async def stop(self):
        """Stop all background tasks and clean up"""
        if self._cleanup_task:
            self._cleanup_task.cancel()
            try:
                await self._cleanup_task
            except asyncio.CancelledError:
                pass

        # Close all Redis subscriptions
        for group in self.subscription_groups.values():
            if group.message_task:
                group.message_task.cancel()
            if group.redis_subscription:
                try:
                    await group.redis_subscription.unsubscribe()
                    await group.redis_subscription.close()
                except Exception as e:
                    logger.error(f"Error closing Redis subscription for channel {group.channel}: {e}")

        # Close Redis client
        try:
            await self.redis_client.close()
        except Exception as e:
            logger.error(f"Error closing Redis client: {e}")

        logger.info("WebSocket connection manager stopped")

    def _get_channel_key(self, symbol: str) -> str:
        """Generate Redis channel key for symbol (matches ingestion service)"""
        return f"live_bars:{symbol}"

    # No need for _get_group_key if groups are per symbol channel
    # def _get_group_key(self, symbol: str, interval: str) -> str:
    #     """Generate unique group key for symbol/interval combination"""
    #     return f"{symbol}:{interval}"

    async def add_connection(self, websocket: WebSocket, symbol: str, interval: str, timezone: str) -> bool:
        """
        Add a new WebSocket connection to the manager.
        Returns True if successfully added, False otherwise.
        """
        try:
            # Store connection info
            conn_info = ConnectionInfo(
                websocket=websocket,
                symbol=symbol,
                interval=interval,
                timezone=timezone
            )
            self.connections[websocket] = conn_info

            # Get or create subscription group for this symbol (Redis channel)
            channel_key = self._get_channel_key(symbol) # Group by channel, which is per symbol

            if channel_key not in self.subscription_groups:
                # Create new subscription group
                # The 'interval' here is somewhat arbitrary for the group itself, as the channel carries 1s bars.
                # Client-specific intervals are handled during message processing.
                group = SubscriptionGroup(
                    channel=channel_key,
                    symbol=symbol,
                    interval="1s" # Assuming source data is 1s, this is just for group info
                )
                self.subscription_groups[channel_key] = group

                # Start Redis subscription for this group
                await self._start_redis_subscription(group)

                logger.info(f"Created new subscription group for Redis channel {channel_key}")

            # Add connection to existing group
            group = self.subscription_groups[channel_key]
            group.connections.add(websocket)

            # Send cached message if available (resampled for this interval)
            if group.last_message:
                try:
                    # Resample the last 1-second bar for the new connection's interval
                    resampled_data = self._resample_for_interval(group.last_message, interval, timezone)
                    if resampled_data:
                        await websocket.send_json(resampled_data)
                        logger.debug(f"Sent cached bar to new connection {symbol}/{interval}")
                except Exception as e:
                    logger.warning(f"Failed to send cached message to new connection {symbol}/{interval}: {e}")

            self.metrics['total_connections'] += 1
            logger.info(f"Added WebSocket connection for {symbol}/{interval} (total: {len(self.connections)})")

            return True

        except Exception as e:
            logger.error(f"Failed to add WebSocket connection for {symbol}/{interval}: {e}")
            return False

    async def remove_connection(self, websocket: WebSocket):
        """Remove a WebSocket connection and clean up if necessary"""
        if websocket not in self.connections:
            return

        conn_info = self.connections.pop(websocket)
        channel_key = self._get_channel_key(conn_info.symbol) # Group by channel

        # Remove from subscription group
        if channel_key in self.subscription_groups:
            group = self.subscription_groups[channel_key]
            group.connections.discard(websocket)

            # If no more connections in this group, mark for cleanup
            if not group.connections:
                logger.info(f"No more connections for {channel_key}, will cleanup subscription shortly.")

        self.metrics['total_connections'] -= 1
        logger.info(f"Removed WebSocket connection for {conn_info.symbol}/{conn_info.interval} (remaining: {len(self.connections)})")


    async def _start_redis_subscription(self, group: SubscriptionGroup):
        """Start Redis subscription for a subscription group"""
        try:
            # Create pubsub instance
            pubsub = self.redis_client.pubsub()
            await pubsub.subscribe(group.channel)

            group.redis_subscription = pubsub
            self.metrics['active_subscriptions'] += 1

            # Start background task to handle messages
            group.message_task = asyncio.create_task(self._handle_redis_messages(group))

            # Reset reconnect delay on successful connection
            self._redis_reconnect_delay = 1.0

            logger.info(f"Started Redis subscription for channel {group.channel}")

        except Exception as e:
            logger.error(f"Failed to start Redis subscription for {group.channel}: {e}")
            # Schedule reconnection
            asyncio.create_task(self._schedule_redis_reconnect(group))

    async def _handle_redis_messages(self, group: SubscriptionGroup):
        """Handle incoming Redis messages for a subscription group"""
        try:
            while group.redis_subscription and self.redis_client.connection_pool.max_connections: # Check client health too
                try:
                    message = await group.redis_subscription.get_message(
                        ignore_subscribe_messages=True,
                        timeout=1.0 # Poll for messages
                    )

                    if message and message['data']:
                        try:
                            # Parse the message (1-second bar data from ingestion service)
                            bar_data = json.loads(message['data'])
                            group.last_message = bar_data # Cache the latest 1s bar

                            # Process this bar for all connections in the group
                            await self._process_bar_for_group(group, bar_data)

                        except json.JSONDecodeError as e:
                            logger.error(f"Failed to parse Redis message for {group.channel}: {e}")

                except asyncio.TimeoutError:
                    # Timeout is normal, continue listening
                    continue

                except Exception as e:
                    logger.error(f"Error handling Redis message for {group.channel}: {e}")
                    # Reconnection will be handled by _schedule_redis_reconnect
                    break # Exit loop to allow scheduling of reconnect

        except asyncio.CancelledError:
            logger.info(f"Redis message handler cancelled for {group.channel}")

        except Exception as e:
            logger.error(f"Redis message handler top-level error for {group.channel}: {e}")
            # Schedule reconnection for persistent errors
            asyncio.create_task(self._schedule_redis_reconnect(group))

    async def _process_bar_for_group(self, group: SubscriptionGroup, bar_data: dict):
        """
        Process a 1-second bar for all connections in a group, performing
        resampling and sending updates in parallel.
        """
        if not group.connections:
            return

        tasks = []
        # Cache for resampled bars to avoid redundant calculations for same interval/timezone
        # Key: (client_interval, client_timezone) -> value: {completed_bar, current_bar}
        interval_resampling_cache: Dict[tuple[str, str], dict] = {}
        
        # Create a list copy to iterate safely in case connections are removed during processing
        active_websockets = list(group.connections)

        for websocket in active_websockets:
            try:
                conn_info = self.connections.get(websocket)
                if not conn_info: # Connection might have been removed concurrently
                    continue

                client_interval = conn_info.interval
                client_timezone = conn_info.timezone
                
                cache_key = (client_interval, client_timezone)

                if cache_key not in interval_resampling_cache:
                    # Only resample if not already done for this interval/timezone combo
                    resampler = BarResampler(client_interval, client_timezone)
                    
                    # Add the raw 1-second bar to the resampler
                    completed_bar = resampler.add_bar(bar_data)
                    
                    # Prepare live update payload
                    live_update_payload = {
                        "completed_bar": completed_bar.model_dump() if completed_bar else None,
                        "current_bar": resampler.current_bar.model_dump() if resampler.current_bar else None
                    }
                    interval_resampling_cache[cache_key] = live_update_payload

                # Create an async task for sending this specific client's data
                tasks.append(self._send_to_websocket(websocket, interval_resampling_cache[cache_key]))

                # Update last activity for this connection
                conn_info.last_activity = datetime.now()
                self.metrics['messages_sent'] += 1

            except (WebSocketDisconnect, ConnectionClosed) as e:
                logger.info(f"WebSocket disconnected during bar processing for {conn_info.symbol}/{conn_info.interval}: {e}")
                # Don't add to failed_connections here; the disconnect method will handle removal
                asyncio.create_task(self.remove_connection(websocket)) # Schedule removal
            except Exception as e:
                logger.error(f"Failed to process bar for WebSocket {conn_info.symbol}/{conn_info.interval}: {e}", exc_info=True)
                asyncio.create_task(self.remove_connection(websocket)) # Schedule removal

        # Process all send tasks in parallel
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
            logger.debug(f"Processed bar for {len(tasks)} connections in parallel for {group.symbol}")
        else:
            logger.debug(f"No active connections to send bar data to for {group.symbol}")

    async def _send_to_websocket(self, websocket: WebSocket, data: dict):
        """Helper to send JSON data to a single websocket connection."""
        try:
            if websocket in self.connections: # Double check if connection is still valid
                await websocket.send_json(data)
            else:
                logger.warning("Attempted to send to a WebSocket that was already disconnected.")
        except (WebSocketDisconnect, ConnectionClosed) as e:
            logger.info(f"Client disconnected during send operation: {e}")
            await self.remove_connection(websocket) # Ensure proper cleanup
        except Exception as e:
            logger.error(f"Error sending data to websocket: {e}", exc_info=True)
            await self.remove_connection(websocket) # Disconnect on other errors as well

    # This function is now only used for sending cached messages to new connections,
    # as main processing happens in _process_bar_for_group
    def _resample_for_interval(self, bar_data: dict, interval: str, timezone: str) -> Optional[dict]:
        """Resample a single 1-second bar for a specific interval (for cached messages)"""
        try:
            resampler = BarResampler(interval, timezone)
            completed_bar = resampler.add_bar(bar_data)

            return {
                "completed_bar": completed_bar.model_dump() if completed_bar else None,
                "current_bar": resampler.current_bar.model_dump() if resampler.current_bar else None
            }
        except Exception as e:
            logger.error(f"Error resampling bar for interval {interval} (cached message): {e}")
            return None

    async def _schedule_redis_reconnect(self, group: SubscriptionGroup):
        """Schedule Redis reconnection with exponential backoff"""
        delay = min(self._redis_reconnect_delay, self._max_reconnect_delay)

        logger.info(f"Scheduling Redis reconnect for {group.channel} in {delay:.2f}s")

        await asyncio.sleep(delay)

        # Double the delay for next time (exponential backoff)
        self._redis_reconnect_delay = min(self._redis_reconnect_delay * 2, self._max_reconnect_delay)
        self.metrics['redis_reconnects'] += 1

        # Attempt reconnection if group still has connections
        if group.connections: # Only reconnect if there are still clients interested
            logger.info(f"Attempting Redis reconnection for {group.channel}")
            await self._start_redis_subscription(group)
        else:
            logger.info(f"No connections left for {group.channel}, skipping Redis reconnection.")


    async def _cleanup_loop(self):
        """Background task to clean up unused subscriptions and dead connections"""
        while True:
            try:
                await asyncio.sleep(30)  # Run every 30 seconds
                await self._cleanup_unused_subscriptions()
                await self._cleanup_dead_connections()
                self.metrics['cleanup_cycles'] += 1

            except asyncio.CancelledError:
                logger.info("Cleanup loop cancelled.")
                break
            except Exception as e:
                logger.error(f"Error in cleanup loop: {e}", exc_info=True)

    async def _cleanup_unused_subscriptions(self):
        """Remove subscription groups with no active connections"""
        to_remove = []

        for channel_key, group in list(self.subscription_groups.items()): # Iterate over copy for safe modification
            if not group.connections:
                # Give a grace period before actual cleanup (e.g., 60 seconds)
                if datetime.now() - group.created_at > timedelta(seconds=60): # Adjusted grace period
                    to_remove.append(channel_key)

        for channel_key in to_remove:
            group = self.subscription_groups.pop(channel_key)

            # Cancel message processing task
            if group.message_task:
                group.message_task.cancel()
                try:
                    await group.message_task
                except asyncio.CancelledError:
                    pass # Task was successfully cancelled

            # Close Redis subscription
            if group.redis_subscription:
                try:
                    await group.redis_subscription.unsubscribe()
                    await group.redis_subscription.close()
                except Exception as e:
                    logger.error(f"Error closing unused subscription {channel_key}: {e}")

            self.metrics['active_subscriptions'] -= 1
            logger.info(f"Cleaned up unused subscription: {channel_key}")

    async def _cleanup_dead_connections(self):
        """Remove connections that are no longer active based on last_activity"""
        current_time = datetime.now()
        # Create a list of websockets to remove to avoid modifying dict during iteration
        websockets_to_remove = []

        for websocket, conn_info in self.connections.items():
            # Consider connection dead if no activity for 10 minutes
            if current_time - conn_info.last_activity > timedelta(minutes=10):
                websockets_to_remove.append(websocket)

        for websocket in websockets_to_remove:
            await self.remove_connection(websocket) # Use the existing removal logic
            logger.info(f"Cleaned up dead WebSocket connection for {self.connections.get(websocket).symbol if websocket in self.connections else 'N/A'}")

    def get_metrics(self) -> dict:
        """Get current metrics for monitoring"""
        return {
            **self.metrics,
            'active_connections': len(self.connections),
            'active_subscription_groups': len(self.subscription_groups),
            'redis_reconnect_delay': self._redis_reconnect_delay
        }


# Global connection manager instance
connection_manager = ConnectionManager()


# Lifecycle management functions for FastAPI
async def startup_connection_manager():
    """Start the connection manager on app startup"""
    await connection_manager.start()


async def shutdown_connection_manager():
    """Stop the connection manager on app shutdown"""
    await connection_manager.stop()