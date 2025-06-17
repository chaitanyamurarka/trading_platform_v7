# app/websocket_manager.py
# FIXED: Proper Redis subscription and message forwarding

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
    interval: str
    connections: Set[WebSocket] = field(default_factory=set)
    redis_subscription: Optional[Any] = None
    last_message: Optional[dict] = None
    created_at: datetime = field(default_factory=datetime.now)
    message_task: Optional[asyncio.Task] = None
    
class ConnectionManager:
    """
    Manages WebSocket connections with Redis subscription pooling.
    
    Key optimizations:
    - Shared Redis subscriptions for same symbol (regardless of interval)
    - Automatic cleanup of unused subscriptions  
    - Connection health monitoring
    - Exponential backoff for Redis reconnections
    """
    
    def __init__(self):
        # Core data structures
        self.connections: Dict[WebSocket, ConnectionInfo] = {}
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
        self._max_reconnect_delay = 60.0   # Max 60 seconds
        
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
                    logger.error(f"Error closing Redis subscription: {e}")
        
        # Close Redis client
        try:
            await self.redis_client.close()
        except Exception as e:
            logger.error(f"Error closing Redis client: {e}")
            
        logger.info("WebSocket connection manager stopped")
    
    def _get_channel_key(self, symbol: str) -> str:
        """Generate Redis channel key for symbol (matches ingestion service)"""
        return f"live_bars:{symbol}"
    
    def _get_group_key(self, symbol: str, interval: str) -> str:
        """Generate unique group key for symbol/interval combination"""
        return f"{symbol}:{interval}"
    
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
            
            # Get or create subscription group for this symbol/interval
            group_key = self._get_group_key(symbol, interval)
            
            if group_key not in self.subscription_groups:
                # Create new subscription group
                channel_key = self._get_channel_key(symbol)
                group = SubscriptionGroup(
                    channel=channel_key,
                    symbol=symbol,
                    interval=interval
                )
                self.subscription_groups[group_key] = group
                
                # Start Redis subscription for this group
                await self._start_redis_subscription(group)
                
                logger.info(f"Created new subscription group for {group_key} -> Redis channel {channel_key}")
            
            # Add connection to existing group
            group = self.subscription_groups[group_key]
            group.connections.add(websocket)
            
            # Send cached message if available (resampled for this interval)
            if group.last_message:
                try:
                    resampled_data = await self._resample_for_interval(group.last_message, interval, timezone)
                    if resampled_data:
                        await websocket.send_json(resampled_data)
                except Exception as e:
                    logger.warning(f"Failed to send cached message to new connection: {e}")
            
            self.metrics['total_connections'] += 1
            logger.info(f"Added WebSocket connection for {symbol}/{interval} (total: {len(self.connections)})")
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to add WebSocket connection: {e}")
            return False
    
    async def remove_connection(self, websocket: WebSocket):
        """Remove a WebSocket connection and clean up if necessary"""
        if websocket not in self.connections:
            return
            
        conn_info = self.connections.pop(websocket)
        group_key = self._get_group_key(conn_info.symbol, conn_info.interval)
        
        # Remove from subscription group
        if group_key in self.subscription_groups:
            group = self.subscription_groups[group_key]
            group.connections.discard(websocket)
            
            # If no more connections in this group, mark for cleanup
            if not group.connections:
                logger.info(f"No more connections for {group_key}, will cleanup subscription")
        
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
            
            logger.info(f"Started Redis subscription for {group.channel}")
            
        except Exception as e:
            logger.error(f"Failed to start Redis subscription for {group.channel}: {e}")
            # Schedule reconnection
            asyncio.create_task(self._schedule_redis_reconnect(group))
    
    async def _handle_redis_messages(self, group: SubscriptionGroup):
        """Handle incoming Redis messages for a subscription group"""
        try:
            while group.redis_subscription and group.connections:
                try:
                    message = await group.redis_subscription.get_message(
                        ignore_subscribe_messages=True, 
                        timeout=1.0
                    )
                    
                    if message and message['data']:
                        try:
                            # Parse the message (1-second bar data from ingestion service)
                            bar_data = json.loads(message['data'])
                            group.last_message = bar_data
                            
                            # Process this bar for all connections in the group
                            await self._process_bar_for_group(group, bar_data)
                            
                        except json.JSONDecodeError as e:
                            logger.error(f"Failed to parse Redis message: {e}")
                            
                except asyncio.TimeoutError:
                    # Timeout is normal, continue listening
                    continue
                    
                except Exception as e:
                    logger.error(f"Error handling Redis message for {group.channel}: {e}")
                    break
                    
        except asyncio.CancelledError:
            logger.info(f"Redis message handler cancelled for {group.channel}")
            
        except Exception as e:
            logger.error(f"Redis message handler error for {group.channel}: {e}")
            # Schedule reconnection
            asyncio.create_task(self._schedule_redis_reconnect(group))
    
    async def _process_bar_for_group(self, group: SubscriptionGroup, bar_data: dict):
        """Process a 1-second bar for all connections in a group"""
        if not group.connections:
            return
        
        # Import here to avoid circular imports
        from .live_data_service import BarResampler
            
        failed_connections = []
        
        # Process for each connection (they might have different intervals/timezones)
        for websocket in group.connections.copy():
            try:
                if websocket not in self.connections:
                    failed_connections.append(websocket)
                    continue
                    
                conn_info = self.connections[websocket]
                
                # Create a resampler for this connection's interval/timezone
                # Note: In production, you might want to cache resamplers per connection
                resampler = BarResampler(conn_info.interval, conn_info.timezone)
                
                # Add the bar and check if a completed bar is returned
                completed_bar = resampler.add_bar(bar_data)
                
                # Prepare live update payload
                live_update_payload = {
                    "completed_bar": completed_bar.model_dump() if completed_bar else None,
                    "current_bar": resampler.current_bar.model_dump() if resampler.current_bar else None
                }
                
                # Send update to this connection
                await websocket.send_json(live_update_payload)
                
                # Update last activity
                conn_info.last_activity = datetime.now()
                self.metrics['messages_sent'] += 1
                
            except (WebSocketDisconnect, ConnectionClosed) as e:
                logger.info(f"WebSocket disconnected during bar processing: {e}")
                failed_connections.append(websocket)
                
            except Exception as e:
                logger.error(f"Failed to process bar for WebSocket: {e}")
                failed_connections.append(websocket)
        
        # Remove failed connections
        for websocket in failed_connections:
            group.connections.discard(websocket)
            self.connections.pop(websocket, None)
    
    async def _resample_for_interval(self, bar_data: dict, interval: str, timezone: str) -> Optional[dict]:
        """Resample a single bar for a specific interval (for cached messages)"""
        try:
            from .live_data_service import BarResampler
            
            resampler = BarResampler(interval, timezone)
            completed_bar = resampler.add_bar(bar_data)
            
            return {
                "completed_bar": completed_bar.model_dump() if completed_bar else None,
                "current_bar": resampler.current_bar.model_dump() if resampler.current_bar else None
            }
        except Exception as e:
            logger.error(f"Error resampling bar for interval {interval}: {e}")
            return None
    
    async def _schedule_redis_reconnect(self, group: SubscriptionGroup):
        """Schedule Redis reconnection with exponential backoff"""
        delay = min(self._redis_reconnect_delay, self._max_reconnect_delay)
        
        logger.info(f"Scheduling Redis reconnect for {group.channel} in {delay}s")
        
        await asyncio.sleep(delay)
        
        # Double the delay for next time (exponential backoff)
        self._redis_reconnect_delay = min(self._redis_reconnect_delay * 2, self._max_reconnect_delay)
        self.metrics['redis_reconnects'] += 1
        
        # Attempt reconnection if group still has connections
        if group.connections:
            await self._start_redis_subscription(group)
    
    async def _cleanup_loop(self):
        """Background task to clean up unused subscriptions and dead connections"""
        while True:
            try:
                await asyncio.sleep(30)  # Run every 30 seconds
                await self._cleanup_unused_subscriptions()
                await self._cleanup_dead_connections()
                self.metrics['cleanup_cycles'] += 1
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in cleanup loop: {e}")
    
    async def _cleanup_unused_subscriptions(self):
        """Remove subscription groups with no active connections"""
        to_remove = []
        
        for group_key, group in self.subscription_groups.items():
            if not group.connections:
                # Grace period before cleanup (5 minutes)
                if datetime.now() - group.created_at > timedelta(minutes=5):
                    to_remove.append(group_key)
        
        for group_key in to_remove:
            group = self.subscription_groups.pop(group_key)
            
            # Cancel message processing task
            if group.message_task:
                group.message_task.cancel()
            
            # Close Redis subscription
            if group.redis_subscription:
                try:
                    await group.redis_subscription.unsubscribe()
                    await group.redis_subscription.close()
                except Exception as e:
                    logger.error(f"Error closing unused subscription {group_key}: {e}")
            
            self.metrics['active_subscriptions'] -= 1
            logger.info(f"Cleaned up unused subscription: {group_key}")
    
    async def _cleanup_dead_connections(self):
        """Remove connections that are no longer active"""
        current_time = datetime.now()
        dead_connections = []
        
        for websocket, conn_info in self.connections.items():
            # Consider connection dead if no activity for 10 minutes
            if current_time - conn_info.last_activity > timedelta(minutes=10):
                dead_connections.append(websocket)
        
        for websocket in dead_connections:
            await self.remove_connection(websocket)
            logger.info("Cleaned up dead WebSocket connection")
    
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