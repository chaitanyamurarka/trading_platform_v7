# app/websocket_manager.py
# FIXED: Subscribes to raw ticks, manages stateful resamplers, and forwards aggregated bars.

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
from .services.live_data_service import BarResampler, TickBarResampler
from .services.heikin_ashi_calculator import HeikinAshiLiveCalculator

logger = logging.getLogger(__name__)

# (ConnectionInfo and SubscriptionGroup dataclasses remain the same)
@dataclass
class ConnectionInfo:
    """Information about a WebSocket connection"""
    websocket: WebSocket
    symbol: str
    interval: str
    timezone: str
    data_type: schemas.DataType # <-- ADDED
    connected_at: datetime = field(default_factory=datetime.now)

@dataclass
class SubscriptionGroup:
    """Manages a group of connections for the same Redis channel"""
    channel: str
    symbol: str
    connections: Set[WebSocket] = field(default_factory=set)
    resamplers: Dict[tuple, Any] = field(default_factory=dict)
    redis_subscription: Optional[Any] = None
    message_task: Optional[asyncio.Task] = None

class ConnectionManager:
    """Manages WebSocket connections with Redis subscription pooling."""
    def __init__(self):
        self.connections: Dict[WebSocket, ConnectionInfo] = {}
        self.subscription_groups: Dict[str, SubscriptionGroup] = {}
        self.redis_client = aioredis.from_url(settings.REDIS_URL, decode_responses=True)
        self._cleanup_task: Optional[asyncio.Task] = None

        self._redis_reconnect_delay = 1.0
        self._max_reconnect_delay = 60.0

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
        """
        Generate Redis channel key for raw ticks.
        """
        # MODIFIED: Subscribe to the new raw ticks channel.
        return f"live_ticks:{symbol}"
    
    def _create_resampler(self, interval: str, timezone: str, data_type: schemas.DataType):
        """Factory to create the correct resampler or calculator."""
        if data_type == schemas.DataType.HEIKIN_ASHI:
            return HeikinAshiLiveCalculator()
        elif 'tick' in interval:
            return TickBarResampler(interval)
        else:
            return BarResampler(interval, timezone)

    async def add_connection(self, websocket: WebSocket, symbol: str, interval: str, timezone: str, data_type: schemas.DataType) -> bool:
        """Adds a new WebSocket connection to the manager."""
        conn_info = ConnectionInfo(websocket, symbol, interval, timezone, data_type)
        self.connections[websocket] = conn_info
        channel_key = self._get_channel_key(symbol)

        if channel_key not in self.subscription_groups:
            group = SubscriptionGroup(channel=channel_key, symbol=symbol)
            self.subscription_groups[channel_key] = group
            await self._start_redis_subscription(group)
        
        group = self.subscription_groups[channel_key]
        group.connections.add(websocket)

        # Create resampler for this connection's config if it doesn't exist
        resampler_key = (interval, timezone, data_type.value)
        if resampler_key not in group.resamplers:
            group.resamplers[resampler_key] = self._create_resampler(interval, timezone, data_type)
            logger.info(f"Created new resampler for group {symbol}, key: {resampler_key}")
        
        return True

    async def remove_connection(self, websocket: WebSocket):
        """Remove a WebSocket connection and clean up if necessary"""
        if websocket not in self.connections:
            return

        conn_info = self.connections.pop(websocket)
        channel_key = self._get_channel_key(conn_info.symbol)

        if channel_key in self.subscription_groups:
            group = self.subscription_groups[channel_key]
            group.connections.discard(websocket)
            
            # Check if any resampler is now unused and can be removed
            resampler_key_to_remove = (conn_info.interval, conn_info.timezone)
            is_resampler_in_use = any(
                (c.interval, c.timezone) == resampler_key_to_remove
                for c in self.connections.values() if c.symbol == conn_info.symbol
            )
            if not is_resampler_in_use and resampler_key_to_remove in group.resamplers:
                del group.resamplers[resampler_key_to_remove]
                logger.info(f"Cleaned up unused BarResampler for key: {resampler_key_to_remove}")


            if not group.connections:
                logger.info(f"No more connections for {channel_key}, will cleanup subscription shortly.")

        self.metrics['total_connections'] -= 1
        logger.info(f"Removed WebSocket connection for {conn_info.symbol}/{conn_info.interval} (remaining: {len(self.connections)})")

    async def _start_redis_subscription(self, group: SubscriptionGroup):
        """Start Redis subscription for a subscription group"""
        try:
            pubsub = self.redis_client.pubsub()
            await pubsub.subscribe(group.channel)

            group.redis_subscription = pubsub
            self.metrics['active_subscriptions'] += 1
            group.message_task = asyncio.create_task(self._handle_redis_messages(group))

            self._redis_reconnect_delay = 1.0
            logger.info(f"Started Redis subscription for channel {group.channel}")

        except Exception as e:
            logger.error(f"Failed to start Redis subscription for {group.channel}: {e}")
            asyncio.create_task(self._schedule_redis_reconnect(group))

    async def _handle_redis_messages(self, group: SubscriptionGroup):
        """Handle incoming raw tick messages for a subscription group"""
        try:
            while group.redis_subscription:
                message = await group.redis_subscription.get_message(ignore_subscribe_messages=True, timeout=1.0)

                if message and message['data']:
                    try:
                        tick_data = json.loads(message['data'])
                        group.last_message = tick_data
                        await self._process_tick_for_group(group, tick_data)
                    except json.JSONDecodeError as e:
                        logger.error(f"Failed to parse Redis message for {group.channel}: {e}")

        except asyncio.CancelledError:
            logger.info(f"Redis message handler cancelled for {group.channel}")
        except Exception as e:
            logger.error(f"Redis message handler error for {group.channel}: {e}")
            asyncio.create_task(self._schedule_redis_reconnect(group))

    async def _process_tick_for_group(self, group: SubscriptionGroup, tick_data: dict):
        """Processes a raw tick for all connections in a group."""
        tasks = []
        payloads: Dict[tuple, schemas.LiveDataMessage] = {}
        
        regular_resamplers = {k: v for k, v in group.resamplers.items() if isinstance(v, (BarResampler, TickBarResampler))}

        # 1. Process all regular resamplers first
        for resampler_key, resampler in regular_resamplers.items():
            completed_bar = resampler.add_bar(tick_data)
            payloads[resampler_key] = schemas.LiveDataMessage(
                completed_bar=completed_bar,
                current_bar=resampler.current_bar
            )
        
        # 2. Process Heikin Ashi calculators using the results of regular resamplers
        for resampler_key, calculator in group.resamplers.items():
            if isinstance(calculator, HeikinAshiLiveCalculator):
                # Find the matching regular resampler payload
                regular_key = (resampler_key[0], resampler_key[1], 'regular')
                if regular_key in payloads:
                    regular_payload = payloads[regular_key]
                    completed_ha, current_ha = None, None
                    if regular_payload.completed_bar:
                        completed_ha = calculator.calculate_next_completed(regular_payload.completed_bar)
                    if regular_payload.current_bar:
                        current_ha = calculator.calculate_current_bar(regular_payload.current_bar)
                    
                    payloads[resampler_key] = schemas.LiveDataMessage(
                        completed_bar=completed_ha,
                        current_bar=current_ha
                    )

        # 3. Send the correct payload to each websocket
        for websocket in list(group.connections):
            conn_info = self.connections.get(websocket)
            if conn_info:
                payload_key = (conn_info.interval, conn_info.timezone, conn_info.data_type.value)
                if payload_key in payloads:
                    tasks.append(self._send_to_websocket(websocket, payloads[payload_key].model_dump(exclude_none=True)))
        
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _send_to_websocket(self, websocket: WebSocket, data: dict):
        """Helper to send JSON data to a single websocket connection."""
        try:
            if websocket in self.connections:
                await websocket.send_json(data)
        except (WebSocketDisconnect, ConnectionClosed):
            logger.info("Client disconnected during send operation.")
            await self.remove_connection(websocket)
        except Exception as e:
            logger.error(f"Error sending data to websocket: {e}", exc_info=True)
            await self.remove_connection(websocket)

    # _resample_for_interval is no longer needed here as resampling is stateful

    async def _schedule_redis_reconnect(self, group: SubscriptionGroup):
        """Schedule Redis reconnection with exponential backoff"""
        delay = min(self._redis_reconnect_delay, self._max_reconnect_delay)
        logger.info(f"Scheduling Redis reconnect for {group.channel} in {delay:.2f}s")
        await asyncio.sleep(delay)
        self._redis_reconnect_delay = min(self._redis_reconnect_delay * 2, self._max_reconnect_delay)
        self.metrics['redis_reconnects'] += 1
        if group.connections:
            logger.info(f"Attempting Redis reconnection for {group.channel}")
            await self._start_redis_subscription(group)

    async def _cleanup_loop(self):
        """Background task to clean up unused subscriptions and dead connections"""
        while True:
            try:
                await asyncio.sleep(30)
                await self._cleanup_unused_subscriptions()
                # Dead connection cleanup could be added here if needed
                self.metrics['cleanup_cycles'] += 1
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in cleanup loop: {e}", exc_info=True)

    async def _cleanup_unused_subscriptions(self):
        """Remove subscription groups with no active connections"""
        to_remove = [key for key, group in self.subscription_groups.items() if not group.connections]
        for channel_key in to_remove:
            group = self.subscription_groups.pop(channel_key)
            if group.message_task:
                group.message_task.cancel()
            if group.redis_subscription:
                await group.redis_subscription.unsubscribe()
                await group.redis_subscription.close()
            self.metrics['active_subscriptions'] -= 1
            logger.info(f"Cleaned up unused subscription: {channel_key}")

    def get_metrics(self) -> dict:
        """Get current metrics for monitoring"""
        return {
            **self.metrics,
            'active_connections': len(self.connections),
            'active_subscription_groups': len(self.subscription_groups),
            'resamplers_in_memory': sum(len(g.resamplers) for g in self.subscription_groups.values()),
            'redis_reconnect_delay': self._redis_reconnect_delay
        }

# Global connection manager instance
connection_manager = ConnectionManager()

# Lifecycle management functions for FastAPI
async def startup_connection_manager():
    await connection_manager.start()

async def shutdown_connection_manager():
    await connection_manager.stop()