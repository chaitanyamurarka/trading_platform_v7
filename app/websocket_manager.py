# chaitanyamurarka/trading_platform_v7/trading_platform_v7-e2d358352a61cb7a1309edf91f97d1e2f22f6d7b/app/websocket_manager.py
import asyncio
import json
import logging
from typing import Dict, Set, Optional, Any, Union
from dataclasses import dataclass, field
from datetime import datetime
import redis.asyncio as aioredis
from starlette.websockets import WebSocket, WebSocketDisconnect, WebSocketState
from websockets.exceptions import ConnectionClosed

from .config import settings
from . import schemas
from .services.live_data_handler import BarResampler, TickBarResampler, resample_ticks_to_bars
from .services.heikin_ashi_calculator import HeikinAshiLiveCalculator, calculate_historical_heikin_ashi


logger = logging.getLogger(__name__)

@dataclass
class ConnectionInfo:
    """Information about a WebSocket connection, now including data_type."""
    websocket: WebSocket
    symbol: str
    interval: str
    timezone: str
    data_type: schemas.DataType
    connected_at: datetime = field(default_factory=datetime.now)

@dataclass
class SubscriptionGroup:
    """Manages a group of connections for the same Redis channel, with resamplers for different data types."""
    channel: str
    symbol: str
    connections: Set[WebSocket] = field(default_factory=set)
    resamplers: Dict[tuple[str, str], Union[BarResampler, TickBarResampler]] = field(default_factory=dict)
    heikin_ashi_calculators: Dict[tuple[str, str], HeikinAshiLiveCalculator] = field(default_factory=dict)
    redis_subscription: Optional[Any] = None
    message_task: Optional[asyncio.Task] = None

class ConnectionManager:
    """Manages WebSocket connections, subscribing to raw ticks and performing server-side aggregation for all data types."""
    def __init__(self):
        self.connections: Dict[WebSocket, ConnectionInfo] = {}
        self.subscription_groups: Dict[str, SubscriptionGroup] = {}
        self.redis_client = aioredis.from_url(settings.REDIS_URL, decode_responses=True, max_connections=50)
        self._cleanup_task: Optional[asyncio.Task] = None

    async def start(self):
        if not self._cleanup_task:
            self._cleanup_task = asyncio.create_task(self._cleanup_loop())
            logger.info("ConnectionManager started with cleanup loop.")

    async def stop(self):
        if self._cleanup_task:
            self._cleanup_task.cancel()
        for group in self.subscription_groups.values():
            if group.message_task:
                group.message_task.cancel()
            if group.redis_subscription:
                await group.redis_subscription.unsubscribe()
        await self.redis_client.close()
        logger.info("ConnectionManager stopped.")

    def _get_channel_key(self, symbol: str) -> str:
        return f"live_ticks:{symbol}"

    async def _send_backfill_data(self, websocket: WebSocket, conn_info: ConnectionInfo) -> bool:
        """
        Sends backfill data and returns a boolean indicating success.
        Returns False if the client disconnects at any point.
        """
        logger.info(f"Attempting backfill for {conn_info.symbol}/{conn_info.interval} ({conn_info.data_type.value})")
        try:
            if websocket.client_state != WebSocketState.CONNECTED:
                logger.warning(f"Client disconnected before backfill could start for {conn_info.symbol}. Aborting.")
                return False

            cache_key = f"intraday_ticks:{conn_info.symbol}"
            cached_ticks_str = await self.redis_client.lrange(cache_key, 0, -1)
            
            if not cached_ticks_str:
                if websocket.client_state == WebSocketState.CONNECTED: await websocket.send_json([])
                return True

            ticks = [json.loads(t) for t in cached_ticks_str]
            if not ticks:
                if websocket.client_state == WebSocketState.CONNECTED: await websocket.send_json([])
                return True

            resampled_bars = await resample_ticks_to_bars(ticks, conn_info.interval, conn_info.timezone)

            if websocket.client_state != WebSocketState.CONNECTED:
                logger.warning(f"Client disconnected during backfill processing for {conn_info.symbol}. Aborting send.")
                return False

            final_bars = resampled_bars
            if conn_info.data_type == schemas.DataType.HEIKIN_ASHI:
                final_bars = calculate_historical_heikin_ashi(resampled_bars)

            if final_bars:
                payload = [bar.model_dump() for bar in final_bars]
                await websocket.send_json(payload)
                logger.info(f"Sent {len(payload)} backfilled bars to client for {conn_info.symbol}/{conn_info.interval}")
            else:
                await websocket.send_json([])

            return True

        except (WebSocketDisconnect, ConnectionClosed):
            logger.info(f"Could not send backfill to {conn_info.symbol}; client disconnected during the process.")
            return True
        
        except Exception as e:
            logger.error(f"An unexpected error occurred sending backfill data for {conn_info.symbol}: {e}", exc_info=True)
            return False


    async def add_connection(self, websocket: WebSocket, symbol: str, interval: str, timezone: str, data_type: schemas.DataType) -> bool:
        """
        Adds a new WebSocket connection. Returns False if the connection is terminated during setup.
        """
        conn_info = ConnectionInfo(websocket, symbol, interval, timezone, data_type)
        self.connections[websocket] = conn_info
        
        channel_key = self._get_channel_key(symbol)
        
        if channel_key not in self.subscription_groups:
            group = SubscriptionGroup(channel=channel_key, symbol=symbol)
            self.subscription_groups[channel_key] = group
            await self._start_redis_subscription(group)
        
        group = self.subscription_groups[channel_key]
        resampler_key = (interval, timezone)
        
        if resampler_key not in group.resamplers:
            resampler_class = TickBarResampler if 'tick' in interval else BarResampler
            group.resamplers[resampler_key] = resampler_class(interval, timezone)
            logger.info(f"Created new {resampler_class.__name__} for group {symbol}, key: {resampler_key}")

        if data_type == schemas.DataType.HEIKIN_ASHI and resampler_key not in group.heikin_ashi_calculators:
            group.heikin_ashi_calculators[resampler_key] = HeikinAshiLiveCalculator()
            logger.info(f"Created new HeikinAshiLiveCalculator for group {symbol}, key: {resampler_key}")

        # The backfill function now returns a status.
        backfill_successful = await self._send_backfill_data(websocket, conn_info)

        if backfill_successful and websocket.client_state == WebSocketState.CONNECTED:
            group.connections.add(websocket)
            logger.info(f"Connection {websocket.client.host}:{websocket.client.port} for {symbol}/{interval} is now live.")
            return True
        else:
            logger.warning(f"Did not add {symbol}/{interval} to live group; client disconnected during backfill.")
            return False

    async def remove_connection(self, websocket: WebSocket):
        if websocket not in self.connections: return
        conn_info = self.connections.pop(websocket)
        group = self.subscription_groups.get(self._get_channel_key(conn_info.symbol))
        if group:
            group.connections.discard(websocket)
            logger.info(f"Removed connection {websocket.client.host}:{websocket.client.port} from group {group.symbol}")


    async def _start_redis_subscription(self, group: SubscriptionGroup):
        pubsub = self.redis_client.pubsub()
        await pubsub.subscribe(group.channel)
        group.redis_subscription = pubsub
        group.message_task = asyncio.create_task(self._handle_redis_messages(group, pubsub))
        logger.info(f"Redis subscription task created for channel: {group.channel}")

    async def _handle_redis_messages(self, group: SubscriptionGroup, pubsub: aioredis.client.PubSub):
        """Listens for raw ticks and dispatches them for processing."""
        logger.info(f"STARTING Redis message listener for channel: {group.channel}")
        try:
            async for message in pubsub.listen():
                logger.debug(f"Received raw message from Redis on channel {group.channel}: {message}")
                if message['type'] == 'message':
                    tick_data = json.loads(message['data'])
                    await self._process_tick_for_group(group, tick_data)
        except asyncio.CancelledError:
             logger.warning(f"Redis message listener for {group.channel} was cancelled.")
        except Exception as e:
            logger.error(f"FATAL: Redis message listener for {group.channel} failed: {e}", exc_info=True)
        finally:
            logger.warning(f"STOPPED Redis message listener for channel: {group.channel}")

    async def _process_tick_for_group(self, group: SubscriptionGroup, tick_data: dict):
        """Processes a single raw tick, generates all required data types, and sends them to the correct clients."""
        if not group.connections: return
        
        payloads: Dict[tuple, dict] = {}

        for resampler_key, resampler in group.resamplers.items():
            try:
                completed_bar = resampler.add_bar(tick_data)
                current_bar = resampler.current_bar
                
                if isinstance(resampler, TickBarResampler):
                    data_type_key = schemas.DataType.TICK
                else:
                    data_type_key = schemas.DataType.REGULAR

                payloads[(data_type_key, *resampler_key)] = {
                    "completed_bar": completed_bar.model_dump() if completed_bar else None,
                    "current_bar": current_bar.model_dump() if current_bar else None
                }

                if resampler_key in group.heikin_ashi_calculators:
                    ha_calc = group.heikin_ashi_calculators[resampler_key]
                    completed_ha_bar, current_ha_bar = None, None
                    
                    if completed_bar:
                        ha_live_calc = group.heikin_ashi_calculators.get(resampler_key)
                        if ha_live_calc:
                             completed_ha_bar = ha_live_calc.calculate_next_completed(completed_bar)

                    if current_bar:
                        ha_live_calc = group.heikin_ashi_calculators.get(resampler_key)
                        if ha_live_calc:
                            current_ha_bar = ha_live_calc.calculate_current_bar(current_bar)
                    
                    payloads[(schemas.DataType.HEIKIN_ASHI, *resampler_key)] = {
                        "completed_bar": completed_ha_bar.model_dump() if completed_ha_bar else None,
                        "current_bar": current_ha_bar.model_dump() if current_ha_bar else None
                    }
            except Exception as e:
                logger.error(f"Error processing tick in resampler {resampler_key}: {e}", exc_info=True)
                continue


        tasks = []
        for websocket in list(group.connections):
            conn_info = self.connections.get(websocket)
            if not conn_info: continue
            
            payload_key = (conn_info.data_type, conn_info.interval, conn_info.timezone)
            if payload_key in payloads:
                tasks.append(websocket.send_json(payloads[payload_key]))
        
        if tasks:
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    if isinstance(result, (WebSocketDisconnect, ConnectionClosed)):
                         logger.warning(f"Failed to send update to a client, connection already closed.")
                    else:
                         logger.error(f"Error sending data to client: {result}", exc_info=False)


    async def _cleanup_loop(self):
        """Periodically cleans up subscription groups with no active connections."""
        while True:
            await asyncio.sleep(60)
            to_remove = [key for key, group in self.subscription_groups.items() if not group.connections]
            for key in to_remove:
                group = self.subscription_groups.pop(key)
                if group.message_task: group.message_task.cancel()
                if group.redis_subscription: await group.redis_subscription.unsubscribe()
                logger.info(f"Cleaned up unused subscription: {key}")

connection_manager = ConnectionManager()

async def startup_connection_manager():
    await connection_manager.start()

async def shutdown_connection_manager():
    await connection_manager.stop()
