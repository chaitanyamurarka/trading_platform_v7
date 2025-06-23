# chaitanyamurarka/trading_platform_v7/trading_platform_v7-b416586615a6242166ca4cc60bdb75122687b219/app/websocket_manager.py
import asyncio
import json
import logging
from typing import Dict, Set, Optional, Any, Union
from dataclasses import dataclass, field
from datetime import datetime
import redis.asyncio as aioredis
from starlette.websockets import WebSocket
from websockets.exceptions import ConnectionClosed

from .config import settings
from . import schemas
# Correctly import the resampler classes to check their type
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
    data_type: schemas.DataType # NEW: Track what kind of data the client wants
    connected_at: datetime = field(default_factory=datetime.now)

@dataclass
class SubscriptionGroup:
    """Manages a group of connections for the same Redis channel, with resamplers for different data types."""
    channel: str
    symbol: str
    connections: Set[WebSocket] = field(default_factory=set)
    # Stateful resamplers for regular OHLC, keyed by (interval, timezone)
    resamplers: Dict[tuple[str, str], Union[BarResampler, TickBarResampler]] = field(default_factory=dict)
    # NEW: Stateful calculators for Heikin Ashi, keyed by (interval, timezone)
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

    async def _send_backfill_data(self, websocket: WebSocket, conn_info: ConnectionInfo):
        """Sends a backfill of recent data from cache to a new client by resampling raw ticks."""
        logger.info(f"Attempting backfill for {conn_info.symbol}/{conn_info.interval} ({conn_info.data_type.value})")
        try:
            # 1. Fetch raw ticks from the Redis cache populated by the ingestor
            cache_key = f"intraday_ticks:{conn_info.symbol}"
            cached_ticks_str = await self.redis_client.lrange(cache_key, 0, -1)
            
            if not cached_ticks_str:
                logger.warning(f"No backfill tick data found in Redis cache for {conn_info.symbol}.")
                await websocket.send_json([]) # Send empty array to confirm connection
                return

            ticks = [json.loads(t) for t in cached_ticks_str]
            if not ticks:
                logger.warning(f"Backfill tick data for {conn_info.symbol} was empty after JSON parsing.")
                await websocket.send_json([])
                return

            # 2. Resample the raw ticks into the client's requested interval
            resampled_bars = resample_ticks_to_bars(
                ticks, conn_info.interval, conn_info.timezone
            )
            
            # Note: resample_ticks_to_bars can return an empty list if not enough ticks exist to form a bar.
            # This is expected behavior.

            final_bars = resampled_bars
            # 3. If Heikin Ashi is requested, convert the resampled bars
            if conn_info.data_type == schemas.DataType.HEIKIN_ASHI:
                final_bars = calculate_historical_heikin_ashi(resampled_bars)

            # 4. Send the final list of bars to the client
            if final_bars:
                payload = [bar.model_dump() for bar in final_bars]
                await websocket.send_json(payload)
                logger.info(f"Sent {len(payload)} backfilled bars to client for {conn_info.symbol}/{conn_info.interval}")
            else:
                logger.info(f"No bars to send for backfill for {conn_info.symbol}/{conn_info.interval}, sending empty list.")
                await websocket.send_json([])


        except Exception as e:
            logger.error(f"Error sending backfill data for {conn_info.symbol}: {e}", exc_info=True)


    async def add_connection(self, websocket: WebSocket, symbol: str, interval: str, timezone: str, data_type: schemas.DataType):
        """
        Adds a new WebSocket connection, sends backfill data, and sets up live processing.
        --- MODIFIED: Starts the live listener before sending backfill to prevent race conditions. ---
        """
        conn_info = ConnectionInfo(websocket, symbol, interval, timezone, data_type)
        self.connections[websocket] = conn_info
        
        channel_key = self._get_channel_key(symbol)
        
        # --- FIX: Ensure subscription group and listener are running BEFORE backfill ---
        if channel_key not in self.subscription_groups:
            # Create a new group if it's the first connection for this symbol
            group = SubscriptionGroup(channel=channel_key, symbol=symbol)
            self.subscription_groups[channel_key] = group
            # Start the persistent Redis listener task for this new group
            await self._start_redis_subscription(group)
        
        group = self.subscription_groups[channel_key]
        group.connections.add(websocket)
        
        resampler_key = (interval, timezone)
        
        # Create a new resampler for this specific interval/timezone if it doesn't exist
        if resampler_key not in group.resamplers:
            resampler = TickBarResampler(interval, timezone) if 'tick' in interval else BarResampler(interval, timezone)
            group.resamplers[resampler_key] = resampler
            logger.info(f"Created new {type(resampler).__name__} for group {symbol}, key: {resampler_key}")

        # Create a Heikin Ashi calculator if requested and not already present
        if data_type == schemas.DataType.HEIKIN_ASHI and resampler_key not in group.heikin_ashi_calculators:
            group.heikin_ashi_calculators[resampler_key] = HeikinAshiLiveCalculator()
            logger.info(f"Created new HeikinAshiLiveCalculator for group {symbol}, key: {resampler_key}")
            
        # Now that all live listeners and resamplers are confirmed to be in place, send historical data.
        await self._send_backfill_data(websocket, conn_info)
        # --- END FIX ---


    async def remove_connection(self, websocket: WebSocket):
        if websocket not in self.connections: return
        conn_info = self.connections.pop(websocket)
        group = self.subscription_groups.get(self._get_channel_key(conn_info.symbol))
        if group:
            group.connections.discard(websocket)


    async def _start_redis_subscription(self, group: SubscriptionGroup):
        pubsub = self.redis_client.pubsub()
        await pubsub.subscribe(group.channel)
        group.redis_subscription = pubsub
        # --- MODIFIED: Pass the pubsub object directly to the handler ---
        group.message_task = asyncio.create_task(self._handle_redis_messages(group, pubsub))
        logger.info(f"Redis subscription task created for channel: {group.channel}")

    async def _handle_redis_messages(self, group: SubscriptionGroup, pubsub: aioredis.client.PubSub):
        """
        Listens for raw ticks and dispatches them for processing.
        --- MODIFIED: Added crucial logging for diagnostics. ---
        """
        logger.info(f"STARTING Redis message listener for channel: {group.channel}")
        try:
            async for message in pubsub.listen():
                # This log is essential to confirm Redis messages are being received at all
                logger.debug(f"Received raw message from Redis on channel {group.channel}: {message}")
                if message['type'] == 'message':
                    tick_data = json.loads(message['data'])
                    await self._process_tick_for_group(group, tick_data)
        except asyncio.CancelledError:
             logger.warning(f"Redis message listener for {group.channel} was cancelled.")
        except Exception as e:
            # This will catch errors during connection or message processing
            logger.error(f"FATAL: Redis message listener for {group.channel} failed: {e}", exc_info=True)
        finally:
            # This log will show if the listener loop ever exits for any reason
            logger.warning(f"STOPPED Redis message listener for channel: {group.channel}")

    async def _process_tick_for_group(self, group: SubscriptionGroup, tick_data: dict):
        """Processes a single raw tick, generates all required data types, and sends them to the correct clients."""
        if not group.connections: return
        
        payloads: Dict[tuple, dict] = {}

        for resampler_key, resampler in group.resamplers.items():
            try:
                completed_bar = resampler.add_bar(tick_data)
                current_bar = resampler.current_bar
                
                # ========================= FIX START =========================
                # Determine the correct data type based on the resampler instance.
                # This ensures that tick data is keyed correctly for tick-based clients.
                if isinstance(resampler, TickBarResampler):
                    data_type_key = schemas.DataType.TICK
                else:
                    data_type_key = schemas.DataType.REGULAR

                payloads[(data_type_key, *resampler_key)] = {
                    "completed_bar": completed_bar.model_dump() if completed_bar else None,
                    "current_bar": current_bar.model_dump() if current_bar else None
                }
                # ========================== FIX END ==========================

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
                # Continue to next resampler
                continue


        tasks = []
        for websocket in list(group.connections):
            conn_info = self.connections.get(websocket)
            if not conn_info: continue
            
            # This lookup will now succeed for tick-based connections
            payload_key = (conn_info.data_type, conn_info.interval, conn_info.timezone)
            if payload_key in payloads:
                tasks.append(websocket.send_json(payloads[payload_key]))
        
        if tasks:
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for result in results:
                if isinstance(result, Exception):
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