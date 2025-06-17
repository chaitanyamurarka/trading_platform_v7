# app/routers/heikin_ashi_live_router.py

import asyncio
import json
import logging
from fastapi import APIRouter, WebSocket, WebSocketDisconnect, Path
from websockets.exceptions import ConnectionClosed

from .. import live_data_service, heikin_ashi_service, schemas
from ..config import settings
import redis.asyncio as aioredis

router = APIRouter(
    prefix="/ws-ha",
    tags=["Live Heikin Ashi Data"]
)

logger = logging.getLogger(__name__)
redis_client = aioredis.from_url(settings.REDIS_URL, decode_responses=True)

@router.websocket("/live/{symbol}/{interval}/{timezone:path}")
async def get_live_heikin_ashi_data(
    websocket: WebSocket,
    symbol: str = Path(..., description="Asset symbol (e.g., 'AAPL')"),
    interval: str = Path(..., description="Data interval (e.g., '1m', '5m')"),
    timezone: str = Path(..., description="Client's IANA timezone (e.g., 'America/New_York')")
):
    """
    Provides historical and live Heikin Ashi data for a given symbol and interval.

    - **On Connect**: Sends a batch of historical Heikin Ashi data, calculated from
      the most recent regular intraday bars.
    - **Live Updates**: Streams live Heikin Ashi updates as they occur, calculating
      them from the real-time 1-second bar feed.
    """
    await websocket.accept()

    # Resampler for aggregating 1s bars into the requested interval for live data
    regular_resampler = live_data_service.BarResampler(interval, timezone)
    # State-aware calculator for converting regular candles to Heikin Ashi candles
    ha_calculator = heikin_ashi_service.HeikinAshiLiveCalculator()
    
    pubsub = None
    try:
        # --- 1. Fetch, Calculate, and Send Historical Data ---
        logger.info(f"[{symbol}] Getting cached intraday bars for HA backfill.")
        
        # Fetch raw 1-second bars from Redis cache
        cache_key = f"intraday_bars:{symbol}"
        cached_bars_str = await redis_client.lrange(cache_key, 0, -1)
        one_sec_bars = [json.loads(b) for b in cached_bars_str]
        logger.info(f"[{symbol}] Fetched {len(one_sec_bars)} 1-sec bars from cache for HA backfill.")

        # Resample the 1s bars into regular OHLC candles for the chosen interval
        backfill_resampler = live_data_service.BarResampler(interval, timezone)
        historical_regular_candles = []
        for bar in one_sec_bars:
            completed_bar = backfill_resampler.add_bar(bar)
            if completed_bar:
                historical_regular_candles.append(completed_bar)
        if backfill_resampler.current_bar:
             historical_regular_candles.append(backfill_resampler.current_bar)

        # Convert the resampled regular candles into a full set of historical Heikin Ashi candles
        historical_ha_candles = heikin_ashi_service.calculate_heikin_ashi_candles(historical_regular_candles)

        # Initialize the live calculator with the state from the historical data
        ha_calculator.initialize_from_history(historical_ha_candles)

        # Send the historical HA data to the client as the initial payload
        if historical_ha_candles:
            await websocket.send_json([bar.model_dump() for bar in historical_ha_candles])
            logger.info(f"[{symbol}] Sent {len(historical_ha_candles)} historical HA bars to client.")

        # --- 2. Subscribe to Live Data Stream ---
        channel_name = f"live_bars:{symbol}"
        pubsub = redis_client.pubsub()
        await pubsub.subscribe(channel_name)
        logger.info(f"[{symbol}] Subscribed to Redis channel: {channel_name} for live HA updates.")

        # --- 3. Process Live Messages ---
        while True:
            message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)

            if message:
                bar_message = json.loads(message['data'])
                # The resampler processes the 1s bar and returns a completed regular bar when the interval is met
                completed_regular_bar = regular_resampler.add_bar(bar_message)

                completed_ha_bar = None
                if completed_regular_bar:
                    # If we have a new completed regular bar, calculate the new completed HA bar
                    completed_ha_bar = ha_calculator.calculate_next_completed(completed_regular_bar)

                # Always calculate the current, in-progress bar for a smooth UI
                current_ha_bar = None
                if regular_resampler.current_bar:
                     current_ha_bar = ha_calculator.calculate_current_bar(regular_resampler.current_bar)

                # Send the payload with the completed bar (if any) and the current forming bar
                live_update_payload = {
                    "completed_bar": completed_ha_bar.model_dump() if completed_ha_bar else None,
                    "current_bar": current_ha_bar.model_dump() if current_ha_bar else None
                }
                await websocket.send_json(live_update_payload)
            
            await asyncio.sleep(0.01)

    except (WebSocketDisconnect, ConnectionClosed):
        logger.info(f"[{symbol}] Client disconnected from HA stream.")
    except Exception as e:
        logger.error(f"[{symbol}] An unexpected error occurred in the HA live stream: {e}", exc_info=True)
    finally:
        logger.info(f"[{symbol}] Cleaning up resources for HA stream...")
        if pubsub:
            await pubsub.unsubscribe()
            await pubsub.close()
        try:
            if websocket.client_state.name != "DISCONNECTED":
                await websocket.close()
        except Exception as e:
            logger.debug(f"[{symbol}] Error closing HA websocket: {e}")
        logger.info(f"[{symbol}] Resource cleanup complete for HA stream.")