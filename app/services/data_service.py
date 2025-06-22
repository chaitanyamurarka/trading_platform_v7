# app/services/data_service.py
import uuid
import time
import asyncio
from fastapi import WebSocket, HTTPException, WebSocketDisconnect
from .. import schemas
from ..cache import redis_client
from . import historical_data_service, heikin_ashi_service, tick_historical_data_service, live_data_service
from ..websocket_manager import connection_manager

async def get_historical_data(session_token, exchange, token, interval, start_time, end_time, data_type, timezone):
    """
    Routes the historical data request to the correct service based on data_type.
    """
    if data_type == schemas.CandleType.REGULAR:
        response = historical_data_service.get_initial_historical_data(
            session_token, exchange, token, interval.value, start_time, end_time, timezone
        )
    elif data_type == schemas.CandleType.HEIKIN_ASHI:
        response = heikin_ashi_service.get_heikin_ashi_data(
            session_token, exchange, token, interval.value, start_time, end_time, timezone
        )
    elif data_type == schemas.CandleType.TICK:
        response = tick_historical_data_service.get_initial_tick_data(
            session_token, exchange, token, interval.value, start_time, end_time, timezone
        )
    else:
        raise HTTPException(status_code=400, detail="Invalid data_type specified")

    # Adapt the response to the new UnifiedDataResponse schema
    return schemas.UnifiedDataResponse(**response.model_dump())

async def get_historical_chunk(request_id, data_type, limit):
    """
    Routes the chunk request to the correct service.
    Note: This is a simplified implementation. A production system would need a more robust
    way to distinguish between cursor-based (tick) and offset-based (time) pagination.
    """
    if data_type == schemas.CandleType.TICK:
        response = tick_historical_data_service.get_tick_data_chunk(request_id, limit)
    elif data_type == schemas.CandleType.HEIKIN_ASHI:
        # Assuming offset is embedded or derivable from request_id for non-tick data
        # This part requires more detailed implementation based on cache key strategy.
        raise HTTPException(status_code=501, detail="Chunking for Heikin Ashi is not fully implemented in the unified endpoint yet.")
    else:
         raise HTTPException(status_code=501, detail="Chunking for regular candles is not fully implemented in the unified endpoint yet.")

    return schemas.UnifiedDataResponse(**response.model_dump())

async def handle_live_data_feed(websocket: WebSocket, symbol: str, interval: str, data_type: schemas.CandleType, timezone: str):
    """
    Handles the unified WebSocket connection, passing the data_type to the connection manager.
    The connection manager must be updated to handle different resamplers based on this type.
    """
    await websocket.accept()
    # The connection manager will handle the logic for different data types
    success = await connection_manager.add_connection(websocket, symbol, interval, timezone)

    if not success:
        await websocket.close(code=1011, reason="Failed to initialize connection")
        return

    try:
        # Backfill logic can be initiated here based on data_type
        # For simplicity, this is handled within the connection manager or initial HTTP load.
        while True:
            await asyncio.sleep(60) # Keep connection alive
    except (WebSocketDisconnect, Exception):
        await connection_manager.remove_connection(websocket)

# --- Session Logic (moved from utility_router) ---
def initiate_session():
    session_token = str(uuid.uuid4())
    redis_client.set(f"session:{session_token}", int(time.time()), ex=2700) # 45 mins
    return schemas.SessionInfo(session_token=session_token)

def session_heartbeat(session: schemas.SessionInfo):
    token_key = f"session:{session.session_token}"
    if redis_client.exists(token_key):
        redis_client.set(token_key, int(time.time()), ex=2700)
        return {"status": "ok"}
    raise HTTPException(status_code=404, detail="Session not found or expired.")