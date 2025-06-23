from fastapi import APIRouter, Query, HTTPException, WebSocket, WebSocketDisconnect, Path
from datetime import datetime
import logging

from .. import schemas
from ..services import data_service
from ..websocket_manager import connection_manager

router = APIRouter(
    prefix="/data",
    tags=["Unified Data API"]
)

# --- HTTP Endpoints ---

@router.get("/historical", response_model=schemas.UnifiedDataResponse)
async def get_historical_data(
    session_token: str = Query(..., description="The user's session token."),
    exchange: str = Query(..., description="Exchange name (e.g., 'NASDAQ')"),
    token: str = Query(..., description="Asset symbol or token (e.g., 'AAPL')"),
    interval: str = Query(..., description="Data interval (e.g., '1m', '10tick')"),
    data_type: schemas.DataType = Query(..., description="Type of data to fetch (e.g., 'regular', 'heikin_ashi')"),
    start_time: datetime = Query(..., description="Start datetime for the data range (ISO format)"),
    end_time: datetime = Query(..., description="End datetime for the data range (ISO format)"),
    timezone: str = Query("UTC", description="The timezone for the chart display."),
):
    """
    Unified endpoint to retrieve the initial chunk of historical data for any data type.
    """
    if start_time >= end_time:
        raise HTTPException(status_code=400, detail="start_time must be earlier than end_time")

    response = await data_service.get_historical_data(
        session_token=session_token,
        exchange=exchange,
        token=token,
        interval=interval,
        data_type=data_type,
        start_time=start_time,
        end_time=end_time,
        timezone=timezone
    )
    return response

@router.get("/historical/chunk", response_model=schemas.UnifiedDataChunkResponse)
async def get_historical_chunk(
    request_id: str = Query(..., description="The cursor/ID for the data request session."),
    limit: int = Query(5000, ge=1, le=10000, description="The number of bars to fetch.")
):
    """
    Unified endpoint to retrieve a subsequent chunk of historical data using a cursor.
    The data_type is encoded within the request_id.
    """
    response = await data_service.get_historical_chunk(request_id=request_id, limit=limit)
    return response

@router.get("/session", response_model=schemas.SessionInfo)
async def initiate_session():
    """Initiates a new user session."""
    return data_service.initiate_session()

# --- WebSocket Endpoint ---

@router.websocket("/live/{data_type}/{symbol}/{interval}/{timezone:path}")
async def live_data_feed(
    websocket: WebSocket,
    data_type: schemas.DataType = Path(..., description="Type of data to stream."),
    symbol: str = Path(..., description="Asset symbol to subscribe to."),
    interval: str = Path(..., description="Data interval for aggregation."),
    timezone: str = Path(..., description="Client's IANA timezone for resampling.")
):
    """
    Unified WebSocket endpoint for all live data streams.
    Handles regular candles, Heikin Ashi, and tick-based aggregations.
    """
    await websocket.accept()
    
    try:
        # Add connection to the manager, which handles all pooling and subscriptions
        success = await connection_manager.add_connection(websocket, symbol, interval, timezone, data_type)
        if not success:
            await websocket.close(code=1011, reason="Failed to initialize connection")
            return

        # Send initial backfill data for the session
        await data_service.send_initial_live_data(websocket, symbol, interval, timezone, data_type)

        # Keep the connection alive; the manager will push updates
        while True:
            await websocket.receive_text() # Keep connection open, can be used for client heartbeats

    except WebSocketDisconnect:
        logging.info(f"WebSocket disconnected for {symbol}/{interval}/{data_type.value}")
    except Exception as e:
        logging.error(f"Error in WebSocket for {symbol}/{interval}/{data_type.value}: {e}", exc_info=True)
    finally:
        await connection_manager.remove_connection(websocket)