# app/routers/data_router.py
from fastapi import APIRouter, Query, HTTPException, WebSocket, WebSocketDisconnect
from datetime import datetime
from .. import schemas
from ..services import data_service

router = APIRouter(
    prefix="/data",
    tags=["Unified Data API"]
)

@router.get("/historical", response_model=schemas.UnifiedDataResponse)
async def get_historical_data(
    session_token: str,
    exchange: str,
    token: str,
    interval: schemas.Interval,
    start_time: datetime,
    end_time: datetime,
    data_type: schemas.CandleType = Query(schemas.CandleType.REGULAR, description="The type of data to retrieve."),
    timezone: str = Query("UTC", description="The IANA timezone for the data range.")
):
    """
    Unified endpoint to retrieve historical data for various candle types (regular, Heikin Ashi, or tick-based).
    """
    if start_time >= end_time:
        raise HTTPException(status_code=400, detail="start_time must be earlier than end_time")

    # Delegate to the unified data service
    return await data_service.get_historical_data(
        session_token=session_token,
        exchange=exchange,
        token=token,
        interval=interval,
        start_time=start_time,
        end_time=end_time,
        data_type=data_type,
        timezone=timezone
    )

@router.get("/historical/chunk", response_model=schemas.UnifiedDataResponse)
async def get_historical_chunk(
    request_id: str = Query(..., description="The unique ID from the initial data request."),
    data_type: schemas.CandleType = Query(schemas.CandleType.REGULAR, description="The type of data for the chunk."),
    limit: int = Query(5000, ge=1, le=10000)
):
    """
    Unified endpoint to retrieve subsequent chunks of historical data using a request ID.
    """
    return await data_service.get_historical_chunk(
        request_id=request_id,
        data_type=data_type,
        limit=limit
    )

@router.websocket("/live/{symbol}/{interval}/{data_type}/{timezone:path}")
async def live_data_feed(
    websocket: WebSocket,
    symbol: str,
    interval: str,
    data_type: schemas.CandleType,
    timezone: str
):
    """
    Unified WebSocket for all live data streams, managed by a centralized connection pooler.
    """
    await data_service.handle_live_data_feed(
        websocket=websocket,
        symbol=symbol,
        interval=interval,
        data_type=data_type,
        timezone=timezone
    )

# --- Consolidated Utility Endpoints ---
@router.get("/session", response_model=schemas.SessionInfo)
async def get_session():
    """Initiates a new user session."""
    return data_service.initiate_session()

@router.post("/session/heartbeat", response_model=dict)
async def session_heartbeat(session: schemas.SessionInfo):
    """Keeps the user session alive."""
    return data_service.session_heartbeat(session)

@router.get("/symbols", response_model=list[str])
async def get_available_symbols():
    """Returns a list of available symbols."""
    return ["AAPL", "AMZN", "TSLA", "@NQ#"]

@router.get("/intervals", response_model=list[str])
async def get_available_intervals():
    """Returns a list of available intervals."""
    return [e.value for e in schemas.Interval]