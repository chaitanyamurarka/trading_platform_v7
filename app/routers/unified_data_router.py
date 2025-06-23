import logging
from fastapi import APIRouter, Query, HTTPException, WebSocket, WebSocketDisconnect, Path
from datetime import datetime

from .. import schemas
from ..services import historical_service, session_service
from ..websocket_manager import connection_manager

logger = logging.getLogger(__name__)
router = APIRouter()

# --- FAKE /historical/ Endpoints ---

@router.get("/historical/", response_model=schemas.HistoricalDataResponse, tags=["Fake Legacy Routes"])
async def fetch_initial_historical_data_fake(
    session_token: str = Query(...,), exchange: str = Query(...,), token: str = Query(...,),
    interval: schemas.Interval = Query(...,), start_time: datetime = Query(...,),
    end_time: datetime = Query(...,), timezone: str = Query("UTC",),
):
    if start_time >= end_time:
        raise HTTPException(status_code=400, detail="start_time must be earlier than end_time")
    
    return historical_service.get_historical_data(
        session_token=session_token, exchange=exchange, token=token, interval_val=interval.value,
        start_time=start_time, end_time=end_time, timezone=timezone, data_type=schemas.DataType.REGULAR
    )

@router.get("/historical/chunk", response_model=schemas.HistoricalDataChunkResponse, tags=["Fake Legacy Routes"])
async def fetch_historical_data_chunk_fake(
    request_id: str = Query(...,), offset: int = Query(..., ge=0), limit: int = Query(5000, ge=1, le=10000),
):
    return historical_service.get_historical_chunk(
        request_id=request_id, offset=offset, limit=limit, data_type=schemas.DataType.REGULAR
    )

# --- FAKE /heikin-ashi/ Endpoints ---

@router.get("/heikin-ashi/", response_model=schemas.HeikinAshiDataResponse, tags=["Fake Legacy Routes"])
async def fetch_heikin_ashi_data_fake(
    session_token: str = Query(...,), exchange: str = Query(...,), token: str = Query(...,),
    interval: schemas.Interval = Query(...,), start_time: datetime = Query(...,),
    end_time: datetime = Query(...,), timezone: str = Query("UTC",),
):
    if start_time >= end_time:
        raise HTTPException(status_code=400, detail="start_time must be earlier than end_time")
        
    return historical_service.get_historical_data(
        session_token=session_token, exchange=exchange, token=token, interval_val=interval.value,
        start_time=start_time, end_time=end_time, timezone=timezone, data_type=schemas.DataType.HEIKIN_ASHI
    )

@router.get("/heikin-ashi/chunk", response_model=schemas.HeikinAshiDataChunkResponse, tags=["Fake Legacy Routes"])
async def fetch_heikin_ashi_data_chunk_fake(
    request_id: str = Query(...,), offset: int = Query(..., ge=0), limit: int = Query(5000, ge=1, le=10000),
):
    return historical_service.get_historical_chunk(
        request_id=request_id, offset=offset, limit=limit, data_type=schemas.DataType.HEIKIN_ASHI
    )

# --- FAKE /tick/ Endpoints ---

@router.get("/tick/", response_model=schemas.TickDataResponse, tags=["Fake Legacy Routes"])
async def fetch_initial_tick_data_fake(
    session_token: str = Query(...,), exchange: str = Query(...,), token: str = Query(...,),
    interval: schemas.Interval = Query(...,), start_time: datetime = Query(...,),
    end_time: datetime = Query(...,), timezone: str = Query("UTC",),
):
    if start_time >= end_time:
        raise HTTPException(status_code=400, detail="start_time must be earlier than end_time")
    if "tick" not in interval.value:
        raise HTTPException(status_code=400, detail="This endpoint only supports tick-based intervals.")
        
    return historical_service.get_historical_data(
        session_token=session_token, exchange=exchange, token=token, interval_val=interval.value,
        start_time=start_time, end_time=end_time, timezone=timezone, data_type=schemas.DataType.TICK
    )

@router.get("/tick/chunk", response_model=schemas.TickDataChunkResponse, tags=["Fake Legacy Routes"])
async def fetch_tick_data_chunk_fake(
    request_id: str = Query(...,), limit: int = Query(5000, ge=1, le=10000),
):
    return historical_service.get_historical_chunk(
        request_id=request_id, offset=None, limit=limit, data_type=schemas.DataType.TICK
    )

# --- FAKE /utils/session/ Endpoints ---

@router.get("/utils/session/initiate", response_model=schemas.SessionInfo, tags=["Fake Legacy Routes"])
def initiate_session_fake():
    return session_service.initiate_session()

@router.post("/utils/session/heartbeat", response_model=dict, tags=["Fake Legacy Routes"])
def session_heartbeat_fake(session: schemas.SessionInfo):
    return session_service.process_heartbeat(session)

# --- FAKE WebSocket Endpoints ---

async def websocket_handler(websocket: WebSocket, symbol: str, interval: str, timezone: str, data_type: schemas.DataType):
    """Generic handler for all live data websockets."""
    await websocket.accept()
    try:
        await connection_manager.add_connection(websocket, symbol, interval, timezone, data_type)
        # Keep the connection alive; the manager will push updates.
        while True:
            await websocket.receive_text() # Or use a sleep loop if no client messages are expected
    except WebSocketDisconnect:
        logger.info(f"Client disconnected: {symbol}/{interval}/{data_type.value}")
    except Exception as e:
        logger.error(f"Error in websocket handler for {symbol}: {e}", exc_info=True)
    finally:
        await connection_manager.remove_connection(websocket)
        logger.info(f"Cleaned up connection for: {symbol}/{interval}/{data_type.value}")

@router.websocket("/ws/live/{symbol}/{interval}/{timezone:path}", name="Live Regular/Tick Data")
async def get_live_data_fake(
    websocket: WebSocket, symbol: str = Path(...), interval: str = Path(...), timezone: str = Path(...)
):
    data_type = schemas.DataType.TICK if 'tick' in interval else schemas.DataType.REGULAR
    await websocket_handler(websocket, symbol, interval, timezone, data_type)

@router.websocket("/ws-ha/live/{symbol}/{interval}/{timezone:path}", name="Live Heikin Ashi Data")
async def get_live_heikin_ashi_data_fake(
    websocket: WebSocket, symbol: str = Path(...), interval: str = Path(...), timezone: str = Path(...)
):
    await websocket_handler(websocket, symbol, interval, timezone, schemas.DataType.HEIKIN_ASHI)