from fastapi import APIRouter, Query, HTTPException
from datetime import datetime

from .. import schemas
from ..services import heikin_ashi_service

router = APIRouter(
    prefix="/heikin-ashi",
    tags=["Heikin Ashi Data"]
)

@router.get("/", response_model=schemas.HeikinAshiDataResponse)
async def fetch_heikin_ashi_data(
    session_token: str = Query(..., description="The user's session token"),
    exchange: str = Query(..., description="Exchange name (e.g., 'NASDAQ')"),
    token: str = Query(..., description="Asset symbol (e.g., 'AAPL')"),
    interval: schemas.Interval = Query(..., description="Data interval (e.g., '1m', '5m', '1d')"),
    start_time: datetime = Query(..., description="Start datetime (ISO format)"),
    end_time: datetime = Query(..., description="End datetime (ISO format)"),
    timezone: str = Query("UTC", description="Timezone for display (e.g., 'America/New_York')"),
):
    """
    Retrieve Heikin Ashi candle data calculated from regular OHLC data.
    """
    if start_time >= end_time:
        raise HTTPException(status_code=400, detail="start_time must be earlier than end_time")

    response = heikin_ashi_service.get_heikin_ashi_data(
        session_token=session_token,
        exchange=exchange,
        token=token,
        interval_val=interval.value,
        start_time=start_time,
        end_time=end_time,
        timezone=timezone
    )
    return response

# Add to app/routers/heikin_ashi_router.py
@router.get("/chunk", response_model=schemas.HeikinAshiDataChunkResponse)
async def fetch_heikin_ashi_data_chunk(
    request_id: str = Query(..., description="The unique ID of the Heikin Ashi data request session."),
    offset: int = Query(..., ge=0, description="The starting index of the data to fetch."),
    limit: int = Query(5000, ge=1, le=10000, description="The number of Heikin Ashi candles to fetch.")
):
    """
    Retrieve a subsequent chunk of Heikin Ashi data that has already been calculated and cached.
    """
    response = heikin_ashi_service.get_heikin_ashi_data_chunk(
        request_id=request_id,
        offset=offset,
        limit=limit
    )
    return response