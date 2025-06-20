from fastapi import APIRouter, Query, HTTPException
from datetime import datetime

from .. import schemas
from .. import tick_historical_data_service
from ..historical_data_service import get_historical_data_chunk # We can reuse the chunk service

router = APIRouter(
    prefix="/tick",
    tags=["Tick Data"]
)

@router.get("/", response_model=schemas.HistoricalDataResponse)
async def fetch_initial_tick_data(
    session_token: str = Query(..., description="The user's session token."),
    exchange: str = Query(..., description="Exchange name or code (e.g., 'NASDAQ')"),
    token: str = Query(..., description="Asset symbol or token (e.g., 'AAPL')"),
    interval: schemas.Interval = Query(..., description="Data interval (e.g., '1tick', '10tick')"),
    start_time: datetime = Query(..., description="Start datetime for the data range (ISO format)"),
    end_time: datetime = Query(..., description="End datetime for the data range (ISO format)"),
    timezone: str = Query("UTC", description="The timezone for the chart display."),
):
    """
    Retrieve the initial chunk of historical aggregated tick data from the database.
    """
    if start_time >= end_time:
        raise HTTPException(status_code=400, detail="start_time must be earlier than end_time")

    if not interval.value.endswith("tick"):
        raise HTTPException(status_code=400, detail="This endpoint only supports tick-based intervals.")

    response = tick_historical_data_service.get_initial_tick_data(
        session_token=session_token,
        exchange=exchange,
        token=token,
        interval_val=interval.value,
        start_time=start_time,
        end_time=end_time,
        timezone=timezone
    )
    return response

@router.get("/chunk", response_model=schemas.HistoricalDataChunkResponse)
async def fetch_tick_data_chunk(
    request_id: str = Query(..., description="The unique ID of the data request session."),
    offset: int = Query(..., ge=0, description="The starting index of the data to fetch."),
    limit: int = Query(5000, ge=1, le=10000, description="The number of bars to fetch.")
):
    """
    Retrieve a subsequent chunk of historical tick data that has already been processed and cached.
    This reuses the chunking logic from the main historical service.
    """
    response = get_historical_data_chunk(
        request_id=request_id,
        offset=offset,
        limit=limit
    )
    return response