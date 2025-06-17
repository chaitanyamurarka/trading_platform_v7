from fastapi import APIRouter, Query, HTTPException
from datetime import datetime

from .. import schemas
from .. import historical_data_service

router = APIRouter(
    prefix="/historical",
    tags=["Historical Data"]
)

@router.get("/", response_model=schemas.HistoricalDataResponse)
async def fetch_initial_historical_data(
    # REMOVED: The BackgroundTasks dependency is gone
    session_token: str = Query(..., description="The user's session token."),
    exchange: str = Query(..., description="Exchange name or code (e.g., 'NASDAQ')"),
    token: str = Query(..., description="Asset symbol or token (e.g., 'AAPL')"),
    interval: schemas.Interval = Query(..., description="Data interval (e.g., '1m', '5m', '1d')"),
    start_time: datetime = Query(..., description="Start datetime for the data range (ISO format)"),
    end_time: datetime = Query(..., description="End datetime for the data range (ISO format)"),
    timezone: str = Query("UTC", description="The timezone for the chart display (e.g., 'America/New_York')."),
):
    """
    Retrieve the initial chunk of historical OHLC data from the database.
    The server queries the entire range, caches it, and returns the most recent data.
    A 'request_id' is returned for fetching older chunks.
    """
    if start_time >= end_time:
        raise HTTPException(status_code=400, detail="start_time must be earlier than end_time")

    # The service function is now called with the timezone argument
    response = historical_data_service.get_initial_historical_data(
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
async def fetch_historical_data_chunk(
    request_id: str = Query(..., description="The unique ID of the data request session."),
    offset: int = Query(..., ge=0, description="The starting index of the data to fetch."),
    limit: int = Query(5000, ge=1, le=10000, description="The number of candles to fetch.")
):
    """
    Retrieve a subsequent chunk of historical OHLC data that has already been processed and cached.
    """
    # This function remains the same as it was already correct.
    response = historical_data_service.get_historical_data_chunk(
        request_id=request_id,
        offset=offset,
        limit=limit
    )
    return response