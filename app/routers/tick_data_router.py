from fastapi import APIRouter, Query, HTTPException
from datetime import datetime
from typing import Optional

from .. import schemas
from .. import tick_historical_data_service

router = APIRouter(
    prefix="/tick",
    tags=["Tick Data"]
)

@router.get("/", response_model=schemas.TickDataResponse)
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
    Uses cursor-based pagination.
    """
    if start_time >= end_time:
        raise HTTPException(status_code=400, detail="start_time must be earlier than end_time")

    if "tick" not in interval.value:
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

@router.get("/chunk", response_model=schemas.TickDataChunkResponse)
async def fetch_tick_data_chunk(
    request_id: str = Query(..., description="The cursor for the data request session."),
    limit: int = Query(5000, ge=1, le=10000, description="The number of bars to fetch.")
    # The 'offset' parameter from the frontend will be ignored, as we are using a cursor.
):
    """
    Retrieve a subsequent chunk of historical tick data using a cursor.
    """
    response = tick_historical_data_service.get_tick_data_chunk(
        request_id=request_id,
        limit=limit
    )
    return response