from pydantic import BaseModel, Field
from datetime import datetime
from typing import List, Optional, Union
from enum import Enum

class DataType(str, Enum):
    """Enumeration of supported data types."""
    REGULAR = "regular"
    HEIKIN_ASHI = "heikin_ashi"
    TICK = "tick"

class Candle(BaseModel):
    """Represents a single regular OHLC candle."""
    open: float
    high: float
    low: float
    close: float
    volume: Optional[float] = None
    unix_timestamp: float
    timestamp: Optional[datetime] = Field(None, exclude=True)

class HeikinAshiCandle(BaseModel):
    """Represents a single Heikin Ashi candle."""
    open: float
    high: float
    low: float
    close: float
    volume: Optional[float] = None
    unix_timestamp: float

# --- Unified Response Schemas ---

class UnifiedDataResponse(BaseModel):
    """Unified response for initial historical data requests."""
    request_id: Optional[str] = Field(None, description="A cursor for fetching the next chunk.")
    candles: List[Union[Candle, HeikinAshiCandle]]
    is_partial: bool
    message: str

class UnifiedDataChunkResponse(BaseModel):
    """Unified response for subsequent data chunk requests."""
    request_id: Optional[str] = Field(None, description="The new cursor for the next page.")
    candles: List[Union[Candle, HeikinAshiCandle]]
    is_partial: bool

class SessionInfo(BaseModel):
    """Schema for session token."""
    session_token: str

class LiveDataMessage(BaseModel):
    """Unified message format for live WebSocket updates."""
    completed_bar: Optional[Union[Candle, HeikinAshiCandle]] = None
    current_bar: Optional[Union[Candle, HeikinAshiCandle]] = None