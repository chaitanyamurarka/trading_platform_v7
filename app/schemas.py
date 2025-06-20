"""
schemas.py

This module defines the Pydantic models that are used for data validation,
serialization, and defining the shapes of API requests and responses. These models
act as a clear and enforceable contract for the data moving through the application.
"""
from pydantic import BaseModel, Field, model_validator
from datetime import datetime
from typing import List, Dict, Any, Optional
from enum import Enum

class Interval(str, Enum):
    """Enumeration of allowed timeframe intervals for OHLC data."""
    SEC_1 = "1s"
    SEC_5 = "5s"
    SEC_10 = "10s"
    SEC_15 = "15s"
    SEC_30 = "30s"
    SEC_45 = "45s"
    MIN_1 = "1m"
    MIN_5 = "5m"
    MIN_10 = "10m"
    MIN_15 = "15m"
    MIN_30 = "30m"
    MIN_45 = "45m"
    HOUR_1 = "1h"
    DAY_1 = "1d"
    # NEW: Tick-based intervals
    TICK_1 = "1t"    # 1 tick per bar
    TICK_10 = "10t"  # 10 ticks per bar
    TICK_1000 = "1000t" # 1000 ticks per bar
    
class CandleBase(BaseModel):
    """
    Base schema for a single OHLC (Open, High, Low, Close) data point.
    The UNIX timestamp is now provided directly by the InfluxDB query.
    """
    # The datetime timestamp can be kept for logging/debugging or removed if not needed.
    open: float = Field(..., description="The opening price for the candle period.")
    high: float = Field(..., description="The highest price for the candle period.")
    low: float = Field(..., description="The lowest price for the candle period.")
    close: float = Field(..., description="The closing price for the candle period.")
    volume: Optional[float] = Field(None, description="The trading volume for the candle period.")
    unix_timestamp: float = Field(..., description="The timestamp represented as a UNIX epoch float.")
class Candle(CandleBase):
    """
    Represents a single OHLC candle, configured for ORM (Object-Relational Mapping) mode.
    This allows it to be created directly from a SQLAlchemy database object.
    """
    class Config:
        from_attributes = True  # Pydantic v2 setting for ORM mode.

class HistoricalDataResponse(BaseModel):
    """
    Defines the structured response for an initial historical data request.
    It includes the candle data plus metadata for pagination (lazy loading).
    """
    request_id: Optional[str] = Field(None, description="A unique ID for this data session, used for fetching subsequent chunks.")
    candles: List[Candle] = Field(description="The list of OHLC candle data.")
    offset: Optional[int] = Field(None, description="The starting offset of this chunk within the full dataset.")
    total_available: int = Field(description="The total number of candles available on the server for the requested range.")
    is_partial: bool = Field(description="True if the returned 'candles' are a subset of the total available.")
    message: str = Field(description="A descriptive message about the result of the data load.")

class HistoricalDataChunkResponse(BaseModel):
    """Defines the response for a subsequent chunk of historical data."""
    candles: List[Candle]
    offset: int
    limit: int
    total_available: int

class SessionInfo(BaseModel):
    """Schema for returning a new session token to the client."""
    session_token: str

class CandleType(str, Enum):
    """Enumeration of supported candle types."""
    REGULAR = "regular"
    HEIKIN_ASHI = "heikin_ashi"

class HeikinAshiCandle(BaseModel):
    """Schema for Heikin Ashi candle data."""
    open: float = Field(..., description="Heikin Ashi opening price")
    high: float = Field(..., description="Heikin Ashi highest price") 
    low: float = Field(..., description="Heikin Ashi lowest price")
    close: float = Field(..., description="Heikin Ashi closing price")
    volume: Optional[float] = Field(None, description="Trading volume")
    unix_timestamp: float = Field(..., description="UNIX timestamp")
    
    # Additional fields for debugging/analysis
    regular_open: Optional[float] = Field(None, description="Original OHLC open")
    regular_close: Optional[float] = Field(None, description="Original OHLC close")

class HeikinAshiDataResponse(BaseModel):
    """Response schema for Heikin Ashi data requests."""
    request_id: Optional[str] = Field(None, description="Unique ID for pagination")
    candles: List[HeikinAshiCandle] = Field(description="List of Heikin Ashi candles")
    offset: Optional[int] = Field(None, description="Starting offset of this chunk")
    total_available: int = Field(description="Total candles available")
    is_partial: bool = Field(description="True if this is a subset of total data")
    message: str = Field(description="Descriptive message about the result")

class HeikinAshiDataChunkResponse(BaseModel):
    """Response schema for Heikin Ashi data chunk requests (pagination)."""
    candles: List[HeikinAshiCandle] = Field(description="List of Heikin Ashi candles for this chunk")
    offset: int = Field(description="Starting offset of this chunk")
    limit: int = Field(description="Number of candles requested")
    total_available: int = Field(description="Total candles available")