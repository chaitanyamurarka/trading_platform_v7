import logging
from datetime import datetime, timezone as dt_timezone
from typing import List, Optional
from fastapi import HTTPException

from .. import schemas
from ..cache import get_cached_ohlc_data, build_ohlc_cache_key, build_heikin_ashi_cache_key, get_cached_heikin_ashi_data, set_cached_heikin_ashi_data
from ..services import historical_data_service

def calculate_heikin_ashi_candles(regular_candles: List[schemas.Candle]) -> List[schemas.HeikinAshiCandle]:
    """
    Calculate Heikin Ashi candles from regular OHLC data.
    
    Heikin Ashi formulas:
    - HA Close = (O + H + L + C) / 4
    - HA Open = (Previous HA Open + Previous HA Close) / 2
    - HA High = Max(H, HA Open, HA Close)
    - HA Low = Min(L, HA Open, HA Close)
    """
    if not regular_candles:
        return []
    
    heikin_ashi_candles = []
    prev_ha_open = None
    prev_ha_close = None
    
    for i, candle in enumerate(regular_candles):
        # Calculate HA Close first
        ha_close = (candle.open + candle.high + candle.low + candle.close) / 4
        
        # Calculate HA Open
        if i == 0:
            # First candle: HA Open = (O + C) / 2
            ha_open = (candle.open + candle.close) / 2
        else:
            # Subsequent candles: HA Open = (Previous HA Open + Previous HA Close) / 2
            ha_open = (prev_ha_open + prev_ha_close) / 2
        
        # Calculate HA High and Low
        ha_high = max(candle.high, ha_open, ha_close)
        ha_low = min(candle.low, ha_open, ha_close)
        
        # Create Heikin Ashi candle
        ha_candle = schemas.HeikinAshiCandle(
            open=ha_open,
            high=ha_high,
            low=ha_low,
            close=ha_close,
            volume=candle.volume,
            unix_timestamp=candle.unix_timestamp,
            regular_open=candle.open,
            regular_close=candle.close
        )
        
        heikin_ashi_candles.append(ha_candle)
        
        # Store for next iteration
        prev_ha_open = ha_open
        prev_ha_close = ha_close
    
    return heikin_ashi_candles

def get_heikin_ashi_data(
    session_token: str,
    exchange: str,
    token: str,
    interval_val: str,
    start_time: datetime,
    end_time: datetime,
    timezone: str,
) -> schemas.HeikinAshiDataResponse:
    """
    Get Heikin Ashi data by first fetching regular OHLC data and then converting it.
    Enhanced with proper Heikin Ashi caching for chunk support.
    """
    try:
        # Build cache key for Heikin Ashi data
        ha_cache_key = build_heikin_ashi_cache_key(
            exchange=exchange,
            token=token,
            interval=interval_val,
            start_time=start_time, # Pass datetime object directly
            end_time=end_time,     # Pass datetime object directly
            timezone=timezone,
            session_token=session_token
        )
        
        # Check if we already have calculated HA data in cache
        cached_ha_data = get_cached_heikin_ashi_data(ha_cache_key)
        
        if cached_ha_data:
            logging.info(f"Cache HIT for Heikin Ashi data: {ha_cache_key}")
            heikin_ashi_candles = cached_ha_data
        else:
            logging.info(f"Cache MISS for Heikin Ashi data: {ha_cache_key}")
            
            # First get the regular OHLC data using existing service
            regular_response = historical_data_service.get_initial_historical_data(
                session_token=session_token,
                exchange=exchange,
                token=token,
                interval_val=interval_val,
                start_time=start_time,
                end_time=end_time,
                timezone=timezone
            )
            
            if not regular_response.candles:
                return schemas.HeikinAshiDataResponse(
                    candles=[],
                    total_available=0,
                    is_partial=False,
                    message="No regular OHLC data available to convert to Heikin Ashi",
                    request_id=None,
                    offset=None
                )
            
            # Get the full dataset from OHLC cache for complete HA calculation
            ohlc_cache_key = build_ohlc_cache_key(
                exchange=exchange,
                token=token,
                interval=interval_val,
                start_time=start_time, # Pass datetime object directly
                end_time=end_time,     # Pass datetime object directly
                timezone=timezone,
                session_token=session_token
            )
            
            full_regular_data = get_cached_ohlc_data(ohlc_cache_key)
            if not full_regular_data:
                # Fallback to using just the returned candles
                full_regular_data = regular_response.candles
            
            # Calculate Heikin Ashi candles from the full regular dataset
            heikin_ashi_candles = calculate_heikin_ashi_candles(full_regular_data)
            
            # Cache the calculated Heikin Ashi data for future chunk requests
            set_cached_heikin_ashi_data(ha_cache_key, heikin_ashi_candles)
            logging.info(f"Cache SET for Heikin Ashi data: {ha_cache_key} with {len(heikin_ashi_candles)} candles")
        
        # Apply the same pagination logic as regular candles
        total_available = len(heikin_ashi_candles)
        initial_fetch_limit = 5000
        initial_offset = max(0, total_available - initial_fetch_limit)
        candles_to_send = heikin_ashi_candles[initial_offset:]
        
        # Use the HA cache key as the request ID for chunk requests
        ha_request_id = ha_cache_key
        
        return schemas.HeikinAshiDataResponse(
            request_id=ha_request_id,
            candles=candles_to_send,
            offset=initial_offset,
            total_available=total_available,
            is_partial=(total_available > len(candles_to_send)),
            message=f"Heikin Ashi data loaded. Displaying last {len(candles_to_send)} of {total_available} candles."
        )
        
    except Exception as e:
        logging.error(f"Error generating Heikin Ashi data: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to generate Heikin Ashi data: {str(e)}")

def get_heikin_ashi_data_chunk(
    request_id: str,
    offset: int,
    limit: int = 5000
) -> schemas.HeikinAshiDataChunkResponse:
    """
    Retrieves a subsequent chunk of Heikin Ashi data from the cache using the request_id.
    """
    # The request_id is actually the HA cache key
    full_ha_data = get_cached_heikin_ashi_data(request_id)
    if full_ha_data is None:
        raise HTTPException(status_code=404, detail="Heikin Ashi data for this request not found or has expired.")

    total_available = len(full_ha_data)
    if offset < 0 or offset >= total_available:
        return schemas.HeikinAshiDataChunkResponse(
            candles=[], 
            offset=offset, 
            limit=limit, 
            total_available=total_available
        )
        
    chunk = full_ha_data[offset: offset + limit]
    
    return schemas.HeikinAshiDataChunkResponse(
        candles=chunk,
        offset=offset,
        limit=limit,
        total_available=total_available
    )

class HeikinAshiLiveCalculator:
    """
    Calculates live Heikin Ashi candles one at a time by maintaining the state
    of the previous HA open and close, which is essential for the calculation.
    """
    def __init__(self):
        self.prev_ha_open: Optional[float] = None
        self.prev_ha_close: Optional[float] = None

    def initialize_from_history(self, historical_ha_candles: List[schemas.HeikinAshiCandle]):
        """Initializes the state from a list of historical HA candles."""
        if historical_ha_candles:
            last_candle = historical_ha_candles[-1]
            self.prev_ha_open = last_candle.open
            self.prev_ha_close = last_candle.close
            logging.info(f"HA Live Calculator initialized. Prev HA Open: {self.prev_ha_open}, Prev HA Close: {self.prev_ha_close}")

    def _calculate_candle(self, regular_candle: schemas.Candle) -> schemas.HeikinAshiCandle:
        """Core calculation logic for converting a single regular candle to a Heikin Ashi candle."""
        # HA Close = (O + H + L + C) / 4
        ha_close = (regular_candle.open + regular_candle.high + regular_candle.low + regular_candle.close) / 4

        # HA Open = (Previous HA Open + Previous HA Close) / 2
        if self.prev_ha_open is None or self.prev_ha_close is None:
            # For the first candle, HA Open = (Regular O + Regular C) / 2
            ha_open = (regular_candle.open + regular_candle.close) / 2
        else:
            ha_open = (self.prev_ha_open + self.prev_ha_close) / 2

        # HA High = Max(H, HA Open, HA Close)
        ha_high = max(regular_candle.high, ha_open, ha_close)
        
        # HA Low = Min(L, HA Open, HA Close)
        ha_low = min(regular_candle.low, ha_open, ha_close)

        return schemas.HeikinAshiCandle(
            open=ha_open, high=ha_high, low=ha_low, close=ha_close,
            volume=regular_candle.volume,
            unix_timestamp=regular_candle.unix_timestamp,
            regular_open=regular_candle.open,
            regular_close=regular_candle.close
        )

    def calculate_next_completed(self, completed_regular_candle: schemas.Candle) -> schemas.HeikinAshiCandle:
        """
        Calculates the next COMPLETED Heikin Ashi candle and updates the internal
        state for the subsequent calculation.
        """
        ha_candle = self._calculate_candle(completed_regular_candle)
        # Update state for the next completed candle
        self.prev_ha_open = ha_candle.open
        self.prev_ha_close = ha_candle.close
        return ha_candle

    def calculate_current_bar(self, in_progress_regular_candle: schemas.Candle) -> schemas.HeikinAshiCandle:
        """
        Calculates the CURRENT, in-progress Heikin Ashi candle for live UI updates
        without modifying the state.
        """
        # This calculation uses the state from the *last completed* candle
        return self._calculate_candle(in_progress_regular_candle)