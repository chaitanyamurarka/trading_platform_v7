import logging
from typing import List, Optional
from .. import schemas

logger = logging.getLogger(__name__)

def calculate_historical_heikin_ashi(regular_candles: List[schemas.Candle]) -> List[schemas.HeikinAshiCandle]:
    """Calculates a full series of Heikin Ashi candles from regular OHLC data."""
    if not regular_candles:
        return []
    
    ha_candles = []
    prev_ha_open, prev_ha_close = None, None
    
    for i, candle in enumerate(regular_candles):
        ha_close = (candle.open + candle.high + candle.low + candle.close) / 4
        ha_open = ((prev_ha_open + prev_ha_close) / 2) if i > 0 else ((candle.open + candle.close) / 2)
        ha_high = max(candle.high, ha_open, ha_close)
        ha_low = min(candle.low, ha_open, ha_close)
        
        ha_candle = schemas.HeikinAshiCandle(
            open=ha_open, high=ha_high, low=ha_low, close=ha_close,
            volume=candle.volume, unix_timestamp=candle.unix_timestamp
        )
        ha_candles.append(ha_candle)
        prev_ha_open, prev_ha_close = ha_open, ha_close
    
    return ha_candles

class HeikinAshiLiveCalculator:
    """Calculates live Heikin Ashi candles one at a time by maintaining state."""
    def __init__(self):
        self.prev_ha_open: Optional[float] = None
        self.prev_ha_close: Optional[float] = None

    def initialize_from_history(self, historical_ha_candles: List[schemas.HeikinAshiCandle]):
        """Initializes state from historical HA candles."""
        if historical_ha_candles:
            last_candle = historical_ha_candles[-1]
            self.prev_ha_open = last_candle.open
            self.prev_ha_close = last_candle.close
            logger.info(f"HA Live Calculator initialized. Prev HA Open: {self.prev_ha_open}, Prev HA Close: {self.prev_ha_close}")

    def _calculate_candle(self, regular_candle: schemas.Candle) -> schemas.HeikinAshiCandle:
        """Core logic for converting a single regular candle to a Heikin Ashi candle."""
        ha_close = (regular_candle.open + regular_candle.high + regular_candle.low + regular_candle.close) / 4
        ha_open = ((self.prev_ha_open + self.prev_ha_close) / 2) if self.prev_ha_open is not None else ((regular_candle.open + regular_candle.close) / 2)
        ha_high = max(regular_candle.high, ha_open, ha_close)
        ha_low = min(regular_candle.low, ha_open, ha_close)

        return schemas.HeikinAshiCandle(
            open=ha_open, high=ha_high, low=ha_low, close=ha_close,
            volume=regular_candle.volume, unix_timestamp=regular_candle.unix_timestamp
        )

    def calculate_next_completed(self, completed_regular_candle: schemas.Candle) -> schemas.HeikinAshiCandle:
        """Calculates the next COMPLETED Heikin Ashi candle and updates internal state."""
        ha_candle = self._calculate_candle(completed_regular_candle)
        self.prev_ha_open, self.prev_ha_close = ha_candle.open, ha_candle.close
        return ha_candle

    def calculate_current_bar(self, in_progress_regular_candle: schemas.Candle) -> schemas.HeikinAshiCandle:
        """Calculates the CURRENT, in-progress Heikin Ashi candle without modifying state."""
        return self._calculate_candle(in_progress_regular_candle)