import logging
from datetime import datetime, timezone as dt_timezone
from typing import List
from zoneinfo import ZoneInfo

from .. import schemas
from ..config import settings
from influxdb_client.client.influxdb_client_async import InfluxDBClientAsync

logger = logging.getLogger(__name__)

class QueryService:
    def __init__(self):
        self.influx_client = InfluxDBClientAsync(
            url=settings.INFLUX_URL,
            token=settings.INFLUX_TOKEN,
            org=settings.INFLUX_ORG,
            timeout=20_000
        )
        self.query_api = self.influx_client.query_api()

    async def close(self):
        await self.influx_client.close()

    async def query_candles(self, flux_query: str, timezone_str: str) -> List[schemas.Candle]:
        """Executes a Flux query and processes the results into Candle objects."""
        try:
            target_tz = ZoneInfo(timezone_str)
        except Exception:
            target_tz = ZoneInfo("UTC")
            logger.warning(f"Invalid timezone '{timezone_str}' provided. Defaulting to UTC.")
            
        tables = await self.query_api.query(query=flux_query)
        candles = []

        for table in tables:
            for record in table.records:
                utc_dt = record.get_time()
                local_dt = utc_dt.astimezone(target_tz)
                # Create a "fake" UTC datetime to get a UNIX timestamp that reflects the local time
                fake_utc_dt = datetime(
                    local_dt.year, local_dt.month, local_dt.day,
                    local_dt.hour, local_dt.minute, local_dt.second,
                    microsecond=local_dt.microsecond,
                    tzinfo=dt_timezone.utc
                )
                unix_timestamp_for_chart = fake_utc_dt.timestamp()

                candles.append(schemas.Candle(
                    timestamp=utc_dt,
                    open=record.values.get('open'),
                    high=record.values.get('high'),
                    low=record.values.get('low'),
                    close=record.values.get('close'),
                    volume=record.values.get('volume'),
                    unix_timestamp=unix_timestamp_for_chart
                ))
        return candles

# Singleton instance
query_service = QueryService()