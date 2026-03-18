"""OHLCV candle resolver — aggregates Silver trades by configurable interval."""

from __future__ import annotations

from datetime import datetime
from typing import Optional

from app.cache.memory_cache import MemoryCache
from app.config import settings
from app.db.iceberg_duckdb import IcebergDuckDB
from app.schema.inputs import DateRangeInput
from app.schema.pagination import (
    MinuteCandleConnection,
    MinuteCandleEdge,
    PageInfo,
    decode_cursor,
    encode_cursor,
)
from app.schema.types import MinuteCandle

# Allowed intervals mapped to DuckDB time_bucket/date_trunc expressions.
# For sub-day: use time_bucket with INTERVAL.
# For day/week: use date_trunc.
INTERVAL_SQL = {
    "1m": "time_bucket(INTERVAL '1 minute', timestamp)",
    "5m": "time_bucket(INTERVAL '5 minutes', timestamp)",
    "15m": "time_bucket(INTERVAL '15 minutes', timestamp)",
    "30m": "time_bucket(INTERVAL '30 minutes', timestamp)",
    "1h": "time_bucket(INTERVAL '1 hour', timestamp)",
    "4h": "time_bucket(INTERVAL '4 hours', timestamp)",
    "1d": "date_trunc('day', timestamp)",
    "1w": "date_trunc('week', timestamp)",
}

VALID_INTERVALS = list(INTERVAL_SQL.keys())


def _row_to_candle(row: dict) -> MinuteCandle:
    ts = row["bucket_ts"]
    if isinstance(ts, str):
        ts = datetime.fromisoformat(ts)
    return MinuteCandle(
        symbol=row["symbol"],
        timestamp=ts,
        open_price=float(row["open_price"]),
        close_price=float(row["close_price"]),
        high_price=float(row["high_price"]),
        low_price=float(row["low_price"]),
        total_volume=int(row["total_volume"]),
        trade_count=int(row["trade_count"]),
    )


class MinuteCandleResolver:
    def __init__(self, engine: IcebergDuckDB, cache: MemoryCache) -> None:
        self.engine = engine
        self.cache = cache

    async def resolve(
        self,
        symbol: str,
        date_range: DateRangeInput,
        interval: str = "1m",
        first: int = 500,
        after: Optional[str] = None,
    ) -> MinuteCandleConnection:
        if interval not in INTERVAL_SQL:
            msg = f"Invalid interval '{interval}'. Must be one of: {', '.join(VALID_INTERVALS)}"
            raise ValueError(msg)

        offset = decode_cursor(after) if after else 0
        bucket_expr = INTERVAL_SQL[interval]
        cache_key = {
            "type": "candles",
            "symbol": symbol,
            "interval": interval,
            "start": str(date_range.start),
            "end": str(date_range.end),
        }
        cache_ns = "candles"

        cached = await self.cache.get(cache_ns, cache_key)
        if cached:
            all_rows = cached
        else:
            # bucket_expr is from a trusted allowlist, safe to interpolate
            all_rows = await self.engine.execute(
                namespace="silver",
                table="trades",
                sql=f"""
                    SELECT
                        symbol,
                        {bucket_expr} AS bucket_ts,
                        arg_min(price, timestamp) AS open_price,
                        arg_max(price, timestamp) AS close_price,
                        max(price) AS high_price,
                        min(price) AS low_price,
                        sum(quantity) AS total_volume,
                        count(*) AS trade_count
                    FROM t
                    WHERE symbol = ?
                      AND CAST(timestamp AS DATE) BETWEEN ? AND ?
                    GROUP BY symbol, {bucket_expr}
                    ORDER BY bucket_ts ASC
                """,
                params=[symbol, str(date_range.start), str(date_range.end)],
            )
            # Use the candle-specific TTL — shorter than daily_summary because
            # intraday candle data updates more frequently.
            await self.cache.set(cache_ns, cache_key, all_rows, settings.cache_ttl_candles)

        page = all_rows[offset : offset + first]
        has_next = len(all_rows) > offset + first
        edges = [MinuteCandleEdge(node=_row_to_candle(r), cursor=encode_cursor(offset + i)) for i, r in enumerate(page)]
        return MinuteCandleConnection(
            edges=edges,
            page_info=PageInfo(
                has_next_page=has_next,
                has_previous_page=offset > 0,
                start_cursor=edges[0].cursor if edges else None,
                end_cursor=edges[-1].cursor if edges else None,
            ),
            total_count=len(all_rows),
        )
