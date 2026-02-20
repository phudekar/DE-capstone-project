"""Daily summary resolver — queries the Gold layer."""

from __future__ import annotations

from datetime import date
from typing import Optional

from app.cache.memory_cache import MemoryCache
from app.config import settings
from app.db.iceberg_duckdb import IcebergDuckDB
from app.schema.inputs import DateRangeInput
from app.schema.pagination import (
    DailySummaryConnection,
    DailySummaryEdge,
    PageInfo,
    encode_cursor,
    decode_cursor,
)
from app.schema.types import DailySummary


def _row_to_summary(row: dict) -> DailySummary:
    td = row["trading_date"]
    if isinstance(td, str):
        td = date.fromisoformat(td)
    return DailySummary(
        symbol=row["symbol"],
        trading_date=td,
        open_price=float(row["open_price"]),
        close_price=float(row["close_price"]),
        high_price=float(row["high_price"]),
        low_price=float(row["low_price"]),
        vwap=float(row["vwap"]),
        total_volume=int(row["total_volume"]),
        trade_count=int(row["trade_count"]),
        total_value=float(row["total_value"]),
        company_name=row.get("company_name"),
        sector=row.get("sector"),
    )


class DailySummaryResolver:
    def __init__(self, engine: IcebergDuckDB, cache: MemoryCache) -> None:
        self.engine = engine
        self.cache = cache

    async def resolve(
        self,
        symbol: str,
        date_range: DateRangeInput,
        first: int = 30,
        after: Optional[str] = None,
    ) -> DailySummaryConnection:
        offset = decode_cursor(after) if after else 0
        cache_key = {"symbol": symbol, "start": str(date_range.start), "end": str(date_range.end)}
        cache_ns = "daily_summary"

        cached = await self.cache.get(cache_ns, cache_key)
        if cached:
            all_rows = cached
        else:
            all_rows = await self.engine.execute(
                namespace="gold",
                table="daily_trading_summary",
                sql="""
                    SELECT symbol, trading_date, open_price, close_price,
                           high_price, low_price, vwap, total_volume,
                           trade_count, total_value, company_name, sector
                    FROM t
                    WHERE symbol = ?
                      AND trading_date BETWEEN ? AND ?
                    ORDER BY trading_date DESC
                """,
                params=[symbol, str(date_range.start), str(date_range.end)],
            )
            await self.cache.set(cache_ns, cache_key, all_rows, settings.cache_ttl_daily_summary)

        page = all_rows[offset: offset + first]
        has_next = len(all_rows) > offset + first
        edges = [
            DailySummaryEdge(
                node=_row_to_summary(r), cursor=encode_cursor(offset + i)
            )
            for i, r in enumerate(page)
        ]
        return DailySummaryConnection(
            edges=edges,
            page_info=PageInfo(
                has_next_page=has_next,
                has_previous_page=offset > 0,
                start_cursor=edges[0].cursor if edges else None,
                end_cursor=edges[-1].cursor if edges else None,
            ),
            total_count=len(all_rows),
        )
