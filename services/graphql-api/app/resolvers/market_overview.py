"""Market overview resolver — aggregates Gold layer data for a given date."""

from __future__ import annotations

from datetime import date

from app.cache.memory_cache import MemoryCache
from app.config import settings
from app.db.iceberg_duckdb import IcebergDuckDB
from app.resolvers.daily_summary import _row_to_summary
from app.schema.types import MarketOverview, DailySummary


class MarketOverviewResolver:
    def __init__(self, engine: IcebergDuckDB, cache: MemoryCache) -> None:
        self.engine = engine
        self.cache = cache

    async def resolve(self, target_date: date) -> MarketOverview:
        cache_ns = "market_overview"
        cache_key = {"date": str(target_date)}
        ttl = (
            settings.cache_ttl_market_overview
            if target_date == date.today()
            else 86400
        )

        cached = await self.cache.get(cache_ns, cache_key)
        if cached:
            return cached

        rows = await self.engine.execute(
            namespace="gold",
            table="daily_trading_summary",
            sql="""
                SELECT symbol, trading_date, open_price, close_price,
                       high_price, low_price, vwap, total_volume,
                       trade_count, total_value, company_name, sector
                FROM t
                WHERE trading_date = ?
                ORDER BY total_volume DESC
            """,
            params=[str(target_date)],
        )

        if not rows:
            overview = MarketOverview(
                trading_date=target_date,
                total_trades=0,
                total_volume=0,
                total_value=0.0,
                unique_symbols=0,
                advancing=0,
                declining=0,
                unchanged=0,
                top_gainers=[],
                top_losers=[],
                most_active=[],
            )
            await self.cache.set(cache_ns, cache_key, overview, ttl)
            return overview

        summaries = [_row_to_summary(r) for r in rows]

        advancing = sum(1 for s in summaries if s.price_change() > 0)
        declining = sum(1 for s in summaries if s.price_change() < 0)
        unchanged = len(summaries) - advancing - declining

        sorted_by_change = sorted(summaries, key=lambda s: s.price_change_pct(), reverse=True)
        top_gainers = sorted_by_change[:5]
        top_losers = sorted_by_change[-5:][::-1]
        most_active = sorted(summaries, key=lambda s: s.total_volume, reverse=True)[:5]

        overview = MarketOverview(
            trading_date=target_date,
            total_trades=sum(s.trade_count for s in summaries),
            total_volume=sum(s.total_volume for s in summaries),
            total_value=sum(s.total_value for s in summaries),
            unique_symbols=len(summaries),
            advancing=advancing,
            declining=declining,
            unchanged=unchanged,
            top_gainers=top_gainers,
            top_losers=top_losers,
            most_active=most_active,
        )
        await self.cache.set(cache_ns, cache_key, overview, ttl)
        return overview
