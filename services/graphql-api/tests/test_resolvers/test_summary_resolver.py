"""Unit tests for DailySummaryResolver and MarketOverviewResolver."""

from __future__ import annotations

import pytest
from datetime import date
from unittest.mock import AsyncMock, MagicMock

from app.cache.memory_cache import MemoryCache
from app.db.iceberg_duckdb import IcebergDuckDB
from app.resolvers.daily_summary import DailySummaryResolver, _row_to_summary
from app.resolvers.market_overview import MarketOverviewResolver
from app.schema.inputs import DateRangeInput
from tests.conftest import SAMPLE_SUMMARY_ROW


@pytest.fixture
def engine():
    e = MagicMock(spec=IcebergDuckDB)
    e.execute = AsyncMock(return_value=[SAMPLE_SUMMARY_ROW])
    return e


@pytest.fixture
def cache():
    return MemoryCache()


def test_row_to_summary():
    summary = _row_to_summary(SAMPLE_SUMMARY_ROW)
    assert summary.symbol == "AAPL"
    assert summary.open_price == 148.0
    assert summary.close_price == 152.0
    assert summary.high_price == 153.5


def test_summary_computed_price_range():
    summary = _row_to_summary(SAMPLE_SUMMARY_ROW)
    assert summary.price_range() == pytest.approx(6.0)  # 153.5 - 147.5


def test_summary_computed_price_change():
    summary = _row_to_summary(SAMPLE_SUMMARY_ROW)
    assert summary.price_change() == pytest.approx(4.0)  # 152.0 - 148.0


def test_summary_price_change_pct():
    summary = _row_to_summary(SAMPLE_SUMMARY_ROW)
    expected = (152.0 - 148.0) / 148.0 * 100
    assert summary.price_change_pct() == pytest.approx(expected)


def test_summary_string_date():
    row = {**SAMPLE_SUMMARY_ROW, "trading_date": "2024-03-15"}
    summary = _row_to_summary(row)
    assert summary.trading_date == date(2024, 3, 15)


@pytest.mark.asyncio
async def test_daily_summary_resolver_returns_connection(engine, cache):
    resolver = DailySummaryResolver(engine, cache)
    dr = DateRangeInput(start=date(2024, 1, 1), end=date(2024, 3, 31))
    result = await resolver.resolve("AAPL", dr, first=10)
    assert result.total_count == 1
    assert len(result.edges) == 1
    assert result.edges[0].node.symbol == "AAPL"


@pytest.mark.asyncio
async def test_daily_summary_caching(engine, cache):
    resolver = DailySummaryResolver(engine, cache)
    dr = DateRangeInput(start=date(2024, 1, 1), end=date(2024, 3, 31))

    await resolver.resolve("AAPL", dr, first=10)
    call_count = engine.execute.call_count

    # Second call should hit cache
    await resolver.resolve("AAPL", dr, first=10)
    assert engine.execute.call_count == call_count  # no extra DB call


@pytest.mark.asyncio
async def test_market_overview_empty(cache):
    engine = MagicMock(spec=IcebergDuckDB)
    engine.execute = AsyncMock(return_value=[])
    resolver = MarketOverviewResolver(engine, cache)
    overview = await resolver.resolve(date(2024, 3, 15))
    assert overview.total_trades == 0
    assert overview.unique_symbols == 0


@pytest.mark.asyncio
async def test_market_overview_with_data(cache):
    engine = MagicMock(spec=IcebergDuckDB)
    # Two symbols: one advancing, one declining
    row2 = {**SAMPLE_SUMMARY_ROW, "symbol": "GOOG", "open_price": 160.0, "close_price": 155.0}
    engine.execute = AsyncMock(return_value=[SAMPLE_SUMMARY_ROW, row2])
    resolver = MarketOverviewResolver(engine, cache)
    overview = await resolver.resolve(date(2024, 3, 15))

    assert overview.unique_symbols == 2
    assert overview.advancing == 1
    assert overview.declining == 1
    assert overview.total_trades == SAMPLE_SUMMARY_ROW["trade_count"] + row2["trade_count"]


@pytest.mark.asyncio
async def test_market_overview_caches_result(cache):
    engine = MagicMock(spec=IcebergDuckDB)
    engine.execute = AsyncMock(return_value=[SAMPLE_SUMMARY_ROW])
    resolver = MarketOverviewResolver(engine, cache)

    from datetime import date as d
    target = d(2023, 1, 1)  # past date — long TTL
    await resolver.resolve(target)
    call_count = engine.execute.call_count
    await resolver.resolve(target)
    assert engine.execute.call_count == call_count  # served from cache
