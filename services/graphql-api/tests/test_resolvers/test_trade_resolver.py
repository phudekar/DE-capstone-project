"""Unit tests for TradeResolver."""

from __future__ import annotations

import pytest
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, call

from app.db.iceberg_duckdb import IcebergDuckDB
from app.resolvers.trade import TradeResolver, _row_to_trade, _build_where
from app.schema.inputs import TradeFilterInput, DateRangeInput
from tests.conftest import SAMPLE_TRADE_ROW

COUNT_ROW = {"n": 1}


def _engine_mock(data_rows=None, count=1):
    """Return an engine mock that returns data_rows for data queries and count for count queries."""
    e = MagicMock(spec=IcebergDuckDB)
    data_rows = data_rows if data_rows is not None else [SAMPLE_TRADE_ROW]

    async def _side(namespace, table, sql, params=None, **kwargs):
        if "COUNT(*)" in sql:
            return [{"n": count}]
        return data_rows

    e.execute = AsyncMock(side_effect=_side)
    return e


@pytest.fixture
def engine():
    return _engine_mock()


@pytest.fixture
def resolver(engine):
    return TradeResolver(engine)


def test_row_to_trade_basic():
    trade = _row_to_trade(SAMPLE_TRADE_ROW)
    assert trade.trade_id == "trade-001"
    assert trade.symbol == "AAPL"
    assert trade.price == 150.25
    assert trade.quantity == 100
    assert trade.aggressor_side == "BUY"
    assert trade.company_name == "Apple Inc."


def test_row_to_trade_string_timestamp():
    row = {**SAMPLE_TRADE_ROW, "timestamp": "2024-03-15T10:30:00+00:00"}
    trade = _row_to_trade(row)
    assert isinstance(trade.timestamp, datetime)


def test_build_where_empty_filter():
    clause, params = _build_where(TradeFilterInput())
    assert "1=1" in clause
    assert params == []


def test_build_where_symbol():
    f = TradeFilterInput(symbol="AAPL")
    clause, params = _build_where(f)
    assert "symbol = ?" in clause
    assert "AAPL" in params


def test_build_where_multiple_symbols():
    f = TradeFilterInput(symbols=["AAPL", "GOOG"])
    clause, params = _build_where(f)
    assert "symbol IN" in clause
    assert "AAPL" in params
    assert "GOOG" in params


def test_build_where_price_range():
    f = TradeFilterInput(min_price=100.0, max_price=200.0)
    clause, params = _build_where(f)
    assert "price >= ?" in clause
    assert "price <= ?" in clause
    assert 100.0 in params
    assert 200.0 in params


def test_build_where_aggressor_side():
    f = TradeFilterInput(aggressor_side="SELL")
    clause, params = _build_where(f)
    assert "aggressor_side = ?" in clause
    assert "SELL" in params


def test_build_where_date_range():
    from datetime import date
    f = TradeFilterInput(date_range=DateRangeInput(start=date(2024, 1, 1), end=date(2024, 12, 31)))
    clause, params = _build_where(f)
    assert "BETWEEN" in clause


@pytest.mark.asyncio
async def test_resolve_by_id_found(resolver, engine):
    trade = await resolver.resolve_by_id("trade-001")
    assert trade is not None
    assert trade.trade_id == "trade-001"


@pytest.mark.asyncio
async def test_resolve_by_id_not_found():
    engine = _engine_mock(data_rows=[])
    resolver = TradeResolver(engine)
    trade = await resolver.resolve_by_id("nonexistent")
    assert trade is None


@pytest.mark.asyncio
async def test_resolve_returns_connection():
    engine = _engine_mock(data_rows=[SAMPLE_TRADE_ROW], count=1)
    resolver = TradeResolver(engine)
    result = await resolver.resolve(first=20)
    assert result.total_count == 1
    assert result.page_info is not None


@pytest.mark.asyncio
async def test_resolve_pagination_has_next():
    # Return first+1 rows to trigger has_next_page = True
    engine = _engine_mock(data_rows=[SAMPLE_TRADE_ROW] * 21, count=21)
    resolver = TradeResolver(engine)
    result = await resolver.resolve(first=20)
    assert result.page_info.has_next_page is True
    assert len(result.edges) == 20


@pytest.mark.asyncio
async def test_resolve_pagination_no_next():
    engine = _engine_mock(data_rows=[SAMPLE_TRADE_ROW] * 5, count=5)
    resolver = TradeResolver(engine)
    result = await resolver.resolve(first=20)
    assert result.page_info.has_next_page is False
    assert len(result.edges) == 5
