"""Integration-style tests that execute GraphQL queries against the schema with mock data."""

from __future__ import annotations

import pytest
from datetime import date, datetime, timezone
from unittest.mock import AsyncMock, MagicMock

from strawberry.dataloader import DataLoader

from app.schema import schema
from app.auth.models import ANONYMOUS
from app.cache.memory_cache import MemoryCache
from app.context import GraphQLContext
from app.db.iceberg_duckdb import IcebergDuckDB
from app.services.watchlist_service import WatchlistService
from app.streaming.kafka_consumer import KafkaConsumerFactory
from tests.conftest import SAMPLE_TRADE_ROW, SAMPLE_SUMMARY_ROW, SAMPLE_SYMBOL_ROW


def _make_context(trade_rows=None, summary_rows=None, symbol_rows=None, user=None):
    engine = MagicMock(spec=IcebergDuckDB)

    async def _side_effect(namespace, table, sql, params=None, **kwargs):
        if "COUNT(*)" in sql:
            return [{"n": len(trade_rows or [])}]
        if table == "trades":
            return trade_rows or []
        if table == "daily_trading_summary":
            return summary_rows or []
        if table == "dim_symbol":
            return symbol_rows or []
        if table == "orderbook_snapshots":
            return []
        return []

    engine.execute = AsyncMock(side_effect=_side_effect)

    async def _noop(keys):
        sym_map = {r["symbol"]: r for r in (symbol_rows or [])}
        return [sym_map.get(k) for k in keys]

    return GraphQLContext(
        engine=engine,
        cache=MemoryCache(),
        user=user or ANONYMOUS,
        symbol_loader=DataLoader(load_fn=_noop),
        watchlist_service=WatchlistService(),
        kafka_factory=MagicMock(spec=KafkaConsumerFactory),
    )


@pytest.mark.asyncio
async def test_trades_query_returns_edges():
    ctx = _make_context(trade_rows=[SAMPLE_TRADE_ROW])
    result = await schema.execute(
        "{ trades(first: 5) { edges { node { tradeId symbol price } } totalCount } }",
        context_value=ctx,
    )
    assert result.errors is None
    data = result.data["trades"]
    assert data["totalCount"] == 1
    assert data["edges"][0]["node"]["symbol"] == "AAPL"


@pytest.mark.asyncio
async def test_symbols_query_returns_symbols():
    ctx = _make_context(symbol_rows=[SAMPLE_SYMBOL_ROW])
    result = await schema.execute(
        "{ symbols(first: 10) { edges { node { symbol companyName sector } } totalCount } }",
        context_value=ctx,
    )
    assert result.errors is None
    data = result.data["symbols"]
    assert data["totalCount"] == 1
    assert data["edges"][0]["node"]["symbol"] == "AAPL"


@pytest.mark.asyncio
async def test_daily_summary_query():
    ctx = _make_context(summary_rows=[SAMPLE_SUMMARY_ROW])
    result = await schema.execute(
        """
        {
          dailySummary(
            symbol: "AAPL",
            dateRange: { start: "2024-03-01", end: "2024-03-31" }
          ) {
            edges { node { symbol tradingDate openPrice closePrice vwap priceRange } }
            totalCount
          }
        }
        """,
        context_value=ctx,
    )
    assert result.errors is None
    data = result.data["dailySummary"]
    assert data["totalCount"] == 1
    node = data["edges"][0]["node"]
    assert node["symbol"] == "AAPL"
    assert node["priceRange"] == pytest.approx(6.0)


@pytest.mark.asyncio
async def test_market_overview_query():
    ctx = _make_context(summary_rows=[SAMPLE_SUMMARY_ROW])
    result = await schema.execute(
        '{ marketOverview(targetDate: "2024-03-15") { tradingDate totalTrades uniqueSymbols } }',
        context_value=ctx,
    )
    assert result.errors is None
    data = result.data["marketOverview"]
    assert data["uniqueSymbols"] == 1


@pytest.mark.asyncio
async def test_create_watchlist_mutation():
    from app.auth.models import UserContext
    user = UserContext(user_id="u1", roles=["viewer"])
    ctx = _make_context(user=user)
    result = await schema.execute(
        """
        mutation {
          createWatchlist(input: { name: "My List", symbols: ["AAPL", "GOOG"] }) {
            id name symbols userId
          }
        }
        """,
        context_value=ctx,
    )
    assert result.errors is None
    wl = result.data["createWatchlist"]
    assert wl["name"] == "My List"
    assert "AAPL" in wl["symbols"]


@pytest.mark.asyncio
async def test_mutation_permission_denied_for_no_role():
    from app.auth.models import UserContext
    no_role_user = UserContext(user_id="x", roles=[])
    ctx = _make_context(user=no_role_user)
    result = await schema.execute(
        """
        mutation {
          createWatchlist(input: { name: "Fail", symbols: [] }) {
            id
          }
        }
        """,
        context_value=ctx,
    )
    # Expect a permission error
    assert result.errors is not None
