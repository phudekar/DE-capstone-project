"""Unit tests for SymbolResolver."""

from __future__ import annotations

import pytest
from unittest.mock import AsyncMock, MagicMock

from app.cache.memory_cache import MemoryCache
from app.db.iceberg_duckdb import IcebergDuckDB
from app.resolvers.symbol import SymbolResolver, _row_to_symbol
from app.schema.inputs import SymbolFilterInput
from tests.conftest import SAMPLE_SYMBOL_ROW


@pytest.fixture
def engine():
    e = MagicMock(spec=IcebergDuckDB)
    e.execute = AsyncMock(return_value=[SAMPLE_SYMBOL_ROW])
    return e


@pytest.fixture
def cache():
    return MemoryCache()


def test_row_to_symbol():
    sym = _row_to_symbol(SAMPLE_SYMBOL_ROW)
    assert sym.symbol == "AAPL"
    assert sym.company_name == "Apple Inc."
    assert sym.sector == "Technology"
    assert sym.is_current is True


@pytest.mark.asyncio
async def test_resolve_returns_connection(engine, cache):
    resolver = SymbolResolver(engine, cache)
    result = await resolver.resolve(first=50)
    assert result.total_count == 1
    assert result.edges[0].node.symbol == "AAPL"


@pytest.mark.asyncio
async def test_resolve_caches_result(engine, cache):
    resolver = SymbolResolver(engine, cache)
    await resolver.resolve(first=50)
    call_count = engine.execute.call_count
    await resolver.resolve(first=50)
    assert engine.execute.call_count == call_count


@pytest.mark.asyncio
async def test_resolve_sector_filter(engine, cache):
    resolver = SymbolResolver(engine, cache)
    f = SymbolFilterInput(sector="Technology")
    await resolver.resolve(filter=f)
    # The sector value should appear in the params passed to execute
    _, kwargs = engine.execute.call_args
    params = kwargs.get("params", [])
    assert "Technology" in params


@pytest.mark.asyncio
async def test_resolve_pagination_cursor(engine, cache):
    engine.execute = AsyncMock(return_value=[SAMPLE_SYMBOL_ROW] * 5)
    resolver = SymbolResolver(engine, cache)
    result = await resolver.resolve(first=3)
    assert len(result.edges) == 3
    assert result.page_info.has_next_page is True
    assert result.page_info.end_cursor is not None
