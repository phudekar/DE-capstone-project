"""Shared test fixtures for graphql-api tests."""

from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
import strawberry
from strawberry.dataloader import DataLoader

from app.auth.models import UserContext, ANONYMOUS
from app.cache.memory_cache import MemoryCache
from app.db.iceberg_duckdb import IcebergDuckDB
from app.services.watchlist_service import WatchlistService
from app.streaming.kafka_consumer import KafkaConsumerFactory


# ─── Sample data ──────────────────────────────────────────────────────────────

SAMPLE_TRADE_ROW: dict[str, Any] = {
    "trade_id": "trade-001",
    "symbol": "AAPL",
    "price": 150.25,
    "quantity": 100,
    "buyer_agent_id": "agent-1",
    "seller_agent_id": "agent-2",
    "aggressor_side": "BUY",
    "timestamp": datetime(2024, 3, 15, 10, 30, 0, tzinfo=timezone.utc),
    "company_name": "Apple Inc.",
    "sector": "Technology",
}

SAMPLE_SUMMARY_ROW: dict[str, Any] = {
    "symbol": "AAPL",
    "trading_date": date(2024, 3, 15),
    "open_price": 148.0,
    "close_price": 152.0,
    "high_price": 153.5,
    "low_price": 147.5,
    "vwap": 150.1,
    "total_volume": 5_000_000,
    "trade_count": 12_000,
    "total_value": 750_500_000.0,
    "company_name": "Apple Inc.",
    "sector": "Technology",
}

SAMPLE_SYMBOL_ROW: dict[str, Any] = {
    "symbol": "AAPL",
    "company_name": "Apple Inc.",
    "sector": "Technology",
    "market_cap_category": "large",
    "is_current": True,
}


# ─── Fixtures ─────────────────────────────────────────────────────────────────

@pytest.fixture
def mock_engine() -> MagicMock:
    engine = MagicMock(spec=IcebergDuckDB)
    engine.execute = AsyncMock(return_value=[])
    engine.execute_multi = AsyncMock(return_value=[])
    return engine


@pytest.fixture
def memory_cache() -> MemoryCache:
    return MemoryCache()


@pytest.fixture
def admin_user() -> UserContext:
    return UserContext(
        user_id="admin-001",
        roles=["admin", "trader", "analyst", "viewer"],
        account_id="ACC-001",
    )


@pytest.fixture
def viewer_user() -> UserContext:
    return UserContext(user_id="viewer-001", roles=["viewer"], account_id="ACC-002")


@pytest.fixture
def watchlist_service() -> WatchlistService:
    return WatchlistService()


@pytest.fixture
def noop_symbol_loader() -> DataLoader:
    async def _load(keys):
        return [None] * len(keys)

    return DataLoader(load_fn=_load)


def make_context(
    engine=None,
    cache=None,
    user=None,
    symbol_loader=None,
    watchlist_service=None,
    kafka_factory=None,
):
    from app.context import GraphQLContext

    _engine = engine or MagicMock(spec=IcebergDuckDB)
    _engine.execute = AsyncMock(return_value=[])

    async def _noop(keys):
        return [None] * len(keys)

    return GraphQLContext(
        engine=_engine,
        cache=cache or MemoryCache(),
        user=user or ANONYMOUS,
        symbol_loader=symbol_loader or DataLoader(load_fn=_noop),
        watchlist_service=watchlist_service or WatchlistService(),
        kafka_factory=kafka_factory or MagicMock(spec=KafkaConsumerFactory),
    )
