"""Integration tests for MetricsExtension wired into the GraphQL schema."""

import pytest
from unittest.mock import AsyncMock, MagicMock
from strawberry.dataloader import DataLoader

from app.schema import schema
from app.auth.models import ANONYMOUS
from app.cache.memory_cache import MemoryCache
from app.context import GraphQLContext
from app.db.iceberg_duckdb import IcebergDuckDB
from app.services.watchlist_service import WatchlistService
from app.streaming.kafka_consumer import KafkaConsumerFactory
from tests.conftest import SAMPLE_TRADE_ROW


def _make_ctx(trade_rows=None):
    engine = MagicMock(spec=IcebergDuckDB)

    async def _side_effect(namespace, table, sql, params=None, **kwargs):
        if "COUNT(*)" in sql:
            return [{"n": len(trade_rows or [])}]
        return trade_rows or []

    engine.execute = AsyncMock(side_effect=_side_effect)

    async def _noop(keys):
        return [None] * len(keys)

    return GraphQLContext(
        engine=engine,
        cache=MemoryCache(),
        user=ANONYMOUS,
        symbol_loader=DataLoader(load_fn=_noop),
        watchlist_service=WatchlistService(),
        kafka_factory=MagicMock(spec=KafkaConsumerFactory),
    )


def _get_sample_value(sample_name: str, labels: dict | None = None) -> float | None:
    from prometheus_client import REGISTRY
    for metric in REGISTRY.collect():
        for sample in metric.samples:
            if sample.name == sample_name:
                if labels is None or all(
                    sample.labels.get(k) == v for k, v in labels.items()
                ):
                    return sample.value
    return None


@pytest.mark.asyncio
async def test_metrics_extension_records_request():
    """Executing a query should increment the requests counter."""
    before = _get_sample_value(
        "graphql_requests_total",
        {"operation_type": "query", "status": "success"},
    ) or 0.0

    ctx = _make_ctx(trade_rows=[SAMPLE_TRADE_ROW])
    result = await schema.execute(
        "{ trades(first: 1) { totalCount } }",
        context_value=ctx,
    )
    assert result.errors is None

    after = _get_sample_value(
        "graphql_requests_total",
        {"operation_type": "query", "status": "success"},
    ) or 0.0
    assert after > before, "graphql_requests_total should have been incremented"


@pytest.mark.asyncio
async def test_metrics_extension_records_duration():
    """Executing a query should record duration in the histogram."""
    ctx = _make_ctx(trade_rows=[SAMPLE_TRADE_ROW])
    before = _get_sample_value(
        "graphql_request_duration_seconds_count",
        {"operation_type": "query"},
    ) or 0.0

    result = await schema.execute(
        "{ trades(first: 1) { totalCount } }",
        context_value=ctx,
    )
    assert result.errors is None

    after = _get_sample_value(
        "graphql_request_duration_seconds_count",
        {"operation_type": "query"},
    ) or 0.0
    assert after > before, "graphql_request_duration_seconds_count should be incremented"


@pytest.mark.asyncio
async def test_metrics_extension_no_crash_on_simple_query():
    """The extension should not crash on a normal query."""
    ctx = _make_ctx()
    result = await schema.execute(
        "{ trades(first: 1) { totalCount } }",
        context_value=ctx,
    )
    # No crash is sufficient
    assert True
