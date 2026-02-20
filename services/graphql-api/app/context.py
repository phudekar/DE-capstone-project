"""GraphQL request context factory."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

from fastapi import Request
from strawberry.fastapi import BaseContext
from strawberry.dataloader import DataLoader

from app.auth.models import UserContext, ANONYMOUS
from app.cache.memory_cache import get_cache, MemoryCache
from app.db.iceberg_duckdb import get_engine, IcebergDuckDB
from app.loaders.symbol_loader import make_symbol_loader
from app.services.watchlist_service import get_watchlist_service, WatchlistService
from app.streaming.kafka_consumer import KafkaConsumerFactory


@dataclass
class GraphQLContext(BaseContext):
    engine: IcebergDuckDB
    cache: MemoryCache
    user: UserContext
    symbol_loader: DataLoader
    watchlist_service: WatchlistService
    kafka_factory: KafkaConsumerFactory

    @classmethod
    def from_request(cls, request: Request) -> "GraphQLContext":
        engine = get_engine()
        return cls(
            engine=engine,
            cache=get_cache(),
            user=getattr(request.state, "user", ANONYMOUS),
            symbol_loader=make_symbol_loader(engine),
            watchlist_service=get_watchlist_service(),
            kafka_factory=KafkaConsumerFactory(),
        )


async def get_context(request: Request) -> GraphQLContext:
    return GraphQLContext.from_request(request)
