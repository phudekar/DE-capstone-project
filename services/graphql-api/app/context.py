"""GraphQL request context factory."""

from fastapi import Request
from strawberry.dataloader import DataLoader
from strawberry.fastapi import BaseContext

from app.auth.models import ANONYMOUS, UserContext
from app.cache.memory_cache import MemoryCache, get_cache
from app.db.iceberg_duckdb import IcebergDuckDB, get_engine
from app.loaders.symbol_loader import make_symbol_loader
from app.services.watchlist_service import WatchlistService, get_watchlist_service
from app.streaming.kafka_consumer import KafkaConsumerFactory


class GraphQLContext(BaseContext):
    def __init__(
        self,
        engine: IcebergDuckDB,
        cache: MemoryCache,
        symbol_loader: DataLoader,
        watchlist_service: WatchlistService,
        kafka_factory: KafkaConsumerFactory,
        user: UserContext = ANONYMOUS,
    ) -> None:
        super().__init__()
        self.engine = engine
        self.cache = cache
        self.symbol_loader = symbol_loader
        self.watchlist_service = watchlist_service
        self.kafka_factory = kafka_factory
        self._fallback_user = user

    @property
    def user(self) -> UserContext:
        """Resolve user from request.state (HTTP) or fallback (WebSocket/test)."""
        if self.request is not None and isinstance(self.request, Request):
            return getattr(self.request.state, "user", self._fallback_user)
        return self._fallback_user


async def get_context() -> GraphQLContext:
    engine = get_engine()
    return GraphQLContext(
        engine=engine,
        cache=get_cache(),
        symbol_loader=make_symbol_loader(engine),
        watchlist_service=get_watchlist_service(),
        kafka_factory=KafkaConsumerFactory(),
    )
