# 09 - Data Access & Serving Layer (GraphQL API)

## Overview

This document details the design and implementation of the GraphQL API that serves as the
primary data access layer for the stock market trade data platform. The API exposes
aggregated analytics, real-time trade feeds, and detailed trade history to downstream
consumers including the Superset dashboards, external clients, and internal services.

---

## 1. Technology Choice: Strawberry GraphQL

### Why Strawberry GraphQL

| Criterion                | Strawberry GraphQL               | Alternatives Considered            |
|--------------------------|----------------------------------|------------------------------------|
| **Language**             | Python (native to our stack)     | Ariadne (Python), Apollo (Node.js) |
| **Type Safety**          | First-class: Python dataclasses  | Ariadne uses SDL-first approach    |
| **Async Support**        | Full async/await support         | Apollo requires Node.js runtime    |
| **FastAPI Integration**  | Official integration, zero-glue  | Ariadne also integrates well       |
| **Code-first Schema**    | Yes, via decorators + dataclass  | Apollo is schema-first             |
| **Subscriptions**        | Built-in WebSocket support       | Apollo has mature subscriptions    |
| **Community & Maturity** | Growing, well-maintained         | Apollo is most mature overall      |
| **Relay Support**        | Built-in cursor pagination       | Apollo has relay-style pagination  |

### Key Advantages for This Project

- **Python-native**: Aligns with the rest of the data engineering stack (PySpark, dbt-python,
  Airflow). Shared models and utilities across the codebase.
- **Type-safe schema**: Schema is defined via Python dataclasses and type annotations. Errors
  are caught at development time rather than runtime.
- **Async resolvers**: Critical for performance when querying DuckDB and Iceberg concurrently.
  Async resolvers prevent blocking the event loop during I/O-heavy operations.
- **FastAPI synergy**: Strawberry has an official FastAPI integration that provides automatic
  OpenAPI docs alongside GraphQL, shared middleware, and dependency injection.

### Version Requirements

```
strawberry-graphql[fastapi] >= 0.220.0
fastapi >= 0.110.0
uvicorn[standard] >= 0.27.0
duckdb >= 0.10.0
redis >= 5.0.0
PyJWT >= 2.8.0
strawberry-graphql[channels]  # for WebSocket subscriptions
```

---

## 2. API Architecture

### 2.1 High-Level Architecture

```
                                 +---------------------+
                                 |   Load Balancer     |
                                 |   (Nginx / Traefik) |
                                 +----------+----------+
                                            |
                              +-------------+-------------+
                              |                           |
                     +--------v--------+        +---------v--------+
                     | FastAPI Instance |        | FastAPI Instance  |
                     | (Strawberry GQL) |        | (Strawberry GQL)  |
                     +--------+--------+        +---------+--------+
                              |                           |
            +-----------------+---------------------------+-----------+
            |                 |                 |                     |
   +--------v------+  +------v-------+  +------v-------+   +--------v------+
   | DuckDB/Iceberg|  |   Redis      |  |   Kafka      |   |  PostgreSQL   |
   | (Gold/Silver) |  |   Cache      |  |  (Real-time) |   |  (User Data)  |
   +---------------+  +--------------+  +--------------+   +---------------+
```

### 2.2 FastAPI + Strawberry Integration

```python
# services/graphql-api/app/main.py

import strawberry
from fastapi import FastAPI
from strawberry.fastapi import GraphQLRouter
from strawberry.subscriptions import GRAPHQL_TRANSPORT_WS_PROTOCOL

from app.schema import Query, Mutation, Subscription
from app.context import get_context
from app.middleware.auth import JWTAuthMiddleware
from app.middleware.rate_limit import RateLimitMiddleware

schema = strawberry.Schema(
    query=Query,
    mutation=Mutation,
    subscription=Subscription,
    extensions=[
        QueryDepthLimiter(max_depth=5),
        QueryComplexityAnalyzer(max_complexity=100),
    ],
)

graphql_app = GraphQLRouter(
    schema,
    context_getter=get_context,
    subscription_protocols=[GRAPHQL_TRANSPORT_WS_PROTOCOL],
)

app = FastAPI(
    title="Trade Data GraphQL API",
    version="1.0.0",
    description="GraphQL API for stock market trade data analytics",
)

app.add_middleware(JWTAuthMiddleware)
app.add_middleware(RateLimitMiddleware, requests_per_minute=60)

app.include_router(graphql_app, prefix="/graphql")
```

### 2.3 Async Resolvers with DuckDB/Iceberg Backends

All resolvers use async patterns to prevent blocking the event loop. DuckDB connections are
managed via a connection pool since DuckDB supports concurrent read connections.

```python
# services/graphql-api/app/db/duckdb_pool.py

import asyncio
from contextlib import asynccontextmanager
from dataclasses import dataclass
import duckdb

@dataclass
class DuckDBPool:
    database_path: str
    max_connections: int = 10

    def __post_init__(self):
        self._semaphore = asyncio.Semaphore(self.max_connections)
        self._base_conn = duckdb.connect(self.database_path, read_only=True)
        # Load Iceberg extension once
        self._base_conn.execute("INSTALL iceberg; LOAD iceberg;")

    @asynccontextmanager
    async def acquire(self):
        async with self._semaphore:
            conn = self._base_conn.cursor()
            try:
                yield conn
            finally:
                conn.close()

    async def execute_query(self, query: str, params: list = None):
        async with self.acquire() as cursor:
            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(
                None, lambda: cursor.execute(query, params).fetchall()
            )
            return result
```

### 2.4 Connection Pooling Strategy

| Backend      | Pool Type                | Max Connections | Idle Timeout |
|--------------|--------------------------|-----------------|--------------|
| DuckDB       | Custom async semaphore   | 10              | 300s         |
| PostgreSQL   | asyncpg pool             | 20              | 300s         |
| Redis        | aioredis pool            | 50              | 600s         |
| Kafka        | aiokafka consumer group  | 5 (partitions)  | N/A          |

### 2.5 Authentication (JWT Tokens)

```python
# services/graphql-api/app/middleware/auth.py

from fastapi import Request
from starlette.middleware.base import BaseHTTPMiddleware
import jwt

class JWTAuthMiddleware(BaseHTTPMiddleware):
    """
    Validates JWT tokens from the Authorization header.
    Tokens are issued by the identity service and contain:
    - sub: user ID
    - roles: list of roles (analyst, trader, admin)
    - exp: expiration timestamp
    """
    ALGORITHM = "RS256"

    async def dispatch(self, request: Request, call_next):
        # Allow GraphiQL playground in development
        if request.url.path == "/graphql" and request.method == "GET":
            return await call_next(request)

        token = request.headers.get("Authorization", "").replace("Bearer ", "")
        if not token:
            request.state.user = None
            return await call_next(request)

        try:
            payload = jwt.decode(
                token,
                self.public_key,
                algorithms=[self.ALGORITHM],
                audience="trade-api",
            )
            request.state.user = UserContext(
                user_id=payload["sub"],
                roles=payload.get("roles", []),
                account_id=payload.get("account_id"),
            )
        except jwt.ExpiredSignatureError:
            return JSONResponse(status_code=401, content={"error": "Token expired"})
        except jwt.InvalidTokenError:
            return JSONResponse(status_code=401, content={"error": "Invalid token"})

        return await call_next(request)
```

**JWT Token Structure:**

```json
{
  "sub": "user-uuid-1234",
  "roles": ["analyst", "trader"],
  "account_id": "ACC-001",
  "iss": "trade-platform-auth",
  "aud": "trade-api",
  "exp": 1710000000,
  "iat": 1709996400
}
```

**Role-based Access Control:**

| Role      | Permissions                                                        |
|-----------|--------------------------------------------------------------------|
| `viewer`  | Read market data, daily summaries, public symbol info              |
| `analyst` | All viewer + trader performance, portfolio queries, SQL Lab access |
| `trader`  | All analyst + own portfolio mutations, watchlists                  |
| `admin`   | All trader + pipeline health, all trader data, system queries      |

### 2.6 Rate Limiting

```python
# services/graphql-api/app/middleware/rate_limit.py

import time
from collections import defaultdict
from fastapi import Request
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse

class RateLimitMiddleware(BaseHTTPMiddleware):
    """
    Token-bucket rate limiting per API key / user.
    Configurable per role.
    """
    RATE_LIMITS = {
        "viewer":  {"requests_per_minute": 30,  "burst": 10},
        "analyst": {"requests_per_minute": 60,  "burst": 20},
        "trader":  {"requests_per_minute": 120, "burst": 40},
        "admin":   {"requests_per_minute": 300, "burst": 100},
        "anonymous": {"requests_per_minute": 10, "burst": 5},
    }

    async def dispatch(self, request: Request, call_next):
        user = getattr(request.state, "user", None)
        role = user.roles[0] if user and user.roles else "anonymous"
        key = user.user_id if user else request.client.host

        limits = self.RATE_LIMITS.get(role, self.RATE_LIMITS["anonymous"])

        if not self._check_rate(key, limits):
            return JSONResponse(
                status_code=429,
                content={"error": "Rate limit exceeded"},
                headers={"Retry-After": "60"},
            )

        response = await call_next(request)
        return response
```

---

## 3. GraphQL Schema Design

### 3.1 Core Types

```python
# services/graphql-api/app/schema/types.py

import strawberry
from datetime import datetime, date
from decimal import Decimal
from enum import Enum
from typing import Optional, List

@strawberry.enum
class TradeType(Enum):
    BUY = "BUY"
    SELL = "SELL"

@strawberry.enum
class OrderType(Enum):
    MARKET = "MARKET"
    LIMIT = "LIMIT"
    STOP = "STOP"
    STOP_LIMIT = "STOP_LIMIT"

@strawberry.enum
class SortDirection(Enum):
    ASC = "ASC"
    DESC = "DESC"

# ─── Trade ───────────────────────────────────────────────────────────

@strawberry.type
class Trade:
    id: strawberry.ID
    symbol: str
    trade_type: TradeType
    price: Decimal
    quantity: int
    total_value: Decimal
    timestamp: datetime
    exchange: str
    trader_id: str
    account_id: str
    order_type: OrderType
    commission: Decimal
    is_institutional: bool

    @strawberry.field
    async def symbol_details(self, info) -> "Symbol":
        """Resolve related symbol details (uses DataLoader to prevent N+1)."""
        return await info.context.symbol_loader.load(self.symbol)

    @strawberry.field
    async def trader(self, info) -> Optional["TraderPerformance"]:
        """Resolve trader info (requires analyst+ role)."""
        return await info.context.trader_loader.load(self.trader_id)

# ─── Symbol ──────────────────────────────────────────────────────────

@strawberry.type
class Symbol:
    ticker: str
    company_name: str
    sector: str
    industry: str
    exchange: str
    market_cap: Optional[Decimal]
    currency: str
    is_active: bool

    @strawberry.field
    async def latest_price(self, info) -> Optional[Decimal]:
        return await info.context.price_loader.load(self.ticker)

    @strawberry.field
    async def daily_summary(
        self, info, date_val: Optional[date] = None
    ) -> Optional["DailySummary"]:
        target_date = date_val or date.today()
        return await info.context.summary_loader.load((self.ticker, target_date))

# ─── OrderBook ───────────────────────────────────────────────────────

@strawberry.type
class OrderBookLevel:
    price: Decimal
    quantity: int
    order_count: int

@strawberry.type
class OrderBook:
    symbol: str
    timestamp: datetime
    bids: List[OrderBookLevel]
    asks: List[OrderBookLevel]
    spread: Decimal
    mid_price: Decimal

# ─── MarketData ──────────────────────────────────────────────────────

@strawberry.type
class MarketData:
    symbol: str
    open_price: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: int
    vwap: Decimal
    timestamp: datetime
    change_percent: Decimal

# ─── DailySummary ────────────────────────────────────────────────────

@strawberry.type
class DailySummary:
    symbol: str
    trade_date: date
    open_price: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: int
    trade_count: int
    total_value: Decimal
    vwap: Decimal
    buy_volume: int
    sell_volume: int
    avg_trade_size: Decimal
    price_range: Decimal
    volatility: Optional[Decimal]

# ─── TraderPerformance ───────────────────────────────────────────────

@strawberry.type
class TraderPerformance:
    trader_id: str
    total_trades: int
    total_volume: int
    total_value: Decimal
    avg_trade_size: Decimal
    buy_count: int
    sell_count: int
    unique_symbols: int
    most_traded_symbol: str
    win_rate: Optional[Decimal]
    pnl: Optional[Decimal]
    avg_hold_time_minutes: Optional[Decimal]
    active_days: int
    first_trade: datetime
    last_trade: datetime

# ─── PortfolioPosition ──────────────────────────────────────────────

@strawberry.type
class PortfolioPosition:
    account_id: str
    symbol: str
    quantity: int
    avg_cost_basis: Decimal
    current_price: Decimal
    market_value: Decimal
    unrealized_pnl: Decimal
    unrealized_pnl_percent: Decimal
    weight_percent: Decimal
    last_trade_date: datetime

# ─── MarketOverview ──────────────────────────────────────────────────

@strawberry.type
class MarketOverview:
    date: date
    total_trades: int
    total_volume: int
    total_value: Decimal
    advancing_symbols: int
    declining_symbols: int
    unchanged_symbols: int
    most_active: List[Symbol]
    top_gainers: List[MarketData]
    top_losers: List[MarketData]
    sector_performance: List["SectorPerformance"]

@strawberry.type
class SectorPerformance:
    sector: str
    avg_change_percent: Decimal
    total_volume: int
    total_value: Decimal
    symbol_count: int

# ─── PriceAlert ──────────────────────────────────────────────────────

@strawberry.type
class PriceAlert:
    symbol: str
    alert_type: str       # "ABOVE", "BELOW", "CHANGE_PERCENT"
    threshold: Decimal
    current_price: Decimal
    triggered_at: datetime

# ─── Watchlist ───────────────────────────────────────────────────────

@strawberry.type
class Watchlist:
    id: strawberry.ID
    name: str
    user_id: str
    symbols: List[str]
    created_at: datetime
    updated_at: datetime
```

### 3.2 Input Types and Filters

```python
# services/graphql-api/app/schema/inputs.py

import strawberry
from datetime import date, datetime
from typing import Optional, List

@strawberry.input
class DateRangeInput:
    start: date
    end: date

@strawberry.input
class TradeFilterInput:
    symbol: Optional[str] = None
    symbols: Optional[List[str]] = None
    trade_type: Optional[TradeType] = None
    order_type: Optional[OrderType] = None
    min_price: Optional[Decimal] = None
    max_price: Optional[Decimal] = None
    min_quantity: Optional[int] = None
    max_quantity: Optional[int] = None
    exchange: Optional[str] = None
    is_institutional: Optional[bool] = None
    date_range: Optional[DateRangeInput] = None

@strawberry.input
class TradeSortInput:
    field: str = "timestamp"          # timestamp, price, quantity, total_value
    direction: SortDirection = SortDirection.DESC

@strawberry.input
class SymbolFilterInput:
    sector: Optional[str] = None
    exchange: Optional[str] = None
    is_active: Optional[bool] = True
    search: Optional[str] = None       # fuzzy search on ticker + company name

@strawberry.input
class WatchlistInput:
    name: str
    symbols: List[str] = strawberry.field(default_factory=list)

@strawberry.input
class AddToWatchlistInput:
    watchlist_id: strawberry.ID
    symbols: List[str]
```

### 3.3 Pagination (Relay Cursor Spec)

```python
# services/graphql-api/app/schema/pagination.py

import strawberry
from typing import TypeVar, Generic, List, Optional
import base64
import json

T = TypeVar("T")

@strawberry.type
class PageInfo:
    has_next_page: bool
    has_previous_page: bool
    start_cursor: Optional[str]
    end_cursor: Optional[str]

@strawberry.type
class Edge(Generic[T]):
    node: T
    cursor: str

@strawberry.type
class Connection(Generic[T]):
    edges: List[Edge[T]]
    page_info: PageInfo
    total_count: int

# Concrete connection types for each entity
TradeConnection = strawberry.type(Connection[Trade], name="TradeConnection")
SymbolConnection = strawberry.type(Connection[Symbol], name="SymbolConnection")
DailySummaryConnection = strawberry.type(
    Connection[DailySummary], name="DailySummaryConnection"
)

def encode_cursor(offset: int) -> str:
    """Encode offset as an opaque cursor string."""
    return base64.b64encode(json.dumps({"offset": offset}).encode()).decode()

def decode_cursor(cursor: str) -> int:
    """Decode cursor string back to offset."""
    data = json.loads(base64.b64decode(cursor.encode()).decode())
    return data["offset"]
```

### 3.4 Query Root

```python
# services/graphql-api/app/schema/query.py

import strawberry
from typing import Optional, List
from datetime import date

@strawberry.type
class Query:

    @strawberry.field(description="Fetch paginated trades with filtering and sorting.")
    async def trades(
        self,
        info,
        filter: Optional[TradeFilterInput] = None,
        sort: Optional[TradeSortInput] = None,
        first: Optional[int] = 20,
        after: Optional[str] = None,
        last: Optional[int] = None,
        before: Optional[str] = None,
    ) -> TradeConnection:
        """
        Queries the Silver layer (detailed trade data) via DuckDB/Iceberg.
        Supports cursor-based pagination, filtering by symbol/type/date/price,
        and sorting by any numeric or timestamp field.
        """
        resolver = TradeResolver(info.context)
        return await resolver.resolve(
            filter=filter, sort=sort,
            first=first, after=after, last=last, before=before,
        )

    @strawberry.field(description="Fetch a single trade by its unique ID.")
    async def trade_by_id(self, info, id: strawberry.ID) -> Optional[Trade]:
        resolver = TradeResolver(info.context)
        return await resolver.resolve_by_id(id)

    @strawberry.field(description="Daily OHLCV summaries for a symbol.")
    async def daily_summary(
        self,
        info,
        symbol: str,
        date_range: DateRangeInput,
        first: Optional[int] = 30,
        after: Optional[str] = None,
    ) -> DailySummaryConnection:
        """
        Queries the Gold layer daily_trade_summary table.
        Returns OHLCV data plus computed metrics (VWAP, volatility).
        """
        resolver = DailySummaryResolver(info.context)
        return await resolver.resolve(
            symbol=symbol, date_range=date_range,
            first=first, after=after,
        )

    @strawberry.field(description="Market-wide overview for a given date.")
    async def market_overview(
        self, info, target_date: Optional[date] = None
    ) -> MarketOverview:
        """
        Queries the Gold layer for aggregate market statistics.
        Results are cached in Redis (TTL: 5 minutes for current day, 24h for past).
        """
        resolver = MarketOverviewResolver(info.context)
        return await resolver.resolve(target_date=target_date or date.today())

    @strawberry.field(description="Performance metrics for a specific trader.")
    async def trader_performance(
        self,
        info,
        trader_id: str,
        date_range: Optional[DateRangeInput] = None,
    ) -> Optional[TraderPerformance]:
        """
        Requires analyst+ role. Queries Gold layer trader_performance_metrics.
        """
        require_role(info, ["analyst", "trader", "admin"])
        resolver = TraderPerformanceResolver(info.context)
        return await resolver.resolve(trader_id=trader_id, date_range=date_range)

    @strawberry.field(description="List symbols with optional filtering.")
    async def symbols(
        self,
        info,
        filter: Optional[SymbolFilterInput] = None,
        first: Optional[int] = 50,
        after: Optional[str] = None,
    ) -> SymbolConnection:
        """
        Queries the symbol reference table. Cached in Redis (TTL: 1 hour).
        """
        resolver = SymbolResolver(info.context)
        return await resolver.resolve(filter=filter, first=first, after=after)

    @strawberry.field(description="Current order book for a symbol.")
    async def order_book(self, info, symbol: str) -> Optional[OrderBook]:
        """
        Real-time order book from Kafka state store or Redis snapshot.
        Refreshed every 100ms via Kafka Streams.
        """
        resolver = OrderBookResolver(info.context)
        return await resolver.resolve(symbol=symbol)

    @strawberry.field(description="Portfolio positions for an account.")
    async def portfolio_positions(
        self,
        info,
        account_id: str,
    ) -> List[PortfolioPosition]:
        """
        Requires trader+ role. Users can only query their own account
        unless they have admin role.
        """
        require_role(info, ["trader", "admin"])
        enforce_account_access(info, account_id)
        resolver = PortfolioResolver(info.context)
        return await resolver.resolve(account_id=account_id)
```

### 3.5 Subscriptions (WebSocket)

```python
# services/graphql-api/app/schema/subscription.py

import strawberry
from typing import AsyncGenerator, Optional
import asyncio

@strawberry.type
class Subscription:

    @strawberry.subscription(
        description="Real-time trade feed. Optionally filter by symbol."
    )
    async def on_new_trade(
        self, info, symbol: Optional[str] = None
    ) -> AsyncGenerator[Trade, None]:
        """
        Consumes from the Kafka 'trades-enriched' topic.
        Each WebSocket client gets a dedicated consumer with a unique group ID.
        Filtered server-side by symbol if provided.
        """
        consumer = info.context.kafka_consumer_factory.create(
            topic="trades-enriched",
            group_id=f"ws-{info.context.connection_id}",
        )
        try:
            async for message in consumer:
                trade = Trade.from_kafka_message(message)
                if symbol is None or trade.symbol == symbol:
                    yield trade
        finally:
            await consumer.stop()

    @strawberry.subscription(
        description="Price alerts triggered when thresholds are crossed."
    )
    async def on_price_alert(
        self, info, symbols: Optional[list[str]] = None
    ) -> AsyncGenerator[PriceAlert, None]:
        """
        Consumes from the Kafka 'price-alerts' topic.
        Alerts are generated by the stream processing layer when:
        - Price crosses a user-defined threshold
        - Percentage change exceeds configured limit (e.g., >5% in 1 min)
        """
        consumer = info.context.kafka_consumer_factory.create(
            topic="price-alerts",
            group_id=f"ws-alerts-{info.context.connection_id}",
        )
        try:
            async for message in consumer:
                alert = PriceAlert.from_kafka_message(message)
                if symbols is None or alert.symbol in symbols:
                    yield alert
        finally:
            await consumer.stop()
```

### 3.6 Mutations

```python
# services/graphql-api/app/schema/mutation.py

import strawberry
from typing import List

@strawberry.type
class Mutation:

    @strawberry.mutation(description="Create a new watchlist for the authenticated user.")
    async def create_watchlist(
        self, info, input: WatchlistInput
    ) -> Watchlist:
        """
        Persists to PostgreSQL (user preferences database).
        Validates that all symbols exist in the reference table.
        """
        require_role(info, ["viewer", "analyst", "trader", "admin"])
        user_id = info.context.user.user_id

        # Validate symbols
        valid_symbols = await info.context.symbol_service.validate_symbols(
            input.symbols
        )
        if len(valid_symbols) != len(input.symbols):
            invalid = set(input.symbols) - set(valid_symbols)
            raise ValueError(f"Invalid symbols: {invalid}")

        return await info.context.watchlist_service.create(
            user_id=user_id,
            name=input.name,
            symbols=input.symbols,
        )

    @strawberry.mutation(description="Add symbols to an existing watchlist.")
    async def add_to_watchlist(
        self, info, input: AddToWatchlistInput
    ) -> Watchlist:
        """
        Appends symbols to a watchlist. Verifies ownership.
        """
        require_role(info, ["viewer", "analyst", "trader", "admin"])
        user_id = info.context.user.user_id

        watchlist = await info.context.watchlist_service.get(input.watchlist_id)
        if watchlist.user_id != user_id:
            raise PermissionError("Cannot modify another user's watchlist")

        return await info.context.watchlist_service.add_symbols(
            watchlist_id=input.watchlist_id,
            symbols=input.symbols,
        )

    @strawberry.mutation(description="Remove symbols from a watchlist.")
    async def remove_from_watchlist(
        self, info, watchlist_id: strawberry.ID, symbols: List[str]
    ) -> Watchlist:
        require_role(info, ["viewer", "analyst", "trader", "admin"])
        user_id = info.context.user.user_id

        watchlist = await info.context.watchlist_service.get(watchlist_id)
        if watchlist.user_id != user_id:
            raise PermissionError("Cannot modify another user's watchlist")

        return await info.context.watchlist_service.remove_symbols(
            watchlist_id=watchlist_id,
            symbols=symbols,
        )

    @strawberry.mutation(description="Delete a watchlist.")
    async def delete_watchlist(
        self, info, watchlist_id: strawberry.ID
    ) -> bool:
        require_role(info, ["viewer", "analyst", "trader", "admin"])
        user_id = info.context.user.user_id

        watchlist = await info.context.watchlist_service.get(watchlist_id)
        if watchlist.user_id != user_id:
            raise PermissionError("Cannot delete another user's watchlist")

        await info.context.watchlist_service.delete(watchlist_id)
        return True
```

---

## 4. Data Sources for Resolvers

### 4.1 Resolver-to-Source Mapping

| Query / Resolver          | Data Source              | Layer    | Cache TTL  |
|---------------------------|--------------------------|----------|------------|
| `trades`                  | DuckDB -> Iceberg        | Silver   | None       |
| `tradeById`               | DuckDB -> Iceberg        | Silver   | 60s        |
| `dailySummary`            | DuckDB -> Iceberg        | Gold     | 5 min      |
| `marketOverview`          | DuckDB -> Iceberg        | Gold     | 5 min (today), 24h (past) |
| `traderPerformance`       | DuckDB -> Iceberg        | Gold     | 15 min     |
| `symbols`                 | DuckDB -> Iceberg + Redis| Gold     | 1 hour     |
| `orderBook`               | Redis (Kafka state)      | Real-time| 100ms      |
| `portfolioPositions`      | DuckDB -> Iceberg        | Gold     | 1 min      |
| `onNewTrade` (sub)        | Kafka consumer           | Real-time| N/A        |
| `onPriceAlert` (sub)      | Kafka consumer           | Real-time| N/A        |
| `createWatchlist` (mut)   | PostgreSQL               | App DB   | Invalidate |
| `addToWatchlist` (mut)    | PostgreSQL               | App DB   | Invalidate |

### 4.2 Gold Layer Queries (DuckDB/Iceberg)

```python
# services/graphql-api/app/resolvers/daily_summary.py

class DailySummaryResolver:
    QUERY = """
        SELECT
            symbol,
            trade_date,
            open_price,
            high,
            low,
            close,
            volume,
            trade_count,
            total_value_traded AS total_value,
            vwap,
            buy_volume,
            sell_volume,
            avg_trade_size,
            (high - low) AS price_range,
            volatility
        FROM iceberg_scan('s3://trade-lakehouse/gold/daily_trade_summary')
        WHERE symbol = ?
          AND trade_date BETWEEN ? AND ?
        ORDER BY trade_date DESC
        LIMIT ? OFFSET ?
    """

    async def resolve(self, symbol, date_range, first, after):
        offset = decode_cursor(after) if after else 0
        rows = await self.ctx.duckdb.execute_query(
            self.QUERY,
            [symbol, date_range.start, date_range.end, first + 1, offset],
        )
        # Build connection with pagination info
        has_next = len(rows) > first
        edges = [
            Edge(node=DailySummary(**row), cursor=encode_cursor(offset + i))
            for i, row in enumerate(rows[:first])
        ]
        return DailySummaryConnection(
            edges=edges,
            page_info=PageInfo(
                has_next_page=has_next,
                has_previous_page=offset > 0,
                start_cursor=edges[0].cursor if edges else None,
                end_cursor=edges[-1].cursor if edges else None,
            ),
            total_count=await self._count(symbol, date_range),
        )
```

### 4.3 Silver Layer Queries (Detailed Trades)

```python
# services/graphql-api/app/resolvers/trade.py

class TradeResolver:
    """
    Queries the Silver layer for detailed, individual trade records.
    Supports complex filtering and predicate pushdown via Iceberg.
    """

    BASE_QUERY = """
        SELECT *
        FROM iceberg_scan('s3://trade-lakehouse/silver/trades_enriched')
        WHERE 1=1
    """

    def _build_where(self, filter: TradeFilterInput) -> tuple[str, list]:
        clauses = []
        params = []

        if filter.symbol:
            clauses.append("AND symbol = ?")
            params.append(filter.symbol)
        if filter.symbols:
            placeholders = ",".join(["?"] * len(filter.symbols))
            clauses.append(f"AND symbol IN ({placeholders})")
            params.extend(filter.symbols)
        if filter.trade_type:
            clauses.append("AND trade_type = ?")
            params.append(filter.trade_type.value)
        if filter.order_type:
            clauses.append("AND order_type = ?")
            params.append(filter.order_type.value)
        if filter.min_price is not None:
            clauses.append("AND price >= ?")
            params.append(filter.min_price)
        if filter.max_price is not None:
            clauses.append("AND price <= ?")
            params.append(filter.max_price)
        if filter.date_range:
            clauses.append("AND trade_date BETWEEN ? AND ?")
            params.extend([filter.date_range.start, filter.date_range.end])
        if filter.is_institutional is not None:
            clauses.append("AND is_institutional = ?")
            params.append(filter.is_institutional)

        return " ".join(clauses), params
```

### 4.4 Kafka Consumer for Real-Time Subscriptions

```python
# services/graphql-api/app/streaming/kafka_consumer.py

from aiokafka import AIOKafkaConsumer
import json

class KafkaConsumerFactory:
    def __init__(self, bootstrap_servers: str):
        self.bootstrap_servers = bootstrap_servers

    def create(self, topic: str, group_id: str) -> AIOKafkaConsumer:
        return AIOKafkaConsumer(
            topic,
            bootstrap_servers=self.bootstrap_servers,
            group_id=group_id,
            auto_offset_reset="latest",       # Only new messages for subscriptions
            enable_auto_commit=True,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            max_poll_interval_ms=300000,
            session_timeout_ms=30000,
        )
```

### 4.5 Redis Caching Layer

```python
# services/graphql-api/app/cache/redis_cache.py

import json
import hashlib
from typing import Optional, Any
from redis.asyncio import Redis

class RedisCache:
    def __init__(self, redis: Redis):
        self.redis = redis

    def _key(self, namespace: str, params: dict) -> str:
        """Generate a deterministic cache key from namespace + params."""
        param_hash = hashlib.md5(
            json.dumps(params, sort_keys=True, default=str).encode()
        ).hexdigest()
        return f"gql:{namespace}:{param_hash}"

    async def get(self, namespace: str, params: dict) -> Optional[Any]:
        key = self._key(namespace, params)
        data = await self.redis.get(key)
        if data:
            return json.loads(data)
        return None

    async def set(
        self, namespace: str, params: dict, value: Any, ttl_seconds: int
    ):
        key = self._key(namespace, params)
        await self.redis.setex(key, ttl_seconds, json.dumps(value, default=str))

    async def invalidate(self, namespace: str):
        """Invalidate all keys in a namespace."""
        pattern = f"gql:{namespace}:*"
        async for key in self.redis.scan_iter(match=pattern):
            await self.redis.delete(key)
```

---

## 5. Performance Optimization

### 5.1 DataLoader for N+1 Query Prevention

The N+1 problem occurs when a list query (e.g., list of trades) triggers individual queries
for related entities (e.g., symbol details per trade). DataLoader batches these into a single
query per request cycle.

```python
# services/graphql-api/app/loaders/symbol_loader.py

from strawberry.dataloader import DataLoader
from typing import List, Optional

async def load_symbols(keys: List[str]) -> List[Optional[Symbol]]:
    """
    Batch load symbols by ticker. Called once per request cycle
    regardless of how many trades reference these symbols.
    """
    placeholders = ",".join(["?"] * len(keys))
    query = f"""
        SELECT * FROM iceberg_scan('s3://trade-lakehouse/gold/symbol_reference')
        WHERE ticker IN ({placeholders})
    """
    rows = await duckdb_pool.execute_query(query, list(keys))
    symbol_map = {row["ticker"]: Symbol(**row) for row in rows}
    return [symbol_map.get(key) for key in keys]

# Created per request in the context factory:
def create_loaders(duckdb_pool):
    return {
        "symbol_loader": DataLoader(load_fn=load_symbols),
        "price_loader": DataLoader(load_fn=load_latest_prices),
        "trader_loader": DataLoader(load_fn=load_trader_profiles),
        "summary_loader": DataLoader(load_fn=load_daily_summaries),
    }
```

**DataLoader Batching Example:**

```
Without DataLoader:                With DataLoader:
───────────────────                ────────────────────
Query: trades(first: 50)          Query: trades(first: 50)
  -> 50 x SELECT symbol_details   -> 1 x SELECT ... WHERE ticker IN (50 tickers)
  -> 50 x SELECT trader_info      -> 1 x SELECT ... WHERE trader_id IN (50 ids)
  = 101 database queries           = 3 database queries
```

### 5.2 Query Depth Limiting

Prevents deeply nested queries that could cause excessive joins or recursive resolution.

```python
# services/graphql-api/app/extensions/depth_limiter.py

from strawberry.extensions import SchemaExtension

class QueryDepthLimiter(SchemaExtension):
    """
    Rejects queries deeper than max_depth to prevent abuse.

    Example (depth = 3):
    {
      trades {           # depth 1
        symbolDetails {  # depth 2
          dailySummary {  # depth 3
            ...           # depth 4 -> REJECTED
          }
        }
      }
    }
    """
    def __init__(self, max_depth: int = 5):
        self.max_depth = max_depth

    def on_operation(self):
        depth = self._calculate_depth(self.execution_context.query)
        if depth > self.max_depth:
            raise ValueError(
                f"Query depth {depth} exceeds maximum allowed depth {self.max_depth}"
            )
        yield
```

### 5.3 Query Complexity Analysis

Assigns cost to each field and rejects queries that exceed a total complexity budget.

```python
# services/graphql-api/app/extensions/complexity_analyzer.py

FIELD_COSTS = {
    "trades": 10,                # List query, hits Silver layer
    "dailySummary": 5,           # Aggregated, often cached
    "marketOverview": 8,         # Multiple sub-queries
    "traderPerformance": 7,      # Heavy aggregation
    "orderBook": 3,              # Redis lookup, fast
    "symbolDetails": 1,          # Batched via DataLoader
    "portfolioPositions": 5,     # Moderate query
}

# Multiplier for list fields based on requested count
LIST_MULTIPLIER = {
    "first": lambda n: max(1, n / 10),   # first:100 = 10x cost
}

class QueryComplexityAnalyzer(SchemaExtension):
    def __init__(self, max_complexity: int = 100):
        self.max_complexity = max_complexity

    def on_operation(self):
        complexity = self._calculate_complexity(self.execution_context.query)
        if complexity > self.max_complexity:
            raise ValueError(
                f"Query complexity {complexity} exceeds maximum {self.max_complexity}. "
                f"Simplify your query or reduce pagination limits."
            )
        yield
```

### 5.4 Response Caching

```python
# HTTP-level caching headers for GET requests (persisted queries)
@app.middleware("http")
async def add_cache_headers(request: Request, call_next):
    response = await call_next(request)

    if request.url.path == "/graphql" and request.method == "GET":
        # Parse operation name to determine cache policy
        operation = request.query_params.get("operationName", "")

        cache_policies = {
            "MarketOverview": "public, max-age=300",       # 5 min
            "Symbols": "public, max-age=3600",              # 1 hour
            "DailySummary": "public, max-age=300",          # 5 min
            "TradeById": "private, max-age=60",             # 1 min
        }

        policy = cache_policies.get(operation, "private, no-cache")
        response.headers["Cache-Control"] = policy

    return response
```

### 5.5 DuckDB Query Optimization

```sql
-- Predicate pushdown: Iceberg metadata filtering
-- DuckDB pushes WHERE clauses into Iceberg's manifest scanning,
-- skipping entire data files that don't match.

-- GOOD: Partition-aware query (trade_date is partition key)
SELECT * FROM iceberg_scan('s3://trade-lakehouse/silver/trades_enriched')
WHERE trade_date = '2024-03-15'
  AND symbol = 'AAPL';
-- Scans only 1 partition, skips ~364 others

-- BAD: Full table scan
SELECT * FROM iceberg_scan('s3://trade-lakehouse/silver/trades_enriched')
WHERE EXTRACT(month FROM timestamp) = 3;
-- Cannot use partition pruning on derived expressions

-- Use column projection to minimize I/O
-- GOOD: Only read required columns
SELECT symbol, price, quantity, timestamp
FROM iceberg_scan('s3://trade-lakehouse/silver/trades_enriched')
WHERE trade_date = '2024-03-15';

-- Leverage Iceberg's sort order for range queries
-- Tables sorted by (symbol, timestamp) enable efficient range scans
```

**DuckDB Configuration for API:**

```python
# services/graphql-api/app/db/config.py

DUCKDB_CONFIG = {
    "threads": 4,                          # Per connection
    "memory_limit": "2GB",                 # Per connection
    "temp_directory": "/tmp/duckdb",
    "enable_object_cache": True,           # Cache Iceberg metadata
    "enable_http_metadata_cache": True,    # Cache S3 metadata responses
    "s3_region": "us-east-1",
    "s3_access_key_id": "${AWS_ACCESS_KEY_ID}",
    "s3_secret_access_key": "${AWS_SECRET_ACCESS_KEY}",
}
```

---

## 6. API Documentation

### Auto-Generated Documentation

Strawberry GraphQL provides automatic documentation through the GraphiQL playground, which is
available at the `/graphql` endpoint when accessed via a browser (GET request).

**Features:**

- **Interactive Query Editor**: Write and execute queries directly in the browser
- **Schema Explorer**: Browse all types, queries, mutations, and subscriptions
- **Auto-complete**: IntelliSense-style suggestions as you type queries
- **Documentation Panel**: Inline docs from Python docstrings on resolvers
- **Query History**: Saved queries persist across sessions

**Configuration:**

```python
# services/graphql-api/app/main.py

graphql_app = GraphQLRouter(
    schema,
    context_getter=get_context,
    graphiql=settings.ENABLE_GRAPHIQL,       # True in dev/staging, False in prod
    allow_queries_via_get=True,               # Enable GET for persisted queries
)
```

**Additional Documentation Endpoints:**

```python
# Expose schema as SDL for external tooling
@app.get("/graphql/schema.graphql")
async def get_schema_sdl():
    return Response(
        content=str(schema),
        media_type="text/plain",
    )

# Expose schema as JSON introspection result
@app.get("/graphql/schema.json")
async def get_schema_json():
    result = await schema.execute("{ __schema { types { name } } }")
    return result.data
```

---

## 7. Implementation Steps & File Structure

```
services/graphql-api/
|-- Dockerfile
|-- docker-compose.yml
|-- pyproject.toml
|-- requirements.txt
|-- README.md
|
|-- app/
|   |-- __init__.py
|   |-- main.py                          # FastAPI app entry point
|   |-- config.py                        # Settings (env vars, feature flags)
|   |-- context.py                       # GraphQL context factory (loaders, pools)
|   |
|   |-- schema/
|   |   |-- __init__.py                  # Re-exports schema, Query, Mutation, Subscription
|   |   |-- types.py                     # All Strawberry type definitions
|   |   |-- inputs.py                    # Input types and filter definitions
|   |   |-- pagination.py               # Relay-style cursor pagination helpers
|   |   |-- query.py                     # Query root type
|   |   |-- mutation.py                  # Mutation root type
|   |   |-- subscription.py             # Subscription root type
|   |   |-- enums.py                     # Enum definitions
|   |
|   |-- resolvers/
|   |   |-- __init__.py
|   |   |-- trade.py                     # Trade query resolvers
|   |   |-- daily_summary.py            # DailySummary resolvers
|   |   |-- market_overview.py          # MarketOverview resolver
|   |   |-- trader_performance.py       # TraderPerformance resolver
|   |   |-- symbol.py                    # Symbol list/detail resolvers
|   |   |-- order_book.py               # OrderBook resolver (Redis-backed)
|   |   |-- portfolio.py                # PortfolioPosition resolver
|   |   |-- watchlist.py                # Watchlist mutation resolvers
|   |
|   |-- loaders/
|   |   |-- __init__.py
|   |   |-- symbol_loader.py            # Batch symbol loader
|   |   |-- price_loader.py             # Batch latest price loader
|   |   |-- trader_loader.py            # Batch trader profile loader
|   |   |-- summary_loader.py           # Batch daily summary loader
|   |
|   |-- db/
|   |   |-- __init__.py
|   |   |-- duckdb_pool.py              # Async DuckDB connection pool
|   |   |-- postgres.py                 # Asyncpg PostgreSQL pool
|   |   |-- config.py                   # DB configuration constants
|   |
|   |-- cache/
|   |   |-- __init__.py
|   |   |-- redis_cache.py              # Redis caching utilities
|   |   |-- cache_keys.py               # Cache key namespace constants
|   |
|   |-- streaming/
|   |   |-- __init__.py
|   |   |-- kafka_consumer.py           # Kafka consumer factory for subscriptions
|   |   |-- message_parser.py           # Kafka message deserialization
|   |
|   |-- middleware/
|   |   |-- __init__.py
|   |   |-- auth.py                      # JWT authentication middleware
|   |   |-- rate_limit.py               # Rate limiting middleware
|   |   |-- logging.py                  # Request/response logging
|   |   |-- cors.py                      # CORS configuration
|   |
|   |-- extensions/
|   |   |-- __init__.py
|   |   |-- depth_limiter.py            # Query depth limiting extension
|   |   |-- complexity_analyzer.py      # Query complexity analysis
|   |   |-- tracing.py                  # OpenTelemetry tracing extension
|   |
|   |-- auth/
|   |   |-- __init__.py
|   |   |-- permissions.py              # Role checking utilities
|   |   |-- models.py                   # UserContext, JWT models
|   |
|   |-- services/
|   |   |-- __init__.py
|   |   |-- symbol_service.py           # Symbol validation and lookup
|   |   |-- watchlist_service.py        # Watchlist CRUD operations
|   |
|   |-- utils/
|       |-- __init__.py
|       |-- sql_builder.py              # Dynamic SQL query builder
|       |-- validators.py               # Input validation helpers
|       |-- converters.py               # Type conversion utilities
|
|-- tests/
|   |-- __init__.py
|   |-- conftest.py                      # Pytest fixtures (test DB, mock context)
|   |-- test_schema/
|   |   |-- test_types.py               # Type definition tests
|   |   |-- test_pagination.py          # Cursor encoding/decoding tests
|   |
|   |-- test_resolvers/
|   |   |-- test_trade_resolver.py      # Trade resolver unit tests
|   |   |-- test_summary_resolver.py    # Summary resolver unit tests
|   |   |-- test_market_overview.py     # Market overview tests
|   |   |-- test_order_book.py          # Order book resolver tests
|   |   |-- test_portfolio.py           # Portfolio resolver tests
|   |
|   |-- test_loaders/
|   |   |-- test_symbol_loader.py       # DataLoader batching tests
|   |   |-- test_price_loader.py        # Price loader tests
|   |
|   |-- test_middleware/
|   |   |-- test_auth.py                # JWT auth tests
|   |   |-- test_rate_limit.py          # Rate limit tests
|   |
|   |-- test_integration/
|   |   |-- test_full_query.py          # End-to-end query tests
|   |   |-- test_subscriptions.py       # WebSocket subscription tests
|   |   |-- test_mutations.py           # Mutation tests
|   |
|   |-- test_performance/
|       |-- locustfile.py               # Load testing with Locust
|       |-- test_query_complexity.py    # Complexity limit tests
|
|-- scripts/
    |-- seed_data.py                     # Seed test data for development
    |-- generate_schema_sdl.py          # Export schema to SDL file
```

**Implementation Order:**

| Phase | Files                                         | Description                        |
|-------|-----------------------------------------------|------------------------------------|
| 1     | `main.py`, `config.py`, `context.py`          | App bootstrap, configuration       |
| 2     | `schema/types.py`, `schema/enums.py`          | Core type definitions              |
| 3     | `schema/inputs.py`, `schema/pagination.py`    | Inputs and pagination              |
| 4     | `db/duckdb_pool.py`, `db/postgres.py`         | Database connections               |
| 5     | `resolvers/trade.py`, `resolvers/symbol.py`   | First query resolvers              |
| 6     | `schema/query.py`                             | Wire up Query root                 |
| 7     | `loaders/*.py`                                | DataLoader implementations         |
| 8     | `cache/redis_cache.py`                        | Caching layer                      |
| 9     | `middleware/auth.py`, `auth/permissions.py`    | Authentication                     |
| 10    | `resolvers/daily_summary.py`, etc.            | Remaining resolvers                |
| 11    | `schema/mutation.py`, `resolvers/watchlist.py` | Mutations                         |
| 12    | `streaming/kafka_consumer.py`                 | Kafka consumer for subscriptions   |
| 13    | `schema/subscription.py`                      | Subscriptions                      |
| 14    | `extensions/*.py`                             | Security extensions                |
| 15    | `middleware/rate_limit.py`                    | Rate limiting                      |
| 16    | `tests/**`                                    | Full test suite                    |

---

## 8. Testing Strategy

### 8.1 Schema Tests

Verify that the GraphQL schema compiles correctly and matches expected structure.

```python
# tests/test_schema/test_types.py

import strawberry
from app.schema import schema

def test_schema_compiles():
    """Schema should compile without errors."""
    sdl = str(schema)
    assert "type Trade" in sdl
    assert "type Query" in sdl
    assert "type Mutation" in sdl
    assert "type Subscription" in sdl

def test_trade_type_fields():
    """Trade type should have all required fields."""
    trade_type = schema.get_type_by_name("Trade")
    field_names = {f.name for f in trade_type.fields}
    assert "id" in field_names
    assert "symbol" in field_names
    assert "price" in field_names
    assert "quantity" in field_names
    assert "symbolDetails" in field_names  # camelCase in GQL

def test_query_root_fields():
    """Query root should expose all expected endpoints."""
    query_type = schema.get_type_by_name("Query")
    field_names = {f.name for f in query_type.fields}
    expected = {
        "trades", "tradeById", "dailySummary", "marketOverview",
        "traderPerformance", "symbols", "orderBook", "portfolioPositions",
    }
    assert expected.issubset(field_names)

def test_pagination_types():
    """Connection types should follow Relay spec."""
    trade_conn = schema.get_type_by_name("TradeConnection")
    field_names = {f.name for f in trade_conn.fields}
    assert "edges" in field_names
    assert "pageInfo" in field_names
    assert "totalCount" in field_names
```

### 8.2 Resolver Unit Tests

```python
# tests/test_resolvers/test_trade_resolver.py

import pytest
from unittest.mock import AsyncMock, MagicMock
from app.resolvers.trade import TradeResolver
from app.schema.inputs import TradeFilterInput, DateRangeInput
from datetime import date

@pytest.fixture
def mock_context():
    ctx = MagicMock()
    ctx.duckdb = AsyncMock()
    ctx.cache = AsyncMock()
    ctx.cache.get.return_value = None  # No cache hit
    return ctx

@pytest.mark.asyncio
async def test_trades_basic_query(mock_context):
    """Should return trades with default pagination."""
    mock_context.duckdb.execute_query.return_value = [
        {"id": "1", "symbol": "AAPL", "price": 150.0, "quantity": 100},
    ]
    resolver = TradeResolver(mock_context)
    result = await resolver.resolve(filter=None, sort=None, first=20, after=None)

    assert len(result.edges) == 1
    assert result.edges[0].node.symbol == "AAPL"
    assert result.page_info.has_next_page is False

@pytest.mark.asyncio
async def test_trades_with_filter(mock_context):
    """Should pass filter predicates to DuckDB query."""
    filter_input = TradeFilterInput(
        symbol="AAPL",
        trade_type=TradeType.BUY,
        date_range=DateRangeInput(start=date(2024, 1, 1), end=date(2024, 3, 31)),
    )
    mock_context.duckdb.execute_query.return_value = []
    resolver = TradeResolver(mock_context)
    await resolver.resolve(filter=filter_input, sort=None, first=20, after=None)

    call_args = mock_context.duckdb.execute_query.call_args
    query = call_args[0][0]
    params = call_args[0][1]
    assert "symbol = ?" in query
    assert "trade_type = ?" in query
    assert "AAPL" in params
    assert "BUY" in params

@pytest.mark.asyncio
async def test_trades_pagination(mock_context):
    """Should correctly handle cursor-based pagination."""
    # Return 21 rows (first=20 + 1 to detect has_next)
    mock_context.duckdb.execute_query.return_value = [
        {"id": str(i), "symbol": "AAPL", "price": 150.0, "quantity": 100}
        for i in range(21)
    ]
    resolver = TradeResolver(mock_context)
    result = await resolver.resolve(filter=None, sort=None, first=20, after=None)

    assert len(result.edges) == 20
    assert result.page_info.has_next_page is True
    assert result.page_info.start_cursor is not None
    assert result.page_info.end_cursor is not None
```

### 8.3 Integration Tests

```python
# tests/test_integration/test_full_query.py

import pytest
from httpx import AsyncClient
from app.main import app

@pytest.fixture
async def client():
    async with AsyncClient(app=app, base_url="http://test") as ac:
        yield ac

@pytest.mark.asyncio
async def test_market_overview_query(client):
    query = """
    query {
        marketOverview(targetDate: "2024-03-15") {
            totalTrades
            totalVolume
            advancingSymbols
            decliningSymbols
            sectorPerformance {
                sector
                avgChangePercent
            }
        }
    }
    """
    response = await client.post("/graphql", json={"query": query})
    assert response.status_code == 200
    data = response.json()
    assert "errors" not in data
    assert data["data"]["marketOverview"]["totalTrades"] > 0

@pytest.mark.asyncio
async def test_auth_required_query(client):
    """Trader performance requires analyst+ role."""
    query = """
    query {
        traderPerformance(traderId: "T-001") {
            totalTrades
            pnl
        }
    }
    """
    # No auth header
    response = await client.post("/graphql", json={"query": query})
    data = response.json()
    assert "errors" in data
    assert "permission" in data["errors"][0]["message"].lower()

@pytest.mark.asyncio
async def test_subscription_connection(client):
    """WebSocket subscription should establish and receive messages."""
    async with client.websocket_connect("/graphql") as ws:
        await ws.send_json({
            "type": "connection_init",
            "payload": {"Authorization": "Bearer test-token"},
        })
        init_ack = await ws.receive_json()
        assert init_ack["type"] == "connection_ack"

        await ws.send_json({
            "id": "1",
            "type": "subscribe",
            "payload": {
                "query": "subscription { onNewTrade(symbol: \"AAPL\") { id symbol price } }"
            },
        })
        # Message receipt tested with mock Kafka producer
```

### 8.4 Load Tests (Locust)

```python
# tests/test_performance/locustfile.py

from locust import HttpUser, task, between

class GraphQLUser(HttpUser):
    wait_time = between(0.5, 2)
    host = "http://localhost:8000"

    def on_start(self):
        # Obtain JWT token
        self.token = self._get_token()
        self.headers = {"Authorization": f"Bearer {self.token}"}

    @task(weight=5)
    def query_trades(self):
        self.client.post("/graphql", json={
            "query": """
            query {
                trades(filter: { symbol: "AAPL" }, first: 20) {
                    edges { node { id symbol price quantity } }
                    pageInfo { hasNextPage endCursor }
                }
            }
            """
        }, headers=self.headers)

    @task(weight=3)
    def query_market_overview(self):
        self.client.post("/graphql", json={
            "query": """
            query {
                marketOverview {
                    totalTrades totalVolume
                    topGainers { symbol changePercent }
                }
            }
            """
        }, headers=self.headers)

    @task(weight=2)
    def query_daily_summary(self):
        self.client.post("/graphql", json={
            "query": """
            query {
                dailySummary(symbol: "AAPL", dateRange: { start: "2024-01-01", end: "2024-03-31" }) {
                    edges { node { tradeDate open close volume } }
                }
            }
            """
        }, headers=self.headers)

    @task(weight=1)
    def query_order_book(self):
        self.client.post("/graphql", json={
            "query": """
            query {
                orderBook(symbol: "AAPL") {
                    bids { price quantity }
                    asks { price quantity }
                    spread
                }
            }
            """
        }, headers=self.headers)
```

**Performance Targets:**

| Metric                  | Target              |
|-------------------------|---------------------|
| P50 latency             | < 50ms              |
| P95 latency             | < 200ms             |
| P99 latency             | < 500ms             |
| Throughput              | > 1,000 req/s       |
| Concurrent connections  | > 500 WebSocket     |
| Error rate              | < 0.1%              |
| Cache hit ratio         | > 70% (warm cache)  |

---

## Appendix: Example Queries

### A. Fetch Recent Trades with Symbol Details

```graphql
query RecentTrades {
  trades(
    filter: { symbol: "AAPL", tradeType: BUY }
    sort: { field: "timestamp", direction: DESC }
    first: 10
  ) {
    totalCount
    edges {
      cursor
      node {
        id
        symbol
        price
        quantity
        totalValue
        timestamp
        orderType
        symbolDetails {
          companyName
          sector
          latestPrice
        }
      }
    }
    pageInfo {
      hasNextPage
      endCursor
    }
  }
}
```

### B. Market Overview with Sector Breakdown

```graphql
query MarketSnapshot {
  marketOverview(targetDate: "2024-03-15") {
    date
    totalTrades
    totalVolume
    totalValue
    advancingSymbols
    decliningSymbols
    mostActive {
      ticker
      companyName
    }
    topGainers {
      symbol
      changePercent
      close
    }
    sectorPerformance {
      sector
      avgChangePercent
      totalVolume
    }
  }
}
```

### C. Real-Time Trade Subscription

```graphql
subscription LiveTrades {
  onNewTrade(symbol: "AAPL") {
    id
    symbol
    price
    quantity
    tradeType
    timestamp
    isInstitutional
  }
}
```
