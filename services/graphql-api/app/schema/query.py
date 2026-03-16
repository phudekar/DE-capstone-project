"""GraphQL Query root type."""

from __future__ import annotations

from datetime import date
from typing import Optional

import strawberry
from strawberry.types import Info

from app.resolvers.daily_summary import DailySummaryResolver
from app.resolvers.market_overview import MarketOverviewResolver
from app.resolvers.order_book import OrderBookResolver
from app.resolvers.symbol import SymbolResolver
from app.resolvers.trade import TradeResolver
from app.schema.inputs import DateRangeInput, SymbolFilterInput, TradeFilterInput
from app.schema.pagination import DailySummaryConnection, SymbolConnection, TradeConnection
from app.schema.types import MarketOverview, OrderBookSnapshot, Trade


@strawberry.type
class Query:
    @strawberry.field(description="Paginated trade list with optional filtering.")
    async def trades(
        self,
        info: Info,
        filter: Optional[TradeFilterInput] = None,
        first: Optional[int] = 20,
        after: Optional[str] = None,
    ) -> TradeConnection:
        resolver = TradeResolver(info.context.engine)
        return await resolver.resolve(filter=filter, first=first or 20, after=after)

    @strawberry.field(description="Fetch a single trade by its unique ID.")
    async def trade_by_id(self, info: Info, id: strawberry.ID) -> Optional[Trade]:
        resolver = TradeResolver(info.context.engine)
        return await resolver.resolve_by_id(str(id))

    @strawberry.field(description="Daily OHLCV summary for a symbol over a date range.")
    async def daily_summary(
        self,
        info: Info,
        symbol: str,
        date_range: DateRangeInput,
        first: Optional[int] = 30,
        after: Optional[str] = None,
    ) -> DailySummaryConnection:
        resolver = DailySummaryResolver(info.context.engine, info.context.cache)
        return await resolver.resolve(symbol=symbol, date_range=date_range, first=first or 30, after=after)

    @strawberry.field(description="Market-wide overview for a given date.")
    async def market_overview(self, info: Info, target_date: Optional[date] = None) -> MarketOverview:
        resolver = MarketOverviewResolver(info.context.engine, info.context.cache)
        return await resolver.resolve(target_date=target_date or date.today())

    @strawberry.field(description="List instruments/symbols with optional filtering.")
    async def symbols(
        self,
        info: Info,
        filter: Optional[SymbolFilterInput] = None,
        first: Optional[int] = 50,
        after: Optional[str] = None,
    ) -> SymbolConnection:
        resolver = SymbolResolver(info.context.engine, info.context.cache)
        return await resolver.resolve(filter=filter, first=first or 50, after=after)

    @strawberry.field(description="Latest orderbook snapshot for a symbol.")
    async def order_book(self, info: Info, symbol: str) -> Optional[OrderBookSnapshot]:
        resolver = OrderBookResolver(info.context.engine)
        return await resolver.resolve(symbol=symbol)
