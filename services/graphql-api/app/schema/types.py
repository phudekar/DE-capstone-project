"""Strawberry GraphQL type definitions aligned with actual Silver/Gold Iceberg schemas."""

from __future__ import annotations

import strawberry
from datetime import date, datetime
from strawberry.types import Info
from typing import Optional


@strawberry.type
class Trade:
    """A single trade from the Silver layer (trades_enriched table)."""

    trade_id: strawberry.ID
    symbol: str
    price: float
    quantity: int
    buyer_agent_id: str
    seller_agent_id: str
    aggressor_side: str  # "BUY" or "SELL"
    timestamp: datetime
    company_name: Optional[str] = None
    sector: Optional[str] = None

    @strawberry.field(description="Batch-loaded symbol details (DataLoader, no N+1).")
    async def symbol_details(self, info: Info) -> Optional["Symbol"]:
        return await info.context.symbol_loader.load(self.symbol)


@strawberry.type
class Symbol:
    """An instrument/symbol from the dimensions layer (dim_symbol table)."""

    symbol: str
    company_name: str
    sector: str
    market_cap_category: str
    is_current: bool


@strawberry.type
class DailySummary:
    """Daily OHLCV aggregate from the Gold layer (daily_trading_summary table)."""

    symbol: str
    trading_date: date
    open_price: float
    close_price: float
    high_price: float
    low_price: float
    vwap: float
    total_volume: int
    trade_count: int
    total_value: float
    company_name: Optional[str] = None
    sector: Optional[str] = None

    @strawberry.field
    def price_range(self) -> float:
        return self.high_price - self.low_price

    @strawberry.field
    def price_change(self) -> float:
        return self.close_price - self.open_price

    @strawberry.field
    def price_change_pct(self) -> float:
        if self.open_price == 0:
            return 0.0
        return (self.close_price - self.open_price) / self.open_price * 100


@strawberry.type
class MarketOverview:
    """Market-wide summary for a given date."""

    trading_date: date
    total_trades: int
    total_volume: int
    total_value: float
    unique_symbols: int
    advancing: int
    declining: int
    unchanged: int
    top_gainers: list[DailySummary]
    top_losers: list[DailySummary]
    most_active: list[DailySummary]


@strawberry.type
class OrderBookSnapshot:
    """Latest orderbook snapshot from the Silver layer."""

    symbol: str
    timestamp: datetime
    best_bid_price: Optional[float] = None
    best_bid_qty: Optional[int] = None
    best_ask_price: Optional[float] = None
    best_ask_qty: Optional[int] = None
    bid_depth: int = 0
    ask_depth: int = 0
    spread: Optional[float] = None
    mid_price: Optional[float] = None
    company_name: Optional[str] = None
    sector: Optional[str] = None


@strawberry.type
class Watchlist:
    """User watchlist (persisted in-memory for local dev)."""

    id: strawberry.ID
    name: str
    user_id: str
    symbols: list[str]
    created_at: datetime
    updated_at: datetime


@strawberry.type
class TradeAlert:
    """Real-time alert from the Kafka trade-alerts topic."""

    symbol: str
    alert_type: str
    price: float
    threshold: float
    triggered_at: datetime
    message: str
