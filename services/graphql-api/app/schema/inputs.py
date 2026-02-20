"""Strawberry input types and filter definitions."""

from __future__ import annotations

import strawberry
from datetime import date
from typing import Optional


@strawberry.input
class DateRangeInput:
    start: date
    end: date


@strawberry.input
class TradeFilterInput:
    symbol: Optional[str] = None
    symbols: Optional[list[str]] = None
    aggressor_side: Optional[str] = None  # "BUY" or "SELL"
    min_price: Optional[float] = None
    max_price: Optional[float] = None
    min_quantity: Optional[int] = None
    max_quantity: Optional[int] = None
    date_range: Optional[DateRangeInput] = None


@strawberry.input
class SymbolFilterInput:
    sector: Optional[str] = None
    market_cap_category: Optional[str] = None
    is_current: Optional[bool] = True
    search: Optional[str] = None


@strawberry.input
class WatchlistInput:
    name: str
    symbols: list[str] = strawberry.field(default_factory=list)


@strawberry.input
class AddToWatchlistInput:
    watchlist_id: strawberry.ID
    symbols: list[str]
