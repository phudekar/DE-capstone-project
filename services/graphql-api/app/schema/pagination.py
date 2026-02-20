"""Relay-style cursor pagination helpers."""

from __future__ import annotations

import base64
import json
from typing import Generic, TypeVar, Optional

import strawberry

from app.schema.types import Trade, DailySummary, Symbol

T = TypeVar("T")


def encode_cursor(offset: int) -> str:
    return base64.b64encode(json.dumps({"offset": offset}).encode()).decode()


def decode_cursor(cursor: str) -> int:
    data = json.loads(base64.b64decode(cursor.encode()).decode())
    return data["offset"]


@strawberry.type
class PageInfo:
    has_next_page: bool
    has_previous_page: bool
    start_cursor: Optional[str] = None
    end_cursor: Optional[str] = None


@strawberry.type
class TradeEdge:
    node: Trade
    cursor: str


@strawberry.type
class TradeConnection:
    edges: list[TradeEdge]
    page_info: PageInfo
    total_count: int


@strawberry.type
class DailySummaryEdge:
    node: DailySummary
    cursor: str


@strawberry.type
class DailySummaryConnection:
    edges: list[DailySummaryEdge]
    page_info: PageInfo
    total_count: int


@strawberry.type
class SymbolEdge:
    node: Symbol
    cursor: str


@strawberry.type
class SymbolConnection:
    edges: list[SymbolEdge]
    page_info: PageInfo
    total_count: int
