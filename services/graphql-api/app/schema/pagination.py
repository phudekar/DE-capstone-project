"""Relay-style cursor pagination helpers."""

import base64
import json
import logging
from typing import Optional, TypeVar

import strawberry

from app.schema.types import DailySummary, MinuteCandle, Symbol, Trade

T = TypeVar("T")
logger = logging.getLogger(__name__)


def encode_cursor(offset: int) -> str:
    return base64.b64encode(json.dumps({"offset": offset}).encode()).decode()


def decode_cursor(cursor: str) -> int:
    """Decode a base64-encoded cursor back to an integer offset.

    Returns 0 as a safe default if the cursor is malformed, expired, or
    otherwise undecodable — this restarts pagination from the beginning
    rather than raising a 500 error to the client.
    """
    try:
        data = json.loads(base64.b64decode(cursor.encode()).decode())
        return data["offset"]
    except Exception:
        logger.warning("Malformed pagination cursor, defaulting to offset 0: %r", cursor)
        return 0


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
class MinuteCandleEdge:
    node: MinuteCandle
    cursor: str


@strawberry.type
class MinuteCandleConnection:
    edges: list[MinuteCandleEdge]
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
