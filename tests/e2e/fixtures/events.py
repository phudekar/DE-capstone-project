"""Factory helpers that produce raw WebSocket JSON payloads exactly as DE-Stock
emits them — used across all e2e stage tests.

All helpers return plain Python dicts (as if parsed from JSON), so the same
data can be fed into:
  - validate_message()  (Kafka bridge validation)
  - route_event()       (Kafka bridge routing)
  - BronzeWriter._parse_trade / _parse_orderbook
  - Silver / Gold DuckDB simulations
  - GraphQL resolver mocks
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


# ─── TradeExecuted ─────────────────────────────────────────────────────────────

def make_trade_event(
    symbol: str = "AAPL",
    price: float = 182.50,
    quantity: int = 100,
    aggressor_side: str = "Buy",
    timestamp: str | None = None,
) -> dict:
    """Return a raw DE-Stock WebSocket envelope for a TradeExecuted event."""
    return {
        "event_type": "TradeExecuted",
        "timestamp": timestamp or _now(),
        "data": {
            "trade_id": f"T-{uuid.uuid4().hex[:12]}",
            "symbol": symbol,
            "price": price,
            "quantity": quantity,
            "buyer_order_id": f"O-{uuid.uuid4().hex[:8]}",
            "seller_order_id": f"O-{uuid.uuid4().hex[:8]}",
            "buyer_agent_id": f"A-{uuid.uuid4().hex[:6]}",
            "seller_agent_id": f"A-{uuid.uuid4().hex[:6]}",
            "aggressor_side": aggressor_side,
        },
    }


def make_trade_batch(
    symbols: list[str],
    n_per_symbol: int = 5,
    base_price: float = 100.0,
) -> list[dict]:
    """Return a list of TradeExecuted envelopes across multiple symbols."""
    import random
    rng = random.Random(42)
    events = []
    for sym in symbols:
        price = base_price
        for _ in range(n_per_symbol):
            price *= rng.uniform(0.995, 1.005)
            events.append(make_trade_event(
                symbol=sym,
                price=round(price, 4),
                quantity=rng.randint(10, 500),
                aggressor_side=rng.choice(["Buy", "Sell"]),
            ))
    return events


# ─── OrderBookSnapshot ─────────────────────────────────────────────────────────

def make_orderbook_event(
    symbol: str = "AAPL",
    mid_price: float = 182.50,
    n_levels: int = 5,
    timestamp: str | None = None,
) -> dict:
    """Return a raw DE-Stock WebSocket envelope for an OrderBookSnapshot event."""
    spread = 0.02
    bids = [
        {"price": round(mid_price - spread / 2 - i * 0.10, 2), "quantity": 100 + i * 20}
        for i in range(n_levels)
    ]
    asks = [
        {"price": round(mid_price + spread / 2 + i * 0.10, 2), "quantity": 80 + i * 15}
        for i in range(n_levels)
    ]
    return {
        "event_type": "OrderBookSnapshot",
        "timestamp": timestamp or _now(),
        "data": {
            "symbol": symbol,
            "bids": bids,
            "asks": asks,
            "sequence_number": 1001,
        },
    }


# ─── QuoteUpdate ───────────────────────────────────────────────────────────────

def make_quote_event(symbol: str = "AAPL", mid_price: float = 182.50) -> dict:
    spread = 0.05
    return {
        "event_type": "QuoteUpdate",
        "timestamp": _now(),
        "data": {
            "symbol": symbol,
            "bid_price": round(mid_price - spread / 2, 4),
            "bid_size": 200,
            "ask_price": round(mid_price + spread / 2, 4),
            "ask_size": 150,
            "spread": spread,
            "mid_price": mid_price,
        },
    }


# ─── OrderPlaced ───────────────────────────────────────────────────────────────

def make_order_placed_event(symbol: str = "AAPL") -> dict:
    return {
        "event_type": "OrderPlaced",
        "timestamp": _now(),
        "data": {
            "order_id": f"O-{uuid.uuid4().hex[:8]}",
            "symbol": symbol,
            "side": "Buy",
            "order_type": "Limit",
            "price": 182.00,
            "quantity": 50,
            "agent_id": f"A-{uuid.uuid4().hex[:6]}",
            "agent_type": "Retail",
        },
    }


# ─── MarketStats ───────────────────────────────────────────────────────────────

def make_market_stats_event(symbol: str = "AAPL", volume: int = 500_000) -> dict:
    return {
        "event_type": "MarketStats",
        "timestamp": _now(),
        "data": {
            "symbol": symbol,
            "open": 180.00,
            "high": 185.00,
            "low": 179.50,
            "last": 182.50,
            "volume": volume,
            "trade_count": 2500,
            "vwap": 182.10,
            "turnover": volume * 182.10,
        },
    }


# ─── Invalid events ────────────────────────────────────────────────────────────

def make_invalid_event() -> dict:
    """An envelope with an unknown event_type (should be routed to DLQ)."""
    return {
        "event_type": "UnknownEvent",
        "timestamp": _now(),
        "data": {"foo": "bar"},
    }


def make_malformed_trade_event() -> dict:
    """A TradeExecuted with a missing required field (should fail validation)."""
    return {
        "event_type": "TradeExecuted",
        "timestamp": _now(),
        "data": {
            # missing trade_id, buyer_order_id, etc.
            "symbol": "AAPL",
            "price": 182.50,
        },
    }


# ─── Reference data ────────────────────────────────────────────────────────────

SYMBOL_REFERENCE = {
    "AAPL": {"company_name": "Apple Inc.", "sector": "Technology"},
    "MSFT": {"company_name": "Microsoft Corp.", "sector": "Technology"},
    "GOOG": {"company_name": "Alphabet Inc.", "sector": "Technology"},
    "AMZN": {"company_name": "Amazon.com Inc.", "sector": "Consumer Discretionary"},
    "TSLA": {"company_name": "Tesla Inc.", "sector": "Consumer Discretionary"},
    "JPM":  {"company_name": "JPMorgan Chase & Co.", "sector": "Financials"},
}
