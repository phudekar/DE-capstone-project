"""Stage 1 — WebSocket event validation.

Verifies that every event type emitted by DE-Stock can be:
  1. Parsed from a raw JSON dict into the correct Pydantic model.
  2. Rejected (None) when required fields are missing.

No Kafka, no Docker — pure model validation.
"""

import sys
from pathlib import Path

ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(ROOT / "libs/common/src"))

import pytest
from pydantic import TypeAdapter

from de_common.models.events import MarketEvent
from de_common.models.enums import Side, AgentType, OrderType
from tests.e2e.fixtures.events import (
    make_trade_event,
    make_orderbook_event,
    make_quote_event,
    make_order_placed_event,
    make_market_stats_event,
    make_invalid_event,
    make_malformed_trade_event,
)

_adapter = TypeAdapter(MarketEvent)


def _parse(raw: dict):
    return _adapter.validate_python(raw)


# ── TradeExecuted ──────────────────────────────────────────────────────────────

def test_trade_event_parses():
    raw = make_trade_event(symbol="AAPL", price=182.50, quantity=100)
    event = _parse(raw)
    assert event.event_type == "TradeExecuted"
    assert event.data.symbol == "AAPL"
    assert event.data.price == pytest.approx(182.50)
    assert event.data.quantity == 100


def test_trade_event_aggressor_side_buy():
    raw = make_trade_event(aggressor_side="Buy")
    event = _parse(raw)
    assert event.data.aggressor_side == Side.BUY


def test_trade_event_aggressor_side_sell():
    raw = make_trade_event(aggressor_side="Sell")
    event = _parse(raw)
    assert event.data.aggressor_side == Side.SELL


def test_trade_event_has_all_required_ids():
    raw = make_trade_event()
    event = _parse(raw)
    assert event.data.trade_id
    assert event.data.buyer_order_id
    assert event.data.seller_order_id
    assert event.data.buyer_agent_id
    assert event.data.seller_agent_id


def test_trade_event_timestamp_parsed():
    from datetime import datetime
    raw = make_trade_event()
    event = _parse(raw)
    assert isinstance(event.timestamp, datetime)


# ── OrderBookSnapshot ──────────────────────────────────────────────────────────

def test_orderbook_event_parses():
    raw = make_orderbook_event(symbol="MSFT", mid_price=310.00, n_levels=3)
    event = _parse(raw)
    assert event.event_type == "OrderBookSnapshot"
    assert event.data.symbol == "MSFT"
    assert len(event.data.bids) == 3
    assert len(event.data.asks) == 3


def test_orderbook_bids_descending_price():
    raw = make_orderbook_event(n_levels=5)
    event = _parse(raw)
    prices = [b.price for b in event.data.bids]
    assert prices == sorted(prices, reverse=True)


def test_orderbook_asks_ascending_price():
    raw = make_orderbook_event(n_levels=5)
    event = _parse(raw)
    prices = [a.price for a in event.data.asks]
    assert prices == sorted(prices)


def test_orderbook_best_bid_lt_best_ask():
    raw = make_orderbook_event()
    event = _parse(raw)
    assert event.data.bids[0].price < event.data.asks[0].price


# ── QuoteUpdate ────────────────────────────────────────────────────────────────

def test_quote_event_parses():
    raw = make_quote_event(symbol="GOOG", mid_price=140.00)
    event = _parse(raw)
    assert event.event_type == "QuoteUpdate"
    assert event.data.symbol == "GOOG"
    assert event.data.bid_price < event.data.ask_price


def test_quote_spread_positive():
    raw = make_quote_event()
    event = _parse(raw)
    assert event.data.spread > 0


def test_quote_mid_price_between_bid_ask():
    raw = make_quote_event(mid_price=150.0)
    event = _parse(raw)
    assert event.data.bid_price < event.data.mid_price < event.data.ask_price


# ── OrderPlaced ────────────────────────────────────────────────────────────────

def test_order_placed_event_parses():
    raw = make_order_placed_event(symbol="AAPL")
    event = _parse(raw)
    assert event.event_type == "OrderPlaced"
    assert event.data.symbol == "AAPL"
    assert event.data.side == Side.BUY
    assert event.data.order_type == OrderType.LIMIT


def test_order_placed_has_agent_type():
    raw = make_order_placed_event()
    event = _parse(raw)
    assert event.data.agent_type == AgentType.RETAIL


# ── MarketStats ────────────────────────────────────────────────────────────────

def test_market_stats_event_parses():
    raw = make_market_stats_event(symbol="JPM", volume=250_000)
    event = _parse(raw)
    assert event.event_type == "MarketStats"
    assert event.data.symbol == "JPM"
    assert event.data.volume == 250_000


def test_market_stats_ohlc_ordering():
    raw = make_market_stats_event()
    event = _parse(raw)
    assert event.data.low <= event.data.open <= event.data.high
    assert event.data.low <= event.data.last <= event.data.high


# ── Validation failures ────────────────────────────────────────────────────────

def test_malformed_trade_fails_validation():
    from pydantic import ValidationError
    raw = make_malformed_trade_event()
    with pytest.raises(ValidationError):
        _parse(raw)


def test_invalid_event_type_fails_validation():
    from pydantic import ValidationError
    raw = make_invalid_event()
    with pytest.raises(ValidationError):
        _parse(raw)


def test_empty_dict_fails_validation():
    from pydantic import ValidationError
    with pytest.raises(ValidationError):
        _parse({})
