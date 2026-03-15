"""Tests for message validation logic."""

from kafka_bridge.validation.message_validator import validate_message


class TestValidateMessage:
    def test_valid_order_placed(self):
        raw = {
            "event_type": "OrderPlaced",
            "data": {
                "event_id": "evt-1",
                "timestamp": "2024-01-01T00:00:00Z",
                "order_id": "ord-1",
                "symbol": "AAPL",
                "side": "Buy",
                "order_type": "Limit",
                "price": 150.0,
                "quantity": 100,
                "agent_id": "agent-1",
                "agent_type": "RetailTrader",
            },
        }
        event = validate_message(raw)
        assert event is not None
        assert event.event_type == "OrderPlaced"
        assert event.data.symbol == "AAPL"

    def test_valid_trade_executed(self):
        raw = {
            "event_type": "TradeExecuted",
            "data": {
                "event_id": "evt-2",
                "timestamp": "2024-01-01T00:00:00Z",
                "trade_id": "trade-1",
                "symbol": "TSLA",
                "price": 200.0,
                "quantity": 50,
                "buy_order_id": "ord-1",
                "sell_order_id": "ord-2",
                "buyer_agent_id": "agent-1",
                "seller_agent_id": "agent-2",
                "is_aggressive_buy": True,
            },
        }
        event = validate_message(raw)
        assert event is not None
        assert event.event_type == "TradeExecuted"

    def test_valid_quote_update(self):
        raw = {
            "event_type": "QuoteUpdate",
            "data": {
                "event_id": "evt-3",
                "timestamp": "2024-01-01T00:00:00Z",
                "symbol": "GOOG",
                "best_bid": 100.0,
                "best_bid_size": 10,
                "best_ask": 101.0,
                "best_ask_size": 15,
                "spread": 1.0,
            },
        }
        event = validate_message(raw)
        assert event is not None
        assert event.data.symbol == "GOOG"

    def test_invalid_event_type_returns_none(self):
        raw = {
            "event_type": "UnknownEvent",
            "data": {},
        }
        event = validate_message(raw)
        assert event is None

    def test_missing_required_field_returns_none(self):
        raw = {
            "event_type": "OrderPlaced",
            "data": {
                "order_id": "ord-1",
                # missing symbol, side, event_id, timestamp, etc.
            },
        }
        event = validate_message(raw)
        assert event is None

    def test_invalid_enum_value_returns_none(self):
        raw = {
            "event_type": "OrderPlaced",
            "data": {
                "event_id": "evt-4",
                "timestamp": "2024-01-01T00:00:00Z",
                "order_id": "ord-1",
                "symbol": "AAPL",
                "side": "InvalidSide",
                "order_type": "Limit",
                "price": 150.0,
                "quantity": 100,
                "agent_id": "agent-1",
                "agent_type": "RetailTrader",
            },
        }
        event = validate_message(raw)
        assert event is None
