"""Tests for message validation logic."""

from kafka_bridge.validation.message_validator import validate_message


class TestValidateMessage:
    def test_valid_order_placed(self):
        raw = {
            "event_type": "OrderPlaced",
            "timestamp": "2024-01-01T00:00:00Z",
            "data": {
                "order_id": "ord-1",
                "symbol": "AAPL",
                "side": "Buy",
                "order_type": "Limit",
                "price": 150.0,
                "quantity": 100,
                "agent_id": "agent-1",
                "agent_type": "Retail",
            },
        }
        event = validate_message(raw)
        assert event is not None
        assert event.event_type == "OrderPlaced"
        assert event.data.symbol == "AAPL"

    def test_valid_trade_executed(self):
        raw = {
            "event_type": "TradeExecuted",
            "timestamp": "2024-01-01T00:00:00Z",
            "data": {
                "trade_id": "trade-1",
                "symbol": "TSLA",
                "price": 200.0,
                "quantity": 50,
                "buyer_order_id": "ord-1",
                "seller_order_id": "ord-2",
                "buyer_agent_id": "agent-1",
                "seller_agent_id": "agent-2",
                "aggressor_side": "Buy",
            },
        }
        event = validate_message(raw)
        assert event is not None
        assert event.event_type == "TradeExecuted"

    def test_valid_quote_update(self):
        raw = {
            "event_type": "QuoteUpdate",
            "timestamp": "2024-01-01T00:00:00Z",
            "data": {
                "symbol": "GOOG",
                "bid_price": 100.0,
                "bid_size": 10,
                "ask_price": 101.0,
                "ask_size": 15,
                "spread": 1.0,
                "mid_price": 100.5,
            },
        }
        event = validate_message(raw)
        assert event is not None
        assert event.data.symbol == "GOOG"

    def test_invalid_event_type_returns_none(self):
        raw = {
            "event_type": "UnknownEvent",
            "timestamp": "2024-01-01T00:00:00Z",
            "data": {},
        }
        event = validate_message(raw)
        assert event is None

    def test_missing_required_field_returns_none(self):
        raw = {
            "event_type": "OrderPlaced",
            "timestamp": "2024-01-01T00:00:00Z",
            "data": {
                "order_id": "ord-1",
                # missing symbol, side, etc.
            },
        }
        event = validate_message(raw)
        assert event is None

    def test_invalid_enum_value_returns_none(self):
        raw = {
            "event_type": "OrderPlaced",
            "timestamp": "2024-01-01T00:00:00Z",
            "data": {
                "order_id": "ord-1",
                "symbol": "AAPL",
                "side": "InvalidSide",
                "order_type": "Limit",
                "price": 150.0,
                "quantity": 100,
                "agent_id": "agent-1",
                "agent_type": "Retail",
            },
        }
        event = validate_message(raw)
        assert event is None
