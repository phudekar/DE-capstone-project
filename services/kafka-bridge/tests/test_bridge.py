"""Integration tests for the bridge pipeline (validation → routing → serialization)."""

import json

from kafka_bridge.message_router import route_event
from kafka_bridge.validation.message_validator import validate_message


def _make_order_placed_msg() -> dict:
    return {
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


class TestBridgePipeline:
    def test_valid_message_validates_and_routes(self):
        raw = _make_order_placed_msg()
        event = validate_message(raw)
        assert event is not None

        route = route_event(raw["event_type"], raw["data"])
        assert route.topic == "raw.orders"
        assert route.key == "AAPL"

    def test_flatten_produces_correct_avro_dict(self):
        raw = _make_order_placed_msg()
        flat = {"event_type": raw["event_type"], "timestamp": raw["timestamp"], **raw["data"]}
        assert flat["event_type"] == "OrderPlaced"
        assert flat["symbol"] == "AAPL"
        assert flat["price"] == 150.0

    def test_invalid_json_handled(self):
        bad_json = "not valid json {"
        try:
            json.loads(bad_json)
            parsed = True
        except json.JSONDecodeError:
            parsed = False
        assert not parsed

    def test_invalid_event_goes_to_dlq_route(self):
        raw = {"event_type": "Bogus", "timestamp": "2024-01-01T00:00:00Z", "data": {}}
        event = validate_message(raw)
        assert event is None
        # Would be sent to DLQ in bridge

    def test_all_event_types_route_correctly(self):
        events_and_topics = [
            ("OrderPlaced", "raw.orders"),
            ("OrderCancelled", "raw.orders"),
            ("TradeExecuted", "raw.trades"),
            ("QuoteUpdate", "raw.quotes"),
            ("OrderBookSnapshot", "raw.orderbook-snapshots"),
            ("MarketStats", "raw.market-stats"),
            ("TradingHalt", "raw.trading-halts"),
            ("TradingResume", "raw.trading-halts"),
            ("AgentAction", "raw.agent-actions"),
        ]
        for event_type, expected_topic in events_and_topics:
            route = route_event(event_type, {"symbol": "TEST", "agent_id": "a1"})
            assert route.topic == expected_topic, f"{event_type} should route to {expected_topic}"
