"""Tests for message routing logic."""

from kafka_bridge.message_router import DLQ_TOPIC, route_event


class TestRouteEvent:
    def test_order_placed_routes_to_raw_orders(self):
        route = route_event("OrderPlaced", {"symbol": "AAPL", "order_id": "123"})
        assert route.topic == "raw.orders"
        assert route.key == "AAPL"

    def test_order_cancelled_routes_to_raw_orders(self):
        route = route_event("OrderCancelled", {"symbol": "TSLA"})
        assert route.topic == "raw.orders"
        assert route.key == "TSLA"

    def test_trade_executed_routes_to_raw_trades(self):
        route = route_event("TradeExecuted", {"symbol": "GOOG"})
        assert route.topic == "raw.trades"
        assert route.key == "GOOG"

    def test_quote_update_routes_to_raw_quotes(self):
        route = route_event("QuoteUpdate", {"symbol": "MSFT"})
        assert route.topic == "raw.quotes"
        assert route.key == "MSFT"

    def test_orderbook_snapshot_routes_correctly(self):
        route = route_event("OrderBookSnapshot", {"symbol": "AMZN"})
        assert route.topic == "raw.orderbook-snapshots"
        assert route.key == "AMZN"

    def test_market_stats_routes_correctly(self):
        route = route_event("MarketStats", {"symbol": "META"})
        assert route.topic == "raw.market-stats"
        assert route.key == "META"

    def test_trading_halt_routes_correctly(self):
        route = route_event("TradingHalt", {"symbol": "NVDA"})
        assert route.topic == "raw.trading-halts"
        assert route.key == "NVDA"

    def test_trading_resume_routes_to_same_topic(self):
        route = route_event("TradingResume", {"symbol": "NVDA"})
        assert route.topic == "raw.trading-halts"
        assert route.key == "NVDA"

    def test_agent_action_keyed_by_agent_id(self):
        route = route_event("AgentAction", {"agent_id": "agent-42", "symbol": "AAPL"})
        assert route.topic == "raw.agent-actions"
        assert route.key == "agent-42"

    def test_unknown_event_type_routes_to_dlq(self):
        route = route_event("SomethingUnknown", {"symbol": "AAPL"})
        assert route.topic == DLQ_TOPIC
        assert route.key is None

    def test_missing_key_field_returns_none_key(self):
        route = route_event("OrderPlaced", {"order_id": "123"})
        assert route.topic == "raw.orders"
        assert route.key is None
