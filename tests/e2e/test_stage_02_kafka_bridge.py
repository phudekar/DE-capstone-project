"""Stage 2 — Kafka Bridge: routing and validation logic.

Tests:
  - route_event() returns the correct Kafka topic and partition key
  - validate_message() accepts valid events and rejects invalid ones
  - Unknown event types are routed to DLQ
  - The "flatten" (envelope merge) produces the expected schema
"""

import sys
from pathlib import Path

ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(ROOT / "libs/common/src"))
sys.path.insert(0, str(ROOT / "services/kafka-bridge/src"))

import pytest
from kafka_bridge.message_router import route_event, DLQ_TOPIC
from kafka_bridge.validation.message_validator import validate_message
from tests.e2e.fixtures.events import (
    make_trade_event,
    make_orderbook_event,
    make_quote_event,
    make_order_placed_event,
    make_market_stats_event,
    make_invalid_event,
    make_malformed_trade_event,
)


# ── route_event ────────────────────────────────────────────────────────────────

def test_trade_routes_to_raw_trades():
    ev = make_trade_event(symbol="AAPL")
    route = route_event("TradeExecuted", ev["data"])
    assert route.topic == "raw.trades"
    assert route.key == "AAPL"


def test_orderbook_routes_to_raw_orderbook():
    ev = make_orderbook_event(symbol="MSFT")
    route = route_event("OrderBookSnapshot", ev["data"])
    assert route.topic == "raw.orderbook-snapshots"
    assert route.key == "MSFT"


def test_quote_routes_to_raw_quotes():
    ev = make_quote_event(symbol="GOOG")
    route = route_event("QuoteUpdate", ev["data"])
    assert route.topic == "raw.quotes"
    assert route.key == "GOOG"


def test_order_placed_routes_to_raw_orders():
    ev = make_order_placed_event(symbol="AAPL")
    route = route_event("OrderPlaced", ev["data"])
    assert route.topic == "raw.orders"
    assert route.key == "AAPL"


def test_market_stats_routes_to_raw_market_stats():
    ev = make_market_stats_event(symbol="JPM")
    route = route_event("MarketStats", ev["data"])
    assert route.topic == "raw.market-stats"
    assert route.key == "JPM"


def test_unknown_event_routes_to_dlq():
    ev = make_invalid_event()
    route = route_event("UnknownEvent", ev["data"])
    assert route.topic == DLQ_TOPIC
    assert route.key is None


def test_trading_halt_routes_correctly():
    data = {"symbol": "TSLA", "reason": "circuit_breaker", "level": 1,
            "trigger_price": 100.0, "reference_price": 110.0,
            "decline_pct": 9.0, "halt_duration_secs": 300}
    route = route_event("TradingHalt", data)
    assert route.topic == "raw.trading-halts"
    assert route.key == "TSLA"


def test_agent_action_uses_agent_id_as_key():
    data = {"agent_id": "A-abc123", "agent_type": "HFT",
            "action": "submit_order", "symbol": "NVDA", "details": {}}
    route = route_event("AgentAction", data)
    assert route.topic == "raw.agent-actions"
    assert route.key == "A-abc123"


# ── validate_message ───────────────────────────────────────────────────────────

def test_validate_trade_event_succeeds():
    raw = make_trade_event()
    result = validate_message(raw)
    assert result is not None
    assert result.event_type == "TradeExecuted"


def test_validate_orderbook_event_succeeds():
    raw = make_orderbook_event()
    result = validate_message(raw)
    assert result is not None
    assert result.event_type == "OrderBookSnapshot"


def test_validate_quote_event_succeeds():
    raw = make_quote_event()
    result = validate_message(raw)
    assert result is not None


def test_validate_malformed_trade_returns_none():
    raw = make_malformed_trade_event()
    result = validate_message(raw)
    assert result is None


def test_validate_invalid_event_type_returns_none():
    raw = make_invalid_event()
    result = validate_message(raw)
    assert result is None


def test_validate_empty_dict_returns_none():
    result = validate_message({})
    assert result is None


# ── Flatten logic (envelope merge) ────────────────────────────────────────────

def _flatten(raw: dict) -> dict:
    return {"event_type": raw["event_type"], "timestamp": raw["timestamp"], **raw["data"]}


def test_flatten_trade_includes_trade_id():
    raw = make_trade_event()
    flat = _flatten(raw)
    assert "trade_id" in flat
    assert flat["event_type"] == "TradeExecuted"
    assert "timestamp" in flat
    assert "symbol" in flat


def test_flatten_preserves_all_data_fields():
    raw = make_trade_event()
    flat = _flatten(raw)
    for key in raw["data"]:
        assert key in flat, f"Missing field: {key}"


def test_flatten_orderbook_includes_bids_asks():
    raw = make_orderbook_event()
    flat = _flatten(raw)
    assert "bids" in flat
    assert "asks" in flat
    assert "sequence_number" in flat


# ── Full bridge pipeline (uses PipelineSimulator) ─────────────────────────────

def test_bridge_pipeline_valid_trade(simulator):
    raw = make_trade_event(symbol="AAPL")
    result = simulator.bridge_process(raw)
    assert result is not None
    assert result["topic"] == "raw.trades"
    assert result["key"] == "AAPL"
    assert result["flat"]["symbol"] == "AAPL"
    assert "trade_id" in result["flat"]


def test_bridge_pipeline_invalid_event_returns_none(simulator):
    raw = make_invalid_event()
    result = simulator.bridge_process(raw)
    assert result is None


def test_bridge_pipeline_malformed_returns_none(simulator):
    raw = make_malformed_trade_event()
    result = simulator.bridge_process(raw)
    assert result is None
