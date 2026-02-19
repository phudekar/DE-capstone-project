"""Route events to the correct Kafka topic with appropriate partition key."""

from dataclasses import dataclass


@dataclass(frozen=True)
class Route:
    topic: str
    key: str | None


# event_type → (topic, key_field) — key_field is extracted from event data
_ROUTES: dict[str, tuple[str, str]] = {
    "OrderPlaced": ("raw.orders", "symbol"),
    "OrderCancelled": ("raw.orders", "symbol"),
    "TradeExecuted": ("raw.trades", "symbol"),
    "QuoteUpdate": ("raw.quotes", "symbol"),
    "OrderBookSnapshot": ("raw.orderbook-snapshots", "symbol"),
    "MarketStats": ("raw.market-stats", "symbol"),
    "TradingHalt": ("raw.trading-halts", "symbol"),
    "TradingResume": ("raw.trading-halts", "symbol"),
    "AgentAction": ("raw.agent-actions", "agent_id"),
}

DLQ_TOPIC = "dlq.raw.failed"


def route_event(event_type: str, data: dict) -> Route:
    """Return the Kafka topic and partition key for the given event."""
    if event_type not in _ROUTES:
        return Route(topic=DLQ_TOPIC, key=None)

    topic, key_field = _ROUTES[event_type]
    key = data.get(key_field)
    return Route(topic=topic, key=key)
