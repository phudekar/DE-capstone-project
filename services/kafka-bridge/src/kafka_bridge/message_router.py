"""Route events to the correct Kafka topic with appropriate partition key.

Routing strategy:
  - Each recognised ``event_type`` maps to a dedicated ``raw.*`` Kafka topic
    (e.g. ``TradeExecuted`` → ``raw.trades``).
  - The partition key is extracted from the event's data payload (typically
    ``symbol``) so that all events for the same instrument land on the same
    partition, preserving per-symbol ordering.
  - Any event_type that does not appear in the routing table is sent to the
    dead-letter queue (``dlq.raw.failed``) as a catch-all fallback, ensuring
    no message is silently dropped.
"""

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
