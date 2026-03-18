"""
GraphQL Subscription root type — real-time Kafka feeds.

Subscriptions use a Kafka-backed WebSocket model: each GraphQL subscription
maps to a Kafka consumer that streams messages to the client over WebSocket.
Every WebSocket connection gets its own unique consumer group ID so that
each client receives all messages independently (no load-balancing across
subscribers). Messages are filtered server-side before being pushed to the
client to reduce network overhead.
"""

import uuid
from datetime import datetime, timezone
from typing import AsyncGenerator, Optional

import strawberry
from strawberry.types import Info

from app.schema.types import Trade, TradeAlert


@strawberry.type
class Subscription:
    @strawberry.subscription(description="Real-time trade feed. Optionally filter by symbol.")
    async def on_new_trade(self, info: Info, symbol: Optional[str] = None) -> AsyncGenerator["Trade", None]:
        """
        Consumes from the 'enriched.trades' Kafka topic.
        Each WebSocket connection gets a unique consumer group ID.
        Filtered server-side by symbol if provided.
        """
        connection_id = str(uuid.uuid4())
        group_id = f"gql-trade-sub-{connection_id}"
        factory = info.context.kafka_factory

        async for msg in factory.stream("enriched.trades", group_id):
            try:
                if symbol is not None and msg.get("symbol") != symbol:
                    continue
                ts_raw = msg.get("timestamp", "")
                ts = datetime.fromisoformat(ts_raw) if ts_raw else datetime.now(timezone.utc)
                yield Trade(
                    trade_id=str(msg.get("trade_id", "")),
                    symbol=msg.get("symbol", ""),
                    price=float(msg.get("price", 0)),
                    quantity=int(msg.get("quantity", 0)),
                    buy_order_id=msg.get("buy_order_id", ""),
                    sell_order_id=msg.get("sell_order_id", ""),
                    buyer_agent_id=msg.get("buyer_agent_id", ""),
                    seller_agent_id=msg.get("seller_agent_id", ""),
                    is_aggressive_buy=bool(msg.get("is_aggressive_buy", False)),
                    timestamp=ts,
                    company_name=msg.get("company_name"),
                    sector=msg.get("sector"),
                )
            except Exception:
                continue

    @strawberry.subscription(description="Price alerts triggered when thresholds are crossed.")
    async def on_trade_alert(
        self, info: Info, symbols: Optional[list[str]] = None
    ) -> AsyncGenerator["TradeAlert", None]:
        """
        Consumes from the 'alerts.price-movement' Kafka topic.
        Filtered server-side by symbol list if provided.
        """
        connection_id = str(uuid.uuid4())
        group_id = f"gql-alert-sub-{connection_id}"
        factory = info.context.kafka_factory

        # Topic name must match the Flink Python pipeline output topic
        # (services/flink-processor alert job writes to 'alerts.price-movement').
        async for msg in factory.stream("alerts.price-movement", group_id):
            try:
                sym = msg.get("symbol", "")
                if symbols is not None and sym not in symbols:
                    continue
                ts_raw = msg.get("triggered_at", "")
                ts = datetime.fromisoformat(ts_raw) if ts_raw else datetime.now(timezone.utc)
                yield TradeAlert(
                    symbol=sym,
                    alert_type=msg.get("alert_type", ""),
                    price=float(msg.get("price", 0)),
                    threshold=float(msg.get("threshold", 0)),
                    triggered_at=ts,
                    message=msg.get("message", ""),
                )
            except Exception:
                continue
