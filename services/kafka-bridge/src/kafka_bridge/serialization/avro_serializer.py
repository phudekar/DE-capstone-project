"""Avro serialization via Confluent Schema Registry."""

import logging
import os
from pathlib import Path
from typing import Any

from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext

logger = logging.getLogger(__name__)

# In Docker: /app/libs/common/schemas/avro (set via AVRO_SCHEMA_DIR or auto-detected)
_SCHEMA_DIR = Path(
    os.environ.get("AVRO_SCHEMA_DIR", "")
    or Path(__file__).resolve().parents[5] / "libs" / "common" / "schemas" / "avro"
)

# event_type → (schema file, subject name)
_SCHEMA_MAP: dict[str, tuple[str, str]] = {
    "OrderPlaced": ("order_placed.avsc", "raw.orders-OrderPlaced"),
    "OrderCancelled": ("order_cancelled.avsc", "raw.orders-OrderCancelled"),
    "TradeExecuted": ("trade_executed.avsc", "raw.trades-TradeExecuted"),
    "QuoteUpdate": ("quote_update.avsc", "raw.quotes-QuoteUpdate"),
    "OrderBookSnapshot": ("orderbook_snapshot.avsc", "raw.orderbook-snapshots-OrderBookSnapshot"),
    "MarketStats": ("market_stats.avsc", "raw.market-stats-MarketStats"),
    "TradingHalt": ("trading_halt.avsc", "raw.trading-halts-TradingHalt"),
    "TradingResume": ("trading_resume.avsc", "raw.trading-halts-TradingResume"),
    "AgentAction": ("agent_action.avsc", "raw.agent-actions-AgentAction"),
}


class AvroEventSerializer:
    """Manages per-event-type Avro serializers backed by Schema Registry."""

    def __init__(self, schema_registry_url: str, schema_dir: Path | None = None) -> None:
        self._sr_client = SchemaRegistryClient({"url": schema_registry_url})
        self._schema_dir = schema_dir or _SCHEMA_DIR
        self._serializers: dict[str, AvroSerializer] = {}
        self._init_serializers()

    def _init_serializers(self) -> None:
        for event_type, (schema_file, subject) in _SCHEMA_MAP.items():
            schema_path = self._schema_dir / schema_file
            schema_str = schema_path.read_text()
            self._serializers[event_type] = AvroSerializer(
                self._sr_client,
                schema_str,
                conf={"auto.register.schemas": True, "subject.name.strategy": lambda ctx, name=subject: name},
            )
            logger.debug("Initialized Avro serializer for %s (subject: %s)", event_type, subject)

    def serialize(self, event_type: str, topic: str, value: dict[str, Any]) -> bytes | None:
        """Serialize a flattened event dict to Avro bytes. Returns None if event_type is unknown."""
        serializer = self._serializers.get(event_type)
        if serializer is None:
            logger.warning("No Avro serializer for event_type=%s", event_type)
            return None
        ctx = SerializationContext(topic, MessageField.VALUE)
        return serializer(value, ctx)
