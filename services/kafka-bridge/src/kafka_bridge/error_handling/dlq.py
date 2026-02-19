"""Dead Letter Queue producer for messages that fail validation or serialization."""

import json
import logging

from confluent_kafka import Producer

from kafka_bridge.config.settings import Settings
from kafka_bridge.message_router import DLQ_TOPIC
from kafka_bridge.metrics.prometheus import DLQ_PRODUCED_TOTAL

logger = logging.getLogger(__name__)


class DLQProducer:
    """Sends failed messages to the dead letter queue topic as raw JSON."""

    def __init__(self, settings: Settings) -> None:
        self._producer = Producer({
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "client.id": "kafka-bridge-dlq",
        })

    def send(self, raw_message: str, error: str) -> None:
        """Send a failed message to the DLQ with error metadata."""
        envelope = json.dumps({"error": error, "original_message": raw_message})
        try:
            self._producer.produce(
                topic=DLQ_TOPIC,
                value=envelope.encode("utf-8"),
            )
            self._producer.poll(0)
            DLQ_PRODUCED_TOTAL.inc()
            logger.debug("Sent message to DLQ: %s", error)
        except Exception:
            logger.exception("Failed to send to DLQ")

    def flush(self, timeout: float = 5.0) -> None:
        self._producer.flush(timeout)
