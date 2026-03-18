"""Main orchestrator for the Kafka Bridge service.

Pipeline flow:
  1. WebSocket receive — async generator yields raw text from DE-Stock.
  2. JSON parse       — decode the text; malformed JSON goes to the DLQ.
  3. Pydantic validate — ``validate_message`` returns a typed ``MarketEvent``
                         or None (→ DLQ).
  4. Route             — ``route_event`` maps event_type to a Kafka topic and
                         partition key; unknown types fall through to the DLQ
                         topic.
  5. Serialize         — the validated event is flattened and JSON-encoded.
  6. Produce           — the bytes are handed to the confluent-kafka producer.
"""

import json
import logging
import time

from kafka_bridge.config.settings import Settings
from kafka_bridge.error_handling.dlq import DLQProducer
from kafka_bridge.kafka_producer import KafkaProducer
from kafka_bridge.message_router import DLQ_TOPIC, route_event
from kafka_bridge.metrics.prometheus import (
    BRIDGE_LATENCY,
    DLQ_PRODUCED_TOTAL,
    VALIDATION_FAILURES,
    WS_MESSAGES_RECEIVED,
    start_metrics_server,
)
from kafka_bridge.validation.message_validator import validate_message
from kafka_bridge.websocket_client import WebSocketClient

logger = logging.getLogger(__name__)


class Bridge:
    """Connects DE-Stock WebSocket to Kafka topics via JSON serialization."""

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._ws_client = WebSocketClient(settings)
        self._producer = KafkaProducer(settings)
        self._dlq = DLQProducer(settings)

    def request_shutdown(self) -> None:
        self._ws_client.request_shutdown()

    async def run(self) -> None:
        """Main loop: consume WS messages, validate, serialize, produce."""
        start_metrics_server(self._settings.metrics_port)
        logger.info("Prometheus metrics on port %d", self._settings.metrics_port)

        async for raw_text in self._ws_client.messages():
            start = time.monotonic()
            WS_MESSAGES_RECEIVED.inc()

            # Parse JSON
            try:
                raw = json.loads(raw_text)
            except json.JSONDecodeError as exc:
                logger.warning("Invalid JSON: %s", exc)
                self._dlq.send(raw_text, f"json_decode_error: {exc}")
                continue

            # Skip non-event messages (e.g. welcome handshake)
            if "event_type" not in raw:
                logger.debug("Skipping non-event message: %s", raw.get("type", "unknown"))
                continue

            # Validate with Pydantic
            event = validate_message(raw)
            if event is None:
                VALIDATION_FAILURES.inc()
                self._dlq.send(raw_text, "validation_error")
                continue

            # Use the validated model's attributes so that any Pydantic
            # coercion (e.g. type casting, default population) is respected.
            event_type = event.event_type
            data = event.data

            # Route to topic based on event_type
            route = route_event(event_type, data)

            # If the router fell back to the DLQ topic, the event_type is
            # unrecognised — log a warning and count it before producing.
            if route.topic == DLQ_TOPIC:
                logger.warning("Unknown event type routed to DLQ: %s", event_type)
                DLQ_PRODUCED_TOTAL.inc()

            # Flatten: merge envelope fields into data
            flat = {"event_type": event_type, **data}

            # Serialize to JSON
            json_bytes = json.dumps(flat).encode("utf-8")

            # Produce to Kafka
            self._producer.produce(route.topic, json_bytes, key=route.key)
            self._producer.poll(0)

            BRIDGE_LATENCY.observe(time.monotonic() - start)

        # Shutdown: flush producers
        logger.info("Flushing Kafka producers...")
        self._producer.flush()
        self._dlq.flush()
        logger.info("Bridge shutdown complete")
