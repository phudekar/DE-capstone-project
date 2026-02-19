"""Main orchestrator: WS recv → validate → serialize → produce."""

import json
import logging
import time

from kafka_bridge.config.settings import Settings
from kafka_bridge.error_handling.dlq import DLQProducer
from kafka_bridge.kafka_producer import KafkaProducer
from kafka_bridge.message_router import route_event
from kafka_bridge.metrics.prometheus import (
    BRIDGE_LATENCY,
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

            # Validate with Pydantic
            event = validate_message(raw)
            if event is None:
                VALIDATION_FAILURES.inc()
                self._dlq.send(raw_text, "validation_error")
                continue

            event_type = raw["event_type"]
            data = raw["data"]

            # Route to topic
            route = route_event(event_type, data)

            # Flatten: merge envelope fields into data
            flat = {"event_type": event_type, "timestamp": raw["timestamp"], **data}

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
