"""Confluent Kafka producer wrapper with delivery callbacks and metrics."""

import logging

from confluent_kafka import KafkaError, KafkaException, Producer

from kafka_bridge.config.settings import Settings
from kafka_bridge.metrics.prometheus import KAFKA_PRODUCE_ERRORS, KAFKA_PRODUCED_TOTAL

logger = logging.getLogger(__name__)


class KafkaProducer:
    """Wraps confluent_kafka.Producer with idempotent, batched, snappy config."""

    def __init__(self, settings: Settings) -> None:
        self._producer = Producer({
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "acks": settings.kafka_acks,
            "compression.type": settings.kafka_compression_type,
            "linger.ms": settings.kafka_linger_ms,
            "batch.size": settings.kafka_batch_size,
            "enable.idempotence": settings.kafka_enable_idempotence,
            "client.id": "kafka-bridge",
        })
        logger.info("Kafka producer initialized (servers=%s)", settings.kafka_bootstrap_servers)

    @staticmethod
    def _delivery_callback(err: KafkaError | None, msg) -> None:
        if err is not None:
            logger.error("Delivery failed for %s: %s", msg.topic(), err)
            KAFKA_PRODUCE_ERRORS.labels(topic=msg.topic()).inc()
        else:
            KAFKA_PRODUCED_TOTAL.labels(topic=msg.topic()).inc()

    def produce(self, topic: str, value: bytes, key: str | None = None) -> None:
        """Produce a message to a Kafka topic."""
        try:
            self._producer.produce(
                topic=topic,
                value=value,
                key=key.encode("utf-8") if key else None,
                callback=self._delivery_callback,
            )
        except KafkaException as exc:
            logger.error("Failed to enqueue message to %s: %s", topic, exc)
            KAFKA_PRODUCE_ERRORS.labels(topic=topic).inc()
            raise

    def poll(self, timeout: float = 0.0) -> int:
        """Poll for delivery callbacks."""
        return self._producer.poll(timeout)

    def flush(self, timeout: float = 10.0) -> int:
        """Flush all pending messages."""
        return self._producer.flush(timeout)
