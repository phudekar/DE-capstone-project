"""Kafka resource for Dagster — wraps confluent-kafka Consumer."""

from __future__ import annotations

import logging

from dagster import ConfigurableResource

logger = logging.getLogger(__name__)


class KafkaResource(ConfigurableResource):
    """Dagster resource wrapping a Kafka consumer connection."""

    bootstrap_servers: str = "kafka-broker-1:29092"
    group_id: str = "dagster-orchestrator"

    def get_consumer_config(self) -> dict:
        """Return confluent-kafka consumer configuration dict."""
        return {
            "bootstrap.servers": self.bootstrap_servers,
            "group.id": self.group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }

    def get_high_watermark(self, topic: str, partition: int = 0) -> int | None:
        """Get the high watermark offset for a topic partition.

        Returns None if Kafka is unreachable.
        """
        try:
            from confluent_kafka import Consumer, TopicPartition

            consumer = Consumer(
                {
                    "bootstrap.servers": self.bootstrap_servers,
                    "group.id": f"{self.group_id}-watermark",
                }
            )
            tp = TopicPartition(topic, partition)
            low, high = consumer.get_watermark_offsets(tp, timeout=5.0)
            consumer.close()
            return high
        except Exception:
            logger.warning("Failed to get watermark for %s/%d", topic, partition)
            return None
