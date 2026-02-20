"""Kafka utility helpers for sensors and assets."""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


def get_topic_high_watermark(
    bootstrap_servers: str, topic: str, partition: int = 0
) -> int | None:
    """Get the high watermark offset for a Kafka topic partition.

    Returns None if Kafka is unreachable.
    """
    try:
        from confluent_kafka import Consumer, TopicPartition

        consumer = Consumer(
            {
                "bootstrap.servers": bootstrap_servers,
                "group.id": "dagster-watermark-check",
            }
        )
        tp = TopicPartition(topic, partition)
        _low, high = consumer.get_watermark_offsets(tp, timeout=5.0)
        consumer.close()
        return high
    except Exception:
        logger.warning("Failed to get watermark for %s/%d", topic, partition)
        return None
