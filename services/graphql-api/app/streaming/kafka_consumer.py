"""Kafka consumer factory for GraphQL subscriptions."""

from __future__ import annotations

import json
import logging
from typing import AsyncIterator

from aiokafka import AIOKafkaConsumer

from app.config import settings

logger = logging.getLogger(__name__)


class KafkaConsumerFactory:
    """Creates per-subscription Kafka consumers."""

    def __init__(self, bootstrap_servers: str | None = None) -> None:
        self.bootstrap_servers = bootstrap_servers or settings.kafka_bootstrap_servers

    def create(self, topic: str, group_id: str) -> AIOKafkaConsumer:
        return AIOKafkaConsumer(
            topic,
            bootstrap_servers=self.bootstrap_servers,
            group_id=group_id,
            auto_offset_reset="latest",
            enable_auto_commit=True,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            max_poll_interval_ms=300_000,
            session_timeout_ms=30_000,
        )

    async def stream(
        self, topic: str, group_id: str
    ) -> AsyncIterator[dict]:
        """Async generator that yields deserialized Kafka messages."""
        consumer = self.create(topic, group_id)
        await consumer.start()
        try:
            async for msg in consumer:
                yield msg.value
        finally:
            await consumer.stop()
