"""Kafka sensors — trigger Bronze ingestion when new messages arrive."""

import logging

from dagster import (
    AssetKey,
    DefaultSensorStatus,
    RunRequest,
    SensorEvaluationContext,
    sensor,
)

from orchestrator.resources.kafka import KafkaResource

logger = logging.getLogger(__name__)

TRADES_TOPIC = "raw.trades"
ORDERBOOK_TOPIC = "raw.orderbook-snapshots"


@sensor(
    name="kafka_trades_sensor",
    asset_selection=[AssetKey("bronze_raw_trades")],
    minimum_interval_seconds=30,
    default_status=DefaultSensorStatus.STOPPED,
    description="Polls Kafka raw.trades topic and triggers bronze_raw_trades on new messages.",
)
def kafka_trades_sensor(
    context: SensorEvaluationContext,
    kafka: KafkaResource,
) -> None:
    """Check high watermark of raw.trades and trigger if new data exists."""
    cursor = int(context.cursor) if context.cursor else 0
    high_watermark = kafka.get_high_watermark(TRADES_TOPIC)

    if high_watermark is None:
        context.log.warning("Could not reach Kafka — skipping sensor tick.")
        return

    if high_watermark > cursor:
        context.log.info(
            "New trades detected: watermark %d > cursor %d", high_watermark, cursor
        )
        from datetime import date

        partition_key = date.today().isoformat()
        yield RunRequest(
            run_key=f"trades-{high_watermark}",
            partition_key=partition_key,
        )
        context.update_cursor(str(high_watermark))
    else:
        context.log.debug("No new trades. Watermark: %d", high_watermark)


@sensor(
    name="kafka_orderbook_sensor",
    asset_selection=[AssetKey("bronze_raw_orderbook")],
    minimum_interval_seconds=30,
    default_status=DefaultSensorStatus.STOPPED,
    description="Polls Kafka raw.orderbook-snapshots topic and triggers bronze_raw_orderbook.",
)
def kafka_orderbook_sensor(
    context: SensorEvaluationContext,
    kafka: KafkaResource,
) -> None:
    """Check high watermark of raw.orderbook-snapshots and trigger if new data."""
    cursor = int(context.cursor) if context.cursor else 0
    high_watermark = kafka.get_high_watermark(ORDERBOOK_TOPIC)

    if high_watermark is None:
        context.log.warning("Could not reach Kafka — skipping sensor tick.")
        return

    if high_watermark > cursor:
        context.log.info(
            "New orderbook data detected: watermark %d > cursor %d",
            high_watermark,
            cursor,
        )
        from datetime import date

        partition_key = date.today().isoformat()
        yield RunRequest(
            run_key=f"orderbook-{high_watermark}",
            partition_key=partition_key,
        )
        context.update_cursor(str(high_watermark))
    else:
        context.log.debug("No new orderbook data. Watermark: %d", high_watermark)
