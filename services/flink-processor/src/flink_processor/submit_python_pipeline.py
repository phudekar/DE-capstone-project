"""Job B entry point: Python Analytics Pipeline (DataStream API).

Runs two sub-pipelines:
  1. PriceAlertDetector: raw.trades → alerts.price-movement
  2. OrderBookAnalyzer: raw.orderbook-snapshots → analytics.orderbook-metrics

Usage:
    python -m flink_processor.submit_python_pipeline
"""

import json
import logging
import sys

from pyflink.common import WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    KafkaOffsetsInitializer,
    KafkaRecordSerializationSchema,
    KafkaSink,
    KafkaSource,
)

from flink_processor.config import (
    CHECKPOINT_INTERVAL_MS,
    KAFKA_BOOTSTRAP_SERVERS,
    PARALLELISM,
    TOPIC_ORDERBOOK_METRICS,
    TOPIC_PRICE_ALERTS,
    TOPIC_RAW_ORDERBOOK,
    TOPIC_RAW_TRADES,
)
from flink_processor.operators.orderbook_analyzer import analyze_orderbook
from flink_processor.operators.price_alert_detector import PriceAlertDetector

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger(__name__)


def _build_kafka_source(topic: str, group_id: str) -> KafkaSource:
    return (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BOOTSTRAP_SERVERS)
        .set_topics(topic)
        .set_group_id(group_id)
        .set_starting_offsets(KafkaOffsetsInitializer.latest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )


def _build_kafka_sink(topic: str) -> KafkaSink:
    return (
        KafkaSink.builder()
        .set_bootstrap_servers(KAFKA_BOOTSTRAP_SERVERS)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(topic)
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .build()
    )


def main() -> None:
    logger.info("Starting Job B: Python Analytics Pipeline")

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(PARALLELISM)
    env.enable_checkpointing(CHECKPOINT_INTERVAL_MS)

    # Add Kafka connector JAR
    env.add_jars("file:///opt/flink/lib/flink-sql-connector-kafka-3.0.2-1.18.jar")

    # --- Sub-pipeline 1: Price Alert Detection ---
    logger.info("Setting up price alert detection: %s → %s", TOPIC_RAW_TRADES, TOPIC_PRICE_ALERTS)

    trades_source = _build_kafka_source(TOPIC_RAW_TRADES, "flink-python-price-alerts")
    trades_stream = env.from_source(
        trades_source, WatermarkStrategy.no_watermarks(), "raw-trades-source"
    )

    detector = PriceAlertDetector()

    def detect_alerts(trade_json: str):
        try:
            trade = json.loads(trade_json)
            result = detector.process(
                symbol=trade["symbol"],
                price=trade["price"],
                timestamp_str=trade["timestamp"],
            )
            if result is not None:
                yield json.dumps(result)
        except (json.JSONDecodeError, KeyError):
            pass

    alerts_stream = trades_stream.flat_map(detect_alerts)
    alerts_sink = _build_kafka_sink(TOPIC_PRICE_ALERTS)
    alerts_stream.sink_to(alerts_sink)

    # --- Sub-pipeline 2: Orderbook Analytics ---
    logger.info(
        "Setting up orderbook analytics: %s → %s",
        TOPIC_RAW_ORDERBOOK,
        TOPIC_ORDERBOOK_METRICS,
    )

    orderbook_source = _build_kafka_source(
        TOPIC_RAW_ORDERBOOK, "flink-python-orderbook-analytics"
    )
    orderbook_stream = env.from_source(
        orderbook_source, WatermarkStrategy.no_watermarks(), "orderbook-source"
    )

    def analyze_snapshot(snapshot_json: str):
        try:
            snapshot = json.loads(snapshot_json)
            result = analyze_orderbook(snapshot)
            return json.dumps(result)
        except (json.JSONDecodeError, KeyError):
            return None

    metrics_stream = orderbook_stream.map(analyze_snapshot).filter(lambda x: x is not None)
    metrics_sink = _build_kafka_sink(TOPIC_ORDERBOOK_METRICS)
    metrics_stream.sink_to(metrics_sink)

    # Execute
    logger.info("Submitting Job B")
    env.execute("Python Analytics Pipeline")


if __name__ == "__main__":
    try:
        main()
    except Exception:
        logger.exception("Job B failed")
        sys.exit(1)
