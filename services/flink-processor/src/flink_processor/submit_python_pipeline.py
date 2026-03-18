"""Job B entry point: Python Analytics Pipeline (DataStream API).

Runs two independent sub-pipelines within a single Flink job:

  1. Price Alert Detection (flat_map):
     raw.trades → PriceAlertDetector → alerts.price-movement
     Statefully tracks per-symbol price baselines and emits alerts when
     significant price movements are detected (>2% within a 5-min window).

  2. Orderbook Analytics (map):
     raw.orderbook-snapshots → OrderBookAnalyzer → analytics.orderbook-metrics
     Stateless computation of spread, depth, and imbalance metrics from
     each orderbook snapshot.

Both sub-pipelines consume from Kafka (KafkaSource) and produce to Kafka
(KafkaSink). They share the same StreamExecutionEnvironment but operate
on independent data streams.

Usage:
    python -m flink_processor.submit_python_pipeline
"""

import logging
import sys

from pyflink.common import Types, WatermarkStrategy
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
from flink_processor.operators.orderbook_analyzer import process_orderbook_snapshot
from flink_processor.operators.price_alert_detector import (
    PriceAlertDetector,
    process_trade_for_alert,
)

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
    trades_stream = env.from_source(trades_source, WatermarkStrategy.no_watermarks(), "raw-trades-source")

    detector = PriceAlertDetector()

    # NOTE: DLQ topic (dlq.processing-errors) is defined in config but is not yet
    # wired as a Flink sink. Malformed records are logged as warnings for now.

    def detect_alerts(trade_json: str):
        """Thin wrapper around process_trade_for_alert for use as a flat_map function.

        Yields alert JSON strings (0 or 1 per trade). Logs warnings on malformed input
        so bad records are visible in Flink task manager logs.
        """
        try:
            result = process_trade_for_alert(detector, trade_json)
            if result is not None:
                yield result
        except Exception as exc:
            logger.warning("Unexpected error in detect_alerts: %s — input: %.200s", exc, trade_json)

    # output_type=Types.STRING() is required because PyFlink runs Python UDFs in a
    # separate process and must serialize results across the JVM boundary. Without an
    # explicit output type, Flink cannot infer the Java type for downstream operators.
    alerts_stream = trades_stream.flat_map(detect_alerts, output_type=Types.STRING())
    alerts_sink = _build_kafka_sink(TOPIC_PRICE_ALERTS)
    alerts_stream.sink_to(alerts_sink)

    # --- Sub-pipeline 2: Orderbook Analytics ---
    logger.info(
        "Setting up orderbook analytics: %s → %s",
        TOPIC_RAW_ORDERBOOK,
        TOPIC_ORDERBOOK_METRICS,
    )

    orderbook_source = _build_kafka_source(TOPIC_RAW_ORDERBOOK, "flink-python-orderbook-analytics")
    orderbook_stream = env.from_source(orderbook_source, WatermarkStrategy.no_watermarks(), "orderbook-source")

    def analyze_snapshot(snapshot_json: str):
        """Thin wrapper around process_orderbook_snapshot for use as a map function.

        Returns analytics JSON or None for malformed input. Logs warnings on errors
        so bad records are visible in Flink task manager logs.
        """
        try:
            return process_orderbook_snapshot(snapshot_json)
        except Exception as exc:
            logger.warning("Unexpected error in analyze_snapshot: %s — input: %.200s", exc, snapshot_json)
            return None

    # output_type=Types.STRING() is required because PyFlink runs Python UDFs in a
    # separate process and must serialize results across the JVM boundary.
    metrics_stream = orderbook_stream.map(analyze_snapshot, output_type=Types.STRING()).filter(lambda x: x is not None)
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
