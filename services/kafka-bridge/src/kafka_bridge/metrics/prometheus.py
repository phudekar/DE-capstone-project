"""Prometheus metrics for the Kafka Bridge."""

from prometheus_client import Counter, Histogram, start_http_server

# Messages received from WebSocket
WS_MESSAGES_RECEIVED = Counter(
    "kafka_bridge_ws_messages_received_total",
    "Total WebSocket messages received",
)

# Messages successfully produced to Kafka
KAFKA_PRODUCED_TOTAL = Counter(
    "kafka_bridge_kafka_produced_total",
    "Total messages produced to Kafka",
    ["topic"],
)

# Kafka produce errors
KAFKA_PRODUCE_ERRORS = Counter(
    "kafka_bridge_kafka_produce_errors_total",
    "Total Kafka produce errors",
    ["topic"],
)

# Validation failures
VALIDATION_FAILURES = Counter(
    "kafka_bridge_validation_failures_total",
    "Total message validation failures",
)

# Serialization failures
SERIALIZATION_FAILURES = Counter(
    "kafka_bridge_serialization_failures_total",
    "Total Avro serialization failures",
)

# DLQ messages
DLQ_PRODUCED_TOTAL = Counter(
    "kafka_bridge_dlq_produced_total",
    "Total messages sent to DLQ",
)

# WebSocket reconnections
WS_RECONNECTS = Counter(
    "kafka_bridge_ws_reconnects_total",
    "Total WebSocket reconnections",
)

# End-to-end latency (WS receive → Kafka produce)
BRIDGE_LATENCY = Histogram(
    "kafka_bridge_latency_seconds",
    "End-to-end bridge latency in seconds",
    buckets=(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0),
)


def start_metrics_server(port: int) -> None:
    """Start the Prometheus metrics HTTP server."""
    start_http_server(port)
