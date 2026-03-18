"""Configuration via environment variables with KAFKA_BRIDGE_ prefix.

All settings can be overridden by setting the corresponding environment
variable with a ``KAFKA_BRIDGE_`` prefix (e.g. ``KAFKA_BRIDGE_WS_URI``).
Pydantic-settings handles parsing and type coercion automatically.
"""

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    model_config = {"env_prefix": "KAFKA_BRIDGE_"}

    # --- WebSocket connection to DE-Stock simulator ---
    # ws_uri: WebSocket endpoint for the DE-Stock exchange simulator.
    ws_uri: str = "ws://de-stock:8765"
    # ws_reconnect_delay / max_delay: exponential backoff bounds (seconds) for
    # reconnecting after a WebSocket disconnect.
    ws_reconnect_delay: float = 1.0
    ws_reconnect_max_delay: float = 30.0

    # --- Kafka broker and Schema Registry ---
    # kafka_bootstrap_servers: comma-separated broker addresses.
    kafka_bootstrap_servers: str = "kafka-broker-1:29092"
    # schema_registry_url: Confluent Schema Registry (used by avro_serializer
    # when Avro serialization is enabled; unused in the current JSON path).
    schema_registry_url: str = "http://schema-registry:8081"

    # --- Producer tuning ---
    # kafka_acks="all": wait for all ISR replicas to acknowledge (strongest
    # durability guarantee).
    kafka_acks: str = "all"
    # kafka_compression_type: Snappy provides a good balance of CPU vs. bandwidth.
    kafka_compression_type: str = "snappy"
    # kafka_linger_ms / kafka_batch_size: allow the producer to accumulate
    # messages for up to 20 ms or 64 KB before flushing, improving throughput
    # under high message rates.
    kafka_linger_ms: int = 20
    kafka_batch_size: int = 65536
    # kafka_queue_buffering_max_messages: upper bound on the internal producer
    # queue before back-pressure is applied.
    kafka_queue_buffering_max_messages: int = 100000
    kafka_socket_send_buffer_bytes: int = 1048576
    # kafka_enable_idempotence: ensures exactly-once delivery semantics at the
    # producer level (implies max.in.flight.requests.per.connection <= 5).
    kafka_enable_idempotence: bool = True

    # --- Observability ---
    # metrics_port: HTTP port for the Prometheus /metrics endpoint.
    metrics_port: int = 9090

    # --- Logging ---
    log_level: str = "INFO"
