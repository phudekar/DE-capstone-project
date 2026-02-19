"""Configuration via environment variables with KAFKA_BRIDGE_ prefix."""

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    model_config = {"env_prefix": "KAFKA_BRIDGE_"}

    # WebSocket
    ws_uri: str = "ws://de-stock:8765"
    ws_reconnect_delay: float = 1.0
    ws_reconnect_max_delay: float = 30.0

    # Kafka
    kafka_bootstrap_servers: str = "kafka-broker-1:29092"
    schema_registry_url: str = "http://schema-registry:8081"

    # Producer tuning
    kafka_acks: str = "all"
    kafka_compression_type: str = "snappy"
    kafka_linger_ms: int = 5
    kafka_batch_size: int = 16384
    kafka_enable_idempotence: bool = True

    # Metrics
    metrics_port: int = 9090

    # Logging
    log_level: str = "INFO"
