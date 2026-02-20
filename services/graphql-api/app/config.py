"""Application configuration from environment variables."""

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # Server
    host: str = "0.0.0.0"
    port: int = 8000
    debug: bool = False
    enable_graphiql: bool = True

    # Authentication — API key for simplicity in local dev
    api_key: str = "dev-api-key"
    auth_enabled: bool = False  # Disable by default for local dev

    # Iceberg / MinIO
    iceberg_catalog_uri: str = "http://iceberg-rest:8181"
    s3_endpoint: str = "http://minio:9000"
    aws_access_key_id: str = "minio"
    aws_secret_access_key: str = "minio123"
    aws_region: str = "us-east-1"
    s3_warehouse: str = "s3://warehouse"
    iceberg_catalog_name: str = "lakehouse"

    # Kafka
    kafka_bootstrap_servers: str = "kafka-broker-1:29092"

    # Cache TTL (seconds)
    cache_ttl_symbols: int = 3600
    cache_ttl_daily_summary: int = 300
    cache_ttl_market_overview: int = 300

    # DuckDB
    duckdb_threads: int = 4
    duckdb_memory_limit: str = "1GB"

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8"}


settings = Settings()
