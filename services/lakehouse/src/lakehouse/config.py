"""Lakehouse configuration from environment variables."""

import os

# Iceberg REST catalog
ICEBERG_CATALOG_URI = os.getenv("ICEBERG_CATALOG_URI", "http://iceberg-rest:8181")
ICEBERG_CATALOG_NAME = "lakehouse"

# S3 / MinIO
S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://minio:9000")
S3_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID", "minio")
S3_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "minio123")
S3_WAREHOUSE = os.getenv("S3_WAREHOUSE", "s3://warehouse")
S3_REGION = os.getenv("AWS_REGION", "us-east-1")

# Kafka
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka-broker-1:29092")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "lakehouse-bronze-writer")

# Topics
TOPIC_RAW_TRADES = "raw.trades"
TOPIC_RAW_ORDERBOOK = "raw.orderbook-snapshots"

# Namespaces
NS_BRONZE = "bronze"
NS_SILVER = "silver"
NS_GOLD = "gold"
NS_DIM = "dimensions"

# Bronze writer batch settings
BATCH_MAX_MESSAGES = int(os.getenv("BATCH_MAX_MESSAGES", "1000"))
BATCH_TIMEOUT_SECONDS = float(os.getenv("BATCH_TIMEOUT_SECONDS", "5.0"))

# Reference data
REFERENCE_DATA_PATH = os.getenv(
    "REFERENCE_DATA_PATH", "/app/data/reference/symbols.json"
)

# Default Iceberg table properties
DEFAULT_TABLE_PROPERTIES = {
    "format-version": "2",
    "write.format.default": "parquet",
    "write.parquet.compression-codec": "zstd",
    "write.parquet.dict-encoding.enabled": "true",
    "write.metadata.delete-after-commit.enabled": "true",
    "write.metadata.previous-versions-max": "10",
}
