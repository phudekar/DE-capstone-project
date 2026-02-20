"""Tests for Dagster resource definitions."""

from __future__ import annotations

from orchestrator.resources.duckdb_resource import DuckDBResource
from orchestrator.resources.iceberg import IcebergResource
from orchestrator.resources.kafka import KafkaResource
from orchestrator.resources.prometheus import PrometheusResource


def test_kafka_resource_defaults():
    """KafkaResource has correct default configuration."""
    resource = KafkaResource()
    assert resource.bootstrap_servers == "kafka-broker-1:29092"
    assert resource.group_id == "dagster-orchestrator"


def test_kafka_resource_consumer_config():
    """KafkaResource generates valid consumer config dict."""
    resource = KafkaResource(bootstrap_servers="localhost:9092", group_id="test")
    config = resource.get_consumer_config()
    assert config["bootstrap.servers"] == "localhost:9092"
    assert config["group.id"] == "test"
    assert config["auto.offset.reset"] == "earliest"
    assert config["enable.auto.commit"] is False


def test_iceberg_resource_defaults():
    """IcebergResource has correct default configuration."""
    resource = IcebergResource()
    assert resource.catalog_uri == "http://iceberg-rest:8181"
    assert resource.catalog_name == "lakehouse"
    assert resource.s3_endpoint == "http://minio:9000"


def test_duckdb_resource_connection():
    """DuckDBResource returns a valid DuckDB connection."""
    resource = DuckDBResource()
    con = resource.get_connection()
    result = con.execute("SELECT 42 AS answer").fetchone()
    assert result[0] == 42
    con.close()


def test_prometheus_resource_stub(capsys):
    """PrometheusResource logs metrics without error."""
    resource = PrometheusResource()
    resource.push_metric("test_metric", 42.0, {"env": "test"})


def test_prometheus_resource_no_url():
    """PrometheusResource works with empty pushgateway URL."""
    resource = PrometheusResource(pushgateway_url="")
    resource.push_metric("test_metric", 1.0)


def test_kafka_resource_watermark_returns_none_on_failure():
    """KafkaResource returns None when Kafka is unreachable."""
    resource = KafkaResource(bootstrap_servers="nonexistent:9092")
    result = resource.get_high_watermark("test-topic")
    assert result is None


def test_iceberg_resource_custom_config():
    """IcebergResource accepts custom configuration."""
    resource = IcebergResource(
        catalog_uri="http://custom:8181",
        s3_endpoint="http://custom-minio:9000",
        s3_access_key="custom_key",
        s3_secret_key="custom_secret",
    )
    assert resource.catalog_uri == "http://custom:8181"
    assert resource.s3_access_key == "custom_key"
