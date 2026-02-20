"""Shared test fixtures for dagster-orchestrator tests."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from orchestrator.resources.duckdb_resource import DuckDBResource
from orchestrator.resources.iceberg import IcebergResource
from orchestrator.resources.kafka import KafkaResource
from orchestrator.resources.prometheus import PrometheusResource


@pytest.fixture
def mock_kafka():
    """Mock KafkaResource."""
    resource = MagicMock(spec=KafkaResource)
    resource.bootstrap_servers = "localhost:9092"
    resource.group_id = "test-group"
    resource.get_consumer_config.return_value = {
        "bootstrap.servers": "localhost:9092",
        "group.id": "test-group",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    }
    resource.get_high_watermark.return_value = 100
    return resource


@pytest.fixture
def mock_iceberg():
    """Mock IcebergResource."""
    resource = MagicMock(spec=IcebergResource)
    resource.catalog_uri = "http://localhost:8181"
    resource.get_catalog.return_value = MagicMock()
    resource.load_table.return_value = MagicMock()
    return resource


@pytest.fixture
def mock_duckdb():
    """Mock DuckDBResource."""
    resource = MagicMock(spec=DuckDBResource)
    mock_conn = MagicMock()
    resource.get_connection.return_value = mock_conn
    return resource


@pytest.fixture
def mock_prometheus():
    """Mock PrometheusResource."""
    resource = MagicMock(spec=PrometheusResource)
    resource.pushgateway_url = ""
    return resource
