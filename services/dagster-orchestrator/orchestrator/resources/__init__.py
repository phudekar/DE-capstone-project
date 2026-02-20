"""Dagster resources for external service connections."""

from orchestrator.resources.duckdb_resource import DuckDBResource
from orchestrator.resources.iceberg import IcebergResource
from orchestrator.resources.kafka import KafkaResource
from orchestrator.resources.prometheus import PrometheusResource

__all__ = [
    "KafkaResource",
    "IcebergResource",
    "DuckDBResource",
    "PrometheusResource",
]
