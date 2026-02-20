"""DuckDB resource for Dagster — in-memory analytical queries."""

from __future__ import annotations

import logging

import duckdb
from dagster import ConfigurableResource

logger = logging.getLogger(__name__)


class DuckDBResource(ConfigurableResource):
    """Dagster resource providing in-memory DuckDB connections."""

    def get_connection(self) -> duckdb.DuckDBPyConnection:
        """Return a new in-memory DuckDB connection."""
        return duckdb.connect()
