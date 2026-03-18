"""
Async-compatible DuckDB engine over Iceberg tables via PyIceberg -> Arrow -> DuckDB.

Architecture (DuckDB-on-Iceberg pattern):
  1. PyIceberg reads table metadata from the REST catalog and resolves Parquet
     file locations in S3/MinIO.
  2. PyIceberg's scan().to_arrow() fetches the Parquet data into an Arrow table
     (predicate pushdown happens at the Iceberg layer via row_filter).
  3. The Arrow table is registered into an ephemeral in-process DuckDB connection,
     which executes the SQL query and returns results as Python dicts.

This approach avoids a persistent DuckDB database file and leverages DuckDB's
zero-copy Arrow integration for fast analytical queries.
"""

import asyncio
import logging
from typing import Any

import duckdb
import pyarrow as pa
from pyiceberg.catalog.rest import RestCatalog

from app.config import settings

logger = logging.getLogger(__name__)


def _get_catalog() -> RestCatalog:
    return RestCatalog(
        settings.iceberg_catalog_name,
        **{
            "uri": settings.iceberg_catalog_uri,
            "s3.endpoint": settings.s3_endpoint,
            "s3.access-key-id": settings.aws_access_key_id,
            "s3.secret-access-key": settings.aws_secret_access_key,
            "s3.region": settings.aws_region,
            "s3.path-style-access": "true",
        },
    )


class IcebergDuckDB:
    """
    Helper that loads Iceberg tables into Arrow and queries with DuckDB.

    DuckDB connections are not async-native, so we delegate blocking work to an
    executor thread to avoid stalling the event loop.

    The Iceberg REST catalog is lazily initialised on first query to avoid
    network calls during import/startup. This also means the service can
    start even if the catalog is temporarily unavailable.
    """

    def __init__(self) -> None:
        # Lazy-initialised on first _ensure_catalog() call.
        self._catalog: RestCatalog | None = None

    def _ensure_catalog(self) -> RestCatalog:
        if self._catalog is None:
            self._catalog = _get_catalog()
        return self._catalog

    def _scan_table(self, namespace: str, table: str, row_filter: str | None = None) -> pa.Table:
        catalog = self._ensure_catalog()
        iceberg_table = catalog.load_table(f"{namespace}.{table}")
        scan = iceberg_table.scan(row_filter=row_filter) if row_filter else iceberg_table.scan()
        return scan.to_arrow()

    def _execute_sync(self, arrow_tables: dict[str, pa.Table], sql: str, params: list[Any] | None = None) -> list[dict]:
        """Register Arrow tables in an in-process DuckDB connection and execute SQL."""
        con = duckdb.connect()
        try:
            for alias, tbl in arrow_tables.items():
                con.register(alias, tbl)
            result = con.execute(sql, params or [])
            columns = [desc[0] for desc in result.description]
            return [dict(zip(columns, row)) for row in result.fetchall()]
        finally:
            con.close()

    async def execute(
        self,
        namespace: str,
        table: str,
        sql: str,
        params: list[Any] | None = None,
        row_filter: str | None = None,
        table_alias: str = "t",
    ) -> list[dict]:
        """Load an Iceberg table and run SQL against it, returning rows as dicts."""
        loop = asyncio.get_running_loop()

        def _blocking():
            arrow = self._scan_table(namespace, table, row_filter=row_filter)
            return self._execute_sync({table_alias: arrow}, sql, params)

        return await loop.run_in_executor(None, _blocking)

    async def execute_multi(
        self,
        tables: dict[str, tuple[str, str, str | None]],  # alias -> (namespace, table, filter)
        sql: str,
        params: list[Any] | None = None,
    ) -> list[dict]:
        """Load multiple Iceberg tables and run a JOIN/UNION query."""
        loop = asyncio.get_running_loop()

        def _blocking():
            arrow_tables: dict[str, pa.Table] = {}
            for alias, (ns, tbl, filt) in tables.items():
                arrow_tables[alias] = self._scan_table(ns, tbl, row_filter=filt)
            return self._execute_sync(arrow_tables, sql, params)

        return await loop.run_in_executor(None, _blocking)


# Singleton instance, initialised once at startup
_engine: IcebergDuckDB | None = None


def get_engine() -> IcebergDuckDB:
    global _engine
    if _engine is None:
        _engine = IcebergDuckDB()
    return _engine
