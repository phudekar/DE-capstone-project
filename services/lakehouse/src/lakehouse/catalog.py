"""PyIceberg catalog connection and table management."""

from __future__ import annotations

import logging

from pyiceberg.catalog import load_catalog
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.transforms import DayTransform

from lakehouse import config
from lakehouse.schemas.bronze import RAW_ORDERBOOK_SCHEMA, RAW_TRADES_SCHEMA
from lakehouse.schemas.dimensions import DIM_SYMBOL_SCHEMA, DIM_TIME_SCHEMA
from lakehouse.schemas.gold import DAILY_TRADING_SUMMARY_SCHEMA
from lakehouse.schemas.silver import ORDERBOOK_SNAPSHOTS_SCHEMA, TRADES_SCHEMA

logger = logging.getLogger(__name__)

# Table definitions: (namespace, name, schema, partition_spec or None)
_TABLE_DEFS: list[tuple[str, str, Schema, PartitionSpec | None]] = [
    # Bronze
    (
        config.NS_BRONZE,
        "raw_trades",
        RAW_TRADES_SCHEMA,
        PartitionSpec(PartitionField(11, 1000, DayTransform(), "ts_day")),
    ),
    (
        config.NS_BRONZE,
        "raw_orderbook",
        RAW_ORDERBOOK_SCHEMA,
        PartitionSpec(PartitionField(6, 1000, DayTransform(), "ts_day")),
    ),
    # Silver
    (
        config.NS_SILVER,
        "trades",
        TRADES_SCHEMA,
        PartitionSpec(PartitionField(10, 1000, DayTransform(), "ts_day")),
    ),
    (
        config.NS_SILVER,
        "orderbook_snapshots",
        ORDERBOOK_SNAPSHOTS_SCHEMA,
        PartitionSpec(PartitionField(2, 1000, DayTransform(), "ts_day")),
    ),
    # Gold
    (
        config.NS_GOLD,
        "daily_trading_summary",
        DAILY_TRADING_SUMMARY_SCHEMA,
        PartitionSpec(PartitionField(2, 1000, DayTransform(), "date_day")),
    ),
    # Dimensions (unpartitioned — small tables)
    (config.NS_DIM, "dim_symbol", DIM_SYMBOL_SCHEMA, None),
    (config.NS_DIM, "dim_time", DIM_TIME_SCHEMA, None),
]


def get_catalog():
    """Load the Iceberg REST catalog."""
    return load_catalog(
        config.ICEBERG_CATALOG_NAME,
        **{
            "type": "rest",
            "uri": config.ICEBERG_CATALOG_URI,
            "s3.endpoint": config.S3_ENDPOINT,
            "s3.access-key-id": config.S3_ACCESS_KEY,
            "s3.secret-access-key": config.S3_SECRET_KEY,
            "s3.region": config.S3_REGION,
        },
    )


def create_namespaces(catalog) -> None:
    """Create all namespaces if they don't exist."""
    existing = {ns[0] for ns in catalog.list_namespaces()}
    for ns in (config.NS_BRONZE, config.NS_SILVER, config.NS_GOLD, config.NS_DIM):
        if ns not in existing:
            catalog.create_namespace(ns)
            logger.info("Created namespace: %s", ns)


def create_tables(catalog) -> dict[str, Table]:
    """Create all Iceberg tables if they don't exist. Returns name→Table map."""
    tables = {}
    for ns, name, schema, partition_spec in _TABLE_DEFS:
        full_name = f"{ns}.{name}"
        try:
            table = catalog.load_table(full_name)
            logger.info("Table already exists: %s", full_name)
        except Exception:
            kwargs = {"properties": config.DEFAULT_TABLE_PROPERTIES}
            if partition_spec is not None:
                kwargs["partition_spec"] = partition_spec
            table = catalog.create_table(full_name, schema=schema, **kwargs)
            logger.info("Created table: %s", full_name)
        tables[full_name] = table
    return tables


def init_catalog():
    """Full initialization: catalog → namespaces → tables."""
    catalog = get_catalog()
    create_namespaces(catalog)
    tables = create_tables(catalog)
    logger.info("Catalog initialization complete. %d tables ready.", len(tables))
    return catalog, tables


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    init_catalog()
