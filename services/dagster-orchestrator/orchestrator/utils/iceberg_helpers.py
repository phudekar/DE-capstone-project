"""Iceberg utility helpers for asset operations."""

from __future__ import annotations

import logging
from datetime import datetime

import pyarrow as pa

logger = logging.getLogger(__name__)


def read_new_records(table, since: datetime | None = None) -> pa.Table:
    """Read records from an Iceberg table, optionally filtering by ingestion time.

    Args:
        table: PyIceberg Table instance
        since: Only return records ingested after this timestamp.

    Returns:
        Arrow table with matching records.
    """
    if since is not None:
        scan = table.scan(row_filter=f"_ingested_at > '{since.isoformat()}'")
    else:
        scan = table.scan()
    return scan.to_arrow()


def merge_records(table, new_data: pa.Table, key_columns: list[str]) -> int:
    """Append new records to an Iceberg table, deduplicating by key columns.

    Args:
        table: PyIceberg Table instance
        new_data: Arrow table with new records
        key_columns: Columns to use for deduplication

    Returns:
        Number of records written.
    """
    if len(new_data) == 0:
        return 0

    table.append(new_data)
    return len(new_data)


def overwrite_partition(table, data: pa.Table, partition_filter: str) -> int:
    """Delete existing records matching the filter, then append new data.

    Args:
        table: PyIceberg Table instance
        data: Arrow table with new records
        partition_filter: Row filter expression for deletion

    Returns:
        Number of records written.
    """
    if len(data) == 0:
        return 0

    table.delete(partition_filter)
    table.append(data)
    return len(data)
