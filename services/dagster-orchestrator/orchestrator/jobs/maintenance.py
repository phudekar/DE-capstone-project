"""Maintenance job — table compaction and cleanup."""

import logging

from dagster import In, Nothing, job, op

from orchestrator.resources.iceberg import IcebergResource

logger = logging.getLogger(__name__)

TABLES_TO_COMPACT = [
    "bronze.raw_trades",
    "bronze.raw_orderbook",
    "silver.trades",
    "silver.orderbook_snapshots",
    "gold.daily_trading_summary",
]


@op(description="Log table compaction intent for Iceberg tables.")
def compact_tables(context, iceberg: IcebergResource) -> None:
    """Log compaction intent for each table.

    PyIceberg's compaction API is limited; this logs the intent
    so operators know which tables would benefit from compaction.
    """
    for table_name in TABLES_TO_COMPACT:
        try:
            table = iceberg.load_table(table_name)
            snapshots = table.metadata.snapshots
            snapshot_count = len(snapshots) if snapshots else 0
            context.log.info(
                "MAINTENANCE: %s has %d snapshots. "
                "Consider compaction if data files are fragmented.",
                table_name,
                snapshot_count,
            )
        except Exception:
            context.log.warning(
                "MAINTENANCE: Could not inspect %s — table may not exist.",
                table_name,
            )


@op(ins={"start": In(Nothing)}, description="Expire old snapshots metadata.")
def expire_snapshots(context, iceberg: IcebergResource) -> None:
    """Log snapshot expiration intent."""
    context.log.info(
        "MAINTENANCE: Snapshot expiration would run here. "
        "PyIceberg REST catalog handles metadata cleanup via table properties."
    )


@job(description="Weekly maintenance: compaction + snapshot expiration.")
def maintenance_job() -> None:
    """Run table maintenance tasks sequentially."""
    expire_snapshots(start=compact_tables())
