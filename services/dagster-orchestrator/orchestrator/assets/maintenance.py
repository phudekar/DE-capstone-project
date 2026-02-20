"""Iceberg table maintenance assets — compaction reporting and snapshot stats."""

import logging
from datetime import datetime, timezone

from dagster import AssetExecutionContext, Output, asset

from orchestrator.resources.iceberg import IcebergResource

logger = logging.getLogger(__name__)

TABLES_TO_MAINTAIN = [
    "bronze.raw_trades",
    "bronze.raw_orderbook",
    "silver.trades",
    "silver.orderbook_snapshots",
    "gold.daily_trading_summary",
]


@asset(
    group_name="maintenance",
    description="Collect Iceberg table snapshot statistics for each managed table.",
)
def iceberg_table_stats(
    context: AssetExecutionContext,
    iceberg: IcebergResource,
) -> Output[dict]:
    """Gather snapshot counts, file counts, and record counts per table."""
    stats = {}
    for table_name in TABLES_TO_MAINTAIN:
        try:
            table = iceberg.load_table(table_name)
            snapshots = table.metadata.snapshots or []
            snapshot_count = len(snapshots)

            # Latest snapshot summary (if any)
            current_snapshot = table.metadata.current_snapshot()
            if current_snapshot and current_snapshot.summary:
                summary = dict(current_snapshot.summary)
                total_data_files = int(summary.get("total-data-files", 0))
                total_records = int(summary.get("total-records", 0))
                added_files = int(summary.get("added-data-files", 0))
            else:
                total_data_files = 0
                total_records = 0
                added_files = 0

            stats[table_name] = {
                "snapshot_count": snapshot_count,
                "total_data_files": total_data_files,
                "total_records": total_records,
                "added_files_last_commit": added_files,
                "needs_compaction": total_data_files > 50,
            }
            context.log.info(
                "TABLE STATS %s: snapshots=%d files=%d records=%d",
                table_name,
                snapshot_count,
                total_data_files,
                total_records,
            )
        except Exception as exc:
            context.log.warning("Could not inspect %s: %s", table_name, exc)
            stats[table_name] = {"error": str(exc)}

    return Output(
        value=stats,
        metadata={
            "tables_inspected": len(stats),
            "tables_needing_compaction": sum(
                1 for v in stats.values() if v.get("needs_compaction", False)
            ),
            "collected_at": datetime.now(timezone.utc).isoformat(),
        },
    )


@asset(
    group_name="maintenance",
    deps=["iceberg_table_stats"],
    description="Log compaction recommendations based on table stats.",
)
def compaction_report(
    context: AssetExecutionContext,
    iceberg_table_stats: dict,
) -> Output[list]:
    """Produce a list of tables recommended for compaction."""
    recommendations = []
    for table_name, stats in iceberg_table_stats.items():
        if stats.get("needs_compaction"):
            recommendations.append(
                {
                    "table": table_name,
                    "data_files": stats["total_data_files"],
                    "records": stats["total_records"],
                    "action": "rewrite_data_files",
                }
            )
            context.log.warning(
                "COMPACTION RECOMMENDED: %s has %d files (>50 threshold)",
                table_name,
                stats["total_data_files"],
            )

    if not recommendations:
        context.log.info("All tables are within compaction threshold.")

    return Output(
        value=recommendations,
        metadata={
            "recommendations_count": len(recommendations),
            "generated_at": datetime.now(timezone.utc).isoformat(),
        },
    )
