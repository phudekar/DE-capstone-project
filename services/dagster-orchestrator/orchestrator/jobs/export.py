"""Export job — export Gold daily summary to CSV."""

import logging
from datetime import date

from dagster import job, op

from orchestrator.resources.duckdb_resource import DuckDBResource
from orchestrator.resources.iceberg import IcebergResource

logger = logging.getLogger(__name__)


@op(description="Export Gold daily trading summary to CSV.")
def export_daily_summary_csv(
    context,
    iceberg: IcebergResource,
    duckdb_resource: DuckDBResource,
) -> None:
    """Export the Gold daily trading summary to a CSV file.

    In production this would write to MinIO/S3. For the capstone,
    it writes to a local path and logs the output.
    """
    try:
        table = iceberg.load_table("gold.daily_trading_summary")
        arrow = table.scan().to_arrow()

        if len(arrow) == 0:
            context.log.info("EXPORT: gold.daily_trading_summary is empty — nothing to export.")
            return

        con = duckdb_resource.get_connection()
        con.register("gold_summary", arrow)

        export_path = f"/tmp/daily_trading_summary_{date.today().isoformat()}.csv"
        con.execute(f"COPY gold_summary TO '{export_path}' (HEADER, DELIMITER ',')")
        con.close()

        row_count = len(arrow)
        context.log.info(
            "EXPORT: Wrote %d rows to %s", row_count, export_path
        )

    except Exception:
        context.log.warning(
            "EXPORT: Could not export gold.daily_trading_summary — table may not exist."
        )


@job(description="Export Gold summary data to CSV for external consumption.")
def export_job() -> None:
    """Run the export pipeline."""
    export_daily_summary_csv()
