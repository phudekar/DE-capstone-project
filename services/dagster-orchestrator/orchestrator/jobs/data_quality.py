"""Data quality job — basic checks using DuckDB SQL."""

import logging

from dagster import In, Nothing, job, op

from orchestrator.resources.duckdb_resource import DuckDBResource
from orchestrator.resources.iceberg import IcebergResource

logger = logging.getLogger(__name__)


@op(description="Check Bronze data quality: non-null trade IDs and positive prices.")
def check_bronze_quality(context, iceberg: IcebergResource, duckdb_resource: DuckDBResource) -> None:
    """Run basic quality checks on Bronze trade data."""
    try:
        table = iceberg.load_table("bronze.raw_trades")
        arrow = table.scan().to_arrow()

        if len(arrow) == 0:
            context.log.info("DQ: bronze.raw_trades is empty — skipping checks.")
            return

        con = duckdb_resource.get_connection()
        con.register("bronze_trades", arrow)

        # Check: no null trade_ids
        result = con.execute(
            "SELECT count(*) AS cnt FROM bronze_trades WHERE trade_id IS NULL"
        ).fetchone()
        null_count = result[0]
        if null_count > 0:
            context.log.warning("DQ FAIL: %d null trade_ids in bronze.raw_trades.", null_count)
        else:
            context.log.info("DQ PASS: No null trade_ids in bronze.raw_trades.")

        # Check: all prices > 0
        result = con.execute(
            "SELECT count(*) AS cnt FROM bronze_trades WHERE price <= 0"
        ).fetchone()
        bad_prices = result[0]
        if bad_prices > 0:
            context.log.warning("DQ FAIL: %d non-positive prices in bronze.raw_trades.", bad_prices)
        else:
            context.log.info("DQ PASS: All prices positive in bronze.raw_trades.")

        con.close()
    except Exception:
        context.log.warning("DQ: Could not check bronze.raw_trades — table may not exist.")


@op(ins={"start": In(Nothing)}, description="Check Silver data quality: dedup and enrichment.")
def check_silver_quality(context, iceberg: IcebergResource, duckdb_resource: DuckDBResource) -> None:
    """Run basic quality checks on Silver trade data."""
    try:
        table = iceberg.load_table("silver.trades")
        arrow = table.scan().to_arrow()

        if len(arrow) == 0:
            context.log.info("DQ: silver.trades is empty — skipping checks.")
            return

        con = duckdb_resource.get_connection()
        con.register("silver_trades", arrow)

        # Check: no duplicate trade_ids
        result = con.execute(
            "SELECT count(*) - count(DISTINCT trade_id) AS dups FROM silver_trades"
        ).fetchone()
        dup_count = result[0]
        if dup_count > 0:
            context.log.warning("DQ FAIL: %d duplicate trade_ids in silver.trades.", dup_count)
        else:
            context.log.info("DQ PASS: No duplicate trade_ids in silver.trades.")

        con.close()
    except Exception:
        context.log.warning("DQ: Could not check silver.trades — table may not exist.")


@op(ins={"start": In(Nothing)}, description="Check Gold data quality: trade counts > 0.")
def check_gold_quality(context, iceberg: IcebergResource, duckdb_resource: DuckDBResource) -> None:
    """Run basic quality checks on Gold summary data."""
    try:
        table = iceberg.load_table("gold.daily_trading_summary")
        arrow = table.scan().to_arrow()

        if len(arrow) == 0:
            context.log.info("DQ: gold.daily_trading_summary is empty — skipping checks.")
            return

        con = duckdb_resource.get_connection()
        con.register("gold_summary", arrow)

        # Check: trade_count > 0 for all rows
        result = con.execute(
            "SELECT count(*) AS cnt FROM gold_summary WHERE trade_count <= 0"
        ).fetchone()
        bad_count = result[0]
        if bad_count > 0:
            context.log.warning(
                "DQ FAIL: %d rows with trade_count <= 0 in gold.daily_trading_summary.",
                bad_count,
            )
        else:
            context.log.info("DQ PASS: All trade_counts positive in gold.daily_trading_summary.")

        con.close()
    except Exception:
        context.log.warning(
            "DQ: Could not check gold.daily_trading_summary — table may not exist."
        )


@job(description="Data quality checks across Bronze, Silver, and Gold layers.")
def data_quality_job() -> None:
    """Run quality checks sequentially through each layer."""
    bronze_result = check_bronze_quality()
    silver_result = check_silver_quality(start=bronze_result)
    check_gold_quality(start=silver_result)
