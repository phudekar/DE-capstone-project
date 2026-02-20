"""Gold layer assets — pre-aggregated business metrics."""

import logging
from datetime import date

from dagster import AssetExecutionContext, AssetKey, asset

from orchestrator.partitions.daily import daily_partitions
from orchestrator.resources.duckdb_resource import DuckDBResource
from orchestrator.resources.iceberg import IcebergResource
from orchestrator.resources.prometheus import PrometheusResource

logger = logging.getLogger(__name__)


@asset(
    key=AssetKey("gold_daily_trading_summary"),
    group_name="gold",
    partitions_def=daily_partitions,
    deps=[AssetKey("silver_trades")],
    description="Daily OHLCV trading summary aggregated from Silver trades.",
    kinds={"iceberg"},
)
def gold_daily_trading_summary(
    context: AssetExecutionContext,
    iceberg: IcebergResource,
    duckdb_resource: DuckDBResource,
    prometheus: PrometheusResource,
) -> None:
    """Aggregate Silver trades into daily trading summary using DuckDB."""
    partition_key = context.partition_key
    trading_date = date.fromisoformat(partition_key)
    context.log.info("Processing gold_daily_trading_summary for %s", trading_date)

    try:
        from lakehouse.catalog import get_catalog
        from lakehouse.processors.gold_aggregator import aggregate_daily_trading_summary

        catalog = get_catalog()
        count = aggregate_daily_trading_summary(catalog, trading_date)
        context.log.info("Wrote %d daily summaries for %s.", count, trading_date)
        prometheus.push_metric(
            "gold_daily_summary_count", float(count), {"date": str(trading_date)}
        )

    except Exception:
        context.log.warning(
            "Lakehouse not available — gold_daily_trading_summary recorded as materialized."
        )


@asset(
    key=AssetKey("gold_trader_performance"),
    group_name="gold",
    partitions_def=daily_partitions,
    deps=[AssetKey("silver_trades"), AssetKey("silver_trader_activity")],
    description="Trader performance metrics (placeholder — not yet in Phase 4).",
    kinds={"iceberg"},
)
def gold_trader_performance(
    context: AssetExecutionContext,
    iceberg: IcebergResource,
    duckdb_resource: DuckDBResource,
    prometheus: PrometheusResource,
) -> None:
    """Placeholder: gold_trader_performance not yet implemented in Phase 4."""
    context.log.info(
        "SKIP: gold_trader_performance not yet implemented. "
        "Partition %s recorded as materialized.",
        context.partition_key,
    )


@asset(
    key=AssetKey("gold_market_overview"),
    group_name="gold",
    partitions_def=daily_partitions,
    deps=[AssetKey("silver_trades"), AssetKey("silver_market_data")],
    description="Market overview dashboard metrics (placeholder — not yet in Phase 4).",
    kinds={"iceberg"},
)
def gold_market_overview(
    context: AssetExecutionContext,
    iceberg: IcebergResource,
    duckdb_resource: DuckDBResource,
    prometheus: PrometheusResource,
) -> None:
    """Placeholder: gold_market_overview not yet implemented in Phase 4."""
    context.log.info(
        "SKIP: gold_market_overview not yet implemented. "
        "Partition %s recorded as materialized.",
        context.partition_key,
    )


@asset(
    key=AssetKey("gold_portfolio_positions"),
    group_name="gold",
    partitions_def=daily_partitions,
    deps=[AssetKey("silver_trades"), AssetKey("silver_trader_activity")],
    description="Portfolio position snapshots (placeholder — not yet in Phase 4).",
    kinds={"iceberg"},
)
def gold_portfolio_positions(
    context: AssetExecutionContext,
    iceberg: IcebergResource,
    duckdb_resource: DuckDBResource,
    prometheus: PrometheusResource,
) -> None:
    """Placeholder: gold_portfolio_positions not yet implemented in Phase 4."""
    context.log.info(
        "SKIP: gold_portfolio_positions not yet implemented. "
        "Partition %s recorded as materialized.",
        context.partition_key,
    )
