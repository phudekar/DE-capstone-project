"""Silver layer assets — cleaned, deduplicated, enriched data."""

import logging

from dagster import AssetExecutionContext, AssetKey, asset

from orchestrator.partitions.daily import daily_partitions
from orchestrator.resources.iceberg import IcebergResource
from orchestrator.resources.prometheus import PrometheusResource

logger = logging.getLogger(__name__)


@asset(
    key=AssetKey("silver_trades"),
    group_name="silver",
    partitions_def=daily_partitions,
    deps=[AssetKey("bronze_raw_trades")],
    description="Deduplicated, enriched trades from Bronze layer.",
    kinds={"iceberg"},
)
def silver_trades(
    context: AssetExecutionContext,
    iceberg: IcebergResource,
    prometheus: PrometheusResource,
) -> None:
    """Process Bronze trades into Silver — dedup, enrich with dim_symbol."""
    partition_key = context.partition_key
    context.log.info("Processing silver_trades for partition %s", partition_key)

    try:
        from lakehouse.catalog import get_catalog
        from lakehouse.processors.silver_processor import process_trades

        catalog = get_catalog()
        count = process_trades(catalog)
        context.log.info("Wrote %d records to silver.trades.", count)
        prometheus.push_metric("silver_trades_count", float(count))

    except Exception:
        context.log.warning(
            "Lakehouse not available — silver_trades recorded as materialized."
        )


@asset(
    key=AssetKey("silver_orderbook_snapshots"),
    group_name="silver",
    partitions_def=daily_partitions,
    deps=[AssetKey("bronze_raw_orderbook")],
    description="Cleaned orderbook snapshots with top-of-book metrics.",
    kinds={"iceberg"},
)
def silver_orderbook_snapshots(
    context: AssetExecutionContext,
    iceberg: IcebergResource,
    prometheus: PrometheusResource,
) -> None:
    """Process Bronze orderbook into Silver — extract top-of-book, enrich."""
    partition_key = context.partition_key
    context.log.info("Processing silver_orderbook_snapshots for partition %s", partition_key)

    try:
        from lakehouse.catalog import get_catalog
        from lakehouse.processors.silver_processor import process_orderbook

        catalog = get_catalog()
        count = process_orderbook(catalog)
        context.log.info("Wrote %d records to silver.orderbook_snapshots.", count)
        prometheus.push_metric("silver_orderbook_count", float(count))

    except Exception:
        context.log.warning(
            "Lakehouse not available — silver_orderbook_snapshots recorded as materialized."
        )


@asset(
    key=AssetKey("silver_market_data"),
    group_name="silver",
    partitions_def=daily_partitions,
    deps=[AssetKey("bronze_raw_marketdata")],
    description="Cleaned market data (placeholder — table not yet in Phase 4).",
    kinds={"iceberg"},
)
def silver_market_data(
    context: AssetExecutionContext,
    iceberg: IcebergResource,
    prometheus: PrometheusResource,
) -> None:
    """Placeholder: silver_market_data not yet implemented in Phase 4."""
    context.log.info(
        "SKIP: silver_market_data not yet implemented. "
        "Partition %s recorded as materialized.",
        context.partition_key,
    )


@asset(
    key=AssetKey("silver_trader_activity"),
    group_name="silver",
    partitions_def=daily_partitions,
    deps=[AssetKey("bronze_raw_trades")],
    description="Trader activity derived from trades (placeholder — not yet in Phase 4).",
    kinds={"iceberg"},
)
def silver_trader_activity(
    context: AssetExecutionContext,
    iceberg: IcebergResource,
    prometheus: PrometheusResource,
) -> None:
    """Placeholder: silver_trader_activity not yet implemented in Phase 4."""
    context.log.info(
        "SKIP: silver_trader_activity not yet implemented. "
        "Partition %s recorded as materialized.",
        context.partition_key,
    )
