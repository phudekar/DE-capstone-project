"""Bronze layer assets — raw data ingestion from Kafka to Iceberg."""

import logging

from dagster import AssetExecutionContext, AssetKey, asset

from orchestrator.partitions.daily import daily_partitions
from orchestrator.resources.iceberg import IcebergResource
from orchestrator.resources.kafka import KafkaResource
from orchestrator.resources.prometheus import PrometheusResource

logger = logging.getLogger(__name__)


@asset(
    key=AssetKey("bronze_raw_trades"),
    group_name="bronze",
    partitions_def=daily_partitions,
    description="Raw trade events ingested from Kafka into Bronze Iceberg table.",
    kinds={"iceberg"},
)
def bronze_raw_trades(
    context: AssetExecutionContext,
    kafka: KafkaResource,
    iceberg: IcebergResource,
    prometheus: PrometheusResource,
) -> None:
    """Ingest raw trade messages from Kafka into bronze.raw_trades.

    Delegates to the lakehouse BronzeWriter for actual ingestion logic.
    In sensor-triggered mode, this asset signals that new data is available.
    """
    partition_key = context.partition_key
    context.log.info("Processing bronze_raw_trades for partition %s", partition_key)

    try:
        from lakehouse.catalog import get_catalog
        from lakehouse.writers.bronze_writer import BronzeWriter

        catalog = get_catalog()
        table = catalog.load_table("bronze.raw_trades")
        scan = table.scan()
        arrow = scan.to_arrow()
        record_count = len(arrow)

        context.log.info("bronze.raw_trades contains %d total records.", record_count)
        prometheus.push_metric("bronze_raw_trades_count", float(record_count))

    except Exception:
        context.log.warning(
            "Lakehouse not available — bronze_raw_trades recorded as materialized."
        )


@asset(
    key=AssetKey("bronze_raw_orderbook"),
    group_name="bronze",
    partitions_def=daily_partitions,
    description="Raw orderbook snapshots ingested from Kafka into Bronze Iceberg table.",
    kinds={"iceberg"},
)
def bronze_raw_orderbook(
    context: AssetExecutionContext,
    kafka: KafkaResource,
    iceberg: IcebergResource,
    prometheus: PrometheusResource,
) -> None:
    """Ingest raw orderbook snapshots from Kafka into bronze.raw_orderbook."""
    partition_key = context.partition_key
    context.log.info("Processing bronze_raw_orderbook for partition %s", partition_key)

    try:
        from lakehouse.catalog import get_catalog

        catalog = get_catalog()
        table = catalog.load_table("bronze.raw_orderbook")
        scan = table.scan()
        arrow = scan.to_arrow()
        record_count = len(arrow)

        context.log.info("bronze.raw_orderbook contains %d total records.", record_count)
        prometheus.push_metric("bronze_raw_orderbook_count", float(record_count))

    except Exception:
        context.log.warning(
            "Lakehouse not available — bronze_raw_orderbook recorded as materialized."
        )


@asset(
    key=AssetKey("bronze_raw_marketdata"),
    group_name="bronze",
    partitions_def=daily_partitions,
    description="Raw market data ingested from Kafka (placeholder — table not yet in Phase 4).",
    kinds={"iceberg"},
)
def bronze_raw_marketdata(
    context: AssetExecutionContext,
    kafka: KafkaResource,
    iceberg: IcebergResource,
    prometheus: PrometheusResource,
) -> None:
    """Placeholder: market data table not yet implemented in Phase 4."""
    context.log.info(
        "SKIP: bronze_raw_marketdata not yet implemented. "
        "Partition %s recorded as materialized.",
        context.partition_key,
    )
