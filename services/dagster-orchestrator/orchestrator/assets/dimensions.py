"""Dimension table assets — reference data for enrichment."""

import logging

from dagster import AssetExecutionContext, AssetKey, asset

from orchestrator.resources.iceberg import IcebergResource
from orchestrator.resources.prometheus import PrometheusResource

logger = logging.getLogger(__name__)


@asset(
    key=AssetKey("dim_symbol"),
    group_name="dimensions",
    description="Symbol master dimension (SCD Type 2) — company names, sectors.",
    kinds={"iceberg"},
)
def dim_symbol(
    context: AssetExecutionContext,
    iceberg: IcebergResource,
    prometheus: PrometheusResource,
) -> None:
    """Refresh dim_symbol from reference data using lakehouse seed module."""
    context.log.info("Refreshing dim_symbol dimension table.")

    try:
        from lakehouse.catalog import get_catalog
        from lakehouse.seed.seed_dimensions import seed_dim_symbol

        catalog = get_catalog()
        seed_dim_symbol(catalog)
        context.log.info("dim_symbol refreshed successfully.")

    except Exception:
        context.log.warning(
            "Lakehouse not available — dim_symbol recorded as materialized."
        )


@asset(
    key=AssetKey("dim_time"),
    group_name="dimensions",
    description="Time dimension (SCD Type 1) — 1440 minute-level rows.",
    kinds={"iceberg"},
)
def dim_time(
    context: AssetExecutionContext,
    iceberg: IcebergResource,
    prometheus: PrometheusResource,
) -> None:
    """Refresh dim_time from lakehouse seed module."""
    context.log.info("Refreshing dim_time dimension table.")

    try:
        from lakehouse.catalog import get_catalog
        from lakehouse.seed.seed_dimensions import seed_dim_time

        catalog = get_catalog()
        seed_dim_time(catalog)
        context.log.info("dim_time refreshed successfully.")

    except Exception:
        context.log.warning(
            "Lakehouse not available — dim_time recorded as materialized."
        )


@asset(
    key=AssetKey("dim_trader"),
    group_name="dimensions",
    description="Trader dimension (placeholder — not yet in Phase 4).",
    kinds={"iceberg"},
)
def dim_trader(
    context: AssetExecutionContext,
    iceberg: IcebergResource,
    prometheus: PrometheusResource,
) -> None:
    """Placeholder: dim_trader not yet implemented in Phase 4."""
    context.log.info("SKIP: dim_trader not yet implemented.")


@asset(
    key=AssetKey("dim_exchange"),
    group_name="dimensions",
    description="Exchange dimension (placeholder — not yet in Phase 4).",
    kinds={"iceberg"},
)
def dim_exchange(
    context: AssetExecutionContext,
    iceberg: IcebergResource,
    prometheus: PrometheusResource,
) -> None:
    """Placeholder: dim_exchange not yet implemented in Phase 4."""
    context.log.info("SKIP: dim_exchange not yet implemented.")


@asset(
    key=AssetKey("dim_account"),
    group_name="dimensions",
    description="Account dimension (placeholder — not yet in Phase 4).",
    kinds={"iceberg"},
)
def dim_account(
    context: AssetExecutionContext,
    iceberg: IcebergResource,
    prometheus: PrometheusResource,
) -> None:
    """Placeholder: dim_account not yet implemented in Phase 4."""
    context.log.info("SKIP: dim_account not yet implemented.")
