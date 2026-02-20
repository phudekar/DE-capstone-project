"""Weekly schedules for dimension refresh and maintenance."""

from __future__ import annotations

from dagster import (
    AssetKey,
    AssetSelection,
    DefaultScheduleStatus,
    ScheduleDefinition,
    define_asset_job,
)

# Weekly dimension refresh: Sunday at 2 AM UTC
dimension_refresh_job = define_asset_job(
    name="dimension_refresh_job",
    selection=AssetSelection.assets(
        AssetKey("dim_symbol"),
        AssetKey("dim_trader"),
        AssetKey("dim_exchange"),
        AssetKey("dim_time"),
        AssetKey("dim_account"),
    ),
    description="Refresh all dimension tables.",
)

weekly_dimension_schedule = ScheduleDefinition(
    name="weekly_dimension_schedule",
    job=dimension_refresh_job,
    cron_schedule="0 2 * * 0",
    default_status=DefaultScheduleStatus.STOPPED,
    description="Refresh dimension tables every Sunday at 2 AM UTC.",
)

# Weekly maintenance: Sunday at 4 AM UTC
weekly_maintenance_schedule = ScheduleDefinition(
    name="weekly_maintenance_schedule",
    job_name="maintenance_job",
    cron_schedule="0 4 * * 0",
    default_status=DefaultScheduleStatus.STOPPED,
    description="Run table maintenance every Sunday at 4 AM UTC.",
)
