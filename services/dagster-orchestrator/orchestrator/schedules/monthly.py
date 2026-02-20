"""Monthly schedules for Gold recomputation."""

from __future__ import annotations

from dagster import (
    AssetKey,
    AssetSelection,
    DefaultScheduleStatus,
    ScheduleDefinition,
    define_asset_job,
)

# Monthly recompute: 1st of month at 3 AM UTC
gold_monthly_recompute_job = define_asset_job(
    name="gold_monthly_recompute_job",
    selection=AssetSelection.assets(
        AssetKey("gold_daily_trading_summary"),
        AssetKey("gold_trader_performance"),
        AssetKey("gold_market_overview"),
        AssetKey("gold_portfolio_positions"),
    ),
    description="Recompute Gold assets for the prior month (backfill).",
)

monthly_recompute_schedule = ScheduleDefinition(
    name="monthly_recompute_schedule",
    job=gold_monthly_recompute_job,
    cron_schedule="0 3 1 * *",
    default_status=DefaultScheduleStatus.STOPPED,
    description="Recompute Gold assets on the 1st of each month at 3 AM UTC.",
)
